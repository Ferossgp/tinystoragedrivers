//! Reads and write transactions in as few requests as possible.
//!
//! A [`Tx`] opens `BEGIN IMMEDIATE` in the same request as its first reads,
//! queues its writes, and sends them with the next reads or with the
//! `COMMIT`, so a read-decide-write transaction costs two requests. A
//! transaction that never reads is sent as one transactional batch. Statements
//! that share a request are rendered with their parameters inlined
//! ([`Statement::render`]), because a multi-statement request is SQL text
//! only.

use std::borrow::Cow;
use std::time::Duration;

use libsql::params::IntoParams;
use libsql::{Connection, Value};
use tinystoragedrivers_types::{Result, StorageError, WriteCondition};

use super::Statement;
use super::error::{commit_failed, during, unknown_outcome};
use super::transactional_batch;
use crate::SqliteTables;
use crate::layout::{GuardRow, StoredBody, judge_clause};

/// Client bound on one request. Longer than a remote server's five-second
/// transaction limit, so a server-side abort surfaces first; this only
/// catches a connection that stopped answering.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One result row: the column values in select order.
#[derive(Debug, Clone, PartialEq)]
pub struct Row(Vec<Value>);

impl Row {
    /// The value of column `index`.
    ///
    /// # Errors
    /// Returns a backend error when the row has no such column.
    pub fn value(&self, index: usize) -> Result<&Value> {
        self.0
            .get(index)
            .ok_or_else(|| corrupt("a selected column is missing"))
    }

    /// Move the value of column `index` out, leaving `NULL`.
    ///
    /// # Errors
    /// Returns a backend error when the row has no such column.
    pub fn take(&mut self, index: usize) -> Result<Value> {
        self.0
            .get_mut(index)
            .map(|value| std::mem::replace(value, Value::Null))
            .ok_or_else(|| corrupt("a selected column is missing"))
    }

    /// An `INTEGER` column.
    ///
    /// # Errors
    /// Returns a backend error for a missing column or another type.
    pub fn integer(&self, index: usize) -> Result<i64> {
        match self.value(index)? {
            Value::Integer(value) => Ok(*value),
            _ => Err(corrupt("a column is not an integer")),
        }
    }

    /// An `INTEGER` column that may be `NULL`.
    ///
    /// # Errors
    /// Returns a backend error for a missing column or another type.
    pub fn opt_integer(&self, index: usize) -> Result<Option<i64>> {
        match self.value(index)? {
            Value::Null => Ok(None),
            Value::Integer(value) => Ok(Some(*value)),
            _ => Err(corrupt("a column is not an integer")),
        }
    }

    /// A numeric column as a float.
    ///
    /// # Errors
    /// Returns a backend error for a missing column or another type.
    #[allow(
        clippy::cast_precision_loss,
        reason = "SQLite itself compares integers and reals this way"
    )]
    pub fn real(&self, index: usize) -> Result<f64> {
        match self.value(index)? {
            Value::Real(value) => Ok(*value),
            Value::Integer(value) => Ok(*value as f64),
            _ => Err(corrupt("a column is not a number")),
        }
    }

    /// A `TEXT` column.
    ///
    /// # Errors
    /// Returns a backend error for a missing column or another type.
    pub fn text(&self, index: usize) -> Result<String> {
        self.opt_text(index)?
            .ok_or_else(|| corrupt("a column is not text"))
    }

    /// A `TEXT` column that may be `NULL`.
    ///
    /// # Errors
    /// Returns a backend error for a missing column or another type.
    pub fn opt_text(&self, index: usize) -> Result<Option<String>> {
        match self.value(index)? {
            Value::Null => Ok(None),
            Value::Text(text) => Ok(Some(text.clone())),
            _ => Err(corrupt("a column is not text")),
        }
    }

    /// A `BLOB` column.
    ///
    /// # Errors
    /// Returns a backend error for a missing column or another type.
    pub fn blob(&self, index: usize) -> Result<Vec<u8>> {
        match self.value(index)? {
            Value::Blob(bytes) => Ok(bytes.clone()),
            _ => Err(corrupt("a column is not a blob")),
        }
    }
}

fn corrupt(what: &str) -> StorageError {
    StorageError::backend(format!("corrupt libsql row: {what}"))
}

/// Run `reads` without a write transaction in one request and return each
/// one's rows. Several reads share one read transaction, so they see one
/// snapshot.
///
/// # Errors
/// Returns the first failing statement's error, mapped for `operation`.
pub async fn query(
    conn: &Connection,
    operation: &'static str,
    reads: &[Statement],
) -> Result<Vec<Vec<Row>>> {
    match reads {
        [] => Ok(Vec::new()),
        [read] => {
            let rows = within(conn, operation, async {
                let mut rows = conn.query(read.sql(), read.params().to_vec()).await?;
                drain(&mut rows).await
            })
            .await?;
            Ok(vec![rows])
        }
        _ => {
            let mut sql = String::from("BEGIN;\n");
            for read in reads {
                push(&mut sql, read).map_err(during(operation))?;
            }
            sql.push_str("COMMIT");
            let sets = within(conn, operation, async {
                collect(conn.execute_batch(&sql).await?).await
            })
            .await;
            if sets.is_err() && !conn.is_autocommit() && conn.execute("ROLLBACK", ()).await.is_err()
            {
                conn.reset().await;
            }
            let mut sets = expect_sets(sets?, reads.len() + 2)?;
            sets.pop();
            Ok(sets.split_off(1))
        }
    }
}

/// One write transaction on a locked connection.
///
/// Reads run inside `BEGIN IMMEDIATE`; writes are queued and sent with the
/// next read or the commit. End every transaction with [`Tx::finish`], which
/// commits or rolls back.
#[derive(Debug)]
pub struct Tx<'a> {
    conn: &'a Connection,
    operation: &'static str,
    condition: Option<(&'a SqliteTables, &'a WriteCondition, i64)>,
    begun: bool,
    pending: Vec<Statement>,
}

impl<'a> Tx<'a> {
    /// A transaction named `operation` in errors.
    #[must_use]
    pub fn new(conn: &'a Connection, operation: &'static str) -> Self {
        Self {
            conn,
            operation,
            condition: None,
            begun: false,
            pending: Vec::new(),
        }
    }

    /// Check `condition`, if any, at `now_ms` in the transaction's first
    /// request, before anything is written.
    #[must_use]
    pub fn conditioned(
        mut self,
        tables: &'a SqliteTables,
        condition: Option<&'a WriteCondition>,
        now_ms: i64,
    ) -> Self {
        self.condition = condition.map(|condition| (tables, condition, now_ms));
        self
    }

    /// Queue a write.
    pub fn write(&mut self, statement: Statement) {
        self.pending.push(statement);
    }

    /// Queue a write from its text and positional parameters.
    ///
    /// # Errors
    /// Returns a backend error for parameters that do not convert.
    pub fn write_sql(
        &mut self,
        sql: impl Into<Cow<'static, str>>,
        params: impl IntoParams,
    ) -> Result<()> {
        let statement = Statement::new(sql, params).map_err(during(self.operation))?;
        self.pending.push(statement);
        Ok(())
    }

    /// Send the queued writes and then `reads` in one request, opening the
    /// transaction (and checking its condition) first if this is its first
    /// request; return each read's rows. The reads see the writes.
    ///
    /// # Errors
    /// Returns `Conflict` when the condition fails, or the first failing
    /// statement's error.
    pub async fn read(&mut self, reads: &[Statement]) -> Result<Vec<Vec<Row>>> {
        let operation = self.operation;
        let mut sql = String::new();
        let mut skipped = 0;
        let mut guards = Vec::new();
        // The condition is checked once, in the request that opens the
        // transaction; later reads see the same locked state.
        let judged = if self.begun { None } else { self.condition };
        if !self.begun {
            sql.push_str("BEGIN IMMEDIATE;\n");
            skipped += 1;
            if let Some((tables, condition, _)) = judged {
                guards = condition_reads(tables, condition).map_err(during(operation))?;
            }
        }
        for write in &self.pending {
            push(&mut sql, write).map_err(during(operation))?;
            skipped += 1;
        }
        for read in guards.iter().chain(reads) {
            push(&mut sql, read).map_err(during(operation))?;
        }
        self.begun = true;
        self.pending.clear();
        let conn = self.conn;
        let sets = within(conn, operation, async {
            collect(conn.execute_batch(&sql).await?).await
        })
        .await?;
        let mut sets = expect_sets(sets, skipped + guards.len() + reads.len())?;
        let mut answers = sets.split_off(skipped);
        let rest = answers.split_off(guards.len());
        if let Some((_, condition, now_ms)) = judged {
            judge(condition, answers, now_ms)?;
        }
        Ok(rest)
    }

    /// [`Self::read`] for one statement.
    ///
    /// # Errors
    /// As [`Self::read`].
    pub async fn read_one(&mut self, read: Statement) -> Result<Vec<Row>> {
        self.read(std::slice::from_ref(&read))
            .await?
            .pop()
            .ok_or_else(|| corrupt("a read returned no result"))
    }

    /// Commit when `outcome` is a success, roll back otherwise.
    ///
    /// # Errors
    /// Returns the error of `outcome`, or of the commit. A commit whose
    /// request may have been applied is reported as a backend error naming
    /// the unknown outcome.
    pub async fn finish<T>(mut self, outcome: Result<T>) -> Result<T> {
        let outcome = match outcome {
            Ok(value) => self.commit().await.map(|()| value),
            Err(error) => Err(error),
        };
        if outcome.is_err()
            && !self.conn.is_autocommit()
            && self.conn.execute("ROLLBACK", ()).await.is_err()
        {
            self.conn.reset().await;
        }
        outcome
    }

    async fn commit(&mut self) -> Result<()> {
        if !self.begun && self.condition.is_some() {
            self.read(&[]).await?;
        }
        let operation = self.operation;
        let conn = self.conn;
        let sent = if self.begun {
            let mut sql = String::new();
            for write in &self.pending {
                push(&mut sql, write).map_err(during(operation))?;
            }
            sql.push_str("COMMIT");
            tokio::time::timeout(REQUEST_TIMEOUT, async {
                conn.execute_batch(&sql).await.map(drop)
            })
            .await
        } else if self.pending.is_empty() {
            return Ok(());
        } else {
            tokio::time::timeout(REQUEST_TIMEOUT, async {
                transactional_batch(conn, &self.pending).await.map(drop)
            })
            .await
        };
        self.pending.clear();
        match sent {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(commit_failed(operation, error)),
            Err(_) => {
                conn.reset().await;
                Err(unknown_outcome(operation))
            }
        }
    }
}

/// Append one rendered statement and its terminator.
fn push(sql: &mut String, statement: &Statement) -> libsql::Result<()> {
    sql.push_str(&statement.render()?);
    sql.push_str(";\n");
    Ok(())
}

/// Run one request under [`REQUEST_TIMEOUT`]; a request that does not answer
/// abandons the connection's server stream.
async fn within<T>(
    conn: &Connection,
    operation: &'static str,
    request: impl Future<Output = libsql::Result<T>>,
) -> Result<T> {
    let Ok(result) = tokio::time::timeout(REQUEST_TIMEOUT, request).await else {
        conn.reset().await;
        return Err(StorageError::unavailable(format!(
            "libsql did not answer during {operation}"
        )));
    };
    result.map_err(during(operation))
}

async fn collect(mut batch: libsql::BatchRows) -> libsql::Result<Vec<Vec<Row>>> {
    let mut sets = Vec::new();
    while let Some(entry) = batch.next_stmt_row() {
        sets.push(match entry {
            Some(mut rows) => drain(&mut rows).await?,
            None => Vec::new(),
        });
    }
    Ok(sets)
}

async fn drain(rows: &mut libsql::Rows) -> libsql::Result<Vec<Row>> {
    let width = rows.column_count();
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(Row((0..width)
            .map(|index| row.get_value(index))
            .collect::<libsql::Result<_>>()?));
    }
    Ok(out)
}

/// A local batch reports no entry for a statement that is all whitespace and
/// a remote one an entry per statement; ours have none, so the counts match.
fn expect_sets(sets: Vec<Vec<Row>>, expected: usize) -> Result<Vec<Vec<Row>>> {
    if sets.len() == expected {
        Ok(sets)
    } else {
        Err(StorageError::backend(
            "libsql returned a different number of statement results than sent",
        ))
    }
}

/// The document and declaration reads of every clause of `condition`.
fn condition_reads(
    tables: &SqliteTables,
    condition: &WriteCondition,
) -> libsql::Result<Vec<Statement>> {
    let mut reads = Vec::with_capacity(condition.clauses().len() * 2);
    for clause in condition.clauses() {
        reads.push(Statement::new(
            tables.condition_query(),
            [clause.scope().as_str(), clause.collection(), clause.id()],
        )?);
        reads.push(Statement::new(tables.spec_query(), [clause.collection()])?);
    }
    Ok(reads)
}

/// Decide `condition` from the rows of [`condition_reads`].
fn judge(condition: &WriteCondition, sets: Vec<Vec<Row>>, now_ms: i64) -> Result<()> {
    let mut sets = sets.into_iter();
    for clause in condition.clauses() {
        let (Some(document), Some(spec)) = (sets.next(), sets.next()) else {
            return Err(corrupt("a condition read is missing"));
        };
        let row = document.into_iter().next().map(guard_row).transpose()?;
        let spec = spec.first().map(|row| row.text(0)).transpose()?;
        judge_clause(clause, row, spec.as_deref(), now_ms)?;
    }
    Ok(())
}

fn guard_row(mut row: Row) -> Result<GuardRow> {
    let body = match row.take(4)? {
        Value::Text(text) => StoredBody::Text(text),
        Value::Blob(bytes) => StoredBody::Blob(bytes),
        _ => return Err(StorageError::backend("corrupt conditional document body")),
    };
    Ok(GuardRow {
        schema: u32::try_from(row.integer(0)?)
            .map_err(|_| corrupt("a document schema is out of range"))?,
        created_ms: row.integer(1)?,
        updated_ms: row.integer(2)?,
        expires_ms: row.opt_integer(3)?,
        body,
    })
}
