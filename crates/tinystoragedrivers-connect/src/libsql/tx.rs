//! Reads and write transactions in few requests.
//!
//! Every read is its own request: `libsql` sends each statement of a
//! multi-statement request with `want_rows: false`, and a server that honors
//! it (Turso Cloud; a local sqld ignores it) returns no rows. Writes carry no
//! rows, so a [`Tx`] opens `BEGIN IMMEDIATE` and sends its queued writes as
//! one request before its next reads, and the rest with the `COMMIT`. A
//! transaction that never reads is sent as one transactional batch.
//! Statements that share a request are rendered with their parameters
//! inlined ([`Statement::render`]), because a multi-statement request is SQL
//! text only.

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

/// Run `reads` without a write transaction and return each one's rows.
/// Several reads share one read transaction, so they see one snapshot.
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
        [read] => Ok(vec![read_rows(conn, operation, read).await?]),
        _ => {
            let sets = async {
                within(conn, operation, conn.execute("BEGIN", ())).await?;
                let sets = read_each(conn, operation, reads).await?;
                within(conn, operation, conn.execute("COMMIT", ())).await?;
                Ok(sets)
            }
            .await;
            if sets.is_err() && !conn.is_autocommit() && conn.execute("ROLLBACK", ()).await.is_err()
            {
                conn.reset().await;
            }
            sets
        }
    }
}

/// One write transaction on a locked connection.
///
/// Reads run inside `BEGIN IMMEDIATE`; writes are queued and sent before the
/// next read or with the commit. End every transaction with [`Tx::finish`], which
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

    /// Send the queued writes, opening the transaction (and checking its
    /// condition) first if this is its first request, then run `reads`;
    /// return each read's rows. The reads see the writes.
    ///
    /// # Errors
    /// Returns `Conflict` when the condition fails, or the first failing
    /// statement's error.
    pub async fn read(&mut self, reads: &[Statement]) -> Result<Vec<Vec<Row>>> {
        let operation = self.operation;
        let conn = self.conn;
        // The condition is checked once, when the transaction opens; later
        // reads see the same locked state.
        let judged = if self.begun { None } else { self.condition };
        let mut sql = String::new();
        if !self.begun {
            sql.push_str("BEGIN IMMEDIATE;\n");
        }
        for write in &self.pending {
            push(&mut sql, write).map_err(during(operation))?;
        }
        self.begun = true;
        self.pending.clear();
        if !sql.is_empty() {
            within(conn, operation, conn.execute_batch(&sql)).await?;
        }
        if let Some((tables, condition, now_ms)) = judged {
            let guards = condition_reads(tables, condition).map_err(during(operation))?;
            judge(condition, read_each(conn, operation, &guards).await?, now_ms)?;
        }
        read_each(conn, operation, reads).await
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

async fn read_each(
    conn: &Connection,
    operation: &'static str,
    reads: &[Statement],
) -> Result<Vec<Vec<Row>>> {
    let mut sets = Vec::with_capacity(reads.len());
    for read in reads {
        sets.push(read_rows(conn, operation, read).await?);
    }
    Ok(sets)
}

async fn read_rows(conn: &Connection, operation: &'static str, read: &Statement) -> Result<Vec<Row>> {
    within(conn, operation, async {
        let mut rows = conn.query(read.sql(), read.params().to_vec()).await?;
        drain(&mut rows).await
    })
    .await
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
