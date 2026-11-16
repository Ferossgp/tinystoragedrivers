//! [`StreamStore`] on libSQL, with the SQLite engine's layout and rules.
//!
//! Records live in one table keyed by `(scope, stream, offset)`; a head row
//! per stream holds the first retained offset (`base`) and the next offset to
//! allocate (`len`). Allocating and inserting happen in one immediate
//! transaction (the head is read in the request that opens it, the records
//! go with the commit), so offsets are dense even with concurrent appenders.
//! A sealed stream keeps its entries as one compressed blob in the blob
//! table; the private `seal` module holds the protocol.

mod seal;

use std::fmt;

use serde_json::Value;
use tinystoragedrivers_api::{StreamStore, async_trait};
use tinystoragedrivers_connect::LibsqlConnection;
use tinystoragedrivers_connect::libsql::libsql::{Connection, Value as SqlValue};
use tinystoragedrivers_connect::libsql::{Row, Statement, Tx, query};
use tinystoragedrivers_types::{
    Capabilities, Capability, Result, Scope, SealOutcome, StorageError, StreamEntry, StreamInfo,
    WriteCondition,
};

use crate::libsql::{SqliteTables, ident, statement};
use crate::validate_stream;

/// libSQL streams bound to one scope.
#[derive(Clone)]
pub struct LibsqlStreams {
    connection: LibsqlConnection,
    scope: Scope,
    condition: Option<WriteCondition>,
}

impl fmt::Debug for LibsqlStreams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LibsqlStreams")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// A stream's head row.
struct Head {
    /// The first retained offset.
    base: u64,
    /// The next offset to allocate.
    len: u64,
    /// Whether the entries are in the sealed blob rather than in rows.
    sealed: bool,
    /// When the stream was sealed, if known.
    sealed_ms: Option<i64>,
    /// When a record was last appended, if known.
    last_append_ms: Option<i64>,
}

/// The statement reading a stream's head.
fn head_statement(tables: &SqliteTables, scope: &str, stream: &str) -> Result<Statement> {
    statement(
        format!(
            "SELECT base, len, sealed, sealed_ms, last_append_ms FROM {}
             WHERE scope = ?1 AND name = ?2",
            ident(tables.stream_heads())
        ),
        [scope, stream],
    )
}

/// The head in the rows of [`head_statement`], if the stream exists.
fn head_of(rows: &[Row]) -> Result<Option<Head>> {
    rows.first()
        .map(|row| {
            Ok(Head {
                base: to_offset(row.integer(0)?),
                len: to_offset(row.integer(1)?),
                sealed: row.integer(2)? != 0,
                sealed_ms: row.opt_integer(3)?,
                last_append_ms: row.opt_integer(4)?,
            })
        })
        .transpose()
}

/// Queue the removal of a stream's entries, head and sealed blob.
fn delete_in(tx: &mut Tx<'_>, tables: &SqliteTables, scope: &str, stream: &str) -> Result<()> {
    for table in [tables.streams(), tables.stream_heads()] {
        tx.write(statement(
            format!(
                "DELETE FROM {} WHERE scope = ?1 AND name = ?2",
                ident(table)
            ),
            [scope, stream],
        )?);
    }
    // Even when the stream is gone, clear a blob a crashed seal left.
    seal::delete_blob(tx, tables, scope, stream)
}

fn to_offset(stored: i64) -> u64 {
    u64::try_from(stored).unwrap_or(0)
}

fn to_stored(offset: u64) -> Result<i64> {
    i64::try_from(offset).map_err(|_| StorageError::backend("stream offset space is exhausted"))
}

impl LibsqlStreams {
    /// Bind a stream adapter to a shared libSQL connection and opaque scope.
    #[must_use]
    pub fn new(connection: LibsqlConnection, scope: Scope) -> Self {
        Self {
            connection,
            scope,
            condition: None,
        }
    }

    /// Bind a stream adapter whose mutations check `condition` in their
    /// write transaction.
    #[must_use]
    pub fn conditioned(
        connection: LibsqlConnection,
        scope: Scope,
        condition: WriteCondition,
    ) -> Self {
        Self {
            connection,
            scope,
            condition: Some(condition),
        }
    }

    fn tables(&self) -> &SqliteTables {
        self.connection.tables()
    }

    fn scope(&self) -> &str {
        self.scope.as_str()
    }

    /// A write transaction that checks this handle's condition.
    fn tx<'a>(&'a self, conn: &'a Connection, operation: &'static str) -> Tx<'a> {
        Tx::new(conn, operation).conditioned(
            self.tables(),
            self.condition.as_ref(),
            self.connection.now_ms(),
        )
    }

    /// Read one stream head without a transaction.
    async fn read_head(&self, conn: &Connection, stream: &str) -> Result<Option<Head>> {
        let read = head_statement(self.tables(), self.scope(), stream)?;
        let sets = query(conn, "read stream", std::slice::from_ref(&read)).await?;
        head_of(sets.first().map_or(&[], Vec::as_slice))
    }

    async fn append_inner(
        &self,
        stream: &str,
        expected: Option<u64>,
        values: Vec<Value>,
    ) -> Result<u64> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        let now_ms = self.connection.now_ms();
        let mut tx = self.tx(&conn, "append");
        let outcome = self
            .append_in(&mut tx, stream, expected, &values, now_ms)
            .await;
        tx.finish(outcome).await
    }

    async fn append_in(
        &self,
        tx: &mut Tx<'_>,
        stream: &str,
        expected: Option<u64>,
        values: &[Value],
        now_ms: i64,
    ) -> Result<u64> {
        let (tables, scope) = (self.tables(), self.scope());
        let head = head_of(&tx.read_one(head_statement(tables, scope, stream)?).await?)?;
        let (base, first) = match head {
            Some(head) if head.sealed => {
                return Err(StorageError::conflict("stream is sealed"));
            }
            Some(head) => (head.base, head.len),
            None => (0, 0),
        };
        if expected.is_some_and(|offset| offset != first) {
            return Err(StorageError::conflict("stream next offset changed"));
        }
        first
            .checked_add(values.len() as u64)
            .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
        if values.is_empty() {
            return Ok(first);
        }
        let insert = format!(
            "INSERT INTO {} (scope, name, seq, body) VALUES (?1, ?2, ?3, ?4)",
            ident(tables.streams())
        );
        let mut next = first;
        for value in values {
            tx.write(statement(
                insert.clone(),
                vec![
                    SqlValue::Text(scope.to_owned()),
                    SqlValue::Text(stream.to_owned()),
                    SqlValue::Integer(to_stored(next)?),
                    SqlValue::Text(serde_json::to_string(value)?),
                ],
            )?);
            next += 1;
        }
        tx.write(statement(
            format!(
                "INSERT INTO {} (scope, name, base, len, last_append_ms) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (scope, name) DO UPDATE SET len = excluded.len,
                     last_append_ms = excluded.last_append_ms",
                ident(tables.stream_heads())
            ),
            vec![
                SqlValue::Text(scope.to_owned()),
                SqlValue::Text(stream.to_owned()),
                SqlValue::Integer(to_stored(base)?),
                SqlValue::Integer(to_stored(next)?),
                SqlValue::Integer(now_ms),
            ],
        )?);
        Ok(first)
    }

    async fn truncate_in(&self, tx: &mut Tx<'_>, stream: &str, offset: u64) -> Result<u64> {
        let (tables, scope) = (self.tables(), self.scope());
        let Some(Head {
            base, len, sealed, ..
        }) = head_of(&tx.read_one(head_statement(tables, scope, stream)?).await?)?
        else {
            return Ok(0);
        };
        if sealed {
            return Err(StorageError::conflict("stream is sealed"));
        }
        let cut = to_stored(offset.clamp(base, len))?;
        tx.write(statement(
            format!(
                "DELETE FROM {} WHERE scope = ?1 AND name = ?2 AND seq < ?3",
                ident(tables.streams())
            ),
            vec![
                SqlValue::Text(scope.to_owned()),
                SqlValue::Text(stream.to_owned()),
                SqlValue::Integer(cut),
            ],
        )?);
        tx.write(statement(
            format!(
                "UPDATE {} SET base = ?3 WHERE scope = ?1 AND name = ?2",
                ident(tables.stream_heads())
            ),
            vec![
                SqlValue::Text(scope.to_owned()),
                SqlValue::Text(stream.to_owned()),
                SqlValue::Integer(cut),
            ],
        )?);
        Ok(offset.clamp(base, len) - base)
    }

    /// Delete the stream when `sealed_ms` is `None` or its seal time; report
    /// whether a head was there to delete.
    async fn delete_in_tx(
        &self,
        tx: &mut Tx<'_>,
        stream: &str,
        sealed_ms: Option<i64>,
    ) -> Result<bool> {
        let (tables, scope) = (self.tables(), self.scope());
        let head = head_of(&tx.read_one(head_statement(tables, scope, stream)?).await?)?;
        if let Some(sealed_ms) = sealed_ms
            && head
                .as_ref()
                .is_none_or(|head| head.sealed_ms != Some(sealed_ms))
        {
            return Ok(false);
        }
        delete_in(tx, tables, scope, stream)?;
        Ok(head.is_some())
    }
}

#[async_trait]
impl StreamStore for LibsqlStreams {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none()
            .with(Capability::Streams)
            .with(Capability::ConditionalStreamAppend)
            .with(Capability::StreamSeal);
        self.condition.as_ref().map_or(capabilities, |_| {
            capabilities.with(Capability::ConditionalWrites)
        })
    }

    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.append_inner(stream, None, values).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.append_inner(stream, Some(expected_next_offset), values)
            .await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let (tables, scope) = (self.tables(), self.scope());
        let conn = self.connection.lock().await;
        // The head and the rows in one snapshot; the blob only when sealed.
        let rows = statement(
            format!(
                "SELECT seq, body FROM {} WHERE scope = ?1 AND name = ?2 AND seq >= ?3
                 ORDER BY seq LIMIT ?4",
                ident(tables.streams())
            ),
            vec![
                SqlValue::Text(scope.to_owned()),
                SqlValue::Text(stream.to_owned()),
                SqlValue::Integer(i64::try_from(from).unwrap_or(i64::MAX)),
                SqlValue::Integer(i64::try_from(limit).unwrap_or(i64::MAX)),
            ],
        )?;
        let sets = query(
            &conn,
            "read stream",
            &[head_statement(tables, scope, stream)?, rows],
        )
        .await?;
        let (Some(head), Some(rows)) = (sets.first(), sets.get(1)) else {
            return Err(StorageError::backend("libsql dropped a stream read"));
        };
        if head_of(head)?.is_some_and(|head| head.sealed) {
            return Ok(seal::read_sealed(&conn, tables, scope, stream)
                .await?
                .window(from, limit));
        }
        rows.iter()
            .map(|row| {
                Ok(StreamEntry {
                    offset: to_offset(row.integer(0)?),
                    value: serde_json::from_str(&row.text(1)?)?,
                })
            })
            .collect()
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        Ok(self
            .read_head(&conn, stream)
            .await?
            .map_or(0, |head| head.len))
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        let mut tx = self.tx(&conn, "truncate stream");
        let outcome = self.truncate_in(&mut tx, stream, offset).await;
        tx.finish(outcome).await
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        let mut tx = self.tx(&conn, "delete stream");
        let outcome = self.delete_in_tx(&mut tx, stream, None).await;
        tx.finish(outcome).await
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        // The check and the delete share the write transaction, so the stream
        // cannot be replaced between them.
        let mut tx = self.tx(&conn, "delete sealed stream");
        let outcome = self.delete_in_tx(&mut tx, stream, Some(sealed_ms)).await;
        tx.finish(outcome).await
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.seal_stream(stream).await
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        Ok(self
            .read_head(&conn, stream)
            .await?
            .map(|head| StreamInfo::new(head.base, head.len, head.sealed_ms, head.last_append_ms)))
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let read = statement(
            format!(
                "SELECT name FROM {} WHERE scope = ?1 ORDER BY name",
                ident(self.tables().stream_heads())
            ),
            [self.scope()],
        )?;
        let conn = self.connection.lock().await;
        let sets = query(&conn, "list streams", std::slice::from_ref(&read)).await?;
        let mut names = Vec::new();
        for row in sets.first().map_or(&[][..], Vec::as_slice) {
            let name = row.text(0)?;
            if name.starts_with(prefix) {
                names.push(name);
            }
        }
        Ok(names)
    }
}
