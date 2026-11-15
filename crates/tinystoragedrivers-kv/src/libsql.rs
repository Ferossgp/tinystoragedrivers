//! libSQL KV adapter over the shared database connection, with the SQLite
//! engine's table and rules. A write reads the record in the request that
//! opens its transaction and sends the new row with the commit.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_connect::LibsqlConnection;
use tinystoragedrivers_connect::libsql::error::during;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_connect::libsql::{Row, Statement, Tx, query};
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Meta, Page, Precondition, Result, Scope, StorageError,
    Version, Versioned, WriteCondition,
};

use crate::common::{
    check_precondition, cursor_key, duration_millis, from_sqlite_version, next_version,
    validate_key, validate_limit, validate_prefix, version_to_i64,
};

#[derive(Debug)]
struct Record {
    version: Version,
    expires_ms: Option<i64>,
    value: Option<Value>,
}

impl Record {
    fn live(&self, now: i64) -> bool {
        self.value.is_some() && self.expires_ms.is_none_or(|expires_ms| expires_ms > now)
    }
}

/// libSQL key/value engine sharing a connection with the other engines. The
/// connection creates its table.
#[derive(Debug, Clone)]
pub struct LibsqlKv {
    connection: LibsqlConnection,
    scope: String,
    condition: Option<WriteCondition>,
}

impl LibsqlKv {
    /// Bind this adapter to `scope` on `connection`.
    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the same shape as the other engines' constructors"
    )]
    pub fn new(connection: LibsqlConnection, scope: Scope) -> Self {
        Self {
            connection,
            scope: scope.as_str().to_owned(),
            condition: None,
        }
    }

    /// Bind this KV handle to a host condition checked in the same write
    /// transaction as each mutation.
    #[must_use]
    pub fn conditioned(
        connection: LibsqlConnection,
        scope: Scope,
        condition: WriteCondition,
    ) -> Self {
        let mut kv = Self::new(connection, scope);
        kv.condition = Some(condition);
        kv
    }

    fn table(&self) -> String {
        format!("\"{}\"", self.connection.tables().kv())
    }

    fn read_record(&self, key: &str) -> Result<Statement> {
        Statement::new(
            format!(
                "SELECT version, expires_ms, deleted, value FROM {} WHERE scope=?1 AND key=?2",
                self.table()
            ),
            [self.scope.as_str(), key],
        )
        .map_err(during("read a KV value"))
    }

    /// The record of a write transaction's key, read in its first request.
    async fn record_in(&self, tx: &mut Tx<'_>, key: &str) -> Result<Option<Record>> {
        let rows = tx.read_one(self.read_record(key)?).await?;
        rows.first().map(record).transpose()
    }

    async fn write(
        &self,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<i64>,
    ) -> Result<Version> {
        validate_key(key)?;
        let encoded = serde_json::to_string(&value)?;
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let mut tx = Tx::new(&conn, "write a KV value").conditioned(
            self.connection.tables(),
            self.condition.as_ref(),
            now,
        );
        let outcome = async {
            let old = self.record_in(&mut tx, key).await?;
            let current = old
                .as_ref()
                .filter(|record| record.live(now))
                .map(|record| record.version);
            check_precondition(expected, current)?;
            let version = version_to_i64(next_version(old.map(|record| record.version))?)?;
            tx.write_sql(
                format!(
                    "INSERT INTO {}(scope,key,version,expires_ms,deleted,value) VALUES (?1,?2,?3,?4,0,?5) ON CONFLICT(scope,key) DO UPDATE SET version=excluded.version, expires_ms=excluded.expires_ms, deleted=0, value=excluded.value",
                    self.table()
                ),
                vec![
                    SqlValue::Text(self.scope.clone()),
                    SqlValue::Text(key.to_owned()),
                    SqlValue::Integer(version),
                    expires_ms.map_or(SqlValue::Null, SqlValue::Integer),
                    SqlValue::Text(encoded),
                ],
            )?;
            Ok(Version(u64::try_from(version).map_err(|_| {
                StorageError::backend("key version space is exhausted")
            })?))
        }
        .await;
        tx.finish(outcome).await
    }
}

/// A record from its `version, expires_ms, deleted, value` columns.
fn record(row: &Row) -> Result<Record> {
    let version = from_sqlite_version(row.integer(0)?)?;
    let value = if row.integer(2)? != 0 {
        None
    } else {
        row.opt_text(3)?
            .map(|value| serde_json::from_str(&value))
            .transpose()?
    };
    Ok(Record {
        version,
        expires_ms: row.opt_integer(1)?,
        value,
    })
}

#[async_trait]
impl KvStore for LibsqlKv {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none()
            .with(Capability::Kv)
            .with(Capability::Ttl)
            .with(Capability::SharedAcrossProcesses)
            .with(Capability::CrossProcessCas);
        if self.condition.is_some() {
            capabilities.with(Capability::ConditionalWrites)
        } else {
            capabilities
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        validate_key(key)?;
        let read = self.read_record(key)?;
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let sets = query(&conn, "read a KV value", std::slice::from_ref(&read)).await?;
        let record = sets
            .first()
            .and_then(|rows| rows.first())
            .map(record)
            .transpose()?;
        Ok(record.filter(|record| record.live(now)).and_then(|record| {
            record.value.map(|value| Versioned {
                id: key.to_owned(),
                version: record.version,
                doc: value,
                meta: Meta::unknown(),
            })
        }))
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.write(key, value, Precondition::None, None).await
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.write(key, value, expected, None).await
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        validate_key(key)?;
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let mut tx = Tx::new(&conn, "delete a KV value").conditioned(
            self.connection.tables(),
            self.condition.as_ref(),
            now,
        );
        let outcome = async {
            let old = self.record_in(&mut tx, key).await?;
            let current = old
                .as_ref()
                .filter(|record| record.live(now))
                .map(|record| record.version);
            check_precondition(expected, current)?;
            let Some(current) = current else {
                return Ok(false);
            };
            let version = version_to_i64(next_version(Some(current))?)?;
            tx.write_sql(
                format!(
                    "UPDATE {} SET version=?3, expires_ms=NULL, deleted=1, value=NULL WHERE scope=?1 AND key=?2",
                    self.table()
                ),
                vec![
                    SqlValue::Text(self.scope.clone()),
                    SqlValue::Text(key.to_owned()),
                    SqlValue::Integer(version),
                ],
            )?;
            Ok(true)
        }
        .await;
        tx.finish(outcome).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        let after = cursor
            .as_ref()
            .map(|cursor| cursor_key(cursor, prefix))
            .transpose()?;
        let read = Statement::new(
            format!(
                "SELECT key, version, expires_ms, deleted, value FROM {} WHERE scope=?1 AND key>=?2 ORDER BY key",
                self.table()
            ),
            [self.scope.as_str(), prefix],
        )
        .map_err(during("list KV values"))?;
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let sets = query(&conn, "list KV values", std::slice::from_ref(&read)).await?;
        drop(conn);
        let mut found = Vec::new();
        for row in sets.first().map_or(&[][..], Vec::as_slice) {
            let key = row.text(0)?;
            let expires_ms = row.opt_integer(2)?;
            if !key.starts_with(prefix) {
                // Keys sort bytewise, so no later key has the prefix.
                break;
            }
            if after.as_ref().is_some_and(|last| key <= *last)
                || row.integer(3)? != 0
                || expires_ms.is_some_and(|expires| expires <= now)
            {
                continue;
            }
            let value = row
                .opt_text(4)?
                .ok_or_else(|| StorageError::backend("libsql KV value is missing"))?;
            found.push(Versioned {
                id: key,
                version: from_sqlite_version(row.integer(1)?)?,
                doc: serde_json::from_str(&value)?,
                meta: Meta::unknown(),
            });
            if found.len() > limit {
                break;
            }
        }
        let has_more = found.len() > limit;
        if has_more {
            found.pop();
        }
        let next = if has_more {
            found
                .last()
                .map(|item| crate::common::make_cursor(&item.id, prefix))
                .transpose()?
        } else {
            None
        };
        Ok(Page { items: found, next })
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        let now = self.connection.now_ms();
        let ttl = i64::try_from(duration_millis(ttl)).unwrap_or(i64::MAX);
        self.write(
            key,
            value,
            Precondition::None,
            Some(now.saturating_add(ttl)),
        )
        .await
    }
}
