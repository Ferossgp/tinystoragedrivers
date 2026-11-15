//! libSQL blob adapter over the shared connection, with the SQLite engine's
//! tables and rules. A plain put is one transactional batch; a delete or a
//! sweep reads in the request that opens its transaction and deletes with
//! the commit.

use std::{ops::Range, time::Duration};

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_connect::LibsqlConnection;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_connect::libsql::{Row, Statement, Tx, query};
use tinystoragedrivers_types::{
    Blob, BlobListEntry, BlobListPage, BlobMeta, Capabilities, Capability,
    MAX_BLOB_LIST_PAGE_ITEMS, Result, Scope, StorageError, WriteCondition,
};

use crate::key::{MAX_KEY_LEN, clamp_range, validate_key, validate_prefix, validate_sealed_key};

/// A scope-bound blob handle over a shared libSQL database.
#[derive(Debug, Clone)]
pub struct LibsqlBlobStore {
    connection: LibsqlConnection,
    scope: Scope,
    condition: Option<WriteCondition>,
}

impl LibsqlBlobStore {
    /// Bind a shared database connection to one scope.
    #[must_use]
    pub fn new(connection: LibsqlConnection, scope: Scope) -> Self {
        Self {
            connection,
            scope,
            condition: None,
        }
    }

    /// Bind a blob adapter whose mutations check `condition` in their write
    /// transaction.
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

    fn blobs(&self) -> String {
        format!("\"{}\"", self.connection.tables().blobs())
    }

    fn written(&self) -> String {
        format!("\"{}\"", self.connection.tables().blob_written())
    }

    fn scope_value(&self) -> SqlValue {
        SqlValue::Text(self.scope.as_str().to_owned())
    }

    /// Run `read` on its own and return its rows.
    async fn read(&self, read: Statement) -> Result<Vec<Row>> {
        let conn = self.connection.lock().await;
        Ok(query(&conn, "read blobs", std::slice::from_ref(&read))
            .await?
            .pop()
            .unwrap_or_default())
    }

    /// The blob at `key` with its bytes, unless absent or expired.
    async fn fetch(&self, key: &str) -> Result<Option<Blob>> {
        let read = statement(
            format!(
                "SELECT content_type, size, expires_ms, bytes FROM {} WHERE scope=?1 AND key=?2",
                self.blobs()
            ),
            vec![self.scope_value(), SqlValue::Text(key.to_owned())],
        )?;
        let now = self.connection.now_ms();
        let Some(row) = self.read(read).await?.into_iter().next() else {
            return Ok(None);
        };
        if row.opt_integer(2)?.is_some_and(|expires| expires <= now) {
            return Ok(None);
        }
        let bytes = row.blob(3)?;
        Ok(Some(Blob {
            meta: BlobMeta {
                key: key.to_owned(),
                len: u64::try_from(row.integer(1)?).unwrap_or(bytes.len() as u64),
                content_type: row.opt_text(0)?,
            },
            bytes,
        }))
    }

    /// The metadata of the blob at `key`, unless absent or expired.
    async fn fetch_meta(&self, key: &str) -> Result<Option<BlobMeta>> {
        let read = statement(
            format!(
                "SELECT content_type, size, expires_ms FROM {} WHERE scope=?1 AND key=?2",
                self.blobs()
            ),
            vec![self.scope_value(), SqlValue::Text(key.to_owned())],
        )?;
        let now = self.connection.now_ms();
        let Some(row) = self.read(read).await?.into_iter().next() else {
            return Ok(None);
        };
        if row.opt_integer(2)?.is_some_and(|expires| expires <= now) {
            return Ok(None);
        }
        Ok(Some(BlobMeta {
            key: key.to_owned(),
            len: u64::try_from(row.integer(1)?).unwrap_or(0),
            content_type: row.opt_text(0)?,
        }))
    }

    /// Delete the blob at `key`; whether a live one was there.
    async fn remove(&self, key: &str) -> Result<bool> {
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let mut tx = Tx::new(&conn, "delete a blob").conditioned(
            self.connection.tables(),
            self.condition.as_ref(),
            now,
        );
        let outcome = async {
            let key = SqlValue::Text(key.to_owned());
            let rows = tx
                .read_one(statement(
                    format!(
                        "SELECT expires_ms FROM {} WHERE scope=?1 AND key=?2",
                        self.blobs()
                    ),
                    vec![self.scope_value(), key.clone()],
                )?)
                .await?;
            let expires = rows.first().map(|row| row.opt_integer(0)).transpose()?;
            for table in [self.blobs(), self.written()] {
                tx.write(statement(
                    format!("DELETE FROM {table} WHERE scope=?1 AND key=?2"),
                    vec![self.scope_value(), key.clone()],
                )?);
            }
            Ok(expires.is_some_and(|expires| expires.is_none_or(|expires| expires > now)))
        }
        .await;
        tx.finish(outcome).await
    }

    /// The live blobs under `prefix`, in key order; sealed stream blobs only
    /// when `sealed`.
    async fn listing(&self, prefix: &str, sealed: bool) -> Result<Vec<BlobMeta>> {
        let hide_sealed = if sealed {
            ""
        } else {
            " AND substr(key,1,12)!='_tsd/sealed/'"
        };
        let read = statement(
            format!(
                "SELECT key, content_type, size FROM {} WHERE scope=?1 AND substr(key,1,length(?2))=?2{hide_sealed} AND (expires_ms IS NULL OR expires_ms>?3) ORDER BY key COLLATE BINARY",
                self.blobs()
            ),
            vec![
                self.scope_value(),
                SqlValue::Text(prefix.to_owned()),
                SqlValue::Integer(self.connection.now_ms()),
            ],
        )?;
        self.read(read)
            .await?
            .iter()
            .map(|row| {
                Ok(BlobMeta {
                    key: row.text(0)?,
                    content_type: row.opt_text(1)?,
                    len: u64::try_from(row.integer(2)?).unwrap_or(0),
                })
            })
            .collect()
    }

    async fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
        allow_sealed: bool,
    ) -> Result<BlobMeta> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let hash = Sha256::digest(&bytes).to_vec();
        let len = bytes.len() as u64;
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let expires =
            ttl.map(|ttl| now.saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)));
        let mut tx = Tx::new(&conn, "write a blob").conditioned(
            self.connection.tables(),
            self.condition.as_ref(),
            now,
        );
        let outcome = (|| {
            tx.write(statement(
                format!(
                    "INSERT OR REPLACE INTO {} (scope,key,content_type,size,sha256,expires_ms,bytes) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                    self.blobs()
                ),
                vec![
                    self.scope_value(),
                    SqlValue::Text(key.to_owned()),
                    content_type.map_or(SqlValue::Null, |kind| SqlValue::Text(kind.to_owned())),
                    SqlValue::Integer(i64::try_from(len).unwrap_or(i64::MAX)),
                    SqlValue::Blob(hash),
                    expires.map_or(SqlValue::Null, SqlValue::Integer),
                    SqlValue::Blob(bytes),
                ],
            )?);
            tx.write(statement(
                format!(
                    "INSERT OR REPLACE INTO {} (scope,key,written_ms) VALUES (?1,?2,?3)",
                    self.written()
                ),
                vec![
                    self.scope_value(),
                    SqlValue::Text(key.to_owned()),
                    SqlValue::Integer(now),
                ],
            )?);
            Ok(BlobMeta {
                key: key.to_owned(),
                len,
                content_type: content_type.map(str::to_owned),
            })
        })();
        tx.finish(outcome).await
    }
}

fn statement(sql: String, params: Vec<SqlValue>) -> Result<Statement> {
    Statement::new(sql, params).map_err(sql_error)
}

fn sql_error(error: tinystoragedrivers_connect::libsql::libsql::Error) -> StorageError {
    StorageError::backend("libSQL blob operation failed").with_source(error)
}

#[async_trait]
impl BlobStore for LibsqlBlobStore {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none()
            .with(Capability::Blobs)
            .with(Capability::BlobTtl)
            .with(Capability::BlobListing);
        self.condition.as_ref().map_or(capabilities, |_| {
            capabilities.with(Capability::ConditionalWrites)
        })
    }
    fn max_key_len(&self) -> usize {
        MAX_KEY_LEN
    }
    fn supports_sealed_blobs(&self) -> bool {
        true
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, false).await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl), false).await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_key(key)?;
        self.fetch(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        clamp_range(0, range.start, range.end)?;
        let Some(blob) = self.get(key).await? else {
            return Ok(None);
        };
        Ok(Some(
            blob.bytes[clamp_range(blob.bytes.len(), range.start, range.end)?].to_vec(),
        ))
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_key(key)?;
        self.fetch_meta(key).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        validate_key(key)?;
        self.remove(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_prefix(prefix, None)?;
        self.listing(prefix, false).await
    }

    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, true).await
    }
    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        validate_sealed_key(key)?;
        self.fetch(key).await
    }
    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_sealed_key(key)?;
        self.fetch_meta(key).await
    }
    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        validate_sealed_key(key)?;
        self.remove(key).await
    }
    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_sealed_key(prefix)?;
        self.listing(prefix, true).await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS {
            return Err(StorageError::invalid_input(
                "invalid blob listing page size",
            ));
        }
        validate_prefix(prefix, after)?;
        let read = statement(
            format!(
                "SELECT b.key,b.content_type,b.size,w.written_ms FROM {} b LEFT JOIN {} w ON w.scope=b.scope AND w.key=b.key WHERE b.scope=?1 AND substr(b.key,1,length(?2))=?2 AND substr(b.key,1,12)!='_tsd/sealed/' AND (?3 IS NULL OR b.key COLLATE BINARY > ?3 COLLATE BINARY) ORDER BY b.key COLLATE BINARY LIMIT ?4",
                self.blobs(),
                self.written()
            ),
            vec![
                self.scope_value(),
                SqlValue::Text(prefix.to_owned()),
                after.map_or(SqlValue::Null, |after| SqlValue::Text(after.to_owned())),
                SqlValue::Integer(i64::try_from(limit + 1).unwrap_or(i64::MAX)),
            ],
        )?;
        let mut items = self
            .read(read)
            .await?
            .iter()
            .map(|row| {
                Ok(BlobListEntry {
                    meta: BlobMeta {
                        key: row.text(0)?,
                        content_type: row.opt_text(1)?,
                        len: u64::try_from(row.integer(2)?).unwrap_or(0),
                    },
                    written_at_ms: row.opt_integer(3)?.and_then(|age| u64::try_from(age).ok()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let more = items.len() > limit;
        items.truncate(limit);
        let next_after = if more {
            items.last().map(|item| item.meta.key.clone())
        } else {
            None
        };
        Ok(BlobListPage { items, next_after })
    }
    async fn sweep_expired(&self) -> Result<u64> {
        let conn = self.connection.lock().await;
        let now = self.connection.now_ms();
        let mut tx = Tx::new(&conn, "sweep expired blobs").conditioned(
            self.connection.tables(),
            self.condition.as_ref(),
            now,
        );
        let outcome = async {
            let lapsed = vec![self.scope_value(), SqlValue::Integer(now)];
            let rows = tx
                .read_one(statement(
                    format!(
                        "SELECT key FROM {} WHERE scope=?1 AND expires_ms IS NOT NULL AND expires_ms<=?2",
                        self.blobs()
                    ),
                    lapsed.clone(),
                )?)
                .await?;
            tx.write(statement(
                format!(
                    "DELETE FROM {} WHERE scope=?1 AND expires_ms IS NOT NULL AND expires_ms<=?2",
                    self.blobs()
                ),
                lapsed,
            )?);
            for row in &rows {
                tx.write(statement(
                    format!("DELETE FROM {} WHERE scope=?1 AND key=?2", self.written()),
                    vec![self.scope_value(), SqlValue::Text(row.text(0)?)],
                )?);
            }
            Ok(rows.len() as u64)
        }
        .await;
        tx.finish(outcome).await
    }
}
