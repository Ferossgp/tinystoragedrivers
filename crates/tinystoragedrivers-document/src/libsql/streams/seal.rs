//! Sealing a libSQL stream, as the SQLite engine does: write the compressed
//! entries to the blob table, mark the head sealed, and delete the rows the
//! blob replaces, all in one immediate transaction, so an append cannot slip
//! between the snapshot and the mark. The head, the rows and the existing
//! blob's size are read in the request that opens the transaction; the three
//! writes go with the commit. On a stream that is already sealed, sealing only
//! deletes leftover rows.

use tinystoragedrivers_connect::libsql::libsql::{Connection, Value as SqlValue};
use tinystoragedrivers_connect::libsql::{Tx, query};
use tinystoragedrivers_types::{Result, SealOutcome, StorageError};

use super::{LibsqlStreams, head_of, head_statement, to_offset};
use crate::libsql::{SqliteTables, ident, statement};
use crate::stream::SealedLog;
use crate::{sealed_blob_key, validate_stream};

/// The blob's `content_type`.
const CONTENT_TYPE: &str = "application/zstd";

fn missing_blob() -> StorageError {
    StorageError::backend("sealed stream has no blob")
}

/// The entries of a sealed stream, decoded from its blob.
pub(super) async fn read_sealed(
    conn: &Connection,
    tables: &SqliteTables,
    scope: &str,
    stream: &str,
) -> Result<SealedLog> {
    let read = statement(
        format!(
            "SELECT bytes FROM {} WHERE scope = ?1 AND key = ?2",
            ident(tables.blobs())
        ),
        [scope, sealed_blob_key(stream)?.as_str()],
    )?;
    let sets = query(conn, "read sealed stream", std::slice::from_ref(&read)).await?;
    let bytes = sets
        .first()
        .and_then(|rows| rows.first())
        .ok_or_else(missing_blob)?
        .blob(0)?;
    SealedLog::decode(&bytes)
}

/// Queue the removal of the sealed blob, if there is one.
pub(super) fn delete_blob(
    tx: &mut Tx<'_>,
    tables: &SqliteTables,
    scope: &str,
    stream: &str,
) -> Result<()> {
    tx.write(statement(
        format!(
            "DELETE FROM {} WHERE scope = ?1 AND key = ?2",
            ident(tables.blobs())
        ),
        [scope, sealed_blob_key(stream)?.as_str()],
    )?);
    Ok(())
}

impl LibsqlStreams {
    /// Seal `stream`; see [`StreamStore::seal`](tinystoragedrivers_api::StreamStore::seal).
    pub(super) async fn seal_stream(&self, stream: &str) -> Result<SealOutcome> {
        validate_stream(stream)?;
        let conn = self.connection.lock().await;
        let now_ms = self.connection.now_ms();
        let mut tx = self.tx(&conn, "seal stream");
        let outcome = self.seal_in(&mut tx, stream, now_ms).await;
        tx.finish(outcome).await
    }

    async fn seal_in(&self, tx: &mut Tx<'_>, stream: &str, now_ms: i64) -> Result<SealOutcome> {
        let (tables, scope) = (self.tables(), self.scope());
        let key = sealed_blob_key(stream)?;
        let rows = statement(
            format!(
                "SELECT seq, body FROM {} WHERE scope = ?1 AND name = ?2 ORDER BY seq",
                ident(tables.streams())
            ),
            [scope, stream],
        )?;
        let blob_len = statement(
            format!(
                "SELECT length(bytes) FROM {} WHERE scope = ?1 AND key = ?2",
                ident(tables.blobs())
            ),
            [scope, key.as_str()],
        )?;
        let sets = tx
            .read(&[head_statement(tables, scope, stream)?, rows, blob_len])
            .await?;
        let [head, rows, blob_len] = <[_; 3]>::try_from(sets)
            .map_err(|_| StorageError::backend("libsql dropped a seal read"))?;
        let head =
            head_of(&head)?.ok_or_else(|| StorageError::not_found("stream does not exist"))?;
        let delete_rows = statement(
            format!(
                "DELETE FROM {} WHERE scope = ?1 AND name = ?2",
                ident(tables.streams())
            ),
            [scope, stream],
        )?;
        if head.sealed {
            let bytes = blob_len
                .first()
                .ok_or_else(missing_blob)
                .and_then(|row| row.integer(0))?;
            tx.write(delete_rows);
            return Ok(SealOutcome::new(head.base, head.len, to_offset(bytes)));
        }
        let entries: Vec<(i64, String)> = rows
            .iter()
            .map(|row| Ok((row.integer(0)?, row.text(1)?)))
            .collect::<Result<_>>()?;
        let dense = entries.len() as u64 == head.len - head.base
            && entries
                .iter()
                .zip(head.base..)
                .all(|((seq, _), expected)| to_offset(*seq) == expected);
        if !dense {
            return Err(StorageError::backend(
                "stream rows do not match the stream head; refusing to seal",
            ));
        }
        let values = entries
            .iter()
            .map(|(_, value)| serde_json::from_str(value))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let log = SealedLog::new(head.base, values)?;
        let bytes = log.encode()?;
        let outcome = SealOutcome::new(log.base(), log.len(), bytes.len() as u64);
        tx.write(statement(
            format!(
                "INSERT INTO {} (scope, key, content_type, size, sha256, bytes, expires_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)
                 ON CONFLICT (scope, key) DO UPDATE SET content_type = excluded.content_type, size = excluded.size,
                     sha256 = excluded.sha256, bytes = excluded.bytes, expires_ms = NULL",
                ident(tables.blobs())
            ),
            vec![
                SqlValue::Text(scope.to_owned()),
                SqlValue::Text(key),
                SqlValue::Text(CONTENT_TYPE.to_owned()),
                SqlValue::Integer(i64::try_from(bytes.len()).unwrap_or(i64::MAX)),
                SqlValue::Blob(digest(&bytes)),
                SqlValue::Blob(bytes),
            ],
        )?);
        tx.write(statement(
            format!(
                "UPDATE {} SET sealed = 1, sealed_ms = ?3 WHERE scope = ?1 AND name = ?2",
                ident(tables.stream_heads())
            ),
            vec![
                SqlValue::Text(scope.to_owned()),
                SqlValue::Text(stream.to_owned()),
                SqlValue::Integer(now_ms),
            ],
        )?);
        tx.write(delete_rows);
        Ok(outcome)
    }
}

fn digest(bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).to_vec()
}
