//! How a document body sits in the `docs.body` column; the encoding of the
//! SQLite engine. A body is the document's compact JSON, stored as `TEXT`
//! up to [`COMPRESS_OVER`] bytes and as a `BLOB` of one marker byte plus a
//! zstd frame above, unless compression would not shrink it.

use serde_json::Value;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_types::{Result, StorageError};

/// Serialized JSON longer than this many bytes is compressed.
const COMPRESS_OVER: usize = 4096;

/// zstd's default level: a good ratio at document-write speeds.
const LEVEL: i32 = 3;

/// The first byte of a compressed body.
const MARKER: u8 = 1;

/// The column value for `doc`: `TEXT` for a small body, `BLOB` for a large one.
pub(crate) fn encode(doc: &Value) -> Result<SqlValue> {
    let text = serde_json::to_string(doc)?;
    if text.len() <= COMPRESS_OVER {
        return Ok(SqlValue::Text(text));
    }
    let frame = zstd::encode_all(text.as_bytes(), LEVEL)
        .map_err(|error| StorageError::backend("compress document body").with_source(error))?;
    if frame.len() >= text.len() {
        return Ok(SqlValue::Text(text));
    }
    let mut blob = Vec::with_capacity(frame.len() + 1);
    blob.push(MARKER);
    blob.extend_from_slice(&frame);
    Ok(SqlValue::Blob(blob))
}

/// The document held by a `docs.body` value.
pub(crate) fn decode(stored: &SqlValue) -> Result<Value> {
    match stored {
        SqlValue::Text(text) => Ok(serde_json::from_str(text)?),
        SqlValue::Blob(bytes) => match bytes.split_first() {
            Some((&MARKER, frame)) => {
                let json = zstd::decode_all(frame).map_err(|error| {
                    StorageError::backend("corrupt libsql storage: bad compressed body")
                        .with_source(error)
                })?;
                Ok(serde_json::from_slice(&json)?)
            }
            _ => Err(StorageError::backend(
                "corrupt libsql storage: unknown body encoding",
            )),
        },
        _ => Err(StorageError::backend(
            "corrupt libsql storage: body is neither text nor a blob",
        )),
    }
}
