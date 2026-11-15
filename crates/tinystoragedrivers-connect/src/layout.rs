//! The layout-2 table set shared by the SQLite and libSQL handles: names,
//! `CREATE` statements and the write-condition rule.
//!
//! # Layout 2
//!
//! One database holds these tables, each name preceded by the handle's table
//! prefix (empty by default):
//!
//! ```text
//! docs(scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
//! doc_fields(scope, coll, id, path, kind, value)    declared fields
//! streams(scope, name, seq, body)            stream_heads(scope, name, base, len, sealed)
//! blobs(scope, key, content_type, size, sha256, expires_ms, bytes)
//! fts                                         FTS5, searchable collections only
//! meta(key, value)                            `layout` = 2
//! _tsd_collections, _tsd_scope_admissions, _tsd_purge_receipts, _tsd_blob_written
//! _tsd_kv                                     created by the key/value engine
//! ```
//!
//! A removed document keeps its row with `deleted = 1` and the body `{}`, so
//! its version continues if the id is written again. Every read skips deleted
//! rows.
//!
//! `docs.body` holds JSON text, or a compressed `BLOB` for a body over 4 KiB;
//! the declared index and search fields live in `doc_fields`, which is what
//! indexes and SQL filters read.
//!
//! Every name that reaches SQL text is either validated by the core (collection
//! names are `[A-Za-z0-9_.-]`, table prefixes `[A-Za-z0-9_]`) or quoted here;
//! values are bound as parameters.

use serde_json::Value;
use tinystoragedrivers_types::{
    CollectionSpec, Meta, Result, StorageError, WriteConditionClause, value,
};

/// Quote an SQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The `meta.layout` value this driver writes and understands.
pub(crate) const LAYOUT: &str = "2";

/// The longest accepted table prefix, in bytes.
const MAX_PREFIX_LEN: usize = 64;

/// The table names of the database.
#[derive(Debug, Clone)]
pub struct SqliteTables {
    pub(crate) docs: String,
    pub(crate) fields: String,
    pub(crate) admissions: String,
    pub(crate) purge_receipts: String,
    pub(crate) specs: String,
    pub(crate) fts: String,
    pub(crate) streams: String,
    pub(crate) stream_heads: String,
    pub(crate) blobs: String,
    pub(crate) meta: String,
    pub(crate) blob_written: String,
    pub(crate) kv: String,
}

impl SqliteTables {
    /// The unprefixed table names.
    #[cfg(all(test, feature = "sqlite"))]
    pub(crate) fn new() -> Self {
        Self::named("")
    }

    /// The table names, each preceded by `prefix`, so the driver's tables can
    /// share a database with tables it does not own.
    ///
    /// # Errors
    /// Returns invalid input unless `prefix` is at most 64 ASCII letters,
    /// digits and underscores.
    pub(crate) fn with_prefix(prefix: &str) -> Result<Self> {
        let valid = prefix.len() <= MAX_PREFIX_LEN
            && prefix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
        if !valid {
            return Err(StorageError::invalid_input(
                "table prefix must be at most 64 ASCII letters, digits and underscores",
            ));
        }
        Ok(Self::named(prefix))
    }

    fn named(prefix: &str) -> Self {
        let internal = |table: &str| format!("{prefix}_tsd_{table}");
        let plain = |table: &str| format!("{prefix}{table}");
        Self {
            docs: plain("docs"),
            fields: plain("doc_fields"),
            admissions: internal("scope_admissions"),
            purge_receipts: internal("purge_receipts"),
            specs: internal("collections"),
            fts: plain("fts"),
            streams: plain("streams"),
            stream_heads: plain("stream_heads"),
            blobs: plain("blobs"),
            meta: plain("meta"),
            blob_written: internal("blob_written"),
            kv: internal("kv"),
        }
    }

    /// Every table with its `CREATE` statement, in creation order.
    pub(crate) fn declarations(&self) -> Vec<(&str, String)> {
        let mut all = self.document_declarations();
        all.extend(self.stream_declarations());
        all.extend(self.metadata_declarations());
        all
    }

    /// The internal, document and search tables.
    fn document_declarations(&self) -> Vec<(&str, String)> {
        let (docs, fields, admissions, purge_receipts, specs, fts) = (
            ident(&self.docs),
            ident(&self.fields),
            ident(&self.admissions),
            ident(&self.purge_receipts),
            ident(&self.specs),
            ident(&self.fts),
        );
        vec![
            (
                &self.admissions,
                format!(
                    "CREATE TABLE IF NOT EXISTS {admissions} (
                 scope TEXT PRIMARY KEY,
                 generation INTEGER NOT NULL,
                 operation TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.purge_receipts,
                format!(
                    "CREATE TABLE IF NOT EXISTS {purge_receipts} (
                 scope TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 PRIMARY KEY (scope, operation)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.docs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {docs} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 schema INTEGER NOT NULL DEFAULT 0,
                 created_ms INTEGER NOT NULL,
                 updated_ms INTEGER NOT NULL,
                 expires_ms INTEGER,
                 deleted INTEGER NOT NULL DEFAULT 0,
                 body TEXT NOT NULL,
                 PRIMARY KEY (scope, coll, id)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.fields,
                format!(
                    "CREATE TABLE IF NOT EXISTS {fields} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 path TEXT NOT NULL,
                 kind INTEGER NOT NULL,
                 value,
                 PRIMARY KEY (scope, coll, id, path)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.specs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {specs} (
                 coll TEXT PRIMARY KEY,
                 spec TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.fts,
                format!(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS {fts} USING fts5(
                 scope UNINDEXED, coll UNINDEXED, id UNINDEXED, body,
                 tokenize = 'unicode61'
             )"
                ),
            ),
        ]
    }

    /// The stream, blob and meta tables.
    fn stream_declarations(&self) -> Vec<(&str, String)> {
        let (streams, heads, blobs, meta) = (
            ident(&self.streams),
            ident(&self.stream_heads),
            ident(&self.blobs),
            ident(&self.meta),
        );
        vec![
            (
                &self.streams,
                format!(
                    "CREATE TABLE IF NOT EXISTS {streams} (
                 scope TEXT NOT NULL,
                 name TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 body TEXT NOT NULL,
                 PRIMARY KEY (scope, name, seq)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.stream_heads,
                format!(
                    "CREATE TABLE IF NOT EXISTS {heads} (
                 scope TEXT NOT NULL,
                 name TEXT NOT NULL,
                 base INTEGER NOT NULL,
                 len INTEGER NOT NULL,
                 sealed INTEGER NOT NULL DEFAULT 0,
                 sealed_ms INTEGER,
                 last_append_ms INTEGER,
                 PRIMARY KEY (scope, name)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.blobs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {blobs} (
                 scope TEXT NOT NULL,
                 key TEXT NOT NULL,
                 content_type TEXT,
                 size INTEGER NOT NULL,
                 sha256 BLOB NOT NULL,
                 expires_ms INTEGER,
                 bytes BLOB NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.meta,
                format!(
                    "CREATE TABLE IF NOT EXISTS {meta} (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
        ]
    }

    /// The optional write-time metadata for blobs added after the v1 layout.
    fn metadata_declarations(&self) -> Vec<(&str, String)> {
        let written = ident(&self.blob_written);
        vec![(
            &self.blob_written,
            format!(
                "CREATE TABLE IF NOT EXISTS {written} (
                 scope TEXT NOT NULL,
                 key TEXT NOT NULL,
                 written_ms INTEGER NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID"
            ),
        )]
    }

    /// The key/value engine's table.
    #[must_use]
    pub fn kv_declaration(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (
                    scope TEXT NOT NULL,
                    key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    expires_ms INTEGER,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    value TEXT,
                    PRIMARY KEY (scope, key)
                ) WITHOUT ROWID",
            ident(&self.kv)
        )
    }

    /// Indexes that live beside the tables rather than in a collection spec.
    pub(crate) fn index_declarations(&self) -> [String; 2] {
        [
            // Lets a lifetime write find lapsed blobs without reading every one.
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} (expires_ms) WHERE expires_ms IS NOT NULL",
                ident(&format!("{}_expiry", self.blobs)),
                ident(&self.blobs)
            ),
            // Serves equality, membership and keyset seeks on a projected field.
            format!(
                "CREATE INDEX IF NOT EXISTS {} ON {} (scope, coll, path, value, id)",
                ident(&format!("{}_lookup", self.fields)),
                ident(&self.fields)
            ),
        ]
    }

    /// The statement recording the layout marker, bound to [`LAYOUT`].
    pub(crate) fn layout_marker(&self) -> String {
        format!(
            "INSERT OR IGNORE INTO {} (key, value) VALUES ('layout', ?1)",
            ident(&self.meta)
        )
    }

    /// The statement reading the layout marker.
    pub(crate) fn layout_query(&self) -> String {
        format!(
            "SELECT value FROM {} WHERE key = 'layout'",
            ident(&self.meta)
        )
    }

    /// The statement reading one guarded document of a write condition,
    /// bound to its scope, collection and id.
    pub(crate) fn condition_query(&self) -> String {
        format!(
            "SELECT schema, created_ms, updated_ms, expires_ms, body FROM {} WHERE scope=?1 AND coll=?2 AND id=?3 AND deleted=0",
            ident(&self.docs)
        )
    }

    /// The statement reading one collection declaration, bound to its name.
    pub(crate) fn spec_query(&self) -> String {
        format!("SELECT spec FROM {} WHERE coll=?1", ident(&self.specs))
    }

    /// Name of the document table.
    #[must_use]
    pub fn docs(&self) -> &str {
        &self.docs
    }
    /// Name of the document field projection table.
    #[must_use]
    pub fn doc_fields(&self) -> &str {
        &self.fields
    }
    /// Name of the legacy scope admission metadata table, retained for layout compatibility.
    #[must_use]
    pub fn scope_admissions(&self) -> &str {
        &self.admissions
    }
    /// Name of the legacy purge receipt table, retained for layout compatibility.
    #[must_use]
    pub fn purge_receipts(&self) -> &str {
        &self.purge_receipts
    }
    /// Name of the collection declaration table.
    #[must_use]
    pub fn collections(&self) -> &str {
        &self.specs
    }
    /// Name of the full-text search table.
    #[must_use]
    pub fn fts(&self) -> &str {
        &self.fts
    }
    /// Name of the stream entry table.
    #[must_use]
    pub fn streams(&self) -> &str {
        &self.streams
    }
    /// Name of the stream head table.
    #[must_use]
    pub fn stream_heads(&self) -> &str {
        &self.stream_heads
    }
    /// Name of the blob table.
    #[must_use]
    pub fn blobs(&self) -> &str {
        &self.blobs
    }
    /// Name of the table storing blob write timestamps added after v1.0.0.
    #[must_use]
    pub fn blob_written(&self) -> &str {
        &self.blob_written
    }
    /// Name of the layout metadata table.
    #[must_use]
    pub fn meta(&self) -> &str {
        &self.meta
    }
    /// Name of the key/value table.
    #[must_use]
    pub fn kv(&self) -> &str {
        &self.kv
    }
}

/// Whether a stored `CREATE` statement declares the same table as `expected`,
/// ignoring case, whitespace and `IF NOT EXISTS`.
pub(crate) fn same_declaration(stored: &str, expected: &str) -> bool {
    let normalize = |sql: &str| {
        sql.to_ascii_uppercase()
            .replace("IF NOT EXISTS", "")
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect::<String>()
    };
    normalize(stored) == normalize(expected)
}

/// A stored document body: JSON text, or a marked zstd frame.
#[derive(Debug)]
pub(crate) enum StoredBody {
    Text(String),
    Blob(Vec<u8>),
}

/// The row a write condition reads for one guarded document.
#[derive(Debug)]
pub(crate) struct GuardRow {
    pub(crate) schema: u32,
    pub(crate) created_ms: i64,
    pub(crate) updated_ms: i64,
    pub(crate) expires_ms: Option<i64>,
    pub(crate) body: StoredBody,
}

/// Decide one condition clause from its guarded document (`None` when absent
/// or deleted) and the stored declaration of its collection, if any.
///
/// # Errors
/// Returns `Conflict` if the guard is absent, expired, or does not match;
/// returns a backend error for corrupt persisted rows.
pub(crate) fn judge_clause(
    clause: &WriteConditionClause,
    row: Option<GuardRow>,
    spec_json: Option<&str>,
    now_ms: i64,
) -> Result<()> {
    let Some(row) = row else {
        return Err(StorageError::conflict("write condition document is absent"));
    };
    if row.expires_ms.is_some_and(|expires| expires <= now_ms) {
        return Err(StorageError::conflict(
            "write condition document is expired",
        ));
    }
    let body: Value = match row.body {
        StoredBody::Text(text) => serde_json::from_str(&text)?,
        StoredBody::Blob(bytes) => {
            let Some((&1, compressed)) = bytes.split_first() else {
                return Err(StorageError::backend("corrupt conditional document body"));
            };
            let json = zstd::decode_all(compressed).map_err(|error| {
                StorageError::backend("decompress condition document").with_source(error)
            })?;
            serde_json::from_slice(&json)?
        }
    };
    let spec = match spec_json {
        Some(json) => serde_json::from_str::<CollectionSpec>(json)?,
        None => CollectionSpec::new(clause.collection()),
    };
    let ttl_expired = spec
        .ttl_field
        .as_deref()
        .and_then(|field| value::lookup(&body, field))
        .and_then(Value::as_f64)
        .is_some_and(|expires| {
            // Epoch milliseconds remain exactly representable at current dates.
            #[allow(
                clippy::cast_precision_loss,
                reason = "epoch milliseconds are far below 2^53"
            )]
            let now = now_ms as f64;
            expires <= now
        });
    if ttl_expired {
        return Err(StorageError::conflict(
            "write condition document is expired",
        ));
    }
    let meta = Meta {
        schema: row.schema,
        created_ms: row.created_ms,
        updated_ms: row.updated_ms,
        expires_ms: row.expires_ms,
    };
    if !clause.matches_at(clause.id(), &body, &meta, now_ms) {
        return Err(StorageError::conflict(
            "write condition no longer matches or is expired",
        ));
    }
    Ok(())
}
