//! Async document operations on one locked connection.
//!
//! Reads go through a [`Reader`]: on their own, or inside the caller's write
//! transaction, where they share requests with the queued writes. Writes are
//! only queued here; the caller's [`Tx::finish`] sends them with the commit,
//! so a read-check-write sequence (preconditions, unique indexes, claims)
//! is atomic.

use serde_json::Value;
use tinystoragedrivers_connect::LibsqlConnection;
use tinystoragedrivers_connect::libsql::libsql::{Connection, Value as SqlValue};
use tinystoragedrivers_connect::libsql::{Row, Statement, Tx, query};
use tinystoragedrivers_types::{
    CollectionSpec, CursorPosition, Filter, Meta, Precondition, Result, Sort, StorageError,
    Version, Versioned, WriteOptions, sort_documents_with_meta, validate_collection, validate_doc,
    validate_id, value,
};

use super::{ordered, pushdown};
use crate::libsql::{SqliteTables, body, fields, ident, statement};

/// Who is asking and when.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ctx<'a> {
    pub(crate) connection: &'a LibsqlConnection,
    pub(crate) scope: &'a str,
    pub(crate) now_ms: u64,
}

impl<'a> Ctx<'a> {
    pub(crate) fn tables(&self) -> &'a SqliteTables {
        self.connection.tables()
    }

    fn now(&self) -> i64 {
        i64::try_from(self.now_ms).unwrap_or(i64::MAX)
    }
}

/// Where reads run: on their own, or in a write transaction.
#[derive(Debug)]
pub(crate) enum Reader<'r, 'c> {
    Plain(&'r Connection),
    Tx(&'r mut Tx<'c>),
}

impl Reader<'_, '_> {
    pub(crate) async fn read(&mut self, reads: &[Statement]) -> Result<Vec<Vec<Row>>> {
        match self {
            Self::Plain(conn) => query(conn, "read documents", reads).await,
            Self::Tx(tx) => tx.read(reads).await,
        }
    }

    pub(crate) async fn read_one(&mut self, read: Statement) -> Result<Vec<Row>> {
        Ok(self
            .read(std::slice::from_ref(&read))
            .await?
            .pop()
            .unwrap_or_default())
    }
}

/// The largest version SQLite's signed 64-bit column can hold.
const MAX_STORED_VERSION: u64 = i64::MAX as u64;

/// The columns [`stored_at`] reads, in order.
pub(super) const STORED_COLUMNS: &str =
    "version, schema, created_ms, updated_ms, expires_ms, deleted, body";

fn to_version(stored: i64) -> Version {
    Version(u64::try_from(stored).unwrap_or(0))
}

fn spec_statement(tables: &SqliteTables, collection: &str) -> Result<Statement> {
    statement(
        format!(
            "SELECT spec FROM {} WHERE coll = ?1",
            ident(tables.collections())
        ),
        [collection],
    )
}

/// The stored declaration text, read now.
async fn read_spec(
    reader: &mut Reader<'_, '_>,
    tables: &SqliteTables,
    collection: &str,
) -> Result<Option<String>> {
    reader
        .read_one(spec_statement(tables, collection)?)
        .await?
        .first()
        .map(|row| row.text(0))
        .transpose()
}

/// The collection's declaration, or a bare one when never declared; read
/// once per connection.
pub(crate) async fn load_spec(
    reader: &mut Reader<'_, '_>,
    ctx: Ctx<'_>,
    collection: &str,
) -> Result<CollectionSpec> {
    if let Some(spec) = ctx.connection.cached_spec(collection) {
        return Ok(spec);
    }
    let spec = match read_spec(reader, ctx.tables(), collection).await? {
        Some(text) => serde_json::from_str(&text)?,
        None => CollectionSpec::new(collection),
    };
    ctx.connection.cache_spec(spec.clone());
    Ok(spec)
}

/// Validate actual metadata presence without declaring schema.
pub(crate) async fn validate_spec(
    conn: &Connection,
    tables: &SqliteTables,
    required: &CollectionSpec,
) -> Result<()> {
    let stored = read_spec(&mut Reader::Plain(conn), tables, &required.name)
        .await?
        .ok_or_else(|| StorageError::not_found("collection has no persisted declaration"))?;
    let stored: CollectionSpec = serde_json::from_str(&stored)?;
    stored.validate()?;
    if stored.merge(required)? != stored {
        return Err(StorageError::invalid_input(
            "stored declaration lacks required features",
        ));
    }
    Ok(())
}

/// A live or deleted document of a collection, with its scope and expiry.
struct Held {
    scope: String,
    id: String,
    doc: Value,
    expires_ms: Option<i64>,
}

/// Merge `spec` into the stored declaration, refuse unique indexes the
/// stored documents violate, and rebuild the projection and search rows when
/// their fields changed. Returns the merged declaration.
pub(crate) async fn declare(
    tx: &mut Tx<'_>,
    ctx: Ctx<'_>,
    spec: &CollectionSpec,
) -> Result<CollectionSpec> {
    let tables = ctx.tables();
    let existing = match read_spec(&mut Reader::Tx(tx), tables, &spec.name).await? {
        Some(text) => serde_json::from_str(&text)?,
        None => CollectionSpec::new(&spec.name),
    };
    let merged = existing.merge(spec)?;
    let unique_added = merged
        .indexes
        .iter()
        .any(|index| index.unique && !existing.indexes.contains(index));
    let rebuild = fields::projected(&existing) != fields::projected(&merged);
    let reindex = existing.search != merged.search;
    let held = if unique_added || rebuild || reindex {
        let read = statement(
            format!(
                "SELECT scope, id, body, expires_ms FROM {} WHERE coll = ?1 AND deleted = 0",
                ident(tables.docs())
            ),
            [merged.name.as_str()],
        )?;
        let mut held = Vec::new();
        for mut row in tx.read_one(read).await? {
            held.push(Held {
                scope: row.text(0)?,
                id: row.text(1)?,
                doc: body::decode(&row.take(2)?)?,
                expires_ms: row.opt_integer(3)?,
            });
        }
        held
    } else {
        Vec::new()
    };
    check_existing_unique(&existing, &merged, &held, ctx.now_ms)?;
    tx.write(statement(
        format!(
            "INSERT INTO {} (coll, spec) VALUES (?1, ?2)
             ON CONFLICT (coll) DO UPDATE SET spec = excluded.spec",
            ident(tables.collections())
        ),
        [merged.name.clone(), serde_json::to_string(&merged)?],
    )?);
    if rebuild {
        fields::clear(tx, tables, &merged.name)?;
        for found in &held {
            fields::insert(tx, tables, &merged, &found.scope, &found.id, &found.doc)?;
        }
    }
    if reindex {
        tx.write(statement(
            format!("DELETE FROM {} WHERE coll = ?1", ident(tables.fts())),
            [merged.name.as_str()],
        )?);
        for found in &held {
            index_text(tx, tables, &merged, &found.scope, &found.id, &found.doc)?;
        }
    }
    Ok(merged)
}

/// Refuse newly declared unique indexes that the stored live documents
/// already violate, in any scope.
fn check_existing_unique(
    existing: &CollectionSpec,
    merged: &CollectionSpec,
    held: &[Held],
    now_ms: u64,
) -> Result<()> {
    let live: Vec<&Held> = held
        .iter()
        .filter(|found| !lapsed(merged, &found.doc, found.expires_ms, now_ms))
        .collect();
    for index in merged
        .indexes
        .iter()
        .filter(|index| index.unique && !existing.indexes.contains(index))
    {
        let mut keys: Vec<(&str, Vec<Value>)> = live
            .iter()
            .filter_map(|found| {
                let key = index
                    .fields
                    .iter()
                    .map(|field| value::lookup(&found.doc, field).cloned())
                    .collect::<Option<Vec<Value>>>()?;
                Some((found.scope.as_str(), key))
            })
            .collect();
        let order = |a: &(&str, Vec<Value>), b: &(&str, Vec<Value>)| {
            a.0.cmp(b.0).then_with(|| {
                value::compare(&Value::Array(a.1.clone()), &Value::Array(b.1.clone()))
            })
        };
        keys.sort_by(order);
        if keys
            .windows(2)
            .any(|pair| order(&pair[0], &pair[1]).is_eq())
        {
            return Err(StorageError::already_exists(
                "stored documents already violate this unique index",
            ));
        }
    }
    Ok(())
}

/// The searchable text of `doc` under `spec`, if the collection is searchable.
fn search_body(spec: &CollectionSpec, doc: &Value) -> Option<String> {
    let search = spec.search.as_ref()?;
    let tokens: Vec<String> = search
        .fields
        .iter()
        .filter_map(|field| value::lookup(doc, field))
        .flat_map(value::tokens)
        .collect();
    Some(tokens.join(" "))
}

fn index_text(
    tx: &mut Tx<'_>,
    tables: &SqliteTables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    if let Some(body) = search_body(spec, doc) {
        tx.write(statement(
            format!(
                "INSERT INTO {} (scope, coll, id, body) VALUES (?1, ?2, ?3, ?4)",
                ident(tables.fts())
            ),
            [scope, spec.name.as_str(), id, body.as_str()],
        )?);
    }
    Ok(())
}

fn unindex_text(tx: &mut Tx<'_>, ctx: Ctx<'_>, collection: &str, id: Option<&str>) -> Result<()> {
    let table = ident(ctx.tables().fts());
    tx.write(match id {
        Some(id) => statement(
            format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2 AND id = ?3"),
            [ctx.scope, collection, id],
        )?,
        None => statement(
            format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2"),
            [ctx.scope, collection],
        )?,
    });
    Ok(())
}

/// Whether `doc` has passed its collection's expiry time.
pub(crate) fn expired(spec: &CollectionSpec, doc: &Value, now_ms: u64) -> bool {
    spec.ttl_field
        .as_deref()
        .and_then(|field| value::lookup(doc, field))
        .and_then(Value::as_f64)
        .is_some_and(|expires| {
            // Epoch milliseconds fit an f64 exactly until the year 287396.
            #[allow(
                clippy::cast_precision_loss,
                reason = "epoch milliseconds are far below 2^53"
            )]
            let now = now_ms as f64;
            expires <= now
        })
}

/// Whether a document with `expires_ms` has lapsed: past its own expiry or
/// its collection's v1 ttl field.
pub(crate) fn lapsed(
    spec: &CollectionSpec,
    doc: &Value,
    expires_ms: Option<i64>,
    now_ms: u64,
) -> bool {
    let now = i64::try_from(now_ms).unwrap_or(i64::MAX);
    expires_ms.is_some_and(|expires| expires <= now) || expired(spec, doc, now_ms)
}

/// A stored row: live, expired or deleted.
pub(super) struct Stored {
    pub(super) version: Version,
    pub(super) meta: Meta,
    deleted: bool,
    pub(super) doc: Value,
}

impl Stored {
    /// Whether a reader sees this row.
    pub(super) fn is_live(&self, spec: &CollectionSpec, now_ms: u64) -> bool {
        !self.deleted && !lapsed(spec, &self.doc, self.meta.expires_ms, now_ms)
    }
}

/// The [`STORED_COLUMNS`] of `row`, read from `first` onward.
pub(super) fn stored_at(row: &mut Row, first: usize) -> Result<Stored> {
    Ok(Stored {
        version: to_version(row.integer(first)?),
        meta: Meta {
            schema: u32::try_from(row.integer(first + 1)?).unwrap_or(0),
            created_ms: row.integer(first + 2)?,
            updated_ms: row.integer(first + 3)?,
            expires_ms: row.opt_integer(first + 4)?,
        },
        deleted: row.integer(first + 5)? != 0,
        doc: body::decode(&row.take(first + 6)?)?,
    })
}

fn stored_statement(ctx: Ctx<'_>, collection: &str, id: &str) -> Result<Statement> {
    statement(
        format!(
            "SELECT {STORED_COLUMNS} FROM {} WHERE scope = ?1 AND coll = ?2 AND id = ?3",
            ident(ctx.tables().docs())
        ),
        [ctx.scope, collection, id],
    )
}

fn first_stored(rows: Vec<Row>) -> Result<Option<Stored>> {
    rows.into_iter()
        .next()
        .map(|mut row| stored_at(&mut row, 0))
        .transpose()
}

/// Read one live document.
pub(crate) async fn get(
    reader: &mut Reader<'_, '_>,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
) -> Result<Option<Versioned<Value>>> {
    validate_collection(collection)?;
    validate_id(id)?;
    let spec = load_spec(reader, ctx, collection).await?;
    let rows = reader
        .read_one(stored_statement(ctx, collection, id)?)
        .await?;
    Ok(first_stored(rows)?
        .filter(|found| found.is_live(&spec, ctx.now_ms))
        .map(|found| Versioned {
            id: id.to_owned(),
            version: found.version,
            doc: found.doc,
            meta: found.meta,
        }))
}

/// Enforce a precondition against the live document, if any.
fn check(precondition: Precondition, current: Option<Version>) -> Result<()> {
    match (precondition, current) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(_)) => {
            Err(StorageError::conflict("the document already exists"))
        }
        (Precondition::Version(expected), Some(found)) if found == expected => Ok(()),
        (Precondition::Version(_), _) => Err(StorageError::conflict(
            "the document is not at the expected version",
        )),
    }
}

/// The unordered candidate rows of `filter`: live by the row expiry, with the
/// pushed-down part of the filter applied.
fn matching_statement(ctx: Ctx<'_>, spec: &CollectionSpec, filter: &Filter) -> Result<Statement> {
    let mut sql = format!(
        "SELECT id, {STORED_COLUMNS} FROM {}
         WHERE scope = ? AND coll = ? AND deleted = 0 AND (expires_ms IS NULL OR expires_ms > ?)",
        ident(ctx.tables().docs()),
    );
    let mut params = vec![
        SqlValue::Text(ctx.scope.to_owned()),
        SqlValue::Text(spec.name.clone()),
        SqlValue::Integer(ctx.now()),
    ];
    let projection = pushdown::Projection::new(ctx.tables(), ctx.scope, spec);
    if let Some(clause) = pushdown::clause(filter, &projection) {
        sql.push_str(" AND (");
        sql.push_str(&clause.sql);
        sql.push(')');
        params.extend(clause.params);
    }
    statement(sql, params)
}

/// The rows of [`matching_statement`] that match `filter`, in `sort` order,
/// strictly after `after`.
fn matched(
    rows: Vec<Row>,
    spec: &CollectionSpec,
    filter: &Filter,
    sort: &[Sort],
    after: Option<&CursorPosition>,
    now_ms: u64,
) -> Result<Vec<Versioned<Value>>> {
    let mut found = Vec::with_capacity(rows.len());
    for mut row in rows {
        let id = row.text(0)?;
        let stored = stored_at(&mut row, 1)?;
        if !expired(spec, &stored.doc, now_ms)
            && filter.matches_with(&id, &stored.doc, &stored.meta)
        {
            found.push(Versioned {
                id,
                version: stored.version,
                doc: stored.doc,
                meta: stored.meta,
            });
        }
    }
    sort_documents_with_meta(&mut found, sort, |item| {
        (item.id.as_str(), &item.doc, &item.meta)
    });
    if let Some(after) = after {
        found.retain(|item| {
            CursorPosition::for_record(&item.id, &item.doc, &item.meta, sort).compare(after, sort)
                == std::cmp::Ordering::Greater
        });
    }
    Ok(found)
}

/// Live documents of the collection matching `filter`, in `sort` order.
pub(crate) async fn matching(
    reader: &mut Reader<'_, '_>,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
    sort: &[Sort],
) -> Result<Vec<Versioned<Value>>> {
    matching_after(reader, ctx, collection, filter, sort, None, None).await
}

/// Live documents strictly after an optional cursor in `sort` order; with a
/// `limit`, at least the first `limit` of them (SQL may stop there).
pub(crate) async fn matching_after(
    reader: &mut Reader<'_, '_>,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
    sort: &[Sort],
    after: Option<&CursorPosition>,
    limit: Option<usize>,
) -> Result<Vec<Versioned<Value>>> {
    validate_collection(collection)?;
    filter.validate()?;
    let spec = load_spec(reader, ctx, collection).await?;
    if let Some(limit) = limit
        && let Some(found) =
            ordered::matching_ordered(reader, ctx, &spec, filter, sort, after, limit).await?
    {
        return Ok(found);
    }
    let rows = reader
        .read_one(matching_statement(ctx, &spec, filter)?)
        .await?;
    matched(rows, &spec, filter, sort, after, ctx.now_ms)
}

/// One unique index's equality filter for `doc` with its candidate query.
struct Probe {
    filter: Filter,
    read: Statement,
}

fn unique_probes(ctx: Ctx<'_>, spec: &CollectionSpec, doc: &Value) -> Result<Vec<Probe>> {
    let mut probes = Vec::new();
    for index in spec.indexes.iter().filter(|index| index.unique) {
        let Some(key) = index
            .fields
            .iter()
            .map(|field| value::lookup(doc, field).cloned())
            .collect::<Option<Vec<Value>>>()
        else {
            continue;
        };
        let filter = index
            .fields
            .iter()
            .zip(key)
            .fold(Filter::All, |filter, (field, value)| {
                filter.and(Filter::eq(field.clone(), value))
            });
        let read = matching_statement(ctx, spec, &filter)?;
        probes.push(Probe { filter, read });
    }
    Ok(probes)
}

/// Write one document under `precondition`; return its new version and
/// metadata.
///
/// A live document keeps its `created_ms`; one that was never there, was
/// deleted or has expired is created now. `updated_ms` never runs backwards.
pub(crate) async fn put_with(
    tx: &mut Tx<'_>,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    doc: &Value,
    precondition: Precondition,
    options: WriteOptions,
) -> Result<(Version, Meta)> {
    validate_collection(collection)?;
    validate_id(id)?;
    validate_doc(doc)?;
    let spec = load_spec(&mut Reader::Tx(tx), ctx, collection).await?;
    let probes = unique_probes(ctx, &spec, doc)?;
    let mut reads = vec![stored_statement(ctx, collection, id)?];
    reads.extend(probes.iter().map(|probe| probe.read.clone()));
    let mut sets = tx.read(&reads).await?.into_iter();
    let previous = first_stored(sets.next().unwrap_or_default())?;
    let live = previous
        .as_ref()
        .filter(|found| found.is_live(&spec, ctx.now_ms));
    check(precondition, live.map(|found| found.version))?;
    for (probe, rows) in probes.iter().zip(sets) {
        let clash = matched(rows, &spec, &probe.filter, &[], None, ctx.now_ms)?
            .into_iter()
            .any(|other| other.id != id);
        if clash {
            return Err(StorageError::already_exists(
                "a unique index already holds this value",
            ));
        }
    }
    // Versions keep rising across expiry and deletion (a deleted row keeps its
    // version), so a stale CAS on a re-created document still fails.
    let version = match previous.as_ref().map(|found| found.version) {
        None => Version::FIRST,
        Some(last) => last
            .next()
            .filter(|next| next.0 <= MAX_STORED_VERSION)
            .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
    };
    let stored_version = i64::try_from(version.0)
        .map_err(|_| StorageError::backend("document version space is exhausted"))?;
    let now = ctx.now();
    // A migration may carry a record's original creation time over.
    let created_ms = options
        .created_ms
        .unwrap_or_else(|| live.map_or(now, |found| found.meta.created_ms));
    let updated_ms = live.map_or(now, |found| now.max(found.meta.updated_ms));
    tx.write(statement(
        format!(
            "INSERT INTO {} (scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9)
             ON CONFLICT (scope, coll, id) DO UPDATE SET
                 version = excluded.version, schema = excluded.schema,
                 created_ms = excluded.created_ms, updated_ms = excluded.updated_ms,
                 expires_ms = excluded.expires_ms, deleted = 0, body = excluded.body",
            ident(ctx.tables().docs())
        ),
        vec![
            SqlValue::Text(ctx.scope.to_owned()),
            SqlValue::Text(collection.to_owned()),
            SqlValue::Text(id.to_owned()),
            SqlValue::Integer(stored_version),
            SqlValue::Integer(i64::from(options.schema)),
            SqlValue::Integer(created_ms),
            SqlValue::Integer(updated_ms),
            options.expires_ms.map_or(SqlValue::Null, SqlValue::Integer),
            body::encode(doc)?,
        ],
    )?);
    fields::write(tx, ctx.tables(), &spec, ctx.scope, id, doc)?;
    unindex_text(tx, ctx, collection, Some(id))?;
    index_text(tx, ctx.tables(), &spec, ctx.scope, id, doc)?;
    let meta = Meta {
        schema: options.schema,
        created_ms,
        updated_ms,
        expires_ms: options.expires_ms,
    };
    Ok((version, meta))
}

/// Mark one document (or, with `id` unset, every document of the collection
/// in this scope) deleted. The row stays with its version and an empty body,
/// so the id never reuses a version.
fn bury(tx: &mut Tx<'_>, ctx: Ctx<'_>, collection: &str, id: Option<&str>) -> Result<()> {
    let table = ident(ctx.tables().docs());
    tx.write(match id {
        Some(id) => statement(
            format!(
                "UPDATE {table} SET deleted = 1, expires_ms = NULL, body = '{{}}'
                 WHERE scope = ?1 AND coll = ?2 AND id = ?3"
            ),
            [ctx.scope, collection, id],
        )?,
        None => statement(
            format!(
                "UPDATE {table} SET deleted = 1, expires_ms = NULL, body = '{{}}'
                 WHERE scope = ?1 AND coll = ?2"
            ),
            [ctx.scope, collection],
        )?,
    });
    fields::remove(tx, ctx.tables(), ctx.scope, collection, id)?;
    unindex_text(tx, ctx, collection, id)
}

/// Remove one document under `precondition`; report whether a live one went.
pub(crate) async fn delete(
    tx: &mut Tx<'_>,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    precondition: Precondition,
) -> Result<bool> {
    let current = get(&mut Reader::Tx(tx), ctx, collection, id).await?;
    check(precondition, current.as_ref().map(|found| found.version))?;
    bury(tx, ctx, collection, Some(id))?;
    Ok(current.is_some())
}

/// Remove every live document matching `filter`.
pub(crate) async fn delete_where(
    tx: &mut Tx<'_>,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
) -> Result<u64> {
    let doomed = matching(&mut Reader::Tx(tx), ctx, collection, filter, &[]).await?;
    for found in &doomed {
        bury(tx, ctx, collection, Some(&found.id))?;
    }
    Ok(doomed.len() as u64)
}

/// Remove every document of the collection in this scope.
pub(crate) fn drop_collection(tx: &mut Tx<'_>, ctx: Ctx<'_>, collection: &str) -> Result<()> {
    validate_collection(collection)?;
    bury(tx, ctx, collection, None)
}

/// List collections with records (including deleted ones) in one scope.
pub(crate) async fn collections(
    conn: &Connection,
    ctx: Ctx<'_>,
    prefix: &str,
) -> Result<Vec<String>> {
    let read = statement(
        format!(
            "SELECT DISTINCT coll FROM {} WHERE scope = ?1 ORDER BY coll",
            ident(ctx.tables().docs()),
        ),
        [ctx.scope],
    )?;
    let mut names = Vec::new();
    for row in Reader::Plain(conn).read_one(read).await? {
        let name = row.text(0)?;
        if name.starts_with(prefix) {
            names.push(name);
        }
    }
    Ok(names)
}

/// Full-text matches, best first.
pub(crate) async fn search(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    text: &str,
    limit: usize,
) -> Result<Vec<(String, f64)>> {
    validate_collection(collection)?;
    let mut reader = Reader::Plain(conn);
    let spec = load_spec(&mut reader, ctx, collection).await?;
    if spec.search.is_none() {
        return Err(StorageError::invalid_input(
            "this collection declares no search fields",
        ));
    }
    let wanted = value::tokens(&Value::String(text.to_owned()));
    if wanted.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let query = wanted
        .iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ");
    let read = statement(
        format!(
            "SELECT f.id, bm25({fts}), d.body, d.expires_ms FROM {fts} AS f
             JOIN {docs} AS d ON d.scope = f.scope AND d.coll = f.coll AND d.id = f.id
             WHERE {fts} MATCH ?1 AND f.scope = ?2 AND f.coll = ?3 AND d.deleted = 0
             ORDER BY bm25({fts}), f.id",
            fts = ident(ctx.tables().fts()),
            docs = ident(ctx.tables().docs()),
        ),
        [query.as_str(), ctx.scope, collection],
    )?;
    let mut hits = Vec::new();
    for mut row in reader.read_one(read).await? {
        let doc = body::decode(&row.take(2)?)?;
        if !lapsed(&spec, &doc, row.opt_integer(3)?, ctx.now_ms) {
            // bm25 is lower-is-better; the port reports larger-is-better.
            hits.push((row.text(0)?, -row.real(1)?));
        }
        if hits.len() == limit {
            break;
        }
    }
    Ok(hits)
}
