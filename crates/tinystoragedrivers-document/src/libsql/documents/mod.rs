//! [`DocumentStore`] on libSQL, with the SQLite engine's layout and rules.
//!
//! All collections share one `WITHOUT ROWID` table keyed by
//! `(scope, collection, id)`; declared index and search fields are projected
//! into `doc_fields` and an FTS5 table, and filters push their exact parts
//! down to SQL and are re-checked in Rust with [`Filter::matches_with`].
//!
//! Every write is one `BEGIN IMMEDIATE` transaction: its reads (condition,
//! stored row, unique-index candidates) run inside it, and its writes travel
//! with the commit. That keeps preconditions, unique indexes, claims and
//! batches atomic.

pub(crate) mod ops;
mod order;
mod ordered;
mod pushdown;

use serde_json::Value;
use std::fmt;
use tinystoragedrivers_api::{DocumentStore, async_trait};
use tinystoragedrivers_connect::LibsqlConnection;
use tinystoragedrivers_connect::libsql::Tx;
use tinystoragedrivers_connect::libsql::libsql::Connection;
use tinystoragedrivers_types::{
    Capabilities, Capability, CollectionSpec, Cursor, CursorPosition, Filter, Page, Precondition,
    Query, ReadCheck, Result, Scope, SearchHit, Sort, StorageError, Version, Versioned,
    WriteCondition, WriteOp, WriteOptions, WriteResult, validate_collection, validate_doc,
    validate_id, value,
};

use ops::{Ctx, Reader};

/// The cursor kind this engine issues.
const CURSOR_KIND: &str = "libsql";

/// What this driver supports.
pub(crate) fn capabilities() -> Capabilities {
    Capabilities::none()
        .with(Capability::Documents)
        .with(Capability::FullText)
        .with(Capability::Transactions)
        .with(Capability::ValidatedBatch)
        .with(Capability::Ttl)
        .with(Capability::CollectionEnumeration)
        .with(Capability::CollectionValidation)
        .with(Capability::RecordMetadata)
        .with(Capability::SharedAcrossProcesses)
        .with(Capability::CrossProcessCas)
}

/// libSQL documents bound to one scope.
#[derive(Clone)]
pub struct LibsqlDocuments {
    connection: LibsqlConnection,
    scope: Scope,
    condition: Option<WriteCondition>,
}

impl fmt::Debug for LibsqlDocuments {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LibsqlDocuments")
            .field("scope", &self.scope)
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}

impl LibsqlDocuments {
    /// Bind a document adapter to a shared libSQL connection and opaque scope.
    #[must_use]
    pub fn new(connection: LibsqlConnection, scope: Scope) -> Self {
        Self {
            connection,
            scope,
            condition: None,
        }
    }

    /// Bind a document adapter whose mutations check `condition` in their
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

    /// The context of one call; take it after locking the connection.
    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            connection: &self.connection,
            scope: self.scope.as_str(),
            now_ms: (self.connection.clock())(),
        }
    }

    /// A write transaction that checks this handle's condition.
    fn tx<'a>(&'a self, conn: &'a Connection, operation: &'static str, ctx: Ctx<'_>) -> Tx<'a> {
        Tx::new(conn, operation).conditioned(
            self.connection.tables(),
            self.condition.as_ref(),
            i64::try_from(ctx.now_ms).unwrap_or(i64::MAX),
        )
    }
}

/// FNV-1a: a stable hash, so a cursor stays valid across processes.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A fingerprint of what decides a query's result order.
fn fingerprint(collection: &str, scope: &Scope, query: &Query) -> Result<u64> {
    let shape = serde_json::to_string(&(collection, scope.as_str(), &query.filter, &query.sort))?;
    Ok(fnv1a(shape.as_bytes()))
}

fn parse_cursor(collection: &str, scope: &Scope, query: &Query) -> Result<Option<CursorPosition>> {
    let Some(cursor) = &query.cursor else {
        return Ok(None);
    };
    let position = cursor.decode_keyset(CURSOR_KIND, fingerprint(collection, scope, query)?)?;
    position.validate(&query.sort)?;
    Ok(Some(position))
}

fn validate_op(op: &WriteOp) -> Result<()> {
    match op {
        WriteOp::Put {
            collection,
            id,
            doc,
            ..
        } => {
            validate_collection(collection)?;
            validate_id(id)?;
            validate_doc(doc)
        }
        WriteOp::Delete { collection, id, .. } => {
            validate_collection(collection)?;
            validate_id(id)
        }
    }
}

/// Run a checked batch inside `tx`.
async fn run_batch(
    tx: &mut Tx<'_>,
    ctx: Ctx<'_>,
    checks: &[ReadCheck],
    writes: Vec<WriteOp>,
) -> Result<Vec<WriteResult>> {
    for check in checks {
        check.validate()?;
        let current = ops::get(&mut Reader::Tx(tx), ctx, &check.collection, &check.id).await?;
        check.verify(current.map(|found| found.version))?;
    }
    let mut results = Vec::with_capacity(writes.len());
    for op in writes {
        results.push(match op {
            WriteOp::Put {
                collection,
                id,
                doc,
                precondition,
            } => WriteResult::Put {
                version: ops::put_with(
                    tx,
                    ctx,
                    &collection,
                    &id,
                    &doc,
                    precondition,
                    WriteOptions::default(),
                )
                .await?
                .0,
            },
            WriteOp::Delete {
                collection,
                id,
                precondition,
            } => WriteResult::Delete {
                removed: ops::delete(tx, ctx, &collection, &id, precondition).await?,
            },
        });
    }
    Ok(results)
}

/// Claim the first match inside `tx`.
async fn run_claim(
    tx: &mut Tx<'_>,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
    sort: &[Sort],
    patch: &Value,
) -> Result<Option<Versioned<Value>>> {
    let Some(first) = ops::matching_after(
        &mut Reader::Tx(tx),
        ctx,
        collection,
        filter,
        sort,
        None,
        Some(1),
    )
    .await?
    .into_iter()
    .next() else {
        return Ok(None);
    };
    let mut doc = first.doc;
    value::merge_patch(&mut doc, patch);
    // A claim patches the body only: the schema and expiry carry over.
    let (version, meta) = ops::put_with(
        tx,
        ctx,
        collection,
        &first.id,
        &doc,
        Precondition::Version(first.version),
        WriteOptions {
            schema: first.meta.schema,
            expires_ms: first.meta.expires_ms,
            created_ms: None,
        },
    )
    .await?;
    Ok(Some(Versioned {
        id: first.id,
        version,
        doc,
        meta,
    }))
}

#[async_trait]
impl DocumentStore for LibsqlDocuments {
    fn capabilities(&self) -> Capabilities {
        let capabilities = capabilities();
        self.condition.as_ref().map_or(capabilities, |_| {
            capabilities.with(Capability::ConditionalWrites)
        })
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        spec.validate()?;
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        // Declarations are not conditioned, as in the SQLite engine.
        let mut tx = Tx::new(&conn, "declare collection");
        let outcome = ops::declare(&mut tx, ctx, spec).await;
        match tx.finish(outcome).await {
            Ok(merged) => {
                self.connection.cache_spec(merged);
                Ok(())
            }
            Err(error) => {
                self.connection.forget_spec(&spec.name);
                Err(error)
            }
        }
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        required.validate()?;
        let conn = self.connection.lock().await;
        ops::validate_spec(&conn, self.connection.tables(), required).await
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        validate_id(id)?;
        let conn = self.connection.lock().await;
        ops::get(&mut Reader::Plain(&conn), self.ctx(), collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.put_with(collection, id, doc, precondition, WriteOptions::default())
            .await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        validate_collection(collection)?;
        validate_id(id)?;
        validate_doc(&doc)?;
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        let mut tx = self.tx(&conn, "put", ctx);
        let outcome =
            ops::put_with(&mut tx, ctx, collection, id, &doc, precondition, options).await;
        tx.finish(outcome).await.map(|(version, _)| version)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        validate_collection(collection)?;
        validate_id(id)?;
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        let mut tx = self.tx(&conn, "delete", ctx);
        let outcome = ops::delete(&mut tx, ctx, collection, id, precondition).await;
        tx.finish(outcome).await
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        validate_collection(collection)?;
        query.validate()?;
        let position = parse_cursor(collection, &self.scope, query)?;
        let fingerprint = fingerprint(collection, &self.scope, query)?;
        let conn = self.connection.lock().await;
        let found = ops::matching_after(
            &mut Reader::Plain(&conn),
            self.ctx(),
            collection,
            &query.filter,
            &query.sort,
            position.as_ref(),
            query.limit.map(|limit| limit.saturating_add(1)),
        )
        .await?;
        drop(conn);
        let end = query
            .limit
            .map_or(found.len(), |limit| limit.min(found.len()));
        let items = found.get(..end).map(<[_]>::to_vec).unwrap_or_default();
        let next = if end < found.len() {
            let last = items
                .last()
                .ok_or_else(|| StorageError::backend("a continued page was empty"))?;
            Some(Cursor::keyset(
                CURSOR_KIND,
                fingerprint,
                &CursorPosition::for_record(&last.id, &last.doc, &last.meta, &query.sort),
            )?)
        } else {
            None
        };
        Ok(Page { items, next })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let conn = self.connection.lock().await;
        let found = ops::matching(
            &mut Reader::Plain(&conn),
            self.ctx(),
            collection,
            filter,
            &[],
        )
        .await?;
        Ok(found.len() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        validate_collection(collection)?;
        filter.validate()?;
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        let mut tx = self.tx(&conn, "delete matching", ctx);
        let outcome = ops::delete_where(&mut tx, ctx, collection, filter).await;
        tx.finish(outcome).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        validate_collection(collection)?;
        filter.validate()?;
        for order in sort {
            Filter::exists(order.field.clone(), true).validate()?;
        }
        validate_doc(patch)?;
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        let mut tx = self.tx(&conn, "claim", ctx);
        let outcome = run_claim(&mut tx, ctx, collection, filter, sort, patch).await;
        tx.finish(outcome).await
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.atomic_batch_checked(Vec::new(), ops).await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        for check in &checks {
            check.validate()?;
        }
        for op in &ops {
            validate_op(op)?;
        }
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        let mut tx = self.tx(&conn, "atomic batch", ctx);
        let outcome = run_batch(&mut tx, ctx, &checks, ops).await;
        tx.finish(outcome).await
    }

    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        validate_collection(collection)?;
        let conn = self.connection.lock().await;
        let hits = ops::search(&conn, self.ctx(), collection, text, limit).await?;
        Ok(hits
            .into_iter()
            .map(|(id, score)| SearchHit { id, score })
            .collect())
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        validate_collection(collection)?;
        let conn = self.connection.lock().await;
        let ctx = self.ctx();
        let mut tx = self.tx(&conn, "drop collection", ctx);
        let outcome = ops::drop_collection(&mut tx, ctx, collection);
        tx.finish(outcome).await
    }

    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let conn = self.connection.lock().await;
        ops::collections(&conn, self.ctx(), prefix).await
    }
}
