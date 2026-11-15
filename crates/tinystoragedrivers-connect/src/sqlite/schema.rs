//! Creating, validating and guarding the layout-2 tables on a SQLite file.
//!
//! The table names and `CREATE` statements live in [`crate::layout`], shared
//! with the libSQL handle. [`SqliteTables::ensure`] refuses a v0.x file
//! (`_tsd_docs` and friends); converting one is the host's job.
//!
//! Every name that reaches SQL text is either validated by the core or
//! quoted; values are always bound as parameters. The one place a string is
//! inlined is the correlated `doc_fields` sub-select of a keyset cursor, and
//! it goes through a literal quoter.

use rusqlite::{Connection, OptionalExtension};
use tinystoragedrivers_types::{Result, StorageError, WriteCondition};

use super::error::during;
pub use crate::layout::SqliteTables;
use crate::layout::{GuardRow, StoredBody, judge_clause, same_declaration};
pub(crate) use crate::layout::{LAYOUT, ident};

/// Quote an SQL string literal.
#[cfg(test)]
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

impl SqliteTables {
    /// Check a host condition using this connection's transaction snapshot.
    /// Call only from the same write transaction as the mutation being guarded.
    ///
    /// # Errors
    /// Returns `Conflict` if the guard is absent, expired, deleted, or does not
    /// match; returns a backend error for corrupt persisted rows.
    pub fn check_write_condition(
        &self,
        conn: &Connection,
        condition: &WriteCondition,
        now_ms: i64,
    ) -> Result<()> {
        for clause in condition.clauses() {
            let stored = conn
                .query_row(
                    &self.condition_query(),
                    rusqlite::params![clause.scope().as_str(), clause.collection(), clause.id()],
                    |row| {
                        Ok((
                            row.get::<_, u32>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, rusqlite::types::Value>(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(during("read condition document"))?;
            let row = stored
                .map(|(schema, created_ms, updated_ms, expires_ms, body)| {
                    let body = match body {
                        rusqlite::types::Value::Text(text) => StoredBody::Text(text),
                        rusqlite::types::Value::Blob(bytes) => StoredBody::Blob(bytes),
                        _ => {
                            return Err(StorageError::backend("corrupt conditional document body"));
                        }
                    };
                    Ok(GuardRow {
                        schema,
                        created_ms,
                        updated_ms,
                        expires_ms,
                        body,
                    })
                })
                .transpose()?;
            let spec_json = match row {
                Some(_) => conn
                    .query_row(&self.spec_query(), [clause.collection()], |row| {
                        row.get::<_, String>(0)
                    })
                    .optional()
                    .map_err(during("read condition collection spec"))?,
                None => None,
            };
            judge_clause(clause, row, spec_json.as_deref(), now_ms)?;
        }
        Ok(())
    }

    /// Create or validate the current generic storage layout.
    /// Legacy schema conversion belongs to the host and is intentionally not
    /// part of a connection handle.
    pub(crate) fn ensure(&self, conn: &Connection) -> Result<()> {
        if table_exists(conn, "_tsd_docs")? {
            return Err(StorageError::invalid_input(
                "legacy sqlite storage must be upgraded by the host",
            ));
        }
        self.create(conn)?;
        self.validate(conn)
    }

    /// Create the layout-2 tables, the expiry index and the layout marker.
    fn create(&self, conn: &Connection) -> Result<()> {
        let ddl: Vec<String> = self
            .declarations()
            .into_iter()
            .map(|(_, ddl)| ddl)
            .collect();
        conn.execute_batch(&ddl.join(";\n"))
            .map_err(during("create tables"))?;
        self.finish(conn)?;
        conn.execute(&self.layout_marker(), [LAYOUT])
            .map_err(during("record layout"))?;
        Ok(())
    }

    /// Add the head time columns to a layout-2 file created without them.
    /// Idempotent; the layout stays 2.
    fn add_head_times(&self, conn: &Connection) -> Result<()> {
        for column in ["sealed_ms", "last_append_ms"] {
            if !has_column(conn, &self.stream_heads, column)? {
                conn.execute_batch(&format!(
                    "ALTER TABLE {} ADD COLUMN {column} INTEGER",
                    ident(&self.stream_heads)
                ))
                .map_err(during("add stream head times"))?;
            }
        }
        Ok(())
    }

    /// Indexes that live beside the tables rather than in a collection spec.
    fn finish(&self, conn: &Connection) -> Result<()> {
        self.add_head_times(conn)?;
        for index in self.index_declarations() {
            conn.execute_batch(&index)
                .map_err(during("index blobs and projected fields"))?;
        }
        Ok(())
    }

    /// The recorded layout, `None` when the file has no `layout` row.
    fn layout(&self, conn: &Connection) -> Result<Option<String>> {
        conn.query_row(&self.layout_query(), [], |row| row.get(0))
            .optional()
            .map_err(during("read layout"))
    }

    /// Inspect the exact driver layout without creating or migrating tables.
    pub(crate) fn validate(&self, conn: &Connection) -> Result<()> {
        for (name, expected) in self.declarations() {
            let stored: Option<String> = conn
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                    [name],
                    |row| row.get(0),
                )
                .optional()
                .map_err(during("inspect existing schema"))?;
            let stored =
                stored.ok_or_else(|| StorageError::not_found("driver table does not exist"))?;
            if !same_declaration(&stored, &expected) {
                return Err(StorageError::invalid_input(
                    "existing driver table has incompatible schema",
                ));
            }
        }
        match self.layout(conn)?.as_deref() {
            Some(LAYOUT) => Ok(()),
            _ => Err(StorageError::invalid_input(
                "existing driver tables are not layout 2",
            )),
        }
    }
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut statement = conn
        .prepare(&format!("PRAGMA table_info({})", ident(table)))
        .map_err(during("inspect table columns"))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(during("inspect table columns"))?;
    for name in names {
        if name.map_err(during("inspect table columns"))? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether a table (or virtual table) named `name` exists.
pub(crate) fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [name],
        |row| row.get(0),
    )
    .map_err(during("inspect tables"))
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
