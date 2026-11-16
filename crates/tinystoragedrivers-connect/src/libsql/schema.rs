//! Creating and validating the layout-2 tables on a libSQL database.
//!
//! The statements are the SQLite handle's ([`crate::layout`]) plus the
//! key/value table, which the libSQL key/value engine cannot create lazily.
//! Nothing upgrades an older layout: a libSQL database starts at layout 2.

use libsql::Connection;
use tinystoragedrivers_types::{Result, StorageError};

use super::error::during;
use super::tx::query_on;
use super::{Row, Statement, transactional_batch};
use crate::SqliteTables;
use crate::layout::{LAYOUT, same_declaration};

/// Create the tables unless they all exist, then validate them.
pub(super) async fn ensure(tables: &SqliteTables, conn: &Connection) -> Result<()> {
    let declarations = declarations(tables);
    let schema = schema_query(&declarations).map_err(during("inspect existing schema"))?;
    let found = query_on(
        conn,
        "inspect existing schema",
        std::slice::from_ref(&schema),
    )
    .await?;
    if found.first().map_or(0, Vec::len) < declarations.len() {
        let statements = create(tables, &declarations).map_err(during("create tables"))?;
        transactional_batch(conn, &statements)
            .await
            .map_err(during("create tables"))?;
    }
    let layout = Statement::new(tables.layout_query(), ()).map_err(during("read layout"))?;
    let mut found = query_on(conn, "inspect existing schema", &[schema, layout]).await?;
    let layout = found.pop().unwrap_or_default();
    let stored = found.pop().unwrap_or_default();
    validate(&declarations, &stored, &layout)
}

/// Every table with its `CREATE` statement.
fn declarations(tables: &SqliteTables) -> Vec<(String, String)> {
    let mut all: Vec<(String, String)> = tables
        .declarations()
        .into_iter()
        .map(|(name, ddl)| (name.to_owned(), ddl))
        .collect();
    all.push((tables.kv().to_owned(), tables.kv_declaration()));
    all
}

/// The stored `CREATE` statement of each declared table that exists.
fn schema_query(declarations: &[(String, String)]) -> libsql::Result<Statement> {
    let marks = vec!["?"; declarations.len()].join(", ");
    Statement::new(
        format!("SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name IN ({marks})"),
        declarations
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
    )
}

/// The tables, the side indexes and the layout marker, as one batch.
fn create(
    tables: &SqliteTables,
    declarations: &[(String, String)],
) -> libsql::Result<Vec<Statement>> {
    let mut statements = Vec::with_capacity(declarations.len() + 3);
    for (_, ddl) in declarations {
        statements.push(Statement::new(ddl.clone(), ())?);
    }
    for index in tables.index_declarations() {
        statements.push(Statement::new(index, ())?);
    }
    statements.push(Statement::new(tables.layout_marker(), [LAYOUT])?);
    Ok(statements)
}

/// Check every declared table against its stored statement and the layout
/// marker.
fn validate(declarations: &[(String, String)], stored: &[Row], layout: &[Row]) -> Result<()> {
    for (name, expected) in declarations {
        let mut found = None;
        for row in stored {
            if row.text(0)? == *name {
                found = Some(row.text(1)?);
            }
        }
        let found = found.ok_or_else(|| StorageError::not_found("driver table does not exist"))?;
        if !same_declaration(&found, expected) {
            return Err(StorageError::invalid_input(
                "existing driver table has incompatible schema",
            ));
        }
    }
    match layout
        .first()
        .map(|row| row.text(0))
        .transpose()?
        .as_deref()
    {
        Some(LAYOUT) => Ok(()),
        _ => Err(StorageError::invalid_input(
            "existing driver tables are not layout 2",
        )),
    }
}
