//! The `doc_fields` side table: declared fields, projected out of the body,
//! as in the SQLite engine. Every function here queues writes on the
//! caller's transaction.

use std::collections::BTreeSet;

use serde_json::Value;
use tinystoragedrivers_connect::libsql::Tx;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_types::{CollectionSpec, Result, value};

use crate::libsql::{SqliteTables, ident, statement};

/// The paths of `spec` that are projected: index fields and search fields.
pub(crate) fn projected(spec: &CollectionSpec) -> BTreeSet<String> {
    spec.indexes
        .iter()
        .flat_map(|index| index.fields.iter())
        .chain(spec.search.iter().flat_map(|search| search.fields.iter()))
        .cloned()
        .collect()
}

/// The sort rank of a JSON value, the same one the cursors use.
pub(crate) fn rank(value: &Value) -> i64 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// The SQL value of a scalar, `NULL` for the rest.
pub(crate) fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Bool(flag) => SqlValue::Integer(i64::from(*flag)),
        Value::Number(number) => number.as_i64().map_or_else(
            || number.as_f64().map_or(SqlValue::Null, SqlValue::Real),
            SqlValue::Integer,
        ),
        Value::String(text) => SqlValue::Text(text.clone()),
        Value::Null | Value::Array(_) | Value::Object(_) => SqlValue::Null,
    }
}

/// Replace the projection of one document.
pub(crate) fn write(
    tx: &mut Tx<'_>,
    tables: &SqliteTables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    remove(tx, tables, scope, &spec.name, Some(id))?;
    insert(tx, tables, spec, scope, id, doc)
}

/// Project one document that has no projection rows.
pub(crate) fn insert(
    tx: &mut Tx<'_>,
    tables: &SqliteTables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    let sql = format!(
        "INSERT INTO {} (scope, coll, id, path, kind, value) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        ident(tables.doc_fields())
    );
    for path in &projected(spec) {
        if let Some(found) = value::lookup(doc, path) {
            tx.write(statement(
                sql.clone(),
                vec![
                    SqlValue::Text(scope.to_owned()),
                    SqlValue::Text(spec.name.clone()),
                    SqlValue::Text(id.to_owned()),
                    SqlValue::Text(path.clone()),
                    SqlValue::Integer(rank(found)),
                    sql_value(found),
                ],
            )?);
        }
    }
    Ok(())
}

/// Drop the projection of one document, or of the whole collection in `scope`.
pub(crate) fn remove(
    tx: &mut Tx<'_>,
    tables: &SqliteTables,
    scope: &str,
    collection: &str,
    id: Option<&str>,
) -> Result<()> {
    let table = ident(tables.doc_fields());
    tx.write(match id {
        Some(id) => statement(
            format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2 AND id = ?3"),
            [scope, collection, id],
        )?,
        None => statement(
            format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2"),
            [scope, collection],
        )?,
    });
    Ok(())
}

/// Drop the projection of the collection in every scope.
pub(crate) fn clear(tx: &mut Tx<'_>, tables: &SqliteTables, collection: &str) -> Result<()> {
    tx.write(statement(
        format!("DELETE FROM {} WHERE coll = ?1", ident(tables.doc_fields())),
        [collection],
    )?);
    Ok(())
}
