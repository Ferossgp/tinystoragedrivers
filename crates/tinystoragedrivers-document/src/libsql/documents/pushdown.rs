//! Translating the part of a [`Filter`] SQL can evaluate exactly enough; the
//! rules of the SQLite engine.
//!
//! The SQL produced here is only a *superset* condition: every row is
//! re-checked in Rust with [`Filter::matches_with`]. Pushed clauses are
//! equality and membership on strings and booleans, ranges on projected
//! fields, the document id and the metadata columns. Body fields are answered
//! from the `doc_fields` side table. Unlike the SQLite engine, the scope,
//! collection and field path are bound as parameters too, so no caller
//! string reaches the SQL text.

use std::collections::BTreeSet;

use serde_json::Value;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_types::{
    CREATED_FIELD, CollectionSpec, EXPIRES_FIELD, Filter, ID_FIELD, SCHEMA_FIELD, UPDATED_FIELD,
};

use crate::libsql::fields;
use crate::libsql::{SqliteTables, ident};

/// SQL text and its bound parameters, in placeholder order.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Clause {
    pub(crate) sql: String,
    pub(crate) params: Vec<SqlValue>,
}

/// Which body fields of one collection in one scope SQL can see, and where.
#[derive(Debug, Clone)]
pub(crate) struct Projection {
    paths: BTreeSet<String>,
    fields_table: String,
    docs_table: String,
    scope: String,
    coll: String,
}

impl Projection {
    pub(crate) fn new(tables: &SqliteTables, scope: &str, spec: &CollectionSpec) -> Self {
        Self {
            paths: fields::projected(spec),
            fields_table: ident(tables.doc_fields()),
            docs_table: ident(tables.docs()),
            scope: scope.to_owned(),
            coll: spec.name.clone(),
        }
    }

    /// The `doc_fields` rows of `field` in this collection and scope, as a
    /// `FROM … WHERE` over alias `f` that still needs a closing condition.
    fn rows(&self, field: &str) -> Option<Clause> {
        self.paths.contains(field).then(|| Clause {
            sql: format!(
                "FROM {} AS f WHERE f.scope = ? AND f.coll = ? AND f.path = ?",
                self.fields_table
            ),
            params: vec![
                SqlValue::Text(self.scope.clone()),
                SqlValue::Text(self.coll.clone()),
                SqlValue::Text(field.to_owned()),
            ],
        })
    }

    /// A condition on the document id: `id IN (…)` over the projected rows of
    /// `field` whose value satisfies `test` (for example `= ?`).
    fn ids_where(&self, field: &str, test: &str, values: Vec<SqlValue>) -> Option<Clause> {
        let mut rows = self.rows(field)?;
        rows.params.extend(values);
        Some(Clause {
            sql: format!("id IN (SELECT f.id {} AND f.value {test})", rows.sql),
            params: rows.params,
        })
    }

    /// A `LEFT JOIN` that exposes the projected value of `field` as
    /// `s{index}.k` (kind) and `s{index}.v` (value) on each document row.
    pub(super) fn join(&self, index: usize, field: &str) -> Option<Clause> {
        let rows = self.rows(field)?;
        Some(Clause {
            sql: format!(
                "LEFT JOIN (SELECT f.id AS sid, f.kind AS k, f.value AS v {}) AS s{index} ON s{index}.sid = {}.id",
                rows.sql, self.docs_table
            ),
            params: rows.params,
        })
    }

    /// A query returning a row when `field` holds a number SQLite may have
    /// rounded (a `u64` above `i64::MAX`, stored as a float); `None` when SQL
    /// cannot see the field. A sort over such a path stays in Rust.
    pub(super) fn rounded_numbers(&self, field: &str) -> Option<Clause> {
        let rows = self.rows(field)?;
        Some(Clause {
            sql: format!(
                "SELECT 1 {} AND f.value >= 9223372036854775808.0 AND f.value <= 18446744073709551616.0 LIMIT 1",
                rows.sql
            ),
            params: rows.params,
        })
    }
}

/// The SQL form of a value an equality can be pushed down for.
fn pushable(value: &Value) -> Option<SqlValue> {
    match value {
        Value::String(text) => Some(SqlValue::Text(text.clone())),
        Value::Bool(flag) => Some(SqlValue::Integer(i64::from(*flag))),
        _ => None,
    }
}

/// The column that holds a reserved metadata path, `None` for any other path.
pub(super) fn meta_column(field: &str) -> Option<&'static str> {
    match field {
        SCHEMA_FIELD => Some("schema"),
        CREATED_FIELD => Some("created_ms"),
        UPDATED_FIELD => Some("updated_ms"),
        EXPIRES_FIELD => Some("expires_ms"),
        _ => None,
    }
}

/// A whole-number bound on a metadata column.
fn integer(value: &Value) -> Option<SqlValue> {
    value.as_i64().map(SqlValue::Integer)
}

/// The condition "`field` satisfies `test`", or `None` when SQL cannot see
/// the field.
fn on_field(
    field: &str,
    test: &str,
    values: Vec<SqlValue>,
    projection: &Projection,
) -> Option<Clause> {
    if field == ID_FIELD {
        Some(Clause {
            sql: format!("id {test}"),
            params: values,
        })
    } else {
        projection.ids_where(field, test, values)
    }
}

/// The `kind` rank and SQL value of a range bound, and whether the comparison
/// must be inclusive to stay a superset (see the SQLite engine).
fn range_bound(value: &Value) -> Option<(i64, SqlValue, bool)> {
    let lossy = match value {
        Value::Number(number) => {
            number.as_i64().is_none() && number.as_f64().is_some_and(|n| n.abs() >= LOSSY_FROM)
        }
        Value::String(_) | Value::Bool(_) => false,
        _ => return None,
    };
    Some((fields::rank(value), fields::sql_value(value), lossy))
}

/// Numbers from here up may be stored rounded (2^62).
const LOSSY_FROM: f64 = 4.611_686_018_427_388e18;

/// A superset condition for a range over a projected field or the id.
fn range(
    field: &str,
    bounds: [(&str, &Option<Value>); 4],
    projection: &Projection,
) -> Option<Clause> {
    let mut terms = Vec::new();
    let mut params = Vec::new();
    for (operator, bound) in bounds {
        let Some(bound) = bound else { continue };
        if field == ID_FIELD {
            if let Value::String(text) = bound {
                terms.push(format!("id {operator} ?"));
                params.push(SqlValue::Text(text.clone()));
            }
            continue;
        }
        let Some((kind, sql, lossy)) = range_bound(bound) else {
            continue;
        };
        let operator = match (lossy, operator) {
            (true, ">") => ">=",
            (true, "<") => "<=",
            _ => operator,
        };
        terms.push(format!("f.kind = {kind} AND f.value {operator} ?"));
        params.push(sql);
    }
    if terms.is_empty() {
        return None;
    }
    if field == ID_FIELD {
        return Some(Clause {
            sql: terms.join(" AND "),
            params,
        });
    }
    let mut rows = projection.rows(field)?;
    rows.params.extend(params);
    Some(Clause {
        sql: format!(
            "id IN (SELECT f.id {} AND {})",
            rows.sql,
            terms.join(" AND ")
        ),
        params: rows.params,
    })
}

/// A superset condition for `filter`, or `None` when nothing can be pushed.
pub(crate) fn clause(filter: &Filter, projection: &Projection) -> Option<Clause> {
    match filter {
        Filter::Eq { field, value } if meta_column(field).is_some() => Some(Clause {
            sql: format!("{} = ?", meta_column(field)?),
            params: vec![integer(value)?],
        }),
        Filter::In { field, values } if meta_column(field).is_some() => {
            let bound: Vec<SqlValue> = values.iter().map(integer).collect::<Option<_>>()?;
            if bound.is_empty() {
                return None;
            }
            Some(Clause {
                sql: format!(
                    "{} IN ({})",
                    meta_column(field)?,
                    vec!["?"; bound.len()].join(", ")
                ),
                params: bound,
            })
        }
        Filter::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } if meta_column(field).is_some() => {
            let column = meta_column(field)?;
            let mut terms = Vec::new();
            let mut params = Vec::new();
            for (operator, bound) in [(">", gt), (">=", gte), ("<", lt), ("<=", lte)] {
                if let Some(bound) = bound {
                    terms.push(format!("{column} {operator} ?"));
                    params.push(integer(bound)?);
                }
            }
            (!terms.is_empty()).then(|| Clause {
                sql: terms.join(" AND "),
                params,
            })
        }
        Filter::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } => range(
            field,
            [(">", gt), (">=", gte), ("<", lt), ("<=", lte)],
            projection,
        ),
        Filter::Eq { field, value } => {
            let bound = pushable(value)?;
            if field == ID_FIELD && !value.is_string() {
                return None;
            }
            on_field(field, "= ?", vec![bound], projection)
        }
        Filter::In { field, values } => {
            let bound: Vec<SqlValue> = values.iter().map(pushable).collect::<Option<_>>()?;
            if bound.is_empty() || (field == ID_FIELD && !values.iter().all(Value::is_string)) {
                return None;
            }
            let marks = vec!["?"; bound.len()].join(", ");
            on_field(field, &format!("IN ({marks})"), bound, projection)
        }
        Filter::And { filters } => {
            let parts: Vec<Clause> = filters
                .iter()
                .filter_map(|part| clause(part, projection))
                .collect();
            if parts.is_empty() {
                return None;
            }
            let sql = parts
                .iter()
                .map(|part| format!("({})", part.sql))
                .collect::<Vec<_>>()
                .join(" AND ");
            Some(Clause {
                sql,
                params: parts.into_iter().flat_map(|part| part.params).collect(),
            })
        }
        _ => None,
    }
}
