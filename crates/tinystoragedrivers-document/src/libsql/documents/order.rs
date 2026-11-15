//! Ordering and keyset seeks in SQL, as in the SQLite engine.
//!
//! The port's order is null, booleans, numbers, strings, then arrays and
//! objects; ties by id. For a scalar that is "(kind, value)" read from the
//! `doc_fields` side table, one `LEFT JOIN` per projected sort key. Arrays,
//! objects and rounded numbers send the query back to the Rust sort.

use serde_json::Value;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_types::{CursorPosition, Direction, EXPIRES_FIELD, ID_FIELD, Sort};

use super::pushdown::{Clause, Projection, meta_column};
use crate::libsql::fields;

/// The kind rank at and above which a value is an array or an object.
pub(super) const FIRST_STRUCTURAL: i64 = 4;

/// One sort key as SQL expressions.
#[derive(Debug)]
struct Key {
    rank: String,
    value: String,
    direction: Direction,
    varies: bool,
    projected: bool,
}

/// The joins and ordering that put a query's rows in the port's order.
#[derive(Debug)]
pub(crate) struct Order {
    keys: Vec<Key>,
    /// `LEFT JOIN`s, one per projected key, with their parameters.
    pub(crate) joins: Clause,
    /// The `ORDER BY` terms, ending with the id.
    pub(crate) terms: String,
}

impl Order {
    /// The rank expressions of the body-field keys.
    pub(crate) fn structural_ranks(&self) -> Vec<&str> {
        self.keys
            .iter()
            .filter(|key| key.projected)
            .map(|key| key.rank.as_str())
            .collect()
    }
}

/// How `sorts` can be ordered in SQL, or `None` when a key is a path SQL
/// cannot see.
pub(crate) fn order(sorts: &[Sort], projection: &Projection) -> Option<Order> {
    let mut keys = Vec::with_capacity(sorts.len());
    let mut joins = Vec::new();
    for (index, sort) in sorts.iter().enumerate() {
        let field = sort.field.as_str();
        let (rank, value, varies, projected) = if field == ID_FIELD {
            ("3".to_owned(), "id".to_owned(), false, false)
        } else if let Some(column) = meta_column(field) {
            if field == EXPIRES_FIELD {
                (
                    "CASE WHEN expires_ms IS NULL THEN 0 ELSE 2 END".to_owned(),
                    column.to_owned(),
                    true,
                    false,
                )
            } else {
                ("2".to_owned(), column.to_owned(), false, false)
            }
        } else {
            joins.push(projection.join(index, field)?);
            (
                format!("COALESCE(s{index}.k, 0)"),
                format!("s{index}.v"),
                true,
                true,
            )
        };
        keys.push(Key {
            rank,
            value,
            direction: sort.direction,
            varies,
            projected,
        });
    }
    let mut terms = Vec::new();
    for key in &keys {
        let direction = match key.direction {
            Direction::Asc => "ASC",
            Direction::Desc => "DESC",
        };
        if key.varies {
            terms.push(format!("{} {direction}", key.rank));
        }
        terms.push(format!("{} {direction}", key.value));
    }
    terms.push("id ASC".to_owned());
    Some(Order {
        keys,
        joins: Clause {
            sql: joins
                .iter()
                .map(|join| join.sql.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            params: joins.into_iter().flat_map(|join| join.params).collect(),
        },
        terms: terms.join(", "),
    })
}

/// The SQL form of a cursor value, `None` for an array or object.
fn bound(value: &Value) -> Option<SqlValue> {
    (!matches!(value, Value::Array(_) | Value::Object(_))).then(|| fields::sql_value(value))
}

/// A predicate that is true exactly for rows strictly after `position` in
/// the order of `order`, or `None` when a cursor value is an array or object.
pub(crate) fn after(position: &CursorPosition, order: &Order) -> Option<Clause> {
    let mut alternatives: Vec<String> = Vec::with_capacity(order.keys.len() + 1);
    let mut params = Vec::new();
    for current in 0..order.keys.len() {
        let mut terms = Vec::with_capacity(current + 1);
        for (key, value) in order.keys.iter().zip(&position.values).take(current) {
            terms.push(format!("({} = ? AND {} IS ?)", key.rank, key.value));
            params.push(SqlValue::Integer(fields::rank(value)));
            params.push(bound(value)?);
        }
        let key = order.keys.get(current)?;
        let value = position.values.get(current)?;
        let (rank, value_param) = (fields::rank(value), bound(value)?);
        let operator = match key.direction {
            Direction::Asc => ">",
            Direction::Desc => "<",
        };
        if rank == 0 {
            terms.push(format!("{} {operator} ?", key.rank));
            params.push(SqlValue::Integer(rank));
        } else {
            terms.push(format!(
                "({0} {operator} ? OR ({0} = ? AND {1} {operator} ?))",
                key.rank, key.value
            ));
            params.extend([
                SqlValue::Integer(rank),
                SqlValue::Integer(rank),
                value_param,
            ]);
        }
        alternatives.push(format!("({})", terms.join(" AND ")));
    }
    let mut terms = Vec::with_capacity(order.keys.len() + 1);
    for (key, value) in order.keys.iter().zip(&position.values) {
        terms.push(format!("({} = ? AND {} IS ?)", key.rank, key.value));
        params.push(SqlValue::Integer(fields::rank(value)));
        params.push(bound(value)?);
    }
    terms.push("id > ?".to_owned());
    params.push(SqlValue::Text(position.id.clone()));
    alternatives.push(format!("({})", terms.join(" AND ")));
    Some(Clause {
        sql: alternatives.join(" OR "),
        params,
    })
}
