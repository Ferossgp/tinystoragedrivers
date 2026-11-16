//! A limited query answered by SQL in the port's order, as in the SQLite
//! engine: when every sort key is SQL-visible and the page is limited, the
//! database sorts and limits, and Rust judges only the rows it was handed,
//! continuing from the last row it saw until the page is full. Each page
//! read returns the whole rows, and the first one also runs the
//! rounded-number checks in the same snapshot.

use std::cmp::Ordering;

use serde_json::Value;
use tinystoragedrivers_connect::libsql::libsql::Value as SqlValue;
use tinystoragedrivers_types::{CollectionSpec, CursorPosition, Filter, Result, Sort, Versioned};

use super::ops::{Ctx, Reader, STORED_COLUMNS, stored_at};
use super::order::{self, FIRST_STRUCTURAL};
use super::pushdown::{self, Projection};
use crate::libsql::{ident, statement};

/// The first column of the selected rank expressions: after the id and the
/// seven stored columns.
const FIRST_RANK: usize = 8;

/// The first rows after `after` that match `filter`, in `sort` order, at most
/// `limit` of them; `None` when SQL cannot answer faithfully and the caller
/// must sort in Rust.
pub(super) async fn matching_ordered(
    reader: &mut Reader<'_, '_>,
    ctx: Ctx<'_>,
    spec: &CollectionSpec,
    filter: &Filter,
    sort: &[Sort],
    after: Option<&CursorPosition>,
    limit: usize,
) -> Result<Option<Vec<Versioned<Value>>>> {
    let projection = Projection::new(ctx.tables(), ctx.scope, spec);
    let Some(order) = order::order(sort, &projection) else {
        return Ok(None);
    };
    let mut checks = Vec::new();
    for key in sort {
        if let Some(check) = projection.rounded_numbers(&key.field) {
            checks.push(statement(check.sql, check.params)?);
        }
    }
    let ranks = order.structural_ranks();
    let selected: String = ranks.iter().flat_map(|rank| [", ", rank]).collect();
    let narrowing = pushdown::clause(filter, &projection);

    let mut found = Vec::new();
    let mut seek = after.cloned();
    let mut batch = limit.min(MAX_SQL_LIMIT);
    loop {
        let mut sql = format!(
            "SELECT id, {STORED_COLUMNS}{selected} FROM {docs} {joins} WHERE scope = ? AND coll = ? \
             AND deleted = 0 AND (expires_ms IS NULL OR expires_ms > ?)",
            docs = ident(ctx.tables().docs()),
            joins = order.joins.sql,
        );
        let mut params = order.joins.params.clone();
        params.extend([
            SqlValue::Text(ctx.scope.to_owned()),
            SqlValue::Text(spec.name.clone()),
            SqlValue::Integer(i64::try_from(ctx.now_ms).unwrap_or(i64::MAX)),
        ]);
        if let Some(clause) = &narrowing {
            and(&mut sql, &clause.sql);
            params.extend(clause.params.iter().cloned());
        }
        if let Some(position) = &seek {
            let Some(clause) = order::after(position, &order) else {
                return Ok(None);
            };
            and(&mut sql, &clause.sql);
            params.extend(clause.params);
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&order.terms);
        sql.push_str(" LIMIT ?");
        params.push(SqlValue::Integer(i64::try_from(batch).unwrap_or(i64::MAX)));
        let mut reads = std::mem::take(&mut checks);
        let rounded = reads.len();
        reads.push(statement(sql, params)?);
        let mut sets = reader.read(&reads).await?;
        let rows = sets.pop().unwrap_or_default();
        if rounded > 0 && sets.iter().any(|set| !set.is_empty()) {
            return Ok(None);
        }
        for row in &rows {
            for index in 0..ranks.len() {
                if row.integer(FIRST_RANK + index)? >= FIRST_STRUCTURAL {
                    return Ok(None);
                }
            }
        }
        let returned = rows.len();
        for mut row in rows {
            let id = row.text(0)?;
            let stored = stored_at(&mut row, 1)?;
            seek = Some(CursorPosition::for_record(
                &id,
                &stored.doc,
                &stored.meta,
                sort,
            ));
            let past = after.is_none_or(|after| {
                seek.as_ref()
                    .is_some_and(|here| here.compare(after, sort) == Ordering::Greater)
            });
            if past
                && stored.is_live(spec, ctx.now_ms)
                && filter.matches_with(&id, &stored.doc, &stored.meta)
            {
                found.push(Versioned {
                    id,
                    version: stored.version,
                    doc: stored.doc,
                    meta: stored.meta,
                });
                if found.len() == limit {
                    return Ok(Some(found));
                }
            }
        }
        if returned < batch {
            return Ok(Some(found));
        }
        batch = (limit - found.len()).clamp(MIN_CONTINUATION, MAX_SQL_LIMIT);
    }
}

/// The largest `LIMIT` SQLite takes: a signed 64-bit integer.
const MAX_SQL_LIMIT: usize = usize::MAX >> 1;

/// Rows fetched when a page comes up short and the query continues.
const MIN_CONTINUATION: usize = 16;

fn and(sql: &mut String, condition: &str) {
    sql.push_str(" AND (");
    sql.push_str(condition);
    sql.push(')');
}
