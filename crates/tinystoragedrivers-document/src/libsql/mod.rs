//! libSQL document and stream engines sharing the generic connection handle.
//!
//! They keep the SQLite engines' layout-2 tables, rules and results
//! ([`crate::sqlite`] when that feature is built), over an async
//! [`LibsqlConnection`](tinystoragedrivers_connect::LibsqlConnection) that may
//! be a remote database. What changes is the number of requests: reads that
//! decide a write go in the first request of the transaction, which opens it,
//! and the writes travel with the `COMMIT`; collection declarations are read
//! once per connection. Every value reaches SQL as a bound parameter.
pub use tinystoragedrivers_connect::SqliteTables;

mod body;
pub mod documents;
mod fields;
pub mod streams;

use std::borrow::Cow;

use tinystoragedrivers_connect::libsql::Statement;
use tinystoragedrivers_connect::libsql::error::during;
use tinystoragedrivers_connect::libsql::libsql::params::IntoParams;
use tinystoragedrivers_types::Result;

pub use documents::LibsqlDocuments;
pub use streams::LibsqlStreams;

pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A statement with positional parameters.
pub(crate) fn statement(
    sql: impl Into<Cow<'static, str>>,
    params: impl IntoParams,
) -> Result<Statement> {
    Statement::new(sql, params).map_err(during("prepare a statement"))
}
