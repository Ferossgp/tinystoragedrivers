//! Reads outside a transaction in one HTTP request.
//!
//! libSQL 0.10 runs `Connection::query` as two requests: a `describe` of the
//! statement (its column names, which only `Statement::columns` uses) and a
//! cursor, and neither closes the server stream. [`Reader::query`] sends one
//! `/v3/pipeline` request instead: `execute` with `want_rows` and `close`, on
//! a fresh stream. It is one statement, not a batch, so the server returns its
//! rows. Use it only while the connection has no open transaction: a read
//! inside one must run on the transaction's stream.

use std::sync::Arc;

use libsql::params::{IntoParams, Params};
use libsql::{Error, Value};
use libsql_hrana::proto;

use super::transport::Connector;

/// One-request reads on a remote database, over the connection transport.
#[derive(Clone)]
pub struct Reader {
    client: hyper::Client<Connector, hyper::Body>,
    pipeline_url: Arc<str>,
    auth: Arc<str>,
}

impl std::fmt::Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader").finish_non_exhaustive()
    }
}

impl Reader {
    /// The reader of the database at `url`, or `None` for a local file.
    ///
    /// # Errors
    /// Returns a connection error when the TLS configuration fails.
    pub fn open(url: &str, auth_token: &str) -> libsql::Result<Option<Self>> {
        if !super::is_remote(url) {
            return Ok(None);
        }
        let connector =
            Connector::new().map_err(|error| Error::ConnectionFailed(error.to_string()))?;
        // As libSQL: `libsql://` is `https://`, and the pipeline sits under
        // the URL's path, before its query.
        let url = url.replacen("libsql://", "https://", 1);
        let (base, query) = url.split_at(url.find('?').unwrap_or(url.len()));
        let base = base.trim_end_matches('/');
        Ok(Some(Self {
            client: hyper::Client::builder().build(connector),
            pipeline_url: Arc::from(format!("{base}/v3/pipeline{query}")),
            auth: Arc::from(format!("Bearer {auth_token}")),
        }))
    }

    /// Every row of `sql`, each value converted as libSQL's `Rows` does.
    ///
    /// # Errors
    /// The statement's error as the server reports it, or a request failure.
    /// Errors read like libSQL's own, so callers classify them the same way.
    pub async fn query(
        &self,
        sql: &str,
        params: impl IntoParams,
    ) -> libsql::Result<Vec<Vec<Value>>> {
        let mut stmt = proto::Stmt::new(sql, true);
        match params.into_params()? {
            Params::None => {}
            Params::Positional(values) => {
                for value in values {
                    stmt.bind(into_proto(value));
                }
            }
            Params::Named(values) => {
                for (name, value) in values {
                    stmt.bind_named(name, into_proto(value));
                }
            }
        }
        let body = serde_json::to_string(&proto::PipelineReqBody {
            baton: None,
            requests: vec![
                proto::StreamRequest::Execute(proto::ExecuteStreamReq { stmt }),
                proto::StreamRequest::Close(proto::CloseStreamReq {}),
            ],
        })
        .map_err(|error| hrana(format!("json error: `{error}`")))?;
        let request = hyper::Request::post(self.pipeline_url.as_ref())
            .header(http::header::AUTHORIZATION, self.auth.as_ref())
            .body(hyper::Body::from(body))
            .map_err(|error| hrana(format!("http error: `{error:?}`")))?;
        let response = self
            .client
            .request(request)
            .await
            .map_err(|error| hrana(format!("http error: `{error}`")))?;
        let status = response.status();
        let bytes = hyper::body::to_bytes(response.into_body())
            .await
            .map_err(|error| hrana(format!("http error: `{error}`")))?;
        if status != http::StatusCode::OK {
            return Err(hrana(format!(
                "api error: `status={status}, body={}`",
                String::from_utf8_lossy(&bytes)
            )));
        }
        let body: proto::PipelineRespBody = serde_json::from_slice(&bytes)
            .map_err(|error| hrana(format!("json error: `{error}`")))?;
        match body.results.into_iter().next() {
            Some(proto::StreamResult::Ok {
                response: proto::StreamResponse::Execute(executed),
            }) => Ok(executed
                .result
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(from_proto).collect())
                .collect()),
            Some(proto::StreamResult::Error { error }) => {
                Err(hrana(format!("stream error: `{error:?}`")))
            }
            other => Err(hrana(format!("unexpected response: `{other:?}`"))),
        }
    }
}

/// libSQL's error for a Hrana failure, with libSQL's message for it.
fn hrana(message: String) -> Error {
    Error::Hrana(message.into())
}

fn into_proto(value: Value) -> proto::Value {
    match value {
        Value::Null => proto::Value::Null,
        Value::Integer(value) => proto::Value::Integer { value },
        Value::Real(value) => proto::Value::Float { value },
        Value::Text(value) => proto::Value::Text {
            value: value.into(),
        },
        Value::Blob(value) => proto::Value::Blob {
            value: value.into(),
        },
    }
}

fn from_proto(value: proto::Value) -> Value {
    match value {
        proto::Value::Null | proto::Value::None => Value::Null,
        proto::Value::Integer { value } => Value::Integer(value),
        proto::Value::Float { value } => Value::Real(value),
        proto::Value::Text { value } => Value::Text(value.to_string()),
        proto::Value::Blob { value } => Value::Blob(value.into()),
    }
}
