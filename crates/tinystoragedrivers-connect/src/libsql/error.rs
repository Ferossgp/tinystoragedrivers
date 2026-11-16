//! libSQL error mapping shared by connection setup and primitive engines.

use tinystoragedrivers_types::StorageError;

/// Map a libSQL failure without including query or data contents.
///
/// A locked database and a failed request (transport, timeout, server
/// unavailable) are retryable; every other failure is a backend error.
#[must_use]
pub fn map(operation: &'static str, error: libsql::Error) -> StorageError {
    if busy(&error) {
        StorageError::unavailable(format!("libsql is busy during {operation}")).with_source(error)
    } else if transport(&error) {
        StorageError::unavailable(format!("libsql is unreachable during {operation}"))
            .with_source(error)
    } else {
        StorageError::backend(format!("libsql failed during {operation}")).with_source(error)
    }
}

/// Bind the operation name for `map_err`.
pub fn during(operation: &'static str) -> impl Fn(libsql::Error) -> StorageError {
    move |error| map(operation, error)
}

/// Map a failed commit request. When the request may have been applied the
/// error is a non-retryable backend error that says so, because repeating a
/// write whose outcome is unknown could apply it twice.
#[must_use]
pub fn commit_failed(operation: &'static str, error: libsql::Error) -> StorageError {
    if transport(&error) {
        unknown_outcome(operation).with_source(error)
    } else {
        map(operation, error)
    }
}

/// The error of a commit whose outcome is unknown.
pub(crate) fn unknown_outcome(operation: &'static str) -> StorageError {
    StorageError::backend(format!(
        "libsql commit outcome is unknown during {operation}"
    ))
}

/// Whether the database refused the statement because it was locked.
fn busy(error: &libsql::Error) -> bool {
    const BUSY: i32 = 5;
    const LOCKED: i32 = 6;
    match error {
        libsql::Error::SqliteFailure(code, _) | libsql::Error::RemoteSqliteFailure(code, _, _) => {
            matches!(code & 0xff, BUSY | LOCKED)
        }
        libsql::Error::Hrana(inner) => {
            let message = inner.to_string();
            message.contains("SQLITE_BUSY") || message.contains("database is locked")
        }
        _ => false,
    }
}

/// Whether the request failed in transit rather than being rejected by the
/// server. libSQL keeps its Hrana error type private, so the class is read
/// from the message: a "stream error" is a statement the server rejected, an
/// "api error" without a status is a batch step the server rejected, and a
/// 4xx status is a request the server refused. Any other Hrana failure
/// (transport, closed stream, 5xx from the edge) may have happened after the
/// server applied the request.
fn transport(error: &libsql::Error) -> bool {
    match error {
        libsql::Error::ConnectionFailed(_) => true,
        libsql::Error::Hrana(inner) => {
            let message = inner.to_string();
            if let Some(status) = message.strip_prefix("api error: `status=") {
                return !status.starts_with('4');
            }
            !(message.starts_with("stream error") || message.starts_with("api error"))
        }
        _ => false,
    }
}

/// Whether the server refused a request because it no longer knows the
/// stream the request named, so it ran nothing: Turso answers `404 stream not
/// found`, sqld `400 Received an invalid baton` for a closed stream and
/// `STREAM_EXPIRED` for an expired one.
pub(crate) fn stream_lost(error: &libsql::Error) -> bool {
    let libsql::Error::Hrana(inner) = error else {
        return false;
    };
    let message = inner.to_string();
    message.starts_with("api error: `status=4")
        && ["stream not found", "invalid baton", "STREAM_EXPIRED"]
            .iter()
            .any(|reason| message.contains(reason))
}
