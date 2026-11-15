//! Cloneable shared connections used by storage primitive engines.
//!
//! This crate owns connection setup and engine-neutral connection handles. It
//! does not implement storage operations or choose a primitive engine.

use std::sync::Arc;

/// Clock injected into local database handles, in Unix epoch milliseconds.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// The wall clock, in Unix epoch milliseconds.
#[cfg(any(feature = "sqlite", feature = "libsql"))]
fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(any(feature = "sqlite", feature = "libsql"))]
mod layout;
#[cfg(any(feature = "sqlite", feature = "libsql"))]
pub use layout::SqliteTables;

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteConnection;

#[cfg(feature = "libsql")]
pub mod libsql;
#[cfg(feature = "libsql")]
pub use libsql::LibsqlConnection;

#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mongodb")]
pub use mongodb::MongoConnection;

#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
pub use s3::{S3Connection, S3ConnectionConfig};

#[cfg(feature = "file")]
mod file;
#[cfg(feature = "file")]
pub use file::FileConnection;

#[cfg(feature = "amqp")]
mod amqp;
#[cfg(feature = "amqp")]
pub use amqp::AmqpConnection;
