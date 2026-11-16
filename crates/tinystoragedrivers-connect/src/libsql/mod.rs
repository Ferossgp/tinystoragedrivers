//! Shared libSQL database handles: a remote database over Hrana (HTTP) or a
//! local file, holding the same layout-2 tables as the SQLite handle.
//!
//! The `libsql` feature links libSQL's own SQLite build, which exports the
//! same C symbols as the `sqlite` feature's bundled SQLite: the two features
//! cannot link into one binary, so enable one of them per build.
//!
//! A remote statement costs a network round trip, and the server aborts a
//! transaction that stays open for five seconds. The engines therefore send
//! each transaction's writes in as few requests as possible, with the commit
//! where they can ([`Tx`]), and never keep a transaction open across
//! anything but statements. A remote server rejects `PRAGMA journal_mode`,
//! `busy_timeout`, `auto_vacuum` and `synchronous`, so none is issued, and
//! there is no incremental vacuum.

mod batch;
pub mod error;
mod read;
mod response;
mod schema;
mod transport;
mod tx;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use batch::{Statement, transactional_batch};
pub use libsql;
pub use read::Reader;
use tinystoragedrivers_types::{CollectionSpec, Result};
pub use tx::{Row, Tx, query};

use crate::{Clock, SqliteTables};

/// How long a local connection waits for another writer's lock.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Open one connection to the database at `url`.
///
/// `libsql://`, `https://` and `http://` URLs name a remote database and use
/// `auth_token` (an empty token for a local `sqld` without auth). `file:`
/// URLs name a local database file, which is created if absent; the token is
/// ignored.
///
/// # Errors
/// Returns a libSQL error when the URL is unsupported or the database cannot
/// be opened.
pub async fn connect(url: &str, auth_token: &str) -> libsql::Result<libsql::Connection> {
    if let Some(path) = url.strip_prefix("file:") {
        let path = path.strip_prefix("//").unwrap_or(path);
        let conn = libsql::Builder::new_local(path).build().await?.connect()?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        return Ok(conn);
    }
    if is_remote(url) {
        let connector = transport::Connector::new()
            .map_err(|error| libsql::Error::ConnectionFailed(error.to_string()))?;
        return libsql::Builder::new_remote(url.to_owned(), auth_token.to_owned())
            .connector(connector)
            .build()
            .await?
            .connect();
    }
    Err(libsql::Error::Misuse(
        "libsql URL must start with libsql://, https://, http:// or file:".to_owned(),
    ))
}

fn is_remote(url: &str) -> bool {
    ["libsql://", "https://", "http://"]
        .iter()
        .any(|scheme| url.starts_with(scheme))
}

/// Put `conn` back outside any transaction, on a fresh server stream.
///
/// Call it after a failed request and before reusing a connection that a
/// cancelled caller may have left behind. libSQL 0.10 keeps a remote stream's
/// baton when a request fails or its answer never arrives, and sends every
/// later request with it. Once the server has moved on (it applied the lost
/// request, or dropped the stream), each of those requests fails with
/// "stream not found" until the stream is reset. libSQL also keeps the
/// transaction state of the last answered request, so it can report an open
/// transaction that is gone.
///
/// An open transaction is rolled back first; a rollback that fails or stalls
/// leaves it to the server, which rolls back a stream it no longer hears
/// from. The reset itself is local: it forgets the baton, sends nothing, and
/// does nothing on a local file. Never reset a healthy connection: a read
/// leaves its stream open for the next request to reuse and close, and a
/// forgotten stream holds a server connection until it expires.
pub async fn recover(conn: &libsql::Connection) {
    if !conn.is_autocommit() {
        let _ = tokio::time::timeout(BUSY_TIMEOUT, conn.execute("ROLLBACK", ())).await;
    }
    conn.reset().await;
}

/// One libSQL connection shared by every primitive engine of a database.
///
/// Statements of all clones run one at a time, in order, because a
/// transaction is a property of the connection. Collection declarations are
/// cached after their first read: a declaration only ever grows, and this
/// handle refreshes its cache whenever it declares one. A declaration
/// another process adds is seen after the next open.
#[derive(Clone)]
pub struct LibsqlConnection {
    inner: Arc<Inner>,
}

struct Inner {
    conn: tokio::sync::Mutex<libsql::Connection>,
    tables: SqliteTables,
    clock: Clock,
    specs: Mutex<HashMap<String, CollectionSpec>>,
    location: String,
    reader: Option<Reader>,
}

/// The connection of a [`LibsqlConnection`] while [`LibsqlConnection::lock`]
/// holds it, with the database's one-request [`Reader`] for [`query`].
pub struct Locked<'a> {
    conn: tokio::sync::MutexGuard<'a, libsql::Connection>,
    reader: Option<&'a Reader>,
}

impl std::ops::Deref for Locked<'_> {
    type Target = libsql::Connection;

    fn deref(&self) -> &libsql::Connection {
        &self.conn
    }
}

impl fmt::Debug for Locked<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Locked").finish_non_exhaustive()
    }
}

impl fmt::Debug for LibsqlConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LibsqlConnection")
            .field("location", &self.inner.location)
            .finish_non_exhaustive()
    }
}

impl LibsqlConnection {
    /// Open the database at `url` (see [`connect`]) and create or validate
    /// the layout-2 tables, each named with `table_prefix` in front.
    ///
    /// # Errors
    /// Returns invalid input for a prefix other than at most 64 ASCII
    /// letters, digits and underscores, or for existing tables of another
    /// shape; otherwise a libSQL error.
    pub async fn open(url: &str, auth_token: &str, table_prefix: &str) -> Result<Self> {
        Self::open_with_clock(url, auth_token, table_prefix, Arc::new(crate::system_clock)).await
    }

    /// As [`Self::open`], using `clock` for document and blob expiries and
    /// stream timestamps.
    ///
    /// # Errors
    /// As [`Self::open`].
    pub async fn open_with_clock(
        url: &str,
        auth_token: &str,
        table_prefix: &str,
        clock: Clock,
    ) -> Result<Self> {
        let tables = SqliteTables::with_prefix(table_prefix)?;
        let conn = connect(url, auth_token)
            .await
            .map_err(error::during("open"))?;
        schema::ensure(&tables, &conn).await?;
        // Credentials never ride in the URL, but a query string might.
        let location = url.split('?').next().unwrap_or_default().to_owned();
        let reader = Reader::open(url, auth_token).map_err(error::during("open"))?;
        Ok(Self {
            inner: Arc::new(Inner {
                conn: tokio::sync::Mutex::new(conn),
                tables,
                clock,
                specs: Mutex::new(HashMap::new()),
                location,
                reader,
            }),
        })
    }

    /// Exclusive use of the connection, outside any transaction, until the
    /// guard drops. Hold it only while statements run. A caller cancelled
    /// inside a [`Tx`] leaves its transaction open; the next caller recovers
    /// the connection first (see [`recover`]). A failed request recovers in
    /// [`query`] and [`Tx::finish`].
    pub async fn lock(&self) -> Locked<'_> {
        let conn = self.inner.conn.lock().await;
        if !conn.is_autocommit() {
            recover(&conn).await;
        }
        Locked {
            conn,
            reader: self.inner.reader.as_ref(),
        }
    }

    /// Return the injected clock shared by all primitive adapters.
    #[must_use]
    pub fn clock(&self) -> Clock {
        Arc::clone(&self.inner.clock)
    }

    /// The current time of the injected clock, as stored in the tables.
    #[must_use]
    pub fn now_ms(&self) -> i64 {
        i64::try_from((self.inner.clock)()).unwrap_or(i64::MAX)
    }

    /// Return names for the generic tables shared by the primitive engines.
    #[must_use]
    pub fn tables(&self) -> &SqliteTables {
        &self.inner.tables
    }

    /// The cached declaration of `collection`, if one was read or declared
    /// through this handle.
    #[must_use]
    pub fn cached_spec(&self, collection: &str) -> Option<CollectionSpec> {
        self.inner
            .specs
            .lock()
            .ok()
            .and_then(|specs| specs.get(collection).cloned())
    }

    /// Remember the stored declaration of a collection.
    pub fn cache_spec(&self, spec: CollectionSpec) {
        if let Ok(mut specs) = self.inner.specs.lock() {
            specs.insert(spec.name.clone(), spec);
        }
    }

    /// Forget a collection's declaration, so the next use reads it again.
    pub fn forget_spec(&self, collection: &str) {
        if let Ok(mut specs) = self.inner.specs.lock() {
            specs.remove(collection);
        }
    }
}
