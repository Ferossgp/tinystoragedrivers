//! The HTTP transport of remote libSQL connections: TCP, plus TLS for
//! `https://`, over rustls 0.23 with ring and the webpki roots.
//!
//! libSQL's own `tls` feature links `hyper-rustls` 0.25, whose rustls-webpki
//! 0.102 accepts distrusted intermediates (RUSTSEC-2026-0382). Remove this
//! module when libsql ships rustls 0.23.
//!
//! The stream also closes a kept-alive connection that sat idle for
//! [`IDLE_CLOSE`] after a complete response. Turso closes an idle
//! keep-alive connection after 5 s, while libsql builds its hyper 0.14
//! client with the default 90 s pool idle timeout and exposes no knob. A
//! request sent on a pooled connection near the 5 s mark races the server's
//! close and fails with "connection closed before message completed", after
//! the request may have been sent, so it cannot be retried safely. Reporting
//! end of stream to hyper while the connection is idle makes hyper drop it
//! from the pool; the next request opens a fresh connection.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::client::connect::{Connected, Connection};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep};
use tokio_rustls::client::TlsStream;

use super::response::Response;

/// Idle time after a complete response at which this side closes a pooled
/// connection: below Turso's 5 s keep-alive close, by more than a round trip.
///
/// The timer runs only between the end of a response ([`Response`]) and the
/// next request, so a slow or stalled response is never cut. A response the
/// reader cannot frame never arms it; that connection keeps the server-close
/// race this removes.
const IDLE_CLOSE: Duration = Duration::from_secs(3);

/// Opens plain TCP for `http://` URIs and verified TLS for `https://` URIs.
#[derive(Clone)]
pub(crate) struct Connector {
    tls: tokio_rustls::TlsConnector,
}

impl Connector {
    /// A connector that verifies servers against the webpki roots.
    pub(crate) fn new() -> Result<Self, rustls::Error> {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
        Ok(Self {
            tls: tokio_rustls::TlsConnector::from(Arc::new(config)),
        })
    }
}

impl tower_service::Service<http::Uri> for Connector {
    type Response = Stream;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<Stream>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let tls = self.tls.clone();
        Box::pin(async move {
            let secure = match uri.scheme_str() {
                Some("https") => true,
                Some("http") => false,
                _ => return Err(invalid("libsql transport needs an http or https URI")),
            };
            let host = uri
                .host()
                .ok_or_else(|| invalid("libsql transport URI has no host"))?
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_owned();
            let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
            let tcp = TcpStream::connect((host.as_str(), port)).await?;
            tcp.set_nodelay(true)?;
            if !secure {
                return Ok(Stream::new(Io::Plain(tcp)));
            }
            let name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|_| invalid("libsql transport host is not a valid server name"))?;
            Ok(Stream::new(Io::Tls(Box::new(
                tls.connect(name, tcp).await?,
            ))))
        })
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

enum Io {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

/// Where the connection is in hyper's request/response cycle.
enum Phase {
    /// Connected, or a request was written: reading its response, no timer.
    Exchanging(Response),
    /// The response is complete and no request followed: the timer runs.
    Idle,
    /// Idle past [`IDLE_CLOSE`]; reads report end of stream from now on.
    Closed,
}

/// A connected transport stream.
pub(crate) struct Stream {
    io: Io,
    phase: Phase,
    idle: Pin<Box<Sleep>>,
}

impl Stream {
    fn new(io: Io) -> Self {
        Self {
            io,
            phase: Phase::Exchanging(Response::new()),
            idle: Box::pin(tokio::time::sleep(IDLE_CLOSE)),
        }
    }
}

impl Connection for Stream {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if matches!(this.phase, Phase::Closed) {
            return Poll::Ready(Ok(()));
        }
        let before = buf.filled().len();
        let read = match &mut this.io {
            Io::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Io::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        };
        match read {
            Poll::Ready(Ok(())) => {
                if let Phase::Exchanging(response) = &mut this.phase
                    && response.feed(&buf.filled()[before..])
                {
                    this.phase = Phase::Idle;
                    this.idle.as_mut().reset(Instant::now() + IDLE_CLOSE);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Pending
                if matches!(this.phase, Phase::Idle) && this.idle.as_mut().poll(cx).is_ready() =>
            {
                this.phase = Phase::Closed;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = match &mut this.io {
            Io::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Io::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        };
        if matches!(written, Poll::Ready(Ok(n)) if n > 0) && matches!(this.phase, Phase::Idle) {
            this.phase = Phase::Exchanging(Response::new());
        }
        written
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Io::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Io::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
