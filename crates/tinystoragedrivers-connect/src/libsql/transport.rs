//! The HTTP transport of remote libSQL connections: TCP, plus TLS for
//! `https://`, over rustls 0.23 with ring and the webpki roots.
//!
//! libSQL's own `tls` feature links `hyper-rustls` 0.25, whose rustls-webpki
//! 0.102 accepts distrusted intermediates (RUSTSEC-2026-0382). Remove this
//! module when libsql ships rustls 0.23.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::client::connect::{Connected, Connection};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

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
                return Ok(Stream::Plain(tcp));
            }
            let name = rustls::pki_types::ServerName::try_from(host)
                .map_err(|_| invalid("libsql transport host is not a valid server name"))?;
            Ok(Stream::Tls(Box::new(tls.connect(name, tcp).await?)))
        })
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// A connected transport stream.
pub(crate) enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
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
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}
