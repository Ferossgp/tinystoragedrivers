//! Where an HTTP/1.1 response ends, read from its bytes alone.
//!
//! The transport closes a pooled connection only after a whole response, so
//! it has to know where one ends: after the head for `204`/`304`, after
//! `Content-Length` bytes, or after the last chunk and its trailer section.
//! A response it cannot frame never counts as complete. It assumes one
//! response per request and no `HEAD` requests: hyper 0.14 does not pipeline
//! and libsql only sends `POST`/`GET`.

/// The longest head or line accepted; longer means the framing is unknown.
const MAX_HEAD: usize = 64 * 1024;

/// Progress through one HTTP/1.1 response.
pub(super) enum Response {
    /// The status line and headers.
    Head(Vec<u8>),
    /// The bytes left of a `Content-Length` body.
    Body(u64),
    /// A chunk-size line.
    ChunkSize(Vec<u8>),
    /// The bytes left of a chunk's data.
    ChunkData(u64),
    /// The bytes left of the CRLF after a chunk's data.
    ChunkEnd(u64),
    /// The current line of the trailer section after the last chunk.
    Trailer(Vec<u8>),
    /// The whole response was read.
    Complete,
    /// A response this reader cannot frame: it never completes.
    Unframed,
}

impl Response {
    /// A response whose first byte has not arrived.
    pub(super) fn new() -> Self {
        Self::Head(Vec::new())
    }

    /// Consume the next response bytes; true once the response is complete.
    pub(super) fn feed(&mut self, mut bytes: &[u8]) -> bool {
        while !bytes.is_empty() {
            let next = match self {
                Self::Complete | Self::Unframed => break,
                Self::Body(left) => {
                    if !skip(left, &mut bytes) {
                        break;
                    }
                    Self::Complete
                }
                Self::ChunkData(left) => {
                    if !skip(left, &mut bytes) {
                        break;
                    }
                    Self::ChunkEnd(2)
                }
                Self::ChunkEnd(left) => {
                    if !skip(left, &mut bytes) {
                        break;
                    }
                    Self::ChunkSize(Vec::new())
                }
                Self::Head(head) => match line(head, &mut bytes, b"\r\n\r\n") {
                    Line::Done => after_head(head),
                    Line::Partial => break,
                    Line::TooLong => Self::Unframed,
                },
                Self::ChunkSize(size) => match line(size, &mut bytes, b"\r\n") {
                    Line::Done => after_chunk_size(size),
                    Line::Partial => break,
                    Line::TooLong => Self::Unframed,
                },
                Self::Trailer(field) => match line(field, &mut bytes, b"\r\n") {
                    Line::Done if field.len() == 2 => Self::Complete,
                    Line::Done => Self::Trailer(Vec::new()),
                    Line::Partial => break,
                    Line::TooLong => Self::Unframed,
                },
            };
            *self = next;
        }
        matches!(self, Self::Complete)
    }
}

/// Skip up to `left` bytes; true once none are left.
fn skip(left: &mut u64, bytes: &mut &[u8]) -> bool {
    let n = usize::try_from(*left).map_or(bytes.len(), |left| left.min(bytes.len()));
    *bytes = &bytes[n..];
    *left -= n as u64;
    *left == 0
}

enum Line {
    Done,
    Partial,
    TooLong,
}

/// Move bytes into `buf` until it ends with `end`.
fn line(buf: &mut Vec<u8>, bytes: &mut &[u8], end: &[u8]) -> Line {
    while let Some((&byte, rest)) = bytes.split_first() {
        *bytes = rest;
        buf.push(byte);
        if buf.ends_with(end) {
            return Line::Done;
        }
        if buf.len() > MAX_HEAD {
            return Line::TooLong;
        }
    }
    Line::Partial
}

fn after_head(head: &[u8]) -> Response {
    let head = String::from_utf8_lossy(head);
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|status| status.split(' ').nth(1))
        .and_then(|code| code.parse::<u16>().ok());
    let mut length = None;
    let mut chunked = false;
    for field in lines {
        let Some((name, value)) = field.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = value
                .split(',')
                .any(|coding| coding.trim().eq_ignore_ascii_case("chunked"));
        } else if name.eq_ignore_ascii_case("content-length") {
            length = value.trim().parse::<u64>().ok();
        }
    }
    match status {
        Some(100..=199) => Response::new(),
        Some(204 | 304) => Response::Complete,
        Some(_) if chunked => Response::ChunkSize(Vec::new()),
        Some(_) => match length {
            Some(0) => Response::Complete,
            Some(length) => Response::Body(length),
            None => Response::Unframed,
        },
        None => Response::Unframed,
    }
}

fn after_chunk_size(line: &[u8]) -> Response {
    let line = String::from_utf8_lossy(line);
    let size = line.trim_end().split(';').next().unwrap_or_default().trim();
    match u64::from_str_radix(size, 16) {
        Ok(0) => Response::Trailer(Vec::new()),
        Ok(size) => Response::ChunkData(size),
        Err(_) => Response::Unframed,
    }
}
