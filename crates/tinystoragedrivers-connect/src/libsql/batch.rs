//! Parameterized transactional batches in one round trip.
//!
//! `libsql` sends a transactional batch as SQL text only, so the parameters of
//! each statement are inlined as literals before the batch is sent. This is
//! security-critical, so the rules are strict:
//!
//! - Only parameter values are inlined, never identifiers: the statement text
//!   is the caller's static SQL and is copied unchanged except at `?`/`?NNN`
//!   placeholders outside string literals, quoted identifiers and comments.
//! - Every literal is built from the value's bytes alone, so no value can
//!   change the statement's structure: text is `CAST(X'<hex of UTF-8>' AS
//!   TEXT)`, a blob is `X'<hex>'`, an integer is decimal (`i64::MIN` as
//!   `(-9223372036854775807-1)`), NULL is `NULL`, and a real is the shortest
//!   decimal that reads back to the same `f64`.
//! - NaN and infinities have no SQL literal and are refused with an error.
//! - Named placeholders, a placeholder without a value and a value without a
//!   placeholder are refused with an error.

use std::borrow::Cow;
use std::fmt::Write as _;

use libsql::params::{IntoParams, Params};
use libsql::{Connection, Error, Value};

/// One statement of a batch: SQL text with `?` or `?NNN` placeholders and the
/// values bound to them.
#[derive(Debug, Clone)]
pub struct Statement {
    sql: Cow<'static, str>,
    params: Vec<Value>,
}

impl Statement {
    /// A statement with positional parameters.
    ///
    /// # Errors
    /// Returns a misuse error for named parameters or a value that does not
    /// convert.
    pub fn new(sql: impl Into<Cow<'static, str>>, params: impl IntoParams) -> libsql::Result<Self> {
        let params = match params.into_params()? {
            Params::None => Vec::new(),
            Params::Positional(values) => values,
            Params::Named(_) => {
                return Err(Error::Misuse(
                    "batch statements take positional parameters only".to_owned(),
                ));
            }
        };
        Ok(Self {
            sql: sql.into(),
            params,
        })
    }

    /// The statement text with every parameter inlined as a literal.
    ///
    /// # Errors
    /// Returns a misuse error when a placeholder has no value, a value has no
    /// placeholder, or the text uses a named placeholder.
    pub fn render(&self) -> libsql::Result<String> {
        inline(&self.sql, &self.params)
    }

    /// The statement text with its placeholders.
    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    /// The values bound to the placeholders, in order.
    pub(crate) fn params(&self) -> &[Value] {
        &self.params
    }
}

/// Run `statements` as one transaction in one request: all of them commit, or
/// none does.
///
/// # Errors
/// Returns the first failing statement's error; nothing is committed then.
/// A transport error leaves the outcome unknown to the caller.
pub async fn transactional_batch(
    conn: &Connection,
    statements: &[Statement],
) -> libsql::Result<libsql::BatchRows> {
    let mut sql = String::new();
    for statement in statements {
        sql.push_str(&statement.render()?);
        sql.push_str(";\n");
    }
    conn.execute_transactional_batch(&sql).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lexeme {
    Code,
    Quoted(char),
    LineComment,
    BlockComment,
}

fn inline(sql: &str, params: &[Value]) -> libsql::Result<String> {
    let mut out = String::with_capacity(sql.len() + params.len() * 16);
    let mut used = vec![false; params.len()];
    let mut next_positional = 0usize;
    let mut state = Lexeme::Code;
    let mut chars = sql.char_indices().peekable();
    while let Some((_, c)) = chars.next() {
        match state {
            Lexeme::Code => match c {
                '\'' | '"' | '`' => {
                    state = Lexeme::Quoted(c);
                    out.push(c);
                }
                '[' => {
                    state = Lexeme::Quoted(']');
                    out.push(c);
                }
                '-' if chars.peek().is_some_and(|(_, next)| *next == '-') => {
                    state = Lexeme::LineComment;
                    out.push(c);
                }
                '/' if chars.peek().is_some_and(|(_, next)| *next == '*') => {
                    chars.next();
                    state = Lexeme::BlockComment;
                    out.push_str("/*");
                }
                '?' => {
                    let mut digits = String::new();
                    while let Some((_, digit)) = chars.peek().filter(|(_, d)| d.is_ascii_digit()) {
                        digits.push(*digit);
                        chars.next();
                    }
                    let index = if digits.is_empty() {
                        next_positional
                    } else {
                        digits
                            .parse::<usize>()
                            .ok()
                            .and_then(|n| n.checked_sub(1))
                            .ok_or_else(|| misuse("placeholder ?0 is invalid"))?
                    };
                    next_positional = index + 1;
                    let value = params
                        .get(index)
                        .ok_or_else(|| misuse("placeholder has no bound value"))?;
                    used[index] = true;
                    literal(value, &mut out)?;
                }
                ':' | '@' | '$'
                    if chars
                        .peek()
                        .is_some_and(|(_, next)| next.is_ascii_alphabetic() || *next == '_') =>
                {
                    return Err(misuse("batch statements take positional parameters only"));
                }
                _ => out.push(c),
            },
            Lexeme::Quoted(close) => {
                out.push(c);
                if c == close {
                    state = Lexeme::Code;
                }
            }
            Lexeme::LineComment => {
                out.push(c);
                if c == '\n' {
                    state = Lexeme::Code;
                }
            }
            Lexeme::BlockComment => {
                out.push(c);
                if c == '*' && chars.peek().is_some_and(|(_, next)| *next == '/') {
                    chars.next();
                    out.push('/');
                    state = Lexeme::Code;
                }
            }
        }
    }
    if used.iter().any(|used| !used) {
        return Err(misuse("a bound value has no placeholder"));
    }
    Ok(out)
}

fn literal(value: &Value, out: &mut String) -> libsql::Result<()> {
    match value {
        Value::Null => out.push_str("NULL"),
        Value::Integer(i64::MIN) => out.push_str("(-9223372036854775807-1)"),
        Value::Integer(n) => {
            let _ = write!(out, "{n}");
        }
        Value::Real(x) if !x.is_finite() => {
            return Err(misuse("NaN and infinite reals have no SQL literal"));
        }
        // `{:?}` prints the shortest text that reads back to the same `f64`
        // and always keeps a `.` or an exponent, so SQLite parses a REAL.
        Value::Real(x) => {
            let _ = write!(out, "{x:?}");
        }
        Value::Text(text) => {
            out.push_str("CAST(");
            hex(text.as_bytes(), out);
            out.push_str(" AS TEXT)");
        }
        Value::Blob(bytes) => hex(bytes, out),
    }
    Ok(())
}

fn hex(bytes: &[u8], out: &mut String) {
    out.push_str("X'");
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out.push('\'');
}

fn misuse(message: &str) -> Error {
    Error::Misuse(message.to_owned())
}
