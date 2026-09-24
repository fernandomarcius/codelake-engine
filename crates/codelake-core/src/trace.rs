//! Distributed tracing: the vendor-neutral half.
//!
//! The core emits plain [`tracing`] spans under the target [`TARGET`] and knows
//! nothing about OpenTelemetry. The server installs a [`TraceBackend`] that
//! bridges those spans to OTLP; without one (tests, `check-config`, telemetry
//! disabled) every function here is a cheap no-op apart from the span itself.
//!
//! What this module provides:
//!
//! - [`TraceParent`]: W3C Trace Context `traceparent` parsing and rendering.
//! - [`QueryTrace`]: what a client protocol hands to the engine per query:
//!   remote parent, `tracestate`, query id and a client correlation id
//!   (`X-Trino-Trace-Token`). Build it with [`QueryTrace::from_headers`].
//! - SQL redaction ([`redact_sql`]) and fingerprinting ([`sql_hash`]): SQL is
//!   recorded with every literal replaced by `?`, so traces never carry the
//!   values a query filtered on.
//! - [`TracedStream`]: a record-batch stream that runs inside a span, counts
//!   rows and closes the span when the stream ends.
//! - Propagation of the current span into tasks `DataFusion` spawns
//!   ([`install_task_propagation`]), so spans created by sources during
//!   execution stay children of the query.
//!
//! Span names and attributes follow the OpenTelemetry database semantic
//! conventions where one exists (`db.query.text`, `db.response.returned_rows`,
//! `enduser.id`, `server.address`); `CodeLake`-specific ones are prefixed
//! `codelake.`.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::{JoinSetTracer, set_join_set_tracer};
use datafusion::error::Result as DfResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Whitespace};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use tracing::{Instrument, Span};

/// `tracing` target of every span meant for distributed tracing. The server
/// exports exactly this target over OTLP; logs keep their own filter.
pub const TARGET: &str = "codelake::trace";

/// Field names shared by spans across crates.
pub mod field {
    pub const STATUS_CODE: &str = "otel.status_code";
    pub const STATUS_DESCRIPTION: &str = "otel.status_description";
    pub const ROWS: &str = "db.response.returned_rows";
    pub const QUERY_TEXT: &str = "db.query.text";
    pub const SQL_HASH: &str = "codelake.sql.hash";
    pub const CANCELLED: &str = "codelake.cancelled";
    pub const TRACE_ID: &str = "codelake.trace_id";
}

/// Longest `tracestate` passed on (W3C: at least 512 characters must be kept).
const MAX_TRACESTATE: usize = 512;
/// Longest client correlation id recorded.
const MAX_CORRELATION_ID: usize = 256;
/// Longest SQL text recorded on a span.
const MAX_SQL_TEXT: usize = 8 * 1024;

// ---------------------------------------------------------------------------
// W3C traceparent
// ---------------------------------------------------------------------------

/// A W3C Trace Context `traceparent` value:
/// `version-traceid-parentid-flags`, e.g.
/// `00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceParent {
    pub trace_id: [u8; 16],
    /// Span id of the caller's span: the parent of the query span.
    pub parent_id: [u8; 8],
    pub flags: u8,
}

impl fmt::Debug for TraceParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceParent({self})")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TraceParentError {
    #[error("traceparent must be `version-traceid-parentid-flags` in lower-case hex")]
    Malformed,
    #[error("traceparent version ff is invalid")]
    InvalidVersion,
    #[error("traceparent trace id is all zeros")]
    ZeroTraceId,
    #[error("traceparent parent id is all zeros")]
    ZeroParentId,
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Decodes lower-case hex (the only case W3C allows) into `out`.
fn decode_hex(text: &str, out: &mut [u8]) -> Option<()> {
    let bytes = text.as_bytes();
    if bytes.len() != out.len() * 2 {
        return None;
    }
    for (slot, pair) in out.iter_mut().zip(bytes.as_chunks::<2>().0) {
        *slot = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Some(())
}

fn encode_hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

impl TraceParent {
    pub const SAMPLED: u8 = 0x01;

    /// Parses a header value following the W3C rules: lower-case hex only,
    /// version `ff` and all-zero ids are invalid, and a *future* version may
    /// carry extra fields after the four known ones (they are ignored).
    pub fn parse(header: &str) -> Result<Self, TraceParentError> {
        let value = header.trim_matches([' ', '\t']);
        if value.len() < 55 || !value.is_ascii() {
            return Err(TraceParentError::Malformed);
        }
        let (known, rest) = value.split_at(55);
        let mut parts = known.split('-');
        let (Some(version), Some(trace), Some(parent), Some(flags), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(TraceParentError::Malformed);
        };
        let mut version_byte = [0_u8; 1];
        decode_hex(version, &mut version_byte).ok_or(TraceParentError::Malformed)?;
        if version_byte[0] == 0xff {
            return Err(TraceParentError::InvalidVersion);
        }
        // Version 00 has exactly four fields; later versions may append more.
        if !rest.is_empty() && (version_byte[0] == 0 || !rest.starts_with('-')) {
            return Err(TraceParentError::Malformed);
        }
        let mut parsed = Self {
            trace_id: [0; 16],
            parent_id: [0; 8],
            flags: 0,
        };
        decode_hex(trace, &mut parsed.trace_id).ok_or(TraceParentError::Malformed)?;
        decode_hex(parent, &mut parsed.parent_id).ok_or(TraceParentError::Malformed)?;
        let mut flag_byte = [0_u8; 1];
        decode_hex(flags, &mut flag_byte).ok_or(TraceParentError::Malformed)?;
        parsed.flags = flag_byte[0];
        if parsed.trace_id == [0; 16] {
            return Err(TraceParentError::ZeroTraceId);
        }
        if parsed.parent_id == [0; 8] {
            return Err(TraceParentError::ZeroParentId);
        }
        Ok(parsed)
    }

    pub fn is_sampled(&self) -> bool {
        self.flags & Self::SAMPLED != 0
    }

    /// Trace id as 32 lower-case hex characters, as tracing backends show it.
    pub fn trace_id_hex(&self) -> String {
        encode_hex(&self.trace_id)
    }

    pub fn parent_id_hex(&self) -> String {
        encode_hex(&self.parent_id)
    }
}

impl fmt::Display for TraceParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "00-{}-{}-{:02x}",
            self.trace_id_hex(),
            self.parent_id_hex(),
            self.flags
        )
    }
}

impl FromStr for TraceParent {
    type Err = TraceParentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

// ---------------------------------------------------------------------------
// Per-query trace context
// ---------------------------------------------------------------------------

/// Trace context of one query, supplied by the client protocol.
///
/// The default (everything `None`) starts a new trace, subject to sampling,
/// with an engine-generated query id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryTrace {
    /// Caller's span (W3C `traceparent`): the query span becomes its child.
    pub parent: Option<TraceParent>,
    /// W3C `tracestate`, passed on untouched. Ignored without `parent`.
    pub tracestate: Option<String>,
    /// Query id assigned by the protocol (Trino's `queryId`). When `None`,
    /// the engine assigns one with [`next_query_id`].
    pub query_id: Option<String>,
    /// Opaque client correlation id, e.g. Trino's `X-Trino-Trace-Token` or a
    /// PostgreSQL session id. Recorded as `codelake.correlation_id`.
    pub correlation_id: Option<String>,
}

impl QueryTrace {
    /// Builds the context from protocol headers. Meant to be called by the
    /// Trino protocol (`POST /v1/statement`) with the raw values of
    /// `traceparent`, `tracestate` and `X-Trino-Trace-Token`.
    ///
    /// Follows W3C Trace Context: an invalid `traceparent` is ignored (the
    /// query starts a new trace) and so is its `tracestate`. The trace token
    /// is kept only when it is printable ASCII of at most 256 characters, so a
    /// client cannot inflate every span it touches.
    pub fn from_headers(
        traceparent: Option<&str>,
        tracestate: Option<&str>,
        trace_token: Option<&str>,
    ) -> Self {
        let parent = traceparent.and_then(|value| TraceParent::parse(value).ok());
        let tracestate = parent.and(tracestate).and_then(|state| {
            let state = state.trim();
            let valid = !state.is_empty()
                && state.len() <= MAX_TRACESTATE
                && state.bytes().all(|b| (0x20..0x7f).contains(&b));
            valid.then(|| state.to_owned())
        });
        Self {
            parent,
            tracestate,
            query_id: None,
            correlation_id: trace_token.and_then(sanitize_correlation_id),
        }
    }

    #[must_use]
    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = Some(query_id.into());
        self
    }

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: &str) -> Self {
        self.correlation_id = sanitize_correlation_id(correlation_id);
        self
    }
}

fn sanitize_correlation_id(value: &str) -> Option<String> {
    let value = value.trim();
    let valid = !value.is_empty()
        && value.len() <= MAX_CORRELATION_ID
        && value.bytes().all(|b| (0x21..0x7f).contains(&b));
    valid.then(|| value.to_owned())
}

// ---------------------------------------------------------------------------
// Query ids
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Lower-case base-32 digits Trino uses for the coordinator part of a query id.
const ID_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Five characters identifying this process in query ids.
fn node_tag() -> &'static str {
    static TAG: OnceLock<String> = OnceLock::new();
    TAG.get_or_init(|| {
        use std::hash::{BuildHasher, Hasher};
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        let mut bits = hasher.finish();
        (0..5)
            .map(|_| {
                let digit = ID_ALPHABET[usize::from(u8::try_from(bits & 31).unwrap_or(0))];
                bits >>= 5;
                char::from(digit)
            })
            .collect()
    })
}

/// Formats a query id in Trino's shape, `YYYYMMDD_HHMMSS_NNNNN_xxxxx` (UTC),
/// so tools that parse Trino ids keep working.
fn format_query_id(unix_secs: u64, sequence: u32, node: &str) -> String {
    let secs = i64::try_from(unix_secs).unwrap_or(i64::MAX);
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let of_day = secs.rem_euclid(86_400);
    format!(
        "{year:04}{month:02}{day:02}_{:02}{:02}{:02}_{:05}_{node}",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60,
        sequence % 100_000
    )
}

/// A new query id, unique within this process.
pub fn next_query_id() -> String {
    static SEQUENCE: AtomicU32 = AtomicU32::new(0);
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    format_query_id(now, sequence, node_tag())
}

// ---------------------------------------------------------------------------
// Backend (installed by the server)
// ---------------------------------------------------------------------------

/// Bridge from `tracing` spans to a tracing system. The server implements it
/// for OpenTelemetry; the core only calls it.
pub trait TraceBackend: Send + Sync + fmt::Debug + 'static {
    /// Makes `parent` (a remote span) the parent of `span`. Called right after
    /// the span is created, before it is entered.
    fn set_remote_parent(&self, span: &Span, parent: &TraceParent, tracestate: Option<&str>);

    /// Trace id of `span` as 32 hex characters, if it is being traced.
    fn trace_id(&self, span: &Span) -> Option<String>;
}

static BACKEND: OnceLock<Box<dyn TraceBackend>> = OnceLock::new();

/// Installs the process-wide backend. Returns `false` if one was already
/// installed (the first one stays).
pub fn install_backend(backend: Box<dyn TraceBackend>) -> bool {
    BACKEND.set(backend).is_ok()
}

/// See [`TraceBackend::set_remote_parent`]. No-op without a backend.
pub fn set_remote_parent(span: &Span, parent: &TraceParent, tracestate: Option<&str>) {
    if let Some(backend) = BACKEND.get() {
        backend.set_remote_parent(span, parent, tracestate);
    }
}

/// See [`TraceBackend::trace_id`]. `None` without a backend.
pub fn trace_id(span: &Span) -> Option<String> {
    BACKEND.get().and_then(|backend| backend.trace_id(span))
}

// ---------------------------------------------------------------------------
// Span propagation into tasks spawned by DataFusion
// ---------------------------------------------------------------------------

/// Runs futures and closures that `DataFusion` spawns (repartitioning,
/// parallel scans) inside the span that was current when they were spawned.
#[derive(Debug)]
struct CurrentSpanTracer;

impl JoinSetTracer for CurrentSpanTracer {
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        fut.in_current_span().boxed()
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        let span = Span::current();
        Box::new(move || span.in_scope(f))
    }
}

static TASK_TRACER: CurrentSpanTracer = CurrentSpanTracer;

/// Installs span propagation for `DataFusion`'s spawned tasks. Idempotent;
/// if another tracer was installed first, it is kept.
pub fn install_task_propagation() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        if set_join_set_tracer(&TASK_TRACER).is_err() {
            tracing::debug!("a DataFusion join-set tracer was already installed; keeping it");
        }
    });
}

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

/// `sql` with every literal (strings, numbers, hex/byte strings, dollar
/// quoting) replaced by `?` and comments removed. Identifiers, keywords and
/// operators are kept, so the text still shows the query's shape.
///
/// Returns `None` when `sql` cannot be tokenized: the caller then records no
/// text at all rather than risking a literal.
pub fn redact_sql(sql: &str, dialect: &dyn Dialect) -> Option<String> {
    let tokens = Tokenizer::new(dialect, sql).tokenize().ok()?;
    let mut out = String::with_capacity(sql.len().min(MAX_SQL_TEXT));
    for token in &tokens {
        match token {
            Token::EOF => {}
            Token::Word(_) | Token::Placeholder(_) => out.push_str(&token.to_string()),
            Token::Whitespace(Whitespace::Space | Whitespace::Newline | Whitespace::Tab) => {
                out.push_str(&token.to_string());
            }
            // Comments may carry anything, including values.
            Token::Whitespace(_) => out.push(' '),
            Token::Number(..) => out.push('?'),
            other => {
                // Operators and punctuation are short and never contain
                // digits or quotes; every literal kind does (or is long).
                let text = other.to_string();
                let literal = text.len() > 3
                    || text
                        .chars()
                        .any(|c| c.is_ascii_alphanumeric() || matches!(c, '\'' | '"' | '$' | '`'));
                if literal {
                    out.push('?');
                } else {
                    out.push_str(&text);
                }
            }
        }
        if out.len() > MAX_SQL_TEXT {
            let mut end = MAX_SQL_TEXT;
            while !out.is_char_boundary(end) {
                end -= 1;
            }
            out.truncate(end);
            out.push_str(" …");
            break;
        }
    }
    Some(out)
}

/// [`redact_sql`] with a dialect-agnostic tokenizer.
pub fn redact_sql_generic(sql: &str) -> Option<String> {
    redact_sql(sql, &GenericDialect {})
}

/// Stable 64-bit FNV-1a fingerprint of (already redacted) SQL, as 16 hex
/// characters. Hash the *redacted* text: a hash of the raw text would let
/// anyone confirm a guessed low-entropy literal.
pub fn sql_hash(redacted_sql: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let hash = redacted_sql
        .bytes()
        .fold(OFFSET, |hash, byte| (hash ^ u64::from(byte)).wrapping_mul(PRIME));
    format!("{hash:016x}")
}

/// Letters, digits and `_`: characters that glue into one word, so a quote
/// or digit next to them is part of that word (`Can't`, `Int32`).
fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Error text safe for a span. Engine and driver errors quote or print the
/// values that failed, so the message loses:
///
/// - every quoted segment (`'…'`, `"…"`) — the whole *outermost* one when
///   quotes nest, as in a driver that wraps the server's message in quotes
///   (`'…value 'X' to int.' on server …`): pairing quotes left to right there
///   would leave `X` outside every pair;
/// - every number, including hex (`0x…`) and anything else starting with a
///   digit (`98765432101`, `1e10`, `10.5`). Digits inside a word (`Int32`,
///   `col_2`) stay.
///
/// A quote opens a segment only at a word start (not in `Can't`) and closes
/// one only at a word end; an unterminated segment runs to the end of the
/// text. Truncated to 1 KiB.
pub fn redact_message(message: &str) -> String {
    const MAX: usize = 1024;
    let chars: Vec<char> = message.chars().collect();
    let mut out = String::with_capacity(message.len().min(MAX));
    // Quote characters of the segments currently open, outermost first.
    let mut open: Vec<char> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if out.len() >= MAX {
            out.push('…');
            return out;
        }
        let c = chars[i];
        let prev = i.checked_sub(1).map(|p| chars[p]);
        let next = chars.get(i + 1).copied();
        let starts_word = prev.is_none_or(|p| !is_word_char(p));
        if c == '\'' || c == '"' {
            let innermost = open.last() == Some(&c);
            if innermost && next == Some(c) {
                // Doubled quote: an escaped quote inside the segment.
                i += 2;
                continue;
            }
            if innermost && next.is_none_or(|n| !is_word_char(n)) {
                open.pop();
                if open.is_empty() {
                    out.push(c);
                    out.push('?');
                    out.push(c);
                }
            } else if starts_word {
                open.push(c);
            } else if open.is_empty() {
                out.push(c);
            }
            i += 1;
            continue;
        }
        if !open.is_empty() {
            i += 1;
            continue;
        }
        if c.is_numeric() && starts_word {
            // The whole number: digits, letters (hex, exponents, suffixes)
            // and a decimal part.
            i += 1;
            while let Some(&n) = chars.get(i) {
                let decimal = n == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit);
                if !(is_word_char(n) || decimal) {
                    break;
                }
                i += 1;
            }
            out.push('?');
            continue;
        }
        out.push(c);
        i += 1;
    }
    if let Some(&outermost) = open.first() {
        // Unterminated: everything after the opening quote is dropped.
        out.push(outermost);
        out.push('?');
    }
    out
}

/// Records `text` (redacted) and its hash on `span`'s
/// [`field::QUERY_TEXT`] and [`field::SQL_HASH`] fields.
pub fn record_sql(span: &Span, sql: &str, dialect: &dyn Dialect) {
    match redact_sql(sql, dialect) {
        Some(redacted) => {
            span.record(field::SQL_HASH, sql_hash(&redacted).as_str());
            span.record(field::QUERY_TEXT, redacted.as_str());
        }
        None => {
            span.record(field::QUERY_TEXT, "<not recorded: SQL did not tokenize>");
        }
    }
}

/// Marks `span` as failed with a redacted description of `err`.
pub fn record_error(span: &Span, err: &dyn fmt::Display) {
    span.record(field::STATUS_CODE, "ERROR");
    span.record(
        field::STATUS_DESCRIPTION,
        redact_message(&err.to_string()).as_str(),
    );
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

/// A record-batch stream polled inside `span`. Counts rows into
/// [`field::ROWS`], records the first error, and closes the span (and every
/// span passed to [`TracedStream::keep_alive`]) when the stream ends. A stream
/// dropped before its end records [`field::CANCELLED`].
///
/// It buffers nothing: batches pass through as they come.
pub struct TracedStream {
    inner: SendableRecordBatchStream,
    span: Span,
    keep_alive: Vec<Span>,
    rows: u64,
    failed: bool,
    finished: bool,
}

impl fmt::Debug for TracedStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TracedStream")
            .field("span", &self.span)
            .field("rows", &self.rows)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl TracedStream {
    pub fn new(inner: SendableRecordBatchStream, span: Span) -> Self {
        Self {
            inner,
            span,
            keep_alive: Vec::new(),
            rows: 0,
            failed: false,
            finished: false,
        }
    }

    /// Keeps `span` open until the stream ends and records the row count
    /// and any error on it too.
    #[must_use]
    pub fn keep_alive(mut self, span: Span) -> Self {
        self.keep_alive.push(span);
        self
    }

    fn spans(&self) -> impl Iterator<Item = &Span> {
        std::iter::once(&self.span).chain(&self.keep_alive)
    }

    fn finish(&mut self) {
        self.finished = true;
        for span in self.spans() {
            span.record(field::ROWS, self.rows);
        }
        self.span = Span::none();
        self.keep_alive.clear();
    }
}

impl Stream for TracedStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        let poll = {
            let _entered = this.span.enter();
            this.inner.as_mut().poll_next(cx)
        };
        match &poll {
            Poll::Ready(Some(Ok(batch))) => this.rows += batch.num_rows() as u64,
            Poll::Ready(Some(Err(err))) => {
                if !this.failed {
                    this.failed = true;
                    for span in this.spans() {
                        record_error(span, err);
                    }
                }
            }
            Poll::Ready(None) => this.finish(),
            Poll::Pending => {}
        }
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for TracedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Drop for TracedStream {
    fn drop(&mut self) {
        if !self.finished {
            for span in self.spans() {
                span.record(field::CANCELLED, true);
                span.record(field::ROWS, self.rows);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::MsSqlDialect;
    use proptest::prelude::*;

    const EXAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01";

    #[test]
    fn parses_the_w3c_example() {
        let parent = TraceParent::parse(EXAMPLE).unwrap();
        assert_eq!(parent.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(parent.parent_id_hex(), "b7ad6b7169203331");
        assert!(parent.is_sampled());
        assert_eq!(parent.to_string(), EXAMPLE);
        assert_eq!(EXAMPLE.parse::<TraceParent>().unwrap(), parent);
        let unsampled = TraceParent::parse(&EXAMPLE.replace("-01", "-00")).unwrap();
        assert!(!unsampled.is_sampled());
        assert_eq!(unsampled.flags, 0);
    }

    #[test]
    fn accepts_surrounding_optional_whitespace() {
        assert!(TraceParent::parse(&format!(" \t{EXAMPLE} ")).is_ok());
    }

    #[test]
    fn rejects_invalid_values() {
        let cases = [
            ("", TraceParentError::Malformed),
            (&EXAMPLE[..54], TraceParentError::Malformed),
            (&EXAMPLE.to_uppercase(), TraceParentError::Malformed),
            (
                &EXAMPLE.replacen("00-", "ff-", 1),
                TraceParentError::InvalidVersion,
            ),
            (
                "00-00000000000000000000000000000000-b7ad6b7169203331-01",
                TraceParentError::ZeroTraceId,
            ),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
                TraceParentError::ZeroParentId,
            ),
            (&format!("{EXAMPLE}-extra"), TraceParentError::Malformed),
            (&EXAMPLE.replace('-', "_"), TraceParentError::Malformed),
            (
                "00-4bf92f3577b34da6a3ce929d0e0e473g-b7ad6b7169203331-01",
                TraceParentError::Malformed,
            ),
            (
                "0-04bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01",
                TraceParentError::Malformed,
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(TraceParent::parse(value), Err(expected), "{value:?}");
        }
    }

    #[test]
    fn future_versions_may_append_fields() {
        let future = EXAMPLE.replacen("00-", "cc-", 1);
        let parent = TraceParent::parse(&format!("{future}-what-the-future-holds")).unwrap();
        assert_eq!(parent.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(TraceParent::parse(&format!("{future}x")).is_err());
        assert!(TraceParent::parse(&future).is_ok());
    }

    proptest! {
        #[test]
        fn render_then_parse_round_trips(trace in any::<[u8; 16]>(), parent in any::<[u8; 8]>(), flags in any::<u8>()) {
            prop_assume!(trace != [0; 16] && parent != [0; 8]);
            let value = TraceParent { trace_id: trace, parent_id: parent, flags };
            prop_assert_eq!(TraceParent::parse(&value.to_string()), Ok(value));
        }

        #[test]
        fn parse_never_panics(input in ".{0,80}") {
            let _ = TraceParent::parse(&input);
        }
    }

    #[test]
    fn from_headers_follows_w3c_on_invalid_parents() {
        let trace = QueryTrace::from_headers(Some(EXAMPLE), Some("vendor=abc"), Some("dbt-run-42"));
        assert_eq!(trace.parent, Some(TraceParent::parse(EXAMPLE).unwrap()));
        assert_eq!(trace.tracestate.as_deref(), Some("vendor=abc"));
        assert_eq!(trace.correlation_id.as_deref(), Some("dbt-run-42"));
        assert_eq!(trace.query_id, None);

        // Invalid traceparent: new trace, and its tracestate is discarded.
        let trace = QueryTrace::from_headers(Some("garbage"), Some("vendor=abc"), None);
        assert_eq!(trace, QueryTrace::default());

        let oversized = "k=".to_owned() + &"v".repeat(600);
        let trace = QueryTrace::from_headers(Some(EXAMPLE), Some(&oversized), Some(&"t".repeat(257)));
        assert_eq!(trace.tracestate, None);
        assert_eq!(trace.correlation_id, None);
        let trace = QueryTrace::from_headers(None, None, Some("bad\ntoken"));
        assert_eq!(trace.correlation_id, None);
        let trace = QueryTrace::default()
            .with_query_id("q1")
            .with_correlation_id(" session-7 ");
        assert_eq!(trace.query_id.as_deref(), Some("q1"));
        assert_eq!(trace.correlation_id.as_deref(), Some("session-7"));
    }

    #[test]
    fn query_ids_have_trinos_shape() {
        // 2026-09-23T14:25:01Z
        assert_eq!(
            format_query_id(1_790_173_501, 7, "abcde"),
            "20260923_142501_00007_abcde"
        );
        assert_eq!(
            format_query_id(0, 100_003, "zzzzz"),
            "19700101_000000_00003_zzzzz"
        );
        // Leap day.
        assert_eq!(format_query_id(951_782_400, 0, "x"), "20000229_000000_00000_x");
        let a = next_query_id();
        let b = next_query_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 27, "{a}");
        assert!(
            a.bytes()
                .all(|c| c.is_ascii_digit() || c == b'_' || ID_ALPHABET.contains(&c)),
            "{a}"
        );
    }

    #[test]
    fn redaction_removes_every_literal_kind() {
        let sql = "SELECT [Id], \"name\" FROM [dbo].[Contratos] /* user 42 */ \
                   WHERE [Valor] > 10.5 AND [Situacao] = N'segredo' AND [lsn] > 0x0000002A00000A \
                   AND x = 'it''s' -- tail 99\n AND y IN (1, -2) AND z = X'ff'";
        let redacted = redact_sql(sql, &MsSqlDialect {}).unwrap();
        for leaked in ["10.5", "segredo", "0x", "2A", "it''s", "42", "99", "ff'", "-2"] {
            assert!(!redacted.contains(leaked), "`{leaked}` leaked: {redacted}");
        }
        assert!(redacted.contains("[dbo].[Contratos]"), "{redacted}");
        assert!(redacted.contains("[Valor] > ?"), "{redacted}");
        assert!(redacted.contains("\"name\""), "{redacted}");
        assert!(redacted.contains("IN (?, -?)"), "{redacted}");
    }

    #[test]
    fn redaction_handles_generic_literals_and_placeholders() {
        let sql = "select a from t where b = $1 and c = $$dollar$$ and d = E'x' and e = B'01' and f >= 3";
        let redacted = redact_sql(sql, &datafusion::sql::sqlparser::dialect::PostgreSqlDialect {}).unwrap();
        assert_eq!(
            redacted,
            "select a from t where b = $1 and c = ? and d = ? and e = ? and f >= ?"
        );
        assert_eq!(redact_sql_generic("SELECT 'unterminated"), None);
    }

    #[test]
    fn redaction_truncates_huge_statements() {
        let sql = format!("SELECT {}", vec!["col"; 5000].join(", "));
        let redacted = redact_sql_generic(&sql).unwrap();
        assert!(redacted.len() <= MAX_SQL_TEXT + 8, "{}", redacted.len());
        assert!(redacted.ends_with('…'));
    }

    #[test]
    fn same_shape_same_hash() {
        let a = redact_sql_generic("SELECT a FROM t WHERE id = 1").unwrap();
        let b = redact_sql_generic("SELECT a FROM t WHERE id = 99999").unwrap();
        let c = redact_sql_generic("SELECT b FROM t WHERE id = 1").unwrap();
        assert_eq!(sql_hash(&a), sql_hash(&b));
        assert_ne!(sql_hash(&a), sql_hash(&c));
        assert_eq!(sql_hash(""), "cbf29ce484222325");
        assert_eq!(sql_hash("a"), "af63dc4c8601ec8c");
        assert_eq!(sql_hash(&a).len(), 16);
    }

    #[test]
    fn messages_lose_quoted_values_and_hex() {
        assert_eq!(
            redact_message("Conversion failed when converting the nvarchar value 'Maria d''Ávila' to int."),
            "Conversion failed when converting the nvarchar value '?' to int."
        );
        assert_eq!(
            redact_message("column \"secret\" at 0xDEADbeef, line 3"),
            "column \"?\" at ?, line ?"
        );
        let long = redact_message(&"a".repeat(5000));
        assert!(long.len() < 1100 && long.ends_with('…'));
    }

    #[test]
    fn messages_lose_values_inside_a_quoted_driver_message() {
        // tiberius wraps the server message in quotes; the value SQL Server
        // quotes sits inside: pairing left to right would expose it.
        let tiberius = "Token error: 'Conversion failed when converting the nvarchar value \
                        'CONFIDENTIAL-4711' to data type int.' on server db1 executing  on line 1 \
                        (code: 245, state: 1, class: 16)";
        let redacted = redact_message(tiberius);
        assert_eq!(
            redacted,
            "Token error: '?' on server db1 executing  on line ? (code: ?, state: ?, class: ?)"
        );
        assert_eq!(
            redact_message("expected '(', found 'a' and \"b\""),
            "expected '?', found '?' and \"?\""
        );
        assert_eq!(redact_message("value 'O'Brien' here"), "value '?' here");
        assert_eq!(redact_message("value '' here"), "value '?' here");
        assert_eq!(redact_message("unterminated 'secret tail"), "unterminated '?");
        assert_eq!(redact_message("Utf8(\"secret\")"), "Utf8(\"?\")");
    }

    #[test]
    fn messages_lose_numbers_but_keep_words_with_digits() {
        assert_eq!(
            redact_message("Cast error: Can't cast value 98765432101 to type Int32"),
            "Cast error: Can't cast value ? to type Int32"
        );
        assert_eq!(
            redact_message("Int64(-42) / 10.5 at 1e10, col_2 v2 3f2a"),
            "Int64(-?) / ? at ?, col_2 v2 ?"
        );
        assert_eq!(redact_message("users' table"), "users' table");
    }

    proptest! {
        /// Whatever surrounds a value, quoted or bare, it never survives.
        #[test]
        fn redacted_messages_never_carry_the_value(
            prefix in "[ -~]{0,30}",
            quote in prop::sample::select(vec!["'", "\"", ""]),
            outer in prop::sample::select(vec!["'", ""]),
            suffix in "[ -~]{0,30}",
        ) {
            let value = if quote.is_empty() { "98765432101" } else { "SECRET-VALUE" };
            let message = format!("{outer}{prefix} {quote}{value}{quote} {suffix}{outer} on server x");
            let redacted = redact_message(&message);
            prop_assert!(!redacted.contains(value), "{message:?} -> {redacted:?}");
        }
    }

    #[test]
    fn functions_without_backend_are_no_ops() {
        let span = Span::none();
        set_remote_parent(&span, &TraceParent::parse(EXAMPLE).unwrap(), None);
        assert_eq!(trace_id(&span), None);
        record_error(&span, &"boom 'x'");
        record_sql(&span, "SELECT 1", &GenericDialect {});
    }
}
