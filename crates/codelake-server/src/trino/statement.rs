//! `/v1/statement`: submit, page and cancel queries.
//!
//! Each query runs in its own task that plans it and pushes record batches into
//! a small bounded channel, so execution moves ahead of the client by at most a
//! few batches, like Trino's output buffer; the buffered batches are reserved
//! in the engine's memory pool. `GET nextUri` drains the channel into a page of
//! about 1 MiB of JSON. Pages are numbered by the token in the URI; asking again
//! for the current token returns the same page (a client retry after a lost
//! response), an older token is gone (`410`) and a later one unknown (`404`).
//!
//! Every operator of the physical plan is made cooperative, so a canceled,
//! abandoned or timed-out query stops within one budget of batches even when
//! an operator produces rows without waiting for input (a cross join).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use codelake_core::session_properties::parse_duration;
use codelake_core::{Engine, PropertyType, QueryOptions};
use datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, UInt64Type};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use futures::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, error::TryRecvError};

use super::errors::{Code, Everything, Namespace, QueryError};
use super::prepared::{self, Command};
use super::queries::{Info, Limits, QueriesTable, Query, Registry, Release, State as QueryState};
use super::rewrite;
use super::session::{self, ClientSession, Control, SessionError};
use super::types::{self, Column, Encoding};

/// JSON per page; Trino's default target result size is 1 MB.
const PAGE_TARGET_BYTES: usize = 1 << 20;
/// Longest a `GET nextUri` waits for rows before answering with none.
const MAX_WAIT: Duration = Duration::from_secs(1);
/// Batches produced ahead of the client.
const BUFFERED_BATCHES: usize = 2;
/// Version reported to clients: the Trino release whose protocol is implemented.
const PROTOCOL_VERSION: &str = "431";

type TrinoQuery = Query<Exec>;

/// How a statement's result is shown to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A query: its rows.
    Rows,
    /// `INSERT`, `CREATE TABLE AS`, `DELETE`, `UPDATE`, `MERGE`: one `rows`
    /// bigint column with the row count, and `updateCount`.
    Count(&'static str),
    /// Other DDL: `updateType`, an empty column list and no rows (as Trino 431).
    Ack(&'static str),
}

impl Shape {
    fn update_type(self) -> Option<&'static str> {
        match self {
            Self::Rows => None,
            Self::Count(kind) | Self::Ack(kind) => Some(kind),
        }
    }
}

fn shape(sql: &str) -> Shape {
    use datafusion::sql::parser::{DFParser, Statement as DfStatement};
    use datafusion::sql::sqlparser::ast::{AlterTableOperation, ObjectType, Statement};
    if let Some(update_type) = codelake_core::ddl_update_type(sql) {
        return Shape::Ack(update_type);
    }
    let Ok(mut statements) = DFParser::parse_sql(sql) else {
        // The engine reports the parse error.
        return Shape::Rows;
    };
    match statements.pop_front() {
        Some(DfStatement::Statement(statement)) => match *statement {
            Statement::Insert(_) => Shape::Count("INSERT"),
            Statement::Delete(_) => Shape::Count("DELETE"),
            Statement::Update(_) => Shape::Count("UPDATE"),
            Statement::Merge(_) => Shape::Count("MERGE"),
            Statement::CreateTable(create) if create.query.is_some() => Shape::Count("CREATE TABLE"),
            Statement::CreateTable(_) => Shape::Ack("CREATE TABLE"),
            Statement::CreateView(_) => Shape::Ack("CREATE VIEW"),
            Statement::CreateSchema { .. } => Shape::Ack("CREATE SCHEMA"),
            Statement::Truncate(_) => Shape::Ack("TRUNCATE TABLE"),
            Statement::AlterTable(alter) => Shape::Ack(match alter.operations.first() {
                Some(AlterTableOperation::RenameTable { .. }) => "RENAME TABLE",
                Some(AlterTableOperation::RenameColumn { .. }) => "RENAME COLUMN",
                Some(AlterTableOperation::DropColumn { .. }) => "DROP COLUMN",
                _ => "ADD COLUMN",
            }),
            Statement::Drop { object_type, .. } => match object_type {
                ObjectType::Table => Shape::Ack("DROP TABLE"),
                ObjectType::View => Shape::Ack("DROP VIEW"),
                ObjectType::Schema => Shape::Ack("DROP SCHEMA"),
                _ => Shape::Rows,
            },
            _ => Shape::Rows,
        },
        Some(DfStatement::CreateExternalTable(_)) => Shape::Ack("CREATE TABLE"),
        _ => Shape::Rows,
    }
}

/// The table of `DESCRIBE <table>` (Trino's columns `Column, Type, Extra, Comment`).
fn describe_target(sql: &str) -> Option<String> {
    use datafusion::sql::parser::{DFParser, Statement as DfStatement};
    use datafusion::sql::sqlparser::ast::{DescribeAlias, Statement};
    let mut statements = DFParser::parse_sql(sql).ok()?;
    match statements.pop_front()? {
        DfStatement::Statement(statement) => match *statement {
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                hive_format: None,
                table_name,
                ..
            } => Some(table_name.to_string()),
            _ => None,
        },
        _ => None,
    }
}

/// Which properties `SHOW SESSION` lists.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ShowSession {
    All,
    Like(Like),
}

/// `SHOW SESSION [LIKE 'pattern' [ESCAPE 'c']]`; `None` for other statements.
fn show_session_filter(sql: &str) -> Result<Option<ShowSession>, QueryError> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let Some(rest) = session::strip_keyword(text, "SHOW") else {
        return Ok(None);
    };
    if rest.eq_ignore_ascii_case("SESSION") {
        return Ok(Some(ShowSession::All));
    }
    let Some(rest) = session::strip_keyword(rest, "SESSION") else {
        return Ok(None);
    };
    let syntax = |message: String| QueryError::new(Code::SyntaxError, message);
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(rest)
        .map_err(|e| syntax(e.to_string()))?;
    if !parser.parse_keyword(Keyword::LIKE) {
        return Err(syntax("SHOW SESSION expects LIKE 'pattern'".into()));
    }
    let pattern = parser.parse_literal_string().map_err(|e| syntax(e.to_string()))?;
    let escape = if parser.parse_keyword(Keyword::ESCAPE) {
        let escape = parser.parse_literal_string().map_err(|e| syntax(e.to_string()))?;
        let mut chars = escape.chars();
        match (chars.next(), chars.next()) {
            (Some(c), None) => Some(c),
            _ => {
                return Err(QueryError::new(
                    Code::GenericUserError,
                    "Escape string must be a single character",
                ));
            }
        }
    } else {
        None
    };
    if parser.peek_token().token != Token::EOF {
        return Err(syntax("Unexpected text after SHOW SESSION LIKE".into()));
    }
    Ok(Some(ShowSession::Like(Like { pattern, escape })))
}

/// A SQL `LIKE` pattern: `%` any text, `_` one character.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Like {
    pattern: String,
    escape: Option<char>,
}

impl Like {
    fn matches(&self, text: &str) -> bool {
        #[derive(Clone, Copy, PartialEq)]
        enum Part {
            Any,
            One,
            Char(char),
        }
        let mut parts = Vec::new();
        let mut chars = self.pattern.chars();
        while let Some(c) = chars.next() {
            parts.push(match c {
                c if Some(c) == self.escape => chars.next().map_or(Part::Char(c), Part::Char),
                '%' => Part::Any,
                '_' => Part::One,
                c => Part::Char(c),
            });
        }
        let text: Vec<char> = text.chars().collect();
        // matched[j]: the first i characters match the first j parts.
        let mut matched = vec![false; parts.len() + 1];
        matched[0] = true;
        for (j, part) in parts.iter().enumerate() {
            if *part == Part::Any {
                matched[j + 1] = matched[j];
            }
        }
        for c in &text {
            let mut next = vec![false; parts.len() + 1];
            for (j, part) in parts.iter().enumerate() {
                next[j + 1] = match part {
                    Part::Any => next[j] || matched[j + 1],
                    Part::One => matched[j],
                    Part::Char(p) => matched[j] && p == c,
                };
            }
            matched = next;
        }
        matched[parts.len()]
    }
}

/// A batch waiting for the client, with its memory reserved in the pool.
struct Buffered {
    batch: RecordBatch,
    _reservation: MemoryReservation,
}

enum Event {
    Started(SchemaRef),
    Batch(Buffered),
    Done,
    Failed(QueryError),
}

enum Work {
    /// A statement; `planned` is the text actually planned, when it differs
    /// (see [`rewrite::name_unnamed_columns`]).
    Sql {
        sql: String,
        planned: Option<String>,
    },
    Frame(Box<DataFrame>),
    Describe(String),
}

/// Paging state of one query, behind the query's async mutex.
pub(crate) struct Exec {
    events: mpsc::Receiver<Event>,
    shape: Shape,
    encoding: Encoding,
    columns: Option<Arc<Vec<Column>>>,
    count: Option<u64>,
    pending: Option<(Buffered, usize)>,
    finished: bool,
    next_token: u64,
    last: Option<(u64, Bytes)>,
    warnings: Vec<Value>,
}

impl Release for Exec {
    /// Drops every buffered batch (and its reservation) and stops the
    /// producer, whose next send fails.
    fn release(&mut self) {
        self.events.close();
        while self.events.try_recv().is_ok() {}
        self.pending = None;
        self.finished = true;
    }
}

impl Exec {
    fn new(events: mpsc::Receiver<Event>, encoding: Encoding, warnings: Vec<Value>) -> Self {
        Self {
            events,
            shape: Shape::Rows,
            encoding,
            columns: None,
            count: None,
            pending: None,
            finished: false,
            next_token: 1,
            last: None,
            warnings,
        }
    }

    /// Encodes pending rows until the page is full.
    fn take_rows(
        &mut self,
        query: &TrinoQuery,
        rows: &mut Vec<Vec<Value>>,
        bytes: &mut usize,
    ) -> Result<(), QueryError> {
        let Some((buffered, mut offset)) = self.pending.take() else {
            return Ok(());
        };
        let start = offset;
        let batch = &buffered.batch;
        while offset < batch.num_rows() && *bytes < PAGE_TARGET_BYTES {
            let row = batch
                .columns()
                .iter()
                .map(|column| types::value(column, offset, self.encoding))
                .collect::<Result<Vec<_>, _>>()?;
            *bytes += row.iter().map(approximate_size).sum::<usize>();
            rows.push(row);
            offset += 1;
        }
        query.add_rows(u64::try_from(offset - start).unwrap_or(u64::MAX));
        if offset < batch.num_rows() {
            self.pending = Some((buffered, offset));
        }
        Ok(())
    }

    fn absorb(&mut self, event: Event) -> Result<(), QueryError> {
        match event {
            Event::Started(schema) => {
                if self.shape == Shape::Rows {
                    self.columns = Some(Arc::new(types::columns(&schema, self.encoding)?));
                }
            }
            Event::Batch(buffered) => match self.shape {
                Shape::Rows => self.pending = Some((buffered, 0)),
                Shape::Count(_) | Shape::Ack(_) => {
                    if let Some(rows) = row_count(&buffered.batch) {
                        self.count = Some(self.count.unwrap_or(0).saturating_add(rows));
                    }
                }
            },
            Event::Done => self.finished = true,
            Event::Failed(error) => return Err(error),
        }
        Ok(())
    }

    /// Produces the next page: rows, a final result, or an error.
    async fn page(&mut self, query: &TrinoQuery) -> Page {
        if let Some(error) = query.error() {
            self.release();
            return Page::failed(error);
        }
        let deadline = tokio::time::Instant::now() + MAX_WAIT;
        let mut rows = Vec::new();
        let mut bytes = 0;
        loop {
            if let Err(error) = self.take_rows(query, &mut rows, &mut bytes) {
                return self.fail(query, error);
            }
            if bytes >= PAGE_TARGET_BYTES || self.pending.is_some() || self.finished {
                break;
            }
            // Wait for the first rows; once some are in hand, only take what is ready.
            let event = if rows.is_empty() {
                match tokio::time::timeout_at(deadline, self.events.recv()).await {
                    Ok(event) => event,
                    Err(_) => break,
                }
            } else {
                match self.events.try_recv() {
                    Ok(event) => Some(event),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => None,
                }
            };
            let Some(event) = event else {
                // The task ended without a final event: canceled or panicked.
                let error = query.error().unwrap_or_else(|| {
                    QueryError::new(Code::GenericInternalError, "Query execution ended unexpectedly")
                });
                return self.fail(query, error);
            };
            if let Err(error) = self.absorb(event) {
                return self.fail(query, error);
            }
        }
        if !(self.finished && self.pending.is_none()) {
            return Page {
                columns: self.columns.clone(),
                data: (!rows.is_empty()).then_some(rows),
                ..Page::default()
            };
        }
        query.finish();
        let mut page = Page {
            columns: self.columns.clone(),
            data: (!rows.is_empty()).then_some(rows),
            done: true,
            update_type: self.shape.update_type(),
            ..Page::default()
        };
        match (self.shape, self.count) {
            (Shape::Rows, _) => {
                page.columns = page.columns.or_else(|| Some(Arc::new(Vec::new())));
            }
            (Shape::Count(_), Some(count)) => {
                page.update_count = Some(count);
                let schema = Schema::new(vec![Field::new("rows", DataType::Int64, false)]);
                page.columns = types::columns(&schema, self.encoding).ok().map(Arc::new);
                page.data = Some(vec![vec![json!(count)]]);
            }
            // DDL: Trino sends an empty column list and no rows (JDBC requires
            // the list, even for `executeUpdate`).
            (Shape::Count(_) | Shape::Ack(_), _) => {
                page.columns = Some(Arc::new(Vec::new()));
                page.data = None;
            }
        }
        page
    }

    fn fail(&mut self, query: &TrinoQuery, error: QueryError) -> Page {
        self.release();
        query.fail(error);
        // The first recorded failure wins (a cancel racing an execution error).
        Page::failed(
            query
                .error()
                .unwrap_or_else(|| QueryError::new(Code::GenericInternalError, "Query failed")),
        )
    }
}

fn approximate_size(value: &Value) -> usize {
    match value {
        Value::String(text) => text.len() + 2,
        Value::Array(items) => items.iter().map(approximate_size).sum::<usize>() + 2,
        Value::Object(entries) => {
            entries
                .iter()
                .map(|(key, value)| key.len() + 3 + approximate_size(value))
                .sum::<usize>()
                + 2
        }
        _ => 8,
    }
}

/// Row count reported by `INSERT` (`count`) or `CREATE TABLE AS` (`rows`).
fn row_count(batch: &RecordBatch) -> Option<u64> {
    let column = batch.columns().first()?;
    let values: Vec<u64> = match column.data_type() {
        DataType::UInt64 => column.as_primitive::<UInt64Type>().iter().flatten().collect(),
        DataType::Int64 => column
            .as_primitive::<Int64Type>()
            .iter()
            .flatten()
            .map(|v| u64::try_from(v).unwrap_or(0))
            .collect(),
        _ => return None,
    };
    Some(values.iter().sum())
}

#[derive(Default)]
struct Page {
    columns: Option<Arc<Vec<Column>>>,
    data: Option<Vec<Vec<Value>>>,
    error: Option<QueryError>,
    done: bool,
    update_type: Option<&'static str>,
    update_count: Option<u64>,
}

impl Page {
    fn failed(error: QueryError) -> Self {
        Self {
            error: Some(error),
            done: true,
            ..Self::default()
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryResults<'a> {
    id: &'a str,
    info_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<&'a [Column]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a [Vec<Value>]>,
    stats: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a QueryError>,
    warnings: &'a [Value],
    #[serde(skip_serializing_if = "Option::is_none")]
    update_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    update_count: Option<u64>,
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `StatementStats`; the Java client requires every field.
fn stats(info: &Info) -> Value {
    let elapsed = info
        .ended_at
        .unwrap_or_else(Instant::now)
        .saturating_duration_since(info.created_at);
    json!({
        "state": info.state.as_str(),
        "queued": info.state == QueryState::Queued,
        "scheduled": !matches!(info.state, QueryState::Queued | QueryState::Planning),
        "nodes": 1,
        "totalSplits": 0,
        "queuedSplits": 0,
        "runningSplits": 0,
        "completedSplits": 0,
        "cpuTimeMillis": 0,
        "wallTimeMillis": millis(elapsed),
        "queuedTimeMillis": millis(info.queued),
        "elapsedTimeMillis": millis(elapsed),
        "processedRows": info.rows,
        "processedBytes": 0,
        "physicalInputBytes": 0,
        "physicalWrittenBytes": 0,
        "peakMemoryBytes": 0,
        "spilledBytes": 0,
    })
}

/// airlift `Duration.toString()`: `1.00h`, `30.00m`, `250.00ms`.
fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs_f64();
    let (value, unit) = [(86_400.0, "d"), (3600.0, "h"), (60.0, "m"), (1.0, "s")]
        .into_iter()
        .find(|(size, _)| seconds >= *size)
        .map_or((seconds * 1000.0, "ms"), |(size, unit)| (seconds / size, unit));
    format!("{value:.2}{unit}")
}

fn warning(message: &str) -> Value {
    json!({"warningCode": {"code": 65_536, "name": "SESSION_PROPERTY_IGNORED"}, "message": message})
}

/// Catalogs and schemas of the engine, to name what a statement is missing.
struct EngineNames<'a>(&'a Engine);

impl Namespace for EngineNames<'_> {
    fn has_catalog(&self, catalog: &str) -> bool {
        self.0.context().catalog(catalog).is_some()
    }

    fn has_schema(&self, catalog: &str, schema: &str) -> bool {
        self.0
            .context()
            .catalog(catalog)
            .is_some_and(|c| c.schema(schema).is_some())
    }
}

/// State shared by the protocol handlers.
pub(crate) struct TrinoState {
    engine: Arc<Engine>,
    queries: Arc<Registry<Exec>>,
    fallback_host: String,
    started: Instant,
    draining: AtomicBool,
}

impl std::fmt::Debug for TrinoState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoState")
            .field("queries", &self.queries)
            .finish_non_exhaustive()
    }
}

impl TrinoState {
    pub(crate) fn new(engine: Arc<Engine>, limits: Limits, listen: SocketAddr) -> Arc<Self> {
        Arc::new(Self {
            engine,
            queries: Arc::new(Registry::new(limits)),
            fallback_host: listen.to_string(),
            started: Instant::now(),
            draining: AtomicBool::new(false),
        })
    }

    /// Catalog `system` with `runtime.queries`, `metadata.*` and `jdbc.*`.
    pub(crate) fn system_catalog(&self) -> datafusion::error::Result<Arc<dyn CatalogProvider>> {
        let runtime = MemorySchemaProvider::new();
        runtime.register_table(
            "queries".into(),
            Arc::new(QueriesTable::new(Arc::clone(&self.queries))),
        )?;
        let catalog = MemoryCatalogProvider::new();
        catalog.register_schema("runtime", Arc::new(runtime))?;
        super::system_tables::register(&catalog, self.engine.connectors())?;
        Ok(Arc::new(catalog))
    }

    pub(crate) fn sweep(&self) {
        self.queries.sweep();
    }

    /// New submissions are refused; running queries keep paging.
    pub(crate) fn start_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    fn base_uri(&self, headers: &HeaderMap) -> String {
        let host = headers
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .filter(|h| {
                !h.is_empty()
                    && h.chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']' | '_'))
            })
            .unwrap_or(&self.fallback_host);
        format!("http://{host}")
    }

    fn body(
        query: &TrinoQuery,
        base: &str,
        page: &Page,
        next_token: Option<u64>,
        warnings: &[Value],
    ) -> Bytes {
        let info = query.snapshot();
        let results = QueryResults {
            id: &query.id,
            info_uri: format!("{base}/ui/query.html?{}", query.id),
            next_uri: next_token
                .map(|t| format!("{base}/v1/statement/executing/{}/{}/{t}", query.id, query.slug)),
            columns: page.columns.as_deref().map(Vec::as_slice),
            data: page.data.as_deref(),
            stats: stats(&info),
            error: page.error.as_ref(),
            warnings,
            update_type: page.update_type,
            update_count: page.update_count,
        };
        // Serializing plain JSON values cannot fail.
        Bytes::from(serde_json::to_vec(&results).unwrap_or_default())
    }
}

pub(crate) fn router(state: Arc<TrinoState>) -> Router {
    Router::new()
        .route("/v1/statement", post(submit))
        .route(
            "/v1/statement/executing/{id}/{slug}/{token}",
            get(fetch).delete(cancel_statement),
        )
        .route("/v1/query/{id}", delete(kill_query))
        .route("/v1/info", get(info))
        .with_state(state)
}

fn json_response(body: Bytes, headers: &[(&'static str, String)]) -> Response {
    let mut response = (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], body).into_response();
    for (name, value) in headers {
        if let Ok(value) = HeaderValue::from_str(value) {
            response
                .headers_mut()
                .append(HeaderName::from_static(name), value);
        }
    }
    response
}

fn user_canceled() -> QueryError {
    QueryError::new(Code::UserCanceled, "Query was canceled")
}

fn too_large(length: Option<usize>, max: usize) -> QueryError {
    QueryError::new(
        Code::QueryTextTooLarge,
        match length {
            Some(length) => format!("Query text length ({length}) exceeds the maximum length ({max})"),
            None => format!("Query text exceeds the maximum length ({max})"),
        },
    )
}

/// Body bytes read for a statement: the longest accepted text in UTF-8, plus
/// room for surrounding whitespace. Longer bodies are not read.
fn body_limit(max_chars: usize) -> usize {
    max_chars.saturating_mul(4).saturating_add(64 * 1024)
}

/// Registers a query that failed before running and answers with its failure.
fn refuse(
    state: &TrinoState,
    base: &str,
    user: String,
    source: Option<String>,
    sql: &str,
    error: QueryError,
) -> Response {
    let (_, receiver) = mpsc::channel(1);
    let query = Arc::new(Query::new(
        state.queries.next_id(),
        user,
        source,
        sql,
        Exec::new(receiver, Encoding::MODERN, Vec::new()),
    ));
    // A full registry reports its own error instead.
    let error = state.queries.insert(Arc::clone(&query)).err().unwrap_or(error);
    query.fail(error.clone());
    json_response(
        TrinoState::body(&query, base, &Page::failed(error), None, &[]),
        &[],
    )
}

async fn submit(State(state): State<Arc<TrinoState>>, request: Request) -> Response {
    if state.draining.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Server is shutting down").into_response();
    }
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let Some(user) = headers
        .get(session::USER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(str::to_owned)
    else {
        return (StatusCode::UNAUTHORIZED, "User must be set").into_response();
    };
    let source = headers
        .get(session::SOURCE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let base = state.base_uri(&headers);
    let max_length = state.queries.limits().max_query_length;
    let Ok(body) = axum::body::to_bytes(body, body_limit(max_length)).await else {
        let declared = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        return refuse(&state, &base, user, source, "", too_large(declared, max_length));
    };
    let Ok(sql) = std::str::from_utf8(&body) else {
        return (StatusCode::BAD_REQUEST, "SQL statement is not valid UTF-8").into_response();
    };
    if sql.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "SQL statement is empty").into_response();
    }
    let length = sql.chars().count();
    if length > max_length {
        // Keep the history bounded: only the accepted length is recorded.
        let kept: String = sql.chars().take(max_length).collect();
        return refuse(
            &state,
            &base,
            user,
            source,
            &kept,
            too_large(Some(length), max_length),
        );
    }
    let registry = state.engine.session_properties();
    let session = match ClientSession::from_headers(&headers, registry) {
        Ok(session) => Ok(session),
        Err(SessionError::BadRequest(message)) => {
            return (StatusCode::BAD_REQUEST, message).into_response();
        }
        Err(SessionError::Query(error)) => Err(error),
    };
    let warnings = session
        .as_ref()
        .map(|s| {
            s.properties
                .ignored()
                .iter()
                .map(|name| warning(&format!("Session property {name} has no effect in CodeLake")))
                .collect()
        })
        .unwrap_or_default();
    let encoding = session.as_ref().map_or(Encoding::MODERN, |s| s.encoding);
    let (sender, receiver) = mpsc::channel(BUFFERED_BATCHES);
    let query = Arc::new(Query::new(
        state.queries.next_id(),
        user,
        source,
        sql,
        Exec::new(receiver, encoding, warnings),
    ));
    if let Err(error) = state.queries.insert(Arc::clone(&query)) {
        query.fail(error.clone());
        return json_response(
            TrinoState::body(&query, &base, &Page::failed(error), None, &[]),
            &[],
        );
    }

    // Nobody else can reach the query before this response: the lock is free.
    let mut exec = query.exec.lock().await;
    let started = start(&state, &query, &mut exec, sql, session, sender);
    drop(exec);
    match started {
        Ok((page, headers, next)) => {
            json_response(TrinoState::body(&query, &base, &page, next, &[]), &headers)
        }
        Err(error) => {
            query.fail(error.clone());
            json_response(
                TrinoState::body(&query, &base, &Page::failed(error), None, &[]),
                &[],
            )
        }
    }
}

type Started = (Page, Vec<(&'static str, String)>, Option<u64>);

/// A statement answered at once, without execution.
fn answered(query: &TrinoQuery, update_type: &'static str, headers: Vec<(&'static str, String)>) -> Started {
    query.set_state(QueryState::Running);
    query.finish();
    let page = Page {
        columns: Some(Arc::new(Vec::new())),
        done: true,
        update_type: Some(update_type),
        ..Page::default()
    };
    (page, headers, None)
}

fn not_found_prepared(name: &str) -> QueryError {
    QueryError::new(Code::NotFound, format!("Prepared statement not found: {name}"))
}

/// Session-control and prepared-statement commands answer at once; anything
/// else starts executing and the client follows `nextUri`.
fn start(
    state: &TrinoState,
    query: &Arc<TrinoQuery>,
    exec: &mut Exec,
    sql: &str,
    session: Result<ClientSession, QueryError>,
    sender: mpsc::Sender<Event>,
) -> Result<Started, QueryError> {
    let session = session?;
    let names = EngineNames(&state.engine);
    let bound;
    let sql = match prepared::command(sql)? {
        Some(Command::Prepare { name, sql: statement }) => {
            // Trino checks the statement's syntax when it is prepared.
            datafusion::sql::parser::DFParser::parse_sql(&statement)
                .map_err(|e| QueryError::from_engine(&e, &statement, &Everything))?;
            let added = format!(
                "{}={}",
                session::percent_encode(&name),
                session::percent_encode(&statement)
            );
            return Ok(answered(query, "PREPARE", vec![(session::ADDED_PREPARE, added)]));
        }
        Some(Command::Deallocate { name }) => {
            if !session.prepared.contains_key(&name) {
                return Err(not_found_prepared(&name));
            }
            let removed = session::percent_encode(&name);
            return Ok(answered(
                query,
                "DEALLOCATE",
                vec![(session::DEALLOCATED_PREPARE, removed)],
            ));
        }
        Some(Command::Execute { name, parameters }) => {
            let statement = session
                .prepared
                .get(&name)
                .ok_or_else(|| not_found_prepared(&name))?;
            bound = prepared::bind(statement, &parameters)?;
            bound.as_str()
        }
        Some(Command::ExecuteImmediate {
            sql: statement,
            parameters,
        }) => {
            bound = prepared::bind(&statement, &parameters)?;
            bound.as_str()
        }
        None => sql,
    };
    if let Some(control) = session::control(sql, &session, state.engine.session_properties(), &names)? {
        return Ok(answered(
            query,
            control.update_type(),
            control_headers(&control, &session),
        ));
    }
    exec.shape = shape(sql);
    let work = if let Some(filter) = show_session_filter(sql)? {
        let like = match &filter {
            ShowSession::All => None,
            ShowSession::Like(like) => Some(like),
        };
        Work::Frame(Box::new(
            show_session(&state.engine, &session, like).map_err(|e| QueryError::from(&e))?,
        ))
    } else if let Some(table) = describe_target(sql) {
        Work::Describe(table)
    } else {
        Work::Sql {
            planned: rewrite::name_unnamed_columns(sql),
            sql: sql.to_owned(),
        }
    };
    let limit = session.properties.max_run_time();
    let options = QueryOptions {
        catalog: session.catalog,
        schema: session.schema,
        properties: session.properties,
        functions: vec![super::system_tables::version_function(PROTOCOL_VERSION)],
        ..QueryOptions::for_user(&session.user)
    };
    tokio::spawn(execute(
        Arc::clone(&state.engine),
        Arc::clone(query),
        options,
        work,
        sender,
        limit,
    ));
    Ok((Page::default(), Vec::new(), Some(1)))
}

fn control_headers(control: &Control, session: &ClientSession) -> Vec<(&'static str, String)> {
    match control {
        Control::Use { catalog, schema } => {
            let mut headers = Vec::new();
            if let Some(catalog) = catalog.as_ref().or(session.catalog.as_ref()) {
                headers.push((session::SET_CATALOG, catalog.clone()));
            }
            headers.push((session::SET_SCHEMA, schema.clone()));
            headers
        }
        Control::SetSession { name, value } => {
            vec![(
                session::SET_SESSION,
                format!("{name}={}", session::percent_encode(value)),
            )]
        }
        Control::ResetSession { name } => vec![(session::CLEAR_SESSION, name.clone())],
    }
}

/// A property value as Trino prints it: durations in airlift's text (`100.00d`).
fn property_text(property_type: PropertyType, value: &str) -> String {
    match property_type {
        PropertyType::Duration => parse_duration(value).map_or_else(|| value.to_owned(), format_duration),
        _ => value.to_owned(),
    }
}

/// `SHOW SESSION`: every property (matching `like`) with its value in this session.
fn show_session(
    engine: &Engine,
    session: &ClientSession,
    like: Option<&Like>,
) -> datafusion::error::Result<DataFrame> {
    let rows: Vec<_> = engine
        .session_properties()
        .describe()
        .into_iter()
        .filter(|(name, _)| like.is_none_or(|like| like.matches(name)))
        .collect();
    let column = |f: &dyn Fn(&str, &codelake_core::SessionProperty) -> String| -> Arc<dyn Array> {
        Arc::new(
            rows.iter()
                .map(|(name, p)| Some(f(name, p)))
                .collect::<StringArray>(),
        )
    };
    let schema = Arc::new(Schema::new(
        ["Name", "Value", "Default", "Type", "Description"]
            .map(|name| Field::new(name, DataType::Utf8, false))
            .to_vec(),
    ));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            column(&|name, _| name.to_owned()),
            column(&|name, p| {
                session
                    .properties
                    .get(name)
                    .map_or_else(|| property_text(p.property_type, &p.default), str::to_owned)
            }),
            column(&|_, p| property_text(p.property_type, &p.default)),
            column(&|_, p| p.property_type.name().to_owned()),
            column(&|_, p| p.description.clone()),
        ],
    )?;
    engine.context().read_batch(batch)
}

/// `DESCRIBE <table>` as Trino answers it: `Column, Type, Extra, Comment`, with
/// Trino type names.
async fn describe(engine: &Engine, options: &QueryOptions, table: &str) -> Result<DataFrame, QueryError> {
    let names = EngineNames(engine);
    let target = engine
        .sql_with(options, &format!("SELECT * FROM {table}"))
        .await
        .map_err(|e| QueryError::from_engine(e.find_root(), "", &names))?;
    let fields = target.schema().fields();
    let mut column_names = Vec::with_capacity(fields.len());
    let mut type_names = Vec::with_capacity(fields.len());
    for field in fields {
        column_names.push(field.name().clone());
        type_names.push(types::trino_type(field.data_type(), Encoding::MODERN)?.0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("Column", DataType::Utf8, false),
        Field::new("Type", DataType::Utf8, false),
        Field::new("Extra", DataType::Utf8, true),
        Field::new("Comment", DataType::Utf8, true),
    ]));
    let empty = vec![None::<String>; column_names.len()];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(column_names)),
            Arc::new(StringArray::from(type_names)),
            Arc::new(StringArray::from(empty.clone())),
            Arc::new(StringArray::from(empty)),
        ],
    )
    .map_err(|e| QueryError::from(&DataFusionError::from(e)))?;
    engine
        .context()
        .read_batch(batch)
        .map_err(|e| QueryError::from(&e))
}

async fn plan(engine: &Engine, options: &QueryOptions, work: Work) -> Result<DataFrame, QueryError> {
    let names = EngineNames(engine);
    match work {
        Work::Frame(frame) => Ok(*frame),
        Work::Describe(table) => describe(engine, options, &table).await,
        Work::Sql { sql, planned } => {
            let Some(planned) = planned else {
                return engine
                    .sql_with(options, &sql)
                    .await
                    .map_err(|e| QueryError::from_engine(&e, &sql, &names));
            };
            match engine.sql_with(options, &planned).await {
                Ok(frame) => Ok(frame),
                // Report the error on the client's own text: its positions.
                Err(error) => match engine.sql_with(options, &sql).await {
                    Err(original) => Err(QueryError::from_engine(&original, &sql, &names)),
                    Ok(_) => Err(QueryError::from_engine(error.find_root(), &sql, &names)),
                },
            }
        }
    }
}

/// Wraps every operator so it yields to the scheduler after a budget of
/// batches. Operators that emit many batches per input batch (cross joins,
/// `UNNEST`) otherwise keep a worker busy without ever giving a canceled query
/// the chance to stop.
fn cooperative(plan: Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        Ok(if node.as_any().is::<CooperativeExec>() {
            Transformed::no(node)
        } else {
            Transformed::yes(Arc::new(CooperativeExec::new(node)) as Arc<dyn ExecutionPlan>)
        })
    })
    .map(|transformed| transformed.data)
}

async fn run(
    engine: &Engine,
    query: &TrinoQuery,
    options: QueryOptions,
    work: Work,
    sender: &mpsc::Sender<Event>,
) -> Result<(), QueryError> {
    let error = |e: DataFusionError| QueryError::from(&e);
    query.set_state(QueryState::Planning);
    let analysis = Instant::now();
    let frame = plan(engine, &options, work).await?;
    let analysis = analysis.elapsed();
    let planning = Instant::now();
    let task = Arc::new(frame.task_ctx());
    let physical = frame.create_physical_plan().await.map_err(error)?;
    let mut stream = execute_stream(cooperative(physical).map_err(error)?, task).map_err(error)?;
    query.record_timing(analysis, planning.elapsed());
    query.set_state(QueryState::Running);
    if sender.send(Event::Started(stream.schema())).await.is_err() {
        return Ok(());
    }
    // Batches waiting for the client count against the memory limit.
    let buffer = MemoryConsumer::new(format!("Trino output buffer {}", query.id))
        .register(&engine.context().runtime_env().memory_pool);
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(error)?;
        let reservation = buffer.new_empty();
        reservation
            .try_grow(batch.get_array_memory_size())
            .map_err(error)?;
        let buffered = Buffered {
            batch,
            _reservation: reservation,
        };
        if sender.send(Event::Batch(buffered)).await.is_err() {
            // Receiver gone: the query failed or was dropped from the registry.
            return Ok(());
        }
    }
    Ok(())
}

/// Runs `work`, failing with `EXCEEDED_TIME_LIMIT` once `(limit, remaining)` runs out.
async fn within(
    deadline: Option<(Duration, Duration)>,
    work: impl Future<Output = Result<(), QueryError>>,
) -> Result<(), QueryError> {
    let Some((limit, remaining)) = deadline else {
        return work.await;
    };
    tokio::time::timeout(remaining, work).await.unwrap_or_else(|_| {
        Err(QueryError::new(
            Code::ExceededTimeLimit,
            format!("Query exceeded maximum time limit of {}", format_duration(limit)),
        ))
    })
}

async fn execute(
    engine: Arc<Engine>,
    query: Arc<TrinoQuery>,
    options: QueryOptions,
    work: Work,
    sender: mpsc::Sender<Event>,
    limit: Option<Duration>,
) {
    let cancel = query.cancellation();
    let deadline = limit.map(|limit| (limit, limit.saturating_sub(query.snapshot().created_at.elapsed())));
    let work = within(deadline, run(&engine, &query, options, work, &sender));
    // Dropping `work` drops the plan's streams, which abort the tasks they spawned.
    let outcome = tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        outcome = work => outcome,
    };
    match outcome {
        Ok(()) => {
            query.set_state(QueryState::Finishing);
            let _ = sender.send(Event::Done).await;
        }
        Err(error) => {
            query.fail(error.clone());
            let _ = sender.send(Event::Failed(error)).await;
        }
    }
}

fn find(state: &TrinoState, id: &str, slug: &str) -> Option<Arc<TrinoQuery>> {
    state.queries.get(id).filter(|q| q.slug == slug)
}

async fn fetch(
    State(state): State<Arc<TrinoState>>,
    Path((id, slug, token)): Path<(String, String, u64)>,
    headers: HeaderMap,
) -> Response {
    let Some(query) = find(&state, &id, &slug) else {
        return (StatusCode::NOT_FOUND, "Query not found").into_response();
    };
    query.heartbeat();
    let base = state.base_uri(&headers);
    let mut exec = query.exec.lock().await;
    if let Some((last, body)) = &exec.last
        && *last == token
    {
        return json_response(body.clone(), &[]);
    }
    if token > exec.next_token {
        return (StatusCode::NOT_FOUND, "Query not found").into_response();
    }
    if token < exec.next_token {
        return (StatusCode::GONE, "Invalid token").into_response();
    }
    let page = exec.page(&query).await;
    if query.error().is_some() {
        // Failed while this page was built: nothing buffered is needed anymore.
        exec.release();
    }
    let next = (!page.done).then_some(token + 1);
    let body = TrinoState::body(&query, &base, &page, next, &exec.warnings);
    exec.last = Some((token, body.clone()));
    exec.next_token = token + 1;
    json_response(body, &[])
}

async fn cancel_statement(
    State(state): State<Arc<TrinoState>>,
    Path((id, slug, _token)): Path<(String, String, u64)>,
) -> StatusCode {
    match find(&state, &id, &slug) {
        Some(query) => {
            query.fail(user_canceled());
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_FOUND,
    }
}

/// `DELETE /v1/query/{id}`: only the query's owner may cancel it by id.
async fn kill_query(
    State(state): State<Arc<TrinoState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> StatusCode {
    let Some(query) = state.queries.get(&id) else {
        return StatusCode::NOT_FOUND;
    };
    let user = headers
        .get(session::USER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim);
    if user != Some(query.snapshot().user.as_str()) {
        return StatusCode::FORBIDDEN;
    }
    query.fail(user_canceled());
    StatusCode::NO_CONTENT
}

async fn info(State(state): State<Arc<TrinoState>>) -> Response {
    let body = json!({
        "nodeVersion": {"version": PROTOCOL_VERSION},
        "environment": "codelake",
        "coordinator": true,
        "starting": false,
        "uptime": format_duration(state.started.elapsed()),
    });
    json_response(Bytes::from(body.to_string()), &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_shapes() {
        assert_eq!(shape("SELECT 1"), Shape::Rows);
        assert_eq!(shape("INSERT INTO t SELECT 1"), Shape::Count("INSERT"));
        assert_eq!(
            shape("CREATE TABLE t AS SELECT 1 AS a"),
            Shape::Count("CREATE TABLE")
        );
        assert_eq!(shape("DELETE FROM t WHERE a = 1"), Shape::Count("DELETE"));
        assert_eq!(shape("UPDATE t SET b = 'q' WHERE a = 2"), Shape::Count("UPDATE"));
        assert_eq!(shape("CREATE TABLE t (a INT)"), Shape::Ack("CREATE TABLE"));
        assert_eq!(shape("CREATE VIEW v AS SELECT 1"), Shape::Ack("CREATE VIEW"));
        assert_eq!(shape("CREATE SCHEMA s"), Shape::Ack("CREATE SCHEMA"));
        assert_eq!(shape("DROP TABLE t"), Shape::Ack("DROP TABLE"));
        assert_eq!(shape("DROP VIEW IF EXISTS v"), Shape::Ack("DROP VIEW"));
        assert_eq!(shape("ALTER TABLE t ADD COLUMN c INT"), Shape::Ack("ADD COLUMN"));
        assert_eq!(shape("ALTER TABLE t RENAME TO u"), Shape::Ack("RENAME TABLE"));
        assert_eq!(shape("ALTER VIEW v RENAME TO w"), Shape::Ack("RENAME VIEW"));
        assert_eq!(
            shape("CREATE OR REPLACE VIEW v SECURITY DEFINER AS SELECT 1"),
            Shape::Ack("CREATE VIEW")
        );
        assert_eq!(shape("SELECT 1 +"), Shape::Rows);
    }

    #[test]
    fn show_session_and_describe_are_recognized() {
        assert_eq!(
            show_session_filter("  show   SESSION ;").unwrap(),
            Some(ShowSession::All)
        );
        assert_eq!(show_session_filter("SHOW SESSIONS").unwrap(), None);
        assert_eq!(show_session_filter("SHOW TABLES").unwrap(), None);
        let Some(ShowSession::Like(like)) =
            show_session_filter("SHOW SESSION LIKE 'query\\_max%' ESCAPE '\\'").unwrap()
        else {
            panic!("not a LIKE filter");
        };
        assert!(like.matches("query_max_run_time"));
        assert!(!like.matches("queryXmax_run_time"));
        assert!(show_session_filter("SHOW SESSION LIKE").is_err());
        assert_eq!(
            describe_target("DESCRIBE system.runtime.queries").as_deref(),
            Some("system.runtime.queries")
        );
        assert_eq!(describe_target("DESCRIBE SELECT 1"), None);
        assert_eq!(describe_target("SELECT 1"), None);
    }

    #[test]
    fn like_patterns() {
        let like = |pattern: &str| Like {
            pattern: pattern.into(),
            escape: None,
        };
        assert!(like("query_max_run%").matches("query_max_run_time"));
        assert!(like("%time").matches("query_max_run_time"));
        assert!(like("%").matches(""));
        assert!(like("a_c").matches("abc"));
        assert!(!like("a_c").matches("ac"));
        assert!(!like("query").matches("query_max_run_time"));
        assert!(like("%a%b%").matches("xxaxxbxx"));
    }

    #[test]
    fn durations_format_like_airlift() {
        assert_eq!(format_duration(Duration::from_hours(1)), "1.00h");
        assert_eq!(format_duration(Duration::from_secs(90)), "1.50m");
        assert_eq!(format_duration(Duration::from_millis(250)), "250.00ms");
        assert_eq!(format_duration(Duration::from_hours(48)), "2.00d");
        assert_eq!(property_text(PropertyType::Duration, "100d"), "100.00d");
        assert_eq!(property_text(PropertyType::Boolean, "true"), "true");
    }

    #[test]
    fn row_counts() {
        use datafusion::arrow::array::{Int64Array, UInt64Array};
        let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![3, 4]))]).unwrap();
        assert_eq!(row_count(&batch), Some(7));
        let schema = Arc::new(Schema::new(vec![Field::new("rows", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![5]))]).unwrap();
        assert_eq!(row_count(&batch), Some(5));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
        assert_eq!(row_count(&batch), None);
    }
}
