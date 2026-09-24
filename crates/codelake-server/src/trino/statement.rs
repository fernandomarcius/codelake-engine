//! `/v1/statement`: submit, page and cancel queries.
//!
//! Each query runs in its own task that plans it and pushes record batches into
//! a small bounded channel, so execution moves ahead of the client by at most a
//! few batches, like Trino's output buffer. `GET nextUri` drains the channel
//! into a page of about 1 MiB of JSON. Pages are numbered by the token in the
//! URI; asking again for the current token returns the same page (a client
//! retry after a lost response), any other token is refused.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use codelake_core::{Engine, QueryOptions};
use datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, UInt64Type};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::dataframe::DataFrame;
use futures::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::mpsc::{self, error::TryRecvError};

use super::errors::{Code, QueryError};
use super::queries::{Info, Limits, QueriesTable, Query, Registry, State as QueryState};
use super::session::{self, ClientSession, Control};
use super::types::{self, Column};

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
    /// `INSERT` and `CREATE TABLE AS`: one `rows` column with the row count.
    Count(&'static str),
    /// Other DDL: one `result` column with `true`.
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
    use datafusion::sql::sqlparser::ast::{ObjectType, Statement};
    let Ok(mut statements) = DFParser::parse_sql(sql) else {
        // The engine reports the parse error.
        return Shape::Rows;
    };
    match statements.pop_front() {
        Some(DfStatement::Statement(statement)) => match *statement {
            Statement::Insert(_) => Shape::Count("INSERT"),
            Statement::CreateTable(create) if create.query.is_some() => Shape::Count("CREATE TABLE"),
            Statement::CreateTable(_) => Shape::Ack("CREATE TABLE"),
            Statement::CreateView(_) => Shape::Ack("CREATE VIEW"),
            Statement::CreateSchema { .. } => Shape::Ack("CREATE SCHEMA"),
            Statement::AlterTable(_) => Shape::Ack("ADD COLUMN"),
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

fn is_show_session(sql: &str) -> bool {
    let words: Vec<_> = sql.trim().trim_end_matches(';').split_whitespace().collect();
    matches!(words.as_slice(), [show, session] if show.eq_ignore_ascii_case("show") && session.eq_ignore_ascii_case("session"))
}

enum Event {
    Started(SchemaRef),
    Batch(RecordBatch),
    Done,
    Failed(QueryError),
}

enum Work {
    Sql(String),
    Frame(Box<DataFrame>),
}

/// Paging state of one query, behind the query's async mutex.
pub(crate) struct Exec {
    events: mpsc::Receiver<Event>,
    shape: Shape,
    columns: Option<Arc<Vec<Column>>>,
    count: Option<u64>,
    pending: Option<(RecordBatch, usize)>,
    finished: bool,
    next_token: u64,
    last: Option<(u64, Bytes)>,
    warnings: Vec<Value>,
}

impl Exec {
    fn new(events: mpsc::Receiver<Event>, shape: Shape, warnings: Vec<Value>) -> Self {
        Self {
            events,
            shape,
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
        let Some((batch, mut offset)) = self.pending.take() else {
            return Ok(());
        };
        let start = offset;
        while offset < batch.num_rows() && *bytes < PAGE_TARGET_BYTES {
            let row = batch
                .columns()
                .iter()
                .map(|column| types::value(column, offset))
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map_err(|e| QueryError::from(&e))?;
            *bytes += row.iter().map(approximate_size).sum::<usize>();
            rows.push(row);
            offset += 1;
        }
        query.add_rows(u64::try_from(offset - start).unwrap_or(u64::MAX));
        if offset < batch.num_rows() {
            self.pending = Some((batch, offset));
        }
        Ok(())
    }

    fn absorb(&mut self, event: Event) -> Result<(), QueryError> {
        match event {
            Event::Started(schema) => {
                if self.shape == Shape::Rows {
                    self.columns = Some(Arc::new(types::columns(&schema)));
                }
            }
            Event::Batch(batch) => match self.shape {
                Shape::Rows => self.pending = Some((batch, 0)),
                Shape::Count(_) | Shape::Ack(_) => {
                    if let Some(rows) = row_count(&batch) {
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
            self.finished = true;
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
            columns: self.columns.clone().or_else(|| Some(Arc::new(Vec::new()))),
            data: (!rows.is_empty()).then_some(rows),
            done: true,
            update_type: self.shape.update_type(),
            ..Page::default()
        };
        if self.shape != Shape::Rows {
            let (column, value) = match self.count {
                Some(count) => {
                    page.update_count = Some(count);
                    (("rows", DataType::Int64), json!(count))
                }
                None => (("result", DataType::Boolean), json!(true)),
            };
            let schema = Schema::new(vec![Field::new(column.0, column.1, false)]);
            page.columns = Some(Arc::new(types::columns(&schema)));
            page.data = Some(vec![vec![value]]);
        }
        page
    }

    fn fail(&mut self, query: &TrinoQuery, error: QueryError) -> Page {
        self.finished = true;
        self.pending = None;
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

    /// Catalog `system` with `runtime.queries`.
    pub(crate) fn system_catalog(&self) -> datafusion::error::Result<Arc<dyn CatalogProvider>> {
        let runtime = MemorySchemaProvider::new();
        runtime.register_table(
            "queries".into(),
            Arc::new(QueriesTable::new(Arc::clone(&self.queries))),
        )?;
        let catalog = MemoryCatalogProvider::new();
        catalog.register_schema("runtime", Arc::new(runtime))?;
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

async fn submit(State(state): State<Arc<TrinoState>>, headers: HeaderMap, body: Bytes) -> Response {
    if state.draining.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Server is shutting down").into_response();
    }
    let Ok(sql) = std::str::from_utf8(&body).map(str::to_owned) else {
        return (StatusCode::BAD_REQUEST, "SQL statement is not valid UTF-8").into_response();
    };
    if sql.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "SQL statement is empty").into_response();
    }
    let Some(user) = headers
        .get(session::USER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(str::to_owned)
    else {
        return (StatusCode::BAD_REQUEST, "User must be set").into_response();
    };
    let source = headers
        .get(session::SOURCE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let base = state.base_uri(&headers);
    let registry = state.engine.session_properties();

    let session = ClientSession::from_headers(&headers, registry);
    let shape = shape(&sql);
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
    let (sender, receiver) = mpsc::channel(BUFFERED_BATCHES);
    let query = Arc::new(Query::new(
        state.queries.next_id(),
        user,
        source,
        sql.clone(),
        Exec::new(receiver, shape, warnings),
    ));
    if let Err(error) = state.queries.insert(Arc::clone(&query)) {
        query.fail(error.clone());
        return json_response(
            TrinoState::body(&query, &base, &Page::failed(error), None, &[]),
            &[],
        );
    }

    match start(&state, &query, sql, session, sender) {
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

/// Session-control statements answer at once; anything else starts executing
/// and the client follows `nextUri`.
fn start(
    state: &TrinoState,
    query: &Arc<TrinoQuery>,
    sql: String,
    session: Result<ClientSession, QueryError>,
    sender: mpsc::Sender<Event>,
) -> Result<Started, QueryError> {
    let session = session?;
    if let Some(control) = session::control(&sql, &session, state.engine.session_properties())? {
        query.set_state(QueryState::Running);
        query.finish();
        let page = Page {
            done: true,
            update_type: Some(control.update_type()),
            ..Page::default()
        };
        return Ok((page, control_headers(&control, &session), None));
    }
    let work = if is_show_session(&sql) {
        Work::Frame(Box::new(
            show_session(&state.engine, &session).map_err(|e| QueryError::from(&e))?,
        ))
    } else {
        Work::Sql(sql)
    };
    let limit = session.properties.max_run_time();
    let options = QueryOptions {
        catalog: session.catalog,
        schema: session.schema,
        properties: session.properties,
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

/// `SHOW SESSION`: every property with its value in this session.
fn show_session(engine: &Engine, session: &ClientSession) -> datafusion::error::Result<DataFrame> {
    let rows = engine.session_properties().describe();
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
            column(&|name, p| session.properties.get(name).unwrap_or(&p.default).to_owned()),
            column(&|_, p| p.default.clone()),
            column(&|_, p| p.property_type.name().to_owned()),
            column(&|_, p| p.description.clone()),
        ],
    )?;
    engine.context().read_batch(batch)
}

async fn run(
    engine: &Engine,
    query: &TrinoQuery,
    options: QueryOptions,
    work: Work,
    sender: &mpsc::Sender<Event>,
) -> datafusion::error::Result<()> {
    query.set_state(QueryState::Planning);
    let analysis = Instant::now();
    let frame = match work {
        Work::Sql(sql) => engine.sql_with(&options, &sql).await?,
        Work::Frame(frame) => *frame,
    };
    let analysis = analysis.elapsed();
    let planning = Instant::now();
    let mut stream = frame.execute_stream().await?;
    query.record_timing(analysis, planning.elapsed());
    query.set_state(QueryState::Running);
    if sender.send(Event::Started(stream.schema())).await.is_err() {
        return Ok(());
    }
    while let Some(batch) = stream.next().await {
        if sender.send(Event::Batch(batch?)).await.is_err() {
            // Receiver gone: the query was dropped from the registry.
            return Ok(());
        }
    }
    Ok(())
}

/// Runs `work`, failing with `EXCEEDED_TIME_LIMIT` once `(limit, remaining)` runs out.
async fn within(
    deadline: Option<(Duration, Duration)>,
    work: impl Future<Output = datafusion::error::Result<()>>,
) -> Result<(), QueryError> {
    let Some((limit, remaining)) = deadline else {
        return work.await.map_err(|e| QueryError::from(&e));
    };
    match tokio::time::timeout(remaining, work).await {
        Ok(result) => result.map_err(|e| QueryError::from(&e)),
        Err(_) => Err(QueryError::new(
            Code::ExceededTimeLimit,
            format!("Query exceeded maximum time limit of {}", format_duration(limit)),
        )),
    }
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
    let outcome = tokio::select! {
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
    if token != exec.next_token {
        return (StatusCode::GONE, "Invalid token").into_response();
    }
    let page = exec.page(&query).await;
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
        assert_eq!(shape("CREATE TABLE t (a INT)"), Shape::Ack("CREATE TABLE"));
        assert_eq!(shape("CREATE VIEW v AS SELECT 1"), Shape::Ack("CREATE VIEW"));
        assert_eq!(shape("CREATE SCHEMA s"), Shape::Ack("CREATE SCHEMA"));
        assert_eq!(shape("DROP TABLE t"), Shape::Ack("DROP TABLE"));
        assert_eq!(shape("DROP VIEW IF EXISTS v"), Shape::Ack("DROP VIEW"));
        assert_eq!(shape("ALTER TABLE t ADD COLUMN c INT"), Shape::Ack("ADD COLUMN"));
        assert_eq!(shape("SELECT 1 +"), Shape::Rows);
        assert!(is_show_session("  show   SESSION ;"));
        assert!(!is_show_session("SHOW SESSIONS"));
    }

    #[test]
    fn durations_format_like_airlift() {
        assert_eq!(format_duration(Duration::from_hours(1)), "1.00h");
        assert_eq!(format_duration(Duration::from_secs(90)), "1.50m");
        assert_eq!(format_duration(Duration::from_millis(250)), "250.00ms");
        assert_eq!(format_duration(Duration::from_hours(48)), "2.00d");
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
