//! `/v1/statement`: submit, page and cancel queries.
//!
//! Each query runs in its own task that plans it and pushes record batches into
//! a small bounded channel, so execution moves ahead of the client by at most a
//! few batches, like Trino's output buffer. `GET nextUri` drains the channel
//! into a page of about the size the client asks for (`targetResultSize`, 1 MiB
//! of JSON by default, 128 MiB at most): once it has rows, it keeps taking the
//! batches that follow each other closely and sends the page when the size is
//! reached or execution pauses. Pages are numbered by the token in the
//! URI; asking again for the current token returns the same page (a client
//! retry after a lost response), an older token is gone (`410`) and a later one
//! unknown (`404`).
//!
//! Memory: every buffer between the engine and the client is reserved in the
//! engine's pool — the batches in the channel and the one being paged, the
//! rows of the page being built and the body kept for a retry. A reservation
//! the pool refuses fails the query with `EXCEEDED_LOCAL_MEMORY_LIMIT` (a page
//! that already has rows is sent short instead). A failed, canceled or
//! abandoned query drops all of it at once — once the requests paging it at
//! that moment have their (same) page — and so does a query whose final page
//! has been sent: that page is not kept for a retry (`410` instead).
//!
//! Every operator of the physical plan is made cooperative, so a canceled,
//! abandoned or timed-out query stops within one budget of batches even when
//! an operator produces rows without waiting for input (a cross join).
//!
//! Admission: a query whose execution waits for its client (the channel full)
//! for longer than `[governance] suspend_after_secs` gives its running slot
//! back, and takes one again before it produces the next batch
//! ([`governance::Slot`], ADR 0016 amended for D4): a client that went away
//! without canceling no longer holds a running slot of its user until it is
//! declared abandoned.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use codelake_core::governance::{self, KillError, Permit, Protocol, Slot, Ticket, TimeLimits};
use codelake_core::output::ReservedOutputExec;
use codelake_core::session_properties::{format_duration, parse_duration};
use codelake_core::trace::QueryTrace;
use codelake_core::{Engine, PropertyType, QueryOptions};
use datafusion::arrow::array::{Array, AsArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, UInt64Type};
use datafusion::catalog::CatalogProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use futures::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::errors::{Code, Everything, Namespace, QueryError};
use super::prepared::{self, Command};
use super::queries::{Info, Limits, Query, Registry, Release, State as QueryState};
use super::rewrite;
use super::session::{self, ClientSession, Control, SessionError};
use super::types::{self, Column, Encoding};

/// JSON per page when the client asks for no size; Trino's default target
/// result size is 1 MB.
const PAGE_TARGET_BYTES: usize = 1 << 20;
/// Largest page a client may ask for (as the reference engine).
const MAX_PAGE_TARGET_BYTES: usize = 128 << 20;
/// Longest a `GET nextUri` waits for rows before answering with none.
const MAX_WAIT: Duration = Duration::from_secs(1);
/// Once a page has rows, how long it waits for the next batch before it is
/// sent short: execution that produces small batches back to back fills the
/// page, one that pauses does not hold the rows it has.
const FILL_WAIT: Duration = Duration::from_millis(10);
/// Batches produced ahead of the client.
const BUFFERED_BATCHES: usize = 2;

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
    let typed = codelake_core::trino_type_syntax(sql);
    let Ok(mut statements) = DFParser::parse_sql(&codelake_core::engine::trino_text(&typed)) else {
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

/// The table of `DESCRIBE <table>` or `SHOW COLUMNS FROM <table> [LIKE
/// 'pattern']` (Trino answers both with `Column, Type, Extra, Comment`), and
/// the pattern.
fn describe_target(sql: &str) -> Option<(String, Option<Like>)> {
    use datafusion::sql::parser::{DFParser, Statement as DfStatement};
    use datafusion::sql::sqlparser::ast::{
        DescribeAlias, ShowStatementFilter, ShowStatementFilterPosition, ShowStatementIn, Statement,
    };
    let mut statements = DFParser::parse_sql(sql).ok()?;
    match statements.pop_front()? {
        DfStatement::Statement(statement) => match *statement {
            Statement::ExplainTable {
                describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                hive_format: None,
                table_name,
                ..
            } => Some((table_name.to_string(), None)),
            Statement::ShowColumns {
                extended: false,
                full: false,
                show_options,
            } => {
                let Some(ShowStatementIn {
                    parent_name: Some(table),
                    ..
                }) = show_options.show_in
                else {
                    return None;
                };
                let like = match show_options.filter_position {
                    None => None,
                    Some(
                        ShowStatementFilterPosition::Infix(ShowStatementFilter::Like(pattern))
                        | ShowStatementFilterPosition::Suffix(ShowStatementFilter::Like(pattern)),
                    ) => Some(Like {
                        pattern,
                        escape: None,
                    }),
                    Some(_) => return None,
                };
                Some((table.to_string(), like))
            }
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
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(rest)
        .map_err(|e| syntax(e.to_string()))?;
    let Some(like) = like_clause(&mut parser)? else {
        return Err(syntax("SHOW SESSION expects LIKE 'pattern'".into()));
    };
    if parser.peek_token().token != Token::EOF {
        return Err(syntax("Unexpected text after SHOW SESSION LIKE".into()));
    }
    Ok(Some(ShowSession::Like(like)))
}

fn syntax(message: String) -> QueryError {
    QueryError::new(Code::SyntaxError, message)
}

/// `LIKE 'pattern' [ESCAPE 'c']`, when the next tokens are one.
fn like_clause(parser: &mut Parser<'_>) -> Result<Option<Like>, QueryError> {
    if !parser.parse_keyword(Keyword::LIKE) {
        return Ok(None);
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
    Ok(Some(Like { pattern, escape }))
}

/// `SHOW CATALOGS` and `SHOW SCHEMAS`, which `DataFusion` does not plan.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Listing {
    /// `SHOW CATALOGS [LIKE 'pattern' [ESCAPE 'c']]`.
    Catalogs(Option<Like>),
    /// `SHOW SCHEMAS [FROM | IN catalog] [LIKE 'pattern' [ESCAPE 'c']]`.
    Schemas {
        catalog: Option<String>,
        like: Option<Like>,
    },
}

/// The listing `sql` asks for; `None` for other statements.
fn listing(sql: &str) -> Result<Option<Listing>, QueryError> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let Some(rest) = session::strip_keyword(text, "SHOW") else {
        return Ok(None);
    };
    let (what, rest) = rest
        .split_once(char::is_whitespace)
        .map_or((rest, ""), |(what, rest)| (what, rest.trim_start()));
    let catalogs = what.eq_ignore_ascii_case("CATALOGS");
    if !catalogs && !what.eq_ignore_ascii_case("SCHEMAS") {
        return Ok(None);
    }
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(rest)
        .map_err(|e| syntax(e.to_string()))?;
    let mut catalog = None;
    if !catalogs
        && parser
            .parse_one_of_keywords(&[Keyword::FROM, Keyword::IN])
            .is_some()
    {
        let name = parser.parse_identifier().map_err(|e| syntax(e.to_string()))?;
        // Catalog names are case-insensitive, quoted or not.
        catalog = Some(name.value.to_lowercase());
    }
    let like = like_clause(&mut parser)?;
    if parser.peek_token().token != Token::EOF {
        return Err(syntax(format!(
            "Unexpected text after SHOW {}",
            what.to_uppercase()
        )));
    }
    Ok(Some(if catalogs {
        Listing::Catalogs(like)
    } else {
        Listing::Schemas { catalog, like }
    }))
}

/// The work of a listing: `SHOW CATALOGS` answers from the catalogs a client
/// lists (`system.jdbc.catalogs`), as one `Catalog` column typed
/// `varchar(n)`, `n` the longest name (Trino lists them as a `VALUES`);
/// `SHOW SCHEMAS` reads the catalog's `information_schema.schemata`, as
/// Trino rewrites it.
fn listing_work(engine: &Engine, session: &ClientSession, listing: Listing) -> Result<Work, QueryError> {
    match listing {
        Listing::Catalogs(like) => {
            let state = engine.context().state();
            let names = super::system_tables::listed_catalogs(state.catalog_list().as_ref());
            let width = names.iter().map(|name| name.chars().count()).max().unwrap_or(0);
            let shown: StringArray = names
                .iter()
                .filter(|name| like.as_ref().is_none_or(|like| like.matches(name)))
                .map(Some)
                .collect();
            let declared =
                codelake_core::trino_types::Declared::Varchar(u32::try_from(width).unwrap_or(u32::MAX));
            let schema = Arc::new(Schema::new(vec![
                Field::new("Catalog", DataType::Utf8, false).with_metadata(declared.metadata()),
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(shown)])
                .map_err(|e| QueryError::from(&DataFusionError::from(e)))?;
            let frame = engine
                .context()
                .read_batch(batch)
                .map_err(|e| QueryError::from(&e))?;
            Ok(Work::Frame(Box::new(frame)))
        }
        Listing::Schemas { catalog, like } => {
            let Some(catalog) = catalog.or_else(|| session.catalog.clone()) else {
                return Err(QueryError::new(
                    Code::MissingCatalogName,
                    "line 1:1: Catalog must be specified when session catalog is not set",
                ));
            };
            if !EngineNames(engine).has_catalog(&catalog) {
                return Err(QueryError::new(
                    Code::CatalogNotFound,
                    format!("Catalog '{catalog}' does not exist"),
                ));
            }
            let quoted = |text: &str, quote: char| {
                let doubled: String = [quote, quote].iter().collect();
                format!("{quote}{}{quote}", text.replace(quote, &doubled))
            };
            let filter = like.map_or_else(String::new, |like| {
                let escape = like.escape.map_or_else(String::new, |c| {
                    format!(" ESCAPE {}", quoted(&c.to_string(), '\''))
                });
                format!(" WHERE schema_name LIKE {}{escape}", quoted(&like.pattern, '\''))
            });
            Ok(Work::Sql {
                sql: format!(
                    "SELECT schema_name AS \"Schema\" FROM {}.information_schema.schemata{filter} ORDER BY schema_name",
                    quoted(&catalog, '"')
                ),
                planned: None,
            })
        }
    }
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
    Describe(String, Option<Like>),
}

/// Paging state of one query, behind the query's async mutex.
pub(crate) struct Exec {
    events: mpsc::Receiver<Event>,
    shape: Shape,
    encoding: Encoding,
    columns: Option<Arc<Vec<Column>>>,
    /// The Trino type each result column declares beyond its Arrow type.
    declared: Vec<Option<codelake_core::trino_types::Declared>>,
    /// The output items as the statement writes them: the case of the names.
    written: Vec<rewrite::Written>,
    count: Option<u64>,
    pending: Option<(Buffered, usize)>,
    finished: bool,
    next_token: u64,
    last: Option<(u64, Bytes)>,
    /// The rows of the page being built, then the body in `last`.
    memory: MemoryReservation,
    warnings: Vec<Value>,
    /// The final page of a statement answered without execution (session
    /// control, prepared statements, a kill), sent on the first `nextUri`.
    answer: Option<Page>,
}

impl Release for Exec {
    /// Drops every buffered batch and the last page (and their reservations)
    /// and stops the producer, whose next send fails.
    fn release(&mut self) {
        self.events.close();
        while self.events.try_recv().is_ok() {}
        self.pending = None;
        self.finished = true;
        self.last = None;
        self.answer = None;
        self.memory.free();
    }
}

impl Exec {
    fn new(
        events: mpsc::Receiver<Event>,
        encoding: Encoding,
        warnings: Vec<Value>,
        pool: &Arc<dyn MemoryPool>,
        id: &str,
    ) -> Self {
        Self {
            events,
            shape: Shape::Rows,
            encoding,
            columns: None,
            declared: Vec::new(),
            written: Vec::new(),
            count: None,
            pending: None,
            finished: false,
            next_token: 1,
            last: None,
            memory: MemoryConsumer::new(format!("Trino page {id}")).register(pool),
            warnings,
            answer: None,
        }
    }

    /// Encodes pending rows until the page is full, reserving each row. A row
    /// the pool cannot hold ends the page early, or fails the query when the
    /// page has no row yet.
    fn take_rows(
        &mut self,
        query: &TrinoQuery,
        rows: &mut Vec<Vec<Value>>,
        bytes: &mut usize,
        target: usize,
    ) -> Result<(), QueryError> {
        let Some((buffered, mut offset)) = self.pending.take() else {
            return Ok(());
        };
        let start = offset;
        let batch = &buffered.batch;
        let mut refused = None;
        while offset < batch.num_rows() && *bytes < target {
            let row = batch
                .columns()
                .iter()
                .enumerate()
                .map(|(i, column)| {
                    let declared = self.declared.get(i).copied().flatten();
                    types::declared_value(column, offset, self.encoding, declared)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Err(error) = self.memory.try_grow(row_memory(&row)) {
                refused = Some(error);
                break;
            }
            *bytes += row.iter().map(approximate_size).sum::<usize>();
            rows.push(row);
            offset += 1;
        }
        query.add_rows(u64::try_from(offset - start).unwrap_or(u64::MAX));
        if offset < batch.num_rows() {
            self.pending = Some((buffered, offset));
        }
        match refused {
            Some(error) if rows.is_empty() => Err(QueryError::from(&error)),
            // Sent short: the next page starts at the refused row.
            Some(_) => {
                *bytes = target;
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn absorb(&mut self, event: Event) -> Result<(), QueryError> {
        match event {
            Event::Started(schema) => {
                if self.shape == Shape::Rows {
                    let mut columns = types::columns(&schema, self.encoding)?;
                    rewrite::restore_written(
                        columns.iter_mut().map(|c| &mut c.name).collect(),
                        &self.written,
                    );
                    self.columns = Some(Arc::new(columns));
                    self.declared = schema
                        .fields()
                        .iter()
                        .map(|f| codelake_core::trino_types::declared(f))
                        .collect();
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

    /// Produces the next page, of about `target` bytes of rows: rows, a
    /// final result, or an error.
    async fn page(&mut self, query: &TrinoQuery, target: usize) -> Page {
        if let Some(error) = query.error() {
            self.release();
            return Page::failed(error);
        }
        if let Some(answer) = self.answer.take() {
            return answer;
        }
        let deadline = tokio::time::Instant::now() + MAX_WAIT;
        let mut rows = Vec::new();
        let mut bytes = 0;
        loop {
            if let Err(error) = self.take_rows(query, &mut rows, &mut bytes, target) {
                return self.fail(query, &error);
            }
            if bytes >= target || self.pending.is_some() || self.finished {
                break;
            }
            // Wait for the first rows; once some are in hand, only briefly for more.
            let until = if rows.is_empty() {
                deadline
            } else {
                deadline.min(tokio::time::Instant::now() + FILL_WAIT)
            };
            let Ok(event) = tokio::time::timeout_at(until, self.events.recv()).await else {
                break;
            };
            let Some(event) = event else {
                // The task ended without a final event: canceled or panicked.
                let error = query.error().unwrap_or_else(|| {
                    QueryError::new(Code::GenericInternalError, "Query execution ended unexpectedly")
                });
                return self.fail(query, &error);
            };
            if let Err(error) = self.absorb(event) {
                return self.fail(query, &error);
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

    fn fail(&mut self, query: &TrinoQuery, error: &QueryError) -> Page {
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

/// Memory of one decoded row: its JSON text plus the `Value`s holding it.
fn row_memory(row: &[Value]) -> usize {
    row.iter()
        .map(|value| approximate_size(value) + std::mem::size_of::<Value>())
        .sum::<usize>()
        + std::mem::size_of::<Vec<Value>>()
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
    /// Response headers: what a session-control statement changes.
    headers: Vec<(HeaderName, String)>,
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
    /// Header names under the configured prefix (ISS-011).
    headers: session::HeaderNames,
    /// Version the server reports (`/v1/info`, `version()`).
    version: String,
}

impl std::fmt::Debug for TrinoState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoState")
            .field("queries", &self.queries)
            .finish_non_exhaustive()
    }
}

impl TrinoState {
    pub(crate) fn new(
        engine: Arc<Engine>,
        limits: Limits,
        listen: SocketAddr,
        header_prefix: &str,
        version: &str,
    ) -> Arc<Self> {
        Arc::new(Self {
            headers: session::HeaderNames::new(header_prefix),
            version: version.to_owned(),
            queries: Arc::new(Registry::new(Arc::clone(engine.governance().registry()), limits)),
            engine,
            fallback_host: listen.to_string(),
            started: Instant::now(),
            draining: AtomicBool::new(false),
        })
    }

    /// Catalog `system`: the engine's `runtime.queries` (every protocol's
    /// queries) plus this protocol's `metadata.*` and `jdbc.*`.
    pub(crate) fn system_catalog(&self) -> datafusion::error::Result<Arc<dyn CatalogProvider>> {
        let catalog = governance::system_catalog(self.engine.governance().registry())?;
        super::system_tables::register(&catalog, self.engine.connectors())?;
        Ok(catalog as Arc<dyn CatalogProvider>)
    }

    pub(crate) fn sweep(&self) {
        self.queries.sweep();
    }

    fn pool(&self) -> Arc<dyn MemoryPool> {
        Arc::clone(&self.engine.context().runtime_env().memory_pool)
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
        Self::body_as(query, base, page, next_token, warnings, &query.snapshot())
    }

    /// [`Self::body`] with the given statistics.
    fn body_as(
        query: &TrinoQuery,
        base: &str,
        page: &Page,
        next_token: Option<u64>,
        warnings: &[Value],
        info: &Info,
    ) -> Bytes {
        let results = QueryResults {
            id: &query.id,
            info_uri: format!("{base}/ui/query.html?{}", query.id),
            next_uri: next_token
                .map(|t| format!("{base}/v1/statement/executing/{}/{}/{t}", query.id, query.slug)),
            columns: page.columns.as_deref().map(Vec::as_slice),
            data: page.data.as_deref(),
            stats: stats(info),
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

fn json_response(body: Bytes, headers: &[(HeaderName, String)]) -> Response {
    let mut response = (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], body).into_response();
    for (name, value) in headers {
        if let Ok(value) = HeaderValue::from_str(value) {
            response.headers_mut().append(name.clone(), value);
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

/// The answer to every accepted submission, whatever becomes of the
/// statement: `QUEUED` with the first `nextUri`, where the outcome is (rows,
/// a failure, or the page of a statement answered at once), as the reference
/// engine answers. Clients rely on it: one (.NET) follows `nextUri` without
/// reading anything else of this response.
fn accepted(query: &TrinoQuery, base: &str) -> Response {
    let mut info = query.snapshot();
    info.state = QueryState::Queued;
    json_response(
        TrinoState::body_as(query, base, &Page::default(), Some(1), &[], &info),
        &[],
    )
}

/// Fails a query that has not run, keeping it reachable for its first page.
fn fail_indexed(state: &TrinoState, query: &Arc<TrinoQuery>, error: &QueryError) {
    query.fail(error);
    // A full index refused it unfinished; failed, it takes no slot.
    if state.queries.get(&query.id).is_none() {
        let _ = state.queries.insert(Arc::clone(query));
    }
}

/// Registers a query that failed before running and answers with its failure.
fn refuse(
    state: &TrinoState,
    base: &str,
    user: &str,
    source: Option<&str>,
    sql: &str,
    error: QueryError,
) -> Response {
    let (_, receiver) = mpsc::channel(1);
    let record = state
        .engine
        .governance()
        .register(Protocol::Http, user, source, sql);
    let exec = Exec::new(receiver, Encoding::MODERN, Vec::new(), &state.pool(), record.id());
    let query = Arc::new(Query::new(record, exec));
    // A full registry reports its own error instead.
    let error = state.queries.insert(Arc::clone(&query)).err().unwrap_or(error);
    fail_indexed(state, &query, &error);
    accepted(&query, base)
}

/// Warnings for the session properties this engine accepts and ignores.
fn ignored_properties(session: Option<&ClientSession>) -> Vec<Value> {
    session.map_or_else(Vec::new, |s| {
        s.properties
            .ignored()
            .iter()
            .map(|name| warning(&format!("Session property {name} has no effect in CodeLake")))
            .collect()
    })
}

async fn submit(State(state): State<Arc<TrinoState>>, request: Request) -> Response {
    if state.draining.load(Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "Server is shutting down").into_response();
    }
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let Some(user) = headers
        .get(&state.headers.user)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(str::to_owned)
    else {
        return (StatusCode::UNAUTHORIZED, "User must be set").into_response();
    };
    let source = headers
        .get(&state.headers.source)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let base = state.base_uri(&headers);
    let max_length = state.queries.limits().max_query_length;
    let Ok(body) = axum::body::to_bytes(body, body_limit(max_length)).await else {
        let declared = headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        return refuse(
            &state,
            &base,
            &user,
            source.as_deref(),
            "",
            too_large(declared, max_length),
        );
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
            &user,
            source.as_deref(),
            &kept,
            too_large(Some(length), max_length),
        );
    }
    let registry = state.engine.session_properties();
    let session = match ClientSession::from_headers(&headers, registry, &state.headers) {
        Ok(session) => Ok(session),
        Err(SessionError::BadRequest(message)) => {
            return (StatusCode::BAD_REQUEST, message).into_response();
        }
        Err(SessionError::Query(error)) => Err(error),
    };
    let warnings = ignored_properties(session.as_ref().ok());
    let encoding = session.as_ref().map_or(Encoding::MODERN, |s| s.encoding);
    let (sender, receiver) = mpsc::channel(BUFFERED_BATCHES);
    let record = state
        .engine
        .governance()
        .register(Protocol::Http, &user, source.as_deref(), sql);
    let exec = Exec::new(receiver, encoding, warnings, &state.pool(), record.id());
    let query = Arc::new(Query::new(record, exec));
    if let Err(error) = state.queries.insert(Arc::clone(&query)) {
        fail_indexed(&state, &query, &error);
        return accepted(&query, &base);
    }

    // Nobody else can reach the query before this response: the lock is free.
    let mut exec = query.exec.lock().await;
    let started = start(&state, &query, &mut exec, sql, session, sender);
    drop(exec);
    if let Err(error) = started {
        query.fail(&error);
    }
    accepted(&query, &base)
}

/// A statement answered at once, without execution: done now, its page (and
/// the headers that change the client's session) sent on the first `nextUri`.
fn answered(
    query: &TrinoQuery,
    exec: &mut Exec,
    update_type: &'static str,
    headers: Vec<(HeaderName, String)>,
) {
    query.set_state(QueryState::Running);
    query.finish();
    exec.answer = Some(Page {
        columns: Some(Arc::new(Vec::new())),
        done: true,
        update_type: Some(update_type),
        headers,
        ..Page::default()
    });
}

fn not_found_prepared(name: &str) -> QueryError {
    QueryError::new(Code::NotFound, format!("Prepared statement not found: {name}"))
}

/// Session-control and prepared-statement commands are answered at once;
/// anything else starts executing.
fn start(
    state: &TrinoState,
    query: &Arc<TrinoQuery>,
    exec: &mut Exec,
    sql: &str,
    session: Result<ClientSession, QueryError>,
    sender: mpsc::Sender<Event>,
) -> Result<(), QueryError> {
    let session = session?;
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
            answered(
                query,
                exec,
                "PREPARE",
                vec![(state.headers.added_prepare.clone(), added)],
            );
            return Ok(());
        }
        Some(Command::Deallocate { name }) => {
            if !session.prepared.contains_key(&name) {
                return Err(not_found_prepared(&name));
            }
            let removed = session::percent_encode(&name);
            answered(
                query,
                exec,
                "DEALLOCATE",
                vec![(state.headers.deallocated_prepare.clone(), removed)],
            );
            return Ok(());
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
    if answered_at_once(state, query, exec, sql, &session)? {
        return Ok(());
    }
    exec.shape = shape(sql);
    let work = if let Some(listing) = listing(sql)? {
        listing_work(&state.engine, &session, listing)?
    } else if let Some(filter) = show_session_filter(sql)? {
        let like = match &filter {
            ShowSession::All => None,
            ShowSession::Like(like) => Some(like),
        };
        Work::Frame(Box::new(
            show_session(&state.engine, &session, like).map_err(|e| QueryError::from(&e))?,
        ))
    } else if let Some((table, like)) = describe_target(sql) {
        Work::Describe(table, like)
    } else {
        exec.written = rewrite::written_names(sql);
        Work::Sql {
            planned: rewrite::name_unnamed_columns(sql),
            sql: sql.to_owned(),
        }
    };
    let governance = state.engine.governance();
    let limits = governance.session_limits(&session.properties);
    // Refused here, the client reads the failure on the first page.
    let ticket = governance
        .enter(query.record())
        .map_err(|e| QueryError::from_governance(&e))?;
    let options = QueryOptions {
        catalog: session.catalog,
        schema: session.schema,
        time_zone: session.time_zone,
        properties: session.properties,
        functions: vec![super::system_tables::version_function(&state.version)],
        // The query span carries the id clients see.
        trace: QueryTrace::default().with_query_id(query.id.clone()),
        ..QueryOptions::for_user(&session.user)
    };
    tokio::spawn(execute(
        Arc::clone(&state.engine),
        Arc::clone(query),
        options,
        work,
        sender,
        (ticket, limits),
    ));
    Ok(())
}

/// Answers a kill or a session-control statement at once; `false` for any
/// other statement.
fn answered_at_once(
    state: &TrinoState,
    query: &TrinoQuery,
    exec: &mut Exec,
    sql: &str,
    session: &ClientSession,
) -> Result<bool, QueryError> {
    // Killing a query answers at once, without a slot: it works on a full node.
    if let Some(request) = governance::kill_request_sql(sql) {
        request
            .and_then(|request| state.engine.governance().kill(&request, &session.user))
            .map_err(|error| QueryError::from_kill(&error))?;
        answered(query, exec, "CALL", Vec::new());
        return Ok(true);
    }
    let names = EngineNames(&state.engine);
    let Some(control) = session::control(sql, session, state.engine.session_properties(), &names)? else {
        return Ok(false);
    };
    answered(
        query,
        exec,
        control.update_type(),
        control_headers(&control, session, &state.headers),
    );
    Ok(true)
}

fn control_headers(
    control: &Control,
    session: &ClientSession,
    names: &session::HeaderNames,
) -> Vec<(HeaderName, String)> {
    match control {
        Control::Use { catalog, schema } => {
            let mut headers = Vec::new();
            if let Some(catalog) = catalog.as_ref().or(session.catalog.as_ref()) {
                headers.push((names.set_catalog.clone(), catalog.clone()));
            }
            headers.push((names.set_schema.clone(), schema.clone()));
            headers
        }
        Control::SetSession { name, value } => {
            vec![(
                names.set_session.clone(),
                format!("{name}={}", session::percent_encode(value)),
            )]
        }
        Control::ResetSession { name } => vec![(names.clear_session.clone(), name.clone())],
        Control::SetTimeZone { zone: Some(zone) } => vec![(
            names.set_session.clone(),
            format!("{}={}", session::TIME_ZONE_ID, session::percent_encode(zone)),
        )],
        Control::SetTimeZone { zone: None } => {
            vec![(names.clear_session.clone(), session::TIME_ZONE_ID.to_owned())]
        }
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

/// `DESCRIBE <table>` and `SHOW COLUMNS` as Trino answers them: `Column,
/// Type, Extra, Comment`, with Trino type names and empty `Extra` and
/// `Comment`.
async fn describe(
    engine: &Engine,
    options: &QueryOptions,
    table: &str,
    like: Option<&Like>,
) -> Result<DataFrame, QueryError> {
    let names = EngineNames(engine);
    let target = engine
        .sql_with(options, &format!("SELECT * FROM {table}"))
        .await
        .map_err(|e| QueryError::from_engine(e.find_root(), "", &names))?;
    let schema = codelake_core::engine::analyzed_schema(&engine.context().state(), target.logical_plan())
        .map_err(|e| QueryError::from_engine(e.find_root(), "", &names))?;
    let fields = schema.fields();
    let mut column_names = Vec::with_capacity(fields.len());
    let mut type_names = Vec::with_capacity(fields.len());
    for field in fields
        .iter()
        .filter(|f| like.is_none_or(|like| like.matches(f.name())))
    {
        column_names.push(field.name().clone());
        type_names.push(codelake_core::trino_types::field_display_name(field));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("Column", DataType::Utf8, false),
        Field::new("Type", DataType::Utf8, false),
        Field::new("Extra", DataType::Utf8, false),
        Field::new("Comment", DataType::Utf8, false),
    ]));
    let empty = vec![String::new(); column_names.len()];
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

async fn plan(
    engine: &Engine,
    options: &QueryOptions,
    work: Work,
    admission: &Info,
) -> Result<DataFrame, QueryError> {
    let names = EngineNames(engine);
    // The query span records where governance admitted the query.
    let traced = |query: codelake_core::Query| {
        query.record_admission(&admission.resource_group, admission.queued);
        query.into_dataframe()
    };
    match work {
        Work::Frame(frame) => Ok(*frame),
        Work::Describe(table, like) => describe(engine, options, &table, like.as_ref()).await,
        Work::Sql { sql, planned } => {
            let Some(planned) = planned else {
                return engine
                    .query(options, &sql)
                    .await
                    .map(traced)
                    .map_err(|e| QueryError::from_engine(&e, &sql, &names));
            };
            match engine.query(options, &planned).await.map(traced) {
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
        Ok(if node.is::<CooperativeExec>() {
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
    slot: &mut Slot,
) -> Result<(), QueryError> {
    let error = |e: DataFusionError| QueryError::from(&e);
    query.set_state(QueryState::Planning);
    let analysis = Instant::now();
    let frame = plan(engine, &options, work, &query.snapshot()).await?;
    let analysis = analysis.elapsed();
    let planning = Instant::now();
    let task = Arc::new(frame.task_ctx());
    let mut physical = frame.create_physical_plan().await.map_err(error)?;
    // This protocol reserves the batches it holds itself (`Buffered`).
    if let Some(reserved) = physical.downcast_ref::<ReservedOutputExec>() {
        physical = Arc::clone(reserved.input());
    }
    let mut stream = execute_stream(cooperative(physical).map_err(error)?, task).map_err(error)?;
    query.record_timing(analysis, planning.elapsed());
    query.set_state(QueryState::Running);
    if sender.send(Event::Started(stream.schema())).await.is_err() {
        return Ok(());
    }
    // Batches waiting for the client count against the memory limit.
    let buffer = MemoryConsumer::new(format!("Trino output buffer {}", query.id))
        .register(&engine.context().runtime_env().memory_pool);
    let suspend_after = engine.governance().suspend_after();
    loop {
        // A query that gave its slot back while its client did not read
        // takes one again before it works on the next batch.
        if !slot.is_running() {
            slot.resume().await;
            tracing::debug!(query_id = %query.id, "query resumed: its client reads again");
        }
        let Some(batch) = stream.next().await else {
            return Ok(());
        };
        let batch = batch.map_err(error)?;
        let reservation = buffer.new_empty();
        reservation
            .try_grow(batch.get_array_memory_size())
            .map_err(error)?;
        let buffered = Buffered {
            batch,
            _reservation: reservation,
        };
        let room = match sender.try_reserve() {
            Ok(room) => Ok(room),
            Err(mpsc::error::TrySendError::Closed(())) => return Ok(()),
            // The client has not read the batches ahead of this one yet.
            Err(mpsc::error::TrySendError::Full(())) => {
                let room = slot.wait_for_client(suspend_after, sender.reserve()).await;
                if !slot.is_running() {
                    tracing::debug!(query_id = %query.id, "query suspended: its client did not read");
                }
                room
            }
        };
        let Ok(room) = room else {
            // Receiver gone: the query failed or was dropped from the registry.
            return Ok(());
        };
        room.send(Event::Batch(buffered));
    }
}

/// Runs `work` until the query's deadline; past it, `work` is dropped (its
/// streams with it, which stops the operators and frees their memory) and the
/// query fails with `EXCEEDED_TIME_LIMIT`.
async fn within(
    engine: &Engine,
    query: &TrinoQuery,
    limits: &TimeLimits,
    work: impl Future<Output = Result<(), QueryError>>,
) -> Result<(), QueryError> {
    let (deadline, limit) = limits.deadline(&query.snapshot());
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), work)
        .await
        .unwrap_or_else(|_| {
            let error = engine.governance().time_limit_exceeded(query.record(), limit);
            Err(QueryError::from_governance(&error))
        })
}

/// Waits for an admission slot, then runs the query holding it; the slot is
/// freed as soon as execution ends, before the client has fetched the last
/// buffered rows, and given back while execution waits for a client that does
/// not read (see [`run`]).
async fn execute(
    engine: Arc<Engine>,
    query: Arc<TrinoQuery>,
    options: QueryOptions,
    work: Work,
    sender: mpsc::Sender<Event>,
    (ticket, limits): (Ticket, TimeLimits),
) {
    let cancel = query.record().cancellation();
    let permit: Permit = match engine.governance().admit(ticket, query.record(), &limits).await {
        Ok(permit) => permit,
        Err(error) => {
            let error = query
                .error()
                .unwrap_or_else(|| QueryError::from_governance(&error));
            query.fail(&error);
            let _ = sender.send(Event::Failed(error)).await;
            return;
        }
    };
    let mut slot = Slot::new(permit);
    let work = within(
        &engine,
        &query,
        &limits,
        run(&engine, &query, options, work, &sender, &mut slot),
    );
    // Dropping `work` drops the plan's streams, which abort the tasks they spawned.
    let outcome = tokio::select! {
        biased;
        () = cancel.cancelled() => return,
        outcome = work => outcome,
    };
    drop(slot);
    match outcome {
        Ok(()) => {
            query.set_state(QueryState::Finishing);
            let _ = sender.send(Event::Done).await;
        }
        Err(error) => {
            query.fail(&error);
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
    RawQuery(parameters): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(query) = find(&state, &id, &slug) else {
        return (StatusCode::NOT_FOUND, "Query not found").into_response();
    };
    query.heartbeat();
    let base = state.base_uri(&headers);
    fetch_page(&query, &base, token, page_target(parameters.as_deref())).await
}

/// The page size a client asks for (`?targetResultSize=5MB`), up to
/// [`MAX_PAGE_TARGET_BYTES`] and at least one row; [`PAGE_TARGET_BYTES`]
/// when it asks for none or for a size that cannot be read.
fn page_target(parameters: Option<&str>) -> usize {
    parameters
        .into_iter()
        .flat_map(|text| text.split('&'))
        .find_map(|pair| pair.strip_prefix("targetResultSize="))
        .and_then(data_size)
        .map_or(PAGE_TARGET_BYTES, |bytes| bytes.clamp(1, MAX_PAGE_TARGET_BYTES))
}

/// A data size as the reference engine writes it: a number, possibly with a
/// fraction, then `B`, `kB`, `MB`, `GB`, `TB` or `PB` (powers of 1024).
fn data_size(text: &str) -> Option<usize> {
    let split = text.find(|c: char| c.is_ascii_alphabetic())?;
    let (number, unit) = text.split_at(split);
    let shift = match unit {
        "B" => 0,
        "kB" => 10,
        "MB" => 20,
        "GB" => 30,
        "TB" => 40,
        "PB" => 50,
        _ => return None,
    };
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    let (whole, fraction) = match number.split_once('.') {
        Some((whole, fraction)) if digits(fraction) => (whole, fraction),
        Some(_) => return None,
        None => (number, "0"),
    };
    if !digits(whole) {
        return None;
    }
    let scale = 10_u128.checked_pow(u32::try_from(fraction.len()).ok()?)?;
    let scaled = whole
        .parse::<u128>()
        .ok()?
        .checked_mul(scale)?
        .checked_add(fraction.parse().ok()?)?;
    // Past the ceiling anyway: saturate instead of failing.
    let bytes = scaled.checked_mul(1 << shift).map_or(u128::MAX, |b| b / scale);
    Some(usize::try_from(bytes).unwrap_or(usize::MAX))
}

/// Counts a page request while it lives, also when the client goes away
/// mid-request (the handler's future is dropped).
struct Fetching<'a>(&'a TrinoQuery);

impl<'a> Fetching<'a> {
    fn new(query: &'a TrinoQuery) -> Self {
        query.enter_fetch();
        Self(query)
    }
}

impl Drop for Fetching<'_> {
    fn drop(&mut self) {
        self.0.leave_fetch();
    }
}

async fn fetch_page(query: &TrinoQuery, base: &str, token: u64, target: usize) -> Response {
    // Declared before the `exec` guard, so dropped after it: the last request
    // out of a canceled query can then release what the cancel left.
    let _fetching = Fetching::new(query);
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
    let mut page = exec.page(query, target).await;
    let failed = query.error().is_some();
    if failed {
        // Failed while this page was built: no buffered batch is needed anymore.
        exec.release();
    }
    let next = (!page.done).then_some(token + 1);
    let body = TrinoState::body(query, base, &page, next, &exec.warnings);
    let headers = std::mem::take(&mut page.headers);
    drop(page);
    if page_is_final(next, failed) {
        // The query finished: nothing is kept for it, not even this page.
        exec.last = None;
        exec.memory.free();
    } else {
        // Kept for a retry of this token; a failed query drops it once no
        // request for it is left (`Query::leave_fetch`).
        exec.last = Some((token, body.clone()));
        // The body replaces the rows it was encoded from (reserved above
        // them) and the previous body.
        exec.memory.resize(body.len());
    }
    exec.next_token = token + 1;
    json_response(body, &headers)
}

/// The last page of a query that finished (not failed).
fn page_is_final(next: Option<u64>, failed: bool) -> bool {
    next.is_none() && !failed
}

async fn cancel_statement(
    State(state): State<Arc<TrinoState>>,
    Path((id, slug, _token)): Path<(String, String, u64)>,
) -> StatusCode {
    match find(&state, &id, &slug) {
        Some(query) => {
            query.fail(&user_canceled());
            StatusCode::NO_CONTENT
        }
        None => StatusCode::NOT_FOUND,
    }
}

/// `DELETE /v1/query/{id}`: only the query's owner may cancel it by id. The
/// query may run on another protocol (the PostgreSQL wire): it is then
/// canceled in the engine's registry, and its protocol stops it.
async fn kill_query(
    State(state): State<Arc<TrinoState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> StatusCode {
    let user = headers
        .get(&state.headers.user)
        .and_then(|v| v.to_str().ok())
        .map(str::trim);
    let Some(query) = state.queries.get(&id) else {
        return match state.engine.governance().cancel(&id, user.unwrap_or_default()) {
            // As for this protocol's queries: canceling a finished one is a no-op.
            Ok(()) | Err(KillError::NotRunning(_)) => StatusCode::NO_CONTENT,
            Err(KillError::NotOwner(_)) => StatusCode::FORBIDDEN,
            Err(_) => StatusCode::NOT_FOUND,
        };
    };
    if user != Some(query.snapshot().user.as_str()) {
        return StatusCode::FORBIDDEN;
    }
    query.fail(&user_canceled());
    StatusCode::NO_CONTENT
}

async fn info(State(state): State<Arc<TrinoState>>) -> Response {
    let body = json!({
        "nodeVersion": {"version": state.version},
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
    fn catalog_and_schema_listings_are_recognized() {
        let like = |pattern: &str, escape: Option<char>| {
            Some(Like {
                pattern: pattern.to_owned(),
                escape,
            })
        };
        assert_eq!(
            listing(" show  CATALOGS ;").unwrap(),
            Some(Listing::Catalogs(None))
        );
        assert_eq!(
            listing("SHOW CATALOGS LIKE 'h%'").unwrap(),
            Some(Listing::Catalogs(like("h%", None)))
        );
        assert_eq!(
            listing("SHOW SCHEMAS").unwrap(),
            Some(Listing::Schemas {
                catalog: None,
                like: None
            })
        );
        assert_eq!(
            listing("show schemas from Hive like 'x\\_%' escape '\\'").unwrap(),
            Some(Listing::Schemas {
                catalog: Some("hive".into()),
                like: like("x\\_%", Some('\\'))
            })
        );
        assert_eq!(
            listing("SHOW SCHEMAS IN \"PG\"").unwrap(),
            Some(Listing::Schemas {
                catalog: Some("pg".into()),
                like: None
            })
        );
        for other in [
            "SHOW TABLES",
            "SHOW SESSION",
            "SHOW CATALOGSX",
            "SELECT 1",
            "SHOW",
        ] {
            assert_eq!(listing(other).unwrap(), None, "{other}");
        }
        for wrong in [
            "SHOW CATALOGS FROM x",
            "SHOW SCHEMAS FROM",
            "SHOW SCHEMAS LIKE",
            "SHOW CATALOGS LIKE 'a' extra",
        ] {
            assert_eq!(listing(wrong).unwrap_err().name(), "SYNTAX_ERROR", "{wrong}");
        }
        assert_eq!(
            listing("SHOW CATALOGS LIKE 'a' ESCAPE 'ab'").unwrap_err().name(),
            "GENERIC_USER_ERROR"
        );
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
            describe_target("DESCRIBE system.runtime.queries"),
            Some(("system.runtime.queries".to_owned(), None))
        );
        assert_eq!(
            describe_target("SHOW COLUMNS FROM hive.s.t"),
            Some(("hive.s.t".to_owned(), None))
        );
        let Some((table, Some(like))) = describe_target("SHOW COLUMNS FROM hive.s.t LIKE 'd%'") else {
            panic!("SHOW COLUMNS … LIKE");
        };
        assert_eq!(table, "hive.s.t");
        assert!(like.matches("dc") && !like.matches("id"));
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
    fn pages_have_the_size_clients_ask_for() {
        let asked = |value: &str| page_target(Some(&format!("targetResultSize={value}")));
        assert_eq!(page_target(None), PAGE_TARGET_BYTES);
        assert_eq!(page_target(Some("maxWait=1s")), PAGE_TARGET_BYTES);
        assert_eq!(asked("5MB"), 5 << 20);
        assert_eq!(page_target(Some("maxWait=1s&targetResultSize=1.5kB")), 1536);
        assert_eq!(asked("200B"), 200);
        assert_eq!(asked("0.25MB"), 1 << 18);
        assert_eq!(asked("2GB"), MAX_PAGE_TARGET_BYTES, "within the ceiling");
        assert_eq!(asked("128MB"), MAX_PAGE_TARGET_BYTES);
        assert_eq!(
            asked("100000000000000000000000000000000000PB"),
            MAX_PAGE_TARGET_BYTES
        );
        assert_eq!(asked("0B"), 1, "at least one row a page");
        for unreadable in [
            "5mb", "5 MB", "MB", "5", "1.MB", ".5MB", "-1MB", "5MiB", "1.2.3MB", "1e3B",
        ] {
            assert_eq!(asked(unreadable), PAGE_TARGET_BYTES, "{unreadable}");
        }
        for (text, bytes) in [
            ("3B", 3),
            ("3kB", 3 << 10),
            ("3GB", 3 << 30),
            ("3TB", 3 << 40),
            ("3PB", 3 << 50),
        ] {
            assert_eq!(data_size(text), Some(bytes), "{text}");
        }
    }

    #[test]
    fn durations_format_like_airlift() {
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
