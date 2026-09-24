//! Query registry of the Trino protocol and its SQL view,
//! `system.runtime.queries`, with Trino's columns.
//!
//! Lifecycle, as in Trino: a query is `QUEUED` when submitted, `PLANNING` while
//! it is analyzed, `RUNNING` while it produces rows, `FINISHING` once the last
//! row is produced and the client has not fetched it yet, then `FINISHED` or
//! `FAILED`. A client that stops polling for `client_timeout` abandons its
//! query, which is then canceled (`ABANDONED_QUERY`). Completed queries stay
//! listed for `history_retention`, at most `history_size` of them — enforced on
//! every insertion, so the registry never holds more than `max_active +
//! history_size` queries, each with at most `max_query_length` characters of
//! SQL. A failed query drops its buffered results at once ([`Release`]); only
//! its record stays in the history.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{MemTable, Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use tokio_util::sync::CancellationToken;

use super::errors::{Code, QueryError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Queued,
    Planning,
    Running,
    Finishing,
    Finished,
    Failed,
}

impl State {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Planning => "PLANNING",
            Self::Running => "RUNNING",
            Self::Finishing => "FINISHING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
        }
    }

    pub(crate) fn is_done(self) -> bool {
        matches!(self, Self::Finished | Self::Failed)
    }
}

/// What `system.runtime.queries` shows about one query.
#[derive(Debug, Clone)]
pub(crate) struct Info {
    pub(crate) user: String,
    pub(crate) source: Option<String>,
    /// Shared, so snapshots do not copy the text.
    pub(crate) query: Arc<str>,
    pub(crate) state: State,
    pub(crate) created: SystemTime,
    pub(crate) created_at: Instant,
    pub(crate) started: Option<SystemTime>,
    pub(crate) ended: Option<SystemTime>,
    pub(crate) ended_at: Option<Instant>,
    pub(crate) last_heartbeat: SystemTime,
    pub(crate) heartbeat_at: Instant,
    pub(crate) queued: Duration,
    pub(crate) analysis: Duration,
    pub(crate) planning: Duration,
    pub(crate) rows: u64,
    pub(crate) error: Option<QueryError>,
}

pub(crate) struct Query<E> {
    pub(crate) id: String,
    /// Unguessable part of every URI of this query.
    pub(crate) slug: String,
    info: Mutex<Info>,
    pub(crate) exec: tokio::sync::Mutex<E>,
    cancel: CancellationToken,
}

impl<E> std::fmt::Debug for Query<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Query")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Paging state a failed query no longer needs (its buffered results).
pub(crate) trait Release {
    fn release(&mut self);
}

impl Release for () {
    fn release(&mut self) {}
}

impl<E: Release> Query<E> {
    pub(crate) fn new(id: String, user: String, source: Option<String>, query: &str, exec: E) -> Self {
        let now = SystemTime::now();
        let at = Instant::now();
        Self {
            id,
            slug: uuid::Uuid::new_v4().simple().to_string(),
            info: Mutex::new(Info {
                user,
                source,
                query: Arc::from(query),
                state: State::Queued,
                created: now,
                created_at: at,
                started: None,
                ended: None,
                ended_at: None,
                last_heartbeat: now,
                heartbeat_at: at,
                queued: Duration::ZERO,
                analysis: Duration::ZERO,
                planning: Duration::ZERO,
                rows: 0,
                error: None,
            }),
            exec: tokio::sync::Mutex::new(exec),
            cancel: CancellationToken::new(),
        }
    }

    fn info(&self) -> MutexGuard<'_, Info> {
        // A panic while holding this lock leaves plain data behind; keep going.
        self.info
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn snapshot(&self) -> Info {
        self.info().clone()
    }

    pub(crate) fn is_done(&self) -> bool {
        self.info().state.is_done()
    }

    /// When the query finished or failed.
    fn ended_at(&self) -> Option<Instant> {
        self.info().ended_at
    }

    pub(crate) fn heartbeat(&self) {
        let mut info = self.info();
        info.heartbeat_at = Instant::now();
        info.last_heartbeat = SystemTime::now();
    }

    /// Moves forward only; a finished or failed query never changes state.
    pub(crate) fn set_state(&self, state: State) {
        let mut info = self.info();
        if info.state.is_done() {
            return;
        }
        if info.state == State::Queued && state != State::Queued {
            info.queued = info.created_at.elapsed();
            info.started = Some(SystemTime::now());
        }
        info.state = state;
        if state.is_done() {
            info.ended = Some(SystemTime::now());
            info.ended_at = Some(Instant::now());
        }
    }

    pub(crate) fn record_timing(&self, analysis: Duration, planning: Duration) {
        let mut info = self.info();
        info.analysis = analysis;
        info.planning = planning;
    }

    pub(crate) fn add_rows(&self, rows: u64) {
        self.info().rows += rows;
    }

    /// Records the first failure (later ones are consequences of it), stops
    /// the execution and drops the buffered results. When a page is being
    /// built right now, that request releases them when it ends.
    pub(crate) fn fail(&self, error: QueryError) {
        {
            let mut info = self.info();
            if info.state.is_done() {
                return;
            }
            info.error = Some(error);
        }
        self.set_state(State::Failed);
        self.cancel.cancel();
        if let Ok(mut exec) = self.exec.try_lock() {
            exec.release();
        }
    }

    pub(crate) fn finish(&self) {
        self.set_state(State::Finished);
    }

    pub(crate) fn error(&self) -> Option<QueryError> {
        self.info().error.clone()
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) max_active: usize,
    pub(crate) client_timeout: Duration,
    pub(crate) history_retention: Duration,
    pub(crate) history_size: usize,
    /// Longest SQL text accepted, in characters (Trino's `query.max-length`).
    pub(crate) max_query_length: usize,
}

pub(crate) struct Registry<E> {
    queries: Mutex<HashMap<String, Arc<Query<E>>>>,
    sequence: AtomicU64,
    node: String,
    limits: Limits,
}

impl<E> std::fmt::Debug for Registry<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl<E: Release> Registry<E> {
    pub(crate) fn new(limits: Limits) -> Self {
        let node: String = uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .filter(char::is_ascii_lowercase)
            .chain("abcde".chars())
            .take(5)
            .collect();
        Self {
            queries: Mutex::new(HashMap::new()),
            sequence: AtomicU64::new(0),
            node,
            limits,
        }
    }

    fn map(&self) -> MutexGuard<'_, HashMap<String, Arc<Query<E>>>> {
        self.queries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Trino's id format: `YYYYMMDD_HHMMSS_<sequence>_<node>`, in UTC.
    pub(crate) fn next_id(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) % 100_000;
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i128::try_from(d.as_millis()).unwrap_or(0));
        let stamp = super::types::timestamp_text(millis * 1_000_000, 0);
        let compact: String = stamp.chars().filter(char::is_ascii_digit).collect();
        format!(
            "{}_{}_{sequence:05}_{}",
            &compact[..8],
            &compact[8..14],
            self.node
        )
    }

    pub(crate) fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Registers a query, forgetting completed ones beyond the history limits
    /// first, so the registry stays bounded between sweeps.
    pub(crate) fn insert(&self, query: Arc<Query<E>>) -> Result<(), QueryError> {
        let mut map = self.map();
        Self::trim_history(&mut map, &self.limits);
        let active = map.values().filter(|q| !q.is_done()).count();
        if active >= self.limits.max_active {
            return Err(QueryError::new(
                Code::QueryQueueFull,
                format!("Too many queued queries (limit {})", self.limits.max_active),
            ));
        }
        map.insert(query.id.clone(), query);
        Ok(())
    }

    /// Keeps the newest `history_size` completed queries younger than
    /// `history_retention`.
    fn trim_history(map: &mut HashMap<String, Arc<Query<E>>>, limits: &Limits) {
        let mut done: Vec<(String, Instant)> = map
            .iter()
            .filter_map(|(id, q)| q.ended_at().map(|at| (id.clone(), at)))
            .collect();
        done.sort_by_key(|(_, at)| std::cmp::Reverse(*at));
        for (index, (id, at)) in done.into_iter().enumerate() {
            if index >= limits.history_size || at.elapsed() > limits.history_retention {
                map.remove(&id);
            }
        }
    }

    pub(crate) fn get(&self, id: &str) -> Option<Arc<Query<E>>> {
        self.map().get(id).cloned()
    }

    pub(crate) fn snapshot(&self) -> Vec<(String, Info)> {
        let queries: Vec<_> = self.map().values().cloned().collect();
        queries.iter().map(|q| (q.id.clone(), q.snapshot())).collect()
    }

    /// Cancels abandoned queries and forgets old completed ones.
    pub(crate) fn sweep(&self) {
        let queries: Vec<_> = self.map().values().cloned().collect();
        for query in &queries {
            let (done, heartbeat) = {
                let info = query.info();
                (info.state.is_done(), info.heartbeat_at)
            };
            if !done && heartbeat.elapsed() > self.limits.client_timeout {
                query.fail(QueryError::new(
                    Code::AbandonedQuery,
                    format!(
                        "Query {} was abandoned by the client, as it may have exited or stopped checking for query results",
                        query.id
                    ),
                ));
            }
        }
        Self::trim_history(&mut self.map(), &self.limits);
    }
}

fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

fn duration_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// `system.runtime.queries`.
pub(crate) struct QueriesTable<E> {
    registry: Arc<Registry<E>>,
    schema: SchemaRef,
}

impl<E> std::fmt::Debug for QueriesTable<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueriesTable")
    }
}

impl<E: Release> QueriesTable<E> {
    pub(crate) fn new(registry: Arc<Registry<E>>) -> Self {
        let timestamp = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("user", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, true),
            Field::new("query", DataType::Utf8, false),
            Field::new(
                "resource_group_id",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("queued_time_ms", DataType::Int64, true),
            Field::new("analysis_time_ms", DataType::Int64, true),
            Field::new("planning_time_ms", DataType::Int64, true),
            Field::new("created", timestamp.clone(), true),
            Field::new("started", timestamp.clone(), true),
            Field::new("last_heartbeat", timestamp.clone(), true),
            Field::new("end", timestamp, true),
            Field::new("error_type", DataType::Utf8, true),
            Field::new("error_code", DataType::Utf8, true),
        ]));
        Self { registry, schema }
    }

    fn batch(&self) -> Result<RecordBatch> {
        let mut rows = self.registry.snapshot();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let text = |f: &dyn Fn(&Info) -> Option<String>| -> ArrayRef {
            Arc::new(rows.iter().map(|(_, i)| f(i)).collect::<StringArray>())
        };
        let number = |f: &dyn Fn(&Info) -> Duration| -> ArrayRef {
            Arc::new(
                rows.iter()
                    .map(|(_, i)| Some(duration_ms(f(i))))
                    .collect::<Int64Array>(),
            )
        };
        let time = |f: &dyn Fn(&Info) -> Option<SystemTime>| -> ArrayRef {
            Arc::new(
                rows.iter()
                    .map(|(_, i)| f(i).map(millis))
                    .collect::<TimestampMillisecondArray>()
                    .with_timezone("UTC"),
            )
        };
        let resource_groups: ArrayRef = Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            rows.len(),
        ));
        let columns = vec![
            Arc::new(
                rows.iter()
                    .map(|(id, _)| Some(id.as_str()))
                    .collect::<StringArray>(),
            ) as ArrayRef,
            text(&|i| Some(i.state.as_str().to_owned())),
            text(&|i| Some(i.user.clone())),
            text(&|i| i.source.clone()),
            text(&|i| Some(i.query.to_string())),
            resource_groups,
            number(&|i| i.queued),
            number(&|i| i.analysis),
            number(&|i| i.planning),
            time(&|i| Some(i.created)),
            time(&|i| i.started),
            time(&|i| Some(i.last_heartbeat)),
            time(&|i| i.ended),
            text(&|i| i.error.as_ref().map(|e| e.kind().to_owned())),
            text(&|i| i.error.as_ref().map(|e| e.name().to_owned())),
        ];
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[async_trait]
impl<E: Release + Send + 'static> TableProvider for QueriesTable<E> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A snapshot taken at scan time, like Trino's system tables.
        let table = MemTable::try_new(Arc::clone(&self.schema), vec![vec![self.batch()?]])?;
        table.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(max_active: usize) -> Registry<()> {
        Registry::new(Limits {
            max_active,
            client_timeout: Duration::from_millis(50),
            history_retention: Duration::from_secs(60),
            history_size: 2,
            max_query_length: 1000,
        })
    }

    fn query(registry: &Registry<()>) -> Arc<Query<()>> {
        Arc::new(Query::new(registry.next_id(), "u".into(), None, "SELECT 1", ()))
    }

    #[test]
    fn ids_follow_trino_format() {
        let registry = registry(10);
        let id = registry.next_id();
        let parts: Vec<_> = id.split('_').collect();
        assert_eq!(parts.len(), 4, "{id}");
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 6);
        assert_eq!(parts[2], "00000");
        assert_eq!(parts[3].len(), 5);
        assert!(registry.next_id().contains("_00001_"));
    }

    #[test]
    fn state_moves_forward_only() {
        let registry = registry(10);
        let q = query(&registry);
        q.set_state(State::Running);
        assert!(q.snapshot().started.is_some());
        q.finish();
        q.set_state(State::Running);
        q.fail(QueryError::new(Code::GenericUserError, "late"));
        let info = q.snapshot();
        assert_eq!(info.state, State::Finished);
        assert!(info.error.is_none());
        assert!(info.ended.is_some());
    }

    #[test]
    fn active_limit_rejects_with_queue_full() {
        let registry = registry(1);
        registry.insert(query(&registry)).unwrap();
        let err = registry.insert(query(&registry)).unwrap_err();
        assert_eq!(err.name(), "QUERY_QUEUE_FULL");
    }

    #[test]
    fn sweep_cancels_abandoned_and_trims_history() {
        let registry = registry(10);
        let abandoned = query(&registry);
        let cancel = abandoned.cancellation();
        registry.insert(Arc::clone(&abandoned)).unwrap();
        for _ in 0..3 {
            let done = query(&registry);
            done.finish();
            registry.insert(done).unwrap();
        }
        std::thread::sleep(Duration::from_millis(60));
        registry.sweep();
        assert_eq!(abandoned.error().unwrap().name(), "ABANDONED_QUERY");
        assert!(cancel.is_cancelled());
        // history_size = 2: the abandoned one plus the newest finished one stay.
        assert_eq!(registry.snapshot().len(), 2);
    }

    #[test]
    fn history_is_bounded_on_insert() {
        let registry = registry(10);
        for _ in 0..20 {
            let done = query(&registry);
            done.finish();
            registry.insert(done).unwrap();
        }
        // history_size = 2, plus the query just inserted; no sweep needed.
        assert_eq!(registry.snapshot().len(), 3);
    }

    struct Buffer(bool);

    impl Release for Buffer {
        fn release(&mut self) {
            self.0 = true;
        }
    }

    #[test]
    fn failing_releases_the_buffers() {
        let q = Query::new("q".into(), "u".into(), None, "SELECT 1", Buffer(false));
        q.fail(QueryError::new(Code::UserCanceled, "canceled"));
        assert!(q.exec.try_lock().unwrap().0);
        let finished = Query::new("f".into(), "u".into(), None, "SELECT 1", Buffer(false));
        finished.finish();
        finished.fail(QueryError::new(Code::UserCanceled, "late"));
        assert!(!finished.exec.try_lock().unwrap().0);
    }

    #[tokio::test]
    async fn table_lists_queries() {
        let registry = Arc::new(registry(10));
        let q = query(&registry);
        q.fail(QueryError::new(Code::SyntaxError, "bad"));
        registry.insert(q).unwrap();
        let table = QueriesTable::new(Arc::clone(&registry));
        let batch = table.batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 15);
        let state = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(state.value(0), "FAILED");
        let code = batch.column(14).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(code.value(0), "SYNTAX_ERROR");
    }
}
