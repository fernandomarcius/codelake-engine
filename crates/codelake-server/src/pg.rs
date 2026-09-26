//! PostgreSQL wire protocol under query governance (ADR 0016).
//!
//! `datafusion-postgres` parses, plans and encodes statements; this module
//! wraps its query handlers so every statement that executes goes through
//! the engine's governance first, like the HTTP statement protocol:
//!
//! - it is registered (`system.runtime.queries`), with the user of the
//!   connection's startup message and its `application_name` as source;
//! - it takes an admission slot, waits in the queue, or is refused
//!   (SQLSTATE `53400`, `configuration_limit_exceeded`);
//! - it runs until its deadline: `statement_timeout` when the session set
//!   one, else the configured `query_max_run_time`, and the configured
//!   `query_max_execution_time` from admission. Past it the result stream is
//!   dropped — also when the client stopped reading, a portal suspended by a
//!   row limit — which stops the operators and returns their memory to the
//!   pool; the client gets SQLSTATE `57014` (`query_canceled`).
//!
//! The slot is held until the statement's rows are all sent (or its stream
//! dropped): rows go to the client as the stream yields them, on the
//! extended protocol too ([`execute`]), so a client that reads slowly holds
//! its statement, and the statement's deadline, until it has read.
//! Statements the session answers itself (`SET`, `SHOW`, transaction
//! control) neither register nor take a slot.
//!
//! A cursor (`DECLARE … CURSOR`) is its `DECLARE`'s statement: registered,
//! holding its slot and its deadline until its rows are all fetched or it is
//! closed; `FETCH` and `CLOSE` take no slot of their own ([`cursor`]).
//!
//! Stopping a statement from outside, which also frees its slot and memory:
//!
//! - a cancel request (`psql`'s Ctrl-C, JDBC `Statement.cancel`,
//!   `tokio-postgres`' `cancel_query`), which names the connection by its
//!   process id and secret key: while the connection executes a message (a
//!   query, an `Execute`), every governed statement of that connection
//!   fails with `57014`, whether it waits in the queue, is planned, or sends
//!   rows — open cursors and suspended portals too, which `PostgreSQL`'s
//!   aborted transaction would close. Between messages, as in `PostgreSQL`,
//!   a cancel request does nothing: an open cursor or a portal suspended by
//!   a row limit reads on;
//! - `CALL system.runtime.kill_query(query_id => '…', message => '…')` from
//!   a connection of the query's owner, for a query of any protocol (its id
//!   is listed in `system.runtime.queries`); it answers without taking a
//!   slot, so it works on a full node.

use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_trait::async_trait;
use codelake_core::Engine;
use codelake_core::governance::{
    ADMINISTRATIVELY_KILLED, EXCEEDED_TIME_LIMIT, Failure, GovernanceError, KillError, KillRequest, Permit,
    Protocol, QUERY_QUEUE_FULL, QueryRecord, QueryState, USER_CANCELED, kill_request, kill_request_sql,
};
use datafusion::common::{DFSchema, ParamValues};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::portal::{Format, Portal, PortalExecutionState};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, SendableRowStream, Tag,
};
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, ErrorHandler, METADATA_APPLICATION_NAME, METADATA_USER,
    PgWireServerHandlers,
};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::pgwire::messages::cancel::CancelRequest;
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::pgwire::messages::extendedquery::Execute;
use datafusion_postgres::pgwire::messages::simplequery::Query;
use datafusion_postgres::{DfSessionService, QueryHook};
use futures::{Sink, Stream};
use tokio::sync::{Notify, OwnedMutexGuard};

use crate::trino::{Everything, QueryError};

mod cursor;
mod execute;

/// Client metadata key where `datafusion-postgres` keeps `SET
/// statement_timeout` (milliseconds).
const STATEMENT_TIMEOUT: &str = "statement_timeout_ms";
/// User of a connection whose startup message named none.
const UNKNOWN_USER: &str = "unknown";
/// `PostgreSQL`'s message for a statement stopped by a cancel request.
const CANCELED: &str = "canceling statement due to user request";
/// Client metadata key holding the registry id of the statement being run,
/// for the tracing hook ([`crate::hooks`]).
pub(crate) const QUERY_ID: &str = "codelake.query_id";

type DfStatement = <DfSessionService as ExtendedQueryHandler>::Statement;

/// The server's PostgreSQL handlers: `datafusion-postgres`'s, governed.
pub(crate) struct Handlers {
    service: Arc<Governed>,
    startup: Arc<Startup>,
    cancel: Arc<CancelRequests>,
}

impl Debug for Handlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Handlers")
    }
}

impl Handlers {
    /// `hooks` run after the one that plans `kill_query` calls on the
    /// extended protocol ([`KillCallHook`]).
    pub(crate) fn new(
        engine: Arc<Engine>,
        session: Arc<SessionContext>,
        hooks: Vec<Arc<dyn QueryHook>>,
    ) -> Self {
        let manager = Arc::new(ConnectionManager::new());
        let connections = Arc::new(Connections::default());
        let mut all: Vec<Arc<dyn QueryHook>> = vec![Arc::new(KillCallHook)];
        all.extend(hooks);
        Self {
            service: Arc::new(Governed {
                inner: Arc::new(DfSessionService::new_with_hooks(session, all)),
                engine,
                connections: Arc::clone(&connections),
            }),
            startup: Arc::new(Startup {
                manager: Arc::clone(&manager),
            }),
            cancel: Arc::new(CancelRequests {
                connections,
                inner: DefaultCancelHandler::new(manager),
            }),
        }
    }
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.service)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.service)
    }

    fn startup_handler(&self) -> Arc<impl datafusion_postgres::pgwire::api::auth::StartupHandler> {
        Arc::clone(&self.startup)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LogErrors)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel)
    }
}

/// No authentication (ROADMAP M1 item 5); registers connections so cancel
/// requests reach them.
struct Startup {
    manager: Arc<ConnectionManager>,
}

#[async_trait]
impl NoopStartupHandler for Startup {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.manager))
    }
}

struct LogErrors;

impl ErrorHandler for LogErrors {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        tracing::debug!(error = %error, "sending error to PostgreSQL client");
    }
}

/// A connection as cancel requests name it: its process id and secret key.
type ConnectionKey = (i32, Vec<u8>);

fn connection_key(client: &(impl ClientInfo + ?Sized)) -> ConnectionKey {
    let (pid, secret) = client.pid_and_secret_key();
    (pid, secret.to_bytes().to_vec())
}

/// The governed statements alive on each connection, so a cancel request
/// (which names a connection, not a statement) reaches them, and the
/// connections executing a message, the only ones a cancel request stops.
#[derive(Default)]
struct Connections {
    live: Mutex<HashMap<ConnectionKey, Vec<Weak<QueryRecord>>>>,
    executing: Mutex<HashMap<ConnectionKey, usize>>,
}

/// A connection executing a message, until dropped.
struct Executing {
    key: ConnectionKey,
    connections: Arc<Connections>,
}

impl Drop for Executing {
    fn drop(&mut self) {
        let mut executing = lock(&self.connections.executing);
        if let Some(count) = executing.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                executing.remove(&self.key);
            }
        }
    }
}

impl Connections {
    fn execute(self: &Arc<Self>, key: ConnectionKey) -> Executing {
        *lock(&self.executing).entry(key.clone()).or_default() += 1;
        Executing {
            key,
            connections: Arc::clone(self),
        }
    }

    fn executing(&self, key: &ConnectionKey) -> bool {
        lock(&self.executing).contains_key(key)
    }

    fn add(&self, key: &ConnectionKey, record: &Arc<QueryRecord>) {
        lock(&self.live)
            .entry(key.clone())
            .or_default()
            .push(Arc::downgrade(record));
    }

    fn remove(&self, key: &ConnectionKey, record: &QueryRecord) {
        let mut live = lock(&self.live);
        if let Some(records) = live.get_mut(key) {
            records.retain(|r| r.strong_count() > 0 && !std::ptr::eq(r.as_ptr(), record));
            if records.is_empty() {
                live.remove(key);
            }
        }
    }

    fn records(&self, key: &ConnectionKey) -> Vec<Arc<QueryRecord>> {
        lock(&self.live)
            .get(key)
            .map(|records| records.iter().filter_map(Weak::upgrade).collect())
            .unwrap_or_default()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        lock(&self.live).len()
    }
}

/// Cancel requests: when the connection executes a message, fail its
/// governed statements (their watchers then drop their streams), then let
/// `datafusion-postgres` stop what it is planning.
struct CancelRequests {
    connections: Arc<Connections>,
    inner: DefaultCancelHandler,
}

impl CancelRequests {
    fn cancel(&self, key: &ConnectionKey) -> usize {
        if !self.connections.executing(key) {
            return 0;
        }
        let records = self.connections.records(key);
        for record in &records {
            record.fail(Failure::new(USER_CANCELED, "USER_ERROR", CANCELED));
        }
        records.len()
    }
}

#[async_trait]
impl CancelHandler for CancelRequests {
    async fn on_cancel_request(&self, request: CancelRequest) {
        let key = (request.pid, request.secret_key.to_bytes().to_vec());
        if !self.connections.executing(&key) {
            tracing::debug!(pid = request.pid, "PostgreSQL cancel request while idle: ignored");
            return;
        }
        let canceled = self.cancel(&key);
        tracing::debug!(pid = request.pid, canceled, "PostgreSQL cancel request");
        self.inner.on_cancel_request(request).await;
    }
}

/// Plans `CALL system.runtime.kill_query(…)` on the extended protocol as an
/// empty statement, which `DataFusion` could not plan; [`Governed`] runs it.
#[derive(Debug)]
pub(crate) struct KillCallHook;

#[async_trait]
impl QueryHook for KillCallHook {
    async fn handle_simple_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Bad arguments too: the call answers them when it runs.
        kill_request(statement).map(|_| {
            Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            }))
        })
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// The client's error for a failed `kill_query` call.
fn kill_error(error: &KillError) -> PgWireError {
    let code = match error {
        KillError::InvalidArguments(_) | KillError::NullQueryId => "22023",
        KillError::NotText(_) => "42804",
        KillError::NotFound(_) => "42704",
        KillError::NotRunning(_) => "55000",
        KillError::NotOwner(_) => "42501",
    };
    user_error(code, error.to_string())
}

struct Governed {
    inner: Arc<DfSessionService>,
    engine: Arc<Engine>,
    connections: Arc<Connections>,
}

/// Statements the session answers without executing a query. `FETCH` and
/// `CLOSE` read or drop a cursor, governed as its `DECLARE`.
fn answered_by_the_session(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Set(_)
            | Statement::Fetch { .. }
            | Statement::Close { .. }
            | Statement::ShowVariable { .. }
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Savepoint { .. }
            | Statement::ReleaseSavepoint { .. }
            | Statement::Discard { .. }
            | Statement::Deallocate { .. }
    )
}

/// Whether a simple-query text needs governance: anything that does not
/// parse, or holds a statement the session does not answer itself.
fn governed_text(sql: &str) -> bool {
    Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_or(true, |statements| !statements.iter().all(answered_by_the_session))
}

fn metadata<'a>(client: &'a (impl ClientInfo + ?Sized), key: &str) -> Option<&'a str> {
    client.metadata().get(key).map(String::as_str)
}

fn statement_timeout(client: &(impl ClientInfo + ?Sized)) -> Option<Duration> {
    metadata(client, STATEMENT_TIMEOUT)
        .and_then(|ms| ms.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
}

fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.into(),
    )))
}

/// The client's error for a governance failure: `53400`
/// (`configuration_limit_exceeded`) for a full queue, `57014`
/// (`query_canceled`) for time limits and cancellation.
fn pg_error(failure: &Failure) -> PgWireError {
    let code = match failure.name.as_str() {
        QUERY_QUEUE_FULL => "53400",
        EXCEEDED_TIME_LIMIT | USER_CANCELED | ADMINISTRATIVELY_KILLED => "57014",
        _ => "XX000",
    };
    user_error(code, failure.message.clone())
}

/// How the registry records an error of the statement.
fn failure_of(error: &PgWireError, sql: &str) -> Failure {
    match error {
        PgWireError::ApiError(inner) => inner.downcast_ref::<DataFusionError>().map_or_else(
            || Failure::new("GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR", inner.to_string()),
            |e| QueryError::from_engine(e, sql, &Everything).failure(),
        ),
        PgWireError::UserError(info) => failure_of_info(info),
        PgWireError::QueryCanceled => Failure::new(USER_CANCELED, "USER_ERROR", error.to_string()),
        other => Failure::new("GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR", other.to_string()),
    }
}

/// How the registry records an error the client is sent as is.
fn failure_of_info(info: &ErrorInfo) -> Failure {
    if info.code == "57014" {
        Failure::new(
            EXCEEDED_TIME_LIMIT,
            "INSUFFICIENT_RESOURCES",
            info.message.clone(),
        )
    } else {
        Failure::new("GENERIC_USER_ERROR", "USER_ERROR", info.message.clone())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One governed statement while it runs: its record and its admission slot.
/// Dropped when its result streams are done (or were never made): the record
/// finishes, unless it failed or the statement was dropped before it ran (a
/// cancel request while it waited), which fails it as canceled.
struct Execution {
    record: Arc<QueryRecord>,
    permit: Mutex<Option<Permit>>,
    ran: AtomicBool,
    done: Arc<Notify>,
    /// Where cancel requests find the statement.
    connection: ConnectionKey,
    connections: Arc<Connections>,
}

impl Drop for Execution {
    fn drop(&mut self) {
        if self.ran.load(Ordering::SeqCst) {
            self.record.finish();
        } else {
            self.record
                .fail(Failure::new(USER_CANCELED, "USER_ERROR", CANCELED));
        }
        self.connections.remove(&self.connection, &self.record);
        lock(&self.permit).take();
        self.done.notify_one();
    }
}

/// Where a statement's result stream lives while it runs, with the waker of
/// the task reading it: the deadline watcher empties it and wakes that task.
#[derive(Default)]
struct SlotState {
    rows: Option<SendableRowStream>,
    waker: Option<Waker>,
}

type Slot = Arc<Mutex<SlotState>>;

/// Empties `slot` without blocking the runtime: a poll in progress holds the
/// lock only until the stream yields. Dropping the stream stops the operators
/// and frees their memory; the reader, if waiting, is woken to see the error.
async fn empty(slot: &Slot) {
    loop {
        if let Ok(mut state) = slot.try_lock() {
            let rows = state.rows.take();
            let waker = state.waker.take();
            drop(state);
            drop(rows);
            if let Some(waker) = waker {
                waker.wake();
            }
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The rows of a governed statement.
struct GovernedRows {
    slot: Slot,
    execution: Option<Arc<Execution>>,
    rows: u64,
}

impl GovernedRows {
    fn end(&mut self) {
        let rows = std::mem::take(&mut lock(&self.slot).rows);
        drop(rows);
        if let Some(execution) = self.execution.take() {
            execution.record.add_rows(self.rows);
            self.rows = 0;
        }
    }
}

impl Drop for GovernedRows {
    fn drop(&mut self) {
        self.end();
    }
}

impl Stream for GovernedRows {
    type Item = PgWireResult<DataRow>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(execution) = this.execution.clone() else {
            return Poll::Ready(None);
        };
        // A failed query (time limit, cancel) stops here, even when its
        // stream still has rows ready.
        let failed = execution.record.state() == QueryState::Failed;
        let polled = {
            let mut state = lock(&this.slot);
            match (failed, state.rows.as_mut()) {
                (false, Some(rows)) => {
                    let polled = rows.as_mut().poll_next(cx);
                    if polled.is_pending() && !state.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                        state.waker = Some(cx.waker().clone());
                    }
                    Some(polled)
                }
                _ => None,
            }
        };
        match polled {
            None => {
                let failure = execution
                    .record
                    .failure()
                    .unwrap_or_else(|| Failure::new(USER_CANCELED, "USER_ERROR", CANCELED));
                this.end();
                Poll::Ready(Some(Err(pg_error(&failure))))
            }
            Some(Poll::Pending) => Poll::Pending,
            Some(Poll::Ready(Some(Ok(row)))) => {
                this.rows += 1;
                Poll::Ready(Some(Ok(row)))
            }
            Some(Poll::Ready(Some(Err(error)))) => {
                let sql = execution.record.snapshot().query;
                execution.record.fail(failure_of(&error, &sql));
                this.end();
                Poll::Ready(Some(Err(error)))
            }
            Some(Poll::Ready(None)) => {
                this.end();
                Poll::Ready(None)
            }
        }
    }
}

/// Moves `query`'s rows into a slot the deadline watcher can empty, and
/// hands `query` governed rows that read them.
fn govern(execution: &Arc<Execution>, query: &mut QueryResponse) -> Slot {
    let rows = std::mem::replace(&mut query.data_rows, Box::pin(futures::stream::empty()));
    let slot: Slot = Arc::new(Mutex::new(SlotState {
        rows: Some(rows),
        waker: None,
    }));
    query.data_rows = Box::pin(GovernedRows {
        slot: Arc::clone(&slot),
        execution: Some(Arc::clone(execution)),
        rows: 0,
    });
    slot
}

/// A statement admitted to run.
struct Admitted {
    execution: Arc<Execution>,
    deadline: tokio::time::Instant,
    limit: Duration,
}

impl Governed {
    /// Registers the statement and waits for its admission slot.
    async fn admit(&self, client: &mut (impl ClientInfo + ?Sized), sql: &str) -> PgWireResult<Admitted> {
        let governance = self.engine.governance();
        let user = metadata(client, METADATA_USER).unwrap_or(UNKNOWN_USER);
        let source = metadata(client, METADATA_APPLICATION_NAME).filter(|s| !s.is_empty());
        let record = governance.register(Protocol::Postgres, user, source, sql);
        let connection = connection_key(client);
        self.connections.add(&connection, &record);
        let execution = Arc::new(Execution {
            record: Arc::clone(&record),
            permit: Mutex::new(None),
            ran: AtomicBool::new(false),
            done: Arc::new(Notify::new()),
            connection,
            connections: Arc::clone(&self.connections),
        });
        // The statement's span (tracing hook) carries the registry's id.
        client
            .metadata_mut()
            .insert(QUERY_ID.to_owned(), record.id().to_owned());
        let limits = governance.limits(statement_timeout(client), None);
        let ticket = governance.enter(&record).map_err(|e| pg_error(&e.failure()))?;
        let permit = governance
            .admit(ticket, &record, &limits)
            .await
            .map_err(|e| pg_error(&record.failure().unwrap_or_else(|| e.failure())))?;
        *lock(&execution.permit) = Some(permit);
        record.set_state(QueryState::Planning);
        let (deadline, limit) = limits.deadline(&record.snapshot());
        Ok(Admitted {
            execution,
            deadline: tokio::time::Instant::from_std(deadline),
            limit,
        })
    }

    /// The statement's deadline passed while it was planned or executed.
    fn timed_out(&self, admitted: &Admitted) -> PgWireError {
        let record = &admitted.execution.record;
        let error: GovernanceError = self
            .engine
            .governance()
            .time_limit_exceeded(record, admitted.limit);
        record.fail(error.failure());
        pg_error(&error.failure())
    }

    fn failed(admitted: &Admitted, error: PgWireError, sql: &str) -> PgWireError {
        admitted.execution.record.fail(failure_of(&error, sql));
        error
    }

    /// Runs `work` (planning) until the statement's deadline, or until it is
    /// canceled or killed.
    async fn bounded<T>(
        &self,
        admitted: &Admitted,
        sql: &str,
        work: impl Future<Output = PgWireResult<T>>,
    ) -> PgWireResult<T> {
        let record = &admitted.execution.record;
        let stopped = record.cancellation();
        tokio::select! {
            biased;
            () = stopped.cancelled() => {
                let failure = record.failure().unwrap_or_else(|| GovernanceError::Canceled.failure());
                Err(pg_error(&failure))
            }
            () = tokio::time::sleep_until(admitted.deadline) => Err(self.timed_out(admitted)),
            outcome = work => outcome.map_err(|error| Self::failed(admitted, error, sql)),
        }
    }

    /// `CALL system.runtime.kill_query(…)`, as the connection's user.
    fn kill(
        &self,
        client: &(impl ClientInfo + ?Sized),
        request: Result<KillRequest, KillError>,
    ) -> PgWireResult<Response> {
        let user = metadata(client, METADATA_USER).unwrap_or(UNKNOWN_USER);
        request
            .and_then(|request| self.engine.governance().kill(&request, user))
            .map(|()| Response::Execution(Tag::new("CALL")))
            .map_err(|error| kill_error(&error))
    }

    /// Wraps the result streams, the statement's and those of the cursors it
    /// declared, and watches the deadline until they end.
    fn run(
        &self,
        admitted: Admitted,
        responses: Vec<Response>,
        cursors: &mut [OwnedMutexGuard<PortalExecutionState>],
    ) -> Vec<Response> {
        let Admitted {
            execution,
            deadline,
            limit,
        } = admitted;
        execution.ran.store(true, Ordering::SeqCst);
        execution.record.set_state(QueryState::Running);
        let mut slots = Vec::new();
        for cursor in cursors.iter_mut() {
            if let PortalExecutionState::Suspended(query) = &mut **cursor {
                slots.push(govern(&execution, query));
            }
        }
        let responses = responses
            .into_iter()
            .map(|response| match response {
                Response::Query(mut query) => {
                    slots.push(govern(&execution, &mut query));
                    Response::Query(query)
                }
                Response::Error(info) => {
                    execution.record.fail(failure_of_info(&info));
                    Response::Error(info)
                }
                other => other,
            })
            .collect();
        if !slots.is_empty() {
            self.watch(&execution, slots, deadline, limit);
        }
        responses
    }

    /// Past the deadline, or once the query failed, empties the result slots
    /// (dropping the streams stops the operators and frees their memory) and
    /// frees the admission slot; ends when the statement is done.
    fn watch(
        &self,
        execution: &Arc<Execution>,
        slots: Vec<Slot>,
        deadline: tokio::time::Instant,
        limit: Duration,
    ) {
        let weak: Weak<Execution> = Arc::downgrade(execution);
        let record = Arc::clone(&execution.record);
        let done = Arc::clone(&execution.done);
        let engine = Arc::clone(&self.engine);
        tokio::spawn(async move {
            let cancel = record.cancellation();
            tokio::select! {
                () = done.notified() => return,
                () = cancel.cancelled() => {}
                () = tokio::time::sleep_until(deadline) => {
                    let error = engine.governance().time_limit_exceeded(&record, limit);
                    record.fail(error.failure());
                }
            }
            for slot in &slots {
                empty(slot).await;
            }
            if let Some(execution) = weak.upgrade() {
                let permit = lock(&execution.permit).take();
                drop(permit);
            }
        });
    }
}

#[async_trait]
impl SimpleQueryHandler for Governed {
    /// `pgwire`'s, with the connection marked as executing while it runs.
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _executing = self.connections.execute(connection_key(client));
        self._on_query(client, query).await
    }

    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(request) = kill_request_sql(query) {
            return Ok(vec![self.kill(client, request)?]);
        }
        client.metadata_mut().remove(QUERY_ID);
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, query).unwrap_or_default();
        if let [statement] = statements.as_slice()
            && let Some(response) = cursor::fetch(&*client, statement, &Format::UnifiedText).await
        {
            return Ok(vec![response?]);
        }
        if !governed_text(query) {
            return SimpleQueryHandler::do_query(&*self.inner, client, query).await;
        }
        let declared = cursor::Declared::before(&*client, cursor::declared_names(&statements));
        let admitted = self.admit(client, query).await?;
        let responses = self
            .bounded(
                &admitted,
                query,
                SimpleQueryHandler::do_query(&*self.inner, client, query),
            )
            .await;
        let responses = declared.settle(client, responses)?;
        let mut cursors = declared.states(client).await;
        Ok(self.run(admitted, responses, &mut cursors))
    }
}

#[async_trait]
impl ExtendedQueryHandler for Governed {
    type Statement = DfStatement;
    type QueryParser = <DfSessionService as ExtendedQueryHandler>::QueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        ExtendedQueryHandler::query_parser(&*self.inner)
    }

    /// Streams the rows to the client instead of collecting them first.
    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _executing = self.connections.execute(connection_key(client));
        execute::execute(self, client, message).await
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some((statement, _)) = &target.statement.1
            && let Some(fields) = cursor::fetch_fields(&*client, statement, &Format::UnifiedText).await
        {
            return Ok(DescribeStatementResponse::new(Vec::new(), fields?));
        }
        ExtendedQueryHandler::do_describe_statement(&*self.inner, client, target).await
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some((statement, _)) = &target.statement.statement.1
            && let Some(fields) =
                cursor::fetch_fields(&*client, statement, &target.result_column_format).await
        {
            return Ok(DescribePortalResponse::new(fields?));
        }
        ExtendedQueryHandler::do_describe_portal(&*self.inner, client, target).await
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (sql, parsed) = &portal.statement.statement;
        if let Some(request) = parsed.as_ref().and_then(|(statement, _)| kill_request(statement)) {
            return self.kill(client, request);
        }
        if let Some((statement, _)) = parsed
            && let Some(response) = cursor::fetch(&*client, statement, &portal.result_column_format).await
        {
            return response;
        }
        client.metadata_mut().remove(QUERY_ID);
        let governed = parsed
            .as_ref()
            .is_some_and(|(statement, _)| !answered_by_the_session(statement));
        if !governed {
            return ExtendedQueryHandler::do_query(&*self.inner, client, portal, max_rows).await;
        }
        let declared = cursor::Declared::before(
            &*client,
            cursor::declared_names(parsed.as_ref().map(|(statement, _)| statement)),
        );
        let admitted = self.admit(client, sql).await?;
        let response = self
            .bounded(
                &admitted,
                sql,
                ExtendedQueryHandler::do_query(&*self.inner, client, portal, max_rows),
            )
            .await;
        let response = declared.settle(client, response)?;
        let mut cursors = declared.states(client).await;
        Ok(self
            .run(admitted, vec![response], &mut cursors)
            .pop()
            .unwrap_or(Response::EmptyQuery))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_statements_are_not_governed() {
        for sql in [
            "SET extra_float_digits = 3",
            "SET statement_timeout = '5s'",
            "SHOW transaction_isolation",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "BEGIN; SET search_path = public; COMMIT",
            "DISCARD ALL",
        ] {
            assert!(!governed_text(sql), "{sql}");
        }
        for sql in [
            "SELECT 1",
            "SET a = 1; SELECT 1",
            "INSERT INTO t VALUES (1)",
            "SHOW TABLES",
            "SELEC 1",
            "CREATE TABLE t (a INT)",
        ] {
            assert!(governed_text(sql), "{sql}");
        }
    }

    #[test]
    fn governance_failures_map_to_postgres_states() {
        let code = |failure: Failure| match pg_error(&failure) {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            code(Failure::new(QUERY_QUEUE_FULL, "INSUFFICIENT_RESOURCES", "full")),
            ("53400".to_owned(), "full".to_owned())
        );
        assert_eq!(
            code(GovernanceError::TimeLimit(Duration::from_secs(2)).failure()).0,
            "57014"
        );
        assert_eq!(code(GovernanceError::Canceled.failure()).0, "57014");
    }

    #[test]
    fn kill_errors_map_to_postgres_states() {
        let code = |error: KillError| match kill_error(&error) {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            code(KillError::NotFound("q".into())),
            ("42704".to_owned(), "Target query not found: q".to_owned())
        );
        assert_eq!(code(KillError::NotOwner("ana".into())).0, "42501");
        assert_eq!(code(KillError::NotRunning("q".into())).0, "55000");
        assert_eq!(code(KillError::InvalidArguments("x".into())).0, "22023");
        assert_eq!(code(KillError::NullQueryId).0, "22023");
        assert_eq!(code(KillError::NotText("x".into())).0, "42804");
        let killed = Failure::new(
            ADMINISTRATIVELY_KILLED,
            "USER_ERROR",
            "Query killed. No message provided.",
        );
        assert!(matches!(pg_error(&killed), PgWireError::UserError(info) if info.code == "57014"));
    }

    #[test]
    fn cancel_requests_reach_only_their_connection() {
        let connections = Arc::new(Connections::default());
        let requests = CancelRequests {
            connections: Arc::clone(&connections),
            inner: DefaultCancelHandler::new(Arc::new(ConnectionManager::new())),
        };
        let mine = QueryRecord::detached("q1", Protocol::Postgres, "u", "SELECT 1");
        let other = QueryRecord::detached("q2", Protocol::Postgres, "u", "SELECT 2");
        let key = (7, vec![1, 2, 3, 4]);
        let other_key = (7, vec![9, 9, 9, 9]);
        connections.add(&key, &mine);
        connections.add(&other_key, &other);
        // Between messages a cancel request does nothing.
        assert_eq!(requests.cancel(&key), 0);
        assert!(mine.failure().is_none());
        let executing = connections.execute(key.clone());
        let _other_executing = connections.execute(other_key.clone());
        // Same process id, another secret key: nothing is canceled.
        assert_eq!(requests.cancel(&(7, vec![0, 0, 0, 0])), 0);
        assert_eq!(requests.cancel(&key), 1);
        drop(executing);
        assert!(!connections.executing(&key), "the message is done");
        assert!(connections.executing(&other_key));
        assert_eq!(mine.failure().map(|f| f.message), Some(CANCELED.to_owned()));
        assert!(mine.cancellation().is_cancelled());
        assert!(other.failure().is_none());
        connections.remove(&key, &mine);
        connections.remove(&other_key, &other);
        assert_eq!(connections.len(), 0, "entries go away with their statements");
    }

    #[test]
    fn statement_errors_are_recorded_with_names() {
        let error = PgWireError::ApiError(Box::new(DataFusionError::Plan("table 'x' not found".into())));
        let failure = failure_of(&error, "SELECT * FROM x");
        assert_eq!(failure.kind, "USER_ERROR");
        let timeout = user_error("57014", "canceling statement due to statement timeout");
        assert_eq!(failure_of(&timeout, "").name, EXCEEDED_TIME_LIMIT);
        assert_eq!(
            failure_of(&user_error("42601", "bad"), "").name,
            "GENERIC_USER_ERROR"
        );
    }
}
