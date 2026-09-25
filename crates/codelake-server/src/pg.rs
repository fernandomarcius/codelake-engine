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
//! dropped). Statements the session answers itself (`SET`, `SHOW`,
//! transaction control) neither register nor take a slot.

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_trait::async_trait;
use codelake_core::Engine;
use codelake_core::governance::{
    EXCEEDED_TIME_LIMIT, Failure, GovernanceError, Permit, Protocol, QUERY_QUEUE_FULL, QueryRecord,
    QueryState, USER_CANCELED,
};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::cancel::DefaultCancelHandler;
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, Response, SendableRowStream,
};
use datafusion_postgres::pgwire::api::stmt::StoredStatement;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, ErrorHandler, METADATA_APPLICATION_NAME, METADATA_USER,
    PgWireServerHandlers,
};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::{DfSessionService, QueryHook};
use futures::{Sink, Stream};
use tokio::sync::Notify;

use crate::trino::{Everything, QueryError};

/// Client metadata key where `datafusion-postgres` keeps `SET
/// statement_timeout` (milliseconds).
const STATEMENT_TIMEOUT: &str = "statement_timeout_ms";
/// User of a connection whose startup message named none.
const UNKNOWN_USER: &str = "unknown";

type DfStatement = <DfSessionService as ExtendedQueryHandler>::Statement;

/// The server's PostgreSQL handlers: `datafusion-postgres`'s, governed.
pub(crate) struct Handlers {
    service: Arc<Governed>,
    startup: Arc<Startup>,
    cancel: Arc<DefaultCancelHandler>,
}

impl Debug for Handlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Handlers")
    }
}

impl Handlers {
    pub(crate) fn new(
        engine: Arc<Engine>,
        session: Arc<SessionContext>,
        hooks: Vec<Arc<dyn QueryHook>>,
    ) -> Self {
        let manager = Arc::new(ConnectionManager::new());
        Self {
            service: Arc::new(Governed {
                inner: Arc::new(DfSessionService::new_with_hooks(session, hooks)),
                engine,
            }),
            startup: Arc::new(Startup {
                manager: Arc::clone(&manager),
            }),
            cancel: Arc::new(DefaultCancelHandler::new(manager)),
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

    fn cancel_handler(&self) -> Arc<impl datafusion_postgres::pgwire::api::cancel::CancelHandler> {
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

struct Governed {
    inner: Arc<DfSessionService>,
    engine: Arc<Engine>,
}

/// Statements the session answers without executing a query.
fn answered_by_the_session(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Set(_)
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
        EXCEEDED_TIME_LIMIT | USER_CANCELED => "57014",
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
}

impl Drop for Execution {
    fn drop(&mut self) {
        if self.ran.load(Ordering::SeqCst) {
            self.record.finish();
        } else {
            self.record.fail(Failure::new(
                USER_CANCELED,
                "USER_ERROR",
                "canceling statement due to user request",
            ));
        }
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
                let failure = execution.record.failure().unwrap_or_else(|| {
                    Failure::new(
                        USER_CANCELED,
                        "USER_ERROR",
                        "canceling statement due to user request",
                    )
                });
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

/// A statement admitted to run.
struct Admitted {
    execution: Arc<Execution>,
    deadline: tokio::time::Instant,
    limit: Duration,
}

impl Governed {
    /// Registers the statement and waits for its admission slot.
    async fn admit(&self, client: &(impl ClientInfo + ?Sized), sql: &str) -> PgWireResult<Admitted> {
        let governance = self.engine.governance();
        let user = metadata(client, METADATA_USER).unwrap_or(UNKNOWN_USER);
        let source = metadata(client, METADATA_APPLICATION_NAME).filter(|s| !s.is_empty());
        let record = governance.register(Protocol::Postgres, user, source, sql);
        let execution = Arc::new(Execution {
            record: Arc::clone(&record),
            permit: Mutex::new(None),
            ran: AtomicBool::new(false),
            done: Arc::new(Notify::new()),
        });
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

    /// Wraps the result streams and watches the deadline until they end.
    fn run(&self, admitted: Admitted, responses: Vec<Response>) -> Vec<Response> {
        let Admitted {
            execution,
            deadline,
            limit,
        } = admitted;
        execution.ran.store(true, Ordering::SeqCst);
        execution.record.set_state(QueryState::Running);
        let mut slots = Vec::new();
        let responses = responses
            .into_iter()
            .map(|response| match response {
                Response::Query(mut query) => {
                    let rows = std::mem::replace(&mut query.data_rows, Box::pin(futures::stream::empty()));
                    let slot: Slot = Arc::new(Mutex::new(SlotState {
                        rows: Some(rows),
                        waker: None,
                    }));
                    slots.push(Arc::clone(&slot));
                    query.data_rows = Box::pin(GovernedRows {
                        slot,
                        execution: Some(Arc::clone(&execution)),
                        rows: 0,
                    });
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
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !governed_text(query) {
            return SimpleQueryHandler::do_query(&*self.inner, client, query).await;
        }
        let admitted = self.admit(client, query).await?;
        let outcome = tokio::time::timeout_at(
            admitted.deadline,
            SimpleQueryHandler::do_query(&*self.inner, client, query),
        )
        .await;
        match outcome {
            Err(_) => Err(self.timed_out(&admitted)),
            Ok(Err(error)) => Err(Self::failed(&admitted, error, query)),
            Ok(Ok(responses)) => Ok(self.run(admitted, responses)),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for Governed {
    type Statement = DfStatement;
    type QueryParser = <DfSessionService as ExtendedQueryHandler>::QueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        ExtendedQueryHandler::query_parser(&*self.inner)
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
        let governed = parsed
            .as_ref()
            .is_some_and(|(statement, _)| !answered_by_the_session(statement));
        if !governed {
            return ExtendedQueryHandler::do_query(&*self.inner, client, portal, max_rows).await;
        }
        let admitted = self.admit(client, sql).await?;
        let outcome = tokio::time::timeout_at(
            admitted.deadline,
            ExtendedQueryHandler::do_query(&*self.inner, client, portal, max_rows),
        )
        .await;
        match outcome {
            Err(_) => Err(self.timed_out(&admitted)),
            Ok(Err(error)) => Err(Self::failed(&admitted, error, sql)),
            Ok(Ok(response)) => Ok(self
                .run(admitted, vec![response])
                .pop()
                .unwrap_or(Response::EmptyQuery)),
        }
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
