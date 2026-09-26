//! PostgreSQL wire protocol hooks. The metrics and tracing hooks only observe
//! statements; they never answer them. The policy hook refuses statements,
//! and the last hook runs the simple-protocol statements no other hook
//! answered as `datafusion-postgres` would, planning each once
//! ([`SimpleRunHook`]).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use codelake_core::trace::{self, TARGET, TraceParent};
use codelake_core::{operand_checks, statement_policy};
use datafusion::common::ParamValues;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SQLOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_postgres::arrow_pg::datatypes::df::encode_dataframe;
use datafusion_postgres::hooks::cursor::CursorStatementHook;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::hooks::transactions::TransactionStatementHook;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_USER};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;

pub(crate) const STATEMENTS_TOTAL: &str = "codelake_statements_total";

/// Counts statements per protocol path.
#[derive(Debug)]
pub(crate) struct MetricsHook;

#[async_trait]
impl QueryHook for MetricsHook {
    async fn handle_simple_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        metrics::counter!(STATEMENTS_TOTAL, "protocol" => "simple").increment(1);
        None
    }

    async fn handle_extended_parse_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        metrics::counter!(STATEMENTS_TOTAL, "protocol" => "extended").increment(1);
        None
    }
}

/// Refuses, before any other hook or `DataFusion` sees them, the statements
/// no client may run (`COPY`, `CREATE EXTERNAL TABLE`, runtime settings,
/// `""`): the engine's statement policy
/// ([`codelake_core::statement_policy`]), applied to the PostgreSQL wire
/// protocol, which runs statements on the session directly. On the extended
/// protocol it also refuses the operand types the engine does not combine
/// (text against a number, `time ± interval`; ADR 0014), with PostgreSQL's
/// typing rules and errors ([`codelake_core::operand_checks::postgres_refusal`]);
/// on the simple protocol [`SimpleRunHook`] refuses them on the plan it runs.
#[derive(Debug)]
pub(crate) struct PolicyHook;

fn refusal(err: &datafusion::error::DataFusionError) -> PgWireError {
    let (code, message) = match err {
        datafusion::error::DataFusionError::SQL(..) => ("42601", err.to_string()),
        datafusion::error::DataFusionError::NotImplemented(message) => ("0A000", message.clone()),
        other => ("42000", other.to_string()),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

fn check(statement: &Statement) -> PgWireResult<()> {
    statement_policy::check_sql(&statement.to_string(), &PostgreSqlDialect {}).map_err(|err| refusal(&err))
}

/// Refuses a planned statement whose operands the engine does not combine,
/// as PostgreSQL words it (SQLSTATE `42883` "operator does not exist: text >
/// integer", or `0A000`).
fn check_operands(plan: &LogicalPlan) -> PgWireResult<()> {
    match operand_checks::postgres_refusal(plan) {
        Ok(None) => Ok(()),
        Ok(Some(found)) => {
            let mut info = ErrorInfo::new("ERROR".to_owned(), found.code.to_owned(), found.message);
            info.hint = found.hint.map(str::to_owned);
            Err(PgWireError::UserError(Box::new(info)))
        }
        Err(err) => Err(refusal(&err)),
    }
}

#[async_trait]
impl QueryHook for PolicyHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        check(statement).err().map(Err)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        check(statement).err().map(Err)
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if let Err(err) = check(statement) {
            return Some(Err(err));
        }
        if let Err(err) = statement_policy::check_plan(logical_plan) {
            return Some(Err(refusal(&err)));
        }
        check_operands(logical_plan).err().map(Err)
    }
}

/// Runs the simple-protocol statements no earlier hook answered, as
/// `datafusion-postgres` does after its hooks (`SimpleQueryHandler::do_query`
/// in 0.18: the statement's text planned by `SessionContext::sql`, under the
/// session's `statement_timeout`, an `INSERT` answered with its row count),
/// except that the plan is refused when its operands are ones the engine does
/// not combine ([`check_operands`]) before it runs. The operands are checked
/// on the very plan that runs, and the statement is planned once. It must be
/// the last hook: it answers every statement that reaches it.
///
/// A statement that does not plan gets the planner's error, as it would from
/// `datafusion-postgres`.
#[derive(Debug)]
pub(crate) struct SimpleRunHook;

/// Client metadata key where `datafusion-postgres` keeps the session's
/// `SET statement_timeout`, in milliseconds (its accessor is crate-private;
/// a test pins the key against `SET`).
const STATEMENT_TIMEOUT: &str = "statement_timeout_ms";

fn statement_timeout<C: ClientInfo + ?Sized>(client: &C) -> Option<Duration> {
    client
        .metadata()
        .get(STATEMENT_TIMEOUT)
        .and_then(|millis| millis.parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn api_error(err: datafusion::error::DataFusionError) -> PgWireError {
    PgWireError::ApiError(Box::new(err))
}

/// What `SessionContext::sql` does, with the operand check between planning
/// and running.
async fn plan_checked(session_context: &SessionContext, sql: &str) -> PgWireResult<DataFrame> {
    let plan = session_context
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(api_error)?;
    SQLOptions::new().verify_plan(&plan).map_err(api_error)?;
    check_operands(&plan)?;
    session_context
        .execute_logical_plan(plan)
        .await
        .map_err(api_error)
}

async fn run_simple(
    statement: &Statement,
    session_context: &SessionContext,
    client: &mut dyn HookClient,
) -> PgWireResult<Response> {
    let sql = statement.to_string();
    let planned = plan_checked(session_context, &sql);
    let df = match statement_timeout(client) {
        Some(limit) => tokio::time::timeout(limit, planned).await.map_err(|_| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "57014".to_owned(),
                "canceling statement due to statement timeout".to_owned(),
            )))
        })??,
        None => planned.await?,
    };
    if matches!(statement, Statement::Insert(_)) {
        return rows_inserted(df).await;
    }
    let options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    let response = encode_dataframe(df, &Format::UnifiedText, Some(options)).await?;
    Ok(Response::Query(response))
}

/// Runs an `INSERT` and answers `INSERT 0 <rows>`, the count `DataFusion`
/// reports in the `count` column.
async fn rows_inserted(df: DataFrame) -> PgWireResult<Response> {
    let batches = df.collect().await.map_err(api_error)?;
    let rows = batches
        .first()
        .and_then(|batch| batch.column_by_name("count"))
        .and_then(|column| {
            column
                .as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        })
        .map_or(0, |counts| counts.value(0));
    let rows = usize::try_from(rows).unwrap_or(usize::MAX);
    Ok(Response::Execution(
        Tag::new("INSERT").with_oid(0).with_rows(rows),
    ))
}

#[async_trait]
impl QueryHook for SimpleRunHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        Some(run_simple(statement, session_context, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
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

/// Client metadata key holding the session's synthetic trace parent.
const SESSION_TRACEPARENT: &str = "codelake.session_traceparent";

/// Correlates PostgreSQL statements by session: each connection gets one
/// trace (a synthetic parent kept in the connection's metadata, sampled once
/// with the configured ratio) and every statement is a `codelake.pg.statement`
/// span in it, carrying the backend pid as session id, the user, the client
/// address and the redacted SQL.
///
/// The wire protocol has no header for a caller's `traceparent`, and the
/// statement runs after this hook returns, inside `datafusion-postgres`: the
/// span marks when the statement arrived; execution spans are not linked to
/// it until the listener runs statements through `Engine::query` (ADR 0010).
#[derive(Debug)]
pub(crate) struct TracingHook {
    ratio: f64,
}

impl TracingHook {
    fn statement(&self, statement: &Statement, client: &mut dyn HookClient, protocol: &'static str) {
        let parent = session_parent(client, self.ratio);
        let (pid, _) = client.pid_and_secret_key();
        let span = tracing::info_span!(
            target: TARGET,
            "codelake.pg.statement",
            otel.kind = "server",
            codelake.protocol = protocol,
            codelake.session_id = pid,
            // The id `system.runtime.queries` lists; absent for statements
            // the session answers itself.
            codelake.query_id = client.metadata().get(crate::pg::QUERY_ID).map(String::as_str),
            codelake.trace_id = tracing::field::Empty,
            enduser.id = client.metadata().get(METADATA_USER).map(String::as_str),
            client.address = %client.socket_addr().ip(),
            db.query.text = tracing::field::Empty,
            codelake.sql.hash = tracing::field::Empty,
        );
        if span.is_disabled() {
            return;
        }
        trace::set_remote_parent(&span, &parent, None);
        span.record(trace::field::TRACE_ID, parent.trace_id_hex().as_str());
        trace::record_sql(&span, &statement.to_string(), &PostgreSqlDialect {});
    }
}

/// The session's trace parent, created on its first statement.
fn session_parent(client: &mut dyn HookClient, ratio: f64) -> TraceParent {
    if let Some(parent) = client
        .metadata()
        .get(SESSION_TRACEPARENT)
        .and_then(|value| TraceParent::parse(value).ok())
    {
        return parent;
    }
    let parent = codelake_server::telemetry::session_parent(ratio);
    client
        .metadata_mut()
        .insert(SESSION_TRACEPARENT.to_owned(), parent.to_string());
    parent
}

#[async_trait]
impl QueryHook for TracingHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.statement(statement, client, "postgresql.simple");
        None
    }

    async fn handle_extended_parse_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.statement(statement, client, "postgresql.extended");
        None
    }
}

/// The PostgreSQL hooks, in order: the statement policy first, so no other
/// hook and no `DataFusion` path sees a refused statement; the default hooks
/// of `datafusion-postgres` (passing a custom list replaces them, so they are
/// re-added here), after the tracing observer so statements answered by a
/// default hook (`SET`, `BEGIN`, cursors) are traced too, with the server's
/// cursor hook before them ([`crate::pg::CursorHook`]); the metrics
/// counter; and last [`SimpleRunHook`], which runs every simple-protocol
/// statement that got that far.
///
/// `session_sampling` is `None` when traces are not exported: no tracing hook.
pub(crate) fn all(session_sampling: Option<f64>) -> Vec<Arc<dyn QueryHook>> {
    let mut hooks: Vec<Arc<dyn QueryHook>> = Vec::with_capacity(8);
    hooks.push(Arc::new(PolicyHook));
    if let Some(ratio) = session_sampling {
        hooks.push(Arc::new(TracingHook { ratio }));
    }
    // Before `datafusion-postgres`'s cursor and transaction hooks: it sees
    // the statements they answer (crate::pg::CursorHook).
    hooks.push(Arc::new(crate::pg::CursorHook));
    hooks.push(Arc::new(CursorStatementHook));
    hooks.push(Arc::new(SetShowHook));
    hooks.push(Arc::new(TransactionStatementHook));
    hooks.push(Arc::new(MetricsHook));
    hooks.push(Arc::new(SimpleRunHook));
    hooks
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::parser::Parser;

    fn statement(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap().remove(0)
    }

    #[test]
    fn the_wire_protocol_refuses_statements_that_reach_server_files() {
        for sql in [
            "COPY (SELECT 1) TO '/tmp/x.csv'",
            "SET datafusion.runtime.temp_directory = '/etc'",
            "SELECT 1 AS \"\"",
        ] {
            assert!(check(&statement(sql)).is_err(), "{sql}");
        }
        for sql in ["SELECT 1", "SET datafusion.execution.batch_size = 10", "BEGIN"] {
            assert!(check(&statement(sql)).is_ok(), "{sql}");
        }
    }

    /// A schema that counts how often the planner looks its tables up: once
    /// per planning of a statement that reads one table.
    #[derive(Debug, Default)]
    struct CountingSchema {
        inner: datafusion::catalog::MemorySchemaProvider,
        lookups: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl datafusion::catalog::SchemaProvider for CountingSchema {
        fn table_names(&self) -> Vec<String> {
            self.inner.table_names()
        }

        async fn table(
            &self,
            name: &str,
        ) -> datafusion::error::Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
            self.lookups.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.table(name).await
        }

        fn table_exist(&self, name: &str) -> bool {
            self.inner.table_exist(name)
        }
    }

    impl CountingSchema {
        fn lookups(&self) -> usize {
            self.lookups.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// The simple-protocol handler of `datafusion-postgres` with our hooks,
    /// over a session holding `counted.ops (s varchar, n int)`.
    fn simple_service() -> (datafusion_postgres::DfSessionService, Arc<CountingSchema>) {
        use datafusion::arrow::array::{Int32Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::catalog::SchemaProvider as _;

        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["10", "9", "a"])),
                Arc::new(Int32Array::from(vec![5, 100, 1])),
            ],
        )
        .unwrap();
        let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let counted = Arc::new(CountingSchema::default());
        counted
            .inner
            .register_table("ops".to_owned(), Arc::new(table))
            .unwrap();
        let ctx = SessionContext::new();
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema("counted", Arc::clone(&counted) as _)
            .unwrap();
        let service = datafusion_postgres::DfSessionService::new_with_hooks(Arc::new(ctx), all(None));
        (service, counted)
    }

    async fn simple_query(
        service: &datafusion_postgres::DfSessionService,
        client: &mut datafusion_postgres::testing::MockClient,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        use datafusion_postgres::pgwire::api::query::SimpleQueryHandler as _;
        service.do_query(client, sql).await
    }

    async fn row_count(response: Response) -> usize {
        use futures::StreamExt as _;
        let Response::Query(mut query) = response else {
            panic!("not a query response: {response:?}");
        };
        query.data_rows().count().await
    }

    #[tokio::test]
    async fn a_simple_statement_is_planned_once() {
        let (service, counted) = simple_service();
        let mut client = datafusion_postgres::testing::MockClient::new();
        let mut responses = simple_query(&service, &mut client, "SELECT n FROM counted.ops WHERE n > 1")
            .await
            .unwrap();
        assert_eq!(row_count(responses.remove(0)).await, 2);
        assert_eq!(counted.lookups(), 1, "planned more than once");
    }

    fn user_error(err: PgWireError) -> (String, String) {
        match err {
            PgWireError::UserError(info) => (info.code.clone(), info.message.clone()),
            other => panic!("not a user error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_simple_statement_keeps_the_operand_refusals() {
        let (service, counted) = simple_service();
        let mut client = datafusion_postgres::testing::MockClient::new();
        for (sql, code, message) in [
            (
                "SELECT n FROM counted.ops WHERE s > 9",
                "42883",
                "operator does not exist: text > integer",
            ),
            (
                "SELECT n FROM counted.ops WHERE s BETWEEN 1 AND 9",
                "42883",
                "operator does not exist: text >= integer",
            ),
            (
                "SELECT TIME '10:00:00' + INTERVAL '1 hour'",
                "0A000",
                "time without time zone + interval is not supported",
            ),
        ] {
            let err = simple_query(&service, &mut client, sql).await.unwrap_err();
            assert_eq!(user_error(err), (code.to_owned(), message.to_owned()), "{sql}");
        }
        assert_eq!(counted.lookups(), 2, "each refused statement planned once");
        // The statement policy still refuses before anything plans.
        let err = simple_query(
            &service,
            &mut client,
            "COPY (SELECT n FROM counted.ops) TO '/tmp/x.csv'",
        )
        .await
        .unwrap_err();
        assert_eq!(user_error(err).0, "0A000");
        assert_eq!(counted.lookups(), 2);
    }

    #[tokio::test]
    async fn a_simple_statement_that_does_not_plan_gets_the_planner_error() {
        let (service, _) = simple_service();
        let mut client = datafusion_postgres::testing::MockClient::new();
        let err = simple_query(&service, &mut client, "SELECT * FROM counted.missing")
            .await
            .unwrap_err();
        assert!(matches!(err, PgWireError::ApiError(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_simple_insert_answers_the_rows_it_added() {
        let (service, counted) = simple_service();
        let mut client = datafusion_postgres::testing::MockClient::new();
        let mut responses = simple_query(&service, &mut client, "INSERT INTO counted.ops VALUES ('b', 7)")
            .await
            .unwrap();
        match responses.remove(0) {
            Response::Execution(tag) => assert_eq!(tag, Tag::new("INSERT").with_oid(0).with_rows(1)),
            other => panic!("{other:?}"),
        }
        let mut responses = simple_query(&service, &mut client, "SELECT n FROM counted.ops")
            .await
            .unwrap();
        assert_eq!(row_count(responses.remove(0)).await, 4);
        assert_eq!(counted.lookups(), 2, "each statement planned once");
    }

    #[tokio::test]
    async fn statement_timeout_is_read_where_set_statement_timeout_writes_it() {
        let (service, _) = simple_service();
        let mut client = datafusion_postgres::testing::MockClient::new();
        simple_query(&service, &mut client, "SET statement_timeout TO '5000ms'")
            .await
            .unwrap();
        assert_eq!(statement_timeout(&client), Some(Duration::from_millis(5000)));
    }

    #[test]
    fn tracing_hook_only_when_exporting() {
        assert_eq!(all(Some(1.0)).len(), 8);
        assert_eq!(all(None).len(), 7, "no tracing hook when traces are not exported");
    }
}
