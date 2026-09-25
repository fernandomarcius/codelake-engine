//! PostgreSQL wire protocol hooks. The metrics and tracing hooks only observe
//! statements; they never answer them, so the default behaviour of
//! `datafusion-postgres` is kept.

use std::sync::Arc;

use async_trait::async_trait;
use codelake_core::trace::{self, TARGET, TraceParent};
use codelake_core::{operand_checks, statement_policy};
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_postgres::hooks::cursor::CursorStatementHook;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::hooks::transactions::TransactionStatementHook;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_USER};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

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
/// protocol, which runs statements on the session directly. It also refuses
/// the operand types the engine does not combine (text against a number,
/// `time ± interval`; ADR 0014), with PostgreSQL's typing rules and errors
/// ([`codelake_core::operand_checks::postgres_refusal`]).
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

/// A simple-protocol statement arrives unplanned: it is planned here to
/// check its operands. A statement that does not plan (a command answered by
/// a later hook, a syntax `DataFusion` rejects) is left to what runs it,
/// which reports its own error.
async fn check_simple(statement: &Statement, session_context: &SessionContext) -> PgWireResult<()> {
    let planned = session_context
        .state()
        .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
            statement.clone(),
        )))
        .await;
    match planned {
        Ok(plan) => check_operands(&plan),
        Err(_) => Ok(()),
    }
}

#[async_trait]
impl QueryHook for PolicyHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if let Err(err) = check(statement) {
            return Some(Err(err));
        }
        check_simple(statement, session_context).await.err().map(Err)
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

/// Default hooks of `datafusion-postgres` plus ours. Passing a custom list
/// replaces the defaults, so they are re-added explicitly here. The tracing
/// observer goes first so statements answered by a default hook (`SET`,
/// `BEGIN`, cursors) are traced too.
///
/// `session_sampling` is `None` when traces are not exported: no tracing hook.
pub(crate) fn all(session_sampling: Option<f64>) -> Vec<Arc<dyn QueryHook>> {
    let mut hooks: Vec<Arc<dyn QueryHook>> = Vec::with_capacity(5);
    if let Some(ratio) = session_sampling {
        hooks.push(Arc::new(TracingHook { ratio }));
    }
    hooks.push(Arc::new(CursorStatementHook));
    hooks.push(Arc::new(SetShowHook));
    hooks.push(Arc::new(TransactionStatementHook));
    hooks.push(Arc::new(MetricsHook));
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

    #[test]
    fn tracing_hook_only_when_exporting() {
        assert_eq!(all(Some(1.0)).len(), 5);
        assert_eq!(all(None).len(), 4, "no tracing hook when traces are not exported");
    }
}
