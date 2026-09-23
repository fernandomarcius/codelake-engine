//! PostgreSQL wire protocol hooks. The metrics hook only observes statements;
//! it never answers them, so the default behaviour of `datafusion-postgres` is kept.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::hooks::cursor::CursorStatementHook;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::hooks::transactions::TransactionStatementHook;
use datafusion_postgres::hooks::{HookClient, QueryHook};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::error::PgWireResult;

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

/// Default hooks of `datafusion-postgres` plus ours. Passing a custom list
/// replaces the defaults, so they are re-added explicitly here.
pub(crate) fn all() -> Vec<Arc<dyn QueryHook>> {
    vec![
        Arc::new(CursorStatementHook),
        Arc::new(SetShowHook),
        Arc::new(TransactionStatementHook),
        Arc::new(MetricsHook),
    ]
}
