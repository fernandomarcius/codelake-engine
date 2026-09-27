//! Distributed execution (ROADMAP M4, ADR 0011), behind the `distributed`
//! feature.
//!
//! One binary, two roles:
//!
//! - **Coordinator**: the server clients talk to. Its planner is wrapped by
//!   `datafusion-distributed`'s: every query is planned as on a single node
//!   (all of `CodeLake`'s analyzer, logical and physical rules run here, once),
//!   then network boundaries are injected where the plan repartitions. A query
//!   whose input fits one task stays on the coordinator: distribution only
//!   happens where it pays for itself.
//! - **Worker**: executes the stages it is sent. Its session is its own
//!   [`Engine`]'s (the same functions, codecs and memory pool, ADR 0003), with
//!   the query's settings carried from the coordinator by [`SessionOptions`].
//!
//! Workers never plan and never commit: writes stay on the coordinator.

use std::net::SocketAddr;
use std::sync::Arc;

use codelake_core::Engine;
use codelake_core::output::{ACCOUNTED_OUTPUT, AccountedOutput};
use codelake_core::portable::CoreCodec;
use datafusion::common::{DataFusionError, Result, extensions_options};
use datafusion::config::{ConfigExtension, ConfigOptions};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DistributedConfig, DistributedExt, SessionStateBuilderExt, Worker, WorkerQueryContext, WorkerResolver,
};
use url::Url;

extensions_options! {
    /// The query's settings that differ from the node's (time zone, catalog
    /// defaults, session properties), carried to every worker of the query.
    pub struct SessionOptions {
        /// `key\tvalue` lines, base64: header values are visible ASCII only.
        pub entries: String, default = String::new()
    }
}

impl ConfigExtension for SessionOptions {
    const PREFIX: &'static str = "codelake_session";
}

/// Option keys that are never carried: the distributed layer's own.
fn carried(key: &str) -> bool {
    !key.starts_with("distributed.") && !key.starts_with("codelake_session.")
}

fn entries(options: &ConfigOptions) -> Vec<(String, Option<String>)> {
    options
        .entries()
        .into_iter()
        .filter(|entry| carried(&entry.key))
        .map(|entry| (entry.key, entry.value))
        .collect()
}

/// The settings of `query` that differ from `base`, encoded for
/// [`SessionOptions::entries`].
fn encode_difference(base: &[(String, Option<String>)], query: &ConfigOptions) -> String {
    use base64::Engine as _;
    let mut lines = String::new();
    for (key, value) in entries(query) {
        let Some(value) = value else { continue };
        let same = base
            .iter()
            .any(|(k, v)| *k == key && v.as_deref() == Some(value.as_str()));
        if !same {
            lines.push_str(&key);
            lines.push('\t');
            lines.push_str(&value);
            lines.push('\n');
        }
    }
    base64::engine::general_purpose::STANDARD.encode(lines)
}

/// Applies what [`encode_difference`] wrote to `options`.
fn apply_difference(options: &mut ConfigOptions, encoded: &str) -> Result<()> {
    use base64::Engine as _;
    if encoded.is_empty() {
        return Ok(());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| DataFusionError::Internal(format!("session options: {e}")))?;
    let text =
        String::from_utf8(bytes).map_err(|e| DataFusionError::Internal(format!("session options: {e}")))?;
    for line in text.lines() {
        if let Some((key, value)) = line.split_once('\t') {
            options.set(key, value)?;
        }
    }
    Ok(())
}

/// A fixed list of workers (`[distributed] workers` in the configuration).
#[derive(Debug, Clone)]
pub struct StaticWorkers(pub Vec<Url>);

impl WorkerResolver for StaticWorkers {
    fn get_urls(&self) -> Result<Vec<Url>> {
        Ok(self.0.clone())
    }
}

/// Makes `engine` a coordinator: its planner distributes queries to the
/// workers `resolver` names. `customize` adds what the deployment needs on
/// top (a channel resolver, tuning), and runs last.
pub fn coordinate<R: WorkerResolver + 'static>(
    engine: &mut Engine,
    resolver: R,
    customize: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
) {
    engine.map_state(|mut builder| {
        // The output rule turns every `CoalescePartitionsExec` into an
        // `AccountedMergeExec`; the distributed planner cuts stages at the
        // former. It runs after distribution instead, on every stage.
        if let Some(rules) = builder.physical_optimizer_rules() {
            rules.retain(|rule| rule.name() != ACCOUNTED_OUTPUT);
        }
        let mut builder = customize(
            builder
                .with_distributed_option_extension(SessionOptions::default())
                .with_distributed_user_codec(CoreCodec)
                .with_distributed_worker_resolver(resolver)
                .with_distributed_planner(),
        );
        let distributed = std::mem::take(builder.query_planner());
        builder.with_query_planner(Arc::new(AccountedAfterDistribution { distributed }))
    });
    let base = entries(engine.context().state().config_options());
    engine.with_session_hook(Arc::new(move |config: &mut SessionConfig| {
        let encoded = encode_difference(&base, config.options());
        if let Some(options) = config.options_mut().extensions.get_mut::<SessionOptions>() {
            options.entries = encoded;
        }
    }));
}

/// The distributed planner, then the output rule (`AccountedOutput`) on the
/// distributed plan: merges reserve their batches on every node (ADR 0003).
#[derive(Debug)]
struct AccountedAfterDistribution {
    distributed: Option<Arc<dyn QueryPlanner + Send + Sync>>,
}

#[async_trait::async_trait]
impl QueryPlanner for AccountedAfterDistribution {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = match &self.distributed {
            Some(planner) => planner.create_physical_plan(logical_plan, session_state).await?,
            None => {
                DefaultPhysicalPlanner::default()
                    .create_physical_plan(logical_plan, session_state)
                    .await?
            }
        };
        AccountedOutput.optimize(plan, session_state.config_options())
    }
}

/// The session a worker runs one task in: `base` (the worker engine's
/// state: functions, codecs, runtime and memory pool), with the task's
/// configuration from the library and the query's settings from the
/// coordinator.
fn task_state(base: &SessionState, ctx: WorkerQueryContext) -> Result<SessionState> {
    let mut builder = ctx
        .builder
        .with_distributed_option_extension_from_headers::<SessionOptions>(&ctx.headers)?;
    let mut config = builder.config().take().unwrap_or_default();
    let task = config.options();
    let distributed = task.extensions.get::<DistributedConfig>().cloned();
    let session = task.extensions.get::<SessionOptions>().cloned();
    let batch_size = task.execution.batch_size;
    let mut options = base.config_options().as_ref().clone();
    if let Some(session) = &session {
        apply_difference(&mut options, &session.entries)?;
    }
    options.execution.batch_size = batch_size;
    if let Some(distributed) = distributed {
        options.extensions.insert(distributed);
    }
    if let Some(session) = session {
        options.extensions.insert(session);
    }
    *config.options_mut() = options;
    Ok(codelake_core::rebuild_state(base, |builder| {
        builder
            .with_config(config)
            .with_runtime_env(Arc::clone(base.runtime_env()))
            .with_distributed_user_codec(CoreCodec)
    }))
}

/// The worker service of `engine`, to mount on a tonic server.
pub fn worker(engine: &Engine) -> Worker {
    let base = engine.context().state();
    Worker::from_session_builder(move |ctx: WorkerQueryContext| {
        let base = base.clone();
        async move { task_state(&base, ctx) }
    })
    .with_runtime_env(engine.runtime_env())
}

/// Serves `engine`'s worker service on `listen` until `shutdown` resolves.
///
/// # Errors
///
/// The listener could not be bound, or the server failed.
pub async fn serve_worker(
    engine: &Engine,
    listen: SocketAddr,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(worker(engine).into_worker_server())
        .serve_with_shutdown(listen, shutdown)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_differing_settings_are_carried_and_restored() {
        let base_options = ConfigOptions::default();
        let base = entries(&base_options);
        let mut query = ConfigOptions::default();
        query.execution.time_zone = Some("America/Sao_Paulo".to_owned());
        query.catalog.default_schema = "vendas".to_owned();
        let encoded = encode_difference(&base, &query);
        let mut restored = ConfigOptions::default();
        apply_difference(&mut restored, &encoded).unwrap();
        assert_eq!(restored.execution.time_zone.as_deref(), Some("America/Sao_Paulo"));
        assert_eq!(restored.catalog.default_schema, "vendas");
        assert_eq!(encode_difference(&base, &base_options), "");
    }
}
