//! Query engine: one `DataFusion` session per node, federated across sources and
//! bounded by [`EngineRuntime`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion_federation::FederatedQueryPlanner;
use futures::future::join_all;
use serde::Serialize;
use tracing::{info, warn};

use crate::config::{ExecutionConfig, MemoryConfig};
use crate::connector::{Connector, ConnectorError, ViewDefinition, WritableCatalog};
use crate::ddl::{self, Prepared};
use crate::federation_guard::FederationFilterGuard;
use crate::information_schema::CatalogList;
use crate::runtime::{EngineRuntime, MemoryStats};
use crate::session_properties::{PropertyRegistry, SessionProperties};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error("source `{0}` did not answer within {1:?}")]
    Timeout(String, Duration),
}

/// Owner of objects created by statements that carry no user.
pub const DEFAULT_USER: &str = "codelake";

/// Per-query settings supplied by a client protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    pub user: String,
    /// Default catalog for unqualified names (`X-Trino-Catalog`).
    pub catalog: Option<String>,
    /// Default schema for unqualified names (`X-Trino-Schema`).
    pub schema: Option<String>,
    /// Validated session properties (see [`crate::PropertyRegistry`]).
    pub properties: SessionProperties,
}

impl QueryOptions {
    pub fn for_user(user: &str) -> Self {
        Self {
            user: user.to_owned(),
            catalog: None,
            schema: None,
            properties: SessionProperties::default(),
        }
    }
}

/// Logical rules: `DataFusion` defaults with federation right after scalar
/// subquery decorrelation (upstream's position) and an extra `push_down_filter`
/// just before it. Upstream alone federates before filters move below joins,
/// so a filter over a cross-source join never reaches its source. Federating
/// after *all* rules instead lets `optimize_projections` create derived tables
/// that the unparser renders as invalid SQL for PostgreSQL and SQL Server.
///
/// Checked arithmetic (see [`crate::overflow`]) is turned back into operators
/// right before federation, so a federated sub-plan renders as plain SQL, and
/// checked again right after it for what runs in the engine.
pub fn optimizer_rules() -> Vec<Arc<dyn datafusion::optimizer::OptimizerRule + Send + Sync>> {
    let mut rules = datafusion::optimizer::Optimizer::new().rules;
    let position = rules
        .iter()
        .position(|r| r.name() == "scalar_subquery_to_join")
        .map_or(rules.len(), |p| p + 1);
    rules.insert(position, Arc::new(crate::overflow::CheckAfterFederation));
    rules.insert(
        position,
        Arc::new(datafusion_federation::FederationOptimizerRule::new()),
    );
    rules.insert(
        position,
        Arc::new(datafusion::optimizer::push_down_filter::PushDownFilter::new()),
    );
    rules.insert(position, Arc::new(crate::overflow::UncheckBeforeFederation));
    rules
}

/// `DataFusion`'s analyzer rules followed by the engine's: checked arithmetic
/// ([`crate::overflow`]) and the one-row rule of scalar subqueries
/// ([`crate::subquery`]). Both need the types that type coercion settles.
pub fn analyzer_rules() -> Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> {
    let mut rules = datafusion::optimizer::Analyzer::new().rules;
    rules.push(Arc::new(crate::overflow::CheckedArithmetic));
    rules.push(Arc::new(crate::subquery::EnforceScalarSubqueryRows));
    rules
}

/// Default `DataFusion` physical rules, preceded by the federation guard (it must
/// run before `FilterPushdown`; see [`crate::federation_guard`]).
fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![Arc::new(FederationFilterGuard)];
    rules.extend(PhysicalOptimizer::new().rules);
    rules
}

/// Result of attaching one source at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attach {
    Attached,
    /// Optional source that failed; the node starts without it.
    Skipped(String),
}

/// Reachability of one source, as reported by readiness and `/healthz`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceStatus {
    pub name: String,
    pub kind: &'static str,
    /// Required sources gate readiness; optional ones only show up in health.
    pub required: bool,
    pub up: bool,
    pub error: Option<String>,
}

pub struct Engine {
    ctx: SessionContext,
    runtime: EngineRuntime,
    sources: Vec<(Arc<dyn Connector>, bool)>,
    writable: HashMap<String, Arc<dyn WritableCatalog>>,
    properties: PropertyRegistry,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("session_id", &self.ctx.session_id())
            .field("runtime", &self.runtime)
            .field("sources", &self.sources)
            .field("writable", &self.writable.keys().collect::<Vec<_>>())
            .field("properties", &self.properties)
            .finish()
    }
}

impl Engine {
    pub fn new(memory: &MemoryConfig, execution: &ExecutionConfig) -> Result<Self, EngineError> {
        let runtime = EngineRuntime::new(memory)?;
        // `information_schema` is provided per catalog by `CatalogList`.
        let mut session = SessionConfig::new()
            .with_batch_size(execution.batch_size)
            .with_information_schema(false);
        if let Some(partitions) = execution.target_partitions {
            session = session.with_target_partitions(partitions);
        }
        let state = SessionStateBuilder::new()
            .with_config(session)
            .with_runtime_env(runtime.env())
            .with_catalog_list(Arc::new(CatalogList::new()))
            .with_default_features()
            .with_analyzer_rules(analyzer_rules())
            .with_optimizer_rules(optimizer_rules())
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
            .with_physical_optimizer_rules(physical_rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(crate::functions::varbinary_literal());
        Ok(Self {
            ctx,
            runtime,
            sources: Vec::new(),
            writable: HashMap::new(),
            properties: PropertyRegistry::default(),
        })
    }

    /// Runs a statement as the engine's own user. See [`Engine::sql_as`].
    pub async fn sql(&self, sql: &str) -> Result<DataFrame, DataFusionError> {
        self.sql_as(DEFAULT_USER, sql).await
    }

    /// Plans a statement the way clients should run it:
    /// - DDL on a writable catalog (Hive) goes to that catalog, owned by `user`;
    /// - uncorrelated scalar subqueries in predicates are evaluated first so
    ///   their values can be pushed down (see [`crate::subquery`]).
    pub async fn sql_as(&self, user: &str, sql: &str) -> Result<DataFrame, DataFusionError> {
        self.sql_with(&QueryOptions::for_user(user), sql).await
    }

    /// [`Engine::sql_as`] with per-query defaults (catalog, schema), as set by
    /// client protocols. The query runs in a session derived from the engine's:
    /// same catalogs, runtime and memory pool, its own defaults.
    pub async fn sql_with(&self, options: &QueryOptions, sql: &str) -> Result<DataFrame, DataFusionError> {
        let ctx = self.session(options);
        // Planning futures are large; boxing keeps callers' futures small.
        Box::pin(self.plan_and_run(&ctx, &options.user, sql)).await
    }

    fn session(&self, options: &QueryOptions) -> SessionContext {
        let mut state = self.ctx.state();
        let defaults = &mut state.config_mut().options_mut().catalog;
        if let Some(catalog) = &options.catalog {
            defaults.default_catalog.clone_from(catalog);
        }
        if let Some(schema) = &options.schema {
            defaults.default_schema.clone_from(schema);
        }
        SessionContext::new_with_state(state)
    }

    async fn plan_and_run(
        &self,
        ctx: &SessionContext,
        user: &str,
        sql: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let mut statement = state.sql_to_statement(sql, &dialect)?;
        let prepared = ddl::prepare(&mut statement)?;
        // After `prepare`, which keeps a view's text as written.
        crate::literals::rewrite_zoned_timestamps(&mut statement);
        if let Some(Prepared::AddColumn {
            table,
            column,
            sql_type,
        }) = &prepared
        {
            return self.add_column(ctx, table, column, sql_type).await;
        }
        let plan = state.statement_to_plan(statement).await?;
        if ddl::is_routable(&plan)
            && let Some(result) = self.route_ddl(ctx, user, &plan, prepared).await?
        {
            return Ok(result);
        }
        let plan = crate::subquery::pre_evaluate_scalar_subqueries(ctx, plan).await?;
        ctx.execute_logical_plan(plan).await
    }

    fn resolve(ctx: &SessionContext, table: &TableReference) -> (String, String, String) {
        let state = ctx.state();
        let defaults = &state.config().options().catalog;
        let resolved = table
            .clone()
            .resolve(&defaults.default_catalog, &defaults.default_schema);
        (
            resolved.catalog.to_string(),
            resolved.schema.to_string(),
            resolved.table.to_string(),
        )
    }

    fn writable(&self, catalog: &str) -> Option<&Arc<dyn WritableCatalog>> {
        self.writable.get(catalog)
    }

    fn empty(ctx: &SessionContext) -> Result<DataFrame, DataFusionError> {
        ctx.read_empty()
    }

    fn row_count(ctx: &SessionContext, rows: u64) -> Result<DataFrame, DataFusionError> {
        use datafusion::arrow::array::{RecordBatch, UInt64Array};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("rows", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![rows]))])?;
        ctx.read_batch(batch)
    }

    /// `CREATE TABLE [AS]` in `DataFusion`'s in-memory catalogs, as `DataFusion`
    /// does it, but answering the rows written, like a writable catalog (and
    /// like Trino's `CREATE TABLE AS`).
    async fn create_memory_table(
        ctx: &SessionContext,
        create: &datafusion::logical_expr::CreateMemoryTable,
    ) -> Result<DataFrame, DataFusionError> {
        use datafusion::datasource::MemTable;
        if create.temporary {
            return Err(DataFusionError::NotImplemented(
                "Temporary tables not supported".into(),
            ));
        }
        if create.if_not_exists && create.or_replace {
            return Err(DataFusionError::Execution(
                "'IF NOT EXISTS' cannot coexist with 'REPLACE'".into(),
            ));
        }
        let exists = ctx.table_exist(create.name.clone())?;
        if exists && create.if_not_exists {
            return Self::row_count(ctx, 0);
        }
        if exists && !create.or_replace {
            return Err(DataFusionError::Execution(format!(
                "Table '{}' already exists",
                create.name
            )));
        }
        let input =
            crate::subquery::pre_evaluate_scalar_subqueries(ctx, create.input.as_ref().clone()).await?;
        let schema = Arc::clone(input.schema().inner());
        let batches = DataFrame::new(ctx.state(), input).collect_partitioned().await?;
        let rows = batches
            .iter()
            .flatten()
            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .sum();
        let table = MemTable::try_new(schema, batches)?
            .with_constraints(create.constraints.clone())
            .with_column_defaults(create.column_defaults.iter().cloned().collect());
        if exists {
            ctx.deregister_table(create.name.clone())?;
        }
        ctx.register_table(create.name.clone(), Arc::new(table))?;
        Self::row_count(ctx, rows)
    }

    async fn add_column(
        &self,
        ctx: &SessionContext,
        table: &TableReference,
        column: &str,
        sql_type: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let (catalog, schema, name) = Self::resolve(ctx, table);
        let writable = self.writable(&catalog).ok_or_else(|| {
            DataFusionError::Plan(format!("catalog {catalog} does not support ALTER TABLE"))
        })?;
        // Let the SQL planner translate the type name exactly as it would in a cast.
        let probe = ctx.sql(&format!("SELECT CAST(NULL AS {sql_type}) AS t")).await?;
        let data_type = probe.schema().field(0).data_type().clone();
        writable.add_column(&schema, &name, column, &data_type).await?;
        Self::empty(ctx)
    }

    async fn route_ddl(
        &self,
        ctx: &SessionContext,
        user: &str,
        plan: &LogicalPlan,
        prepared: Option<Prepared>,
    ) -> Result<Option<DataFrame>, DataFusionError> {
        let LogicalPlan::Ddl(ddl) = plan else {
            return Ok(None);
        };
        match ddl {
            DdlStatement::CreateMemoryTable(create) => {
                let (catalog, schema, table) = Self::resolve(ctx, &create.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Self::create_memory_table(ctx, create).await.map(Some);
                };
                if create.or_replace {
                    writable.drop_table(&schema, &table, true).await?;
                }
                let input =
                    crate::subquery::pre_evaluate_scalar_subqueries(ctx, create.input.as_ref().clone())
                        .await?;
                let rows = ctx.execute_logical_plan(input).await?.execute_stream().await?;
                let written = writable
                    .create_table_as(&schema, &table, create.if_not_exists, user, rows, ctx.task_ctx())
                    .await?;
                Ok(Some(Self::row_count(ctx, written)?))
            }
            DdlStatement::DropTable(drop) => {
                let (catalog, schema, table) = Self::resolve(ctx, &drop.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Ok(None);
                };
                writable.drop_table(&schema, &table, drop.if_exists).await?;
                Ok(Some(Self::empty(ctx)?))
            }
            DdlStatement::CreateView(view) => {
                let (catalog, schema, name) = Self::resolve(ctx, &view.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Ok(None);
                };
                let Some(Prepared::ViewSql(sql)) = prepared else {
                    return Err(DataFusionError::Internal("view SQL was not captured".into()));
                };
                let session = ctx.state().config().options().catalog.clone();
                let definition = ViewDefinition {
                    sql,
                    output: Arc::new(view.input.schema().as_arrow().clone()),
                    catalog: session.default_catalog,
                    schema: session.default_schema,
                };
                writable
                    .create_view(&schema, &name, view.or_replace, user, definition)
                    .await?;
                Ok(Some(Self::empty(ctx)?))
            }
            DdlStatement::DropView(drop) => {
                let (catalog, schema, name) = Self::resolve(ctx, &drop.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Ok(None);
                };
                writable.drop_view(&schema, &name, drop.if_exists).await?;
                Ok(Some(Self::empty(ctx)?))
            }
            DdlStatement::CreateCatalogSchema(create) => {
                // The session's default catalog: client protocols override it.
                let default_catalog = ctx.state().config().options().catalog.default_catalog.clone();
                let (catalog, schema) = create.schema_name.split_once('.').map_or(
                    (default_catalog.as_str(), create.schema_name.as_str()),
                    |(c, s)| (c, s),
                );
                let Some(writable) = self.writable(catalog) else {
                    return Ok(None);
                };
                writable.create_schema(schema, create.if_not_exists, user).await?;
                Ok(Some(Self::empty(ctx)?))
            }
            _ => Ok(None),
        }
    }

    /// Registers a source as a SQL catalog. Required sources abort startup on
    /// failure; optional ones are skipped and reported.
    pub async fn attach(
        &mut self,
        connector: Arc<dyn Connector>,
        required: bool,
        timeout: Duration,
    ) -> Result<Attach, EngineError> {
        let name = connector.name().to_owned();
        let failure = match tokio::time::timeout(timeout, connector.catalog()).await {
            Ok(Ok(catalog)) => {
                self.ctx.register_catalog(&name, catalog);
                if let Some(writable) = connector.writable() {
                    self.writable.insert(name.clone(), writable);
                }
                self.properties.add_catalog(&name, connector.session_properties());
                self.sources.push((connector, required));
                info!(source = %name, "source attached");
                return Ok(Attach::Attached);
            }
            Ok(Err(err)) => EngineError::from(err),
            Err(_) => EngineError::Timeout(name.clone(), timeout),
        };
        if required {
            return Err(failure);
        }
        warn!(source = %name, error = %failure, "optional source skipped");
        Ok(Attach::Skipped(failure.to_string()))
    }

    /// Session properties clients may set: system ones plus each catalog's.
    pub fn session_properties(&self) -> &PropertyRegistry {
        &self.properties
    }

    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    pub fn memory(&self) -> MemoryStats {
        self.runtime.stats()
    }

    /// Pings every attached source concurrently.
    pub async fn check_sources(&self, timeout: Duration) -> Vec<SourceStatus> {
        join_all(self.sources.iter().map(|(source, required)| async move {
            let result = match tokio::time::timeout(timeout, source.ping()).await {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(err.to_string()),
                Err(_) => Some(format!("no answer within {timeout:?}")),
            };
            SourceStatus {
                name: source.name().to_owned(),
                kind: source.kind(),
                required: *required,
                up: result.is_none(),
                error: result,
            }
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::ByteSize;
    use async_trait::async_trait;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::datasource::MemTable;

    #[derive(Debug)]
    enum Behaviour {
        Healthy,
        Broken,
        Hangs,
    }

    #[derive(Debug)]
    struct FakeSource {
        name: &'static str,
        behaviour: Behaviour,
    }

    #[async_trait]
    impl Connector for FakeSource {
        fn name(&self) -> &str {
            self.name
        }
        fn kind(&self) -> &'static str {
            "fake"
        }
        async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
            self.ping().await?;
            let schema = MemorySchemaProvider::new();
            let catalog = MemoryCatalogProvider::new();
            catalog
                .register_schema("public", Arc::new(schema))
                .map_err(|e| ConnectorError::Metadata {
                    source_name: self.name.into(),
                    cause: e.into(),
                })?;
            Ok(Arc::new(catalog))
        }
        async fn ping(&self) -> Result<(), ConnectorError> {
            match self.behaviour {
                Behaviour::Healthy => Ok(()),
                Behaviour::Broken => Err(ConnectorError::Unreachable {
                    source_name: self.name.into(),
                    cause: "connection refused".into(),
                }),
                Behaviour::Hangs => std::future::pending().await,
            }
        }
    }

    fn engine(dir: &std::path::Path, limit: ByteSize, spill: ByteSize) -> Engine {
        let memory = MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: spill,
            metadata_cache_limit: ByteSize::mib(1),
        };
        let execution = ExecutionConfig {
            target_partitions: Some(1),
            ..ExecutionConfig::default()
        };
        Engine::new(&memory, &execution).unwrap()
    }

    fn source(name: &'static str, behaviour: Behaviour) -> Arc<dyn Connector> {
        Arc::new(FakeSource { name, behaviour })
    }

    const FAST: Duration = Duration::from_millis(200);

    #[test]
    fn filters_are_pushed_right_before_federation() {
        let names: Vec<String> = optimizer_rules().iter().map(|r| r.name().to_owned()).collect();
        let federation = names
            .iter()
            .position(|n| n == "federation_optimizer_rule")
            .unwrap();
        let decorrelate = names.iter().position(|n| n == "scalar_subquery_to_join").unwrap();
        let projections = names.iter().position(|n| n == "optimize_projections").unwrap();
        assert_eq!(names[federation - 1], "push_down_filter", "{names:?}");
        assert!(decorrelate < federation && federation < projections, "{names:?}");
    }

    #[test]
    fn federation_guard_runs_before_filter_pushdown() {
        let names: Vec<String> = physical_rules().iter().map(|r| r.name().to_owned()).collect();
        let guard = names.iter().position(|n| n == "federation_filter_guard").unwrap();
        let pushdown = names.iter().position(|n| n.contains("FilterPushdown")).unwrap();
        assert!(guard < pushdown, "{names:?}");
    }

    #[tokio::test]
    async fn required_source_failure_aborts_and_optional_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));

        let ok = engine
            .attach(source("erp", Behaviour::Healthy), true, FAST)
            .await
            .unwrap();
        assert_eq!(ok, Attach::Attached);
        assert!(engine.context().catalog("erp").is_some());

        let err = engine
            .attach(source("down", Behaviour::Broken), true, FAST)
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Connector(_)), "{err}");

        let err = engine
            .attach(source("slow", Behaviour::Hangs), true, FAST)
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::Timeout(..)), "{err}");

        let skipped = engine
            .attach(source("opt", Behaviour::Broken), false, FAST)
            .await
            .unwrap();
        assert!(matches!(skipped, Attach::Skipped(ref m) if m.contains("connection refused")));
        let skipped = engine
            .attach(source("opt2", Behaviour::Hangs), false, FAST)
            .await
            .unwrap();
        assert!(matches!(skipped, Attach::Skipped(ref m) if m.contains("did not answer")));

        assert!(engine.context().catalog("down").is_none());
        assert!(engine.context().catalog("opt").is_none());
    }

    /// Trino 431: `timestamp(3) with time zone`
    /// `2026-01-01 10:00:00.123 America/Sao_Paulo`, the instant 13:00:00.123 UTC.
    #[tokio::test]
    async fn timestamp_literal_keeps_its_zone_and_precision() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::{TimeUnit, TimestampMillisecondType, TimestampSecondType};
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let batches = engine
            .sql("SELECT TIMESTAMP '2026-01-01 10:00:00.123 America/Sao_Paulo' p, TIMESTAMP '2026-01-01 10:00:00 +03:00' q, TIMESTAMP '2026-01-01 10:00:00.123' r")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let schema = batches[0].schema();
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("America/Sao_Paulo".into()))
        );
        assert_eq!(
            batches[0]
                .column(0)
                .as_primitive::<TimestampMillisecondType>()
                .value(0),
            1_767_272_400_123
        );
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Second, Some("+03:00".into()))
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_primitive::<TimestampSecondType>()
                .value(0),
            1_767_250_800
        );
        // Without a zone: unchanged.
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    /// `<catalog>.information_schema` lists that catalog only.
    #[tokio::test]
    async fn information_schema_is_per_catalog() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        engine
            .attach(source("erp", Behaviour::Healthy), true, FAST)
            .await
            .unwrap();
        for (sql, expected) in [
            (
                "SELECT count(*) FROM datafusion.information_schema.schemata WHERE catalog_name <> 'datafusion'",
                0,
            ),
            (
                "SELECT count(*) FROM datafusion.information_schema.tables WHERE table_catalog <> 'datafusion'",
                0,
            ),
            (
                "SELECT count(*) FROM erp.information_schema.schemata WHERE catalog_name = 'erp'",
                1,
            ),
            (
                "SELECT count(*) FROM erp.information_schema.schemata WHERE catalog_name <> 'erp'",
                0,
            ),
        ] {
            let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
            assert_eq!(
                batches[0].column(0).as_primitive::<Int64Type>().value(0),
                expected,
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn check_sources_reports_each_source() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        engine
            .attach(source("erp", Behaviour::Healthy), true, FAST)
            .await
            .unwrap();

        let statuses = engine.check_sources(FAST).await;
        assert_eq!(
            statuses,
            vec![SourceStatus {
                name: "erp".into(),
                kind: "fake",
                required: true,
                up: true,
                error: None
            }]
        );
    }

    #[tokio::test]
    async fn check_sources_flags_down_and_hanging_sources() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        engine.sources.push((source("down", Behaviour::Broken), true));
        engine.sources.push((source("slow", Behaviour::Hangs), false));

        let statuses = engine.check_sources(FAST).await;
        assert!(statuses.iter().all(|s| !s.up));
        assert!(statuses[0].required && !statuses[1].required);
        assert!(
            statuses[0]
                .error
                .as_deref()
                .unwrap()
                .contains("connection refused")
        );
        assert!(statuses[1].error.as_deref().unwrap().contains("no answer"));
    }

    async fn rows_written(engine: &Engine, sql: &str) -> u64 {
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let rows = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        rows.value(0)
    }

    #[tokio::test]
    async fn in_memory_create_table_as_reports_rows_written() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let sql = "CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3)) v(a)";
        assert_eq!(rows_written(&engine, sql).await, 3);
        assert_eq!(
            rows_written(&engine, "CREATE TABLE IF NOT EXISTS t AS SELECT 9 AS a").await,
            0
        );
        let exists = engine.sql("CREATE TABLE t AS SELECT 9 AS a").await.unwrap_err();
        assert!(exists.to_string().contains("already exists"), "{exists}");
        assert_eq!(
            rows_written(&engine, "CREATE OR REPLACE TABLE t AS SELECT 9 AS a").await,
            1
        );
        let count = engine
            .sql("SELECT count(*) AS n FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let n = count[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 1);
    }

    /// ~40 MiB of rows, sorted under a 16 MiB ceiling.
    async fn sort_large_table(engine: &Engine) -> datafusion::error::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batches = (0..40)
            .map(|b| {
                let keys: Vec<i64> = (0..65_536)
                    .map(|i| (i * 7919 + b * 104_729) % 2_621_440)
                    .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(keys.clone())),
                        Arc::new(Int64Array::from(keys)),
                    ],
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let table = MemTable::try_new(schema, vec![batches])?;
        engine.context().register_table("big", Arc::new(table))?;
        engine
            .context()
            // A full sort (no LIMIT, which would become a bounded TopK) feeding a
            // window function: the sort must hold every row or spill.
            .sql("SELECT max(rn) AS n FROM (SELECT row_number() OVER (ORDER BY k DESC, v) AS rn FROM big)")
            .await?
            .collect()
            .await
    }

    #[tokio::test]
    async fn query_larger_than_memory_spills_instead_of_failing() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(16), ByteSize::mib(512));
        let result = sort_large_table(&engine).await.unwrap();
        let n = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(n.value(0), 40 * 65_536);
        assert!(engine.memory().reserved_bytes <= ByteSize::mib(16).as_u64());
    }

    #[tokio::test]
    async fn query_fails_cleanly_when_memory_and_spill_are_exhausted() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(16), ByteSize::b(1));
        let err = sort_large_table(&engine).await.unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("Resources exhausted") || message.contains("exceeds the maximum"),
            "{message}"
        );
    }
}
