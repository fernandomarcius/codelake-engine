//! Query engine: one `DataFusion` session per node, federated across sources and
//! bounded by [`EngineRuntime`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SessionStateBuilder};
use datafusion::logical_expr::{DdlStatement, LogicalPlan, ScalarUDF};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion_federation::FederatedQueryPlanner;
use futures::future::join_all;
use serde::Serialize;
use tracing::{info, warn};

use crate::computed_batches::ComputedBatches;
use crate::config::{ExecutionConfig, MemoryConfig};
use crate::connector::{Connector, ConnectorError, ViewDefinition, WritableCatalog};
use crate::ddl::{self, Prepared, RelationKind, Rewrite, ViewSecurity};
use crate::federation_guard::FederationFilterGuard;
use crate::information_schema::CatalogList;
use crate::output::AccountedOutput;
use crate::plain_views::PlainViews;
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
    /// Functions this protocol defines on top of the engine's (for instance
    /// `version()`, which reports the emulated server's version).
    pub functions: Vec<Arc<ScalarUDF>>,
    /// Session time zone (`X-Trino-Time-Zone`), as Trino names it (see
    /// [`crate::trino_time_zone`]); `None` keeps the engine's, `UTC`.
    pub time_zone: Option<String>,
}

impl QueryOptions {
    pub fn for_user(user: &str) -> Self {
        Self {
            user: user.to_owned(),
            catalog: None,
            schema: None,
            properties: SessionProperties::default(),
            functions: Vec::new(),
            time_zone: None,
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

/// `DataFusion`'s analyzer rules with the engine's: Trino's decimal types and
/// common types (module `coercion`) right before type coercion (which then
/// recomputes every schema), then
/// checked arithmetic ([`crate::overflow`]) and the one-row rule of scalar
/// subqueries ([`crate::subquery`]), which need the types coercion settles.
pub fn analyzer_rules() -> Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> {
    let mut rules = datafusion::optimizer::Analyzer::new().rules;
    let coercion = rules
        .iter()
        .position(|r| r.name() == "type_coercion")
        .unwrap_or(rules.len());
    rules.insert(coercion, Arc::new(crate::coercion::TrinoCommonTypes));
    rules.insert(coercion, Arc::new(crate::overflow::TrinoDecimalTypes));
    rules.push(Arc::new(crate::overflow::CheckedArithmetic));
    rules.push(Arc::new(crate::subquery::EnforceScalarSubqueryRows));
    rules
}

/// Default `DataFusion` physical rules, preceded by the federation guard (it must
/// run before `FilterPushdown`; see [`crate::federation_guard`]) and followed by
/// [`PlainViews`], which keeps string views out of operators that retain rows
/// (ADR 0003), [`ComputedBatches`], which sizes the batches of computed values
/// in bytes, and [`AccountedOutput`], which keeps the buffers between the plan
/// and the client inside the memory pool.
fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![Arc::new(FederationFilterGuard)];
    rules.extend(PhysicalOptimizer::new().rules);
    rules.push(Arc::new(PlainViews));
    rules.push(Arc::new(ComputedBatches));
    rules.push(Arc::new(AccountedOutput));
    rules
}

/// Output schema of `plan` once analyzed: the analyzer settles some types
/// after SQL planning (Trino's decimal types, see [`crate::overflow`]), so
/// the schema of a freshly planned `DataFrame` can differ from its rows'.
pub fn analyzed_schema(
    state: &datafusion::execution::SessionState,
    plan: &LogicalPlan,
) -> Result<datafusion::common::DFSchemaRef, DataFusionError> {
    let analyzed = state
        .analyzer()
        .execute_and_check(plan.clone(), state.config_options(), |_, _| {})?;
    Ok(Arc::clone(analyzed.schema()))
}

/// Column types a `CREATE TABLE` declares beyond what `DataFusion` reads:
/// `varchar(n)` and friends ([`crate::declared`]), and Arrow types it has no
/// SQL for (`map(K, V)`).
#[derive(Debug, Default)]
struct NewColumns {
    declared: Vec<(String, crate::trino_types::Declared)>,
    types: Vec<(String, datafusion::arrow::datatypes::DataType)>,
}

/// The schema a new table gets from the rows that create it: the types its
/// `CREATE TABLE` declares (`varchar(10)`, see [`crate::declared`]), and
/// nullable columns when a query creates it, as in Trino.
fn table_schema(
    rows: &datafusion::arrow::datatypes::SchemaRef,
    columns: &NewColumns,
    from_query: bool,
) -> datafusion::arrow::datatypes::SchemaRef {
    let fields: Vec<datafusion::arrow::datatypes::Field> = rows
        .fields()
        .iter()
        .map(|f| {
            match columns
                .types
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(f.name()))
            {
                Some((_, data_type)) => f.as_ref().clone().with_data_type(data_type.clone()),
                None => f.as_ref().clone(),
            }
        })
        .collect();
    let rows = datafusion::arrow::datatypes::Schema::new_with_metadata(fields, rows.metadata().clone());
    let schema = crate::declared::with_declared(&rows, &columns.declared);
    if !from_query {
        return Arc::new(schema);
    }
    let fields: Vec<datafusion::arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// The first name that appears twice.
fn duplicate(names: &[String]) -> Option<&str> {
    let mut seen = std::collections::HashSet::new();
    names
        .iter()
        .find(|n| !seen.insert(n.as_str()))
        .map(String::as_str)
}

fn duplicate_column(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!("Column name '{name}' specified more than once"))
}

/// The columns of a table of the in-memory catalog: lower case, as Trino
/// folds every identifier (`"CamelCol"` is the column `camelcol`); two names
/// that differ only in case are one column named twice.
fn lower_case_columns(
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<datafusion::arrow::datatypes::SchemaRef, DataFusionError> {
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().to_lowercase()).collect();
    if let Some(name) = duplicate(&names) {
        return Err(duplicate_column(name));
    }
    if schema.fields().iter().zip(&names).all(|(f, n)| f.name() == n) {
        return Ok(Arc::clone(schema));
    }
    let fields: Vec<datafusion::arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .zip(names)
        .map(|(f, name)| f.as_ref().clone().with_name(name))
        .collect();
    Ok(Arc::new(datafusion::arrow::datatypes::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// `stream` announced with `schema`, which must hold its batches (same
/// columns, more metadata or nullability).
fn with_schema(
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> datafusion::physical_plan::SendableRecordBatchStream {
    use futures::StreamExt;
    let announced = Arc::clone(&schema);
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        announced,
        stream.map(move |batch| batch.and_then(|b| Ok(b.with_schema(Arc::clone(&schema))?))),
    ))
}

/// Rewrites a parsed statement so `DataFusion` plans it with Trino's
/// semantics: `TIMESTAMP` literals and types, numbers with an exponent,
/// `VALUES` lists, `DECIMAL` without precision, `varchar(n)` and friends (see
/// the modules `literals`, `coercion` and `declared`). Every statement the
/// engine plans goes through it, and so must the SQL of a view a connector
/// plans itself.
pub fn trino_statement(statement: &mut datafusion::sql::parser::Statement, session_zone: &str) {
    crate::coercion::name_casts_in_aggregates(statement);
    crate::trino_sql_checks::fold_quoted_columns(statement);
    crate::literals::rewrite_zoned_timestamps(statement);
    crate::literals::rewrite_exponent_numbers(statement);
    crate::coercion::mark_values(statement);
    crate::coercion::default_decimal_precision(statement);
    crate::declared::rewrite_declared_casts(statement);
    crate::literals::rewrite_timestamp_types(statement, session_zone);
}

/// Rewrites SQL text the parser cannot read into the spelling it reads, with
/// Trino's meaning: `VALUES 1, 2` (rows without parentheses) becomes
/// `VALUES (1), (2)` (a row without parentheses is one value). Applied
/// before parsing to every statement the engine plans, and so to the SQL of
/// a view a connector plans itself.
pub fn trino_text(sql: &str) -> std::borrow::Cow<'_, str> {
    crate::trino_sql_checks::values_rows(sql)
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
        session = crate::runtime::sort_settings(session, memory);
        // `1.5` is `decimal(2,1)`, as in Trino (and PostgreSQL), not a double.
        session.options_mut().sql_parser.parse_float_as_decimal = true;
        // NULL sorts last in both directions in Trino (`DataFusion`, like
        // PostgreSQL: first when descending), window ordering included.
        session.options_mut().sql_parser.default_null_ordering = "nulls_last".into();
        // Trino's default session zone is the server's; `CodeLake` runs in UTC.
        session.options_mut().execution.time_zone = Some("UTC".into());
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
        ctx.register_udf(crate::coercion::values_cell());
        ctx.register_udf(crate::timestamp_casts::try_arrow_cast());
        ctx.register_udf(crate::declared::trino_typed());
        ctx.register_udf(crate::coercion::named_cast());
        crate::trino_functions::register(&ctx);
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
        let ctx = self.session(options)?;
        // Planning futures are large; boxing keeps callers' futures small.
        Box::pin(self.plan_and_run(&ctx, &options.user, sql)).await
    }

    fn session(&self, options: &QueryOptions) -> Result<SessionContext, DataFusionError> {
        let mut state = self.ctx.state();
        let defaults = &mut state.config_mut().options_mut().catalog;
        if let Some(catalog) = &options.catalog {
            defaults.default_catalog.clone_from(catalog);
        }
        if let Some(schema) = &options.schema {
            defaults.default_schema.clone_from(schema);
        }
        if let Some(zone) = &options.time_zone {
            state.config_mut().options_mut().execution.time_zone = Some(zone.clone());
        }
        for function in &options.functions {
            // Replacing a built-in is the point; the previous one is not needed.
            state.register_udf(Arc::clone(function))?;
        }
        Ok(SessionContext::new_with_state(state))
    }

    async fn plan_and_run(
        &self,
        ctx: &SessionContext,
        user: &str,
        sql: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let (sql, security) = match ddl::rewrite(sql)? {
            Some(Rewrite::RenameView { from, to }) => {
                return self.rename(ctx, RelationKind::View, &from, &to, false).await;
            }
            Some(Rewrite::ViewSecurity { sql, security }) => (std::borrow::Cow::Owned(sql), security),
            None => (std::borrow::Cow::Borrowed(sql), ViewSecurity::default()),
        };
        let sql = crate::declared::trino_type_syntax(&sql);
        let sql = trino_text(&sql);
        let mut statement = state.sql_to_statement(&sql, &dialect)?;
        crate::trino_sql_checks::check_statement(&statement)?;
        let prepared = ddl::prepare(&mut statement, security)?;
        // After `prepare`, which keeps a view's text as written.
        let declared = crate::declared::declared_columns(&mut statement);
        let maps = crate::declared::map_columns(&mut statement);
        let session_zone = state.config().options().execution.time_zone.clone();
        trino_statement(&mut statement, session_zone.as_deref().unwrap_or("UTC"));
        match &prepared {
            Some(Prepared::AddColumn {
                table,
                column,
                sql_type,
            }) => return self.add_column(ctx, table, column, sql_type).await,
            Some(Prepared::Rename {
                kind,
                from,
                to,
                if_exists,
            }) => return self.rename(ctx, *kind, from, to, *if_exists).await,
            Some(Prepared::View { .. }) | None => {}
        }
        let plan = state.statement_to_plan(statement).await?;
        if ddl::is_routable(&plan)
            && let Some(result) = self
                .route_ddl(
                    ctx,
                    user,
                    &plan,
                    prepared,
                    &NewColumns {
                        declared,
                        types: Self::map_types(ctx, maps).await?,
                    },
                )
                .await?
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
        columns: &NewColumns,
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
        // The executed plan's schema: the analyzer settles some types (Trino's
        // decimal types, see `crate::overflow`) after SQL planning.
        // Columns of `CREATE TABLE AS` are nullable in Trino, whatever the query.
        let from_query = !matches!(input, LogicalPlan::EmptyRelation(_));
        let physical = DataFrame::new(ctx.state(), input).create_physical_plan().await?;
        let schema = lower_case_columns(&table_schema(&physical.schema(), columns, from_query))?;
        let batches = datafusion::physical_plan::collect_partitioned(physical, ctx.task_ctx()).await?;
        // Rows under the table's (lower-case) column names.
        let batches = batches
            .into_iter()
            .map(|partition| {
                partition
                    .into_iter()
                    .map(|batch| {
                        let options = datafusion::arrow::array::RecordBatchOptions::new()
                            .with_row_count(Some(batch.num_rows()));
                        datafusion::arrow::array::RecordBatch::try_new_with_options(
                            Arc::clone(&schema),
                            batch.columns().to_vec(),
                            &options,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = batches
            .iter()
            .flatten()
            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .sum();
        let table = MemTable::try_new(schema, batches)?
            .with_constraints(create.constraints.clone())
            .with_column_defaults(
                create
                    .column_defaults
                    .iter()
                    .map(|(name, default)| (name.to_lowercase(), default.clone()))
                    .collect(),
            );
        if exists {
            ctx.deregister_table(create.name.clone())?;
        }
        ctx.register_table(create.name.clone(), Arc::new(table))?;
        Self::row_count(ctx, rows)
    }

    /// `CREATE VIEW` in `DataFusion`'s in-memory catalogs, as `DataFusion`
    /// does it, but with the column types its query has once analyzed (Trino's
    /// decimal types: `dc * 2` of a `decimal(12,3)` is `decimal(22,3)`, not
    /// `DataFusion`'s `decimal(33,3)`) and its query as written for
    /// `information_schema.views`.
    fn create_memory_view(
        ctx: &SessionContext,
        view: &datafusion::logical_expr::CreateView,
        sql: String,
    ) -> Result<DataFrame, DataFusionError> {
        use datafusion::catalog::view::ViewTable;
        use datafusion::logical_expr::{Cast, Expr, LogicalPlanBuilder};
        if view.temporary {
            return Err(DataFusionError::NotImplemented(
                "Temporary views not supported".into(),
            ));
        }
        let analyzed = analyzed_schema(&ctx.state(), &view.input)?;
        let input = view.input.as_ref().clone();
        let columns: Vec<Expr> = input
            .schema()
            .iter()
            .zip(analyzed.fields())
            .map(|((qualifier, field), typed)| {
                let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
                let column = if field.data_type() == typed.data_type() {
                    column
                } else {
                    Expr::Cast(Cast::new(Box::new(column), typed.data_type().clone()))
                };
                // Lower case, as the columns of a table (see `lower_case_columns`).
                column.alias(field.name().to_lowercase())
            })
            .collect();
        let names: Vec<String> = analyzed
            .fields()
            .iter()
            .map(|f| f.name().to_lowercase())
            .collect();
        if let Some(name) = duplicate(&names) {
            return Err(duplicate_column(name));
        }
        let plan = LogicalPlanBuilder::from(input).project(columns)?.build()?;
        let exists = ctx.table_exist(view.name.clone())?;
        if exists && !view.or_replace {
            return Err(DataFusionError::Execution(format!(
                "Table '{}' already exists",
                view.name
            )));
        }
        if exists {
            ctx.deregister_table(view.name.clone())?;
        }
        ctx.register_table(view.name.clone(), Arc::new(ViewTable::new(plan, Some(sql))))?;
        Self::empty(ctx)
    }

    /// The Arrow type of each `map(K, V)` column (see
    /// [`crate::declared::map_columns`]).
    async fn map_types(
        ctx: &SessionContext,
        maps: Vec<(
            String,
            datafusion::sql::sqlparser::ast::DataType,
            datafusion::sql::sqlparser::ast::DataType,
        )>,
    ) -> Result<Vec<(String, datafusion::arrow::datatypes::DataType)>, DataFusionError> {
        use datafusion::arrow::datatypes::{DataType, Field, Fields};
        let mut types = Vec::with_capacity(maps.len());
        for (name, key, value) in maps {
            let state = ctx.state();
            let dialect = state.config().options().sql_parser.dialect;
            let mut probe = state.sql_to_statement(
                &format!("SELECT CAST(NULL AS {key}) AS k, CAST(NULL AS {value}) AS v"),
                &dialect,
            )?;
            trino_statement(&mut probe, "UTC");
            let probe = state.statement_to_plan(probe).await?;
            let entries = Fields::from(vec![
                Field::new("key", probe.schema().field(0).data_type().clone(), false),
                Field::new("value", probe.schema().field(1).data_type().clone(), true),
            ]);
            let map = DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(entries), false)),
                false,
            );
            types.push((name, map));
        }
        Ok(types)
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
        // Let the SQL planner translate the type name exactly as it would in a
        // cast, with Trino's defaults (`timestamp` is `timestamp(3)`).
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let mut probe = state.sql_to_statement(&format!("SELECT CAST(NULL AS {sql_type}) AS t"), &dialect)?;
        crate::literals::rewrite_timestamp_types(&mut probe, "UTC");
        let probe = state.statement_to_plan(probe).await?;
        let data_type = probe.schema().field(0).data_type().clone();
        writable.add_column(&schema, &name, column, &data_type).await?;
        Self::empty(ctx)
    }

    /// `ALTER TABLE … RENAME TO` and `ALTER VIEW … RENAME TO`. The target is
    /// resolved against the session defaults, as in Trino, and must be in the
    /// same catalog. A writable catalog renames in its own storage; any other
    /// catalog is renamed through its schema providers (the in-memory catalog).
    async fn rename(
        &self,
        ctx: &SessionContext,
        kind: RelationKind,
        from: &TableReference,
        to: &TableReference,
        if_exists: bool,
    ) -> Result<DataFrame, DataFusionError> {
        let (catalog, schema, name) = Self::resolve(ctx, from);
        let (target_catalog, target_schema, target_name) = Self::resolve(ctx, to);
        let noun = kind.noun();
        let Some(provider) = ctx.catalog(&catalog) else {
            return Err(DataFusionError::Plan(format!(
                "Catalog '{catalog}' does not exist"
            )));
        };
        let source = match provider.schema(&schema) {
            Some(source) => source.table(&name).await?,
            None => None,
        };
        let Some(table) = source else {
            if if_exists {
                return Self::empty(ctx);
            }
            return Err(DataFusionError::Plan(format!(
                "{noun} '{catalog}.{schema}.{name}' does not exist"
            )));
        };
        let is_view = table.table_type() == datafusion::datasource::TableType::View;
        match (kind, is_view) {
            (RelationKind::Table, true) => {
                return Err(DataFusionError::Plan(format!(
                    "Table '{catalog}.{schema}.{name}' does not exist, but a view with that name exists. \
                     Did you mean ALTER VIEW {catalog}.{schema}.{name} RENAME TO ...?"
                )));
            }
            (RelationKind::View, false) => {
                return Err(DataFusionError::Plan(format!(
                    "View '{catalog}.{schema}.{name}' does not exist, but a table with that name exists. \
                     Did you mean ALTER TABLE {catalog}.{schema}.{name} RENAME TO ...?"
                )));
            }
            _ => {}
        }
        if ctx.catalog(&target_catalog).is_none() {
            return Err(DataFusionError::Plan(format!(
                "Target catalog '{target_catalog}' does not exist"
            )));
        }
        let Some(target) = provider
            .schema(&target_schema)
            .filter(|_| target_catalog == catalog)
        else {
            if target_catalog != catalog {
                return Err(DataFusionError::NotImplemented(format!(
                    "{noun} rename across catalogs is not supported"
                )));
            }
            return Err(DataFusionError::Plan(format!(
                "Target schema '{target_catalog}.{target_schema}' does not exist"
            )));
        };
        if target.table_exist(&target_name) {
            return Err(DataFusionError::Plan(format!(
                "Target {} '{target_catalog}.{target_schema}.{target_name}' already exists",
                noun.to_lowercase()
            )));
        }
        if let Some(writable) = self.writable(&catalog) {
            match kind {
                RelationKind::Table => {
                    writable
                        .rename_table(&schema, &name, &target_schema, &target_name)
                        .await?;
                }
                RelationKind::View => {
                    writable
                        .rename_view(&schema, &name, &target_schema, &target_name)
                        .await?;
                }
            }
            return Self::empty(ctx);
        }
        let Some(source) = provider.schema(&schema) else {
            return Err(DataFusionError::Plan(format!(
                "Schema '{catalog}.{schema}' does not exist"
            )));
        };
        let Some(removed) = source.deregister_table(&name)? else {
            return Err(DataFusionError::Plan(format!(
                "{noun} '{catalog}.{schema}.{name}' does not exist"
            )));
        };
        if let Err(error) = target.register_table(target_name, Arc::clone(&removed)) {
            // Put the source back: a failed rename leaves the relation as it was.
            source.register_table(name, removed)?;
            return Err(error);
        }
        Self::empty(ctx)
    }

    async fn route_ddl(
        &self,
        ctx: &SessionContext,
        user: &str,
        plan: &LogicalPlan,
        prepared: Option<Prepared>,
        columns: &NewColumns,
    ) -> Result<Option<DataFrame>, DataFusionError> {
        let LogicalPlan::Ddl(ddl) = plan else {
            return Ok(None);
        };
        match ddl {
            DdlStatement::CreateMemoryTable(create) => {
                let (catalog, schema, table) = Self::resolve(ctx, &create.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Self::create_memory_table(ctx, create, columns).await.map(Some);
                };
                if create.or_replace {
                    writable.drop_table(&schema, &table, true).await?;
                }
                let input =
                    crate::subquery::pre_evaluate_scalar_subqueries(ctx, create.input.as_ref().clone())
                        .await?;
                let from_query = !matches!(input, LogicalPlan::EmptyRelation(_));
                let rows = ctx.execute_logical_plan(input).await?.execute_stream().await?;
                let announced = table_schema(&rows.schema(), columns, from_query);
                let rows = with_schema(rows, announced);
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
                    let Some(Prepared::View { sql, .. }) = prepared else {
                        return Ok(None);
                    };
                    return Self::create_memory_view(ctx, view, sql).map(Some);
                };
                let Some(Prepared::View { sql, security }) = prepared else {
                    return Err(DataFusionError::Internal("view SQL was not captured".into()));
                };
                let session = ctx.state().config().options().catalog.clone();
                let output = analyzed_schema(&ctx.state(), &view.input)?;
                let definition = ViewDefinition {
                    sql,
                    security,
                    output: Arc::new(output.as_arrow().clone()),
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

    /// Attached catalogs and the connector family serving each.
    pub fn connectors(&self) -> Vec<(String, &'static str)> {
        self.sources
            .iter()
            .map(|(source, _)| (source.name().to_owned(), source.kind()))
            .collect()
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
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
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
        // Without a zone: `timestamp(3)`, the precision of its digits.
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    /// Result types and values as text (Arrow's display: decimals exact).
    async fn typed_rows(engine: &Engine, sql: &str) -> (Vec<DataType>, Vec<Vec<String>>) {
        use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
        let frame = engine.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let expected = analyzed_schema(&engine.context().state(), frame.logical_plan()).unwrap();
        let batches = frame.collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let types: Vec<DataType> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let analyzed: Vec<DataType> = expected.fields().iter().map(|f| f.data_type().clone()).collect();
        assert_eq!(types, analyzed, "{sql}: analyzed schema and rows differ");
        let mut rows = Vec::new();
        for batch in &batches {
            let options = FormatOptions::default().with_null("NULL");
            let formatters: Vec<ArrayFormatter<'_>> = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                rows.push(formatters.iter().map(|f| f.value(row).to_string()).collect());
            }
        }
        (types, rows)
    }

    /// Each query's first value, with its type.
    async fn assert_first_values(engine: &Engine, cases: &[(String, DataType, &str)]) {
        for (sql, data_type, value) in cases {
            let (types, rows) = typed_rows(engine, sql).await;
            assert_eq!((&types[0], rows[0][0].as_str()), (data_type, *value), "{sql}");
        }
    }

    /// Each query fails with an error containing its message.
    async fn assert_failures(engine: &Engine, cases: &[(&str, &str)]) {
        for (sql, message) in cases {
            let result = match engine.sql(sql).await {
                Ok(frame) => frame.collect().await.map(|_| ()),
                Err(e) => Err(e),
            };
            let error = result.expect_err(sql).to_string();
            assert!(error.contains(message), "{sql}: {error}");
        }
    }

    const LINEITEM: &str = "(VALUES \
        (1, CAST(17 AS decimal(15,2)), CAST(21168.23 AS decimal(15,2)), CAST(0.04 AS decimal(15,2)), CAST(0.02 AS decimal(15,2))), \
        (1, CAST(36 AS decimal(15,2)), CAST(45983.16 AS decimal(15,2)), CAST(0.09 AS decimal(15,2)), CAST(0.06 AS decimal(15,2))), \
        (2, CAST(8 AS decimal(15,2)), CAST(13309.60 AS decimal(15,2)), CAST(0.10 AS decimal(15,2)), CAST(0.02 AS decimal(15,2))), \
        (2, CAST(28 AS decimal(15,2)), CAST(28955.64 AS decimal(15,2)), CAST(0.09 AS decimal(15,2)), CAST(0.06 AS decimal(15,2))), \
        (2, CAST(24 AS decimal(15,2)), CAST(22824.48 AS decimal(15,2)), CAST(0.10 AS decimal(15,2)), CAST(0.04 AS decimal(15,2))), \
        (3, CAST(32 AS decimal(15,2)), CAST(49620.16 AS decimal(15,2)), CAST(0.07 AS decimal(15,2)), CAST(0.02 AS decimal(15,2))), \
        (3, CAST(38 AS decimal(15,2)), CAST(44694.46 AS decimal(15,2)), CAST(0.00 AS decimal(15,2)), CAST(0.05 AS decimal(15,2))) \
        ) l(k, qty, price, disc, tax)";

    /// TPC-H Q01, Q08, Q14 and Q17 shapes; every type and value observed on
    /// Trino 431 with the same rows.
    #[tokio::test]
    async fn decimal_aggregates_and_division_match_trino() {
        use DataType::Decimal128 as D;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let (types, rows) = typed_rows(
            &engine,
            &format!(
                "SELECT k, sum(qty), sum(price), sum(price * (1 - disc)), sum(price * (1 - disc) * (1 + tax)), \
                 avg(qty), avg(price), avg(disc) FROM {LINEITEM} GROUP BY k ORDER BY k"
            ),
        )
        .await;
        assert_eq!(
            types[1..],
            [
                D(38, 2),
                D(38, 2),
                D(38, 4),
                D(38, 6),
                D(15, 2),
                D(15, 2),
                D(15, 2)
            ]
        );
        assert_eq!(
            rows,
            [
                [
                    "1",
                    "53.00",
                    "67151.39",
                    "62166.1764",
                    "65083.286952",
                    "26.50",
                    "33575.70",
                    "0.07"
                ],
                [
                    "2",
                    "60.00",
                    "65089.72",
                    "58870.3044",
                    "61512.536424",
                    "20.00",
                    "21696.57",
                    "0.10"
                ],
                [
                    "3",
                    "70.00",
                    "94314.62",
                    "90841.2088",
                    "93998.866776",
                    "35.00",
                    "47157.31",
                    "0.04"
                ],
            ]
        );
        let cases = [
            (
                format!(
                    "SELECT 100.00 * sum(CASE WHEN k = 2 THEN price * (1 - disc) ELSE 0 END) \
                     / sum(price * (1 - disc)) FROM {LINEITEM}"
                ),
                D(38, 6),
                "27.785042",
            ),
            (
                format!(
                    "SELECT sum(CASE WHEN k = 3 THEN price * (1 - disc) ELSE 0 END) / sum(price * (1 - disc)) \
                     FROM {LINEITEM}"
                ),
                D(38, 4),
                "0.4287",
            ),
            (
                format!("SELECT sum(price) / 7.0 FROM {LINEITEM} WHERE qty < 0.2 * 150"),
                D(38, 2),
                "12322.56",
            ),
            (
                format!("SELECT avg(DISTINCT disc) FROM {LINEITEM}"),
                D(15, 2),
                "0.06",
            ),
        ];
        assert_first_values(&engine, &cases).await;
    }

    /// Trino 431: a sliding window `avg` over decimals rounds like `avg`.
    #[tokio::test]
    async fn decimal_window_average_matches_trino() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let (types, rows) = typed_rows(
            &engine,
            &format!(
                "SELECT avg(price) OVER (ORDER BY price ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
                 FROM {LINEITEM} ORDER BY price"
            ),
        )
        .await;
        assert_eq!(types, [DataType::Decimal128(15, 2)]);
        let window: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
        assert_eq!(
            window,
            [
                "13309.60", "17238.92", "21996.36", "25890.06", "36825.05", "45338.81", "47801.66"
            ]
        );
    }

    /// Trino 431: `1.5` is `decimal(2,1)`, `1e5` a `double`.
    #[tokio::test]
    async fn decimal_literals_match_trino() {
        use DataType::Decimal128 as D;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let (types, rows) = typed_rows(
            &engine,
            "SELECT 1.5, 100.00, 007.50, 0.2, .5, 0.00, -1.5, 1e5, 2.5E-1",
        )
        .await;
        assert_eq!(
            types,
            [
                D(2, 1),
                D(5, 2),
                D(3, 2),
                D(1, 1),
                D(1, 1),
                D(2, 2),
                D(2, 1),
                DataType::Float64,
                DataType::Float64
            ]
        );
        assert_eq!(rows[0][7..], ["100000.0", "0.25"]);
    }

    /// Trino 431: decimal division rounds half away from zero.
    #[tokio::test]
    async fn decimal_division_rounds_like_trino() {
        use DataType::Decimal128 as D;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let cases = [
            (
                "SELECT CAST(10 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0))",
                D(5, 0),
                "3",
            ),
            (
                "SELECT CAST(20 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0))",
                D(5, 0),
                "7",
            ),
            (
                "SELECT CAST(-20 AS DECIMAL(5,0)) / CAST(3 AS DECIMAL(5,0))",
                D(5, 0),
                "-7",
            ),
            (
                "SELECT CAST(-1 AS DECIMAL(5,1)) / CAST(4 AS DECIMAL(5,0))",
                D(5, 1),
                "-0.3",
            ),
            (
                "SELECT CAST(1 AS DECIMAL(5,2)) / CAST(8 AS DECIMAL(5,0))",
                D(5, 2),
                "0.13",
            ),
            (
                "SELECT CAST(-1 AS DECIMAL(5,2)) / CAST(8 AS DECIMAL(5,0))",
                D(5, 2),
                "-0.13",
            ),
            ("SELECT CAST(1.25 AS DECIMAL(5,2)) / 2", D(5, 2), "0.63"),
        ];
        let cases: Vec<(String, DataType, &str)> = cases
            .into_iter()
            .map(|(sql, t, v)| (sql.to_owned(), t, v))
            .collect();
        assert_first_values(&engine, &cases).await;
    }

    /// Trino 431: 38 digits, modulus, mixed integer operands and `avg`.
    #[tokio::test]
    async fn decimal_edges_match_trino() {
        use DataType::Decimal128 as D;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let cases = [
            (
                "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(3 AS DECIMAL(38,18))",
                D(38, 18),
                "0.333333333333333333",
            ),
            (
                "SELECT CAST(1 AS DECIMAL(38,1)) / CAST(3 AS DECIMAL(38,19))",
                D(38, 19),
                "0.3333333333333333333",
            ),
            (
                "SELECT CAST(7.5 AS DECIMAL(5,1)) % CAST(2 AS DECIMAL(5,0))",
                D(5, 1),
                "1.5",
            ),
            ("SELECT CAST(-7.5 AS DECIMAL(5,1)) % 2", D(5, 1), "-1.5"),
            (
                "SELECT CAST(1 AS DECIMAL(20,10)) * CAST(1 AS DECIMAL(20,10))",
                D(38, 20),
                "1.00000000000000000000",
            ),
            ("SELECT CAST(1 AS DECIMAL(12,2)) + 1", D(13, 2), "2.00"),
            (
                "SELECT CAST(1 AS DECIMAL(12,2)) * CAST(1 AS SMALLINT)",
                D(17, 2),
                "1.00",
            ),
            (
                "SELECT CAST(1 AS DECIMAL(12,2)) * CAST(1 AS TINYINT)",
                D(15, 2),
                "1.00",
            ),
            (
                "SELECT avg(x) FROM (VALUES (CAST(-1.25 AS DECIMAL(5,2))), (CAST(-1.26 AS DECIMAL(5,2)))) t(x)",
                D(5, 2),
                "-1.26",
            ),
            (
                "SELECT avg(x) FROM (VALUES (CAST(1 AS DECIMAL(3,0))), (CAST(2 AS DECIMAL(3,0)))) t(x)",
                D(3, 0),
                "2",
            ),
            (
                "SELECT avg(x) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), \
                 (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
                D(38, 0),
                "99999999999999999999999999999999999999",
            ),
        ];
        let cases: Vec<(String, DataType, &str)> = cases
            .into_iter()
            .map(|(sql, t, v)| (sql.to_owned(), t, v))
            .collect();
        assert_first_values(&engine, &cases).await;
    }

    /// Trino 431: `Decimal overflow` and division by zero.
    #[tokio::test]
    async fn decimal_failures_match_trino() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let cases = [
            (
                "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(3 AS DECIMAL(38,19))",
                "Decimal overflow",
            ),
            (
                "SELECT CAST(0 AS DECIMAL(38,0)) / CAST(3 AS DECIMAL(38,19))",
                "Decimal overflow",
            ),
            (
                "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + 1",
                "Decimal overflow",
            ),
            (
                "SELECT sum(x) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), \
                 (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
                "Decimal overflow",
            ),
            (
                "SELECT CAST(1 AS DECIMAL(5,0)) / CAST(0 AS DECIMAL(5,0))",
                "Divide by zero error",
            ),
        ];
        assert_failures(&engine, &cases).await;
    }

    /// Trino 431: `CAST(<text> AS TIMESTAMP)` keeps the local time and ignores
    /// the zone; `WITH TIME ZONE` keeps the zone and the precision.
    #[tokio::test]
    async fn text_to_timestamp_casts_match_trino() {
        use datafusion::arrow::array::{Array, AsArray};
        use datafusion::arrow::datatypes::{TimeUnit, TimestampMicrosecondType, TimestampMillisecondType};
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let ten_am_utc = 1_767_261_600_000_i64;
        let batches = engine
            .sql(
                "SELECT CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP) a, \
                 CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP(3) WITH TIME ZONE) b, \
                 CAST('2026-01-01 10:00:00.1235' AS TIMESTAMP) c, \
                 CAST('2026-01-01 10:00:00.123456 +02:00' AS TIMESTAMP(6) WITH TIME ZONE) d, \
                 CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE) e",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];
        let schema = batch.schema();
        let ms = |i: usize| {
            batch
                .column(i)
                .as_primitive::<TimestampMillisecondType>()
                .value(0)
        };
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(ms(0), ten_am_utc);
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("America/Sao_Paulo".into()))
        );
        assert_eq!(ms(1), ten_am_utc + 3 * 3_600_000);
        assert_eq!(ms(2), ten_am_utc + 124);
        assert_eq!(
            schema.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+02:00".into()))
        );
        assert_eq!(
            batch
                .column(3)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            (ten_am_utc - 2 * 3_600_000) * 1000 + 123_456
        );
        assert_eq!(
            schema.field(4).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(ms(4), ten_am_utc);
        // Column values: the zone of each text is ignored without time zone.
        let batches = engine
            .sql(
                "SELECT CAST(x AS TIMESTAMP) FROM (VALUES ('2026-01-01 10:00:00 America/Sao_Paulo'), \
                 ('2026-01-01 10:00:00.5 +02:00'), (NULL)) t(x)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let column = batches[0].column(0).as_primitive::<TimestampMillisecondType>();
        assert_eq!(
            (column.value(0), column.value(1), column.is_null(2)),
            (ten_am_utc, ten_am_utc + 500, true)
        );
        let message = "Value cannot be cast to timestamp";
        let cases = [
            ("SELECT CAST('2026-01-01T10:00:00' AS TIMESTAMP)", message),
            (
                "SELECT CAST('2026-01-01 10:00:00 Mars/Olympus' AS TIMESTAMP)",
                message,
            ),
            (
                "SELECT CAST(x AS TIMESTAMP) FROM (VALUES ('garbage')) t(x)",
                message,
            ),
        ];
        assert_failures(&engine, &cases).await;
        let (_, rows) = typed_rows(&engine, "SELECT TRY_CAST('garbage' AS TIMESTAMP)").await;
        assert_eq!(rows, [["NULL"]]);
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
            // `public` and, as in Trino, `information_schema` itself.
            (
                "SELECT count(*) FROM erp.information_schema.schemata WHERE catalog_name = 'erp'",
                2,
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

    async fn run(engine: &Engine, sql: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
        engine.sql(sql).await?.collect().await
    }

    fn message(result: Result<Vec<RecordBatch>, DataFusionError>) -> String {
        result.unwrap_err().to_string()
    }

    #[tokio::test]
    async fn rename_in_the_memory_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        for sql in [
            "CREATE SCHEMA datafusion.s",
            "CREATE SCHEMA datafusion.u",
            "CREATE TABLE datafusion.s.t AS SELECT 1 AS a",
            "CREATE VIEW datafusion.s.v AS SELECT 2 AS b",
            "ALTER TABLE datafusion.s.t RENAME TO datafusion.u.t2",
            "ALTER VIEW s.v RENAME TO s.w",
            "ALTER TABLE IF EXISTS datafusion.s.missing RENAME TO datafusion.s.x",
        ] {
            run(&engine, sql).await.unwrap();
        }
        let rows = run(&engine, "SELECT a FROM datafusion.u.t2").await.unwrap();
        assert_eq!(rows[0].num_rows(), 1);
        run(&engine, "SELECT b FROM datafusion.s.w").await.unwrap();
        assert!(run(&engine, "SELECT a FROM datafusion.s.t").await.is_err());
        assert!(run(&engine, "SELECT b FROM datafusion.s.v").await.is_err());
        assert!(run(&engine, "SELECT * FROM datafusion.s.x").await.is_err());

        for (sql, expected) in [
            (
                "ALTER TABLE s.missing RENAME TO s.x",
                "Table 'datafusion.s.missing' does not exist",
            ),
            (
                "ALTER VIEW s.missing RENAME TO s.x",
                "View 'datafusion.s.missing' does not exist",
            ),
            (
                "ALTER TABLE s.w RENAME TO s.x",
                "but a view with that name exists",
            ),
            (
                "ALTER VIEW u.t2 RENAME TO u.x",
                "but a table with that name exists",
            ),
            (
                "ALTER TABLE u.t2 RENAME TO s.w",
                "Target table 'datafusion.s.w' already exists",
            ),
            (
                "ALTER VIEW s.w RENAME TO u.t2",
                "Target view 'datafusion.u.t2' already exists",
            ),
            (
                "ALTER TABLE u.t2 RENAME TO nope.x",
                "Target schema 'datafusion.nope' does not exist",
            ),
            (
                "ALTER TABLE u.t2 RENAME TO erp.public.x",
                "Target catalog 'erp' does not exist",
            ),
            (
                "ALTER TABLE nocat.s.t RENAME TO nocat.s.x",
                "Catalog 'nocat' does not exist",
            ),
        ] {
            let error = message(run(&engine, sql).await);
            assert!(error.contains(expected), "{sql}: {error}");
        }
    }

    #[derive(Debug, Default)]
    struct RecordingCatalog {
        calls: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingCatalog {
        fn record(&self, call: String) {
            if let Ok(mut calls) = self.calls.lock() {
                calls.push(call);
            }
        }
    }

    #[async_trait]
    impl WritableCatalog for RecordingCatalog {
        async fn create_schema(&self, schema: &str, _: bool, _: &str) -> datafusion::error::Result<()> {
            self.record(format!("create_schema {schema}"));
            Ok(())
        }
        async fn create_table_as(
            &self,
            _: &str,
            _: &str,
            _: bool,
            _: &str,
            _: datafusion::physical_plan::SendableRecordBatchStream,
            _: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<u64> {
            Ok(0)
        }
        async fn drop_table(&self, _: &str, _: &str, _: bool) -> datafusion::error::Result<()> {
            Ok(())
        }
        async fn create_view(
            &self,
            schema: &str,
            view: &str,
            _: bool,
            owner: &str,
            definition: ViewDefinition,
        ) -> datafusion::error::Result<()> {
            self.record(format!(
                "create_view {schema}.{view} {owner} {:?} {}",
                definition.security, definition.sql
            ));
            Ok(())
        }
        async fn drop_view(&self, _: &str, _: &str, _: bool) -> datafusion::error::Result<()> {
            Ok(())
        }
        async fn add_column(&self, _: &str, _: &str, _: &str, _: &DataType) -> datafusion::error::Result<()> {
            Ok(())
        }
        async fn rename_table(
            &self,
            schema: &str,
            table: &str,
            to_schema: &str,
            to: &str,
        ) -> datafusion::error::Result<()> {
            self.record(format!("rename_table {schema}.{table} {to_schema}.{to}"));
            Ok(())
        }
        async fn rename_view(
            &self,
            schema: &str,
            view: &str,
            to_schema: &str,
            to: &str,
        ) -> datafusion::error::Result<()> {
            self.record(format!("rename_view {schema}.{view} {to_schema}.{to}"));
            Ok(())
        }
    }

    #[derive(Debug)]
    struct WritableSource(Arc<RecordingCatalog>);

    #[async_trait]
    impl Connector for WritableSource {
        fn name(&self) -> &'static str {
            "lake"
        }
        fn kind(&self) -> &'static str {
            "fake"
        }
        async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
            let schema = MemorySchemaProvider::new();
            let table = MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)])),
                vec![vec![]],
            )
            .unwrap();
            schema.register_table("t".into(), Arc::new(table)).unwrap();
            let view = datafusion::catalog::view::ViewTable::new(
                LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: true,
                    schema: Arc::new(datafusion::common::DFSchema::empty()),
                }),
                None,
            );
            schema.register_table("v".into(), Arc::new(view)).unwrap();
            let catalog = MemoryCatalogProvider::new();
            catalog.register_schema("raw", Arc::new(schema)).unwrap();
            catalog
                .register_schema("mart", Arc::new(MemorySchemaProvider::new()))
                .unwrap();
            Ok(Arc::new(catalog))
        }
        async fn ping(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn writable(&self) -> Option<Arc<dyn WritableCatalog>> {
            Some(Arc::clone(&self.0) as Arc<dyn WritableCatalog>)
        }
    }

    #[tokio::test]
    async fn writable_catalogs_rename_and_store_view_security() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let recorder = Arc::new(RecordingCatalog::default());
        engine
            .attach(Arc::new(WritableSource(Arc::clone(&recorder))), true, FAST)
            .await
            .unwrap();
        let options = QueryOptions {
            catalog: Some("lake".into()),
            schema: Some("raw".into()),
            ..QueryOptions::for_user("dbt")
        };
        for sql in [
            "ALTER TABLE t RENAME TO mart.t2",
            "ALTER VIEW lake.raw.v RENAME TO v2",
            "CREATE VIEW mart.w SECURITY INVOKER AS SELECT 1 AS x",
            "CREATE OR REPLACE VIEW mart.d security definer AS SELECT 2 AS x",
            "CREATE VIEW mart.plain AS SELECT 3 AS x",
        ] {
            engine
                .sql_with(&options, sql)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
        }
        assert_eq!(
            *recorder.calls.lock().unwrap(),
            [
                "rename_table raw.t mart.t2",
                "rename_view raw.v raw.v2",
                "create_view mart.w dbt Invoker SELECT 1 AS x",
                "create_view mart.d dbt Definer SELECT 2 AS x",
                "create_view mart.plain dbt Definer SELECT 3 AS x",
            ]
        );
        let error = engine
            .sql_with(&options, "ALTER TABLE t RENAME TO datafusion.public.t")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("rename across catalogs"), "{error}");
    }

    #[tokio::test]
    async fn protocol_functions_replace_built_ins_for_one_query() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let version = datafusion::logical_expr::create_udf(
            "version",
            Vec::new(),
            DataType::Utf8,
            datafusion::logical_expr::Volatility::Immutable,
            Arc::new(|_| {
                Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                    datafusion::common::ScalarValue::Utf8(Some("431".into())),
                ))
            }),
        );
        let options = QueryOptions {
            functions: vec![Arc::new(version)],
            ..QueryOptions::for_user("u")
        };
        let text = |batches: Vec<RecordBatch>| {
            datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string()
        };
        let emulated = engine.sql_with(&options, "SELECT version()").await.unwrap();
        assert!(text(emulated.collect().await.unwrap()).contains("| 431 "));
        let native = run(&engine, "SELECT version()").await.unwrap();
        assert!(text(native).contains("DataFusion"));
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
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
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
