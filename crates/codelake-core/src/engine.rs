//! Query engine: one `DataFusion` session per node, federated across sources and
//! bounded by [`EngineRuntime`].

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, visit_expressions};
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use datafusion_federation::FederatedQueryPlanner;
use futures::TryStreamExt;
use futures::future::join_all;
use serde::Serialize;
use tracing::field::Empty;
use tracing::{Instrument, Span, info, warn};

use crate::config::{ExecutionConfig, MemoryConfig};
use crate::connector::{Connector, ConnectorError, ViewDefinition, WritableCatalog};
use crate::ddl::{self, Prepared, TableExecute};
use crate::federation_guard::FederationFilterGuard;
use crate::information_schema::CatalogList;
use crate::runtime::{EngineRuntime, MemoryStats};
use crate::session_properties::{PropertyRegistry, SessionProperties};
use crate::table_procedure::{self, ArgumentValue, RawArgument};
use crate::trace::{self, QueryTrace, TARGET, TracedStream};

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
    /// Distributed-trace context: remote parent, query id, correlation id.
    /// Default: a new trace (subject to sampling) and an engine query id.
    pub trace: QueryTrace,
}

impl QueryOptions {
    pub fn for_user(user: &str) -> Self {
        Self {
            user: user.to_owned(),
            catalog: None,
            schema: None,
            properties: SessionProperties::default(),
            trace: QueryTrace::default(),
        }
    }

    /// Sets the trace context, e.g. from [`QueryTrace::from_headers`].
    #[must_use]
    pub fn with_trace(mut self, trace: QueryTrace) -> Self {
        self.trace = trace;
        self
    }
}

/// A planned statement with its trace. The query span stays open until the
/// results are fully fetched (or the stream is dropped), so a trace shows the
/// whole life of the query: `parse`, `plan`, `execute`, `fetch`, and the
/// source spans below them.
#[derive(Debug)]
pub struct Query {
    dataframe: DataFrame,
    span: Span,
    id: String,
}

impl Query {
    /// Engine- or protocol-assigned query id (Trino shape).
    pub fn query_id(&self) -> &str {
        &self.id
    }

    /// Trace id of this query, when it is being traced.
    pub fn trace_id(&self) -> Option<String> {
        trace::trace_id(&self.span)
    }

    /// The query span, for protocols that add their own children.
    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn dataframe(&self) -> &DataFrame {
        &self.dataframe
    }

    /// Drops the trace: the query span ends here and execution is not traced.
    pub fn into_dataframe(self) -> DataFrame {
        self.dataframe
    }

    /// Plans physically and starts execution (`execute` span), then returns a
    /// stream whose polling is the `fetch` span. Nothing is buffered.
    pub async fn execute_stream(self) -> Result<SendableRecordBatchStream, DataFusionError> {
        let execute = tracing::info_span!(
            target: TARGET,
            parent: &self.span,
            "codelake.execute",
            otel.status_code = Empty,
            otel.status_description = Empty,
        );
        let started = self.dataframe.execute_stream().instrument(execute.clone()).await;
        let stream = match started {
            Ok(stream) => stream,
            Err(err) => {
                trace::record_error(&execute, &err);
                trace::record_error(&self.span, &err);
                return Err(err);
            }
        };
        drop(execute);
        let fetch = tracing::info_span!(
            target: TARGET,
            parent: &self.span,
            "codelake.fetch",
            otel.status_code = Empty,
            otel.status_description = Empty,
            db.response.returned_rows = Empty,
            codelake.cancelled = Empty,
        );
        Ok(Box::pin(TracedStream::new(stream, fetch).keep_alive(self.span)))
    }

    /// [`Query::execute_stream`] collected into memory.
    pub async fn collect(self) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.execute_stream().await?.try_collect().await
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

/// A constant as Trino prints it in messages: the SQL literal of its type
/// (`DECIMAL '1.5'`, `DATE '2020-01-01'`, `1.5E0` for a DOUBLE).
fn trino_literal(value: &ScalarValue) -> String {
    let text = value
        .to_array()
        .ok()
        .and_then(|array| datafusion::arrow::util::display::array_value_to_string(&array, 0).ok())
        .unwrap_or_else(|| value.to_string());
    match value {
        ScalarValue::Decimal32(..)
        | ScalarValue::Decimal64(..)
        | ScalarValue::Decimal128(..)
        | ScalarValue::Decimal256(..) => format!("DECIMAL '{text}'"),
        ScalarValue::Date32(_) | ScalarValue::Date64(_) => format!("DATE '{text}'"),
        ScalarValue::TimestampSecond(..)
        | ScalarValue::TimestampMillisecond(..)
        | ScalarValue::TimestampMicrosecond(..)
        | ScalarValue::TimestampNanosecond(..) => format!("TIMESTAMP '{}'", text.replace('T', " ")),
        ScalarValue::Float64(Some(v)) => format!("{v:E}"),
        ScalarValue::Float32(_) => format!("REAL '{text}'"),
        _ => text,
    }
}

/// Default `DataFusion` physical rules, preceded by the federation guard (it must
/// run before `FilterPushdown`; see [`crate::federation_guard`]).
fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = vec![Arc::new(FederationFilterGuard)];
    rules.extend(PhysicalOptimizer::new().rules);
    rules
}

/// A statement after parsing: `ALTER TABLE … EXECUTE`, which the SQL parser
/// does not know, or an ordinary statement with its DDL preparation.
enum Parsed {
    TableExecute(Box<TableExecute>),
    Statement(Box<datafusion::sql::parser::Statement>, Option<Prepared>),
}

/// Phases of a query, one child span each.
#[derive(Debug, Clone, Copy)]
enum Phase {
    Parse,
    Plan,
    Execute,
}

fn phase_span(phase: Phase) -> Span {
    // `tracing` span names must be literals.
    match phase {
        Phase::Parse => tracing::info_span!(
            target: TARGET,
            "codelake.parse",
            otel.status_code = Empty,
            otel.status_description = Empty
        ),
        Phase::Plan => tracing::info_span!(
            target: TARGET,
            "codelake.plan",
            otel.status_code = Empty,
            otel.status_description = Empty
        ),
        Phase::Execute => tracing::info_span!(
            target: TARGET,
            "codelake.execute",
            otel.status_code = Empty,
            otel.status_description = Empty
        ),
    }
}

/// Runs `future` in a new span for `phase`, recording its error.
async fn traced<T>(
    phase: Phase,
    future: impl Future<Output = Result<T, DataFusionError>>,
) -> Result<T, DataFusionError> {
    let span = phase_span(phase);
    future
        .instrument(span.clone())
        .await
        .inspect_err(|err| trace::record_error(&span, err))
}

/// Root span of one query. Its parent is the client's span when the client
/// sent a `traceparent`.
fn query_span(query_id: &str, options: &QueryOptions, sql: &str) -> Span {
    let span = tracing::info_span!(
        target: TARGET,
        "codelake.query",
        otel.kind = "server",
        otel.status_code = Empty,
        otel.status_description = Empty,
        codelake.query_id = query_id,
        codelake.trace_id = Empty,
        codelake.correlation_id = options.trace.correlation_id.as_deref(),
        enduser.id = options.user.as_str(),
        codelake.catalog = options.catalog.as_deref(),
        codelake.schema = options.schema.as_deref(),
        db.query.text = Empty,
        codelake.sql.hash = Empty,
        db.response.returned_rows = Empty,
        codelake.cancelled = Empty,
    );
    if let Some(parent) = &options.trace.parent {
        trace::set_remote_parent(&span, parent, options.trace.tracestate.as_deref());
    }
    // Logs carry the trace id even when spans are not exported.
    let trace_id = trace::trace_id(&span).or_else(|| options.trace.parent.map(|p| p.trace_id_hex()));
    if let Some(trace_id) = trace_id {
        span.record(trace::field::TRACE_ID, trace_id.as_str());
    }
    trace::record_sql(
        &span,
        sql,
        &datafusion::sql::sqlparser::dialect::GenericDialect {},
    );
    span
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
        trace::install_task_propagation();
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
    ///
    /// Only parsing and planning are traced; use [`Engine::query`] to trace
    /// execution and fetching too.
    pub async fn sql_with(&self, options: &QueryOptions, sql: &str) -> Result<DataFrame, DataFusionError> {
        self.query(options, sql).await.map(Query::into_dataframe)
    }

    /// Plans `sql` inside a new query span (see [`Query`]).
    ///
    /// The span is a child of `options.trace.parent` when the client sent a
    /// W3C `traceparent`, and carries the query id, user, default catalog and
    /// schema, correlation id and the SQL with its literals redacted.
    pub async fn query(&self, options: &QueryOptions, sql: &str) -> Result<Query, DataFusionError> {
        let query_id = options
            .trace
            .query_id
            .clone()
            .unwrap_or_else(trace::next_query_id);
        let span = query_span(&query_id, options, sql);
        let ctx = self.session(options);
        // Planning futures are large; boxing keeps callers' futures small.
        let planned = Box::pin(self.plan_and_run(&ctx, options, sql))
            .instrument(span.clone())
            .await;
        match planned {
            Ok(dataframe) => Ok(Query {
                dataframe,
                span,
                id: query_id,
            }),
            Err(err) => {
                trace::record_error(&span, &err);
                Err(err)
            }
        }
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
        options: &QueryOptions,
        sql: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let user = options.user.as_str();
        let state = ctx.state();
        let parse = phase_span(Phase::Parse);
        let parsed = parse
            .in_scope(|| {
                let dialect = state.config().options().sql_parser.dialect;
                // The parser's dialect is a `Box<dyn Dialect>` (not `Send`): it
                // must be gone before any `.await`, or callers cannot spawn
                // this future.
                if let Some(parser_dialect) = dialect_from_str(dialect)
                    && let Some(execute) = ddl::parse_table_execute(sql, parser_dialect.as_ref())?
                {
                    return Ok(Parsed::TableExecute(Box::new(execute)));
                }
                let mut statement = state.sql_to_statement(sql, &dialect)?;
                let prepared = ddl::prepare(&mut statement)?;
                // After `prepare`, which keeps a view's text as written.
                crate::literals::rewrite_zoned_timestamps(&mut statement);
                Ok::<_, DataFusionError>(Parsed::Statement(Box::new(statement), prepared))
            })
            .inspect_err(|err| trace::record_error(&parse, err))?;
        drop(parse);
        let (statement, prepared) = match parsed {
            Parsed::TableExecute(execute) => {
                return traced(Phase::Execute, self.table_execute(ctx, options, *execute)).await;
            }
            Parsed::Statement(statement, prepared) => (*statement, prepared),
        };
        if let Some(Prepared::AddColumn {
            table,
            column,
            sql_type,
        }) = &prepared
        {
            return traced(Phase::Execute, self.add_column(ctx, table, column, sql_type)).await;
        }
        let plan = traced(Phase::Plan, state.statement_to_plan(statement)).await?;
        if ddl::is_routable(&plan)
            && let Some(result) = traced(Phase::Execute, self.route_ddl(ctx, user, &plan, prepared)).await?
        {
            return Ok(result);
        }
        traced(Phase::Plan, async {
            // Uncorrelated scalar subqueries run here, against the sources.
            let plan = crate::subquery::pre_evaluate_scalar_subqueries(ctx, plan).await?;
            ctx.execute_logical_plan(plan).await
        })
        .await
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
        use datafusion::arrow::array::UInt64Array;
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

    /// `ALTER TABLE … EXECUTE <procedure>`: checks run in Trino's order (table,
    /// procedure, arguments, the catalog's session properties, filter) with
    /// Trino's messages, then the catalog runs
    /// the procedure. The answer is Trino's: an empty result with one `rows`
    /// column.
    async fn table_execute(
        &self,
        ctx: &SessionContext,
        options: &QueryOptions,
        execute: TableExecute,
    ) -> Result<DataFrame, DataFusionError> {
        let (catalog, schema, table) = Self::resolve(ctx, &execute.table);
        let schema_provider = ctx.catalog(&catalog).and_then(|c| c.schema(&schema));
        let provider = match schema_provider {
            Some(provider) => provider.table(&table).await?,
            None => None,
        }
        .ok_or_else(|| DataFusionError::Plan(format!("Table '{catalog}.{schema}.{table}' does not exist")))?;
        if provider.table_type() == TableType::View {
            return Err(DataFusionError::Plan(
                "ALTER TABLE EXECUTE is not supported for views".into(),
            ));
        }
        let writable = self.writable(&catalog);
        let procedures = writable.map(|w| w.table_procedures()).unwrap_or_default();
        let procedure =
            table_procedure::find(&procedures, &execute.procedure).map_err(DataFusionError::Plan)?;
        let mut arguments = Vec::with_capacity(execute.arguments.len());
        for (name, expr) in &execute.arguments {
            arguments.push(RawArgument {
                name: name.clone(),
                value: Self::constant(ctx, expr).await?,
            });
        }
        let call = table_procedure::bind(&catalog, procedure, arguments).map_err(DataFusionError::Plan)?;
        let session = self.properties.catalog_session(&catalog, &options.properties);
        if let Some(writable) = writable {
            writable.check_table_procedure(&call, &session)?;
        }
        if execute.filter.is_some() {
            return Err(DataFusionError::Plan(
                "Unexpected FilterNode found in plan; probably connector was not able to handle provided WHERE expression".into(),
            ));
        }
        let Some(writable) = writable else {
            return Err(DataFusionError::Plan(table_procedure::not_registered(
                &execute.procedure,
            )));
        };
        writable
            .execute_table_procedure(&schema, &table, &call, &session, ctx.task_ctx())
            .await?;
        let output = Arc::new(Schema::new(vec![Field::new("rows", DataType::Int64, true)]));
        ctx.read_batch(RecordBatch::new_empty(output))
    }

    /// Evaluates a procedure argument, which must be a constant expression.
    async fn constant(ctx: &SessionContext, expr: &SqlExpr) -> Result<ArgumentValue, DataFusionError> {
        let subquery = visit_expressions(expr, |e| {
            if matches!(
                e,
                SqlExpr::Subquery(_) | SqlExpr::Exists { .. } | SqlExpr::InSubquery { .. }
            ) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        if subquery.is_break() {
            return Err(DataFusionError::Plan(
                "Constant expression cannot contain a subquery".into(),
            ));
        }
        // As in Trino, `1.5` is a DECIMAL literal (DataFusion's default is DOUBLE).
        let mut state = ctx.state();
        state.config_mut().options_mut().sql_parser.parse_float_as_decimal = true;
        let batches = SessionContext::new_with_state(state)
            .sql(&format!("SELECT {expr}"))
            .await?
            .collect()
            .await?;
        let batch = batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .ok_or_else(|| DataFusionError::Internal(format!("constant {expr} produced no value")))?;
        let value = ScalarValue::try_from_array(batch.column(0), 0)?;
        if value.is_null() {
            return Ok(ArgumentValue::Null);
        }
        Ok(match value {
            ScalarValue::Boolean(Some(b)) => ArgumentValue::Boolean(b),
            ScalarValue::Int8(Some(i)) => ArgumentValue::Integer(i.into()),
            ScalarValue::Int16(Some(i)) => ArgumentValue::Integer(i.into()),
            ScalarValue::Int32(Some(i)) => ArgumentValue::Integer(i.into()),
            ScalarValue::Int64(Some(i)) => ArgumentValue::Integer(i),
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
                ArgumentValue::Varchar(s)
            }
            other => ArgumentValue::Other(trino_literal(&other)),
        })
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

    #[test]
    fn constants_print_as_trino_literals() {
        assert_eq!(
            trino_literal(&ScalarValue::Decimal128(Some(15), 2, 1)),
            "DECIMAL '1.5'"
        );
        assert_eq!(
            trino_literal(&ScalarValue::Date32(Some(18_262))),
            "DATE '2020-01-01'"
        );
        assert_eq!(trino_literal(&ScalarValue::Float64(Some(1.5))), "1.5E0");
        assert_eq!(trino_literal(&ScalarValue::Int64(Some(10))), "10");
    }

    /// Servers run each statement on its own task (`tokio::spawn`), so the
    /// future of every kind of statement must be `Send`.
    #[tokio::test]
    async fn statements_run_on_spawned_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64)));
        let mut answers = Vec::new();
        for sql in [
            "SELECT 1",
            "ALTER TABLE nope.s.t EXECUTE optimize",
            "ALTER TABLE nope.s.t ADD COLUMN c varchar",
        ] {
            let engine = Arc::clone(&engine);
            let answer = tokio::spawn(async move {
                let options = QueryOptions::for_user("u");
                match engine.sql_with(&options, sql).await {
                    Ok(frame) => frame.collect().await.map(|b| b.len()).map_err(|e| e.to_string()),
                    Err(err) => Err(err.to_string()),
                }
            })
            .await
            .unwrap();
            answers.push(answer);
        }
        assert_eq!(answers[0], Ok(1));
        assert!(
            answers[1]
                .as_ref()
                .unwrap_err()
                .contains("Table 'nope.s.t' does not exist"),
            "{answers:?}"
        );
        assert!(answers[2].is_err(), "{answers:?}");
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

    /// `(schema, table, file_size_threshold, session "enabled")`.
    type Call = (String, String, Option<String>, Option<String>);

    /// A writable catalog that registers one procedure and records its calls.
    #[derive(Debug, Default)]
    struct Procedures {
        calls: std::sync::Mutex<Vec<Call>>,
    }

    #[async_trait]
    impl WritableCatalog for Procedures {
        async fn create_schema(&self, _: &str, _: bool, _: &str) -> datafusion::error::Result<()> {
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
            _: &str,
            _: &str,
            _: bool,
            _: &str,
            _: ViewDefinition,
        ) -> datafusion::error::Result<()> {
            Ok(())
        }
        async fn drop_view(&self, _: &str, _: &str, _: bool) -> datafusion::error::Result<()> {
            Ok(())
        }
        async fn add_column(&self, _: &str, _: &str, _: &str, _: &DataType) -> datafusion::error::Result<()> {
            Ok(())
        }
        fn table_procedures(&self) -> Vec<crate::TableProcedure> {
            vec![crate::TableProcedure::new(
                "optimize",
                vec![crate::ProcedureArgument::new(
                    "file_size_threshold",
                    crate::PropertyType::DataSize,
                    "100MB",
                    "threshold",
                )],
            )]
        }
        async fn execute_table_procedure(
            &self,
            schema: &str,
            table: &str,
            call: &crate::ProcedureCall,
            session: &crate::CatalogSession,
            _: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::error::Result<()> {
            self.calls.lock().unwrap().push((
                schema.to_owned(),
                table.to_owned(),
                call.get("file_size_threshold").map(str::to_owned),
                session.get("enabled").map(str::to_owned),
            ));
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ProcedureSource {
        writable: Option<Arc<Procedures>>,
    }

    #[async_trait]
    impl Connector for ProcedureSource {
        fn name(&self) -> &str {
            if self.writable.is_some() { "lake" } else { "plain" }
        }
        fn kind(&self) -> &'static str {
            "fake"
        }
        async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
            let schema = MemorySchemaProvider::new();
            let fields = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let table = MemTable::try_new(fields, vec![vec![]]).unwrap();
            datafusion::catalog::SchemaProvider::register_table(&schema, "t".into(), Arc::new(table))
                .unwrap();
            let ctx = SessionContext::new();
            let plan = ctx
                .sql("SELECT 1 AS id")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap();
            let view = datafusion::datasource::ViewTable::new(plan, Some("SELECT 1 AS id".into()));
            datafusion::catalog::SchemaProvider::register_table(&schema, "v".into(), Arc::new(view)).unwrap();
            let catalog = MemoryCatalogProvider::new();
            catalog.register_schema("public", Arc::new(schema)).unwrap();
            Ok(Arc::new(catalog))
        }
        async fn ping(&self) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn writable(&self) -> Option<Arc<dyn WritableCatalog>> {
            self.writable.clone().map(|w| w as Arc<dyn WritableCatalog>)
        }
        fn session_properties(&self) -> Vec<crate::SessionProperty> {
            vec![crate::SessionProperty::new(
                "enabled",
                crate::PropertyType::Boolean,
                "false",
                crate::Effect::Enforced,
                "gate",
            )]
        }
    }

    async fn procedure_engine(dir: &std::path::Path) -> (Engine, Arc<Procedures>) {
        let mut engine = engine(dir, ByteSize::mib(64), ByteSize::mib(64));
        let procedures = Arc::new(Procedures::default());
        for writable in [Some(Arc::clone(&procedures)), None] {
            engine
                .attach(Arc::new(ProcedureSource { writable }), true, FAST)
                .await
                .unwrap();
        }
        (engine, procedures)
    }

    async fn execute_error(engine: &Engine, sql: &str) -> String {
        engine.sql(sql).await.unwrap_err().to_string()
    }

    #[tokio::test]
    async fn table_execute_is_routed_with_bound_arguments_and_session() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, procedures) = procedure_engine(dir.path()).await;

        let result = engine
            .sql("ALTER TABLE lake.public.t EXECUTE optimize")
            .await
            .unwrap();
        assert_eq!(result.schema().field(0).name(), "rows");
        assert_eq!(result.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(
            result
                .collect()
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );

        let mut options = QueryOptions::for_user("dbt");
        options.catalog = Some("lake".into());
        options.schema = Some("public".into());
        options.properties = engine
            .session_properties()
            .session(std::collections::BTreeMap::from([(
                "lake.enabled".to_owned(),
                "true".to_owned(),
            )]))
            .unwrap();
        engine
            .sql_with(
                &options,
                "alter table T execute \"OPTIMIZE\"(file_size_threshold => '1' || 'MB');",
            )
            .await
            .unwrap();
        engine
            .sql_with(&options, "ALTER TABLE t EXECUTE Optimize('2kB')")
            .await
            .unwrap();

        let calls = procedures.calls.lock().unwrap().clone();
        let expected = |threshold: &str, enabled: &str| {
            (
                "public".to_owned(),
                "t".to_owned(),
                Some(threshold.to_owned()),
                Some(enabled.to_owned()),
            )
        };
        assert_eq!(
            calls,
            vec![
                expected("100MB", "false"),
                expected("1MB", "true"),
                expected("2kB", "true")
            ]
        );
    }

    #[tokio::test]
    async fn table_execute_errors_follow_trino() {
        let dir = tempfile::tempdir().unwrap();
        let (engine, procedures) = procedure_engine(dir.path()).await;
        for (sql, message) in [
            (
                "ALTER TABLE lake.public.nope EXECUTE optimize",
                "Table 'lake.public.nope' does not exist",
            ),
            (
                "ALTER TABLE nocat.s.t EXECUTE optimize",
                "Table 'nocat.s.t' does not exist",
            ),
            (
                "ALTER TABLE lake.public.v EXECUTE optimize",
                "ALTER TABLE EXECUTE is not supported for views",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE foo",
                "Table procedure not registered: FOO",
            ),
            (
                "ALTER TABLE plain.public.t EXECUTE optimize",
                "Table procedure not registered: OPTIMIZE",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(foo => '1MB')",
                "Catalog 'lake' table procedure 'OPTIMIZE' property 'foo' does not exist",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(file_size_threshold => 10)",
                "Cannot convert [10] to varchar",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(file_size_threshold => NULL)",
                "Invalid null value for catalog 'lake' table procedure 'OPTIMIZE' property 'file_size_threshold' from [null]",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(file_size_threshold => 'abc')",
                "size is not a valid data size string: abc",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(file_size_threshold => (SELECT '1MB'))",
                "Constant expression cannot contain a subquery",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize WHERE id = 1",
                "Unexpected FilterNode found in plan",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE \"optimize\"",
                "Table procedure not registered: optimize",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(file_size_threshold => 1.5)",
                "Cannot convert [DECIMAL '1.5'] to varchar",
            ),
            (
                "ALTER TABLE lake.public.t EXECUTE optimize(file_size_threshold => DATE '2020-01-01')",
                "Cannot convert [DATE '2020-01-01'] to varchar",
            ),
            ("ALTER TABLE lake.public.t EXECUTE optimize(", "Expected"),
        ] {
            let error = execute_error(&engine, sql).await;
            assert!(error.contains(message), "{sql}: {error}");
        }
        assert!(procedures.calls.lock().unwrap().is_empty());
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

    /// Span structure, checked with an in-process capture layer (no exporter).
    mod traces {
        use super::*;
        use crate::trace::{QueryTrace, TraceBackend, TraceParent};
        use std::collections::BTreeMap;
        use std::sync::Mutex;
        use tracing::Subscriber;
        use tracing::field::{Field as TracingField, Visit};
        use tracing::span::{Attributes, Id, Record};
        use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
        use tracing_subscriber::registry::LookupSpan;

        #[derive(Debug, Clone, Default)]
        struct Captured {
            name: &'static str,
            parent: Option<usize>,
            fields: BTreeMap<String, String>,
            closed: bool,
        }

        #[derive(Clone, Default)]
        struct Capture(Arc<Mutex<Vec<Captured>>>);

        /// Position of a span in the capture (span ids are reused by the registry).
        struct Index(usize);

        struct Fields<'a>(&'a mut BTreeMap<String, String>);

        impl Visit for Fields<'_> {
            fn record_debug(&mut self, field: &TracingField, value: &dyn std::fmt::Debug) {
                self.0.insert(field.name().to_owned(), format!("{value:?}"));
            }
            fn record_str(&mut self, field: &TracingField, value: &str) {
                self.0.insert(field.name().to_owned(), value.to_owned());
            }
        }

        impl<S: Subscriber + for<'a> LookupSpan<'a>> tracing_subscriber::Layer<S> for Capture {
            fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
                if attrs.metadata().target() != TARGET {
                    return;
                }
                let span = ctx.span(id).unwrap();
                let parent = span
                    .parent()
                    .and_then(|p| p.extensions().get::<Index>().map(|i| i.0));
                let mut record = Captured {
                    name: attrs.metadata().name(),
                    parent,
                    ..Captured::default()
                };
                attrs.record(&mut Fields(&mut record.fields));
                let mut spans = self.0.lock().unwrap();
                span.extensions_mut().insert(Index(spans.len()));
                spans.push(record);
            }
            fn on_record(&self, id: &Id, values: &Record<'_>, ctx: LayerContext<'_, S>) {
                let span = ctx.span(id).unwrap();
                if let Some(index) = span.extensions().get::<Index>() {
                    values.record(&mut Fields(&mut self.0.lock().unwrap()[index.0].fields));
                }
            }
            fn on_close(&self, id: Id, ctx: LayerContext<'_, S>) {
                let span = ctx.span(&id).unwrap();
                if let Some(index) = span.extensions().get::<Index>() {
                    self.0.lock().unwrap()[index.0].closed = true;
                }
            }
        }

        impl Capture {
            fn spans(&self) -> Vec<Captured> {
                self.0.lock().unwrap().clone()
            }
        }

        fn only<'a>(spans: &'a [Captured], name: &str) -> (usize, &'a Captured) {
            let found: Vec<_> = spans.iter().enumerate().filter(|(_, s)| s.name == name).collect();
            assert_eq!(found.len(), 1, "{name}: {spans:#?}");
            found[0]
        }

        fn has_ancestor(spans: &[Captured], mut index: usize, ancestor: usize) -> bool {
            while let Some(parent) = spans[index].parent {
                if parent == ancestor {
                    return true;
                }
                index = parent;
            }
            false
        }

        fn traced_engine(dir: &std::path::Path) -> Engine {
            let engine = engine(dir, ByteSize::mib(64), ByteSize::mib(64));
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    crate::testing::int64(&[1, 42, 43]),
                    crate::testing::strings(&["a", "secret", "b"]),
                ],
            )
            .unwrap();
            let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            engine.context().register_table("t", Arc::new(table)).unwrap();
            let remote = crate::testing::remote_table(
                "erp",
                "r",
                vec![("id", crate::testing::int64(&[3, 1, 2]))],
                None,
            );
            engine.context().register_table("r", remote).unwrap();
            engine
        }

        fn capture() -> (Capture, tracing::subscriber::DefaultGuard) {
            let capture = Capture::default();
            let subscriber = tracing_subscriber::registry().with(capture.clone());
            let guard = tracing::subscriber::set_default(subscriber);
            (capture, guard)
        }

        fn ids(batches: &[RecordBatch]) -> Vec<i64> {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect()
        }

        // Current-thread runtime: the thread-local subscriber also sees
        // the tasks DataFusion spawns.
        #[tokio::test(flavor = "current_thread")]
        async fn query_span_has_one_child_per_phase_and_no_literals() {
            let dir = tempfile::tempdir().unwrap();
            let engine = traced_engine(dir.path());
            let (capture, _guard) = capture();
            let mut options = QueryOptions::for_user("alice").with_trace(
                QueryTrace::from_headers(None, None, Some("dbt-run-7"))
                    .with_query_id("20260923_000000_00001_test1"),
            );
            options.catalog = Some("datafusion".into());
            options.schema = Some("public".into());
            let query = engine
                .query(&options, "SELECT id FROM t WHERE name <> 'secret' AND id > 41")
                .await
                .unwrap();
            assert_eq!(query.query_id(), "20260923_000000_00001_test1");
            assert_eq!(query.trace_id(), None, "no backend installed");
            let rows = query.collect().await.unwrap();
            assert_eq!(ids(&rows), vec![43]);

            let spans = capture.spans();
            let (root, query) = only(&spans, "codelake.query");
            assert_eq!(query.parent, None);
            let field = |name: &str| query.fields.get(name).map(String::as_str);
            assert_eq!(field("codelake.query_id"), Some("20260923_000000_00001_test1"));
            assert_eq!(field("enduser.id"), Some("alice"));
            assert_eq!(field("codelake.catalog"), Some("datafusion"));
            assert_eq!(field("codelake.schema"), Some("public"));
            assert_eq!(field("codelake.correlation_id"), Some("dbt-run-7"));
            assert_eq!(field("otel.kind"), Some("server"));
            assert_eq!(field("db.response.returned_rows"), Some("1"));
            assert_eq!(field("otel.status_code"), None);
            let text = field("db.query.text").unwrap();
            assert_eq!(text, "SELECT id FROM t WHERE name <> ? AND id > ?");
            assert_eq!(field("codelake.sql.hash").unwrap(), crate::trace::sql_hash(text));

            for phase in ["codelake.parse", "codelake.execute", "codelake.fetch"] {
                let (_, span) = only(&spans, phase);
                assert_eq!(span.parent, Some(root), "{phase}: {spans:#?}");
            }
            assert!(
                spans
                    .iter()
                    .any(|s| s.name == "codelake.plan" && s.parent == Some(root)),
                "{spans:#?}"
            );
            let (_, fetch) = only(&spans, "codelake.fetch");
            assert_eq!(
                fetch.fields.get("db.response.returned_rows").map(String::as_str),
                Some("1")
            );
            assert!(
                spans.iter().all(|s| s.closed),
                "every span ends with the query: {spans:#?}"
            );
        }

        #[tokio::test(flavor = "current_thread")]
        async fn remote_scans_are_spans_below_the_query() {
            let dir = tempfile::tempdir().unwrap();
            let engine = traced_engine(dir.path());
            let (capture, _guard) = capture();
            let rows = engine
                .query(&QueryOptions::for_user("bob"), "SELECT id FROM r")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            // The fake remote returns its rows as stored: [3, 1, 2].
            assert_eq!(ids(&rows), vec![3, 1, 2]);

            let spans = capture.spans();
            let (root, _) = only(&spans, "codelake.query");
            let (remote, span) = only(&spans, "codelake.remote");
            assert!(has_ancestor(&spans, remote, root), "{spans:#?}");
            let field = |name: &str| span.fields.get(name).map(String::as_str);
            assert_eq!(field("codelake.remote.executor"), Some("ignores_filters"));
            assert_eq!(field("otel.kind"), Some("client"));
            assert_eq!(field("db.response.returned_rows"), Some("3"));
            assert!(span.closed);
        }

        #[tokio::test(flavor = "current_thread")]
        async fn planning_errors_mark_the_query_and_phase_without_values() {
            let dir = tempfile::tempdir().unwrap();
            let engine = traced_engine(dir.path());
            let (capture, _guard) = capture();
            let err = engine
                .query(
                    &QueryOptions::for_user("carol"),
                    "SELECT secret_col FROM t WHERE name = 'top-secret'",
                )
                .await
                .unwrap_err();
            assert!(err.to_string().contains("secret_col"), "{err}");

            let spans = capture.spans();
            let (_, query) = only(&spans, "codelake.query");
            assert_eq!(
                query.fields.get("otel.status_code").map(String::as_str),
                Some("ERROR")
            );
            let description = &query.fields["otel.status_description"];
            assert!(!description.contains("top-secret"), "{description}");
            assert!(
                spans
                    .iter()
                    .any(|s| s.name == "codelake.plan" && s.fields.contains_key("otel.status_code")),
                "{spans:#?}"
            );
            assert!(!query.fields["db.query.text"].contains("top-secret"));
        }

        #[tokio::test(flavor = "current_thread")]
        async fn dropping_the_stream_early_closes_the_query_as_cancelled() {
            let dir = tempfile::tempdir().unwrap();
            let engine = traced_engine(dir.path());
            let (capture, _guard) = capture();
            let stream = engine
                .query(&QueryOptions::for_user("dave"), "SELECT id FROM t")
                .await
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            drop(stream);
            let spans = capture.spans();
            for name in ["codelake.query", "codelake.fetch"] {
                let (_, span) = only(&spans, name);
                assert_eq!(
                    span.fields.get("codelake.cancelled").map(String::as_str),
                    Some("true")
                );
                assert_eq!(
                    span.fields.get("db.response.returned_rows").map(String::as_str),
                    Some("0")
                );
                assert!(span.closed, "{name}");
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn a_query_failing_while_streaming_is_an_error_not_a_cancellation() {
            let dir = tempfile::tempdir().unwrap();
            let engine = traced_engine(dir.path());
            let (capture, _guard) = capture();
            // Fails at id 42 while the results stream.
            let err = engine
                .query(
                    &QueryOptions::for_user("frank"),
                    "SELECT 10 / (id - 42) AS x FROM t",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err();
            assert!(err.to_string().to_lowercase().contains("divide by zero"), "{err}");

            let spans = capture.spans();
            for name in ["codelake.query", "codelake.fetch"] {
                let (_, span) = only(&spans, name);
                let field = |key: &str| span.fields.get(key).map(String::as_str);
                assert_eq!(field("otel.status_code"), Some("ERROR"), "{name}: {span:#?}");
                assert_eq!(field("codelake.cancelled"), None, "{name}: {span:#?}");
                assert!(field("db.response.returned_rows").is_some(), "{name}: {span:#?}");
                assert!(span.closed, "{name}");
            }
        }

        #[derive(Debug, Default)]
        struct RecordingBackend(Mutex<Vec<(TraceParent, Option<String>)>>);

        impl TraceBackend for &'static RecordingBackend {
            fn set_remote_parent(&self, span: &Span, parent: &TraceParent, tracestate: Option<&str>) {
                if !span.is_none() {
                    self.0
                        .lock()
                        .unwrap()
                        .push((*parent, tracestate.map(str::to_owned)));
                }
            }
            fn trace_id(&self, _span: &Span) -> Option<String> {
                None
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn incoming_traceparent_is_handed_to_the_backend() {
            static BACKEND: std::sync::OnceLock<RecordingBackend> = std::sync::OnceLock::new();
            let backend = BACKEND.get_or_init(RecordingBackend::default);
            assert!(crate::trace::install_backend(Box::new(backend)));
            assert!(
                !crate::trace::install_backend(Box::new(backend)),
                "first backend stays"
            );

            let dir = tempfile::tempdir().unwrap();
            let engine = traced_engine(dir.path());
            let (capture, _guard) = capture();
            let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01";
            let options = QueryOptions::for_user("erin").with_trace(QueryTrace::from_headers(
                Some(header),
                Some("v=1"),
                None,
            ));
            let rows = engine
                .query(&options, "SELECT id FROM t ORDER BY id")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(ids(&rows), vec![1, 42, 43]);

            let calls = backend.0.lock().unwrap().clone();
            assert_eq!(
                calls,
                vec![(TraceParent::parse(header).unwrap(), Some("v=1".to_owned()))]
            );
            let spans = capture.spans();
            let (_, query) = only(&spans, "codelake.query");
            // Without an exporter the logs still carry the caller's trace id.
            assert_eq!(
                query.fields.get("codelake.trace_id").map(String::as_str),
                Some("4bf92f3577b34da6a3ce929d0e0e4736")
            );
        }
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
