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
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream, SessionStateBuilder};
use datafusion::logical_expr::{DdlStatement, LogicalPlan, ScalarUDF};
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

use crate::computed_batches::ComputedBatches;
use crate::config::{ExecutionConfig, GovernanceConfig, MemoryConfig};
use crate::connector::{Connector, ConnectorError, ViewDefinition, WritableCatalog};
use crate::ddl::{self, Prepared, RelationKind, Rewrite, TableExecute, ViewSecurity};
use crate::federation_guard::FederationFilterGuard;
use crate::governance::Governance;
use crate::information_schema::CatalogList;
use crate::join_guard::{JoinQueryPlanner, NestedLoopJoinSpillGuard, NullAwareAntiJoinGuard};
use crate::output::AccountedOutput;
use crate::plain_views::PlainViews;
use crate::runtime::{EngineRuntime, MemoryStats};
use crate::session_properties::{
    PropertyRegistry, QUERY_MAX_EXECUTION_TIME, QUERY_MAX_RUN_TIME, SessionProperties,
};
use crate::statement_policy::{self, ObjectKind, check_object_name};
use crate::statistics_guard::guard_aggregate_statistics;
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
    /// Functions this protocol defines on top of the engine's (for instance
    /// `version()`, which reports the emulated server's version).
    pub functions: Vec<Arc<ScalarUDF>>,
    /// Session time zone (`X-Trino-Time-Zone`), as Trino names it (see
    /// [`crate::trino_time_zone`]); `None` keeps the engine's, `UTC`.
    pub time_zone: Option<String>,
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
            functions: Vec::new(),
            time_zone: None,
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

    /// Records on the query span where governance admitted it: its resource
    /// group and how long it waited in the queue.
    pub fn record_admission(&self, resource_group: &str, queued: Duration) {
        self.span.record("codelake.resource_group", resource_group);
        self.span.record(
            "codelake.queued_ms",
            u64::try_from(queued.as_millis()).unwrap_or(u64::MAX),
        );
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
    // Float comparisons with a zero, before the simplifier folds or
    // combines them with the bit order (`crate::trino_functions`).
    let simplify = rules
        .iter()
        .position(|r| r.name() == "simplify_expressions")
        .unwrap_or(0);
    rules.insert(simplify, crate::trino_functions::float_zeros());
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
    // The columns `optimize_projections` keeps above a federated node go
    // into it (`crate::federation_policy::PruneFederatedColumns`).
    let projections = rules
        .iter()
        .position(|r| r.name() == "optimize_projections")
        .map_or(rules.len(), |p| p + 1);
    rules.insert(
        projections,
        Arc::new(crate::federation_policy::PruneFederatedColumns),
    );
    // `COUNT(DISTINCT x)` beside a count, in two spillable phases
    // (`crate::distinct_counts`), before `DataFusion`'s own rewrite.
    let distinct = rules
        .iter()
        .position(|r| r.name() == "single_distinct_aggregation_to_group_by")
        .unwrap_or(rules.len());
    rules.insert(distinct, Arc::new(crate::distinct_counts::DistinctCountToGroupBy));
    rules.extend(crate::trino_functions::optimizer_rules());
    // `NOT IN` keeps its NULL semantics, and nothing is inferred from the
    // keys of a null-aware join (`crate::join_guard`).
    crate::join_guard::guard_filter_pushdown(crate::join_guard::with_null_safe_not_in(rules))
}

/// `DataFusion`'s analyzer rules with the engine's: `char(n)` semantics
/// ([`crate::char_semantics`], which must see only the casts the statement
/// wrote), `RANGE` frames without offsets on keys without range arithmetic as
/// `GROUPS` (module `window_frames`), then Trino's decimal types and common types (module
/// `coercion`) right before type coercion (which then recomputes every
/// schema), then checked arithmetic ([`crate::overflow`]), the one-row rule
/// and the correlated expression keys of scalar subqueries
/// ([`crate::subquery`]), which need the types coercion settles, and
/// `INTERSECT ALL`/`EXCEPT ALL` as multiset operations (module `set_operations`).
pub fn analyzer_rules() -> Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> {
    let mut rules = datafusion::optimizer::Analyzer::new().rules;
    let coercion = rules
        .iter()
        .position(|r| r.name() == "type_coercion")
        .unwrap_or(rules.len());
    rules.insert(coercion, Arc::new(crate::coercion::TrinoCommonTypes));
    rules.insert(coercion, Arc::new(crate::overflow::TrinoDecimalTypes));
    rules.insert(coercion, Arc::new(crate::char_semantics::CharSemantics));
    rules.insert(coercion, Arc::new(crate::window_frames::RangeFramesAsGroups));
    rules.push(Arc::new(crate::overflow::CheckedArithmetic));
    rules.push(Arc::new(crate::subquery::EnforceScalarSubqueryRows));
    rules.push(Arc::new(crate::subquery::CorrelatedExpressionKeys));
    rules.push(Arc::new(crate::set_operations::MultisetOperations));
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

/// Default `DataFusion` physical rules, preceded by the federation guard and
/// the null-aware anti join guard (both must run before `FilterPushdown`; see
/// [`crate::federation_guard`] and [`crate::join_guard`]), with
/// aggregates answered from statistics only over exact sources (see
/// [`crate::statistics_guard`]), hash joins too large for the pool planned as
/// sort-merge joins right after `EnforceSorting` ([`crate::join_spill`]), the
/// null-safe join guard right before `FilterPushdown(Post)` (it must see the
/// build and probe sides `JoinSelection` picked; see [`crate::join_guard`]),
/// and followed by
/// [`PlainViews`], which keeps string views out of operators that retain rows
/// (ADR 0003), [`ComputedBatches`], which sizes the batches of computed values
/// in bytes, the nested loop join spill guard ([`crate::join_guard`]), and
/// [`AccountedOutput`], which keeps the buffers between the plan
/// and the client inside the memory pool.
pub(crate) fn physical_rules() -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
        vec![Arc::new(FederationFilterGuard), Arc::new(NullAwareAntiJoinGuard)];
    rules.extend(PhysicalOptimizer::new().rules);
    let rules = crate::join_spill::with_sort_merge_oversized_joins(rules);
    let rules = crate::join_guard::with_null_safe_join_guard(rules);
    let mut rules = guard_aggregate_statistics(rules);
    rules.extend(crate::trino_functions::physical_rules());
    rules.push(Arc::new(PlainViews));
    rules.push(Arc::new(ComputedBatches));
    rules.push(Arc::new(NestedLoopJoinSpillGuard));
    rules.push(Arc::new(AccountedOutput));
    rules
}

/// The engine's query planner: the federation planner, with null-aware anti
/// joins kept on hash joins ([`crate::join_guard`]), after the check that an
/// `UPDATE` / `DELETE` hands its table every condition of its `WHERE`
/// (`crate::dml_guard`).
pub(crate) fn query_planner() -> Arc<dyn datafusion::execution::context::QueryPlanner + Send + Sync> {
    Arc::new(crate::dml_guard::DmlQueryPlanner::new(Arc::new(
        JoinQueryPlanner::new(Arc::new(FederatedQueryPlanner::new())),
    )))
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
/// column types; nullability and field metadata may differ, as between the
/// branches of a `UNION ALL`, see [`crate::batch_schema`]).
fn with_schema(
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> datafusion::physical_plan::SendableRecordBatchStream {
    use futures::StreamExt;
    let announced = Arc::clone(&schema);
    Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        announced,
        stream.map(move |batch| batch.and_then(|b| crate::batch_schema::announced(b, &schema))),
    ))
}

/// Rewrites a parsed statement so `DataFusion` plans it with Trino's
/// semantics. The function library's literal rewrites come first
/// ([`crate::trino_functions::rewrite_statement`]: `TIMESTAMP '…'`,
/// `DECIMAL '…'`, numeric literals, `AT TIME ZONE`, `CAST(… AS varchar(n))`);
/// then the case of identifiers ([`crate::identifier_case`]), zoned timestamp literals it left, `VALUES`
/// lists, `DECIMAL` without precision, `char(n)` and the `timestamp` types
/// (see the modules `literals`, `coercion` and `declared`). Every statement
/// the engine plans goes through it, and so must the SQL of a view a
/// connector plans itself.
pub fn trino_statement(
    statement: &mut datafusion::sql::parser::Statement,
    session_zone: &str,
) -> Result<(), DataFusionError> {
    crate::trino_sql_checks::regroup_distinct_from(statement);
    crate::char_semantics::refuse_natural_joins(statement)?;
    crate::coercion::name_casts_in_aggregates(statement);
    if let datafusion::sql::parser::Statement::Statement(inner) = statement {
        crate::identifier_case::fold_identifiers(inner);
    }
    crate::trino_sql_checks::name_insert_columns(statement);
    crate::trino_functions::rewrite_statement(statement)?;
    crate::literals::rewrite_zoned_timestamps(statement);
    crate::coercion::mark_values(statement);
    crate::coercion::name_later_branches(statement);
    crate::coercion::default_decimal_precision(statement);
    crate::declared::rewrite_declared_casts(statement);
    crate::literals::rewrite_timestamp_types(statement, session_zone);
    Ok(())
}

/// Rewrites SQL text the parser cannot read into the spelling it reads, with
/// the reference meaning (see [`crate::trino_functions::preprocess_sql`]:
/// `VALUES 1, 2`, `TRIM(TRAILING FROM s)`, `CAST(x AS array(T))`) and the
/// reference type syntax in a `CREATE TABLE` column list
/// ([`crate::trino_type_syntax`]).
/// Applied before parsing to every statement the engine plans, and so to the
/// SQL of a view a connector plans itself.
pub fn trino_text(sql: &str) -> std::borrow::Cow<'_, str> {
    match crate::declared::trino_type_syntax(sql) {
        std::borrow::Cow::Borrowed(sql) => crate::trino_functions::preprocess_sql(sql),
        std::borrow::Cow::Owned(sql) => {
            std::borrow::Cow::Owned(crate::trino_functions::preprocess_sql(&sql).into_owned())
        }
    }
}

/// A statement after parsing: `ALTER TABLE … EXECUTE`, which the SQL parser
/// does not know, or an ordinary statement with its DDL preparation.
enum Parsed {
    TableExecute(Box<TableExecute>),
    Statement(Box<Prepped>),
}

/// A parsed statement, rewritten for planning, with what its DDL declares.
struct Prepped {
    statement: datafusion::sql::parser::Statement,
    prepared: Option<Prepared>,
    /// Column types and options as a `CREATE TABLE` / `ADD COLUMN` wrote them.
    declared: Vec<ddl::DeclaredColumn>,
    /// Types narrower than their Arrow type (`varchar(n)`, see `declared`).
    new_columns: Vec<(String, crate::trino_types::Declared)>,
    /// `map(K, V)` columns of a `CREATE TABLE`.
    maps: Vec<(
        String,
        datafusion::sql::sqlparser::ast::DataType,
        datafusion::sql::sqlparser::ast::DataType,
    )>,
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
        codelake.resource_group = Empty,
        codelake.queued_ms = Empty,
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

/// `field` with the type and comment the DDL declared for it (see
/// [`crate::DECLARED_TYPE`]).
fn declared_field(field: Field, sql_type: &str, comment: Option<&str>) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(crate::connector::DECLARED_TYPE.to_owned(), sql_type.to_owned());
    if let Some(comment) = comment {
        metadata.insert(crate::connector::COLUMN_COMMENT.to_owned(), comment.to_owned());
    }
    field.with_metadata(metadata)
}

/// The rows of `CREATE TABLE t (<columns>)` with each column's declared type
/// and comment in its field metadata, for the writable catalog to store.
fn with_declared_columns(
    rows: SendableRecordBatchStream,
    declared: &[ddl::DeclaredColumn],
) -> Result<SendableRecordBatchStream, DataFusionError> {
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    if declared.is_empty() {
        return Ok(rows);
    }
    let schema = rows.schema();
    if schema.fields().len() != declared.len() {
        return Err(DataFusionError::Internal(format!(
            "CREATE TABLE declares {} columns but plans {}",
            declared.len(),
            schema.fields().len()
        )));
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(declared)
        .map(|(field, column)| {
            declared_field(
                field.as_ref().clone(),
                &column.sql_type,
                column.comment.as_deref(),
            )
        })
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    let adapted = Arc::clone(&schema);
    let batches = rows.map(move |batch| {
        batch.and_then(|batch| {
            RecordBatch::try_new(Arc::clone(&adapted), batch.columns().to_vec())
                .map_err(DataFusionError::from)
        })
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

/// Parses `sql` (after [`ddl::rewrite`]) and rewrites it for planning:
/// `ALTER TABLE … EXECUTE`, or a statement with what its DDL declares (see
/// [`Prepped`]). Synchronous: the parser's dialect is a `Box<dyn Dialect>`
/// (not `Send`), which must be gone before any `.await`, or callers cannot
/// spawn the planning future.
fn parse_statement(
    state: &datafusion::execution::SessionState,
    sql: &str,
    security: ViewSecurity,
) -> Result<Parsed, DataFusionError> {
    let dialect = state.config().options().sql_parser.dialect;
    // Trino syntax the SQL parser rejects (`VALUES 1, 2`, …).
    let sql = trino_text(sql);
    if let Some(parser_dialect) = dialect_from_str(dialect) {
        statement_policy::check_tokens(&sql, parser_dialect.as_ref())?;
        if let Some(execute) = ddl::parse_table_execute(&sql, parser_dialect.as_ref())? {
            return Ok(Parsed::TableExecute(Box::new(execute)));
        }
    }
    let mut statement = state.sql_to_statement(&sql, &dialect)?;
    // Before the DDL preparation records declared column names: an unquoted
    // non-ASCII capital is folded as the planner folds `A`-`Z` (ISS-018).
    if let datafusion::sql::parser::Statement::Statement(inner) = &mut statement {
        crate::identifier_case::fold_unquoted(inner);
    }
    statement_policy::check_statement(&statement)?;
    crate::trino_sql_checks::check_statement(&statement)?;
    ddl::rewrite_show_tables(&mut statement)?;
    // Before `declared::declared_columns`, which rewrites the declared types
    // it records.
    let declared = ddl::declared_columns(&statement);
    let prepared = ddl::prepare(&mut statement, security)?;
    // After `prepare`, which keeps a view's text as written.
    let new_columns = crate::declared::declared_columns(&mut statement);
    let maps = crate::declared::map_columns(&mut statement);
    let session_zone = state.config().options().execution.time_zone.clone();
    trino_statement(&mut statement, session_zone.as_deref().unwrap_or("UTC"))?;
    Ok(Parsed::Statement(Box::new(Prepped {
        statement,
        prepared,
        declared,
        new_columns,
        maps,
    })))
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

/// `state` rebuilt through `f` (`SessionStateBuilder::new_from_existing`),
/// with the functions `state` resolves kept.
///
/// `new_from_existing` re-registers every entry of the function maps, the
/// alias entries of the built-ins the engine replaced among them
/// (`datepart` → `DataFusion`'s `date_part`), in hash order (`DataFusion`
/// 54.1): the built-in can take its name back, and `extract(year …)` turns
/// into `DataFusion`'s `date_part` (an `int`, not Trino's `bigint`) from one
/// rebuild to the next. Each function is registered again under its own name,
/// updated for the rebuilt configuration (the session time zone).
pub fn rebuild_state(
    state: &datafusion::execution::SessionState,
    f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
) -> datafusion::execution::SessionState {
    let mut rebuilt = f(SessionStateBuilder::new_from_existing(state.clone())).build();
    let config = rebuilt.config_options().as_ref().clone();
    for (name, udf) in state.scalar_functions() {
        if name == udf.name() {
            let udf = udf
                .inner()
                .with_updated_config(&config)
                .map_or_else(|| Arc::clone(udf), Arc::new);
            // Registering replaces; there is no failure to report.
            let _ = rebuilt.register_udf(udf);
        }
    }
    for (name, udaf) in state.aggregate_functions() {
        if name == udaf.name() {
            let _ = rebuilt.register_udaf(Arc::clone(udaf));
        }
    }
    for (name, udwf) in state.window_functions() {
        if name == udwf.name() {
            let _ = rebuilt.register_udwf(Arc::clone(udwf));
        }
    }
    rebuilt
}

/// Last step of every query's session configuration (see
/// [`Engine::with_session_hook`]).
pub type SessionHook = Arc<dyn Fn(&mut SessionConfig) + Send + Sync>;

pub struct Engine {
    ctx: SessionContext,
    runtime: EngineRuntime,
    sources: Vec<(Arc<dyn Connector>, bool)>,
    writable: HashMap<String, Arc<dyn WritableCatalog>>,
    properties: PropertyRegistry,
    governance: Arc<Governance>,
    session_hook: Option<SessionHook>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("session_id", &self.ctx.session_id())
            .field("runtime", &self.runtime)
            .field("sources", &self.sources)
            .field("writable", &self.writable.keys().collect::<Vec<_>>())
            .field("properties", &self.properties)
            .field("governance", &self.governance)
            .field("session_hook", &self.session_hook.is_some())
            .finish()
    }
}

impl Engine {
    /// An engine with the default governance (see [`Engine::with_governance`]).
    pub fn new(memory: &MemoryConfig, execution: &ExecutionConfig) -> Result<Self, EngineError> {
        Self::with_governance(memory, execution, &GovernanceConfig::default(), None)
    }

    /// An engine whose queries are governed by `governance` (admission, time
    /// limits, registry); `history` overrides the registry's history size and
    /// retention (see [`crate::Config::query_history`]). The registry is the
    /// table `system.runtime.queries`, and the configured time limits are the
    /// defaults of the `query_max_run_time` and `query_max_execution_time`
    /// session properties.
    pub fn with_governance(
        memory: &MemoryConfig,
        execution: &ExecutionConfig,
        governance: &GovernanceConfig,
        history: Option<(usize, u64)>,
    ) -> Result<Self, EngineError> {
        trace::install_task_propagation();
        let runtime = EngineRuntime::new(memory)?;
        // `information_schema` is provided per catalog by `CatalogList`.
        let mut session = SessionConfig::new()
            .with_batch_size(execution.batch_size)
            .with_information_schema(false);
        session = crate::runtime::sort_settings(session, memory);
        session = session.with_option_extension(crate::join_spill::JoinSpillOptions::for_pool(
            memory.limit.as_usize(),
        ));
        // `1.5` is `decimal(2,1)`, as in Trino (and PostgreSQL), not a double.
        session.options_mut().sql_parser.parse_float_as_decimal = true;
        if let Some(partitions) = execution.target_partitions {
            session = session.with_target_partitions(partitions);
        }
        // NULLs last in both directions and the UTC session zone (see
        // `trino_functions::session_config`).
        let session = crate::trino_functions::session_config(session);
        let state = SessionStateBuilder::new()
            .with_config(session)
            .with_runtime_env(runtime.env())
            .with_catalog_list(Arc::new(CatalogList::new()))
            .with_default_features()
            .with_analyzer_rules(analyzer_rules())
            .with_optimizer_rules(optimizer_rules())
            .with_query_planner(query_planner())
            .with_physical_optimizer_rules(physical_rules())
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        ctx.register_udf(crate::functions::varbinary_literal());
        ctx.register_udf(crate::coercion::values_cell());
        ctx.register_udf(crate::timestamp_casts::try_arrow_cast());
        ctx.register_udf(crate::declared::trino_typed());
        ctx.register_udf(crate::declared::trino_try_typed());
        for udf in crate::char_semantics::functions() {
            ctx.register_udf(udf);
        }
        ctx.register_udf(crate::coercion::named_cast());
        ctx.register_udf(crate::coercion::exact_cast());
        crate::trino_functions::install(&mut ctx)?;
        let mut properties = PropertyRegistry::default();
        for (name, value) in [
            (QUERY_MAX_RUN_TIME, &governance.query_max_run_time),
            (QUERY_MAX_EXECUTION_TIME, &governance.query_max_execution_time),
        ] {
            properties
                .set_system_default(name, value)
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        }
        let governance = Arc::new(Governance::new(governance, history));
        ctx.register_catalog(
            "system",
            crate::governance::system_catalog(governance.registry())?,
        );
        Ok(Self {
            ctx,
            runtime,
            sources: Vec::new(),
            writable: HashMap::new(),
            properties,
            governance,
            session_hook: None,
        })
    }

    /// Rebuilds the engine's session state through `f`, keeping its catalogs,
    /// functions, rules and runtime: how a layer outside the core (the
    /// distributed planner, M4) wraps the query planner or adds codecs.
    pub fn map_state(&mut self, f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder) {
        self.ctx = SessionContext::new_with_state(rebuild_state(&self.ctx.state(), f));
    }

    /// [`Engine::map_state`] with a step that can fail; the engine is left
    /// unchanged when it does.
    ///
    /// # Errors
    ///
    /// What `f` returns.
    pub fn try_map_state<E>(
        &mut self,
        f: impl FnOnce(SessionStateBuilder) -> Result<SessionStateBuilder, E>,
    ) -> Result<(), E> {
        let mut failure = None;
        let rebuilt = rebuild_state(&self.ctx.state(), |builder| match f(builder) {
            Ok(builder) => builder,
            Err(err) => {
                failure = Some(err);
                SessionStateBuilder::new()
            }
        });
        if let Some(err) = failure {
            return Err(err);
        }
        self.ctx = SessionContext::new_with_state(rebuilt);
        Ok(())
    }

    /// Runs `hook` on the configuration of every query's session, after the
    /// query's own settings (catalog, schema, time zone, properties): how a
    /// layer outside the core carries them further (to distributed workers).
    pub fn with_session_hook(&mut self, hook: SessionHook) {
        self.session_hook = Some(hook);
    }

    /// The runtime every query runs in: the node's memory pool, disk manager
    /// and object stores.
    pub fn runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.runtime.env()
    }

    /// Admission, time limits and the query registry of this node.
    pub fn governance(&self) -> &Arc<Governance> {
        &self.governance
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
        let ctx = self.session(options)?;
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
        if let Some(value) = options
            .properties
            .get(crate::session_properties::SORT_MERGE_OVERSIZED_JOINS)
            && let Some(join_spill) = state
                .config_mut()
                .options_mut()
                .extensions
                .get_mut::<crate::join_spill::JoinSpillOptions>()
        {
            join_spill.sort_merge_oversized_joins = value == "true";
        }
        for function in &options.functions {
            // Replacing a built-in is the point; the previous one is not needed.
            state.register_udf(Arc::clone(function))?;
        }
        if let Some(hook) = &self.session_hook {
            hook(state.config_mut());
        }
        Ok(SessionContext::new_with_state(state))
    }

    async fn plan_and_run(
        &self,
        ctx: &SessionContext,
        options: &QueryOptions,
        sql: &str,
    ) -> Result<DataFrame, DataFusionError> {
        let user = options.user.as_str();
        let state = ctx.state();
        // Statements the SQL parser rejects, recognized from their
        // tokens (`ALTER VIEW … RENAME TO`, `CREATE VIEW … SECURITY`).
        let (sql, security) = match ddl::rewrite(sql)? {
            Some(Rewrite::RenameView { from, to }) => {
                return traced(
                    Phase::Execute,
                    self.rename(ctx, RelationKind::View, &from, &to, false),
                )
                .await;
            }
            Some(Rewrite::ViewSecurity { sql, security }) => (std::borrow::Cow::Owned(sql), security),
            None => (std::borrow::Cow::Borrowed(sql), ViewSecurity::default()),
        };
        let parse = phase_span(Phase::Parse);
        let parsed = parse
            .in_scope(|| parse_statement(&state, &sql, security))
            .inspect_err(|err| trace::record_error(&parse, err))?;
        drop(parse);
        let Prepped {
            statement,
            prepared,
            declared,
            new_columns,
            maps,
        } = match parsed {
            Parsed::TableExecute(execute) => {
                return traced(Phase::Execute, self.table_execute(ctx, options, *execute)).await;
            }
            Parsed::Statement(prepped) => *prepped,
        };
        match &prepared {
            Some(Prepared::AddColumn {
                table,
                column,
                sql_type,
            }) => {
                let declared = declared.into_iter().next().unwrap_or_default();
                return traced(
                    Phase::Execute,
                    self.add_column(ctx, table, column, sql_type, &declared),
                )
                .await;
            }
            Some(Prepared::Rename {
                kind,
                from,
                to,
                if_exists,
            }) => {
                return traced(Phase::Execute, self.rename(ctx, *kind, from, to, *if_exists)).await;
            }
            Some(Prepared::ShowCreateTable(table)) => {
                let (catalog, schema, name) = Self::resolve(ctx, table);
                let writable = self.writable(&catalog);
                let show = crate::show_create::create_table(ctx, [&catalog, &schema, &name], writable);
                return traced(Phase::Execute, show).await;
            }
            Some(Prepared::ShowCreateView(view)) => {
                let (catalog, schema, name) = Self::resolve(ctx, view);
                let writable = self.writable(&catalog);
                let show = crate::show_create::create_view(ctx, [&catalog, &schema, &name], writable);
                return traced(Phase::Execute, show).await;
            }
            Some(Prepared::View { .. }) | None => {}
        }
        let plan = traced(Phase::Plan, state.statement_to_plan(statement)).await?;
        statement_policy::check_plan(&plan)?;
        crate::operand_checks::check_plan(&plan)?;
        if ddl::is_routable(&plan)
            && let Some(result) = traced(Phase::Execute, async {
                let columns = NewColumns {
                    declared: new_columns,
                    types: Self::map_types(ctx, maps).await?,
                };
                self.route_ddl(ctx, user, &plan, prepared, &declared, &columns)
                    .await
            })
            .await?
        {
            return Ok(result);
        }
        traced(Phase::Plan, async {
            // Uncorrelated scalar subqueries run here, against the sources.
            let plan = crate::subquery::pre_evaluate_scalar_subqueries(ctx, plan).await?;
            let plan = crate::trino_functions::analyze(ctx, plan)?;
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
    /// like Trino's `CREATE TABLE AS`), and with the rows reserved in the
    /// memory pool for as long as the table exists ([`crate::memory_tables`]).
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
        let reservation = crate::memory_tables::reservation(&create.name.to_string(), &ctx.task_ctx());
        let batches = crate::memory_tables::collect_reserved(physical, ctx.task_ctx(), &reservation).await?;
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
        let table = crate::memory_tables::PooledTable::new(table, reservation);
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
            trino_statement(&mut probe, "UTC")?;
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
        declared: &ddl::DeclaredColumn,
    ) -> Result<DataFrame, DataFusionError> {
        let (catalog, schema, name) = Self::resolve(ctx, table);
        let writable = self.writable(&catalog).ok_or_else(|| {
            DataFusionError::Plan(format!("catalog {catalog} does not support ALTER TABLE"))
        })?;
        check_object_name(ObjectKind::Schema, &schema)?;
        check_object_name(ObjectKind::Table, &name)?;
        check_object_name(ObjectKind::Column, column)?;
        // No writable catalog enforces constraints: refused, never dropped.
        if declared.not_null {
            return Err(DataFusionError::NotImplemented(format!(
                "Catalog '{catalog}' does not support NOT NULL for column '{column}'"
            )));
        }
        if let Some(option) = &declared.other {
            return Err(DataFusionError::NotImplemented(format!(
                "Catalog '{catalog}' does not support column option {option} for column '{column}'"
            )));
        }
        // Let the SQL planner translate the type name exactly as it would in a
        // cast, with Trino's defaults (`timestamp` is `timestamp(3)`).
        let state = ctx.state();
        let dialect = state.config().options().sql_parser.dialect;
        let mut probe = state.sql_to_statement(&format!("SELECT CAST(NULL AS {sql_type}) AS t"), &dialect)?;
        crate::literals::rewrite_timestamp_types(&mut probe, "UTC");
        let probe = state.statement_to_plan(probe).await?;
        let data_type = probe.schema().field(0).data_type().clone();
        let field = declared_field(
            Field::new(column, data_type, true),
            sql_type,
            declared.comment.as_deref(),
        );
        writable.add_column(&schema, &name, &field).await?;
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
            // Names become paths in a writable catalog (see `statement_policy`).
            let object = match kind {
                RelationKind::Table => ObjectKind::Table,
                RelationKind::View => ObjectKind::View,
            };
            check_object_name(ObjectKind::Schema, &target_schema)?;
            check_object_name(object, &target_name)?;
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
        if self.writable(&catalog).is_some() {
            check_object_name(ObjectKind::Schema, &schema)?;
            check_object_name(ObjectKind::Table, &table)?;
        }
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
        // The engine reads `1.5` as a DECIMAL literal (see `Engine::new`).
        let batches = ctx.sql(&format!("SELECT {expr}")).await?.collect().await?;
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

    /// `CREATE [OR REPLACE] TABLE` in a writable catalog: names checked,
    /// column options the catalog cannot keep refused, declared types handed
    /// over with the rows.
    async fn create_in(
        &self,
        ctx: &SessionContext,
        writable: &Arc<dyn WritableCatalog>,
        user: &str,
        create: &datafusion::logical_expr::CreateMemoryTable,
        declared: &[ddl::DeclaredColumn],
        columns: &NewColumns,
    ) -> Result<DataFrame, DataFusionError> {
        let (catalog, schema, table) = Self::resolve(ctx, &create.name);
        check_object_name(ObjectKind::Schema, &schema)?;
        check_object_name(ObjectKind::Table, &table)?;
        if create.if_not_exists && create.or_replace {
            return Err(DataFusionError::Execution(
                "'IF NOT EXISTS' cannot coexist with 'REPLACE'".into(),
            ));
        }
        for column in declared {
            if column.not_null {
                return Err(DataFusionError::NotImplemented(format!(
                    "Catalog '{catalog}' does not support non-null column for column name '{}'",
                    column.name
                )));
            }
            if let Some(option) = &column.other {
                return Err(DataFusionError::NotImplemented(format!(
                    "Catalog '{catalog}' does not support column option {option} for column name '{}'",
                    column.name
                )));
            }
        }
        let input =
            crate::subquery::pre_evaluate_scalar_subqueries(ctx, create.input.as_ref().clone()).await?;
        // Columns of `CREATE TABLE AS` are nullable in Trino, whatever the query.
        let from_query = !matches!(input, LogicalPlan::EmptyRelation(_));
        let rows = ctx.execute_logical_plan(input).await?.execute_stream().await?;
        // The executed plan's schema (the analyzer settles result types after
        // SQL planning), with the types the statement declares.
        let announced = table_schema(&rows.schema(), columns, from_query);
        for field in announced.fields() {
            check_object_name(ObjectKind::Column, field.name())?;
        }
        let rows = with_schema(rows, announced);
        let rows = with_declared_columns(rows, declared)?;
        let written = if create.or_replace {
            writable
                .replace_table_as(&schema, &table, user, rows, ctx.task_ctx())
                .await?
        } else {
            writable
                .create_table_as(&schema, &table, create.if_not_exists, user, rows, ctx.task_ctx())
                .await?
        };
        Self::row_count(ctx, written)
    }

    async fn route_ddl(
        &self,
        ctx: &SessionContext,
        user: &str,
        plan: &LogicalPlan,
        prepared: Option<Prepared>,
        declared: &[ddl::DeclaredColumn],
        columns: &NewColumns,
    ) -> Result<Option<DataFrame>, DataFusionError> {
        let LogicalPlan::Ddl(ddl) = plan else {
            return Ok(None);
        };
        match ddl {
            DdlStatement::CreateMemoryTable(create) => {
                let (catalog, _, _) = Self::resolve(ctx, &create.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Self::create_memory_table(ctx, create, columns).await.map(Some);
                };
                self.create_in(ctx, writable, user, create, declared, columns)
                    .await
                    .map(Some)
            }
            DdlStatement::DropTable(drop) => {
                let (catalog, schema, table) = Self::resolve(ctx, &drop.name);
                let Some(writable) = self.writable(&catalog) else {
                    return Ok(None);
                };
                check_object_name(ObjectKind::Schema, &schema)?;
                check_object_name(ObjectKind::Table, &table)?;
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
                check_object_name(ObjectKind::Schema, &schema)?;
                check_object_name(ObjectKind::View, &name)?;
                for field in view.input.schema().fields() {
                    check_object_name(ObjectKind::Column, field.name())?;
                }
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
                check_object_name(ObjectKind::Schema, &schema)?;
                check_object_name(ObjectKind::View, &name)?;
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
                check_object_name(ObjectKind::Schema, schema)?;
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
                connector.register_object_stores(&self.runtime.env());
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

    /// Most bytes the memory pool held at once since the engine started or
    /// the last [`Engine::reset_memory_peak`].
    pub fn memory_peak(&self) -> u64 {
        self.runtime.peak_reserved()
    }

    /// Restarts [`Engine::memory_peak`] from what is reserved now.
    pub fn reset_memory_peak(&self) {
        self.runtime.reset_peak();
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

    /// Every rebuild of the session state keeps the engine's functions, not
    /// the built-ins they replaced: `extract(year …)` stays Trino's
    /// `bigint`, however the rebuild orders the function maps (a distributed
    /// worker rebuilds the state with each task's configuration).
    #[tokio::test]
    async fn a_rebuilt_state_keeps_the_functions_the_engine_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let base = engine.context().state();
        for _ in 0..32 {
            let mut config = base.config().clone();
            config.options_mut().execution.time_zone = Some("America/Sao_Paulo".to_owned());
            config.options_mut().execution.batch_size = 1024;
            let rebuilt = rebuild_state(&base, |builder| builder.with_config(config));
            let ctx = SessionContext::new_with_state(rebuilt);
            let df = ctx
                .sql("SELECT date_part('year', DATE '2020-05-01') AS y")
                .await
                .unwrap();
            assert_eq!(df.schema().field(0).data_type(), &DataType::Int64);
        }
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
        assert_eq!(
            names[projections + 1],
            crate::federation_policy::PRUNE_FEDERATED_COLUMNS,
            "{names:?}"
        );
        let distinct = names
            .iter()
            .position(|n| n == "single_distinct_aggregation_to_group_by")
            .unwrap();
        assert_eq!(
            names[distinct - 1],
            crate::distinct_counts::DISTINCT_COUNT_TO_GROUP_BY,
            "{names:?}"
        );
    }

    #[test]
    fn federation_guard_runs_before_filter_pushdown() {
        let names: Vec<String> = physical_rules().iter().map(|r| r.name().to_owned()).collect();
        let guard = names.iter().position(|n| n == "federation_filter_guard").unwrap();
        let pushdown = names.iter().position(|n| n.contains("FilterPushdown")).unwrap();
        assert!(guard < pushdown, "{names:?}");
    }

    #[test]
    fn float_zeros_run_right_before_the_simplifier() {
        let names: Vec<String> = optimizer_rules().iter().map(|r| r.name().to_owned()).collect();
        let zeros = names
            .iter()
            .position(|n| n == crate::trino_functions::FLOAT_ZEROS)
            .unwrap();
        assert_eq!(names[zeros + 1], "simplify_expressions", "{names:?}");
        assert!(
            !names[..zeros].iter().any(|n| n == "simplify_expressions"),
            "{names:?}"
        );
    }

    #[test]
    fn null_safe_join_guard_runs_after_join_selection_right_before_dynamic_filters() {
        let names: Vec<String> = physical_rules().iter().map(|r| r.name().to_owned()).collect();
        let guard = names
            .iter()
            .position(|n| n == crate::join_guard::NULL_SAFE_JOIN_GUARD)
            .unwrap();
        let selection = names.iter().position(|n| n == "join_selection").unwrap();
        assert!(selection < guard, "{names:?}");
        assert_eq!(names[guard + 1], "FilterPushdown(Post)", "{names:?}");
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

    /// Trino has neither statement: files are reached through catalogs only
    /// (F2 red-team). Runtime settings belong to the server.
    #[tokio::test]
    async fn client_statements_never_reach_server_files() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let target = dir.path().join("written.csv");
        let file = target.display();
        for sql in [
            format!("COPY (SELECT 1 AS a) TO '{file}' STORED AS CSV"),
            format!("COPY (SELECT 1 AS a) TO '{file}'"),
            format!("EXPLAIN ANALYZE COPY (SELECT 1 AS a) TO '{file}'"),
            "CREATE EXTERNAL TABLE leak (line VARCHAR) STORED AS CSV LOCATION '/etc/hostname'".to_owned(),
            "SET datafusion.runtime.memory_limit = '100G'".to_owned(),
            format!(
                "SET datafusion.runtime.temp_directory = '{}'",
                dir.path().display()
            ),
            "RESET datafusion.runtime.memory_limit".to_owned(),
        ] {
            let err = match engine.sql(&sql).await {
                Ok(frame) => frame.collect().await.map(|_| ()).unwrap_err(),
                Err(err) => err,
            };
            assert!(matches!(err, DataFusionError::NotImplemented(_)), "{sql}: {err}");
        }
        assert!(!target.exists(), "a client statement wrote a server file");
        let err = engine.sql("SELECT 1 AS \"\"").await.unwrap_err();
        assert!(matches!(err, DataFusionError::SQL(..)), "{err}");
        assert!(
            engine
                .sql("SET datafusion.execution.batch_size = 1024")
                .await
                .is_ok()
        );
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
        // Without a zone: no zone, at the precision of its digits
        // (`timestamp(3)` in Trino 431; typed by the function library).
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    /// `CREATE TABLE AS` stores the Trino result types the function library
    /// gives (`decimal(10,2) * decimal(10,2)` is `decimal(20,4)` in Trino 431)
    /// and the rows the query returns.
    #[tokio::test]
    async fn create_table_as_keeps_trino_result_types() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Decimal128Type;
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        let select = "SELECT CAST(1.50 AS DECIMAL(10,2)) * CAST(2.25 AS DECIMAL(10,2)) AS m";
        let direct = engine.sql(select).await.unwrap().collect().await.unwrap();
        assert_eq!(
            direct[0].schema().field(0).data_type(),
            &DataType::Decimal128(20, 4)
        );
        engine
            .sql(&format!(
                "CREATE TABLE datafusion.public.ctas_decimal AS {select}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let stored = engine
            .sql("SELECT m FROM datafusion.public.ctas_decimal")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            stored[0].schema().field(0).data_type(),
            &DataType::Decimal128(20, 4)
        );
        assert_eq!(
            stored[0].column(0).as_primitive::<Decimal128Type>().value(0),
            33_750
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
        async fn add_column(&self, _: &str, _: &str, _: &Field) -> datafusion::error::Result<()> {
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
            let defined = datafusion::catalog::view::ViewTable::new(
                LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: true,
                    schema: Arc::new(datafusion::common::DFSchema::empty()),
                }),
                Some("SELECT 1 AS x".into()),
            );
            schema.register_table("w".into(), Arc::new(defined)).unwrap();
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
        // A target name that is not a directory name is refused (owner
        // decision: '/' and '\' are never part of a name), nothing renamed.
        for (sql, noun) in [
            ("ALTER TABLE t RENAME TO \"a/b\"", "table"),
            ("ALTER VIEW v RENAME TO \"a\\b\"", "view"),
        ] {
            let error = engine.sql_with(&options, sql).await.unwrap_err();
            assert!(
                error.to_string().contains(&format!("Invalid {noun} name")),
                "{sql}: {error}"
            );
        }
        assert_eq!(recorder.calls.lock().unwrap().len(), 5);
    }

    /// A writable catalog without table properties or view security of its
    /// own: no `WITH` clause, `DEFINER` views.
    #[tokio::test]
    async fn writable_catalogs_show_create_with_the_defaults() {
        use datafusion::arrow::array::AsArray;
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), ByteSize::mib(64), ByteSize::mib(64));
        engine
            .attach(Arc::new(WritableSource(Arc::default())), true, FAST)
            .await
            .unwrap();
        for (sql, expected) in [
            (
                "SHOW CREATE TABLE lake.raw.t",
                "CREATE TABLE lake.raw.t (\n   a bigint\n)",
            ),
            (
                "SHOW CREATE VIEW lake.raw.w",
                "CREATE VIEW lake.raw.w SECURITY DEFINER AS\nSELECT 1 x",
            ),
        ] {
            let batches = run(&engine, sql).await.unwrap();
            let text = batches[0].column(0).as_string::<i32>().value(0).to_owned();
            assert_eq!(text, expected, "{sql}");
        }
        let error = message(run(&engine, "SHOW CREATE VIEW lake.raw.v").await);
        assert!(error.contains("View 'lake.raw.v' has no definition"), "{error}");
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
        async fn add_column(&self, _: &str, _: &str, _: &Field) -> datafusion::error::Result<()> {
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
        // ISS-020: the IEC names reach the catalog as written.
        engine
            .sql_with(
                &options,
                "ALTER TABLE t EXECUTE optimize(file_size_threshold => '1.5MiB')",
            )
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
                expected("2kB", "true"),
                expected("1.5MiB", "true")
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
