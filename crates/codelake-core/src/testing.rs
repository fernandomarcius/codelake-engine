//! Test doubles shared by unit tests. A "remote" table here behaves like the
//! real `datafusion-table-providers` executors: it ignores the `filters`
//! argument and returns whatever rows it holds, so any predicate the engine
//! forgets to apply shows up as a wrong result.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::{
    LogicalOptimizer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};

use crate::federation_policy::{PushdownPolicy, restrict_federation};

pub(crate) fn int64(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

pub(crate) fn strings(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Executor that ignores the SQL it receives and returns its whole table,
/// recording every statement so tests can assert what was pushed down.
#[derive(Debug)]
pub(crate) struct IgnoresFilters {
    context: &'static str,
    columns: HashMap<&'static str, ArrayRef>,
    policy: Option<Arc<dyn PushdownPolicy>>,
    pub(crate) statements: Arc<Mutex<Vec<String>>>,
    /// [`crate::federation_guard::remote_time_zone`] at each `execute`.
    pub(crate) zones: Arc<Mutex<Vec<Option<String>>>>,
}

#[async_trait]
impl SQLExecutor for IgnoresFilters {
    fn name(&self) -> &'static str {
        "ignores_filters"
    }
    fn compute_context(&self) -> Option<String> {
        Some(self.context.to_owned())
    }
    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }
    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        let policy = Arc::clone(self.policy.as_ref()?);
        Some(Box::new(move |plan: LogicalPlan| {
            restrict_federation(plan, policy.as_ref())
        }))
    }
    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        if let Ok(mut statements) = self.statements.lock() {
            statements.push(query.to_owned());
        }
        if let Ok(mut zones) = self.zones.lock() {
            zones.push(crate::federation_guard::remote_time_zone());
        }
        let arrays = schema
            .fields()
            .iter()
            .map(|f| {
                self.columns.get(f.name().as_str()).cloned().ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "fake remote can only return plain columns, got `{}` for: {query}",
                        f.name()
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }
    async fn table_names(&self) -> Result<Vec<String>> {
        Ok(vec![])
    }
    async fn get_table_schema(&self, table: &str) -> Result<SchemaRef> {
        Err(DataFusionError::Plan(format!("unknown table {table}")))
    }
}

/// A federated table backed by [`IgnoresFilters`].
pub(crate) fn remote_table(
    context: &'static str,
    table: &str,
    columns: Vec<(&'static str, ArrayRef)>,
    policy: Option<Arc<dyn PushdownPolicy>>,
) -> Arc<dyn TableProvider> {
    remote_table_recording(context, table, columns, policy).0
}

pub(crate) fn remote_table_recording(
    context: &'static str,
    table: &str,
    columns: Vec<(&'static str, ArrayRef)>,
    policy: Option<Arc<dyn PushdownPolicy>>,
) -> (Arc<dyn TableProvider>, Arc<Mutex<Vec<String>>>) {
    let (provider, statements, _) = remote_table_observed(context, table, columns, policy);
    (provider, statements)
}

/// The zones [`crate::federation_guard::remote_time_zone`] answered each time
/// the executor started.
pub(crate) type StartZones = Arc<Mutex<Vec<Option<String>>>>;

/// [`remote_table_recording`], also recording the zone of each start.
pub(crate) fn remote_table_observed(
    context: &'static str,
    table: &str,
    columns: Vec<(&'static str, ArrayRef)>,
    policy: Option<Arc<dyn PushdownPolicy>>,
) -> (Arc<dyn TableProvider>, Arc<Mutex<Vec<String>>>, StartZones) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), array.null_count() > 0))
            .collect::<Vec<_>>(),
    ));
    let statements = Arc::new(Mutex::new(Vec::new()));
    let zones: StartZones = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(IgnoresFilters {
        context,
        columns: columns.into_iter().collect(),
        policy,
        statements: Arc::clone(&statements),
        zones: Arc::clone(&zones),
    });
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let source = SQLTableSource::new_with_schema(
        provider,
        RemoteTableRef::from(TableReference::bare(table)),
        schema,
    );
    (
        Arc::new(FederatedTableProviderAdaptor::new(Arc::new(source))),
        statements,
        zones,
    )
}

/// The statements a fake source received.
pub(crate) type Statements = Arc<Mutex<Vec<String>>>;

/// Several remote tables of one source behind one executor, so a join
/// between them can be federated whole: the executor records each
/// statement and answers with the columns it holds by name (a join's
/// output only when its columns have one length).
pub(crate) fn remote_tables_recording(
    context: &'static str,
    tables: Vec<(&str, Vec<(&'static str, ArrayRef)>)>,
    policy: Option<Arc<dyn PushdownPolicy>>,
) -> (Vec<Arc<dyn TableProvider>>, Statements) {
    let statements = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(IgnoresFilters {
        context,
        columns: tables
            .iter()
            .flat_map(|(_, columns)| columns.iter().cloned())
            .collect(),
        policy,
        statements: Arc::clone(&statements),
        zones: Arc::new(Mutex::new(Vec::new())),
    });
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let providers = tables
        .into_iter()
        .map(|(table, columns)| {
            let schema = Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let source = SQLTableSource::new_with_schema(
                Arc::clone(&provider),
                RemoteTableRef::from(TableReference::bare(table)),
                schema,
            );
            Arc::new(FederatedTableProviderAdaptor::new(Arc::new(source))) as Arc<dyn TableProvider>
        })
        .collect();
    (providers, statements)
}

/// Executor with no data, for building planners in plan-rewrite tests.
pub(crate) fn executor(context: &'static str) -> Arc<dyn SQLExecutor> {
    Arc::new(IgnoresFilters {
        context,
        columns: HashMap::new(),
        policy: None,
        statements: Arc::new(Mutex::new(Vec::new())),
        zones: Arc::new(Mutex::new(Vec::new())),
    })
}

/// Session configured like the engine: federation rules plus the filter guard.
pub(crate) fn federated_session() -> datafusion::prelude::SessionContext {
    use datafusion::execution::SessionStateBuilder;

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_analyzer_rules(crate::engine::analyzer_rules())
        .with_optimizer_rules(crate::engine::optimizer_rules())
        .with_query_planner(crate::engine::query_planner())
        .with_physical_optimizer_rules(crate::engine::physical_rules())
        .build();
    datafusion::prelude::SessionContext::new_with_state(state)
}
