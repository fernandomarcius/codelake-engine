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
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let statements = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(IgnoresFilters {
        context,
        columns: columns.into_iter().collect(),
        policy,
        statements: Arc::clone(&statements),
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
    )
}

/// Executor with no data, for building planners in plan-rewrite tests.
pub(crate) fn executor(context: &'static str) -> Arc<dyn SQLExecutor> {
    Arc::new(IgnoresFilters {
        context,
        columns: HashMap::new(),
        policy: None,
        statements: Arc::new(Mutex::new(Vec::new())),
    })
}

/// Session configured like the engine: federation rules plus the filter guard.
pub(crate) fn federated_session() -> datafusion::prelude::SessionContext {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;

    let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
        vec![Arc::new(crate::federation_guard::FederationFilterGuard)];
    rules.extend(PhysicalOptimizer::new().rules);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rules(datafusion_federation::default_optimizer_rules())
        .with_query_planner(Arc::new(datafusion_federation::FederatedQueryPlanner::new()))
        .with_physical_optimizer_rules(rules)
        .build();
    datafusion::prelude::SessionContext::new_with_state(state)
}
