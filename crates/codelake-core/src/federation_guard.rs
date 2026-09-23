//! Correctness guard for federated scans.
//!
//! **Defect it prevents (found by the MySQL × PostgreSQL integration test):**
//! `datafusion-federation` 0.5.3 answers `PushedDown::Yes` to every filter the
//! `DataFusion` 53 physical `FilterPushdown` rule offers to a
//! `VirtualExecutionPlan`, and removes the `FilterExec` above it. The SQL
//! executors of `datafusion-table-providers` then *ignore* those filters, so the
//! predicate is silently lost and the query returns wrong rows. It happens when
//! a filter stays above the federated node, e.g. in joins across two sources.
//!
//! The guard wraps every federated scan in [`FederationFilterBarrier`], a
//! pass-through node that refuses parent filters. The `FilterExec` stays in the
//! plan and runs in the engine. Logical pushdown (the predicate rendered into
//! the SQL sent to the source) is unaffected, because it happens before
//! physical planning.
//!
//! Remove only when an upstream fix is released **and** the integration test
//! `mysql::joins_across_mysql_and_postgres` passes without this rule.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Detects the remote-SQL leaf of `datafusion-federation` by type, not by name:
/// a renamed string upstream would silently disable the guard, a changed type
/// fails to compile.
fn is_federated_scan(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any()
        .is::<datafusion_federation::sql::VirtualExecutionPlan>()
}

/// Wraps every node matching `is_target` in a [`FederationFilterBarrier`].
fn guard_matching(
    plan: Arc<dyn ExecutionPlan>,
    is_target: &dyn Fn(&dyn ExecutionPlan) -> bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        if is_target(node.as_ref()) {
            let barrier: Arc<dyn ExecutionPlan> = Arc::new(FederationFilterBarrier::new(node));
            Ok(Transformed::yes(barrier))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .data()
}

/// Physical optimizer rule that inserts [`FederationFilterBarrier`] above every
/// federated scan. Must run before `FilterPushdown`.
#[derive(Debug, Default)]
pub struct FederationFilterGuard;

impl PhysicalOptimizerRule for FederationFilterGuard {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        guard_matching(plan, &is_federated_scan)
    }

    fn name(&self) -> &'static str {
        "federation_filter_guard"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Transparent node whose only behaviour is the trait default for filter
/// pushdown: every parent filter is reported as unsupported.
#[derive(Debug)]
pub struct FederationFilterBarrier {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl FederationFilterBarrier {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for FederationFilterBarrier {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FederationFilterBarrier")
    }
}

impl ExecutionPlan for FederationFilterBarrier {
    fn name(&self) -> &'static str {
        "FederationFilterBarrier"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "FederationFilterBarrier expects one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(child)))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
    }

    #[test]
    fn wraps_matching_nodes_only() {
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        let guarded = guard_matching(Arc::clone(&leaf), &|p| p.name() == "EmptyExec").unwrap();
        assert_eq!(guarded.name(), "FederationFilterBarrier");
        assert_eq!(guarded.children()[0].name(), "EmptyExec");

        let untouched = guard_matching(leaf, &|_| false).unwrap();
        assert_eq!(untouched.name(), "EmptyExec");
    }

    #[test]
    fn rule_leaves_non_federated_plans_alone() {
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        let out = FederationFilterGuard
            .optimize(leaf, &ConfigOptions::default())
            .unwrap();
        assert_eq!(out.name(), "EmptyExec");
        assert!(!is_federated_scan(out.as_ref()));
    }

    #[test]
    fn barrier_refuses_every_parent_filter() {
        use datafusion::physical_expr::expressions::lit;
        use datafusion::physical_plan::filter_pushdown::{FilterPushdownPhase, PushedDown};

        let barrier = FederationFilterBarrier::new(Arc::new(EmptyExec::new(schema())));
        let description = barrier
            .gather_filters_for_pushdown(
                FilterPushdownPhase::Pre,
                vec![lit(true)],
                &ConfigOptions::default(),
            )
            .unwrap();
        let child = &description.parent_filters()[0];
        assert!(child.iter().all(|f| matches!(f.discriminant, PushedDown::No)));
    }

    /// Reproduces the upstream defect without Docker: a federated "remote" whose
    /// executor, like the real `datafusion-table-providers` executors, ignores the
    /// `filters` argument and returns whatever its SQL selects.
    mod upstream_defect {
        use super::*;
        use async_trait::async_trait;
        use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::SchemaRef;
        use datafusion::catalog::TableProvider;
        use datafusion::execution::SessionStateBuilder;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
        use datafusion::physical_plan::memory::MemoryStream;
        use datafusion::prelude::SessionContext;
        use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
        use datafusion_federation::FederatedTableProviderAdaptor;
        use datafusion_federation::sql::{
            RemoteTable, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
        };
        use std::collections::HashMap;

        #[derive(Debug)]
        struct IgnoresFilters {
            context: &'static str,
            columns: HashMap<&'static str, ArrayRef>,
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
            fn execute(
                &self,
                _query: &str,
                schema: SchemaRef,
                _filters: &[Arc<dyn PhysicalExpr>],
            ) -> Result<SendableRecordBatchStream> {
                let arrays = schema
                    .fields()
                    .iter()
                    .map(|f| Arc::clone(&self.columns[f.name().as_str()]))
                    .collect();
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

        fn remote(
            context: &'static str,
            table: &str,
            columns: Vec<(&'static str, ArrayRef)>,
        ) -> Arc<dyn TableProvider> {
            let schema = Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
                    .collect::<Vec<_>>(),
            ));
            let executor = Arc::new(IgnoresFilters {
                context,
                columns: columns.into_iter().collect(),
            });
            let table = Arc::new(RemoteTable::new(
                RemoteTableRef::try_from(table.to_owned()).unwrap(),
                schema,
            ));
            let provider = Arc::new(SQLFederationProvider::new(executor));
            Arc::new(FederatedTableProviderAdaptor::new(Arc::new(
                SQLTableSource::new_with_table(provider, table),
            )))
        }

        fn session(guarded: bool) -> SessionContext {
            let mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> = Vec::new();
            if guarded {
                rules.push(Arc::new(FederationFilterGuard));
            }
            rules.extend(PhysicalOptimizer::new().rules);
            let state = SessionStateBuilder::new()
                .with_default_features()
                .with_optimizer_rules(datafusion_federation::default_optimizer_rules())
                .with_query_planner(Arc::new(datafusion_federation::FederatedQueryPlanner::new()))
                .with_physical_optimizer_rules(rules)
                .build();
            let ctx = SessionContext::new_with_state(state);
            ctx.register_table(
                "customers",
                remote(
                    "erp",
                    "customers",
                    vec![
                        ("id", Arc::new(Int64Array::from(vec![1, 2]))),
                        ("name", Arc::new(StringArray::from(vec!["Ana", "Bruno"]))),
                    ],
                ),
            )
            .unwrap();
            ctx.register_table(
                "contracts",
                remote(
                    "crm",
                    "contracts",
                    vec![
                        ("customer_id", Arc::new(Int64Array::from(vec![1, 2, 1]))),
                        (
                            "status",
                            Arc::new(StringArray::from(vec!["active", "cancelled", "active"])),
                        ),
                    ],
                ),
            )
            .unwrap();
            ctx
        }

        async fn active_customers(ctx: &SessionContext) -> Vec<String> {
            let batches = ctx
                .sql(
                    "SELECT DISTINCT c.name FROM customers c JOIN contracts k ON k.customer_id = c.id \
                     WHERE k.status = 'active' ORDER BY c.name",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches
                .iter()
                .flat_map(|b| {
                    let names = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    names.iter().flatten().map(str::to_owned).collect::<Vec<_>>()
                })
                .collect()
        }

        #[tokio::test]
        async fn guard_keeps_cross_source_filters() {
            assert_eq!(active_customers(&session(true)).await, vec!["Ana"]);
        }

        /// Documents the upstream defect. When this starts failing, upstream fixed
        /// it: re-run the integration suite without the guard and remove it.
        #[tokio::test]
        async fn upstream_still_drops_filters_without_guard() {
            assert_eq!(
                active_customers(&session(false)).await,
                vec!["Ana", "Bruno"],
                "upstream behaviour changed: see module docs before removing the guard"
            );
        }
    }

    #[test]
    fn barrier_rejects_wrong_arity() {
        let barrier = Arc::new(FederationFilterBarrier::new(Arc::new(EmptyExec::new(schema()))));
        assert!(barrier.with_new_children(vec![]).is_err());
    }
}
