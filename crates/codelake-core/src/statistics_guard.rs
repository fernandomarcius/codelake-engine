//! Correctness guard for answering aggregates from statistics.
//!
//! **Defect it prevents (found by the hive statistics acceptance suite):**
//! `DataFusion` 53 answers `count(*)`, `count(col)`, `min` and `max` from
//! statistics (`AggregateStatistics`) when the aggregate's input reports an
//! `Exact` row count, and several operators turn an estimate into an `Exact`
//! count on the way up:
//! - `Statistics::with_fetch`, called by limits, sorts, coalesces and hash
//!   joins, reads an `Inexact` count as an upper bound: an estimate of 0
//!   rows, or of fewer rows than an `OFFSET` skips, becomes `Exact(0)`;
//! - the join cardinality estimate is 0 when the inputs' `Inexact` bounds do
//!   not overlap, and the hash join then passes it through `with_fetch`.
//!
//! A source whose statistics are estimates (a stale metastore, a sampled
//! listing) can then make `count(*)` return 0 over rows that exist.
//!
//! The guard runs `DataFusion`'s rule only over subtrees whose every source
//! reports an `Exact` row count, so an answer from statistics derives from
//! exact counts only. Plans over estimated sources keep their aggregates and
//! compute them from the rows; the estimates still steer the plan.
//!
//! Remove only when upstream no longer promotes an `Inexact` count to `Exact`
//! **and** the `hive_statistics_acceptance` defect tests pass without it.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_plan::ExecutionPlan;

/// Name of the upstream rule this guard replaces in the rule list.
pub const REPLACED_RULE: &str = "aggregate_statistics";

/// `AggregateStatistics` restricted to subtrees over exact sources.
#[derive(Debug, Default)]
pub struct ExactSourceAggregateStatistics;

impl PhysicalOptimizerRule for ExactSourceAggregateStatistics {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if exact_sources(plan.as_ref()) {
            return AggregateStatistics::new().optimize(plan, config);
        }
        plan.map_children(|child| self.optimize(child, config).map(Transformed::yes))
            .data()
    }

    fn name(&self) -> &'static str {
        "aggregate_statistics_over_exact_sources"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether every leaf under `plan` reports an exact row count. A leaf whose
/// statistics fail counts as inexact.
fn exact_sources(plan: &dyn ExecutionPlan) -> bool {
    let children = plan.children();
    if children.is_empty() {
        return plan
            .partition_statistics(None)
            .is_ok_and(|stats| matches!(stats.num_rows, Precision::Exact(_)));
    }
    children.into_iter().all(|child| exact_sources(child.as_ref()))
}

/// `rules` with `DataFusion`'s `AggregateStatistics` replaced by the guard,
/// in the same position.
pub fn guard_aggregate_statistics(
    mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    for rule in &mut rules {
        if rule.name() == REPLACED_RULE {
            *rule = Arc::new(ExactSourceAggregateStatistics);
        }
    }
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::fmt;

    use async_trait::async_trait;
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::common::Statistics;
    use datafusion::datasource::TableType;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
    use datafusion::logical_expr::Expr;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
    use datafusion::prelude::SessionContext;

    /// A source leaf that reports `rows` as its row count, whatever it holds.
    #[derive(Debug)]
    struct Reported {
        input: Arc<dyn ExecutionPlan>,
        rows: Precision<usize>,
    }

    impl DisplayAs for Reported {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Reported")
        }
    }

    impl ExecutionPlan for Reported {
        fn name(&self) -> &'static str {
            "Reported"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            Vec::new()
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            self.input.execute(partition, context)
        }

        fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
            let mut stats = Statistics::new_unknown(&self.input.schema());
            stats.num_rows = self.rows;
            Ok(stats)
        }
    }

    /// Table `x BIGINT` with 50 rows (0..49) that reports `rows`.
    #[derive(Debug)]
    struct Table {
        rows: Precision<usize>,
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    #[async_trait]
    impl TableProvider for Table {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            schema()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from_iter_values(0..50))])?;
            let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), projection.cloned())?;
            Ok(Arc::new(Reported {
                input,
                rows: self.rows,
            }))
        }
    }

    fn session(guarded: bool, rows: Precision<usize>) -> SessionContext {
        let rules = PhysicalOptimizer::new().rules;
        let rules = if guarded {
            guard_aggregate_statistics(rules)
        } else {
            rules
        };
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", Arc::new(Table { rows })).unwrap();
        ctx
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let column = batches[0].column(0);
        let counts = column.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.len(), 1);
        counts.value(0)
    }

    async fn plan(ctx: &SessionContext, sql: &str) -> String {
        let physical = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        datafusion::physical_plan::displayable(physical.as_ref())
            .indent(false)
            .to_string()
    }

    const OFFSET: &str = "SELECT count(*) FROM (SELECT x FROM t OFFSET 5) s";
    const ALL: &str = "SELECT count(*) FROM t";
    const LIMIT: &str = "SELECT count(*) FROM (SELECT x FROM t LIMIT 100) s";
    const DISJOINT: &str = "SELECT count(*) FROM t a JOIN t b ON a.x = b.x WHERE a.x > 3";

    #[tokio::test]
    async fn estimates_never_answer_an_aggregate() {
        for rows in [Precision::Inexact(0), Precision::Inexact(1), Precision::Absent] {
            let ctx = session(true, rows);
            assert_eq!(count(&ctx, ALL).await, 50, "{rows:?}");
            assert_eq!(count(&ctx, LIMIT).await, 50, "{rows:?}");
            assert_eq!(count(&ctx, OFFSET).await, 45, "{rows:?}");
            assert_eq!(count(&ctx, DISJOINT).await, 46, "{rows:?}");
            assert!(plan(&ctx, ALL).await.contains("AggregateExec"), "{rows:?}");
        }
    }

    #[tokio::test]
    async fn exact_sources_are_still_answered_from_statistics() {
        let ctx = session(true, Precision::Exact(50));
        let plan = plan(&ctx, ALL).await;
        assert!(!plan.contains("AggregateExec"), "{plan}");
        assert!(plan.contains("PlaceholderRowExec"), "{plan}");
        assert_eq!(count(&ctx, ALL).await, 50);
        assert_eq!(count(&ctx, OFFSET).await, 45);
    }

    /// Documents the upstream promotion the guard exists for; when this
    /// fails, upstream fixed it and the guard can go (see the module docs).
    #[tokio::test]
    async fn upstream_answers_from_an_estimate_promoted_to_exact() {
        assert_eq!(count(&session(false, Precision::Inexact(0)), LIMIT).await, 0);
        assert_eq!(count(&session(false, Precision::Inexact(1)), OFFSET).await, 0);
    }

    #[test]
    fn the_guard_takes_the_upstream_rule_place() {
        let upstream = PhysicalOptimizer::new().rules;
        let guarded = guard_aggregate_statistics(PhysicalOptimizer::new().rules);
        assert_eq!(upstream.len(), guarded.len());
        let position = |rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>], name: &str| {
            rules.iter().position(|r| r.name() == name)
        };
        assert!(position(&guarded, REPLACED_RULE).is_none());
        assert_eq!(
            position(&upstream, REPLACED_RULE),
            position(&guarded, ExactSourceAggregateStatistics.name())
        );
    }

    #[test]
    fn a_leaf_whose_statistics_fail_is_not_exact() {
        #[derive(Debug)]
        struct Failing(Arc<dyn ExecutionPlan>);
        impl DisplayAs for Failing {
            fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "Failing")
            }
        }
        impl ExecutionPlan for Failing {
            fn name(&self) -> &'static str {
                "Failing"
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn properties(&self) -> &Arc<PlanProperties> {
                self.0.properties()
            }
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                Vec::new()
            }
            fn with_new_children(
                self: Arc<Self>,
                _children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(self)
            }
            fn execute(
                &self,
                partition: usize,
                context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                self.0.execute(partition, context)
            }
            fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
                Err(datafusion::error::DataFusionError::Internal(
                    "no statistics".into(),
                ))
            }
        }
        let empty: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(schema()));
        assert!(exact_sources(empty.as_ref()));
        assert!(!exact_sources(&Failing(empty)));
    }
}
