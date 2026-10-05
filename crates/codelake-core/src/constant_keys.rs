//! Aggregations grouped by a column a filter fixed run as unordered ones
//! (ADR 0003, amended 2026-10-02).
//!
//! `DataFusion` treats a column that a filter fixes (`WHERE t = 2`, a join
//! on `t = 2`) as sorted, and an aggregation whose `GROUP BY` holds such a
//! column as reading input sorted on part of its keys
//! (`ordering_mode=PartiallySorted`). It then expects groups to close as that
//! part of the key changes, which a constant never does, and handles running
//! out of memory accordingly: the partial aggregation, which emits its groups
//! early when refused memory, emits only closed groups, so it holds every
//! group until the pool refuses it and fails the query
//! (`EXCEEDED_LOCAL_MEMORY_LIMIT` with `can spill: true`). The engine's other
//! defences skip it too: plain strings ([`crate::plain_views`]) leave out an
//! aggregation over sorted input, which never spills.
//!
//! Shadow run, 2026-10-02: the `Warehouse` commission view grouped 4.78 million
//! rows by 17 columns, one of them `bancotipoid`, which its join fixes at
//! 2; ordered by every column with `LIMIT 50` it failed after 20-30 s under a
//! 4 GiB pool (`GroupedHashAggregateStream` at its share, 381 KB left);
//! Trino answered in 34 s.
//!
//! [`UnorderedConstantKeys`] gives such an aggregation (the part of its key
//! it reads sorted made only of constants) an input whose properties say
//! nothing ([`ForgetOrderExec`]): the aggregation is planned again as
//! unordered, emits early and spills. The rows are the same; an ordering on
//! constant columns holds whatever the order.

use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, InputOrderMode, PlanProperties,
};

/// Name of [`UnorderedConstantKeys`].
pub const UNORDERED_CONSTANT_KEYS: &str = "codelake_unordered_constant_keys";

/// Whether `aggregate` reads its input sorted on part of its keys only
/// because that part is constant.
pub fn ordered_by_constants(aggregate: &AggregateExec) -> bool {
    let InputOrderMode::PartiallySorted(indices) = aggregate.input_order_mode() else {
        return false;
    };
    let keys = aggregate.group_expr().input_exprs();
    let input = aggregate.input().equivalence_properties();
    !indices.is_empty()
        && indices.iter().all(|index| {
            keys.get(*index)
                .is_some_and(|key| input.is_expr_constant(key).is_some())
        })
}

/// Physical rule: see the module documentation.
#[derive(Debug, Default)]
pub struct UnorderedConstantKeys;

impl PhysicalOptimizerRule for UnorderedConstantKeys {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(aggregate) = node.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !ordered_by_constants(aggregate) {
                return Ok(Transformed::no(node));
            }
            tracing::trace!(mode = ?aggregate.mode(), "aggregation over constant keys planned unordered");
            let input: Arc<dyn ExecutionPlan> = Arc::new(ForgetOrderExec::new(Arc::clone(aggregate.input())));
            Ok(Transformed::yes(
                Arc::clone(&node).with_new_children(vec![input])?,
            ))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        UNORDERED_CONSTANT_KEYS
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Its input, with properties that say nothing of its order or constants
/// (its partitioning kept).
#[derive(Debug)]
pub struct ForgetOrderExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl ForgetOrderExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self { input, properties }
    }
}

impl DisplayAs for ForgetOrderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ForgetOrderExec")
    }
}

impl ExecutionPlan for ForgetOrderExec {
    fn name(&self) -> &'static str {
        "ForgetOrderExec"
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
        let [input] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "ForgetOrderExec expects one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(input)))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::physical_plan::displayable;

    fn engine(dir: &std::path::Path, limit: crate::ByteSize) -> crate::Engine {
        let memory = crate::config::MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: crate::ByteSize::gib(2),
            metadata_cache_limit: crate::ByteSize::mib(1),
        };
        let execution = crate::config::ExecutionConfig {
            target_partitions: Some(2),
            ..crate::config::ExecutionConfig::default()
        };
        crate::Engine::new(&memory, &execution).unwrap()
    }

    /// 400 000 groups of ~110-byte keys, one of them fixed by the filter,
    /// under a 32 MiB pool (2 partitions): planned as an aggregation over input sorted on
    /// that key, the partial aggregation could not emit early and failed;
    /// now unordered, it answers the rows of a pool that fits it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_aggregation_by_a_fixed_column_spills() {
        let dir = tempfile::tempdir().unwrap();
        let small = engine(dir.path(), crate::ByteSize::mib(32));
        let large = engine(dir.path(), crate::ByteSize::gib(2));
        let sql = "SELECT count(*), sum(n), sum(length(k)), min(t), max(t) FROM (\
                   SELECT k, t, count(*) AS n FROM (\
                     SELECT lpad(CAST(value % 400000 AS VARCHAR), 110, 'k') AS k, value % 3 AS t \
                     FROM generate_series(1, 1200000)) WHERE t = 2 GROUP BY k, t)";
        let mut answers = Vec::new();
        for engine in [&small, &large] {
            let frame = engine.sql(sql).await.unwrap();
            let plan = frame.clone().create_physical_plan().await.unwrap();
            let text = displayable(plan.as_ref()).indent(true).to_string();
            assert!(!text.contains("ordering_mode=PartiallySorted"), "{text}");
            assert!(
                text.contains("ForgetOrderExec"),
                "vacuous: no aggregation over a fixed key\n{text}"
            );
            let batches = frame.collect().await.unwrap();
            answers.push(
                batches[0]
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c, 0).unwrap())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(answers[0], ["400000", "400000", "44000000", "2", "2"]);
        assert_eq!(answers[0], answers[1]);
    }

    /// Without the rule, the same plan reads the fixed key as sorted: what
    /// the rule exists for.
    #[tokio::test]
    async fn a_fixed_key_reads_as_sorted_without_the_rule() {
        let ctx = datafusion::prelude::SessionContext::new();
        let sql = "SELECT k, t, count(*) FROM (SELECT CAST(value AS VARCHAR) AS k, value % 3 AS t \
                   FROM generate_series(1, 100)) WHERE t = 2 GROUP BY k, t";
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert!(text.contains("ordering_mode=PartiallySorted"), "{text}");
        let fixed = UnorderedConstantKeys
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        let text = displayable(fixed.as_ref()).indent(true).to_string();
        assert!(!text.contains("ordering_mode"), "{text}");
        // The partial aggregation's input; the final one then reads no
        // constant either.
        assert_eq!(text.matches("ForgetOrderExec").count(), 1, "{text}");
        let rows: usize = datafusion::physical_plan::collect(fixed, ctx.task_ctx())
            .await
            .unwrap()
            .iter()
            .map(datafusion::arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 33);
        assert_eq!(UnorderedConstantKeys.name(), UNORDERED_CONSTANT_KEYS);
        assert!(UnorderedConstantKeys.schema_check());
        let forget = ForgetOrderExec::new(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
        )));
        assert_eq!(forget.name(), "ForgetOrderExec");
        assert_eq!(forget.maintains_input_order(), [true]);
        assert_eq!(forget.benefits_from_input_partitioning(), [false]);
        // Part of the key really sorted on (not a constant): left as it is.
        let partly = "SELECT k, v % 2, count(*) FROM (SELECT value AS k, value AS v FROM generate_series(1, 100) \
                      ORDER BY k) GROUP BY k, v % 2";
        let plan = ctx
            .sql(partly)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let before = displayable(plan.as_ref()).indent(true).to_string();
        assert!(before.contains("ordering_mode=PartiallySorted"), "{before}");
        let after = UnorderedConstantKeys
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        assert_eq!(displayable(after.as_ref()).indent(true).to_string(), before);
        // A key really sorted on (not a constant) keeps its order.
        let sorted =
            "SELECT k, count(*) FROM (SELECT value AS k FROM generate_series(1, 100) ORDER BY k) GROUP BY k";
        let plan = ctx
            .sql(sorted)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let before = displayable(plan.as_ref()).indent(true).to_string();
        let after = UnorderedConstantKeys
            .optimize(plan, &ConfigOptions::default())
            .unwrap();
        assert_eq!(displayable(after.as_ref()).indent(true).to_string(), before);
    }
}
