//! Build side of a hash join whose inputs the statistics do not both size.
//!
//! A hash join holds its build side, its first input, in memory, in a
//! reservation that cannot spill. `DataFusion` plans the query's left input
//! as the build side, and `JoinSelection` puts the smaller input there when
//! the statistics size both inputs (by bytes, else by rows). When they size
//! one input or none, it keeps the query's order, unless an input is small
//! enough to be collected on one partition. A federated source reports no
//! statistics: its table, and every join, aggregation or window above it,
//! has no size, and the query's left input was held whole however large it
//! was (`EXCEEDED_LOCAL_MEMORY_LIMIT` on a remote table of millions of rows
//! filtered by `IN (keys of a local table)`). The reference engine builds
//! the right input in that case: the subquery of `IN`, the right side of a
//! join. Queries written for it name the large table first.
//!
//! [`BuildRightWithoutSizes`] runs right before `JoinSelection` and swaps
//! the inputs of every hash join whose inputs the statistics do not both
//! size, so that the right input comes first. `JoinSelection` then decides
//! with what it knows, as before: an input small enough for one partition
//! is still collected (and swapped back to the first place if needed), and
//! otherwise the right input stays the build side. A join whose inputs are
//! both sized is left to `JoinSelection`. The swap is `DataFusion`'s own
//! (`HashJoinExec::swap_inputs`): the join type is mirrored and the columns
//! are put back in the query's order, so the rows are the same. A
//! null-aware anti join (`NOT IN`) keeps its sides, as in `JoinSelection`,
//! and nothing is swapped when `datafusion.optimizer.join_reordering` is
//! off.

use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;

/// Name of [`BuildRightWithoutSizes`].
pub const BUILD_RIGHT_WITHOUT_SIZES: &str = "codelake_build_right_without_sizes";

/// Name of the rule this one precedes.
pub const BEFORE_RULE: &str = "join_selection";

/// Whether the statistics of two inputs compare their sizes: both byte sizes
/// known, or both row counts, as `JoinSelection` compares them.
pub fn sized_alike(left: &Statistics, right: &Statistics) -> bool {
    let bytes = left.total_byte_size.get_value().is_some() && right.total_byte_size.get_value().is_some();
    let rows = left.num_rows.get_value().is_some() && right.num_rows.get_value().is_some();
    bytes || rows
}

/// Whether `join` may take its right input as its build side.
fn swappable(join: &HashJoinExec) -> bool {
    join.join_type().supports_swap() && !join.null_aware
}

/// Physical rule, right before `JoinSelection`: see the module documentation.
#[derive(Debug, Default)]
pub struct BuildRightWithoutSizes;

impl PhysicalOptimizerRule for BuildRightWithoutSizes {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.join_reordering {
            return Ok(plan);
        }
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if !swappable(join) {
                return Ok(Transformed::no(node));
            }
            let left = join.left().partition_statistics(None)?;
            let right = join.right().partition_statistics(None)?;
            if sized_alike(&left, &right) {
                return Ok(Transformed::no(node));
            }
            tracing::trace!(join_type = ?join.join_type(), "hash join builds its right input");
            Ok(Transformed::yes(join.swap_inputs(*join.partition_mode())?))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        BUILD_RIGHT_WITHOUT_SIZES
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `rules` with [`BuildRightWithoutSizes`] right before `JoinSelection`, or
/// first when that rule is missing.
pub fn with_build_right_without_sizes(
    mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let position = rules
        .iter()
        .position(|rule| rule.name() == BEFORE_RULE)
        .unwrap_or(0);
    rules.insert(position, Arc::new(BuildRightWithoutSizes));
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{int64, remote_table, strings};
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    fn stats(bytes: Precision<usize>, rows: Precision<usize>) -> Statistics {
        Statistics {
            num_rows: rows,
            total_byte_size: bytes,
            column_statistics: Vec::new(),
        }
    }

    #[test]
    fn inputs_are_sized_alike_by_bytes_or_by_rows_on_both_sides() {
        use Precision::{Absent, Exact, Inexact};
        let both_bytes = (stats(Exact(10), Absent), stats(Inexact(20), Absent));
        let both_rows = (stats(Absent, Inexact(1)), stats(Absent, Exact(2)));
        let everything = (stats(Exact(10), Exact(1)), stats(Exact(20), Exact(2)));
        assert!(sized_alike(&both_bytes.0, &both_bytes.1));
        assert!(sized_alike(&both_rows.0, &both_rows.1));
        assert!(sized_alike(&everything.0, &everything.1));
        // Bytes on one side, rows on the other: nothing to compare.
        assert!(!sized_alike(&stats(Exact(10), Absent), &stats(Absent, Exact(2))));
        assert!(!sized_alike(&stats(Absent, Exact(2)), &stats(Exact(10), Absent)));
        // One side unsized, whatever the other knows.
        let unsized_side = stats(Absent, Absent);
        assert!(!sized_alike(&everything.0, &unsized_side));
        assert!(!sized_alike(&unsized_side, &everything.1));
        assert!(!sized_alike(&unsized_side, &unsized_side));
    }

    #[test]
    fn the_rule_runs_right_before_join_selection() {
        let rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
        let selection = rules.iter().position(|r| r.name() == BEFORE_RULE).unwrap();
        let rules = with_build_right_without_sizes(rules);
        assert_eq!(rules[selection].name(), BUILD_RIGHT_WITHOUT_SIZES);
        assert_eq!(rules[selection + 1].name(), BEFORE_RULE);
        assert!(rules[selection].schema_check());
        // Without `JoinSelection`, first.
        let alone = with_build_right_without_sizes(Vec::new());
        assert_eq!(alone.len(), 1);
        assert_eq!(alone[0].name(), BUILD_RIGHT_WITHOUT_SIZES);
    }

    /// The engine's session, 4 partitions, with no input small enough to be
    /// collected on one partition unless `collect` is set.
    async fn session(collect: bool) -> SessionContext {
        let ctx = crate::testing::federated_session();
        let mut statements = vec!["SET datafusion.execution.target_partitions = 4"];
        if !collect {
            statements.push("SET datafusion.optimizer.hash_join_single_partition_threshold = 0");
            statements.push("SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0");
        }
        for statement in statements {
            ctx.sql(statement).await.unwrap().collect().await.unwrap();
        }
        // A remote table (no statistics) and two local ones (exact ones).
        let remote = remote_table(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3, 4, 5, 6])),
                ("status", strings(&["a", "b", "c", "d", "e", "f"])),
            ],
            None,
        );
        ctx.register_table("contracts", remote).unwrap();
        local(&ctx, "wanted", &[4, 2, 9]);
        local(&ctx, "many", &(0..200).collect::<Vec<_>>());
        ctx
    }

    fn local(ctx: &SessionContext, name: &str, ids: &[i64]) {
        // Nullable: `NOT IN` over it is a null-aware anti join.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![int64(ids)]).unwrap();
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }

    /// For every hash join of `sql`'s plan (outermost first): its mode and
    /// type, and its build side: `remote` when it reads the remote table,
    /// else the rows its statistics report.
    async fn joins(ctx: &SessionContext, sql: &str) -> Vec<(String, String)> {
        fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<(String, String)>) {
            if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
                let text = displayable(join.left().as_ref()).indent(true).to_string();
                let build = if text.contains("VirtualExecutionPlan") {
                    "remote".to_owned()
                } else {
                    let rows = join.left().partition_statistics(None).unwrap().num_rows;
                    format!("{rows:?}")
                };
                out.push((
                    format!("{:?} {:?}", join.partition_mode(), join.join_type()),
                    build,
                ));
            }
            for child in plan.children() {
                walk(child, out);
            }
        }
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        let mut out = Vec::new();
        walk(&plan, &mut out);
        out
    }

    fn one(mode: &str, build: &str) -> Vec<(String, String)> {
        vec![(mode.to_owned(), build.to_owned())]
    }

    async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                let column = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..column.len())
                    .map(|i| if column.is_null(i) { -1 } else { column.value(i) })
                    .collect::<Vec<_>>()
            })
            .collect();
        out.sort_unstable();
        out
    }

    const IN_LOCAL_KEYS: &str =
        "SELECT customer_id FROM contracts WHERE customer_id IN (SELECT id FROM wanted)";

    #[tokio::test]
    async fn a_remote_table_filtered_by_local_keys_builds_the_keys() {
        let ctx = session(false).await;
        // Before: `LeftSemi`, the remote table the build side.
        assert_eq!(
            joins(&ctx, IN_LOCAL_KEYS).await,
            one("Partitioned RightSemi", "Exact(3)")
        );
        assert_eq!(ids(&ctx, IN_LOCAL_KEYS).await, [2, 4]);
    }

    #[tokio::test]
    async fn a_join_with_an_unsized_input_builds_its_right_input() {
        let ctx = session(false).await;
        let remote_first = "SELECT c.customer_id FROM contracts c JOIN wanted w ON c.customer_id = w.id";
        assert_eq!(
            joins(&ctx, remote_first).await,
            one("Partitioned Inner", "Exact(3)")
        );
        assert_eq!(ids(&ctx, remote_first).await, [2, 4]);
        // As written: the right input, whatever the other one's size.
        let local_first = "SELECT c.customer_id FROM wanted w JOIN contracts c ON c.customer_id = w.id";
        assert_eq!(joins(&ctx, local_first).await, one("Partitioned Inner", "remote"));
        assert_eq!(ids(&ctx, local_first).await, [2, 4]);
        // Outer joins: the preserved side is mirrored with the inputs.
        let left = "SELECT w.id FROM contracts c LEFT JOIN wanted w ON c.customer_id = w.id";
        assert_eq!(joins(&ctx, left).await, one("Partitioned Right", "Exact(3)"));
        assert_eq!(ids(&ctx, left).await, [-1, -1, -1, -1, 2, 4]);
        let full = "SELECT coalesce(c.customer_id, w.id) FROM contracts c FULL JOIN wanted w ON c.customer_id = w.id";
        assert_eq!(joins(&ctx, full).await, one("Partitioned Full", "Exact(3)"));
        assert_eq!(ids(&ctx, full).await, [1, 2, 3, 4, 5, 6, 9]);
    }

    #[tokio::test]
    async fn inputs_both_sized_are_left_to_join_selection() {
        let ctx = session(false).await;
        // The smaller input is the build side, in either order.
        for sql in [
            "SELECT w.id FROM many m JOIN wanted w ON m.id = w.id",
            "SELECT w.id FROM wanted w JOIN many m ON m.id = w.id",
        ] {
            assert_eq!(
                joins(&ctx, sql).await,
                one("Partitioned Inner", "Exact(3)"),
                "{sql}"
            );
            assert_eq!(ids(&ctx, sql).await, [2, 4, 9]);
        }
    }

    #[tokio::test]
    async fn an_input_small_enough_for_one_partition_is_still_collected() {
        let ctx = session(true).await;
        let local_first = "SELECT c.customer_id FROM wanted w JOIN contracts c ON c.customer_id = w.id";
        assert_eq!(
            joins(&ctx, local_first).await,
            one("CollectLeft Inner", "Exact(3)")
        );
        assert_eq!(ids(&ctx, local_first).await, [2, 4]);
        assert_eq!(
            joins(&ctx, IN_LOCAL_KEYS).await,
            one("CollectLeft RightSemi", "Exact(3)")
        );
        assert_eq!(ids(&ctx, IN_LOCAL_KEYS).await, [2, 4]);
    }

    #[tokio::test]
    async fn not_in_keeps_its_sides() {
        let ctx = session(false).await;
        let sql = "SELECT customer_id FROM contracts WHERE customer_id NOT IN (SELECT id FROM wanted)";
        assert_eq!(joins(&ctx, sql).await, one("CollectLeft LeftAnti", "remote"));
        assert_eq!(ids(&ctx, sql).await, [1, 3, 5, 6]);
    }

    #[tokio::test]
    async fn without_join_reordering_the_query_order_stays() {
        let ctx = session(false).await;
        ctx.sql("SET datafusion.optimizer.join_reordering = false")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            joins(&ctx, IN_LOCAL_KEYS).await,
            one("Partitioned LeftSemi", "remote")
        );
        assert_eq!(ids(&ctx, IN_LOCAL_KEYS).await, [2, 4]);
    }
}
