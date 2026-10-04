//! A `LIMIT` straight over a federated sub-plan reaches its source.
//!
//! Federation runs before `DataFusion` pushes limits down
//! ([`crate::engine::optimizer_rules`]), so a limit that `push_down_limit`
//! moves onto a federated node stops there: under a `UNION ALL` of two
//! remote tables, `LIMIT 50` became a local limit over each branch, and each
//! source was asked for its whole table, read until the engine had its rows
//! and dropped the stream (shadow run, 2026-10-02: the `UNION ALL` of the two
//! ERP servers).
//!
//! [`LimitIntoFederation`], after `DataFusion`'s rules, puts a copy of such a
//! limit (`skip + fetch` rows, nothing skipped) at the root of the federated
//! sub-plan; the local limit stays and keeps the result exact. Nothing sits
//! between them, so any rows the source returns are rows the limit could
//! keep. The source's policy then decides (`crate::federation_policy`): SQL
//! Server sends `TOP (n)` when everything below reaches it exactly, and keeps
//! the limit local otherwise (a filter it cannot evaluate, a set operation);
//! a sort below takes the `first_rows` path of PR #84.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::error::Result;
use datafusion::logical_expr::{Extension, FetchType, LogicalPlan, LogicalPlanBuilder, SkipType};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_federation::FederatedPlanNode;

/// Name of [`LimitIntoFederation`].
pub const LIMIT_INTO_FEDERATION: &str = "codelake_limit_into_federation";

/// Optimizer rule: see the module documentation.
#[derive(Debug, Default)]
pub struct LimitIntoFederation;

/// Rows a limit keeps at most counting what it skips, when both are
/// literals.
fn rows_kept(limit: &datafusion::logical_expr::Limit) -> Option<usize> {
    let (Ok(SkipType::Literal(skip)), Ok(FetchType::Literal(Some(fetch)))) =
        (limit.get_skip_type(), limit.get_fetch_type())
    else {
        return None;
    };
    skip.checked_add(fetch).filter(|rows| *rows > 0)
}

impl OptimizerRule for LimitIntoFederation {
    fn name(&self) -> &'static str {
        LIMIT_INTO_FEDERATION
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Extension(Extension { node }) = limit.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>() else {
            return Ok(Transformed::no(plan));
        };
        let Some(rows) = rows_kept(limit) else {
            return Ok(Transformed::no(plan));
        };
        // Already limited (an earlier pass, or the query's own limit inside).
        if let LogicalPlan::Limit(inner) = federated.plan()
            && rows_kept(inner).is_some_and(|inner| inner <= rows)
        {
            return Ok(Transformed::no(plan));
        }
        let limited = LogicalPlanBuilder::from(federated.plan().clone())
            .limit(0, Some(rows))?
            .build()?;
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(FederatedPlanNode::new(limited, Arc::clone(&federated.planner))),
        });
        Ok(Transformed::yes(
            plan.with_new_exprs(plan.expressions(), vec![input])?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation_policy::PushdownPolicy;
    use crate::testing::{int64, remote_table_recording, strings};
    use datafusion::arrow::array::{Array, Int64Array};

    /// Scans, projections, aliases, unions and limits over anything but a
    /// union, as the SQL Server policy (`TOP` cannot apply to a set
    /// operation); text filters stay local.
    #[derive(Debug)]
    struct TopSource;

    impl PushdownPolicy for TopSource {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::TableScan(scan) => scan.filters.is_empty() && scan.fetch.is_none(),
                LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_) => true,
                LogicalPlan::Filter(filter) => !filter.predicate.to_string().contains("Utf8"),
                LogicalPlan::Limit(limit) => {
                    limit.skip.is_none() && !matches!(limit.input.as_ref(), LogicalPlan::Union(_))
                }
                _ => false,
            }
        }
    }

    type Sent = Arc<std::sync::Mutex<Vec<String>>>;

    fn session() -> (datafusion::prelude::SessionContext, Sent, Sent) {
        let ctx = crate::testing::federated_session();
        let columns = || {
            vec![
                ("id", int64(&[1, 2, 3, 4])),
                ("status", strings(&["a", "b", "a", "b"])),
            ]
        };
        let (first, first_sent) =
            remote_table_recording("one", "contracts", columns(), Some(Arc::new(TopSource)));
        let (second, second_sent) =
            remote_table_recording("two", "orders", columns(), Some(Arc::new(TopSource)));
        ctx.register_table("contracts", first).unwrap();
        ctx.register_table("orders", second).unwrap();
        (ctx, first_sent, second_sent)
    }

    async fn ids(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches
            .iter()
            .flat_map(|b| {
                let column = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..column.len()).map(|i| column.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn sent(statements: &Sent) -> String {
        statements.lock().unwrap().join("\n")
    }

    /// A `UNION ALL` of two sources under `LIMIT`: each source is asked for
    /// that many rows (the fake ones return every row; the engine keeps 3).
    #[tokio::test]
    async fn each_source_of_a_union_gets_the_limit() {
        let (ctx, first, second) = session();
        let sql = "SELECT id FROM (SELECT id FROM contracts UNION ALL SELECT id FROM orders) LIMIT 3";
        assert_eq!(ids(&ctx, sql).await.len(), 3);
        for statements in [&first, &second] {
            let text = sent(statements);
            assert!(text.contains("LIMIT 3"), "{text}");
        }
    }

    /// `OFFSET` counts: the source returns the rows skipped too.
    #[tokio::test]
    async fn the_source_returns_the_rows_skipped_too() {
        let (ctx, first, second) = session();
        let sql =
            "SELECT id FROM (SELECT id FROM contracts UNION ALL SELECT id FROM orders) LIMIT 2 OFFSET 1";
        assert_eq!(ids(&ctx, sql).await.len(), 2);
        assert!(sent(&first).contains("LIMIT 3"), "{}", sent(&first));
        assert!(sent(&second).contains("LIMIT 3"), "{}", sent(&second));
    }

    /// A filter the source cannot evaluate stays between the limit and the
    /// source: the source is asked for every row.
    #[tokio::test]
    async fn a_local_filter_keeps_the_limit_local() {
        let (ctx, first, second) = session();
        let sql = "SELECT id FROM (SELECT id, status FROM contracts UNION ALL SELECT id, status FROM orders) \
                   WHERE status = 'a' LIMIT 1";
        assert_eq!(ids(&ctx, sql).await.len(), 1);
        for statements in [&first, &second] {
            let text = sent(statements);
            assert!(!text.contains("LIMIT"), "{text}");
        }
    }

    /// The rule applied twice adds one limit.
    #[test]
    fn a_limited_sub_plan_is_left_as_it_is() {
        use datafusion::optimizer::OptimizerContext;
        let planner: Arc<dyn datafusion_federation::FederationPlanner> = Arc::new(
            datafusion_federation::sql::SQLFederationPlanner::new(crate::testing::executor("x")),
        );
        let scan = LogicalPlanBuilder::empty(false).build().unwrap();
        let federated = LogicalPlan::Extension(Extension {
            node: Arc::new(FederatedPlanNode::new(scan, planner)),
        });
        let plan = LogicalPlanBuilder::from(federated)
            .limit(0, Some(5))
            .unwrap()
            .build()
            .unwrap();
        let config = OptimizerContext::new();
        let once = LimitIntoFederation.rewrite(plan, &config).unwrap();
        assert!(once.transformed);
        let twice = LimitIntoFederation.rewrite(once.data.clone(), &config).unwrap();
        assert!(!twice.transformed);
        assert_eq!(once.data, twice.data);
    }
}
