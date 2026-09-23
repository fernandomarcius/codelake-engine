//! Per-source pushdown policy.
//!
//! `datafusion-federation` sends the largest sub-plan it can to a SQL source.
//! That is only correct when the source evaluates every operator exactly like
//! the engine. Real databases do not: SQL Server compares text with a
//! case-insensitive collation and ignores trailing spaces, truncates `AVG` of
//! integers, and sorts NULLs first. Pushing such operators changes results
//! without any error.
//!
//! A [`PushdownPolicy`] says which plan nodes a source may execute.
//! [`restrict_federation`] rewrites a federated sub-plan so that the largest
//! *safe* subtrees stay federated and everything above them runs in the engine.
//! Table scans are always federated (the remote table is only reachable
//! through its source); filters embedded in a scan that the policy rejects are
//! lifted into a local `Filter`.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Extension, Filter, LogicalPlan, TableScan};
use datafusion_federation::{FederatedPlanNode, FederationPlanner};

/// Decides whether a single plan node (not its inputs) may run at the source.
pub trait PushdownPolicy: Send + Sync + Debug {
    fn node_is_pushable(&self, plan: &LogicalPlan) -> bool;
}

/// Rewrites `Federated(plan)` so only policy-approved subtrees are federated.
/// Plans that are not a federated node are returned unchanged.
pub fn restrict_federation(plan: LogicalPlan, policy: &dyn PushdownPolicy) -> Result<LogicalPlan> {
    let LogicalPlan::Extension(Extension { node }) = &plan else {
        return Ok(plan);
    };
    let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>() else {
        return Ok(plan);
    };
    let planner = Arc::clone(&federated.planner);
    split(federated.plan.clone(), policy, &planner)
}

fn federate(plan: LogicalPlan, planner: &Arc<dyn FederationPlanner>) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(FederatedPlanNode::new(plan, Arc::clone(planner))),
    })
}

fn fully_pushable(plan: &LogicalPlan, policy: &dyn PushdownPolicy) -> bool {
    policy.node_is_pushable(plan)
        && plan
            .inputs()
            .into_iter()
            .all(|input| fully_pushable(input, policy))
}

fn split(
    plan: LogicalPlan,
    policy: &dyn PushdownPolicy,
    planner: &Arc<dyn FederationPlanner>,
) -> Result<LogicalPlan> {
    if fully_pushable(&plan, policy) {
        return Ok(federate(plan, planner));
    }
    if let LogicalPlan::TableScan(scan) = plan {
        return lift_scan_filters(scan, planner);
    }
    let children = plan
        .inputs()
        .into_iter()
        .cloned()
        .map(|child| split(child, policy, planner))
        .collect::<Result<Vec<_>>>()?;
    plan.with_new_exprs(plan.expressions(), children)
}

/// A scan the policy rejects can only be due to its embedded filters: move them
/// into a local `Filter` and federate the bare scan.
fn lift_scan_filters(mut scan: TableScan, planner: &Arc<dyn FederationPlanner>) -> Result<LogicalPlan> {
    let filters = std::mem::take(&mut scan.filters);
    let bare = federate(LogicalPlan::TableScan(scan), planner);
    // Never drop a predicate: if it cannot be rebuilt, planning fails.
    Ok(match conjunction(filters) {
        Some(predicate) => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(bare))?),
        None => bare,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{int64, remote_table, remote_table_recording, strings};
    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
    use datafusion::prelude::SessionContext;
    use datafusion_federation::sql::SQLFederationPlanner;

    /// Rejects aggregations and filters: they must run in the engine.
    #[derive(Debug)]
    struct ScansAndProjectionsOnly;

    impl PushdownPolicy for ScansAndProjectionsOnly {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::TableScan(scan) => scan.filters.is_empty(),
                LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => true,
                _ => false,
            }
        }
    }

    fn contracts(policy: Option<Arc<dyn PushdownPolicy>>) -> SessionContext {
        let ctx = crate::testing::federated_session();
        ctx.register_table(
            "contracts",
            remote_table(
                "crm",
                "contracts",
                vec![
                    ("customer_id", int64(&[1, 2, 1, 3])),
                    ("status", strings(&["active", "cancelled", "active", "ACTIVE"])),
                ],
                policy,
            ),
        )
        .unwrap();
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        ctx.sql(sql).await?.collect().await
    }

    #[tokio::test]
    async fn rejected_filter_runs_in_the_engine() {
        let ctx = contracts(Some(Arc::new(ScansAndProjectionsOnly)));
        let batches = rows(
            &ctx,
            "SELECT customer_id FROM contracts WHERE status = 'active' ORDER BY customer_id",
        )
        .await
        .unwrap();
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Case-sensitive, like the engine: 'ACTIVE' (customer 3) is excluded.
        assert_eq!(ids.values(), &[1, 1]);
    }

    #[tokio::test]
    async fn rejected_aggregate_runs_in_the_engine() {
        let ctx = contracts(Some(Arc::new(ScansAndProjectionsOnly)));
        let batches = rows(
            &ctx,
            "SELECT status, count(*) AS n FROM contracts GROUP BY status ORDER BY status",
        )
        .await
        .unwrap();
        let status = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(status.len(), 3, "ACTIVE and active are different groups");
        assert_eq!(counts.values(), &[1, 2, 1]);
    }

    #[tokio::test]
    async fn without_policy_the_whole_plan_is_pushed() {
        // The fake remote can only return plain columns; a pushed aggregate
        // asks it for `count(*)` and fails, proving the node left the engine.
        let ctx = contracts(None);
        let err = rows(
            &ctx,
            "SELECT status, count(*) AS n FROM contracts GROUP BY status",
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("plain columns"), "{err}");
    }

    #[tokio::test]
    async fn only_the_safe_part_reaches_the_source() {
        let ctx = crate::testing::federated_session();
        let (table, statements) = remote_table_recording(
            "crm",
            "contracts",
            vec![("customer_id", int64(&[1, 2])), ("status", strings(&["a", "b"]))],
            Some(Arc::new(ScansAndProjectionsOnly)),
        );
        ctx.register_table("contracts", table).unwrap();
        rows(&ctx, "SELECT customer_id FROM contracts WHERE status = 'a'")
            .await
            .unwrap();
        let sent = statements.lock().unwrap().join("\n");
        assert!(
            !sent.to_uppercase().contains("WHERE"),
            "filter must stay local: {sent}"
        );
        assert!(sent.contains("contracts"), "{sent}");
    }

    #[test]
    fn scan_filters_are_lifted_when_rejected() {
        let table = remote_table("crm", "contracts", vec![("customer_id", int64(&[1]))], None);
        let scan = LogicalPlanBuilder::scan_with_filters(
            "contracts",
            provider_as_source(table),
            None,
            vec![col("customer_id").gt(lit(0_i64))],
        )
        .unwrap()
        .build()
        .unwrap();
        let executor = crate::testing::executor("crm");
        let planner: Arc<dyn FederationPlanner> = Arc::new(SQLFederationPlanner::new(executor));
        let federated = federate(scan, &planner);

        let rewritten = restrict_federation(federated, &ScansAndProjectionsOnly).unwrap();
        let LogicalPlan::Filter(filter) = &rewritten else {
            panic!("expected a local filter, got:\n{rewritten}");
        };
        let LogicalPlan::Extension(ext) = filter.input.as_ref() else {
            panic!("expected federated scan below the filter");
        };
        let inner = ext.node.as_any().downcast_ref::<FederatedPlanNode>().unwrap();
        let LogicalPlan::TableScan(bare) = inner.plan() else {
            panic!("expected a table scan");
        };
        assert!(bare.filters.is_empty());
    }

    #[test]
    fn non_federated_plans_are_untouched() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let out = restrict_federation(plan.clone(), &ScansAndProjectionsOnly).unwrap();
        assert_eq!(out, plan);
    }
}
