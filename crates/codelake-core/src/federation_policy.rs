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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Column, DFSchema};
use datafusion::error::Result;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, Extension, Filter, LogicalPlan, Projection, SubqueryAlias, TableScan};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_federation::{FederatedPlanNode, FederationPlanner};

/// Decides whether a single plan node (not its inputs) may run at the source.
pub trait PushdownPolicy: Send + Sync + Debug {
    fn node_is_pushable(&self, plan: &LogicalPlan) -> bool;

    /// Last rewrite of a subtree that will be federated, e.g. to express
    /// literals the unparser cannot render. Runs after the optimizer, so its
    /// output is not folded again.
    fn prepare(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    /// A predicate the source may evaluate in place of `term` (a filter term
    /// the policy keeps local) because it keeps *at least* the rows `term`
    /// keeps: text equality under a case-insensitive collation, a date range
    /// on a timestamp. The source then sends fewer rows; `term` itself still
    /// runs in the engine, which keeps the result exact.
    fn superset(&self, _term: &Expr, _schema: &DFSchema) -> Option<Expr> {
        None
    }
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

fn federate_prepared(
    plan: LogicalPlan,
    policy: &dyn PushdownPolicy,
    planner: &Arc<dyn FederationPlanner>,
) -> Result<LogicalPlan> {
    Ok(federate(policy.prepare(unmark_supersets(plan)?)?, planner))
}

/// The superset filters of a sub-plan about to be sent to its source, as the
/// plain predicates the source evaluates.
fn unmark_supersets(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| {
        if !matches!(node, LogicalPlan::Filter(_)) {
            return Ok(Transformed::no(node));
        }
        node.map_expressions(|expr| {
            expr.transform(|e| match crate::functions::as_superset_filter(&e) {
                Some(predicate) => Ok(Transformed::yes(predicate.clone())),
                None => Ok(Transformed::no(e)),
            })
        })
    })
    .data()
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
        return federate_prepared(plan, policy, planner);
    }
    if let LogicalPlan::TableScan(scan) = plan {
        return lift_scan_filters(scan, policy, planner);
    }
    let plan = split_filter(plan, policy)?;
    let children = plan
        .inputs()
        .into_iter()
        .cloned()
        .map(|child| split(child, policy, planner))
        .collect::<Result<Vec<_>>>()?;
    // Runs in the engine: its arithmetic is checked again (see `crate::overflow`).
    let local = without_local_supersets(plan.with_new_exprs(plan.expressions(), children)?)?;
    crate::overflow::check_local_node(local)
}

/// A superset filter only serves a source: in the engine it repeats an exact
/// term that stays above it (`split_filter`), so a filter left local drops
/// it, and a filter of nothing else is removed.
fn without_local_supersets(plan: LogicalPlan) -> Result<LogicalPlan> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(plan);
    };
    let terms = split_conjunction_owned(filter.predicate.clone());
    if !terms
        .iter()
        .any(|term| crate::functions::as_superset_filter(term).is_some())
    {
        return Ok(LogicalPlan::Filter(filter));
    }
    let kept = terms
        .into_iter()
        .filter(|term| crate::functions::as_superset_filter(term).is_none());
    match conjunction(kept) {
        Some(predicate) => Ok(LogicalPlan::Filter(Filter::try_new(predicate, filter.input)?)),
        None => Ok(Arc::unwrap_or_clone(filter.input)),
    }
}

/// A rejected `Filter` whose predicate is a conjunction keeps its unsafe terms
/// locally and puts below them, in a filter that can then be pushed, its safe
/// terms and a superset of each unsafe term the policy offers one for
/// ([`PushdownPolicy::superset`], marked so the policy accepts it and never
/// offered again). Rewritten only when both halves are non-empty; the filter
/// below has no unsafe term, so recursion terminates.
fn split_filter(plan: LogicalPlan, policy: &dyn PushdownPolicy) -> Result<LogicalPlan> {
    let LogicalPlan::Filter(filter) = plan else {
        return Ok(plan);
    };
    let input = Arc::clone(&filter.input);
    let (safe, unsafe_terms): (Vec<_>, Vec<_>) = split_conjunction_owned(filter.predicate.clone())
        .into_iter()
        .partition(|term| {
            Filter::try_new(term.clone(), Arc::clone(&input))
                .is_ok_and(|f| policy.node_is_pushable(&LogicalPlan::Filter(f)))
        });
    let supersets = unsafe_terms
        .iter()
        .filter_map(|term| policy.superset(term, input.schema()))
        .map(crate::functions::superset_filter)
        .filter(|marked| {
            Filter::try_new(marked.clone(), Arc::clone(&input))
                .is_ok_and(|f| policy.node_is_pushable(&LogicalPlan::Filter(f)))
        });
    let safe: Vec<Expr> = safe.into_iter().chain(supersets).collect();
    match (conjunction(safe), conjunction(unsafe_terms)) {
        (Some(safe), Some(rest)) => {
            let lower = LogicalPlan::Filter(Filter::try_new(safe, input)?);
            Ok(LogicalPlan::Filter(Filter::try_new(rest, Arc::new(lower))?))
        }
        _ => Ok(LogicalPlan::Filter(filter)),
    }
}

/// A scan the policy rejects can only be due to its embedded filters: move them
/// into a local `Filter` and federate the bare scan.
fn lift_scan_filters(
    mut scan: TableScan,
    policy: &dyn PushdownPolicy,
    planner: &Arc<dyn FederationPlanner>,
) -> Result<LogicalPlan> {
    let filters = std::mem::take(&mut scan.filters);
    let bare = federate_prepared(LogicalPlan::TableScan(scan), policy, planner)?;
    // Never drop a predicate: if it cannot be rebuilt, planning fails.
    Ok(match conjunction(filters) {
        Some(predicate) => crate::overflow::check_local_node(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::new(bare),
        )?))?,
        None => bare,
    })
}

/// Name of [`PruneFederatedColumns`], for rule ordering.
pub const PRUNE_FEDERATED_COLUMNS: &str = "codelake_prune_federated_columns";

/// Sends a source only the columns the engine uses.
///
/// Federation runs before `optimize_projections` (see
/// [`crate::engine::optimizer_rules`]), so a federated node that is not the
/// whole query (a filter the policy keeps local, a cross-source join) still
/// outputs every column of its tables, and the source returns them all.
/// `optimize_projections` then puts the projection the engine needs *above*
/// the node: it cannot see inside it. This rule, right after it, moves the
/// columns that projection references into the federated sub-plan, as a
/// projection of plain columns every source can evaluate, when that sub-plan
/// is a table scan under filters, aliased or not: over a join or an
/// aggregation, or above an alias, the unparser can reference a relation it
/// did not name.
#[derive(Debug, Default)]
pub struct PruneFederatedColumns;

impl OptimizerRule for PruneFederatedColumns {
    fn name(&self) -> &'static str {
        PRUNE_FEDERATED_COLUMNS
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let LogicalPlan::Extension(Extension { node }) = projection.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };
        let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>() else {
            return Ok(Transformed::no(plan));
        };
        // `FROM t AS r`: the projection goes below the alias, as a plain
        // `SELECT` of the table (`FROM (SELECT a, b FROM t) AS r`).
        let (alias, inner) = match &federated.plan {
            LogicalPlan::SubqueryAlias(aliased) => (Some(aliased.alias.clone()), aliased.input.as_ref()),
            other => (None, other),
        };
        if !is_filtered_scan(inner) {
            return Ok(Transformed::no(plan));
        }
        let used: HashSet<&Column> = projection.expr.iter().flat_map(Expr::column_refs).collect();
        let positions: Vec<usize> = federated
            .plan
            .schema()
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, column)| used.contains(column))
            .map(|(position, _)| position)
            .collect();
        // Nothing to prune, or nothing left: a projection without columns has
        // no portable SQL (`SELECT FROM …`).
        if positions.is_empty() || positions.len() == federated.plan.schema().fields().len() {
            return Ok(Transformed::no(plan));
        }
        // An alias renames the columns of its input, position by position.
        let kept: Vec<Expr> = positions
            .iter()
            .map(|position| Expr::Column(Column::from(inner.schema().qualified_field(*position))))
            .collect();
        let pruned = LogicalPlan::Projection(Projection::try_new(kept, Arc::new(inner.clone()))?);
        let pruned = match alias {
            Some(alias) => LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(pruned), alias)?),
            None => pruned,
        };
        let input = federate(pruned, &federated.planner);
        Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(
            projection.expr.clone(),
            Arc::new(input),
        )?)))
    }
}

/// Filters over one table scan: a projection of its columns renders as a
/// plain `SELECT` in every dialect. Above an alias, a join or an aggregation
/// the unparser can render one that references a relation it did not name
/// (`missing FROM-clause entry` in PostgreSQL), so those are left alone; an
/// alias over the scan takes the projection below it.
fn is_filtered_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::Filter(filter) => is_filtered_scan(&filter.input),
        _ => false,
    }
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

    /// Filters, scans and projections; aggregates stay local.
    #[derive(Debug)]
    struct NumericFilters;

    impl PushdownPolicy for NumericFilters {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            matches!(
                plan,
                LogicalPlan::TableScan(_)
                    | LogicalPlan::Projection(_)
                    | LogicalPlan::SubqueryAlias(_)
                    | LogicalPlan::Filter(_)
            )
        }
    }

    #[tokio::test]
    async fn filter_of_one_side_of_a_cross_source_join_reaches_that_source() {
        let ctx = crate::testing::federated_session();
        let (contracts, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("amount", int64(&[10, 200, 300])),
            ],
            Some(Arc::new(NumericFilters)),
        );
        ctx.register_table("contracts", contracts).unwrap();
        ctx.register_table(
            "customers",
            remote_table(
                "erp",
                "customers",
                vec![("id", int64(&[1, 2, 3]))],
                Some(Arc::new(NumericFilters)),
            ),
        )
        .unwrap();
        // The fake source ignores its SQL, so rows are not asserted here: this
        // test checks that the predicate travels (rows are covered elsewhere).
        let _ = rows(
            &ctx,
            "SELECT c.id FROM customers c JOIN contracts k ON k.customer_id = c.id WHERE k.amount >= 200",
        )
        .await
        .unwrap();
        let sent = sent.lock().unwrap().join("\n");
        assert!(
            sent.contains("WHERE") && sent.contains("200"),
            "filter not pushed: {sent}"
        );
    }

    /// Pushes filters without text literals.
    #[derive(Debug)]
    struct NoTextFilters;

    impl PushdownPolicy for NoTextFilters {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Filter(f) => !f.predicate.to_string().contains("Utf8"),
                LogicalPlan::TableScan(_) | LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
                    true
                }
                _ => false,
            }
        }
    }

    #[tokio::test]
    async fn safe_conjuncts_are_pushed_and_the_rest_stays_local() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("status", strings(&["active", "active", "ACTIVE"])),
            ],
            Some(Arc::new(NoTextFilters)),
        );
        ctx.register_table("contracts", table).unwrap();
        let batches = rows(
            &ctx,
            "SELECT customer_id FROM contracts WHERE customer_id >= 2 AND status = 'active'",
        )
        .await
        .unwrap();
        let sent = sent.lock().unwrap().join("\n");
        assert!(sent.contains(">= 2"), "numeric term not pushed: {sent}");
        assert!(!sent.contains("active"), "text term must stay local: {sent}");
        // The fake source ignores its SQL: the engine applies both terms again
        // only if the text term stayed local, so customer 1 appears here and
        // customer 3 (ACTIVE) does not.
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids.values().iter().all(|id| *id != 3), "{ids:?}");
    }

    #[tokio::test]
    async fn a_partly_federated_query_reads_only_the_columns_it_uses() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("status", strings(&["active", "active", "ACTIVE"])),
                ("notes", strings(&["x", "y", "z"])),
            ],
            Some(Arc::new(NoTextFilters)),
        );
        ctx.register_table("contracts", table).unwrap();
        let batches = rows(
            &ctx,
            "SELECT customer_id FROM contracts WHERE customer_id >= 2 AND status = 'active'",
        )
        .await
        .unwrap();
        let sent = sent.lock().unwrap().join("\n");
        assert!(sent.contains(">= 2"), "numeric term not pushed: {sent}");
        assert!(
            sent.contains("customer_id") && sent.contains("status"),
            "the local filter needs status: {sent}"
        );
        assert!(!sent.contains("notes"), "unused column sent for: {sent}");
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids.values().iter().all(|id| *id != 3), "{ids:?}");
    }

    #[tokio::test]
    async fn an_aliased_scan_reads_only_the_columns_it_uses() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("status", strings(&["active", "active", "ACTIVE"])),
                ("notes", strings(&["x", "y", "z"])),
            ],
            Some(Arc::new(NoTextFilters)),
        );
        ctx.register_table("contracts", table).unwrap();
        let batches = rows(
            &ctx,
            "SELECT c.customer_id FROM contracts c WHERE c.customer_id >= 2 AND c.status = 'active'",
        )
        .await
        .unwrap();
        let sent = sent.lock().unwrap().join("\n");
        assert!(sent.contains("customer_id") && sent.contains("status"), "{sent}");
        assert!(!sent.contains("notes"), "unused column sent for: {sent}");
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(ids.values().iter().all(|id| *id != 3), "{ids:?}");
    }

    /// Pushes no text filter as such, but sends text equality as a superset.
    #[derive(Debug)]
    struct CaseInsensitiveSource;

    impl PushdownPolicy for CaseInsensitiveSource {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Filter(f) => split_conjunction_owned(f.predicate.clone()).iter().all(|term| {
                    crate::functions::as_superset_filter(term).is_some() || !term.to_string().contains("Utf8")
                }),
                LogicalPlan::TableScan(_) | LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
                    true
                }
                _ => false,
            }
        }

        fn superset(&self, term: &Expr, _schema: &DFSchema) -> Option<Expr> {
            term.to_string().contains("Utf8").then(|| term.clone())
        }
    }

    #[tokio::test]
    async fn a_superset_reaches_the_source_and_the_exact_term_stays_local() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("status", strings(&["active", "active", "ACTIVE"])),
            ],
            Some(Arc::new(CaseInsensitiveSource)),
        );
        ctx.register_table("contracts", table).unwrap();
        let batches = rows(
            &ctx,
            "SELECT customer_id FROM contracts WHERE status = 'active' ORDER BY 1",
        )
        .await
        .unwrap();
        let sent = sent.lock().unwrap().join("\n");
        assert!(sent.contains("'active'"), "superset not sent: {sent}");
        assert!(
            !sent.contains("codelake_superset"),
            "the marker is not SQL: {sent}"
        );
        // The fake source ignores its SQL, as a case-insensitive one would
        // return ACTIVE too: the engine keeps only the exact matches.
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
    }

    #[tokio::test]
    async fn a_superset_below_a_cross_source_join_reaches_its_source() {
        let ctx = crate::testing::federated_session();
        let (contracts, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("amount", int64(&[300, 300, 10])),
                ("status", strings(&["active", "ACTIVE", "active"])),
            ],
            Some(Arc::new(CaseInsensitiveSource)),
        );
        ctx.register_table("contracts", contracts).unwrap();
        ctx.register_table(
            "customers",
            remote_table(
                "erp",
                "customers",
                vec![("id", int64(&[1, 2, 3]))],
                Some(Arc::new(NumericFilters)),
            ),
        )
        .unwrap();
        let batches = rows(
            &ctx,
            "SELECT c.id FROM customers c JOIN contracts k ON k.customer_id = c.id \
             WHERE k.amount >= 200 AND k.status = 'active' ORDER BY 1",
        )
        .await
        .unwrap();
        let plan = ctx
            .sql("EXPLAIN SELECT c.id FROM customers c JOIN contracts k ON k.customer_id = c.id WHERE k.amount >= 200 AND k.status = 'active'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        let sent = sent.lock().unwrap().join("\n");
        assert!(
            sent.contains("'active'") && sent.contains("200"),
            "superset not sent: {sent}\n{plan}"
        );
        assert!(
            !plan.contains("FilterExec: codelake_superset"),
            "marker left in the engine: {plan}"
        );
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // The fake source ignores its SQL (the amount term, pushed as exact,
        // is not applied here): the text term, sent only as a superset, still
        // runs in the engine, so ACTIVE (customer 2) is not returned.
        assert!(ids.values().contains(&1) && !ids.values().contains(&2), "{ids:?}");
    }

    #[test]
    fn prepare_runs_on_every_federated_subtree() {
        #[derive(Debug)]
        struct Marks;
        impl PushdownPolicy for Marks {
            fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
                matches!(plan, LogicalPlan::TableScan(_))
            }
            fn prepare(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
                LogicalPlanBuilder::from(plan).filter(lit(true))?.build()
            }
        }
        let table = remote_table("crm", "t", vec![("a", int64(&[1]))], None);
        let scan = LogicalPlanBuilder::scan("t", provider_as_source(table), None)
            .unwrap()
            .build()
            .unwrap();
        let planner: Arc<dyn FederationPlanner> =
            Arc::new(SQLFederationPlanner::new(crate::testing::executor("crm")));
        let out = restrict_federation(federate(scan, &planner), &Marks).unwrap();
        let LogicalPlan::Extension(ext) = out else {
            panic!("expected federated node")
        };
        let inner = ext.node.as_any().downcast_ref::<FederatedPlanNode>().unwrap();
        assert!(
            matches!(inner.plan(), LogicalPlan::Filter(_)),
            "prepare was not applied"
        );
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
