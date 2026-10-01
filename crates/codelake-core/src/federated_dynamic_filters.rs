//! Join dynamic filters sent to federated sources.
//!
//! A hash join reads its build side first and then knows which probe rows
//! can match: `DataFusion` publishes that as a dynamic filter (the keys'
//! min/max, and the keys themselves while they are few:
//! `datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values`,
//! 150 by default, and `hash_join_inlist_pushdown_max_size`), which file
//! scans use to skip data. A federated scan renders its SQL before that, so
//! without this the source returns every row the join then discards (a few
//! thousand local keys against a remote table of millions of rows).
//!
//! [`FederationFilterBarrier`](crate::federation_guard::FederationFilterBarrier)
//! keeps the dynamic filters offered to it and, when its remote query starts
//! (the join has built its side by then), [`narrowed`] adds them to the
//! federated sub-plan: what they say about plain columns of its table
//! (comparisons, `IN` lists, `IS NULL`, `AND`, `OR`, and every branch of the
//! `CASE` of a partitioned join), everything else dropped, which only keeps
//! more rows. The join still filters exactly, so the source only has to keep
//! at least the rows that can match: each term goes through the source's
//! policy ([`crate::federation_policy::restrict_federation`]) as a superset
//! filter, which sends what the source evaluates exactly or offers a superset
//! for (text `IN` under a case-insensitive collation, whole-second bounds on
//! a `datetime`) and drops the rest. A source without a policy gets nothing.
//!
//! A partitioned join lists up to that many keys per partition, and the
//! source receives the lists of every partition. Past the limit together
//! ([`within_key_limit`]), only the rest of the filter is sent (the bounds):
//! a source compares each row with every key of an `IN` it cannot seek, and
//! a list of a few hundred keys that keeps nearly every row made a scan of 6
//! million rows of a SQL Server view six times slower.

use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, ExprSchemable, Extension, Filter, LogicalPlan, Operator, lit};
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column as PhysicalColumn, DynamicFilterPhysicalExpr, InListExpr,
    IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_federation::FederatedPlanNode;
use datafusion_federation::sql::{SQLFederationPlanner, VirtualExecutionPlan};

/// Whether `filter` holds a dynamic filter (the only parent filters the
/// barrier keeps).
pub(crate) fn is_dynamic(filter: &Arc<dyn PhysicalExpr>) -> bool {
    filter
        .exists(|e| Ok(e.is::<DynamicFilterPhysicalExpr>()))
        .unwrap_or(false)
}

/// A predicate on the columns of `schema` that holds on every row `filter`
/// (a physical filter on the same columns) keeps; `None` when nothing of it
/// can be said (every row).
pub(crate) fn superset_of(filter: &Arc<dyn PhysicalExpr>, schema: &DFSchema) -> Option<Expr> {
    if let Some(dynamic) = filter.downcast_ref::<DynamicFilterPhysicalExpr>() {
        // Still being built: the rows it will keep are not known yet.
        if !dynamic.inner().is_complete {
            return None;
        }
        return superset_of(&dynamic.current().ok()?, schema);
    }
    if let Some(binary) = filter.downcast_ref::<BinaryExpr>() {
        let (left, right) = (binary.left(), binary.right());
        return match binary.op() {
            Operator::And => match (superset_of(left, schema), superset_of(right, schema)) {
                (Some(l), Some(r)) => Some(l.and(r)),
                (one, None) | (None, one) => one,
            },
            Operator::Or => Some(or_by_term(
                superset_of(left, schema)?,
                superset_of(right, schema)?,
            )),
            op @ (Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq) => {
                let (l, r) = (value_of(left, schema)?, value_of(right, schema)?);
                let (l, r) = (typed_like(l.clone(), &r, schema)?, typed_like(r, &l, schema)?);
                Some(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
                    Box::new(l),
                    *op,
                    Box::new(r),
                )))
            }
            _ => None,
        };
    }
    if let Some(list) = filter.downcast_ref::<InListExpr>() {
        let expr = value_of(list.expr(), schema)?;
        let items = list
            .list()
            .iter()
            .map(|item| typed_like(literal_of(item)?, &expr, schema))
            .collect::<Option<Vec<_>>>()?;
        if list.negated() {
            return Some(Expr::InList(InList::new(Box::new(expr), items, true)));
        }
        // A NULL key (the build side's) matches nothing: `IN` keeps the same
        // rows without it, and a source may not take NULL literals.
        let items: Vec<Expr> = items
            .into_iter()
            .filter(|item| !matches!(item, Expr::Literal(value, _) if value.is_null()))
            .collect();
        if items.is_empty() {
            return Some(lit(false));
        }
        return Some(Expr::InList(InList::new(Box::new(expr), items, false)));
    }
    if let Some(is_null) = filter.downcast_ref::<IsNullExpr>() {
        return Some(value_of(is_null.arg(), schema)?.is_null());
    }
    if let Some(is_not_null) = filter.downcast_ref::<IsNotNullExpr>() {
        return Some(value_of(is_not_null.arg(), schema)?.is_not_null());
    }
    // A partitioned join routes each row to one branch: it is kept only if
    // that branch keeps it, so by one of them.
    if let Some(case) = filter.downcast_ref::<CaseExpr>() {
        return case
            .when_then_expr()
            .iter()
            .map(|(_, then)| then)
            .chain(case.else_expr())
            .map(|branch| superset_of(branch, schema))
            .reduce(|a, b| Some(or(a?, b?)))
            .flatten();
    }
    // An empty partition's branch keeps nothing.
    if let Some(literal) = filter.downcast_ref::<Literal>()
        && let ScalarValue::Boolean(value) = literal.value()
    {
        return (*value != Some(true)).then(|| lit(false));
    }
    None
}

/// `a OR b`, as `(a1 OR b) AND (a2 OR b) ...` when one side is a single
/// term: a source's policy judges each term on its own, so the dynamic
/// filter of a null-safe join (`(bounds AND keys) OR key IS NULL`,
/// [`crate::join_guard::NullKeysKept`]) still sends its keys when the policy
/// refuses its bounds.
fn or_by_term(a: Expr, b: Expr) -> Expr {
    let (a, b) = (split_conjunction_owned(a), split_conjunction_owned(b));
    let (terms, single) = match (a.len(), b.len()) {
        (_, 1) => (a, b),
        (1, _) => (b, a),
        _ => {
            return or(
                conjunction(a).unwrap_or(lit(true)),
                conjunction(b).unwrap_or(lit(true)),
            );
        }
    };
    let single = conjunction(single).unwrap_or(lit(true));
    conjunction(terms.into_iter().map(|term| or(term, single.clone()))).unwrap_or(lit(true))
}

/// `a OR b`, without a side that keeps nothing.
fn or(a: Expr, b: Expr) -> Expr {
    let nothing = |e: &Expr| matches!(e, Expr::Literal(ScalarValue::Boolean(Some(false)), _));
    match (nothing(&a), nothing(&b)) {
        (true, _) => b,
        (_, true) => a,
        _ => a.or(b),
    }
}

/// A column, literal or cast of one, as a logical expression on `schema`.
fn value_of(expr: &Arc<dyn PhysicalExpr>, schema: &DFSchema) -> Option<Expr> {
    if let Some(column) = expr.downcast_ref::<PhysicalColumn>() {
        if column.index() >= schema.fields().len() {
            return None;
        }
        let (qualifier, field) = schema.qualified_field(column.index());
        // A physical column names its field; anything else is not this schema.
        return (field.name() == column.name()).then(|| Expr::Column(Column::from((qualifier, field))));
    }
    if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        return value_of(cast.expr(), schema)?
            .cast_to(cast.cast_type(), schema)
            .ok();
    }
    literal_of(expr)
}

fn literal_of(expr: &Arc<dyn PhysicalExpr>) -> Option<Expr> {
    let literal = expr.downcast_ref::<Literal>()?;
    Some(Expr::Literal(literal.value().clone(), None))
}

/// `value`, a literal cast to the type of `other` when they differ (a build
/// side read as `Utf8View` against a `Utf8` column); `None` if it cannot be.
fn typed_like(value: Expr, other: &Expr, schema: &DFSchema) -> Option<Expr> {
    let Expr::Literal(scalar, metadata) = &value else {
        return Some(value);
    };
    if matches!(other, Expr::Literal(..)) {
        return Some(value);
    }
    let target = other.get_type(schema).ok()?;
    if scalar.data_type() == target {
        return Some(value);
    }
    // Only a cast that changes no value (1.5 as an integer would move a
    // bound).
    let cast: ScalarValue = scalar.cast_to(&target).ok()?;
    let back = cast.cast_to(&scalar.data_type()).ok()?;
    (back == *scalar).then(|| Expr::Literal(cast, metadata.clone()))
}

/// `predicate` (a [`superset_of`] a join's dynamic filter) without its `IN`
/// lists when they hold more than `max_keys` keys together; `None` when
/// nothing remains (every row).
pub(crate) fn within_key_limit(predicate: Expr, max_keys: usize) -> Option<Expr> {
    if listed_keys(&predicate) <= max_keys {
        return Some(predicate);
    }
    without_lists(predicate)
}

/// Keys of the `IN` lists of `expr`, through `AND` and `OR`.
fn listed_keys(expr: &Expr) -> usize {
    match expr {
        Expr::InList(list) => list.list.len(),
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::And | Operator::Or) => {
            listed_keys(&binary.left) + listed_keys(&binary.right)
        }
        _ => 0,
    }
}

/// `expr` with every `IN` list taken as true: below `AND` and `OR` only, so
/// it keeps at least the same rows.
fn without_lists(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::InList(_) => None,
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            match (without_lists(*binary.left), without_lists(*binary.right)) {
                (Some(l), Some(r)) => Some(l.and(r)),
                (one, None) | (None, one) => one,
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            Some(or(without_lists(*binary.left)?, without_lists(*binary.right)?))
        }
        other => Some(other),
    }
}

/// `input` with the federated scan below it (through single-child wrappers)
/// narrowed by `filters`, each listing at most `max_keys` keys
/// ([`within_key_limit`]); `None` when nothing changes.
pub(crate) fn narrowed(
    input: &Arc<dyn ExecutionPlan>,
    filters: &[Arc<dyn PhysicalExpr>],
    max_keys: usize,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(remote) = input.downcast_ref::<VirtualExecutionPlan>() {
        let predicate =
            conjunction(filters.iter().filter_map(|filter| {
                within_key_limit(superset_of(filter, remote.plan().schema())?, max_keys)
            }));
        let Some(predicate) = predicate else {
            return Ok(None);
        };
        return Ok(narrowed_remote(remote, predicate)?.map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>));
    }
    let children = input.children();
    let [child] = children.as_slice() else {
        return Ok(None);
    };
    match narrowed(child, filters, max_keys)? {
        Some(child) => Ok(Some(Arc::clone(input).with_new_children(vec![child])?)),
        None => Ok(None),
    }
}

/// `remote` reading only the rows its source keeps of `predicate` (on its
/// output columns): the terms the source's policy accepts, in a filter over
/// its plan. Only over a table scan under filters, plain column projections
/// and aliases, where the filter's place does not change what the rest
/// computes (above a `LIMIT` it would); `None` for anything else, or when
/// the policy accepts no term.
fn narrowed_remote(remote: &VirtualExecutionPlan, predicate: Expr) -> Result<Option<VirtualExecutionPlan>> {
    let Some(mut optimize) = remote.executor().logical_optimizer() else {
        return Ok(None);
    };
    if !passes_columns(remote.plan()) {
        return Ok(None);
    }
    // The policy sends each marked term as it is, as a superset it offers,
    // or not at all (`restrict_federation`, `split_filter`).
    let marked = split_conjunction_owned(predicate)
        .into_iter()
        .map(crate::functions::superset_filter);
    let Some(marked) = conjunction(marked) else {
        return Ok(None);
    };
    // The plan's own filters were decided with it (a superset among them
    // is no longer marked): the policy judges the new terms alone.
    let planner = Arc::new(SQLFederationPlanner::new(Arc::clone(remote.executor())));
    let probe = LogicalPlan::Extension(Extension {
        node: Arc::new(FederatedPlanNode::new(
            LogicalPlan::Filter(Filter::try_new(
                marked,
                Arc::new(without_filters(remote.plan())?),
            )?),
            planner,
        )),
    });
    let LogicalPlan::Extension(Extension { node }) = optimize(probe)? else {
        return Ok(None);
    };
    let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>() else {
        return Ok(None);
    };
    let LogicalPlan::Filter(accepted) = federated.plan() else {
        return Ok(None);
    };
    let plan = LogicalPlan::Filter(Filter::try_new(
        accepted.predicate.clone(),
        Arc::new(remote.plan().clone()),
    )?);
    Ok(Some(VirtualExecutionPlan::new(
        plan,
        Arc::clone(remote.executor()),
        remote.statistics().clone(),
    )))
}

/// `plan` (see [`passes_columns`]) without its filters: the same columns.
fn without_filters(plan: &LogicalPlan) -> Result<LogicalPlan> {
    if let LogicalPlan::Filter(filter) = plan {
        return without_filters(&filter.input);
    }
    let inputs = plan
        .inputs()
        .into_iter()
        .map(without_filters)
        .collect::<Result<Vec<_>>>()?;
    plan.with_new_exprs(plan.expressions(), inputs)
}

/// A table scan under filters, plain column projections and aliases: each
/// row it returns is a row of the table.
fn passes_columns(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::Filter(filter) => passes_columns(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => passes_columns(&alias.input),
        LogicalPlan::Projection(projection) => {
            projection.expr.iter().all(|e| matches!(e, Expr::Column(_))) && passes_columns(&projection.input)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation_policy::PushdownPolicy;
    use crate::functions::as_superset_filter;
    use crate::testing::{int64, remote_table_recording, strings};
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_expr::expressions::{self as physical, lit as plit};
    use datafusion::prelude::SessionContext;
    use std::sync::Mutex;

    /// Numbers are compared exactly; text only as a marked `IN` superset
    /// (a case-insensitive source), never as a range.
    #[derive(Debug)]
    struct TextInAsSuperset;

    fn has_text(expr: &Expr) -> bool {
        expr.to_string().contains("Utf8")
    }

    impl PushdownPolicy for TextInAsSuperset {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Filter(f) => split_conjunction_owned(f.predicate.clone()).iter().all(|term| {
                    match as_superset_filter(term) {
                        Some(inner) => !has_text(inner) || matches!(inner, Expr::InList(_)),
                        None => !has_text(term),
                    }
                }),
                LogicalPlan::TableScan(_) | LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
                    true
                }
                _ => false,
            }
        }
    }

    fn session() -> SessionContext {
        crate::testing::federated_session()
    }

    fn local(ctx: &SessionContext, name: &str, ids: &[i64], codes: &[&str]) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("code", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![int64(ids), strings(codes)]).unwrap();
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }

    /// A remote `contracts` table that ignores its SQL (every row comes
    /// back) and records it.
    fn contracts(ctx: &SessionContext, policy: Option<Arc<dyn PushdownPolicy>>) -> Arc<Mutex<Vec<String>>> {
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3, 4, 5])),
                ("status", strings(&["a", "B", "b", "c", "d"])),
            ],
            policy,
        );
        ctx.register_table("contracts", table).unwrap();
        sent
    }

    async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        out.sort_unstable();
        out
    }

    fn sent(statements: &Arc<Mutex<Vec<String>>>) -> String {
        statements.lock().unwrap().join("\n")
    }

    #[tokio::test]
    async fn a_small_build_side_reaches_the_source_as_an_in_list_and_bounds() {
        let ctx = session();
        local(&ctx, "wanted", &[4, 2], &["x", "y"]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id = w.id",
        )
        .await;
        let sql = sent(&statements);
        assert!(sql.contains("IN (2, 4)") || sql.contains("IN (4, 2)"), "{sql}");
        assert!(
            sql.contains("FROM contracts AS k WHERE") && !sql.contains("contracts.customer_id"),
            "the keys name the alias the query reads: {sql}"
        );
        assert!(sql.contains(">= 2") && sql.contains("<= 4"), "{sql}");
        assert!(!sql.contains("codelake_superset"), "the marker is not SQL: {sql}");
        // The source ignores its SQL: the join alone keeps the exact rows.
        assert_eq!(rows, vec![2, 4]);
    }

    #[tokio::test]
    async fn an_in_subquery_is_a_semi_join_whose_keys_reach_the_source() {
        let ctx = session();
        local(&ctx, "wanted", &[5, 1], &["x", "y"]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        let rows = ids(
            &ctx,
            "SELECT customer_id FROM contracts WHERE customer_id IN (SELECT id FROM wanted)",
        )
        .await;
        let sql = sent(&statements);
        assert!(sql.contains(">= 1") && sql.contains("<= 5"), "{sql}");
        assert_eq!(rows, vec![1, 5]);
    }

    #[tokio::test]
    async fn text_keys_go_as_an_in_list_never_as_a_range() {
        let ctx = session();
        local(&ctx, "wanted", &[0, 0], &["b", "a"]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.status = w.code",
        )
        .await;
        let sql = sent(&statements);
        assert!(
            sql.contains("IN ('a', 'b')") || sql.contains("IN ('b', 'a')"),
            "{sql}"
        );
        assert!(!sql.contains(">="), "a text range is not a superset: {sql}");
        // 'B' comes back from a case-insensitive source and the join drops it.
        assert_eq!(rows, vec![1, 3]);
    }

    #[tokio::test]
    async fn past_the_in_list_limit_only_the_bounds_are_sent() {
        let ctx = session();
        ctx.sql("SET datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values = 1")
            .await
            .unwrap();
        local(&ctx, "wanted", &[2, 3], &["x", "y"]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id = w.id",
        )
        .await;
        let sql = sent(&statements);
        assert!(sql.contains(">= 2") && sql.contains("<= 3"), "{sql}");
        assert!(!sql.contains(" IN "), "{sql}");
        assert_eq!(rows, vec![2, 3]);
    }

    #[tokio::test]
    async fn the_keys_of_every_partition_count_together_against_the_limit() {
        let ctx = session();
        for statement in [
            "SET datafusion.execution.target_partitions = 4",
            "SET datafusion.optimizer.hash_join_single_partition_threshold = 0",
            "SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
            "SET datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values = 7",
        ] {
            ctx.sql(statement).await.unwrap().collect().await.unwrap();
        }
        local(&ctx, "wanted", &[1, 2, 3, 4, 5, 6, 7, 8], &["x"; 8]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        // A partitioned join that builds `wanted`, up to 7 keys per partition
        // (they fall in two).
        let sql = "SELECT k.customer_id FROM contracts k JOIN wanted w ON k.customer_id = w.id";
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 5);
        let shown = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(shown.contains("IN (SET)"), "partitions list their keys: {shown}");
        // 8 keys together: the source gets the bounds of each partition.
        let sent = sent(&statements);
        assert!(!sent.contains(" IN ("), "{sent}");
        assert!(sent.contains(">= ") && sent.contains("<= "), "{sent}");
    }

    #[tokio::test]
    async fn a_null_safe_join_gets_the_null_keys_from_the_source() {
        let ctx = session();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let wanted = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![Some(3), None]))],
        )
        .unwrap();
        ctx.register_table(
            "wanted",
            Arc::new(MemTable::try_new(schema, vec![vec![wanted]]).unwrap()),
        )
        .unwrap();
        let (table, statements) = remote_table_recording(
            "crm",
            "contracts",
            vec![(
                "customer_id",
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(5)])),
            )],
            Some(Arc::new(TextInAsSuperset)),
        );
        ctx.register_table("contracts", table).unwrap();
        for sql in [
            "SELECT id FROM wanted INTERSECT SELECT customer_id FROM contracts",
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id IS NOT DISTINCT FROM w.id",
        ] {
            statements.lock().unwrap().clear();
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let (rows, nulls) = batches.iter().fold((0, 0), |(rows, nulls), b| {
                (rows + b.num_rows(), nulls + b.column(0).null_count())
            });
            assert_eq!((rows, nulls), (2, 1), "{sql}");
            let sent = sent(&statements);
            assert!(sent.contains("IN (3)"), "{sql}: {sent}");
            assert!(
                sent.contains("customer_id IN (3) OR ") && sent.contains(".customer_id IS NULL)"),
                "a source honouring its SQL would drop the NULL key: {sent}"
            );
        }
    }

    #[tokio::test]
    async fn without_a_policy_nothing_is_added() {
        let ctx = session();
        local(&ctx, "wanted", &[2], &["x"]);
        let statements = contracts(&ctx, None);
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id = w.id",
        )
        .await;
        let sql = sent(&statements);
        assert!(!sql.contains("WHERE"), "{sql}");
        assert_eq!(rows, vec![2]);
    }

    #[tokio::test]
    async fn the_preserved_side_of_an_outer_join_gets_no_filter() {
        let ctx = session();
        local(&ctx, "wanted", &[2], &["x"]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w RIGHT JOIN contracts k ON k.customer_id = w.id",
        )
        .await;
        let sql = sent(&statements);
        assert!(!sql.contains("WHERE"), "{sql}");
        assert_eq!(rows, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn the_keys_join_the_filter_already_sent_and_explain_shows_them() {
        let ctx = session();
        local(&ctx, "wanted", &[4, 2], &["x", "y"]);
        let statements = contracts(&ctx, Some(Arc::new(TextInAsSuperset)));
        let sql = "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id = w.id \
                   WHERE k.customer_id < 4";
        let rows = ids(&ctx, sql).await;
        let sent = sent(&statements);
        assert!(sent.contains("< 4") && sent.contains(" IN ("), "{sent}");
        // `< 4` went to the source as exact, and this one ignores its SQL.
        assert_eq!(rows, vec![2, 4]);
        let plan = ctx
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let plan = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(
            plan.contains("FederationFilterBarrier: dynamic_filters=[DynamicFilter"),
            "{plan}"
        );
    }

    #[tokio::test]
    async fn keys_of_a_computed_column_are_not_sent() {
        /// Plain column projections only: the computed key stays local.
        #[derive(Debug)]
        struct PlainProjections;
        impl PushdownPolicy for PlainProjections {
            fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
                match plan {
                    LogicalPlan::Projection(p) => p.expr.iter().all(|e| matches!(e, Expr::Column(_))),
                    other => TextInAsSuperset.node_is_pushable(other),
                }
            }
        }
        let ctx = session();
        local(&ctx, "wanted", &[3], &["x"]);
        let statements = contracts(&ctx, Some(Arc::new(PlainProjections)));
        let rows = ids(
            &ctx,
            "SELECT k.cid FROM wanted w JOIN (SELECT customer_id + 1 AS cid FROM contracts) k ON k.cid = w.id",
        )
        .await;
        let sent = sent(&statements);
        assert!(!sent.contains("WHERE"), "{sent}");
        assert_eq!(rows, vec![3]);
    }

    /// Any projection, sort and limit, and a text term only as a superset
    /// of itself (marked).
    #[derive(Debug)]
    struct Everything;

    impl PushdownPolicy for Everything {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Filter(f) => split_conjunction_owned(f.predicate.clone())
                    .iter()
                    .all(|term| as_superset_filter(term).is_some() || !has_text(term)),
                LogicalPlan::TableScan(_)
                | LogicalPlan::Projection(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_) => true,
                _ => false,
            }
        }
        fn superset(&self, term: &Expr, _schema: &DFSchema) -> Option<Expr> {
            has_text(term).then(|| term.clone())
        }
    }

    #[tokio::test]
    async fn keys_go_only_where_each_row_is_a_row_of_the_table() {
        let ctx = session();
        local(&ctx, "wanted", &[3], &["x"]);
        let statements = contracts(&ctx, Some(Arc::new(Everything)));
        for sql in [
            // Above a LIMIT the keys would change which rows it keeps.
            "SELECT k.customer_id FROM wanted w
             JOIN (SELECT customer_id FROM contracts ORDER BY customer_id LIMIT 4) k ON k.customer_id = w.id",
            // A computed column is not the table's.
            "SELECT k.customer_id FROM wanted w
             JOIN (SELECT customer_id * 2 AS customer_id FROM contracts) k ON k.customer_id = w.id",
        ] {
            statements.lock().unwrap().clear();
            ids(&ctx, sql).await;
            let sent = sent(&statements);
            assert!(!sent.contains(" IN (") && !sent.contains(">= 3"), "{sql}: {sent}");
        }
    }

    #[tokio::test]
    async fn the_policy_judges_the_keys_alone_not_the_filters_already_sent() {
        let ctx = session();
        local(&ctx, "wanted", &[3], &["x"]);
        let statements = contracts(&ctx, Some(Arc::new(Everything)));
        // `status = 'b'` was sent as a superset (then unmarked): judged again,
        // it would stay local and nothing would be added.
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id = w.id WHERE k.status = 'b'",
        )
        .await;
        let sent = sent(&statements);
        assert!(
            sent.contains("\"status\" = 'b'") && sent.contains("IN (3)"),
            "{sent}"
        );
        assert_eq!(rows, vec![3]);
    }

    #[tokio::test]
    async fn a_key_the_source_rejects_goes_as_the_superset_it_offers() {
        /// Refuses `<=` and offers `< v + 1` for it.
        #[derive(Debug)]
        struct NoLessOrEqual;
        fn less_or_equal(expr: &Expr) -> bool {
            matches!(expr, Expr::BinaryExpr(b) if b.op == Operator::LtEq)
        }
        impl PushdownPolicy for NoLessOrEqual {
            fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
                match plan {
                    LogicalPlan::Filter(f) => split_conjunction_owned(f.predicate.clone())
                        .iter()
                        .all(|term| !less_or_equal(as_superset_filter(term).unwrap_or(term))),
                    other => TextInAsSuperset.node_is_pushable(other),
                }
            }
            fn superset(&self, term: &Expr, _schema: &DFSchema) -> Option<Expr> {
                let Expr::BinaryExpr(b) = term else { return None };
                let Expr::Literal(ScalarValue::Int64(Some(v)), _) = b.right.as_ref() else {
                    return None;
                };
                less_or_equal(term).then(|| b.left.as_ref().clone().lt(lit(v + 1)))
            }
        }
        let ctx = session();
        ctx.sql("SET datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values = 1")
            .await
            .unwrap();
        local(&ctx, "wanted", &[2, 4], &["x", "y"]);
        let statements = contracts(&ctx, Some(Arc::new(NoLessOrEqual)));
        let rows = ids(
            &ctx,
            "SELECT k.customer_id FROM wanted w JOIN contracts k ON k.customer_id = w.id",
        )
        .await;
        let sent = sent(&statements);
        assert!(sent.contains(">= 2") && sent.contains("< 5"), "{sent}");
        assert!(!sent.contains("<="), "{sent}");
        assert_eq!(rows, vec![2, 4]);
    }

    fn schema() -> DFSchema {
        DFSchema::try_from_qualified_schema(
            "t",
            &Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ]),
        )
        .unwrap()
    }

    fn a() -> Arc<dyn PhysicalExpr> {
        Arc::new(physical::Column::new("a", 0))
    }

    fn s() -> Arc<dyn PhysicalExpr> {
        Arc::new(physical::Column::new("s", 1))
    }

    fn binary(l: Arc<dyn PhysicalExpr>, op: Operator, r: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    /// An expression this cannot translate (like a join's hash lookup).
    fn opaque() -> Arc<dyn PhysicalExpr> {
        Arc::new(physical::NotExpr::new(binary(a(), Operator::Eq, plit(1_i64))))
    }

    fn column(name: &str) -> Expr {
        col(Column::new(Some("t"), name))
    }

    #[test]
    fn comparisons_lists_and_nulls_translate_on_the_columns() {
        let schema = schema();
        let ge = binary(a(), Operator::GtEq, plit(2_i64));
        assert_eq!(superset_of(&ge, &schema), Some(column("a").gt_eq(lit(2_i64))));
        let flipped = binary(plit(2_i64), Operator::Lt, a());
        assert_eq!(superset_of(&flipped, &schema), Some(lit(2_i64).lt(column("a"))));
        for op in [
            Operator::Eq,
            Operator::NotEq,
            Operator::Lt,
            Operator::LtEq,
            Operator::Gt,
        ] {
            let term = binary(a(), op, plit(2_i64));
            assert!(superset_of(&term, &schema).is_some(), "{op}");
        }
        let list: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(a(), vec![plit(1_i64), plit(3_i64)], false, schema.as_arrow()).unwrap(),
        );
        assert_eq!(
            superset_of(&list, &schema),
            Some(column("a").in_list(vec![lit(1_i64), lit(3_i64)], false))
        );
        let with_null: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                a(),
                vec![plit(1_i64), plit(ScalarValue::Int64(None))],
                false,
                schema.as_arrow(),
            )
            .unwrap(),
        );
        assert_eq!(
            superset_of(&with_null, &schema),
            Some(column("a").in_list(vec![lit(1_i64)], false)),
            "a NULL key matches nothing"
        );
        let only_null: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                a(),
                vec![plit(ScalarValue::Int64(None))],
                false,
                schema.as_arrow(),
            )
            .unwrap(),
        );
        assert_eq!(superset_of(&only_null, &schema), Some(lit(false)));
        let not_in: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                a(),
                vec![plit(1_i64), plit(ScalarValue::Int64(None))],
                true,
                schema.as_arrow(),
            )
            .unwrap(),
        );
        assert_eq!(
            superset_of(&not_in, &schema),
            Some(column("a").in_list(vec![lit(1_i64), lit(ScalarValue::Int64(None))], true)),
            "NOT IN with a NULL keeps nothing, as sent"
        );
        let null: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(a()));
        assert_eq!(superset_of(&null, &schema), Some(column("a").is_null()));
        let not_null: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(s()));
        assert_eq!(superset_of(&not_null, &schema), Some(column("s").is_not_null()));
        let arithmetic = binary(a(), Operator::Plus, plit(1_i64));
        assert_eq!(superset_of(&arithmetic, &schema), None);
    }

    #[test]
    fn what_cannot_be_said_only_widens() {
        let schema = schema();
        let ge = binary(a(), Operator::GtEq, plit(2_i64));
        let both = binary(Arc::clone(&ge), Operator::And, opaque());
        assert_eq!(
            superset_of(&both, &schema),
            Some(column("a").gt_eq(lit(2_i64))),
            "AND keeps the known half"
        );
        let flipped = binary(opaque(), Operator::And, Arc::clone(&ge));
        assert_eq!(
            superset_of(&flipped, &schema),
            Some(column("a").gt_eq(lit(2_i64)))
        );
        let either = binary(Arc::clone(&ge), Operator::Or, opaque());
        assert_eq!(
            superset_of(&either, &schema),
            None,
            "OR of an unknown is anything"
        );
        let either = binary(opaque(), Operator::Or, Arc::clone(&ge));
        assert_eq!(superset_of(&either, &schema), None);
        let le = binary(a(), Operator::LtEq, plit(9_i64));
        assert_eq!(
            superset_of(&binary(Arc::clone(&ge), Operator::Or, le), &schema),
            Some(column("a").gt_eq(lit(2_i64)).or(column("a").lt_eq(lit(9_i64))))
        );
        assert_eq!(superset_of(&opaque(), &schema), None);
    }

    #[test]
    fn each_branch_of_a_case_is_one_of_the_rows_kept() {
        let schema = schema();
        let branch = |v: i64| binary(a(), Operator::Eq, plit(v));
        let case: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(binary(a(), Operator::Modulo, plit(2_i64))),
                vec![(plit(0_i64), branch(2)), (plit(1_i64), branch(3))],
                Some(plit(false)),
            )
            .unwrap(),
        );
        assert_eq!(
            superset_of(&case, &schema),
            Some(column("a").eq(lit(2_i64)).or(column("a").eq(lit(3_i64)))),
            "the ELSE of empty partitions keeps nothing"
        );
        let empty_first: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(binary(a(), Operator::Modulo, plit(2_i64))),
                vec![(plit(0_i64), plit(false)), (plit(1_i64), branch(3))],
                Some(plit(ScalarValue::Boolean(None))),
            )
            .unwrap(),
        );
        assert_eq!(
            superset_of(&empty_first, &schema),
            Some(column("a").eq(lit(3_i64)))
        );
        assert_eq!(superset_of(&plit(true), &schema), None, "keeps everything");
        assert_eq!(superset_of(&plit(false), &schema), Some(lit(false)));
        let unknown: Arc<dyn PhysicalExpr> = Arc::new(
            CaseExpr::try_new(
                Some(binary(a(), Operator::Modulo, plit(2_i64))),
                vec![(plit(0_i64), branch(2)), (plit(1_i64), opaque())],
                None,
            )
            .unwrap(),
        );
        assert_eq!(superset_of(&unknown, &schema), None);
    }

    #[test]
    fn past_the_key_limit_together_the_lists_are_dropped() {
        let keys = |values: &[i64]| column("a").in_list(values.iter().map(|v| lit(*v)).collect(), false);
        let bounds = |lo: i64, hi: i64| column("a").gt_eq(lit(lo)).and(column("a").lt_eq(lit(hi)));
        // The filter of a partitioned join: bounds and keys per partition.
        let two = or(
            bounds(1, 2).and(keys(&[1, 2])),
            bounds(5, 9).and(keys(&[5, 7, 9])),
        );
        assert_eq!(within_key_limit(two.clone(), 5), Some(two.clone()), "5 keys");
        assert_eq!(
            within_key_limit(two, 4),
            Some(or(bounds(1, 2), bounds(5, 9))),
            "the keys of both partitions count"
        );
        // An AND keeps its other side, on either side of the list.
        assert_eq!(
            within_key_limit(keys(&[1, 2]).and(bounds(1, 2)), 1),
            Some(bounds(1, 2))
        );
        // A list alone, or an OR with a branch reduced to it, keeps every row.
        assert_eq!(within_key_limit(keys(&[1, 2]), 1), None);
        assert_eq!(within_key_limit(or(keys(&[1, 2]), bounds(5, 9)), 1), None);
        assert_eq!(within_key_limit(or(bounds(5, 9), keys(&[1, 2])), 1), None);
        // Anything else stays.
        let null = column("a").is_null();
        assert_eq!(within_key_limit(null.clone(), 0), Some(null.clone()));
        assert_eq!(
            within_key_limit(null.clone().or(keys(&[1])).and(keys(&[2])), 1),
            None,
            "an OR of a list is every row"
        );
        assert_eq!(within_key_limit(null.clone().and(keys(&[1, 2])), 1), Some(null));
    }

    #[test]
    fn a_dynamic_filter_counts_only_once_complete() {
        let schema = schema();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![a()], plit(true)));
        let filter: Arc<dyn PhysicalExpr> = Arc::clone(&dynamic) as _;
        assert!(is_dynamic(&filter));
        assert!(is_dynamic(&binary(Arc::clone(&filter), Operator::Or, opaque())));
        assert!(!is_dynamic(&opaque()));
        dynamic.update(binary(a(), Operator::GtEq, plit(7_i64))).unwrap();
        assert_eq!(superset_of(&filter, &schema), None, "still being built");
        dynamic.mark_complete();
        assert_eq!(superset_of(&filter, &schema), Some(column("a").gt_eq(lit(7_i64))));
    }

    #[test]
    fn an_or_with_a_single_term_is_sent_term_by_term() {
        let schema = schema();
        let bounds = binary(
            binary(a(), Operator::GtEq, plit(2_i64)),
            Operator::And,
            binary(s(), Operator::Eq, plit("x")),
        );
        let null_key: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(a()));
        let expected = column("a")
            .gt_eq(lit(2_i64))
            .or(column("a").is_null())
            .and(column("s").eq(lit("x")).or(column("a").is_null()));
        assert_eq!(
            superset_of(
                &binary(Arc::clone(&bounds), Operator::Or, Arc::clone(&null_key)),
                &schema
            ),
            Some(expected),
            "the dynamic filter of a null-safe join"
        );
        assert_eq!(
            superset_of(
                &binary(Arc::clone(&null_key), Operator::Or, Arc::clone(&bounds)),
                &schema
            ),
            Some(
                column("a")
                    .gt_eq(lit(2_i64))
                    .or(column("a").is_null())
                    .and(column("s").eq(lit("x")).or(column("a").is_null()))
            )
        );
        let both = superset_of(&binary(Arc::clone(&bounds), Operator::Or, bounds), &schema);
        let one = column("a").gt_eq(lit(2_i64)).and(column("s").eq(lit("x")));
        assert_eq!(both, Some(one.clone().or(one)), "two conjunctions stay one term");
    }

    #[test]
    fn literals_take_the_column_type_and_columns_must_match_the_schema() {
        let schema = schema();
        let view = plit(ScalarValue::Utf8View(Some("x".into())));
        let eq = binary(s(), Operator::Eq, Arc::clone(&view));
        assert_eq!(superset_of(&eq, &schema), Some(column("s").eq(lit("x"))));
        let list: Arc<dyn PhysicalExpr> =
            Arc::new(InListExpr::try_new(s(), vec![Arc::clone(&view)], true, schema.as_arrow()).unwrap());
        assert_eq!(
            superset_of(&list, &schema),
            Some(column("s").in_list(vec![lit("x")], true))
        );
        let impossible = binary(a(), Operator::Eq, plit("not a number"));
        assert_eq!(superset_of(&impossible, &schema), None);
        let two_literals = binary(plit(1_i64), Operator::Eq, plit(ScalarValue::Int32(Some(1))));
        assert_eq!(
            superset_of(&two_literals, &schema),
            Some(lit(1_i64).eq(lit(ScalarValue::Int32(Some(1)))))
        );
        assert_eq!(
            superset_of(&binary(a(), Operator::Lt, plit(1.5_f64)), &schema),
            None,
            "1.5 as an integer would move the bound"
        );
        let past: Arc<dyn PhysicalExpr> = Arc::new(physical::Column::new("a", 2));
        assert_eq!(
            superset_of(&binary(past, Operator::Eq, plit(1_i64)), &schema),
            None
        );
        let renamed: Arc<dyn PhysicalExpr> = Arc::new(physical::Column::new("b", 0));
        assert_eq!(
            superset_of(&binary(renamed, Operator::Eq, plit(1_i64)), &schema),
            None
        );
        let cast: Arc<dyn PhysicalExpr> = Arc::new(CastExpr::new(a(), DataType::Float64, None));
        assert_eq!(
            superset_of(&binary(cast, Operator::Gt, plit(1.5_f64)), &schema),
            Some(
                Expr::Cast(datafusion::logical_expr::Cast::new(
                    Box::new(column("a")),
                    DataType::Float64
                ))
                .gt(lit(1.5_f64))
            )
        );
    }
}
