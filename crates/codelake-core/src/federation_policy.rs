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
use datafusion::logical_expr::{
    Expr, Extension, FetchType, Filter, LogicalPlan, LogicalPlanBuilder, Projection, SkipType, Sort,
    SubqueryAlias, TableScan,
};
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

    /// Whether the source may return only the first rows of `sort` (as
    /// many as a `LIMIT` above it keeps), ordered by its keys: the rows it
    /// picks must be rows the engine's order could pick (its order agrees
    /// on the keys, NULLs included). Asked only when everything below the
    /// sort reaches the source exactly; the engine sorts what comes back
    /// again.
    fn first_rows(&self, _sort: &Sort) -> bool {
        false
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

/// `plan` federated as its source will receive it, or `None` when the
/// unparser would not write one of its joins as the plan means it
/// ([`crate::federated_joins`]): the caller then federates less.
fn federate_prepared(
    plan: LogicalPlan,
    policy: &dyn PushdownPolicy,
    planner: &Arc<dyn FederationPlanner>,
) -> Result<Option<LogicalPlan>> {
    let prepared = policy.prepare(unmark_supersets(plan)?)?;
    let prepared = crate::federated_joins::for_unparser(aliased_scan_filters(prepared)?)?;
    if !crate::federated_joins::renders_faithfully(&prepared) {
        return Ok(None);
    }
    Ok(Some(federate(with_select_list(prepared)?, planner)))
}

/// An aggregation at the root of a sub-plan renders its aggregates before its
/// groups (`SELECT count(1), k … GROUP BY k`), the reverse of its schema; the
/// engine reads the source's columns by position, so each value would land
/// in another column, without any error when the types cast. A projection of
/// its columns renders them in schema order.
fn with_select_list(plan: LogicalPlan) -> Result<LogicalPlan> {
    if !matches!(plan, LogicalPlan::Aggregate(_)) {
        return Ok(plan);
    }
    let columns = plan.schema().columns().into_iter().map(Expr::Column).collect();
    Ok(LogicalPlan::Projection(Projection::try_new(
        columns,
        Arc::new(plan),
    )?))
}

/// `FROM t AS r WHERE …` reaches here as filters between the alias and the
/// scan that name the table, which the unparser renders as written
/// (`FROM t AS [r] WHERE [t].[c] = 1`, "could not be bound"). As filters of
/// the scan, it renders them against the alias.
fn aliased_scan_filters(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| {
        let LogicalPlan::SubqueryAlias(aliased) = &node else {
            return Ok(Transformed::no(node));
        };
        let mut filters = Vec::new();
        let mut input = aliased.input.as_ref();
        while let LogicalPlan::Filter(filter) = input {
            filters.extend(split_conjunction_owned(filter.predicate.clone()));
            input = filter.input.as_ref();
        }
        let LogicalPlan::TableScan(scan) = input else {
            return Ok(Transformed::no(node));
        };
        if filters.is_empty() || scan.fetch.is_some() {
            return Ok(Transformed::no(node));
        }
        let mut scan = scan.clone();
        scan.filters.extend(filters);
        let alias = aliased.alias.clone();
        let scan = Arc::new(LogicalPlan::TableScan(scan));
        Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
            SubqueryAlias::try_new(scan, alias)?,
        )))
    })
    .data()
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
    !projection_over_projection(plan)
        && !aggregate_over_derived_columns(plan)
        && policy.node_is_pushable(plan)
        && plan
            .inputs()
            .into_iter()
            .all(|input| fully_pushable(input, policy))
}

/// A projection straight over another (a hive view read through its declared
/// columns) renders as nested derived tables whose outer columns keep the
/// inner table's name, which SQL Server and PostgreSQL reject ("could not be
/// bound", "missing FROM-clause entry"): the upper one runs in the engine and
/// the lower one, with what is below it, still reaches the source.
fn projection_over_projection(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Projection(p) if matches!(p.input.as_ref(), LogicalPlan::Projection(_)))
}

/// An aggregation over a projection that computes a column renders the
/// projection as a derived table, and the aggregation keeps naming the
/// projection's other columns by their table (`SELECT count(DISTINCT [o].[k])
/// FROM (SELECT [o].[k], count(1) AS [c] …) AS [derived_projection]`, "could
/// not be bound"): the aggregation runs in the engine. A projection of plain
/// columns is left to the policy's `prepare`, which can drop it.
fn aggregate_over_derived_columns(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Aggregate(aggregate) = plan else {
        return false;
    };
    let LogicalPlan::Projection(projection) = aggregate.input.as_ref() else {
        return false;
    };
    !projection.expr.iter().all(|e| matches!(e, Expr::Column(_)))
        && projection.schema.iter().any(|(qualifier, _)| qualifier.is_some())
}

fn split(
    plan: LogicalPlan,
    policy: &dyn PushdownPolicy,
    planner: &Arc<dyn FederationPlanner>,
) -> Result<LogicalPlan> {
    if fully_pushable(&plan, policy)
        && let Some(federated) = federate_prepared(plan.clone(), policy, planner)?
    {
        return Ok(federated);
    }
    if let Some(first) = first_rows(&plan, policy, planner)? {
        return Ok(first);
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
    let local = without_local_supersets(with_new_inputs(&plan, children)?)?;
    crate::overflow::check_local_node(local)
}

/// `ORDER BY … LIMIT n [OFFSET m]` over a sub-plan the source evaluates
/// exactly: the source sends its first `m + n` rows (`SELECT TOP … ORDER
/// BY`), not all of them, and the engine sorts them again before skipping
/// and keeping its own. Not over a superset filter: the engine would drop
/// some of the source's first rows and miss the ones after them.
fn first_rows(
    plan: &LogicalPlan,
    policy: &dyn PushdownPolicy,
    planner: &Arc<dyn FederationPlanner>,
) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Limit(limit) = plan else {
        return Ok(None);
    };
    // Projections of plain columns may sit between them (`SELECT a … ORDER BY b`).
    let mut between = Vec::new();
    let mut node = limit.input.as_ref();
    while let LogicalPlan::Projection(projection) = node
        && projection.expr.iter().all(|e| matches!(e, Expr::Column(_)))
    {
        between.push(node);
        node = projection.input.as_ref();
    }
    let LogicalPlan::Sort(sort) = node else {
        return Ok(None);
    };
    let (Ok(SkipType::Literal(skip)), Ok(FetchType::Literal(Some(fetch)))) =
        (limit.get_skip_type(), limit.get_fetch_type())
    else {
        return Ok(None);
    };
    let Some(rows) = skip.checked_add(fetch).filter(|n| *n > 0) else {
        return Ok(None);
    };
    // Exact: a superset filter is only made below a term the policy
    // rejects, which then stays above it, out of this sub-plan.
    if !fully_pushable(&sort.input, policy) || !policy.first_rows(sort) {
        return Ok(None);
    }
    let first = LogicalPlanBuilder::from(Arc::clone(&sort.input))
        .sort(sort.expr.clone())?
        .limit(0, Some(rows))?
        .build()?;
    let Some(federated) = federate_prepared(first, policy, planner)? else {
        return Ok(None);
    };
    let mut local = LogicalPlan::Sort(Sort {
        expr: sort.expr.clone(),
        input: Arc::new(federated),
        fetch: sort.fetch,
    });
    for node in between.into_iter().rev() {
        local = with_new_inputs(node, vec![local])?;
    }
    with_new_inputs(plan, vec![local]).map(Some)
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
/// offered again; a marked term the policy rejects is offered as the
/// predicate it marks). Rewritten only when both halves are non-empty; the
/// filter below has no unsafe term, so recursion terminates.
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
        .filter_map(|term| {
            let term = crate::functions::as_superset_filter(term).unwrap_or(term);
            policy.superset(term, input.schema())
        })
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
    // A table scan has no join: always federated.
    let bare = federate_prepared(LogicalPlan::TableScan(scan), policy, planner)?.ok_or_else(|| {
        datafusion::error::DataFusionError::Internal("a table scan has no join to refuse".into())
    })?;
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
/// `optimize_projections` cannot see inside the node, and puts nothing above
/// it under a join. This rule, right after it, carries the columns each node
/// reads down the plan (projections, filters, joins with their keys, sorts,
/// windows, aggregations, aliases, unions) to every federated node, and
/// narrows it to those, as a projection of plain columns every source can
/// evaluate, when that sub-plan
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
        None
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        // A plan this cannot rebuild keeps every column: pruning is only an
        // optimization, never a reason to fail the query.
        prune(plan.clone(), None).or_else(|_| Ok(Transformed::no(plan)))
    }
}

/// Columns a node's parent reads from it; `None` when unknown (all of them).
type Needed = Option<HashSet<Column>>;

fn columns_of<'a>(exprs: impl IntoIterator<Item = &'a Expr>) -> HashSet<Column> {
    exprs
        .into_iter()
        .flat_map(|e| e.column_refs().into_iter().cloned())
        .collect()
}

fn within(needed: &HashSet<Column>, schema: &DFSchema) -> HashSet<Column> {
    needed.iter().filter(|c| schema.has_column(c)).cloned().collect()
}

/// What each input of `plan` must provide, from what its parent needs of it.
fn needs_of_inputs(plan: &LogicalPlan, needed: Needed) -> Vec<Needed> {
    let with = |mut needed: HashSet<Column>, more: HashSet<Column>| {
        needed.extend(more);
        needed
    };
    match plan {
        LogicalPlan::Projection(p) => vec![Some(columns_of(&p.expr))],
        LogicalPlan::Aggregate(a) => vec![Some(columns_of(a.group_expr.iter().chain(&a.aggr_expr)))],
        LogicalPlan::Filter(f) => vec![needed.map(|n| with(n, columns_of([&f.predicate])))],
        LogicalPlan::Sort(s) => vec![needed.map(|n| with(n, columns_of(s.expr.iter().map(|e| &e.expr))))],
        LogicalPlan::Limit(_) => vec![needed],
        LogicalPlan::Window(w) => {
            vec![needed.map(|n| within(&with(n, columns_of(&w.window_expr)), w.input.schema()))]
        }
        LogicalPlan::Join(j) => match needed {
            None => vec![None, None],
            Some(n) => {
                let keys = j.on.iter().flat_map(|(l, r)| [l, r]).chain(j.filter.as_ref());
                let all = with(n, columns_of(keys));
                vec![
                    Some(within(&all, j.left.schema())),
                    Some(within(&all, j.right.schema())),
                ]
            }
        },
        // An alias and a union rename their input's columns position by position.
        LogicalPlan::SubqueryAlias(_) | LogicalPlan::Union(_) => match needed {
            None => plan.inputs().iter().map(|_| None).collect(),
            Some(n) => {
                let positions: Vec<usize> = plan
                    .schema()
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| n.contains(c))
                    .map(|(i, _)| i)
                    .collect();
                plan.inputs()
                    .iter()
                    .map(|input| {
                        Some(
                            positions
                                .iter()
                                .map(|i| Column::from(input.schema().qualified_field(*i)))
                                .collect(),
                        )
                    })
                    .collect()
            }
        },
        _ => plan.inputs().iter().map(|_| None).collect(),
    }
}

/// `plan` with each federated table scan below it narrowed to the columns
/// the nodes above it read. Each input says whether it changed: a node is
/// rebuilt when any of its inputs did, whatever was pruned elsewhere in the
/// plan (every branch of a union, both sides of a join).
fn prune(plan: LogicalPlan, needed: Needed) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Extension(Extension { node }) = &plan {
        if let (Some(federated), Some(needed)) = (node.as_any().downcast_ref::<FederatedPlanNode>(), &needed)
            && let Some(pruned) = prune_federated(federated, needed)?
        {
            return Ok(Transformed::yes(pruned));
        }
        return Ok(Transformed::no(plan));
    }
    let needs = needs_of_inputs(&plan, needed);
    let inputs = plan
        .inputs()
        .into_iter()
        .cloned()
        .zip(needs.iter().cloned())
        .map(|(input, need)| prune(input, need))
        .collect::<Result<Vec<_>>>()?;
    if !inputs.iter().any(|input| input.transformed) {
        return Ok(Transformed::no(plan));
    }
    let inputs: Vec<LogicalPlan> = inputs.into_iter().map(|input| input.data).collect();
    if !lined_up(&plan, &needs, &inputs) {
        return Ok(Transformed::no(plan));
    }
    with_new_inputs(&plan, inputs).map(Transformed::yes)
}

/// A union reads its inputs by position, so its narrowed `inputs` must still
/// line up: each returns every column it did, or each exactly the columns
/// the union reads (`needs`). One input narrowed and another not (a
/// sub-plan [`prune_federated`] leaves alone), or each keeping other columns
/// of its own (a filter's), would put another column at a position: such a
/// union keeps its inputs as they were. Any other plan names its columns.
fn lined_up(plan: &LogicalPlan, needs: &[Needed], inputs: &[LogicalPlan]) -> bool {
    let LogicalPlan::Union(union) = plan else {
        return true;
    };
    let columns = |input: &LogicalPlan| input.schema().columns();
    let as_before = union
        .inputs
        .iter()
        .zip(inputs)
        .all(|(before, input)| columns(before) == columns(input));
    let as_needed = union
        .inputs
        .iter()
        .zip(needs)
        .zip(inputs)
        .all(|((before, need), input)| {
            need.as_ref().is_some_and(|need| {
                let kept: Vec<Column> = columns(before).into_iter().filter(|c| need.contains(c)).collect();
                kept == columns(input)
            })
        });
    as_before || as_needed
}

/// `plan` over `inputs`, its expressions unchanged. An unnest lists the
/// columns it expands among its expressions but takes none back
/// (`with_new_exprs` asserts it gets none), so it is rebuilt from its inputs
/// alone.
fn with_new_inputs(plan: &LogicalPlan, inputs: Vec<LogicalPlan>) -> Result<LogicalPlan> {
    let expressions = match plan {
        LogicalPlan::Unnest(_) => Vec::new(),
        _ => plan.expressions(),
    };
    plan.with_new_exprs(expressions, inputs)
}

/// The federated table scan `federated` (under filters, aliased or not)
/// returning only the columns in `needed`; `None` when there is nothing to
/// prune or it is not such a scan.
fn prune_federated(federated: &FederatedPlanNode, needed: &HashSet<Column>) -> Result<Option<LogicalPlan>> {
    // Federation wraps each input of a cross-source join in a projection of
    // its plain columns, and a join sent whole gets one
    // (`crate::federated_joins`): that list narrows, and so do the lists of
    // the derived tables below it.
    if let LogicalPlan::Projection(projection) = &federated.plan
        && projection.expr.iter().all(|e| matches!(e, Expr::Column(_)))
    {
        let narrowed = narrow(federated.plan.clone(), Some(needed.clone()))?;
        if !narrowed.transformed {
            return Ok(None);
        }
        return Ok(Some(federate(narrowed.data, &federated.planner)));
    }
    // `FROM t AS r`: the projection goes below the alias, as a plain
    // `SELECT` of the table (`FROM (SELECT a, b FROM t) AS r`).
    let (alias, inner) = match &federated.plan {
        LogicalPlan::SubqueryAlias(aliased) => (Some(aliased.alias.clone()), aliased.input.as_ref()),
        other => (None, other),
    };
    if !is_filtered_scan(inner) {
        return Ok(None);
    }
    let positions: Vec<usize> = federated
        .plan
        .schema()
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, column)| needed.contains(column))
        .map(|(position, _)| position)
        .collect();
    // Nothing to prune, or nothing left: a projection without columns has
    // no portable SQL (`SELECT FROM …`).
    if positions.is_empty() || positions.len() == federated.plan.schema().fields().len() {
        return Ok(None);
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
    // The scan's own list of columns too, which the unparser writes below
    // the alias.
    let pruned = narrow(pruned, Some(needed.clone()))?.data;
    Ok(Some(federate(pruned, &federated.planner)))
}

/// `plan`, a federated sub-plan, with each of its projections listing only
/// the columns the nodes above it read (`needed` of the root; at least one,
/// since `SELECT` without a column has no portable SQL), the inputs of a
/// union still lined up. Only columns leave a list: the plan keeps its
/// shape, which the unparser renders as it did.
fn narrow(plan: LogicalPlan, needed: Needed) -> Result<Transformed<LogicalPlan>> {
    if let (LogicalPlan::Projection(projection), Some(needed)) = (&plan, &needed)
        && !over_distinct(&projection.input)
    {
        let mut kept: Vec<Expr> = projection
            .expr
            .iter()
            .zip(plan.schema().columns())
            .filter(|(_, column)| needed.contains(column))
            .map(|(expr, _)| expr.clone())
            .collect();
        if kept.is_empty() {
            kept.extend(projection.expr.first().cloned());
        }
        let input = narrow(
            Arc::unwrap_or_clone(Arc::clone(&projection.input)),
            Some(columns_of(&kept)),
        )?;
        if kept.len() == projection.expr.len() && !input.transformed {
            return Ok(Transformed::no(plan));
        }
        let narrowed = LogicalPlan::Projection(Projection::try_new(kept, Arc::new(input.data))?);
        return Ok(Transformed::yes(narrowed));
    }
    if let (LogicalPlan::TableScan(scan), Some(needed)) = (&plan, &needed)
        && let Some(listed) = &scan.projection
    {
        return narrow_scan(scan, listed, needed);
    }
    let needs = needs_of_inputs(&plan, needed);
    let inputs = plan
        .inputs()
        .into_iter()
        .cloned()
        .zip(needs.iter().cloned())
        .map(|(input, need)| narrow(input, need))
        .collect::<Result<Vec<_>>>()?;
    if !inputs.iter().any(|input| input.transformed) {
        return Ok(Transformed::no(plan));
    }
    let inputs: Vec<LogicalPlan> = inputs.into_iter().map(|input| input.data).collect();
    if !lined_up(&plan, &needs, &inputs) {
        return Ok(Transformed::no(plan));
    }
    with_new_inputs(&plan, inputs).map(Transformed::yes)
}

/// A scan that lists its columns (`listed`, the SQL Server policy lists
/// them all) listing only those in `needed` and those its own filters read,
/// at least one: the unparser applies the filters over the list
/// (`SELECT … FROM (SELECT <list> FROM t) WHERE <filters>`).
fn narrow_scan(
    scan: &TableScan,
    listed: &[usize],
    needed: &HashSet<Column>,
) -> Result<Transformed<LogicalPlan>> {
    let filtered: HashSet<String> = scan
        .filters
        .iter()
        .flat_map(|filter| {
            filter
                .column_refs()
                .into_iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        })
        .collect();
    let mut kept: Vec<usize> = listed
        .iter()
        .zip(scan.projected_schema.columns())
        .filter(|(_, column)| needed.contains(column) || filtered.contains(&column.name))
        .map(|(index, _)| *index)
        .collect();
    if kept.is_empty() {
        kept.extend(listed.first());
    }
    if kept.len() == listed.len() {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan.clone())));
    }
    let narrowed = TableScan::try_new(
        scan.table_name.clone(),
        Arc::clone(&scan.source),
        Some(kept),
        scan.filters.clone(),
        scan.fetch,
    )?;
    Ok(Transformed::yes(LogicalPlan::TableScan(narrowed)))
}

/// A `DISTINCT` the unparser writes into the same `SELECT` as a projection
/// above it: the projection's list is what it compares, so it cannot lose
/// a column.
fn over_distinct(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Distinct(_) => true,
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => {
            plan.inputs().into_iter().any(over_distinct)
        }
        _ => false,
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

    /// `CaseInsensitiveSource` that also returns the first rows of a sort
    /// by plain columns.
    #[derive(Debug)]
    struct FirstRowsSource;

    impl PushdownPolicy for FirstRowsSource {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            CaseInsensitiveSource.node_is_pushable(plan)
        }

        fn superset(&self, term: &Expr, schema: &DFSchema) -> Option<Expr> {
            CaseInsensitiveSource.superset(term, schema)
        }

        fn first_rows(&self, sort: &Sort) -> bool {
            sort.expr.iter().all(|key| matches!(key.expr, Expr::Column(_)))
        }
    }

    /// `ORDER BY … LIMIT n` asks the source for its first `n` rows only
    /// when everything below reaches it exactly; the engine sorts them
    /// again (the fake source ignores its SQL and returns every row).
    #[tokio::test]
    async fn the_source_returns_the_first_rows_of_an_exact_sub_plan() {
        for (sql, first, expected) in [
            (
                "SELECT customer_id FROM contracts ORDER BY customer_id DESC LIMIT 2",
                true,
                vec![3, 2],
            ),
            (
                "SELECT customer_id FROM contracts WHERE customer_id >= 2 ORDER BY customer_id DESC LIMIT 1",
                true,
                vec![3],
            ),
            // The engine drops rows of a superset: the source's first two
            // could all be dropped.
            (
                "SELECT customer_id FROM contracts WHERE status = 'active' ORDER BY customer_id DESC LIMIT 2",
                false,
                vec![2, 1],
            ),
            // The policy refuses the key, or something below the sort stays local.
            (
                "SELECT customer_id FROM contracts ORDER BY customer_id + 1 DESC LIMIT 2",
                false,
                vec![3, 2],
            ),
            (
                "SELECT max(customer_id) AS m FROM contracts GROUP BY status ORDER BY m DESC LIMIT 1",
                false,
                vec![3],
            ),
            (
                "SELECT customer_id FROM contracts ORDER BY customer_id DESC",
                false,
                vec![3, 2, 1],
            ),
            (
                "SELECT customer_id FROM contracts WHERE customer_id >= 2 ORDER BY status LIMIT 1",
                true,
                vec![3],
            ),
            // The source's first `offset + limit`, skipped in the engine.
            (
                "SELECT customer_id FROM contracts ORDER BY customer_id DESC LIMIT 1 OFFSET 1",
                true,
                vec![2],
            ),
        ] {
            let ctx = crate::testing::federated_session();
            let (table, sent) = remote_table_recording(
                "crm",
                "contracts",
                vec![
                    ("customer_id", int64(&[1, 2, 3])),
                    ("status", strings(&["active", "active", "ACTIVE"])),
                ],
                Some(Arc::new(FirstRowsSource)),
            );
            ctx.register_table("contracts", table).unwrap();
            let batches = rows(&ctx, sql).await.unwrap();
            let ids: Vec<i64> = batches
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
            assert_eq!(ids, expected, "{sql}");
            let sent = sent.lock().unwrap().join("\n");
            assert_eq!(sent.contains("LIMIT"), first, "{sql}: {sent}");
            assert_eq!(sent.contains("ORDER BY"), first, "{sql}: {sent}");
        }
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

    #[tokio::test]
    async fn each_side_of_a_join_reads_only_the_columns_it_uses() {
        let ctx = crate::testing::federated_session();
        let (contracts, sent_contracts) = remote_table_recording(
            "crm",
            "contracts",
            vec![
                ("customer_id", int64(&[1, 2, 3])),
                ("amount", int64(&[300, 100, 500])),
                ("notes", strings(&["x", "y", "z"])),
            ],
            Some(Arc::new(NumericFilters)),
        );
        let (customers, sent_customers) = remote_table_recording(
            "erp",
            "customers",
            vec![
                ("id", int64(&[1, 2, 3])),
                ("name", strings(&["a", "b", "c"])),
                ("city", strings(&["p", "q", "r"])),
            ],
            Some(Arc::new(NumericFilters)),
        );
        ctx.register_table("contracts", contracts).unwrap();
        ctx.register_table("customers", customers).unwrap();
        let batches = rows(
            &ctx,
            "SELECT c.name, k.amount FROM customers c JOIN contracts k ON k.customer_id = c.id ORDER BY k.amount",
        )
        .await
        .unwrap();
        let contracts = sent_contracts.lock().unwrap().join("\n");
        let customers = sent_customers.lock().unwrap().join("\n");
        assert!(!contracts.contains("notes"), "unused column sent: {contracts}");
        assert!(
            contracts.contains("customer_id") && contracts.contains("amount"),
            "{contracts}"
        );
        assert!(!customers.contains("city"), "unused column sent: {customers}");
        assert!(
            customers.contains("name") && customers.contains("id"),
            "{customers}"
        );
        let amounts = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amounts.values(), &[100, 300, 500]);
    }

    /// Five remote tables of one source, more than the optimizer has passes,
    /// and the local keys that pick their rows. The policy sends no filter,
    /// so the fake source's rows are the query's.
    fn replicas() -> (SessionContext, Vec<Arc<std::sync::Mutex<Vec<String>>>>) {
        let ctx = crate::testing::federated_session();
        let mut sent = Vec::new();
        for name in ["t1", "t2", "t3", "t4", "t5"] {
            let (table, statements) = remote_table_recording(
                "crm",
                name,
                vec![
                    ("k", int64(&[1, 1, 2, 3])),
                    ("seq", int64(&[1, 2, 1, 1])),
                    ("deleted", int64(&[0, 0, 0, 1])),
                    ("v", int64(&[10, 11, 20, 30])),
                    ("notes", strings(&["a", "b", "c", "d"])),
                ],
                Some(Arc::new(ScansAndProjectionsOnly)),
            );
            ctx.register_table(name, table).unwrap();
            sent.push(statements);
        }
        let keys = RecordBatch::try_from_iter([("k", int64(&[1, 3]))]).unwrap();
        ctx.register_batch("keys", keys).unwrap();
        (ctx, sent)
    }

    /// Every statement the source received, sorted.
    fn statements(sent: &[Arc<std::sync::Mutex<Vec<String>>>]) -> Vec<String> {
        let mut all: Vec<String> = sent.iter().flat_map(|s| s.lock().unwrap().clone()).collect();
        all.sort_unstable();
        all
    }

    fn pairs(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
        let column = |batch: &RecordBatch, i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        let mut pairs: Vec<(i64, i64)> = batches
            .iter()
            .flat_map(|b| column(b, 0).into_iter().zip(column(b, 1)))
            .collect();
        pairs.sort_unstable();
        pairs
    }

    /// The newest row of each key the local table lists, as a reconciliation
    /// writes it: every column and a row number, filtered outside.
    fn newest(table: &str) -> String {
        format!(
            "SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY seq DESC) AS rn \
             FROM {table} WHERE lower(CAST(k AS varchar)) IN (SELECT DISTINCT lower(CAST(k AS varchar)) FROM keys)) z \
             WHERE rn = 1 AND deleted = 0"
        )
    }

    #[tokio::test]
    async fn every_scan_of_a_union_reads_only_the_columns_it_uses() {
        let (ctx, sent) = replicas();
        let sql = ["t1", "t2", "t3", "t4", "t5"].map(newest).join(" UNION ALL ");
        let batches = rows(&ctx, &sql).await.unwrap();
        // A window over `SELECT *` reads its keys and what the query returns.
        assert_eq!(
            statements(&sent),
            ["t1", "t2", "t3", "t4", "t5"]
                .map(|t| format!("SELECT {t}.k, {t}.seq, {t}.deleted, {t}.v FROM {t}"))
        );
        assert_eq!(pairs(&batches), [(1, 11); 5]);
    }

    #[tokio::test]
    async fn every_scan_under_joins_reads_only_the_columns_it_uses() {
        let (ctx, sent) = replicas();
        // Outer joins, an anti join and a semi join, the tables aliased: the
        // projection goes below each alias.
        let batches = rows(
            &ctx,
            "SELECT a.k, b.v FROM t1 a \
             FULL JOIN t2 b ON b.k = a.k AND b.seq = a.seq \
             LEFT JOIN t3 c ON c.k = a.k AND c.seq = a.seq \
             WHERE a.deleted = 0 AND c.k IS NOT NULL \
               AND a.k NOT IN (SELECT d.k FROM t4 d WHERE d.v = 20) \
               AND EXISTS (SELECT 1 FROM t5 e WHERE e.k = a.k AND e.v > 10)",
        )
        .await
        .unwrap();
        assert_eq!(
            statements(&sent),
            [
                "SELECT * FROM (SELECT t1.k, t1.seq, t1.deleted FROM t1) AS a",
                "SELECT * FROM (SELECT t2.k, t2.seq, t2.v FROM t2) AS b",
                "SELECT * FROM (SELECT t3.k, t3.seq FROM t3) AS c",
                "SELECT t4.k, t4.v FROM t4",
                "SELECT t5.k, t5.v FROM t5",
            ]
        );
        assert_eq!(pairs(&batches), [(1, 10), (1, 11)]);
    }

    /// The scan of a remote table `name(a, b, c)`.
    fn remote_scan(name: &str) -> LogicalPlanBuilder {
        let fields = ["a", "b", "c"].map(|column| {
            datafusion::arrow::datatypes::Field::new(
                column,
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            )
        });
        let schema = datafusion::arrow::datatypes::Schema::new(fields.to_vec());
        datafusion::logical_expr::table_scan(Some(name), &schema, None).unwrap()
    }

    /// `SELECT <column> FROM (<t> UNION ALL <u>)`, each input built by
    /// `input` over the federated sub-plan `remote` returns for its table.
    fn union_reading(
        column: &str,
        remote: impl Fn(&str) -> LogicalPlanBuilder,
        input: impl Fn(&str, LogicalPlan) -> LogicalPlanBuilder,
    ) -> LogicalPlan {
        let planner: Arc<dyn FederationPlanner> =
            Arc::new(SQLFederationPlanner::new(crate::testing::executor("crm")));
        let side = |name: &str| {
            let federated = federate(remote(name).build().unwrap(), &planner);
            input(name, federated).build().unwrap()
        };
        LogicalPlanBuilder::from(side("t"))
            .union(side("u"))
            .unwrap()
            .project(vec![col(column)])
            .unwrap()
            .build()
            .unwrap()
    }

    /// The columns each federated sub-plan of `plan` returns.
    fn federated_columns(plan: &LogicalPlan) -> Vec<Vec<String>> {
        let mut found = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(Extension { node }) = node
                && let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>()
            {
                let columns = federated.plan().schema().columns();
                found.push(columns.iter().map(Column::flat_name).collect());
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .unwrap();
        found
    }

    /// A join sent whole (D3 of the shadow run): its select list and the
    /// lists of the derived tables below it name only what the engine
    /// reads above it, the join's keys included.
    fn federated_join(left: LogicalPlan) -> LogicalPlan {
        let planner: Arc<dyn FederationPlanner> =
            Arc::new(SQLFederationPlanner::new(crate::testing::executor("crm")));
        let joined = LogicalPlanBuilder::from(left)
            .join(
                remote_scan("u").build().unwrap(),
                datafusion::logical_expr::JoinType::Left,
                (
                    vec![Column::from_qualified_name("c.a")],
                    vec![Column::from_qualified_name("u.a")],
                ),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        federate(crate::federated_joins::for_unparser(joined).unwrap(), &planner)
    }

    fn derived_projection(plan: &LogicalPlan) -> Vec<String> {
        let mut found = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::Extension(Extension { node }) = node
                && let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>()
            {
                federated.plan().apply(|inner| {
                    if let LogicalPlan::SubqueryAlias(alias) = inner
                        && let LogicalPlan::Projection(projection) = alias.input.as_ref()
                    {
                        found = projection.expr.iter().map(ToString::to_string).collect();
                    }
                    Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                })?;
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })
        .unwrap();
        found
    }

    #[test]
    fn a_join_sent_whole_lists_only_the_columns_read_above_it() {
        let left = remote_scan("t")
            .project(vec![col("t.a"), col("t.b"), col("t.c")])
            .unwrap()
            .alias("c")
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(federated_join(left.clone()))
            .project(vec![col("c.b"), col("u.c")])
            .unwrap()
            .build()
            .unwrap();
        let pruned = prune(plan, None).unwrap();
        assert!(pruned.transformed);
        assert_eq!(federated_columns(&pruned.data), [["c.b", "u.c"]]);
        // The derived table keeps the join's key.
        assert_eq!(derived_projection(&pruned.data), ["t.a", "t.b"]);
        // Everything read: nothing to narrow.
        let all = LogicalPlanBuilder::from(federated_join(left.clone()))
            .project(["c.a", "c.b", "c.c", "u.a", "u.b", "u.c"].map(col).to_vec())
            .unwrap()
            .build()
            .unwrap();
        assert!(!prune(all, None).unwrap().transformed);
        // Nothing read (`count(*)`): one column, `SELECT` needs one.
        let counted = LogicalPlanBuilder::from(federated_join(left))
            .aggregate(
                Vec::<Expr>::new(),
                vec![datafusion::functions_aggregate::expr_fn::count(lit(1))],
            )
            .unwrap()
            .build()
            .unwrap();
        let pruned = prune(counted, None).unwrap();
        assert!(pruned.transformed);
        assert_eq!(federated_columns(&pruned.data), [["c.a"]]);
        assert_eq!(derived_projection(&pruned.data), ["t.a"]);
    }

    #[test]
    fn a_union_below_a_join_sent_whole_is_narrowed_alike_or_not_at_all() {
        let union = |left: LogicalPlanBuilder| {
            left.union(
                remote_scan("v")
                    .project(vec![col("v.a"), col("v.b"), col("v.c")])
                    .unwrap()
                    .build()
                    .unwrap(),
            )
            .unwrap()
            .alias("c")
            .unwrap()
            .build()
            .unwrap()
        };
        let lined = union(
            remote_scan("t")
                .project(vec![col("t.a"), col("t.b"), col("t.c")])
                .unwrap(),
        );
        let plan = LogicalPlanBuilder::from(federated_join(lined))
            .project(vec![col("c.b")])
            .unwrap()
            .build()
            .unwrap();
        let pruned = prune(plan, None).unwrap();
        assert_eq!(federated_columns(&pruned.data), [["c.b"]]);
        let mut lists = Vec::new();
        pruned
            .data
            .apply(|node| {
                if let LogicalPlan::Extension(Extension { node }) = node
                    && let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>()
                {
                    federated.plan().apply(|inner| {
                        if let LogicalPlan::Union(union) = inner {
                            lists = union.inputs.iter().map(|i| i.schema().fields().len()).collect();
                        }
                        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                    })?;
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert_eq!(lists, [2, 2], "the key and the column read, in both inputs");
        // A distinct union keeps every column it compares.
        let distinct = LogicalPlanBuilder::from(
            remote_scan("t")
                .project(vec![col("t.a"), col("t.b"), col("t.c")])
                .unwrap()
                .union(
                    remote_scan("v")
                        .project(vec![col("v.a"), col("v.b"), col("v.c")])
                        .unwrap()
                        .build()
                        .unwrap(),
                )
                .unwrap()
                .aggregate(vec![col("a"), col("b"), col("c")], Vec::<Expr>::new())
                .unwrap()
                .alias("c")
                .unwrap()
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
        let plan = LogicalPlanBuilder::from(federated_join(distinct))
            .project(vec![col("c.b")])
            .unwrap()
            .build()
            .unwrap();
        let pruned = prune(plan, None).unwrap();
        assert_eq!(federated_columns(&pruned.data), [["c.b"]]);
        assert_eq!(derived_projection(&pruned.data), Vec::<String>::new());
    }

    /// A scan that lists its columns (as the SQL Server policy makes every
    /// scan do) lists only those read above it, at least one.
    #[test]
    fn a_listed_scan_lists_only_the_columns_read() {
        let planner: Arc<dyn FederationPlanner> =
            Arc::new(SQLFederationPlanner::new(crate::testing::executor("crm")));
        let fields = ["a", "b", "c"].map(|column| {
            datafusion::arrow::datatypes::Field::new(
                column,
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            )
        });
        let schema = datafusion::arrow::datatypes::Schema::new(fields.to_vec());
        let listed =
            || datafusion::logical_expr::table_scan(Some("t"), &schema, Some(vec![0, 1, 2])).unwrap();
        let scan_list = |plan: &LogicalPlan| {
            let mut found = None;
            plan.apply(|node| {
                if let LogicalPlan::Extension(Extension { node }) = node
                    && let Some(federated) = node.as_any().downcast_ref::<FederatedPlanNode>()
                {
                    federated.plan().apply(|inner| {
                        if let LogicalPlan::TableScan(scan) = inner {
                            found = scan.projection.clone();
                        }
                        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
                    })?;
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })
            .unwrap();
            found
        };
        // Aliased, filtered: the filter's column stays, below the alias.
        let aliased = listed()
            .filter(col("t.c").gt(lit(1_i64)))
            .unwrap()
            .alias("r")
            .unwrap()
            .build()
            .unwrap();
        let plan = LogicalPlanBuilder::from(federate(aliased, &planner))
            .project(vec![col("r.b")])
            .unwrap()
            .build()
            .unwrap();
        let pruned = prune(plan, None).unwrap();
        assert_eq!(federated_columns(&pruned.data), [["r.b"]]);
        assert_eq!(scan_list(&pruned.data), Some(vec![1, 2]));
        // A filter of the scan itself keeps its column in the list.
        let LogicalPlan::TableScan(mut filtered) = listed().build().unwrap() else {
            panic!("a scan")
        };
        filtered.filters = vec![col("t.c").gt(lit(1_i64))];
        let needs_b: HashSet<Column> = [Column::new(Some("t"), "b")].into();
        let narrowed = narrow(LogicalPlan::TableScan(filtered), Some(needs_b)).unwrap();
        assert!(narrowed.transformed);
        let LogicalPlan::TableScan(narrowed) = narrowed.data else {
            panic!("a scan")
        };
        assert_eq!(narrowed.projection, Some(vec![1, 2]));
        // Nothing read: the first column.
        let narrowed = narrow(listed().build().unwrap(), Some(HashSet::new())).unwrap();
        assert!(narrowed.transformed);
        assert_eq!(narrowed.data.schema().fields().len(), 1);
        // Everything read: unchanged.
        let all: HashSet<Column> = ["a", "b", "c"].map(|c| Column::new(Some("t"), c)).into();
        assert!(!narrow(listed().build().unwrap(), Some(all)).unwrap().transformed);
        // A scan without a list keeps none.
        let unlisted = datafusion::logical_expr::table_scan(Some("t"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        assert!(!narrow(unlisted, Some(HashSet::new())).unwrap().transformed);
    }

    /// A projection over `DISTINCT` renders as one `SELECT DISTINCT`: it
    /// keeps every column it lists.
    #[test]
    fn a_projection_over_distinct_keeps_its_columns() {
        let planner: Arc<dyn FederationPlanner> =
            Arc::new(SQLFederationPlanner::new(crate::testing::executor("crm")));
        let distinct = LogicalPlan::Distinct(datafusion::logical_expr::Distinct::All(Arc::new(
            remote_scan("t").build().unwrap(),
        )));
        for input in [
            distinct.clone(),
            LogicalPlanBuilder::from(distinct)
                .filter(col("t.a").gt(lit(1_i64)))
                .unwrap()
                .build()
                .unwrap(),
        ] {
            let listed = LogicalPlanBuilder::from(input)
                .project(vec![col("t.a"), col("t.b"), col("t.c")])
                .unwrap()
                .build()
                .unwrap();
            let plan = LogicalPlanBuilder::from(federate(listed, &planner))
                .project(vec![col("t.a")])
                .unwrap()
                .build()
                .unwrap();
            assert!(!prune(plan, None).unwrap().transformed);
        }
        assert!(!over_distinct(&remote_scan("t").build().unwrap()));
    }

    #[test]
    fn the_inputs_of_a_union_are_narrowed_alike() {
        let union = union_reading("b", remote_scan, |_, scan| LogicalPlanBuilder::from(scan));
        let pruned = prune(union, None).unwrap();
        assert!(pruned.transformed);
        assert_eq!(federated_columns(&pruned.data), [["t.b"], ["u.b"]]);
    }

    #[test]
    fn inputs_of_a_union_that_return_what_they_did_are_narrowed_below() {
        // Each input names two columns, the union's reader one of them.
        let union = union_reading("a", remote_scan, |name, scan| {
            let (a, b) = (format!("{name}.a"), format!("{name}.b"));
            LogicalPlanBuilder::from(scan)
                .project(vec![col(a), col(b)])
                .unwrap()
        });
        let pruned = prune(union, None).unwrap();
        assert!(pruned.transformed);
        assert_eq!(federated_columns(&pruned.data), [["t.a", "t.b"], ["u.a", "u.b"]]);
    }

    #[test]
    fn a_union_with_an_input_that_is_not_narrowed_keeps_its_inputs() {
        // The second sub-plan stops early: not a scan the rule narrows.
        let union = union_reading(
            "b",
            |name| match name {
                "u" => remote_scan(name).limit(0, Some(1)).unwrap(),
                _ => remote_scan(name),
            },
            |_, scan| LogicalPlanBuilder::from(scan),
        );
        let pruned = prune(union.clone(), None).unwrap();
        assert!(!pruned.transformed, "{}", pruned.data.display_indent());
        assert_eq!(pruned.data, union);
    }

    #[test]
    fn a_union_whose_inputs_keep_other_columns_of_their_own_keeps_its_inputs() {
        // Each filter runs in the engine and reads another column: narrowed,
        // the inputs would return (a, b) and (b, c).
        let union = union_reading("b", remote_scan, |name, scan| {
            let filtered = if name == "t" { "t.a" } else { "u.c" };
            LogicalPlanBuilder::from(scan)
                .filter(col(filtered).gt(lit(1_i64)))
                .unwrap()
        });
        let pruned = prune(union.clone(), None).unwrap();
        assert!(!pruned.transformed, "{}", pruned.data.display_indent());
        assert_eq!(pruned.data, union);
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

    /// Pushes aggregations but not the projections above them.
    #[derive(Debug)]
    struct AggregatesWithoutProjections;

    impl PushdownPolicy for AggregatesWithoutProjections {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            matches!(
                plan,
                LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) | LogicalPlan::Aggregate(_)
            )
        }
    }

    #[tokio::test]
    async fn an_aggregation_sent_alone_returns_its_groups_first() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![("customer_id", int64(&[1, 2, 1]))],
            Some(Arc::new(AggregatesWithoutProjections)),
        );
        ctx.register_table("contracts", table).unwrap();
        // The fake source cannot count: only the statement is asserted. The
        // engine reads its columns by position, groups first.
        let _ = rows(
            &ctx,
            "SELECT customer_id, count(*) AS n FROM contracts GROUP BY customer_id",
        )
        .await;
        let sent = sent.lock().unwrap().join("\n");
        let list = sent.split(" FROM ").next().unwrap_or_default();
        let group = list.find("customer_id");
        let count = list.find("count(");
        assert!(group.is_some() && count.is_some() && group < count, "{sent}");
    }

    /// Pushes aggregations, projections, filters and scans.
    #[derive(Debug)]
    struct Aggregates;

    impl PushdownPolicy for Aggregates {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            matches!(
                plan,
                LogicalPlan::TableScan(_)
                    | LogicalPlan::SubqueryAlias(_)
                    | LogicalPlan::Aggregate(_)
                    | LogicalPlan::Projection(_)
                    | LogicalPlan::Filter(_)
            )
        }
    }

    #[tokio::test]
    async fn an_aggregation_over_computed_derived_columns_runs_in_the_engine() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![("customer_id", int64(&[1, 2, 1]))],
            Some(Arc::new(Aggregates)),
        );
        ctx.register_table("contracts", table).unwrap();
        let _ = rows(
            &ctx,
            "SELECT count(DISTINCT customer_id), sum(n) FROM \
             (SELECT c.customer_id, count(*) AS n FROM contracts c GROUP BY c.customer_id)",
        )
        .await;
        let sent = sent.lock().unwrap().join("\n");
        assert!(sent.contains("GROUP BY"), "the inner aggregation is sent: {sent}");
        assert!(!sent.contains("DISTINCT"), "the outer one stays local: {sent}");
    }

    #[tokio::test]
    async fn a_filter_of_an_aliased_table_is_sent_against_the_alias() {
        let ctx = crate::testing::federated_session();
        let (table, sent) = remote_table_recording(
            "crm",
            "contracts",
            vec![("customer_id", int64(&[1, 2, 3])), ("amount", int64(&[5, 6, 7]))],
            Some(Arc::new(NumericFilters)),
        );
        ctx.register_table("contracts", table).unwrap();
        let _ = rows(&ctx, "SELECT c.customer_id FROM contracts c WHERE c.amount >= 6")
            .await
            .unwrap();
        let sent = sent.lock().unwrap().join("\n");
        assert!(sent.contains("c.amount >= 6"), "{sent}");
        assert!(!sent.contains("contracts.amount"), "the table is aliased: {sent}");
    }

    #[tokio::test]
    async fn an_unnest_beside_a_federated_table_is_rebuilt() {
        let ctx = contracts(Some(Arc::new(ScansAndProjectionsOnly)));
        let batches = rows(
            &ctx,
            "SELECT count(*) FROM unnest([1, 2, 3]) AS t(x) CROSS JOIN contracts",
        )
        .await
        .unwrap();
        let counts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[12]);
    }

    fn table(name: &str) -> LogicalPlanBuilder {
        let schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "a",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
            datafusion::arrow::datatypes::Field::new(
                "b",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]);
        datafusion::logical_expr::table_scan(Some(name), &schema, None).unwrap()
    }

    #[test]
    fn only_an_aggregation_over_computed_qualified_columns_is_kept_local() {
        let count = || datafusion::functions_aggregate::count::count(lit(1_i64));
        let over = |projection: Vec<Expr>| {
            table("t")
                .project(projection)
                .unwrap()
                .aggregate(Vec::<Expr>::new(), vec![count()])
                .unwrap()
                .build()
                .unwrap()
        };
        assert!(aggregate_over_derived_columns(&over(vec![
            col("t.a"),
            (col("t.b") + lit(1_i64)).alias("c"),
        ])));
        // Plain columns: the policy's `prepare` can drop the projection.
        assert!(!aggregate_over_derived_columns(&over(vec![
            col("t.a"),
            col("t.b")
        ])));
        // Only new names: the aggregation names them without a table.
        assert!(!aggregate_over_derived_columns(&over(vec![
            (col("t.b") + lit(1_i64)).alias("c")
        ])));
        let direct = table("t")
            .aggregate(Vec::<Expr>::new(), vec![count()])
            .unwrap()
            .build()
            .unwrap();
        assert!(!aggregate_over_derived_columns(&direct));
        let projected = table("t")
            .project(vec![col("t.a"), (col("t.b") + lit(1_i64)).alias("c")])
            .unwrap()
            .project(vec![col("t.a")])
            .unwrap()
            .build()
            .unwrap();
        assert!(!aggregate_over_derived_columns(&projected));
    }

    #[test]
    fn filters_between_an_alias_and_its_scan_become_scan_filters() {
        let aliased = table("t")
            .filter(col("t.a").gt(lit(1_i64)))
            .unwrap()
            .filter(col("t.b").lt(lit(9_i64)))
            .unwrap()
            .alias("r")
            .unwrap()
            .build()
            .unwrap();
        let folded = aliased_scan_filters(aliased.clone()).unwrap();
        let LogicalPlan::SubqueryAlias(alias) = &folded else {
            panic!("{folded}");
        };
        let LogicalPlan::TableScan(scan) = alias.input.as_ref() else {
            panic!("{folded}");
        };
        assert_eq!(scan.filters.len(), 2, "{folded}");
        assert_eq!(folded.schema(), aliased.schema());

        // A scan that stops early would filter only the rows it returned.
        let mut limited = table("t").build().unwrap();
        if let LogicalPlan::TableScan(scan) = &mut limited {
            scan.fetch = Some(1);
        }
        let limited = LogicalPlanBuilder::from(limited)
            .filter(col("t.a").gt(lit(1_i64)))
            .unwrap()
            .alias("r")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(aliased_scan_filters(limited.clone()).unwrap(), limited);

        for untouched in [
            table("t").alias("r").unwrap().build().unwrap(),
            table("t")
                .filter(col("t.a").gt(lit(1_i64)))
                .unwrap()
                .build()
                .unwrap(),
            table("t")
                .project(vec![col("t.a")])
                .unwrap()
                .filter(col("t.a").gt(lit(1_i64)))
                .unwrap()
                .alias("r")
                .unwrap()
                .build()
                .unwrap(),
        ] {
            assert_eq!(aliased_scan_filters(untouched.clone()).unwrap(), untouched);
        }
    }

    #[test]
    fn only_an_aggregation_gets_a_select_list() {
        let aggregate = table("t")
            .aggregate(
                vec![col("t.a")],
                vec![datafusion::functions_aggregate::count::count(lit(1_i64))],
            )
            .unwrap()
            .build()
            .unwrap();
        let listed = with_select_list(aggregate.clone()).unwrap();
        let LogicalPlan::Projection(projection) = &listed else {
            panic!("{listed}");
        };
        assert_eq!(projection.input.as_ref(), &aggregate);
        assert_eq!(listed.schema(), aggregate.schema());
        let scan = table("t").build().unwrap();
        assert_eq!(with_select_list(scan.clone()).unwrap(), scan);
    }

    #[test]
    fn an_unnest_takes_new_inputs_without_its_expressions() {
        let unnest = LogicalPlanBuilder::empty(true)
            .project(vec![
                datafusion::functions_nested::expr_fn::make_array(vec![lit(1_i64)]).alias("xs"),
            ])
            .unwrap()
            .unnest_column("xs")
            .unwrap()
            .build()
            .unwrap();
        let inputs = unnest.inputs().into_iter().cloned().collect();
        assert_eq!(with_new_inputs(&unnest, inputs).unwrap(), unnest);
        let projection = table("t").project(vec![col("t.a")]).unwrap().build().unwrap();
        let inputs = projection.inputs().into_iter().cloned().collect();
        assert_eq!(with_new_inputs(&projection, inputs).unwrap(), projection);
    }

    #[test]
    fn non_federated_plans_are_untouched() {
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let out = restrict_federation(plan.clone(), &ScansAndProjectionsOnly).unwrap();
        assert_eq!(out, plan);
    }
}
