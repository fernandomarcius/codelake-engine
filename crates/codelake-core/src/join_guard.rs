//! Correctness guards for joins `DataFusion` 54.1 answers wrongly (ADR 0014).
//!
//! Each guard keeps the engine off a path that returns wrong rows; none of
//! them changes what a correct plan answers.
//!
//! 1. **Null-aware anti joins keep every row of the subquery.**
//!    `x NOT IN (SELECT y …)` is a null-aware `LeftAnti` hash join: one NULL
//!    among the `y` makes every `x` UNKNOWN, so the query returns nothing.
//!    `DataFusion` 54 pushes the join's dynamic filter (`IN`/bounds built from
//!    the `x` values) into the scan of the subquery, which then drops the NULL
//!    before the join sees it (apache/datafusion#23104, fixed after 54.1.0);
//!    the physical filter pushdown also sends a parent filter on `x` to the
//!    subquery side, with the same effect. [`NullAwareAntiJoinGuard`] puts a
//!    [`NullAwareProbeBarrier`] above the subquery side of every null-aware
//!    hash join: no filter reaches it from above. The logical
//!    `push_down_filter` does the same with a filter over the join: from
//!    `x > 5 AND x NOT IN (SELECT y …)` it infers `y > 5` for the subquery,
//!    which drops its NULL.
//!    [`NullAwareFilterPushdown`] runs that rule with the keys of null-aware
//!    joins moved into their join filter, where nothing is inferred from them,
//!    and moves them back.
//! 2. **Null-aware anti joins are hash joins.** With
//!    `datafusion.optimizer.prefer_hash_join = false` the physical planner
//!    turns a null-aware anti join into a `SortMergeJoinExec`, which ignores
//!    NULLs (apache/datafusion#22810). [`JoinQueryPlanner`] plans a query that
//!    holds one with hash joins preferred.
//! 3. **`NOT IN` compared with a constant or over a correlated subquery
//!    keeps its NULL semantics.** `DataFusion` handles NULLs only in a
//!    null-aware hash join on the one key `x = y`. `3 NOT IN (SELECT y …)`
//!    has no key and becomes a nested loop join; a correlated subquery adds
//!    its correlation keys, and the join then checks the correlation keys for
//!    NULLs instead of `y` (both wrong in `DataFusion` 53 and 54).
//!    [`NullSafeNotIn`] rewrites those conditions, before decorrelation, into
//!    `NOT EXISTS (SELECT … WHERE <correlation> AND (x = y OR x IS NULL OR
//!    y IS NULL))`, which is `x NOT IN (…)` wherever UNKNOWN filters a row
//!    out (a conjunct of a `WHERE`).
//! 4. **Nested loop joins do not fall back to spilling.** Over the memory
//!    limit, `DataFusion` 54's `NestedLoopJoinExec` spills its build side and
//!    runs in chunks. The fallback emits a `LEFT`/`LEFT ANTI` join's unmatched
//!    rows once per probe partition (apache/datafusion#24675), drops the
//!    unmatched probe rows of a `RIGHT`/`RIGHT ANTI` join
//!    (apache/datafusion#24746) and executes the build side a second time
//!    (apache/datafusion#24677): a file scan hands the second execution only
//!    the files the first one left unopened, an exchange panics. The fallback
//!    only starts when the runtime can write temporary files;
//!    [`NestedLoopJoinWithoutFallback`] runs the join with a runtime that
//!    cannot and gives its inputs the engine's runtime back
//!    ([`JoinInputRuntime`]). Over the limit the join fails with a resources
//!    error, as in `DataFusion` 53.
//! 5. **`IN (subquery)` is refused where a mark join would lose its
//!    UNKNOWN.** Outside a conjunct of a `WHERE` or `HAVING`, `DataFusion`
//!    answers `x [NOT] IN (subquery)` with a mark join, whose mark is FALSE
//!    where SQL says UNKNOWN (no match, and `x` or a value of the subquery
//!    is NULL): `x NOT IN (SELECT 1 UNION ALL SELECT NULL) OR x = 99` kept
//!    every `x` other than 1, `CASE WHEN x NOT IN (…) THEN …` took the wrong
//!    branch (`DataFusion` 53 and 54). The mark is right wherever FALSE and
//!    UNKNOWN filter the same rows: inside a `WHERE`/`HAVING` under `AND`
//!    and `OR`, with an even number of negations above a positive `IN` (an
//!    odd number above a `NOT IN`). [`UnknownMembershipGuard`] refuses every
//!    other `IN (subquery)` whose value or subquery column may be NULL
//!    (`NOT_SUPPORTED`), before decorrelation; `NOT EXISTS` and a conjunct
//!    `NOT IN` keep answering.
//!
//! Remove a guard only when a `DataFusion` release carries the upstream fix
//! **and** the matching test of
//! `crates/codelake-connectors/tests/integration/join_regressions.rs` passes
//! without it.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Filter, Join, LogicalPlan, LogicalPlanBuilder, Operator, Subquery,
};
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Name of [`NullAwareAntiJoinGuard`].
pub const NULL_AWARE_ANTI_JOIN_GUARD: &str = "codelake_null_aware_anti_join_guard";

/// Name of [`NestedLoopJoinSpillGuard`].
pub const NESTED_LOOP_JOIN_SPILL_GUARD: &str = "codelake_nested_loop_join_spill_guard";

fn one_child(name: &str, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
    let [child] = <[_; 1]>::try_from(children).map_err(|children| {
        DataFusionError::Internal(format!("{name} expects one child, got {}", children.len()))
    })?;
    Ok(child)
}

// ---------------------------------------------------------------------------
// 1. Null-aware anti joins: no filter reaches the subquery side.
// ---------------------------------------------------------------------------

fn is_null_aware(plan: &dyn ExecutionPlan) -> bool {
    plan.downcast_ref::<HashJoinExec>()
        .is_some_and(|join| join.null_aware)
}

/// Physical rule, first in the list (before `FilterPushdown`): puts a
/// [`NullAwareProbeBarrier`] above the right input (the subquery) of every
/// null-aware hash join.
#[derive(Debug, Default)]
pub struct NullAwareAntiJoinGuard;

impl PhysicalOptimizerRule for NullAwareAntiJoinGuard {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            if !is_null_aware(node.as_ref()) {
                return Ok(Transformed::no(node));
            }
            let children = node.children();
            let [left, right] = children.as_slice() else {
                return Ok(Transformed::no(node));
            };
            if right.is::<NullAwareProbeBarrier>() {
                return Ok(Transformed::no(node));
            }
            let barrier: Arc<dyn ExecutionPlan> = Arc::new(NullAwareProbeBarrier::new(Arc::clone(right)));
            let children = vec![Arc::clone(left), barrier];
            Ok(Transformed::yes(Arc::clone(&node).with_new_children(children)?))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        NULL_AWARE_ANTI_JOIN_GUARD
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Transparent node whose only behaviour is the trait default for filter
/// pushdown: every filter from above is reported as unsupported, so neither a
/// join's dynamic filter nor a parent filter reaches its input.
#[derive(Debug)]
pub struct NullAwareProbeBarrier {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl NullAwareProbeBarrier {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for NullAwareProbeBarrier {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NullAwareProbeBarrier")
    }
}

impl ExecutionPlan for NullAwareProbeBarrier {
    fn name(&self) -> &'static str {
        "NullAwareProbeBarrier"
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
        Ok(Arc::new(Self::new(one_child(self.name(), children)?)))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

/// `push_down_filter` (`inner`) with nothing inferred from the keys of a
/// null-aware join (see the module documentation). Keeps the rule's name, so
/// the optimizer's rule list reads the same.
#[derive(Debug)]
pub struct NullAwareFilterPushdown {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
}

impl NullAwareFilterPushdown {
    pub fn new(inner: Arc<dyn OptimizerRule + Send + Sync>) -> Self {
        Self { inner }
    }
}

/// Runs `rule` over the whole plan the way the optimizer would.
fn run_rule(
    rule: &dyn OptimizerRule,
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    match rule.apply_order() {
        Some(ApplyOrder::TopDown) => plan.transform_down_with_subqueries(|node| rule.rewrite(node, config)),
        Some(ApplyOrder::BottomUp) => plan.transform_up_with_subqueries(|node| rule.rewrite(node, config)),
        None => rule.rewrite(plan, config),
    }
}

/// A null-aware join with its equi-keys moved into its filter.
fn hide_keys(node: LogicalPlan) -> Transformed<LogicalPlan> {
    match node {
        LogicalPlan::Join(join) if join.null_aware && !join.on.is_empty() => {
            let keys = join.on.iter().map(|(l, r)| l.clone().eq(r.clone()));
            let filter = conjunction(keys.chain(join.filter.clone()));
            Transformed::yes(LogicalPlan::Join(Join {
                on: Vec::new(),
                filter,
                ..join
            }))
        }
        other => Transformed::no(other),
    }
}

/// A null-aware join with its equi-keys taken back from its filter.
fn restore_keys(node: LogicalPlan, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
    match node {
        LogicalPlan::Join(ref join) if join.null_aware && join.on.is_empty() && join.filter.is_some() => {
            ExtractEquijoinPredicate::new().rewrite(node, config)
        }
        other => Ok(Transformed::no(other)),
    }
}

impl OptimizerRule for NullAwareFilterPushdown {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn rewrite(&self, plan: LogicalPlan, config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        if !has_null_aware_join(&plan) {
            return run_rule(self.inner.as_ref(), plan, config);
        }
        let hidden = plan
            .transform_up_with_subqueries(|node| Ok(hide_keys(node)))?
            .data;
        let pushed = run_rule(self.inner.as_ref(), hidden, config)?;
        let transformed = pushed.transformed;
        let restored = pushed
            .data
            .transform_up_with_subqueries(|node| restore_keys(node, config))?
            .data;
        Ok(Transformed::new_transformed(restored, transformed))
    }
}

/// `rules` with every `push_down_filter` wrapped in [`NullAwareFilterPushdown`].
pub fn guard_filter_pushdown(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    rules
        .into_iter()
        .map(|rule| {
            if rule.name() == PUSH_DOWN_FILTER {
                Arc::new(NullAwareFilterPushdown::new(rule)) as Arc<dyn OptimizerRule + Send + Sync>
            } else {
                rule
            }
        })
        .collect()
}

/// Name of `DataFusion`'s logical filter pushdown rule.
const PUSH_DOWN_FILTER: &str = "push_down_filter";

// ---------------------------------------------------------------------------
// 2. Null-aware anti joins are planned as hash joins.
// ---------------------------------------------------------------------------

/// A plan (or one of its subqueries) holds a null-aware join.
fn has_null_aware_join(plan: &LogicalPlan) -> bool {
    let mut found = false;
    // The closure never fails.
    plan.apply_with_subqueries(|node| {
        found = matches!(node, LogicalPlan::Join(join) if join.null_aware);
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .is_ok()
        && found
}

/// Query planner of the engine: `inner` (the federation planner), with hash
/// joins preferred for a plan that holds a null-aware join (see the module
/// documentation). Other plans keep the session's setting.
#[derive(Debug)]
pub struct JoinQueryPlanner {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl JoinQueryPlanner {
    pub fn new(inner: Arc<dyn QueryPlanner + Send + Sync>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl QueryPlanner for JoinQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if session_state.config().options().optimizer.prefer_hash_join || !has_null_aware_join(logical_plan) {
            return self.inner.create_physical_plan(logical_plan, session_state).await;
        }
        let mut state = session_state.clone();
        state.config_mut().options_mut().optimizer.prefer_hash_join = true;
        self.inner.create_physical_plan(logical_plan, &state).await
    }
}

// ---------------------------------------------------------------------------
// 3. NOT IN compared with a constant or over a correlated subquery.
// ---------------------------------------------------------------------------

/// Name of [`NullSafeNotIn`].
pub const NULL_SAFE_NOT_IN: &str = "codelake_null_safe_not_in";

/// Name of the rule [`NullSafeNotIn`] runs before.
const DECORRELATE_PREDICATE_SUBQUERY: &str = "decorrelate_predicate_subquery";

/// Logical rule, right before `decorrelate_predicate_subquery`: see the module
/// documentation.
#[derive(Debug, Default)]
pub struct NullSafeNotIn;

/// A `NOT IN` that `DataFusion` would not plan as a null-aware join on its
/// one key: compared with a constant, or over a correlated subquery.
fn needs_null_safe_rewrite(in_subquery: &InSubquery) -> bool {
    in_subquery.negated && (in_subquery.expr.column_refs().is_empty() || is_correlated(&in_subquery.subquery))
}

/// A subquery that refers to columns of the query around it.
fn is_correlated(subquery: &Subquery) -> bool {
    !subquery.outer_ref_columns.is_empty()
        || subquery
            .subquery
            .exists(|node| Ok(node.contains_outer_reference()))
            .unwrap_or(true)
}

/// `x NOT IN (subquery)` as `NOT EXISTS (subquery WHERE x = y OR x IS NULL OR
/// y IS NULL)`, with `x`'s columns as outer references (`outer` is the
/// schema they come from).
fn not_exists(in_subquery: InSubquery, outer: &DFSchema) -> Result<Expr> {
    let InSubquery { expr, subquery, .. } = in_subquery;
    let plan = subquery.subquery;
    let (qualifier, field) = plan.schema().qualified_field(0);
    let inner = Expr::Column(Column::from((qualifier, field)));
    let mut outer_refs = subquery.outer_ref_columns;
    let value = expr
        .transform(|e| match e {
            Expr::Column(column) => {
                let field = Arc::clone(outer.qualified_field_from_column(&column)?.1);
                let reference = Expr::OuterReferenceColumn(field, column);
                outer_refs.push(reference.clone());
                Ok(Transformed::yes(reference))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data;
    let matches = value
        .clone()
        .eq(inner.clone())
        .or(value.is_null())
        .or(inner.is_null());
    let filtered = LogicalPlanBuilder::from(Arc::unwrap_or_clone(plan))
        .filter(matches)?
        .build()?;
    Ok(Expr::Exists(Exists::new(
        Subquery {
            subquery: Arc::new(filtered),
            outer_ref_columns: outer_refs,
            spans: subquery.spans,
        },
        true,
    )))
}

impl OptimizerRule for NullSafeNotIn {
    fn name(&self) -> &str {
        NULL_SAFE_NOT_IN
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };
        let conjuncts = split_conjunction_owned(filter.predicate.clone());
        if !conjuncts
            .iter()
            .any(|c| matches!(c, Expr::InSubquery(in_subquery) if needs_null_safe_rewrite(in_subquery)))
        {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let outer = Arc::clone(filter.input.schema());
        let conjuncts = conjuncts
            .into_iter()
            .map(|conjunct| match conjunct {
                Expr::InSubquery(in_subquery) if needs_null_safe_rewrite(&in_subquery) => {
                    not_exists(in_subquery, &outer)
                }
                other => Ok(other),
            })
            .collect::<Result<Vec<_>>>()?;
        let predicate =
            conjunction(conjuncts).unwrap_or_else(|| Expr::Literal(ScalarValue::Boolean(Some(true)), None));
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            filter.input,
        )?)))
    }
}

/// `rules` with [`NullSafeNotIn`], then [`UnknownMembershipGuard`], right
/// before `decorrelate_predicate_subquery`.
pub fn with_null_safe_not_in(
    mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let position = rules
        .iter()
        .position(|r| r.name() == DECORRELATE_PREDICATE_SUBQUERY)
        .unwrap_or(0);
    rules.insert(position, Arc::new(UnknownMembershipGuard));
    rules.insert(position, Arc::new(NullSafeNotIn));
    rules
}

// ---------------------------------------------------------------------------
// 4. Nested loop joins run without the spill fallback.
// ---------------------------------------------------------------------------

/// The engine's runtime, carried in the session config of a nested loop
/// join's task so that its inputs get it back.
#[derive(Debug)]
struct EngineRuntime(Arc<RuntimeEnv>);

/// `context` with `runtime` and `config`.
fn task_context(
    context: &TaskContext,
    config: datafusion::execution::config::SessionConfig,
    runtime: Arc<RuntimeEnv>,
) -> Arc<TaskContext> {
    Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        config,
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        runtime,
    ))
}

/// Physical rule, after every rule that rebuilds joins: wraps each
/// `NestedLoopJoinExec` in [`NestedLoopJoinWithoutFallback`] and each of its
/// inputs in [`JoinInputRuntime`].
#[derive(Debug, Default)]
pub struct NestedLoopJoinSpillGuard;

impl PhysicalOptimizerRule for NestedLoopJoinSpillGuard {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            // A join guarded already (a view planned on its own) has guarded
            // inputs.
            if !node.is::<NestedLoopJoinExec>() || node.children().iter().all(|c| c.is::<JoinInputRuntime>())
            {
                return Ok(Transformed::no(node));
            }
            let inputs = node
                .children()
                .into_iter()
                .map(|child| Arc::new(JoinInputRuntime::new(Arc::clone(child))) as Arc<dyn ExecutionPlan>)
                .collect();
            let join = Arc::clone(&node).with_new_children(inputs)?;
            let guarded: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinWithoutFallback::new(join));
            Ok(Transformed::yes(guarded))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        NESTED_LOOP_JOIN_SPILL_GUARD
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Runs its input (a nested loop join) with a runtime that cannot write
/// temporary files, which keeps the join off its spill fallback. The memory
/// pool, caches and object stores are the engine's.
#[derive(Debug)]
pub struct NestedLoopJoinWithoutFallback {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl NestedLoopJoinWithoutFallback {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for NestedLoopJoinWithoutFallback {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NestedLoopJoinWithoutFallback")
    }
}

impl ExecutionPlan for NestedLoopJoinWithoutFallback {
    fn name(&self) -> &'static str {
        "NestedLoopJoinWithoutFallback"
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
        Ok(Arc::new(Self::new(one_child(self.name(), children)?)))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let runtime = context.runtime_env();
        let without_disk = RuntimeEnvBuilder::from_runtime_env(&runtime)
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled))
            .build_arc()?;
        let config = context
            .session_config()
            .clone()
            .with_extension(Arc::new(EngineRuntime(runtime)));
        self.input
            .execute(partition, task_context(&context, config, without_disk))
    }
}

/// Input of a guarded nested loop join: runs with the engine's runtime again,
/// so sorts and aggregations below the join still spill.
#[derive(Debug)]
pub struct JoinInputRuntime {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl JoinInputRuntime {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }
}

impl DisplayAs for JoinInputRuntime {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinInputRuntime")
    }
}

impl ExecutionPlan for JoinInputRuntime {
    fn name(&self) -> &'static str {
        "JoinInputRuntime"
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
        Ok(Arc::new(Self::new(one_child(self.name(), children)?)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        match context.session_config().get_extension::<EngineRuntime>() {
            Some(engine) => {
                let config = context.session_config().clone();
                let restored = task_context(&context, config, Arc::clone(&engine.0));
                self.input.execute(partition, restored)
            }
            None => self.input.execute(partition, context),
        }
    }
}

// ---------------------------------------------------------------------------
// 5. `IN (subquery)` whose UNKNOWN a mark join would answer as FALSE.
// ---------------------------------------------------------------------------

/// Name of [`UnknownMembershipGuard`].
pub const UNKNOWN_MEMBERSHIP_GUARD: &str = "codelake_unknown_membership_guard";

/// Logical rule, right before `decorrelate_predicate_subquery`: see the
/// module documentation. Changes nothing; fails the statement when an
/// `IN (subquery)` would be answered by a mark join where its UNKNOWN matters.
#[derive(Debug, Default)]
pub struct UnknownMembershipGuard;

/// Where an `IN (subquery)` sits in a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    /// A conjunct of a filter (`WHERE`, `HAVING`), possibly negated: planned
    /// as a semi join or a null-aware anti join, which keep UNKNOWN.
    Conjunct,
    /// Inside a filter under `AND`, `OR` and `NOT` only; `positive` when an
    /// even number of negations lies above it.
    Condition { positive: bool },
    /// Anywhere else: a projection, a `CASE`, a function argument, a
    /// comparison, `IS [NOT] …`, an aggregate.
    Value,
}

/// `x IN (subquery)` can be UNKNOWN: `x` or the subquery's column may be NULL.
fn may_be_unknown(in_subquery: &InSubquery, schema: &DFSchema) -> bool {
    in_subquery.expr.nullable(schema).unwrap_or(true)
        || in_subquery
            .subquery
            .subquery
            .schema()
            .fields()
            .first()
            .is_none_or(|field| field.is_nullable())
}

/// `x IN (subquery)` at `position` is answered as SQL does: as a semi or
/// anti join, or through a mark join (which answers FALSE for UNKNOWN) where
/// FALSE and UNKNOWN filter the same rows.
fn keeps_unknown(position: Position, negated: bool) -> bool {
    match position {
        Position::Conjunct => true,
        Position::Condition { positive } => positive != negated,
        Position::Value => false,
    }
}

fn refusal(negated: bool) -> DataFusionError {
    let form = if negated { "NOT IN" } else { "IN" };
    DataFusionError::NotImplemented(format!(
        "NOT_SUPPORTED: {form} (subquery) whose value or subquery may be NULL is supported only as a \
         condition of WHERE or HAVING (NOT IN joined by AND to the other conditions); write it with \
         EXISTS, or exclude the NULLs"
    ))
}

/// Checks every `IN (subquery)` of `expr` at `position`.
fn check_membership(expr: &Expr, position: Position, schema: &DFSchema) -> Result<()> {
    let inside = match position {
        Position::Conjunct => Position::Condition { positive: true },
        other => other,
    };
    match expr {
        Expr::BinaryExpr(binary) if position != Position::Value && binary.op == Operator::And => {
            check_membership(&binary.left, position, schema)?;
            check_membership(&binary.right, position, schema)
        }
        Expr::BinaryExpr(binary) if position != Position::Value && binary.op == Operator::Or => {
            check_membership(&binary.left, inside, schema)?;
            check_membership(&binary.right, inside, schema)
        }
        // `NOT (x IN …)` as a conjunct is `x NOT IN …`, an anti join.
        Expr::Not(negated)
            if position == Position::Conjunct && matches!(negated.as_ref(), Expr::InSubquery(_)) =>
        {
            check_membership(negated, Position::Conjunct, schema)
        }
        Expr::Not(negated) => match inside {
            Position::Condition { positive } => {
                check_membership(negated, Position::Condition { positive: !positive }, schema)
            }
            _ => check_membership(negated, Position::Value, schema),
        },
        Expr::InSubquery(in_subquery) => {
            if !keeps_unknown(position, in_subquery.negated) && may_be_unknown(in_subquery, schema) {
                return Err(refusal(in_subquery.negated));
            }
            check_membership(&in_subquery.expr, Position::Value, schema)
        }
        other => other
            .apply_children(|child| {
                check_membership(child, Position::Value, schema)?;
                Ok(TreeNodeRecursion::Continue)
            })
            .map(|_| ()),
    }
}

/// Checks every `IN (subquery)` of `node`'s own expressions.
fn check_node(node: &LogicalPlan) -> Result<()> {
    if let LogicalPlan::Filter(filter) = node {
        return check_membership(&filter.predicate, Position::Conjunct, filter.input.schema());
    }
    let inputs = node.inputs();
    let schema = match inputs.as_slice() {
        [input] => input.schema(),
        _ => node.schema(),
    };
    node.apply_expressions(|expr| {
        check_membership(expr, Position::Value, schema)?;
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

impl OptimizerRule for UnknownMembershipGuard {
    fn name(&self) -> &str {
        UNKNOWN_MEMBERSHIP_GUARD
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        plan.apply_with_subqueries(|node| {
            check_node(node)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::ByteSize;
    use crate::config::MemoryConfig;
    use crate::runtime::EngineRuntime as NodeRuntime;
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::config::SessionConfig;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use std::sync::Mutex;

    /// A session with the engine's physical rules and query planner
    /// (`guarded`), or `DataFusion`'s.
    fn session(config: SessionConfig, runtime: Option<Arc<RuntimeEnv>>, guarded: bool) -> SessionContext {
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        if let Some(runtime) = runtime {
            builder = builder.with_runtime_env(runtime);
        }
        if guarded {
            builder = builder
                .with_analyzer_rules(crate::engine::analyzer_rules())
                .with_optimizer_rules(crate::engine::optimizer_rules())
                .with_physical_optimizer_rules(crate::engine::physical_rules())
                .with_query_planner(crate::engine::query_planner());
        }
        SessionContext::new_with_state(builder.build())
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Result<Vec<Vec<Option<i64>>>> {
        let batches = ctx.sql(sql).await?.collect().await?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).map(move |row| {
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                            (!values.is_null(row)).then(|| values.value(row))
                        })
                        .collect()
                })
            })
            .collect())
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        rows(ctx, sql).await.unwrap()[0][0].unwrap()
    }

    // -- 1. Dynamic filter of a null-aware anti join -----------------------

    /// `o.k` in 0..29 with NULLs; `s.n` holds 0..24 in one file and a NULL in
    /// another (what an `INSERT` leaves).
    async fn not_in_files(dir: &std::path::Path) {
        let writer = SessionContext::new();
        let path = |name: &str| dir.join(name).display().to_string();
        std::fs::create_dir_all(dir.join("o")).unwrap();
        std::fs::create_dir_all(dir.join("s")).unwrap();
        for (file, sql) in [
            (
                "o/part-0.parquet",
                "SELECT CAST(value AS BIGINT) AS id, \
                        CASE WHEN value % 100 = 7 THEN NULL ELSE CAST(value % 30 AS BIGINT) END AS k \
                 FROM generate_series(1, 3000)",
            ),
            (
                "s/part-0.parquet",
                "SELECT CAST(value AS BIGINT) AS n FROM generate_series(0, 24)",
            ),
            ("s/part-1.parquet", "SELECT CAST(NULL AS BIGINT) AS n"),
        ] {
            writer
                .sql(&format!("COPY ({sql}) TO '{}' STORED AS PARQUET", path(file)))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
        }
    }

    async fn register_not_in(ctx: &SessionContext, dir: &std::path::Path) {
        for table in ["o", "s"] {
            ctx.register_parquet(
                table,
                format!("{}/", dir.join(table).display()),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        }
    }

    const NOT_IN: &str = "SELECT count(*) FROM o WHERE k NOT IN (SELECT n FROM s)";

    #[tokio::test]
    async fn not_in_sees_the_null_of_the_subquery() {
        let dir = tempfile::tempdir().unwrap();
        not_in_files(dir.path()).await;
        for pushdown in ["false", "true"] {
            let ctx = session(SessionConfig::new(), None, true);
            register_not_in(&ctx, dir.path()).await;
            ctx.sql(&format!(
                "SET datafusion.execution.parquet.pushdown_filters = {pushdown}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
            assert_eq!(count(&ctx, NOT_IN).await, 0, "pushdown_filters = {pushdown}");
            // A filter on the outer key stays on the outer side.
            assert_eq!(
                count(
                    &ctx,
                    "SELECT count(*) FROM o WHERE k > 5 AND k NOT IN (SELECT n FROM s)"
                )
                .await,
                0,
                "pushdown_filters = {pushdown}"
            );
        }
    }

    #[tokio::test]
    async fn not_in_without_a_null_keeps_the_rows_not_in_the_subquery() {
        let dir = tempfile::tempdir().unwrap();
        not_in_files(dir.path()).await;
        std::fs::remove_file(dir.path().join("s/part-1.parquet")).unwrap();
        let ctx = session(SessionConfig::new(), None, true);
        register_not_in(&ctx, dir.path()).await;
        let expected = count(&ctx, "SELECT count(*) FROM o WHERE k >= 25").await;
        assert!(expected > 0);
        assert_eq!(count(&ctx, NOT_IN).await, expected);
    }

    /// Documents apache/datafusion#23104 (row-level filtering, where the
    /// pushed filter drops the NULL row). When this starts failing, upstream
    /// fixed it: run `join_regressions` without the guard, then remove it.
    #[tokio::test]
    async fn upstream_still_prunes_the_null_of_the_subquery() {
        let dir = tempfile::tempdir().unwrap();
        not_in_files(dir.path()).await;
        let ctx = session(
            SessionConfig::new().set_bool("datafusion.execution.parquet.pushdown_filters", true),
            None,
            false,
        );
        register_not_in(&ctx, dir.path()).await;
        assert!(
            count(&ctx, NOT_IN).await > 0,
            "upstream behaviour changed: see the module documentation before removing the guard"
        );
    }

    /// Documents that `push_down_filter` infers a filter for the subquery of
    /// a null-aware join from a filter over it (see
    /// `upstream_still_prunes_the_null_of_the_subquery`).
    #[tokio::test]
    async fn upstream_still_infers_filters_for_the_subquery() {
        let dir = tempfile::tempdir().unwrap();
        not_in_files(dir.path()).await;
        let ctx = session(SessionConfig::new(), None, false);
        register_not_in(&ctx, dir.path()).await;
        assert!(
            count(
                &ctx,
                "SELECT count(*) FROM o WHERE k > 5 AND k NOT IN (SELECT n FROM s)"
            )
            .await
                > 0,
            "upstream behaviour changed: see the module documentation before removing the guard"
        );
    }

    // -- 5. IN (subquery) answered by a mark join ---------------------------

    const MARKED_NOT_IN: &str = "SELECT count(*) FROM (VALUES (1), (2), (3)) t(x) \
         WHERE x NOT IN (SELECT y FROM (VALUES (1), (NULL)) u(y)) OR x = 99";

    /// Documents that `DataFusion` answers a membership outside a conjunct
    /// with a mark join that reads UNKNOWN as FALSE (Trino 431: 0 rows).
    /// When this starts failing, upstream fixed it: see the module
    /// documentation before removing the guard.
    #[tokio::test]
    async fn upstream_mark_join_still_reads_unknown_as_false() {
        let ctx = session(SessionConfig::new(), None, false);
        assert_eq!(
            count(&ctx, MARKED_NOT_IN).await,
            2,
            "upstream behaviour changed: see the module documentation before removing the guard"
        );
    }

    #[tokio::test]
    async fn a_membership_whose_unknown_matters_is_refused() {
        let ctx = session(SessionConfig::new(), None, true);
        let error = rows(&ctx, MARKED_NOT_IN).await.unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::NotImplemented(m) if m.starts_with("NOT_SUPPORTED: NOT IN")),
            "{error}"
        );
        let error = rows(
            &ctx,
            "SELECT count(*) FROM (VALUES (1), (2)) t(x) WHERE (x IN (SELECT y FROM (VALUES (1), (NULL)) u(y))) IS NOT TRUE",
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::NotImplemented(m) if m.starts_with("NOT_SUPPORTED: IN")),
            "{error}"
        );
        // The same conditions over values that cannot be NULL (VALUES
        // columns may be) answer.
        assert_eq!(
            count(
                &ctx,
                "SELECT count(*) FROM (VALUES (1), (2), (3)) t(x) \
                 WHERE coalesce(x, 0) NOT IN (SELECT coalesce(y, 0) FROM (VALUES (1), (2)) u(y)) OR x = 1"
            )
            .await,
            2
        );
        assert_eq!(
            count(
                &ctx,
                "SELECT count(*) FROM (VALUES (1), (2)) t(x) \
                 WHERE (coalesce(x, 0) IN (SELECT coalesce(y, 0) FROM (VALUES (1), (5)) u(y))) IS NOT TRUE"
            )
            .await,
            1
        );
    }

    /// Positions, polarities and nullability, one at a time.
    #[test]
    fn a_membership_keeps_its_unknown_where_false_filters_the_same_rows() {
        use Position::{Condition, Conjunct, Value};
        assert!(keeps_unknown(Conjunct, false));
        assert!(keeps_unknown(Conjunct, true));
        assert!(keeps_unknown(Condition { positive: true }, false));
        assert!(!keeps_unknown(Condition { positive: true }, true));
        assert!(!keeps_unknown(Condition { positive: false }, false));
        assert!(keeps_unknown(Condition { positive: false }, true));
        assert!(!keeps_unknown(Value, false));
        assert!(!keeps_unknown(Value, true));

        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]))
        .unwrap();
        let member = |column: &str, nullable_subquery: bool, negated: bool| {
            let values = Schema::new(vec![Field::new("y", DataType::Int64, nullable_subquery)]);
            let plan = datafusion::logical_expr::table_scan(Some("u"), &values, None)
                .unwrap()
                .build()
                .unwrap();
            let subquery = Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns: Vec::new(),
                spans: datafusion::common::Spans::new(),
            };
            Expr::InSubquery(InSubquery::new(
                Box::new(Expr::Column(Column::from_name(column))),
                subquery,
                negated,
            ))
        };
        for (column, nullable_subquery, unknown) in
            [("v", false, false), ("n", false, true), ("v", true, true)]
        {
            let Expr::InSubquery(in_subquery) = member(column, nullable_subquery, false) else {
                unreachable!()
            };
            assert_eq!(
                may_be_unknown(&in_subquery, &schema),
                unknown,
                "{column} {nullable_subquery}"
            );
        }
        let other = Expr::Column(Column::from_name("v")).gt(Expr::Literal(ScalarValue::Int64(Some(0)), None));
        let ok = |expr: &Expr| check_membership(expr, Conjunct, &schema).is_ok();
        // Conjuncts, negated or not.
        assert!(ok(&member("n", true, true).and(other.clone())));
        assert!(ok(&Expr::Not(Box::new(member("n", true, false)))));
        // Under OR: a positive IN answers, a NOT IN does not; NOT flips.
        assert!(ok(&member("n", true, false).or(other.clone())));
        assert!(!ok(&member("n", true, true).or(other.clone())));
        assert!(!ok(&Expr::Not(Box::new(
            member("n", true, false).or(other.clone())
        ))));
        assert!(ok(&Expr::Not(Box::new(
            member("n", true, true).or(other.clone())
        ))));
        assert!(ok(&Expr::Not(Box::new(Expr::Not(Box::new(
            member("n", true, false).or(other.clone())
        ))))));
        // Anywhere else only without a possible NULL.
        assert!(!ok(&member("n", true, false).is_not_true()));
        assert!(!ok(&member("v", true, false).eq(other.clone())));
        assert!(ok(&member("v", false, true).is_not_true()));
        assert!(ok(&member("v", false, true).or(other)));
        assert!(check_membership(&member("v", false, false), Value, &schema).is_ok());
        assert!(check_membership(&member("v", true, false), Value, &schema).is_err());
    }

    #[test]
    fn every_push_down_filter_is_wrapped_and_keeps_its_name() {
        let rules = crate::engine::optimizer_rules();
        let names: Vec<&str> = rules.iter().map(|r| r.name()).collect();
        assert!(
            names.iter().filter(|n| **n == PUSH_DOWN_FILTER).count() >= 2,
            "{names:?}"
        );
        let wrapped = guard_filter_pushdown(datafusion::optimizer::Optimizer::new().rules);
        assert_eq!(
            wrapped.iter().map(|r| r.name().to_owned()).collect::<Vec<_>>(),
            datafusion::optimizer::Optimizer::new()
                .rules
                .iter()
                .map(|r| r.name().to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn the_barrier_refuses_every_parent_filter() {
        use datafusion::physical_expr::expressions::lit;
        use datafusion::physical_plan::filter_pushdown::{FilterPushdownPhase, PushedDown};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let barrier = NullAwareProbeBarrier::new(Arc::new(EmptyExec::new(schema)));
        for phase in [FilterPushdownPhase::Pre, FilterPushdownPhase::Post] {
            let description = barrier
                .gather_filters_for_pushdown(phase, vec![lit(true)], &ConfigOptions::default())
                .unwrap();
            let child = &description.parent_filters()[0];
            assert!(child.iter().all(|f| matches!(f.discriminant, PushedDown::No)));
        }
        assert!(Arc::new(barrier).with_new_children(vec![]).is_err());
    }

    // -- 2. Sort-merge join ------------------------------------------------

    fn int_table(name: &str, values: &[Option<i64>], partitions: usize) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        let chunk = values.len().div_ceil(partitions).max(1);
        let batches = values
            .chunks(chunk)
            .map(|part| {
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from(part.to_vec()))],
                    )
                    .unwrap(),
                ]
            })
            .collect();
        Arc::new(MemTable::try_new(schema, batches).unwrap())
    }

    fn sort_merge_session(guarded: bool, subquery_null: bool) -> SessionContext {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_repartition_joins(true)
            .set_bool("datafusion.optimizer.prefer_hash_join", false)
            .set_bool("datafusion.optimizer.enable_join_dynamic_filter_pushdown", false);
        let ctx = session(config, None, guarded);
        let outer: Vec<Option<i64>> = (0..400).map(|x| (x % 13 != 0).then_some(x % 30)).collect();
        let mut inner: Vec<Option<i64>> = (0..25).map(Some).collect();
        if subquery_null {
            inner.push(None);
        }
        ctx.register_table("o", int_table("k", &outer, 4)).unwrap();
        ctx.register_table("s", int_table("n", &inner, 4)).unwrap();
        ctx
    }

    const NOT_IN_MEMORY: &str = "SELECT count(*) FROM o WHERE k NOT IN (SELECT n FROM s)";

    #[tokio::test]
    async fn not_in_is_null_aware_when_hash_joins_are_not_preferred() {
        let with_null = sort_merge_session(true, true);
        assert_eq!(count(&with_null, NOT_IN_MEMORY).await, 0);
        let without_null = sort_merge_session(true, false);
        let expected = count(&without_null, "SELECT count(*) FROM o WHERE k >= 25").await;
        assert_eq!(count(&without_null, NOT_IN_MEMORY).await, expected);
    }

    /// Documents apache/datafusion#22810 (see
    /// `upstream_still_prunes_the_null_of_the_subquery`).
    #[tokio::test]
    async fn upstream_sort_merge_join_still_ignores_nulls() {
        let ctx = sort_merge_session(false, true);
        assert!(
            count(&ctx, NOT_IN_MEMORY).await > 0,
            "upstream behaviour changed: see the module documentation before removing the guard"
        );
    }

    // -- 3. Nested loop join spill fallback ----------------------------------

    fn wide_table(prefix: &str, rows: i64, modulo: i64, partitions: usize) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("pad", DataType::Utf8, false),
        ]));
        let per = usize::try_from(rows).unwrap().div_ceil(partitions);
        let ids: Vec<i64> = (1..=rows).collect();
        let batches = ids
            .chunks(per)
            .map(|part| {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(part.to_vec())),
                        Arc::new(Int64Array::from(
                            part.iter().map(|id| id % modulo).collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(
                            part.iter()
                                .map(|id| format!("{prefix}{id:0>4000}"))
                                .collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap();
                vec![batch]
            })
            .collect();
        Arc::new(MemTable::try_new(schema, batches).unwrap())
    }

    /// `s` (about 5 MB) builds a `LEFT` nested loop join probed by the four
    /// partitions of `b`; the pool holds 4 MiB.
    fn nested_loop_session(dir: &std::path::Path, limit: ByteSize, guarded: bool) -> SessionContext {
        let runtime = NodeRuntime::new(&MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(512),
            metadata_cache_limit: ByteSize::mib(1),
        })
        .unwrap()
        .env();
        let config = SessionConfig::new().with_target_partitions(4).with_batch_size(64);
        let ctx = session(config, Some(runtime), guarded);
        ctx.register_table("s", wide_table("s", 1200, 97, 1)).unwrap();
        ctx.register_table("b", wide_table("b", 2000, 101, 4)).unwrap();
        ctx
    }

    const LEFT_JOIN: &str = "SELECT count(*), count(b.id), sum(length(s.pad)), sum(length(b.pad)) FROM s LEFT JOIN b ON s.v > b.v + 90";

    #[tokio::test]
    async fn a_nested_loop_join_over_the_limit_fails_instead_of_spilling() {
        let dir = tempfile::tempdir().unwrap();
        let roomy = nested_loop_session(dir.path(), ByteSize::gib(1), true);
        let expected = rows(&roomy, LEFT_JOIN).await.unwrap();
        let tight = nested_loop_session(dir.path(), ByteSize::mib(4), true);
        let plan = tight
            .sql(&format!("EXPLAIN {LEFT_JOIN}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(text.contains("NestedLoopJoinWithoutFallback"), "{text}");
        assert!(text.contains("NestedLoopJoinExec: join_type="), "{text}");
        let error = rows(&tight, LEFT_JOIN).await.unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert!(error.to_string().contains("NestedLoopJoinLoad"), "{error}");
        assert_eq!(rows(&roomy, LEFT_JOIN).await.unwrap(), expected);
    }

    /// Documents apache/datafusion#24675 (see
    /// `upstream_still_prunes_the_null_of_the_subquery`): without the guard the
    /// join spills and emits unmatched rows once per probe partition.
    #[tokio::test]
    async fn upstream_nested_loop_fallback_still_repeats_unmatched_rows() {
        let dir = tempfile::tempdir().unwrap();
        let expected = rows(
            &nested_loop_session(dir.path(), ByteSize::gib(1), true),
            LEFT_JOIN,
        )
        .await
        .unwrap();
        let unguarded = nested_loop_session(dir.path(), ByteSize::mib(4), false);
        let answer = rows(&unguarded, LEFT_JOIN).await;
        assert!(
            answer.as_ref().is_ok_and(|answer| *answer != expected),
            "upstream behaviour changed ({answer:?} vs {expected:?}): see the module documentation \
             before removing the guard"
        );
    }

    /// Records whether the runtime it runs with can write temporary files.
    #[derive(Debug)]
    struct SeesDisk {
        properties: Arc<PlanProperties>,
        seen: Arc<Mutex<Vec<bool>>>,
    }

    impl SeesDisk {
        fn new(seen: Arc<Mutex<Vec<bool>>>) -> Self {
            let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
            Self {
                properties: Arc::clone(EmptyExec::new(schema).properties()),
                seen,
            }
        }
    }

    impl DisplayAs for SeesDisk {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "SeesDisk")
        }
    }

    impl ExecutionPlan for SeesDisk {
        fn name(&self) -> &'static str {
            "SeesDisk"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, _partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            self.seen
                .lock()
                .unwrap()
                .push(context.runtime_env().disk_manager.tmp_files_enabled());
            let schema = self.schema();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )))
        }
    }

    #[tokio::test]
    async fn the_join_runs_without_disk_and_its_inputs_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = NodeRuntime::new(&MemoryConfig {
            limit: ByteSize::mib(64),
            spill_dir: dir.path().to_owned(),
            spill_limit: ByteSize::mib(64),
            metadata_cache_limit: ByteSize::mib(1),
        })
        .unwrap()
        .env();
        let context = Arc::new(TaskContext::default().with_runtime(Arc::clone(&runtime)));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let join_itself = NestedLoopJoinWithoutFallback::new(Arc::new(SeesDisk::new(Arc::clone(&seen))));
        join_itself.execute(0, Arc::clone(&context)).unwrap();
        let input = NestedLoopJoinWithoutFallback::new(Arc::new(JoinInputRuntime::new(Arc::new(
            SeesDisk::new(Arc::clone(&seen)),
        ))));
        input.execute(0, Arc::clone(&context)).unwrap();
        // Outside a guarded join the input keeps its context.
        JoinInputRuntime::new(Arc::new(SeesDisk::new(Arc::clone(&seen))))
            .execute(0, context)
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![false, true, true]);
    }

    #[test]
    fn the_spill_guard_wraps_each_join_once() {
        use datafusion::physical_plan::joins::NestedLoopJoinExec;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let side = || Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let join: Arc<dyn ExecutionPlan> = Arc::new(
            NestedLoopJoinExec::try_new(side(), side(), None, &datafusion::common::JoinType::Inner, None)
                .unwrap(),
        );
        let once = NestedLoopJoinSpillGuard
            .optimize(join, &ConfigOptions::default())
            .unwrap();
        let twice = NestedLoopJoinSpillGuard
            .optimize(Arc::clone(&once), &ConfigOptions::default())
            .unwrap();
        for plan in [&once, &twice] {
            assert_eq!(plan.name(), "NestedLoopJoinWithoutFallback");
            let join = plan.children()[0];
            assert_eq!(join.name(), "NestedLoopJoinExec");
            assert!(join.children().iter().all(|c| c.name() == "JoinInputRuntime"));
            assert!(
                join.children()
                    .iter()
                    .all(|c| c.children()[0].name() == "EmptyExec")
            );
        }
    }
}
