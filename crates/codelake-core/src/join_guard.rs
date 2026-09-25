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
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, Filter, Join, LogicalPlan, LogicalPlanBuilder, Subquery};
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

/// Name of [`NullAwareAntiJoinGuard`].
pub const NULL_AWARE_ANTI_JOIN_GUARD: &str = "codelake_null_aware_anti_join_guard";

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

/// `rules` with [`NullSafeNotIn`] right before `decorrelate_predicate_subquery`.
pub fn with_null_safe_not_in(
    mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let position = rules
        .iter()
        .position(|r| r.name() == DECORRELATE_PREDICATE_SUBQUERY)
        .unwrap_or(0);
    rules.insert(position, Arc::new(NullSafeNotIn));
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};

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
}
