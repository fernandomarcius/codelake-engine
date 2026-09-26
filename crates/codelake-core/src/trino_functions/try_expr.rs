//! `try(expression)`: the value of `expression`, or NULL for the rows where
//! evaluating it fails with a user error (division by zero, invalid cast,
//! invalid function argument, numeric overflow — Trino's `TryFunction`).
//!
//! Arguments of a scalar function are evaluated before the function runs, so
//! `try` cannot catch anything as a plain UDF. The UDF below only carries the
//! call through planning; [`TrinoTry`], the last physical optimizer rule,
//! replaces it by [`TryExpr`] in projections and filters. `TryExpr` evaluates
//! the whole batch and, when that fails with a user error, splits the batch
//! until each failing row is isolated and returns NULL for it.
//!
//! It also rewrites aggregate arguments, filters, grouping expressions, join
//! keys and conditions (hash and nested-loop joins) and sort keys. Window
//! function arguments are made to fail on overflow by the logical rule in
//! `keys`; there, and in any other operator, a `try` call stays a
//! pass-through: the expression's error surfaces as a query failure, never as
//! a different answer.
//!
//! The same rule makes `+`, `-` and `*` fail on overflow, as Trino's integer
//! and decimal arithmetic does (`DataFusion` wraps around silently by
//! default), and compares floats with IEEE NaN semantics (see `nan`).

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, new_null_array};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, Operator, ScalarUDF, Volatility};
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, NestedLoopJoinExecBuilder};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use super::errors::caught_by_try;
use super::support::{Func, not_found};

pub(crate) const TRY: &str = "try";
/// `try` after the function rewrite guarded its argument (see `rewrite`).
pub(crate) const GUARDED_TRY: &str = "codelake_trino_try";
/// Identity the simplifier cannot evaluate: constants inside `try(…)` must
/// fail at run time, where `try` catches them, not while planning.
pub(crate) const BARRIER: &str = "codelake_trino_try_barrier";

fn passthrough(name: &'static str) -> ScalarUDF {
    Func::new(
        name,
        |name, types| match types {
            [t] => Ok(vec![t.clone()]),
            _ => Err(not_found(name, types)),
        },
        |r| {
            r.types
                .first()
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
        },
        |call| call.arg(0).cloned(),
    )
    .volatility(Volatility::Volatile)
    .udf()
}

/// The guarded `try` and the barrier.
pub(crate) fn planning_udfs() -> Vec<ScalarUDF> {
    vec![passthrough(GUARDED_TRY), passthrough(BARRIER)]
}

/// The planning-time `try`: returns its argument.
pub(crate) fn try_udf() -> ScalarUDF {
    Func::new(
        TRY,
        |name, types| match types {
            [t] => Ok(vec![t.clone()]),
            _ => Err(not_found(name, types)),
        },
        |r| {
            r.types
                .first()
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("try without argument".into()))
        },
        |call| call.arg(0).cloned(),
    )
    // Volatile: the simplifier must not fold `try(…)` away from its argument.
    .volatility(Volatility::Volatile)
    .udf()
}

/// `try(child)` evaluated with Trino semantics.
#[derive(Debug, Clone)]
pub(crate) struct TryExpr {
    child: Arc<dyn PhysicalExpr>,
}

impl std::hash::Hash for TryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PartialEq for TryExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for TryExpr {}

impl TryExpr {
    pub(crate) fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }

    /// Evaluates `batch`, bisecting around the rows that fail.
    fn evaluate_rows(&self, batch: &RecordBatch, data_type: &DataType) -> Result<ArrayRef> {
        let rows = batch.num_rows();
        match self.child.evaluate(batch).and_then(|v| v.into_array(rows)) {
            Ok(values) => Ok(values),
            Err(error) if !caught_by_try(&error) => Err(error),
            Err(_) if rows <= 1 => Ok(new_null_array(data_type, rows)),
            Err(_) => {
                let half = rows / 2;
                let head = self.evaluate_rows(&batch.slice(0, half), data_type)?;
                let tail = self.evaluate_rows(&batch.slice(half, rows - half), data_type)?;
                Ok(concat(&[head.as_ref(), tail.as_ref()])?)
            }
        }
    }
}

impl fmt::Display for TryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "try({})", self.child)
    }
}

impl PhysicalExpr for TryExpr {
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let field = self.child.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            field.name(),
            field.data_type().clone(),
            true,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        match self.child.evaluate(batch) {
            Ok(value) => Ok(value),
            Err(error) if !caught_by_try(&error) => Err(error),
            Err(_) => {
                let data_type = self.child.data_type(batch.schema().as_ref())?;
                if collect_columns(&self.child).is_empty() {
                    // Fails the same way on every row.
                    return Ok(ColumnarValue::Scalar(ScalarValue::try_from(&data_type)?));
                }
                Ok(ColumnarValue::Array(self.evaluate_rows(batch, &data_type)?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let child = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("try needs one child".into()))?;
        Ok(Arc::new(Self::new(child)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "try(")?;
        self.child.fmt_sql(f)?;
        write!(f, ")")
    }
}

/// Replaces planned `try` calls by [`TryExpr`] and makes arithmetic check overflow.
fn install_try(expr: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expr.transform_up(|e| {
        if let Some(function) = e.downcast_ref::<ScalarFunctionExpr>()
            && (function.name() == TRY || function.name() == GUARDED_TRY)
            && let [child] = function.args()
        {
            let replaced: Arc<dyn PhysicalExpr> = Arc::new(TryExpr::new(Arc::clone(child)));
            return Ok(Transformed::yes(replaced));
        }
        if let Some(binary) = e.downcast_ref::<BinaryExpr>()
            && super::nan::is_float_comparison(binary, schema)
            && !super::nan::same_in_both_orders(binary)
        {
            let replaced: Arc<dyn PhysicalExpr> = Arc::new(super::nan::NanAwareComparison::new(binary));
            return Ok(Transformed::yes(replaced));
        }
        if let Some(binary) = e.downcast_ref::<BinaryExpr>()
            && matches!(binary.op(), Operator::Plus | Operator::Minus | Operator::Multiply)
        {
            let checked = BinaryExpr::new(
                Arc::clone(binary.left()),
                *binary.op(),
                Arc::clone(binary.right()),
            )
            .with_fail_on_overflow(true);
            if checked != *binary {
                let replaced: Arc<dyn PhysicalExpr> = Arc::new(checked);
                return Ok(Transformed::yes(replaced));
            }
        }
        Ok(Transformed::no(e))
    })
}

fn rewrite_node(node: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        let mut changed = false;
        let exprs = projection
            .expr()
            .iter()
            .map(|e| {
                let rewritten = install_try(Arc::clone(&e.expr), &projection.input().schema())?;
                changed |= rewritten.transformed;
                Ok(ProjectionExpr {
                    expr: rewritten.data,
                    alias: e.alias.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if changed {
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(ProjectionExec::try_new(exprs, Arc::clone(projection.input()))?);
            return Ok(Transformed::yes(plan));
        }
    } else if let Some(filter) = node.downcast_ref::<FilterExec>() {
        let predicate = install_try(Arc::clone(filter.predicate()), &filter.input().schema())?;
        if predicate.transformed {
            let plan: Arc<dyn ExecutionPlan> = Arc::new(
                FilterExecBuilder::from(filter)
                    .with_predicate(predicate.data)
                    .build()?,
            );
            return Ok(Transformed::yes(plan));
        }
    } else if let Some(aggregate) = node.downcast_ref::<AggregateExec>()
        && let Some(rewritten) = rewrite_aggregate(aggregate)?
    {
        return Ok(Transformed::yes(rewritten));
    } else if let Some(join) = node.downcast_ref::<HashJoinExec>()
        && let Some(rewritten) = rewrite_hash_join(join)?
    {
        return Ok(Transformed::yes(rewritten));
    } else if let Some(join) = node.downcast_ref::<NestedLoopJoinExec>()
        && let Some(filter) = rewrite_join_filter(join.filter())?
    {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            NestedLoopJoinExecBuilder::new(
                Arc::clone(join.left()),
                Arc::clone(join.right()),
                *join.join_type(),
            )
            .with_projection_ref(join.projection().clone())
            .with_filter(Some(filter))
            .build()?,
        );
        return Ok(Transformed::yes(plan));
    } else if let Some(sort) = node.downcast_ref::<SortExec>()
        && let Some(ordering) = rewrite_ordering(sort.expr(), &sort.input().schema())?
    {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering, Arc::clone(sort.input()))
                .with_preserve_partitioning(sort.preserve_partitioning())
                .with_fetch(sort.fetch()),
        );
        return Ok(Transformed::yes(plan));
    } else if let Some(merge) = node.downcast_ref::<SortPreservingMergeExec>()
        && let Some(ordering) = rewrite_ordering(merge.expr(), &merge.input().schema())?
    {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortPreservingMergeExec::new(ordering, Arc::clone(merge.input())).with_fetch(merge.fetch()),
        );
        return Ok(Transformed::yes(plan));
    }
    Ok(Transformed::no(node))
}

/// A join's residual condition (non-equi part), checked like a filter.
fn rewrite_join_filter(filter: Option<&JoinFilter>) -> Result<Option<JoinFilter>> {
    let Some(filter) = filter else { return Ok(None) };
    let expression = install_try(Arc::clone(filter.expression()), filter.schema())?;
    if !expression.transformed {
        return Ok(None);
    }
    Ok(Some(JoinFilter::new(
        expression.data,
        filter.column_indices().to_vec(),
        Arc::clone(filter.schema()),
    )))
}

/// Join keys and residual condition of a hash join. New keys drop the
/// dynamic filter the join would push to its probe side (built from the old
/// keys; the pushed copy stays a pass-through `true`).
fn rewrite_hash_join(join: &HashJoinExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let (left, right) = (join.left().schema(), join.right().schema());
    let mut keys_changed = false;
    let on = join
        .on()
        .iter()
        .map(|(l, r)| {
            let (l, r) = (
                install_try(Arc::clone(l), &left)?,
                install_try(Arc::clone(r), &right)?,
            );
            keys_changed |= l.transformed || r.transformed;
            Ok((l.data, r.data))
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = rewrite_join_filter(join.filter())?;
    if !keys_changed && filter.is_none() {
        return Ok(None);
    }
    let mut builder = join.builder();
    if keys_changed {
        builder = builder.with_on(on).reset_state();
    }
    if let Some(filter) = filter {
        builder = builder.with_filter(Some(filter));
    }
    Ok(Some(builder.build_exec()?))
}

/// Sort keys (`ORDER BY x + 1`), checked like projections.
fn rewrite_ordering(ordering: &LexOrdering, schema: &Schema) -> Result<Option<LexOrdering>> {
    let mut changed = false;
    let exprs = ordering
        .iter()
        .map(|sort| {
            let expr = install_try(Arc::clone(&sort.expr), schema)?;
            changed |= expr.transformed;
            Ok(PhysicalSortExpr::new(expr.data, sort.options))
        })
        .collect::<Result<Vec<_>>>()?;
    if !changed {
        return Ok(None);
    }
    Ok(LexOrdering::new(exprs))
}

/// Aggregate arguments, filters and grouping expressions of the modes that
/// read input rows (the final modes read partial states).
fn rewrite_aggregate(aggregate: &AggregateExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if !matches!(
        aggregate.mode(),
        AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned
    ) {
        return Ok(None);
    }
    let schema = aggregate.input_schema();
    let mut changed = false;
    let mut install = |expr: &Arc<dyn PhysicalExpr>| -> Result<Arc<dyn PhysicalExpr>> {
        let rewritten = install_try(Arc::clone(expr), &schema)?;
        changed |= rewritten.transformed;
        Ok(rewritten.data)
    };
    let mut aggr_exprs = Vec::with_capacity(aggregate.aggr_expr().len());
    for function in aggregate.aggr_expr() {
        let args = function
            .expressions()
            .iter()
            .map(&mut install)
            .collect::<Result<Vec<_>>>()?;
        let order_by = function
            .order_bys()
            .iter()
            .map(|s| install(&s.expr))
            .collect::<Result<Vec<_>>>()?;
        aggr_exprs.push(match function.with_new_expressions(args, order_by) {
            Some(rewritten) => Arc::new(rewritten),
            None => Arc::clone(function),
        });
    }
    let filters = aggregate
        .filter_expr()
        .iter()
        .map(|f| f.as_ref().map(&mut install).transpose())
        .collect::<Result<Vec<_>>>()?;
    let group_by = aggregate.group_expr();
    let groups = group_by
        .expr()
        .iter()
        .map(|(e, name)| Ok((install(e)?, name.clone())))
        .collect::<Result<Vec<_>>>()?;
    if !changed {
        return Ok(None);
    }
    let group_by = PhysicalGroupBy::new(
        groups,
        group_by.null_expr().to_vec(),
        group_by.groups().to_vec(),
        group_by.has_grouping_set(),
    );
    let rebuilt = AggregateExec::try_new(
        *aggregate.mode(),
        group_by,
        aggr_exprs,
        filters,
        Arc::clone(aggregate.input()),
        schema,
    )?
    .with_limit_options(aggregate.limit_options());
    Ok(Some(Arc::new(rebuilt)))
}

/// Physical optimizer rule installing [`TryExpr`]; runs after every other rule.
#[derive(Debug, Default)]
pub(crate) struct TrinoTry;

impl PhysicalOptimizerRule for TrinoTry {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(rewrite_node).data()
    }

    fn name(&self) -> &'static str {
        "trino_try"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, lit};

    fn batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn ten_divided_by_x() -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            lit(10_i64),
            Operator::Divide,
            Arc::new(Column::new("x", 0)),
        ))
    }

    #[test]
    fn failing_rows_become_null_and_the_others_keep_their_value() {
        let expr = TryExpr::new(ten_divided_by_x());
        let out = expr
            .evaluate(&batch(vec![0, 2, 5, 0, 10]))
            .unwrap()
            .into_array(5)
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        let values: Vec<Option<i64>> = (0..out.len())
            .map(|i| (!out.is_null(i)).then(|| out.value(i)))
            .collect();
        assert_eq!(values, vec![None, Some(5), Some(2), None, Some(1)]);
    }

    #[test]
    fn arithmetic_fails_on_overflow() {
        let sum: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Plus,
            lit(1_i64),
        ));
        let schema = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let checked = install_try(sum, &schema).unwrap();
        assert!(checked.transformed);
        assert!(checked.data.evaluate(&batch(vec![i64::MAX])).is_err());
        assert!(
            !install_try(checked.data, &schema).unwrap().transformed,
            "already checked"
        );
    }

    #[test]
    fn errors_try_does_not_catch_still_fail() {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct Fails;
        impl fmt::Display for Fails {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "fails")
            }
        }
        impl PhysicalExpr for Fails {
            fn return_field(&self, _: &Schema) -> Result<FieldRef> {
                Ok(Arc::new(Field::new("f", DataType::Int64, true)))
            }
            fn evaluate(&self, _: &RecordBatch) -> Result<ColumnarValue> {
                Err(DataFusionError::ResourcesExhausted("pool".into()))
            }
            fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
                vec![]
            }
            fn with_new_children(
                self: Arc<Self>,
                _: Vec<Arc<dyn PhysicalExpr>>,
            ) -> Result<Arc<dyn PhysicalExpr>> {
                Ok(self)
            }
            fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "fails")
            }
        }
        let expr = TryExpr::new(Arc::new(Fails));
        assert!(expr.evaluate(&batch(vec![1])).is_err());
    }
}
