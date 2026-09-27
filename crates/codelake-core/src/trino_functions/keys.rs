//! Logical rule, run after every other optimizer rule (after federation,
//! so remote plans are never touched), for what the physical rule in
//! `try_expr` cannot reach:
//!
//! - **Float keys.** The engine hashes and compares floats by their bits:
//!   `-0.0` and `0.0` are two groups, two join keys that do not match, and
//!   NaN join keys match each other. Trino's `=` treats `-0.0 = 0.0` and never
//!   matches NaN; `GROUP BY`, `DISTINCT` and set operations put every NaN in
//!   one group. Float equi-join keys become `codelake_trino_float_key(k)`
//!   (NaN → NULL, which matches nothing; for `IS NOT DISTINCT FROM` and
//!   null-aware joins only the zero is normalized), grouping keys and
//!   `DISTINCT` aggregate arguments `codelake_trino_float_group_key(k)`, both
//!   under the original output name (in `GROUPING SETS`, `ROLLUP` and
//!   `CUBE` too). Window `PARTITION BY` and `ORDER BY` keys get the grouping
//!   form too (`-0.0` and `0.0` are peers).
//! - **Window function arguments.** `lag(x + 1)` evaluates `x + 1` inside
//!   the window operator, where the physical rule cannot make it fail on
//!   overflow: integer `+ - *` there becomes Trino's checked functions.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, NullEquality, Result};
use datafusion::logical_expr::expr::{AggregateFunction, GroupingSet, ScalarFunction};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

use super::nan::{FLOAT_GROUP_KEY, FLOAT_KEY};

#[derive(Debug)]
pub(crate) struct TrinoKeys {
    float_key: Arc<ScalarUDF>,
    group_key: Arc<ScalarUDF>,
    checked: HashMap<Operator, Arc<ScalarUDF>>,
}

fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float32 | DataType::Float64)
}

fn is_key_call(expr: &Expr) -> bool {
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    matches!(inner, Expr::ScalarFunction(f) if f.name() == FLOAT_KEY || f.name() == FLOAT_GROUP_KEY)
}

impl TrinoKeys {
    pub(crate) fn new() -> Option<Self> {
        let find = |name: &str| {
            super::nan::udfs()
                .into_iter()
                .chain(super::decimal::udfs())
                .find(|f| f.name() == name)
                .map(Arc::new)
        };
        let mut checked = HashMap::new();
        for op in [Operator::Plus, Operator::Minus, Operator::Multiply] {
            checked.insert(op, find(super::decimal::checked_integer_name(op)?)?);
        }
        Some(Self {
            float_key: find(FLOAT_KEY)?,
            group_key: find(FLOAT_GROUP_KEY)?,
            checked,
        })
    }

    fn wrap(function: &Arc<ScalarUDF>, expr: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(function), vec![expr]))
    }

    fn join(&self, mut join: datafusion::logical_expr::Join) -> Transformed<LogicalPlan> {
        let function = if join.null_equality == NullEquality::NullEqualsNothing && !join.null_aware {
            &self.float_key
        } else {
            &self.group_key
        };
        let mut changed = false;
        let mut on = Vec::with_capacity(join.on.len());
        for (left, right) in std::mem::take(&mut join.on) {
            let float = left.get_type(join.left.schema()).is_ok_and(|t| is_float(&t))
                && right.get_type(join.right.schema()).is_ok_and(|t| is_float(&t));
            if float && !is_key_call(&left) {
                changed = true;
                on.push((Self::wrap(function, left), Self::wrap(function, right)));
            } else {
                on.push((left, right));
            }
        }
        join.on = on;
        Transformed::new_transformed(LogicalPlan::Join(join), changed)
    }

    fn aggregate(&self, mut aggregate: datafusion::logical_expr::Aggregate) -> Transformed<LogicalPlan> {
        let input = Arc::clone(aggregate.input.schema());
        let output = Arc::clone(&aggregate.schema);
        let mut changed = false;
        let groups = aggregate.group_expr.len();
        if let [Expr::GroupingSet(set)] = aggregate.group_expr.as_mut_slice() {
            changed |= self.grouping_set(set, &input, &output);
        }
        for (i, expr) in aggregate.group_expr.iter_mut().enumerate() {
            if matches!(expr, Expr::GroupingSet(_)) || is_key_call(expr) {
                continue;
            }
            if expr.get_type(&input).is_ok_and(|t| is_float(&t)) {
                let (qualifier, field) = output.qualified_field(i);
                let original =
                    std::mem::replace(expr, Expr::Literal(datafusion::common::ScalarValue::Null, None));
                *expr =
                    Self::wrap(&self.group_key, original).alias_qualified(qualifier.cloned(), field.name());
                changed = true;
            }
        }
        for (i, expr) in aggregate.aggr_expr.iter_mut().enumerate() {
            if let Some(rewritten) = self.distinct_argument(expr, &input, &output, groups + i) {
                *expr = rewritten;
                changed = true;
            }
        }
        Transformed::new_transformed(LogicalPlan::Aggregate(aggregate), changed)
    }

    /// The float expressions of `GROUPING SETS`, `ROLLUP` and `CUBE` as
    /// grouping keys, under the name of the output column each one is.
    fn grouping_set(&self, set: &mut GroupingSet, input: &DFSchema, output: &DFSchema) -> bool {
        let keyed: Vec<(Expr, Expr)> = set
            .distinct_expr()
            .into_iter()
            .enumerate()
            .filter(|(_, e)| !is_key_call(e) && e.get_type(input).is_ok_and(|t| is_float(&t)))
            .map(|(i, e)| {
                let (qualifier, field) = output.qualified_field(i);
                let key =
                    Self::wrap(&self.group_key, e.clone()).alias_qualified(qualifier.cloned(), field.name());
                (e.clone(), key)
            })
            .collect();
        if keyed.is_empty() {
            return false;
        }
        let replace = |exprs: &mut Vec<Expr>| {
            for expr in exprs {
                if let Some((_, key)) = keyed.iter().find(|(original, _)| original == expr) {
                    *expr = key.clone();
                }
            }
        };
        match set {
            GroupingSet::Rollup(exprs) | GroupingSet::Cube(exprs) => replace(exprs),
            GroupingSet::GroupingSets(sets) => sets.iter_mut().for_each(replace),
        }
        true
    }

    /// `count(DISTINCT x)` of a float: the argument as a grouping key.
    fn distinct_argument(
        &self,
        expr: &Expr,
        input: &DFSchema,
        output: &DFSchema,
        index: usize,
    ) -> Option<Expr> {
        let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
            return None;
        };
        if !params.distinct || params.args.len() != 1 || params.args.iter().any(is_key_call) {
            return None;
        }
        if !params.args[0].get_type(input).is_ok_and(|t| is_float(&t)) {
            return None;
        }
        let mut params = params.clone();
        params.args = vec![Self::wrap(&self.group_key, params.args[0].clone())];
        let (qualifier, field) = output.qualified_field(index);
        Some(
            Expr::AggregateFunction(AggregateFunction {
                func: Arc::clone(func),
                params,
            })
            .alias_qualified(qualifier.cloned(), field.name()),
        )
    }

    /// Integer `+ - *` in `expr` as checked functions.
    fn checked_arithmetic(&self, expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        expr.transform_up(|e| {
            if let Expr::BinaryExpr(binary) = &e
                && let Some(function) = self.checked.get(&binary.op)
                && binary
                    .left
                    .get_type(schema)
                    .is_ok_and(|t| super::support::is_integer(&t))
                && binary
                    .right
                    .get_type(schema)
                    .is_ok_and(|t| super::support::is_integer(&t))
            {
                let args = vec![binary.left.as_ref().clone(), binary.right.as_ref().clone()];
                return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::clone(function),
                    args,
                ))));
            }
            Ok(Transformed::no(e))
        })
    }

    fn window(
        &self,
        mut window: datafusion::logical_expr::logical_plan::Window,
    ) -> Result<Transformed<LogicalPlan>> {
        let input = Arc::clone(window.input.schema());
        let output = Arc::clone(&window.schema);
        let offset = input.fields().len();
        let mut changed = false;
        for (i, expr) in window.window_expr.iter_mut().enumerate() {
            let aliased = matches!(expr, Expr::Alias(_));
            let inner = match &mut *expr {
                Expr::Alias(alias) => alias.expr.as_mut(),
                other => other,
            };
            let Expr::WindowFunction(function) = inner else {
                continue;
            };
            let mut touched = false;
            let args = std::mem::take(&mut function.params.args);
            let mut rewritten = Vec::with_capacity(args.len());
            for arg in args {
                let result = self.checked_arithmetic(arg, &input)?;
                touched |= result.transformed;
                rewritten.push(result.data);
            }
            function.params.args = rewritten;
            // `PARTITION BY` a float: `-0.0` and `0.0` (and every NaN) are
            // one partition, as one group.
            for key in &mut function.params.partition_by {
                if !is_key_call(key) && key.get_type(&input).is_ok_and(|t| is_float(&t)) {
                    let original =
                        std::mem::replace(key, Expr::Literal(datafusion::common::ScalarValue::Null, None));
                    *key = Self::wrap(&self.group_key, original);
                    touched = true;
                }
            }
            // `ORDER BY` a float: `-0.0` and `0.0` are peers (ranks, frames),
            // as they are one value everywhere (ADR 0017).
            for sort in &mut function.params.order_by {
                if !is_key_call(&sort.expr) && sort.expr.get_type(&input).is_ok_and(|t| is_float(&t)) {
                    let original = std::mem::replace(
                        &mut sort.expr,
                        Expr::Literal(datafusion::common::ScalarValue::Null, None),
                    );
                    sort.expr = Self::wrap(&self.group_key, original);
                    touched = true;
                }
            }
            if touched && !aliased {
                let (qualifier, field) = output.qualified_field(offset + i);
                let original =
                    std::mem::replace(expr, Expr::Literal(datafusion::common::ScalarValue::Null, None));
                *expr = original.alias_qualified(qualifier.cloned(), field.name());
            }
            changed |= touched;
        }
        Ok(Transformed::new_transformed(LogicalPlan::Window(window), changed))
    }
}

impl OptimizerRule for TrinoKeys {
    fn name(&self) -> &'static str {
        "trino_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(join) => Ok(self.join(join)),
            LogicalPlan::Aggregate(aggregate) => Ok(self.aggregate(aggregate)),
            LogicalPlan::Window(window) => self.window(window),
            other => Ok(Transformed::no(other)),
        }
    }
}
