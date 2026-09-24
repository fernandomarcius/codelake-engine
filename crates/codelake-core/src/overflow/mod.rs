//! Exact-number arithmetic that fails instead of returning a wrong number.
//!
//! `DataFusion` wraps integer `+ - *`, unary minus and `sum` around on
//! overflow, lets decimal results exceed their precision and truncates when a
//! floating point or decimal value is cast to an integer. Trino 431 fails with
//! `NUMERIC_VALUE_OUT_OF_RANGE` and rounds half away from zero. `DataFusion` 53
//! has no switch for this (`enable_ansi_mode` only affects the Spark functions
//! and `BinaryExpr::fail_on_overflow` is never set by the planner), so the
//! engine rewrites the plan:
//!
//! - [`CheckedArithmetic`] (analyzer, after type coercion) replaces integer and
//!   decimal `+ - * / %` and integer negation with the checked functions of
//!   [`arith`], integer `abs` with a checked `abs`, `sum` over exact numbers
//!   with [`sum::CheckedSum`], and makes casts from floating point or decimal
//!   to an integer round first. Constant folding then fails to fold an
//!   overflowing constant and the error surfaces when the query runs.
//! - Sub-plans sent to a SQL source are rendered back to SQL, which does not
//!   know the checked functions: [`UncheckBeforeFederation`] turns them back
//!   into operators right before federation and [`CheckAfterFederation`]
//!   re-applies them to what stays in the engine. The source evaluates its own
//!   arithmetic (SQL Server, PostgreSQL and MySQL all fail on overflow).
//!   [`check_local_node`] does the same for nodes the federation pushdown
//!   policy keeps in the engine.
//!
//! Every overflow is an `ArrowError::ArithmeticOverflow` carrying Trino's
//! message; the Trino protocol maps it to `NUMERIC_VALUE_OUT_OF_RANGE`.

mod arith;
mod sum;

use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    AggregateUDF, BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF, TryCast,
    WindowFunctionDefinition,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};

use arith::{CheckedAbs, CheckedBinary, CheckedNegate, is_arithmetic, is_decimal, is_integer};
use sum::{CheckedSum, is_checked_sum_type};

static CHECKED_OPS: LazyLock<[(Operator, Arc<ScalarUDF>); 5]> = LazyLock::new(|| {
    [
        Operator::Plus,
        Operator::Minus,
        Operator::Multiply,
        Operator::Divide,
        Operator::Modulo,
    ]
    .map(|op| (op, Arc::new(ScalarUDF::new_from_impl(CheckedBinary::new(op)))))
});
static CHECKED_NEGATE: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(CheckedNegate::new())));
static CHECKED_ABS: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(CheckedAbs::new())));
static CHECKED_SUM: LazyLock<Arc<AggregateUDF>> =
    LazyLock::new(|| Arc::new(AggregateUDF::new_from_impl(CheckedSum::default())));

fn checked_op(op: Operator) -> Option<&'static Arc<ScalarUDF>> {
    CHECKED_OPS.iter().find(|(o, _)| *o == op).map(|(_, udf)| udf)
}

/// Whether `a <op> b` with these operand types is evaluated by [`arith`].
fn needs_check(op: Operator, left: &DataType, right: &DataType) -> bool {
    is_arithmetic(op) && ((is_integer(left) && left == right) || (is_decimal(left) && is_decimal(right)))
}

/// Integer and decimal operators and integer negation → checked functions.
fn check_arithmetic(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if needs_check(op, &left.get_type(schema)?, &right.get_type(schema)?) =>
        {
            let Some(udf) = checked_op(op) else {
                return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })));
            };
            Transformed::yes(udf.call(vec![*left, *right]))
        }
        Expr::Negative(inner) if inner.get_type(schema)?.is_signed_integer() => {
            Transformed::yes(CHECKED_NEGATE.call(vec![*inner]))
        }
        other => Transformed::no(other),
    })
}

fn is_builtin<T: 'static>(udf: &AggregateUDF) -> bool {
    udf.inner().as_any().is::<T>()
}

fn sums_exact_numbers(func: &AggregateUDF, args: &[Expr], schema: &DFSchema) -> Result<bool> {
    if !is_builtin::<Sum>(func) {
        return Ok(false);
    }
    let [arg] = args else {
        return Ok(false);
    };
    Ok(func
        .return_type(&[arg.get_type(schema)?])
        .is_ok_and(|t| is_checked_sum_type(&t)))
}

fn is_round(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(f) if f.name() == "round" && f.args.len() == 1)
}

/// A cast to an integer rounds half away from zero, like Trino, instead of
/// truncating: floating point through `round`, decimals through a cast to
/// scale 0 (Arrow rounds half away from zero when it reduces a decimal's
/// scale). One more integral digit holds the carry (9.5 → 10).
fn rounded(source: &Expr, from: &DataType, to: &DataType) -> Option<Expr> {
    if !to.is_integer() {
        return None;
    }
    let to_scale_0 = |t: DataType| Some(Expr::Cast(Cast::new(Box::new(source.clone()), t)));
    let integral = |p: u8, s: i8| p - u8::try_from(s).unwrap_or(0) + 1;
    match *from {
        DataType::Float16 | DataType::Float32 | DataType::Float64 if !is_round(source) => {
            Some(datafusion::functions::math::round().call(vec![source.clone()]))
        }
        DataType::Decimal32(p, s) if s > 0 => to_scale_0(DataType::Decimal32(integral(p, s), 0)),
        DataType::Decimal64(p, s) if s > 0 => to_scale_0(DataType::Decimal64(integral(p, s), 0)),
        DataType::Decimal128(p, s) if s > 0 => to_scale_0(DataType::Decimal128(integral(p, s), 0)),
        DataType::Decimal256(p, s) if s > 0 => to_scale_0(DataType::Decimal256(integral(p, s), 0)),
        _ => None,
    }
}

/// Everything [`CheckedArithmetic`] rewrites in one expression node.
fn check_node(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(_) | Expr::Negative(_) => check_arithmetic(expr, schema),
        Expr::ScalarFunction(ScalarFunction { func, args })
            if func.name() == "abs"
                && !func.inner().as_any().is::<CheckedAbs>()
                && args.len() == 1
                && args[0].get_type(schema)?.is_integer() =>
        {
            Ok(Transformed::yes(CHECKED_ABS.call(args)))
        }
        Expr::AggregateFunction(AggregateFunction { func, params })
            if sums_exact_numbers(&func, &params.args, schema)? =>
        {
            Ok(Transformed::yes(Expr::AggregateFunction(AggregateFunction {
                func: Arc::clone(&CHECKED_SUM),
                params,
            })))
        }
        Expr::WindowFunction(mut window) => {
            let is_sum = match &window.fun {
                WindowFunctionDefinition::AggregateUDF(func) => {
                    sums_exact_numbers(func, &window.params.args, schema)?
                }
                WindowFunctionDefinition::WindowUDF(_) => false,
            };
            if !is_sum {
                return Ok(Transformed::no(Expr::WindowFunction(window)));
            }
            let WindowFunction { fun, .. } = window.as_mut();
            *fun = WindowFunctionDefinition::AggregateUDF(Arc::clone(&CHECKED_SUM));
            Ok(Transformed::yes(Expr::WindowFunction(window)))
        }
        Expr::Cast(Cast { expr, data_type }) => match rounded(&expr, &expr.get_type(schema)?, &data_type) {
            Some(source) => Ok(Transformed::yes(Expr::Cast(Cast::new(
                Box::new(source),
                data_type,
            )))),
            None => Ok(Transformed::no(Expr::Cast(Cast { expr, data_type }))),
        },
        Expr::TryCast(TryCast { expr, data_type }) => {
            match rounded(&expr, &expr.get_type(schema)?, &data_type) {
                Some(source) => Ok(Transformed::yes(Expr::TryCast(TryCast::new(
                    Box::new(source),
                    data_type,
                )))),
                None => Ok(Transformed::no(Expr::TryCast(TryCast { expr, data_type }))),
            }
        }
        other => Ok(Transformed::no(other)),
    }
}

/// Checked functions → the operators they replace (for SQL sources).
fn uncheck_node(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, mut args }) = expr else {
        return Transformed::no(expr);
    };
    if let Some(binary) = func.inner().as_any().downcast_ref::<CheckedBinary>()
        && args.len() == 2
        && let (Some(right), Some(left)) = (args.pop(), args.pop())
    {
        return Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            binary.op(),
            Box::new(right),
        )));
    }
    if func.inner().as_any().is::<CheckedNegate>()
        && args.len() == 1
        && let Some(inner) = args.pop()
    {
        return Transformed::yes(Expr::Negative(Box::new(inner)));
    }
    Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args }))
}

/// Schema that resolves the columns of `plan`'s expressions.
fn expression_schema(plan: &LogicalPlan) -> Result<DFSchema> {
    let mut schema = merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = plan {
        let source = DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())?;
        schema.merge(&source);
    }
    Ok(schema)
}

/// Rewrites the expressions of one plan node, keeping its column names.
fn rewrite_node(
    plan: LogicalPlan,
    mut f: impl FnMut(Expr, &DFSchema) -> Result<Transformed<Expr>>,
) -> Result<Transformed<LogicalPlan>> {
    let schema = expression_schema(&plan)?;
    let names = NamePreserver::new(&plan);
    let rewritten = plan.map_expressions(|expr| {
        let name = names.save(&expr);
        expr.transform_up(|e| f(e, &schema))
            .map(|t| t.update_data(|e| name.restore(e)))
    })?;
    if rewritten.transformed {
        rewritten.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(rewritten)
    }
}

/// Re-checks the arithmetic of a node that runs in the engine after
/// federation (see the module documentation).
pub fn check_local_node(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(rewrite_node(plan, check_arithmetic)?.data)
}

/// Analyzer rule: see the module documentation.
#[derive(Debug, Default)]
pub struct CheckedArithmetic;

impl AnalyzerRule for CheckedArithmetic {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|node| rewrite_node(node, check_node))?
            .data)
    }

    fn name(&self) -> &'static str {
        "codelake_checked_arithmetic"
    }
}

/// Optimizer rule placed right before federation: checked functions become
/// operators again so a federated sub-plan renders as plain SQL.
#[derive(Debug, Default)]
pub struct UncheckBeforeFederation;

impl OptimizerRule for UncheckBeforeFederation {
    fn name(&self) -> &'static str {
        "codelake_uncheck_before_federation"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        rewrite_node(plan, |expr, _| Ok(uncheck_node(expr)))
    }
}

/// Optimizer rule placed right after federation: what stays in the engine is
/// checked again. Federated sub-plans are opaque leaves and keep operators.
#[derive(Debug, Default)]
pub struct CheckAfterFederation;

impl OptimizerRule for CheckAfterFederation {
    fn name(&self) -> &'static str {
        "codelake_check_after_federation"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        rewrite_node(plan, check_arithmetic)
    }
}

#[cfg(test)]
mod tests;
