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
//!   `arith`, integer `abs` with a checked `abs`, `sum` over exact numbers
//!   with `sum::CheckedSum`, and makes casts from floating point or decimal
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
//! Decimals also take Trino's types and rounding (modules `decimal`, `avg`):
//! [`TrinoDecimalTypes`] (analyzer, *before* type coercion, so every node
//! above sees the new types) replaces decimal `+ - * / %` with
//! `decimal::DecimalBinary`, `avg` with `avg::TrinoAvg` and `sum` with a
//! `sum::CheckedSum` typed `decimal(38,s)`. Where the `DataFusion` type of a
//! decimal operator differs from Trino's, the operator sent to a source is
//! wrapped in a cast to Trino's type, so the plan's types never change.
//!
//! Every overflow is an `ArrowError::ArithmeticOverflow` carrying Trino's
//! message; the Trino protocol maps it to `NUMERIC_VALUE_OUT_OF_RANGE`.

mod arith;
mod avg;
pub(crate) mod decimal;
mod sum;

use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    AggregateUDF, BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF, ScalarUDFImpl,
    TryCast, WindowFunctionDefinition,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};

use arith::{CheckedAbs, CheckedBinary, CheckedNegate, is_arithmetic, is_decimal, is_integer};
use avg::TrinoAvg;
use decimal::{DecimalBinary, as_decimal, is_decimal_operation};
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
static TRINO_SUM: LazyLock<Arc<AggregateUDF>> =
    LazyLock::new(|| Arc::new(AggregateUDF::new_from_impl(CheckedSum::trino())));
static TRINO_AVG: LazyLock<Arc<AggregateUDF>> =
    LazyLock::new(|| Arc::new(AggregateUDF::new_from_impl(TrinoAvg::default())));
static DECIMAL_OPS: LazyLock<[(Operator, Arc<ScalarUDF>); 5]> = LazyLock::new(|| {
    [
        Operator::Plus,
        Operator::Minus,
        Operator::Multiply,
        Operator::Divide,
        Operator::Modulo,
    ]
    .map(|op| (op, Arc::new(ScalarUDF::new_from_impl(DecimalBinary::new(op)))))
});

fn decimal_op(op: Operator) -> Option<&'static Arc<ScalarUDF>> {
    DECIMAL_OPS.iter().find(|(o, _)| *o == op).map(|(_, udf)| udf)
}

fn is_decimal_binary(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(f) if f.func.inner().as_any().is::<DecimalBinary>())
}

fn checked_op(op: Operator) -> Option<&'static Arc<ScalarUDF>> {
    CHECKED_OPS.iter().find(|(o, _)| *o == op).map(|(_, udf)| udf)
}

/// Whether `a <op> b` with these operand types is evaluated by `arith`.
fn needs_check(op: Operator, left: &DataType, right: &DataType) -> bool {
    is_arithmetic(op) && ((is_integer(left) && left == right) || (is_decimal(left) && is_decimal(right)))
}

/// A coerced decimal operator → Trino's decimal function. Where `DataFusion`'s
/// type differs from Trino's, the result is cast back to `DataFusion`'s type,
/// so the plan does not change; [`collapse_decimal_cast`] then drops the pair
/// of casts that `unchecked_decimal` leaves around it.
fn check_decimal_operator(
    left: Expr,
    op: Operator,
    right: Expr,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let (left_type, right_type) = (left.get_type(schema)?, right.get_type(schema)?);
    let (Some(udf), Some(l), Some(r)) = (decimal_op(op), as_decimal(&left_type), as_decimal(&right_type))
    else {
        // More than 38 digits: checked with `DataFusion`'s types.
        return Ok(match checked_op(op) {
            Some(udf) => Transformed::yes(udf.call(vec![left, right])),
            None => Transformed::no(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(left),
                op,
                Box::new(right),
            ))),
        });
    };
    let native = BinaryTypeCoercer::new(&left_type, &op, &right_type).get_result_type()?;
    let trino = decimal::result_type(op, l, r)?;
    let call = udf.call(vec![left, right]);
    Ok(Transformed::yes(if native == trino {
        call
    } else {
        Expr::Cast(Cast::new(Box::new(call), native))
    }))
}

/// `CAST(CAST(<decimal function> AS a) AS b)` where `b` is the function's own
/// type → the function: the inner cast could only lose digits.
fn collapse_decimal_cast(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::Cast(Cast {
        expr: outer,
        data_type,
    }) = expr
    else {
        return Ok(Transformed::no(expr));
    };
    if let Expr::Cast(Cast { expr: inner, .. }) = outer.as_ref()
        && is_decimal_binary(inner)
        && inner.get_type(schema)? == data_type
    {
        return Ok(Transformed::yes(inner.as_ref().clone()));
    }
    Ok(Transformed::no(Expr::Cast(Cast::new(outer, data_type))))
}

/// Integer and decimal operators and integer negation → checked functions.
fn check_arithmetic(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if needs_check(op, &left.get_type(schema)?, &right.get_type(schema)?) =>
        {
            if is_decimal(&left.get_type(schema)?) {
                return check_decimal_operator(*left, op, *right, schema);
            }
            let Some(udf) = checked_op(op) else {
                return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })));
            };
            Transformed::yes(udf.call(vec![*left, *right]))
        }
        Expr::Negative(inner) if inner.get_type(schema)?.is_signed_integer() => {
            Transformed::yes(CHECKED_NEGATE.call(vec![*inner]))
        }
        Expr::Cast(_) => {
            let collapsed = collapse_decimal_cast(expr, schema)?;
            if collapsed.transformed {
                return Ok(collapsed);
            }
            return crate::timestamps::check_text_cast(collapsed.data, schema);
        }
        Expr::TryCast(_) | Expr::ScalarFunction(_) => {
            return crate::timestamps::check_text_cast(expr, schema);
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
        Expr::Cast(_) => {
            let collapsed = collapse_decimal_cast(expr, schema)?;
            if collapsed.transformed {
                return Ok(collapsed);
            }
            let text = crate::timestamps::check_text_cast(collapsed.data, schema)?;
            if text.transformed {
                return Ok(text);
            }
            let Expr::Cast(Cast { expr, data_type }) = text.data else {
                return Ok(text);
            };
            match rounded(&expr, &expr.get_type(schema)?, &data_type) {
                Some(source) => Ok(Transformed::yes(Expr::Cast(Cast::new(
                    Box::new(source),
                    data_type,
                )))),
                None => Ok(Transformed::no(Expr::Cast(Cast { expr, data_type }))),
            }
        }
        Expr::TryCast(TryCast { expr, data_type }) if !matches!(data_type, DataType::Timestamp(..)) => {
            match rounded(&expr, &expr.get_type(schema)?, &data_type) {
                Some(source) => Ok(Transformed::yes(Expr::TryCast(TryCast::new(
                    Box::new(source),
                    data_type,
                )))),
                None => Ok(Transformed::no(Expr::TryCast(TryCast { expr, data_type }))),
            }
        }
        Expr::TryCast(_) | Expr::ScalarFunction(_) => crate::timestamps::check_text_cast(expr, schema),
        other => Ok(Transformed::no(other)),
    }
}

/// The operator a decimal function replaces, as `DataFusion` coerces it
/// (integer operands cast to their decimal type), cast to Trino's type when
/// `DataFusion` would type it differently.
fn unchecked_decimal(binary: &DecimalBinary, left: Expr, right: Expr, schema: &DFSchema) -> Result<Expr> {
    let op = binary.op();
    let (left_type, right_type) = (left.get_type(schema)?, right.get_type(schema)?);
    let decimal = |expr: Expr, data_type: &DataType| -> Result<(Expr, DataType)> {
        if is_decimal(data_type) {
            return Ok((expr, data_type.clone()));
        }
        let Some((p, s)) = as_decimal(data_type) else {
            return datafusion::common::plan_err!("{data_type} is not an exact number");
        };
        let target = DataType::Decimal128(p, s);
        Ok((Expr::Cast(Cast::new(Box::new(expr), target.clone())), target))
    };
    let (left, left_type2) = decimal(left, &left_type)?;
    let (right, right_type2) = decimal(right, &right_type)?;
    let native = BinaryTypeCoercer::new(&left_type2, &op, &right_type2).get_result_type()?;
    let trino = ScalarUDFImpl::return_type(binary, &[left_type, right_type])?;
    let operator = Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)));
    Ok(if native == trino {
        operator
    } else {
        Expr::Cast(Cast::new(Box::new(operator), trino))
    })
}

/// Checked functions → the operators they replace (for SQL sources).
fn uncheck_node(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let expr = match crate::timestamps::uncheck_text_cast(expr) {
        done @ Transformed {
            transformed: true, ..
        } => return Ok(done),
        other => other.data,
    };
    let Expr::ScalarFunction(ScalarFunction { func, mut args }) = expr else {
        return Ok(Transformed::no(expr));
    };
    if let Some(binary) = func.inner().as_any().downcast_ref::<DecimalBinary>()
        && args.len() == 2
        && let (Some(right), Some(left)) = (args.pop(), args.pop())
    {
        return Ok(Transformed::yes(unchecked_decimal(binary, left, right, schema)?));
    }
    if let Some(binary) = func.inner().as_any().downcast_ref::<CheckedBinary>()
        && args.len() == 2
        && let (Some(right), Some(left)) = (args.pop(), args.pop())
    {
        return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            binary.op(),
            Box::new(right),
        ))));
    }
    if func.inner().as_any().is::<CheckedNegate>()
        && args.len() == 1
        && let Some(inner) = args.pop()
    {
        return Ok(Transformed::yes(Expr::Negative(Box::new(inner))));
    }
    Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
        func,
        args,
    })))
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
        rewrite_node(plan, uncheck_node)
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

fn is_decimal128_argument(args: &[Expr], schema: &DFSchema) -> bool {
    args.first()
        .and_then(|a| a.get_type(schema).ok())
        .is_some_and(|t| matches!(t, DataType::Decimal32(..) | DataType::Decimal64(..)))
}

/// `decimal32`/`decimal64` → `decimal128` of the same `(p,s)`, the type
/// Trino's `sum` works with.
fn widen_small_decimal(args: Vec<Expr>, schema: &DFSchema) -> Vec<Expr> {
    args.into_iter()
        .map(|arg| match arg.get_type(schema) {
            Ok(DataType::Decimal32(p, s) | DataType::Decimal64(p, s)) => {
                Expr::Cast(Cast::new(Box::new(arg), DataType::Decimal128(p, s)))
            }
            _ => arg,
        })
        .collect()
}

/// The Trino replacement of a built-in `sum` or `avg`, with its arguments.
fn trino_aggregate(
    func: &AggregateUDF,
    args: Vec<Expr>,
    schema: &DFSchema,
) -> Option<(Arc<AggregateUDF>, Vec<Expr>)> {
    if is_builtin::<Avg>(func) {
        return Some((Arc::clone(&TRINO_AVG), args));
    }
    if is_builtin::<Sum>(func) {
        let args = if is_decimal128_argument(&args, schema) {
            widen_small_decimal(args, schema)
        } else {
            args
        };
        return Some((Arc::clone(&TRINO_SUM), args));
    }
    None
}

/// Trino types an integer literal `integer` when it fits 32 bits (`bigint`
/// otherwise), which decides its decimal type (`decimal(10,0)`, not
/// `decimal(19,0)`); `DataFusion` reads every integer literal as `bigint`.
fn integer_literal(expr: Expr) -> Expr {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), metadata) if i32::try_from(v).is_ok() => {
            Expr::Literal(ScalarValue::Int32(i32::try_from(v).ok()), metadata)
        }
        other => other,
    }
}

/// Everything [`TrinoDecimalTypes`] rewrites in one expression node.
fn trino_decimal_node(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let is_decimal = match (left.get_type(schema), right.get_type(schema)) {
                (Ok(l), Ok(r)) => is_decimal_operation(op, &l, &r),
                _ => false,
            };
            match decimal_op(op) {
                Some(udf) if is_decimal => {
                    Transformed::yes(udf.call(vec![integer_literal(*left), integer_literal(*right)]))
                }
                _ => Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })),
            }
        }
        Expr::AggregateFunction(AggregateFunction { func, mut params }) => {
            let args = std::mem::take(&mut params.args);
            if let Some((func, args)) = trino_aggregate(&func, args.clone(), schema) {
                params.args = args;
                return Transformed::yes(Expr::AggregateFunction(AggregateFunction { func, params }));
            }
            params.args = args;
            Transformed::no(Expr::AggregateFunction(AggregateFunction { func, params }))
        }
        Expr::WindowFunction(mut window) => {
            let WindowFunctionDefinition::AggregateUDF(func) = &window.fun else {
                return Transformed::no(Expr::WindowFunction(window));
            };
            let func = Arc::clone(func);
            let args = std::mem::take(&mut window.params.args);
            if let Some((replacement, args)) = trino_aggregate(&func, args.clone(), schema) {
                window.fun = WindowFunctionDefinition::AggregateUDF(replacement);
                window.params.args = args;
                return Transformed::yes(Expr::WindowFunction(window));
            }
            window.params.args = args;
            Transformed::no(Expr::WindowFunction(window))
        }
        other => Transformed::no(other),
    }
}

/// Analyzer rule placed right before type coercion: decimal operators,
/// `sum` and `avg` take Trino's types (see the module documentation). Type
/// coercion then recomputes every node's schema from them.
#[derive(Debug, Default)]
pub struct TrinoDecimalTypes;

impl AnalyzerRule for TrinoDecimalTypes {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|node| {
                let schema = expression_schema(&node)?;
                let names = NamePreserver::new(&node);
                node.map_expressions(|expr| {
                    let name = names.save(&expr);
                    expr.transform_up(|e| Ok(trino_decimal_node(e, &schema)))
                        .map(|t| t.update_data(|e| name.restore(e)))
                })
            })?
            .data)
    }

    fn name(&self) -> &'static str {
        "codelake_trino_decimal_types"
    }
}

#[cfg(test)]
mod tests;
