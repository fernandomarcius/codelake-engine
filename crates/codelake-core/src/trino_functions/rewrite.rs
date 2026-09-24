//! Expression rewrites applied before type coercion.
//!
//! - The SQL planner builds some functions directly instead of looking them
//!   up by name: `EXTRACT` (`date_part`), `POSITION` (`strpos`),
//!   `SUBSTRING(… FROM …)` (`substr`). Those calls are swapped for the Trino
//!   function of the same name.
//! - `if(condition, a [, b])` becomes `CASE WHEN condition THEN a ELSE b END`,
//!   so only the chosen branch is evaluated, as in Trino; `mod(n, m)` becomes
//!   `n % m`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{Case, Expr, ExprSchemable, Operator, ScalarUDF, binary_expr};

#[derive(Debug)]
pub(crate) struct TrinoRewrites {
    by_name: HashMap<String, Arc<ScalarUDF>>,
}

impl TrinoRewrites {
    /// `a op b` with a decimal operand as Trino's decimal function (see `decimal`).
    fn decimal_arithmetic(&self, expr: &Expr, schema: &DFSchema) -> Option<Expr> {
        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        let (left, right) = (
            binary.left.get_type(schema).ok()?,
            binary.right.get_type(schema).ok()?,
        );
        if !super::decimal::is_decimal_arithmetic(&left, binary.op, &right) {
            return None;
        }
        let function = self.by_name.get(super::decimal::function_name(binary.op)?)?;
        let args = vec![binary.left.as_ref().clone(), binary.right.as_ref().clone()];
        Some(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(function),
            args,
        )))
    }

    /// A float comparison the simplifier would decide with the engine's total
    /// order: IEEE instead.
    fn constant_float_comparison(&self, expr: &Expr, schema: &DFSchema) -> Option<Expr> {
        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        let name = super::nan::ieee_name(binary.op)?;
        let float = |e: &Expr| {
            e.get_type(schema)
                .is_ok_and(|t| matches!(t, DataType::Float32 | DataType::Float64))
        };
        let constant = |e: &Expr| e.column_refs().is_empty();
        // Constants would be folded with the engine's total order, and the
        // simplifier turns `x = x` into `x IS NOT NULL` (true for NaN).
        let folded = (constant(&binary.left) && constant(&binary.right)) || binary.left == binary.right;
        if !(float(&binary.left) && float(&binary.right) && folded) {
            return None;
        }
        let function = self.by_name.get(name)?;
        let args = vec![binary.left.as_ref().clone(), binary.right.as_ref().clone()];
        Some(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(function),
            args,
        )))
    }

    /// Integer `+ - *` of constants: the simplifier folds them with wrapping
    /// arithmetic, so they go through a checked function (column arithmetic
    /// is checked at run time, see `try_expr`).
    fn constant_integer_arithmetic(&self, expr: &Expr, schema: &DFSchema) -> Option<Expr> {
        let Expr::BinaryExpr(binary) = expr else {
            return None;
        };
        let name = super::decimal::checked_integer_name(binary.op)?;
        let integer = |e: &Expr| e.get_type(schema).is_ok_and(|t| super::support::is_integer(&t));
        let constant = |e: &Expr| e.column_refs().is_empty();
        let applies = integer(&binary.left)
            && integer(&binary.right)
            && constant(&binary.left)
            && constant(&binary.right);
        if !applies {
            return None;
        }
        let function = self.by_name.get(name)?;
        let args = vec![binary.left.as_ref().clone(), binary.right.as_ref().clone()];
        Some(Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::clone(function),
            args,
        )))
    }

    pub(crate) fn new(functions: Vec<Arc<ScalarUDF>>) -> Self {
        let by_name = functions.into_iter().map(|f| (f.name().to_owned(), f)).collect();
        Self { by_name }
    }
}

/// `CAST(literal AS T)` as a literal of type `T`, for the shapes the literal
/// rewrite produces (`CAST(1 AS integer)`, `CAST('1.5' AS decimal(2,1))`,
/// `CAST('1.5E0' AS double)`): functions that need a constant argument see it
/// before type coercion.
fn fold_typed_literal(expr: &Expr) -> Option<Expr> {
    let Expr::Cast(cast) = expr else { return None };
    let Expr::Literal(value, _) = cast.expr.as_ref() else {
        return None;
    };
    let unsigned = |text: &str| text.strip_prefix(['+', '-']).unwrap_or(text).to_owned();
    let foldable = match (value, &cast.data_type) {
        (ScalarValue::Int64(Some(_)), DataType::Int8 | DataType::Int16 | DataType::Int32) => true,
        // Exact decimals only (rounding is Trino's cast's job).
        (ScalarValue::Utf8(Some(text)), DataType::Decimal128(_, scale)) => {
            let digits = unsigned(text);
            let (int_part, fraction) = digits.split_once('.').unwrap_or((&digits, ""));
            !digits.is_empty()
                && int_part
                    .bytes()
                    .chain(fraction.bytes())
                    .all(|b| b.is_ascii_digit())
                && i8::try_from(fraction.len()).is_ok_and(|f| f <= *scale)
        }
        (ScalarValue::Utf8(Some(text)), DataType::Float64) => {
            let digits = unsigned(text);
            !digits.is_empty()
                && digits
                    .bytes()
                    .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'))
        }
        _ => false,
    };
    if !foldable {
        return None;
    }
    value
        .cast_to(&cast.data_type)
        .ok()
        .map(|v| Expr::Literal(v, None))
}

/// `timestamp with time zone ± INTERVAL 'n' DAY`: a day-to-second interval is
/// an exact duration in Trino (24 hours per day, across DST changes); the
/// engine adds calendar days.
fn exact_day_interval(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if !matches!(binary.op, Operator::Plus | Operator::Minus)
        || !matches!(
            binary.left.get_type(schema).ok()?,
            DataType::Timestamp(_, Some(_))
        )
    {
        return None;
    }
    let Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(step)), _) = binary.right.as_ref() else {
        return None;
    };
    if step.months != 0 || step.days == 0 {
        return None;
    }
    let nanos = i64::from(step.days)
        .checked_mul(86_400_000_000_000)?
        .checked_add(step.nanoseconds)?;
    let exact = datafusion::arrow::datatypes::IntervalMonthDayNano::new(0, 0, nanos);
    Some(binary_expr(
        binary.left.as_ref().clone(),
        binary.op,
        Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(exact)), None),
    ))
}

fn if_to_case(mut args: Vec<Expr>) -> Option<Expr> {
    if !(2..=3).contains(&args.len()) {
        return None;
    }
    let otherwise = if args.len() == 3 {
        args.pop()?
    } else {
        Expr::Literal(ScalarValue::Null, None)
    };
    let then = args.pop()?;
    let condition = args.pop()?;
    Some(Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(Box::new(condition), Box::new(then))],
        else_expr: Some(Box::new(otherwise)),
    }))
}

impl FunctionRewrite for TrinoRewrites {
    fn name(&self) -> &'static str {
        "trino_functions"
    }

    fn rewrite(&self, expr: Expr, schema: &DFSchema, _config: &ConfigOptions) -> Result<Transformed<Expr>> {
        if let Expr::BinaryExpr(binary) = &expr
            && matches!(binary.op, Operator::Plus | Operator::Minus)
            && let Ok(DataType::Date32) = binary.left.get_type(schema)
            && let Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(step)), _) = binary.right.as_ref()
            && step.nanoseconds != 0
        {
            // The engine would truncate to a date; Trino refuses.
            return Err(DataFusionError::Plan(
                "INVALID_FUNCTION_ARGUMENT: Cannot add hour, minutes or seconds to a date".into(),
            ));
        }
        if let Some(literal) = fold_typed_literal(&expr) {
            return Ok(Transformed::yes(literal));
        }
        if let Some(exact) = exact_day_interval(&expr, schema) {
            return Ok(Transformed::yes(exact));
        }
        if let Some(compare) = self.constant_float_comparison(&expr, schema) {
            return Ok(Transformed::yes(compare));
        }
        if let Some(decimal) = self.decimal_arithmetic(&expr, schema) {
            return Ok(Transformed::yes(decimal));
        }
        if let Some(checked) = self.constant_integer_arithmetic(&expr, schema) {
            return Ok(Transformed::yes(checked));
        }
        let Expr::ScalarFunction(function) = expr else {
            return Ok(Transformed::no(expr));
        };
        if function.name() == "if"
            && let Some(case) = if_to_case(function.args.clone())
        {
            return Ok(Transformed::yes(case));
        }
        if function.name() == super::try_expr::TRY
            && let (Some(guarded), Some(barrier)) = (
                self.by_name.get(super::try_expr::GUARDED_TRY),
                self.by_name.get(super::try_expr::BARRIER),
            )
        {
            // Constants inside `try` stay unevaluated until run time.
            let args = function
                .args
                .into_iter()
                .map(|arg| {
                    // Columns too: common-subexpression elimination must not
                    // lift part of the argument out of `try`.
                    arg.transform_up(|e| match e {
                        Expr::Literal(..) | Expr::Column(_) => Ok(Transformed::yes(Expr::ScalarFunction(
                            ScalarFunction::new_udf(Arc::clone(barrier), vec![e]),
                        ))),
                        other => Ok(Transformed::no(other)),
                    })
                    .map(|t| t.data)
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::clone(guarded),
                args,
            ))));
        }
        if function.name() == "mod"
            && let [dividend, divisor] = function.args.as_slice()
        {
            let remainder = binary_expr(dividend.clone(), Operator::Modulo, divisor.clone());
            // A decimal `%` goes through the decimal rewrite too.
            return Ok(Transformed::yes(
                self.decimal_arithmetic(&remainder, schema).unwrap_or(remainder),
            ));
        }
        match self.by_name.get(function.name()) {
            Some(trino) if function.func.as_ref() != trino.as_ref() => Ok(Transformed::yes(
                Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(trino), function.args)),
            )),
            _ => Ok(Transformed::no(Expr::ScalarFunction(function))),
        }
    }
}
