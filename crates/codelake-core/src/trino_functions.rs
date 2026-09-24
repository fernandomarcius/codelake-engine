//! Functions whose result type (or value) differs between `DataFusion` and
//! Trino 431, replaced in the plan by [`crate::coercion::TrinoCommonTypes`]:
//!
//! - `round(x, n)` of a decimal keeps its scale and gains one digit of
//!   precision (`round(CAST(9.99 AS DECIMAL(3,2)), 1)` is `decimal(4,2)`
//!   `10.00`), of an integer keeps the integer type; `DataFusion` answers a
//!   decimal of scale `n`, or a `double` ([`TrinoRound`]);
//! - `row_number()`, `rank()`, `dense_rank()` and `ntile(n)` are `bigint`;
//!   `DataFusion`'s are unsigned, sent as `decimal(20,0)` ([`BigintWindow`]);
//! - `extract(<field> FROM x)` (`date_part`) is `bigint`; `DataFusion`'s is
//!   `integer` ([`BigintDatePart`]);
//! - `to_unixtime(x)` is a `double` with the fraction of a second;
//!   `DataFusion`'s a `bigint` of whole seconds ([`ToUnixtime`]).
//!
//! The replacements keep the functions' names, so a sub-plan sent to a SQL
//! source renders the same SQL; `round` goes back to the built-in there,
//! cast to Trino's type ([`uncheck_round`]).

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::{ScalarFunction, WindowFunction};
use datafusion::logical_expr::function::{ExpressionArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::window_state::WindowAggState;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LimitEffect, PartitionEvaluator, ReturnFieldArgs, ReversedUDWF,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, WindowFunctionDefinition, WindowUDF,
    WindowUDFImpl,
};
use datafusion::physical_expr::PhysicalExpr;

use crate::overflow::decimal::{MAX_PRECISION, as_decimal};

// ---------------------------------------------------------------- round

/// Trino's type of `round(x, n)`, `None` when `x` is not a decimal or an
/// integer.
fn round_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Some(data_type.clone()),
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) => {
            let (p, s) = as_decimal(data_type)?;
            Some(DataType::Decimal128((p + 1).min(MAX_PRECISION), s))
        }
        _ => None,
    }
}

fn overflow() -> ArrowError {
    ArrowError::ArithmeticOverflow("Numeric value out of range".into())
}

/// `value` (scaled by `10^scale`) rounded half away from zero to `digits`
/// fraction digits, still scaled by `10^scale`.
fn round_scaled(value: i128, scale: i64, digits: i64) -> Result<i128, ArrowError> {
    if digits >= scale {
        return Ok(value);
    }
    let Some(factor) = u32::try_from(scale - digits)
        .ok()
        .and_then(|e| 10_i128.checked_pow(e))
    else {
        return Ok(0);
    };
    let (quotient, remainder) = (value / factor, value % factor);
    let rounded = if remainder.unsigned_abs() * 2 >= factor.unsigned_abs() {
        quotient + value.signum()
    } else {
        quotient
    };
    rounded.checked_mul(factor).ok_or_else(overflow)
}

/// `round(x, n)` with Trino's types (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct TrinoRound {
    signature: Signature,
}

impl TrinoRound {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

fn integers<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    digits: &PrimitiveArray<Int64Type>,
    to_i128: impl Fn(T::Native) -> i128,
    from_i128: impl Fn(i128) -> Option<T::Native>,
) -> Result<ArrayRef> {
    let values = array.as_primitive::<T>();
    let mut out = Vec::with_capacity(values.len());
    for (value, n) in values.iter().zip(digits.iter()) {
        out.push(match (value, n) {
            (Some(v), Some(n)) => Some(from_i128(round_scaled(to_i128(v), 0, n)?).ok_or_else(overflow)?),
            _ => None,
        });
    }
    Ok(Arc::new(out.into_iter().collect::<PrimitiveArray<T>>()))
}

impl ScalarUDFImpl for TrinoRound {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(t) = arg_types.first().and_then(round_type) else {
            return exec_err!("round: {arg_types:?} is not a decimal or an integer");
        };
        Ok(t)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, digits] = args.args.as_slice() else {
            return exec_err!("round expects two arguments");
        };
        let rows = args.number_rows;
        let scalar = matches!(
            (value, digits),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let array = value.to_array(rows)?;
        let digits = cast(&digits.to_array(rows)?, &DataType::Int64)?;
        let digits = digits.as_primitive::<Int64Type>();
        let result: ArrayRef = match array.data_type() {
            DataType::Int8 => integers::<Int8Type>(&array, digits, i128::from, |v| i8::try_from(v).ok())?,
            DataType::Int16 => integers::<Int16Type>(&array, digits, i128::from, |v| i16::try_from(v).ok())?,
            DataType::Int32 => integers::<Int32Type>(&array, digits, i128::from, |v| i32::try_from(v).ok())?,
            DataType::Int64 => integers::<Int64Type>(&array, digits, i128::from, |v| i64::try_from(v).ok())?,
            source => {
                let Some(DataType::Decimal128(p, s)) = round_type(source) else {
                    return exec_err!("round: {source} is not a decimal or an integer");
                };
                let values = cast(&array, &DataType::Decimal128(p, s))?;
                let limit = 10_i128.pow(u32::from(p));
                let mut out = Vec::with_capacity(values.len());
                for (v, n) in values.as_primitive::<Decimal128Type>().iter().zip(digits.iter()) {
                    out.push(match (v, n) {
                        (Some(v), Some(n)) => {
                            let rounded = round_scaled(v, i64::from(s), n)?;
                            if rounded.unsigned_abs() >= limit.unsigned_abs() {
                                return Err(overflow().into());
                            }
                            Some(rounded)
                        }
                        _ => None,
                    });
                }
                Arc::new(PrimitiveArray::<Decimal128Type>::from(out).with_precision_and_scale(p, s)?)
            }
        };
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

fn is_builtin_round(func: &ScalarUDF) -> bool {
    func.name() == "round" && !func.inner().as_any().is::<TrinoRound>()
}

/// `round(<decimal or integer>, n)` → [`TrinoRound`].
fn check_round(func: Arc<ScalarUDF>, args: Vec<Expr>, schema: &DFSchema) -> Result<Transformed<Expr>> {
    if is_builtin_round(&func) && args.len() == 2 && round_type(&args[0].get_type(schema)?).is_some() {
        return Ok(Transformed::yes(
            ScalarUDF::new_from_impl(TrinoRound::new()).call(args),
        ));
    }
    Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
        func,
        args,
    })))
}

/// [`TrinoRound`] → `DataFusion`'s `round`, cast to Trino's type (for SQL
/// sources).
pub(crate) fn uncheck_round(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(Transformed::no(expr));
    };
    if !func.inner().as_any().is::<TrinoRound>() {
        return Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
            func,
            args,
        })));
    }
    let trino = func.return_type(
        &args
            .iter()
            .map(|a| a.get_type(schema))
            .collect::<Result<Vec<_>>>()?,
    )?;
    let builtin = datafusion::functions::math::round().call(args);
    Ok(Transformed::yes(Expr::Cast(Cast::new(Box::new(builtin), trino))))
}

// ---------------------------------------------------------------- windows

/// A window function answering `bigint` instead of its own integer type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct BigintWindow {
    inner: Arc<WindowUDF>,
}

fn to_bigint(array: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast(array, &DataType::Int64)?)
}

impl WindowUDFImpl for BigintWindow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn expressions(&self, expr_args: ExpressionArgs<'_>) -> Vec<Arc<dyn PhysicalExpr>> {
        self.inner.expressions(expr_args)
    }

    fn partition_evaluator(&self, args: PartitionEvaluatorArgs<'_>) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(BigintEvaluator(
            self.inner.partition_evaluator_factory(args)?,
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs<'_>) -> Result<FieldRef> {
        let field = self.inner.field(field_args)?;
        Ok(Arc::new(
            Field::new(field.name(), DataType::Int64, field.is_nullable())
                .with_metadata(field.metadata().clone()),
        ))
    }

    fn sort_options(&self) -> Option<datafusion::arrow::compute::SortOptions> {
        self.inner.sort_options()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        match self.inner.reverse_expr() {
            ReversedUDWF::Reversed(reversed) => ReversedUDWF::Reversed(Arc::new(bigint_window(reversed))),
            other => other,
        }
    }

    fn limit_effect(&self, args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        self.inner.inner().limit_effect(args)
    }
}

fn bigint_window(inner: Arc<WindowUDF>) -> WindowUDF {
    WindowUDF::new_from_impl(BigintWindow { inner })
}

/// The evaluator of a [`BigintWindow`]: the inner one, answers cast.
#[derive(Debug)]
struct BigintEvaluator(Box<dyn PartitionEvaluator>);

impl PartitionEvaluator for BigintEvaluator {
    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        self.0.memoize(state)
    }

    fn get_range(&self, idx: usize, n_rows: usize) -> Result<Range<usize>> {
        self.0.get_range(idx, n_rows)
    }

    fn is_causal(&self) -> bool {
        self.0.is_causal()
    }

    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        to_bigint(&self.0.evaluate_all(values, num_rows)?)
    }

    fn evaluate(&mut self, values: &[ArrayRef], range: &Range<usize>) -> Result<ScalarValue> {
        self.0.evaluate(values, range)?.cast_to(&DataType::Int64)
    }

    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        to_bigint(&self.0.evaluate_all_with_rank(num_rows, ranks_in_partition)?)
    }

    fn supports_bounded_execution(&self) -> bool {
        self.0.supports_bounded_execution()
    }

    fn uses_window_frame(&self) -> bool {
        self.0.uses_window_frame()
    }

    fn include_rank(&self) -> bool {
        self.0.include_rank()
    }
}

/// The ranking functions Trino types `bigint`.
fn is_ranking(udwf: &WindowUDF) -> bool {
    !udwf.inner().as_any().is::<BigintWindow>()
        && matches!(udwf.name(), "row_number" | "rank" | "dense_rank" | "ntile")
}

// ---------------------------------------------------------------- date_part

/// `date_part` (`extract`) answering `bigint` where `DataFusion` answers
/// `integer`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct BigintDatePart {
    inner: Arc<ScalarUDF>,
}

fn widened(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Int32 => DataType::Int64,
        other => other.clone(),
    }
}

impl ScalarUDFImpl for BigintDatePart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(widened(&self.inner.return_type(arg_types)?))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(
            field.as_ref().clone().with_data_type(widened(field.data_type())),
        ))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let bigint = args.return_field.data_type() == &DataType::Int64;
        if bigint {
            let field = args.return_field.as_ref().clone().with_data_type(DataType::Int32);
            args.return_field = Arc::new(field);
        }
        let result = self.inner.invoke_with_args(args)?;
        if !bigint || result.data_type() != DataType::Int32 {
            return Ok(result);
        }
        result.cast_to(&DataType::Int64, None)
    }
}

fn is_builtin_date_part(func: &ScalarUDF) -> bool {
    func.name() == "date_part" && !func.inner().as_any().is::<BigintDatePart>()
}

// ---------------------------------------------------------------- to_unixtime

/// `to_unixtime(<timestamp with time zone>)`: seconds since the epoch as a
/// `double`, with the fraction (`DataFusion`: whole seconds, `bigint`). A
/// timestamp without time zone is placed in the session zone first.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ToUnixtime {
    signature: Signature,
}

impl ScalarUDFImpl for ToUnixtime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "to_unixtime"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("to_unixtime expects one argument");
        };
        let DataType::Timestamp(unit, _) = value.data_type() else {
            return exec_err!("to_unixtime expects a timestamp, got {}", value.data_type());
        };
        let per_second: f64 = match unit {
            datafusion::arrow::datatypes::TimeUnit::Second => 1.0,
            datafusion::arrow::datatypes::TimeUnit::Millisecond => 1e3,
            datafusion::arrow::datatypes::TimeUnit::Microsecond => 1e6,
            datafusion::arrow::datatypes::TimeUnit::Nanosecond => 1e9,
        };
        let seconds = |array: &ArrayRef| -> Result<ArrayRef> {
            let raw = cast(&cast(array, &DataType::Int64)?, &DataType::Float64)?;
            let divisor = datafusion::arrow::array::Float64Array::new_scalar(per_second);
            Ok(datafusion::arrow::compute::kernels::numeric::div(&raw, &divisor)?)
        };
        Ok(match value {
            ColumnarValue::Array(array) => ColumnarValue::Array(seconds(array)?),
            ColumnarValue::Scalar(scalar) => {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&seconds(&scalar.to_array()?)?, 0)?)
            }
        })
    }
}

/// `to_unixtime(<timestamp>)` → [`ToUnixtime`], a timestamp without time zone
/// cast to the session zone.
fn check_to_unixtime(
    func: Arc<ScalarUDF>,
    args: Vec<Expr>,
    schema: &DFSchema,
    session_zone: &str,
) -> Result<Transformed<Expr>> {
    let [arg] = args.as_slice() else {
        return Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
            func,
            args,
        })));
    };
    let arg = match arg.get_type(schema)? {
        DataType::Timestamp(_, Some(_)) => arg.clone(),
        DataType::Timestamp(unit, None) => Expr::Cast(Cast::new(
            Box::new(arg.clone()),
            DataType::Timestamp(unit, Some(session_zone.into())),
        )),
        _ => {
            return Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
                func,
                args,
            })));
        }
    };
    let udf = ScalarUDF::new_from_impl(ToUnixtime {
        signature: Signature::any(1, Volatility::Immutable),
    });
    Ok(Transformed::yes(udf.call(vec![arg])))
}

fn is_builtin_to_unixtime(func: &ScalarUDF) -> bool {
    func.name() == "to_unixtime" && !func.inner().as_any().is::<ToUnixtime>()
}

/// Every function of one expression node that answers another type than in
/// Trino (see the module documentation).
pub(crate) fn trino_function(expr: Expr, schema: &DFSchema, session_zone: &str) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarFunction(ScalarFunction { func, args }) if is_builtin_to_unixtime(&func) => {
            check_to_unixtime(func, args, schema, session_zone)
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) if is_builtin_round(&func) => {
            check_round(func, args, schema)
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) if is_builtin_date_part(&func) => {
            let wrapped = ScalarUDF::new_from_impl(BigintDatePart { inner: func });
            Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                Arc::new(wrapped),
                args,
            ))))
        }
        Expr::WindowFunction(mut window) => {
            let WindowFunction { fun, .. } = window.as_mut();
            if let WindowFunctionDefinition::WindowUDF(udwf) = fun
                && is_ranking(udwf)
            {
                *fun = WindowFunctionDefinition::WindowUDF(Arc::new(bigint_window(Arc::clone(udwf))));
                return Ok(Transformed::yes(Expr::WindowFunction(window)));
            }
            Ok(Transformed::no(Expr::WindowFunction(window)))
        }
        other => Ok(Transformed::no(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values observed on Trino 431.
    #[test]
    fn round_keeps_the_scale_and_rounds_half_away_from_zero() {
        assert_eq!(round_scaled(12_345, 4, 2).ok(), Some(12_300));
        assert_eq!(round_scaled(12_355, 4, 3).ok(), Some(12_360));
        assert_eq!(round_scaled(-12_355, 4, 3).ok(), Some(-12_360));
        assert_eq!(round_scaled(155, 0, -1).ok(), Some(160));
        assert_eq!(round_scaled(-155, 0, -1).ok(), Some(-160));
        assert_eq!(round_scaled(155, 0, -5).ok(), Some(0));
        assert_eq!(round_scaled(999, 2, 1).ok(), Some(1000));
        assert_eq!(round_scaled(-5, 2, 1).ok(), Some(-10));
        assert_eq!(round_scaled(125, 2, 5).ok(), Some(125));
        assert_eq!(round_scaled(7, 0, -60).ok(), Some(0));
        assert_eq!(
            round_type(&DataType::Decimal128(10, 4)),
            Some(DataType::Decimal128(11, 4))
        );
        assert_eq!(
            round_type(&DataType::Decimal128(38, 2)),
            Some(DataType::Decimal128(38, 2))
        );
        assert_eq!(round_type(&DataType::Int32), Some(DataType::Int32));
        assert_eq!(round_type(&DataType::Float64), None);
    }
}
