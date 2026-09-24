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
//!   `DataFusion`'s a `bigint` of whole seconds ([`ToUnixtime`]);
//! - `ceil(x)`, `floor(x)` and `round(x)` of a `decimal(p,s)` are
//!   `decimal(p-s+1,0)` (`decimal(p,0)` when `s` is 0), of an integer the
//!   integer itself; `DataFusion` keeps `decimal(p,s)`, which overflows on
//!   `ceil(9.5)` of a `decimal(2,1)`, and answers a `double` for integers
//!   ([`Integral`]);
//! - `date_trunc(unit, <date>)` is a `date` (`DataFusion`: a timestamp);
//! - `length(x)` is `bigint` (`DataFusion`: `integer`);
//! - `extract(TIMEZONE_HOUR FROM x)` and `TIMEZONE_MINUTE`, which
//!   `DataFusion` does not know ([`crate::trino_datetime`]).
//!
//! Functions `DataFusion` lacks are registered with the engine ([`register`]):
//! `sign`, `truncate`, `nan`, `infinity`, `is_nan`, `variance`, and the date
//! and time functions of [`crate::trino_datetime`].
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
        if let [
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(part)) | ScalarValue::Utf8View(Some(part))),
            value,
        ] = args.args.as_slice()
            && let Some(part) = crate::trino_datetime::Part::of_date_part(part)
        {
            return crate::trino_datetime::field_of(part, value, args.number_rows);
        }
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

// ---------------------------------------------------------------- ceil, floor, round, truncate

/// `(p, s)` of a decimal type (not of an integer, unlike [`as_decimal`]).
fn decimal_of(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) => as_decimal(data_type),
        _ => None,
    }
}

/// How an integral function rounds a decimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Rounding {
    Ceil,
    Floor,
    /// Half away from zero.
    Round,
    /// Toward zero.
    Truncate,
}

impl Rounding {
    fn of(name: &str) -> Option<Self> {
        Some(match name {
            "ceil" | "ceiling" => Self::Ceil,
            "floor" => Self::Floor,
            "round" => Self::Round,
            "truncate" => Self::Truncate,
            _ => return None,
        })
    }

    /// `value` scaled by `10^scale` → the integer it rounds to.
    fn apply(self, value: i128, scale: u32) -> i128 {
        let factor = 10_i128.pow(scale);
        let (quotient, remainder) = (value / factor, value % factor);
        let away = match self {
            Self::Truncate => false,
            Self::Ceil => remainder > 0,
            Self::Floor => remainder < 0,
            Self::Round => remainder.unsigned_abs() * 2 >= factor.unsigned_abs(),
        };
        if away { quotient + value.signum() } else { quotient }
    }

    /// Trino's type of the function over `decimal(p,s)`: `decimal(p-s+1,0)`
    /// (`decimal(p,0)` when `s` is 0); `truncate` has no carry,
    /// `decimal(max(p-s,1),0)`.
    fn decimal_type(self, precision: u8, scale: i8) -> DataType {
        let scale = u8::try_from(scale).unwrap_or(0);
        let integral = precision.saturating_sub(scale);
        let p = match self {
            Self::Truncate => integral.max(1),
            _ if scale == 0 => precision,
            _ => (integral + 1).min(MAX_PRECISION),
        };
        DataType::Decimal128(p, 0)
    }
}

/// `ceil`, `floor`, `round` or `truncate` of a decimal, with Trino's type
/// (see [`Rounding::decimal_type`]).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Integral {
    name: &'static str,
    rounding: Rounding,
    signature: Signature,
}

impl Integral {
    fn new(name: &'static str, rounding: Rounding) -> Self {
        Self {
            name,
            rounding,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Integral {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some((p, s)) = arg_types.first().and_then(decimal_of) else {
            return exec_err!("{}: {arg_types:?} is not a decimal", self.name);
        };
        Ok(self.rounding.decimal_type(p, s))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("{} expects one argument", self.name);
        };
        let rows = match value {
            ColumnarValue::Scalar(_) => 1,
            ColumnarValue::Array(_) => args.number_rows,
        };
        let array = value.to_array(rows)?;
        let Some((p, s)) = as_decimal(array.data_type()) else {
            return exec_err!("{}: {} is not a decimal", self.name, array.data_type());
        };
        let DataType::Decimal128(target, _) = self.rounding.decimal_type(p, s) else {
            return exec_err!("{}: no decimal type", self.name);
        };
        let values = cast(&array, &DataType::Decimal128(p.max(1), s))?;
        let scale = u32::try_from(s.max(0)).unwrap_or(0);
        let rounded: PrimitiveArray<Decimal128Type> = values
            .as_primitive::<Decimal128Type>()
            .unary(|v| self.rounding.apply(v, scale));
        let result: ArrayRef = Arc::new(rounded.with_precision_and_scale(target, 0)?);
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(result),
        })
    }
}

/// `ceil`, `floor` and one-argument `round` of a decimal → [`Integral`]; of
/// an integer, the value itself (Trino keeps the integer type).
fn check_integral(func: Arc<ScalarUDF>, args: Vec<Expr>, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let unchanged = |func, args| {
        Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
            func,
            args,
        })))
    };
    let [arg] = args.as_slice() else {
        return unchanged(func, args);
    };
    let Some(rounding) = Rounding::of(func.name()) else {
        return unchanged(func, args);
    };
    let data_type = arg.get_type(schema)?;
    if data_type.is_integer() && data_type.is_signed_integer() {
        return Ok(Transformed::yes(arg.clone()));
    }
    if decimal_of(&data_type).is_none() {
        return unchanged(func, args);
    }
    let name = match rounding {
        Rounding::Ceil => "ceil",
        Rounding::Floor => "floor",
        Rounding::Round => "round",
        Rounding::Truncate => "truncate",
    };
    Ok(Transformed::yes(
        ScalarUDF::new_from_impl(Integral::new(name, rounding)).call(args),
    ))
}

/// [`Integral`] → `DataFusion`'s function, cast to Trino's type (for SQL
/// sources).
pub(crate) fn uncheck_integral(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(Transformed::no(expr));
    };
    let Some(integral) = func.inner().as_any().downcast_ref::<Integral>() else {
        return Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
            func,
            args,
        })));
    };
    let trino = func.return_type(
        &args
            .iter()
            .map(|a| a.get_type(schema))
            .collect::<Result<Vec<_>>>()?,
    )?;
    let builtin = match integral.rounding {
        Rounding::Ceil => datafusion::functions::math::ceil(),
        Rounding::Floor => datafusion::functions::math::floor(),
        Rounding::Round => datafusion::functions::math::round(),
        Rounding::Truncate => datafusion::functions::math::trunc(),
    };
    // One more integral digit holds the carry (`ceil(9.5)` of a
    // `decimal(2,1)` is `10.0`), which `DataFusion` would overflow on.
    let args = args
        .into_iter()
        .map(|arg| match arg.get_type(schema) {
            Ok(t) => match decimal_of(&t) {
                Some((p, s)) if p < MAX_PRECISION => {
                    Expr::Cast(Cast::new(Box::new(arg), DataType::Decimal128(p + 1, s)))
                }
                _ => arg,
            },
            Err(_) => arg,
        })
        .collect();
    Ok(Transformed::yes(Expr::Cast(Cast::new(
        Box::new(builtin.call(args)),
        trino,
    ))))
}

/// `truncate(x)` and `truncate(x, n)`: toward zero. A `decimal(p,s)` is
/// `decimal(max(p-s,1),0)` without digits and keeps its type with them;
/// floating point keeps its type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Truncate {
    signature: Signature,
}

impl ScalarUDFImpl for Truncate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [t] => match decimal_of(t) {
                Some((p, s)) => Ok(Rounding::Truncate.decimal_type(p, s)),
                None if matches!(t, DataType::Float32 | DataType::Float64) => Ok(t.clone()),
                None => datafusion::common::plan_err!("Unexpected parameters ({t}) for function truncate"),
            },
            [t, n] if n.is_integer() || n.is_null() => match decimal_of(t) {
                Some((p, s)) => Ok(DataType::Decimal128(p, s)),
                None if matches!(t, DataType::Float32 | DataType::Float64) => Ok(t.clone()),
                None => {
                    datafusion::common::plan_err!("Unexpected parameters ({t}, {n}) for function truncate")
                }
            },
            _ => datafusion::common::plan_err!("Unexpected parameters ({arg_types:?}) for function truncate"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args.args.iter().all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|v| v.to_array(rows))
            .collect::<Result<Vec<_>>>()?;
        // Digits kept: `None` for a NULL, 0 without the argument.
        let digits: Vec<Option<i64>> = match arrays.get(1) {
            Some(n) => cast(n, &DataType::Int64)?
                .as_primitive::<Int64Type>()
                .iter()
                .collect(),
            None => vec![Some(0); rows],
        };
        let with_digits = arrays.len() == 2;
        let value = &arrays[0];
        let target = args.return_field.data_type().clone();
        let result: ArrayRef = match value.data_type() {
            DataType::Float32 | DataType::Float64 => {
                let doubles = cast(value, &DataType::Float64)?;
                let doubles = doubles.as_primitive::<datafusion::arrow::datatypes::Float64Type>();
                let out: datafusion::arrow::array::Float64Array = doubles
                    .iter()
                    .zip(&digits)
                    .map(|(v, n)| match (v, n) {
                        (Some(v), Some(0)) => Some(v.trunc()),
                        (Some(v), Some(n)) => {
                            let factor = 10_f64.powi(i32::try_from(*n).unwrap_or(i32::MAX));
                            Some((v * factor).trunc() / factor)
                        }
                        _ => None,
                    })
                    .collect();
                cast(&(Arc::new(out) as ArrayRef), &target)?
            }
            source => {
                let Some((p, s)) = decimal_of(source) else {
                    return exec_err!("truncate: {source} is not a number");
                };
                let values = cast(value, &DataType::Decimal128(p, s))?;
                let scale = i64::from(s);
                let out: PrimitiveArray<Decimal128Type> = values
                    .as_primitive::<Decimal128Type>()
                    .iter()
                    .zip(&digits)
                    .map(|(v, n)| match (v, n) {
                        // `truncate(x)`: an integer, scale 0.
                        (Some(v), Some(_)) if !with_digits => {
                            Some(Rounding::Truncate.apply(v, u32::try_from(scale.max(0)).unwrap_or(0)))
                        }
                        // `truncate(x, n)`: zeros past `n` digits, same scale.
                        (Some(v), Some(n)) if *n >= scale => Some(v),
                        (Some(v), Some(n)) => Some(
                            u32::try_from(scale - *n)
                                .ok()
                                .and_then(|drop| 10_i128.checked_pow(drop))
                                .map_or(0, |factor| v / factor * factor),
                        ),
                        _ => None,
                    })
                    .collect();
                let DataType::Decimal128(tp, ts) = target else {
                    return exec_err!("truncate: {target} is not a decimal");
                };
                Arc::new(out.with_precision_and_scale(tp, ts)?)
            }
        };
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

// ---------------------------------------------------------------- sign, nan, infinity

/// `sign(x)`: `-1`, `0` or `1` of `x`'s type; `decimal(1,0)` for a decimal.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Sign {
    signature: Signature,
}

impl ScalarUDFImpl for Sign {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "sign"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [t] if decimal_of(t).is_some() => Ok(DataType::Decimal128(1, 0)),
            [t] if t.is_signed_integer() || matches!(t, DataType::Float32 | DataType::Float64) => {
                Ok(t.clone())
            }
            _ => datafusion::common::plan_err!("Unexpected parameters ({arg_types:?}) for function sign"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("sign expects one argument");
        };
        let rows = match value {
            ColumnarValue::Scalar(_) => 1,
            ColumnarValue::Array(_) => args.number_rows,
        };
        let array = value.to_array(rows)?;
        let target = args.return_field.data_type().clone();
        let result: ArrayRef = match array.data_type() {
            DataType::Float32 | DataType::Float64 => {
                let doubles = cast(&array, &DataType::Float64)?;
                let signs: PrimitiveArray<datafusion::arrow::datatypes::Float64Type> = doubles
                    .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
                    .unary(|v| if v == 0.0 || v.is_nan() { v } else { v.signum() });
                cast(&(Arc::new(signs) as ArrayRef), &target)?
            }
            source => {
                // Integers and decimals: the sign of the unscaled value.
                let unscaled = match decimal_of(source) {
                    Some((p, s)) => cast(&array, &DataType::Decimal128(p, s))?,
                    None => cast(&array, &DataType::Decimal128(MAX_PRECISION, 0))?,
                };
                let signs: PrimitiveArray<Decimal128Type> =
                    unscaled.as_primitive::<Decimal128Type>().unary(i128::signum);
                let signs: ArrayRef = Arc::new(signs.with_precision_and_scale(1, 0)?);
                cast(&signs, &target)?
            }
        };
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(result),
        })
    }
}

/// A `double` constant: `nan()`, `infinity()`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DoubleConstant {
    name: &'static str,
    signature: Signature,
}

impl ScalarUDFImpl for DoubleConstant {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = if self.name == "nan" {
            f64::NAN
        } else {
            f64::INFINITY
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
    }
}

/// Registers the functions `DataFusion` lacks (see the module documentation).
pub(crate) fn register(ctx: &datafusion::prelude::SessionContext) {
    let nullary = || Signature::nullary(Volatility::Immutable);
    ctx.register_udf(ScalarUDF::new_from_impl(Sign {
        signature: Signature::any(1, Volatility::Immutable),
    }));
    ctx.register_udf(ScalarUDF::new_from_impl(Truncate {
        signature: Signature::variadic_any(Volatility::Immutable),
    }));
    for name in ["nan", "infinity"] {
        ctx.register_udf(ScalarUDF::new_from_impl(DoubleConstant {
            name,
            signature: nullary(),
        }));
    }
    // `is_nan` is `DataFusion`'s `isnan`.
    ctx.register_udf(
        datafusion::functions::math::isnan()
            .as_ref()
            .clone()
            .with_aliases(["is_nan"]),
    );
    // `variance` is `var_samp`.
    ctx.register_udaf(
        datafusion::functions_aggregate::variance::var_samp_udaf()
            .as_ref()
            .clone()
            .with_aliases(["variance"]),
    );
    for function in crate::trino_datetime::functions() {
        ctx.register_udf(function);
    }
}

/// Every function of one expression node that answers another type than in
/// Trino (see the module documentation).
pub(crate) fn trino_function(expr: Expr, schema: &DFSchema, session_zone: &str) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarFunction(ScalarFunction { func, args }) if is_builtin_to_unixtime(&func) => {
            check_to_unixtime(func, args, schema, session_zone)
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if !func.inner().as_any().is::<Integral>()
                && (matches!(func.name(), "ceil" | "floor")
                    || (is_builtin_round(&func) && args.len() == 1)) =>
        {
            check_integral(func, args, schema)
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) if is_builtin_round(&func) => {
            check_round(func, args, schema)
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if func.name() == "date_trunc"
                && args.len() == 2
                && matches!(args[1].get_type(schema)?, DataType::Date32 | DataType::Date64) =>
        {
            let truncated = Expr::ScalarFunction(ScalarFunction { func, args });
            Ok(Transformed::yes(Expr::Cast(Cast::new(
                Box::new(truncated),
                DataType::Date32,
            ))))
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if matches!(func.name(), "character_length" | "length" | "char_length") =>
        {
            let length = Expr::ScalarFunction(ScalarFunction { func, args });
            if length.get_type(schema)? == DataType::Int32 {
                return Ok(Transformed::yes(Expr::Cast(Cast::new(
                    Box::new(length),
                    DataType::Int64,
                ))));
            }
            Ok(Transformed::no(length))
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
