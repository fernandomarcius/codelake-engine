//! Checked arithmetic kernels behind the engine's `+ - * / %`, unary minus and
//! `abs` on exact numbers.
//!
//! `DataFusion` evaluates integer `+ - *` and negation with wrapping arithmetic
//! and decimal `+ - *` without checking the declared precision, so an overflow
//! returns a wrong number. Trino fails with `NUMERIC_VALUE_OUT_OF_RANGE`; these
//! kernels do the same and report every overflow as
//! [`ArrowError::ArithmeticOverflow`] with Trino's message
//! (`bigint addition overflow: 9223372036854775807 + 1`, `Decimal overflow`).
//! Integer `%` follows Java: `MIN % -1` is `0`, not an error.

use std::fmt::Display;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, Datum, PrimitiveArray, new_null_array,
};
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

/// Name of an integer type as Trino spells it in its overflow messages.
pub(super) fn sql_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "bigint".into(),
        other => other.to_string().to_lowercase(),
    }
}

pub(super) fn is_integer(data_type: &DataType) -> bool {
    data_type.is_integer()
}

pub(super) fn is_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

pub(super) fn overflow(message: impl Into<String>) -> DataFusionError {
    DataFusionError::from(ArrowError::ArithmeticOverflow(message.into()))
}

/// Trino's words for each operator in "bigint addition overflow: a + b".
fn operation(op: Operator) -> (&'static str, &'static str) {
    match op {
        Operator::Plus => ("addition", "+"),
        Operator::Minus => ("subtraction", "-"),
        Operator::Multiply => ("multiplication", "*"),
        Operator::Divide => ("division", "/"),
        _ => ("modulus", "%"),
    }
}

/// Operators this module evaluates.
pub(super) fn is_arithmetic(op: Operator) -> bool {
    matches!(
        op,
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo
    )
}

fn integer_op<N>(op: Operator, type_name: &str) -> impl Fn(N, N) -> Result<N, ArrowError> + '_
where
    N: ArrowNativeTypeOp + Display,
{
    move |a: N, b: N| {
        let (word, symbol) = operation(op);
        let failed =
            || ArrowError::ArithmeticOverflow(format!("{type_name} {word} overflow: {a} {symbol} {b}"));
        match op {
            Operator::Plus => a.add_checked(b).map_err(|_| failed()),
            Operator::Minus => a.sub_checked(b).map_err(|_| failed()),
            Operator::Multiply => a.mul_checked(b).map_err(|_| failed()),
            Operator::Divide | Operator::Modulo if b.is_zero() => Err(ArrowError::DivideByZero),
            Operator::Divide => a.div_checked(b).map_err(|_| failed()),
            // Java's `%`: MIN % -1 is 0.
            Operator::Modulo => Ok(a.mod_wrapping(b)),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "{op} is not arithmetic"
            ))),
        }
    }
}

fn scalar_value<T: ArrowPrimitiveType>(scalar: &ScalarValue) -> Result<Option<T::Native>> {
    let array = scalar.to_array_of_size(1)?;
    let array = array.as_primitive::<T>();
    Ok(array.is_valid(0).then(|| array.value(0)))
}

fn integer_binary<T>(
    op: Operator,
    left: &ColumnarValue,
    right: &ColumnarValue,
    rows: usize,
) -> Result<ColumnarValue>
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp + Display,
{
    let type_name = sql_type_name(&T::DATA_TYPE);
    let f = integer_op::<T::Native>(op, &type_name);
    let result: ArrayRef = match (left, right) {
        (ColumnarValue::Scalar(a), ColumnarValue::Scalar(b)) => {
            let value = match (scalar_value::<T>(a)?, scalar_value::<T>(b)?) {
                (Some(a), Some(b)) => Some(f(a, b)?),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::new_primitive::<T>(
                value,
                &T::DATA_TYPE,
            )?));
        }
        (ColumnarValue::Array(a), ColumnarValue::Scalar(b)) => match scalar_value::<T>(b)? {
            Some(b) => Arc::new(a.as_primitive::<T>().try_unary::<_, T, _>(|a| f(a, b))?),
            None => new_null_array(&T::DATA_TYPE, rows),
        },
        (ColumnarValue::Scalar(a), ColumnarValue::Array(b)) => match scalar_value::<T>(a)? {
            Some(a) => Arc::new(b.as_primitive::<T>().try_unary::<_, T, _>(|b| f(a, b))?),
            None => new_null_array(&T::DATA_TYPE, rows),
        },
        (ColumnarValue::Array(a), ColumnarValue::Array(b)) => {
            Arc::new(datafusion::arrow::compute::kernels::arity::try_binary::<
                _,
                _,
                _,
                T,
            >(a.as_primitive::<T>(), b.as_primitive::<T>(), f)?)
        }
    };
    Ok(ColumnarValue::Array(result))
}

fn validate_precision<T: DecimalType>(array: &ArrayRef, precision: u8) -> Result<()> {
    array
        .as_primitive::<T>()
        .validate_decimal_precision(precision)
        .map_err(|_| overflow("Decimal overflow"))
}

/// Fails with `Decimal overflow` if a value does not fit its decimal type.
pub(super) fn check_decimal(array: &ArrayRef) -> Result<()> {
    match array.data_type() {
        DataType::Decimal32(p, _) => validate_precision::<Decimal32Type>(array, *p),
        DataType::Decimal64(p, _) => validate_precision::<Decimal64Type>(array, *p),
        DataType::Decimal128(p, _) => validate_precision::<Decimal128Type>(array, *p),
        DataType::Decimal256(p, _) => validate_precision::<Decimal256Type>(array, *p),
        _ => Ok(()),
    }
}

fn datum(value: &ColumnarValue) -> Result<Box<dyn Datum>> {
    Ok(match value {
        ColumnarValue::Array(array) => Box::new(Arc::clone(array)),
        ColumnarValue::Scalar(scalar) => Box::new(scalar.to_scalar()?),
    })
}

/// Decimal arithmetic with Arrow's checked kernels, then the declared precision.
fn decimal_binary(
    op: Operator,
    left: &ColumnarValue,
    right: &ColumnarValue,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    let (l, r) = (datum(left)?, datum(right)?);
    let (l, r) = (l.as_ref(), r.as_ref());
    let computed = match op {
        Operator::Plus => numeric::add(l, r),
        Operator::Minus => numeric::sub(l, r),
        Operator::Multiply => numeric::mul(l, r),
        Operator::Divide => numeric::div(l, r),
        _ => numeric::rem(l, r),
    }
    .map_err(|e| match e {
        ArrowError::ArithmeticOverflow(_) => overflow("Decimal overflow"),
        other => DataFusionError::from(other),
    })?;
    let mut result = computed;
    if result.data_type() != return_type {
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        result =
            cast_with_options(&result, return_type, &options).map_err(|_| overflow("Decimal overflow"))?;
    }
    check_decimal(&result)?;
    let both_scalar = matches!(
        (left, right),
        (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
    );
    Ok(if both_scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
    } else {
        ColumnarValue::Array(result)
    })
}

macro_rules! by_integer_type {
    ($data_type:expr, $call:ident, $($arg:expr),*) => {
        match $data_type {
            DataType::Int8 => $call::<Int8Type>($($arg),*),
            DataType::Int16 => $call::<Int16Type>($($arg),*),
            DataType::Int32 => $call::<Int32Type>($($arg),*),
            DataType::Int64 => $call::<Int64Type>($($arg),*),
            DataType::UInt8 => $call::<UInt8Type>($($arg),*),
            DataType::UInt16 => $call::<UInt16Type>($($arg),*),
            DataType::UInt32 => $call::<UInt32Type>($($arg),*),
            DataType::UInt64 => $call::<UInt64Type>($($arg),*),
            other => exec_err!("checked arithmetic does not support {other}"),
        }
    };
}

fn nullable_field(name: &str, data_type: DataType, args: &ReturnFieldArgs<'_>) -> FieldRef {
    let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
    Arc::new(Field::new(name, data_type, nullable))
}

/// `a <op> b` on integers or decimals, failing on overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedBinary {
    op: Operator,
    signature: Signature,
}

impl CheckedBinary {
    pub(crate) fn new(op: Operator) -> Self {
        Self {
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    pub(crate) fn op(&self) -> Operator {
        self.op
    }
}

impl ScalarUDFImpl for CheckedBinary {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "codelake_checked_add",
            Operator::Minus => "codelake_checked_subtract",
            Operator::Multiply => "codelake_checked_multiply",
            Operator::Divide => "codelake_checked_divide",
            _ => "codelake_checked_modulo",
        }
    }

    /// The column name of the operator it replaces (`a + b`), so rewriting a
    /// plan never renames its output.
    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [left, right] = args else {
            return exec_err!("{} expects two arguments", self.name());
        };
        let original = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left.clone()),
            self.op,
            Box::new(right.clone()),
        ));
        Ok(original.schema_name().to_string())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [left, right] = arg_types else {
            return exec_err!("{} expects two arguments", self.name());
        };
        BinaryTypeCoercer::new(left, &self.op, right).get_result_type()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        Ok(nullable_field(self.name(), self.return_type(&types)?, &args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("{} expects two arguments", self.name());
        };
        let (left_type, right_type) = (left.data_type(), right.data_type());
        if is_decimal(&left_type) || is_decimal(&right_type) {
            return decimal_binary(self.op, left, right, args.return_field.data_type());
        }
        if left_type != right_type {
            return exec_err!("{}: {left_type} and {right_type} differ", self.name());
        }
        by_integer_type!(&left_type, integer_binary, self.op, left, right, args.number_rows)
    }
}

fn integer_unary<T>(
    value: &ColumnarValue,
    f: &dyn Fn(T::Native) -> Result<T::Native, ArrowError>,
) -> Result<ColumnarValue>
where
    T: ArrowPrimitiveType,
{
    match value {
        ColumnarValue::Scalar(scalar) => {
            let result = scalar_value::<T>(scalar)?.map(f).transpose()?;
            Ok(ColumnarValue::Scalar(ScalarValue::new_primitive::<T>(
                result,
                &T::DATA_TYPE,
            )?))
        }
        ColumnarValue::Array(array) => {
            let result: PrimitiveArray<T> = array.as_primitive::<T>().try_unary(f)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

fn negate<T>(value: &ColumnarValue) -> Result<ColumnarValue>
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp + Display,
{
    let type_name = sql_type_name(&T::DATA_TYPE);
    integer_unary::<T>(value, &|x: T::Native| {
        x.neg_checked()
            .map_err(|_| ArrowError::ArithmeticOverflow(format!("{type_name} negation overflow: {x}")))
    })
}

fn absolute<T>(value: &ColumnarValue) -> Result<ColumnarValue>
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp + Display,
{
    let type_name = sql_type_name(&T::DATA_TYPE);
    integer_unary::<T>(value, &|x: T::Native| {
        if x.is_lt(T::Native::ZERO) {
            x.neg_checked().map_err(|_| {
                ArrowError::ArithmeticOverflow(format!("Value {x} is out of range for abs({type_name})"))
            })
        } else {
            Ok(x)
        }
    })
}

/// Unary minus on an integer, failing on `-MIN`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedNegate {
    signature: Signature,
}

impl CheckedNegate {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CheckedNegate {
    fn name(&self) -> &'static str {
        "codelake_checked_negate"
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [arg] = args else {
            return exec_err!("{} expects one argument", self.name());
        };
        Ok(Expr::Negative(Box::new(arg.clone())).schema_name().to_string())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("{} expects one argument", self.name())))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        Ok(nullable_field(self.name(), self.return_type(&types)?, &args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("{} expects one argument", self.name());
        };
        by_integer_type!(&value.data_type(), negate, value)
    }
}

/// `abs` on an integer, failing on `abs(MIN)` like Trino. Keeps the name
/// `abs`, so a plan that reaches a SQL source still says `abs(x)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedAbs {
    signature: Signature,
}

impl CheckedAbs {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CheckedAbs {
    fn name(&self) -> &'static str {
        "abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| DataFusionError::Plan("abs expects one argument".into()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        Ok(nullable_field(self.name(), self.return_type(&types)?, &args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("abs expects one argument");
        };
        by_integer_type!(&value.data_type(), absolute, value)
    }
}
