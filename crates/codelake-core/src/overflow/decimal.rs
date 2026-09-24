//! Trino's decimal arithmetic: result types, rounding and overflow.
//!
//! Trino 431 types `a <op> b` on `decimal(p1,s1)` and `decimal(p2,s2)` as
//! (an integer operand counts as `decimal(19,0)` for `bigint`, `decimal(10,0)`
//! for `integer`, `decimal(5,0)` for `smallint`, `decimal(3,0)` for `tinyint`):
//!
//! | op  | scale             | precision (at most 38)                    |
//! |-----|-------------------|-------------------------------------------|
//! | `+ -` | `max(s1,s2)`    | `1 + max(s1,s2) + max(p1-s1, p2-s2)`      |
//! | `*` | `s1 + s2` (≤ 38)  | `p1 + p2`                                 |
//! | `/` | `max(s1,s2)`      | `p1 + s2 + max(s2 - s1, 0)`               |
//! | `%` | `max(s1,s2)`      | `min(p1-s1, p2-s2) + max(s1,s2)`          |
//!
//! `/` rounds half away from zero at the result scale. It rescales the
//! dividend by `10^(max(s1,s2) - s1 + s2)` and fails with `Decimal overflow`
//! when that factor reaches 38, whatever the values. A result that does not
//! fit its precision fails with `Decimal overflow`; a zero divisor with
//! `Division by zero`. `DataFusion` types `/` with a scale of at least 6,
//! `*` with one more digit and truncates, so the same query answered other
//! numbers (TPC-H Q08, Q14, Q17).
//!
//! Values are computed exactly: 128-bit first, 256-bit when an intermediate
//! value does not fit (operands have at most 38 digits and rescaling factors
//! at most 37, so 256 bits always hold the exact value).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Decimal128Array, new_null_array};
use datafusion::arrow::compute::kernels::arity::try_binary;
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, i256};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use super::arith::is_arithmetic;

/// Trino's largest decimal precision.
pub(crate) const MAX_PRECISION: u8 = 38;

/// The `decimal(p,s)` an exact operand takes in decimal arithmetic, or `None`
/// for any other type (floating point, a negative scale, more than 38 digits).
pub(crate) fn as_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match *data_type {
        DataType::Int8 | DataType::UInt8 => Some((3, 0)),
        DataType::Int16 | DataType::UInt16 => Some((5, 0)),
        DataType::Int32 | DataType::UInt32 => Some((10, 0)),
        DataType::Int64 => Some((19, 0)),
        DataType::UInt64 => Some((20, 0)),
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s)
            if s >= 0 && p <= MAX_PRECISION =>
        {
            Some((p, s))
        }
        _ => None,
    }
}

/// [`as_decimal`], plus Trino's `unknown` (a bare `NULL`), which takes part in
/// decimal arithmetic as `decimal(1,0)`: `0.2 * NULL` is `decimal(2,1)`.
pub(crate) fn as_decimal_operand(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Null => Some((1, 0)),
        other => as_decimal(other),
    }
}

fn is_exact_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..)
    ) && as_decimal(data_type).is_some()
}

/// Whether `a <op> b` is decimal arithmetic in Trino: an arithmetic operator,
/// both operands exact numbers (or `NULL`) and at least one of them a decimal.
pub(crate) fn is_decimal_operation(op: Operator, left: &DataType, right: &DataType) -> bool {
    is_arithmetic(op)
        && as_decimal_operand(left).is_some()
        && as_decimal_operand(right).is_some()
        && (is_exact_decimal(left) || is_exact_decimal(right))
}

fn digits(precision: u8, scale: i8) -> i16 {
    i16::from(precision) - i16::from(scale)
}

fn precision(value: i16) -> u8 {
    u8::try_from(value.clamp(1, i16::from(MAX_PRECISION))).unwrap_or(MAX_PRECISION)
}

/// Trino's result type of `decimal(p1,s1) <op> decimal(p2,s2)` (see the
/// module documentation).
pub(crate) fn result_type(op: Operator, (p1, s1): (u8, i8), (p2, s2): (u8, i8)) -> Result<DataType> {
    let scale = s1.max(s2);
    let (p, s) = match op {
        Operator::Plus | Operator::Minus => (
            precision(1 + i16::from(scale) + digits(p1, s1).max(digits(p2, s2))),
            scale,
        ),
        Operator::Multiply => {
            let s = i16::from(s1) + i16::from(s2);
            if s > i16::from(MAX_PRECISION) {
                return plan_err!("DECIMAL scale must be in range [0, precision ({MAX_PRECISION})]: {s}");
            }
            (
                precision(i16::from(p1) + i16::from(p2)),
                i8::try_from(s).unwrap_or(0),
            )
        }
        Operator::Divide => (
            precision(i16::from(p1) + i16::from(s2) + (i16::from(s2) - i16::from(s1)).max(0)),
            scale,
        ),
        Operator::Modulo => (
            precision(digits(p1, s1).min(digits(p2, s2)) + i16::from(scale)),
            scale,
        ),
        other => return plan_err!("{other} is not decimal arithmetic"),
    };
    Ok(DataType::Decimal128(p, s))
}

fn overflow() -> ArrowError {
    ArrowError::ArithmeticOverflow("Decimal overflow".into())
}

fn pow10_i128(exp: u32) -> Option<i128> {
    10_i128.checked_pow(exp)
}

fn pow10_i256(exp: u32) -> Option<i256> {
    i256::from_i128(10).checked_pow(exp)
}

/// `a * 10^exp` in 256 bits.
fn rescale(value: i256, exp: u32) -> Result<i256, ArrowError> {
    value
        .checked_mul(pow10_i256(exp).ok_or_else(overflow)?)
        .ok_or_else(overflow)
}

fn scale_gap(to: i8, from: i8) -> u32 {
    u32::try_from(i16::from(to) - i16::from(from)).unwrap_or(0)
}

/// `n / d` rounded half away from zero.
fn divide_rounded_i128(n: i128, d: i128) -> Option<i128> {
    let q = n.checked_div(d)?;
    let r = n.checked_rem(d)?;
    if r.unsigned_abs() >= d.unsigned_abs() - r.unsigned_abs() {
        Some(if (n < 0) == (d < 0) { q + 1 } else { q - 1 })
    } else {
        Some(q)
    }
}

fn divide_rounded_i256(n: i256, d: i256) -> Option<i256> {
    let q = n.checked_div(d)?;
    let r = n.checked_rem(d)?;
    let (r, d_abs) = (r.checked_abs()?, d.checked_abs()?);
    if r >= d_abs.checked_sub(r)? {
        let one = if n.is_negative() == d.is_negative() {
            i256::ONE
        } else {
            i256::MINUS_ONE
        };
        q.checked_add(one)
    } else {
        Some(q)
    }
}

/// One decimal operation between values already scaled by `10^s1`, `10^s2`,
/// producing a value scaled by `10^rs` that fits `rp` digits.
#[derive(Debug, Clone, Copy)]
struct Kernel {
    op: Operator,
    s1: i8,
    s2: i8,
    rp: u8,
    rs: i8,
}

impl Kernel {
    fn apply(self, a: i128, b: i128) -> Result<i128, ArrowError> {
        let value = match self.fast(a, b)? {
            Some(value) => value,
            None => self.wide(a, b)?,
        };
        let limit = pow10_i128(u32::from(self.rp)).ok_or_else(overflow)?;
        if value.unsigned_abs() >= limit.unsigned_abs() {
            return Err(overflow());
        }
        Ok(value)
    }

    fn check_divisor(self, b: i128) -> Result<(), ArrowError> {
        if matches!(self.op, Operator::Divide | Operator::Modulo) && b == 0 {
            return Err(ArrowError::DivideByZero);
        }
        if self.op == Operator::Divide && self.division_factor() >= u32::from(MAX_PRECISION) {
            return Err(overflow());
        }
        Ok(())
    }

    fn division_factor(self) -> u32 {
        scale_gap(self.rs, self.s1) + u32::try_from(self.s2).unwrap_or(0)
    }

    /// 128-bit evaluation; `None` when an intermediate value does not fit.
    fn fast(self, a: i128, b: i128) -> Result<Option<i128>, ArrowError> {
        self.check_divisor(b)?;
        let up = |v: i128, from: i8| pow10_i128(scale_gap(self.rs, from)).and_then(|m| v.checked_mul(m));
        Ok(match self.op {
            Operator::Plus => up(a, self.s1)
                .zip(up(b, self.s2))
                .and_then(|(a, b)| a.checked_add(b)),
            Operator::Minus => up(a, self.s1)
                .zip(up(b, self.s2))
                .and_then(|(a, b)| a.checked_sub(b)),
            Operator::Multiply => a.checked_mul(b),
            Operator::Divide => pow10_i128(self.division_factor())
                .and_then(|m| a.checked_mul(m))
                .and_then(|n| divide_rounded_i128(n, b)),
            _ => up(a, self.s1)
                .zip(up(b, self.s2))
                .and_then(|(a, b)| a.checked_rem(b)),
        })
    }

    /// Exact 256-bit evaluation.
    fn wide(self, a: i128, b: i128) -> Result<i128, ArrowError> {
        let (a, b) = (i256::from_i128(a), i256::from_i128(b));
        let up = |v: i256, from: i8| rescale(v, scale_gap(self.rs, from));
        let value = match self.op {
            Operator::Plus => up(a, self.s1)?.checked_add(up(b, self.s2)?),
            Operator::Minus => up(a, self.s1)?.checked_sub(up(b, self.s2)?),
            Operator::Multiply => a.checked_mul(b),
            Operator::Divide => divide_rounded_i256(rescale(a, self.division_factor())?, b),
            _ => up(a, self.s1)?.checked_rem(up(b, self.s2)?),
        };
        value.and_then(i256::to_i128).ok_or_else(overflow)
    }
}

fn to_decimal128(value: &ColumnarValue, (p, s): (u8, i8)) -> Result<ColumnarValue> {
    let target = DataType::Decimal128(p, s);
    if value.data_type() == target {
        return Ok(value.clone());
    }
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    Ok(match value {
        ColumnarValue::Array(array) => ColumnarValue::Array(cast_with_options(array, &target, &options)?),
        ColumnarValue::Scalar(scalar) => {
            ColumnarValue::Scalar(scalar.cast_to_with_options(&target, &options)?)
        }
    })
}

fn scalar_i128(value: &ScalarValue) -> Result<Option<i128>> {
    match value {
        ScalarValue::Decimal128(v, _, _) => Ok(*v),
        other => exec_err!("expected a decimal, got {other:?}"),
    }
}

fn evaluate(
    kernel: Kernel,
    left: &ColumnarValue,
    right: &ColumnarValue,
    rows: usize,
) -> Result<ColumnarValue> {
    let result_type = DataType::Decimal128(kernel.rp, kernel.rs);
    let f = |a: i128, b: i128| kernel.apply(a, b);
    let array: Decimal128Array = match (left, right) {
        (ColumnarValue::Scalar(a), ColumnarValue::Scalar(b)) => {
            let value = match (scalar_i128(a)?, scalar_i128(b)?) {
                (Some(a), Some(b)) => Some(f(a, b)?),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                value, kernel.rp, kernel.rs,
            )));
        }
        (ColumnarValue::Array(a), ColumnarValue::Scalar(b)) => match scalar_i128(b)? {
            Some(b) => a
                .as_primitive::<Decimal128Type>()
                .try_unary::<_, Decimal128Type, _>(|a| f(a, b))?,
            None => return Ok(ColumnarValue::Array(new_null_array(&result_type, rows))),
        },
        (ColumnarValue::Scalar(a), ColumnarValue::Array(b)) => match scalar_i128(a)? {
            Some(a) => b
                .as_primitive::<Decimal128Type>()
                .try_unary::<_, Decimal128Type, _>(|b| f(a, b))?,
            None => return Ok(ColumnarValue::Array(new_null_array(&result_type, rows))),
        },
        (ColumnarValue::Array(a), ColumnarValue::Array(b)) => try_binary::<_, _, _, Decimal128Type>(
            a.as_primitive::<Decimal128Type>(),
            b.as_primitive::<Decimal128Type>(),
            f,
        )?,
    };
    let array: ArrayRef = Arc::new(array.with_precision_and_scale(kernel.rp, kernel.rs)?);
    Ok(ColumnarValue::Array(array))
}

/// `a <op> b` with Trino's decimal types and rounding (see the module
/// documentation). An integer operand is converted to its decimal type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct DecimalBinary {
    op: Operator,
    signature: Signature,
}

impl DecimalBinary {
    pub(crate) fn new(op: Operator) -> Self {
        Self {
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    pub(crate) fn op(&self) -> Operator {
        self.op
    }

    fn operands(&self, arg_types: &[DataType]) -> Result<((u8, i8), (u8, i8))> {
        let [left, right] = arg_types else {
            return plan_err!("{} expects two arguments", self.name());
        };
        let (Some(l), Some(r)) = (as_decimal_operand(left), as_decimal_operand(right)) else {
            return plan_err!("{}: {left} and {right} are not exact numbers", self.name());
        };
        Ok((l, r))
    }
}

impl ScalarUDFImpl for DecimalBinary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "codelake_decimal_add",
            Operator::Minus => "codelake_decimal_subtract",
            Operator::Multiply => "codelake_decimal_multiply",
            Operator::Divide => "codelake_decimal_divide",
            _ => "codelake_decimal_modulo",
        }
    }

    /// The column name of the operator it replaces (`a / b`).
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
        let (left, right) = self.operands(arg_types)?;
        result_type(self.op, left, right)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args.arg_fields.iter().map(|f| f.data_type().clone()).collect();
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            nullable,
        )))
    }

    /// A `NULL` operand makes the result `NULL` of the result type. Scalar
    /// subquery decorrelation evaluates the subquery's expression with every
    /// aggregate replaced by an untyped `NULL` (TPC-H Q17's `0.2 * avg(…)`).
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let is_null = |e: &Expr| matches!(e, Expr::Literal(v, _) if v.is_null());
        if !args.iter().any(is_null) {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let types = args
            .iter()
            .map(|a| info.get_data_type(a))
            .collect::<Result<Vec<_>>>()?;
        let null = ScalarValue::try_from(&self.return_type(&types)?)?;
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(null, None)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return exec_err!("{} expects two arguments", self.name());
        };
        let ((p1, s1), (p2, s2)) = self.operands(&[left.data_type(), right.data_type()])?;
        let DataType::Decimal128(rp, rs) = *args.return_field.data_type() else {
            return exec_err!(
                "{} returns a decimal, planned {}",
                self.name(),
                args.return_field.data_type()
            );
        };
        let left = to_decimal128(left, (p1, s1))?;
        let right = to_decimal128(right, (p2, s2))?;
        let kernel = Kernel {
            op: self.op,
            s1,
            s2,
            rp,
            rs,
        };
        evaluate(kernel, &left, &right, args.number_rows).map_err(|e| match e {
            DataFusionError::ArrowError(arrow, _) if matches!(*arrow, ArrowError::DivideByZero) => {
                DataFusionError::from(ArrowError::DivideByZero)
            }
            other => other,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(op: Operator, l: (u8, i8), r: (u8, i8)) -> DataType {
        result_type(op, l, r).unwrap()
    }

    /// Types observed on Trino 431 (`typeof(a <op> b)`).
    #[test]
    fn result_types_follow_trino() {
        use DataType::Decimal128 as D;
        use Operator::{Divide, Minus, Modulo, Multiply, Plus};
        assert_eq!(t(Multiply, (12, 2), (13, 2)), D(25, 4));
        assert_eq!(t(Divide, (12, 2), (13, 2)), D(14, 2));
        assert_eq!(t(Divide, (38, 4), (38, 4)), D(38, 4));
        assert_eq!(t(Divide, (38, 2), (2, 1)), D(38, 2));
        assert_eq!(t(Modulo, (5, 2), (7, 3)), D(6, 3));
        assert_eq!(t(Plus, (12, 2), (10, 0)), D(13, 2));
        assert_eq!(t(Minus, (12, 2), (10, 0)), D(13, 2));
        assert_eq!(t(Multiply, (12, 2), (5, 0)), D(17, 2));
        assert_eq!(t(Multiply, (12, 2), (3, 0)), D(15, 2));
        assert_eq!(t(Multiply, (20, 10), (20, 10)), D(38, 20));
        assert_eq!(t(Modulo, (5, 1), (10, 0)), D(5, 1));
        assert!(result_type(Multiply, (30, 20), (30, 20)).is_err());
    }

    fn apply(
        op: Operator,
        (a, s1): (i128, i8),
        (b, s2): (i128, i8),
        (rp, rs): (u8, i8),
    ) -> Result<i128, ArrowError> {
        Kernel { op, s1, s2, rp, rs }.apply(a, b)
    }

    #[test]
    fn division_rounds_half_away_from_zero() {
        use Operator::Divide;
        assert_eq!(apply(Divide, (10, 0), (3, 0), (5, 0)).unwrap(), 3);
        assert_eq!(apply(Divide, (20, 0), (3, 0), (5, 0)).unwrap(), 7);
        assert_eq!(apply(Divide, (-20, 0), (3, 0), (5, 0)).unwrap(), -7);
        assert_eq!(apply(Divide, (-10, 1), (4, 0), (6, 1)).unwrap(), -3);
        assert_eq!(apply(Divide, (100, 2), (8, 0), (7, 2)).unwrap(), 13);
        assert_eq!(apply(Divide, (-100, 2), (8, 0), (7, 2)).unwrap(), -13);
        assert_eq!(apply(Divide, (125, 2), (2, 0), (7, 2)).unwrap(), 63);
        assert_eq!(apply(Divide, (125, 2), (-2, 0), (7, 2)).unwrap(), -63);
    }

    #[test]
    fn wide_values_are_exact() {
        use Operator::{Divide, Multiply, Plus};
        let nines = 10_i128.pow(38) - 1;
        // 1 / 3 with scale 18: factor 36, dividend 10^36 fits 128 bits.
        assert_eq!(
            apply(Divide, (1, 0), (3 * 10_i128.pow(18), 18), (38, 18)).unwrap(),
            333_333_333_333_333_333
        );
        // 1 / 3 with scale 19 from scale 1: factor 37.
        assert_eq!(
            apply(Divide, (10, 1), (3 * 10_i128.pow(19), 19), (38, 19)).unwrap(),
            3_333_333_333_333_333_333
        );
        // Rescaling past 128 bits (1.8e38) keeps the exact value.
        assert_eq!(
            apply(
                Operator::Minus,
                (18 * 10_i128.pow(36), 0),
                (99 * 10_i128.pow(36), 1),
                (38, 1)
            )
            .unwrap(),
            81 * 10_i128.pow(36)
        );
        assert!(apply(Plus, (nines, 0), (1, 1), (38, 1)).is_err());
        assert_eq!(
            apply(Multiply, (10_i128.pow(19), 0), (10_i128.pow(18), 0), (38, 0)).unwrap(),
            10_i128.pow(37)
        );
        assert!(apply(Multiply, (10_i128.pow(19), 0), (10_i128.pow(19), 0), (38, 0)).is_err());
    }

    #[test]
    fn overflow_and_division_by_zero_match_trino() {
        use Operator::{Divide, Modulo, Plus};
        let nines = 10_i128.pow(38) - 1;
        assert!(matches!(
            apply(Plus, (nines, 0), (1, 0), (38, 0)),
            Err(ArrowError::ArithmeticOverflow(m)) if m == "Decimal overflow"
        ));
        // A rescaling factor of 38 fails whatever the values.
        assert!(matches!(
            apply(Divide, (0, 0), (3, 19), (38, 19)),
            Err(ArrowError::ArithmeticOverflow(_))
        ));
        assert!(matches!(
            apply(Divide, (1, 0), (0, 0), (5, 0)),
            Err(ArrowError::DivideByZero)
        ));
        assert!(matches!(
            apply(Modulo, (1, 2), (0, 0), (5, 2)),
            Err(ArrowError::DivideByZero)
        ));
    }

    #[test]
    fn modulus_keeps_the_dividend_sign() {
        use Operator::Modulo;
        assert_eq!(apply(Modulo, (75, 1), (2, 0), (5, 1)).unwrap(), 15);
        assert_eq!(apply(Modulo, (-75, 1), (2, 0), (5, 1)).unwrap(), -15);
    }
}
