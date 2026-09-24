//! Decimal arithmetic with Trino's result types and rounding.
//!
//! `DataFusion` types `decimal ⊕ decimal` like Spark: `DECIMAL '1' /
//! DECIMAL '3'` is `0.3333` there and `0` in Trino, and `bigint / 3600.0` gets
//! four more fractional digits than Trino's one. The rewrite in `rewrite`
//! sends `+ - * / %` with a decimal operand (the other decimal or integer)
//! here, before type coercion. Integers take part as `decimal(p, 0)`
//! (`tinyint` 3, `smallint` 5, `integer` 10, `bigint` 19 digits). Result
//! types follow Trino's `DecimalOperators`:
//!
//! | op | precision | scale |
//! |---|---|---|
//! | `+` `-` | `min(38, 1 + max(s1, s2) + max(p1 - s1, p2 - s2))` | `max(s1, s2)` |
//! | `*` | `min(38, p1 + p2)` | `s1 + s2` |
//! | `/` | `min(38, p1 - s1 + s2 + max(s1, s2))` | `max(s1, s2)` |
//! | `%` | `min(p2 - s2, p1 - s1) + max(s1, s2)` | `max(s1, s2)` |
//!
//! Quotients round half up; a result that does not fit its precision is
//! `NUMERIC_VALUE_OUT_OF_RANGE`, a zero divisor `DIVISION_BY_ZERO`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, i256};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Operator, ScalarUDF};

use super::support::{Call, Func, is_decimal, is_integer, not_found, out_of_range};

const MAX_PRECISION: u8 = 38;

/// Decimal form of an integer or decimal type.
pub(crate) fn as_decimal(t: &DataType) -> Option<(u8, i8)> {
    match t {
        DataType::Int8 | DataType::UInt8 => Some((3, 0)),
        DataType::Int16 | DataType::UInt16 => Some((5, 0)),
        DataType::Int32 | DataType::UInt32 => Some((10, 0)),
        DataType::Int64 | DataType::Null => Some((19, 0)),
        DataType::Decimal128(p, s) => Some((*p, *s)),
        _ => None,
    }
}

/// True when `left op right` is decimal arithmetic in Trino.
pub(crate) fn is_decimal_arithmetic(left: &DataType, op: Operator, right: &DataType) -> bool {
    matches!(
        op,
        Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide | Operator::Modulo
    ) && (is_decimal(left) || is_decimal(right))
        && (is_decimal(left) || is_integer(left))
        && (is_decimal(right) || is_integer(right))
        && as_decimal(left).is_some()
        && as_decimal(right).is_some()
}

/// Trino's common super type of a decimal and a decimal or integer:
/// `decimal(max(p1 - s1, p2 - s2) + max(s1, s2), max(s1, s2))`.
pub(crate) fn common_decimal(left: &DataType, right: &DataType) -> Option<DataType> {
    if !(is_decimal(left) || is_decimal(right)) {
        return None;
    }
    let ((p1, s1), (p2, s2)) = (as_decimal(left)?, as_decimal(right)?);
    let scale = s1.max(s2);
    let integral = (i64::from(p1) - i64::from(s1)).max(i64::from(p2) - i64::from(s2));
    Some(DataType::Decimal128(narrow(integral + i64::from(scale)), scale))
}

fn narrow(value: i64) -> u8 {
    u8::try_from(value.clamp(1, i64::from(MAX_PRECISION))).unwrap_or(MAX_PRECISION)
}

/// Trino's result type of `(p1, s1) op (p2, s2)`.
pub(crate) fn result_type(op: Operator, (p1, s1): (u8, i8), (p2, s2): (u8, i8)) -> Result<(u8, i8)> {
    let (p1, s1, p2, s2) = (i64::from(p1), i64::from(s1), i64::from(p2), i64::from(s2));
    let scale = s1.max(s2);
    let (precision, scale) = match op {
        Operator::Plus | Operator::Minus => (1 + scale + (p1 - s1).max(p2 - s2), scale),
        Operator::Multiply => (p1 + p2, s1 + s2),
        Operator::Divide => (p1 - s1 + s2 + scale, scale),
        Operator::Modulo => ((p2 - s2).min(p1 - s1) + scale, scale),
        other => {
            return Err(DataFusionError::Internal(format!(
                "not a decimal operator: {other}"
            )));
        }
    };
    if scale > i64::from(MAX_PRECISION) {
        return Err(out_of_range(format!(
            "DECIMAL scale {scale} is larger than {MAX_PRECISION}"
        )));
    }
    let precision = narrow(precision);
    let scale = i8::try_from(scale)
        .unwrap_or(0)
        .min(i8::try_from(precision).unwrap_or(i8::MAX));
    Ok((precision, scale))
}

fn pow10(exponent: i64) -> Option<i256> {
    i256::from_i128(10).checked_pow(u32::try_from(exponent).ok()?)
}

fn overflow() -> DataFusionError {
    out_of_range("Decimal overflow")
}

/// `a op b` on unscaled values, result at `(precision, scale)`.
fn apply(op: Operator, a: i128, s1: i8, b: i128, s2: i8, (precision, scale): (u8, i8)) -> Result<i128> {
    let (a, b) = (i256::from_i128(a), i256::from_i128(b));
    let (s1, s2, scale_64) = (i64::from(s1), i64::from(s2), i64::from(scale));
    let up = |v: i256, digits: i64| pow10(digits).and_then(|f| v.checked_mul(f)).ok_or_else(overflow);
    let value = match op {
        Operator::Plus | Operator::Minus => {
            let (x, y) = (up(a, scale_64 - s1)?, up(b, scale_64 - s2)?);
            if op == Operator::Plus {
                x.checked_add(y)
            } else {
                x.checked_sub(y)
            }
            .ok_or_else(overflow)?
        }
        Operator::Multiply => a.checked_mul(b).ok_or_else(overflow)?,
        Operator::Divide | Operator::Modulo if b == i256::ZERO => {
            return Err(DataFusionError::Execution(
                "DIVISION_BY_ZERO: Division by zero".into(),
            ));
        }
        Operator::Divide => {
            let dividend = up(a, scale_64 - s1 + s2)?;
            let quotient = dividend.checked_div(b).ok_or_else(overflow)?;
            let remainder = dividend.checked_rem(b).ok_or_else(overflow)?;
            // Half up: away from zero on ties.
            let twice = remainder
                .checked_mul(i256::from_i128(2))
                .ok_or_else(overflow)?
                .wrapping_abs();
            if twice >= b.wrapping_abs() {
                let negative = a.is_negative() != b.is_negative();
                if negative {
                    quotient.checked_sub(i256::ONE)
                } else {
                    quotient.checked_add(i256::ONE)
                }
                .ok_or_else(overflow)?
            } else {
                quotient
            }
        }
        Operator::Modulo => {
            let (x, y) = (up(a, scale_64 - s1)?, up(b, scale_64 - s2)?);
            x.checked_rem(y).ok_or_else(overflow)?
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "not a decimal operator: {other}"
            )));
        }
    };
    let limit = pow10(i64::from(precision)).ok_or_else(overflow)?;
    if value.wrapping_abs() >= limit {
        return Err(overflow());
    }
    value.to_i128().ok_or_else(overflow)
}

fn operator_of(name: &str) -> Option<Operator> {
    Some(match name {
        "codelake_trino_decimal_add" => Operator::Plus,
        "codelake_trino_decimal_subtract" => Operator::Minus,
        "codelake_trino_decimal_multiply" => Operator::Multiply,
        "codelake_trino_decimal_divide" => Operator::Divide,
        "codelake_trino_decimal_modulus" => Operator::Modulo,
        _ => return None,
    })
}

/// Function name for a decimal operator.
pub(crate) fn function_name(op: Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Plus => "codelake_trino_decimal_add",
        Operator::Minus => "codelake_trino_decimal_subtract",
        Operator::Multiply => "codelake_trino_decimal_multiply",
        Operator::Divide => "codelake_trino_decimal_divide",
        Operator::Modulo => "codelake_trino_decimal_modulus",
        _ => return None,
    })
}

fn decimals(array: &ArrayRef) -> Result<Decimal128Array> {
    let (p, s) = as_decimal(array.data_type())
        .ok_or_else(|| DataFusionError::Internal(format!("not a decimal: {}", array.data_type())))?;
    let widened = cast(array, &DataType::Decimal128(p, s))?;
    widened
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("decimal".into()))
}

fn invoke(name: &str, call: &Call<'_>) -> Result<ArrayRef> {
    let op = operator_of(name).ok_or_else(|| DataFusionError::Internal(format!("unknown {name}")))?;
    let DataType::Decimal128(precision, scale) = call.return_type else {
        return Err(DataFusionError::Internal("decimal result".into()));
    };
    let (a, b) = (decimals(call.arg(0)?)?, decimals(call.arg(1)?)?);
    let (s1, s2) = (a.scale(), b.scale());
    let out = (0..call.rows)
        .map(|i| {
            if a.is_null(i) || b.is_null(i) {
                return Ok(None);
            }
            apply(op, a.value(i), s1, b.value(i), s2, (*precision, *scale)).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        Decimal128Array::from(out).with_precision_and_scale(*precision, *scale)?,
    ))
}

fn decimal_function(name: &'static str, invoke: super::support::InvokeFn) -> ScalarUDF {
    Func::new(
        name,
        |name, types| match types {
            [l, r] if as_decimal(l).is_some() && as_decimal(r).is_some() => {
                let widen = |t: &DataType| {
                    let (p, s) = as_decimal(t).unwrap_or((19, 0));
                    DataType::Decimal128(p, s)
                };
                Ok(vec![widen(l), widen(r)])
            }
            _ => Err(not_found(name, types)),
        },
        |r| match r.types {
            [DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)] => {
                let op = operator_of(r.name).ok_or_else(|| DataFusionError::Internal("decimal op".into()))?;
                let (p, s) = result_type(op, (*p1, *s1), (*p2, *s2))?;
                Ok(DataType::Decimal128(p, s))
            }
            _ => Err(DataFusionError::Internal("decimal operands".into())),
        },
        invoke,
    )
    .udf()
}

/// Checked integer function for `op` (constant folding, see `rewrite`).
pub(crate) fn checked_integer_name(op: Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Plus => "codelake_trino_checked_add",
        Operator::Minus => "codelake_trino_checked_subtract",
        Operator::Multiply => "codelake_trino_checked_multiply",
        _ => return None,
    })
}

fn integer_width(t: &DataType) -> u8 {
    match t {
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        _ => 8,
    }
}

fn checked_integer(name: &'static str, invoke: super::support::InvokeFn) -> ScalarUDF {
    Func::new(
        name,
        |name, types| match types {
            [l, r] if is_integer(l) && is_integer(r) => {
                let wider = if integer_width(l) >= integer_width(r) {
                    l.clone()
                } else {
                    r.clone()
                };
                Ok(vec![wider.clone(), wider])
            }
            _ => Err(not_found(name, types)),
        },
        |r| {
            r.types
                .first()
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("integer operands".into()))
        },
        invoke,
    )
    .udf()
}

fn checked_op(call: &Call<'_>, op: Operator) -> Result<ArrayRef> {
    use datafusion::arrow::compute::kernels::numeric::{add, mul, sub};
    let (l, r) = (call.arg(0)?, call.arg(1)?);
    let out = match op {
        Operator::Plus => add(l, r),
        Operator::Minus => sub(l, r),
        _ => mul(l, r),
    };
    out.map_err(|e| {
        out_of_range(format!(
            "{} overflow: {e}",
            super::support::trino_type_name(l.data_type())
        ))
    })
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        checked_integer("codelake_trino_checked_add", |c| checked_op(c, Operator::Plus)),
        checked_integer("codelake_trino_checked_subtract", |c| {
            checked_op(c, Operator::Minus)
        }),
        checked_integer("codelake_trino_checked_multiply", |c| {
            checked_op(c, Operator::Multiply)
        }),
        decimal_function("codelake_trino_decimal_add", |c| {
            invoke("codelake_trino_decimal_add", c)
        }),
        decimal_function("codelake_trino_decimal_subtract", |c| {
            invoke("codelake_trino_decimal_subtract", c)
        }),
        decimal_function("codelake_trino_decimal_multiply", |c| {
            invoke("codelake_trino_decimal_multiply", c)
        }),
        decimal_function("codelake_trino_decimal_divide", |c| {
            invoke("codelake_trino_decimal_divide", c)
        }),
        decimal_function("codelake_trino_decimal_modulus", |c| {
            invoke("codelake_trino_decimal_modulus", c)
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_types_follow_trino() {
        assert_eq!(result_type(Operator::Plus, (3, 2), (3, 2)).unwrap(), (4, 2));
        assert_eq!(result_type(Operator::Plus, (2, 1), (10, 0)).unwrap(), (12, 1));
        assert_eq!(result_type(Operator::Multiply, (3, 2), (2, 1)).unwrap(), (5, 3));
        assert_eq!(result_type(Operator::Multiply, (4, 2), (10, 0)).unwrap(), (14, 2));
        assert_eq!(result_type(Operator::Divide, (1, 0), (1, 0)).unwrap(), (1, 0));
        assert_eq!(result_type(Operator::Divide, (19, 0), (5, 1)).unwrap(), (21, 1));
        assert_eq!(result_type(Operator::Modulo, (2, 1), (10, 0)).unwrap(), (2, 1));
    }

    #[test]
    fn quotients_round_half_up() {
        assert_eq!(apply(Operator::Divide, 1, 0, 3, 0, (1, 0)).unwrap(), 0);
        assert_eq!(apply(Operator::Divide, 2, 0, 3, 0, (1, 0)).unwrap(), 1);
        assert_eq!(apply(Operator::Divide, -2, 0, 3, 0, (1, 0)).unwrap(), -1);
        assert_eq!(apply(Operator::Divide, 5400, 0, 36_000, 1, (21, 1)).unwrap(), 15);
        assert_eq!(apply(Operator::Divide, 100, 0, 36_000, 1, (21, 1)).unwrap(), 0);
        assert!(apply(Operator::Divide, 1, 0, 0, 0, (1, 0)).is_err());
        assert_eq!(apply(Operator::Modulo, 75, 1, 2, 0, (2, 1)).unwrap(), 15);
        assert_eq!(apply(Operator::Multiply, 110, 2, 25, 1, (5, 3)).unwrap(), 2750);
        assert!(
            apply(Operator::Plus, 9, 0, 1, 0, (1, 0)).is_err(),
            "overflow of the precision"
        );
    }
}
