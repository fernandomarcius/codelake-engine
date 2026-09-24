//! Decimal and integer helpers of the function library.
//!
//! Decimal arithmetic itself (`+ - * / %` with the reference result types, rounding
//! and overflow) is the engine's, in `crate::overflow::decimal`: one
//! implementation for every query. Here: the decimal form of integer types
//! and the reference common decimal type (for functions such as `greatest`), and
//! the checked integer functions constant folding goes through (see
//! `rewrite`).

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
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
    ]
}
