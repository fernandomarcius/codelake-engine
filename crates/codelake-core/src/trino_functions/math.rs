//! Mathematical and comparison functions (Trino "Mathematical functions",
//! "Comparison functions").
//!
//! Differences from `DataFusion` that matter: `round`/`ceil`/`floor`/`abs`/
//! `sign` keep integer types, `round` of a decimal follows Trino's result
//! type and `HALF_UP`, `abs` overflows loudly, `power` is always `double`, and
//! `greatest`/`least` return NULL when any argument is NULL.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array};
use datafusion::arrow::compute::kernels::cmp::{gt, lt};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::{cast, nullif, or_kleene};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use num_traits::ToPrimitive;

use super::support::{
    At, BIGINT, BOOLEAN, Call, DOUBLE, Func, as_f64, as_i64, as_str, bool_array, coerce_any, coerce_each,
    float_array, floats, int_array, ints, invalid_argument, is_float, is_integer, not_found, out_of_range,
    string_array, strings,
};

fn first_type(r: &super::support::Ret<'_>) -> Result<DataType> {
    r.types
        .first()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
}

/// Integer, float or decimal argument, kept as is (`NULL` becomes BIGINT).
fn numeric_same(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..) => Some(t.clone()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::Null => Some(DataType::Int64),
        _ => None,
    }
}

fn integer_range(t: &DataType) -> (i64, i64) {
    match t {
        DataType::Int8 => (i64::from(i8::MIN), i64::from(i8::MAX)),
        DataType::Int16 => (i64::from(i16::MIN), i64::from(i16::MAX)),
        DataType::Int32 => (i64::from(i32::MIN), i64::from(i32::MAX)),
        _ => (i64::MIN, i64::MAX),
    }
}

/// Applies an `i64` function to an integer column of any width, checking the range.
fn map_integers(array: &ArrayRef, rows: usize, f: impl Fn(i64) -> Result<i64>) -> Result<ArrayRef> {
    let target = array.data_type().clone();
    let (min, max) = integer_range(&target);
    let values = cast(array, &DataType::Int64)?;
    let values = ints(&values)?;
    let out = (0..rows)
        .map(|i| {
            values
                .at(i)
                .map(|v| {
                    let r = f(v)?;
                    if (min..=max).contains(&r) {
                        Ok(r)
                    } else {
                        Err(out_of_range(format!(
                            "Value {r} is out of range for {}",
                            super::support::trino_type_name(&target)
                        )))
                    }
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(cast(&int_array(out), &target)?)
}

fn map_floats(array: &ArrayRef, rows: usize, f: impl Fn(f64) -> f64) -> Result<ArrayRef> {
    let target = array.data_type().clone();
    let values = cast(array, &DataType::Float64)?;
    let values = floats(&values)?;
    let out = (0..rows).map(|i| values.at(i).map(&f)).collect();
    Ok(cast(&float_array(out), &target)?)
}

fn decimals(array: &ArrayRef) -> Result<&Decimal128Array> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| DataFusionError::Internal("expected Decimal128".into()))
}

fn decimal_array(values: Vec<Option<i128>>, precision: u8, scale: i8) -> Result<ArrayRef> {
    Ok(Arc::new(
        Decimal128Array::from(values).with_precision_and_scale(precision, scale)?,
    ))
}

fn pow10(exp: u32) -> i128 {
    10_i128.checked_pow(exp).unwrap_or(i128::MAX)
}

/// Rounds `value` (scaled by `from_scale`) to `to_scale` digits, `HALF_UP`.
fn rescale_half_up(value: i128, from_scale: i8, to_scale: i8) -> Option<i128> {
    if to_scale >= from_scale {
        return value.checked_mul(pow10(u32::try_from(to_scale - from_scale).ok()?));
    }
    let factor = pow10(u32::try_from(from_scale - to_scale).ok()?);
    let (q, r) = (value / factor, value % factor);
    Some(if r.abs() * 2 >= factor {
        q + value.signum()
    } else {
        q
    })
}

/// Floor/ceiling/truncation of a scaled decimal to `to_scale` digits.
fn rescale_with(value: i128, from_scale: i8, to_scale: i8, mode: Rounding) -> Option<i128> {
    if to_scale >= from_scale {
        return rescale_half_up(value, from_scale, to_scale);
    }
    let factor = pow10(u32::try_from(from_scale - to_scale).ok()?);
    let (q, r) = (value / factor, value % factor);
    Some(match mode {
        Rounding::HalfUp => return rescale_half_up(value, from_scale, to_scale),
        Rounding::Down => q,
        Rounding::Floor => {
            if r < 0 {
                q - 1
            } else {
                q
            }
        }
        Rounding::Ceiling => {
            if r > 0 {
                q + 1
            } else {
                q
            }
        }
    })
}

#[derive(Debug, Clone, Copy)]
enum Rounding {
    HalfUp,
    Down,
    Floor,
    Ceiling,
}

/// Trino `decimal(p - s + min(s, 1), 0)` for `round`/`ceil`/`floor` without digits.
fn integral_decimal(t: &DataType) -> DataType {
    match t {
        DataType::Decimal128(p, s) => {
            let s_pos = i16::from(*s).max(0);
            let precision = i16::from(*p) - s_pos + s_pos.min(1);
            DataType::Decimal128(u8::try_from(precision.clamp(1, 38)).unwrap_or(38), 0)
        }
        other => other.clone(),
    }
}

fn round_decimal_column(
    array: &ArrayRef,
    rows: usize,
    target: &DataType,
    digits: Option<&Int64Array>,
    mode: Rounding,
) -> Result<ArrayRef> {
    let values = decimals(array)?;
    let scale = values.scale();
    let DataType::Decimal128(precision, out_scale) = target else {
        return Err(DataFusionError::Internal("decimal target expected".into()));
    };
    let out = (0..rows)
        .map(|i| {
            if values.is_null(i) {
                return Ok(None);
            }
            let v = values.value(i);
            let rounded = match digits {
                None => rescale_with(v, scale, 0, mode),
                Some(d) => {
                    let Some(d) = d.at(i) else { return Ok(None) };
                    let keep = i8::try_from(d.clamp(-38, 38)).unwrap_or(0);
                    if keep >= scale {
                        Some(v)
                    } else {
                        rescale_with(v, scale, keep, mode).and_then(|r| rescale_half_up(r, keep, scale))
                    }
                }
            };
            rounded.ok_or_else(|| out_of_range("decimal overflow")).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    decimal_array(out, *precision, *out_scale)
}

/// Java `Math.round`-based rounding Trino applies to doubles: ties away from zero.
fn round_double(value: f64, digits: i64) -> f64 {
    if value.is_nan() || value.is_infinite() {
        return value;
    }
    let Ok(exp) = i32::try_from(digits.clamp(-400, 400)) else {
        return value;
    };
    let factor = 10_f64.powi(exp);
    let scaled = value * factor;
    if !scaled.is_finite() {
        return value;
    }
    scaled.round() / factor
}

fn round_integer(value: i64, digits: i64) -> Result<i64> {
    if digits >= 0 {
        return Ok(value);
    }
    let Some(factor) = u32::try_from(-digits).ok().and_then(|d| 10_i64.checked_pow(d)) else {
        return Ok(0);
    };
    let (q, r) = (value / factor, value % factor);
    let rounded = if r.unsigned_abs() * 2 >= factor.unsigned_abs() {
        q + value.signum()
    } else {
        q
    };
    rounded
        .checked_mul(factor)
        .ok_or_else(|| out_of_range("rounding overflow"))
}

fn invoke_round(call: &Call<'_>, mode: Rounding) -> Result<ArrayRef> {
    let x = call.arg(0)?;
    let digits = match call.args.get(1) {
        Some(d) => Some(ints(d)?),
        None => None,
    };
    let digit_at = |i: usize| digits.map_or(Some(0), |d| d.at(i));
    match x.data_type() {
        DataType::Decimal128(..) => round_decimal_column(x, call.rows, call.return_type, digits, mode),
        t if is_float(t) => {
            let values = cast(x, &DataType::Float64)?;
            let values = floats(&values)?;
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(v), Some(d)) = (values.at(i), digit_at(i)) else {
                        return None;
                    };
                    Some(match mode {
                        Rounding::HalfUp => round_double(v, d),
                        Rounding::Down => v.trunc(),
                        Rounding::Floor => v.floor(),
                        Rounding::Ceiling => v.ceil(),
                    })
                })
                .collect();
            Ok(cast(&float_array(out), call.return_type)?)
        }
        _ => {
            let values = cast(x, &DataType::Int64)?;
            let values = ints(&values)?;
            let out = (0..call.rows)
                .map(|i| match (values.at(i), digit_at(i)) {
                    (Some(v), Some(d)) => match mode {
                        Rounding::HalfUp => round_integer(v, d).map(Some),
                        _ => Ok(Some(v)),
                    },
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(cast(&int_array(out), call.return_type)?)
        }
    }
}

pub(crate) fn round() -> ScalarUDF {
    Func::new(
        "round",
        |name, types| coerce_any(name, types, &[&[numeric_same], &[numeric_same, as_i64]]),
        |r| match (r.types.first(), r.types.len()) {
            (Some(DataType::Decimal128(p, s)), 2) => Ok(DataType::Decimal128((*p + 1).min(38), *s)),
            (Some(t), 1) => Ok(integral_decimal(t)),
            (Some(t), _) => Ok(t.clone()),
            _ => Err(DataFusionError::Internal("missing argument".into())),
        },
        |call| invoke_round(call, Rounding::HalfUp),
    )
    .udf()
}

fn floor_ceiling(name: &'static str, mode_ceiling: bool) -> ScalarUDF {
    let invoke = if mode_ceiling {
        |call: &Call<'_>| invoke_round(call, Rounding::Ceiling)
    } else {
        |call: &Call<'_>| invoke_round(call, Rounding::Floor)
    };
    let mut f = Func::new(
        name,
        |name, types| coerce_each(name, types, &[numeric_same]),
        |r| {
            r.types
                .first()
                .map(integral_decimal)
                .ok_or_else(|| DataFusionError::Internal("missing".into()))
        },
        invoke,
    );
    if mode_ceiling {
        f = f.alias("ceil");
    }
    f.udf()
}

pub(crate) fn truncate() -> ScalarUDF {
    Func::new(
        "truncate",
        |name, types| coerce_any(name, types, &[&[numeric_same], &[numeric_same, as_i64]]),
        |r| match (r.types.first(), r.types.len()) {
            (Some(DataType::Decimal128(p, s)), 1) => {
                let digits = (i16::from(*p) - i16::from(*s).max(0)).max(1);
                Ok(DataType::Decimal128(u8::try_from(digits).unwrap_or(38), 0))
            }
            (Some(t), _) => Ok(t.clone()),
            _ => Err(DataFusionError::Internal("missing argument".into())),
        },
        |call| invoke_round(call, Rounding::Down),
    )
    .udf()
}

pub(crate) fn abs() -> ScalarUDF {
    Func::new(
        "abs",
        |name, types| coerce_each(name, types, &[numeric_same]),
        first_type,
        |call| {
            let x = call.arg(0)?;
            match x.data_type() {
                DataType::Decimal128(p, s) => {
                    let values = decimals(x)?;
                    let out = (0..call.rows)
                        .map(|i| (!values.is_null(i)).then(|| values.value(i).abs()))
                        .collect();
                    decimal_array(out, *p, *s)
                }
                t if is_float(t) => map_floats(x, call.rows, f64::abs),
                _ => map_integers(x, call.rows, |v| {
                    v.checked_abs()
                        .ok_or_else(|| out_of_range(format!("Value {v} is out of range for abs()")))
                }),
            }
        },
    )
    .udf()
}

pub(crate) fn sign() -> ScalarUDF {
    Func::new(
        "sign",
        |name, types| coerce_each(name, types, &[numeric_same]),
        |r| match r.types.first() {
            Some(DataType::Decimal128(..)) => Ok(DataType::Decimal128(1, 0)),
            Some(t) => Ok(t.clone()),
            None => Err(DataFusionError::Internal("missing argument".into())),
        },
        |call| {
            let x = call.arg(0)?;
            match x.data_type() {
                DataType::Decimal128(..) => {
                    let values = decimals(x)?;
                    let out = (0..call.rows)
                        .map(|i| (!values.is_null(i)).then(|| values.value(i).signum()))
                        .collect();
                    decimal_array(out, 1, 0)
                }
                t if is_float(t) => {
                    map_floats(
                        x,
                        call.rows,
                        |v| if v == 0.0 || v.is_nan() { v } else { v.signum() },
                    )
                }
                _ => map_integers(x, call.rows, |v| Ok(v.signum())),
            }
        },
    )
    .udf()
}

fn map_double(call: &Call<'_>, f: impl Fn(f64) -> f64) -> Result<ArrayRef> {
    let x = floats(call.arg(0)?)?;
    Ok(float_array((0..call.rows).map(|i| x.at(i).map(&f)).collect()))
}

fn map_double2(call: &Call<'_>, f: impl Fn(f64, f64) -> f64) -> Result<ArrayRef> {
    let x = floats(call.arg(0)?)?;
    let y = floats(call.arg(1)?)?;
    Ok(float_array(
        (0..call.rows).map(|i| Some(f(x.at(i)?, y.at(i)?))).collect(),
    ))
}

pub(crate) fn power() -> ScalarUDF {
    Func::new(
        "power",
        |name, types| coerce_each(name, types, &[as_f64, as_f64]),
        DOUBLE,
        |call| map_double2(call, f64::powf),
    )
    .alias("pow")
    .udf()
}

fn predicates() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            "is_nan",
            |n, t| coerce_each(n, t, &[as_f64]),
            BOOLEAN,
            |call| {
                let x = floats(call.arg(0)?)?;
                Ok(bool_array(
                    (0..call.rows).map(|i| x.at(i).map(f64::is_nan)).collect(),
                ))
            },
        )
        .udf(),
        Func::new(
            "is_finite",
            |n, t| coerce_each(n, t, &[as_f64]),
            BOOLEAN,
            |call| {
                let x = floats(call.arg(0)?)?;
                Ok(bool_array(
                    (0..call.rows).map(|i| x.at(i).map(f64::is_finite)).collect(),
                ))
            },
        )
        .udf(),
        Func::new(
            "is_infinite",
            |n, t| coerce_each(n, t, &[as_f64]),
            BOOLEAN,
            |call| {
                let x = floats(call.arg(0)?)?;
                Ok(bool_array(
                    (0..call.rows).map(|i| x.at(i).map(f64::is_infinite)).collect(),
                ))
            },
        )
        .udf(),
    ]
}

/// Trigonometric functions: `double` for any numeric argument (the engine's
/// own versions keep `real` for some integer arguments).
fn trigonometry() -> Vec<ScalarUDF> {
    let unary: [(&'static str, super::support::InvokeFn); 8] = [
        ("sin", |c| map_double(c, f64::sin)),
        ("cos", |c| map_double(c, f64::cos)),
        ("tan", |c| map_double(c, f64::tan)),
        ("asin", |c| map_double(c, f64::asin)),
        ("acos", |c| map_double(c, f64::acos)),
        ("atan", |c| map_double(c, f64::atan)),
        ("cosh", |c| map_double(c, f64::cosh)),
        ("tanh", |c| map_double(c, f64::tanh)),
    ];
    let mut all: Vec<ScalarUDF> = unary
        .into_iter()
        .map(|(name, invoke)| Func::new(name, |n, t| coerce_each(n, t, &[as_f64]), DOUBLE, invoke).udf())
        .collect();
    all.push(
        Func::new(
            "atan2",
            |n, t| coerce_each(n, t, &[as_f64, as_f64]),
            DOUBLE,
            |c| map_double2(c, f64::atan2),
        )
        .udf(),
    );
    all
}

/// Constants, roots and logarithms.
fn constants_and_logs() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            "e",
            |n, t| coerce_each(n, t, &[]),
            DOUBLE,
            |call| Ok(float_array(vec![Some(std::f64::consts::E); call.rows.max(1)])),
        )
        .udf(),
        Func::new(
            "infinity",
            |n, t| coerce_each(n, t, &[]),
            DOUBLE,
            |call| Ok(float_array(vec![Some(f64::INFINITY); call.rows.max(1)])),
        )
        .udf(),
        Func::new(
            "nan",
            |n, t| coerce_each(n, t, &[]),
            DOUBLE,
            |call| Ok(float_array(vec![Some(f64::NAN); call.rows.max(1)])),
        )
        .udf(),
        Func::new(
            "cbrt",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::cbrt),
        )
        .udf(),
        Func::new(
            "ln",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::ln),
        )
        .udf(),
        Func::new(
            "log2",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::log2),
        )
        .udf(),
        Func::new(
            "log10",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::log10),
        )
        .udf(),
        Func::new(
            "log",
            |n, t| coerce_each(n, t, &[as_f64, as_f64]),
            DOUBLE,
            |call| map_double2(call, |b, x| x.ln() / b.ln()),
        )
        .udf(),
        Func::new(
            "exp",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::exp),
        )
        .udf(),
        Func::new(
            "sqrt",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::sqrt),
        )
        .udf(),
        Func::new(
            "degrees",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::to_degrees),
        )
        .udf(),
        Func::new(
            "radians",
            |n, t| coerce_each(n, t, &[as_f64]),
            DOUBLE,
            |call| map_double(call, f64::to_radians),
        )
        .udf(),
    ]
}

/// `random()` in [0, 1) or `random(n)` in [0, n).
pub(crate) fn random() -> ScalarUDF {
    Func::new(
        "random",
        |name, types| coerce_any(name, types, &[&[], &[numeric_same]]),
        |r| Ok(r.types.first().cloned().unwrap_or(DataType::Float64)),
        |call| {
            let rows = call.rows.max(1);
            let Some(bound) = call.args.first() else {
                return Ok(float_array((0..rows).map(|_| Some(uniform())).collect()));
            };
            let values = cast(bound, &DataType::Int64)?;
            let values = ints(&values)?;
            let out = (0..rows)
                .map(|i| {
                    values
                        .at(i)
                        .map(|n| {
                            if n <= 0 {
                                return Err(invalid_argument("bound must be positive"));
                            }
                            let pick = (uniform() * super::cast::i64_to_f64(n)).floor();
                            Ok(pick.to_i64().unwrap_or(0).min(n - 1))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(cast(&int_array(out), call.return_type)?)
        },
    )
    .alias("rand")
    .volatility(Volatility::Volatile)
    .udf()
}

/// A uniform double in [0, 1) from the process RNG behind `uuid`.
fn uniform() -> f64 {
    let bits = uuid::Uuid::new_v4().as_u64_pair().0 >> 11;
    super::cast::i64_to_f64(i64::try_from(bits).unwrap_or(0)) / 9_007_199_254_740_992.0
}

pub(crate) fn width_bucket() -> ScalarUDF {
    Func::new(
        "width_bucket",
        |name, types| coerce_each(name, types, &[as_f64, as_f64, as_f64, as_i64]),
        |_| Ok(DataType::Int64),
        |call| {
            let x = floats(call.arg(0)?)?;
            let low = floats(call.arg(1)?)?;
            let high = floats(call.arg(2)?)?;
            let n = ints(call.arg(3)?)?;
            let out = (0..call.rows)
                .map(|i| match (x.at(i), low.at(i), high.at(i), n.at(i)) {
                    (Some(x), Some(lo), Some(hi), Some(n)) => bucket(x, lo, hi, n).map(Some),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

fn bucket(x: f64, low: f64, high: f64, count: i64) -> Result<i64> {
    if count <= 0 {
        return Err(invalid_argument("bucketCount must be greater than 0"));
    }
    if x.is_nan() {
        return Err(invalid_argument("operand must not be NaN"));
    }
    if !low.is_finite() {
        return Err(invalid_argument("first bound must be finite"));
    }
    if !high.is_finite() {
        return Err(invalid_argument("second bound must be finite"));
    }
    if low.partial_cmp(&high) == Some(std::cmp::Ordering::Equal) {
        return Err(invalid_argument("bounds cannot equal each other"));
    }
    // Trino's `MathFunctions.widthBucket`: buckets over [min, max), then
    // mirrored when the first bound is the larger one.
    let (lower, upper) = (low.min(high), low.max(high));
    let result = if x < lower {
        0
    } else if x >= upper {
        count
            .checked_add(1)
            .ok_or_else(|| out_of_range("Bucket for value exceeds bucket count"))?
    } else {
        let n = super::cast::i64_to_f64(count);
        (n * (x - lower) / (upper - lower) + 1.0)
            .trunc()
            .to_i64()
            .unwrap_or(0)
    };
    Ok(if low > high { count - result + 1 } else { result })
}

pub(crate) fn to_base() -> ScalarUDF {
    Func::new(
        "to_base",
        |name, types| coerce_each(name, types, &[as_i64, as_i64]),
        |_| Ok(DataType::Utf8),
        |call| {
            let x = ints(call.arg(0)?)?;
            let radix = ints(call.arg(1)?)?;
            let out = (0..call.rows)
                .map(|i| match (x.at(i), radix.at(i)) {
                    (Some(v), Some(r)) => to_radix(v, r).map(Some),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

fn to_radix(value: i64, radix: i64) -> Result<String> {
    let radix = u32::try_from(radix)
        .ok()
        .filter(|r| (2..=36).contains(r))
        .ok_or_else(|| invalid_argument("Radix must be between 2 and 36"))?;
    let mut n = value.unsigned_abs();
    let mut digits = Vec::new();
    loop {
        let d = u32::try_from(n % u64::from(radix)).unwrap_or(0);
        digits.push(char::from_digit(d, radix).unwrap_or('0'));
        n /= u64::from(radix);
        if n == 0 {
            break;
        }
    }
    if value < 0 {
        digits.push('-');
    }
    Ok(digits.iter().rev().collect())
}

pub(crate) fn from_base() -> ScalarUDF {
    Func::new(
        "from_base",
        |name, types| coerce_each(name, types, &[as_str, as_i64]),
        |_| Ok(DataType::Int64),
        |call| {
            let s = strings(call.arg(0)?)?;
            let radix = ints(call.arg(1)?)?;
            let out = (0..call.rows)
                .map(|i| match (s.at(i), radix.at(i)) {
                    (Some(text), Some(r)) => {
                        let r = u32::try_from(r)
                            .ok()
                            .filter(|r| (2..=36).contains(r))
                            .ok_or_else(|| invalid_argument("Radix must be between 2 and 36"))?;
                        i64::from_str_radix(text, r)
                            .map(Some)
                            .map_err(|_| invalid_argument(format!("Not a valid base-{r} number: {text}")))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

fn map_int2(call: &Call<'_>, f: impl Fn(i64, i64) -> i64) -> Result<ArrayRef> {
    let a = ints(call.arg(0)?)?;
    let b = ints(call.arg(1)?)?;
    Ok(int_array(
        (0..call.rows).map(|i| Some(f(a.at(i)?, b.at(i)?))).collect(),
    ))
}

#[derive(Debug, Clone, Copy)]
enum Shift {
    Left,
    Right,
    Arithmetic,
}

/// Bits of a Trino integer type (shifts keep the argument's type).
fn width_of(t: &DataType) -> u32 {
    match t {
        DataType::Int8 => 8,
        DataType::Int16 => 16,
        DataType::Int32 => 32,
        _ => 64,
    }
}

/// Sign-extends the low `width` bits of `value`.
fn wrap_to(value: i64, width: u32) -> i64 {
    if width >= 64 {
        value
    } else {
        let unused = 64 - width;
        value.wrapping_shl(unused) >> unused
    }
}

/// Trino's `bitwise_*_shift` on a `width`-bit integer.
fn shift_value(value: i64, amount: i64, width: u32, kind: Shift) -> i64 {
    let amount = u32::try_from(amount).ok().filter(|a| *a < width);
    match (kind, amount) {
        (Shift::Left, Some(a)) => wrap_to(value.wrapping_shl(a), width),
        (Shift::Right, Some(a)) => {
            let mask = if width >= 64 {
                u64::MAX
            } else {
                (1_u64 << width) - 1
            };
            let unsigned = u64::from_ne_bytes(value.to_ne_bytes()) & mask;
            wrap_to(i64::from_ne_bytes((unsigned >> a).to_ne_bytes()), width)
        }
        (Shift::Arithmetic, Some(a)) => value >> a,
        (Shift::Arithmetic, None) if value < 0 => -1,
        _ => 0,
    }
}

fn shift_column(call: &Call<'_>, kind: Shift) -> Result<ArrayRef> {
    let width = width_of(call.return_type);
    let values = cast(call.arg(0)?, &DataType::Int64)?;
    let values = ints(&values)?;
    let amounts = ints(call.arg(1)?)?;
    Ok(int_array(
        (0..call.rows)
            .map(|i| Some(shift_value(values.at(i)?, amounts.at(i)?, width, kind)))
            .collect(),
    ))
}

fn shift_function(name: &'static str, invoke: super::support::InvokeFn) -> ScalarUDF {
    Func::new(
        name,
        |name, types| match types {
            [value, amount] if (is_integer(value) || value.is_null()) && as_i64(amount).is_some() => {
                let value = if value.is_null() {
                    DataType::Int64
                } else {
                    value.clone()
                };
                Ok(vec![value, DataType::Int64])
            }
            _ => Err(not_found(name, types)),
        },
        |r| {
            r.types
                .first()
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("shift argument".into()))
        },
        invoke,
    )
    .udf()
}

fn bitwise_functions() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            "bitwise_and",
            |n, t| coerce_each(n, t, &[as_i64, as_i64]),
            BIGINT,
            |c| map_int2(c, |a, b| a & b),
        )
        .udf(),
        Func::new(
            "bitwise_or",
            |n, t| coerce_each(n, t, &[as_i64, as_i64]),
            BIGINT,
            |c| map_int2(c, |a, b| a | b),
        )
        .udf(),
        Func::new(
            "bitwise_xor",
            |n, t| coerce_each(n, t, &[as_i64, as_i64]),
            BIGINT,
            |c| map_int2(c, |a, b| a ^ b),
        )
        .udf(),
        Func::new(
            "bitwise_not",
            |n, t| coerce_each(n, t, &[as_i64]),
            BIGINT,
            |c| {
                let a = ints(c.arg(0)?)?;
                Ok(int_array((0..c.rows).map(|i| a.at(i).map(|v| !v)).collect()))
            },
        )
        .udf(),
        shift_function("bitwise_left_shift", |c| shift_column(c, Shift::Left)),
        shift_function("bitwise_right_shift", |c| shift_column(c, Shift::Right)),
        shift_function("bitwise_right_shift_arithmetic", |c| {
            shift_column(c, Shift::Arithmetic)
        }),
        Func::new(
            "bit_count",
            |n, t| coerce_each(n, t, &[as_i64, as_i64]),
            BIGINT,
            |c| {
                let a = ints(c.arg(0)?)?;
                let bits = ints(c.arg(1)?)?;
                let out = (0..c.rows)
                    .map(|i| match (a.at(i), bits.at(i)) {
                        (Some(v), Some(b)) => bit_count(v, b).map(Some),
                        _ => Ok(None),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(int_array(out))
            },
        )
        .udf(),
    ]
}

fn bit_count(value: i64, bits: i64) -> Result<i64> {
    if !(2..=64).contains(&bits) {
        return Err(invalid_argument(
            "Bits specified in bit_count must be between 2 and 64, got ".to_owned() + &bits.to_string(),
        ));
    }
    let low = -(1_i128 << (bits - 1));
    let high = (1_i128 << (bits - 1)) - 1;
    if !(low..=high).contains(&i128::from(value)) {
        return Err(invalid_argument(format!(
            "Number must be representable with the bits specified. {value} cannot be represented with {bits} bits"
        )));
    }
    let mask: u64 = if bits == 64 { u64::MAX } else { (1_u64 << bits) - 1 };
    let raw = u64::from_ne_bytes(value.to_ne_bytes()) & mask;
    Ok(i64::from(raw.count_ones()))
}

// ---------------------------------------------------------------------------
// greatest / least
// ---------------------------------------------------------------------------

fn coerce_extreme(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    if types.is_empty() {
        return Err(not_found(name, types));
    }
    let mut common: Option<DataType> = None;
    for t in types {
        if t.is_null() {
            continue;
        }
        common = Some(match common {
            None => t.clone(),
            Some(c) => super::decimal::common_decimal(&c, t)
                .or_else(|| comparison_coercion(&c, t))
                .ok_or_else(|| not_found(name, types))?,
        });
    }
    let common = common.unwrap_or(DataType::Int64);
    let common = match common {
        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
        other => other,
    };
    Ok(vec![common; types.len()])
}

fn extreme(call: &Call<'_>, greatest: bool) -> Result<ArrayRef> {
    let first = call.arg(0)?.clone();
    let any_null = call
        .args
        .iter()
        .try_fold(None::<datafusion::arrow::array::BooleanArray>, |acc, a| {
            let null = datafusion::arrow::compute::is_null(a)?;
            Ok::<_, DataFusionError>(Some(match acc {
                None => null,
                Some(prev) => or_kleene(&prev, &null)?,
            }))
        })?;
    let result = if is_float(first.data_type()) {
        let columns = call
            .args
            .iter()
            .map(|a| cast(a, &DataType::Float64))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let columns = columns
            .iter()
            .map(floats)
            .collect::<Result<Vec<&Float64Array>>>()?;
        let out = (0..call.rows)
            .map(|i| {
                let values: Vec<f64> = columns.iter().filter_map(|c| c.at(i)).collect();
                let finite: Vec<f64> = values.iter().copied().filter(|v| !v.is_nan()).collect();
                if finite.is_empty() {
                    return values.first().copied();
                }
                let pick = if greatest {
                    finite.iter().copied().fold(f64::NEG_INFINITY, f64::max)
                } else {
                    finite.iter().copied().fold(f64::INFINITY, f64::min)
                };
                Some(pick)
            })
            .collect();
        cast(&float_array(out), first.data_type())?
    } else {
        let mut result = first;
        for next in call.args.iter().skip(1) {
            let take = if greatest {
                gt(next, &result)?
            } else {
                lt(next, &result)?
            };
            let take = super::support::nulls_as_false(&take);
            result = zip(&take, next, &result)?;
        }
        result
    };
    match any_null {
        Some(mask) => Ok(nullif(&result, &mask)?),
        None => Ok(result),
    }
}

pub(crate) fn greatest() -> ScalarUDF {
    Func::new("greatest", coerce_extreme, first_type, |call| extreme(call, true)).udf()
}

pub(crate) fn least() -> ScalarUDF {
    Func::new("least", coerce_extreme, first_type, |call| extreme(call, false)).udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    let mut all = vec![
        round(),
        floor_ceiling("ceiling", true),
        floor_ceiling("floor", false),
        truncate(),
        abs(),
        sign(),
        power(),
        random(),
        width_bucket(),
        to_base(),
        from_base(),
        greatest(),
        least(),
    ];
    all.extend(predicates());
    all.extend(constants_and_logs());
    all.extend(trigonometry());
    all.extend(bitwise_functions());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_rounding_is_half_up() {
        assert_eq!(rescale_half_up(125, 2, 1), Some(13));
        assert_eq!(rescale_half_up(-125, 2, 1), Some(-13));
        assert_eq!(rescale_half_up(124, 2, 1), Some(12));
        assert_eq!(rescale_with(-125, 2, 0, Rounding::Floor), Some(-2));
        assert_eq!(rescale_with(125, 2, 0, Rounding::Ceiling), Some(2));
        assert_eq!(rescale_with(-125, 2, 0, Rounding::Down), Some(-1));
        assert_eq!(
            integral_decimal(&DataType::Decimal128(18, 6)),
            DataType::Decimal128(13, 0)
        );
        assert_eq!(
            integral_decimal(&DataType::Decimal128(5, 0)),
            DataType::Decimal128(5, 0)
        );
    }

    #[test]
    fn integer_and_double_rounding() {
        assert_eq!(round_integer(1250, -2).unwrap(), 1300);
        assert_eq!(round_integer(-1250, -2).unwrap(), -1300);
        assert_eq!(round_integer(1249, -2).unwrap(), 1200);
        assert_eq!(round_double(2.5, 0).to_bits(), 3.0_f64.to_bits());
        assert_eq!(round_double(-2.5, 0).to_bits(), (-3.0_f64).to_bits());
        assert_eq!(
            round_double(1.005, 2).to_bits(),
            1.0_f64.to_bits(),
            "binary 1.005 is below the tie"
        );
    }

    #[test]
    fn shifts_keep_the_integer_width() {
        assert_eq!(shift_value(1, 63, 32, Shift::Left), 0);
        assert_eq!(shift_value(1, 31, 32, Shift::Left), i64::from(i32::MIN));
        assert_eq!(shift_value(-8, 1, 32, Shift::Right), 2_147_483_644);
        assert_eq!(shift_value(-8, 1, 64, Shift::Right), i64::MAX - 3);
        assert_eq!(shift_value(-1, 60, 32, Shift::Right), 0);
        assert_eq!(shift_value(-1, 60, 32, Shift::Arithmetic), -1);
        assert_eq!(shift_value(-8, 1, 64, Shift::Arithmetic), -4);
        assert_eq!(shift_value(5, -1, 64, Shift::Left), 0);
    }

    #[test]
    fn radix_and_bits() {
        assert_eq!(to_radix(255, 16).unwrap(), "ff");
        assert_eq!(to_radix(-10, 2).unwrap(), "-1010");
        assert!(to_radix(1, 1).is_err());
        assert_eq!(bit_count(-1, 64).unwrap(), 64);
        assert_eq!(bit_count(-1, 8).unwrap(), 8);
        assert!(bit_count(300, 8).is_err());
        assert_eq!(bucket(5.35, 0.024, 10.06, 5).unwrap(), 3);
        assert_eq!(bucket(-1.0, 0.0, 10.0, 5).unwrap(), 0);
        assert_eq!(bucket(10.0, 0.0, 10.0, 5).unwrap(), 6);
    }
}
