//! Trino's casts to `decimal(p,s)` from text and from floating point.
//!
//! Trino 431 reads text with Java's `BigDecimal` grammar — an optional sign,
//! digits with an optional point, an optional exponent (`1e2`, `-1.5E-1`),
//! nothing else (no surrounding spaces) — and rounds half up to the scale.
//! Text that is not a number fails with `Cannot cast VARCHAR '<text>' to
//! DECIMAL(p, s). Value is not a number.`; a value with more than `p - s`
//! integral digits with `… Value too large.` (both `INVALID_CAST_ARGUMENT`).
//!
//! A `double` or `real` is converted through its shortest decimal text (what
//! Java prints), not its binary value, so `CAST(1.005E0 AS DECIMAL(5,2))` is
//! `1.01` (the binary value is `1.00499…`) and `CAST(CAST(0.1 AS REAL) AS
//! DECIMAL(20,18))` is `0.100000000000000000`. `NaN`, infinities and values
//! too large fail with `Cannot cast DOUBLE '<value>' to DECIMAL(p, s)`.
//!
//! Arrow's casts trim spaces, reject exponents, and convert the binary value.
//! [`check_cast`] replaces such a cast with [`ToDecimal`] where it runs in the
//! engine; [`uncheck_cast`] puts the cast back for a SQL source (see
//! [`crate::overflow`], which calls both around federation).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal128Array};
use datafusion::arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TryCast, Volatility,
};

/// Why a text is not a `decimal(p,s)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    NotANumber,
    TooLarge,
}

fn pow10(exp: u32) -> Option<i128> {
    10_i128.checked_pow(exp)
}

/// Digits (leading zeros removed) as an integer, `None` above 38 digits.
fn integer_of(digits: &[u8]) -> Option<i128> {
    digits
        .iter()
        .try_fold(0_i128, |acc, d| acc.checked_mul(10)?.checked_add(i128::from(*d)))
}

fn strip_leading_zeros(digits: &[u8]) -> &[u8] {
    let first = digits.iter().position(|d| *d != 0).unwrap_or(digits.len());
    &digits[first..]
}

/// `text` (Java `BigDecimal` grammar) as an unscaled `decimal(p,s)`, rounded
/// half up (away from zero).
fn parse_decimal(text: &str, precision: u8, scale: i8) -> Result<i128, Failure> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let negative = match bytes.first() {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    let mut digits: Vec<u8> = Vec::new();
    let mut fraction_digits: i64 = 0;
    let mut point = false;
    while let Some(&b) = bytes.get(i) {
        match b {
            b'0'..=b'9' => {
                digits.push(b - b'0');
                if point {
                    fraction_digits += 1;
                }
            }
            b'.' if !point => point = true,
            b'e' | b'E' => break,
            _ => return Err(Failure::NotANumber),
        }
        i += 1;
    }
    if digits.is_empty() {
        return Err(Failure::NotANumber);
    }
    let mut exponent: i64 = 0;
    if i < bytes.len() {
        // `e` or `E`, then a signed integer.
        let rest = &text[i + 1..];
        let unsigned = rest.strip_prefix(['+', '-']).unwrap_or(rest);
        if unsigned.is_empty() || !unsigned.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Failure::NotANumber);
        }
        exponent = rest.parse::<i32>().map_err(|_| Failure::NotANumber)?.into();
    }
    let significant = strip_leading_zeros(&digits);
    if significant.is_empty() {
        return Ok(0);
    }
    // value = significant × 10^(exponent - fraction_digits); unscaled = value × 10^scale.
    let shift = exponent - fraction_digits + i64::from(scale);
    let precision_digits = i64::from(precision);
    let limit = pow10(u32::from(precision)).ok_or(Failure::TooLarge)?;
    let unscaled = if shift >= 0 {
        let count = i64::try_from(significant.len()).unwrap_or(i64::MAX);
        if count.saturating_add(shift) > precision_digits {
            return Err(Failure::TooLarge);
        }
        let factor = pow10(u32::try_from(shift).map_err(|_| Failure::TooLarge)?).ok_or(Failure::TooLarge)?;
        integer_of(significant)
            .and_then(|v| v.checked_mul(factor))
            .ok_or(Failure::TooLarge)?
    } else {
        let dropped = usize::try_from(shift.unsigned_abs()).unwrap_or(usize::MAX);
        let (kept, round_digit) = if dropped > significant.len() {
            (&[][..], 0)
        } else {
            let at = significant.len() - dropped;
            (&significant[..at], significant[at])
        };
        let kept = strip_leading_zeros(kept);
        if kept.len() > usize::from(precision) {
            return Err(Failure::TooLarge);
        }
        let value = integer_of(kept).ok_or(Failure::TooLarge)?;
        if round_digit >= 5 { value + 1 } else { value }
    };
    if unscaled >= limit {
        return Err(Failure::TooLarge);
    }
    Ok(if negative { -unscaled } else { unscaled })
}

/// Java's `Double.toString` / `Float.toString` of a value whose shortest
/// decimal text Rust wrote as `{:e}` (`1.2345e2`): plain between `1e-3` and
/// `1e7` (`123.45`, `1.0`), scientific otherwise (`1.0E300`).
fn java_text(scientific: &str) -> String {
    match scientific {
        "NaN" => return "NaN".into(),
        "inf" => return "Infinity".into(),
        "-inf" => return "-Infinity".into(),
        _ => {}
    }
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((scientific, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let (sign, mantissa) = mantissa.strip_prefix('-').map_or(("", mantissa), |m| ("-", m));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if digits.chars().all(|c| c == '0') {
        return format!("{sign}0.0");
    }
    if (-3..7).contains(&exponent) {
        let point = exponent + 1;
        let (int, fraction) = if point <= 0 {
            let zeros = "0".repeat(usize::try_from(-point).unwrap_or(0));
            ("0".to_owned(), format!("{zeros}{digits}"))
        } else {
            let at = usize::try_from(point).unwrap_or(0);
            let padded = format!("{digits:0<at$}");
            (padded[..at].to_owned(), padded[at..].to_owned())
        };
        let fraction = if fraction.is_empty() {
            "0".to_owned()
        } else {
            fraction
        };
        format!("{sign}{int}.{fraction}")
    } else {
        let (first, rest) = digits.split_at(1);
        let rest = if rest.is_empty() { "0" } else { rest };
        format!("{sign}{first}.{rest}E{exponent}")
    }
}

/// What a [`ToDecimal`] converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Source {
    Text,
    Double,
    Real,
}

impl Source {
    fn of(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Self::Text),
            DataType::Float64 => Some(Self::Double),
            DataType::Float32 | DataType::Float16 => Some(Self::Real),
            _ => None,
        }
    }
}

/// `CAST(<text or floating point> AS decimal(p,s))` with Trino's semantics;
/// `TRY_CAST` when `safe` (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ToDecimal {
    precision: u8,
    scale: i8,
    safe: bool,
    signature: Signature,
}

impl ToDecimal {
    fn new(precision: u8, scale: i8, safe: bool) -> Self {
        Self {
            precision,
            scale,
            safe,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn target(&self) -> DataType {
        DataType::Decimal128(self.precision, self.scale)
    }

    /// The cast it replaces.
    fn cast(&self, arg: Expr) -> Expr {
        if self.safe {
            Expr::TryCast(TryCast::new(Box::new(arg), self.target()))
        } else {
            Expr::Cast(Cast::new(Box::new(arg), self.target()))
        }
    }

    fn convert(&self, source: Source, text: &str) -> Result<i128, ArrowError> {
        let (p, s) = (self.precision, self.scale);
        parse_decimal(text, p, s).map_err(|failure| {
            ArrowError::CastError(match (source, failure) {
                (Source::Text, Failure::NotANumber) => {
                    format!("Cannot cast VARCHAR '{text}' to DECIMAL({p}, {s}). Value is not a number.")
                }
                (Source::Text, Failure::TooLarge) => {
                    format!("Cannot cast VARCHAR '{text}' to DECIMAL({p}, {s}). Value too large.")
                }
                (Source::Double, _) => {
                    format!("Cannot cast DOUBLE '{}' to DECIMAL({p}, {s})", java_text(text))
                }
                (Source::Real, _) => format!("Cannot cast REAL '{}' to DECIMAL({p}, {s})", java_text(text)),
            })
        })
    }

    fn values(&self, array: &ArrayRef) -> Result<Vec<Option<i128>>> {
        let source = Source::of(array.data_type());
        let mut out = Vec::with_capacity(array.len());
        let mut push = |source: Source, text: Option<&str>| -> Result<()> {
            out.push(match text {
                None => None,
                Some(text) => match self.convert(source, text) {
                    Ok(value) => Some(value),
                    Err(_) if self.safe => None,
                    Err(error) => return Err(error.into()),
                },
            });
            Ok(())
        };
        match (source, array.data_type()) {
            (Some(Source::Text), DataType::Utf8) => {
                for text in array.as_string::<i32>() {
                    push(Source::Text, text)?;
                }
            }
            (Some(Source::Text), DataType::LargeUtf8) => {
                for text in array.as_string::<i64>() {
                    push(Source::Text, text)?;
                }
            }
            (Some(Source::Text), _) => {
                for text in array.as_string_view() {
                    push(Source::Text, text)?;
                }
            }
            (Some(Source::Double), _) => {
                for value in array.as_primitive::<Float64Type>() {
                    push(Source::Double, value.map(|v| format!("{v:e}")).as_deref())?;
                }
            }
            (Some(Source::Real), DataType::Float16) => {
                for value in array.as_primitive::<Float16Type>() {
                    push(
                        Source::Real,
                        value.map(|v| format!("{:e}", v.to_f32())).as_deref(),
                    )?;
                }
            }
            (Some(Source::Real), _) => {
                for value in array.as_primitive::<Float32Type>() {
                    push(Source::Real, value.map(|v| format!("{v:e}")).as_deref())?;
                }
            }
            (None, other) => return exec_err!("{} cannot convert {other}", self.name()),
        }
        Ok(out)
    }
}

impl ScalarUDFImpl for ToDecimal {
    fn name(&self) -> &'static str {
        if self.safe {
            "codelake_try_to_decimal"
        } else {
            "codelake_to_decimal"
        }
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [arg] = args else {
            return exec_err!("{} expects one argument", self.name());
        };
        Ok(self.cast(arg.clone()).schema_name().to_string())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("{} expects one argument", self.name());
        };
        let rows = match value {
            ColumnarValue::Scalar(_) => 1,
            ColumnarValue::Array(_) => args.number_rows,
        };
        let array = value.to_array(rows)?;
        let result: ArrayRef = Arc::new(
            Decimal128Array::from(self.values(&array)?)
                .with_precision_and_scale(self.precision, self.scale)?,
        );
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(result),
        })
    }
}

/// `CAST`/`TRY_CAST` of text or floating point to `decimal128` →
/// [`ToDecimal`].
pub(crate) fn check_cast(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let (arg, data_type, safe) = match &expr {
        Expr::Cast(Cast { expr: arg, field }) => (arg, field.data_type(), false),
        Expr::TryCast(TryCast { expr: arg, field }) => (arg, field.data_type(), true),
        _ => return Ok(Transformed::no(expr)),
    };
    let DataType::Decimal128(p, s) = *data_type else {
        return Ok(Transformed::no(expr));
    };
    if s < 0 || Source::of(&arg.get_type(schema)?).is_none() {
        return Ok(Transformed::no(expr));
    }
    let udf = ScalarUDF::new_from_impl(ToDecimal::new(p, s, safe));
    Ok(Transformed::yes(udf.call(vec![arg.as_ref().clone()])))
}

/// [`ToDecimal`] → the cast it replaces (for SQL sources).
pub(crate) fn uncheck_cast(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, mut args }) = expr else {
        return Transformed::no(expr);
    };
    if let Some(cast) = func.inner().downcast_ref::<ToDecimal>()
        && args.len() == 1
        && let Some(arg) = args.pop()
    {
        return Transformed::yes(cast.cast(arg));
    }
    Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str, p: u8, s: i8) -> Result<i128, Failure> {
        parse_decimal(text, p, s)
    }

    /// Values observed on Trino 431 (`CAST('<text>' AS DECIMAL(5,1))`).
    #[test]
    fn text_follows_java_big_decimal() {
        for (text, expected) in [
            ("+1.5", 15),
            ("1.", 10),
            (".5", 5),
            ("1e2", 1000),
            ("1E+2", 1000),
            ("-1.5E-1", -2),
            ("1.25", 13),
            ("-1.25", -13),
            ("0.05", 1),
            ("00012.50", 125),
            ("0e5", 0),
            ("-0.04", 0),
        ] {
            assert_eq!(dec(text, 5, 1), Ok(expected), "{text}");
        }
        for text in [
            " 12.5 ", "12.5 ", "", "1_000", "NaN", "1e", "1.2.3", "e5", "+", "-.", "1e2.5",
        ] {
            assert_eq!(dec(text, 5, 1), Err(Failure::NotANumber), "{text}");
        }
        assert_eq!(dec("99999", 5, 1), Err(Failure::TooLarge));
        assert_eq!(dec("9999.96", 5, 1), Err(Failure::TooLarge));
        assert_eq!(dec("9999.94", 5, 1), Ok(99_999));
        assert_eq!(dec("12.345", 38, 0), Ok(12));
        assert_eq!(dec("1e40", 38, 0), Err(Failure::TooLarge));
        assert_eq!(dec("1e-40", 38, 0), Ok(0));
        assert_eq!(
            dec("99999999999999999999999999999999999999", 38, 0),
            Ok(10_i128.pow(38) - 1)
        );
    }

    /// Trino 431 converts through the shortest text: `1.005E0` → `1.01`.
    #[test]
    fn floating_point_rounds_its_shortest_text() {
        let cases: [(f64, u8, i8, i128); 7] = [
            (1.005, 5, 2, 101),
            (2.675, 5, 2, 268),
            (0.125, 5, 2, 13),
            (-0.125, 5, 2, -13),
            (1.115, 5, 2, 112),
            (1e20, 38, 0, 10_i128.pow(20)),
            (
                1.234_567_890_123_456_7e25,
                38,
                2,
                1_234_567_890_123_456_600_000_000_000,
            ),
        ];
        for (value, p, s, expected) in cases {
            assert_eq!(dec(&format!("{value:e}"), p, s), Ok(expected), "{value}");
        }
        assert_eq!(dec(&format!("{:e}", 0.1_f32), 20, 18), Ok(10_i128.pow(17)));
        assert_eq!(dec(&format!("{:e}", 1e30_f32), 38, 0), Ok(10_i128.pow(30)));
        assert_eq!(dec(&format!("{:e}", 123_456.7), 5, 2), Err(Failure::TooLarge));
    }

    #[test]
    fn messages_print_values_like_java() {
        for (value, expected) in [
            (1e300, "1.0E300"),
            (123_456.7, "123456.7"),
            (1.0, "1.0"),
            (0.001, "0.001"),
            (0.000_1, "1.0E-4"),
            (1e7, "1.0E7"),
            (-2.5e-5, "-2.5E-5"),
            (f64::NAN, "NaN"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(java_text(&format!("{value:e}")), expected, "{value}");
        }
    }
}
