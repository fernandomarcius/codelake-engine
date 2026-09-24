//! Trino's casts between timestamps, dates and text.
//!
//! A `timestamp(p) with time zone` travels as an Arrow timestamp whose type
//! carries the zone, its value the UTC instant; a `timestamp(p)` as the local
//! time, as if in UTC. Arrow's casts keep the stored number, so they get
//! Trino's conversions wrong. Trino 431:
//!
//! - `CAST(<with time zone> AS timestamp(p))` is the local time in the
//!   value's zone (`2026-01-01 10:00 America/Sao_Paulo` → `10:00`; Arrow:
//!   `13:00`);
//! - `CAST(<timestamp or date> AS timestamp(p) with time zone)` reads the
//!   local time in the target zone (the session's, or the one `AT TIME ZONE`
//!   names), a time in a daylight-saving gap moving forward;
//! - a cast to a smaller precision rounds half up (Arrow truncates);
//! - `CAST(<timestamp> AS varchar)` is `2026-01-01 10:00:00.123`, and with a
//!   time zone `2019-02-16 23:30:00.000 America/Sao_Paulo` (Arrow: RFC 3339).
//!
//! [`check_cast`] replaces such casts with [`TimestampCast`] where they run in
//! the engine, [`uncheck_cast`] restores them for a SQL source (see
//! [`crate::overflow`]).
//!
//! `TRY_CAST(… AS TIMESTAMP WITH TIME ZONE)` has no SQL spelling for the
//! zone of the result, so the statement is rewritten to
//! `codelake_try_arrow_cast(x, '<Arrow type>')` ([`TryArrowCast`]), the
//! `TRY_CAST` counterpart of `arrow_cast`.

use std::any::Any;
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Field, FieldRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::temporal_conversions::NANOSECONDS;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TryCast, Volatility,
};

use crate::timestamps::{arrow_tz, offset_at_instant, offset_seconds, zone_label};

pub(crate) const TRY_ARROW_CAST: &str = "codelake_try_arrow_cast";

const NANOS_PER_SECOND: i128 = NANOSECONDS as i128;
const NANOS_PER_DAY: i128 = 86_400 * NANOS_PER_SECOND;

fn nanos_per(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => NANOS_PER_SECOND,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

pub(crate) fn digits_of(unit: TimeUnit) -> u32 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn out_of_range() -> ArrowError {
    ArrowError::CastError("Timestamp out of range".into())
}

/// Nanoseconds → `unit`, rounding half up.
fn to_unit(nanos: i128, unit: TimeUnit) -> Result<i64, ArrowError> {
    let per = nanos_per(unit);
    let value = if per == 1 {
        nanos
    } else {
        (nanos + per / 2).div_euclid(per)
    };
    i64::try_from(value).map_err(|_| out_of_range())
}

fn seconds_of(nanos: i128) -> Result<i64, ArrowError> {
    i64::try_from(nanos.div_euclid(NANOS_PER_SECOND)).map_err(|_| out_of_range())
}

/// Local time (as if UTC) of the instant `nanos` in `zone`.
fn local_of(nanos: i128, zone: &str) -> Result<i128, ArrowError> {
    let offset = offset_at_instant(arrow_tz(zone)?, seconds_of(nanos)?);
    Ok(nanos + i128::from(offset) * NANOS_PER_SECOND)
}

/// The instant of local time `nanos` (as if UTC) in `zone`: a time in a gap
/// moves forward, one in an overlap takes the earlier offset.
fn instant_of(nanos: i128, zone: &str) -> Result<i128, ArrowError> {
    let local = DateTime::from_timestamp(seconds_of(nanos)?, 0)
        .ok_or_else(out_of_range)?
        .naive_utc();
    let offset = offset_seconds(arrow_tz(zone)?, &local);
    Ok(nanos - i128::from(offset) * NANOS_PER_SECOND)
}

/// `YYYY-MM-DD HH:MM:SS[.fraction]` of local time `nanos`, `digits` digits.
pub(crate) fn timestamp_text(nanos: i128, digits: u32) -> Result<String, ArrowError> {
    let seconds = seconds_of(nanos)?;
    let fraction = nanos.rem_euclid(NANOS_PER_SECOND);
    let time = DateTime::from_timestamp(seconds, 0).ok_or_else(out_of_range)?;
    let mut text = time.format("%Y-%m-%d %H:%M:%S").to_string();
    if digits > 0 {
        let scaled = fraction / 10_i128.pow(9 - digits);
        let _ = write!(text, ".{scaled:0width$}", width = digits as usize);
    }
    Ok(text)
}

/// What a [`TimestampCast`] converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    /// With time zone → without: the local time in the value's zone.
    ZonedToLocal,
    /// Without time zone (or a date) → with: the local time placed in the zone.
    LocalToZoned,
    /// To a smaller precision, zone unchanged: rounded half up.
    Rescale,
    /// To text, as Trino prints it.
    ToText,
}

impl Kind {
    fn of(from: &DataType, to: &DataType) -> Option<Self> {
        use DataType::{Date32, Date64, LargeUtf8, Timestamp, Utf8, Utf8View};
        Some(match (from, to) {
            (Timestamp(_, Some(_)), Timestamp(_, None)) => Self::ZonedToLocal,
            (Timestamp(_, None) | Date32 | Date64, Timestamp(_, Some(_))) => Self::LocalToZoned,
            (Timestamp(from, _), Timestamp(to, _)) if nanos_per(*to) > nanos_per(*from) => Self::Rescale,
            (Timestamp(..), Utf8 | LargeUtf8 | Utf8View) => Self::ToText,
            _ => return None,
        })
    }
}

/// A timestamp cast with Trino's semantics (see the module documentation);
/// `TRY_CAST` when `safe`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct TimestampCast {
    kind: Kind,
    target: DataType,
    safe: bool,
    signature: Signature,
}

impl TimestampCast {
    fn new(kind: Kind, target: DataType, safe: bool) -> Self {
        Self {
            kind,
            target,
            safe,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// The cast it replaces.
    fn cast(&self, arg: Expr) -> Expr {
        if self.safe {
            Expr::TryCast(TryCast::new(Box::new(arg), self.target.clone()))
        } else {
            Expr::Cast(Cast::new(Box::new(arg), self.target.clone()))
        }
    }

    /// One value: nanoseconds of the source (instant or local time) → the
    /// target's value in its unit, or its text with `digits` fraction digits.
    fn convert(&self, nanos: i128, from_zone: Option<&str>, digits: u32) -> Result<Converted, ArrowError> {
        Ok(match (&self.kind, &self.target) {
            (Kind::ZonedToLocal, DataType::Timestamp(unit, None)) => {
                let zone = from_zone.unwrap_or("UTC");
                Converted::Value(to_unit(local_of(nanos, zone)?, *unit)?)
            }
            (Kind::LocalToZoned, DataType::Timestamp(unit, Some(zone))) => {
                Converted::Value(to_unit(instant_of(nanos, zone)?, *unit)?)
            }
            (Kind::Rescale, DataType::Timestamp(unit, _)) => Converted::Value(to_unit(nanos, *unit)?),
            (Kind::ToText, _) => Converted::Text(match from_zone {
                Some(zone) => format!(
                    "{} {}",
                    timestamp_text(local_of(nanos, zone)?, digits)?,
                    zone_label(zone)
                ),
                None => timestamp_text(nanos, digits)?,
            }),
            _ => {
                return Err(ArrowError::CastError(format!(
                    "{:?} cannot produce {}",
                    self.kind, self.target
                )));
            }
        })
    }
}

enum Converted {
    Value(i64),
    Text(String),
}

/// Nanoseconds of each value of a timestamp or date array, and the zone of
/// a timestamp with time zone.
pub(crate) fn source_nanos(array: &ArrayRef) -> Result<(Vec<Option<i128>>, Option<String>, u32)> {
    let scaled = |values: Vec<Option<i64>>, per: i128| -> Vec<Option<i128>> {
        values
            .into_iter()
            .map(|v| v.map(|v| i128::from(v) * per))
            .collect()
    };
    Ok(match array.data_type() {
        DataType::Timestamp(unit, zone) => {
            let values: Vec<Option<i64>> = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().iter().collect(),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().iter().collect(),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().iter().collect(),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().iter().collect(),
            };
            (
                scaled(values, nanos_per(*unit)),
                zone.as_deref().map(str::to_owned),
                digits_of(*unit),
            )
        }
        DataType::Date32 => {
            let days: Vec<Option<i64>> = array
                .as_primitive::<Date32Type>()
                .iter()
                .map(|d| d.map(i64::from))
                .collect();
            (scaled(days, NANOS_PER_DAY), None, 0)
        }
        DataType::Date64 => {
            let millis: Vec<Option<i64>> = array.as_primitive::<Date64Type>().iter().collect();
            (scaled(millis, 1_000_000), None, 0)
        }
        other => return exec_err!("expected a timestamp or a date, got {other}"),
    })
}

impl ScalarUDFImpl for TimestampCast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        if self.safe {
            "codelake_try_timestamp_cast"
        } else {
            "codelake_timestamp_cast"
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
        Ok(self.target.clone())
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
        let (nanos, zone, digits) = source_nanos(&array)?;
        let mut numbers: Vec<Option<i64>> = Vec::new();
        let mut texts: Vec<Option<String>> = Vec::new();
        for value in nanos {
            let converted = match value {
                None => None,
                Some(nanos) => match self.convert(nanos, zone.as_deref(), digits) {
                    Ok(converted) => Some(converted),
                    Err(_) if self.safe => None,
                    Err(error) => return Err(error.into()),
                },
            };
            match converted {
                Some(Converted::Text(text)) => texts.push(Some(text)),
                Some(Converted::Value(v)) => numbers.push(Some(v)),
                None if self.kind == Kind::ToText => texts.push(None),
                None => numbers.push(None),
            }
        }
        let result: ArrayRef = if self.kind == Kind::ToText {
            cast_with_options(
                &(Arc::new(StringArray::from(texts)) as ArrayRef),
                &self.target,
                &CastOptions::default(),
            )?
        } else {
            let DataType::Timestamp(unit, zone) = &self.target else {
                return exec_err!("{} returns a timestamp, not {}", self.name(), self.target);
            };
            crate::timestamps::timestamps(*unit, numbers, zone.clone())
        };
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(result),
        })
    }
}

/// `arrow_cast(x, '<type>')` as a `TRY_CAST`: `NULL` where the cast fails.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct TryArrowCast {
    signature: Signature,
}

pub(crate) fn try_arrow_cast() -> ScalarUDF {
    ScalarUDF::new_from_impl(TryArrowCast {
        signature: Signature::any(2, Volatility::Immutable),
    })
}

/// The type named by the second argument of `arrow_cast`-like functions.
fn named_type(scalar: Option<&ScalarValue>) -> Result<DataType> {
    let Some(
        ScalarValue::Utf8(Some(name))
        | ScalarValue::Utf8View(Some(name))
        | ScalarValue::LargeUtf8(Some(name)),
    ) = scalar
    else {
        return plan_err!("{TRY_ARROW_CAST} expects a type name as its second argument");
    };
    Ok(DataType::from_str(name)?)
}

impl ScalarUDFImpl for TryArrowCast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        TRY_ARROW_CAST
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("{TRY_ARROW_CAST} is typed by its arguments")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let data_type = named_type(args.scalar_arguments.get(1).copied().flatten())?;
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(value) = args.args.first() else {
            return exec_err!("{TRY_ARROW_CAST} expects two arguments");
        };
        let options = CastOptions {
            safe: true,
            ..CastOptions::default()
        };
        let target = args.return_field.data_type();
        Ok(match value {
            ColumnarValue::Array(array) => ColumnarValue::Array(cast_with_options(array, target, &options)?),
            ColumnarValue::Scalar(scalar) => {
                ColumnarValue::Scalar(scalar.cast_to_with_options(target, &options)?)
            }
        })
    }
}

/// The cast an expression spells, if any: `CAST`, `TRY_CAST`,
/// `arrow_cast(x, '<type>')` or `codelake_try_arrow_cast(x, '<type>')` →
/// (argument, target, safe).
fn spelled_cast(expr: &Expr, schema: &DFSchema) -> Result<Option<(Expr, DataType, bool)>> {
    Ok(match expr {
        Expr::Cast(Cast { expr: arg, data_type }) => Some((arg.as_ref().clone(), data_type.clone(), false)),
        Expr::TryCast(TryCast { expr: arg, data_type }) => {
            Some((arg.as_ref().clone(), data_type.clone(), true))
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if (func.name() == "arrow_cast" || func.name() == TRY_ARROW_CAST) && args.len() == 2 =>
        {
            Some((
                args[0].clone(),
                expr.get_type(schema)?,
                func.name() == TRY_ARROW_CAST,
            ))
        }
        _ => None,
    })
}

/// A timestamp cast → [`TimestampCast`] (see the module documentation).
pub(crate) fn check_cast(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Some((arg, target, safe)) = spelled_cast(&expr, schema)? else {
        return Ok(Transformed::no(expr));
    };
    let source = arg.get_type(schema)?;
    let Some(kind) = Kind::of(&source, &target) else {
        // `codelake_try_arrow_cast` of anything else is a plain `TRY_CAST`.
        if let Expr::ScalarFunction(ScalarFunction { func, .. }) = &expr
            && func.name() == TRY_ARROW_CAST
        {
            return Ok(Transformed::yes(Expr::TryCast(TryCast::new(
                Box::new(arg),
                target,
            ))));
        }
        return Ok(Transformed::no(expr));
    };
    let udf = ScalarUDF::new_from_impl(TimestampCast::new(kind, target, safe));
    Ok(Transformed::yes(udf.call(vec![arg])))
}

/// [`TimestampCast`] → the cast it replaces (for SQL sources).
pub(crate) fn uncheck_cast(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, mut args }) = expr else {
        return Transformed::no(expr);
    };
    if let Some(cast) = func.inner().as_any().downcast_ref::<TimestampCast>()
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

    const HOUR: i128 = 3600 * NANOS_PER_SECOND;
    /// 2026-01-01 10:00:00 as if UTC.
    const TEN_AM: i128 = 1_767_261_600 * NANOS_PER_SECOND;

    fn cast(kind: Kind, target: DataType) -> TimestampCast {
        TimestampCast::new(kind, target, false)
    }

    fn value(converted: Converted) -> i64 {
        match converted {
            Converted::Value(v) => v,
            Converted::Text(t) => panic!("text {t}"),
        }
    }

    fn text(converted: Converted) -> String {
        match converted {
            Converted::Text(t) => t,
            Converted::Value(v) => panic!("value {v}"),
        }
    }

    /// Values observed on Trino 431.
    #[test]
    fn zoned_values_become_their_local_time() {
        let to_local = cast(
            Kind::ZonedToLocal,
            DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        // 10:00 America/Sao_Paulo is 13:00 UTC; its local time is 10:00.
        let converted = to_local
            .convert(TEN_AM + 3 * HOUR, Some("America/Sao_Paulo"), 3)
            .unwrap();
        assert_eq!(i128::from(value(converted)) * 1_000_000, TEN_AM);
        let converted = to_local
            .convert(TEN_AM - 9 * HOUR, Some("Asia/Tokyo"), 3)
            .unwrap();
        assert_eq!(i128::from(value(converted)) * 1_000_000, TEN_AM);
    }

    #[test]
    fn local_values_are_placed_in_the_zone() {
        let sao_paulo = DataType::Timestamp(TimeUnit::Millisecond, Some("America/Sao_Paulo".into()));
        let to_zoned = cast(Kind::LocalToZoned, sao_paulo);
        let converted = to_zoned.convert(TEN_AM, None, 3).unwrap();
        assert_eq!(i128::from(value(converted)) * 1_000_000, TEN_AM + 3 * HOUR);
        // 2018-11-04 00:30 does not exist in Sao Paulo: 01:30 -02:00, 03:30 UTC.
        let gap = 1_541_291_400 * NANOS_PER_SECOND;
        let converted = to_zoned.convert(gap, None, 3).unwrap();
        assert_eq!(i128::from(value(converted)) * 1_000_000, gap + 3 * HOUR);
    }

    #[test]
    fn smaller_precisions_round_half_up() {
        let millis = cast(Kind::Rescale, DataType::Timestamp(TimeUnit::Millisecond, None));
        assert_eq!(
            value(millis.convert(TEN_AM + 123_500_000, None, 6).unwrap()),
            1_767_261_600_124
        );
        assert_eq!(value(millis.convert(-500_000, None, 6).unwrap()), 0);
        assert_eq!(value(millis.convert(-500_001, None, 6).unwrap()), -1);
    }

    #[test]
    fn text_is_trino_s() {
        let to_text = cast(Kind::ToText, DataType::Utf8);
        // 2019-02-16 23:30 America/Sao_Paulo (first, -02:00) = 2019-02-17 01:30 UTC.
        let instant = 1_550_367_000 * NANOS_PER_SECOND;
        assert_eq!(
            text(to_text.convert(instant, Some("America/Sao_Paulo"), 3).unwrap()),
            "2019-02-16 23:30:00.000 America/Sao_Paulo"
        );
        assert_eq!(
            text(to_text.convert(TEN_AM + 123_000_000, None, 3).unwrap()),
            "2026-01-01 10:00:00.123"
        );
        assert_eq!(
            text(to_text.convert(TEN_AM, Some("+00:00"), 3).unwrap()),
            "2026-01-01 10:00:00.000 UTC"
        );
    }
}
