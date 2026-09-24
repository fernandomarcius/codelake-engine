//! Trino 431's date and time functions `DataFusion` does not have.
//!
//! - `year(x)`, `quarter`, `month`, `week` (ISO week), `day`,
//!   `day_of_week` (ISO, Monday = 1), `day_of_year`, `hour`, `minute`,
//!   `second`, `millisecond` (of the second), `year_of_week` (ISO year) and
//!   their aliases (`day_of_month`, `dow`, `doy`, `week_of_year`, `yow`):
//!   a `bigint` field of a date or timestamp, in the local time of a
//!   `timestamp with time zone` ([`DateField`]);
//! - `timezone_hour(x)`, `timezone_minute(x)` (and `extract(TIMEZONE_HOUR
//!   FROM x)`): the offset of the value's zone at its instant, `-03:30` being
//!   `-3` and `-30`;
//! - `date_diff(unit, a, b)` ([`DateDiff`]): units up to `hour` count
//!   elapsed time (23 hours across the day daylight saving begins); `day` and
//!   `week` count the local time elapsed; `month`, `quarter` and `year` count
//!   calendar periods as Trino's Joda-Time does (from `2026-01-31` to
//!   `2026-02-28` is one month, the end of a shorter month counting as the
//!   same day). Every count is truncated toward zero;
//! - `current_timezone()`: the session zone.

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::timestamp_casts::source_nanos;
use crate::timestamps::{arrow_tz, offset_at_instant};

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const NANOS_PER_DAY: i128 = 86_400 * NANOS_PER_SECOND;

/// A field of a date or timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Part {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    DayOfWeek,
    DayOfYear,
    Hour,
    Minute,
    Second,
    Millisecond,
    YearOfWeek,
    TimezoneHour,
    TimezoneMinute,
}

impl Part {
    /// The part `extract` and `date_part` name (`TIMEZONE_HOUR`), for the
    /// parts `DataFusion` does not know.
    pub(crate) fn of_date_part(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "timezone_hour" => Some(Self::TimezoneHour),
            "timezone_minute" => Some(Self::TimezoneMinute),
            _ => None,
        }
    }

    fn needs_zone(self) -> bool {
        matches!(self, Self::TimezoneHour | Self::TimezoneMinute)
    }

    fn of(self, local: &NaiveDateTime, offset_seconds: i64) -> i64 {
        match self {
            Self::Year => i64::from(local.year()),
            Self::Quarter => i64::from(local.month0() / 3 + 1),
            Self::Month => i64::from(local.month()),
            Self::Week => i64::from(local.iso_week().week()),
            Self::Day => i64::from(local.day()),
            Self::DayOfWeek => i64::from(local.weekday().number_from_monday()),
            Self::DayOfYear => i64::from(local.ordinal()),
            Self::Hour => i64::from(local.hour()),
            Self::Minute => i64::from(local.minute()),
            Self::Second => i64::from(local.second()),
            Self::Millisecond => i64::from(local.nanosecond() / 1_000_000),
            Self::YearOfWeek => i64::from(local.iso_week().year()),
            Self::TimezoneHour => offset_seconds / 3600,
            Self::TimezoneMinute => offset_seconds % 3600 / 60,
        }
    }
}

/// Local date-time of `nanos` (local time, as if UTC).
fn naive(nanos: i128) -> Option<NaiveDateTime> {
    let seconds = i64::try_from(nanos.div_euclid(NANOS_PER_SECOND)).ok()?;
    let fraction = u32::try_from(nanos.rem_euclid(NANOS_PER_SECOND)).ok()?;
    DateTime::from_timestamp(seconds, fraction).map(|d| d.naive_utc())
}

/// Each value of a date or timestamp array as (local nanoseconds, offset of
/// its zone in seconds); `zone` places the instants of a `timestamp with time
/// zone` (the array's own zone when `None`).
fn locals(array: &ArrayRef, zone: Option<&str>) -> Result<Vec<Option<(i128, i64)>>> {
    let (nanos, own_zone, _) = source_nanos(array)?;
    let zone = zone.map(str::to_owned).or(own_zone);
    let tz = match &zone {
        Some(name) => Some(arrow_tz(name)?),
        None => None,
    };
    nanos
        .into_iter()
        .map(|value| {
            value
                .map(|nanos| {
                    let offset = match tz {
                        Some(tz) => {
                            let seconds =
                                i64::try_from(nanos.div_euclid(NANOS_PER_SECOND)).map_err(|_| {
                                    datafusion::error::DataFusionError::Execution(
                                        "timestamp out of range".into(),
                                    )
                                })?;
                            offset_at_instant(tz, seconds)
                        }
                        None => 0,
                    };
                    Ok((nanos + i128::from(offset) * NANOS_PER_SECOND, offset))
                })
                .transpose()
        })
        .collect()
}

fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..)
    )
}

fn result(value: &ColumnarValue, array: ArrayRef) -> Result<ColumnarValue> {
    Ok(match value {
        ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0)?),
        ColumnarValue::Array(_) => ColumnarValue::Array(array),
    })
}

/// The part of every value of `value` (see the module documentation).
pub(crate) fn field_of(part: Part, value: &ColumnarValue, rows: usize) -> Result<ColumnarValue> {
    let rows = match value {
        ColumnarValue::Scalar(_) => 1,
        ColumnarValue::Array(_) => rows,
    };
    let array = value.to_array(rows)?;
    if part.needs_zone() && !matches!(array.data_type(), DataType::Timestamp(_, Some(_))) {
        return exec_err!(
            "{part:?} needs a timestamp with time zone, got {}",
            array.data_type()
        );
    }
    let mut out = Vec::with_capacity(array.len());
    for local in locals(&array, None)? {
        out.push(match local {
            None => None,
            Some((nanos, offset)) => {
                let Some(local) = naive(nanos) else {
                    return exec_err!("timestamp out of range");
                };
                Some(part.of(&local, offset))
            }
        });
    }
    result(value, Arc::new(Int64Array::from(out)))
}

/// `year(x)` and the other fields (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateField {
    name: &'static str,
    part: Part,
    signature: Signature,
}

impl ScalarUDFImpl for DateField {
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
        match arg_types {
            [DataType::Timestamp(_, Some(_))] => Ok(DataType::Int64),
            [t] if is_temporal(t) && !self.part.needs_zone() => Ok(DataType::Int64),
            _ => plan_err!("Unexpected parameters ({arg_types:?}) for function {}", self.name),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("{} expects one argument", self.name);
        };
        field_of(self.part, value, args.number_rows)
    }
}

// ---------------------------------------------------------------- date_diff

/// A unit of `date_diff`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    /// Elapsed time in units of this many nanoseconds.
    Elapsed(i128),
    /// Local time elapsed, in units of this many nanoseconds (day, week).
    Local(i128),
    Months,
    Quarters,
    Years,
}

impl Unit {
    fn parse(text: &str) -> Option<Self> {
        Some(match text.trim().to_ascii_lowercase().as_str() {
            "millisecond" => Self::Elapsed(1_000_000),
            "second" => Self::Elapsed(NANOS_PER_SECOND),
            "minute" => Self::Elapsed(60 * NANOS_PER_SECOND),
            "hour" => Self::Elapsed(3600 * NANOS_PER_SECOND),
            "day" => Self::Local(NANOS_PER_DAY),
            "week" => Self::Local(7 * NANOS_PER_DAY),
            "month" => Self::Months,
            "quarter" => Self::Quarters,
            "year" => Self::Years,
            _ => return None,
        })
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|d| d.pred_opt())
        .map_or(31, |d| d.day())
}

/// Nanoseconds since the start of the month (day of month included).
fn month_remainder(local: &NaiveDateTime, day: u32) -> i128 {
    i128::from(day - 1) * NANOS_PER_DAY
        + i128::from(local.num_seconds_from_midnight()) * NANOS_PER_SECOND
        + i128::from(local.nanosecond())
}

/// Joda-Time's months between `subtrahend` and `minuend`.
fn months_between(minuend: &NaiveDateTime, subtrahend: &NaiveDateTime) -> i64 {
    if minuend < subtrahend {
        return -months_between(subtrahend, minuend);
    }
    let mut difference = (i64::from(minuend.year()) - i64::from(subtrahend.year())) * 12
        + i64::from(minuend.month())
        - i64::from(subtrahend.month());
    let minuend_day = minuend.day();
    let mut subtrahend_day = subtrahend.day();
    // The last day of a month is as late as any later day of another.
    if minuend_day == days_in_month(minuend.year(), minuend.month()) && subtrahend_day > minuend_day {
        subtrahend_day = minuend_day;
    }
    if month_remainder(minuend, minuend_day) < month_remainder(subtrahend, subtrahend_day) {
        difference -= 1;
    }
    difference
}

fn is_leap(year: i32) -> bool {
    NaiveDate::from_ymd_opt(year, 2, 29).is_some()
}

/// Joda-Time's years between `subtrahend` and `minuend`.
fn years_between(minuend: &NaiveDateTime, subtrahend: &NaiveDateTime) -> i64 {
    if minuend < subtrahend {
        return -years_between(subtrahend, minuend);
    }
    let remainder = |d: &NaiveDateTime| {
        i128::from(d.ordinal0()) * NANOS_PER_DAY
            + i128::from(d.num_seconds_from_midnight()) * NANOS_PER_SECOND
            + i128::from(d.nanosecond())
    };
    // February 29 onwards.
    let feb_29 = 59 * NANOS_PER_DAY;
    let (mut minuend_rem, mut subtrahend_rem) = (remainder(minuend), remainder(subtrahend));
    if subtrahend_rem >= feb_29 && is_leap(subtrahend.year()) {
        if !is_leap(minuend.year()) {
            subtrahend_rem -= NANOS_PER_DAY;
        }
    } else if minuend_rem >= feb_29 && is_leap(minuend.year()) && !is_leap(subtrahend.year()) {
        minuend_rem -= NANOS_PER_DAY;
    }
    let mut difference = i64::from(minuend.year()) - i64::from(subtrahend.year());
    if minuend_rem < subtrahend_rem {
        difference -= 1;
    }
    difference
}

fn truncated(nanos: i128, per: i128) -> Result<i64> {
    i64::try_from(nanos / per).or_else(|_| exec_err!("date_diff out of range"))
}

/// `unit` between two values (instant nanos, local nanos).
fn difference(unit: Unit, a: (i128, i128), b: (i128, i128)) -> Result<i64> {
    let local = |nanos: i128| {
        naive(nanos)
            .ok_or_else(|| datafusion::error::DataFusionError::Execution("timestamp out of range".into()))
    };
    match unit {
        Unit::Elapsed(per) => truncated(b.0 - a.0, per),
        Unit::Local(per) => truncated(b.1 - a.1, per),
        Unit::Months => Ok(months_between(&local(b.1)?, &local(a.1)?)),
        Unit::Quarters => Ok(months_between(&local(b.1)?, &local(a.1)?) / 3),
        Unit::Years => Ok(years_between(&local(b.1)?, &local(a.1)?)),
    }
}

/// `date_diff(unit, a, b)` (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
struct DateDiff {
    signature: Signature,
}

/// The type both values of `date_diff` are compared in.
fn common_temporal(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::{Date32, Date64, Timestamp};
    let finer = |x: TimeUnit, y: TimeUnit| {
        let rank = |u: TimeUnit| match u {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        };
        if rank(x) >= rank(y) { x } else { y }
    };
    Some(match (a, b) {
        (Date32 | Date64, Date32 | Date64) => Date32,
        (Timestamp(x, None), Timestamp(y, None)) => Timestamp(finer(*x, *y), None),
        (Timestamp(x, Some(zone)), Timestamp(y, Some(_))) => Timestamp(finer(*x, *y), Some(Arc::clone(zone))),
        (Date32 | Date64, Timestamp(unit, None)) | (Timestamp(unit, None), Date32 | Date64) => {
            Timestamp(*unit, None)
        }
        _ => return None,
    })
}

impl ScalarUDFImpl for DateDiff {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "date_diff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [unit, a, b] = arg_types else {
            return plan_err!("date_diff expects three arguments");
        };
        let Some(common) = common_temporal(a, b) else {
            return plan_err!("Unexpected parameters ({arg_types:?}) for function date_diff");
        };
        let unit = match unit {
            DataType::Null => DataType::Utf8,
            other => other.clone(),
        };
        Ok(vec![unit, common.clone(), common])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [unit, a, b] = args.args.as_slice() else {
            return exec_err!("date_diff expects three arguments");
        };
        let scalar = args.args.iter().all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let rows = if scalar { 1 } else { args.number_rows };
        let units = unit.to_array(rows)?;
        let units = datafusion::arrow::compute::cast(&units, &DataType::Utf8)?;
        let units: &StringArray = units.as_string::<i32>();
        let (a, b) = (a.to_array(rows)?, b.to_array(rows)?);
        // The first value's zone reads both (Trino's chronology).
        let zone = match a.data_type() {
            DataType::Timestamp(_, Some(zone)) => Some(zone.to_string()),
            _ => None,
        };
        let (instants_a, _, _) = source_nanos(&a)?;
        let (instants_b, _, _) = source_nanos(&b)?;
        let locals_a = locals(&a, zone.as_deref())?;
        let locals_b = locals(&b, zone.as_deref())?;
        let mut out = Vec::with_capacity(rows);
        for i in 0..rows {
            out.push(
                match (
                    units.is_valid(i).then(|| units.value(i)),
                    instants_a[i].zip(locals_a[i]),
                    instants_b[i].zip(locals_b[i]),
                ) {
                    (Some(text), Some((ia, (la, _))), Some((ib, (lb, _)))) => {
                        let Some(unit) = Unit::parse(text) else {
                            return exec_err!("'{text}' is not a valid DATE field");
                        };
                        Some(difference(unit, (ia, la), (ib, lb))?)
                    }
                    _ => None,
                },
            );
        }
        let array: ArrayRef = Arc::new(Int64Array::from(out));
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0)?)
        } else {
            ColumnarValue::Array(array)
        })
    }
}

// ---------------------------------------------------------------- current_timezone

/// `current_timezone()`: the session zone, as Trino names it.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentTimezone {
    signature: Signature,
}

impl ScalarUDFImpl for CurrentTimezone {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "current_timezone"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone = args
            .config_options
            .execution
            .time_zone
            .clone()
            .unwrap_or_else(|| "UTC".to_owned());
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            crate::timestamps::zone_label(&zone),
        ))))
    }
}

/// Every function of this module.
pub(crate) fn functions() -> Vec<ScalarUDF> {
    let any = || Signature::any(1, Volatility::Immutable);
    let fields = [
        ("year", Part::Year),
        ("quarter", Part::Quarter),
        ("month", Part::Month),
        ("week", Part::Week),
        ("week_of_year", Part::Week),
        ("day", Part::Day),
        ("day_of_month", Part::Day),
        ("day_of_week", Part::DayOfWeek),
        ("dow", Part::DayOfWeek),
        ("day_of_year", Part::DayOfYear),
        ("doy", Part::DayOfYear),
        ("hour", Part::Hour),
        ("minute", Part::Minute),
        ("second", Part::Second),
        ("millisecond", Part::Millisecond),
        ("year_of_week", Part::YearOfWeek),
        ("yow", Part::YearOfWeek),
        ("timezone_hour", Part::TimezoneHour),
        ("timezone_minute", Part::TimezoneMinute),
    ];
    let mut all: Vec<ScalarUDF> = fields
        .into_iter()
        .map(|(name, part)| {
            ScalarUDF::new_from_impl(DateField {
                name,
                part,
                signature: any(),
            })
        })
        .collect();
    all.push(ScalarUDF::new_from_impl(DateDiff {
        signature: Signature::user_defined(Volatility::Immutable),
    }));
    all.push(ScalarUDF::new_from_impl(CurrentTimezone {
        signature: Signature::nullary(Volatility::Stable),
    }));
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").unwrap()
    }

    fn nanos(text: &str) -> i128 {
        i128::from(at(text).and_utc().timestamp_nanos_opt().unwrap())
    }

    /// `date_diff` answers observed on Trino 431.
    #[test]
    fn differences_follow_trino() {
        let local = |unit: &str, a: &str, b: &str| {
            let (a, b) = (nanos(a), nanos(b));
            difference(Unit::parse(unit).unwrap(), (a, a), (b, b)).unwrap()
        };
        assert_eq!(local("day", "2026-01-01 00:00:00", "2025-12-01 00:00:00"), -31);
        assert_eq!(local("month", "2026-01-31 00:00:00", "2026-02-28 00:00:00"), 1);
        assert_eq!(local("month", "2026-02-28 00:00:00", "2026-01-31 00:00:00"), -1);
        assert_eq!(local("month", "2026-01-31 10:00:00", "2026-02-28 09:00:00"), 0);
        assert_eq!(local("year", "2020-02-29 10:00:00", "2021-02-28 10:00:00"), 1);
        assert_eq!(local("year", "2020-02-29 10:00:00", "2021-02-28 09:59:59"), 0);
        assert_eq!(
            local("second", "2020-01-01 00:00:00.999", "2020-01-01 00:00:01.998"),
            0
        );
        assert_eq!(
            local(
                "millisecond",
                "2020-01-01 00:00:00.999",
                "2020-01-01 00:00:01.998"
            ),
            999
        );
        assert_eq!(local("week", "2026-01-01 00:00:00", "2026-01-15 00:00:00"), 2);
        assert_eq!(local("quarter", "2026-01-01 00:00:00", "2026-12-31 00:00:00"), 3);
        assert_eq!(local("hour", "2026-01-01 10:00:00", "2026-01-01 08:30:00"), -1);
        // 2018-11-03 12:00 to 2018-11-04 12:00 in Sao Paulo, where daylight
        // saving began at midnight: 23 hours elapsed, one local day.
        let (a, b) = (nanos("2018-11-03 15:00:00"), nanos("2018-11-04 14:00:00"));
        let (la, lb) = (nanos("2018-11-03 12:00:00"), nanos("2018-11-04 12:00:00"));
        assert_eq!(
            difference(Unit::parse("hour").unwrap(), (a, la), (b, lb)).unwrap(),
            23
        );
        assert_eq!(
            difference(Unit::parse("DAY").unwrap(), (a, la), (b, lb)).unwrap(),
            1
        );
        assert_eq!(Unit::parse("fortnight"), None);
    }

    #[test]
    fn fields_follow_trino() {
        let d = at("2026-09-24 10:07:09.5");
        assert_eq!(Part::DayOfWeek.of(&d, 0), 4);
        assert_eq!(Part::DayOfYear.of(&d, 0), 267);
        assert_eq!(Part::Week.of(&d, 0), 39);
        assert_eq!(Part::Quarter.of(&d, 0), 3);
        assert_eq!(Part::Millisecond.of(&d, 0), 500);
        assert_eq!(Part::Second.of(&d, 0), 9);
        assert_eq!(Part::YearOfWeek.of(&at("2021-01-01 00:00:00"), 0), 2020);
        assert_eq!(Part::TimezoneHour.of(&d, -12_600), -3);
        assert_eq!(Part::TimezoneMinute.of(&d, -12_600), -30);
        assert_eq!(Part::TimezoneMinute.of(&d, 20_700), 45);
    }
}
