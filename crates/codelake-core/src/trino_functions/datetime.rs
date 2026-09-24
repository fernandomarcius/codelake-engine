//! Date and time functions with Trino 431 semantics.
//!
//! Model: `timestamp(p)` is an Arrow `Timestamp(unit, None)` holding the wall
//! clock; `timestamp(p) with time zone` is `Timestamp(unit, Some(zone))`
//! holding the UTC instant. Calendar arithmetic on a zoned value happens in
//! local time (Joda's zoned chronology); units shorter than 12 hours are pure
//! instant arithmetic, exactly as Trino does.
//!
//! Arrow has one zone per column: functions whose result zone comes from an
//! argument (`with_timezone`, `at_timezone`) require that argument to be a
//! constant.

use std::fmt::Write as _;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Duration, Months, NaiveDate, NaiveDateTime, Timelike, Utc};
use datafusion::arrow::array::{Array, ArrayRef, Date32Array, Time32MillisecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, ScalarUDF, Volatility};
use num_traits::ToPrimitive;

use super::support::{
    At, Call, Func, Ret, as_f64, as_i64, as_str, as_timestamp, coerce_any, coerce_each, dates, float_array,
    floats, int_array, invalid_argument, not_found, out_of_range, per_second, string_array, strings,
    timestamp_array, timestamps,
};
use super::zone::Zone;

pub(crate) const MILLIS_PER_DAY: i64 = 86_400_000;
/// Days from 0001-01-01 (CE day 1) to 1970-01-01.
const EPOCH_DAYS_FROM_CE: i32 = 719_163;

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

pub(crate) fn to_naive(value: i64, unit: TimeUnit) -> Option<NaiveDateTime> {
    let per = per_second(unit);
    let seconds = value.div_euclid(per);
    let nanos = u32::try_from(value.rem_euclid(per) * (1_000_000_000 / per)).ok()?;
    DateTime::from_timestamp(seconds, nanos).map(|d| d.naive_utc())
}

pub(crate) fn from_naive(dt: NaiveDateTime, unit: TimeUnit) -> Option<i64> {
    let per = per_second(unit);
    let utc = dt.and_utc();
    let sub = i64::from(utc.timestamp_subsec_nanos()) / (1_000_000_000 / per);
    utc.timestamp().checked_mul(per)?.checked_add(sub)
}

pub(crate) fn date_from_days(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(days.checked_add(EPOCH_DAYS_FROM_CE)?)
}

pub(crate) fn days_from_date(date: NaiveDate) -> i32 {
    date.num_days_from_ce() - EPOCH_DAYS_FROM_CE
}

pub(crate) fn epoch_millis(dt: NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_millis()
}

pub(crate) fn from_epoch_millis(millis: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_millis(millis).map(|d| d.naive_utc())
}

/// Zone of a `timestamp with time zone` column.
pub(crate) fn column_zone(zone: Option<&Arc<str>>) -> Result<Option<Zone>> {
    match zone {
        None => Ok(None),
        Some(name) => Zone::parse(name)
            .map(Some)
            .ok_or_else(|| invalid_argument(format!("'{name}' is not a valid time zone"))),
    }
}

pub(crate) fn parse_zone(id: &str) -> Result<Zone> {
    Zone::parse(id).ok_or_else(|| invalid_argument(format!("'{id}' is not a valid time zone")))
}

fn out_of_range_ts() -> DataFusionError {
    out_of_range("timestamp is out of the supported range")
}

// ---------------------------------------------------------------------------
// Units (Joda fields)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Field {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Field {
    fn parse(unit: &str) -> Option<Field> {
        Some(match unit.to_ascii_lowercase().as_str() {
            "millisecond" => Field::Millisecond,
            "second" => Field::Second,
            "minute" => Field::Minute,
            "hour" => Field::Hour,
            "day" => Field::Day,
            "week" => Field::Week,
            "month" => Field::Month,
            "quarter" => Field::Quarter,
            "year" => Field::Year,
            _ => return None,
        })
    }

    fn for_timestamp(unit: &str) -> Result<Field> {
        Field::parse(unit).ok_or_else(|| invalid_argument(format!("'{unit}' is not a valid TIMESTAMP field")))
    }

    fn for_date(unit: &str) -> Result<Field> {
        match Field::parse(unit) {
            Some(f) if f.precise_millis().is_none_or(|ms| ms >= MILLIS_PER_DAY) => Ok(f),
            _ => Err(invalid_argument(format!("'{unit}' is not a valid DATE field"))),
        }
    }

    /// Length of fixed-size units in milliseconds (UTC chronology).
    fn precise_millis(self) -> Option<i64> {
        Some(match self {
            Field::Millisecond => 1,
            Field::Second => 1_000,
            Field::Minute => 60_000,
            Field::Hour => 3_600_000,
            Field::Day => MILLIS_PER_DAY,
            Field::Week => 7 * MILLIS_PER_DAY,
            Field::Month | Field::Quarter | Field::Year => return None,
        })
    }

    /// Joda does instant (not local) arithmetic for units under 12 hours.
    fn is_time_field(self) -> bool {
        matches!(
            self,
            Field::Millisecond | Field::Second | Field::Minute | Field::Hour
        )
    }

    fn months(self) -> Option<i64> {
        match self {
            Field::Month => Some(1),
            Field::Quarter => Some(3),
            Field::Year => Some(12),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Calendar arithmetic (Joda ISO chronology in UTC)
// ---------------------------------------------------------------------------

fn add_months(dt: NaiveDateTime, months: i64) -> Result<NaiveDateTime> {
    let result = if months >= 0 {
        u32::try_from(months)
            .ok()
            .and_then(|m| dt.checked_add_months(Months::new(m)))
    } else {
        u32::try_from(months.unsigned_abs())
            .ok()
            .and_then(|m| dt.checked_sub_months(Months::new(m)))
    };
    result.ok_or_else(out_of_range_ts)
}

/// `field.add(instant, value)` on a wall clock.
pub(crate) fn add_field(dt: NaiveDateTime, field: Field, value: i64) -> Result<NaiveDateTime> {
    if let Some(months) = field.months() {
        let total = months.checked_mul(value).ok_or_else(out_of_range_ts)?;
        return add_months(dt, total);
    }
    let millis = field
        .precise_millis()
        .and_then(|ms| ms.checked_mul(value))
        .ok_or_else(out_of_range_ts)?;
    dt.checked_add_signed(Duration::milliseconds(millis))
        .ok_or_else(out_of_range_ts)
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

fn is_leap(year: i32) -> bool {
    NaiveDate::from_ymd_opt(year, 2, 29).is_some()
}

fn start_of_month(dt: NaiveDateTime) -> NaiveDateTime {
    dt.date()
        .with_day(1)
        .unwrap_or(dt.date())
        .and_time(chrono::NaiveTime::MIN)
}

fn start_of_year(year: i32) -> Option<NaiveDateTime> {
    NaiveDate::from_ymd_opt(year, 1, 1).map(|d| d.and_time(chrono::NaiveTime::MIN))
}

/// Joda `BasicMonthOfYearDateTimeField.getDifferenceAsLong`.
fn month_difference(minuend: NaiveDateTime, subtrahend: NaiveDateTime) -> i64 {
    if minuend < subtrahend {
        return -month_difference(subtrahend, minuend);
    }
    let mut difference = i64::from(minuend.year() - subtrahend.year()) * 12 + i64::from(minuend.month())
        - i64::from(subtrahend.month());
    let mut subtrahend = subtrahend;
    let minuend_day = minuend.day();
    if minuend_day == days_in_month(minuend.year(), minuend.month())
        && subtrahend.day() > minuend_day
        && let Some(clamped) = subtrahend.with_day(minuend_day)
    {
        subtrahend = clamped;
    }
    if minuend - start_of_month(minuend) < subtrahend - start_of_month(subtrahend) {
        difference -= 1;
    }
    difference
}

/// Joda `BasicChronology.getYearDifference`.
fn year_difference(minuend: NaiveDateTime, subtrahend: NaiveDateTime) -> i64 {
    if minuend < subtrahend {
        return -year_difference(subtrahend, minuend);
    }
    let (my, sy) = (minuend.year(), subtrahend.year());
    let (Some(m_start), Some(s_start)) = (start_of_year(my), start_of_year(sy)) else {
        return i64::from(my - sy);
    };
    let mut minuend_rem = epoch_millis(minuend) - epoch_millis(m_start);
    let mut subtrahend_rem = epoch_millis(subtrahend) - epoch_millis(s_start);
    let feb_29 = (31 + 28) * MILLIS_PER_DAY;
    if subtrahend_rem >= feb_29 {
        if is_leap(sy) {
            if !is_leap(my) {
                subtrahend_rem -= MILLIS_PER_DAY;
            }
        } else if minuend_rem >= feb_29 && is_leap(my) {
            minuend_rem -= MILLIS_PER_DAY;
        }
    }
    let mut difference = i64::from(my - sy);
    if minuend_rem < subtrahend_rem {
        difference -= 1;
    }
    difference
}

/// `field.getDifferenceAsLong(second, first)` on millisecond wall clocks.
fn field_difference(field: Field, first: i64, second: i64) -> Result<i64> {
    if let Some(unit) = field.precise_millis() {
        let delta = second.checked_sub(first).ok_or_else(out_of_range_ts)?;
        return Ok(delta / unit);
    }
    let (Some(a), Some(b)) = (from_epoch_millis(first), from_epoch_millis(second)) else {
        return Err(out_of_range_ts());
    };
    Ok(match field {
        Field::Year => year_difference(b, a),
        Field::Quarter => month_difference(b, a) / 3,
        _ => month_difference(b, a),
    })
}

fn truncate_naive(dt: NaiveDateTime, field: Field) -> NaiveDateTime {
    let date = dt.date();
    let midnight = |d: NaiveDate| d.and_time(chrono::NaiveTime::MIN);
    match field {
        Field::Millisecond => dt
            .with_nanosecond(dt.nanosecond() / 1_000_000 * 1_000_000)
            .unwrap_or(dt),
        Field::Second => dt.with_nanosecond(0).unwrap_or(dt),
        Field::Minute => dt.with_nanosecond(0).and_then(|d| d.with_second(0)).unwrap_or(dt),
        Field::Hour => date.and_hms_opt(dt.hour(), 0, 0).unwrap_or(dt),
        Field::Day => midnight(date),
        Field::Week => midnight(date - Duration::days(i64::from(date.weekday().num_days_from_monday()))),
        Field::Month => midnight(date.with_day(1).unwrap_or(date)),
        Field::Quarter => {
            let month = (date.month() - 1) / 3 * 3 + 1;
            midnight(NaiveDate::from_ymd_opt(date.year(), month, 1).unwrap_or(date))
        }
        Field::Year => midnight(NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap_or(date)),
    }
}

// ---------------------------------------------------------------------------
// date_add / date_diff / date_trunc
// ---------------------------------------------------------------------------

fn datetime_arg(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Date32 | DataType::Timestamp(..) => Some(t.clone()),
        DataType::Date64 => Some(DataType::Date32),
        DataType::Null => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
        _ => None,
    }
}

fn coerce_date_add(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    coerce_each(name, types, &[as_str, as_i64, datetime_arg])
}

fn type_of_third(ret: &Ret<'_>) -> Result<DataType> {
    ret.types
        .get(2)
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
}

fn add_timestamp(field: Field, value: i64, raw: i64, unit: TimeUnit, zone: Option<Zone>) -> Result<i64> {
    let dt = to_naive(raw, unit).ok_or_else(out_of_range_ts)?;
    let result = match zone {
        Some(zone) if !field.is_time_field() => {
            let local = zone.to_local(dt);
            zone.to_utc_near(add_field(local, field, value)?, dt)
        }
        _ => add_field(dt, field, value)?,
    };
    from_naive(result, unit).ok_or_else(out_of_range_ts)
}

fn invoke_date_add(call: &Call<'_>) -> Result<ArrayRef> {
    let units = strings(call.arg(0)?)?;
    let values = super::support::ints(call.arg(1)?)?;
    let target = call.arg(2)?;
    if let DataType::Date32 = target.data_type() {
        let days = dates(target)?;
        let out: Result<Vec<Option<i32>>> = (0..call.rows)
            .map(|i| {
                let (Some(unit), Some(value), Some(day)) = (units.at(i), values.at(i), days.at(i)) else {
                    return Ok(None);
                };
                let field = Field::for_date(unit)?;
                let date = date_from_days(day).ok_or_else(out_of_range_ts)?;
                let result = add_field(date.and_time(chrono::NaiveTime::MIN), field, value)?;
                Ok(Some(days_from_date(result.date())))
            })
            .collect();
        return Ok(Arc::new(Date32Array::from(out?)));
    }
    let ts = timestamps(target)?;
    let zone = column_zone(ts.zone.as_ref())?;
    let out = (0..call.rows)
        .map(|i| {
            let (Some(unit), Some(value), Some(raw)) = (units.at(i), values.at(i), ts.at(i)) else {
                return Ok(None);
            };
            let field = Field::for_timestamp(unit)?;
            add_timestamp(field, value, raw, ts.unit, zone).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    timestamp_array(out, ts.unit, ts.zone.clone())
}

pub(crate) fn date_add() -> ScalarUDF {
    Func::new("date_add", coerce_date_add, type_of_third, invoke_date_add).udf()
}

/// Both operands of `date_diff` share one type (Trino coerces the narrower).
fn coerce_date_diff(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    let [unit, a, b] = types else {
        return Err(not_found(name, types));
    };
    let unit = as_str(unit).ok_or_else(|| not_found(name, types))?;
    let common = common_datetime(a, b).ok_or_else(|| not_found(name, types))?;
    Ok(vec![unit, common.clone(), common])
}

fn finer(a: TimeUnit, b: TimeUnit) -> TimeUnit {
    if super::support::per_second(a) >= super::support::per_second(b) {
        a
    } else {
        b
    }
}

/// Common type of two date/time operands, as Trino's implicit coercions pick it.
pub(crate) fn common_datetime(a: &DataType, b: &DataType) -> Option<DataType> {
    use DataType::{Date32, Null, Timestamp};
    Some(match (datetime_arg(a)?, datetime_arg(b)?) {
        (Date32, Date32) => Date32,
        (Timestamp(u1, z1), Timestamp(u2, z2)) => Timestamp(finer(u1, u2), z1.or(z2)),
        (Timestamp(u, z), Null | Date32) | (Null | Date32, Timestamp(u, z)) => Timestamp(u, z),
        _ => return None,
    })
}

fn millis_floor(raw: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second => raw.checked_mul(1_000),
        TimeUnit::Millisecond => Some(raw),
        TimeUnit::Microsecond => Some(raw.div_euclid(1_000)),
        TimeUnit::Nanosecond => Some(raw.div_euclid(1_000_000)),
    }
}

fn invoke_date_diff(call: &Call<'_>) -> Result<ArrayRef> {
    let units = strings(call.arg(0)?)?;
    let (a, b) = (call.arg(1)?, call.arg(2)?);
    if let DataType::Date32 = a.data_type() {
        let (a, b) = (dates(a)?, dates(b)?);
        let out = (0..call.rows)
            .map(|i| {
                let (Some(unit), Some(x), Some(y)) = (units.at(i), a.at(i), b.at(i)) else {
                    return Ok(None);
                };
                let field = Field::for_date(unit)?;
                field_difference(
                    field,
                    i64::from(x) * MILLIS_PER_DAY,
                    i64::from(y) * MILLIS_PER_DAY,
                )
                .map(Some)
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(int_array(out));
    }
    let (a, b) = (timestamps(a)?, timestamps(b)?);
    // Trino uses the first operand's chronology.
    let zone = column_zone(a.zone.as_ref())?;
    let out = (0..call.rows)
        .map(|i| {
            let (Some(unit), Some(x), Some(y)) = (units.at(i), a.at(i), b.at(i)) else {
                return Ok(None);
            };
            let field = Field::for_timestamp(unit)?;
            let (Some(mut x), Some(mut y)) = (millis_floor(x, a.unit), millis_floor(y, b.unit)) else {
                return Err(out_of_range_ts());
            };
            if let Some(zone) = zone.filter(|_| !field.is_time_field()) {
                let offset =
                    |ms: i64| from_epoch_millis(ms).map_or(0, |dt| i64::from(zone.offset_at(dt)) * 1_000);
                x += offset(x);
                y += offset(y);
            }
            field_difference(field, x, y).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(int_array(out))
}

pub(crate) fn date_diff() -> ScalarUDF {
    Func::new(
        "date_diff",
        coerce_date_diff,
        |_| Ok(DataType::Int64),
        invoke_date_diff,
    )
    .udf()
}

fn coerce_date_trunc(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    coerce_each(name, types, &[as_str, datetime_arg])
}

fn truncate_timestamp(field: Field, raw: i64, unit: TimeUnit, zone: Option<Zone>) -> Result<i64> {
    let dt = to_naive(raw, unit).ok_or_else(out_of_range_ts)?;
    let result = match zone {
        None => truncate_naive(dt, field),
        Some(zone) => {
            let offset = Duration::seconds(i64::from(zone.offset_at(dt)));
            if field.is_time_field() {
                truncate_naive(dt + offset, field) - offset
            } else {
                zone.to_utc_near(truncate_naive(dt + offset, field), dt)
            }
        }
    };
    from_naive(result, unit).ok_or_else(out_of_range_ts)
}

fn invoke_date_trunc(call: &Call<'_>) -> Result<ArrayRef> {
    let units = strings(call.arg(0)?)?;
    let target = call.arg(1)?;
    if let DataType::Date32 = target.data_type() {
        let days = dates(target)?;
        let out = (0..call.rows)
            .map(|i| {
                let (Some(unit), Some(day)) = (units.at(i), days.at(i)) else {
                    return Ok(None);
                };
                let field = Field::for_date(unit)?;
                let date = date_from_days(day).ok_or_else(out_of_range_ts)?;
                let truncated = truncate_naive(date.and_time(chrono::NaiveTime::MIN), field);
                Ok(Some(days_from_date(truncated.date())))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(Date32Array::from(out)));
    }
    let ts = timestamps(target)?;
    let zone = column_zone(ts.zone.as_ref())?;
    let out = (0..call.rows)
        .map(|i| {
            let (Some(unit), Some(raw)) = (units.at(i), ts.at(i)) else {
                return Ok(None);
            };
            truncate_timestamp(Field::for_timestamp(unit)?, raw, ts.unit, zone).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    timestamp_array(out, ts.unit, ts.zone.clone())
}

pub(crate) fn date_trunc() -> ScalarUDF {
    Func::new(
        "date_trunc",
        coerce_date_trunc,
        |r| {
            r.types
                .get(1)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
        },
        invoke_date_trunc,
    )
    .udf()
}

// ---------------------------------------------------------------------------
// Field extraction: year(), month(), …, EXTRACT
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    DayOfWeek,
    DayOfYear,
    YearOfWeek,
    Hour,
    Minute,
    Second,
    Millisecond,
    TimezoneHour,
    TimezoneMinute,
}

impl Part {
    fn parse(name: &str) -> Option<Part> {
        Some(match name.to_ascii_lowercase().as_str() {
            "year" => Part::Year,
            "quarter" => Part::Quarter,
            "month" => Part::Month,
            "week" | "week_of_year" => Part::Week,
            "day" | "day_of_month" => Part::Day,
            "day_of_week" | "dow" => Part::DayOfWeek,
            "day_of_year" | "doy" => Part::DayOfYear,
            "year_of_week" | "yow" => Part::YearOfWeek,
            "hour" => Part::Hour,
            "minute" => Part::Minute,
            "second" => Part::Second,
            "millisecond" => Part::Millisecond,
            "timezone_hour" => Part::TimezoneHour,
            "timezone_minute" => Part::TimezoneMinute,
            _ => return None,
        })
    }

    fn is_time(self) -> bool {
        matches!(self, Part::Hour | Part::Minute | Part::Second | Part::Millisecond)
    }

    fn is_zone(self) -> bool {
        matches!(self, Part::TimezoneHour | Part::TimezoneMinute)
    }

    fn of(self, local: NaiveDateTime, offset_seconds: i32) -> i64 {
        let date = local.date();
        match self {
            Part::Year => i64::from(date.year()),
            Part::Quarter => i64::from((date.month() - 1) / 3 + 1),
            Part::Month => i64::from(date.month()),
            Part::Week => i64::from(date.iso_week().week()),
            Part::Day => i64::from(date.day()),
            Part::DayOfWeek => i64::from(date.weekday().number_from_monday()),
            Part::DayOfYear => i64::from(date.ordinal()),
            Part::YearOfWeek => i64::from(date.iso_week().year()),
            Part::Hour => i64::from(local.hour()),
            Part::Minute => i64::from(local.minute()),
            Part::Second => i64::from(local.second()),
            Part::Millisecond => i64::from(local.nanosecond() / 1_000_000),
            Part::TimezoneHour => i64::from(offset_seconds / 3600),
            Part::TimezoneMinute => i64::from((offset_seconds % 3600) / 60),
        }
    }
}

/// Wall clock and offset of every row of a date or timestamp column.
fn local_values(array: &ArrayRef, rows: usize) -> Result<Vec<Option<(NaiveDateTime, i32)>>> {
    match array.data_type() {
        DataType::Date32 => {
            let days = dates(array)?;
            Ok((0..rows)
                .map(|i| {
                    days.at(i)
                        .and_then(date_from_days)
                        .map(|d| (d.and_time(chrono::NaiveTime::MIN), 0))
                })
                .collect())
        }
        DataType::Timestamp(..) => {
            let ts = timestamps(array)?;
            let zone = column_zone(ts.zone.as_ref())?;
            Ok((0..rows)
                .map(|i| {
                    let dt = to_naive(ts.at(i)?, ts.unit)?;
                    Some(match zone {
                        None => (dt, 0),
                        Some(zone) => (zone.to_local(dt), zone.offset_at(dt)),
                    })
                })
                .collect())
        }
        other => Err(DataFusionError::Internal(format!("unexpected {other}"))),
    }
}

fn part_types(part: Part, t: &DataType) -> bool {
    match t {
        DataType::Date32 => !part.is_time() && !part.is_zone(),
        DataType::Timestamp(_, zone) => !part.is_zone() || zone.is_some(),
        _ => false,
    }
}

fn extract(part: Part, array: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    let values = local_values(array, rows)?;
    Ok(int_array(
        values
            .into_iter()
            .map(|v| v.map(|(local, offset)| part.of(local, offset)))
            .collect(),
    ))
}

macro_rules! part_function {
    ($fn_name:ident, $sql:literal, $part:expr $(, $alias:literal)*) => {
        pub(crate) fn $fn_name() -> ScalarUDF {
            Func::new(
                $sql,
                |name, types| {
                    coerce_each(name, types, &[datetime_arg]).and_then(|t| {
                        if t.first().is_some_and(|t| part_types($part, t)) {
                            Ok(t)
                        } else {
                            Err(not_found(name, types))
                        }
                    })
                },
                |_| Ok(DataType::Int64),
                |call| extract($part, call.arg(0)?, call.rows),
            )
            $(.alias($alias))*
            .udf()
        }
    };
}

part_function!(year, "year", Part::Year);
part_function!(quarter, "quarter", Part::Quarter);
part_function!(month, "month", Part::Month);
part_function!(week, "week", Part::Week, "week_of_year");
part_function!(day, "day", Part::Day, "day_of_month");
part_function!(day_of_week, "day_of_week", Part::DayOfWeek, "dow");
part_function!(day_of_year, "day_of_year", Part::DayOfYear, "doy");
part_function!(year_of_week, "year_of_week", Part::YearOfWeek, "yow");
part_function!(hour, "hour", Part::Hour);
part_function!(minute, "minute", Part::Minute);
part_function!(second, "second", Part::Second);
part_function!(millisecond, "millisecond", Part::Millisecond);
part_function!(timezone_hour, "timezone_hour", Part::TimezoneHour);
part_function!(timezone_minute, "timezone_minute", Part::TimezoneMinute);

/// `EXTRACT(field FROM x)`: `DataFusion` plans it as `date_part('field', x)`.
pub(crate) fn date_part() -> ScalarUDF {
    Func::new(
        "date_part",
        |name, types| coerce_each(name, types, &[as_str, datetime_arg]),
        |r| {
            if let Some(field) = r.literal_str(0) {
                let part = Part::parse(field).ok_or_else(|| {
                    DataFusionError::Plan(format!("SYNTAX_ERROR: Invalid EXTRACT field: {field}"))
                })?;
                if !r.types.get(1).is_some_and(|t| part_types(part, t)) {
                    return Err(DataFusionError::Plan(format!(
                        "TYPE_MISMATCH: Cannot extract {field} from {}",
                        r.types
                            .get(1)
                            .map_or_else(String::new, super::support::trino_type_name)
                    )));
                }
            }
            Ok(DataType::Int64)
        },
        |call| {
            let fields = strings(call.arg(0)?)?;
            let values = local_values(call.arg(1)?, call.rows)?;
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(field), Some((local, offset))) =
                        (fields.at(i), values.get(i).copied().flatten())
                    else {
                        return Ok(None);
                    };
                    let part = Part::parse(field)
                        .ok_or_else(|| invalid_argument(format!("Invalid EXTRACT field: {field}")))?;
                    Ok(Some(part.of(local, offset)))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

fn last_day(local: NaiveDateTime) -> i32 {
    let date = local.date();
    let last = days_in_month(date.year(), date.month());
    days_from_date(date.with_day(last).unwrap_or(date))
}

pub(crate) fn last_day_of_month() -> ScalarUDF {
    Func::new(
        "last_day_of_month",
        |name, types| coerce_each(name, types, &[datetime_arg]),
        |_| Ok(DataType::Date32),
        |call| {
            let values = local_values(call.arg(0)?, call.rows)?;
            Ok(Arc::new(Date32Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|(local, _)| last_day(local)))
                    .collect::<Vec<_>>(),
            )))
        },
    )
    .udf()
}

/// `date(x)`: the (local) date of a timestamp, or a date parsed from text.
pub(crate) fn date() -> ScalarUDF {
    Func::new(
        "date",
        |name, types| {
            coerce_any(
                name,
                types,
                &[
                    &[datetime_arg],
                    &[|t| super::support::is_string(t).then_some(DataType::Utf8)],
                ],
            )
        },
        |_| Ok(DataType::Date32),
        |call| {
            let arg = call.arg(0)?;
            if let DataType::Utf8 = arg.data_type() {
                let text = strings(arg)?;
                let out = (0..call.rows)
                    .map(|i| {
                        text.at(i)
                            .map(|s| {
                                parse_date(s).map(days_from_date).ok_or_else(|| {
                                    super::support::invalid_cast(format!("Value cannot be cast to date: {s}"))
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Arc::new(Date32Array::from(out)));
            }
            let values = local_values(arg, call.rows)?;
            Ok(Arc::new(Date32Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|(local, _)| days_from_date(local.date())))
                    .collect::<Vec<_>>(),
            )))
        },
    )
    .udf()
}

/// Trino date literal syntax: `YYYY-MM-DD` (year may have a sign).
pub(crate) fn parse_date(text: &str) -> Option<NaiveDate> {
    let text = text.trim();
    let mut parts = text.rsplitn(3, '-');
    let day: u32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let year: i32 = parts.next()?.parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

// ---------------------------------------------------------------------------
// Session constants: current_timestamp, localtimestamp, current_date, …
// ---------------------------------------------------------------------------

fn start_time(info: &SimplifyContext) -> Option<DateTime<Utc>> {
    info.query_execution_start_time()
}

fn precision_unit(precision: i64) -> Result<TimeUnit> {
    match precision {
        0 => Ok(TimeUnit::Second),
        1..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=12 => Ok(TimeUnit::Nanosecond),
        _ => Err(invalid_argument(format!("Invalid precision: {precision}"))),
    }
}

/// Optional literal precision argument (`current_timestamp(6)`).
fn coerce_precision(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    match types {
        [] => Ok(vec![]),
        [t] => as_i64(t).map(|t| vec![t]).ok_or_else(|| not_found(name, types)),
        _ => Err(not_found(name, types)),
    }
}

fn literal_precision(ret: &Ret<'_>) -> Result<TimeUnit> {
    match ret.scalars.first().copied().flatten() {
        None if ret.types.is_empty() => Ok(TimeUnit::Millisecond),
        Some(scalar) => {
            let value = scalar
                .cast_to(&DataType::Int64)
                .ok()
                .and_then(|v| {
                    if let ScalarValue::Int64(Some(v)) = v {
                        Some(v)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| DataFusionError::Plan("precision must be a constant".into()))?;
            precision_unit(value)
        }
        None => Err(DataFusionError::Plan("precision must be a constant".into())),
    }
}

fn scalar_timestamp(raw: i64, unit: TimeUnit, zone: Option<Arc<str>>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(raw), zone),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(raw), zone),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(raw), zone),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(raw), zone),
    }
}

/// Local wall clock of `now` in the session zone.
fn local_now(now: DateTime<Utc>, zone: &str) -> Result<NaiveDateTime> {
    Ok(parse_zone(zone)?.to_local(now.naive_utc()))
}

fn current_timestamp_value(now: DateTime<Utc>, return_type: &DataType) -> Result<ScalarValue> {
    let DataType::Timestamp(unit, zone) = return_type else {
        return Err(DataFusionError::Internal("expected timestamp".into()));
    };
    let raw = from_naive(now.naive_utc(), *unit).ok_or_else(out_of_range_ts)?;
    Ok(scalar_timestamp(raw, *unit, zone.clone()))
}

fn localtimestamp_value(now: DateTime<Utc>, zone: &str, return_type: &DataType) -> Result<ScalarValue> {
    let DataType::Timestamp(unit, _) = return_type else {
        return Err(DataFusionError::Internal("expected timestamp".into()));
    };
    let raw = from_naive(local_now(now, zone)?, *unit).ok_or_else(out_of_range_ts)?;
    Ok(scalar_timestamp(raw, *unit, None))
}

fn repeat(value: &ScalarValue, rows: usize) -> Result<ArrayRef> {
    value.to_array_of_size(rows.max(1))
}

fn simplify_current_timestamp(func: &Func, args: &[Expr], info: &SimplifyContext) -> Result<Option<Expr>> {
    // Only the default precision is folded; `current_timestamp(p)` is evaluated per batch.
    let Some(now) = start_time(info).filter(|_| args.is_empty()) else {
        return Ok(None);
    };
    let return_type = DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::clone(func.zone())));
    Ok(Some(Expr::Literal(
        current_timestamp_value(now, &return_type)?,
        None,
    )))
}

fn simplify_localtimestamp(func: &Func, args: &[Expr], info: &SimplifyContext) -> Result<Option<Expr>> {
    let Some(now) = start_time(info).filter(|_| args.is_empty()) else {
        return Ok(None);
    };
    let return_type = DataType::Timestamp(TimeUnit::Millisecond, None);
    Ok(Some(Expr::Literal(
        localtimestamp_value(now, func.zone(), &return_type)?,
        None,
    )))
}

fn simplify_current_date(func: &Func, _args: &[Expr], info: &SimplifyContext) -> Result<Option<Expr>> {
    let Some(now) = start_time(info) else {
        return Ok(None);
    };
    let days = days_from_date(local_now(now, func.zone())?.date());
    Ok(Some(Expr::Literal(ScalarValue::Date32(Some(days)), None)))
}

pub(crate) fn current_timestamp() -> ScalarUDF {
    Func::new(
        "current_timestamp",
        coerce_precision,
        |r| {
            Ok(DataType::Timestamp(
                literal_precision(r)?,
                Some(Arc::from(r.zone)),
            ))
        },
        |call| repeat(&current_timestamp_value(Utc::now(), call.return_type)?, call.rows),
    )
    .alias("now")
    .volatility(Volatility::Stable)
    .zone_sensitive()
    .simplify_with(simplify_current_timestamp)
    .udf()
}

pub(crate) fn localtimestamp() -> ScalarUDF {
    Func::new(
        "localtimestamp",
        coerce_precision,
        |r| Ok(DataType::Timestamp(literal_precision(r)?, None)),
        |call| {
            repeat(
                &localtimestamp_value(Utc::now(), &call.zone, call.return_type)?,
                call.rows,
            )
        },
    )
    .volatility(Volatility::Stable)
    .zone_sensitive()
    .simplify_with(simplify_localtimestamp)
    .udf()
}

pub(crate) fn current_date() -> ScalarUDF {
    Func::new(
        "current_date",
        |name, types| coerce_each(name, types, &[]),
        |_| Ok(DataType::Date32),
        |call| {
            let days = days_from_date(local_now(Utc::now(), &call.zone)?.date());
            repeat(&ScalarValue::Date32(Some(days)), call.rows)
        },
    )
    .volatility(Volatility::Stable)
    .zone_sensitive()
    .simplify_with(simplify_current_date)
    .udf()
}

pub(crate) fn localtime() -> ScalarUDF {
    Func::new(
        "localtime",
        |name, types| coerce_each(name, types, &[]),
        |_| Ok(DataType::Time32(TimeUnit::Millisecond)),
        |call| {
            let local = local_now(Utc::now(), &call.zone)?;
            let millis = i32::try_from(local.num_seconds_from_midnight()).unwrap_or(0) * 1_000
                + i32::try_from(local.nanosecond() / 1_000_000).unwrap_or(0);
            Ok(Arc::new(Time32MillisecondArray::from(vec![
                millis;
                call.rows.max(1)
            ])))
        },
    )
    .volatility(Volatility::Stable)
    .zone_sensitive()
    .udf()
}

pub(crate) fn current_timezone() -> ScalarUDF {
    Func::new(
        "current_timezone",
        |name, types| coerce_each(name, types, &[]),
        |_| Ok(DataType::Utf8),
        |call| Ok(string_array(vec![Some(call.zone.to_string()); call.rows.max(1)])),
    )
    .volatility(Volatility::Stable)
    .zone_sensitive()
    .simplify_with(|func, _, _| {
        Ok(Some(Expr::Literal(
            ScalarValue::Utf8(Some(func.zone().to_string())),
            None,
        )))
    })
    .udf()
}

// ---------------------------------------------------------------------------
// Zones: with_timezone, at_timezone (AT TIME ZONE), timestamp casts
// ---------------------------------------------------------------------------

fn constant_zone(ret: &Ret<'_>, index: usize, function: &str) -> Result<Zone> {
    match ret.literal_str(index) {
        Some(id) => parse_zone(id),
        None => Err(DataFusionError::Plan(format!(
            "NOT_SUPPORTED: {function} needs a constant time zone (one zone per column)"
        ))),
    }
}

fn zoned_type(t: &DataType, zone: Zone) -> Result<DataType> {
    match t {
        DataType::Timestamp(unit, _) => Ok(DataType::Timestamp(*unit, Some(Arc::from(zone.name())))),
        other => Err(DataFusionError::Internal(format!("unexpected {other}"))),
    }
}

pub(crate) fn with_timezone() -> ScalarUDF {
    Func::new(
        "with_timezone",
        |name, types| {
            coerce_each(name, types, &[as_timestamp, as_str]).and_then(|t| match t.first() {
                Some(DataType::Timestamp(_, None)) => Ok(t),
                _ => Err(not_found(name, types)),
            })
        },
        |r| {
            let zone = constant_zone(r, 1, "with_timezone")?;
            zoned_type(&r.types[0], zone)
        },
        |call| {
            let ts = timestamps(call.arg(0)?)?;
            let zones = strings(call.arg(1)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(raw), Some(id)) = (ts.at(i), zones.at(i)) else {
                        return Ok(None);
                    };
                    let zone = parse_zone(id)?;
                    let local = to_naive(raw, ts.unit).ok_or_else(out_of_range_ts)?;
                    from_naive(zone.to_utc(local), ts.unit)
                        .ok_or_else(out_of_range_ts)
                        .map(Some)
                })
                .collect::<Result<Vec<_>>>()?;
            let DataType::Timestamp(unit, zone) = call.return_type else {
                return Err(DataFusionError::Internal("expected timestamp".into()));
            };
            timestamp_array(out, *unit, zone.clone())
        },
    )
    .udf()
}

/// Instants of a timestamp column: zoned values as they are, plain ones
/// interpreted in the session zone (Trino's `timestamp` → `timestamp with time zone`).
pub(crate) fn instants(array: &ArrayRef, session: &str, rows: usize) -> Result<(Vec<Option<i64>>, TimeUnit)> {
    let ts = timestamps(array)?;
    if ts.zone.is_some() {
        return Ok(((0..rows).map(|i| ts.at(i)).collect(), ts.unit));
    }
    let zone = parse_zone(session)?;
    let values = (0..rows)
        .map(|i| {
            ts.at(i)
                .map(|raw| {
                    let local = to_naive(raw, ts.unit).ok_or_else(out_of_range_ts)?;
                    from_naive(zone.to_utc(local), ts.unit).ok_or_else(out_of_range_ts)
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((values, ts.unit))
}

pub(crate) fn at_timezone() -> ScalarUDF {
    Func::new(
        "at_timezone",
        |name, types| coerce_each(name, types, &[as_timestamp, as_str]),
        |r| {
            let zone = constant_zone(r, 1, "AT TIME ZONE")?;
            zoned_type(&r.types[0], zone)
        },
        |call| {
            let (values, _) = instants(call.arg(0)?, &call.zone, call.rows)?;
            let zones = strings(call.arg(1)?)?;
            for i in 0..call.rows {
                if let Some(id) = zones.at(i) {
                    parse_zone(id)?;
                }
            }
            let DataType::Timestamp(unit, zone) = call.return_type else {
                return Err(DataFusionError::Internal("expected timestamp".into()));
            };
            timestamp_array(values, *unit, zone.clone())
        },
    )
    .zone_sensitive()
    .udf()
}

/// Wall clock of zoned values in their own zone (`CAST(tstz AS timestamp)`).
pub(crate) fn local_wall_clock(array: &ArrayRef, rows: usize) -> Result<Vec<Option<i64>>> {
    let ts = timestamps(array)?;
    let zone = column_zone(ts.zone.as_ref())?;
    (0..rows)
        .map(|i| {
            ts.at(i)
                .map(|raw| {
                    let utc = to_naive(raw, ts.unit).ok_or_else(out_of_range_ts)?;
                    let local = zone.map_or(utc, |z| z.to_local(utc));
                    from_naive(local, ts.unit).ok_or_else(out_of_range_ts)
                })
                .transpose()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Unix time and ISO 8601
// ---------------------------------------------------------------------------

/// Zone of `from_unixtime(t, hours, minutes)`: a fixed offset.
fn offset_zone(ret: &Ret<'_>) -> Result<Zone> {
    let minutes = |i: usize| match ret.scalars.get(i).copied().flatten() {
        Some(ScalarValue::Int64(Some(v))) => Some(*v),
        Some(ScalarValue::Int32(Some(v))) => Some(i64::from(*v)),
        Some(ScalarValue::Int16(Some(v))) => Some(i64::from(*v)),
        Some(ScalarValue::Int8(Some(v))) => Some(i64::from(*v)),
        _ => None,
    };
    let (Some(hours), Some(extra)) = (minutes(1), minutes(2)) else {
        return Err(DataFusionError::Plan(
            "NOT_SUPPORTED: from_unixtime needs constant hours and minutes (one zone per column)".into(),
        ));
    };
    let total = hours
        .checked_mul(60)
        .and_then(|m| m.checked_add(extra))
        .filter(|m| m.abs() <= 14 * 60)
        .ok_or_else(|| invalid_argument(format!("Invalid offset minutes {hours}:{extra}")))?;
    let seconds = i32::try_from(total * 60).map_err(|_| invalid_argument("Invalid offset"))?;
    Ok(Zone::Fixed(seconds))
}

pub(crate) fn from_unixtime() -> ScalarUDF {
    Func::new(
        "from_unixtime",
        |name, types| match types {
            // Hours and minutes keep their integer type: the zone is read
            // from their literal values when planning.
            [t, h, m] if super::support::is_integer(h) && super::support::is_integer(m) => Ok(vec![
                as_f64(t).ok_or_else(|| not_found(name, types))?,
                h.clone(),
                m.clone(),
            ]),
            _ => coerce_any(name, types, &[&[as_f64], &[as_f64, as_str]]),
        },
        |r| {
            let zone = match r.types.len() {
                2 => constant_zone(r, 1, "from_unixtime")?.name(),
                3 => offset_zone(r)?.name(),
                _ => r.zone.to_owned(),
            };
            Ok(DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from(zone))))
        },
        |call| {
            let seconds = floats(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    seconds
                        .at(i)
                        // Java's Math.round: half up.
                        .map(|s| (s * 1_000.0 + 0.5).floor().to_i64().ok_or_else(out_of_range_ts))
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let DataType::Timestamp(_, zone) = call.return_type else {
                return Err(DataFusionError::Internal("expected timestamp".into()));
            };
            timestamp_array(out, TimeUnit::Millisecond, zone.clone())
        },
    )
    .zone_sensitive()
    .udf()
}

pub(crate) fn from_unixtime_nanos() -> ScalarUDF {
    Func::new(
        "from_unixtime_nanos",
        |name, types| coerce_each(name, types, &[as_i64]),
        |r| Ok(DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(r.zone)))),
        |call| {
            let nanos = super::support::ints(call.arg(0)?)?;
            let out = (0..call.rows).map(|i| nanos.at(i)).collect();
            timestamp_array(out, TimeUnit::Nanosecond, Some(Arc::clone(&call.zone)))
        },
    )
    .zone_sensitive()
    .udf()
}

pub(crate) fn to_unixtime() -> ScalarUDF {
    Func::new(
        "to_unixtime",
        |name, types| coerce_each(name, types, &[as_timestamp]),
        |_| Ok(DataType::Float64),
        |call| {
            let (values, unit) = instants(call.arg(0)?, &call.zone, call.rows)?;
            let out = values
                .into_iter()
                .map(|v| {
                    // Trino: epoch millis / 1000 plus the sub-millisecond part.
                    v.and_then(|raw| {
                        let ms = millis_floor(raw, unit)?;
                        let per_ms = per_second(unit) / 1_000;
                        let sub = if per_ms > 1 {
                            raw - ms.checked_mul(per_ms)?
                        } else {
                            0
                        };
                        let sub_seconds =
                            super::cast::i64_to_f64(sub) / super::cast::i64_to_f64(per_second(unit));
                        Some(super::cast::i64_to_f64(ms) / 1_000.0 + sub_seconds)
                    })
                })
                .collect();
            Ok(float_array(out))
        },
    )
    .zone_sensitive()
    .udf()
}

fn to_iso8601_text(local: NaiveDateTime, precision: u32, offset: Option<i32>) -> String {
    let mut text = super::cast::render_timestamp(local, precision).replacen(' ', "T", 1);
    if let Some(offset) = offset {
        if offset == 0 {
            text.push('Z');
        } else {
            let sign = if offset < 0 { '-' } else { '+' };
            let abs = offset.abs();
            let _ = write!(text, "{sign}{:02}:{:02}", abs / 3600, (abs % 3600) / 60);
        }
    }
    text
}

pub(crate) fn to_iso8601() -> ScalarUDF {
    Func::new(
        "to_iso8601",
        |name, types| coerce_each(name, types, &[datetime_arg]),
        |_| Ok(DataType::Utf8),
        |call| {
            let arg = call.arg(0)?;
            let values = local_values(arg, call.rows)?;
            let (precision, zoned) = match arg.data_type() {
                DataType::Timestamp(unit, zone) => {
                    (Some(super::support::precision_of(*unit)), zone.is_some())
                }
                _ => (None, false),
            };
            let out = values
                .into_iter()
                .map(|v| {
                    v.map(|(local, offset)| match precision {
                        None => local.date().format("%Y-%m-%d").to_string(),
                        Some(p) => to_iso8601_text(local, p, zoned.then_some(offset)),
                    })
                })
                .collect();
            Ok(string_array(out))
        },
    )
    .udf()
}

/// Joda's ISO date element (Trino's `from_iso8601_date`): `yyyy`,
/// `yyyy-MM`, `yyyy-MM-dd`, the ordinal `yyyy-DDD` and the week date
/// `xxxx-Www[-e]`.
fn parse_iso8601_date(text: &str) -> Option<NaiveDate> {
    let (year, rest) = match text.split_once('-') {
        Some((year, rest)) => (year, Some(rest)),
        None => (text, None),
    };
    if year.len() < 4 || !year.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year: i32 = year.parse().ok()?;
    let digits = |s: &str, min: usize, max: usize| {
        (min..=max).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit())
    };
    let Some(rest) = rest else {
        return NaiveDate::from_ymd_opt(year, 1, 1);
    };
    if let Some(week) = rest.strip_prefix('W') {
        let (week, day) = match week.split_once('-') {
            Some((week, day)) if digits(day, 1, 1) => (week, day.parse::<u8>().ok()?),
            Some(_) => return None,
            None => (week, 1_u8),
        };
        if !digits(week, 2, 2) {
            return None;
        }
        let weekday = chrono::Weekday::try_from(day.checked_sub(1)?).ok()?;
        return NaiveDate::from_isoywd_opt(year, week.parse().ok()?, weekday);
    }
    match rest.split_once('-') {
        Some((month, day)) if digits(month, 1, 2) && digits(day, 1, 2) => {
            NaiveDate::from_ymd_opt(year, month.parse().ok()?, day.parse().ok()?)
        }
        None if digits(rest, 3, 3) => NaiveDate::from_yo_opt(year, rest.parse().ok()?),
        None if digits(rest, 1, 2) => NaiveDate::from_ymd_opt(year, rest.parse().ok()?, 1),
        _ => None,
    }
}

pub(crate) fn from_iso8601_date() -> ScalarUDF {
    Func::new(
        "from_iso8601_date",
        |name, types| coerce_each(name, types, &[as_str]),
        |_| Ok(DataType::Date32),
        |call| {
            let text = strings(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    text.at(i)
                        .map(|s| {
                            parse_iso8601_date(s)
                                .map(days_from_date)
                                .ok_or_else(|| invalid_argument(format!("Invalid format: \"{s}\"")))
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Date32Array::from(out)))
        },
    )
    .udf()
}

/// ISO 8601 date-time with an optional offset; values without one are in the session zone.
/// UTC instant of an ISO 8601 timestamp and the zone it names (`Z` or an
/// offset); text without a zone is read in `session`.
fn parse_iso8601_timestamp(text: &str, session: Zone) -> Option<(NaiveDateTime, Option<Zone>)> {
    let zoned = DateTime::parse_from_rfc3339(text).ok().or_else(|| {
        [
            "%Y-%m-%dT%H:%M:%S%.f%:z",
            "%Y-%m-%dT%H:%M%:z",
            "%Y-%m-%dT%H:%M:%S%.f%z",
        ]
        .iter()
        .find_map(|f| DateTime::parse_from_str(text, f).ok())
    });
    if let Some(dt) = zoned {
        return Some((dt.naive_utc(), Some(Zone::Fixed(dt.offset().local_minus_utc()))));
    }
    let local = ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H"]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(text, f).ok())
        .or_else(|| parse_iso8601_date(text).map(|d| d.and_time(chrono::NaiveTime::MIN)))?;
    Some((session.to_utc(local), None))
}

/// Zone of the result column: the literal's own zone, else the session's.
/// Arrow keeps one zone per column (see ADR 0008).
fn iso_result_zone(r: &Ret<'_>) -> String {
    let session = parse_zone(r.zone).ok();
    match (r.literal_str(0), session) {
        (Some(text), Some(session)) => parse_iso8601_timestamp(text, session)
            .and_then(|(_, zone)| zone)
            .map_or_else(|| r.zone.to_owned(), Zone::name),
        _ => r.zone.to_owned(),
    }
}

fn iso_timestamps(call: &Call<'_>, unit: TimeUnit) -> Result<ArrayRef> {
    let text = strings(call.arg(0)?)?;
    let session = parse_zone(&call.zone)?;
    let DataType::Timestamp(_, Some(column_zone)) = call.return_type else {
        return Err(DataFusionError::Internal("expected a zoned timestamp".into()));
    };
    let out = (0..call.rows)
        .map(|i| {
            let Some(s) = text.at(i) else { return Ok(None) };
            let (utc, zone) = parse_iso8601_timestamp(s, session)
                .ok_or_else(|| invalid_argument(format!("Invalid format: \"{s}\"")))?;
            let zone = zone.unwrap_or(session).name();
            if zone != column_zone.as_ref() {
                return Err(DataFusionError::Plan(format!(
                    "NOT_SUPPORTED: '{s}' is in time zone {zone}, the column in {column_zone}; \
                     a timestamp with time zone column holds one zone"
                )));
            }
            from_naive(utc, unit).ok_or_else(out_of_range_ts).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    timestamp_array(out, unit, Some(Arc::clone(column_zone)))
}

pub(crate) fn from_iso8601_timestamp() -> ScalarUDF {
    Func::new(
        "from_iso8601_timestamp",
        |name, types| coerce_each(name, types, &[as_str]),
        |r| {
            Ok(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some(Arc::from(iso_result_zone(r))),
            ))
        },
        |call| iso_timestamps(call, TimeUnit::Millisecond),
    )
    .zone_sensitive()
    .udf()
}

pub(crate) fn from_iso8601_timestamp_nanos() -> ScalarUDF {
    Func::new(
        "from_iso8601_timestamp_nanos",
        |name, types| coerce_each(name, types, &[as_str]),
        |r| {
            Ok(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from(iso_result_zone(r))),
            ))
        },
        |call| iso_timestamps(call, TimeUnit::Nanosecond),
    )
    .zone_sensitive()
    .udf()
}

/// `parse_duration('1.5h')`: Airlift's duration syntax, as an exact
/// day-to-second interval rounded to milliseconds.
fn parse_duration_millis(text: &str) -> Result<i64> {
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| c.is_ascii_alphabetic())
        .ok_or_else(|| invalid_argument(format!("duration is not a valid data duration string: {text}")))?;
    let (number, unit) = trimmed.split_at(split);
    let number: f64 = number
        .trim()
        .parse()
        .ok()
        .filter(|n: &f64| n.is_finite() && *n >= 0.0)
        .ok_or_else(|| invalid_argument(format!("duration is not a valid data duration string: {text}")))?;
    let millis_per_unit = match unit.trim() {
        "ns" => 1e-6,
        "us" => 1e-3,
        "ms" => 1.0,
        "s" => 1e3,
        "m" => 6e4,
        "h" => 3.6e6,
        "d" => 8.64e7,
        other => return Err(invalid_argument(format!("Unknown time unit: {other}"))),
    };
    (number * millis_per_unit)
        .round()
        .to_i64()
        .ok_or_else(|| out_of_range(format!("duration is too large: {text}")))
}

pub(crate) fn parse_duration() -> ScalarUDF {
    Func::new(
        "parse_duration",
        |name, types| coerce_each(name, types, &[as_str]),
        |_| {
            Ok(DataType::Interval(
                datafusion::arrow::datatypes::IntervalUnit::MonthDayNano,
            ))
        },
        |call| {
            use datafusion::arrow::array::IntervalMonthDayNanoArray;
            use datafusion::arrow::datatypes::IntervalMonthDayNano;
            let text = strings(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    let Some(s) = text.at(i) else { return Ok(None) };
                    let millis = parse_duration_millis(s)?;
                    let nanos = millis
                        .checked_mul(1_000_000)
                        .ok_or_else(|| out_of_range("duration is too large"))?;
                    Ok(Some(IntervalMonthDayNano::new(0, 0, nanos)))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(IntervalMonthDayNanoArray::from(out)))
        },
    )
    .udf()
}

/// `to_milliseconds(interval day to second)`.
pub(crate) fn to_milliseconds() -> ScalarUDF {
    use datafusion::arrow::datatypes::IntervalUnit;
    Func::new(
        "to_milliseconds",
        |name, types| match types {
            [DataType::Interval(IntervalUnit::MonthDayNano | IntervalUnit::DayTime) | DataType::Null] => {
                Ok(vec![DataType::Interval(IntervalUnit::MonthDayNano)])
            }
            _ => Err(not_found(name, types)),
        },
        |_| Ok(DataType::Int64),
        |call| {
            use datafusion::arrow::array::IntervalMonthDayNanoArray;
            let values = call
                .arg(0)?
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .ok_or_else(|| DataFusionError::Internal("expected an interval".into()))?;
            let out = (0..call.rows)
                .map(|i| {
                    if values.is_null(i) {
                        return Ok(None);
                    }
                    let v = values.value(i);
                    if v.months != 0 {
                        return Err(invalid_argument("to_milliseconds needs a day to second interval"));
                    }
                    i64::from(v.days)
                        .checked_mul(86_400_000)
                        .and_then(|d| d.checked_add(v.nanoseconds / 1_000_000))
                        .map(Some)
                        .ok_or_else(|| out_of_range("interval is too large"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

/// `human_readable_seconds(double)`: "1 week, 2 days, 3 hours, …".
pub(crate) fn human_readable_seconds() -> ScalarUDF {
    Func::new(
        "human_readable_seconds",
        |name, types| coerce_each(name, types, &[as_f64]),
        |_| Ok(DataType::Utf8),
        |call| {
            let values = floats(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| values.at(i).map(human_readable).transpose())
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

fn human_readable(seconds: f64) -> Result<String> {
    if seconds.is_nan() {
        return Err(invalid_argument("Invalid argument found: NaN"));
    }
    if seconds.is_infinite() {
        return Err(invalid_argument("Invalid argument found: Infinity"));
    }
    let rounded = seconds.abs().round();
    if rounded > 9.0e15 {
        return Err(out_of_range("value is too large"));
    }
    let mut remaining = rounded.to_i64().unwrap_or(i64::MAX);
    let units: [(&str, i64); 5] = [
        ("week", 604_800),
        ("day", 86_400),
        ("hour", 3_600),
        ("minute", 60),
        ("second", 1),
    ];
    let mut parts = Vec::new();
    for (name, size) in units {
        let count = remaining / size;
        remaining %= size;
        if count > 0 {
            parts.push(format!("{count} {name}{}", if count == 1 { "" } else { "s" }));
        }
    }
    if parts.is_empty() {
        return Ok("0 seconds".to_owned());
    }
    Ok(parts.join(", "))
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        date_add(),
        date_diff(),
        date_trunc(),
        date_part(),
        year(),
        quarter(),
        month(),
        week(),
        day(),
        day_of_week(),
        day_of_year(),
        year_of_week(),
        hour(),
        minute(),
        second(),
        millisecond(),
        timezone_hour(),
        timezone_minute(),
        last_day_of_month(),
        date(),
        current_timestamp(),
        localtimestamp(),
        current_date(),
        localtime(),
        current_timezone(),
        with_timezone(),
        at_timezone(),
        from_unixtime(),
        from_unixtime_nanos(),
        to_unixtime(),
        to_iso8601(),
        from_iso8601_date(),
        from_iso8601_timestamp(),
        from_iso8601_timestamp_nanos(),
        parse_duration(),
        to_milliseconds(),
        human_readable_seconds(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").unwrap()
    }

    #[test]
    fn month_difference_follows_joda() {
        assert_eq!(
            month_difference(dt("2024-02-29 00:00:00"), dt("2024-01-31 00:00:00")),
            1
        );
        assert_eq!(
            month_difference(dt("2024-02-28 00:00:00"), dt("2024-01-31 00:00:00")),
            0
        );
        assert_eq!(
            month_difference(dt("2024-01-31 00:00:00"), dt("2024-02-29 00:00:00")),
            -1
        );
        // Joda compares the time of month: 30 days 10h after March 1st is past 28 days 11h.
        assert_eq!(
            month_difference(dt("2024-03-31 10:00:00"), dt("2024-02-29 11:00:00")),
            1
        );
        assert_eq!(
            month_difference(dt("2024-03-01 00:00:00"), dt("2023-03-01 00:00:00")),
            12
        );
    }

    #[test]
    fn year_difference_balances_leap_days() {
        assert_eq!(
            year_difference(dt("2025-02-28 00:00:00"), dt("2024-02-29 00:00:00")),
            1
        );
        assert_eq!(
            year_difference(dt("2025-02-27 00:00:00"), dt("2024-02-29 00:00:00")),
            0
        );
        assert_eq!(
            year_difference(dt("2024-02-29 00:00:00"), dt("2023-03-01 00:00:00")),
            0
        );
        assert_eq!(
            year_difference(dt("2024-03-01 00:00:00"), dt("2023-03-01 00:00:00")),
            1
        );
        assert_eq!(
            year_difference(dt("2023-01-01 00:00:00"), dt("2024-01-01 00:00:00")),
            -1
        );
    }

    #[test]
    fn add_months_clamps_to_month_end() {
        assert_eq!(
            add_field(dt("2024-01-31 10:00:00"), Field::Month, 1).unwrap(),
            dt("2024-02-29 10:00:00")
        );
        assert_eq!(
            add_field(dt("2024-02-29 00:00:00"), Field::Year, 1).unwrap(),
            dt("2025-02-28 00:00:00")
        );
        assert_eq!(
            add_field(dt("2024-03-31 00:00:00"), Field::Quarter, -1).unwrap(),
            dt("2023-12-31 00:00:00")
        );
        assert!(add_field(dt("2024-03-31 00:00:00"), Field::Day, i64::MAX).is_err());
    }

    #[test]
    fn precise_differences_truncate_toward_zero() {
        assert_eq!(field_difference(Field::Second, 1_000, 500).unwrap(), 0);
        assert_eq!(field_difference(Field::Second, 0, -1_500).unwrap(), -1);
        assert_eq!(field_difference(Field::Day, 0, MILLIS_PER_DAY - 1).unwrap(), 0);
    }

    #[test]
    fn truncation_units() {
        let t = dt("2024-02-29 10:11:12.345");
        assert_eq!(truncate_naive(t, Field::Week), dt("2024-02-26 00:00:00"));
        assert_eq!(truncate_naive(t, Field::Quarter), dt("2024-01-01 00:00:00"));
        assert_eq!(truncate_naive(t, Field::Second), dt("2024-02-29 10:11:12"));
        assert_eq!(truncate_naive(t, Field::Millisecond), t);
    }

    #[test]
    fn unit_conversions_round_trip_negative_values() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            for raw in [-1_i64, 0, 1, -1_234_567, 1_234_567] {
                assert_eq!(
                    from_naive(to_naive(raw, unit).unwrap(), unit),
                    Some(raw),
                    "{unit:?} {raw}"
                );
            }
        }
    }

    #[test]
    fn human_readable_formats() {
        assert_eq!(human_readable(96.0).unwrap(), "1 minute, 36 seconds");
        assert_eq!(human_readable(0.0).unwrap(), "0 seconds");
        assert!(human_readable(f64::NAN).is_err());
    }
}
