//! Trino's casts from text to `timestamp(p)` and `timestamp(p) with time zone`.
//!
//! Trino 431 reads `YYYY-MM-DD[ H:M[:S[.f…]]][ <zone>]` (a year of four or
//! more digits, optionally signed; one or two digits for the other fields;
//! surrounding spaces ignored; `T` is not a separator) and rounds the
//! fraction half up to the target precision. The zone is validated but
//! **ignored** for `timestamp(p)` — `CAST('2026-01-01 10:00:00
//! America/Sao_Paulo' AS TIMESTAMP)` is `10:00`, the local time as written —
//! and places the local time for `timestamp(p) with time zone`, whose
//! default is the session zone. A local time in a daylight-saving gap moves
//! forward by the gap; in an overlap it takes the earlier offset. Anything
//! else fails with `Value cannot be cast to timestamp: <text>`
//! (`INVALID_CAST_ARGUMENT`). Arrow's cast instead converts a zoned value to
//! UTC (`13:00`), accepts other layouts and truncates.
//!
//! The engine keeps `CAST` in the plan (so a federated sub-plan renders it
//! as SQL for its source) and evaluates casts from text that stay in the
//! engine with [`TextToTimestamp`] (see [`crate::overflow`], which swaps it
//! in and out around federation).
//!
//! `timestamp` without precision is `timestamp(3)` in Trino, and a zone of a
//! `timestamp with time zone` travels in the Arrow type; [`trino_zone`]
//! normalizes a zone the way Trino prints it (`+00:00` → `UTC`,
//! `Etc/GMT+3` → `-03:00`).

use std::str::FromStr;
use std::sync::Arc;

use chrono::{Duration, LocalResult, NaiveDate, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TryCast, Volatility,
};

/// A zone as Trino names it, with its Arrow time zone.
#[derive(Debug, Clone)]
pub(crate) struct Zone {
    /// `UTC`, `+03:00` or an IANA name, as Trino prints it.
    pub(crate) name: String,
    tz: Tz,
}

fn offset_name(sign: char, hours: u32, minutes: u32) -> Option<String> {
    if hours > 14 || minutes > 59 || (hours == 14 && minutes > 0) {
        return None;
    }
    if hours == 0 && minutes == 0 {
        return Some("UTC".into());
    }
    Some(format!("{sign}{hours:02}:{minutes:02}"))
}

/// `+3`, `+03`, `+0300`, `+03:00` → `+03:00`.
fn parse_offset(text: &str) -> Option<String> {
    let sign = text.chars().next().filter(|c| *c == '+' || *c == '-')?;
    let rest = &text[1..];
    let (hours, minutes) = match rest.split_once(':') {
        Some((h, m)) if m.len() == 2 => (h, m),
        Some(_) => return None,
        None if rest.len() == 4 => rest.split_at(2),
        None => (rest, "0"),
    };
    let digits = |s: &str| !s.is_empty() && s.len() <= 2 && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(hours) || !digits(minutes) {
        return None;
    }
    offset_name(sign, hours.parse().ok()?, minutes.parse().ok()?)
}

/// A zone in Trino's spelling (see the module documentation), or `None`.
pub(crate) fn trino_zone(text: &str) -> Option<Zone> {
    let name = if [
        "Z", "UTC", "GMT", "UT", "Etc/UTC", "Etc/GMT", "Etc/UCT", "UCT", "Zulu",
    ]
    .contains(&text)
    {
        "UTC".to_owned()
    } else if text.starts_with(['+', '-']) {
        parse_offset(text)?
    } else if let Some(offset) = ["UTC", "GMT", "UT"]
        .iter()
        .find_map(|prefix| text.strip_prefix(prefix))
        .filter(|rest| rest.starts_with(['+', '-']))
    {
        parse_offset(offset)?
    } else if let Some(offset) = text.strip_prefix("Etc/GMT") {
        // POSIX sign: Etc/GMT+3 is three hours *behind* UTC.
        let flipped = match offset.chars().next()? {
            '+' => format!("-{}", &offset[1..]),
            '-' => format!("+{}", &offset[1..]),
            _ => return None,
        };
        parse_offset(&flipped)?
    } else if text.contains('/')
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "/_-+".contains(c))
    {
        text.to_owned()
    } else {
        return None;
    };
    let tz = Tz::from_str(if name == "UTC" { "+00:00" } else { &name }).ok()?;
    Some(Zone { name, tz })
}

/// A time zone as Trino names it (`UTC`, `-03:00`, `America/Sao_Paulo`), or
/// `None` when Trino does not know it: how a client's session zone is read.
pub fn trino_time_zone(text: &str) -> Option<String> {
    trino_zone(text).map(|zone| zone.name)
}

/// The Arrow time zone of a zone name as the engine stores it.
pub(crate) fn arrow_tz(name: &str) -> Result<Tz, ArrowError> {
    Tz::from_str(if name == "UTC" { "+00:00" } else { name })
}

/// The zone of an Arrow timestamp type as Trino prints it.
pub(crate) fn zone_label(name: &str) -> String {
    trino_zone(name).map_or_else(|| name.to_owned(), |zone| zone.name)
}

/// The zone a timestamp text names, as Trino prints it (`None` without a
/// zone, or when the text is not a timestamp).
pub(crate) fn zone_of_text(text: &str) -> Option<String> {
    trino_zone(parse_parts(text)?.zone?).map(|zone| zone.name)
}

/// A text value split the way Trino reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Parts<'a> {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    /// Fraction digits as written (any count).
    fraction: &'a str,
    zone: Option<&'a str>,
}

fn take_digits(text: &str, min: usize, max: usize) -> Option<(&str, &str)> {
    let end = text.bytes().take_while(u8::is_ascii_digit).count().min(max);
    (end >= min).then(|| text.split_at(end))
}

/// `H:M[:S[.f…]]` at the start of `text` → (h, m, s, fraction, rest).
fn parse_time(text: &str) -> Option<(u32, u32, u32, &str, &str)> {
    let (hour, rest) = take_digits(text, 1, 2)?;
    let rest = rest.strip_prefix(':')?;
    let (minute, mut rest) = take_digits(rest, 1, 2)?;
    let (mut second, mut fraction) = ("0", "");
    if let Some(after) = rest.strip_prefix(':')
        && let Some((s, after)) = take_digits(after, 1, 2)
    {
        second = s;
        rest = after;
        if let Some(after) = rest.strip_prefix('.')
            && let Some((f, after)) = take_digits(after, 1, usize::MAX)
        {
            fraction = f;
            rest = after;
        }
    }
    Some((
        hour.parse().ok()?,
        minute.parse().ok()?,
        second.parse().ok()?,
        fraction,
        rest,
    ))
}

fn parse_parts(text: &str) -> Option<Parts<'_>> {
    let text = text.trim_matches(|c: char| c <= ' ');
    let (sign, unsigned) = match text.strip_prefix(['+', '-']) {
        Some(rest) => (text.as_bytes()[0], rest),
        None => (b'+', text),
    };
    let (year, rest) = take_digits(unsigned, 4, 9)?;
    let (month, rest) = take_digits(rest.strip_prefix('-')?, 1, 2)?;
    let (day, rest) = take_digits(rest.strip_prefix('-')?, 1, 2)?;
    let year: i32 = year.parse().ok()?;
    let mut parts = Parts {
        year: if sign == b'-' { -year } else { year },
        month: month.parse().ok()?,
        day: day.parse().ok()?,
        hour: 0,
        minute: 0,
        second: 0,
        fraction: "",
        zone: None,
    };
    if rest.is_empty() {
        return Some(parts);
    }
    let mut rest = rest.strip_prefix(' ')?;
    if let Some((hour, minute, second, fraction, after)) = parse_time(rest) {
        (parts.hour, parts.minute, parts.second, parts.fraction) = (hour, minute, second, fraction);
        rest = after;
    }
    let zone = rest.trim_start_matches(|c: char| c.is_whitespace());
    if !zone.is_empty() {
        parts.zone = Some(zone);
    }
    Some(parts)
}

fn digits_of(unit: TimeUnit) -> u32 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// The fraction rounded half up to `digits` digits: (value, carry into the
/// seconds).
fn round_fraction(fraction: &str, digits: u32) -> Option<(i64, i64)> {
    let digits = usize::try_from(digits).ok()?;
    let kept: String = fraction
        .chars()
        .chain(std::iter::repeat('0'))
        .take(digits)
        .collect();
    let mut value: i64 = if kept.is_empty() { 0 } else { kept.parse().ok()? };
    if fraction.as_bytes().get(digits).is_some_and(|b| *b >= b'5') {
        value += 1;
    }
    let one = 10_i64.checked_pow(u32::try_from(digits).ok()?)?;
    Some((value % one, value / one))
}

fn invalid(text: &str) -> ArrowError {
    ArrowError::CastError(format!("Value cannot be cast to timestamp: {text}"))
}

/// Offset in seconds of local time `local` in `zone` (gap: the offset before
/// it, so the time moves forward; overlap: the earlier offset).
pub(crate) fn offset_seconds(zone: Tz, local: &chrono::NaiveDateTime) -> i64 {
    let offset = match zone.offset_from_local_datetime(local) {
        LocalResult::Single(offset) | LocalResult::Ambiguous(offset, _) => offset,
        LocalResult::None => zone.offset_from_utc_datetime(&(*local - Duration::days(1))),
    };
    i64::from(offset.fix().local_minus_utc())
}

/// Offset in seconds of `zone` at the UTC instant `seconds`.
pub(crate) fn offset_at_instant(zone: Tz, seconds: i64) -> i64 {
    let utc = chrono::DateTime::from_timestamp(seconds, 0)
        .map_or_else(chrono::NaiveDateTime::default, |d| d.naive_utc());
    i64::from(zone.offset_from_utc_datetime(&utc).fix().local_minus_utc())
}

/// Seconds of `local` (as if UTC) once placed in `zone` and read back: the
/// same time, except in a daylight-saving gap, which it leaves forward.
fn normalized_local(zone: Tz, local: &chrono::NaiveDateTime) -> i64 {
    let instant = local.and_utc().timestamp() - offset_seconds(zone, local);
    instant + offset_at_instant(zone, instant)
}

/// `text` as a value of `Timestamp(unit, zone)` (see the module
/// documentation). `default_zone` places a text without zone.
fn parse_timestamp(text: &str, unit: TimeUnit, target_zone: Option<&Tz>) -> Result<i64, ArrowError> {
    let parts = parse_parts(text).ok_or_else(|| invalid(text))?;
    let zone = match parts.zone {
        Some(name) => Some(trino_zone(name).ok_or_else(|| invalid(text))?),
        None => None,
    };
    let local = NaiveDate::from_ymd_opt(parts.year, parts.month, parts.day)
        .and_then(|d| d.and_hms_opt(parts.hour, parts.minute, parts.second))
        .ok_or_else(|| invalid(text))?;
    let digits = digits_of(unit);
    let (fraction, carry) = round_fraction(parts.fraction, digits).ok_or_else(|| invalid(text))?;
    let seconds = match (target_zone, &zone) {
        (Some(target), _) => {
            local.and_utc().timestamp() - offset_seconds(zone.as_ref().map_or(*target, |z| z.tz), &local)
        }
        // A zone the text names still places the local time: one in a
        // daylight-saving gap moves forward (Trino reads the value in its
        // zone, then takes its local time).
        (None, Some(zone)) => normalized_local(zone.tz, &local),
        (None, None) => local.and_utc().timestamp(),
    };
    let scale = 10_i64.pow(digits);
    (seconds + carry)
        .checked_mul(scale)
        .and_then(|v| v.checked_add(fraction))
        .ok_or_else(|| invalid(text))
}

fn texts(array: &ArrayRef) -> Result<Vec<Option<&str>>> {
    Ok(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().iter().collect(),
        DataType::LargeUtf8 => array.as_string::<i64>().iter().collect(),
        DataType::Utf8View => array.as_string_view().iter().collect(),
        other => return exec_err!("expected text, got {other}"),
    })
}

pub(crate) fn timestamps(unit: TimeUnit, values: Vec<Option<i64>>, zone: Option<Arc<str>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => {
            Arc::new(PrimitiveArray::<TimestampSecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Millisecond => {
            Arc::new(PrimitiveArray::<TimestampMillisecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(PrimitiveArray::<TimestampMicrosecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(PrimitiveArray::<TimestampNanosecondType>::from(values).with_timezone_opt(zone))
        }
    }
}

/// `CAST(<text> AS timestamp…)` with Trino's semantics; `TRY_CAST` when
/// `safe` (NULL instead of an error).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct TextToTimestamp {
    target: DataType,
    safe: bool,
    signature: Signature,
}

impl TextToTimestamp {
    pub(crate) fn new(target: DataType, safe: bool) -> Self {
        Self {
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
}

impl ScalarUDFImpl for TextToTimestamp {
    fn name(&self) -> &'static str {
        if self.safe {
            "codelake_try_text_to_timestamp"
        } else {
            "codelake_text_to_timestamp"
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
        let DataType::Timestamp(unit, zone) = &self.target else {
            return exec_err!("{} returns a timestamp, not {}", self.name(), self.target);
        };
        let target_zone = match zone {
            Some(name) => Some(Tz::from_str(name)?),
            None => None,
        };
        let rows = match value {
            ColumnarValue::Scalar(_) => 1,
            ColumnarValue::Array(_) => args.number_rows,
        };
        let array = value.to_array(rows)?;
        let mut values = Vec::with_capacity(array.len());
        for text in texts(&array)? {
            values.push(match text {
                None => None,
                Some(text) => match parse_timestamp(text, *unit, target_zone.as_ref()) {
                    Ok(value) => Some(value),
                    Err(_) if self.safe => None,
                    Err(error) => return Err(error.into()),
                },
            });
        }
        let result = timestamps(*unit, values, zone.clone());
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(result),
        })
    }
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn text_to_timestamp(arg: Expr, target: DataType, safe: bool) -> Expr {
    ScalarUDF::new_from_impl(TextToTimestamp::new(target, safe)).call(vec![arg])
}

/// `CAST(<text> AS timestamp…)`, `TRY_CAST` and `arrow_cast(<text>,
/// 'Timestamp(…)')` → [`TextToTimestamp`].
pub(crate) fn check_text_cast(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let replaced = match &expr {
        Expr::Cast(Cast { expr: arg, field }) | Expr::TryCast(TryCast { expr: arg, field })
            if matches!(field.data_type(), DataType::Timestamp(..)) && is_text(&arg.get_type(schema)?) =>
        {
            Some(text_to_timestamp(
                arg.as_ref().clone(),
                field.data_type().clone(),
                matches!(expr, Expr::TryCast(_)),
            ))
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if (func.name() == "arrow_cast" || func.name() == crate::timestamp_casts::TRY_ARROW_CAST)
                && args.len() == 2
                && is_text(&args[0].get_type(schema)?) =>
        {
            let safe = func.name() == crate::timestamp_casts::TRY_ARROW_CAST;
            match expr.get_type(schema)? {
                target @ DataType::Timestamp(..) => Some(text_to_timestamp(args[0].clone(), target, safe)),
                _ => None,
            }
        }
        // `CAST(CAST(<text> AS … WITH TIME ZONE) AS timestamp(p))`: each text's
        // own zone gives its local time (the zone of the column type would
        // give UTC's), see `fuse`.
        Expr::Cast(Cast { expr: arg, field }) | Expr::TryCast(TryCast { expr: arg, field }) => {
            fuse(arg, field.data_type(), matches!(expr, Expr::TryCast(_)))
        }
        _ => None,
    };
    Ok(match replaced {
        Some(new) => Transformed::yes(new),
        None => Transformed::no(expr),
    })
}

/// `CAST(<text → timestamp with time zone> AS timestamp(p))` → the text read
/// as `timestamp(p)`: the local time each text names, moved forward out of a
/// daylight-saving gap of its zone, as Trino answers it. A cast to `date`
/// takes the date of that local time.
fn fuse(inner: &Expr, target: &DataType, safe: bool) -> Option<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = inner else {
        return None;
    };
    let text = func.inner().downcast_ref::<TextToTimestamp>()?;
    let [arg] = args.as_slice() else {
        return None;
    };
    // `CAST(<text → with time zone> AS DATE)`: the date where each value is,
    // its local date (the zone of the column type would give UTC's).
    if let (DataType::Timestamp(zoned_unit, Some(_)), DataType::Date32 | DataType::Date64) =
        (&text.target, target)
    {
        let local = text_to_timestamp(
            arg.clone(),
            DataType::Timestamp(*zoned_unit, None),
            safe || text.safe,
        );
        return Some(Expr::Cast(Cast::new(Box::new(local), target.clone())));
    }
    let (DataType::Timestamp(zoned_unit, Some(_)), DataType::Timestamp(unit, None)) = (&text.target, target)
    else {
        return None;
    };
    // The coarser of the two precisions rounds; widening back is exact.
    let coarser = if digits_of(*zoned_unit) < digits_of(*unit) {
        *zoned_unit
    } else {
        *unit
    };
    let parsed = text_to_timestamp(arg.clone(), DataType::Timestamp(coarser, None), safe || text.safe);
    Some(if coarser == *unit {
        parsed
    } else {
        Expr::Cast(Cast::new(Box::new(parsed), target.clone()))
    })
}

/// [`TextToTimestamp`] → the cast it replaces (for SQL sources).
pub(crate) fn uncheck_text_cast(expr: Expr) -> Transformed<Expr> {
    let Expr::ScalarFunction(ScalarFunction { func, mut args }) = expr else {
        return Transformed::no(expr);
    };
    if let Some(cast) = func.inner().downcast_ref::<TextToTimestamp>()
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

    fn millis(text: &str, zone: Option<&str>) -> Result<i64, ArrowError> {
        let zone = zone.map(|z| Tz::from_str(z).unwrap());
        parse_timestamp(text, TimeUnit::Millisecond, zone.as_ref())
    }

    /// 2026-01-01 10:00:00 as if UTC, in milliseconds.
    const TEN_AM: i64 = 1_767_261_600_000;

    /// Values observed on Trino 431.
    #[test]
    fn a_zone_is_ignored_without_time_zone() {
        for text in [
            "2026-01-01 10:00:00 America/Sao_Paulo",
            "2026-01-01 10:00:00 +03:00",
            "2026-01-01 10:00:00Z",
            " 2026-01-01 10:00:00 ",
            "2026-01-01 10:00",
            "2026-1-1 10:0:0",
            "2026-01-01 10:00:00+03:00",
        ] {
            assert_eq!(millis(text, None).unwrap(), TEN_AM, "{text}");
        }
        assert_eq!(millis("2026-01-01", None).unwrap(), TEN_AM - 36_000_000);
    }

    #[test]
    fn a_zone_places_the_local_time_with_time_zone() {
        let hour = 3_600_000;
        assert_eq!(
            millis("2026-01-01 10:00:00 America/Sao_Paulo", Some("+00:00")).unwrap(),
            TEN_AM + 3 * hour
        );
        assert_eq!(
            millis("2026-01-01 10:00:00 +03:00", Some("America/Sao_Paulo")).unwrap(),
            TEN_AM - 3 * hour
        );
        // Without a zone: the target's (the session's).
        assert_eq!(
            millis("2026-01-01 10:00:00", Some("America/Sao_Paulo")).unwrap(),
            TEN_AM + 3 * hour
        );
        // New York: 02:30 does not exist on 2026-03-08 (→ 03:30 EDT, 07:30
        // UTC); 01:30 happens twice on 2026-11-01 (→ the first, EDT, 05:30 UTC).
        let ny = Some("America/New_York");
        assert_eq!(
            millis("2026-03-08 02:30:00", ny).unwrap(),
            millis("2026-03-08 07:30:00", None).unwrap()
        );
        assert_eq!(
            millis("2026-11-01 01:30:00", ny).unwrap(),
            millis("2026-11-01 05:30:00", None).unwrap()
        );
    }

    #[test]
    fn fractions_round_half_up() {
        assert_eq!(millis("2026-01-01 10:00:00.1235", None).unwrap(), TEN_AM + 124);
        assert_eq!(millis("2026-01-01 10:00:00.1234", None).unwrap(), TEN_AM + 123);
        assert_eq!(
            millis("2026-01-01 23:59:59.9995", None).unwrap(),
            millis("2026-01-02", None).unwrap()
        );
        assert_eq!(
            parse_timestamp("2026-01-01 10:00:00.5", TimeUnit::Second, None).unwrap(),
            TEN_AM / 1000 + 1
        );
        assert_eq!(
            parse_timestamp("2026-01-01 10:00:00.123456789123", TimeUnit::Nanosecond, None).unwrap(),
            TEN_AM * 1_000_000 + 123_456_789
        );
    }

    #[test]
    fn other_layouts_are_refused() {
        for text in [
            "garbage",
            "2026-02-30 10:00:00",
            "2026-01-01T10:00:00",
            "2026-01-01 10",
            "2026-01-01 10:00:00.",
            "26-01-01",
            "2026-01-01  10:00",
            "2026-01-01 24:00",
            "2026-01-01 10:00:00 Mars/Olympus",
            "2026-01-01 10:00:00 america/sao_paulo",
            "2026-01-01 10:00:00 EST",
            "2026-01-01 10:00:00 +15:00",
        ] {
            assert!(
                matches!(millis(text, None), Err(ArrowError::CastError(m)) if m == format!("Value cannot be cast to timestamp: {text}")),
                "{text}"
            );
        }
    }

    #[test]
    fn zones_are_named_like_trino() {
        let name = |z: &str| trino_zone(z).map(|z| z.name);
        for (text, expected) in [
            ("Z", "UTC"),
            ("GMT", "UTC"),
            ("+00:00", "UTC"),
            ("-00:00", "UTC"),
            ("Etc/UTC", "UTC"),
            ("UTC+00:00", "UTC"),
            ("Etc/GMT+3", "-03:00"),
            ("UTC-03:00", "-03:00"),
            ("GMT-3", "-03:00"),
            ("UTC+14", "+14:00"),
            ("+3", "+03:00"),
            ("+0300", "+03:00"),
            ("-03:30", "-03:30"),
            ("Europe/Lisbon", "Europe/Lisbon"),
        ] {
            assert_eq!(name(text).as_deref(), Some(expected), "{text}");
        }
        for text in ["EST", "BRT", "+15:00", "Mars/Olympus", "america/sao_paulo"] {
            assert!(trino_zone(text).is_none(), "{text}");
        }
    }
}
