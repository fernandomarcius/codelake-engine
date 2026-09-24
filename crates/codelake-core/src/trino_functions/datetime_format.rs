//! `date_format`/`date_parse` (MySQL specifiers) and `format_datetime`/
//! `parse_datetime` (Joda patterns), as Trino implements them on top of Joda
//! with the English locale.
//!
//! Both syntaxes compile to the same tokens, which print and parse. Trino
//! formats timestamps at millisecond precision. Joda's time zone *names*
//! (`z`) come from Java locale data; only offsets and ids are supported.

use std::fmt::Write as _;

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

use super::datetime::{column_zone, date_from_days, from_naive, parse_zone, to_naive};
use super::support::{
    At, Call, Func, Ret, UTF8, as_str, coerce_each, dates, invalid_argument, not_found, string_array,
    strings, timestamp_array, timestamps,
};
use super::zone::Zone;

const DAYS: [&str; 7] = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
];
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Era,
    Century,
    Year,
    YearOfEra,
    Weekyear,
    TwoDigitYear,
    TwoDigitWeekyear,
    WeekOfWeekyear,
    DayOfWeek,
    DayOfWeekText {
        full: bool,
    },
    DayOfYear,
    Month,
    MonthText {
        full: bool,
    },
    DayOfMonth,
    Halfday,
    HourOfHalfday,
    ClockhourOfHalfday,
    HourOfDay,
    ClockhourOfDay,
    Minute,
    Second,
    Fraction {
        min: usize,
        max: usize,
    },
    Offset {
        colon: bool,
    },
    ZoneId,
    /// Short zone name (`z`), print only.
    ZoneName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Literal(String),
    Field { kind: Kind, min: usize, max: usize },
}

fn field(kind: Kind, min: usize, max: usize) -> Token {
    Token::Field { kind, min, max }
}

impl Kind {
    fn is_numeric(self) -> bool {
        !matches!(
            self,
            Kind::Era
                | Kind::DayOfWeekText { .. }
                | Kind::MonthText { .. }
                | Kind::Halfday
                | Kind::Offset { .. }
                | Kind::ZoneId
                | Kind::ZoneName
        )
    }

    fn is_zone(self) -> bool {
        matches!(self, Kind::Offset { .. } | Kind::ZoneId | Kind::ZoneName)
    }
}

/// MySQL specifiers as Trino maps them to Joda (`DateTimeFunctions`).
fn mysql_tokens(format: &str) -> Result<Vec<Token>> {
    // Joda builds no formatter from an empty pattern.
    if format.is_empty() {
        return Err(invalid_argument("Both printing and parsing not supported"));
    }
    let mut tokens = Vec::new();
    let mut literal = String::new();
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            literal.push(c);
            continue;
        }
        let Some(spec) = chars.next() else { break };
        let token = match spec {
            'a' => field(Kind::DayOfWeekText { full: false }, 0, 0),
            'b' => field(Kind::MonthText { full: false }, 0, 0),
            'c' => field(Kind::Month, 1, 2),
            'd' => field(Kind::DayOfMonth, 2, 2),
            'e' => field(Kind::DayOfMonth, 1, 2),
            'f' => field(Kind::Fraction { min: 6, max: 9 }, 6, 9),
            'H' => field(Kind::HourOfDay, 2, 2),
            'h' | 'I' => field(Kind::ClockhourOfHalfday, 2, 2),
            'i' => field(Kind::Minute, 2, 2),
            'j' => field(Kind::DayOfYear, 3, 3),
            'k' => field(Kind::HourOfDay, 1, 2),
            'l' => field(Kind::ClockhourOfHalfday, 1, 2),
            'M' => field(Kind::MonthText { full: true }, 0, 0),
            'm' => field(Kind::Month, 2, 2),
            'p' => field(Kind::Halfday, 0, 0),
            'r' => {
                push_literal(&mut tokens, &mut literal);
                tokens.extend([
                    field(Kind::ClockhourOfHalfday, 2, 2),
                    Token::Literal(":".into()),
                    field(Kind::Minute, 2, 2),
                    Token::Literal(":".into()),
                    field(Kind::Second, 2, 2),
                    Token::Literal(" ".into()),
                ]);
                field(Kind::Halfday, 0, 0)
            }
            'S' | 's' => field(Kind::Second, 2, 2),
            'T' => {
                push_literal(&mut tokens, &mut literal);
                tokens.extend([
                    field(Kind::HourOfDay, 2, 2),
                    Token::Literal(":".into()),
                    field(Kind::Minute, 2, 2),
                    Token::Literal(":".into()),
                ]);
                field(Kind::Second, 2, 2)
            }
            'v' => field(Kind::WeekOfWeekyear, 2, 2),
            'x' => field(Kind::Weekyear, 4, 4),
            'W' => field(Kind::DayOfWeekText { full: true }, 0, 0),
            'Y' => field(Kind::Year, 4, 4),
            'y' => field(Kind::TwoDigitYear, 2, 2),
            'w' | 'D' | 'U' | 'u' | 'V' | 'X' => {
                return Err(invalid_argument(format!(
                    "%{spec} not supported in date format string"
                )));
            }
            other => {
                literal.push(other);
                continue;
            }
        };
        push_literal(&mut tokens, &mut literal);
        tokens.push(token);
    }
    push_literal(&mut tokens, &mut literal);
    Ok(tokens)
}

fn push_literal(tokens: &mut Vec<Token>, literal: &mut String) {
    if !literal.is_empty() {
        tokens.push(Token::Literal(std::mem::take(literal)));
    }
}

fn joda_field(letter: char, count: usize) -> Result<Token> {
    Ok(match letter {
        'G' => field(Kind::Era, 0, 0),
        'C' => field(Kind::Century, count, 3),
        'Y' | 'y' if count == 2 => field(Kind::TwoDigitYear, 2, 2),
        'Y' => field(Kind::YearOfEra, count, 9),
        'y' => field(Kind::Year, count, 9),
        'x' if count == 2 => field(Kind::TwoDigitWeekyear, 2, 2),
        'x' => field(Kind::Weekyear, count, 9),
        'w' => field(Kind::WeekOfWeekyear, count, 2),
        'e' => field(Kind::DayOfWeek, count, 1),
        'E' => field(Kind::DayOfWeekText { full: count >= 4 }, 0, 0),
        'D' => field(Kind::DayOfYear, count, 3),
        'M' if count >= 3 => field(Kind::MonthText { full: count >= 4 }, 0, 0),
        'M' => field(Kind::Month, count, 2),
        'd' => field(Kind::DayOfMonth, count, 2),
        'a' => field(Kind::Halfday, 0, 0),
        'K' => field(Kind::HourOfHalfday, count, 2),
        'h' => field(Kind::ClockhourOfHalfday, count, 2),
        'H' => field(Kind::HourOfDay, count, 2),
        'k' => field(Kind::ClockhourOfDay, count, 2),
        'm' => field(Kind::Minute, count, 2),
        's' => field(Kind::Second, count, 2),
        'S' => field(
            Kind::Fraction {
                min: count,
                max: count,
            },
            count,
            count,
        ),
        'Z' if count == 1 => field(Kind::Offset { colon: false }, 0, 0),
        'Z' if count == 2 => field(Kind::Offset { colon: true }, 0, 0),
        'Z' => field(Kind::ZoneId, 0, 0),
        'z' if count < 4 => field(Kind::ZoneName, 0, 0),
        'z' => {
            return Err(invalid_argument(
                "Long time zone names ('zzzz') are not supported; use 'ZZZ' for the zone id",
            ));
        }
        other => return Err(invalid_argument(format!("Illegal pattern component: {other}"))),
    })
}

/// Joda `DateTimeFormat.forPattern`.
fn joda_tokens(pattern: &str) -> Result<Vec<Token>> {
    if pattern.is_empty() {
        return Err(invalid_argument("Invalid pattern specification"));
    }
    let chars: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::new();
    let mut literal = String::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_ascii_alphabetic() {
            let mut count = 1;
            while chars.get(i + count) == Some(&c) {
                count += 1;
            }
            push_literal(&mut tokens, &mut literal);
            tokens.push(joda_field(c, count)?);
            i += count;
        } else if c == '\'' {
            if chars.get(i + 1) == Some(&'\'') {
                literal.push('\'');
                i += 2;
                continue;
            }
            i += 1;
            loop {
                match chars.get(i) {
                    None => break,
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                        literal.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        literal.push(*ch);
                        i += 1;
                    }
                }
            }
        } else {
            literal.push(c);
            i += 1;
        }
    }
    push_literal(&mut tokens, &mut literal);
    // Adjacent numeric fields parse with fixed widths.
    let numeric = |t: &Token| matches!(t, Token::Field { kind, .. } if kind.is_numeric());
    for i in 0..tokens.len().saturating_sub(1) {
        if numeric(&tokens[i + 1])
            && let Token::Field { kind, min, max } = &mut tokens[i]
            && kind.is_numeric()
        {
            *max = (*min).max(1);
        }
    }
    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Printing
// ---------------------------------------------------------------------------

fn pad(out: &mut String, value: i64, min: usize) {
    if value < 0 {
        out.push('-');
    }
    let digits = value.unsigned_abs().to_string();
    for _ in digits.len()..min {
        out.push('0');
    }
    out.push_str(&digits);
}

fn print(tokens: &[Token], local: NaiveDateTime, zone: Option<(Zone, i32)>) -> Result<String> {
    let mut out = String::new();
    let date = local.date();
    let hour = i64::from(local.hour());
    for token in tokens {
        let Token::Field { kind, min, .. } = token else {
            if let Token::Literal(text) = token {
                out.push_str(text);
            }
            continue;
        };
        let min = *min;
        match *kind {
            Kind::Era => out.push_str(if date.year() > 0 { "AD" } else { "BC" }),
            Kind::Century => pad(&mut out, i64::from(date.year().abs() / 100), min),
            Kind::Year => pad(&mut out, i64::from(date.year()), min),
            Kind::YearOfEra => pad(&mut out, i64::from(date.year_ce().1), min),
            Kind::Weekyear => pad(&mut out, i64::from(date.iso_week().year()), min),
            Kind::TwoDigitYear => pad(&mut out, i64::from(date.year().rem_euclid(100)), 2),
            Kind::TwoDigitWeekyear => pad(&mut out, i64::from(date.iso_week().year().rem_euclid(100)), 2),
            Kind::WeekOfWeekyear => pad(&mut out, i64::from(date.iso_week().week()), min),
            Kind::DayOfWeek => pad(&mut out, i64::from(date.weekday().number_from_monday()), min),
            Kind::DayOfWeekText { full } => {
                let name = DAYS[usize::try_from(date.weekday().num_days_from_monday()).unwrap_or(0) % 7];
                out.push_str(if full { name } else { &name[..3] });
            }
            Kind::DayOfYear => pad(&mut out, i64::from(date.ordinal()), min),
            Kind::Month => pad(&mut out, i64::from(date.month()), min),
            Kind::MonthText { full } => {
                let name = MONTHS[usize::try_from(date.month0()).unwrap_or(0) % 12];
                out.push_str(if full { name } else { &name[..3] });
            }
            Kind::DayOfMonth => pad(&mut out, i64::from(date.day()), min),
            Kind::Halfday => out.push_str(if hour < 12 { "AM" } else { "PM" }),
            Kind::HourOfHalfday => pad(&mut out, hour % 12, min),
            Kind::ClockhourOfHalfday => pad(&mut out, if hour % 12 == 0 { 12 } else { hour % 12 }, min),
            Kind::HourOfDay => pad(&mut out, hour, min),
            Kind::ClockhourOfDay => pad(&mut out, if hour == 0 { 24 } else { hour }, min),
            Kind::Minute => pad(&mut out, i64::from(local.minute()), min),
            Kind::Second => pad(&mut out, i64::from(local.second()), min),
            Kind::Fraction { min: digits, .. } => {
                // Trino passes milliseconds: finer digits are zero.
                let millis = format!("{:03}", local.nanosecond() / 1_000_000 % 1_000);
                let text: String = millis
                    .chars()
                    .chain(std::iter::repeat('0'))
                    .take(digits)
                    .collect();
                out.push_str(&text);
            }
            Kind::Offset { colon } => {
                let (_, offset) = zone.ok_or_else(|| invalid_argument("no time zone"))?;
                let sign = if offset < 0 { '-' } else { '+' };
                let abs = offset.abs();
                let (h, m) = (abs / 3600, abs % 3600 / 60);
                out.push(sign);
                let separator = if colon { ":" } else { "" };
                let _ = write!(out, "{h:02}{separator}{m:02}");
            }
            Kind::ZoneId => {
                let (zone, _) = zone.ok_or_else(|| invalid_argument("no time zone"))?;
                out.push_str(&zone.name());
            }
            Kind::ZoneName => {
                let (zone, offset) = zone.ok_or_else(|| invalid_argument("no time zone"))?;
                let utc = local - Duration::seconds(i64::from(offset));
                let name = zone.short_name(utc).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "NOT_SUPPORTED: the short name of time zone {} is not known; use 'ZZZ' for the zone id",
                        zone.name()
                    ))
                })?;
                out.push_str(&name);
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Parsed {
    year: Option<i64>,
    month: Option<i64>,
    day: Option<i64>,
    day_of_year: Option<i64>,
    hour: Option<i64>,
    pm: Option<bool>,
    clock12: bool,
    minute: Option<i64>,
    second: Option<i64>,
    nanos: Option<i64>,
    offset: Option<i32>,
    zone: Option<Zone>,
}

fn malformed(text: &str, at: &str) -> DataFusionError {
    invalid_argument(format!("Invalid format: \"{text}\" is malformed at \"{at}\""))
}

fn take_number(rest: &str, max: usize, signed: bool) -> Option<(i64, usize)> {
    let bytes = rest.as_bytes();
    let mut len = 0;
    if signed && matches!(bytes.first(), Some(b'-' | b'+')) {
        len = 1;
    }
    let digits_start = len;
    while len < bytes.len() && bytes[len].is_ascii_digit() && len - digits_start < max {
        len += 1;
    }
    if len == digits_start {
        return None;
    }
    rest[..len].parse().ok().map(|v| (v, len))
}

fn take_text(rest: &str, names: &[&str]) -> Option<(usize, usize)> {
    let lower = rest.to_ascii_lowercase();
    names
        .iter()
        .enumerate()
        .filter_map(|(i, name)| {
            let full = name.to_ascii_lowercase();
            if lower.starts_with(&full) {
                Some((i, full.len()))
            } else {
                // Abbreviated form (`Feb`, `Mon`) of names longer than three letters.
                full.get(..3)
                    .filter(|short| short.len() < full.len() && lower.starts_with(short))
                    .map(|_| (i, 3))
            }
        })
        .max_by_key(|(_, len)| *len)
}

fn parse_field(kind: Kind, max: usize, rest: &str, parsed: &mut Parsed) -> Option<usize> {
    let number = |signed| take_number(rest, max, signed);
    let (value, used) = match kind {
        Kind::DayOfWeekText { .. } => return take_text(rest, &DAYS).map(|(_, n)| n),
        Kind::MonthText { .. } => {
            let (i, n) = take_text(rest, &MONTHS)?;
            parsed.month = Some(i64::try_from(i).ok()? + 1);
            return Some(n);
        }
        Kind::Halfday => {
            let (i, n) = take_text(rest, &["AM", "PM"]).filter(|(_, n)| *n == 2)?;
            parsed.pm = Some(i == 1);
            return Some(n);
        }
        Kind::Era => return take_text(rest, &["AD", "BC"]).map(|(_, n)| n),
        Kind::Offset { .. } => {
            let end = rest
                .find(|c: char| !(c.is_ascii_digit() || matches!(c, '+' | '-' | ':')))
                .unwrap_or(rest.len());
            if rest.starts_with('Z') {
                parsed.offset = Some(0);
                return Some(1);
            }
            parsed.offset = Some(match Zone::parse(&rest[..end])? {
                Zone::Fixed(s) => s,
                Zone::Named(_) => return None,
            });
            return Some(end);
        }
        Kind::ZoneId => {
            let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
            parsed.zone = Some(Zone::parse(&rest[..end])?);
            return Some(end);
        }
        Kind::Year | Kind::YearOfEra | Kind::Weekyear => number(true)?,
        _ => number(false)?,
    };
    match kind {
        Kind::Year | Kind::YearOfEra | Kind::Weekyear => parsed.year = Some(value),
        Kind::TwoDigitYear | Kind::TwoDigitWeekyear => {
            // Joda pivot 2020: [1970, 2069].
            let year = 1900 + value;
            parsed.year = Some(if year < 1970 { year + 100 } else { year });
        }
        Kind::Month => parsed.month = Some(value),
        Kind::DayOfMonth => parsed.day = Some(value),
        Kind::DayOfYear => parsed.day_of_year = Some(value),
        Kind::HourOfDay | Kind::ClockhourOfDay => parsed.hour = Some(value % 24),
        Kind::HourOfHalfday | Kind::ClockhourOfHalfday => {
            parsed.hour = Some(value);
            parsed.clock12 = true;
        }
        Kind::Minute => parsed.minute = Some(value),
        Kind::Second => parsed.second = Some(value),
        Kind::Fraction { .. } => {
            let digits = u32::try_from(used.min(9)).ok()?;
            parsed.nanos = Some(value * 10_i64.pow(9 - digits));
        }
        _ => {}
    }
    Some(used)
}

fn range_error(field: &str, value: i64, low: i64, high: i64) -> DataFusionError {
    invalid_argument(format!(
        "Cannot parse: Value {value} for {field} must be in the range [{low},{high}]"
    ))
}

fn build(parsed: &Parsed) -> Result<NaiveDateTime> {
    let year =
        i32::try_from(parsed.year.unwrap_or(1970)).map_err(|_| invalid_argument("year out of range"))?;
    let date = if let Some(doy) = parsed.day_of_year {
        NaiveDate::from_yo_opt(year, u32::try_from(doy).unwrap_or(0))
            .ok_or_else(|| range_error("dayOfYear", doy, 1, 366))?
    } else {
        let month = parsed.month.unwrap_or(1);
        if !(1..=12).contains(&month) {
            return Err(range_error("monthOfYear", month, 1, 12));
        }
        let day = parsed.day.unwrap_or(1);
        let first = NaiveDate::from_ymd_opt(year, u32::try_from(month).unwrap_or(1), 1)
            .ok_or_else(|| invalid_argument("date out of range"))?;
        let last = first
            .checked_add_months(chrono::Months::new(1))
            .and_then(|d| d.pred_opt())
            .map_or(31, |d| i64::from(d.day()));
        if !(1..=last).contains(&day) {
            return Err(range_error("dayOfMonth", day, 1, last));
        }
        first + Duration::days(day - 1)
    };
    let mut hour = parsed.hour.unwrap_or(0);
    if parsed.clock12 {
        if !(0..=12).contains(&hour) {
            return Err(range_error("clockhourOfHalfday", hour, 1, 12));
        }
        hour %= 12;
    }
    if parsed.pm == Some(true) {
        hour += 12;
    }
    let minute = parsed.minute.unwrap_or(0);
    let second = parsed.second.unwrap_or(0);
    let to_u32 = |v: i64| u32::try_from(v).unwrap_or(u32::MAX);
    let time = NaiveTime::from_hms_nano_opt(
        to_u32(hour),
        to_u32(minute),
        to_u32(second),
        to_u32(parsed.nanos.unwrap_or(0)),
    )
    .ok_or_else(|| invalid_argument("time field out of range"))?;
    Ok(date.and_time(time))
}

fn parse_with(tokens: &[Token], text: &str) -> Result<Parsed> {
    let mut parsed = Parsed::default();
    let mut rest = text;
    for token in tokens {
        match token {
            Token::Literal(literal) => {
                rest = rest
                    .strip_prefix(literal.as_str())
                    .ok_or_else(|| malformed(text, rest))?;
            }
            Token::Field { kind, max, .. } => {
                let used =
                    parse_field(*kind, *max, rest, &mut parsed).ok_or_else(|| malformed(text, rest))?;
                rest = &rest[used..];
            }
        }
    }
    if !rest.is_empty() {
        return Err(malformed(text, rest));
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

fn datetime_arg(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Timestamp(..) | DataType::Date32 => Some(t.clone()),
        DataType::Null => Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
        _ => None,
    }
}

/// Local wall clock (truncated to milliseconds) and zone of each row.
/// Local wall clock and, for zoned values, the zone and its offset in seconds.
type WallClock = Option<(NaiveDateTime, Option<(Zone, i32)>)>;

fn wall_clocks(array: &ArrayRef, rows: usize) -> Result<Vec<WallClock>> {
    if let DataType::Date32 = array.data_type() {
        let days = dates(array)?;
        return Ok((0..rows)
            .map(|i| {
                days.at(i)
                    .and_then(date_from_days)
                    .map(|d| (d.and_time(NaiveTime::MIN), None))
            })
            .collect());
    }
    let ts = timestamps(array)?;
    let zone = column_zone(ts.zone.as_ref())?;
    Ok((0..rows)
        .map(|i| {
            let utc = to_naive(ts.at(i)?, ts.unit)?;
            let utc = utc.with_nanosecond(utc.nanosecond() / 1_000_000 * 1_000_000)?;
            Some(match zone {
                None => (utc, None),
                Some(z) => (z.to_local(utc), Some((z, z.offset_at(utc)))),
            })
        })
        .collect())
}

fn format_function(call: &Call<'_>, compile: fn(&str) -> Result<Vec<Token>>, joda: bool) -> Result<ArrayRef> {
    let values = wall_clocks(call.arg(0)?, call.rows)?;
    let formats = strings(call.arg(1)?)?;
    let zoned = matches!(call.arg(0)?.data_type(), DataType::Timestamp(_, Some(_)));
    let mut compiled: Option<(String, Vec<Token>)> = None;
    let out = (0..call.rows)
        .map(|i| {
            let (Some((local, zone)), Some(format)) = (values[i], formats.at(i)) else {
                return Ok(None);
            };
            if compiled.as_ref().is_none_or(|(f, _)| f != format) {
                let tokens = compile(format)?;
                if joda && !zoned && tokens.iter().any(|t| matches!(t, Token::Field { kind, .. } if kind.is_zone())) {
                    return Err(invalid_argument(
                        "format_datetime for TIMESTAMP type, cannot use 'Z' nor 'z' in format, as this type does not contain TZ information",
                    ));
                }
                compiled = Some((format.to_owned(), tokens));
            }
            let tokens = compiled.as_ref().map_or(&[][..], |(_, t)| t.as_slice());
            print(tokens, local, zone).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(string_array(out))
}

pub(crate) fn date_format() -> ScalarUDF {
    Func::new(
        "date_format",
        |name, types| coerce_each(name, types, &[datetime_arg, as_str]),
        UTF8,
        |call| format_function(call, mysql_tokens, false),
    )
    .udf()
}

pub(crate) fn format_datetime() -> ScalarUDF {
    Func::new(
        "format_datetime",
        |name, types| coerce_each(name, types, &[datetime_arg, as_str]),
        UTF8,
        |call| format_function(call, joda_tokens, true),
    )
    .udf()
}

/// Zone a parsed value names, else `session`.
fn row_zone_name(parsed: &Parsed, session: Zone) -> String {
    match (parsed.offset, parsed.zone) {
        (Some(offset), _) => Zone::Fixed(offset).name(),
        (None, Some(zone)) => zone.name(),
        (None, None) => session.name(),
    }
}

/// Result zone of `parse_datetime`: the zone a literal value names, else the session's.
fn parsed_zone(r: &Ret<'_>) -> String {
    let (Some(value), Some(format), Ok(session)) = (r.literal_str(0), r.literal_str(1), parse_zone(r.zone))
    else {
        return r.zone.to_owned();
    };
    joda_tokens(format)
        .and_then(|tokens| parse_with(&tokens, value))
        .map_or_else(|_| r.zone.to_owned(), |parsed| row_zone_name(&parsed, session))
}

fn parse_function(call: &Call<'_>, compile: fn(&str) -> Result<Vec<Token>>) -> Result<ArrayRef> {
    let text = strings(call.arg(0)?)?;
    let formats = strings(call.arg(1)?)?;
    let DataType::Timestamp(unit, zone) = call.return_type else {
        return Err(DataFusionError::Internal("timestamp expected".into()));
    };
    let session = match zone {
        Some(_) => Some(parse_zone(&call.zone)?),
        None => None,
    };
    let out = (0..call.rows)
        .map(|i| {
            let (Some(value), Some(format)) = (text.at(i), formats.at(i)) else {
                return Ok(None);
            };
            let parsed = parse_with(&compile(format)?, value)?;
            let local = build(&parsed)?;
            let utc = match (session, parsed.offset, parsed.zone) {
                (None, _, _) => local,
                (Some(_), Some(offset), _) => local - Duration::seconds(i64::from(offset)),
                (Some(_), None, Some(z)) => z.to_utc(local),
                (Some(s), None, None) => s.to_utc(local),
            };
            // One zone per column: a row in another zone would change its
            // wall clock silently, so it fails instead.
            if let (Some(s), Some(column)) = (session, zone) {
                let row_zone = row_zone_name(&parsed, s);
                if row_zone != column.as_ref() {
                    return Err(DataFusionError::Plan(format!(
                        "NOT_SUPPORTED: '{value}' is in time zone {row_zone}, the column in {column}; \
                         a timestamp with time zone column holds one zone"
                    )));
                }
            }
            from_naive(utc, *unit)
                .ok_or_else(|| invalid_argument("timestamp out of range"))
                .map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    timestamp_array(out, *unit, zone.clone())
}

pub(crate) fn date_parse() -> ScalarUDF {
    Func::new(
        "date_parse",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        |_| Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        |call| parse_function(call, mysql_tokens),
    )
    .udf()
}

pub(crate) fn parse_datetime() -> ScalarUDF {
    Func::new(
        "parse_datetime",
        |name, types| match types {
            [a, b] => Ok(vec![
                as_str(a).ok_or_else(|| not_found(name, types))?,
                as_str(b).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        |r| {
            Ok(DataType::Timestamp(
                TimeUnit::Millisecond,
                Some(Arc::from(parsed_zone(r))),
            ))
        },
        |call| parse_function(call, joda_tokens),
    )
    .zone_sensitive()
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![date_format(), format_datetime(), date_parse(), parse_datetime()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f").unwrap()
    }

    #[test]
    fn mysql_specifiers() {
        let tokens =
            mysql_tokens("%Y-%m-%d %H:%i:%s %f %a %b %c %e %h %I %j %k %l %M %p %r %T %v %W %x %y %% %q")
                .unwrap();
        assert_eq!(
            print(&tokens, at("2024-02-09 14:05:06.789"), None).unwrap(),
            "2024-02-09 14:05:06 789000 Fri Feb 2 9 02 02 040 14 2 February PM 02:05:06 PM 14:05:06 06 Friday 2024 24 % q"
        );
        assert!(mysql_tokens("%D").is_err());
    }

    #[test]
    fn joda_patterns() {
        let tokens =
            joda_tokens("yyyy/MM/dd HH:mm:ss.SSS EEE EEEE MMM MMMM a hh K k D w xxxx e 'quoted''s'").unwrap();
        assert_eq!(
            print(&tokens, at("2024-02-09 14:05:06.789"), None).unwrap(),
            "2024/02/09 14:05:06.789 Fri Friday Feb February PM 02 2 14 40 6 2024 5 quoted's"
        );
        assert!(joda_tokens("q").is_err());
    }

    #[test]
    fn parsing_round_trips() {
        let tokens = mysql_tokens("%Y-%m-%d %H:%i:%s.%f").unwrap();
        let parsed = parse_with(&tokens, "2024-02-29 23:59:58.5").unwrap();
        assert_eq!(build(&parsed).unwrap(), at("2024-02-29 23:59:58.500"));
        let bad = parse_with(&tokens, "2024-02-30 00:00:00.0").unwrap();
        assert!(build(&bad).is_err());
        assert!(parse_with(&tokens, "2024-02-29 23:59:58.5 extra").is_err());
        let compact = joda_tokens("yyyyMMdd").unwrap();
        assert_eq!(
            build(&parse_with(&compact, "20240229").unwrap()).unwrap(),
            at("2024-02-29 00:00:00")
        );
        let twelve = mysql_tokens("%h:%i %p").unwrap();
        assert_eq!(
            build(&parse_with(&twelve, "12:30 AM").unwrap()).unwrap(),
            at("1970-01-01 00:30:00")
        );
        assert_eq!(
            build(&parse_with(&twelve, "12:30 PM").unwrap()).unwrap(),
            at("1970-01-01 12:30:00")
        );
    }
}
