//! Arrow → Trino client protocol: type names, type signatures and JSON values,
//! encoded exactly as Trino 431 sends them (the `trino` Python client and the
//! CLI parse them by `rawType`, JDBC by the signature).
//!
//! A type Trino cannot represent faithfully is refused with `NOT_SUPPORTED`
//! instead of travelling as text: a client must never read a value of another
//! type, or another value, than the one the engine produced.

use std::fmt::Write as _;
use std::str::FromStr as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType, Field,
    Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, Schema, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion::arrow::temporal_conversions::as_datetime_with_timezone;
use serde::Serialize;
use serde_json::{Value, json};

use super::errors::{Code, QueryError};
use super::rewrite::UNNAMED;

type Result<T> = std::result::Result<T, QueryError>;

/// Trino's largest decimal precision.
const MAX_DECIMAL_PRECISION: u32 = 38;
const NANOS_PER_DAY: i128 = 86_400_000_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Column {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) trino_type: String,
    pub(crate) type_signature: Value,
}

/// What the client understands, from `X-Trino-Client-Capabilities`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Encoding {
    /// `PARAMETRIC_DATETIME`: `timestamp(p)` and `time(p)` with their precision.
    /// Without it Trino sends the legacy `timestamp` / `time`, in milliseconds.
    pub(crate) parametric_datetime: bool,
}

impl Encoding {
    /// What every current client (CLI, JDBC, Python) asks for.
    pub(crate) const MODERN: Self = Self {
        parametric_datetime: true,
    };
    /// A client that sends no capabilities (a plain HTTP script).
    #[cfg(test)]
    pub(crate) const LEGACY: Self = Self {
        parametric_datetime: false,
    };

    /// Fraction digits sent for a value of precision `precision`.
    fn digits(self, precision: u32) -> u32 {
        if self.parametric_datetime { precision } else { 3 }
    }
}

fn not_supported(what: impl std::fmt::Display) -> QueryError {
    QueryError::new(
        Code::NotSupported,
        format!("{what} cannot be sent over the Trino protocol"),
    )
}

fn long_arg(value: i64) -> Value {
    json!({"kind": "LONG", "value": value})
}

fn type_arg(signature: &Value) -> Value {
    json!({"kind": "TYPE", "value": signature})
}

fn signature(raw: &str, arguments: &[Value]) -> Value {
    json!({"rawType": raw, "arguments": arguments})
}

fn precision(unit: TimeUnit) -> u32 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

fn simple(name: &str) -> (String, Value) {
    (name.to_owned(), signature(name, &[]))
}

/// Trino decimal for Arrow's `(precision, scale)`. A negative scale (`12300`
/// stored as `123` with scale -2) becomes an integral decimal holding the full
/// value; a scale above the precision widens the precision.
fn decimal_type(precision: u8, scale: i8) -> Result<(String, Value)> {
    let (p, s) = if scale < 0 {
        (u32::from(precision) + u32::from(scale.unsigned_abs()), 0)
    } else {
        let s = u32::from(scale.unsigned_abs());
        (u32::from(precision).max(s), s)
    };
    if p > MAX_DECIMAL_PRECISION {
        return Err(not_supported(format!(
            "decimal({precision}, {scale}) (precision above {MAX_DECIMAL_PRECISION})"
        )));
    }
    Ok((
        format!("decimal({p}, {s})"),
        signature("decimal", &[long_arg(i64::from(p)), long_arg(i64::from(s))]),
    ))
}

/// Row field names in type text: plain when a simple identifier, quoted otherwise.
fn field_name(name: &str) -> String {
    let simple = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if simple {
        name.to_owned()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn map_entries(entries: &Field) -> Result<(&Field, &Field)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => Ok((&fields[0], &fields[1])),
        other => Err(not_supported(format!("Map entries {other}"))),
    }
}

/// Trino type name and signature for an Arrow type.
pub(crate) fn trino_type(data_type: &DataType, encoding: Encoding) -> Result<(String, Value)> {
    Ok(match data_type {
        DataType::Null => simple("unknown"),
        DataType::Boolean => simple("boolean"),
        DataType::Int8 => simple("tinyint"),
        DataType::Int16 | DataType::UInt8 => simple("smallint"),
        DataType::Int32 | DataType::UInt16 => simple("integer"),
        DataType::Int64 | DataType::UInt32 => simple("bigint"),
        // Above i64::MAX a bigint would overflow in every client.
        DataType::UInt64 => decimal_type(20, 0)?,
        DataType::Float16 | DataType::Float32 => simple("real"),
        DataType::Float64 => simple("double"),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => decimal_type(*p, *s)?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => (
            "varchar".into(),
            signature("varchar", &[long_arg(i64::from(i32::MAX))]),
        ),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            simple("varbinary")
        }
        DataType::Date32 | DataType::Date64 => simple("date"),
        DataType::Timestamp(unit, zone) => {
            let raw = if zone.is_some() {
                "timestamp with time zone"
            } else {
                "timestamp"
            };
            if encoding.parametric_datetime {
                let p = precision(*unit);
                let name = match zone {
                    Some(_) => format!("timestamp({p}) with time zone"),
                    None => format!("timestamp({p})"),
                };
                (name, signature(raw, &[long_arg(i64::from(p))]))
            } else {
                simple(raw)
            }
        }
        DataType::Time32(unit) | DataType::Time64(unit) => {
            if encoding.parametric_datetime {
                let p = precision(*unit);
                (format!("time({p})"), signature("time", &[long_arg(i64::from(p))]))
            } else {
                simple("time")
            }
        }
        DataType::Interval(IntervalUnit::YearMonth) => simple("interval year to month"),
        DataType::Interval(IntervalUnit::DayTime | IntervalUnit::MonthDayNano) | DataType::Duration(_) => {
            simple("interval day to second")
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let (inner, inner_signature) = trino_type(field.data_type(), encoding)?;
            (
                format!("array({inner})"),
                signature("array", &[type_arg(&inner_signature)]),
            )
        }
        DataType::Map(entries, _) => {
            let (key, value) = map_entries(entries)?;
            let (key_name, key_signature) = trino_type(key.data_type(), encoding)?;
            let (value_name, value_signature) = trino_type(value.data_type(), encoding)?;
            (
                format!("map({key_name}, {value_name})"),
                signature("map", &[type_arg(&key_signature), type_arg(&value_signature)]),
            )
        }
        DataType::Struct(fields) => {
            let mut names = Vec::with_capacity(fields.len());
            let mut arguments = Vec::with_capacity(fields.len());
            for field in fields {
                let (name, field_signature) = trino_type(field.data_type(), encoding)?;
                names.push(format!("{} {name}", field_name(field.name())));
                arguments.push(json!({
                    "kind": "NAMED_TYPE",
                    "value": {"fieldName": {"name": field.name()}, "typeSignature": field_signature},
                }));
            }
            (format!("row({})", names.join(", ")), signature("row", &arguments))
        }
        DataType::Dictionary(_, values) => trino_type(values, encoding)?,
        other => return Err(not_supported(format!("Type {other}"))),
    })
}

/// Trino type name and signature of a field: its Arrow type's, or the
/// narrower type it declares (`varchar(5)`, `char(3)`, `timestamp(1)`, see
/// [`codelake_core::trino_types::Declared`]).
pub(crate) fn field_type(field: &Field, encoding: Encoding) -> Result<(String, Value)> {
    use codelake_core::trino_types::{Declared, declared};
    Ok(match declared(field) {
        Some(Declared::Varchar(n)) => (
            format!("varchar({n})"),
            signature("varchar", &[long_arg(i64::from(n))]),
        ),
        Some(Declared::Char(n)) => (format!("char({n})"), signature("char", &[long_arg(i64::from(n))])),
        Some(Declared::Timestamp(p)) if encoding.parametric_datetime => {
            let zoned = matches!(field.data_type(), DataType::Timestamp(_, Some(_)));
            let (raw, name) = if zoned {
                (
                    "timestamp with time zone",
                    format!("timestamp({p}) with time zone"),
                )
            } else {
                ("timestamp", format!("timestamp({p})"))
            };
            (name, signature(raw, &[long_arg(i64::from(p))]))
        }
        _ => trino_type(field.data_type(), encoding)?,
    })
}

/// Output columns; the names given to unnamed expressions (see
/// [`super::rewrite`]) become Trino's `_col<position>`.
pub(crate) fn columns(schema: &Schema, encoding: Encoding) -> Result<Vec<Column>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            let (trino_type, type_signature) = field_type(field, encoding)?;
            let name = if field.name().starts_with(UNNAMED) {
                format!("_col{position}")
            } else {
                field.name().clone()
            };
            Ok(Column {
                name,
                trino_type,
                type_signature,
            })
        })
        .collect()
}

fn float(value: f64) -> Value {
    if value.is_nan() {
        json!("NaN")
    } else if value.is_infinite() {
        json!(if value > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        json!(value)
    }
}

/// A `real` as Java's `Float` prints it: the shortest decimal that reads back
/// as the same `f32` (`0.1`, not the widened `0.10000000149011612`).
fn real(value: f32) -> Value {
    if value.is_finite() {
        let shortest: f64 = value.to_string().parse().unwrap_or(f64::from(value));
        json!(shortest)
    } else {
        float(f64::from(value))
    }
}

/// `YYYY-MM-DD HH:MM:SS[.fraction]` with the fraction truncated to `digits`.
pub(crate) fn timestamp_text(nanos_since_epoch: i128, digits: u32) -> String {
    let days = i64::try_from(nanos_since_epoch.div_euclid(NANOS_PER_DAY)).unwrap_or_default();
    let of_day = nanos_since_epoch.rem_euclid(NANOS_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02} {}", time_text(of_day, digits))
}

/// `HH:MM:SS[.fraction]` of a time of day in nanoseconds.
fn time_text(nanos_of_day: i128, digits: u32) -> String {
    let secs = nanos_of_day / 1_000_000_000;
    let fraction = nanos_of_day % 1_000_000_000;
    let mut text = format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60);
    if digits > 0 {
        let scaled = fraction / 10_i128.pow(9 - digits);
        let _ = write!(text, ".{scaled:0width$}", width = digits as usize);
    }
    text
}

/// Rounds half up to `digits` fraction digits, like Trino's datetime casts.
fn round_nanos(nanos: i128, digits: u32) -> i128 {
    if digits >= 9 {
        return nanos;
    }
    let factor = 10_i128.pow(9 - digits);
    (nanos + factor / 2).div_euclid(factor) * factor
}

fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Days since 1970-01-01 → (year, month, day).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// Decimal text of an unscaled integer (`-12345`, scale 2 → `-123.45`).
fn decimal_text(unscaled: &str, scale: i8) -> String {
    let (sign, digits) = unscaled
        .strip_prefix('-')
        .map_or(("", unscaled), |digits| ("-", digits));
    if scale <= 0 {
        if digits.bytes().all(|b| b == b'0') {
            return "0".into();
        }
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{sign}{digits}{zeros}");
    }
    let scale = usize::from(scale.unsigned_abs());
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

/// Trino's zone label: `UTC` for UTC itself, the zone id or offset otherwise.
fn zone_label(zone: &str) -> String {
    match zone {
        "UTC" | "Z" | "+00:00" | "-00:00" | "+0000" | "+00" | "Etc/UTC" => "UTC".into(),
        other => other.to_owned(),
    }
}

/// Offset of `zone` from UTC at the instant `nanos`, in nanoseconds.
fn zone_offset(zone: &str, nanos: i128) -> Result<i128> {
    let tz = Tz::from_str(zone).map_err(|e| not_supported(format!("Time zone {zone} ({e})")))?;
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000))
        .map_err(|_| not_supported("A timestamp out of range"))?;
    let local = as_datetime_with_timezone::<TimestampSecondType>(seconds, tz)
        .ok_or_else(|| not_supported("A timestamp out of range"))?;
    let offset = local.naive_local() - local.naive_utc();
    Ok(i128::from(offset.num_seconds()) * 1_000_000_000)
}

fn day_to_second_text(millis: i64) -> String {
    let sign = if millis < 0 { "-" } else { "" };
    let millis = millis.unsigned_abs();
    let day = millis / 86_400_000;
    let rest = millis % 86_400_000;
    format!(
        "{sign}{day} {:02}:{:02}:{:02}.{:03}",
        rest / 3_600_000,
        (rest % 3_600_000) / 60_000,
        (rest % 60_000) / 1000,
        rest % 1000
    )
}

fn year_to_month_text(months: i32) -> String {
    let sign = if months < 0 { "-" } else { "" };
    let months = months.unsigned_abs();
    format!("{sign}{}-{}", months / 12, months % 12)
}

/// A day-to-second interval of `nanos`; Trino's has milliseconds, so a finer
/// value is refused rather than truncated.
fn interval_millis(nanos: i128) -> Result<Value> {
    if nanos % 1_000_000 != 0 {
        return Err(not_supported(
            "An interval with sub-millisecond precision (Trino's interval day to second has milliseconds)",
        ));
    }
    let millis = i64::try_from(nanos / 1_000_000).map_err(|_| not_supported("An interval out of range"))?;
    Ok(json!(day_to_second_text(millis)))
}

fn timestamp_value(
    array: &ArrayRef,
    row: usize,
    unit: TimeUnit,
    zone: Option<&str>,
    encoding: Encoding,
) -> Result<Value> {
    timestamp_value_digits(array, row, unit, zone, encoding.digits(precision(unit)))
}

/// A timestamp value shown with `digits` fraction digits.
fn timestamp_value_digits(
    array: &ArrayRef,
    row: usize,
    unit: TimeUnit,
    zone: Option<&str>,
    digits: u32,
) -> Result<Value> {
    let (raw, factor) = match unit {
        TimeUnit::Second => (
            array.as_primitive::<TimestampSecondType>().value(row),
            1_000_000_000,
        ),
        TimeUnit::Millisecond => (
            array.as_primitive::<TimestampMillisecondType>().value(row),
            1_000_000,
        ),
        TimeUnit::Microsecond => (array.as_primitive::<TimestampMicrosecondType>().value(row), 1_000),
        TimeUnit::Nanosecond => (array.as_primitive::<TimestampNanosecondType>().value(row), 1),
    };
    let instant = round_nanos(i128::from(raw) * factor, digits);
    Ok(json!(match zone {
        // Stored as UTC instants; shown as the local time in their zone, like Trino.
        Some(zone) => {
            let local = instant + zone_offset(zone, instant)?;
            format!("{} {}", timestamp_text(local, digits), zone_label(zone))
        }
        None => timestamp_text(instant, digits),
    }))
}

fn time_value(nanos_of_day: i128, unit: TimeUnit, encoding: Encoding) -> Value {
    let digits = encoding.digits(precision(unit));
    let rounded = round_nanos(nanos_of_day, digits).rem_euclid(NANOS_PER_DAY);
    json!(time_text(rounded, digits))
}

/// Map keys are JSON object keys: the key's text.
fn key_text(key: Value) -> String {
    match key {
        Value::String(text) => text,
        other => other.to_string(),
    }
}

fn values(list: &ArrayRef, encoding: Encoding) -> Result<Value> {
    Ok(Value::Array(
        (0..list.len())
            .map(|i| value(list, i, encoding))
            .collect::<Result<_>>()?,
    ))
}

/// [`value`] of a column declaring a narrower Trino type: a `timestamp(p)`
/// shows `p` fraction digits, a `char(n)` its padding.
pub(crate) fn declared_value(
    array: &ArrayRef,
    row: usize,
    encoding: Encoding,
    declared: Option<codelake_core::trino_types::Declared>,
) -> Result<Value> {
    use codelake_core::trino_types::Declared;
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    match (declared, array.data_type()) {
        (Some(Declared::Timestamp(p)), DataType::Timestamp(unit, zone)) => {
            let digits = if encoding.parametric_datetime {
                u32::from(p)
            } else {
                3
            };
            timestamp_value_digits(array, row, *unit, zone.as_deref(), digits)
        }
        (Some(Declared::Char(n)), _) => match value(array, row, encoding)? {
            Value::String(text) => {
                let width = usize::try_from(n).unwrap_or(usize::MAX);
                let padding = width.saturating_sub(text.chars().count());
                Ok(json!(format!("{text}{}", " ".repeat(padding))))
            }
            other => Ok(other),
        },
        _ => value(array, row, encoding),
    }
}

/// JSON value of row `row` of `array`, as Trino encodes it.
#[allow(
    clippy::too_many_lines,
    reason = "one arm per Arrow type keeps the encoding readable"
)]
pub(crate) fn value(array: &ArrayRef, row: usize, encoding: Encoding) -> Result<Value> {
    // A `NullArray` has no validity bitmap: every row is null.
    if array.is_null(row) || matches!(array.data_type(), DataType::Null) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        DataType::Boolean => json!(array.as_boolean().value(row)),
        DataType::Int8 => json!(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => json!(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => json!(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => json!(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => json!(array.as_primitive::<UInt8Type>().value(row)),
        DataType::UInt16 => json!(array.as_primitive::<UInt16Type>().value(row)),
        DataType::UInt32 => json!(array.as_primitive::<UInt32Type>().value(row)),
        // Announced as decimal(20, 0): decimals travel as strings.
        DataType::UInt64 => json!(array.as_primitive::<UInt64Type>().value(row).to_string()),
        DataType::Float16 => real(array.as_primitive::<Float16Type>().value(row).to_f32()),
        DataType::Float32 => real(array.as_primitive::<Float32Type>().value(row)),
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal32(_, s) => json!(decimal_text(
            &array.as_primitive::<Decimal32Type>().value(row).to_string(),
            *s
        )),
        DataType::Decimal64(_, s) => json!(decimal_text(
            &array.as_primitive::<Decimal64Type>().value(row).to_string(),
            *s
        )),
        DataType::Decimal128(_, s) => json!(decimal_text(
            &array.as_primitive::<Decimal128Type>().value(row).to_string(),
            *s
        )),
        DataType::Decimal256(_, s) => json!(decimal_text(
            &array.as_primitive::<Decimal256Type>().value(row).to_string(),
            *s
        )),
        DataType::Utf8 => json!(array.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(array.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(array.as_string_view().value(row)),
        DataType::Binary => json!(BASE64.encode(array.as_binary::<i32>().value(row))),
        DataType::LargeBinary => json!(BASE64.encode(array.as_binary::<i64>().value(row))),
        DataType::BinaryView => json!(BASE64.encode(array.as_binary_view().value(row))),
        DataType::FixedSizeBinary(_) => json!(BASE64.encode(array.as_fixed_size_binary().value(row))),
        DataType::Date32 => json!(date_text(i64::from(
            array.as_primitive::<Date32Type>().value(row)
        ))),
        DataType::Date64 => {
            let millis = array.as_primitive::<Date64Type>().value(row);
            if millis.rem_euclid(MILLIS_PER_DAY) != 0 {
                return Err(not_supported("A Date64 value with a time of day"));
            }
            json!(date_text(millis.div_euclid(MILLIS_PER_DAY)))
        }
        DataType::Timestamp(unit, zone) => timestamp_value(array, row, *unit, zone.as_deref(), encoding)?,
        DataType::Time32(unit) => {
            let (raw, factor) = match unit {
                TimeUnit::Second => (array.as_primitive::<Time32SecondType>().value(row), 1_000_000_000),
                _ => (
                    array.as_primitive::<Time32MillisecondType>().value(row),
                    1_000_000,
                ),
            };
            time_value(i128::from(raw) * factor, *unit, encoding)
        }
        DataType::Time64(unit) => {
            let nanos = match unit {
                TimeUnit::Nanosecond => i128::from(array.as_primitive::<Time64NanosecondType>().value(row)),
                _ => i128::from(array.as_primitive::<Time64MicrosecondType>().value(row)) * 1_000,
            };
            time_value(nanos, *unit, encoding)
        }
        DataType::Interval(IntervalUnit::YearMonth) => json!(year_to_month_text(
            array.as_primitive::<IntervalYearMonthType>().value(row)
        )),
        DataType::Interval(IntervalUnit::DayTime) => {
            let v = array.as_primitive::<IntervalDayTimeType>().value(row);
            interval_millis((i128::from(v.days) * 86_400_000 + i128::from(v.milliseconds)) * 1_000_000)?
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let v = array.as_primitive::<IntervalMonthDayNanoType>().value(row);
            if v.months != 0 {
                return Err(not_supported(
                    "An interval with months (announced as interval day to second)",
                ));
            }
            interval_millis(i128::from(v.days) * NANOS_PER_DAY + i128::from(v.nanoseconds))?
        }
        DataType::Duration(unit) => {
            let nanos = match unit {
                TimeUnit::Second => {
                    i128::from(array.as_primitive::<DurationSecondType>().value(row)) * 1_000_000_000
                }
                TimeUnit::Millisecond => {
                    i128::from(array.as_primitive::<DurationMillisecondType>().value(row)) * 1_000_000
                }
                TimeUnit::Microsecond => {
                    i128::from(array.as_primitive::<DurationMicrosecondType>().value(row)) * 1_000
                }
                TimeUnit::Nanosecond => i128::from(array.as_primitive::<DurationNanosecondType>().value(row)),
            };
            interval_millis(nanos)?
        }
        DataType::List(_) => values(&array.as_list::<i32>().value(row), encoding)?,
        DataType::LargeList(_) => values(&array.as_list::<i64>().value(row), encoding)?,
        DataType::FixedSizeList(..) => values(&array.as_fixed_size_list().value(row), encoding)?,
        DataType::Map(..) => {
            let entries = array.as_map().value(row);
            let (keys, items) = (entries.column(0), entries.column(1));
            let mut object = serde_json::Map::with_capacity(entries.len());
            for i in 0..entries.len() {
                object.insert(key_text(value(keys, i, encoding)?), value(items, i, encoding)?);
            }
            Value::Object(object)
        }
        DataType::Struct(_) => Value::Array(
            array
                .as_struct()
                .columns()
                .iter()
                .map(|column| value(column, row, encoding))
                .collect::<Result<_>>()?,
        ),
        DataType::Dictionary(..) => {
            let dictionary = array.as_any_dictionary();
            let key = datafusion::arrow::compute::cast(&dictionary.keys().slice(row, 1), &DataType::Int64)
                .map_err(|e| QueryError::from(&datafusion::error::DataFusionError::from(e)))?;
            let index = usize::try_from(key.as_primitive::<Int64Type>().value(0))
                .map_err(|_| not_supported("A dictionary key out of range"))?;
            value(dictionary.values(), index, encoding)?
        }
        other => return Err(not_supported(format!("Type {other}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
        Int32Array, Int32Builder, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
        IntervalYearMonthArray, MapBuilder, NullArray, StringArray, StringBuilder, StructArray,
        Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{IntervalDayTime, IntervalMonthDayNano, UnionFields, UnionMode};
    use std::sync::Arc;

    const M: Encoding = Encoding::MODERN;
    const L: Encoding = Encoding::LEGACY;

    fn arr(array: impl Array + 'static) -> ArrayRef {
        Arc::new(array)
    }

    fn name(data_type: &DataType, encoding: Encoding) -> String {
        trino_type(data_type, encoding).unwrap().0
    }

    fn one(array: impl Array + 'static, encoding: Encoding) -> Value {
        value(&arr(array), 0, encoding).unwrap()
    }

    #[test]
    fn type_names_match_trino() {
        assert_eq!(name(&DataType::Int64, M), "bigint");
        assert_eq!(name(&DataType::Int32, M), "integer");
        assert_eq!(name(&DataType::Utf8, M), "varchar");
        assert_eq!(name(&DataType::Decimal128(18, 2), M), "decimal(18, 2)");
        assert_eq!(
            trino_type(&DataType::Decimal128(18, 2), M).unwrap().1,
            json!({"rawType": "decimal", "arguments": [{"kind": "LONG", "value": 18}, {"kind": "LONG", "value": 2}]})
        );
        assert_eq!(name(&DataType::Decimal128(5, -2), M), "decimal(7, 0)");
        assert_eq!(name(&DataType::UInt64, M), "decimal(20, 0)");
        assert_eq!(
            name(&DataType::Timestamp(TimeUnit::Millisecond, None), M),
            "timestamp(3)"
        );
        assert_eq!(
            name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), M),
            "timestamp(6) with time zone"
        );
        assert_eq!(name(&DataType::Binary, M), "varbinary");
        assert_eq!(name(&DataType::Date32, M), "date");
        assert_eq!(name(&DataType::Time32(TimeUnit::Millisecond), M), "time(3)");
        assert_eq!(
            name(&DataType::Interval(IntervalUnit::MonthDayNano), M),
            "interval day to second"
        );
        assert_eq!(
            name(&DataType::Interval(IntervalUnit::YearMonth), M),
            "interval year to month"
        );
        assert!(trino_type(&DataType::Decimal256(76, 0), M).is_err());
        let union = DataType::Union(UnionFields::empty(), UnionMode::Sparse);
        assert_eq!(trino_type(&union, M).unwrap_err().name(), "NOT_SUPPORTED");
    }

    #[test]
    fn legacy_clients_get_datetime_types_without_precision() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            trino_type(&ts, L).unwrap(),
            (
                "timestamp".into(),
                json!({"rawType": "timestamp", "arguments": []})
            )
        );
        assert_eq!(
            name(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), L),
            "timestamp with time zone"
        );
        assert_eq!(name(&DataType::Time64(TimeUnit::Microsecond), L), "time");
        // 1960-01-02 03:04:05.123456
        let micros = TimestampMicrosecondArray::from(vec![-315_521_754_876_544]);
        assert_eq!(one(micros.clone(), L), json!("1960-01-02 03:04:05.123"));
        assert_eq!(one(micros, M), json!("1960-01-02 03:04:05.123456"));
        let rounds_up = TimestampMicrosecondArray::from(vec![999_500]);
        assert_eq!(one(rounds_up, L), json!("1970-01-01 00:00:01.000"));
    }

    #[test]
    fn nested_types_are_typed() {
        let fields = [
            Arc::new(Field::new("x", DataType::Int32, true)),
            Arc::new(Field::new("Y", DataType::Utf8, true)),
        ];
        let row = StructArray::from(vec![
            (Arc::clone(&fields[0]), arr(Int32Array::from(vec![1]))),
            (Arc::clone(&fields[1]), arr(StringArray::from(vec!["x"]))),
        ]);
        let (text, sig) = trino_type(row.data_type(), M).unwrap();
        assert_eq!(text, "row(x integer, \"Y\" varchar)");
        assert_eq!(sig["rawType"], "row");
        assert_eq!(sig["arguments"][1]["value"]["fieldName"]["name"], "Y");
        assert_eq!(one(row, M), json!([1, "x"]));

        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.keys().append_value("b");
        builder.values().append_null();
        builder.append(true).unwrap();
        let map = builder.finish();
        let (text, sig) = trino_type(map.data_type(), M).unwrap();
        assert_eq!(text, "map(varchar, integer)");
        assert_eq!(
            sig["arguments"][1]["value"],
            json!({"rawType": "integer", "arguments": []})
        );
        assert_eq!(one(map, M), json!({"a": 1, "b": null}));
    }

    #[test]
    fn values_are_encoded_like_trino() {
        assert_eq!(one(Int64Array::from(vec![42]), M), json!(42));
        assert_eq!(one(BooleanArray::from(vec![true]), M), json!(true));
        assert_eq!(
            value(&arr(StringArray::from(vec![Some("ação"), None])), 1, M).unwrap(),
            Value::Null
        );
        assert_eq!(one(NullArray::new(1), M), Value::Null);
        let dec = Decimal128Array::from(vec![1050, -5, 0])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(value(&arr(dec.clone()), 0, M).unwrap(), json!("10.50"));
        assert_eq!(value(&arr(dec.clone()), 1, M).unwrap(), json!("-0.05"));
        assert_eq!(value(&arr(dec), 2, M).unwrap(), json!("0.00"));
        let negative_scale = Decimal128Array::from(vec![123])
            .with_precision_and_scale(5, -2)
            .unwrap();
        assert_eq!(one(negative_scale, M), json!("12300"));
        assert_eq!(
            one(UInt64Array::from(vec![u64::MAX]), M),
            json!("18446744073709551615")
        );
        assert_eq!(one(Float64Array::from(vec![f64::NAN]), M), json!("NaN"));
        assert_eq!(one(Float32Array::from(vec![0.1_f32]), M).to_string(), "0.1");
        assert_eq!(
            one(Float32Array::from(vec![1.0_f32 / 3.0]), M).to_string(),
            "0.33333334"
        );
        assert_eq!(one(Float32Array::from(vec![f32::INFINITY]), M), json!("Infinity"));
        assert_eq!(one(BinaryArray::from(vec![&[0_u8, 42][..]]), M), json!("ACo="));
        assert_eq!(one(Date32Array::from(vec![20_454]), M), json!("2026-01-01"));
        assert_eq!(
            one(Date64Array::from(vec![20_454 * MILLIS_PER_DAY]), M),
            json!("2026-01-01")
        );
        assert!(value(&arr(Date64Array::from(vec![1])), 0, M).is_err());
        let ts = TimestampMillisecondArray::from(vec![1_767_261_600_123]);
        assert_eq!(one(ts, M), json!("2026-01-01 10:00:00.123"));
        let before_epoch = TimestampMicrosecondArray::from(vec![-1]);
        assert_eq!(one(before_epoch, M), json!("1969-12-31 23:59:59.999999"));
        let utc = TimestampMillisecondArray::from(vec![0]).with_timezone("UTC");
        assert_eq!(one(utc, M), json!("1970-01-01 00:00:00.000 UTC"));
        let sao_paulo = TimestampMillisecondArray::from(vec![0]).with_timezone("America/Sao_Paulo");
        assert_eq!(
            one(sao_paulo, M),
            json!("1969-12-31 21:00:00.000 America/Sao_Paulo")
        );
        let offset = TimestampMillisecondArray::from(vec![0]).with_timezone("+05:30");
        assert_eq!(one(offset, M), json!("1970-01-01 05:30:00.000 +05:30"));
        assert_eq!(
            one(Time32MillisecondArray::from(vec![36_000_120]), M),
            json!("10:00:00.120")
        );
    }

    #[test]
    fn intervals_are_trino_intervals() {
        let day = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(0, 1, 1_500_000_000)]);
        assert_eq!(one(day, M), json!("1 00:00:01.500"));
        let negative = IntervalDayTimeArray::from(vec![IntervalDayTime::new(-2, -3_000)]);
        assert_eq!(one(negative, M), json!("-2 00:00:03.000"));
        assert_eq!(one(IntervalYearMonthArray::from(vec![14]), M), json!("1-2"));
        assert_eq!(one(IntervalYearMonthArray::from(vec![-14]), M), json!("-1-2"));
        let months = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(1, 0, 0)]);
        assert!(value(&arr(months), 0, M).is_err());
        let sub_milli = IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNano::new(0, 0, 1)]);
        assert!(value(&arr(sub_milli), 0, M).is_err());
    }

    #[test]
    fn decimal_text_edges() {
        assert_eq!(decimal_text("5", 3), "0.005");
        assert_eq!(decimal_text("-12345", 2), "-123.45");
        assert_eq!(decimal_text("7", 0), "7");
        assert_eq!(decimal_text("0", -3), "0");
        assert_eq!(decimal_text("-12", -1), "-120");
    }

    #[test]
    fn unnamed_columns_are_numbered_by_position() {
        let schema = Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new(format!("{UNNAMED}1"), DataType::Utf8, false),
        ]);
        let names: Vec<String> = columns(&schema, M).unwrap().into_iter().map(|c| c.name).collect();
        assert_eq!(names, ["value", "_col1"]);
    }
}
