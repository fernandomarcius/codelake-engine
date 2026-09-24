//! `CAST` with Trino semantics where Arrow's cast differs.
//!
//! [`TrinoCasts`] runs after type coercion, so it also sees the implicit
//! casts `DataFusion` inserts. It replaces only the conversions whose result
//! would otherwise differ from Trino 431:
//!
//! | from → to | Arrow | Trino |
//! |---|---|---|
//! | `timestamp with time zone` → `date` | UTC date | local date |
//! | `double`/`real` → `varchar` | `1.5` | `1.5E0` |
//! | `double`/`decimal` → integer | truncates | rounds half away from zero; NaN or out of range is `INVALID_CAST_ARGUMENT` |
//! | `varchar` → integer, `double`, `real`, `boolean`, `date` | Arrow's text rules | Trino's |
//!
//! The engine's own casts (`crate::timestamp_casts`, `crate::timestamps`,
//! `crate::casts`) run first
//! and are not repeated here: timestamps between zones and precisions, dates
//! to `timestamp with time zone`, timestamps to text, text to timestamps and
//! decimals, and floating point to decimals.
//!
//! Everything else keeps `DataFusion`'s cast.

use std::fmt::Write as _;
use std::sync::{Arc, LazyLock};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::array::{Array, ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, ScalarUDF};
use datafusion::optimizer::analyzer::AnalyzerRule;
use num_traits::ToPrimitive;
use regex::Regex;

use super::datetime::{days_from_date, from_naive, local_wall_clock, to_naive};
use super::support::{
    At, Call, Func, invalid_cast, out_of_range, per_second, precision_of, string_array, strings, timestamps,
};
use super::zone::Zone;

pub(crate) const CAST: &str = "codelake_trino_cast";
pub(crate) const TRY_CAST: &str = "codelake_trino_try_cast";

pub(crate) fn i64_to_f64(value: i64) -> f64 {
    value.to_f64().unwrap_or(f64::NAN)
}

// ---------------------------------------------------------------------------
// Number and timestamp text
// ---------------------------------------------------------------------------

/// Scientific notation as Java's `DecimalFormat("0.0…E0")` prints it.
fn scientific(value: f64, max_digits: usize) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0E0".into()
        } else {
            "0E0".into()
        };
    }
    // JDK 17 prints the smallest subnormal with two digits.
    if value.abs().to_bits() == 1 {
        return if value < 0.0 {
            "-4.9E-324".into()
        } else {
            "4.9E-324".into()
        };
    }
    let text = format!("{:e}", value.abs());
    let (mantissa, exponent) = text.split_once('e').unwrap_or((text.as_str(), "0"));
    let mut exponent: i32 = exponent.parse().unwrap_or(0);
    let mut digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    if digits.len() > max_digits {
        let keep = digits.split_off(max_digits);
        let first = keep.first().copied().unwrap_or(b'0');
        let rest_zero = keep.iter().skip(1).all(|d| *d == b'0');
        let last_odd = digits.last().is_some_and(|d| (d - b'0') % 2 == 1);
        // HALF_EVEN, DecimalFormat's default.
        if first > b'5' || (first == b'5' && (!rest_zero || last_odd)) {
            let mut i = digits.len();
            loop {
                if i == 0 {
                    digits.insert(0, b'1');
                    exponent += 1;
                    digits.pop();
                    break;
                }
                i -= 1;
                if digits[i] == b'9' {
                    digits[i] = b'0';
                } else {
                    digits[i] += 1;
                    break;
                }
            }
        }
    }
    while digits.len() > 1 && digits.last() == Some(&b'0') {
        digits.pop();
    }
    let sign = if value < 0.0 { "-" } else { "" };
    let head = char::from(digits.first().copied().unwrap_or(b'0'));
    let tail: String = digits.iter().skip(1).map(|d| char::from(*d)).collect();
    let tail = if tail.is_empty() { "0".to_owned() } else { tail };
    format!("{sign}{head}.{tail}E{exponent}")
}

/// `CAST(double AS varchar)`.
pub(crate) fn format_double(value: f64) -> String {
    scientific(value, 17)
}

/// `CAST(real AS varchar)`: seven significant digits of the widened value.
pub(crate) fn format_real(value: f32) -> String {
    scientific(f64::from(value), 7)
}

/// `YYYY-MM-DD HH:MM:SS[.fff…]` with `precision` fractional digits.
pub(crate) fn render_timestamp(dt: NaiveDateTime, precision: u32) -> String {
    let mut text = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if precision > 0 {
        let scale = 10_u32.pow(9 - precision.min(9));
        let fraction = dt.nanosecond() % 1_000_000_000 / scale;
        let width = usize::try_from(precision.min(9)).unwrap_or(9);
        let _ = write!(text, ".{fraction:0width$}");
    }
    text
}

static TIMESTAMP_TEXT: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(
        r"^\s*([+-]?\d{4,})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,12}))?)?)?\s*([+-]\d{1,2}(?::?\d{2})?|[A-Za-z][A-Za-z0-9_/+\-:]*)?\s*$",
    )
    .ok()
});

/// Parsed Trino timestamp text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimestampText {
    pub(crate) local: NaiveDateTime,
    pub(crate) fraction_digits: u32,
    pub(crate) zone: Option<String>,
}

/// Trino's timestamp literal syntax, with an optional zone.
pub(crate) fn parse_timestamp_text(text: &str) -> Option<TimestampText> {
    let caps = TIMESTAMP_TEXT.as_ref()?.captures(text)?;
    let number = |i: usize| -> Option<u32> { caps.get(i).map_or(Some(0), |m| m.as_str().parse().ok()) };
    let year: i32 = caps.get(1)?.as_str().parse().ok()?;
    let date = NaiveDate::from_ymd_opt(year, number(2)?, number(3)?)?;
    let fraction = caps.get(7).map_or("", |m| m.as_str());
    let fraction_digits = u32::try_from(fraction.len()).ok()?;
    let nanos = if fraction.is_empty() {
        0
    } else {
        let padded: String = fraction.chars().chain(std::iter::repeat('0')).take(9).collect();
        padded.parse().ok()?
    };
    let time = NaiveTime::from_hms_nano_opt(number(4)?, number(5)?, number(6)?, nanos)?;
    Some(TimestampText {
        local: date.and_time(time),
        fraction_digits,
        zone: caps.get(8).map(|m| m.as_str().to_owned()),
    })
}

// ---------------------------------------------------------------------------
// Which casts change
// ---------------------------------------------------------------------------

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

fn is_int(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

pub(crate) fn needs_trino_cast(from: &DataType, to: &DataType) -> bool {
    use DataType::{Date32, Decimal128, Float32, Float64, Timestamp};
    match (from, to) {
        (Timestamp(_, Some(_)), Date32) => true,
        (Float32 | Float64, t) => is_text(t) || is_int(t),
        (Decimal128(..), t) => is_int(t),
        (f, t) if is_text(f) => is_int(t) || matches!(t, Float32 | Float64 | DataType::Boolean | Date32),
        _ => false,
    }
}

/// Analyzer rule replacing the casts listed in the module docs.
#[derive(Debug, Default)]
pub(crate) struct TrinoCasts;

impl AnalyzerRule for TrinoCasts {
    fn name(&self) -> &'static str {
        "trino_casts"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_node).data()
    }
}

fn rewrite_node(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = merge_schema(&node.inputs());
    if let LogicalPlan::TableScan(scan) = &node {
        schema.merge(&DFSchema::try_from_qualified_schema(
            scan.table_name.clone(),
            &scan.source.schema(),
        )?);
    }
    // Keep each output column's name, and recompute the node's schema: the
    // replacement's type is the same, its name and nullability are not.
    let names = NamePreserver::new(&node);
    let rewritten = node.map_expressions(|expr| {
        let saved = names.save(&expr);
        Ok(expr
            .transform_up(|e| rewrite_expr(e, &schema))?
            .update_data(|e| saved.restore(e)))
    })?;
    // Children rewritten below may have changed nullability too.
    if rewritten.transformed || !rewritten.data.inputs().is_empty() {
        return rewritten.map_data(LogicalPlan::recompute_schema);
    }
    Ok(rewritten)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let (inner, target, is_try) = match &expr {
        Expr::Cast(c) => (c.expr.as_ref(), &c.data_type, false),
        Expr::TryCast(c) => (c.expr.as_ref(), &c.data_type, true),
        _ => return Ok(Transformed::no(expr)),
    };
    if let Ok((_, field)) = inner.to_field(schema)
        && super::support::is_json_field(&field)
    {
        // `CAST(json AS T)` reads the JSON value (see `json_cast`).
        let name = if is_try {
            super::json_cast::TRY_FROM_JSON
        } else {
            super::json_cast::FROM_JSON
        };
        let function = super::json_cast::udfs()
            .into_iter()
            .find(|f| f.name() == name)
            .ok_or_else(|| DataFusionError::Internal("json cast function".into()))?;
        let target = Expr::Literal(ScalarValue::try_from(target)?, None);
        return Ok(Transformed::yes(Expr::ScalarFunction(
            datafusion::logical_expr::expr::ScalarFunction::new_udf(
                Arc::new(function),
                vec![inner.clone(), target],
            ),
        )));
    }
    let Ok(source) = inner.get_type(schema) else {
        return Ok(Transformed::no(expr));
    };
    if matches!(
        source,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) && is_text(target)
    {
        return Err(DataFusionError::Plan(
            "TYPE_MISMATCH: Cannot cast varbinary to varchar".into(),
        ));
    }
    if !needs_trino_cast(&source, target) {
        return Ok(Transformed::no(expr));
    }
    let function = if is_try { try_cast_udf() } else { cast_udf() };
    let target = Expr::Literal(ScalarValue::try_from(target)?, None);
    Ok(Transformed::yes(Expr::ScalarFunction(
        datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(function),
            vec![inner.clone(), target],
        ),
    )))
}

// ---------------------------------------------------------------------------
// The cast functions
// ---------------------------------------------------------------------------

type RowResult<T> = Result<Option<T>>;

/// Collects per-row results; `TRY_CAST` turns failures into NULL.
fn collect<T>(rows: usize, is_try: bool, mut f: impl FnMut(usize) -> RowResult<T>) -> Result<Vec<Option<T>>> {
    (0..rows)
        .map(|i| match f(i) {
            Err(_) if is_try => Ok(None),
            other => other,
        })
        .collect()
}

fn out_of_range_timestamp() -> DataFusionError {
    out_of_range("timestamp is out of the supported range")
}

fn zoned_to_date(call: &Call<'_>) -> Result<ArrayRef> {
    let input = call.arg(0)?;
    let ts = timestamps(input)?;
    let local = local_wall_clock(input, call.rows)?;
    let out = local
        .into_iter()
        .map(|v| {
            v.and_then(|raw| to_naive(raw, ts.unit))
                .map(|dt| days_from_date(dt.date()))
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Date32Array::from(out)))
}

fn floats_to_text(array: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    let out: Vec<Option<String>> = if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        (0..rows)
            .map(|i| (!values.is_null(i)).then(|| format_real(values.value(i))))
            .collect()
    } else {
        let values = cast(array, &DataType::Float64)?;
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal("expected Float64".into()))?;
        (0..rows)
            .map(|i| (!values.is_null(i)).then(|| format_double(values.value(i))))
            .collect()
    };
    Ok(string_array(out))
}

fn timestamps_to_text(array: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    let ts = timestamps(array)?;
    let precision = precision_of(ts.unit);
    let local = local_wall_clock(array, rows)?;
    let zone_name = super::datetime::column_zone(ts.zone.as_ref())?.map(super::zone::Zone::name);
    let out = local
        .into_iter()
        .map(|v| {
            v.and_then(|raw| to_naive(raw, ts.unit)).map(|dt| {
                let text = render_timestamp(dt, precision);
                match &zone_name {
                    Some(zone) => format!("{text} {zone}"),
                    None => text,
                }
            })
        })
        .collect();
    Ok(string_array(out))
}

fn integer_bounds(target: &DataType) -> (i128, i128, &'static str) {
    match target {
        DataType::Int8 => (i128::from(i8::MIN), i128::from(i8::MAX), "tinyint"),
        DataType::Int16 => (i128::from(i16::MIN), i128::from(i16::MAX), "smallint"),
        DataType::Int32 => (i128::from(i32::MIN), i128::from(i32::MAX), "integer"),
        _ => (i128::from(i64::MIN), i128::from(i64::MAX), "bigint"),
    }
}

/// Trino's `varchar` → integer: optional sign and digits only (no blanks).
fn text_to_i64(text: &str, target: &DataType) -> Result<i64> {
    let name = match target {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INT",
        _ => "BIGINT",
    };
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (min, max, _) = integer_bounds(target);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid_cast(format!("Cannot cast '{text}' to {name}")));
    }
    text.parse::<i128>()
        .ok()
        .filter(|v| (min..=max).contains(v))
        .and_then(|v| v.to_i64())
        .ok_or_else(|| invalid_cast(format!("Cannot cast '{text}' to {name}")))
}

static DATE_TEXT: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"^\s*([+-]?\d{4,})-(\d{1,2})-(\d{1,2})\s*$").ok());

fn text_to_date(text: &str) -> Result<i32> {
    let bad = || invalid_cast(format!("Value cannot be cast to date: {text}"));
    let caps = DATE_TEXT
        .as_ref()
        .and_then(|r| r.captures(text))
        .ok_or_else(bad)?;
    let part = |i: usize| caps.get(i).map(|m| m.as_str());
    let year: i32 = part(1).and_then(|y| y.parse().ok()).ok_or_else(bad)?;
    let month: u32 = part(2).and_then(|m| m.parse().ok()).ok_or_else(bad)?;
    let day: u32 = part(3).and_then(|d| d.parse().ok()).ok_or_else(bad)?;
    NaiveDate::from_ymd_opt(year, month, day)
        .map(days_from_date)
        .ok_or_else(bad)
}

/// Trino's casts from text read a blank between the date and the time; the
/// ISO `T` separator is rejected (`from_iso8601_timestamp` reads it).
fn iso_separator(text: &str) -> bool {
    TIMESTAMP_TEXT
        .as_ref()
        .and_then(|r| r.captures(text))
        .and_then(|c| c.get(3))
        .is_some_and(|day| text[day.end()..].starts_with('T'))
}

/// `CAST(varchar AS timestamp)`: a zone after the time is checked and
/// dropped (the wall clock is kept), as in Trino.
fn text_to_timestamp(text: &str, unit: TimeUnit) -> Result<i64> {
    let bad = || invalid_cast(format!("Value cannot be cast to timestamp: {text}"));
    let parsed = parse_timestamp_text(text)
        .filter(|_| !iso_separator(text))
        .ok_or_else(bad)?;
    if let Some(zone) = &parsed.zone {
        Zone::parse(zone.trim()).ok_or_else(bad)?;
    }
    round_to_unit(parsed.local, unit).ok_or_else(out_of_range_timestamp)
}

/// Raw value of a wall clock in `unit`, the extra fraction rounded half up
/// (works outside the nanosecond range, e.g. year 99).
pub(crate) fn round_to_unit(local: NaiveDateTime, unit: TimeUnit) -> Option<i64> {
    let nanos_per_unit = 1_000_000_000 / per_second(unit);
    let remainder = i64::from(local.nanosecond() % 1_000_000_000) % nanos_per_unit;
    let truncated = local - chrono::Duration::nanoseconds(remainder);
    let base = from_naive(truncated, unit)?;
    if remainder * 2 >= nanos_per_unit {
        base.checked_add(1)
    } else {
        Some(base)
    }
}

fn decimal_scalar(unscaled: Option<i128>, precision: u8, scale: i8, shown: &str) -> Result<ScalarValue> {
    let limit = 10_i128.checked_pow(u32::from(precision)).unwrap_or(i128::MAX);
    let value = unscaled
        .filter(|v| v.abs() < limit)
        .ok_or_else(|| invalid_cast(format!("Cannot cast {shown} to DECIMAL({precision}, {scale})")))?;
    Ok(ScalarValue::Decimal128(Some(value), precision, scale))
}

/// `CAST(varchar AS T)` for numbers, booleans, dates and plain timestamps.
fn text_to_scalar(text: &str, target: &DataType) -> Result<ScalarValue> {
    Ok(match target {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            ScalarValue::Int64(Some(text_to_i64(text, target)?)).cast_to(target)?
        }
        DataType::Float64 | DataType::Float32 => {
            let value = super::json_cast::java_parse_double(text)
                .ok_or_else(|| invalid_cast(format!("Cannot cast '{text}' to DOUBLE")))?;
            ScalarValue::Float64(Some(value)).cast_to(target)?
        }
        DataType::Decimal128(precision, scale) => decimal_scalar(
            super::json_cast::decimal_unscaled(text, *scale),
            *precision,
            *scale,
            &format!("VARCHAR '{text}'"),
        )?,
        DataType::Boolean => ScalarValue::Boolean(Some(match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => true,
            "f" | "false" | "0" => false,
            _ => return Err(invalid_cast(format!("Cannot cast '{text}' to BOOLEAN"))),
        })),
        DataType::Date32 => ScalarValue::Date32(Some(text_to_date(text)?)),
        DataType::Timestamp(unit, None) => {
            ScalarValue::Int64(Some(text_to_timestamp(text, *unit)?)).cast_to(target)?
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "unexpected text cast to {other}"
            )));
        }
    })
}

fn text_to_value(input: &ArrayRef, rows: usize, target: &DataType, is_try: bool) -> Result<ArrayRef> {
    let text = cast(input, &DataType::Utf8)?;
    let text = strings(&text)?;
    let values = collect(rows, is_try, |i| {
        text.at(i).map(|t| text_to_scalar(t, target)).transpose()
    })?;
    let values = values
        .into_iter()
        .map(|v| v.map_or_else(|| ScalarValue::try_from(target), Ok))
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Ok(datafusion::arrow::array::new_empty_array(target));
    }
    ScalarValue::iter_to_array(values)
}

fn floats_to_int(array: &ArrayRef, rows: usize, target: &DataType, is_try: bool) -> Result<ArrayRef> {
    let values = cast(array, &DataType::Float64)?;
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Internal("expected Float64".into()))?;
    let (min, max, name) = integer_bounds(target);
    let out = collect(rows, is_try, |i| {
        let Some(v) = values.at(i) else { return Ok(None) };
        if v.is_nan() {
            return Err(invalid_cast(format!("Cannot cast double NaN to {name}")));
        }
        let rounded = v.round();
        match rounded.to_i128() {
            Some(n) if (min..=max).contains(&n) => Ok(n.to_i64()),
            // Trino: `bigint` reports a failed cast, the narrower types a
            // value out of their range.
            _ if *target == DataType::Int64 => Err(invalid_cast(format!(
                "Unable to cast {} to {name}",
                format_double(v)
            ))),
            _ => Err(out_of_range(format!(
                "Out of range for {name}: {}",
                format_double(v)
            ))),
        }
    })?;
    Ok(cast(&super::support::int_array(out), target)?)
}

fn decimals_to_int(array: &ArrayRef, rows: usize, target: &DataType, is_try: bool) -> Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| DataFusionError::Internal("expected Decimal128".into()))?;
    let scale = u32::try_from(values.scale().max(0)).unwrap_or(0);
    let factor = 10_i128.checked_pow(scale).unwrap_or(i128::MAX);
    let (min, max, name) = integer_bounds(target);
    let out = collect(rows, is_try, |i| {
        if values.is_null(i) {
            return Ok(None);
        }
        let v = values.value(i);
        let (q, r) = (v / factor, v % factor);
        // HALF_UP: away from zero on ties.
        let rounded = if r.abs() * 2 >= factor { q + v.signum() } else { q };
        if (min..=max).contains(&rounded) {
            Ok(rounded.to_i64())
        } else {
            Err(invalid_cast(format!(
                "Cannot cast '{rounded}' to {}",
                name.to_uppercase()
            )))
        }
    })?;
    Ok(cast(&super::support::int_array(out), target)?)
}

fn invoke_cast(call: &Call<'_>, is_try: bool) -> Result<ArrayRef> {
    let input = call.arg(0)?;
    match (input.data_type(), call.return_type) {
        (DataType::Timestamp(_, Some(_)), DataType::Date32) => zoned_to_date(call),
        (DataType::Float32 | DataType::Float64, t) if is_text(t) => floats_to_text(input, call.rows),
        (DataType::Float32 | DataType::Float64, t) if is_int(t) => floats_to_int(input, call.rows, t, is_try),
        (DataType::Decimal128(..), t) if is_int(t) => decimals_to_int(input, call.rows, t, is_try),
        (f, t) if is_text(f) => text_to_value(input, call.rows, t, is_try),
        (from, to) => Err(DataFusionError::Internal(format!(
            "unexpected Trino cast {from} -> {to}"
        ))),
    }
}

const IDENTITY: super::support::CoerceFn = |_, types| Ok(types.to_vec());

fn target_type(ret: &super::support::Ret<'_>) -> Result<DataType> {
    ret.types
        .get(1)
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("cast target missing".into()))
}

/// Trino's text of any value (`CAST(x AS varchar)`).
pub(crate) fn to_text(array: &ArrayRef, rows: usize) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Float32 | DataType::Float64 => floats_to_text(array, rows),
        DataType::Timestamp(..) => timestamps_to_text(array, rows),
        _ => Ok(cast(array, &DataType::Utf8)?),
    }
}

pub(crate) fn cast_udf() -> ScalarUDF {
    Func::new(CAST, IDENTITY, target_type, |call| invoke_cast(call, false))
        .zone_sensitive()
        .udf()
}

pub(crate) fn try_cast_udf() -> ScalarUDF {
    Func::new(TRY_CAST, IDENTITY, target_type, |call| invoke_cast(call, true))
        .zone_sensitive()
        .udf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_print_like_trino() {
        let cases = [
            (1.5, "1.5E0"),
            (1e10, "1.0E10"),
            (0.001, "1.0E-3"),
            (123_456_789.123, "1.23456789123E8"),
            (0.0, "0E0"),
            (-0.0, "-0E0"),
            (100.0, "1.0E2"),
            (1.0 / 3.0, "3.333333333333333E-1"),
            (f64::MAX, "1.7976931348623157E308"),
            (4.9e-324, "4.9E-324"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (f64::NAN, "NaN"),
            (-1.5, "-1.5E0"),
        ];
        for (value, text) in cases {
            assert_eq!(format_double(value), text, "{value}");
        }
    }

    #[test]
    fn reals_print_seven_digits() {
        assert_eq!(format_real(0.1), "1.0E-1");
        assert_eq!(format_real(1e10), "1.0E10");
        assert_eq!(format_real(f32::MAX), "3.402823E38");
        assert_eq!(format_real(1.1), "1.1E0");
        assert_eq!(format_real(0.0), "0E0");
    }

    #[test]
    fn timestamps_render_with_precision() {
        let dt = NaiveDateTime::parse_from_str("2024-01-01 10:00:00.123456", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        assert_eq!(render_timestamp(dt, 3), "2024-01-01 10:00:00.123");
        assert_eq!(render_timestamp(dt, 6), "2024-01-01 10:00:00.123456");
        assert_eq!(render_timestamp(dt, 0), "2024-01-01 10:00:00");
        assert_eq!(render_timestamp(dt, 9), "2024-01-01 10:00:00.123456000");
    }

    #[test]
    fn parses_timestamp_literals() {
        let t = parse_timestamp_text("2024-01-01 10:00:00.12 America/Sao_Paulo").unwrap();
        assert_eq!(t.fraction_digits, 2);
        assert_eq!(t.zone.as_deref(), Some("America/Sao_Paulo"));
        let t = parse_timestamp_text("2024-01-01 10:00:00+02:00").unwrap();
        assert_eq!(t.zone.as_deref(), Some("+02:00"));
        let t = parse_timestamp_text("2024-01-01").unwrap();
        assert_eq!(t.local.hour(), 0);
        assert!(parse_timestamp_text("2024-13-01").is_none());
        assert!(parse_timestamp_text("yesterday").is_none());
    }

    #[test]
    /// The kinds the engine's casts do not cover (the others are theirs, see
    /// the module documentation).
    fn only_divergent_casts_are_rewritten() {
        let ms_utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        let us = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert!(needs_trino_cast(&ms_utc, &DataType::Date32));
        assert!(needs_trino_cast(&DataType::Float64, &DataType::Utf8View));
        assert!(!needs_trino_cast(&DataType::Int64, &DataType::Utf8));
        assert!(
            needs_trino_cast(&DataType::Utf8, &DataType::Int64),
            "Trino's varchar parsing"
        );
        assert!(!needs_trino_cast(&DataType::Utf8, &DataType::Utf8));
        assert!(!needs_trino_cast(&DataType::Int64, &DataType::Decimal128(10, 2)));
        assert!(needs_trino_cast(&DataType::Decimal128(5, 2), &DataType::Int32));
        assert!(needs_trino_cast(&DataType::Float64, &DataType::Int64));
        // The engine's (`crate::timestamp_casts`, `crate::casts`, `crate::overflow`).
        for (from, to) in [
            (&ms_utc, &ms),
            (&ms, &ms_utc),
            (&us, &ms),
            (&DataType::Float64, &DataType::Decimal128(10, 2)),
            (&DataType::Utf8, &ms_utc),
        ] {
            assert!(!needs_trino_cast(from, to), "{from} -> {to}");
        }
    }
}
