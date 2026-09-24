//! `CAST` to and from `json`.
//!
//! A `json` value is `varchar` text on a field marked as json (see
//! `support::json_field`); `json_parse`, `json_extract`, `json_array_get`,
//! `JSON '…'` and `CAST(… AS json)` produce it. `CAST(json AS T)` then reads
//! the JSON value the way Trino's `JsonUtil` does (a JSON string cast to
//! `varchar` is its text, not the quoted token) instead of treating the text
//! as a `varchar` (see the `cast` analyzer rule). A json value that lost the
//! mark (e.g. through `CASE`) is plain `varchar` again.
//!
//! Supported targets: `varchar`, `boolean`, integers, `real`, `double`,
//! `decimal`, `array(T)`, `map(K, V)`, `row(…)`, and nesting of them, as in
//! Trino; any other target is `TYPE_MISMATCH`.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ScalarUDF;
use num_traits::ToPrimitive;

use super::cast::format_double;
use super::json::{Json, is_integer_token, java_double, parse, write_string};
use super::support::{Call, Func, invalid_cast, string_array, strings, trino_type_name};

pub(crate) const TO_JSON: &str = "codelake_trino_to_json";
pub(crate) const FROM_JSON: &str = "codelake_trino_json_cast";
pub(crate) const TRY_FROM_JSON: &str = "codelake_trino_json_try_cast";

// ---------------------------------------------------------------------------
// value → json
// ---------------------------------------------------------------------------

/// Trino's text of a value as a map key or JSON string (`CAST(x AS varchar)`).
fn text_of(value: &ScalarValue) -> Result<String> {
    let array = value.to_array()?;
    let text = super::cast::to_text(&array, 1)?;
    let text = strings(&text)?;
    Ok(text.value(0).to_owned())
}

/// Java's text of NaN and the infinities.
fn non_finite(nan: bool, positive: bool) -> String {
    match (nan, positive) {
        (true, _) => "NaN".to_owned(),
        (false, true) => "Infinity".to_owned(),
        (false, false) => "-Infinity".to_owned(),
    }
}

fn write_json(value: &ScalarValue, out: &mut String) -> Result<()> {
    if value.is_null() {
        out.push_str("null");
        return Ok(());
    }
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            write_string(s, out);
        }
        ScalarValue::Boolean(Some(b)) => out.push_str(if *b { "true" } else { "false" }),
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => out.push_str(&value.to_string()),
        ScalarValue::Decimal128(..) => out.push_str(&text_of(value)?),
        // Jackson quotes the numbers JSON cannot spell.
        ScalarValue::Float64(Some(f)) if !f.is_finite() => {
            write_string(&non_finite(f.is_nan(), *f > 0.0), out);
        }
        ScalarValue::Float32(Some(f)) if !f.is_finite() => {
            write_string(&non_finite(f.is_nan(), *f > 0.0), out);
        }
        ScalarValue::Float64(Some(f)) => out.push_str(&java_double(*f)),
        // Jackson writes a float with Java's `Float.toString`.
        ScalarValue::Float32(Some(f)) => {
            let text = f.to_string();
            out.push_str(&text);
            if !text.contains(['.', 'e', 'E', 'N', 'n']) {
                out.push_str(".0");
            }
        }
        ScalarValue::List(list) => {
            out.push('[');
            let values = list.value(0);
            for i in 0..values.len() {
                if i > 0 {
                    out.push(',');
                }
                write_json(&ScalarValue::try_from_array(&values, i)?, out)?;
            }
            out.push(']');
        }
        ScalarValue::Map(map) => {
            let entries = map.value(0);
            let mut pairs = (0..entries.len())
                .map(|i| {
                    let key = text_of(&ScalarValue::try_from_array(entries.column(0), i)?)?;
                    Ok((key, ScalarValue::try_from_array(entries.column(1), i)?))
                })
                .collect::<Result<Vec<_>>>()?;
            pairs.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
            out.push('{');
            for (i, (key, item)) in pairs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write_json(item, out)?;
            }
            out.push('}');
        }
        // A row is an object keyed by field name; anonymous fields (`c0`,
        // `c1`, … here) have an empty name, as in Trino.
        ScalarValue::Struct(row) => {
            out.push('{');
            for (i, (field, column)) in row.fields().iter().zip(row.columns()).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let anonymous = field
                    .name()
                    .strip_prefix('c')
                    .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
                write_string(if anonymous { "" } else { field.name() }, out);
                out.push(':');
                write_json(&ScalarValue::try_from_array(column, 0)?, out)?;
            }
            out.push('}');
        }
        other => write_string(&text_of(other)?, out),
    }
    Ok(())
}

/// `CAST(x AS json)`; a SQL NULL stays NULL.
fn to_json_column(call: &Call<'_>) -> Result<ArrayRef> {
    let input = call.arg(0)?;
    let out = (0..call.rows)
        .map(|i| {
            let value = ScalarValue::try_from_array(input, i)?;
            if value.is_null() {
                return Ok(None);
            }
            let mut text = String::new();
            write_json(&value, &mut text)?;
            Ok(Some(text))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(string_array(out))
}

// ---------------------------------------------------------------------------
// json → value
// ---------------------------------------------------------------------------

fn cannot_cast(target: &DataType, value: &Json) -> DataFusionError {
    let mut text = String::new();
    super::json::write(value, false, &mut text);
    invalid_cast(format!(
        "Cannot cast to {}. Unexpected JSON value: {text}",
        trino_type_name(target)
    ))
}

fn not_supported(target: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "TYPE_MISMATCH: Cannot cast json to {}",
        trino_type_name(target)
    ))
}

/// `DoubleOperators.castToLong`: half away from zero, range checked.
fn double_to_i64(value: f64, type_name: &str) -> Result<i64> {
    let rounded = value.round();
    if value.is_nan() || !(-9.223_372_036_854_776e18..9.223_372_036_854_776e18).contains(&rounded) {
        return Err(invalid_cast(format!(
            "Unable to cast {} to {type_name}",
            format_double(value)
        )));
    }
    rounded
        .to_i64()
        .ok_or_else(|| invalid_cast(format!("Unable to cast {} to {type_name}", format_double(value))))
}

fn json_to_i64(value: &Json, target: &DataType) -> Result<i64> {
    match value {
        Json::String(s) => s
            .parse::<i64>()
            .map_err(|_| invalid_cast(format!("Cannot cast '{s}' to BIGINT"))),
        Json::Number(token) if is_integer_token(token) => token
            .parse::<i64>()
            .map_err(|_| invalid_cast(format!("Out of range for bigint: {token}"))),
        Json::Number(token) => double_to_i64(token.parse::<f64>().unwrap_or(f64::NAN), "bigint"),
        Json::Bool(b) => Ok(i64::from(*b)),
        other => Err(cannot_cast(target, other)),
    }
}

fn json_to_f64(value: &Json, target: &DataType) -> Result<f64> {
    match value {
        Json::String(s) => {
            java_parse_double(s).ok_or_else(|| invalid_cast(format!("Cannot cast '{s}' to DOUBLE")))
        }
        Json::Number(token) => Ok(token.parse::<f64>().unwrap_or(f64::NAN)),
        Json::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
        other => Err(cannot_cast(target, other)),
    }
}

/// `Double.parseDouble`: surrounding blanks, `NaN`, `Infinity`.
pub(crate) fn java_parse_double(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    match trimmed {
        "NaN" => Some(f64::NAN),
        "Infinity" | "+Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ if trimmed
            .chars()
            .any(|c| c.is_ascii_alphabetic() && !matches!(c, 'e' | 'E')) =>
        {
            None
        }
        _ => trimmed.parse().ok(),
    }
}

/// Decimal text rounded half up to `scale`, as the unscaled integer.
pub(crate) fn decimal_unscaled(text: &str, scale: i8) -> Option<i128> {
    let text = text.trim();
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match digits.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().ok()?),
        None => (digits, 0),
    };
    let (int_part, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int_part.is_empty() && fraction.is_empty()
        || !int_part
            .chars()
            .chain(fraction.chars())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let all: String = format!("{int_part}{fraction}");
    // value = all × 10^(exponent − len(fraction)); keep `scale` fractional digits.
    let shift = i32::from(scale) + exponent - i32::try_from(fraction.len()).ok()?;
    let mut unscaled: i128 = if all.is_empty() { 0 } else { all.parse().ok()? };
    if shift >= 0 {
        unscaled = unscaled.checked_mul(10_i128.checked_pow(u32::try_from(shift).ok()?)?)?;
    } else {
        let divisor = 10_i128.checked_pow(u32::try_from(-shift).ok()?)?;
        let (q, r) = (unscaled / divisor, unscaled % divisor);
        unscaled = if r * 2 >= divisor { q + 1 } else { q };
    }
    Some(if negative { -unscaled } else { unscaled })
}

fn json_to_map(value: &Json, target: &DataType) -> Result<ScalarValue> {
    let (Json::Object(pairs), DataType::Map(entries, _)) = (value, target) else {
        return Err(cannot_cast(target, value));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(not_supported(target));
    };
    let (Some(key_field), Some(value_field)) = (fields.first(), fields.get(1)) else {
        return Err(not_supported(target));
    };
    let mut out: Vec<(ScalarValue, ScalarValue)> = Vec::with_capacity(pairs.len());
    for (key, item) in pairs {
        let key = json_to_scalar(&Json::String(key.clone()), key_field.data_type())?;
        if out.iter().any(|(k, _)| k == &key) {
            return Err(invalid_cast(format!(
                "Duplicate map keys ({key}) are not allowed"
            )));
        }
        out.push((key, json_to_scalar(item, value_field.data_type())?));
    }
    let maps =
        super::collections::build_maps(vec![Some(out)], key_field.data_type(), value_field.data_type())?;
    ScalarValue::try_from_array(&maps, 0)
}

/// A row from a JSON array (by position) or object (by field name, any case).
fn json_to_row(value: &Json, target: &DataType, fields: &Fields) -> Result<ScalarValue> {
    let values: Vec<Json> = match value {
        Json::Array(items) if items.len() == fields.len() => items.clone(),
        Json::Object(pairs) => fields
            .iter()
            .map(|f| {
                pairs
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(f.name()))
                    .map_or(Json::Null, |(_, v)| v.clone())
            })
            .collect(),
        other => return Err(cannot_cast(target, other)),
    };
    let columns = values
        .iter()
        .zip(fields.iter())
        .map(|(v, f)| json_to_scalar(v, f.data_type())?.to_array())
        .collect::<Result<Vec<_>>>()?;
    Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
        fields.clone(),
        columns,
        None,
    )?)))
}

fn json_to_scalar(value: &Json, target: &DataType) -> Result<ScalarValue> {
    if matches!(value, Json::Null) {
        return ScalarValue::try_from(target);
    }
    match target {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let text = match value {
                Json::String(s) => s.clone(),
                Json::Number(token) if is_integer_token(token) => token.clone(),
                Json::Number(token) => format_double(token.parse::<f64>().unwrap_or(f64::NAN)),
                Json::Bool(b) => b.to_string(),
                other => return Err(cannot_cast(target, other)),
            };
            ScalarValue::Utf8(Some(text)).cast_to(target)
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let v = json_to_i64(value, target)?;
            let name = trino_type_name(target);
            ScalarValue::Int64(Some(v))
                .cast_to(target)
                .map_err(|_| invalid_cast(format!("Out of range for {name}: {v}")))
        }
        DataType::Float64 => Ok(ScalarValue::Float64(Some(json_to_f64(value, target)?))),
        DataType::Float32 => ScalarValue::Float64(Some(json_to_f64(value, target)?)).cast_to(target),
        DataType::Boolean => Ok(ScalarValue::Boolean(Some(match value {
            Json::Bool(b) => *b,
            Json::Number(token) => token.parse::<f64>().unwrap_or(f64::NAN) != 0.0,
            Json::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => true,
                "false" | "f" | "0" => false,
                _ => return Err(invalid_cast(format!("Cannot cast '{s}' to BOOLEAN"))),
            },
            other => return Err(cannot_cast(target, other)),
        }))),
        DataType::Decimal128(precision, scale) => {
            let text = match value {
                Json::String(s) | Json::Number(s) => s.clone(),
                Json::Bool(b) => i32::from(*b).to_string(),
                other => return Err(cannot_cast(target, other)),
            };
            let unscaled = decimal_unscaled(&text, *scale)
                .filter(|v| v.unsigned_abs() < 10_u128.pow(u32::from(*precision)))
                .ok_or_else(|| {
                    invalid_cast(format!("Cannot cast '{text}' to {}", trino_type_name(target)))
                })?;
            Ok(ScalarValue::Decimal128(Some(unscaled), *precision, *scale))
        }
        DataType::List(field) => {
            let Json::Array(items) = value else {
                return Err(cannot_cast(target, value));
            };
            let values = items
                .iter()
                .map(|item| json_to_scalar(item, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(ScalarValue::List(ScalarValue::new_list_nullable(
                &values,
                field.data_type(),
            )))
        }
        DataType::Map(..) => json_to_map(value, target),
        DataType::Struct(fields) => json_to_row(value, target, fields),
        _ => Err(not_supported(target)),
    }
}

/// JSON texts read as `target` (`CAST(json AS target)`).
pub(crate) fn from_json_text(
    text: &datafusion::arrow::array::StringArray,
    target: &DataType,
    rows: usize,
    is_try: bool,
) -> Result<ArrayRef> {
    let values = (0..rows)
        .map(|i| {
            if text.is_null(i) {
                return ScalarValue::try_from(target);
            }
            let raw = text.value(i);
            let converted = parse(raw)
                .ok_or_else(|| {
                    invalid_cast(format!(
                        "Cannot cast to {}. Invalid JSON: {raw}",
                        trino_type_name(target)
                    ))
                })
                .and_then(|json| json_to_scalar(&json, target));
            match converted {
                Err(error) if is_try && super::errors::caught_by_try(&error) => ScalarValue::try_from(target),
                other => other,
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Ok(datafusion::arrow::array::new_empty_array(target));
    }
    ScalarValue::iter_to_array(values)
}

fn from_json_column(call: &Call<'_>, is_try: bool) -> Result<ArrayRef> {
    from_json_text(strings(call.arg(0)?)?, call.return_type, call.rows, is_try)
}

/// Rejects targets `CAST(json AS …)` does not support, at planning time.
fn check_target(target: &DataType) -> Result<()> {
    match target {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..) => Ok(()),
        DataType::List(field) => check_target(field.data_type()),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields.iter().try_for_each(|f| check_target(f.data_type())),
            _ => Err(not_supported(target)),
        },
        DataType::Struct(fields) => fields.iter().try_for_each(|f| check_target(f.data_type())),
        _ => Err(not_supported(target)),
    }
}

fn from_json_function(name: &'static str, invoke: super::support::InvokeFn) -> ScalarUDF {
    Func::new(
        name,
        |_, types| Ok(types.to_vec()),
        |r| {
            let target = r
                .types
                .get(1)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("json cast target".into()))?;
            check_target(&target)?;
            Ok(target)
        },
        invoke,
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            TO_JSON,
            |_, types| Ok(types.to_vec()),
            super::support::UTF8,
            to_json_column,
        )
        .returns_json()
        .udf(),
        from_json_function(FROM_JSON, |call| from_json_column(call, false)),
        from_json_function(TRY_FROM_JSON, |call| from_json_column(call, true)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn cast(text: &str, target: &DataType) -> Result<ScalarValue> {
        json_to_scalar(&parse(text).unwrap(), target)
    }

    #[test]
    fn scalars_read_json_values() {
        assert_eq!(
            cast(r#""abc""#, &DataType::Utf8).unwrap(),
            ScalarValue::from("abc")
        );
        assert_eq!(cast("1.5", &DataType::Utf8).unwrap(), ScalarValue::from("1.5E0"));
        assert_eq!(cast("12", &DataType::Utf8).unwrap(), ScalarValue::from("12"));
        assert_eq!(
            cast("1.5", &DataType::Int64).unwrap(),
            ScalarValue::Int64(Some(2))
        );
        assert_eq!(
            cast(r#""12""#, &DataType::Int32).unwrap(),
            ScalarValue::Int32(Some(12))
        );
        assert!(cast("[1]", &DataType::Utf8).is_err());
        assert!(cast("9223372036854775808", &DataType::Int64).is_err());
        assert_eq!(cast("null", &DataType::Int64).unwrap(), ScalarValue::Int64(None));
        assert_eq!(
            cast("1.255", &DataType::Decimal128(4, 2)).unwrap(),
            ScalarValue::Decimal128(Some(126), 4, 2)
        );
    }

    #[test]
    fn decimals_round_half_up() {
        assert_eq!(decimal_unscaled("1.255", 2), Some(126));
        assert_eq!(decimal_unscaled("-1.255", 2), Some(-126));
        assert_eq!(decimal_unscaled("1e2", 1), Some(1000));
        assert_eq!(decimal_unscaled("x", 1), None);
    }

    #[test]
    fn values_write_as_json() {
        let mut out = String::new();
        let list = ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
            &DataType::Int64,
        ));
        write_json(&list, &mut out).unwrap();
        assert_eq!(out, "[1,null]");
        let mut out = String::new();
        write_json(&ScalarValue::from("a\"b"), &mut out).unwrap();
        assert_eq!(out, r#""a\"b""#);
        let date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
        let days = super::super::datetime::days_from_date(date);
        let mut out = String::new();
        write_json(&ScalarValue::Date32(Some(days)), &mut out).unwrap();
        assert_eq!(out, r#""2024-01-02""#);
    }
}
