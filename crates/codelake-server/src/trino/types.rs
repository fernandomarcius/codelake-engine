//! Arrow → Trino client protocol: type names, type signatures and JSON values,
//! encoded exactly as Trino 431 sends them (the `trino` Python client and the
//! CLI parse them by `rawType`).

use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::{DataFusionError, Result};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Column {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) trino_type: String,
    pub(crate) type_signature: Value,
}

fn long_arg(value: i64) -> Value {
    json!({"kind": "LONG", "value": value})
}

fn signature(raw: &str, arguments: &[Value]) -> Value {
    json!({"rawType": raw, "arguments": arguments})
}

fn precision(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// Trino type name and signature for an Arrow type.
pub(crate) fn trino_type(data_type: &DataType) -> (String, Value) {
    match data_type {
        DataType::Boolean => ("boolean".into(), signature("boolean", &[])),
        DataType::Int8 => ("tinyint".into(), signature("tinyint", &[])),
        DataType::Int16 | DataType::UInt8 => ("smallint".into(), signature("smallint", &[])),
        DataType::Int32 | DataType::UInt16 => ("integer".into(), signature("integer", &[])),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => ("bigint".into(), signature("bigint", &[])),
        DataType::Float32 => ("real".into(), signature("real", &[])),
        DataType::Float64 => ("double".into(), signature("double", &[])),
        DataType::Decimal128(p, s) => (
            format!("decimal({p},{s})"),
            signature("decimal", &[long_arg(i64::from(*p)), long_arg(i64::from(*s))]),
        ),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            ("varbinary".into(), signature("varbinary", &[]))
        }
        DataType::Date32 | DataType::Date64 => ("date".into(), signature("date", &[])),
        DataType::Timestamp(unit, None) => {
            let p = precision(*unit);
            (format!("timestamp({p})"), signature("timestamp", &[long_arg(p)]))
        }
        DataType::Timestamp(unit, Some(_)) => {
            let p = precision(*unit);
            (
                format!("timestamp({p}) with time zone"),
                signature("timestamp with time zone", &[long_arg(p)]),
            )
        }
        DataType::Time64(unit) | DataType::Time32(unit) => {
            let p = precision(*unit);
            (format!("time({p})"), signature("time", &[long_arg(p)]))
        }
        DataType::List(field) | DataType::LargeList(field) => {
            let (inner, inner_signature) = trino_type(field.data_type());
            (
                format!("array({inner})"),
                signature("array", &[json!({"kind": "TYPE", "value": inner_signature})]),
            )
        }
        DataType::Null => ("unknown".into(), signature("unknown", &[])),
        // Everything else travels as text.
        _ => (
            "varchar".into(),
            signature("varchar", &[long_arg(i64::from(i32::MAX))]),
        ),
    }
}

pub(crate) fn columns(schema: &datafusion::arrow::datatypes::Schema) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let (trino_type, type_signature) = trino_type(field.data_type());
            Column {
                name: field.name().clone(),
                trino_type,
                type_signature,
            }
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

/// `YYYY-MM-DD HH:MM:SS[.fraction]` with the fraction padded to `digits`.
pub(crate) fn timestamp_text(nanos_since_epoch: i128, digits: u32) -> String {
    let nanos_per_day: i128 = 86_400_000_000_000;
    let days = i64::try_from(nanos_since_epoch.div_euclid(nanos_per_day)).unwrap_or_default();
    let of_day = nanos_since_epoch.rem_euclid(nanos_per_day);
    let (y, m, d) = civil_from_days(days);
    let secs = of_day / 1_000_000_000;
    let fraction = of_day % 1_000_000_000;
    let mut text = format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    );
    if digits > 0 {
        let scaled = fraction / 10_i128.pow(9 - digits);
        let _ = write!(text, ".{scaled:0width$}", width = digits as usize);
    }
    text
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

fn decimal_text(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let digits = value.unsigned_abs().to_string();
    let scale = usize::try_from(scale).unwrap_or(0);
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{}{int}.{frac}", if value < 0 { "-" } else { "" })
}

/// JSON value of row `row` of `array`, as Trino encodes it.
#[allow(
    clippy::too_many_lines,
    reason = "one arm per Arrow type keeps the encoding readable"
)]
pub(crate) fn value(array: &ArrayRef, row: usize) -> Result<Value> {
    if array.is_null(row) {
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
        DataType::UInt64 => json!(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => float(f64::from(array.as_primitive::<Float32Type>().value(row))),
        DataType::Float64 => float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Decimal128(_, s) => json!(decimal_text(
            array.as_primitive::<Decimal128Type>().value(row),
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
        DataType::Timestamp(unit, tz) => {
            let raw = i128::from(match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
            });
            let nanos = raw
                * match unit {
                    TimeUnit::Second => 1_000_000_000,
                    TimeUnit::Millisecond => 1_000_000,
                    TimeUnit::Microsecond => 1_000,
                    TimeUnit::Nanosecond => 1,
                };
            let digits = u32::try_from(precision(*unit)).unwrap_or(3);
            let text = timestamp_text(nanos, digits);
            // Values are stored in UTC; name the zone like Trino does.
            json!(if tz.is_some() { format!("{text} UTC") } else { text })
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let nanos = i128::from(array.as_primitive::<Time64MicrosecondType>().value(row)) * 1_000;
            json!(timestamp_text(nanos, 6)[11..].to_owned())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let nanos = i128::from(array.as_primitive::<Time64NanosecondType>().value(row));
            json!(timestamp_text(nanos, 9)[11..].to_owned())
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>().value(row);
            Value::Array((0..list.len()).map(|i| value(&list, i)).collect::<Result<_>>()?)
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>().value(row);
            Value::Array((0..list.len()).map(|i| value(&list, i)).collect::<Result<_>>()?)
        }
        _ => {
            let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
                .map_err(DataFusionError::from)?;
            json!(formatter.value(row).to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use std::sync::Arc;

    fn arr(array: impl Array + 'static) -> ArrayRef {
        Arc::new(array)
    }

    #[test]
    fn type_names_match_trino() {
        assert_eq!(trino_type(&DataType::Int64).0, "bigint");
        assert_eq!(trino_type(&DataType::Int32).0, "integer");
        assert_eq!(trino_type(&DataType::Utf8).0, "varchar");
        assert_eq!(trino_type(&DataType::Decimal128(18, 2)).0, "decimal(18,2)");
        assert_eq!(
            trino_type(&DataType::Decimal128(18, 2)).1,
            json!({"rawType": "decimal", "arguments": [{"kind": "LONG", "value": 18}, {"kind": "LONG", "value": 2}]})
        );
        assert_eq!(
            trino_type(&DataType::Timestamp(TimeUnit::Millisecond, None)).0,
            "timestamp(3)"
        );
        assert_eq!(
            trino_type(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))).0,
            "timestamp(6) with time zone"
        );
        assert_eq!(trino_type(&DataType::Binary).0, "varbinary");
        assert_eq!(trino_type(&DataType::Date32).0, "date");
    }

    #[test]
    fn values_are_encoded_like_trino() {
        assert_eq!(value(&arr(Int64Array::from(vec![42])), 0).unwrap(), json!(42));
        assert_eq!(
            value(&arr(BooleanArray::from(vec![true])), 0).unwrap(),
            json!(true)
        );
        assert_eq!(
            value(&arr(StringArray::from(vec![Some("ação"), None])), 1).unwrap(),
            Value::Null
        );
        let dec = Decimal128Array::from(vec![1050, -5, 0])
            .with_precision_and_scale(18, 2)
            .unwrap();
        assert_eq!(value(&arr(dec.clone()), 0).unwrap(), json!("10.50"));
        assert_eq!(value(&arr(dec.clone()), 1).unwrap(), json!("-0.05"));
        assert_eq!(value(&arr(dec), 2).unwrap(), json!("0.00"));
        assert_eq!(
            value(&arr(Float64Array::from(vec![f64::NAN])), 0).unwrap(),
            json!("NaN")
        );
        assert_eq!(
            value(&arr(BinaryArray::from(vec![&[0_u8, 42][..]])), 0).unwrap(),
            json!("ACo=")
        );
        assert_eq!(
            value(&arr(Date32Array::from(vec![20_454])), 0).unwrap(),
            json!("2026-01-01")
        );
        let ts = TimestampMillisecondArray::from(vec![1_767_261_600_123]);
        assert_eq!(value(&arr(ts), 0).unwrap(), json!("2026-01-01 10:00:00.123"));
        let before_epoch = TimestampMicrosecondArray::from(vec![-1]);
        assert_eq!(
            value(&arr(before_epoch), 0).unwrap(),
            json!("1969-12-31 23:59:59.999999")
        );
        let tz = TimestampMillisecondArray::from(vec![0]).with_timezone("UTC");
        assert_eq!(value(&arr(tz), 0).unwrap(), json!("1970-01-01 00:00:00.000 UTC"));
    }

    #[test]
    fn decimal_text_edges() {
        assert_eq!(decimal_text(5, 3), "0.005");
        assert_eq!(decimal_text(-12_345, 2), "-123.45");
        assert_eq!(decimal_text(7, 0), "7");
    }
}
