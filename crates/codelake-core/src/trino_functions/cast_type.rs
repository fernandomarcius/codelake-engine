//! `CAST(x AS <Trino type>)` for the types the SQL parser cannot spell
//! (`array(T)`, `map(K, V)`, `row(name T, …)`); `preprocess` turns the cast
//! into `codelake_trino_cast_type(x, '<type>')`.
//!
//! The type text follows Trino's grammar and maps to Arrow as the rest of the
//! library does (`integer` → Int32, `json` → varchar, `timestamp(p) with time
//! zone` in the session zone). A json value is read as JSON (`json_cast`);
//! anything else is converted element by element (rows by position).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, MapArray, StructArray, new_null_array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ScalarUDF;

use super::errors::caught_by_try;
use super::support::{Call, Func, Ret, invalid_cast, is_json_field, strings};

pub(crate) const CAST_TYPE: &str = "codelake_trino_cast_type";
pub(crate) const TRY_CAST_TYPE: &str = "codelake_trino_try_cast_type";

fn type_error(text: &str) -> DataFusionError {
    DataFusionError::Plan(format!("TYPE_MISMATCH: Unknown type: {text}"))
}

/// Splits `a, b(c, d), e` at top-level commas.
fn split_args(text: &str) -> Vec<&str> {
    let (mut parts, mut depth, mut start) = (Vec::new(), 0_usize, 0);
    for (i, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(text[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(text[start..].trim());
    parts
}

fn timestamp_unit(precision: Option<&str>) -> Result<TimeUnit> {
    let p: u32 = precision.map_or(Ok(3), |p| p.trim().parse().map_err(|_| type_error(p)))?;
    Ok(match p {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        7..=9 => TimeUnit::Nanosecond,
        _ => return Err(type_error(&format!("timestamp({p})"))),
    })
}

/// Arrow type of the element (field, key, value) type `text` of an array,
/// a map or a row. `char(n)` and `varchar(n)` are refused there: their
/// values would have to be padded or cut one by one (`CAST(ARRAY['ab  ']
/// AS array(char(3)))` is `['ab ']` in Trino 431), which the conversion
/// does not do.
fn element_type(text: &str, zone: &str) -> Result<DataType> {
    let lower = text.trim().to_ascii_lowercase();
    let head = lower.split('(').next().unwrap_or_default().trim();
    if head == "char" || (head == "varchar" && lower.contains('(')) {
        return Err(DataFusionError::NotImplemented(format!(
            "a cast to an array, map or row of {} is not supported yet: cast to one of varchar, then \
             the values to {}",
            text.trim(),
            text.trim()
        )));
    }
    parse_type(text, zone)
}

/// Arrow type of a Trino type text; `zone` is the session zone.
pub(crate) fn parse_type(text: &str, zone: &str) -> Result<DataType> {
    let text = text.trim();
    let lower = text.to_ascii_lowercase();
    let (head, args) = match lower.find('(') {
        Some(open) if lower.ends_with(')') || lower.contains(") with time zone") => {
            let close = text.rfind(')').ok_or_else(|| type_error(text))?;
            (lower[..open].trim().to_owned(), Some(&text[open + 1..close]))
        }
        _ => (lower.clone(), None),
    };
    let zoned = lower.ends_with("with time zone");
    Ok(match (head.as_str(), args) {
        ("boolean", None) => DataType::Boolean,
        ("tinyint", None) => DataType::Int8,
        ("smallint", None) => DataType::Int16,
        ("integer" | "int", None) => DataType::Int32,
        ("bigint", None) => DataType::Int64,
        ("real", None) => DataType::Float32,
        ("double" | "double precision", None) => DataType::Float64,
        ("decimal", None) => DataType::Decimal128(38, 0),
        ("decimal", Some(a)) => {
            let parts = split_args(a);
            let precision: u8 = parts
                .first()
                .and_then(|p| p.parse().ok())
                .ok_or_else(|| type_error(text))?;
            let scale: i8 = parts
                .get(1)
                .map_or(Some(0), |s| s.parse().ok())
                .ok_or_else(|| type_error(text))?;
            DataType::Decimal128(precision, scale)
        }
        ("varchar" | "char" | "json" | "uuid", _) => DataType::Utf8,
        ("varbinary", None) => DataType::Binary,
        ("date", None) => DataType::Date32,
        ("timestamp" | "timestamp with time zone" | "timestamp without time zone", p) => {
            let unit = timestamp_unit(p)?;
            DataType::Timestamp(unit, zoned.then(|| Arc::from(zone)))
        }
        ("array", Some(a)) => DataType::List(Arc::new(Field::new_list_field(element_type(a, zone)?, true))),
        ("map", Some(a)) => match split_args(a).as_slice() {
            [k, v] => super::collections::map_type(element_type(k, zone)?, element_type(v, zone)?),
            _ => return Err(type_error(text)),
        },
        ("row", Some(a)) => {
            let fields = split_args(a)
                .into_iter()
                .enumerate()
                .map(|(i, field)| {
                    // `name type` or just `type` (an anonymous field).
                    match field.split_once(char::is_whitespace) {
                        Some((name, rest)) if parse_type(field, zone).is_err() => Ok(Field::new(
                            name.trim_matches('"'),
                            element_type(rest, zone)?,
                            true,
                        )),
                        _ => Ok(Field::new(format!("c{i}"), element_type(field, zone)?, true)),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(Fields::from(fields))
        }
        _ => return Err(type_error(text)),
    })
}

/// Converts `array` to `target`, descending into lists, maps and rows.
pub(crate) fn convert(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    match (array.data_type(), target) {
        (DataType::Null, t) => Ok(new_null_array(t, array.len())),
        (DataType::List(_), DataType::List(field)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| DataFusionError::Internal("list".into()))?;
            let values = convert(list.values(), field.data_type())?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(field),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Struct(from), DataType::Struct(to)) if from.len() == to.len() => {
            let row = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Internal("row".into()))?;
            let columns = row
                .columns()
                .iter()
                .zip(to.iter())
                .map(|(c, f)| convert(c, f.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                to.clone(),
                columns,
                row.nulls().cloned(),
            )?))
        }
        (DataType::Map(..), DataType::Map(field, sorted)) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| DataFusionError::Internal("map".into()))?;
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let entries = convert(&entries, field.data_type())?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| DataFusionError::Internal("map entries".into()))?
                .clone();
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                entries,
                map.nulls().cloned(),
                *sorted,
            )?))
        }
        (from, to) if matches!(to, DataType::List(_) | DataType::Map(..) | DataType::Struct(_)) => {
            Err(invalid_cast(format!(
                "Cannot cast {} to {}",
                super::support::trino_type_name(from),
                super::support::trino_type_name(to)
            )))
        }
        _ => Ok(cast(array, target)?),
    }
}

fn target_type(r: &Ret<'_>) -> Result<DataType> {
    let text = r
        .literal_str(1)
        .ok_or_else(|| DataFusionError::Internal("cast type text".into()))?;
    parse_type(text, r.zone)
}

fn invoke(call: &Call<'_>, is_try: bool) -> Result<ArrayRef> {
    let input = call.arg(0)?;
    let target = call.return_type;
    if call.fields.first().is_some_and(|f| is_json_field(f)) {
        return super::json_cast::from_json_text(strings(input)?, target, call.rows, is_try);
    }
    match convert(input, target) {
        Err(error) if is_try && caught_by_try(&error) => {
            // Row by row, so only failing rows become NULL.
            let values = (0..call.rows)
                .map(|i| {
                    let one = input.slice(i, 1);
                    match convert(&one, target) {
                        Ok(v) => ScalarValue::try_from_array(&v, 0),
                        Err(e) if caught_by_try(&e) => ScalarValue::try_from(target),
                        Err(e) => Err(e),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            if values.is_empty() {
                return Ok(new_null_array(target, 0));
            }
            ScalarValue::iter_to_array(values)
        }
        other => other,
    }
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        Func::new(
            CAST_TYPE,
            |_, types| Ok(types.to_vec()),
            target_type,
            |call| invoke(call, false),
        )
        .zone_sensitive()
        .udf(),
        Func::new(
            TRY_CAST_TYPE,
            |_, types| Ok(types.to_vec()),
            target_type,
            |call| invoke(call, true),
        )
        .zone_sensitive()
        .udf(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bounded text inside a container is refused, never converted as
    /// unbounded text (Trino 431 pads or cuts each value).
    #[test]
    fn bounded_text_inside_containers_is_refused() {
        for text in [
            "array(char(3))",
            "array(char)",
            "array(varchar(1))",
            "row(a char(4), b integer)",
            "row(char(4))",
            "map(varchar, varchar(2))",
            "array(array(char(2)))",
        ] {
            let err = parse_type(text, "UTC").unwrap_err();
            assert!(matches!(err, DataFusionError::NotImplemented(_)), "{text}: {err}");
        }
        assert!(parse_type("array(varchar)", "UTC").is_ok());
        assert!(parse_type("row(a varchar, b array(varchar))", "UTC").is_ok());
    }

    #[test]
    fn trino_types_map_to_arrow() {
        assert_eq!(parse_type("integer", "UTC").unwrap(), DataType::Int32);
        assert_eq!(
            parse_type("decimal(18, 6)", "UTC").unwrap(),
            DataType::Decimal128(18, 6)
        );
        assert_eq!(parse_type("varchar(3)", "UTC").unwrap(), DataType::Utf8);
        assert_eq!(
            parse_type("timestamp(6) with time zone", "America/Sao_Paulo").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("America/Sao_Paulo")))
        );
        let DataType::List(inner) = parse_type("array(map(varchar, bigint))", "UTC").unwrap() else {
            panic!("list")
        };
        assert!(matches!(inner.data_type(), DataType::Map(..)));
        let DataType::Struct(fields) = parse_type("row(a integer, b array(varchar))", "UTC").unwrap() else {
            panic!("row")
        };
        assert_eq!(fields[0].name(), "a");
        assert_eq!(
            fields[1].data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
        );
        let DataType::Struct(anonymous) = parse_type("row(integer, double)", "UTC").unwrap() else {
            panic!("row")
        };
        assert_eq!(anonymous[1].name(), "c1");
        assert!(parse_type("array(nope)", "UTC").is_err());
    }
}
