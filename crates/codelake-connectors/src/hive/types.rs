//! Hive type names (as written by Trino's file metastore) ↔ Arrow types.
//!
//! Trino's Hive connector stores timestamps with millisecond precision by
//! default (`hive.timestamp-precision=MILLISECONDS`), `varchar` without length
//! as `string`, and `integer` as `int`.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};

fn parse_decimal(args: &str) -> Option<DataType> {
    let (p, s) = args.split_once(',')?;
    Some(DataType::Decimal128(
        p.trim().parse().ok()?,
        s.trim().parse().ok()?,
    ))
}

/// Arrow type for a Hive type name.
pub(crate) fn arrow_type(hive: &str) -> Result<DataType> {
    let lower = hive.trim().to_ascii_lowercase();
    let unsupported = || DataFusionError::NotImplemented(format!("hive type `{hive}` is not supported"));
    let base = lower.split('(').next().unwrap_or_default();
    Ok(match base {
        "boolean" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "float" | "real" => DataType::Float32,
        "double" => DataType::Float64,
        "string" | "varchar" | "char" => DataType::Utf8,
        "binary" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "decimal" => lower
            .strip_prefix("decimal(")
            .and_then(|rest| rest.strip_suffix(')'))
            .and_then(parse_decimal)
            .unwrap_or(DataType::Decimal128(10, 0)),
        _ => return Err(unsupported()),
    })
}

/// Hive type name for an Arrow type, as Trino would record it.
pub(crate) fn hive_type(arrow: &DataType) -> Result<String> {
    Ok(match arrow {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::Date32 => "date".into(),
        DataType::Timestamp(_, None) => "timestamp".into(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "cannot store Arrow type {other} in a hive table"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_every_type_trino_writes() {
        let cases = [
            ("boolean", DataType::Boolean),
            ("tinyint", DataType::Int8),
            ("smallint", DataType::Int16),
            ("int", DataType::Int32),
            ("bigint", DataType::Int64),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
            ("string", DataType::Utf8),
            ("varchar(10)", DataType::Utf8),
            ("char(3)", DataType::Utf8),
            ("binary", DataType::Binary),
            ("date", DataType::Date32),
            ("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None)),
            ("decimal(18,2)", DataType::Decimal128(18, 2)),
            ("DECIMAL(38, 6)", DataType::Decimal128(38, 6)),
        ];
        for (hive, arrow) in cases {
            assert_eq!(arrow_type(hive).unwrap(), arrow, "{hive}");
        }
        assert!(arrow_type("array<int>").is_err());
        assert!(arrow_type("map<string,int>").is_err());
    }

    #[test]
    fn writes_types_trino_can_read_back() {
        for arrow in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Decimal128(18, 2),
        ] {
            let hive = hive_type(&arrow).unwrap();
            assert_eq!(arrow_type(&hive).unwrap(), arrow, "{hive}");
        }
        assert_eq!(hive_type(&DataType::Utf8View).unwrap(), "string");
        assert_eq!(
            hive_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "timestamp"
        );
        assert!(hive_type(&DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))).is_err());
        assert!(hive_type(&DataType::Float16).is_err());
    }
}
