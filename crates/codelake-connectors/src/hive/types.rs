//! Hive type names (as written by Trino's file metastore) ↔ Arrow types.
//!
//! Trino's Hive connector stores timestamps with millisecond precision by
//! default (`hive.timestamp-precision=MILLISECONDS`), `varchar` without length
//! as `string`, and `integer` as `int`.

use codelake_core::trino_types::{Declared, declared};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
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

/// A column read from a Hive type: `varchar(10)` and `char(3)` are text
/// that records its declared type (see [`codelake_core::trino_types::Declared`]).
pub(crate) fn hive_field(name: &str, hive: &str) -> Result<Field> {
    let field = Field::new(name, arrow_type(hive)?, true);
    Ok(match declared_text(hive) {
        Some(declared) => field.with_metadata(declared.metadata()),
        None => field,
    })
}

/// `varchar(n)` / `char(n)` of a type name.
pub(crate) fn declared_text(name: &str) -> Option<Declared> {
    Declared::parse(name).filter(|d| matches!(d, Declared::Varchar(_) | Declared::Char(_)))
}

/// Hive type name of a column, as Trino would record it: its declared
/// `varchar(n)` / `char(n)`, or its Arrow type's.
pub(crate) fn hive_field_type(field: &Field) -> Result<String> {
    match declared(field) {
        Some(Declared::Varchar(n)) => Ok(format!("varchar({n})")),
        Some(Declared::Char(n)) => Ok(format!("char({n})")),
        _ => hive_type(field.data_type()),
    }
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
    fn bounded_text_keeps_its_declared_type() {
        let field = hive_field("v10", "varchar(10)").unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(declared(&field), Some(Declared::Varchar(10)));
        assert_eq!(hive_field_type(&field).unwrap(), "varchar(10)");
        let field = hive_field("c3", "char(3)").unwrap();
        assert_eq!(hive_field_type(&field).unwrap(), "char(3)");
        let plain = hive_field("v", "string").unwrap();
        assert_eq!(declared(&plain), None);
        assert_eq!(hive_field_type(&plain).unwrap(), "string");
    }

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
