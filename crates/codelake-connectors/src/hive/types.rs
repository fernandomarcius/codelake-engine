//! Hive type names (as written by Trino's file metastore) ↔ Arrow types.
//!
//! Trino's Hive connector stores timestamps at the catalog's precision
//! (`hive.timestamp-precision`, default `MILLISECONDS`), `varchar` without
//! length as `string`, and `integer` as `int`.
//!
//! What Arrow types do not say is kept in field metadata, per column:
//! - `varchar(n)` ([`HIVE_TYPE`]): reads cut values to `n` characters, as
//!   Trino's reader does; writes refuse longer values, as Trino's `INSERT`;
//! - `char(n)` ([`HIVE_TYPE`]): values are stored without trailing spaces and
//!   read padded to `n` characters, as Trino shows them (`'ab   '`, length
//!   5); writes refuse longer values. The engine has no char type, and Trino
//!   compares char values padded (`ch = 'ab'` is true): a filter on a char
//!   column is refused rather than compared as varchar (see `table.rs`);
//! - a type `CodeLake` cannot read yet ([`UNSUPPORTED`]: `array`, `map`,
//!   `struct`): the table stays listed and readable, and only a query that
//!   reads the column, or writes the table, fails. Never a wrong value.

use std::collections::HashMap;

use codelake_core::DECLARED_TYPE;
use codelake_core::config::HiveTimestampPrecision;
use codelake_core::trino_types::{Declared, declared};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::error::{DataFusionError, Result};

/// Field metadata: the stored hive type, when reads or writes depend on it
/// (`varchar(n)`, `char(n)`). It is the key the engine reads a column's
/// narrower type from ([`codelake_core::trino_types::declared`]), so the
/// client sees `varchar(10)` where the metastore says so.
pub(crate) const HIVE_TYPE: &str = codelake_core::trino_types::TRINO_TYPE;
/// Field metadata: why the column cannot be read or written.
pub(crate) const UNSUPPORTED: &str = "codelake.hive.unsupported";

fn parse_decimal(args: &str) -> Option<DataType> {
    let (p, s) = args.split_once(',')?;
    Some(DataType::Decimal128(
        p.trim().parse().ok()?,
        s.trim().parse().ok()?,
    ))
}

/// Arrow type for a Hive type name, timestamps at Trino's default precision.
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

/// Arrow unit of a catalog precision.
pub(crate) fn time_unit(precision: HiveTimestampPrecision) -> TimeUnit {
    match precision {
        HiveTimestampPrecision::Milliseconds => TimeUnit::Millisecond,
        HiveTimestampPrecision::Microseconds => TimeUnit::Microsecond,
        HiveTimestampPrecision::Nanoseconds => TimeUnit::Nanosecond,
    }
}

/// Fractional digits of an Arrow unit (Trino's `timestamp(p)`).
pub(crate) fn unit_digits(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// `n` of `varchar(n)`.
fn varchar_length(lower: &str) -> Option<usize> {
    lower
        .strip_prefix("varchar(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|n| n.trim().parse().ok())
}

/// `n` of `char(n)` (`char` alone is `char(1)`).
fn char_length(lower: &str) -> Option<usize> {
    if lower.trim() == "char" {
        return Some(1);
    }
    lower
        .strip_prefix("char(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|n| n.trim().parse().ok())
}

/// The Arrow field of a hive column, with the metadata reads and writes need
/// (see the module documentation). Never fails: a type `CodeLake` cannot
/// read yet becomes an [`UNSUPPORTED`] column.
pub(crate) fn column_field(name: &str, hive: &str, precision: HiveTimestampPrecision) -> Field {
    let lower = hive.trim().to_ascii_lowercase();
    let base = lower.split(['(', '<']).next().unwrap_or_default().trim();
    let marked = |data_type: DataType, key: &str, value: String| {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(key.to_owned(), value)]))
    };
    match base {
        "timestamp" => Field::new(name, DataType::Timestamp(time_unit(precision), None), true),
        "varchar" => match varchar_length(&lower) {
            Some(length) => marked(DataType::Utf8, HIVE_TYPE, format!("varchar({length})")),
            None => Field::new(name, DataType::Utf8, true),
        },
        "char" => match char_length(&lower) {
            Some(length) => marked(DataType::Utf8, HIVE_TYPE, format!("char({length})")),
            None => marked(
                DataType::Null,
                UNSUPPORTED,
                format!("column {name}: hive type `{hive}` is not supported"),
            ),
        },
        _ => match arrow_type(&lower) {
            Ok(data_type) => Field::new(name, data_type, true),
            Err(_) => marked(
                DataType::Null,
                UNSUPPORTED,
                format!("column {name}: hive type `{hive}` is not supported"),
            ),
        },
    }
}

/// Why `field` cannot be read or written, if it cannot.
pub(crate) fn unsupported(field: &Field) -> Option<&str> {
    field.metadata().get(UNSUPPORTED).map(String::as_str)
}

/// Declared length of a `varchar(n)` column.
pub(crate) fn declared_length(field: &Field) -> Option<usize> {
    field
        .metadata()
        .get(HIVE_TYPE)
        .and_then(|hive| varchar_length(hive))
}

/// Length of a `char(n)` column.
pub(crate) fn char_column(field: &Field) -> Option<usize> {
    field.metadata().get(HIVE_TYPE).and_then(|hive| char_length(hive))
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

/// Trino 431's refusal of a timestamp column whose precision is not the
/// catalog's (`NOT_SUPPORTED`).
fn incorrect_precision(digits: u8, precision: HiveTimestampPrecision, column: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "Incorrect timestamp precision for timestamp({digits}); the configured precision is {}; column name: {column}",
        precision.name()
    ))
}

/// How a new column is stored: its hive type, from the type the statement
/// declared ([`DECLARED_TYPE`]) when it declared one, else from its Arrow
/// type (CTAS).
///
/// `check_precision`: `CREATE TABLE` and CTAS refuse a timestamp whose
/// precision is not the catalog's, as Trino 431 does
/// (`HiveMetadata.validateTimestampColumns`); `ADD COLUMN` accepts any and
/// records `timestamp`, as Trino does.
pub(crate) fn stored_type(
    field: &Field,
    precision: HiveTimestampPrecision,
    check_precision: bool,
) -> Result<String> {
    let name = field.name();
    if let Some(declared) = field.metadata().get(DECLARED_TYPE) {
        let lower = declared.trim().to_ascii_lowercase();
        let base = lower.split('(').next().unwrap_or_default().trim();
        match base {
            "varchar" | "character varying" => {
                let length = lower
                    .split_once('(')
                    .and_then(|(_, rest)| rest.strip_suffix(')'))
                    .and_then(|n| n.trim().parse::<usize>().ok());
                return Ok(length.map_or_else(|| "string".to_owned(), |n| format!("varchar({n})")));
            }
            "char" | "character" => {
                let length = lower
                    .split_once('(')
                    .and_then(|(_, rest)| rest.strip_suffix(')'))
                    .map_or(Some(1), |n| n.trim().parse::<usize>().ok())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("invalid type {declared} for column {name}"))
                    })?;
                return Ok(format!("char({length})"));
            }
            "timestamp" if !lower.contains("with time zone") => {
                let digits = lower
                    .split_once('(')
                    .and_then(|(_, rest)| rest.split_once(')'))
                    .and_then(|(p, _)| p.trim().parse::<u8>().ok())
                    .unwrap_or(3);
                if check_precision && digits != precision.digits() {
                    return Err(incorrect_precision(digits, precision, name));
                }
                return Ok("timestamp".into());
            }
            _ => {}
        }
    }
    // A column of the query (`CREATE TABLE AS`) keeps its `varchar(n)` /
    // `char(n)`, as the reference engine does.
    match declared(field) {
        Some(Declared::Varchar(n)) => return Ok(format!("varchar({n})")),
        Some(Declared::Char(n)) => return Ok(format!("char({n})")),
        _ => {}
    }
    if let DataType::Timestamp(unit, None) = field.data_type() {
        let digits = unit_digits(*unit);
        if check_precision && digits != precision.digits() {
            return Err(incorrect_precision(digits, precision, name));
        }
    }
    hive_type(field.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_text_keeps_its_declared_type() {
        let ms = HiveTimestampPrecision::Milliseconds;
        let stored = |field: &Field| stored_type(field, ms, true).unwrap();
        let field = column_field("v10", "varchar(10)", ms);
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(
            codelake_core::trino_types::declared(&field),
            Some(Declared::Varchar(10))
        );
        assert_eq!(stored(&field), "varchar(10)");
        let field = column_field("c3", "char(3)", ms);
        assert_eq!(stored(&field), "char(3)");
        let plain = column_field("v", "string", ms);
        assert_eq!(codelake_core::trino_types::declared(&plain), None);
        assert_eq!(stored(&plain), "string");
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

    #[test]
    fn columns_carry_what_reads_and_writes_enforce() {
        let ms = HiveTimestampPrecision::Milliseconds;
        let ts = column_field("ts", "timestamp", HiveTimestampPrecision::Microseconds);
        assert_eq!(ts.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None));
        let bounded = column_field("c", "varchar(3)", ms);
        assert_eq!(
            (bounded.data_type(), declared_length(&bounded)),
            (&DataType::Utf8, Some(3))
        );
        assert_eq!(declared_length(&column_field("c", "string", ms)), None);
        for hive in ["array<int>", "map<string,int>", "struct<a:int>", "uniontype<int>"] {
            let field = column_field("x", hive, ms);
            assert!(unsupported(&field).is_some(), "{hive}");
        }
        assert!(unsupported(&column_field("x", "decimal(5,2)", ms)).is_none());
        let padded = column_field("ch", "char(5)", ms);
        assert_eq!((unsupported(&padded), char_column(&padded)), (None, Some(5)));
        assert_eq!(char_column(&column_field("ch", "char", ms)), Some(1));
        assert_eq!(char_column(&bounded), None);
    }

    fn declared(name: &str, data_type: DataType, sql: Option<&str>) -> Field {
        let field = Field::new(name, data_type, true);
        match sql {
            Some(sql) => field.with_metadata(HashMap::from([(DECLARED_TYPE.to_owned(), sql.to_owned())])),
            None => field,
        }
    }

    #[test]
    fn new_columns_are_stored_like_trino_431() {
        let ms = HiveTimestampPrecision::Milliseconds;
        let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let stored = |field: &Field, check| stored_type(field, ms, check);
        assert_eq!(
            stored(&declared("c", DataType::Utf8, Some("VARCHAR(3)")), true).unwrap(),
            "varchar(3)"
        );
        assert_eq!(
            stored(&declared("c", DataType::Utf8, Some("VARCHAR")), true).unwrap(),
            "string"
        );
        assert_eq!(
            stored(&declared("ts", ns.clone(), Some("TIMESTAMP")), true).unwrap(),
            "timestamp"
        );
        assert_eq!(
            stored(&declared("ts", ns.clone(), Some("TIMESTAMP(3)")), true).unwrap(),
            "timestamp"
        );
        assert_eq!(
            stored(&declared("ts", ns.clone(), Some("TIMESTAMP(6)")), true)
                .unwrap_err()
                .to_string(),
            "This feature is not implemented: Incorrect timestamp precision for timestamp(6); the configured \
             precision is MILLISECONDS; column name: ts"
        );
        // ADD COLUMN takes any precision, as Trino.
        assert_eq!(
            stored(&declared("ts", ns.clone(), Some("TIMESTAMP(6)")), false).unwrap(),
            "timestamp"
        );
        // CTAS: the precision of the query's type.
        let second = DataType::Timestamp(TimeUnit::Second, None);
        assert!(
            stored(&declared("ts", second, None), true)
                .unwrap_err()
                .to_string()
                .contains("timestamp(0)")
        );
        let millis = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert_eq!(stored(&declared("ts", millis, None), true).unwrap(), "timestamp");
        assert_eq!(
            stored(&declared("ch", DataType::Utf8, Some("CHAR(5)")), true).unwrap(),
            "char(5)"
        );
        assert_eq!(
            stored(&declared("ch", DataType::Utf8, Some("CHAR")), true).unwrap(),
            "char(1)"
        );
        assert_eq!(
            stored_type(
                &declared("ts", ns, None),
                HiveTimestampPrecision::Nanoseconds,
                true
            )
            .unwrap(),
            "timestamp"
        );
    }
}
