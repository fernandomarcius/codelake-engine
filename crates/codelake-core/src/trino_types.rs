//! Trino type names for Arrow types.
//!
//! [`display_name`] is Trino's `Type.getDisplayName()` for the Trino type an
//! Arrow type is sent as over the Trino protocol, the spelling of
//! `information_schema.columns.data_type` (observed on Trino 431):
//! `bigint`, `varchar`, `decimal(10,2)`, `timestamp(3)`,
//! `timestamp(3) with time zone`, `array(integer)`, `map(varchar, bigint)`,
//! `row(x integer, Y z varchar)`.

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};

/// Field metadata naming the Trino type of a field whose Arrow type holds
/// more than it: `varchar(5)`, `char(3)` (Arrow `Utf8`), `timestamp(1)`
/// (Arrow milliseconds, values rounded to one digit). See [`Declared`].
pub const TRINO_TYPE: &str = "codelake.trino_type";

/// A Trino type narrower than the Arrow type carrying it, recorded in the
/// field's metadata ([`TRINO_TYPE`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Declared {
    /// `varchar(n)`: at most `n` characters.
    Varchar(u32),
    /// `char(n)`: `n` characters, padded with spaces when shown; stored
    /// without the padding.
    Char(u32),
    /// `timestamp(p)` or `timestamp(p) with time zone` (the zone is in the
    /// Arrow type): values rounded to `p` fraction digits.
    Timestamp(u8),
}

/// Longest `varchar(n)` Trino accepts.
const MAX_VARCHAR: u32 = 2_147_483_646;

impl Declared {
    /// `varchar(10)`, `char(3)`, `timestamp(1)` (a `with time zone` suffix is
    /// ignored); `None` for anything else.
    pub fn parse(text: &str) -> Option<Self> {
        let lower = text.trim().to_ascii_lowercase();
        let lower = lower.strip_suffix(" with time zone").unwrap_or(&lower);
        let (name, rest) = lower.split_once('(')?;
        let argument: u32 = rest.strip_suffix(')')?.trim().parse().ok()?;
        match name.trim() {
            "varchar" if argument <= MAX_VARCHAR => Some(Self::Varchar(argument)),
            "char" if (1..=65_536).contains(&argument) => Some(Self::Char(argument)),
            "timestamp" if argument <= 12 => Some(Self::Timestamp(u8::try_from(argument).ok()?)),
            _ => None,
        }
    }

    /// Whether values of `data_type` can carry this type.
    pub fn fits(self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (Self::Varchar(_) | Self::Char(_), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
                true
            }
            (Self::Timestamp(p), DataType::Timestamp(unit, _)) => p <= digits(*unit),
            _ => false,
        }
    }

    /// Trino's name of this type for a field of `data_type`.
    pub fn name(self, data_type: &DataType) -> String {
        match self {
            Self::Varchar(n) => format!("varchar({n})"),
            Self::Char(n) => format!("char({n})"),
            Self::Timestamp(p) if matches!(data_type, DataType::Timestamp(_, Some(_))) => {
                format!("timestamp({p}) with time zone")
            }
            Self::Timestamp(p) => format!("timestamp({p})"),
        }
    }

    /// The field metadata recording it.
    pub fn metadata(self) -> HashMap<String, String> {
        let text = match self {
            Self::Varchar(n) => format!("varchar({n})"),
            Self::Char(n) => format!("char({n})"),
            Self::Timestamp(p) => format!("timestamp({p})"),
        };
        HashMap::from([(TRINO_TYPE.to_owned(), text)])
    }
}

/// The Trino type `field` declares in its metadata, when its Arrow type can
/// carry it.
pub fn declared(field: &Field) -> Option<Declared> {
    field
        .metadata()
        .get(TRINO_TYPE)
        .and_then(|text| Declared::parse(text))
        .filter(|declared| declared.fits(field.data_type()))
}

/// [`display_name`] of a field, honouring the type it declares
/// ([`declared`]).
pub fn field_display_name(field: &Field) -> String {
    match declared(field) {
        Some(declared) => declared.name(field.data_type()),
        None => display_name(field.data_type()),
    }
}

fn digits(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// `decimal(p,s)` for Arrow's `(precision, scale)`: a negative scale becomes
/// an integral decimal holding the whole value, a scale above the precision
/// widens it (as the Trino protocol encoder sends them).
fn decimal(precision: u8, scale: i8) -> String {
    let (p, s) = if scale < 0 {
        (u16::from(precision) + u16::from(scale.unsigned_abs()), 0)
    } else {
        let s = u16::from(scale.unsigned_abs());
        (u16::from(precision).max(s), s)
    };
    format!("decimal({p},{s})")
}

/// Trino's display name of the type `data_type` is sent as.
pub fn display_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 | DataType::UInt8 => "smallint".into(),
        DataType::Int32 | DataType::UInt16 => "integer".into(),
        DataType::Int64 | DataType::UInt32 => "bigint".into(),
        DataType::UInt64 => decimal(20, 0),
        DataType::Float16 | DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => decimal(*p, *s),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            "varbinary".into()
        }
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(unit, None) => format!("timestamp({})", digits(*unit)),
        DataType::Timestamp(unit, Some(_)) => format!("timestamp({}) with time zone", digits(*unit)),
        DataType::Time32(unit) | DataType::Time64(unit) => format!("time({})", digits(*unit)),
        DataType::Interval(IntervalUnit::YearMonth) => "interval year to month".into(),
        DataType::Interval(IntervalUnit::DayTime | IntervalUnit::MonthDayNano) | DataType::Duration(_) => {
            "interval day to second".into()
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("array({})", display_name(field.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "map({}, {})",
                display_name(fields[0].data_type()),
                display_name(fields[1].data_type())
            ),
            other => format!("map({})", display_name(other)),
        },
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), display_name(f.data_type())))
                .collect();
            format!("row({})", fields.join(", "))
        }
        DataType::Dictionary(_, values) => display_name(values),
        // No Trino type: the Arrow name, never a Trino name it is not.
        other => other.to_string().to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Fields};
    use std::sync::Arc;

    /// `information_schema.columns.data_type` on Trino 431 for a memory table
    /// with every type `CodeLake` produces.
    #[test]
    fn names_match_trino_information_schema() {
        let row = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("Y z", DataType::Utf8, true),
        ]));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        );
        let cases = [
            (DataType::Int64, "bigint"),
            (DataType::Int32, "integer"),
            (DataType::Int16, "smallint"),
            (DataType::Int8, "tinyint"),
            (DataType::Boolean, "boolean"),
            (DataType::Float32, "real"),
            (DataType::Float64, "double"),
            (DataType::Decimal128(10, 2), "decimal(10,2)"),
            (DataType::Decimal128(38, 0), "decimal(38,0)"),
            (DataType::Utf8, "varchar"),
            (DataType::Utf8View, "varchar"),
            (DataType::Date32, "date"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), "timestamp(3)"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), "timestamp(6)"),
            (
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                "timestamp(3) with time zone",
            ),
            (DataType::Binary, "varbinary"),
            (DataType::new_list(DataType::Int32, true), "array(integer)"),
            (map, "map(varchar, bigint)"),
            (row, "row(x integer, Y z varchar)"),
            (DataType::Time64(TimeUnit::Microsecond), "time(6)"),
            (
                DataType::Interval(IntervalUnit::MonthDayNano),
                "interval day to second",
            ),
            (
                DataType::Interval(IntervalUnit::YearMonth),
                "interval year to month",
            ),
        ];
        for (data_type, expected) in cases {
            assert_eq!(display_name(&data_type), expected, "{data_type}");
        }
    }
}
