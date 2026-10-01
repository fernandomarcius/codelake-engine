//! Arrow types of PostgreSQL columns, and the form array columns travel in.
//!
//! A column that is not an array keeps the type the upstream provider gave
//! it (the rows are decoded by the upstream row decoder, which must agree), a
//! type it does not know is refused, as before. Arrays follow the reference
//! engine (its `array-mapping=AS_ARRAY`): an array of a type it maps is a
//! list of that type, nested once per declared dimension; any other array is
//! left out of the relation, as it leaves it out:
//!
//! | Elements | List of |
//! |---|---|
//! | `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean` | the same type |
//! | `numeric(p, s)` | `decimal(p, s)` (`numeric` without precision: left out) |
//! | `text`, `varchar`, `char`, `"char"`, `name`, `uuid`, `json`, `jsonb`, `money`, enums | text |
//! | `date`, `time`, `timestamp`, `timestamptz` | the same type |
//!
//! Left out (also by the reference engine): `bytea[]`, `interval[]`, network
//! addresses, geometric and range types, composites, `xml[]`. Left out here,
//! mapped there (ISS-060): `oid[]` (`bigint`), `timetz[]` and `bit[]`.
//!
//! The upstream row decoder reads arrays of numbers, booleans and `text`
//! only, and those without `NULL` elements (`error deserializing column`),
//! and fails on the others (`unsupported data type: _date`). [`readable`]
//! wraps a statement whose result has a list so that the source sends it as
//! JSON (`to_json`), which the decoder turns into the list the schema
//! declares.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};

/// Precision and scale of a `numeric` without them (the upstream provider's).
const UNBOUNDED_NUMERIC: (u8, i8) = (38, 20);

/// A column as `pg_catalog` describes it.
#[derive(Debug, Clone, Default)]
pub(super) struct Column {
    pub name: String,
    /// `format_type`: `numeric(10,2)`, `timestamp(3) with time zone`, `integer[]`.
    pub type_name: String,
    /// `typtype`: `b` base, `e` enum, `c` composite, `d` domain…
    pub kind: String,
    /// `typcategory`: `A` for arrays.
    pub category: String,
    /// An array's element type, as `format_type` names it with the column's modifier.
    pub element: Option<String>,
    pub element_kind: Option<String>,
    /// Declared dimensions of an array, at least 1.
    pub dimensions: i32,
    pub not_null: bool,
    /// A composite's fields: name and `format_type`.
    pub fields: Vec<(String, String)>,
}

/// The column's Arrow type; `Ok(None)` leaves it out of the relation,
/// `Err` names a type the engine cannot read.
pub(super) fn arrow_type(column: &Column) -> Result<Option<DataType>, String> {
    if column.category == "A" {
        return Ok(array_type(column));
    }
    match column.kind.as_str() {
        "e" => Ok(Some(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        ))),
        "c" => column
            .fields
            .iter()
            .map(|(name, type_name)| {
                scalar(type_name)
                    .map(|data_type| Field::new(name, data_type, true))
                    .ok_or_else(|| type_name.clone())
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|fields| Some(DataType::Struct(Fields::from(fields)))),
        _ => scalar(&column.type_name)
            .map(Some)
            .ok_or_else(|| column.type_name.clone()),
    }
}

/// `format_type`'s name without precision or length: `timestamp(3) with
/// time zone` is `timestamp with time zone`, `character varying(10)` is
/// `character varying`.
fn base_name(type_name: &str) -> Cow<'_, str> {
    match (type_name.find('('), type_name.find(')')) {
        (Some(open), Some(close)) if open < close => {
            Cow::Owned(format!("{}{}", &type_name[..open], &type_name[close + 1..]))
        }
        _ => Cow::Borrowed(type_name),
    }
}

/// `(p, s)` of `numeric(p,s)` / `numeric(p)`; `None` without precision.
fn numeric_precision(type_name: &str) -> Option<(u8, i8)> {
    let inner = type_name.split_once('(')?.1.split_once(')')?.0;
    let mut parts = inner.split(',').map(str::trim);
    let precision = parts.next()?.parse().ok()?;
    let scale = parts.next().map_or(Some(0), |s| s.parse().ok())?;
    Some((precision, scale))
}

/// Arrow type of a type that is not an array, enum nor composite.
fn scalar(type_name: &str) -> Option<DataType> {
    Some(match base_name(type_name).as_ref() {
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" | "money" => DataType::Int64,
        "oid" | "xid" | "regproc" => DataType::UInt32,
        "numeric" => {
            let (precision, scale) = numeric_precision(type_name).unwrap_or(UNBOUNDED_NUMERIC);
            DataType::Decimal128(precision, scale)
        }
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "\"char\"" => DataType::Int8,
        "character" | "character varying" | "text" | "bpchar" | "uuid" | "name" | "inet" | "cidr"
        | "macaddr" | "xml" | "json" | "aclitem" | "pg_node_tree" => DataType::Utf8,
        "bytea" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" | "bit" | "bit varying"
        | "geometry" | "geography" => DataType::Binary,
        "date" => DataType::Date32,
        "time without time zone" => DataType::Time64(TimeUnit::Nanosecond),
        "timestamp without time zone" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "timestamp with time zone" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        "boolean" => DataType::Boolean,
        "tsvector" | "tsquery" => DataType::LargeUtf8,
        "point" => DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
        "int4range" => DataType::Struct(Fields::from(vec![
            Field::new("lower", DataType::Int32, true),
            Field::new("upper", DataType::Int32, true),
        ])),
        name if name.starts_with("interval") => DataType::Interval(IntervalUnit::MonthDayNano),
        _ => return None,
    })
}

/// Arrow type of an array's elements, when the reference engine maps them.
fn element_type(type_name: &str, kind: Option<&str>) -> Option<DataType> {
    if kind == Some("e") {
        return Some(DataType::Utf8);
    }
    Some(match base_name(type_name).as_ref() {
        "numeric" => {
            let (precision, scale) = numeric_precision(type_name)?;
            DataType::Decimal128(precision, scale)
        }
        "character" | "character varying" | "text" | "bpchar" | "\"char\"" | "name" | "uuid" | "json"
        | "jsonb" | "money" => DataType::Utf8,
        name @ ("smallint"
        | "integer"
        | "bigint"
        | "real"
        | "double precision"
        | "boolean"
        | "date"
        | "time without time zone"
        | "timestamp without time zone"
        | "timestamp with time zone") => scalar(name)?,
        _ => return None,
    })
}

fn array_type(column: &Column) -> Option<DataType> {
    let mut data_type = element_type(column.element.as_deref()?, column.element_kind.as_deref())?;
    for _ in 0..column.dimensions.max(1) {
        data_type = DataType::List(Arc::new(Field::new("item", data_type, true)));
    }
    Some(data_type)
}

/// How column `column` of the wrapped statement is sent: as it is, or as
/// JSON when it is a list. Text elements are cast to `text` first, so JSON
/// holds them as strings (a `json[]` element stays its text).
fn sent_as(data_type: &DataType, column: &str) -> Option<String> {
    let DataType::List(item) = data_type else {
        return None;
    };
    let mut innermost = item.data_type();
    while let DataType::List(inner) = innermost {
        innermost = inner.data_type();
    }
    Some(if matches!(innermost, DataType::Utf8) {
        format!("to_json({column}::text[])")
    } else {
        format!("to_json({column})")
    })
}

fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `sql`, or, when its result (`projected`, by position) has a list, `sql`
/// under a select that sends each list as JSON and every column under its
/// projected name.
pub(super) fn readable<'a>(sql: &'a str, projected: Option<&Schema>) -> Cow<'a, str> {
    let Some(schema) = projected else {
        return Cow::Borrowed(sql);
    };
    let fields = schema.fields();
    if !fields.iter().any(|f| sent_as(f.data_type(), "c").is_some()) {
        return Cow::Borrowed(sql);
    }
    let mut select = String::from("SELECT ");
    let mut positions = String::new();
    for (index, field) in fields.iter().enumerate() {
        let column = format!("codelake_rows.c{}", index + 1);
        let value = sent_as(field.data_type(), &column).unwrap_or(column);
        if index > 0 {
            select.push_str(", ");
            positions.push_str(", ");
        }
        let _ = write!(select, "{value} AS {}", quoted(field.name()));
        let _ = write!(positions, "c{}", index + 1);
    }
    Cow::Owned(format!("{select} FROM ({sql}) AS codelake_rows({positions})"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(type_name: &str) -> Column {
        Column {
            name: "c".into(),
            type_name: type_name.into(),
            kind: "b".into(),
            dimensions: 1,
            ..Column::default()
        }
    }

    fn array(element: &str, dimensions: i32) -> Column {
        Column {
            name: "c".into(),
            type_name: format!("{element}[]"),
            kind: "b".into(),
            category: "A".into(),
            element: Some(element.into()),
            element_kind: Some("b".into()),
            dimensions,
            ..Column::default()
        }
    }

    fn list(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    #[test]
    fn scalars_keep_the_upstream_types() {
        let cases = [
            ("smallint", DataType::Int16),
            ("integer", DataType::Int32),
            ("bigint", DataType::Int64),
            ("money", DataType::Int64),
            ("numeric(10,2)", DataType::Decimal128(10, 2)),
            ("numeric(5)", DataType::Decimal128(5, 0)),
            ("numeric", DataType::Decimal128(38, 20)),
            ("real", DataType::Float32),
            ("double precision", DataType::Float64),
            ("character varying(10)", DataType::Utf8),
            ("character(3)", DataType::Utf8),
            ("text", DataType::Utf8),
            ("uuid", DataType::Utf8),
            ("bytea", DataType::Binary),
            ("date", DataType::Date32),
            (
                "time(3) without time zone",
                DataType::Time64(TimeUnit::Nanosecond),
            ),
            (
                "timestamp without time zone",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "timestamp(3) with time zone",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
            ("interval", DataType::Interval(IntervalUnit::MonthDayNano)),
            ("boolean", DataType::Boolean),
            ("tsvector", DataType::LargeUtf8),
            ("oid", DataType::UInt32),
            ("\"char\"", DataType::Int8),
            (
                "point",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
            ),
            (
                "int4range",
                DataType::Struct(Fields::from(vec![
                    Field::new("lower", DataType::Int32, true),
                    Field::new("upper", DataType::Int32, true),
                ])),
            ),
        ];
        for (type_name, expected) in cases {
            assert_eq!(arrow_type(&column(type_name)), Ok(Some(expected)), "{type_name}");
        }
        assert_eq!(arrow_type(&column("jsonb")), Err("jsonb".to_owned()));
        assert_eq!(
            arrow_type(&column("time with time zone")),
            Err("time with time zone".to_owned())
        );
    }

    #[test]
    fn enums_and_composites() {
        let mood = Column {
            kind: "e".into(),
            ..column("mood")
        };
        assert_eq!(
            arrow_type(&mood),
            Ok(Some(DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(DataType::Utf8)
            )))
        );
        let pair = Column {
            kind: "c".into(),
            fields: vec![("a".into(), "integer".into()), ("b".into(), "text".into())],
            ..column("pair")
        };
        assert_eq!(
            arrow_type(&pair),
            Ok(Some(DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]))))
        );
        let odd = Column {
            fields: vec![("a".into(), "jsonb".into())],
            ..pair
        };
        assert_eq!(arrow_type(&odd), Err("jsonb".to_owned()));
    }

    #[test]
    fn arrays_follow_the_reference_engine() {
        let cases = [
            ("date", DataType::Date32),
            (
                "timestamp(3) without time zone",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
            (
                "timestamp with time zone",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            ),
            ("time without time zone", DataType::Time64(TimeUnit::Nanosecond)),
            ("numeric(10,2)", DataType::Decimal128(10, 2)),
            ("text", DataType::Utf8),
            ("character varying(10)", DataType::Utf8),
            ("character(3)", DataType::Utf8),
            ("\"char\"", DataType::Utf8),
            ("name", DataType::Utf8),
            ("uuid", DataType::Utf8),
            ("json", DataType::Utf8),
            ("jsonb", DataType::Utf8),
            ("money", DataType::Utf8),
            ("smallint", DataType::Int16),
            ("integer", DataType::Int32),
            ("bigint", DataType::Int64),
            ("real", DataType::Float32),
            ("double precision", DataType::Float64),
            ("boolean", DataType::Boolean),
        ];
        for (element, item) in cases {
            assert_eq!(
                arrow_type(&array(element, 1)),
                Ok(Some(list(item))),
                "{element}[]"
            );
        }
        for element in [
            "numeric",
            "bytea",
            "interval",
            "inet",
            "oid",
            "xml",
            "point",
            "int4range",
        ] {
            assert_eq!(arrow_type(&array(element, 1)), Ok(None), "{element}[]");
        }
        let mood = Column {
            element_kind: Some("e".into()),
            ..array("mood", 1)
        };
        assert_eq!(arrow_type(&mood), Ok(Some(list(DataType::Utf8))));
        assert_eq!(
            arrow_type(&array("integer", 2)),
            Ok(Some(list(list(DataType::Int32))))
        );
        assert_eq!(arrow_type(&array("integer", 0)), Ok(Some(list(DataType::Int32))));
        let missing = Column {
            element: None,
            ..array("integer", 1)
        };
        assert_eq!(arrow_type(&missing), Ok(None));
    }

    #[test]
    fn lists_travel_as_json() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("label", DataType::Utf8, true),
        ]);
        let sql = "SELECT id, label FROM t";
        assert!(matches!(readable(sql, Some(&schema)), Cow::Borrowed(s) if s == sql));
        assert!(matches!(readable(sql, None), Cow::Borrowed(s) if s == sql));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("days", list(DataType::Date32), true),
            Field::new("tags \"x\"", list(DataType::Utf8), true),
            Field::new("grid", list(list(DataType::Int32)), true),
            Field::new("names", list(list(DataType::Utf8)), true),
            Field::new("amounts", list(DataType::Decimal128(10, 2)), true),
        ]);
        assert_eq!(
            readable("SELECT * FROM t", Some(&schema)),
            "SELECT codelake_rows.c1 AS \"id\", to_json(codelake_rows.c2) AS \"days\", \
             to_json(codelake_rows.c3::text[]) AS \"tags \"\"x\"\"\", to_json(codelake_rows.c4) AS \"grid\", \
             to_json(codelake_rows.c5::text[]) AS \"names\", to_json(codelake_rows.c6) AS \"amounts\" \
             FROM (SELECT * FROM t) AS codelake_rows(c1, c2, c3, c4, c5, c6)"
        );
    }

    #[test]
    fn names_without_precision() {
        assert_eq!(base_name("character varying(10)"), "character varying");
        assert_eq!(
            base_name("timestamp(3) with time zone"),
            "timestamp with time zone"
        );
        assert_eq!(base_name("text"), "text");
        assert_eq!(base_name("odd)("), "odd)(");
        assert_eq!(base_name("a)b(c"), "a)b(c");
        assert_eq!(numeric_precision("numeric(10, 2)"), Some((10, 2)));
        assert_eq!(numeric_precision("numeric(7)"), Some((7, 0)));
        assert_eq!(numeric_precision("numeric"), None);
        assert_eq!(numeric_precision("numeric(x)"), None);
    }
}
