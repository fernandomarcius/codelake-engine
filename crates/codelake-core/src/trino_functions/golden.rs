//! Golden tests: every case in `tests/fixtures/trino431-functions/*.json` was
//! run on a real Trino 431 (`scripts/golden/regenerate-functions.sh`); the
//! engine must return the same types, the same rows and, for failures, an
//! error carrying the same Trino error name. No Trino is needed to run them.
//!
//! Comparison rules:
//! - types use Trino's spelling; `varchar(n)` compares as `varchar` (Arrow has
//!   no length), `json` and `uuid` values are `varchar` here;
//! - doubles are equal bit for bit unless the case sets `ulps` (only
//!   approximations and transcendental functions do);
//! - values are compared in the JSON form of Trino's client protocol;
//! - an expected error compares Trino's `errorName` with
//!   [`super::trino_error_name`] of the engine's error;
//! - a case marked `"unsupported": "<reason>"` documents Trino syntax or a
//!   function the engine does not implement: the engine must fail loudly
//!   (any error), never return rows. A case that starts returning rows fails
//!   until the mark is removed and its rows are compared.

use std::collections::BTreeMap;
use std::path::PathBuf;

use base64::Engine as _;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, ListArray, MapArray, StringArray,
    StringViewArray, StructArray, Time32MillisecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use serde::Deserialize;
use serde_json::{Value, json};

use super::cast::render_timestamp;
use super::datetime::{column_zone, date_from_days, to_naive};
use super::support::{precision_of, timestamps};
use crate::bytes::ByteSize;
use crate::config::{ExecutionConfig, MemoryConfig};
use crate::engine::Engine;

#[derive(Debug, Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    sql: String,
    #[serde(default)]
    ulps: u64,
    #[serde(default)]
    unsupported: Option<String>,
    trino: Option<Expected>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Expected {
    Rows {
        types: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Error {
        error: String,
    },
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431-functions")
}

pub(crate) fn engine(dir: &std::path::Path) -> Engine {
    let memory = MemoryConfig {
        limit: ByteSize::mib(256),
        spill_dir: dir.to_owned(),
        spill_limit: ByteSize::mib(256),
        metadata_cache_limit: ByteSize::mib(1),
    };
    let execution = ExecutionConfig {
        target_partitions: Some(2),
        ..ExecutionConfig::default()
    };
    Engine::new(&memory, &execution).unwrap()
}

// ---------------------------------------------------------------------------
// Arrow → Trino client JSON
// ---------------------------------------------------------------------------

pub(crate) fn trino_type(t: &DataType) -> String {
    match t {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "varbinary".into(),
        DataType::Date32 => "date".into(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::Timestamp(unit, zone) => {
            let p = precision_of(*unit);
            if zone.is_some() {
                format!("timestamp({p}) with time zone")
            } else {
                format!("timestamp({p})")
            }
        }
        DataType::Time32(TimeUnit::Second) => "time(0)".into(),
        DataType::Time32(_) => "time(3)".into(),
        DataType::Time64(TimeUnit::Microsecond) => "time(6)".into(),
        DataType::Time64(_) => "time(9)".into(),
        DataType::List(f) | DataType::LargeList(f) => format!("array({})", trino_type(f.data_type())),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "map({},{})",
                trino_type(fields[0].data_type()),
                trino_type(fields[1].data_type())
            ),
            other => format!("map(?{other})"),
        },
        // Field names are not compared: see `strip_row_names`.
        DataType::Struct(fields) => format!(
            "row({})",
            fields
                .iter()
                .map(|f| trino_type(f.data_type()))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => format!("arrow:{other}"),
    }
}

/// Type words that start an anonymous `row` field (`row(integer, varchar)`).
const TYPE_WORDS: [&str; 20] = [
    "boolean",
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "real",
    "double",
    "decimal",
    "varchar",
    "char",
    "varbinary",
    "json",
    "date",
    "time",
    "timestamp",
    "interval",
    "array",
    "map",
    "row",
    "uuid",
];

/// Index of the `)` closing the `(` just before `text`.
fn closing_paren(text: &str) -> Option<usize> {
    let mut depth = 0_usize;
    for (i, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' if depth == 0 => return Some(i),
            ')' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// Top-level comma-separated items.
fn split_top_level(text: &str) -> Vec<&str> {
    let (mut items, mut depth, mut start) = (Vec::new(), 0_usize, 0);
    for (i, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                items.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    items.push(&text[start..]);
    items
}

/// `row(x integer, y varchar)` → `row(integer, varchar)`: Arrow struct field
/// names do not carry Trino's (anonymous rows get `c0`, `c1`, …).
fn strip_row_names(t: &str) -> String {
    let mut out = String::new();
    let mut rest = t;
    while let Some(pos) = rest.find("row(") {
        let inner_start = pos + "row(".len();
        let Some(len) = closing_paren(&rest[inner_start..]) else {
            break;
        };
        out.push_str(&rest[..inner_start]);
        let items: Vec<String> = split_top_level(&rest[inner_start..inner_start + len])
            .into_iter()
            .map(|item| {
                let item = item.trim();
                let first = item.split([' ', '(']).next().unwrap_or_default();
                let typed = if TYPE_WORDS.contains(&first) {
                    item
                } else {
                    item.split_once(' ').map_or(item, |(_, t)| t.trim())
                };
                strip_row_names(typed)
            })
            .collect();
        out.push_str(&items.join(","));
        out.push(')');
        rest = &rest[inner_start + len + 1..];
    }
    out.push_str(rest);
    out
}

/// Trino's type text without spaces, lengths and row field names:
/// `decimal(5, 1)` → `decimal(5,1)`, `varchar(3)` → `varchar`,
/// `json`/`uuid` → `varchar`.
fn normalize_type(t: &str) -> String {
    let compact: String = strip_row_names(t).chars().filter(|c| *c != ' ').collect();
    let mut out = String::new();
    let mut rest = compact.as_str();
    while let Some(pos) = rest.find("varchar(") {
        out.push_str(&rest[..pos]);
        out.push_str("varchar");
        let after = &rest[pos + "varchar(".len()..];
        rest = after.find(')').map_or("", |end| &after[end + 1..]);
    }
    out.push_str(rest);
    out.replace("json", "varchar")
        .replace("uuid", "varchar")
        .replace("timestampwithtimezone", "timestamp with time zone")
        .replace(")withtimezone", ") with time zone")
}

fn value_at(array: &ArrayRef, row: usize) -> Value {
    if array.is_null(row) {
        return Value::Null;
    }
    macro_rules! num {
        ($t:ty) => {
            array
                .as_any()
                .downcast_ref::<$t>()
                .map(|a| json!(a.value(row)))
        };
    }
    let value = match array.data_type() {
        DataType::Null => Some(Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| json!(a.value(row))),
        DataType::Int8 => num!(Int8Array),
        DataType::Int16 => num!(Int16Array),
        DataType::Int32 => num!(Int32Array),
        DataType::Int64 => num!(Int64Array),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            // Trino prints a real with its own shortest digits (1.1, not 1.100000023841858).
            .map(|a| float_json(a.value(row).to_string().parse().unwrap_or(f64::NAN))),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| float_json(a.value(row))),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| json!(a.value(row))),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| json!(a.value(row))),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|a| json!(a.value(row))),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            cast(array, &DataType::Binary).ok().and_then(|b| {
                b.as_any()
                    .downcast_ref::<BinaryArray>()
                    .map(|a| json!(base64::engine::general_purpose::STANDARD.encode(a.value(row))))
            })
        }
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .and_then(|a| date_from_days(a.value(row)))
            .map(|d| json!(d.format("%Y-%m-%d").to_string())),
        DataType::Decimal128(..) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|a| json!(a.value_as_string(row))),
        DataType::Timestamp(..) => Some(timestamp_json(array, row)),
        DataType::Time32(TimeUnit::Millisecond) => {
            array.as_any().downcast_ref::<Time32MillisecondArray>().map(|a| {
                let ms = a.value(row);
                json!(format!(
                    "{:02}:{:02}:{:02}.{:03}",
                    ms / 3_600_000,
                    ms / 60_000 % 60,
                    ms / 1_000 % 60,
                    ms % 1_000
                ))
            })
        }
        DataType::List(_) => array.as_any().downcast_ref::<ListArray>().map(|a| {
            let values = a.value(row);
            Value::Array((0..values.len()).map(|i| value_at(&values, i)).collect())
        }),
        DataType::Map(..) => array.as_any().downcast_ref::<MapArray>().map(|a| {
            let entries = a.value(row);
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut object = serde_json::Map::new();
            for i in 0..entries.len() {
                let key = match value_at(keys, i) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                object.insert(key, value_at(values, i));
            }
            Value::Object(object)
        }),
        DataType::Struct(_) => array
            .as_any()
            .downcast_ref::<StructArray>()
            .map(|a| Value::Array(a.columns().iter().map(|c| value_at(c, row)).collect())),
        _ => None,
    };
    value.unwrap_or_else(|| json!(format!("<unrenderable {}>", array.data_type())))
}

fn float_json(v: f64) -> Value {
    if v.is_nan() {
        json!("NaN")
    } else if v.is_infinite() {
        json!(if v > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        json!(v)
    }
}

fn timestamp_json(array: &ArrayRef, row: usize) -> Value {
    let Ok(ts) = timestamps(array) else {
        return Value::Null;
    };
    let Some(raw) = ts.at(row) else {
        return Value::Null;
    };
    let Some(utc) = to_naive(raw, ts.unit) else {
        return json!("<out of range>");
    };
    match column_zone(ts.zone.as_ref()) {
        Ok(Some(zone)) => json!(format!(
            "{} {}",
            render_timestamp(zone.to_local(utc), precision_of(ts.unit)),
            zone.name()
        )),
        _ => json!(render_timestamp(utc, precision_of(ts.unit))),
    }
}

fn ulps_apart(a: f64, b: f64) -> u64 {
    if a.partial_cmp(&b) == Some(std::cmp::Ordering::Equal) {
        return 0;
    }
    if a.is_sign_negative() != b.is_sign_negative() {
        return u64::MAX;
    }
    a.to_bits().abs_diff(b.to_bits())
}

fn same_value(expected: &Value, actual: &Value, ulps: u64) -> bool {
    match (expected, actual) {
        (Value::Number(e), Value::Number(a)) if e.is_f64() || a.is_f64() => match (e.as_f64(), a.as_f64()) {
            (Some(e), Some(a)) => ulps_apart(e, a) <= ulps,
            _ => false,
        },
        (Value::Array(e), Value::Array(a)) => {
            e.len() == a.len() && e.iter().zip(a).all(|(x, y)| same_value(x, y, ulps))
        }
        (Value::Object(e), Value::Object(a)) => {
            e.len() == a.len()
                && e.iter()
                    .all(|(k, v)| a.get(k).is_some_and(|w| same_value(v, w, ulps)))
        }
        _ => expected == actual,
    }
}

type Outcome = Result<(Vec<String>, Vec<Vec<Value>>), datafusion::error::DataFusionError>;

async fn run(engine: &Engine, sql: &str) -> Outcome {
    let frame = engine.sql(sql).await?;
    let types: Vec<String> = frame
        .schema()
        .fields()
        .iter()
        .map(|f| trino_type(f.data_type()))
        .collect();
    let batches = frame.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push(batch.columns().iter().map(|c| value_at(c, row)).collect());
        }
    }
    Ok((types, rows))
}

fn check(case: &Case, expected: &Expected, actual: Outcome) -> Option<String> {
    if let Some(reason) = &case.unsupported {
        return actual.ok().map(|(_, rows)| {
            format!("marked unsupported ({reason}) but returned rows {rows:?}: remove the mark")
        });
    }
    match (expected, actual) {
        (Expected::Error { error }, Err(actual)) => {
            let name = super::trino_error_name(&actual);
            (name != error.as_str()).then(|| format!("expected error {error}, got {name}: {actual}"))
        }
        (Expected::Error { error }, Ok((_, rows))) => {
            Some(format!("expected error {error}, got rows {rows:?}"))
        }
        (Expected::Rows { .. }, Err(message)) => Some(format!("unexpected error: {message}")),
        (Expected::Rows { types, rows }, Ok((actual_types, actual_rows))) => {
            let expected_types: Vec<String> = types.iter().map(|t| normalize_type(t)).collect();
            let actual_types: Vec<String> = actual_types.iter().map(|t| normalize_type(t)).collect();
            if expected_types != actual_types {
                return Some(format!(
                    "types: expected {expected_types:?}, got {actual_types:?}"
                ));
            }
            let same_rows = rows.len() == actual_rows.len()
                && rows.iter().zip(&actual_rows).all(|(e, a)| {
                    e.len() == a.len() && e.iter().zip(a).all(|(x, y)| same_value(x, y, case.ulps))
                });
            (!same_rows).then(|| format!("rows: expected {rows:?}, got {actual_rows:?}"))
        }
    }
}

#[tokio::test]
async fn functions_match_trino_431() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let mut files: Vec<PathBuf> = std::fs::read_dir(fixtures_dir())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no golden fixtures found");
    let mut failures = BTreeMap::new();
    let mut total = 0;
    for file in &files {
        let fixture: Fixture = serde_json::from_str(&std::fs::read_to_string(file).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        let stem = file.file_stem().unwrap().to_string_lossy().to_string();
        for case in &fixture.cases {
            total += 1;
            let key = format!("{stem}/{}", case.name);
            let Some(expected) = &case.trino else {
                failures.insert(key, "fixture has no Trino result: regenerate it".to_owned());
                continue;
            };
            let actual = run(&engine, &case.sql).await;
            if let Some(failure) = check(case, expected, actual) {
                failures.insert(key, format!("{}\n    sql: {}", failure, case.sql));
            }
        }
    }
    let report: Vec<String> = failures.iter().map(|(k, v)| format!("{k}: {v}")).collect();
    assert!(
        failures.is_empty(),
        "{} of {total} golden cases differ from Trino 431:\n{}",
        failures.len(),
        report.join("\n")
    );
}

#[test]
fn type_names_normalize() {
    assert_eq!(normalize_type("decimal(5, 1)"), "decimal(5,1)");
    assert_eq!(normalize_type("varchar(3)"), "varchar");
    assert_eq!(normalize_type("array(varchar(1))"), "array(varchar)");
    assert_eq!(
        normalize_type("timestamp(3) with time zone"),
        "timestamp(3) with time zone"
    );
    assert_eq!(normalize_type("json"), "varchar");
    assert_eq!(
        normalize_type("array(row(x integer, y varchar(1)))"),
        "array(row(integer,varchar))"
    );
    assert_eq!(
        normalize_type("row(integer, row(a double))"),
        "row(integer,row(double))"
    );
    assert_eq!(
        normalize_type("map(varchar(1), array(bigint))"),
        "map(varchar,array(bigint))"
    );
    assert_eq!(trino_type(&DataType::Decimal128(5, 1)), "decimal(5,1)");
    assert!(same_value(&json!(1.0), &json!(1.0), 0));
    assert!(!same_value(&json!(1.0), &json!(1.000_000_000_000_000_2), 0));
    assert!(same_value(&json!(1.0), &json!(1.000_000_000_000_000_2), 1));
}
