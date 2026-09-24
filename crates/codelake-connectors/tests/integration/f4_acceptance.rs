//! F4 acceptance suite, written by an independent tester: black-box checks of
//! the Trino 431 function library (ADR 0008) through the public `Engine` API.
//!
//! Wherever the answer is Trino's semantics, the suite asks a real
//! `trinodb/trino:431` started for the test (`codelake-f4-acc-*`) instead of
//! trusting the recorded golden fixtures: every case runs on both engines and
//! the column names, types and rows (or the error name) must be the same.
//! Cases run on two physical shapes (big batches over 4 partitions, 3-row
//! batches over 3 partitions), and then all at once on one engine, because a
//! function must not depend on batching or on concurrent queries.
//!
//! The engine-only tests (memory pool, failure in the middle of a stream,
//! cancellation, concurrency) check values that follow from the data itself.
//!
//! Tests marked `DEFECT-<n>` expose defects found by this suite; their
//! expected values were observed on `trinodb/trino:431` (the same statements
//! run by the differential tests). They are ignored only on this branch, until
//! the fix removes the mark.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::trino_functions::trino_error_name;
use codelake_core::{ByteSize, Engine};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Field, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt32Type, UInt64Type,
};
use datafusion::error::DataFusionError;
use futures::StreamExt;
use serde_json::{Value, json};
use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

// ---------------------------------------------------------------------------
// CodeLake side: engines and Arrow → Trino client-protocol JSON
// ---------------------------------------------------------------------------

/// Engine with `limit` of query memory. `batch_size`/`partitions` vary the
/// physical shape: a function must answer the same whatever the batching.
fn engine_with(spill: &Path, limit: ByteSize, batch_size: usize, partitions: usize) -> Engine {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let execution = ExecutionConfig {
        batch_size,
        target_partitions: Some(partitions),
        ..ExecutionConfig::default()
    };
    Engine::new(&memory, &execution).expect("engine")
}

/// One answer in Trino's client-protocol shape.
#[derive(Debug, Clone, PartialEq)]
enum Answer {
    Rows {
        columns: Vec<(String, String)>,
        rows: Vec<Vec<Value>>,
    },
    Error {
        name: String,
        message: String,
    },
}

fn precision(unit: TimeUnit) -> usize {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// Trino's spelling of an Arrow type.
fn trino_type(field: &Field) -> String {
    if let Some(t) = field.metadata().get("codelake.trino_type") {
        return t.clone();
    }
    type_name(field.data_type())
}

fn type_name(t: &DataType) -> String {
    match t {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => "bigint".into(),
        DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "varbinary".into(),
        DataType::Date32 => "date".into(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::Timestamp(unit, zone) => {
            let p = precision(*unit);
            if zone.is_some() {
                format!("timestamp({p}) with time zone")
            } else {
                format!("timestamp({p})")
            }
        }
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("array({})", trino_type(f))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                format!("map({},{})", trino_type(&fields[0]), trino_type(&fields[1]))
            }
            other => format!("map(?{other})"),
        },
        DataType::Struct(fields) => format!(
            "row({})",
            fields.iter().map(|f| trino_type(f)).collect::<Vec<_>>().join(",")
        ),
        other => format!("arrow:{other}"),
    }
}

/// Canonical type text: row field names dropped (Arrow names anonymous
/// fields `c0`…), no blanks, `varchar(n)` → `varchar` (Arrow strings have no
/// length: documented bound).
fn normalize_type(t: &str) -> String {
    let unnamed = strip_row_names(&t.to_lowercase());
    let compact: String = unnamed.chars().filter(|c| !c.is_whitespace()).collect();
    let mut out = String::new();
    let mut rest = compact.as_str();
    while let Some(pos) = rest.find("varchar(") {
        out.push_str(&rest[..pos + "varchar".len()]);
        let after = &rest[pos + "varchar(".len()..];
        rest = after.find(')').map_or("", |close| &after[close + 1..]);
    }
    out.push_str(rest);
    out
}

const TYPE_WORDS: [&str; 17] = [
    "boolean",
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "real",
    "double",
    "decimal",
    "varchar",
    "varbinary",
    "json",
    "date",
    "timestamp",
    "array",
    "map",
    "row",
    "unknown",
];

fn is_type(text: &str) -> bool {
    TYPE_WORDS.iter().any(|w| {
        text.strip_prefix(w)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('(') || rest.starts_with(' '))
    })
}

/// Splits `text` at top-level commas.
fn split_top(text: &str) -> Vec<&str> {
    let (mut items, mut depth, mut start) = (Vec::new(), 0_i32, 0);
    for (i, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
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

/// `row(a integer, b array(row(c varchar)))` → `row(integer, array(row(varchar)))`.
fn strip_row_names(t: &str) -> String {
    let t = t.trim();
    if let Some(inner) = t.strip_prefix("row(").and_then(|r| r.strip_suffix(')')) {
        let fields: Vec<String> = split_top(inner)
            .into_iter()
            .map(|item| {
                let item = item.trim();
                let typed = if is_type(item) {
                    item
                } else {
                    item.split_once(' ').map_or(item, |(_, ty)| ty.trim())
                };
                strip_row_names(typed)
            })
            .collect();
        return format!("row({})", fields.join(","));
    }
    for prefix in ["array(", "map("] {
        if let Some(inner) = t.strip_prefix(prefix).and_then(|r| r.strip_suffix(')')) {
            let items: Vec<String> = split_top(inner).into_iter().map(strip_row_names).collect();
            return format!("{prefix}{})", items.join(","));
        }
    }
    t.to_owned()
}

fn render_float(v: f64) -> Value {
    if v.is_nan() {
        json!("NaN")
    } else if v.is_infinite() {
        json!(if v > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        json!(v)
    }
}

fn render_decimal(v: i128, scale: i8) -> String {
    let negative = v < 0;
    let digits = v.unsigned_abs().to_string();
    let scale = usize::try_from(scale).unwrap_or(0);
    let text = if scale == 0 {
        digits
    } else {
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (int, frac) = padded.split_at(padded.len() - scale);
        format!("{int}.{frac}")
    };
    if negative { format!("-{text}") } else { text }
}

fn timestamp_text(array: &ArrayRef, i: usize, unit: TimeUnit, zone: Option<&str>) -> Value {
    let p = precision(unit);
    let fmt = match p {
        0 => "%Y-%m-%d %H:%M:%S".to_owned(),
        _ => format!("%Y-%m-%d %H:%M:%S%.{p}f"),
    };
    macro_rules! render {
        ($t:ty) => {{
            let a = array.as_primitive::<$t>();
            match zone {
                None => a.value_as_datetime(i).map(|d| d.format(&fmt).to_string()),
                Some(z) => {
                    let tz: datafusion::arrow::array::timezone::Tz = z.parse().expect("zone");
                    a.value_as_datetime_with_tz(i, tz)
                        .map(|d| format!("{} {z}", d.format(&fmt)))
                }
            }
        }};
    }
    let text = match unit {
        TimeUnit::Second => render!(TimestampSecondType),
        TimeUnit::Millisecond => render!(TimestampMillisecondType),
        TimeUnit::Microsecond => render!(TimestampMicrosecondType),
        TimeUnit::Nanosecond => render!(TimestampNanosecondType),
    };
    text.map_or(Value::Null, Value::String)
}

/// Value `i` of `array` in Trino's client-protocol JSON.
fn cell(array: &ArrayRef, i: usize) -> Value {
    if array.is_null(i) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => json!(array.as_boolean().value(i)),
        DataType::Int8 => json!(array.as_primitive::<Int8Type>().value(i)),
        DataType::Int16 => json!(array.as_primitive::<Int16Type>().value(i)),
        DataType::Int32 => json!(array.as_primitive::<Int32Type>().value(i)),
        DataType::Int64 => json!(array.as_primitive::<Int64Type>().value(i)),
        DataType::UInt32 => json!(array.as_primitive::<UInt32Type>().value(i)),
        DataType::UInt64 => json!(array.as_primitive::<UInt64Type>().value(i)),
        DataType::Float32 => {
            let v = array.as_primitive::<Float32Type>().value(i);
            if v.is_finite() {
                // Trino prints a real with Float.toString: the shortest text.
                json!(v.to_string().parse::<f64>().unwrap_or(f64::NAN))
            } else {
                render_float(f64::from(v))
            }
        }
        DataType::Float64 => render_float(array.as_primitive::<Float64Type>().value(i)),
        DataType::Decimal128(_, s) => json!(render_decimal(
            array.as_primitive::<Decimal128Type>().value(i),
            *s
        )),
        DataType::Utf8 => json!(array.as_string::<i32>().value(i)),
        DataType::LargeUtf8 => json!(array.as_string::<i64>().value(i)),
        DataType::Utf8View => json!(array.as_string_view().value(i)),
        DataType::Binary => {
            json!(base64::engine::general_purpose::STANDARD.encode(array.as_binary::<i32>().value(i)))
        }
        DataType::LargeBinary => {
            json!(base64::engine::general_purpose::STANDARD.encode(array.as_binary::<i64>().value(i)))
        }
        DataType::BinaryView => {
            json!(base64::engine::general_purpose::STANDARD.encode(array.as_binary_view().value(i)))
        }
        DataType::Date32 => {
            let d = array.as_primitive::<Date32Type>().value_as_date(i);
            d.map_or(Value::Null, |d| json!(d.format("%Y-%m-%d").to_string()))
        }
        DataType::Timestamp(unit, zone) => timestamp_text(array, i, *unit, zone.as_deref()),
        DataType::List(_) => {
            let list = array.as_list::<i32>().value(i);
            Value::Array((0..list.len()).map(|j| cell(&list, j)).collect())
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>().value(i);
            Value::Array((0..list.len()).map(|j| cell(&list, j)).collect())
        }
        DataType::Map(_, _) => {
            let entries = array.as_map().value(i);
            let keys = entries.column(0);
            let values = entries.column(1);
            let mut object = serde_json::Map::new();
            for j in 0..entries.len() {
                let key = match cell(keys, j) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                object.insert(key, cell(values, j));
            }
            Value::Object(object)
        }
        DataType::Struct(_) => {
            let s = array.as_struct();
            Value::Array(s.columns().iter().map(|c| cell(c, i)).collect())
        }
        other => json!(format!("<arrow {other}>")),
    }
}

/// Runs `sql` on `engine` and renders the answer like Trino's client.
async fn codelake(engine: &Engine, sql: &str) -> Answer {
    let frame = match engine.sql(sql).await {
        Ok(frame) => frame,
        Err(e) => {
            return Answer::Error {
                name: trino_error_name(&e).to_owned(),
                message: e.to_string(),
            };
        }
    };
    let schema = frame.schema().as_arrow().clone();
    let batches: Vec<RecordBatch> = match frame.collect().await {
        Ok(b) => b,
        Err(e) => {
            return Answer::Error {
                name: trino_error_name(&e).to_owned(),
                message: e.to_string(),
            };
        }
    };
    let columns = schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), trino_type(f)))
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        for i in 0..batch.num_rows() {
            rows.push(batch.columns().iter().map(|c| cell(c, i)).collect());
        }
    }
    Answer::Rows { columns, rows }
}

/// Error name of an engine error, as the Trino protocol would report it.
fn error_name(error: &DataFusionError) -> String {
    trino_error_name(error).to_owned()
}

// ---------------------------------------------------------------------------
// Comparison with Trino's answer
// ---------------------------------------------------------------------------

/// True when the statement's result order is defined: its last `ORDER BY`
/// is at the top level (not inside a window or an aggregate call).
fn ordered(sql: &str) -> bool {
    sql.rfind("ORDER BY").is_some_and(|at| !sql[at..].contains(')'))
}

/// `a` equals `b`; numbers within `tolerance` (relative) when it is not zero.
fn same_value(a: &Value, b: &Value, tolerance: f64) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let exact = tolerance <= 0.0;
            if let (Some(x), Some(y)) = (x.as_i64(), y.as_i64())
                && exact
            {
                return x == y;
            }
            let (x, y) = (x.as_f64().unwrap_or(f64::NAN), y.as_f64().unwrap_or(f64::NAN));
            if exact {
                within_ulps(x, y)
            } else {
                (x - y).abs() <= tolerance * x.abs().max(y.abs())
            }
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same_value(x, y, tolerance))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| same_value(v, w, tolerance)))
        }
        _ => a == b,
    }
}

/// Doubles equal up to `MAX_ULPS` units in the last place: ADR 0008 bounds
/// libm against Java's `StrictMath` (and accumulation order) to a few ulps.
const MAX_ULPS: u64 = 16;

fn within_ulps(x: f64, y: f64) -> bool {
    if x.to_bits() == y.to_bits() {
        return true;
    }
    if !x.is_finite() || !y.is_finite() || x.is_sign_negative() != y.is_sign_negative() {
        return false;
    }
    x.to_bits().abs_diff(y.to_bits()) <= MAX_ULPS
}

fn same_rows(expected: &[Vec<Value>], actual: &[Vec<Value>], in_order: bool, tolerance: f64) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let row = |r: &Vec<Value>| Value::Array(r.clone());
    if in_order {
        return expected
            .iter()
            .zip(actual)
            .all(|(e, a)| same_value(&row(e), &row(a), tolerance));
    }
    let key = |r: &&Vec<Value>| r.iter().map(Value::to_string).collect::<Vec<_>>().join("\u{1}");
    let mut e: Vec<&Vec<Value>> = expected.iter().collect();
    let mut a: Vec<&Vec<Value>> = actual.iter().collect();
    e.sort_by_key(key);
    a.sort_by_key(key);
    e.iter()
        .zip(&a)
        .all(|(e, a)| same_value(&row(e), &row(a), tolerance))
}

/// `None` when `actual` is Trino's `expected` answer to `sql`.
fn difference(sql: &str, expected: &Answer, actual: &Answer, tolerance: f64) -> Option<String> {
    match (expected, actual) {
        (Answer::Error { name: e, .. }, Answer::Error { name: a, .. }) => {
            (e != a).then(|| format!("error name: Trino {e}, CodeLake {a}"))
        }
        (Answer::Error { name, message }, Answer::Rows { rows, .. }) => Some(format!(
            "Trino fails with {name} ({message}), CodeLake returns {}",
            truncate(&json!(rows).to_string())
        )),
        (Answer::Rows { rows, .. }, Answer::Error { name, message }) => Some(format!(
            "CodeLake fails with {name} ({}), Trino returns {}",
            truncate(message),
            truncate(&json!(rows).to_string())
        )),
        (
            Answer::Rows {
                columns: ec,
                rows: er,
            },
            Answer::Rows {
                columns: ac,
                rows: ar,
            },
        ) => {
            let mut problems = Vec::new();
            let names = |c: &[(String, String)]| c.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>();
            let types = |c: &[(String, String)]| c.iter().map(|(_, t)| normalize_type(t)).collect::<Vec<_>>();
            if names(ec) != names(ac) {
                problems.push(format!("names: Trino {:?}, CodeLake {:?}", names(ec), names(ac)));
            }
            if types(ec) != types(ac) {
                problems.push(format!("types: Trino {:?}, CodeLake {:?}", types(ec), types(ac)));
            }
            if !same_rows(er, ar, ordered(sql), tolerance) {
                problems.push(format!(
                    "rows: Trino {}, CodeLake {}",
                    truncate(&json!(er).to_string()),
                    truncate(&json!(ar).to_string())
                ));
            }
            (!problems.is_empty()).then(|| problems.join("; "))
        }
    }
}

fn truncate(text: &str) -> String {
    text.chars().take(400).collect()
}

// ---------------------------------------------------------------------------
// Trino 431 side
// ---------------------------------------------------------------------------

/// Heap capped: the image default (80% of the container) gets the Docker VM
/// OOM-killed when several suites run in parallel.
const JVM: &str = "-server\n-Xmx768M\n-XX:G1HeapRegionSize=32M\n-XX:+ExplicitGCInvokesConcurrent\n\
                   -XX:+ExitOnOutOfMemoryError\n-XX:-OmitStackTraceInFastThrow\n\
                   -XX:ReservedCodeCacheSize=128M\n-Djdk.attach.allowAttachSelf=true\n\
                   -Djdk.nio.maxCachedBufferSize=2000000\n";

/// Runs the statements of a JSON file through Trino's REST protocol (session
/// zone UTC, parametric datetime types) and prints one JSON line per
/// statement: `{"columns": [[name, type]...], "rows": [...]}` or
/// `{"error": errorName, "message": ...}`.
const CLIENT: &str = r#"
import json, sys, time, urllib.request

HEADERS = {"X-Trino-User": "f4", "Content-Type": "text/plain",
           "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME"}

def fetch(request):
    return json.load(urllib.request.urlopen(request, timeout=300))

def run_once(sql):
    body = fetch(urllib.request.Request("http://localhost:8080/v1/statement", data=sql.encode(), headers=HEADERS, method="POST"))
    columns, rows = None, []
    while True:
        if body.get("columns") and columns is None:
            columns = [[c["name"], c["type"]] for c in body["columns"]]
        rows.extend(body.get("data") or [])
        if "error" in body:
            return {"error": body["error"]["errorName"], "message": body["error"]["message"]}
        if not body.get("nextUri"):
            return {"columns": columns or [], "rows": rows}
        body = fetch(urllib.request.Request(body["nextUri"], headers=HEADERS))

def run(sql):
    # Right after startup the coordinator may not list itself as a worker yet.
    for _ in range(60):
        answer = run_once(sql)
        if answer.get("message") != "No nodes available to run query":
            return answer
        time.sleep(1)
    return answer

for statement in json.load(open(sys.argv[1])):
    print(json.dumps(run(statement)), flush=True)
"#;

struct Trino {
    container: ContainerAsync<GenericImage>,
}

impl Trino {
    /// Starts Trino with each statement list copied in as `/tmp/<name>.json`
    /// (the container API has no copy after start).
    async fn start(lists: &[(&str, &[&str])]) -> Self {
        let mut image = GenericImage::new("trinodb/trino", "431")
            .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
            .with_container_name(format!("codelake-f4-acc-trino-{}", uuid::Uuid::new_v4()))
            .with_user("root")
            .with_startup_timeout(Duration::from_secs(240))
            .with_host_config_modifier(|config| config.memory = Some(3 << 29))
            .with_copy_to("/etc/trino/jvm.config", JVM.as_bytes().to_vec())
            .with_copy_to("/tmp/client.py", CLIENT.as_bytes().to_vec())
            .with_copy_to("/tmp/ready.json", br#"["SELECT 1"]"#.to_vec());
        for (name, statements) in lists {
            image = image.with_copy_to(
                format!("/tmp/{name}.json"),
                json!(statements).to_string().into_bytes(),
            );
        }
        let container = image.start().await.expect("trino 431 starts");
        let trino = Self { container };
        let ready = trino.run("ready", 1).await;
        assert!(
            matches!(ready.first(), Some(Answer::Rows { .. })),
            "trino answers: {ready:?}"
        );
        trino
    }

    async fn exec(&self, command: Vec<String>) -> String {
        let mut result = self
            .container
            .exec(ExecCommand::new(command))
            .await
            .expect("exec in trino container");
        let stdout = String::from_utf8_lossy(&result.stdout_to_vec().await.expect("stdout")).into_owned();
        let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.expect("stderr")).into_owned();
        let mut code = None;
        for _ in 0..200 {
            code = result.exit_code().await.expect("exit code");
            if code.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(code, Some(0), "trino client failed: {stderr}");
        stdout
    }

    /// Trino's answers to the statements of list `name`, in order.
    async fn run(&self, name: &str, count: usize) -> Vec<Answer> {
        let file = format!("/tmp/{name}.json");
        let stdout = self
            .exec(vec!["python3".into(), "/tmp/client.py".into(), file])
            .await;
        let answers: Vec<Answer> = stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                let value: Value = serde_json::from_str(line).expect("client line is JSON");
                if let Some(Value::String(name)) = value.get("error") {
                    Answer::Error {
                        name: name.clone(),
                        message: value["message"].as_str().unwrap_or_default().to_owned(),
                    }
                } else {
                    let columns = value["columns"]
                        .as_array()
                        .expect("columns")
                        .iter()
                        .map(|c| {
                            (
                                c[0].as_str().unwrap().to_owned(),
                                c[1].as_str().unwrap().to_owned(),
                            )
                        })
                        .collect();
                    let rows = value["rows"]
                        .as_array()
                        .expect("rows")
                        .iter()
                        .map(|r| r.as_array().expect("row").clone())
                        .collect();
                    Answer::Rows { columns, rows }
                }
            })
            .collect();
        assert_eq!(answers.len(), count, "one answer per statement: {stdout}");
        answers
    }
}

// ---------------------------------------------------------------------------
// Differential tests against a real Trino 431
// ---------------------------------------------------------------------------

/// Relative tolerance for approximate aggregates. Trino's `approx_distinct`
/// has a standard error of 2.3%, so two correct answers can differ by more.
const APPROX_TOLERANCE: f64 = 0.05;

async fn check_all(
    trino_answers: &[Answer],
    cases: &[&str],
    engine: &Engine,
    shape: &str,
    tolerance: f64,
    failures: &mut Vec<String>,
) {
    for (sql, expected) in cases.iter().zip(trino_answers) {
        let actual = codelake(engine, sql).await;
        if let Some(problem) = difference(sql, expected, &actual, tolerance) {
            failures.push(format!("[{shape}] {sql}\n    {problem}"));
        }
    }
}

fn report(failures: &[String], total: usize) {
    assert!(
        failures.is_empty(),
        "{} of {total} checks differ from Trino 431:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Every category on small inputs: the same names, types and rows as Trino,
/// on two physical shapes, and again with every case running concurrently
/// on one shared engine (UDF state must not leak between queries).
#[tokio::test]
async fn f4_small_cases_match_trino_431() {
    let trino = Trino::start(&[("small", SMALL_CASES)]).await;
    let expected = trino.run("small", SMALL_CASES.len()).await;
    let spill = tempfile::tempdir().unwrap();
    let mut failures = Vec::new();

    let wide = engine_with(spill.path(), ByteSize::mib(512), 8192, 4);
    check_all(
        &expected,
        SMALL_CASES,
        &wide,
        "batch 8192 x 4",
        0.0,
        &mut failures,
    )
    .await;
    let narrow = engine_with(spill.path(), ByteSize::mib(512), 3, 3);
    check_all(&expected, SMALL_CASES, &narrow, "batch 3 x 3", 0.0, &mut failures).await;

    let shared = Arc::new(engine_with(spill.path(), ByteSize::mib(512), 1024, 4));
    let answers: Vec<(usize, Answer)> = futures::stream::iter(SMALL_CASES.iter().enumerate())
        .map(|(i, sql)| {
            let engine = Arc::clone(&shared);
            async move { (i, codelake(&engine, sql).await) }
        })
        .buffer_unordered(24)
        .collect()
        .await;
    for (i, actual) in answers {
        if let Some(problem) = difference(SMALL_CASES[i], &expected[i], &actual, 0.0) {
            failures.push(format!("[concurrent] {}\n    {problem}", SMALL_CASES[i]));
        }
    }
    assert_eq!(
        shared.memory().reserved_bytes,
        0,
        "memory left reserved after the queries"
    );
    report(&failures, SMALL_CASES.len() * 3);
}

/// Tens of thousands of generated rows (many batches, partial aggregation,
/// DST transitions, overflow in the middle of the data), compared through
/// order-sensitive digests (`checksum`, `md5` of `array_join`).
#[tokio::test]
async fn f4_volume_cases_match_trino_431() {
    let trino = Trino::start(&[("volume", VOLUME_CASES), ("approx", APPROX_CASES)]).await;
    let expected = trino.run("volume", VOLUME_CASES.len()).await;
    let approx_expected = trino.run("approx", APPROX_CASES.len()).await;
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_with(spill.path(), ByteSize::mib(512), 8192, 4);
    let mut failures = Vec::new();
    check_all(&expected, VOLUME_CASES, &engine, "volume", 0.0, &mut failures).await;
    check_all(
        &approx_expected,
        APPROX_CASES,
        &engine,
        "approx",
        APPROX_TOLERANCE,
        &mut failures,
    )
    .await;
    assert_eq!(
        engine.memory().reserved_bytes,
        0,
        "memory left reserved after the queries"
    );
    report(&failures, VOLUME_CASES.len() + APPROX_CASES.len());
}

// ---------------------------------------------------------------------------
// Engine-only tests: memory, failure in the middle, cancellation, concurrency
// ---------------------------------------------------------------------------

/// 1 000 000 distinct `bigint`s (1..=1 000 000).
const ONE_MILLION: &str = "(SELECT i * 2000 + j AS x FROM UNNEST(sequence(0, 499)) AS a(i) \
                           CROSS JOIN UNNEST(sequence(1, 2000)) AS b(j))";

/// 50 000 rows, x = 1..=50 000; `CAST(x AS integer) * 50000` overflows from
/// x = 42 950 on (observed on Trino 431: `integer multiplication overflow`).
const FIFTY_THOUSAND: &str = "(SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) \
                              CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j))";

async fn rows_of(engine: &Engine, sql: &str) -> Result<Vec<Vec<Value>>, DataFusionError> {
    let frame = engine.sql(sql).await?;
    let batches = frame.collect().await?;
    Ok(batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(move |i| b.columns().iter().map(|c| cell(c, i)).collect()))
        .collect())
}

async fn wait_released(engine: &Engine) {
    let start = Instant::now();
    while engine.memory().reserved_bytes != 0 && start.elapsed() < Duration::from_secs(10) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(engine.memory().reserved_bytes, 0, "memory still reserved");
}

/// Aggregates that keep every input value reserve it from the pool: over a
/// small pool they fail with a resource error instead of growing unbounded,
/// release everything, and succeed (with the right values) over a big pool.
#[tokio::test]
async fn f4_accumulating_aggregates_are_bounded_by_the_memory_pool() {
    let spill = tempfile::tempdir().unwrap();
    let small = engine_with(spill.path(), ByteSize::mib(4), 8192, 4);
    let big = engine_with(spill.path(), ByteSize::gib(1), 8192, 4);
    let cases: [(String, Value); 4] = [
        (
            format!("SELECT cardinality(array_agg(x)) FROM {ONE_MILLION} AS t"),
            json!(1_000_000),
        ),
        (
            format!(
                "SELECT length(listagg(CAST(x AS varchar), ',') WITHIN GROUP (ORDER BY x)) FROM {ONE_MILLION} AS t"
            ),
            // 9·1 + 90·2 + 900·3 + 9000·4 + 90000·5 + 900000·6 + 1·7 digits,
            // plus 999 999 separators.
            json!(9 + 180 + 2_700 + 36_000 + 450_000 + 5_400_000 + 7 + 999_999),
        ),
        (
            format!("SELECT cardinality(histogram(x)) FROM {ONE_MILLION} AS t"),
            json!(1_000_000),
        ),
        (
            format!("SELECT cardinality(map_agg(x, x)) FROM {ONE_MILLION} AS t"),
            json!(1_000_000),
        ),
    ];
    for (sql, value) in &cases {
        match rows_of(&small, sql).await {
            Ok(rows) => panic!("{sql}: a 4 MiB pool cannot hold 1M values, got {rows:?}"),
            Err(e) => assert!(
                e.to_string().contains("Resources exhausted"),
                "{sql}: expected a resource error, got {e}"
            ),
        }
        wait_released(&small).await;
        let rows = rows_of(&big, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows, vec![vec![value.clone()]], "{sql}");
        wait_released(&big).await;
    }
    // The small pool is usable again after the failures.
    let rows = rows_of(
        &small,
        &format!("SELECT count(*), sum(x) FROM {ONE_MILLION} AS t"),
    )
    .await
    .unwrap();
    assert_eq!(rows, vec![vec![json!(1_000_000), json!(500_000_500_000_i64)]]);
}

/// A query that fails half-way (integer overflow at x = 42 950) surfaces the
/// error, never a truncated or wrapped result; no row delivered before the
/// error is wrong; memory is released and the engine keeps answering.
#[tokio::test]
async fn f4_failure_in_the_middle_of_a_stream() {
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_with(spill.path(), ByteSize::mib(256), 1024, 4);
    let sql = format!("SELECT x, CAST(x AS integer) * 50000 FROM {FIFTY_THOUSAND} AS t");
    let mut stream = engine.sql(&sql).await.unwrap().execute_stream().await.unwrap();
    let mut delivered = 0_usize;
    let mut failure = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(batch) => {
                for i in 0..batch.num_rows() {
                    let x = cell(batch.column(0), i).as_i64().unwrap();
                    let y = cell(batch.column(1), i).as_i64().unwrap();
                    assert_eq!(y, x * 50000, "wrapped value delivered for x = {x}");
                    assert!(x < 42_950, "x = {x} overflows and must not be delivered");
                }
                delivered += batch.num_rows();
            }
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    let failure = failure.expect("the overflow must fail the query");
    assert_eq!(error_name(&failure), "NUMERIC_VALUE_OUT_OF_RANGE", "{failure}");
    assert!(delivered < 50_000);
    drop(stream);
    wait_released(&engine).await;

    // Same failure under an aggregate that holds memory.
    let sql = format!(
        "SELECT x % 100, array_agg(CAST(x AS integer) * 50000) FROM {FIFTY_THOUSAND} AS t GROUP BY 1"
    );
    let e = rows_of(&engine, &sql)
        .await
        .expect_err("overflow inside an aggregate");
    assert_eq!(error_name(&e), "NUMERIC_VALUE_OUT_OF_RANGE", "{e}");
    wait_released(&engine).await;

    // try() turns exactly the overflowing rows into NULL.
    let rows = rows_of(
        &engine,
        &format!("SELECT count(try(CAST(x AS integer) * 50000)), count(*) FROM {FIFTY_THOUSAND} AS t"),
    )
    .await
    .unwrap();
    assert_eq!(rows, vec![vec![json!(42_949), json!(50_000)]]);
}

/// Dropping a stream half-way (client gone) releases the aggregation state.
#[tokio::test]
async fn f4_cancelled_query_releases_memory() {
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_with(spill.path(), ByteSize::mib(512), 1024, 4);
    let sql = format!(
        "SELECT x % 1000, array_agg(x), max_by(x, x, 3), approx_percentile(x, 0.5) FROM {ONE_MILLION} AS t GROUP BY 1"
    );
    let mut stream = engine.sql(&sql).await.unwrap().execute_stream().await.unwrap();
    let first = stream.next().await.expect("a first batch").expect("no error");
    assert!(first.num_rows() > 0);
    drop(stream);
    wait_released(&engine).await;
    let rows = rows_of(&engine, "SELECT count(*) FROM (VALUES 1, 2) AS t(x)")
        .await
        .unwrap();
    assert_eq!(rows, vec![vec![json!(2)]]);
}

/// Concurrent queries on one engine: `uuid()` never repeats across queries,
/// a failing query does not affect its neighbours, and `try` stays per row.
#[tokio::test]
async fn f4_concurrent_queries_are_isolated() {
    let spill = tempfile::tempdir().unwrap();
    let engine = Arc::new(engine_with(spill.path(), ByteSize::mib(512), 1024, 4));
    let mut tasks = Vec::new();
    for i in 0..24 {
        let engine = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            match i % 3 {
                0 => {
                    let rows = rows_of(
                        &engine,
                        "SELECT CAST(uuid() AS varchar) FROM UNNEST(sequence(1, 5000)) AS t(x)",
                    )
                    .await
                    .unwrap();
                    (i, rows)
                }
                1 => {
                    let e = rows_of(&engine, &format!("SELECT sum(CAST(x AS integer) * 50000) FROM {FIFTY_THOUSAND} AS t"))
                        .await
                        .expect_err("overflow");
                    (i, vec![vec![json!(error_name(&e))]])
                }
                _ => {
                    let rows = rows_of(
                        &engine,
                        &format!("SELECT count(try(1000 / (x % 10))), sum(try(1000 / (x % 10))) FROM {FIFTY_THOUSAND} AS t"),
                    )
                    .await
                    .unwrap();
                    (i, rows)
                }
            }
        }));
    }
    let mut uuids = HashSet::new();
    for task in tasks {
        let (i, rows) = task.await.unwrap();
        match i % 3 {
            0 => {
                assert_eq!(rows.len(), 5000);
                for row in rows {
                    let text = row[0].as_str().unwrap().to_owned();
                    assert_eq!(text.len(), 36, "{text}");
                    assert_eq!(&text[14..15], "4", "version 4 uuid: {text}");
                    assert!(uuids.insert(text), "uuid repeated");
                }
            }
            1 => assert_eq!(rows, vec![vec![json!("NUMERIC_VALUE_OUT_OF_RANGE")]]),
            // Observed on Trino 431 (the same statement is a volume case).
            _ => assert_eq!(rows, vec![vec![json!(45_000), json!(14_135_000)]]),
        }
    }
    assert_eq!(uuids.len(), 8 * 5000);
    wait_released(&engine).await;
}

// ---------------------------------------------------------------------------
// Defects: expected values observed on trinodb/trino:431
// ---------------------------------------------------------------------------

async fn defect_answer(sql: &str) -> Answer {
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_with(spill.path(), ByteSize::mib(256), 8192, 4);
    codelake(&engine, sql).await
}

/// `sql` must return Trino's `types` and `rows` (column names not compared).
async fn expect_rows(sql: &str, types: &[&str], rows: Value) {
    let actual = defect_answer(sql).await;
    let names = match &actual {
        Answer::Rows { columns, .. } if columns.len() == types.len() => {
            columns.iter().map(|(n, _)| n.clone()).collect()
        }
        _ => vec![String::new(); types.len()],
    };
    let expected = Answer::Rows {
        columns: names
            .into_iter()
            .zip(types.iter().map(|t| (*t).to_owned()))
            .collect(),
        rows: rows
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| r.as_array().expect("row").clone())
            .collect(),
    };
    if let Some(problem) = difference(sql, &expected, &actual, 0.0) {
        panic!("{sql}\n    {problem}");
    }
}

/// `sql` must fail with Trino's error name.
async fn expect_error(sql: &str, name: &str) {
    let expected = Answer::Error {
        name: name.to_owned(),
        message: String::new(),
    };
    if let Some(problem) = difference(sql, &expected, &defect_answer(sql).await, 0.0) {
        panic!("{sql}\n    {problem}");
    }
}

/// Two `array_agg` calls over the same argument with different `ORDER BY`
/// (or `listagg … WITHIN GROUP` next to one) share a single ordering: the
/// second silently returns the first one's order. The reference pipelines build
/// column signatures with `array_join(array_agg(… ORDER BY …))`.
#[tokio::test]
async fn defect_01_array_agg_orderings_in_one_query() {
    expect_rows(
        "SELECT array_agg(x ORDER BY x)[1], array_agg(x ORDER BY x DESC)[1] FROM UNNEST(sequence(1, 5000)) AS t(x)",
        &["bigint", "bigint"],
        json!([[1, 5000]]),
    )
    .await;
    expect_rows(
        "SELECT listagg(CAST(x AS varchar), ',') WITHIN GROUP (ORDER BY x DESC), \
         array_join(array_agg(CAST(x AS varchar) ORDER BY x), ',') FROM UNNEST(sequence(1, 5)) AS t(x)",
        &["varchar", "varchar"],
        json!([["5,4,3,2,1", "1,2,3,4,5"]]),
    )
    .await;
}

/// Trino sorts NULLs last in both directions unless `NULLS FIRST` is
/// written; the engine puts them first for `DESC`. Dedup with
/// `row_number() OVER (… ORDER BY x DESC) = 1` (134 uses in reference) then keeps
/// the NULL row instead of the greatest one.
#[tokio::test]
async fn defect_02_desc_order_puts_nulls_last() {
    expect_rows(
        "SELECT x FROM (VALUES 3, NULL, 1) AS t(x) ORDER BY x DESC",
        &["integer"],
        json!([[3], [1], [null]]),
    )
    .await;
    expect_rows(
        "SELECT g, x, row_number() OVER (PARTITION BY g ORDER BY x DESC) \
         FROM (VALUES (1, 5), (1, NULL), (1, 7), (2, NULL), (2, 1)) AS t(g, x) ORDER BY g, x",
        &["integer", "integer", "bigint"],
        json!([[1, 5, 2], [1, 7, 1], [1, null, 3], [2, 1, 1], [2, null, 2]]),
    )
    .await;
    expect_rows(
        "SELECT array_agg(x ORDER BY x DESC) FROM (VALUES 3, NULL, 1) AS t(x)",
        &["array(integer)"],
        json!([[[3, 1, null]]]),
    )
    .await;
}

/// Overflow checks are lost when the arithmetic sits in a filter above a
/// join (or is a projection read only by such a filter): the value wraps and
/// rows are silently dropped or kept. Trino fails the query.
#[tokio::test]
async fn defect_03_overflow_in_filter_over_join() {
    expect_error(
        &format!("SELECT count(*) FROM {FIFTY_THOUSAND} AS t WHERE CAST(x AS integer) * 50000 > 0"),
        "NUMERIC_VALUE_OUT_OF_RANGE",
    )
    .await;
    expect_error(
        &format!("SELECT count(*) FROM (SELECT CAST(x AS integer) * 50000 AS y FROM {FIFTY_THOUSAND} AS t) WHERE y <> 0"),
        "NUMERIC_VALUE_OUT_OF_RANGE",
    )
    .await;
}

/// Trino treats `-0.0` and `0.0` as the same value for grouping, distinct,
/// joins, `histogram`, `checksum` and `array_distinct`; the engine keeps
/// them apart (a checksum comparison between two tables then differs).
#[tokio::test]
async fn defect_04_negative_zero_equals_zero() {
    expect_rows(
        "SELECT x, count(*) FROM (VALUES 0E0, -0E0, 1E0) AS t(x) GROUP BY x ORDER BY 1",
        &["double", "bigint"],
        json!([[0.0, 2], [1.0, 1]]),
    )
    .await;
    expect_rows(
        "SELECT count(DISTINCT x) FROM (VALUES 0E0, -0E0) AS t(x)",
        &["bigint"],
        json!([[1]]),
    )
    .await;
    expect_rows(
        "SELECT histogram(x) FROM (VALUES 0E0, -0E0) AS t(x)",
        &["map(double, bigint)"],
        json!([[{"0.0": 2}]]),
    )
    .await;
    // Same checksum as two positive zeros.
    expect_rows(
        "SELECT checksum(x) FROM (VALUES 0E0, -0E0) AS t(x)",
        &["varbinary"],
        json!([["Olss8tigItU="]]),
    )
    .await;
    expect_rows(
        "SELECT count(*) FROM (VALUES 0E0) AS a(x) JOIN (VALUES -0E0) AS b(y) ON x = y",
        &["bigint"],
        json!([[1]]),
    )
    .await;
    expect_rows(
        "SELECT array_distinct(ARRAY[0E0, -0E0])",
        &["array(double)"],
        json!([[[0.0]]]),
    )
    .await;
}

/// A `decimal(38, s)` sum past 38 digits returns a 39-digit value in a
/// `decimal(38,0)` column; Trino fails (`Decimal overflow`).
#[tokio::test]
async fn defect_05_decimal_sum_overflow() {
    expect_error(
        "SELECT sum(x) FROM (VALUES DECIMAL '99999999999999999999999999999999999999', DECIMAL '1') AS t(x)",
        "GENERIC_INTERNAL_ERROR",
    )
    .await;
}

/// `abs(integer)` of the minimum value returns a `bigint` 2147483648.
#[tokio::test]
async fn defect_06_abs_integer_minimum() {
    expect_error("SELECT abs(-2147483648)", "NUMERIC_VALUE_OUT_OF_RANGE").await;
    expect_rows(
        "SELECT typeof(abs(x)) FROM (VALUES 1) AS t(x)",
        &["varchar"],
        json!([["integer"]]),
    )
    .await;
}

/// NaN: `max_by`/`min_by` never pick a NaN key, `array_max`/`array_min`
/// skip NaN, and NaN is not equal to itself in `contains`/`array_position`.
#[tokio::test]
async fn defect_07_nan_in_max_by_and_arrays() {
    expect_rows(
        "SELECT max_by(x, y), min_by(x, y) FROM (VALUES (1, nan()), (2, 1E0), (3, NULL)) AS t(x, y)",
        &["integer", "integer"],
        json!([[2, 2]]),
    )
    .await;
    expect_rows(
        "SELECT array_max(ARRAY[1E0, nan()]), array_min(ARRAY[nan(), 1E0]), contains(ARRAY[nan()], nan()), \
         array_position(ARRAY[nan()], nan())",
        &["double", "double", "boolean", "bigint"],
        json!([[1.0, 1.0, false, 0]]),
    )
    .await;
}

/// `CAST(double AS json)` writes `NaN`/`Infinity` bare, which is not JSON;
/// Trino writes them as strings.
#[tokio::test]
async fn defect_08_json_of_nan() {
    expect_rows(
        "SELECT CAST(ARRAY[1E0, nan(), infinity()] AS json)",
        &["json"],
        json!([["[1.0,\"NaN\",\"Infinity\"]"]]),
    )
    .await;
    expect_rows(
        "SELECT json_format(CAST(ARRAY[nan()] AS json))",
        &["varchar"],
        json!([["[\"NaN\"]"]]),
    )
    .await;
}

/// `listagg` over a group with only NULLs is NULL in Trino, `''` here.
#[tokio::test]
async fn defect_09_listagg_of_nulls() {
    expect_rows(
        "SELECT listagg(s, ',') WITHIN GROUP (ORDER BY s) FROM (VALUES CAST(NULL AS varchar), NULL) AS t(s)",
        &["varchar"],
        json!([[null]]),
    )
    .await;
    expect_rows(
        "SELECT g, listagg(x, '') WITHIN GROUP (ORDER BY x DESC) FROM (VALUES (1, 'y'), (1, 'x'), (2, NULL)) AS t(g, x) \
         GROUP BY g ORDER BY 1",
        &["integer", "varchar"],
        json!([[1, "yx"], [2, null]]),
    )
    .await;
}

/// `try(with_timezone(ts, 'constant zone'))` fails planning: the zone is a
/// constant, but not once wrapped by `try`.
#[tokio::test]
async fn defect_10_try_with_timezone() {
    expect_rows(
        "SELECT try(with_timezone(TIMESTAMP '2018-11-04 00:30:00.000', 'America/Sao_Paulo'))",
        &["timestamp(3) with time zone"],
        json!([["2018-11-04 01:30:00.000 America/Sao_Paulo"]]),
    )
    .await;
    expect_rows(
        "SELECT count(*) FROM (VALUES TIMESTAMP '2018-11-04 00:30:00.000', TIMESTAMP '2018-11-05 00:30:00.000') AS t(x) \
         WHERE try(with_timezone(x, 'America/Sao_Paulo')) IS NOT NULL",
        &["bigint"],
        json!([[2]]),
    )
    .await;
}

/// `VALUES ARRAY[1, 2], ARRAY[3]`: the `VALUES 1, 2` text rewrite splits
/// rows at the commas inside the brackets.
#[tokio::test]
async fn defect_11_values_rows_with_arrays() {
    expect_rows(
        "SELECT count(*) FROM (VALUES ARRAY[1, 2], ARRAY[3]) AS t(a)",
        &["bigint"],
        json!([[2]]),
    )
    .await;
    expect_rows(
        "SELECT cardinality(a) FROM (VALUES ARRAY[1, 2], ARRAY[3]) AS t(a) ORDER BY 1",
        &["bigint"],
        json!([[1], [2]]),
    )
    .await;
}

/// `CROSS JOIN UNNEST(column)` (the usual way to explode arrays in Trino)
/// fails with an internal error in the federation optimizer rule.
#[tokio::test]
async fn defect_12_cross_join_unnest_column() {
    expect_rows(
        "SELECT g, x FROM (VALUES (1, ARRAY[10, 20]), (2, ARRAY[])) AS t(g, a) CROSS JOIN UNNEST(a) AS u(x) ORDER BY 1, 2",
        &["integer", "integer"],
        json!([[1, 10], [1, 20]]),
    )
    .await;
}

/// `UNNEST(map)` yields key/value rows in Trino.
#[tokio::test]
async fn defect_13_unnest_map() {
    expect_rows(
        "SELECT k, v FROM UNNEST(map(ARRAY['a', 'b'], ARRAY[1, 2])) AS t(k, v) ORDER BY 1",
        &["varchar", "integer"],
        json!([["a", 1], ["b", 2]]),
    )
    .await;
}

/// `varbinary || varbinary` (reference-style `X'0000' || …`) fails planning.
#[tokio::test]
async fn defect_14_varbinary_concat_operator() {
    expect_rows("SELECT to_hex(X'01' || X'02')", &["varchar"], json!([["0102"]])).await;
}

/// Trino's regular expressions (Joni) support look-around and
/// back-references; the engine rejects them.
#[tokio::test]
async fn defect_15_regexp_lookaround_backreference() {
    expect_rows(
        r"SELECT regexp_like('abc', 'a(?=b)'), regexp_like('abab', '(ab)\1'), regexp_replace('aXbX', '(?<=a)X', '-')",
        &["boolean", "boolean", "varchar"],
        json!([[true, true, "a-bX"]]),
    )
    .await;
}

/// `from_iso8601_date` accepts ISO week dates, ordinal dates and reduced
/// precision in Trino.
#[tokio::test]
async fn defect_16_from_iso8601_date_forms() {
    expect_rows(
        "SELECT from_iso8601_date('2020-W10-3'), from_iso8601_date('2020-060'), from_iso8601_date('2020-02'), \
         from_iso8601_date('2020')",
        &["date", "date", "date", "date"],
        json!([["2020-03-04", "2020-02-29", "2020-02-01", "2020-01-01"]]),
    )
    .await;
}

/// Joda `z` (zone short name) in `format_datetime`.
#[tokio::test]
async fn defect_17_format_datetime_zone_name() {
    expect_rows(
        "SELECT format_datetime(TIMESTAMP '2020-06-01 10:00:00 America/Sao_Paulo', 'z')",
        &["varchar"],
        json!([["BRT"]]),
    )
    .await;
}

/// Aggregate result types: `avg(real)` is `real`, `bitwise_and_agg` and
/// `bitwise_or_agg` are `bigint`, `corr`/`covar_samp`/`regr_slope` of
/// integers resolve to the `real` overloads.
#[tokio::test]
async fn defect_18_aggregate_result_types() {
    expect_rows(
        "SELECT avg(x), typeof(avg(x)) FROM (VALUES REAL '1.5', REAL '2.25') AS t(x)",
        &["real", "varchar"],
        json!([[1.875, "real"]]),
    )
    .await;
    expect_rows(
        "SELECT bitwise_and_agg(x), typeof(bitwise_and_agg(x)), typeof(bitwise_or_agg(x)) FROM (VALUES 12, 10) AS t(x)",
        &["bigint", "varchar", "varchar"],
        json!([[8, "bigint", "bigint"]]),
    )
    .await;
    expect_rows(
        "SELECT typeof(corr(x, y)), typeof(covar_samp(x, y)), typeof(regr_slope(y, x)) FROM (VALUES (1, 1), (2, 3)) AS t(x, y)",
        &["varchar", "varchar", "varchar"],
        json!([["real", "real", "real"]]),
    )
    .await;
}

/// `timestamp(0) ± INTERVAL` is `timestamp(3)` in Trino (interval
/// precision is milliseconds).
#[tokio::test]
async fn defect_19_timestamp_interval_precision() {
    expect_rows(
        "SELECT TIMESTAMP '2020-01-01 00:00:00' - INTERVAL '1' SECOND, typeof(TIMESTAMP '2020-01-01 00:00:00' - INTERVAL '1' SECOND)",
        &["timestamp(3)", "varchar"],
        json!([["2019-12-31 23:59:59.000", "timestamp(3)"]]),
    )
    .await;
    expect_rows(
        "SELECT TIMESTAMP '2019-11-02 12:00:00 America/Sao_Paulo' + INTERVAL '1' DAY",
        &["timestamp(3) with time zone"],
        json!([["2019-11-03 12:00:00.000 America/Sao_Paulo"]]),
    )
    .await;
}

/// A bare `VALUES` query names its column `_col0` in Trino.
#[tokio::test]
async fn defect_20_values_column_name() {
    let answer = defect_answer("VALUES 1, 2").await;
    let expected = Answer::Rows {
        columns: vec![("_col0".into(), "integer".into())],
        rows: vec![vec![json!(1)], vec![json!(2)]],
    };
    assert_eq!(difference("VALUES 1, 2", &expected, &answer, 0.0), None);
}

/// Integer literals out of the `bigint` range are a syntax error in Trino;
/// the engine turns them into a `double` or an Arrow `UInt64`.
#[tokio::test]
async fn defect_21_integer_literal_out_of_range() {
    expect_error(
        "SELECT 12345678901234567890123456789012345678 * 10",
        "SYNTAX_ERROR",
    )
    .await;
    expect_error("SELECT typeof(9999999999999999999)", "SYNTAX_ERROR").await;
}

/// `CAST(double AS integer)` out of range is `NUMERIC_VALUE_OUT_OF_RANGE`.
#[tokio::test]
async fn defect_22_cast_double_out_of_range_error_name() {
    expect_error("SELECT CAST(3E9 AS integer)", "NUMERIC_VALUE_OUT_OF_RANGE").await;
}

/// `approx_percentile` over a NaN input fails in Trino; the engine returns
/// a value.
#[tokio::test]
async fn defect_23_approx_percentile_nan() {
    expect_error(
        "SELECT approx_percentile(x, 0.5) FROM (VALUES 1E0, nan(), 3E0) AS t(x)",
        "GENERIC_INTERNAL_ERROR",
    )
    .await;
}

/// Trino accepts blanks between the digits of a binary literal.
#[tokio::test]
async fn defect_24_binary_literal_with_blanks() {
    expect_rows("SELECT to_hex(X'ED A0 80')", &["varchar"], json!([["EDA080"]])).await;
}

/// `from_utf8` replaces one malformed sequence with one U+FFFD (a UTF-8
/// encoded surrogate, an overlong form); the engine emits one per byte.
#[tokio::test]
async fn defect_25_from_utf8_malformed_sequences() {
    expect_rows(
        "SELECT from_utf8(X'EDA080'), from_utf8(X'C0AF'), length(from_utf8(X'EDA080'))",
        &["varchar", "varchar", "bigint"],
        json!([["\u{fffd}", "\u{fffd}", 1]]),
    )
    .await;
}

/// `checksum` accepts any comparable type in Trino (arrays, rows).
#[tokio::test]
async fn defect_26_checksum_of_containers() {
    expect_rows(
        "SELECT checksum(ARRAY[1, 2]), checksum(ROW(1, 'a'))",
        &["varbinary", "varbinary"],
        json!([["nVq7OL66rCk=", "xrWIvAx17Jk="]]),
    )
    .await;
}

/// Wrong results listed as bounds in ADR 0008 ("join conditions, sort keys,
/// window arguments"): a float equi-join matches NaN, and overflow in a sort
/// key or a window-function argument wraps silently. Kept as a defect: the
/// answer differs from Trino without any error.
#[tokio::test]
async fn defect_27_documented_bounds_are_wrong_results() {
    expect_rows(
        "SELECT count(*) FROM (VALUES nan(), 1E0) AS a(x) JOIN (VALUES nan(), 1E0) AS b(y) ON x = y",
        &["bigint"],
        json!([[1]]),
    )
    .await;
    expect_error(
        "SELECT x FROM (VALUES 2147483647, 1) AS t(x) ORDER BY x + 1",
        "NUMERIC_VALUE_OUT_OF_RANGE",
    )
    .await;
    expect_error(
        "SELECT lag(x + 1) OVER (ORDER BY x) FROM (VALUES 2147483647, 1) AS t(x)",
        "NUMERIC_VALUE_OUT_OF_RANGE",
    )
    .await;
}

/// Trino's `approx_distinct` (`HyperLogLog`) and `approx_percentile`
/// (`qdigest`) run in constant memory; the engine keeps every value (the
/// distinct count comes out exact), so a big input fails with a resource
/// error where Trino answers.
#[tokio::test]
async fn defect_28_approximate_aggregates_in_bounded_memory() {
    let spill = tempfile::tempdir().unwrap();
    let small = engine_with(spill.path(), ByteSize::mib(4), 8192, 4);
    for (sql, expected) in [
        (format!("SELECT approx_distinct(x) FROM {ONE_MILLION} AS t"), 1e6),
        (
            format!("SELECT approx_percentile(x, 0.5) FROM {ONE_MILLION} AS t"),
            5e5,
        ),
    ] {
        let rows = rows_of(&small, &sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        let value = rows[0][0].as_f64().unwrap();
        assert!((value - expected).abs() <= 0.07 * expected, "{sql}: {value}");
    }
}

/// `max_by(x, y, n)` takes time proportional to rows x n: 500 000 rows with
/// n = 50 000 take about a minute (debug build) where a bounded heap needs
/// well under a second.
#[tokio::test]
async fn defect_29_max_by_n_is_not_quadratic() {
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_with(spill.path(), ByteSize::gib(1), 8192, 4);
    let sql = "SELECT max_by(x, x, 50000)[1], max_by(x, x, 50000)[50000] FROM (SELECT i * 1000 + j AS x \
               FROM UNNEST(sequence(0, 499)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t";
    let start = Instant::now();
    let rows = rows_of(&engine, sql).await.unwrap();
    assert_eq!(rows, vec![vec![json!(500_000), json!(450_001)]]);
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "took {:?}",
        start.elapsed()
    );
}

/// `TRIM(LEADING | TRAILING | BOTH FROM s)` without a character list.
#[tokio::test]
async fn defect_30_trim_side_without_characters() {
    expect_rows(
        "SELECT trim(TRAILING FROM '  a  '), trim(BOTH FROM '  a  ')",
        &["varchar", "varchar"],
        json!([["  a", "a"]]),
    )
    .await;
}

/// `greatest`/`least` over arrays compare element by element in Trino; the
/// engine fails with an Arrow internal error.
#[tokio::test]
async fn defect_31_greatest_of_arrays() {
    expect_rows(
        "SELECT greatest(ARRAY[1, 2], ARRAY[1, 3]), least(ARRAY['b'], ARRAY['a', 'z'])",
        &["array(integer)", "array(varchar)"],
        json!([[[1, 3], ["a", "z"]]]),
    )
    .await;
}

/// `array_agg` over a sliding window frame (`ROWS BETWEEN 1 PRECEDING AND
/// CURRENT ROW`) is rejected: the accumulator cannot retract.
#[tokio::test]
async fn defect_32_array_agg_sliding_window() {
    expect_rows(
        "SELECT x, array_agg(x) OVER (PARTITION BY g ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
         FROM (VALUES (1, 1), (1, 2), (1, 3), (2, 4)) AS t(g, x) ORDER BY x",
        &["integer", "array(integer)"],
        json!([[1, [1]], [2, [1, 2]], [3, [2, 3]], [4, [4]]]),
    )
    .await;
}

/// `CAST(varchar AS timestamp)` accepts an ISO `T` separator that Trino
/// rejects, so `try_cast` returns a value where Trino returns NULL; and it
/// rejects a zone suffix, which Trino drops (the wall clock is kept).
#[tokio::test]
async fn defect_33_cast_iso_t_separator_to_timestamp() {
    expect_error(
        "SELECT CAST('2020-01-01T10:00:00' AS timestamp)",
        "INVALID_CAST_ARGUMENT",
    )
    .await;
    expect_rows(
        "SELECT try_cast('2020-01-01T10:00:00' AS timestamp)",
        &["timestamp(3)"],
        json!([[null]]),
    )
    .await;
    expect_rows(
        "SELECT CAST('2020-01-01 10:00:00Z' AS timestamp), CAST('2020-01-01 10:00:00 America/Sao_Paulo' AS timestamp), \
         CAST('2020-01-01 10:00:00+05:00' AS timestamp)",
        &["timestamp(3)", "timestamp(3)", "timestamp(3)"],
        json!([["2020-01-01 10:00:00.000", "2020-01-01 10:00:00.000", "2020-01-01 10:00:00.000"]]),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Cases (every one was run on trinodb/trino:431 when written; the tests
// above ask Trino again on every run)
// ---------------------------------------------------------------------------

/// Literals and small `VALUES` tables: every category, NULLs, edges and errors.
const SMALL_CASES: &[&str] = &[
    "SELECT x, date_add('day', n, x) AS a, date_add('month', n, x) AS b, date_add('year', n, x) AS c FROM (VALUES (DATE '2020-01-31', 1), (DATE '2020-02-29', 12), (DATE '2019-03-31', -1), (NULL, 1), (DATE '2021-12-31', NULL)) AS t(x, n) ORDER BY 1, 2",
    "SELECT date_add('millisecond', (s - 1) * 5, ts) FROM (VALUES (TIMESTAMP '2024-02-29 23:59:59.998', 1), (TIMESTAMP '2024-02-29 23:59:59.998', 2), (TIMESTAMP '2024-02-29 23:59:59.998', 3), (NULL, 4)) AS t(ts, s)",
    "SELECT date_add('hour', h, TIMESTAMP '2023-10-29 00:30:00 Europe/Lisbon') FROM (VALUES 0, 1, 2, 3) AS t(h) ORDER BY 1",
    "SELECT date_add('day', 1, TIMESTAMP '2019-11-02 12:00:00 America/Sao_Paulo')",
    "SELECT date_add('hour', 24, TIMESTAMP '2019-11-02 12:00:00 America/Sao_Paulo')",
    "SELECT date_diff('second', a, b), date_diff('minute', a, b), date_diff('hour', a, b), date_diff('day', a, b), date_diff('month', a, b), date_diff('year', a, b) FROM (VALUES (TIMESTAMP '2020-01-31 10:00:00', TIMESTAMP '2020-02-29 09:59:59'), (TIMESTAMP '2020-02-29 00:00:00', TIMESTAMP '2021-02-28 00:00:00'), (TIMESTAMP '2021-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:01'), (NULL, TIMESTAMP '2020-01-01 00:00:00')) AS t(a, b)",
    "SELECT date_diff('day', DATE '2020-03-01', DATE '2020-02-01'), date_diff('week', DATE '2020-01-01', DATE '2020-12-31'), date_diff('quarter', DATE '2020-01-31', DATE '2020-04-30')",
    "SELECT date_diff('hour', TIMESTAMP '2019-11-03 00:00:00 America/Sao_Paulo', TIMESTAMP '2019-11-04 00:00:00 America/Sao_Paulo'), date_diff('day', TIMESTAMP '2019-11-03 00:00:00 America/Sao_Paulo', TIMESTAMP '2019-11-04 00:00:00 America/Sao_Paulo')",
    "SELECT date_diff('second', TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 01:00:00') / 3600.0",
    "SELECT date_diff('second', x, TIMESTAMP '2020-01-01 12:00:00') / 3600.0 FROM (VALUES TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 11:20:00', NULL) AS t(x) ORDER BY 1",
    "SELECT date_format(x, '%Y-%m-%d %H:%i:%s.%f'), date_format(x, '%a %b %e %c %j %k %l %p %r %T %v %x %W %M %D %y') FROM (VALUES TIMESTAMP '2020-02-29 13:05:09.123', TIMESTAMP '2021-01-03 00:00:00.000', NULL) AS t(x)",
    "SELECT format_datetime(x, 'yyyy/MM/dd'), format_datetime(x, 'YYYY-ww e EEE EEEE MMM MMMM a hh HH kk KK mm ss SSS'), format_datetime(x, 'D DDD') FROM (VALUES TIMESTAMP '2020-12-31 23:59:59.999', TIMESTAMP '2021-01-01 00:00:00.000', NULL) AS t(x)",
    "SELECT date_parse(s, '%Y-%m-%d %H:%i:%s') FROM (VALUES '2020-02-29 23:59:59', '2020-1-2 3:4:5', NULL) AS t(s)",
    "SELECT date_parse('2020-02-30', '%Y-%m-%d')",
    "SELECT date_parse('12/31/2020 11:59:59 PM', '%m/%d/%Y %h:%i:%s %p')",
    "SELECT date_trunc('week', x), date_trunc('month', x), date_trunc('quarter', x), date_trunc('year', x), date_trunc('day', x), date_trunc('hour', x) FROM (VALUES TIMESTAMP '2021-01-03 13:14:15.678', TIMESTAMP '2020-02-29 00:00:00.000', NULL) AS t(x)",
    "SELECT date_trunc('day', TIMESTAMP '2019-11-03 12:00:00 America/Sao_Paulo')",
    "SELECT with_timezone(CAST(x AS timestamp(3)), 'America/Sao_Paulo') FROM (VALUES TIMESTAMP '2020-06-01 10:00:00.123', TIMESTAMP '2019-11-03 00:30:00.000', NULL) AS t(x)",
    "SELECT CAST(with_timezone(x, 'America/Sao_Paulo') AT TIME ZONE 'UTC' AS timestamp(3)) FROM (VALUES TIMESTAMP '2020-06-01 10:00:00.123', TIMESTAMP '2019-11-03 00:30:00.000') AS t(x) ORDER BY 1",
    "SELECT CAST(TIMESTAMP '2020-06-01 10:00:00 UTC' AT TIME ZONE 'America/Sao_Paulo' AS timestamp(3))",
    "SELECT day_of_week(x), day_of_year(x), week_of_year(x), year_of_week(x), quarter(x), last_day_of_month(x) FROM (VALUES DATE '2020-12-31', DATE '2021-01-03', DATE '2024-02-10', NULL) AS t(x) ORDER BY 1",
    "SELECT extract(year FROM x), extract(dow FROM x), extract(doy FROM x), extract(week FROM x), extract(hour FROM x), extract(second FROM x) FROM (VALUES TIMESTAMP '2021-01-01 23:59:58.999') AS t(x)",
    "SELECT from_unixtime(x), to_unixtime(from_unixtime(x)) FROM (VALUES -1.5E0, 0E0, 1600000000.1234E0, NULL) AS t(x) ORDER BY 2",
    "SELECT from_unixtime(1600000000, 'America/Sao_Paulo'), from_unixtime(1600000000, 5, 30)",
    "SELECT human_readable_seconds(x) FROM (VALUES 0E0, 59.9E0, 3601E0, 96400E0, 1000000E0, NULL) AS t(x)",
    "SELECT CAST(x AS varchar) FROM (VALUES TIMESTAMP '2020-01-01 00:00:00.100', TIMESTAMP '2020-01-01 00:00:00.000') AS t(x) ORDER BY 1",
    "SELECT CAST('2020-02-30' AS date)",
    "SELECT CAST(s AS date) FROM (VALUES '2020-02-29', ' 2020-02-28 ', '2020-2-3') AS t(s) ORDER BY 1",
    "SELECT CAST(s AS timestamp) FROM (VALUES '2020-02-29 01:02:03', '2020-02-29', '2020-02-29 01:02:03.456789') AS t(s) ORDER BY 1",
    "SELECT CAST(TIMESTAMP '2020-01-01 10:00:00.999999' AS timestamp(3)), CAST(TIMESTAMP '2020-12-31 23:59:59.9999' AS timestamp(3)), CAST(TIMESTAMP '2020-12-31 23:59:59.5' AS timestamp(0))",
    "SELECT substr(s, 2), substr(s, -2), substr(s, 2, 2), substr(s, 0), substr(s, 10), length(s), reverse(s), upper(s), lower(s) FROM (VALUES 'abcde', 'ção😀x', '', NULL) AS t(s) ORDER BY 1",
    "SELECT substr(s, -3, 2) FROM (VALUES 'abcde', 'ab') AS t(s) ORDER BY 1",
    "SELECT concat(a, b), concat(a, b, a), a || b, concat_ws('_', a, b), concat_ws('_', a, NULL, b) FROM (VALUES ('x', 'y'), ('x', NULL), (NULL, NULL), ('', '')) AS t(a, b) ORDER BY 1, 2",
    "SELECT concat_ws(NULL, 'a', 'b'), concat_ws('', 'a', 'b'), concat_ws(',', ARRAY['a', NULL, 'c'])",
    "SELECT CONCAT_WS('_', CAST(g AS VARCHAR), COALESCE(CAST(e AS VARCHAR), '')) FROM (VALUES (1, 2.50), (2, NULL), (3, -0.01)) AS t(g, e) ORDER BY 1",
    "SELECT lpad(s, 5, 'xy'), rpad(s, 5, 'xy'), lpad(s, 2, '0'), rpad(s, 0, '0') FROM (VALUES 'abc', 'abcdefg', '', NULL) AS t(s) ORDER BY 1",
    "SELECT lpad('abc', 5, '')",
    "SELECT lpad('abc', -1, 'x')",
    "SELECT split(s, ','), split(s, ',', 2), split_part(s, ',', 2), split_part(s, ',', 5) FROM (VALUES 'a,b,,c', ',', '', NULL) AS t(s) ORDER BY 1",
    "SELECT split_part('a,b', ',', 0)",
    "SELECT strpos(s, 'b'), strpos(s, 'b', 2), strpos(s, 'b', -1), strpos(s, ''), position('c' IN s) FROM (VALUES 'abcbab', 'xyz', NULL) AS t(s) ORDER BY 1",
    "SELECT strpos('abc', 'b', 0)",
    "SELECT replace(s, 'a'), replace(s, 'a', 'XY'), replace(s, '', '-'), translate(s, 'ab', 'x') FROM (VALUES 'banana', '', NULL) AS t(s) ORDER BY 1",
    "SELECT levenshtein_distance(a, b), hamming_distance(a, a) FROM (VALUES ('kitten', 'sitting'), ('', 'abc'), ('ção', 'cao'), (NULL, 'x')) AS t(a, b) ORDER BY 1",
    "SELECT hamming_distance('abc', 'ab')",
    "SELECT chr(x), codepoint(chr(x)) FROM (VALUES 65, 231, 128512, NULL) AS t(x) ORDER BY 2",
    "SELECT chr(-1)",
    "SELECT chr(1114112)",
    "SELECT trim(s), ltrim(s), rtrim(s), trim(BOTH 'x' FROM s), length(trim(s)) FROM (VALUES '  a b  ', 'xxaxx', '', NULL) AS t(s) ORDER BY 1",
    "SELECT starts_with(s, 'ab'), soundex(s), luhn_check(d) FROM (VALUES ('abc', '79927398713'), ('Robert', '1234'), (NULL, NULL)) AS t(s, d) ORDER BY 1",
    "SELECT from_utf8(x), from_utf8(x, '?'), to_utf8('ção') FROM (VALUES X'61FF62', X'C3A7', NULL) AS t(x) ORDER BY 1",
    "SELECT normalize('ção', NFD) = 'ção', length(normalize('ção', NFD)), upper('ß'), lower('İ')",
    r"SELECT regexp_like(s, 'a.c'), regexp_extract(s, '(\d+)-(\d+)', 2), regexp_extract_all(s, '\d+'), regexp_replace(s, '(\d+)', '<$1>'), regexp_count(s, '\d'), regexp_position(s, '\d', 1, 2), regexp_split(s, '-') FROM (VALUES 'abc 12-34', 'x', NULL) AS t(s) ORDER BY 1",
    r"SELECT regexp_replace('aaa', 'a*', 'X'), regexp_extract_all('aaa', 'a*'), regexp_split('a1b2c', '\d'), regexp_replace('abc', '')",
    "SELECT regexp_extract('abc', '(x)?c', 1), regexp_extract('abc', 'z')",
    "SELECT regexp_extract('abc', '(b)', 2)",
    r"SELECT regexp_replace('Ação 123', '[^\w]', '_'), regexp_like('ção', '^\w+$'), regexp_replace('a.b', '.', '\$')",
    "SELECT regexp_replace('abc', '(b)', '$2')",
    "SELECT to_hex(x), from_hex(to_hex(x)), length(x), md5(x), sha1(x), sha256(x), xxhash64(x), crc32(x), to_base64(x), to_base64url(x) FROM (VALUES X'', X'00FF10', X'DEADBEEF', NULL) AS t(x) ORDER BY 3",
    "SELECT from_big_endian_64(substr(x, 3)), to_hex(x), CAST(from_big_endian_64(substr(x, 3)) % 2147483647 AS INTEGER) FROM (VALUES X'00000000000000000001', X'0000FFFFFFFFFFFFFFFF', X'00007FFFFFFFFFFFFFFF', NULL) AS t(x) ORDER BY 1",
    "SELECT from_big_endian_64(X'0001')",
    "SELECT from_big_endian_32(X'FFFFFFFE'), to_big_endian_32(-2), to_big_endian_64(1), from_ieee754_64(to_ieee754_64(1.5E0)), to_ieee754_32(REAL '1.5')",
    "SELECT from_hex('ABC')",
    "SELECT from_hex('zz')",
    "SELECT from_base64('YWJj'), from_base64url('_-8'), from_base64('YQ')",
    "SELECT from_base64('***')",
    "SELECT hmac_sha256(X'61', X'6B6579'), sha512(X'61'), md5(to_utf8('ção'))",
    "SELECT max(x), min(x), to_hex(max(x)) FROM (VALUES X'00', X'00FF', X'FF', X'', NULL) AS t(x)",
    "SELECT round(x), round(x, 1), round(x, -1), ceil(x), floor(x), truncate(x), abs(x), sign(x) FROM (VALUES 2.5, -2.5, 15.55, -0.05, NULL) AS t(x) ORDER BY 1",
    "SELECT round(x), round(x, 1), ceil(x), floor(x), truncate(x), sign(x) FROM (VALUES 2.5E0, -2.5E0, 0.5E0, -0.5E0, 1.45E0, NULL) AS t(x) ORDER BY 1",
    "SELECT round(x), round(x, -1), abs(x), x / 2, x % 3, -x FROM (VALUES 7, -7, 15, NULL) AS t(x) ORDER BY 1",
    "SELECT round(CAST(x AS decimal(18,6)), 2), CAST(x AS decimal(18,6)) FROM (VALUES 1.005, 2.675, -1.005, 123456789.9999995) AS t(x) ORDER BY 1",
    "SELECT abs(-9223372036854775808)",
    "SELECT 2147483647 + 1",
    "SELECT CAST(2147483647 AS bigint) + 1, 9223372036854775807",
    "SELECT 9223372036854775807 + 1",
    "SELECT -(-9223372036854775807 - 1)",
    "SELECT x * 2 FROM (VALUES 2147483647, 1) AS t(x)",
    "SELECT try(x * 2) FROM (VALUES 2147483647, 1, NULL, -1073741825, 5) AS t(x)",
    "SELECT x / y, x % y, try(x / y), mod(x, y) FROM (VALUES (7, 2), (-7, 2), (7, -2), (0, 5)) AS t(x, y) ORDER BY 1, 2",
    "SELECT try(x / y) FROM (VALUES (7, 0), (8, 2), (1, 0), (9, 3), (NULL, 0)) AS t(x, y)",
    "SELECT 1 / 0",
    "SELECT 1.0 / 0",
    "SELECT 1E0 / 0, -1E0 / 0, 0E0 / 0",
    "SELECT 0.1 + 0.2, 1.0 / 3, 10 / 4, 10 / 4.0, 1.5 * 1.5, 3.0 % 2, DECIMAL '1' / DECIMAL '3', 2.5E0 * 2",
    "SELECT CAST(x AS integer), CAST(x AS bigint), CAST(x AS smallint), CAST(x AS decimal(4,1)) FROM (VALUES 2.5E0, -2.5E0, 3.49E0, NULL) AS t(x) ORDER BY 1",
    "SELECT CAST(x AS integer) FROM (VALUES 2.5, -2.5, 99.99) AS t(x) ORDER BY 1",
    "SELECT CAST(nan() AS integer)",
    "SELECT CAST(s AS integer) FROM (VALUES '12', '-0', '+7') AS t(s) ORDER BY 1",
    "SELECT CAST(' 12' AS integer)",
    "SELECT CAST('1.0' AS integer)",
    "SELECT CAST(s AS double) FROM (VALUES '1.5', '1e3', 'NaN', '-Infinity', ' 2 ') AS t(s)",
    "SELECT CAST(s AS decimal(5,2)) FROM (VALUES '1.005', '-1.005', '999.994', '1e2') AS t(s) ORDER BY 1",
    "SELECT CAST('999.995' AS decimal(5,2))",
    "SELECT CAST(s AS boolean) FROM (VALUES 'true', 'FALSE', 't', '0', '1') AS t(s) ORDER BY 1",
    "SELECT CAST('yes' AS boolean)",
    "SELECT CAST(x AS varchar) FROM (VALUES 1.5E0, 1E10, 1E-5, 0E0, -0E0, 123456789.123E0, 1E7, 1E6, NULL) AS t(x) ORDER BY 1",
    "SELECT CAST(x AS varchar) FROM (VALUES REAL '1.5', REAL '1.1', REAL '1E10') AS t(x) ORDER BY 1",
    "SELECT CAST(x AS varchar), CAST(y AS varchar), CAST(b AS varchar) FROM (VALUES (DECIMAL '1.50', 12, true), (DECIMAL '-0.01', -3, false)) AS t(x, y, b) ORDER BY 1",
    "SELECT CAST(x AS varchar(3)) FROM (VALUES 'abcdef', 'ab') AS t(x) ORDER BY 1",
    "SELECT CAST(123456 AS varchar(3))",
    "SELECT try_cast(s AS integer), try_cast(s AS date), try_cast(s AS double) FROM (VALUES '1', 'x', '2020-01-01', NULL) AS t(s) ORDER BY 1, 2",
    "SELECT greatest(a, b), least(a, b), greatest(a, b, 0), greatest(1, 2.5), least(1.5E0, 2) FROM (VALUES (1, 2), (NULL, 2), (3, NULL), (-1, -2)) AS t(a, b) ORDER BY 1, 2",
    "SELECT greatest(nan(), 1E0), least(nan(), 1E0), greatest(1E0, infinity())",
    "SELECT GREATEST(COALESCE(a, 0), COALESCE(b, 0)) FROM (VALUES (1, NULL), (NULL, NULL), (2, 3)) AS t(a, b) ORDER BY 1",
    "SELECT if(x > 1, 'big'), if(x > 1, 'big', 'small'), nullif(x, 2), coalesce(x, 10), x IS DISTINCT FROM 2 FROM (VALUES 1, 2, 3, NULL) AS t(x) ORDER BY 1",
    "SELECT if(x = 0, 0, 10 / x) FROM (VALUES 0, 5) AS t(x) ORDER BY 1",
    "SELECT CASE WHEN x = 0 THEN 0 ELSE 10 / x END FROM (VALUES 0, 5) AS t(x) ORDER BY 1",
    "SELECT power(x, 2), sqrt(x), cbrt(x), ln(x), log10(x), log2(x), exp(0), mod(x, 3), x % 2.5 FROM (VALUES 4E0, -8E0, 0E0, NULL) AS t(x) ORDER BY 1",
    "SELECT power(0, -1), power(-8, 1E0 / 3), sqrt(-1), ln(0), ln(-1)",
    "SELECT width_bucket(x, 0, 10, 5), is_nan(x), is_finite(x), is_infinite(x) FROM (VALUES 5.5E0, -1E0, 10E0, nan(), infinity()) AS t(x)",
    "SELECT width_bucket(5, 10, 10, 5)",
    "SELECT to_base(x, 16), to_base(x, 2), from_base(to_base(x, 36), 36) FROM (VALUES 255, -255, 0) AS t(x) ORDER BY 3",
    "SELECT bitwise_and(x, 12), bitwise_or(x, 3), bitwise_xor(x, 5), bitwise_not(x), bit_count(x, 64), bitwise_left_shift(x, 2), bitwise_right_shift(x, 1), bitwise_right_shift_arithmetic(x, 1) FROM (VALUES 10, -10, NULL) AS t(x) ORDER BY 1",
    "SELECT pi(), e(), degrees(pi()), radians(180), sin(0), cos(0), atan2(1, 1), cosh(0), tanh(0)",
    "SELECT x = x, x < 1, x > 1, x <> x, x IS NULL FROM (VALUES nan(), 1E0) AS t(x) ORDER BY 3",
    "SELECT count(*) FROM (VALUES nan(), 2E0, 0E0) AS t(x) WHERE x > 1",
    "SELECT max(x), min(x), sum(x), avg(x), count(x) FROM (VALUES nan(), 2E0, 0E0) AS t(x)",
    "SELECT x FROM (VALUES 1E0, nan(), -infinity(), NULL, infinity(), -0E0) AS t(x) ORDER BY x",
    "SELECT sum(x), avg(x), min(x), max(x), count(x), count(*), count_if(x > 1), bool_and(x > 0), bool_or(x > 2), arbitrary(x) IS NOT NULL FROM (VALUES 1, 2, 3, NULL) AS t(x)",
    "SELECT sum(x), avg(x), max(x), count_if(x > 1), bool_and(x > 0), checksum(x), approx_distinct(x), array_agg(x), max_by(x, x), stddev(x), variance(x) FROM (VALUES 1, 2) AS t(x) WHERE x > 5",
    "SELECT g, sum(x), avg(x), typeof(avg(x)), typeof(sum(x)) FROM (VALUES (1, 1.5), (1, 2.25), (2, NULL), (2, -1.10)) AS t(g, x) GROUP BY g ORDER BY 1",
    "SELECT sum(x) FROM (VALUES 9223372036854775807, 1) AS t(x)",
    "SELECT sum(x) FROM (VALUES 2147483647, 2147483647) AS t(x)",
    "SELECT avg(x) FROM (VALUES 9223372036854775807, 9223372036854775807) AS t(x)",
    "SELECT max_by(v, k), min_by(v, k), max_by(v, k, 2), min_by(v, k, 2) FROM (VALUES ('a', 1), ('b', 3), ('c', NULL), (NULL, 5), ('e', 2)) AS t(v, k)",
    "SELECT g, max_by(v, k) FROM (VALUES (1, 'a', 1), (1, 'b', 1), (2, 'c', NULL), (2, 'd', NULL)) AS t(g, v, k) GROUP BY g ORDER BY 1",
    "SELECT checksum(s) FROM (VALUES 'a', 'a', 'b') AS t(s)",
    "SELECT checksum(d), checksum(ts), checksum(b), checksum(f) FROM (VALUES (DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00.123', X'00FF', 1.5E0), (NULL, NULL, NULL, NULL)) AS t(d, ts, b, f)",
    "SELECT approx_percentile(x, 0.5), approx_percentile(x, 0.95), approx_percentile(x, ARRAY[0.0, 1.0]), approx_percentile(x, 2, 0.5) FROM (VALUES 1E0, 2E0, 3E0, 4E0, 100E0, NULL) AS t(x)",
    "SELECT approx_percentile(x, 0.5), typeof(approx_percentile(x, 0.5)) FROM (VALUES 1, 2, 3, 4) AS t(x)",
    "SELECT approx_percentile(x, 1.5) FROM (VALUES 1E0) AS t(x)",
    "SELECT CAST(approx_percentile(x, 0.95) AS bigint) FROM (VALUES 10, 20, 30) AS t(x)",
    "SELECT histogram(x), map_agg(x, y), array_agg(DISTINCT x ORDER BY x) FROM (VALUES (1, 'a'), (1, 'b'), (2, 'c')) AS t(x, y)",
    "SELECT stddev(x), stddev_pop(x), variance(x), var_pop(x), skewness(x), kurtosis(x), corr(x, y), covar_pop(x, y), regr_slope(y, x), regr_intercept(y, x) FROM (VALUES (1E0, 2E0), (2E0, 4.1E0), (3E0, 5.9E0), (4E0, 8.2E0)) AS t(x, y)",
    "SELECT stddev(x), variance(x), stddev_pop(x) FROM (VALUES 5) AS t(x)",
    "SELECT x, stddev_pop(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), row_number() OVER (ORDER BY x DESC), rank() OVER (ORDER BY x % 2), ntile(3) OVER (ORDER BY x), lag(x, 2, -1) OVER (ORDER BY x), first_value(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM (VALUES 1, 2, 3, 4, 5) AS t(x) ORDER BY x",
    "SELECT ARRAY[1, 2][3]",
    "SELECT ARRAY[1, 2][0]",
    "SELECT element_at(ARRAY[1, 2], 0)",
    "SELECT contains(ARRAY[1, NULL], 2), contains(ARRAY[1, NULL], 1), array_position(ARRAY[1, NULL], NULL), arrays_overlap(ARRAY[1, NULL], ARRAY[2]), arrays_overlap(ARRAY[1], ARRAY[1, NULL])",
    "SELECT array_union(ARRAY[1, 2, 2], ARRAY[3, 1]), array_intersect(ARRAY[1, 2, 2, NULL], ARRAY[2, NULL]), array_except(ARRAY[1, 2, 2, 3], ARRAY[3]), flatten(ARRAY[ARRAY[1], NULL, ARRAY[2, 3]]), concat(ARRAY[1], ARRAY[2, 3]), ARRAY[1] || 2, repeat('a', 3), sequence(1, 7, 3), sequence(5, 1), sequence(DATE '2020-02-27', DATE '2020-03-01')",
    "SELECT array_join(ARRAY[1, NULL, 3], '-'), array_join(ARRAY[1, NULL, 3], '-', 'x'), array_join(ARRAY[1.5E0, 2E0], ','), array_join(ARRAY[DATE '2020-01-01'], ',')",
    "SELECT sequence(1, 5, 0)",
    "SELECT map(ARRAY['a', 'b'], ARRAY[1, 2])['b'], element_at(map(ARRAY['a'], ARRAY[1]), 'z'), map_keys(map(ARRAY['b', 'a'], ARRAY[1, 2])), map_values(map_concat(map(ARRAY['a'], ARRAY[1]), map(ARRAY['a'], ARRAY[9]))), cardinality(map(ARRAY[1], ARRAY[NULL]))",
    "SELECT map(ARRAY['a', 'a'], ARRAY[1, 2])",
    "SELECT map(ARRAY['a'], ARRAY[1])['z']",
    "SELECT map(ARRAY[NULL], ARRAY[1])",
    "SELECT map_from_entries(ARRAY[ROW('a', 1), ROW('b', 2)]), map_entries(map(ARRAY['x'], ARRAY[1])), split_to_map('a=1&b=2', '&', '=')",
    r#"SELECT json_extract_scalar(j, '$.a'), json_extract_scalar(j, '$.b[1]'), json_extract(j, '$.b'), json_extract_scalar(j, '$.c.d'), json_array_length(json_extract(j, '$.b')), json_size(j, '$.b'), json_extract_scalar(j, '$["e f"]') FROM (VALUES '{"a": 1, "b": [10, "x", null], "c": {"d": true}, "e f": "g"}', '{"a": "str"}', 'not json', NULL) AS t(j) ORDER BY 1"#,
    r#"SELECT json_parse('{"b":1, "a":[1,2.50,"x"]}'), json_format(json_parse('[1, 2]')), CAST(json_parse('{"a":1}') AS varchar), json_extract_scalar('{"a":1.0}', '$.a'), json_extract_scalar('{"a":{"b":1}}', '$.a')"#,
    r#"SELECT json_parse('{"a":')"#,
    r#"SELECT CAST(json_parse('[1,2,null]') AS array(bigint)), CAST(json_parse('{"a":1,"b":null}') AS map(varchar, integer)), CAST(json_parse('"12"') AS integer), CAST(json_parse('true') AS varchar), CAST(json_parse('1.5') AS decimal(3,1))"#,
    "SELECT CAST(ARRAY[1, NULL] AS json), CAST(map(ARRAY['k'], ARRAY[1.5E0]) AS json), CAST('x' AS json), CAST(1.5 AS json), json_array_contains('[1, 2]', 2), json_array_get('[1, [2]]', 1), is_json_scalar('1')",
    r#"SELECT CAST(json_parse('"abc"') AS integer)"#,
    "SELECT url_extract_host(u), url_extract_port(u), url_extract_path(u), url_extract_query(u), url_extract_fragment(u), url_extract_protocol(u), url_extract_parameter(u, 'b') FROM (VALUES 'https://user@example.com:8080/a/b%20c?x=1&b=2&b=3#frag', 'mailto:a@b.c', 'not a url', NULL) AS t(u) ORDER BY 1",
    "SELECT url_encode('a b&c/ç~'), url_decode('a+b%26c%2F%C3%A7')",
    "SELECT x FROM (VALUES 1, 2) AS t(x) WHERE try(10 / (x - 1)) IS NULL",
    "SELECT sum(try(10 / x)) FROM (VALUES 0, 1, 2, 0) AS t(x)",
    "SELECT typeof(x), x FROM (VALUES 1, 2) AS t(x) ORDER BY 2",
    "SELECT * FROM (VALUES 1, 2) AS t(x) ORDER BY 1",
    "SELECT 'VALUES 1, 2' AS s, 'CAST(a AS array(bigint))' AS c",
    r#"SELECT x AS "VALUES 1, 2" FROM (VALUES 5) AS t(x)"#,
    "SELECT CAST(ARRAY[1, 2] AS array(varchar)), CAST(ARRAY['1', '2'] AS array(integer)), CAST(ROW(1, 'a') AS row(x bigint, y varchar))",
    "SELECT X'0A0b' , typeof(X'00'), INTERVAL '1-2' YEAR TO MONTH IS NOT NULL",
    "SELECT a, a + 1, count(*) FROM (VALUES 1, 2, 2) AS t(a) GROUP BY a ORDER BY 1",
    "SELECT 1 AS x, x + 1 FROM (VALUES 1) AS t(x)",
    "SELECT (SELECT 1), 1 + 1, 'a' || 'b', CAST(1 AS bigint), lower('A')",
    "SELECT DATE '2020-01-01' - DATE '2019-01-01' IS NOT NULL",
    "SELECT current_timezone(), typeof(current_timestamp), typeof(localtimestamp), typeof(now()), typeof(current_date)",
    "SELECT date_diff('second', localtimestamp, CAST(current_timestamp AS timestamp(3))) BETWEEN -1 AND 1, current_date = CAST(localtimestamp AS date)",
    "SELECT date_trunc('day', date_add('day', -7, localtimestamp)) <= localtimestamp, typeof(date_add('day', -7, localtimestamp))",
    "SELECT CAST(current_timestamp AT TIME ZONE 'America/Sao_Paulo' AS timestamp(3)) < localtimestamp",
    "SELECT typeof(CAST(current_timestamp AT TIME ZONE 'America/Sao_Paulo' AS timestamp(3))), typeof(current_timestamp AT TIME ZONE 'America/Sao_Paulo')",
    "SELECT date_format(x, '%Y-%m-%d %H:%i:%s.%f'), date_format(x, '%a %b %e %c %j %k %l %p %r %T %v %x %W %M %y %h %I %S %u') FROM (VALUES TIMESTAMP '2020-02-29 13:05:09.123', TIMESTAMP '2021-01-03 00:00:00.000', NULL) AS t(x)",
    r#"SELECT json_parse('{"b":1, "a":[1,2.50,"x"]}'), json_format(json_parse('[1, 2]')), json_extract_scalar('{"a":1.0}', '$.a'), json_extract_scalar('{"a":{"b":1}}', '$.a'), json_extract_scalar('{"a":"ç"}', '$.a'), json_extract_scalar('{"a":1e400}', '$.a')"#,
    "SELECT width_bucket(x, 0, 10, 5), is_nan(x), is_finite(x), is_infinite(x) FROM (VALUES 5.5E0, -1E0, 10E0, infinity()) AS t(x) ORDER BY 1",
    "SELECT cardinality(a), element_at(a, 1), element_at(a, -1), element_at(a, 10), contains(a, 2), array_position(a, 3), array_max(a), array_min(a), array_distinct(a), array_sort(a), array_remove(a, 2), slice(a, 2, 2), reverse(a) FROM (VALUES (ARRAY[3, 1, 2, 1]), (ARRAY[1, NULL, 2]), (ARRAY[]), (NULL)) AS t(a)",
    "SELECT a[2], a[1] FROM (VALUES (ARRAY[10, 20]), (ARRAY[5, 6, 7])) AS t(a) ORDER BY 2",
    "SELECT to_hex(md5(to_utf8(array_join(array_agg(CAST(x AS varchar) ORDER BY x DESC), ',')))), to_hex(md5(to_utf8(listagg(CAST(x AS varchar), ',') WITHIN GROUP (ORDER BY x)))) FROM UNNEST(sequence(1, 30000)) AS t(x)",
    "SELECT checksum(format_datetime(from_unixtime(x * 1800, 'America/Sao_Paulo'), 'yyyy-MM-dd HH:mm ZZ')), checksum(hour(from_unixtime(x * 1800, 'America/Sao_Paulo'))), checksum(CAST(from_unixtime(x * 1800, 'America/Sao_Paulo') AS timestamp(3))), checksum(date_trunc('day', from_unixtime(x * 1800, 'America/Sao_Paulo'))), checksum(date_add('day', 1, from_unixtime(x * 1800, 'America/Sao_Paulo'))) FROM UNNEST(sequence(1480000, 1530000)) AS t(x)",
    "SELECT with_timezone(TIMESTAMP '2018-11-04 00:30:00.000', 'America/Sao_Paulo')",
    "SELECT count(*) FROM (VALUES 2147483647) AS a(x) JOIN (VALUES -2147483648) AS b(y) ON x + 1 = y",
    "SELECT sum(x) OVER (ORDER BY x) FROM (VALUES 9223372036854775807, 1) AS t(x)",
    "SELECT g, CHECKSUM(CASE WHEN p IS NOT NULL AND q IS NOT NULL THEN CAST(pv AS VARCHAR) END), CHECKSUM(CASE WHEN p IS NOT NULL AND q IS NOT NULL THEN CAST(qv AS VARCHAR) END) FROM (SELECT COALESCE(a.g, b.g) AS g, a.id AS p, b.id AS q, a.v AS pv, b.v AS qv FROM (VALUES (1, 10, DECIMAL '1.50'), (1, 11, DECIMAL '2.00'), (2, 20, NULL)) AS a(g, id, v) FULL OUTER JOIN (VALUES (1, 10, DECIMAL '1.50'), (1, 12, DECIMAL '9.99'), (2, 20, DECIMAL '0.10')) AS b(g, id, v) ON a.id = b.id) GROUP BY g ORDER BY g",
    "SELECT date_add('month', 1, TIMESTAMP '2020-01-31 10:00:00 America/Sao_Paulo'), date_diff('day', TIMESTAMP '2020-01-01 00:00:00 UTC', TIMESTAMP '2020-01-02 00:00:00 America/Sao_Paulo'), TIMESTAMP '2020-01-01 00:00:00 UTC' AT TIME ZONE '+05:30', with_timezone(TIMESTAMP '2020-01-01 00:00:00', '+05:00')",
    "SELECT CAST(TIMESTAMP '2020-01-01 01:00:00 America/Sao_Paulo' AS date), CAST(TIMESTAMP '2020-01-01 01:00:00 America/Sao_Paulo' AS timestamp(3)), hour(TIMESTAMP '2020-01-01 01:00:00 America/Sao_Paulo'), timezone_hour(TIMESTAMP '2020-01-01 01:00:00 America/Sao_Paulo'), to_unixtime(TIMESTAMP '2020-01-01 01:00:00 America/Sao_Paulo')",
    "SELECT parse_datetime('2020-02-29 10:00', 'yyyy-MM-dd HH:mm'), CAST('2020-01-01 10:00:00 America/Sao_Paulo' AS timestamp with time zone), CAST('2020-01-01 10:00:00 UTC' AS timestamp(3) with time zone)",
    r"SELECT regexp_like('Ação', '^\p{L}+$'), regexp_replace('AbA', '(?i)a', 'x'), regexp_extract_all('a1b22c333', '\d+', 0), regexp_like('a\nb', 'a.b'), regexp_like('abc', '^b')",
    "SELECT split('abc', '')",
    "SELECT typeof(NULL), typeof(NULL + 1), typeof(CAST(NULL AS varchar)), NULL IS NULL, typeof(ARRAY[]), typeof(ARRAY[NULL])",
    "SELECT round(2.675E0, 2), round(1.005E0, 2), round(-1.5E0), round(0.5E0), ceil(-0.5E0), floor(-0.0E0), round(-0.4E0), truncate(-1.99E0), truncate(1.99, 1), round(1.99, 1), round(-1.95, 1)",
    "SELECT mod(-7, 3), mod(-7.5, 2), mod(-7E0, 3), 7E0 % 0E0, -7 % 3, mod(7, -3)",
    "SELECT DECIMAL '1.00' / DECIMAL '3', -2.5 / 2, -2.4 / 2, 2.5 / 2, CAST(1.25 AS decimal(2,1)), CAST(-1.25 AS decimal(2,1)), CAST(-1.35E0 AS decimal(2,1)), typeof(DECIMAL '1.00' / DECIMAL '3')",
    "SELECT CAST(99.9 AS decimal(2,1))",
    "SELECT DECIMAL '9999999999999999999999999999999999999' * 10",
    "SELECT x % 2, sum(x) FROM (VALUES 1, 2) AS t(x) WHERE x > 5 GROUP BY x % 2",
    "SELECT typeof(x), x FROM UNNEST(sequence(1, 3)) AS t(x) ORDER BY 2",
    "SELECT x, y FROM UNNEST(ARRAY[1, 2], ARRAY['a']) AS t(x, y) ORDER BY 1",
    "SELECT map_agg(x, 1) FROM (VALUES 1, 1) AS t(x)",
    "SELECT x, count(*) FROM (VALUES DECIMAL '1.0', DECIMAL '1.00', NULL, NULL) AS t(x) GROUP BY x ORDER BY 1",
    "SELECT count(*) FROM (VALUES ('a', 1), ('a', 1)) AS t(s, n) GROUP BY s, n",
    r#"SELECT json_extract('{"a":1}', 'a')"#,
    r#"SELECT json_extract('{"a":{"b":[1,{"c":2}]}}', '$.a.b[1].c'), json_extract_scalar('{"a":[1,2]}', '$.a[-1]'), json_extract_scalar('{"a":true}', '$.a'), json_extract_scalar('{"a":null}', '$.a'), json_extract('[1,2]', '$[*]')"#,
    "SELECT url_extract_host('http://[::1]:8080/x'), url_extract_port('http://a.com'), url_extract_parameter('http://a.com/?x=%20y&x=2', 'x'), url_extract_path('http://a.com'), url_extract_query('http://a.com/?'), url_extract_protocol('HTTP://a.com')",
    "SELECT CAST(json_parse('[1.5]') AS array(integer))",
    "SELECT CAST(json_parse('300') AS tinyint)",
    "SELECT try_cast('128' AS tinyint), try_cast('1e2' AS integer), try_cast('0x10' AS integer), try_cast('' AS integer), try_cast('  1.5 ' AS double), try_cast('1,5' AS double)",
    "SELECT CAST(x AS varchar) FROM (VALUES 1E-4, 1.0E-3, 0.001E0, 9999999E0, 10000000E0, 1E21, 123456789012345680000E0, 5E-324, 1.7976931348623157E308) AS t(x) ORDER BY x",
    "SELECT CAST(x AS varchar) FROM (VALUES REAL '1E-4', REAL '1E7', REAL '9999999', REAL '0.1', REAL '3.4028235E38') AS t(x) ORDER BY x",
    "SELECT CAST(TIMESTAMP '2020-01-01 10:00:00.123456' AS varchar), CAST(TIMESTAMP '2020-01-01 10:00:00 America/Sao_Paulo' AS varchar), CAST(DATE '2020-01-01' AS varchar), CAST(X'00' AS varchar)",
    "SELECT CAST(DATE '2020-01-01' AS varchar), CAST(TIMESTAMP '2020-01-01 10:00:00.123456' AS varchar), CAST(TIMESTAMP '2020-01-01 10:00:00 America/Sao_Paulo' AS varchar)",
    "SELECT CAST(1E0 AS real), CAST(3.4028236E38 AS real), CAST(1.23456789E0 AS real), CAST(REAL '1.1' AS double)",
    "SELECT typeof(1 + 1.5E0), typeof(CAST(1 AS tinyint) + CAST(1 AS smallint)), typeof(CAST(1 AS tinyint) * 2), typeof(REAL '1' + 1), typeof(REAL '1' + 1E0), typeof(1 / 2), typeof(CAST(1 AS bigint) + 1.5)",
    "SELECT CAST(127 AS tinyint) + CAST(1 AS tinyint)",
    "SELECT CAST(32767 AS smallint) + CAST(1 AS smallint)",
    "SELECT -CAST(-128 AS tinyint)",
    "SELECT CAST(-9223372036854775807 - 1 AS bigint) / -1",
    "SELECT CAST(-2147483648 AS integer) % -1, 5 % -3",
    "SELECT abs(CAST(-128 AS tinyint))",
    "SELECT sign(-0E0), sign(nan()), abs(-0E0), sqrt(-0E0), power(2, 1024), power(-2, 0.5E0), exp(710E0), ln(1E0)",
    "SELECT cosine_similarity(map(ARRAY['a', 'b'], ARRAY[1E0, 2E0]), map(ARRAY['a', 'c'], ARRAY[1E0, 3E0])), normal_cdf(0, 1, 1.96E0), inverse_normal_cdf(0, 1, 0.975E0), wilson_interval_lower(3, 10, 1.96E0), from_base('ff', 16), to_base(-1, 36)",
    "SELECT from_base('zz', 10)",
    "SELECT from_base('8000000000000000', 16)",
    "SELECT bitwise_left_shift(1, 64), bitwise_left_shift(CAST(1 AS integer), 31), bitwise_right_shift(-1, 63), bit_count(-1, 8), bitwise_right_shift_arithmetic(-8, 70)",
    "SELECT bit_count(128, 8)",
    "SELECT reverse('😀a'), length('😀'), substr('😀abc', 2, 1), strpos('a😀b', 'b'), lpad('😀', 3, 'x'), chr(128512) = '😀', codepoint('😀'), upper('ǆ'), lower('ΣΑΣ')",
    "SELECT soundex(''), soundex('Tymczak'), soundex('Ashcraft'), soundex('ção'), luhn_check(''), luhn_check('a1')",
    "SELECT normalize('ﬁ', NFKC), normalize('é'), normalize(NULL)",
    "SELECT format_datetime(TIMESTAMP '2020-01-05 00:00:00.000', 'G C Y x w e E k K h H S SS SSSSSS'), format_datetime(TIMESTAMP '2020-01-05 13:07:08.100', 'h:mm a, ''quoted''')",
    "SELECT date_format(TIMESTAMP '2020-01-05 13:07:08.100', '%U')",
    "SELECT date_parse('2020-02-29', '%Y-%m-%d'), date_parse('29/Feb/2020 13', '%d/%b/%Y %H'), date_parse('2020 60', '%Y %j'), date_parse('2020-02-29 10:00:00.123456', '%Y-%m-%d %H:%i:%s.%f'), date_parse('1/1/20', '%m/%d/%y')",
    "SELECT date_parse('2020-01-01 extra', '%Y-%m-%d')",
    "SELECT date_parse('13/01/2020', '%m/%d/%Y')",
    "SELECT last_day_of_month(TIMESTAMP '2020-02-10 10:00:00'), last_day_of_month(TIMESTAMP '2019-11-03 10:00:00 America/Sao_Paulo'), day_of_week(TIMESTAMP '2019-11-03 00:30:00 America/Sao_Paulo'), date(TIMESTAMP '2020-01-01 23:00:00 America/Sao_Paulo')",
    "SELECT year_of_week(DATE '2021-01-01'), week(DATE '2021-01-01'), year_of_week(DATE '2019-12-30'), week(DATE '2019-12-30'), millisecond(TIMESTAMP '2020-01-01 00:00:00.999'), second(TIMESTAMP '2020-01-01 00:00:59.999')",
    "SELECT date_add('second', 1, DATE '2020-01-01')",
    "SELECT date_diff('millisecond', TIMESTAMP '2020-01-01 00:00:00.001', TIMESTAMP '2020-01-01 00:00:00.000'), date_diff('month', DATE '2020-01-31', DATE '2020-02-29'), date_diff('month', DATE '2020-02-29', DATE '2020-01-31'), date_diff('year', DATE '2020-02-29', DATE '2021-02-28')",
    "SELECT date_trunc('minute', TIMESTAMP '2020-01-01 10:10:59.999999'), date_trunc('second', TIMESTAMP '2020-01-01 10:10:59.999999'), date_trunc('millisecond', TIMESTAMP '2020-01-01 10:10:59.999999'), date_trunc('month', DATE '2020-02-29')",
    "SELECT date_trunc('hour', DATE '2020-01-01')",
    "SELECT from_unixtime(x) FROM (VALUES 1.9999E0, -0.0005E0, 253402300800E0) AS t(x) ORDER BY 1",
    "SELECT localtimestamp IS NOT NULL, current_date - INTERVAL '1' DAY < current_date, typeof(localtime), typeof(current_date)",
    r#"SELECT json_extract_scalar(j, '$.a'), json_extract(j, '$.a'), json_size(j, '$'), json_array_length(j) FROM (VALUES '[1,2]', '{"a":[]}', '{"a":null}', '"s"') AS t(j)"#,
    "SELECT format_datetime(TIMESTAMP '2020-06-01 10:00:00 America/Sao_Paulo', 'yyyy-MM-dd HH:mm ZZ ZZZ Z')",
    "SELECT x FROM (VALUES map(ARRAY['a'], ARRAY[1])) AS t(x)",
    "SELECT x, y FROM (VALUES (1, ARRAY['a,b']), (2, ARRAY['c'])) AS t(x, y) ORDER BY 1",
    "SELECT 'x' AS a FROM (VALUES 1, 2) AS t(x) ORDER BY 1",
    "SELECT x FROM (VALUES 'a, b', 'c') AS t(x) ORDER BY 1",
    "SELECT * FROM (VALUES 1, 2) AS t(x) WHERE x IN (SELECT * FROM (VALUES 2, 3) AS u(y))",
    "SELECT typeof(x), x FROM (VALUES 1, 2.5) AS t(x) ORDER BY 2",
    "SELECT x FROM (VALUES 'a', NULL, 'b') AS t(x) ORDER BY x DESC NULLS LAST",
    "SELECT approx_percentile(x, 0.5) FROM (VALUES 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AS t(x)",
    "SELECT approx_percentile(x, w, 0.5) FROM (VALUES (1, 1), (2, 1), (3, 100)) AS t(x, w)",
    "SELECT checksum(x), checksum(y) FROM (VALUES (nan(), REAL 'NaN')) AS t(x, y)",
    "SELECT checksum(x) FROM (VALUES TIMESTAMP '2020-01-01 00:00:00.123456', TIMESTAMP '2020-01-01 00:00:00.000001') AS t(x)",
    "SELECT checksum(x) FROM (VALUES TIMESTAMP '2020-01-01 00:00:00.123 America/Sao_Paulo', TIMESTAMP '2020-01-01 00:00:00.123 America/Sao_Paulo') AS t(x)",
    "SELECT checksum(x) FROM (VALUES true, false, NULL) AS t(x)",
    "SELECT max_by(x, y), min_by(x, y), max_by(x, y, 5), min_by(x, y, 2) FROM (VALUES (1, 'b'), (2, 'a'), (3, 'b'), (4, NULL), (NULL, 'c')) AS t(x, y)",
    "SELECT max_by(x, y, 0) FROM (VALUES (1, 2)) AS t(x, y)",
    "SELECT arbitrary(x), any_value(x) FROM (VALUES NULL, 5, NULL) AS t(x)",
    "SELECT count_if(x), bool_and(x), bool_or(x), every(x) FROM (VALUES true, NULL, false) AS t(x)",
    "SELECT listagg(x, ', ') WITHIN GROUP (ORDER BY x) FROM (VALUES 'b', NULL, 'a') AS t(x)",
    "SELECT listagg(DISTINCT x, ',') WITHIN GROUP (ORDER BY x) FROM (VALUES 'b', 'a', 'b') AS t(x)",
    "SELECT geometric_mean(x) FROM (VALUES -1, 2) AS t(x)",
    "SELECT variance(x), stddev_samp(x), var_pop(x), typeof(variance(x)) FROM (VALUES DECIMAL '1.5', DECIMAL '2.5', DECIMAL '3.5') AS t(x)",
    "SELECT avg(x), typeof(avg(x)), sum(x), typeof(sum(x)) FROM (VALUES CAST(1 AS tinyint), CAST(2 AS tinyint)) AS t(x)",
    "SELECT avg(x), sum(x) FROM (VALUES DECIMAL '0.005', DECIMAL '0.004') AS t(x)",
    "SELECT avg(x) FROM (VALUES DECIMAL '1', DECIMAL '2') AS t(x)",
    "SELECT avg(x), typeof(avg(x)) FROM (VALUES DECIMAL '1.00', DECIMAL '2.01', DECIMAL '2.00') AS t(x)",
    "SELECT avg(x) FROM (VALUES DECIMAL '-1.00', DECIMAL '-2.01', DECIMAL '-2.00') AS t(x)",
    "SELECT x, count(*) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW), sum(x) OVER (ORDER BY x ROWS 1 PRECEDING), avg(x) OVER (PARTITION BY x % 2), max_by(x, -x) OVER (), checksum(x) OVER () FROM (VALUES 1, 2, 3, 5) AS t(x) ORDER BY x",
    "SELECT typeof(ntile(2) OVER (ORDER BY x)), typeof(row_number() OVER ()), typeof(rank() OVER (ORDER BY x)), typeof(count(*) OVER ()) FROM (VALUES 1) AS t(x)",
    "SELECT date_add(u, n, ts), date_diff(u, ts, TIMESTAMP '2021-03-01 00:00:00'), date_trunc(u, ts) FROM (VALUES ('day', 1, TIMESTAMP '2020-02-28 10:00:00'), ('month', -1, TIMESTAMP '2020-03-31 10:00:00'), ('hour', 5, NULL), (NULL, 1, TIMESTAMP '2020-01-01 00:00:00')) AS t(u, n, ts) ORDER BY 1",
    "SELECT format_datetime(ts, f), date_format(ts, g), date_parse(s, g) FROM (VALUES (TIMESTAMP '2020-02-29 10:11:12.345', 'yyyy', '%Y', '2021'), (TIMESTAMP '2020-02-29 10:11:12.345', 'HH:mm', '%H:%i', '07:08'), (NULL, 'x', '%Y', NULL)) AS t(ts, f, g, s) ORDER BY 1",
    r"SELECT regexp_like(s, p), regexp_replace(s, p, r), regexp_extract(s, p), split_part(s, d, i), lpad(s, n, d), strpos(s, d), replace(s, d, r) FROM (VALUES ('a1b2', '\d', '#', ',', 1, 6), ('x,y,z', '[a-z]', '', ',', 3, 2), ('abc', 'b+', 'B', 'b', 2, 1)) AS t(s, p, r, d, i, n) ORDER BY 1",
    r#"SELECT round(x, d), truncate(x, d), json_extract_scalar(j, p), url_extract_parameter(u, k) FROM (VALUES (1.2345, 2, '{"a":1,"b":"z"}', '$.a', 'http://h/?a=1&b=2', 'b'), (-9.875, 1, '{"a":1,"b":"z"}', '$.b', 'http://h/?a=1', 'a'), (5.5, 0, NULL, '$.a', NULL, 'a')) AS t(x, d, j, p, u, k) ORDER BY 1"#,
    "SELECT round(x, d) FROM (VALUES (2.675E0, 2), (1.5E0, 0), (-1.5E0, 0), (1234.5E0, -2)) AS t(x, d) ORDER BY 1",
    "SELECT count(*) FROM (VALUES 1, 2147483647) AS t(x) WHERE x + 1 > 0",
    "SELECT array_join(array_agg(s ORDER BY s DESC), ','), listagg(s, ',') WITHIN GROUP (ORDER BY s) FROM (VALUES 'a', 'b', 'c') AS t(s)",
    "SELECT array_agg(x ORDER BY x), array_agg(x ORDER BY x DESC) FROM (VALUES 1, 2, 3) AS t(x)",
    "SELECT array_agg(x ORDER BY x)[1], array_agg(x ORDER BY y)[1] FROM (VALUES (1, 3), (2, 2), (3, 1)) AS t(x, y)",
    "SELECT max_by(x, y), max_by(x, z) FROM (VALUES (1, 3, 1), (2, 2, 2), (3, 1, 3)) AS t(x, y, z)",
    "SELECT count(*) FILTER (WHERE x > 1), count(*) FILTER (WHERE x > 2), sum(x) FILTER (WHERE x > 1), sum(x) FILTER (WHERE x < 3) FROM (VALUES 1, 2, 3) AS t(x)",
    "SELECT listagg(s, ',') WITHIN GROUP (ORDER BY s), listagg(s, ';') WITHIN GROUP (ORDER BY s) FROM (VALUES 'a', 'b') AS t(s)",
    "SELECT listagg(s, ',') WITHIN GROUP (ORDER BY s), listagg(s, ',') WITHIN GROUP (ORDER BY s DESC) FROM (VALUES 'a', 'b') AS t(s)",
    "SELECT listagg(s) WITHIN GROUP (ORDER BY s) FROM (VALUES 'a', 'b') AS t(s)",
    "SELECT array_join(array_agg(s), ',') FROM (VALUES CAST(NULL AS varchar)) AS t(s)",
    "SELECT approx_distinct(x) FROM (VALUES 1, 2, 3, 1) AS t(x)",
    "SELECT x FROM (VALUES 1, 2, 3) AS t(x) ORDER BY x DESC LIMIT 2",
    "SELECT try(CAST(s AS integer)) + 1, try(CAST(s AS integer) + 2147483647) FROM (VALUES '1', 'x', '0') AS t(s) ORDER BY 1",
    "SELECT coalesce(try(1 / x), -1) FROM (VALUES 0, 1) AS t(x) ORDER BY 1",
    "SELECT try(array_agg_x) FROM (SELECT 1 AS array_agg_x)",
    "SELECT try(x / 0) FROM (VALUES 1E0, 0E0) AS t(x) ORDER BY 1",
    "SELECT x, try(ARRAY[1, 2][x]), try(element_at(map(ARRAY['a'], ARRAY[1]), 'b')), try(map(ARRAY['a'], ARRAY[1])['b']) FROM (VALUES 1, 3) AS t(x) ORDER BY 1",
    "SELECT x FROM (VALUES 1, 2, 3) AS t(x) WHERE try(ARRAY[10, 20][x]) IS NOT NULL ORDER BY 1",
    "SELECT try(CAST(x AS tinyint)) FROM (VALUES 1, 200, -129, 127) AS t(x) ORDER BY 1",
    "SELECT try(date_parse(s, '%Y-%m-%d')) FROM (VALUES '2020-01-01', '2020-13-01', 'x', NULL) AS t(s) ORDER BY 1",
    "SELECT sum(x) FROM (VALUES CAST(9223372036854775807 AS bigint), CAST(-1 AS bigint), CAST(1 AS bigint)) AS t(x)",
    "SELECT try(sum(x)) FROM (VALUES 9223372036854775807, 1) AS t(x)",
    "SELECT sum(x) FROM (VALUES REAL '3.4028235E38', REAL '3.4028235E38') AS t(x)",
    "SELECT sum(x), avg(x) FROM (VALUES 1E308, 1E308) AS t(x)",
    "SELECT typeof(sum(x)), sum(x) FROM (VALUES CAST(1 AS smallint), CAST(32767 AS smallint)) AS t(x)",
    "SELECT typeof(sum(x)), sum(x) FROM (VALUES CAST(2147483647 AS integer), CAST(2147483647 AS integer)) AS t(x)",
    "SELECT typeof(min(x)), min(x), max(x) FROM (VALUES DECIMAL '1.50', DECIMAL '-0.01') AS t(x)",
    "SELECT min(x), max(x), min(y), max(y) FROM (VALUES ('b', DATE '2020-01-02'), ('a', NULL), (NULL, DATE '2019-12-31')) AS t(x, y)",
    "SELECT max(x), min(x) FROM (VALUES (ARRAY[1, 2]), (ARRAY[1, 3]), (ARRAY[1])) AS t(x)",
    "SELECT array_agg(x ORDER BY x)[1], array_agg(x ORDER BY x DESC)[1] FROM (VALUES 1, 2, 3) AS t(x)",
    "SELECT array_join(array_agg(s ORDER BY s), ','), array_join(array_agg(s ORDER BY s DESC), ',') FROM (VALUES 'a', 'b', 'c') AS t(s)",
    "SELECT cardinality(sequence(1, 10000)), sequence(1, 10001) IS NULL",
    "SELECT sequence(1, 10001)",
    "SELECT from_iso8601_timestamp('2020-02-29T12:00:00Z'), to_iso8601(DATE '2020-02-29'), to_iso8601(TIMESTAMP '2020-02-29 12:00:00.123')",
    "SELECT count(*) FILTER (WHERE x > 1), sum(x) FILTER (WHERE x IS NULL), approx_distinct(x), geometric_mean(x) FROM (VALUES 1, 2, 3, 4) AS t(x)",
    "SELECT sum(x), typeof(sum(x)) FROM (VALUES REAL '1.5', REAL '2.25') AS t(x)",
    "SELECT array_sort(ARRAY[3, NULL, 1]), array_sort(ARRAY['b', 'A', 'a']), array_distinct(ARRAY[NULL, 1, NULL]), ngrams(ARRAY['a', 'b', 'c'], 2), combinations(ARRAY[1, 2, 3], 2), zip(ARRAY[1, 2], ARRAY['a'])",
    "SELECT try(CAST('x' AS integer)), try(1 / 0), try(from_hex('z')), try(date_parse('x', '%Y'))",
    "SELECT CAST(x AS array(bigint)) FROM (VALUES (ARRAY[1, NULL]), (NULL)) AS t(x) ORDER BY 1",
    "SELECT substr(X'0102030405', -2), substr(X'0102030405', 2, 2), length(X'0102'), concat(X'01', X'02'), reverse(X'0102'), to_hex(substr(X'0102030405', 3))",
    "SELECT array_sort(ARRAY[nan(), 1E0, -infinity(), NULL])",
    r#"SELECT CAST(ROW(1, 'a') AS json), CAST(map(ARRAY[1, 2], ARRAY['a', 'b']) AS json), CAST(json_parse('{"a":[1,2]}') AS map(varchar, array(integer))), CAST(json_parse('[1,"a"]') AS row(integer, varchar))"#,
    "SELECT levenshtein_distance('😀a', 'a'), hamming_distance('😀a', 'bb'), translate('abc', 'abc', 'x'), replace('aaa', 'aa', 'b'), starts_with('', '')",
    "SELECT to_unixtime(TIMESTAMP '1969-12-31 23:59:59.999'), from_unixtime_nanos(1500000000123456789), to_milliseconds(parse_duration('1.5s')), human_readable_seconds(-5E0)",
    "SELECT from_iso8601_timestamp('2020-02-29T12:00:00.123+05:30'), from_iso8601_timestamp('2020-02-29')",
    "SELECT corr(x, y), regr_slope(y, x), covar_samp(x, y), var_samp(x) FROM (VALUES (1E0, 1E0), (1E0, 2E0)) AS t(x, y)",
    "SELECT g, listagg(x, '') WITHIN GROUP (ORDER BY x DESC) FROM (VALUES (1, 'b'), (1, 'a'), (2, 'c')) AS t(g, x) GROUP BY g ORDER BY 1",
    "SELECT x, count(*) FROM (VALUES 1E0, 2E0, 1E0, nan()) AS t(x) GROUP BY x ORDER BY 1",
    "SELECT DATE '2020-02-29' + INTERVAL '1' YEAR, DATE '2020-01-31' + INTERVAL '1' MONTH, TIMESTAMP '2020-01-01 00:00:00.000' - INTERVAL '1' SECOND, DATE '2020-03-01' - INTERVAL '1' DAY",
    "SELECT TIMESTAMP '2019-11-02 12:00:00.000 America/Sao_Paulo' + INTERVAL '1' DAY, TIMESTAMP '2019-11-02 12:00:00.000 America/Sao_Paulo' + INTERVAL '24' HOUR",
    "SELECT x FROM (VALUES 3, NULL, 1) AS t(x) ORDER BY x DESC NULLS LAST",
    "SELECT x FROM (VALUES 3, NULL, 1) AS t(x) ORDER BY x NULLS FIRST",
    "SELECT 1 AS a -- VALUES 1, 2",
    "/* CAST(a AS map(varchar, bigint)) */ SELECT 2 AS b",
    "select x from (values 1, 2) as t(x) order by x",
    "SELECT x FROM (VALUES (1), (2)) AS t(x) ORDER BY x",
    "SELECT x FROM (VALUES 'a,b', 'c') AS t(x) ORDER BY 1",
    "SELECT x IN (1, 2.5), x = 1.0, x + 0.5 FROM (VALUES 1, 2) AS t(x) ORDER BY 3",
    "SELECT 2 * x, count(*) FROM (VALUES 1, 1, 2) AS t(x) GROUP BY 1 ORDER BY 1",
    "SELECT x FROM (VALUES 3, 1, 2) AS t(x) ORDER BY 1 DESC LIMIT 2",
    "SELECT CAST('2020-01-01 10:00:00.123456789' AS timestamp(9)), CAST(TIMESTAMP '2020-01-01 10:00:00.123456' AS timestamp(9)), date_add('microsecond', 1, TIMESTAMP '2020-01-01 10:00:00.123456')",
    "SELECT TIMESTAMP '2019-11-03 03:30:00 UTC' AT TIME ZONE 'America/Sao_Paulo', TIMESTAMP '2019-02-17 01:30:00 UTC' AT TIME ZONE 'America/Sao_Paulo'",
    "SELECT substring('abcdef' FROM 2 FOR 3), substring('abcdef' FROM 4), position('cd' IN 'abcdef'), trim(LEADING 'x' FROM 'xxaxx')",
    "SELECT EXTRACT(TIMEZONE_HOUR FROM TIMESTAMP '2020-06-01 10:00:00 America/Sao_Paulo'), EXTRACT(TIMEZONE_MINUTE FROM TIMESTAMP '2020-06-01 10:00:00 Asia/Kolkata'), EXTRACT(QUARTER FROM DATE '2020-05-01'), EXTRACT(DAY_OF_WEEK FROM DATE '2020-05-03')",
    r#"SELECT json_extract('{"a":[1,2]}', '$.a[*]'), json_extract_scalar('{"a b":1}', '$["a b"]')"#,
    "SELECT json_extract('{}', '$.[')",
    r"SELECT regexp_replace('John Smith', '(?<first>\w+) (?<last>\w+)', '${last}, ${first}'), regexp_extract_all('a1b22', '([a-z])(\d+)', 2), regexp_split('abc', ''), regexp_count('aaa', 'a*')",
    "SELECT split_to_map('a=1&a=2', '&', '=')",
    "SELECT lpad('ab', 7, 'ção'), rpad('ab', 6, '😀x'), reverse(CAST(NULL AS varchar)), length(CAST(NULL AS varchar))",
    "SELECT upper(s), lower(s), length(s) FROM (VALUES 'İstanbul', 'ΣΊΣΥΦΟΣ', 'ǅ') AS t(s) ORDER BY 3",
    "SELECT date_add('hour', 1, x), date_trunc('hour', x), hour(x) FROM (VALUES TIMESTAMP '2019-11-03 00:30:00.000 America/Sao_Paulo', TIMESTAMP '2019-02-16 23:30:00.000 America/Sao_Paulo') AS t(x) ORDER BY 1",
    "SELECT date_diff('hour', TIMESTAMP '2019-02-16 22:00:00 America/Sao_Paulo', TIMESTAMP '2019-02-17 02:00:00 America/Sao_Paulo'), date_diff('day', TIMESTAMP '2019-11-02 12:00:00 America/Sao_Paulo', TIMESTAMP '2019-11-03 12:00:00 America/Sao_Paulo')",
    "SELECT to_iso8601(TIMESTAMP '2020-06-01 10:00:00.123 America/Sao_Paulo'), to_unixtime(TIMESTAMP '2020-06-01 10:00:00.123 America/Sao_Paulo'), CAST(TIMESTAMP '2020-06-01 10:00:00.123 America/Sao_Paulo' AS varchar)",
    "SELECT x, y, x / y, x % y FROM (VALUES (DECIMAL '10.50', DECIMAL '3.0'), (DECIMAL '-10.50', DECIMAL '3.0'), (DECIMAL '0.01', DECIMAL '-0.03')) AS t(x, y) ORDER BY 1",
    "SELECT x * y, typeof(x * y) FROM (VALUES (DECIMAL '12345678901234567.8', DECIMAL '98765432109876543.2')) AS t(x, y)",
    "SELECT DECIMAL '99999999999999999999.99' * DECIMAL '99999999999999999999.99'",
    "SELECT CAST(x AS decimal(10,4)) / 3, round(CAST(x AS decimal(10,4)) / 3, 2) FROM (VALUES 1, 2, -2) AS t(x) ORDER BY 1",
    "SELECT sum(x), avg(x) FROM (VALUES DECIMAL '0.1', DECIMAL '0.2', DECIMAL '0.3') AS t(x)",
    "SELECT 1E0 / 3, 2E0 / 3 * 3, 0.1E0 + 0.2E0, CAST(0.1E0 + 0.2E0 AS varchar), CAST(1E0 / 3 AS varchar)",
    "SELECT x, round(x, 2), CAST(x AS decimal(10,2)), CAST(x AS varchar) FROM (VALUES 0.125E0, 0.135E0, 1.005E0, -0.125E0) AS t(x) ORDER BY 1",
    "SELECT coalesce(NULL, NULL, 3), coalesce(CAST(NULL AS varchar), 'x'), nullif(1, NULL), if(NULL, 1, 2), CASE NULL WHEN NULL THEN 1 ELSE 2 END",
    "SELECT greatest('b', 'a', 'c'), least(DATE '2020-01-01', DATE '2019-01-01'), greatest(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 00:00:01')",
    "SELECT typeof(greatest(1, 2)), typeof(greatest(CAST(1 AS bigint), 2)), typeof(greatest(1, 2E0))",
    "SELECT count(*), count(x), count(DISTINCT x), sum(DISTINCT x), avg(DISTINCT x) FROM (VALUES 1, 1, 2, NULL) AS t(x)",
    "SELECT max_by(x, y) FILTER (WHERE y > 1), count_if(y > 1) FILTER (WHERE x > 1) FROM (VALUES (1, 1), (2, 2), (3, 3)) AS t(x, y)",
    "SELECT g, checksum(x), bool_or(x > 1), arbitrary(x) IS NOT NULL FROM (VALUES (1, 1), (1, 2), (2, NULL)) AS t(g, x) GROUP BY g ORDER BY 1",
    "SELECT x, sum(x) OVER (PARTITION BY g ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), array_agg(x) OVER (PARTITION BY g ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), count_if(x > 1) OVER (PARTITION BY g) FROM (VALUES (1, 1), (1, 2), (1, 3), (2, 4)) AS t(g, x) ORDER BY x",
    "SELECT histogram(x), map_agg(x, y) FROM (VALUES ('a', 1), ('b', NULL)) AS t(x, y)",
    "SELECT map_agg(x, y) FROM (VALUES (NULL, 1), ('a', 2)) AS t(x, y)",
    "SELECT element_at(m, 'a'), cardinality(m), map_keys(m) FROM (SELECT map_agg(x, y) AS m FROM (VALUES ('a', 1), ('b', 2)) AS t(x, y))",
    "SELECT array_join(array_agg(x ORDER BY y DESC, x), ',') FROM (VALUES ('a', 1), ('b', 2), ('c', 2)) AS t(x, y)",
    "SELECT array_agg(x ORDER BY x NULLS FIRST), array_agg(x ORDER BY x DESC NULLS LAST) FROM (VALUES 2, NULL, 1) AS t(x)",
    "SELECT slice(ARRAY[1, 2, 3], -2, 2), slice(ARRAY[1, 2, 3], 5, 1), trim_array(ARRAY[1, 2, 3], 1), contains_sequence(ARRAY[1, 2, 3], ARRAY[2, 3]), array_remove(ARRAY[1, NULL, 1], NULL), array_except(ARRAY[1, NULL], ARRAY[NULL])",
    "SELECT slice(ARRAY[1, 2], 0, 1)",
    "SELECT trim_array(ARRAY[1], 2)",
    "SELECT repeat(1, 0), repeat('a', 2), sequence(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-01 02:00:00', INTERVAL '1' HOUR), sequence(DATE '2020-01-31', DATE '2020-05-01', INTERVAL '1' MONTH)",
    "SELECT repeat('x', -1)",
    "SELECT url_extract_parameter('http://a.com/?a=1&a=2&b', 'b'), url_extract_parameter('http://a.com/?a=%zz', 'a'), url_decode('%zz'), url_extract_host('http://User@Example.COM/'), url_extract_path('http://a.com/%7Euser')",
    "SELECT to_hex(to_utf8('ç')), from_utf8(from_hex('C3A7')), md5(to_utf8('')), to_hex(sha1(X'')), xxhash64(to_utf8('')), crc32(to_utf8('abc')), to_base64(X'FF')",
    "SELECT typeof(to_utf8('a')), typeof(md5(X'00')), typeof(crc32(X'00')), typeof(xxhash64(X'00')), typeof(from_big_endian_64(X'0000000000000001'))",
    "SELECT try_cast('2020-01-01 10:00:00' AS timestamp(3)), try_cast('20200101' AS date), try_cast('abc' AS varbinary), CAST('2020-01-01 10:00' AS timestamp)",
    "SELECT CAST(TIMESTAMP '2020-01-01 10:00:00.123' AS date), CAST(DATE '2020-01-01' AS timestamp), CAST(DATE '2020-01-01' AS timestamp with time zone), CAST(TIMESTAMP '2020-01-01 10:00:00' AS timestamp with time zone)",
    "SELECT CAST(1 AS boolean), CAST(0 AS boolean), CAST(2E0 AS boolean), CAST(true AS integer), CAST(true AS varchar), CAST(false AS double)",
    "SELECT CAST(x AS bigint) FROM (VALUES 9.2233720368547758E18) AS t(x)",
    "SELECT CAST(DECIMAL '12345.678' AS decimal(6,1)), CAST(DECIMAL '-0.05' AS decimal(2,1)), CAST(DECIMAL '0.05' AS integer), CAST(DECIMAL '-0.5' AS bigint), CAST(DECIMAL '1.5' AS double), CAST(DECIMAL '1.5' AS real)",
    "SELECT CAST(DECIMAL '12345.678' AS decimal(4,1))",
    "SELECT CAST(REAL '1.1' AS decimal(10,5)), CAST(1.1E0 AS decimal(20,18)), CAST(1E20 AS decimal(38,0)), CAST(123.456E0 AS decimal(5,1))",
    "SELECT CAST('2020-01-01 10:00:00.123456789' AS timestamp(9)), CAST(TIMESTAMP '2020-01-01 10:00:00.123456' AS timestamp(9)), date_add('millisecond', 1, TIMESTAMP '2020-01-01 10:00:00.123456')",
    "SELECT url_extract_parameter('http://a.com/?a=1&a=2&b', 'b'), url_extract_host('http://User@Example.COM/'), url_extract_path('http://a.com/%7Euser')",
    "SELECT url_decode('%zz')",
    "SELECT format_datetime(DATE '2020-01-05', 'yyyy/MM/dd'), date_format(DATE '2020-01-05', '%Y/%m/%d'), date_trunc('day', DATE '2020-01-05')",
    "SELECT date_add('hour', 1, DATE '2020-01-05')",
];

/// Tens of thousands of generated rows, compared through order-sensitive digests.
const VOLUME_CASES: &[&str] = &[
    "SELECT checksum(CAST(x AS varchar)), sum(CAST(x AS double)) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 99)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT checksum(g), checksum(h) FROM (SELECT x % 1000 AS g, array_join(array_agg(x ORDER BY x DESC), ',') AS h FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t GROUP BY x % 1000)",
    "SELECT checksum(date_add('millisecond', x * 7, TIMESTAMP '2024-02-28 23:00:00.000')), checksum(date_format(date_add('minute', x * 13, TIMESTAMP '2019-12-25 00:00:00.000'), '%Y-%m-%d %H:%i:%s %W')), checksum(format_datetime(date_add('hour', x, TIMESTAMP '2020-01-01 00:00:00.000'), 'yyyy-MM-dd HH EEE ww YYYY')), checksum(date_trunc('week', date_add('hour', x, TIMESTAMP '2020-01-01 00:00:00.000'))), checksum(date_diff('month', TIMESTAMP '2020-01-31 00:00:00', date_add('day', x % 3000, TIMESTAMP '2020-01-31 00:00:00'))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT checksum(format_datetime(from_unixtime(x * 1800 + 1480000000, 'America/Sao_Paulo'), 'yyyy-MM-dd HH:mm ZZ')), checksum(hour(from_unixtime(x * 1800 + 1480000000, 'America/Sao_Paulo'))), checksum(CAST(from_unixtime(x * 1800 + 1480000000, 'America/Sao_Paulo') AS timestamp(3))), checksum(date_trunc('day', from_unixtime(x * 1800 + 1480000000, 'America/Sao_Paulo'))), checksum(date_add('day', 1, from_unixtime(x * 1800 + 1480000000, 'America/Sao_Paulo'))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT checksum(with_timezone(date_add('minute', x * 17, TIMESTAMP '2018-10-01 00:00:00.000'), 'America/Sao_Paulo')), checksum(date_diff('hour', with_timezone(date_add('minute', x * 17, TIMESTAMP '2018-10-01 00:00:00.000'), 'America/Sao_Paulo'), TIMESTAMP '2019-01-01 00:00:00 UTC')) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    r"SELECT checksum(lpad(CAST(x AS varchar), 8, 'ab')), checksum(to_hex(md5(to_utf8(CAST(x AS varchar))))), checksum(regexp_replace(CAST(x AS varchar), '(\d)(\d)', '$2$1')), checksum(split_part(CAST(x * 1.5E0 AS varchar), '.', 2)), checksum(CAST(x / 7.0 AS varchar)), checksum(CAST(x / 7E0 AS varchar)), checksum(substr(to_hex(sha256(to_big_endian_64(x))), x % 60)) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT checksum(from_big_endian_64(substr(concat(X'0000', to_big_endian_64(x * 7919 - 100000000)), 3))), to_hex(max(to_big_endian_64(x * 7919 - 100000000))), to_hex(min(concat(X'00', to_big_endian_32(CAST(x AS integer))))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT sum(CAST(x AS decimal(18,6)) / 7), avg(CAST(x AS decimal(10,2))), sum(round(CAST(x AS decimal(18,6)) / 3, 2)), max(CAST(x AS decimal(38,10)) * CAST(x AS decimal(38,10))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT count(try(1000 / (x % 10))), sum(try(1000 / (x % 10))), count(*) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT sum(CAST(x AS integer) * 100000) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT count(try(CAST(x AS integer) * 50000)) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT sum(x * 1000000000000000) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT count(DISTINCT u), count(*) FROM (SELECT CAST(uuid() AS varchar) AS u FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t)",
    "SELECT x % 10, count(*), max_by(x, x), min_by(x, x), checksum(x), array_join(slice(array_agg(x ORDER BY x DESC), 1, 3), ',') FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 99)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t GROUP BY x % 10 ORDER BY 1",
    "SELECT g, rn, x FROM (SELECT x % 3 AS g, x, row_number() OVER (PARTITION BY x % 3 ORDER BY x DESC) AS rn FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t) WHERE rn <= 2 ORDER BY 1, 2",
    "SELECT CAST(x AS integer) * 50000 FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT approx_percentile(x, 0.5), approx_percentile(x, 0.95), approx_percentile(x, 0.99), approx_percentile(x, ARRAY[0.01, 0.5]) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 99)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT to_hex(md5(to_utf8(listagg(CAST(x AS varchar), ',') WITHIN GROUP (ORDER BY x)))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT to_hex(md5(to_utf8(array_join(array_agg(CAST(x AS varchar) ORDER BY x), ',')))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT to_hex(md5(to_utf8(array_join(array_agg(CAST(x AS varchar) ORDER BY x DESC), ',')))) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 49)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT array_agg(x ORDER BY x)[1] FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 4)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT array_agg(x ORDER BY x DESC)[1] FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 4)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT min_by(x, x), max_by(x, x), array_agg(x ORDER BY x DESC)[1] FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 4)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
];

/// Approximate aggregates: numbers compared within `APPROX_TOLERANCE` of Trino's.
const APPROX_CASES: &[&str] = &[
    "SELECT approx_distinct(x) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 99)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT approx_distinct(CAST(x AS varchar)) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 99)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT approx_percentile(CAST(x AS double), 0.5), approx_percentile(CAST(x AS double), 0.95) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 99)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
    "SELECT approx_distinct(x) FROM (SELECT i * 1000 + j AS x FROM UNNEST(sequence(0, 9)) AS a(i) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(j)) AS t",
];
