//! F9 acceptance suite, written by an independent tester: black-box checks of
//! `ALTER TABLE … EXECUTE optimize` (ADR 0009) through the public engine API.
//!
//! Wherever the answer is Trino's semantics (messages, thresholds, what a
//! reader sees in the files), the suite asks a real `trinodb/trino:431`
//! started for the test (`codelake-f9-acc-*`) instead of trusting a recorded
//! expectation. Every test checks rows and values, never only "no error".
//!
//! The Trino warehouse and the local copy `CodeLake` reads are kept in sync by copy:
//! the tests run inside the dev container, so a bind mount would point at the
//! wrong host path. Trino is driven through its REST API by a small Python
//! client inside its own container (one JVM start per statement is too slow
//! and too heavy for the 12 GB Docker VM).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine, QueryOptions};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::ScalarValue;
use datafusion::parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

// ---------------------------------------------------------------------------
// CodeLake side
// ---------------------------------------------------------------------------

const MARKER: &str = ".codelake-optimize.json";

fn params(warehouse: &Path) -> HiveParams {
    HiveParams {
        warehouse: warehouse.display().to_string(),
        s3_endpoint: None,
        s3_region: "us-east-1".into(),
        credentials: None,
        path_style: false,
    }
}

/// A fresh engine (a fresh "process": own locks, own catalog open and its
/// recovery) over `warehouse`, with the hive connector attached as `catalog`.
async fn try_open(warehouse: &Path, spill: &Path, catalog: &str, limit: ByteSize) -> Result<Engine, String> {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).map_err(|e| e.to_string())?;
    engine
        .attach(
            Arc::new(HiveConnector::new(catalog.into(), params(warehouse))),
            true,
            Duration::from_secs(60),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(engine)
}

async fn open(warehouse: &Path, spill: &Path) -> Engine {
    try_open(warehouse, spill, "hive", ByteSize::mib(256))
        .await
        .unwrap_or_else(|e| panic!("hive catalog must attach: {e}"))
}

fn options(engine: &Engine, properties: &[(&str, &str)]) -> QueryOptions {
    let mut options = QueryOptions::for_user("f9-acceptance");
    options.properties = engine
        .session_properties()
        .session(
            properties
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        )
        .unwrap();
    options
}

fn enabled(engine: &Engine) -> QueryOptions {
    options(engine, &[("hive.non_transactional_optimize_enabled", "true")])
}

async fn batches(engine: &Engine, options: &QueryOptions, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let frame = engine.sql_with(options, sql).await.map_err(|e| e.to_string())?;
    frame.collect().await.map_err(|e| e.to_string())
}

/// Runs `sql` and renders the answer; panics on error.
async fn sql(engine: &Engine, sql: &str) -> String {
    let result = batches(engine, &QueryOptions::for_user("f9-acceptance"), sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    pretty_format_batches(&result).unwrap().to_string()
}

async fn optimize(engine: &Engine, statement: &str) -> Result<Vec<RecordBatch>, String> {
    batches(engine, &enabled(engine), statement).await
}

/// First row of `sql`, each column as `i128` (counts, sums).
async fn numbers(engine: &Engine, sql: &str) -> Result<Vec<i128>, String> {
    let result = batches(engine, &QueryOptions::for_user("f9-acceptance"), sql).await?;
    let batch = result
        .iter()
        .find(|b| b.num_rows() > 0)
        .ok_or_else(|| format!("`{sql}` returned no row"))?;
    batch
        .columns()
        .iter()
        .map(|column| {
            let value = ScalarValue::try_from_array(column, 0).map_err(|e| e.to_string())?;
            if value.is_null() {
                return Ok(0);
            }
            value
                .to_string()
                .parse::<i128>()
                .map_err(|e| format!("{value}: {e}"))
        })
        .collect()
}

/// Visible data files (what Trino and scans read): name to bytes.
fn visible(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .filter(|e| e.file_type().unwrap().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with(['.', '_']))
        .map(|n| {
            let bytes = std::fs::read(dir.join(&n)).unwrap();
            (n, bytes)
        })
        .collect()
}

/// What an interrupted or failed OPTIMIZE could leave behind: hidden
/// temporary files, the swap marker, object-store staging files.
fn leftovers(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".inprogress") || n == MARKER || n.contains('#'))
        .collect();
    names.sort();
    names
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn replace_dir(from: &Path, to: &Path) {
    if to.exists() {
        std::fs::remove_dir_all(to).unwrap();
    }
    copy_dir(from, to);
}

/// The swap marker as ADR 0009 documents it (`remove`: name and size of each
/// replaced file; `add`: hidden and final name and rows of each new file).
fn marker(remove: &[(String, u64)], add: &[(String, String, u64)]) -> String {
    json!({
        "version": 1,
        "id": uuid::Uuid::new_v4().to_string(),
        "remove": remove.iter().map(|(name, size)| json!({"name": name, "size": size})).collect::<Vec<_>>(),
        "add": add.iter().map(|(temporary, name, rows)| json!({"temporary": temporary, "name": name, "rows": rows})).collect::<Vec<_>>(),
    })
    .to_string()
}

fn size_of(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap()
}

/// Polls `condition` every few milliseconds, up to `limit`.
async fn wait_until(limit: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while start.elapsed() < limit {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Trino 431 side
// ---------------------------------------------------------------------------

const HIVE: &str = "connector.name=hive\n\
                    hive.metastore=file\n\
                    hive.metastore.catalog.dir=file:/warehouse\n\
                    hive.allow-add-column=true\n\
                    hive.allow-drop-column=true\n\
                    hive.allow-rename-column=true\n";
/// Same warehouse read at microsecond precision.
const HIVE_US: &str = "connector.name=hive\n\
                       hive.metastore=file\n\
                       hive.metastore.catalog.dir=file:/warehouse\n\
                       hive.timestamp-precision=MICROSECONDS\n";
/// Heap capped: the image default (80% of the container) gets the whole VM
/// OOM-killed when several suites run in parallel.
const JVM: &str = "-server\n-Xmx768M\n-XX:G1HeapRegionSize=32M\n-XX:+ExplicitGCInvokesConcurrent\n\
                   -XX:+ExitOnOutOfMemoryError\n-XX:-OmitStackTraceInFastThrow\n\
                   -XX:ReservedCodeCacheSize=128M\n-Djdk.attach.allowAttachSelf=true\n\
                   -Djdk.nio.maxCachedBufferSize=2000000\n";

/// Runs statements through Trino's REST protocol and prints one JSON line
/// per statement: `{"rows": [...], "columns": [...]}` or `{"error": "..."}`.
const CLIENT: &str = r#"
import json, sys, time, urllib.request

def fetch(request):
    return json.load(urllib.request.urlopen(request, timeout=120))

def run(sql, session):
    # Right after startup the coordinator may not list itself as a worker
    # yet; nothing ran, so the statement is simply sent again.
    for _ in range(60):
        answer = run_once(sql, session)
        if answer.get("error") != "No nodes available to run query":
            return answer
        time.sleep(1)
    return answer

def run_once(sql, session):
    # Without PARAMETRIC_DATETIME Trino rounds every timestamp to
    # milliseconds on the wire: a timestamp(6) column must be seen as stored.
    headers = {"X-Trino-User": "f9", "Content-Type": "text/plain",
               "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME"}
    if session:
        headers["X-Trino-Session"] = ",".join(session)
    body = fetch(urllib.request.Request("http://localhost:8080/v1/statement", data=sql.encode(), headers=headers, method="POST"))
    columns, rows = None, []
    while True:
        if body.get("columns") and columns is None:
            columns = [c["name"] for c in body["columns"]]
        rows.extend(body.get("data") or [])
        if "error" in body:
            return {"error": body["error"]["message"]}
        if not body.get("nextUri"):
            return {"columns": columns, "rows": rows, "update": body.get("updateType")}
        body = fetch(urllib.request.Request(body["nextUri"], headers={"X-Trino-User": "f9"}))

request = json.loads(sys.argv[1])
for statement in request["statements"]:
    print(json.dumps(run(statement, request.get("session", []))), flush=True)
"#;

struct Trino {
    container: ContainerAsync<GenericImage>,
}

/// Trino's answer to one statement.
#[derive(Debug, Clone, PartialEq)]
enum Answer {
    Rows(Value),
    Error(String),
}

impl Answer {
    fn rows(&self) -> &Value {
        match self {
            Self::Rows(rows) => rows,
            Self::Error(e) => panic!("trino failed: {e}"),
        }
    }
}

impl Trino {
    async fn start() -> Self {
        let container = GenericImage::new("trinodb/trino", "431")
            .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
            .with_container_name(format!("codelake-f9-acc-trino-{}", uuid::Uuid::new_v4()))
            .with_user("root")
            .with_startup_timeout(Duration::from_secs(240))
            .with_host_config_modifier(|config| config.memory = Some(3 << 29))
            .with_copy_to("/etc/trino/jvm.config", JVM.as_bytes().to_vec())
            .with_copy_to("/etc/trino/catalog/hive.properties", HIVE.as_bytes().to_vec())
            .with_copy_to(
                "/etc/trino/catalog/hive_us.properties",
                HIVE_US.as_bytes().to_vec(),
            )
            .with_copy_to("/tmp/client.py", CLIENT.as_bytes().to_vec())
            .start()
            .await
            .expect("trino 431 starts");
        let trino = Self { container };
        for _ in 0..120 {
            if let Ok(answers) = trino.try_run(&[], &["SELECT 1"]).await
                && matches!(answers.first(), Some(Answer::Rows(_)))
            {
                return trino;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("trino did not accept queries");
    }

    async fn exec(&self, command: Vec<String>) -> Result<String, String> {
        let program = command[0].clone();
        let mut result = self
            .container
            .exec(ExecCommand::new(command))
            .await
            .map_err(|e| e.to_string())?;
        let stdout =
            String::from_utf8_lossy(&result.stdout_to_vec().await.map_err(|e| e.to_string())?).into_owned();
        let stderr =
            String::from_utf8_lossy(&result.stderr_to_vec().await.map_err(|e| e.to_string())?).into_owned();
        let mut code = None;
        for _ in 0..100 {
            code = result.exit_code().await.map_err(|e| e.to_string())?;
            if code.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if code == Some(0) {
            Ok(stdout)
        } else {
            Err(format!("{program}: exit {code:?}: {stderr}"))
        }
    }

    async fn try_run(&self, session: &[&str], statements: &[&str]) -> Result<Vec<Answer>, String> {
        let request = json!({"session": session, "statements": statements}).to_string();
        let stdout = self
            .exec(vec!["python3".into(), "/tmp/client.py".into(), request])
            .await?;
        let answers: Vec<Answer> = stdout
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                let value: Value = serde_json::from_str(line).unwrap();
                match value.get("error") {
                    Some(Value::String(message)) => Answer::Error(message.clone()),
                    _ => Answer::Rows(value["rows"].clone()),
                }
            })
            .collect();
        if answers.len() == statements.len() {
            Ok(answers)
        } else {
            Err(format!("expected {} answers, got {stdout}", statements.len()))
        }
    }

    async fn run(&self, session: &[&str], statements: &[&str]) -> Vec<Answer> {
        self.try_run(session, statements)
            .await
            .unwrap_or_else(|e| panic!("trino client: {e}"))
    }

    /// Runs every statement; any error fails the test.
    async fn ok(&self, statements: &[&str]) {
        for (statement, answer) in statements.iter().zip(self.run(&[], statements).await) {
            if let Answer::Error(e) = answer {
                panic!("trino `{statement}`: {e}");
            }
        }
    }

    async fn query(&self, sql: &str) -> Answer {
        self.run(&[], &[sql]).await.remove(0)
    }

    /// Copies Trino's whole warehouse into `local`.
    async fn copy_out(&self, local: &Path) {
        let listing = self
            .exec(vec![
                "find".into(),
                "/warehouse".into(),
                "-type".into(),
                "f".into(),
            ])
            .await
            .unwrap();
        for path in listing.lines().filter(|l| !l.is_empty()) {
            let relative = path.strip_prefix("/warehouse/").unwrap();
            let target = local.join(relative);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            self.container
                .copy_file_from(path.to_owned(), target)
                .await
                .unwrap();
        }
    }

    /// Replaces `/warehouse/<relative>` in the container with the local
    /// directory `local/<relative>`.
    async fn push(&self, local: &Path, relative: &str) {
        let remote = format!("/warehouse/{relative}");
        self.exec(vec!["rm".into(), "-rf".into(), remote.clone()])
            .await
            .unwrap();
        let root = local.join(relative);
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    stack.push(entry.path());
                    continue;
                }
                let rel = entry
                    .path()
                    .strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                let target = format!("{remote}/{rel}");
                let bytes = std::fs::read(entry.path()).unwrap();
                let chunks: Vec<&[u8]> = if bytes.is_empty() {
                    vec![&[]]
                } else {
                    bytes.chunks(48 * 1024).collect()
                };
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let mode = if i == 0 { "wb" } else { "ab" };
                    self.exec(vec![
                        "python3".into(),
                        "-c".into(),
                        "import base64, os, sys; os.makedirs(os.path.dirname(sys.argv[1]), exist_ok=True); \
                         open(sys.argv[1], sys.argv[3]).write(base64.b64decode(sys.argv[2]))"
                            .into(),
                        target.clone(),
                        base64::engine::general_purpose::STANDARD.encode(chunk),
                        mode.into(),
                    ])
                    .await
                    .unwrap();
                }
            }
        }
    }
}

/// Trino 431 prints some names as `Optional[name]` (a Java artifact);
/// `CodeLake` is not expected to copy it.
fn without_java_optional(message: &str) -> String {
    if let Some(start) = message.find("Optional[")
        && let Some(end) = message[start..].find(']')
    {
        return format!(
            "{}{}{}",
            &message[..start],
            &message[start + "Optional[".len()..start + end],
            &message[start + end + 1..]
        );
    }
    message.to_owned()
}

/// Trino's message without the `line 1:62: ` position prefix.
fn trino_message(message: &str) -> &str {
    if let Some(rest) = message.strip_prefix("line ")
        && let Some((position, text)) = rest.split_once(": ")
        && position.chars().all(|c| c.is_ascii_digit() || c == ':')
    {
        return text;
    }
    message
}

// ---------------------------------------------------------------------------
// Trino semantics
// ---------------------------------------------------------------------------

/// `(optimize enabled, statement, syntax error: only require that both fail)`.
fn parity_cases() -> Vec<(bool, String, bool)> {
    let mut cases = Vec::new();
    for argument in [
        "'10mb'",
        "''",
        "' 1MB '",
        "'1 MB'",
        "'1.5MB'",
        "'-1MB'",
        "'1e3B'",
        "'100'",
        "'abc'",
        "'.5MB'",
        "'5.MB'",
        "'1kb'",
        "'1KB'",
        "'1kB'",
        "'99999999999PB'",
        "'16EB'",
        "10",
        "NULL",
        "true",
        "1.5",
        "DATE '2020-01-01'",
        "'1' || 'MB'",
        "(SELECT '1MB')",
        "CAST('1MB' AS varchar(3))",
    ] {
        cases.push((
            true,
            format!("ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => {argument})"),
            false,
        ));
    }
    for statement in [
        "ALTER TABLE hive.s.t EXECUTE optimize(foo => '1MB')",
        "ALTER TABLE hive.s.t EXECUTE optimize(foo => NULL)",
        "ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => '1MB', file_size_threshold => '2MB')",
        "ALTER TABLE hive.s.t EXECUTE optimize('1MB', file_size_threshold => '2MB')",
        "ALTER TABLE hive.s.t EXECUTE optimize('1MB', '2MB')",
        "ALTER TABLE hive.s.t EXECUTE optimize('1MB')",
        "ALTER TABLE hive.s.t EXECUTE optimize()",
        "ALTER TABLE hive.s.t EXECUTE optimize(FILE_SIZE_THRESHOLD => '1MB')",
        "ALTER TABLE hive.s.t EXECUTE optimize(\"file_size_threshold\" => '1MB')",
        "ALTER TABLE hive.s.t EXECUTE foo",
        "ALTER TABLE hive.s.t EXECUTE foo(bar => 1)",
        "ALTER TABLE hive.s.t EXECUTE \"optimize\"",
        "ALTER TABLE hive.s.t EXECUTE OPTIMIZE",
        "ALTER TABLE hive.s.nope EXECUTE optimize",
        "ALTER TABLE hive.s.nope EXECUTE foo",
        "ALTER TABLE hive.nos.nope EXECUTE optimize",
        "ALTER TABLE hive.s.v EXECUTE optimize",
        "ALTER TABLE hive.s.t EXECUTE optimize WHERE id = 1",
        "ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => 'abc') WHERE id = 1",
        "ALTER TABLE hive.\"S\".\"T\" EXECUTE optimize",
        "ALTER TABLE \"hive\".\"s\".\"t\" EXECUTE optimize",
        "alter   table\n hive . s . t\texecute  optimize ( file_size_threshold=>'1MB' )",
        "ALTER TABLE /* c */ hive.s.t -- c\n EXECUTE optimize",
    ] {
        cases.push((true, statement.to_owned(), false));
    }
    for statement in [
        "ALTER TABLE hive.s.t EXECUTE optimize(",
        "ALTER TABLE hive.s.t EXECUTE",
        "ALTER TABLE hive.s.t EXECUTE optimize garbage",
    ] {
        cases.push((true, statement.to_owned(), true));
    }
    for statement in [
        "ALTER TABLE hive.s.t EXECUTE optimize",
        "ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => 'abc')",
        "ALTER TABLE hive.s.nope EXECUTE optimize",
        "ALTER TABLE hive.s.t EXECUTE optimize WHERE id = 1",
        "ALTER TABLE hive.s.t EXECUTE foo",
        "ALTER TABLE hive.s.v EXECUTE optimize",
    ] {
        cases.push((false, statement.to_owned(), false));
    }

    cases
}

/// `None` when `CodeLake` answered like Trino.
fn verdict(expected: &Answer, actual: &Result<Vec<RecordBatch>, String>, syntax: bool) -> Option<String> {
    match (expected, actual) {
        (Answer::Rows(_), Ok(_)) => None,
        (Answer::Error(want), Err(got)) => {
            if syntax || got.contains(&without_java_optional(trino_message(want))) {
                None
            } else {
                Some(format!("trino: {want}\n    codelake: {got}"))
            }
        }
        (Answer::Error(want), Ok(_)) => Some(format!("trino: {want}\n    codelake: succeeded")),
        (Answer::Rows(_), Err(got)) => Some(format!("trino: succeeded\n    codelake: {got}")),
    }
}

/// Every argument, name, procedure and table variant answers like Trino 431:
/// both succeed, or both fail and `CodeLake`'s message carries Trino's.
#[tokio::test]
async fn statements_answer_like_trino_431() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    trino
        .ok(&[
            "CREATE SCHEMA hive.s",
            "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS bigint) AS id, 'a' AS nome",
            "INSERT INTO hive.s.t VALUES (2, 'b')",
            "INSERT INTO hive.s.t VALUES (3, 'c')",
            "CREATE VIEW hive.s.v AS SELECT 1 AS a",
        ])
        .await;
    trino.copy_out(local.path()).await;
    let lake = open(local.path(), spill.path()).await;

    let cases = parity_cases();
    let mut mismatches = Vec::new();
    for (on, statement, syntax) in &cases {
        let session: &[&str] = if *on {
            &["hive.non_transactional_optimize_enabled=true"]
        } else {
            &[]
        };
        let expected = trino.run(session, &[statement.as_str()]).await.remove(0);
        let codelake_options = if *on {
            enabled(&lake)
        } else {
            QueryOptions::for_user("f9-acceptance")
        };
        let actual = batches(&lake, &codelake_options, statement).await;
        if let Some(verdict) = verdict(&expected, &actual, *syntax) {
            mismatches.push(format!(
                "[{}] {statement}\n    {verdict}",
                if *on { "on" } else { "off" }
            ));
        }
    }
    // The table still holds its rows after every successful call above.
    assert_eq!(
        sql(&lake, "SELECT id, nome FROM hive.s.t ORDER BY id").await,
        "+----+------+\n| id | nome |\n+----+------+\n| 1  | a    |\n| 2  | b    |\n| 3  | c    |\n+----+------+"
    );
    assert!(
        mismatches.is_empty(),
        "{} of {} statements answer differently from Trino 431:\n{}",
        mismatches.len(),
        cases.len(),
        mismatches.join("\n")
    );
}

/// airlift `DataSize` rounds a fractional byte count to the nearest byte
/// (`374.5B` is 375 bytes), so a file of exactly 375 bytes is compacted.
#[tokio::test]
async fn fractional_threshold_rounds_like_trino_431() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    let mut setup = vec!["CREATE SCHEMA hive.s".to_owned()];
    for table in ["below", "half_bytes", "half_kilobytes"] {
        setup.push(format!(
            "CREATE TABLE hive.s.{table} WITH (format = 'PARQUET') AS SELECT CAST(1 AS bigint) AS id, 'a' AS s"
        ));
        setup.push(format!("INSERT INTO hive.s.{table} VALUES (2, 'b')"));
        setup.push(format!("INSERT INTO hive.s.{table} VALUES (3, 'c')"));
    }
    trino
        .ok(&setup.iter().map(String::as_str).collect::<Vec<_>>())
        .await;
    trino.copy_out(local.path()).await;
    let sizes: BTreeSet<u64> = ["below", "half_bytes", "half_kilobytes"]
        .iter()
        .flat_map(|t| {
            visible(&local.path().join("s").join(t))
                .into_values()
                .map(|b| size_of(&b))
        })
        .collect();
    assert_eq!(sizes.len(), 1, "Trino wrote files of different sizes: {sizes:?}");
    let size = *sizes.first().unwrap();
    // (size - 0.5) bytes in kB: (2·size - 1) / 2048, exact in 11 decimals.
    let numerator = (2 * u128::from(size) - 1) * 48_828_125;
    let kilobytes = format!(
        "{}.{:011}",
        numerator / 100_000_000_000,
        numerator % 100_000_000_000
    );
    let cases = [
        ("below", format!("{}.4B", size - 1)),
        ("half_bytes", format!("{}.5B", size - 1)),
        ("half_kilobytes", format!("{kilobytes}kB")),
    ];
    let lake = open(local.path(), spill.path()).await;
    let mut outcome = Vec::new();
    for (table, threshold) in &cases {
        let statement =
            format!("ALTER TABLE hive.s.{table} EXECUTE optimize(file_size_threshold => '{threshold}')");
        let answer = trino
            .run(
                &["hive.non_transactional_optimize_enabled=true"],
                &[statement.as_str()],
            )
            .await
            .remove(0);
        assert!(matches!(answer, Answer::Rows(_)), "trino {statement}: {answer:?}");
        optimize(&lake, &statement).await.unwrap();
        let trino_files = trino
            .query(&format!("SELECT count(DISTINCT \"$path\") FROM hive.s.{table}"))
            .await;
        let lake_files = visible(&local.path().join("s").join(table)).len();
        outcome.push((
            (*table).to_owned(),
            threshold.clone(),
            trino_files.rows()[0][0].as_u64().unwrap(),
            u64::try_from(lake_files).unwrap(),
        ));
        assert_eq!(
            sql(
                &lake,
                &format!("SELECT count(*) AS n, sum(id) AS s FROM hive.s.{table}")
            )
            .await,
            "+---+---+\n| n | s |\n+---+---+\n| 3 | 6 |\n+---+---+"
        );
    }
    let differing: Vec<_> = outcome.iter().filter(|(_, _, t, l)| t != l).collect();
    assert!(
        differing.is_empty(),
        "files of {size} bytes; (table, threshold, files left by Trino, files left by CodeLake): {outcome:?}"
    );
}

/// Trino statements writing a table with every mapped type, in five or more files.
const TYPED_TABLE: &[&str] = &[
    "CREATE SCHEMA hive.s",
    "CREATE TABLE hive.s.t (id bigint, ti tinyint, si smallint, i integer, bi bigint, r real, \
             d double, dec_s decimal(5,2), dec_l decimal(38,10), c char(3), vc varchar(10), s varchar, \
             b varbinary, dt date, ts timestamp(3), bo boolean) WITH (format = 'PARQUET')",
    "INSERT INTO hive.s.t VALUES (1, TINYINT '-128', SMALLINT '-32768', INTEGER '-2147483648', \
             BIGINT '-9223372036854775808', CAST(-infinity() AS real), nan(), -999.99, \
             -9999999999999999999999999999.9999999999, CAST('a' AS char(3)), '', 'ação 🚀 日本', X'', DATE '0001-01-01', \
             TIMESTAMP '0001-01-01 00:00:00.000', false)",
    "INSERT INTO hive.s.t VALUES (2, TINYINT '127', SMALLINT '32767', 2147483647, \
             9223372036854775807, CAST(infinity() AS real), -infinity(), 999.99, \
             9999999999999999999999999999.9999999999, CAST('abc' AS char(3)), 'ten chars!', \
             'line' || chr(10) || 'break \"q\" ''a'' ,;', X'00FF00', DATE '9999-12-31', \
             TIMESTAMP '9999-12-31 23:59:59.999', true), \
             (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    "INSERT INTO hive.s.t VALUES (4, TINYINT '0', SMALLINT '0', 0, 0, CAST(nan() AS real), \
             1.7976931348623157E308, 0.01, 0.0000000001, CAST(' ' AS char(3)), 'x', '', X'2A', DATE '1582-10-10', \
             TIMESTAMP '1500-01-01 12:34:56.789', true), \
             (5, TINYINT '-1', SMALLINT '-1', -1, -1, REAL '1.4E-45', 4.9E-324, -0.01, -0.0000000001, \
             CAST('z' AS char(3)), 'y', ' leading', X'FF', DATE '1970-01-01', TIMESTAMP '1969-12-31 23:59:59.999', false)",
    "ALTER TABLE hive.s.t ADD COLUMN extra varchar",
    "INSERT INTO hive.s.t (id, s, extra) VALUES (6, NULL, 'only-new'), (7, 'x', NULL)",
    "INSERT INTO hive.s.t (id, s, bi, d) SELECT x + 1000, 'seq ' || CAST(x AS varchar), \
             x * 1000000007, CAST(x AS double) / 3 FROM UNNEST(sequence(1, 3000)) AS u(x)",
];

/// A table Trino wrote with every type the hive catalog maps, extremes, NaN,
/// infinities, NULLs and a column added later, compacted by `CodeLake`: a
/// strict Trino returns exactly the same rows from one file. A crash state
/// with the marker and hidden files present is invisible to Trino.
#[tokio::test]
async fn every_type_extreme_and_null_survives_and_trino_reads_it() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    trino.ok(TYPED_TABLE).await;
    let all: &str = "SELECT * FROM hive.s.t ORDER BY id";
    let reference = trino.query(all).await.rows().clone();
    assert_eq!(reference.as_array().unwrap().len(), 3007);
    trino.copy_out(local.path()).await;
    let table = local.path().join("s/t");
    let originals = visible(&table);
    assert!(originals.len() >= 5, "{:?}", originals.keys());

    // A crash right after the marker, before any rename: old files, one
    // hidden new file and the marker. Trino must not see any of it.
    let crash = local.path().join("s/t_crash");
    copy_dir(&table, &crash);
    let (first, bytes) = originals.iter().next().unwrap();
    let hidden = format!(".{first}_new.inprogress");
    std::fs::write(crash.join(&hidden), bytes).unwrap();
    let removed: Vec<(String, u64)> = originals.iter().map(|(n, b)| (n.clone(), size_of(b))).collect();
    std::fs::write(
        crash.join(MARKER),
        marker(&removed, &[(hidden.clone(), format!("{first}_new"), 1)]),
    )
    .unwrap();
    trino.push(local.path(), "s/t_crash").await;
    assert_eq!(
        trino
            .query("SELECT * FROM hive.s.t_crash ORDER BY id")
            .await
            .rows(),
        &reference,
        "Trino reading a table with a pending OPTIMIZE marker"
    );
    std::fs::remove_dir_all(&crash).unwrap();
    trino
        .exec(vec!["rm".into(), "-rf".into(), "/warehouse/s/t_crash".into()])
        .await
        .unwrap();

    let lake = open(local.path(), spill.path()).await;
    let before = sql(&lake, all).await;
    optimize(&lake, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    let after = visible(&table);
    assert_eq!(after.len(), 1, "{:?}", after.keys());
    assert!(after.keys().all(|n| !originals.contains_key(n)));
    assert!(leftovers(&table).is_empty(), "{:?}", leftovers(&table));
    for name in originals.keys() {
        assert!(
            !table.join(format!(".{name}.crc")).exists(),
            "checksum of {name} left behind"
        );
    }
    assert_eq!(sql(&lake, all).await, before);

    trino.push(local.path(), "s/t").await;
    assert_eq!(trino.query(all).await.rows(), &reference);
    assert_eq!(
        trino
            .query("SELECT count(DISTINCT \"$path\") FROM hive.s.t")
            .await
            .rows(),
        &json!([[1]])
    );
    // Trino keeps writing into the compacted table and reads everything.
    trino
        .ok(&["INSERT INTO hive.s.t (id, s) VALUES (9999, 'after')"])
        .await;
    assert_eq!(
        trino
            .query("SELECT count(*), count(extra), sum(id) FROM hive.s.t")
            .await
            .rows(),
        &json!([[3008, 1, 7_511_527]])
    );
}

/// A catalog configured at microsecond precision (`hive.timestamp-precision
/// = MICROSECONDS`, a Trino catalog setting) reads the same timestamps
/// after `CodeLake` compacts its files.
#[tokio::test]
async fn microsecond_timestamps_survive_compaction() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    trino
        .ok(&[
            "CREATE SCHEMA hive.s",
            "CREATE TABLE hive_us.s.t WITH (format = 'PARQUET') AS \
             SELECT 1 AS id, TIMESTAMP '2026-01-01 10:00:00.123999' AS ts",
            "INSERT INTO hive_us.s.t VALUES (2, TIMESTAMP '1500-06-01 00:00:00.000001')",
            "INSERT INTO hive_us.s.t VALUES (3, TIMESTAMP '2026-01-01 10:00:00.000500')",
        ])
        .await;
    let all: &str = "SELECT id, ts FROM hive_us.s.t ORDER BY id";
    let reference = trino.query(all).await.rows().clone();
    assert_eq!(
        reference,
        json!([
            [1, "2026-01-01 10:00:00.123999"],
            [2, "1500-06-01 00:00:00.000001"],
            [3, "2026-01-01 10:00:00.000500"]
        ])
    );
    trino.copy_out(local.path()).await;
    let lake = open(local.path(), spill.path()).await;
    optimize(&lake, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(visible(&local.path().join("s/t")).len(), 1);
    trino.push(local.path(), "s/t").await;
    assert_eq!(trino.query(all).await.rows(), &reference);
}

/// Trino matches Parquet columns to table columns by name, ignoring case, so
/// a file written by another tool with `ID`/`NOME` is read. Compaction must
/// not turn those values into NULL.
#[tokio::test]
async fn column_names_match_case_insensitively_like_trino() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    trino
        .ok(&[
            "CREATE SCHEMA hive.s",
            "CREATE TABLE hive.s.t (id bigint, nome varchar) WITH (format = 'PARQUET')",
            "INSERT INTO hive.s.t VALUES (1, 'a')",
        ])
        .await;
    trino.copy_out(local.path()).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ID", DataType::Int64, true),
        Field::new("NOME", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["b", "c"])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = std::fs::File::create(local.path().join("s/t/20200101_000000_00000_other_writer")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    trino.push(local.path(), "s/t").await;
    let all: &str = "SELECT id, nome FROM hive.s.t ORDER BY id";
    let reference = trino.query(all).await.rows().clone();
    assert_eq!(
        reference,
        json!([[1, "a"], [2, "b"], [3, "c"]]),
        "Trino 431 reference"
    );

    let lake = open(local.path(), spill.path()).await;
    optimize(&lake, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    trino.push(local.path(), "s/t").await;
    assert_eq!(
        trino.query(all).await.rows(),
        &reference,
        "Trino after CodeLake's OPTIMIZE"
    );
    assert_eq!(
        sql(&lake, all).await,
        "+----+------+\n| id | nome |\n+----+------+\n| 1  | a    |\n| 2  | b    |\n| 3  | c    |\n+----+------+"
    );
}

/// A column dropped and added again with another type: Trino refuses to
/// read the old files (`Unsupported Trino column type`). Compaction must not
/// turn the dropped integers into values of the new varchar column.
#[tokio::test]
async fn retyped_column_is_not_fabricated() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    trino
        .ok(&[
            "CREATE SCHEMA hive.s",
            "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS bigint) AS id, 10 AS k",
            "INSERT INTO hive.s.t VALUES (2, 20)",
            "ALTER TABLE hive.s.t DROP COLUMN k",
            "ALTER TABLE hive.s.t ADD COLUMN k varchar",
            "INSERT INTO hive.s.t VALUES (3, 'z')",
        ])
        .await;
    let all: &str = "SELECT id, k FROM hive.s.t ORDER BY id";
    let before = trino.query(all).await;
    let Answer::Error(refusal) = &before else {
        panic!("Trino 431 was expected to refuse the retyped column: {before:?}");
    };
    assert!(refusal.contains("Unsupported Trino column type"), "{refusal}");
    trino.copy_out(local.path()).await;

    let lake = open(local.path(), spill.path()).await;
    let compacted = optimize(&lake, "ALTER TABLE hive.s.t EXECUTE optimize").await;
    trino.push(local.path(), "s/t").await;
    let after = trino.query(all).await;
    let fabricated = match &after {
        Answer::Rows(rows) => rows
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row[0].as_i64() != Some(3) && !row[1].is_null()),
        Answer::Error(_) => false,
    };
    assert!(
        !fabricated,
        "OPTIMIZE ({compacted:?}) turned dropped integers into varchar values: Trino now answers {after:?}"
    );
}

/// Trino skips zero-byte files (left by writers that crashed) when reading
/// and when compacting. `CodeLake` must answer the same and compact the rest.
#[tokio::test]
async fn zero_byte_file_is_skipped_like_trino() {
    let local = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let trino = Trino::start().await;
    trino
        .ok(&[
            "CREATE SCHEMA hive.s",
            "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS bigint) AS id, 'a' AS s",
            "INSERT INTO hive.s.t VALUES (2, 'b')",
            "INSERT INTO hive.s.t VALUES (3, 'c')",
        ])
        .await;
    trino.copy_out(local.path()).await;
    std::fs::write(local.path().join("s/t/zz_empty"), b"").unwrap();
    trino.push(local.path(), "s/t").await;
    let summary_sql: &str = "SELECT count(*), sum(id) FROM hive.s.t";
    let reference = trino.query(summary_sql).await.rows().clone();
    assert_eq!(reference, json!([[3, 6]]), "Trino 431 reference");

    let lake = open(local.path(), spill.path()).await;
    let compacted = optimize(&lake, "ALTER TABLE hive.s.t EXECUTE optimize").await;
    assert!(compacted.is_ok(), "OPTIMIZE: {compacted:?}");
    trino.push(local.path(), "s/t").await;
    assert_eq!(trino.query(summary_sql).await.rows(), &reference);
    assert_eq!(numbers(&lake, summary_sql).await, Ok(vec![3, 6]));
}

// ---------------------------------------------------------------------------
// CodeLake only: volume, memory, thresholds, generic catalogs
// ---------------------------------------------------------------------------

/// Columns with NULLs, empty strings, unicode, decimals, dates, timestamps.
fn generated(from: u64, to: u64) -> String {
    format!(
        "SELECT CAST(value AS BIGINT) AS id, CAST(value % 7 AS INT) AS grp, \
         CASE WHEN value % 5 = 0 THEN NULL WHEN value % 11 = 0 THEN '' ELSE concat('ação-', CAST(value AS VARCHAR)) END AS s, \
         CAST(value * 1.25 AS DECIMAL(18,4)) AS amount, \
         CASE WHEN value % 3 = 0 THEN NULL ELSE CAST(value AS DOUBLE) / 7 END AS ratio, \
         to_timestamp_millis(value * 1000003) AS ts, \
         CAST(to_timestamp_millis(value * 86400000) AS DATE) AS day, \
         value % 2 = 0 AS even, \
         md5(CAST(value AS VARCHAR)) AS digest \
         FROM generate_series({from}, {to})"
    )
}

/// 60 000 rows in 40 files, compacted into several files (small target),
/// then again into one: every value is the same after each step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sixty_thousand_rows_in_forty_files_keep_every_value() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    sql(
        &lake,
        &format!(
            "CREATE TABLE hive.s.v WITH (format = 'PARQUET') AS {}",
            generated(1, 1500)
        ),
    )
    .await;
    for k in 1..40 {
        sql(
            &lake,
            &format!("INSERT INTO hive.s.v {}", generated(k * 1500 + 1, (k + 1) * 1500)),
        )
        .await;
    }
    let dir = warehouse.path().join("s/v");
    let originals = visible(&dir);
    assert_eq!(originals.len(), 40);
    let all: &str = "SELECT * FROM hive.s.v ORDER BY id";
    let summary_sql: &str = "SELECT count(*), count(DISTINCT id), sum(id), count(s), sum(CAST(grp AS BIGINT)), \
                           count(ratio), sum(CAST(amount * 10000 AS BIGINT)), count(DISTINCT digest) FROM hive.s.v";
    let before = sql(&lake, all).await;
    let summary = numbers(&lake, summary_sql).await.unwrap();
    assert_eq!(summary[0], 60_000);
    assert_eq!(summary[2], 60_000 * 60_001 / 2);

    let small = options(
        &lake,
        &[
            ("hive.non_transactional_optimize_enabled", "true"),
            ("hive.target_max_file_size", "256kB"),
        ],
    );
    batches(&lake, &small, "ALTER TABLE hive.s.v EXECUTE optimize")
        .await
        .unwrap();
    let rolled = visible(&dir);
    assert!(
        rolled.len() >= 2,
        "a 256kB target must roll over: {} file(s)",
        rolled.len()
    );
    assert!(rolled.len() < 40, "{} files", rolled.len());
    assert!(rolled.keys().all(|n| !originals.contains_key(n)));
    assert!(leftovers(&dir).is_empty(), "{:?}", leftovers(&dir));
    assert_eq!(numbers(&lake, summary_sql).await.unwrap(), summary);
    assert_eq!(sql(&lake, all).await, before);

    optimize(&lake, "ALTER TABLE hive.s.v EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(visible(&dir).len(), 1);
    assert_eq!(sql(&lake, all).await, before);
    // A new process reads the same.
    drop(lake);
    let reopened = open(warehouse.path(), spill.path()).await;
    assert_eq!(sql(&reopened, all).await, before);
}

/// Streaming: a pool much smaller than the table compacts it; a pool too
/// small for one row group fails with "Resources exhausted" and leaves the
/// table exactly as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_pool_bounds_the_compaction() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    // ~200 bytes per row in memory, 150 000 rows: ~30 MB, twice the pool.
    let rows = |from: u64, to: u64| {
        format!(
            "SELECT CAST(value AS BIGINT) AS id, concat(md5(CAST(value AS VARCHAR)), md5(CAST(value + 1 AS VARCHAR)), \
             md5(CAST(value + 2 AS VARCHAR)), md5(CAST(value + 3 AS VARCHAR)), md5(CAST(value + 4 AS VARCHAR))) AS payload \
             FROM generate_series({from}, {to})"
        )
    };
    sql(
        &lake,
        &format!(
            "CREATE TABLE hive.s.m WITH (format = 'PARQUET') AS {}",
            rows(1, 15_000)
        ),
    )
    .await;
    for k in 1..10 {
        sql(
            &lake,
            &format!("INSERT INTO hive.s.m {}", rows(k * 15_000 + 1, (k + 1) * 15_000)),
        )
        .await;
    }
    let summary_sql: &str =
        "SELECT count(*), count(DISTINCT payload), sum(id), sum(length(payload)) FROM hive.s.m";
    let expected = numbers(&lake, summary_sql).await.unwrap();
    assert_eq!(expected[0], 150_000);
    let dir = warehouse.path().join("s/m");
    let originals = visible(&dir);

    // Too small for one row group: fails, nothing changes.
    let tiny_spill = tempfile::tempdir().unwrap();
    let tiny = try_open(
        warehouse.path(),
        tiny_spill.path(),
        "hive",
        ByteSize::b(64 * 1024),
    )
    .await
    .unwrap();
    let refused = optimize(&tiny, "ALTER TABLE hive.s.m EXECUTE optimize").await;
    let error = refused.expect_err("a 64 KiB pool cannot hold a row group");
    assert!(error.contains("Resources exhausted"), "{error}");
    assert_eq!(visible(&dir), originals, "a failed OPTIMIZE changed data files");
    assert!(leftovers(&dir).is_empty(), "{:?}", leftovers(&dir));
    drop(tiny);

    // Half the table's in-memory size: streams through.
    let small_spill = tempfile::tempdir().unwrap();
    let small = try_open(warehouse.path(), small_spill.path(), "hive", ByteSize::mib(16))
        .await
        .unwrap();
    optimize(&small, "ALTER TABLE hive.s.m EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(visible(&dir).len(), 1);
    assert_eq!(numbers(&lake, summary_sql).await.unwrap(), expected);
}

/// Exactly the files no larger than the threshold are rewritten; larger
/// ones keep their name and bytes. `0B`, an empty table and a table with a
/// single small file change nothing.
#[tokio::test]
async fn threshold_selects_exactly_the_small_files() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    let rows = |from: u64, to: u64| {
        format!(
            "SELECT CAST(value AS BIGINT) AS id, md5(CAST(value AS VARCHAR)) AS h FROM generate_series({from}, {to})"
        )
    };
    sql(
        &lake,
        &format!(
            "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS {}",
            rows(1, 10)
        ),
    )
    .await;
    sql(&lake, &format!("INSERT INTO hive.s.t {}", rows(11, 40))).await;
    sql(&lake, &format!("INSERT INTO hive.s.t {}", rows(41, 140))).await;
    sql(&lake, &format!("INSERT INTO hive.s.t {}", rows(141, 540))).await;
    let dir = warehouse.path().join("s/t");
    let originals = visible(&dir);
    let mut by_size: Vec<(u64, String)> = originals.iter().map(|(n, b)| (size_of(b), n.clone())).collect();
    by_size.sort();
    assert_eq!(by_size.len(), 4);
    assert!(by_size.windows(2).all(|w| w[0].0 < w[1].0), "{by_size:?}");
    let summary_sql: &str = "SELECT count(*), sum(id), count(DISTINCT h) FROM hive.s.t";
    let expected = vec![540, 540 * 541 / 2, 540];
    assert_eq!(numbers(&lake, summary_sql).await.unwrap(), expected);

    optimize(
        &lake,
        "ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => '0B')",
    )
    .await
    .unwrap();
    assert_eq!(visible(&dir), originals, "0B must not touch any file");

    let threshold = by_size[1].0;
    optimize(
        &lake,
        &format!("ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => '{threshold}B')"),
    )
    .await
    .unwrap();
    let after = visible(&dir);
    assert_eq!(after.len(), 3, "{:?}", after.keys());
    for (_, kept) in &by_size[2..] {
        assert_eq!(
            after.get(kept),
            originals.get(kept),
            "{kept} is above the threshold"
        );
    }
    for (_, gone) in &by_size[..2] {
        assert!(!after.contains_key(gone), "{gone} is at or below the threshold");
    }
    assert_eq!(numbers(&lake, summary_sql).await.unwrap(), expected);

    // A table without files and a table with one file: nothing to do.
    sql(
        &lake,
        "CREATE TABLE hive.s.empty WITH (format = 'PARQUET') AS SELECT CAST(1 AS BIGINT) AS id WHERE 1 = 0",
    )
    .await;
    optimize(&lake, "ALTER TABLE hive.s.empty EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(
        numbers(&lake, "SELECT count(*) FROM hive.s.empty").await.unwrap(),
        vec![0]
    );
    sql(
        &lake,
        &format!(
            "CREATE TABLE hive.s.one WITH (format = 'PARQUET') AS {}",
            rows(1, 5)
        ),
    )
    .await;
    let one = visible(&warehouse.path().join("s/one"));
    optimize(&lake, "ALTER TABLE hive.s.one EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(visible(&warehouse.path().join("s/one")), one);
    assert_eq!(
        numbers(&lake, "SELECT count(*), sum(id) FROM hive.s.one")
            .await
            .unwrap(),
        vec![5, 15]
    );
}

/// Nothing is specific to a catalog called `hive`: a hive catalog attached as
/// `lake` takes `lake.*` session properties, resolves unqualified names from
/// the session's catalog and schema, names itself in errors, and answers
/// with Trino's shape (one `rows` bigint column, no rows).
#[tokio::test]
async fn any_catalog_name_and_session_defaults_work() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = try_open(warehouse.path(), spill.path(), "lake", ByteSize::mib(256))
        .await
        .unwrap();
    sql(&lake, "CREATE SCHEMA lake.s").await;
    sql(
        &lake,
        "CREATE TABLE lake.s.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS BIGINT) AS id",
    )
    .await;
    sql(&lake, "INSERT INTO lake.s.t VALUES (2)").await;
    sql(&lake, "INSERT INTO lake.s.t VALUES (3)").await;
    let dir = warehouse.path().join("s/t");
    assert_eq!(visible(&dir).len(), 3);

    assert!(
        lake.session_properties()
            .session(BTreeMap::from([(
                "hive.non_transactional_optimize_enabled".to_owned(),
                "true".to_owned()
            )]))
            .is_err(),
        "there is no catalog called hive"
    );
    let disabled = batches(
        &lake,
        &QueryOptions::for_user("u"),
        "ALTER TABLE lake.s.t EXECUTE optimize",
    )
    .await
    .unwrap_err();
    assert!(
        disabled.contains(
            "OPTIMIZE procedure must be explicitly enabled via non_transactional_optimize_enabled session property"
        ),
        "{disabled}"
    );
    let mut session = options(&lake, &[("lake.non_transactional_optimize_enabled", "true")]);
    let unknown = batches(
        &lake,
        &session,
        "ALTER TABLE lake.s.t EXECUTE optimize(foo => '1MB')",
    )
    .await
    .unwrap_err();
    assert!(
        unknown.contains("Catalog 'lake' table procedure 'OPTIMIZE' property 'foo' does not exist"),
        "{unknown}"
    );
    session.catalog = Some("lake".into());
    session.schema = Some("s".into());
    let answer = batches(&lake, &session, "ALTER TABLE t EXECUTE optimize")
        .await
        .unwrap();
    let schema = answer.first().map(RecordBatch::schema).unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "rows");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(answer.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(visible(&dir).len(), 1);
    sql(&lake, "INSERT INTO lake.s.t VALUES (4)").await;
    batches(&lake, &session, "ALTER TABLE s.t EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(visible(&dir).len(), 1);
    assert_eq!(
        numbers(&lake, "SELECT count(*), sum(id) FROM lake.s.t")
            .await
            .unwrap(),
        vec![4, 10]
    );
}

// ---------------------------------------------------------------------------
// CodeLake only: concurrency and failures
// ---------------------------------------------------------------------------

async fn seed_counted(lake: &Engine, table: &str, files: u64) {
    let rows = |k: u64| {
        format!(
            "SELECT CAST(value AS BIGINT) AS id, concat('p', CAST(value AS VARCHAR)) AS payload \
             FROM generate_series({}, {})",
            k * 1000,
            k * 1000 + 999
        )
    };
    sql(
        lake,
        &format!(
            "CREATE TABLE hive.s.{table} WITH (format = 'PARQUET') AS {}",
            rows(0)
        ),
    )
    .await;
    for k in 1..files {
        sql(lake, &format!("INSERT INTO hive.s.{table} {}", rows(k))).await;
    }
}

/// Counts rows until `done`: (answers, wrong answers, failures).
async fn read_counts(lake: &Engine, done: &AtomicBool) -> (u32, Vec<Vec<i128>>, Vec<String>) {
    let (mut answers, mut wrong, mut failures) = (0_u32, Vec::new(), Vec::new());
    while !done.load(Ordering::Relaxed) {
        match numbers(
            lake,
            "SELECT count(*), count(DISTINCT id), count(DISTINCT payload) FROM hive.s.c",
        )
        .await
        {
            Ok(v) => {
                answers += 1;
                let valid =
                    v[0] == v[1] && v[1] == v[2] && v[0] % 1000 == 0 && (20_000..=30_000).contains(&v[0]);
                if !valid {
                    wrong.push(v);
                }
            }
            Err(e) => failures.push(e),
        }
    }
    (answers, wrong, failures)
}

/// Readers and writers run while the table is compacted over and over in
/// the same process: no reader ever sees a duplicated or missing batch of
/// rows, no INSERT is lost, every OPTIMIZE succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn readers_and_inserts_during_optimize_never_see_wrong_rows() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    seed_counted(&lake, "c", 20).await;
    let done = AtomicBool::new(false);

    // `Engine::sql_with` futures are not `Send` (DEFECT-10), so the workload
    // runs as concurrent futures on one task instead of spawned tasks.
    let compactions = async {
        let mut results = Vec::new();
        for _ in 0..6 {
            results.push(
                optimize(&lake, "ALTER TABLE hive.s.c EXECUTE optimize")
                    .await
                    .map(|_| ()),
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        results
    };
    let inserts = async {
        for k in 20..30_u64 {
            sql(
                &lake,
                &format!(
                    "INSERT INTO hive.s.c SELECT CAST(value AS BIGINT) AS id, concat('p', CAST(value AS VARCHAR)) \
                     AS payload FROM generate_series({}, {})",
                    k * 1000,
                    k * 1000 + 999
                ),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    };
    let writers = async {
        let (results, ()) = tokio::join!(compactions, inserts);
        done.store(true, Ordering::Relaxed);
        results
    };
    let (compacted, first, second, third) = tokio::join!(
        writers,
        read_counts(&lake, &done),
        read_counts(&lake, &done),
        read_counts(&lake, &done)
    );
    let mut answers = 0;
    for (count, wrong, failures) in [first, second, third] {
        answers += count;
        assert!(
            wrong.is_empty(),
            "wrong answers during OPTIMIZE (count, distinct ids, distinct payloads): {wrong:?}"
        );
        // ADR 0009: a query listed before a swap may fail loudly on a
        // removed file; any other failure is a defect.
        for failure in failures {
            assert!(
                failure.contains("not found")
                    || failure.contains("No such file")
                    || failure.contains("NotFound"),
                "reader failed: {failure}"
            );
        }
    }
    assert!(answers > 0, "no reader finished a query");
    assert!(compacted.iter().all(Result::is_ok), "{compacted:?}");
    assert_eq!(
        numbers(
            &lake,
            "SELECT count(*), count(DISTINCT id), sum(id), count(DISTINCT payload) FROM hive.s.c"
        )
        .await
        .unwrap(),
        vec![30_000, 30_000, 29_999 * 30_000 / 2, 30_000]
    );
    optimize(&lake, "ALTER TABLE hive.s.c EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(visible(&warehouse.path().join("s/c")).len(), 1);
    assert_eq!(
        numbers(&lake, "SELECT count(*), sum(id) FROM hive.s.c")
            .await
            .unwrap(),
        vec![30_000, 29_999 * 30_000 / 2]
    );
}

/// A table large enough for OPTIMIZE to take a while in a debug build.
async fn seed_large(lake: &Engine, table: &str, files: u64, rows_per_file: u64) {
    let rows = |from: u64, to: u64| {
        format!(
            "SELECT CAST(value AS BIGINT) AS id, concat(md5(CAST(value AS VARCHAR)), md5(CAST(value + 7 AS VARCHAR))) AS payload \
             FROM generate_series({from}, {to})"
        )
    };
    sql(
        lake,
        &format!(
            "CREATE TABLE hive.s.{table} WITH (format = 'PARQUET') AS {}",
            rows(1, rows_per_file)
        ),
    )
    .await;
    for k in 1..files {
        sql(
            lake,
            &format!(
                "INSERT INTO hive.s.{table} {}",
                rows(k * rows_per_file + 1, (k + 1) * rows_per_file)
            ),
        )
        .await;
    }
}

fn has_inprogress(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|entries| {
        entries
            .filter_map(Result::ok)
            // `.<name>.inprogress#<n>`: the object store stages the upload.
            .any(|e| e.file_name().to_string_lossy().contains(".inprogress"))
    })
}

/// `ADD COLUMN` + `INSERT` issued while a second OPTIMIZE of the same table
/// waits for the first: the second one must compact the new file with the
/// new column, not with the schema it saw when it was submitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn column_added_while_optimize_waits_keeps_its_values() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    seed_large(&lake, "w", 40, 30_000).await;
    let dir = warehouse.path().join("s/w");
    let first_done = AtomicBool::new(false);

    let first = async {
        let result = optimize(&lake, "ALTER TABLE hive.s.w EXECUTE optimize")
            .await
            .map(|_| ());
        first_done.store(true, Ordering::SeqCst);
        result
    };
    let rest = async {
        assert!(
            wait_until(Duration::from_secs(120), || has_inprogress(&dir)).await,
            "the first OPTIMIZE never started writing"
        );
        let second = optimize(&lake, "ALTER TABLE hive.s.w EXECUTE optimize");
        let schema_change = async {
            // Let the second statement be planned and queue behind the first.
            tokio::time::sleep(Duration::from_millis(150)).await;
            sql(&lake, "ALTER TABLE hive.s.w ADD COLUMN c varchar").await;
            sql(
                &lake,
                "INSERT INTO hive.s.w VALUES (-1, 'x', 'keep-me'), (-2, 'y', 'me-too')",
            )
            .await;
            !first_done.load(Ordering::SeqCst)
        };
        tokio::join!(second, schema_change)
    };
    let (first, (second, first_still_running)) = tokio::join!(first, rest);
    assert!(
        first_still_running,
        "inconclusive: the first OPTIMIZE ended before the schema change; enlarge seed_large"
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(
        sql(&lake, "SELECT id, c FROM hive.s.w WHERE id < 0 ORDER BY id").await,
        "+----+---------+\n| id | c       |\n+----+---------+\n| -2 | me-too  |\n| -1 | keep-me |\n+----+---------+"
    );
    assert_eq!(
        numbers(&lake, "SELECT count(*), count(c) FROM hive.s.w")
            .await
            .unwrap(),
        vec![1_200_002, 2]
    );
}

/// `DROP TABLE` while OPTIMIZE is writing: the OPTIMIZE fails or finds
/// nothing, and a table created again under the same name holds only its own
/// rows (no file of the dropped table comes back).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_table_during_optimize_leaves_nothing_behind() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    seed_large(&lake, "d", 12, 25_000).await;
    let dir = warehouse.path().join("s/d");
    let running = async {
        optimize(&lake, "ALTER TABLE hive.s.d EXECUTE optimize")
            .await
            .map(|_| ())
    };
    let drop_it = async {
        assert!(
            wait_until(Duration::from_secs(120), || has_inprogress(&dir)).await,
            "OPTIMIZE never started writing"
        );
        sql(&lake, "DROP TABLE hive.s.d").await;
    };
    let (outcome, ()) = tokio::join!(running, drop_it);
    sql(&lake, "CREATE TABLE hive.s.d WITH (format = 'PARQUET') AS SELECT CAST(42 AS BIGINT) AS id, 'new' AS payload").await;
    assert_eq!(
        numbers(&lake, "SELECT count(*), sum(id) FROM hive.s.d")
            .await
            .unwrap(),
        vec![1, 42],
        "OPTIMIZE outcome: {outcome:?}"
    );
    let remaining: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".inprogress") || n == MARKER)
        .collect();
    assert!(remaining.is_empty(), "{remaining:?} (OPTIMIZE: {outcome:?})");
}

/// Another node (engine with its own locks) restarts again and again while
/// this one compacts: its catalog open runs the crash recovery on the live
/// swap. It must neither fail to start nor undo a swap in progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn node_restarting_during_another_nodes_optimize_loses_nothing() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let node_a = Arc::new(open(warehouse.path(), spill.path()).await);
    sql(&node_a, "CREATE SCHEMA hive.s").await;
    let rows = |k: u64| {
        format!(
            "SELECT CAST(value AS BIGINT) AS id, md5(CAST(value AS VARCHAR)) AS h FROM generate_series({}, {})",
            k * 300,
            k * 300 + 299
        )
    };
    sql(
        &node_a,
        &format!("CREATE TABLE hive.s.r WITH (format = 'PARQUET') AS {}", rows(0)),
    )
    .await;
    for k in 1..80 {
        sql(&node_a, &format!("INSERT INTO hive.s.r {}", rows(k))).await;
    }
    let summary_sql: &str = "SELECT count(*), count(DISTINCT id), sum(id), count(DISTINCT h) FROM hive.s.r";
    let expected = vec![24_000, 24_000, 23_999 * 24_000 / 2, 24_000];
    assert_eq!(numbers(&node_a, summary_sql).await.unwrap(), expected);
    // A tiny target keeps the number of files (and the swap) large.
    let tiny_target = options(
        &node_a,
        &[
            ("hive.non_transactional_optimize_enabled", "true"),
            ("hive.target_max_file_size", "4kB"),
        ],
    );
    let mut restarts = 0_u32;
    let mut failed_starts = Vec::new();
    for round in 0..6 {
        let done = Arc::new(AtomicBool::new(false));
        let restarter = {
            let done = Arc::clone(&done);
            let warehouse = warehouse.path().to_owned();
            tokio::spawn(async move {
                let spill = tempfile::tempdir().unwrap();
                let (mut count, mut failures) = (0_u32, Vec::new());
                while !done.load(Ordering::Relaxed) {
                    match try_open(&warehouse, spill.path(), "hive", ByteSize::mib(64)).await {
                        Ok(_) => count += 1,
                        Err(e) => failures.push(e),
                    }
                }
                (count, failures)
            })
        };
        let result = batches(&node_a, &tiny_target, "ALTER TABLE hive.s.r EXECUTE optimize").await;
        done.store(true, Ordering::Relaxed);
        let (count, failures) = restarter.await.unwrap();
        restarts += count;
        failed_starts.extend(failures);
        // The compacting node may lose the race against a recovery that
        // finished its swap, but the rows must be intact either way.
        let fresh_spill = tempfile::tempdir().unwrap();
        let observer = open(warehouse.path(), fresh_spill.path()).await;
        let dir = warehouse.path().join("s/r");
        assert_eq!(
            numbers(&observer, summary_sql).await.unwrap(),
            expected,
            "round {round}: OPTIMIZE {:?}; {count} restarts during it; {} visible files, leftovers {:?}; \
             failed starts: {failed_starts:?}",
            result.map(|_| ()),
            visible(&dir).len(),
            leftovers(&dir)
        );
        assert!(
            leftovers(&warehouse.path().join("s/r")).is_empty(),
            "round {round}: {:?}",
            leftovers(&warehouse.path().join("s/r"))
        );
    }
    assert!(restarts > 0);
    assert!(
        failed_starts.is_empty(),
        "a node failed to start during a swap: {failed_starts:?}"
    );
}

/// One crash point rebuilt on disk from the snapshot before OPTIMIZE.
struct CrashState {
    name: &'static str,
    /// The new file under its hidden temporary name.
    hidden_new: bool,
    /// The new file under its final name.
    visible_new: bool,
    /// How many old files were already deleted.
    delete_old: usize,
    marker: Option<String>,
    /// Recovery finishes the swap (else the old files stay).
    forward: bool,
}

/// The crash points of ADR 0009, all with the same `marker` content.
fn crash_states(marker: &str) -> Vec<CrashState> {
    let state = |name, hidden_new, visible_new, delete_old, with_marker: bool, forward| CrashState {
        name,
        hidden_new,
        visible_new,
        delete_old,
        marker: with_marker.then(|| marker.to_owned()),
        forward,
    };
    vec![
        state("after marker, before renames", true, false, 0, true, true),
        state("after renames, before deletes", false, true, 0, true, true),
        state("in the middle of deletes", false, true, 2, true, true),
        state("before the marker is removed", false, true, 4, true, true),
        state("new file lost, old files intact", false, false, 0, true, false),
        state(
            "crash before the marker (orphan temporary)",
            true,
            false,
            0,
            false,
            false,
        ),
    ]
}

/// Crash states rebuilt from a real run (old files, the new file, the
/// marker), as ADR 0009 lists them: a new process opening the catalog
/// finishes or undoes the swap and every row is there exactly once.
#[tokio::test]
async fn every_crash_state_recovers_on_restart_with_rows_exactly_once() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    seed_counted(&lake, "k", 4).await;
    let dir = warehouse.path().join("s/k");
    let snapshot = warehouse.path().join("snapshot");
    copy_dir(&dir, &snapshot);
    let old = visible(&dir);
    optimize(&lake, "ALTER TABLE hive.s.k EXECUTE optimize")
        .await
        .unwrap();
    let new = visible(&dir);
    assert_eq!(new.len(), 1);
    let (new_name, new_bytes) = new.iter().next().map(|(n, b)| (n.clone(), b.clone())).unwrap();
    drop(lake);
    let hidden = format!(".{new_name}.inprogress");
    let removed: Vec<(String, u64)> = old.iter().map(|(n, b)| (n.clone(), size_of(b))).collect();
    let added = vec![(hidden.clone(), new_name.clone(), 4000)];
    let old_names: Vec<String> = old.keys().cloned().collect();
    let summary_sql: &str = "SELECT count(*), count(DISTINCT id), sum(id) FROM hive.s.k";
    let expected = vec![4000, 4000, 3999 * 4000 / 2];

    let full = marker(&removed, &added);
    let states = crash_states(&full);
    for state in &states {
        replace_dir(&snapshot, &dir);
        if state.hidden_new {
            std::fs::write(dir.join(&hidden), &new_bytes).unwrap();
        }
        if state.visible_new {
            std::fs::write(dir.join(&new_name), &new_bytes).unwrap();
        }
        for name in old_names.iter().take(state.delete_old) {
            std::fs::remove_file(dir.join(name)).unwrap();
        }
        if let Some(marker) = &state.marker {
            std::fs::write(dir.join(MARKER), marker).unwrap();
        }
        let restarted = try_open(warehouse.path(), spill.path(), "hive", ByteSize::mib(256))
            .await
            .unwrap_or_else(|e| panic!("{}: restart failed: {e}", state.name));
        assert_eq!(
            numbers(&restarted, summary_sql).await.unwrap(),
            expected,
            "{}",
            state.name
        );
        let files: BTreeSet<String> = visible(&dir).into_keys().collect();
        let wanted: BTreeSet<String> = if state.forward {
            BTreeSet::from([new_name.clone()])
        } else {
            old_names.iter().cloned().collect()
        };
        assert_eq!(files, wanted, "{}", state.name);
        assert!(!dir.join(MARKER).exists(), "{}: marker left", state.name);
        // The next OPTIMIZE works from here and keeps the rows.
        optimize(&restarted, "ALTER TABLE hive.s.k EXECUTE optimize")
            .await
            .unwrap();
        assert_eq!(
            numbers(&restarted, summary_sql).await.unwrap(),
            expected,
            "{}",
            state.name
        );
        assert_eq!(visible(&dir).len(), 1, "{}", state.name);
    }
}

/// A marker that cannot be read is a problem of one table: the other tables
/// of the catalog stay available (Trino keeps serving them too), and the
/// damaged table refuses OPTIMIZE instead of guessing.
#[tokio::test]
async fn unreadable_marker_only_affects_its_table() {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let lake = open(warehouse.path(), spill.path()).await;
    sql(&lake, "CREATE SCHEMA hive.s").await;
    seed_counted(&lake, "damaged", 2).await;
    seed_counted(&lake, "healthy", 2).await;
    drop(lake);
    std::fs::write(warehouse.path().join("s/damaged").join(MARKER), b"{ not json").unwrap();
    let restarted = try_open(warehouse.path(), spill.path(), "hive", ByteSize::mib(256)).await;
    let restarted = restarted.unwrap_or_else(|e| panic!("the catalog must attach: {e}"));
    assert_eq!(
        numbers(&restarted, "SELECT count(*), sum(id) FROM hive.s.healthy")
            .await
            .unwrap(),
        vec![2000, 1999 * 1000]
    );
    let refused = optimize(&restarted, "ALTER TABLE hive.s.damaged EXECUTE optimize").await;
    assert!(refused.is_err(), "{refused:?}");
    assert_eq!(visible(&warehouse.path().join("s/damaged")).len(), 2);
}
