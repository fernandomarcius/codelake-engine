//! F7 acceptance suite, written by an independent tester: black-box checks of
//! the OTLP tracing contract (ADR 0010) against a real collector (Jaeger v2,
//! read back through its OTLP-shaped API v3), real sources (SQL Server 2022, a
//! Hive warehouse on disk), the real `codelake` binary, and a real Trino 431
//! wherever Trino's behaviour is the contract (W3C `traceparent`/`tracestate`
//! handling, query id shape).
//!
//! Every test checks values: the rows the query returned *and* the numbers the
//! spans report (rows, files, bytes), never only "no error". Containers are
//! named `codelake-f7-acc-*` and removed when the test ends.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::net::TcpListener as StdListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::config::{ExecutionConfig, MemoryConfig, TelemetryConfig};
use codelake_core::{ByteSize, Engine, QueryOptions, QueryTrace};
use codelake_server::telemetry::{self, Telemetry};
use datafusion::arrow::array::{Array, Int32Array, Int64Array, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use secrecy::SecretString;
use serde_json::Value;
use testcontainers::core::{Host, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const JAEGER: (&str, &str) = ("jaegertracing/jaeger", "2.21.0");
const TRINO: (&str, &str) = ("trinodb/trino", "431");
const SA_PASSWORD: &str = "F7-Acceptance-Only-1";
/// Rows of `dbo.Docs`: ids 1..=`DOCS`, `Grp = Id % 7`.
const DOCS: i64 = 50_000;
/// `dbo.Docs.Doc` is the id as text except at this id, where it holds
/// [`CONFIDENTIAL`]: a data value that must never reach the collector.
const BAD_ROW: i64 = 30_000;
const CONFIDENTIAL: &str = "CONFIDENTIAL-4711";
/// Span kinds as the OTLP JSON encodes them.
const KIND_SERVER: i64 = 2;
const KIND_CLIENT: i64 = 3;
/// OTLP status code for errors.
const STATUS_ERROR: i64 = 2;

fn host() -> String {
    std::env::var("TESTCONTAINERS_HOST_OVERRIDE").unwrap_or_else(|_| "127.0.0.1".to_owned())
}

fn container_name(tag: &str) -> String {
    format!("codelake-f7-acc-{tag}-{}", std::process::id())
}

fn service_name(tag: &str) -> String {
    format!("codelake-f7-acc-{tag}-{}", std::process::id())
}

/// A W3C `traceparent` for test case `n` (unique per process) and its trace id.
fn traceparent(n: u32, flags: &str) -> (String, String) {
    let trace = format!("f7acc{:011x}{n:016x}", std::process::id());
    (format!("00-{trace}-b7ad6b71{n:08x}-{flags}"), trace)
}

// ---------------------------------------------------------------------------
// Collector
// ---------------------------------------------------------------------------

struct Jaeger {
    _container: ContainerAsync<GenericImage>,
    otlp_http: String,
    otlp_grpc_port: u16,
    api: String,
}

async fn jaeger(tag: &str) -> Jaeger {
    let container = GenericImage::new(JAEGER.0, JAEGER.1)
        .with_exposed_port(4317.tcp())
        .with_exposed_port(4318.tcp())
        .with_exposed_port(16686.tcp())
        .with_wait_for(WaitFor::message_on_either_std("Everything is ready"))
        .with_container_name(container_name(tag))
        .start()
        .await
        .expect("jaeger starts");
    let host = host();
    let http = container.get_host_port_ipv4(4318).await.unwrap();
    let grpc = container.get_host_port_ipv4(4317).await.unwrap();
    let api = container.get_host_port_ipv4(16686).await.unwrap();
    Jaeger {
        _container: container,
        otlp_http: format!("http://{host}:{http}"),
        otlp_grpc_port: grpc,
        api: format!("http://{host}:{api}"),
    }
}

/// One span as Jaeger's API v3 returns it (OTLP JSON).
#[derive(Debug, Clone)]
struct OSpan {
    trace: String,
    id: String,
    parent: Option<String>,
    name: String,
    state: Option<String>,
    kind: i64,
    status: i64,
    message: String,
    attrs: BTreeMap<String, String>,
}

impl OSpan {
    fn attr(&self, key: &str) -> Option<&str> {
        self.attrs.get(key).map(String::as_str)
    }

    fn rows(&self) -> Option<u64> {
        self.attr("db.response.returned_rows").map(|v| v.parse().unwrap())
    }

    fn cancelled(&self) -> bool {
        self.attr("codelake.cancelled") == Some("true")
    }

    /// `true` when `text` appears in any exported attribute or the status.
    fn carries(&self, text: &str) -> bool {
        self.message.contains(text) || self.attrs.values().any(|v| v.contains(text))
    }
}

fn value_text(value: &Value) -> String {
    match value.as_object().and_then(|o| o.values().next()) {
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
        None => value.to_string(),
    }
}

fn parse_span(span: &Value) -> OSpan {
    let text = |key: &str| span[key].as_str().filter(|s| !s.is_empty()).map(str::to_owned);
    OSpan {
        trace: text("traceId").unwrap(),
        id: text("spanId").unwrap(),
        parent: text("parentSpanId"),
        name: text("name").unwrap(),
        state: text("traceState"),
        kind: span["kind"].as_i64().unwrap_or(0),
        status: span["status"]["code"].as_i64().unwrap_or(0),
        message: span["status"]["message"].as_str().unwrap_or("").to_owned(),
        attrs: span["attributes"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|a| (a["key"].as_str().unwrap().to_owned(), value_text(&a["value"])))
            .collect(),
    }
}

fn parse_spans(json: &Value) -> Vec<OSpan> {
    let mut out = Vec::new();
    for resource in json["result"]["resourceSpans"].as_array().into_iter().flatten() {
        for scope in resource["scopeSpans"].as_array().into_iter().flatten() {
            out.extend(scope["spans"].as_array().into_iter().flatten().map(parse_span));
        }
    }
    out
}

impl Jaeger {
    async fn get(&self, path: &str) -> (Value, String) {
        let body = reqwest::get(format!("{}{path}", self.api))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let json = serde_json::from_str(&body).unwrap_or(Value::Null);
        (json, body)
    }

    /// Spans of one trace plus the raw JSON (for leak checks over everything
    /// exported: attributes, status messages, events).
    async fn trace(&self, trace_id: &str) -> (Vec<OSpan>, String) {
        let (json, raw) = self.get(&format!("/api/v3/traces/{trace_id}")).await;
        (parse_spans(&json), raw)
    }

    /// Polls a trace until `ready` holds (ingestion is asynchronous).
    async fn wait_trace(&self, trace_id: &str, ready: impl Fn(&[OSpan]) -> bool) -> (Vec<OSpan>, String) {
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            let (spans, raw) = self.trace(trace_id).await;
            if ready(&spans) {
                return (spans, raw);
            }
            assert!(
                Instant::now() < deadline,
                "trace {trace_id} incomplete: {spans:#?}"
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// Every span stored for `service`, plus the raw JSON.
    async fn service(&self, service: &str) -> (Vec<OSpan>, String) {
        let (json, raw) = self
            .get(&format!(
                "/api/v3/traces?query.service_name={service}\
                 &query.start_time_min=2000-01-01T00:00:00Z&query.start_time_max=2100-01-01T00:00:00Z\
                 &query.search_depth=1000"
            ))
            .await;
        (parse_spans(&json), raw)
    }

    async fn wait_service(&self, service: &str, ready: impl Fn(&[OSpan]) -> bool) -> (Vec<OSpan>, String) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let (spans, raw) = self.service(service).await;
            if ready(&spans) {
                return (spans, raw);
            }
            assert!(
                Instant::now() < deadline,
                "service {service} incomplete: {} spans",
                spans.len()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

fn named<'a>(spans: &'a [OSpan], name: &str) -> Vec<&'a OSpan> {
    spans.iter().filter(|s| s.name == name).collect()
}

fn the<'a>(spans: &'a [OSpan], name: &str) -> &'a OSpan {
    let found = named(spans, name);
    assert_eq!(found.len(), 1, "exactly one {name}: {spans:#?}");
    found[0]
}

/// The `codelake.query` span with this query id.
fn query_span<'a>(spans: &'a [OSpan], query_id: &str) -> &'a OSpan {
    spans
        .iter()
        .find(|s| s.name == "codelake.query" && s.attr("codelake.query_id") == Some(query_id))
        .unwrap_or_else(|| panic!("no query {query_id}"))
}

fn has_ancestor(spans: &[OSpan], span: &OSpan, ancestor: &OSpan) -> bool {
    let mut parent = span.parent.as_deref();
    while let Some(id) = parent {
        if id == ancestor.id {
            return true;
        }
        parent = spans
            .iter()
            .find(|s| s.id == id)
            .and_then(|s| s.parent.as_deref());
    }
    false
}

/// Spans called `name` somewhere below `query`.
fn below<'a>(spans: &'a [OSpan], query: &OSpan, name: &str) -> Vec<&'a OSpan> {
    spans
        .iter()
        .filter(|s| s.name == name && has_ancestor(spans, s, query))
        .collect()
}

/// Every span except the query roots has its parent inside the same trace:
/// no source span lost its query (it would be an orphan root elsewhere).
fn assert_no_orphans(spans: &[OSpan]) {
    for span in spans.iter().filter(|s| s.name != "codelake.query") {
        let parent = span
            .parent
            .as_deref()
            .unwrap_or_else(|| panic!("root span {} outside any query: {span:#?}", span.name));
        assert!(
            spans.iter().any(|s| s.id == parent && s.trace == span.trace),
            "{} has a parent outside its trace: {span:#?}",
            span.name
        );
    }
}

// ---------------------------------------------------------------------------
// Engine, sources, exporter
// ---------------------------------------------------------------------------

fn memory(spill: &Path) -> MemoryConfig {
    MemoryConfig {
        limit: ByteSize::mib(128),
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    }
}

async fn engine(spill: &Path, warehouse: Option<&Path>, mssql_port: Option<u16>) -> Engine {
    let mut engine = Engine::new(&memory(spill), &ExecutionConfig::default()).unwrap();
    let timeout = Duration::from_secs(60);
    if let Some(port) = mssql_port {
        let sqlserver = SqlServerConnector::new(
            "erp".into(),
            SqlServerParams {
                host: host(),
                port,
                database: "acc".into(),
                user: "sa".into(),
                password: SecretString::from(SA_PASSWORD),
                encrypt: "required".into(),
                trust_server_certificate: true,
                pool_size: 2,
            },
        );
        engine.attach(Arc::new(sqlserver), true, timeout).await.unwrap();
    }
    if let Some(warehouse) = warehouse {
        let hive = HiveConnector::new(
            "hive".into(),
            HiveParams {
                warehouse: warehouse.display().to_string(),
                s3_endpoint: None,
                s3_region: "us-east-1".into(),
                credentials: None,
                path_style: false,
            },
        );
        engine.attach(Arc::new(hive), true, timeout).await.unwrap();
    }
    engine
}

fn export_to(endpoint: &str, service: &str, ratio: f64, timeout_secs: u64) -> Telemetry {
    let config = TelemetryConfig {
        otlp_endpoint: Some(endpoint.to_owned()),
        service_name: service.to_owned(),
        sampling_ratio: ratio,
        export_timeout_secs: timeout_secs,
        ..TelemetryConfig::default()
    };
    let telemetry = telemetry::init(Some(&config)).unwrap();
    assert!(telemetry.is_exporting());
    telemetry
}

async fn flush(telemetry: Telemetry) -> Telemetry {
    tokio::task::spawn_blocking(move || {
        telemetry.flush();
        telemetry
    })
    .await
    .unwrap()
}

async fn shutdown(telemetry: Telemetry) {
    tokio::task::spawn_blocking(move || telemetry.shutdown())
        .await
        .unwrap();
}

fn traced(user: &str, header: &str, query_id: &str) -> QueryOptions {
    QueryOptions::for_user(user)
        .with_trace(QueryTrace::from_headers(Some(header), None, None).with_query_id(query_id))
}

/// Runs a traced query to the end and returns its rows.
async fn run(engine: &Engine, options: &QueryOptions, sql: &str) -> Vec<RecordBatch> {
    engine
        .query(options, sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("running `{sql}`: {e}"))
}

/// Column `column` as i64, whatever integer width the plan chose.
fn ints(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch.column(column);
        if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
            out.extend(a.iter().map(Option::unwrap));
        } else if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
            out.extend(a.iter().map(|v| i64::from(v.unwrap())));
        } else if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
            out.extend(a.iter().map(|v| i64::try_from(v.unwrap()).unwrap()));
        } else {
            panic!("column {column} is {:?}", array.data_type());
        }
    }
    out
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

async fn tsql(port: u16, database: &str, batch: &str) {
    let mut config = Config::new();
    config.host(host());
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server("sa", SA_PASSWORD));
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
    let mut client = Client::connect(config, tcp.compat_write()).await.unwrap();
    client
        .simple_query(batch)
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
}

/// SQL Server 2022 with `acc.dbo.Docs` ([`DOCS`] rows) and the view
/// `acc.dbo.DocNumbers`, whose `CAST(Doc AS INT)` fails at [`BAD_ROW`].
async fn sql_server(tag: &str) -> (ContainerAsync<MssqlServer>, u16) {
    let mssql = MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag("2022-latest")
        .with_container_name(container_name(tag))
        .start()
        .await
        .unwrap();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    tsql(port, "master", "CREATE DATABASE acc").await;
    tsql(
        port,
        "acc",
        &format!(
            "CREATE TABLE dbo.Docs (Id INT PRIMARY KEY, Grp INT NOT NULL, Doc NVARCHAR(40) NOT NULL);
             WITH n AS (SELECT TOP ({DOCS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                        FROM sys.all_objects a CROSS JOIN sys.all_objects b)
             INSERT INTO dbo.Docs (Id, Grp, Doc)
             SELECT i, i % 7, CASE WHEN i = {BAD_ROW} THEN N'{CONFIDENTIAL}' ELSE CAST(i AS NVARCHAR(40)) END FROM n;"
        ),
    )
    .await;
    tsql(
        port,
        "acc",
        "CREATE VIEW dbo.DocNumbers AS SELECT Id, CAST(Doc AS INT) AS N FROM dbo.Docs",
    )
    .await;
    (mssql, port)
}

/// Data files of a Hive table on disk (what `hive.scan` must report).
fn data_files(dir: &Path) -> BTreeMap<String, u64> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .filter(|e| !e.file_name().to_string_lossy().starts_with(['.', '_']))
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().unwrap().len(),
            )
        })
        .collect()
}

fn sum_to(last: i64) -> i64 {
    last * (last + 1) / 2
}

// ---------------------------------------------------------------------------
// 1. Config: what check-config accepts, serve must be able to use
// ---------------------------------------------------------------------------

/// A server config with `[telemetry] otlp_endpoint`; returns it and its ops
/// HTTP address.
fn telemetry_config(dir: &Path, index: usize, endpoint: &str) -> (PathBuf, String) {
    let (pg, http) = (super::free_port(), super::free_port());
    let config = super::write_config(dir, pg, http);
    let mut text = std::fs::read_to_string(&config).unwrap();
    write!(text, "\n[telemetry]\notlp_endpoint = \"{endpoint}\"\n").unwrap();
    let path = dir.join(format!("telemetry-{index}.toml"));
    std::fs::write(&path, text).unwrap();
    (path, format!("127.0.0.1:{http}"))
}

/// Why an accepted endpoint is unusable, if it is.
fn traces_url_problem(path: &Path) -> Option<String> {
    let loaded = codelake_core::Config::load(path).unwrap();
    let url = loaded.config.telemetry.traces_url().unwrap();
    match reqwest::Url::parse(&url) {
        Ok(parsed)
            if parsed.host_str().is_some_and(|h| !h.is_empty())
                && parsed.path().ends_with("/v1/traces")
                && parsed.query().is_none()
                && parsed.fragment().is_none() =>
        {
            None
        }
        Ok(_) => Some(format!("traces URL is `{url}`")),
        Err(err) => Some(format!("traces URL `{url}` is invalid: {err}")),
    }
}

/// Starts `serve`; `Some(exit code)` if it quit instead of serving.
async fn serve_exit(path: &Path, http: &str) -> Option<Option<i32>> {
    let mut server = super::Server(
        Command::new(super::BIN)
            .args(["serve", "--config"])
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(exit) = server.0.try_wait().unwrap() {
            return Some(exit.code());
        }
        if reqwest::get(format!("http://{http}/livez")).await.is_ok() {
            return None;
        }
        assert!(Instant::now() < deadline, "serve neither ran nor exited");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Fail-fast contract: an endpoint `check-config` accepts must be one the
/// exporter can send to (`serve` does not refuse it at startup) and whose
/// traces URL is well formed: a host, a valid port, path `…/v1/traces`, no
/// query or fragment that would swallow the path.
#[tokio::test]
async fn f7_config_check_config_accepts_only_endpoints_serve_can_use() {
    let dir = tempfile::tempdir().unwrap();
    let mut violations = Vec::new();
    let mut accepted = 0;
    for (index, endpoint) in [
        "http://127.0.0.1:4318",
        "http://collector.local:4318/otlp",
        "http://:4318",
        "http://collector.local:99999",
        "http://127.0.0.1:4318?token=abc",
        "http://127.0.0.1:4318#frag",
    ]
    .into_iter()
    .enumerate()
    {
        let (path, http) = telemetry_config(dir.path(), index, endpoint);
        let check = Command::new(super::BIN)
            .args(["check-config", "--config"])
            .arg(&path)
            .output()
            .unwrap();
        if check.status.code() != Some(0) {
            continue; // refused up front: that is fail-fast
        }
        accepted += 1;
        if let Some(problem) = traces_url_problem(&path) {
            violations.push(format!("{endpoint}: accepted, but {problem}"));
        }
        if let Some(code) = serve_exit(&path, &http).await {
            violations.push(format!("{endpoint}: check-config exit 0, serve exited {code:?}"));
        }
    }
    assert!(accepted >= 2, "the two well-formed endpoints are accepted");
    assert!(
        violations.is_empty(),
        "endpoints accepted by validation but unusable:\n{}",
        violations.join("\n")
    );
}

// ---------------------------------------------------------------------------
// 2. W3C trace context: same decisions as Trino 431
// ---------------------------------------------------------------------------

async fn trino(otlp_grpc_port: u16) -> (ContainerAsync<GenericImage>, String) {
    let config = format!(
        "coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port=8080\n\
         discovery.uri=http://localhost:8080\ntracing.enabled=true\n\
         tracing.exporter.endpoint=http://host.docker.internal:{otlp_grpc_port}\n"
    );
    let jvm = "-server\n-Xmx1G\n-XX:+ExitOnOutOfMemoryError\n-Djdk.attach.allowAttachSelf=true\n";
    let container = GenericImage::new(TRINO.0, TRINO.1)
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::message_on_either_std("SERVER STARTED"))
        .with_copy_to("/etc/trino/config.properties", config.into_bytes())
        .with_copy_to("/etc/trino/jvm.config", jvm.as_bytes().to_vec())
        .with_host("host.docker.internal", Host::HostGateway)
        .with_startup_timeout(Duration::from_secs(180))
        .with_container_name(container_name("trino"))
        .start()
        .await
        .expect("trino starts");
    let port = container.get_host_port_ipv4(8080).await.unwrap();
    let url = format!("http://{}:{port}", host());
    wait_for_active_worker(&url).await;
    (container, url)
}

/// Waits until the coordinator lists an active worker: it logs "SERVER
/// STARTED" before its own worker registers, and a query sent in between
/// fails with "No nodes available to run query".
async fn wait_for_active_worker(url: &str) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let mut workers = None;
        let mut response = client
            .post(format!("{url}/v1/statement"))
            .header("X-Trino-User", "acc")
            .body("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")
            .send()
            .await
            .ok();
        while let Some(sent) = response.take() {
            let Some(page) = sent
                .text()
                .await
                .ok()
                .and_then(|body| serde_json::from_str::<Value>(&body).ok())
            else {
                break;
            };
            if let Some(count) = page["data"][0][0].as_u64() {
                workers = Some(count);
            }
            if let Some(next) = page["nextUri"].as_str() {
                response = client.get(next).send().await.ok();
            }
        }
        if workers.is_some_and(|count| count > 0) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the reference server never listed an active worker"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Runs `SELECT 1` on Trino with the given headers; returns the query id and
/// the value.
async fn trino_select_one(url: &str, traceparent: &str, tracestate: &str) -> (String, i64) {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("{url}/v1/statement"))
        .header("X-Trino-User", "acc")
        .header("traceparent", traceparent);
    if !tracestate.is_empty() {
        request = request.header("tracestate", tracestate);
    }
    let body = request
        .body("SELECT 1")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let mut response: Value = serde_json::from_str(&body).unwrap();
    let id = response["id"].as_str().unwrap().to_owned();
    let mut value = None;
    loop {
        assert!(response["error"].is_null(), "{response}");
        if let Some(data) = response["data"].as_array() {
            value = data[0][0].as_i64();
        }
        let Some(next) = response["nextUri"].as_str().map(str::to_owned) else {
            break;
        };
        let body = client.get(next).send().await.unwrap().text().await.unwrap();
        response = serde_json::from_str(&body).unwrap();
    }
    (id, value.unwrap())
}

/// How one system handled one incoming context.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Decision {
    /// Nothing exported for the query.
    Dropped,
    /// Exported in the caller's trace; `tracestate` as the spans carry it.
    Adopted(Option<String>),
    /// Exported in a trace of its own.
    NewTrace,
}

fn decide(root: Option<&OSpan>, header_trace: &str) -> Decision {
    match root {
        None => Decision::Dropped,
        Some(span) if span.trace == header_trace => Decision::Adopted(span.state.clone()),
        Some(_) => Decision::NewTrace,
    }
}

fn is_trino_query_id(id: &str) -> bool {
    let parts: Vec<&str> = id.split('_').collect();
    parts.len() == 4
        && [8, 6, 5, 5]
            .iter()
            .zip(&parts)
            .all(|(len, part)| part.len() == *len)
        && parts[..3].iter().all(|p| p.bytes().all(|b| b.is_ascii_digit()))
        && parts[3]
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Seconds since the epoch of a Trino-shaped id's `YYYYMMDD_HHMMSS` (UTC).
fn query_id_epoch(id: &str) -> i64 {
    let field = |range: std::ops::Range<usize>| id[range].parse::<i64>().unwrap();
    let (year, month, day) = (field(0..4), field(4..6), field(6..8));
    // Days from civil (Howard Hinnant).
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = shifted.div_euclid(400);
    let year_of_era = shifted - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    days * 86_400 + field(9..11) * 3600 + field(11..13) * 60 + field(13..15)
}

/// Incoming `traceparent`/`tracestate` pairs: W3C edge cases plus the
/// `tracestate` limits (32 members, 512 characters, invalid members).
fn trace_context_cases() -> Vec<(&'static str, String, String)> {
    let t = |n: u32| traceparent(n, "01").1;
    let p = "b7ad6b7169203331";
    let state = || "vendor=f7".to_owned();
    let forty: Vec<String> = (0..40).map(|i| format!("k{i}=v{i}")).collect();
    vec![
        ("valid", format!("00-{}-{p}-01", t(1)), state()),
        ("unsampled", format!("00-{}-{p}-00", t(2)), state()),
        (
            "uppercase",
            format!("00-{}-{}-01", t(3).to_uppercase(), p.to_uppercase()),
            state(),
        ),
        ("version_ff", format!("ff-{}-{p}-01", t(4)), state()),
        (
            "future_version_extra",
            format!("cc-{}-{p}-01-what-the-future-holds", t(5)),
            state(),
        ),
        ("v00_extra", format!("00-{}-{p}-01-extra", t(6)), state()),
        ("zero_trace", format!("00-{}-{p}-01", "0".repeat(32)), state()),
        (
            "zero_parent",
            format!("00-{}-{}-01", t(8), "0".repeat(16)),
            state(),
        ),
        ("surrounding_spaces", format!("  00-{}-{p}-01  ", t(9)), state()),
        ("flags_09", format!("00-{}-{p}-09", t(10)), state()),
        ("flags_02_unsampled", format!("00-{}-{p}-02", t(11)), state()),
        ("future_version_exact", format!("01-{}-{p}-01", t(12)), state()),
        ("bad_flags", format!("00-{}-{p}-0g", t(13)), state()),
        ("future_version_no_dash", format!("cc-{}-{p}-01x", t(14)), state()),
        ("short_trace", format!("00-{}-{p}-01", &t(15)[1..]), state()),
        (
            "tracestate_malformed",
            format!("00-{}-{p}-01", t(16)),
            "=,,bad state".into(),
        ),
        (
            "tracestate_40_members",
            format!("00-{}-{p}-01", t(17)),
            forty.join(","),
        ),
        (
            "tracestate_over_512",
            format!("00-{}-{p}-01", t(18)),
            format!("a={}", "x".repeat(600)),
        ),
        (
            "tracestate_one_bad_member",
            format!("00-{}-{p}-01", t(19)),
            "good=1,BAD KEY=2,other=3".into(),
        ),
        (
            "tracestate_two_members",
            format!("00-{}-{p}-01", t(20)),
            "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE".into(),
        ),
    ]
}

/// Compares the decisions once both systems exported everything.
fn decision_mismatches(
    cases: &[(&'static str, String, String)],
    trino: &[OSpan],
    trino_ids: &BTreeMap<&str, String>,
    codelake: &[OSpan],
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for (label, header, _) in cases {
        let header = header.trim();
        let header_trace = header.get(3..35).unwrap_or_default();
        let trino_root = trino
            .iter()
            .find(|s| s.name == "query" && s.attr("trino.query_id") == Some(trino_ids[label].as_str()));
        let codelake_id = format!("f7acc_{label}");
        let codelake_root = codelake.iter().find(|s| {
            s.name == "codelake.query" && s.attr("codelake.query_id") == Some(codelake_id.as_str())
        });
        let expected = decide(trino_root, header_trace);
        let got = decide(codelake_root, header_trace);
        if let (Decision::Adopted(_), Some(root)) = (&got, codelake_root) {
            // Adopted means the caller's span is the parent.
            assert_eq!(root.parent.as_deref(), header.get(36..52), "{label}");
            assert_eq!(root.kind, KIND_SERVER, "{label}");
        }
        if expected != got {
            mismatches.push(format!("{label}: Trino 431 {expected:?}, CodeLake {got:?}"));
        }
    }
    mismatches
}

/// Every incoming `traceparent`/`tracestate` is sent to a real Trino 431 and
/// to `CodeLake` (through `QueryTrace::from_headers`, the function the Trino
/// protocol front will call); both export to the same collector. For each
/// case the outcome must match: dropped (caller said "not sampled"), adopted
/// into the caller's trace (with the same `tracestate`), or a new trace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_trino_trace_context_decisions_match_trino_431() {
    let jaeger = jaeger("jaeger-trino").await;
    let (_trino, trino_url) = trino(jaeger.otlp_grpc_port).await;
    let service = service_name("trino-diff");
    let telemetry = export_to(&jaeger.otlp_http, &service, 1.0, 10);
    let spill = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), None, None).await;

    let cases = trace_context_cases();
    let mut trino_ids = BTreeMap::new();
    for (label, header, state) in &cases {
        let (id, one) = trino_select_one(&trino_url, header, state).await;
        assert_eq!(one, 1, "Trino {label}");
        assert!(is_trino_query_id(&id), "Trino id {id}");
        trino_ids.insert(*label, id);

        let options = QueryOptions::for_user("acc").with_trace(
            QueryTrace::from_headers(Some(header.as_str()), Some(state.as_str()), None)
                .with_query_id(format!("f7acc_{label}")),
        );
        let rows = run(&engine, &options, "SELECT 1 AS one").await;
        assert_eq!(ints(&rows, 0), vec![1], "CodeLake {label}");
    }
    // Engine-assigned query ids have Trino's shape and Trino's clock (UTC).
    let own = engine
        .query(&QueryOptions::for_user("acc"), "SELECT 2")
        .await
        .unwrap();
    let own_id = own.query_id().to_owned();
    assert_eq!(ints(&own.collect().await.unwrap(), 0), vec![2]);
    assert!(is_trino_query_id(&own_id), "CodeLake id {own_id}");
    let trino_last = &trino_ids["tracestate_two_members"];
    assert!(
        (query_id_epoch(&own_id) - query_id_epoch(trino_last)).abs() < 300,
        "CodeLake {own_id} vs Trino {trino_last}: same UTC clock"
    );

    // A sampled sentinel last: once both show it, everything before is in.
    let (sentinel, sentinel_trace) = traceparent(99, "01");
    let (sentinel_id, _) = trino_select_one(&trino_url, &sentinel, "").await;
    run(&engine, &traced("acc", &sentinel, "f7acc_sentinel"), "SELECT 3").await;
    let telemetry = flush(telemetry).await;
    jaeger
        .wait_service("trino", |spans| {
            spans
                .iter()
                .any(|s| s.name == "query" && s.attr("trino.query_id") == Some(sentinel_id.as_str()))
        })
        .await;
    jaeger
        .wait_service(&service, |spans| spans.iter().any(|s| s.trace == sentinel_trace))
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (trino_spans, _) = jaeger.service("trino").await;
    let (codelake_spans, _) = jaeger.service(&service).await;

    // Not vacuous: Trino adopted the valid context with its tracestate.
    let valid = trino_spans
        .iter()
        .find(|s| s.name == "query" && s.attr("trino.query_id") == Some(trino_ids["valid"].as_str()));
    assert_eq!(
        decide(valid, &traceparent(1, "01").1),
        Decision::Adopted(Some("vendor=f7".into()))
    );
    let mismatches = decision_mismatches(&cases, &trino_spans, &trino_ids, &codelake_spans);
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    shutdown(telemetry).await;
}

// ---------------------------------------------------------------------------
// 3. SQL Server: volume, LIMIT, early drop, concurrency
// ---------------------------------------------------------------------------

/// 50 000 rows streamed from SQL Server, then written to Hive and read back.
async fn volume_queries(engine: &Engine, header: &str, warehouse: &Path) -> (String, u64) {
    let rows = run(
        engine,
        &traced("vol", header, "f7acc_full"),
        "SELECT id, grp FROM erp.dbo.docs",
    )
    .await;
    let ids = ints(&rows, 0);
    assert_eq!(ids.len(), 50_000);
    assert_eq!(ids.iter().sum::<i64>(), sum_to(DOCS));
    assert!(ints(&rows, 1).iter().zip(&ids).all(|(g, i)| *g == i % 7));

    run(
        engine,
        &traced("vol", header, "f7acc_schema"),
        "CREATE SCHEMA hive.acc",
    )
    .await;
    let written = run(
        engine,
        &traced("vol", header, "f7acc_ctas"),
        "CREATE TABLE hive.acc.docs AS SELECT id, grp FROM erp.dbo.docs",
    )
    .await;
    assert_eq!(ints(&written, 0), vec![50_000]);
    let files = data_files(&warehouse.join("acc/docs"));
    assert_eq!(files.len(), 1, "{files:?}");
    let back = run(
        engine,
        &traced("vol", header, "f7acc_hive_read"),
        "SELECT count(*) AS c, sum(id) AS s FROM hive.acc.docs",
    )
    .await;
    assert_eq!(
        (ints(&back, 0), ints(&back, 1)),
        (vec![50_000], vec![sum_to(DOCS)])
    );
    files.into_iter().next().unwrap()
}

/// Every layer of the 50 000-row read counts every row; the CTAS and the
/// Hive read report the file on disk.
fn assert_volume_spans(spans: &[OSpan], file: &str, size: u64) {
    let query = query_span(spans, "f7acc_full");
    assert_eq!(query.rows(), Some(50_000));
    assert!(!query.cancelled());
    let fetch = spans
        .iter()
        .find(|s| s.name == "codelake.fetch" && s.parent.as_deref() == Some(query.id.as_str()))
        .unwrap();
    assert_eq!(fetch.rows(), Some(50_000));
    let statements = below(spans, query, "sqlserver.query");
    let read: u64 = statements.iter().map(|s| s.rows().unwrap()).sum();
    assert_eq!(read, 50_000, "{statements:#?}");
    for statement in &statements {
        assert_eq!(statement.kind, KIND_CLIENT);
        assert!(!statement.cancelled(), "{statement:#?}");
        let remote = spans
            .iter()
            .find(|s| Some(&s.id) == statement.parent.as_ref())
            .unwrap();
        assert_eq!(remote.name, "codelake.remote");
        assert_eq!(remote.rows(), statement.rows());
    }
    let write = below(spans, query_span(spans, "f7acc_ctas"), "hive.write");
    assert_eq!(write.len(), 1);
    assert_eq!(write[0].rows(), Some(50_000));
    assert_eq!(write[0].attr("codelake.hive.file"), Some(file));
    let read = query_span(spans, "f7acc_hive_read");
    assert_eq!(read.rows(), Some(1));
    let scan = below(spans, read, "hive.scan");
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].attr("codelake.hive.files"), Some("1"));
    assert_eq!(
        scan[0].attr("codelake.hive.bytes"),
        Some(size.to_string().as_str())
    );
    assert_eq!(scan[0].attr("codelake.hive.file_names"), Some(file));
    assert_no_orphans(spans);
}

/// 16 concurrent queries, each in its own caller trace, half on SQL Server
/// and half on Hive; returns (case, trace id) pairs.
async fn concurrent_queries(engine: &Arc<Engine>) -> Vec<(u32, String)> {
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..16_u32 {
        let engine = Arc::clone(engine);
        tasks.spawn(async move {
            let (header, trace) = traceparent(100 + n, "01");
            let group = i64::from(n % 7);
            let low = i64::from(n) * 1_000;
            let source = if n % 2 == 0 {
                "erp.dbo.docs"
            } else {
                "hive.acc.docs"
            };
            let sql = format!(
                "SELECT count(*) AS c, sum(id) AS s FROM {source} WHERE grp = {group} AND id > {low}"
            );
            let rows = run(&engine, &traced("conc", &header, &format!("f7acc_c{n}")), &sql).await;
            let expected: Vec<i64> = (low + 1..=DOCS).filter(|i| i % 7 == group).collect();
            assert_eq!(
                ints(&rows, 0),
                vec![i64::try_from(expected.len()).unwrap()],
                "{sql}"
            );
            assert_eq!(ints(&rows, 1), vec![expected.iter().sum::<i64>()], "{sql}");
            (n, trace)
        });
    }
    let mut done = Vec::new();
    while let Some(result) = tasks.join_next().await {
        done.push(result.unwrap());
    }
    done
}

async fn assert_concurrent_traces(jaeger: &Jaeger, concurrent: Vec<(u32, String)>) {
    for (n, trace) in concurrent {
        let (mine, other) = if n % 2 == 0 {
            ("sqlserver.query", "hive.scan")
        } else {
            ("hive.scan", "sqlserver.query")
        };
        let (spans, _) = jaeger
            .wait_trace(&trace, |s| {
                !named(s, "codelake.fetch").is_empty() && !named(s, mine).is_empty()
            })
            .await;
        let query = the(&spans, "codelake.query");
        assert_eq!(
            query.attr("codelake.query_id"),
            Some(format!("f7acc_c{n}").as_str())
        );
        assert_eq!(query.rows(), Some(1));
        assert!(
            named(&spans, other).is_empty(),
            "query {n} got a {other}: {spans:#?}"
        );
        assert!(named(&spans, mine).iter().all(|s| has_ancestor(&spans, s, query)));
        assert_no_orphans(&spans);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_sqlserver_volume_limit_cancel_and_concurrency_keep_exact_counts() {
    let jaeger = jaeger("jaeger-vol").await;
    let (_mssql, port) = sql_server("mssql-vol").await;
    let telemetry = export_to(&jaeger.otlp_http, &service_name("vol"), 1.0, 10);
    let spill = tempfile::tempdir().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let engine = Arc::new(engine(spill.path(), Some(warehouse.path()), Some(port)).await);

    let (full, full_trace) = traceparent(1, "01");
    let (file, size) = volume_queries(&engine, &full, warehouse.path()).await;

    // LIMIT: the client got 10 rows; the query finished, it was not cancelled.
    let (limit, limit_trace) = traceparent(2, "01");
    let rows = run(
        &engine,
        &traced("vol", &limit, "f7acc_limit"),
        "SELECT id FROM erp.dbo.docs ORDER BY id LIMIT 10",
    )
    .await;
    assert_eq!(ints(&rows, 0), (1..=10).collect::<Vec<_>>());

    // The client reads one batch and goes away.
    let (early, early_trace) = traceparent(3, "01");
    let mut stream = engine
        .query(
            &traced("vol", &early, "f7acc_early"),
            "SELECT id FROM erp.dbo.docs",
        )
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let first = stream.next().await.unwrap().unwrap();
    let seen = u64::try_from(first.num_rows()).unwrap();
    assert!(seen > 0 && seen < 50_000, "{seen}");
    assert!(ints(&[first], 0).iter().all(|id| (1..=DOCS).contains(id)));
    drop(stream);

    let concurrent = concurrent_queries(&engine).await;
    let telemetry = flush(telemetry).await;

    let (spans, _) = jaeger
        .wait_trace(&full_trace, |s| {
            named(s, "codelake.query").len() == 4
                && !named(s, "hive.write").is_empty()
                && !named(s, "hive.scan").is_empty()
        })
        .await;
    assert_volume_spans(&spans, &file, size);

    let (spans, _) = jaeger
        .wait_trace(&limit_trace, |s| !named(s, "codelake.fetch").is_empty())
        .await;
    let query = the(&spans, "codelake.query");
    assert_eq!(query.rows(), Some(10));
    assert_ne!(query.status, STATUS_ERROR);
    assert!(
        !query.cancelled(),
        "a satisfied LIMIT is not a cancellation: {query:#?}"
    );
    assert_eq!(the(&spans, "codelake.fetch").rows(), Some(10));

    let (spans, _) = jaeger
        .wait_trace(&early_trace, |s| {
            !named(s, "sqlserver.query").is_empty() && !named(s, "codelake.fetch").is_empty()
        })
        .await;
    let query = the(&spans, "codelake.query");
    assert!(query.cancelled(), "{query:#?}");
    assert_eq!(query.rows(), Some(seen));
    let statement = the(&spans, "sqlserver.query");
    assert!(statement.cancelled(), "{statement:#?}");
    assert!(statement.rows().unwrap() <= 50_000);

    assert_concurrent_traces(&jaeger, concurrent).await;
    shutdown(telemetry).await;
}

// ---------------------------------------------------------------------------
// 4. SQL Server fails mid-stream: status, error number, no data value
// ---------------------------------------------------------------------------

/// `CAST(Doc AS INT)` in a SQL Server view fails at row 30 000, after
/// ~30 000 rows were streamed. The failure must be on the spans (status,
/// SQL Server error 245) and the pool must recover; the value SQL Server
/// quotes in its message (a customer value) must not be exported anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_sqlserver_failure_mid_stream_is_recorded_without_the_value() {
    let jaeger = jaeger("jaeger-err").await;
    let (_mssql, port) = sql_server("mssql-err").await;
    let telemetry = export_to(&jaeger.otlp_http, &service_name("err"), 1.0, 10);
    let spill = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), None, Some(port)).await;

    let (header, trace) = traceparent(1, "01");
    let failed = engine
        .query(
            &traced("err", &header, "f7acc_fail"),
            "SELECT id, n FROM erp.dbo.docnumbers",
        )
        .await
        .unwrap()
        .collect()
        .await;
    let err = failed.expect_err("conversion error at row 30000");
    assert!(err.to_string().contains("245"), "{err}");

    // The pool recovers: the next statement returns correct values.
    let (ok, _) = traceparent(2, "01");
    let rows = run(
        &engine,
        &traced("err", &ok, "f7acc_after"),
        "SELECT count(*) AS c, sum(id) AS s FROM erp.dbo.docs WHERE id <= 100",
    )
    .await;
    assert_eq!((ints(&rows, 0), ints(&rows, 1)), (vec![100], vec![5_050]));
    let telemetry = flush(telemetry).await;

    let (spans, raw) = jaeger
        .wait_trace(&trace, |s| {
            named(s, "sqlserver.query")
                .iter()
                .any(|q| q.status == STATUS_ERROR)
                && named(s, "codelake.query")
                    .iter()
                    .any(|q| q.status == STATUS_ERROR)
        })
        .await;
    let statement = the(&spans, "sqlserver.query");
    assert_eq!(statement.attr("db.response.status_code"), Some("245"));
    assert_eq!(the(&spans, "codelake.remote").status, STATUS_ERROR);
    assert_eq!(the(&spans, "codelake.fetch").status, STATUS_ERROR);
    let leaks: Vec<String> = spans
        .iter()
        .filter(|s| s.carries(CONFIDENTIAL))
        .map(|s| format!("{}: status `{}`", s.name, s.message))
        .collect();
    assert!(leaks.is_empty(), "data value exported:\n{}", leaks.join("\n"));
    assert!(
        !raw.contains(CONFIDENTIAL),
        "data value somewhere in the exported trace"
    );
    shutdown(telemetry).await;
}

// ---------------------------------------------------------------------------
// 5. Engine errors: failed is not cancelled; literals stay out of errors
// ---------------------------------------------------------------------------

/// A division by zero at row 30 000 of 50 000: the query failed. Its spans
/// must say ERROR, and must not also claim the client cancelled it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f7_failed_query_is_an_error_not_a_cancellation() {
    let jaeger = jaeger("jaeger-fail").await;
    let telemetry = export_to(&jaeger.otlp_http, &service_name("fail"), 1.0, 10);
    let spill = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), None, None).await;

    let (header, trace) = traceparent(1, "01");
    let err = engine
        .query(
            &traced("fail", &header, "f7acc_div0"),
            "SELECT 10 / (value - 30000) AS x FROM generate_series(1, 50000)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("division by zero");
    assert!(err.to_string().to_lowercase().contains("divide by zero"), "{err}");
    // Control: the same shape without the zero returns every row.
    let (ok, _) = traceparent(2, "01");
    let rows = run(
        &engine,
        &traced("fail", &ok, "f7acc_ok"),
        "SELECT 10 / (value - 60000) AS x FROM generate_series(1, 50000)",
    )
    .await;
    assert_eq!(row_count(&rows), 50_000);
    let telemetry = flush(telemetry).await;

    let (spans, _) = jaeger
        .wait_trace(&trace, |s| {
            named(s, "codelake.query")
                .iter()
                .any(|q| q.status == STATUS_ERROR)
        })
        .await;
    for name in ["codelake.query", "codelake.fetch"] {
        let span = the(&spans, name);
        assert_eq!(span.status, STATUS_ERROR, "{span:#?}");
        assert!(
            !span.cancelled(),
            "{name} failed, it was not cancelled by the client: {span:#?}"
        );
    }
    shutdown(telemetry).await;
}

/// ADR 0010: traces never carry the values a query filtered on. A numeric
/// literal is as sensitive as a string one (document numbers, LSNs).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn f7_literals_of_a_failed_query_are_not_exported() {
    let jaeger = jaeger("jaeger-lit").await;
    let telemetry = export_to(&jaeger.otlp_http, &service_name("lit"), 1.0, 10);
    let spill = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), None, None).await;

    let (header, trace) = traceparent(1, "01");
    let cases = [
        (
            "f7acc_str",
            "SELECT value FROM generate_series(1, 3) WHERE CAST(value AS VARCHAR) = CAST(CAST('CONFIDENTIAL-STR' AS INT) AS VARCHAR)",
            "CONFIDENTIAL-STR",
        ),
        (
            "f7acc_num",
            "SELECT value FROM generate_series(1, 3) WHERE value = CAST(98765432101 AS INT)",
            "98765432101",
        ),
    ];
    for (id, sql, _) in cases {
        let failed = match engine.query(&traced("lit", &header, id), sql).await {
            Ok(query) => query.collect().await.map(|_| ()),
            Err(err) => Err(err),
        };
        assert!(failed.is_err(), "{sql} must fail");
    }
    let telemetry = flush(telemetry).await;
    let (spans, _) = jaeger
        .wait_trace(&trace, |s| {
            named(s, "codelake.query")
                .iter()
                .filter(|q| q.status == STATUS_ERROR)
                .count()
                == 2
        })
        .await;
    let mut leaks = Vec::new();
    for (id, _, literal) in cases {
        let query = query_span(&spans, id);
        for span in spans
            .iter()
            .filter(|s| s.id == query.id || has_ancestor(&spans, s, query))
        {
            if span.carries(literal) {
                leaks.push(format!("{}: `{literal}` in status `{}`", span.name, span.message));
            }
        }
    }
    assert!(leaks.is_empty(), "{}", leaks.join("\n"));
    shutdown(telemetry).await;
}

// ---------------------------------------------------------------------------
// 6. Hive: files and bytes as on disk, many files, zero-row insert
// ---------------------------------------------------------------------------

/// 60 000 rows in one CTAS, 55 one-row writes, one zero-row INSERT, reads.
async fn hive_queries(engine: &Engine, header: &str) {
    let t = |id: &str| traced("hive", header, id);
    run(engine, &t("f7acc_schema"), "CREATE SCHEMA hive.acc").await;
    let written = run(
        engine,
        &t("f7acc_big"),
        "CREATE TABLE hive.acc.big AS SELECT value AS id FROM generate_series(1, 60000)",
    )
    .await;
    assert_eq!(ints(&written, 0), vec![60_000]);
    run(
        engine,
        &t("f7acc_ins1"),
        "CREATE TABLE hive.acc.small AS SELECT CAST(1 AS BIGINT) AS id",
    )
    .await;
    for i in 2..=55 {
        let rows = run(
            engine,
            &t(&format!("f7acc_ins{i}")),
            &format!("INSERT INTO hive.acc.small VALUES ({i})"),
        )
        .await;
        assert_eq!(ints(&rows, 0), vec![1]);
    }
    let empty = run(
        engine,
        &t("f7acc_empty"),
        "INSERT INTO hive.acc.small SELECT id FROM hive.acc.small WHERE id < 0",
    )
    .await;
    assert_eq!(ints(&empty, 0), vec![0]);
    let small = run(
        engine,
        &t("f7acc_read_small"),
        "SELECT count(*) AS c, sum(id) AS s FROM hive.acc.small",
    )
    .await;
    assert_eq!((ints(&small, 0), ints(&small, 1)), (vec![55], vec![sum_to(55)]));
    let big = run(
        engine,
        &t("f7acc_read_big"),
        "SELECT count(*) AS c, sum(id) AS s FROM hive.acc.big",
    )
    .await;
    assert_eq!(
        (ints(&big, 0), ints(&big, 1)),
        (vec![60_000], vec![sum_to(60_000)])
    );
}

fn assert_hive_writes(spans: &[OSpan], small: &BTreeMap<String, u64>, big: &BTreeMap<String, u64>) {
    let big_write = below(spans, query_span(spans, "f7acc_big"), "hive.write");
    assert_eq!(big_write.len(), 1);
    assert_eq!(big_write[0].rows(), Some(60_000));
    assert_eq!(
        big_write[0].attr("codelake.hive.file"),
        big.keys().next().map(String::as_str)
    );

    let empty_write = below(spans, query_span(spans, "f7acc_empty"), "hive.write");
    assert_eq!(empty_write.len(), 1);
    assert_eq!(empty_write[0].rows(), Some(0));
    assert!(
        !small.contains_key(empty_write[0].attr("codelake.hive.file").unwrap()),
        "a zero-row insert names no real file"
    );

    // Every write names the file it created; together they are the directory.
    let mut files = BTreeSet::new();
    for i in 1..=55 {
        let id = format!("f7acc_ins{i}");
        let write = below(spans, query_span(spans, &id), "hive.write");
        assert_eq!(write.len(), 1, "{id}");
        assert_eq!(write[0].rows(), Some(1), "{id}");
        files.insert(write[0].attr("codelake.hive.file").unwrap().to_owned());
    }
    assert_eq!(files, small.keys().cloned().collect::<BTreeSet<_>>());
}

fn assert_hive_scans(spans: &[OSpan], small: &BTreeMap<String, u64>, big: &BTreeMap<String, u64>) {
    let scan = below(spans, query_span(spans, "f7acc_read_small"), "hive.scan");
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].attr("codelake.hive.files"), Some("55"));
    let total: u64 = small.values().sum();
    assert_eq!(
        scan[0].attr("codelake.hive.bytes"),
        Some(total.to_string().as_str())
    );
    let listed: Vec<&str> = scan[0]
        .attr("codelake.hive.file_names")
        .unwrap()
        .split(',')
        .collect();
    assert_eq!(listed.len(), 51, "50 names and a truncation marker: {listed:?}");
    assert_eq!(listed[50], "…");
    assert!(listed[..50].iter().all(|n| small.contains_key(*n)), "{listed:?}");

    let scan = below(spans, query_span(spans, "f7acc_read_big"), "hive.scan");
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].attr("codelake.hive.files"), Some("1"));
    let size: u64 = big.values().sum();
    assert_eq!(
        scan[0].attr("codelake.hive.bytes"),
        Some(size.to_string().as_str())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_hive_scan_and_write_report_what_is_on_disk() {
    let jaeger = jaeger("jaeger-hive").await;
    let telemetry = export_to(&jaeger.otlp_http, &service_name("hive"), 1.0, 10);
    let spill = tempfile::tempdir().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), Some(warehouse.path()), None).await;

    let (header, trace) = traceparent(1, "01");
    hive_queries(&engine, &header).await;
    let small = data_files(&warehouse.path().join("acc/small"));
    assert_eq!(small.len(), 55, "one file per write, none for zero rows");
    let big = data_files(&warehouse.path().join("acc/big"));
    assert_eq!(big.len(), 1);
    let telemetry = flush(telemetry).await;

    // schema + big + 55 writes + empty + 2 reads
    let (spans, raw) = jaeger
        .wait_trace(&trace, |s| {
            named(s, "codelake.query").len() == 60 && named(s, "hive.scan").len() >= 3
        })
        .await;
    assert_no_orphans(&spans);
    assert_hive_writes(&spans, &small, &big);
    assert_hive_scans(&spans, &small, &big);
    assert!(
        raw.contains("INSERT INTO hive.acc.small VALUES (?)"),
        "redacted text is exported"
    );
    assert!(!raw.contains("VALUES (37)"), "literals are redacted");
    shutdown(telemetry).await;
}

// ---------------------------------------------------------------------------
// 7. A stalled collector never blocks queries or shutdown
// ---------------------------------------------------------------------------

/// The collector accepts TCP connections and never answers. Queries keep
/// their values and latency; flush and shutdown end within a bound derived
/// from `export_timeout_secs` (a hung shutdown would hang SIGTERM).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_stalled_collector_never_blocks_queries_or_shutdown() {
    let stalled = StdListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", stalled.local_addr().unwrap());
    let held = Arc::new(std::sync::Mutex::new(Vec::new()));
    let keep = Arc::clone(&held);
    std::thread::spawn(move || {
        for connection in stalled.incoming() {
            // Hold every connection open, answer nothing.
            keep.lock().unwrap().push(connection);
        }
    });
    let telemetry = export_to(&endpoint, &service_name("stalled"), 1.0, 1);
    let spill = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), None, None).await;

    let started = Instant::now();
    for n in 0..300_u32 {
        let (header, _) = traceparent(n, "01");
        let rows = run(
            &engine,
            &traced("stall", &header, &format!("f7acc_s{n}")),
            &format!("SELECT {n} * 2 AS v, count(*) AS c FROM generate_series(1, 1000)"),
        )
        .await;
        assert_eq!(
            (ints(&rows, 0), ints(&rows, 1)),
            (vec![i64::from(n) * 2], vec![1_000])
        );
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(60),
        "300 queries took {elapsed:?} with a stalled collector"
    );
    let flushing = Instant::now();
    let telemetry = flush(telemetry).await;
    let flushed = flushing.elapsed();
    assert!(flushed < Duration::from_secs(30), "flush took {flushed:?}");
    let stopping = Instant::now();
    shutdown(telemetry).await;
    let stopped = stopping.elapsed();
    assert!(stopped < Duration::from_secs(30), "shutdown took {stopped:?}");
    assert!(
        !held.lock().unwrap().is_empty(),
        "the exporter did try the collector"
    );
}

// ---------------------------------------------------------------------------
// 8. PostgreSQL wire: one trace per session, never parameter values
// ---------------------------------------------------------------------------

fn server_config(dir: &Path, jaeger: &Jaeger, service: &str, ratio: f64) -> (PathBuf, u16) {
    let (pg, http) = (super::free_port(), super::free_port());
    let config = super::write_config(dir, pg, http);
    let mut text = std::fs::read_to_string(&config).unwrap();
    write!(
        text,
        "\n[telemetry]\notlp_endpoint = \"{}\"\nservice_name = \"{service}\"\nsampling_ratio = {ratio:.1}\n",
        jaeger.otlp_http
    )
    .unwrap();
    let path = dir.join(format!("{service}.toml"));
    std::fs::write(&path, text).unwrap();
    (path, pg)
}

async fn start_server(config: &Path, pg: u16) -> super::Server {
    let server = super::Server(
        Command::new(super::BIN)
            .args(["serve", "--config"])
            .arg(config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::net::TcpStream::connect(("127.0.0.1", pg)).is_err() {
        assert!(Instant::now() < deadline, "server did not listen");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server
}

/// SIGTERM: the server drains, exits and flushes the exporter.
async fn stop(server: &mut super::Server) {
    let status = Command::new("kill")
        .args(["-TERM", &server.0.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(20);
    while server.0.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "server did not exit");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn connect(pg: u16) -> tokio_postgres::Client {
    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

async fn simple_value(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap()
}

/// Two sessions interleave their statements; each session is one trace
/// holding only its own statements. A bound parameter never reaches a span.
fn assert_sessions(statements: &[&OSpan]) {
    let mut by_trace: BTreeMap<&str, Vec<&OSpan>> = BTreeMap::new();
    for statement in statements {
        by_trace
            .entry(statement.trace.as_str())
            .or_default()
            .push(statement);
    }
    assert_eq!(by_trace.len(), 2, "one trace per session: {statements:#?}");
    let mut sessions = BTreeSet::new();
    let mut texts = Vec::new();
    for group in by_trace.values() {
        let ids: BTreeSet<&str> = group
            .iter()
            .filter_map(|s| s.attr("codelake.session_id"))
            .collect();
        assert_eq!(ids.len(), 1, "{group:#?}");
        sessions.extend(ids);
        let simple: BTreeSet<&str> = group
            .iter()
            .filter(|s| s.attr("codelake.protocol") == Some("postgresql.simple"))
            .filter_map(|s| s.attr("db.query.text"))
            .collect();
        texts.push(simple);
    }
    assert_eq!(sessions.len(), 2, "distinct sessions");
    texts.sort();
    assert_eq!(
        texts,
        vec![
            BTreeSet::from(["SELECT ? * ? AS b_round"]),
            BTreeSet::from(["SELECT ? + ? AS a_round"]),
        ],
        "statements stay in their own session's trace"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f7_pg_wire_sessions_are_separate_traces_and_sampling_zero_exports_nothing() {
    let jaeger = jaeger("jaeger-pg").await;
    let dir = tempfile::tempdir().unwrap();

    let service = service_name("pg");
    let (config, pg) = server_config(dir.path(), &jaeger, &service, 1.0);
    let mut server = start_server(&config, pg).await;
    let (a, b) = (connect(pg).await, connect(pg).await);
    for round in 1..=3_i64 {
        let a_value = simple_value(&a, &format!("SELECT {round} + 1000 AS a_round")).await;
        assert_eq!(a_value, (round + 1000).to_string());
        let b_value = simple_value(&b, &format!("SELECT {round} * 7 AS b_round")).await;
        assert_eq!(b_value, (round * 7).to_string());
    }
    // Extended protocol with a bound parameter: the value is not SQL text.
    let rows = a
        .query("SELECT $1::TEXT AS p", &[&"param-confidential-7"])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "param-confidential-7");
    drop((a, b));
    stop(&mut server).await;

    let (spans, raw) = jaeger
        .wait_service(&service, |s| named(s, "codelake.pg.statement").len() >= 7)
        .await;
    let statements = named(&spans, "codelake.pg.statement");
    assert_eq!(statements.len(), 7, "{statements:#?}");
    assert_sessions(&statements);
    let extended = statements
        .iter()
        .filter(|s| s.attr("codelake.protocol") == Some("postgresql.extended"))
        .count();
    assert_eq!(extended, 1, "{statements:#?}");
    assert!(!raw.contains("param-confidential-7"), "parameter value exported");

    // sampling_ratio = 0: nothing is exported for any session.
    let quiet = service_name("pg0");
    let (config, pg) = server_config(dir.path(), &jaeger, &quiet, 0.0);
    let mut server = start_server(&config, pg).await;
    let client = connect(pg).await;
    assert_eq!(simple_value(&client, "SELECT 6 * 7 AS v").await, "42");
    drop(client);
    stop(&mut server).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (quiet_spans, _) = jaeger.service(&quiet).await;
    assert!(quiet_spans.is_empty(), "ratio 0 exported: {quiet_spans:#?}");
}
