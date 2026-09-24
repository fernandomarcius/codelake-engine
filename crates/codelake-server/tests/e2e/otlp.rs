//! OTLP traces end to end, against a real collector (Jaeger v2, OTLP/HTTP in,
//! query API out): what arrives there is what an operator would see.
//!
//! 1. Engine in-process with the server's telemetry pipeline: SQL Server and
//!    Hive sources, queries carrying a W3C `traceparent`. Checks the trace id
//!    is the caller's, the parent/child tree, source attributes and that no
//!    literal from the SQL reaches the collector. A query without
//!    `traceparent` under `sampling_ratio = 0` must not be exported.
//! 2. The `codelake` binary with `[telemetry]`: PostgreSQL statements of one
//!    session land in one trace.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::config::{ExecutionConfig, MemoryConfig, TelemetryConfig};
use codelake_core::{ByteSize, Engine, QueryOptions, QueryTrace};
use codelake_server::telemetry;
use datafusion::arrow::array::{Array, Decimal128Array, Int32Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use secrecy::SecretString;
use serde_json::Value;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const JAEGER: (&str, &str) = ("jaegertracing/jaeger", "2.21.0");
const SA_PASSWORD: &str = "Otlp-Test-Only-1";
/// The caller's span, as a client would send it (sampled).
const INCOMING: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const INCOMING_TRACE: &str = "0af7651916cd43dd8448eb211c80319c";
const INCOMING_PARENT: &str = "b7ad6b7169203331";
/// A literal of the pushed-down filter: must never reach the collector.
const SECRET_LITERAL: &str = "424242";

fn host() -> String {
    std::env::var("TESTCONTAINERS_HOST_OVERRIDE").unwrap_or_else(|_| "127.0.0.1".to_owned())
}

struct Jaeger {
    _container: ContainerAsync<GenericImage>,
    otlp: String,
    api: String,
}

async fn jaeger(name: &str) -> Jaeger {
    let container = GenericImage::new(JAEGER.0, JAEGER.1)
        .with_exposed_port(4318.tcp())
        .with_exposed_port(16686.tcp())
        .with_wait_for(WaitFor::message_on_either_std("Everything is ready"))
        .with_container_name(format!("codelake-f7-{name}-{}", std::process::id()))
        .start()
        .await
        .expect("jaeger starts");
    let host = host();
    let otlp = container.get_host_port_ipv4(4318).await.unwrap();
    let api = container.get_host_port_ipv4(16686).await.unwrap();
    Jaeger {
        _container: container,
        otlp: format!("http://{host}:{otlp}"),
        api: format!("http://{host}:{api}"),
    }
}

impl Jaeger {
    async fn get(&self, path: &str) -> Value {
        let body = reqwest::get(format!("{}{path}", self.api))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{path}: {e}: {body}"))
    }

    /// Spans of one trace once `ready` holds for them (ingestion is async).
    async fn trace(&self, trace_id: &str, ready: impl Fn(&[Span]) -> bool) -> Vec<Span> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let json = self.get(&format!("/api/traces/{trace_id}")).await;
            let spans = json["data"]
                .as_array()
                .and_then(|traces| traces.first())
                .map(Span::all)
                .unwrap_or_default();
            if ready(&spans) {
                return spans;
            }
            assert!(
                Instant::now() < deadline,
                "trace {trace_id} incomplete: {spans:#?}"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Every trace id stored for `service` (API v3: the v1 search endpoint
    /// is not served by Jaeger v2).
    async fn trace_ids(&self, service: &str) -> BTreeSet<String> {
        let json = self
            .get(&format!(
                "/api/v3/traces?query.service_name={service}\
                 &query.start_time_min=2000-01-01T00:00:00Z&query.start_time_max=2100-01-01T00:00:00Z"
            ))
            .await;
        let mut ids = BTreeSet::new();
        for resource in json["result"]["resourceSpans"].as_array().into_iter().flatten() {
            for scope in resource["scopeSpans"].as_array().into_iter().flatten() {
                for span in scope["spans"].as_array().into_iter().flatten() {
                    if let Some(id) = span["traceId"].as_str() {
                        ids.insert(id.to_owned());
                    }
                }
            }
        }
        ids
    }
}

/// One span as the Jaeger query API returns it.
#[derive(Debug, Clone)]
struct Span {
    trace: String,
    id: String,
    parent: Option<String>,
    name: String,
    service: String,
    tags: BTreeMap<String, String>,
}

impl Span {
    fn all(trace: &Value) -> Vec<Self> {
        let processes = &trace["processes"];
        trace["spans"]
            .as_array()
            .unwrap()
            .iter()
            .map(|span| {
                let tags = span["tags"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|tag| {
                        let value = match &tag["value"] {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        (tag["key"].as_str().unwrap().to_owned(), value)
                    })
                    .collect();
                let parent = span["references"].as_array().and_then(|refs| {
                    refs.iter()
                        .find(|r| r["refType"] == "CHILD_OF")
                        .and_then(|r| r["spanID"].as_str().map(str::to_owned))
                });
                let process = span["processID"].as_str().unwrap();
                Self {
                    trace: span["traceID"].as_str().unwrap().to_owned(),
                    id: span["spanID"].as_str().unwrap().to_owned(),
                    parent,
                    name: span["operationName"].as_str().unwrap().to_owned(),
                    service: processes[process]["serviceName"].as_str().unwrap().to_owned(),
                    tags,
                }
            })
            .collect()
    }

    fn tag(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(String::as_str)
    }
}

fn named<'a>(spans: &'a [Span], name: &str) -> Vec<&'a Span> {
    spans.iter().filter(|s| s.name == name).collect()
}

fn by_id<'a>(spans: &'a [Span], id: &str) -> &'a Span {
    spans
        .iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("no span {id}"))
}

/// Names of `span`'s ancestors, nearest first, up to the first remote parent.
fn ancestors<'a>(spans: &'a [Span], span: &Span) -> Vec<&'a Span> {
    let mut chain: Vec<&Span> = Vec::new();
    let mut parent = span.parent.as_deref();
    while let Some(id) = parent {
        let Some(found) = spans.iter().find(|s| s.id == id) else {
            break;
        };
        chain.push(found);
        parent = found.parent.as_deref();
    }
    chain
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

async fn engine(spill: &std::path::Path, warehouse: &std::path::Path, mssql_port: u16) -> Engine {
    let memory = MemoryConfig {
        limit: ByteSize::mib(256),
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    let timeout = Duration::from_secs(60);
    let sqlserver = SqlServerConnector::new(
        "erp".into(),
        SqlServerParams {
            host: host(),
            port: mssql_port,
            database: "otlp".into(),
            user: "sa".into(),
            password: SecretString::from(SA_PASSWORD),
            encrypt: "required".into(),
            trust_server_certificate: true,
            pool_size: 2,
        },
    );
    engine.attach(Arc::new(sqlserver), true, timeout).await.unwrap();
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
    engine
}

fn traced(user: &str) -> QueryOptions {
    QueryOptions::for_user(user).with_trace(QueryTrace::from_headers(
        Some(INCOMING),
        Some("vendor=f7"),
        Some("dbt-run-7"),
    ))
}

fn int_column(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let array = b.column(column);
            if let Some(ints) = array.as_any().downcast_ref::<Int32Array>() {
                ints.iter().map(|v| i64::from(v.unwrap())).collect::<Vec<_>>()
            } else {
                let ints = array.as_any().downcast_ref::<Int64Array>().unwrap();
                ints.iter().map(|v| v.unwrap()).collect()
            }
        })
        .collect()
}

/// SQL Server 2022 with `otlp.dbo.Contratos`: ids 1, 2 and [`SECRET_LITERAL`].
async fn sql_server() -> (ContainerAsync<MssqlServer>, u16) {
    let mssql = MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag("2022-latest")
        .with_container_name(format!("codelake-f7-mssql-{}", std::process::id()))
        .start()
        .await
        .unwrap();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    tsql(port, "master", "CREATE DATABASE otlp").await;
    tsql(
        port,
        "otlp",
        "CREATE TABLE dbo.Contratos (Id INT PRIMARY KEY, Valor DECIMAL(18,2) NOT NULL);
         INSERT INTO dbo.Contratos VALUES (1, 10.50), (2, 20.00), (424242, 99.99);",
    )
    .await;
    (mssql, port)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spans_reach_the_collector_as_one_tree_under_the_callers_span() {
    let jaeger = jaeger("jaeger").await;
    let (_mssql, mssql_port) = sql_server().await;

    let service = format!("codelake-f7-engine-{}", std::process::id());
    let config = TelemetryConfig {
        otlp_endpoint: Some(jaeger.otlp.clone()),
        service_name: service.clone(),
        // New traces are never sampled: only the caller's decision exports.
        sampling_ratio: 0.0,
        ..TelemetryConfig::default()
    };
    let telemetry = telemetry::init(Some(&config)).unwrap();
    assert!(telemetry.is_exporting());

    let spill = tempfile::tempdir().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let engine = engine(spill.path(), warehouse.path(), mssql_port).await;

    // Not sampled: no traceparent and ratio 0.
    let untraced = engine
        .query(
            &QueryOptions::for_user("nobody"),
            "SELECT count(*) FROM erp.dbo.contratos",
        )
        .await
        .unwrap();
    assert_eq!(untraced.trace_id(), None, "unsampled spans have no valid context");
    assert_eq!(int_column(&untraced.collect().await.unwrap(), 0), vec![3]);

    // Sampled by the caller: schema, CTAS reading SQL Server, then a Hive read.
    engine
        .query(&traced("dbt"), "CREATE SCHEMA hive.pilot")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let ctas = engine
        .query(
            &traced("dbt"),
            &format!(
                "CREATE TABLE hive.pilot.contratos AS SELECT id, valor FROM erp.dbo.contratos WHERE id <> {SECRET_LITERAL}"
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        ctas.trace_id().as_deref(),
        Some(INCOMING_TRACE),
        "trace id is the caller's"
    );
    let ctas_id = ctas.query_id().to_owned();
    let written = ctas.collect().await.unwrap();
    let rows = written[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(rows.value(0), 2);

    let read = engine
        .query(
            &traced("analyst"),
            "SELECT id, valor FROM hive.pilot.contratos ORDER BY id",
        )
        .await
        .unwrap();
    let select_id = read.query_id().to_owned();
    let batches = read.collect().await.unwrap();
    assert_eq!(int_column(&batches, 0), vec![1, 2]);
    let valor = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!((valor.value(0), valor.value(1)), (1050, 2000));

    let flushed = tokio::task::spawn_blocking(move || {
        telemetry.flush();
        telemetry
    })
    .await
    .unwrap();

    let spans = jaeger
        .trace(INCOMING_TRACE, |spans| {
            named(spans, "codelake.query").len() == 3
                && named(spans, "codelake.fetch").len() == 3
                && !named(spans, "sqlserver.query").is_empty()
                && !named(spans, "hive.scan").is_empty()
        })
        .await;

    assert_tree(&spans, &service);
    assert_ctas(&spans, &ctas_id, warehouse.path());
    assert_select(&spans, &select_id);

    // No literal of any statement reached the collector.
    for span in &spans {
        for (key, value) in &span.tags {
            assert!(!value.contains(SECRET_LITERAL), "{} {key}={value}", span.name);
        }
    }

    // The unsampled query was dropped by the sampler.
    assert_eq!(
        jaeger.trace_ids(&service).await,
        BTreeSet::from([INCOMING_TRACE.to_owned()])
    );
    tokio::task::spawn_blocking(move || flushed.shutdown())
        .await
        .unwrap();
}

/// Everything is in the caller's trace: queries under the caller's span,
/// phases under their query.
fn assert_tree(spans: &[Span], service: &str) {
    assert!(
        spans
            .iter()
            .all(|s| s.trace == INCOMING_TRACE && s.service == service)
    );
    let queries = named(spans, "codelake.query");
    for query in &queries {
        assert_eq!(query.parent.as_deref(), Some(INCOMING_PARENT), "{query:#?}");
        assert_eq!(query.tag("span.kind"), Some("server"));
        assert_eq!(query.tag("codelake.correlation_id"), Some("dbt-run-7"));
        assert_eq!(query.tag("codelake.trace_id"), Some(INCOMING_TRACE));
    }
    for phase in [
        "codelake.parse",
        "codelake.plan",
        "codelake.execute",
        "codelake.fetch",
    ] {
        for span in named(spans, phase) {
            let parent = by_id(spans, span.parent.as_deref().unwrap());
            assert_eq!(parent.name, "codelake.query", "{phase}: {span:#?}");
        }
    }
}

fn query<'a>(spans: &'a [Span], query_id: &str) -> &'a Span {
    named(spans, "codelake.query")
        .into_iter()
        .find(|q| q.tag("codelake.query_id") == Some(query_id))
        .unwrap_or_else(|| panic!("no query {query_id}"))
}

fn below<'a>(spans: &'a [Span], name: &str, ancestor: &Span) -> &'a Span {
    named(spans, name)
        .into_iter()
        .find(|s| ancestors(spans, s).iter().any(|a| a.id == ancestor.id))
        .unwrap_or_else(|| panic!("no {name} under {}", ancestor.name))
}

/// CTAS: the SQL Server statement below the remote node, below the query;
/// the Hive file it wrote.
fn assert_ctas(spans: &[Span], ctas_id: &str, warehouse: &std::path::Path) {
    let ctas_span = query(spans, ctas_id);
    assert_eq!(ctas_span.tag("enduser.id"), Some("dbt"));
    let statement = below(spans, "sqlserver.query", ctas_span);
    let chain: Vec<&str> = ancestors(spans, statement)
        .iter()
        .map(|s| s.name.as_str())
        .collect();
    assert_eq!(chain.first(), Some(&"codelake.remote"), "{chain:?}");
    assert!(chain.contains(&"codelake.execute"), "{chain:?}");
    assert_eq!(statement.tag("span.kind"), Some("client"));
    assert_eq!(statement.tag("db.system.name"), Some("microsoft.sql_server"));
    assert_eq!(statement.tag("db.namespace"), Some("otlp"));
    assert_eq!(statement.tag("db.response.returned_rows"), Some("2"));
    let text = statement.tag("db.query.text").unwrap();
    assert!(
        text.contains("[dbo].[Contratos]") && text.contains("<> ?"),
        "{text}"
    );
    assert_eq!(statement.tag("codelake.sql.hash").map(str::len), Some(16));
    let remote = by_id(spans, statement.parent.as_deref().unwrap());
    assert_eq!(remote.tag("codelake.remote.executor"), Some("sqlserver"));
    assert_eq!(remote.tag("db.response.returned_rows"), Some("2"));
    let write = below(spans, "hive.write", ctas_span);
    assert_eq!(write.tag("db.response.returned_rows"), Some("2"));
    assert_eq!(write.tag("codelake.hive.table"), Some("contratos"));
    let file = write.tag("codelake.hive.file").unwrap();
    let on_disk: Vec<String> = std::fs::read_dir(warehouse.join("pilot/contratos"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(on_disk.iter().any(|f| f == file), "{file} not in {on_disk:?}");
}

/// SELECT: the scan lists the one file the CTAS wrote; fetch counts the rows.
fn assert_select(spans: &[Span], select_id: &str) {
    let select_span = query(spans, select_id);
    assert_eq!(select_span.tag("db.response.returned_rows"), Some("2"));
    let fetch = below(spans, "codelake.fetch", select_span);
    assert_eq!(fetch.tag("db.response.returned_rows"), Some("2"));
    let scan = below(spans, "hive.scan", select_span);
    assert_eq!(scan.tag("codelake.hive.files"), Some("1"));
    let names = scan.tag("codelake.hive.file_names").unwrap();
    assert!(!names.contains(',') && names.starts_with("20"), "{names}");
    assert_eq!(
        by_id(spans, scan.parent.as_deref().unwrap()).name,
        "codelake.execute"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_wire_statements_of_one_session_share_a_trace() {
    let jaeger = jaeger("jaeger-pg").await;
    let dir = tempfile::tempdir().unwrap();
    let (pg, http) = (super::free_port(), super::free_port());
    let config = super::write_config(dir.path(), pg, http);
    let service = format!("codelake-f7-server-{}", std::process::id());
    let mut text = std::fs::read_to_string(&config).unwrap();
    write!(
        text,
        "\n[telemetry]\notlp_endpoint = \"{}\"\nservice_name = \"{service}\"\nsampling_ratio = 1.0\n",
        jaeger.otlp
    )
    .unwrap();
    std::fs::write(&config, text).unwrap();
    let mut server = super::Server(
        Command::new(super::BIN)
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    super::wait_http(&format!("http://127.0.0.1:{http}/livez")).await;

    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    for sql in ["SELECT 40 + 2 AS answer", "SELECT 'hidden-value' AS v"] {
        client.simple_query(sql).await.unwrap();
    }
    drop(client);

    // SIGTERM: the server drains, exits and flushes the exporter.
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

    let deadline = Instant::now() + Duration::from_secs(30);
    let traces = loop {
        let traces = jaeger.trace_ids(&service).await;
        if !traces.is_empty() || Instant::now() > deadline {
            break traces;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    assert_eq!(traces.len(), 1, "one session, one trace: {traces:?}");
    let trace_id = traces.into_iter().next().unwrap();
    let spans = jaeger
        .trace(&trace_id, |spans| {
            named(spans, "codelake.pg.statement").len() == 2
        })
        .await;
    let statements = named(&spans, "codelake.pg.statement");
    let texts: BTreeSet<&str> = statements.iter().filter_map(|s| s.tag("db.query.text")).collect();
    assert_eq!(texts, BTreeSet::from(["SELECT ? + ? AS answer", "SELECT ? AS v"]));
    let sessions: BTreeSet<&str> = statements
        .iter()
        .filter_map(|s| s.tag("codelake.session_id"))
        .collect();
    assert_eq!(sessions.len(), 1, "{statements:#?}");
    let parents: BTreeSet<Option<&str>> = statements.iter().map(|s| s.parent.as_deref()).collect();
    assert_eq!(parents.len(), 1, "same synthetic session parent: {statements:#?}");
    for statement in &statements {
        assert_eq!(statement.tag("enduser.id"), Some("codelake"));
        assert_eq!(statement.tag("codelake.protocol"), Some("postgresql.simple"));
        assert_eq!(statement.tag("codelake.trace_id"), Some(trace_id.as_str()));
        assert!(!statement.tags.values().any(|v| v.contains("hidden-value")));
    }
}
