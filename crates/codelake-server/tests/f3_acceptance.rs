//! F3 acceptance suite: the Trino client protocol of `CodeLake` against the
//! behavior of a real Trino 431 server, written by an independent tester.
//!
//! How the comparison works: every protocol expectation is a [`Check`] — SQL,
//! optional setup, and an assertion on the outcome. The test
//! `trino_431_reference_confirms_every_check` runs **every** check (passing
//! and defect ones) against a real `trinodb/trino:431` server, so each
//! expectation is Trino's observed behavior, not a reading of the docs. The
//! other tests run the same checks against the real `codelake` binary.
//!
//! Real clients: the Trino 431 CLI, the JDBC driver 431, the Python `trino`
//! client (DB-API and `SQLAlchemy`) and dbt-trino run in containers on the
//! default bridge network and reach the server through this container's IP.
//!
//! Tests that expose a defect are `#[ignore = "DEFECT-<n>: ..."]` and assert
//! Trino's behavior; run them with `--run-ignored all`.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    reason = "test crate"
)]

use std::io::Write;
use std::net::{IpAddr, TcpListener, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};

const BIN: &str = env!("CARGO_BIN_EXE_codelake");
/// Every real client sends it (CLI, JDBC, Python); without it Trino falls back
/// to legacy datetime types.
const CAPABILITIES: (&str, &str) = ("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME");

// ---------------------------------------------------------------- server

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// This container's address on the Docker bridge, reachable by client
/// containers (no packet is sent: `connect` on UDP only picks a route).
fn own_ip() -> IpAddr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    socket.local_addr().unwrap().ip()
}

struct Limits {
    max_active_queries: usize,
    client_timeout_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_active_queries: 100,
            client_timeout_secs: 300,
        }
    }
}

struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    async fn start(limits: &Limits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (pg, http, port) = (free_port(), free_port(), free_port());
        let path = dir.path().join("codelake.toml");
        let mut file = std::fs::File::create(&path).unwrap();
        write!(
            file,
            r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = 1
# Client containers reach the Trino listener over the Docker bridge.
allow_unauthenticated_network = true

[server.trino]
listen = "0.0.0.0:{port}"
max_active_queries = {max_active}
client_timeout_secs = {client_timeout}

[memory]
limit = "1GiB"
spill_dir = "{spill}"
spill_limit = "1GiB"
metadata_cache_limit = "8MiB"
"#,
            max_active = limits.max_active_queries,
            client_timeout = limits.client_timeout_secs,
            spill = dir.path().join("spill").display()
        )
        .unwrap();
        let child = Command::new(BIN)
            .args(["serve", "--config"])
            .arg(&path)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            port,
            _dir: dir,
        };
        wait_info(&server.base()).await;
        server
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Host and port as seen from a client container.
    fn external(&self) -> (String, u16) {
        (own_ip().to_string(), self.port)
    }

    /// CPU seconds (user + system) the server process has used so far.
    fn cpu_seconds(&self) -> f64 {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", self.child.id())).unwrap();
        // Fields after the `(comm)`; utime and stime are fields 14 and 15.
        let after = stat.rsplit_once(')').unwrap().1;
        let fields: Vec<&str> = after.split_whitespace().collect();
        let ticks: f64 = fields[11].parse::<f64>().unwrap() + fields[12].parse::<f64>().unwrap();
        ticks / 100.0
    }
}

async fn wait_info(base: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(response) = reqwest::get(format!("{base}/v1/info")).await
            && response.status() == 200
            && let Ok(body) = response.text().await
            && body.contains("\"starting\":false")
        {
            return;
        }
        assert!(Instant::now() < deadline, "{base} did not start");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// A real Trino 431 server with the `memory` catalog and an Iceberg catalog
/// (for `DELETE`/`UPDATE`/`ALTER`, which `memory` does not support).
async fn trino_431() -> (ContainerAsync<GenericImage>, String) {
    const JVM: &str = "-server\n-Xmx1G\n-XX:+UseG1GC\n-XX:G1HeapRegionSize=32M\n\
        -XX:+ExplicitGCInvokesConcurrent\n-XX:+ExitOnOutOfMemoryError\n\
        -XX:-OmitStackTraceInFastThrow\n-XX:ReservedCodeCacheSize=150M\n\
        -Djdk.attach.allowAttachSelf=true\n-Djdk.nio.maxCachedBufferSize=2000000\n";
    const ICEBERG: &str = "connector.name=iceberg\niceberg.catalog.type=TESTING_FILE_METASTORE\n\
        hive.metastore.catalog.dir=file:///tmp/iceberg-wh\n";
    let container = GenericImage::new("trinodb/trino", "431")
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_wait_for(WaitFor::message_on_either_std("SERVER STARTED"))
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/jvm.config").with_mode(0o644),
            JVM.as_bytes().to_vec(),
        )
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/catalog/iceberg.properties").with_mode(0o644),
            ICEBERG.as_bytes().to_vec(),
        )
        .with_container_name(format!("codelake-f3v-acc-trino-{}", std::process::id()))
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(8080).await.unwrap();
    let base = format!("http://{host}:{port}");
    wait_info(&base).await;
    // Right after start the memory connector may answer NO_NODES_AVAILABLE
    // until the node registers itself; wait until it can scan a table.
    run(&base, "CREATE TABLE memory.default.warmup AS SELECT 1 AS a", &[]).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while run(&base, "SELECT a FROM memory.default.warmup", &[])
        .await
        .error()
        .is_some()
    {
        assert!(
            Instant::now() < deadline,
            "Trino memory connector never became ready"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (container, base)
}

// ---------------------------------------------------------------- HTTP

struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }
}

async fn http(method: reqwest::Method, url: &str, body: Option<&str>, headers: &[(&str, &str)]) -> Resp {
    let mut request = reqwest::Client::new()
        .request(method, url)
        .timeout(Duration::from_secs(120));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    if let Some(body) = body {
        request = request.body(body.to_owned());
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(n, v)| (n.as_str().to_owned(), v.to_str().unwrap_or_default().to_owned()))
        .collect();
    let body = response.bytes().await.unwrap().to_vec();
    Resp {
        status,
        headers,
        body,
    }
}

fn with_user<'a>(headers: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut all = vec![("X-Trino-User", "acc")];
    all.retain(|(name, _)| !headers.iter().any(|(n, _)| n.eq_ignore_ascii_case(name)));
    all.extend_from_slice(headers);
    all
}

async fn post(base: &str, sql: &str, headers: &[(&str, &str)]) -> Resp {
    http(
        reqwest::Method::POST,
        &format!("{base}/v1/statement"),
        Some(sql),
        &with_user(headers),
    )
    .await
}

async fn get(uri: &str) -> Resp {
    http(reqwest::Method::GET, uri, None, &with_user(&[])).await
}

/// A statement followed through `nextUri` to its end.
#[derive(Debug)]
struct Outcome {
    columns: Option<Vec<Value>>,
    data: Vec<Value>,
    last: Value,
    headers: Vec<(String, String)>,
    data_pages: usize,
}

impl Outcome {
    fn error(&self) -> Option<&Value> {
        self.last.get("error")
    }

    fn update_type(&self) -> Option<&str> {
        self.last.get("updateType").and_then(Value::as_str)
    }

    fn update_count(&self) -> Option<u64> {
        self.last.get("updateCount").and_then(Value::as_u64)
    }

    fn state(&self) -> &str {
        self.last["stats"]["state"].as_str().unwrap_or_default()
    }

    fn columns(&self) -> &[Value] {
        self.columns.as_deref().unwrap_or_default()
    }

    fn names(&self) -> Vec<&str> {
        self.columns()
            .iter()
            .map(|c| c["name"].as_str().unwrap())
            .collect()
    }

    fn types(&self) -> Vec<&str> {
        self.columns()
            .iter()
            .map(|c| c["type"].as_str().unwrap())
            .collect()
    }

    fn signature(&self, index: usize) -> &Value {
        &self.columns()[index]["typeSignature"]
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn assert_ok(&self) {
        assert!(self.error().is_none(), "unexpected error: {}", self.last);
        assert_eq!(self.state(), "FINISHED", "{}", self.last);
    }

    fn assert_error(&self, name: &str, code: i64, kind: &str) {
        let error = self
            .error()
            .unwrap_or_else(|| panic!("expected {name}, got success: {}", self.last));
        assert_eq!(error["errorName"], name, "{error}");
        assert_eq!(error["errorCode"], code, "{error}");
        assert_eq!(error["errorType"], kind, "{error}");
        assert_eq!(self.state(), "FAILED", "{}", self.last);
    }
}

async fn run(base: &str, sql: &str, headers: &[(&str, &str)]) -> Outcome {
    let first = post(base, sql, headers).await;
    assert_eq!(
        first.status,
        200,
        "POST {sql}: {}",
        String::from_utf8_lossy(&first.body)
    );
    let mut response_headers = first.headers.clone();
    let mut page = first.json();
    let mut columns = page.get("columns").and_then(Value::as_array).cloned();
    let mut data: Vec<Value> = page
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut data_pages = usize::from(!data.is_empty());
    let deadline = Instant::now() + Duration::from_secs(240);
    while let Some(next) = page.get("nextUri").and_then(Value::as_str).map(str::to_owned) {
        assert!(Instant::now() < deadline, "{sql} did not finish");
        let response = get(&next).await;
        assert_eq!(
            response.status,
            200,
            "GET {next}: {}",
            String::from_utf8_lossy(&response.body)
        );
        response_headers.extend(response.headers.clone());
        page = response.json();
        if columns.is_none() {
            columns = page.get("columns").and_then(Value::as_array).cloned();
        }
        if let Some(rows) = page.get("data").and_then(Value::as_array) {
            data_pages += 1;
            data.extend(rows.iter().cloned());
        }
    }
    Outcome {
        columns,
        data,
        last: page,
        headers: response_headers,
        data_pages,
    }
}

fn sig(raw: &str, arguments: &Value) -> Value {
    json!({"rawType": raw, "arguments": arguments})
}

fn long(value: i64) -> Value {
    json!({"kind": "LONG", "value": value})
}

// ---------------------------------------------------------------- checks

/// One protocol expectation, confirmed against Trino 431 by
/// `trino_431_reference_confirms_every_check`.
struct Check {
    id: &'static str,
    /// Trino catalog: `memory`, or `iceberg` where `memory` lacks the feature.
    trino_catalog: &'static str,
    setup: &'static [&'static str],
    /// `{s}` is replaced by `<catalog>.<schema of this check>`, `{c}` by the catalog.
    sql: &'static str,
    /// Trino spelling when the engines' SQL dialects differ.
    trino_sql: Option<&'static str>,
    headers: &'static [(&'static str, &'static str)],
    /// Send no client capabilities (a plain HTTP script).
    legacy: bool,
    expect: fn(&Outcome),
}

const fn check(id: &'static str, sql: &'static str, expect: fn(&Outcome)) -> Check {
    Check {
        id,
        trino_catalog: "memory",
        setup: &[],
        sql,
        trino_sql: None,
        headers: &[],
        legacy: false,
        expect,
    }
}

const SCHEMA: &[&str] = &["CREATE SCHEMA {s}"];
const TABLE: &[&str] = &["CREATE SCHEMA {s}", "CREATE TABLE {s}.t (a bigint, b varchar)"];
const ROWS: &[&str] = &[
    "CREATE SCHEMA {s}",
    "CREATE TABLE {s}.t (a bigint, b varchar)",
    "INSERT INTO {s}.t VALUES (1, 'x'), (2, 'y'), (3, 'z')",
];

/// Trino 431's `system.runtime.queries` (observed with `DESCRIBE`).
const RUNTIME_QUERIES: &[(&str, &str)] = &[
    ("query_id", "varchar"),
    ("state", "varchar"),
    ("user", "varchar"),
    ("source", "varchar"),
    ("query", "varchar"),
    ("resource_group_id", "array(varchar)"),
    ("queued_time_ms", "bigint"),
    ("analysis_time_ms", "bigint"),
    ("planning_time_ms", "bigint"),
    ("created", "timestamp(3) with time zone"),
    ("started", "timestamp(3) with time zone"),
    ("last_heartbeat", "timestamp(3) with time zone"),
    ("end", "timestamp(3) with time zone"),
    ("error_type", "varchar"),
    ("error_code", "varchar"),
];

fn assert_ddl(o: &Outcome, update_type: &str) {
    o.assert_ok();
    assert_eq!(o.update_type(), Some(update_type), "{}", o.last);
    assert!(
        o.columns().is_empty(),
        "Trino sends no result columns for DDL: {:?}",
        o.columns
    );
    assert!(o.data.is_empty(), "Trino sends no rows for DDL: {:?}", o.data);
    assert_eq!(o.update_count(), None);
}

fn assert_row_count(o: &Outcome, update_type: &str, count: u64) {
    o.assert_ok();
    assert_eq!(o.update_type(), Some(update_type), "{}", o.last);
    assert_eq!(o.update_count(), Some(count), "{}", o.last);
    assert_eq!(o.names(), ["rows"]);
    assert_eq!(o.types(), ["bigint"]);
    assert_eq!(o.data, [json!([count])]);
}

/// Checks `CodeLake` passes today.
fn passing_checks() -> Vec<Check> {
    vec![
        check(
            "p_integers",
            "SELECT CAST(1 AS TINYINT) a, CAST(2 AS SMALLINT) b, CAST(3 AS INTEGER) c, \
             CAST(4 AS BIGINT) d, CAST(2.5 AS DOUBLE) f, true w",
            |o| {
                o.assert_ok();
                let types = ["tinyint", "smallint", "integer", "bigint", "double", "boolean"];
                assert_eq!(o.types(), types);
                for (i, t) in types.iter().enumerate() {
                    assert_eq!(o.signature(i), &sig(t, &json!([])));
                }
                assert_eq!(o.data, [json!([1, 2, 3, 4, 2.5, true])]);
            },
        ),
        check(
            "p_decimal_values",
            "SELECT CAST('12.50' AS DECIMAL(10,2)) g, CAST('-0.05' AS DECIMAL(38,10)) h, \
             CAST('0.005' AS DECIMAL(4,3)) k",
            |o| {
                o.assert_ok();
                assert_eq!(o.signature(0), &sig("decimal", &json!([long(10), long(2)])));
                assert_eq!(o.signature(1), &sig("decimal", &json!([long(38), long(10)])));
                assert_eq!(o.signature(2), &sig("decimal", &json!([long(4), long(3)])));
                assert_eq!(o.data, [json!(["12.50", "-0.0500000000", "0.005"])]);
            },
        ),
        check("p_varchar", "SELECT CAST('ação' AS VARCHAR) i", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["varchar"]);
            assert_eq!(o.signature(0), &sig("varchar", &json!([long(2_147_483_647)])));
            assert_eq!(o.data, [json!(["ação"])]);
        }),
        check("p_varbinary_base64", "SELECT X'002A' l", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["varbinary"]);
            assert_eq!(o.data, [json!(["ACo="])]);
        }),
        check(
            "p_dates",
            "SELECT DATE '1969-07-20' m, DATE '0001-01-01' x, DATE '2026-09-23' y",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["date", "date", "date"]);
                assert_eq!(o.data, [json!(["1969-07-20", "0001-01-01", "2026-09-23"])]);
            },
        ),
        check(
            "p_timestamp_before_epoch",
            "SELECT CAST(TIMESTAMP '1960-01-02 03:04:05.123456' AS TIMESTAMP(6)) o, \
             CAST(TIMESTAMP '1969-12-31 23:59:59.999999' AS TIMESTAMP(6)) r",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["timestamp(6)", "timestamp(6)"]);
                assert_eq!(o.signature(0), &sig("timestamp", &json!([long(6)])));
                assert_eq!(
                    o.data,
                    [json!([
                        "1960-01-02 03:04:05.123456",
                        "1969-12-31 23:59:59.999999"
                    ])]
                );
            },
        ),
        check(
            "p_nan_infinity",
            "SELECT CAST('NaN' AS DOUBLE) a, CAST('Infinity' AS DOUBLE) b, CAST('-Infinity' AS DOUBLE) c",
            |o| {
                o.assert_ok();
                assert_eq!(o.data, [json!(["NaN", "Infinity", "-Infinity"])]);
            },
        ),
        check(
            "p_array",
            "SELECT ARRAY[CAST(1 AS BIGINT), CAST(2 AS BIGINT), NULL] q",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["array(bigint)"]);
                assert_eq!(
                    o.signature(0),
                    &sig(
                        "array",
                        &json!([{"kind": "TYPE", "value": sig("bigint", &json!([]))}])
                    )
                );
                assert_eq!(o.data, [json!([[1, 2, null]])]);
            },
        ),
        check(
            "p_typed_null",
            "SELECT CAST(NULL AS BIGINT) n, CAST(NULL AS VARCHAR) s",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["bigint", "varchar"]);
                assert_eq!(o.data, [json!([null, null])]);
            },
        ),
        Check {
            setup: TABLE,
            ..check(
                "p_insert_count",
                "INSERT INTO {s}.t VALUES (1, 'x'), (2, 'y'), (3, 'z')",
                |o| assert_row_count(o, "INSERT", 3),
            )
        },
        Check {
            setup: ROWS,
            ..check(
                "p_insert_zero_rows",
                "INSERT INTO {s}.t SELECT a, b FROM {s}.t WHERE a > 100",
                |o| assert_row_count(o, "INSERT", 0),
            )
        },
        check("p_set_session", "SET SESSION query_max_run_time = '2h'", |o| {
            o.assert_ok();
            assert_eq!(o.update_type(), Some("SET SESSION"));
            assert_eq!(o.header("x-trino-set-session"), Some("query_max_run_time=2h"));
        }),
        check("p_reset_session", "RESET SESSION query_max_run_time", |o| {
            o.assert_ok();
            assert_eq!(o.update_type(), Some("RESET SESSION"));
            assert_eq!(o.header("x-trino-clear-session"), Some("query_max_run_time"));
        }),
        Check {
            setup: SCHEMA,
            ..check("p_use", "USE {s}", |o| {
                o.assert_ok();
                assert_eq!(o.update_type(), Some("USE"));
                assert_eq!(o.header("x-trino-set-schema"), Some("acc_p_use"));
                assert!(o.header("x-trino-set-catalog").is_some_and(|c| !c.is_empty()));
            })
        },
        check("p_set_session_unknown", "SET SESSION nope = 1", |o| {
            o.assert_error("INVALID_SESSION_PROPERTY", 14, "USER_ERROR");
        }),
        check(
            "p_set_session_bad_value",
            "SET SESSION query_max_run_time = 'soon'",
            |o| o.assert_error("INVALID_SESSION_PROPERTY", 14, "USER_ERROR"),
        ),
        Check {
            headers: &[("X-Trino-Session", "nope=1")],
            ..check("p_session_header_unknown", "SELECT 1 AS x", |o| {
                o.assert_error("INVALID_SESSION_PROPERTY", 14, "USER_ERROR");
            })
        },
        Check {
            headers: &[("X-Trino-Session", "query_max_run_time=1h")],
            ..check("p_show_session", "SHOW SESSION", |o| {
                o.assert_ok();
                assert_eq!(o.names(), ["Name", "Value", "Default", "Type", "Description"]);
                for i in 0..5 {
                    assert_eq!(o.signature(i)["rawType"], "varchar");
                }
                let row = o
                    .data
                    .iter()
                    .find(|r| r[0] == "query_max_run_time")
                    .unwrap_or_else(|| panic!("no query_max_run_time: {:?}", o.data));
                assert_eq!(row[1], "1h", "{row}");
                assert_eq!(row[3], "varchar", "{row}");
            })
        },
        check("p_error_syntax", "SELEC 1", |o| {
            o.assert_error("SYNTAX_ERROR", 1, "USER_ERROR");
            assert_eq!(
                o.error().unwrap()["errorLocation"],
                json!({"lineNumber": 1, "columnNumber": 1})
            );
        }),
        Check {
            setup: SCHEMA,
            ..check("p_error_table", "SELECT * FROM {s}.missing_table", |o| {
                o.assert_error("TABLE_NOT_FOUND", 46, "USER_ERROR");
            })
        },
        Check {
            setup: TABLE,
            ..check("p_error_column", "SELECT nope FROM {s}.t", |o| {
                o.assert_error("COLUMN_NOT_FOUND", 47, "USER_ERROR");
            })
        },
        check("p_error_division", "SELECT 1/0 AS x", |o| {
            o.assert_error("DIVISION_BY_ZERO", 8, "USER_ERROR");
        }),
        check("p_error_cast", "SELECT CAST('abc' AS INTEGER) AS x", |o| {
            o.assert_error("INVALID_CAST_ARGUMENT", 9, "USER_ERROR");
        }),
        Check {
            headers: &[("X-Trino-Session", "query_max_run_time=2s")],
            trino_sql: Some(
                "SELECT count(*) FROM UNNEST(sequence(1, 10000)) a CROSS JOIN UNNEST(sequence(1, 10000)) b \
                 CROSS JOIN UNNEST(sequence(1, 10000)) c",
            ),
            ..check(
                "p_query_max_run_time",
                "SELECT count(*) FROM generate_series(1, 100000) a CROSS JOIN generate_series(1, 100000) b \
                 WHERE a.value + b.value = 7",
                |o| {
                    o.assert_error("EXCEEDED_TIME_LIMIT", 131_075, "INSUFFICIENT_RESOURCES");
                    assert_eq!(
                        o.error().unwrap()["message"],
                        "Query exceeded maximum time limit of 2.00s"
                    );
                },
            )
        },
        check(
            "p_runtime_queries_columns",
            "SELECT * FROM system.runtime.queries WHERE query_id = 'none'",
            |o| {
                o.assert_ok();
                let expected: Vec<(&str, &str)> = RUNTIME_QUERIES.to_vec();
                let actual: Vec<(&str, &str)> = o.names().into_iter().zip(o.types()).collect();
                assert_eq!(actual, expected);
            },
        ),
    ]
}

/// Checks `CodeLake` fails today; each is asserted by one `DEFECT-<n>` test.
fn defect_checks() -> Vec<Check> {
    vec![
        check("d_null_literal", "SELECT NULL AS v", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["unknown"]);
            assert_eq!(o.data, [json!([null])], "a NULL literal must travel as JSON null");
        }),
        check("d_real", "SELECT CAST(1.1 AS REAL) e, CAST(0.1 AS REAL) f", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["real", "real"]);
            assert_eq!(o.data[0][0].as_f64(), Some(1.1), "{:?}", o.data);
            assert_eq!(o.data[0][1].as_f64(), Some(0.1), "{:?}", o.data);
        }),
        Check {
            trino_sql: Some("SELECT MAP(ARRAY['a'], ARRAY[1]) r"),
            ..check("d_map", "SELECT map(['a'], [CAST(1 AS INTEGER)]) r", |o| {
                o.assert_ok();
                let signature = o.signature(0);
                assert_eq!(signature["rawType"], "map", "{signature}");
                assert_eq!(signature["arguments"][1]["value"], sig("integer", &json!([])));
                assert_eq!(o.data, [json!([{"a": 1}])]);
            })
        },
        Check {
            trino_sql: Some("SELECT CAST(ROW(1, 'x') AS ROW(x INTEGER, y VARCHAR)) s"),
            ..check(
                "d_row",
                "SELECT named_struct('x', CAST(1 AS INTEGER), 'y', 'x') s",
                |o| {
                    o.assert_ok();
                    let signature = o.signature(0);
                    assert_eq!(signature["rawType"], "row", "{signature}");
                    let fields: Vec<&Value> = signature["arguments"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|a| &a["value"]["fieldName"]["name"])
                        .collect();
                    assert_eq!(fields, [&json!("x"), &json!("y")]);
                    assert_eq!(o.data, [json!([[1, "x"]])]);
                },
            )
        },
        Check {
            trino_sql: Some("SELECT from_unixtime(0, 'America/Sao_Paulo') t"),
            ..check(
                "d_timestamp_tz_keeps_zone",
                "SELECT arrow_cast(0, 'Timestamp(Millisecond, Some(\"America/Sao_Paulo\"))') t",
                |o| {
                    o.assert_ok();
                    assert_eq!(o.types(), ["timestamp(3) with time zone"]);
                    assert_eq!(o.data, [json!(["1969-12-31 21:00:00.000 America/Sao_Paulo"])]);
                },
            )
        },
        check(
            "d_timestamp_tz_literal",
            "SELECT TIMESTAMP '2026-01-01 10:00:00.123 America/Sao_Paulo' p",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["timestamp(3) with time zone"]);
                assert_eq!(o.data, [json!(["2026-01-01 10:00:00.123 America/Sao_Paulo"])]);
            },
        ),
        check("d_char", "SELECT CAST('ab' AS CHAR(3)) k", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["char(3)"]);
            assert_eq!(o.data, [json!(["ab "])]);
        }),
        check("d_varchar_n", "SELECT CAST('ab' AS VARCHAR(5)) j", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["varchar(5)"]);
            assert_eq!(o.signature(0), &sig("varchar", &json!([long(5)])));
        }),
        check("d_json", "SELECT JSON '{\"a\":1}' t", |o| {
            o.assert_ok();
            assert_eq!(o.types(), ["json"]);
            assert_eq!(o.data, [json!(["{\"a\":1}"])]);
        }),
        check(
            "d_uuid",
            "SELECT UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' u",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["uuid"]);
                assert_eq!(o.data, [json!(["12151fd2-7586-11e9-8f9e-2a86e4085a59"])]);
            },
        ),
        Check {
            legacy: true,
            ..check(
                "d_legacy_datetime",
                "SELECT CAST(TIMESTAMP '1960-01-02 03:04:05.123456' AS TIMESTAMP(6)) o",
                |o| {
                    o.assert_ok();
                    assert_eq!(o.types(), ["timestamp"]);
                    assert_eq!(o.signature(0), &sig("timestamp", &json!([])));
                    assert_eq!(o.data, [json!(["1960-01-02 03:04:05.123"])]);
                },
            )
        },
        check(
            "d_decimal_type_text",
            "SELECT CAST('12.50' AS DECIMAL(10,2)) g",
            |o| {
                o.assert_ok();
                assert_eq!(o.types(), ["decimal(10, 2)"]);
            },
        ),
        check(
            "d_unnamed_column",
            "SELECT count(*) FROM (VALUES (1), (2)) t(x)",
            |o| {
                o.assert_ok();
                assert_eq!(o.names(), ["_col0"]);
                assert_eq!(o.data, [json!([2])]);
            },
        ),
        Check {
            trino_sql: Some("SELECT value, CAST(value AS VARCHAR) FROM UNNEST(sequence(1, 3)) t(value)"),
            ..check(
                "d_unaliased_expressions",
                "SELECT value, CAST(value AS VARCHAR) FROM generate_series(1, 3)",
                |o| {
                    o.assert_ok();
                    assert_eq!(o.names(), ["value", "_col1"]);
                    assert_eq!(o.data.len(), 3);
                    assert_eq!(o.data[2][1], "3");
                },
            )
        },
        check("d_ddl_create_schema", "CREATE SCHEMA {s}", |o| {
            assert_ddl(o, "CREATE SCHEMA");
        }),
        Check {
            setup: SCHEMA,
            ..check("d_ddl_create_table", "CREATE TABLE {s}.t (a bigint)", |o| {
                assert_ddl(o, "CREATE TABLE");
            })
        },
        Check {
            setup: TABLE,
            ..check(
                "d_ddl_create_view",
                "CREATE VIEW {s}.v AS SELECT a FROM {s}.t",
                |o| {
                    assert_ddl(o, "CREATE VIEW");
                },
            )
        },
        Check {
            setup: TABLE,
            ..check("d_ddl_drop_table", "DROP TABLE {s}.t", |o| {
                assert_ddl(o, "DROP TABLE");
            })
        },
        Check {
            setup: ROWS,
            ..check("d_ctas_count", "CREATE TABLE {s}.c AS SELECT * FROM {s}.t", |o| {
                assert_row_count(o, "CREATE TABLE", 3);
            })
        },
        Check {
            setup: ROWS,
            trino_catalog: "iceberg",
            ..check("d_delete_count", "DELETE FROM {s}.t WHERE a = 1", |o| {
                assert_row_count(o, "DELETE", 1);
            })
        },
        Check {
            setup: ROWS,
            trino_catalog: "iceberg",
            ..check("d_update_count", "UPDATE {s}.t SET b = 'q' WHERE a = 2", |o| {
                assert_row_count(o, "UPDATE", 1);
            })
        },
        Check {
            setup: TABLE,
            ..check("d_rename_table", "ALTER TABLE {s}.t RENAME TO {s}.t2", |o| {
                o.assert_ok();
                assert_eq!(o.update_type(), Some("RENAME TABLE"));
            })
        },
        Check {
            setup: SCHEMA,
            ..check(
                "d_view_security_definer",
                "CREATE OR REPLACE VIEW {s}.v SECURITY DEFINER AS SELECT 1 AS x",
                |o| {
                    o.assert_ok();
                    assert_eq!(o.update_type(), Some("CREATE VIEW"));
                },
            )
        },
        check(
            "d_metadata_materialized_views",
            "SELECT count(*) AS n FROM system.metadata.materialized_views WHERE catalog_name = '{c}'",
            |o| {
                o.assert_ok();
                assert_eq!(o.data, [json!([0])]);
            },
        ),
        Check {
            setup: SCHEMA,
            ..check(
                "d_information_schema_tables_scope",
                "SELECT count(*) AS n FROM {c}.information_schema.tables WHERE table_catalog <> '{c}'",
                |o| {
                    o.assert_ok();
                    assert_eq!(o.data, [json!([0])], "rows of other catalogs leak in");
                },
            )
        },
        Check {
            setup: SCHEMA,
            ..check(
                "d_information_schema_schemata_scope",
                "SELECT count(*) AS n FROM {c}.information_schema.schemata WHERE catalog_name <> '{c}'",
                |o| {
                    o.assert_ok();
                    assert_eq!(o.data, [json!([0])], "schemas of other catalogs leak in");
                },
            )
        },
        check(
            "d_describe_runtime_queries",
            "DESCRIBE system.runtime.queries",
            |o| {
                o.assert_ok();
                assert_eq!(o.names(), ["Column", "Type", "Extra", "Comment"]);
                let rows: Vec<(&str, &str)> = o
                    .data
                    .iter()
                    .map(|r| (r[0].as_str().unwrap(), r[1].as_str().unwrap()))
                    .collect();
                assert_eq!(rows, RUNTIME_QUERIES.to_vec());
            },
        ),
        check("d_show_session_like", "SHOW SESSION LIKE 'query_max_run%'", |o| {
            o.assert_ok();
            assert_eq!(o.data.len(), 1, "{:?}", o.data);
            assert_eq!(o.data[0][0], "query_max_run_time");
        }),
        check("d_show_session_default_format", "SHOW SESSION", |o| {
            o.assert_ok();
            let row = o
                .data
                .iter()
                .find(|r| r[0] == "query_max_run_time")
                .unwrap_or_else(|| panic!("{:?}", o.data));
            assert_eq!(row[2], "100.00d", "airlift Duration text: {row}");
        }),
        Check {
            setup: TABLE,
            ..check("d_error_table_exists", "CREATE TABLE {s}.t (a bigint)", |o| {
                o.assert_error("TABLE_ALREADY_EXISTS", 50, "USER_ERROR");
            })
        },
        Check {
            setup: SCHEMA,
            ..check("d_error_schema_exists", "CREATE SCHEMA {s}", |o| {
                o.assert_error("SCHEMA_ALREADY_EXISTS", 49, "USER_ERROR");
            })
        },
        Check {
            setup: SCHEMA,
            ..check("d_error_drop_missing", "DROP TABLE {s}.nope", |o| {
                o.assert_error("TABLE_NOT_FOUND", 46, "USER_ERROR");
            })
        },
        check("d_error_function", "SELECT nosuchfn(1) AS x", |o| {
            o.assert_error("FUNCTION_NOT_FOUND", 6, "USER_ERROR");
        }),
        check("d_error_catalog", "SELECT * FROM nocat.s.t", |o| {
            o.assert_error("CATALOG_NOT_FOUND", 44, "USER_ERROR");
        }),
        check("d_error_schema", "SELECT * FROM {c}.noschema.t", |o| {
            o.assert_error("SCHEMA_NOT_FOUND", 45, "USER_ERROR");
        }),
        check("d_error_type_mismatch", "SELECT 1 + 'a' AS x", |o| {
            o.assert_error("TYPE_MISMATCH", 58, "USER_ERROR");
        }),
        Check {
            setup: TABLE,
            ..check(
                "d_error_ambiguous",
                "SELECT * FROM {s}.t t1 JOIN {s}.t t2 ON t1.a = t2.a WHERE a = 1",
                |o| o.assert_error("AMBIGUOUS_NAME", 62, "USER_ERROR"),
            )
        },
        check("d_error_location_eof", "SELECT 1 +", |o| {
            o.assert_error("SYNTAX_ERROR", 1, "USER_ERROR");
            assert_eq!(
                o.error().unwrap()["errorLocation"],
                json!({"lineNumber": 1, "columnNumber": 11})
            );
        }),
        Check {
            setup: SCHEMA,
            ..check(
                "d_error_location_semantic",
                "SELECT 1\n  FROM\n  {s}.missing2",
                |o| {
                    o.assert_error("TABLE_NOT_FOUND", 46, "USER_ERROR");
                    assert_eq!(
                        o.error().unwrap()["errorLocation"],
                        json!({"lineNumber": 3, "columnNumber": 3})
                    );
                },
            )
        },
        check(
            "d_scalar_subquery_rows",
            "SELECT (SELECT a FROM (VALUES (1), (2)) x(a)) AS y",
            |o| o.assert_error("SUBQUERY_MULTIPLE_ROWS", 28, "USER_ERROR"),
        ),
    ]
}

fn find_check(id: &str) -> Check {
    passing_checks()
        .into_iter()
        .chain(defect_checks())
        .find(|c| c.id == id)
        .unwrap_or_else(|| panic!("no check {id}"))
}

fn expand(sql: &str, catalog: &str, id: &str) -> String {
    sql.replace("{s}", &format!("{catalog}.acc_{id}"))
        .replace("{c}", catalog)
}

/// Runs `check` against the server at `base`; `trino` picks Trino's catalog
/// and spelling, otherwise `CodeLake`'s in-memory catalog.
async fn run_check(base: &str, check: &Check, trino: bool) -> Outcome {
    let catalog = if trino { check.trino_catalog } else { "datafusion" };
    let mut headers: Vec<(&str, &str)> = check.headers.to_vec();
    if !check.legacy {
        headers.push(CAPABILITIES);
    }
    for statement in check.setup {
        let setup = run(base, &expand(statement, catalog, check.id), &headers).await;
        assert!(
            setup.error().is_none(),
            "{} setup {statement}: {}",
            check.id,
            setup.last
        );
    }
    let sql = match (trino, check.trino_sql) {
        (true, Some(sql)) => sql,
        _ => check.sql,
    };
    run(base, &expand(sql, catalog, check.id), &headers).await
}

async fn assert_codelake(ids: &[&str]) {
    let server = Server::start(&Limits::default()).await;
    for id in ids {
        let check = find_check(id);
        let outcome = run_check(&server.base(), &check, false).await;
        (check.expect)(&outcome);
    }
}

// ---------------------------------------------------------------- protocol flows

/// What happens around `nextUri`: retries, stale and future tokens, bad ids,
/// and `DELETE` of a running statement.
#[derive(Debug, PartialEq, Eq)]
struct TokenReport {
    repeat_is_identical: bool,
    old_token_after_advance: u16,
    future_token: u16,
    wrong_slug: u16,
    unknown_id: u16,
    delete: u16,
    after_delete: (String, i64, bool),
}

fn with_token(uri: &str, token: u64) -> String {
    format!("{}/{token}", uri.rsplit_once('/').unwrap().0)
}

fn token_of(uri: &str) -> u64 {
    uri.rsplit_once('/').unwrap().1.parse().unwrap()
}

async fn token_report(base: &str, big_query: &str) -> TokenReport {
    let mut page = post(base, big_query, &[CAPABILITIES]).await.json();
    let id = page["id"].as_str().unwrap().to_owned();
    // Advance to the first page with rows.
    let (current, body) = loop {
        let uri = page["nextUri"].as_str().unwrap().to_owned();
        let response = get(&uri).await;
        page = response.json();
        if page.get("data").is_some() {
            break (uri, response.body);
        }
    };
    let next = page["nextUri"].as_str().unwrap().to_owned();
    let repeat_is_identical = get(&current).await.body == body;
    let future_token = get(&with_token(&next, token_of(&next) + 1)).await.status;
    let (prefix, token) = next.rsplit_once('/').unwrap();
    let (prefix, _slug) = prefix.rsplit_once('/').unwrap();
    let wrong_slug = get(&format!("{prefix}/wrongslug/{token}")).await.status;
    let unknown_id = get(&next.replace(&id, "20200101_000000_00000_aaaaa"))
        .await
        .status;
    let advanced = get(&next).await.json();
    let old_token_after_advance = get(&current).await.status;
    let latest = advanced["nextUri"].as_str().unwrap().to_owned();
    let delete = http(reqwest::Method::DELETE, &latest, None, &with_user(&[]))
        .await
        .status;
    // Trino may answer one more page before it notices the cancel.
    let mut uri = latest;
    let mut last;
    loop {
        last = get(&uri).await.json();
        match last.get("nextUri").and_then(Value::as_str) {
            Some(next) => uri = next.to_owned(),
            None => break,
        }
    }
    TokenReport {
        repeat_is_identical,
        old_token_after_advance,
        future_token,
        wrong_slug,
        unknown_id,
        delete,
        after_delete: (
            last["error"]["errorName"].as_str().unwrap_or_default().to_owned(),
            last["error"]["errorCode"].as_i64().unwrap_or(-1),
            last.get("nextUri").is_some(),
        ),
    }
}

/// What Trino 431 answers (observed on `trinodb/trino:431`).
fn trino_tokens() -> TokenReport {
    TokenReport {
        repeat_is_identical: true,
        old_token_after_advance: 410,
        future_token: 404,
        wrong_slug: 404,
        unknown_id: 404,
        delete: 204,
        after_delete: ("USER_CANCELED".to_owned(), 3, false),
    }
}

const CODELAKE_BIG: &str = "SELECT value AS x, 'padding-padding-padding-padding-' || CAST(value AS VARCHAR) AS p \
                            FROM generate_series(1, 200000)";
const TRINO_BIG: &str = "SELECT a * 100 + b AS x, 'padding-padding-padding-padding-' || CAST(a AS VARCHAR) AS p \
                         FROM UNNEST(sequence(1, 2000)) t(a) CROSS JOIN UNNEST(sequence(1, 100)) u(b)";

/// HTTP statuses of malformed submissions: (no user, schema without catalog).
async fn submission_statuses(base: &str) -> (u16, u16) {
    let no_user = http(
        reqwest::Method::POST,
        &format!("{base}/v1/statement"),
        Some("SELECT 1"),
        &[],
    )
    .await
    .status;
    let schema_only = post(base, "SELECT 1", &[("X-Trino-Schema", "x")]).await.status;
    (no_user, schema_only)
}

// ---------------------------------------------------------------- client containers

/// Runs a client container on the default bridge to completion; returns
/// (exit code, stdout, stderr).
async fn run_client(
    tag: &str,
    image: GenericImage,
    cmd: Vec<String>,
    env: &[(&str, String)],
    files: &[(&str, &str)],
) -> (i64, String, String) {
    let mut request = image
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_cmd(cmd);
    for (name, value) in env {
        request = request.with_env_var(*name, value.clone());
    }
    for (path, content) in files {
        request = request.with_copy_to(
            CopyTargetOptions::new(*path).with_mode(0o644),
            content.as_bytes().to_vec(),
        );
    }
    let container = request
        .with_container_name(format!("codelake-f3v-acc-{tag}-{}", std::process::id()))
        .with_startup_timeout(Duration::from_secs(280))
        .start()
        .await
        .unwrap();
    let code = container.exit_code().await.unwrap().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&container.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap()).into_owned();
    (code, stdout, stderr)
}

async fn trino_cli(server: &Server, tag: &str, args: &[&str]) -> (i64, String, String) {
    let (host, port) = server.external();
    let mut cmd = vec![format!("--server=http://{host}:{port}"), "--user=cli".to_owned()];
    cmd.extend(args.iter().map(|a| (*a).to_owned()));
    run_client(
        tag,
        GenericImage::new("trinodb/trino", "431").with_entrypoint("trino"),
        cmd,
        &[],
        &[],
    )
    .await
}

async fn python(server: &Server, tag: &str, packages: &str, script: &str) -> (i64, String, String) {
    let (host, port) = server.external();
    let command = format!(
        "pip install --quiet --disable-pip-version-check {packages} >/tmp/pip.log 2>&1 || cat /tmp/pip.log; \
         python /w/script.py"
    );
    run_client(
        tag,
        GenericImage::new("python", "3.12-slim").with_entrypoint("sh"),
        vec!["-c".to_owned(), command],
        &[("HOST", host), ("PORT", port.to_string())],
        &[("/w/script.py", script)],
    )
    .await
}

const TRINO_PY: &str = "'trino>=0.328,<1'";

// ---------------------------------------------------------------- reference

/// Every check, passing and defect, holds on a real Trino 431 server — the
/// expectations of this suite are Trino's observed behavior.
#[tokio::test]
async fn trino_431_reference_confirms_every_check() {
    let (_trino, base) = trino_431().await;
    let mut disagreements = Vec::new();
    for check in passing_checks().into_iter().chain(defect_checks()) {
        let outcome = run_check(&base, &check, true).await;
        let expect = check.expect;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| expect(&outcome)));
        if result.is_err() {
            disagreements.push(format!("{}: {}", check.id, outcome.last));
        }
    }
    assert!(
        disagreements.is_empty(),
        "Trino 431 disagrees with: {disagreements:#?}"
    );
    assert_eq!(token_report(&base, TRINO_BIG).await, trino_tokens());
    assert_eq!(submission_statuses(&base).await, (401, 400));
    let info = get(&format!("{base}/v1/info")).await.json();
    assert_eq!(info["nodeVersion"]["version"], "431");
}

// ---------------------------------------------------------------- passing

/// Types, encodings, DML counts, session statements and common errors match
/// Trino 431.
#[tokio::test]
async fn codelake_passes_trino_431_checks() {
    let ids: Vec<&str> = passing_checks().iter().map(|c| c.id).collect();
    assert_codelake(&ids).await;
}

#[tokio::test]
async fn info_reports_trino_431() {
    let server = Server::start(&Limits::default()).await;
    let info = get(&format!("{}/v1/info", server.base())).await.json();
    assert_eq!(info["nodeVersion"]["version"], "431");
    assert_eq!(info["coordinator"], true);
    assert_eq!(info["starting"], false);
}

/// Pages, retries of the current token, stale tokens (410), unknown ids and
/// slugs (404) and `DELETE` of a running statement, as Trino 431; every row
/// arrives once across the pages.
#[tokio::test]
async fn paging_tokens_and_cancel_match_trino() {
    let server = Server::start(&Limits::default()).await;
    let report = token_report(&server.base(), CODELAKE_BIG).await;
    let expected = trino_tokens();
    assert_eq!(report.repeat_is_identical, expected.repeat_is_identical);
    assert_eq!(report.old_token_after_advance, expected.old_token_after_advance);
    assert_eq!(report.wrong_slug, expected.wrong_slug);
    assert_eq!(report.unknown_id, expected.unknown_id);
    assert_eq!(report.delete, expected.delete);
    assert_eq!(report.after_delete, expected.after_delete);

    let all = run(&server.base(), CODELAKE_BIG, &[]).await;
    all.assert_ok();
    assert!(all.data_pages > 1, "200k rows must span pages");
    assert_eq!(all.data.len(), 200_000);
    let sum: i64 = all.data.iter().map(|r| r[0].as_i64().unwrap()).sum();
    assert_eq!(sum, 200_000 * 200_001 / 2);
    let empty = http(
        reqwest::Method::POST,
        &format!("{}/v1/statement", server.base()),
        Some("  "),
        &with_user(&[]),
    )
    .await;
    assert_eq!(empty.status, 400);
}

const SLOW: &str = "SELECT count(*) FROM generate_series(1, 100000) a CROSS JOIN generate_series(1, 100000) b \
                    WHERE a.value + b.value = 7";

/// `DELETE /v1/query/{id}` by its owner cancels it (`USER_CANCELED`), and
/// `system.runtime.queries` shows the outcome of each query.
#[tokio::test]
async fn kill_by_id_and_runtime_queries() {
    let server = Server::start(&Limits::default()).await;
    let base = server.base();
    let first = post(&base, SLOW, &[("X-Trino-Source", "acc-src")]).await.json();
    let id = first["id"].as_str().unwrap().to_owned();
    let mut uri = first["nextUri"].as_str().unwrap().to_owned();
    let page = get(&uri).await.json();
    uri = page["nextUri"].as_str().unwrap_or(&uri).to_owned();
    let killed = http(
        reqwest::Method::DELETE,
        &format!("{base}/v1/query/{id}"),
        None,
        &with_user(&[]),
    )
    .await;
    assert_eq!(killed.status, 204);
    let mut last;
    loop {
        last = get(&uri).await.json();
        match last.get("nextUri").and_then(Value::as_str) {
            Some(next) => uri = next.to_owned(),
            None => break,
        }
    }
    assert_eq!(last["error"]["errorName"], "USER_CANCELED", "{last}");
    assert_eq!(last["error"]["errorCode"], 3);

    run(&base, "SELECT 42 AS answer", &[("X-Trino-Source", "acc-src")])
        .await
        .assert_ok();
    let rows = run(
        &base,
        "SELECT state, error_type, error_code, user, source, query FROM system.runtime.queries \
         WHERE source = 'acc-src' ORDER BY created",
        &[],
    )
    .await;
    rows.assert_ok();
    assert_eq!(
        rows.data,
        [
            json!(["FAILED", "USER_ERROR", "USER_CANCELED", "acc", "acc-src", SLOW]),
            json!(["FINISHED", null, null, "acc", "acc-src", "SELECT 42 AS answer"]),
        ]
    );
}

/// A client that stops polling for `client_timeout_secs` abandons its query:
/// it fails with `ABANDONED_QUERY`, as with Trino's `query.client.timeout`.
#[tokio::test]
async fn abandoned_query_is_canceled() {
    let server = Server::start(&Limits {
        client_timeout_secs: 2,
        ..Limits::default()
    })
    .await;
    let base = server.base();
    let first = post(&base, "SELECT value FROM generate_series(1, 5000000)", &[])
        .await
        .json();
    let id = first["id"].as_str().unwrap().to_owned();
    let page = get(first["nextUri"].as_str().unwrap()).await.json();
    // The sweeper runs every 5 s; the timeout is 2 s.
    tokio::time::sleep(Duration::from_secs(9)).await;
    let after = get(page["nextUri"].as_str().unwrap()).await.json();
    assert_eq!(after["error"]["errorName"], "ABANDONED_QUERY", "{after}");
    assert_eq!(after["error"]["errorCode"], 2);
    assert_eq!(after["error"]["errorType"], "USER_ERROR");
    let registry = run(
        &base,
        &format!("SELECT state, error_type, error_code FROM system.runtime.queries WHERE query_id = '{id}'"),
        &[],
    )
    .await;
    assert_eq!(
        registry.data,
        [json!(["FAILED", "USER_ERROR", "ABANDONED_QUERY"])]
    );
}

/// Beyond `max_active_queries` unfinished queries, submissions fail with
/// `QUERY_QUEUE_FULL`; a slot frees once a query ends.
#[tokio::test]
async fn queue_limit_rejects_with_query_queue_full() {
    let server = Server::start(&Limits {
        max_active_queries: 2,
        ..Limits::default()
    })
    .await;
    let base = server.base();
    let a = post(&base, SLOW, &[]).await.json();
    let _b = post(&base, SLOW, &[]).await.json();
    let full = post(&base, "SELECT 1", &[]).await.json();
    assert!(full.get("nextUri").is_none(), "{full}");
    assert_eq!(full["error"]["errorName"], "QUERY_QUEUE_FULL", "{full}");
    assert_eq!(full["error"]["errorCode"], 131_074);
    assert_eq!(full["error"]["errorType"], "INSUFFICIENT_RESOURCES");
    assert_eq!(full["stats"]["state"], "FAILED");
    let deleted = http(
        reqwest::Method::DELETE,
        &format!("{base}/v1/query/{}", a["id"].as_str().unwrap()),
        None,
        &with_user(&[]),
    )
    .await;
    assert_eq!(deleted.status, 204);
    let accepted = run(&base, "SELECT 7 AS x", &[]).await;
    accepted.assert_ok();
    assert_eq!(accepted.data, [json!([7])]);
}

/// The Trino 431 CLI in several output formats, with `USE` carried across
/// statements and a session limit enforced; outputs match the CLI against a
/// real Trino 431.
#[tokio::test]
async fn trino_cli_431_output_formats_and_session() {
    let server = Server::start(&Limits::default()).await;
    let select = "--execute=SELECT CAST(1 AS BIGINT) AS a, CAST('x' AS VARCHAR) AS b, \
                  CAST('12.50' AS DECIMAL(10,2)) AS d, CAST(NULL AS VARCHAR) AS n, DATE '1969-07-20' AS dt";
    let expected = [
        ("TSV_HEADER", "a\tb\td\tn\tdt\n1\tx\t12.50\t\t1969-07-20"),
        (
            "JSON",
            r#"{"a":1,"b":"x","d":"12.50","n":null,"dt":"1969-07-20"}"#,
        ),
        (
            "CSV_HEADER",
            "\"a\",\"b\",\"d\",\"n\",\"dt\"\n\"1\",\"x\",\"12.50\",\"\",\"1969-07-20\"",
        ),
    ];
    for (format, output) in expected {
        let (code, out, err) =
            trino_cli(&server, "cli", &[&format!("--output-format={format}"), select]).await;
        assert_eq!(code, 0, "{format}: {out}\n{err}");
        assert_eq!(out.trim(), output, "{format}");
    }

    let (code, out, err) = trino_cli(
        &server,
        "cli",
        &[
            "--output-format=CSV",
            "--execute=CREATE SCHEMA datafusion.cli; USE datafusion.cli; \
             CREATE TABLE t (a bigint); INSERT INTO t VALUES (1), (2), (3); SELECT sum(a) FROM t",
        ],
    )
    .await;
    assert_eq!(code, 0, "{out}\n{err}");
    assert_eq!(out.lines().last().unwrap().trim(), "\"6\"", "{out}");

    let (code, out, err) = trino_cli(
        &server,
        "cli",
        &["--session=query_max_run_time=1s", &format!("--execute={SLOW}")],
    )
    .await;
    assert_ne!(code, 0, "{out}");
    assert!(
        err.contains("Query exceeded maximum time limit of 1.00s"),
        "{err}"
    );
}

const PYTHON_CLIENT: &str = r#"
import os, time, datetime, decimal, math, concurrent.futures as cf
import trino
from trino.exceptions import TrinoUserError

def connect():
    return trino.dbapi.connect(host=os.environ["HOST"], port=int(os.environ["PORT"]),
                               user="py", catalog="datafusion", schema="public")

cur = connect().cursor()
cur.execute("SELECT CAST('12.50' AS DECIMAL(10,2)), DATE '1969-07-20', "
            "CAST(TIMESTAMP '1960-01-02 03:04:05.123456' AS TIMESTAMP(6)), X'002A', "
            "CAST('NaN' AS DOUBLE), CAST(NULL AS BIGINT), 'ação', true")
row = cur.fetchone()
assert row[0] == decimal.Decimal("12.50"), row
assert row[1] == datetime.date(1969, 7, 20), row
assert row[2] == datetime.datetime(1960, 1, 2, 3, 4, 5, 123456), row
assert row[3] == b"\x00*", row
assert math.isnan(row[4]), row
assert row[5] is None and row[6] == "ação" and row[7] is True, row

cur.execute("CREATE TABLE py_t (a bigint)")
cur.fetchall()
cur.execute("INSERT INTO py_t VALUES (1), (2), (3)")
cur.fetchall()
assert cur.rowcount == 3, cur.rowcount

try:
    cur.execute("SELECT * FROM py_missing")
    cur.fetchall()
    raise AssertionError("TABLE_NOT_FOUND expected")
except TrinoUserError as e:
    assert e.error_name == "TABLE_NOT_FOUND", e.error_name

# Volume: 2 million rows through the client, checked by count, sum and text size.
started = time.time()
cur.execute("SELECT value, CAST(value AS VARCHAR) AS text FROM generate_series(1, 2000000)")
count = total = text = 0
while True:
    rows = cur.fetchmany(50000)
    if not rows:
        break
    count += len(rows)
    total += sum(r[0] for r in rows)
    text += sum(len(r[1]) for r in rows)
assert count == 2000000, count
assert total == 2000000 * 2000001 // 2, total
assert text == sum(len(str(i)) for i in range(1, 2000001)), text
print("volume", round(time.time() - started, 1), "s")

# 20 concurrent queries, each on its own connection, each with its own answer.
def one(i):
    c = connect().cursor()
    n = 200000 + i
    c.execute(f"SELECT count(*), sum(value) FROM generate_series(1, {n}) WHERE value % 3 <> {i % 3}")
    expected = [v for v in range(1, n + 1) if v % 3 != i % 3]
    return c.fetchall() == [[len(expected), sum(expected)]]

with cf.ThreadPoolExecutor(20) as pool:
    results = list(pool.map(one, range(20)))
assert all(results), results
print("PYTHON OK")
"#;

/// The Python DB-API client decodes Trino types, reports DML counts and error
/// classes, reads 2 million rows and runs 20 queries at once.
#[tokio::test]
async fn python_client_types_volume_and_concurrency() {
    let server = Server::start(&Limits::default()).await;
    let (code, out, err) = python(&server, "py", TRINO_PY, PYTHON_CLIENT).await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert!(out.contains("PYTHON OK"), "{out}");
}

const JDBC_PROBE: &str = r#"
import java.sql.*;

public class Probe {
    public static void main(String[] args) throws Exception {
        try (Connection c = DriverManager.getConnection(args[0], "jdbc", null)) {
            Statement s = c.createStatement();
            if (args[1].equals("basic")) {
                try (ResultSet rs = s.executeQuery("SELECT CAST(7 AS BIGINT) AS b, CAST('12.50' AS DECIMAL(10,2)) AS d, CAST('ação' AS VARCHAR) AS v, DATE '1969-07-20' AS dt, CAST(NULL AS INTEGER) AS n, true AS t")) {
                    ResultSetMetaData md = rs.getMetaData();
                    StringBuilder sb = new StringBuilder("meta:");
                    for (int i = 1; i <= md.getColumnCount(); i++) sb.append(' ').append(md.getColumnLabel(i)).append('=').append(md.getColumnTypeName(i));
                    System.out.println(sb);
                    rs.next();
                    System.out.println("row: " + rs.getLong(1) + " " + rs.getBigDecimal(2) + " " + rs.getString(3) + " " + rs.getDate(4) + " " + rs.getObject(5) + " " + rs.getBoolean(6));
                }
                long sum = 0, count = 0;
                try (ResultSet rs = s.executeQuery("SELECT value FROM generate_series(1, 300000)")) {
                    while (rs.next()) { count++; sum += rs.getLong(1); }
                }
                System.out.println("big: " + count + " " + sum);
                System.out.println("ddl: " + s.executeUpdate("CREATE TABLE jdbc_t (a bigint, b varchar)"));
                System.out.println("insert: " + s.executeUpdate("INSERT INTO jdbc_t VALUES (1, 'x'), (2, 'y')"));
                try { s.executeQuery("SELECT * FROM jdbc_missing"); }
                catch (SQLException e) { System.out.println("error: " + e.getErrorCode()); }
            } else {
                s.executeUpdate("CREATE TABLE jdbc_p (a bigint, b varchar)");
                s.executeUpdate("INSERT INTO jdbc_p VALUES (1, 'x'), (2, 'y')");
                try (PreparedStatement ps = c.prepareStatement("SELECT a, b FROM jdbc_p WHERE a = ? AND b <> ?")) {
                    ps.setLong(1, 1);
                    ps.setString(2, "x'y");
                    try (ResultSet rs = ps.executeQuery()) { rs.next(); System.out.println("prepared: " + rs.getLong(1) + " " + rs.getString(2)); }
                } catch (SQLException e) { System.out.println("prepared ERROR: " + e.getMessage()); }
                DatabaseMetaData dm = c.getMetaData();
                System.out.println("version: " + dm.getDatabaseProductVersion());
                try (ResultSet rs = dm.getCatalogs()) {
                    StringBuilder sb = new StringBuilder("catalogs:");
                    while (rs.next()) sb.append(' ').append(rs.getString(1));
                    System.out.println(sb);
                } catch (SQLException e) { System.out.println("catalogs ERROR: " + e.getMessage()); }
                try (ResultSet rs = dm.getTables(null, null, "jdbc_p", null)) {
                    StringBuilder sb = new StringBuilder("tables:");
                    while (rs.next()) sb.append(' ').append(rs.getString(1)).append('.').append(rs.getString(2)).append('.').append(rs.getString(3));
                    System.out.println(sb);
                } catch (SQLException e) { System.out.println("tables ERROR: " + e.getMessage()); }
                try (ResultSet rs = dm.getColumns(null, null, "jdbc_p", null)) {
                    StringBuilder sb = new StringBuilder("columns:");
                    while (rs.next()) sb.append(' ').append(rs.getString(4)).append('=').append(rs.getString(6));
                    System.out.println(sb);
                } catch (SQLException e) { System.out.println("columns ERROR: " + e.getMessage()); }
            }
            System.out.println("JDBC DONE");
        }
    }
}
"#;

/// Runs `Probe.java` with the Trino JDBC driver 431 (Maven Central) inside the
/// Trino 431 image, which ships a JDK.
async fn jdbc(server: &Server, mode: &str) -> (i64, String, String) {
    let (host, port) = server.external();
    let script = format!(
        "set -e; cd /tmp; \
         curl -fsSL -o trino-jdbc.jar https://repo1.maven.org/maven2/io/trino/trino-jdbc/431/trino-jdbc-431.jar; \
         cp /w/Probe.java . && javac Probe.java; \
         java -cp trino-jdbc.jar:. Probe 'jdbc:trino://{host}:{port}/datafusion/public' {mode}"
    );
    run_client(
        "jdbc",
        GenericImage::new("trinodb/trino", "431").with_entrypoint("sh"),
        vec!["-c".to_owned(), script],
        &[],
        &[("/w/Probe.java", JDBC_PROBE)],
    )
    .await
}

/// The JDBC driver 431: metadata, values, 300k rows, DDL/DML counts and error
/// codes (as against Trino 431: `ddl: 0`, `insert: 2`, `error: 46`).
#[tokio::test]
async fn jdbc_431_driver_queries_ddl_and_errors() {
    let server = Server::start(&Limits::default()).await;
    let (code, out, err) = jdbc(&server, "basic").await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    let lines: Vec<&str> = out.lines().collect();
    let expected = [
        "meta: b=bigint d=decimal(10,2) v=varchar dt=date n=integer t=boolean",
        "row: 7 12.50 ação 1969-07-20 null true",
        "big: 300000 45000150000",
        "ddl: 0",
        "insert: 2",
        "error: 46",
        "JDBC DONE",
    ];
    assert_eq!(lines, expected, "{out}");
}

// ---------------------------------------------------------------- defects

#[tokio::test]
async fn defect_01_null_literal_is_json_null() {
    assert_codelake(&["d_null_literal"]).await;
}

#[tokio::test]
async fn defect_02_real_values_encode_like_trino() {
    assert_codelake(&["d_real"]).await;
}

#[tokio::test]
async fn defect_03_map_and_row_are_typed() {
    assert_codelake(&["d_map", "d_row"]).await;
}

#[tokio::test]
async fn defect_04_timestamp_with_time_zone_keeps_zone() {
    assert_codelake(&["d_timestamp_tz_keeps_zone"]).await;
}

#[tokio::test]
#[ignore = "DEFECT-5: TIMESTAMP '... <zone>' literal becomes timestamp(9) without zone, shifted to UTC"]
async fn defect_05_timestamp_literal_with_zone() {
    assert_codelake(&["d_timestamp_tz_literal"]).await;
}

#[tokio::test]
async fn defect_06_ddl_has_no_result_rows() {
    assert_codelake(&[
        "d_ddl_create_schema",
        "d_ddl_create_table",
        "d_ddl_create_view",
        "d_ddl_drop_table",
    ])
    .await;
}

#[tokio::test]
async fn defect_07_ctas_reports_row_count() {
    assert_codelake(&["d_ctas_count"]).await;
}

#[tokio::test]
async fn defect_08_delete_update_report_row_count() {
    assert_codelake(&["d_delete_count", "d_update_count"]).await;
}

#[tokio::test]
async fn defect_09_ddl_user_errors_are_user_errors() {
    assert_codelake(&[
        "d_error_table_exists",
        "d_error_schema_exists",
        "d_error_drop_missing",
    ])
    .await;
}

#[tokio::test]
async fn defect_10_error_names_match_trino() {
    assert_codelake(&[
        "d_error_function",
        "d_error_catalog",
        "d_error_schema",
        "d_error_type_mismatch",
        "d_error_ambiguous",
    ])
    .await;
}

#[tokio::test]
async fn defect_11_error_location_like_trino() {
    assert_codelake(&["d_error_location_eof", "d_error_location_semantic"]).await;
}

#[tokio::test]
async fn defect_12_describe_runtime_queries_matches_trino() {
    assert_codelake(&["d_describe_runtime_queries"]).await;
}

#[tokio::test]
async fn defect_13_show_session_like_and_defaults() {
    assert_codelake(&["d_show_session_like", "d_show_session_default_format"]).await;
}

#[tokio::test]
async fn defect_14_unnamed_columns_are_col_n() {
    assert_codelake(&["d_unnamed_column", "d_unaliased_expressions"]).await;
}

#[tokio::test]
async fn defect_15_decimal_type_text() {
    assert_codelake(&["d_decimal_type_text"]).await;
}

#[tokio::test]
#[ignore = "DEFECT-16: char(n), varchar(n), json and uuid types are missing"]
async fn defect_16_char_varchar_json_uuid_types() {
    assert_codelake(&["d_char", "d_varchar_n", "d_json", "d_uuid"]).await;
}

#[tokio::test]
async fn defect_17_legacy_datetime_without_capability() {
    assert_codelake(&["d_legacy_datetime"]).await;
}

#[tokio::test]
#[ignore = "DEFECT-18: ALTER TABLE RENAME TO is not supported (dbt table materialization)"]
async fn defect_18_rename_table() {
    assert_codelake(&["d_rename_table"]).await;
}

#[tokio::test]
#[ignore = "DEFECT-19: CREATE VIEW ... SECURITY DEFINER is a syntax error (dbt views)"]
async fn defect_19_view_security_clause() {
    assert_codelake(&["d_view_security_definer"]).await;
}

#[tokio::test]
#[ignore = "DEFECT-20: system.metadata.materialized_views is missing (dbt relation listing)"]
async fn defect_20_metadata_materialized_views() {
    assert_codelake(&["d_metadata_materialized_views"]).await;
}

#[tokio::test]
#[ignore = "DEFECT-21: <catalog>.information_schema lists other catalogs' schemas and tables"]
async fn defect_21_information_schema_is_catalog_scoped() {
    assert_codelake(&[
        "d_information_schema_tables_scope",
        "d_information_schema_schemata_scope",
    ])
    .await;
}

#[tokio::test]
#[ignore = "DEFECT-22: scalar subquery returning 2 rows yields 2 rows instead of SUBQUERY_MULTIPLE_ROWS"]
async fn defect_22_scalar_subquery_multiple_rows() {
    assert_codelake(&["d_scalar_subquery_rows"]).await;
}

#[tokio::test]
async fn defect_23_http_statuses_match_trino() {
    let server = Server::start(&Limits::default()).await;
    let report = token_report(&server.base(), CODELAKE_BIG).await;
    assert_eq!(report.future_token, trino_tokens().future_token);
    assert_eq!(submission_statuses(&server.base()).await, (401, 400));
}

/// Trino stops a canceled query; `CodeLake` reports `USER_CANCELED` but the
/// query keeps a core busy (observed for minutes, until the process restarts).
#[tokio::test]
async fn defect_24_cancel_stops_execution() {
    let server = Server::start(&Limits::default()).await;
    let base = server.base();
    let heavy = "SELECT count(*) FROM generate_series(1, 100000) a CROSS JOIN generate_series(1, 100000) b \
                 CROSS JOIN generate_series(1, 1000) c";
    let first = post(&base, heavy, &[]).await.json();
    let mut uri = first["nextUri"].as_str().unwrap().to_owned();
    for _ in 0..3 {
        let page = get(&uri).await.json();
        uri = page["nextUri"].as_str().unwrap_or(&uri).to_owned();
    }
    let canceled = http(reqwest::Method::DELETE, &uri, None, &with_user(&[])).await;
    assert_eq!(canceled.status, 204);
    let after = get(&uri).await.json();
    assert_eq!(after["error"]["errorName"], "USER_CANCELED", "{after}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let before = server.cpu_seconds();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let busy = server.cpu_seconds() - before;
    assert!(busy < 1.0, "server used {busy:.1} CPU s in 5 s after the cancel");
}

const PYTHON_PARAMS: &str = r#"
import os, datetime, decimal
import trino, sqlalchemy as sa

host, port = os.environ["HOST"], int(os.environ["PORT"])
failures = []
conn = trino.dbapi.connect(host=host, port=port, user="params", catalog="datafusion", schema="public")
cur = conn.cursor()
try:
    cur.execute("SELECT ?, ?, ?, ?", (1, "a'b", decimal.Decimal("12.34"), datetime.date(1969, 7, 20)))
    row = cur.fetchall()
    if row != [[1, "a'b", decimal.Decimal("12.34"), datetime.date(1969, 7, 20)]]:
        failures.append(f"dbapi params row {row}")
except Exception as e:
    failures.append(f"dbapi params: {e}")

engine = sa.create_engine(f"trino://sa@{host}:{port}/datafusion")
with engine.connect() as c:
    c.execute(sa.text("CREATE TABLE sa_t (a bigint)"))
    try:
        names = sa.inspect(c).get_table_names(schema="public")
        if "sa_t" not in names:
            failures.append(f"get_table_names {names}")
    except Exception as e:
        failures.append(f"get_table_names: {str(e)[:200]}")
    try:
        got = c.execute(sa.text("SELECT :x + 1 AS y"), {"x": 41}).fetchall()
        if [tuple(r) for r in got] != [(42,)]:
            failures.append(f"bound {got}")
    except Exception as e:
        failures.append(f"bound: {str(e)[:200]}")
print("FAILURES", failures)
assert not failures
print("PARAMS OK")
"#;

/// Bound parameters: the Python client sends `EXECUTE IMMEDIATE ... USING`
/// (falling back to `PREPARE` + `X-Trino-Prepared-Statement`); `SQLAlchemy`'s
/// inspector binds its filters the same way. All pass against Trino 431.
#[tokio::test]
async fn defect_25_python_and_sqlalchemy_bound_parameters() {
    let server = Server::start(&Limits::default()).await;
    let (code, out, err) = python(&server, "params", "'trino[sqlalchemy]>=0.328,<1'", PYTHON_PARAMS).await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert!(out.contains("PARAMS OK"), "{out}");
}

/// Against Trino 431: `prepared: 1 x`, `version: 431`, the catalogs, and the
/// table and its columns from `DatabaseMetaData` (served by `system.jdbc`).
#[tokio::test]
#[ignore = "DEFECT-26: JDBC PreparedStatement fails; system.jdbc missing (getCatalogs/getTables); version is DataFusion's"]
async fn defect_26_jdbc_prepared_statement_and_metadata() {
    let server = Server::start(&Limits::default()).await;
    let (code, out, err) = jdbc(&server, "meta").await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(
        lines,
        [
            "prepared: 1 x",
            "version: 431",
            "catalogs: datafusion system",
            "tables: datafusion.public.jdbc_p",
            "columns: a=bigint b=varchar",
            "JDBC DONE",
        ],
        "{out}"
    );
}

const DBT_PROJECT: &str =
    "name: acc\nversion: \"1.0\"\nconfig-version: 2\nprofile: acc\nmodel-paths: [\"models\"]\n";
const DBT_PROFILES: &str = "acc:\n  target: t\n  outputs:\n    t:\n      type: trino\n      method: none\n      \
                            user: dbt\n      host: \"{{ env_var('HOST') }}\"\n      port: \"{{ env_var('PORT') | int }}\"\n      \
                            database: datafusion\n      schema: dbt_acc\n      http_scheme: http\n      threads: 1\n";
const DBT_VIEW: &str = "{{ config(materialized='view') }}\nselect 1 as id, 'a' as name\nunion all select 2, 'b'\nunion all select 3, 'c'\n";
const DBT_TABLE: &str = "{{ config(materialized='table') }}\nselect count(*) as n, sum(id) as total from {{ ref('base_view') }}\n";
const DBT_INCREMENTAL: &str = "{{ config(materialized='incremental', incremental_strategy='append') }}\n\
                               select id, name from {{ ref('base_view') }}\n\
                               {% if is_incremental() %}\nwhere id > (select coalesce(max(id), 0) - 1 from {{ this }})\n{% endif %}\n";
const DBT_CHECK: &str = r#"
import os, trino
c = trino.dbapi.connect(host=os.environ["HOST"], port=int(os.environ["PORT"]), user="check").cursor()
c.execute("SELECT n, total FROM datafusion.dbt_acc.agg_table")
agg = c.fetchall()
c.execute("SELECT count(*), sum(id) FROM datafusion.dbt_acc.events_inc")
inc = c.fetchall()
print("agg", agg, "inc", inc)
assert agg == [[3, 6]] and inc == [[4, 9]]
print("DBT OK")
"#;

/// dbt-trino 1.10 (real `pip install`) runs a view, a table and an append
/// incremental model twice; against Trino 431 (memory catalog) both runs pass
/// with `agg_table = (3, 6)` and `events_inc` = 4 rows summing 9.
#[tokio::test]
#[ignore = "DEFECT-27: dbt-trino cannot run a view/table/incremental project (see DEFECT-18/19/20)"]
async fn defect_27_dbt_trino_runs_view_table_incremental() {
    let server = Server::start(&Limits::default()).await;
    let (host, port) = server.external();
    let script = "pip install --quiet --disable-pip-version-check 'dbt-trino~=1.10.0' >/tmp/pip.log 2>&1 || cat /tmp/pip.log; \
                  cd /w && dbt run --profiles-dir . --no-use-colors && dbt run --profiles-dir . --no-use-colors \
                  && python /w/check.py";
    let (code, out, err) = run_client(
        "dbt",
        GenericImage::new("python", "3.12-slim").with_entrypoint("sh"),
        vec!["-c".to_owned(), script.to_owned()],
        &[("HOST", host), ("PORT", port.to_string())],
        &[
            ("/w/dbt_project.yml", DBT_PROJECT),
            ("/w/profiles.yml", DBT_PROFILES),
            ("/w/models/base_view.sql", DBT_VIEW),
            ("/w/models/agg_table.sql", DBT_TABLE),
            ("/w/models/events_inc.sql", DBT_INCREMENTAL),
            ("/w/check.py", DBT_CHECK),
        ],
    )
    .await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert!(out.contains("DBT OK"), "{out}");
}
