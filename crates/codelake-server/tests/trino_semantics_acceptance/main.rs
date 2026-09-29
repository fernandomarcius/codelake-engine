//! Independent acceptance suite for Trino semantics in the engine: `CodeLake`
//! and a real Trino 431 answer the **same** SQL side by side, and every
//! answer is compared (column types, names and rows, or the error name).
//! Nothing here is a golden written by hand: the expectation is whatever
//! Trino 431 answers at run time.
//!
//! Themes:
//! - decimal arithmetic and aggregation: a matrix of precisions and scales,
//!   through constant folding (literals) and through columns, with the
//!   result type and value of `+ - * / %`, `sum`, `avg`, windows, overflow;
//! - casts from text with time zones: several zones, the historical DST of
//!   `America/Sao_Paulo` (gap and overlap), instants before 1970 and LMT;
//! - `information_schema` (`tables`, `columns` with `ordinal_position`,
//!   `data_type` and `is_nullable`, `schemata`, `views`) in the in-memory
//!   catalog (`memory` in Trino, `datafusion` in `CodeLake`) and in `hive`
//!   (Trino's file metastore on both sides, separate warehouses);
//! - `SHOW CREATE TABLE` and `SHOW CREATE VIEW` in both catalogs;
//! - TPC-H Q01, Q08, Q14 and Q17 on SF 0.01 (`tpch.tiny`), the same rows
//!   loaded into both engines.
//!
//! Tests that expose a defect are `#[ignore = "DEFECT-SEM<n>: ..."]` with the
//! expected (Trino) and obtained (`CodeLake`) answers; they assert Trino's
//! behaviour, so `--run-ignored all` shows each one failing.
//!
//! The Trino container is `codelake-sem-acc-trino-<pid>-<n>`.

#![cfg(unix)]

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};

const BIN: &str = env!("CARGO_BIN_EXE_codelake");

type Res<T> = Result<T, String>;

fn text<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}

// ---------------------------------------------------------------- servers

fn free_port() -> Res<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(text)?;
    Ok(listener.local_addr().map_err(text)?.port())
}

struct CodeLake {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for CodeLake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl CodeLake {
    /// The `codelake` binary with a `hive` catalog on a fresh local warehouse.
    async fn start() -> Res<Self> {
        let dir = tempfile::tempdir().map_err(text)?;
        let warehouse = dir.path().join("warehouse");
        std::fs::create_dir_all(&warehouse).map_err(text)?;
        let (pg, http, port) = (free_port()?, free_port()?, free_port()?);
        let path = dir.path().join("codelake.toml");
        let mut file = std::fs::File::create(&path).map_err(text)?;
        write!(
            file,
            r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = 1

[server.trino]
listen = "127.0.0.1:{port}"
header_prefix = "X-Trino-"
reported_version = "431"

[memory]
limit = "1GiB"
spill_dir = "{spill}"
spill_limit = "1GiB"
metadata_cache_limit = "8MiB"

[[source]]
name = "hive"
kind = "hive"
warehouse = "{warehouse}"

[source.metastore_names]
metadata_file = ".trinoSchema"
permissions_dir = ".trinoPermissions"
view_marker = "Presto View"
view_parameter = "presto_view"
version_parameter = "presto_version"
writer_version = "431"
"#,
            spill = dir.path().join("spill").display(),
            warehouse = warehouse.display(),
        )
        .map_err(text)?;
        let child = Command::new(BIN)
            .args(["serve", "--config"])
            .arg(&path)
            .stdout(Stdio::null())
            .spawn()
            .map_err(text)?;
        let server = Self {
            child,
            port,
            _dir: dir,
        };
        wait_info(&server.base()).await?;
        Ok(server)
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

async fn wait_info(base: &str) -> Res<()> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(response) = reqwest::get(format!("{base}/v1/info")).await
            && response.status() == 200
            && let Ok(body) = response.text().await
            && body.contains("\"starting\":false")
        {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("{base} did not start"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Counts the workers the reference server's coordinator lists as active: it
/// logs "SERVER STARTED" before its own worker registers, and a query sent in
/// between fails with "No nodes available to run query".
const ACTIVE_WORKERS: &str = "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'";

static TRINO_SEQ: AtomicUsize = AtomicUsize::new(0);

/// A real Trino 431 with its stock catalogs (`memory`, `tpch`) plus `hive` on
/// the file metastore, the layout the `CodeLake` hive catalog reads (ADR 0007).
async fn trino_431() -> Res<(ContainerAsync<GenericImage>, String)> {
    const JVM: &str = "-server\n-Xmx1536M\n-XX:+UseG1GC\n-XX:G1HeapRegionSize=32M\n\
        -XX:+ExplicitGCInvokesConcurrent\n-XX:+ExitOnOutOfMemoryError\n\
        -XX:-OmitStackTraceInFastThrow\n-XX:ReservedCodeCacheSize=150M\n\
        -Djdk.attach.allowAttachSelf=true\n-Djdk.nio.maxCachedBufferSize=2000000\n";
    const HIVE: &str = "connector.name=hive\nhive.metastore=file\n\
        hive.metastore.catalog.dir=file:/tmp/hive-wh\n";
    let name = format!(
        "codelake-sem-acc-trino-{}-{}",
        std::process::id(),
        TRINO_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let container = GenericImage::new("trinodb/trino", "431")
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_wait_for(WaitFor::message_on_either_std("SERVER STARTED"))
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/jvm.config").with_mode(0o644),
            JVM.as_bytes().to_vec(),
        )
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/catalog/hive.properties").with_mode(0o644),
            HIVE.as_bytes().to_vec(),
        )
        .with_container_name(name)
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .map_err(text)?;
    let host = container.get_host().await.map_err(text)?;
    let port = container.get_host_port_ipv4(8080).await.map_err(text)?;
    let base = format!("http://{host}:{port}");
    wait_info(&base).await?;
    let deadline = Instant::now() + Duration::from_secs(90);
    while run(&base, "system", ACTIVE_WORKERS, None).await?["rows"][0][0]
        .as_u64()
        .is_none_or(|workers| workers == 0)
    {
        if Instant::now() > deadline {
            return Err("Trino never listed an active worker".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // The memory connector answers NO_NODES_AVAILABLE until the node
    // registers itself; wait until it can scan a table.
    run(
        &base,
        "memory",
        "CREATE TABLE memory.default.warmup AS SELECT 1 AS a",
        None,
    )
    .await?;
    let deadline = Instant::now() + Duration::from_secs(90);
    while run(&base, "memory", "SELECT a FROM memory.default.warmup", None)
        .await?
        .get("error")
        .is_some()
    {
        if Instant::now() > deadline {
            return Err("Trino memory connector never became ready".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Ok((container, base))
}

// ---------------------------------------------------------------- protocol

async fn fetch(request: reqwest::RequestBuilder) -> Res<Value> {
    let response = request.send().await.map_err(text)?;
    let status = response.status();
    let body = response.text().await.map_err(text)?;
    if !status.is_success() {
        return Err(format!("{status}: {body}"));
    }
    serde_json::from_str(&body).map_err(|e| format!("not JSON ({e}): {body}"))
}

/// A statement followed to its end: `{"types", "names", "rows"}` or
/// `{"error", "message"}`. `zone` is the session time zone.
async fn run(base: &str, catalog: &str, sql: &str, zone: Option<&str>) -> Res<Value> {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "acceptance")
        .header("X-Trino-Catalog", catalog)
        .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
        .timeout(Duration::from_secs(240))
        .body(sql.to_owned());
    if let Some(zone) = zone {
        request = request.header("X-Trino-Time-Zone", zone);
    }
    let mut page = fetch(request).await?;
    let mut columns: Option<Vec<Value>> = None;
    let mut rows: Vec<Value> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(280);
    loop {
        if columns.is_none() {
            columns = page.get("columns").and_then(Value::as_array).cloned();
        }
        if let Some(data) = page.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().cloned());
        }
        if let Some(error) = page.get("error") {
            return Ok(json!({"error": error["errorName"], "message": error["message"]}));
        }
        let Some(next) = page.get("nextUri").and_then(Value::as_str).map(str::to_owned) else {
            break;
        };
        if Instant::now() > deadline {
            return Err(format!("{sql} did not finish"));
        }
        page = fetch(client.get(next).header("X-Trino-User", "acceptance")).await?;
    }
    let columns = columns.unwrap_or_default();
    Ok(json!({
        "types": columns.iter().map(|c| c["type"].clone()).collect::<Vec<_>>(),
        "names": columns.iter().map(|c| c["name"].clone()).collect::<Vec<_>>(),
        "rows": rows,
    }))
}

/// Same answer: both failed with the same error name (messages differ by the
/// `Arrow error:` prefix, a protocol matter), or identical types, names and
/// rows.
fn equivalent(trino: &Value, codelake: &Value) -> bool {
    match (trino.get("error"), codelake.get("error")) {
        (Some(a), Some(b)) => a == b,
        (None, None) => trino == codelake,
        _ => false,
    }
}

/// Trino 431 and `CodeLake` side by side. `{c}` in SQL is the in-memory
/// catalog: `memory` in Trino, `datafusion` in `CodeLake`.
struct Pair {
    _container: ContainerAsync<GenericImage>,
    trino: String,
    codelake: CodeLake,
}

impl Pair {
    async fn start() -> Res<Self> {
        let (container, trino) = trino_431().await?;
        let codelake = CodeLake::start().await?;
        Ok(Self {
            _container: container,
            trino,
            codelake,
        })
    }

    async fn both(&self, template: &str, zone: Option<&str>) -> Res<(Value, Value)> {
        let trino = run(&self.trino, "memory", &template.replace("{c}", "memory"), zone).await?;
        let codelake = run(
            &self.codelake.base(),
            "datafusion",
            &template.replace("{c}", "datafusion"),
            zone,
        )
        .await?;
        Ok((trino, codelake))
    }

    /// Statements both engines must accept.
    async fn setup(&self, statements: &[&str]) -> Res<()> {
        for statement in statements {
            let (trino, codelake) = self.both(statement, None).await?;
            if trino.get("error").is_some() || codelake.get("error").is_some() {
                return Err(format!(
                    "setup `{statement}`\n  trino    {trino}\n  codelake {codelake}"
                ));
            }
        }
        Ok(())
    }

    /// `None` when both engines answer the same; otherwise both answers.
    async fn mismatch(&self, template: &str, zone: Option<&str>) -> Res<Option<String>> {
        let (trino, codelake) = self.both(template, zone).await?;
        Ok((!equivalent(&trino, &codelake))
            .then(|| format!("{template}\n  trino    {trino}\n  codelake {codelake}")))
    }

    /// Every query, with the session zone, must answer the same.
    async fn mismatches(&self, queries: &[&str], zone: Option<&str>) -> Res<Vec<String>> {
        let mut wrong = Vec::new();
        for query in queries {
            if let Some(m) = self.mismatch(query, zone).await? {
                wrong.push(m);
            }
        }
        Ok(wrong)
    }

    /// Several expressions over `from` in one query; on any difference each
    /// expression is compared alone, so one error does not hide the others.
    async fn batch(&self, exprs: &[String], from: &str) -> Res<Vec<String>> {
        let all = format!("SELECT {} {from}", exprs.join(", "));
        if self.mismatch(&all, None).await?.is_none() {
            let (trino, _) = self.both(&all, None).await?;
            if trino.get("error").is_none() {
                return Ok(Vec::new());
            }
        }
        let mut wrong = Vec::new();
        for expr in exprs {
            if let Some(m) = self.mismatch(&format!("SELECT {expr} {from}"), None).await? {
                wrong.push(m);
            }
        }
        Ok(wrong)
    }
}

fn verdict(wrong: &[String]) -> Res<()> {
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} answer(s) differ from Trino 431:\n{}",
            wrong.len(),
            wrong.join("\n")
        ))
    }
}

/// Starts both engines, runs `setup` on both, then compares every query.
async fn expect_same(setup: &[&str], queries: &[&str], zone: Option<&str>) -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(setup).await?;
    verdict(&pair.mismatches(queries, zone).await?)
}

mod decimal;
mod information_schema;
mod show_create;
mod timestamps;
mod tpch;
