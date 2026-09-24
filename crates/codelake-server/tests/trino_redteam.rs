//! Red-team probes of the Trino protocol (front F3) against the real
//! `codelake` binary with a Hive warehouse: cancellation of writes, SIGTERM
//! drain and real-client impact. `DEFECT-RT<n>` tests assert Trino 431's
//! behavior and fail today; `rt_pass_*` are attacks that did not break
//! anything. All are `#[ignore]`d: run with `--run-ignored all` in the
//! integration profile.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test crate"
)]

use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use testcontainers::core::WaitFor;
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const BIN: &str = env!("CARGO_BIN_EXE_codelake");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

fn start(dir: &Path, grace_secs: u64) -> (Server, u16, PathBuf) {
    let (pg, http, trino) = (free_port(), free_port(), free_port());
    let warehouse = dir.join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let path = dir.join("codelake.toml");
    let mut file = std::fs::File::create(&path).unwrap();
    write!(
        file,
        r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = {grace_secs}

[server.trino]
listen = "127.0.0.1:{trino}"

[memory]
limit = "512MiB"
spill_dir = "{spill}"
spill_limit = "512MiB"
metadata_cache_limit = "8MiB"

[[source]]
name = "hive"
kind = "hive"
warehouse = "{warehouse}"
"#,
        spill = dir.join("spill").display(),
        warehouse = warehouse.display(),
    )
    .unwrap();
    let child = Command::new(BIN)
        .args(["serve", "--config"])
        .arg(&path)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    (Server(child), trino, warehouse)
}

async fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let url = format!("http://127.0.0.1:{port}/v1/info");
    while reqwest::get(&url).await.is_err() {
        assert!(Instant::now() < deadline, "Trino listener did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct Client {
    http: reqwest::Client,
    base: String,
}

impl Client {
    fn new(port: u16) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: format!("http://127.0.0.1:{port}"),
        }
    }

    async fn post(&self, sql: &str) -> (u16, Value) {
        let r = self
            .http
            .post(format!("{}/v1/statement", self.base))
            .header("x-trino-user", "rt")
            .body(sql.to_owned())
            .send()
            .await
            .unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn get(&self, uri: &str) -> (u16, Value) {
        let r = self.http.get(uri).send().await.unwrap();
        let status = r.status().as_u16();
        let text = r.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn delete(&self, uri: &str) -> u16 {
        self.http.delete(uri).send().await.unwrap().status().as_u16()
    }

    /// Runs to the end; returns the last page.
    async fn run(&self, sql: &str) -> Value {
        let (status, mut page) = self.post(sql).await;
        assert_eq!(status, 200, "{sql}: {page}");
        while let Some(next) = page["nextUri"].as_str().map(str::to_owned) {
            page = self.get(&next).await.1;
        }
        page
    }
}

fn files_under(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_owned()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p.strip_prefix(dir).unwrap().display().to_string());
            }
        }
    }
    out.sort();
    out
}

/// Cancelling a Hive `CREATE TABLE AS` (or `INSERT`) drops the write future
/// mid-file. `HiveSink::write_data` only deletes its `.<name>.inprogress` file
/// on an error, but on local disk `object_store`'s `LocalUpload` removes its
/// staging file on drop, so nothing is left behind and the table stays
/// invisible. (Not covered: S3, whose multipart upload has no abort on drop;
/// the default build has no `hive-s3`.)
#[tokio::test]
#[ignore = "red-team probe (passes): canceled Hive CTAS/INSERT leaves no files on local disk"]
async fn rt_pass_canceled_hive_write_leaves_no_files() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, port, warehouse) = start(dir.path(), 1);
    wait_ready(port).await;
    let c = Client::new(port);
    let schema = c.run("CREATE SCHEMA hive.rt").await;
    assert_eq!(schema["stats"]["state"], "FINISHED", "{schema}");
    let created = c.run("CREATE TABLE hive.rt.small AS SELECT 1 AS a").await;
    assert_eq!(created["stats"]["state"], "FINISHED", "{created}");

    let big = "SELECT value, repeat('x', 100) AS pad FROM generate_series(1, 400000000)";
    for statement in [
        format!("CREATE TABLE hive.rt.big AS {big}"),
        format!("INSERT INTO hive.rt.small SELECT value FROM ({big})"),
    ] {
        let (status, first) = c.post(&statement).await;
        assert_eq!(status, 200);
        let next = first["nextUri"].as_str().unwrap().to_owned();
        // Let the writer start producing the file.
        let (_, page) = c.get(&next).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(c.delete(&next).await, 204);
        if let Some(n) = page["nextUri"].as_str() {
            let (_, after) = c.get(n).await;
            assert_eq!(after["error"]["errorName"], "USER_CANCELED", "{after}");
        }
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let leftovers: Vec<String> = files_under(&warehouse)
        .into_iter()
        .filter(|f| f.contains("inprogress"))
        .collect();
    let sizes: Vec<u64> = leftovers
        .iter()
        .map(|f| std::fs::metadata(warehouse.join(f)).map_or(0, |m| m.len()))
        .collect();
    assert!(
        leftovers.is_empty(),
        "canceled writes left partial files: {leftovers:?} (bytes {sizes:?})"
    );
    let (_, first) = c.post("SELECT count(*) FROM hive.rt.small").await;
    let mut rows = Vec::new();
    let mut page = first;
    while let Some(next) = page["nextUri"].as_str().map(str::to_owned) {
        page = c.get(&next).await.1;
        rows.extend(page["data"].as_array().cloned().unwrap_or_default());
    }
    assert_eq!(rows, vec![serde_json::json!([1])], "canceled INSERT added rows: {page}");
    let big = c.run("SELECT count(*) FROM hive.rt.big").await;
    assert_eq!(big["stats"]["state"], "FAILED", "canceled CTAS left a visible table: {big}");
}

/// SIGTERM: new statements are refused with 503 while a query already running
/// keeps paging through the grace period.
#[tokio::test]
#[ignore = "red-team probe (passes): drain refuses POST, running queries keep paging"]
async fn rt_pass_sigterm_drain_keeps_running_queries() {
    let dir = tempfile::tempdir().unwrap();
    let (server, port, _) = start(dir.path(), 4);
    wait_ready(port).await;
    let c = Client::new(port);
    let (status, first) = c.post("SELECT value FROM generate_series(1, 2000000)").await;
    assert_eq!(status, 200);
    let mut next = first["nextUri"].as_str().unwrap().to_owned();
    let (_, page) = c.get(&next).await;
    next = page["nextUri"].as_str().unwrap().to_owned();
    let pid = server.0.id().to_string();
    let term = Command::new("sh")
        .args(["-c", &format!("kill -TERM {pid}")])
        .status()
        .unwrap();
    assert!(term.success());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (refused, body) = c.post("SELECT 1").await;
    assert_eq!(refused, 503, "{body}");
    let mut rows = 0;
    loop {
        let (status, page) = c.get(&next).await;
        assert_eq!(status, 200);
        rows += page["data"].as_array().map_or(0, Vec::len);
        let Some(n) = page["nextUri"].as_str() else {
            assert_eq!(page["stats"]["state"], "FINISHED", "{page}");
            break;
        };
        next = n.to_owned();
    }
    assert!(rows > 1_000_000, "{rows}");
}

/// This container's id (Docker sets the hostname to it).
fn own_container() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap()
        .trim()
        .to_owned()
}

/// The same script passes against a real `trinodb/trino:431` (catalog
/// `memory`, schema `default`); against `CodeLake` it lists what a DB-API user
/// (dbt-trino) sees differently.
const PYTHON_PROBE: &str = r#"
import sys, trino
from trino.exceptions import TrinoUserError, TrinoQueryError
port, catalog, schema = int(sys.argv[1]), sys.argv[2], sys.argv[3]
conn = trino.dbapi.connect(host="127.0.0.1", port=port, user="rt", catalog=catalog, schema=schema)
cur = conn.cursor()
problems = []

cur.execute("CREATE TABLE rt_py (a bigint)")
if cur.fetchall() != []:
    problems.append("RT5 CREATE TABLE fetchall() is not []")
cur.execute("DROP TABLE rt_py")
if cur.fetchall() != []:
    problems.append("RT5 DROP TABLE fetchall() is not []")

cur.execute("SELECT CAST(0.1 AS REAL)")
v = cur.fetchone()[0]
if v != 0.1:
    problems.append(f"RT4 REAL 0.1 arrived as {v!r}")

try:
    cur.execute("SELECT 9223372036854775807 + 1")
    cur.fetchall()
    problems.append("RT15 overflow did not fail")
except TrinoUserError as e:
    pass
except TrinoQueryError as e:
    problems.append(f"RT15 overflow is not a user error: {e.error_name}/{e.error_type}")

try:
    cur.execute("USE nocatalog.noschema")
    cur.fetchall()
    problems.append("RT1 USE of a missing catalog succeeded")
except TrinoQueryError:
    pass

print("PROBLEMS:", problems)
print("PYTHON PROBE OK" if not problems else "PYTHON PROBE FAILED")
"#;

#[tokio::test]
#[ignore = "DEFECT-RT1/RT4/RT5/RT15: impact seen by the Python trino client (dbt-trino's DB-API)"]
async fn rt_python_client_sees_trino_behavior() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, port, _) = start(dir.path(), 1);
    wait_ready(port).await;
    let script = format!(
        "pip install --quiet --disable-pip-version-check 'trino>=0.328,<1' && python -c \"$SCRIPT\" {port} datafusion public"
    );
    let container = GenericImage::new("python", "3.12-slim")
        .with_entrypoint("sh")
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_cmd(vec!["-c".to_owned(), script])
        .with_env_var("SCRIPT", PYTHON_PROBE)
        .with_network(format!("container:{}", own_container()))
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await
        .unwrap();
    let out = String::from_utf8_lossy(&container.stdout_to_vec().await.unwrap()).into_owned();
    let err = String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap()).into_owned();
    assert!(out.contains("PYTHON PROBE OK"), "stdout: {out}\nstderr: {err}");
}
