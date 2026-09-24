//! Real Trino clients against the real `codelake` binary: the CLI shipped in
//! `trinodb/trino:431` and the Python `trino` package (the DB-API used by
//! dbt-trino). Client containers join this container's network namespace, so
//! they reach the server on 127.0.0.1. Runs in the integration lane.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test crate"
)]

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

fn start_server(dir: &std::path::Path) -> (Server, u16) {
    let (pg, http, trino) = (free_port(), free_port(), free_port());
    let path = dir.join("codelake.toml");
    let mut file = std::fs::File::create(&path).unwrap();
    write!(
        file,
        r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = 1

[server.trino]
listen = "127.0.0.1:{trino}"

[memory]
limit = "256MiB"
spill_dir = "{spill}"
spill_limit = "512MiB"
metadata_cache_limit = "8MiB"
"#,
        spill = dir.join("spill").display()
    )
    .unwrap();
    let server = Server(
        Command::new(BIN)
            .args(["serve", "--config"])
            .arg(&path)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    (server, trino)
}

async fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let url = format!("http://127.0.0.1:{port}/v1/info");
    while reqwest::get(&url).await.is_err() {
        assert!(Instant::now() < deadline, "Trino listener did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// This container's id (Docker sets the hostname to it).
fn own_container() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap()
        .trim()
        .to_owned()
}

/// Runs a client container to completion; returns (exit code, stdout, stderr).
async fn run_client(image: GenericImage, cmd: Vec<String>, env: &[(&str, &str)]) -> (i64, String, String) {
    let mut request = image
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_cmd(cmd);
    for (name, value) in env {
        request = request.with_env_var(*name, *value);
    }
    let container = request
        .with_network(format!("container:{}", own_container()))
        .with_startup_timeout(Duration::from_secs(300))
        .start()
        .await
        .unwrap();
    let code = container.exit_code().await.unwrap().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&container.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap()).into_owned();
    (code, stdout, stderr)
}

async fn trino_cli(port: u16, args: &[&str]) -> (i64, String, String) {
    let mut cmd = vec![
        format!("--server=http://127.0.0.1:{port}"),
        "--user=cli".to_owned(),
        "--output-format=CSV".to_owned(),
    ];
    cmd.extend(args.iter().map(|a| (*a).to_owned()));
    run_client(
        GenericImage::new("trinodb/trino", "431").with_entrypoint("trino"),
        cmd,
        &[],
    )
    .await
}

/// Both clients in one test: testcontainers creates a throwaway network entry
/// for `container:<id>` mode, and two tests creating it at once collide.
#[tokio::test]
async fn trino_cli_and_python_client_work_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, port) = start_server(dir.path());
    wait_ready(port).await;
    trino_cli_runs_queries_ddl_and_reports_errors(port).await;
    python_client_dbapi_works(port).await;
}

async fn trino_cli_runs_queries_ddl_and_reports_errors(port: u16) {
    let (code, out, err) = trino_cli(
        port,
        &["--execute=SELECT count(*), sum(value) FROM generate_series(1, 300000)"],
    )
    .await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert_eq!(out.trim(), "\"300000\",\"45000150000\"");

    let (code, out, err) = trino_cli(
        port,
        &[
            "--catalog=datafusion",
            "--schema=public",
            "--execute=CREATE TABLE t AS SELECT 1 AS a; INSERT INTO t VALUES (2), (3); SELECT sum(a) FROM t",
        ],
    )
    .await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert_eq!(out.lines().last().unwrap().trim(), "\"6\"", "{out}");

    let (code, out, err) = trino_cli(port, &["--execute=SELECT * FROM missing_table"]).await;
    assert_ne!(code, 0, "{out}");
    assert!(err.contains("missing_table"), "stderr: {err}");
}

const PYTHON_CLIENT: &str = r#"
import sys, trino
from trino.exceptions import TrinoUserError

port = int(sys.argv[1])
conn = trino.dbapi.connect(
    host="127.0.0.1", port=port, user="dbt", catalog="datafusion", schema="public",
    session_properties={"query_max_run_time": "1h", "scale_writers": "false"},
)
cur = conn.cursor()

cur.execute("SELECT value FROM generate_series(1, 250000) ORDER BY value")
rows = cur.fetchall()
assert len(rows) == 250000, len(rows)
assert sum(r[0] for r in rows) == 250000 * 250001 // 2
assert [r[0] for r in rows[:3]] == [1, 2, 3]

cur.execute("SELECT CAST('12.50' AS DECIMAL(10,2)), 'ação', CAST(NULL AS BIGINT), true")
row = cur.fetchone()
assert str(row[0]) == "12.50", row
assert row[1] == "ação" and row[2] is None and row[3] is True, row

cur.execute("CREATE TABLE py_items AS SELECT 1 AS id")
cur.fetchall()
cur.execute("INSERT INTO py_items VALUES (2), (3), (4)")
cur.fetchall()
assert cur.rowcount == 3, cur.rowcount
cur.execute("SELECT count(*) FROM py_items")
assert cur.fetchone()[0] == 4

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'py_items'")
assert cur.fetchall() == [["py_items"]]

try:
    cur.execute("SELECT 1 +")
    cur.fetchall()
    raise AssertionError("syntax error expected")
except TrinoUserError as e:
    assert e.error_name == "SYNTAX_ERROR", e.error_name

try:
    bad = trino.dbapi.connect(host="127.0.0.1", port=port, user="dbt", session_properties={"nope": "1"})
    c = bad.cursor()
    c.execute("SELECT 1")
    c.fetchall()
    raise AssertionError("invalid session property expected")
except TrinoUserError as e:
    assert e.error_name == "INVALID_SESSION_PROPERTY", e.error_name

cur.execute("SELECT state FROM system.runtime.queries WHERE query LIKE '%py_items VALUES%' AND state = 'FINISHED'")
assert len(cur.fetchall()) == 1
print("PYTHON CLIENT OK")
"#;

async fn python_client_dbapi_works(port: u16) {
    let script = format!(
        "pip install --quiet --disable-pip-version-check 'trino>=0.328,<1' && python -c \"$SCRIPT\" {port}"
    );
    let image = GenericImage::new("python", "3.12-slim").with_entrypoint("sh");
    let (code, out, err) =
        run_client(image, vec!["-c".to_owned(), script], &[("SCRIPT", PYTHON_CLIENT)]).await;
    assert_eq!(code, 0, "stdout: {out}\nstderr: {err}");
    assert!(out.contains("PYTHON CLIENT OK"), "{out}");
}
