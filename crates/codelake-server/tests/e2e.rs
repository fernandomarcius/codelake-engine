//! End-to-end: the real `codelake` binary, a real PostgreSQL wire client, real
//! HTTP probes and a real SIGTERM. Runs in the integration lane.

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

const BIN: &str = env!("CARGO_BIN_EXE_codelake");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write_config(dir: &std::path::Path, pg: u16, http: u16) -> std::path::PathBuf {
    let path = dir.join("codelake.toml");
    let mut file = std::fs::File::create(&path).unwrap();
    write!(
        file,
        r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = 1

[memory]
limit = "128MiB"
spill_dir = "{spill}"
spill_limit = "256MiB"
metadata_cache_limit = "8MiB"
"#,
        spill = dir.join("spill").display()
    )
    .unwrap();
    path
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

async fn wait_http(url: &str) -> reqwest::Response {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = reqwest::get(url).await {
            return response;
        }
        assert!(Instant::now() < deadline, "server did not answer {url}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn serves_sql_probes_and_drains_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let (pg, http) = (free_port(), free_port());
    let config = write_config(dir.path(), pg, http);
    let mut server = Server(
        Command::new(BIN)
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );

    assert_eq!(
        wait_http(&format!("http://127.0.0.1:{http}/livez"))
            .await
            .status(),
        200
    );
    assert_eq!(
        reqwest::get(format!("http://127.0.0.1:{http}/readyz"))
            .await
            .unwrap()
            .status(),
        200
    );

    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    let rows = client.simple_query("SELECT 40 + 2 AS answer").await.unwrap();
    let answer = rows
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("answer").map(str::to_owned),
            _ => None,
        })
        .unwrap();
    assert_eq!(answer, "42");

    let metrics = reqwest::get(format!("http://127.0.0.1:{http}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("codelake_statements_total"), "{metrics}");
    assert!(
        metrics.contains("codelake_memory_limit_bytes 134217728"),
        "{metrics}"
    );

    // SIGTERM: readiness must flip to 503 while the grace period runs, then exit 0.
    let status = Command::new("kill")
        .args(["-TERM", &server.0.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    // The signal is handled asynchronously; readiness must flip well within
    // the 1 s grace period.
    let readyz = format!("http://127.0.0.1:{http}/readyz");
    let flip_deadline = Instant::now() + Duration::from_millis(900);
    let mut ready = reqwest::get(&readyz).await.unwrap().status();
    while ready != 503 && Instant::now() < flip_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
        ready = reqwest::get(&readyz).await.unwrap().status();
    }
    assert_eq!(ready, 503, "readiness must fail while draining");

    let deadline = Instant::now() + Duration::from_secs(20);
    let exit = loop {
        if let Some(exit) = server.0.try_wait().unwrap() {
            break exit;
        }
        assert!(Instant::now() < deadline, "server did not exit after SIGTERM");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(exit.success(), "{exit:?}");
}

#[test]
fn check_config_accepts_valid_and_reports_every_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path(), free_port(), free_port());
    let ok = Command::new(BIN)
        .args(["check-config", "--config"])
        .arg(&config)
        .output()
        .unwrap();
    assert!(ok.status.success(), "{ok:?}");

    let broken = dir.path().join("broken.toml");
    std::fs::write(
        &broken,
        std::fs::read_to_string(&config)
            .unwrap()
            .replace("limit = \"128MiB\"", "limit = \"1MiB\"")
            .replace("127.0.0.1:", "0.0.0.0:"),
    )
    .unwrap();
    let bad = Command::new(BIN)
        .args(["check-config", "--config"])
        .arg(&broken)
        .output()
        .unwrap();
    assert_eq!(bad.status.code(), Some(2), "config errors exit with code 2");
    let log = String::from_utf8_lossy(&bad.stdout);
    assert!(log.contains("memory.limit 1MiB is below"), "{log}");
    assert!(log.contains("client authentication is not implemented"), "{log}");
}

#[test]
fn missing_config_file_is_a_config_error() {
    let out = Command::new(BIN)
        .args(["check-config", "--config", "/nope/codelake.toml"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
