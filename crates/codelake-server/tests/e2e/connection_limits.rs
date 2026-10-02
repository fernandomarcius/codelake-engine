//! ADR 0018 through the real binary: idle keep-alive connections past a
//! deliberately low open-files limit never leave a new client hanging, and
//! the server closes them after its idle timeout.
//!
//! Before the fix, with a limit of 1024, ~1000 idle connections made every
//! new client — `SELECT 1`, the health checks — wait until it timed out, and
//! no idle connection was ever closed.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// Longest wait for an answer: a refusal must come at once, not after a
/// client timeout.
const FAST: Duration = Duration::from_secs(2);

struct Ports {
    pg: u16,
    ops: u16,
    trino: u16,
}

fn config(dir: &Path, ports: &Ports, server_keys: &str) -> PathBuf {
    let path = dir.join("codelake.toml");
    let mut doc = String::new();
    write!(
        doc,
        r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{ops}"
shutdown_grace_secs = 1
{server_keys}

[server.trino]
listen = "127.0.0.1:{trino}"
header_prefix = "X-Trino-"
reported_version = "431"

[memory]
limit = "128MiB"
spill_dir = "{spill}"
spill_limit = "256MiB"
metadata_cache_limit = "8MiB"
"#,
        pg = ports.pg,
        ops = ports.ops,
        trino = ports.trino,
        spill = dir.join("spill").display()
    )
    .unwrap();
    std::fs::write(&path, doc).unwrap();
    path
}

async fn start(dir: &Path, server_keys: &str) -> (super::Server, Ports, PathBuf) {
    let ports = Ports {
        pg: super::free_port(),
        ops: super::free_port(),
        trino: super::free_port(),
    };
    let config = config(dir, &ports, server_keys);
    let log = dir.join("server.log");
    let server = super::Server(
        Command::new(super::BIN)
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(std::fs::File::create(&log).unwrap())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    super::wait_http(&format!("http://127.0.0.1:{}/livez", ports.ops)).await;
    // Every listener up (PostgreSQL binds last) and no connection of the
    // probing above still counted.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let text = metrics(ports.ops).await;
        let open = |listener: &str| {
            metric(
                &text,
                &format!("codelake_connections_open{{listener=\"{listener}\"}}"),
            )
        };
        if text.contains("codelake_connections_max{listener=\"pg\"}")
            && open("trino") == 0.0
            && open("pg") == 0.0
        {
            break;
        }
        assert!(Instant::now() < deadline, "server did not start: {text}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (server, ports, log)
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// A keep-alive `GET path` on a new connection: the connection and the
/// first bytes of the answer (`None` if none came within [`FAST`]).
async fn get(port: u16, path: &str) -> (TcpStream, Option<String>) {
    let mut stream = TcpStream::connect(addr(port)).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\nconnection: keep-alive\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut buf = vec![0_u8; 8192];
    let answer = match tokio::time::timeout(FAST, stream.read(&mut buf)).await {
        Ok(Ok(n)) => Some(String::from_utf8_lossy(&buf[..n]).into_owned()),
        Ok(Err(e)) => Some(format!("error: {e}")),
        Err(_) => None,
    };
    (stream, answer)
}

/// Whether the server closed `stream` (EOF or reset, after whatever it
/// sent) within `wait`.
async fn closed_by_server(stream: &mut TcpStream, wait: Duration) -> bool {
    let mut buf = [0_u8; 1024];
    tokio::time::timeout(wait, async {
        while matches!(stream.read(&mut buf).await, Ok(n) if n > 0) {}
    })
    .await
    .is_ok()
}

/// The whole `/metrics` text (`connection: close`, read to the end).
async fn metrics(ops: u16) -> String {
    let mut stream = TcpStream::connect(addr(ops)).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut text = String::new();
    tokio::time::timeout(FAST, stream.read_to_string(&mut text))
        .await
        .expect("/metrics answers at once")
        .unwrap();
    text
}

fn metric(text: &str, series: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(series)?.trim().parse().ok())
        .unwrap_or(0.0)
}

async fn select_one(trino: u16) {
    let (_, rows, error) = super::limits_redteam::http(trino, "SELECT 1").await;
    assert_eq!(error, None);
    assert_eq!(rows, vec![serde_json::json!([1])]);
}

/// PostgreSQL's own share: its 4 sessions work whatever the Trino listener
/// holds, and a fifth gets PostgreSQL's error at once.
async fn postgres_keeps_its_share(pg: u16) {
    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let mut sessions = Vec::new();
    for _ in 0..4 {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client.simple_query("SELECT 1").await.unwrap();
        sessions.push(client);
    }
    // One more PostgreSQL session: PostgreSQL's own error, at once.
    let started = Instant::now();
    let err = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .map(|_| ())
        .unwrap_err();
    assert!(started.elapsed() < FAST);
    let db = err
        .as_db_error()
        .map(|e| (e.code().code().to_owned(), e.message().to_owned()));
    assert_eq!(
        db.as_ref().map(|(code, _)| code.as_str()),
        Some("53300"),
        "{err:?}"
    );
    assert!(db.unwrap().1.contains("too many clients"));
    drop(sessions);
}

#[tokio::test]
async fn idle_connections_past_the_limit_get_a_fast_503_and_are_closed() {
    let dir = tempfile::tempdir().unwrap();
    // 160 - 32 reserved - 32 ops - 4 PostgreSQL = 92 Trino connections.
    let (_server, ports, _log) = start(
        dir.path(),
        "open_files_limit = 160\nreserved_files = 32\nmax_connections = 4\n\
         http_idle_timeout_secs = 4\nhttp_header_read_timeout_secs = 2",
    )
    .await;

    // 20 more idle keep-alive clients than the listener holds.
    let mut held = Vec::new();
    let (mut served, mut refused) = (0, 0);
    for i in 0..112 {
        let (stream, answer) = get(ports.trino, "/v1/info").await;
        let answer = answer.unwrap_or_else(|| panic!("connection {i} got no answer within {FAST:?}"));
        if answer.starts_with("HTTP/1.1 200") {
            served += 1;
            held.push(stream);
        } else {
            assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
            assert!(answer.contains("maximum of 92 open connections"), "{answer}");
            refused += 1;
        }
    }
    assert_eq!((served, refused), (92, 20));

    // While they are held: a new Trino client gets a 503 at once, the probes
    // and PostgreSQL (their own shares) answer.
    let started = Instant::now();
    let (_, answer) = get(ports.trino, "/v1/info").await;
    assert!(answer.unwrap().starts_with("HTTP/1.1 503"));
    assert!(started.elapsed() < FAST);
    for probe in ["/livez", "/readyz"] {
        let (_, answer) = get(ports.ops, probe).await;
        assert!(answer.unwrap().starts_with("HTTP/1.1 200"), "{probe}");
    }
    postgres_keeps_its_share(ports.pg).await;

    // After the idle timeout (4 s) the server closed every idle connection.
    let started = Instant::now();
    for stream in &mut held {
        let left = Duration::from_secs(10).saturating_sub(started.elapsed());
        assert!(
            closed_by_server(stream, left).await,
            "an idle connection stayed open"
        );
    }

    // A client that connects and sends nothing is closed after the
    // header-read timeout (2 s), not the idle one.
    let mut silent = TcpStream::connect(addr(ports.trino)).await.unwrap();
    let started = Instant::now();
    assert!(closed_by_server(&mut silent, Duration::from_secs(6)).await);
    let after = started.elapsed();
    assert!(
        after > Duration::from_millis(1500) && after < Duration::from_secs(4),
        "{after:?}"
    );

    let text = metrics(ports.ops).await;
    assert!(
        metric(
            &text,
            "codelake_connections_closed_total{listener=\"trino\",reason=\"idle\"}"
        ) >= 92.0,
        "{text}"
    );
    assert!(
        metric(
            &text,
            "codelake_connections_refused_total{listener=\"trino\",reason=\"limit\"}"
        ) >= 21.0,
        "{text}"
    );
    assert!(
        metric(
            &text,
            "codelake_connections_refused_total{listener=\"pg\",reason=\"limit\"}"
        ) >= 1.0,
        "{text}"
    );
    assert!(
        metric(
            &text,
            "codelake_connections_closed_total{listener=\"trino\",reason=\"header_read\"}"
        ) >= 1.0,
        "{text}"
    );
    assert!(
        (metric(&text, "codelake_open_files_limit") - 160.0).abs() < 0.5,
        "{text}"
    );

    // And serves new clients again.
    select_one(ports.trino).await;
}

#[tokio::test]
async fn exhausted_descriptors_are_answered_and_logged_never_a_silent_hang() {
    let dir = tempfile::tempdir().unwrap();
    // No reserve: the listeners' shares add up to the whole limit, so the
    // descriptors the process already holds make `accept` fail with EMFILE
    // before the Trino listener reaches its share (96 - 32 - 4 = 60).
    let (_server, ports, log) = start(
        dir.path(),
        "open_files_limit = 96\nreserved_files = 0\nmax_connections = 4",
    )
    .await;

    // 31 idle ops connections: one ops slot stays free for a probe.
    let mut held = Vec::new();
    for _ in 0..31 {
        let (stream, answer) = get(ports.ops, "/livez").await;
        assert!(answer.unwrap().starts_with("HTTP/1.1 200"));
        held.push(stream);
    }
    let mut exhausted = 0;
    for i in 0..70 {
        let (stream, answer) = get(ports.trino, "/v1/info").await;
        let answer = answer.unwrap_or_else(|| panic!("Trino connection {i} got no answer within {FAST:?}"));
        if answer.starts_with("HTTP/1.1 200") {
            held.push(stream);
        } else {
            assert!(answer.contains("no file descriptor left"), "{answer}");
            exhausted += 1;
        }
    }
    assert!(
        exhausted > 0,
        "descriptors never ran out: the test proves nothing"
    );

    // The ops listener serves a probe on its spare descriptor.
    let (_, answer) = get(ports.ops, "/livez").await;
    let answer = answer.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200"), "{answer}");
    let text = metrics(ports.ops).await;
    assert!(
        metric(
            &text,
            "codelake_accept_errors_total{listener=\"trino\",error=\"descriptors\"}"
        ) >= 1.0,
        "{text}"
    );

    drop(held);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, answer) = get(ports.trino, "/v1/info").await;
        if answer.is_some_and(|a| a.starts_with("HTTP/1.1 200")) {
            break;
        }
        assert!(Instant::now() < deadline, "service did not resume");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    select_one(ports.trino).await;
    let logged = std::fs::read_to_string(&log).unwrap();
    assert!(logged.contains("cannot accept connections"), "{logged}");
    assert!(logged.contains("open files limit"), "{logged}");
}
