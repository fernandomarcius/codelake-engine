//! Query governance with real clients against the real `codelake` binary: a
//! PostgreSQL wire client (`tokio-postgres`) and the Python client of the
//! HTTP statement protocol (the DB-API dbt uses), sharing one node's
//! admission slots, time limits and query registry. Memory is read from the
//! engine's pool (`/healthz`), never as process RSS. Runs in the integration
//! lane.

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
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

const BIN: &str = env!("CARGO_BIN_EXE_codelake");

/// Rows enough that a query holds its slot while its client does not read.
const HOLDS_A_SLOT: &str = "SELECT value FROM generate_series(1, 100000000)";
/// A join whose build side (2 M rows) stays reserved in the pool while the
/// probe side (3 G rows) streams: it holds memory until it is stopped.
const HOLDS_MEMORY: &str = "SELECT count(*) FROM generate_series(1, 2000000) a \
                            JOIN generate_series(1, 3000000000) b ON a.value = b.value";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Server {
    child: Child,
    pg: u16,
    http: u16,
    statement: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

fn start_server(dir: &std::path::Path) -> Server {
    let (pg, http, statement) = (free_port(), free_port(), free_port());
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
listen = "127.0.0.1:{statement}"

[memory]
limit = "256MiB"
spill_dir = "{spill}"
spill_limit = "512MiB"
metadata_cache_limit = "8MiB"

[governance]
max_running_queries = 8
max_running_per_user = 1
max_queued_per_user = 1
queue_timeout_secs = 60
"#,
        spill = dir.join("spill").display()
    )
    .unwrap();
    let child = Command::new(BIN)
        .args(["serve", "--config"])
        .arg(&path)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    Server {
        child,
        pg,
        http,
        statement,
    }
}

impl Server {
    async fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        for url in [
            format!("http://127.0.0.1:{}/livez", self.http),
            format!("http://127.0.0.1:{}/v1/info", self.statement),
        ] {
            while reqwest::get(&url).await.is_err() {
                assert!(Instant::now() < deadline, "server did not start: {url}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    async fn connect(&self, user: &str) -> Client {
        let dsn = format!(
            "host=127.0.0.1 port={} user={user} dbname=codelake application_name=governance-pg",
            self.pg
        );
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    }

    /// Bytes reserved in the engine's memory pool.
    async fn reserved(&self) -> u64 {
        let health = json(
            reqwest::get(format!("http://127.0.0.1:{}/healthz", self.http))
                .await
                .unwrap(),
        )
        .await;
        health["memory"]["reserved_bytes"].as_u64().unwrap()
    }

    /// Waits (at most 15 s) for the pool to hold nothing.
    async fn pool_drains(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let reserved = self.reserved().await;
            if reserved == 0 || Instant::now() > deadline {
                return reserved;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn metrics(&self) -> String {
        reqwest::get(format!("http://127.0.0.1:{}/metrics", self.http))
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    /// Waits (at most 15 s) until the node has no query running or queued.
    async fn slots_free(&self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let metrics = self.metrics().await;
            if metrics.contains("codelake_queries_running 0\n")
                && metrics.contains("codelake_queries_queued 0\n")
            {
                return;
            }
            assert!(Instant::now() < deadline, "slots still taken:\n{metrics}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn json(response: reqwest::Response) -> serde_json::Value {
    serde_json::from_str(&response.text().await.unwrap()).unwrap()
}

async fn text_rows(client: &Client, sql: &str) -> Vec<Vec<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

/// Polls the pool while `work` runs; returns its outcome and the most bytes
/// seen reserved.
async fn peak_while<T>(server: &Server, work: impl Future<Output = T>) -> (T, u64) {
    let mut work = std::pin::pin!(work);
    let mut peak = 0;
    loop {
        tokio::select! {
            outcome = &mut work => return (outcome, peak),
            () = tokio::time::sleep(Duration::from_millis(100)) => {
                peak = peak.max(server.reserved().await);
            }
        }
    }
}

/// Both protocols in one test: they share the node's governance, and client
/// containers join this container's network namespace (two tests creating
/// that network entry at once collide).
#[tokio::test]
async fn governance_with_real_clients_on_both_protocols() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    postgres_timeout_stops_the_query_and_frees_the_pool(&server).await;
    postgres_per_user_limit_queues_and_refuses(&server).await;
    postgres_timeout_reaches_a_client_that_stopped_reading(&server).await;
    postgres_cancel_request_stops_the_query_and_frees_the_pool(&server).await;
    kill_query_stops_a_postgres_query(&server).await;
    kill_query_stops_an_http_query(&server).await;
    delete_cancels_a_postgres_query(&server).await;
    http_client_limits_and_timeout(&server).await;
    registry_lists_both_protocols(&server).await;
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0, "reservations left at the end");
    let metrics = server.metrics().await;
    for name in [
        "codelake_queries_rejected_total{scope=\"user\"}",
        "codelake_queries_timed_out_total{phase=\"running\"}",
        "codelake_queries_completed_total{protocol=\"postgresql\",state=\"FAILED\"}",
        "codelake_queries_completed_total{protocol=\"http\",state=\"FINISHED\"}",
    ] {
        assert!(metrics.contains(name), "{name} missing:\n{metrics}");
    }
}

/// Definition of done: `statement_timeout` stops the query for real; the pool, which held
/// the join's build side, returns to zero.
async fn postgres_timeout_stops_the_query_and_frees_the_pool(server: &Server) {
    let client = server.connect("tina").await;
    client
        .batch_execute("SET statement_timeout = '2s'")
        .await
        .unwrap();
    let started = Instant::now();
    let (outcome, peak) = peak_while(server, client.simple_query(HOLDS_MEMORY)).await;
    let error = outcome.unwrap_err();
    let elapsed = started.elapsed();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    let message = error.as_db_error().unwrap().message().to_owned();
    assert_eq!(message, "Query exceeded maximum time limit of 2.00s");
    assert!(elapsed < Duration::from_secs(8), "stopped after {elapsed:?}");
    assert!(
        peak > 1 << 20,
        "the pool held {peak} bytes: too little to prove anything"
    );
    let left = server.pool_drains().await;
    assert_eq!(left, 0, "reservations left after the timeout (peak {peak})");
    // The connection stays usable.
    assert_eq!(text_rows(&client, "SELECT 40 + 2").await, [["42"]]);
}

/// One running and one queued query per user (configured): the third is
/// refused with `53400`; another user is not held back; the queued one runs
/// once the first ends.
async fn postgres_per_user_limit_queues_and_refuses(server: &Server) {
    let mut holder = server.connect("alice").await;
    let transaction = holder.transaction().await.unwrap();
    let portal = transaction.bind(HOLDS_A_SLOT, &[]).await.unwrap();
    // Ten rows read; the portal stays open, suspended, holding the slot.
    let first = transaction.query_portal(&portal, 10).await.unwrap();
    assert_eq!(first.len(), 10);

    let queued_client = server.connect("alice").await;
    let queued = tokio::spawn(async move { text_rows(&queued_client, "SELECT 1 AS one").await });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!queued.is_finished(), "alice's second query must wait");

    let refused = server.connect("alice").await;
    let error = refused.simple_query("SELECT 2").await.unwrap_err();
    assert_eq!(
        error.code(),
        Some(&SqlState::CONFIGURATION_LIMIT_EXCEEDED),
        "{error:?}"
    );
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "Too many queued queries for user alice (limit 1)"
    );
    // Extended protocol too.
    let error = refused.query("SELECT 3", &[]).await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::CONFIGURATION_LIMIT_EXCEEDED));

    let bob = server.connect("bob").await;
    assert_eq!(text_rows(&bob, "SELECT 4").await, [["4"]]);
    let listed = text_rows(
        &bob,
        "SELECT state, coalesce(error_code, '-') FROM system.runtime.queries \
         WHERE \"user\" = 'alice' ORDER BY created, query_id",
    )
    .await;
    assert_eq!(
        listed,
        [
            ["RUNNING", "-"],
            ["QUEUED", "-"],
            ["FAILED", "QUERY_QUEUE_FULL"],
            ["FAILED", "QUERY_QUEUE_FULL"],
        ]
    );

    // Closing the portal ends the first query: the queued one runs.
    drop(portal);
    transaction.commit().await.unwrap();
    let rows = tokio::time::timeout(Duration::from_secs(20), queued)
        .await
        .expect("the queued query ran once the slot freed")
        .unwrap();
    assert_eq!(rows, [["1"]]);
    drop(holder.simple_query("SELECT 1").await.unwrap());
}

/// A client that stopped reading (a suspended portal) does not keep its
/// query past the deadline: the slot and the memory are given back.
async fn postgres_timeout_reaches_a_client_that_stopped_reading(server: &Server) {
    let mut client = server.connect("idle").await;
    client
        .batch_execute("SET statement_timeout = '1s'")
        .await
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    let portal = transaction.bind(HOLDS_A_SLOT, &[]).await.unwrap();
    assert_eq!(transaction.query_portal(&portal, 5).await.unwrap().len(), 5);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    server.slots_free().await;
    let error = transaction.query_portal(&portal, 5).await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    drop(transaction);
    let observer = server.connect("observer").await;
    let listed = text_rows(
        &observer,
        "SELECT state, error_code FROM system.runtime.queries WHERE \"user\" = 'idle'",
    )
    .await;
    assert_eq!(listed, [["FAILED", "EXCEEDED_TIME_LIMIT"]]);
}

/// Waits (at most 15 s) until the pool holds more than 1 MiB: the query
/// under test is executing and holds memory.
async fn pool_fills(server: &Server) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let reserved = server.reserved().await;
        if reserved > 1 << 20 {
            return reserved;
        }
        assert!(
            Instant::now() < deadline,
            "the query never held memory ({reserved} bytes)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `state` and `error_code` of `user`'s queries, oldest first.
async fn states_of(client: &Client, user: &str) -> Vec<Vec<String>> {
    text_rows(
        client,
        &format!(
            "SELECT state, coalesce(error_code, '-') FROM system.runtime.queries \
             WHERE \"user\" = '{user}' ORDER BY created, query_id"
        ),
    )
    .await
}

/// A PostgreSQL cancel request (what `psql`'s Ctrl-C and JDBC's
/// `Statement.cancel` send) stops a statement that is executing: SQLSTATE
/// 57014, the pool back at zero, the connection still usable.
async fn postgres_cancel_request_stops_the_query_and_frees_the_pool(server: &Server) {
    let client = server.connect("carl").await;
    let token = client.cancel_token();
    let running = tokio::spawn(async move {
        let outcome = client.simple_query(HOLDS_MEMORY).await.map(|_| ());
        (client, outcome)
    });
    let held = pool_fills(server).await;
    token.cancel_query(NoTls).await.unwrap();
    let (client, outcome) = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the canceled statement ended")
        .unwrap();
    let error = outcome.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "canceling statement due to user request"
    );
    assert_eq!(server.pool_drains().await, 0, "reservations left (held {held})");
    assert_eq!(text_rows(&client, "SELECT 6 * 7").await, [["42"]]);
    let observer = server.connect("observer").await;
    assert_eq!(
        states_of(&observer, "carl").await,
        [["FAILED", "USER_CANCELED"], ["FINISHED", "-"]]
    );
}

/// The id of `user`'s running join (`HOLDS_MEMORY`), read by another user:
/// `user` has no free running slot.
async fn running_join_of(server: &Server, user: &str) -> String {
    let observer = server.connect("observer").await;
    let ids = text_rows(
        &observer,
        &format!(
            "SELECT query_id FROM system.runtime.queries \
             WHERE \"user\" = '{user}' AND state = 'RUNNING' AND query LIKE '%3000000000%'"
        ),
    )
    .await;
    assert_eq!(ids.len(), 1, "{ids:?}");
    ids[0][0].clone()
}

/// `CALL system.runtime.kill_query` from another connection of the owner
/// stops a PostgreSQL query; another user is refused. The call needs no
/// slot (the owner has none free). Memory returns to the pool.
async fn kill_query_stops_a_postgres_query(server: &Server) {
    let client = server.connect("kim").await;
    let running = tokio::spawn(async move { client.simple_query(HOLDS_MEMORY).await.map(|_| ()) });
    pool_fills(server).await;
    let id = running_join_of(server, "kim").await;
    let stranger = server.connect("lee").await;
    let error = stranger
        .simple_query(&format!("CALL system.runtime.kill_query('{id}')"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INSUFFICIENT_PRIVILEGE), "{error:?}");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "Access Denied: Cannot kill query owned by kim"
    );
    let owner = server.connect("kim").await;
    owner
        .simple_query(&format!(
            "CALL system.runtime.kill_query(query_id => '{id}', message => 'runbook')"
        ))
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the killed statement ended")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "Query killed. Message: runbook"
    );
    assert_eq!(
        server.pool_drains().await,
        0,
        "reservations left after kill_query"
    );
    // Killing it again (extended protocol): it is no longer running.
    let error = owner
        .execute(&format!("CALL system.runtime.kill_query('{id}')"), &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE),
        "{error:?}"
    );
}

/// An HTTP query (its client does not page), killed from a PostgreSQL
/// connection of its owner over the extended protocol.
async fn kill_query_stops_an_http_query(server: &Server) {
    let http = reqwest::Client::new();
    let submitted = json(
        http.post(format!("http://127.0.0.1:{}/v1/statement", server.statement))
            .header("X-Trino-User", "kim")
            .body(HOLDS_MEMORY)
            .send()
            .await
            .unwrap(),
    )
    .await;
    let id = submitted["id"].as_str().unwrap().to_owned();
    let mut page = json(
        http.get(submitted["nextUri"].as_str().unwrap())
            .send()
            .await
            .unwrap(),
    )
    .await;
    pool_fills(server).await;
    let owner = server.connect("kim").await;
    let call = format!("CALL system.runtime.kill_query('{id}', 'from psql')");
    assert_eq!(owner.execute(&call, &[]).await.unwrap(), 0);
    while let Some(next) = page["nextUri"].as_str().map(str::to_owned) {
        page = json(http.get(next).send().await.unwrap()).await;
    }
    assert_eq!(page["error"]["errorName"], "ADMINISTRATIVELY_KILLED", "{page}");
    assert_eq!(
        page["error"]["message"], "Query killed. Message: from psql",
        "{page}"
    );
    assert_eq!(
        server.pool_drains().await,
        0,
        "reservations left after the HTTP kill"
    );
}

/// A PostgreSQL query canceled over HTTP by id (`DELETE /v1/query/{id}`), by
/// its owner only.
async fn delete_cancels_a_postgres_query(server: &Server) {
    let client = server.connect("kim").await;
    let running = tokio::spawn(async move { client.simple_query(HOLDS_MEMORY).await.map(|_| ()) });
    pool_fills(server).await;
    let id = running_join_of(server, "kim").await;
    let http = reqwest::Client::new();
    let delete = |user: &'static str| {
        http.delete(format!("http://127.0.0.1:{}/v1/query/{id}", server.statement))
            .header("X-Trino-User", user)
            .send()
    };
    assert_eq!(delete("lee").await.unwrap().status(), 403);
    assert_eq!(delete("kim").await.unwrap().status(), 204);
    let error = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the canceled statement ended")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    assert_eq!(server.pool_drains().await, 0, "reservations left after DELETE");
    let observer = server.connect("observer").await;
    let states = states_of(&observer, "kim").await;
    let codes: Vec<&str> = states
        .iter()
        .filter(|row| row[0] == "FAILED")
        .map(|row| row[1].as_str())
        .collect();
    assert_eq!(
        codes,
        [
            "ADMINISTRATIVELY_KILLED",
            "ADMINISTRATIVELY_KILLED",
            "USER_CANCELED"
        ],
        "{states:?}"
    );
}

/// This container's id (Docker sets the hostname to it).
fn own_container() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap()
        .trim()
        .to_owned()
}

const PYTHON_CLIENT: &str = r#"
import sys, threading, time, trino
from trino.exceptions import TrinoQueryError

port = int(sys.argv[1])
def cursor(user, **kw):
    return trino.dbapi.connect(host="127.0.0.1", port=port, user=user, **kw).cursor()

# carol holds her one running slot: the client reads a row and stops polling.
holder = cursor("carol")
holder.execute("SELECT value FROM generate_series(1, 100000000)")
assert holder.fetchone() is not None

# Her second query waits in the queue (one queued per user).
result = {}
def queued():
    c = cursor("carol")
    c.execute("SELECT 1")
    result["rows"] = c.fetchall()
waiter = threading.Thread(target=queued)
waiter.start()
time.sleep(1)
assert "rows" not in result, result

# Her third is refused.
try:
    c = cursor("carol")
    c.execute("SELECT 2")
    c.fetchall()
    raise AssertionError("QUERY_QUEUE_FULL expected")
except TrinoQueryError as e:
    assert e.error_name == "QUERY_QUEUE_FULL", e.error_name
    assert e.error_type == "INSUFFICIENT_RESOURCES", e.error_type
    assert "user carol" in e.message, e.message

# Another user is not held back.
other = cursor("dave")
other.execute("SELECT 3")
assert other.fetchall() == [[3]]

# Canceling the first lets the queued one run.
holder.cancel()
waiter.join(30)
assert result.get("rows") == [[1]], result

# query_max_run_time stops a query.
try:
    c = cursor("erin", session_properties={"query_max_run_time": "2s"})
    c.execute("SELECT count(*) FROM generate_series(1, 2000000) a JOIN generate_series(1, 3000000000) b ON a.value = b.value")
    c.fetchall()
    raise AssertionError("EXCEEDED_TIME_LIMIT expected")
except TrinoQueryError as e:
    assert e.error_name == "EXCEEDED_TIME_LIMIT", e.error_name
    assert e.message == "Query exceeded maximum time limit of 2.00s", e.message
print("GOVERNANCE CLIENT OK")
"#;

/// The same limits through the HTTP protocol, with its Python client.
async fn http_client_limits_and_timeout(server: &Server) {
    let script = format!(
        "pip install --quiet --disable-pip-version-check 'trino>=0.328,<1' && python -c \"$SCRIPT\" {}",
        server.statement
    );
    let container = GenericImage::new("python", "3.12-slim")
        .with_entrypoint("sh")
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_cmd(vec!["-c".to_owned(), script])
        .with_env_var("SCRIPT", PYTHON_CLIENT)
        .with_network(format!("container:{}", own_container()))
        .with_startup_timeout(Duration::from_secs(300));
    let (container, peak) = peak_while(server, container.start()).await;
    let container = container.unwrap();
    let code = container.exit_code().await.unwrap().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&container.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap()).into_owned();
    assert_eq!(code, 0, "stdout: {stdout}\nstderr: {stderr}");
    assert!(stdout.contains("GOVERNANCE CLIENT OK"), "{stdout}");
    assert!(
        peak > 1 << 20,
        "the pool held {peak} bytes during the timed-out join"
    );
    assert_eq!(server.pool_drains().await, 0, "reservations left (peak {peak})");
}

/// One registry: queries of both protocols, readable from both.
async fn registry_lists_both_protocols(server: &Server) {
    let client = server.connect("observer").await;
    let by_source = text_rows(
        &client,
        "SELECT coalesce(source, '-') AS source, count(*) FROM system.runtime.queries \
         WHERE \"user\" IN ('carol', 'bob') GROUP BY 1 ORDER BY 1",
    )
    .await;
    // bob's through PostgreSQL (its `application_name`), carol's through HTTP
    // (the Python client names itself).
    assert_eq!(by_source.len(), 2, "{by_source:?}");
    assert!(
        by_source.iter().any(|row| row[0] == "governance-pg"),
        "{by_source:?}"
    );

    let body = json(
        reqwest::Client::new()
            .post(format!("http://127.0.0.1:{}/v1/statement", server.statement))
            .header("X-Trino-User", "observer")
            .body("SELECT count(*) FROM system.runtime.queries WHERE source = 'governance-pg'")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let mut page = body;
    let mut data = None;
    while data.is_none() {
        data = page.get("data").cloned();
        let Some(next) = page["nextUri"].as_str().map(str::to_owned) else {
            break;
        };
        page = json(reqwest::get(next).await.unwrap()).await;
    }
    let count = data.unwrap()[0][0].as_i64().unwrap();
    assert!(count >= 5, "PostgreSQL queries seen over HTTP: {count}");
}

// ------------------------------------------------------------ audit (F6) ---

/// `(running, queued)` admission slots, from `/healthz`.
async fn load(server: &Server) -> (u64, u64) {
    let health = json(
        reqwest::get(format!("http://127.0.0.1:{}/healthz", server.http))
            .await
            .unwrap(),
    )
    .await;
    (
        health["queries"]["running"].as_u64().unwrap(),
        health["queries"]["queued"].as_u64().unwrap(),
    )
}

/// A `DECLARE … CURSOR` (psycopg2 named cursors, pandas chunked reads) runs
/// under governance like a suspended portal: its time limit stops it and its
/// memory returns to the pool. Found by the F6 audit: the `DECLARE` statement
/// finishes at once, and the cursor's stream then lives in the connection
/// without a slot, a deadline or a registry entry anyone can kill, holding
/// its reservations until `CLOSE`.
#[tokio::test]
async fn audit_declared_cursor_is_bounded_by_its_time_limit() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    let client = server.connect("zed").await;
    client
        .batch_execute("SET statement_timeout = '5s'")
        .await
        .unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(
            "DECLARE c1 CURSOR FOR SELECT a.value FROM generate_series(1, 2000000) a \
             JOIN generate_series(1, 3000000000) b ON a.value = b.value",
        )
        .await
        .unwrap();
    assert_eq!(text_rows(&client, "FETCH 1 FROM c1").await.len(), 1);
    let held = pool_fills(&server).await;
    let slots = load(&server).await;
    // Past the 5 s limit, idle: a suspended portal is stopped by now.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let observer = server.connect("observer").await;
    let states = states_of(&observer, "zed").await;
    let left = server.pool_drains().await;
    assert_eq!(
        left, 0,
        "the cursor kept {left} bytes reserved past its time limit (held {held}; \
         slots while it held them: {slots:?}; zed's queries: {states:?})"
    );
}
