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
use testcontainers_modules::postgres::Postgres;
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
        // The PostgreSQL listener too: it may bind after the HTTP ones.
        while tokio::net::TcpStream::connect(("127.0.0.1", self.pg))
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "PostgreSQL port {} not open", self.pg);
            tokio::time::sleep(Duration::from_millis(100)).await;
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

/// Values of a one-column `rows`, as numbers.
fn numbers(rows: &[Vec<String>]) -> Vec<i64> {
    rows.iter().map(|row| row[0].parse().unwrap()).collect()
}

/// Cursors, declared on either protocol, return every row, in order,
/// across fetches of each kind, holding their slot until read to the end.
async fn cursors_return_every_row(server: &Server, client: &Client, observer: &Client) {
    // Declared on the simple protocol, read by fetches of each kind.
    client
        .batch_execute("DECLARE c0 CURSOR FOR SELECT value FROM generate_series(1, 5000)")
        .await
        .unwrap();
    assert_eq!(states_of(observer, "zed").await, [["RUNNING", "-"]]);
    assert_eq!(load(server).await, (1, 0), "the cursor holds its slot");
    assert_eq!(numbers(&text_rows(client, "FETCH 3 FROM c0").await), [1, 2, 3]);
    assert_eq!(numbers(&text_rows(client, "FETCH NEXT FROM c0").await), [4]);
    assert_eq!(
        numbers(&text_rows(client, "FETCH FORWARD 1000 FROM c0").await),
        (5..=1004).collect::<Vec<_>>()
    );
    assert_eq!(
        numbers(&text_rows(client, "FETCH ALL FROM c0").await),
        (1005..=5000).collect::<Vec<_>>()
    );
    assert!(text_rows(client, "FETCH ALL FROM c0").await.is_empty());
    assert_eq!(states_of(observer, "zed").await, [["FINISHED", "-"]]);
    server.slots_free().await;
    client.batch_execute("CLOSE c0").await.unwrap();

    // Declared on the extended protocol too.
    client
        .execute(
            "DECLARE c1 CURSOR FOR SELECT value FROM generate_series(1, 10)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(load(server).await, (1, 0));
    assert_eq!(
        numbers(&text_rows(client, "FETCH ALL FROM c1").await),
        (1..=10).collect::<Vec<_>>()
    );
    server.slots_free().await;
}

/// A PostgreSQL connection spoken to message by message, for what
/// `tokio-postgres` does not send: an extended-protocol `FETCH` with its
/// results in text (psycopg3's server-side cursors).
struct Wire(std::net::TcpStream, BackendKey);

/// The process id and secret key a cancel request names a connection by.
#[derive(Clone, Default)]
struct BackendKey {
    pid: i32,
    secret: Vec<u8>,
}

impl BackendKey {
    /// Sends a cancel request for the connection, as `psql`'s Ctrl-C does.
    fn cancel(&self, port: u16) {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        let len = i32::try_from(12 + self.secret.len()).unwrap();
        let request = [
            &len.to_be_bytes()[..],
            &80_877_102_i32.to_be_bytes(),
            &self.pid.to_be_bytes(),
            &self.secret,
        ]
        .concat();
        stream.write_all(&request).unwrap();
    }
}

/// A backend message: its type byte and body.
type Message = (u8, Vec<u8>);

fn cstr(text: &str) -> Vec<u8> {
    let mut bytes = text.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

impl Wire {
    fn connect(port: u16, user: &str) -> Self {
        Self::connect_to("127.0.0.1", port, user, "codelake")
    }

    /// A connection to any PostgreSQL server that trusts `user`.
    fn connect_to(host: &str, port: u16, user: &str, database: &str) -> Self {
        let stream = std::net::TcpStream::connect((host, port)).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        let mut wire = Self(stream, BackendKey::default());
        let mut body = 196_608_i32.to_be_bytes().to_vec();
        for part in ["user", user, "database", database] {
            body.extend(cstr(part));
        }
        body.push(0);
        let len = i32::try_from(body.len() + 4).unwrap();
        wire.0
            .write_all(&[len.to_be_bytes().as_slice(), &body].concat())
            .unwrap();
        let started = wire.until_ready();
        if let Some((_, key)) = started.iter().find(|(kind, _)| *kind == b'K') {
            wire.1 = BackendKey {
                pid: i32::from_be_bytes(key[..4].try_into().unwrap()),
                secret: key[4..].to_vec(),
            };
        }
        wire
    }

    fn send(&mut self, kind: u8, body: &[u8]) {
        let len = i32::try_from(body.len() + 4).unwrap();
        self.0
            .write_all(&[&[kind][..], &len.to_be_bytes(), body].concat())
            .unwrap();
    }

    /// Backend messages up to and including `ReadyForQuery`.
    fn until_ready(&mut self) -> Vec<Message> {
        use std::io::Read;
        let mut messages = Vec::new();
        loop {
            let mut head = [0_u8; 5];
            self.0.read_exact(&mut head).unwrap();
            let len = i32::from_be_bytes(head[1..5].try_into().unwrap());
            let mut body = vec![0_u8; usize::try_from(len - 4).unwrap()];
            self.0.read_exact(&mut body).unwrap();
            messages.push((head[0], body));
            if head[0] == b'Z' {
                return messages;
            }
        }
    }

    fn query(&mut self, sql: &str) -> Vec<Message> {
        self.send(b'Q', &cstr(sql));
        self.until_ready()
    }

    fn parse(&mut self, statement: &str, sql: &str) {
        self.send(b'P', &[cstr(statement), cstr(sql), vec![0, 0]].concat());
    }

    /// Binds `statement` to the unnamed portal, results in `format` (0 text,
    /// 1 binary).
    fn bind(&mut self, statement: &str, format: i16) {
        self.bind_portal("", statement, format);
    }

    /// Binds `statement` to `portal`, results in `format`.
    fn bind_portal(&mut self, portal: &str, statement: &str, format: i16) {
        let tail = [
            &0_i16.to_be_bytes()[..],
            &0_i16.to_be_bytes(),
            &1_i16.to_be_bytes(),
            &format.to_be_bytes(),
        ]
        .concat();
        self.send(b'B', &[cstr(portal), cstr(statement), tail].concat());
    }

    fn describe(&mut self, kind: u8, name: &str) {
        self.send(b'D', &[vec![kind], cstr(name)].concat());
    }

    fn execute(&mut self) {
        self.execute_portal("", 0);
    }

    /// Executes `portal`, `max_rows` at most (`0`: all).
    fn execute_portal(&mut self, portal: &str, max_rows: i32) {
        self.send(b'E', &[cstr(portal), max_rows.to_be_bytes().to_vec()].concat());
    }

    fn sync(&mut self) -> Vec<Message> {
        self.send(b'S', &[]);
        self.until_ready()
    }
}

/// The message types of `messages`, as text (`1t T Z`, …).
fn kinds(messages: &[Message]) -> String {
    messages.iter().map(|(kind, _)| char::from(*kind)).collect()
}

/// Column count of the `RowDescription` in `messages`; `None` for `NoData`.
fn described_columns(messages: &[Message]) -> Option<i16> {
    messages
        .iter()
        .find(|(kind, _)| *kind == b'T')
        .map(|(_, body)| i16::from_be_bytes([body[0], body[1]]))
}

/// Text values of the `DataRow`s in `messages`.
fn data_rows(messages: &[Message]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter(|(kind, _)| *kind == b'D')
        .map(|(_, body)| {
            let columns = i16::from_be_bytes([body[0], body[1]]);
            let mut at = 2;
            (0..columns)
                .map(|_| {
                    let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    let len = usize::try_from(len).unwrap();
                    let value = String::from_utf8(body[at..at + len].to_vec()).unwrap();
                    at += len;
                    value
                })
                .collect()
        })
        .collect()
}

/// Command tags of the `CommandComplete`s in `messages` (`FETCH 4`, …).
fn tags(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .filter(|(kind, _)| *kind == b'C')
        .map(|(_, body)| String::from_utf8_lossy(body.strip_suffix(&[0]).unwrap_or(body)).into_owned())
        .collect()
}

/// SQLSTATE of the `ErrorResponse` in `messages`.
fn error_code(messages: &[Message]) -> Option<String> {
    let (_, body) = messages.iter().find(|(kind, _)| *kind == b'E')?;
    let mut fields = body.split(|byte| *byte == 0);
    fields
        .find(|field| field.first() == Some(&b'C'))
        .map(|field| String::from_utf8_lossy(&field[1..]).into_owned())
}

/// Rows `from..=to` of `SELECT value, value % 13`, as text.
fn value_and_mod(from: i64, to: i64) -> Vec<Vec<String>> {
    (from..=to)
        .map(|value| vec![value.to_string(), (value % 13).to_string()])
        .collect()
}

/// `FETCH` on the extended protocol (psycopg3's server-side cursors) is
/// described with its cursor's columns and returns every row, in order,
/// with the simple protocol reading on where it stopped; bound for binary
/// results (`tokio-postgres`) it is refused before it reads a row. (Found by
/// the F6 re-verification: a `FETCH` was described with no columns, so the
/// client failed on its first row after the server had consumed the rows it
/// asked for, which the next `FETCH` no longer returned.)
async fn fetch_on_the_extended_protocol(server: &Server) {
    let port = server.pg;
    tokio::task::spawn_blocking(move || {
        let mut wire = Wire::connect(port, "wire");
        assert_eq!(error_code(&wire.query("BEGIN")), None);
        let declared =
            wire.query("DECLARE cx CURSOR FOR SELECT value, value % 13 AS m FROM generate_series(1, 20)");
        assert_eq!(error_code(&declared), None, "{declared:?}");

        // Described as a statement, then as a portal, then read.
        wire.parse("f4", "FETCH 4 FROM cx");
        wire.describe(b'S', "f4");
        let described = wire.sync();
        assert_eq!(kinds(&described), "1tTZ", "{described:?}");
        assert_eq!(described_columns(&described), Some(2));
        wire.bind("f4", 0);
        wire.describe(b'P', "");
        wire.execute();
        let fetched = wire.sync();
        assert_eq!(described_columns(&fetched), Some(2), "{fetched:?}");
        assert_eq!(data_rows(&fetched), value_and_mod(1, 4));
        assert_eq!(tags(&fetched), ["FETCH 4"]);
        // Executed again, without a Describe: the next rows.
        wire.bind("f4", 0);
        wire.execute();
        assert_eq!(data_rows(&wire.sync()), value_and_mod(5, 8));
        // The simple protocol reads on from there: no row was lost.
        let rest = wire.query("FETCH ALL FROM cx");
        assert_eq!(data_rows(&rest), value_and_mod(9, 20));
        assert_eq!(tags(&rest), ["FETCH 12"]);
        let done = wire.query("COMMIT");
        assert_eq!(error_code(&done), None, "{done:?}");
    })
    .await
    .unwrap();
    server.slots_free().await;

    // Results bound in binary: refused, no row read (a held cursor, outside
    // a transaction block: the error aborts none, and the next FETCH reads
    // on).
    let client = server.connect("wire").await;
    client
        .batch_execute("DECLARE cb CURSOR WITH HOLD FOR SELECT value FROM generate_series(1, 5)")
        .await
        .unwrap();
    let error = client.query("FETCH 2 FROM cb", &[]).await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED), "{error:?}");
    assert_eq!(
        numbers(&text_rows(&client, "FETCH ALL FROM cb").await),
        [1, 2, 3, 4, 5]
    );
    client.batch_execute("CLOSE cb").await.unwrap();
    // In a transaction block, the error aborts it, as any error does.
    client
        .batch_execute("BEGIN; DECLARE cb CURSOR FOR SELECT value FROM generate_series(1, 5)")
        .await
        .unwrap();
    let error = client.query("FETCH 2 FROM cb", &[]).await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::FEATURE_NOT_SUPPORTED), "{error:?}");
    let error = client.simple_query("FETCH ALL FROM cb").await.unwrap_err();
    assert_eq!(
        error.code(),
        Some(&SqlState::IN_FAILED_SQL_TRANSACTION),
        "{error:?}"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    server.slots_free().await;
}

/// A cancel request that arrives while the connection executes nothing (a
/// late Ctrl-C, JDBC's `Statement.cancel` of another statement) does
/// nothing, as in PostgreSQL: an open cursor and a portal suspended by a
/// row limit read on, every row in order. (Found by the F6 re-verification:
/// it failed them with 57014.)
async fn cancel_request_between_messages_does_nothing(server: &Server) {
    let client = server.connect("idle").await;
    client
        .batch_execute("BEGIN; DECLARE ci CURSOR FOR SELECT value FROM generate_series(1, 100)")
        .await
        .unwrap();
    assert_eq!(numbers(&text_rows(&client, "FETCH 3 FROM ci").await), [1, 2, 3]);
    client.cancel_token().cancel_query(NoTls).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(numbers(&text_rows(&client, "FETCH 3 FROM ci").await), [4, 5, 6]);
    assert_eq!(
        numbers(&text_rows(&client, "FETCH ALL FROM ci").await),
        (7..=100).collect::<Vec<_>>()
    );
    client.batch_execute("COMMIT").await.unwrap();
    server.slots_free().await;

    let mut client = server.connect("idle").await;
    let token = client.cancel_token();
    let transaction = client.transaction().await.unwrap();
    let portal = transaction
        .bind("SELECT value FROM generate_series(1, 100)", &[])
        .await
        .unwrap();
    let mut values = Vec::new();
    let first = transaction.query_portal(&portal, 5).await.unwrap();
    values.extend(first.iter().map(|row| row.get::<_, i64>(0)));
    token.cancel_query(NoTls).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    loop {
        let rows = transaction.query_portal(&portal, 30).await.unwrap();
        if rows.is_empty() {
            break;
        }
        values.extend(rows.iter().map(|row| row.get::<_, i64>(0)));
    }
    assert_eq!(values, (1..=100).collect::<Vec<_>>());
    drop(portal);
    transaction.commit().await.unwrap();
    server.slots_free().await;
}

/// A `DECLARE … CURSOR` (psycopg2 named cursors, pandas chunked reads) is
/// one governed statement until its cursor is read to the end or closed: it
/// holds its slot, and `FETCH` takes none (the user's only slot is the
/// cursor's); fetches return every row, in order; `CLOSE`, `kill_query` and
/// its time limit stop it and return its memory to the pool. (Found by the
/// F6 audit: the `DECLARE` finished at once, and the cursor's stream then
/// lived in the connection with no slot, deadline or registry entry,
/// holding its reservations until `CLOSE`.)
#[tokio::test]
async fn declared_cursor_runs_under_governance() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    let client = server.connect("zed").await;
    let observer = server.connect("observer").await;
    client.batch_execute("BEGIN").await.unwrap();

    cursors_return_every_row(&server, &client, &observer).await;
    fetch_on_the_extended_protocol(&server).await;
    cancel_request_between_messages_does_nothing(&server).await;

    // `CLOSE` ends it and frees the slot and the memory.
    client
        .batch_execute(&format!("DECLARE c2 CURSOR FOR {STREAMS_WITH_MEMORY}"))
        .await
        .unwrap();
    assert_eq!(text_rows(&client, "FETCH 2 FROM c2").await.len(), 2);
    let held = pool_fills(&server).await;
    client.batch_execute("CLOSE c2").await.unwrap();
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0, "reservations left (held {held})");

    // `kill_query` reaches an open cursor.
    client
        .batch_execute(&format!("DECLARE c3 CURSOR FOR {STREAMS_WITH_MEMORY}"))
        .await
        .unwrap();
    assert_eq!(text_rows(&client, "FETCH 1 FROM c3").await.len(), 1);
    pool_fills(&server).await;
    let id = running_join_of(&server, "zed").await;
    let killer = server.connect("zed").await;
    killer
        .simple_query(&format!(
            "CALL system.runtime.kill_query(query_id => '{id}', message => 'cursor')"
        ))
        .await
        .unwrap();
    server.slots_free().await;
    assert_eq!(
        server.pool_drains().await,
        0,
        "reservations left after kill_query"
    );
    let error = client.simple_query("FETCH 1 FROM c3").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "Query killed. Message: cursor"
    );
    client.batch_execute("ROLLBACK").await.unwrap();

    // Past its time limit an idle cursor is stopped.
    client
        .batch_execute("SET statement_timeout = '5s'")
        .await
        .unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(&format!("DECLARE c4 CURSOR FOR {STREAMS_WITH_MEMORY}"))
        .await
        .unwrap();
    assert_eq!(text_rows(&client, "FETCH 1 FROM c4").await.len(), 1);
    let held = pool_fills(&server).await;
    tokio::time::sleep(Duration::from_secs(6)).await;
    server.slots_free().await;
    assert_eq!(
        server.pool_drains().await,
        0,
        "the cursor kept its reservations past its time limit (held {held})"
    );
    let error = client.simple_query("FETCH 1 FROM c4").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        states_of(&observer, "zed").await,
        [
            ["FINISHED", "-"],
            ["FINISHED", "-"],
            ["FINISHED", "-"],
            ["FAILED", "ADMINISTRATIVELY_KILLED"],
            ["FAILED", "EXCEEDED_TIME_LIMIT"],
        ]
    );
}

/// `user` (who may run one query) runs one at once: no statement of theirs
/// holds the slot.
async fn next_query_runs(server: &Server, user: &str) {
    let other = server.connect(user).await;
    let rows = tokio::time::timeout(Duration::from_secs(10), text_rows(&other, "SELECT 1"))
        .await
        .expect("the user's slot was not given back");
    assert_eq!(rows, [["1"]]);
}

/// A transaction's end closes its cursors and portals, as in PostgreSQL:
/// after `COMMIT`, `ROLLBACK`, a message outside a transaction block or an
/// extended-protocol `Sync` outside one, their statements are over — slot
/// and memory given back, the user's next query runs at once — and reading
/// them fails with `34000`. Only a `WITH HOLD` cursor outlives the
/// transaction that commits it, every row in order; a `DECLARE` without it
/// outside a transaction block fails with `25P01`. (Found by the F6
/// re-verification: a cursor read halfway kept its slot and 61 MB of the
/// pool after `COMMIT`, for as long as its pooled connection lived, and a
/// `FETCH` after the `COMMIT` still returned rows.)
#[tokio::test]
async fn transaction_end_closes_cursors_and_portals() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    let client = server.connect("tx").await;
    commit_and_rollback_close_cursors(&server, &client).await;
    cursors_need_a_transaction_block(&server, &client).await;
    held_cursors_outlive_their_commit(&server, &client).await;
    portals_end_with_their_transaction(&server).await;
}

async fn commit_and_rollback_close_cursors(server: &Server, client: &Client) {
    for end in ["COMMIT", "ROLLBACK"] {
        client
            .batch_execute(&format!("BEGIN; DECLARE c CURSOR FOR {STREAMS_WITH_MEMORY}"))
            .await
            .unwrap();
        assert_eq!(text_rows(client, "FETCH 2 FROM c").await.len(), 2);
        let held = pool_fills(server).await;
        assert_eq!(load(server).await, (1, 0));
        client.batch_execute(end).await.unwrap();
        server.slots_free().await;
        assert_eq!(
            server.pool_drains().await,
            0,
            "{end} left reservations (held {held})"
        );
        next_query_runs(server, "tx").await;
        let error = client.simple_query("FETCH 1 FROM c").await.unwrap_err();
        assert_eq!(
            error.code(),
            Some(&SqlState::INVALID_CURSOR_NAME),
            "{end}: {error:?}"
        );
    }
}

/// Without `WITH HOLD`, a cursor needs a transaction block, on both
/// protocols; a message of several statements is one (implicit), whose
/// cursor closes with the message.
async fn cursors_need_a_transaction_block(server: &Server, client: &Client) {
    let error = client
        .simple_query("DECLARE o CURSOR FOR SELECT 1")
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Some(&SqlState::NO_ACTIVE_SQL_TRANSACTION),
        "{error:?}"
    );
    let error = client
        .execute("DECLARE o CURSOR FOR SELECT 1", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.code(),
        Some(&SqlState::NO_ACTIVE_SQL_TRANSACTION),
        "{error:?}"
    );
    assert_eq!(
        numbers(
            &text_rows(
                client,
                "DECLARE m CURSOR FOR SELECT value FROM generate_series(1, 5); FETCH 2 FROM m"
            )
            .await
        ),
        [1, 2]
    );
    server.slots_free().await;
    let error = client.simple_query("FETCH 1 FROM m").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INVALID_CURSOR_NAME), "{error:?}");
}

/// `WITH HOLD` outlives its commit, and a later rollback (cursor moves are
/// not undone), every row arriving once, in order; declared in a
/// transaction rolled back, it is closed.
async fn held_cursors_outlive_their_commit(server: &Server, client: &Client) {
    client
        .batch_execute("BEGIN; DECLARE h CURSOR WITH HOLD FOR SELECT value FROM generate_series(1, 100)")
        .await
        .unwrap();
    let mut values = numbers(&text_rows(client, "FETCH 10 FROM h").await);
    client.batch_execute("COMMIT").await.unwrap();
    assert_eq!(load(server).await, (1, 0), "the held cursor keeps its statement");
    values.extend(numbers(&text_rows(client, "FETCH 10 FROM h").await));
    client.batch_execute("BEGIN").await.unwrap();
    values.extend(numbers(&text_rows(client, "FETCH 5 FROM h").await));
    client.batch_execute("ROLLBACK").await.unwrap();
    values.extend(numbers(&text_rows(client, "FETCH ALL FROM h").await));
    assert_eq!(values, (1..=100).collect::<Vec<_>>());
    server.slots_free().await;
    client.batch_execute("CLOSE h").await.unwrap();

    client
        .batch_execute(&format!(
            "BEGIN; DECLARE r CURSOR WITH HOLD FOR {STREAMS_WITH_MEMORY}; ROLLBACK"
        ))
        .await
        .unwrap();
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0);
    let error = client.simple_query("FETCH 1 FROM r").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::INVALID_CURSOR_NAME), "{error:?}");
}

/// A portal suspended by a row limit (JDBC's fetch size) is closed by the
/// `COMMIT` of its transaction; outside a transaction block, by the `Sync`
/// after its `Execute`, which ends its implicit transaction.
async fn portals_end_with_their_transaction(server: &Server) {
    let mut pooled = server.connect("tx").await;
    let transaction = pooled.transaction().await.unwrap();
    let portal = transaction.bind(STREAMS_WITH_MEMORY, &[]).await.unwrap();
    assert_eq!(transaction.query_portal(&portal, 2).await.unwrap().len(), 2);
    let held = pool_fills(server).await;
    transaction.commit().await.unwrap();
    server.slots_free().await;
    assert_eq!(
        server.pool_drains().await,
        0,
        "the portal kept its reservations (held {held})"
    );
    next_query_runs(server, "tx").await;
    drop(portal);

    let port = server.pg;
    let wire = tokio::task::spawn_blocking(move || {
        let mut wire = Wire::connect(port, "tx");
        wire.parse("s", STREAMS_WITH_MEMORY);
        wire.bind_portal("p", "s", 0);
        wire.execute_portal("p", 2);
        let first = wire.sync();
        assert_eq!(data_rows(&first).len(), 2, "{first:?}");
        assert!(kinds(&first).contains('s'), "suspended: {first:?}");
        wire
    })
    .await
    .unwrap();
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0, "the portal outlived its Sync");
    tokio::task::spawn_blocking(move || {
        let mut wire = wire;
        wire.execute_portal("p", 2);
        let again = wire.sync();
        assert_eq!(error_code(&again).as_deref(), Some("34000"), "{again:?}");
        assert!(data_rows(&again).is_empty());
    })
    .await
    .unwrap();
    server.slots_free().await;
}

/// What a client sees of a simple-protocol message: the text of its rows, or
/// its SQLSTATE.
type Seen = Result<Vec<Vec<String>>, String>;

async fn seen(client: &Client, sql: &str) -> Seen {
    match client.simple_query(sql).await {
        Ok(messages) => Ok(messages
            .into_iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|i| row.get(i).unwrap_or("NULL").to_owned())
                        .collect(),
                ),
                _ => None,
            })
            .collect()),
        Err(error) => Err(error
            .code()
            .map_or_else(|| error.to_string(), |code| code.code().to_owned())),
    }
}

/// Ten rows, `1` to `10`, that PostgreSQL and the engine both return in
/// order.
const TEN: &str = "SELECT x FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(x)";

/// A step of [`cursor_script`]: the values of the rows it returns, or its
/// SQLSTATE.
enum Want {
    Rows(Vec<i64>),
    Code(&'static str),
}

impl Want {
    fn none() -> Self {
        Self::Rows(Vec::new())
    }

    fn seen(&self) -> Seen {
        match self {
            Self::Rows(values) => Ok(values.iter().map(|value| vec![value.to_string()]).collect()),
            Self::Code(code) => Err((*code).to_owned()),
        }
    }
}

/// Cursors across transactions, as a client sees them, message by message.
fn cursor_script() -> Vec<(String, Want)> {
    use Want::{Code, Rows};
    let none = Want::none;
    let declare = |name: &str, how: &str| format!("DECLARE {name} {how} FOR {TEN}");
    vec![
        ("BEGIN".into(), none()),
        (declare("c", "CURSOR"), none()),
        // FETCH 0 reads the row the cursor stands on again: none before
        // the first row and after the last, never every row.
        ("FETCH 0 FROM c".into(), none()),
        ("FETCH 2 FROM c".into(), Rows(vec![1, 2])),
        ("FETCH 0 FROM c".into(), Rows(vec![2])),
        ("FETCH FORWARD 0 FROM c".into(), Rows(vec![2])),
        ("FETCH RELATIVE 0 FROM c".into(), Rows(vec![2])),
        ("FETCH NEXT FROM c".into(), Rows(vec![3])),
        ("FETCH 3 FROM c".into(), Rows(vec![4, 5, 6])),
        ("FETCH 0 FROM c".into(), Rows(vec![6])),
        ("FETCH ALL FROM c".into(), Rows((7..=10).collect())),
        ("FETCH 0 FROM c".into(), none()),
        ("FETCH 1 FROM c".into(), none()),
        // COMMIT closes it.
        ("COMMIT".into(), none()),
        ("FETCH 1 FROM c".into(), Code("34000")),
        // Among other statements of a message, too.
        (
            format!("BEGIN; {}; FETCH 10 FROM e", declare("e", "CURSOR")),
            Rows((1..=10).collect()),
        ),
        (
            "FETCH 0 FROM e; FETCH 1 FROM e; FETCH 0 FROM e".into(),
            Rows(vec![10]),
        ),
        ("ROLLBACK".into(), none()),
        ("FETCH 1 FROM e".into(), Code("34000")),
        // NO SCROLL: the current row cannot be read again.
        (format!("BEGIN; {}", declare("n", "NO SCROLL CURSOR")), none()),
        ("FETCH 0 FROM n".into(), none()),
        ("FETCH 1 FROM n".into(), Rows(vec![1])),
        ("FETCH 0 FROM n".into(), Code("55000")),
        ("ROLLBACK".into(), none()),
        // What the SQL parser does not read, as PostgreSQL reads it, and at
        // once: the cursor holds the user's only slot.
        (format!("BEGIN; {}", declare("p", "NO SCROLL CURSOR")), none()),
        ("FETCH FROM p".into(), Rows(vec![1])),
        ("FETCH p".into(), Rows(vec![2])),
        ("FETCH NEXT p".into(), Rows(vec![3])),
        ("FETCH FORWARD FROM p".into(), Rows(vec![4])),
        ("FETCH 2 p".into(), Rows(vec![5, 6])),
        ("FETCH BACKWARD -2 FROM p".into(), Rows(vec![7, 8])),
        ("FETCH -1 FROM p".into(), Code("55000")),
        ("ROLLBACK".into(), none()),
        (format!("BEGIN; {}", declare("q", "NO SCROLL CURSOR")), none()),
        ("FETCH FORWARD -1 FROM q".into(), Code("55000")),
        ("ROLLBACK".into(), none()),
        (format!("BEGIN; {}", declare("q", "NO SCROLL CURSOR")), none()),
        ("FETCH 1 FROM q".into(), Rows(vec![1])),
        ("FETCH BACKWARD 0 FROM q".into(), Code("55000")),
        ("ROLLBACK".into(), none()),
        // An error aborts the transaction: only its end runs.
        (format!("BEGIN; {}", declare("f", "NO SCROLL CURSOR")), none()),
        ("FETCH 1 FROM f".into(), Rows(vec![1])),
        ("FETCH -1 FROM f".into(), Code("55000")),
        ("FETCH 1 FROM f".into(), Code("25P02")),
        ("CLOSE f".into(), Code("25P02")),
        ("SELECT 1".into(), Code("25P02")),
        ("COMMIT".into(), none()),
        ("FETCH 1 FROM f".into(), Code("34000")),
        // Names, CLOSE and INTO as PostgreSQL answers them.
        (format!("BEGIN; {}", declare("d", "CURSOR")), none()),
        (declare("d", "CURSOR"), Code("42P03")),
        ("ROLLBACK".into(), none()),
        ("CLOSE nothing".into(), Code("34000")),
        (
            format!("BEGIN; {}; {}", declare("a1", "CURSOR"), declare("a2", "CURSOR")),
            none(),
        ),
        ("CLOSE ALL".into(), none()),
        ("FETCH 1 FROM a2".into(), Code("34000")),
        ("ROLLBACK".into(), none()),
        (format!("BEGIN; {}", declare("t", "CURSOR")), none()),
        ("FETCH 1 FROM t INTO x".into(), Code("42601")),
        ("ROLLBACK".into(), none()),
        // A count that is not a whole number.
        (format!("BEGIN; {}", declare("i", "CURSOR")), none()),
        ("FETCH 1.5 FROM i".into(), Code("42601")),
        ("ROLLBACK".into(), none()),
        // Outside a transaction block: refused, unless the message holds
        // other statements (an implicit block, closed with the message).
        (declare("o", "CURSOR"), Code("25P01")),
        (
            format!("{}; FETCH 2 FROM m", declare("m", "CURSOR")),
            Rows(vec![1, 2]),
        ),
        ("FETCH 1 FROM m".into(), Code("34000")),
        // WITH HOLD outlives its commit and later rollbacks.
        (declare("h", "CURSOR WITH HOLD"), none()),
        ("FETCH 2 FROM h".into(), Rows(vec![1, 2])),
        ("BEGIN".into(), none()),
        ("FETCH 1 FROM h".into(), Rows(vec![3])),
        ("ROLLBACK".into(), none()),
        ("FETCH 0 FROM h".into(), Rows(vec![3])),
        ("FETCH ALL FROM h".into(), Rows((4..=10).collect())),
        ("CLOSE h".into(), none()),
        ("FETCH 1 FROM h".into(), Code("34000")),
        (
            format!("BEGIN; {}; ROLLBACK", declare("r", "CURSOR WITH HOLD")),
            none(),
        ),
        ("FETCH 1 FROM r".into(), Code("34000")),
    ]
}

/// A step of a wire script: a simple-protocol message, or one statement on
/// the extended protocol (`Parse`, `Bind` with results in text, `Execute`,
/// `Sync`).
#[derive(Clone, Debug)]
enum Step {
    Simple(String),
    Extended(String),
}

/// What a client sees of one step: its command tags, the text of its rows
/// and its SQLSTATE.
type Exchange = (Vec<String>, Vec<Vec<String>>, Option<String>);

/// Runs `steps` on a new connection to a PostgreSQL server.
async fn exchanged(
    host: String,
    port: u16,
    user: &'static str,
    database: &'static str,
    steps: &[Step],
) -> Vec<Exchange> {
    let steps = steps.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut wire = Wire::connect_to(&host, port, user, database);
        steps
            .iter()
            .map(|step| {
                let messages = match step {
                    Step::Simple(sql) => wire.query(sql),
                    Step::Extended(sql) => {
                        wire.parse("", sql);
                        wire.bind("", 0);
                        wire.execute();
                        wire.sync()
                    }
                };
                (tags(&messages), data_rows(&messages), error_code(&messages))
            })
            .collect()
    })
    .await
    .unwrap()
}

/// Wire scripts, each with what every step answers.
fn extended_scripts() -> Vec<(Vec<Step>, Vec<Exchange>)> {
    use Step::{Extended, Simple};
    let ok = |tag: &str| (vec![tag.to_owned()], Vec::new(), None);
    let rows = |tag: &str, values: &[i64]| {
        (
            vec![tag.to_owned()],
            values.iter().map(|v| vec![v.to_string()]).collect(),
            None,
        )
    };
    let failed = |code: &str| (Vec::new(), Vec::new(), Some(code.to_owned()));
    let declare = |name: &str, how: &str| format!("DECLARE {name} {how} FOR {TEN}");
    vec![
        // In a failed transaction block, a FETCH and a CLOSE sent on the
        // extended protocol fail with 25P02 too, before reading the cursor.
        (
            vec![
                Simple("BEGIN".into()),
                Simple(declare("a", "NO SCROLL CURSOR")),
                Extended("FETCH 1 FROM a".into()),
                Extended("FETCH 1.5 FROM a".into()),
                Extended("FETCH 1 FROM a".into()),
                Extended("CLOSE a".into()),
                Extended("COMMIT".into()),
                Simple("FETCH 1 FROM a".into()),
            ],
            vec![
                ok("BEGIN"),
                ok("DECLARE CURSOR"),
                rows("FETCH 1", &[1]),
                failed("42601"),
                failed("25P02"),
                failed("25P02"),
                ok("ROLLBACK"),
                failed("34000"),
            ],
        ),
        // All on the extended protocol: the COMMIT of a failed transaction
        // block rolls it back, closing the held cursor it declared.
        (
            vec![
                Extended("BEGIN".into()),
                Extended(declare("h", "NO SCROLL CURSOR WITH HOLD")),
                Extended("FETCH 1.5 FROM h".into()),
                Extended("COMMIT".into()),
                Extended("FETCH 1 FROM h".into()),
            ],
            vec![
                ok("BEGIN"),
                ok("DECLARE CURSOR"),
                failed("42601"),
                ok("ROLLBACK"),
                failed("34000"),
            ],
        ),
        // Committed, it is read on, on either protocol.
        (
            vec![
                Extended("BEGIN".into()),
                Extended(declare("h", "NO SCROLL CURSOR WITH HOLD")),
                Extended("COMMIT".into()),
                Extended("FETCH 2 FROM h".into()),
                Simple("FETCH 1 FROM h".into()),
                Extended("CLOSE h".into()),
            ],
            vec![
                ok("BEGIN"),
                ok("DECLARE CURSOR"),
                ok("COMMIT"),
                rows("FETCH 2", &[1, 2]),
                rows("FETCH 1", &[3]),
                ok("CLOSE CURSOR"),
            ],
        ),
    ]
}

/// Cursors answer as PostgreSQL's (17) do, statement by statement: the
/// same rows, or the same SQLSTATE. Both are checked against the answers
/// written down, so neither side is taken on trust. (Found by the F6
/// re-verification: `FETCH 0` returned every row left and emptied the
/// cursor; cursors survived `COMMIT`.)
#[tokio::test]
async fn cursors_answer_like_postgresql() {
    let reference = Postgres::default()
        .with_host_auth()
        .with_tag("17-alpine")
        .start()
        .await
        .unwrap();
    let host = std::env::var("TESTCONTAINERS_HOST_OVERRIDE").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = reference.get_host_port_ipv4(5432).await.unwrap();
    let (postgres, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    let engine = server.connect("pgcursors").await;

    for (sql, want) in cursor_script() {
        let want = want.seen();
        assert_eq!(seen(&postgres, &sql).await, want, "PostgreSQL: {sql}");
        // Well before the queue's timeout (60 s): no step waits for a slot.
        let answered = tokio::time::timeout(Duration::from_secs(20), seen(&engine, &sql))
            .await
            .unwrap_or_else(|_| panic!("engine: {sql} did not answer at once"));
        assert_eq!(answered, want, "engine: {sql}");
    }
    // MOVE is not supported: refused at once, not queued. A BINARY cursor
    // neither: its rows would be read as binary, and are text.
    assert_eq!(seen(&engine, "MOVE 1 IN c").await, Err("0A000".to_owned()));
    let binary = format!("BEGIN; DECLARE b BINARY CURSOR FOR {TEN}");
    assert_eq!(seen(&engine, &binary).await, Err("0A000".to_owned()));
    assert_eq!(seen(&engine, "ROLLBACK").await, Ok(Vec::new()));
    // FETCH tags and rows on both protocols, message by message.
    let exchanges = |host: String, port: u16, user: &'static str, database: &'static str| {
        tokio::task::spawn_blocking(move || {
            let mut wire = Wire::connect_to(&host, port, user, database);
            let mut seen = Vec::new();
            let declared = wire.query(&format!("BEGIN; DECLARE w CURSOR FOR {TEN}"));
            assert_eq!(error_code(&declared), None, "{declared:?}");
            seen.push(wire.query("FETCH 2 FROM w"));
            wire.parse("", "FETCH 3 FROM w");
            wire.bind("", 0);
            wire.describe(b'P', "");
            wire.execute();
            seen.push(wire.sync());
            seen.push(wire.query("FETCH 0 FROM w"));
            seen.push(wire.query("FETCH ALL FROM w"));
            seen.push(wire.query("CLOSE w"));
            seen.push(wire.query(&format!("DECLARE v CURSOR FOR {TEN}; CLOSE ALL")));
            seen.push(wire.query("COMMIT"));
            seen.iter()
                .map(|messages| (tags(messages), data_rows(messages)))
                .collect::<Vec<_>>()
        })
    };
    let reference = exchanges(host.clone(), port, "postgres", "postgres")
        .await
        .unwrap();
    let answered = exchanges("127.0.0.1".to_owned(), server.pg, "pgcursors", "codelake")
        .await
        .unwrap();
    let text = |values: &[i64]| -> Vec<Vec<String>> { values.iter().map(|v| vec![v.to_string()]).collect() };
    let want = vec![
        (vec!["FETCH 2".to_owned()], text(&[1, 2])),
        (vec!["FETCH 3".to_owned()], text(&[3, 4, 5])),
        (vec!["FETCH 1".to_owned()], text(&[5])),
        (vec!["FETCH 5".to_owned()], text(&[6, 7, 8, 9, 10])),
        (vec!["CLOSE CURSOR".to_owned()], text(&[])),
        (
            vec!["DECLARE CURSOR".to_owned(), "CLOSE CURSOR ALL".to_owned()],
            text(&[]),
        ),
        (vec!["COMMIT".to_owned()], text(&[])),
    ];
    assert_eq!(reference, want, "PostgreSQL");
    assert_eq!(answered, want, "engine");
    for (steps, want) in extended_scripts() {
        let reference = exchanged(host.clone(), port, "postgres", "postgres", &steps).await;
        assert_eq!(reference, want, "PostgreSQL: {steps:?}");
        let answered = exchanged("127.0.0.1".to_owned(), server.pg, "pgcursors", "codelake", &steps).await;
        assert_eq!(answered, want, "engine: {steps:?}");
    }
    // On the extended protocol too, a DECLARE needs a transaction block.
    let declare = format!("DECLARE x CURSOR FOR {TEN}");
    for client in [&postgres, &engine] {
        let error = client.execute(&declare, &[]).await.unwrap_err();
        assert_eq!(
            error.code(),
            Some(&SqlState::NO_ACTIVE_SQL_TRANSACTION),
            "{error:?}"
        );
    }
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0);
}

/// A cancel request stops a statement executing on the extended protocol
/// (JDBC, psycopg3, `tokio-postgres`' `query`), not only on the simple one:
/// SQLSTATE `57014`, the pool back at zero, the connection still usable.
/// Both a query and a `FETCH` of a cursor are stopped while they execute.
/// (Found by the F6 re-verification: no test covered the `Execute` path,
/// where the connection is marked as executing for cancel requests to
/// reach it; a regression there passed green.)
#[tokio::test]
async fn cancel_requests_reach_the_extended_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;

    let client = server.connect("ext-cancel").await;
    let token = client.cancel_token();
    let running = tokio::spawn(async move {
        let outcome = client.query(HOLDS_MEMORY, &[]).await.map(|_| ());
        (client, outcome)
    });
    let held = pool_fills(&server).await;
    token.cancel_query(NoTls).await.unwrap();
    let (client, outcome) = tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the canceled Execute ended")
        .unwrap();
    let error = outcome.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "canceling statement due to user request"
    );
    assert_eq!(server.pool_drains().await, 0, "reservations left (held {held})");
    let rows = client.query("SELECT CAST(6 * 7 AS BIGINT)", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 42);
    server.slots_free().await;

    // A FETCH executing on the extended protocol: the cursor's join runs
    // under it.
    let port = server.pg;
    let (key_sent, key) = tokio::sync::oneshot::channel();
    let fetching = tokio::task::spawn_blocking(move || {
        let mut wire = Wire::connect(port, "ext-cancel");
        assert_eq!(error_code(&wire.query("BEGIN")), None);
        let declared = wire.query(&format!("DECLARE cm CURSOR FOR {HOLDS_MEMORY}"));
        assert_eq!(error_code(&declared), None, "{declared:?}");
        key_sent
            .send(wire.1.clone())
            .unwrap_or_else(|_| panic!("key not sent"));
        wire.parse("", "FETCH 1 FROM cm");
        wire.bind("", 0);
        wire.execute();
        let fetched = wire.sync();
        (wire, fetched)
    });
    let key = key.await.unwrap();
    let held = pool_fills(&server).await;
    tokio::task::spawn_blocking(move || key.cancel(port))
        .await
        .unwrap();
    let (mut wire, fetched) = tokio::time::timeout(Duration::from_secs(10), fetching)
        .await
        .expect("the canceled FETCH ended")
        .unwrap();
    assert_eq!(error_code(&fetched).as_deref(), Some("57014"), "{fetched:?}");
    assert!(data_rows(&fetched).is_empty());
    assert_eq!(server.pool_drains().await, 0, "reservations left (held {held})");
    // The transaction is aborted, as in PostgreSQL: the next FETCH fails
    // (it returned no row and no error).
    let (next, ended) = tokio::task::spawn_blocking(move || {
        let next = wire.query("FETCH 1 FROM cm");
        (next, wire.query("ROLLBACK"))
    })
    .await
    .unwrap();
    assert_eq!(error_code(&next).as_deref(), Some("25P02"), "{next:?}");
    assert!(data_rows(&next).is_empty());
    assert_eq!(error_code(&ended), None, "{ended:?}");
    server.slots_free().await;
    canceled_fetches_leave_their_cursor_unreadable(&server).await;
}

/// A `FETCH` canceled while it executes (simple protocol): in a transaction
/// block the transaction is aborted (`25P02` for the next statement); a
/// held cursor, outside one, cannot be read any more (`55000`), as
/// PostgreSQL marks a portal that failed.
async fn canceled_fetches_leave_their_cursor_unreadable(server: &Server) {
    for held in [false, true] {
        let client = server.connect("ext-cancel").await;
        let declare = if held {
            format!("DECLARE cf CURSOR WITH HOLD FOR {HOLDS_MEMORY}")
        } else {
            format!("BEGIN; DECLARE cf CURSOR FOR {HOLDS_MEMORY}")
        };
        client.batch_execute(&declare).await.unwrap();
        let token = client.cancel_token();
        let running = tokio::spawn(async move {
            let outcome = client.simple_query("FETCH 1 FROM cf").await.map(|_| ());
            (client, outcome)
        });
        let reserved = pool_fills(server).await;
        token.cancel_query(NoTls).await.unwrap();
        let (client, outcome) = tokio::time::timeout(Duration::from_secs(10), running)
            .await
            .expect("the canceled FETCH ended")
            .unwrap();
        let error = outcome.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
        assert_eq!(
            server.pool_drains().await,
            0,
            "reservations left (held {reserved})"
        );
        let error = client.simple_query("FETCH 1 FROM cf").await.unwrap_err();
        let (want, end) = if held {
            (SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE, "CLOSE cf")
        } else {
            (SqlState::IN_FAILED_SQL_TRANSACTION, "ROLLBACK")
        };
        assert_eq!(error.code(), Some(&want), "held {held}: {error:?}");
        client.batch_execute(end).await.unwrap();
        server.slots_free().await;
    }
}

/// A join that streams rows (2 M, as its probe side advances) while its
/// build side stays reserved in the pool.
const STREAMS_WITH_MEMORY: &str = "SELECT a.value FROM generate_series(1, 2000000) a \
                                   JOIN generate_series(1, 3000000000) b ON a.value = b.value";

/// The extended protocol (JDBC, psycopg3, `tokio-postgres`' `query`) sends
/// rows as they are produced: a client that has read nothing keeps its
/// statement `RUNNING` (slot held, time limit counting) instead of the whole
/// result waiting in server memory outside the pool, and every row arrives,
/// in order. Past its time limit a statement whose client stopped reading is
/// stopped: `57014`, slot and pool given back. (`pgwire`'s default handler
/// collected the rows of an `Execute` without a row limit before sending
/// any; found by the F6 audit.)
#[tokio::test]
async fn extended_protocol_streams_rows_under_governance() {
    use futures::StreamExt;

    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    let client = server.connect("ext").await;
    let observer = server.connect("observer").await;
    let rows = client
        .query_raw(
            "SELECT value FROM generate_series(1, 3000000)",
            std::iter::empty::<i64>(),
        )
        .await
        .unwrap();
    let mut rows = std::pin::pin!(rows);
    // The client reads nothing for a while: the statement waits for it.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(states_of(&observer, "ext").await, [["RUNNING", "-"]]);
    assert_eq!(load(&server).await, (1, 0), "its slot is held");
    let (mut count, mut sum, mut previous) = (0_i64, 0_i64, 0_i64);
    while let Some(row) = rows.next().await {
        let value: i64 = row.unwrap().get(0);
        assert_eq!(value, previous + 1, "rows out of order");
        previous = value;
        count += 1;
        sum += value;
    }
    assert_eq!((count, sum), (3_000_000, 3_000_000 * 3_000_001 / 2));
    assert_eq!(rows.rows_affected(), Some(3_000_000));
    assert_eq!(states_of(&observer, "ext").await, [["FINISHED", "-"]]);

    // The time limit covers the sending: a client that stops reading is cut
    // off at its deadline and the join's memory returns to the pool.
    client
        .batch_execute("SET statement_timeout = '2s'")
        .await
        .unwrap();
    let rows = client
        .query_raw(STREAMS_WITH_MEMORY, std::iter::empty::<i64>())
        .await
        .unwrap();
    let mut rows = std::pin::pin!(rows);
    let held = pool_fills(&server).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0, "reservations left (held {held})");
    let mut read = 0_u64;
    let error = loop {
        match rows.next().await {
            Some(Ok(_)) => read += 1,
            Some(Err(error)) => break error,
            None => panic!("all rows arrived after the time limit ({read})"),
        }
    };
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED), "{error:?}");
    assert!(read < 2_000_000, "{read} rows read");
    assert_eq!(
        states_of(&observer, "ext").await,
        [["FINISHED", "-"], ["FAILED", "EXCEEDED_TIME_LIMIT"]]
    );
}

/// A `FETCH` among other statements of a message reads its rows before the
/// next statement runs, and holds them until they are sent: in the engine's
/// memory pool, every row arriving in order; past what the pool gives, the
/// `FETCH` fails with `53200` (`out_of_memory`) and its cursor cannot be read
/// again, the rows never held outside the pool. (Found by the F6
/// re-verification: `FETCH ALL FROM c; CLOSE c` over 6 M rows took the
/// server from 114 MB to 815 MB of RSS, with a 256 MiB pool.)
#[tokio::test]
async fn fetch_among_other_statements_holds_its_rows_in_the_pool() {
    const ROWS: i64 = 400_000;
    let dir = tempfile::tempdir().unwrap();
    let server = start_server(dir.path());
    server.wait_ready().await;
    let port = server.pg;
    let (sent, fetch_sent) = tokio::sync::oneshot::channel();
    let (read, go_read) = std::sync::mpsc::channel::<()>();
    let fetching = tokio::task::spawn_blocking(move || {
        let mut wire = Wire::connect(port, "multi");
        let declared = wire.query(&format!(
            "BEGIN; DECLARE big CURSOR FOR SELECT value FROM generate_series(1, {ROWS})"
        ));
        assert_eq!(error_code(&declared), None, "{declared:?}");
        // Sent, not read: the rows wait for this client.
        wire.send(b'Q', &cstr("FETCH ALL FROM big; CLOSE big"));
        sent.send(()).unwrap();
        go_read.recv().unwrap();
        let fetched = wire.until_ready();
        let ended = wire.query("COMMIT");
        (fetched, ended)
    });
    fetch_sent.await.unwrap();
    // Held in the pool while they wait: at least a row's bookkeeping each.
    let deadline = Instant::now() + Duration::from_secs(20);
    let held = loop {
        let reserved = server.reserved().await;
        if reserved >= u64::try_from(ROWS).unwrap() * 32 {
            break reserved;
        }
        assert!(
            Instant::now() < deadline,
            "the rows waiting for the client are not in the pool ({reserved} bytes)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    read.send(()).unwrap();
    let (fetched, ended) = fetching.await.unwrap();
    assert_eq!(error_code(&fetched), None);
    assert_eq!(
        numbers(&data_rows(&fetched)),
        (1..=ROWS).collect::<Vec<_>>(),
        "held {held} bytes"
    );
    assert_eq!(
        tags(&fetched),
        [format!("FETCH {ROWS}"), "CLOSE CURSOR".to_owned()]
    );
    assert_eq!(error_code(&ended), None, "{ended:?}");
    server.slots_free().await;
    assert_eq!(server.pool_drains().await, 0, "rows kept in the pool once sent");

    // More rows than the pool holds (256 MiB): refused, not held outside it.
    let client = server.connect("multi").await;
    client
        .batch_execute("DECLARE h CURSOR WITH HOLD FOR SELECT value FROM generate_series(1, 100000000)")
        .await
        .unwrap();
    let (outcome, peak) = peak_while(&server, client.simple_query("FETCH ALL FROM h; CLOSE h")).await;
    let error = outcome.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::OUT_OF_MEMORY), "{error:?}");
    assert!(
        peak > 64 << 20,
        "the rows were not held in the pool ({peak} bytes)"
    );
    assert_eq!(server.pool_drains().await, 0, "reservations left (peak {peak})");
    // The rows it read are gone: the cursor cannot be read on.
    let error = client.simple_query("FETCH 1 FROM h").await.unwrap_err();
    assert_eq!(
        error.code(),
        Some(&SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE),
        "{error:?}"
    );
    client.batch_execute("CLOSE h").await.unwrap();
    server.slots_free().await;
    let observer = server.connect("observer").await;
    let states = states_of(&observer, "multi").await;
    assert_eq!(
        states.last().map(Vec::as_slice),
        Some(&["FAILED".to_owned(), "EXCEEDED_LOCAL_MEMORY_LIMIT".to_owned()][..]),
        "{states:?}"
    );
}
