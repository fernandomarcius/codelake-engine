//! Hard memory ceiling of the real `codelake` binary (ADR 0003), end to end:
//! clients that read slowly, abandon or cancel large queries over the Trino
//! protocol and the PostgreSQL wire, and operators (sort, hash join,
//! aggregation) larger than the pool. Runs in the integration lane.
//!
//! Measure: the server process's peak resident set, Linux `VmHWM` from
//! `/proc/<pid>/status`, reset through `/proc/<pid>/clear_refs` (`5`) after
//! warm-up. A peak only grows, so unlike a current-RSS reading it does not
//! depend on the allocator returning pages, and no other test moves it (the
//! server is its own process).
//!
//! Budget: ADR 0003 sets `memory.limit` to ~75% of the container limit, the
//! other 25% (a third of the limit) for the allocator, connection buffers and
//! caches. The work may therefore grow the process by at most
//! `limit + limit / 3` (128 MiB → 170.7 MiB) over its warmed-up idle footprint,
//! which is measured and reported but not charged (it depends on the number of
//! cores: one runtime worker per core). Past that, the only acceptable answer
//! is a query failing with `EXCEEDED_LOCAL_MEMORY_LIMIT`, never the process
//! growing.

#![cfg(target_os = "linux")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test crate"
)]

use std::io::Write;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BIN: &str = env!("CARGO_BIN_EXE_codelake");
const MIB: u64 = 1 << 20;
const LIMIT: u64 = 128 * MIB;
/// ADR 0003: `memory.limit` is ~75% of the container limit; growth allowed
/// over the idle footprint.
const BUDGET: u64 = LIMIT + LIMIT / 3;

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
    trino: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (pg, http, trino) = (free_port(), free_port(), free_port());
        let path = dir.path().join("codelake.toml");
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
header_prefix = "X-Trino-"
reported_version = "431"
client_timeout_secs = 2

[execution]
target_partitions = 4

[memory]
limit = "128MiB"
spill_dir = "{spill}"
spill_limit = "4GiB"
metadata_cache_limit = "8MiB"
"#,
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
            pg,
            http,
            trino,
            _dir: dir,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        while reqwest::get(format!("http://127.0.0.1:{http}/livez"))
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "server did not start");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        server
    }

    fn status(&self, key: &str) -> u64 {
        let status = std::fs::read_to_string(format!("/proc/{}/status", self.child.id())).unwrap();
        let line = status
            .lines()
            .find(|l| l.starts_with(key))
            .unwrap_or_else(|| panic!("{key} missing"));
        let kib: u64 = line
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap();
        kib * 1024
    }

    /// Warms the server up, then restarts the peak from the current RSS.
    async fn baseline(&self) -> u64 {
        let trino = Trino::new(self.trino);
        let (outcome, _) = trino.run("SELECT count(*) FROM generate_series(1, 1000)").await;
        assert_eq!(outcome.unwrap()[0][0], 1000);
        std::fs::write(format!("/proc/{}/clear_refs", self.child.id()), "5").unwrap();
        self.status("VmRSS:")
    }

    fn peak(&self) -> u64 {
        self.status("VmHWM:")
    }

    async fn health(&self) -> Value {
        let body = reqwest::get(format!("http://127.0.0.1:{}/healthz", self.http))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// Waits (at most 20 s) until nothing is reserved in the pool.
    async fn drained(&self) -> u64 {
        for _ in 0..200 {
            let reserved = self.health().await["memory"]["reserved_bytes"].as_u64().unwrap();
            if reserved == 0 {
                return 0;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.health().await["memory"]["reserved_bytes"].as_u64().unwrap()
    }

    fn assert_alive(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "server exited: {:?}",
            self.child.try_wait()
        );
    }
}

fn mib(bytes: u64) -> String {
    format!("{}.{} MiB", bytes / MIB, bytes % MIB * 10 / MIB)
}

/// A minimal Trino protocol client.
#[derive(Clone)]
struct Trino {
    http: reqwest::Client,
    base: String,
}

impl Trino {
    fn new(port: u16) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: format!("http://127.0.0.1:{port}"),
        }
    }

    async fn json(response: reqwest::Response) -> Value {
        let body = response.bytes().await.unwrap();
        serde_json::from_slice(&body).unwrap_or(Value::Null)
    }

    async fn submit(&self, sql: &str) -> Value {
        let response = self
            .http
            .post(format!("{}/v1/statement", self.base))
            .header("x-trino-user", "tester")
            .body(sql.to_owned())
            .send()
            .await
            .unwrap();
        Self::json(response).await
    }

    async fn get(&self, uri: &str) -> Value {
        Self::json(self.http.get(uri).send().await.unwrap()).await
    }

    async fn delete(&self, uri: &str) {
        let _ = self.http.delete(uri).send().await.unwrap();
    }

    /// Runs `sql` to the end: its rows, or its error; and the pages fetched.
    async fn run(&self, sql: &str) -> (Result<Vec<Vec<Value>>, Value>, usize) {
        let mut page = self.submit(sql).await;
        let mut rows = Vec::new();
        let mut pages = 0;
        loop {
            if page["error"].is_object() {
                return (Err(page["error"].clone()), pages);
            }
            for row in page["data"].as_array().into_iter().flatten() {
                rows.push(row.as_array().unwrap().clone());
            }
            let Some(next) = page["nextUri"].as_str().map(str::to_owned) else {
                return (Ok(rows), pages);
            };
            page = self.get(&next).await;
            pages += 1;
        }
    }
}

/// ~1.2 GiB of 4 KiB rows: 32 MiB per 8192-row batch.
const WIDE: &str = "SELECT value, lpad('', 4096, 'x') AS s FROM generate_series(1, 300000)";
const WIDE_ROWS: i64 = 300_000;
const WIDE_BYTES: usize = 4096;

fn check_wide_rows(page: &Value) -> usize {
    let mut n = 0;
    for row in page["data"].as_array().into_iter().flatten() {
        let value = row[0].as_i64().unwrap();
        assert!((1..=WIDE_ROWS).contains(&value), "{value}");
        assert_eq!(row[1].as_str().map(str::len), Some(WIDE_BYTES));
        n += 1;
    }
    n
}

/// The only error a memory-bound client may see.
fn assert_memory_error(error: &Value) {
    assert_eq!(
        error["errorName"], "EXCEEDED_LOCAL_MEMORY_LIMIT",
        "unexpected failure: {error}"
    );
}

/// Twelve wide queries (~1.2 GiB of rows each) at once over the Trino
/// protocol: 4 clients read a page every 250 ms for 6 s then cancel, 4 read one
/// page and abandon (the server cancels them after `client_timeout_secs` = 2),
/// 4 read one page and cancel. Peak within the ADR 0003 budget; every page
/// holds right rows or `EXCEEDED_LOCAL_MEMORY_LIMIT`; nothing stays reserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trino_slow_abandoned_and_canceled_clients_stay_under_the_ceiling() {
    let mut server = Server::start().await;
    let baseline = server.baseline().await;
    let trino = Trino::new(server.trino);
    let mut tasks = Vec::new();
    for i in 0..12 {
        let trino = trino.clone();
        tasks.push(tokio::spawn(async move {
            let first = trino.submit(WIDE).await;
            let mut next = first["nextUri"].as_str().unwrap().to_owned();
            let mut rows = 0;
            let mut refused = 0;
            let started = Instant::now();
            loop {
                let page = trino.get(&next).await;
                if page["error"].is_object() {
                    assert_memory_error(&page["error"]);
                    refused += 1;
                    break;
                }
                rows += check_wide_rows(&page);
                let Some(uri) = page["nextUri"].as_str() else {
                    break;
                };
                next = uri.to_owned();
                match i % 3 {
                    // Slow reader, then cancels.
                    0 if started.elapsed() < Duration::from_secs(6) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    // Abandons: stops polling once it has rows, never cancels.
                    1 if rows > 0 => break,
                    // Reads one page, then cancels.
                    2 if rows > 0 => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        trino.delete(&next).await;
                        break;
                    }
                    0 => {
                        trino.delete(&next).await;
                        break;
                    }
                    _ => {}
                }
            }
            (rows, refused)
        }));
    }
    let mut refused = 0;
    let mut rows = 0;
    for task in tasks {
        let (r, f) = task.await.unwrap();
        rows += r;
        refused += f;
    }
    // Sweeps run every 5 s; abandoned queries are canceled 2 s after their last poll.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let reserved = server.drained().await;
    let peak = server.peak();
    server.assert_alive();
    eprintln!(
        "trino: idle {}, peak {}, growth {} (budget {}), rows read {rows}, memory errors {refused}, reserved after {}",
        mib(baseline),
        mib(peak),
        mib(peak.saturating_sub(baseline)),
        mib(BUDGET),
        mib(reserved)
    );
    assert!(rows > 0);
    assert_eq!(reserved, 0, "reservations left after every query ended");
    assert!(
        peak.saturating_sub(baseline) <= BUDGET,
        "server grew by {} over its idle {} with memory.limit = {} (budget {})",
        mib(peak.saturating_sub(baseline)),
        mib(baseline),
        mib(LIMIT),
        mib(BUDGET)
    );
}

/// A raw PostgreSQL simple-query client that controls how fast it reads, so
/// the server meets real TCP backpressure (`tokio-postgres` reads everything
/// into its own buffers).
struct Pg {
    stream: TcpStream,
}

impl Pg {
    async fn connect(port: u16) -> Self {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&196_608_i32.to_be_bytes());
        for text in ["user", "codelake", "database", "codelake", ""] {
            body.extend_from_slice(text.as_bytes());
            body.push(0);
        }
        let len = i32::try_from(body.len() + 4).unwrap();
        stream.write_all(&len.to_be_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
        let mut pg = Self { stream };
        while pg.message().await.0 != b'Z' {}
        pg
    }

    async fn message(&mut self) -> (u8, Vec<u8>) {
        let tag = self.stream.read_u8().await.unwrap();
        let len = self.stream.read_i32().await.unwrap();
        let mut body = vec![0; usize::try_from(len - 4).unwrap()];
        self.stream.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    async fn query(&mut self, sql: &str) {
        let len = i32::try_from(sql.len() + 5).unwrap();
        self.stream.write_u8(b'Q').await.unwrap();
        self.stream.write_all(&len.to_be_bytes()).await.unwrap();
        self.stream.write_all(sql.as_bytes()).await.unwrap();
        self.stream.write_u8(0).await.unwrap();
    }
}

/// `(value, payload length)` of one `DataRow` of the wide query.
fn wide_row(body: &[u8]) -> (i64, usize) {
    let mut at = 2;
    let mut columns = Vec::new();
    for _ in 0..2 {
        let len = usize::try_from(i32::from_be_bytes(body[at..at + 4].try_into().unwrap())).unwrap();
        at += 4;
        columns.push(&body[at..at + len]);
        at += len;
    }
    let value = std::str::from_utf8(columns[0]).unwrap().parse().unwrap();
    assert!(columns[1].iter().all(|b| *b == b'x'));
    (value, columns[1].len())
}

/// Eight PostgreSQL connections run the wide query: 4 read 2000 rows every
/// 300 ms for 6 s then disconnect, 4 read one row and stop reading with the
/// connection open. Peak within the ADR 0003 budget, right rows, and the
/// memory is back once the connections close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_wire_slow_and_stalled_clients_stay_under_the_ceiling() {
    let mut server = Server::start().await;
    let baseline = server.baseline().await;
    let mut tasks = Vec::new();
    for i in 0..8 {
        let port = server.pg;
        tasks.push(tokio::spawn(async move {
            let mut pg = Pg::connect(port).await;
            pg.query(WIDE).await;
            let started = Instant::now();
            let mut rows = 0;
            loop {
                let (tag, body) = pg.message().await;
                match tag {
                    b'D' => {
                        let (value, len) = wide_row(&body);
                        assert!((1..=WIDE_ROWS).contains(&value));
                        assert_eq!(len, WIDE_BYTES);
                        rows += 1;
                    }
                    b'E' => {
                        let text = String::from_utf8_lossy(&body).into_owned();
                        assert!(text.contains("Resources exhausted"), "{text}");
                        return (rows, 1);
                    }
                    b'Z' => return (rows, 0),
                    _ => {}
                }
                if i % 2 == 1 && rows == 1 {
                    // Stalled: keeps the connection, stops reading.
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    return (rows, 0);
                }
                if rows % 2000 == 0 {
                    if started.elapsed() > Duration::from_secs(6) {
                        return (rows, 0);
                    }
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
        }));
    }
    let mut rows = 0;
    let mut refused = 0;
    for task in tasks {
        let (r, f) = task.await.unwrap();
        rows += r;
        refused += f;
    }
    let reserved = server.drained().await;
    let peak = server.peak();
    server.assert_alive();
    eprintln!(
        "pg wire: idle {}, peak {}, growth {} (budget {}), rows read {rows}, memory errors {refused}, reserved after {}",
        mib(baseline),
        mib(peak),
        mib(peak.saturating_sub(baseline)),
        mib(BUDGET),
        mib(reserved)
    );
    assert!(rows > 0);
    assert_eq!(reserved, 0, "reservations left after every connection closed");
    assert!(
        peak.saturating_sub(baseline) <= BUDGET,
        "server grew by {} over its idle {} with memory.limit = {} (budget {})",
        mib(peak.saturating_sub(baseline)),
        mib(baseline),
        mib(LIMIT),
        mib(BUDGET)
    );
}

/// Runs one heavy statement on a fresh server: its result (or its memory
/// error), the growth of the server over its idle footprint and the most
/// spill seen while it ran. Returns a report and what is wrong with it.
async fn heavy(name: &str, sql: &str, expected: &[Value], must_spill: bool) -> (String, Vec<String>) {
    let mut server = Server::start().await;
    let baseline = server.baseline().await;
    let trino = Trino::new(server.trino);
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let done = Arc::clone(&done);
        let http = server.http;
        tokio::spawn(async move {
            let mut spill = 0;
            while !done.load(Ordering::SeqCst) {
                if let Ok(response) = reqwest::get(format!("http://127.0.0.1:{http}/healthz")).await {
                    let body: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
                    spill = spill.max(body["memory"]["spill_used_bytes"].as_u64().unwrap_or(0));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            spill
        })
    };
    let (result, _) = trino.run(sql).await;
    done.store(true, Ordering::SeqCst);
    let spill = sampler.await.unwrap();
    let growth = server.peak().saturating_sub(baseline);
    server.assert_alive();
    let outcome = match &result {
        Ok(rows) => format!("{rows:?}"),
        Err(error) => format!("{} {}", error["errorName"], error["message"]),
    };
    let report = format!(
        "{name}: idle {}, growth {} (budget {}), max spill {}, {outcome}",
        mib(baseline),
        mib(growth),
        mib(BUDGET),
        mib(spill)
    );
    let mut wrong = Vec::new();
    match result.map(|rows| rows.into_iter().map(Value::Array).collect::<Vec<_>>()) {
        Ok(rows) if rows == expected => {}
        Ok(rows) => wrong.push(format!("wrong result {rows:?}, expected {expected:?}")),
        Err(error) if !must_spill && error["errorName"] == "EXCEEDED_LOCAL_MEMORY_LIMIT" => {}
        Err(error) => wrong.push(format!("failed: {} {}", error["errorName"], error["message"])),
    }
    if must_spill && spill == 0 {
        wrong.push("did not spill".to_owned());
    }
    if growth > BUDGET {
        wrong.push(format!(
            "grew by {} over its idle {} (budget {}, memory.limit {})",
            mib(growth),
            mib(baseline),
            mib(BUDGET),
            mib(LIMIT)
        ));
    }
    let left = server.drained().await;
    if left > 0 {
        wrong.push(format!(
            "{} still reserved 20 s after the statement ended",
            mib(left)
        ));
    }
    (report, wrong)
}

/// A full sort of ~330 MiB (`row_number` over the whole input) under 128 MiB:
/// spills and returns the first and last rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heavy_sort_spills_under_the_ceiling() {
    let (report, wrong) = heavy(
        "sort",
        "SELECT r, length(s) AS l, substr(s, 201) AS v FROM (\
           SELECT row_number() OVER (ORDER BY s DESC) AS r, s FROM (\
             SELECT lpad('', 200, 'x') || CAST(value AS VARCHAR) AS s \
             FROM generate_series(1, 1500000))) \
         WHERE r IN (1, 1500000) ORDER BY r",
        &[json!([1, 206, "999999"]), json!([1_500_000, 201, "1"])],
        true,
    )
    .await;
    eprintln!("{report}");
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// 1,000,000 groups of ~210 B keys, each seen twice, under 128 MiB: spills and
/// counts right.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heavy_aggregation_spills_under_the_ceiling() {
    let (report, wrong) = heavy(
        "aggregation",
        "SELECT count(*) AS groups, sum(c) AS n, min(c) AS lo, max(c) AS hi FROM (\
           SELECT p, count(*) AS c FROM (\
             SELECT lpad('', 200, 'x') || CAST(value % 1000000 AS VARCHAR) AS p \
             FROM generate_series(1, 2000000)) GROUP BY p)",
        &[json!([1_000_000, 2_000_000, 2, 2])],
        true,
    )
    .await;
    eprintln!("{report}");
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A hash join whose build side (either one) is ~220 MiB, under 128 MiB: the
/// right numbers or `EXCEEDED_LOCAL_MEMORY_LIMIT`, within the budget either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heavy_hash_join_fails_cleanly_under_the_ceiling() {
    // Digits of 1..=1,000,000: 9*1 + 90*2 + 900*3 + 9000*4 + 90000*5 + 900000*6 + 7.
    let digits: i64 = 5_888_896;
    let (report, wrong) = heavy(
        "hash join",
        "SELECT count(*) AS n, sum(a.v) AS total, sum(length(a.p) + length(b.p)) AS bytes FROM (\
           SELECT value AS v, lpad('', 200, 'x') || CAST(value AS VARCHAR) AS p \
           FROM generate_series(1, 1000000)) a JOIN (\
           SELECT value AS v, lpad('', 200, 'y') || CAST(value AS VARCHAR) AS p \
           FROM generate_series(1, 1000000)) b ON a.v = b.v",
        &[json!([
            1_000_000,
            500_000_500_000_i64,
            2 * (200 * 1_000_000 + digits)
        ])],
        false,
    )
    .await;
    eprintln!("{report}");
    assert!(wrong.is_empty(), "{wrong:#?}");
}
