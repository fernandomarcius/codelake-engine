//! A SQL Server connection that goes away while a read is under way.
//!
//! The shadow run saw one `Connection reset by peer (os error 104)` in about
//! 2200 cycles of a dbt model reading ~961 000 rows. The engine retries a read
//! whose connection was lost **only while no batch has reached the consumer**;
//! after that the error surfaces, because the rows already delivered cannot be
//! taken back and a second read would repeat them or end the stream short.
//!
//! The faults come from a TCP proxy between the engine and a real SQL Server:
//! it relays bytes untouched until a connection has carried a number of bytes
//! from the server, then resets it (an RST, as the shadow run saw) or closes it
//! (a FIN: the end of the stream inside a TDS message). Reading the engine's
//! stream item by item shows what the consumer gets: the rows, the error, and
//! whether the stream ever ended without one.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::common;
use crate::sqlserver_dynamic_filter::{SA_PASSWORD, exec, start_mssql};
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::Engine;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use futures::StreamExt;
use secrecy::SecretString;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Rows of the table: ~66 bytes each on the wire, 8192 per batch (~540 KB).
const ROWS: i64 = 100_000;
const BATCH_ROWS: usize = 8192;

/// What the attach (a login plus the catalog's metadata queries: ~36 KB) may
/// carry for the cut point below to fall after a login and before a batch.
const LOGIN_CEILING: u64 = 64 * 1024;
/// Before the first batch: after any login, ~1900 rows into the first 8192
/// (a first batch needs more than 500 KB).
const BEFORE_FIRST_BATCH: u64 = 128 * 1024;
/// Several batches in, short of the end of the result (6.6 MB).
const MID_STREAM: u64 = 4 * 1024 * 1024;

const UNLIMITED: usize = usize::MAX;

/// When and how the proxy breaks a connection.
#[derive(Debug, Clone, Copy, Default)]
struct Plan {
    /// Bytes from the server a connection carries, counted from the moment
    /// the plan is armed, before it is broken.
    cut_after: Option<u64>,
    /// `true` closes with a FIN, `false` resets with an RST.
    clean_close: bool,
    /// Connections still to break.
    faults_left: usize,
}

#[derive(Default)]
struct State {
    plan: Mutex<Plan>,
    /// Connections broken so far.
    injected: AtomicUsize,
    /// Most bytes from the server any one connection carried in an epoch.
    peak: AtomicU64,
    /// Changes with every plan: a connection counts its bytes from it.
    epoch: AtomicU64,
}

impl State {
    fn arm(&self, plan: Plan) {
        *self.plan.lock().unwrap() = plan;
        self.injected.store(0, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn disarm(&self) {
        *self.plan.lock().unwrap() = Plan::default();
    }

    /// Whether the connection that has carried `carried` bytes breaks now,
    /// and how.
    fn fault(&self, carried: u64) -> Option<bool> {
        let mut plan = self.plan.lock().unwrap();
        let cut = plan.cut_after?;
        if carried < cut || plan.faults_left == 0 {
            return None;
        }
        plan.faults_left -= 1;
        self.injected.fetch_add(1, Ordering::SeqCst);
        Some(plan.clean_close)
    }
}

struct Proxy {
    port: u16,
    state: Arc<State>,
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State::default());
        let accept = tokio::spawn({
            let state = Arc::clone(&state);
            async move {
                while let Ok((client, _)) = listener.accept().await {
                    tokio::spawn(relay(client, upstream.clone(), Arc::clone(&state)));
                }
            }
        });
        Self { port, state, accept }
    }
}

/// `SO_LINGER` 0: closing the socket sends an RST, not a FIN.
#[allow(
    deprecated,
    reason = "an RST is what the shadow run saw and only a zero linger sends it; the warning is about lingering sockets blocking the thread, which a zero linger does not do"
)]
fn reset_on_close(socket: &TcpStream) {
    let _ = socket.set_linger(Some(Duration::ZERO));
}

async fn relay(mut client: TcpStream, upstream: String, state: Arc<State>) {
    let Ok(mut server) = TcpStream::connect(&upstream).await else {
        return;
    };
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    let mut from_client = vec![0_u8; 16 * 1024];
    let mut from_server = vec![0_u8; 16 * 1024];
    let mut carried = 0_u64;
    let mut epoch = state.epoch.load(Ordering::SeqCst);
    loop {
        tokio::select! {
            read = client.read(&mut from_client) => match read {
                Ok(n) if n > 0 => {
                    if server.write_all(&from_client[..n]).await.is_err() {
                        return;
                    }
                }
                _ => return,
            },
            read = server.read(&mut from_server) => match read {
                Ok(n) if n > 0 => {
                    // A connection that outlives a plan (the pool's idle one)
                    // counts from the plan's start, as a new one does.
                    let now = state.epoch.load(Ordering::SeqCst);
                    if now != epoch {
                        epoch = now;
                        carried = 0;
                    }
                    carried += u64::try_from(n).unwrap();
                    state.peak.fetch_max(carried, Ordering::SeqCst);
                    if client.write_all(&from_server[..n]).await.is_err() {
                        return;
                    }
                    if let Some(clean) = state.fault(carried) {
                        if clean {
                            let _ = client.shutdown().await;
                        } else {
                            reset_on_close(&client);
                        }
                        return;
                    }
                }
                _ => return,
            },
        }
    }
}

/// What a consumer of the engine's stream got.
#[derive(Debug)]
struct Reading {
    rows: usize,
    batches: usize,
    ids: HashSet<i64>,
    sum: i64,
    /// The error that ended the stream; `None` for a clean end.
    error: Option<String>,
    took: Duration,
}

impl Reading {
    fn duplicates(&self) -> usize {
        self.rows - self.ids.len()
    }
}

/// Reads `SELECT id, filler` (a plain projection: an aggregate would be
/// computed by SQL Server and send a few bytes) one batch at a time.
async fn read_stream(engine: &Engine) -> Reading {
    let began = Instant::now();
    let mut stream = engine
        .context()
        .sql("SELECT id, filler FROM src.dbo.t")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let mut reading = Reading {
        rows: 0,
        batches: 0,
        ids: HashSet::new(),
        sum: 0,
        error: None,
        took: Duration::ZERO,
    };
    while let Some(item) = stream.next().await {
        match item {
            Ok(batch) => {
                assert!(
                    batch.num_rows() <= BATCH_ROWS,
                    "a batch of {} rows",
                    batch.num_rows()
                );
                let ids = cast(batch.column(0), &DataType::Int64).unwrap();
                for id in ids.as_primitive::<Int64Type>().iter().flatten() {
                    reading.ids.insert(id);
                    reading.sum += id;
                }
                reading.rows += batch.num_rows();
                reading.batches += 1;
            }
            Err(e) => {
                reading.error = Some(e.to_string());
                break;
            }
        }
    }
    reading.took = began.elapsed();
    reading
}

fn assert_whole(reading: &Reading, what: &str) {
    let rows = usize::try_from(ROWS).unwrap();
    assert_eq!(reading.error, None, "{what}: {reading:?}");
    assert_eq!(reading.rows, rows, "{what}: row count");
    assert_eq!(reading.ids.len(), rows, "{what}: distinct ids");
    assert_eq!(reading.sum, ROWS * (ROWS + 1) / 2, "{what}: sum of ids");
}

fn assert_connection_error(reading: &Reading, what: &str) {
    let error = reading
        .error
        .as_deref()
        .unwrap_or_else(|| panic!("{what}: the stream ended cleanly: {reading:?}"));
    // The error the client has always seen for a lost connection: the type and
    // the text of the driver's io error, unchanged.
    assert!(
        error.starts_with("External error: ") && error.contains("performing I/O"),
        "{what}: {error}"
    );
}

/// A SQL Server with the table, and an engine reading it through the proxy.
/// The first value keeps the container alive.
async fn start() -> (impl Sized, Proxy, Engine, tempfile::TempDir) {
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE retry").await;
    exec(
        &host,
        port,
        "retry",
        &format!(
            "CREATE TABLE dbo.t (id INT NOT NULL PRIMARY KEY, filler VARCHAR(60) NOT NULL);
             WITH n AS (SELECT TOP ({ROWS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                        FROM sys.all_objects a CROSS JOIN sys.all_objects b)
             INSERT INTO dbo.t
             SELECT i, RIGHT(REPLICATE('0', 60) + CAST(i AS VARCHAR(10)), 60) FROM n;"
        ),
    )
    .await;

    let proxy = Proxy::start(format!("{host}:{port}")).await;
    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        Arc::new(SqlServerConnector::new(
            "src".into(),
            SqlServerParams {
                host: "127.0.0.1".into(),
                port: proxy.port,
                database: "retry".into(),
                user: "sa".into(),
                password: SecretString::from(SA_PASSWORD),
                encrypt: "required".into(),
                trust_server_certificate: true,
                // One connection: a retry has to take a new one from the pool.
                pool_size: 1,
            },
        )),
    )
    .await;
    (mssql, proxy, engine, spill)
}

#[tokio::test]
async fn a_lost_connection_is_retried_only_before_the_first_batch() {
    let (_mssql, proxy, engine, _spill) = start().await;
    let state = Arc::clone(&proxy.state);

    // The thresholds below assume a connection costs a few KB before its
    // first row: what the attach just spent bounds what a login spends.
    let login = state.peak.load(Ordering::SeqCst);
    assert!(
        login > 0 && login < LOGIN_CEILING,
        "login and metadata queries carried {login} bytes; the cut points assume under {LOGIN_CEILING}"
    );

    // Control: nothing broken, every row once.
    let clean = read_stream(&engine).await;
    assert_whole(&clean, "clean read");
    assert!(clean.batches > 1, "{clean:?}");
    assert_eq!(state.injected.load(Ordering::SeqCst), 0);

    // A. Reset before the first batch (one connection): the read starts over
    // on a new connection and the consumer gets every row once.
    state.arm(Plan {
        cut_after: Some(BEFORE_FIRST_BATCH),
        clean_close: false,
        faults_left: 1,
    });
    let retried = read_stream(&engine).await;
    assert_whole(&retried, "reset before the first batch");
    assert_eq!(
        state.injected.load(Ordering::SeqCst),
        1,
        "the connection was broken once and the read finished on another"
    );
    assert!(
        retried.took >= Duration::from_millis(200),
        "the retry waits its backoff: {:?}",
        retried.took
    );

    // A'. The same with the connection closed in the middle of a message.
    state.arm(Plan {
        cut_after: Some(BEFORE_FIRST_BATCH),
        clean_close: true,
        faults_left: 1,
    });
    let closed = read_stream(&engine).await;
    assert_whole(&closed, "close before the first batch");
    assert_eq!(state.injected.load(Ordering::SeqCst), 1);

    // B. Reset after several batches reached the consumer, on every
    // connection: the error surfaces as it always did, the rows delivered are
    // not repeated and the stream does not end as if it were complete. A
    // retry would break again, so exactly one broken connection is no retry.
    state.arm(Plan {
        cut_after: Some(MID_STREAM),
        clean_close: false,
        faults_left: UNLIMITED,
    });
    let mid = read_stream(&engine).await;
    assert_connection_error(&mid, "reset mid-stream");
    assert!(
        mid.batches >= 1 && mid.rows >= BATCH_ROWS,
        "batches reached the consumer before the cut: {mid:?}"
    );
    assert!(
        mid.rows < usize::try_from(ROWS).unwrap(),
        "the stream is short, and said so: {mid:?}"
    );
    assert_eq!(mid.duplicates(), 0, "no row twice: {mid:?}");
    assert_eq!(
        state.injected.load(Ordering::SeqCst),
        1,
        "a read that delivered rows is not run again"
    );

    // C. Reset on every connection before the first batch: three attempts
    // (the read and two retries, a 200 ms and a 1 s pause), then the error.
    state.arm(Plan {
        cut_after: Some(BEFORE_FIRST_BATCH),
        clean_close: false,
        faults_left: UNLIMITED,
    });
    let exhausted = read_stream(&engine).await;
    assert_connection_error(&exhausted, "reset on every attempt");
    assert_eq!(exhausted.rows, 0, "{exhausted:?}");
    assert_eq!(
        state.injected.load(Ordering::SeqCst),
        3,
        "one attempt and two retries"
    );
    assert!(
        exhausted.took >= Duration::from_millis(1200),
        "both pauses were taken: {:?}",
        exhausted.took
    );

    // The pool gives a working connection once the proxy lets it.
    state.disarm();
    let after = read_stream(&engine).await;
    assert_whole(&after, "read after the faults");
}
