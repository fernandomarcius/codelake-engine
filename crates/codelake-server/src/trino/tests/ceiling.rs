//! Hard memory ceiling on the Trino protocol path, measured by exact
//! accounting instead of process RSS (front: hard memory ceiling, red team).
//!
//! Why not RSS: the allocator keeps freed pages resident (glibc arenas per
//! thread), so an RSS reading after a release cannot tell freed buffers from
//! held ones — RT10 read 1324 MiB both before and after the sweep and reported
//! "held 0", a pass that proved nothing — and other tests running in the same
//! process move it (RT10 failed 2/2 in the full suite). These probes count
//! bytes instead:
//!
//! - [`Engine::memory`]`().reserved_bytes`: what the pool believes is in use;
//! - [`Probe::live`]: bytes of every value buffer made by `rt_payload(x)` that
//!   something in the process still references (a `RecordBatch`, a slice, a
//!   channel, a page being built). Each buffer is built on a `bytes::Bytes`
//!   owner whose `Drop` subtracts its size, so the count is exact and does not
//!   depend on the allocator.
//!
//! A payload byte that is live but not reserved is outside the ceiling.
//!
//! [`Engine::memory`]: codelake_core::Engine::memory

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Bytes;
use axum::http::{Method, StatusCode};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;
use datafusion::arrow::array::{AsArray, StringArray};
use datafusion::arrow::buffer::{Buffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

use super::{Harness, harness_config, limits, path_of};

const MIB: usize = 1 << 20;
/// Digits of the row value at the start of every payload.
const DIGITS: usize = 12;

/// Counters of the `rt_payload` buffers of one harness.
#[derive(Debug, Clone)]
pub(super) struct Probe {
    live: Arc<AtomicUsize>,
    made: Arc<AtomicUsize>,
    pub(super) row_bytes: usize,
}

impl Probe {
    /// Registers `rt_payload(value BIGINT) -> VARCHAR` on the harness's
    /// engine: `value` zero-padded to 12 digits, then `x` up to `row_bytes`.
    pub(super) fn register(h: &Harness, row_bytes: usize) -> Self {
        assert!(row_bytes > DIGITS);
        let probe = Self {
            live: Arc::new(AtomicUsize::new(0)),
            made: Arc::new(AtomicUsize::new(0)),
            row_bytes,
        };
        h.engine
            .context()
            .register_udf(ScalarUDF::new_from_impl(PayloadUdf {
                signature: Signature::exact(vec![DataType::Int64], Volatility::Volatile),
                probe: probe.clone(),
            }));
        probe
    }

    /// Payload bytes referenced anywhere in the process right now.
    pub(super) fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    /// Payload bytes produced so far.
    fn made(&self) -> usize {
        self.made.load(Ordering::SeqCst)
    }

    /// Bytes of one 8192-row batch.
    pub(super) fn batch_bytes(&self) -> usize {
        self.row_bytes * 8192
    }

    /// Checks one encoded payload value.
    pub(super) fn check(&self, value: &Value) -> Result<i64, String> {
        let text = value.as_str().ok_or_else(|| format!("not text: {value}"))?;
        if text.len() != self.row_bytes || !text[DIGITS..].bytes().all(|b| b == b'x') {
            return Err(format!("bad payload of {} bytes", text.len()));
        }
        text[..DIGITS]
            .parse()
            .map_err(|_| format!("bad prefix {:?}", &text[..DIGITS]))
    }
}

/// Owns one payload buffer; subtracts it from `live` when the last reference
/// to it goes away.
struct Owned {
    data: Vec<u8>,
    live: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for Owned {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for Owned {
    fn drop(&mut self) {
        self.live.fetch_sub(self.data.len(), Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct PayloadUdf {
    signature: Signature,
    probe: Probe,
}

impl PartialEq for PayloadUdf {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.probe.live, &other.probe.live)
    }
}

impl Eq for PayloadUdf {}

impl Hash for PayloadUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.probe.row_bytes.hash(state);
    }
}

impl ScalarUDFImpl for PayloadUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "rt_payload"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let values = values.as_primitive::<Int64Type>();
        let row = self.probe.row_bytes;
        let mut data = Vec::with_capacity(rows * row);
        for i in 0..rows {
            let prefix = format!("{:0>DIGITS$}", values.value(i));
            data.extend_from_slice(&prefix.as_bytes()[..DIGITS]);
            data.resize(data.len() + row - DIGITS, b'x');
        }
        let len = data.len();
        self.probe.live.fetch_add(len, Ordering::SeqCst);
        self.probe.made.fetch_add(len, Ordering::SeqCst);
        let owned = Owned {
            data,
            live: Arc::clone(&self.probe.live),
        };
        let buffer = Buffer::from(Bytes::from_owner(owned));
        let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(row, rows));
        let array = StringArray::try_new(offsets, buffer, None)?;
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// `(live payload bytes, pool reserved bytes)`.
pub(super) fn reading(h: &Harness, probe: &Probe) -> (usize, usize) {
    let reserved = usize::try_from(h.engine.memory().reserved_bytes).unwrap_or(usize::MAX);
    (probe.live(), reserved)
}

/// Waits until production stops (the producers are blocked on the client) and
/// the readings hold still for 400 ms; at most 20 s.
pub(super) async fn settle(h: &Harness, probe: &Probe) -> (usize, usize) {
    let mut last = (probe.made(), reading(h, probe));
    let mut still = 0;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = (probe.made(), reading(h, probe));
        if now == last {
            still += 1;
            if still == 8 {
                return now.1;
            }
        } else {
            still = 0;
            last = now;
        }
    }
    panic!("production never settled: {last:?}");
}

/// Waits (at most 10 s) for every payload buffer and reservation to be gone;
/// returns the last reading.
pub(super) async fn released(h: &Harness, probe: &Probe) -> (usize, usize) {
    for _ in 0..200 {
        let now = reading(h, probe);
        if now == (0, 0) {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    reading(h, probe)
}

pub(super) fn partitions(n: usize) -> ExecutionConfig {
    ExecutionConfig {
        target_partitions: Some(n),
        ..ExecutionConfig::default()
    }
}

fn mib(bytes: usize) -> String {
    format!("{}.{} MiB", bytes / MIB, bytes % MIB * 10 / MIB)
}

/// Starts `sql`, fetches its first page and checks its rows; returns the
/// `nextUri` path (or the failure).
async fn first_page(h: &Harness, probe: &Probe, sql: &str) -> Result<String, Value> {
    let first = h.submit(sql, &[]).await;
    let one = path_of(first.body["nextUri"].as_str().unwrap());
    let page = h.call(Method::GET, &one, &[], "").await;
    assert_eq!(page.status, StatusCode::OK);
    if page.body["error"].is_object() {
        return Err(page.body["error"].clone());
    }
    for row in page.body["data"].as_array().into_iter().flatten() {
        let n = probe.check(&row[0]).unwrap();
        assert!((1..=2_000_000).contains(&n), "row value {n}");
    }
    Ok(path_of(page.body["nextUri"].as_str().unwrap()))
}

/// The module contract of `statement.rs`: "the buffered batches are reserved
/// in the engine's memory pool". A client that reads one page and pauses
/// (a slow client, or one about to abandon the query) leaves the plan blocked;
/// every payload byte it holds must then be reserved. With 4 partitions, the
/// round-robin repartition feeding the projection and the merge
/// (`CoalescePartitionsExec`) above it hold batches in their channels that
/// only the output buffer's own 2 + 1 batches account for.
#[tokio::test]
async fn mem1_buffered_output_of_a_paused_client_is_reserved() {
    let h = harness_config(limits(), ByteSize::mib(256), &partitions(4));
    // 1 KiB rows: 8 MiB per 8192-row batch.
    let probe = Probe::register(&h, 1024);
    let next = first_page(
        &h,
        &probe,
        "SELECT rt_payload(value) AS s FROM generate_series(1, 2000000)",
    )
    .await
    .unwrap();
    let (live, reserved) = settle(&h, &probe).await;
    eprintln!(
        "paused client: live payload {} ({} batches of {}), reserved {}",
        mib(live),
        live / probe.batch_bytes(),
        mib(probe.batch_bytes()),
        mib(reserved)
    );
    assert!(live > 0, "the probe sees no payload");
    let _ = h.call(Method::DELETE, &next, &[], "").await;
    assert!(
        live <= reserved,
        "{} of payload is held while the pool reserves {}: {} outside the ceiling",
        mib(live),
        mib(reserved),
        mib(live - reserved)
    );
}

/// N concurrent paused clients under a small pool: the payload the process
/// holds never exceeds the limit. Each query either keeps its buffered batches
/// inside its reservation or fails with `EXCEEDED_LOCAL_MEMORY_LIMIT`; no
/// other error, and the rows that do arrive are the right ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem2_concurrent_paused_clients_hold_at_most_the_limit() {
    let limit = 64 * MIB;
    let h = Arc::new(harness_config(limits(), ByteSize::mib(64), &partitions(4)));
    let probe = Probe::register(&h, 1024);
    let sql = "SELECT rt_payload(value) AS s FROM generate_series(1, 2000000)";
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let (h, probe) = (Arc::clone(&h), probe.clone());
        tasks.push(tokio::spawn(async move { first_page(&h, &probe, sql).await }));
    }
    let mut nexts = Vec::new();
    let mut refused = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(next) => nexts.push(next),
            Err(error) => {
                assert_eq!(error["errorName"], "EXCEEDED_LOCAL_MEMORY_LIMIT", "{error}");
                refused += 1;
            }
        }
    }
    let (live, reserved) = settle(&h, &probe).await;
    // A second page: running queries keep working or fail for memory only.
    let mut failed_later = 0;
    for next in &nexts {
        let page = h.call(Method::GET, next, &[], "").await;
        if page.body["error"].is_object() {
            assert_eq!(
                page.body["error"]["errorName"], "EXCEEDED_LOCAL_MEMORY_LIMIT",
                "{}",
                page.body["error"]
            );
            failed_later += 1;
        }
        for row in page.body["data"].as_array().into_iter().flatten() {
            probe.check(&row[0]).unwrap();
        }
        let _ = h.call(Method::DELETE, next, &[], "").await;
    }
    eprintln!(
        "12 paused queries, limit {}: live payload {}, reserved {}, refused at first page {refused}, failed at second page {failed_later}",
        mib(limit),
        mib(live),
        mib(reserved)
    );
    assert!(reserved <= limit);
    assert!(
        live <= limit,
        "{} of payload held under memory.limit = {}",
        mib(live),
        mib(limit)
    );
}

/// A canceled or abandoned query keeps the JSON of its last page
/// (`Exec.last`, for a client retry) while it stays in the history
/// (`history_retention_secs` = 900 s, `history_size` = 100). That page is
/// outside the pool and as large as the widest row (one row per page at
/// least), so 100 canceled queries whose last page carried a wide row pin
/// 100 x that row with 0 bytes reserved. Observed by asking for the last token
/// again after the cancel: it is still served, in full.
#[tokio::test]
async fn mem3_history_does_not_keep_page_bodies_outside_the_pool() {
    let limit = 64 * MIB;
    let h = harness_config(limits(), ByteSize::mib(64), &partitions(1));
    let mut held = 0;
    for _ in 0..8 {
        // Two 8 MiB rows per batch: every page carries one whole row.
        let first = h
            .submit("SELECT repeat('x', 8388608) AS s FROM generate_series(1, 4)", &[])
            .await;
        let id = first.body["id"].as_str().unwrap().to_owned();
        let one = path_of(first.body["nextUri"].as_str().unwrap());
        let page = h.call(Method::GET, &one, &[], "").await;
        assert_eq!(page.body["data"][0][0].as_str().map(str::len), Some(8_388_608));
        let killed = h
            .call(
                Method::DELETE,
                &format!("/v1/query/{id}"),
                &[("x-trino-user", "tester")],
                "",
            )
            .await;
        assert_eq!(killed.status, StatusCode::NO_CONTENT);
        let again = h.call(Method::GET, &one, &[], "").await;
        if again.status == StatusCode::OK && again.body["data"].is_array() {
            held += again.raw.len();
        }
    }
    let reserved = usize::try_from(h.engine.memory().reserved_bytes).unwrap();
    let listed = h
        .run(
            "SELECT count(*) FROM system.runtime.queries WHERE state = 'FAILED'",
            &[],
        )
        .await;
    eprintln!(
        "8 canceled queries in the history: page bodies still served {}, reserved {}, limit {}, FAILED listed {:?}",
        mib(held),
        mib(reserved),
        mib(limit),
        listed.rows
    );
    assert!(
        held <= reserved,
        "the history holds {} of page bodies with {} reserved (memory.limit {})",
        mib(held),
        mib(reserved),
        mib(limit)
    );
}

/// Sorts `rows` payload rows of 256 B (a full sort: `row_number` over the whole
/// input) under a pool of `limit_mib` with `partitions` partitions. The sort
/// must spill, keep the payload it holds under the limit and return the right
/// rows. The payload is sampled every millisecond: sampling can miss a peak,
/// never invent one, so a failure is real.
async fn sort_under(limit_mib: u64, partitions_n: usize, rows: i64) -> Vec<String> {
    let limit = usize::try_from(ByteSize::mib(limit_mib).as_u64()).unwrap();
    let h = Arc::new(harness_config(
        limits(),
        ByteSize::mib(limit_mib),
        &partitions(partitions_n),
    ));
    let probe = Probe::register(&h, 256);
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (h, probe, done) = (Arc::clone(&h), probe.clone(), Arc::clone(&done));
        tokio::spawn(async move {
            let (mut live, mut spill) = (0, 0);
            while !done.load(Ordering::SeqCst) {
                live = live.max(probe.live());
                spill = spill.max(h.engine.memory().spill_used_bytes);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            (live, spill)
        })
    };
    let out = h
        .run(
            &format!(
                "SELECT r, s FROM (SELECT row_number() OVER (ORDER BY s DESC) AS r, s \
                 FROM (SELECT rt_payload(value) AS s FROM generate_series(1, {rows}))) \
                 WHERE r IN (1, 2, {rows})"
            ),
            &[],
        )
        .await;
    done.store(true, Ordering::SeqCst);
    let (peak_live, peak_spill) = sampler.await.unwrap();
    let outcome = out.error_name().map_or_else(
        || format!("{:?}", out.rows.iter().map(|r| r[0].clone()).collect::<Vec<_>>()),
        |name| format!("{name}: {}", out.last["error"]["message"]),
    );
    eprintln!(
        "sort of {rows} x 256 B, pool {}, {partitions_n} partitions: peak live payload {}, peak spill {}, {outcome}",
        mib(limit),
        mib(peak_live),
        mib(usize::try_from(peak_spill).unwrap_or(usize::MAX))
    );
    let mut wrong = Vec::new();
    if out.state() == "FINISHED" {
        let mut got: Vec<(i64, i64)> = out
            .rows
            .iter()
            .map(|row| (row[0].as_i64().unwrap(), probe.check(&row[1]).unwrap()))
            .collect();
        got.sort_unstable();
        if got != vec![(1, rows), (2, rows - 1), (rows, 1)] {
            wrong.push(format!("wrong rows {got:?}"));
        }
        if peak_spill == 0 {
            wrong.push("finished without spilling".to_owned());
        }
    } else {
        wrong.push(format!("did not finish: {outcome}"));
    }
    if peak_live > limit {
        wrong.push(format!(
            "held {} of payload under memory.limit = {}",
            mib(peak_live),
            mib(limit)
        ));
    }
    wrong
}

/// ~100 MiB sorted under 64 MiB with 2 partitions: it spills, then the merge
/// of the spill files reserves more than the pool has left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "DEFECT-MEM4: the sort spills and finishes with the right rows, but row_number() is sent as decimal(20, 0), not bigint (fixed on front/f4-functions)"]
async fn mem4_sort_of_1_5x_the_pool_spills_and_finishes() {
    let wrong = sort_under(64, 2, 400_000).await;
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// ~100 MiB sorted under 32 MiB with 2 partitions: each partition's sort
/// first reserves `sort_spill_reservation_bytes` (10 MiB) for its merge, and
/// what is left is too little to buffer the one batch it could spill.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "DEFECT-MEM5: the sort spills and finishes with the right rows, but row_number() is sent as decimal(20, 0), not bigint (fixed on front/f4-functions)"]
async fn mem5_sort_spills_under_32_mib_with_2_partitions() {
    let wrong = sort_under(32, 2, 400_000).await;
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// ~100 MiB sorted under 64 MiB with 8 partitions (a node with 8 cores and
/// the default `target_partitions`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "DEFECT-MEM5: the sort spills and finishes with the right rows, but row_number() is sent as decimal(20, 0), not bigint (fixed on front/f4-functions)"]
async fn mem5_sort_spills_under_64_mib_with_8_partitions() {
    let wrong = sort_under(64, 8, 400_000).await;
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The same run, twice: the probe itself returns to zero after a completed
/// query, so a non-zero reading elsewhere is a real hold, not probe drift.
#[tokio::test]
async fn mem_probe_returns_to_zero_after_a_completed_query() {
    let h = harness_config(limits(), ByteSize::mib(64), &partitions(2));
    let probe = Probe::register(&h, 64);
    for _ in 0..2 {
        let out = h
            .run(
                "SELECT count(*) AS n, min(s) AS lo, max(s) AS hi \
                 FROM (SELECT rt_payload(value) AS s FROM generate_series(1, 50000))",
                &[],
            )
            .await;
        assert_eq!(out.rows[0][0], 50_000);
        assert_eq!(probe.check(&out.rows[0][1]), Ok(1));
        assert_eq!(probe.check(&out.rows[0][2]), Ok(50_000));
        assert_eq!(released(&h, &probe).await, (0, 0));
    }
    assert_eq!(probe.made(), 2 * 50_000 * 64);
}
