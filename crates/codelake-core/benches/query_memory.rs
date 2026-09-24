//! Throughput under a fixed memory ceiling. CI compares these numbers against
//! the base branch with `cargo xtask bench-gate` (> 25% slower blocks the PR).
//!
//! Groups: aggregation and sort (roomy vs spilling), Parquet scan with a
//! filter, and a hash join (roomy vs spilling). End-to-end numbers against
//! Trino come from `scripts/bench/run.sh` instead (docs/benchmarks.md).

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "benchmark harness"
)]

use std::sync::Arc;

use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::ParquetReadOptions;

const ROWS_PER_BATCH: i64 = 8_192;
const BATCHES: i64 = 128; // ~1M rows
const CUSTOMERS: i64 = 50_000;

fn engine(spill: &std::path::Path, limit: ByteSize) -> Engine {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(4),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let regions = ["north", "south", "east", "west", "center"];
    let batches = (0..BATCHES)
        .map(|b| {
            let ids: Vec<i64> = (0..ROWS_PER_BATCH)
                .map(|i| (b * ROWS_PER_BATCH + i) % 50_000)
                .collect();
            let amounts: Vec<i64> = (0..ROWS_PER_BATCH).map(|i| (i * 31 + b) % 10_000).collect();
            let region: Vec<&str> = (0..ROWS_PER_BATCH)
                .map(|i| regions[usize::try_from(i % 5).unwrap()])
                .collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(amounts)),
                    Arc::new(StringArray::from(region)),
                ],
            )
            .unwrap()
        })
        .collect();
    let table = MemTable::try_new(schema, vec![batches]).unwrap();
    engine.context().register_table("sales", Arc::new(table)).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("segment", DataType::Utf8, false),
    ]));
    let segments = ["retail", "wholesale", "public", "export"];
    let ids: Vec<i64> = (0..CUSTOMERS).collect();
    let segment: Vec<&str> = (0..CUSTOMERS)
        .map(|i| segments[usize::try_from(i % 4).unwrap()])
        .collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(segment)),
        ],
    )
    .unwrap();
    let customers = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    engine
        .context()
        .register_table("customers", Arc::new(customers))
        .unwrap();
    engine
}

/// Writes `sales` once as Parquet (several files) and registers it as `sales_pq`.
fn with_parquet(rt: &tokio::runtime::Runtime, engines: &[&Engine], dir: &std::path::Path) {
    let path = dir.join("sales");
    rt.block_on(async {
        let copy = format!(
            "COPY (SELECT * FROM sales) TO '{}' STORED AS PARQUET",
            path.display()
        );
        engines[0]
            .context()
            .sql(&copy)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for engine in engines {
            engine
                .context()
                .register_parquet(
                    "sales_pq",
                    path.display().to_string(),
                    ParquetReadOptions::default(),
                )
                .await
                .unwrap();
        }
    });
}

fn run(rt: &tokio::runtime::Runtime, engine: &Engine, sql: &str) {
    rt.block_on(async {
        engine.context().sql(sql).await.unwrap().collect().await.unwrap();
    });
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let roomy = engine(spill.path(), ByteSize::gib(2));
    let tight = engine(spill.path(), ByteSize::mib(64));
    let parquet = tempfile::tempdir().unwrap();
    with_parquet(&rt, &[&roomy, &tight], parquet.path());

    let mut group = c.benchmark_group("aggregate_1m_rows");
    group.sample_size(20);
    let agg = "SELECT region, customer_id, sum(amount) FROM sales GROUP BY region, customer_id";
    group.bench_function("roomy_2gib", |b| b.iter(|| run(&rt, &roomy, agg)));
    group.bench_function("tight_64mib_spills", |b| b.iter(|| run(&rt, &tight, agg)));
    group.finish();

    let mut group = c.benchmark_group("sort_1m_rows");
    group.sample_size(20);
    let sort = "SELECT max(rn) FROM (SELECT row_number() OVER (ORDER BY amount, customer_id) rn FROM sales)";
    group.bench_function("roomy_2gib", |b| b.iter(|| run(&rt, &roomy, sort)));
    group.bench_function("tight_64mib_spills", |b| b.iter(|| run(&rt, &tight, sort)));
    group.finish();

    let mut group = c.benchmark_group("parquet_scan_1m_rows");
    group.sample_size(20);
    let scan = "SELECT region, count(*), sum(amount) FROM sales_pq WHERE amount > 5000 GROUP BY region";
    group.bench_function("filter_aggregate", |b| b.iter(|| run(&rt, &roomy, scan)));
    group.finish();

    let mut group = c.benchmark_group("hash_join_1m_x_50k");
    group.sample_size(20);
    let join = "SELECT c.segment, s.region, count(*), sum(s.amount) FROM sales s \
                JOIN customers c ON s.customer_id = c.customer_id GROUP BY c.segment, s.region";
    group.bench_function("roomy_2gib", |b| b.iter(|| run(&rt, &roomy, join)));
    // DataFusion's hash join does not spill; the ceiling constrains the aggregation.
    group.bench_function("tight_64mib", |b| b.iter(|| run(&rt, &tight, join)));
    group.finish();
}

criterion_group!(query_memory, benches);
criterion_main!(query_memory);
