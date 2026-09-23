//! Throughput under a fixed memory ceiling. CI compares these numbers against
//! the base branch with `cargo xtask bench-gate` (> 25% slower blocks the PR).

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

const ROWS_PER_BATCH: i64 = 8_192;
const BATCHES: i64 = 128; // ~1M rows

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
    engine
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
}

criterion_group!(query_memory, benches);
criterion_main!(query_memory);
