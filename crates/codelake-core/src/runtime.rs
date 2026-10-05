//! Memory governance for the query engine.
//!
//! This is the reason `CodeLake` exists: a hard, observable memory ceiling. Every
//! memory-hungry operator (hash join, sort, aggregation) must reserve from a
//! [`CeilingPool`] before allocating. When the pool is exhausted the operator
//! spills to disk; when spill is exhausted the *query* fails with a resources
//! error. The *process* never grows past the limit.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{MemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;

use crate::config::MemoryConfig;
use crate::pool::CeilingPool;

/// How many top memory consumers to name in an out-of-memory error. This is
/// what turns "query failed" into "query failed because `HashJoin` held 1.8GiB".
const TOP_CONSUMERS_IN_ERRORS: usize = 5;

/// Sort settings sized to the pool (ADR 0003, "Sorts").
///
/// - `repartition_sorts = false`: a global `ORDER BY` runs one external sort
///   over the merged input instead of one per partition. Per-partition sorts
///   share the pool, and once spilled each one's final merge reserves as many
///   spill files as the pool has room for: the first to get there takes the
///   memory the others need to merge or to go on sorting, and they fail with
///   `ResourcesExhausted` although spill was left. The single sort still sorts
///   its batches in parallel tasks; only the final merge is one stream, as the
///   merge of the per-partition sorts was.
/// - `sort_spill_reservation_bytes`: headroom a sort keeps for merging its
///   runs before it spills, `DataFusion`'s 10 MiB, but at most 1/16 of the
///   pool so that a small pool still has room for the rows being sorted.
pub(crate) fn sort_settings(session: SessionConfig, memory: &MemoryConfig) -> SessionConfig {
    let headroom =
        (memory.limit.as_usize() / SORT_HEADROOM_DIVISOR).clamp(MIN_SORT_HEADROOM, MAX_SORT_HEADROOM);
    session
        .with_repartition_sorts(false)
        .with_sort_spill_reservation_bytes(headroom)
}

/// Exchange spill settings: every batch a `RepartitionExec` spills goes to a
/// file of its own (`max_spill_file_size_bytes` = [`EXCHANGE_SPILL_FILE_BYTES`]).
///
/// `DataFusion` 54.1 can deadlock otherwise (apache/datafusion#24883, fixed
/// upstream by #24891, which no 54 release carries). An exchange that does
/// not keep order shares one spill pool per output partition among all its
/// input tasks, and a pool's file stays open for the next batches until it
/// reaches the limit (128 MiB by default). When two input tasks spill at the
/// same time, the second finds the file taken and opens another: the pool
/// holds two open files. Its reader reads only the oldest; once that file has
/// no unread batch it waits for it to grow or close, while the batch it was
/// told of sits in the newer one, and its partition stops reading its channel.
/// When the other partition stops the same way, every channel holds data, the
/// exchange's gate closes, the input tasks wait to send, nothing writes to or
/// closes the oldest files, and the query hangs, no thread running, holding its
/// memory. An aggregation under memory pressure did so in about 3% of its
/// runs under load (`constant_keys::tests`).
///
/// With a limit of 0 bytes, the call that spills a batch creates its file,
/// writes the batch and closes the file before it returns, without waiting
/// on anything in between: the oldest file a reader waits for is always one a
/// writer is finishing at that moment. The cost is a file per spilled batch;
/// only exchanges spill through this pool. Pinned against clients
/// (`crate::statement_policy`).
pub(crate) fn exchange_spill_settings(mut session: SessionConfig) -> SessionConfig {
    session.options_mut().execution.max_spill_file_size_bytes = EXCHANGE_SPILL_FILE_BYTES;
    session
}

/// See [`exchange_spill_settings`].
pub(crate) const EXCHANGE_SPILL_FILE_BYTES: usize = 0;

const SORT_HEADROOM_DIVISOR: usize = 16;
const MIN_SORT_HEADROOM: usize = 1 << 20;
const MAX_SORT_HEADROOM: usize = 10 << 20;

/// Snapshot of engine memory, exported as metrics and on `/healthz`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct MemoryStats {
    pub limit_bytes: u64,
    pub reserved_bytes: u64,
    pub spill_used_bytes: u64,
    pub spill_limit_bytes: u64,
}

/// Owns the [`RuntimeEnv`] shared by every query on this node.
#[derive(Debug, Clone)]
pub struct EngineRuntime {
    env: Arc<RuntimeEnv>,
    pool: Arc<TrackConsumersPool<CeilingPool>>,
    limit: u64,
}

impl EngineRuntime {
    pub fn new(config: &MemoryConfig) -> Result<Self> {
        let limit = config.limit.as_usize();
        let top = NonZeroUsize::new(TOP_CONSUMERS_IN_ERRORS).unwrap_or(NonZeroUsize::MIN);
        let pool = Arc::new(TrackConsumersPool::new(CeilingPool::new(limit), top));

        let disk = DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Directories(vec![config.spill_dir.clone()]))
            .with_max_temp_directory_size(config.spill_limit.as_u64());

        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .with_disk_manager_builder(disk)
            .with_metadata_cache_limit(config.metadata_cache_limit.as_usize())
            .build_arc()?;

        Ok(Self {
            env,
            pool,
            limit: config.limit.as_u64(),
        })
    }

    pub fn env(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.env)
    }

    /// Most bytes the pool held at once since the engine started or the last
    /// [`EngineRuntime::reset_peak`]: what operators reserved, not the
    /// process's resident memory.
    pub fn peak_reserved(&self) -> u64 {
        self.pool.inner().peak() as u64
    }

    /// Restarts [`EngineRuntime::peak_reserved`] from what is reserved now.
    pub fn reset_peak(&self) {
        self.pool.inner().reset_peak();
    }

    pub fn stats(&self) -> MemoryStats {
        let disk = &self.env.disk_manager;
        MemoryStats {
            limit_bytes: self.limit,
            reserved_bytes: self.pool.reserved() as u64,
            spill_used_bytes: disk.used_disk_space(),
            spill_limit_bytes: disk.max_temp_directory_size(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::ByteSize;
    use datafusion::execution::memory_pool::MemoryConsumer;

    fn config(dir: &std::path::Path, limit: ByteSize) -> MemoryConfig {
        MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(256),
            metadata_cache_limit: ByteSize::mib(1),
        }
    }

    /// apache/datafusion#24883 (see [`exchange_spill_settings`]): an
    /// exchange over two inputs of 2 000 batches each, under a 64 KiB pool
    /// that makes it spill nearly every batch, read by two partitions that
    /// yield after each batch. With the pool's files kept open every run hung
    /// (5 of 5, no thread running); each now ends, in well under a second. The
    /// timeout only turns a hang into a failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn exchange_spills_from_two_inputs_do_not_hang() {
        use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::expressions::col;
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::{ExecutionPlan, Partitioning};
        use futures::StreamExt;

        const BATCHES: i64 = 2_000;
        const ROWS: i64 = 64;
        let dir = tempfile::tempdir().unwrap();
        let memory = MemoryConfig {
            metadata_cache_limit: ByteSize::b(16 << 10),
            ..config(dir.path(), ByteSize::b(64 << 10))
        };
        let execution = crate::config::ExecutionConfig {
            batch_size: usize::try_from(ROWS).unwrap(),
            target_partitions: Some(2),
            ..crate::config::ExecutionConfig::default()
        };
        let engine = crate::Engine::new(&memory, &execution).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let input = |partition: i64| -> Vec<RecordBatch> {
            (0..BATCHES)
                .map(|b| {
                    let keys: Vec<i64> = (0..ROWS).map(|r| (partition * BATCHES + b) * ROWS + r).collect();
                    let text: Vec<String> = keys.iter().map(|k| format!("{k:0>40}")).collect();
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(keys)),
                            Arc::new(StringArray::from(text)),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        };
        let input = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[input(0), input(1)],
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        for run in 0..3 {
            let context = Arc::new(engine.sql("SELECT 1").await.unwrap().task_ctx());
            assert_eq!(
                context
                    .session_config()
                    .options()
                    .execution
                    .max_spill_file_size_bytes,
                EXCHANGE_SPILL_FILE_BYTES
            );
            let exchange: Arc<dyn ExecutionPlan> = Arc::new(
                RepartitionExec::try_new(
                    Arc::clone(&input) as Arc<dyn ExecutionPlan>,
                    Partitioning::Hash(vec![col("k", &schema).unwrap()], 2),
                )
                .unwrap(),
            );
            let readers: Vec<_> = (0..2)
                .map(|partition| {
                    let mut stream = exchange.execute(partition, Arc::clone(&context)).unwrap();
                    tokio::spawn(async move {
                        let mut rows = 0;
                        while let Some(batch) = stream.next().await {
                            rows += batch.unwrap().num_rows();
                            tokio::task::yield_now().await;
                        }
                        rows
                    })
                })
                .collect();
            let read = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                futures::future::join_all(readers),
            )
            .await;
            let files = exchange.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
            let Ok(read) = read else {
                panic!("run {run} hung after {files} spill files");
            };
            let rows: usize = read.into_iter().map(Result::unwrap).sum();
            assert_eq!(rows, usize::try_from(2 * BATCHES * ROWS).unwrap(), "run {run}");
            // Vacuous unless both inputs spilled into the pools many times.
            assert!(files > 100, "run {run}: {files} spill files");
        }
    }

    #[test]
    fn pool_enforces_the_configured_limit() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = EngineRuntime::new(&config(dir.path(), ByteSize::mib(64))).unwrap();

        let reservation = MemoryConsumer::new("probe").register(&runtime.env().memory_pool);
        reservation.try_grow(ByteSize::mib(32).as_usize()).unwrap();
        assert_eq!(runtime.stats().reserved_bytes, ByteSize::mib(32).as_u64());

        let err = reservation.try_grow(ByteSize::mib(64).as_usize()).unwrap_err();
        assert!(err.to_string().contains("Resources exhausted"), "{err}");
        assert!(
            err.to_string().contains("probe"),
            "error must name the consumer: {err}"
        );
    }

    #[test]
    fn stats_report_limits() {
        let dir = tempfile::tempdir().unwrap();
        let runtime = EngineRuntime::new(&config(dir.path(), ByteSize::mib(128))).unwrap();
        let stats = runtime.stats();
        assert_eq!(stats.limit_bytes, ByteSize::mib(128).as_u64());
        assert_eq!(stats.spill_limit_bytes, ByteSize::mib(256).as_u64());
        assert_eq!(stats.reserved_bytes, 0);
        assert_eq!(stats.spill_used_bytes, 0);
    }
}
