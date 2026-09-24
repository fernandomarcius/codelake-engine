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
    pool: Arc<dyn MemoryPool>,
    limit: u64,
}

impl EngineRuntime {
    pub fn new(config: &MemoryConfig) -> Result<Self> {
        let limit = config.limit.as_usize();
        let top = NonZeroUsize::new(TOP_CONSUMERS_IN_ERRORS).unwrap_or(NonZeroUsize::MIN);
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(CeilingPool::new(limit), top));

        let disk = DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Directories(vec![config.spill_dir.clone()]))
            .with_max_temp_directory_size(config.spill_limit.as_u64());

        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
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
