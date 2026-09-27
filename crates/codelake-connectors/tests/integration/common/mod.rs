//! Shared helpers for integration tests. Every test here talks to a real,
//! disposable instance started by Testcontainers: a mock would only validate our
//! assumptions about the database, not the database.

#![allow(dead_code, unreachable_pub)]

use std::sync::Arc;
use std::time::Duration;

use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Connector, Engine};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;

pub const ATTACH_TIMEOUT: Duration = Duration::from_secs(60);

pub fn engine(spill_dir: &std::path::Path) -> Engine {
    let memory = MemoryConfig {
        limit: ByteSize::mib(256),
        spill_dir: spill_dir.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    };
    Engine::new(&memory, &ExecutionConfig::default()).expect("engine")
}

pub async fn attach(engine: &mut Engine, connector: Arc<dyn Connector>) {
    engine
        .attach(connector, true, ATTACH_TIMEOUT)
        .await
        .expect("source must attach");
}

pub async fn query(engine: &Engine, sql: &str) -> String {
    let batches: Vec<RecordBatch> = engine
        .context()
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}` failed: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("executing `{sql}` failed: {e}"));
    pretty_format_batches(&batches).expect("format").to_string()
}

/// Host to reach containers from inside the dev/CI container.
pub fn host() -> String {
    std::env::var("TESTCONTAINERS_HOST_OVERRIDE").unwrap_or_else(|_| "127.0.0.1".to_owned())
}

/// Counts the workers the reference engine's coordinator lists as active.
/// It logs "SERVER STARTED" before its own worker registers, and a query sent
/// in between fails with "No nodes available to run query": a server is
/// ready once this answers at least one.
pub const ACTIVE_WORKERS: &str = "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'";

/// Whether an answer to [`ACTIVE_WORKERS`] (its first cell, as text, quoted
/// or not) counts at least one worker.
pub fn counts_a_worker(first_cell: Option<&str>) -> bool {
    first_cell
        .and_then(|cell| cell.trim().trim_matches('"').parse::<u64>().ok())
        .is_some_and(|workers| workers > 0)
}

/// The names Trino 431's file metastore writes: the reference engine these
/// tests run in containers shares the warehouse (ISS-011).
#[cfg(feature = "hive")]
pub fn reference_names() -> codelake_core::config::MetastoreNames {
    codelake_core::config::MetastoreNames {
        metadata_file: ".trinoSchema".into(),
        permissions_dir: ".trinoPermissions".into(),
        view_marker: "Presto View".into(),
        view_parameter: "presto_view".into(),
        version_parameter: "presto_version".into(),
        writer_version: "431".into(),
    }
}
