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
