//! Distributed execution (M4, ADR 0011): the 22 TPC-H queries on a
//! coordinator with three workers (real gRPC servers on loopback ports)
//! return what the same engine returns on one node.
//!
//! Data: TPC-H generated in-process by `tpchgen` (the `dbgen` algorithm),
//! money columns `decimal(15,2)` as in production warehouses, written as
//! Parquet with several files per table so scans split across tasks.

#![cfg(feature = "distributed")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test crate"
)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine};
use codelake_server::distributed::{StaticWorkers, coordinate, worker};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::{DistributedExt, display_plan_ascii};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

const SCALE: f64 = 0.01;

fn money() -> DataType {
    DataType::Decimal128(15, 2)
}

fn tables() -> Vec<(&'static str, Vec<(&'static str, DataType)>)> {
    use DataType::{Date32, Int32, Int64, Utf8};
    vec![
        (
            "region",
            vec![("r_regionkey", Int64), ("r_name", Utf8), ("r_comment", Utf8)],
        ),
        (
            "nation",
            vec![
                ("n_nationkey", Int64),
                ("n_name", Utf8),
                ("n_regionkey", Int64),
                ("n_comment", Utf8),
            ],
        ),
        (
            "supplier",
            vec![
                ("s_suppkey", Int64),
                ("s_name", Utf8),
                ("s_address", Utf8),
                ("s_nationkey", Int64),
                ("s_phone", Utf8),
                ("s_acctbal", money()),
                ("s_comment", Utf8),
            ],
        ),
        (
            "customer",
            vec![
                ("c_custkey", Int64),
                ("c_name", Utf8),
                ("c_address", Utf8),
                ("c_nationkey", Int64),
                ("c_phone", Utf8),
                ("c_acctbal", money()),
                ("c_mktsegment", Utf8),
                ("c_comment", Utf8),
            ],
        ),
        (
            "part",
            vec![
                ("p_partkey", Int64),
                ("p_name", Utf8),
                ("p_mfgr", Utf8),
                ("p_brand", Utf8),
                ("p_type", Utf8),
                ("p_size", Int32),
                ("p_container", Utf8),
                ("p_retailprice", money()),
                ("p_comment", Utf8),
            ],
        ),
        (
            "partsupp",
            vec![
                ("ps_partkey", Int64),
                ("ps_suppkey", Int64),
                ("ps_availqty", Int32),
                ("ps_supplycost", money()),
                ("ps_comment", Utf8),
            ],
        ),
        (
            "orders",
            vec![
                ("o_orderkey", Int64),
                ("o_custkey", Int64),
                ("o_orderstatus", Utf8),
                ("o_totalprice", money()),
                ("o_orderdate", Date32),
                ("o_orderpriority", Utf8),
                ("o_clerk", Utf8),
                ("o_shippriority", Int32),
                ("o_comment", Utf8),
            ],
        ),
        (
            "lineitem",
            vec![
                ("l_orderkey", Int64),
                ("l_partkey", Int64),
                ("l_suppkey", Int64),
                ("l_linenumber", Int32),
                ("l_quantity", money()),
                ("l_extendedprice", money()),
                ("l_discount", money()),
                ("l_tax", money()),
                ("l_returnflag", Utf8),
                ("l_linestatus", Utf8),
                ("l_shipdate", Date32),
                ("l_commitdate", Date32),
                ("l_receiptdate", Date32),
                ("l_shipinstruct", Utf8),
                ("l_shipmode", Utf8),
                ("l_comment", Utf8),
            ],
        ),
    ]
}

/// `dbgen` rows of `table` in its `|`-terminated text format.
fn generate(table: &str) -> String {
    use tpchgen::generators::{
        CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
        PartSuppGenerator, RegionGenerator, SupplierGenerator,
    };
    fn lines<T: std::fmt::Display>(rows: impl Iterator<Item = T>) -> String {
        let mut out = String::new();
        for row in rows {
            writeln!(out, "{row}").unwrap();
        }
        out
    }
    match table {
        "region" => lines(RegionGenerator::new(SCALE, 1, 1).iter()),
        "nation" => lines(NationGenerator::new(SCALE, 1, 1).iter()),
        "supplier" => lines(SupplierGenerator::new(SCALE, 1, 1).iter()),
        "customer" => lines(CustomerGenerator::new(SCALE, 1, 1).iter()),
        "part" => lines(PartGenerator::new(SCALE, 1, 1).iter()),
        "partsupp" => lines(PartSuppGenerator::new(SCALE, 1, 1).iter()),
        "orders" => lines(OrderGenerator::new(SCALE, 1, 1).iter()),
        "lineitem" => lines(LineItemGenerator::new(SCALE, 1, 1).iter()),
        other => panic!("no table {other}"),
    }
}

/// Writes every table as Parquet under `dir/<table>/`, four files each.
async fn write_tpch(dir: &Path) {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    for (table, columns) in tables() {
        let text = dir.join(format!("{table}.tbl"));
        std::fs::write(&text, generate(table)).unwrap();
        let mut fields: Vec<Field> = columns
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), false))
            .collect();
        // Each line ends with the delimiter: one empty trailing field.
        fields.push(Field::new("_end", DataType::Utf8, true));
        let schema = Schema::new(fields);
        let options = CsvReadOptions::new()
            .has_header(false)
            .delimiter(b'|')
            .schema(&schema)
            .file_extension(".tbl");
        let names: Vec<&str> = columns.iter().map(|(name, _)| *name).collect();
        ctx.read_csv(text.to_str().unwrap(), options)
            .await
            .unwrap()
            .select_columns(&names)
            .unwrap()
            .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(4))
            .unwrap()
            .write_parquet(
                dir.join(table).to_str().unwrap(),
                DataFrameWriteOptions::new(),
                None,
            )
            .await
            .unwrap();
        std::fs::remove_file(text).unwrap();
    }
}

fn memory(dir: &Path) -> MemoryConfig {
    MemoryConfig {
        limit: ByteSize::mib(512),
        spill_dir: dir.to_owned(),
        spill_limit: ByteSize::mib(512),
        metadata_cache_limit: ByteSize::mib(8),
    }
}

/// An engine with the TPC-H tables as `bench.tpch.*`.
async fn engine_with_tpch(data: &Path, spill: &Path) -> Engine {
    let engine = Engine::new(&memory(spill), &ExecutionConfig::default()).unwrap();
    register_tpch(&engine, data).await;
    engine
}

async fn register_tpch(engine: &Engine, data: &Path) {
    let ctx = engine.context();
    let catalog = MemoryCatalogProvider::new();
    catalog
        .register_schema("tpch", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("bench", Arc::new(catalog));
    for (table, _) in tables() {
        ctx.register_parquet(
            format!("bench.tpch.{table}"),
            data.join(table).to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    }
}

/// Starts a worker engine's gRPC service on a loopback port.
async fn start_worker(spill: &Path) -> url::Url {
    let engine = Engine::new(&memory(spill), &ExecutionConfig::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = worker(&engine).into_worker_server();
    tokio::spawn(async move {
        let _engine = engine;
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    url::Url::parse(&format!("http://{addr}")).unwrap()
}

fn query(n: usize) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("../../bench/queries/tpch/q{n:02}.sql"));
    std::fs::read_to_string(path)
        .unwrap()
        .replace("{cat}", "bench")
        .replace("{tpch}", "tpch")
}

/// Rows as text, sorted: the comparison ignores the order of rows that
/// `ORDER BY` leaves tied.
async fn rows(engine: &Engine, sql: &str) -> Result<Vec<String>, String> {
    let batches = engine
        .sql(sql)
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    let options = FormatOptions::default();
    let mut out = Vec::new();
    for batch in &batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = formatters.iter().map(|f| f.value(row).to_string()).collect();
            out.push(cells.join("|"));
        }
    }
    out.sort();
    Ok(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tpch_on_three_workers_returns_what_one_node_returns() {
    let data = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    write_tpch(data.path()).await;

    let single = engine_with_tpch(data.path(), spill.path()).await;
    let mut urls = Vec::new();
    for _ in 0..3 {
        urls.push(start_worker(spill.path()).await);
    }
    let mut coordinator = Engine::new(&memory(spill.path()), &ExecutionConfig::default()).unwrap();
    // Small tasks so SF 0.01 is split across the workers.
    coordinate(&mut coordinator, StaticWorkers(urls), |builder| {
        builder
            .with_distributed_file_scan_config_bytes_per_partition(8 * 1024)
            .unwrap()
    });
    register_tpch(&coordinator, data.path()).await;

    let mut report = String::new();
    let mut failures = BTreeMap::new();
    let mut distributed_queries = 0;
    let only: Option<usize> = std::env::var("CODELAKE_TPCH_ONLY")
        .ok()
        .and_then(|v| v.parse().ok());
    for n in 1..=22 {
        if only.is_some_and(|o| o != n) {
            continue;
        }
        let sql = query(n);
        let plan = coordinator
            .sql(&sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let shown = display_plan_ascii(plan.as_ref(), false);
        if shown.contains("Stage") {
            distributed_queries += 1;
        }
        writeln!(report, "== Q{n:02}\n{shown}").unwrap();
        let expected = rows(&single, &sql).await;
        let actual = rows(&coordinator, &sql).await;
        if expected != actual {
            failures.insert(n, (expected, actual));
        }
    }
    if let Ok(path) = std::env::var("CODELAKE_DISTRIBUTED_PLANS") {
        std::fs::write(path, &report).unwrap();
    }
    assert!(
        failures.is_empty(),
        "distributed results differ from single-node on {:?}:\n{:#?}",
        failures.keys().collect::<Vec<_>>(),
        failures
    );
    assert!(
        distributed_queries >= 18,
        "only {distributed_queries} of 22 queries were distributed:\n{report}"
    );
}
