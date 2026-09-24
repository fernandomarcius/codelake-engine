//! The hive scan hands statistics to the planner: the smaller input of a join
//! builds the hash table, a small dimension is collected once, and the rows
//! are the same as with a scan of the same files without statistics.

use std::path::Path;
use std::sync::Arc;

use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::Engine;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};

use crate::common;

async fn open_engine(warehouse: &Path, spill: &Path) -> Engine {
    let mut engine = common::engine(spill);
    common::attach(
        &mut engine,
        Arc::new(HiveConnector::new(
            "hive".into(),
            HiveParams {
                warehouse: warehouse.display().to_string(),
                s3_endpoint: None,
                s3_region: "us-east-1".into(),
                credentials: None,
                path_style: false,
            },
        )),
    )
    .await;
    engine
}

/// Through the engine's planning path, which routes DDL to the hive catalog.
async fn run(engine: &Engine, sql: &str) -> String {
    let batches = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("running `{sql}`: {e}"));
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

/// The physical plan `EXPLAIN` prints.
async fn explain(engine: &Engine, sql: &str) -> String {
    let batches = engine
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}` failed: {e}"))
        .collect()
        .await
        .unwrap();
    for batch in &batches {
        let kinds = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let plans = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "physical_plan" {
                return plans.value(row).to_owned();
            }
        }
    }
    panic!("no physical plan for `{sql}`");
}

/// The hash join line and the first scan below it, which is the build side.
fn build_side(plan: &str) -> (String, String) {
    let mut lines = plan.lines().skip_while(|l| !l.contains("HashJoinExec"));
    let join = lines
        .next()
        .unwrap_or_else(|| panic!("no hash join:\n{plan}"))
        .to_owned();
    let scan = lines
        .find(|l| l.contains("DataSourceExec"))
        .unwrap_or_else(|| panic!("no scan under the join:\n{plan}"))
        .to_owned();
    (join, scan)
}

/// Data files of `schema.table` in the warehouse.
fn files(warehouse: &Path, table: &str) -> Vec<ListingTableUrl> {
    let mut paths: Vec<_> = std::fs::read_dir(warehouse.join("tpch").join(table))
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file() && !e.file_name().to_string_lossy().starts_with('.'))
        .map(|e| e.path())
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|p| ListingTableUrl::parse(p.to_str().unwrap()).unwrap())
        .collect()
}

/// Catalog `plain`: the same files as `hive.tpch`, read by the stock Parquet
/// listing without statistics.
async fn register_plain(engine: &Engine, warehouse: &Path) {
    let schema = MemorySchemaProvider::new();
    for table in ["lineitem", "orders", "nation"] {
        let hive = engine
            .context()
            .table_provider(format!("hive.tpch.{table}"))
            .await
            .unwrap();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension("")
            .with_collect_stat(false);
        let config = ListingTableConfig::new_with_multi_paths(files(warehouse, table))
            .with_listing_options(options)
            .with_schema(hive.schema());
        schema
            .register_table(table.into(), Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
    }
    let catalog = MemoryCatalogProvider::new();
    catalog.register_schema("tpch", Arc::new(schema)).unwrap();
    engine.context().register_catalog("plain", Arc::new(catalog));
}

const LINEITEM_ORDERS: &str = "SELECT o.o_orderkey, o.o_totalprice, COUNT(*) AS lines, SUM(l.l_linenumber) AS total \
     FROM {c}.tpch.lineitem l JOIN {c}.tpch.orders o ON l.l_orderkey = o.o_orderkey \
     GROUP BY o.o_orderkey, o.o_totalprice ORDER BY o.o_orderkey";
const ORDERS_NATION: &str = "SELECT n.n_name, COUNT(*) AS orders, SUM(o.o_orderkey) AS keys \
     FROM {c}.tpch.orders o JOIN {c}.tpch.nation n ON o.o_nationkey = n.n_nationkey \
     GROUP BY n.n_name ORDER BY n.n_name";
const FILTERED: &str = "SELECT COUNT(*) AS n, MIN(l.l_linenumber) AS lo, MAX(l.l_linenumber) AS hi \
     FROM {c}.tpch.lineitem l JOIN {c}.tpch.orders o ON l.l_orderkey = o.o_orderkey \
     WHERE o.o_orderkey BETWEEN 49990 AND 50010 AND l.l_linenumber > 1000";

fn on(catalog: &str, sql: &str) -> String {
    sql.replace("{c}", catalog)
}

#[tokio::test]
async fn joins_build_on_the_smaller_input_and_return_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let spill = tempfile::tempdir().unwrap();
    let engine = open_engine(&warehouse, spill.path()).await;

    run(&engine, "CREATE SCHEMA hive.tpch").await;
    // lineitem: 300k rows over 3 row groups; orders: 50k rows, then an INSERT
    // (a second file, metastore statistics removed); nation: 25 rows.
    run(
        &engine,
        "CREATE TABLE hive.tpch.lineitem AS SELECT \
           CAST(value % 51000 AS BIGINT) AS l_orderkey, CAST(value AS BIGINT) AS l_linenumber, \
           CAST(value % 97 AS DECIMAL(12,2)) AS l_extendedprice \
         FROM generate_series(1, 300000)",
    )
    .await;
    run(
        &engine,
        "CREATE TABLE hive.tpch.orders AS SELECT \
           CAST(value AS BIGINT) AS o_orderkey, CAST(value % 25 AS BIGINT) AS o_nationkey, \
           CAST(value * 3 AS DECIMAL(15,2)) AS o_totalprice, CONCAT('order ', CAST(value AS VARCHAR)) AS o_comment \
         FROM generate_series(0, 49999)",
    )
    .await;
    run(
        &engine,
        "INSERT INTO hive.tpch.orders SELECT CAST(value AS BIGINT), CAST(value % 25 AS BIGINT), \
           CAST(value * 3 AS DECIMAL(15,2)), 'late' FROM generate_series(50000, 50999)",
    )
    .await;
    run(
        &engine,
        "CREATE TABLE hive.tpch.nation AS SELECT CAST(value AS BIGINT) AS n_nationkey, \
           CONCAT('nation ', CAST(value AS VARCHAR)) AS n_name FROM generate_series(0, 24)",
    )
    .await;
    assert_eq!(files(&warehouse, "orders").len(), 2);
    register_plain(&engine, &warehouse).await;

    // lineitem is written first; with statistics orders (6x fewer rows) builds.
    let with = explain(&engine, &on("hive", LINEITEM_ORDERS)).await;
    let (join, build) = build_side(&with);
    assert!(build.contains("tpch/orders/"), "orders must build:\n{with}");
    assert!(
        join.contains("mode=Partitioned"),
        "orders (key and price, 51k rows) is above the 1 MiB collect threshold:\n{with}"
    );
    // Without statistics the planner keeps the written order: lineitem builds.
    let without = explain(&engine, &on("plain", LINEITEM_ORDERS)).await;
    let (_, build) = build_side(&without);
    assert!(
        build.contains("tpch/lineitem/"),
        "baseline builds on lineitem:\n{without}"
    );

    // A 25-row dimension on the right is collected once, as the build side.
    let with = explain(&engine, &on("hive", ORDERS_NATION)).await;
    let (join, build) = build_side(&with);
    assert!(join.contains("mode=CollectLeft"), "{with}");
    assert!(build.contains("tpch/nation/"), "nation must build:\n{with}");
    let without = explain(&engine, &on("plain", ORDERS_NATION)).await;
    let (join, build) = build_side(&without);
    assert!(
        join.contains("mode=Partitioned") && build.contains("tpch/orders/"),
        "{without}"
    );

    // Plans differ, rows do not.
    for sql in [LINEITEM_ORDERS, ORDERS_NATION, FILTERED] {
        let with = run(&engine, &on("hive", sql)).await;
        let without = run(&engine, &on("plain", sql)).await;
        assert_eq!(with, without, "{sql}");
        assert!(with.lines().count() > 4, "{with}");
    }
    let checked = run(&engine, &on("hive", FILTERED)).await;
    assert!(checked.contains("| 105 | 49990 | 254010 |"), "{checked}");
}
