//! Implementer's smoke test for the hive catalog (local warehouse). The F2
//! acceptance suite (Trino round trip, SQL Server OPENROWSET) is written
//! separately by an independent tester.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::common;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::Engine;

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

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

/// Data files of a table with the CRC-32 of their bytes.
fn data_files(dir: &Path) -> BTreeMap<String, u32> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file())
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .map(|e| {
            let bytes = std::fs::read(e.path()).unwrap();
            (e.file_name().to_string_lossy().into_owned(), crc32(&bytes))
        })
        .collect()
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[tokio::test]
async fn reads_trino_warehouse_and_writes_compatible_tables() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse"),
        &warehouse,
    );
    let spill = tempfile::tempdir().unwrap();
    let engine = open_engine(&warehouse, spill.path()).await;

    // Written by Trino 431: INT96 timestamps, decimal, binary, accents, a view.
    let t1 = run(
        &engine,
        "SELECT id, nome, valor, lsn, ts, d, flag, dbl, i, vc FROM hive.pilot.t1 ORDER BY id",
    )
    .await;
    assert!(t1.contains("| 1  | ação | 10.50 | 0000000000000000002a | 2026-01-01T10:00:00.123 | 2026-01-02 | true  | 3.5 | 7 | x  |"), "{t1}");
    assert!(t1.contains("| 2  | b    | 1.00  | 00000000000000000030 | 2026-01-03T00:00:00     | 2026-01-03 | false | 1.0 | 8 | y  |"), "{t1}");
    let view = run(&engine, "SELECT id, nome FROM hive.pilot.v1 ORDER BY id").await;
    assert!(
        view.contains("| 1  | ação |") && view.contains("| 2  | b    |"),
        "{view}"
    );

    // CTAS with Trino's syntax, then append.
    run(&engine, "CREATE SCHEMA hive.raw").await;
    let created = run(
        &engine,
        "CREATE TABLE hive.raw.orders WITH (format = 'PARQUET') AS \
         SELECT id, CAST(valor AS DECIMAL(18,2)) AS valor, ts FROM hive.pilot.t1",
    )
    .await;
    assert!(created.contains("| 2    |"), "{created}");
    let table_dir = warehouse.join("raw/orders");
    let before = data_files(&table_dir);
    assert_eq!(before.len(), 1, "CTAS writes one file");
    assert!(table_dir.join(".trinoSchema").exists() && table_dir.join("..trinoSchema.crc").exists());
    assert!(table_dir.join(".trinoPermissions/user_codelake").exists());

    run(&engine, "INSERT INTO hive.raw.orders SELECT 3, CAST(7.25 AS DECIMAL(18,2)), CAST('2026-02-01T00:00:00' AS TIMESTAMP)").await;
    let after = data_files(&table_dir);
    assert_eq!(after.len(), 2, "one new file per INSERT");
    for (name, crc) in &before {
        assert_eq!(
            after.get(name),
            Some(crc),
            "existing file {name} must be untouched"
        );
    }
    let names: Vec<&String> = after.keys().collect();
    assert!(names[0] < names[1], "file names sort by write time: {names:?}");

    // Schema evolution: old files read the new column as NULL.
    run(&engine, "ALTER TABLE hive.raw.orders ADD COLUMN origem varchar").await;
    run(&engine, "INSERT INTO hive.raw.orders SELECT 4, CAST(1 AS DECIMAL(18,2)), CAST('2026-02-02T00:00:00' AS TIMESTAMP), 'cdc'").await;
    let evolved = run(&engine, "SELECT id, origem FROM hive.raw.orders ORDER BY id").await;
    assert!(
        evolved.contains("| 1  |        |") && evolved.contains("| 4  | cdc    |"),
        "{evolved}"
    );

    // A view created here survives a restart.
    run(
        &engine,
        "CREATE VIEW hive.raw.recent AS SELECT id FROM hive.raw.orders WHERE id >= 3",
    )
    .await;
    drop(engine);
    let engine = open_engine(&warehouse, spill.path()).await;
    let recent = run(&engine, "SELECT id FROM hive.raw.recent ORDER BY id").await;
    assert!(
        recent.contains("| 3  |") && recent.contains("| 4  |") && !recent.contains("| 1  |"),
        "{recent}"
    );
    let listed = run(
        &engine,
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = 'hive' AND table_schema = 'raw' ORDER BY table_name",
    )
    .await;
    assert!(
        listed.contains("orders") && listed.contains("recent") && listed.contains("VIEW"),
        "{listed}"
    );

    run(&engine, "DROP VIEW hive.raw.recent").await;
    run(&engine, "DROP TABLE hive.raw.orders").await;
    assert!(!table_dir.join(".trinoSchema").exists());
    assert!(engine.sql("SELECT * FROM hive.raw.orders").await.is_err());
}
