//! `ALTER TABLE … EXECUTE optimize` against a real Trino 431 (ADR 0009):
//! a table written by Trino is compacted by `CodeLake`, a table written and
//! compacted by `CodeLake` is read by Trino, and both engines return the same
//! rows before and after.
//!
//! The warehouse moves between the test and the Trino containers by copy
//! (the tests themselves run in a container, so a bind mount of a local
//! directory would point at the wrong host path).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::{Engine, QueryOptions};
use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::common;

const CATALOG: &str = "connector.name=hive\n\
                       hive.metastore=file\n\
                       hive.metastore.catalog.dir=file:/warehouse\n\
                       hive.allow-add-column=true\n";

/// Same warehouse, accepting metadata whose `writerVersion` is not `431`.
/// Tables *created* by `CodeLake` carry `writerVersion: codelake`, which Trino's
/// file metastore refuses by default (an ADR 0007 gap, independent of
/// OPTIMIZE: tables created by Trino keep their version when `CodeLake`
/// rewrites their metadata, and are read through the strict `hive` catalog).
const LENIENT: &str = "connector.name=hive\n\
                       hive.metastore=file\n\
                       hive.metastore.catalog.dir=file:/warehouse\n\
                       hive.metastore.version-compatibility=UNSAFE_ASSUME_COMPATIBILITY\n";

/// Same warehouse read and written at microsecond precision (a Trino catalog
/// setting; `hive` reads milliseconds).
const MICROSECONDS: &str = "connector.name=hive\n\
                            hive.metastore=file\n\
                            hive.metastore.catalog.dir=file:/warehouse\n\
                            hive.timestamp-precision=MICROSECONDS\n";

/// Starts Trino 431 with a file-metastore `hive` catalog over `/warehouse`,
/// pre-filled with `files` (path relative to the warehouse, bytes).
async fn trino(files: Vec<(String, Vec<u8>)>) -> ContainerAsync<GenericImage> {
    let mut request = GenericImage::new("trinodb/trino", "431")
        .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
        .with_container_name(format!("codelake-f9-trino-{}", uuid::Uuid::new_v4()))
        .with_user("root")
        .with_startup_timeout(Duration::from_secs(180))
        .with_host_config_modifier(|config| config.memory = Some(2 << 30))
        .with_copy_to("/etc/trino/catalog/hive.properties", CATALOG.as_bytes().to_vec())
        .with_copy_to(
            "/etc/trino/catalog/lenient.properties",
            LENIENT.as_bytes().to_vec(),
        )
        .with_copy_to(
            "/etc/trino/catalog/hive_us.properties",
            MICROSECONDS.as_bytes().to_vec(),
        );
    for (path, bytes) in files {
        request = request.with_copy_to(format!("/warehouse/{path}"), bytes);
    }
    let container = request.start().await.expect("trino 431 starts");
    for _ in 0..60 {
        if run_trino(&container, "SELECT 1").await.is_ok() {
            return container;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("trino did not accept queries");
}

/// Runs `sql` with the Trino CLI. A coordinator under load can drop out of
/// its own worker list for a moment ("No nodes available to run query");
/// nothing ran then, so the statement is sent again.
async fn run_trino(container: &ContainerAsync<GenericImage>, sql: &str) -> Result<String, String> {
    let mut attempt = 0;
    loop {
        match run_trino_once(container, sql).await {
            Err(err) if err.contains("No nodes available to run query") && attempt < 60 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            other => return other,
        }
    }
}

async fn run_trino_once(container: &ContainerAsync<GenericImage>, sql: &str) -> Result<String, String> {
    let mut result = container
        .exec(ExecCommand::new([
            "trino",
            "--output-format",
            "CSV_HEADER",
            "--execute",
            sql,
        ]))
        .await
        .map_err(|e| e.to_string())?;
    let stdout = String::from_utf8(result.stdout_to_vec().await.map_err(|e| e.to_string())?).unwrap();
    let stderr = String::from_utf8(result.stderr_to_vec().await.map_err(|e| e.to_string())?).unwrap();
    let mut code = None;
    for _ in 0..50 {
        code = result.exit_code().await.map_err(|e| e.to_string())?;
        if code.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if code == Some(0) {
        Ok(stdout)
    } else {
        Err(format!("{sql}: exit {code:?}: {stderr}"))
    }
}

/// Runs `sql`; on failure, panics with the causes from Trino's server log.
async fn trino_sql(container: &ContainerAsync<GenericImage>, sql: &str) -> String {
    match run_trino(container, sql).await {
        Ok(stdout) => stdout,
        Err(err) => {
            let log = container.stderr_to_vec().await.unwrap_or_default();
            let causes: Vec<String> = String::from_utf8_lossy(&log)
                .lines()
                .filter(|l| l.contains("Caused by") || l.contains("Exception:"))
                .map(str::to_owned)
                .collect();
            panic!("{err}\nserver log:\n{}", causes.join("\n"));
        }
    }
}

/// Copies Trino's whole warehouse into `local`.
async fn copy_out(container: &ContainerAsync<GenericImage>, local: &Path) {
    let listing = trino_find(container).await;
    for path in listing.lines().filter(|l| !l.is_empty()) {
        let relative = path.strip_prefix("/warehouse/").unwrap();
        container
            .copy_file_from(path.to_owned(), local.join(relative))
            .await
            .unwrap();
    }
}

async fn trino_find(container: &ContainerAsync<GenericImage>) -> String {
    let mut result = container
        .exec(ExecCommand::new(["find", "/warehouse", "-type", "f"]))
        .await
        .unwrap();
    String::from_utf8(result.stdout_to_vec().await.unwrap()).unwrap()
}

/// Every file under `root`, relative path to bytes.
fn read_tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                walk(root, &path, out);
            } else {
                let relative = path.strip_prefix(root).unwrap().to_string_lossy().into_owned();
                out.push((relative, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out
}

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

/// Visible data files of a table directory.
fn data_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with(['.', '_']))
        .collect();
    names.sort();
    names
}

async fn engine(warehouse: &Path, spill: &Path) -> Engine {
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

async fn codelake(engine: &Engine, sql: &str) -> String {
    let mut options = QueryOptions::for_user("dbt");
    options.properties = engine
        .session_properties()
        .session(BTreeMap::from([(
            "hive.non_transactional_optimize_enabled".to_owned(),
            "true".to_owned(),
        )]))
        .unwrap();
    let batches = engine
        .sql_with(&options, sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("running `{sql}`: {e}"));
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string()
}

const TRINO_ROWS: &str = "SELECT id, nome, valor, lsn, ts, d, flag, dbl, i, origem \
                          FROM hive.trino_side.t ORDER BY id";
/// Aggregates both engines compute identically, as text.
const SUMMARY: &str = "SELECT count(*) AS n, sum(id) AS ids, count(origem) AS origens, \
                       count(nome) AS named FROM hive.trino_side.t";

#[tokio::test]
async fn optimize_round_trips_with_trino_431() {
    let local = tempfile::tempdir().unwrap();
    let warehouse: PathBuf = local.path().join("warehouse");
    let spill = tempfile::tempdir().unwrap();

    // 1. Trino writes a table in four statements, with a column added
    //    before the last one, and keeps its answer as the reference.
    let writer = trino(Vec::new()).await;
    for sql in [
        "CREATE SCHEMA hive.trino_side",
        "CREATE TABLE hive.trino_side.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS bigint) AS id, \
         CAST('ação' AS varchar) AS nome, CAST(10.50 AS decimal(18,2)) AS valor, \
         X'0000000000000000002A' AS lsn, CAST(TIMESTAMP '2026-01-01 10:00:00.123' AS timestamp(3)) AS ts, \
         DATE '2026-01-02' AS d, true AS flag, CAST(3.5 AS double) AS dbl, CAST(7 AS integer) AS i",
        "INSERT INTO hive.trino_side.t VALUES (2, 'b', 1.00, X'30', TIMESTAMP '2026-01-03 00:00:00.000', \
         DATE '2026-01-03', false, 1.0, 8)",
        "INSERT INTO hive.trino_side.t VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        "ALTER TABLE hive.trino_side.t ADD COLUMN origem varchar",
        "INSERT INTO hive.trino_side.t VALUES (4, 'd', -2.75, X'FF', TIMESTAMP '1999-12-31 23:59:59.999', \
         DATE '1900-01-01', true, -0.5, -1, 'cdc')",
    ] {
        trino_sql(&writer, sql).await;
    }
    let reference = trino_sql(&writer, TRINO_ROWS).await;
    assert_eq!(reference.lines().count(), 5, "{reference}");
    let reference_summary = trino_sql(&writer, SUMMARY).await;
    assert_eq!(
        reference_summary.trim(),
        "\"n\",\"ids\",\"origens\",\"named\"\n\"4\",\"10\",\"1\",\"3\""
    );
    copy_out(&writer, &warehouse).await;
    drop(writer);

    // 2. CodeLake compacts Trino's files and reads the same rows.
    let lake = engine(&warehouse, spill.path()).await;
    let trino_table = warehouse.join("trino_side/t");
    assert_eq!(data_files(&trino_table).len(), 4);
    let before = codelake(&lake, "SELECT * FROM hive.trino_side.t ORDER BY id").await;
    codelake(&lake, "ALTER TABLE hive.trino_side.t EXECUTE optimize").await;
    assert_eq!(data_files(&trino_table).len(), 1);
    assert_eq!(
        codelake(&lake, "SELECT * FROM hive.trino_side.t ORDER BY id").await,
        before
    );
    let summary = codelake(&lake, SUMMARY).await;
    assert!(summary.contains("| 4 | 10  | 1       | 3     |"), "{summary}");

    // 3. CodeLake writes its own table the same way, keeps a copy of the
    //    uncompacted directory as `t_before`, then compacts `t`.
    for sql in [
        "CREATE SCHEMA hive.codelake_side",
        "CREATE TABLE hive.codelake_side.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS BIGINT) AS id, \
         'ação' AS nome, CAST(10.50 AS DECIMAL(18,2)) AS valor, X'2A' AS lsn, \
         CAST('2026-01-01 10:00:00.123' AS TIMESTAMP) AS ts, CAST('2026-01-02' AS DATE) AS d",
        "INSERT INTO hive.codelake_side.t VALUES (2, 'b', 1.00, X'30', CAST('2026-01-03 00:00:00' AS TIMESTAMP), \
         CAST('2026-01-03' AS DATE))",
        "INSERT INTO hive.codelake_side.t VALUES (3, NULL, NULL, NULL, NULL, NULL)",
        "ALTER TABLE hive.codelake_side.t ADD COLUMN origem varchar",
        "INSERT INTO hive.codelake_side.t VALUES (4, 'd', -2.75, X'FF', CAST('1999-12-31 23:59:59.999' AS TIMESTAMP), \
         CAST('1900-01-01' AS DATE), 'cdc')",
    ] {
        codelake(&lake, sql).await;
    }
    let own_table = warehouse.join("codelake_side/t");
    copy_dir(&own_table, &warehouse.join("codelake_side/t_before"));
    codelake(&lake, "ALTER TABLE hive.codelake_side.t EXECUTE optimize").await;
    assert_eq!(data_files(&own_table).len(), 1);
    drop(lake);

    // 4. A fresh Trino over the compacted warehouse returns the reference
    //    rows for its table, and the same rows for CodeLake's table as for
    //    the uncompacted copy, from a single file each.
    let reader = trino(read_tree(&warehouse)).await;
    assert_eq!(trino_sql(&reader, TRINO_ROWS).await, reference);
    assert_eq!(trino_sql(&reader, SUMMARY).await, reference_summary);
    let own = |table: &str| {
        format!("SELECT id, nome, valor, lsn, ts, d, origem FROM lenient.codelake_side.{table} ORDER BY id")
    };
    let compacted = trino_sql(&reader, &own("t")).await;
    assert_eq!(compacted, trino_sql(&reader, &own("t_before")).await);
    assert_eq!(compacted.lines().count(), 5, "{compacted}");
    assert!(compacted.contains("\"ação\""), "{compacted}");
    for table in ["hive.trino_side.t", "lenient.codelake_side.t"] {
        let files = trino_sql(&reader, &format!("SELECT count(DISTINCT \"$path\") FROM {table}")).await;
        assert_eq!(files.trim().lines().last(), Some("\"1\""), "{table}: {files}");
    }
}

/// A table Trino wrote through a `MICROSECONDS` catalog keeps every
/// microsecond once `CodeLake` compacts it (scans read milliseconds, as
/// Trino's default catalog does; compaction must not truncate). The Trino
/// CLI declares parametric timestamps, so it prints all six digits.
#[tokio::test]
async fn optimize_keeps_microseconds_for_trino_431() {
    let local = tempfile::tempdir().unwrap();
    let warehouse: PathBuf = local.path().join("warehouse");
    let spill = tempfile::tempdir().unwrap();
    let rows = "SELECT id, ts FROM hive_us.us_side.t ORDER BY id";

    let writer = trino(Vec::new()).await;
    for sql in [
        "CREATE SCHEMA hive.us_side",
        "CREATE TABLE hive_us.us_side.t WITH (format = 'PARQUET') AS \
         SELECT 1 AS id, TIMESTAMP '2026-01-01 10:00:00.123999' AS ts",
        "INSERT INTO hive_us.us_side.t VALUES (2, TIMESTAMP '1700-06-01 00:00:00.000001')",
        "INSERT INTO hive_us.us_side.t VALUES (3, TIMESTAMP '2026-01-01 10:00:00.000500')",
    ] {
        trino_sql(&writer, sql).await;
    }
    let reference = trino_sql(&writer, rows).await;
    assert_eq!(
        reference.trim(),
        "\"id\",\"ts\"\n\"1\",\"2026-01-01 10:00:00.123999\"\n\"2\",\"1700-06-01 00:00:00.000001\"\n\
         \"3\",\"2026-01-01 10:00:00.000500\""
    );
    copy_out(&writer, &warehouse).await;
    drop(writer);

    let lake = engine(&warehouse, spill.path()).await;
    let table = warehouse.join("us_side/t");
    assert_eq!(data_files(&table).len(), 3);
    codelake(&lake, "ALTER TABLE hive.us_side.t EXECUTE optimize").await;
    assert_eq!(data_files(&table).len(), 1);
    drop(lake);

    let reader = trino(read_tree(&warehouse)).await;
    assert_eq!(trino_sql(&reader, rows).await, reference);
}
