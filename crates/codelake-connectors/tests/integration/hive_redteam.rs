//! Red-team probes for the hive catalog (F2, ADR 0007). Each probe asserts the
//! behavior Trino 431 has, or that data safety requires; a failing probe is a
//! finding. Observations are printed with `[F2RT]` so a run is evidence.
//!
//! The probes that run against the warehouse alone, brought from the red-team
//! branch `probe/f2-redteam` as they were written. Its two differential runs
//! against a Trino 431 container stay on that branch: they also check owner
//! security (R11), which belongs to F5 (authentication). So does
//! `rt_wide_insert_fits_a_small_pool`: its SQL `concat(…, repeat('x', 1000))`
//! is not valid Trino 431 SQL (`repeat` returns an array), which the F4
//! function library now follows. And `rt_drop_during_select_never_returns_a_
//! partial_count`, whose expected sum (500000500000) is not the sum of the
//! rows it inserts (16 × 1..250000 = 500002000000): a complete read before
//! the DROP counts as partial.

#![allow(clippy::print_stderr, reason = "probe evidence")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine};
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::util::display::array_value_to_string;

use crate::common;

type Rows = Vec<Vec<String>>;

fn note(what: &str, detail: impl std::fmt::Debug) {
    eprintln!("[F2RT] {what}: {detail:?}");
}

fn params(warehouse: &Path) -> HiveParams {
    HiveParams {
        warehouse: warehouse.display().to_string(),
        s3_endpoint: None,
        s3_region: "us-east-1".into(),
        credentials: None,
        path_style: false,
    }
}

async fn codelake_with(warehouse: &Path, spill: &Path, limit: ByteSize) -> Engine {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    common::attach(
        &mut engine,
        Arc::new(HiveConnector::new("hive".into(), params(warehouse))),
    )
    .await;
    engine
}

async fn codelake(warehouse: &Path, spill: &Path) -> Engine {
    codelake_with(warehouse, spill, ByteSize::mib(256)).await
}

struct Local {
    _dir: tempfile::TempDir,
    _spill: tempfile::TempDir,
    root: PathBuf,
    warehouse: PathBuf,
    engine: Engine,
}

async fn local() -> Local {
    let dir = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let root = dir.path().to_owned();
    let warehouse = root.join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let engine = codelake(&warehouse, spill.path()).await;
    Local {
        _dir: dir,
        _spill: spill,
        root,
        warehouse,
        engine,
    }
}

fn to_rows(batches: &[RecordBatch]) -> Rows {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(row) {
                            "NULL".to_owned()
                        } else {
                            array_value_to_string(c, row).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

async fn cl_as(engine: &Engine, user: &str, sql: &str) -> Result<Rows, String> {
    let frame = engine.sql_as(user, sql).await.map_err(|e| e.to_string())?;
    let batches = frame.collect().await.map_err(|e| e.to_string())?;
    Ok(to_rows(&batches))
}

async fn cl_try(engine: &Engine, sql: &str) -> Result<Rows, String> {
    cl_as(engine, "codelake", sql).await
}

async fn cl(engine: &Engine, sql: &str) -> Rows {
    cl_try(engine, sql)
        .await
        .unwrap_or_else(|e| panic!("CodeLake `{sql}` failed: {e}"))
}

fn rows(values: &[&[&str]]) -> Rows {
    values
        .iter()
        .map(|r| r.iter().map(|v| (*v).to_owned()).collect())
        .collect()
}

/// Every path under `dir`, relative, sorted: evidence of what is on disk.
fn tree(dir: &Path) -> Vec<String> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let entry = entry.unwrap();
            let path = entry.path();
            out.push(path.strip_prefix(base).unwrap().display().to_string());
            if entry.file_type().unwrap().is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// Data loss through DDL.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rt_or_replace_table_reading_itself_keeps_its_rows() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS x FROM (VALUES (1),(2),(3)) v(x)",
    )
    .await;
    let replaced = cl_try(
        &l.engine,
        "CREATE OR REPLACE TABLE hive.s.t AS SELECT x * 10 AS x FROM hive.s.t",
    )
    .await;
    note("OR REPLACE reading itself", &replaced);
    let after = cl_try(&l.engine, "SELECT x FROM hive.s.t ORDER BY x").await;
    note("rows after", &after);
    note("disk", tree(&l.warehouse));
    // Either the replacement (10, 20, 30) or an error with the table intact.
    let intact = after == Ok(rows(&[&["1"], &["2"], &["3"]]));
    let replaced_ok = after == Ok(rows(&[&["10"], &["20"], &["30"]]));
    assert!(
        intact || replaced_ok,
        "rows lost: {after:?} (statement: {replaced:?})"
    );
}

#[tokio::test]
async fn rt_or_replace_table_that_fails_keeps_the_old_table() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.src AS SELECT CAST(x AS BIGINT) AS x FROM (VALUES (1),(2)) v(x)",
    )
    .await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS y FROM (VALUES (7),(8)) v(x)",
    )
    .await;
    let failed = cl_try(
        &l.engine,
        "CREATE OR REPLACE TABLE hive.s.t AS SELECT 1 / (x - x) AS y FROM hive.s.src",
    )
    .await;
    note("failing OR REPLACE", &failed);
    assert!(failed.is_err(), "division by zero must fail");
    let after = cl_try(&l.engine, "SELECT y FROM hive.s.t ORDER BY y").await;
    note("old table after failed replace", &after);
    note("disk", tree(&l.warehouse));
    assert_eq!(after, Ok(rows(&[&["7"], &["8"]])), "the old table must survive");
}

#[tokio::test]
async fn rt_empty_identifier_does_not_reach_the_schema_directory() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS x",
    )
    .await;
    let view = cl_try(&l.engine, "CREATE VIEW hive.s.\"\" AS SELECT 1 AS a").await;
    note("CREATE VIEW hive.s.\"\"", &view);
    note("disk after create", tree(&l.warehouse));
    let dropped = cl_try(&l.engine, "DROP VIEW hive.s.\"\"").await;
    note("DROP VIEW hive.s.\"\"", &dropped);
    note("disk after drop", tree(&l.warehouse));
    let after = cl_try(&l.engine, "SELECT x FROM hive.s.t").await;
    note("table in the same schema", &after);
    assert_eq!(after, Ok(rows(&[&["1"]])), "a table of schema s was destroyed");
    // Trino: "Zero-length delimited identifier not allowed".
    assert!(view.is_err(), "empty identifiers must be rejected");
}

#[tokio::test]
async fn rt_empty_schema_name_does_not_reach_other_schemas() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.victim").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.victim.t AS SELECT CAST(1 AS BIGINT) AS x",
    )
    .await;
    let schema = cl_try(&l.engine, "CREATE SCHEMA hive.\"\"").await;
    note("CREATE SCHEMA hive.\"\"", &schema);
    let view = cl_try(&l.engine, "CREATE VIEW hive.\"\".victim AS SELECT 1 AS a").await;
    note("CREATE VIEW hive.\"\".victim", &view);
    note("disk after create", tree(&l.warehouse));
    let dropped = cl_try(&l.engine, "DROP VIEW hive.\"\".victim").await;
    note("DROP VIEW hive.\"\".victim", &dropped);
    note("disk after drop", tree(&l.warehouse));
    let after = cl_try(&l.engine, "SELECT x FROM hive.victim.t").await;
    note("table of the other schema", &after);
    assert_eq!(after, Ok(rows(&[&["1"]])), "schema victim was destroyed");
}

#[tokio::test]
async fn rt_names_never_escape_the_warehouse() {
    let l = local().await;
    let outside = l.root.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("keep"), b"x").unwrap();
    for name in ["..", "../outside", "a/b", "..\\x", ".", "%2E%2E", "ção/../.."] {
        let schema = cl_try(&l.engine, &format!("CREATE SCHEMA hive.\"{name}\"")).await;
        let ctas = cl_try(
            &l.engine,
            &format!("CREATE TABLE hive.\"{name}\".\"{name}\" AS SELECT 1 AS a"),
        )
        .await;
        let drop = cl_try(&l.engine, &format!("DROP TABLE hive.\"{name}\".\"{name}\"")).await;
        note(&format!("name {name}"), (&schema, &ctas, &drop));
    }
    note("root", tree(&l.root));
    assert!(outside.join("keep").exists());
    assert_eq!(tree(&outside), vec!["keep".to_owned()]);
    for entry in tree(&l.root) {
        assert!(
            entry.starts_with("warehouse") || entry.starts_with("outside"),
            "{entry}"
        );
    }
}

// ---------------------------------------------------------------------------
// Concurrency.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_concurrent_ctas_of_one_name_never_mixes_rows() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let mut bad = Vec::new();
    for i in 0..8 {
        let name = format!("hive.s.c{i}");
        let sql = |tag: &str| {
            format!(
                "CREATE TABLE {name} AS SELECT CAST(value AS BIGINT) AS v, '{tag}' AS w \
                 FROM generate_series(1, 200000)"
            )
        };
        let (sa, sb) = (sql("a"), sql("b"));
        let (a, b) = tokio::join!(cl_try(&l.engine, &sa), cl_try(&l.engine, &sb));
        let content = cl_try(
            &l.engine,
            &format!("SELECT w, count(*) FROM {name} GROUP BY w ORDER BY w"),
        )
        .await;
        note(&format!("round {i}"), (&a, &b, &content));
        let winners = usize::from(a.is_ok()) + usize::from(b.is_ok());
        let ok = winners == 1
            && content.as_ref().is_ok_and(|rows| {
                rows.len() == 1 && rows[0][1] == "200000" && (rows[0][0] == "a") == a.is_ok()
            });
        if !ok {
            bad.push((i, a, b, content));
        }
    }
    note("disk", tree(&l.warehouse.join("s")));
    assert!(bad.is_empty(), "{bad:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_concurrent_inserts_keep_every_row() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(0 AS BIGINT) AS v",
    )
    .await;
    let insert = "INSERT INTO hive.s.t SELECT CAST(value AS BIGINT) FROM generate_series(1, 10000)";
    for _ in 0..5 {
        let (a, b) = tokio::join!(cl_try(&l.engine, insert), cl_try(&l.engine, insert));
        assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
    }
    let count = cl(&l.engine, "SELECT count(*) FROM hive.s.t").await;
    let files = tree(&l.warehouse.join("s/t"))
        .into_iter()
        .filter(|f| !f.starts_with('.'))
        .count();
    note("rows/files", (&count, files));
    assert_eq!(count, rows(&[&["100001"]]));
    assert_eq!(files, 11);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_concurrent_add_columns_are_all_kept() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(0 AS BIGINT) AS v",
    )
    .await;
    let mut lost = Vec::new();
    for i in 0..10 {
        let a = format!("ALTER TABLE hive.s.t ADD COLUMN a{i} BIGINT");
        let b = format!("ALTER TABLE hive.s.t ADD COLUMN b{i} BIGINT");
        let (ra, rb) = tokio::join!(cl_try(&l.engine, &a), cl_try(&l.engine, &b));
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(l.warehouse.join("s/t/.trinoSchema")).unwrap()).unwrap();
        let names: Vec<String> = metadata["dataColumns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_owned())
            .collect();
        for (column, result) in [(format!("a{i}"), &ra), (format!("b{i}"), &rb)] {
            if result.is_ok() && !names.contains(&column) {
                lost.push((column, names.clone()));
            }
        }
    }
    note("lost columns (statement said OK)", &lost);
    assert!(lost.is_empty(), "{lost:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_drop_during_insert_frees_the_name() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let mut bad = Vec::new();
    for (i, delay) in [5_u64, 30, 100, 300, 800].into_iter().enumerate() {
        let name = format!("hive.s.d{i}");
        cl(
            &l.engine,
            &format!("CREATE TABLE {name} AS SELECT CAST(0 AS BIGINT) AS v"),
        )
        .await;
        let insert =
            format!("INSERT INTO {name} SELECT CAST(value AS BIGINT) FROM generate_series(1, 3000000)");
        let drop = async {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            cl_try(&l.engine, &format!("DROP TABLE {name}")).await
        };
        let (ins, dropped) = tokio::join!(cl_try(&l.engine, &insert), drop);
        let left = tree(&l.warehouse.join(format!("s/d{i}")));
        let recreate = cl_try(
            &l.engine,
            &format!("CREATE TABLE {name} AS SELECT CAST(1 AS BIGINT) AS v"),
        )
        .await;
        note(&format!("delay {delay}ms"), (&ins, &dropped, &left, &recreate));
        if dropped.is_ok() && recreate.is_err() {
            bad.push((delay, ins, left, recreate));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

// ---------------------------------------------------------------------------
// Stored view SQL.
// ---------------------------------------------------------------------------

/// Replaces the SQL of a stored view (as someone with write access to the
/// warehouse, or a buggy writer, could).
fn tamper_view(file: &Path, sql: &str) {
    let mut metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let text = metadata["viewOriginalText"].as_str().unwrap().to_owned();
    let encoded = text
        .strip_prefix("/* Presto View: ")
        .and_then(|r| r.strip_suffix(" */"))
        .unwrap();
    let mut view: serde_json::Value = serde_json::from_slice(&BASE64.decode(encoded).unwrap()).unwrap();
    view["originalSql"] = serde_json::Value::String(sql.to_owned());
    view["columns"] = serde_json::json!([{"name": "count", "type": "bigint"}]);
    metadata["viewOriginalText"] =
        serde_json::Value::String(format!("/* Presto View: {} */", BASE64.encode(view.to_string())));
    std::fs::write(file, serde_json::to_vec_pretty(&metadata).unwrap()).unwrap();
}

#[tokio::test]
async fn rt_reading_a_view_never_writes() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    cl(&l.engine, "CREATE VIEW hive.s.ins AS SELECT 1 AS count").await;
    cl(&l.engine, "CREATE VIEW hive.s.cp AS SELECT 1 AS count").await;
    tamper_view(
        &l.warehouse.join("s/ins/.trinoSchema"),
        "INSERT INTO hive.s.t VALUES (99)",
    );
    let target = l.root.join("written-by-a-select.csv");
    tamper_view(
        &l.warehouse.join("s/cp/.trinoSchema"),
        &format!("COPY (SELECT 'x' AS a) TO '{}' STORED AS CSV", target.display()),
    );
    let ins = cl_try(&l.engine, "SELECT * FROM hive.s.ins").await;
    let cp = cl_try(&l.engine, "SELECT * FROM hive.s.cp").await;
    note("SELECT from INSERT view", &ins);
    note("SELECT from COPY view", &cp);
    let t = cl(&l.engine, "SELECT v FROM hive.s.t ORDER BY v").await;
    note("rows of t", &t);
    note("file written by SELECT", (target.display(), target.exists()));
    assert_eq!(t, rows(&[&["1"]]), "a SELECT inserted rows");
    assert!(!target.exists(), "a SELECT wrote a server file");
}

// ---------------------------------------------------------------------------
// Catalog listing robust to tables CodeLake cannot read.
// ---------------------------------------------------------------------------

fn patch_table(file: &Path, patch: impl FnOnce(&mut serde_json::Value)) {
    let mut metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    patch(&mut metadata);
    std::fs::write(file, serde_json::to_vec_pretty(&metadata).unwrap()).unwrap();
}

#[tokio::test]
async fn rt_one_unreadable_table_does_not_break_the_catalog() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    for t in ["good", "arr", "part", "broken"] {
        cl(
            &l.engine,
            &format!("CREATE TABLE hive.s.{t} AS SELECT CAST(1 AS BIGINT) AS x"),
        )
        .await;
    }
    // What Trino writes for `a array(integer)` and a partitioned table.
    patch_table(&l.warehouse.join("s/arr/.trinoSchema"), |m| {
        m["dataColumns"][0]["type"] = "array<int>".into();
    });
    patch_table(&l.warehouse.join("s/part/.trinoSchema"), |m| {
        m["partitionColumns"] = serde_json::json!([{"name": "p", "type": "string", "properties": {}}]);
    });
    std::fs::write(l.warehouse.join("s/broken/.trinoSchema"), b"{\"tableType\": ").unwrap();
    let mut failures = Vec::new();
    for sql in [
        "SELECT x FROM hive.s.good",
        "SELECT table_name FROM hive.information_schema.tables WHERE table_schema = 's' ORDER BY 1",
        "SELECT table_name, column_name FROM hive.information_schema.columns \
         WHERE table_schema = 's' AND table_name = 'good'",
        "SELECT count(*) FROM hive.information_schema.columns WHERE table_schema = 's'",
        "SHOW TABLES FROM hive.s",
    ] {
        let result = cl_try(&l.engine, sql).await;
        note(sql, &result);
        if result.is_err() {
            failures.push((sql, result));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// ---------------------------------------------------------------------------
// Client statements that reach the warehouse without the catalog.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rt_client_cannot_write_or_read_warehouse_files_directly() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    // Trino has no COPY and no CREATE EXTERNAL TABLE: a client only reaches
    // files through a catalog.
    let copy = cl_try(
        &l.engine,
        &format!(
            "COPY (SELECT CAST(666 AS BIGINT) AS v) TO '{}' STORED AS PARQUET",
            l.warehouse.join("s/t/injected.parquet").display()
        ),
    )
    .await;
    note("client COPY into a table directory", &copy);
    let rows_after = cl_try(&l.engine, "SELECT v FROM hive.s.t ORDER BY v").await;
    note("rows of hive.s.t after COPY", &rows_after);
    let external = cl_try(
        &l.engine,
        &format!(
            "CREATE EXTERNAL TABLE leak (line VARCHAR) STORED AS CSV LOCATION '{}' \
             OPTIONS ('format.has_header' 'false')",
            "/etc/hostname"
        ),
    )
    .await;
    let leak = cl_try(&l.engine, "SELECT line FROM leak").await;
    note(
        "client CREATE EXTERNAL TABLE on a server file",
        (&external, &leak),
    );
    assert!(copy.is_err(), "COPY TO wrote into the warehouse");
    assert!(external.is_err() || leak.is_err(), "server file read by a client");
}

#[tokio::test]
async fn rt_failed_insert_leaves_no_rows_behind() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    // Statistics as Trino records them, so the INSERT has metadata to rewrite,
    // and a metadata write that fails (injected: the checksum path is taken).
    patch_table(&l.warehouse.join("s/t/.trinoSchema"), |m| {
        m["parameters"]["numRows"] = "1".into();
    });
    let crc = l.warehouse.join("s/t/..trinoSchema.crc");
    std::fs::remove_file(&crc).unwrap();
    std::fs::create_dir_all(crc.join("blocker")).unwrap();
    let insert = cl_try(&l.engine, "INSERT INTO hive.s.t VALUES (2), (3)").await;
    note("INSERT whose metadata update fails", &insert);
    let after = cl(&l.engine, "SELECT v FROM hive.s.t ORDER BY v").await;
    note("rows after the failed INSERT", &after);
    assert!(insert.is_err(), "the injected failure must surface");
    assert_eq!(after, rows(&[&["1"]]), "a failed INSERT committed rows");
}

// ---------------------------------------------------------------------------
// Round 2: more concurrency, DROP while reading, types/views/owners vs Trino.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_failing_concurrent_ctas_never_deletes_the_winner() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let mut bad = Vec::new();
    for i in 0..6 {
        let name = format!("hive.s.w{i}");
        let good = format!(
            "CREATE TABLE {name} AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, 2000000)"
        );
        // Fails (division by zero) half-way, after both passed the existence checks.
        let failing = format!(
            "CREATE TABLE {name} AS SELECT CAST(1 / (value - 1000000) AS BIGINT) AS v \
             FROM generate_series(1, 2000000)"
        );
        let (a, b) = tokio::join!(cl_try(&l.engine, &good), cl_try(&l.engine, &failing));
        let count = cl_try(&l.engine, &format!("SELECT count(*) FROM {name}")).await;
        note(&format!("round {i}"), (&a, &b, &count));
        if a.is_ok() && count != Ok(rows(&[&["2000000"]])) {
            bad.push((i, a, b, count));
        }
    }
    assert!(
        bad.is_empty(),
        "a CTAS reported success and its table is gone or partial: {bad:#?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_insert_racing_add_column_keeps_the_column() {
    let l = local().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(0 AS BIGINT) AS v",
    )
    .await;
    let file = l.warehouse.join("s/t/.trinoSchema");
    let mut lost = Vec::new();
    for i in 0..10 {
        // A table Trino wrote to last has statistics; CodeLake's INSERT rewrites them.
        patch_table(&file, |m| {
            m["parameters"]["numRows"] = "1".into();
        });
        let insert = cl_try(
            &l.engine,
            "INSERT INTO hive.s.t (v) SELECT CAST(value AS BIGINT) FROM generate_series(1, 20000)",
        );
        let add = format!("ALTER TABLE hive.s.t ADD COLUMN c{i} BIGINT");
        let (ins, added) = tokio::join!(insert, cl_try(&l.engine, &add));
        let metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        let names: Vec<String> = metadata["dataColumns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_owned())
            .collect();
        if added.is_ok() && !names.contains(&format!("c{i}")) {
            lost.push((format!("c{i}"), ins, names));
        }
    }
    note("columns lost to a concurrent INSERT", &lost);
    assert!(lost.is_empty(), "{lost:#?}");
}
