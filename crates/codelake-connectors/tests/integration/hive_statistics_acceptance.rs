//! Independent acceptance suite for the statistics the hive scan hands to the
//! planner. Written by a tester who did not implement them.
//!
//! What must hold:
//! - rows: statistics may change the plan, never the result. Every query runs
//!   on Trino 431 (the reference), on `CodeLake` with statistics and on
//!   `CodeLake` with `datafusion.execution.collect_statistics = false`; the
//!   three must return the same rows. That includes tables whose statistics
//!   lie (row count, bounds, null counts), are stale (files added by another
//!   writer, with or without the counters updated), describe an empty table
//!   while files exist, and files written by a writer other than Trino and
//!   `CodeLake` with types that differ from the table's.
//! - plan: the smaller input builds the hash table and a small dimension is
//!   collected once (`CollectLeft`), for tables written by Trino (metastore
//!   statistics) and by `CodeLake` (footer statistics).
//! - cost: planning over thousands of files stays bounded, the footer cache
//!   stays under `memory.metadata_cache_limit`, the pool is given back and a
//!   pool that refuses the footer read only loses the statistics.
//!
//! The warehouse is shared with the Trino container the same way as in
//! `hive_acceptance.rs` (same absolute path in both containers).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine};
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::util::display::array_value_to_string;
use serde_json::{Value, json};
use testcontainers::bollard::Docker;
use testcontainers::bollard::models::MountPointTypeEnum;
use testcontainers::core::{CmdWaitFor, ExecCommand, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);
const SHARED_MOUNT: &str = "/work/target";
/// Prefix of the Trino containers (one per test, suffixed).
const TRINO_CONTAINER: &str = "codelake-hs-trino";

type Rows = Vec<Vec<String>>;

#[allow(clippy::print_stderr, reason = "evidence for the acceptance report")]
fn note(what: &str, detail: impl std::fmt::Display) {
    eprintln!("[HS] {what}: {detail}");
}

// ---------------------------------------------------------------------------
// Shared warehouse directory (same absolute path here and in Trino).
// ---------------------------------------------------------------------------

struct Shared {
    dir: PathBuf,
    mount: Mount,
}

impl Shared {
    fn warehouse(&self) -> PathBuf {
        self.dir.join("warehouse")
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn own_mount(destination: &str) -> Option<Mount> {
    let id = std::fs::read_to_string("/etc/hostname").ok()?.trim().to_owned();
    let docker = Docker::connect_with_local_defaults().ok()?;
    let info = docker.inspect_container(&id, None).await.ok()?;
    let point = info
        .mounts?
        .into_iter()
        .find(|m| m.destination.as_deref() == Some(destination))?;
    match point.typ? {
        MountPointTypeEnum::VOLUME => Some(Mount::volume_mount(point.name?, destination)),
        MountPointTypeEnum::BIND => Some(Mount::bind_mount(point.source?, destination)),
        _ => None,
    }
}

async fn shared() -> Shared {
    let run = format!("run-{}", uuid::Uuid::new_v4().simple());
    let (base, mount) = if let Some(mount) = own_mount(SHARED_MOUNT).await {
        (Path::new(SHARED_MOUNT).join("hive-stats-acceptance"), mount)
    } else {
        let base = std::env::temp_dir().join("codelake-hive-stats-acceptance");
        let path = base.display().to_string();
        (base, Mount::bind_mount(path.clone(), path))
    };
    let dir = base.join(run);
    std::fs::create_dir_all(dir.join("warehouse")).unwrap();
    Shared { dir, mount }
}

// ---------------------------------------------------------------------------
// Trino 431, the reference.
// ---------------------------------------------------------------------------

struct Trino(ContainerAsync<GenericImage>);

impl Trino {
    async fn start(shared: &Shared, name: &str) -> Self {
        let properties = format!(
            "connector.name=hive\n\
             hive.metastore=file\n\
             hive.metastore.catalog.dir=file:{}\n\
             hive.storage-format=PARQUET\n\
             hive.allow-drop-table=true\n",
            shared.warehouse().display()
        );
        let container = GenericImage::new("trinodb/trino", "431")
            .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
            .with_user("root")
            .with_container_name(format!("{TRINO_CONTAINER}-{name}"))
            .with_mount(shared.mount.clone())
            .with_copy_to("/etc/trino/catalog/hive.properties", properties.into_bytes())
            .with_startup_timeout(STARTUP_TIMEOUT)
            .start()
            .await
            .unwrap();
        let trino = Self(container);
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            match trino.try_sql("SELECT 1").await {
                Ok(_) => break,
                Err(e) if Instant::now() < deadline && e.contains("No nodes available") => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => panic!("Trino did not accept queries: {e}"),
            }
        }
        trino
    }

    async fn try_sql(&self, sql: &str) -> Result<Rows, String> {
        let args = vec![
            "trino",
            "--debug",
            "--output-format",
            "TSV",
            "--catalog",
            "hive",
            "--execute",
            sql,
        ];
        let mut result = self
            .0
            .exec(ExecCommand::new(args).with_cmd_ready_condition(CmdWaitFor::exit()))
            .await
            .unwrap();
        let stdout = String::from_utf8(result.stdout_to_vec().await.unwrap()).unwrap();
        let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap()).into_owned();
        let mut code = None;
        for _ in 0..50 {
            code = result.exit_code().await.unwrap();
            if code.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let errors: Vec<&str> = stderr
            .lines()
            .filter(|l| {
                (l.starts_with("Query ") && l.contains(" failed"))
                    || l.starts_with("Caused by:")
                    || (!l.starts_with(char::is_whitespace) && l.contains("Exception: "))
            })
            .collect();
        if code != Some(0) || !errors.is_empty() {
            return Err(format!("exit {code:?}: {}", errors.join(" | ")));
        }
        Ok(stdout
            .lines()
            .map(|line| line.split('\t').map(str::to_owned).collect())
            .collect())
    }

    async fn sql(&self, sql: &str) -> Rows {
        self.try_sql(sql)
            .await
            .unwrap_or_else(|e| panic!("Trino `{sql}` failed: {e}"))
    }

    /// Runs `statements` in one CLI call (each start of the CLI costs
    /// seconds) and splits the output by a marker row.
    async fn batch(&self, statements: &[&str]) -> Vec<Rows> {
        const MARK: &str = "@@end-of-statement@@";
        let mut script = String::new();
        for statement in statements {
            script.push_str(statement);
            script.push_str(";\nSELECT '");
            script.push_str(MARK);
            script.push_str("';\n");
        }
        let mut results = vec![Vec::new()];
        for row in self.sql(&script).await {
            if row == [MARK] {
                results.push(Vec::new());
            } else {
                results.last_mut().unwrap().push(row);
            }
        }
        results.pop();
        assert_eq!(results.len(), statements.len(), "Trino batch output");
        results
    }
}

// ---------------------------------------------------------------------------
// CodeLake side.
// ---------------------------------------------------------------------------

fn memory(spill: &Path, limit: ByteSize, metadata_cache_limit: ByteSize) -> MemoryConfig {
    MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit,
    }
}

async fn open(warehouse: &Path, memory: &MemoryConfig, statistics: bool) -> Engine {
    let mut engine = Engine::new(memory, &ExecutionConfig::default()).expect("engine");
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
    if !statistics {
        set(&engine, "datafusion.execution.collect_statistics", "false").await;
    }
    engine
}

async fn set(engine: &Engine, key: &str, value: &str) {
    engine
        .context()
        .sql(&format!("SET {key} = {value}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

fn to_rows(batches: &[RecordBatch]) -> Rows {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            "NULL".to_owned()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

async fn cl(engine: &Engine, sql: &str) -> Rows {
    let frame = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("CodeLake planning `{sql}` failed: {e}"));
    to_rows(
        &frame
            .collect()
            .await
            .unwrap_or_else(|e| panic!("CodeLake running `{sql}` failed: {e}")),
    )
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

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// Every hash join of `plan`: its line and the subtree of its build (first)
/// child.
fn hash_joins(plan: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = plan.lines().collect();
    let mut joins = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("HashJoinExec") {
            continue;
        }
        let child = indent(line) + 2;
        let Some(start) = (i + 1..lines.len()).find(|&j| indent(lines[j]) == child) else {
            continue;
        };
        let end = (start + 1..lines.len())
            .find(|&j| indent(lines[j]) <= child)
            .unwrap_or(lines.len());
        joins.push(((*line).to_owned(), lines[start..end].join("\n")));
    }
    assert!(!joins.is_empty(), "no hash join in:\n{plan}");
    joins
}

/// The `DataSourceExec` line that scans `table` (schema `hs`).
fn scan_line<'a>(plan: &'a str, table: &str) -> &'a str {
    let needle = format!("hs/{table}/");
    plan.lines()
        .find(|l| l.contains("DataSourceExec") && l.contains(&needle))
        .unwrap_or_else(|| panic!("no scan of {table}:\n{plan}"))
}

/// Row estimate the scan of `hs.<table>` reports.
async fn estimated_rows(engine: &Engine, table: &str) -> String {
    set(engine, "datafusion.explain.show_statistics", "true").await;
    let plan = explain(engine, &format!("SELECT * FROM hive.hs.{table}")).await;
    set(engine, "datafusion.explain.show_statistics", "false").await;
    let line = scan_line(&plan, table);
    let start = line
        .find("Rows=")
        .unwrap_or_else(|| panic!("no row statistics: {line}"));
    line[start..].split([',', ']']).next().unwrap().to_owned()
}

fn sorted(mut rows: Rows) -> Rows {
    rows.sort();
    rows
}

// ---------------------------------------------------------------------------
// Warehouse surgery: what other writers (or a broken one) leave behind.
// ---------------------------------------------------------------------------

fn table_dir(warehouse: &Path, table: &str) -> PathBuf {
    warehouse.join("hs").join(table)
}

fn data_files(warehouse: &Path, table: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(table_dir(warehouse, table))
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            e.file_type().unwrap().is_file() && !name.starts_with(['.', '_'])
        })
        .map(|e| e.path())
        .collect();
    files.sort();
    files
}

/// Hadoop `ChecksumFileSystem` checksum, which Trino verifies on read.
fn hadoop_crc(data: &[u8]) -> Vec<u8> {
    let mut out = b"crc\0".to_vec();
    out.extend_from_slice(&512_i32.to_be_bytes());
    for chunk in data.chunks(512) {
        out.extend_from_slice(&crc32fast::hash(chunk).to_be_bytes());
    }
    out
}

/// Rewrites `hs.<table>`'s metadata (and its checksum, so Trino still reads it).
fn tamper(warehouse: &Path, table: &str, edit: impl FnOnce(&mut Value)) {
    let dir = table_dir(warehouse, table);
    let path = dir.join(".trinoSchema");
    let mut metadata: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    edit(&mut metadata);
    let bytes = serde_json::to_vec_pretty(&metadata).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    std::fs::write(dir.join("..trinoSchema.crc"), hadoop_crc(&bytes)).unwrap();
}

/// Sets `numFiles`/`totalSize` to what is on disk now, keeping every other
/// statistic: a writer that updates the counters and nothing else.
fn recount(warehouse: &Path, table: &str) {
    let files = data_files(warehouse, table);
    let size: u64 = files.iter().map(|f| f.metadata().unwrap().len()).sum();
    tamper(warehouse, table, |m| {
        m["parameters"]["numFiles"] = json!(files.len().to_string());
        m["parameters"]["totalSize"] = json!(size.to_string());
    });
}

// ---------------------------------------------------------------------------
// The data set.
// ---------------------------------------------------------------------------

/// 200 000 rows; `f_key` in 0..29 (25..29 have no dimension row), NULL on
/// 200 rows.
const FACT: &str = "CREATE TABLE hive.hs.fact AS \
    SELECT CAST(b.j * 1000 + a.i AS bigint) AS f_id, \
           CASE WHEN a.i = 7 THEN NULL ELSE CAST((b.j * 1000 + a.i) % 30 AS bigint) END AS f_key, \
           CAST(a.i % 50 AS integer) AS f_qty, \
           date_add('day', a.i % 400, DATE '2025-01-01') AS f_day, \
           CAST((b.j * 1000 + a.i) % 997 AS decimal(15,2)) AS f_amt, \
           'n' || CAST(a.i % 13 AS varchar) AS f_tag \
    FROM UNNEST(sequence(0, 999)) a(i) CROSS JOIN UNNEST(sequence(0, 199)) b(j)";

const TRINO_SETUP: [&str; 11] = [
    "CREATE SCHEMA hive.hs",
    FACT,
    "CREATE TABLE hive.hs.dim AS SELECT CAST(x AS bigint) AS n_key, 'nation ' || CAST(x AS varchar) AS n_name \
     FROM UNNEST(sequence(0, 24)) t(x)",
    "CREATE TABLE hive.hs.dim_null AS SELECT * FROM hive.hs.dim UNION ALL SELECT CAST(NULL AS bigint), 'none'",
    "CREATE TABLE hive.hs.dim_huge AS SELECT * FROM hive.hs.dim",
    "CREATE TABLE hive.hs.liar AS SELECT * FROM hive.hs.fact WHERE f_id < 100000",
    "CREATE TABLE hive.hs.zero AS SELECT * FROM hive.hs.fact WHERE f_id < 50000",
    "CREATE TABLE hive.hs.empty AS SELECT * FROM hive.hs.fact WHERE f_id < 0",
    "CREATE TABLE hive.hs.stale AS SELECT * FROM hive.hs.fact WHERE f_id < 10000",
    "CREATE TABLE hive.hs.stale_counted AS SELECT * FROM hive.hs.fact WHERE f_id < 10000",
    "CREATE TABLE hive.hs.stale_extra AS SELECT * FROM hive.hs.fact WHERE f_id >= 190000",
];

/// Written by `CodeLake` (footer statistics: its INSERT drops the metastore's).
const CODELAKE_SETUP: [&str; 3] = [
    "CREATE TABLE hive.hs.mid AS SELECT CAST(value * 10 AS BIGINT) AS m_fact, \
       CAST(value % 25 AS BIGINT) AS m_key, CONCAT('m', CAST(value AS VARCHAR)) AS m_name \
     FROM generate_series(0, 14999)",
    "INSERT INTO hive.hs.mid SELECT CAST(value * 10 AS BIGINT), CAST(value % 25 AS BIGINT), \
       CONCAT('m', CAST(value AS VARCHAR)) FROM generate_series(15000, 19999)",
    "INSERT INTO hive.hs.mid SELECT CAST(value * 10 AS BIGINT), CAST(value % 25 AS BIGINT), 'late' \
     FROM generate_series(20000, 20999)",
];

/// Statistics that lie while `numFiles`/`totalSize` still match the files.
fn lie(warehouse: &Path) {
    // liar: 100 000 rows recorded as 1; bounds far narrower than the data;
    // no NULL recorded where there are 100.
    tamper(warehouse, "liar", |m| {
        m["parameters"]["numRows"] = json!("1");
        let c = &mut m["columnStatistics"];
        c["f_id"]["integerStatistics"] = json!({"min": 0, "max": 10});
        c["f_key"]["integerStatistics"] = json!({"min": 0, "max": 1});
        c["f_key"]["nullsCount"] = json!(0);
        c["f_qty"]["integerStatistics"] = json!({"min": 0, "max": 0});
        c["f_day"]["dateStatistics"] = json!({"min": "2025-01-01", "max": "2025-01-02"});
        c["f_amt"]["decimalStatistics"] = json!({"min": 0, "max": 1});
    });
    // zero: data files present, metastore says the table is empty and every
    // value is NULL.
    tamper(warehouse, "zero", |m| {
        m["parameters"]["numRows"] = json!("0");
        let c = &mut m["columnStatistics"];
        for column in ["f_id", "f_key", "f_qty", "f_day", "f_amt", "f_tag"] {
            c[column]["nullsCount"] = json!(0);
        }
        c["f_id"]["integerStatistics"] = json!({"min": -5, "max": -1});
    });
    // dim_huge: 25 rows recorded as a trillion.
    tamper(warehouse, "dim_huge", |m| {
        m["parameters"]["numRows"] = json!("999999999999");
    });
}

/// Files added by another writer without touching the metastore (`stale`),
/// and the same with only the counters updated (`stale_counted`: row count and
/// bounds still describe the first 10 000 rows).
fn add_foreign_files(warehouse: &Path) {
    for (i, file) in data_files(warehouse, "stale_extra").iter().enumerate() {
        for table in ["stale", "stale_counted"] {
            std::fs::copy(
                file,
                table_dir(warehouse, table).join(format!("zz_other_writer_{i}")),
            )
            .unwrap();
        }
    }
    recount(warehouse, "stale_counted");
}

// ---------------------------------------------------------------------------
// The queries: every one must return the same rows on the three engines.
// ---------------------------------------------------------------------------

const QUERIES: &[(&str, &str)] = &[
    (
        "inner fact x dim",
        "SELECT d.n_name, count(*), sum(f.f_qty) FROM hive.hs.fact f JOIN hive.hs.dim d ON f.f_key = d.n_key \
         GROUP BY d.n_name",
    ),
    (
        "left dim -> fact with extra condition",
        "SELECT d.n_key, count(f.f_id) FROM hive.hs.dim d LEFT JOIN hive.hs.fact f \
         ON f.f_key = d.n_key AND f.f_qty > 45 GROUP BY d.n_key",
    ),
    (
        "left fact -> dim with unmatched and NULL keys",
        "SELECT coalesce(d.n_name, 'NULL'), count(*), count(f.f_key) FROM hive.hs.fact f \
         LEFT JOIN hive.hs.dim d ON f.f_key = d.n_key GROUP BY 1",
    ),
    (
        "right fact -> dim",
        "SELECT d.n_key, count(f.f_id) FROM hive.hs.fact f RIGHT JOIN hive.hs.dim d \
         ON f.f_key = d.n_key AND f.f_id < 100 GROUP BY d.n_key",
    ),
    (
        "full fact <-> dim",
        "SELECT count(*), count(f.f_id), count(d.n_key) FROM hive.hs.fact f FULL JOIN hive.hs.dim d \
         ON f.f_key = d.n_key AND f.f_qty = 3",
    ),
    (
        "semi IN",
        "SELECT count(*), sum(f_id) FROM hive.hs.fact WHERE f_key IN (SELECT n_key FROM hive.hs.dim WHERE n_key < 5)",
    ),
    (
        "semi EXISTS",
        "SELECT count(*) FROM hive.hs.dim d WHERE EXISTS \
         (SELECT 1 FROM hive.hs.fact f WHERE f.f_key = d.n_key AND f.f_qty = 49)",
    ),
    (
        "anti NOT IN (no NULL in subquery)",
        "SELECT count(*) FROM hive.hs.fact WHERE f_key NOT IN (SELECT n_key FROM hive.hs.dim)",
    ),
    (
        "anti NOT IN (NULL in subquery)",
        "SELECT count(*) FROM hive.hs.fact WHERE f_key NOT IN (SELECT n_key FROM hive.hs.dim_null)",
    ),
    (
        "anti NOT IN (big subquery with NULLs)",
        "SELECT count(*) FROM hive.hs.dim WHERE n_key NOT IN (SELECT f_key FROM hive.hs.fact)",
    ),
    (
        "anti NOT EXISTS",
        "SELECT count(*) FROM hive.hs.fact f WHERE NOT EXISTS (SELECT 1 FROM hive.hs.dim d WHERE d.n_key = f.f_key)",
    ),
    (
        "three tables",
        "SELECT d.n_name, count(*), sum(m.m_key) FROM hive.hs.fact f JOIN hive.hs.mid m ON f.f_id = m.m_fact \
         JOIN hive.hs.dim d ON f.f_key = d.n_key GROUP BY d.n_name",
    ),
    (
        "four tables with the liar",
        "SELECT count(*), sum(l.f_qty), sum(m.m_key) FROM hive.hs.liar l JOIN hive.hs.fact f ON l.f_id = f.f_id \
         JOIN hive.hs.mid m ON m.m_fact = f.f_id JOIN hive.hs.dim d ON d.n_key = l.f_key",
    ),
    (
        "semi IN over an aggregate (Q18 shape)",
        "SELECT f.f_key, count(*) FROM hive.hs.fact f WHERE f.f_id IN \
         (SELECT m_fact FROM hive.hs.mid GROUP BY m_fact HAVING count(*) >= 1) GROUP BY f.f_key",
    ),
    (
        "liar: filters outside the recorded bounds",
        "SELECT count(*), min(f_id), max(f_id), count(f_key), min(f_day), max(f_day), CAST(max(f_amt) AS bigint) \
         FROM hive.hs.liar WHERE f_id > 50 AND f_day > DATE '2025-01-05' AND f_amt > 2 AND f_qty > 0",
    ),
    (
        "liar: aggregates without a filter",
        "SELECT count(*), min(f_id), max(f_id), min(f_key), max(f_key), count(f_key) FROM hive.hs.liar",
    ),
    (
        "liar: NULLs the metastore denies",
        "SELECT count(*) FROM hive.hs.liar WHERE f_key IS NULL",
    ),
    (
        "liar: right join to a dimension",
        "SELECT d.n_key, count(l.f_id) FROM hive.hs.liar l RIGHT JOIN hive.hs.dim d ON l.f_key = d.n_key \
         GROUP BY d.n_key",
    ),
    (
        "liar: full join to a dimension recorded as huge",
        "SELECT count(*), count(d.n_key), count(l.f_id) FROM hive.hs.liar l FULL JOIN hive.hs.dim_huge d \
         ON l.f_key = d.n_key",
    ),
    (
        "dimension recorded as huge",
        "SELECT d.n_name, count(*) FROM hive.hs.fact f JOIN hive.hs.dim_huge d ON f.f_key = d.n_key GROUP BY d.n_name",
    ),
    (
        "recorded as empty: inner join",
        "SELECT count(*), sum(z.f_qty) FROM hive.hs.zero z JOIN hive.hs.dim d ON z.f_key = d.n_key",
    ),
    // `count(*)` alone over `zero` is DEFECT-HS1 (see below): Trino returns
    // 50000 for `... WHERE f_id IN (SELECT f_id FROM hive.hs.zero)`,
    // CodeLake with statistics 0.
    (
        "recorded as empty: NOT IN",
        "SELECT count(*) FROM hive.hs.dim WHERE n_key NOT IN (SELECT f_key FROM hive.hs.zero WHERE f_key IS NOT NULL)",
    ),
    (
        "recorded as empty: count and bounds",
        "SELECT count(*), min(f_id), max(f_id), count(f_key) FROM hive.hs.zero",
    ),
    (
        "empty: inner join",
        "SELECT count(*) FROM hive.hs.fact f JOIN hive.hs.empty e ON f.f_id = e.f_id",
    ),
    (
        "empty: left join",
        "SELECT count(*), count(e.f_id) FROM hive.hs.dim d LEFT JOIN hive.hs.empty e ON d.n_key = e.f_key",
    ),
    (
        "empty: NOT IN",
        "SELECT count(*) FROM hive.hs.dim WHERE n_key NOT IN (SELECT f_key FROM hive.hs.empty)",
    ),
    (
        "empty: NOT EXISTS",
        "SELECT count(*) FROM hive.hs.dim d WHERE NOT EXISTS (SELECT 1 FROM hive.hs.empty e WHERE e.f_key = d.n_key)",
    ),
    (
        "stale: rows of the added files",
        "SELECT count(*), min(f_id), max(f_id) FROM hive.hs.stale WHERE f_id >= 10000",
    ),
    (
        "stale: join on the added rows",
        "SELECT count(*), sum(f.f_qty) FROM hive.hs.stale s JOIN hive.hs.fact f ON s.f_id = f.f_id \
         WHERE s.f_id > 150000",
    ),
    (
        "stale counters: rows beyond the recorded bounds",
        "SELECT count(*), min(f_id), max(f_id) FROM hive.hs.stale_counted WHERE f_id >= 10000",
    ),
    (
        "stale counters: join",
        "SELECT d.n_name, count(*) FROM hive.hs.stale_counted s JOIN hive.hs.dim d ON s.f_key = d.n_key \
         WHERE s.f_id > 195000 GROUP BY d.n_name",
    ),
    (
        "footer bounds of Trino files: strings, dates, decimals",
        "SELECT count(*), min(f_tag), max(f_tag), min(f_day), max(f_day) FROM hive.hs.stale \
         WHERE f_tag > 'n5' AND f_day BETWEEN DATE '2025-02-01' AND DATE '2025-12-31' AND f_amt >= 500",
    ),
    (
        "footer bounds of CodeLake files: strings",
        "SELECT count(*), min(m_name), max(m_name) FROM hive.hs.mid WHERE m_name >= 'm19990' AND m_fact > 1000",
    ),
    (
        "stale counters: semi join from the fact",
        "SELECT count(*) FROM hive.hs.fact f WHERE f.f_id IN (SELECT f_id FROM hive.hs.stale_counted)",
    ),
];

/// Files of a third writer (`DataFusion`'s own Parquet writer): `k` stored
/// as INT32 in one file and INT64 in the other for a `bigint` column; `s` has
/// non-ASCII values, which sort after 'z' as bytes.
async fn write_foreign(engine: &Engine, warehouse: &Path) {
    cl(
        engine,
        "CREATE TABLE hive.hs.foreign AS SELECT CAST(0 AS BIGINT) AS k, 'x' AS s WHERE 1 = 0",
    )
    .await;
    let dir = table_dir(warehouse, "foreign");
    let copies = [
        (
            "CAST(value AS INT)",
            "CASE WHEN value % 3 = 0 THEN CONCAT('é', CAST(value AS VARCHAR)) ELSE CONCAT('a', CAST(value AS VARCHAR)) END",
            "0, 99",
            "foreign_a.parquet",
        ),
        (
            "CAST(value AS BIGINT)",
            "CONCAT('b', CAST(value AS VARCHAR))",
            "100, 199",
            "foreign_b.parquet",
        ),
    ];
    for (k, s, range, name) in copies {
        let sql = format!(
            "COPY (SELECT {k} AS k, {s} AS s FROM generate_series({range})) TO '{}' STORED AS PARQUET",
            dir.join(name).display()
        );
        engine
            .context()
            .sql(&sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(data_files(warehouse, "foreign").len(), 2);
}

/// Queries over the foreign files, with the rows computed by hand.
const FOREIGN: &[(&str, &[&[&str]])] = &[
    (
        "SELECT count(*), min(k), max(k) FROM hive.hs.foreign WHERE k > 150",
        &[&["49", "151", "199"]],
    ),
    (
        "SELECT count(*) FROM hive.hs.foreign WHERE k BETWEEN 90 AND 110",
        &[&["21"]],
    ),
    ("SELECT count(*) FROM hive.hs.foreign WHERE k < 0", &[&["0"]]),
    ("SELECT count(*) FROM hive.hs.foreign WHERE s > 'z'", &[&["34"]]),
    (
        "SELECT count(*), sum(f.k) FROM hive.hs.foreign f JOIN hive.hs.dim d ON f.k = d.n_key",
        &[&["25", "300"]],
    ),
    (
        "SELECT count(*) FROM hive.hs.fact x WHERE x.f_id IN (SELECT k FROM hive.hs.foreign WHERE s LIKE 'é%')",
        &[&["34"]],
    ),
];

fn strings(rows: &[&[&str]]) -> Rows {
    rows.iter()
        .map(|r| r.iter().map(|c| (*c).to_owned()).collect())
        .collect()
}

#[tokio::test]
async fn statistics_change_plans_never_rows_even_when_they_lie() {
    let shared = shared().await;
    let warehouse = shared.warehouse();
    let trino = Trino::start(&shared, "rows").await;
    trino.batch(&TRINO_SETUP).await;
    let spill = tempfile::tempdir().unwrap();
    let config = memory(spill.path(), ByteSize::mib(512), ByteSize::mib(8));
    let with = open(&warehouse, &config, true).await;
    let off = open(&warehouse, &config, false).await;
    for sql in CODELAKE_SETUP {
        cl(&with, sql).await;
    }
    assert_eq!(data_files(&warehouse, "mid").len(), 3);

    // Which statistics each table carries before the surgery: Trino's are in
    // the metastore and match the files.
    assert_eq!(estimated_rows(&with, "fact").await, "Rows=Inexact(200000)");
    assert_eq!(estimated_rows(&with, "dim").await, "Rows=Inexact(25)");
    assert_eq!(estimated_rows(&with, "mid").await, "Rows=Inexact(21000)");
    note(
        "empty table, files written by Trino",
        data_files(&warehouse, "empty").len(),
    );

    lie(&warehouse);
    add_foreign_files(&warehouse);
    write_foreign(&with, &warehouse).await;
    // The lies are what the planner sees (the metastore path is taken).
    assert_eq!(estimated_rows(&with, "liar").await, "Rows=Inexact(1)");
    assert_eq!(estimated_rows(&with, "zero").await, "Rows=Inexact(0)");
    assert_eq!(
        estimated_rows(&with, "dim_huge").await,
        "Rows=Inexact(999999999999)"
    );
    assert_eq!(
        estimated_rows(&with, "stale").await,
        "Rows=Inexact(20000)",
        "footers"
    );
    assert_eq!(
        estimated_rows(&with, "stale_counted").await,
        "Rows=Inexact(10000)",
        "counters match, so the stale row count is used"
    );
    assert_eq!(estimated_rows(&off, "fact").await, "Rows=Absent");

    let statements: Vec<&str> = QUERIES.iter().map(|(_, sql)| *sql).collect();
    let references = trino.batch(&statements).await;
    let mut failures = Vec::new();
    for ((label, sql), reference) in QUERIES.iter().zip(references) {
        let reference = sorted(reference);
        let with_rows = sorted(cl(&with, sql).await);
        let off_rows = sorted(cl(&off, sql).await);
        note(label, format!("{reference:?}"));
        if with_rows != reference || off_rows != reference {
            failures.push(format!(
                "{label}: trino={reference:?} with={with_rows:?} without={off_rows:?}\n  {sql}"
            ));
        }
    }
    for (sql, expected) in FOREIGN {
        let expected = strings(expected);
        let with_rows = sorted(cl(&with, sql).await);
        let off_rows = sorted(cl(&off, sql).await);
        let reference = trino.try_sql(sql).await.map(sorted);
        note(sql, format!("expected={expected:?} trino={reference:?}"));
        if with_rows != expected || off_rows != expected {
            failures.push(format!(
                "foreign: expected={expected:?} with={with_rows:?} without={off_rows:?}\n  {sql}"
            ));
        }
    }
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
    assert_eq!(with.memory().reserved_bytes, 0, "pool given back");
}

#[tokio::test]
async fn the_smaller_input_builds_and_a_small_dimension_is_collected_once() {
    let shared = shared().await;
    let warehouse = shared.warehouse();
    let trino = Trino::start(&shared, "plan").await;
    trino.batch(&TRINO_SETUP[..3]).await;
    let spill = tempfile::tempdir().unwrap();
    let config = memory(spill.path(), ByteSize::mib(512), ByteSize::mib(8));
    let with = open(&warehouse, &config, true).await;
    let off = open(&warehouse, &config, false).await;
    for sql in CODELAKE_SETUP {
        cl(&with, sql).await;
    }

    // Trino-written fact on the left, 25-row dimension on the right.
    let sql = "SELECT d.n_name, count(*) FROM hive.hs.fact f JOIN hive.hs.dim d ON f.f_key = d.n_key GROUP BY d.n_name";
    let plan = explain(&with, sql).await;
    let [(join, build)] = hash_joins(&plan).try_into().unwrap();
    assert!(join.contains("mode=CollectLeft"), "{plan}");
    assert!(build.contains("hs/dim/"), "dim must build:\n{plan}");
    let plan = explain(&off, sql).await;
    let [(_, build)] = hash_joins(&plan).try_into().unwrap();
    assert!(
        build.contains("hs/fact/"),
        "without statistics the written order stays:\n{plan}"
    );

    // Dimension on the left stays the collected build side.
    let sql = "SELECT d.n_name, count(*) FROM hive.hs.dim d JOIN hive.hs.fact f ON f.f_key = d.n_key GROUP BY d.n_name";
    let plan = explain(&with, sql).await;
    let [(join, build)] = hash_joins(&plan).try_into().unwrap();
    assert!(
        join.contains("mode=CollectLeft") && build.contains("hs/dim/"),
        "{plan}"
    );

    // Metastore statistics (fact, 200k rows) against footer statistics (mid,
    // 21k rows over 3 files).
    let sql = "SELECT count(*) FROM hive.hs.fact f JOIN hive.hs.mid m ON f.f_id = m.m_fact";
    let plan = explain(&with, sql).await;
    let [(_, build)] = hash_joins(&plan).try_into().unwrap();
    assert!(build.contains("hs/mid/"), "mid must build:\n{plan}");

    // Three tables (the revenue mart shape): the fact never builds.
    let sql = "SELECT d.n_name, count(*), sum(m.m_key) FROM hive.hs.fact f \
               JOIN hive.hs.mid m ON f.f_id = m.m_fact JOIN hive.hs.dim d ON f.f_key = d.n_key GROUP BY d.n_name";
    let plan = explain(&with, sql).await;
    let joins = hash_joins(&plan);
    assert_eq!(joins.len(), 2, "{plan}");
    for (join, build) in &joins {
        assert!(!build.contains("hs/fact/"), "fact builds in `{join}`:\n{plan}");
    }
    assert!(
        joins
            .iter()
            .any(|(j, b)| j.contains("mode=CollectLeft") && b.contains("hs/dim/") && !b.contains("hs/mid/")),
        "the dimension is collected once:\n{plan}"
    );
    let plan = explain(&off, sql).await;
    assert!(
        hash_joins(&plan).iter().any(|(_, b)| b.contains("hs/fact/")),
        "baseline builds on the fact:\n{plan}"
    );

    // Q18 shape: the fact is the probe side of the semi join.
    let sql = "SELECT f.f_key, count(*) FROM hive.hs.fact f WHERE f.f_id IN \
               (SELECT m_fact FROM hive.hs.mid GROUP BY m_fact HAVING count(*) >= 1) GROUP BY f.f_key";
    let plan = explain(&with, sql).await;
    for (join, build) in hash_joins(&plan) {
        assert!(!build.contains("hs/fact/"), "fact builds in `{join}`:\n{plan}");
    }
    drop(trino);
}

// ---------------------------------------------------------------------------
// Cost: many files, the footer cache limit and a pool that refuses.
// ---------------------------------------------------------------------------

const MANY_FILES: usize = 3000;

async fn timed_explain(engine: &Engine, sql: &str) -> (Duration, String) {
    let start = Instant::now();
    let plan = explain(engine, sql).await;
    (start.elapsed(), plan)
}

#[tokio::test]
async fn planning_over_thousands_of_files_is_bounded_and_the_cache_respects_its_limit() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let spill = tempfile::tempdir().unwrap();
    let cache_limit = ByteSize::b(64 * 1024);
    let config = memory(spill.path(), ByteSize::mib(256), cache_limit);
    let writer = open(&warehouse, &config, true).await;
    cl(&writer, "CREATE SCHEMA hive.hs").await;
    cl(
        &writer,
        "CREATE TABLE hive.hs.many AS SELECT CAST(value AS BIGINT) AS k, CAST(value % 25 AS BIGINT) AS g, \
         CONCAT('v', CAST(value AS VARCHAR)) AS s FROM generate_series(0, 9)",
    )
    .await;
    cl(
        &writer,
        "CREATE TABLE hive.hs.small AS SELECT CAST(value AS BIGINT) AS g FROM generate_series(0, 24)",
    )
    .await;
    let [file] = data_files(&warehouse, "many").try_into().unwrap();
    for i in 1..MANY_FILES {
        std::fs::copy(&file, table_dir(&warehouse, "many").join(format!("copy_{i:05}"))).unwrap();
    }
    drop(writer);

    let with = open(&warehouse, &config, true).await;
    let off = open(&warehouse, &config, false).await;
    let sql = "SELECT s.g, count(*) FROM hive.hs.small s JOIN hive.hs.many m ON m.g = s.g GROUP BY s.g";
    let (without, _) = timed_explain(&off, sql).await;
    let (cold, _) = timed_explain(&with, sql).await;
    let (warm, plan) = timed_explain(&with, sql).await;
    note(
        "planning over 3000 files",
        format!("without statistics {without:?}, with (cold cache) {cold:?}, with (warm) {warm:?}"),
    );
    assert!(cold < Duration::from_secs(10), "cold planning took {cold:?}");
    assert!(
        cold < without + Duration::from_secs(3),
        "statistics add {:?} to planning",
        cold.saturating_sub(without)
    );
    // Same content in every file: the sample extrapolates exactly.
    assert_eq!(estimated_rows(&with, "many").await, "Rows=Inexact(30000)");
    let [(_, build)] = hash_joins(&plan).try_into().unwrap();
    assert!(build.contains("hs/small/"), "{plan}");

    let cache = with
        .context()
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache();
    let cached: usize = cache.list_entries().values().map(|e| e.size_bytes).sum();
    note(
        "footer cache",
        format!(
            "{} entries, {cached} bytes, limit {}",
            cache.list_entries().len(),
            cache.cache_limit()
        ),
    );
    assert_eq!(cache.cache_limit(), cache_limit.as_usize());
    assert!(
        cached <= cache.cache_limit(),
        "{cached} bytes cached over the limit"
    );
    assert_eq!(with.memory().reserved_bytes, 0, "footer reservation given back");

    assert_eq!(
        cl(&with, "SELECT count(*), count(DISTINCT k) FROM hive.hs.many").await,
        strings(&[&["30000", "10"]])
    );
}

#[tokio::test]
async fn a_pool_that_refuses_the_footer_read_only_loses_the_statistics() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let spill = tempfile::tempdir().unwrap();
    let roomy = memory(spill.path(), ByteSize::mib(256), ByteSize::mib(8));
    let writer = open(&warehouse, &roomy, true).await;
    cl(&writer, "CREATE SCHEMA hive.hs").await;
    // About 1.5 MiB of incompressible text: the footer prefetch is larger than
    // the pool below.
    cl(
        &writer,
        "CREATE TABLE hive.hs.big AS SELECT CAST(value AS BIGINT) AS k, CAST(value % 25 AS BIGINT) AS g, \
         md5(CAST(value AS VARCHAR)) AS h FROM generate_series(0, 49999)",
    )
    .await;
    cl(
        &writer,
        "CREATE TABLE hive.hs.small AS SELECT CAST(value AS BIGINT) AS g FROM generate_series(0, 24)",
    )
    .await;
    // CodeLake's CTAS records no row count, so the scan goes to the footers.
    assert_eq!(estimated_rows(&writer, "big").await, "Rows=Inexact(50000)");
    let size = data_files(&warehouse, "big")[0].metadata().unwrap().len();
    note("big file size", size);
    assert!(size > 512 * 1024, "{size}");
    drop(writer);

    let tight = memory(spill.path(), ByteSize::b(256 * 1024), ByteSize::b(64 * 1024));
    let engine = open(&warehouse, &tight, true).await;
    let plan = explain(
        &engine,
        "SELECT count(*) FROM hive.hs.big b JOIN hive.hs.small s ON b.g = s.g",
    )
    .await;
    assert!(plan.contains("HashJoinExec"), "{plan}");
    assert_eq!(estimated_rows(&engine, "big").await, "Rows=Absent");
    assert_eq!(engine.memory().reserved_bytes, 0);
}

// ---------------------------------------------------------------------------
// Defects: statistics that changed the result. Fast reproductions without
// Trino; the expected rows are what the files hold (the rows suite above shows
// Trino agrees on the same shapes).
// ---------------------------------------------------------------------------

/// A warehouse with `hs.fact` (`f_id` 0..199999), written by `CodeLake`.
async fn small_warehouse() -> (tempfile::TempDir, tempfile::TempDir, PathBuf, Engine) {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    let spill = tempfile::tempdir().unwrap();
    let config = memory(spill.path(), ByteSize::mib(256), ByteSize::mib(8));
    let engine = open(&warehouse, &config, true).await;
    cl(&engine, "CREATE SCHEMA hive.hs").await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.fact AS SELECT CAST(value AS BIGINT) AS f_id FROM generate_series(0, 199999)",
    )
    .await;
    (dir, spill, warehouse, engine)
}

/// `(sql, expected)` pairs that differ, as `sql: expected=.. obtained=..`.
async fn wrong_rows(engine: &Engine, checks: &[(&str, &[&[&str]])]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (sql, expected) in checks {
        let obtained = cl(engine, sql).await;
        if obtained != strings(expected) {
            wrong.push(format!("{sql}: expected={expected:?} obtained={obtained:?}"));
        }
    }
    wrong
}

/// Hive's `LOAD DATA` (and other writers) record `numRows=0` while
/// `numFiles`/`totalSize` describe the files: the counters match, so the
/// metastore row count is used, and `DataFusion` turns an estimate of 0 rows
/// into an exact 0 (`Statistics::with_fetch` treats an inexact count as an
/// upper bound), which `count(*)` is then answered from.
#[tokio::test]
#[ignore = "DEFECT-HS1: numRows=0 in the metastore with matching numFiles/totalSize: \
            count(*) of fact JOIN t expected 50000, obtained 0"]
async fn a_recorded_row_count_of_zero_does_not_empty_the_table() {
    let (_dir, _spill, warehouse, engine) = small_warehouse().await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.t AS SELECT CAST(value AS BIGINT) AS f_id FROM generate_series(0, 49999)",
    )
    .await;
    recount(&warehouse, "t");
    tamper(&warehouse, "t", |m| {
        m["parameters"]["numRows"] = json!("0");
    });
    assert_eq!(estimated_rows(&engine, "t").await, "Rows=Inexact(0)");
    let wrong = wrong_rows(
        &engine,
        &[
            ("SELECT count(*) FROM hive.hs.t", &[&["50000"]]),
            (
                "SELECT count(*) FROM hive.hs.fact f JOIN hive.hs.t t ON f.f_id = t.f_id",
                &[&["50000"]],
            ),
            (
                "SELECT count(*) FROM hive.hs.fact WHERE f_id IN (SELECT f_id FROM hive.hs.t)",
                &[&["50000"]],
            ),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A writer appends a file and refreshes only the basic statistics (as
/// `ANALYZE TABLE .. COMPUTE STATISTICS` without `FOR COLUMNS` does): the
/// counters match, the column bounds still describe the old rows. The join
/// estimate calls the inputs disjoint and `count(*)` is answered with 0.
#[tokio::test]
#[ignore = "DEFECT-HS2: stale metastore column bounds with refreshed counters: count(*) of t JOIN late \
            and of late WHERE f_id IN (SELECT f_id FROM t) expected 10000, obtained 0"]
async fn stale_column_bounds_do_not_empty_a_join() {
    let (_dir, _spill, warehouse, engine) = small_warehouse().await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.t AS SELECT CAST(value AS BIGINT) AS f_id FROM generate_series(0, 9999)",
    )
    .await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.late AS SELECT CAST(value AS BIGINT) AS f_id FROM generate_series(40000, 49999)",
    )
    .await;
    // Column statistics of the first 10 000 rows, as a writer recorded them.
    tamper(&warehouse, "t", |m| {
        m["columnStatistics"]["f_id"] =
            json!({"integerStatistics": {"min": 0, "max": 9999}, "nullsCount": 0});
    });
    // The other writer's file, then the basic statistics refreshed.
    let [late] = data_files(&warehouse, "late").try_into().unwrap();
    std::fs::copy(&late, table_dir(&warehouse, "t").join("other_writer_0")).unwrap();
    recount(&warehouse, "t");
    tamper(&warehouse, "t", |m| {
        m["parameters"]["numRows"] = json!("20000");
    });
    assert_eq!(estimated_rows(&engine, "t").await, "Rows=Inexact(20000)");
    let wrong = wrong_rows(
        &engine,
        &[
            (
                "SELECT count(*) FROM hive.hs.t t JOIN hive.hs.late l ON t.f_id = l.f_id",
                &[&["10000"]],
            ),
            (
                "SELECT count(*) FROM hive.hs.late WHERE f_id IN (SELECT f_id FROM hive.hs.t)",
                &[&["10000"]],
            ),
            (
                "SELECT count(*) FROM hive.hs.t WHERE f_id >= 40000",
                &[&["10000"]],
            ),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Positions the footer sample of a listing of `files` does not read (an even
/// spread of 256 files).
fn unsampled(files: usize) -> Vec<usize> {
    let sampled: std::collections::BTreeSet<usize> = (0..256).map(|i| i * files / 256).collect();
    (0..files).filter(|i| !sampled.contains(i)).collect()
}

/// Writes `hs.<table>` as `files` Parquet files (`part-00000`, ...): the
/// `rows_at` positions hold a copy of `rich`, every other one a copy of `poor`.
fn lay_out(warehouse: &Path, table: &str, files: usize, rich: &Path, poor: &Path, rows_at: &[usize]) {
    let dir = table_dir(warehouse, table);
    for old in data_files(warehouse, table) {
        std::fs::remove_file(old).unwrap();
    }
    for i in 0..files {
        let source = if rows_at.contains(&i) { rich } else { poor };
        std::fs::copy(source, dir.join(format!("part-{i:05}"))).unwrap();
    }
}

/// A Parquet file with `table`'s schema and no rows (Spark writes these for
/// empty tasks).
async fn empty_parquet(engine: &Engine, path: &Path) {
    let schema = engine
        .context()
        .table_provider("hive.hs.t")
        .await
        .unwrap()
        .schema();
    let file = std::fs::File::create(path).unwrap();
    let writer = datafusion::parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.close().unwrap();
}

/// More than 256 files: the footers of an even sample are read and the rest
/// extrapolated by size. When the sample holds only empty files (a Spark job
/// with many empty tasks) the estimate is 0 rows, which `DataFusion` makes
/// exact, and `count(*)` returns 0.
#[tokio::test]
#[ignore = "DEFECT-HS3: >256 files whose footer sample is all empty files (estimate Rows=Inexact(0)): \
            count(*) of fact JOIN t expected 10000, obtained 0"]
async fn an_empty_footer_sample_does_not_empty_the_table() {
    let (dir, _spill, warehouse, engine) = small_warehouse().await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.t AS SELECT CAST(value AS BIGINT) AS f_id FROM generate_series(0, 999)",
    )
    .await;
    let [rich] = data_files(&warehouse, "t").try_into().unwrap();
    let rich_copy = dir.path().join("rich");
    std::fs::copy(&rich, &rich_copy).unwrap();
    let poor = dir.path().join("empty");
    empty_parquet(&engine, &poor).await;
    let rows_at: Vec<usize> = unsampled(3000).into_iter().step_by(100).take(10).collect();
    assert_eq!(rows_at.len(), 10);
    lay_out(&warehouse, "t", 3000, &rich_copy, &poor, &rows_at);
    note(
        "estimate over an empty sample",
        estimated_rows(&engine, "t").await,
    );
    let wrong = wrong_rows(
        &engine,
        &[
            ("SELECT count(*) FROM hive.hs.t", &[&["10000"]]),
            (
                "SELECT count(*) FROM hive.hs.fact f JOIN hive.hs.t t ON f.f_id = t.f_id",
                &[&["10000"]],
            ),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The extrapolation can also fall short without reaching 0: the sampled
/// files hold 1 row each, the others 1000 rows in a few bytes. An `OFFSET`
/// larger than the estimate is then taken as skipping every row
/// (`Statistics::with_fetch`), and `count(*)` returns 0.
#[tokio::test]
#[ignore = "DEFECT-HS4: footer extrapolation underestimates (Rows=Inexact(3066) for 2744256 rows); \
            count(*) over OFFSET 100000 expected 2644256, obtained 0"]
async fn an_underestimate_does_not_empty_an_offset() {
    let (dir, _spill, warehouse, engine) = small_warehouse().await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.t AS SELECT CAST(1 AS BIGINT) AS f_id FROM generate_series(1, 1000)",
    )
    .await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.one AS SELECT CAST(7 AS BIGINT) AS f_id",
    )
    .await;
    let [rich] = data_files(&warehouse, "t").try_into().unwrap();
    let [poor] = data_files(&warehouse, "one").try_into().unwrap();
    let rich_copy = dir.path().join("rich");
    let poor_copy = dir.path().join("poor");
    std::fs::copy(&rich, &rich_copy).unwrap();
    std::fs::copy(&poor, &poor_copy).unwrap();
    let rows_at = unsampled(3000);
    assert_eq!(rows_at.len(), 2744);
    lay_out(&warehouse, "t", 3000, &rich_copy, &poor_copy, &rows_at);
    note(
        "estimate over a sparse sample",
        estimated_rows(&engine, "t").await,
    );
    let wrong = wrong_rows(
        &engine,
        &[
            ("SELECT count(*) FROM hive.hs.t", &[&["2744256"]]),
            (
                "SELECT count(*) FROM (SELECT f_id FROM hive.hs.t OFFSET 100000) s",
                &[&["2644256"]],
            ),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The same `OFFSET` over a metastore row count that is too low (garbage in,
/// but the rule is the same: an inexact count must not skip rows).
#[tokio::test]
#[ignore = "DEFECT-HS4: numRows=1 recorded for 50000 rows: count(*) over OFFSET 5 expected 49995, obtained 0"]
async fn a_low_recorded_row_count_does_not_empty_an_offset() {
    let (_dir, _spill, warehouse, engine) = small_warehouse().await;
    cl(
        &engine,
        "CREATE TABLE hive.hs.t AS SELECT CAST(value AS BIGINT) AS f_id FROM generate_series(0, 49999)",
    )
    .await;
    recount(&warehouse, "t");
    tamper(&warehouse, "t", |m| {
        m["parameters"]["numRows"] = json!("1");
    });
    let wrong = wrong_rows(
        &engine,
        &[(
            "SELECT count(*) FROM (SELECT f_id FROM hive.hs.t OFFSET 5) s",
            &[&["49995"]],
        )],
    )
    .await;
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
