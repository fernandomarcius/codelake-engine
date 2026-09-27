//! Red-team probes of the hive catalog on `integ/wave1`, after the F2
//! corrections and the F9 (`OPTIMIZE`) integration. Each probe asserts the
//! behavior Trino 431 has, or that data safety requires; a probe marked
//! `DEFECT-W1-<n>` fails on this branch and documents a finding.
//!
//! Evidence (what each engine answered) is appended to
//! `target/w1rt-evidence/<probe>.log`, since the lints forbid printing.
//!
//! Also here, as the F2 correction asked: the two red-team probes left on
//! `probe/f2-redteam` with a wrong setup (`rt_wide_insert_fits_a_small_pool`
//! used `concat(…, repeat(…))`, an array in Trino 431;
//! `rt_drop_during_select_never_returns_a_partial_count` expected the sum
//! 500000500000 of rows whose sum is 16 × 31250125000 = 500002000000).

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::config::{ExecutionConfig, HiveTimestampPrecision, MemoryConfig};
use codelake_core::parquet_time_zone::ParquetTimeZone;
use codelake_core::{ByteSize, Engine, QueryOptions};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, TimestampMillisecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use testcontainers::bollard::Docker;
use testcontainers::bollard::models::MountPointTypeEnum;
use testcontainers::core::{ExecCommand, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

type Rows = Vec<Vec<String>>;

// ---------------------------------------------------------------------------
// CodeLake side
// ---------------------------------------------------------------------------

fn params(warehouse: &Path) -> HiveParams {
    HiveParams {
        warehouse: warehouse.display().to_string(),
        s3_endpoint: None,
        s3_region: "us-east-1".into(),
        credentials: None,
        path_style: false,
    }
}

/// A fresh engine (a fresh "process": own locks) over `warehouse`, its
/// catalog in the default Parquet time zone.
async fn engine_at(
    warehouse: &Path,
    spill: &Path,
    limit: ByteSize,
    precision: HiveTimestampPrecision,
) -> Engine {
    engine_in(warehouse, spill, limit, precision, ParquetTimeZone::default()).await
}

/// [`engine_at`] with the catalog's Parquet time zone `zone`.
async fn engine_in(
    warehouse: &Path,
    spill: &Path,
    limit: ByteSize,
    precision: HiveTimestampPrecision,
    zone: ParquetTimeZone,
) -> Engine {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(1),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    let connector = HiveConnector::new("hive".into(), params(warehouse))
        .with_timestamp_precision(precision)
        .with_parquet_time_zone(zone);
    engine
        .attach(Arc::new(connector), true, Duration::from_secs(60))
        .await
        .unwrap();
    engine
}

async fn engine(warehouse: &Path, spill: &Path) -> Engine {
    engine_at(
        warehouse,
        spill,
        ByteSize::mib(256),
        HiveTimestampPrecision::Milliseconds,
    )
    .await
}

struct Local {
    _dir: tempfile::TempDir,
    spill: tempfile::TempDir,
    warehouse: PathBuf,
    engine: Engine,
}

impl Local {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let spill = tempfile::tempdir().unwrap();
        let warehouse = dir.path().join("warehouse");
        std::fs::create_dir_all(&warehouse).unwrap();
        let engine = engine(&warehouse, spill.path()).await;
        Self {
            _dir: dir,
            spill,
            warehouse,
            engine,
        }
    }

    /// Another engine on the same warehouse (another `CodeLake` node).
    async fn other(&self) -> Engine {
        engine(&self.warehouse, self.spill.path()).await
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

async fn run_with(engine: &Engine, options: &QueryOptions, sql: &str) -> Result<Rows, String> {
    let frame = engine.sql_with(options, sql).await.map_err(|e| e.to_string())?;
    let batches = frame.collect().await.map_err(|e| e.to_string())?;
    Ok(to_rows(&batches))
}

async fn cl_try(engine: &Engine, sql: &str) -> Result<Rows, String> {
    run_with(engine, &QueryOptions::for_user("w1rt"), sql).await
}

async fn cl(engine: &Engine, sql: &str) -> Rows {
    cl_try(engine, sql)
        .await
        .unwrap_or_else(|e| panic!("CodeLake `{sql}` failed: {e}"))
}

/// `ALTER TABLE … EXECUTE optimize` with the session property Trino requires.
async fn optimize(engine: &Engine, table: &str) -> Result<Rows, String> {
    let mut options = QueryOptions::for_user("w1rt");
    options.properties = engine
        .session_properties()
        .session(
            [(
                "hive.non_transactional_optimize_enabled".to_owned(),
                "true".to_owned(),
            )]
            .into_iter()
            .collect(),
        )
        .unwrap();
    run_with(engine, &options, &format!("ALTER TABLE {table} EXECUTE optimize")).await
}

fn rows(values: &[&[&str]]) -> Rows {
    values
        .iter()
        .map(|r| r.iter().map(|v| (*v).to_owned()).collect())
        .collect()
}

/// Appends evidence to `target/w1rt-evidence/<probe>.log`.
fn note(probe: &str, what: &str, detail: impl std::fmt::Debug) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/w1rt-evidence");
    let _ = std::fs::create_dir_all(&dir);
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(format!("{probe}.log")))
    {
        let _ = writeln!(file, "[W1RT] {what}: {detail:?}");
    }
}

/// Visible data files of a table directory.
fn data_files(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .map(Result::unwrap)
        .filter(|e| e.file_type().unwrap().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| !n.starts_with(['.', '_']))
        .collect();
    names.sort();
    names
}

/// Every path under `dir`, relative, sorted.
fn tree(dir: &Path) -> Vec<String> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.map(Result::unwrap) {
            let path = entry.path();
            out.push(path.strip_prefix(base).unwrap().display().to_string());
            if path.is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// Writes `batch` as a Parquet file at `path`, as Spark or pandas would.
fn put_parquet(path: &Path, batch: &RecordBatch) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// `count(*)` and `sum(v)` of a table.
async fn count_sum(engine: &Engine, table: &str) -> Result<Rows, String> {
    cl_try(
        engine,
        &format!("SELECT count(*), CAST(sum(v) AS varchar) FROM {table}"),
    )
    .await
}

/// A table with `files` files of `per_file` rows, `v` = `1..=files * per_file`.
async fn seeded(engine: &Engine, table: &str, files: i64, per_file: i64) {
    cl(
        engine,
        &format!(
            "CREATE TABLE {table} AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, {per_file})"
        ),
    )
    .await;
    for f in 1..files {
        let (from, to) = (f * per_file + 1, (f + 1) * per_file);
        cl(
            engine,
            &format!("INSERT INTO {table} SELECT CAST(value AS BIGINT) FROM generate_series({from}, {to})"),
        )
        .await;
    }
}

fn triangle(n: i64) -> String {
    (i128::from(n) * i128::from(n + 1) / 2).to_string()
}

// ---------------------------------------------------------------------------
// OPTIMIZE (F9) against the F2 writers
// ---------------------------------------------------------------------------

/// Concurrent `INSERT`s (from another node) while `OPTIMIZE` compacts: every
/// row exactly once, no temporary left in the table directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_optimize_racing_inserts_keeps_every_row_once() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    seeded(&l.engine, "hive.s.t", 12, 5_000).await;
    let other = l.other().await;
    let mut expected_rows = 12 * 5_000;
    let mut expected_sum = i128::from(12 * 5_000) * i128::from(12 * 5_000 + 1) / 2;
    for round in 0..4_i64 {
        let inserts = async {
            let mut done = Vec::new();
            for i in 0..6_i64 {
                let v = 1_000_000 + round * 100 + i;
                done.push(cl_try(&other, &format!("INSERT INTO hive.s.t VALUES ({v})")).await);
            }
            done
        };
        let (compacted, inserted) = tokio::join!(optimize(&l.engine, "hive.s.t"), inserts);
        note(
            "w1_optimize_racing_inserts",
            &format!("round {round}"),
            (&compacted, &inserted),
        );
        for (i, result) in inserted.iter().enumerate() {
            if result.is_ok() {
                expected_rows += 1;
                expected_sum += i128::from(1_000_000 + round * 100 + i64::try_from(i).unwrap());
            }
        }
    }
    let got = count_sum(&l.engine, "hive.s.t").await;
    let dir = l.warehouse.join("s/t");
    let hidden: Vec<String> = tree(&dir)
        .into_iter()
        .filter(|n| n.ends_with(".inprogress") || n.contains('#') || n.contains("optimize"))
        .collect();
    assert_eq!(
        got,
        Ok(vec![vec![expected_rows.to_string(), expected_sum.to_string()]]),
        "files: {:?}",
        data_files(&dir)
    );
    assert!(hidden.is_empty(), "leftovers: {hidden:?}");
}

/// `CREATE OR REPLACE TABLE … AS SELECT … FROM` the same table, racing an
/// `OPTIMIZE` of it on another node. The hive catalog replaces tables since
/// ISS-017 (Trino 431 refuses): each round the table must be the old one or
/// the replacement, whole (never a mix, never empty), whatever each
/// statement answered, and nothing of either statement stays behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_or_replace_racing_optimize_is_old_or_new_never_mixed() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let other = l.other().await;
    let mut bad = Vec::new();
    let mut replaced_rounds = 0;
    for round in 0..5 {
        let table = format!("hive.s.r{round}");
        seeded(&l.engine, &table, 10, 4_000).await;
        let replace_sql = format!("CREATE OR REPLACE TABLE {table} AS SELECT v * 10 AS v FROM {table}");
        let replace = cl_try(&other, &replace_sql);
        let (compacted, replaced) = tokio::join!(optimize(&l.engine, &table), replace);
        let got = count_sum(&l.engine, &table).await;
        note(
            "w1_or_replace_racing_optimize",
            &format!("round {round}"),
            (&compacted, &replaced, &got),
        );
        let old = vec![vec!["40000".to_owned(), triangle(40_000)]];
        let new = vec![vec![
            "40000".to_owned(),
            (10 * i128::from(40_000_i64) * 40_001 / 2).to_string(),
        ]];
        replaced_rounds += usize::from(replaced.is_ok());
        let whole = match &replaced {
            Ok(_) => got.as_ref() == Ok(&new),
            Err(_) => got.as_ref() == Ok(&old),
        };
        let hidden: Vec<String> = tree(&l.warehouse.join(format!("s/r{round}")))
            .into_iter()
            .filter(|p| p.starts_with('.') && !p.starts_with(".trino") && !p.starts_with("..trino"))
            .collect();
        if !whole || !hidden.is_empty() {
            bad.push((round, compacted, replaced, got, hidden));
        }
    }
    let internal = tree(&l.warehouse.join(".codelake"))
        .into_iter()
        .filter(|p| p.starts_with("staging/") || p.starts_with("replace/"))
        .collect::<Vec<_>>();
    assert!(bad.is_empty(), "{bad:#?}");
    assert!(replaced_rounds > 0, "no replacement succeeded");
    assert!(internal.is_empty(), "left in .codelake: {internal:?}");
}

/// `DROP TABLE` and a new `CREATE TABLE` of the same name while `OPTIMIZE`
/// of the old table runs: the new table holds exactly its own rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_drop_and_recreate_during_optimize_keeps_only_the_new_table() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let other = l.other().await;
    let mut bad = Vec::new();
    for (round, delay) in [0_u64, 5, 20, 60].into_iter().enumerate() {
        let table = format!("hive.s.d{round}");
        seeded(&l.engine, &table, 10, 4_000).await;
        let recreate = async {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            let dropped = cl_try(&other, &format!("DROP TABLE {table}")).await;
            let created = cl_try(
                &other,
                &format!("CREATE TABLE {table} AS SELECT CAST(7 AS BIGINT) AS v"),
            )
            .await;
            (dropped, created)
        };
        let (compacted, recreated) = tokio::join!(optimize(&l.engine, &table), recreate);
        let got = count_sum(&l.engine, &table).await;
        note(
            "w1_drop_and_recreate_during_optimize",
            &format!("round {round}"),
            (&compacted, &recreated, &got),
        );
        if recreated.1.is_ok() && got != Ok(rows(&[&["1", "7"]])) {
            bad.push((delay, compacted, recreated, got));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

/// Trino 431's file metastore renames tables; `CodeLake` must either do the
/// same or refuse, never answer OK and leave the table where it was.
#[tokio::test]
async fn w1_rename_table_is_done_or_refused() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.a AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    let renamed = cl_try(&l.engine, "ALTER TABLE hive.s.a RENAME TO hive.s.b").await;
    let a = cl_try(&l.engine, "SELECT v FROM hive.s.a").await;
    let b = cl_try(&l.engine, "SELECT v FROM hive.s.b").await;
    note("w1_rename_table", "rename, then a, b", (&renamed, &a, &b));
    if renamed.is_ok() {
        assert!(a.is_err(), "old name still answers after RENAME: {a:?}");
        assert_eq!(b, Ok(rows(&[&["1"]])));
    } else {
        assert_eq!(a, Ok(rows(&[&["1"]])));
    }
}

// ---------------------------------------------------------------------------
// CREATE OR REPLACE (new in the F2 correction)
// ---------------------------------------------------------------------------

/// `CREATE TABLE` refuses a target directory that exists without a table
/// (files of another writer, as Trino does). `CREATE OR REPLACE` of a name
/// with no table is a `CREATE`: it must refuse too, not delete those files
/// (still true now that `OR REPLACE` replaces tables, ISS-017).
#[tokio::test]
async fn w1_or_replace_never_deletes_a_foreign_directory() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let foreign = l.warehouse.join("s/t/part-00000.parquet");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
    )
    .unwrap();
    put_parquet(&foreign, &batch);
    let created = cl_try(
        &l.engine,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    note("w1_or_replace_foreign_dir", "CREATE over foreign dir", &created);
    assert!(created.is_err(), "CREATE must refuse the foreign directory");
    let replaced = cl_try(
        &l.engine,
        "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    note(
        "w1_or_replace_foreign_dir",
        "OR REPLACE over foreign dir, then disk",
        (&replaced, tree(&l.warehouse.join("s"))),
    );
    assert!(
        foreign.exists(),
        "OR REPLACE ({replaced:?}) deleted {}",
        foreign.display()
    );
    assert!(
        replaced.as_ref().is_err_and(|e| e.contains("already exists")),
        "OR REPLACE over a foreign directory: {replaced:?}"
    );
}

// ---------------------------------------------------------------------------
// Types and precision (new in the F2 correction)
// ---------------------------------------------------------------------------

/// A NANOSECONDS catalog (Trino's `hive.timestamp-precision`) over `INT96`
/// timestamps outside 1677–2262, which Trino writes and reads: `CodeLake`
/// must read the value or fail, never answer another instant.
#[tokio::test]
async fn w1_nanosecond_catalog_never_reads_a_wrong_instant() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    // Written by the MILLISECONDS catalog as INT96, Trino's layout.
    cl(
        &l.engine,
        "CREATE TABLE hive.s.far AS SELECT id, ts FROM (VALUES \
         (CAST(1 AS BIGINT), TIMESTAMP '1500-06-01 00:00:00.000'), (2, TIMESTAMP '9999-12-31 23:59:59.999'), \
         (3, TIMESTAMP '2026-01-01 10:00:00.123')) v(id, ts)",
    )
    .await;
    let nanos = engine_at(
        &l.warehouse,
        l.spill.path(),
        ByteSize::mib(256),
        HiveTimestampPrecision::Nanoseconds,
    )
    .await;
    let q = "SELECT id, CAST(ts AS varchar) FROM hive.s.far ORDER BY id";
    let got = cl_try(&nanos, q).await;
    note("w1_nanosecond_catalog", "NANOSECONDS read", &got);
    // 1500-06-01 and 9999-12-31 are outside what timestamp(9) holds: the
    // read fails naming the value, never answers another instant.
    let err = got.expect_err("instants outside timestamp(9) must not read");
    assert!(
        err.contains("is outside the range of timestamp(9)") && err.contains("in column ts"),
        "{err}"
    );
}

/// A NANOSECONDS catalog reads a table two ways and pairs the reads row by
/// row (`exact_timestamps.rs`): over many files of unequal sizes, each row
/// must keep its own timestamp, whichever partition reads its file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_nanosecond_catalog_pairs_rows_of_many_files() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.many AS SELECT CAST(0 AS BIGINT) AS id, TIMESTAMP '2026-01-01 00:00:00.000' AS ts",
    )
    .await;
    let mut next = 1;
    for file in 1..=12_i64 {
        let rows = file * file * 500;
        cl(
            &l.engine,
            &format!(
                "INSERT INTO hive.s.many SELECT CAST(value AS BIGINT), \
                 date_add('millisecond', CAST(value AS BIGINT), TIMESTAMP '2026-01-01 00:00:00.000') \
                 FROM generate_series({next}, {}) ",
                next + rows - 1
            ),
        )
        .await;
        next += rows;
    }
    let nanos = engine_at(
        &l.warehouse,
        l.spill.path(),
        ByteSize::mib(256),
        HiveTimestampPrecision::Nanoseconds,
    )
    .await;
    let q = "SELECT count(*), \
             count(*) FILTER (WHERE ts <> date_add('millisecond', id, TIMESTAMP '2026-01-01 00:00:00.000')) \
             FROM hive.s.many";
    let millis = cl_try(&l.engine, q).await;
    let got = cl_try(&nanos, q).await;
    note(
        "w1_nanosecond_many_files",
        "MILLISECONDS / NANOSECONDS",
        (&millis, &got),
    );
    let expected = Ok(rows(&[&[&next.to_string(), "0"]]));
    assert_eq!(millis, expected);
    assert_eq!(got, expected);
}

/// `OPTIMIZE` in a NANOSECONDS catalog must not rewrite those rows as other
/// instants: the MILLISECONDS catalog reads what it wrote.
#[tokio::test]
async fn w1_nanosecond_optimize_never_rewrites_other_instants() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.far AS SELECT CAST(1 AS BIGINT) AS id, TIMESTAMP '1500-06-01 00:00:00.000' AS ts",
    )
    .await;
    cl(
        &l.engine,
        "INSERT INTO hive.s.far VALUES (2, TIMESTAMP '2026-01-01 10:00:00.123')",
    )
    .await;
    let q = "SELECT id, CAST(ts AS varchar) FROM hive.s.far ORDER BY id";
    let before = cl(&l.engine, q).await;
    let nanos = engine_at(
        &l.warehouse,
        l.spill.path(),
        ByteSize::mib(256),
        HiveTimestampPrecision::Nanoseconds,
    )
    .await;
    let compacted = optimize(&nanos, "hive.s.far").await;
    let after = cl_try(&l.engine, q).await;
    note(
        "w1_nanosecond_optimize",
        "before, OPTIMIZE, after",
        (&before, &compacted, &after),
    );
    assert_eq!(after, Ok(before));
}

/// Trino compares `char(n)` padded: for a `char(5)` holding `'ab'`,
/// `ch = 'ab'` is true. The F2 correction refuses char comparisons pushed to
/// the scan; a comparison the optimizer keeps above the scan (volatile
/// predicate, `LIMIT` or window below it) must not answer differently.
#[tokio::test]
async fn w1_char_comparisons_above_the_scan_never_drop_rows() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(&l.engine, "CREATE TABLE hive.s.ch (id bigint, ch char(5))").await;
    cl(&l.engine, "INSERT INTO hive.s.ch VALUES (1, 'ab'), (2, 'zz')").await;
    let queries = [
        "SELECT id FROM hive.s.ch WHERE ch = 'ab' OR random() > 2",
        "SELECT id FROM (SELECT id, ch FROM hive.s.ch ORDER BY id LIMIT 10) x WHERE ch = 'ab'",
        "SELECT id FROM (SELECT id, ch, row_number() OVER (ORDER BY id) AS n FROM hive.s.ch) x WHERE ch = 'ab'",
    ];
    let mut wrong = Vec::new();
    for q in queries {
        let got = cl_try(&l.engine, q).await;
        note("w1_char_comparisons", q, &got);
        // Trino 431 answers [[1]] to each; refusing is acceptable.
        let right = match &got {
            Ok(r) => *r == rows(&[&["1"]]),
            Err(e) => e.contains("comparing the char column ch is not supported yet"),
        };
        if !right {
            wrong.push((q, got));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

// ---------------------------------------------------------------------------
// Memory and file access from the core DDL routes
// ---------------------------------------------------------------------------

/// Trino has no writable in-memory catalog. `CREATE TABLE` in `DataFusion`'s
/// default catalog keeps every row in server memory after the statement,
/// outside the pool: with a 48 MiB pool it must not hold ~120 MB.
#[tokio::test]
async fn w1_in_memory_tables_never_escape_the_pool() {
    let dir = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_at(
        dir.path(),
        spill.path(),
        ByteSize::mib(48),
        HiveTimestampPrecision::Milliseconds,
    )
    .await;
    let created = cl_try(
        &engine,
        "CREATE TABLE datafusion.public.big AS SELECT CAST(value AS BIGINT) AS id, \
         array_join(repeat('x', 1000), '') AS s FROM generate_series(1, 120000)",
    )
    .await;
    let held = cl_try(
        &engine,
        "SELECT count(*), sum(length(s)) FROM datafusion.public.big",
    )
    .await;
    let other_user = run_with(
        &engine,
        &QueryOptions::for_user("mallory"),
        "SELECT count(*) FROM datafusion.public.big",
    )
    .await;
    note(
        "w1_in_memory_tables",
        "create, read, other user",
        (&created, &held, &other_user),
    );
    assert!(
        created.as_ref().is_err_and(|e| e.contains("Resources exhausted")),
        "120000 rows of ~1 KB live in a 48 MiB engine: {created:?} {held:?}; visible to another user: \
         {other_user:?}"
    );
    assert!(held.is_err(), "a failed CREATE registered its table: {held:?}");
}

/// Files outside the catalogs stay out of reach, whatever the spelling.
#[tokio::test]
async fn w1_no_statement_spelling_reaches_server_files() {
    let l = Local::new().await;
    let target = l.warehouse.join("leak.csv");
    let attempts = [
        format!("/* c */ COPY (SELECT 1) TO '{}'", target.display()),
        format!("EXPLAIN ANALYZE COPY (SELECT 1) TO '{}'", target.display()),
        format!("copy (select 1) to '{}' stored as csv", target.display()),
        "CREATE UNBOUNDED EXTERNAL TABLE x (a int) STORED AS CSV LOCATION '/etc/hostname'".to_owned(),
        "SELECT * FROM '/etc/hostname'".to_owned(),
        "SELECT * FROM \"file:///etc/hostname\"".to_owned(),
        "SET datafusion.runtime.memory_limit = '1G'".to_owned(),
        "SET DataFusion.Runtime.temp_directory = '/tmp'".to_owned(),
    ];
    let mut reached = Vec::new();
    for sql in &attempts {
        let got = cl_try(&l.engine, sql).await;
        note("w1_no_statement_spelling", sql, &got);
        if got.is_ok() {
            reached.push((sql.clone(), got));
        }
    }
    assert!(!target.exists(), "a file was written: {}", target.display());
    assert!(reached.is_empty(), "{reached:#?}");
}

// ---------------------------------------------------------------------------
// Red-team F2 probes with their setup corrected (see the module docs)
// ---------------------------------------------------------------------------

/// `rt_wide_insert_fits_a_small_pool` with Trino 431 SQL: 120k rows of ~1 KB
/// streamed through a 48 MiB pool. A writer that reserves memory flushes
/// early instead of failing the statement.
#[tokio::test]
async fn w1_wide_ctas_fits_a_small_pool() {
    let dir = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let engine = engine_at(
        &warehouse,
        spill.path(),
        ByteSize::mib(48),
        HiveTimestampPrecision::Milliseconds,
    )
    .await;
    cl(&engine, "CREATE SCHEMA hive.s").await;
    let result = cl_try(
        &engine,
        "CREATE TABLE hive.s.wide AS SELECT CAST(value AS BIGINT) AS id, \
         concat(CAST(value AS VARCHAR), array_join(repeat('x', 1000), '')) AS s \
         FROM generate_series(1, 120000)",
    )
    .await;
    note("w1_wide_ctas", "wide CTAS in 48 MiB", &result);
    assert_eq!(result, Ok(rows(&[&["120000"]])));
    assert_eq!(
        cl_try(&engine, "SELECT count(*), sum(length(s)) FROM hive.s.wide").await,
        Ok(rows(&[&["120000", "120608895"]]))
    );
}

/// `rt_drop_during_select_never_returns_a_partial_count` with the sum of the
/// rows it inserts: a scan racing a `DROP` answers everything or an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_drop_during_select_never_returns_a_partial_count() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    let mut bad = Vec::new();
    for (i, delay) in [0_u64, 10, 40, 120].into_iter().enumerate() {
        let name = format!("hive.s.r{i}");
        cl(
            &l.engine,
            &format!("CREATE TABLE {name} AS SELECT CAST(0 AS BIGINT) AS v"),
        )
        .await;
        for _ in 0..16 {
            cl(
                &l.engine,
                &format!("INSERT INTO {name} SELECT CAST(value AS BIGINT) FROM generate_series(1, 250000)"),
            )
            .await;
        }
        let select_sql = format!("SELECT count(*), sum(v) FROM {name}");
        let select = cl_try(&l.engine, &select_sql);
        let drop = async {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            cl_try(&l.engine, &format!("DROP TABLE {name}")).await
        };
        let (read, dropped) = tokio::join!(select, drop);
        note(
            "w1_drop_during_select",
            &format!("delay {delay}ms"),
            (&read, &dropped),
        );
        let full = rows(&[&["4000001", "500002000000"]]);
        if let Ok(r) = &read
            && *r != full
        {
            bad.push((delay, read.clone(), dropped));
        }
    }
    assert!(bad.is_empty(), "partial result instead of an error: {bad:#?}");
}

// ---------------------------------------------------------------------------
// Trino 431 on the same warehouse
// ---------------------------------------------------------------------------

/// The dev container's `/work/target` (volume `codelake-target-w1`) is
/// mounted at the same path in Trino, so both engines see one warehouse.
const SHARED_MOUNT: &str = "/work/target";

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

/// Heap capped: the image default takes most of the 12 GB VM.
const JVM: &str = "-server\n-Xmx768M\n-XX:G1HeapRegionSize=32M\n-XX:+ExplicitGCInvokesConcurrent\n\
                   -XX:+ExitOnOutOfMemoryError\n-XX:-OmitStackTraceInFastThrow\n\
                   -XX:ReservedCodeCacheSize=128M\n-Djdk.attach.allowAttachSelf=true\n\
                   -Djdk.nio.maxCachedBufferSize=2000000\n";

/// Trino's REST protocol, one JSON line per statement. `PARAMETRIC_DATETIME`
/// lets the client see timestamps at their precision (without it Trino
/// rounds every timestamp to milliseconds on the wire).
const CLIENT: &str = r#"
import json, sys, time, urllib.request

def fetch(request):
    return json.load(urllib.request.urlopen(request, timeout=300))

def run(sql, user):
    for _ in range(60):
        answer = run_once(sql, user)
        if answer.get("error") != "No nodes available to run query":
            return answer
        time.sleep(1)
    return answer

def run_once(sql, user):
    headers = {"X-Trino-User": user, "Content-Type": "text/plain",
               "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME"}
    body = fetch(urllib.request.Request("http://localhost:8080/v1/statement", data=sql.encode(), headers=headers, method="POST"))
    rows = []
    while True:
        rows.extend(body.get("data") or [])
        if "error" in body:
            return {"error": body["error"]["message"]}
        if not body.get("nextUri"):
            return {"rows": rows}
        body = fetch(urllib.request.Request(body["nextUri"], headers=headers))

request = json.loads(sys.argv[1])
for statement in request["statements"]:
    print(json.dumps(run(statement, request["user"])), flush=True)
"#;

struct Trino {
    container: ContainerAsync<GenericImage>,
}

fn cell(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_owned(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

impl Trino {
    async fn start(name: &str, warehouse: &Path, mount: Mount) -> Self {
        // Every catalog in `CodeLake`'s default Parquet time zone, but
        // `hive_utc` (the official image's default, the JVM's zone).
        let base = format!(
            "connector.name=hive\nhive.metastore=file\nhive.metastore.catalog.dir=file:{}\n",
            warehouse.display()
        );
        let catalog =
            |more: &str| format!("{base}hive.parquet.time-zone=America/Sao_Paulo\n{more}").into_bytes();
        let container = GenericImage::new("trinodb/trino", "431")
            .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
            .with_container_name(name)
            .with_user("root")
            .with_mount(mount)
            .with_startup_timeout(Duration::from_secs(240))
            .with_host_config_modifier(|config| config.memory = Some(3 << 29))
            .with_copy_to("/etc/trino/jvm.config", JVM.as_bytes().to_vec())
            .with_copy_to(
                "/etc/trino/catalog/hive.properties",
                catalog("hive.security=allow-all\n"),
            )
            .with_copy_to(
                "/etc/trino/catalog/hive_us.properties",
                catalog("hive.security=allow-all\nhive.timestamp-precision=MICROSECONDS\n"),
            )
            .with_copy_to(
                "/etc/trino/catalog/hive_ns.properties",
                catalog("hive.security=allow-all\nhive.timestamp-precision=NANOSECONDS\n"),
            )
            .with_copy_to(
                "/etc/trino/catalog/hive_std.properties",
                catalog("hive.security=sql-standard\n"),
            )
            .with_copy_to(
                "/etc/trino/catalog/hive_berlin.properties",
                format!("{base}hive.parquet.time-zone=Europe/Berlin\nhive.security=allow-all\n").into_bytes(),
            )
            .with_copy_to(
                "/etc/trino/catalog/hive_utc.properties",
                format!("{base}hive.parquet.time-zone=UTC\nhive.security=allow-all\n").into_bytes(),
            )
            .with_copy_to("/tmp/client.py", CLIENT.as_bytes().to_vec())
            .start()
            .await
            .expect("trino 431 starts");
        let trino = Self { container };
        // Ready once the coordinator lists an active worker.
        for _ in 0..120 {
            if let Ok(Ok(rows)) = trino.try_sql_as("root", crate::common::ACTIVE_WORKERS).await
                && crate::common::counts_a_worker(
                    rows.first().and_then(|row| row.first()).map(String::as_str),
                )
            {
                return trino;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        panic!("trino did not list an active worker");
    }

    async fn try_sql_as(&self, user: &str, sql: &str) -> Result<Result<Rows, String>, String> {
        let request = json!({"user": user, "statements": [sql]}).to_string();
        let mut result = self
            .container
            .exec(ExecCommand::new(vec![
                "python3".to_owned(),
                "/tmp/client.py".to_owned(),
                request,
            ]))
            .await
            .map_err(|e| e.to_string())?;
        let stdout =
            String::from_utf8_lossy(&result.stdout_to_vec().await.map_err(|e| e.to_string())?).into_owned();
        let line = stdout
            .lines()
            .find(|l| !l.trim().is_empty())
            .ok_or_else(|| format!("no answer: {stdout}"))?;
        let value: Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
        if let Some(Value::String(message)) = value.get("error") {
            return Ok(Err(message.clone()));
        }
        Ok(Ok(value["rows"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.as_array()
                            .map(|r| r.iter().map(cell).collect())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .unwrap_or_default()))
    }

    async fn sql_as(&self, user: &str, sql: &str) -> Result<Rows, String> {
        self.try_sql_as(user, sql)
            .await
            .unwrap_or_else(|e| panic!("trino client: {e}"))
    }

    async fn sql(&self, sql: &str) -> Result<Rows, String> {
        self.sql_as("root", sql).await
    }

    async fn ok(&self, sql: &str) -> Rows {
        self.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("Trino `{sql}` failed: {e}"))
    }
}

/// A warehouse on the shared volume, Trino on it, and a `CodeLake` engine.
struct Shared {
    base: PathBuf,
    warehouse: PathBuf,
    spill: tempfile::TempDir,
    trino: Trino,
    engine: Engine,
}

impl Shared {
    async fn new(name: &str) -> Self {
        let Some(mount) = own_mount(SHARED_MOUNT).await else {
            panic!("run inside scripts/dev.sh: the warehouse is shared through /work/target");
        };
        let base = Path::new(SHARED_MOUNT)
            .join("hive-w1rt")
            .join(uuid::Uuid::new_v4().simple().to_string());
        let warehouse = base.join("warehouse");
        std::fs::create_dir_all(&warehouse).unwrap();
        let spill = tempfile::tempdir().unwrap();
        let trino = Trino::start(name, &warehouse, mount).await;
        let engine = engine(&warehouse, spill.path()).await;
        Self {
            base,
            warehouse,
            spill,
            trino,
            engine,
        }
    }

    async fn at(&self, precision: HiveTimestampPrecision) -> Engine {
        engine_at(&self.warehouse, self.spill.path(), ByteSize::mib(256), precision).await
    }

    async fn zoned(&self, precision: HiveTimestampPrecision, zone: &str) -> Engine {
        engine_in(
            &self.warehouse,
            self.spill.path(),
            ByteSize::mib(256),
            precision,
            zone.parse().unwrap(),
        )
        .await
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

/// Differences between the engines, by finding.
#[derive(Default)]
struct Checks {
    probe: &'static str,
    diffs: Vec<String>,
}

impl Checks {
    fn new(probe: &'static str) -> Self {
        Self {
            probe,
            diffs: Vec::new(),
        }
    }

    /// Same rows as `reference`. Two errors are a difference too: a check
    /// that expects both engines to refuse says so with
    /// [`Checks::both_refuse`]. `got` is usually `CodeLake`'s answer and
    /// `reference` Trino's, but either can be another read or a literal
    /// (`what` says which), so the difference names neither engine.
    fn same(&mut self, id: &str, what: &str, got: &Result<Rows, String>, reference: &Result<Rows, String>) {
        note(self.probe, &format!("{id} {what}"), (got, reference));
        let same = matches!((got, reference), (Ok(a), Ok(b)) if a == b);
        if !same {
            self.diffs
                .push(format!("{id} {what}: got {got:?} / expected {reference:?}"));
        }
    }

    /// Both engines refuse, `CodeLake` with an error containing `codelake`
    /// and Trino with one containing `trino`.
    fn both_refuse(
        &mut self,
        id: &str,
        what: &str,
        (got, codelake): (&Result<Rows, String>, &str),
        (reference, trino): (&Result<Rows, String>, &str),
    ) {
        note(self.probe, &format!("{id} {what}"), (got, reference));
        let refused =
            matches!((got, reference), (Err(a), Err(b)) if a.contains(codelake) && b.contains(trino));
        if !refused {
            self.diffs.push(format!(
                "{id} {what}: expected both to refuse ({codelake:?} / {trino:?}): CodeLake {got:?} / Trino {reference:?}"
            ));
        }
    }

    fn assert_none(&self) {
        assert!(
            self.diffs.is_empty(),
            "differences from Trino 431: {:#?}",
            self.diffs
        );
    }
}

// ---------------------------------------------------------------------------
// The 21 F2 findings, re-checked against Trino 431 (R11 apart, below)
// ---------------------------------------------------------------------------

const P: &str = "w1_f2_findings_answer_like_trino_431";

/// Findings 12 and 13: listings see other writers at once, and a table
/// `CodeLake` cannot read hides nothing else.
async fn findings_12_13(s: &Shared, checks: &mut Checks) {
    s.trino
        .ok("CREATE TABLE hive.s.early AS SELECT BIGINT '1' AS id")
        .await;
    let listing = "SELECT table_name FROM hive.information_schema.tables WHERE table_schema = 's' ORDER BY 1";
    checks.same(
        "F12",
        "listing after another writer's CREATE",
        &cl_try(&s.engine, listing).await,
        &s.trino.sql(listing).await,
    );
    s.trino
        .ok("CREATE TABLE hive.s.arr (a array(integer), b bigint) WITH (format = 'PARQUET')")
        .await;
    s.trino
        .ok("CREATE TABLE hive.s.pq WITH (format = 'PARQUET') AS SELECT BIGINT '1' AS id")
        .await;
    s.trino
        .ok("CREATE TABLE hive.s.part (x bigint, p varchar) WITH (partitioned_by = ARRAY['p'])")
        .await;
    s.trino
        .ok("CREATE TABLE hive.s.orc (x bigint) WITH (format = 'ORC')")
        .await;
    checks.same(
        "F13",
        "tables with an array, a partitioned and an ORC table",
        &cl_try(&s.engine, listing).await,
        &s.trino.sql(listing).await,
    );
    let columns = "SELECT table_name, column_name FROM hive.information_schema.columns \
                   WHERE table_schema = 's' AND table_name IN ('pq', 'orc', 'arr') ORDER BY 1, 2";
    let codelake = cl_try(&s.engine, columns).await;
    note(P, "F13 columns of pq, orc (ORC) and arr (array)", &codelake);
    let pq_only =
        |r: Result<Rows, String>| r.map(|rows| rows.into_iter().filter(|r| r[0] == "pq").collect::<Rows>());
    checks.same(
        "F13",
        "columns of a readable table next to unreadable ones",
        &pq_only(codelake),
        &pq_only(s.trino.sql(columns).await),
    );
    checks.same(
        "F13",
        "the readable column of a table with an array column",
        &cl_try(&s.engine, "SELECT b FROM hive.s.arr").await,
        &s.trino.sql("SELECT b FROM hive.s.arr").await,
    );
}

/// Finding 8: `varchar(n)` and `char(n)`.
async fn finding_8(s: &Shared, checks: &mut Checks) {
    for t in ["by_trino", "by_codelake"] {
        s.trino
            .ok(&format!(
                "CREATE TABLE hive.s.{t} (id bigint, c varchar(3), ch char(5)) WITH (format = 'PARQUET')"
            ))
            .await;
    }
    for values in [
        "(1, 'abcdef', NULL)",
        "(2, NULL, 'ab')",
        "(3, NULL, 'abcdefgh')",
        "(9, 'ab ', NULL)",
        "(10, 'abc   ', 'abcde   ')",
    ] {
        let t = s
            .trino
            .sql(&format!("INSERT INTO hive.s.by_trino VALUES {values}"))
            .await;
        let c = cl_try(
            &s.engine,
            &format!("INSERT INTO hive.s.by_codelake VALUES {values}"),
        )
        .await;
        let (c, t) = (c.map(|_| Vec::new()), t.map(|_| Vec::new()));
        let what = format!("INSERT {values}");
        if t.is_err() {
            let too_long = "Cannot truncate non-space characters when casting from varchar(";
            checks.both_refuse("F8", &what, (&c, too_long), (&t, too_long));
        } else {
            checks.same("F8", &what, &c, &t);
        }
    }
    let read = "SELECT id, c, CAST(ch AS varchar), length(ch) FROM hive.s.{t} ORDER BY id";
    checks.same(
        "F8",
        "Trino reads the rows each engine wrote",
        &s.trino.sql(&read.replace("{t}", "by_codelake")).await,
        &s.trino.sql(&read.replace("{t}", "by_trino")).await,
    );
    let read = read.replace("{t}", "by_trino");
    checks.same(
        "F8",
        "both read the rows Trino wrote",
        &cl_try(&s.engine, &read).await,
        &s.trino.sql(&read).await,
    );
    let created = cl_try(
        &s.engine,
        "CREATE TABLE hive.s.decl_cl (c varchar(3), ch char(5))",
    )
    .await;
    s.trino
        .ok("CREATE TABLE hive.s.decl_trino (c varchar(3), ch char(5))")
        .await;
    note(P, "F8 CodeLake CREATE with bounded types", &created);
    let describe = |t: &str| {
        format!(
            "SELECT column_name, data_type FROM hive.information_schema.columns \
             WHERE table_schema = 's' AND table_name = '{t}' ORDER BY ordinal_position"
        )
    };
    checks.same(
        "F8",
        "declared types as Trino sees them",
        &s.trino.sql(&describe("decl_cl")).await,
        &s.trino.sql(&describe("decl_trino")).await,
    );
    // Oracle for DEFECT-W1-4: what Trino answers to char comparisons that
    // the optimizer keeps above the scan.
    for q in [
        "SELECT id FROM hive.s.by_trino WHERE ch = 'ab' OR random() > 2",
        "SELECT id FROM (SELECT id, ch FROM hive.s.by_trino ORDER BY id LIMIT 10) x WHERE ch = 'ab'",
    ] {
        note(
            P,
            &format!("oracle W1-4 {q}"),
            (s.trino.sql(q).await, cl_try(&s.engine, q).await),
        );
    }
}

/// Finding 9: rounding of timestamps, written and read.
async fn finding_9(s: &Shared, checks: &mut Checks) {
    for t in ["ts_trino", "ts_codelake"] {
        s.trino
            .ok(&format!(
                "CREATE TABLE hive.s.{t} (id bigint, ts timestamp(3)) WITH (format = 'PARQUET')"
            ))
            .await;
    }
    let values = "(6, TIMESTAMP '2026-01-01 00:00:00.9996'), (7, TIMESTAMP '2026-01-01 00:00:00.1235'), \
                  (8, TIMESTAMP '1969-12-31 23:59:59.9984')";
    s.trino
        .ok(&format!("INSERT INTO hive.s.ts_trino VALUES {values}"))
        .await;
    let inserted = cl_try(
        &s.engine,
        &format!("INSERT INTO hive.s.ts_codelake VALUES {values}"),
    )
    .await;
    note(P, "F9 CodeLake INSERT", &inserted);
    let read = "SELECT id, CAST(ts AS varchar) FROM hive.s.{t} ORDER BY id";
    checks.same(
        "F9",
        "Trino reads the timestamps each engine wrote",
        &s.trino.sql(&read.replace("{t}", "ts_codelake")).await,
        &s.trino.sql(&read.replace("{t}", "ts_trino")).await,
    );
    s.trino
        .ok("CREATE TABLE hive.s.nanos (ts timestamp(3)) WITH (format = 'PARQUET')")
        .await;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            1_767_225_600_999_600_000,
            -400_000,
            1_767_225_600_123_500_000,
        ])) as ArrayRef],
    )
    .unwrap();
    put_parquet(&s.warehouse.join("s/nanos/20200101_000000_00000_spark"), &batch);
    let read = "SELECT CAST(ts AS varchar) FROM hive.s.nanos ORDER BY 1";
    checks.same(
        "F9",
        "a nanosecond file read into timestamp(3)",
        &cl_try(&s.engine, read).await,
        &s.trino.sql(read).await,
    );
}

/// Findings 10 and 11: checksums, and Parquet column names in another case.
async fn findings_10_11(s: &Shared, checks: &mut Checks) {
    cl(
        &s.engine,
        "CREATE TABLE hive.s.crc AS SELECT CAST(1 AS BIGINT) AS aa",
    )
    .await;
    let file = s.warehouse.join("s/crc/.trinoSchema");
    let text = std::fs::read_to_string(&file)
        .unwrap()
        .replace("\"aa\"", "\"zz\"");
    std::fs::write(&file, text).unwrap();
    checks.both_refuse(
        "F10",
        "metadata that does not match its .crc",
        (
            &cl_try(&s.engine, "SELECT * FROM hive.s.crc").await,
            "does not match its checksum file",
        ),
        (
            &s.trino.sql("SELECT * FROM hive.s.crc").await,
            "Could not read table schema",
        ),
    );
    s.trino
        .ok("CREATE TABLE hive.s.upper (id bigint) WITH (format = 'PARQUET')")
        .await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![5])) as ArrayRef],
    )
    .unwrap();
    put_parquet(&s.warehouse.join("s/upper/20200101_000000_00000_pandas"), &batch);
    checks.same(
        "F11",
        "Parquet column ID in table column id",
        &cl_try(&s.engine, "SELECT id FROM hive.s.upper").await,
        &s.trino.sql("SELECT id FROM hive.s.upper").await,
    );
}

/// Findings 15, 16 and 17: views across the engines.
async fn findings_15_16_17(s: &Shared, checks: &mut Checks) {
    let zoned = "SELECT TIMESTAMP '2026-01-01 10:00:00 America/Sao_Paulo' AS t";
    s.trino.ok(&format!("CREATE VIEW hive.s.vtz AS {zoned}")).await;
    let instant = "SELECT CAST(to_unixtime(t) AS bigint) FROM hive.s.vtz";
    checks.same(
        "F15",
        "instant of a Trino view with timestamp with time zone",
        &cl_try(&s.engine, instant).await,
        &s.trino.sql(instant).await,
    );
    let text = "SELECT CAST(t AS varchar) FROM hive.s.vtz";
    note(
        P,
        "F15 residual: text of the zoned value (CodeLake, Trino)",
        (cl_try(&s.engine, text).await, s.trino.sql(text).await),
    );
    let created = cl_try(&s.engine, &format!("CREATE VIEW hive.s.vz AS {zoned}")).await;
    note(P, "F15 CodeLake creates the zoned view", &created);
    checks.same(
        "F15",
        "instant of a CodeLake view with timestamp with time zone, read by Trino",
        &s.trino.sql(&instant.replace("vtz", "vz")).await,
        &s.trino.sql(instant).await,
    );
    s.trino
        .ok("CREATE VIEW hive.s.tva AS SELECT BIGINT '1' AS id, ARRAY[1, 2] AS arr")
        .await;
    checks.same(
        "F16",
        "Trino view with an array column, array not selected",
        &cl_try(&s.engine, "SELECT id FROM hive.s.tva").await,
        &s.trino.sql("SELECT id FROM hive.s.tva").await,
    );
    let created = cl_try(
        &s.engine,
        "CREATE VIEW hive.s.vm AS SELECT \"MixedCol\" FROM (SELECT 1 AS \"MixedCol\") t",
    )
    .await;
    note(P, "F17 CodeLake creates vm", &created);
    checks.same(
        "F17",
        "CodeLake view with a quoted mixed-case alias",
        &cl_try(&s.engine, "SELECT * FROM hive.s.vm").await,
        &s.trino.sql("SELECT * FROM hive.s.vm").await,
    );
}

/// Findings 18, 20 and 21: column options, precision, far timestamps.
async fn findings_18_20_21(s: &Shared, checks: &mut Checks) {
    s.trino
        .ok("CREATE TABLE hive.s.alt_trino AS SELECT BIGINT '1' AS id")
        .await;
    cl(
        &s.engine,
        "CREATE TABLE hive.s.alt_cl AS SELECT CAST(1 AS BIGINT) AS id",
    )
    .await;
    let not_null = "Catalog 'hive' does not support NOT NULL for column 'n'";
    checks.both_refuse(
        "F18",
        "ADD COLUMN NOT NULL",
        (
            &cl_try(&s.engine, "ALTER TABLE hive.s.alt_cl ADD COLUMN n INT NOT NULL").await,
            not_null,
        ),
        (
            &s.trino
                .sql("ALTER TABLE hive.s.alt_trino ADD COLUMN n INT NOT NULL")
                .await,
            not_null,
        ),
    );
    let create_refused = "Catalog 'hive' does not support non-null column for column name 'n'";
    checks.both_refuse(
        "F18",
        "CREATE TABLE with NOT NULL",
        (
            &cl_try(&s.engine, "CREATE TABLE hive.s.nn_cl (n INT NOT NULL)").await,
            create_refused,
        ),
        (
            &s.trino.sql("CREATE TABLE hive.s.nn_trino (n INT NOT NULL)").await,
            create_refused,
        ),
    );
    let precision =
        "Incorrect timestamp precision for timestamp(6); the configured precision is MILLISECONDS";
    checks.both_refuse(
        "F20",
        "CREATE TABLE timestamp(6)",
        (
            &cl_try(&s.engine, "CREATE TABLE hive.s.t6_cl (ts timestamp(6))").await,
            precision,
        ),
        (
            &s.trino
                .sql("CREATE TABLE hive.s.t6_trino (ts timestamp(6))")
                .await,
            precision,
        ),
    );
    let ctas = "CREATE TABLE hive.s.{t} AS SELECT TIMESTAMP '2026-01-01 00:00:00.9996' AS ts";
    // The engine carries the literal in microseconds and names timestamp(6)
    // where Trino names timestamp(4); both refuse the precision.
    let refused = "the configured precision is MILLISECONDS; column name: ts";
    checks.both_refuse(
        "F20",
        "CTAS of a timestamp(4) literal",
        (&cl_try(&s.engine, &ctas.replace("{t}", "c4_cl")).await, refused),
        (&s.trino.sql(&ctas.replace("{t}", "c4_trino")).await, refused),
    );
    let far = "CREATE TABLE hive.s.{t} AS SELECT id, ts FROM (VALUES \
               (BIGINT '1', TIMESTAMP '1582-10-10 12:00:00.000'), (2, TIMESTAMP '0001-01-01 00:00:00.000'), \
               (3, TIMESTAMP '9999-12-31 23:59:59.999'), (4, TIMESTAMP '1899-12-31 23:59:59.999')) v(id, ts)";
    let created = cl_try(&s.engine, &far.replace("{t}", "far_cl")).await;
    note(P, "F21 CodeLake CTAS", &created);
    s.trino.ok(&far.replace("{t}", "far_trino")).await;
    let read = "SELECT id, CAST(ts AS varchar) FROM hive.s.{t} ORDER BY id";
    checks.same(
        "F21",
        "far timestamps CodeLake wrote, read by Trino",
        &s.trino.sql(&read.replace("{t}", "far_cl")).await,
        &s.trino.sql(&read.replace("{t}", "far_trino")).await,
    );
}

/// Findings 8–13, 15–18, 20 and 21 of the F2 red-team, against Trino 431 on
/// the same warehouse (1–7 and 19 are the local `hive_redteam` probes).
#[tokio::test]
async fn w1_f2_findings_answer_like_trino_431() {
    let s = Shared::new("codelake-w1-trino-f2").await;
    let mut checks = Checks::new(P);
    s.trino.ok("CREATE SCHEMA hive.s").await;
    findings_12_13(&s, &mut checks).await;
    finding_8(&s, &mut checks).await;
    finding_9(&s, &mut checks).await;
    findings_10_11(&s, &mut checks).await;
    findings_15_16_17(&s, &mut checks).await;
    findings_18_20_21(&s, &mut checks).await;
    // Oracle for DEFECT-W1-1: Trino over a directory without a table.
    std::fs::create_dir_all(s.warehouse.join("s/foreign")).unwrap();
    std::fs::write(s.warehouse.join("s/foreign/part-0"), b"x").unwrap();
    note(
        P,
        "oracle W1-1: Trino CREATE over a foreign directory",
        s.trino.sql("CREATE TABLE hive.s.foreign AS SELECT 1 AS x").await,
    );
    checks.assert_none();
}

/// Finding 14 (R11): owners. Pending F5 (authentication); kept as a probe.
#[tokio::test]
#[ignore = "DEFECT-W1-6: R11 still open (known, F5): any user drops or writes another user's table"]
async fn w1_r11_owner_checks_like_trino_sql_standard() {
    let s = Shared::new("codelake-w1-trino-r11").await;
    let mut checks = Checks::new("w1_r11_owner_checks");
    s.trino.ok("CREATE SCHEMA hive.s").await;
    for t in ["alice1", "alice2"] {
        run_with(
            &s.engine,
            &QueryOptions::for_user("alice"),
            &format!("CREATE TABLE hive.s.{t} AS SELECT CAST(1 AS BIGINT) AS id"),
        )
        .await
        .unwrap();
    }
    checks.same(
        "F14",
        "mallory drops alice's table",
        &run_with(
            &s.engine,
            &QueryOptions::for_user("mallory"),
            "DROP TABLE hive.s.alice1",
        )
        .await,
        &s.trino.sql_as("mallory", "DROP TABLE hive_std.s.alice2").await,
    );
    checks.same(
        "F14",
        "mallory inserts into alice's table",
        &run_with(
            &s.engine,
            &QueryOptions::for_user("mallory"),
            "INSERT INTO hive.s.alice2 VALUES (666)",
        )
        .await
        .map(|_| Vec::new()),
        &s.trino
            .sql_as("mallory", "INSERT INTO hive_std.s.alice2 VALUES (666)")
            .await
            .map(|_| Vec::new()),
    );
    checks.assert_none();
}

// ---------------------------------------------------------------------------
// OPTIMIZE and the F2 writers, read by Trino 431 after every operation
// ---------------------------------------------------------------------------

const O: &str = "w1_trino_reads_every_operation";

/// `count(*)`, `sum(id)`, `count(x)` in both engines after `step`.
async fn both_read(s: &Shared, checks: &mut Checks, step: &str) {
    let q = "SELECT count(*), sum(id), count(x), count(DISTINCT v) FROM hive.s.ops";
    checks.same("OPS", step, &cl_try(&s.engine, q).await, &s.trino.sql(q).await);
}

/// Every write `CodeLake` does, then Trino reads the table.
async fn operations(s: &Shared, checks: &mut Checks) {
    cl(
        &s.engine,
        "CREATE TABLE hive.s.ops AS SELECT CAST(1 AS BIGINT) AS id, 'a' AS v",
    )
    .await;
    both_read_no_x(s, checks, "CTAS").await;
    cl(&s.engine, "INSERT INTO hive.s.ops VALUES (2, 'b'), (3, 'c')").await;
    cl(&s.engine, "ALTER TABLE hive.s.ops ADD COLUMN x bigint").await;
    both_read(s, checks, "INSERT, ADD COLUMN").await;
    cl(&s.engine, "INSERT INTO hive.s.ops VALUES (4, 'd', 40)").await;
    s.trino.ok("INSERT INTO hive.s.ops VALUES (5, 'e', 50)").await;
    both_read(s, checks, "INSERT by each engine").await;
    let compacted = optimize(&s.engine, "hive.s.ops").await;
    note(O, "OPTIMIZE", &compacted);
    both_read(s, checks, "OPTIMIZE").await;
    let replaced = cl_try(
        &s.engine,
        "CREATE OR REPLACE TABLE hive.s.ops AS SELECT id, v, x FROM hive.s.ops WHERE id > 1",
    )
    .await;
    note(O, "OR REPLACE", &replaced);
    both_read(s, checks, "CREATE OR REPLACE").await;
    s.trino.ok("INSERT INTO hive.s.ops VALUES (6, 'f', 60)").await;
    let compacted = optimize(&s.engine, "hive.s.ops").await;
    note(O, "OPTIMIZE after OR REPLACE", &compacted);
    both_read(s, checks, "Trino INSERT, OPTIMIZE after OR REPLACE").await;
    cl(&s.engine, "DROP TABLE hive.s.ops").await;
    checks.same(
        "OPS",
        "DROP, then Trino creates the name again",
        &Ok(Vec::new()),
        &s.trino
            .sql("CREATE TABLE hive.s.ops AS SELECT 1 AS id")
            .await
            .map(|_| Vec::new()),
    );
}

async fn both_read_no_x(s: &Shared, checks: &mut Checks, step: &str) {
    let q = "SELECT count(*), sum(id), count(DISTINCT v) FROM hive.s.ops";
    checks.same("OPS", step, &cl_try(&s.engine, q).await, &s.trino.sql(q).await);
}

/// `INT96` timestamps Trino wrote, at milliseconds and at microseconds,
/// compacted by `CodeLake`: Trino reads the same values.
async fn precision_through_optimize(s: &Shared, checks: &mut Checks) {
    s.trino
        .ok("CREATE TABLE hive.s.tsx (id bigint, ts timestamp(3)) WITH (format = 'PARQUET')")
        .await;
    for values in [
        "(1, TIMESTAMP '0001-01-01 00:00:00.000'), (2, TIMESTAMP '1582-10-10 12:00:00.000')",
        "(3, TIMESTAMP '1899-12-31 23:59:59.999'), (4, TIMESTAMP '1969-12-31 23:59:59.999')",
        "(5, TIMESTAMP '9999-12-31 23:59:59.999'), (6, TIMESTAMP '2026-01-01 10:00:00.123')",
    ] {
        s.trino
            .ok(&format!("INSERT INTO hive.s.tsx VALUES {values}"))
            .await;
    }
    let q = "SELECT id, CAST(ts AS varchar) FROM hive.s.tsx ORDER BY id";
    let (trino_before, codelake_before) = (s.trino.sql(q).await, cl_try(&s.engine, q).await);
    let compacted = optimize(&s.engine, "hive.s.tsx").await;
    note(
        O,
        "OPTIMIZE tsx",
        (&compacted, data_files(&s.warehouse.join("s/tsx"))),
    );
    checks.same(
        "INT96",
        "Trino after OPTIMIZE (ms)",
        &s.trino.sql(q).await,
        &trino_before,
    );
    checks.same(
        "INT96",
        "CodeLake after OPTIMIZE (ms)",
        &cl_try(&s.engine, q).await,
        &codelake_before,
    );
    s.trino
        .ok("CREATE TABLE hive_us.s.tus (id bigint, ts timestamp(6)) WITH (format = 'PARQUET')")
        .await;
    for values in [
        "(1, TIMESTAMP '2026-01-01 10:00:00.123999')",
        "(2, TIMESTAMP '1500-06-01 00:00:00.000001')",
        "(3, TIMESTAMP '9999-12-31 23:59:59.999999')",
    ] {
        s.trino
            .ok(&format!("INSERT INTO hive_us.s.tus VALUES {values}"))
            .await;
    }
    let q = "SELECT id, CAST(ts AS varchar) FROM hive_us.s.tus ORDER BY id";
    let before = s.trino.sql(q).await;
    let compacted = optimize(&s.engine, "hive.s.tus").await;
    note(O, "OPTIMIZE tus", &compacted);
    checks.same(
        "INT96",
        "Trino MICROSECONDS after OPTIMIZE",
        &s.trino.sql(q).await,
        &before,
    );
}

/// Trino `INSERT`s (no `CodeLake` lock) racing `CodeLake`'s `OPTIMIZE`.
async fn trino_inserts_racing_optimize(s: &Shared, checks: &mut Checks) {
    seeded(&s.engine, "hive.s.race", 8, 2_000).await;
    let mut expected = (16_000_i64, i128::from(16_000) * 16_001 / 2);
    for round in 0..3_i64 {
        let inserts = async {
            let mut done = Vec::new();
            for i in 0..4_i64 {
                let v = 1_000_000 + round * 10 + i;
                done.push((
                    v,
                    s.trino.sql(&format!("INSERT INTO hive.s.race VALUES {v}")).await,
                ));
            }
            done
        };
        let (compacted, inserted) = tokio::join!(optimize(&s.engine, "hive.s.race"), inserts);
        note(O, &format!("race round {round}"), (&compacted, &inserted));
        for (v, result) in inserted {
            if result.is_ok() {
                expected.0 += 1;
                expected.1 += i128::from(v);
            }
        }
    }
    let want = Ok(vec![vec![expected.0.to_string(), expected.1.to_string()]]);
    let q = "SELECT count(*), CAST(sum(v) AS varchar) FROM hive.s.race";
    checks.same("RACE", "Trino count after the race", &s.trino.sql(q).await, &want);
    checks.same(
        "RACE",
        "CodeLake count after the race",
        &cl_try(&s.engine, q).await,
        &want,
    );
}

/// Trino 431 reads what `CodeLake` leaves after each write, `OPTIMIZE`
/// included, and compaction keeps the timestamps Trino wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_trino_reads_every_operation_and_optimize() {
    let s = Shared::new("codelake-w1-trino-ops").await;
    let mut checks = Checks::new(O);
    s.trino.ok("CREATE SCHEMA hive.s").await;
    operations(&s, &mut checks).await;
    precision_through_optimize(&s, &mut checks).await;
    trino_inserts_racing_optimize(&s, &mut checks).await;
    checks.assert_none();
}

/// Nanoseconds Trino wrote (NANOSECONDS catalog), compacted by a default
/// `CodeLake` catalog: the NANOSECONDS reader must see the same values.
#[tokio::test]
async fn w1_optimize_keeps_the_nanoseconds_trino_wrote() {
    let s = Shared::new("codelake-w1-trino-ns").await;
    let mut checks = Checks::new("w1_optimize_keeps_nanoseconds");
    s.trino.ok("CREATE SCHEMA hive.s").await;
    s.trino
        .ok("CREATE TABLE hive_ns.s.tns (id bigint, ts timestamp(9)) WITH (format = 'PARQUET')")
        .await;
    for values in [
        "(1, TIMESTAMP '2026-01-01 10:00:00.123456789')",
        "(2, TIMESTAMP '2026-01-01 10:00:00.000000001')",
        "(3, TIMESTAMP '1500-06-01 00:00:00.000000001')",
    ] {
        s.trino
            .ok(&format!("INSERT INTO hive_ns.s.tns VALUES {values}"))
            .await;
    }
    let q = "SELECT id, CAST(ts AS varchar) FROM hive_ns.s.tns ORDER BY id";
    let before = s.trino.sql(q).await;
    let nanos = s.at(HiveTimestampPrecision::Nanoseconds).await;
    note(
        "w1_optimize_keeps_nanoseconds",
        "oracle W1-2: Trino NANOSECONDS read / CodeLake NANOSECONDS read",
        (
            &before,
            cl_try(
                &nanos,
                "SELECT id, CAST(ts AS varchar) FROM hive.s.tns ORDER BY id",
            )
            .await,
        ),
    );
    let compacted = optimize(&s.engine, "hive.s.tns").await;
    note(
        "w1_optimize_keeps_nanoseconds",
        "OPTIMIZE (MILLISECONDS catalog)",
        &compacted,
    );
    checks.same(
        "NS",
        "Trino NANOSECONDS after OPTIMIZE",
        &s.trino.sql(q).await,
        &before,
    );
    checks.assert_none();
}

// ---------------------------------------------------------------------------
// Parquet time zone (`parquet_time_zone`, Trino's `hive.parquet.time-zone`)
// ---------------------------------------------------------------------------

const Z: &str = "w1_parquet_time_zone";

/// Wall-clock times of `America/Sao_Paulo` that move differently: standard
/// and daylight time, one the clocks repeated (2019-02-16 23:30), the local
/// mean time before 1914, and the ends of the range (whose instants stay
/// within years 1 to 9999). Times the clocks skipped are in [`GAP`].
const ZONED: &str = "(1, TIMESTAMP '2026-01-01 10:00:00.123'), (2, TIMESTAMP '2018-11-04 01:30:00.000'), \
                     (3, TIMESTAMP '2019-02-16 23:30:00.000'), (4, TIMESTAMP '2019-02-17 00:30:00.500'), \
                     (5, TIMESTAMP '2016-01-15 12:00:00.000'), (6, TIMESTAMP '1500-06-01 00:00:00.000'), \
                     (7, TIMESTAMP '1913-12-31 23:59:59.999'), (8, TIMESTAMP '1969-12-31 23:59:59.999'), \
                     (9, TIMESTAMP '9999-12-31 20:59:59.999'), (10, TIMESTAMP '0001-01-01 12:00:00.000')";

/// The same in nanoseconds, within what a NANOSECONDS catalog holds.
const ZONED_NS: [&str; 4] = [
    "(1, TIMESTAMP '2026-01-01 10:00:00.123456789')",
    "(2, TIMESTAMP '2019-02-17 00:30:00.000000001')",
    "(3, TIMESTAMP '2019-02-16 23:30:00.999999999')",
    "(4, TIMESTAMP '1900-01-01 00:00:00.000000001')",
];

/// Times the clocks skipped on 2018-11-04 (00:00 became 01:00). The
/// reference engine's writer re-reads a sample of its files and fails when a
/// value comes back different, which such a time does (moved forward), so
/// only `CodeLake` writes them.
const GAP: &str = "(1, TIMESTAMP '2018-11-04 00:30:00.000'), (2, TIMESTAMP '2018-11-04 00:59:59.999')";

fn ctas(table: &str, values: &str) -> String {
    format!("CREATE TABLE {table} AS SELECT CAST(id AS bigint) AS id, ts FROM (VALUES {values}) v(id, ts)")
}

/// [`ctas`] for the reference engine, whose default storage format is ORC.
fn parquet_ctas(table: &str, values: &str) -> String {
    format!(
        "CREATE TABLE {table} WITH (format = 'PARQUET') AS SELECT CAST(id AS bigint) AS id, ts \
         FROM (VALUES {values}) v(id, ts)"
    )
}

fn read_zoned(table: &str) -> String {
    format!("SELECT id, CAST(ts AS varchar) FROM {table} ORDER BY id")
}

/// `writer.time.zone` of the data files of `schema.table`.
fn footer_zones(s: &Shared, table: &str) -> Vec<Option<String>> {
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
    let dir = s.warehouse.join("s").join(table);
    data_files(&dir)
        .into_iter()
        .map(|name| {
            let path = dir.join(&name);
            let size = std::fs::metadata(&path).map(|m| m.len());
            let reader = SerializedFileReader::new(std::fs::File::open(&path).unwrap())
                .unwrap_or_else(|e| panic!("{} ({size:?} bytes): {e}", path.display()));
            reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .and_then(|pairs| pairs.iter().find(|kv| kv.key == "writer.time.zone"))
                .and_then(|kv| kv.value.clone())
        })
        .collect()
}

/// A catalog in `America/Sao_Paulo` reads and writes `INT96` like the
/// reference engine (431) with `hive.parquet.time-zone=America/Sao_Paulo`,
/// rows compared one by one: what each engine writes, what each reads, the
/// instants stored (read through UTC catalogs), what a UTC writer left, and
/// nanoseconds through a NANOSECONDS catalog and `OPTIMIZE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1_parquet_time_zone_reads_and_writes_like_the_reference_engine() {
    let s = Shared::new("codelake-w1-trino-tz").await;
    let mut checks = Checks::new(Z);
    s.trino.ok("CREATE SCHEMA hive.s").await;
    let utc = s.zoned(HiveTimestampPrecision::Milliseconds, "UTC").await;

    // Each engine writes the same wall-clock times in the zone.
    s.trino.ok(&parquet_ctas("hive.s.by_trino", ZONED)).await;
    cl(&s.engine, &ctas("hive.s.by_codelake", ZONED)).await;
    for table in ["by_trino", "by_codelake"] {
        let q = read_zoned(&format!("hive.s.{table}"));
        checks.same(
            "TZ1",
            &format!("{table} read in the zone"),
            &cl_try(&s.engine, &q).await,
            &s.trino.sql(&q).await,
        );
    }
    checks.same(
        "TZ2",
        "the same wall-clock times, whoever wrote them",
        &s.trino.sql(&read_zoned("hive.s.by_codelake")).await,
        &s.trino.sql(&read_zoned("hive.s.by_trino")).await,
    );
    // The instants stored, read without conversion: the writers agree.
    checks.same(
        "TZ3",
        "instants CodeLake stored / instants Trino stored",
        &cl_try(&utc, &read_zoned("hive.s.by_codelake")).await,
        &s.trino.sql(&read_zoned("hive_utc.s.by_trino")).await,
    );
    let footer = |zones: Vec<Option<String>>| -> Result<Rows, String> {
        Ok(zones
            .into_iter()
            .map(|z| vec![z.unwrap_or_else(|| "NULL".into())])
            .collect())
    };
    checks.same(
        "TZ4",
        "writer.time.zone in the footer",
        &footer(footer_zones(&s, "by_codelake")),
        &footer(footer_zones(&s, "by_trino")),
    );

    skipped_times(&s, &mut checks).await;

    // A table another engine wrote in UTC reads shifted, as in Trino.
    s.trino.ok(&parquet_ctas("hive_utc.s.by_utc", ZONED)).await;
    checks.same(
        "TZ5",
        "a UTC writer's table read in the zone",
        &cl_try(&s.engine, &read_zoned("hive.s.by_utc")).await,
        &s.trino.sql(&read_zoned("hive.s.by_utc")).await,
    );
    checks.same(
        "TZ6",
        "a UTC writer's table read in UTC",
        &cl_try(&utc, &read_zoned("hive.s.by_utc")).await,
        &s.trino.sql(&read_zoned("hive_utc.s.by_utc")).await,
    );

    nanoseconds_in_the_zone(&s, &mut checks).await;
    checks.assert_none();
}

/// Times the clocks skipped ([`GAP`]), written by `CodeLake` in the zone.
async fn skipped_times(s: &Shared, checks: &mut Checks) {
    // Skipped times move forward by the gap, stored as the instant after it.
    cl(&s.engine, &ctas("hive.s.gap", GAP)).await;
    let moved = Ok(rows(&[
        &["1", "2018-11-04 01:30:00.000"],
        &["2", "2018-11-04 01:59:59.999"],
    ]));
    checks.same(
        "TZ-GAP1",
        "skipped times read by the reference engine",
        &s.trino.sql(&read_zoned("hive.s.gap")).await,
        &moved,
    );
    checks.same(
        "TZ-GAP2",
        "skipped times read by CodeLake",
        &cl_try(&s.engine, &read_zoned("hive.s.gap")).await,
        &moved,
    );
    checks.same(
        "TZ-GAP3",
        "instants of the skipped times",
        &s.trino.sql(&read_zoned("hive_utc.s.gap")).await,
        &Ok(rows(&[
            &["1", "2018-11-04 03:30:00.000"],
            &["2", "2018-11-04 03:59:59.999"],
        ])),
    );
}

/// NANOSECONDS catalogs in the zone, then `OPTIMIZE` from a MILLISECONDS
/// catalog, which must keep every digit and instant.
async fn nanoseconds_in_the_zone(s: &Shared, checks: &mut Checks) {
    let nanos = s
        .zoned(HiveTimestampPrecision::Nanoseconds, "America/Sao_Paulo")
        .await;
    s.trino
        .ok("CREATE TABLE hive_ns.s.ns_trino (id bigint, ts timestamp(9)) WITH (format = 'PARQUET')")
        .await;
    for values in ZONED_NS {
        s.trino
            .ok(&format!("INSERT INTO hive_ns.s.ns_trino VALUES {values}"))
            .await;
    }
    cl(&nanos, &ctas("hive.s.ns_codelake", &ZONED_NS.join(", "))).await;
    let trino_ns = s.trino.sql(&read_zoned("hive_ns.s.ns_trino")).await;
    checks.same(
        "TZ7",
        "NANOSECONDS read of Trino's table",
        &cl_try(&nanos, &read_zoned("hive.s.ns_trino")).await,
        &trino_ns,
    );
    checks.same(
        "TZ8",
        "NANOSECONDS: CodeLake's table read by Trino",
        &s.trino.sql(&read_zoned("hive_ns.s.ns_codelake")).await,
        &trino_ns,
    );
    let compacted = optimize(&s.engine, "hive.s.ns_trino").await;
    note(Z, "OPTIMIZE of ns_trino (MILLISECONDS catalog)", &compacted);
    checks.same(
        "TZ9",
        "NANOSECONDS read after OPTIMIZE",
        &s.trino.sql(&read_zoned("hive_ns.s.ns_trino")).await,
        &trino_ns,
    );
    checks.same(
        "TZ10",
        "OPTIMIZE left one file",
        &Ok(rows(&[&["1"]])),
        &Ok(vec![vec![
            data_files(&s.warehouse.join("s/ns_trino")).len().to_string(),
        ]]),
    );
}

// ---------------------------------------------------------------------------
// Tester probes of the W1 corrections (front/w1-df54)
// ---------------------------------------------------------------------------

/// A Parquet file with `id` and an `INT64` timestamp (Arrow milliseconds,
/// not adjusted to UTC), as Spark or pandas write it.
fn int64_timestamps(ids: &[i64], millis: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(millis.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// 2026-01-01 10:00:00.456 as milliseconds since the epoch.
const TEN_O_CLOCK_456: i64 = 1_767_261_600_456;

/// `OPTIMIZE` rewrites every timestamp as `INT96` without converting it, and
/// in a catalog whose `parquet_time_zone` is not UTC only `INT96` converts on
/// read: an `INT64` timestamp another writer left in the table must read the
/// same after compaction.
#[tokio::test]
async fn w1t_optimize_keeps_int64_timestamps_of_other_writers() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.mixed AS SELECT CAST(1 AS BIGINT) AS id, TIMESTAMP '2026-01-01 10:00:00.123' AS ts",
    )
    .await;
    put_parquet(
        &l.warehouse.join("s/mixed/spark-00000.parquet"),
        &int64_timestamps(&[2, 3], &[TEN_O_CLOCK_456, -1]),
    );
    let q = "SELECT id, CAST(ts AS varchar) FROM hive.s.mixed ORDER BY id";
    let before = cl_try(&l.engine, q).await;
    let compacted = optimize(&l.engine, "hive.s.mixed").await;
    let after = cl_try(&l.engine, q).await;
    note(
        "w1t_optimize_int64",
        "before, OPTIMIZE, files, after",
        (
            &before,
            &compacted,
            data_files(&l.warehouse.join("s/mixed")),
            &after,
        ),
    );
    assert_eq!(
        before,
        Ok(rows(&[
            &["1", "2026-01-01 10:00:00.123"],
            &["2", "2026-01-01 10:00:00.456"],
            &["3", "1969-12-31 23:59:59.999"],
        ])),
        "INT64 timestamps read as stored"
    );
    assert!(compacted.is_ok(), "{compacted:?}");
    assert_eq!(after, before, "OPTIMIZE changed the values");
}

/// An `INT64` wall-clock time the zone skips (2018-11-04 00:30 in
/// `America/Sao_Paulo`) has no `INT96` instant that reads back as it:
/// `OPTIMIZE` refuses the table and leaves its files as they were.
#[tokio::test]
async fn w1t_optimize_refuses_an_int64_time_the_zone_skips() {
    let l = Local::new().await;
    cl(&l.engine, "CREATE SCHEMA hive.s").await;
    cl(
        &l.engine,
        "CREATE TABLE hive.s.gap AS SELECT CAST(1 AS BIGINT) AS id, TIMESTAMP '2026-01-01 10:00:00.123' AS ts",
    )
    .await;
    put_parquet(
        &l.warehouse.join("s/gap/spark-00000.parquet"),
        &int64_timestamps(&[2], &[1_541_291_400_000]),
    );
    let q = "SELECT id, CAST(ts AS varchar) FROM hive.s.gap ORDER BY id";
    let before = cl_try(&l.engine, q).await;
    let files = data_files(&l.warehouse.join("s/gap"));
    let compacted = optimize(&l.engine, "hive.s.gap").await;
    let err = compacted.expect_err("OPTIMIZE must refuse a time the zone skips");
    assert!(
        err.contains("does not exist in time zone America/Sao_Paulo"),
        "{err}"
    );
    assert_eq!(data_files(&l.warehouse.join("s/gap")), files);
    assert_eq!(
        before,
        Ok(rows(&[
            &["1", "2026-01-01 10:00:00.123"],
            &["2", "2018-11-04 00:30:00.000"],
        ]))
    );
    assert_eq!(cl_try(&l.engine, q).await, before);
}

/// [`w1t_optimize_keeps_int64_timestamps_of_other_writers`] with Trino 431
/// as the oracle: it reads the `INT64` file unconverted, and must read the
/// same after `CodeLake` compacts the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1t_optimize_keeps_int64_timestamps_like_trino_431() {
    let s = Shared::new("codelake-w1t-trino-i64").await;
    let mut checks = Checks::new("w1t_optimize_int64_trino");
    s.trino.ok("CREATE SCHEMA hive.s").await;
    s.trino
        .ok("CREATE TABLE hive.s.i64 (id bigint, ts timestamp(3)) WITH (format = 'PARQUET')")
        .await;
    s.trino
        .ok("INSERT INTO hive.s.i64 VALUES (1, TIMESTAMP '2026-01-01 10:00:00.123')")
        .await;
    put_parquet(
        &s.warehouse.join("s/i64/spark-00000.parquet"),
        &int64_timestamps(&[2], &[TEN_O_CLOCK_456]),
    );
    let q = "SELECT id, CAST(ts AS varchar) FROM hive.s.i64 ORDER BY id";
    let trino_before = s.trino.sql(q).await;
    checks.same(
        "I64",
        "Trino before OPTIMIZE",
        &trino_before,
        &Ok(rows(&[
            &["1", "2026-01-01 10:00:00.123"],
            &["2", "2026-01-01 10:00:00.456"],
        ])),
    );
    checks.same(
        "I64",
        "CodeLake before OPTIMIZE",
        &cl_try(&s.engine, q).await,
        &trino_before,
    );
    let compacted = optimize(&s.engine, "hive.s.i64").await;
    note(checks.probe, "OPTIMIZE i64", &compacted);
    checks.same(
        "I64",
        "Trino after OPTIMIZE",
        &s.trino.sql(q).await,
        &trino_before,
    );
    checks.same(
        "I64",
        "CodeLake after OPTIMIZE",
        &cl_try(&s.engine, q).await,
        &trino_before,
    );
    checks.assert_none();
}

/// In-memory tables keep their rows inside the pool (W1-5): an `UPDATE` that
/// makes the rows larger must not take the table past the ceiling, and an
/// `UPDATE` answered with an error must not have changed the rows.
#[tokio::test]
async fn w1t_in_memory_update_never_escapes_the_pool() {
    let dir = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let limit = ByteSize::mib(48);
    let engine = engine_at(
        dir.path(),
        spill.path(),
        limit,
        HiveTimestampPrecision::Milliseconds,
    )
    .await;
    cl(
        &engine,
        "CREATE TABLE datafusion.public.grow AS SELECT CAST(value AS BIGINT) AS id, \
         array_join(repeat('x', 1000), '') AS s FROM generate_series(1, 4000)",
    )
    .await;
    let created = engine.memory().reserved_bytes;
    let mut steps = Vec::new();
    for _ in 0..5 {
        let updated = cl_try(&engine, "UPDATE datafusion.public.grow SET s = concat(s, s)").await;
        // The table's reservation once the statement answered.
        let reserved = engine.memory().reserved_bytes;
        steps.push((updated, reserved));
    }
    note(
        "w1t_in_memory_update",
        "updates (answer, reserved bytes), limit",
        (&steps, limit.as_u64()),
    );
    let mut wrong = Vec::new();
    let mut held = created;
    for (updated, reserved) in &steps {
        if *reserved > limit.as_u64() {
            wrong.push(format!(
                "{reserved} bytes reserved in a {} byte pool",
                limit.as_u64()
            ));
        }
        if updated.is_err() && *reserved != held {
            wrong.push(format!(
                "an UPDATE answered {updated:?} but the rows went from {held} to {reserved} bytes"
            ));
        }
        if let Err(err) = updated
            && !err.contains("Resources exhausted")
        {
            wrong.push(format!(
                "an UPDATE failed for another reason than the pool: {err}"
            ));
        }
        held = *reserved;
    }
    // Doubling 4000 rows of 1000 bytes five times outgrows 48 MiB: the probe
    // must see an UPDATE answer and one fail, or it proved nothing.
    if !steps.iter().any(|(updated, _)| updated.is_ok()) {
        wrong.push("no UPDATE answered".into());
    }
    if !steps.iter().any(|(updated, _)| updated.is_err()) {
        wrong.push("no UPDATE reached the pool's limit".into());
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The refusal of a comparison of char values (`codelake_core::char_comparisons`).
const CHAR_REFUSAL: &[&str] = &["comparing the char column ", " is not supported yet"];

/// Same rows as Trino, or `CodeLake`'s refusal: an error holding every one
/// of `refusal` (any other error is a difference).
fn same_or_refused(
    checks: &mut Checks,
    (id, what): (&str, &str),
    codelake: &Result<Rows, String>,
    trino: &Result<Rows, String>,
    refusal: &[&str],
) {
    match codelake {
        Err(err) if refusal.iter().all(|part| err.contains(part)) => {
            note(checks.probe, &format!("{id} {what} (refused)"), (codelake, trino));
        }
        _ => checks.same(id, what, codelake, trino),
    }
}

/// `char(n)` values compared where the W1-4 rule does not look: set
/// operations, `USING` joins and functions that compare, over tables Trino
/// 431 wrote. Each answer must be Trino's, or a refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1t_char_values_compared_outside_predicates_like_trino_431() {
    let s = Shared::new("codelake-w1t-trino-char").await;
    let mut checks = Checks::new("w1t_char_trino");
    s.trino.ok("CREATE SCHEMA hive.s").await;
    s.trino
        .ok("CREATE TABLE hive.s.cht (id bigint, ch char(5)) WITH (format = 'PARQUET')")
        .await;
    s.trino
        .ok("INSERT INTO hive.s.cht VALUES (1, 'ab'), (2, 'zz')")
        .await;
    s.trino
        .ok("CREATE TABLE hive.s.vct (id bigint, ch varchar) WITH (format = 'PARQUET')")
        .await;
    s.trino
        .ok("INSERT INTO hive.s.vct VALUES (1, 'ab'), (3, 'cd')")
        .await;
    for (id, q, refusal) in [
        (
            "CH1",
            "SELECT count(*) FROM hive.s.cht JOIN hive.s.vct USING (ch)",
            CHAR_REFUSAL,
        ),
        (
            "CH2",
            "SELECT count(*) FROM (SELECT ch FROM hive.s.cht INTERSECT SELECT ch FROM hive.s.vct)",
            CHAR_REFUSAL,
        ),
        (
            "CH3",
            "SELECT count(*) FROM (SELECT ch FROM hive.s.cht EXCEPT SELECT ch FROM hive.s.vct)",
            CHAR_REFUSAL,
        ),
        (
            "CH4",
            "SELECT count(*) FROM (SELECT ch FROM hive.s.cht UNION SELECT ch FROM hive.s.vct)",
            CHAR_REFUSAL,
        ),
        (
            "CH5",
            "SELECT id FROM hive.s.cht WHERE nullif(ch, 'ab') IS NULL ORDER BY 1",
            CHAR_REFUSAL,
        ),
        (
            "CH6",
            "SELECT count(DISTINCT c) FROM (SELECT ch AS c FROM hive.s.cht UNION ALL SELECT CAST('ab' AS char(5)))",
            CHAR_REFUSAL,
        ),
        (
            "CH7",
            "SELECT id FROM hive.s.cht WHERE (ch, id) = ('ab', 1) ORDER BY 1",
            CHAR_REFUSAL,
        ),
        (
            "CH8",
            "SELECT id FROM hive.s.cht WHERE ch = ANY (SELECT ch FROM hive.s.vct) ORDER BY 1",
            CHAR_REFUSAL,
        ),
        (
            "CH9",
            "SELECT count(*) FROM (SELECT 'ab ' UNION SELECT CAST('ab' AS char(5)))",
            CHAR_REFUSAL,
        ),
        ("CH10", "SELECT CAST('ab' AS char(5)) = 'ab '", CHAR_REFUSAL),
        (
            "CH11",
            "SELECT count(DISTINCT x) FROM (VALUES ('ab '), (CAST('ab' AS char(5)))) AS t(x)",
            CHAR_REFUSAL,
        ),
        (
            "CH12",
            "SELECT TRY_CAST('ab' AS char(5)) = 'ab '",
            &["TRY_CAST to char(5) is not supported yet"],
        ),
        (
            "CH13",
            "SELECT count(*) FROM (SELECT max(ch) FROM hive.s.cht UNION SELECT ch FROM hive.s.vct)",
            CHAR_REFUSAL,
        ),
        (
            "CH14",
            "SELECT count(*) FROM (SELECT coalesce(ch, 'x') FROM hive.s.cht UNION SELECT ch FROM hive.s.vct)",
            CHAR_REFUSAL,
        ),
        (
            "CH15",
            "SELECT id FROM (SELECT id, max(ch) AS m FROM hive.s.cht GROUP BY id) WHERE m = 'ab' ORDER BY 1",
            CHAR_REFUSAL,
        ),
        (
            "CH16",
            "SELECT id FROM hive.s.cht WHERE upper(ch) = 'AB ' ORDER BY 1",
            CHAR_REFUSAL,
        ),
    ] {
        let (codelake, trino) = (cl_try(&s.engine, q).await, s.trino.sql(q).await);
        same_or_refused(&mut checks, (id, q), &codelake, &trino, refusal);
    }
    checks.assert_none();
}

/// `parquet_time_zone` east of Greenwich (`Europe/Berlin`), with Trino 431
/// set to the same zone: a repeated time written by each engine is the same
/// instant, and a skipped time `CodeLake` wrote reads the same in both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn w1t_parquet_time_zone_east_of_greenwich_like_trino_431() {
    let s = Shared::new("codelake-w1t-trino-berlin").await;
    let mut checks = Checks::new("w1t_berlin");
    s.trino.ok("CREATE SCHEMA hive.s").await;
    let berlin = s
        .zoned(HiveTimestampPrecision::Milliseconds, "Europe/Berlin")
        .await;
    let utc = s.zoned(HiveTimestampPrecision::Milliseconds, "UTC").await;
    let repeated = "(1, TIMESTAMP '2024-10-27 02:30:00.000'), (2, TIMESTAMP '2024-07-01 12:00:00.000'), \
                    (3, TIMESTAMP '1890-01-01 00:00:00.000')";
    s.trino
        .ok(&parquet_ctas("hive_berlin.s.be_trino", repeated))
        .await;
    cl(&berlin, &ctas("hive.s.be_codelake", repeated)).await;
    checks.same(
        "BE1",
        "instants of Berlin wall-clock times: CodeLake / Trino",
        &cl_try(&utc, &read_zoned("hive.s.be_codelake")).await,
        &s.trino.sql(&read_zoned("hive_utc.s.be_trino")).await,
    );
    checks.same(
        "BE2",
        "Trino's Berlin table read in Berlin",
        &cl_try(&berlin, &read_zoned("hive.s.be_trino")).await,
        &s.trino.sql(&read_zoned("hive_berlin.s.be_trino")).await,
    );
    cl(
        &berlin,
        &ctas("hive.s.be_gap", "(1, TIMESTAMP '2024-03-31 02:30:00.000')"),
    )
    .await;
    checks.same(
        "BE3",
        "a skipped Berlin time CodeLake wrote, read in Berlin",
        &cl_try(&berlin, &read_zoned("hive.s.be_gap")).await,
        &s.trino.sql(&read_zoned("hive_berlin.s.be_gap")).await,
    );
    checks.same(
        "BE4",
        "its instant",
        &cl_try(&utc, &read_zoned("hive.s.be_gap")).await,
        &s.trino.sql(&read_zoned("hive_utc.s.be_gap")).await,
    );
    checks.same(
        "BE5",
        "the instants, read without the engines' help",
        &cl_try(&utc, &read_zoned("hive.s.be_codelake")).await,
        &Ok(rows(&[
            &["1", "2024-10-27 01:30:00.000"],
            &["2", "2024-07-01 10:00:00.000"],
            &["3", "1889-12-31 23:06:32.000"],
        ])),
    );
    checks.assert_none();
}
