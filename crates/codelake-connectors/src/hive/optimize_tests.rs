//! `OPTIMIZE` on a local warehouse, through the engine: rows, crash recovery,
//! concurrency and memory. Interoperability with a real Trino 431 is in
//! `tests/integration/hive_optimize.rs`.

use std::collections::BTreeMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Connector, Engine, QueryOptions};
use datafusion::arrow::util::pretty::pretty_format_batches;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};

use super::catalog::HiveCatalog;
use super::metastore::FileMetastore;
use super::optimize::{self, MARKER, Manifest, Removed};
use super::table::HiveTable;
use super::{HiveConnector, HiveParams};

const ENABLED: &str = "hive.non_transactional_optimize_enabled";

struct Lake {
    warehouse: tempfile::TempDir,
    spill: tempfile::TempDir,
    engine: Engine,
}

async fn engine_over(warehouse: &FsPath, spill: &FsPath, limit: ByteSize) -> Engine {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::mib(256),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    let connector: Arc<dyn Connector> = Arc::new(HiveConnector::new(
        "hive".into(),
        HiveParams {
            warehouse: warehouse.display().to_string(),
            s3_endpoint: None,
            s3_region: "us-east-1".into(),
            credentials: None,
            path_style: false,
        },
    ));
    engine
        .attach(connector, true, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    engine
}

async fn lake() -> Lake {
    lake_with_limit(ByteSize::mib(256)).await
}

async fn lake_with_limit(limit: ByteSize) -> Lake {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_over(warehouse.path(), spill.path(), limit).await;
    Lake {
        warehouse,
        spill,
        engine,
    }
}

impl Lake {
    async fn reopen(&mut self) {
        self.engine = engine_over(self.warehouse.path(), self.spill.path(), ByteSize::mib(256)).await;
    }

    fn table_dir(&self, table: &str) -> PathBuf {
        self.warehouse.path().join("s").join(table)
    }

    fn catalog(&self) -> HiveCatalog {
        let store = LocalFileSystem::new_with_prefix(self.warehouse.path()).unwrap();
        HiveCatalog::new("hive", FileMetastore::new(Arc::new(store), Path::default())).unwrap()
    }

    /// A table handle with its own locks, like another `CodeLake` process.
    async fn table(&self, table: &str) -> HiveTable {
        let catalog = self.catalog();
        let metadata = catalog.metastore().table("s", table).await.unwrap().unwrap();
        catalog.table_provider("s", table, &metadata).unwrap()
    }
}

fn options(properties: &[(&str, &str)], engine: &Engine) -> QueryOptions {
    let mut options = QueryOptions::for_user("dbt");
    let values: BTreeMap<String, String> = properties
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    options.properties = engine.session_properties().session(values).unwrap();
    options
}

async fn run(engine: &Engine, sql: &str) -> String {
    run_with(engine, &QueryOptions::for_user("dbt"), sql)
        .await
        .unwrap()
}

async fn run_with(engine: &Engine, options: &QueryOptions, sql: &str) -> datafusion::error::Result<String> {
    let batches = engine.sql_with(options, sql).await?.collect().await?;
    Ok(pretty_format_batches(&batches)?.to_string())
}

async fn optimize(engine: &Engine, sql: &str) -> datafusion::error::Result<String> {
    run_with(engine, &options(&[(ENABLED, "true")], engine), sql).await
}

/// Visible data files of a table directory, by name, with their size.
fn data_files(dir: &FsPath) -> BTreeMap<String, u64> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file())
        .filter(|e| !e.file_name().to_string_lossy().starts_with(['.', '_']))
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().unwrap().len(),
            )
        })
        .collect()
}

/// Hidden files other than metadata: temporaries, markers, checksums.
fn hidden_leftovers(dir: &FsPath) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| {
            n.starts_with('.') && n != ".trinoSchema" && n != "..trinoSchema.crc" && n != ".trinoPermissions"
        })
        .collect()
}

const ALL_ROWS: &str = "SELECT * FROM hive.s.t ORDER BY id";

/// `s.t` with five files: CTAS, three inserts (one with NULLs), then ADD
/// COLUMN and a fifth insert, so the first four files lack `origem`.
async fn five_files(lake: &Lake) -> String {
    let e = &lake.engine;
    run(e, "CREATE SCHEMA hive.s").await;
    run(
        e,
        "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS SELECT CAST(1 AS BIGINT) AS id, \
         'ação' AS nome, CAST(10.50 AS DECIMAL(18,2)) AS valor, X'0000000000000000002A' AS lsn, \
         CAST('2026-01-01 10:00:00.123' AS TIMESTAMP) AS ts, CAST('2026-01-02' AS DATE) AS d",
    )
    .await;
    run(
        e,
        "INSERT INTO hive.s.t VALUES (2, 'b', 1.00, X'00000000000000000030', \
         CAST('2026-01-03 00:00:00' AS TIMESTAMP), CAST('2026-01-03' AS DATE)), \
         (3, 'c', 2.25, X'00000000000000000031', CAST('2026-01-04 00:00:00' AS TIMESTAMP), CAST('2026-01-04' AS DATE))",
    )
    .await;
    run(e, "INSERT INTO hive.s.t VALUES (4, NULL, NULL, NULL, NULL, NULL)").await;
    run(
        e,
        "INSERT INTO hive.s.t SELECT 5 AS id, 'e' AS nome, CAST(5 AS DECIMAL(18,2)) AS valor, X'01' AS lsn, \
         CAST('2026-02-01 00:00:00' AS TIMESTAMP) AS ts, CAST('2026-02-01' AS DATE) AS d",
    )
    .await;
    run(e, "ALTER TABLE hive.s.t ADD COLUMN origem varchar").await;
    run(
        e,
        "INSERT INTO hive.s.t VALUES (6, 'f', 6.00, X'02', CAST('2026-02-02 00:00:00' AS TIMESTAMP), \
         CAST('2026-02-02' AS DATE), 'cdc')",
    )
    .await;
    assert_eq!(data_files(&lake.table_dir("t")).len(), 5);
    run(e, ALL_ROWS).await
}

#[tokio::test]
async fn compacts_every_small_file_into_one_and_keeps_rows_and_columns() {
    let lake = lake().await;
    let before = five_files(&lake).await;
    assert!(
        before.contains("| 1  | ação |") && before.contains("| cdc    |"),
        "{before}"
    );
    let old = data_files(&lake.table_dir("t"));

    optimize(&lake.engine, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();

    let after = data_files(&lake.table_dir("t"));
    assert_eq!(after.len(), 1, "{after:?}");
    assert!(after.keys().all(|name| !old.contains_key(name)));
    assert!(hidden_leftovers(&lake.table_dir("t")).is_empty());
    assert_eq!(run(&lake.engine, ALL_ROWS).await, before);
    // Files that predate ADD COLUMN still read NULL for it.
    let origem = run(
        &lake.engine,
        "SELECT id, origem FROM hive.s.t WHERE origem IS NULL ORDER BY id",
    )
    .await;
    assert_eq!(origem.lines().count(), 4 + 5, "{origem}");

    // Running again with a single file left changes nothing.
    optimize(&lake.engine, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(data_files(&lake.table_dir("t")), after);
}

#[tokio::test]
async fn needs_the_session_property_like_trino() {
    let lake = lake().await;
    let before = five_files(&lake).await;
    let files = data_files(&lake.table_dir("t"));
    let err = lake
        .engine
        .sql("ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(
            "OPTIMIZE procedure must be explicitly enabled via non_transactional_optimize_enabled session property"
        ),
        "{err}"
    );
    let disabled = options(&[(ENABLED, "false")], &lake.engine);
    assert!(
        run_with(&lake.engine, &disabled, "ALTER TABLE hive.s.t EXECUTE optimize")
            .await
            .is_err()
    );
    assert_eq!(data_files(&lake.table_dir("t")), files);
    assert_eq!(run(&lake.engine, ALL_ROWS).await, before);

    run(&lake.engine, "CREATE VIEW hive.s.v AS SELECT id FROM hive.s.t").await;
    let err = optimize(&lake.engine, "ALTER TABLE hive.s.v EXECUTE optimize")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not supported for views"), "{err}");
    let err = optimize(&lake.engine, "ALTER TABLE hive.s.nope EXECUTE optimize")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Table 'hive.s.nope' does not exist"),
        "{err}"
    );
}

#[tokio::test]
async fn files_above_the_threshold_are_left_alone() {
    let lake = lake().await;
    let e = &lake.engine;
    run(e, "CREATE SCHEMA hive.s").await;
    run(
        e,
        "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS \
         SELECT CAST(value AS BIGINT) AS id, array_join(repeat(CAST(value AS VARCHAR), 20), '') AS pad FROM range(0, 20000)",
    )
    .await;
    for id in [20_000, 20_001, 20_002] {
        run(e, &format!("INSERT INTO hive.s.t VALUES ({id}, 'x')")).await;
    }
    let files = data_files(&lake.table_dir("t"));
    let (big, big_size) = files.iter().max_by_key(|(_, size)| **size).unwrap();
    let small_max = files.values().filter(|s| *s != big_size).max().unwrap();
    assert!(big_size > small_max);
    let before = run(e, "SELECT count(*), sum(id), count(DISTINCT pad) FROM hive.s.t").await;

    // The threshold includes files of exactly that size, as in Trino.
    optimize(
        e,
        &format!("ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => '{small_max}B')"),
    )
    .await
    .unwrap();
    let after = data_files(&lake.table_dir("t"));
    assert_eq!(after.len(), 2, "{after:?}");
    assert_eq!(after.get(big), Some(big_size), "the large file is untouched");
    assert_eq!(
        run(e, "SELECT count(*), sum(id), count(DISTINCT pad) FROM hive.s.t").await,
        before
    );
}

#[tokio::test]
async fn output_rolls_over_at_the_target_size() {
    let lake = lake().await;
    let before = five_files(&lake).await;
    let small_target = options(
        &[(ENABLED, "true"), ("hive.target_max_file_size", "1B")],
        &lake.engine,
    );
    run_with(
        &lake.engine,
        &small_target,
        "ALTER TABLE hive.s.t EXECUTE optimize",
    )
    .await
    .unwrap();
    // One output file per input batch here: the size check runs per batch.
    assert!(data_files(&lake.table_dir("t")).len() > 1);
    assert_eq!(run(&lake.engine, ALL_ROWS).await, before);
}

#[tokio::test]
async fn trino_written_files_are_compacted_with_their_checksums() {
    let lake = lake().await;
    let fixture = FsPath::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse");
    copy_dir(&fixture, lake.warehouse.path());
    let mut lake = lake;
    lake.reopen().await;
    let sql = "SELECT id, nome, valor, lsn, ts, d, flag, dbl, i, vc FROM hive.pilot.t1 ORDER BY id";
    let before = run(&lake.engine, sql).await;
    assert!(before.contains("2026-01-01T10:00:00.123"), "{before}");
    optimize(&lake.engine, "ALTER TABLE hive.pilot.t1 EXECUTE optimize")
        .await
        .unwrap();
    let dir = lake.warehouse.path().join("pilot/t1");
    assert_eq!(data_files(&dir).len(), 1);
    assert!(hidden_leftovers(&dir).is_empty(), "{:?}", hidden_leftovers(&dir));
    assert_eq!(run(&lake.engine, sql).await, before);
}

fn copy_dir(from: &FsPath, to: &FsPath) {
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

/// Writes the compacted files of `s.t` but stops before the swap, as a crash
/// right after the write phase would. Returns the manifest the swap would use.
async fn prepared(lake: &Lake) -> (HiveTable, Manifest) {
    let table = lake.table("t").await;
    let candidates = table.data_files().await.unwrap();
    let context = lake.engine.context().task_ctx();
    let add = optimize::write_compacted(&table, &candidates, u64::MAX, &context)
        .await
        .unwrap();
    let manifest = Manifest {
        version: 1,
        id: "test".into(),
        remove: candidates
            .iter()
            .map(|m| Removed {
                name: m.location.filename().unwrap().to_owned(),
                size: m.size,
            })
            .collect(),
        add,
    };
    (table, manifest)
}

async fn write_marker(table: &HiveTable, manifest: &Manifest) {
    table
        .metastore
        .store()
        .put(
            &table.dir().join(MARKER),
            PutPayload::from(serde_json::to_vec(manifest).unwrap()),
        )
        .await
        .unwrap();
}

async fn rename(table: &HiveTable, from: &str, to: &str) {
    let store = table.metastore.store();
    store
        .rename(&table.dir().join(from), &table.dir().join(to))
        .await
        .unwrap();
}

async fn remove(table: &HiveTable, name: &str) {
    table
        .metastore
        .store()
        .delete(&table.dir().join(name))
        .await
        .unwrap();
}

/// Rows are counted, not only compared: a duplicated file doubles them.
async fn assert_recovers_to(lake: &mut Lake, before: &str, expected_files: usize) {
    lake.reopen().await;
    assert_eq!(run(&lake.engine, ALL_ROWS).await, before);
    assert_eq!(
        run(&lake.engine, "SELECT count(*) AS n FROM hive.s.t")
            .await
            .lines()
            .nth(3),
        Some("| 6 |")
    );
    assert_eq!(data_files(&lake.table_dir("t")).len(), expected_files);
    assert!(
        hidden_leftovers(&lake.table_dir("t")).is_empty(),
        "{:?}",
        hidden_leftovers(&lake.table_dir("t"))
    );
}

#[tokio::test]
async fn crash_before_the_marker_leaves_only_hidden_files() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let (_table, manifest) = prepared(&lake).await;
    assert_eq!(manifest.add.len(), 1);
    // The orphan temporary is invisible to readers.
    lake.reopen().await;
    assert_eq!(run(&lake.engine, ALL_ROWS).await, before);
    assert_eq!(data_files(&lake.table_dir("t")).len(), 5);
    std::fs::remove_file(lake.table_dir("t").join(&manifest.add[0].temporary)).unwrap();
    assert_recovers_to(&mut lake, &before, 5).await;
}

#[tokio::test]
async fn crash_after_the_marker_is_finished_on_restart() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    write_marker(&table, &manifest).await;
    assert_recovers_to(&mut lake, &before, 1).await;
}

#[tokio::test]
async fn crash_while_renaming_is_finished_on_restart() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let table = lake.table("t").await;
    let candidates = table.data_files().await.unwrap();
    let context = lake.engine.context().task_ctx();
    // A 1-byte target writes one file per input file: several renames.
    let add = optimize::write_compacted(&table, &candidates, 1, &context)
        .await
        .unwrap();
    assert!(add.len() > 1);
    let manifest = Manifest {
        version: 1,
        id: "test".into(),
        remove: candidates
            .iter()
            .map(|m| Removed {
                name: m.location.filename().unwrap().to_owned(),
                size: m.size,
            })
            .collect(),
        add,
    };
    write_marker(&table, &manifest).await;
    rename(&table, &manifest.add[0].temporary, &manifest.add[0].name).await;
    // Mid-swap, old and new rows are both visible: this is the window
    // recovery closes.
    let count = manifest.add.len();
    assert_recovers_to(&mut lake, &before, count).await;
}

#[tokio::test]
async fn crash_while_deleting_is_finished_on_restart() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    write_marker(&table, &manifest).await;
    rename(&table, &manifest.add[0].temporary, &manifest.add[0].name).await;
    remove(&table, &manifest.remove[0].name).await;
    remove(&table, &manifest.remove[1].name).await;
    assert_recovers_to(&mut lake, &before, 1).await;
}

#[tokio::test]
async fn a_lost_new_file_rolls_back_to_the_old_files() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    write_marker(&table, &manifest).await;
    remove(&table, &manifest.add[0].temporary).await;
    assert_recovers_to(&mut lake, &before, 5).await;
}

#[tokio::test]
async fn an_inconsistent_marker_stops_instead_of_guessing() {
    let lake = lake().await;
    five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    write_marker(&table, &manifest).await;
    remove(&table, &manifest.add[0].temporary).await;
    remove(&table, &manifest.remove[0].name).await;
    let err = optimize(&lake.engine, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("lists files that are gone"), "{err}");
    assert!(lake.table_dir("t").join(MARKER).exists());

    std::fs::write(lake.table_dir("t").join(MARKER), b"{not json").unwrap();
    let err = optimize(&lake.engine, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("is unreadable"), "{err}");
}

#[tokio::test]
async fn an_insert_during_optimize_is_kept() {
    let mut lake = lake().await;
    five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    run(
        &lake.engine,
        "INSERT INTO hive.s.t VALUES (7, 'g', 7.00, X'03', CAST('2026-03-01 00:00:00' AS TIMESTAMP), \
         CAST('2026-03-01' AS DATE), 'late')",
    )
    .await;
    let expected = run(&lake.engine, ALL_ROWS).await;
    optimize::commit(&table, &manifest).await.unwrap();
    lake.reopen().await;
    assert_eq!(run(&lake.engine, ALL_ROWS).await, expected);
    assert!(expected.contains("| late"), "{expected}");
    assert_eq!(data_files(&lake.table_dir("t")).len(), 2);
}

#[tokio::test]
async fn a_file_compacted_elsewhere_meanwhile_aborts_the_swap() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    // Another process compacted the same files first.
    optimize(&lake.engine, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    let err = optimize::commit(&table, &manifest).await.unwrap_err();
    assert!(err.to_string().contains("changed while OPTIMIZE ran"), "{err}");
    assert_recovers_to(&mut lake, &before, 1).await;
}

#[tokio::test]
async fn a_second_swap_waits_for_the_first_marker() {
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let (table, manifest) = prepared(&lake).await;
    let other = Manifest {
        id: "other".into(),
        add: Vec::new(),
        remove: Vec::new(),
        ..manifest.clone()
    };
    write_marker(&table, &other).await;
    let err = optimize::commit(&table, &manifest).await.unwrap_err();
    assert!(err.to_string().contains("another OPTIMIZE"), "{err}");
    std::fs::remove_file(lake.table_dir("t").join(MARKER)).unwrap();
    assert_recovers_to(&mut lake, &before, 5).await;
}

/// The one failure ADR 0009 allows a read racing a swap ("loud, not
/// wrong"): planned before the swap, it opens a data file the swap removed.
/// The error must name one of the files the table had before `OPTIMIZE`.
fn is_missing_compacted_file(
    err: &datafusion::error::DataFusionError,
    compacted: &BTreeMap<String, u64>,
) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = source {
        if let Some(object_store::Error::NotFound { path, .. }) = e.downcast_ref::<object_store::Error>() {
            return compacted.keys().any(|name| path.ends_with(&format!("/{name}")));
        }
        source = e.source();
    }
    false
}

/// Reads racing two `OPTIMIZE`s of the same table (ADR 0009): each read sees
/// every row exactly once, or, planned before a swap, fails because a file it
/// planned to read was compacted away. Any other error fails the test, as
/// does any error of a read started after both `OPTIMIZE`s ended.
#[tokio::test]
async fn concurrent_optimize_and_reads_never_duplicate_rows() {
    /// Successful reads required, so the test never passes on errors alone.
    const MIN_OK: usize = 10;
    let mut lake = lake().await;
    let before = five_files(&lake).await;
    let compacted = data_files(&lake.table_dir("t"));
    let sql = "ALTER TABLE hive.s.t EXECUTE optimize";
    let optimizing = std::cell::Cell::new(true);
    let optimizes = async {
        let both = tokio::join!(optimize(&lake.engine, sql), optimize(&lake.engine, sql));
        optimizing.set(false);
        both
    };
    let reads = async {
        let (mut ok, mut racing_ok, mut missing) = (0_usize, 0_usize, 0_usize);
        while optimizing.get() || ok < MIN_OK {
            let racing = optimizing.get();
            match run_with(&lake.engine, &QueryOptions::for_user("dbt"), ALL_ROWS).await {
                Ok(rows) => {
                    // Every row once: none duplicated, none lost.
                    assert_eq!(rows, before, "read racing OPTIMIZE: {racing}");
                    ok += 1;
                    racing_ok += usize::from(racing);
                }
                Err(err) if racing && is_missing_compacted_file(&err, &compacted) => missing += 1,
                Err(err) => panic!("read failed (racing OPTIMIZE: {racing}): {err:?}"),
            }
        }
        (ok, racing_ok, missing)
    };
    let ((a, b), (ok, racing_ok, missing)) = tokio::join!(optimizes, reads);
    eprintln!("reads: {ok} ok ({racing_ok} while OPTIMIZE ran), {missing} failed on a compacted file");
    a.unwrap();
    b.unwrap();
    assert!(ok >= MIN_OK, "{ok} successful reads");
    assert!(
        racing_ok + missing >= 1,
        "no read ran while OPTIMIZE did: the race was not exercised"
    );
    assert_recovers_to(&mut lake, &before, 1).await;
}

#[tokio::test]
async fn compaction_reserves_memory_and_fails_cleanly_when_it_is_short() {
    let lake = lake().await;
    let e = &lake.engine;
    run(e, "CREATE SCHEMA hive.s").await;
    // Incompressible text: the writer buffers about as much as it writes.
    let rows = "SELECT CAST(value AS BIGINT) AS id, sha256(to_utf8(CAST(value AS VARCHAR))) AS h, \
                sha256(to_utf8(CAST(value + 1 AS VARCHAR))) AS h2 FROM range(0, 20000)";
    run(
        e,
        &format!("CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS {rows}"),
    )
    .await;
    run(e, &format!("INSERT INTO hive.s.t {rows}")).await;
    let before = run(e, "SELECT count(*), sum(id) FROM hive.s.t").await;
    let files = data_files(&lake.table_dir("t"));

    let small = engine_over(lake.warehouse.path(), lake.spill.path(), ByteSize::mib(1)).await;
    let err = optimize(&small, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Resources exhausted"), "{err}");
    assert!(err.to_string().contains("HiveOptimize[t]"), "{err}");
    assert_eq!(data_files(&lake.table_dir("t")), files);
    assert!(hidden_leftovers(&lake.table_dir("t")).is_empty());
    assert_eq!(run(e, "SELECT count(*), sum(id) FROM hive.s.t").await, before);

    // With room in the pool the same files compact.
    optimize(e, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    assert_eq!(data_files(&lake.table_dir("t")).len(), 1);
    assert_eq!(run(e, "SELECT count(*), sum(id) FROM hive.s.t").await, before);
}

/// A Trino catalog at `hive.timestamp-precision=MICROSECONDS` writes INT96
/// microseconds; scans read milliseconds, but compaction must rewrite the
/// stored values exactly.
#[tokio::test]
async fn microsecond_timestamps_are_rewritten_exactly() {
    use datafusion::arrow::array::{Array, AsArray, Int64Array, RecordBatch, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampNanosecondType};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::trino_parquet::TrinoParquetWriter;

    let lake = lake().await;
    let e = &lake.engine;
    run(e, "CREATE SCHEMA hive.s").await;
    run(
        e,
        "CREATE TABLE hive.s.t WITH (format = 'PARQUET') AS SELECT CAST(0 AS BIGINT) AS id, \
         CAST('2026-01-01 00:00:00' AS TIMESTAMP) AS ts",
    )
    .await;
    let dir = lake.table_dir("t");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));
    // 2026-01-01 10:00:00.123999, 1700-06-01 00:00:00.000001, 2026-01-01 10:00:00.000500
    let values = [
        (1_i64, 1_767_261_600_123_999_i64),
        (2, -8_507_289_599_999_999),
        (3, 1_767_261_600_000_500),
    ];
    for (id, micros) in values {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(TimestampMicrosecondArray::from(vec![micros])),
            ],
        )
        .unwrap();
        let mut buffer = Vec::new();
        let mut writer = TrinoParquetWriter::try_new(&mut buffer, Arc::clone(&schema), 1024).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
        std::fs::write(dir.join(format!("20200101_000000_{id}_micro")), buffer).unwrap();
    }
    optimize(e, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    let files = data_files(&dir);
    assert_eq!(files.len(), 1, "{files:?}");
    let file = std::fs::File::open(dir.join(files.keys().next().unwrap())).unwrap();
    let mut stored: Vec<(i64, i64)> = Vec::new();
    for batch in ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
    {
        let batch = batch.unwrap();
        let ids = batch
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        // INT96 without a hint reads in nanoseconds: every digit is there.
        let ts = batch.column(1).as_primitive::<TimestampNanosecondType>();
        for row in 0..batch.num_rows() {
            assert!(ts.is_valid(row));
            stored.push((ids.value(row), ts.value(row)));
        }
    }
    stored.sort_unstable();
    let mut expected: Vec<(i64, i64)> = values.iter().map(|(id, micros)| (*id, micros * 1_000)).collect();
    expected.insert(0, (0, 1_767_225_600_000_000_000));
    assert_eq!(stored, expected);
}
