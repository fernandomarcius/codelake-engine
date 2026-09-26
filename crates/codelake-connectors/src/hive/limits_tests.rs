//! Limitations copied from the reference engine and removed (ISS-017,
//! ISS-018, ISS-023): each test states what the reference engine does and
//! what `CodeLake` does instead, and checks the rows.

use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::Duration;

use codelake_core::config::HiveTimestampPrecision;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use object_store::path::Path;

use super::f2_tests::{Lake, Rows, engine_over, lake, rows, run_on};
use super::metastore::{ColumnMetadata, TABLE_ID, TableMetadata};

fn int_batch(name: &str, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
    )
    .unwrap()
}

/// Every file under `dir` with its contents, sorted by relative path.
fn snapshot(dir: &FsPath) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &FsPath, dir: &FsPath, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let relative = path.strip_prefix(root).unwrap().display().to_string();
                out.push((relative, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

fn names(files: &[(String, Vec<u8>)]) -> Vec<&str> {
    files.iter().map(|(name, _)| name.as_str()).collect()
}

// ---------------------------------------------------------------------------
// ISS-023: a view owns no files
// ---------------------------------------------------------------------------

/// Trino 431 lets `CREATE VIEW` take a directory that already holds another
/// writer's files and deletes them all on `DROP VIEW`. `CodeLake` removes
/// only the view's metadata: every foreign file stays, byte for byte.
#[tokio::test]
async fn drop_view_over_a_foreign_directory_keeps_its_files() {
    let lake = lake().await;
    std::fs::create_dir_all(lake.dir("v")).unwrap();
    lake.put_file("v", "part-00000.parquet", &int_batch("x", vec![7]));
    std::fs::write(lake.dir("v").join("notes.txt"), b"someone else's").unwrap();
    let foreign = snapshot(&lake.dir("v"));

    lake.ok("CREATE VIEW hive.s.v AS SELECT 1 AS a").await;
    assert_eq!(lake.ok("SELECT a FROM hive.s.v").await, rows(&[&["1"]]));
    lake.ok("DROP VIEW hive.s.v").await;

    assert!(lake.err("SELECT a FROM hive.s.v").await.contains("s.v"));
    assert_eq!(snapshot(&lake.dir("v")), foreign, "foreign files changed");
    // The directory still is not a table's: CREATE TABLE refuses it.
    let err = lake.err("CREATE TABLE hive.s.v AS SELECT 1 AS a").await;
    assert!(err.contains("already exists"), "{err}");
    assert_eq!(snapshot(&lake.dir("v")), foreign);
}

/// A view in a directory of its own leaves nothing behind: the name can be
/// taken again by a table.
#[tokio::test]
async fn drop_view_removes_its_own_directory() {
    let lake = lake().await;
    lake.ok("CREATE VIEW hive.s.v AS SELECT 1 AS a").await;
    assert!(lake.dir("v").join(".trinoSchema").exists());
    lake.ok("DROP VIEW hive.s.v").await;
    assert!(!lake.dir("v").exists());
    lake.ok("CREATE TABLE hive.s.v AS SELECT CAST(2 AS BIGINT) AS a")
        .await;
    assert_eq!(lake.ok("SELECT a FROM hive.s.v").await, rows(&[&["2"]]));
}

/// `CREATE OR REPLACE VIEW` and `ALTER VIEW … RENAME` over a foreign
/// directory move and rewrite only the view's metadata.
#[tokio::test]
async fn view_rename_and_replace_never_move_foreign_files() {
    let lake = lake().await;
    std::fs::create_dir_all(lake.dir("v")).unwrap();
    lake.put_file("v", "part-00000.parquet", &int_batch("x", vec![7]));
    let foreign = snapshot(&lake.dir("v"));

    lake.ok("CREATE VIEW hive.s.v AS SELECT 1 AS a").await;
    lake.ok("CREATE OR REPLACE VIEW hive.s.v AS SELECT 2 AS a").await;
    assert_eq!(lake.ok("SELECT a FROM hive.s.v").await, rows(&[&["2"]]));
    lake.ok("ALTER VIEW hive.s.v RENAME TO hive.s.w").await;

    assert_eq!(lake.ok("SELECT a FROM hive.s.w").await, rows(&[&["2"]]));
    assert!(lake.err("SELECT a FROM hive.s.v").await.contains("s.v"));
    assert_eq!(snapshot(&lake.dir("v")), foreign, "foreign files moved");
    let moved = snapshot(&lake.dir("w"));
    assert_eq!(
        names(&moved),
        vec![
            "..trinoSchema.crc",
            ".trinoPermissions/.user_codelake.crc",
            ".trinoPermissions/user_codelake",
            ".trinoSchema",
        ]
    );
    lake.ok("DROP VIEW hive.s.w").await;
    assert!(!lake.dir("w").exists());
    assert_eq!(snapshot(&lake.dir("v")), foreign);
}

/// A view in a directory of its own is renamed whole: nothing is left
/// under the old name.
#[tokio::test]
async fn renaming_a_view_leaves_nothing_under_the_old_name() {
    let lake = lake().await;
    lake.ok("CREATE VIEW hive.s.v AS SELECT 1 AS a").await;
    lake.ok("ALTER VIEW hive.s.v RENAME TO hive.s.w").await;
    assert!(!lake.dir("v").exists());
    assert_eq!(lake.ok("SELECT a FROM hive.s.w").await, rows(&[&["1"]]));
    lake.ok("CREATE TABLE hive.s.v AS SELECT CAST(3 AS BIGINT) AS a")
        .await;
    assert_eq!(lake.ok("SELECT a FROM hive.s.v").await, rows(&[&["3"]]));
}

// ---------------------------------------------------------------------------
// ISS-017: CREATE OR REPLACE TABLE, atomic
// ---------------------------------------------------------------------------

/// Entries of `dir` (not recursive), sorted; empty when it does not exist.
fn entries(dir: &FsPath) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn staging(lake: &Lake) -> Vec<String> {
    entries(&lake.warehouse.path().join(".codelake/staging"))
}

/// Visible data files of a table directory.
fn data_files(lake: &Lake, table: &str) -> Vec<String> {
    entries(&lake.dir(table))
        .into_iter()
        .filter(|name| !name.starts_with(['.', '_']))
        .collect()
}

fn triangle(n: i64) -> String {
    (n * (n + 1) / 2).to_string()
}

/// Trino 431's hive connector refuses `CREATE OR REPLACE TABLE` ("This
/// connector does not support replacing tables"). `CodeLake` replaces the
/// table: the new rows and columns, the old files gone, nothing left in
/// staging, and the table can read itself while it is replaced.
#[tokio::test]
async fn or_replace_replaces_the_table_and_its_files() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS x FROM (VALUES (1), (2)) v(x)")
        .await;
    lake.ok("INSERT INTO hive.s.t VALUES (3)").await;
    let old_files = data_files(&lake, "t");
    assert_eq!(old_files.len(), 2);

    let written = lake
        .ok("CREATE OR REPLACE TABLE hive.s.t AS SELECT x * 10 AS y, 'a' AS z FROM hive.s.t")
        .await;
    assert_eq!(written, rows(&[&["3"]]));
    assert_eq!(
        lake.ok("SELECT y, z FROM hive.s.t ORDER BY y").await,
        rows(&[&["10", "a"], &["20", "a"], &["30", "a"]])
    );
    assert_eq!(
        lake.columns("t"),
        vec![("y".into(), "bigint".into()), ("z".into(), "varchar(1)".into())]
    );
    assert!(lake.err("SELECT x FROM hive.s.t").await.contains('x'));
    let new_files = data_files(&lake, "t");
    assert_eq!(new_files.len(), 1, "{new_files:?}");
    assert!(old_files.iter().all(|f| !new_files.contains(f)));
    assert!(staging(&lake).is_empty(), "{:?}", staging(&lake));
    // A second replacement, of a table a replacement wrote.
    lake.ok("CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(7 AS BIGINT) AS w")
        .await;
    assert_eq!(lake.ok("SELECT w FROM hive.s.t").await, rows(&[&["7"]]));
    assert!(staging(&lake).is_empty());
    // Every replacement has its own identity.
    let metadata = lake.metastore().table("s", "t").await.unwrap().unwrap();
    assert!(metadata.parameters.get(TABLE_ID).is_some_and(|id| id.len() == 32));
}

/// `CREATE OR REPLACE` of a free name is a `CREATE TABLE`; `CREATE OR
/// REPLACE TABLE … (columns)` replaces with an empty table.
#[tokio::test]
async fn or_replace_of_a_free_name_creates_the_table() {
    let lake = lake().await;
    lake.ok("CREATE OR REPLACE TABLE hive.s.fresh AS SELECT CAST(1 AS BIGINT) AS a")
        .await;
    assert_eq!(lake.ok("SELECT a FROM hive.s.fresh").await, rows(&[&["1"]]));
    lake.ok("CREATE OR REPLACE TABLE hive.s.fresh (b varchar(3))")
        .await;
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.fresh").await,
        rows(&[&["0"]])
    );
    assert_eq!(lake.columns("fresh"), vec![("b".into(), "varchar(3)".into())]);
    assert!(data_files(&lake, "fresh").is_empty());
    let err = lake
        .err("CREATE OR REPLACE TABLE IF NOT EXISTS hive.s.fresh AS SELECT 1 AS a")
        .await;
    assert!(!err.is_empty());
}

/// What `CREATE OR REPLACE` never takes: a view (the view stays), a
/// directory without a table (its files stay: red-team W1-1), a table whose
/// data is not its own (`EXTERNAL_TABLE`: its data and metadata stay).
#[tokio::test]
async fn or_replace_never_takes_a_view_a_foreign_directory_or_external_data() {
    let lake = lake().await;
    lake.ok("CREATE VIEW hive.s.v AS SELECT 1 AS a").await;
    let err = lake
        .err("CREATE OR REPLACE TABLE hive.s.v AS SELECT CAST(2 AS BIGINT) AS a")
        .await;
    assert!(err.contains("is a view"), "{err}");
    assert_eq!(lake.ok("SELECT a FROM hive.s.v").await, rows(&[&["1"]]));

    std::fs::create_dir_all(lake.dir("foreign")).unwrap();
    lake.put_file("foreign", "part-00000.parquet", &int_batch("v", vec![42]));
    let foreign = snapshot(&lake.dir("foreign"));
    let err = lake
        .err("CREATE OR REPLACE TABLE hive.s.foreign AS SELECT CAST(1 AS BIGINT) AS v")
        .await;
    assert!(err.contains("already exists"), "{err}");
    assert_eq!(snapshot(&lake.dir("foreign")), foreign);

    lake.ok("CREATE TABLE hive.s.ext AS SELECT CAST(5 AS BIGINT) AS v")
        .await;
    let elsewhere = tempfile::tempdir().unwrap();
    let data = std::fs::File::create(elsewhere.path().join("data")).unwrap();
    let mut writer = ArrowWriter::try_new(data, int_batch("v", vec![9]).schema(), None).unwrap();
    writer.write(&int_batch("v", vec![9])).unwrap();
    writer.close().unwrap();
    let metastore = lake.metastore();
    let mut external = metastore.table("s", "ext").await.unwrap().unwrap();
    external.table_type = "EXTERNAL_TABLE".into();
    external.external_location = Some(format!("file:{}", elsewhere.path().display()));
    metastore.write_table("s", "ext", &external).await.unwrap();
    let before = (snapshot(&lake.dir("ext")), snapshot(elsewhere.path()));
    let err = lake
        .err("CREATE OR REPLACE TABLE hive.s.ext AS SELECT CAST(1 AS BIGINT) AS v")
        .await;
    assert!(err.contains("replaces only managed tables"), "{err}");
    assert_eq!((snapshot(&lake.dir("ext")), snapshot(elsewhere.path())), before);
    assert!(staging(&lake).is_empty());
}

/// A replacement whose query fails leaves the old table, rows and files.
#[tokio::test]
async fn a_failed_or_replace_keeps_the_old_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS x FROM (VALUES (7), (8)) v(x)")
        .await;
    let before = snapshot(&lake.dir("t"));
    let err = lake
        .err("CREATE OR REPLACE TABLE hive.s.t AS SELECT 1 / (x - x) AS y FROM hive.s.t")
        .await;
    assert!(err.to_lowercase().contains("divi"), "{err}");
    assert_eq!(snapshot(&lake.dir("t")), before);
    assert_eq!(
        lake.ok("SELECT x FROM hive.s.t ORDER BY x").await,
        rows(&[&["7"], &["8"]])
    );
    assert!(staging(&lake).is_empty());
}

/// A process that dies while the new table is being written (the
/// statement's future dropped mid-write, before any cleanup) leaves the old
/// table as it was; what it wrote stays hidden in staging, and the next
/// replacement works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_while_writing_the_new_table_keeps_the_old_one() {
    let lake = Arc::new(lake().await);
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, 10)")
        .await;
    let before = snapshot(&lake.dir("t"));
    let writer = Arc::clone(&lake);
    let replace = tokio::spawn(async move {
        writer
            .run(
                "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v, \
                 rpad(CAST(value AS varchar), 200, 'x') AS pad FROM generate_series(1, 200000000)",
            )
            .await
    });
    // Until the new table has started to land in staging.
    let started = async {
        loop {
            if replace.is_finished() {
                return false;
            }
            let written = staging(&lake)
                .into_iter()
                .any(|id| !entries(&lake.warehouse.path().join(".codelake/staging").join(id)).is_empty());
            if written {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let started = tokio::time::timeout(Duration::from_secs(60), started)
        .await
        .expect("the replacement never started writing");
    assert!(
        started,
        "the replacement ended before the crash: {:?}",
        replace.await
    );
    replace.abort();
    assert!(replace.await.unwrap_err().is_cancelled());

    assert_eq!(snapshot(&lake.dir("t")), before);
    assert_eq!(
        lake.ok("SELECT count(*), sum(v) FROM hive.s.t").await,
        rows(&[&["10", "55"]])
    );
    assert!(!staging(&lake).is_empty(), "the crash leaves its staging");
    lake.ok("CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(3 AS BIGINT) AS v")
        .await;
    assert_eq!(lake.ok("SELECT v FROM hive.s.t").await, rows(&[&["3"]]));
}

/// Stages the table `v` = `values` in a new staging directory, as a
/// replacement does before its swap.
async fn staged_table(lake: &Lake, values: Vec<i64>) -> super::metastore::Staging {
    let metastore = lake.metastore();
    let staging = metastore.staging();
    let dir = lake.warehouse.path().join(staging.dir.as_ref());
    std::fs::create_dir_all(&dir).unwrap();
    let batch = int_batch("v", values);
    let file = std::fs::File::create(dir.join("20260926_000000_00000_codelake_staged")).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut metadata = TableMetadata::managed_parquet(
        "codelake",
        vec![ColumnMetadata {
            name: "v".into(),
            hive_type: "bigint".into(),
            comment: None,
            properties: std::collections::BTreeMap::new(),
            extra: std::collections::BTreeMap::new(),
        }],
    );
    metadata.parameters.insert(TABLE_ID.into(), "staged".into());
    metastore
        .stage_table(&staging, &metadata, "codelake")
        .await
        .unwrap();
    staging
}

/// A new process over the same warehouse (catalog opened again).
async fn reopened(lake: &Lake, sql: &str) -> Result<Rows, String> {
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_over(
        lake.warehouse.path(),
        spill.path(),
        HiveTimestampPrecision::Milliseconds,
    )
    .await;
    run_on(&engine, sql).await
}

/// The crash points around the swap: staged but not swapped (the old table,
/// here and after a restart), swapped but not cleaned up (the new table,
/// here and after a restart; the old one only in hidden staging).
#[tokio::test]
async fn a_crash_around_the_swap_leaves_the_old_or_the_new_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS v FROM (VALUES (1), (2)) v(x)")
        .await;
    let old = rows(&[&["2", "3"]]);
    let query = "SELECT count(*), sum(v) FROM hive.s.t";

    let staged = staged_table(&lake, vec![10, 20, 30]).await;
    assert_eq!(lake.ok(query).await, old, "staged, not swapped");
    assert_eq!(reopened(&lake, query).await, Ok(old.clone()));

    lake.metastore().replace_table(&staged, "s", "t").await.unwrap();
    let new = rows(&[&["3", "60"]]);
    assert_eq!(lake.ok(query).await, new, "swapped, not cleaned up");
    assert_eq!(reopened(&lake, query).await, Ok(new.clone()));
    let hidden = lake.warehouse.path().join(staged.dir.as_ref());
    assert!(
        hidden.join(".trinoSchema").exists(),
        "the old table waits in staging"
    );
    assert_eq!(
        lake.ok("SELECT table_name FROM hive.information_schema.tables WHERE table_schema = 's'")
            .await,
        rows(&[&["t"]])
    );
    lake.metastore().discard(&staged).await;
    assert_eq!(lake.ok(query).await, new);
    assert!(!hidden.exists());
}

/// A query or an `INSERT` planned against the table that a replacement then
/// swapped out fails; it never reads or writes the new table.
#[tokio::test]
async fn statements_planned_before_a_replace_never_touch_the_new_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, 10)")
        .await;
    let select = lake
        .engine
        .sql("SELECT count(*), sum(v) FROM hive.s.t")
        .await
        .unwrap();
    let insert = lake.engine.sql("INSERT INTO hive.s.t VALUES (99)").await.unwrap();
    lake.ok("CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(5 AS BIGINT) AS v")
        .await;
    let err = select.collect().await.unwrap_err().to_string();
    assert!(err.contains("was replaced"), "{err}");
    let err = insert.collect().await.unwrap_err().to_string();
    assert!(err.contains("was replaced while the INSERT ran"), "{err}");
    assert_eq!(lake.ok("SELECT v FROM hive.s.t").await, rows(&[&["5"]]));
    // Planned after the replacement: the new table.
    lake.ok("INSERT INTO hive.s.t VALUES (6)").await;
    assert_eq!(
        lake.ok("SELECT v FROM hive.s.t ORDER BY v").await,
        rows(&[&["5"], &["6"]])
    );
}

/// Readers in this process and in another one, racing replacements of the
/// table by tables of other sizes: every answer is the count and sum of one
/// whole version, never a mix and never an empty table. A reader that
/// listed the old files just before a swap fails loudly (the file is gone).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn readers_racing_or_replace_see_the_old_or_the_new_table() {
    let lake = Arc::new(lake().await);
    let versions: Vec<i64> = (1..=12).map(|k| 1_000 * k).collect();
    lake.ok(&format!(
        "CREATE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, {})",
        versions[0]
    ))
    .await;
    let valid: Vec<Rows> = versions
        .iter()
        .map(|n| vec![vec![n.to_string(), triangle(*n)]])
        .collect();
    let spill = tempfile::tempdir().unwrap();
    let other = Arc::new(
        engine_over(
            lake.warehouse.path(),
            spill.path(),
            HiveTimestampPrecision::Milliseconds,
        )
        .await,
    );
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for reader in 0..4 {
        let (lake, other, done) = (Arc::clone(&lake), Arc::clone(&other), Arc::clone(&done));
        readers.push(tokio::spawn(async move {
            let mut answers = Vec::new();
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                let sql = "SELECT count(*), CAST(sum(v) AS varchar) FROM hive.s.t";
                let answer = if reader % 2 == 0 {
                    lake.run(sql).await
                } else {
                    run_on(&other, sql).await
                };
                answers.push(answer);
            }
            answers
        }));
    }
    for n in &versions[1..] {
        lake.ok(&format!(
            "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v \
             FROM generate_series(1, {n})"
        ))
        .await;
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut ok = 0;
    for reader in readers {
        for answer in reader.await.unwrap() {
            match answer {
                Ok(rows) => {
                    assert!(valid.contains(&rows), "a mix of versions: {rows:?}");
                    ok += 1;
                }
                Err(err) => assert!(
                    err.contains("was replaced") || err.contains("not found") || err.contains("No such file"),
                    "unexpected failure: {err}"
                ),
            }
        }
    }
    assert!(ok > 0);
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.t").await,
        rows(&[&[&versions[versions.len() - 1].to_string()]])
    );
    assert!(staging(&lake).is_empty(), "{:?}", staging(&lake));
}

/// Two replacements of one table from two processes at once: the table is
/// one of them, whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_replacements_leave_one_whole_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(0 AS BIGINT) AS v")
        .await;
    let spill = tempfile::tempdir().unwrap();
    let other = engine_over(
        lake.warehouse.path(),
        spill.path(),
        HiveTimestampPrecision::Milliseconds,
    )
    .await;
    for round in 0..5 {
        let (a, b) = tokio::join!(
            lake.run("CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, 3000)"),
            run_on(
                &other,
                "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) * 2 AS v FROM generate_series(1, 5000)"
            ),
        );
        a.unwrap();
        b.unwrap();
        let got = lake
            .ok("SELECT count(*), CAST(sum(v) AS varchar) FROM hive.s.t")
            .await;
        assert!(
            got == rows(&[&["3000", &triangle(3000)]])
                || got == rows(&[&["5000", &(2 * 12_502_500).to_string()]]),
            "round {round}: {got:?}"
        );
        assert_eq!(data_files(&lake, "t").len(), 1);
    }
    assert!(staging(&lake).is_empty());
}

#[test]
fn staging_paths_are_object_store_paths() {
    // The helper above joins the staging path onto the warehouse directory.
    let path = Path::from(".codelake/staging/abc");
    assert_eq!(path.as_ref(), ".codelake/staging/abc");
}

// ---------------------------------------------------------------------------
// ISS-018: names keep the case they were written with
// ---------------------------------------------------------------------------

/// Each column's `codelake.name` property (the name as written).
async fn written_names(lake: &Lake, table: &str) -> Vec<Option<String>> {
    let metadata = lake.metastore().table("s", table).await.unwrap().unwrap();
    metadata
        .data_columns
        .iter()
        .map(|c| c.properties.get("codelake.name").cloned())
        .collect()
}

/// Column names of every visible data file of a table, in file-name order.
fn file_columns(lake: &Lake, table: &str) -> Vec<Vec<String>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    data_files(lake, table)
        .into_iter()
        .map(|name| {
            let file = std::fs::File::open(lake.dir(table).join(name)).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            builder
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .collect()
}

fn optimize_options(lake: &Lake) -> codelake_core::QueryOptions {
    let mut options = codelake_core::QueryOptions::for_user("codelake");
    options.properties = lake
        .engine
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
    options
}

/// Trino 431 folds every name: a quoted `"CustomerCode"` is stored as
/// `customercode` in the metastore and the files. `CodeLake` keeps the case
/// written, in the metastore and in every file it writes (`CREATE TABLE AS`,
/// `INSERT`, `ADD COLUMN`, `OPTIMIZE`), and resolves the column whatever
/// case a query uses. An unquoted name is folded, as SQL reads it.
#[tokio::test]
async fn column_names_keep_their_case_in_the_metastore_and_the_files() {
    let lake = lake().await;
    lake.ok(
        "CREATE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS \"CustomerCode\", \
         CAST(x * 10 AS BIGINT) AS \"Valor\", CAST(x AS BIGINT) AS Unquoted \
         FROM (VALUES (1), (2)) v(x)",
    )
    .await;
    // The metastore names stay folded (the reference engine writes only into
    // those); the name as written is the column's `codelake.name`.
    assert_eq!(
        lake.columns("t"),
        vec![
            ("customercode".into(), "bigint".into()),
            ("valor".into(), "bigint".into()),
            ("unquoted".into(), "bigint".into()),
        ]
    );
    assert_eq!(
        written_names(&lake, "t").await,
        vec![Some("CustomerCode".to_owned()), Some("Valor".into()), None]
    );
    assert_eq!(
        file_columns(&lake, "t"),
        vec![vec!["CustomerCode".to_owned(), "Valor".into(), "unquoted".into()]]
    );
    for sql in [
        "SELECT customercode, valor FROM hive.s.t ORDER BY 1",
        "SELECT CUSTOMERCODE, VALOR FROM hive.s.t ORDER BY 1",
        "SELECT \"CustomerCode\", \"Valor\" FROM hive.s.t ORDER BY 1",
        "SELECT \"customercode\", \"VALOR\" FROM hive.s.t ORDER BY 1",
    ] {
        assert_eq!(lake.ok(sql).await, rows(&[&["1", "10"], &["2", "20"]]), "{sql}");
    }
    // Filters over the stored spelling prune and answer by the values.
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.t WHERE customercode = 2")
            .await,
        rows(&[&["1"]])
    );
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.t WHERE \"CustomerCode\" > 100")
            .await,
        rows(&[&["0"]])
    );

    lake.ok("INSERT INTO hive.s.t VALUES (3, 30, 3)").await;
    lake.ok("ALTER TABLE hive.s.t ADD COLUMN \"DataCadastro\" date")
        .await;
    lake.ok("INSERT INTO hive.s.t (customercode, valor, unquoted, datacadastro) VALUES (4, 40, 4, DATE '2026-09-26')")
        .await;
    assert_eq!(lake.columns("t")[3], ("datacadastro".into(), "date".into()));
    assert_eq!(
        written_names(&lake, "t").await[3].as_deref(),
        Some("DataCadastro")
    );
    let spelled = file_columns(&lake, "t");
    assert_eq!(spelled.len(), 3);
    assert_eq!(spelled[1], vec!["CustomerCode", "Valor", "unquoted"]);
    assert_eq!(
        spelled[2],
        vec!["CustomerCode", "Valor", "unquoted", "DataCadastro"]
    );
    let all = rows(&[
        &["1", "10", "NULL"],
        &["2", "20", "NULL"],
        &["3", "30", "NULL"],
        &["4", "40", "2026-09-26"],
    ]);
    let query = "SELECT customercode, valor, datacadastro FROM hive.s.t ORDER BY 1";
    assert_eq!(lake.ok(query).await, all);

    // Compaction keeps the spelling and the rows.
    run_with_options(&lake, "ALTER TABLE hive.s.t EXECUTE optimize").await;
    assert_eq!(
        file_columns(&lake, "t"),
        vec![vec!["CustomerCode", "Valor", "unquoted", "DataCadastro"]]
    );
    assert_eq!(lake.ok(query).await, all);
    // The engine shows the folded names (what every query resolves).
    assert_eq!(
        lake.ok("SELECT column_name FROM hive.information_schema.columns \
             WHERE table_schema = 's' AND table_name = 't' ORDER BY ordinal_position")
            .await,
        rows(&[&["customercode"], &["valor"], &["unquoted"], &["datacadastro"]])
    );
}

async fn run_with_options(lake: &Lake, sql: &str) {
    let options = optimize_options(lake);
    lake.engine
        .sql_with(&options, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

/// Two names that differ only by case, at the same level, are refused with
/// an error that says so: columns of one table, a column added next to
/// another, tables, views and schemas. Nothing is created.
#[tokio::test]
async fn names_that_differ_only_by_case_are_refused() {
    let lake = lake().await;
    let err = lake
        .err("CREATE TABLE hive.s.d AS SELECT 1 AS \"Valor\", 2 AS \"VALOR\"")
        .await;
    assert!(
        err.contains("`Valor` and `VALOR`") && err.contains("differ only by case"),
        "{err}"
    );
    assert!(!lake.dir("d").exists());
    let err = lake.err("CREATE TABLE hive.s.d (\"Id\" bigint, id bigint)").await;
    assert!(err.contains("differ only by case"), "{err}");

    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS \"Valor\"")
        .await;
    let err = lake.err("ALTER TABLE hive.s.t ADD COLUMN valor bigint").await;
    assert!(
        err.contains("column Valor exists") && err.contains("differ only by case"),
        "{err}"
    );
    let err = lake.err("ALTER TABLE hive.s.t ADD COLUMN \"VALOR\" bigint").await;
    assert!(err.contains("differ only by case"), "{err}");
    assert_eq!(lake.columns("t"), vec![("valor".into(), "bigint".into())]);

    let err = lake.err("CREATE TABLE hive.s.\"T\" AS SELECT 2 AS x").await;
    assert!(
        err.contains("hive.s.T cannot be created") && err.contains("hive.s.t already exists"),
        "{err}"
    );
    lake.ok("CREATE VIEW hive.s.v AS SELECT 1 AS a").await;
    let err = lake.err("CREATE VIEW hive.s.\"V\" AS SELECT 2 AS a").await;
    assert!(err.contains("differ only by case"), "{err}");
    assert_eq!(lake.ok("SELECT a FROM hive.s.v").await, rows(&[&["1"]]));
    let err = lake.err("CREATE SCHEMA hive.\"S\"").await;
    assert!(
        err.contains("hive.S cannot be created") && err.contains("differ only by case"),
        "{err}"
    );
    lake.ok("CREATE TABLE hive.s.u AS SELECT 1 AS x").await;
    // The engine finds the target taken (resolution ignores case) first.
    let err = lake.err("ALTER TABLE hive.s.u RENAME TO hive.s.\"T\"").await;
    assert!(err.contains("Target table 'hive.s.T' already exists"), "{err}");
    assert_eq!(lake.ok("SELECT x FROM hive.s.u").await, rows(&[&["1"]]));
    assert_eq!(lake.ok("SELECT \"Valor\" FROM hive.s.t").await, rows(&[&["1"]]));
}

/// Schema and table names keep their directory in lower case (the only one
/// the reference engine finds) and every case resolves to it; the name as
/// written is recorded in the metadata (`codelake.name`), and follows a
/// rename.
#[tokio::test]
async fn schema_and_table_names_record_the_case_written() {
    let lake = lake().await;
    lake.ok("CREATE SCHEMA hive.\"Vendas\"").await;
    lake.ok("CREATE TABLE hive.\"Vendas\".\"PedidoItem\" AS SELECT CAST(1 AS BIGINT) AS id")
        .await;
    let warehouse = lake.warehouse.path();
    assert!(warehouse.join("vendas/pedidoitem/.trinoSchema").exists());
    let schema: serde_json::Value =
        serde_json::from_slice(&std::fs::read(warehouse.join(".vendas.trinoSchema")).unwrap()).unwrap();
    assert_eq!(schema["parameters"]["codelake.name"], "Vendas");
    let metastore = lake.metastore();
    let table = metastore.table("vendas", "pedidoitem").await.unwrap().unwrap();
    assert_eq!(
        table.parameters.get("codelake.name").map(String::as_str),
        Some("PedidoItem")
    );
    for name in [
        "hive.vendas.pedidoitem",
        "hive.VENDAS.PEDIDOITEM",
        "hive.\"Vendas\".\"PedidoItem\"",
        "hive.\"vendas\".\"PEDIDOITEM\"",
    ] {
        assert_eq!(
            lake.ok(&format!("SELECT id FROM {name}")).await,
            rows(&[&["1"]]),
            "{name}"
        );
    }
    lake.ok("ALTER TABLE hive.vendas.pedidoitem RENAME TO hive.vendas.\"ItemPedido\"")
        .await;
    let table = metastore.table("vendas", "itempedido").await.unwrap().unwrap();
    assert_eq!(
        table.parameters.get("codelake.name").map(String::as_str),
        Some("ItemPedido")
    );
    lake.ok("ALTER TABLE hive.vendas.itempedido RENAME TO hive.vendas.lower_case")
        .await;
    let table = metastore.table("vendas", "lower_case").await.unwrap().unwrap();
    assert!(!table.parameters.contains_key("codelake.name"));
    // Unquoted names record nothing: the metadata is what it always was.
    lake.ok("CREATE TABLE hive.s.plain AS SELECT CAST(1 AS BIGINT) AS id")
        .await;
    let plain = metastore.table("s", "plain").await.unwrap().unwrap();
    assert!(!plain.parameters.contains_key("codelake.name"));
}

/// Metadata another writer left with two columns that differ only by case
/// is refused when the table is read, naming both; never one read for the
/// other.
#[tokio::test]
async fn a_table_whose_columns_differ_only_by_case_is_refused() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS a, CAST(2 AS BIGINT) AS b")
        .await;
    let metastore = lake.metastore();
    let mut metadata = metastore.table("s", "t").await.unwrap().unwrap();
    metadata.data_columns[1].name = "A".into();
    metastore.write_table("s", "t", &metadata).await.unwrap();
    let err = lake.err("SELECT * FROM hive.s.t").await;
    assert!(
        err.contains("`a` and `A`") && err.contains("differ only by case"),
        "{err}"
    );
}

/// A `codelake.name` that no longer spells the column (another engine
/// renamed it) is ignored: files are written with the metastore's name.
#[tokio::test]
async fn a_stale_written_name_is_ignored() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS \"CustomerCode\"")
        .await;
    let metastore = lake.metastore();
    let mut metadata = metastore.table("s", "t").await.unwrap().unwrap();
    metadata.data_columns[0].name = "cod".into();
    metastore.write_table("s", "t", &metadata).await.unwrap();
    lake.ok("INSERT INTO hive.s.t VALUES (2)").await;
    let spelled = file_columns(&lake, "t");
    assert!(spelled.contains(&vec!["cod".to_owned()]), "{spelled:?}");
    assert_eq!(
        lake.ok("SELECT cod FROM hive.s.t ORDER BY 1").await,
        rows(&[&["2"], &["NULL"]])
    );
}
