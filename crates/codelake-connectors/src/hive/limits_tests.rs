//! Limitations copied from the reference engine and removed (ISS-017,
//! ISS-018, ISS-023): each test states what the reference engine does and
//! what `CodeLake` does instead, and checks the rows.

use std::path::Path as FsPath;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use super::f2_tests::{lake, rows};

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
