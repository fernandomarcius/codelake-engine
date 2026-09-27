//! Independent red-team probes of the removed limitations (ISS-017,
//! ISS-018, ISS-023): each probe checks rows, files or metadata.

use std::path::Path as FsPath;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use futures::StreamExt;

use super::f2_tests::{Lake, lake, rows};

fn batch(columns: &[(&str, Vec<i64>)]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, _)| Field::new(*name, DataType::Int64, true))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|(_, values)| Arc::new(Int64Array::from(values.clone())) as ArrayRef)
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn snapshot(dir: &FsPath) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &FsPath, dir: &FsPath, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let name = path.strip_prefix(root).unwrap().display().to_string();
                out.push((name, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
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

/// A table `s.t` with a quoted mixed-case column, spread over many files
/// (one per `INSERT`, values 1..=40), plus one file spelled in lower case
/// and one spelled in upper case (as another writer would), values
/// 41..=44.
async fn mixed_case_table(lake: &Lake) {
    lake.ok(
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS \"CustomerCode\", \
         CAST(10 AS BIGINT) AS \"Valor\"",
    )
    .await;
    for k in 2..=40 {
        lake.ok(&format!("INSERT INTO hive.s.t VALUES ({k}, {})", k * 10))
            .await;
    }
    lake.put_file(
        "t",
        "zz_lower_writer",
        &batch(&[("customercode", vec![41, 42]), ("valor", vec![410, 420])]),
    );
    lake.put_file(
        "t",
        "zz_upper_writer",
        &batch(&[("CUSTOMERCODE", vec![43, 44]), ("VALOR", vec![430, 440])]),
    );
}

/// Filters, pruning, `TopK` and hash-join dynamic filters over a column
/// stored with other spellings in its files answer by the values.
#[tokio::test]
async fn filters_and_pruning_over_mixed_spellings_answer_the_right_rows() {
    let lake = lake().await;
    mixed_case_table(&lake).await;
    for (sql, expected) in [
        ("SELECT count(*) FROM hive.s.t", "44"),
        ("SELECT count(customercode) FROM hive.s.t", "44"),
        ("SELECT count(*) FROM hive.s.t WHERE customercode IS NULL", "0"),
        (
            "SELECT count(*) FROM hive.s.t WHERE \"CustomerCode\" IS NOT NULL",
            "44",
        ),
        ("SELECT count(*) FROM hive.s.t WHERE customercode = 7", "1"),
        ("SELECT count(*) FROM hive.s.t WHERE customercode = 42", "1"),
        ("SELECT count(*) FROM hive.s.t WHERE CUSTOMERCODE = 44", "1"),
        (
            "SELECT count(*) FROM hive.s.t WHERE customercode BETWEEN 39 AND 43",
            "5",
        ),
        (
            "SELECT count(*) FROM hive.s.t WHERE customercode IN (1, 20, 41, 43, 99)",
            "4",
        ),
        ("SELECT sum(valor) FROM hive.s.t WHERE customercode > 40", "1700"),
        ("SELECT min(customercode) FROM hive.s.t", "1"),
        ("SELECT max(\"CustomerCode\") FROM hive.s.t", "44"),
        ("SELECT sum(valor) FROM hive.s.t", "9900"),
    ] {
        assert_eq!(lake.ok(sql).await, rows(&[&[expected]]), "{sql}");
    }
    assert_eq!(
        lake.ok("SELECT customercode, valor FROM hive.s.t ORDER BY customercode DESC LIMIT 3")
            .await,
        rows(&[&["44", "440"], &["43", "430"], &["42", "420"]])
    );
    assert_eq!(
        lake.ok("SELECT \"CustomerCode\" FROM hive.s.t ORDER BY 1 LIMIT 2")
            .await,
        rows(&[&["1"], &["2"]])
    );
    // A small build side: its values reach the probe scan as a dynamic
    // filter over the stored spelling.
    lake.ok(
        "CREATE TABLE hive.s.pick AS SELECT CAST(x AS BIGINT) AS \"Id\" FROM (VALUES (3), (42), (44)) v(x)",
    )
    .await;
    assert_eq!(
        lake.ok(
            "SELECT t.customercode, t.valor FROM hive.s.pick p JOIN hive.s.t t \
             ON t.\"CustomerCode\" = p.\"Id\" ORDER BY 1"
        )
        .await,
        rows(&[&["3", "30"], &["42", "420"], &["44", "440"]])
    );
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.t WHERE customercode NOT IN (SELECT id FROM hive.s.pick)")
            .await,
        rows(&[&["41"]])
    );
}

/// `INSERT` with a column list in any spelling and order puts each value in
/// its column; `JOIN … USING`, views and a CTAS over the table answer the
/// same rows.
#[tokio::test]
async fn every_sql_path_resolves_mixed_case_columns_to_the_right_values() {
    let lake = lake().await;
    lake.ok(
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS \"CustomerCode\", \
         CAST(10 AS BIGINT) AS \"Valor\"",
    )
    .await;
    lake.ok("INSERT INTO hive.s.t (valor, customercode) VALUES (500, 5)")
        .await;
    lake.ok("INSERT INTO hive.s.t (VALOR, CustomerCode) VALUES (600, 6)")
        .await;
    lake.ok("INSERT INTO hive.s.t (customercode) VALUES (7)").await;
    assert_eq!(
        lake.ok("SELECT customercode, valor FROM hive.s.t ORDER BY 1")
            .await,
        rows(&[&["1", "10"], &["5", "500"], &["6", "600"], &["7", "NULL"]])
    );

    lake.ok(
        "CREATE TABLE hive.s.u AS SELECT CAST(x AS BIGINT) AS \"CUSTOMERCODE\", 'n' || CAST(x AS varchar) AS \"Nome\" \
         FROM (VALUES (5), (7), (9)) v(x)",
    )
    .await;
    assert_eq!(
        lake.ok(
            "SELECT customercode, valor, nome FROM hive.s.t JOIN hive.s.u USING (customercode) ORDER BY 1"
        )
        .await,
        rows(&[&["5", "500", "n5"], &["7", "NULL", "n7"]])
    );

    lake.ok("CREATE VIEW hive.s.v AS SELECT \"CustomerCode\", \"Valor\" * 2 AS \"Dobro\" FROM hive.s.t")
        .await;
    assert_eq!(
        lake.ok("SELECT customercode, \"DOBRO\" FROM hive.s.v WHERE \"customercode\" > 1 ORDER BY 1")
            .await,
        rows(&[&["5", "1000"], &["6", "1200"], &["7", "NULL"]])
    );
    lake.ok("CREATE VIEW hive.s.star AS SELECT * FROM hive.s.t").await;
    assert_eq!(
        lake.ok("SELECT \"Valor\" FROM hive.s.star WHERE \"CustomerCode\" = 6")
            .await,
        rows(&[&["600"]])
    );

    lake.ok("CREATE TABLE hive.s.copy AS SELECT * FROM hive.s.t")
        .await;
    assert_eq!(
        lake.ok("SELECT sum(customercode), sum(valor) FROM hive.s.copy")
            .await,
        rows(&[&["19", "1110"]])
    );

    lake.ok("CREATE TABLE hive.s.plain AS SELECT CAST(1 AS BIGINT) AS a, CAST(2 AS BIGINT) AS b")
        .await;
    let plain = lake.run("SHOW COLUMNS FROM hive.s.plain").await;
    let mixed = lake.run("SHOW COLUMNS FROM hive.s.t").await;
    assert_eq!(
        plain.as_ref().map(Vec::len),
        mixed.as_ref().map(Vec::len),
        "SHOW COLUMNS: plain {plain:?} mixed {mixed:?}"
    );
    for sql in [
        "DESCRIBE hive.s.t",
        "SELECT column_name FROM hive.information_schema.columns WHERE table_schema = 's' AND table_name = 't'",
    ] {
        let answer = lake.ok(sql).await;
        assert_eq!(answer.len(), 2, "{sql}: {answer:?}");
    }
}

/// `CREATE OR REPLACE` whose new columns differ only by case is refused
/// before anything moves: the old table stays, byte for byte.
#[tokio::test]
async fn or_replace_with_case_duplicates_keeps_the_old_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS a")
        .await;
    let before = snapshot(&lake.dir("t"));
    let err = lake
        .err("CREATE OR REPLACE TABLE hive.s.t AS SELECT 1 AS \"A\", 2 AS a")
        .await;
    assert!(err.contains("differ only by case"), "{err}");
    assert_eq!(snapshot(&lake.dir("t")), before);
    lake.ok("CREATE OR REPLACE TABLE hive.s.\"T\" AS SELECT CAST(2 AS BIGINT) AS \"B\"")
        .await;
    assert_eq!(lake.ok("SELECT b FROM hive.s.t").await, rows(&[&["2"]]));
    let metadata = lake.metastore().table("s", "t").await.unwrap().unwrap();
    assert_eq!(
        metadata.parameters.get("codelake.name").map(String::as_str),
        Some("T")
    );
}

/// A reader that already streams the old table while a replacement swaps
/// it in: its answer is the whole old table or an error, never a mix and
/// never a short count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_streaming_through_a_replacement_never_mixes() {
    for round in 0..6 {
        let lake = lake().await;
        lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v")
            .await;
        for k in 2..=30 {
            lake.ok(&format!("INSERT INTO hive.s.t VALUES ({k})")).await;
        }
        let frame = lake.engine.sql("SELECT v FROM hive.s.t").await.unwrap();
        let mut stream = frame.execute_stream().await.unwrap();
        let mut seen: Vec<i64> = Vec::new();
        let mut failed = None;
        let take = |b: &RecordBatch, seen: &mut Vec<i64>| {
            let values = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            seen.extend(values.iter().flatten());
        };
        match stream.next().await {
            Some(Ok(b)) => take(&b, &mut seen),
            Some(Err(e)) => failed = Some(e.to_string()),
            None => {}
        }
        lake.ok(
            "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(-value AS BIGINT) AS v FROM generate_series(1, 5)",
        )
        .await;
        while failed.is_none() {
            match stream.next().await {
                Some(Ok(b)) => take(&b, &mut seen),
                Some(Err(e)) => failed = Some(e.to_string()),
                None => break,
            }
        }
        assert!(
            seen.iter().all(|v| *v > 0),
            "round {round}: rows of the new table in an old read: {seen:?}"
        );
        if failed.is_none() {
            seen.sort_unstable();
            assert_eq!(seen, (1..=30).collect::<Vec<_>>(), "round {round}");
        }
        assert_eq!(
            lake.ok("SELECT count(*), sum(v) FROM hive.s.t").await,
            rows(&[&["5", "-15"]])
        );
    }
}

/// `OPTIMIZE` planned before a replacement: the old table's rows never land
/// among the new table's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn optimize_planned_before_a_replacement_changes_nothing() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v")
        .await;
    for k in 2..=20 {
        lake.ok(&format!("INSERT INTO hive.s.t VALUES ({k})")).await;
    }
    let options = optimize_options(&lake);
    let optimize = lake
        .engine
        .sql_with(&options, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    lake.ok("CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(-7 AS BIGINT) AS v")
        .await;
    // Planned by name: it runs on the new table (one file, nothing to do).
    let _ = optimize.collect().await;
    assert_eq!(
        lake.ok("SELECT count(*), sum(v) FROM hive.s.t").await,
        rows(&[&["1", "-7"]])
    );
}

/// A view created over a directory another writer fills: rows of the
/// foreign files never become the view's, and `DROP VIEW` leaves the
/// directory exactly as the other writer left it, even after it wrote more.
#[tokio::test]
async fn drop_view_keeps_files_written_after_the_view() {
    let lake = lake().await;
    std::fs::create_dir_all(lake.dir("v")).unwrap();
    lake.put_file("v", "part-0", &batch(&[("x", vec![1])]));
    lake.ok("CREATE VIEW hive.s.v AS SELECT CAST(5 AS BIGINT) AS x")
        .await;
    lake.put_file("v", "part-1", &batch(&[("x", vec![2])]));
    std::fs::create_dir_all(lake.dir("v").join("sub")).unwrap();
    std::fs::write(lake.dir("v").join("sub/keep"), b"k").unwrap();
    assert_eq!(lake.ok("SELECT x FROM hive.s.v").await, rows(&[&["5"]]));
    let foreign: Vec<(String, Vec<u8>)> = snapshot(&lake.dir("v"))
        .into_iter()
        .filter(|(name, _)| !name.contains("trino"))
        .collect();
    let err = lake.err("DROP TABLE hive.s.v").await;
    assert!(!err.is_empty());
    lake.ok("DROP VIEW hive.s.v").await;
    assert_eq!(snapshot(&lake.dir("v")), foreign);
}

/// Another engine reads the warehouse without `CodeLake`'s locks (the
/// reference engine reads `.trinoSchema`, then lists the directory): while
/// replacements run, the table's metadata file is always there and always
/// names a whole table (the exchange is one step, never "no table").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lock_free_reader_always_finds_the_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(0 AS BIGINT) AS v")
        .await;
    let metadata = lake.dir("t").join(".trinoSchema");
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watcher = {
        let (metadata, done) = (metadata.clone(), Arc::clone(&done));
        std::thread::spawn(move || {
            let (mut reads, mut missing) = (0_u64, Vec::new());
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                match std::fs::read(&metadata) {
                    Ok(bytes) => {
                        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                        assert!(parsed["dataColumns"].is_array(), "{parsed}");
                    }
                    Err(err) => missing.push(err.to_string()),
                }
                reads += 1;
            }
            (reads, missing)
        })
    };
    for k in 1..=15 {
        lake.ok(&format!(
            "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, {k})"
        ))
        .await;
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    let (reads, missing) = watcher.join().unwrap();
    assert!(reads > 0);
    assert!(
        missing.is_empty(),
        "{} of {reads} reads found no table: {:?}",
        missing.len(),
        missing.first()
    );
    assert_eq!(lake.ok("SELECT count(*) FROM hive.s.t").await, rows(&[&["15"]]));
}

// Each test below failed before its fix and proves a defect of the tester's
// report.

/// An `INSERT` column list or a `JOIN … USING` names columns as the
/// statement that created the table wrote them (quoted): every spelling
/// resolves, as in a query (and as in the reference engine).
#[tokio::test]
async fn insert_column_lists_resolve_quoted_spellings() {
    let lake = lake().await;
    lake.ok(
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS \"CustomerCode\", \
         CAST(10 AS BIGINT) AS \"Valor\"",
    )
    .await;
    assert_eq!(
        lake.ok("SELECT \"CustomerCode\", \"Valor\" FROM hive.s.t").await,
        rows(&[&["1", "10"]])
    );
    let mut failures = Vec::new();
    for (sql, key) in [
        (
            "INSERT INTO hive.s.t (\"CustomerCode\", \"Valor\") VALUES (2, 20)",
            "2",
        ),
        (
            "INSERT INTO hive.s.t (\"Valor\", \"CUSTOMERCODE\") VALUES (30, 3)",
            "3",
        ),
    ] {
        match lake.run(sql).await {
            Ok(_) => assert_eq!(
                lake.ok(&format!("SELECT valor FROM hive.s.t WHERE customercode = {key}"))
                    .await,
                rows(&[&[&format!("{key}0")]]),
                "{sql}"
            ),
            Err(err) => failures.push(format!("{sql}: {err}")),
        }
    }
    lake.ok("CREATE TABLE hive.s.u AS SELECT CAST(2 AS BIGINT) AS \"CustomerCode\"")
        .await;
    for sql in [
        "SELECT count(*) FROM hive.s.t JOIN hive.s.u USING (\"CustomerCode\")",
        "SELECT count(*) FROM hive.s.t JOIN hive.s.u USING (CustomerCode)",
    ] {
        if let Err(err) = lake.run(sql).await {
            failures.push(format!("{sql}: {err}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A non-ASCII capital letter in a column name: an unquoted name folds as
/// an ASCII one does (`DESCRIÇÃO` is `descrição`, recorded as nothing else),
/// a quoted one keeps its spelling, and every spelling reads the column.
#[tokio::test]
async fn non_ascii_capitals_keep_a_written_spelling_and_resolve() {
    let lake = lake().await;
    lake.ok(
        "CREATE TABLE hive.s.acc AS SELECT CAST(1 AS BIGINT) AS \"CÓDIGO\", CAST(2 AS BIGINT) AS DESCRIÇÃO",
    )
    .await;
    let metadata = lake.metastore().table("s", "acc").await.unwrap().unwrap();
    let written: Vec<Option<String>> = metadata
        .data_columns
        .iter()
        .map(|c| c.properties.get("codelake.name").cloned())
        .collect();
    let names: Vec<String> = metadata.data_columns.iter().map(|c| c.name.clone()).collect();
    assert_eq!(names, vec!["código".to_owned(), "descrição".to_owned()]);
    let mut failures = Vec::new();
    if written != vec![Some("CÓDIGO".to_owned()), None] {
        failures.push(format!("written spellings recorded: {written:?}"));
    }
    for sql in [
        "SELECT \"CÓDIGO\", \"DESCRIÇÃO\" FROM hive.s.acc",
        "SELECT código, descrição FROM hive.s.acc",
        "SELECT CÓDIGO, DESCRIÇÃO FROM hive.s.acc",
    ] {
        let answer = lake.run(sql).await;
        if answer != Ok(rows(&[&["1", "2"]])) {
            failures.push(format!("{sql}: {answer:?}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A plan kept by a client after `CREATE OR REPLACE` put in a table with
/// the same columns: the kept plan names the replaced table, so every run of
/// it is refused with an error that says what to do (plan it again; the
/// PostgreSQL wire does that itself before running a prepared statement,
/// e2e `a_prepared_statement_answers_after_or_replace`), and the statement
/// planned again answers the new table's rows.
#[tokio::test]
async fn a_kept_plan_is_refused_after_a_replacement_and_a_new_plan_reads_the_new_table() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v")
        .await;
    let sql = "SELECT count(*), sum(v) FROM hive.s.t";
    let kept = lake.engine.sql(sql).await.unwrap();
    assert!(kept.clone().collect().await.is_ok());
    lake.ok("CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(2 AS BIGINT) AS v")
        .await;
    for _ in 0..2 {
        let err = kept.clone().collect().await.unwrap_err().to_string();
        assert!(
            err.contains("was replaced (CREATE OR REPLACE) after this statement was planned")
                && err.contains("plan the statement again"),
            "{err}"
        );
    }
    assert_eq!(lake.ok(sql).await, rows(&[&["1", "2"]]));
}
