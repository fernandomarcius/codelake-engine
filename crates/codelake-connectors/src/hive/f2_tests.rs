//! Hive catalog behavior checked against Trino 431 answers (F2 red-team
//! findings), on a local warehouse through the engine. Expected values are
//! what `trinodb/trino:431` answers for the same statements (checked on a
//! container with `hive.metastore=file`); the red-team probes themselves are
//! in `tests/integration/hive_redteam.rs`.

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use codelake_core::config::{ExecutionConfig, HiveTimestampPrecision, MemoryConfig};
use codelake_core::{ByteSize, Connector, Engine};
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::parquet::arrow::ArrowWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

use super::crc::hadoop_crc;
use super::metastore::FileMetastore;
use super::{HiveConnector, HiveParams};

pub(super) type Rows = Vec<Vec<String>>;

pub(super) struct Lake {
    pub(super) warehouse: tempfile::TempDir,
    /// Kept alive while the engine spills there.
    _spill: tempfile::TempDir,
    pub(super) engine: Engine,
}

pub(super) async fn engine_over(
    warehouse: &FsPath,
    spill: &FsPath,
    precision: HiveTimestampPrecision,
) -> Engine {
    let memory = MemoryConfig {
        limit: ByteSize::mib(256),
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::mib(256),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    let connector: Arc<dyn Connector> = Arc::new(
        HiveConnector::new(
            "hive".into(),
            HiveParams {
                warehouse: warehouse.display().to_string(),
                s3_endpoint: None,
                s3_region: "us-east-1".into(),
                credentials: None,
                path_style: false,
            },
        )
        .with_timestamp_precision(precision)
        .with_metastore_names(crate::hive::reference_names()),
    );
    engine
        .attach(connector, true, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    engine
}

async fn lake_at(precision: HiveTimestampPrecision) -> Lake {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let engine = engine_over(warehouse.path(), spill.path(), precision).await;
    let lake = Lake {
        warehouse,
        _spill: spill,
        engine,
    };
    lake.ok("CREATE SCHEMA hive.s").await;
    lake
}

pub(super) async fn lake() -> Lake {
    lake_at(HiveTimestampPrecision::Milliseconds).await
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

pub(super) fn rows(values: &[&[&str]]) -> Rows {
    values
        .iter()
        .map(|r| r.iter().map(|v| (*v).to_owned()).collect())
        .collect()
}

impl Lake {
    pub(super) async fn run(&self, sql: &str) -> Result<Rows, String> {
        run_on(&self.engine, sql).await
    }

    pub(super) async fn ok(&self, sql: &str) -> Rows {
        self.run(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    pub(super) async fn err(&self, sql: &str) -> String {
        match self.run(sql).await {
            Ok(rows) => panic!("`{sql}` should fail, returned {rows:?}"),
            Err(err) => err,
        }
    }

    pub(super) fn dir(&self, table: &str) -> PathBuf {
        self.warehouse.path().join("s").join(table)
    }

    pub(super) fn metastore(&self) -> FileMetastore {
        let store = LocalFileSystem::new_with_prefix(self.warehouse.path()).unwrap();
        FileMetastore::new(Arc::new(store), Path::default())
            .with_names(crate::hive::reference_names())
            .with_local_dir(self.warehouse.path().into())
    }

    /// Rewrites a table's metadata (and its checksum) the way another writer
    /// would.
    fn patch(&self, table: &str, patch: impl FnOnce(&mut serde_json::Value)) {
        let file = self.dir(table).join(".trinoSchema");
        let mut metadata: serde_json::Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        patch(&mut metadata);
        let bytes = serde_json::to_vec_pretty(&metadata).unwrap();
        std::fs::write(self.dir(table).join("..trinoSchema.crc"), hadoop_crc(&bytes)).unwrap();
        std::fs::write(file, bytes).unwrap();
    }

    /// Writes a Parquet file into a table directory, as Spark or pandas would.
    pub(super) fn put_file(&self, table: &str, name: &str, batch: &RecordBatch) {
        let file = std::fs::File::create(self.dir(table).join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    pub(super) fn columns(&self, table: &str) -> Vec<(String, String)> {
        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.dir(table).join(".trinoSchema")).unwrap()).unwrap();
        metadata["dataColumns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["name"].as_str().unwrap().to_owned(),
                    c["type"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    }
}

pub(super) async fn run_on(engine: &Engine, sql: &str) -> Result<Rows, String> {
    let frame = engine.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = frame.collect().await.map_err(|e| e.to_string())?;
    Ok(to_rows(&batches))
}

// ---------------------------------------------------------------------------
// varchar(n) and char(n)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bounded_varchar_is_enforced_on_insert_and_cut_on_read_like_trino() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.v (id bigint, c varchar(3))").await;
    assert_eq!(
        lake.columns("v"),
        vec![("id".into(), "bigint".into()), ("c".into(), "varchar(3)".into())],
        "Trino's DESCRIBE shows varchar(3), not an unbounded varchar"
    );
    // Trino 431: "Cannot truncate non-space characters when casting from
    // varchar(6) to varchar(3) on INSERT" (INVALID_CAST_ARGUMENT).
    let err = lake.err("INSERT INTO hive.s.v VALUES (1, 'abcdef')").await;
    assert!(
        err.contains("INVALID_CAST_ARGUMENT: Cannot truncate non-space characters"),
        "{err}"
    );
    // Only spaces beyond the length: cut, as Trino does.
    lake.ok("INSERT INTO hive.s.v VALUES (2, 'ab   '), (3, 'abc  ')")
        .await;
    assert_eq!(
        lake.ok("SELECT id, c, length(c) FROM hive.s.v ORDER BY id").await,
        rows(&[&["2", "ab ", "3"], &["3", "abc", "3"]])
    );
    // A longer value written by another tool reads cut to the length, as
    // Trino's reader returns it ('abc', length 3).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("c", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["abcdef"])),
        ],
    )
    .unwrap();
    lake.put_file("v", "20200101_000000_00000_other", &batch);
    assert_eq!(
        lake.ok("SELECT id, c, length(c) FROM hive.s.v WHERE id = 1")
            .await,
        rows(&[&["1", "abc", "3"]])
    );
}

#[tokio::test]
async fn char_columns_read_padded_and_compare_like_trino() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.ch (id bigint, ch char(5))").await;
    assert_eq!(lake.columns("ch")[1], ("ch".into(), "char(5)".into()));
    // Trino 431: "Cannot truncate non-space characters when casting from
    // varchar(8) to char(5) on INSERT".
    let err = lake.err("INSERT INTO hive.s.ch VALUES (1, 'abcdefgh')").await;
    assert!(
        err.contains("INVALID_CAST_ARGUMENT: Cannot truncate non-space characters"),
        "{err}"
    );
    lake.ok("INSERT INTO hive.s.ch VALUES (2, 'ab'), (3, 'abcde  '), (4, ''), (5, NULL)")
        .await;
    // Trino 431 answers exactly these rows (values padded, length 5).
    assert_eq!(
        lake.ok("SELECT id, CAST(ch AS varchar), length(ch), ch || '|' FROM hive.s.ch ORDER BY id")
            .await,
        rows(&[
            &["2", "ab   ", "5", "ab   |"],
            &["3", "abcde", "5", "abcde|"],
            &["4", "     ", "5", "     |"],
            &["5", "NULL", "NULL", "NULL"],
        ])
    );
    // Trino compares char values padded: `ch = 'ab'` and `ch = 'ab '` are
    // true, `ch < 'ab' || chr(9)` is false (the tab sorts below a space).
    let two: &[&[&str]] = &[&["2"]];
    for (q, expected) in [
        ("SELECT id FROM hive.s.ch WHERE ch = 'ab' ORDER BY 1", two),
        ("SELECT id FROM hive.s.ch WHERE ch = 'ab  ' ORDER BY 1", two),
        ("SELECT id FROM hive.s.ch WHERE ch IN ('ab', 'x') ORDER BY 1", two),
        ("SELECT id FROM hive.s.ch WHERE ch = '' ORDER BY 1", &[&["4"]]),
        (
            "SELECT id FROM hive.s.ch WHERE ch < 'ab' || chr(9) ORDER BY 1",
            &[&["4"]],
        ),
        (
            "SELECT id FROM hive.s.ch WHERE ch > 'ab' || chr(9) ORDER BY 1",
            &[&["2"], &["3"]],
        ),
    ] {
        assert_eq!(lake.ok(q).await, rows(expected), "{q}");
    }
    assert_eq!(
        lake.ok("SELECT id FROM hive.s.ch WHERE ch IS NULL").await,
        rows(&[&["5"]])
    );
    // What Trino stored, read back padded: values are stored without their
    // trailing spaces, as Trino stores them.
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS id, 'ab' AS ch")
        .await;
    lake.patch("t", |m| m["dataColumns"][1]["type"] = "char(5)".into());
    assert_eq!(
        lake.ok("SELECT id, ch, length(ch) FROM hive.s.t").await,
        rows(&[&["1", "ab   ", "5"]])
    );
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn timestamps_round_half_up_on_insert_like_trino() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.ts (id bigint, ts timestamp(3))")
        .await;
    lake.ok(
        "INSERT INTO hive.s.ts VALUES (6, TIMESTAMP '2026-01-01 00:00:00.9996'), \
         (7, TIMESTAMP '2026-01-01 00:00:00.1235'), (8, TIMESTAMP '1969-12-31 23:59:59.9984')",
    )
    .await;
    // Trino 431 reads back what it wrote for the same INSERT.
    assert_eq!(
        lake.ok("SELECT id, CAST(ts AS varchar) FROM hive.s.ts ORDER BY id")
            .await,
        rows(&[
            &["6", "2026-01-01 00:00:01.000"],
            &["7", "2026-01-01 00:00:00.124"],
            &["8", "1969-12-31 23:59:59.998"],
        ])
    );
}

fn nanos_file(values: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampNanosecondArray::from(values.to_vec())) as ArrayRef],
    )
    .unwrap()
}

#[tokio::test]
async fn finer_timestamps_in_files_round_on_read_like_trino() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.nanos (ts timestamp(3))").await;
    // 2026-01-01 00:00:00.9996 and 1969-12-31 23:59:59.9996, in nanoseconds.
    let file = nanos_file(&[1_767_225_600_999_600_000, -400_000]);
    lake.put_file("nanos", "20200101_000000_00000_spark", &file);
    assert_eq!(
        lake.ok("SELECT CAST(ts AS varchar) FROM hive.s.nanos ORDER BY 1")
            .await,
        rows(&[&["1970-01-01 00:00:00.000"], &["2026-01-01 00:00:01.000"]])
    );
    // A filter sees the rounded value too.
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.nanos WHERE ts = TIMESTAMP '2026-01-01 00:00:01.000'")
            .await,
        rows(&[&["1"]])
    );
}

#[tokio::test]
async fn timestamp_precision_is_the_catalogs_like_hive_timestamp_precision() {
    let lake = lake().await;
    // Trino 431, MILLISECONDS: "Incorrect timestamp precision for
    // timestamp(6); the configured precision is MILLISECONDS; column name: ts".
    let err = lake.err("CREATE TABLE hive.s.t6 (ts timestamp(6))").await;
    assert!(
        err.contains(
            "Incorrect timestamp precision for timestamp(6); the configured precision is MILLISECONDS; \
             column name: ts"
        ),
        "{err}"
    );
    lake.ok("CREATE TABLE hive.s.t3 (ts timestamp(3))").await;
    lake.ok("CREATE TABLE hive.s.tdefault (ts timestamp)").await;
    let err = lake
        .err("CREATE TABLE hive.s.c6 AS SELECT TIMESTAMP '2026-01-01 10:00:00.123456' AS ts")
        .await;
    assert!(
        err.contains(
            "Incorrect timestamp precision for timestamp(6); the configured precision is MILLISECONDS"
        ),
        "{err}"
    );
    // ADD COLUMN takes any precision and records `timestamp`, as Trino.
    lake.ok("ALTER TABLE hive.s.t3 ADD COLUMN u timestamp(6)").await;
    assert_eq!(lake.columns("t3")[1], ("u".into(), "timestamp".into()));

    // A MICROSECONDS catalog over the same kind of files keeps microseconds.
    let micros = lake_at(HiveTimestampPrecision::Microseconds).await;
    micros.ok("CREATE TABLE hive.s.us (ts timestamp(6))").await;
    assert!(
        micros
            .run("CREATE TABLE hive.s.ms (ts timestamp(3))")
            .await
            .is_err()
    );
    micros
        .ok("INSERT INTO hive.s.us VALUES (TIMESTAMP '2026-01-01 00:00:00.1234565')")
        .await;
    let file = nanos_file(&[1_767_225_600_000_001_500]);
    micros.put_file("us", "20200101_000000_00000_spark", &file);
    assert_eq!(
        micros
            .ok("SELECT CAST(ts AS varchar) FROM hive.s.us ORDER BY 1")
            .await,
        rows(&[&["2026-01-01 00:00:00.000002"], &["2026-01-01 00:00:00.123457"]])
    );
}

#[tokio::test]
async fn ctas_timestamps_follow_trino_431_precision_and_range() {
    let lake = lake().await;
    // Trino 431: `TIMESTAMP '… 10:00:00'` is timestamp(0), refused by a
    // MILLISECONDS catalog ("Incorrect timestamp precision for timestamp(0)").
    let err = lake
        .err("CREATE TABLE hive.s.c0 AS SELECT TIMESTAMP '2026-01-01 10:00:00' AS ts")
        .await;
    assert!(
        err.contains("Incorrect timestamp precision for timestamp(0)"),
        "{err}"
    );
    // Millisecond literals far from 1970 store and read back, as in Trino.
    lake.ok("CREATE TABLE hive.s.far AS SELECT id, ts FROM (VALUES \
         (CAST(1 AS BIGINT), TIMESTAMP '1582-10-10 12:00:00.000'), (2, TIMESTAMP '0001-01-01 00:00:00.000'), \
         (3, TIMESTAMP '9999-12-31 23:59:59.999')) v(id, ts)")
        .await;
    assert_eq!(
        lake.ok("SELECT id, CAST(ts AS varchar) FROM hive.s.far ORDER BY id")
            .await,
        rows(&[
            &["1", "1582-10-10 12:00:00.000"],
            &["2", "0001-01-01 00:00:00.000"],
            &["3", "9999-12-31 23:59:59.999"],
        ])
    );
}

// ---------------------------------------------------------------------------
// Metadata: checksums, Parquet column names, listing, unreadable tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_that_does_not_match_its_checksum_is_an_error() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.crc AS SELECT CAST(1 AS BIGINT) AS aa")
        .await;
    let file = lake.dir("crc").join(".trinoSchema");
    let text = std::fs::read_to_string(&file)
        .unwrap()
        .replace("\"aa\"", "\"zz\"");
    std::fs::write(&file, text).unwrap();
    // Trino 431: "Could not read table schema … ChecksumException: Checksum error".
    let err = lake.err("SELECT * FROM hive.s.crc").await;
    assert!(err.contains("Checksum error"), "{err}");
    // The rest of the catalog is still listed and readable.
    lake.ok("CREATE TABLE hive.s.fine AS SELECT CAST(2 AS BIGINT) AS x")
        .await;
    assert_eq!(
        lake.ok("SELECT table_name FROM hive.information_schema.tables WHERE table_schema = 's' ORDER BY 1")
            .await,
        rows(&[&["crc"], &["fine"]])
    );
    // Metadata without a checksum file is read as it is (Trino does too).
    std::fs::remove_file(lake.dir("fine").join("..trinoSchema.crc")).unwrap();
    assert_eq!(lake.ok("SELECT x FROM hive.s.fine").await, rows(&[&["2"]]));
}

#[tokio::test]
async fn parquet_columns_match_table_columns_ignoring_case() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.upper (id bigint)").await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("ID", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![5])) as ArrayRef],
    )
    .unwrap();
    lake.put_file("upper", "20200101_000000_00000_pandas", &batch);
    // Trino 431 reads 5.
    assert_eq!(lake.ok("SELECT id FROM hive.s.upper").await, rows(&[&["5"]]));
}

#[tokio::test]
async fn tables_another_writer_creates_are_listed_at_once() {
    let lake = lake().await;
    // Another node on the same warehouse (Trino during a canary, or another
    // CodeLake): its DDL never goes through this node's catalog.
    let spill = tempfile::tempdir().unwrap();
    let other = engine_over(
        lake.warehouse.path(),
        spill.path(),
        HiveTimestampPrecision::Milliseconds,
    )
    .await;
    run_on(&other, "CREATE SCHEMA hive.late").await.unwrap();
    run_on(
        &other,
        "CREATE TABLE hive.s.early AS SELECT CAST(1 AS BIGINT) AS id",
    )
    .await
    .unwrap();
    assert_eq!(
        lake.ok("SELECT table_name FROM hive.information_schema.tables WHERE table_schema = 's'")
            .await,
        rows(&[&["early"]])
    );
    assert!(
        lake.ok("SELECT schema_name FROM hive.information_schema.schemata")
            .await
            .contains(&vec!["late".to_owned()])
    );
    run_on(&other, "DROP TABLE hive.s.early").await.unwrap();
    assert!(
        lake.ok("SHOW TABLES FROM hive.s").await.is_empty(),
        "a dropped table leaves the listing at once too"
    );
}

#[tokio::test]
async fn tables_codelake_cannot_read_fail_alone() {
    let lake = lake().await;
    for t in ["good", "arr", "part", "orc"] {
        lake.ok(&format!(
            "CREATE TABLE hive.s.{t} AS SELECT CAST(1 AS BIGINT) AS id, CAST(2 AS BIGINT) AS x"
        ))
        .await;
    }
    // What Trino writes for `x array(integer)`, a partitioned table, ORC.
    lake.patch("arr", |m| m["dataColumns"][1]["type"] = "array<int>".into());
    lake.patch("part", |m| {
        m["partitionColumns"] = serde_json::json!([{"name": "p", "type": "string", "properties": {}}]);
    });
    lake.patch("orc", |m| m["storageFormat"] = "ORC".into());
    assert_eq!(
        lake.ok("SELECT table_name FROM hive.information_schema.tables WHERE table_schema = 's' ORDER BY 1")
            .await,
        rows(&[&["arr"], &["good"], &["orc"], &["part"]])
    );
    assert_eq!(
        lake.ok(
            "SELECT table_name, column_name FROM hive.information_schema.columns \
             WHERE table_schema = 's' AND table_name IN ('arr', 'good') ORDER BY 1, 2"
        )
        .await,
        rows(&[&["arr", "id"], &["arr", "x"], &["good", "id"], &["good", "x"]])
    );
    // The array table reads, except its array column.
    assert_eq!(lake.ok("SELECT id FROM hive.s.arr").await, rows(&[&["1"]]));
    assert!(lake.err("SELECT x FROM hive.s.arr").await.contains("array<int>"));
    assert!(
        lake.err("SELECT id FROM hive.s.part")
            .await
            .contains("partitioned")
    );
    assert!(lake.err("SELECT id FROM hive.s.orc").await.contains("PARQUET"));
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

/// Stores a view as Trino 431 writes it.
fn trino_view(lake: &Lake, name: &str, sql: &str, columns: &serde_json::Value) {
    let definition = serde_json::json!({
        "originalSql": sql,
        "catalog": "hive",
        "schema": "s",
        "columns": columns,
        "owner": "trino",
        "runAsInvoker": false,
        "path": []
    });
    let text = format!("/* Presto View: {} */", BASE64.encode(definition.to_string()));
    let metadata = serde_json::json!({
        "writerVersion": "431",
        "owner": "trino",
        "tableType": "VIRTUAL_VIEW",
        "dataColumns": [{"name": "dummy", "type": "string", "properties": {}}],
        "partitionColumns": [],
        "parameters": {"comment": "Presto View", "presto_view": "true", "presto_version": "431"},
        "viewOriginalText": text,
        "viewExpandedText": "/* Presto View */",
        "serdeParameters": {},
        "columnStatistics": {}
    });
    let dir = lake.dir(name);
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = serde_json::to_vec_pretty(&metadata).unwrap();
    std::fs::write(dir.join("..trinoSchema.crc"), hadoop_crc(&bytes)).unwrap();
    std::fs::write(dir.join(".trinoSchema"), bytes).unwrap();
}

#[tokio::test]
async fn trino_views_keep_their_instants_and_unread_columns() {
    let lake = lake().await;
    trino_view(
        &lake,
        "vtz",
        "SELECT TIMESTAMP '2026-01-01 10:00:00 America/Sao_Paulo' AS t",
        &serde_json::json!([{"name": "t", "type": "timestamp(0) with time zone"}]),
    );
    // Trino shows 2026-01-01 10:00:00 America/Sao_Paulo: the same instant
    // as 13:00 UTC. Before, CodeLake returned 13:00 without a zone.
    let batches = lake
        .engine
        .sql("SELECT t FROM hive.s.vtz")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let column = batches[0].column(0);
    assert!(
        matches!(column.data_type(), DataType::Timestamp(_, Some(_))),
        "{}",
        column.data_type()
    );
    let instant =
        datafusion::arrow::compute::cast(column, &DataType::Timestamp(TimeUnit::Second, Some("UTC".into())))
            .unwrap();
    let seconds = datafusion::arrow::compute::cast(&instant, &DataType::Int64).unwrap();
    let seconds = seconds.as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(seconds, 1_767_272_400, "2026-01-01T13:00:00Z");

    trino_view(
        &lake,
        "tva",
        "SELECT BIGINT '1' AS id, ARRAY[1, 2] AS arr",
        &serde_json::json!([{"name": "id", "type": "bigint"}, {"name": "arr", "type": "array(integer)"}]),
    );
    // Trino 431: 1. The array column fails only when it is read.
    assert_eq!(lake.ok("SELECT id FROM hive.s.tva").await, rows(&[&["1"]]));
    assert!(
        lake.err("SELECT arr FROM hive.s.tva")
            .await
            .contains("array(integer)")
    );
}

#[tokio::test]
async fn a_view_whose_stored_sql_is_not_a_query_never_runs_it() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v")
        .await;
    // Metadata with a valid checksum, written by someone with access to the
    // warehouse: the stored SQL is an INSERT.
    trino_view(
        &lake,
        "ins",
        "INSERT INTO hive.s.t VALUES (99)",
        &serde_json::json!([{"name": "count", "type": "bigint"}]),
    );
    let err = lake.err("SELECT * FROM hive.s.ins").await;
    assert!(err.contains("a view must be a single query"), "{err}");
    assert_eq!(lake.ok("SELECT v FROM hive.s.t").await, rows(&[&["1"]]));
}

#[tokio::test]
async fn a_view_over_columns_with_source_metadata_can_be_aggregated() {
    // Shadow run, 2026-09-28: a view over SQL Server columns (field metadata
    // `sqlserver.type`) failed under GROUP BY: "Physical input schema should
    // be the same as the one converted from logical input schema".
    use datafusion::arrow::array::{Int64Array, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::datasource::MemTable;
    let lake = lake().await;
    let meta = |t: &str| std::collections::HashMap::from([("sqlserver.type".to_owned(), t.to_owned())]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("data_rec", DataType::Timestamp(TimeUnit::Millisecond, None), true)
            .with_metadata(meta("datetime")),
        Field::new("v", DataType::Int64, true).with_metadata(meta("bigint")),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1_000, 1_000, 2_000])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    lake.engine
        .context()
        .register_table(
            datafusion::sql::TableReference::full("datafusion", "public", "src"),
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    lake.ok("CREATE VIEW hive.s.recebimentos AS SELECT data_rec, v FROM datafusion.public.src")
        .await;
    assert_eq!(
        lake.ok("SELECT data_rec, sum(v) FROM hive.s.recebimentos GROUP BY data_rec ORDER BY data_rec")
            .await,
        rows(&[&["1970-01-01T00:00:01", "3"], &["1970-01-01T00:00:02", "3"]])
    );
}

#[tokio::test]
async fn views_with_quoted_mixed_case_columns_read_back() {
    let lake = lake().await;
    lake.ok("CREATE VIEW hive.s.vm AS SELECT \"MixedCol\" FROM (SELECT 1 AS \"MixedCol\") t")
        .await;
    // Trino 431 reads 1, under the folded name `mixedcol`.
    assert_eq!(lake.ok("SELECT * FROM hive.s.vm").await, rows(&[&["1"]]));
    assert_eq!(lake.ok("SELECT mixedcol FROM hive.s.vm").await, rows(&[&["1"]]));
    let view: serde_json::Value =
        serde_json::from_slice(&std::fs::read(lake.dir("vm").join(".trinoSchema")).unwrap()).unwrap();
    let text = view["viewOriginalText"].as_str().unwrap();
    let encoded = text
        .strip_prefix("/* Presto View: ")
        .and_then(|r| r.strip_suffix(" */"))
        .unwrap();
    let definition: serde_json::Value = serde_json::from_slice(&BASE64.decode(encoded).unwrap()).unwrap();
    assert_eq!(definition["columns"][0]["name"], "mixedcol");
}

// ---------------------------------------------------------------------------
// DDL options, names, OR REPLACE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_options_the_catalog_cannot_keep_are_refused() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS id")
        .await;
    // Trino 431: "Catalog 'hive' does not support NOT NULL for column 'n'".
    let err = lake.err("ALTER TABLE hive.s.t ADD COLUMN n INT NOT NULL").await;
    assert!(
        err.contains("Catalog 'hive' does not support NOT NULL for column 'n'"),
        "{err}"
    );
    // Trino 431: "Catalog 'hive' does not support non-null column for column name 'x'".
    let err = lake.err("CREATE TABLE hive.s.nn (x int NOT NULL)").await;
    assert!(
        err.contains("Catalog 'hive' does not support non-null column for column name 'x'"),
        "{err}"
    );
    assert!(
        lake.err("ALTER TABLE hive.s.t ADD COLUMN d INT DEFAULT 1")
            .await
            .contains("DEFAULT")
    );
    // Comments are kept, as Trino keeps them.
    lake.ok("ALTER TABLE hive.s.t ADD COLUMN c int COMMENT 'hello'")
        .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(lake.dir("t").join(".trinoSchema")).unwrap()).unwrap();
    assert_eq!(metadata["dataColumns"][1]["comment"], "hello");
    assert_eq!(lake.columns("t").len(), 2, "nothing half-added");
}

#[tokio::test]
async fn names_that_are_not_a_directory_of_their_own_are_refused() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS x")
        .await;
    // Trino 431: "Invalid table name" for `.` and `..`.
    for sql in [
        "CREATE TABLE hive.s.\"..\" AS SELECT 1 AS a",
        "CREATE TABLE hive.s.\".\" AS SELECT 1 AS a",
        "CREATE VIEW hive.s.\"a/b\" AS SELECT 1 AS a",
        "CREATE SCHEMA hive.\"..\"",
        "DROP VIEW hive.s.\"..\"",
    ] {
        let err = lake.err(sql).await;
        assert!(err.contains("Invalid"), "{sql}: {err}");
    }
    // Trino 431: "Zero-length delimited identifier not allowed" (SYNTAX_ERROR).
    let err = lake.err("CREATE VIEW hive.s.\"\" AS SELECT 1 AS a").await;
    assert!(
        err.contains("Zero-length delimited identifier not allowed"),
        "{err}"
    );
    assert_eq!(lake.ok("SELECT x FROM hive.s.t").await, rows(&[&["1"]]));
}

// ---------------------------------------------------------------------------
// Concurrency on tables whose metadata carries statistics (as Trino's do)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn add_column_racing_inserts_is_never_lost() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(0 AS BIGINT) AS v")
        .await;
    let metastore = lake.metastore();
    for i in 0..10 {
        // Statistics as Trino records them: every INSERT rewrites the metadata.
        let mut metadata = metastore.table("s", "t").await.unwrap().unwrap();
        metadata.parameters.insert("numRows".into(), "1".into());
        metastore.write_table("s", "t", &metadata).await.unwrap();
        let insert =
            lake.run("INSERT INTO hive.s.t (v) SELECT CAST(value AS BIGINT) FROM generate_series(1, 20000)");
        let add = format!("ALTER TABLE hive.s.t ADD COLUMN c{i} BIGINT");
        let (inserted, added) = tokio::join!(insert, lake.run(&add));
        inserted.unwrap();
        added.unwrap();
        let names: Vec<String> = lake.columns("t").into_iter().map(|(name, _)| name).collect();
        assert!(names.contains(&format!("c{i}")), "round {i}: {names:?}");
    }
    assert_eq!(lake.columns("t").len(), 11);
    assert_eq!(
        lake.ok("SELECT count(*) FROM hive.s.t").await,
        rows(&[&["200001"]])
    );
}

#[tokio::test]
async fn a_table_dropped_after_planning_is_an_error_not_an_empty_result() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v FROM generate_series(1, 10)")
        .await;
    // Planned (the table resolved) before the DROP, run after it.
    let planned = lake
        .engine
        .sql("SELECT count(*), sum(v) FROM hive.s.t")
        .await
        .unwrap();
    lake.ok("DROP TABLE hive.s.t").await;
    let err = planned.collect().await.unwrap_err().to_string();
    assert!(err.contains("does not exist anymore"), "{err}");
}

#[tokio::test]
async fn an_insert_that_cannot_commit_leaves_no_file() {
    let lake = lake().await;
    lake.ok("CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v")
        .await;
    let metastore = lake.metastore();
    let mut metadata = metastore.table("s", "t").await.unwrap().unwrap();
    metadata.parameters.insert("numRows".into(), "1".into());
    metastore.write_table("s", "t", &metadata).await.unwrap();
    // The metadata rewrite fails (its checksum path is taken by a directory).
    let crc = lake.dir("t").join("..trinoSchema.crc");
    std::fs::remove_file(&crc).unwrap();
    std::fs::create_dir_all(crc.join("blocker")).unwrap();
    let before: Vec<_> = std::fs::read_dir(lake.dir("t"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(lake.run("INSERT INTO hive.s.t VALUES (2)").await.is_err());
    let after: Vec<_> = std::fs::read_dir(lake.dir("t"))
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(before.len(), after.len(), "{after:?}");
    assert_eq!(lake.ok("SELECT v FROM hive.s.t").await, rows(&[&["1"]]));
}
