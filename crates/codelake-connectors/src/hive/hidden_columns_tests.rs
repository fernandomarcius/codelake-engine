//! Hidden columns of Hive tables (`"$path"`, `"$file_size"`,
//! `"$file_modified_time"`) on a local warehouse, through the engine.
//! Expected values are what `trinodb/trino:431` answers for the same
//! statements on a file metastore (`hive.metastore.catalog.dir` spelled
//! `file:/wh`, `file:///wh` or `/wh`); the differential gate compares both
//! engines on one warehouse (`tools/differential/cases/hidden_columns.toml`).

use std::collections::BTreeMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Connector, Engine, QueryOptions};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

use super::catalog::HiveCatalog;
use super::metastore::FileMetastore;
use super::{HiveConnector, HiveParams};

const ENABLED: &str = "hive.non_transactional_optimize_enabled";

struct Lake {
    warehouse: tempfile::TempDir,
    _spill: tempfile::TempDir,
    engine: Engine,
    /// The warehouse as the catalog spells it (`$path` starts with it).
    spelled: String,
}

/// A lake whose `warehouse` is its directory as `written` spells it.
async fn lake_spelled(written: impl Fn(&FsPath) -> String) -> Lake {
    let warehouse = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let spelled = written(warehouse.path());
    let memory = MemoryConfig {
        limit: ByteSize::mib(256),
        spill_dir: spill.path().to_owned(),
        spill_limit: ByteSize::mib(256),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
    let connector: Arc<dyn Connector> = Arc::new(
        HiveConnector::new(
            "hive".into(),
            HiveParams {
                warehouse: spelled.clone(),
                s3_endpoint: None,
                s3_region: "us-east-1".into(),
                credentials: None,
                path_style: false,
            },
        )
        .with_parquet_time_zone(codelake_core::parquet_time_zone::ParquetTimeZone::UTC)
        .with_metastore_names(crate::hive::reference_names()),
    );
    engine
        .attach(connector, true, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let lake = Lake {
        warehouse,
        _spill: spill,
        engine,
        spelled,
    };
    lake.ok("CREATE SCHEMA hive.s").await;
    lake
}

/// A lake whose `warehouse` is `file:` and its directory, as the reference
/// engine's catalog is spelled in the differential gate.
async fn lake() -> Lake {
    lake_spelled(|dir| format!("file:{}", dir.display())).await
}

impl Lake {
    async fn run_with(&self, options: &QueryOptions, sql: &str) -> Result<String, String> {
        let frame = self
            .engine
            .sql_with(options, sql)
            .await
            .map_err(|e| e.to_string())?;
        let batches = frame.collect().await.map_err(|e| e.to_string())?;
        Ok(pretty_format_batches(&batches)
            .map_err(|e| e.to_string())?
            .to_string())
    }

    async fn run(&self, sql: &str) -> Result<String, String> {
        self.run_with(&QueryOptions::for_user("dbt"), sql).await
    }

    async fn ok(&self, sql: &str) -> String {
        self.run(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    async fn err(&self, sql: &str) -> String {
        match self.run(sql).await {
            Ok(rows) => panic!("`{sql}` should fail, returned {rows}"),
            Err(err) => err,
        }
    }

    fn dir(&self, table: &str) -> PathBuf {
        self.warehouse.path().join("s").join(table)
    }

    /// Visible data files of a table, by name.
    fn files(&self, table: &str) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(self.dir(table))
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with(['.', '_']))
            .collect();
        names.sort();
        names
    }

    /// `$path` of the data file `name` of `table`.
    fn path(&self, table: &str, name: &str) -> String {
        format!("{}/s/{table}/{name}", self.spelled.trim_end_matches('/'))
    }

    /// Size and modification time (UTC, milliseconds cut) of a data file,
    /// as the reference engine shows them.
    fn file_facts(&self, table: &str, name: &str) -> (u64, String) {
        let metadata = std::fs::metadata(self.dir(table).join(name)).unwrap();
        let millis = metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let millis = i64::try_from(millis).unwrap();
        let time = chrono_like(millis);
        (metadata.len(), time)
    }

    fn catalog(&self) -> HiveCatalog {
        let store = LocalFileSystem::new_with_prefix(self.warehouse.path()).unwrap();
        HiveCatalog::new(
            "hive",
            FileMetastore::new(Arc::new(store), Path::default()).with_names(crate::hive::reference_names()),
        )
        .unwrap()
    }
}

/// `millis` since the epoch as the engine prints a UTC timestamp
/// (`2026-10-02T20:59:43.857Z`).
fn chrono_like(millis: i64) -> String {
    let array = datafusion::arrow::array::TimestampMillisecondArray::from(vec![millis]).with_timezone("UTC");
    datafusion::arrow::util::display::array_value_to_string(&array, 0).unwrap()
}

/// A table `t` with two files: rows 1 and 2 in the first, 3 in the second.
async fn two_files(lake: &Lake) -> Vec<String> {
    lake.ok("CREATE TABLE hive.s.t (a integer, b varchar)").await;
    lake.ok("INSERT INTO hive.s.t VALUES (1, 'x'), (2, 'y')").await;
    lake.ok("INSERT INTO hive.s.t VALUES (3, 'z')").await;
    let files = lake.files("t");
    assert_eq!(files.len(), 2, "{files:?}");
    files
}

fn lines(table: &str) -> Vec<String> {
    table
        .lines()
        .filter(|line| !line.starts_with('+'))
        .map(|line| line.split('|').map(str::trim).collect::<Vec<_>>().join("|"))
        .collect()
}

#[tokio::test]
async fn hidden_columns_name_the_file_of_each_row() {
    let lake = lake().await;
    let files = two_files(&lake).await;
    let (size0, time0) = lake.file_facts("t", &files[0]);
    let (size1, time1) = lake.file_facts("t", &files[1]);
    let answer = lake
        .ok("SELECT a, \"$path\", \"$file_size\", \"$file_modified_time\" FROM hive.s.t ORDER BY a")
        .await;
    let first = lake.path("t", &files[0]);
    let second = lake.path("t", &files[1]);
    assert!(first.starts_with("file:/"), "{first}");
    assert_eq!(
        lines(&answer),
        [
            "|a|$path|$file_size|$file_modified_time|".to_owned(),
            format!("|1|{first}|{size0}|{time0}|"),
            format!("|2|{first}|{size0}|{time0}|"),
            format!("|3|{second}|{size1}|{time1}|"),
        ]
    );
    // Types as the reference engine's: varchar, bigint and
    // timestamp(3) with time zone, in UTC whatever the session's zone.
    let mut options = QueryOptions::for_user("dbt");
    options.time_zone = Some("America/Sao_Paulo".into());
    let frame = lake
        .engine
        .sql_with(
            &options,
            "SELECT \"$path\", \"$file_size\", \"$file_modified_time\" FROM hive.s.t WHERE a = 3",
        )
        .await
        .unwrap();
    let types: Vec<DataType> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    assert_eq!(
        types,
        [
            DataType::Utf8,
            DataType::Int64,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ]
    );
    let zoned = lake
        .run_with(
            &options,
            "SELECT \"$file_modified_time\" FROM hive.s.t WHERE a = 3",
        )
        .await
        .unwrap();
    assert_eq!(lines(&zoned)[1], format!("|{time1}|"));
}

#[tokio::test]
async fn hidden_columns_stay_out_of_star_describe_and_information_schema() {
    let lake = lake().await;
    two_files(&lake).await;
    assert_eq!(
        lines(&lake.ok("SELECT * FROM hive.s.t WHERE a = 1").await),
        ["|a|b|", "|1|x|"]
    );
    let describe = lake.ok("DESCRIBE hive.s.t").await;
    assert!(!describe.contains('$'), "{describe}");
    let show = lake.ok("SHOW COLUMNS FROM hive.s.t").await;
    assert!(!show.contains('$'), "{show}");
    let columns = lake
        .ok("SELECT column_name FROM hive.information_schema.columns WHERE table_name = 't' ORDER BY 1")
        .await;
    assert_eq!(lines(&columns), ["|column_name|", "|a|", "|b|"]);
    // `*` next to a hidden column, in the order written.
    let both = lake
        .ok("SELECT *, \"$file_size\" > 0 AS sized FROM hive.s.t WHERE a = 1")
        .await;
    assert_eq!(lines(&both), ["|a|b|sized|", "|1|x|true|"]);
}

#[tokio::test]
async fn a_filter_on_path_reads_only_that_file() {
    let lake = lake().await;
    let files = two_files(&lake).await;
    // A file that is no Parquet: a scan that reads it fails.
    std::fs::write(lake.dir("t").join("zzzz_not_parquet"), b"not a parquet file").unwrap();
    let all = lake.err("SELECT count(*) FROM hive.s.t").await;
    assert!(!all.is_empty());
    let second = lake.path("t", &files[1]);
    let one = lake
        .ok(&format!("SELECT a, b FROM hive.s.t WHERE \"$path\" = '{second}'"))
        .await;
    assert_eq!(lines(&one), ["|a|b|", "|3|z|"]);
    let size = lake.file_facts("t", &files[0]).0;
    let by_size_and_path = lake
        .ok(&format!(
            "SELECT count(*) AS n FROM hive.s.t WHERE \"$path\" IN ('{}', '{second}') AND \"$file_size\" = {size}",
            lake.path("t", &files[0])
        ))
        .await;
    assert_eq!(lines(&by_size_and_path), ["|n|", "|2|"]);
    // A filter that mixes a hidden column with a stored one prunes nothing
    // by itself and still answers right.
    let mixed = lake
        .err(&format!(
            "SELECT a FROM hive.s.t WHERE \"$path\" = '{second}' OR a = 1"
        ))
        .await;
    assert!(!mixed.is_empty());
    let none = lake
        .ok("SELECT count(*) AS n FROM hive.s.t WHERE \"$path\" = 'nowhere'")
        .await;
    assert_eq!(lines(&none), ["|n|", "|0|"]);
}

#[tokio::test]
async fn insert_and_optimize_show_the_new_files() {
    let lake = lake().await;
    two_files(&lake).await;
    lake.ok("INSERT INTO hive.s.t SELECT a + 10, \"$path\" FROM hive.s.t WHERE a = 3")
        .await;
    let files = lake.files("t");
    assert_eq!(files.len(), 3);
    let copied = lake.ok("SELECT b, \"$path\" FROM hive.s.t WHERE a = 13").await;
    assert_eq!(
        lines(&copied)[1],
        format!("|{}|{}|", lake.path("t", &files[1]), lake.path("t", &files[2]))
    );
    let mut options = QueryOptions::for_user("dbt");
    options.properties = lake
        .engine
        .session_properties()
        .session(BTreeMap::from([(ENABLED.to_owned(), "true".to_owned())]))
        .unwrap();
    lake.run_with(&options, "ALTER TABLE hive.s.t EXECUTE optimize")
        .await
        .unwrap();
    let after = lake.files("t");
    assert_eq!(after.len(), 1, "{after:?}");
    let (size, time) = lake.file_facts("t", &after[0]);
    let compacted = lake
        .ok(
            "SELECT count(*) AS n, min(\"$path\") AS p, max(\"$path\") AS q, max(\"$file_size\") AS s, \
             max(\"$file_modified_time\") AS m FROM hive.s.t",
        )
        .await;
    let only = lake.path("t", &after[0]);
    assert_eq!(lines(&compacted)[1], format!("|4|{only}|{only}|{size}|{time}|"));
}

#[tokio::test]
async fn views_joins_and_tables_made_from_hidden_columns() {
    let lake = lake().await;
    let files = two_files(&lake).await;
    let first = lake.path("t", &files[0]);
    let second = lake.path("t", &files[1]);
    lake.ok("CREATE VIEW hive.s.v AS SELECT *, \"$path\" AS p FROM hive.s.t")
        .await;
    assert_eq!(
        lines(&lake.ok("SELECT * FROM hive.s.v ORDER BY a").await),
        [
            "|a|b|p|".to_owned(),
            format!("|1|x|{first}|"),
            format!("|2|y|{first}|"),
            format!("|3|z|{second}|"),
        ]
    );
    // A view has no hidden columns of its own.
    let err = lake.err("SELECT \"$path\" FROM hive.s.v").await;
    assert!(err.contains("No field named \"$path\""), "{err}");
    // The file name as views take it apart.
    let parts = lake
        .ok(
            "SELECT regexp_extract(\"$path\", '[^/]+$') AS f, element_at(split(\"$path\", '/'), -1) AS g \
             FROM hive.s.t WHERE a = 3",
        )
        .await;
    assert_eq!(lines(&parts)[1], format!("|{0}|{0}|", files[1]));
    // A self join on the file, with `*` over both sides.
    let joined = lake
        .ok("SELECT * FROM hive.s.t x JOIN hive.s.t y ON x.\"$path\" = y.\"$path\" AND x.a < y.a")
        .await;
    assert_eq!(lines(&joined), ["|a|b|a|b|", "|1|x|2|y|"]);
    let ambiguous = lake
        .err("SELECT \"$path\" FROM hive.s.t x JOIN hive.s.t y ON x.a = y.a")
        .await;
    assert!(ambiguous.contains("$path"), "{ambiguous}");
    // CREATE TABLE AS: plain columns of the hidden columns' types.
    lake.ok("CREATE TABLE hive.s.c AS SELECT \"$path\" AS p, \"$file_size\" AS s FROM hive.s.t WHERE a = 1")
        .await;
    let metadata: serde_json::Value =
        serde_json::from_slice(&std::fs::read(lake.dir("c").join(".trinoSchema")).unwrap()).unwrap();
    assert_eq!(
        metadata["dataColumns"],
        serde_json::json!([
            {"name": "p", "type": "string", "properties": {}},
            {"name": "s", "type": "bigint", "properties": {}},
        ])
    );
    assert_eq!(
        lines(&lake.ok("SELECT * FROM hive.s.c").await)[1],
        format!("|{first}|{}|", lake.file_facts("t", &files[0]).0)
    );
}

#[tokio::test]
async fn partition_and_bucket_are_unknown_on_tables_without_them() {
    let lake = lake().await;
    two_files(&lake).await;
    for column in ["$partition", "$bucket"] {
        let err = lake.err(&format!("SELECT \"{column}\" FROM hive.s.t")).await;
        assert!(err.contains(&format!("No field named \"{column}\"")), "{err}");
    }
    let err = lake.err("SELECT * FROM hive.s.\"t$hidden\"").await;
    assert!(err.contains("not found"), "{err}");
}

#[tokio::test]
async fn path_spells_the_warehouse_as_the_catalog_does() {
    for spelling in [
        (|dir: &FsPath| format!("file://{}", dir.display())) as fn(&FsPath) -> String,
        |dir: &FsPath| dir.display().to_string(),
        |dir: &FsPath| format!("file:{}/", dir.display()),
    ] {
        let lake = lake_spelled(spelling).await;
        lake.ok("CREATE TABLE hive.s.t (a integer)").await;
        lake.ok("INSERT INTO hive.s.t VALUES (1)").await;
        let file = lake.files("t").remove(0);
        let expected = lake.path("t", &file);
        assert!(!expected.contains("//s/"), "{expected}");
        let answer = lake.ok("SELECT \"$path\" FROM hive.s.t").await;
        assert_eq!(lines(&answer)[1], format!("|{expected}|"), "{}", lake.spelled);
    }
}

#[tokio::test]
async fn the_second_name_serves_base_tables_only() {
    let lake = lake().await;
    two_files(&lake).await;
    lake.ok("CREATE VIEW hive.s.v AS SELECT a FROM hive.s.t").await;
    let catalog = lake.catalog();
    let provider = super::catalog::HiveCatalogProvider(Arc::new(catalog));
    let schema = datafusion::catalog::CatalogProvider::schema(&provider, "s").unwrap();
    let plain = schema.table("t").await.unwrap().unwrap();
    let hidden = schema.table("t$hidden").await.unwrap().unwrap();
    let names = |table: &Arc<dyn TableProvider>| {
        table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(names(&plain), ["a", "b"]);
    assert_eq!(
        names(&hidden),
        ["a", "b", "$path", "$file_size", "$file_modified_time"]
    );
    assert!(schema.table("v$hidden").await.unwrap().is_none());
    assert!(schema.table("nope$hidden").await.unwrap().is_none());
    // Read on the coordinator: their values come from the listing.
    let hive = hidden.downcast_ref::<super::HiveTable>().unwrap();
    assert!(!hive.distributable());
    assert!(plain.downcast_ref::<super::HiveTable>().unwrap().distributable());
}

#[test]
fn file_uris_start_with_the_warehouse_as_written() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let file = Path::from("s/t/f1");
    for (warehouse, expected) in [
        ("file:/wh", "file:/wh/s/t/f1"),
        ("file:/wh/", "file:/wh/s/t/f1"),
        ("file:///wh", "file:///wh/s/t/f1"),
        ("/wh", "/wh/s/t/f1"),
        ("s3://lake/wh/", "s3://lake/wh/s/t/f1"),
    ] {
        let metastore = FileMetastore::new(store.clone(), Path::default()).with_location(warehouse);
        assert_eq!(metastore.file_uri(&file), expected, "{warehouse}");
    }
    // Under a prefix (an S3 warehouse's root), the path after it.
    let metastore = FileMetastore::new(store, Path::from("wh")).with_location("s3://lake/wh");
    assert_eq!(
        metastore.file_uri(&Path::from("wh/s/t/f1")),
        "s3://lake/wh/s/t/f1"
    );
}
