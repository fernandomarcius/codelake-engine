//! Independent acceptance suite for F2 (hive catalog compatible with Trino's
//! file metastore, ADR 0007). Written by a tester who did not implement it.
//!
//! Every check compares rows. Expected values are literals written here; the
//! same literals feed both engines, so a value that changes on the way through
//! Parquet shows up as a mismatch.
//!
//! Sharing the warehouse with sibling containers: the tests run inside the dev
//! container (`scripts/dev.sh`), and Trino / SQL Server run as sibling
//! containers, so they cannot see this container's `/tmp`. The suite inspects
//! its own container through the Docker API, finds what is mounted at
//! `/work/target` (the `codelake-target` named volume locally, a host
//! directory in CI) and mounts the same source at the same path in Trino and
//! SQL Server. The warehouse therefore has one absolute path everywhere
//! (`/work/target/hive-acceptance/<run>/warehouse`), which the file metastore
//! needs, and every run removes its directory when done. Outside a container
//! the suite falls back to a host temporary directory bind-mounted at the same
//! path.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::common;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::Engine;
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::util::display::array_value_to_string;
use testcontainers::bollard::Docker;
use testcontainers::bollard::models::MountPointTypeEnum;
use testcontainers::core::{CmdWaitFor, ExecCommand, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;
use tiberius::{AuthMethod, Client, ColumnData, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);
const SA_PASSWORD: &str = "Acceptance-Only-1";
/// Where the dev container mounts the build cache (see `scripts/dev.sh`).
const SHARED_MOUNT: &str = "/work/target";

type Rows = Vec<Vec<String>>;

#[allow(clippy::print_stderr, reason = "evidence for the acceptance report")]
fn note(what: &str, detail: impl std::fmt::Display) {
    eprintln!("[F2] {what}: {detail}");
}

// ---------------------------------------------------------------------------
// Shared warehouse directory.
// ---------------------------------------------------------------------------

/// A directory with the same absolute path here and in sibling containers.
/// Removed on drop.
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

/// What this process's container has mounted at `destination`, if it runs in one.
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
        (Path::new(SHARED_MOUNT).join("hive-acceptance"), mount)
    } else {
        let base = std::env::temp_dir().join("codelake-hive-acceptance");
        let path = base.display().to_string();
        (base, Mount::bind_mount(path.clone(), path))
    };
    let dir = base.join(run);
    std::fs::create_dir_all(dir.join("warehouse")).unwrap();
    Shared { dir, mount }
}

// ---------------------------------------------------------------------------
// CodeLake side.
// ---------------------------------------------------------------------------

async fn codelake(warehouse: &Path, spill: &Path) -> Engine {
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

async fn cl_try(engine: &Engine, sql: &str) -> Result<Rows, String> {
    let frame = engine.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = frame.collect().await.map_err(|e| e.to_string())?;
    Ok(to_rows(&batches))
}

async fn cl(engine: &Engine, sql: &str) -> Rows {
    cl_try(engine, sql)
        .await
        .unwrap_or_else(|e| panic!("CodeLake `{sql}` failed: {e}"))
}

async fn cl_err(engine: &Engine, sql: &str) -> String {
    match cl_try(engine, sql).await {
        Ok(rows) => panic!("CodeLake `{sql}` should fail, returned {rows:?}"),
        Err(err) => err,
    }
}

// ---------------------------------------------------------------------------
// Trino 431 side (CLI inside the container).
// ---------------------------------------------------------------------------

struct Trino(ContainerAsync<GenericImage>);

impl Trino {
    async fn start(shared: &Shared) -> Self {
        let properties = format!(
            "connector.name=hive\n\
             hive.metastore=file\n\
             hive.metastore.catalog.dir=file:{}\n\
             hive.allow-add-column=true\n\
             hive.allow-drop-table=true\n",
            shared.warehouse().display()
        );
        let container = GenericImage::new("trinodb/trino", "431")
            .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
            // Root, so Trino can write into directories this process created.
            .with_user("root")
            .with_mount(shared.mount.clone())
            .with_copy_to("/etc/trino/catalog/hive.properties", properties.into_bytes())
            .with_startup_timeout(STARTUP_TIMEOUT)
            .start()
            .await
            .unwrap();
        let trino = Self(container);
        trino.wait_until_it_runs_queries().await;
        trino
    }

    /// "SERVER STARTED" comes before the coordinator registers itself as a
    /// worker; a query sent in between fails with "No nodes available".
    async fn wait_until_it_runs_queries(&self) {
        let deadline = std::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            match self.try_sql_in(None, "SELECT 1").await {
                Ok(_) => return,
                Err(error) if std::time::Instant::now() < deadline => {
                    assert!(
                        error.contains("No nodes available"),
                        "Trino failed to start: {error}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(error) => panic!("Trino did not accept queries in time: {error}"),
            }
        }
    }

    async fn try_sql_in(&self, schema: Option<&str>, sql: &str) -> Result<Rows, String> {
        // `--debug` prints the Java cause chain of a failure.
        let mut args = vec!["trino", "--debug", "--output-format", "TSV", "--catalog", "hive"];
        if let Some(schema) = schema {
            args.extend(["--schema", schema]);
        }
        args.extend(["--execute", sql]);
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

    async fn try_sql(&self, sql: &str) -> Result<Rows, String> {
        self.try_sql_in(None, sql).await
    }

    async fn sql(&self, sql: &str) -> Rows {
        self.try_sql(sql)
            .await
            .unwrap_or_else(|e| panic!("Trino `{sql}` failed: {e}"))
    }
}

// ---------------------------------------------------------------------------
// The typed rows both engines write and read.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Float,
    Timestamp,
    Binary,
}

struct Col {
    name: &'static str,
    trino: &'static str,
    codelake: &'static str,
    kind: Kind,
}

const COLS: [Col; 14] = [
    Col {
        name: "id",
        trino: "bigint",
        codelake: "BIGINT",
        kind: Kind::Plain,
    },
    Col {
        name: "i",
        trino: "integer",
        codelake: "INT",
        kind: Kind::Plain,
    },
    Col {
        name: "si",
        trino: "smallint",
        codelake: "SMALLINT",
        kind: Kind::Plain,
    },
    Col {
        name: "ti",
        trino: "tinyint",
        codelake: "TINYINT",
        kind: Kind::Plain,
    },
    Col {
        name: "dbl",
        trino: "double",
        codelake: "DOUBLE",
        kind: Kind::Float,
    },
    Col {
        name: "r",
        trino: "real",
        codelake: "REAL",
        kind: Kind::Float,
    },
    Col {
        name: "vc",
        trino: "varchar",
        codelake: "VARCHAR",
        kind: Kind::Plain,
    },
    Col {
        name: "vcn",
        trino: "varchar(10)",
        codelake: "VARCHAR(10)",
        kind: Kind::Plain,
    },
    Col {
        name: "d18",
        trino: "decimal(18,2)",
        codelake: "DECIMAL(18,2)",
        kind: Kind::Plain,
    },
    Col {
        name: "d38",
        trino: "decimal(38,10)",
        codelake: "DECIMAL(38,10)",
        kind: Kind::Plain,
    },
    Col {
        name: "b",
        trino: "boolean",
        codelake: "BOOLEAN",
        kind: Kind::Plain,
    },
    Col {
        name: "dt",
        trino: "date",
        codelake: "DATE",
        kind: Kind::Plain,
    },
    Col {
        name: "ts",
        trino: "timestamp(3)",
        codelake: "TIMESTAMP",
        kind: Kind::Timestamp,
    },
    Col {
        name: "bin",
        trino: "varbinary",
        codelake: "BYTEA",
        kind: Kind::Binary,
    },
];

type Values = [Option<&'static str>; 14];

const ROW_1: Values = [
    Some("1"),
    Some("2147483647"),
    Some("32767"),
    Some("127"),
    Some("3.141592653589793"),
    Some("1.5"),
    Some("ação"),
    Some("Çé ü"),
    Some("12345678.90"),
    Some("1234567890123456789012345678.0123456789"),
    Some("true"),
    Some("2026-01-02"),
    Some("2026-01-01 10:00:00.123"),
    Some("0000000000000000002a"),
];

const ROW_2: Values = [
    Some("2"),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
];

const ROW_3: Values = [
    Some("3"),
    Some("-2147483648"),
    Some("-32768"),
    Some("-128"),
    Some("-0.25"),
    Some("-2.5"),
    Some("São Paulo – àéîõü"),
    Some(""),
    Some("-9999999999999999.99"),
    Some("-0.0000000001"),
    Some("false"),
    Some("1900-03-01"),
    Some("1969-12-31 23:59:59.999"),
    Some("ff00"),
];

#[derive(Clone, Copy)]
enum Dialect {
    Trino,
    CodeLake,
}

fn literal(col: &Col, value: Option<&str>, dialect: Dialect) -> String {
    let ty = match dialect {
        Dialect::Trino => col.trino,
        Dialect::CodeLake => col.codelake,
    };
    match (value, col.kind) {
        (None, _) => format!("CAST(NULL AS {ty})"),
        (Some(v), Kind::Binary) => format!("X'{v}'"),
        (Some(v), _) => format!("CAST('{v}' AS {ty})"),
    }
}

/// `SELECT <typed literals> AS <names>` for one row.
fn select_row(values: &Values, dialect: Dialect) -> String {
    let items: Vec<String> = COLS
        .iter()
        .zip(values)
        .map(|(col, value)| format!("{} AS {}", literal(col, *value, dialect), col.name))
        .collect();
    format!("SELECT {}", items.join(", "))
}

fn with_id(values: &Values, id: &'static str) -> Values {
    let mut out = *values;
    out[0] = Some(id);
    out
}

/// Canonical form both formatters below produce.
fn expected(values: &Values) -> Vec<String> {
    COLS.iter()
        .zip(values)
        .map(|(col, value)| match value {
            None => "NULL".to_owned(),
            Some(v) if col.kind == Kind::Float => format!("~{v}"),
            Some(v) => (*v).to_owned(),
        })
        .collect()
}

/// Trino projection rendering every column as text.
fn trino_projection() -> String {
    COLS.iter()
        .map(|col| {
            let text = match col.kind {
                Kind::Binary => format!("lower(to_hex({}))", col.name),
                _ => format!("CAST({} AS varchar)", col.name),
            };
            format!("coalesce({text}, 'NULL')")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// `CodeLake` projection; NULLs become "NULL" in [`to_rows`].
fn codelake_projection() -> String {
    COLS.iter()
        .map(|col| match col.kind {
            Kind::Timestamp => format!("to_char({}, '%Y-%m-%d %H:%M:%S%.3f')", col.name),
            Kind::Binary => format!("encode({}, 'hex')", col.name),
            _ => col.name.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn same_cell(expected: &str, actual: &str) -> bool {
    match expected.strip_prefix('~') {
        Some(number) => match (number.parse::<f64>(), actual.parse::<f64>()) {
            (Ok(e), Ok(a)) => e.to_bits() == a.to_bits(),
            _ => false,
        },
        None => expected == actual,
    }
}

fn assert_rows(label: &str, actual: &Rows, expected: &[Vec<String>]) {
    let equal = actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(a, e)| a.len() == e.len() && a.iter().zip(e).all(|(x, y)| same_cell(y, x)));
    assert!(
        equal,
        "{label}: rows differ\n  expected: {expected:?}\n  actual:   {actual:?}"
    );
    note(label, format!("{} rows equal", actual.len()));
}

fn strings(rows: &[&[&str]]) -> Rows {
    rows.iter()
        .map(|r| r.iter().map(|s| (*s).to_owned()).collect())
        .collect()
}

// ---------------------------------------------------------------------------
// Files on disk.
// ---------------------------------------------------------------------------

/// Visible data files of a table directory with the CRC-32 of their bytes.
fn data_files(dir: &Path) -> BTreeMap<String, u32> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .filter(|e| e.file_type().unwrap().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| !name.starts_with(['.', '_']))
        .map(|name| {
            let bytes = std::fs::read(dir.join(&name)).unwrap();
            (name, crc32fast::hash(&bytes))
        })
        .collect()
}

/// Every entry below `dir`, relative, recursively (empty when missing).
fn all_entries(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            out.push(path.strip_prefix(dir).unwrap().display().to_string());
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Leftovers of an interrupted write (`.x.inprogress`, `object_store` staging `#n`).
async fn leftovers(dir: &Path) -> Vec<String> {
    // object_store removes an abandoned staging file from a blocking task.
    for _ in 0..20 {
        let found: Vec<String> = all_entries(dir)
            .into_iter()
            .filter(|e| e.contains("inprogress") || e.contains('#'))
            .collect();
        if found.is_empty() {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    all_entries(dir)
        .into_iter()
        .filter(|e| e.contains("inprogress") || e.contains('#'))
        .collect()
}

// ---------------------------------------------------------------------------
// 1. Trino writes, CodeLake reads.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trino_writes_codelake_reads_every_type() {
    let shared = shared().await;
    let trino = Trino::start(&shared).await;
    seed_trino_table(&trino).await;
    let want = vec![expected(&ROW_1), expected(&ROW_2), expected(&ROW_3)];

    // The literals are right: Trino reads back what it wrote.
    let by_trino = trino
        .sql(&format!(
            "SELECT {} FROM hive.t2c.tipos ORDER BY id",
            trino_projection()
        ))
        .await;
    assert_rows("1. Trino reads its own table (control)", &by_trino, &want);

    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    let types = cl(
        &engine,
        "SELECT column_name, data_type FROM hive.information_schema.columns \
         WHERE table_catalog = 'hive' AND table_schema = 't2c' AND table_name = 'tipos' \
         ORDER BY ordinal_position",
    )
    .await;
    note("1. CodeLake column types", format!("{types:?}"));
    let by_codelake = cl(
        &engine,
        &format!("SELECT {} FROM hive.t2c.tipos ORDER BY id", codelake_projection()),
    )
    .await;
    assert_rows("1. CodeLake reads the Trino table", &by_codelake, &want);
}

async fn seed_trino_table(trino: &Trino) {
    trino.sql("CREATE SCHEMA hive.t2c").await;
    trino
        .sql(&format!(
            "CREATE TABLE hive.t2c.tipos WITH (format = 'PARQUET') AS {}",
            select_row(&ROW_1, Dialect::Trino)
        ))
        .await;
    trino
        .sql(&format!(
            "INSERT INTO hive.t2c.tipos {} UNION ALL {}",
            select_row(&ROW_2, Dialect::Trino),
            select_row(&ROW_3, Dialect::Trino)
        ))
        .await;
}

#[tokio::test]
async fn codelake_insert_into_trino_table_keeps_it_readable_by_trino() {
    let shared = shared().await;
    let trino = Trino::start(&shared).await;
    seed_trino_table(&trino).await;
    let want = vec![expected(&ROW_1), expected(&ROW_2), expected(&ROW_3)];
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;

    // Rollback path: CodeLake appends to Trino's table (which carries
    // statistics), Trino's files stay intact and Trino reads everything back.
    let table_dir = shared.warehouse().join("t2c/tipos");
    let before = data_files(&table_dir);
    cl(
        &engine,
        &format!(
            "INSERT INTO hive.t2c.tipos {}",
            select_row(&with_id(&ROW_1, "4"), Dialect::CodeLake)
        ),
    )
    .await;
    let after = data_files(&table_dir);
    assert_eq!(after.len(), before.len() + 1, "{after:?}");
    for (name, crc) in &before {
        assert_eq!(after.get(name), Some(crc), "Trino file {name} rewritten");
    }
    let mut want4 = want.clone();
    want4.push(expected(&with_id(&ROW_1, "4")));
    let back = trino
        .sql(&format!(
            "SELECT {} FROM hive.t2c.tipos ORDER BY id",
            trino_projection()
        ))
        .await;
    assert_rows("1. Trino reads the table after a CodeLake INSERT", &back, &want4);
}

// ---------------------------------------------------------------------------
// 2. CodeLake writes, Trino reads (table, evolved schema, view, checksums).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn codelake_writes_trino_reads_table_and_view() {
    codelake_to_trino(Metadata::AsWritten).await;
}

/// Diagnostic: the same round trip after the test itself rewrites the
/// metadata the way Trino 431 needs it (`writerVersion` 431, empty
/// `columnStatistics` and `serdeParameters`, checksums recomputed). Separates metadata defects from
/// the Parquet data and view SQL, which this variant still checks row by row.
#[tokio::test]
async fn codelake_writes_trino_reads_once_metadata_is_patched() {
    codelake_to_trino(Metadata::PatchedByTest).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Metadata {
    AsWritten,
    PatchedByTest,
}

/// Hadoop `ChecksumFileSystem` side file: `crc\0`, 512 bytes per chunk, CRC-32 per chunk.
fn hadoop_crc(data: &[u8]) -> Vec<u8> {
    let mut out = b"crc\0".to_vec();
    out.extend_from_slice(&512_i32.to_be_bytes());
    for chunk in data.chunks(512) {
        out.extend_from_slice(&crc32fast::hash(chunk).to_be_bytes());
    }
    out
}

fn patch_metadata_for_trino(warehouse: &Path) {
    for entry in all_entries(warehouse) {
        let path = warehouse.join(&entry);
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.ends_with("trinoSchema") {
            continue;
        }
        let mut json: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let object = json.as_object_mut().unwrap();
        object.insert("writerVersion".into(), "431".into());
        if object.contains_key("tableType") {
            for field in ["columnStatistics", "serdeParameters"] {
                object.entry(field).or_insert_with(|| serde_json::json!({}));
            }
        }
        let bytes = serde_json::to_vec_pretty(&json).unwrap();
        std::fs::write(path.with_file_name(format!(".{name}.crc")), hadoop_crc(&bytes)).unwrap();
        std::fs::write(&path, bytes).unwrap();
    }
}

#[allow(clippy::too_many_lines, reason = "one scenario, read top to bottom")]
async fn codelake_to_trino(metadata: Metadata) {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.c2t").await;
    let created = cl(
        &engine,
        &format!(
            "CREATE TABLE hive.c2t.tipos WITH (format = 'PARQUET') AS {} UNION ALL {}",
            select_row(&ROW_1, Dialect::CodeLake),
            select_row(&ROW_2, Dialect::CodeLake)
        ),
    )
    .await;
    assert_eq!(created, strings(&[&["2"]]), "CTAS row count");
    cl(
        &engine,
        &format!(
            "INSERT INTO hive.c2t.tipos {}",
            select_row(&ROW_3, Dialect::CodeLake)
        ),
    )
    .await;
    cl(&engine, "ALTER TABLE hive.c2t.tipos ADD COLUMN extra varchar").await;
    let fourth = with_id(&ROW_1, "4");
    cl(
        &engine,
        &format!(
            "INSERT INTO hive.c2t.tipos {}, 'novo ç' AS extra",
            select_row(&fourth, Dialect::CodeLake)
        ),
    )
    .await;
    cl(
        &engine,
        "CREATE VIEW hive.c2t.v AS SELECT id, vc, d38, ts, bin FROM hive.c2t.tipos WHERE id <> 2",
    )
    .await;

    if metadata == Metadata::PatchedByTest {
        patch_metadata_for_trino(&shared.warehouse());
    }
    let trino = Trino::start(&shared).await;
    let described: Rows = trino
        .sql("DESCRIBE hive.c2t.tipos")
        .await
        .into_iter()
        .map(|r| r.into_iter().take(2).collect())
        .collect();
    let mut want_types: Vec<Vec<String>> = COLS
        .iter()
        // A declared varchar(n) keeps its length, as Trino 431 shows it.
        .map(|c| vec![c.name.to_owned(), c.trino.to_owned()])
        .collect();
    want_types.push(vec!["extra".into(), "varchar".into()]);
    assert_rows("2. Trino DESCRIBE of the CodeLake table", &described, &want_types);

    let table_rows = trino
        .sql(&format!(
            "SELECT {}, coalesce(extra, 'NULL') FROM hive.c2t.tipos ORDER BY id",
            trino_projection()
        ))
        .await;
    let mut want: Vec<Vec<String>> = [ROW_1, ROW_2, ROW_3]
        .iter()
        .map(|r| {
            let mut e = expected(r);
            e.push("NULL".into());
            e
        })
        .collect();
    let mut e4 = expected(&fourth);
    e4.push("novo ç".into());
    want.push(e4);
    assert_rows(
        "2. Trino reads the CodeLake table (new column NULL in old rows)",
        &table_rows,
        &want,
    );

    let view = trino
        .sql(
            "SELECT CAST(id AS varchar), vc, CAST(d38 AS varchar), CAST(ts AS varchar), \
             lower(to_hex(bin)) FROM hive.c2t.v ORDER BY id",
        )
        .await;
    let pick = |r: &Values| -> Vec<String> {
        [0, 6, 9, 12, 13]
            .iter()
            .map(|i| r[*i].unwrap_or("NULL").to_owned())
            .collect()
    };
    assert_rows(
        "2. Trino reads the CodeLake view",
        &view,
        &[pick(&ROW_1), pick(&ROW_3), pick(&fourth)],
    );
    let listed = trino.sql("SHOW TABLES FROM hive.c2t").await;
    assert_rows(
        "2. Trino lists table and view",
        &listed,
        &strings(&[&["tipos"], &["v"]]),
    );

    // Rollback path the other way: Trino appends to the CodeLake table.
    trino
        .sql("INSERT INTO hive.c2t.tipos (id, vc) VALUES (5, 'do trino')")
        .await;
    let back = cl(&engine, "SELECT id, vc, extra FROM hive.c2t.tipos WHERE id = 5").await;
    assert_rows(
        "2. CodeLake reads the row Trino appended",
        &back,
        &strings(&[&["5", "do trino", "NULL"]]),
    );
}

// ---------------------------------------------------------------------------
// 3. One new file per INSERT, old files untouched.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_appends_exactly_one_file_and_never_rewrites() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.app").await;
    cl(
        &engine,
        "CREATE TABLE hive.app.t WITH (format = 'PARQUET') AS SELECT CAST(0 AS BIGINT) AS id, CAST('zero' AS VARCHAR) AS s",
    )
    .await;
    let dir = shared.warehouse().join("app/t");
    let mut files = data_files(&dir);
    assert_eq!(files.len(), 1, "CTAS writes one file: {files:?}");

    let inserts = [
        "INSERT INTO hive.app.t VALUES (1, 'um')",
        "INSERT INTO hive.app.t SELECT value, 'gerada' FROM generate_series(10, 20000)",
        // Two input partitions (union with a scan of the table itself).
        "INSERT INTO hive.app.t SELECT value + 100000, 'uniao' FROM generate_series(1, 300000) \
         UNION ALL SELECT id + 1000000, s FROM hive.app.t",
        "INSERT INTO hive.app.t VALUES (2, 'dois'), (3, 'três')",
    ];
    for (n, sql) in inserts.iter().enumerate() {
        cl(&engine, sql).await;
        let after = data_files(&dir);
        assert_eq!(
            after.len(),
            files.len() + 1,
            "INSERT #{n} adds one file: {after:?}"
        );
        for (name, crc) in &files {
            assert_eq!(after.get(name), Some(crc), "INSERT #{n} rewrote {name}");
        }
        let newest = after.keys().find(|k| !files.contains_key(*k)).unwrap();
        assert!(
            files.keys().all(|old| old < newest),
            "new file {newest} does not sort after {:?}",
            files.keys().collect::<Vec<_>>()
        );
        files = after;
    }
    assert_eq!(files.len(), inserts.len() + 1);

    let zero = cl(
        &engine,
        "INSERT INTO hive.app.t SELECT * FROM hive.app.t WHERE id < 0",
    )
    .await;
    assert_eq!(zero, strings(&[&["0"]]));
    assert_eq!(data_files(&dir), files, "INSERT of 0 rows must not create a file");

    let hidden: Vec<String> = all_entries(&dir)
        .into_iter()
        .filter(|e| e.starts_with('.'))
        .filter(|e| {
            !matches!(
                e.as_str(),
                ".trinoSchema" | "..trinoSchema.crc" | ".trinoPermissions"
            ) && !e.starts_with(".trinoPermissions/")
        })
        .collect();
    assert!(hidden.is_empty(), "unexpected hidden files: {hidden:?}");
    let count = cl(&engine, "SELECT count(*), count(DISTINCT id) FROM hive.app.t").await;
    // seed 1 + 1 + 19991, then 300000 + those 19993, then 2.
    assert_eq!(count, strings(&[&["339988", "339988"]]));
    note(
        "3. files after 4 inserts",
        format!("{:?}", files.keys().collect::<Vec<_>>()),
    );
}

// ---------------------------------------------------------------------------
// 4. Views: restart, and across engines.
// ---------------------------------------------------------------------------

async fn seed_view_base(engine: &Engine) {
    cl(engine, "CREATE SCHEMA hive.vw").await;
    cl(
        engine,
        "CREATE TABLE hive.vw.base WITH (format = 'PARQUET') AS \
         SELECT CAST(1 AS BIGINT) AS id, 'ação' AS nome, CAST('10.50' AS DECIMAL(18,2)) AS valor \
         UNION ALL SELECT CAST(2 AS BIGINT), 'b', CAST('1.00' AS DECIMAL(18,2)) \
         UNION ALL SELECT CAST(3 AS BIGINT), 'ç', CAST('-7.25' AS DECIMAL(18,2))",
    )
    .await;
}

#[tokio::test]
async fn views_survive_restart_and_cross_engines() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    seed_view_base(&engine).await;
    cl(
        &engine,
        "CREATE VIEW hive.vw.cl_v AS SELECT id, nome, valor FROM hive.vw.base WHERE id >= 2",
    )
    .await;
    // Metadata defects are covered by codelake_writes_trino_reads_table_and_view;
    // patch them here so this test measures only its own subject.
    patch_metadata_for_trino(&shared.warehouse());
    let trino = Trino::start(&shared).await;
    trino
        .sql(
            "CREATE VIEW hive.vw.tr_v AS SELECT id, upper(nome) AS nome_up, \
             CAST(valor AS decimal(38,10)) AS valor38, CAST(nome AS varchar(20)) AS nome20 \
             FROM hive.vw.base WHERE id <= 2",
        )
        .await;

    // Restart: a new engine over the same warehouse.
    drop(engine);
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    let cl_v = cl(&engine, "SELECT id, nome, valor FROM hive.vw.cl_v ORDER BY id").await;
    let want_cl = strings(&[&["2", "b", "1.00"], &["3", "ç", "-7.25"]]);
    assert_rows("4. CodeLake view after restart", &cl_v, &want_cl);
    let tr_v = cl(
        &engine,
        "SELECT id, nome_up, valor38, nome20 FROM hive.vw.tr_v ORDER BY id",
    )
    .await;
    assert_rows(
        "4. Trino view read by CodeLake after restart",
        &tr_v,
        &strings(&[
            &["1", "AÇÃO", "10.5000000000", "ação"],
            &["2", "B", "1.0000000000", "b"],
        ]),
    );
    let listed = cl(
        &engine,
        "SELECT table_name, table_type FROM hive.information_schema.tables \
         WHERE table_catalog = 'hive' AND table_schema = 'vw' ORDER BY table_name",
    )
    .await;
    assert_rows(
        "4. information_schema after restart",
        &listed,
        &strings(&[&["base", "BASE TABLE"], &["cl_v", "VIEW"], &["tr_v", "VIEW"]]),
    );
    let by_trino = trino
        .sql("SELECT CAST(id AS varchar), nome, CAST(valor AS varchar) FROM hive.vw.cl_v ORDER BY id")
        .await;
    assert_rows("4. CodeLake view read by Trino", &by_trino, &want_cl);
}

#[tokio::test]
async fn trino_view_with_unqualified_names_reads_in_codelake() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    seed_view_base(&engine).await;
    // Metadata defects are covered by codelake_writes_trino_reads_table_and_view;
    // patch them here so this test measures only its own subject.
    patch_metadata_for_trino(&shared.warehouse());
    let trino = Trino::start(&shared).await;
    // dbt and people write views relative to the session catalog/schema;
    // Trino stores that catalog/schema next to the SQL.
    trino
        .try_sql_in(
            Some("vw"),
            "CREATE VIEW rel_v AS SELECT id, nome FROM base WHERE id = 1",
        )
        .await
        .unwrap();
    let by_trino = trino
        .sql("SELECT CAST(id AS varchar), nome FROM hive.vw.rel_v")
        .await;
    assert_rows(
        "4b. Trino reads its relative view",
        &by_trino,
        &strings(&[&["1", "ação"]]),
    );
    let rows = cl_try(&engine, "SELECT id, nome FROM hive.vw.rel_v").await;
    note("4b. CodeLake reading the relative view", format!("{rows:?}"));
    assert_rows(
        "4b. CodeLake reads the relative Trino view",
        &rows.unwrap_or_else(|e| panic!("{e}")),
        &strings(&[&["1", "ação"]]),
    );
}

// ---------------------------------------------------------------------------
// 5. SQL Server 2022 loads CodeLake Parquet with OPENROWSET.
// ---------------------------------------------------------------------------

async fn start_mssql(shared: &Shared) -> ContainerAsync<MssqlServer> {
    MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag("2022-latest")
        .with_mount(shared.mount.clone())
        .with_startup_timeout(STARTUP_TIMEOUT)
        .start()
        .await
        .unwrap()
}

fn tds_cell(value: &ColumnData<'_>) -> String {
    match value {
        ColumnData::String(Some(s)) => s.to_string(),
        ColumnData::String(None) => "NULL".to_owned(),
        other => format!("{other:?}"),
    }
}

async fn mssql_query(port: u16, sql: &str) -> Result<Rows, String> {
    let mut config = Config::new();
    config.host(common::host());
    config.port(port);
    config.authentication(AuthMethod::sql_server("sa", SA_PASSWORD));
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| e.to_string())?;
    let mut client = Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| e.to_string())?;
    let rows = client
        .simple_query(sql)
        .await
        .map_err(|e| e.to_string())?
        .into_first_result()
        .await
        .map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| row.into_iter().map(|c| tds_cell(&c)).collect())
        .collect())
}

/// The sink's query for one file: every column rendered as text by SQL Server.
fn openrowset(path: &Path, columns: &[(&str, &str)]) -> String {
    let items: Vec<String> = columns
        .iter()
        .map(|(name, render)| {
            let text = match *render {
                "ts" => format!("CONVERT(NVARCHAR(23), {name}, 121)"),
                "bin" => format!("LOWER(CONVERT(NVARCHAR(400), {name}, 2))"),
                "float" => format!("CONVERT(NVARCHAR(60), CAST({name} AS FLOAT), 3)"),
                _ => format!("CONVERT(NVARCHAR(400), {name})"),
            };
            format!("ISNULL({text}, N'NULL')")
        })
        .collect();
    format!(
        "SELECT {} FROM OPENROWSET(BULK N'{}', FORMAT = 'PARQUET') AS r ORDER BY {}",
        items.join(", "),
        path.display(),
        columns[0].0
    )
}

fn only_data_file(dir: &Path) -> PathBuf {
    let files = data_files(dir);
    assert_eq!(files.len(), 1, "{files:?}");
    dir.join(files.keys().next().unwrap())
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one scenario, read top to bottom")]
async fn sqlserver_2022_openrowset_reads_codelake_parquet() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.sink").await;
    cl(
        &engine,
        &format!(
            "CREATE TABLE hive.sink.carga WITH (format = 'PARQUET') AS \
             SELECT id, vc, d18, d38, ts, bin FROM ({} UNION ALL {} UNION ALL {})",
            select_row(&ROW_1, Dialect::CodeLake),
            select_row(&ROW_2, Dialect::CodeLake),
            select_row(&ROW_3, Dialect::CodeLake)
        ),
    )
    .await;
    let table_dir = shared.warehouse().join("sink/carga");
    let codelake_file = only_data_file(&table_dir);

    // Control: a Parquet file written by a real Trino 431 (INT96 timestamps).
    let control = shared.dir.join("trino431-control.parquet");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse/pilot/t1");
    let fixture_file = data_files(&fixture).into_keys().min().unwrap();
    std::fs::copy(fixture.join(fixture_file), &control).unwrap();

    let mssql = start_mssql(&shared).await;
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    let version = mssql_query(port, "SELECT CAST(@@VERSION AS NVARCHAR(120))")
        .await
        .unwrap();
    note("5. SQL Server", &version[0][0]);

    let control_rows = mssql_query(
        port,
        &openrowset(
            &control,
            &[
                ("id", "text"),
                ("nome", "text"),
                ("valor", "text"),
                ("ts", "ts"),
                ("lsn", "bin"),
            ],
        ),
    )
    .await;
    note(
        "5. OPENROWSET over the Trino control file",
        format!("{control_rows:?}"),
    );

    // How the sink finds files: list the directory, skip dotfiles.
    let listed = mssql_query(
        port,
        &format!(
            "SELECT CAST(file_or_directory_name AS NVARCHAR(400)) FROM sys.dm_os_enumerate_filesystem(N'{}', N'*') \
             WHERE is_directory = 0 AND file_or_directory_name NOT LIKE '.%'",
            table_dir.display()
        ),
    )
    .await;
    note("5. files the sink would load", format!("{listed:?}"));

    // The types SQL Server infers for the CodeLake file (the sink loads by them).
    let inferred = mssql_query(
        port,
        &format!(
            "SELECT CAST(name AS NVARCHAR(100)), CAST(system_type_name AS NVARCHAR(100)) \
             FROM sys.dm_exec_describe_first_result_set(N'SELECT * FROM OPENROWSET(BULK N''{}'', \
             FORMAT = ''PARQUET'') AS r', NULL, 0)",
            codelake_file.display()
        ),
    )
    .await;
    note(
        "5. types SQL Server infers for the CodeLake file",
        format!("{inferred:?}"),
    );

    let rows = mssql_query(
        port,
        &openrowset(
            &codelake_file,
            &[
                ("id", "text"),
                ("vc", "text"),
                ("d18", "text"),
                ("d38", "text"),
                ("ts", "ts"),
                ("bin", "bin"),
            ],
        ),
    )
    .await;
    let pick = |r: &Values| -> Vec<String> {
        [0, 6, 8, 9, 12, 13]
            .iter()
            .map(|i| r[*i].unwrap_or("NULL").to_owned())
            .collect()
    };
    assert_rows(
        "5. SQL Server OPENROWSET reads the CodeLake file",
        &rows.unwrap_or_else(|e| panic!("OPENROWSET over the CodeLake file failed: {e}")),
        &[pick(&ROW_1), pick(&ROW_2), pick(&ROW_3)],
    );
    assert_rows(
        "5. SQL Server OPENROWSET reads the Trino control file",
        &control_rows.unwrap(),
        &strings(&[&[
            "1",
            "ação",
            "10.50",
            "2026-01-01 10:00:00.123",
            "0000000000000000002a",
        ]]),
    );
    let file_name = codelake_file.file_name().unwrap().to_string_lossy().into_owned();
    assert_eq!(listed.unwrap(), vec![vec![file_name]], "sink listing");
}

#[tokio::test]
async fn sqlserver_2022_openrowset_reads_every_codelake_type() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.sink").await;
    cl(
        &engine,
        &format!(
            "CREATE TABLE hive.sink.tudo AS {} UNION ALL {} UNION ALL {}",
            select_row(&ROW_1, Dialect::CodeLake),
            select_row(&ROW_2, Dialect::CodeLake),
            select_row(&ROW_3, Dialect::CodeLake)
        ),
    )
    .await;
    let file = only_data_file(&shared.warehouse().join("sink/tudo"));
    let mssql = start_mssql(&shared).await;
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    let columns: Vec<(&str, &str)> = COLS
        .iter()
        .map(|c| {
            let render = match c.kind {
                Kind::Float => "float",
                Kind::Timestamp => "ts",
                Kind::Binary => "bin",
                Kind::Plain => "text",
            };
            (c.name, render)
        })
        .collect();
    let rows = mssql_query(port, &openrowset(&file, &columns)).await;
    note("5b. OPENROWSET over every type", format!("{rows:?}"));
    // SQL Server renders booleans as bit (1/0).
    let sqlserver = |r: &Values| -> Vec<String> {
        let mut e = expected(r);
        for v in &mut e {
            match v.as_str() {
                "true" => "1".clone_into(v),
                "false" => "0".clone_into(v),
                _ => {}
            }
        }
        e
    };
    assert_rows(
        "5b. SQL Server reads every CodeLake type",
        &rows.unwrap_or_else(|e| panic!("{e}")),
        &[sqlserver(&ROW_1), sqlserver(&ROW_2), sqlserver(&ROW_3)],
    );
}

// ---------------------------------------------------------------------------
// 6. Failures leave nothing behind and are reported.
// ---------------------------------------------------------------------------

/// Fails with a division by zero at row 1 400 000 of 1 500 000.
const FAILING_SELECT: &str = "SELECT value AS id, lower(to_hex(md5(to_utf8(CAST(value AS VARCHAR))))) AS h, \
     CAST(1 AS BIGINT) / (value - 1400000) AS q FROM generate_series(1, 1500000)";

#[tokio::test]
async fn failures_leave_nothing_visible_and_are_errors() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.f").await;

    // The SELECT fails after ~10 row groups (~50 MB) went to the writer, well
    // past its in-memory buffer, so part of the file is already on disk.
    let err = cl_err(
        &engine,
        &format!("CREATE TABLE hive.f.bad WITH (format = 'PARQUET') AS {FAILING_SELECT}"),
    )
    .await;
    note("6. failing CTAS", &err);
    assert!(err.to_lowercase().contains("divide by zero"), "{err}");
    let visible = cl_err(&engine, "SELECT * FROM hive.f.bad").await;
    assert!(
        visible.contains("not found") || visible.contains("does not exist"),
        "{visible}"
    );
    let tables = cl(
        &engine,
        "SELECT table_name FROM hive.information_schema.tables WHERE table_catalog = 'hive' AND table_schema = 'f'",
    )
    .await;
    assert!(tables.is_empty(), "{tables:?}");
    let bad_dir = shared.warehouse().join("f/bad");
    assert_eq!(
        leftovers(&bad_dir).await,
        Vec::<String>::new(),
        "partial files left"
    );
    let entries = all_entries(&bad_dir);
    note(
        "6. table directory after failed CTAS",
        format!("exists={} entries={entries:?}", bad_dir.exists()),
    );
    assert!(entries.is_empty(), "failed CTAS left {entries:?}");

    let missing = cl_err(&engine, "DROP TABLE hive.f.nope").await;
    note("6. DROP of a missing table", &missing);
    cl(&engine, "DROP TABLE IF EXISTS hive.f.nope").await;
    let missing_view = cl_err(&engine, "DROP VIEW hive.f.nope").await;
    note("6. DROP VIEW of a missing view", &missing_view);
    cl(&engine, "DROP VIEW IF EXISTS hive.f.nope").await;

    let no_schema = cl_err(&engine, "CREATE TABLE hive.nosuch.t AS SELECT 1 AS a").await;
    note("6. CREATE TABLE without schema", &no_schema);
    assert!(no_schema.contains("nosuch"), "{no_schema}");
    assert!(
        !shared.warehouse().join("nosuch").exists(),
        "a directory was created for a missing schema"
    );
    let no_schema_view = cl_err(&engine, "CREATE VIEW hive.nosuch.v AS SELECT 1 AS a").await;
    note("6. CREATE VIEW without schema", &no_schema_view);

    let orc = cl_err(
        &engine,
        "CREATE TABLE hive.f.orc WITH (format = 'ORC') AS SELECT 1 AS a",
    )
    .await;
    note("6. format ORC", &orc);
    assert!(orc.contains("ORC"), "{orc}");
    assert!(cl_try(&engine, "SELECT * FROM hive.f.orc").await.is_err());
    assert!(all_entries(&shared.warehouse().join("f/orc")).is_empty());

    cl(&engine, "CREATE SCHEMA IF NOT EXISTS hive.f").await;
    let twice = cl_err(&engine, "CREATE SCHEMA hive.f").await;
    note("6. CREATE SCHEMA twice", &twice);
    cl(&engine, "CREATE TABLE hive.f.ok AS SELECT 1 AS a").await;
    let again = cl_err(&engine, "CREATE TABLE hive.f.ok AS SELECT 2 AS a").await;
    note("6. CTAS over an existing table", &again);
    assert_eq!(cl(&engine, "SELECT a FROM hive.f.ok").await, strings(&[&["1"]]));
}

#[tokio::test]
async fn trino_can_recreate_a_table_codelake_dropped_or_failed_to_create() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.re").await;
    cl(
        &engine,
        "CREATE TABLE hive.re.dropped AS SELECT CAST(1 AS BIGINT) AS a",
    )
    .await;
    cl(&engine, "DROP TABLE hive.re.dropped").await;
    let _ = cl_err(
        &engine,
        &format!("CREATE TABLE hive.re.failed AS {FAILING_SELECT}"),
    )
    .await;
    note(
        "6b. directories left",
        format!(
            "dropped: exists={} {:?}; failed: exists={} {:?}",
            shared.warehouse().join("re/dropped").exists(),
            all_entries(&shared.warehouse().join("re/dropped")),
            shared.warehouse().join("re/failed").exists(),
            all_entries(&shared.warehouse().join("re/failed")),
        ),
    );
    // Metadata defects are covered by codelake_writes_trino_reads_table_and_view;
    // patch them here so this test measures only its own subject.
    patch_metadata_for_trino(&shared.warehouse());
    let trino = Trino::start(&shared).await;
    // dbt's full refresh (and a rollback to Trino) recreate the same names.
    let dropped = trino
        .try_sql("CREATE TABLE hive.re.dropped AS SELECT CAST(2 AS bigint) AS a")
        .await;
    let failed = trino
        .try_sql("CREATE TABLE hive.re.failed AS SELECT CAST(3 AS bigint) AS a")
        .await;
    note("6b. Trino CTAS after CodeLake DROP TABLE", format!("{dropped:?}"));
    note("6b. Trino CTAS after CodeLake failed CTAS", format!("{failed:?}"));
    assert!(dropped.is_ok() && failed.is_ok(), "{dropped:?} / {failed:?}");
}

// ---------------------------------------------------------------------------
// 7. Concurrent INSERT statements.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_write_separate_files_and_lose_nothing() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    let other = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.cc").await;
    cl(
        &engine,
        "CREATE TABLE hive.cc.t AS SELECT CAST(0 AS BIGINT) AS id, 'seed' AS origem",
    )
    .await;
    let dir = shared.warehouse().join("cc/t");
    assert_eq!(data_files(&dir).len(), 1);

    // Same engine, two statements at once.
    let (a, b) = tokio::join!(
        cl(
            &engine,
            "INSERT INTO hive.cc.t SELECT value, 'a' FROM generate_series(1, 50000)"
        ),
        cl(
            &engine,
            "INSERT INTO hive.cc.t SELECT value + 100000, 'b' FROM generate_series(1, 50000)"
        ),
    );
    assert_eq!((a, b), (strings(&[&["50000"]]), strings(&[&["50000"]])));
    assert_eq!(data_files(&dir).len(), 3);

    // Two engines (two processes in production) at once, several rounds.
    for round in 0..3_i64 {
        let base = 1_000_000 * (round + 1);
        let sql_c =
            format!("INSERT INTO hive.cc.t SELECT value + {base}, 'c' FROM generate_series(1, 20000)");
        let sql_d = format!(
            "INSERT INTO hive.cc.t SELECT value + {}, 'd' FROM generate_series(1, 20000)",
            base + 500_000
        );
        let (c, d) = tokio::join!(cl(&engine, &sql_c), cl(&other, &sql_d));
        assert_eq!((c, d), (strings(&[&["20000"]]), strings(&[&["20000"]])));
    }
    let files = data_files(&dir);
    assert_eq!(files.len(), 3 + 6, "{files:?}");
    let counts = cl(
        &engine,
        "SELECT origem, count(*), count(DISTINCT id) FROM hive.cc.t GROUP BY origem ORDER BY origem",
    )
    .await;
    assert_rows(
        "7. rows per writer",
        &counts,
        &strings(&[
            &["a", "50000", "50000"],
            &["b", "50000", "50000"],
            &["c", "60000", "60000"],
            &["d", "60000", "60000"],
            &["seed", "1", "1"],
        ]),
    );
    assert_eq!(leftovers(&dir).await, Vec::<String>::new());
}

// ---------------------------------------------------------------------------
// Beyond the DoD.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn insert_with_column_list_in_another_order() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.ord").await;
    cl(
        &engine,
        "CREATE TABLE hive.ord.t AS SELECT CAST(1 AS BIGINT) AS a, CAST('um' AS VARCHAR) AS b, CAST('1.50' AS DECIMAL(18,2)) AS c",
    )
    .await;
    cl(
        &engine,
        "INSERT INTO hive.ord.t (c, b, a) VALUES (2.25, 'dois', 2)",
    )
    .await;
    cl(&engine, "INSERT INTO hive.ord.t (b, a) VALUES ('três', 3)").await;
    let rows = cl(&engine, "SELECT a, b, c FROM hive.ord.t ORDER BY a").await;
    assert_rows(
        "extra: INSERT with reordered / partial column list",
        &rows,
        &strings(&[
            &["1", "um", "1.50"],
            &["2", "dois", "2.25"],
            &["3", "três", "NULL"],
        ]),
    );
}

#[tokio::test]
async fn trino_partitioned_table_is_a_clear_error() {
    let shared = shared().await;
    let trino = Trino::start(&shared).await;
    trino.sql("CREATE SCHEMA hive.pt").await;
    trino
        .sql(
            "CREATE TABLE hive.pt.part (id bigint, p varchar) \
             WITH (format = 'PARQUET', partitioned_by = ARRAY['p'])",
        )
        .await;
    trino
        .sql("INSERT INTO hive.pt.part VALUES (1, 'x'), (2, 'y')")
        .await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    let read = cl_err(&engine, "SELECT * FROM hive.pt.part").await;
    note("extra: SELECT from a partitioned Trino table", &read);
    assert!(read.to_lowercase().contains("partition"), "{read}");
    let write = cl_err(&engine, "INSERT INTO hive.pt.part VALUES (3, 'z')").await;
    note("extra: INSERT into a partitioned Trino table", &write);
    assert!(write.to_lowercase().contains("partition"), "{write}");
    let dir = shared.warehouse().join("pt/part");
    let stray: Vec<String> = all_entries(&dir)
        .into_iter()
        .filter(|e| !e.starts_with('.') && !e.starts_with("p="))
        .collect();
    assert!(stray.is_empty(), "files written outside partitions: {stray:?}");
    let back = trino.sql("SELECT count(*) FROM hive.pt.part").await;
    assert_eq!(back, strings(&[&["2"]]));
}

#[tokio::test]
async fn mixed_case_table_name_stays_visible_to_trino() {
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let engine = codelake(&shared.warehouse(), spill.path()).await;
    cl(&engine, "CREATE SCHEMA hive.mc").await;
    // Unquoted: folded to lower case, like Trino.
    cl(
        &engine,
        "CREATE TABLE hive.mc.Pedidos AS SELECT CAST(1 AS BIGINT) AS Id",
    )
    .await;
    // Quoted: Trino folds every identifier to lower case, so a table stored as
    // `MixedCase` cannot be reached from Trino. CodeLake must refuse it or fold it.
    let quoted = cl_try(
        &engine,
        "CREATE TABLE hive.mc.\"MixedCase\" AS SELECT CAST(2 AS BIGINT) AS id",
    )
    .await;
    note("extra: CTAS with a quoted mixed-case name", format!("{quoted:?}"));
    note(
        "extra: warehouse entries",
        format!("{:?}", all_entries(&shared.warehouse().join("mc"))),
    );
    // Metadata defects are covered by codelake_writes_trino_reads_table_and_view;
    // patch them here so this test measures only its own subject.
    patch_metadata_for_trino(&shared.warehouse());
    let trino = Trino::start(&shared).await;
    let unquoted = trino.sql("SELECT CAST(id AS varchar) FROM hive.mc.pedidos").await;
    assert_eq!(unquoted, strings(&[&["1"]]));
    if quoted.is_ok() {
        let tables = trino.sql("SHOW TABLES FROM hive.mc").await;
        let read = trino
            .try_sql("SELECT CAST(id AS varchar) FROM hive.mc.\"MixedCase\"")
            .await;
        note(
            "extra: Trino view of the mixed-case table",
            format!("{tables:?} / {read:?}"),
        );
        assert_eq!(
            read,
            Ok(strings(&[&["2"]])),
            "CodeLake created a table Trino cannot read"
        );
    }
}
