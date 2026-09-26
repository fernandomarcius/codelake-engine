//! Trino file metastore: JSON metadata files laid out exactly like Trino 431
//! writes them (see `tests/fixtures/trino431/README.md`).
//!
//! ```text
//! <root>/.<schema>.trinoSchema            schema metadata (+ ..<schema>.trinoSchema.crc)
//! <root>/<schema>/<table>/.trinoSchema     table or view metadata (+ ..trinoSchema.crc)
//! <root>/<schema>/<table>/.trinoPermissions/user_<owner>
//! <root>/<schema>/<table>/<data files>     Parquet, no extension
//! ```
//!
//! Unknown JSON fields are preserved on rewrite (`extra`), so metadata written
//! by a newer Trino survives a round trip through `CodeLake`.
//!
//! Checksums: when a metadata file has its Hadoop `.crc`, reads verify it, as
//! Trino does ("Checksum error"): a file changed or cut short is an error,
//! never columns read as NULL.
//!
//! Writers: every change of one table's metadata, and every commit of new
//! files, runs under the table's metadata lock ([`FileMetastore::lock_metadata`]:
//! a `tokio` mutex in this process, an advisory `flock` across processes on a
//! local warehouse). New tables and new data files are written under
//! `.codelake/staging/<id>/` at the warehouse root (hidden from Trino, the
//! sink and scans) and published by a rename under that lock, after checking
//! that the target is still what the statement expects. So two `CREATE
//! TABLE` of one name never mix rows, an `ALTER TABLE ADD COLUMN` is never
//! lost to a concurrent writer, and a statement that fails leaves nothing
//! visible and removes only what it wrote.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use codelake_core::config::HiveTimestampPrecision;
use codelake_core::parquet_time_zone::ParquetTimeZone;
use codelake_core::statement_policy::is_valid_object_name;

use super::crc::{crc_name, hadoop_crc};

pub(crate) const MANAGED_TABLE: &str = "MANAGED_TABLE";
pub(crate) const VIRTUAL_VIEW: &str = "VIRTUAL_VIEW";
/// Data lives at `externalLocation`, owned by someone else: dropping the table
/// removes only its metadata.
#[cfg(test)]
const EXTERNAL_TABLE: &str = "EXTERNAL_TABLE";
/// Table parameter: the identity of a table `CREATE OR REPLACE` wrote. Two
/// handles of one name with different identities are different tables, so
/// a scan or an `INSERT` planned against the table that was replaced never
/// reads or writes the new one (ISS-017).
pub(crate) const TABLE_ID: &str = "codelake.table_id";
/// Table, schema and column property: the name as the statement that
/// created (or renamed) it wrote it, when that is not its folded form
/// (ISS-018). The metastore's names stay folded, as Trino writes and needs
/// them (its `INSERT` fails on a column name that is not lower case); the
/// directory, and every lookup, use the folded name; the files a table's
/// writes produce spell each column as written.
pub(crate) const WRITTEN_NAME: &str = "codelake.name";
pub(crate) const METADATA_FILE: &str = ".trinoSchema";
const PERMISSIONS_DIR: &str = ".trinoPermissions";
/// Directory of `CodeLake`'s own files at the warehouse root: locks, staging
/// of new tables and files. Trino lists
/// schemas by their `.<schema>.trinoSchema` files and ignores it.
const CODELAKE_DIR: &str = ".codelake";
/// How many times a read retries a metadata file whose checksum does not
/// match: a writer replaces the checksum first and the file right after.
const CHECKSUM_RETRIES: u32 = 10;
/// Trino's file metastore refuses metadata whose `writerVersion` is not its own
/// version ("File metastore provides no compatibility for metadata written with
/// a different version"). `CodeLake` shares the warehouse with Trino 431, so new
/// metadata carries 431; rewrites keep the version already in the file.
pub(crate) const TRINO_VERSION: &str = "431";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ColumnMetadata {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) hive_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) comment: Option<String>,
    #[serde(default)]
    pub(crate) properties: BTreeMap<String, String>,
    /// Fields this version does not model, kept on rewrite.
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

impl ColumnMetadata {
    /// The column's name as it was written (ISS-018): its [`WRITTEN_NAME`]
    /// property, or else its metastore name. Files are written with it.
    pub(crate) fn written_name(&self) -> &str {
        self.properties
            .get(WRITTEN_NAME)
            .filter(|written| written.to_lowercase() == self.name.to_lowercase())
            .map_or(self.name.as_str(), String::as_str)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TableMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) writer_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    pub(crate) table_type: String,
    #[serde(default)]
    pub(crate) data_columns: Vec<ColumnMetadata>,
    #[serde(default)]
    pub(crate) partition_columns: Vec<ColumnMetadata>,
    #[serde(default)]
    pub(crate) parameters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) storage_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) external_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) view_original_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) view_expanded_text: Option<String>,
    /// Always written: Trino 431 rejects metadata without it
    /// (`serdeParameters is null`).
    #[serde(default)]
    pub(crate) serde_parameters: BTreeMap<String, String>,
    /// Always written: Trino 431 rejects metadata without it
    /// (`columnStatistics is null`). Values are kept as Trino wrote them.
    #[serde(default)]
    pub(crate) column_statistics: BTreeMap<String, serde_json::Value>,
    /// Fields this version does not model (`originalStorageFormat`,
    /// `bucketProperty`, …), kept on rewrite.
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

impl TableMetadata {
    pub(crate) fn is_view(&self) -> bool {
        self.table_type == VIRTUAL_VIEW
    }

    /// New managed Parquet table, the only kind `CodeLake` creates. Same fields
    /// as Trino 431 writes for `CREATE TABLE … WITH (format = 'PARQUET')`;
    /// statistics start unknown (Trino then treats them as missing).
    pub(crate) fn managed_parquet(owner: &str, columns: Vec<ColumnMetadata>) -> Self {
        Self {
            writer_version: Some(TRINO_VERSION.into()),
            owner: Some(owner.into()),
            table_type: MANAGED_TABLE.into(),
            data_columns: columns,
            partition_columns: Vec::new(),
            parameters: BTreeMap::from([
                ("presto_version".into(), TRINO_VERSION.into()),
                ("transactional".into(), "false".into()),
                ("auto.purge".into(), "false".into()),
            ]),
            storage_format: Some("PARQUET".into()),
            external_location: None,
            view_original_text: None,
            view_expanded_text: None,
            serde_parameters: BTreeMap::new(),
            column_statistics: BTreeMap::new(),
            extra: BTreeMap::new(),
        }
    }

    /// New Trino view (`VIRTUAL_VIEW`), laid out as Trino 431 writes it.
    pub(crate) fn trino_view(owner: &str, original_text: String) -> Self {
        Self {
            writer_version: Some(TRINO_VERSION.into()),
            owner: Some(owner.into()),
            table_type: VIRTUAL_VIEW.into(),
            data_columns: vec![ColumnMetadata {
                name: "dummy".into(),
                hive_type: "string".into(),
                comment: None,
                properties: BTreeMap::new(),
                extra: BTreeMap::new(),
            }],
            partition_columns: Vec::new(),
            parameters: BTreeMap::from([
                ("comment".into(), "Presto View".into()),
                ("presto_view".into(), "true".into()),
                ("presto_version".into(), TRINO_VERSION.into()),
            ]),
            storage_format: None,
            external_location: None,
            view_original_text: Some(original_text),
            view_expanded_text: Some("/* Presto View */".into()),
            serde_parameters: BTreeMap::new(),
            column_statistics: BTreeMap::new(),
            extra: BTreeMap::from([("originalStorageFormat".into(), serde_json::json!({}))]),
        }
    }

    /// The identity [`TABLE_ID`] records, if any.
    pub(crate) fn table_id(&self) -> Option<&str> {
        self.parameters.get(TABLE_ID).map(String::as_str)
    }

    /// After new data files, the table's statistics no longer describe it.
    /// `CodeLake` does not compute statistics, so they become unknown exactly
    /// as Trino records unknown statistics when it merges an insert without
    /// them: basic counters removed, column statistics empty (the field stays,
    /// Trino needs it). Every other field is kept as it was.
    pub(crate) fn invalidate_statistics(&mut self) {
        self.column_statistics.clear();
        for key in ["numFiles", "numRows", "rawDataSize", "totalSize"] {
            self.parameters.remove(key);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) writer_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner_type: Option<String>,
    #[serde(default)]
    pub(crate) parameters: BTreeMap<String, String>,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TrinoGrant<'a> {
    permission: &'a str,
    grant_option: bool,
    grantee: Grantee<'a>,
}

#[derive(Debug, Serialize)]
struct Grantee<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    name: &'a str,
}

pub(crate) type MetastoreResult<T> = Result<T, MetastoreError>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum MetastoreError {
    #[error("object store: {0}")]
    Store(#[from] object_store::Error),
    #[error("metadata {path}: {source}")]
    Json { path: String, source: serde_json::Error },
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("{0} does not exist")]
    NotFound(String),
    #[error("local warehouse: {0}")]
    Io(#[from] std::io::Error),
    #[error("Checksum error: {0} does not match its checksum file")]
    Checksum(String),
    #[error("{0}")]
    Conflict(String),
}

/// In-process coordination of one table's maintenance (ADR 0009).
#[derive(Debug, Default)]
pub(crate) struct TableLocks {
    /// One `OPTIMIZE` at a time per table.
    pub(crate) optimize: tokio::sync::Mutex<()>,
    /// Held exclusively while `OPTIMIZE` swaps files; scans hold it shared
    /// while listing, so they never list old and new files together.
    pub(crate) commit: tokio::sync::RwLock<()>,
    /// Held while the table's metadata changes or new files are published
    /// (see the module documentation). Always taken last.
    metadata: Arc<tokio::sync::Mutex<()>>,
}

/// Held metadata lock of one table (in-process and cross-process).
#[derive(Debug)]
pub(crate) struct MetadataLock {
    _local: tokio::sync::OwnedMutexGuard<()>,
    _file: FileLock,
}

/// A statement's private directory under `.codelake/staging/`.
#[derive(Debug, Clone)]
pub(crate) struct Staging {
    pub(crate) dir: Path,
}

/// Held cross-process lock on one table's data files (ADR 0009); released
/// when dropped. Empty when the warehouse is not a local file system.
#[derive(Debug)]
pub(crate) struct FileLock {
    _file: Option<std::fs::File>,
}

/// Locks of every table touched so far, by `(schema, table)`.
type LockMap = HashMap<(String, String), Arc<TableLocks>>;

/// Metadata access for one catalog root.
#[derive(Debug, Clone)]
pub(crate) struct FileMetastore {
    store: Arc<dyn ObjectStore>,
    root: Path,
    locks: Arc<Mutex<LockMap>>,
    /// Directory of `root` when the warehouse is a local file system. Object
    /// stores have no directories, but Trino on a local warehouse does: it
    /// refuses to create a table whose directory exists, even empty.
    local_dir: Option<PathBuf>,
    /// The catalog's `hive.timestamp-precision`.
    precision: HiveTimestampPrecision,
    /// The catalog's `hive.parquet.time-zone`.
    zone: ParquetTimeZone,
}

/// Removes a local directory tree; absent is fine. Retries briefly because an
/// abandoned upload may still be deleting its staging file inside it.
async fn remove_local_dir(path: PathBuf) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut attempt = 0;
        loop {
            match std::fs::remove_dir_all(&path) {
                Ok(()) => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(_) if attempt < 10 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err) => return Err(err),
            }
        }
    })
    .await
    .map_err(std::io::Error::other)?
}

/// Exchanges the directories `a` and `b` in one step (`renameat2` with
/// `RENAME_EXCHANGE` on Linux, `renamex_np` with `RENAME_SWAP` on macOS):
/// any reader sees one or the other under each name, never neither.
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn exchange_dirs(a: &std::path::Path, b: &std::path::Path) -> std::io::Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};
    renameat_with(CWD, a, CWD, b, RenameFlags::EXCHANGE).map_err(std::io::Error::from)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
fn exchange_dirs(_: &std::path::Path, _: &std::path::Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

/// Directory of `path` (every part but the last).
fn parent(path: &Path) -> Path {
    Path::from_iter(path.parts().take(path.parts().count().saturating_sub(1)))
}

/// Names of the direct subdirectories of `dir` that hold a `.trinoSchema`,
/// read from the local file system.
fn local_tables(dir: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut tables = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if name.starts_with('.') || !entry.file_type()?.is_dir() {
            continue;
        }
        if entry.path().join(METADATA_FILE).is_file() {
            tables.push(name);
        }
    }
    tables.sort();
    Ok(tables)
}

impl FileMetastore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, root: Path) -> Self {
        Self {
            store,
            root,
            locks: Arc::default(),
            local_dir: None,
            precision: HiveTimestampPrecision::default(),
            zone: ParquetTimeZone::default(),
        }
    }

    /// Sets the catalog's timestamp precision.
    pub(crate) fn with_timestamp_precision(mut self, precision: HiveTimestampPrecision) -> Self {
        self.precision = precision;
        self
    }

    pub(crate) fn timestamp_precision(&self) -> HiveTimestampPrecision {
        self.precision
    }

    /// Sets the zone of the catalog's Parquet `INT96` timestamps.
    pub(crate) fn with_parquet_time_zone(mut self, zone: ParquetTimeZone) -> Self {
        self.zone = zone;
        self
    }

    /// The zone of the catalog's Parquet `INT96` timestamps.
    pub(crate) fn parquet_time_zone(&self) -> ParquetTimeZone {
        self.zone
    }

    pub(crate) fn table_locks(&self, schema: &str, table: &str) -> Arc<TableLocks> {
        let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(locks.entry((schema.to_owned(), table.to_owned())).or_default())
    }

    /// Cross-process lock of one table's data files: exclusive while
    /// `OPTIMIZE` swaps them or recovers a swap, shared while a scan lists
    /// them. The in-process [`TableLocks`] do not reach other processes (other
    /// `CodeLake` nodes on the same warehouse); this advisory `flock` does, on
    /// a local warehouse (`.codelake/locks/<schema>/<table>.lock` under the
    /// root, hidden from Trino). The OS releases it when a process dies, so a
    /// crash never leaves it held. Object stores have no such lock: `None`.
    pub(crate) async fn table_file_lock(
        &self,
        schema: &str,
        table: &str,
        exclusive: bool,
    ) -> MetastoreResult<FileLock> {
        self.lock_file(schema, &format!("{table}.lock"), exclusive).await
    }

    /// The table's metadata lock (see the module documentation). Taken after
    /// any other lock of the table, never before.
    pub(crate) async fn lock_metadata(&self, schema: &str, table: &str) -> MetastoreResult<MetadataLock> {
        let mutex = Arc::clone(&self.table_locks(schema, table).metadata);
        let local = mutex.lock_owned().await;
        let file = self
            .lock_file(schema, &format!("{table}.metadata.lock"), true)
            .await?;
        Ok(MetadataLock {
            _local: local,
            _file: file,
        })
    }

    async fn lock_file(&self, schema: &str, file_name: &str, exclusive: bool) -> MetastoreResult<FileLock> {
        let Some(dir) = &self.local_dir else {
            return Ok(FileLock { _file: None });
        };
        if !is_valid_object_name(schema) || !is_valid_object_name(file_name) {
            return Err(MetastoreError::Conflict(format!(
                "invalid name {schema}.{file_name}"
            )));
        }
        let path = dir.join(CODELAKE_DIR).join("locks").join(schema).join(file_name);
        let file = tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
        })
        .await
        .map_err(std::io::Error::other)??;
        loop {
            let attempt = if exclusive {
                file.try_lock()
            } else {
                file.try_lock_shared()
            };
            match attempt {
                Ok(()) => return Ok(FileLock { _file: Some(file) }),
                Err(std::fs::TryLockError::WouldBlock) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(std::fs::TryLockError::Error(err)) => return Err(err.into()),
            }
        }
    }

    /// Declares that `root` is the local directory `dir`.
    pub(crate) fn with_local_dir(mut self, dir: PathBuf) -> Self {
        self.local_dir = Some(dir);
        self
    }

    /// Local file-system path of `path`, for a local warehouse.
    fn local_path(&self, path: &Path) -> Option<PathBuf> {
        let dir = self.local_dir.as_ref()?;
        let relative = path.prefix_match(&self.root)?;
        Some(relative.fold(dir.clone(), |acc, part| acc.join(part.as_ref())))
    }

    pub(crate) fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn schema_file(&self, schema: &str) -> Path {
        self.root.clone().join(format!(".{schema}{METADATA_FILE}"))
    }

    pub(crate) fn table_dir(&self, schema: &str, table: &str) -> Path {
        self.root.clone().join(schema).join(table)
    }

    fn table_file(&self, schema: &str, table: &str) -> Path {
        self.table_dir(schema, table).join(METADATA_FILE)
    }

    async fn read_bytes(&self, path: &Path) -> MetastoreResult<Option<bytes::Bytes>> {
        match self.store.get(path).await {
            Ok(result) => Ok(Some(result.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Reads a metadata file and checks it against its `.crc`, when it has
    /// one. A mismatch is read again a few times (a writer puts the checksum
    /// first, the file right after), then is an error.
    async fn read_verified(&self, path: &Path) -> MetastoreResult<Option<bytes::Bytes>> {
        let name = path.filename().unwrap_or_default().to_owned();
        let checksum = parent(path).join(crc_name(&name));
        let mut attempt = 0;
        loop {
            let Some(bytes) = self.read_bytes(path).await? else {
                return Ok(None);
            };
            match self.read_bytes(&checksum).await? {
                None => return Ok(Some(bytes)),
                Some(expected) if expected.as_ref() == hadoop_crc(&bytes).as_slice() => {
                    return Ok(Some(bytes));
                }
                Some(_) if attempt < CHECKSUM_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Some(_) => return Err(MetastoreError::Checksum(path.to_string())),
            }
        }
    }

    async fn read_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> MetastoreResult<Option<T>> {
        let Some(bytes) = self.read_verified(path).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| MetastoreError::Json {
                path: path.to_string(),
                source,
            })
    }

    /// Writes `bytes` and its Hadoop checksum. The checksum goes first so a
    /// reader never sees new content with a stale checksum.
    async fn write_with_crc(&self, path: &Path, bytes: Vec<u8>) -> MetastoreResult<()> {
        let name = path.filename().unwrap_or_default().to_owned();
        let parent = parent(path);
        self.store
            .put(
                &parent.join(crc_name(&name)),
                PutPayload::from(hadoop_crc(&bytes)),
            )
            .await?;
        self.store.put(path, PutPayload::from(bytes)).await?;
        Ok(())
    }

    async fn write_json<T: Serialize>(&self, path: &Path, value: &T) -> MetastoreResult<()> {
        let bytes = serde_json::to_vec_pretty(value).map_err(|source| MetastoreError::Json {
            path: path.to_string(),
            source,
        })?;
        self.write_with_crc(path, bytes).await
    }

    pub(crate) async fn list_schemas(&self) -> MetastoreResult<Vec<String>> {
        let listing = self.store.list_with_delimiter(Some(&self.root)).await?;
        let mut schemas: Vec<String> = listing
            .objects
            .iter()
            .filter_map(|o| o.location.filename())
            .filter_map(|f| {
                f.strip_prefix('.')?
                    .strip_suffix(METADATA_FILE)
                    .map(str::to_owned)
            })
            .filter(|s| !s.is_empty() && !s.starts_with('.'))
            .collect();
        schemas.sort();
        Ok(schemas)
    }

    pub(crate) async fn schema_exists(&self, schema: &str) -> MetastoreResult<bool> {
        Ok(self
            .read_json::<SchemaMetadata>(&self.schema_file(schema))
            .await?
            .is_some())
    }

    pub(crate) async fn list_tables(&self, schema: &str) -> MetastoreResult<Vec<String>> {
        let listing = self
            .store
            .list_with_delimiter(Some(&self.root.clone().join(schema)))
            .await?;
        let mut tables = Vec::new();
        for prefix in listing.common_prefixes {
            let Some(name) = prefix.filename() else { continue };
            if name.starts_with('.') {
                continue;
            }
            if self.store.head(&prefix.clone().join(METADATA_FILE)).await.is_ok() {
                tables.push(name.to_owned());
            }
        }
        tables.sort();
        Ok(tables)
    }

    pub(crate) async fn table(&self, schema: &str, table: &str) -> MetastoreResult<Option<TableMetadata>> {
        // A name that is not its own directory names no table (an empty one
        // would be the schema's directory).
        if !is_valid_object_name(schema) || !is_valid_object_name(table) {
            return Ok(None);
        }
        self.read_json(&self.table_file(schema, table)).await
    }

    /// Creates the schema `schema` (folded), recording `written` when the
    /// statement wrote it with other letter case (ISS-018).
    pub(crate) async fn create_schema(
        &self,
        schema: &str,
        written: &str,
        owner: &str,
    ) -> MetastoreResult<()> {
        if self.schema_exists(schema).await? {
            return Err(MetastoreError::AlreadyExists(format!("schema {schema}")));
        }
        let parameters = if written == schema {
            BTreeMap::new()
        } else {
            BTreeMap::from([(WRITTEN_NAME.to_owned(), written.to_owned())])
        };
        let metadata = SchemaMetadata {
            writer_version: Some(TRINO_VERSION.into()),
            owner_name: Some(owner.into()),
            owner_type: Some("USER".into()),
            parameters,
            extra: BTreeMap::new(),
        };
        self.write_json(&self.schema_file(schema), &metadata).await?;
        // Trino creates the schema directory and requires it for new tables.
        if let Some(dir) = self.local_path(&self.root.clone().join(schema)) {
            std::fs::create_dir_all(dir)?;
        }
        Ok(())
    }

    /// Commit point of a table: once this file exists, the table is visible.
    pub(crate) async fn write_table(
        &self,
        schema: &str,
        table: &str,
        metadata: &TableMetadata,
    ) -> MetastoreResult<()> {
        self.write_json(&self.table_file(schema, table), metadata).await
    }

    pub(crate) async fn write_owner_permissions(
        &self,
        schema: &str,
        table: &str,
        owner: &str,
    ) -> MetastoreResult<()> {
        self.write_owner_grants(&self.table_dir(schema, table), owner)
            .await
    }

    async fn write_owner_grants(&self, dir: &Path, owner: &str) -> MetastoreResult<()> {
        let grants: Vec<TrinoGrant<'_>> = ["SELECT", "INSERT", "UPDATE", "DELETE"]
            .into_iter()
            .map(|permission| TrinoGrant {
                permission,
                grant_option: true,
                grantee: Grantee {
                    kind: "USER",
                    name: owner,
                },
            })
            .collect();
        let path = dir.clone().join(PERMISSIONS_DIR).join(format!("user_{owner}"));
        self.write_json(&path, &grants).await
    }

    /// Records `written` as the name of the table or view `schema.table`
    /// (after a rename), under its metadata lock.
    pub(crate) async fn record_written_name(
        &self,
        schema: &str,
        table: &str,
        written: &str,
    ) -> MetastoreResult<()> {
        let _metadata = self.lock_metadata(schema, table).await?;
        let Some(mut metadata) = self.table(schema, table).await? else {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        };
        let before = metadata.parameters.clone();
        if written == table {
            metadata.parameters.remove(WRITTEN_NAME);
        } else {
            metadata
                .parameters
                .insert(WRITTEN_NAME.into(), written.to_owned());
        }
        if metadata.parameters != before {
            self.write_table(schema, table, &metadata).await?;
        }
        Ok(())
    }

    /// Drops statistics after new data files were written, when any exist.
    /// Takes the table's metadata lock.
    pub(crate) async fn invalidate_statistics(&self, schema: &str, table: &str) -> MetastoreResult<()> {
        let _metadata = self.lock_metadata(schema, table).await?;
        self.invalidate_statistics_locked(schema, table).await
    }

    /// [`FileMetastore::invalidate_statistics`] under a held metadata lock.
    async fn invalidate_statistics_locked(&self, schema: &str, table: &str) -> MetastoreResult<()> {
        let Some(mut metadata) = self.table(schema, table).await? else {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        };
        let before = metadata.clone();
        metadata.invalidate_statistics();
        if metadata != before {
            self.write_table(schema, table, &metadata).await?;
        }
        Ok(())
    }

    /// Whether anything, even an empty directory, exists where the table's
    /// files would go. Trino refuses to create a table over it.
    pub(crate) async fn table_location_exists(&self, schema: &str, table: &str) -> MetastoreResult<bool> {
        let dir = self.table_dir(schema, table);
        if let Some(path) = self.local_path(&dir) {
            return Ok(path.try_exists()?);
        }
        Ok(self.store.list(Some(&dir)).try_next().await?.is_some())
    }

    async fn delete_all(&self, prefix: &Path) -> MetastoreResult<()> {
        let objects: Vec<_> = self.store.list(Some(prefix)).try_collect().await?;
        for object in objects {
            match self.store.delete(&object.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(())
    }

    /// Removes everything under the table's location, directory included.
    pub(crate) async fn remove_table_location(&self, schema: &str, table: &str) -> MetastoreResult<()> {
        let dir = self.table_dir(schema, table);
        self.delete_all(&dir).await?;
        if let Some(path) = self.local_path(&dir) {
            remove_local_dir(path).await?;
        }
        Ok(())
    }

    /// Schemas listed from a local warehouse right now, without the object
    /// store (synchronous: `DataFusion` lists names synchronously). `None`
    /// on an object store.
    pub(crate) fn local_schemas(&self) -> Option<Vec<String>> {
        let dir = self.local_dir.as_ref()?;
        let entries = std::fs::read_dir(dir).ok()?;
        let mut schemas: Vec<String> = entries
            .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
            .filter_map(|file| {
                file.strip_prefix('.')?
                    .strip_suffix(METADATA_FILE)
                    .map(str::to_owned)
            })
            .filter(|s| !s.is_empty() && !s.starts_with('.'))
            .collect();
        schemas.sort();
        Some(schemas)
    }

    /// Tables and views of `schema` listed from a local warehouse right now
    /// (see [`FileMetastore::local_schemas`]).
    pub(crate) fn local_tables(&self, schema: &str) -> Option<Vec<String>> {
        let dir = self.local_path(&self.root.clone().join(schema))?;
        Some(local_tables(&dir).unwrap_or_default())
    }

    /// A new private directory for one statement's files.
    pub(crate) fn staging(&self) -> Staging {
        Staging {
            dir: self
                .root
                .clone()
                .join(CODELAKE_DIR)
                .join("staging")
                .join(uuid::Uuid::new_v4().simple().to_string()),
        }
    }

    /// Removes a staging directory and everything in it; absent is fine.
    pub(crate) async fn discard(&self, staging: &Staging) {
        let removed = async {
            self.delete_all(&staging.dir).await?;
            if let Some(path) = self.local_path(&staging.dir) {
                remove_local_dir(path).await?;
            }
            Ok::<_, MetastoreError>(())
        }
        .await;
        if let Err(err) = removed {
            tracing::warn!(staging = %staging.dir, error = %err, "could not remove a staging directory");
        }
    }

    /// Writes a table's metadata and owner permissions into `staging`,
    /// where [`FileMetastore::publish_table`] will find them.
    pub(crate) async fn stage_table(
        &self,
        staging: &Staging,
        metadata: &TableMetadata,
        owner: &str,
    ) -> MetastoreResult<()> {
        self.write_json(&staging.dir.clone().join(METADATA_FILE), metadata)
            .await?;
        self.write_owner_grants(&staging.dir, owner).await
    }

    /// Makes the table staged in `staging` visible as `schema.table`, if no
    /// table, view or directory took the name meanwhile. The caller holds the
    /// table's metadata lock.
    pub(crate) async fn publish_table(
        &self,
        staging: &Staging,
        schema: &str,
        table: &str,
    ) -> MetastoreResult<()> {
        if self.table(schema, table).await?.is_some() {
            return Err(MetastoreError::AlreadyExists(format!("table {schema}.{table}")));
        }
        if self.table_location_exists(schema, table).await? {
            return Err(MetastoreError::Conflict(format!(
                "target directory for table {schema}.{table} already exists"
            )));
        }
        let target = self.table_dir(schema, table);
        if let (Some(from), Some(to)) = (self.local_path(&staging.dir), self.local_path(&target)) {
            // One rename: the directory appears whole, metadata included.
            tokio::task::spawn_blocking(move || {
                if let Some(schema_dir) = to.parent() {
                    std::fs::create_dir_all(schema_dir)?;
                }
                std::fs::rename(from, to)
            })
            .await
            .map_err(std::io::Error::other)??;
            return Ok(());
        }
        // Object store: files first, the metadata (the commit point) last.
        let objects: Vec<_> = self.store.list(Some(&staging.dir)).try_collect().await?;
        let mut metadata = Vec::new();
        for object in objects {
            let Some(relative) = object.location.prefix_match(&staging.dir) else {
                continue;
            };
            let destination = relative.fold(target.clone(), Path::join);
            if destination.filename() == Some(METADATA_FILE) {
                metadata.push((object.location, destination));
                continue;
            }
            self.store.rename(&object.location, &destination).await?;
        }
        for (from, to) in metadata {
            self.store.rename(&from, &to).await?;
        }
        Ok(())
    }

    /// `CREATE OR REPLACE TABLE`: makes the table staged in `staging` the
    /// table `schema.table` (ISS-017). The caller holds the table's commit,
    /// file and metadata locks.
    ///
    /// - No table of that name: [`FileMetastore::publish_table`] (a
    ///   directory without a table is refused, never deleted).
    /// - A managed table: its directory and the staging directory are
    ///   exchanged in one step, metadata and data together, so a reader
    ///   (this engine, another node, the reference engine, the sink) finds
    ///   the old table or the new one, never a mix and never nothing. The
    ///   old table is then in `staging`, for the caller to remove. A crash
    ///   before the exchange leaves the old table; after it, the new one
    ///   (and the old one in hidden staging).
    /// - A view, or a table whose data is not its own (`EXTERNAL_TABLE`):
    ///   refused, nothing changed.
    ///
    /// Only a local warehouse on a file system that can exchange two
    /// directories replaces a table; elsewhere the statement is refused
    /// with the table unchanged.
    pub(crate) async fn replace_table(
        &self,
        staging: &Staging,
        schema: &str,
        table: &str,
    ) -> MetastoreResult<()> {
        let Some(existing) = self.table(schema, table).await? else {
            return self.publish_table(staging, schema, table).await;
        };
        if existing.is_view() {
            return Err(MetastoreError::Conflict(format!(
                "{schema}.{table} is a view: use CREATE OR REPLACE VIEW, or DROP VIEW first"
            )));
        }
        if existing.table_type != MANAGED_TABLE {
            return Err(MetastoreError::Conflict(format!(
                "{schema}.{table} is a table of type {} whose data does not belong to it; \
                 CREATE OR REPLACE replaces only managed tables (DROP TABLE it first)",
                existing.table_type
            )));
        }
        let target = self.table_dir(schema, table);
        let (Some(from), Some(to)) = (self.local_path(&staging.dir), self.local_path(&target)) else {
            return Err(MetastoreError::Conflict(format!(
                "CREATE OR REPLACE TABLE {schema}.{table} needs a local warehouse (the table's \
                 directory is exchanged with the new one in one step); the table is unchanged"
            )));
        };
        tokio::task::spawn_blocking(move || exchange_dirs(&from, &to))
            .await
            .map_err(std::io::Error::other)?
            .map_err(|err| match rustix::io::Errno::from_io_error(&err) {
                // What a file system answers when it cannot exchange.
                Some(errno)
                    if [
                        rustix::io::Errno::INVAL,
                        rustix::io::Errno::NOSYS,
                        rustix::io::Errno::OPNOTSUPP,
                    ]
                    .contains(&errno) =>
                {
                    MetastoreError::Conflict(format!(
                        "the warehouse's file system cannot exchange two directories in one step \
                         ({err}), which CREATE OR REPLACE TABLE {schema}.{table} needs; the table is unchanged"
                    ))
                }
                _ if err.kind() == std::io::ErrorKind::Unsupported => MetastoreError::Conflict(format!(
                    "this platform cannot exchange two directories in one step, which CREATE OR \
                     REPLACE TABLE {schema}.{table} needs; the table is unchanged"
                )),
                _ => MetastoreError::Io(err),
            })
    }

    /// Commits the data file `staged` of an `INSERT` into `schema.table` as
    /// `name`: under the table's metadata lock, checks that the table still
    /// exists with the columns the file was written with (an `ADD COLUMN`
    /// meanwhile only appends) and is the table the statement was planned
    /// against (`table_id`: not replaced meanwhile), marks its statistics
    /// unknown, then renames the file into the table directory. The rename is
    /// the commit: any failure before it leaves no row visible.
    pub(crate) async fn commit_insert(
        &self,
        (schema, table): (&str, &str),
        table_id: Option<&str>,
        written: &[ColumnMetadata],
        staged: &Path,
        name: &str,
    ) -> MetastoreResult<()> {
        let _metadata = self.lock_metadata(schema, table).await?;
        let Some(metadata) = self.table(schema, table).await?.filter(|m| !m.is_view()) else {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        };
        if metadata.table_id() != table_id {
            return Err(MetastoreError::Conflict(format!(
                "table {schema}.{table} was replaced (CREATE OR REPLACE) after the INSERT was planned; nothing was written"
            )));
        }
        let same = |a: &ColumnMetadata, b: &ColumnMetadata| {
            a.name.to_lowercase() == b.name.to_lowercase() && a.hive_type.eq_ignore_ascii_case(&b.hive_type)
        };
        let unchanged = metadata.data_columns.len() >= written.len()
            && written
                .iter()
                .zip(&metadata.data_columns)
                .all(|(a, b)| same(a, b));
        if !unchanged {
            return Err(MetastoreError::Conflict(format!(
                "table {schema}.{table} was replaced after the INSERT was planned; nothing was written"
            )));
        }
        self.invalidate_statistics_locked(schema, table).await?;
        self.store
            .rename(staged, &self.table_dir(schema, table).join(name))
            .await?;
        Ok(())
    }

    /// Adds a column: read, change and write the metadata under the table's
    /// metadata lock, so no concurrent writer loses it.
    pub(crate) async fn add_column(
        &self,
        schema: &str,
        table: &str,
        column: ColumnMetadata,
    ) -> MetastoreResult<()> {
        let _metadata = self.lock_metadata(schema, table).await?;
        let Some(mut metadata) = self.table(schema, table).await? else {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        };
        if metadata.is_view() {
            return Err(MetastoreError::Conflict(format!("{schema}.{table} is a view")));
        }
        let folded = column.name.to_lowercase();
        if let Some(existing) = metadata
            .data_columns
            .iter()
            .chain(&metadata.partition_columns)
            .find(|c| c.name.to_lowercase() == folded)
        {
            let (new, old) = (column.written_name(), existing.written_name());
            return Err(MetastoreError::Conflict(if new == old {
                format!("column {new} already exists in {schema}.{table}")
            } else {
                format!(
                    "column {new} cannot be added to {schema}.{table}: column {old} exists, and column names \
                     that differ only by case are the same column"
                )
            }));
        }
        metadata.data_columns.push(column);
        self.write_table(schema, table, &metadata).await
    }

    /// Drops a table or view: metadata first (it disappears atomically), then
    /// its directory. A managed table's data goes with it, as in Trino. An
    /// external table keeps its data and loses only what the metastore wrote;
    /// so does a view (ISS-023): a view owns no data, and one created over a
    /// directory that already held files (another writer's) must not take
    /// them with it. Trino 431 deletes that directory on `DROP VIEW`.
    pub(crate) async fn drop_table(&self, schema: &str, table: &str) -> MetastoreResult<()> {
        let Some(metadata) = self.table(schema, table).await? else {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        };
        let file = self.table_file(schema, table);
        self.store.delete(&file).await?;
        if metadata.table_type == MANAGED_TABLE {
            return self.remove_table_location(schema, table).await;
        }
        self.remove_metadata_leftovers(&self.table_dir(schema, table))
            .await
    }

    /// Removes what the metastore writes next to `.trinoSchema` (its
    /// checksum and the permissions) from `dir`, and the directory itself
    /// only if nothing else is left in it. Any other file stays.
    async fn remove_metadata_leftovers(&self, dir: &Path) -> MetastoreResult<()> {
        self.delete_all(&dir.clone().join(PERMISSIONS_DIR)).await?;
        match self
            .store
            .delete(&dir.clone().join(crc_name(METADATA_FILE)))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
        if let Some(path) = self.local_path(dir) {
            // Only empty directories go; any other file under it stays.
            let _ = std::fs::remove_dir(path.join(PERMISSIONS_DIR));
            let _ = std::fs::remove_dir(path);
        }
        Ok(())
    }

    /// Metadata files of the table or view in `dir`: the `.trinoSchema`
    /// commit point last, so a copy in that order makes the name visible
    /// only once everything else is there.
    async fn metadata_files(&self, dir: &Path) -> MetastoreResult<Vec<Path>> {
        let file = dir.clone().join(METADATA_FILE);
        let crc = dir.clone().join(crc_name(METADATA_FILE));
        let permissions = dir.clone().join(PERMISSIONS_DIR);
        let mut files: Vec<Path> = self
            .store
            .list(Some(&permissions))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await?;
        files.sort();
        for path in [crc, file] {
            match self.store.head(&path).await {
                Ok(_) => files.push(path),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }
        Ok(files)
    }

    /// `ALTER VIEW … RENAME TO`: moves only the view's metadata (ISS-023),
    /// never other files in its directory. The new name appears whole
    /// (its `.trinoSchema` is copied last), then the old one disappears
    /// (its `.trinoSchema` is deleted first): a failure part way leaves the
    /// old name, or both names with the same definition.
    async fn rename_view(
        &self,
        (schema, view): (&str, &str),
        (new_schema, new_view): (&str, &str),
    ) -> MetastoreResult<()> {
        let (old_dir, new_dir) = (self.table_dir(schema, view), self.table_dir(new_schema, new_view));
        let files = self.metadata_files(&old_dir).await?;
        for location in &files {
            if let Some(relative) = location.prefix_match(&old_dir) {
                let to = Path::from_iter(new_dir.parts().chain(relative));
                self.store.copy(location, &to).await?;
            }
        }
        self.store.delete(&self.table_file(schema, view)).await?;
        self.remove_metadata_leftovers(&old_dir).await
    }

    /// Moves a table or view to a new name, like Trino's file metastore,
    /// which renames the table directory.
    ///
    /// On a local warehouse the directory is renamed in one step (`rename(2)`),
    /// so readers see the old name or the new one, and no empty directory is
    /// left behind (Trino refuses to create a table over one). Object stores
    /// have no directories: there the metadata file is the commit point. Data
    /// files and permissions are copied first, the metadata copy makes the new
    /// name visible, then the old metadata goes (the old name disappears) and
    /// its files with it. A failure part way leaves the old name intact, or
    /// both names over identical files; never a name without its data.
    ///
    /// A target directory holding any object is refused: its files would
    /// otherwise become rows of the renamed table. A view moves only its
    /// metadata ([`FileMetastore::rename_view`]).
    pub(crate) async fn rename_table(
        &self,
        schema: &str,
        table: &str,
        new_schema: &str,
        new_table: &str,
    ) -> MetastoreResult<()> {
        let (old_dir, new_dir) = (
            self.table_dir(schema, table),
            self.table_dir(new_schema, new_table),
        );
        let (old_metadata, new_metadata) = (
            self.table_file(schema, table),
            self.table_file(new_schema, new_table),
        );
        if self.store.head(&old_metadata).await.is_err() {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        }
        let occupied: Vec<_> = self.store.list(Some(&new_dir)).try_collect().await?;
        if !occupied.is_empty() {
            return Err(MetastoreError::AlreadyExists(format!(
                "table {new_schema}.{new_table}"
            )));
        }
        if self.table(schema, table).await?.is_some_and(|m| m.is_view()) {
            return self.rename_view((schema, table), (new_schema, new_table)).await;
        }
        match self.store.rename(&old_dir, &new_dir).await {
            // A local directory: renamed as a whole.
            Ok(()) => {
                return match self.store.head(&new_metadata).await {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err.into()),
                };
            }
            // Object stores: the directory is not an object.
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err.into()),
        }
        let objects: Vec<_> = self.store.list(Some(&old_dir)).try_collect().await?;
        let files: Vec<&Path> = objects
            .iter()
            .map(|o| &o.location)
            .filter(|location| **location != old_metadata)
            .collect();
        for location in &files {
            if let Some(relative) = location.prefix_match(&old_dir) {
                let to = Path::from_iter(new_dir.parts().chain(relative));
                self.store.copy(location, &to).await?;
            }
        }
        self.store.copy(&old_metadata, &new_metadata).await?;
        self.store.delete(&old_metadata).await?;
        for location in files {
            self.store.delete(location).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;

    fn fixture() -> FileMetastore {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse");
        FileMetastore::new(
            Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()),
            Path::default(),
        )
    }

    fn scratch() -> (tempfile::TempDir, FileMetastore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let metastore =
            FileMetastore::new(Arc::new(store), Path::default()).with_local_dir(dir.path().into());
        (dir, metastore)
    }

    #[tokio::test]
    async fn reads_what_trino_431_wrote() {
        let metastore = fixture();
        assert_eq!(metastore.list_schemas().await.unwrap(), vec!["pilot"]);
        assert_eq!(metastore.list_tables("pilot").await.unwrap(), vec!["t1", "v1"]);
        let table = metastore.table("pilot", "t1").await.unwrap().unwrap();
        assert_eq!(table.table_type, MANAGED_TABLE);
        assert_eq!(table.storage_format.as_deref(), Some("PARQUET"));
        let columns: Vec<(&str, &str)> = table
            .data_columns
            .iter()
            .map(|c| (c.name.as_str(), c.hive_type.as_str()))
            .collect();
        assert_eq!(
            columns,
            vec![
                ("id", "bigint"),
                ("nome", "string"),
                ("valor", "decimal(18,2)"),
                ("lsn", "binary"),
                ("ts", "timestamp"),
                ("d", "date"),
                ("flag", "boolean"),
                ("dbl", "double"),
                ("i", "int"),
                ("vc", "varchar(10)"),
            ]
        );
        assert_eq!(table.column_statistics.len(), 10, "statistics are read");
        assert!(table.serde_parameters.is_empty());
        let view = metastore.table("pilot", "v1").await.unwrap().unwrap();
        assert!(view.is_view());
        assert!(view.view_original_text.unwrap().starts_with("/* Presto View: "));
        assert_eq!(metastore.table("pilot", "nope").await.unwrap(), None);
    }

    #[tokio::test]
    async fn round_trip_keeps_unknown_fields_and_writes_checksums() {
        let (dir, metastore) = scratch();
        let original = fixture().table("pilot", "t1").await.unwrap().unwrap();
        metastore.create_schema("pilot", "pilot", "dbt").await.unwrap();
        metastore.write_table("pilot", "t1", &original).await.unwrap();
        let back = metastore.table("pilot", "t1").await.unwrap().unwrap();
        assert_eq!(back, original);

        let written = std::fs::read(dir.path().join("pilot/t1/.trinoSchema")).unwrap();
        let crc = std::fs::read(dir.path().join("pilot/t1/..trinoSchema.crc")).unwrap();
        assert_eq!(crc, hadoop_crc(&written));
        let schema_file = std::fs::read(dir.path().join(".pilot.trinoSchema")).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("..pilot.trinoSchema.crc")).unwrap(),
            hadoop_crc(&schema_file)
        );
    }

    fn written_json(dir: &tempfile::TempDir, file: &str) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(dir.path().join(file)).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn metadata_has_every_field_trino_431_requires() {
        let (dir, metastore) = scratch();
        metastore.create_schema("raw", "raw", "dbt").await.unwrap();
        let table = TableMetadata::managed_parquet("dbt", Vec::new());
        metastore.write_table("raw", "t", &table).await.unwrap();
        let view = TableMetadata::trino_view("dbt", "/* Presto View: e30= */".into());
        metastore.write_table("raw", "v", &view).await.unwrap();
        for file in ["raw/t/.trinoSchema", "raw/v/.trinoSchema"] {
            let json = written_json(&dir, file);
            for field in [
                "dataColumns",
                "partitionColumns",
                "parameters",
                "serdeParameters",
                "columnStatistics",
            ] {
                assert!(
                    json[field].is_object() || json[field].is_array(),
                    "{file}: {field} in {json}"
                );
            }
        }
        let view = written_json(&dir, "raw/v/.trinoSchema");
        assert_eq!(view["originalStorageFormat"], serde_json::json!({}));
        assert_eq!(view["parameters"]["presto_view"], "true");
    }

    #[tokio::test]
    async fn insert_keeps_every_field_and_marks_statistics_unknown() {
        let (dir, metastore) = scratch();
        let mut original = fixture().table("pilot", "t1").await.unwrap().unwrap();
        original
            .extra
            .insert("bucketProperty".into(), serde_json::json!({"bucketCount": 4}));
        metastore.create_schema("pilot", "pilot", "trino").await.unwrap();
        metastore.write_table("pilot", "t1", &original).await.unwrap();

        metastore.invalidate_statistics("pilot", "t1").await.unwrap();
        let json = written_json(&dir, "pilot/t1/.trinoSchema");
        assert_eq!(json["columnStatistics"], serde_json::json!({}), "present, empty");
        assert_eq!(json["serdeParameters"], serde_json::json!({}));
        assert_eq!(json["bucketProperty"]["bucketCount"], 4, "unknown field kept");
        assert_eq!(json["writerVersion"], "431");
        let parameters = &json["parameters"];
        for kept in [
            "presto_version",
            "presto_query_id",
            "transactional",
            "auto.purge",
            "STATS_GENERATED_VIA_STATS_TASK",
        ] {
            assert_eq!(parameters[kept], original.parameters[kept], "{kept}");
        }
        for unknown in ["numFiles", "numRows", "rawDataSize", "totalSize"] {
            assert!(parameters.get(unknown).is_none(), "{unknown} would be stale");
        }
        let back = metastore.table("pilot", "t1").await.unwrap().unwrap();
        assert_eq!(back.data_columns, original.data_columns);
        assert_eq!(back.storage_format, original.storage_format);
    }

    #[tokio::test]
    async fn new_metadata_carries_the_trino_version() {
        let (dir, metastore) = scratch();
        metastore.create_schema("raw", "raw", "dbt").await.unwrap();
        let table = TableMetadata::managed_parquet("dbt", Vec::new());
        metastore.write_table("raw", "t", &table).await.unwrap();
        for file in [".raw.trinoSchema", "raw/t/.trinoSchema"] {
            let json: serde_json::Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(file)).unwrap()).unwrap();
            assert_eq!(json["writerVersion"], "431", "{file}");
        }
        // A rewrite keeps the version of the Trino that wrote the file.
        let mut written_by_other = table.clone();
        written_by_other.writer_version = Some("440".into());
        metastore
            .write_table("raw", "t", &written_by_other)
            .await
            .unwrap();
        let back = metastore.table("raw", "t").await.unwrap().unwrap();
        assert_eq!(back.writer_version.as_deref(), Some("440"));
    }

    #[tokio::test]
    async fn dropping_an_external_table_keeps_its_data() {
        let (dir, metastore) = scratch();
        metastore.create_schema("raw", "raw", "dbt").await.unwrap();
        let mut table = TableMetadata::managed_parquet("dbt", Vec::new());
        table.table_type = EXTERNAL_TABLE.into();
        metastore.write_table("raw", "ext", &table).await.unwrap();
        metastore
            .write_owner_permissions("raw", "ext", "dbt")
            .await
            .unwrap();
        std::fs::write(dir.path().join("raw/ext/part-0"), b"data").unwrap();
        metastore.drop_table("raw", "ext").await.unwrap();
        assert_eq!(metastore.table("raw", "ext").await.unwrap(), None);
        let left: Vec<_> = std::fs::read_dir(dir.path().join("raw/ext"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(left, vec!["part-0"], "only metastore files removed");
    }

    #[tokio::test]
    async fn removing_a_table_location_removes_the_directory() {
        let (dir, metastore) = scratch();
        metastore.create_schema("raw", "raw", "dbt").await.unwrap();
        std::fs::create_dir_all(dir.path().join("raw/t/.hidden")).unwrap();
        std::fs::write(dir.path().join("raw/t/.hidden/x"), b"x").unwrap();
        std::fs::write(dir.path().join("raw/t/f"), b"x").unwrap();
        metastore.remove_table_location("raw", "t").await.unwrap();
        assert!(!dir.path().join("raw/t").exists());
        metastore.remove_table_location("raw", "t").await.unwrap();
        // An object store without directories: listing decides.
        let bare = FileMetastore::new(
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
            Path::default(),
        );
        assert!(!bare.table_location_exists("raw", "t").await.unwrap());
        std::fs::create_dir_all(dir.path().join("raw/u")).unwrap();
        std::fs::write(dir.path().join("raw/u/f"), b"x").unwrap();
        assert!(bare.table_location_exists("raw", "u").await.unwrap());
        bare.remove_table_location("raw", "u").await.unwrap();
        assert!(!bare.table_location_exists("raw", "u").await.unwrap());
    }

    /// Object stores have no directories: the rename copies, commits the
    /// metadata, then removes the old objects.
    #[tokio::test]
    async fn rename_on_an_object_store() {
        let metastore = FileMetastore::new(Arc::new(object_store::memory::InMemory::new()), Path::from("wh"));
        let table = fixture().table("pilot", "t1").await.unwrap().unwrap();
        metastore.create_schema("raw", "raw", "dbt").await.unwrap();
        metastore.write_table("raw", "t", &table).await.unwrap();
        metastore
            .write_owner_permissions("raw", "t", "dbt")
            .await
            .unwrap();
        let data = Path::from("wh/raw/t/part-0");
        metastore
            .store()
            .put(&data, PutPayload::from_static(b"rows"))
            .await
            .unwrap();

        metastore.rename_table("raw", "t", "raw", "t2").await.unwrap();
        assert!(metastore.table("raw", "t").await.unwrap().is_none());
        assert_eq!(metastore.table("raw", "t2").await.unwrap().unwrap(), table);
        let left: Vec<_> = metastore
            .store()
            .list(Some(&Path::from("wh/raw/t")))
            .try_collect()
            .await
            .unwrap();
        assert!(left.is_empty(), "{left:?}");
        let moved = metastore
            .store()
            .get(&Path::from("wh/raw/t2/part-0"))
            .await
            .unwrap();
        assert_eq!(moved.bytes().await.unwrap().as_ref(), b"rows");
        assert_eq!(metastore.list_tables("raw").await.unwrap(), vec!["t2"]);
    }

    #[tokio::test]
    async fn schema_and_table_lifecycle() {
        let (dir, metastore) = scratch();
        metastore.create_schema("raw", "raw", "dbt").await.unwrap();
        assert!(matches!(
            metastore.create_schema("raw", "raw", "dbt").await,
            Err(MetastoreError::AlreadyExists(_))
        ));
        let mut table = TableMetadata::managed_parquet(
            "dbt",
            vec![ColumnMetadata {
                name: "id".into(),
                hive_type: "bigint".into(),
                comment: None,
                properties: BTreeMap::new(),
                extra: BTreeMap::new(),
            }],
        );
        table.parameters.insert("numRows".into(), "10".into());
        table
            .column_statistics
            .insert("id".into(), serde_json::json!({"nullsCount": 0}));
        table.invalidate_statistics();
        assert!(!table.parameters.contains_key("numRows") && table.column_statistics.is_empty());
        // Trino 431 cannot read table metadata without these keys, even empty.
        let json: serde_json::Value = serde_json::to_value(&table).unwrap();
        assert_eq!(json["serdeParameters"], serde_json::json!({}));
        assert_eq!(json["columnStatistics"], serde_json::json!({}));

        metastore.write_table("raw", "t", &table).await.unwrap();
        metastore
            .write_owner_permissions("raw", "t", "dbt")
            .await
            .unwrap();
        std::fs::write(dir.path().join("raw/t/20260101_000000_x_y"), b"data").unwrap();
        assert_eq!(metastore.list_tables("raw").await.unwrap(), vec!["t"]);
        let grants: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("raw/t/.trinoPermissions/user_dbt")).unwrap(),
        )
        .unwrap();
        assert_eq!(grants.as_array().unwrap().len(), 4);
        assert_eq!(grants[0]["grantee"]["name"], "dbt");

        assert!(dir.path().join("raw").is_dir(), "schema directory, as Trino");
        assert!(metastore.table_location_exists("raw", "t").await.unwrap());
        metastore.create_schema("mart", "mart", "dbt").await.unwrap();
        metastore.rename_table("raw", "t", "mart", "t2").await.unwrap();
        assert!(metastore.table("raw", "t").await.unwrap().is_none());
        assert_eq!(metastore.table("mart", "t2").await.unwrap().unwrap(), table);
        assert!(
            !dir.path().join("raw/t").exists(),
            "the directory moves as a whole"
        );
        for moved in [
            "..trinoSchema.crc",
            ".trinoPermissions/user_dbt",
            "20260101_000000_x_y",
        ] {
            assert!(dir.path().join("mart/t2").join(moved).exists(), "{moved}");
        }
        assert!(matches!(
            metastore.rename_table("raw", "t", "mart", "t3").await,
            Err(MetastoreError::NotFound(_))
        ));
        metastore.write_table("raw", "t", &table).await.unwrap();
        assert!(matches!(
            metastore.rename_table("raw", "t", "mart", "t2").await,
            Err(MetastoreError::AlreadyExists(_))
        ));
        metastore.drop_table("raw", "t").await.unwrap();
        metastore.rename_table("mart", "t2", "raw", "t").await.unwrap();
        assert!(dir.path().join("raw/t/20260101_000000_x_y").exists());

        metastore.drop_table("raw", "t").await.unwrap();
        assert!(metastore.list_tables("raw").await.unwrap().is_empty());
        assert!(
            !dir.path().join("raw/t").exists(),
            "Trino recreates the name only if the directory is gone"
        );
        assert!(!metastore.table_location_exists("raw", "t").await.unwrap());
        assert!(dir.path().join("raw").is_dir(), "the schema stays");
        assert!(matches!(
            metastore.drop_table("raw", "t").await,
            Err(MetastoreError::NotFound(_))
        ));
    }
}
