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

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};

use super::crc::{crc_name, hadoop_crc};

pub(crate) const MANAGED_TABLE: &str = "MANAGED_TABLE";
pub(crate) const VIRTUAL_VIEW: &str = "VIRTUAL_VIEW";
const METADATA_FILE: &str = ".trinoSchema";
const PERMISSIONS_DIR: &str = ".trinoPermissions";
const WRITER_VERSION: &str = "codelake";

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
    /// Trino 431 refuses metadata without this key ("Could not read table
    /// schema"), so it is always written, empty when unknown.
    #[serde(default)]
    pub(crate) serde_parameters: BTreeMap<String, String>,
    /// Per-column statistics, kept opaque. Always written, like
    /// `serdeParameters`: Trino 431 requires the key.
    #[serde(default)]
    pub(crate) column_statistics: BTreeMap<String, serde_json::Value>,
    /// Fields this version does not model (bucketing, original format, …).
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

impl TableMetadata {
    pub(crate) fn is_view(&self) -> bool {
        self.table_type == VIRTUAL_VIEW
    }

    /// New managed Parquet table, the only kind `CodeLake` creates.
    pub(crate) fn managed_parquet(owner: &str, columns: Vec<ColumnMetadata>) -> Self {
        Self {
            writer_version: Some(WRITER_VERSION.into()),
            owner: Some(owner.into()),
            table_type: MANAGED_TABLE.into(),
            data_columns: columns,
            partition_columns: Vec::new(),
            parameters: BTreeMap::from([
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

    /// Statistics describe the files at the time they were computed; after a
    /// write they would lie to Trino's cost-based optimizer. Drop them.
    pub(crate) fn invalidate_statistics(&mut self) {
        self.column_statistics.clear();
        for key in [
            "numFiles",
            "numRows",
            "rawDataSize",
            "totalSize",
            "STATS_GENERATED_VIA_STATS_TASK",
        ] {
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
}

/// Metadata access for one catalog root.
#[derive(Debug, Clone)]
pub(crate) struct FileMetastore {
    store: Arc<dyn ObjectStore>,
    root: Path,
}

impl FileMetastore {
    pub(crate) fn new(store: Arc<dyn ObjectStore>, root: Path) -> Self {
        Self { store, root }
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

    async fn read_json<T: for<'de> Deserialize<'de>>(&self, path: &Path) -> MetastoreResult<Option<T>> {
        match self.store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|source| MetastoreError::Json {
                        path: path.to_string(),
                        source,
                    })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Writes `bytes` and its Hadoop checksum. The checksum goes first so a
    /// reader never sees new content with a stale checksum.
    async fn write_with_crc(&self, path: &Path, bytes: Vec<u8>) -> MetastoreResult<()> {
        let name = path.filename().unwrap_or_default().to_owned();
        let parent = Path::from_iter(path.parts().take(path.parts().count().saturating_sub(1)));
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
        self.read_json(&self.table_file(schema, table)).await
    }

    pub(crate) async fn create_schema(&self, schema: &str, owner: &str) -> MetastoreResult<()> {
        if self.schema_exists(schema).await? {
            return Err(MetastoreError::AlreadyExists(format!("schema {schema}")));
        }
        let metadata = SchemaMetadata {
            writer_version: Some(WRITER_VERSION.into()),
            owner_name: Some(owner.into()),
            owner_type: Some("USER".into()),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        };
        self.write_json(&self.schema_file(schema), &metadata).await
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
        let path = self
            .table_dir(schema, table)
            .join(PERMISSIONS_DIR)
            .join(format!("user_{owner}"));
        self.write_json(&path, &grants).await
    }

    /// Drops statistics after new data files were written, when any exist.
    pub(crate) async fn invalidate_statistics(&self, schema: &str, table: &str) -> MetastoreResult<()> {
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

    /// Removes the table directory: metadata first (the table disappears
    /// atomically), then data files.
    pub(crate) async fn drop_table(&self, schema: &str, table: &str) -> MetastoreResult<()> {
        let metadata = self.table_file(schema, table);
        if self.store.head(&metadata).await.is_err() {
            return Err(MetastoreError::NotFound(format!("table {schema}.{table}")));
        }
        self.store.delete(&metadata).await?;
        let dir = self.table_dir(schema, table);
        let objects: Vec<_> = self.store.list(Some(&dir)).try_collect().await?;
        for object in objects {
            self.store.delete(&object.location).await?;
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
        (dir, FileMetastore::new(Arc::new(store), Path::default()))
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
        assert!(table.column_statistics.contains_key("nome"));
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
        metastore.create_schema("pilot", "dbt").await.unwrap();
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

    #[tokio::test]
    async fn schema_and_table_lifecycle() {
        let (dir, metastore) = scratch();
        metastore.create_schema("raw", "dbt").await.unwrap();
        assert!(matches!(
            metastore.create_schema("raw", "dbt").await,
            Err(MetastoreError::AlreadyExists(_))
        ));
        let mut table = TableMetadata::managed_parquet(
            "dbt",
            vec![ColumnMetadata {
                name: "id".into(),
                hive_type: "bigint".into(),
                comment: None,
                properties: BTreeMap::new(),
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

        metastore.drop_table("raw", "t").await.unwrap();
        assert!(metastore.list_tables("raw").await.unwrap().is_empty());
        assert!(!dir.path().join("raw/t/20260101_000000_x_y").exists());
        assert!(matches!(
            metastore.drop_table("raw", "t").await,
            Err(MetastoreError::NotFound(_))
        ));
    }
}
