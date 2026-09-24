//! A Trino-compatible Hive table: Parquet files in one directory.
//!
//! Reads list the visible files (names not starting with `.` or `_`, which
//! skips `.trinoSchema`, `.crc` and `.trinoPermissions`) and scan them with
//! `DataFusion`'s Parquet reader. `INT96` timestamps written by Trino are read
//! as milliseconds, Trino's default Hive timestamp precision. Columns added
//! after a file was written read as NULL.
//!
//! Each `INSERT` appends exactly one new file and never rewrites existing ones:
//! a SQL Server sink downstream loads each file once, by name. The file is
//! written under a hidden temporary name and renamed when complete, so no
//! reader ever sees a partial file. The name starts with a UTC timestamp so
//! that listing order is write order, as with Trino. Files are laid out like
//! Trino's (`INT96` timestamps, see `trino_parquet.rs`).

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::TableParquetOptions;
use datafusion::datasource::TableType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::arrow::async_writer::ParquetObjectWriter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStoreExt};

use super::metastore::{FileMetastore, TableMetadata};
use super::optimize::MARKER;
use super::trino_parquet::{ROW_GROUP_ROWS, TrinoParquetWriter};
use super::types::arrow_type;

/// Casts `batch` to the declared column types of `schema`.
pub(crate) fn conform(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

static SEQUENCE: AtomicU32 = AtomicU32::new(0);

/// `YYYYMMDD_HHMMSS_<seq>_codelake_<uuid>`: sortable by write time like
/// Trino's `<query_id>_<uuid>`.
pub(crate) fn data_file_name(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let days = i64::try_from(secs / 86_400).unwrap_or_default();
    let (y, m, d) = civil_from_days(days);
    let rest = secs % 86_400;
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed) % 100_000;
    format!(
        "{y:04}{m:02}{d:02}_{:02}{:02}{:02}_{sequence:05}_codelake_{}",
        rest / 3600,
        (rest % 3600) / 60,
        rest % 60,
        uuid::Uuid::new_v4()
    )
}

/// Days since 1970-01-01 to (year, month, day) — Howard Hinnant's algorithm.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn is_data_file(dir: &Path, meta: &ObjectMeta) -> bool {
    let Some(relative) = meta.location.prefix_match(dir) else {
        return false;
    };
    let parts: Vec<_> = relative.collect();
    // Direct children only (non-partitioned tables), no hidden or marker files.
    parts.len() == 1 && !parts[0].as_ref().starts_with(['.', '_'])
}

pub(crate) fn external(err: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[derive(Debug, Clone)]
pub(crate) struct HiveTable {
    pub(crate) metastore: FileMetastore,
    pub(crate) store_url: ObjectStoreUrl,
    pub(crate) schema_name: String,
    pub(crate) table: String,
    pub(crate) schema: SchemaRef,
}

impl HiveTable {
    /// The table `schema.table` described by `metadata`.
    pub(crate) fn from_metadata(
        metastore: FileMetastore,
        store_url: ObjectStoreUrl,
        schema: &str,
        table: &str,
        metadata: &TableMetadata,
    ) -> Result<Self> {
        if metadata.storage_format.as_deref().is_some_and(|f| f != "PARQUET") {
            return Err(DataFusionError::NotImplemented(format!(
                "{schema}.{table}: only PARQUET hive tables are supported"
            )));
        }
        if !metadata.partition_columns.is_empty() {
            return Err(DataFusionError::NotImplemented(format!(
                "{schema}.{table}: partitioned hive tables are not supported yet"
            )));
        }
        let fields = metadata
            .data_columns
            .iter()
            .map(|c| Ok(Field::new(&c.name, arrow_type(&c.hive_type)?, true)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            metastore,
            store_url,
            schema_name: schema.to_owned(),
            table: table.to_owned(),
            schema: Arc::new(Schema::new(fields)),
        })
    }

    /// The same table as the metastore describes it now (its columns may
    /// have changed since this handle was made).
    pub(crate) async fn reload(&self) -> Result<Self> {
        let metadata = self
            .metastore
            .table(&self.schema_name, &self.table)
            .await
            .map_err(external)?
            .filter(|metadata| !metadata.is_view())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "table {}.{} does not exist anymore",
                    self.schema_name, self.table
                ))
            })?;
        Self::from_metadata(
            self.metastore.clone(),
            self.store_url.clone(),
            &self.schema_name,
            &self.table,
            &metadata,
        )
    }

    pub(crate) fn dir(&self) -> Path {
        self.metastore.table_dir(&self.schema_name, &self.table)
    }

    /// Data files as seen between two commits: an `OPTIMIZE` in this process
    /// or in another one on the same local warehouse never swaps files while
    /// the list is being taken.
    async fn committed_data_files(&self) -> Result<Vec<ObjectMeta>> {
        let locks = self.metastore.table_locks(&self.schema_name, &self.table);
        let _between_commits = locks.commit.read().await;
        // A warehouse this process cannot lock (read-only) has no swap of
        // this process to wait for; other writers are then not excluded.
        let _other_processes = match self
            .metastore
            .table_file_lock(&self.schema_name, &self.table, false)
            .await
        {
            Ok(lock) => Some(lock),
            Err(err) => {
                tracing::debug!(table = %self.table, error = %err, "listing without the cross-process lock");
                None
            }
        };
        let (files, pending) = self.listing().await?;
        if pending {
            return Err(DataFusionError::Plan(format!(
                "table {}.{} has an unfinished OPTIMIZE ({MARKER} in its directory): the next OPTIMIZE \
                 of the table or opening the catalog again finishes it; if that fails, inspect the directory",
                self.schema_name, self.table
            )));
        }
        Ok(files)
    }

    /// A Parquet scan of `groups` in the table's schema: columns a file lacks
    /// (added later) read as NULL, `INT96` timestamps as milliseconds.
    pub(crate) fn scan_files(
        &self,
        runtime: &RuntimeEnv,
        groups: Vec<Vec<PartitionedFile>>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if runtime.object_store(&self.store_url).is_err() {
            runtime.register_object_store(self.store_url.as_ref(), Arc::clone(self.metastore.store()));
        }
        let mut options = TableParquetOptions::default();
        // Trino writes Hive timestamps as INT96; read them at its default precision.
        options.global.coerce_int96 = Some("ms".into());
        let source = ParquetSource::new(Arc::clone(&self.schema)).with_table_parquet_options(options);
        let config = FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(source))
            .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    pub(crate) async fn data_files(&self) -> Result<Vec<ObjectMeta>> {
        Ok(self.listing().await?.0)
    }

    /// Data files sorted by name, and whether an `OPTIMIZE` marker is present.
    async fn listing(&self) -> Result<(Vec<ObjectMeta>, bool)> {
        let dir = self.dir();
        let marker = dir.clone().join(MARKER);
        let all: Vec<ObjectMeta> = self
            .metastore
            .store()
            .list(Some(&dir))
            .try_collect()
            .await
            .map_err(external)?;
        let pending = all.iter().any(|meta| meta.location == marker);
        let mut files: Vec<ObjectMeta> = all.into_iter().filter(|meta| is_data_file(&dir, meta)).collect();
        files.sort_by(|a, b| a.location.cmp(&b.location));
        Ok((files, pending))
    }

    fn register_store(&self, state: &dyn Session) {
        let runtime = state.runtime_env();
        if runtime.object_store(&self.store_url).is_err() {
            runtime.register_object_store(self.store_url.as_ref(), Arc::clone(self.metastore.store()));
        }
    }
}

#[async_trait]
impl TableProvider for HiveTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // Row-group statistics prune files; the filter is still applied above.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let files = self.committed_data_files().await?;
        let partitions = state.config().target_partitions().max(1);
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); partitions.min(files.len().max(1))];
        let group_count = groups.len();
        for (index, meta) in files.into_iter().enumerate() {
            groups[index % group_count].push(PartitionedFile::new_from_meta(meta));
        }
        self.scan_files(state.runtime_env(), groups, projection, limit)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(format!(
                "hive tables only support INSERT (append), not {insert_op}"
            )));
        }
        self.register_store(state);
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(HiveSink { table: self.clone() }),
            None,
        )))
    }
}

/// Writes one Parquet file per `INSERT`.
#[derive(Debug)]
pub(crate) struct HiveSink {
    pub(crate) table: HiveTable,
}

impl DisplayAs for HiveSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HiveSink({}.{})", self.table.schema_name, self.table.table)
    }
}

impl HiveSink {
    /// Streams `data` into `temporary`; returns the number of rows written.
    async fn write_file(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
        temporary: &Path,
    ) -> Result<u64> {
        let store = Arc::clone(self.table.metastore.store());
        let mut writer = TrinoParquetWriter::try_new(
            ParquetObjectWriter::new(store, temporary.clone()),
            Arc::clone(&self.table.schema),
            ROW_GROUP_ROWS,
        )?;
        let reservation =
            MemoryConsumer::new(format!("HiveSink[{}]", self.table.table)).register(context.memory_pool());
        let mut rows = 0_u64;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            writer.write(&conform(&self.table.schema, &batch)?).await?;
            rows += batch.num_rows() as u64;
            // The encoder buffers the open row group: account for it.
            reservation.try_resize(writer.memory_size())?;
        }
        writer.close().await?;
        reservation.free();
        Ok(rows)
    }
}

#[async_trait]
impl DataSink for HiveSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.table.schema
    }

    async fn write_all(&self, data: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
        let rows = self.write_data(data, context).await?;
        if rows > 0 {
            self.table
                .metastore
                .invalidate_statistics(&self.table.schema_name, &self.table.table)
                .await
                .map_err(external)?;
        }
        Ok(rows)
    }
}

impl HiveSink {
    /// Writes one data file (hidden name, then renamed). Metadata untouched:
    /// CTAS commits the table only after this returns.
    pub(crate) async fn write_data(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let dir = self.table.dir();
        let name = data_file_name(SystemTime::now());
        let temporary = dir.clone().join(format!(".{name}.inprogress"));
        let store = Arc::clone(self.table.metastore.store());
        let written = self.write_file(data, context, &temporary).await;
        let rows = match written {
            Ok(rows) => rows,
            Err(err) => {
                let _ = store.delete(&temporary).await;
                return Err(err);
            }
        };
        if rows == 0 {
            // No rows, no file: the sink downstream must not see empty files.
            let _ = store.delete(&temporary).await;
            return Ok(0);
        }
        store
            .rename(&temporary, &dir.join(name))
            .await
            .map_err(external)?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_sort_by_write_time() {
        let earlier = UNIX_EPOCH + std::time::Duration::from_secs(1_790_000_000);
        let later = earlier + std::time::Duration::from_secs(1);
        let a = data_file_name(earlier);
        let b = data_file_name(later);
        assert!(a < b, "{a} < {b}");
        assert!(a.starts_with("20260921_141320_"), "{a}");
        assert!(a.contains("_codelake_"));
        assert!(!a.starts_with(['.', '_']));
    }

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_454), (2026, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    #[allow(
        clippy::default_trait_access,
        reason = "chrono is not a direct dependency of this crate"
    )]
    fn only_visible_direct_children_are_data() {
        let dir = Path::from("pilot/t1");
        let meta = |p: &str| ObjectMeta {
            location: Path::from(p),
            last_modified: Default::default(),
            size: 1,
            e_tag: None,
            version: None,
        };
        assert!(is_data_file(
            &dir,
            &meta("pilot/t1/20260923_235834_00003_h47gc_74f5")
        ));
        assert!(!is_data_file(&dir, &meta("pilot/t1/.trinoSchema")));
        assert!(!is_data_file(&dir, &meta("pilot/t1/..trinoSchema.crc")));
        assert!(!is_data_file(&dir, &meta("pilot/t1/.x.inprogress")));
        assert!(!is_data_file(&dir, &meta("pilot/t1/_SUCCESS")));
        assert!(!is_data_file(
            &dir,
            &meta("pilot/t1/.trinoPermissions/user_trino")
        ));
        assert!(!is_data_file(&dir, &meta("pilot/t2/file")));
    }
}
