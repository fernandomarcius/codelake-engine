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
//! written in the statement's staging directory (`.codelake/staging/<id>/`,
//! see `metastore.rs`) and renamed into the table directory as the commit,
//! under the table's metadata lock: no reader ever sees a partial file, and
//! an `INSERT` that fails before the rename (its metadata update included)
//! leaves no row behind. The name starts with a UTC timestamp so that listing
//! order is write order, as with Trino. Files are laid out like Trino's
//! (`INT96` timestamps, see `trino_parquet.rs`).
//!
//! Values follow Trino's Hive connector: timestamps are rounded half up to
//! the catalog's precision on write and on read (a file written by another
//! tool may hold nanoseconds); a `varchar(n)` value longer than `n` is
//! refused on write unless only spaces are cut, and cut to `n` characters on
//! read.
//!
//! Tracing: `hive.scan` records the files a scan selected (count, bytes,
//! names); `hive.write` records the file an `INSERT`/CTAS wrote and its rows.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use codelake_core::trace::{self, TARGET, field};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringArray};
use datafusion::arrow::compute::{CastOptions, cast, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit};
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
use tracing::field::Empty;
use tracing::{Instrument, Span};

use super::catalog::{hive_name, refuse_unsupported};
use super::columns::{HiveColumns, INT96_ZONE_MARKER};
use super::exact_timestamps;
use super::metastore::{ColumnMetadata, FileMetastore, TableMetadata};
use super::optimize::MARKER;
use super::statistics::{MetastoreStatistics, scan_statistics};
use super::trino_parquet::{ROW_GROUP_ROWS, TrinoParquetWriter};
use super::types::{char_column, column_field, declared_length, unsupported};

/// Raw value of `value` (in `from` units) in the coarser `to` units, rounded
/// half up as Trino rounds timestamps (`floor((v + f/2) / f)`).
fn round_down_to(value: i64, factor: i64) -> Option<i64> {
    let value = i128::from(value) + i128::from(factor / 2);
    i64::try_from(value.div_euclid(i128::from(factor))).ok()
}

/// Units per second.
pub(crate) fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Whether `from` holds finer values than `to`.
pub(crate) fn finer(from: TimeUnit, to: TimeUnit) -> bool {
    per_second(from) > per_second(to)
}

/// A timestamp array in a coarser unit, rounded half up (Trino), with the
/// target's zone. Arrow's cast truncates toward zero instead: `.9996` would
/// become `.999`, and `1969-12-31 23:59:59.9984` `.999` instead of `.998`.
pub(crate) fn round_timestamps(array: &ArrayRef, to: TimeUnit, zone: Option<Arc<str>>) -> Result<ArrayRef> {
    let DataType::Timestamp(from, _) = array.data_type() else {
        return Err(DataFusionError::Internal(format!(
            "rounding a {} as a timestamp",
            array.data_type()
        )));
    };
    let factor = per_second(*from) / per_second(to);
    let raw = cast(array, &DataType::Int64)?;
    let raw = raw.as_primitive::<Int64Type>();
    let rounded: Int64Array = raw
        .iter()
        .map(|value| {
            value
                .map(|v| {
                    round_down_to(v, factor).ok_or_else(|| {
                        DataFusionError::Execution("timestamp out of range while rounding".into())
                    })
                })
                .transpose()
        })
        .collect::<Result<_>>()?;
    let rounded: ArrayRef = Arc::new(rounded);
    Ok(cast(&rounded, &DataType::Timestamp(to, zone))?)
}

/// Checks the values of a `varchar(n)` column like Trino's `INSERT`: longer
/// values fail unless only trailing spaces go, which are cut.
fn fit_varchar(array: &ArrayRef, length: usize) -> Result<ArrayRef> {
    let text = cast(array, &DataType::Utf8)?;
    let text = text.as_string::<i32>();
    if text.iter().flatten().all(|v| v.chars().count() <= length) {
        return Ok(Arc::new(text.clone()));
    }
    let fitted: StringArray = text
        .iter()
        .map(|value| {
            value
                .map(|v| {
                    let cut = v.char_indices().nth(length).map_or(v.len(), |(at, _)| at);
                    if v[cut..].chars().all(|c| c == ' ') {
                        Ok(&v[..cut])
                    } else {
                        Err(DataFusionError::Execution(format!(
                            "INVALID_CAST_ARGUMENT: Cannot truncate non-space characters when casting from \
                             varchar to varchar({length}) on INSERT"
                        )))
                    }
                })
                .transpose()
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(fitted))
}

/// Values of a `char(n)` column as Trino stores them: trailing spaces
/// removed (reads pad them back); a longer value fails like Trino's `INSERT`.
fn fit_char(array: &ArrayRef, length: usize) -> Result<ArrayRef> {
    let text = cast(array, &DataType::Utf8)?;
    let text = text.as_string::<i32>();
    let fitted: StringArray = text
        .iter()
        .map(|value| {
            value
                .map(|v| {
                    let trimmed = v.trim_end_matches(' ');
                    if trimmed.chars().count() <= length {
                        Ok(trimmed)
                    } else {
                        Err(DataFusionError::Execution(format!(
                            "INVALID_CAST_ARGUMENT: Cannot truncate non-space characters when casting from \
                             varchar to char({length}) on INSERT"
                        )))
                    }
                })
                .transpose()
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(fitted))
}

/// `batch` in the table's column types, as an `INSERT` or CTAS stores it:
/// timestamps rounded to the column's precision, `varchar(n)` and `char(n)`
/// checked. A value the column type cannot hold (an instant outside
/// 1677–2262 in a NANOSECONDS catalog) fails the statement: Arrow's default
/// cast would store NULL instead.
pub(crate) fn conform_for_write(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| match (column.data_type(), field.data_type()) {
            (DataType::Timestamp(from, _), DataType::Timestamp(to, zone)) if finer(*from, *to) => {
                round_timestamps(column, *to, zone.clone())
            }
            _ => {
                let cast = if column.data_type() == field.data_type() {
                    Arc::clone(column)
                } else {
                    let checked = CastOptions {
                        safe: false,
                        ..CastOptions::default()
                    };
                    cast_with_options(column, field.data_type(), &checked)?
                };
                match (declared_length(field), char_column(field)) {
                    (Some(length), _) => fit_varchar(&cast, length),
                    (_, Some(length)) => fit_char(&cast, length),
                    _ => Ok(cast),
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

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

/// `schema` with its fields renamed to the `columns`' stored names (same
/// order, one column per field).
pub(crate) fn stored_names(schema: &SchemaRef, columns: &[ColumnMetadata]) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, column)| field.as_ref().clone().with_name(column.written_name()))
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// File names listed on a `hive.scan` span; the count is always complete.
const MAX_TRACED_FILE_NAMES: usize = 50;

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
    // An empty file (a writer that died before its first byte) holds no rows
    // and is no Parquet file: Trino skips it when reading and compacting.
    if meta.size == 0 {
        return false;
    }
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

/// How a scan reads the stored values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reading {
    /// As the table shows them: timestamps rounded half up to the table's
    /// units, `varchar(n)` values cut, `char(n)` values padded.
    Table,
    /// As they are stored (compaction must not change them).
    Raw,
    /// Only the timestamps a file stores in nanoseconds, as stored; any
    /// other column reads as NULL (see `exact_timestamps.rs`).
    NanosOnly,
}

#[derive(Debug, Clone)]
pub(crate) struct HiveTable {
    pub(crate) metastore: FileMetastore,
    pub(crate) store_url: ObjectStoreUrl,
    pub(crate) schema_name: String,
    pub(crate) table: String,
    pub(crate) schema: SchemaRef,
    /// The columns as the metastore records them.
    pub(crate) columns: Vec<ColumnMetadata>,
    /// Statistics recorded in the metastore when the table was resolved.
    pub(crate) statistics: Arc<MetastoreStatistics>,
    /// Identity of the table this handle was made for (`None` for tables no
    /// `CREATE OR REPLACE` wrote): a table of the same name with another
    /// identity replaced it, and this handle must not read or write it.
    pub(crate) table_id: Option<String>,
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
        // The engine sees every column by its folded name (identifiers are
        // case-insensitive); the metastore and the files keep the case it
        // was written with (ISS-018). Two stored names that fold to one
        // would be one column read twice: refused, never a guess.
        let mut seen: HashMap<String, &str> = HashMap::new();
        for column in &metadata.data_columns {
            if let Some(other) = seen.insert(hive_name(&column.name), column.written_name()) {
                return Err(DataFusionError::Plan(format!(
                    "{schema}.{table} has columns `{other}` and `{}`, which differ only by case: \
                     column names are case-insensitive, so the table cannot be read",
                    column.written_name()
                )));
            }
        }
        let precision = metastore.timestamp_precision();
        let fields: Vec<_> = metadata
            .data_columns
            .iter()
            .map(|c| column_field(&hive_name(&c.name), &c.hive_type, precision))
            .collect();
        Ok(Self {
            metastore,
            store_url,
            schema_name: schema.to_owned(),
            table: table.to_owned(),
            schema: Arc::new(Schema::new(fields)),
            columns: metadata.data_columns.clone(),
            statistics: Arc::new(MetastoreStatistics::from_metadata(metadata)),
            table_id: metadata.table_id().map(str::to_owned),
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

    /// The table's columns spelled as they were written (ISS-018): the
    /// column names of the files it writes.
    pub(crate) fn stored_schema(&self) -> SchemaRef {
        stored_names(&self.schema, &self.columns)
    }

    /// Each column's stored name, by the engine's (folded) name.
    fn stored_names(&self) -> HashMap<String, String> {
        self.schema
            .fields()
            .iter()
            .zip(&self.columns)
            .map(|(field, column)| (field.name().clone(), column.written_name().to_owned()))
            .collect()
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
        // A table dropped (or replaced by a view) since the query resolved it
        // has no files left to list: an error, never an empty result. DROP
        // holds these locks while it removes the metadata and the files.
        let current = self
            .metastore
            .table(&self.schema_name, &self.table)
            .await
            .map_err(external)?
            .filter(|metadata| !metadata.is_view());
        let Some(current) = current else {
            return Err(DataFusionError::Plan(format!(
                "table {}.{} does not exist anymore: it was dropped while the query ran",
                self.schema_name, self.table
            )));
        };
        // Replaced (`CREATE OR REPLACE`, which holds these locks while it
        // swaps) since the statement was planned (while it ran, or before a
        // kept plan runs again): its files belong to another table, with
        // other columns and rows. An error, never a mix of the two tables in
        // one query; a client protocol that keeps plans plans the statement
        // again before running it (the PostgreSQL wire does, ISS-017).
        if current.table_id() != self.table_id.as_deref() {
            return Err(DataFusionError::Plan(format!(
                "table {}.{} was replaced (CREATE OR REPLACE) after this statement was planned; \
                 plan the statement again (run it again, or prepare it again)",
                self.schema_name, self.table
            )));
        }
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

    /// The configuration of a Parquet scan of `groups` in the table's schema: columns match by name
    /// ignoring case, a column a file lacks (added later) reads as NULL, one
    /// stored with a type the table's cannot hold is an error (`columns.rs`),
    /// timestamps round half up to the catalog's precision and `varchar(n)`
    /// values are cut to `n` characters, as Trino reads them.
    pub(crate) fn scan_files(
        &self,
        runtime: &RuntimeEnv,
        groups: Vec<Vec<PartitionedFile>>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<FileScanConfigBuilder> {
        self.scan_config(
            runtime,
            Arc::clone(&self.schema),
            Reading::Table,
            groups,
            projection,
            limit,
        )
    }

    /// [`HiveTable::scan_files`] into `schema` (the table's columns, possibly
    /// at another timestamp precision), reading the stored values as
    /// `reading` says. Each partition reads the files of its group, in order:
    /// `exact_timestamps.rs` pairs two such scans of the same files row by
    /// row, which `DataFusion`'s default (idle partitions take files queued
    /// for their siblings) would pair with rows of other files.
    pub(crate) fn scan_files_as(
        &self,
        runtime: &RuntimeEnv,
        schema: SchemaRef,
        reading: Reading,
        groups: Vec<Vec<PartitionedFile>>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = self
            .scan_config(runtime, schema, reading, groups, projection, limit)?
            .with_preserve_order(true);
        Ok(DataSourceExec::from_data_source(config.build()))
    }

    /// The scan configuration of [`HiveTable::scan_files_as`], for a caller
    /// that adds to it (the planner's statistics).
    fn scan_config(
        &self,
        runtime: &RuntimeEnv,
        schema: SchemaRef,
        reading: Reading,
        groups: Vec<Vec<PartitionedFile>>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<FileScanConfigBuilder> {
        if runtime.object_store(&self.store_url).is_err() {
            runtime.register_object_store(self.store_url.as_ref(), Arc::clone(self.metastore.store()));
        }
        // `INT96` is decoded in one unit for the whole file: never coarser
        // than microseconds, so a finer value is rounded (below), not cut.
        // In nanoseconds, a value outside 1677–2262 wraps into another
        // instant: see `exact_timestamps.rs` for how those are read.
        let finest = schema
            .fields()
            .iter()
            .filter_map(|field| match field.data_type() {
                DataType::Timestamp(unit, _) => Some(*unit),
                _ => None,
            })
            .fold(TimeUnit::Microsecond, |a, b| if finer(b, a) { b } else { a });
        let int96 = match finest {
            TimeUnit::Nanosecond => "ns",
            _ => "us",
        };
        let mut options = TableParquetOptions::default();
        options.global.coerce_int96 = Some(int96.into());
        // `INT96` instants read in the catalog's zone (`columns.rs`): the
        // marker tells them from other timestamps, which keep their values.
        let zone = Some(self.metastore.parquet_time_zone()).filter(|zone| !zone.is_utc());
        if zone.is_some() {
            options.global.coerce_int96_tz = Some(INT96_ZONE_MARKER.into());
        }
        let source = ParquetSource::new(schema).with_table_parquet_options(options);
        let columns = HiveColumns {
            table: format!("{}.{}", self.schema_name, self.table),
            raw: reading != Reading::Table,
            nanos_only: reading == Reading::NanosOnly,
            zone,
            stored: Arc::new(self.stored_names()),
        };
        Ok(
            FileScanConfigBuilder::new(self.store_url.clone(), Arc::new(source))
                .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .with_expr_adapter(Some(Arc::new(columns))),
        )
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

impl HiveTable {
    /// See [`crate::distributable`].
    pub(crate) fn distributable(&self) -> bool {
        !exact_timestamps::has_nanoseconds(&self.schema, None)
    }
}

#[async_trait]
impl TableProvider for HiveTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // Applied again above the scan: a filter on a char column never
        // prunes by statistics of the stored (unpadded) text (`types.rs`).
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let span = tracing::info_span!(
            target: TARGET,
            "hive.scan",
            otel.status_code = Empty,
            otel.status_description = Empty,
            codelake.hive.schema = self.schema_name.as_str(),
            codelake.hive.table = self.table.as_str(),
            codelake.hive.files = Empty,
            codelake.hive.bytes = Empty,
            codelake.hive.file_names = Empty,
        );
        refuse_unsupported(&self.schema, projection)?;
        let files = self
            .committed_data_files()
            .instrument(span.clone())
            .await
            .inspect_err(|err| trace::record_error(&span, err))?;
        record_files(&span, &files);
        self.register_store(state);
        let statistics = scan_statistics(self, state, &files).await;
        let partitions = state.config().target_partitions().max(1);
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); partitions.min(files.len().max(1))];
        let group_count = groups.len();
        for (index, meta) in files.into_iter().enumerate() {
            groups[index % group_count].push(PartitionedFile::new_from_meta(meta));
        }
        // A NANOSECONDS catalog cannot take `INT96` values in nanoseconds
        // from the Parquet reader as they come (see `exact_timestamps.rs`).
        if exact_timestamps::has_nanoseconds(&self.schema, projection) {
            return exact_timestamps::nanosecond_scan(
                self,
                state.runtime_env(),
                groups,
                projection,
                limit,
                &statistics,
            );
        }
        // Statistics are all inexact (see `statistics.rs`): they change the
        // plan, never an answer.
        let config = self
            .scan_files(state.runtime_env(), groups, projection, limit)?
            .with_statistics(statistics);
        Ok(DataSourceExec::from_data_source(config.build()))
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
        if let Some(reason) = self.schema.fields().iter().find_map(|f| unsupported(f)) {
            return Err(DataFusionError::NotImplemented(format!(
                "INSERT into {}.{}: {reason}",
                self.schema_name, self.table
            )));
        }
        self.register_store(state);
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(HiveSink::new(self.clone())),
            None,
        )))
    }
}

/// Records what a scan will read: file count, total bytes, first names.
fn record_files(span: &Span, files: &[ObjectMeta]) {
    let bytes: u64 = files.iter().map(|meta| meta.size).sum();
    let names: Vec<&str> = files
        .iter()
        .take(MAX_TRACED_FILE_NAMES)
        .filter_map(|meta| meta.location.filename())
        .collect();
    let mut listed = names.join(",");
    if files.len() > MAX_TRACED_FILE_NAMES {
        listed.push_str(",…");
    }
    span.record("codelake.hive.files", files.len());
    span.record("codelake.hive.bytes", bytes);
    span.record("codelake.hive.file_names", listed.as_str());
}

/// Writes one Parquet file per `INSERT` or CTAS.
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
    pub(crate) fn new(table: HiveTable) -> Self {
        Self { table }
    }

    /// Streams `data` into `path`; returns the number of rows written.
    async fn write_file(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
        path: &Path,
    ) -> Result<u64> {
        let store = Arc::clone(self.table.metastore.store());
        // Files carry each column's stored spelling (ISS-018), so another
        // reader of the files (SQL Server `OPENROWSET`, pandas) sees the
        // names as they were written.
        let mut writer = TrinoParquetWriter::try_new(
            ParquetObjectWriter::new(store, path.clone()),
            self.table.stored_schema(),
            ROW_GROUP_ROWS,
        )?
        .in_time_zone(self.table.metastore.parquet_time_zone());
        let reservation =
            MemoryConsumer::new(format!("HiveSink[{}]", self.table.table)).register(context.memory_pool());
        let mut rows = 0_u64;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            writer
                .write(&conform_for_write(&self.table.schema, &batch)?)
                .await?;
            rows += batch.num_rows() as u64;
            // The encoder buffers the open row group: account for it.
            reservation.try_resize(writer.memory_size())?;
        }
        writer.close().await?;
        reservation.free();
        Ok(rows)
    }

    /// Writes one data file into the private directory `dir` (a statement's
    /// staging directory), traced as `hive.write`. No rows, no file.
    pub(crate) async fn write_into(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
        dir: &Path,
    ) -> Result<u64> {
        let name = data_file_name(SystemTime::now());
        self.traced(&name, async {
            let path = dir.clone().join(name.as_str());
            let rows = self.write_file(data, context, &path).await;
            if !matches!(rows, Ok(rows) if rows > 0) {
                // No rows, no file: the sink downstream must not see empty files.
                let _ = self.table.metastore.store().delete(&path).await;
            }
            rows
        })
        .await
    }

    /// Runs `write` in a `hive.write` span naming the file and its rows.
    async fn traced(&self, name: &str, write: impl Future<Output = Result<u64>>) -> Result<u64> {
        let span = tracing::info_span!(
            target: TARGET,
            "hive.write",
            otel.status_code = Empty,
            otel.status_description = Empty,
            codelake.hive.schema = self.table.schema_name.as_str(),
            codelake.hive.table = self.table.table.as_str(),
            codelake.hive.file = name,
            db.response.returned_rows = Empty,
        );
        let written = write.instrument(span.clone()).await;
        match &written {
            Ok(rows) => {
                span.record(field::ROWS, rows);
                if *rows == 0 {
                    span.record("codelake.hive.file", "<none: no rows>");
                }
            }
            Err(err) => trace::record_error(&span, err),
        }
        written
    }
}

#[async_trait]
impl DataSink for HiveSink {
    fn schema(&self) -> &SchemaRef {
        &self.table.schema
    }

    /// `INSERT`: the file is written in a staging directory, then committed
    /// into the table (`FileMetastore::commit_insert`); whatever fails before
    /// the commit leaves nothing visible.
    async fn write_all(&self, data: SendableRecordBatchStream, context: &Arc<TaskContext>) -> Result<u64> {
        let metastore = &self.table.metastore;
        let staging = metastore.staging();
        let name = data_file_name(SystemTime::now());
        let staged = staging.dir.clone().join(name.as_str());
        let result = self
            .traced(&name, async {
                let rows = self.write_file(data, context, &staged).await?;
                if rows > 0 {
                    metastore
                        .commit_insert(
                            (&self.table.schema_name, &self.table.table),
                            self.table.table_id.as_deref(),
                            &self.table.columns,
                            &staged,
                            &name,
                        )
                        .await
                        .map_err(|err| match err {
                            super::metastore::MetastoreError::Conflict(message) => {
                                DataFusionError::Plan(message)
                            }
                            other => external(other),
                        })?;
                }
                Ok(rows)
            })
            .await;
        metastore.discard(&staging).await;
        result
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
        let empty = ObjectMeta {
            size: 0,
            ..meta("pilot/t1/20260923_235834_00004_h47gc_74f5")
        };
        assert!(
            !is_data_file(&dir, &empty),
            "zero-byte files are skipped, as in Trino"
        );
    }
}
