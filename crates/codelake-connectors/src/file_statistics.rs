//! Table statistics summed from Parquet footers, for connectors that list and
//! scan Parquet files themselves (the hive catalog today).
//!
//! The planner uses them to choose join sides (the smaller input builds the
//! hash table) and to collect a small input once instead of repartitioning
//! it. They steer the plan and never the result: every value is `Inexact`, and
//! anything that cannot be trusted is `Absent`. Being `Inexact` alone is not
//! enough: `DataFusion` turns some estimates into exact counts on the way up
//! (an estimate of 0 rows, or below an `OFFSET`), so the engine answers
//! aggregates from statistics only over sources whose counts are exact (see
//! `codelake_core::statistics_guard`).
//!
//! Min/max are kept only where the file orders values as the engine does:
//! signed integers, dates and decimals compared by value, strings compared as
//! unsigned bytes, as declared by the column order in the footer (integers
//! are signed under every writer, so they need no declaration). Floating point
//! (NaN is left out of Parquet bounds but sorts last in the engine), `INT96`
//! timestamps (undefined order), binary and booleans report no bounds. A
//! column whose type in the file differs from the table's (the reader casts
//! it) reports no bounds and no null count.
//!
//! Cost is bounded: footers go through the runtime's file metadata cache
//! (`memory.metadata_cache_limit`), at most [`MAX_FOOTERS`] per scan (larger
//! tables are extrapolated from an even sample, by file size), a few at a
//! time, and the bytes held while reading are reserved from the query memory
//! pool. When the pool refuses, a footer does not parse or anything else goes
//! wrong, the statistics are unknown and planning proceeds as without them.

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use datafusion::parquet::arrow::parquet_to_arrow_schema;
use datafusion::parquet::basic::{ColumnOrder, SortOrder, Type as PhysicalType};
use datafusion::parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use datafusion::parquet::schema::types::ColumnDescriptor;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

/// Footers read per scan; a table with more files is extrapolated from an
/// even sample of this many.
pub(crate) const MAX_FOOTERS: usize = 256;
/// Footers read at the same time.
const CONCURRENT_FOOTERS: usize = 8;
/// Bytes of an Arrow string or binary offset, per row.
const OFFSET_BYTES: u64 = 4;

/// Statistics of the table made of `files`, for `schema` (the table schema,
/// every column). Never fails: problems give unknown statistics.
pub(crate) async fn footer_statistics(
    store: &Arc<dyn ObjectStore>,
    files: &[ObjectMeta],
    schema: &SchemaRef,
    runtime: &Arc<RuntimeEnv>,
    size_hint: Option<usize>,
    consumer: &str,
) -> Statistics {
    let reservation =
        MemoryConsumer::new(format!("FooterStatistics[{consumer}]")).register(&runtime.memory_pool);
    let limits = FooterLimits {
        max_footers: MAX_FOOTERS,
        size_hint,
    };
    let summary = summarize(store, files, schema, runtime, limits, &reservation).await;
    reservation.free();
    match summary {
        Ok(summary) => summary.into_statistics(),
        Err(err) => {
            tracing::debug!(table = consumer, error = %err, "footer statistics unavailable");
            Statistics::new_unknown(schema)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FooterLimits {
    max_footers: usize,
    /// Bytes fetched from the end of a file in one request, as the scan does.
    size_hint: Option<usize>,
}

async fn summarize(
    store: &Arc<dyn ObjectStore>,
    files: &[ObjectMeta],
    schema: &SchemaRef,
    runtime: &Arc<RuntimeEnv>,
    limits: FooterLimits,
    reservation: &MemoryReservation,
) -> Result<TableSummary> {
    let FooterLimits {
        max_footers,
        size_hint,
    } = limits;
    let sample = even_sample(files, max_footers);
    let cache = runtime.cache_manager.get_file_metadata_cache();
    let owned: Vec<ObjectMeta> = sample.iter().map(|meta| (*meta).clone()).collect();
    let summaries = futures::stream::iter(owned)
        .map(move |meta| {
            let cache = Arc::clone(&cache);
            async move {
                // The prefetch buffer, then the decoded footer, live while it is read.
                let prefetch = size_hint.map_or(0, |hint| {
                    hint.min(usize::try_from(meta.size).unwrap_or(usize::MAX))
                });
                reservation.try_grow(prefetch)?;
                let fetched = DFParquetMetadata::new(store.as_ref(), &meta)
                    .with_metadata_size_hint(size_hint)
                    .with_file_metadata_cache(Some(cache))
                    .fetch_metadata()
                    .await;
                reservation.shrink(prefetch);
                let metadata = fetched?;
                let held = metadata.memory_size();
                reservation.try_grow(held)?;
                let summary = file_summary(&metadata, schema);
                drop(metadata);
                reservation.shrink(held);
                summary.ok_or_else(|| {
                    DataFusionError::Execution(format!("{}: footer row counts are invalid", meta.location))
                })
            }
        })
        .buffer_unordered(CONCURRENT_FOOTERS)
        .try_fold(TableSummary::empty(schema), |total, file| async move {
            Ok(total.merge(file))
        })
        .await?;
    if sample.len() == files.len() {
        return Ok(summaries);
    }
    let total = files.iter().fold(0_u64, |sum, f| sum.saturating_add(f.size));
    let sampled = sample.iter().fold(0_u64, |sum, f| sum.saturating_add(f.size));
    Ok(summaries.extrapolate(total, sampled))
}

/// At most `max` files, spread evenly over the listing.
fn even_sample(files: &[ObjectMeta], max: usize) -> Vec<&ObjectMeta> {
    if files.len() <= max {
        return files.iter().collect();
    }
    (0..max)
        .filter_map(|i| files.get(i * files.len() / max))
        .collect()
}

/// What the files say about one column's values.
#[derive(Debug, Clone, PartialEq)]
enum Bounds {
    /// No non-null value (no rows, or only nulls).
    Empty,
    Known {
        min: ScalarValue,
        max: ScalarValue,
    },
    Unknown,
}

impl Bounds {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::Empty, bounds) | (bounds, Self::Empty) => bounds,
            (
                Self::Known {
                    min: a_min,
                    max: a_max,
                },
                Self::Known {
                    min: b_min,
                    max: b_max,
                },
            ) => match (a_min.partial_cmp(&b_min), a_max.partial_cmp(&b_max)) {
                (Some(low), Some(high)) => Self::Known {
                    min: if low == Ordering::Greater { b_min } else { a_min },
                    max: if high == Ordering::Less { b_max } else { a_max },
                },
                _ => Self::Unknown,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ColumnSummary {
    null_count: Option<u64>,
    bounds: Bounds,
    /// Estimated Arrow size of the column once read.
    byte_size: Option<u64>,
}

impl ColumnSummary {
    fn unknown(byte_size: Option<u64>) -> Self {
        Self {
            null_count: None,
            bounds: Bounds::Unknown,
            byte_size,
        }
    }

    fn merge(self, other: Self) -> Self {
        Self {
            null_count: add(self.null_count, other.null_count),
            bounds: self.bounds.merge(other.bounds),
            byte_size: add(self.byte_size, other.byte_size),
        }
    }

    fn into_statistics(self) -> ColumnStatistics {
        let (min, max) = match self.bounds {
            Bounds::Known { min, max } => (Precision::Inexact(min), Precision::Inexact(max)),
            Bounds::Empty | Bounds::Unknown => (Precision::Absent, Precision::Absent),
        };
        ColumnStatistics {
            null_count: inexact(self.null_count),
            max_value: max,
            min_value: min,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: inexact(self.byte_size),
        }
    }
}

fn add(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    a?.checked_add(b?)
}

fn inexact(value: Option<u64>) -> Precision<usize> {
    value
        .and_then(|v| usize::try_from(v).ok())
        .map_or(Precision::Absent, Precision::Inexact)
}

/// Totals over some files of one table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableSummary {
    rows: Option<u64>,
    columns: Vec<ColumnSummary>,
}

impl TableSummary {
    fn empty(schema: &Schema) -> Self {
        Self {
            rows: Some(0),
            columns: vec![
                ColumnSummary {
                    null_count: Some(0),
                    bounds: Bounds::Empty,
                    byte_size: Some(0),
                };
                schema.fields().len()
            ],
        }
    }

    fn merge(self, other: Self) -> Self {
        Self {
            rows: add(self.rows, other.rows),
            columns: self
                .columns
                .into_iter()
                .zip(other.columns)
                .map(|(a, b)| a.merge(b))
                .collect(),
        }
    }

    /// Scales a sample covering `sampled` bytes of files to `total` bytes.
    /// Only sizes scale; per-column counts and bounds become unknown. A sample
    /// without rows says nothing about the files left out (a writer's empty
    /// files can fill it): rows and sizes are then unknown too.
    fn extrapolate(self, total: u64, sampled: u64) -> Self {
        let empty_sample = self.rows == Some(0) && total > sampled;
        let scale = |value: Option<u64>| {
            let value = u128::from(value.filter(|_| !empty_sample)?);
            u64::try_from(value * u128::from(total) / u128::from(sampled.max(1))).ok()
        };
        Self {
            rows: scale(self.rows),
            columns: self
                .columns
                .into_iter()
                .map(|c| ColumnSummary::unknown(scale(c.byte_size)))
                .collect(),
        }
    }

    pub(crate) fn into_statistics(self) -> Statistics {
        let total_byte_size = self
            .columns
            .iter()
            .try_fold(0_u64, |sum, c| sum.checked_add(c.byte_size?));
        Statistics {
            num_rows: inexact(self.rows),
            total_byte_size: inexact(total_byte_size),
            column_statistics: self
                .columns
                .into_iter()
                .map(ColumnSummary::into_statistics)
                .collect(),
        }
    }
}

/// Summary of one file for the table `schema`; `None` when the footer's row
/// counts are unusable.
fn file_summary(metadata: &ParquetMetaData, schema: &Schema) -> Option<TableSummary> {
    let row_groups = metadata.row_groups();
    let rows = row_groups.iter().try_fold(0_u64, |sum, rg| {
        sum.checked_add(u64::try_from(rg.num_rows()).ok()?)
    })?;
    let file_metadata = metadata.file_metadata();
    let file_schema =
        parquet_to_arrow_schema(file_metadata.schema_descr(), file_metadata.key_value_metadata()).ok();
    let columns = schema
        .fields()
        .iter()
        .map(|field| match &file_schema {
            Some(file_schema) => column_summary(field, rows, metadata, file_schema),
            None => ColumnSummary::unknown(fixed_size(field.data_type(), rows)),
        })
        .collect();
    Some(TableSummary {
        rows: Some(rows),
        columns,
    })
}

/// Arrow size of `rows` values of a fixed-width type (booleans are bits).
pub(crate) fn fixed_size(data_type: &DataType, rows: u64) -> Option<u64> {
    if *data_type == DataType::Boolean {
        return Some(rows.div_ceil(8));
    }
    u64::try_from(data_type.primitive_width()?)
        .ok()?
        .checked_mul(rows)
}

/// The file column holding the table column `name`: the exact name, or
/// else the one column whose name differs only by case (files keep the
/// case a name was written with, ISS-018; the engine folds it). `Err` when
/// several columns differ from `name` only by case.
fn file_field<'a>(file_schema: &'a Schema, name: &str) -> Option<Result<&'a Field, ()>> {
    let folded = name.to_lowercase();
    let mut matches = file_schema
        .fields()
        .iter()
        .filter(|f| f.name().to_lowercase() == folded);
    match (matches.next(), matches.next()) {
        (None, _) => None,
        (Some(only), None) => Some(Ok(only.as_ref())),
        (Some(_), Some(_)) => Some(Err(())),
    }
}

fn column_summary(
    field: &Field,
    rows: u64,
    metadata: &ParquetMetaData,
    file_schema: &Schema,
) -> ColumnSummary {
    let data_type = field.data_type();
    let file_field = match file_field(file_schema, field.name()) {
        Some(Ok(file_field)) => file_field,
        // Added after this file was written: the reader fills it with NULL.
        None => {
            return ColumnSummary {
                null_count: Some(rows),
                bounds: Bounds::Empty,
                byte_size: Some(fixed_size(data_type, rows).unwrap_or(rows.saturating_mul(OFFSET_BYTES))),
            };
        }
        // Several spellings of the name: the reader decides which (or
        // refuses); the statistics claim nothing.
        Some(Err(())) => return ColumnSummary::unknown(fixed_size(data_type, rows)),
    };
    let descriptor = metadata.file_metadata().schema_descr();
    let Ok(converter) = StatisticsConverter::try_new(file_field.name(), file_schema, descriptor) else {
        return ColumnSummary::unknown(fixed_size(data_type, rows));
    };
    let converter = converter.with_missing_null_counts_as_zero(false);
    let Some(index) = converter.parquet_column_index() else {
        return ColumnSummary::unknown(fixed_size(data_type, rows));
    };
    let row_groups = metadata.row_groups();
    let byte_size = fixed_size(data_type, rows).or_else(|| {
        row_groups.iter().try_fold(0_u64, |sum, rg| {
            sum.checked_add(u64::try_from(rg.column(index).uncompressed_size()).ok()?)
        })
    });
    if !same_nulls(file_field.data_type(), data_type) {
        return ColumnSummary::unknown(byte_size);
    }
    let null_counts = converter.row_group_null_counts(row_groups).ok();
    let null_count = null_counts
        .as_ref()
        .filter(|counts| counts.null_count() == 0)
        .map(|counts| counts.values().iter().sum());
    let order = metadata.file_metadata().column_order(index);
    let bounds = match &null_counts {
        Some(null_counts)
            if file_field.data_type() == data_type
                && trusted_order(data_type, descriptor.column(index).as_ref(), order) =>
        {
            row_group_bounds(&converter, row_groups, null_counts)
        }
        _ => Bounds::Unknown,
    };
    ColumnSummary {
        null_count,
        bounds,
        byte_size,
    }
}

/// Whether reading `file` values as `table` keeps exactly the same nulls:
/// identical types, or timestamps whose unit the reader coerces.
fn same_nulls(file: &DataType, table: &DataType) -> bool {
    file == table
        || matches!(
            (file, table),
            (DataType::Timestamp(_, None), DataType::Timestamp(_, None))
        )
}

/// Whether the Parquet bounds of this column order values as the engine does.
fn trusted_order(data_type: &DataType, column: &ColumnDescriptor, order: ColumnOrder) -> bool {
    let expected = match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Decimal128(..) => SortOrder::SIGNED,
        DataType::Utf8 => SortOrder::UNSIGNED,
        _ => return false,
    };
    let physical = column.physical_type();
    if ColumnOrder::sort_order_for_type(column.logical_type_ref(), column.converted_type(), physical)
        != expected
    {
        return false;
    }
    match order {
        ColumnOrder::TYPE_DEFINED_ORDER(declared) => declared == expected,
        // Writers that predate column orders compared byte arrays as signed
        // bytes; plain integers were always compared as signed values.
        ColumnOrder::UNDEFINED | ColumnOrder::UNKNOWN => {
            matches!(physical, PhysicalType::INT32 | PhysicalType::INT64)
        }
    }
}

/// Bounds over the row groups. A row group without bounds is fine only when
/// all its values are null; otherwise the column's bounds are unknown.
fn row_group_bounds(
    converter: &StatisticsConverter<'_>,
    row_groups: &[RowGroupMetaData],
    null_counts: &UInt64Array,
) -> Bounds {
    let (Ok(mins), Ok(maxes)) = (
        converter.row_group_mins(row_groups),
        converter.row_group_maxes(row_groups),
    ) else {
        return Bounds::Unknown;
    };
    row_groups
        .iter()
        .enumerate()
        .fold(Bounds::Empty, |bounds, (i, rg)| {
            bounds.merge(row_group_bound(&mins, &maxes, null_counts, i, rg.num_rows()))
        })
}

fn row_group_bound(
    mins: &ArrayRef,
    maxes: &ArrayRef,
    null_counts: &UInt64Array,
    i: usize,
    rows: i64,
) -> Bounds {
    if mins.is_null(i) || maxes.is_null(i) {
        let all_null = null_counts.is_valid(i) && i64::try_from(null_counts.value(i)).ok() == Some(rows);
        return if all_null { Bounds::Empty } else { Bounds::Unknown };
    }
    match (
        ScalarValue::try_from_array(mins, i),
        ScalarValue::try_from_array(maxes, i),
    ) {
        (Ok(min), Ok(max))
            if !min.is_null() && !max.is_null() && min.partial_cmp(&max).is_some_and(Ordering::is_le) =>
        {
            Bounds::Known { min, max }
        }
        _ => Bounds::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
    };
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Decimal128(12, 2), true),
            Field::new("day", DataType::Date32, true),
            Field::new("ratio", DataType::Float64, true),
        ]))
    }

    fn batch(ids: &[Option<i64>], names: &[Option<&str>]) -> RecordBatch {
        let n = ids.len();
        let ids_i64: Vec<i64> = ids.iter().map(|v| v.unwrap_or(0)).collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(
                    Decimal128Array::from(ids_i64.iter().map(|v| i128::from(*v) * 150).collect::<Vec<_>>())
                        .with_precision_and_scale(12, 2)
                        .unwrap(),
                ),
                Arc::new(Date32Array::from(vec![Some(20_000); n])),
                Arc::new(Float64Array::from(vec![Some(f64::NAN); n])),
            ],
        )
        .unwrap()
    }

    fn parquet(batches: &[RecordBatch], row_group_rows: usize) -> Vec<u8> {
        let mut buffer = Vec::new();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(row_group_rows))
            .build();
        let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(properties)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        buffer
    }

    struct Files {
        _dir: tempfile::TempDir,
        store: Arc<dyn ObjectStore>,
        metas: Vec<ObjectMeta>,
    }

    async fn files(contents: Vec<Vec<u8>>) -> Files {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let mut metas = Vec::new();
        for (i, bytes) in contents.into_iter().enumerate() {
            let path = Path::from(format!("t/{i:03}"));
            store.put(&path, PutPayload::from(bytes)).await.unwrap();
            metas.push(store.head(&path).await.unwrap());
        }
        Files {
            _dir: dir,
            store,
            metas,
        }
    }

    fn runtime(pool_bytes: usize) -> Arc<RuntimeEnv> {
        RuntimeEnvBuilder::new()
            .with_memory_limit(pool_bytes, 1.0)
            .with_metadata_cache_limit(1024 * 1024)
            .build_arc()
            .unwrap()
    }

    async fn statistics(files: &Files, runtime: &Arc<RuntimeEnv>) -> Statistics {
        footer_statistics(
            &files.store,
            &files.metas,
            &schema(),
            runtime,
            Some(64 * 1024),
            "t",
        )
        .await
    }

    /// ISS-018: a file stores `ID` for the table's `id` (the case it was
    /// written with); its statistics are the column's, not "absent, so all
    /// NULL". Two spellings of one name in a file: nothing is claimed.
    #[tokio::test]
    async fn statistics_match_file_columns_without_regard_to_case() {
        let spelled = |names: &[&str]| {
            let fields: Vec<Field> = names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64, true))
                .collect();
            let columns: Vec<ArrayRef> = names
                .iter()
                .map(|_| Arc::new(Int64Array::from(vec![Some(4), Some(8)])) as ArrayRef)
                .collect();
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        let table = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let one = files(vec![parquet(&[spelled(&["ID"])], 10)]).await;
        let one = footer_statistics(
            &one.store,
            &one.metas,
            &table,
            &runtime(64 << 20),
            Some(64 * 1024),
            "t",
        )
        .await;
        let id = &one.column_statistics[0];
        assert_eq!(id.null_count, Precision::Inexact(0), "{one:?}");
        assert_eq!(id.min_value, Precision::Inexact(ScalarValue::Int64(Some(4))));
        assert_eq!(id.max_value, Precision::Inexact(ScalarValue::Int64(Some(8))));

        let two = files(vec![parquet(&[spelled(&["ID", "Id"])], 10)]).await;
        let two = footer_statistics(
            &two.store,
            &two.metas,
            &table,
            &runtime(64 << 20),
            Some(64 * 1024),
            "t",
        )
        .await;
        let id = &two.column_statistics[0];
        assert_ne!(id.null_count, Precision::Inexact(2), "never 'all NULL'");
        assert_eq!(id.min_value, Precision::Absent);
    }

    #[tokio::test]
    async fn sums_rows_and_merges_bounds_across_files_and_row_groups() {
        let first = batch(&[Some(5), None, Some(9)], &[Some("b"), Some("a"), None]);
        let second = batch(&[Some(-3), Some(7)], &[Some("zz"), Some("m")]);
        let files = files(vec![parquet(&[first], 2), parquet(&[second], 10)]).await;
        let stats = statistics(&files, &runtime(64 * 1024 * 1024)).await;

        assert_eq!(stats.num_rows, Precision::Inexact(5));
        let id = &stats.column_statistics[0];
        assert_eq!(id.min_value, Precision::Inexact(ScalarValue::Int64(Some(-3))));
        assert_eq!(id.max_value, Precision::Inexact(ScalarValue::Int64(Some(9))));
        assert_eq!(id.null_count, Precision::Inexact(1));
        assert_eq!(id.byte_size, Precision::Inexact(40));
        assert_eq!(id.distinct_count, Precision::Absent);
        let name = &stats.column_statistics[1];
        assert_eq!(name.min_value, Precision::Inexact(ScalarValue::from("a")));
        assert_eq!(name.max_value, Precision::Inexact(ScalarValue::from("zz")));
        assert_eq!(name.null_count, Precision::Inexact(1));
        let price = &stats.column_statistics[2];
        assert_eq!(
            price.min_value,
            Precision::Inexact(ScalarValue::Decimal128(Some(-450), 12, 2))
        );
        assert_eq!(
            price.max_value,
            Precision::Inexact(ScalarValue::Decimal128(Some(1350), 12, 2))
        );
        let day = &stats.column_statistics[3];
        assert_eq!(
            day.min_value,
            Precision::Inexact(ScalarValue::Date32(Some(20_000)))
        );
        // Floating point: NaN is outside Parquet bounds but not outside the engine's.
        let ratio = &stats.column_statistics[4];
        assert_eq!(ratio.min_value, Precision::Absent);
        assert_eq!(ratio.max_value, Precision::Absent);
        assert!(matches!(stats.total_byte_size, Precision::Inexact(n) if n > 0));
        // Nothing is ever exact: statistics must never answer a query.
        assert!(!stats.num_rows.is_exact().unwrap_or(false));
    }

    #[tokio::test]
    async fn a_column_missing_from_a_file_counts_as_nulls() {
        let old_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let old = RecordBatch::try_new(
            Arc::clone(&old_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap();
        let new = batch(&[Some(10)], &[Some("x")]);
        let files = files(vec![parquet(&[old], 10), parquet(&[new], 10)]).await;
        let stats = statistics(&files, &runtime(64 * 1024 * 1024)).await;
        assert_eq!(stats.num_rows, Precision::Inexact(3));
        let name = &stats.column_statistics[1];
        assert_eq!(name.null_count, Precision::Inexact(2));
        assert_eq!(name.min_value, Precision::Inexact(ScalarValue::from("x")));
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Inexact(ScalarValue::Int64(Some(10)))
        );
    }

    #[tokio::test]
    async fn a_file_typed_differently_reports_no_bounds_or_nulls() {
        let narrow = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let file =
            RecordBatch::try_new(narrow, vec![Arc::new(Int32Array::from(vec![Some(1), None]))]).unwrap();
        let files = files(vec![parquet(&[file], 10)]).await;
        let stats = statistics(&files, &runtime(64 * 1024 * 1024)).await;
        assert_eq!(stats.num_rows, Precision::Inexact(2));
        let id = &stats.column_statistics[0];
        assert_eq!(id.min_value, Precision::Absent);
        assert_eq!(id.null_count, Precision::Absent);
        assert_eq!(id.byte_size, Precision::Inexact(16), "size of the table type");
    }

    #[tokio::test]
    async fn a_row_group_without_bounds_makes_the_column_unknown() {
        let first = batch(&[Some(1)], &[Some("a")]);
        let without = {
            let mut buffer = Vec::new();
            let properties = WriterProperties::builder()
                .set_statistics_enabled(datafusion::parquet::file::properties::EnabledStatistics::None)
                .build();
            let mut writer = ArrowWriter::try_new(&mut buffer, schema(), Some(properties)).unwrap();
            writer.write(&batch(&[Some(100)], &[Some("z")])).unwrap();
            writer.close().unwrap();
            buffer
        };
        let files = files(vec![parquet(&[first], 10), without]).await;
        let stats = statistics(&files, &runtime(64 * 1024 * 1024)).await;
        assert_eq!(stats.num_rows, Precision::Inexact(2));
        assert_eq!(stats.column_statistics[0].max_value, Precision::Absent);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Absent);
    }

    #[tokio::test]
    async fn a_refused_reservation_or_a_broken_footer_gives_unknown() {
        let good = parquet(&[batch(&[Some(1)], &[Some("a")])], 10);
        let files_ok = files(vec![good.clone()]).await;
        let starved = statistics(&files_ok, &runtime(1024)).await;
        assert_eq!(starved, Statistics::new_unknown(&schema()));

        let files_bad = files(vec![good, b"not parquet at all".to_vec()]).await;
        let broken = statistics(&files_bad, &runtime(64 * 1024 * 1024)).await;
        assert_eq!(broken, Statistics::new_unknown(&schema()));
    }

    #[tokio::test]
    async fn footers_go_through_the_metadata_cache_and_free_the_pool() {
        let files = files(vec![parquet(&[batch(&[Some(1)], &[Some("a")])], 10)]).await;
        let runtime = runtime(64 * 1024 * 1024);
        let first = statistics(&files, &runtime).await;
        let cache = runtime.cache_manager.get_file_metadata_cache();
        assert!(cache.get(&files.metas[0].location).is_some(), "footer cached");
        assert_eq!(runtime.memory_pool.reserved(), 0, "reservation released");
        assert_eq!(statistics(&files, &runtime).await, first);
    }

    #[tokio::test]
    async fn many_files_are_sampled_and_extrapolated_by_size() {
        let one = parquet(&[batch(&[Some(1), Some(2)], &[Some("a"), Some("b")])], 10);
        let files = files(vec![one; 5]).await;
        let runtime = runtime(64 * 1024 * 1024);
        let limits = FooterLimits {
            max_footers: 2,
            size_hint: None,
        };
        let reservation = MemoryConsumer::new("t").register(&runtime.memory_pool);
        let summary = summarize(
            &files.store,
            &files.metas,
            &schema(),
            &runtime,
            limits,
            &reservation,
        )
        .await
        .unwrap();
        let stats = summary.into_statistics();
        assert_eq!(stats.num_rows, Precision::Inexact(10));
        assert_eq!(stats.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(stats.column_statistics[0].byte_size, Precision::Inexact(80));
    }

    #[tokio::test]
    async fn a_sample_of_empty_files_gives_unknown_rows_and_sizes() {
        let empty = parquet(&[batch(&[], &[])], 10);
        let one = parquet(&[batch(&[Some(1), Some(2)], &[Some("a"), Some("b")])], 10);
        // The sample (files 0 and 2 of 4) holds only the empty files.
        let files = files(vec![empty.clone(), one.clone(), empty, one]).await;
        let runtime = runtime(64 * 1024 * 1024);
        let limits = FooterLimits {
            max_footers: 2,
            size_hint: None,
        };
        let reservation = MemoryConsumer::new("t").register(&runtime.memory_pool);
        let summary = summarize(
            &files.store,
            &files.metas,
            &schema(),
            &runtime,
            limits,
            &reservation,
        )
        .await
        .unwrap();
        let stats = summary.into_statistics();
        assert_eq!(stats.num_rows, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Absent);
        assert_eq!(stats.column_statistics[0].byte_size, Precision::Absent);
    }

    #[test]
    fn bounds_merge() {
        let known = |a: i64, b: i64| Bounds::Known {
            min: ScalarValue::Int64(Some(a)),
            max: ScalarValue::Int64(Some(b)),
        };
        assert_eq!(known(1, 5).merge(known(-2, 3)), known(-2, 5));
        assert_eq!(Bounds::Empty.merge(known(1, 2)), known(1, 2));
        assert_eq!(known(1, 2).merge(Bounds::Empty), known(1, 2));
        assert_eq!(known(1, 2).merge(Bounds::Unknown), Bounds::Unknown);
        assert_eq!(Bounds::Empty.merge(Bounds::Empty), Bounds::Empty);
        let mixed = Bounds::Known {
            min: ScalarValue::from("a"),
            max: ScalarValue::from("b"),
        };
        assert_eq!(known(1, 2).merge(mixed), Bounds::Unknown, "incomparable types");
    }

    #[test]
    fn sampling_is_even_and_bounded() {
        let meta = |i: usize| ObjectMeta {
            location: Path::from(format!("f{i}")),
            last_modified: std::time::SystemTime::UNIX_EPOCH.into(),
            size: 1,
            e_tag: None,
            version: None,
        };
        let metas: Vec<ObjectMeta> = (0..10).map(meta).collect();
        let picked: Vec<String> = even_sample(&metas, 4)
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(picked, vec!["f0", "f2", "f5", "f7"]);
        assert_eq!(even_sample(&metas, 20).len(), 10);
    }
}
