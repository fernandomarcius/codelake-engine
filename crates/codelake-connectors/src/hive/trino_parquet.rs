//! Parquet files laid out the way Trino 431's Hive connector writes them, so
//! that Trino and SQL Server 2022 (`OPENROWSET`) read every value exactly
//! (compare with `tests/fixtures/trino431/warehouse/pilot/t1`):
//!
//! - Hive `timestamp` as `INT96` (Julian day + nanoseconds of the day)
//!   holding the instant of the wall-clock time in the catalog's Parquet time
//!   zone (`codelake_core::parquet_time_zone`), with the zone's name under
//!   `writer.time.zone` in the footer, as the reference engine writes them.
//!   That engine converts `INT96` with its `hive.parquet.time-zone` (UTC in
//!   its official image) and ignores the key; SQL Server maps `INT96` to
//!   `datetime2` without conversion, so it shows the UTC time (ADR 0007). An `INT64`
//!   timestamp without the legacy `TIMESTAMP_MILLIS` annotation reaches SQL
//!   Server as `bigint`.
//! - decimals as `FIXED_LEN_BYTE_ARRAY` of the smallest width for the precision;
//! - root group `trino_schema`, no embedded Arrow schema.
//!
//! arrow-rs does not write `INT96`, so timestamp columns are buffered for the
//! open row group and written through the low-level column writer; every other
//! column goes through arrow-rs' column writers. No `INT96` statistics are
//! written: their sort order is undefined and must never prune row groups.

use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType, TimestampNanosecondType,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::parquet::arrow::ArrowSchemaConverter;
use datafusion::parquet::arrow::arrow_writer::{
    ArrowColumnWriter, ArrowRowGroupWriterFactory, compute_leaves,
};
use datafusion::parquet::arrow::async_writer::AsyncFileWriter;
use datafusion::parquet::basic::{Compression, LogicalType, Type as PhysicalType};
use datafusion::parquet::data_type::{Int96, Int96Type};
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
use datafusion::parquet::file::writer::SerializedFileWriter;
use datafusion::parquet::schema::types::{ColumnPath, SchemaDescriptor, Type, TypePtr};

use codelake_core::parquet_time_zone::ParquetTimeZone;

/// Root group name Trino gives its Parquet schema.
const ROOT: &str = "trino_schema";
/// Footer key Trino (and Hive 3) record the `INT96` time zone under, and
/// the zone recorded when none is given ([`TrinoParquetWriter::in_time_zone`]).
const WRITER_TIME_ZONE: (&str, &str) = ("writer.time.zone", "Etc/UTC");
/// Rows per Parquet row group. Bounded so the writer's buffer stays small.
pub(crate) const ROW_GROUP_ROWS: usize = 128 * 1024;
/// Close the open row group once its buffers hold this many bytes.
const FLUSH_BYTES: usize = 64 * 1024 * 1024;
const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const NANOS_PER_DAY: i128 = 86_400_000_000_000;
const INT96_BYTES: usize = 12;

fn is_int96(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, None))
}

/// Smallest byte width of a two's complement integer holding `precision`
/// decimal digits (the width Trino and the Parquet spec use).
fn decimal_width(precision: u8) -> i32 {
    let limit = 10_u128.pow(u32::from(precision));
    (1..=16)
        .find(|bytes| limit <= 1_u128 << (8 * bytes - 1))
        .unwrap_or(16)
}

fn parquet_type(field: &DataType, name: &str, converted: &TypePtr) -> Result<TypePtr> {
    let repetition = converted.get_basic_info().repetition();
    let built = match field {
        DataType::Timestamp(_, None) => Type::primitive_type_builder(name, PhysicalType::INT96)
            .with_repetition(repetition)
            .build()?,
        DataType::Decimal128(precision, scale) => {
            let scale = u8::try_from(*scale).map_err(|_| {
                DataFusionError::NotImplemented(format!("negative decimal scale in `{name}`"))
            })?;
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(repetition)
                .with_length(decimal_width(*precision))
                .with_logical_type(Some(LogicalType::Decimal {
                    scale: i32::from(scale),
                    precision: i32::from(*precision),
                }))
                .with_precision(i32::from(*precision))
                .with_scale(i32::from(scale))
                .build()?
        }
        _ => return Ok(Arc::clone(converted)),
    };
    Ok(Arc::new(built))
}

/// The Parquet schema Trino 431 writes for `schema`.
pub(crate) fn trino_parquet_schema(schema: &Schema) -> Result<SchemaDescriptor> {
    let converted = ArrowSchemaConverter::new().schema_root(ROOT).convert(schema)?;
    let fields = schema
        .fields()
        .iter()
        .zip(converted.root_schema().get_fields())
        .map(|(field, parquet)| parquet_type(field.data_type(), field.name(), parquet))
        .collect::<Result<Vec<_>>>()?;
    if fields.len() != converted.num_columns() {
        return Err(DataFusionError::NotImplemented(
            "nested columns are not supported in hive tables".into(),
        ));
    }
    let root = Type::group_type_builder(ROOT).with_fields(fields).build()?;
    Ok(SchemaDescriptor::new(Arc::new(root)))
}

/// Wall-clock nanoseconds since the epoch as `INT96` (Julian day and
/// nanoseconds of the day), exactly: `i128` holds every instant `INT96`
/// stores, `0001`–`9999` at nanosecond precision included.
fn int96_from_nanos(nanos: i128) -> Result<Int96> {
    let day = i64::try_from(nanos.div_euclid(NANOS_PER_DAY))
        .ok()
        .map(|day| day + JULIAN_DAY_OF_EPOCH)
        // Readers take the day as a signed 32-bit number.
        .and_then(|day| i32::try_from(day).ok())
        .and_then(|day| u32::try_from(day).ok())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "timestamp {nanos} ns since 1970 is outside the range Parquet INT96 stores"
            ))
        })?;
    let nanos = u64::try_from(nanos.rem_euclid(NANOS_PER_DAY)).unwrap_or_default();
    // Less than 2^47: both halves fit.
    let low = u32::try_from(nanos & 0xFFFF_FFFF).unwrap_or_default();
    let high = u32::try_from(nanos >> 32).unwrap_or_default();
    let mut value = Int96::new();
    value.set_data(low, high, day);
    Ok(value)
}

/// Wall-clock microseconds since the epoch as `INT96`.
#[cfg(test)]
fn int96_from_micros(micros: i64) -> Result<Int96> {
    int96_from_nanos(i128::from(micros) * 1_000)
}

/// Nanoseconds since the epoch of each value of a timestamp column, exactly:
/// nanosecond arrays as they are, coarser units scaled, and `Decimal128`
/// arrays of scale 0 as exact nanoseconds (compaction's values outside the
/// range of Arrow nanoseconds, see `exact_timestamps.rs`).
fn exact_nanos(array: &ArrayRef) -> Result<Vec<Option<i128>>> {
    Ok(match array.data_type() {
        DataType::Decimal128(_, 0) => array.as_primitive::<Decimal128Type>().iter().collect(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .map(|v| v.map(i128::from))
            .collect(),
        _ => {
            let micros_type = DataType::Timestamp(TimeUnit::Microsecond, None);
            let micros = if array.data_type() == &micros_type {
                Arc::clone(array)
            } else {
                cast(array, &micros_type)?
            };
            micros
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .map(|v| v.map(|v| i128::from(v) * 1_000))
                .collect()
        }
    })
}

/// One column of the open row group.
enum Column {
    Arrow(Box<ArrowColumnWriter>),
    /// `INT96` values of the open row group and their definition levels
    /// (`None` for a required column).
    Int96 {
        values: Vec<Int96>,
        definition: Option<Vec<i16>>,
    },
}

impl Column {
    /// Estimated encoded size of what the open row group holds.
    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Arrow(writer) => writer.get_estimated_total_bytes(),
            Self::Int96 { values, .. } => values.len() * INT96_BYTES,
        }
    }

    fn memory_size(&self) -> usize {
        match self {
            Self::Arrow(writer) => writer.memory_size(),
            Self::Int96 { values, definition } => {
                values.capacity() * INT96_BYTES + definition.as_ref().map_or(0, |d| d.capacity() * 2)
            }
        }
    }

    /// Appends `array`; `zone` converts wall-clock timestamps to the
    /// instants `INT96` stores (`None`: they are instants already).
    fn write(
        &mut self,
        field: &datafusion::arrow::datatypes::Field,
        array: &ArrayRef,
        zone: Option<ParquetTimeZone>,
    ) -> Result<()> {
        match self {
            Self::Arrow(writer) => {
                for leaf in compute_leaves(field, array)? {
                    writer.write(&leaf)?;
                }
            }
            Self::Int96 { values, definition } => {
                for value in exact_nanos(array)? {
                    match definition {
                        Some(levels) => levels.push(i16::from(value.is_some())),
                        None if value.is_none() => {
                            return Err(DataFusionError::Execution(format!(
                                "NULL in required column `{}`",
                                field.name()
                            )));
                        }
                        None => {}
                    }
                    if let Some(nanos) = value {
                        let instant = match zone {
                            None => nanos,
                            Some(zone) => zone.local_to_utc(nanos).ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "timestamp {nanos} ns since 1970 in column `{}` is outside the range \
                                     of time zone {zone}",
                                    field.name()
                                ))
                            })?,
                        };
                        values.push(int96_from_nanos(instant)?);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Streams record batches into one Trino-compatible Parquet file.
pub(crate) struct TrinoParquetWriter<W: AsyncFileWriter> {
    file: SerializedFileWriter<Vec<u8>>,
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    optional: Vec<bool>,
    output: W,
    open: Option<Vec<Column>>,
    open_rows: usize,
    row_group_rows: usize,
    /// Bytes handed to `output` so far.
    written: usize,
    /// Zone of the wall-clock timestamps written, unless UTC or already
    /// instants.
    zone: Option<ParquetTimeZone>,
    /// Zone recorded under `writer.time.zone`.
    zone_name: &'static str,
}

impl<W: AsyncFileWriter> TrinoParquetWriter<W> {
    pub(crate) fn try_new(output: W, schema: SchemaRef, row_group_rows: usize) -> Result<Self> {
        let descriptor = trino_parquet_schema(&schema)?;
        let mut properties = WriterProperties::builder().set_compression(Compression::SNAPPY);
        for field in schema.fields().iter().filter(|f| is_int96(f.data_type())) {
            properties = properties.set_column_statistics_enabled(
                ColumnPath::new(vec![field.name().clone()]),
                EnabledStatistics::None,
            );
        }
        let optional = descriptor
            .columns()
            .iter()
            .map(|c| c.max_def_level() > 0)
            .collect();
        let file = SerializedFileWriter::new(
            Vec::new(),
            descriptor.root_schema_ptr(),
            Arc::new(properties.build()),
        )?;
        let factory = ArrowRowGroupWriterFactory::new(&file, Arc::clone(&schema));
        Ok(Self {
            file,
            factory,
            schema,
            optional,
            output,
            open: None,
            open_rows: 0,
            row_group_rows: row_group_rows.max(1),
            written: 0,
            zone: None,
            zone_name: WRITER_TIME_ZONE.1,
        })
    }

    /// Timestamps written are wall-clock times of `zone` (the catalog's
    /// Parquet time zone): `INT96` stores them as UTC instants, as the
    /// reference engine's `hive.parquet.time-zone` does. Without it,
    /// timestamps are written as they are (compaction rewrites stored
    /// instants).
    #[must_use]
    pub(crate) fn in_time_zone(mut self, zone: ParquetTimeZone) -> Self {
        self.zone = Some(zone).filter(|zone| !zone.is_utc());
        self.zone_name = zone.name();
        self
    }

    /// Records `zone` under `writer.time.zone` without converting anything:
    /// the timestamps written are instants already, stored in that zone
    /// (compaction).
    #[must_use]
    pub(crate) fn recording_time_zone(mut self, zone: ParquetTimeZone) -> Self {
        self.zone_name = zone.name();
        self
    }

    /// Bytes of closed row groups already handed to the output.
    pub(crate) fn bytes_written(&self) -> usize {
        self.written
    }

    /// Estimated encoded size of the open row group (not its buffers, which
    /// can be much larger).
    pub(crate) fn in_progress_size(&self) -> usize {
        self.open
            .as_ref()
            .map_or(0, |columns| columns.iter().map(Column::estimated_bytes).sum())
    }

    /// Bytes buffered for the open row group.
    pub(crate) fn memory_size(&self) -> usize {
        self.open
            .as_ref()
            .map_or(0, |columns| columns.iter().map(Column::memory_size).sum())
    }

    fn open_row_group(&self) -> Result<Vec<Column>> {
        let writers = self
            .factory
            .create_column_writers(self.file.flushed_row_groups().len())?;
        Ok(writers
            .into_iter()
            .zip(self.schema.fields())
            .zip(&self.optional)
            .map(|((writer, field), optional)| {
                if is_int96(field.data_type()) {
                    Column::Int96 {
                        values: Vec::new(),
                        definition: optional.then(Vec::new),
                    }
                } else {
                    Column::Arrow(Box::new(writer))
                }
            })
            .collect())
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let rows = (self.row_group_rows - self.open_rows).min(batch.num_rows() - offset);
            let slice = batch.slice(offset, rows);
            if self.open.is_none() {
                self.open = Some(self.open_row_group()?);
            }
            if let Some(columns) = self.open.as_mut() {
                for ((column, field), array) in
                    columns.iter_mut().zip(self.schema.fields()).zip(slice.columns())
                {
                    column.write(field, array, self.zone)?;
                }
            }
            self.open_rows += rows;
            offset += rows;
            if self.open_rows >= self.row_group_rows || self.memory_size() >= FLUSH_BYTES {
                self.flush().await?;
            }
        }
        Ok(())
    }

    /// Closes the open row group and hands its bytes to the output.
    pub(crate) async fn flush(&mut self) -> Result<()> {
        let Some(columns) = self.open.take() else {
            return Ok(());
        };
        let mut row_group = self.file.next_row_group()?;
        for column in columns {
            match column {
                Column::Arrow(writer) => writer.close()?.append_to_row_group(&mut row_group)?,
                Column::Int96 { values, definition } => {
                    let mut writer = row_group.next_column()?.ok_or_else(|| {
                        DataFusionError::Internal("parquet schema has fewer columns".into())
                    })?;
                    writer
                        .typed::<Int96Type>()
                        .write_batch(&values, definition.as_deref(), None)?;
                    writer.close()?;
                }
            }
        }
        row_group.close()?;
        self.open_rows = 0;
        self.drain().await
    }

    async fn drain(&mut self) -> Result<()> {
        self.file.flush()?;
        let bytes = std::mem::take(self.file.inner_mut());
        if !bytes.is_empty() {
            self.written += bytes.len();
            self.output.write(Bytes::from(bytes)).await?;
        }
        Ok(())
    }

    /// Writes the footer and completes the output; returns the number of
    /// rows the footer records.
    pub(crate) async fn close(mut self) -> Result<i64> {
        self.flush().await?;
        self.file.append_key_value_metadata(KeyValue::new(
            WRITER_TIME_ZONE.0.into(),
            self.zone_name.to_owned(),
        ));
        let metadata = self.file.finish()?;
        self.drain().await?;
        self.output.complete().await?;
        Ok(metadata.file_metadata().num_rows())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BinaryArray, Date32Array, Decimal128Array, Int64Array, StringArray, TimestampMillisecondArray,
        TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::Field;
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::parquet::basic::Type as Physical;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("d18", DataType::Decimal128(18, 2), true),
            Field::new("d38", DataType::Decimal128(38, 10), true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
            Field::new("dt", DataType::Date32, true),
        ]))
    }

    const MILLIS: [Option<i64>; 4] = [
        Some(1_767_261_600_123), // 2026-01-01 10:00:00.123
        None,
        Some(-1),                 // 1969-12-31 23:59:59.999
        Some(-2_203_891_200_000), // 1900-03-01 00:00:00
    ];

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                Arc::new(TimestampMillisecondArray::from(MILLIS.to_vec())),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(1_234_567_890),
                        None,
                        Some(-999_999_999_999_999_999),
                        Some(0),
                    ])
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(12_345_678_901_234_567_890_123_456_789_012_345_678_i128),
                        None,
                        Some(-1),
                        Some(0),
                    ])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec![Some("ação"), None, Some(""), Some("x")])),
                Arc::new(BinaryArray::from(vec![
                    Some(&b"\x00\x2a"[..]),
                    None,
                    Some(&b""[..]),
                    Some(&b"\xff"[..]),
                ])),
                Arc::new(Date32Array::from(vec![
                    Some(20_455),
                    None,
                    Some(-25_508),
                    Some(0),
                ])),
            ],
        )
        .unwrap()
    }

    async fn written(row_group_rows: usize) -> Bytes {
        let mut buffer = Vec::new();
        let mut writer = TrinoParquetWriter::try_new(&mut buffer, schema(), row_group_rows).unwrap();
        let batch = batch();
        writer.write(&batch.slice(0, 1)).await.unwrap();
        writer.write(&batch.slice(1, 3)).await.unwrap();
        writer.close().await.unwrap();
        Bytes::from(buffer)
    }

    #[tokio::test]
    async fn lays_out_the_file_like_trino_431() {
        let file = written(ROW_GROUP_ROWS).await;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata().file_metadata();
        let descriptor = metadata.schema_descr();
        assert_eq!(descriptor.root_schema().name(), "trino_schema");
        let physical: Vec<(Physical, i32)> = descriptor
            .columns()
            .iter()
            .map(|c| (c.physical_type(), c.type_length()))
            .collect();
        assert_eq!(physical[1].0, Physical::INT96);
        assert_eq!(
            physical[2],
            (Physical::FIXED_LEN_BYTE_ARRAY, 8),
            "decimal(18,2) as Trino writes it"
        );
        assert_eq!(physical[3], (Physical::FIXED_LEN_BYTE_ARRAY, 16));
        let keys: Vec<(String, Option<String>)> = metadata
            .key_value_metadata()
            .unwrap()
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        assert_eq!(keys, vec![("writer.time.zone".into(), Some("Etc/UTC".into()))]);
        let ts_stats = reader.metadata().row_group(0).column(1).statistics();
        assert!(
            ts_stats.is_none_or(|s| s.min_bytes_opt().is_none()),
            "no INT96 min/max"
        );
    }

    #[tokio::test]
    async fn values_round_trip_across_row_groups() {
        let file = written(3).await;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
        let batches: Vec<RecordBatch> = reader.build().unwrap().map(Result::unwrap).collect();
        let back = datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let expected = batch();
        // INT96 reads back as nanoseconds.
        let ts = back
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let want: Vec<Option<i64>> = MILLIS.iter().map(|m| m.map(|v| v * 1_000_000)).collect();
        assert_eq!(ts.iter().collect::<Vec<_>>(), want);
        for index in [0, 2, 3, 4, 5, 6] {
            assert_eq!(
                back.column(index).as_ref(),
                expected.column(index).as_ref(),
                "column {index}"
            );
        }
    }

    const MICROS_PER_DAY: i64 = 86_400_000_000;

    #[test]
    fn int96_keeps_nanoseconds_over_the_whole_range() {
        // 1500-06-01 00:00:00.000000001 and 9999-12-31 23:59:59.999999999:
        // outside what Arrow nanoseconds hold, exact here.
        let far = [
            -14_818_723_199_999_999_999_i128,
            253_402_300_799_999_999_999,
            1_767_261_600_123_456_789,
            -1,
        ];
        for nanos in far {
            let value = int96_from_nanos(nanos).unwrap();
            let (low, high, day) = (value.data()[0], value.data()[1], value.data()[2]);
            let of_day = (i128::from(high) << 32) | i128::from(low);
            let back = (i128::from(day) - i128::from(JULIAN_DAY_OF_EPOCH)) * NANOS_PER_DAY + of_day;
            assert_eq!(back, nanos);
        }
        assert!(int96_from_nanos(i128::MIN).is_err());
        assert!(int96_from_nanos(i128::from(i32::MAX) * NANOS_PER_DAY).is_err());
    }

    #[test]
    fn timestamp_columns_reach_int96_exactly() {
        let nanos: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_767_261_600_123_456_789),
            None,
        ]));
        assert_eq!(
            exact_nanos(&nanos).unwrap(),
            vec![Some(1_767_261_600_123_456_789), None]
        );
        let millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![Some(-1)]));
        assert_eq!(exact_nanos(&millis).unwrap(), vec![Some(-1_000_000)]);
        let exact: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(-14_818_723_199_999_999_999), None])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        assert_eq!(
            exact_nanos(&exact).unwrap(),
            vec![Some(-14_818_723_199_999_999_999), None]
        );
    }

    #[test]
    fn int96_encoding() {
        for micros in [
            0,
            1,
            -1,
            1_767_261_600_123_999,
            -2_203_891_200_000_000,
            -46_388_476_799_999_999,
            MICROS_PER_DAY - 1,
        ] {
            assert_eq!(int96_from_micros(micros).unwrap().to_micros(), micros, "{micros}");
        }
        assert_eq!(int96_from_micros(0).unwrap().data(), &[0, 0, 2_440_588]);
        assert!(int96_from_micros(i64::MIN).is_err());
    }

    #[test]
    fn decimal_widths_match_arrow_rs() {
        assert_eq!(decimal_width(1), 1);
        assert_eq!(decimal_width(2), 1);
        assert_eq!(decimal_width(3), 2);
        assert_eq!(decimal_width(9), 4);
        assert_eq!(decimal_width(10), 5);
        assert_eq!(decimal_width(18), 8);
        assert_eq!(decimal_width(38), 16);
        // arrow-rs picks the width itself above 18 digits: both must agree.
        for precision in 19..=38 {
            let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(precision, 0), true)]);
            let converted = ArrowSchemaConverter::new().convert(&schema).unwrap();
            assert_eq!(
                converted.column(0).type_length(),
                decimal_width(precision),
                "{precision}"
            );
        }
    }
}
