//! Timestamps read without losing their range or their digits.
//!
//! Parquet `INT96` holds a Julian day and the nanoseconds of that day: any
//! instant from 0001 to 9999 at nanosecond precision (hive writers use it at
//! every timestamp precision). The Parquet reader decodes it in one Arrow
//! unit for the whole file, with wrapping arithmetic:
//!
//! - in microseconds every such instant fits, but the last three digits of a
//!   nanosecond value are lost;
//! - in nanoseconds an instant outside 1677-09-21 … 2262-04-11 wraps, without
//!   an error, into another instant (1500-06-01 reads as 2084-12-18).
//!
//! So a table whose values must keep their nanoseconds (a NANOSECONDS
//! catalog's scans, and compaction, which rewrites whatever the files hold)
//! is read twice, over the same files in the same order: every column with
//! its timestamps in microseconds (the *coarse* read), and the timestamp
//! columns alone in nanoseconds (the *fine* read, [`Reading::NanosOnly`]:
//! only values a file stores in nanoseconds, NULL otherwise). The exact value
//! is the microseconds, times 1000, plus the difference between the two reads
//! taken modulo 2^64: the wrapped nanoseconds differ from the true ones by a
//! multiple of 2^64, and the true ones from the microseconds by less than
//! 1000 nanoseconds (the coarse read may round or truncate a finer file).
//! A value without a fine read was stored at microseconds or coarser. In a
//! catalog whose Parquet time zone is not UTC, the coarse read of a
//! NANOSECONDS catalog's scan shows `INT96` instants as wall-clock times
//! (`columns.rs`), whole seconds away from the fine read: there only the
//! difference below a second pairs the two reads.
//!
//! [`ExactTimestampsExec`] pairs the two reads row by row (their batches may
//! be cut differently) and answers either nanoseconds (a NANOSECONDS
//! catalog's scan: an instant Arrow nanoseconds cannot hold is an error,
//! never another instant) or exact `Decimal128(38, 0)` nanoseconds
//! (compaction, which writes them back to `INT96` unchanged, see
//! `trino_parquet.rs`).
//!
//! The node is a leaf for the optimizer: the two scans must read the same
//! files in the same partitions, so no rule may repartition one of them, and
//! each scan keeps every file in its partition, in order
//! (`HiveTable::scan_files_as`).

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, RecordBatch, TimestampNanosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::ColumnStatistics;
use datafusion::common::stats::Statistics;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning, PlanProperties,
};
use futures::StreamExt;

use super::table::{HiveTable, Reading};

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Arrow type of exact nanoseconds since the epoch ([`Output::Exact`]).
pub(crate) const EXACT: DataType = DataType::Decimal128(38, 0);

/// Whether the projected columns of `schema` hold nanosecond timestamps.
pub(crate) fn has_nanoseconds(schema: &Schema, projection: Option<&Vec<usize>>) -> bool {
    columns(schema, projection)
        .iter()
        .any(|index| is_nanos(schema.field(*index).data_type()))
}

fn is_nanos(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, _))
}

fn columns(schema: &Schema, projection: Option<&Vec<usize>>) -> Vec<usize> {
    projection
        .cloned()
        .unwrap_or_else(|| (0..schema.fields().len()).collect())
}

/// `schema` with every timestamp column `to` (a unit, or [`EXACT`]).
fn retyped(schema: &Schema, to: impl Fn(&Option<Arc<str>>) -> DataType) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Timestamp(_, zone) => field.as_ref().clone().with_data_type(to(zone)),
            _ => field.as_ref().clone(),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn timestamps_in(unit: TimeUnit) -> impl Fn(&Option<Arc<str>>) -> DataType {
    move |zone| DataType::Timestamp(unit, zone.clone())
}

/// A scan of a NANOSECONDS catalog's table: its values as the table shows
/// them, the timestamps exactly (an instant outside what nanoseconds hold is
/// an error). `statistics` are the table's.
pub(crate) fn nanosecond_scan(
    table: &HiveTable,
    runtime: &Arc<RuntimeEnv>,
    groups: Vec<Vec<PartitionedFile>>,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    statistics: &Statistics,
) -> Result<Arc<dyn ExecutionPlan>> {
    let projected = columns(&table.schema, projection);
    let coarse_schema = retyped(&table.schema, timestamps_in(TimeUnit::Microsecond));
    let coarse = table.scan_files_as(
        runtime,
        coarse_schema,
        Reading::Table,
        groups.clone(),
        projection,
        limit,
    )?;
    let timestamps: Vec<(usize, usize)> = projected
        .iter()
        .enumerate()
        .filter(|(_, index)| is_nanos(table.schema.field(**index).data_type()))
        .map(|(output, index)| (output, *index))
        .collect();
    let fine_columns: Vec<usize> = timestamps.iter().map(|(_, index)| *index).collect();
    let fine = table.scan_files_as(
        runtime,
        Arc::clone(&table.schema),
        Reading::NanosOnly,
        groups,
        Some(&fine_columns),
        limit,
    )?;
    let schema = Arc::new(table.schema.project(&projected)?);
    let mut statistics = statistics.clone().project(Some(&projected));
    for (output, _) in &timestamps {
        if let Some(column) = statistics.column_statistics.get_mut(*output) {
            // Min/max recorded in another unit: unknown here.
            *column = ColumnStatistics::new_unknown();
        }
    }
    let pairs = timestamps
        .iter()
        .enumerate()
        .map(|(fine_index, (output, _))| (*output, fine_index))
        .collect();
    let zoned = !table.metastore.parquet_time_zone().is_utc();
    Ok(Arc::new(
        ExactTimestampsExec::new(coarse, Some(fine), pairs, Output::Nanoseconds, schema, statistics)
            .with_zoned_coarse_read(zoned),
    ))
}

/// Every column of `groups` as stored, the timestamps as exact nanoseconds
/// ([`EXACT`]): what compaction rewrites.
pub(crate) fn exact_scan(
    table: &HiveTable,
    runtime: &Arc<RuntimeEnv>,
    groups: Vec<Vec<PartitionedFile>>,
) -> Result<ExactTimestampsExec> {
    let coarse = table.scan_files_as(
        runtime,
        retyped(&table.schema, timestamps_in(TimeUnit::Microsecond)),
        Reading::Raw,
        groups.clone(),
        None,
        None,
    )?;
    let timestamps: Vec<usize> = table
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| matches!(field.data_type(), DataType::Timestamp(..)))
        .map(|(index, _)| index)
        .collect();
    let fine = if timestamps.is_empty() {
        None
    } else {
        Some(table.scan_files_as(
            runtime,
            retyped(&table.schema, timestamps_in(TimeUnit::Nanosecond)),
            Reading::NanosOnly,
            groups,
            Some(&timestamps),
            None,
        )?)
    };
    let pairs = timestamps
        .iter()
        .enumerate()
        .map(|(fine_index, output)| (*output, fine_index))
        .collect();
    let schema = retyped(&table.schema, |_| EXACT);
    let statistics = Statistics::new_unknown(&schema);
    Ok(ExactTimestampsExec::new(
        coarse,
        fine,
        pairs,
        Output::Exact,
        schema,
        statistics,
    ))
}

/// What [`ExactTimestampsExec`] answers for a timestamp column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Output {
    /// Arrow nanoseconds; an instant they cannot hold is an error.
    Nanoseconds,
    /// [`EXACT`] nanoseconds since the epoch.
    Exact,
}

/// The coarse and the fine reads of the same files, paired row by row (see
/// the module documentation).
#[derive(Debug)]
pub(crate) struct ExactTimestampsExec {
    coarse: Arc<dyn ExecutionPlan>,
    fine: Option<Arc<dyn ExecutionPlan>>,
    /// Each timestamp column: its index in the output (and in `coarse`), and
    /// in `fine`.
    pairs: Vec<(usize, usize)>,
    output: Output,
    /// Whether the coarse read shows `INT96` instants in the catalog's zone.
    zoned: bool,
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    properties: Arc<PlanProperties>,
}

impl ExactTimestampsExec {
    fn new(
        coarse: Arc<dyn ExecutionPlan>,
        fine: Option<Arc<dyn ExecutionPlan>>,
        pairs: Vec<(usize, usize)>,
        output: Output,
        schema: SchemaRef,
        statistics: Statistics,
    ) -> Self {
        let partitions = coarse.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            coarse,
            fine,
            pairs,
            output,
            zoned: false,
            schema,
            statistics: Arc::new(statistics),
            properties,
        }
    }

    /// The coarse read shows `INT96` instants in the catalog's zone, whole
    /// seconds away from the fine read (see the module documentation).
    fn with_zoned_coarse_read(mut self, zoned: bool) -> Self {
        self.zoned = zoned;
        self
    }
}

impl DisplayAs for ExactTimestampsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExactTimestampsExec: {:?}, timestamp columns={:?}",
            self.output,
            self.pairs.iter().map(|(output, _)| output).collect::<Vec<_>>()
        )
    }
}

impl ExecutionPlan for ExactTimestampsExec {
    fn name(&self) -> &'static str {
        "ExactTimestampsExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // A leaf: the two scans must stay partitioned alike.
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ExactTimestampsExec has no children".into(),
            ))
        }
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(match partition {
            None => Arc::clone(&self.statistics),
            Some(_) => Arc::new(Statistics::new_unknown(&self.schema)),
        })
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let coarse = self.coarse.execute(partition, Arc::clone(&context))?;
        let fine = self
            .fine
            .as_ref()
            .map(|fine| fine.execute(partition, context))
            .transpose()?;
        let pairing = Pairing {
            coarse,
            fine,
            coarse_rest: None,
            fine_rest: None,
            pairs: self.pairs.clone(),
            output: self.output,
            zoned: self.zoned,
            schema: Arc::clone(&self.schema),
        };
        let stream = futures::stream::try_unfold(pairing, |mut pairing| async move {
            Ok(pairing.next().await?.map(|batch| (batch, pairing)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

fn disagree() -> DataFusionError {
    DataFusionError::Execution(
        "the two reads of the same hive files returned different rows; nothing was answered".into(),
    )
}

/// The state of one partition's pairing.
struct Pairing {
    coarse: SendableRecordBatchStream,
    fine: Option<SendableRecordBatchStream>,
    /// Rows of the last coarse batch not paired yet.
    coarse_rest: Option<RecordBatch>,
    /// Rows of the last fine batch not paired yet.
    fine_rest: Option<RecordBatch>,
    pairs: Vec<(usize, usize)>,
    output: Output,
    zoned: bool,
    schema: SchemaRef,
}

impl Pairing {
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let coarse = if let Some(batch) = self.coarse_rest.take() {
                batch
            } else if let Some(batch) = self.coarse.next().await {
                batch?
            } else {
                self.finish().await?;
                return Ok(None);
            };
            if coarse.num_rows() == 0 {
                continue;
            }
            let Some(fine) = self.next_fine().await? else {
                return self.combine(&coarse, None).map(Some);
            };
            let rows = coarse.num_rows().min(fine.num_rows());
            if coarse.num_rows() > rows {
                self.coarse_rest = Some(coarse.slice(rows, coarse.num_rows() - rows));
            }
            if fine.num_rows() > rows {
                self.fine_rest = Some(fine.slice(rows, fine.num_rows() - rows));
            }
            return self
                .combine(&coarse.slice(0, rows), Some(&fine.slice(0, rows)))
                .map(Some);
        }
    }

    /// The next fine rows, `None` without a fine read; the fine read ending
    /// before the coarse one is an error.
    async fn next_fine(&mut self) -> Result<Option<RecordBatch>> {
        let Some(stream) = self.fine.as_mut() else {
            return Ok(None);
        };
        loop {
            let batch = match self.fine_rest.take() {
                Some(batch) => batch,
                None => match stream.next().await {
                    Some(batch) => batch?,
                    None => return Err(disagree()),
                },
            };
            if batch.num_rows() > 0 {
                return Ok(Some(batch));
            }
        }
    }

    /// The coarse read ended: the fine one must end too.
    async fn finish(&mut self) -> Result<()> {
        if self.fine_rest.as_ref().is_some_and(|batch| batch.num_rows() > 0) {
            return Err(disagree());
        }
        if let Some(stream) = self.fine.as_mut() {
            while let Some(batch) = stream.next().await {
                if batch?.num_rows() > 0 {
                    return Err(disagree());
                }
            }
        }
        Ok(())
    }

    fn combine(&self, coarse: &RecordBatch, fine: Option<&RecordBatch>) -> Result<RecordBatch> {
        let mut columns = coarse.columns().to_vec();
        for (output, fine_index) in &self.pairs {
            let fine_column = fine.map(|batch| batch.column(*fine_index));
            let exact = exact_values(coarse.column(*output), fine_column, self.zoned)?;
            columns[*output] = match self.output {
                Output::Exact => Arc::new(Decimal128Array::from(exact).with_precision_and_scale(38, 0)?),
                Output::Nanoseconds => {
                    nanoseconds(coarse.column(*output), &exact, self.schema.field(*output))?
                }
            };
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

/// Exact nanoseconds since the epoch of each row, from the coarse read (a
/// timestamp in microseconds) and the fine one (nanoseconds, possibly
/// wrapped; NULL for a value stored at microseconds or coarser). `zoned`: the
/// coarse read may be whole seconds away from the fine one (see the module
/// documentation); the answer keeps the coarse read's seconds.
fn exact_values(coarse: &ArrayRef, fine: Option<&ArrayRef>, zoned: bool) -> Result<Vec<Option<i128>>> {
    let micros = cast(coarse, &DataType::Int64)?;
    let micros = micros.as_primitive::<Int64Type>();
    let nanos = fine.map(|fine| cast(fine, &DataType::Int64)).transpose()?;
    let nanos = nanos.as_deref().map(AsArray::as_primitive::<Int64Type>);
    (0..micros.len())
        .map(|row| {
            if micros.is_null(row) {
                return Ok(None);
            }
            let micros = micros.value(row);
            let scaled = i128::from(micros) * 1_000;
            match nanos.filter(|nanos| nanos.is_valid(row)) {
                None => Ok(Some(scaled)),
                Some(nanos) => {
                    // The true nanoseconds minus `scaled`, modulo 2^64: small
                    // (plus whole seconds when `zoned`).
                    let mut difference = nanos.value(row).wrapping_sub(micros.wrapping_mul(1_000));
                    if zoned {
                        difference = difference.rem_euclid(NANOS_PER_SECOND);
                        if difference > NANOS_PER_SECOND / 2 {
                            difference -= NANOS_PER_SECOND;
                        }
                    }
                    if difference.unsigned_abs() >= 1_000 {
                        return Err(disagree());
                    }
                    Ok(Some(scaled + i128::from(difference)))
                }
            }
        })
        .collect()
}

/// Exact values as Arrow nanoseconds (`field`'s zone); one outside their
/// range is an error naming it.
fn nanoseconds(coarse: &ArrayRef, exact: &[Option<i128>], field: &Field) -> Result<ArrayRef> {
    let values = exact
        .iter()
        .enumerate()
        .map(|(row, value)| {
            value
                .map(|nanos| {
                    i64::try_from(nanos).map_err(|_| {
                        let shown = array_value_to_string(coarse, row).unwrap_or_else(|_| nanos.to_string());
                        DataFusionError::Execution(format!(
                            "timestamp {shown} in column {} is outside the range of timestamp(9) \
                             (1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807); \
                             read it through a catalog at MILLISECONDS or MICROSECONDS precision",
                            field.name()
                        ))
                    })
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let zone = match field.data_type() {
        DataType::Timestamp(_, zone) => zone.clone(),
        _ => None,
    };
    Ok(Arc::new(
        TimestampNanosecondArray::from(values).with_timezone_opt(zone),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::TimestampMicrosecondArray;

    /// 1500-06-01 00:00:00.000000001 and 9999-12-31 23:59:59.999999999.
    const FAR_PAST: i128 = -14_818_723_199_999_999_999;
    const FAR_FUTURE: i128 = 253_402_300_799_999_999_999;

    /// What the Parquet reader answers for `nanos` in each unit.
    fn reads(nanos: &[i128]) -> (ArrayRef, ArrayRef) {
        let micros: Vec<Option<i64>> = nanos
            .iter()
            .map(|n| Some(i64::try_from(n.div_euclid(1_000)).unwrap()))
            .collect();
        // Wrapping, as `Int96::to_nanos` does.
        let wrapped: Vec<Option<i64>> = nanos
            .iter()
            .map(|n| {
                let low = n.rem_euclid(1 << 64);
                Some(i64::try_from(if low >= 1 << 63 { low - (1 << 64) } else { low }).unwrap())
            })
            .collect();
        (
            Arc::new(TimestampMicrosecondArray::from(micros)),
            Arc::new(TimestampNanosecondArray::from(wrapped)),
        )
    }

    #[test]
    fn exact_values_recover_wrapped_nanoseconds() {
        let values = [FAR_PAST, FAR_FUTURE, 1_767_261_600_123_456_789, -1, 0];
        let (coarse, fine) = reads(&values);
        let exact = exact_values(&coarse, Some(&fine), false).unwrap();
        assert_eq!(exact, values.iter().map(|v| Some(*v)).collect::<Vec<_>>());
    }

    #[test]
    fn a_value_without_a_fine_read_has_no_sub_microseconds() {
        let coarse: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(-1), None]));
        let fine: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![None::<i64>, None]));
        assert_eq!(
            exact_values(&coarse, Some(&fine), false).unwrap(),
            vec![Some(-1_000), None]
        );
        assert_eq!(
            exact_values(&coarse, None, false).unwrap(),
            vec![Some(-1_000), None]
        );
    }

    #[test]
    fn a_rounded_coarse_read_still_gives_the_stored_nanoseconds() {
        // 1969-12-31 23:59:59.999999500 rounded half up to 0 µs.
        let coarse: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0]));
        let fine: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![-500]));
        assert_eq!(
            exact_values(&coarse, Some(&fine), false).unwrap(),
            vec![Some(-500)]
        );
    }

    #[test]
    fn a_zoned_coarse_read_keeps_its_seconds_and_the_stored_nanoseconds() {
        // 1500-06-01 00:00:00.000000001 local, stored as the instant
        // 03:06:28.000000001 (Sao Paulo's local mean time), read wrapped.
        let local = FAR_PAST;
        let stored = FAR_PAST + 11_188 * 1_000_000_000;
        let (_, fine) = reads(&[stored]);
        let (coarse, _) = reads(&[local]);
        assert_eq!(
            exact_values(&coarse, Some(&fine), true).unwrap(),
            vec![Some(local)]
        );
        assert!(exact_values(&coarse, Some(&fine), false).is_err());
        let off: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000 + 5_000]));
        let zero: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0]));
        assert!(exact_values(&zero, Some(&off), true).is_err());
    }

    #[test]
    fn reads_that_disagree_are_an_error() {
        let coarse: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0]));
        let fine: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5_000]));
        assert!(exact_values(&coarse, Some(&fine), false).is_err());
    }

    #[test]
    fn nanoseconds_refuse_what_they_cannot_hold() {
        let (coarse, fine) = reads(&[FAR_PAST, 1_767_261_600_123_456_789]);
        let exact = exact_values(&coarse, Some(&fine), false).unwrap();
        let field = Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true);
        let err = nanoseconds(&coarse, &exact, &field).unwrap_err().to_string();
        assert!(err.contains("1500-06-01"), "{err}");
        assert!(err.contains("column ts"), "{err}");
        let (coarse, fine) = reads(&[1_767_261_600_123_456_789]);
        let exact = exact_values(&coarse, Some(&fine), false).unwrap();
        let read = nanoseconds(&coarse, &exact, &field).unwrap();
        assert_eq!(
            read.as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>()
                .value(0),
            1_767_261_600_123_456_789
        );
    }
}
