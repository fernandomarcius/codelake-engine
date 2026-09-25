//! How a Parquet file's columns map to a hive table's columns, the way
//! Trino 431's Hive connector maps them:
//!
//! - by name, ignoring case: a file written by another tool with `ID`/`NOME`
//!   feeds the table columns `id`/`nome` (an exact match wins);
//! - a column the file lacks reads as NULL (added after the file was written);
//! - a column whose stored type the table's type cannot hold without
//!   inventing values is an error, never a cast: a column dropped and added
//!   again as `varchar` must not turn the old integers into strings. Trino
//!   refuses such files with `Unsupported Trino column type`; so do we.
//!
//! - timestamps finer than the table's precision (a file written by Spark,
//!   pandas or a Trino catalog at another precision) are rounded half up, as
//!   Trino reads them; Arrow's cast would cut them;
//! - `varchar(n)` values longer than `n` are cut to `n` characters, as Trino's
//!   reader does; `char(n)` values are padded with spaces to `n` characters,
//!   as Trino shows them.
//!
//! `DataFusion`'s default adapter matches names exactly and casts whatever
//! Arrow can cast. This one renames the table's columns to the file's
//! spelling, checks the types, then hands over to the default adapter.
//! `INT96` timestamps hold instants: in a catalog whose Parquet time zone is
//! not UTC they read as the wall-clock time of that zone
//! (`codelake_core::parquet_time_zone`), as the reference engine's
//! `hive.parquet.time-zone` reads them; other timestamps keep their values.
//! The Parquet reader marks `INT96` columns with the zone
//! [`INT96_ZONE_MARKER`] so the adapter can tell them apart.
//!
//! A `raw` adapter (compaction) keeps stored values as they are, but for
//! one thing: compaction writes every timestamp back as an `INT96` instant,
//! so in a catalog whose Parquet time zone is not UTC it reads the other
//! timestamps (wall-clock times, `INT64` from Spark or pandas) as the
//! instants of that zone. A wall-clock time the zone skips has no instant
//! that reads back as it: reading it is an error, never another value. A
//! `nanos_only` adapter reads only the timestamps a file stores in
//! nanoseconds, every other column as NULL (see `exact_timestamps.rs`).

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::{CastOptions, cast, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};

use codelake_core::parquet_time_zone::ParquetTimeZone;

use super::table::{finer, per_second, round_timestamps};
use super::types::{char_column, declared_length};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapter, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};

/// Adapter factory of one hive table (`name` is used in errors).
#[derive(Debug)]
pub(crate) struct HiveColumns {
    pub(crate) table: String,
    /// Stored values as they are: no rounding, no `varchar(n)` cut.
    pub(crate) raw: bool,
    /// Only columns the file stores as nanosecond timestamps (`INT96` read
    /// in nanoseconds included); any other column reads as NULL.
    pub(crate) nanos_only: bool,
    /// The catalog's Parquet time zone, unless UTC: `INT96` instants read as
    /// its wall-clock time (not in a `raw` read).
    pub(crate) zone: Option<ParquetTimeZone>,
}

/// Zone the Parquet reader gives `INT96` columns (`coerce_int96_tz`) when the
/// catalog's zone is not UTC. The values stay UTC instants; the name only
/// marks the column, and no writer names a zone with this alias of UTC.
pub(crate) const INT96_ZONE_MARKER: &str = "Etc/Universal";

fn is_int96(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, Some(zone)) if zone.as_ref() == INT96_ZONE_MARKER)
}

/// How a column's stored values become the table's.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Read {
    /// Round half up to the table's unit (and zone).
    Round(TimeUnit, Option<Arc<str>>),
    /// Cut to this many characters.
    Cut(usize),
    /// A `char(n)`: trailing spaces removed, cut to `n`, padded to `n`.
    Pad(usize),
    /// `INT96` instants as the zone's wall-clock time, in this unit (the
    /// stored one, or a finer one).
    Local(ParquetTimeZone, TimeUnit),
    /// Wall-clock times (not `INT96`) as the zone's instants, in this unit
    /// (cast from the stored one): what compaction writes as `INT96`.
    Instant(ParquetTimeZone, TimeUnit),
}

/// A column read with [`Read`] applied (see the module documentation).
#[derive(Debug, Clone)]
struct HiveRead {
    child: Arc<dyn PhysicalExpr>,
    read: Read,
}

impl PartialEq for HiveRead {
    fn eq(&self, other: &Self) -> bool {
        self.read == other.read && self.child.eq(&other.child)
    }
}

impl Eq for HiveRead {}

impl std::hash::Hash for HiveRead {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.read.hash(state);
    }
}

impl fmt::Display for HiveRead {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.read {
            Read::Round(unit, _) => write!(f, "hive_round({}, {unit:?})", self.child),
            Read::Cut(length) => write!(f, "hive_varchar({}, {length})", self.child),
            Read::Pad(length) => write!(f, "hive_char({}, {length})", self.child),
            Read::Local(zone, _) => write!(f, "hive_local({}, {zone})", self.child),
            Read::Instant(zone, _) => write!(f, "hive_instant({}, {zone})", self.child),
        }
    }
}

/// A `char(n)` value as Trino shows it: without its trailing spaces, cut to
/// `n` characters, then padded with spaces to `n`.
pub(crate) fn pad_char(value: &str, length: usize) -> String {
    let trimmed = value.trim_end_matches(' ');
    let mut padded: String = trimmed.chars().take(length).collect();
    let count = padded.chars().count();
    padded.extend(std::iter::repeat_n(' ', length.saturating_sub(count)));
    padded
}

impl PhysicalExpr for HiveRead {
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let child = self.child.return_field(input_schema)?;
        let data_type = match &self.read {
            Read::Round(unit, zone) => DataType::Timestamp(*unit, zone.clone()),
            Read::Local(_, unit) | Read::Instant(_, unit) => DataType::Timestamp(*unit, None),
            Read::Cut(_) | Read::Pad(_) => DataType::Utf8,
        };
        Ok(Arc::new(child.as_ref().clone().with_data_type(data_type)))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let read = match &self.read {
            Read::Round(unit, zone) => round_timestamps(&values, *unit, zone.clone())?,
            Read::Local(zone, unit) => local_timestamps(&values, *zone, *unit)?,
            Read::Instant(zone, unit) => instant_timestamps(&values, *zone, *unit)?,
            Read::Cut(length) => {
                let text = cast(&values, &DataType::Utf8)?;
                let text = text.as_string::<i32>();
                let cut: StringArray = text
                    .iter()
                    .map(|value| {
                        value.map(|v| match v.char_indices().nth(*length) {
                            Some((at, _)) => &v[..at],
                            None => v,
                        })
                    })
                    .collect();
                Arc::new(cut)
            }
            Read::Pad(length) => {
                let text = cast(&values, &DataType::Utf8)?;
                let text = text.as_string::<i32>();
                let padded: StringArray = text
                    .iter()
                    .map(|value| value.map(|v| pad_char(v, *length)))
                    .collect();
                Arc::new(padded)
            }
        };
        Ok(ColumnarValue::Array(read))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [child]: [Arc<dyn PhysicalExpr>; 1] = children
            .try_into()
            .map_err(|_| DataFusionError::Internal("a hive column read has one child".into()))?;
        Ok(Arc::new(Self {
            child,
            read: self.read.clone(),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.child.fmt_sql(f)
    }
}

/// `INT96` instants (UTC) as the wall-clock time of `zone`, in `unit` (the
/// stored unit or a finer one).
fn local_timestamps(array: &ArrayRef, zone: ParquetTimeZone, unit: TimeUnit) -> Result<ArrayRef> {
    let DataType::Timestamp(stored, _) = array.data_type() else {
        return Err(DataFusionError::Internal(format!(
            "reading a {} as a timestamp",
            array.data_type()
        )));
    };
    let per = per_second(*stored);
    let raw = cast(array, &DataType::Int64)?;
    let local: Int64Array = raw
        .as_primitive::<Int64Type>()
        .iter()
        .map(|value| {
            value
                .map(|v| {
                    zone.offset_at(v.div_euclid(per))
                        .and_then(|offset| offset.checked_mul(per))
                        .and_then(|offset| v.checked_add(offset))
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!("timestamp out of range in time zone {zone}"))
                        })
                })
                .transpose()
        })
        .collect::<Result<_>>()?;
    let local: ArrayRef = Arc::new(local);
    let local = cast(&local, &DataType::Timestamp(*stored, None))?;
    if *stored == unit {
        return Ok(local);
    }
    let checked = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    Ok(cast_with_options(
        &local,
        &DataType::Timestamp(unit, None),
        &checked,
    )?)
}

/// The timestamp type a column stored in `unit` holds, and its range.
fn range_of(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "timestamp(0)",
        TimeUnit::Millisecond => "timestamp(3)",
        TimeUnit::Microsecond => "timestamp(6)",
        TimeUnit::Nanosecond => {
            "timestamp(9) (1677-09-21 00:12:43.145224192 to 2262-04-11 23:47:16.854775807 UTC)"
        }
    }
}

/// Wall-clock times (a timestamp array that is not `INT96`) as the instants
/// of `zone`, in `unit`. The zone's offsets are whole seconds, so the digits
/// below a second stay; the cast to `unit` is the one a raw read without a
/// zone makes. A time the zone skips (a gap) is an error: its instant reads
/// back as another wall-clock time.
fn instant_timestamps(array: &ArrayRef, zone: ParquetTimeZone, unit: TimeUnit) -> Result<ArrayRef> {
    let DataType::Timestamp(stored, _) = array.data_type() else {
        return Err(DataFusionError::Internal(format!(
            "reading a {} as a timestamp",
            array.data_type()
        )));
    };
    let per = i128::from(per_second(*stored));
    let nanos_per = 1_000_000_000 / per;
    let raw = cast(array, &DataType::Int64)?;
    let instants: Int64Array = raw
        .as_primitive::<Int64Type>()
        .iter()
        .enumerate()
        .map(|(row, value)| {
            value
                .map(|v| {
                    let local = i128::from(v) * nanos_per;
                    let shown = || array_value_to_string(array, row).unwrap_or_else(|_| v.to_string());
                    let out_of_range = || {
                        DataFusionError::Execution(format!(
                            "timestamp {} in time zone {zone} is an instant outside the range of {}",
                            shown(),
                            range_of(*stored)
                        ))
                    };
                    let instant = zone.local_to_utc(local).ok_or_else(out_of_range)?;
                    match zone.utc_to_local(instant) {
                        Some(back) if back == local => {
                            i64::try_from(instant / nanos_per).map_err(|_| out_of_range())
                        }
                        Some(_) => Err(DataFusionError::Execution(format!(
                            "timestamp {} does not exist in time zone {zone} (the clocks skip it): \
                             written as INT96 it would read as another time",
                            shown()
                        ))),
                        None => Err(out_of_range()),
                    }
                })
                .transpose()
        })
        .collect::<Result<_>>()?;
    let instants: ArrayRef = Arc::new(instants);
    let instants = cast(&instants, &DataType::Timestamp(*stored, None))?;
    if *stored == unit {
        return Ok(instants);
    }
    let checked = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    Ok(cast_with_options(
        &instants,
        &DataType::Timestamp(unit, None),
        &checked,
    )?)
}

/// Whether values stored as `file` read as `table` without loss or invention.
fn readable_as(file: &DataType, table: &DataType) -> bool {
    use DataType::{
        Binary, BinaryView, Date32, Date64, Decimal128, Dictionary, FixedSizeBinary, Float32, Float64, Int8,
        Int16, Int32, Int64, LargeBinary, LargeUtf8, Null, Timestamp, Utf8, Utf8View,
    };
    if let Dictionary(_, values) = file {
        return readable_as(values, table);
    }
    if file == table || matches!(file, Null) {
        return true;
    }
    match (file, table) {
        (Int8, Int16 | Int32 | Int64)
        | (Int16, Int32 | Int64)
        | (Int32, Int64)
        | (Float32, Float64)
        | (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
        | (Binary | LargeBinary | BinaryView | FixedSizeBinary(_), Binary | LargeBinary | BinaryView)
        | (Timestamp(_, _), Timestamp(_, None))
        | (Date32 | Date64, Date32) => true,
        // A wider decimal holds every value; more fraction digits only add zeros.
        (Decimal128(file_precision, file_scale), Decimal128(precision, scale)) => {
            let (file_precision, file_scale) = (i16::from(*file_precision), i16::from(*file_scale));
            let (precision, scale) = (i16::from(*precision), i16::from(*scale));
            file_scale <= scale && file_precision - file_scale <= precision - scale
        }
        _ => false,
    }
}

impl PhysicalExprAdapterFactory for HiveColumns {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let mut fields: Vec<Field> = Vec::with_capacity(logical_file_schema.fields().len());
        let mut refused = Vec::with_capacity(logical_file_schema.fields().len());
        let mut reads = Vec::with_capacity(logical_file_schema.fields().len());
        let mut nulls = Vec::with_capacity(logical_file_schema.fields().len());
        for field in logical_file_schema.fields() {
            let stored = if let Ok(index) = physical_file_schema.index_of(field.name()) {
                Some(index)
            } else {
                let mut matches = physical_file_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| f.name().eq_ignore_ascii_case(field.name()));
                match (matches.next(), matches.next()) {
                    (Some((index, _)), None) => Some(index),
                    _ => None,
                }
            };
            let stored = stored.map(|index| physical_file_schema.field(index));
            let skipped = self.nanos_only
                && !stored
                    .is_some_and(|f| matches!(f.data_type(), DataType::Timestamp(TimeUnit::Nanosecond, _)));
            nulls.push(skipped);
            let Some(stored) = stored.filter(|_| !skipped) else {
                fields.push(field.as_ref().clone());
                refused.push(None);
                reads.push(Vec::new());
                continue;
            };
            let read: Vec<Read> = match (stored.data_type(), field.data_type()) {
                (DataType::Timestamp(..), DataType::Timestamp(to, _))
                    if self.raw && !is_int96(stored.data_type()) =>
                {
                    self.zone
                        .map(|zone| Read::Instant(zone, *to))
                        .into_iter()
                        .collect()
                }
                _ if self.raw => Vec::new(),
                (DataType::Timestamp(from, _), DataType::Timestamp(to, zone)) => {
                    let local = self
                        .zone
                        .filter(|_| is_int96(stored.data_type()))
                        .map(|parquet_zone| {
                            Read::Local(parquet_zone, if finer(*from, *to) { *from } else { *to })
                        });
                    let round = finer(*from, *to).then(|| Read::Round(*to, zone.clone()));
                    local.into_iter().chain(round).collect()
                }
                _ => declared_length(field)
                    .map(Read::Cut)
                    .or_else(|| char_column(field).map(Read::Pad))
                    .into_iter()
                    .collect(),
            };
            // Refused only if the query reads the column, as in Trino.
            refused.push((!readable_as(stored.data_type(), field.data_type())).then(|| {
                format!(
                    "Unsupported Trino column type for Parquet column [{}] of {}: the file stores {}, \
                     the table declares {}",
                    stored.name(),
                    self.table,
                    stored.data_type(),
                    field.data_type()
                )
            }));
            // A rounded or zoned column reaches the adapter below in its
            // stored type, so it does not cast (cut) it first.
            let spelled = field.as_ref().clone().with_name(stored.name());
            fields.push(
                if read
                    .iter()
                    .any(|read| matches!(read, Read::Round(..) | Read::Local(..) | Read::Instant(..)))
                {
                    spelled.with_data_type(stored.data_type().clone())
                } else {
                    spelled
                },
            );
            reads.push(read);
        }
        let renamed = Arc::new(Schema::new_with_metadata(
            fields,
            logical_file_schema.metadata().clone(),
        ));
        Ok(Arc::new(HiveColumnAdapter {
            refused,
            reads,
            nulls,
            logical: logical_file_schema,
            renamed: Arc::clone(&renamed),
            inner: DefaultPhysicalExprAdapter::new(renamed, physical_file_schema),
        }))
    }
}

#[derive(Debug)]
struct HiveColumnAdapter {
    /// Why each table column cannot be read from this file, if it cannot.
    refused: Vec<Option<String>>,
    /// How each table column's stored values become the table's, in order.
    reads: Vec<Vec<Read>>,
    /// Columns read as NULL whatever the file holds (`nanos_only`).
    nulls: Vec<bool>,
    /// The table's columns (what expressions refer to).
    logical: SchemaRef,
    /// The same columns, spelled as in the file.
    renamed: SchemaRef,
    inner: DefaultPhysicalExprAdapter,
}

impl PhysicalExprAdapter for HiveColumnAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let respelled = expr
            .transform(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let index = column.index();
                let (Some(logical), Some(renamed)) =
                    (self.logical.fields().get(index), self.renamed.fields().get(index))
                else {
                    return Ok(Transformed::no(expr));
                };
                if logical.name() != column.name() {
                    return Ok(Transformed::no(expr));
                }
                if self.nulls.get(index).copied().unwrap_or(false) {
                    let null = ScalarValue::try_from(logical.data_type())?;
                    return Ok(Transformed::yes(
                        Arc::new(Literal::new(null)) as Arc<dyn PhysicalExpr>
                    ));
                }
                if let Some(Some(reason)) = self.refused.get(index) {
                    return Err(DataFusionError::Execution(reason.clone()));
                }
                let reads = self.reads.get(index).cloned().unwrap_or_default();
                if renamed.name() == column.name() && reads.is_empty() {
                    return Ok(Transformed::no(expr));
                }
                let respelled: Arc<dyn PhysicalExpr> = Arc::new(Column::new(renamed.name(), index));
                Ok(Transformed::yes(
                    reads.into_iter().fold(respelled, |child, read| {
                        Arc::new(HiveRead { child, read }) as Arc<dyn PhysicalExpr>
                    }),
                ))
            })
            .data()?;
        self.inner.rewrite(respelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::TimeUnit;

    #[test]
    fn stored_types_a_table_type_can_hold() {
        let ms = DataType::Timestamp(TimeUnit::Millisecond, None);
        for (file, table) in [
            (DataType::Int32, DataType::Int64),
            (DataType::Int8, DataType::Int32),
            (DataType::Float32, DataType::Float64),
            (DataType::Utf8View, DataType::Utf8),
            (DataType::Timestamp(TimeUnit::Nanosecond, None), ms.clone()),
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ms.clone(),
            ),
            (DataType::Decimal128(5, 2), DataType::Decimal128(18, 4)),
            (
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                DataType::Utf8,
            ),
            (DataType::Null, DataType::Utf8),
        ] {
            assert!(readable_as(&file, &table), "{file} as {table}");
        }
        for (file, table) in [
            (DataType::Int32, DataType::Utf8),
            (DataType::Int64, DataType::Int32),
            (DataType::Utf8, DataType::Int64),
            (DataType::Float64, DataType::Float32),
            (DataType::Int64, DataType::Float64),
            (DataType::Decimal128(18, 4), DataType::Decimal128(18, 2)),
            (DataType::Decimal128(20, 2), DataType::Decimal128(18, 2)),
            (DataType::Boolean, DataType::Int32),
            (DataType::Date32, ms),
        ] {
            assert!(!readable_as(&file, &table), "{file} as {table}");
        }
    }

    #[test]
    fn compaction_reads_wall_clock_times_as_instants_of_the_zone() {
        use datafusion::arrow::array::{Array, TimestampMillisecondArray};
        let zone: ParquetTimeZone = "America/Sao_Paulo".parse().unwrap();
        // 2026-01-01 10:00:00.456 (UTC-3) and 1969-12-31 23:59:59.999 (UTC-3).
        let wall: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(1_767_261_600_456),
            None,
            Some(-1),
        ]));
        let instants = instant_timestamps(&wall, zone, TimeUnit::Microsecond).unwrap();
        assert_eq!(
            instants.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let micros = cast(&instants, &DataType::Int64).unwrap();
        let micros = micros.as_primitive::<Int64Type>();
        assert_eq!(micros.value(0), (1_767_261_600_456 + 3 * 3_600_000) * 1_000);
        assert!(micros.is_null(1));
        assert_eq!(micros.value(2), (-1 + 3 * 3_600_000) * 1_000);
        // Reading them back in the zone gives the wall-clock times again.
        let back = local_timestamps(&instants, zone, TimeUnit::Millisecond).unwrap();
        assert_eq!(
            cast(&back, &DataType::Int64)
                .unwrap()
                .as_primitive::<Int64Type>()
                .values(),
            cast(&wall, &DataType::Int64)
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
        );
    }

    #[test]
    fn compaction_refuses_a_wall_clock_time_the_zone_skips() {
        use datafusion::arrow::array::TimestampSecondArray;
        let zone: ParquetTimeZone = "America/Sao_Paulo".parse().unwrap();
        // 2018-11-04 00:30:00: the clocks jumped from 00:00 to 01:00.
        let wall: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1_541_291_400]));
        let err = instant_timestamps(&wall, zone, TimeUnit::Microsecond)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("2018-11-04T00:30:00")
                && err.contains("does not exist in time zone America/Sao_Paulo"),
            "{err}"
        );
    }

    #[test]
    fn compaction_names_an_instant_nanoseconds_cannot_hold() {
        use datafusion::arrow::array::TimestampNanosecondArray;
        let zone: ParquetTimeZone = "America/Sao_Paulo".parse().unwrap();
        // 2262-04-11 21:50:36.854775807 in São Paulo is 2262-04-12
        // 00:50:36.854775807 UTC, past the last instant Arrow nanoseconds hold.
        let wall: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![i64::MAX - 7_000_000_000_000]));
        let err = instant_timestamps(&wall, zone, TimeUnit::Nanosecond)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("2262-04-11T21:50:36.854775807")
                && err.contains(
                    "in time zone America/Sao_Paulo is an instant outside the range of timestamp(9)"
                )
                && !err.contains("does not exist"),
            "{err}"
        );
    }

    #[test]
    fn char_values_read_padded_like_trino() {
        assert_eq!(pad_char("ab", 5), "ab   ");
        assert_eq!(pad_char("abcde  ", 5), "abcde");
        assert_eq!(pad_char("", 2), "  ");
        assert_eq!(pad_char("abcdef", 3), "abc");
        assert_eq!(pad_char("ç", 2), "ç ");
    }

    #[test]
    fn columns_match_ignoring_case_and_refuse_foreign_types() {
        let table = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("nome", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let file = Arc::new(Schema::new(vec![
            Field::new("ID", DataType::Int64, true),
            Field::new("NOME", DataType::Utf8, true),
        ]));
        let factory = HiveColumns {
            table: "s.t".into(),
            raw: false,
            nanos_only: false,
            zone: None,
        };
        let adapter = factory.create(Arc::clone(&table), Arc::clone(&file)).unwrap();
        let nome: Arc<dyn PhysicalExpr> = Arc::new(Column::new("nome", 1));
        let rewritten = adapter.rewrite(nome).unwrap();
        let column = rewritten.downcast_ref::<Column>().unwrap();
        assert_eq!((column.name(), column.index()), ("NOME", 1));
        // Missing from the file: NULL, not an error.
        let extra: Arc<dyn PhysicalExpr> = Arc::new(Column::new("extra", 2));
        assert!(adapter.rewrite(extra).unwrap().downcast_ref::<Column>().is_none());

        // A retyped column fails only when it is read.
        let retyped = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("NOME", DataType::Int32, true),
        ]));
        let adapter = factory.create(table, retyped).unwrap();
        let id: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        assert!(adapter.rewrite(id).is_ok());
        let nome: Arc<dyn PhysicalExpr> = Arc::new(Column::new("nome", 1));
        let err = adapter.rewrite(nome).unwrap_err().to_string();
        assert!(err.contains("Unsupported Trino column type"), "{err}");
        assert!(err.contains("s.t"), "{err}");
    }
}
