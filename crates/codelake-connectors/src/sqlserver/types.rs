//! SQL Server → Arrow type mapping and row-to-batch conversion.
//!
//! Choices that affect results (all covered by integration tests):
//! - `tinyint` (0..255) maps to `Int16`, as in Trino.
//! - Temporal types keep the precision they declare, as in Trino:
//!   `datetime` is `timestamp(3)` (its 1/300 s ticks rounded to the nearest
//!   millisecond, as SQL Server converts to `datetime2(3)`), `smalldatetime`
//!   `timestamp(0)`, `datetime2(p)` and `datetimeoffset(p)` seconds (`p = 0`),
//!   milliseconds (`p ≤ 3`) or microseconds. Nanoseconds cannot represent the
//!   `9999-12-31` sentinel common in ERPs; the 7th fractional digit of
//!   `datetime2(7)` is truncated.
//! - `datetimeoffset` is converted to UTC.
//! - `uniqueidentifier` is rendered upper-case, like `CAST(x AS varchar)`.
//! - `money`/`smallmoney` map to `Decimal128(19,4)`/`(10,4)`. TDS decodes them
//!   to `f64`; values are converted back exactly and rejected beyond 2^53
//!   ten-thousandths, where `f64` has already lost digits.
//! - Unsupported types (`sql_variant`, `geography`, `hierarchyid`, …) are not
//!   exposed, which is also Trino's default.
//!
//! A value that does not fit the column's Arrow type is an error, never a
//! silent NULL or truncation.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    Time64NanosecondBuilder,
};
use datafusion::arrow::datatypes::{
    DataType, SchemaRef, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampSecondType,
};
use datafusion::error::{DataFusionError, Result};
use tiberius::ColumnData;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};

/// Days from 0001-01-01 to 1970-01-01.
const DAYS_0001_TO_EPOCH: i64 = 719_162;
/// Days from 1900-01-01 to 1970-01-01.
const DAYS_1900_TO_EPOCH: i64 = 25_567;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const MILLIS_PER_DAY: i64 = 86_400_000;
const UTC: &str = "+00:00";

/// Unit of a `datetime2(p)` or `datetimeoffset(p)` (`p` defaults to 7).
fn fraction_unit(datetime_precision: Option<i16>) -> TimeUnit {
    match datetime_precision {
        Some(0) => TimeUnit::Second,
        Some(1..=3) => TimeUnit::Millisecond,
        _ => TimeUnit::Microsecond,
    }
}

/// Arrow type for a column described by `INFORMATION_SCHEMA.COLUMNS`, or `None`
/// when the type is not supported.
pub(crate) fn arrow_type(
    data_type: &str,
    precision: Option<u8>,
    scale: Option<i8>,
    datetime_precision: Option<i16>,
) -> Option<DataType> {
    let mapped = match data_type.to_ascii_lowercase().as_str() {
        "bit" => DataType::Boolean,
        "tinyint" | "smallint" => DataType::Int16,
        "int" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "float" => DataType::Float64,
        "money" => DataType::Decimal128(19, 4),
        "smallmoney" => DataType::Decimal128(10, 4),
        "decimal" | "numeric" => DataType::Decimal128(precision.unwrap_or(18), scale.unwrap_or(0)),
        "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext" | "xml" | "uniqueidentifier" => {
            DataType::Utf8
        }
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => DataType::Binary,
        "date" => DataType::Date32,
        "datetime" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "smalldatetime" => DataType::Timestamp(TimeUnit::Second, None),
        "datetime2" => DataType::Timestamp(fraction_unit(datetime_precision), None),
        "datetimeoffset" => DataType::Timestamp(fraction_unit(datetime_precision), Some(UTC.into())),
        "time" => DataType::Time64(TimeUnit::Nanosecond),
        _ => return None,
    };
    Some(mapped)
}

fn mismatch(column: &str, target: &DataType, value: &ColumnData<'_>) -> DataFusionError {
    DataFusionError::Execution(format!(
        "sqlserver: column `{column}` expects {target} but the server sent {}",
        variant_name(value)
    ))
}

fn variant_name(value: &ColumnData<'_>) -> &'static str {
    match value {
        ColumnData::U8(_) => "tinyint",
        ColumnData::I16(_) => "smallint",
        ColumnData::I32(_) => "int",
        ColumnData::I64(_) => "bigint",
        ColumnData::F32(_) => "real",
        ColumnData::F64(_) => "float",
        ColumnData::Bit(_) => "bit",
        ColumnData::String(_) => "string",
        ColumnData::Guid(_) => "uniqueidentifier",
        ColumnData::Binary(_) => "binary",
        ColumnData::Numeric(_) => "numeric",
        ColumnData::Xml(_) => "xml",
        ColumnData::DateTime(_) => "datetime",
        ColumnData::SmallDateTime(_) => "smalldatetime",
        ColumnData::Time(_) => "time",
        ColumnData::Date(_) => "date",
        ColumnData::DateTime2(_) => "datetime2",
        ColumnData::DateTimeOffset(_) => "datetimeoffset",
    }
}

fn is_null(value: &ColumnData<'_>) -> bool {
    match value {
        ColumnData::U8(v) => v.is_none(),
        ColumnData::I16(v) => v.is_none(),
        ColumnData::I32(v) => v.is_none(),
        ColumnData::I64(v) => v.is_none(),
        ColumnData::F32(v) => v.is_none(),
        ColumnData::F64(v) => v.is_none(),
        ColumnData::Bit(v) => v.is_none(),
        ColumnData::String(v) => v.is_none(),
        ColumnData::Guid(v) => v.is_none(),
        ColumnData::Binary(v) => v.is_none(),
        ColumnData::Numeric(v) => v.is_none(),
        ColumnData::Xml(v) => v.is_none(),
        ColumnData::DateTime(v) => v.is_none(),
        ColumnData::SmallDateTime(v) => v.is_none(),
        ColumnData::Time(v) => v.is_none(),
        ColumnData::Date(v) => v.is_none(),
        ColumnData::DateTime2(v) => v.is_none(),
        ColumnData::DateTimeOffset(v) => v.is_none(),
    }
}

/// Integer value of any integral TDS type.
fn integer(value: &ColumnData<'_>) -> Option<i64> {
    match value {
        ColumnData::U8(Some(v)) => Some(i64::from(*v)),
        ColumnData::I16(Some(v)) => Some(i64::from(*v)),
        ColumnData::I32(Some(v)) => Some(i64::from(*v)),
        ColumnData::I64(Some(v)) => Some(*v),
        _ => None,
    }
}

/// Largest magnitude an `f64` holds exactly: beyond it TDS money has lost digits.
const F64_EXACT: f64 = 9_007_199_254_740_992.0;

/// `f64` → decimal at `scale`, only when the conversion is exact.
fn exact_decimal(value: f64, scale: i8) -> Option<i128> {
    let scaled = value * 10_f64.powi(i32::from(scale));
    let rounded = scaled.round();
    if !rounded.is_finite() || rounded.abs() >= F64_EXACT || (scaled - rounded).abs() > 1e-6 {
        return None;
    }
    #[allow(clippy::cast_possible_truncation, reason = "bounded by F64_EXACT above")]
    Some(rounded as i128)
}

pub(crate) fn date_days(date: Date) -> i32 {
    // `Date::days()` is at most 3_652_058 (9999-12-31): fits i32 after the shift.
    i32::try_from(i64::from(date.days()) - DAYS_0001_TO_EPOCH).unwrap_or(i32::MAX)
}

fn nanos_of_day(increments: u64, scale: u8) -> i64 {
    let factor = 10_u64.pow(9_u32.saturating_sub(u32::from(scale)));
    i64::try_from(increments.saturating_mul(factor)).unwrap_or(i64::MAX)
}

pub(crate) fn time_nanos(time: Time) -> i64 {
    nanos_of_day(time.increments(), time.scale())
}

pub(crate) fn datetime2_micros(value: DateTime2) -> i64 {
    let days = i64::from(value.date().days()) - DAYS_0001_TO_EPOCH;
    days * MICROS_PER_DAY + time_nanos(value.time()) / 1_000
}

/// `datetime` stores 1/300 s ticks; round to the nearest microsecond.
pub(crate) fn datetime_micros(value: DateTime) -> i64 {
    let days = i64::from(value.days()) - DAYS_1900_TO_EPOCH;
    let ticks = i64::from(value.seconds_fragments());
    days * MICROS_PER_DAY + (ticks * 20_000 + 3) / 6
}

/// `datetime` to milliseconds, each 1/300 s tick rounded to the nearest
/// millisecond (`.xx3`, `.xx7`, `.xx0`), as SQL Server and the reference
/// engine read it. A third of a millisecond never ties.
pub(crate) fn datetime_millis(value: DateTime) -> i64 {
    let days = i64::from(value.days()) - DAYS_1900_TO_EPOCH;
    let ticks = i64::from(value.seconds_fragments());
    days * MILLIS_PER_DAY + (ticks * 10 + 1) / 3
}

/// `smalldatetime` stores minutes since midnight.
pub(crate) fn smalldatetime_micros(value: SmallDateTime) -> i64 {
    let days = i64::from(value.days()) - DAYS_1900_TO_EPOCH;
    days * MICROS_PER_DAY + i64::from(value.seconds_fragments()) * 60_000_000
}

/// `datetimeoffset` is transmitted as UTC plus an offset; the UTC part is kept.
pub(crate) fn datetimeoffset_micros(value: DateTimeOffset) -> i64 {
    datetime2_micros(value.datetime2())
}

fn rescale(numeric: Numeric, target_scale: i8) -> Option<i128> {
    let from = i32::from(numeric.scale());
    let to = i32::from(target_scale);
    if from == to {
        return Some(numeric.value());
    }
    // Losing digits would change the value; only widen.
    let diff = u32::try_from(to - from).ok()?;
    numeric.value().checked_mul(10_i128.checked_pow(diff)?)
}

/// Column builder bound to one Arrow type.
enum Column {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder, u8, i8),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    /// Values in the column's unit; typed when the array is finished.
    Timestamp(Int64Builder, TimeUnit),
    Time64(Time64NanosecondBuilder),
}

/// Microseconds to `unit`. The column types never make a value finer than
/// its unit (`datetime` has its own rounding), so this division is exact.
fn from_micros(micros: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => micros.div_euclid(1_000_000),
        TimeUnit::Millisecond => micros.div_euclid(1_000),
        TimeUnit::Microsecond => micros,
        TimeUnit::Nanosecond => micros.saturating_mul(1_000),
    }
}

impl Column {
    fn new(data_type: &DataType, capacity: usize) -> Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Int16 => Self::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Decimal128(p, s) => Self::Decimal(Decimal128Builder::with_capacity(capacity), *p, *s),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16)),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(
                unit @ (TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond),
                _,
            ) => Self::Timestamp(Int64Builder::with_capacity(capacity), *unit),
            DataType::Time64(TimeUnit::Nanosecond) => {
                Self::Time64(Time64NanosecondBuilder::with_capacity(capacity))
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "sqlserver: cannot produce Arrow type {other}"
                )));
            }
        })
    }

    fn len(&self) -> usize {
        match self {
            Self::Boolean(b) => b.len(),
            Self::Int16(b) => b.len(),
            Self::Int32(b) => b.len(),
            Self::Int64(b) | Self::Timestamp(b, _) => b.len(),
            Self::Float32(b) => b.len(),
            Self::Float64(b) => b.len(),
            Self::Decimal(b, ..) => b.len(),
            Self::Utf8(b) => b.len(),
            Self::Binary(b) => b.len(),
            Self::Date32(b) => b.len(),
            Self::Time64(b) => b.len(),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Boolean(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) | Self::Timestamp(b, _) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Decimal(b, ..) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Time64(b) => b.append_null(),
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "integer to float follows SQL float semantics"
    )]
    fn append(&mut self, value: &ColumnData<'_>) -> Option<()> {
        if is_null(value) {
            self.append_null();
            return Some(());
        }
        match (self, value) {
            (Self::Boolean(b), ColumnData::Bit(Some(v))) => b.append_value(*v),
            (Self::Int16(b), v) => b.append_value(i16::try_from(integer(v)?).ok()?),
            (Self::Int32(b), v) => b.append_value(i32::try_from(integer(v)?).ok()?),
            (Self::Int64(b), v) => b.append_value(integer(v)?),
            (Self::Float32(b), ColumnData::F32(Some(v))) => b.append_value(*v),
            (Self::Float64(b), ColumnData::F64(Some(v))) => b.append_value(*v),
            (Self::Float64(b), ColumnData::F32(Some(v))) => b.append_value(f64::from(*v)),
            (Self::Float64(b), ColumnData::Numeric(Some(n))) => {
                b.append_value(n.value() as f64 / 10_f64.powi(i32::from(n.scale())));
            }
            (Self::Float64(b), v) => b.append_value(integer(v)? as f64),
            (Self::Decimal(b, _, scale), ColumnData::Numeric(Some(n))) => {
                b.append_value(rescale(*n, *scale)?);
            }
            (Self::Decimal(b, _, scale), ColumnData::F64(Some(v))) => {
                b.append_value(exact_decimal(*v, *scale)?);
            }
            (Self::Decimal(b, _, scale), v) => {
                let factor = 10_i128.checked_pow(u32::try_from(*scale).ok()?)?;
                b.append_value(i128::from(integer(v)?).checked_mul(factor)?);
            }
            (Self::Utf8(b), ColumnData::String(Some(s))) => b.append_value(s),
            (Self::Utf8(b), ColumnData::Guid(Some(g))) => b.append_value(g.to_string().to_uppercase()),
            (Self::Utf8(b), ColumnData::Xml(Some(x))) => b.append_value(x.to_string()),
            (Self::Binary(b), ColumnData::Binary(Some(bytes))) => b.append_value(bytes),
            (Self::Date32(b), ColumnData::Date(Some(d))) => b.append_value(date_days(*d)),
            (Self::Timestamp(b, TimeUnit::Millisecond), ColumnData::DateTime(Some(v))) => {
                b.append_value(datetime_millis(*v));
            }
            (Self::Timestamp(b, unit), ColumnData::DateTime(Some(v))) => {
                b.append_value(from_micros(datetime_micros(*v), *unit));
            }
            (Self::Timestamp(b, unit), ColumnData::DateTime2(Some(v))) => {
                b.append_value(from_micros(datetime2_micros(*v), *unit));
            }
            (Self::Timestamp(b, unit), ColumnData::SmallDateTime(Some(v))) => {
                b.append_value(from_micros(smalldatetime_micros(*v), *unit));
            }
            (Self::Timestamp(b, unit), ColumnData::DateTimeOffset(Some(v))) => {
                b.append_value(from_micros(datetimeoffset_micros(*v), *unit));
            }
            (Self::Timestamp(b, unit), ColumnData::Date(Some(d))) => {
                b.append_value(from_micros(i64::from(date_days(*d)) * MICROS_PER_DAY, *unit));
            }
            (Self::Time64(b), ColumnData::Time(Some(t))) => b.append_value(time_nanos(*t)),
            _ => return None,
        }
        Some(())
    }

    fn finish(&mut self, data_type: &DataType) -> Result<ArrayRef> {
        Ok(match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Decimal(b, p, s) => Arc::new(b.finish().with_precision_and_scale(*p, *s)?),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Timestamp(b, unit) => {
                let values = b.finish();
                let zone = match data_type {
                    DataType::Timestamp(_, Some(tz)) => Some(Arc::clone(tz)),
                    _ => None,
                };
                match unit {
                    TimeUnit::Second => Arc::new(
                        values
                            .reinterpret_cast::<TimestampSecondType>()
                            .with_timezone_opt(zone),
                    ),
                    TimeUnit::Millisecond => Arc::new(
                        values
                            .reinterpret_cast::<TimestampMillisecondType>()
                            .with_timezone_opt(zone),
                    ),
                    TimeUnit::Microsecond | TimeUnit::Nanosecond => Arc::new(
                        values
                            .reinterpret_cast::<TimestampMicrosecondType>()
                            .with_timezone_opt(zone),
                    ),
                }
            }
            Self::Time64(b) => Arc::new(b.finish()),
        })
    }
}

/// Accumulates rows into record batches of at most `batch_size` rows.
pub(crate) struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<Column>,
    batch_size: usize,
}

impl BatchBuilder {
    pub(crate) fn new(schema: SchemaRef, batch_size: usize) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|f| Column::new(f.data_type(), batch_size))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            columns,
            batch_size,
        })
    }

    /// Appends one row. Returns a full batch when `batch_size` is reached.
    pub(crate) fn push<'a>(
        &mut self,
        row: impl IntoIterator<Item = ColumnData<'a>>,
    ) -> Result<Option<RecordBatch>> {
        let mut seen = 0;
        for (index, value) in row.into_iter().enumerate() {
            let field = self.schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "sqlserver: row has more columns than the expected {}",
                    self.schema.fields().len()
                ))
            })?;
            let column = &mut self.columns[index];
            column
                .append(&value)
                .ok_or_else(|| mismatch(field.name(), field.data_type(), &value))?;
            seen = index + 1;
        }
        if seen != self.columns.len() {
            return Err(DataFusionError::Execution(format!(
                "sqlserver: row has {seen} columns, expected {}",
                self.columns.len()
            )));
        }
        if self.columns.first().is_some_and(|c| c.len() >= self.batch_size) {
            return self.finish().map(Some);
        }
        Ok(None)
    }

    /// Returns the pending rows, if any.
    pub(crate) fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.columns.first().is_none_or(|c| c.len() == 0) {
            return Ok(None);
        }
        self.finish().map(Some)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .iter_mut()
            .zip(self.schema.fields())
            .map(|(column, field)| column.finish(field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::{
        Date32Type, Decimal128Type, Field, Int16Type, Int64Type, Schema, Time64NanosecondType,
    };
    use std::borrow::Cow;

    #[test]
    fn maps_every_supported_type() {
        let cases = [
            ("bit", DataType::Boolean),
            ("TINYINT", DataType::Int16),
            ("smallint", DataType::Int16),
            ("int", DataType::Int32),
            ("bigint", DataType::Int64),
            ("real", DataType::Float32),
            ("float", DataType::Float64),
            ("money", DataType::Decimal128(19, 4)),
            ("smallmoney", DataType::Decimal128(10, 4)),
            ("nvarchar", DataType::Utf8),
            ("uniqueidentifier", DataType::Utf8),
            ("varbinary", DataType::Binary),
            ("timestamp", DataType::Binary),
            ("date", DataType::Date32),
            ("datetime", DataType::Timestamp(TimeUnit::Millisecond, None)),
            ("smalldatetime", DataType::Timestamp(TimeUnit::Second, None)),
            ("datetime2", DataType::Timestamp(TimeUnit::Microsecond, None)),
            (
                "datetimeoffset",
                DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            ),
            ("time", DataType::Time64(TimeUnit::Nanosecond)),
        ];
        for (sql, arrow) in cases {
            assert_eq!(arrow_type(sql, None, None, None), Some(arrow), "{sql}");
        }
        assert_eq!(
            arrow_type("decimal", Some(18), Some(6), None),
            Some(DataType::Decimal128(18, 6))
        );
        assert_eq!(
            arrow_type("numeric", None, None, None),
            Some(DataType::Decimal128(18, 0))
        );
        for unsupported in ["sql_variant", "geography", "hierarchyid", "cursor"] {
            assert_eq!(arrow_type(unsupported, None, None, None), None, "{unsupported}");
        }
        // Declared precision of datetime2(p) / datetimeoffset(p).
        for (p, unit) in [
            (0, TimeUnit::Second),
            (1, TimeUnit::Millisecond),
            (3, TimeUnit::Millisecond),
            (4, TimeUnit::Microsecond),
            (7, TimeUnit::Microsecond),
        ] {
            assert_eq!(
                arrow_type("datetime2", None, None, Some(p)),
                Some(DataType::Timestamp(unit, None)),
                "datetime2({p})"
            );
            assert_eq!(
                arrow_type("datetimeoffset", None, None, Some(p)),
                Some(DataType::Timestamp(unit, Some(UTC.into()))),
                "datetimeoffset({p})"
            );
        }
    }

    #[test]
    fn date_and_time_conversions_match_known_instants() {
        // 1970-01-01 is day 719162 since 0001-01-01; 9999-12-31 is day 3652058.
        assert_eq!(date_days(Date::new(719_162)), 0);
        assert_eq!(date_days(Date::new(3_652_058)), 2_932_896);
        // 12:00:00.1234567 at scale 7 → nanoseconds, then truncated to micros.
        let noon = Time::new(432_001_234_567, 7);
        assert_eq!(time_nanos(noon), 43_200_123_456_700);
        let dt2 = DateTime2::new(Date::new(719_163), noon);
        assert_eq!(datetime2_micros(dt2), MICROS_PER_DAY + 43_200_123_456);
        // datetime: 1970-01-02 + 300 ticks (1 s) + 1 tick (3.333 ms → 3333 µs).
        let dt = DateTime::new(25_568, 301);
        assert_eq!(datetime_micros(dt), MICROS_PER_DAY + 1_003_333);
        // …and to milliseconds, rounded: 1 tick → .003, 2 → .007, 3 → .010.
        assert_eq!(datetime_millis(dt), MILLIS_PER_DAY + 1_003);
        assert_eq!(datetime_millis(DateTime::new(25_567, 2)), 7);
        assert_eq!(datetime_millis(DateTime::new(25_567, 3)), 10);
        // 19:53:49.846667 (254 ticks after :49) reads as .847.
        let ticks = (19 * 3600 + 53 * 60 + 49) * 300 + 254;
        assert_eq!(datetime_millis(DateTime::new(25_567, ticks)), 71_629_847);
        // Last tick of a day (.997) stays in the day; before 1970 too.
        assert_eq!(datetime_millis(DateTime::new(25_567, 25_919_999)), 86_399_997);
        assert_eq!(datetime_millis(DateTime::new(25_566, 1)), -MILLIS_PER_DAY + 3);
        // smalldatetime: 1970-01-01 + 90 minutes.
        assert_eq!(
            smalldatetime_micros(SmallDateTime::new(25_567, 90)),
            5_400_000_000
        );
    }

    #[test]
    fn money_converts_exactly_or_fails() {
        assert_eq!(exact_decimal(1.2345, 4), Some(12_345));
        assert_eq!(exact_decimal(-922_337.580_7, 4), Some(-9_223_375_807));
        assert_eq!(exact_decimal(1.23456, 4), None, "a fifth decimal is not money");
        assert_eq!(
            exact_decimal(922_337_203_685_477.5, 4),
            None,
            "f64 already lost digits"
        );
        assert_eq!(exact_decimal(f64::NAN, 4), None);
    }

    #[test]
    fn decimals_are_rescaled_only_upwards() {
        let n = Numeric::new_with_scale(12_345, 2); // 123.45
        assert_eq!(rescale(n, 2), Some(12_345));
        assert_eq!(rescale(n, 4), Some(1_234_500));
        assert_eq!(rescale(n, 1), None, "dropping digits must be an error");
    }

    fn schema(fields: Vec<(&str, DataType)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(n, t)| Field::new(n, t, true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn builds_batches_with_coercions_and_nulls() {
        let schema = schema(vec![
            ("tiny", DataType::Int16),
            ("total", DataType::Int64),
            ("amount", DataType::Decimal128(10, 2)),
            ("lsn", DataType::Binary),
            ("name", DataType::Utf8),
            ("day", DataType::Date32),
            ("at", DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("t", DataType::Time64(TimeUnit::Nanosecond)),
        ]);
        let mut builder = BatchBuilder::new(Arc::clone(&schema), 2).unwrap();
        let lsn: &[u8] = &[0, 0, 0, 42, 0, 0, 0, 1, 0, 3];
        let first = builder
            .push(vec![
                ColumnData::U8(Some(255)),
                ColumnData::I32(Some(7)),
                ColumnData::Numeric(Some(Numeric::new_with_scale(15, 1))),
                ColumnData::Binary(Some(Cow::Borrowed(lsn))),
                ColumnData::String(Some(Cow::Borrowed("Ana"))),
                ColumnData::Date(Some(Date::new(719_162))),
                ColumnData::DateTime2(Some(DateTime2::new(Date::new(719_162), Time::new(1, 7)))),
                ColumnData::Time(Some(Time::new(5, 0))),
            ])
            .unwrap();
        assert!(first.is_none(), "batch not full yet");
        let batch = builder
            .push(vec![
                ColumnData::U8(None),
                ColumnData::I64(None),
                ColumnData::Numeric(None),
                ColumnData::Binary(None),
                ColumnData::String(None),
                ColumnData::Date(None),
                ColumnData::DateTime2(None),
                ColumnData::Time(None),
            ])
            .unwrap()
            .expect("full batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).as_primitive::<Int16Type>().value(0), 255);
        assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 7);
        assert_eq!(batch.column(2).as_primitive::<Decimal128Type>().value(0), 150);
        assert_eq!(batch.column(3).as_binary::<i32>().value(0), lsn);
        assert_eq!(batch.column(4).as_string::<i32>().value(0), "Ana");
        assert_eq!(batch.column(5).as_primitive::<Date32Type>().value(0), 0);
        assert_eq!(
            batch
                .column(6)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            0
        );
        assert_eq!(
            batch.column(7).as_primitive::<Time64NanosecondType>().value(0),
            5_000_000_000
        );
        for column in batch.columns() {
            assert!(column.is_null(1));
        }
        assert!(builder.flush().unwrap().is_none());
    }

    #[test]
    fn out_of_range_values_fail_instead_of_wrapping() {
        let schema = schema(vec![("small", DataType::Int16)]);
        let mut builder = BatchBuilder::new(schema, 8).unwrap();
        let err = builder.push(vec![ColumnData::I32(Some(70_000))]).unwrap_err();
        assert!(err.to_string().contains("expects Int16"), "{err}");
    }

    #[test]
    fn type_mismatches_fail_loudly() {
        let schema = schema(vec![("n", DataType::Int64)]);
        let mut builder = BatchBuilder::new(schema, 8).unwrap();
        let err = builder
            .push(vec![ColumnData::String(Some(Cow::Borrowed("x")))])
            .unwrap_err();
        assert!(err.to_string().contains("server sent string"), "{err}");
    }

    #[test]
    fn wrong_column_count_fails() {
        let schema = schema(vec![("a", DataType::Int64), ("b", DataType::Int64)]);
        let mut builder = BatchBuilder::new(schema, 8).unwrap();
        assert!(builder.push(vec![ColumnData::I64(Some(1))]).is_err());
        let mut builder = BatchBuilder::new(schema_one(), 8).unwrap();
        assert!(
            builder
                .push(vec![ColumnData::I64(Some(1)), ColumnData::I64(Some(2))])
                .is_err()
        );
    }

    fn schema_one() -> SchemaRef {
        schema(vec![("a", DataType::Int64)])
    }

    #[test]
    fn flush_returns_partial_batch_and_guid_is_upper_case() {
        let schema = schema(vec![("id", DataType::Utf8)]);
        let mut builder = BatchBuilder::new(schema, 8).unwrap();
        let guid = tiberius::Uuid::parse_str("6f9619ff-8b86-d011-b42d-00c04fc964ff").unwrap();
        assert!(
            builder
                .push(vec![ColumnData::Guid(Some(guid))])
                .unwrap()
                .is_none()
        );
        let batch = builder.flush().unwrap().unwrap();
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "6F9619FF-8B86-D011-B42D-00C04FC964FF"
        );
        assert_eq!(batch.column(0).len(), 1);
    }
}
