//! Statistics the hive scan hands to the planner, all `Inexact` (see
//! [`crate::file_statistics`] for why they can only change the plan).
//!
//! The metastore's table statistics are used while they still describe the
//! files on disk: `numFiles` and `totalSize` must equal what the listing
//! found. A writer that adds or replaces files without updating them, or
//! `CodeLake`'s own `INSERT` (which removes them, see
//! `TableMetadata::invalidate_statistics`), sends the scan to the Parquet
//! footers instead, which is the common case for a live table.
//!
//! From the metastore's column statistics only what keeps its meaning is used:
//! null counts, integer and date bounds, decimal bounds that a JSON double
//! carries exactly (precision up to 15 digits), and value sizes. Distinct
//! counts are not: appends merge them by maximum, so a table can record one
//! distinct value where it holds two (see the 431 fixture `pilot.t1`).

use std::collections::BTreeMap;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::common::config::TableParquetOptions;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use object_store::ObjectMeta;
use serde_json::Value;

use super::metastore::TableMetadata;
use super::table::HiveTable;
use crate::file_statistics::{fixed_size, footer_statistics};

/// Bytes of an Arrow string or binary offset, per row.
const OFFSET_BYTES: u64 = 4;
/// Largest decimal precision whose every value survives a round trip through
/// the JSON double the metastore stores decimal bounds as.
const EXACT_DOUBLE_DIGITS: u8 = 15;

/// Table statistics as recorded in the metastore when the table was resolved.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct MetastoreStatistics {
    num_files: Option<u64>,
    num_rows: Option<u64>,
    total_size: Option<u64>,
    columns: BTreeMap<String, Value>,
}

fn counter(parameters: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    // Hive records unknown counters as -1, which does not parse as u64.
    parameters.get(key)?.trim().parse().ok()
}

impl MetastoreStatistics {
    pub(crate) fn from_metadata(metadata: &TableMetadata) -> Self {
        Self {
            num_files: counter(&metadata.parameters, "numFiles"),
            num_rows: counter(&metadata.parameters, "numRows"),
            total_size: counter(&metadata.parameters, "totalSize"),
            columns: metadata.column_statistics.clone(),
        }
    }

    /// Statistics for `schema` when the recorded counters match `files`.
    pub(crate) fn describe(&self, schema: &Schema, files: &[ObjectMeta]) -> Option<Statistics> {
        let rows = self.num_rows?;
        let listed_files = u64::try_from(files.len()).ok()?;
        let listed_size = files.iter().try_fold(0_u64, |sum, f| sum.checked_add(f.size))?;
        if self.num_files? != listed_files || self.total_size? != listed_size {
            return None;
        }
        let columns: Vec<ColumnStatistics> = schema
            .fields()
            .iter()
            .map(|field| column_statistics(field, self.column(field.name()), rows))
            .collect();
        let total_byte_size = columns.iter().try_fold(0_usize, |sum, c| match c.byte_size {
            Precision::Inexact(size) => sum.checked_add(size),
            _ => None,
        });
        Some(Statistics {
            num_rows: inexact(Some(rows)),
            total_byte_size: total_byte_size.map_or(Precision::Absent, Precision::Inexact),
            column_statistics: columns,
        })
    }

    fn column(&self, name: &str) -> Option<&Value> {
        self.columns.get(name).or_else(|| {
            self.columns
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value)
        })
    }
}

fn inexact(value: Option<u64>) -> Precision<usize> {
    value
        .and_then(|v| usize::try_from(v).ok())
        .map_or(Precision::Absent, Precision::Inexact)
}

fn column_statistics(field: &Field, stats: Option<&Value>, rows: u64) -> ColumnStatistics {
    let data_type = field.data_type();
    let fixed = fixed_size(data_type, rows);
    let variable = || {
        let values = stats?.get("totalSizeInBytes")?.as_u64()?;
        matches!(data_type, DataType::Utf8 | DataType::Binary)
            .then(|| values.checked_add(rows.checked_mul(OFFSET_BYTES)?))
            .flatten()
    };
    let null_count = stats
        .and_then(|s| s.get("nullsCount")?.as_u64())
        .filter(|nulls| *nulls <= rows);
    let (min, max) = stats
        .and_then(|s| bounds(data_type, s))
        .map_or((Precision::Absent, Precision::Absent), |(min, max)| {
            (Precision::Inexact(min), Precision::Inexact(max))
        });
    ColumnStatistics {
        null_count: inexact(null_count),
        max_value: max,
        min_value: min,
        sum_value: Precision::Absent,
        distinct_count: Precision::Absent,
        byte_size: inexact(fixed.or_else(variable)),
    }
}

/// Recorded bounds as values of `data_type`, when both exist, convert exactly
/// and are ordered.
fn bounds(data_type: &DataType, stats: &Value) -> Option<(ScalarValue, ScalarValue)> {
    let pair = |key: &str| {
        let recorded = stats.get(key)?;
        Some((recorded.get("min")?, recorded.get("max")?))
    };
    let (min, max) = match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let (min, max) = pair("integerStatistics")?;
            (integer(data_type, min)?, integer(data_type, max)?)
        }
        DataType::Date32 => {
            let (min, max) = pair("dateStatistics")?;
            (date(min)?, date(max)?)
        }
        DataType::Decimal128(precision, scale) if *precision <= EXACT_DOUBLE_DIGITS => {
            let (min, max) = pair("decimalStatistics")?;
            (
                decimal(min, *precision, *scale)?,
                decimal(max, *precision, *scale)?,
            )
        }
        _ => return None,
    };
    min.partial_cmp(&max)?.is_le().then_some((min, max))
}

fn integer(data_type: &DataType, value: &Value) -> Option<ScalarValue> {
    let value = value.as_i64()?;
    Some(match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(i8::try_from(value).ok()?)),
        DataType::Int16 => ScalarValue::Int16(Some(i16::try_from(value).ok()?)),
        DataType::Int32 => ScalarValue::Int32(Some(i32::try_from(value).ok()?)),
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        _ => return None,
    })
}

fn date(value: &Value) -> Option<ScalarValue> {
    let text = value.as_str()?;
    // Only the plain ISO form; the cast accepts more than the metastore writes.
    let shape = text.len() == 10
        && text
            .bytes()
            .enumerate()
            .all(|(i, b)| (i == 4 || i == 7) == (b == b'-'));
    if !shape {
        return None;
    }
    let day = ScalarValue::from(text).cast_to(&DataType::Date32).ok()?;
    (!day.is_null()).then_some(day)
}

/// A decimal bound stored as a JSON number. Only a plain literal with no more
/// fractional digits than the column's scale is accepted: its digits are then
/// exactly the decimal the metastore wrote (precision is at most 15).
fn decimal(value: &Value, precision: u8, scale: i8) -> Option<ScalarValue> {
    let number = value.as_number()?;
    let text = number.to_string();
    let fraction = text.split_once('.').map_or(0, |(_, f)| f.len());
    let plain = text.bytes().all(|b| b.is_ascii_digit() || b == b'-' || b == b'.');
    if !plain || fraction > usize::try_from(scale).ok()? {
        return None;
    }
    let decimal = ScalarValue::from(text.as_str())
        .cast_to(&DataType::Decimal128(precision, scale))
        .ok()?;
    (!decimal.is_null()).then_some(decimal)
}

/// Statistics for a scan of `table` over the listed `files`: the metastore's
/// when they still match the files, the footers' otherwise, none when the
/// session turned statistics off (`datafusion.execution.collect_statistics`).
pub(crate) async fn scan_statistics(
    table: &HiveTable,
    state: &dyn Session,
    files: &[ObjectMeta],
) -> Statistics {
    if !state.config_options().execution.collect_statistics {
        return Statistics::new_unknown(&table.schema);
    }
    if let Some(recorded) = table.statistics.describe(&table.schema, files) {
        return recorded;
    }
    let size_hint = TableParquetOptions::default().global.metadata_size_hint;
    footer_statistics(
        table.metastore.store(),
        files,
        &table.schema,
        state.runtime_env(),
        size_hint,
        &format!("{}.{}", table.schema_name, table.table),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::catalog::TableProvider;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use super::super::metastore::FileMetastore;
    use super::super::types::arrow_type;

    const FIXTURE: &str = include_str!("../../tests/fixtures/trino431/warehouse/pilot/t1/.trinoSchema");

    fn fixture_metadata() -> TableMetadata {
        serde_json::from_str(FIXTURE).unwrap()
    }

    fn table_schema(metadata: &TableMetadata) -> Schema {
        Schema::new(
            metadata
                .data_columns
                .iter()
                .map(|c| Field::new(&c.name, arrow_type(&c.hive_type).unwrap(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[allow(
        clippy::default_trait_access,
        reason = "chrono is not a direct dependency of this crate"
    )]
    fn listed(sizes: &[u64]) -> Vec<ObjectMeta> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, size)| ObjectMeta {
                location: Path::from(format!("pilot/t1/f{i}")),
                last_modified: Default::default(),
                size: *size,
                e_tag: None,
                version: None,
            })
            .collect()
    }

    #[test]
    fn uses_the_metastore_while_it_matches_the_files() {
        let metadata = fixture_metadata();
        let schema = table_schema(&metadata);
        let recorded = MetastoreStatistics::from_metadata(&metadata);
        let stats = recorded.describe(&schema, &listed(&[1535, 1503])).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(2));
        let column = |name: &str| &stats.column_statistics[schema.index_of(name).unwrap()];

        let id = column("id");
        assert_eq!(id.min_value, Precision::Inexact(ScalarValue::Int64(Some(1))));
        assert_eq!(id.max_value, Precision::Inexact(ScalarValue::Int64(Some(2))));
        assert_eq!(id.null_count, Precision::Inexact(0));
        assert_eq!(id.byte_size, Precision::Inexact(16));
        // Recorded as 1 for two distinct values: never trusted.
        assert_eq!(id.distinct_count, Precision::Absent);
        assert_eq!(
            column("i").min_value,
            Precision::Inexact(ScalarValue::Int32(Some(7)))
        );
        assert_eq!(
            column("d").max_value,
            Precision::Inexact(ScalarValue::Date32(Some(20_456)))
        );
        assert_eq!(
            column("valor").min_value,
            Precision::Absent,
            "decimal(18,2) does not survive a JSON double"
        );
        assert_eq!(column("dbl").min_value, Precision::Absent);
        assert_eq!(column("ts").min_value, Precision::Absent);
        assert_eq!(column("nome").byte_size, Precision::Inexact(7 + 8));
        assert_eq!(column("lsn").byte_size, Precision::Inexact(20 + 8));
        assert!(matches!(stats.total_byte_size, Precision::Inexact(n) if n > 0));
    }

    #[test]
    fn stale_or_missing_counters_are_ignored() {
        let metadata = fixture_metadata();
        let schema = table_schema(&metadata);
        let recorded = MetastoreStatistics::from_metadata(&metadata);
        assert_eq!(recorded.describe(&schema, &listed(&[1535])), None, "file count");
        assert_eq!(
            recorded.describe(&schema, &listed(&[1535, 1504])),
            None,
            "total size"
        );
        assert_eq!(
            recorded.describe(&schema, &listed(&[1535, 1503, 0])),
            None,
            "an extra file"
        );

        let mut invalidated = metadata.clone();
        invalidated.invalidate_statistics();
        let recorded = MetastoreStatistics::from_metadata(&invalidated);
        assert_eq!(recorded.describe(&schema, &listed(&[1535, 1503])), None);

        let mut unknown = metadata;
        unknown.parameters.insert("numRows".into(), "-1".into());
        let recorded = MetastoreStatistics::from_metadata(&unknown);
        assert_eq!(recorded.describe(&schema, &listed(&[1535, 1503])), None);
    }

    #[test]
    fn column_bounds_convert_exactly_or_not_at_all() {
        let stats = |json: &str| -> Value { serde_json::from_str(json).unwrap() };
        let ints = stats(r#"{"integerStatistics":{"min":-5,"max":300}}"#);
        assert_eq!(
            bounds(&DataType::Int16, &ints),
            Some((ScalarValue::Int16(Some(-5)), ScalarValue::Int16(Some(300))))
        );
        assert_eq!(bounds(&DataType::Int8, &ints), None, "300 is not a tinyint");
        let reversed = stats(r#"{"integerStatistics":{"min":9,"max":1}}"#);
        assert_eq!(bounds(&DataType::Int64, &reversed), None);
        let half = stats(r#"{"integerStatistics":{"min":1}}"#);
        assert_eq!(bounds(&DataType::Int64, &half), None);

        let decimals = stats(r#"{"decimalStatistics":{"min":-1.5,"max":1234567890123.25}}"#);
        assert_eq!(
            bounds(&DataType::Decimal128(15, 2), &decimals),
            Some((
                ScalarValue::Decimal128(Some(-150), 15, 2),
                ScalarValue::Decimal128(Some(123_456_789_012_325), 15, 2)
            ))
        );
        assert_eq!(bounds(&DataType::Decimal128(15, 1), &decimals), None, "scale");
        assert_eq!(bounds(&DataType::Decimal128(16, 2), &decimals), None, "precision");
        let huge = stats(r#"{"decimalStatistics":{"min":1e20,"max":2e20}}"#);
        assert_eq!(bounds(&DataType::Decimal128(15, 0), &huge), None);

        let dates = stats(r#"{"dateStatistics":{"min":"1970-01-02","max":"2026-01-03"}}"#);
        assert_eq!(
            bounds(&DataType::Date32, &dates),
            Some((ScalarValue::Date32(Some(1)), ScalarValue::Date32(Some(20_456))))
        );
        let odd = stats(r#"{"dateStatistics":{"min":"+1970-01-02","max":"2026-01-03"}}"#);
        assert_eq!(bounds(&DataType::Date32, &odd), None);

        let doubles = stats(r#"{"doubleStatistics":{"min":1.0,"max":2.0}}"#);
        assert_eq!(bounds(&DataType::Float64, &doubles), None);
        assert_eq!(
            bounds(&DataType::Timestamp(TimeUnit::Millisecond, None), &doubles),
            None
        );
    }

    #[test]
    fn null_counts_above_the_row_count_are_ignored() {
        let field = Field::new("x", DataType::Int64, true);
        let stats: Value = serde_json::from_str(r#"{"nullsCount":5}"#).unwrap();
        assert_eq!(
            column_statistics(&field, Some(&stats), 4).null_count,
            Precision::Absent
        );
        assert_eq!(
            column_statistics(&field, Some(&stats), 5).null_count,
            Precision::Inexact(5)
        );
        let text = Field::new("s", DataType::Utf8, true);
        assert_eq!(column_statistics(&text, None, 5).byte_size, Precision::Absent);
    }

    /// The scan of a table whose metastore statistics are gone (after an
    /// `INSERT`) still reports rows and bounds, from the footers.
    #[tokio::test]
    async fn the_scan_carries_footer_statistics_when_the_metastore_has_none() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/trino431/warehouse");
        let metastore = FileMetastore::new(
            Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()),
            Path::default(),
        );
        let mut metadata = fixture_metadata();
        let schema = Arc::new(table_schema(&metadata));
        let ctx = SessionContext::new();
        let off = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new()
                .set_bool("datafusion.execution.collect_statistics", false),
        );
        let columns = metadata.data_columns.clone();
        let scan_stats = |statistics: MetastoreStatistics, ctx: &SessionContext| {
            let table = HiveTable {
                metastore: metastore.clone(),
                store_url: ObjectStoreUrl::parse("codelake://hive").unwrap(),
                schema_name: "pilot".into(),
                table: "t1".into(),
                schema: Arc::clone(&schema),
                columns: columns.clone(),
                statistics: Arc::new(statistics),
                table_id: None,
            };
            let state = ctx.state();
            async move {
                let plan = table.scan(&state, None, &[], None).await.unwrap();
                Arc::unwrap_or_clone(plan.partition_statistics(None).unwrap())
            }
        };

        let recorded = scan_stats(MetastoreStatistics::from_metadata(&metadata), &ctx).await;
        let disabled = scan_stats(MetastoreStatistics::from_metadata(&metadata), &off).await;
        assert_eq!(
            disabled,
            Statistics::new_unknown(&schema),
            "the session turned them off"
        );
        metadata.invalidate_statistics();
        let footers = scan_stats(MetastoreStatistics::from_metadata(&metadata), &ctx).await;

        for stats in [&recorded, &footers] {
            assert_eq!(stats.num_rows, Precision::Inexact(2));
            let id = &stats.column_statistics[0];
            assert_eq!(id.min_value, Precision::Inexact(ScalarValue::Int64(Some(1))));
            assert_eq!(id.max_value, Precision::Inexact(ScalarValue::Int64(Some(2))));
        }
        // INT96 timestamps: undefined order, no bounds from the footers.
        let ts = &footers.column_statistics[schema.index_of("ts").unwrap()];
        assert_eq!(ts.min_value, Precision::Absent);
    }
}
