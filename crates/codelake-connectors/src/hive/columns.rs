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
//! A `raw` adapter (compaction) keeps stored values as they are.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;

use super::table::{finer, round_timestamps};
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
            Read::Cut(_) | Read::Pad(_) => DataType::Utf8,
        };
        Ok(Arc::new(child.as_ref().clone().with_data_type(data_type)))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let read = match &self.read {
            Read::Round(unit, zone) => round_timestamps(&values, *unit, zone.clone())?,
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
            let Some(index) = stored else {
                fields.push(field.as_ref().clone());
                refused.push(None);
                reads.push(None);
                continue;
            };
            let stored = physical_file_schema.field(index);
            let read = match (stored.data_type(), field.data_type()) {
                _ if self.raw => None,
                (DataType::Timestamp(from, _), DataType::Timestamp(to, zone)) if finer(*from, *to) => {
                    Some(Read::Round(*to, zone.clone()))
                }
                _ => declared_length(field)
                    .map(Read::Cut)
                    .or_else(|| char_column(field).map(Read::Pad)),
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
            // A rounded column reaches the adapter below in its stored type,
            // so it does not cast (cut) it first.
            let spelled = field.as_ref().clone().with_name(stored.name());
            fields.push(match read {
                Some(Read::Round(..)) => spelled.with_data_type(stored.data_type().clone()),
                _ => spelled,
            });
            reads.push(read);
        }
        let renamed = Arc::new(Schema::new_with_metadata(
            fields,
            logical_file_schema.metadata().clone(),
        ));
        Ok(Arc::new(HiveColumnAdapter {
            refused,
            reads,
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
    /// How each table column's stored values become the table's.
    reads: Vec<Option<Read>>,
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
                if let Some(Some(reason)) = self.refused.get(index) {
                    return Err(DataFusionError::Execution(reason.clone()));
                }
                let read = self.reads.get(index).cloned().flatten();
                if renamed.name() == column.name() && read.is_none() {
                    return Ok(Transformed::no(expr));
                }
                let respelled: Arc<dyn PhysicalExpr> = Arc::new(Column::new(renamed.name(), index));
                Ok(Transformed::yes(match read {
                    Some(read) => Arc::new(HiveRead {
                        child: respelled,
                        read,
                    }),
                    None => respelled,
                }))
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
