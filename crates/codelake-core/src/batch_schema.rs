//! A batch under the schema its stream announces.
//!
//! The branches of a `UNION ALL` keep their own field metadata and
//! nullability: a literal branch is not nullable, a column of another source
//! is, and two sources record their own type names (`numeric` in one branch,
//! `decimal` in another). The union announces one schema for all of them, so
//! a consumer that puts its batches under an announced schema cannot demand
//! that each batch's schema be contained in it. [`announced`] accepts the
//! batch when only nullability (a non-null column fits a nullable field) and
//! field metadata differ, and refuses any other difference: a type, a number
//! of columns, or NULLs in a column the schema declares non-null.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::error::DataFusionError;

/// `batch` under `schema` (see the module documentation).
pub(crate) fn announced(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, DataFusionError> {
    if schema.contains(batch.schema_ref()) {
        return Ok(batch.with_schema(Arc::clone(schema))?);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "a batch of {} columns under a schema of {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| column_as(column, field))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// `column` with the type of `field`, when the two differ only in the
/// nullability and metadata of nested fields.
fn column_as(column: &ArrayRef, field: &Field) -> Result<ArrayRef, DataFusionError> {
    if column.data_type() == field.data_type() {
        return Ok(Arc::clone(column));
    }
    if !holds(field.data_type(), column.data_type()) {
        return Err(DataFusionError::Internal(format!(
            "column {} is {} under a schema of {}",
            field.name(),
            column.data_type(),
            field.data_type()
        )));
    }
    Ok(cast(column, field.data_type())?)
}

/// Whether values of type `actual` are values of type `target`: the same
/// type, where a nested field of `target` may be nullable where `actual`'s is
/// not and carry other metadata.
fn holds(target: &DataType, actual: &DataType) -> bool {
    let field = |target: &Field, actual: &Field| {
        target.name() == actual.name()
            && (target.is_nullable() || !actual.is_nullable())
            && holds(target.data_type(), actual.data_type())
    };
    match (target, actual) {
        (DataType::List(target), DataType::List(actual))
        | (DataType::LargeList(target), DataType::LargeList(actual)) => field(target, actual),
        (DataType::FixedSizeList(target, n), DataType::FixedSizeList(actual, m)) => {
            n == m && field(target, actual)
        }
        (DataType::Map(target, sorted), DataType::Map(actual, also)) => {
            sorted == also && field(target, actual)
        }
        (DataType::Struct(target), DataType::Struct(actual)) => {
            target.len() == actual.len() && target.iter().zip(actual.iter()).all(|(t, a)| field(t, a))
        }
        _ => target == actual,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::array::{Array, Decimal128Array, Int32Array, ListArray, StringArray, StructArray};
    use datafusion::arrow::datatypes::{Fields, Int32Type, Schema};

    use super::*;

    fn tagged(field: Field, key: &str, value: &str) -> Field {
        field.with_metadata(HashMap::from([(key.to_owned(), value.to_owned())]))
    }

    fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn decimals(values: &[Option<i128>]) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values.to_vec())
                .with_precision_and_scale(18, 6)
                .unwrap(),
        )
    }

    #[test]
    fn other_metadata_and_a_non_null_column_fit_a_nullable_field() {
        let decimal = DataType::Decimal128(18, 6);
        let schema = Arc::new(Schema::new(vec![
            Field::new("tabela", DataType::Utf8, true),
            tagged(
                Field::new("valor", decimal.clone(), true),
                "source.type",
                "numeric",
            ),
        ]));
        let rows = batch(
            vec![
                Field::new("tabela", DataType::Utf8, false),
                tagged(Field::new("valor", decimal, true), "source.type", "decimal"),
            ],
            vec![
                Arc::new(StringArray::from(vec!["vendas", "compras"])),
                decimals(&[Some(1_500_000), None]),
            ],
        );
        assert!(rows.clone().with_schema(Arc::clone(&schema)).is_err());
        let fitted = announced(rows.clone(), &schema).unwrap();
        assert_eq!(fitted.schema(), schema);
        assert_eq!(fitted.columns(), rows.columns());
    }

    #[test]
    fn a_contained_schema_is_taken_as_it_is() {
        let schema = Arc::new(Schema::new(vec![tagged(
            Field::new("n", DataType::Int32, true),
            "source.type",
            "int",
        )]));
        let rows = batch(
            vec![Field::new("n", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );
        let fitted = announced(rows, &schema).unwrap();
        assert_eq!(fitted.schema(), schema);
        assert_eq!(fitted.num_rows(), 2);
    }

    #[test]
    fn nested_fields_widen_to_nullable() {
        let strict = Fields::from(vec![tagged(Field::new("a", DataType::Int32, false), "k", "w")]);
        let loose = Fields::from(vec![tagged(Field::new("a", DataType::Int32, true), "k", "v")]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("r", DataType::Struct(loose), true),
            Field::new("l", DataType::new_list(DataType::Int32, true), true),
        ]));
        let row: ArrayRef = Arc::new(StructArray::new(
            strict.clone(),
            vec![Arc::new(Int32Array::from(vec![7]))],
            None,
        ));
        let list: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(
            vec![Some(1)],
        )]));
        let list = cast(&list, &DataType::new_list(DataType::Int32, false)).unwrap();
        let rows = batch(
            vec![
                Field::new("r", DataType::Struct(strict), false),
                Field::new("l", list.data_type().clone(), true),
            ],
            vec![row, list],
        );
        let fitted = announced(rows, &schema).unwrap();
        assert_eq!(fitted.schema(), schema);
        assert_eq!(fitted.column(0).data_type(), schema.field(0).data_type());
        assert_eq!(fitted.column(1).data_type(), schema.field(1).data_type());
    }

    #[test]
    fn a_different_type_is_refused() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let rows = batch(
            vec![Field::new("n", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        let error = announced(rows, &schema).unwrap_err().to_string();
        assert!(error.contains("column n is Int32"), "{error}");
    }

    #[test]
    fn nulls_in_a_non_null_field_are_refused() {
        let schema = Arc::new(Schema::new(vec![tagged(
            Field::new("n", DataType::Int32, false),
            "k",
            "v",
        )]));
        let rows = batch(
            vec![Field::new("n", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        );
        let error = announced(rows, &schema).unwrap_err().to_string();
        assert!(error.contains("non-nullable"), "{error}");
    }

    #[test]
    fn another_number_of_columns_is_refused() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            tagged(Field::new("b", DataType::Int32, true), "k", "v"),
        ]));
        let rows = batch(
            vec![Field::new("a", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );
        let error = announced(rows, &schema).unwrap_err().to_string();
        assert!(error.contains("1 columns under a schema of 2"), "{error}");
    }

    #[test]
    fn nested_types_hold_only_widened_fields() {
        let int = |nullable| Arc::new(Field::new("a", DataType::Int32, nullable));
        let list = |nullable| DataType::List(int(nullable));
        assert!(holds(&list(true), &list(false)));
        assert!(!holds(&list(false), &list(true)));
        assert!(holds(
            &DataType::LargeList(int(true)),
            &DataType::LargeList(int(false))
        ));
        assert!(!holds(&DataType::LargeList(int(true)), &list(false)));
        assert!(holds(
            &DataType::FixedSizeList(int(true), 2),
            &DataType::FixedSizeList(int(false), 2)
        ));
        assert!(!holds(
            &DataType::FixedSizeList(int(true), 2),
            &DataType::FixedSizeList(int(true), 3)
        ));
        let other_name = DataType::List(Arc::new(Field::new("b", DataType::Int32, false)));
        assert!(!holds(&list(true), &other_name));
        let other_type = DataType::List(Arc::new(Field::new("a", DataType::Int64, false)));
        assert!(!holds(&list(true), &other_type));
        let entries = |nullable| {
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, nullable),
                ])),
                false,
            ))
        };
        assert!(holds(
            &DataType::Map(entries(true), false),
            &DataType::Map(entries(false), false)
        ));
        assert!(!holds(
            &DataType::Map(entries(true), true),
            &DataType::Map(entries(false), false)
        ));
        assert!(!holds(
            &DataType::Map(entries(false), false),
            &DataType::Map(entries(true), false)
        ));
        let row = |fields: Vec<Field>| DataType::Struct(Fields::from(fields));
        assert!(!holds(
            &row(vec![Field::new("a", DataType::Int32, true)]),
            &row(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true)
            ])
        ));
        assert!(holds(&DataType::Int32, &DataType::Int32));
        assert!(!holds(&DataType::Int32, &DataType::Int64));
    }
}
