//! Trino semantics on table columns, through `Engine::sql`: the physical
//! paths (`try`, overflow checks, NaN comparisons, grouped aggregates) that the
//! golden cases, mostly constants, cannot reach once the simplifier folds them.
//! Every test checks the rows returned.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;

use super::golden::engine;
use super::trino_error_name;
use crate::engine::Engine;

/// Registers `t` as `partitions` batches of the given columns.
fn register(engine: &Engine, columns: &[(&str, ArrayRef)], partitions: usize) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let rows = columns.first().map_or(0, |(_, a)| a.len());
    let per = rows.div_ceil(partitions).max(1);
    let batches: Vec<Vec<RecordBatch>> = (0..rows)
        .step_by(per)
        .map(|start| {
            let len = per.min(rows - start);
            let arrays = columns.iter().map(|(_, a)| a.slice(start, len)).collect();
            vec![RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap()]
        })
        .collect();
    let table = MemTable::try_new(schema, batches).unwrap();
    engine.context().register_table("t", Arc::new(table)).unwrap();
}

async fn rows(engine: &Engine, sql: &str) -> Vec<RecordBatch> {
    engine.sql(sql).await.unwrap().collect().await.unwrap()
}

async fn error_name(engine: &Engine, sql: &str) -> &'static str {
    let error = match engine.sql(sql).await {
        Ok(frame) => frame.collect().await.expect_err("the query must fail"),
        Err(error) => error,
    };
    trino_error_name(&error)
}

fn int64s(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..a.len()).map(move |i| (!a.is_null(i)).then(|| a.value(i)))
        })
        .collect()
}

#[tokio::test]
async fn nan_rows_fail_every_comparison_but_not_equal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let x: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(f64::NAN),
        Some(2.0),
        Some(0.5),
        None,
    ]));
    register(&engine, &[("x", x)], 2);
    let out = rows(
        &engine,
        "SELECT count_if(x > 1E0), count_if(x < 1E0), count_if(x = x), count_if(x <> x), count(*) FROM t",
    )
    .await;
    let counts: Vec<i64> = (0..5).map(|c| int64s(&out, c)[0].unwrap()).collect();
    // NaN > 1, NaN < 1 and NaN = NaN are false; NaN <> NaN is true; NULL is unknown.
    assert_eq!(counts, vec![1, 1, 2, 1, 4]);
    let filtered = rows(&engine, "SELECT count(*) FROM t WHERE x > 1E0").await;
    assert_eq!(int64s(&filtered, 0), vec![Some(1)]);
}

#[tokio::test]
async fn try_turns_only_the_failing_rows_into_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    // Three thousand rows over several batches; every hundredth divisor is 0.
    let divisors: ArrayRef = Arc::new(Int64Array::from_iter_values((0..3_000).map(|i| i % 100)));
    let texts: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..3_000).map(|i| if i % 7 == 0 { "x".to_owned() } else { i.to_string() }),
    ));
    register(&engine, &[("d", divisors), ("s", texts)], 3);
    let out = rows(
        &engine,
        "SELECT count(*) - count(try(100 / d)), sum(try(100 / d)), \
         count(*) - count(try(CAST(s AS bigint))), count(coalesce(try(CAST(s AS bigint)), -1)) FROM t",
    )
    .await;
    assert_eq!(
        int64s(&out, 0),
        vec![Some(30)],
        "one zero divisor per hundred rows"
    );
    let expected_sum: i64 = (0..3_000_i64)
        .filter(|i| i % 100 != 0)
        .map(|i| 100 / (i % 100))
        .sum();
    assert_eq!(int64s(&out, 1), vec![Some(expected_sum)]);
    assert_eq!(
        int64s(&out, 2),
        vec![Some(429)],
        "every seventh text is not a number"
    );
    assert_eq!(int64s(&out, 3), vec![Some(3_000)]);
    // Without `try` the same expression fails the query.
    assert_eq!(
        error_name(&engine, "SELECT 100 / d FROM t").await,
        "DIVISION_BY_ZERO"
    );
}

#[tokio::test]
async fn bigint_divided_by_a_decimal_literal_keeps_trino_scale() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    // The reference sensors: date_diff('second', …) / 3600.0.
    let seconds: ArrayRef = Arc::new(Int64Array::from(vec![5_400, 100, 179, 180, -180, 0]));
    register(&engine, &[("s", seconds)], 1);
    let frame = engine.sql("SELECT s / 3600.0 FROM t").await.unwrap();
    assert_eq!(frame.schema().field(0).data_type(), &DataType::Decimal128(21, 1));
    let out = frame.collect().await.unwrap();
    let values: Vec<String> = out
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .clone();
            (0..a.len()).map(move |i| a.value_as_string(i))
        })
        .collect();
    // Half up to one fractional digit: 179 s is 0.0497 h → 0.0, 180 s → 0.1.
    assert_eq!(values, vec!["1.5", "0.0", "0.0", "0.1", "-0.1", "0.0"]);
}

/// `avg` of a decimal keeps its type and rounds half up, away from zero
/// (the cases of the library's former decimal `avg` accumulator).
#[tokio::test]
async fn decimal_avg_rounds_half_up() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![10, 11, -10, -11])
            .with_precision_and_scale(5, 1)
            .unwrap(),
    );
    let side: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, -1, -1]));
    register(&engine, &[("v", values), ("side", side)], 2);
    let frame = engine
        .sql("SELECT avg(v) FROM t GROUP BY side ORDER BY side DESC")
        .await
        .unwrap();
    assert_eq!(frame.schema().field(0).data_type(), &DataType::Decimal128(5, 1));
    let out = frame.collect().await.unwrap();
    let averages: Vec<String> = out
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .clone();
            (0..a.len()).map(move |i| a.value_as_string(i))
        })
        .collect();
    assert_eq!(averages, vec!["1.1", "-1.1"]);
}

#[tokio::test]
async fn integer_overflow_fails_instead_of_wrapping() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let a: ArrayRef = Arc::new(Int32Array::from(vec![100_000, 3]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![100_000, 4]));
    let big: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 1]));
    let g: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
    register(&engine, &[("a", a), ("b", b), ("big", big), ("g", g)], 2);
    // integer * integer is integer in Trino: 10^10 does not fit.
    assert_eq!(
        error_name(&engine, "SELECT a * b FROM t").await,
        "NUMERIC_VALUE_OUT_OF_RANGE"
    );
    assert_eq!(
        error_name(&engine, "SELECT g, sum(big) FROM t GROUP BY g").await,
        "NUMERIC_VALUE_OUT_OF_RANGE"
    );
    let fits = rows(&engine, "SELECT sum(CAST(a AS bigint) * b) FROM t").await;
    assert_eq!(int64s(&fits, 0), vec![Some(10_000_000_012)]);
}

#[tokio::test]
async fn grouped_min_and_max_skip_nan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let g: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2]));
    let x: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, 1.0, -1.0, f64::NAN, 5.0]));
    register(&engine, &[("g", g), ("x", x)], 2);
    let out = rows(&engine, "SELECT g, min(x), max(x) FROM t GROUP BY g ORDER BY g").await;
    let floats = |c: usize| -> Vec<f64> {
        out.iter()
            .flat_map(|b| {
                let a = b
                    .column(c)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone();
                (0..a.len()).map(move |i| a.value(i))
            })
            .collect()
    };
    assert_eq!(
        floats(1).iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        [-1.0_f64, 5.0].map(f64::to_bits)
    );
    assert_eq!(
        floats(2).iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        [1.0_f64, 5.0].map(f64::to_bits)
    );
}

#[tokio::test]
async fn json_values_read_from_columns_cast_as_trino() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let docs: ArrayRef = Arc::new(StringArray::from(vec![
        r#"{"v": "7", "n": 1.5}"#,
        r#"{"v": 8, "n": null}"#,
    ]));
    register(&engine, &[("doc", docs)], 1);
    let out = rows(
        &engine,
        "SELECT CAST(json_extract(doc, '$.v') AS varchar), CAST(json_extract(doc, '$.v') AS bigint), \
         CAST(json_extract(doc, '$.n') AS varchar) FROM t",
    )
    .await;
    let texts = |c: usize| -> Vec<Option<String>> {
        out.iter()
            .flat_map(|b| {
                let a = datafusion::arrow::compute::cast(b.column(c), &DataType::Utf8).unwrap();
                let a = a.as_any().downcast_ref::<StringArray>().unwrap().clone();
                (0..a.len()).map(move |i| (!a.is_null(i)).then(|| a.value(i).to_owned()))
            })
            .collect()
    };
    // A JSON string cast to varchar is its text, not the quoted token.
    assert_eq!(texts(0), vec![Some("7".to_owned()), Some("8".to_owned())]);
    assert_eq!(int64s(&out, 1), vec![Some(7), Some(8)]);
    assert_eq!(texts(2), vec![Some("1.5E0".to_owned()), None]);
}
