//! IEEE comparisons of `double`/`real` values, as Trino evaluates them.
//!
//! `DataFusion` compares floats with a total order: `nan() = nan()` is true
//! and `nan() > 1` is true, so `WHERE x > 1` keeps NaN rows. In Trino (and
//! IEEE 754) every comparison with NaN is false, except `<>`, which is true.
//! [`NanAwareComparison`] wraps a float comparison and corrects the rows where
//! an operand is NaN; the physical rule in `try_expr` installs it.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Float32Array, Float64Array};
use datafusion::arrow::compute::kernels::boolean::{and, or};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::BinaryExpr;

/// True for a comparison of two floats.
pub(crate) fn is_float_comparison(binary: &BinaryExpr, schema: &Schema) -> bool {
    let float = |e: &Arc<dyn PhysicalExpr>| {
        e.data_type(schema)
            .is_ok_and(|t| matches!(t, DataType::Float32 | DataType::Float64))
    };
    matches!(
        binary.op(),
        Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) && float(binary.left())
        && float(binary.right())
}

/// Rows where the value is NaN (false for NULL).
pub(crate) fn nan_mask(array: &ArrayRef) -> Result<BooleanArray> {
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(values.iter().map(|v| Some(v.is_some_and(f64::is_nan))).collect());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(values.iter().map(|v| Some(v.is_some_and(f32::is_nan))).collect());
    }
    Err(DataFusionError::Internal(format!(
        "not a float: {}",
        array.data_type()
    )))
}

/// A float comparison with NaN handled as in IEEE 754.
#[derive(Debug, Clone)]
pub(crate) struct NanAwareComparison {
    inner: Arc<dyn PhysicalExpr>,
    left: Arc<dyn PhysicalExpr>,
    right: Arc<dyn PhysicalExpr>,
    op: Operator,
}

impl NanAwareComparison {
    pub(crate) fn new(binary: &BinaryExpr) -> Self {
        Self {
            inner: Arc::new(binary.clone()),
            left: Arc::clone(binary.left()),
            right: Arc::clone(binary.right()),
            op: *binary.op(),
        }
    }
}

impl PartialEq for NanAwareComparison {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for NanAwareComparison {}

impl std::hash::Hash for NanAwareComparison {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl fmt::Display for NanAwareComparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ieee({})", self.inner)
    }
}

impl PhysicalExpr for NanAwareComparison {
    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let rows = batch.num_rows();
        let result = self.inner.evaluate(batch)?.into_array(rows)?;
        let (left, right) = (
            self.left.evaluate(batch)?.into_array(rows)?,
            self.right.evaluate(batch)?.into_array(rows)?,
        );
        // Only rows where both sides are present: NaN against NULL stays NULL.
        let both = BooleanArray::from_iter((0..rows).map(|i| Some(left.is_valid(i) && right.is_valid(i))));
        let nan = and(&or(&nan_mask(&left)?, &nan_mask(&right)?)?, &both)?;
        if nan.true_count() == 0 {
            return Ok(ColumnarValue::Array(result));
        }
        let replacement = ScalarValue::Boolean(Some(self.op == Operator::NotEq)).to_scalar()?;
        Ok(ColumnarValue::Array(zip(&nan, &replacement, &result)?))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [left, right]: [Arc<dyn PhysicalExpr>; 2] = children
            .try_into()
            .map_err(|_| DataFusionError::Internal("comparison needs two children".into()))?;
        let binary = BinaryExpr::new(left, self.op, right);
        Ok(Arc::new(Self::new(&binary)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_sql(f)
    }
}

/// Name of the constant-folding IEEE comparison for `op` (see `rewrite`).
pub(crate) fn ieee_name(op: Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Eq => "codelake_trino_ieee_eq",
        Operator::NotEq => "codelake_trino_ieee_ne",
        Operator::Lt => "codelake_trino_ieee_lt",
        Operator::LtEq => "codelake_trino_ieee_le",
        Operator::Gt => "codelake_trino_ieee_gt",
        Operator::GtEq => "codelake_trino_ieee_ge",
        _ => return None,
    })
}

fn ieee(call: &super::support::Call<'_>, f: fn(f64, f64) -> bool) -> Result<ArrayRef> {
    use super::support::At;
    let (a, b) = (
        super::support::floats(call.arg(0)?)?,
        super::support::floats(call.arg(1)?)?,
    );
    Ok(super::support::bool_array(
        (0..call.rows).map(|i| Some(f(a.at(i)?, b.at(i)?))).collect(),
    ))
}

fn ieee_function(
    name: &'static str,
    invoke: super::support::InvokeFn,
) -> datafusion::logical_expr::ScalarUDF {
    use super::support::{BOOLEAN, Func, as_f64, coerce_each};
    Func::new(name, |n, t| coerce_each(n, t, &[as_f64, as_f64]), BOOLEAN, invoke).udf()
}

/// A float join key: `-0.0` is `0.0` and NaN matches nothing (NULL), as
/// Trino's `=`.
pub(crate) const FLOAT_KEY: &str = "codelake_trino_float_key";
/// A float grouping key: `-0.0` is `0.0`, every NaN one NaN (Trino's
/// `IS DISTINCT FROM`, used by `GROUP BY`, `DISTINCT` and set operations).
pub(crate) const FLOAT_GROUP_KEY: &str = "codelake_trino_float_group_key";

fn canonical_floats(array: &ArrayRef, nan: Option<f64>) -> Result<ArrayRef> {
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        let out: Float64Array = values
            .iter()
            .map(|v| {
                v.and_then(|v| {
                    if v.is_nan() {
                        nan
                    } else if v == 0.0 {
                        Some(0.0)
                    } else {
                        Some(v)
                    }
                })
            })
            .collect();
        return Ok(Arc::new(out));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        let nan = nan.map(|_| f32::NAN);
        let out: Float32Array = values
            .iter()
            .map(|v| {
                v.and_then(|v| {
                    if v.is_nan() {
                        nan
                    } else if v == 0.0 {
                        Some(0.0)
                    } else {
                        Some(v)
                    }
                })
            })
            .collect();
        return Ok(Arc::new(out));
    }
    Err(DataFusionError::Internal(format!(
        "not a float: {}",
        array.data_type()
    )))
}

fn key_function(name: &'static str, invoke: super::support::InvokeFn) -> datafusion::logical_expr::ScalarUDF {
    use super::support::{Func, not_found};
    Func::new(
        name,
        |name, types| match types {
            [t @ (DataType::Float32 | DataType::Float64)] => Ok(vec![t.clone()]),
            _ => Err(not_found(name, types)),
        },
        |r| {
            r.types
                .first()
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("float key".into()))
        },
        invoke,
    )
    .udf()
}

/// IEEE comparisons of constants, which the simplifier would otherwise fold
/// with the engine's total order; float key normalizations.
pub(crate) fn udfs() -> Vec<datafusion::logical_expr::ScalarUDF> {
    vec![
        key_function(FLOAT_KEY, |c| canonical_floats(c.arg(0)?, None)),
        key_function(FLOAT_GROUP_KEY, |c| canonical_floats(c.arg(0)?, Some(f64::NAN))),
        ieee_function("codelake_trino_ieee_eq", |c| {
            ieee(c, |a, b| a.partial_cmp(&b) == Some(std::cmp::Ordering::Equal))
        }),
        ieee_function("codelake_trino_ieee_ne", |c| {
            ieee(c, |a, b| a.partial_cmp(&b) != Some(std::cmp::Ordering::Equal))
        }),
        ieee_function("codelake_trino_ieee_lt", |c| ieee(c, |a, b| a < b)),
        ieee_function("codelake_trino_ieee_le", |c| ieee(c, |a, b| a <= b)),
        ieee_function("codelake_trino_ieee_gt", |c| ieee(c, |a, b| a > b)),
        ieee_function("codelake_trino_ieee_ge", |c| ieee(c, |a, b| a >= b)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::{Column, lit};

    fn compare(op: Operator, values: Vec<Option<f64>>) -> Vec<Option<bool>> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Float64Array::from(values))]).unwrap();
        let binary = BinaryExpr::new(Arc::new(Column::new("x", 0)), op, lit(1.0_f64));
        assert!(is_float_comparison(&binary, &schema));
        let out = NanAwareComparison::new(&binary)
            .evaluate(&batch)
            .unwrap()
            .into_array(3)
            .unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        out.iter().collect()
    }

    #[test]
    fn nan_compares_false_except_not_equal() {
        let values = vec![Some(f64::NAN), Some(2.0), None];
        assert_eq!(
            compare(Operator::Gt, values.clone()),
            vec![Some(false), Some(true), None]
        );
        assert_eq!(
            compare(Operator::Eq, values.clone()),
            vec![Some(false), Some(false), None]
        );
        assert_eq!(
            compare(Operator::NotEq, values),
            vec![Some(true), Some(true), None]
        );
    }
}
