//! IEEE comparisons of `double`/`real` values, as Trino evaluates them.
//!
//! `DataFusion` compares floats with a total order: `nan() = nan()` is true
//! and `nan() > 1` is true, so `WHERE x > 1` keeps NaN rows. In Trino (and
//! IEEE 754) every comparison with NaN is false, except `<>`, which is true.
//! The total order also puts `-0.0` below `0.0`, where IEEE 754, Trino and
//! PostgreSQL have `-0.0 = 0.0` (ISS-031). [`NanAwareComparison`] wraps a
//! float comparison (and `IS [NOT] DISTINCT FROM`) and corrects the rows
//! where an operand is NaN or both are zeros; the physical rule in `try_expr`
//! installs it. Comparisons with a zero constant are rewritten before
//! planning (`zeros`), so scans and the planner see them right too.

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
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) && float(binary.left())
        && float(binary.right())
}

/// Whether the engine's total order answers `binary` as IEEE 754 does:
/// `=` and `<>` with a literal that is neither NaN nor a zero. Left as it
/// is, the planner keeps reading `x = 1.5` in a filter as a constant `x`
/// (a window partitioned by `x` above it is not sorted); wrapped, the
/// operator would no longer say so, and the window would fail.
pub(crate) fn same_in_both_orders(binary: &BinaryExpr) -> bool {
    use datafusion::physical_expr::expressions::Literal;
    let plain = |e: &Arc<dyn PhysicalExpr>| {
        e.downcast_ref::<Literal>()
            .is_some_and(|literal| match literal.value() {
                ScalarValue::Float64(Some(v)) => !v.is_nan() && *v != 0.0,
                ScalarValue::Float32(Some(v)) => !v.is_nan() && *v != 0.0,
                _ => false,
            })
    };
    matches!(binary.op(), Operator::Eq | Operator::NotEq) && (plain(binary.left()) || plain(binary.right()))
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

/// Rows where the value is `0.0` or `-0.0` (false for NULL).
fn zero_mask(array: &ArrayRef) -> Result<BooleanArray> {
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(values.iter().map(|v| Some(v == Some(0.0))).collect());
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(values.iter().map(|v| Some(v == Some(0.0))).collect());
    }
    Err(DataFusionError::Internal(format!(
        "not a float: {}",
        array.data_type()
    )))
}

/// A float comparison with NaN and the zeros handled as in IEEE 754; for `IS
/// [NOT] DISTINCT FROM`, the zeros are one value and so are all NaNs (as
/// `GROUP BY` has them).
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
        // `-0.0` and `0.0` are equal (the total order puts `-0.0` first).
        let zeros = and(&zero_mask(&left)?, &zero_mask(&right)?)?;
        let result = if zeros.true_count() == 0 {
            result
        } else {
            let equal = matches!(
                self.op,
                Operator::Eq | Operator::LtEq | Operator::GtEq | Operator::IsNotDistinctFrom
            );
            zip(&zeros, &ScalarValue::Boolean(Some(equal)).to_scalar()?, &result)?
        };
        let (left_nan, right_nan) = (nan_mask(&left)?, nan_mask(&right)?);
        if matches!(self.op, Operator::IsDistinctFrom | Operator::IsNotDistinctFrom) {
            // Every NaN is one value, whatever its bits.
            let both_nan = and(&left_nan, &right_nan)?;
            if both_nan.true_count() == 0 {
                return Ok(ColumnarValue::Array(result));
            }
            let same = ScalarValue::Boolean(Some(self.op == Operator::IsNotDistinctFrom)).to_scalar()?;
            return Ok(ColumnarValue::Array(zip(&both_nan, &same, &result)?));
        }
        // Only rows where both sides are present: NaN against NULL stays NULL.
        let both = BooleanArray::from_iter((0..rows).map(|i| Some(left.is_valid(i) && right.is_valid(i))));
        let nan = and(&or(&left_nan, &right_nan)?, &both)?;
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

    fn compare_pairs(op: Operator, pairs: &[(Option<f64>, Option<f64>)]) -> Vec<Option<bool>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let (xs, ys): (Vec<_>, Vec<_>) = pairs.iter().copied().unzip();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(xs)), Arc::new(Float64Array::from(ys))],
        )
        .unwrap();
        let binary = BinaryExpr::new(Arc::new(Column::new("x", 0)), op, Arc::new(Column::new("y", 1)));
        assert!(is_float_comparison(&binary, &schema));
        let out = NanAwareComparison::new(&binary)
            .evaluate(&batch)
            .unwrap()
            .into_array(pairs.len())
            .unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        out.iter().collect()
    }

    /// `-0.0 = 0.0` (IEEE 754, PostgreSQL, Trino's comparisons; ISS-031).
    #[test]
    fn the_two_zeros_are_one_value() {
        let pairs = [
            (Some(-0.0), Some(0.0)),
            (Some(0.0), Some(-0.0)),
            (Some(-0.0), None),
        ];
        for (op, expected) in [
            (Operator::Eq, [Some(true), Some(true), None]),
            (Operator::NotEq, [Some(false), Some(false), None]),
            (Operator::Lt, [Some(false), Some(false), None]),
            (Operator::LtEq, [Some(true), Some(true), None]),
            (Operator::Gt, [Some(false), Some(false), None]),
            (Operator::GtEq, [Some(true), Some(true), None]),
            (Operator::IsDistinctFrom, [Some(false), Some(false), Some(true)]),
            (Operator::IsNotDistinctFrom, [Some(true), Some(true), Some(false)]),
        ] {
            assert_eq!(compare_pairs(op, &pairs), expected, "{op}");
        }
    }

    /// `IS [NOT] DISTINCT FROM` has every NaN as one value (bits aside), and
    /// NaN distinct from a number.
    #[test]
    fn nans_are_not_distinct_from_each_other() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1 | (1 << 63));
        let pairs = [
            (Some(f64::NAN), Some(other_nan)),
            (Some(f64::NAN), Some(1.0)),
            (Some(f64::NAN), None),
        ];
        assert_eq!(
            compare_pairs(Operator::IsNotDistinctFrom, &pairs),
            [Some(true), Some(false), Some(false)]
        );
        assert_eq!(
            compare_pairs(Operator::IsDistinctFrom, &pairs),
            [Some(false), Some(true), Some(true)]
        );
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
