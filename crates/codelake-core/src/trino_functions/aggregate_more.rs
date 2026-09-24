//! More aggregate functions: `sum` with Trino types, `geometric_mean`,
//! `skewness`, `kurtosis`, `histogram`, `map_agg`, and the `n` form of
//! `max_by`/`min_by`.
//!
//! Memory: like `aggregate`, every accumulator reports its buffered state
//! through `Accumulator::size`, which the aggregation operator reserves from
//! the pool.

use std::any::Any;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float64Array, Int64Array, ListArray,
    MapArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::i256;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
    Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use super::aggregate::{compare, hashable, state_field, udaf_boilerplate};
use super::aggregate_types::{Retyped, RetypedGroups, engine_args, engine_state_args, fields};
use super::collections::{build_maps, map_type};
use super::support::{invalid_argument, is_decimal, is_integer, is_numeric, not_found, out_of_range};

fn list_of(t: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(t, true)))
}

fn doubles(array: &ArrayRef) -> Result<Float64Array> {
    let values = cast(array, &DataType::Float64)?;
    values
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("expected Float64".into()))
}

fn list_rows(array: &ArrayRef) -> Result<&ListArray> {
    array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Internal(format!("expected list state, got {}", array.data_type())))
}

fn scalars(array: &ArrayRef) -> Result<Vec<ScalarValue>> {
    (0..array.len())
        .map(|i| ScalarValue::try_from_array(array, i))
        .collect()
}

fn list_scalar(values: &[ScalarValue], element: &DataType) -> ScalarValue {
    ScalarValue::List(ScalarValue::new_list_nullable(values, element))
}

// ---------------------------------------------------------------------------
// sum
// ---------------------------------------------------------------------------

fn builtin_sum() -> Arc<AggregateUDF> {
    datafusion::functions_aggregate::sum::sum_udaf()
}

/// Exact sum of `bigint` values, kept as `decimal(38, 0)`.
const WIDE: DataType = DataType::Decimal128(38, 0);

/// Trino's `sum`: integers sum to `bigint` and fail on overflow, `real` stays
/// `real`, `decimal(p, s)` sums to `decimal(38, s)`; `double` and `DISTINCT`
/// keep the engine's accumulators.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Sum {
    signature: Signature,
}

impl Sum {
    fn exact_bigint(args: &AccumulatorArgs<'_>) -> bool {
        !args.is_distinct && args.return_field.data_type() == &DataType::Int64
    }

    fn real(args: &AccumulatorArgs<'_>) -> bool {
        args.return_field.data_type() == &DataType::Float32
    }

    /// The engine's arguments with the wide `decimal(38, 0)` sum type.
    fn wide_args<'a>(args: &AccumulatorArgs<'a>, wide_fields: &'a [FieldRef]) -> AccumulatorArgs<'a> {
        AccumulatorArgs {
            return_field: Arc::new(Field::new(args.name, WIDE, true)),
            schema: args.schema,
            ignore_nulls: args.ignore_nulls,
            order_bys: args.order_bys,
            is_reversed: args.is_reversed,
            name: args.name,
            is_distinct: args.is_distinct,
            exprs: args.exprs,
            expr_fields: wide_fields,
        }
    }
}

impl AggregateUDFImpl for Sum {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "sum"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] if is_integer(t) || t.is_null() => Ok(vec![DataType::Int64]),
            [DataType::Float32] => Ok(vec![DataType::Float32]),
            [t] if is_decimal(t) => Ok(vec![t.clone()]),
            [DataType::Float16 | DataType::Float64] => Ok(vec![DataType::Float64]),
            [t @ (DataType::Duration(_) | DataType::Interval(_))] => Ok(vec![t.clone()]),
            _ => Err(not_found("sum", arg_types)),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [DataType::Int64] => Ok(DataType::Int64),
            [DataType::Float32] => Ok(DataType::Float32),
            [DataType::Decimal128(_, scale)] => Ok(DataType::Decimal128(38, *scale)),
            _ => builtin_sum().return_type(arg_types),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        match (args.return_field.data_type(), args.is_distinct) {
            (DataType::Int64, false) => Ok(vec![state_field(args.name, "sum", WIDE)]),
            (DataType::Float32, false) => Ok(vec![state_field(args.name, "sum", DataType::Float64)]),
            (DataType::Decimal128(_, scale), _) => {
                let wide = wide_decimal(*scale);
                let inputs = fields(args.name, std::slice::from_ref(&wide));
                builtin_sum()
                    .inner()
                    .state_fields(engine_state_args(&args, &inputs, &wide))
            }
            _ => builtin_sum().inner().state_fields(args),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        if Self::exact_bigint(&args) {
            return Ok(Box::new(ExactSum::default()));
        }
        if Self::real(&args) && !args.is_distinct {
            return Ok(Box::new(RealSum::default()));
        }
        if let DataType::Decimal128(_, scale) = args.return_field.data_type() {
            let wide = wide_decimal(*scale);
            let inputs = fields(args.name, std::slice::from_ref(&wide));
            let inner = builtin_sum()
                .inner()
                .accumulator(engine_args(&args, &inputs, &wide))?;
            return Ok(Box::new(Retyped {
                inner,
                inputs: vec![wide],
                finish: Some(narrow_decimal),
            }));
        }
        builtin_sum().inner().accumulator(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        if Self::real(&args) {
            return false;
        }
        if let DataType::Decimal128(_, scale) = args.return_field.data_type() {
            let wide = wide_decimal(*scale);
            let inputs = fields(args.name, std::slice::from_ref(&wide));
            return builtin_sum()
                .inner()
                .groups_accumulator_supported(engine_args(&args, &inputs, &wide));
        }
        builtin_sum().inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        if Self::exact_bigint(&args) {
            let wide_fields = vec![Arc::new(Field::new(args.name, WIDE, true))];
            let inner = builtin_sum()
                .inner()
                .create_groups_accumulator(Self::wide_args(&args, &wide_fields))?;
            return Ok(Box::new(ExactSumGroups { inner }));
        }
        if let DataType::Decimal128(_, scale) = args.return_field.data_type() {
            let wide = wide_decimal(*scale);
            let inputs = fields(args.name, std::slice::from_ref(&wide));
            let inner = builtin_sum()
                .inner()
                .create_groups_accumulator(engine_args(&args, &inputs, &wide))?;
            return Ok(Box::new(RetypedGroups {
                inner,
                inputs: vec![wide],
                finish: Some(narrow_decimal_array),
            }));
        }
        builtin_sum().inner().create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        if Self::exact_bigint(&args) {
            return Ok(Box::new(ExactSum::default()));
        }
        if Self::real(&args) && !args.is_distinct {
            return Ok(Box::new(RealSum::default()));
        }
        if let DataType::Decimal128(_, scale) = args.return_field.data_type() {
            let wide = wide_decimal(*scale);
            let inputs = fields(args.name, std::slice::from_ref(&wide));
            let inner = builtin_sum()
                .inner()
                .create_sliding_accumulator(engine_args(&args, &inputs, &wide))?;
            return Ok(Box::new(Retyped {
                inner,
                inputs: vec![wide],
                finish: Some(narrow_decimal),
            }));
        }
        builtin_sum().inner().create_sliding_accumulator(args)
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

/// `decimal(p, s)` sums run on 76 digits, so the running total never
/// wraps; the result must fit Trino's `decimal(38, s)`.
fn wide_decimal(scale: i8) -> DataType {
    DataType::Decimal256(76, scale)
}

/// Largest unscaled `decimal(38, s)` value.
const DECIMAL_38_MAX: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

fn decimal_overflow() -> DataFusionError {
    DataFusionError::Execution("GENERIC_INTERNAL_ERROR: Decimal overflow".into())
}

fn narrow(value: i256) -> Result<i128> {
    value
        .to_i128()
        .filter(|v| (-DECIMAL_38_MAX..=DECIMAL_38_MAX).contains(v))
        .ok_or_else(decimal_overflow)
}

fn narrow_decimal(value: &ScalarValue) -> Result<ScalarValue> {
    match value {
        ScalarValue::Decimal256(v, _, scale) => {
            Ok(ScalarValue::Decimal128(v.map(narrow).transpose()?, 38, *scale))
        }
        other => Err(DataFusionError::Internal(format!("decimal sum: {other:?}"))),
    }
}

fn narrow_decimal_array(values: &ArrayRef) -> Result<ArrayRef> {
    let sums = values
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .ok_or_else(|| DataFusionError::Internal("decimal sum groups".into()))?;
    let out = sums
        .iter()
        .map(|v| v.map(narrow).transpose())
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        Decimal128Array::from(out).with_precision_and_scale(38, sums.scale())?,
    ))
}

fn to_bigint(sum: i128) -> Result<i64> {
    i64::try_from(sum).map_err(|_| out_of_range(format!("bigint addition overflow: {sum}")))
}

/// `sum(bigint)`: exact in `i128`, `NUMERIC_VALUE_OUT_OF_RANGE` when the total
/// does not fit a `bigint`.
#[derive(Debug, Default)]
struct ExactSum {
    sum: i128,
    count: u64,
}

impl ExactSum {
    fn add(&mut self, values: &ArrayRef, sign: i128) -> Result<()> {
        let values = cast(values, &DataType::Int64)?;
        let values = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("sum input".into()))?;
        for v in values.iter().flatten() {
            self.sum += sign * i128::from(v);
            if sign > 0 {
                self.count += 1;
            } else {
                self.count = self.count.saturating_sub(1);
            }
        }
        Ok(())
    }
}

impl Accumulator for ExactSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        values.first().map_or(Ok(()), |v| self.add(v, 1))
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        values.first().map_or(Ok(()), |v| self.add(v, -1))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(sums) = states.first() else { return Ok(()) };
        let sums = sums
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| DataFusionError::Internal("sum state".into()))?;
        for v in sums.iter().flatten() {
            self.sum += v;
            self.count += 1;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sum = (self.count > 0).then_some(self.sum);
        Ok(vec![ScalarValue::Decimal128(sum, 38, 0)])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::Int64(None));
        }
        Ok(ScalarValue::Int64(Some(to_bigint(self.sum)?)))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

/// Grouped `sum(bigint)`: the engine's `decimal(38, 0)` sum, checked on output.
struct ExactSumGroups {
    inner: Box<dyn GroupsAccumulator>,
}

impl std::fmt::Debug for ExactSumGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExactSumGroups").finish_non_exhaustive()
    }
}

impl ExactSumGroups {
    fn widen(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        values.iter().map(|v| Ok(cast(v, &WIDE)?)).collect()
    }
}

impl GroupsAccumulator for ExactSumGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .update_batch(&Self::widen(values)?, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = self.inner.evaluate(emit_to)?;
        let sums = sums
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| DataFusionError::Internal("sum groups".into()))?;
        let out = sums
            .iter()
            .map(|v| v.map(to_bigint).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(Int64Array::from(out)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(&Self::widen(values)?, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.inner.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

/// `sum(real)`: summed as `double`, returned as `real`.
#[derive(Debug, Default)]
struct RealSum {
    sum: f64,
    count: u64,
}

impl RealSum {
    fn add(&mut self, values: &ArrayRef, sign: f64) -> Result<()> {
        for v in doubles(values)?.iter().flatten() {
            self.sum += sign * v;
            if sign > 0.0 {
                self.count += 1;
            } else {
                self.count = self.count.saturating_sub(1);
            }
        }
        Ok(())
    }
}

impl Accumulator for RealSum {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        values.first().map_or(Ok(()), |v| self.add(v, 1.0))
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        values.first().map_or(Ok(()), |v| self.add(v, -1.0))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(sums) = states.first() else { return Ok(()) };
        for v in doubles(sums)?.iter().flatten() {
            self.sum += v;
            self.count += 1;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64((self.count > 0).then_some(self.sum))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::Float64((self.count > 0).then_some(self.sum)).cast_to(&DataType::Float32)
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

// ---------------------------------------------------------------------------
// geometric_mean
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct GeometricMean {
    signature: Signature,
}

impl AggregateUDFImpl for GeometricMean {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "geometric_mean"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] if is_numeric(t) || t.is_null() => Ok(vec![DataType::Float64]),
            _ => Err(not_found("geometric_mean", arg_types)),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![
            state_field(args.name, "count", DataType::Int64),
            state_field(args.name, "log_sum", DataType::Float64),
        ])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(GeometricMeanAccumulator::default()))
    }
}

/// Trino's `GeometricMeanAggregations`: `exp(Σ ln x / n)`.
#[derive(Debug, Default)]
struct GeometricMeanAccumulator {
    count: i64,
    log_sum: f64,
}

impl Accumulator for GeometricMeanAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return Ok(());
        };
        for v in doubles(values)?.iter().flatten() {
            self.count += 1;
            self.log_sum += v.ln();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [counts, sums] = states else {
            return Err(DataFusionError::Internal("geometric_mean state".into()));
        };
        let counts = cast(counts, &DataType::Int64)?;
        let counts = counts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("geometric_mean count".into()))?;
        let sums = doubles(sums)?;
        for i in 0..counts.len() {
            if !counts.is_null(i) {
                self.count += counts.value(i);
                self.log_sum += sums.value(i);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Int64(Some(self.count)),
            ScalarValue::Float64(Some(self.log_sum)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::Float64(None));
        }
        let n = super::cast::i64_to_f64(self.count);
        Ok(ScalarValue::Float64(Some((self.log_sum / n).exp())))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

// ---------------------------------------------------------------------------
// skewness, kurtosis
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CentralMoments {
    signature: Signature,
    kurtosis: bool,
}

impl AggregateUDFImpl for CentralMoments {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        if self.kurtosis { "kurtosis" } else { "skewness" }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] if is_numeric(t) || t.is_null() => Ok(vec![DataType::Float64]),
            _ => Err(not_found(self.name(), arg_types)),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(["count", "m1", "m2", "m3", "m4"]
            .iter()
            .map(|f| state_field(args.name, f, DataType::Float64))
            .collect())
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MomentsAccumulator {
            moments: Moments::default(),
            kurtosis: self.kurtosis,
        }))
    }
}

/// Count and central moment sums, updated and combined as Trino's
/// `CentralMomentsAggregation` does.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
struct Moments {
    n: f64,
    m1: f64,
    m2: f64,
    m3: f64,
    m4: f64,
}

impl Moments {
    fn add(&mut self, value: f64) {
        let n1 = self.n;
        let n = n1 + 1.0;
        let delta = value - self.m1;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let dm2 = delta * delta_n * n1;
        let (m2, m3) = (self.m2, self.m3);
        self.n = n;
        self.m1 += delta_n;
        self.m2 = m2 + dm2;
        self.m3 = m3 + dm2 * delta_n * (n - 2.0) - 3.0 * delta_n * m2;
        self.m4 += dm2 * delta_n2 * (n * n - 3.0 * n + 3.0) + 6.0 * delta_n2 * m2 - 4.0 * delta_n * m3;
    }

    fn merge(&mut self, other: Self) {
        if other.n == 0.0 {
            return;
        }
        if self.n == 0.0 {
            *self = other;
            return;
        }
        let (na, nb) = (self.n, other.n);
        let n = na + nb;
        let delta = other.m1 - self.m1;
        let delta2 = delta * delta;
        let delta3 = delta * delta2;
        let delta4 = delta2 * delta2;
        let m1 = (na * self.m1 + nb * other.m1) / n;
        let m2 = self.m2 + other.m2 + delta2 * na * nb / n;
        let m3 = self.m3
            + other.m3
            + delta3 * na * nb * (na - nb) / (n * n)
            + 3.0 * delta * (na * other.m2 - nb * self.m2) / n;
        let m4 = self.m4
            + other.m4
            + delta4 * na * nb * (na * na - na * nb + nb * nb) / (n * n * n)
            + 6.0 * delta2 * (na * na * other.m2 + nb * nb * self.m2) / (n * n)
            + 4.0 * delta * (na * other.m3 - nb * self.m3) / n;
        *self = Self { n, m1, m2, m3, m4 };
    }

    /// Fisher's moment coefficient of skewness; NULL below three values.
    fn skewness(&self) -> Option<f64> {
        (self.n >= 3.0).then(|| self.n.sqrt() * self.m3 / self.m2.powf(1.5))
    }

    /// Unbiased excess kurtosis; NULL below four values.
    fn kurtosis(&self) -> Option<f64> {
        let n = self.n;
        (n >= 4.0).then(|| {
            (n - 1.0) * ((n + 1.0) * n * self.m4 / (self.m2 * self.m2) - 3.0 * (n - 1.0))
                / ((n - 2.0) * (n - 3.0))
        })
    }
}

#[derive(Debug)]
struct MomentsAccumulator {
    moments: Moments,
    kurtosis: bool,
}

impl Accumulator for MomentsAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return Ok(());
        };
        for v in doubles(values)?.iter().flatten() {
            self.moments.add(v);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let columns = states.iter().map(doubles).collect::<Result<Vec<_>>>()?;
        let [n, m1, m2, m3, m4] = columns.as_slice() else {
            return Err(DataFusionError::Internal("central moments state".into()));
        };
        for i in 0..n.len() {
            if !n.is_null(i) {
                self.moments.merge(Moments {
                    n: n.value(i),
                    m1: m1.value(i),
                    m2: m2.value(i),
                    m3: m3.value(i),
                    m4: m4.value(i),
                });
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let m = self.moments;
        Ok([m.n, m.m1, m.m2, m.m3, m.m4]
            .into_iter()
            .map(|v| ScalarValue::Float64(Some(v)))
            .collect())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = if self.kurtosis {
            self.moments.kurtosis()
        } else {
            self.moments.skewness()
        };
        Ok(ScalarValue::Float64(value))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

// ---------------------------------------------------------------------------
// histogram, map_agg
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct MapAggregate {
    signature: Signature,
    histogram: bool,
}

impl MapAggregate {
    fn key_value_types(&self, arg_types: &[DataType]) -> Result<(DataType, DataType)> {
        match (self.histogram, arg_types) {
            (true, [k]) => Ok((k.clone(), DataType::Int64)),
            (false, [k, v]) => Ok((k.clone(), v.clone())),
            _ => Err(not_found(self.name(), arg_types)),
        }
    }
}

impl AggregateUDFImpl for MapAggregate {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        if self.histogram { "histogram" } else { "map_agg" }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let key = arg_types
            .first()
            .ok_or_else(|| not_found(self.name(), arg_types))?;
        if !(hashable(key) || key.is_null()) {
            return Err(not_found(self.name(), arg_types));
        }
        let key = if key.is_null() {
            DataType::Utf8
        } else {
            key.clone()
        };
        match (self.histogram, arg_types) {
            (true, [_]) => Ok(vec![key]),
            (false, [_, v]) => Ok(vec![key, if v.is_null() { DataType::Int64 } else { v.clone() }]),
            _ => Err(not_found(self.name(), arg_types)),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (k, v) = self.key_value_types(arg_types)?;
        Ok(map_type(k, v))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let types: Vec<DataType> = args.input_fields.iter().map(|f| f.data_type().clone()).collect();
        let (k, v) = self.key_value_types(&types)?;
        Ok(vec![
            state_field(args.name, "keys", list_of(k)),
            state_field(args.name, "values", list_of(v)),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
        let (key_type, value_type) = self.key_value_types(&types)?;
        Ok(Box::new(MapAccumulator {
            histogram: self.histogram,
            key_type,
            value_type,
            index: HashMap::new(),
            entries: Vec::new(),
            bytes: 0,
        }))
    }
}

/// Entries in first-seen order; `histogram` counts, `map_agg` keeps the first value.
#[derive(Debug)]
struct MapAccumulator {
    histogram: bool,
    key_type: DataType,
    value_type: DataType,
    index: HashMap<ScalarValue, usize>,
    entries: Vec<(ScalarValue, ScalarValue)>,
    bytes: usize,
}

impl MapAccumulator {
    fn offer(&mut self, key: ScalarValue, value: ScalarValue) -> Result<()> {
        if key.is_null() {
            return Ok(());
        }
        let key = super::aggregate::canonical_float(key);
        match self.index.get(&key) {
            Some(&slot) if self.histogram => {
                let Some((_, ScalarValue::Int64(Some(count)))) = self.entries.get_mut(slot) else {
                    return Err(DataFusionError::Internal("histogram count".into()));
                };
                let ScalarValue::Int64(Some(add)) = value else {
                    return Err(DataFusionError::Internal("histogram increment".into()));
                };
                *count += add;
            }
            Some(_) => {}
            None => {
                self.bytes += 2 * key.size() + value.size() + size_of::<usize>();
                self.index.insert(key.clone(), self.entries.len());
                self.entries.push((key, value));
            }
        }
        Ok(())
    }
}

impl Accumulator for MapAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(keys) = values.first() else { return Ok(()) };
        for i in 0..keys.len() {
            let key = ScalarValue::try_from_array(keys, i)?;
            let value = match values.get(1) {
                Some(v) if !self.histogram => ScalarValue::try_from_array(v, i)?,
                _ => ScalarValue::Int64(Some(1)),
            };
            self.offer(key, value)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [keys, values] = states else {
            return Err(DataFusionError::Internal("map aggregate state".into()));
        };
        let (keys, values) = (list_rows(keys)?, list_rows(values)?);
        for row in 0..keys.len() {
            if keys.is_null(row) {
                continue;
            }
            let (k, v) = (scalars(&keys.value(row))?, scalars(&values.value(row))?);
            for (key, value) in k.into_iter().zip(v) {
                self.offer(key, value)?;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let keys: Vec<ScalarValue> = self.entries.iter().map(|(k, _)| k.clone()).collect();
        let values: Vec<ScalarValue> = self.entries.iter().map(|(_, v)| v.clone()).collect();
        Ok(vec![
            list_scalar(&keys, &self.key_type),
            list_scalar(&values, &self.value_type),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let row = (!self.entries.is_empty()).then(|| self.entries.clone());
        let maps = build_maps(vec![row], &self.key_type, &self.value_type)?;
        let maps = maps
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| DataFusionError::Internal("map result".into()))?;
        ScalarValue::try_from_array(maps, 0)
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.bytes
    }
}

// ---------------------------------------------------------------------------
// max_by(x, y, n), min_by(x, y, n)
// ---------------------------------------------------------------------------

/// The literal `n` of `max_by(x, y, n)`.
pub(crate) fn top_n(args: &AccumulatorArgs<'_>, name: &str) -> Result<usize> {
    let literal = args
        .exprs
        .get(2)
        .and_then(|e| e.as_any().downcast_ref::<Literal>())
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "NOT_SUPPORTED: the third argument of {name} must be a constant"
            ))
        })?;
    let n = match literal.value() {
        ScalarValue::Int64(Some(n)) => *n,
        ScalarValue::Int32(Some(n)) => i64::from(*n),
        other => {
            return Err(invalid_argument(format!(
                "third argument of {name} must be a positive integer, got {other}"
            )));
        }
    };
    if n <= 0 {
        return Err(invalid_argument(format!(
            "third argument of {name} must be a positive integer"
        )));
    }
    usize::try_from(n).map_err(|_| invalid_argument(format!("third argument of {name} is too large")))
}

/// One candidate of `max_by(x, y, n)`: `seq` is its arrival order, which
/// breaks ties (the first arrival ranks first).
#[derive(Debug)]
struct Candidate {
    x: ScalarValue,
    y: ScalarValue,
    seq: u64,
    /// `max_by`: larger `y` is better; `min_by`: smaller.
    max: bool,
}

impl Candidate {
    /// `Greater` when `self` ranks better than `other`.
    fn rank(&self, other: &Self) -> std::cmp::Ordering {
        let by_y = compare(&self.y, &other.y);
        let by_y = if self.max { by_y } else { by_y.reverse() };
        by_y.then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.rank(other).is_eq()
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Heap order: the worst candidate on top.
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.rank(self)
    }
}

/// The `n` pairs with the largest (or smallest) `y`, best first, in a
/// bounded heap: `O(rows × log n)`.
#[derive(Debug)]
pub(crate) struct ByNAccumulator {
    n: usize,
    max: bool,
    x_type: DataType,
    y_type: DataType,
    best: BinaryHeap<Candidate>,
    seq: u64,
    bytes: usize,
}

impl ByNAccumulator {
    pub(crate) fn new(n: usize, max: bool, x_type: DataType, y_type: DataType) -> Self {
        Self {
            n,
            max,
            x_type,
            y_type,
            best: BinaryHeap::new(),
            seq: 0,
            bytes: 0,
        }
    }

    fn offer(&mut self, x: ScalarValue, y: ScalarValue) {
        // NULL and NaN keys are never picked (see `max_by`).
        if y.is_null() || super::aggregate::is_nan(&y) {
            return;
        }
        let candidate = Candidate {
            x,
            y,
            seq: self.seq,
            max: self.max,
        };
        self.seq += 1;
        if self.best.len() < self.n {
            self.bytes += candidate.x.size() + candidate.y.size();
            self.best.push(candidate);
        } else if let Some(mut worst) = self.best.peek_mut()
            && candidate.rank(&worst).is_gt()
        {
            self.bytes =
                self.bytes + candidate.x.size() + candidate.y.size() - worst.x.size() - worst.y.size();
            *worst = candidate;
        }
    }

    /// The kept pairs, best first.
    fn ranked(&self) -> Vec<&Candidate> {
        let mut all: Vec<&Candidate> = self.best.iter().collect();
        all.sort_by(|a, b| b.rank(a));
        all
    }

    pub(crate) fn state_fields(name: &str, x: &DataType, y: &DataType) -> Vec<FieldRef> {
        vec![
            state_field(name, "x", list_of(x.clone())),
            state_field(name, "y", list_of(y.clone())),
        ]
    }
}

impl Accumulator for ByNAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [xs, ys, ..] = values else {
            return Err(DataFusionError::Internal("max_by arguments".into()));
        };
        for i in 0..ys.len() {
            self.offer(
                ScalarValue::try_from_array(xs, i)?,
                ScalarValue::try_from_array(ys, i)?,
            );
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [xs, ys] = states else {
            return Err(DataFusionError::Internal("max_by state".into()));
        };
        let (xs, ys) = (list_rows(xs)?, list_rows(ys)?);
        for row in 0..xs.len() {
            if xs.is_null(row) {
                continue;
            }
            for (x, y) in scalars(&xs.value(row))?.into_iter().zip(scalars(&ys.value(row))?) {
                self.offer(x, y);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let ranked = self.ranked();
        let xs: Vec<ScalarValue> = ranked.iter().map(|c| c.x.clone()).collect();
        let ys: Vec<ScalarValue> = ranked.iter().map(|c| c.y.clone()).collect();
        Ok(vec![
            list_scalar(&xs, &self.x_type),
            list_scalar(&ys, &self.y_type),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.best.is_empty() {
            return ScalarValue::try_from(&list_of(self.x_type.clone()));
        }
        let xs: Vec<ScalarValue> = self.ranked().iter().map(|c| c.x.clone()).collect();
        Ok(list_scalar(&xs, &self.x_type))
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.bytes + self.best.capacity() * size_of::<Candidate>()
    }
}

// ---------------------------------------------------------------------------
// min, max
// ---------------------------------------------------------------------------

/// Float inputs with NaN turned into NULL: Trino's `min`/`max` skip NaN
/// (the engine orders NaN above every number).
fn without_nan(values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
    values
        .iter()
        .map(|v| match v.data_type() {
            DataType::Float32 | DataType::Float64 => {
                let mask = super::nan::nan_mask(v)?;
                Ok(datafusion::arrow::compute::nullif(v, &mask)?)
            }
            _ => Ok(Arc::clone(v)),
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Extreme {
    signature: Signature,
    max: bool,
}

fn engine_extreme(max: bool) -> Arc<AggregateUDF> {
    if max {
        datafusion::functions_aggregate::min_max::max_udaf()
    } else {
        datafusion::functions_aggregate::min_max::min_udaf()
    }
}

impl Extreme {
    fn new(max: bool) -> Self {
        Self {
            signature: engine_extreme(max).signature().clone(),
            max,
        }
    }

    fn engine(&self) -> Arc<AggregateUDF> {
        engine_extreme(self.max)
    }
}

impl AggregateUDFImpl for Extreme {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        if self.max { "max" } else { "min" }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.engine().inner().coerce_types(arg_types)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.engine().return_type(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        self.engine().inner().state_fields(args)
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NanFree(self.engine().inner().accumulator(args)?)))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        self.engine().inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(NanFreeGroups(
            self.engine().inner().create_groups_accumulator(args)?,
        )))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NanFree(
            self.engine().inner().create_sliding_accumulator(args)?,
        )))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn is_descending(&self) -> Option<bool> {
        Some(self.max)
    }
}

struct NanFree(Box<dyn Accumulator>);

impl std::fmt::Debug for NanFree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NanFree").field(&self.0).finish()
    }
}

impl Accumulator for NanFree {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.update_batch(&without_nan(values)?)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.retract_batch(&without_nan(values)?)
    }

    fn supports_retract_batch(&self) -> bool {
        self.0.supports_retract_batch()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge_batch(&without_nan(states)?)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.evaluate()
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

struct NanFreeGroups(Box<dyn GroupsAccumulator>);

impl std::fmt::Debug for NanFreeGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NanFreeGroups").finish_non_exhaustive()
    }
}

impl GroupsAccumulator for NanFreeGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.0
            .update_batch(&without_nan(values)?, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.0
            .merge_batch(&without_nan(values)?, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.0.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.0.state(emit_to)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.0.convert_to_state(&without_nan(values)?, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.0.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

pub(crate) fn udafs() -> Vec<AggregateUDF> {
    let user_defined = || Signature::user_defined(Volatility::Immutable);
    vec![
        AggregateUDF::from(Sum {
            signature: user_defined(),
        }),
        AggregateUDF::from(Extreme::new(true)),
        AggregateUDF::from(Extreme::new(false)),
        AggregateUDF::from(GeometricMean {
            signature: user_defined(),
        }),
        AggregateUDF::from(CentralMoments {
            signature: user_defined(),
            kurtosis: false,
        }),
        AggregateUDF::from(CentralMoments {
            signature: user_defined(),
            kurtosis: true,
        }),
        AggregateUDF::from(MapAggregate {
            signature: user_defined(),
            histogram: true,
        }),
        AggregateUDF::from(MapAggregate {
            signature: user_defined(),
            histogram: false,
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn moments(values: &[f64]) -> Moments {
        let mut m = Moments::default();
        for v in values {
            m.add(*v);
        }
        m
    }

    #[test]
    fn moments_merge_like_a_single_pass() {
        let all = moments(&[1.0, 2.0, 3.0, 10.0, 4.5]);
        let mut split = moments(&[1.0, 2.0]);
        split.merge(moments(&[3.0, 10.0, 4.5]));
        for (a, b) in [(all.m2, split.m2), (all.m3, split.m3), (all.m4, split.m4)] {
            assert!((a - b).abs() < 1e-9, "{a} vs {b}");
        }
        assert!(moments(&[1.0, 2.0]).skewness().is_none());
        assert!(moments(&[1.0, 2.0, 3.0]).kurtosis().is_none());
        // Symmetric data has no skew.
        assert!(
            moments(&[1.0, 2.0, 3.0])
                .skewness()
                .is_some_and(|s| s.abs() < 1e-12)
        );
    }

    #[test]
    fn top_n_keeps_the_best_in_order() {
        let mut acc = ByNAccumulator::new(2, true, DataType::Utf8, DataType::Int64);
        for (x, y) in [("a", 1), ("b", 3), ("c", 2)] {
            acc.offer(ScalarValue::from(x), ScalarValue::Int64(Some(y)));
        }
        acc.offer(ScalarValue::from("n"), ScalarValue::Int64(None));
        let xs: Vec<ScalarValue> = acc.ranked().iter().map(|c| c.x.clone()).collect();
        assert_eq!(xs, vec![ScalarValue::from("b"), ScalarValue::from("c")]);
    }
}
