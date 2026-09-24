//! Aggregates whose Trino result type differs from the engine's, computed by
//! the engine's accumulator on converted inputs:
//!
//! - `corr`, `covar_samp`, `covar_pop`, `regr_slope`, `regr_intercept` over
//!   `integer`/`real` arguments resolve to Trino's `real` overload (the
//!   cheapest coercion): the result is `real`;
//! - `bitwise_and_agg`, `bitwise_or_agg` return `bigint` for any integer;
//! - [`Retyped`] and [`RetypedGroups`] run an engine accumulator on inputs
//!   cast to its type and convert the result (used by `avg(real)` and the
//!   `decimal` `sum`).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};

use super::aggregate::udaf_boilerplate;
use super::support::{is_integer, is_numeric, not_found};

/// Converts the final value of a [`Retyped`] accumulator.
pub(crate) type Finish = fn(&ScalarValue) -> Result<ScalarValue>;
/// Converts the final values of a [`RetypedGroups`] accumulator.
pub(crate) type FinishArray = fn(&ArrayRef) -> Result<ArrayRef>;

fn cast_all(values: &[ArrayRef], inputs: &[DataType]) -> Result<Vec<ArrayRef>> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| match inputs.get(i) {
            Some(t) if v.data_type() != t => Ok(cast(v, t)?),
            _ => Ok(Arc::clone(v)),
        })
        .collect()
}

/// Fields of the engine's arguments.
pub(crate) fn fields(name: &str, types: &[DataType]) -> Vec<FieldRef> {
    types
        .iter()
        .map(|t| Arc::new(Field::new(name, t.clone(), true)))
        .collect()
}

/// `args` with the engine's argument fields and result type.
pub(crate) fn engine_args<'a>(
    args: &AccumulatorArgs<'a>,
    input_fields: &'a [FieldRef],
    result: &DataType,
) -> AccumulatorArgs<'a> {
    AccumulatorArgs {
        return_field: Arc::new(Field::new(args.name, result.clone(), true)),
        schema: args.schema,
        ignore_nulls: args.ignore_nulls,
        order_bys: args.order_bys,
        is_reversed: args.is_reversed,
        name: args.name,
        is_distinct: args.is_distinct,
        exprs: args.exprs,
        expr_fields: input_fields,
    }
}

/// `args` with the engine's argument fields and result type.
pub(crate) fn engine_state_args<'a>(
    args: &StateFieldsArgs<'a>,
    input_fields: &'a [FieldRef],
    result: &DataType,
) -> StateFieldsArgs<'a> {
    StateFieldsArgs {
        name: args.name,
        input_fields,
        return_field: Arc::new(Field::new(args.name, result.clone(), true)),
        ordering_fields: args.ordering_fields,
        is_distinct: args.is_distinct,
    }
}

/// An engine accumulator fed with inputs cast to `inputs`, its result
/// converted by `finish`.
pub(crate) struct Retyped {
    pub(crate) inner: Box<dyn Accumulator>,
    pub(crate) inputs: Vec<DataType>,
    /// Result conversion (none: the engine's result).
    pub(crate) finish: Option<Finish>,
}

impl std::fmt::Debug for Retyped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Retyped")
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

impl Accumulator for Retyped {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&cast_all(values, &self.inputs)?)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(&cast_all(values, &self.inputs)?)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.inner.evaluate()?;
        match self.finish {
            Some(finish) => finish(&value),
            None => Ok(value),
        }
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.inner.size()
    }
}

/// The grouped form of [`Retyped`].
pub(crate) struct RetypedGroups {
    pub(crate) inner: Box<dyn GroupsAccumulator>,
    pub(crate) inputs: Vec<DataType>,
    /// Result conversion (none: the engine's result).
    pub(crate) finish: Option<FinishArray>,
}

impl std::fmt::Debug for RetypedGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetypedGroups")
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

impl GroupsAccumulator for RetypedGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner.update_batch(
            &cast_all(values, &self.inputs)?,
            group_indices,
            opt_filter,
            total_num_groups,
        )
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
        let values = self.inner.evaluate(emit_to)?;
        match self.finish {
            Some(finish) => finish(&values),
            None => Ok(values),
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner
            .convert_to_state(&cast_all(values, &self.inputs)?, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.inner.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

pub(crate) fn to_real(value: &ScalarValue) -> Result<ScalarValue> {
    value.cast_to(&DataType::Float32)
}

pub(crate) fn to_real_array(values: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast(values, &DataType::Float32)?)
}

// ---------------------------------------------------------------------------
// Statistics with a `real` overload
// ---------------------------------------------------------------------------

/// A two-argument statistic (`corr`, `covar_*`, `regr_*`) with Trino's
/// overloads: `real` arguments (integers coerce to `real` first) give a
/// `real`, any other numeric argument a `double`.
#[derive(Debug)]
struct RealOverload {
    signature: Signature,
    name: &'static str,
    engine: fn() -> Arc<AggregateUDF>,
}

impl PartialEq for RealOverload {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for RealOverload {}

impl std::hash::Hash for RealOverload {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl RealOverload {
    fn real(types: &[DataType]) -> bool {
        types.iter().all(|t| *t == DataType::Float32)
    }

    fn doubles() -> Vec<DataType> {
        vec![DataType::Float64; 2]
    }
}

impl AggregateUDFImpl for RealOverload {
    udaf_boilerplate!();

    fn name(&self) -> &str {
        self.name
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 || !arg_types.iter().all(|t| is_numeric(t) || t.is_null()) {
            return Err(not_found(self.name, arg_types));
        }
        let realish = |t: &DataType| is_integer(t) || *t == DataType::Float32;
        if arg_types.iter().all(realish) {
            Ok(vec![DataType::Float32; 2])
        } else {
            Ok(Self::doubles())
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if Self::real(arg_types) {
            Ok(DataType::Float32)
        } else {
            (self.engine)().return_type(arg_types)
        }
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let doubles = fields(args.name, &Self::doubles());
        (self.engine)()
            .inner()
            .state_fields(engine_state_args(&args, &doubles, &DataType::Float64))
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let doubles = fields(args.name, &Self::doubles());
        let inner = (self.engine)()
            .inner()
            .accumulator(engine_args(&args, &doubles, &DataType::Float64))?;
        let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
        let finish = Self::real(&types).then_some(to_real as Finish);
        Ok(Box::new(Retyped {
            inner,
            inputs: Self::doubles(),
            finish,
        }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        let doubles = fields(args.name, &Self::doubles());
        (self.engine)()
            .inner()
            .groups_accumulator_supported(engine_args(&args, &doubles, &DataType::Float64))
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        let doubles = fields(args.name, &Self::doubles());
        let inner = (self.engine)().inner().create_groups_accumulator(engine_args(
            &args,
            &doubles,
            &DataType::Float64,
        ))?;
        let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
        let finish = Self::real(&types).then_some(to_real_array as FinishArray);
        Ok(Box::new(RetypedGroups {
            inner,
            inputs: Self::doubles(),
            finish,
        }))
    }
}

// ---------------------------------------------------------------------------
// bitwise_and_agg, bitwise_or_agg
// ---------------------------------------------------------------------------

/// Trino's `bitwise_*_agg` only exist for `bigint`: narrower integers are
/// widened first, so the result is `bigint`.
#[derive(Debug)]
struct BigintOnly {
    signature: Signature,
    name: &'static str,
    engine: fn() -> Arc<AggregateUDF>,
}

impl PartialEq for BigintOnly {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for BigintOnly {}

impl std::hash::Hash for BigintOnly {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl AggregateUDFImpl for BigintOnly {
    udaf_boilerplate!();

    fn name(&self) -> &str {
        self.name
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] if is_integer(t) || t.is_null() => Ok(vec![DataType::Int64]),
            _ => Err(not_found(self.name, arg_types)),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        (self.engine)().inner().state_fields(args)
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        (self.engine)().inner().accumulator(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        (self.engine)().inner().groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        (self.engine)().inner().create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        (self.engine)().inner().create_sliding_accumulator(args)
    }
}

pub(crate) fn udafs() -> Vec<AggregateUDF> {
    use datafusion::functions_aggregate::{bit_and_or_xor, correlation, covariance, regr};
    let user_defined = || Signature::user_defined(Volatility::Immutable);
    let statistic = |name: &'static str, engine: fn() -> Arc<AggregateUDF>| {
        AggregateUDF::from(RealOverload {
            signature: user_defined(),
            name,
            engine,
        })
    };
    vec![
        statistic("corr", correlation::corr_udaf),
        statistic("covar_samp", covariance::covar_samp_udaf),
        statistic("covar_pop", covariance::covar_pop_udaf),
        statistic("regr_slope", regr::regr_slope_udaf),
        statistic("regr_intercept", regr::regr_intercept_udaf),
        AggregateUDF::from(BigintOnly {
            signature: user_defined(),
            name: "bitwise_and_agg",
            engine: bit_and_or_xor::bit_and_udaf,
        }),
        AggregateUDF::from(BigintOnly {
            signature: user_defined(),
            name: "bitwise_or_agg",
            engine: bit_and_or_xor::bit_or_udaf,
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_statistics_resolve_to_real() {
        let corr = AggregateUDF::from(RealOverload {
            signature: Signature::user_defined(Volatility::Immutable),
            name: "corr",
            engine: datafusion::functions_aggregate::correlation::corr_udaf,
        });
        let coerced = corr
            .inner()
            .coerce_types(&[DataType::Int32, DataType::Int64])
            .unwrap();
        assert_eq!(coerced, vec![DataType::Float32; 2]);
        assert_eq!(corr.inner().return_type(&coerced).unwrap(), DataType::Float32);
        let mixed = corr
            .inner()
            .coerce_types(&[DataType::Int32, DataType::Float64])
            .unwrap();
        assert_eq!(corr.inner().return_type(&mixed).unwrap(), DataType::Float64);
    }

    #[test]
    fn retyped_casts_inputs_and_result() {
        let doubles = fields("avg", &[DataType::Float64]);
        let schema = datafusion::arrow::datatypes::Schema::empty();
        let args = AccumulatorArgs {
            return_field: Arc::new(Field::new("avg", DataType::Float64, true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "avg",
            is_distinct: false,
            exprs: &[],
            expr_fields: &doubles,
        };
        let inner = datafusion::functions_aggregate::average::avg_udaf()
            .inner()
            .accumulator(args)
            .unwrap();
        let mut acc = Retyped {
            inner,
            inputs: vec![DataType::Float64],
            finish: Some(to_real),
        };
        let values: ArrayRef = Arc::new(datafusion::arrow::array::Float32Array::from(vec![1.5, 2.25]));
        acc.update_batch(&[values]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float32(Some(1.875)));
    }
}
