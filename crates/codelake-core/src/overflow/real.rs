//! `sum` and `avg` of `real`: Trino 431 adds in `double` and answers `real`
//! (`sum(CAST(n AS REAL))` is `real`); `DataFusion` answers `double`.
//! [`RealResult`] wraps `DataFusion`'s function (which adds in `double`) and
//! casts its result, keeping its name and states.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};

/// An aggregate of `double`s answering `real` (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct RealResult {
    inner: Arc<AggregateUDF>,
}

impl RealResult {
    pub(crate) fn new(inner: Arc<AggregateUDF>) -> Self {
        Self { inner }
    }
}

fn double(field: &FieldRef) -> FieldRef {
    Arc::new(
        Field::new(field.name(), DataType::Float64, field.is_nullable())
            .with_metadata(field.metadata().clone()),
    )
}

/// `args` as the inner function sees them: a `double` result.
fn inner_args<'a>(args: &AccumulatorArgs<'a>) -> AccumulatorArgs<'a> {
    AccumulatorArgs {
        return_field: double(&args.return_field),
        ..args.clone()
    }
}

fn to_real(value: &ScalarValue) -> Result<ScalarValue> {
    value.cast_to(&DataType::Float32)
}

impl AggregateUDFImpl for RealResult {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(RealAccumulator(
            self.inner.accumulator(inner_args(&args))?,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(StateFieldsArgs {
            name: args.name,
            input_fields: args.input_fields,
            return_field: double(&args.return_field),
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        })
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        self.inner.groups_accumulator_supported(inner_args(&args))
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(RealGroups(
            self.inner.create_groups_accumulator(inner_args(&args))?,
        )))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(RealAccumulator(
            self.inner.create_sliding_accumulator(inner_args(&args))?,
        )))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        match self.inner.inner().reverse_expr() {
            ReversedUDAF::Reversed(reversed) => {
                ReversedUDAF::Reversed(Arc::new(AggregateUDF::new_from_impl(Self::new(reversed))))
            }
            other => other,
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float32(None))
    }
}

/// The inner accumulator, its result cast to `real`.
#[derive(Debug)]
struct RealAccumulator(Box<dyn Accumulator>);

impl Accumulator for RealAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        to_real(&self.0.evaluate()?)
    }

    fn size(&self) -> usize {
        self.0.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.merge_batch(states)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.retract_batch(values)
    }

    fn supports_retract_batch(&self) -> bool {
        self.0.supports_retract_batch()
    }
}

/// The inner groups accumulator, its results cast to `real`.
struct RealGroups(Box<dyn GroupsAccumulator>);

impl GroupsAccumulator for RealGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.0
            .update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        Ok(cast(&self.0.evaluate(emit_to)?, &DataType::Float32)?)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.0.state(emit_to)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.0
            .merge_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.0.convert_to_state(values, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.0.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}
