//! `sum` over exact numbers that fails on overflow.
//!
//! `DataFusion`'s `sum` adds with wrapping arithmetic (`bigint` wraps around)
//! and never checks a decimal result against its precision. Trino fails with
//! `bigint addition overflow` / `Decimal overflow`. [`CheckedSum`] keeps the
//! name, signature and result type of `DataFusion`'s `sum` (so plans, column
//! names and pushed-down SQL do not change) and replaces only the arithmetic.
//! [`CheckedSum::trino`] also takes Trino's result type for decimals,
//! `decimal(38,s)`.

use std::fmt::Display;
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ArrowNativeTypeOp, AsArray, BooleanArray, PrimitiveArray};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, FieldRef,
    Int64Type, UInt64Type,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, SetMonotonicity, Signature,
};

use super::arith::{check_decimal, overflow, sql_type_name};

/// Context of a decimal `sum` that overflows: Trino 431 reports it as
/// `GENERIC_INTERNAL_ERROR` (`Decimal overflow`), not as the
/// `NUMERIC_VALUE_OUT_OF_RANGE` of decimal arithmetic. The overflow itself
/// stays the root of the error (see [`super::is_trino_internal_error`]).
pub(crate) const TRINO_INTERNAL_ERROR: &str = "Trino GENERIC_INTERNAL_ERROR";

/// A decimal `sum` that does not fit its type.
fn decimal_sum_overflow(error: datafusion::error::DataFusionError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Context(TRINO_INTERNAL_ERROR.to_owned(), Box::new(error))
}

/// [`check_decimal`] of a `sum`'s result.
fn check_sum(array: &ArrayRef) -> Result<()> {
    check_decimal(array).map_err(decimal_sum_overflow)
}

fn addition_overflow(
    data_type: &DataType,
    a: impl Display,
    b: impl Display,
) -> datafusion::error::DataFusionError {
    if super::arith::is_decimal(data_type) {
        decimal_sum_overflow(overflow("Decimal overflow"))
    } else {
        overflow(format!(
            "{} addition overflow: {a} + {b}",
            sql_type_name(data_type)
        ))
    }
}

fn subtraction_overflow(
    data_type: &DataType,
    a: impl Display,
    b: impl Display,
) -> datafusion::error::DataFusionError {
    if super::arith::is_decimal(data_type) {
        decimal_sum_overflow(overflow("Decimal overflow"))
    } else {
        overflow(format!(
            "{} subtraction overflow: {a} - {b}",
            sql_type_name(data_type)
        ))
    }
}

/// Checked sum of the valid values of `array`.
fn sum_of<T>(array: &PrimitiveArray<T>, data_type: &DataType) -> Result<Option<T::Native>>
where
    T: ArrowPrimitiveType,
    T::Native: ArrowNativeTypeOp + Display,
{
    let mut total: Option<T::Native> = None;
    for value in array.iter().flatten() {
        total = Some(match total {
            None => value,
            Some(sum) => sum
                .add_checked(value)
                .map_err(|_| addition_overflow(data_type, sum, value))?,
        });
    }
    Ok(total)
}

fn checked_result<T: ArrowPrimitiveType>(
    value: Option<T::Native>,
    data_type: &DataType,
) -> Result<ScalarValue> {
    let array: ArrayRef = Arc::new(PrimitiveArray::<T>::from_iter([value]).with_data_type(data_type.clone()));
    check_sum(&array)?;
    ScalarValue::try_from_array(&array, 0)
}

#[derive(Debug)]
struct CheckedSumAccumulator<T: ArrowPrimitiveType> {
    sum: Option<T::Native>,
    data_type: DataType,
}

impl<T> Accumulator for CheckedSumAccumulator<T>
where
    T: ArrowPrimitiveType + std::fmt::Debug,
    T::Native: ArrowNativeTypeOp + Display,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(batch) = sum_of(values[0].as_primitive::<T>(), &self.data_type)? else {
            return Ok(());
        };
        self.sum = Some(match self.sum {
            None => batch,
            Some(sum) => sum
                .add_checked(batch)
                .map_err(|_| addition_overflow(&self.data_type, sum, batch))?,
        });
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::new_primitive::<T>(self.sum, &self.data_type)?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        checked_result::<T>(self.sum, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

/// Window frames that slide: values leave the frame through `retract_batch`.
#[derive(Debug)]
struct CheckedSlidingSum<T: ArrowPrimitiveType> {
    sum: T::Native,
    count: u64,
    data_type: DataType,
}

impl<T> Accumulator for CheckedSlidingSum<T>
where
    T: ArrowPrimitiveType + std::fmt::Debug,
    T::Native: ArrowNativeTypeOp + Display,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .add_checked(value)
                .map_err(|_| addition_overflow(&self.data_type, self.sum, value))?;
            self.count += 1;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .sub_checked(value)
                .map_err(|_| subtraction_overflow(&self.data_type, self.sum, value))?;
            self.count -= 1;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = states[0].as_primitive::<T>();
        if let Some(batch) = sum_of(sums, &self.data_type)? {
            self.sum = self
                .sum
                .add_checked(batch)
                .map_err(|_| addition_overflow(&self.data_type, self.sum, batch))?;
        }
        let counts = states[1].as_primitive::<UInt64Type>();
        self.count += counts.iter().flatten().sum::<u64>();
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, self.count.into()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        checked_result::<T>((self.count != 0).then_some(self.sum), &self.data_type)
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

/// `sum(DISTINCT x)`: `DataFusion`'s accumulator collects the distinct values;
/// only the final addition is replaced.
#[derive(Debug)]
struct CheckedDistinctSum<T: ArrowPrimitiveType> {
    inner: Box<dyn Accumulator>,
    data_type: DataType,
    _type: std::marker::PhantomData<fn() -> T>,
}

impl<T> Accumulator for CheckedDistinctSum<T>
where
    T: ArrowPrimitiveType + std::fmt::Debug,
    T::Native: ArrowNativeTypeOp + Display,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut total: Option<T::Native> = None;
        for state in self.inner.state()? {
            let ScalarValue::List(list) = state else {
                continue;
            };
            for values in list.iter().flatten() {
                if let Some(batch) = sum_of(values.as_primitive::<T>(), &self.data_type)? {
                    total = Some(match total {
                        None => batch,
                        Some(sum) => sum
                            .add_checked(batch)
                            .map_err(|_| addition_overflow(&self.data_type, sum, batch))?,
                    });
                }
            }
        }
        checked_result::<T>(total, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.inner.size()
    }
}

/// Grouped sums: one slot per group, `seen` marks groups with a value.
#[derive(Debug)]
struct CheckedSumGroups<T: ArrowPrimitiveType> {
    sums: Vec<T::Native>,
    seen: Vec<bool>,
    data_type: DataType,
}

impl<T> CheckedSumGroups<T>
where
    T: ArrowPrimitiveType + std::fmt::Debug,
    T::Native: ArrowNativeTypeOp + Display,
{
    fn add(
        &mut self,
        values: &ArrayRef,
        group_indices: &[usize],
        filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.sums.resize(total_num_groups, T::Native::ZERO);
        self.seen.resize(total_num_groups, false);
        let values = values.as_primitive::<T>();
        for (row, &group) in group_indices.iter().enumerate() {
            if values.is_null(row) || filter.is_some_and(|f| !f.is_valid(row) || !f.value(row)) {
                continue;
            }
            let (sum, value) = (self.sums[group], values.value(row));
            self.sums[group] = if self.seen[group] {
                sum.add_checked(value)
                    .map_err(|_| addition_overflow(&self.data_type, sum, value))?
            } else {
                value
            };
            self.seen[group] = true;
        }
        Ok(())
    }

    fn emit(&mut self, emit_to: EmitTo) -> ArrayRef {
        let sums = emit_to.take_needed(&mut self.sums);
        let seen = emit_to.take_needed(&mut self.seen);
        let nulls = NullBuffer::new(BooleanBuffer::from(seen));
        let array = PrimitiveArray::<T>::new(sums.into(), Some(nulls)).with_data_type(self.data_type.clone());
        Arc::new(array)
    }
}

impl<T> GroupsAccumulator for CheckedSumGroups<T>
where
    T: ArrowPrimitiveType + std::fmt::Debug + Send,
    T::Native: ArrowNativeTypeOp + Display,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.add(&values[0], group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.add(&values[0], group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let array = self.emit(emit_to);
        check_sum(&array)?;
        Ok(array)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.emit(emit_to)])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * size_of::<T::Native>() + self.seen.capacity()
    }
}

macro_rules! by_sum_type {
    ($data_type:expr, $build:ident, $fallback:expr) => {
        match $data_type {
            DataType::Int64 => $build!(Int64Type),
            DataType::UInt64 => $build!(UInt64Type),
            DataType::Decimal32(..) => $build!(Decimal32Type),
            DataType::Decimal64(..) => $build!(Decimal64Type),
            DataType::Decimal128(..) => $build!(Decimal128Type),
            DataType::Decimal256(..) => $build!(Decimal256Type),
            _ => $fallback,
        }
    };
}

/// Result types whose addition this module checks (floats and durations keep
/// `DataFusion`'s accumulators).
pub(super) fn is_checked_sum_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int64
            | DataType::UInt64
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

/// `sum` with checked addition; see the module documentation.
#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub(crate) struct CheckedSum {
    inner: Sum,
    /// Trino's result type for `decimal(p,s)`: `decimal(38,s)`.
    trino_decimal: bool,
}

impl CheckedSum {
    /// `sum` typed like Trino: `decimal(p,s)` sums to `decimal(38,s)`.
    pub(crate) fn trino() -> Self {
        Self {
            inner: Sum::new(),
            trino_decimal: true,
        }
    }
}

impl AggregateUDFImpl for CheckedSum {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Decimal128(_, s)) if self.trino_decimal && *s >= 0 => {
                Ok(DataType::Decimal128(super::decimal::MAX_PRECISION, *s))
            }
            _ => self.inner.return_type(arg_types),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let data_type = args.return_type().clone();
        if args.is_distinct {
            let inner = self.inner.accumulator(args)?;
            macro_rules! distinct {
                ($t:ty) => {
                    Ok(Box::new(CheckedDistinctSum::<$t> {
                        inner,
                        data_type,
                        _type: std::marker::PhantomData,
                    }))
                };
            }
            return by_sum_type!(&data_type, distinct, Ok(inner));
        }
        macro_rules! plain {
            ($t:ty) => {
                Ok(Box::new(CheckedSumAccumulator::<$t> {
                    sum: None,
                    data_type: data_type.clone(),
                }))
            };
        }
        by_sum_type!(&data_type, plain, self.inner.accumulator(args))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        let data_type = args.return_type().clone();
        macro_rules! groups {
            ($t:ty) => {
                Ok(Box::new(CheckedSumGroups::<$t> {
                    sums: Vec::new(),
                    seen: Vec::new(),
                    data_type: data_type.clone(),
                }))
            };
        }
        by_sum_type!(&data_type, groups, self.inner.create_groups_accumulator(args))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let data_type = args.return_type().clone();
        if args.is_distinct {
            return self.inner.create_sliding_accumulator(args);
        }
        macro_rules! sliding {
            ($t:ty) => {
                Ok(Box::new(CheckedSlidingSum::<$t> {
                    sum: <$t as ArrowPrimitiveType>::Native::ZERO,
                    count: 0,
                    data_type: data_type.clone(),
                }))
            };
        }
        by_sum_type!(&data_type, sliding, self.inner.create_sliding_accumulator(args))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }
}

#[cfg(feature = "portable")]
impl CheckedSum {
    /// Whether this is [`CheckedSum::trino`] (`crate::portable`).
    pub(crate) fn is_trino(&self) -> bool {
        self.trino_decimal
    }
}
