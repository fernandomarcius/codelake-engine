//! `avg` over decimals with Trino's type and rounding.
//!
//! Trino 431 answers `avg(decimal(p,s))` as `decimal(p,s)`, rounded half away
//! from zero, and adds the values without ever overflowing. `DataFusion`
//! answers `decimal(p+4,s+4)` truncated, and adds in 128 bits with wrapping
//! arithmetic. [`TrinoAvg`] keeps `DataFusion`'s accumulators but feeds them
//! values they cannot overflow and rounds their `s+4`-digit, truncated result
//! to `s` digits. That rounding is exact: half a unit of the `s`-th digit is
//! representable with `s+4` digits, so truncating first never crosses it.
//!
//! The values stay in 128 bits (`decimal(38,s)`, the input's own buffer) when
//! `p <= 19`: a sum of fewer than 2^63 of them is below 10^19 · 2^63 < 2^127,
//! and scaling it by 10^4 for the result stays in range below ~10^15 rows.
//! That covers money columns (`decimal(15,2)` and the like); wider decimals go
//! to `decimal(76,s)` (256 bits), which costs a cast of every value and
//! arithmetic about four times slower. Other argument types are `DataFusion`'s
//! `avg` unchanged.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, Decimal128Type, FieldRef,
};
use datafusion::common::{Result, ScalarValue};
use datafusion::functions_aggregate::average::Avg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, SetMonotonicity, Signature,
};

use super::decimal::MAX_PRECISION;

/// `(p, s)` of a decimal argument Trino averages as `decimal(p,s)`.
fn decimal_argument(data_type: &DataType) -> Option<(u8, i8)> {
    match *data_type {
        DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s)
            if s >= 0 && p <= MAX_PRECISION =>
        {
            Some((p, s))
        }
        _ => None,
    }
}

/// Types the wrapped `DataFusion` accumulator works with.
#[derive(Debug, Clone)]
struct Widened {
    /// `decimal(76,s)`: what the values are cast to.
    input: DataType,
    /// `decimal(76,s+4)`: `DataFusion`'s result for that input.
    inner_result: DataType,
    /// `decimal(p,s)`: Trino's result.
    result: DataType,
}

/// Widest precision whose sums stay in 128 bits (see the module documentation).
const NARROW_PRECISION: u8 = 19;

impl Widened {
    fn new(p: u8, s: i8) -> Self {
        let max128 = i8::try_from(DECIMAL128_MAX_PRECISION).unwrap_or(i8::MAX);
        if p <= NARROW_PRECISION && s + 4 <= max128 {
            return Self {
                input: DataType::Decimal128(DECIMAL128_MAX_PRECISION, s),
                inner_result: DataType::Decimal128(DECIMAL128_MAX_PRECISION, s + 4),
                result: DataType::Decimal128(p, s),
            };
        }
        let max = i8::try_from(DECIMAL256_MAX_PRECISION).unwrap_or(i8::MAX);
        Self {
            input: DataType::Decimal256(DECIMAL256_MAX_PRECISION, s),
            inner_result: DataType::Decimal256(DECIMAL256_MAX_PRECISION, (s + 4).min(max)),
            result: DataType::Decimal128(p, s),
        }
    }

    fn of(data_type: &DataType) -> Option<Self> {
        decimal_argument(data_type).map(|(p, s)| Self::new(p, s))
    }

    fn input_fields(&self, fields: &[FieldRef]) -> Vec<FieldRef> {
        fields
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_data_type(self.input.clone())))
            .collect()
    }

    fn return_field(&self, field: &FieldRef) -> FieldRef {
        Arc::new(field.as_ref().clone().with_data_type(self.inner_result.clone()))
    }

    fn cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
        let options = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        Ok(cast_with_options(array, to, &options)?)
    }

    fn widen(&self, values: &[ArrayRef]) -> Result<Vec<ArrayRef>> {
        values.iter().map(|v| self.widen_one(v)).collect()
    }

    /// `value` as [`Widened::input`]; a `decimal128` of the same scale only
    /// gets the wider precision, sharing its buffer (every value fits).
    fn widen_one(&self, value: &ArrayRef) -> Result<ArrayRef> {
        if let (DataType::Decimal128(_, from), DataType::Decimal128(p, to)) = (value.data_type(), &self.input)
            && from == to
        {
            let decimals = value.as_primitive::<Decimal128Type>().clone();
            return Ok(Arc::new(decimals.with_precision_and_scale(*p, *to)?));
        }
        Self::cast(value, &self.input)
    }

    /// Rounds the inner result half away from zero to Trino's type.
    fn round(&self, array: &ArrayRef) -> Result<ArrayRef> {
        Self::cast(array, &self.result)
    }
}

/// Calls `f` with `args` rewritten for the wrapped accumulator.
fn with_widened<T>(
    args: &AccumulatorArgs<'_>,
    widened: &Widened,
    f: impl FnOnce(AccumulatorArgs<'_>) -> Result<T>,
) -> Result<T> {
    let expr_fields = widened.input_fields(args.expr_fields);
    f(AccumulatorArgs {
        return_field: widened.return_field(&args.return_field),
        schema: args.schema,
        ignore_nulls: args.ignore_nulls,
        order_bys: args.order_bys,
        is_reversed: args.is_reversed,
        name: args.name,
        is_distinct: args.is_distinct,
        exprs: args.exprs,
        expr_fields: &expr_fields,
    })
}

#[derive(Debug)]
struct RoundedAvg {
    inner: Box<dyn Accumulator>,
    widened: Widened,
}

impl Accumulator for RoundedAvg {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(&self.widened.widen(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.inner.evaluate()?.to_array()?;
        ScalarValue::try_from_array(&self.widened.round(&value)?, 0)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.retract_batch(&self.widened.widen(values)?)
    }

    fn supports_retract_batch(&self) -> bool {
        self.inner.supports_retract_batch()
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.inner.size()
    }
}

struct RoundedAvgGroups {
    inner: Box<dyn GroupsAccumulator>,
    widened: Widened,
}

impl std::fmt::Debug for RoundedAvgGroups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoundedAvgGroups")
            .field("widened", &self.widened)
            .finish_non_exhaustive()
    }
}

impl GroupsAccumulator for RoundedAvgGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.inner.update_batch(
            &self.widened.widen(values)?,
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.widened.round(&self.inner.evaluate(emit_to)?)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.inner.state(emit_to)
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

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        self.inner
            .convert_to_state(&self.widened.widen(values)?, opt_filter)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.inner.supports_convert_to_state()
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.inner.size()
    }
}

/// `avg` with Trino's decimal semantics; see the module documentation. Keeps
/// the name `avg`, so plans and pushed-down SQL do not change.
#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub(crate) struct TrinoAvg {
    inner: Avg,
}

impl TrinoAvg {
    fn widened(args: &AccumulatorArgs<'_>) -> Option<Widened> {
        args.expr_fields.first().and_then(|f| Widened::of(f.data_type()))
    }
}

impl AggregateUDFImpl for TrinoAvg {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first().and_then(decimal_argument) {
            Some((p, s)) => Ok(DataType::Decimal128(p, s)),
            None => self.inner.return_type(arg_types),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let Some(widened) = Self::widened(&args) else {
            return self.inner.accumulator(args);
        };
        let inner = with_widened(&args, &widened, |a| self.inner.accumulator(a))?;
        Ok(Box::new(RoundedAvg { inner, widened }))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let Some(widened) = args.input_fields.first().and_then(|f| Widened::of(f.data_type())) else {
            return self.inner.state_fields(args);
        };
        let input_fields = widened.input_fields(args.input_fields);
        self.inner.state_fields(StateFieldsArgs {
            name: args.name,
            input_fields: &input_fields,
            return_field: widened.return_field(&args.return_field),
            ordering_fields: args.ordering_fields,
            is_distinct: args.is_distinct,
        })
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        match Self::widened(&args) {
            Some(widened) => with_widened(&args, &widened, |a| {
                Ok(self.inner.groups_accumulator_supported(a))
            })
            .unwrap_or(false),
            None => self.inner.groups_accumulator_supported(args),
        }
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        let Some(widened) = Self::widened(&args) else {
            return self.inner.create_groups_accumulator(args);
        };
        let inner = with_widened(&args, &widened, |a| self.inner.create_groups_accumulator(a))?;
        Ok(Box::new(RoundedAvgGroups { inner, widened }))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let Some(widened) = Self::widened(&args) else {
            return self.inner.create_sliding_accumulator(args);
        };
        let inner = with_widened(&args, &widened, |a| self.inner.create_sliding_accumulator(a))?;
        Ok(Box::new(RoundedAvg { inner, widened }))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_arguments_keep_their_type() {
        let avg = TrinoAvg::default();
        assert_eq!(
            avg.return_type(&[DataType::Decimal128(15, 2)]).unwrap(),
            DataType::Decimal128(15, 2)
        );
        assert_eq!(
            avg.return_type(&[DataType::Decimal64(3, 0)]).unwrap(),
            DataType::Decimal128(3, 0)
        );
        assert_eq!(avg.return_type(&[DataType::Float64]).unwrap(), DataType::Float64);
        let widened = Widened::new(38, 38);
        assert_eq!(widened.inner_result, DataType::Decimal256(76, 42));
        // Money columns stay in 128 bits.
        let narrow = Widened::new(15, 2);
        assert_eq!(narrow.input, DataType::Decimal128(38, 2));
        assert_eq!(narrow.inner_result, DataType::Decimal128(38, 6));
        assert_eq!(Widened::new(20, 2).input, DataType::Decimal256(76, 2));
    }

    /// The 128-bit path gives Trino's answer: rounded half away from zero, on
    /// the input's buffer, with and without groups.
    #[tokio::test]
    async fn narrow_decimals_average_in_128_bits_with_trino_rounding() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        ctx.register_udaf(datafusion::logical_expr::AggregateUDF::new_from_impl(
            TrinoAvg::default(),
        ));
        let sql = "SELECT g, avg(v) AS a FROM (VALUES (1, CAST(0.05 AS DECIMAL(15,2))), (1, CAST(0.10 AS DECIMAL(15,2))), \
                   (2, CAST(-0.05 AS DECIMAL(15,2))), (2, CAST(-0.10 AS DECIMAL(15,2))), (3, CAST(999999999999.99 AS DECIMAL(15,2)))) t(g, v) \
                   GROUP BY g ORDER BY g";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let shown = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        // 0.075 → 0.08, -0.075 → -0.08 (half away from zero), type decimal(15,2).
        assert!(shown.contains("| 1 | 0.08            |"), "{shown}");
        assert!(shown.contains("| 2 | -0.08           |"), "{shown}");
        assert!(shown.contains("| 3 | 999999999999.99 |"), "{shown}");
        assert_eq!(
            batches[0].schema().field(1).data_type(),
            &DataType::Decimal128(15, 2)
        );
        let whole = ctx
            .sql("SELECT avg(v) AS a FROM (VALUES (CAST(1.00 AS DECIMAL(15,2))), (CAST(2.01 AS DECIMAL(15,2)))) t(v)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let shown = datafusion::arrow::util::pretty::pretty_format_batches(&whole)
            .unwrap()
            .to_string();
        assert!(shown.contains("| 1.51 |"), "{shown}");
    }
}
