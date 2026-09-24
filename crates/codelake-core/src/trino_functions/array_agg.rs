//! `array_agg(x [ORDER BY …])` with Trino semantics.
//!
//! `DataFusion`'s `array_agg` trusts the input order when the planner says it
//! is sorted, and "reverses" an aggregate whose `ORDER BY` is the opposite of
//! another one's. With two orderings in one query (`array_agg(x ORDER BY x)`
//! next to `array_agg(x ORDER BY x DESC)`, or a `listagg … WITHIN GROUP`
//! next to an `array_agg`), both come out in the same order: a wrong answer
//! without any error. This version never relies on the input order: it keeps
//! the ordering keys next to the values and sorts them itself (a stable sort,
//! so equal keys keep their arrival order), and it cannot be reversed.
//!
//! It also retracts rows, so it works over sliding window frames
//! (`ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`).
//!
//! Memory: the buffered values and keys are reported through
//! `Accumulator::size`, which the aggregation operator reserves from the pool.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, ListArray, UInt32Array};
use datafusion::arrow::compute::{SortOptions, concat, filter, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::functions_aggregate::array_agg::array_agg_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};

use super::aggregate::{state_field, udaf_boilerplate};

fn list_of(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(element, true)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrayAgg {
    signature: Signature,
}

impl AggregateUDFImpl for ArrayAgg {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "array_agg"
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let element = arg_types
            .first()
            .ok_or_else(|| DataFusionError::Plan("array_agg needs one argument".into()))?;
        Ok(list_of(element.clone()))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let element = args
            .input_fields
            .first()
            .ok_or_else(|| DataFusionError::Internal("array_agg state".into()))?;
        let mut fields = vec![state_field(
            args.name,
            "values",
            list_of(element.data_type().clone()),
        )];
        for (i, key) in args.ordering_fields.iter().enumerate() {
            fields.push(state_field(
                args.name,
                &format!("key{i}"),
                list_of(key.data_type().clone()),
            ));
        }
        Ok(fields)
    }

    /// The accumulator sorts by itself: the input order only matters for
    /// speed, and a "reversed" copy of the aggregate never exists.
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Beneficial
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(self))
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let element = args
            .expr_fields
            .first()
            .ok_or_else(|| DataFusionError::Internal("array_agg argument".into()))?;
        let keys = args
            .order_bys
            .iter()
            .map(|sort| Ok((sort.expr.data_type(args.schema)?, sort.options)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(ArrayAggAccumulator::new(
            element.data_type().clone(),
            keys,
            args.is_distinct,
            args.ignore_nulls && element.is_nullable(),
        )))
    }

    /// Without `ORDER BY` and `DISTINCT`, the engine's grouped accumulator
    /// (insertion order, as here) is used for `GROUP BY`.
    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        !args.is_distinct && args.order_bys.is_empty()
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        array_agg_udaf().inner().create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }
}

/// Values of one update or merge, with their ordering keys.
#[derive(Debug)]
struct Chunk {
    values: ArrayRef,
    keys: Vec<ArrayRef>,
}

impl Chunk {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn slice(&self, offset: usize) -> Self {
        let length = self.len() - offset;
        Self {
            values: self.values.slice(offset, length),
            keys: self.keys.iter().map(|k| k.slice(offset, length)).collect(),
        }
    }

    fn bytes(&self) -> usize {
        std::iter::once(&self.values)
            .chain(&self.keys)
            .map(|a| a.to_data().get_slice_memory_size().unwrap_or_default())
            .sum()
    }
}

#[derive(Debug)]
struct ArrayAggAccumulator {
    element: DataType,
    keys: Vec<(DataType, SortOptions)>,
    distinct: bool,
    ignore_nulls: bool,
    /// In arrival order; `skip` rows of the first chunk were retracted.
    chunks: VecDeque<Chunk>,
    skip: usize,
    bytes: usize,
}

impl ArrayAggAccumulator {
    fn new(
        element: DataType,
        keys: Vec<(DataType, SortOptions)>,
        distinct: bool,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            element,
            keys,
            distinct,
            ignore_nulls,
            chunks: VecDeque::new(),
            skip: 0,
            bytes: 0,
        }
    }

    fn rows(&self) -> usize {
        self.chunks.iter().map(Chunk::len).sum::<usize>() - self.skip
    }

    fn push(&mut self, chunk: Chunk) {
        if chunk.len() > 0 {
            self.bytes += chunk.bytes();
            self.chunks.push_back(chunk);
        }
    }

    /// Input columns of one update: the value and the ordering keys, without
    /// the NULL values when `IGNORE NULLS` applies.
    fn chunk(&self, values: &[ArrayRef]) -> Result<Chunk> {
        let Some((value, keys)) = values.split_first() else {
            return Err(DataFusionError::Internal("array_agg arguments".into()));
        };
        if keys.len() < self.keys.len() {
            return Err(DataFusionError::NotImplemented(
                "NOT_SUPPORTED: array_agg(… ORDER BY …) is not supported as a window function".into(),
            ));
        }
        let keys = &keys[..self.keys.len()];
        if self.ignore_nulls && value.null_count() > 0 {
            let valid = BooleanArray::from_iter((0..value.len()).map(|i| Some(value.is_valid(i))));
            return Ok(Chunk {
                values: filter(value, &valid)?,
                keys: keys
                    .iter()
                    .map(|k| Ok(filter(k, &valid)?))
                    .collect::<Result<_>>()?,
            });
        }
        Ok(Chunk {
            values: Arc::clone(value),
            keys: keys.to_vec(),
        })
    }

    /// Every buffered row as one chunk (and the buffer compacted to it).
    fn compact(&mut self) -> Result<Option<&Chunk>> {
        if self.rows() == 0 {
            return Ok(None);
        }
        if self.chunks.len() > 1 || self.skip > 0 {
            let parts: Vec<Chunk> = self
                .chunks
                .iter()
                .enumerate()
                .map(|(i, c)| if i == 0 { c.slice(self.skip) } else { c.slice(0) })
                .collect();
            let column = |pick: &dyn Fn(&Chunk) -> &ArrayRef| -> Result<ArrayRef> {
                let arrays: Vec<&dyn Array> = parts.iter().map(|c| pick(c).as_ref()).collect();
                Ok(concat(&arrays)?)
            };
            let values = column(&|c| &c.values)?;
            let keys = (0..self.keys.len())
                .map(|k| column(&|c| &c.keys[k]))
                .collect::<Result<Vec<_>>>()?;
            let merged = Chunk { values, keys };
            self.bytes = merged.bytes();
            self.chunks = VecDeque::from([merged]);
            self.skip = 0;
        }
        Ok(self.chunks.front())
    }

    /// The values in `ORDER BY` order (stable), without duplicates for
    /// `DISTINCT`.
    fn ordered(&mut self) -> Result<Option<ArrayRef>> {
        let options: Vec<SortOptions> = self.keys.iter().map(|(_, o)| *o).collect();
        let fields: Vec<SortField> = self
            .keys
            .iter()
            .map(|(t, o)| SortField::new_with_options(t.clone(), *o))
            .collect();
        let distinct = self.distinct;
        let Some(chunk) = self.compact()? else {
            return Ok(None);
        };
        let mut values = Arc::clone(&chunk.values);
        if !options.is_empty() {
            let converter = RowConverter::new(fields)?;
            let rows = converter.convert_columns(&chunk.keys)?;
            let mut order: Vec<u32> = (0..u32::try_from(values.len())
                .map_err(|_| DataFusionError::Execution("array_agg: too many values".into()))?)
                .collect();
            order.sort_by(|a, b| rows.row(*a as usize).cmp(&rows.row(*b as usize)));
            values = take(values.as_ref(), &UInt32Array::from(order), None)?;
        }
        if distinct {
            let converter = RowConverter::new(vec![SortField::new(values.data_type().clone())])?;
            let rows = converter.convert_columns(&[Arc::clone(&values)])?;
            let mut seen = HashSet::new();
            let keep: Vec<u32> = (0..values.len())
                .filter(|i| seen.insert(rows.row(*i).as_ref().to_vec()))
                .filter_map(|i| u32::try_from(i).ok())
                .collect();
            values = take(values.as_ref(), &UInt32Array::from(keep), None)?;
        }
        Ok(Some(values))
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let chunk = self.chunk(values)?;
        self.push(chunk);
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let mut remaining = self.chunk(values)?.len();
        while remaining > 0 {
            let Some(front) = self.chunks.front() else {
                return Err(DataFusionError::Internal(
                    "array_agg: retracting unseen rows".into(),
                ));
            };
            let available = front.len() - self.skip;
            if remaining >= available {
                remaining -= available;
                self.bytes = self.bytes.saturating_sub(front.bytes());
                self.chunks.pop_front();
                self.skip = 0;
            } else {
                self.skip += remaining;
                remaining = 0;
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some((values, keys)) = states.split_first() else {
            return Err(DataFusionError::Internal("array_agg state".into()));
        };
        let values = values.as_list::<i32>();
        let keys: Vec<&ListArray> = keys.iter().map(AsArray::as_list).collect();
        for row in 0..values.len() {
            if values.is_null(row) {
                continue;
            }
            let chunk = Chunk {
                values: values.value(row),
                keys: keys.iter().map(|k| k.value(row)).collect(),
            };
            self.push(chunk);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let key_types: Vec<DataType> = self.keys.iter().map(|(t, _)| t.clone()).collect();
        let element = self.element.clone();
        let Some(chunk) = self.compact()? else {
            let mut state = vec![ScalarValue::new_null_list(element, true, 1)];
            state.extend(
                key_types
                    .into_iter()
                    .map(|t| ScalarValue::new_null_list(t, true, 1)),
            );
            return Ok(state);
        };
        Ok(std::iter::once(&chunk.values)
            .chain(&chunk.keys)
            .map(|a| SingleRowListArrayBuilder::new(Arc::clone(a)).build_list_scalar())
            .collect())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.ordered()? {
            None => Ok(ScalarValue::new_null_list(self.element.clone(), true, 1)),
            Some(values) => Ok(SingleRowListArrayBuilder::new(values).build_list_scalar()),
        }
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.bytes + self.chunks.capacity() * size_of::<Chunk>()
    }
}

pub(crate) fn udafs() -> Vec<AggregateUDF> {
    vec![AggregateUDF::from(ArrayAgg {
        signature: Signature::any(1, Volatility::Immutable),
    })]
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn list(value: &ScalarValue) -> Vec<Option<i64>> {
        let ScalarValue::List(list) = value else {
            panic!("list expected: {value:?}")
        };
        list.value(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .iter()
            .collect()
    }

    fn descending() -> SortOptions {
        SortOptions {
            descending: true,
            nulls_first: false,
        }
    }

    #[test]
    fn sorts_by_its_own_keys_whatever_the_input_order() {
        let mut acc = ArrayAggAccumulator::new(
            DataType::Int64,
            vec![(DataType::Int64, descending())],
            false,
            false,
        );
        acc.update_batch(&[ints(&[Some(1), Some(3)]), ints(&[Some(1), Some(3)])])
            .unwrap();
        acc.update_batch(&[ints(&[None, Some(2)]), ints(&[None, Some(2)])])
            .unwrap();
        assert_eq!(
            list(&acc.evaluate().unwrap()),
            vec![Some(3), Some(2), Some(1), None]
        );
    }

    #[test]
    fn state_merges_with_keys() {
        let mut a = ArrayAggAccumulator::new(
            DataType::Int64,
            vec![(DataType::Int64, SortOptions::default())],
            false,
            false,
        );
        a.update_batch(&[ints(&[Some(10), Some(30)]), ints(&[Some(1), Some(3)])])
            .unwrap();
        let state: Vec<ArrayRef> = a.state().unwrap().iter().map(|s| s.to_array().unwrap()).collect();
        let mut b = ArrayAggAccumulator::new(
            DataType::Int64,
            vec![(DataType::Int64, SortOptions::default())],
            false,
            false,
        );
        b.update_batch(&[ints(&[Some(20)]), ints(&[Some(2)])]).unwrap();
        b.merge_batch(&state).unwrap();
        assert_eq!(list(&b.evaluate().unwrap()), vec![Some(10), Some(20), Some(30)]);
    }

    #[test]
    fn retracts_the_oldest_rows() {
        let mut acc = ArrayAggAccumulator::new(DataType::Int64, vec![], false, false);
        acc.update_batch(&[ints(&[Some(1), Some(2)])]).unwrap();
        acc.update_batch(&[ints(&[Some(3)])]).unwrap();
        acc.retract_batch(&[ints(&[Some(1)])]).unwrap();
        assert_eq!(list(&acc.evaluate().unwrap()), vec![Some(2), Some(3)]);
        acc.retract_batch(&[ints(&[Some(2), Some(3)])]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::new_null_list(DataType::Int64, true, 1)
        );
    }

    #[test]
    fn distinct_keeps_the_first_of_each_value() {
        let mut acc = ArrayAggAccumulator::new(
            DataType::Int64,
            vec![(DataType::Int64, descending())],
            true,
            false,
        );
        acc.update_batch(&[
            ints(&[Some(1), Some(2), Some(1)]),
            ints(&[Some(1), Some(2), Some(1)]),
        ])
        .unwrap();
        assert_eq!(list(&acc.evaluate().unwrap()), vec![Some(2), Some(1)]);
    }
}
