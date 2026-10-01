//! Aggregate functions (Trino "Aggregate functions").
//!
//! Memory: every accumulator reports its real size through
//! `Accumulator::size`, which the aggregation operator reserves from the
//! memory pool, so buffered state (e.g. `approx_percentile`'s values) counts
//! against the query's limit and spills or fails like any other operator.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    LargeStringArray, ListArray, StringArray, StringViewArray, StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};

use super::aggregate_types::{
    Retyped, RetypedGroups, engine_args, engine_state_args, fields, to_real, to_real_array,
};
use super::binary::xxhash64_bytes;
use super::sketch::{Hll, QuantileSketch, Run};
use super::support::{
    invalid_argument, is_decimal, is_float, is_integer, is_string, not_found, not_supported,
};

/// Trino's `ChecksumAggregationFunction.PRIME64`.
const PRIME64: u64 = 0x9E37_79B1_85EB_CA87;

fn hash_long(value: i64) -> u64 {
    xxhash64_bytes(&value.to_le_bytes())
}

fn u64_bits(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}

fn i64_bits(value: i64) -> u64 {
    u64::from_ne_bytes(value.to_ne_bytes())
}

fn downcast<T: 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| DataFusionError::Internal(format!("unexpected {}", array.data_type())))
}

fn longs(array: &ArrayRef) -> Result<Int64Array> {
    Ok(downcast::<Int64Array>(&cast(array, &DataType::Int64)?)?.clone())
}

fn per_row<T: Array + 'static>(array: &ArrayRef, f: impl Fn(&T, usize) -> u64) -> Result<Vec<Option<u64>>> {
    let typed = downcast::<T>(array)?;
    Ok((0..typed.len())
        .map(|i| (!typed.is_null(i)).then(|| f(typed, i)))
        .collect())
}

/// Trino's `xxhash64` operator of each value, as `checksum` and
/// `approx_distinct` use it.
pub(crate) fn hash_column(array: &ArrayRef) -> Result<Vec<Option<u64>>> {
    match array.data_type() {
        t if is_integer(t) || matches!(t, DataType::Date32) => {
            let values = longs(array)?;
            Ok((0..values.len())
                .map(|i| (!values.is_null(i)).then(|| hash_long(values.value(i))))
                .collect())
        }
        DataType::Boolean => per_row::<BooleanArray>(array, |a, i| hash_long(i64::from(a.value(i)))),
        DataType::Float64 => per_row::<Float64Array>(array, |a, i| hash_long(double_bits(a.value(i)))),
        DataType::Float32 => per_row::<Float32Array>(array, |a, i| hash_long(real_bits(a.value(i)))),
        // Trino: an array hashes its elements' `xxhash64`, a row its fields'
        // hash codes (`31 * h + element`, NULL as 0).
        DataType::List(_) | DataType::LargeList(_) => {
            let list = cast(
                array,
                &DataType::List(Arc::new(Field::new_list_field(
                    list_element(array.data_type()),
                    true,
                ))),
            )?;
            let list = downcast::<ListArray>(&list)?;
            (0..list.len())
                .map(|i| {
                    if list.is_null(i) {
                        return Ok(None);
                    }
                    Ok(Some(combine(0, &hash_column(&list.value(i))?)))
                })
                .collect()
        }
        DataType::Struct(_) => struct_hash_codes(array),
        DataType::Utf8 => per_row::<StringArray>(array, |a, i| xxhash64_bytes(a.value(i).as_bytes())),
        DataType::LargeUtf8 => {
            per_row::<LargeStringArray>(array, |a, i| xxhash64_bytes(a.value(i).as_bytes()))
        }
        DataType::Utf8View => per_row::<StringViewArray>(array, |a, i| xxhash64_bytes(a.value(i).as_bytes())),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_) => {
            per_row::<BinaryArray>(&cast(array, &DataType::Binary)?, |a, i| {
                xxhash64_bytes(a.value(i))
            })
        }
        DataType::Decimal128(precision, _) => {
            let short = *precision <= 18;
            per_row::<Decimal128Array>(array, |a, i| {
                let v = a.value(i);
                if short {
                    hash_long(i64::try_from(v).unwrap_or(0))
                } else {
                    let high = i64::try_from(v >> 64).unwrap_or(0);
                    let low = u64_bits(u64::try_from(v & i128::from(u64::MAX)).unwrap_or(0));
                    hash_long(high) ^ hash_long(low)
                }
            })
        }
        DataType::Timestamp(unit, zone) => {
            let values = longs(array)?;
            let unit = *unit;
            let zoned = zone.is_some();
            Ok((0..values.len())
                .map(|i| (!values.is_null(i)).then(|| timestamp_hash(values.value(i), unit, zoned)))
                .collect())
        }
        other => Err(DataFusionError::Plan(format!(
            "NOT_SUPPORTED: hashing values of type {other} is not supported"
        ))),
    }
}

/// `doubleToLongBits` with Trino's zero: `-0.0` hashes as `0.0`, every NaN
/// as the canonical NaN.
fn double_bits(v: f64) -> i64 {
    let bits = if v.is_nan() {
        0x7ff8_0000_0000_0000
    } else if v == 0.0 {
        0
    } else {
        v.to_bits()
    };
    u64_bits(bits)
}

/// `floatToIntBits` with Trino's zero (see `double_bits`).
fn real_bits(v: f32) -> i64 {
    let bits = if v.is_nan() {
        0x7fc0_0000
    } else if v == 0.0 {
        0
    } else {
        v.to_bits()
    };
    i64::from(i32::from_ne_bytes(bits.to_ne_bytes()))
}

fn list_element(t: &DataType) -> DataType {
    match t {
        DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
        other => other.clone(),
    }
}

/// `CombineHashFunction`: `31 * previous + value` over the elements, NULL as 0.
fn combine(start: u64, hashes: &[Option<u64>]) -> u64 {
    hashes
        .iter()
        .fold(start, |h, v| h.wrapping_mul(31).wrapping_add(v.unwrap_or(0)))
}

/// Trino's `AbstractLongType.hash` (the hash code of fixed-width values).
fn long_hash_code(value: i64) -> u64 {
    i64_bits(value)
        .wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
        .rotate_left(31)
        .wrapping_mul(PRIME64)
}

/// Trino's `HASH_CODE` operator (rows hash their fields with it).
fn hash_codes(array: &ArrayRef) -> Result<Vec<Option<u64>>> {
    match array.data_type() {
        t if is_integer(t) || matches!(t, DataType::Date32) => {
            let values = longs(array)?;
            Ok((0..values.len())
                .map(|i| (!values.is_null(i)).then(|| long_hash_code(values.value(i))))
                .collect())
        }
        DataType::Boolean => per_row::<BooleanArray>(array, |a, i| if a.value(i) { 1231 } else { 1237 }),
        DataType::Float64 => per_row::<Float64Array>(array, |a, i| long_hash_code(double_bits(a.value(i)))),
        DataType::Float32 => per_row::<Float32Array>(array, |a, i| long_hash_code(real_bits(a.value(i)))),
        DataType::Decimal128(precision, _) if *precision <= 18 => {
            per_row::<Decimal128Array>(array, |a, i| {
                long_hash_code(i64::try_from(a.value(i)).unwrap_or(0))
            })
        }
        t if is_string(t) || matches!(t, DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => {
            hash_column(array)
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let list = cast(
                array,
                &DataType::List(Arc::new(Field::new_list_field(
                    list_element(array.data_type()),
                    true,
                ))),
            )?;
            let list = downcast::<ListArray>(&list)?;
            (0..list.len())
                .map(|i| {
                    if list.is_null(i) {
                        return Ok(None);
                    }
                    Ok(Some(combine(0, &hash_codes(&list.value(i))?)))
                })
                .collect()
        }
        DataType::Struct(_) => struct_hash_codes(array),
        other => Err(DataFusionError::Plan(format!(
            "NOT_SUPPORTED: hashing values of type {other} inside a row is not supported"
        ))),
    }
}

fn struct_hash_codes(array: &ArrayRef) -> Result<Vec<Option<u64>>> {
    let rows = downcast::<StructArray>(array)?;
    let columns = rows
        .columns()
        .iter()
        .map(hash_codes)
        .collect::<Result<Vec<_>>>()?;
    Ok((0..rows.len())
        .map(|i| {
            (!rows.is_null(i)).then(|| {
                let fields: Vec<Option<u64>> = columns.iter().map(|c| c[i]).collect();
                combine(1, &fields)
            })
        })
        .collect())
}

/// Types `checksum` accepts: hashable values, arrays and rows of them.
fn checksum_type(t: &DataType) -> bool {
    match t {
        DataType::List(f) | DataType::LargeList(f) => checksum_type(f.data_type()),
        DataType::Struct(fields) => fields.iter().all(|f| checksum_type(f.data_type())),
        t => hashable(t),
    }
}

/// Trino stores `timestamp(p ≤ 6)` as epoch micros and `timestamp(p) with
/// time zone (p ≤ 3)` as epoch millis; longer precisions add a fraction.
fn timestamp_hash(raw: i64, unit: TimeUnit, zoned: bool) -> u64 {
    if zoned {
        let (millis, picos) = match unit {
            TimeUnit::Second => (raw.saturating_mul(1_000), 0),
            TimeUnit::Millisecond => (raw, 0),
            TimeUnit::Microsecond => (raw.div_euclid(1_000), raw.rem_euclid(1_000) * 1_000_000),
            TimeUnit::Nanosecond => (raw.div_euclid(1_000_000), raw.rem_euclid(1_000_000) * 1_000),
        };
        return if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
            hash_long(millis)
        } else {
            hash_long(millis) ^ hash_long(picos)
        };
    }
    match unit {
        TimeUnit::Second => hash_long(raw.saturating_mul(1_000_000)),
        TimeUnit::Millisecond => hash_long(raw.saturating_mul(1_000)),
        TimeUnit::Microsecond => hash_long(raw),
        TimeUnit::Nanosecond => hash_long(raw.div_euclid(1_000)) ^ hash_long(raw.rem_euclid(1_000) * 1_000),
    }
}

pub(crate) fn hashable(t: &DataType) -> bool {
    is_integer(t)
        || is_float(t)
        || is_string(t)
        || matches!(
            t,
            DataType::Boolean
                | DataType::Date32
                | DataType::Decimal128(..)
                | DataType::Timestamp(..)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
                | DataType::Null
        )
}

pub(crate) fn state_field(name: &str, field: &str, data_type: DataType) -> FieldRef {
    Arc::new(Field::new(format!("{name}[{field}]"), data_type, true))
}

macro_rules! udaf_boilerplate {
    () => {
        fn signature(&self) -> &Signature {
            &self.signature
        }
    };
}
pub(crate) use udaf_boilerplate;

// ---------------------------------------------------------------------------
// checksum
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Checksum {
    signature: Signature,
}

impl AggregateUDFImpl for Checksum {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "checksum"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] if checksum_type(t) => Ok(vec![if t.is_null() { DataType::Utf8 } else { t.clone() }]),
            _ => Err(not_found("checksum", arg_types)),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![
            state_field(args.name, "sum", DataType::Int64),
            state_field(args.name, "seen", DataType::Boolean),
        ])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ChecksumAccumulator::default()))
    }
}

#[derive(Debug, Default)]
struct ChecksumAccumulator {
    sum: u64,
    seen: bool,
}

impl Accumulator for ChecksumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return Ok(());
        };
        for hash in hash_column(values)? {
            self.seen = true;
            let term = hash.map_or(PRIME64, |h| h.wrapping_mul(PRIME64));
            self.sum = self.sum.wrapping_add(term);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [sums, seen] = states else {
            return Err(DataFusionError::Internal("checksum state".into()));
        };
        let sums = downcast::<Int64Array>(sums)?;
        let seen = downcast::<BooleanArray>(seen)?;
        for i in 0..sums.len() {
            if !seen.is_null(i) && seen.value(i) {
                self.seen = true;
                self.sum = self.sum.wrapping_add(i64_bits(sums.value(i)));
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Int64(Some(u64_bits(self.sum))),
            ScalarValue::Boolean(Some(self.seen)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(
            self.seen.then(|| self.sum.to_le_bytes().to_vec()),
        ))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

// ---------------------------------------------------------------------------
// max_by / min_by
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct By {
    signature: Signature,
    name: &'static str,
    max: bool,
}

/// Types with an order: the scalars, and rows and arrays of them (compared
/// field by field and element by element, see [`key_order`]); maps have none.
pub(crate) fn orderable(t: &DataType) -> bool {
    match t {
        DataType::Struct(fields) => fields.iter().all(|f| orderable(f.data_type())),
        DataType::List(element) | DataType::LargeList(element) | DataType::FixedSizeList(element, _) => {
            orderable(element.data_type())
        }
        t => hashable(t) || matches!(t, DataType::Time32(_) | DataType::Time64(_)),
    }
}

impl AggregateUDFImpl for By {
    udaf_boilerplate!();

    fn name(&self) -> &str {
        self.name
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let y_type = |y: &DataType| if y.is_null() { DataType::Int64 } else { y.clone() };
        match arg_types {
            [x, y] if orderable(y) => Ok(vec![x.clone(), y_type(y)]),
            [x, y, n] if orderable(y) && (is_integer(n) || n.is_null()) => {
                Ok(vec![x.clone(), y_type(y), DataType::Int64])
            }
            _ => Err(not_found(self.name, arg_types)),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [x, _] => Ok(x.clone()),
            [x, _, _] => Ok(DataType::List(Arc::new(Field::new_list_field(x.clone(), true)))),
            _ => Err(not_found(self.name, arg_types)),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let types: Vec<DataType> = args.input_fields.iter().map(|f| f.data_type().clone()).collect();
        match types.as_slice() {
            [x, y] => Ok(vec![
                state_field(args.name, "x", x.clone()),
                state_field(args.name, "y", y.clone()),
            ]),
            [x, y, _] => Ok(super::aggregate_more::ByNAccumulator::state_fields(
                args.name, x, y,
            )),
            _ => Err(not_found(self.name, &types)),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
        if let [x, y, _] = types.as_slice() {
            let n = super::aggregate_more::top_n(&args, self.name)?;
            return Ok(Box::new(super::aggregate_more::ByNAccumulator::new(
                n,
                self.max,
                x.clone(),
                y.clone(),
            )));
        }
        let [x, y] = types.as_slice() else {
            return Err(not_found(self.name, &types));
        };
        Ok(Box::new(ByAccumulator {
            best_x: ScalarValue::try_from(x)?,
            best_y: ScalarValue::try_from(y)?,
            max: self.max,
        }))
    }
}

#[derive(Debug)]
struct ByAccumulator {
    best_x: ScalarValue,
    best_y: ScalarValue,
    max: bool,
}

/// Trino's identity for floats as keys (grouping, `DISTINCT`, maps):
/// `-0.0` is `0.0` and every NaN is the same NaN.
pub(crate) fn canonical_float(value: ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(if v.is_nan() {
            f64::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        })),
        ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(if v.is_nan() {
            f32::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        })),
        other => other,
    }
}

/// A float NaN.
pub(crate) fn is_nan(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float64(Some(v)) => v.is_nan(),
        ScalarValue::Float32(Some(v)) => v.is_nan(),
        _ => false,
    }
}

/// Total order for the comparison value: NaN above every number.
pub(crate) fn compare(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => x.total_cmp(y),
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => x.total_cmp(y),
        _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
    }
}

const ROW_NULL_FIELD: &str = "ROW comparison not supported for fields with null elements";
const ARRAY_NULL_ELEMENT: &str = "ARRAY comparison not supported for arrays with null elements";

/// Order of two non-null keys of `max_by`/`min_by`. Rows compare field by
/// field and arrays element by element, a prefix before the longer array; a
/// NULL field or element the comparison reaches is an error (a NULL past the
/// first difference is not). NaN is below every number when `nan_low` (the
/// order `max_by` uses) and above otherwise (`min_by`), so a NaN key is
/// picked only when every key is NaN; `-0.0` is below `0.0`.
pub(crate) fn key_order(a: &ScalarValue, b: &ScalarValue, nan_low: bool) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (ScalarValue::Struct(a), ScalarValue::Struct(b)) => {
            for (a, b) in a.columns().iter().zip(b.columns()) {
                let order = element_order(a, b, 0, nan_low, ROW_NULL_FIELD)?;
                if order.is_ne() {
                    return Ok(order);
                }
            }
            Ok(Ordering::Equal)
        }
        (ScalarValue::List(a), ScalarValue::List(b)) => elements_order(&a.value(0), &b.value(0), nan_low),
        (ScalarValue::LargeList(a), ScalarValue::LargeList(b)) => {
            elements_order(&a.value(0), &b.value(0), nan_low)
        }
        (ScalarValue::FixedSizeList(a), ScalarValue::FixedSizeList(b)) => {
            elements_order(&a.value(0), &b.value(0), nan_low)
        }
        _ => Ok(match (is_nan(a), is_nan(b)) {
            (false, false) => compare(a, b),
            (true, true) => Ordering::Equal,
            (true, false) if nan_low => Ordering::Less,
            (false, true) if nan_low => Ordering::Greater,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
        }),
    }
}

/// [`key_order`] of two arrays' elements, then of their lengths.
fn elements_order(a: &ArrayRef, b: &ArrayRef, nan_low: bool) -> Result<std::cmp::Ordering> {
    for i in 0..a.len().min(b.len()) {
        let order = element_order(a, b, i, nan_low, ARRAY_NULL_ELEMENT)?;
        if order.is_ne() {
            return Ok(order);
        }
    }
    Ok(a.len().cmp(&b.len()))
}

/// [`key_order`] of `a[i]` and `b[i]`; `null` is the error when either is NULL.
fn element_order(
    a: &ArrayRef,
    b: &ArrayRef,
    i: usize,
    nan_low: bool,
    null: &str,
) -> Result<std::cmp::Ordering> {
    if a.is_null(i) || b.is_null(i) {
        return Err(not_supported(null));
    }
    key_order(
        &ScalarValue::try_from_array(a, i)?,
        &ScalarValue::try_from_array(b, i)?,
        nan_low,
    )
}

impl ByAccumulator {
    fn offer(&mut self, xs: &ArrayRef, ys: &ArrayRef) -> Result<()> {
        for i in 0..ys.len() {
            if ys.is_null(i) {
                continue;
            }
            let y = ScalarValue::try_from_array(ys, i)?;
            let better = self.best_y.is_null() || {
                let order = key_order(&y, &self.best_y, self.max)?;
                if self.max { order.is_gt() } else { order.is_lt() }
            };
            if better {
                self.best_y = y;
                self.best_x = ScalarValue::try_from_array(xs, i)?;
            }
        }
        Ok(())
    }
}

impl Accumulator for ByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [xs, ys] = values else {
            return Err(DataFusionError::Internal("max_by arguments".into()));
        };
        self.offer(xs, ys)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.best_x.clone(), self.best_y.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.best_x.clone())
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.best_x.size() + self.best_y.size()
    }
}

// ---------------------------------------------------------------------------
// count_if, arbitrary
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CountIf {
    signature: Signature,
}

impl AggregateUDFImpl for CountIf {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "count_if"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Boolean | DataType::Null] => Ok(vec![DataType::Boolean]),
            _ => Err(not_found("count_if", arg_types)),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![state_field(args.name, "count", DataType::Int64)])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountIfAccumulator(0)))
    }
}

#[derive(Debug)]
struct CountIfAccumulator(i64);

impl Accumulator for CountIfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(values) = values.first() else {
            return Ok(());
        };
        let values = downcast::<BooleanArray>(values)?;
        self.0 += i64::try_from(values.true_count()).unwrap_or(i64::MAX);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(counts) = states.first() else {
            return Ok(());
        };
        let counts = downcast::<Int64Array>(counts)?;
        self.0 += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.0))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0)))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Arbitrary {
    signature: Signature,
    aliases: Vec<String>,
}

impl AggregateUDFImpl for Arbitrary {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "arbitrary"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] => Ok(vec![t.clone()]),
            _ => Err(not_found("arbitrary", arg_types)),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| not_found("arbitrary", arg_types))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![state_field(args.name, "value", args.return_type().clone())])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArbitraryAccumulator(ScalarValue::try_from(
            args.return_type(),
        )?)))
    }
}

#[derive(Debug)]
struct ArbitraryAccumulator(ScalarValue);

impl Accumulator for ArbitraryAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.0.is_null() {
            return Ok(());
        }
        let Some(values) = values.first() else {
            return Ok(());
        };
        if let Some(i) = (0..values.len()).find(|i| !values.is_null(*i)) {
            self.0 = ScalarValue::try_from_array(values, i)?;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.0.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.0.clone())
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.0.size()
    }
}

// ---------------------------------------------------------------------------
// approx_distinct
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ApproxDistinct {
    signature: Signature,
}

impl AggregateUDFImpl for ApproxDistinct {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "approx_distinct"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t] if hashable(t) => Ok(vec![if t.is_null() { DataType::Utf8 } else { t.clone() }]),
            [t, e] if hashable(t) && (is_float(e) || is_decimal(e) || e.is_null()) => Ok(vec![
                if t.is_null() { DataType::Utf8 } else { t.clone() },
                DataType::Float64,
            ]),
            _ => Err(not_found("approx_distinct", arg_types)),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![
            state_field(
                args.name,
                "hashes",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            ),
            state_field(args.name, "registers", DataType::Binary),
        ])
    }

    fn accumulator(&self, _args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctAccumulator::default()))
    }
}

/// Value hashes in a [`Hll`]: exact while few, `HyperLogLog` after (Trino's
/// sparse and dense forms), so memory stays bounded.
#[derive(Debug, Default)]
struct DistinctAccumulator {
    hll: Hll,
}

impl Accumulator for DistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(e) = values.get(1) {
            let e = downcast::<Float64Array>(e)?;
            if let Some(v) = e.iter().flatten().next()
                && !(0.004_062_5..=0.26).contains(&v)
            {
                return Err(invalid_argument(
                    "Max standard error must be in [0.0040625, 0.26000]",
                ));
            }
        }
        let Some(values) = values.first() else {
            return Ok(());
        };
        for hash in hash_column(values)?.into_iter().flatten() {
            self.hll.insert(hash);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [lists, registers] = states else {
            return Err(DataFusionError::Internal("approx_distinct state".into()));
        };
        let lists = downcast::<ListArray>(lists)?;
        let registers = downcast::<BinaryArray>(registers)?;
        for i in 0..lists.len() {
            if !registers.is_null(i) {
                self.hll.merge_registers(registers.value(i));
            }
            if lists.is_null(i) {
                continue;
            }
            let values = lists.value(i);
            let values = downcast::<Int64Array>(&values)?;
            for hash in values.iter().flatten() {
                self.hll.insert(i64_bits(hash));
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .hll
            .hashes()
            .map(|h| ScalarValue::Int64(Some(u64_bits(h))))
            .collect();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&values, &DataType::Int64)),
            ScalarValue::Binary(self.hll.registers().map(<[u8]>::to_vec)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.hll.estimate())))
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.hll.size()
    }
}

// ---------------------------------------------------------------------------
// approx_percentile
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ApproxPercentile {
    signature: Signature,
}

fn percentile_value_type(t: &DataType) -> Option<DataType> {
    match t {
        t if is_integer(t) => Some(DataType::Int64),
        DataType::Float32 => Some(DataType::Float32),
        DataType::Float64 => Some(DataType::Float64),
        t if is_decimal(t) => Some(DataType::Float64),
        DataType::Null => Some(DataType::Float64),
        _ => None,
    }
}

fn percentile_arg(t: &DataType) -> Option<DataType> {
    match t {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => Some(DataType::List(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
        )),
        t if is_float(t) || is_decimal(t) || is_integer(t) || t.is_null() => Some(DataType::Float64),
        _ => None,
    }
}

impl AggregateUDFImpl for ApproxPercentile {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "approx_percentile"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let fail = || not_found("approx_percentile", arg_types);
        match arg_types {
            [x, p] => Ok(vec![
                percentile_value_type(x).ok_or_else(fail)?,
                percentile_arg(p).ok_or_else(fail)?,
            ]),
            [x, w, p] if is_integer(w) || w.is_null() => Ok(vec![
                percentile_value_type(x).ok_or_else(fail)?,
                DataType::Int64,
                percentile_arg(p).ok_or_else(fail)?,
            ]),
            _ => Err(fail()),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let x = arg_types
            .first()
            .cloned()
            .ok_or_else(|| not_found("approx_percentile", arg_types))?;
        match arg_types.last() {
            Some(DataType::List(_)) => Ok(DataType::List(Arc::new(Field::new_list_field(x, true)))),
            _ => Ok(x),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        let x = args
            .input_fields
            .first()
            .map(|f| f.data_type().clone())
            .ok_or_else(|| DataFusionError::Internal("approx_percentile input".into()))?;
        let list = |t: DataType| DataType::List(Arc::new(Field::new_list_field(t, true)));
        Ok(vec![
            state_field(args.name, "values", list(x.clone())),
            state_field(args.name, "ends", list(x)),
            state_field(args.name, "weights", list(DataType::Int64)),
            state_field(args.name, "percentiles", list(DataType::Float64)),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        let types: Vec<DataType> = args.expr_fields.iter().map(|f| f.data_type().clone()).collect();
        let x = types
            .first()
            .cloned()
            .ok_or_else(|| DataFusionError::Internal("no input".into()))?;
        let values = if is_float(&x) {
            Values::Float(QuantileSketch::default())
        } else {
            Values::Integer(QuantileSketch::default())
        };
        Ok(Box::new(PercentileAccumulator {
            values,
            percentiles: None,
            value_type: x,
            array_result: matches!(types.last(), Some(DataType::List(_))),
            weighted: types.len() == 3,
        }))
    }
}

/// Values of `approx_percentile`, by kind (`real` values are kept as `double`).
#[derive(Debug)]
enum Values {
    Integer(QuantileSketch<i64>),
    Float(QuantileSketch<f64>),
}

#[derive(Debug)]
struct PercentileAccumulator {
    values: Values,
    percentiles: Option<Vec<f64>>,
    value_type: DataType,
    array_result: bool,
    weighted: bool,
}

fn read_percentiles(array: &ArrayRef, row: usize) -> Result<Option<Vec<f64>>> {
    if array.is_null(row) {
        return Ok(None);
    }
    let values: Vec<Option<f64>> = match array.data_type() {
        DataType::List(_) => {
            let list = downcast::<ListArray>(array)?.value(row);
            downcast::<Float64Array>(&list)?.iter().collect()
        }
        _ => vec![Some(downcast::<Float64Array>(array)?.value(row))],
    };
    let mut out = Vec::with_capacity(values.len());
    for v in values {
        let v = v.ok_or_else(|| invalid_argument("Percentile cannot be null"))?;
        if !(0.0..=1.0).contains(&v) {
            return Err(invalid_argument("Percentile must be between 0 and 1"));
        }
        out.push(v);
    }
    Ok(Some(out))
}

impl PercentileAccumulator {
    fn set_percentiles(&mut self, found: Vec<f64>) -> Result<()> {
        match &self.percentiles {
            Some(existing) if *existing != found => Err(invalid_argument(
                "Percentile argument must be constant for all input values",
            )),
            Some(_) => Ok(()),
            None => {
                self.percentiles = Some(found);
                Ok(())
            }
        }
    }

    /// Adds the non-NULL values of `xs` (runs up to `ends` when merging
    /// states) with their weights.
    fn add(&mut self, xs: &ArrayRef, ends: Option<&ArrayRef>, weights: &[Option<i64>]) -> Result<()> {
        match &mut self.values {
            Values::Integer(sketch) => {
                let los = longs(xs)?;
                let his = ends.map(longs).transpose()?;
                for (i, weight) in weights.iter().enumerate() {
                    if let Some(weight) = weight
                        && !los.is_null(i)
                    {
                        let lo = los.value(i);
                        let hi = his.as_ref().map_or(lo, |h| h.value(i));
                        sketch.add(Run {
                            lo,
                            hi,
                            weight: *weight,
                        });
                    }
                }
            }
            Values::Float(sketch) => {
                let xs = cast(xs, &DataType::Float64)?;
                let xs = downcast::<Float64Array>(&xs)?;
                for (i, weight) in weights.iter().enumerate() {
                    if let Some(weight) = weight
                        && !xs.is_null(i)
                    {
                        let v = xs.value(i);
                        if v.is_nan() {
                            // Trino's quantile digest rejects NaN.
                            return Err(DataFusionError::Execution(
                                "GENERIC_INTERNAL_ERROR: value is NaN".into(),
                            ));
                        }
                        sketch.add(Run {
                            lo: v,
                            hi: v,
                            weight: *weight,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        match &self.values {
            Values::Integer(sketch) => sketch.is_empty(),
            Values::Float(sketch) => sketch.is_empty(),
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let xs = values
            .first()
            .ok_or_else(|| DataFusionError::Internal("no input".into()))?;
        let percentile_column = values
            .last()
            .ok_or_else(|| DataFusionError::Internal("no percentile".into()))?;
        for i in 0..xs.len() {
            if let Some(found) = read_percentiles(percentile_column, i)? {
                self.set_percentiles(found)?;
            }
        }
        let weights: Vec<Option<i64>> = if self.weighted {
            let w = downcast::<Int64Array>(&values[1])?;
            let mut out = Vec::with_capacity(w.len());
            for i in 0..w.len() {
                if w.is_null(i) || xs.is_null(i) {
                    out.push(None);
                } else if w.value(i) <= 0 {
                    return Err(invalid_argument("weight must be > 0"));
                } else {
                    out.push(Some(w.value(i)));
                }
            }
            out
        } else {
            vec![Some(1); xs.len()]
        };
        self.add(xs, None, &weights)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [values, ends, weights, percentiles] = states else {
            return Err(DataFusionError::Internal("approx_percentile state".into()));
        };
        let values = downcast::<ListArray>(values)?;
        let ends = downcast::<ListArray>(ends)?;
        let weights = downcast::<ListArray>(weights)?;
        for i in 0..values.len() {
            if let Some(found) = read_percentiles(percentiles, i)?.filter(|p| !p.is_empty()) {
                self.set_percentiles(found)?;
            }
            if values.is_null(i) {
                continue;
            }
            let v = values.value(i);
            let e = ends.value(i);
            let w = weights.value(i);
            let w: Vec<Option<i64>> = downcast::<Int64Array>(&w)?.iter().collect();
            self.add(&v, Some(&e), &w)?;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let (values, ends, weights): (ArrayRef, ArrayRef, Vec<i64>) = match &self.values {
            Values::Integer(sketch) => (
                Arc::new(Int64Array::from_iter_values(sketch.items().iter().map(|r| r.lo))),
                Arc::new(Int64Array::from_iter_values(sketch.items().iter().map(|r| r.hi))),
                sketch.items().iter().map(|r| r.weight).collect(),
            ),
            Values::Float(sketch) => (
                Arc::new(Float64Array::from_iter_values(
                    sketch.items().iter().map(|r| r.lo),
                )),
                Arc::new(Float64Array::from_iter_values(
                    sketch.items().iter().map(|r| r.hi),
                )),
                sketch.items().iter().map(|r| r.weight).collect(),
            ),
        };
        let (values, ends) = (cast(&values, &self.value_type)?, cast(&ends, &self.value_type)?);
        let percentiles: Vec<ScalarValue> = self
            .percentiles
            .iter()
            .flatten()
            .map(|p| ScalarValue::Float64(Some(*p)))
            .collect();
        Ok(vec![
            ScalarValue::List(Arc::new(
                datafusion::common::utils::SingleRowListArrayBuilder::new(values).build_list_array(),
            )),
            ScalarValue::List(Arc::new(
                datafusion::common::utils::SingleRowListArrayBuilder::new(ends).build_list_array(),
            )),
            ScalarValue::List(Arc::new(
                datafusion::common::utils::SingleRowListArrayBuilder::new(Arc::new(Int64Array::from(
                    weights,
                )))
                .build_list_array(),
            )),
            ScalarValue::List(ScalarValue::new_list_nullable(&percentiles, &DataType::Float64)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let null = ScalarValue::try_from(&self.value_type)?;
        let list_type = DataType::List(Arc::new(Field::new_list_field(self.value_type.clone(), true)));
        let Some(percentiles) = self.percentiles.clone() else {
            return Ok(if self.array_result {
                ScalarValue::try_from(&list_type)?
            } else {
                null
            });
        };
        if self.is_empty() {
            return Ok(if self.array_result {
                ScalarValue::try_from(&list_type)?
            } else {
                null
            });
        }
        let results = match &mut self.values {
            Values::Integer(sketch) => sketch
                .quantiles(&percentiles)
                .into_iter()
                .map(|v| ScalarValue::Int64(v).cast_to(&self.value_type))
                .collect::<Result<Vec<_>>>()?,
            Values::Float(sketch) => sketch
                .quantiles(&percentiles)
                .into_iter()
                .map(|v| ScalarValue::Float64(v).cast_to(&self.value_type))
                .collect::<Result<Vec<_>>>()?,
        };
        if self.array_result {
            Ok(ScalarValue::List(ScalarValue::new_list_nullable(
                &results,
                &self.value_type,
            )))
        } else {
            Ok(results.into_iter().next().unwrap_or(null))
        }
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + match &self.values {
                Values::Integer(sketch) => sketch.size(),
                Values::Float(sketch) => sketch.size(),
            }
    }
}

// ---------------------------------------------------------------------------
// avg: decimals by the engine's decimal `avg` (`crate::overflow::avg`: the
// scale kept, rounded half up, added without overflow); `real` as `double`
// answered `real`; the rest by the built-in
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Avg {
    signature: Signature,
}

fn builtin_avg() -> Arc<AggregateUDF> {
    datafusion::functions_aggregate::average::avg_udaf()
}

/// `avg` of a decimal.
fn decimal_avg() -> crate::overflow::avg::TrinoAvg {
    crate::overflow::avg::TrinoAvg::default()
}

fn is_decimal_input(fields: &[FieldRef]) -> bool {
    matches!(
        fields.first().map(|f| f.data_type()),
        Some(DataType::Decimal128(..))
    )
}

impl AggregateUDFImpl for Avg {
    udaf_boilerplate!();

    fn name(&self) -> &'static str {
        "avg"
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [t @ DataType::Decimal128(..)] => Ok(vec![t.clone()]),
            // `avg(real)` is `real` (computed as `double`).
            [DataType::Float32] => Ok(vec![DataType::Float32]),
            // Integers and doubles average as double (the engine's `avg` has a
            // signature, not a coercion, so windows need this spelled out).
            [t] if is_integer(t) || is_float(t) || t.is_null() => Ok(vec![DataType::Float64]),
            [t @ (DataType::Duration(_) | DataType::Interval(_))] => Ok(vec![t.clone()]),
            _ => Err(not_found("avg", arg_types)),
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types {
            [t @ DataType::Decimal128(..)] => Ok(t.clone()),
            [DataType::Float32] => Ok(DataType::Float32),
            _ => builtin_avg().inner().return_type(arg_types),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        match args.input_fields.first().map(|f| f.data_type()) {
            Some(DataType::Decimal128(..)) => decimal_avg().state_fields(args),
            Some(DataType::Float32) => {
                let doubles = fields(args.name, &[DataType::Float64]);
                builtin_avg()
                    .inner()
                    .state_fields(engine_state_args(&args, &doubles, &DataType::Float64))
            }
            _ => builtin_avg().inner().state_fields(args),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        match args.expr_fields.first().map(|f| f.data_type().clone()) {
            Some(DataType::Decimal128(..)) => decimal_avg().accumulator(args),
            Some(DataType::Float32) => {
                let doubles = fields(args.name, &[DataType::Float64]);
                let inner =
                    builtin_avg()
                        .inner()
                        .accumulator(engine_args(&args, &doubles, &DataType::Float64))?;
                Ok(Box::new(Retyped {
                    inner,
                    inputs: vec![DataType::Float64],
                    finish: Some(to_real),
                }))
            }
            _ => builtin_avg().inner().accumulator(args),
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs<'_>) -> bool {
        match args.expr_fields.first().map(|f| f.data_type()) {
            Some(DataType::Decimal128(..)) => decimal_avg().groups_accumulator_supported(args),
            Some(DataType::Float32) => {
                let doubles = fields(args.name, &[DataType::Float64]);
                builtin_avg().inner().groups_accumulator_supported(engine_args(
                    &args,
                    &doubles,
                    &DataType::Float64,
                ))
            }
            _ => builtin_avg().inner().groups_accumulator_supported(args),
        }
    }

    fn create_groups_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn GroupsAccumulator>> {
        if is_decimal_input(args.expr_fields) {
            return decimal_avg().create_groups_accumulator(args);
        }
        if args.expr_fields.first().map(|f| f.data_type()) == Some(&DataType::Float32) {
            let doubles = fields(args.name, &[DataType::Float64]);
            let inner = builtin_avg().inner().create_groups_accumulator(engine_args(
                &args,
                &doubles,
                &DataType::Float64,
            ))?;
            return Ok(Box::new(RetypedGroups {
                inner,
                inputs: vec![DataType::Float64],
                finish: Some(to_real_array),
            }));
        }
        builtin_avg().inner().create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        match args.expr_fields.first().map(|f| f.data_type()) {
            Some(DataType::Decimal128(..)) => decimal_avg().create_sliding_accumulator(args),
            Some(DataType::Float32) => {
                let doubles = fields(args.name, &[DataType::Float64]);
                let inner = builtin_avg().inner().create_sliding_accumulator(engine_args(
                    &args,
                    &doubles,
                    &DataType::Float64,
                ))?;
                Ok(Box::new(Retyped {
                    inner,
                    inputs: vec![DataType::Float64],
                    finish: Some(to_real),
                }))
            }
            _ => builtin_avg().inner().create_sliding_accumulator(args),
        }
    }
}

/// `max_by` (`max`) or `min_by`.
pub(crate) fn by(max: bool) -> AggregateUDF {
    AggregateUDF::from(By {
        signature: Signature::user_defined(Volatility::Immutable),
        name: if max { "max_by" } else { "min_by" },
        max,
    })
}

pub(crate) fn udafs() -> Vec<AggregateUDF> {
    let user_defined = || Signature::user_defined(Volatility::Immutable);
    vec![
        AggregateUDF::from(Checksum {
            signature: user_defined(),
        }),
        by(true),
        by(false),
        AggregateUDF::from(CountIf {
            signature: user_defined(),
        }),
        AggregateUDF::from(Arbitrary {
            signature: user_defined(),
            aliases: vec!["any_value".to_owned()],
        }),
        AggregateUDF::from(ApproxDistinct {
            signature: user_defined(),
        }),
        AggregateUDF::from(ApproxPercentile {
            signature: user_defined(),
        }),
        AggregateUDF::from(Avg {
            signature: user_defined(),
        }),
        datafusion::functions_aggregate::bool_and_or::bool_and_udaf()
            .as_ref()
            .clone()
            .with_aliases(["every"]),
        // Trino's `variance` is the sample variance.
        datafusion::functions_aggregate::variance::var_samp_udaf()
            .as_ref()
            .clone()
            .with_aliases(["variance"]),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_of_nulls_is_prime64() {
        let mut acc = ChecksumAccumulator::default();
        let nulls: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        acc.update_batch(&[nulls]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Binary(Some(PRIME64.to_le_bytes().to_vec()))
        );
        let mut empty = ChecksumAccumulator::default();
        assert_eq!(empty.evaluate().unwrap(), ScalarValue::Binary(None));
    }

    #[test]
    fn checksum_is_order_insensitive_and_mergeable() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![3, 1, 2]));
        let mut x = ChecksumAccumulator::default();
        x.update_batch(&[a]).unwrap();
        let mut y = ChecksumAccumulator::default();
        y.update_batch(&[b]).unwrap();
        assert_eq!(x.evaluate().unwrap(), y.evaluate().unwrap());
        let state: Vec<ArrayRef> = x.state().unwrap().iter().map(|s| s.to_array().unwrap()).collect();
        let mut merged = ChecksumAccumulator::default();
        merged.merge_batch(&state).unwrap();
        assert_eq!(merged.evaluate().unwrap(), y.evaluate().unwrap());
    }

    fn row(a: Option<i32>, b: Option<f64>) -> ScalarValue {
        ScalarValue::Struct(Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("c0", DataType::Int32, true)),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![a])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("c1", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![b])) as ArrayRef,
            ),
        ])))
    }

    fn list(values: &[Option<i32>]) -> ListArray {
        ListArray::from_iter_primitive::<datafusion::arrow::datatypes::Int32Type, _, _>(vec![Some(
            values.to_vec(),
        )])
    }

    fn order(a: &ScalarValue, b: &ScalarValue, nan_low: bool) -> std::cmp::Ordering {
        key_order(a, b, nan_low).unwrap()
    }

    fn refusal(a: &ScalarValue, b: &ScalarValue) -> String {
        key_order(a, b, true).unwrap_err().to_string()
    }

    #[test]
    fn rows_compare_field_by_field() {
        use std::cmp::Ordering::{Equal, Greater, Less};
        assert_eq!(
            order(&row(Some(1), Some(2.0)), &row(Some(1), Some(3.0)), true),
            Less
        );
        assert_eq!(
            order(&row(Some(2), Some(0.0)), &row(Some(1), Some(9.0)), true),
            Greater
        );
        assert_eq!(
            order(&row(Some(1), Some(2.0)), &row(Some(1), Some(2.0)), false),
            Equal
        );
        // A NULL past the first difference is never reached.
        assert_eq!(
            order(&row(Some(2), None), &row(Some(1), Some(3.0)), true),
            Greater
        );
        assert_eq!(order(&row(Some(1), Some(3.0)), &row(Some(2), None), true), Less);
        let error = refusal(&row(Some(1), None), &row(Some(1), Some(3.0)));
        assert_eq!(
            error,
            "Execution error: NOT_SUPPORTED: ROW comparison not supported for fields with null elements"
        );
        assert!(refusal(&row(Some(1), Some(3.0)), &row(Some(1), None)).contains("ROW comparison"));
        assert!(refusal(&row(None, None), &row(None, None)).contains("ROW comparison"));
    }

    #[test]
    fn nan_is_low_for_max_by_and_high_for_min_by() {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let nan = ScalarValue::Float64(Some(f64::NAN));
        let low = ScalarValue::Float64(Some(f64::NEG_INFINITY));
        assert_eq!(order(&nan, &low, true), Less);
        assert_eq!(order(&low, &nan, true), Greater);
        assert_eq!(order(&nan, &low, false), Greater);
        assert_eq!(order(&low, &nan, false), Less);
        assert_eq!(order(&nan, &nan, true), Equal);
        assert_eq!(order(&nan, &nan, false), Equal);
        assert_eq!(
            order(
                &ScalarValue::Float64(Some(-0.0)),
                &ScalarValue::Float64(Some(0.0)),
                true
            ),
            Less
        );
        // Inside a row too.
        assert_eq!(
            order(&row(Some(1), Some(f64::NAN)), &row(Some(1), Some(-1.0)), true),
            Less
        );
        assert_eq!(
            order(&row(Some(1), Some(f64::NAN)), &row(Some(1), Some(-1.0)), false),
            Greater
        );
    }

    #[test]
    fn arrays_compare_element_by_element_then_by_length() {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let of = |values: &[Option<i32>]| ScalarValue::List(Arc::new(list(values)));
        assert_eq!(
            order(&of(&[Some(1), Some(3)]), &of(&[Some(1), Some(2), Some(0)]), true),
            Greater
        );
        assert_eq!(
            order(&of(&[Some(1), Some(2)]), &of(&[Some(1), Some(2), Some(0)]), true),
            Less
        );
        assert_eq!(
            order(&of(&[Some(1), Some(2), Some(0)]), &of(&[Some(1), Some(2)]), true),
            Greater
        );
        assert_eq!(order(&of(&[Some(1)]), &of(&[Some(1)]), true), Equal);
        assert_eq!(
            order(&of(&[Some(2), None]), &of(&[Some(1), Some(2)]), true),
            Greater
        );
        assert_eq!(
            refusal(&of(&[Some(1), None]), &of(&[Some(1), Some(2)])),
            "Execution error: NOT_SUPPORTED: ARRAY comparison not supported for arrays with null elements"
        );
        assert!(refusal(&of(&[Some(1), Some(2)]), &of(&[Some(1), None])).contains("ARRAY comparison"));
        let large = |values: &[Option<i32>]| {
            ScalarValue::LargeList(Arc::new(
                datafusion::arrow::array::LargeListArray::from_iter_primitive::<
                    datafusion::arrow::datatypes::Int32Type,
                    _,
                    _,
                >(vec![Some(values.to_vec())]),
            ))
        };
        assert_eq!(
            order(&large(&[Some(2)]), &large(&[Some(1), Some(5)]), true),
            Greater
        );
        assert_eq!(order(&large(&[Some(1)]), &large(&[Some(1), Some(5)]), true), Less);
        assert!(refusal(&large(&[Some(1), None]), &large(&[Some(1), Some(5)])).contains("ARRAY comparison"));
        let fixed = |values: &[Option<i32>]| {
            ScalarValue::FixedSizeList(Arc::new(
                datafusion::arrow::array::FixedSizeListArray::from_iter_primitive::<
                    datafusion::arrow::datatypes::Int32Type,
                    _,
                    _,
                >(vec![Some(values.to_vec())], 2),
            ))
        };
        assert_eq!(
            order(&fixed(&[Some(1), Some(2)]), &fixed(&[Some(1), Some(3)]), true),
            Less
        );
        assert_eq!(
            order(&fixed(&[Some(1), Some(4)]), &fixed(&[Some(1), Some(3)]), true),
            Greater
        );
        assert!(refusal(&fixed(&[Some(1), None]), &fixed(&[Some(1), Some(3)])).contains("ARRAY comparison"));
    }

    #[test]
    fn rows_and_arrays_of_orderable_types_are_orderable() {
        let int = Arc::new(Field::new("c0", DataType::Int32, true));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert!(orderable(&DataType::Struct(
            vec![Arc::clone(&int), Arc::clone(&int)].into()
        )));
        assert!(orderable(&DataType::List(Arc::clone(&int))));
        assert!(orderable(&DataType::LargeList(Arc::clone(&int))));
        assert!(orderable(&DataType::FixedSizeList(Arc::clone(&int), 2)));
        assert!(orderable(&DataType::Time64(TimeUnit::Microsecond)));
        assert!(!orderable(&map));
        let with_map = Arc::new(Field::new("c1", map.clone(), true));
        assert!(!orderable(&DataType::Struct(
            vec![Arc::clone(&int), Arc::clone(&with_map)].into()
        )));
        assert!(!orderable(&DataType::List(Arc::clone(&with_map))));
        assert!(!orderable(&DataType::LargeList(Arc::clone(&with_map))));
        assert!(!orderable(&DataType::FixedSizeList(with_map, 2)));
    }

    #[test]
    fn max_by_a_row_key() {
        // The third key is a NULL row: skipped, never compared.
        let keys: ArrayRef = Arc::new(StructArray::new(
            vec![
                Field::new("c0", DataType::Int32, true),
                Field::new("c1", DataType::Int32, true),
            ]
            .into(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    None,
                    Some(0),
                ])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![
                    Some(2),
                    Some(3),
                    None,
                    Some(9),
                ])),
            ],
            Some(vec![true, true, false, true].into()),
        ));
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        for (max, best) in [(true, 2), (false, 4)] {
            let mut by = ByAccumulator {
                best_x: ScalarValue::Int64(None),
                best_y: ScalarValue::try_from(keys.data_type()).unwrap(),
                max,
            };
            by.update_batch(&[Arc::clone(&values), Arc::clone(&keys)])
                .unwrap();
            assert_eq!(
                by.evaluate().unwrap(),
                ScalarValue::Int64(Some(best)),
                "max: {max}"
            );
        }
    }

    #[test]
    fn max_by_picks_a_nan_key_only_when_every_key_is_nan() {
        for max in [true, false] {
            let mut by = ByAccumulator {
                best_x: ScalarValue::Int64(None),
                best_y: ScalarValue::Float64(None),
                max,
            };
            let keys: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
            by.update_batch(&[Arc::new(Int64Array::from(vec![1])), Arc::clone(&keys)])
                .unwrap();
            assert_eq!(by.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
            let keys: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN]));
            by.update_batch(&[Arc::new(Int64Array::from(vec![2, 3])), keys])
                .unwrap();
            assert_eq!(by.evaluate().unwrap(), ScalarValue::Int64(Some(2)), "max: {max}");
        }
    }
}
