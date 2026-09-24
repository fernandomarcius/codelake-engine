//! Array and map functions (Trino "Array functions and operators", "Map
//! functions and operators").
//!
//! Semantics that differ from `DataFusion`'s functions of similar names:
//! `array_position` returns 0 when absent, `contains` returns NULL when the
//! element is absent but the array has NULLs, `array_remove` removes every
//! occurrence, `array_sort` puts NULLs last, `array_max`/`array_min` are NULL
//! when any element is NULL, and set functions keep first-occurrence order.
//! Elements are handled as `ScalarValue`s: correct for every type, not
//! vectorized.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, MapArray, new_empty_array};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;

use super::aggregate::compare;
use super::support::{
    At, Call, Func, as_i64, as_str, bool_array, int_array, ints, invalid_argument, not_found, string_array,
    strings,
};

pub(crate) fn element_type(t: &DataType) -> Option<&DataType> {
    match t {
        DataType::List(f) => Some(f.data_type()),
        _ => None,
    }
}

pub(crate) fn list_of(t: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(t, true)))
}

/// Accepts an array argument (NULL literal becomes `array(bigint)`).
pub(crate) fn as_list(t: &DataType) -> Option<DataType> {
    match t {
        DataType::List(_) => Some(t.clone()),
        DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(list_of(f.data_type().clone())),
        DataType::Null => Some(list_of(DataType::Int64)),
        _ => None,
    }
}

pub(crate) fn lists(array: &ArrayRef) -> Result<&ListArray> {
    array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Internal(format!("expected list, got {}", array.data_type())))
}

/// Elements of row `row`, `None` when the array itself is NULL.
pub(crate) fn elements(list: &ListArray, row: usize) -> Result<Option<Vec<ScalarValue>>> {
    if list.is_null(row) {
        return Ok(None);
    }
    let values = list.value(row);
    (0..values.len())
        .map(|i| ScalarValue::try_from_array(&values, i))
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn scalars_to_array(values: Vec<ScalarValue>, data_type: &DataType) -> Result<ArrayRef> {
    if values.is_empty() {
        return Ok(new_empty_array(data_type));
    }
    ScalarValue::iter_to_array(values)
}

/// Builds a list column from per-row element vectors.
pub(crate) fn build_lists(rows: Vec<Option<Vec<ScalarValue>>>, element: &DataType) -> Result<ArrayRef> {
    let list_type = list_of(element.clone());
    let scalars = rows
        .into_iter()
        .map(|row| match row {
            None => ScalarValue::try_from(&list_type),
            Some(values) => Ok(ScalarValue::List(ScalarValue::new_list_nullable(
                &values, element,
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    scalars_to_array(scalars, &list_type)
}

fn row_scalars(array: &ArrayRef, rows: usize) -> Result<Vec<ScalarValue>> {
    (0..rows).map(|i| ScalarValue::try_from_array(array, i)).collect()
}

fn same(a: &ScalarValue, b: &ScalarValue) -> bool {
    compare(a, b).is_eq()
}

/// `(array(T), T)`: the element coerced to the array's element type.
fn coerce_array_and_element(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    let [array, element] = types else {
        return Err(not_found(name, types));
    };
    let array = as_list(array).ok_or_else(|| not_found(name, types))?;
    let inner = element_type(&array).cloned().unwrap_or(DataType::Null);
    if element.is_null() {
        return Ok(vec![array, inner]);
    }
    let common = comparison_coercion(&inner, element).ok_or_else(|| not_found(name, types))?;
    Ok(vec![list_of(common.clone()), common])
}

fn return_first(r: &super::support::Ret<'_>) -> Result<DataType> {
    r.types
        .first()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
}

fn return_element(r: &super::support::Ret<'_>) -> Result<DataType> {
    r.types
        .first()
        .and_then(element_type)
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("expected array".into()))
}

pub(crate) fn cardinality() -> ScalarUDF {
    Func::new(
        "cardinality",
        |name, types| match types {
            [DataType::Map(..)] => Ok(types.to_vec()),
            [t] => as_list(t).map(|t| vec![t]).ok_or_else(|| not_found(name, types)),
            _ => Err(not_found(name, types)),
        },
        |_| Ok(DataType::Int64),
        |call| {
            let arg = call.arg(0)?;
            let out: Vec<Option<i64>> = if let Some(maps) = arg.as_any().downcast_ref::<MapArray>() {
                (0..call.rows)
                    .map(|i| (!maps.is_null(i)).then(|| i64::from(maps.value_length(i))))
                    .collect()
            } else {
                let l = lists(arg)?;
                (0..call.rows)
                    .map(|i| (!l.is_null(i)).then(|| i64::from(l.value_length(i))))
                    .collect()
            };
            Ok(int_array(out))
        },
    )
    .udf()
}

fn map_value(maps: &MapArray, row: usize, key: &ScalarValue) -> Result<ScalarValue> {
    let entries = maps.value(row);
    let keys = entries.column(0);
    let values = entries.column(1);
    for i in 0..keys.len() {
        if same(&ScalarValue::try_from_array(keys, i)?, key) {
            return ScalarValue::try_from_array(values, i);
        }
    }
    ScalarValue::try_from(values.data_type())
}

fn array_element(values: &[ScalarValue], index: i64, element: &DataType) -> Result<ScalarValue> {
    if index == 0 {
        return Err(invalid_argument("SQL array indices start at 1"));
    }
    let len = i64::try_from(values.len()).unwrap_or(i64::MAX);
    let position = if index > 0 { index - 1 } else { len + index };
    match usize::try_from(position).ok().and_then(|p| values.get(p)) {
        Some(v) => Ok(v.clone()),
        None => ScalarValue::try_from(element),
    }
}

pub(crate) fn element_at() -> ScalarUDF {
    Func::new(
        "element_at",
        |name, types| match types {
            [map @ DataType::Map(entries, _), _] => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(not_found(name, types));
                };
                let key_type = fields
                    .first()
                    .map(|f| f.data_type().clone())
                    .ok_or_else(|| not_found(name, types))?;
                Ok(vec![map.clone(), key_type])
            }
            [array, index] => Ok(vec![
                as_list(array).ok_or_else(|| not_found(name, types))?,
                as_i64(index).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        |r| match r.types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) => fields
                    .get(1)
                    .map(|f| f.data_type().clone())
                    .ok_or_else(|| DataFusionError::Internal("map value".into())),
                _ => Err(DataFusionError::Internal("map entries".into())),
            },
            _ => return_element(r),
        },
        |call| {
            let container = call.arg(0)?;
            let keys = row_scalars(call.arg(1)?, call.rows)?;
            let out = if let DataType::Map(..) = container.data_type() {
                let maps = container
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| DataFusionError::Internal("expected map".into()))?;
                (0..call.rows)
                    .map(|i| {
                        if maps.is_null(i) || keys[i].is_null() {
                            ScalarValue::try_from(call.return_type)
                        } else {
                            map_value(maps, i, &keys[i])
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                let l = lists(container)?;
                (0..call.rows)
                    .map(|i| match (elements(l, i)?, &keys[i]) {
                        (Some(values), ScalarValue::Int64(Some(index))) => {
                            array_element(&values, *index, call.return_type)
                        }
                        _ => ScalarValue::try_from(call.return_type),
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            scalars_to_array(out, call.return_type)
        },
    )
    .udf()
}

pub(crate) fn contains() -> ScalarUDF {
    Func::new(
        "contains",
        coerce_array_and_element,
        |_| Ok(DataType::Boolean),
        |call| {
            let l = lists(call.arg(0)?)?;
            let needles = row_scalars(call.arg(1)?, call.rows)?;
            let out = (0..call.rows)
                .map(|i| {
                    let Some(values) = elements(l, i)? else {
                        return Ok(None);
                    };
                    let needle = &needles[i];
                    if needle.is_null() {
                        return Ok(None);
                    }
                    if values.iter().any(|v| !v.is_null() && same(v, needle)) {
                        return Ok(Some(true));
                    }
                    Ok(if values.iter().any(ScalarValue::is_null) {
                        None
                    } else {
                        Some(false)
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(bool_array(out))
        },
    )
    .udf()
}

pub(crate) fn array_position() -> ScalarUDF {
    Func::new(
        "array_position",
        coerce_array_and_element,
        |_| Ok(DataType::Int64),
        |call| {
            let l = lists(call.arg(0)?)?;
            let needles = row_scalars(call.arg(1)?, call.rows)?;
            let out = (0..call.rows)
                .map(|i| {
                    let Some(values) = elements(l, i)? else {
                        return Ok(None);
                    };
                    let needle = &needles[i];
                    if needle.is_null() {
                        return Ok(None);
                    }
                    let found = values.iter().position(|v| !v.is_null() && same(v, needle));
                    Ok(Some(
                        found.map_or(0, |p| i64::try_from(p).unwrap_or(i64::MAX) + 1),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

/// Applies `f` to the elements of every non-NULL array.
fn map_lists(
    call: &Call<'_>,
    element: &DataType,
    mut f: impl FnMut(Vec<ScalarValue>) -> Result<Vec<ScalarValue>>,
) -> Result<ArrayRef> {
    let l = lists(call.arg(0)?)?;
    let rows = (0..call.rows)
        .map(|i| elements(l, i)?.map(&mut f).transpose())
        .collect::<Result<Vec<_>>>()?;
    build_lists(rows, element)
}

fn distinct(values: Vec<ScalarValue>) -> Vec<ScalarValue> {
    let mut seen = HashSet::new();
    values.into_iter().filter(|v| seen.insert(v.clone())).collect()
}

fn coerce_list1(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    match types {
        [t] => as_list(t).map(|t| vec![t]).ok_or_else(|| not_found(name, types)),
        _ => Err(not_found(name, types)),
    }
}

fn element_of_call(call: &Call<'_>) -> Result<DataType> {
    element_type(call.return_type)
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("list expected".into()))
}

pub(crate) fn array_distinct() -> ScalarUDF {
    Func::new("array_distinct", coerce_list1, return_first, |call| {
        map_lists(call, &element_of_call(call)?, |v| Ok(distinct(v)))
    })
    .udf()
}

pub(crate) fn array_sort() -> ScalarUDF {
    Func::new("array_sort", coerce_list1, return_first, |call| {
        map_lists(call, &element_of_call(call)?, |mut v| {
            v.sort_by(|a, b| match (a.is_null(), b.is_null()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => compare(a, b),
            });
            Ok(v)
        })
    })
    .udf()
}

fn extreme(call: &Call<'_>, max: bool) -> Result<ArrayRef> {
    let l = lists(call.arg(0)?)?;
    let out = (0..call.rows)
        .map(|i| {
            let null = ScalarValue::try_from(call.return_type)?;
            let Some(values) = elements(l, i)? else {
                return Ok(null);
            };
            if values.is_empty() || values.iter().any(ScalarValue::is_null) {
                return Ok(null);
            }
            Ok(values
                .into_iter()
                .reduce(|a, b| {
                    let order = compare(&b, &a);
                    if (max && order.is_gt()) || (!max && order.is_lt()) {
                        b
                    } else {
                        a
                    }
                })
                .unwrap_or(null))
        })
        .collect::<Result<Vec<_>>>()?;
    scalars_to_array(out, call.return_type)
}

pub(crate) fn array_max() -> ScalarUDF {
    Func::new("array_max", coerce_list1, return_element, |call| {
        extreme(call, true)
    })
    .udf()
}

pub(crate) fn array_min() -> ScalarUDF {
    Func::new("array_min", coerce_list1, return_element, |call| {
        extreme(call, false)
    })
    .udf()
}

pub(crate) fn array_remove() -> ScalarUDF {
    Func::new("array_remove", coerce_array_and_element, return_first, |call| {
        let l = lists(call.arg(0)?)?;
        let needles = row_scalars(call.arg(1)?, call.rows)?;
        let element = element_of_call(call)?;
        let rows = (0..call.rows)
            .map(|i| {
                let Some(values) = elements(l, i)? else {
                    return Ok(None);
                };
                let needle = &needles[i];
                if needle.is_null() {
                    return Ok(None);
                }
                Ok(Some(
                    values
                        .into_iter()
                        .filter(|v| v.is_null() || !same(v, needle))
                        .collect(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        build_lists(rows, &element)
    })
    .udf()
}

fn slice_values(values: &[ScalarValue], start: i64, length: i64) -> Result<Vec<ScalarValue>> {
    if start == 0 {
        return Err(invalid_argument("SQL array indices start at 1"));
    }
    if length < 0 {
        return Err(invalid_argument(format!(
            "length must be greater than or equal to 0, but was {length}"
        )));
    }
    let len = i64::try_from(values.len()).unwrap_or(i64::MAX);
    let first = if start > 0 { start - 1 } else { len + start };
    if first < 0 || first >= len {
        return Ok(vec![]);
    }
    let end = first.saturating_add(length).min(len);
    let (Ok(a), Ok(b)) = (usize::try_from(first), usize::try_from(end)) else {
        return Ok(vec![]);
    };
    Ok(values[a..b].to_vec())
}

pub(crate) fn slice() -> ScalarUDF {
    Func::new(
        "slice",
        |name, types| match types {
            [a, s, l] => Ok(vec![
                as_list(a).ok_or_else(|| not_found(name, types))?,
                as_i64(s).ok_or_else(|| not_found(name, types))?,
                as_i64(l).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        return_first,
        |call| {
            let l = lists(call.arg(0)?)?;
            let starts = ints(call.arg(1)?)?;
            let lengths = ints(call.arg(2)?)?;
            let element = element_of_call(call)?;
            let rows = (0..call.rows)
                .map(|i| match (elements(l, i)?, starts.at(i), lengths.at(i)) {
                    (Some(v), Some(s), Some(n)) => slice_values(&v, s, n).map(Some),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            build_lists(rows, &element)
        },
    )
    .udf()
}

pub(crate) fn trim_array() -> ScalarUDF {
    Func::new(
        "trim_array",
        |name, types| match types {
            [a, n] => Ok(vec![
                as_list(a).ok_or_else(|| not_found(name, types))?,
                as_i64(n).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        return_first,
        |call| {
            let l = lists(call.arg(0)?)?;
            let counts = ints(call.arg(1)?)?;
            let element = element_of_call(call)?;
            let rows = (0..call.rows)
                .map(|i| match (elements(l, i)?, counts.at(i)) {
                    (Some(mut v), Some(n)) => {
                        let n = usize::try_from(n).ok().filter(|n| *n <= v.len()).ok_or_else(|| {
                            invalid_argument(format!(
                                "size must not be negative or greater than the array cardinality: {n}"
                            ))
                        })?;
                        v.truncate(v.len() - n);
                        Ok(Some(v))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            build_lists(rows, &element)
        },
    )
    .udf()
}

pub(crate) fn repeat() -> ScalarUDF {
    Func::new(
        "repeat",
        |name, types| match types {
            [e, n] => Ok(vec![
                if e.is_null() { DataType::Int64 } else { e.clone() },
                as_i64(n).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        |r| Ok(list_of(r.types.first().cloned().unwrap_or(DataType::Null))),
        |call| {
            let values = row_scalars(call.arg(0)?, call.rows)?;
            let counts = ints(call.arg(1)?)?;
            let element = call.arg(0)?.data_type().clone();
            let rows = (0..call.rows)
                .map(|i| {
                    let Some(n) = counts.at(i) else { return Ok(None) };
                    if n < 0 {
                        return Err(invalid_argument(
                            "count argument of repeat function must be greater than or equal to 0",
                        ));
                    }
                    if n > 10_000 {
                        return Err(invalid_argument(
                            "count argument of repeat function must be less than or equal to 10000",
                        ));
                    }
                    Ok(Some(vec![values[i].clone(); usize::try_from(n).unwrap_or(0)]))
                })
                .collect::<Result<Vec<_>>>()?;
            build_lists(rows, &element)
        },
    )
    .udf()
}

fn sequence_values(first: i64, last: i64, increment: i64) -> Result<Vec<i64>> {
    if increment == 0 {
        return Err(invalid_argument("step must not be zero"));
    }
    if (increment > 0 && first > last) || (increment < 0 && first < last) {
        return Err(invalid_argument(
            "sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start",
        ));
    }
    let count = (i128::from(last) - i128::from(first)) / i128::from(increment) + 1;
    if count > 10_000 {
        return Err(invalid_argument(
            "result of sequence function must not have more than 10000 entries",
        ));
    }
    let mut out = Vec::new();
    let mut v = i128::from(first);
    for _ in 0..count {
        out.push(i64::try_from(v).map_err(|_| invalid_argument("sequence overflow"))?);
        v += i128::from(increment);
    }
    Ok(out)
}

/// `sequence(start, stop, interval)` over dates or timestamps. A year-month
/// step is added to `start` k times (so month ends clamp as `date_add` does);
/// a day-time step walks in exact units.
fn interval_sequence(call: &Call<'_>) -> Result<ArrayRef> {
    use super::datetime::{Field, add_field, date_from_days, days_from_date, from_naive, to_naive};
    use datafusion::arrow::array::IntervalMonthDayNanoArray;
    use datafusion::arrow::datatypes::TimeUnit;

    let element = element_of_call(call)?;
    let steps = call
        .arg(2)?
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .ok_or_else(|| DataFusionError::Internal("sequence interval".into()))?;
    let unit = match &element {
        DataType::Timestamp(unit, _) => *unit,
        _ => TimeUnit::Nanosecond,
    };
    let per_day: i128 = match &element {
        DataType::Date32 => 1,
        _ => i128::from(super::support::per_second(unit)) * 86_400,
    };
    let as_longs = |a: &ArrayRef| datafusion::arrow::compute::cast(a, &DataType::Int64);
    let (firsts, lasts) = (as_longs(call.arg(0)?)?, as_longs(call.arg(1)?)?);
    let (firsts, lasts) = (ints(&firsts)?, ints(&lasts)?);
    let is_date = matches!(element, DataType::Date32);
    let to_scalar = |v: i64| -> Result<ScalarValue> {
        if is_date {
            return Ok(ScalarValue::Date32(Some(
                i32::try_from(v).map_err(|_| invalid_argument("sequence value is out of range"))?,
            )));
        }
        ScalarValue::Int64(Some(v)).cast_to(&element)
    };
    let rows = (0..call.rows)
        .map(|i| {
            let (Some(first), Some(last)) = (firsts.at(i), lasts.at(i)) else { return Ok(None) };
            if steps.is_null(i) {
                return Ok(None);
            }
            let step = steps.value(i);
            let exact = i128::from(step.days) * per_day
                + if is_date {
                    i128::from(step.nanoseconds) / 86_400_000_000_000
                } else {
                    i128::from(step.nanoseconds) * i128::from(super::support::per_second(unit)) / 1_000_000_000
                };
            if step.months != 0 && (step.days != 0 || step.nanoseconds != 0) {
                return Err(invalid_argument("sequence step must be a year-month or a day-time interval"));
            }
            if is_date && step.months == 0 && i128::from(step.nanoseconds) % 86_400_000_000_000 != 0 {
                return Err(invalid_argument("sequence step must be a day interval if start and end values are dates"));
            }
            if step.months == 0 {
                let increment = i64::try_from(exact).map_err(|_| invalid_argument("sequence step is too large"))?;
                let values = sequence_values(first, last, increment)?;
                return values.into_iter().map(to_scalar).collect::<Result<Vec<_>>>().map(Some);
            }
            // Year-month step: start + k × step, while not past stop.
            let months = i64::from(step.months);
            if (months > 0 && first > last) || (months < 0 && first < last) {
                return Err(invalid_argument("sequence stop value should be greater than or equal to start value if step is greater than zero otherwise stop should be less than or equal to start"));
            }
            let start = if is_date {
                let day = i32::try_from(first).ok().and_then(date_from_days);
                day.map(|d| d.and_time(chrono::NaiveTime::MIN))
            } else {
                to_naive(first, unit)
            }
            .ok_or_else(|| invalid_argument("sequence start is out of range"))?;
            let mut out = Vec::new();
            for k in 0..=10_000_i64 {
                let at = add_field(start, Field::Month, k * months)?;
                let raw = if is_date {
                    i64::from(days_from_date(at.date()))
                } else {
                    from_naive(at, unit).ok_or_else(|| invalid_argument("sequence value is out of range"))?
                };
                if (months > 0 && raw > last) || (months < 0 && raw < last) {
                    return Ok(Some(out));
                }
                if k == 10_000 {
                    break;
                }
                out.push(to_scalar(raw)?);
            }
            Err(invalid_argument("result of sequence function must not have more than 10000 entries"))
        })
        .collect::<Result<Vec<_>>>()?;
    build_lists(rows, &element)
}

fn coerce_sequence(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    match types {
        [
            a @ (DataType::Date32 | DataType::Timestamp(_, None)),
            b,
            DataType::Interval(_),
        ] if a == b || b.is_null() => Ok(vec![
            a.clone(),
            a.clone(),
            DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        ]),
        [
            DataType::Timestamp(u1, None),
            DataType::Timestamp(u2, None),
            DataType::Interval(_),
        ] => {
            let unit = if super::support::per_second(*u1) >= super::support::per_second(*u2) {
                *u1
            } else {
                *u2
            };
            let t = DataType::Timestamp(unit, None);
            Ok(vec![
                t.clone(),
                t,
                DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
            ])
        }
        [a, b]
            if a.is_null()
                || b.is_null()
                || (matches!(a, DataType::Date32) && matches!(b, DataType::Date32)) =>
        {
            if matches!(a, DataType::Date32) || matches!(b, DataType::Date32) {
                Ok(vec![DataType::Date32, DataType::Date32])
            } else {
                Ok(vec![DataType::Int64, DataType::Int64])
            }
        }
        [a, b] => Ok(vec![
            as_i64(a).ok_or_else(|| not_found(name, types))?,
            as_i64(b).ok_or_else(|| not_found(name, types))?,
        ]),
        [a, b, c] => Ok(vec![
            as_i64(a).ok_or_else(|| not_found(name, types))?,
            as_i64(b).ok_or_else(|| not_found(name, types))?,
            as_i64(c).ok_or_else(|| not_found(name, types))?,
        ]),
        _ => Err(not_found(name, types)),
    }
}

pub(crate) fn sequence() -> ScalarUDF {
    Func::new(
        "sequence",
        coerce_sequence,
        |r| Ok(list_of(r.types.first().cloned().unwrap_or(DataType::Int64))),
        |call| {
            if matches!(
                call.args.get(2).map(Array::data_type),
                Some(DataType::Interval(_))
            ) {
                return interval_sequence(call);
            }
            let is_date = matches!(call.arg(0)?.data_type(), DataType::Date32);
            let as_longs = |a: &ArrayRef| datafusion::arrow::compute::cast(a, &DataType::Int64);
            let firsts = as_longs(call.arg(0)?)?;
            let lasts = as_longs(call.arg(1)?)?;
            let (firsts, lasts) = (ints(&firsts)?, ints(&lasts)?);
            let increments = match call.args.get(2) {
                Some(s) => Some(ints(s)?),
                None => None,
            };
            let rows = (0..call.rows)
                .map(|i| {
                    let (Some(a), Some(b)) = (firsts.at(i), lasts.at(i)) else {
                        return Ok(None);
                    };
                    let step = match increments {
                        Some(s) => match s.at(i) {
                            Some(v) => v,
                            None => return Ok(None),
                        },
                        None => {
                            if a <= b {
                                1
                            } else {
                                -1
                            }
                        }
                    };
                    let values = sequence_values(a, b, step)?;
                    Ok(Some(
                        values
                            .into_iter()
                            .map(|v| {
                                if is_date {
                                    ScalarValue::Date32(i32::try_from(v).ok())
                                } else {
                                    ScalarValue::Int64(Some(v))
                                }
                            })
                            .collect(),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            build_lists(
                rows,
                if is_date {
                    &DataType::Date32
                } else {
                    &DataType::Int64
                },
            )
        },
    )
    .udf()
}

fn coerce_two_lists(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    let [a, b] = types else {
        return Err(not_found(name, types));
    };
    let (a, b) = (
        as_list(a).ok_or_else(|| not_found(name, types))?,
        as_list(b).ok_or_else(|| not_found(name, types))?,
    );
    let (ea, eb) = (
        element_type(&a).cloned().unwrap_or(DataType::Null),
        element_type(&b).cloned().unwrap_or(DataType::Null),
    );
    let common = comparison_coercion(&ea, &eb).ok_or_else(|| not_found(name, types))?;
    Ok(vec![list_of(common.clone()), list_of(common)])
}

fn set_function(
    call: &Call<'_>,
    op: fn(Vec<ScalarValue>, Vec<ScalarValue>) -> Vec<ScalarValue>,
) -> Result<ArrayRef> {
    let a = lists(call.arg(0)?)?;
    let b = lists(call.arg(1)?)?;
    let element = element_of_call(call)?;
    let rows = (0..call.rows)
        .map(|i| {
            Ok(match (elements(a, i)?, elements(b, i)?) {
                (Some(x), Some(y)) => Some(op(x, y)),
                _ => None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    build_lists(rows, &element)
}

pub(crate) fn array_union() -> ScalarUDF {
    Func::new("array_union", coerce_two_lists, return_first, |call| {
        set_function(call, |mut x, y| {
            x.extend(y);
            distinct(x)
        })
    })
    .udf()
}

pub(crate) fn array_intersect() -> ScalarUDF {
    Func::new("array_intersect", coerce_two_lists, return_first, |call| {
        set_function(call, |x, y| {
            let right: HashSet<ScalarValue> = y.into_iter().collect();
            distinct(x.into_iter().filter(|v| right.contains(v)).collect())
        })
    })
    .udf()
}

pub(crate) fn array_except() -> ScalarUDF {
    Func::new("array_except", coerce_two_lists, return_first, |call| {
        set_function(call, |x, y| {
            let right: HashSet<ScalarValue> = y.into_iter().collect();
            distinct(x.into_iter().filter(|v| !right.contains(v)).collect())
        })
    })
    .udf()
}

pub(crate) fn arrays_overlap() -> ScalarUDF {
    Func::new(
        "arrays_overlap",
        coerce_two_lists,
        |_| Ok(DataType::Boolean),
        |call| {
            let a = lists(call.arg(0)?)?;
            let b = lists(call.arg(1)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(x), Some(y)) = (elements(a, i)?, elements(b, i)?) else {
                        return Ok(None);
                    };
                    let right: HashSet<&ScalarValue> = y.iter().filter(|v| !v.is_null()).collect();
                    if x.iter().any(|v| !v.is_null() && right.contains(v)) {
                        return Ok(Some(true));
                    }
                    let has_null = x.iter().chain(y.iter()).any(ScalarValue::is_null);
                    Ok(if has_null { None } else { Some(false) })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(bool_array(out))
        },
    )
    .udf()
}

/// `array_join(array, delimiter [, null_replacement])`: elements as Trino
/// casts them to `varchar`; NULL elements are skipped unless replaced.
pub(crate) fn array_join() -> ScalarUDF {
    Func::new(
        "array_join",
        |name, types| match types {
            [a, d] => Ok(vec![
                as_list(a).ok_or_else(|| not_found(name, types))?,
                as_str(d).ok_or_else(|| not_found(name, types))?,
            ]),
            [a, d, n] => Ok(vec![
                as_list(a).ok_or_else(|| not_found(name, types))?,
                as_str(d).ok_or_else(|| not_found(name, types))?,
                as_str(n).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        |_| Ok(DataType::Utf8),
        |call| {
            let l = lists(call.arg(0)?)?;
            let delimiters = strings(call.arg(1)?)?;
            let replacements = match call.args.get(2) {
                Some(r) => Some(strings(r)?),
                None => None,
            };
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(values), Some(delimiter)) = (elements(l, i)?, delimiters.at(i)) else {
                        return Ok(None);
                    };
                    let replacement = match replacements {
                        Some(r) => match r.at(i) {
                            Some(v) => Some(v),
                            None => return Ok(None),
                        },
                        None => None,
                    };
                    let mut parts = Vec::with_capacity(values.len());
                    for v in values {
                        if v.is_null() {
                            if let Some(r) = replacement {
                                parts.push(r.to_owned());
                            }
                        } else {
                            parts.push(varchar_of(&v)?);
                        }
                    }
                    Ok(Some(parts.join(delimiter)))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

/// Trino's `CAST(value AS varchar)` for one value.
pub(crate) fn varchar_of(value: &ScalarValue) -> Result<String> {
    Ok(match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            s.clone()
        }
        ScalarValue::Float64(Some(v)) => super::cast::format_double(*v),
        ScalarValue::Float32(Some(v)) => super::cast::format_real(*v),
        ScalarValue::TimestampSecond(..)
        | ScalarValue::TimestampMillisecond(..)
        | ScalarValue::TimestampMicrosecond(..)
        | ScalarValue::TimestampNanosecond(..) => {
            let array = value.to_array()?;
            let ts = super::support::timestamps(&array)?;
            let raw = ts
                .at(0)
                .ok_or_else(|| DataFusionError::Internal("null timestamp".into()))?;
            let dt = super::datetime::to_naive(raw, ts.unit)
                .ok_or_else(|| invalid_argument("timestamp out of range"))?;
            match super::datetime::column_zone(ts.zone.as_ref())? {
                Some(zone) => format!(
                    "{} {}",
                    super::cast::render_timestamp(zone.to_local(dt), super::support::precision_of(ts.unit)),
                    zone.name()
                ),
                None => super::cast::render_timestamp(dt, super::support::precision_of(ts.unit)),
            }
        }
        other => other.to_string(),
    })
}

/// `concat(array…)`: NULL if any argument is NULL.
pub(crate) fn concat_lists(call: &Call<'_>) -> Result<ArrayRef> {
    let columns = call.args.iter().map(lists).collect::<Result<Vec<_>>>()?;
    let element = element_of_call(call)?;
    let rows = (0..call.rows)
        .map(|i| {
            let mut all = Vec::new();
            for c in &columns {
                match elements(c, i)? {
                    Some(v) => all.extend(v),
                    None => return Ok(None),
                }
            }
            Ok(Some(all))
        })
        .collect::<Result<Vec<_>>>()?;
    build_lists(rows, &element)
}

/// `reverse(array)`.
pub(crate) fn reverse_lists(call: &Call<'_>) -> Result<ArrayRef> {
    let element = element_of_call(call)?;
    map_lists(call, &element, |mut v| {
        v.reverse();
        Ok(v)
    })
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        cardinality(),
        element_at(),
        contains(),
        array_position(),
        array_distinct(),
        array_sort(),
        array_max(),
        array_min(),
        array_remove(),
        slice(),
        trim_array(),
        repeat(),
        sequence(),
        array_union(),
        array_intersect(),
        array_except(),
        arrays_overlap(),
        array_join(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints_of(values: &[i64]) -> Vec<ScalarValue> {
        values.iter().map(|v| ScalarValue::Int64(Some(*v))).collect()
    }

    #[test]
    fn slicing_and_indexing() {
        assert_eq!(
            slice_values(&ints_of(&[1, 2, 3, 4]), 2, 2).unwrap(),
            ints_of(&[2, 3])
        );
        assert_eq!(
            slice_values(&ints_of(&[1, 2, 3, 4]), -2, 5).unwrap(),
            ints_of(&[3, 4])
        );
        assert_eq!(slice_values(&ints_of(&[1, 2]), 5, 1).unwrap(), ints_of(&[]));
        assert!(slice_values(&ints_of(&[1]), 0, 1).is_err());
        assert!(slice_values(&ints_of(&[1]), 1, -1).is_err());
        assert_eq!(
            array_element(&ints_of(&[1, 2]), -1, &DataType::Int64).unwrap(),
            ScalarValue::Int64(Some(2))
        );
        assert_eq!(
            array_element(&ints_of(&[1, 2]), 3, &DataType::Int64).unwrap(),
            ScalarValue::Int64(None)
        );
        assert!(array_element(&ints_of(&[1]), 0, &DataType::Int64).is_err());
    }

    #[test]
    fn sequences() {
        assert_eq!(sequence_values(1, 3, 1).unwrap(), vec![1, 2, 3]);
        assert_eq!(sequence_values(3, 1, -1).unwrap(), vec![3, 2, 1]);
        assert_eq!(sequence_values(1, 6, 2).unwrap(), vec![1, 3, 5]);
        assert!(sequence_values(1, 3, 0).is_err());
        assert!(sequence_values(3, 1, 1).is_err());
        assert!(sequence_values(0, 20_000, 1).is_err());
    }

    #[test]
    fn distinct_keeps_first_occurrence() {
        assert_eq!(distinct(ints_of(&[3, 1, 3, 2, 1])), ints_of(&[3, 1, 2]));
    }
}
