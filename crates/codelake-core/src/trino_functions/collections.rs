//! More array and map functions: `contains_sequence`, `ngrams`,
//! `combinations`, `zip`, `map_from_entries`, `map_concat`, and the map
//! builder shared with the map-producing aggregates (`histogram`, `map_agg`).
//!
//! Elements are handled as `ScalarValue`s, like `array`: correct for every
//! element type, not vectorized.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ScalarUDF;

use super::aggregate::compare;
use super::array::{as_list, build_lists, element_type, elements, list_of, lists};
use super::support::{Call, Func, as_i64, bool_array, ints, invalid_argument, not_found};

const MAX_COMBINATION: i64 = 5;

// ---------------------------------------------------------------------------
// Maps
// ---------------------------------------------------------------------------

/// `map(K, V)` as Arrow spells it (`entries: struct<key, value>`).
pub(crate) fn map_type(key: DataType, value: DataType) -> DataType {
    let entries = Fields::from(vec![
        Field::new("key", key, false),
        Field::new("value", value, true),
    ]);
    DataType::Map(
        Arc::new(Field::new("entries", DataType::Struct(entries), false)),
        false,
    )
}

fn map_parts(t: &DataType) -> Option<(&DataType, &DataType)> {
    let DataType::Map(entries, _) = t else { return None };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    match fields.iter().collect::<Vec<_>>().as_slice() {
        [key, value] => Some((key.data_type(), value.data_type())),
        _ => None,
    }
}

fn values_array(values: Vec<ScalarValue>, data_type: &DataType) -> Result<ArrayRef> {
    if values.is_empty() {
        return Ok(new_empty_array(data_type));
    }
    ScalarValue::iter_to_array(values)
}

/// Builds a map column from per-row entries (keys already unique).
pub(crate) fn build_maps(
    rows: Vec<Option<Vec<(ScalarValue, ScalarValue)>>>,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<ArrayRef> {
    let map = map_type(key_type.clone(), value_type.clone());
    let DataType::Map(entries_field, _) = &map else {
        return Err(DataFusionError::Internal("map type".into()));
    };
    let DataType::Struct(fields) = entries_field.data_type() else {
        return Err(DataFusionError::Internal("map entries".into()));
    };
    let mut offsets = vec![0_i32];
    let mut valid = Vec::with_capacity(rows.len());
    let (mut keys, mut values) = (Vec::new(), Vec::new());
    for row in rows {
        valid.push(row.is_some());
        for (key, value) in row.unwrap_or_default() {
            keys.push(key.cast_to(key_type)?);
            values.push(value.cast_to(value_type)?);
        }
        let end = i32::try_from(keys.len()).map_err(|_| DataFusionError::Internal("map too large".into()))?;
        offsets.push(end);
    }
    let entries = StructArray::try_new(
        fields.clone(),
        vec![values_array(keys, key_type)?, values_array(values, value_type)?],
        None,
    )?;
    let map = MapArray::try_new(
        Arc::clone(entries_field),
        OffsetBuffer::new(offsets.into()),
        entries,
        Some(NullBuffer::from(valid)),
        false,
    )?;
    Ok(Arc::new(map))
}

/// Entries of row `row` of a map column, `None` when the map is NULL.
fn map_entries_at(maps: &MapArray, row: usize) -> Result<Option<Vec<(ScalarValue, ScalarValue)>>> {
    if maps.is_null(row) {
        return Ok(None);
    }
    let entries = maps.value(row);
    let (keys, values) = (entries.column(0), entries.column(1));
    (0..entries.len())
        .map(|i| {
            Ok((
                ScalarValue::try_from_array(keys, i)?,
                ScalarValue::try_from_array(values, i)?,
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn map_arrays(array: &ArrayRef) -> Result<&MapArray> {
    array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| DataFusionError::Internal(format!("expected map, got {}", array.data_type())))
}

fn render_key(key: &ScalarValue) -> String {
    super::array::varchar_of(key).unwrap_or_else(|_| key.to_string())
}

/// Adds an entry, rejecting NULL and duplicate keys as Trino does.
fn insert_unique(
    entries: &mut Vec<(ScalarValue, ScalarValue)>,
    key: ScalarValue,
    value: ScalarValue,
) -> Result<()> {
    if key.is_null() {
        return Err(invalid_argument("map key cannot be null"));
    }
    if entries.iter().any(|(k, _)| compare(k, &key).is_eq()) {
        return Err(invalid_argument(format!(
            "Duplicate map keys ({}) are not allowed",
            render_key(&key)
        )));
    }
    entries.push((key, value));
    Ok(())
}

pub(crate) fn map_from_entries() -> ScalarUDF {
    Func::new(
        "map_from_entries",
        |name, types| match types {
            [t] => {
                let list = as_list(t).ok_or_else(|| not_found(name, types))?;
                match element_type(&list) {
                    Some(DataType::Struct(fields)) if fields.len() == 2 => Ok(vec![list]),
                    Some(DataType::Null) if t.is_null() => Ok(vec![list]),
                    _ => Err(not_found(name, types)),
                }
            }
            _ => Err(not_found(name, types)),
        },
        |r| match r.types.first().and_then(element_type) {
            Some(DataType::Struct(fields)) if fields.len() == 2 => Ok(map_type(
                fields[0].data_type().clone(),
                fields[1].data_type().clone(),
            )),
            _ => Ok(map_type(DataType::Int64, DataType::Int64)),
        },
        |call| {
            let (key_type, value_type) = map_parts(call.return_type)
                .ok_or_else(|| DataFusionError::Internal("map_from_entries type".into()))?;
            let l = lists(call.arg(0)?)?;
            let rows = (0..call.rows)
                .map(|i| {
                    let Some(items) = elements(l, i)? else {
                        return Ok(None);
                    };
                    let mut entries = Vec::with_capacity(items.len());
                    for item in items {
                        let ScalarValue::Struct(row) = &item else {
                            return Err(DataFusionError::Internal("map entry is not a row".into()));
                        };
                        if row.is_null(0) {
                            return Err(invalid_argument("map entry cannot be null"));
                        }
                        let key = ScalarValue::try_from_array(row.column(0), 0)?;
                        let value = ScalarValue::try_from_array(row.column(1), 0)?;
                        insert_unique(&mut entries, key, value)?;
                    }
                    Ok(Some(entries))
                })
                .collect::<Result<Vec<_>>>()?;
            build_maps(rows, key_type, value_type)
        },
    )
    .udf()
}

pub(crate) fn map_concat() -> ScalarUDF {
    Func::new(
        "map_concat",
        |name, types| {
            let maps: Vec<&DataType> = types.iter().filter(|t| !t.is_null()).collect();
            let Some(first) = maps.first() else {
                return Err(not_found(name, types));
            };
            if types.len() < 2 || maps.iter().any(|t| map_parts(t).is_none()) {
                return Err(not_found(name, types));
            }
            let common = (*first).clone();
            Ok(vec![common; types.len()])
        },
        |r| {
            r.types
                .first()
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("map_concat".into()))
        },
        |call| {
            let (key_type, value_type) = map_parts(call.return_type)
                .ok_or_else(|| DataFusionError::Internal("map_concat type".into()))?;
            let maps = call.args.iter().map(map_arrays).collect::<Result<Vec<_>>>()?;
            let rows = (0..call.rows)
                .map(|i| {
                    let mut merged: Vec<(ScalarValue, ScalarValue)> = Vec::new();
                    for map in &maps {
                        let Some(entries) = map_entries_at(map, i)? else {
                            return Ok(None);
                        };
                        for (key, value) in entries {
                            // The last map wins.
                            match merged.iter_mut().find(|(k, _)| compare(k, &key).is_eq()) {
                                Some(slot) => slot.1 = value,
                                None => merged.push((key, value)),
                            }
                        }
                    }
                    Ok(Some(merged))
                })
                .collect::<Result<Vec<_>>>()?;
            build_maps(rows, key_type, value_type)
        },
    )
    .udf()
}

// ---------------------------------------------------------------------------
// Arrays
// ---------------------------------------------------------------------------

fn same_or_both_null(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a.is_null(), b.is_null()) {
        (true, true) => true,
        (false, false) => compare(a, b).is_eq(),
        _ => false,
    }
}

fn coerce_same_lists(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    let [a, b] = types else {
        return Err(not_found(name, types));
    };
    let (a, b) = (
        as_list(a).ok_or_else(|| not_found(name, types))?,
        as_list(b).ok_or_else(|| not_found(name, types))?,
    );
    let (ea, eb) = (element_type(&a).cloned(), element_type(&b).cloned());
    let common = match (ea, eb) {
        (Some(x), Some(y)) if x == y => x,
        (Some(x), Some(y)) => datafusion::logical_expr::type_coercion::binary::comparison_coercion(&x, &y)
            .ok_or_else(|| not_found(name, types))?,
        _ => return Err(not_found(name, types)),
    };
    Ok(vec![list_of(common.clone()), list_of(common)])
}

pub(crate) fn contains_sequence() -> ScalarUDF {
    Func::new(
        "contains_sequence",
        coerce_same_lists,
        super::support::BOOLEAN,
        |call| {
            let (haystack, needle) = (lists(call.arg(0)?)?, lists(call.arg(1)?)?);
            let out = (0..call.rows)
                .map(|i| {
                    let (Some(h), Some(n)) = (elements(haystack, i)?, elements(needle, i)?) else {
                        return Ok(None);
                    };
                    if n.is_empty() {
                        return Ok(Some(true));
                    }
                    Ok(Some(
                        h.windows(n.len())
                            .any(|w| w.iter().zip(&n).all(|(a, b)| same_or_both_null(a, b))),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(bool_array(out))
        },
    )
    .udf()
}

fn coerce_list_and_count(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    match types {
        [list, n] => Ok(vec![
            as_list(list).ok_or_else(|| not_found(name, types))?,
            as_i64(n).ok_or_else(|| not_found(name, types))?,
        ]),
        _ => Err(not_found(name, types)),
    }
}

fn return_list_of_first(r: &super::support::Ret<'_>) -> Result<DataType> {
    r.types
        .first()
        .cloned()
        .map(list_of)
        .ok_or_else(|| DataFusionError::Internal("missing argument".into()))
}

/// Applies `f(elements, n)` per row, producing arrays of arrays.
/// Groups of elements built from one array (`ngrams`, `combinations`).
type Groups = Result<Vec<Vec<ScalarValue>>>;

fn nested(call: &Call<'_>, f: fn(&[ScalarValue], i64) -> Groups) -> Result<ArrayRef> {
    let l = lists(call.arg(0)?)?;
    let counts = ints(call.arg(1)?)?;
    let inner = element_type(call.return_type)
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("nested list type".into()))?;
    let element = element_type(&inner)
        .cloned()
        .ok_or_else(|| DataFusionError::Internal("nested list type".into()))?;
    let rows = (0..call.rows)
        .map(|i| {
            let (Some(values), Some(n)) = (elements(l, i)?, super::support::At::at(counts, i)) else {
                return Ok(None);
            };
            let groups = f(&values, n)?
                .into_iter()
                .map(|g| ScalarValue::List(ScalarValue::new_list_nullable(&g, &element)))
                .collect();
            Ok(Some(groups))
        })
        .collect::<Result<Vec<_>>>()?;
    build_lists(rows, &inner)
}

fn ngram_groups(values: &[ScalarValue], n: i64) -> Groups {
    if n <= 0 {
        return Err(invalid_argument("N must be positive"));
    }
    let n = usize::try_from(n).unwrap_or(usize::MAX);
    if n >= values.len() {
        return Ok(vec![values.to_vec()]);
    }
    Ok(values.windows(n).map(<[ScalarValue]>::to_vec).collect())
}

pub(crate) fn ngrams() -> ScalarUDF {
    Func::new("ngrams", coerce_list_and_count, return_list_of_first, |call| {
        nested(call, ngram_groups)
    })
    .udf()
}

fn combination_groups(values: &[ScalarValue], n: i64) -> Groups {
    if n < 0 {
        return Err(invalid_argument(format!(
            "combination size must not be negative: {n}"
        )));
    }
    if n > MAX_COMBINATION {
        return Err(invalid_argument(format!(
            "combination size must not exceed {MAX_COMBINATION}: {n}"
        )));
    }
    let k = usize::try_from(n).unwrap_or(0);
    if k > values.len() {
        return Ok(vec![]);
    }
    let mut out = Vec::new();
    let mut indices: Vec<usize> = (0..k).collect();
    loop {
        out.push(indices.iter().filter_map(|&i| values.get(i).cloned()).collect());
        // Next combination in lexicographic order of positions.
        let Some(pivot) = (0..k).rev().find(|&i| indices[i] != i + values.len() - k) else {
            break;
        };
        indices[pivot] += 1;
        for j in pivot + 1..k {
            indices[j] = indices[j - 1] + 1;
        }
    }
    Ok(out)
}

pub(crate) fn combinations() -> ScalarUDF {
    Func::new(
        "combinations",
        coerce_list_and_count,
        return_list_of_first,
        |call| nested(call, combination_groups),
    )
    .udf()
}

pub(crate) fn zip() -> ScalarUDF {
    Func::new(
        "zip",
        |name, types| {
            if !(2..=5).contains(&types.len()) {
                return Err(not_found(name, types));
            }
            types
                .iter()
                .map(|t| as_list(t).ok_or_else(|| not_found(name, types)))
                .collect()
        },
        |r| {
            let fields: Vec<Field> = r
                .types
                .iter()
                .enumerate()
                .map(|(i, t)| {
                    Field::new(
                        format!("c{i}"),
                        element_type(t).cloned().unwrap_or(DataType::Null),
                        true,
                    )
                })
                .collect();
            Ok(list_of(DataType::Struct(Fields::from(fields))))
        },
        |call| {
            let row_type = element_type(call.return_type)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("zip type".into()))?;
            let DataType::Struct(fields) = &row_type else {
                return Err(DataFusionError::Internal("zip row type".into()));
            };
            let inputs = call.args.iter().map(lists).collect::<Result<Vec<_>>>()?;
            let rows = (0..call.rows)
                .map(|i| {
                    let mut columns = Vec::with_capacity(inputs.len());
                    for input in &inputs {
                        let Some(values) = elements(input, i)? else {
                            return Ok(None);
                        };
                        columns.push(values);
                    }
                    let len = columns.iter().map(Vec::len).max().unwrap_or(0);
                    let zipped = (0..len)
                        .map(|j| {
                            let arrays = columns
                                .iter()
                                .zip(fields.iter())
                                .map(|(c, f)| match c.get(j) {
                                    Some(v) => v.to_array(),
                                    None => ScalarValue::try_from(f.data_type())?.to_array(),
                                })
                                .collect::<Result<Vec<_>>>()?;
                            Ok(ScalarValue::Struct(Arc::new(StructArray::try_new(
                                fields.clone(),
                                arrays,
                                None,
                            )?)))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Some(zipped))
                })
                .collect::<Result<Vec<_>>>()?;
            build_lists(rows, &row_type)
        },
    )
    .udf()
}

/// `x[i]` / `m[k]` (Trino's subscript operator); planned from the SQL text.
pub(crate) const SUBSCRIPT: &str = "codelake_trino_subscript";

fn subscript_element(values: &[ScalarValue], index: i64, element: &DataType) -> Result<ScalarValue> {
    if index == 0 {
        return Err(invalid_argument("SQL array indices start at 1"));
    }
    if index < 0 {
        return Err(invalid_argument(format!("Array subscript is negative: {index}")));
    }
    let length = i64::try_from(values.len()).unwrap_or(i64::MAX);
    if index > length {
        return Err(invalid_argument(format!(
            "Array subscript must be less than or equal to array length: {index} > {length}"
        )));
    }
    usize::try_from(index - 1)
        .ok()
        .and_then(|i| values.get(i).cloned())
        .map_or_else(|| ScalarValue::try_from(element), Ok)
}

pub(crate) fn subscript() -> ScalarUDF {
    Func::new(
        SUBSCRIPT,
        |name, types| match types {
            [map @ DataType::Map(..), _] => {
                let (key, _) = map_parts(map).ok_or_else(|| not_found(name, types))?;
                Ok(vec![map.clone(), key.clone()])
            }
            [list, index] => Ok(vec![
                as_list(list).ok_or_else(|| not_found(name, types))?,
                as_i64(index).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        |r| match r.types.first() {
            Some(map @ DataType::Map(..)) => map_parts(map)
                .map(|(_, v)| v.clone())
                .ok_or_else(|| DataFusionError::Internal("map value".into())),
            Some(list) => element_type(list)
                .cloned()
                .ok_or_else(|| DataFusionError::Internal("array element".into())),
            None => Err(DataFusionError::Internal("subscript".into())),
        },
        |call| {
            let (container, key) = (call.arg(0)?, call.arg(1)?);
            let element = call.return_type;
            let values = if let Some(maps) = container.as_any().downcast_ref::<MapArray>() {
                (0..call.rows)
                    .map(|i| {
                        let key = ScalarValue::try_from_array(key, i)?;
                        let Some(entries) = map_entries_at(maps, i)? else {
                            return ScalarValue::try_from(element);
                        };
                        if key.is_null() {
                            return ScalarValue::try_from(element);
                        }
                        entries
                            .into_iter()
                            .find(|(k, _)| compare(k, &key).is_eq())
                            .map(|(_, v)| v)
                            .ok_or_else(|| {
                                invalid_argument(format!("Key not present in map: {}", render_key(&key)))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                let l = lists(container)?;
                let indexes = ints(key)?;
                (0..call.rows)
                    .map(|i| match (elements(l, i)?, super::support::At::at(indexes, i)) {
                        (Some(values), Some(index)) => subscript_element(&values, index, element),
                        _ => ScalarValue::try_from(element),
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            if values.is_empty() {
                return Ok(datafusion::arrow::array::new_empty_array(element));
            }
            ScalarValue::iter_to_array(values)
        },
    )
    .udf()
}

/// Trino's `map(keys, values)`: equal lengths, no NULL or repeated key.
pub(crate) fn map_constructor() -> ScalarUDF {
    Func::new(
        "map",
        |name, types| match types {
            [k, v] => Ok(vec![
                as_list(k).ok_or_else(|| not_found(name, types))?,
                as_list(v).ok_or_else(|| not_found(name, types))?,
            ]),
            _ => Err(not_found(name, types)),
        },
        |r| match r.types {
            [k, v] => Ok(map_type(
                element_type(k).cloned().unwrap_or(DataType::Null),
                element_type(v).cloned().unwrap_or(DataType::Null),
            )),
            _ => Err(DataFusionError::Internal("map arguments".into())),
        },
        |call| {
            let (key_type, value_type) =
                map_parts(call.return_type).ok_or_else(|| DataFusionError::Internal("map type".into()))?;
            let (keys, values) = (lists(call.arg(0)?)?, lists(call.arg(1)?)?);
            let rows = (0..call.rows)
                .map(|i| {
                    let (Some(k), Some(v)) = (elements(keys, i)?, elements(values, i)?) else {
                        return Ok(None);
                    };
                    if k.len() != v.len() {
                        return Err(invalid_argument("Key and value arrays must be the same length"));
                    }
                    let mut entries = Vec::with_capacity(k.len());
                    for (key, value) in k.into_iter().zip(v) {
                        insert_unique(&mut entries, key, value)?;
                    }
                    Ok(Some(entries))
                })
                .collect::<Result<Vec<_>>>()?;
            build_maps(rows, key_type, value_type)
        },
    )
    .udf()
}

/// `UNNEST(x) AS t(a, b, …)` with one argument and several columns: Trino
/// expands a map into key and value columns and an array of rows into the
/// row's fields. The AST rewrite (see `syntax`) unnests
/// `codelake_unnest_part(x, 0)`, `codelake_unnest_part(x, 1)`, … instead,
/// each an array the engine's `UNNEST` zips with the others.
pub(crate) const UNNEST_PART: &str = "codelake_unnest_part";

fn unnest_part_index(r: &super::support::Ret<'_>) -> Result<usize> {
    match r.scalars.get(1).copied().flatten() {
        Some(ScalarValue::Int64(Some(i))) => {
            usize::try_from(*i).map_err(|_| invalid_argument("negative part"))
        }
        _ => Err(DataFusionError::Plan(format!(
            "{UNNEST_PART} needs a constant part"
        ))),
    }
}

fn unnest_part_type(input: &DataType, index: usize) -> Option<DataType> {
    if let Some((key, value)) = map_parts(input) {
        return match index {
            0 => Some(list_of(key.clone())),
            1 => Some(list_of(value.clone())),
            _ => None,
        };
    }
    match element_type(input)? {
        DataType::Struct(fields) => fields.get(index).map(|f| list_of(f.data_type().clone())),
        element if index == 0 => Some(list_of(element.clone())),
        _ => None,
    }
}

pub(crate) fn unnest_part() -> ScalarUDF {
    Func::new(
        UNNEST_PART,
        |name, types| match types {
            [t @ DataType::Map(..), i] if super::support::is_integer(i) => {
                Ok(vec![t.clone(), DataType::Int64])
            }
            [t, i] if super::support::is_integer(i) => Ok(vec![
                as_list(t).ok_or_else(|| not_found(name, types))?,
                DataType::Int64,
            ]),
            _ => Err(not_found(name, types)),
        },
        |r| {
            let index = unnest_part_index(r)?;
            let input = r
                .types
                .first()
                .ok_or_else(|| DataFusionError::Internal("unnest argument".into()))?;
            unnest_part_type(input, index).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "NOT_SUPPORTED: UNNEST of {} has no column {}",
                    super::support::trino_type_name(input),
                    index + 1
                ))
            })
        },
        |call| {
            let input = call.arg(0)?;
            let index = ints(call.arg(1)?)?
                .iter()
                .flatten()
                .next()
                .and_then(|i| usize::try_from(i).ok())
                .unwrap_or(0);
            let DataType::List(field) = call.return_type else {
                return Err(DataFusionError::Internal("unnest part type".into()));
            };
            let (offsets, values, nulls): (OffsetBuffer<i32>, ArrayRef, Option<NullBuffer>) =
                if let Some(maps) = input.as_any().downcast_ref::<MapArray>() {
                    let entries = maps.entries();
                    (
                        maps.offsets().clone(),
                        Arc::clone(entries.column(index)),
                        maps.nulls().cloned(),
                    )
                } else {
                    let list = lists(input)?;
                    let values = match list.values().as_any().downcast_ref::<StructArray>() {
                        Some(rows) => Arc::clone(rows.column(index)),
                        None => Arc::clone(list.values()),
                    };
                    (list.offsets().clone(), values, list.nulls().cloned())
                };
            Ok(Arc::new(datafusion::arrow::array::ListArray::try_new(
                Arc::clone(field),
                offsets,
                values,
                nulls,
            )?))
        },
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    vec![
        unnest_part(),
        subscript(),
        map_constructor(),
        map_from_entries(),
        map_concat(),
        contains_sequence(),
        ngrams(),
        combinations(),
        zip(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints_of(values: &[i64]) -> Vec<ScalarValue> {
        values.iter().map(|v| ScalarValue::Int64(Some(*v))).collect()
    }

    #[test]
    fn combinations_follow_positions() {
        let groups = combination_groups(&ints_of(&[1, 2, 3]), 2).unwrap();
        assert_eq!(groups, vec![ints_of(&[1, 2]), ints_of(&[1, 3]), ints_of(&[2, 3])]);
        assert_eq!(
            combination_groups(&ints_of(&[1, 2]), 0).unwrap(),
            vec![Vec::<ScalarValue>::new()]
        );
        assert!(combination_groups(&ints_of(&[1, 2]), 3).unwrap().is_empty());
        assert!(combination_groups(&ints_of(&[1]), 6).is_err());
    }

    #[test]
    fn ngrams_slide_and_cap_at_the_array() {
        assert_eq!(
            ngram_groups(&ints_of(&[1, 2, 3]), 2).unwrap(),
            vec![ints_of(&[1, 2]), ints_of(&[2, 3])]
        );
        assert_eq!(
            ngram_groups(&ints_of(&[1, 2]), 5).unwrap(),
            vec![ints_of(&[1, 2])]
        );
        assert!(ngram_groups(&ints_of(&[1]), 0).is_err());
    }

    #[test]
    fn map_entries_reject_null_and_duplicate_keys() {
        let mut entries = Vec::new();
        insert_unique(&mut entries, ScalarValue::from("a"), ScalarValue::Int64(Some(1))).unwrap();
        assert!(insert_unique(&mut entries, ScalarValue::from("a"), ScalarValue::Int64(Some(2))).is_err());
        assert!(insert_unique(&mut entries, ScalarValue::Utf8(None), ScalarValue::Int64(Some(2))).is_err());
    }
}
