//! String functions (Trino "String functions and operators").
//!
//! Trino counts positions and lengths in Unicode code points, returns
//! `bigint` for lengths and positions, and returns NULL when any argument is
//! NULL (except `concat_ws`, which skips NULL values).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ScalarUDF;

use super::support::{
    At, BIGINT, BOOLEAN, Call, Func, UTF8, as_bin, as_i64, as_str, binaries, binary_array, bool_array,
    coerce_any, coerce_each, int_array, ints, invalid_argument, is_binary, is_string, not_found,
    string_array, strings,
};

fn string_list() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true)))
}

/// `varchar` or `varbinary` result, following the first argument.
const SAME_AS_FIRST: super::support::ReturnFn = |r| {
    Ok(match r.types.first() {
        Some(DataType::Binary) => DataType::Binary,
        _ => DataType::Utf8,
    })
};

// ---------------------------------------------------------------------------
// Code point helpers
// ---------------------------------------------------------------------------

/// Byte offset of code point `n` (0-based), or `None` past the end.
fn offset_of(text: &str, n: usize) -> Option<usize> {
    if n == 0 {
        return Some(0);
    }
    text.char_indices()
        .nth(n)
        .map(|(i, _)| i)
        .or_else(|| (text.chars().count() == n).then_some(text.len()))
}

/// Trino `substr(varchar, start, length)`.
pub(crate) fn substr_text(text: &str, start: i64, length: i64) -> &str {
    if start == 0 || length <= 0 || text.is_empty() {
        return "";
    }
    let count = i64::try_from(text.chars().count()).unwrap_or(i64::MAX);
    let first = if start > 0 {
        start - 1
    } else {
        let from_end = start + count;
        if from_end < 0 {
            return "";
        }
        from_end
    };
    if first >= count {
        return "";
    }
    let last = first.saturating_add(length).min(count);
    let (Ok(first), Ok(last)) = (usize::try_from(first), usize::try_from(last)) else {
        return "";
    };
    let begin = offset_of(text, first).unwrap_or(text.len());
    let end = offset_of(text, last).unwrap_or(text.len());
    &text[begin..end]
}

/// Trino `substr(varbinary, start, length)`.
fn substr_bytes(bytes: &[u8], start: i64, length: i64) -> &[u8] {
    if start == 0 || length <= 0 || bytes.is_empty() {
        return &[];
    }
    let len = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
    let first = if start > 0 { start - 1 } else { start + len };
    if first < 0 || first >= len {
        return &[];
    }
    let last = first.saturating_add(length).min(len);
    match (usize::try_from(first), usize::try_from(last)) {
        (Ok(a), Ok(b)) => &bytes[a..b],
        _ => &[],
    }
}

/// Java's `Character.isWhitespace`, which Trino's `trim` uses.
pub(crate) fn is_java_whitespace(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r' | '\u{1C}'..='\u{1F}')
        || (c.is_whitespace() && !matches!(c, '\u{85}' | '\u{A0}' | '\u{2007}' | '\u{202F}'))
}

/// Java's simple (one code point) case mapping.
pub(crate) fn simple_lower(c: char) -> char {
    if c == '\u{130}' {
        return 'i';
    }
    let mut mapped = c.to_lowercase();
    match (mapped.next(), mapped.next()) {
        (Some(single), None) => single,
        _ => c,
    }
}

pub(crate) fn simple_upper(c: char) -> char {
    let mut mapped = c.to_uppercase();
    match (mapped.next(), mapped.next()) {
        (Some(single), None) => single,
        _ => c,
    }
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

fn map_str1(call: &Call<'_>, f: impl Fn(&str) -> String) -> Result<ArrayRef> {
    let a = strings(call.arg(0)?)?;
    Ok(string_array((0..call.rows).map(|i| a.at(i).map(&f)).collect()))
}

fn coerce_str1(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    coerce_each(name, types, &[as_str])
}

pub(crate) fn lower() -> ScalarUDF {
    Func::new("lower", coerce_str1, UTF8, |call| {
        map_str1(call, |s| s.chars().map(simple_lower).collect())
    })
    .keeps_varchar_length()
    .udf()
}

pub(crate) fn upper() -> ScalarUDF {
    Func::new("upper", coerce_str1, UTF8, |call| {
        map_str1(call, |s| s.chars().map(simple_upper).collect())
    })
    .keeps_varchar_length()
    .udf()
}

pub(crate) fn reverse() -> ScalarUDF {
    Func::new(
        "reverse",
        |name, types| match types {
            [DataType::List(_)] => Ok(types.to_vec()),
            _ => coerce_any(name, types, &[&[as_str], &[as_bin]]),
        },
        |r| match r.types.first() {
            Some(t @ DataType::List(_)) => Ok(t.clone()),
            _ => SAME_AS_FIRST(r),
        },
        |call| {
            let arg = call.arg(0)?;
            if let DataType::List(_) = arg.data_type() {
                return super::array::reverse_lists(call);
            }
            if let DataType::Binary = arg.data_type() {
                let b = binaries(arg)?;
                let out: Vec<Option<Vec<u8>>> = (0..call.rows)
                    .map(|i| b.at(i).map(|v| v.iter().rev().copied().collect()))
                    .collect();
                return Ok(binary_array(&out));
            }
            map_str1(call, |s| s.chars().rev().collect())
        },
    )
    .udf()
}

pub(crate) fn length() -> ScalarUDF {
    Func::new(
        "length",
        |name, types| coerce_any(name, types, &[&[as_str], &[as_bin]]),
        BIGINT,
        |call| {
            let arg = call.arg(0)?;
            if let DataType::Binary = arg.data_type() {
                let b = binaries(arg)?;
                return Ok(int_array(
                    (0..call.rows)
                        .map(|i| b.at(i).map(|v| i64::try_from(v.len()).unwrap_or(i64::MAX)))
                        .collect(),
                ));
            }
            let a = strings(arg)?;
            Ok(int_array(
                (0..call.rows)
                    .map(|i| {
                        a.at(i)
                            .map(|s| i64::try_from(s.chars().count()).unwrap_or(i64::MAX))
                    })
                    .collect(),
            ))
        },
    )
    .udf()
}

fn invoke_substr(call: &Call<'_>) -> Result<ArrayRef> {
    let starts = ints(call.arg(1)?)?;
    let lengths = match call.args.get(2) {
        Some(a) => Some(ints(a)?),
        None => None,
    };
    let length_at = |i: usize| -> Option<i64> {
        match lengths {
            Some(l) => l.at(i),
            None => Some(i64::MAX),
        }
    };
    let arg = call.arg(0)?;
    if let DataType::Binary = arg.data_type() {
        let b = binaries(arg)?;
        let out: Vec<Option<Vec<u8>>> = (0..call.rows)
            .map(|i| match (b.at(i), starts.at(i), length_at(i)) {
                (Some(v), Some(s), Some(l)) => Some(substr_bytes(v, s, l).to_vec()),
                _ => None,
            })
            .collect();
        return Ok(binary_array(&out));
    }
    let a = strings(arg)?;
    Ok(string_array(
        (0..call.rows)
            .map(|i| match (a.at(i), starts.at(i), length_at(i)) {
                (Some(v), Some(s), Some(l)) => Some(substr_text(v, s, l).to_owned()),
                _ => None,
            })
            .collect(),
    ))
}

pub(crate) fn substr() -> ScalarUDF {
    Func::new(
        "substr",
        |name, types| {
            coerce_any(
                name,
                types,
                &[
                    &[as_str, as_i64],
                    &[as_str, as_i64, as_i64],
                    &[as_bin, as_i64],
                    &[as_bin, as_i64, as_i64],
                ],
            )
        },
        SAME_AS_FIRST,
        invoke_substr,
    )
    .alias("substring")
    .keeps_varchar_length()
    .udf()
}

/// `trim`, `ltrim`, `rtrim` (and `btrim`, which `TRIM(…)` syntax plans):
/// whitespace, or the characters of the second argument.
fn trim_function(name: &'static str, left: bool, right: bool) -> ScalarUDF {
    let invoke = match (left, right) {
        (true, true) => |call: &Call<'_>| trim_invoke(call, true, true),
        (true, false) => |call: &Call<'_>| trim_invoke(call, true, false),
        _ => |call: &Call<'_>| trim_invoke(call, false, true),
    };
    Func::new(
        name,
        |name, types| coerce_any(name, types, &[&[as_str], &[as_str, as_str]]),
        UTF8,
        invoke,
    )
    .keeps_varchar_length()
    .udf()
}

fn trim_invoke(call: &Call<'_>, left: bool, right: bool) -> Result<ArrayRef> {
    let a = strings(call.arg(0)?)?;
    let chars = match call.args.get(1) {
        Some(c) => Some(strings(c)?),
        None => None,
    };
    let out = (0..call.rows)
        .map(|i| {
            let text = a.at(i)?;
            let set: Option<Vec<char>> = match chars {
                Some(c) => Some(c.at(i)?.chars().collect()),
                None => None,
            };
            let matches = |c: char| {
                set.as_ref()
                    .map_or_else(|| is_java_whitespace(c), |s| s.contains(&c))
            };
            let mut t = text;
            if left {
                t = t.trim_start_matches(matches);
            }
            if right {
                t = t.trim_end_matches(matches);
            }
            Some(t.to_owned())
        })
        .collect();
    Ok(string_array(out))
}

pub(crate) fn trims() -> Vec<ScalarUDF> {
    vec![
        trim_function("trim", true, true),
        trim_function("btrim", true, true),
        trim_function("ltrim", true, false),
        trim_function("rtrim", false, true),
    ]
}

/// `concat(varchar…)` / `concat(varbinary…)`: NULL if any argument is NULL.
fn coerce_concat(name: &str, types: &[DataType]) -> Result<Vec<DataType>> {
    if types.is_empty() {
        return Err(not_found(name, types));
    }
    if types.iter().all(|t| is_binary(t) || t.is_null()) && types.iter().any(is_binary) {
        return Ok(vec![DataType::Binary; types.len()]);
    }
    if types
        .iter()
        .all(|t| matches!(t, DataType::List(_)) || t.is_null())
        && types.iter().any(|t| matches!(t, DataType::List(_)))
    {
        let elements: Vec<DataType> = types
            .iter()
            .filter_map(|t| super::array::element_type(t).cloned())
            .collect();
        let common = datafusion::logical_expr::type_coercion::binary::type_union_resolution(&elements)
            .ok_or_else(|| not_found(name, types))?;
        let list = DataType::List(Arc::new(Field::new_list_field(common, true)));
        return Ok(vec![list; types.len()]);
    }
    types
        .iter()
        .map(|t| as_str(t).ok_or_else(|| not_found(name, types)))
        .collect()
}

fn invoke_concat(call: &Call<'_>) -> Result<ArrayRef> {
    if let Some(DataType::List(_)) = call.args.first().map(|a| a.data_type().clone()) {
        return super::array::concat_lists(call);
    }
    if let Some(DataType::Binary) = call.args.first().map(|a| a.data_type().clone()) {
        let columns = call.args.iter().map(binaries).collect::<Result<Vec<_>>>()?;
        let out: Vec<Option<Vec<u8>>> = (0..call.rows)
            .map(|i| {
                let mut value = Vec::new();
                for c in &columns {
                    value.extend_from_slice(c.at(i)?);
                }
                Some(value)
            })
            .collect();
        return Ok(binary_array(&out));
    }
    let columns = call.args.iter().map(strings).collect::<Result<Vec<_>>>()?;
    Ok(string_array(
        (0..call.rows)
            .map(|i| {
                let mut value = String::new();
                for c in &columns {
                    value.push_str(c.at(i)?);
                }
                Some(value)
            })
            .collect(),
    ))
}

fn concat_return(r: &super::support::Ret<'_>) -> Result<DataType> {
    match r.types {
        [t @ DataType::List(_), ..] | [_, t @ DataType::List(_)] => Ok(t.clone()),
        _ => SAME_AS_FIRST(r),
    }
}

pub(crate) fn concat() -> ScalarUDF {
    Func::new("concat", coerce_concat, concat_return, invoke_concat).udf()
}

/// The `||` operator (the SQL text rewrite calls it for every `a || b`):
/// Trino's `concat` of two strings, two `varbinary` or two arrays, and an
/// array with an element on either side (appended or prepended). The
/// engine's `||` only joins strings (and casts other operands to text).
pub(crate) const CONCAT_OPERATOR: &str = "codelake_trino_concat_operator";

/// `(array(T), T)` or `(T, array(T))`: the element's side and common type.
fn element_side(types: &[DataType]) -> Option<(usize, DataType)> {
    let (list, element, side) = match types {
        [l @ DataType::List(_), e] if !matches!(e, DataType::List(_)) && !e.is_null() => (l, e, 1),
        [e, l @ DataType::List(_)] if !matches!(e, DataType::List(_)) && !e.is_null() => (l, e, 0),
        _ => return None,
    };
    let inner = super::array::element_type(list)?.clone();
    let common =
        datafusion::logical_expr::type_coercion::binary::type_union_resolution(&[inner, element.clone()])?;
    Some((side, common))
}

pub(crate) fn concat_operator() -> ScalarUDF {
    Func::new(
        CONCAT_OPERATOR,
        |name, types| {
            if types.len() != 2 {
                return Err(not_found(name, types));
            }
            if let Some((side, common)) = element_side(types) {
                let list = DataType::List(Arc::new(Field::new_list_field(common.clone(), true)));
                return Ok(if side == 1 {
                    vec![list, common]
                } else {
                    vec![common, list]
                });
            }
            coerce_concat(name, types)
        },
        concat_return,
        |call| {
            let types: Vec<DataType> = call.args.iter().map(|a| a.data_type().clone()).collect();
            let Some((side, _)) = element_side(&types) else {
                return invoke_concat(call);
            };
            let DataType::List(field) = call.return_type else {
                return Err(DataFusionError::Internal("concat of an array".into()));
            };
            // The element as a one-element array, NULL where the element is
            // NULL: Trino 431 answers NULL for `ARRAY[1] || CAST(NULL AS
            // integer)`, as for any NULL argument of `concat`.
            let element = Arc::clone(call.arg(side)?);
            let nulls = element.logical_nulls();
            let single: ArrayRef = Arc::new(ListArray::try_new(
                Arc::clone(field),
                datafusion::arrow::buffer::OffsetBuffer::from_lengths(std::iter::repeat_n(1, element.len())),
                element,
                nulls,
            )?);
            let mut args = call.args.clone();
            args[side] = single;
            let lists = Call {
                args,
                fields: call.fields.clone(),
                rows: call.rows,
                return_type: call.return_type,
                zone: Arc::clone(&call.zone),
            };
            super::array::concat_lists(&lists)
        },
    )
    .udf()
}

/// `concat_ws(separator, value…)` and `concat_ws(separator, array(varchar))`:
/// NULL values are skipped; a NULL separator gives NULL.
pub(crate) fn concat_ws() -> ScalarUDF {
    Func::new(
        "concat_ws",
        |name, types| {
            let Some((first, rest)) = types.split_first() else {
                return Err(not_found(name, types));
            };
            let separator = as_str(first).ok_or_else(|| not_found(name, types))?;
            if let [DataType::List(_) | DataType::LargeList(_)] = rest {
                return Ok(vec![separator, string_list()]);
            }
            if rest.is_empty() {
                return Err(invalid_argument(
                    "There must be two or more concatenation arguments",
                ));
            }
            let mut out = vec![separator];
            for t in rest {
                out.push(as_str(t).ok_or_else(|| not_found(name, types))?);
            }
            Ok(out)
        },
        UTF8,
        invoke_concat_ws,
    )
    .udf()
}

fn invoke_concat_ws(call: &Call<'_>) -> Result<ArrayRef> {
    let separators = strings(call.arg(0)?)?;
    if let Some(DataType::List(_)) = call.args.get(1).map(|a| a.data_type().clone()) {
        let lists = call
            .arg(1)?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("expected list".into()))?;
        let out = (0..call.rows)
            .map(|i| {
                let sep = separators.at(i)?;
                if lists.is_null(i) {
                    return None;
                }
                let values = lists.value(i);
                let values = strings(&values).ok()?;
                let parts: Vec<&str> = (0..values.len()).filter_map(|j| values.at(j)).collect();
                Some(parts.join(sep))
            })
            .collect();
        return Ok(string_array(out));
    }
    let columns = call
        .args
        .iter()
        .skip(1)
        .map(strings)
        .collect::<Result<Vec<_>>>()?;
    Ok(string_array(
        (0..call.rows)
            .map(|i| {
                let sep = separators.at(i)?;
                let parts: Vec<&str> = columns.iter().filter_map(|c| c.at(i)).collect();
                Some(parts.join(sep))
            })
            .collect(),
    ))
}

pub(crate) fn replace() -> ScalarUDF {
    Func::new(
        "replace",
        |name, types| coerce_any(name, types, &[&[as_str, as_str], &[as_str, as_str, as_str]]),
        UTF8,
        |call| {
            let a = strings(call.arg(0)?)?;
            let search = strings(call.arg(1)?)?;
            let replacement = match call.args.get(2) {
                Some(r) => Some(strings(r)?),
                None => None,
            };
            Ok(string_array(
                (0..call.rows)
                    .map(|i| {
                        let with = match replacement {
                            Some(r) => r.at(i)?,
                            None => "",
                        };
                        Some(a.at(i)?.replace(search.at(i)?, with))
                    })
                    .collect(),
            ))
        },
    )
    .udf()
}

/// 1-based code point position of the `instance`-th occurrence (negative:
/// from the end); 0 when absent.
fn string_position(text: &str, sub: &str, instance: i64) -> Result<i64> {
    if instance == 0 {
        return Err(invalid_argument(
            "'instance' must be a positive or negative number.",
        ));
    }
    if sub.is_empty() {
        return Ok(1);
    }
    let mut found = None;
    if instance > 0 {
        let mut from = 0;
        let mut remaining = instance;
        while let Some(pos) = text.get(from..).and_then(|t| t.find(sub)) {
            let at = from + pos;
            remaining -= 1;
            if remaining == 0 {
                found = Some(at);
                break;
            }
            from = at + text[at..].chars().next().map_or(1, char::len_utf8);
        }
    } else {
        let mut to = text.len();
        let mut remaining = -instance;
        while let Some(at) = text.get(..to).and_then(|t| t.rfind(sub)) {
            remaining -= 1;
            if remaining == 0 {
                found = Some(at);
                break;
            }
            if at == 0 {
                break;
            }
            to = at + sub.len() - 1;
            while !text.is_char_boundary(to) {
                to -= 1;
            }
        }
    }
    Ok(found.map_or(0, |at| {
        i64::try_from(text[..at].chars().count()).unwrap_or(i64::MAX) + 1
    }))
}

pub(crate) fn strpos() -> ScalarUDF {
    Func::new(
        "strpos",
        |name, types| coerce_any(name, types, &[&[as_str, as_str], &[as_str, as_str, as_i64]]),
        BIGINT,
        |call| {
            let a = strings(call.arg(0)?)?;
            let sub = strings(call.arg(1)?)?;
            let instance = match call.args.get(2) {
                Some(n) => Some(ints(n)?),
                None => None,
            };
            let out = (0..call.rows)
                .map(|i| {
                    let n = match instance {
                        Some(n) => n.at(i),
                        None => Some(1),
                    };
                    match (a.at(i), sub.at(i), n) {
                        (Some(t), Some(s), Some(n)) => string_position(t, s, n).map(Some),
                        _ => Ok(None),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .alias("position")
    .udf()
}

pub(crate) fn starts_with() -> ScalarUDF {
    Func::new(
        "starts_with",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        BOOLEAN,
        |call| {
            let a = strings(call.arg(0)?)?;
            let p = strings(call.arg(1)?)?;
            Ok(bool_array(
                (0..call.rows)
                    .map(|i| Some(a.at(i)?.starts_with(p.at(i)?)))
                    .collect(),
            ))
        },
    )
    .udf()
}

fn pad(text: &str, size: i64, padding: &str, left: bool) -> Result<String> {
    if !(0..=i64::from(i32::MAX)).contains(&size) {
        return Err(invalid_argument(format!(
            "Target length must be in the range [0..{}]",
            i32::MAX
        )));
    }
    if padding.is_empty() {
        return Err(invalid_argument("Padding string must not be empty"));
    }
    let size = usize::try_from(size).unwrap_or(0);
    let count = text.chars().count();
    if count >= size {
        return Ok(text.chars().take(size).collect());
    }
    let fill: String = padding.chars().cycle().take(size - count).collect();
    Ok(if left {
        fill + text
    } else {
        format!("{text}{fill}")
    })
}

fn pad_function(name: &'static str, left: bool) -> ScalarUDF {
    let invoke = if left {
        |call: &Call<'_>| pad_invoke(call, true)
    } else {
        |call: &Call<'_>| pad_invoke(call, false)
    };
    Func::new(
        name,
        |name, types| coerce_each(name, types, &[as_str, as_i64, as_str]),
        UTF8,
        invoke,
    )
    .udf()
}

fn pad_invoke(call: &Call<'_>, left: bool) -> Result<ArrayRef> {
    let a = strings(call.arg(0)?)?;
    let size = ints(call.arg(1)?)?;
    let padding = strings(call.arg(2)?)?;
    let out = (0..call.rows)
        .map(|i| match (a.at(i), size.at(i), padding.at(i)) {
            (Some(t), Some(n), Some(p)) => pad(t, n, p, left).map(Some),
            _ => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(string_array(out))
}

pub(crate) fn lpad() -> ScalarUDF {
    pad_function("lpad", true)
}

pub(crate) fn rpad() -> ScalarUDF {
    pad_function("rpad", false)
}

fn split_parts<'a>(text: &'a str, delimiter: &str, limit: Option<i64>) -> Result<Vec<&'a str>> {
    if delimiter.is_empty() {
        return Err(invalid_argument("The delimiter may not be the empty string"));
    }
    match limit {
        None => Ok(text.split(delimiter).collect()),
        Some(n) if n <= 0 => Err(invalid_argument("Limit must be positive")),
        Some(n) => Ok(text
            .splitn(usize::try_from(n).unwrap_or(usize::MAX), delimiter)
            .collect()),
    }
}

pub(crate) fn split() -> ScalarUDF {
    Func::new(
        "split",
        |name, types| coerce_any(name, types, &[&[as_str, as_str], &[as_str, as_str, as_i64]]),
        |_| Ok(string_list()),
        |call| {
            let a = strings(call.arg(0)?)?;
            let d = strings(call.arg(1)?)?;
            let limit = match call.args.get(2) {
                Some(l) => Some(ints(l)?),
                None => None,
            };
            let mut builder = ListBuilder::new(StringBuilder::new());
            for i in 0..call.rows {
                let l = match limit {
                    Some(l) => {
                        let Some(v) = l.at(i) else {
                            builder.append_null();
                            continue;
                        };
                        Some(v)
                    }
                    None => None,
                };
                if let (Some(t), Some(delimiter)) = (a.at(i), d.at(i)) {
                    for part in split_parts(t, delimiter, l)? {
                        builder.values().append_value(part);
                    }
                    builder.append(true);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        },
    )
    .udf()
}

fn split_part_value(text: &str, delimiter: &str, index: i64) -> Result<Option<String>> {
    if index <= 0 {
        return Err(invalid_argument("Index must be greater than zero"));
    }
    let n = usize::try_from(index - 1).unwrap_or(usize::MAX);
    if delimiter.is_empty() {
        return Ok(text.chars().nth(n).map(String::from));
    }
    Ok(text.split(delimiter).nth(n).map(str::to_owned))
}

pub(crate) fn split_part() -> ScalarUDF {
    Func::new(
        "split_part",
        |name, types| coerce_each(name, types, &[as_str, as_str, as_i64]),
        UTF8,
        |call| {
            let a = strings(call.arg(0)?)?;
            let d = strings(call.arg(1)?)?;
            let n = ints(call.arg(2)?)?;
            let out = (0..call.rows)
                .map(|i| match (a.at(i), d.at(i), n.at(i)) {
                    (Some(t), Some(d), Some(n)) => split_part_value(t, d, n),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

pub(crate) fn chr() -> ScalarUDF {
    Func::new(
        "chr",
        |name, types| coerce_each(name, types, &[as_i64]),
        UTF8,
        |call| {
            let n = ints(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    n.at(i)
                        .map(|v| {
                            u32::try_from(v)
                                .ok()
                                .and_then(char::from_u32)
                                .map(String::from)
                                .ok_or_else(|| {
                                    invalid_argument(format!("Not a valid Unicode code point: {v}"))
                                })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

pub(crate) fn codepoint() -> ScalarUDF {
    Func::new(
        "codepoint",
        coerce_str1,
        |_| Ok(DataType::Int32),
        |call| {
            let a = strings(call.arg(0)?)?;
            let out = (0..call.rows)
                .map(|i| {
                    a.at(i)
                        .map(|s| {
                            let mut chars = s.chars();
                            match (chars.next(), chars.next()) {
                                (Some(c), None) => Ok(i64::from(u32::from(c))),
                                _ => Err(invalid_argument("Input string must be a single character string")),
                            }
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

fn levenshtein(a: &str, b: &str) -> i64 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            current[j + 1] = (previous[j] + cost).min(previous[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    i64::try_from(previous[b.len()]).unwrap_or(i64::MAX)
}

pub(crate) fn levenshtein_distance() -> ScalarUDF {
    Func::new(
        "levenshtein_distance",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        BIGINT,
        |call| {
            let a = strings(call.arg(0)?)?;
            let b = strings(call.arg(1)?)?;
            Ok(int_array(
                (0..call.rows)
                    .map(|i| Some(levenshtein(a.at(i)?, b.at(i)?)))
                    .collect(),
            ))
        },
    )
    .udf()
}

pub(crate) fn hamming_distance() -> ScalarUDF {
    Func::new(
        "hamming_distance",
        |name, types| coerce_each(name, types, &[as_str, as_str]),
        BIGINT,
        |call| {
            let left = strings(call.arg(0)?)?;
            let right = strings(call.arg(1)?)?;
            let out = (0..call.rows)
                .map(|i| match (left.at(i), right.at(i)) {
                    (Some(x), Some(y)) => {
                        if x.chars().count() != y.chars().count() {
                            return Err(invalid_argument(
                                "The input strings to hamming_distance function must have the same length",
                            ));
                        }
                        let distance = x.chars().zip(y.chars()).filter(|(p, q)| p != q).count();
                        Ok(Some(i64::try_from(distance).unwrap_or(i64::MAX)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(int_array(out))
        },
    )
    .udf()
}

pub(crate) fn translate() -> ScalarUDF {
    Func::new(
        "translate",
        |name, types| coerce_each(name, types, &[as_str, as_str, as_str]),
        UTF8,
        |call| {
            let a = strings(call.arg(0)?)?;
            let from = strings(call.arg(1)?)?;
            let to = strings(call.arg(2)?)?;
            Ok(string_array(
                (0..call.rows)
                    .map(|i| {
                        let from: Vec<char> = from.at(i)?.chars().collect();
                        let to: Vec<char> = to.at(i)?.chars().collect();
                        Some(
                            a.at(i)?
                                .chars()
                                .filter_map(|c| match from.iter().position(|f| *f == c) {
                                    Some(p) => to.get(p).copied(),
                                    None => Some(c),
                                })
                                .collect(),
                        )
                    })
                    .collect(),
            ))
        },
    )
    .udf()
}

pub(crate) fn to_utf8() -> ScalarUDF {
    Func::new(
        "to_utf8",
        coerce_str1,
        |_| Ok(DataType::Binary),
        |call| {
            let a = strings(call.arg(0)?)?;
            let out: Vec<Option<Vec<u8>>> = (0..call.rows)
                .map(|i| a.at(i).map(|s| s.as_bytes().to_vec()))
                .collect();
            Ok(binary_array(&out))
        },
    )
    .udf()
}

pub(crate) fn from_utf8() -> ScalarUDF {
    Func::new(
        "from_utf8",
        |name, types| coerce_any(name, types, &[&[as_bin], &[as_bin, as_str], &[as_bin, as_i64]]),
        UTF8,
        |call| {
            let b = binaries(call.arg(0)?)?;
            let replacement: Option<Vec<Option<String>>> = match call.args.get(1) {
                None => None,
                Some(r) if is_string(r.data_type()) => {
                    let r = strings(r)?;
                    Some((0..call.rows).map(|i| r.at(i).map(str::to_owned)).collect())
                }
                Some(r) => {
                    let r = ints(r)?;
                    Some(
                        (0..call.rows)
                            .map(|i| {
                                r.at(i)
                                    .and_then(|v| u32::try_from(v).ok())
                                    .and_then(char::from_u32)
                                    .map(String::from)
                            })
                            .collect(),
                    )
                }
            };
            let out = (0..call.rows)
                .map(|i| {
                    let bytes = b.at(i)?;
                    let with = match &replacement {
                        Some(r) => r.get(i).cloned().flatten()?,
                        None => "\u{FFFD}".to_owned(),
                    };
                    Some(decode_utf8_with(bytes, &with))
                })
                .collect();
            Ok(string_array(out))
        },
    )
    .udf()
}

/// One code point of `bytes` (Airlift's `SliceUtf8.tryGetCodePointAt`, used
/// by Trino): `Ok((char, length))`, or `Err(skip)`, the bytes one
/// replacement covers. The lead byte fixes the sequence length (up to six
/// bytes); a missing continuation byte ends the bad sequence before it, and a
/// complete sequence that is not a scalar value (overlong form, surrogate,
/// beyond U+10FFFF) is replaced as a whole.
fn code_point_at(bytes: &[u8]) -> std::result::Result<(char, usize), usize> {
    let Some(&lead) = bytes.first() else {
        return Err(1);
    };
    let (length, initial) = match lead {
        0x00..=0x7F => return Ok((char::from(lead), 1)),
        0xC0..=0xDF => (2, u32::from(lead & 0x1F)),
        0xE0..=0xEF => (3, u32::from(lead & 0x0F)),
        0xF0..=0xF7 => (4, u32::from(lead & 0x07)),
        0xF8..=0xFB => (5, 0),
        0xFC..=0xFD => (6, 0),
        _ => return Err(1),
    };
    let mut code = initial;
    for i in 1..length {
        match bytes.get(i) {
            Some(b) if b & 0xC0 == 0x80 => code = (code << 6) | u32::from(b & 0x3F),
            _ => return Err(i),
        }
    }
    let minimum = match length {
        2 => 0x80,
        3 => 0x800,
        _ => 0x1_0000,
    };
    match char::from_u32(code) {
        Some(c) if length <= 4 && code >= minimum => Ok((c, length)),
        _ => Err(length),
    }
}

fn decode_utf8_with(bytes: &[u8], replacement: &str) -> String {
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    while !rest.is_empty() {
        // Valid runs in one step; the byte-wise decoder only at bad bytes.
        let valid = match std::str::from_utf8(rest) {
            Ok(text) => {
                out.push_str(text);
                break;
            }
            Err(error) => error.valid_up_to(),
        };
        out.push_str(std::str::from_utf8(&rest[..valid]).unwrap_or_default());
        rest = &rest[valid..];
        match code_point_at(rest) {
            Ok((c, length)) => {
                out.push(c);
                rest = &rest[length..];
            }
            Err(skip) => {
                out.push_str(replacement);
                rest = &rest[skip.min(rest.len())..];
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// soundex, luhn_check, split_to_map
// ---------------------------------------------------------------------------

/// American Soundex codes of `A`..`Z` (Apache Commons Codec `US_ENGLISH_MAPPING`).
const SOUNDEX_CODES: &[u8; 26] = b"01230120022455012623010202";

fn soundex_code(c: char) -> Result<u8> {
    let index = i64::from(u32::from(c)) - i64::from(u32::from('A'));
    usize::try_from(index)
        .ok()
        .and_then(|i| SOUNDEX_CODES.get(i).copied())
        .ok_or_else(|| invalid_argument(format!("The character is not mapped: {c} (index={index})")))
}

/// Trino's `soundex`: Commons Codec's algorithm (letters only, `H`/`W` do not
/// separate equal codes, letters outside `A`..`Z` are an error).
fn soundex_text(text: &str) -> Result<String> {
    let letters: String = text
        .chars()
        .filter(|c| c.is_alphabetic())
        .collect::<String>()
        .to_uppercase();
    let mut chars = letters.chars();
    let Some(first) = chars.next() else {
        return Ok(String::new());
    };
    let mut out = String::from(first);
    let mut last = soundex_code(first)?;
    for c in chars {
        if out.len() >= 4 {
            break;
        }
        if matches!(c, 'H' | 'W') {
            continue;
        }
        let code = soundex_code(c)?;
        if code != b'0' && code != last {
            out.push(char::from(code));
        }
        last = code;
    }
    while out.len() < 4 {
        out.push('0');
    }
    Ok(out)
}

pub(crate) fn soundex() -> ScalarUDF {
    Func::new("soundex", coerce_str1, UTF8, |call| {
        let a = strings(call.arg(0)?)?;
        let out = (0..call.rows)
            .map(|i| a.at(i).map(soundex_text).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(string_array(out))
    })
    .udf()
}

/// Luhn checksum of a digit string: empty is false, a non-digit is an error.
fn luhn(text: &str) -> Result<bool> {
    if text.is_empty() {
        return Ok(false);
    }
    let mut sum = 0_u32;
    for (i, c) in text.chars().rev().enumerate() {
        let digit = c
            .to_digit(10)
            .filter(|_| c.is_ascii_digit())
            .ok_or_else(|| invalid_argument("Input contains non-digit character"))?;
        let weighted = if i % 2 == 1 { digit * 2 } else { digit };
        sum += if weighted > 9 { weighted - 9 } else { weighted };
    }
    Ok(sum.is_multiple_of(10))
}

pub(crate) fn luhn_check() -> ScalarUDF {
    Func::new("luhn_check", coerce_str1, BOOLEAN, |call| {
        let a = strings(call.arg(0)?)?;
        let out = (0..call.rows)
            .map(|i| a.at(i).map(luhn).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(bool_array(out))
    })
    .udf()
}

/// Trino's `split_to_map`: every entry has the key-value delimiter exactly once
/// and keys are unique.
fn split_entries(text: &str, entry_delimiter: &str, key_delimiter: &str) -> Result<Vec<(String, String)>> {
    if entry_delimiter.is_empty() {
        return Err(invalid_argument("entryDelimiter is empty"));
    }
    if key_delimiter.is_empty() {
        return Err(invalid_argument("keyValueDelimiter is empty"));
    }
    if entry_delimiter == key_delimiter {
        return Err(invalid_argument(
            "entryDelimiter and keyValueDelimiter must not be the same",
        ));
    }
    let mut entries: Vec<(String, String)> = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let (entry, next) = match rest.find(entry_delimiter) {
            Some(end) => (&rest[..end], Some(&rest[end + entry_delimiter.len()..])),
            None => (rest, None),
        };
        let bad = || {
            invalid_argument(format!(
                "Key-value delimiter must appear exactly once in each entry. Bad input: '{entry}'"
            ))
        };
        let (key, value) = entry.split_once(key_delimiter).ok_or_else(bad)?;
        if value.contains(key_delimiter) {
            return Err(bad());
        }
        if entries.iter().any(|(k, _)| k == key) {
            return Err(invalid_argument(format!(
                "Duplicate keys ({key}) are not allowed"
            )));
        }
        entries.push((key.to_owned(), value.to_owned()));
        match next {
            Some(next) => rest = next,
            None => break,
        }
    }
    Ok(entries)
}

pub(crate) fn split_to_map() -> ScalarUDF {
    Func::new(
        "split_to_map",
        |name, types| coerce_each(name, types, &[as_str, as_str, as_str]),
        |_| Ok(super::collections::map_type(DataType::Utf8, DataType::Utf8)),
        |call| {
            let (a, e, k) = (
                strings(call.arg(0)?)?,
                strings(call.arg(1)?)?,
                strings(call.arg(2)?)?,
            );
            let rows = (0..call.rows)
                .map(|i| match (a.at(i), e.at(i), k.at(i)) {
                    (Some(text), Some(entry), Some(key)) => split_entries(text, entry, key).map(|entries| {
                        Some(
                            entries
                                .into_iter()
                                .map(|(k, v)| (ScalarValue::from(k), ScalarValue::from(v)))
                                .collect(),
                        )
                    }),
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?;
            super::collections::build_maps(rows, &DataType::Utf8, &DataType::Utf8)
        },
    )
    .udf()
}

/// `normalize(string [, form])`: Unicode normalization, NFC by default.
fn normalize_text(text: &str, form: &str) -> Result<String> {
    use unicode_normalization::UnicodeNormalization;
    Ok(match form.to_ascii_uppercase().as_str() {
        "NFC" => text.nfc().collect(),
        "NFD" => text.nfd().collect(),
        "NFKC" => text.nfkc().collect(),
        "NFKD" => text.nfkd().collect(),
        _ => {
            return Err(invalid_argument(format!(
                "Normalization form must be one of [NFD, NFC, NFKD, NFKC]: {form}"
            )));
        }
    })
}

pub(crate) fn normalize() -> ScalarUDF {
    Func::new(
        "normalize",
        |name, types| coerce_any(name, types, &[&[as_str], &[as_str, as_str]]),
        UTF8,
        |call| {
            let a = strings(call.arg(0)?)?;
            let forms = match call.args.get(1) {
                Some(f) => Some(strings(f)?),
                None => None,
            };
            let out = (0..call.rows)
                .map(|i| {
                    let form = match forms {
                        Some(f) => f.at(i),
                        None => Some("NFC"),
                    };
                    match (a.at(i), form) {
                        (Some(text), Some(form)) => normalize_text(text, form).map(Some),
                        _ => Ok(None),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(string_array(out))
        },
    )
    .udf()
}

pub(crate) fn udfs() -> Vec<ScalarUDF> {
    let mut all = vec![
        lower(),
        upper(),
        reverse(),
        length(),
        substr(),
        concat(),
        concat_ws(),
        replace(),
        strpos(),
        starts_with(),
        lpad(),
        rpad(),
        split(),
        split_part(),
        chr(),
        codepoint(),
        levenshtein_distance(),
        hamming_distance(),
        translate(),
        to_utf8(),
        from_utf8(),
        concat_operator(),
        soundex(),
        luhn_check(),
        split_to_map(),
        normalize(),
    ];
    all.extend(trims());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soundex_luhn_and_maps() {
        assert_eq!(soundex_text("Robert").unwrap(), "R163");
        assert_eq!(soundex_text("Ashcraft").unwrap(), "A261");
        assert_eq!(soundex_text("Tymczak").unwrap(), "T522");
        assert_eq!(soundex_text("123").unwrap(), "");
        assert!(soundex_text("ação").is_err());
        assert!(luhn("79927398713").unwrap());
        assert!(!luhn("79927398714").unwrap());
        assert!(!luhn("").unwrap());
        assert!(luhn("0").unwrap());
        assert!(luhn("12a").is_err());
        assert_eq!(split_entries("a=1,b=", ",", "=").unwrap().len(), 2);
        assert!(split_entries("k=v=w", ",", "=").is_err());
        assert!(split_entries("a=1,a=2", ",", "=").is_err());
        assert!(split_entries("", ",", "=").unwrap().is_empty());
    }

    #[test]
    fn substr_matches_trino() {
        assert_eq!(substr_text("abcde", -2, i64::MAX), "de");
        assert_eq!(substr_text("abcde", 0, i64::MAX), "");
        assert_eq!(substr_text("abcde", 2, 2), "bc");
        assert_eq!(substr_text("abcde", 6, i64::MAX), "");
        assert_eq!(substr_text("abcde", 2, -1), "");
        assert_eq!(substr_text("abcde", -9, 2), "");
        assert_eq!(substr_text("ação", 2, 2), "çã");
        assert_eq!(substr_bytes(&[1, 2, 3], 3, 5), &[3]);
        assert_eq!(substr_bytes(&[1, 2, 3], -1, 1), &[3]);
        assert_eq!(substr_bytes(&[1, 2, 3], 4, 1), &[] as &[u8]);
    }

    #[test]
    fn positions_count_code_points() {
        assert_eq!(string_position("ação ação", "ç", 1).unwrap(), 2);
        assert_eq!(string_position("ação ação", "ç", 2).unwrap(), 7);
        assert_eq!(string_position("ação ação", "ç", -1).unwrap(), 7);
        assert_eq!(string_position("aaa", "aa", 2).unwrap(), 2, "occurrences overlap");
        assert_eq!(string_position("aaa", "aa", -1).unwrap(), 2);
        assert_eq!(string_position("aaa", "aa", -2).unwrap(), 1);
        assert_eq!(string_position("abc", "x", 1).unwrap(), 0);
        assert_eq!(string_position("abc", "", 1).unwrap(), 1);
        assert!(string_position("abc", "a", 0).is_err());
    }

    #[test]
    fn padding_and_splitting() {
        assert_eq!(pad("ab", 5, "xy", true).unwrap(), "xyxab");
        assert_eq!(pad("ab", 5, "xy", false).unwrap(), "abxyx");
        assert_eq!(pad("abcdef", 3, "x", true).unwrap(), "abc");
        assert!(pad("a", -1, "x", true).is_err());
        assert!(pad("a", 3, "", true).is_err());
        assert_eq!(split_parts("a,b,", ",", None).unwrap(), vec!["a", "b", ""]);
        assert_eq!(split_parts("a,b,c", ",", Some(2)).unwrap(), vec!["a", "b,c"]);
        assert!(split_parts("a", "", None).is_err());
        assert_eq!(split_part_value("a,b", ",", 3).unwrap(), None);
        assert_eq!(split_part_value("abc", "", 2).unwrap().as_deref(), Some("b"));
    }

    #[test]
    fn case_mapping_is_simple() {
        assert_eq!(simple_lower('\u{130}'), 'i');
        assert_eq!(simple_upper('ß'), 'ß');
        assert_eq!(simple_lower('Σ'), 'σ');
        assert!(is_java_whitespace('\t'));
        assert!(!is_java_whitespace('\u{A0}'));
        assert!(is_java_whitespace('\u{1F}'));
    }

    #[test]
    fn distances() {
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("", "abc"), 3);
        assert_eq!(decode_utf8_with(&[0x61, 0xff, 0x62], "?"), "a?b");
    }
}
