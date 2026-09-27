//! Rows and arrays compared, and padded, as the reference engine (Trino
//! 431) does it, for values the planner does not see written out: a column
//! of a subquery, a CTE or a table, an `array_agg`, a `CASE` of rows.
//!
//! `DataFusion` compares two rows or two arrays as one value: `ROW(1, NULL)
//! = ROW(1, NULL)` is true, and a join on such keys matches `NULL` with
//! `NULL`. The reference compares them value by value: `=` is false when a
//! pair differs, `NULL` when none differs but a pair is `NULL`, true
//! otherwise; arrays of different lengths differ; `IS DISTINCT FROM` takes
//! two `NULL`s as equal. A pair of `char(n)` values (or of a char value
//! and text) is compared without trailing spaces, as their common char
//! type compares them. [`NestedTest`] (`codelake_nested_test(a, b, mode)`)
//! answers that; `crate::char_semantics` plans it, and writes in `mode`
//! which values are char ones.
//!
//! [`NestedPad`] (`codelake_nested_pad(x, spec)`) pads the char values
//! inside a row or an array to a common `char(n)` (an array of `char(3)`
//! values output with one of `char(5)` ones holds `char(5)` values).

use std::cmp::Ordering;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, FixedSizeListArray, LargeListArray, ListArray, StringArray,
    StructArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Name of [`NestedTest`].
pub(crate) const NESTED_TEST: &str = "codelake_nested_test";
/// Name of [`NestedPad`].
pub(crate) const NESTED_PAD: &str = "codelake_nested_pad";

/// What [`NestedTest`] answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Test {
    /// `a = b`.
    Equal,
    /// `a IS DISTINCT FROM b`.
    Distinct,
    /// Whether `a` holds a `NULL` at any depth (itself included).
    HoldsNull,
}

impl Test {
    fn letter(self) -> char {
        match self {
            Self::Equal => 'e',
            Self::Distinct => 'd',
            Self::HoldsNull => 'n',
        }
    }

    fn from_letter(letter: char) -> Option<Self> {
        match letter {
            'e' => Some(Self::Equal),
            'd' => Some(Self::Distinct),
            'n' => Some(Self::HoldsNull),
            _ => None,
        }
    }
}

/// How the values of a nested type are compared: its structure, and for
/// each value whether it is compared as char.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Leaf(bool),
    Row(Vec<Mode>),
    List(Box<Mode>),
}

/// The element field of a list type.
pub(crate) fn element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

/// Whether values of `data_type` are rows or arrays.
pub(crate) fn is_nested(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(_)) || element(data_type).is_some()
}

fn mode_of(data_type: &DataType, flags: &mut impl Iterator<Item = bool>) -> Mode {
    match data_type {
        DataType::Struct(fields) => Mode::Row(fields.iter().map(|f| mode_of(f.data_type(), flags)).collect()),
        other => match element(other) {
            Some(field) => Mode::List(Box::new(mode_of(field.data_type(), flags))),
            None => Mode::Leaf(flags.next().unwrap_or(false)),
        },
    }
}

/// The values of `data_type` in the order [`NestedTest`]'s mode lists
/// them: a row's fields in order, an array's element type once, every
/// other type one value.
pub(crate) fn leaves(data_type: &DataType, out: &mut Vec<DataType>) {
    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                leaves(field.data_type(), out);
            }
        }
        other => match element(other) {
            Some(field) => leaves(field.data_type(), out),
            None => out.push(other.clone()),
        },
    }
}

/// The mode argument of [`NestedTest`]: what it answers, then `c` for each
/// value compared as char and `v` for any other (see [`leaves`]).
pub(crate) fn test_mode(test: Test, chars: &[bool]) -> String {
    std::iter::once(test.letter())
        .chain(chars.iter().map(|c| if *c { 'c' } else { 'v' }))
        .collect()
}

/// The text of a text value.
fn text(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(t)) | ScalarValue::Utf8View(Some(t)) | ScalarValue::LargeUtf8(Some(t)) => {
            Some(t)
        }
        _ => None,
    }
}

/// Two values that are not `NULL`: equal? (IEEE for floats; char values
/// without trailing spaces.)
fn same_value(a: &ScalarValue, b: &ScalarValue, char_values: bool) -> bool {
    if let (Some(x), Some(y)) = (text(a), text(b)) {
        return if char_values {
            x.trim_end_matches(' ') == y.trim_end_matches(' ')
        } else {
            x == y
        };
    }
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => {
            x.partial_cmp(y) == Some(Ordering::Equal)
        }
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => {
            x.partial_cmp(y) == Some(Ordering::Equal)
        }
        _ => a == b,
    }
}

/// Whether two floats are one value to `IS DISTINCT FROM` (every NaN is
/// one, `-0.0` is `0.0`).
fn same_group(a: &ScalarValue, b: &ScalarValue, char_values: bool) -> bool {
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => {
            x.partial_cmp(y) == Some(Ordering::Equal) || (x.is_nan() && y.is_nan())
        }
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => {
            x.partial_cmp(y) == Some(Ordering::Equal) || (x.is_nan() && y.is_nan())
        }
        _ => same_value(a, b, char_values),
    }
}

/// The elements of array `row` of a list array.
fn elements(array: &dyn Array, row: usize) -> Option<ArrayRef> {
    match array.data_type() {
        DataType::List(_) => Some(array.as_list::<i32>().value(row)),
        DataType::LargeList(_) => Some(array.as_list::<i64>().value(row)),
        DataType::FixedSizeList(..) => Some(array.as_fixed_size_list().value(row)),
        _ => None,
    }
}

fn scalar(array: &dyn Array, row: usize) -> Result<ScalarValue> {
    ScalarValue::try_from_array(array, row)
}

/// One value of each of two arrays, compared.
#[derive(Clone, Copy)]
struct Pair<'a> {
    left: &'a dyn Array,
    left_row: usize,
    right: &'a dyn Array,
    right_row: usize,
}

impl<'a> Pair<'a> {
    fn same_rows(left: &'a dyn Array, right: &'a dyn Array, row: usize) -> Self {
        Self {
            left,
            left_row: row,
            right,
            right_row: row,
        }
    }

    fn nulls(self) -> (bool, bool) {
        (
            self.left.is_null(self.left_row),
            self.right.is_null(self.right_row),
        )
    }

    fn values(self) -> Result<(ScalarValue, ScalarValue)> {
        Ok((
            scalar(self.left, self.left_row)?,
            scalar(self.right, self.right_row)?,
        ))
    }

    /// The pairs of the fields of two rows.
    fn fields(self) -> Vec<(ArrayRef, ArrayRef)> {
        self.left
            .as_struct()
            .columns()
            .iter()
            .cloned()
            .zip(self.right.as_struct().columns().iter().cloned())
            .collect()
    }

    /// The elements of two arrays.
    fn elements(self) -> Result<(ArrayRef, ArrayRef)> {
        let (Some(left), Some(right)) = (
            elements(self.left, self.left_row),
            elements(self.right, self.right_row),
        ) else {
            return exec_err!("{NESTED_TEST}: not an array: {}", self.left.data_type());
        };
        Ok((left, right))
    }
}

/// The pair of a field of two rows.
fn field_pair<'a>(pair: Pair<'_>, left: &'a ArrayRef, right: &'a ArrayRef) -> Pair<'a> {
    Pair {
        left: left.as_ref(),
        left_row: pair.left_row,
        right: right.as_ref(),
        right_row: pair.right_row,
    }
}

/// `pair` compared with `=` as the reference answers it: `None` for
/// `NULL`.
fn equal(pair: Pair<'_>, mode: &Mode) -> Result<Option<bool>> {
    if pair.nulls() != (false, false) {
        return Ok(None);
    }
    let mut unknown = false;
    match mode {
        Mode::Leaf(char_values) => {
            let (left, right) = pair.values()?;
            return Ok(Some(same_value(&left, &right, *char_values)));
        }
        Mode::Row(modes) => {
            for ((left, right), field_mode) in pair.fields().iter().zip(modes) {
                match equal(field_pair(pair, left, right), field_mode)? {
                    Some(false) => return Ok(Some(false)),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
        }
        Mode::List(element_mode) => {
            let (left, right) = pair.elements()?;
            if left.len() != right.len() {
                return Ok(Some(false));
            }
            for k in 0..left.len() {
                match equal(Pair::same_rows(left.as_ref(), right.as_ref(), k), element_mode)? {
                    Some(false) => return Ok(Some(false)),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
        }
    }
    Ok((!unknown).then_some(true))
}

/// `pair` compared with `IS DISTINCT FROM`.
fn distinct(pair: Pair<'_>, mode: &Mode) -> Result<bool> {
    match pair.nulls() {
        (true, true) => return Ok(false),
        (true, false) | (false, true) => return Ok(true),
        (false, false) => {}
    }
    match mode {
        Mode::Leaf(char_values) => {
            let (left, right) = pair.values()?;
            Ok(!same_group(&left, &right, *char_values))
        }
        Mode::Row(modes) => {
            for ((left, right), field_mode) in pair.fields().iter().zip(modes) {
                if distinct(field_pair(pair, left, right), field_mode)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Mode::List(element_mode) => {
            let (left, right) = pair.elements()?;
            if left.len() != right.len() {
                return Ok(true);
            }
            for k in 0..left.len() {
                if distinct(Pair::same_rows(left.as_ref(), right.as_ref(), k), element_mode)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

/// Whether row `row` of `array` holds a `NULL` at any depth.
fn holds_null(array: &dyn Array, row: usize, mode: &Mode) -> bool {
    if array.is_null(row) {
        return true;
    }
    match mode {
        Mode::Leaf(_) => false,
        Mode::Row(modes) => array
            .as_struct()
            .columns()
            .iter()
            .zip(modes)
            .any(|(field, field_mode)| holds_null(field.as_ref(), row, field_mode)),
        Mode::List(element_mode) => elements(array, row)
            .is_some_and(|values| (0..values.len()).any(|k| holds_null(values.as_ref(), k, element_mode))),
    }
}

fn mode_argument(value: &ColumnarValue) -> Result<String> {
    match value {
        ColumnarValue::Scalar(s) => {
            text(s).map_or_else(|| exec_err!("{NESTED_TEST} expects a mode"), |t| Ok(t.to_owned()))
        }
        ColumnarValue::Array(_) => exec_err!("{NESTED_TEST} expects a constant mode"),
    }
}

/// The types two nested values are compared in: each value in the common
/// type of its pair, the rows keeping their own field names (rows compare
/// by position, and `DataFusion` casts a row to another only field by
/// name).
fn coerced_pair(a: &DataType, b: &DataType) -> Option<(DataType, DataType)> {
    let with = |field: &FieldRef, data_type: DataType| -> FieldRef {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(data_type)
                .with_nullable(true),
        )
    };
    match (a, b) {
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let mut fields_a = Vec::with_capacity(left.len());
            let mut fields_b = Vec::with_capacity(right.len());
            for (x, y) in left.iter().zip(right.iter()) {
                let (tx, ty) = coerced_pair(x.data_type(), y.data_type())?;
                fields_a.push(with(x, tx));
                fields_b.push(with(y, ty));
            }
            Some((
                DataType::Struct(fields_a.into()),
                DataType::Struct(fields_b.into()),
            ))
        }
        (DataType::List(x), DataType::List(y)) => {
            let (tx, ty) = coerced_pair(x.data_type(), y.data_type())?;
            Some((DataType::List(with(x, tx)), DataType::List(with(y, ty))))
        }
        (DataType::LargeList(x), DataType::LargeList(y)) => {
            let (tx, ty) = coerced_pair(x.data_type(), y.data_type())?;
            Some((DataType::LargeList(with(x, tx)), DataType::LargeList(with(y, ty))))
        }
        _ => comparison_coercion(a, b).map(|t| (t.clone(), t)),
    }
}

/// `codelake_nested_test(a, b, mode)`: see the module documentation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NestedTest {
    signature: Signature,
}

impl ScalarUDFImpl for NestedTest {
    fn name(&self) -> &'static str {
        NESTED_TEST
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [a, b, mode] = arg_types else {
            return plan_err!("{NESTED_TEST} expects three arguments");
        };
        let Some((a, b)) = coerced_pair(a, b) else {
            return plan_err!("{NESTED_TEST}: cannot compare {a} with {b}");
        };
        Ok(vec![a, b, mode.clone()])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [a, b, mode] = args.args.as_slice() else {
            return exec_err!("{NESTED_TEST} expects three arguments");
        };
        let mode = mode_argument(mode)?;
        let mut letters = mode.chars();
        let Some(test) = letters.next().and_then(Test::from_letter) else {
            return exec_err!("{NESTED_TEST}: invalid mode {mode}");
        };
        let rows = args.number_rows;
        let (a, b) = (a.to_array(rows)?, b.to_array(rows)?);
        let mut flags = letters.map(|c| c == 'c');
        let mode = mode_of(a.data_type(), &mut flags);
        let out: BooleanArray = (0..rows)
            .map(|row| {
                let pair = Pair::same_rows(a.as_ref(), b.as_ref(), row);
                Ok(match test {
                    Test::Equal => equal(pair, &mode)?,
                    Test::Distinct => Some(distinct(pair, &mode)?),
                    Test::HoldsNull => Some(holds_null(a.as_ref(), row, &mode)),
                })
            })
            .collect::<Result<_>>()?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// The lengths [`NestedPad`] pads the values of a nested type to, in the
/// order of [`leaves`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Pad {
    Leaf(Option<u32>),
    Row(Vec<Pad>),
    List(Box<Pad>),
}

fn pad_of(data_type: &DataType, lengths: &mut impl Iterator<Item = Option<u32>>) -> Pad {
    match data_type {
        DataType::Struct(fields) => Pad::Row(fields.iter().map(|f| pad_of(f.data_type(), lengths)).collect()),
        other => match element(other) {
            Some(field) => Pad::List(Box::new(pad_of(field.data_type(), lengths))),
            None => Pad::Leaf(lengths.next().flatten()),
        },
    }
}

/// The spec argument of [`NestedPad`]: for each value (see [`leaves`]) the
/// char length it is padded to, `-` for one left as it is.
pub(crate) fn pad_spec(lengths: &[Option<u32>]) -> String {
    lengths
        .iter()
        .map(|l| l.map_or_else(|| "-".to_owned(), |n| n.to_string()))
        .collect::<Vec<_>>()
        .join(",")
}

/// The lengths a spec names.
pub(crate) fn spec_lengths(spec: &str) -> Vec<Option<u32>> {
    spec.split(',').map(|t| t.parse::<u32>().ok()).collect()
}

fn padded_text(array: &ArrayRef, length: u32) -> Result<ArrayRef> {
    let target = array.data_type().clone();
    if !matches!(target, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) {
        return Ok(Arc::clone(array));
    }
    let text = cast(array, &DataType::Utf8)?;
    let out: StringArray = text
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(|t| crate::char_semantics::padded(t, length)))
        .collect();
    Ok(cast(&(Arc::new(out) as ArrayRef), &target)?)
}

fn pad_nested(array: &ArrayRef, pad: &Pad) -> Result<ArrayRef> {
    Ok(match pad {
        Pad::Leaf(None) => Arc::clone(array),
        Pad::Leaf(Some(n)) => padded_text(array, *n)?,
        Pad::Row(pads) => {
            let row = array.as_struct();
            let columns = row
                .columns()
                .iter()
                .zip(pads)
                .map(|(c, p)| pad_nested(c, p))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(StructArray::try_new(
                row.fields().clone(),
                columns,
                row.nulls().cloned(),
            )?)
        }
        Pad::List(p) => match array.data_type() {
            DataType::List(field) => {
                let list = array.as_list::<i32>();
                Arc::new(ListArray::try_new(
                    Arc::clone(field),
                    list.offsets().clone(),
                    pad_nested(list.values(), p)?,
                    list.nulls().cloned(),
                )?)
            }
            DataType::LargeList(field) => {
                let list = array.as_list::<i64>();
                Arc::new(LargeListArray::try_new(
                    Arc::clone(field),
                    list.offsets().clone(),
                    pad_nested(list.values(), p)?,
                    list.nulls().cloned(),
                )?)
            }
            DataType::FixedSizeList(field, size) => {
                let list = array.as_fixed_size_list();
                Arc::new(FixedSizeListArray::try_new(
                    Arc::clone(field),
                    *size,
                    pad_nested(list.values(), p)?,
                    list.nulls().cloned(),
                )?)
            }
            other => return exec_err!("{NESTED_PAD}: not an array: {other}"),
        },
    })
}

/// `codelake_nested_pad(x, spec)`: see the module documentation.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NestedPad {
    signature: Signature,
}

impl ScalarUDFImpl for NestedPad {
    fn name(&self) -> &'static str {
        NESTED_PAD
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().map_or_else(
            || plan_err!("{NESTED_PAD} expects two arguments"),
            |t| Ok(t.clone()),
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        args.arg_fields.first().map_or_else(
            || plan_err!("{NESTED_PAD} expects two arguments"),
            |field| Ok(Arc::new(field.as_ref().clone().with_name(NESTED_PAD))),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, spec] = args.args.as_slice() else {
            return exec_err!("{NESTED_PAD} expects two arguments");
        };
        let spec = mode_argument(spec)?;
        let array = value.to_array(args.number_rows)?;
        let pad = pad_of(array.data_type(), &mut spec_lengths(&spec).into_iter());
        let out = pad_nested(&array, &pad)?;
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(out),
        })
    }
}

pub(crate) static NESTED_TEST_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(NestedTest {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
});

pub(crate) static NESTED_PAD_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(NestedPad {
        signature: Signature::any(2, Volatility::Immutable),
    }))
});

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::Field;

    fn rows(a: Vec<Option<i64>>, b: Vec<Option<&str>>) -> ArrayRef {
        let fields = vec![
            Arc::new(Field::new("c0", DataType::Int64, true)),
            Arc::new(Field::new("c1", DataType::Utf8, true)),
        ];
        Arc::new(
            StructArray::try_new(
                fields.into(),
                vec![Arc::new(Int64Array::from(a)), Arc::new(StringArray::from(b))],
                None,
            )
            .unwrap(),
        )
    }

    #[test]
    fn rows_compare_value_by_value() {
        let a = rows(
            vec![Some(1), Some(1), Some(1), None],
            vec![Some("ab   "), None, Some("x"), None],
        );
        let b = rows(
            vec![Some(1), Some(1), Some(2), None],
            vec![Some("ab"), None, None, None],
        );
        let chars = mode_of(a.data_type(), &mut [false, true].into_iter());
        let plain = mode_of(a.data_type(), &mut [false, false].into_iter());
        let pair = |row: usize| Pair::same_rows(a.as_ref(), b.as_ref(), row);
        let eq = |m: &Mode| -> Vec<Option<bool>> { (0..4).map(|row| equal(pair(row), m).unwrap()).collect() };
        assert_eq!(eq(&chars), [Some(true), None, Some(false), None]);
        assert_eq!(eq(&plain), [Some(false), None, Some(false), None]);
        let dist: Vec<bool> = (0..4).map(|row| distinct(pair(row), &chars).unwrap()).collect();
        assert_eq!(dist, [false, false, true, false]);
        let nulls: Vec<bool> = (0..4).map(|i| holds_null(&a, i, &chars)).collect();
        assert_eq!(nulls, [false, true, false, true]);
    }

    #[test]
    fn specs_round_trip() {
        assert_eq!(pad_spec(&[Some(5), None]), "5,-");
        assert_eq!(spec_lengths("5,-"), [Some(5), None]);
        assert_eq!(test_mode(Test::Equal, &[true, false]), "ecv");
    }
}
