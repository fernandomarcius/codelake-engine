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

use std::cmp::Ordering;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Name of [`NestedTest`].
pub(crate) const NESTED_TEST: &str = "codelake_nested_test";

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
        let Some(common) = comparison_coercion(a, b) else {
            return plan_err!("{NESTED_TEST}: cannot compare {a} with {b}");
        };
        Ok(vec![common.clone(), common, mode.clone()])
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

pub(crate) static NESTED_TEST_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(NestedTest {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
});

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray, StructArray};
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
    fn modes_list_the_char_values() {
        assert_eq!(test_mode(Test::Equal, &[true, false]), "ecv");
    }
}
