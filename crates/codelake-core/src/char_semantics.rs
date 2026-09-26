//! `char(n)` semantics of the reference engine (Trino 431).
//!
//! A `char(n)` column is text to the engine (Arrow has no char type), with
//! `char(n)` in its field metadata ([`crate::trino_types::TRINO_TYPE`]).
//! Every char value is held padded with spaces to `n` characters, wherever
//! it comes from: a catalog's scan (padded when read), `CAST` and `TRY_CAST`
//! to `char(n)`, an `INSERT` or `UPDATE` of a char column, and the rewrites
//! below. Two values of one `char(n)` therefore compare, group, join and
//! sort as the text they are.
//!
//! Where char values meet other text, the reference engine converts both to
//! a common type first, and so does [`CharSemantics`] (analyzer, before type
//! coercion), observed on Trino 431:
//!
//! - the common type of `char(n)` and `char(m)` is `char(max(n, m))`; of
//!   `char(n)` and `varchar(k)` (a text literal is `varchar(its length)`),
//!   `char(max(n, k))`; of `char(n)` and unbounded `varchar`, `char(65536)`.
//!   `NULL` takes the other type;
//! - a comparison (`=`, `<`, `IN`, `BETWEEN`, `= ANY`, `< ALL`,
//!   `IS DISTINCT FROM`, a simple `CASE`, `nullif`, a join condition, rows
//!   and arrays written out, `contains`) compares the two values padded to
//!   the common length: trailing spaces never matter to equality, and
//!   `'ab' || chr(9)` sorts below `'ab'` because the tab is compared with a
//!   space. Equality is planned on the values without trailing spaces
//!   (`codelake_char_key`), ordering with `codelake_char_compare`, and a
//!   text literal short enough is padded to the char's length instead;
//!   `ANY`/`ALL` become the `EXISTS` tests they mean;
//! - a value that is output in the common type is padded to its length: the
//!   branches of `CASE`, `coalesce`, `nvl`, `ifnull`, `nvl2`, `greatest`,
//!   `least`, the default of `lag`/`lead`, the elements of an array, the
//!   columns of `UNION`, `INTERSECT`, `EXCEPT` and `VALUES`, and the columns
//!   of a `USING` join (`codelake_char`, which also records the type in the
//!   field);
//! - `||` and `concat` of a char value give `char(total length)`, each text
//!   padded to its own length (`char(3) || varchar(2)` is `char(5)`); with
//!   unbounded `varchar` or an untyped `NULL` the reference engine fails,
//!   and so does this rule;
//! - functions keep the padded value: `upper`/`lower` give `char(n)`,
//!   `substr` gives `varchar(n)` of the padded text, the trims (`ltrim`,
//!   `rtrim`, `trim`) `varchar(n)` of the text without its padding, `LIKE`
//!   matches the padded text, `length` counts the padding. Functions the
//!   reference engine has no char version of (`strpos`, `replace`, …; it
//!   refuses them) see the padded text, as `CAST(ch AS varchar)` gives it;
//! - text functions have the result length the reference engine derives
//!   from their arguments' (`replace(varchar(x), y, varchar(z))` is
//!   `varchar(x + z * (x + 1))`, `soundex` is `varchar(4)`, an element of
//!   `split(varchar(x), …)` is `varchar(x)`, …), so a char
//!   value output with their result gets the reference's common type.
//!
//! A column's type is followed through the plan (a subquery, an aggregate
//! such as `max(ch)`, a window function, a scalar subquery, one computed
//! ahead by `crate::subquery`), and a projection records the char type of
//! each column it computes, so a client, `typeof` and a `CREATE TABLE … AS`
//! see `char(n)`. A cast to text the statement wrote is marked
//! (`crate::declared`); any other cast between Arrow's text types is type
//! coercion's and keeps the type of its argument, so a plan analyzed twice
//! is rewritten once.
//!
//! Refused, with an error, never another answer (ADR 0015):
//!
//! - where the reference engine fails too: `NATURAL JOIN`
//!   (`refuse_natural_joins`), `char(n) || <unbounded varchar>`,
//!   `char(n) || NULL`, a concatenation longer than `char(65536)`;
//! - a value of a common type whose length this rule cannot tell: text from
//!   a function whose result length it does not know, output together with
//!   a char value (`CASE WHEN … THEN ch ELSE element_at(t.arr, 1) END`);
//!   compare it, or cast it to `varchar(n)`, instead;
//! - a char value searched in an array that is not written out
//!   (`contains(array_agg(s), ch)`): the element type is not followed;
//! - an ordering of rows or arrays of char values whose common length is not
//!   known, and `ANY`/`ALL` of a value holding an aggregate or a subquery.

use std::cmp::Ordering;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    Column, DFSchema, DataFusionError, ExprSchema, Result, ScalarValue, exec_err, plan_err,
};
use datafusion::logical_expr::expr::{
    AggregateFunction, Alias, Between, Case, Cast, Exists, InList, InSubquery, ScalarFunction, SetComparison,
    SetQuantifier, TryCast, WindowFunction,
};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Distinct, Expr, ExprSchemable, Filter, Join, JoinConstraint, LogicalPlan,
    Operator, Projection, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Subquery,
    Union, Values, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{self as sql, Visit, Visitor};

use crate::trino_types::{Declared, declared};

/// The function `a || b` is planned as (`crate::trino_functions`).
const CONCAT_OPERATOR: &str = "codelake_trino_concat_operator";

/// Longest `char(n)` of the reference engine.
const MAX_CHAR: u32 = 65_536;

/// Name of [`CharPad`].
pub(crate) const CHAR_PAD: &str = "codelake_char";
/// Name of [`CharKey`].
pub(crate) const CHAR_KEY: &str = "codelake_char_key";
/// Name of [`CharCompare`].
pub(crate) const CHAR_COMPARE: &str = "codelake_char_compare";

// ---------------------------------------------------------------- functions

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// The Arrow type a text function returns for an argument of `input`.
fn text_result(name: &str, input: &DataType) -> Result<DataType> {
    match input {
        DataType::Null => Ok(DataType::Utf8),
        text if is_text(text) => Ok(text.clone()),
        other => plan_err!("{name} expects text, got {other}"),
    }
}

/// `f` applied to every text of `value`, the result in `value`'s Arrow type.
fn map_text(value: &ColumnarValue, rows: usize, f: impl Fn(&str) -> String) -> Result<ColumnarValue> {
    let array = value.to_array(rows)?;
    let target = text_result("text function", array.data_type())?;
    let text = cast(&array, &DataType::Utf8)?;
    let out: StringArray = text.as_string::<i32>().iter().map(|v| v.map(&f)).collect();
    let out = cast(&(Arc::new(out) as ArrayRef), &target)?;
    Ok(match value {
        ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?),
        ColumnarValue::Array(_) => ColumnarValue::Array(out),
    })
}

/// `text` padded with spaces to `length` characters (a longer text as it is).
pub fn padded(text: &str, length: u32) -> String {
    let width = usize::try_from(length).unwrap_or(usize::MAX);
    let count = text.chars().count();
    let mut out = String::with_capacity(text.len() + width.saturating_sub(count));
    out.push_str(text);
    out.extend(std::iter::repeat_n(' ', width.saturating_sub(count)));
    out
}

/// The reference engine's comparison of char values: both padded with
/// spaces to one length, compared character by character.
fn compare_padded(a: &str, b: &str) -> Ordering {
    let (mut a, mut b) = (a.chars(), b.chars());
    loop {
        let order = match (a.next(), b.next()) {
            (None, None) => return Ordering::Equal,
            (Some(x), None) => x.cmp(&' '),
            (None, Some(y)) => ' '.cmp(&y),
            (Some(x), Some(y)) => x.cmp(&y),
        };
        if order != Ordering::Equal {
            return order;
        }
    }
}

/// The length argument of [`CharPad`].
fn length_argument(scalar: Option<&ScalarValue>) -> Result<u32> {
    let Some(scalar) = scalar else {
        return plan_err!("{CHAR_PAD} expects a length as its second argument");
    };
    match scalar.cast_to(&DataType::Int64)? {
        ScalarValue::Int64(Some(n)) if (1..=i64::from(MAX_CHAR)).contains(&n) => {
            Ok(u32::try_from(n).unwrap_or(MAX_CHAR))
        }
        other => plan_err!("{CHAR_PAD}: invalid length {other}"),
    }
}

/// `codelake_char(x, n)`: `x` as a `char(n)` value, padded with spaces to
/// `n` characters, `char(n)` recorded in the result field.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CharPad {
    signature: Signature,
}

impl ScalarUDFImpl for CharPad {
    fn name(&self) -> &'static str {
        CHAR_PAD
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("{CHAR_PAD} is typed by its arguments")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let length = length_argument(args.scalar_arguments.get(1).copied().flatten())?;
        let Some(input) = args.arg_fields.first() else {
            return plan_err!("{CHAR_PAD} expects two arguments");
        };
        Ok(Arc::new(
            Field::new(
                CHAR_PAD,
                text_result(CHAR_PAD, input.data_type())?,
                input.is_nullable(),
            )
            .with_metadata(Declared::Char(length).metadata()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, length] = args.args.as_slice() else {
            return exec_err!("{CHAR_PAD} expects two arguments");
        };
        let ColumnarValue::Scalar(length) = length else {
            return exec_err!("{CHAR_PAD} expects a constant length");
        };
        let length = length_argument(Some(length))?;
        map_text(value, args.number_rows, |text| padded(text, length))
    }
}

/// `codelake_char_key(x)`: `x` without trailing spaces, what equality of
/// char values compares.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CharKey {
    signature: Signature,
}

impl ScalarUDFImpl for CharKey {
    fn name(&self) -> &'static str {
        CHAR_KEY
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [input] = arg_types else {
            return plan_err!("{CHAR_KEY} expects one argument");
        };
        text_result(CHAR_KEY, input)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return exec_err!("{CHAR_KEY} expects one argument");
        };
        map_text(value, args.number_rows, |text| {
            text.trim_end_matches(' ').to_owned()
        })
    }
}

/// `codelake_char_compare(a, b)`: `-1`, `0` or `1` as `a` sorts before, with
/// or after `b`, both padded with spaces to one length.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CharCompare {
    signature: Signature,
}

impl ScalarUDFImpl for CharCompare {
    fn name(&self) -> &'static str {
        CHAR_COMPARE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return plan_err!("{CHAR_COMPARE} expects two arguments");
        }
        for t in arg_types {
            text_result(CHAR_COMPARE, t)?;
        }
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [a, b] = args.args.as_slice() else {
            return exec_err!("{CHAR_COMPARE} expects two arguments");
        };
        let rows = args.number_rows;
        let (left, right) = (
            cast(&a.to_array(rows)?, &DataType::Utf8)?,
            cast(&b.to_array(rows)?, &DataType::Utf8)?,
        );
        let out: Int32Array = left
            .as_string::<i32>()
            .iter()
            .zip(right.as_string::<i32>().iter())
            .map(|pair| match pair {
                (Some(x), Some(y)) => Some(match compare_padded(x, y) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                }),
                _ => None,
            })
            .collect();
        let out: ArrayRef = Arc::new(out);
        Ok(
            if matches!(a, ColumnarValue::Scalar(_)) && matches!(b, ColumnarValue::Scalar(_)) {
                ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
            } else {
                ColumnarValue::Array(out)
            },
        )
    }
}

static CHAR_PAD_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(CharPad {
        signature: Signature::any(2, Volatility::Immutable),
    }))
});
static CHAR_KEY_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(CharKey {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});
static CHAR_COMPARE_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(CharCompare {
        signature: Signature::any(2, Volatility::Immutable),
    }))
});

/// The functions this module plans, for the engine to register.
pub(crate) fn functions() -> Vec<ScalarUDF> {
    [&CHAR_PAD_UDF, &CHAR_KEY_UDF, &CHAR_COMPARE_UDF]
        .into_iter()
        .map(|udf| udf.as_ref().clone())
        .collect()
}

/// `expr` as a `char(length)` value (`codelake_char`).
pub fn pad(expr: Expr, length: u32) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&CHAR_PAD_UDF),
        vec![
            expr,
            Expr::Literal(ScalarValue::Int64(Some(i64::from(length))), None),
        ],
    ))
}

fn key(expr: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&CHAR_KEY_UDF), vec![expr]))
}

fn compare_call(left: Expr, right: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::clone(&CHAR_COMPARE_UDF),
        vec![left, right],
    ))
}

// ---------------------------------------------------------------- types

/// How long the values of a `varchar` can be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bound {
    /// `varchar(n)`.
    Known(u32),
    /// `varchar`.
    Unbounded,
    /// A length this rule does not know.
    Uncertain,
}

/// The reference engine's type of a text value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Text {
    /// An untyped `NULL`.
    Null,
    Varchar(Bound),
    /// `char(n)`; `None` when `n` is not known.
    Char(Option<u32>),
    /// An array of `char(n)` values.
    Array(Option<u32>),
}

impl Text {
    fn is_char(self) -> bool {
        matches!(self, Self::Char(_))
    }

    /// The length a value of this type has in a common char type.
    fn char_length(self) -> Option<u32> {
        match self {
            Self::Null => Some(0),
            Self::Char(n) => n,
            Self::Varchar(Bound::Known(k)) => Some(k.min(MAX_CHAR)),
            Self::Varchar(Bound::Unbounded) => Some(MAX_CHAR),
            Self::Varchar(Bound::Uncertain) | Self::Array(_) => None,
        }
    }
}

/// The type of a field: what it declares, or `varchar` for other text.
fn from_field(field: &Field) -> Option<Text> {
    match declared(field) {
        Some(Declared::Char(n)) => Some(Text::Char(Some(n))),
        Some(Declared::Varchar(k)) => Some(Text::Varchar(Bound::Known(k))),
        None if is_text(field.data_type()) => Some(Text::Varchar(Bound::Unbounded)),
        None if field.data_type().is_null() => Some(Text::Null),
        _ => None,
    }
}

/// The common type of values of `types` (see the module documentation);
/// `None` when one is not text.
fn common(types: &[Option<Text>]) -> Option<Text> {
    let mut known = Vec::with_capacity(types.len());
    for t in types {
        match t {
            None => return None,
            Some(Text::Null) => {}
            Some(t) => known.push(*t),
        }
    }
    if known.is_empty() {
        return Some(Text::Null);
    }
    if let Some(Text::Array(n)) = known.iter().find(|t| matches!(t, Text::Array(_))) {
        let n = *n;
        return known
            .iter()
            .all(|t| *t == Text::Array(n))
            .then_some(Text::Array(n));
    }
    if known.iter().any(|t| t.is_char()) {
        let mut length = Some(0);
        for t in &known {
            length = match (length, t.char_length()) {
                (Some(a), Some(b)) => Some(a.max(b)),
                _ => None,
            };
        }
        return Some(Text::Char(length));
    }
    let mut bound = Bound::Known(0);
    for t in &known {
        bound = match (bound, t) {
            (Bound::Uncertain, _) | (_, Text::Varchar(Bound::Uncertain)) => Bound::Uncertain,
            (Bound::Known(a), Text::Varchar(Bound::Known(b))) => Bound::Known(a.max(*b)),
            _ => Bound::Unbounded,
        };
    }
    Some(Text::Varchar(bound))
}

/// What a node's expressions refer to: its inputs (their columns together,
/// in `schema`), and the inputs of the nodes around the subquery it is in.
struct Scope<'a> {
    inputs: Vec<&'a LogicalPlan>,
    outer: &'a [&'a LogicalPlan],
    schema: Arc<DFSchema>,
}

impl<'a> Scope<'a> {
    fn new(inputs: Vec<&'a LogicalPlan>, own: &Arc<DFSchema>, outer: &'a [&'a LogicalPlan]) -> Self {
        let schema = match inputs.split_first() {
            None => Arc::clone(own),
            Some((first, [])) => Arc::clone(first.schema()),
            Some((first, rest)) => {
                let mut merged = first.schema().as_ref().clone();
                for input in rest {
                    merged.merge(input.schema());
                }
                Arc::new(merged)
            }
        };
        Self {
            inputs,
            outer,
            schema,
        }
    }

    fn over(input: &'a LogicalPlan, outer: &'a [&'a LogicalPlan]) -> Self {
        Self::new(vec![input], input.schema(), outer)
    }

    fn empty() -> Self {
        Self {
            inputs: Vec::new(),
            outer: &[],
            schema: Arc::new(DFSchema::empty()),
        }
    }

    fn is_text(&self, expr: &Expr) -> bool {
        expr.get_type(self.schema.as_ref()).is_ok_and(|t| is_text(&t))
    }

    /// Text of a type this rule does not follow: a length it does not know.
    fn unknown(&self, expr: &Expr) -> Option<Text> {
        self.is_text(expr).then_some(Text::Varchar(Bound::Uncertain))
    }
}

/// The one of `plans` that has column `column`, and its position there.
fn find_column<'a>(plans: &[&'a LogicalPlan], column: &Column) -> Option<(&'a LogicalPlan, usize)> {
    plans.iter().find_map(|plan| {
        let index = plan.schema().index_of_column(column).ok()?;
        Some((*plan, index))
    })
}

/// The length of column `index` of `plan`'s output, if it is a `char(n)`
/// (the value of a subquery computed ahead, `crate::subquery`).
pub(crate) fn char_column(plan: &LogicalPlan, index: usize) -> Option<u32> {
    match column_type(plan, index, &[]) {
        Some(Text::Char(Some(n))) => Some(n),
        _ => None,
    }
}

/// The type of column `index` of `plan`'s output, followed through the plan.
fn column_type(plan: &LogicalPlan, index: usize, outer: &[&LogicalPlan]) -> Option<Text> {
    match plan {
        LogicalPlan::Projection(projection) => infer(
            projection.expr.get(index)?,
            &Scope::over(&projection.input, outer),
        ),
        LogicalPlan::Aggregate(aggregate) => {
            let groups = grouping_set_to_exprlist(&aggregate.group_expr).ok()?;
            let grouping_id = usize::from(matches!(aggregate.group_expr.as_slice(), [Expr::GroupingSet(_)]));
            let expr = if index < groups.len() {
                groups[index]
            } else {
                aggregate
                    .aggr_expr
                    .get(index.checked_sub(groups.len() + grouping_id)?)?
            };
            infer(expr, &Scope::over(&aggregate.input, outer))
        }
        LogicalPlan::Window(window) => {
            let columns = window.input.schema().fields().len();
            match index.checked_sub(columns) {
                None => column_type(&window.input, index, outer),
                Some(at) => infer(window.window_expr.get(at)?, &Scope::over(&window.input, outer)),
            }
        }
        LogicalPlan::Union(union) => {
            let types: Vec<Option<Text>> = union
                .inputs
                .iter()
                .map(|input| column_type(input, index, outer))
                .collect();
            common(&types)
        }
        LogicalPlan::Values(values) => {
            let empty = Scope::empty();
            let types: Vec<Option<Text>> = values
                .values
                .iter()
                .map(|row| row.get(index).and_then(|cell| infer(value_of_cell(cell), &empty)))
                .collect();
            common(&types)
        }
        LogicalPlan::SubqueryAlias(alias) => column_type(&alias.input, index, outer),
        LogicalPlan::Subquery(subquery) => column_type(&subquery.subquery, index, outer),
        LogicalPlan::Distinct(Distinct::All(input)) => column_type(input, index, outer),
        LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Repartition(_)
            if plan.inputs().len() == 1 =>
        {
            column_type(plan.inputs()[0], index, outer)
        }
        _ if plan.inputs().is_empty() => from_field(plan.schema().field(index)),
        other => {
            // The inputs' columns, found by name (a join's are both sides').
            let (qualifier, field) = other.schema().qualified_field(index);
            let column = Column::from((qualifier, field));
            match find_column(&other.inputs(), &column) {
                Some((input, at)) => column_type(input, at, outer),
                None => from_field(field),
            }
        }
    }
}

/// A cell of a planned `VALUES` list as written: without the cast the SQL
/// planner put around it and the mark of `crate::coercion`.
fn value_of_cell(cell: &Expr) -> &Expr {
    let mut cell = cell;
    loop {
        cell = match cell {
            Expr::Cast(cast) => cast.expr.as_ref(),
            Expr::Alias(alias) => alias.expr.as_ref(),
            Expr::ScalarFunction(ScalarFunction { func, args })
                if func.name() == crate::coercion::VALUES_CELL && args.len() == 1 =>
            {
                &args[0]
            }
            other => return other,
        };
    }
}

fn text_of(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::Utf8View(Some(text))
        | ScalarValue::LargeUtf8(Some(text)) => Some(text),
        _ => None,
    }
}

fn characters(text: &str) -> u32 {
    u32::try_from(text.chars().count()).unwrap_or(u32::MAX)
}

/// The type a `codelake_trino_type(x, '<type>')` names.
fn typed_as(args: &[Expr], scope: &Scope<'_>) -> Option<Text> {
    let name = match args.get(1)? {
        Expr::Literal(value, _) => text_of(value)?,
        _ => return None,
    };
    if name.is_empty() {
        return scope.is_text(&args[0]).then_some(Text::Varchar(Bound::Unbounded));
    }
    match Declared::parse(name)? {
        Declared::Char(n) => Some(Text::Char(Some(n))),
        Declared::Varchar(k) => Some(Text::Varchar(Bound::Known(k))),
        Declared::Timestamp(_) => None,
    }
}

/// The type of `||` / `concat` of `args`: `char(total)` when one is char.
fn concat_type(args: &[Expr], scope: &Scope<'_>) -> Text {
    let types: Vec<Option<Text>> = args.iter().map(|a| infer(a, scope)).collect();
    if !types.iter().flatten().any(|t| t.is_char()) {
        return Text::Varchar(Bound::Unbounded);
    }
    let mut total: Option<u32> = Some(0);
    for t in &types {
        let length = match t {
            Some(Text::Null) => Some(0),
            Some(Text::Char(n)) => *n,
            Some(Text::Varchar(Bound::Known(k))) => Some(*k),
            _ => None,
        };
        total = total
            .zip(length)
            .and_then(|(a, b)| a.checked_add(b))
            .filter(|n| *n <= MAX_CHAR);
    }
    Text::Char(total)
}

/// Functions whose result is text of the type of their first argument's
/// length (`upper(char(n))` is `char(n)`; `rtrim(char(n))` is `varchar(n)`).
fn keeps_char(name: &str) -> bool {
    matches!(name, "upper" | "lower")
}

fn keeps_length(name: &str, arity: usize) -> bool {
    matches!(
        name,
        "rtrim"
            | "ltrim"
            | "btrim"
            | "trim"
            | "substr"
            | "substring"
            | "reverse"
            | "split_part"
            | "regexp_extract"
            | "json_extract_scalar"
            | "url_decode"
            | "url_extract_host"
            | "url_extract_path"
            | "url_extract_query"
            | "url_extract_fragment"
            | "url_extract_protocol"
            | "url_extract_parameter"
    ) || (matches!(name, "replace" | "regexp_replace") && arity == 2)
}

/// Longest `varchar(n)` of the reference engine: a derived length from
/// here on is unbounded `varchar`.
const MAX_VARCHAR: u64 = 2_147_483_646;

/// The length a text argument brings into a derived result length.
enum Length {
    Known(u64),
    Unbounded,
    Uncertain,
}

fn argument_length(arg: Option<&Expr>, scope: &Scope<'_>) -> Length {
    match arg.and_then(|a| infer(a, scope)) {
        Some(Text::Null) => Length::Known(0),
        Some(Text::Varchar(Bound::Known(k)) | Text::Char(Some(k))) => Length::Known(u64::from(k)),
        Some(Text::Varchar(Bound::Unbounded)) => Length::Unbounded,
        Some(Text::Varchar(Bound::Uncertain) | Text::Char(None) | Text::Array(_)) | None => Length::Uncertain,
    }
}

/// `varchar(f(lengths))` from the lengths of `args` (every one known), as
/// the reference engine derives it: unbounded past its longest `varchar`.
fn derived_varchar(args: &[Length], f: impl Fn(&[u64]) -> Option<u64>) -> Text {
    let mut known = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            Length::Known(n) => known.push(*n),
            Length::Unbounded => return Text::Varchar(Bound::Unbounded),
            Length::Uncertain => return Text::Varchar(Bound::Uncertain),
        }
    }
    match f(&known)
        .filter(|n| *n <= MAX_VARCHAR)
        .and_then(|n| u32::try_from(n).ok())
    {
        Some(n) => Text::Varchar(Bound::Known(n)),
        None => Text::Varchar(Bound::Unbounded),
    }
}

/// Text functions whose result length the reference engine computes from
/// its arguments' lengths (observed on `trinodb/trino:431` with `typeof`).
fn computed_length(name: &str, args: &[Expr], scope: &Scope<'_>) -> Option<Text> {
    let length = |i: usize| argument_length(args.get(i), scope);
    match (name, args.len()) {
        // `replace(varchar(x), y, varchar(z))`: `x + z * (x + 1)`.
        ("replace", 3) => Some(derived_varchar(&[length(0), length(2)], |l| {
            l[1].checked_mul(l[0] + 1)?.checked_add(l[0])
        })),
        // `regexp_replace(varchar(x), pattern, varchar(y))`:
        // `x + max(x * y / 2, y) * (x + 1)`.
        ("regexp_replace", 3) => Some(derived_varchar(&[length(0), length(2)], |l| {
            (l[0].checked_mul(l[1])? / 2)
                .max(l[1])
                .checked_mul(l[0] + 1)?
                .checked_add(l[0])
        })),
        ("url_encode", 1) => Some(derived_varchar(&[length(0)], |l| l[0].checked_mul(12))),
        ("soundex", 1) => Some(Text::Varchar(Bound::Known(4))),
        // `to_iso8601(date)`: `varchar(16)` (of a timestamp the length
        // follows its precision, not derived here).
        ("to_iso8601", 1) => args[0]
            .get_type(scope.schema.as_ref())
            .is_ok_and(|t| matches!(t, DataType::Date32 | DataType::Date64))
            .then_some(Text::Varchar(Bound::Known(16))),
        // An element of the array `split`, `regexp_split` or
        // `regexp_extract_all` gives: `array(varchar(x))` of `varchar(x)`.
        ("element_at" | crate::trino_functions::SUBSCRIPT, 2) => match &args[0] {
            Expr::ScalarFunction(ScalarFunction { func, args: inner })
                if matches!(func.name(), "split" | "regexp_split" | "regexp_extract_all") =>
            {
                match argument_length(inner.first(), scope) {
                    Length::Known(n) => u32::try_from(n).ok().map(|n| Text::Varchar(Bound::Known(n))),
                    Length::Unbounded => Some(Text::Varchar(Bound::Unbounded)),
                    Length::Uncertain => Some(Text::Varchar(Bound::Uncertain)),
                }
            }
            _ => None,
        },
        _ => None,
    }
}

/// Functions whose result is unbounded `varchar`.
fn unbounded_result(name: &str) -> bool {
    matches!(
        name,
        "lpad"
            | "rpad"
            | "concat_ws"
            | "format"
            | "translate"
            | "normalize"
            | "to_hex"
            | "date_format"
            | "format_datetime"
            | "array_join"
            | "typeof"
            | "json_format"
            | "to_base64"
            | "listagg"
            | "string_agg"
            | "from_utf8"
            | "to_base64url"
            | "human_readable_seconds"
            | "current_timezone"
            | "version"
    )
}

fn function_type(name: &str, args: &[Expr], scope: &Scope<'_>, whole: &Expr) -> Option<Text> {
    if let Some(text) = computed_length(name, args, scope) {
        return scope.is_text(whole).then_some(text);
    }
    let first = || args.first().and_then(|a| infer(a, scope));
    match name {
        crate::declared::TRINO_TYPED | crate::declared::TRINO_TRY_TYPED => typed_as(args, scope),
        CHAR_PAD => args.get(1).and_then(|n| match n {
            Expr::Literal(value, _) => length_argument(Some(value)).ok().map(|n| Text::Char(Some(n))),
            _ => None,
        }),
        crate::coercion::VALUES_CELL | "nullif" => first(),
        // A char value without its padding: `varchar(n)`.
        CHAR_KEY => match first()? {
            Text::Char(n) => Some(Text::Varchar(n.map_or(Bound::Uncertain, Bound::Known))),
            other => Some(other),
        },
        "coalesce" | "nvl" | "ifnull" | "greatest" | "least" => {
            let types: Vec<Option<Text>> = args.iter().map(|a| infer(a, scope)).collect();
            common(&types)
        }
        "nvl2" => {
            let types: Vec<Option<Text>> = args.iter().skip(1).map(|a| infer(a, scope)).collect();
            common(&types)
        }
        "concat" | CONCAT_OPERATOR if scope.is_text(whole) => Some(concat_type(args, scope)),
        "chr" => Some(Text::Varchar(Bound::Known(1))),
        "make_array" | "array" | "make_list" => {
            let types: Vec<Option<Text>> = args.iter().map(|a| infer(a, scope)).collect();
            match common(&types) {
                Some(Text::Char(n)) => Some(Text::Array(n)),
                _ => None,
            }
        }
        _ if keeps_char(name) => match first()? {
            Text::Null => Some(Text::Null),
            other => Some(other),
        },
        _ if keeps_length(name, args.len()) => match first()? {
            Text::Char(n) => Some(Text::Varchar(n.map_or(Bound::Uncertain, Bound::Known))),
            other => Some(other),
        },
        _ if unbounded_result(name) => scope.is_text(whole).then_some(Text::Varchar(Bound::Unbounded)),
        _ => scope.unknown(whole),
    }
}

/// Aggregates and window functions whose result is one of their first
/// argument's values.
fn picks_a_value(name: &str) -> bool {
    matches!(
        name,
        "max"
            | "min"
            | "first_value"
            | "last_value"
            | "nth_value"
            | "any_value"
            | "arbitrary"
            | "lag"
            | "lead"
    )
}

/// The reference engine's type of `expr`, if it is text.
fn infer(expr: &Expr, scope: &Scope<'_>) -> Option<Text> {
    match expr {
        Expr::Alias(Alias { expr, .. }) => infer(expr, scope),
        Expr::Column(column) => match find_column(&scope.inputs, column) {
            Some((input, at)) => column_type(input, at, scope.outer),
            None => scope
                .schema
                .field_from_column(column)
                .ok()
                .and_then(|f| from_field(f)),
        },
        Expr::OuterReferenceColumn(field, column) => match find_column(scope.outer, column) {
            Some((input, at)) => column_type(input, at, &[]),
            None => from_field(field),
        },
        Expr::Literal(value, metadata) => {
            if value.is_null() {
                return Some(Text::Null);
            }
            let text = text_of(value)?;
            let declared = metadata
                .as_ref()
                .and_then(|m| m.inner().get(crate::trino_types::TRINO_TYPE))
                .and_then(|name| Declared::parse(name));
            Some(match declared {
                Some(Declared::Char(n)) => Text::Char(Some(n)),
                Some(Declared::Varchar(k)) => Text::Varchar(Bound::Known(k)),
                _ => Text::Varchar(Bound::Known(characters(text))),
            })
        }
        // A cast to text the statement wrote is marked (`crate::declared`):
        // any other is type coercion's, between Arrow's text types.
        Expr::Cast(Cast { expr: inner, field }) | Expr::TryCast(TryCast { expr: inner, field }) => {
            if !is_text(field.data_type()) {
                None
            } else if scope.is_text(inner) || matches!(inner.as_ref(), Expr::Literal(v, _) if v.is_null()) {
                infer(inner, scope)
            } else {
                Some(Text::Varchar(Bound::Unbounded))
            }
        }
        Expr::ScalarSubquery(subquery) => column_type(&subquery.subquery, 0, &outer_of(scope)),
        Expr::Case(case) => {
            let types: Vec<Option<Text>> = case
                .when_then_expr
                .iter()
                .map(|(_, then)| infer(then, scope))
                .chain(case.else_expr.iter().map(|e| infer(e, scope)))
                .collect();
            common(&types)
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::StringConcat,
            right,
        }) => Some(concat_type(
            &[left.as_ref().clone(), right.as_ref().clone()],
            scope,
        )),
        Expr::ScalarFunction(ScalarFunction { func, args }) => function_type(func.name(), args, scope, expr),
        Expr::AggregateFunction(AggregateFunction { func, params }) => {
            if func.name() == "array_agg" {
                return match params.args.first().and_then(|a| infer(a, scope)) {
                    Some(Text::Char(n)) => Some(Text::Array(n)),
                    _ => None,
                };
            }
            if picks_a_value(func.name()) {
                params.args.first().and_then(|a| infer(a, scope))
            } else if unbounded_result(func.name()) {
                scope.is_text(expr).then_some(Text::Varchar(Bound::Unbounded))
            } else {
                scope.unknown(expr)
            }
        }
        Expr::WindowFunction(window) => {
            let WindowFunction { fun, params } = window.as_ref();
            let args = &params.args;
            match fun.name() {
                "lag" | "lead" => {
                    let types: Vec<Option<Text>> = [args.first(), args.get(2)]
                        .into_iter()
                        .flatten()
                        .map(|a| infer(a, scope))
                        .collect();
                    common(&types)
                }
                name if picks_a_value(name) => args.first().and_then(|a| infer(a, scope)),
                _ => scope.unknown(expr),
            }
        }
        _ => scope.unknown(expr),
    }
}

/// The plans a subquery of `scope`'s node may refer to.
fn outer_of<'a>(scope: &Scope<'a>) -> Vec<&'a LogicalPlan> {
    let mut all = scope.inputs.clone();
    all.extend(scope.outer.iter().copied());
    all
}

// ---------------------------------------------------------------- rewrites

fn refusal(what: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{what} is not supported yet: the length of the char(n) type the values share cannot be \
         determined; cast the text to varchar(n)"
    ))
}

fn is_equality(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq | Operator::NotEq | Operator::IsDistinctFrom | Operator::IsNotDistinctFrom
    )
}

fn is_ordering(op: Operator) -> bool {
    matches!(op, Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq)
}

/// Whether values of `types` need converting before they are compared: one
/// is char, all are text, and not all the same `char(n)`.
fn needs_char_coercion(types: &[Option<Text>]) -> bool {
    if !types.iter().flatten().any(|t| t.is_char())
        || types.iter().any(Option::is_none)
        || types.iter().flatten().any(|t| matches!(t, Text::Array(_)))
    {
        return false;
    }
    let mut chars = types.iter().flatten().filter(|t| **t != Text::Null);
    let Some(first) = chars.next() else {
        return false;
    };
    !(matches!(first, Text::Char(Some(_))) && chars.all(|t| t == first))
}

/// Whether values of `types` put an array of char values together with
/// another array: its elements would have to be converted, which this rule
/// does not do but for arrays written out.
fn mixes_char_arrays(types: &[Option<Text>]) -> bool {
    let Some(array) = types.iter().flatten().find(|t| matches!(t, Text::Array(_))) else {
        return false;
    };
    !types
        .iter()
        .all(|t| matches!(t, Some(Text::Null)) || *t == Some(*array))
}

fn char_array_refusal(what: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{what} of an array of char(n) values with another array is not supported yet: write the \
         arrays out (ARRAY[...]) or cast the char values to varchar"
    ))
}

/// A text literal, and its text.
fn literal_text(expr: &Expr) -> Option<(&ScalarValue, &str)> {
    match expr {
        Expr::Literal(value, _) => Some((value, text_of(value)?)),
        _ => None,
    }
}

/// A text literal of `value`'s kind holding `text`.
fn text_literal_like(value: &ScalarValue, text: String) -> Expr {
    let value = match value {
        ScalarValue::Utf8View(_) => ScalarValue::Utf8View(Some(text)),
        ScalarValue::LargeUtf8(_) => ScalarValue::LargeUtf8(Some(text)),
        _ => ScalarValue::Utf8(Some(text)),
    };
    Expr::Literal(value, None)
}

/// A text literal compared with a `char(n)` value, padded to `n` so the two
/// compare as they are; `None` when it does not fit in `n` characters
/// (trailing spaces aside for an equality).
fn fitted_literal(expr: &Expr, length: u32, equality: bool) -> Option<Expr> {
    let (value, text) = literal_text(expr)?;
    let text = if equality {
        text.trim_end_matches(' ')
    } else {
        text
    };
    (characters(text) <= length).then(|| text_literal_like(value, padded(text, length)))
}

/// What a row or array constructor builds from its arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Built {
    /// `struct(a, b)`, `row(a, b)`: fields in order.
    Row,
    /// `named_struct('x', a, 'y', b)`: fields at odd positions.
    Named,
    /// `make_array(a, b)`: elements.
    Array,
}

fn built(expr: &Expr) -> Option<Built> {
    let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr else {
        return None;
    };
    match func.name() {
        "struct" | "row" => Some(Built::Row),
        "named_struct" => Some(Built::Named),
        "make_array" | "array" | "make_list" => Some(Built::Array),
        _ => None,
    }
}

/// The positions of a constructor's arguments that are values.
fn value_positions(kind: Built, arity: usize) -> Vec<usize> {
    match kind {
        Built::Named => (1..arity).step_by(2).collect(),
        Built::Row | Built::Array => (0..arity).collect(),
    }
}

fn constructor_args(expr: Expr) -> (Arc<ScalarUDF>, Vec<Expr>) {
    match expr {
        Expr::ScalarFunction(ScalarFunction { func, args }) => (func, args),
        _ => (Arc::clone(&CHAR_KEY_UDF), Vec::new()),
    }
}

/// The values of two constructors converted for `op` where char semantics
/// apply: rows field by field, arrays all their elements together.
fn compare_built(left: Expr, op: Operator, right: Expr, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    let (Some(lk), Some(rk)) = (built(&left), built(&right)) else {
        return Ok(Transformed::no(binary(left, op, right)));
    };
    let (lf, mut la) = constructor_args(left);
    let (rf, mut ra) = constructor_args(right);
    let (lp, rp) = (value_positions(lk, la.len()), value_positions(rk, ra.len()));
    let rebuild = |la: Vec<Expr>, ra: Vec<Expr>| binary(lf.call(la), op, rf.call(ra));
    let convert = |e: Expr, types: &[Option<Text>]| -> Result<Expr> {
        if is_equality(op) {
            return Ok(key(e));
        }
        match common(types) {
            Some(Text::Char(Some(n))) => Ok(pad(e, n)),
            _ => Err(refusal("an ordering of rows or arrays of char(n) values")),
        }
    };
    let arrays = lk == Built::Array && rk == Built::Array;
    let rows = lk != Built::Array && rk != Built::Array && lp.len() == rp.len();
    if arrays {
        let types: Vec<Option<Text>> = lp
            .iter()
            .map(|i| infer(&la[*i], scope))
            .chain(rp.iter().map(|i| infer(&ra[*i], scope)))
            .collect();
        if !needs_char_coercion(&types) {
            return Ok(Transformed::no(rebuild(la, ra)));
        }
        for i in &lp {
            la[*i] = convert(std::mem::take(&mut la[*i]), &types)?;
        }
        for i in &rp {
            ra[*i] = convert(std::mem::take(&mut ra[*i]), &types)?;
        }
        return Ok(Transformed::yes(rebuild(la, ra)));
    }
    if !rows {
        return Ok(Transformed::no(rebuild(la, ra)));
    }
    let mut changed = false;
    for (i, j) in lp.into_iter().zip(rp) {
        let types = [infer(&la[i], scope), infer(&ra[j], scope)];
        if needs_char_coercion(&types) && !types.contains(&Some(Text::Null)) {
            la[i] = convert(std::mem::take(&mut la[i]), &types)?;
            ra[j] = convert(std::mem::take(&mut ra[j]), &types)?;
            changed = true;
        }
    }
    Ok(Transformed::new_transformed(rebuild(la, ra), changed))
}

/// Array functions that compare their array's elements with a value.
fn searches_array(name: &str) -> bool {
    matches!(
        name,
        "array_has"
            | "array_contains"
            | "list_has"
            | "contains"
            | "array_position"
            | "array_indexof"
            | "list_position"
            | "list_indexof"
            | "array_positions"
            | "list_positions"
    )
}

/// `contains(ARRAY[…], x)` and the like: the elements and the value
/// compared with char semantics. Refused when the array is not written out
/// and the value is char.
fn search_array(func: &ScalarUDF, args: Vec<Expr>, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    let (Some(array), Some(needle)) = (args.first(), args.get(1)) else {
        return Ok(Transformed::no(func.call(args)));
    };
    let needle_type = infer(needle, scope);
    if built(array) != Some(Built::Array) {
        let elements = infer(array, scope);
        let same = matches!(
            (elements, needle_type),
            (Some(Text::Array(Some(n))), Some(Text::Char(Some(m)))) if n == m
        );
        let elements_are_char = matches!(elements, Some(Text::Array(_)));
        if !same
            && (needle_type.is_some_and(Text::is_char)
                || elements_are_char && needle_type != Some(Text::Null))
        {
            return Err(DataFusionError::NotImplemented(format!(
                "{}: comparing a char(n) value with the elements of an array that is not written out \
                 (ARRAY[...]), or text with the elements of an array of char(n) values, is not supported yet; \
                 cast the char values to varchar",
                func.name()
            )));
        }
        return Ok(Transformed::no(func.call(args)));
    }
    let mut args = args;
    let (inner, mut elements) = constructor_args(args.remove(0));
    let mut types: Vec<Option<Text>> = elements.iter().map(|e| infer(e, scope)).collect();
    types.push(needle_type);
    if !needs_char_coercion(&types) {
        args.insert(0, inner.call(elements));
        return Ok(Transformed::no(func.call(args)));
    }
    elements = elements.into_iter().map(key).collect();
    let needle = key(args.remove(0));
    let mut rebuilt = vec![inner.call(elements), needle];
    rebuilt.extend(args);
    Ok(Transformed::yes(func.call(rebuilt)))
}

/// `left op right`, with char semantics (see the module documentation).
fn compare(left: Expr, op: Operator, right: Expr, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    if built(&left).is_some() && built(&right).is_some() {
        return compare_built(left, op, right, scope);
    }
    let types = [infer(&left, scope), infer(&right, scope)];
    if types.iter().flatten().any(|t| matches!(t, Text::Array(_)))
        && (mixes_char_arrays(&types) || types.contains(&None))
    {
        return Err(char_array_refusal("a comparison"));
    }
    if !needs_char_coercion(&types) || types.contains(&Some(Text::Null)) {
        return Ok(Transformed::no(binary(left, op, right)));
    }
    let equality = is_equality(op);
    // A literal against a `char(n)` value: pad the literal.
    if let [Some(Text::Char(Some(n))), _] = types
        && let Some(fitted) = fitted_literal(&right, n, equality)
    {
        return Ok(Transformed::yes(binary(left, op, fitted)));
    }
    if let [_, Some(Text::Char(Some(n)))] = types
        && let Some(fitted) = fitted_literal(&left, n, equality)
    {
        return Ok(Transformed::yes(binary(fitted, op, right)));
    }
    if equality {
        return Ok(Transformed::yes(binary(key(left), op, key(right))));
    }
    if is_ordering(op) {
        let zero = Expr::Literal(ScalarValue::Int32(Some(0)), None);
        return Ok(Transformed::yes(binary(compare_call(left, right), op, zero)));
    }
    Ok(Transformed::no(binary(left, op, right)))
}

fn binary(left: Expr, op: Operator, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
}

/// `exprs`, output in their common type: each padded to its length when
/// one is char. `what` names the construct in a refusal.
fn unify_values(exprs: Vec<Expr>, scope: &Scope<'_>, what: &str) -> Result<Transformed<Vec<Expr>>> {
    let types: Vec<Option<Text>> = exprs.iter().map(|e| infer(e, scope)).collect();
    if !needs_char_coercion(&types) {
        return Ok(Transformed::no(exprs));
    }
    let Some(Text::Char(Some(length))) = common(&types) else {
        return Err(refusal(what));
    };
    Ok(Transformed::yes(
        exprs
            .into_iter()
            .zip(types)
            .map(|(e, t)| match t {
                Some(Text::Null) => e,
                Some(Text::Char(Some(n))) if n == length => e,
                _ => pad(e, length),
            })
            .collect(),
    ))
}

/// `||` / `concat` of a char value: every other text padded to its own
/// length (`varchar(k)` to `k`).
fn concat_args(args: Vec<Expr>, scope: &Scope<'_>) -> Result<Transformed<Vec<Expr>>> {
    let types: Vec<Option<Text>> = args.iter().map(|a| infer(a, scope)).collect();
    if !types.iter().flatten().any(|t| t.is_char()) {
        return Ok(Transformed::no(args));
    }
    if types.contains(&Some(Text::Null)) {
        // Trino 431: "Could not choose a best candidate operator".
        return Err(DataFusionError::Plan(
            "Could not choose a best candidate operator for a char(n) value concatenated with an untyped \
             NULL. Explicit type casts must be added."
                .into(),
        ));
    }
    if !matches!(concat_type(&args, scope), Text::Char(Some(_))) {
        return Err(DataFusionError::NotImplemented(
            "concatenating a char(n) value with text of unbounded or unknown length is not supported: \
             the result would be a char longer than char(65536); cast the char value to varchar"
                .into(),
        ));
    }
    let mut changed = false;
    let args = args
        .into_iter()
        .zip(types)
        .map(|(a, t)| match t {
            Some(Text::Varchar(Bound::Known(k)))
                if literal_text(&a).is_none_or(|(_, text)| characters(text) != k) =>
            {
                changed = true;
                pad(a, k)
            }
            _ => a,
        })
        .collect();
    Ok(Transformed::new_transformed(args, changed))
}

/// `x [NOT] IN (list)` with char semantics.
fn in_list(list: InList, scope: &Scope<'_>) -> Transformed<Expr> {
    let InList { expr, list, negated } = list;
    let mut types = vec![infer(&expr, scope)];
    types.extend(list.iter().map(|e| infer(e, scope)));
    if !needs_char_coercion(&types) {
        return Transformed::no(Expr::InList(InList::new(expr, list, negated)));
    }
    if let Some(Text::Char(Some(n))) = types[0] {
        let fitted: Option<Vec<Expr>> = list
            .iter()
            .map(|item| match item {
                Expr::Literal(value, _) if value.is_null() => Some(item.clone()),
                other => fitted_literal(other, n, true),
            })
            .collect();
        if let Some(fitted) = fitted {
            return Transformed::yes(Expr::InList(InList::new(expr, fitted, negated)));
        }
    }
    let list = list
        .into_iter()
        .zip(types.into_iter().skip(1))
        .map(|(item, t)| if t == Some(Text::Null) { item } else { key(item) })
        .collect();
    Transformed::yes(Expr::InList(InList::new(Box::new(key(*expr)), list, negated)))
}

/// Name of the column a subquery compares once converted (see
/// [`map_first_column`]).
const CONVERTED: &str = "codelake_char_value";

/// `plan` with its first column `f(column)`, named [`CONVERTED`]: a
/// projection's first expression changed in place, any other plan under a
/// projection. The name must differ from the column's: `DataFusion` 54
/// cannot plan the mark join of a subquery whose column is computed into a
/// column of the same name (`ProjectionPushdown`: "Input field name … does not
/// match with the projection expression").
fn map_first_column(plan: &LogicalPlan, f: impl FnOnce(Expr) -> Expr) -> Result<LogicalPlan> {
    if let LogicalPlan::Projection(projection) = plan
        && let Some(first) = projection.expr.first()
    {
        let mut exprs = projection.expr.clone();
        exprs[0] = f(first.clone().unalias()).alias(CONVERTED);
        return Ok(LogicalPlan::Projection(Projection::try_new(
            exprs,
            Arc::clone(&projection.input),
        )?));
    }
    let mut exprs: Vec<Expr> = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
        .collect();
    if let Some(first) = exprs.first_mut() {
        let column = std::mem::take(first);
        *first = f(column).alias(CONVERTED);
    }
    Ok(LogicalPlan::Projection(Projection::try_new(
        exprs,
        Arc::new(plan.clone()),
    )?))
}

fn with_plan(subquery: Subquery, plan: LogicalPlan) -> Subquery {
    Subquery {
        subquery: Arc::new(plan),
        outer_ref_columns: subquery.outer_ref_columns,
        spans: subquery.spans,
    }
}

/// `x [NOT] IN (subquery)` and `x op ANY/ALL (subquery)` with char
/// semantics: `IN` compares both sides without trailing spaces (the
/// subquery's column by a projection); `ANY`/`ALL` see [`quantified`].
fn subquery_comparison(expr: Expr, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    let outer = outer_of(scope);
    let (Expr::InSubquery(InSubquery {
        expr: value,
        subquery,
        ..
    })
    | Expr::SetComparison(SetComparison {
        expr: value,
        subquery,
        ..
    })) = &expr
    else {
        return Ok(Transformed::no(expr));
    };
    let types = [infer(value, scope), column_type(&subquery.subquery, 0, &outer)];
    if !needs_char_coercion(&types) || types.contains(&Some(Text::Null)) {
        return Ok(Transformed::no(expr));
    }
    if let Expr::SetComparison(comparison) = expr {
        return quantified(comparison, scope).map(Transformed::yes);
    }
    Ok(match expr {
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => {
            let plan = map_first_column(&subquery.subquery, key)?;
            Transformed::yes(Expr::InSubquery(InSubquery::new(
                Box::new(key(*expr)),
                with_plan(subquery, plan),
                negated,
            )))
        }
        other => Transformed::no(other),
    })
}

/// `x op ANY (s)` / `x op ALL (s)` with char semantics, as the `EXISTS`
/// tests they mean (`NULL` alike): for `ANY`, true when some element holds,
/// `NULL` when none holds but `x` or an element is `NULL`, false otherwise;
/// for `ALL`, false when some element fails, then `NULL` likewise, true
/// otherwise. The element is compared as it is, never computed into a
/// column: `DataFusion` 54 cannot plan the mark join it makes of a quantified
/// comparison over a computed column (`ProjectionPushdown`: "Input field name
/// … does not match with the projection expression").
fn quantified(comparison: SetComparison, scope: &Scope<'_>) -> Result<Expr> {
    let SetComparison {
        expr: value,
        subquery,
        op,
        quantifier,
    } = comparison;
    let computable = !value.exists(|e| {
        Ok(matches!(
            e,
            Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::ScalarSubquery(_)
                | Expr::InSubquery(_)
                | Expr::Exists(_)
                | Expr::SetComparison(_)
        ))
    })?;
    if !computable || !(is_equality(op) || is_ordering(op)) {
        return Err(refusal(
            "a quantified comparison (ANY, ALL) of this char(n) value",
        ));
    }
    // The value as the subquery sees it: its columns are outer references.
    let mut outer_refs = subquery.outer_ref_columns.clone();
    let seen = value
        .as_ref()
        .clone()
        .transform_up(|e| match e {
            Expr::Column(column) => {
                let field = Arc::clone(scope.schema.field_from_column(&column)?);
                let reference = Expr::OuterReferenceColumn(field, column);
                if !outer_refs.contains(&reference) {
                    outer_refs.push(reference.clone());
                }
                Ok(Transformed::yes(reference))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data;
    let (qualifier, field) = subquery.subquery.schema().qualified_field(0);
    let element = Expr::Column(Column::from((qualifier, field)));
    let holds = if is_equality(op) {
        binary(key(seen), op, key(element.clone()))
    } else {
        let zero = Expr::Literal(ScalarValue::Int32(Some(0)), None);
        binary(compare_call(seen, element.clone()), op, zero)
    };
    let exists = |condition: Option<Expr>, references: Vec<Expr>| -> Result<Expr> {
        let plan = match condition {
            Some(condition) => {
                LogicalPlan::Filter(Filter::try_new(condition, Arc::clone(&subquery.subquery))?)
            }
            None => subquery.subquery.as_ref().clone(),
        };
        Ok(Expr::Exists(Exists::new(
            Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns: references,
                spans: subquery.spans.clone(),
            },
            false,
        )))
    };
    let (decisive, answer) = match quantifier {
        SetQuantifier::Any => (holds, true),
        SetQuantifier::All => (Expr::Not(Box::new(holds)), false),
    };
    let decided = exists(Some(decisive), outer_refs)?;
    // NULL: the value, or an element, is NULL (the comparison is NULL
    // exactly then).
    let uncorrelated = subquery.outer_ref_columns.clone();
    let unknown = value
        .as_ref()
        .clone()
        .is_null()
        .and(exists(None, uncorrelated.clone())?)
        .or(exists(Some(element.is_null()), uncorrelated)?);
    let boolean = |b: Option<bool>| Expr::Literal(ScalarValue::Boolean(b), None);
    Ok(Expr::Case(Case::new(
        None,
        vec![
            (Box::new(decided), Box::new(boolean(Some(answer)))),
            (Box::new(unknown), Box::new(boolean(None))),
        ],
        Some(Box::new(boolean(Some(!answer)))),
    )))
}

/// `CASE`: a simple `CASE x WHEN a …` compares `x` with each `a` (made a
/// searched `CASE` when char semantics apply); the results share a type.
fn case(case: Case, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    let Case {
        expr: operand,
        when_then_expr,
        else_expr,
    } = case;
    let mut changed = false;
    let mut whens: Vec<(Box<Expr>, Box<Expr>)> = when_then_expr;
    let mut operand = operand;
    let compared = operand.as_ref().is_some_and(|base| {
        let mut types = vec![infer(base, scope)];
        types.extend(whens.iter().map(|(w, _)| infer(w, scope)));
        needs_char_coercion(&types)
    });
    if compared && let Some(base) = operand.take() {
        whens = whens
            .into_iter()
            .map(|(w, t)| {
                Ok((
                    Box::new(compare(base.as_ref().clone(), Operator::Eq, *w, scope)?.data),
                    t,
                ))
            })
            .collect::<Result<_>>()?;
        changed = true;
    }
    let has_else = else_expr.is_some();
    let (conditions, mut branches): (Vec<Box<Expr>>, Vec<Expr>) =
        whens.into_iter().map(|(w, t)| (w, *t)).unzip();
    branches.extend(else_expr.map(|e| *e));
    let unified = unify_values(branches, scope, "CASE")?;
    changed |= unified.transformed;
    let mut branches = unified.data;
    let else_expr = if has_else {
        branches.pop().map(Box::new)
    } else {
        None
    };
    let when_then = conditions
        .into_iter()
        .zip(branches)
        .map(|(w, t)| (w, Box::new(t)))
        .collect();
    Ok(Transformed::new_transformed(
        Expr::Case(Case::new(operand, when_then, else_expr)),
        changed,
    ))
}

fn scalar_function(function: ScalarFunction, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    let ScalarFunction { func, args } = function;
    let name = func.name().to_owned();
    match name.as_str() {
        "coalesce" | "nvl" | "ifnull" | "greatest" | "least" => {
            Ok(unify_values(args, scope, &name)?.update_data(|args| func.call(args)))
        }
        "nvl2" if args.len() == 3 => {
            let mut args = args;
            let rest = args.split_off(1);
            let unified = unify_values(rest, scope, &name)?;
            Ok(unified.update_data(|rest| {
                args.extend(rest);
                func.call(args)
            }))
        }
        "nullif" if args.len() == 2 => {
            let types = [infer(&args[0], scope), infer(&args[1], scope)];
            if !needs_char_coercion(&types) || types.contains(&Some(Text::Null)) {
                return Ok(Transformed::no(func.call(args)));
            }
            let mut args = args;
            let b = args.pop().unwrap_or_default();
            let a = args.pop().unwrap_or_default();
            if let [Some(Text::Char(Some(n))), _] = types
                && let Some(fitted) = fitted_literal(&b, n, true)
            {
                return Ok(Transformed::yes(func.call(vec![a, fitted])));
            }
            // Compared in the common type, answered as `a`.
            let equal = compare(a.clone(), Operator::Eq, b, scope)?.data;
            let null = Expr::Literal(ScalarValue::try_from(&a.get_type(scope.schema.as_ref())?)?, None);
            Ok(Transformed::yes(Expr::Case(Case::new(
                None,
                vec![(Box::new(equal), Box::new(null))],
                Some(Box::new(a)),
            ))))
        }
        // The trims of a char value work on it without its padding
        // (`ltrim(CAST(' ab' AS char(5)))` is `'ab'`, `ltrim(ch, 'a')` of
        // `'ab'` is `'b'`).
        "ltrim" | "rtrim" | "trim" | "btrim"
            if args
                .first()
                .and_then(|a| infer(a, scope))
                .is_some_and(Text::is_char) =>
        {
            let mut args = args;
            let value = args.remove(0);
            args.insert(0, key(value));
            Ok(Transformed::yes(func.call(args)))
        }
        "concat" | CONCAT_OPERATOR => Ok(concat_args(args, scope)?.update_data(|args| func.call(args))),
        "make_array" | "array" | "make_list" => {
            Ok(unify_values(args, scope, "ARRAY")?.update_data(|args| func.call(args)))
        }
        _ if searches_array(&name) => search_array(&func, args, scope),
        // `typeof` reads the type from its argument's field.
        "typeof" if args.len() == 1 => {
            let recorded = args[0]
                .to_field(scope.schema.as_ref())
                .ok()
                .and_then(|(_, field)| declared(&field));
            match infer(&args[0], scope) {
                Some(Text::Char(Some(n))) if recorded != Some(Declared::Char(n)) => {
                    let mut args = args;
                    let value = args.pop().unwrap_or_default();
                    Ok(Transformed::yes(func.call(vec![pad(value, n)])))
                }
                Some(Text::Varchar(_)) if matches!(recorded, Some(Declared::Char(_))) => {
                    let mut args = args;
                    let value = args.pop().unwrap_or_default();
                    Ok(Transformed::yes(func.call(vec![untyped(value)])))
                }
                _ => Ok(Transformed::no(func.call(args))),
            }
        }
        _ => Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction {
            func,
            args,
        }))),
    }
}

/// `lag`/`lead` with a default: the value and the default share a type.
fn window_function(expr: Expr, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    let Expr::WindowFunction(mut window) = expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(window.fun.name(), "lag" | "lead") || window.params.args.len() != 3 {
        return Ok(Transformed::no(Expr::WindowFunction(window)));
    }
    let mut args = std::mem::take(&mut window.params.args);
    let default = args.pop().unwrap_or_default();
    let value = args.remove(0);
    let unified = unify_values(vec![value, default], scope, window.fun.name())?;
    let changed = unified.transformed;
    let mut pair = unified.data;
    let default = pair.pop().unwrap_or_default();
    let value = pair.pop().unwrap_or_default();
    args.insert(0, value);
    args.push(default);
    window.params.args = args;
    Ok(Transformed::new_transformed(
        Expr::WindowFunction(window),
        changed,
    ))
}

/// One expression node (its children done already).
fn rewrite_one(expr: Expr, scope: &Scope<'_>) -> Result<Transformed<Expr>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_equality(op) || is_ordering(op) => {
            compare(*left, op, *right, scope)
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::StringConcat,
            right,
        }) => Ok(concat_args(vec![*left, *right], scope)?.update_data(|mut pair| {
            let right = pair.pop().unwrap_or_default();
            let left = pair.pop().unwrap_or_default();
            binary(left, Operator::StringConcat, right)
        })),
        Expr::InList(list) => Ok(in_list(list, scope)),
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let types = [infer(&expr, scope), infer(&low, scope), infer(&high, scope)];
            if !needs_char_coercion(&types) {
                return Ok(Transformed::no(Expr::Between(Between::new(
                    expr, negated, low, high,
                ))));
            }
            let above = compare(expr.as_ref().clone(), Operator::GtEq, *low, scope)?.data;
            let below = compare(*expr, Operator::LtEq, *high, scope)?.data;
            let between = above.and(below);
            Ok(Transformed::yes(if negated {
                Expr::Not(Box::new(between))
            } else {
                between
            }))
        }
        e @ (Expr::InSubquery(_) | Expr::SetComparison(_)) => subquery_comparison(e, scope),
        Expr::Case(c) => case(c, scope),
        Expr::ScalarFunction(function) => scalar_function(function, scope),
        e @ Expr::WindowFunction(_) => window_function(e, scope),
        other => Ok(Transformed::no(other)),
    }
}

/// Every expression of `plan`, names kept.
fn rewrite_expressions(plan: LogicalPlan, scope: &Scope<'_>) -> Result<Transformed<LogicalPlan>> {
    let names = NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        Ok(expr
            .transform_up(|e| rewrite_one(e, scope))?
            .update_data(|e| name.restore(e)))
    })
}

/// `expr` without the char type a cast copied into its field from its
/// argument's.
fn untyped(expr: Expr) -> Expr {
    crate::declared::trino_typed().call(vec![
        expr,
        Expr::Literal(ScalarValue::Utf8(Some(String::new())), None),
    ])
}

/// A projection records the reference type of every column it computes:
/// `char(n)` in the field of a char value, none in the field of a text a
/// cast made from one (a cast keeps its argument's field metadata).
fn record_types(projection: Projection, scope: &Scope<'_>) -> Result<Transformed<LogicalPlan>> {
    let schema = projection.input.schema();
    let mut changed = false;
    let mut exprs = Vec::with_capacity(projection.expr.len());
    for expr in projection.expr {
        let (qualifier, field) = expr.to_field(schema.as_ref())?;
        let recorded = declared(&field);
        let wanted = infer(&expr, scope);
        let value = match wanted {
            Some(Text::Char(Some(n))) if recorded != Some(Declared::Char(n)) => {
                Some(pad(expr.clone().unalias(), n))
            }
            Some(Text::Char(_)) | None => None,
            Some(other) if matches!(recorded, Some(Declared::Char(_))) => Some(match other {
                Text::Varchar(Bound::Known(k)) => crate::declared::trino_typed().call(vec![
                    expr.clone().unalias(),
                    Expr::Literal(
                        ScalarValue::Utf8(Some(Declared::Varchar(k).name(&DataType::Utf8))),
                        None,
                    ),
                ]),
                _ => untyped(expr.clone().unalias()),
            }),
            Some(_) => None,
        };
        match value {
            Some(value) => {
                changed = true;
                exprs.push(value.alias_qualified(qualifier, field.name()));
            }
            None => exprs.push(expr),
        }
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Projection(
            Projection::try_new_with_schema(exprs, projection.input, projection.schema)?,
        )));
    }
    Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(
        exprs,
        projection.input,
    )?)))
}

/// `plan` with column `i` padded to `lengths[i]` where given (same
/// qualifier and name).
fn pad_columns(plan: Arc<LogicalPlan>, lengths: &[Option<u32>]) -> Result<Arc<LogicalPlan>> {
    if lengths.iter().all(Option::is_none) {
        return Ok(plan);
    }
    let exprs: Vec<Expr> = plan
        .schema()
        .iter()
        .zip(lengths)
        .map(|((qualifier, field), length)| {
            let column = Expr::Column(Column::from((qualifier, field)));
            match length {
                Some(n) => pad(column, *n).alias_qualified(qualifier.cloned(), field.name()),
                None => column,
            }
        })
        .collect();
    Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
        exprs, plan,
    )?)))
}

/// The length each input's column `j` is padded to (its common char type),
/// `None` for a column left as it is.
fn common_lengths(columns: &[Vec<Option<Text>>], what: &str) -> Result<Vec<Option<u32>>> {
    columns
        .iter()
        .map(|types| {
            if mixes_char_arrays(types) {
                return Err(char_array_refusal(what));
            }
            if !needs_char_coercion(types) {
                return Ok(None);
            }
            match common(types) {
                Some(Text::Char(Some(n))) => Ok(Some(n)),
                _ => Err(refusal(what)),
            }
        })
        .collect()
}

/// A `UNION` of char values: every input's column padded to the common
/// char type.
fn union(union: Union, outer: &[&LogicalPlan]) -> Result<Transformed<LogicalPlan>> {
    let width = union.schema.fields().len();
    let columns: Vec<Vec<Option<Text>>> = (0..width)
        .map(|j| {
            union
                .inputs
                .iter()
                .map(|input| column_type(input, j, outer))
                .collect()
        })
        .collect();
    let lengths = common_lengths(&columns, "UNION")?;
    let chars = char_lengths(&columns, &lengths);
    if lengths.iter().all(Option::is_none) && records(&union.schema, &chars) {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let inputs = union
        .inputs
        .into_iter()
        .enumerate()
        .map(|(i, input)| {
            let own: Vec<Option<u32>> = lengths
                .iter()
                .enumerate()
                .map(|(j, length)| length.filter(|n| columns[j][i] != Some(Text::Char(Some(*n)))))
                .collect();
            pad_columns(input, &own)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs,
        schema: recorded(&union.schema, &chars)?,
    })))
}

/// A `VALUES` list: each column's values in their common type, the column
/// declaring it.
fn values(values: Values) -> Result<Transformed<LogicalPlan>> {
    let empty = Scope::empty();
    let width = values.schema.fields().len();
    let columns: Vec<Vec<Option<Text>>> = (0..width)
        .map(|j| {
            values
                .values
                .iter()
                .map(|row| row.get(j).and_then(|cell| infer(value_of_cell(cell), &empty)))
                .collect()
        })
        .collect();
    let lengths = common_lengths(&columns, "VALUES")?;
    let chars = char_lengths(&columns, &lengths);
    if lengths.iter().all(Option::is_none) && records(&values.schema, &chars) {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let rows = values
        .values
        .into_iter()
        .map(|row| {
            row.into_iter()
                .enumerate()
                .map(|(j, cell)| match lengths[j] {
                    Some(n) => pad_cell(cell, n),
                    None => Ok(cell),
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Values(Values {
        schema: recorded(&values.schema, &chars)?,
        values: rows,
    })))
}

/// A `VALUES` cell padded to `n`, inside its cast and mark, unless it is
/// `NULL` or already a `char(n)`.
fn pad_cell(cell: Expr, length: u32) -> Result<Expr> {
    let empty = Scope::empty();
    match infer(value_of_cell(&cell), &empty) {
        Some(Text::Null) => return Ok(cell),
        Some(Text::Char(Some(n))) if n == length => return Ok(cell),
        _ => {}
    }
    let wraps = |e: &Expr| match e {
        Expr::Cast(_) | Expr::Alias(_) => true,
        Expr::ScalarFunction(function) => function.func.name() == crate::coercion::VALUES_CELL,
        _ => false,
    };
    Ok(cell
        .transform_down(|e| {
            Ok(if wraps(&e) {
                Transformed::no(e)
            } else {
                Transformed::new(
                    pad(e, length),
                    true,
                    datafusion::common::tree_node::TreeNodeRecursion::Jump,
                )
            })
        })?
        .data)
}

/// The char length each column declares: the one it is padded to, or the
/// `char(n)` all its values share.
fn char_lengths(columns: &[Vec<Option<Text>>], lengths: &[Option<u32>]) -> Vec<Option<u32>> {
    columns
        .iter()
        .zip(lengths)
        .map(|(types, length)| {
            length.or_else(|| match common(types) {
                Some(Text::Char(Some(n))) => Some(n),
                _ => None,
            })
        })
        .collect()
}

/// Whether each field of `schema` declares `char(n)` exactly where `chars`
/// says.
fn records(schema: &DFSchema, chars: &[Option<u32>]) -> bool {
    schema
        .fields()
        .iter()
        .zip(chars)
        .all(|(field, length)| match length {
            Some(n) => declared(field) == Some(Declared::Char(*n)),
            None => !matches!(declared(field), Some(Declared::Char(_))),
        })
}

/// `schema` with each field declaring `char(n)` where `chars` says, and no
/// char type anywhere else.
fn recorded(schema: &DFSchema, chars: &[Option<u32>]) -> Result<Arc<DFSchema>> {
    let fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = schema
        .iter()
        .zip(chars)
        .map(|((qualifier, field), length)| {
            let mut metadata = field.metadata().clone();
            match length {
                Some(n) => metadata.extend(Declared::Char(*n).metadata()),
                None if matches!(declared(field), Some(Declared::Char(_))) => {
                    metadata.remove(crate::trino_types::TRINO_TYPE);
                }
                None => {}
            }
            (
                qualifier.cloned(),
                Arc::new(field.as_ref().clone().with_metadata(metadata)),
            )
        })
        .collect();
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )?))
}

/// A join. The keys of `USING`, `INTERSECT` and `EXCEPT` are converted to
/// their common type on both sides, so the output column has it; the keys
/// of `ON` are compared with char semantics.
fn join(join: Join, outer: &[&LogicalPlan]) -> Result<Transformed<LogicalPlan>> {
    let pairs: Vec<Vec<Option<Text>>> = join
        .on
        .iter()
        .map(|(l, r)| {
            vec![
                infer(l, &Scope::over(&join.left, outer)),
                infer(r, &Scope::over(&join.right, outer)),
            ]
        })
        .collect();
    if !pairs.iter().any(|types| needs_char_coercion(types)) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let all_columns = join
        .on
        .iter()
        .all(|(l, r)| matches!((l, r), (Expr::Column(_), Expr::Column(_))));
    let converts_inputs = all_columns
        && (join.join_constraint == JoinConstraint::Using || crate::coercion::is_set_operation(&join));
    if converts_inputs {
        let lengths = common_lengths(&pairs, "a join USING char columns")?;
        let side = |plan: &Arc<LogicalPlan>, at: usize| -> Vec<Option<u32>> {
            let mut out = vec![None; plan.schema().fields().len()];
            for ((pair, length), types) in join.on.iter().zip(&lengths).zip(&pairs) {
                let key = if at == 0 { &pair.0 } else { &pair.1 };
                if let (Expr::Column(column), Some(n)) = (key, length)
                    && types[at] != Some(Text::Char(Some(*n)))
                    && let Ok(index) = plan.schema().index_of_column(column)
                {
                    out[index] = Some(*n);
                }
            }
            out
        };
        let left = pad_columns(Arc::clone(&join.left), &side(&join.left, 0))?;
        let right = pad_columns(Arc::clone(&join.right), &side(&join.right, 1))?;
        return Ok(Transformed::yes(LogicalPlan::Join(Join::try_new(
            left,
            right,
            join.on,
            join.filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
            join.null_aware,
        )?)));
    }
    let on = join
        .on
        .into_iter()
        .zip(&pairs)
        .map(|((l, r), types)| {
            if needs_char_coercion(types) && !types.contains(&Some(Text::Null)) {
                (key(l), key(r))
            } else {
                (l, r)
            }
        })
        .collect();
    Ok(Transformed::yes(LogicalPlan::Join(Join::try_new(
        join.left,
        join.right,
        on,
        join.filter,
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?)))
}

/// One node, its inputs and subqueries done already.
fn rewrite_node(plan: LogicalPlan, outer: &[&LogicalPlan]) -> Result<LogicalPlan> {
    let plan = match plan {
        LogicalPlan::Union(u) => return Ok(union(u, outer)?.data),
        LogicalPlan::Join(j) => join(j, outer)?.data,
        plan @ (LogicalPlan::TableScan(_) | LogicalPlan::Limit(_)) => return Ok(plan),
        other => other,
    };
    // The inputs, owned: the node's expressions change while they are read.
    let inputs: Vec<LogicalPlan> = plan.inputs().into_iter().cloned().collect();
    let own = Arc::clone(plan.schema());
    let scope = Scope::new(inputs.iter().collect(), &own, outer);
    let plan = rewrite_expressions(plan, &scope)?.data;
    match plan {
        LogicalPlan::Values(v) => Ok(values(v)?.data),
        LogicalPlan::Projection(projection) => {
            let projection = match LogicalPlan::Projection(projection).recompute_schema()? {
                LogicalPlan::Projection(p) => p,
                other => return Ok(other),
            };
            Ok(record_types(projection, &scope)?.data)
        }
        other => other.recompute_schema(),
    }
}

fn rewrite_plan(plan: LogicalPlan, outer: &[&LogicalPlan]) -> Result<LogicalPlan> {
    let plan = plan
        .map_children(|child| rewrite_plan(child, outer).map(Transformed::yes))?
        .data;
    let plan = plan.recompute_schema()?;
    let context: Vec<LogicalPlan> = plan.inputs().into_iter().cloned().collect();
    let mut enclosing: Vec<&LogicalPlan> = context.iter().collect();
    enclosing.extend(outer.iter().copied());
    let plan = plan
        .map_subqueries(|subquery| rewrite_plan(subquery, &enclosing).map(Transformed::yes))?
        .data;
    rewrite_node(plan, outer)
}

/// Analyzer rule: see the module documentation.
#[derive(Debug, Default)]
pub struct CharSemantics;

impl AnalyzerRule for CharSemantics {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        rewrite_plan(plan, &[])
    }

    fn name(&self) -> &'static str {
        "codelake_char_semantics"
    }
}

// ---------------------------------------------------------------- NATURAL

/// Whether `join` joins on the columns both sides share.
fn is_natural(join: &sql::Join) -> bool {
    use sql::JoinOperator as J;
    matches!(
        &join.join_operator,
        J::Join(sql::JoinConstraint::Natural)
            | J::Inner(sql::JoinConstraint::Natural)
            | J::Left(sql::JoinConstraint::Natural)
            | J::LeftOuter(sql::JoinConstraint::Natural)
            | J::Right(sql::JoinConstraint::Natural)
            | J::RightOuter(sql::JoinConstraint::Natural)
            | J::FullOuter(sql::JoinConstraint::Natural)
    )
}

fn natural_in_tables(table: &sql::TableWithJoins) -> bool {
    let nested = |factor: &sql::TableFactor| match factor {
        sql::TableFactor::NestedJoin { table_with_joins, .. } => natural_in_tables(table_with_joins),
        _ => false,
    };
    table.joins.iter().any(is_natural)
        || nested(&table.relation)
        || table.joins.iter().any(|j| nested(&j.relation))
}

fn natural_in_body(body: &sql::SetExpr) -> bool {
    match body {
        sql::SetExpr::Select(select) => select.from.iter().any(natural_in_tables),
        sql::SetExpr::SetOperation { left, right, .. } => natural_in_body(left) || natural_in_body(right),
        _ => false,
    }
}

/// Finds a `NATURAL JOIN` in any query of a statement (derived tables and
/// subqueries are queries of their own).
struct NaturalJoins;

impl Visitor for NaturalJoins {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sql::Query) -> ControlFlow<()> {
        if natural_in_body(&query.body) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

/// `NATURAL JOIN` fails, as in the reference engine (`Natural join not
/// supported`).
pub(crate) fn refuse_natural_joins(statement: &DFStatement) -> Result<()> {
    let DFStatement::Statement(inner) = statement else {
        return Ok(());
    };
    if inner.as_ref().visit(&mut NaturalJoins).is_break() {
        return Err(DataFusionError::NotImplemented(
            "Natural join not supported".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

    use crate::bytes::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;

    type Rows = Vec<Vec<String>>;

    /// The tables of every case, in the engine's in-memory catalog (Trino
    /// 431: the same statements in its memory catalog).
    const SETUP: &[&str] = &[
        "CREATE TABLE cht (id bigint, ch char(5))",
        "INSERT INTO cht VALUES (1, 'ab'), (2, 'zz'), (3, NULL), (4, 'a')",
        "CREATE TABLE c3 (id bigint, c char(3))",
        "INSERT INTO c3 VALUES (1, 'ab'), (2, 'a  '), (3, 'zz')",
        "CREATE TABLE vct (id bigint, s varchar)",
        "INSERT INTO vct VALUES (1, 'ab'), (2, 'ab  '), (3, 'cd'), (4, NULL), (5, 'a')",
        "CREATE TABLE v3 (id bigint, s varchar(3))",
        "INSERT INTO v3 VALUES (1, 'ab'), (2, 'a')",
    ];

    async fn engine(dir: &std::path::Path) -> Engine {
        let memory = MemoryConfig {
            limit: ByteSize::mib(64),
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(64),
            metadata_cache_limit: ByteSize::mib(1),
        };
        let engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
        for statement in SETUP {
            rows(&engine, statement)
                .await
                .unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
        engine
    }

    async fn rows(engine: &Engine, sql: &str) -> Result<Rows, String> {
        let frame = engine.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = frame.collect().await.map_err(|e| e.to_string())?;
        let options = FormatOptions::default().with_null("NULL");
        let mut out = Vec::new();
        for batch in &batches {
            let formatters: Vec<ArrayFormatter<'_>> = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                out.push(formatters.iter().map(|f| f.value(row).to_string()).collect());
            }
        }
        Ok(out)
    }

    fn expected(rows: &[&[&str]]) -> Rows {
        rows.iter()
            .map(|r| r.iter().map(|c| (*c).to_owned()).collect())
            .collect()
    }

    /// Each query and the rows Trino 431 answers to it (`typeof` included).
    const CASES: &[(&str, &[&[&str]])] = &[
        // The trims work on the value without its padding.
        (
            "SELECT id, ltrim(ch), length(ltrim(ch)), ltrim(ch) = 'ab' FROM cht ORDER BY id",
            &[
                &["1", "ab", "2", "true"],
                &["2", "zz", "2", "false"],
                &["3", "NULL", "NULL", "NULL"],
                &["4", "a", "1", "false"],
            ],
        ),
        (
            "SELECT ltrim(CAST('  ab' AS char(6))), length(ltrim(CAST('  ab' AS char(6)))), \
             to_hex(to_utf8(ltrim(CAST(' a' || chr(9) AS char(5)))))",
            &[&["ab", "2", "6109"]],
        ),
        (
            "SELECT id, ltrim(ch, 'a'), length(ltrim(ch, 'a')), rtrim(ch, 'b'), length(rtrim(ch, 'b')), \
             trim(ch, 'a'), length(trim(ch, 'a')) FROM cht ORDER BY id",
            &[
                &["1", "b", "1", "a", "1", "b", "1"],
                &["2", "zz", "2", "zz", "2", "zz", "2"],
                &["3", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"],
                &["4", "", "0", "a", "1", "", "0"],
            ],
        ),
        (
            "SELECT id, ch, length(ch), typeof(ch) FROM cht ORDER BY id",
            &[
                &["1", "ab   ", "5", "char(5)"],
                &["2", "zz   ", "5", "char(5)"],
                &["3", "NULL", "NULL", "char(5)"],
                &["4", "a    ", "5", "char(5)"],
            ],
        ),
        ("SELECT id FROM cht WHERE ch = 'ab' ORDER BY id", &[&["1"]]),
        ("SELECT id FROM cht WHERE ch = 'ab   ' ORDER BY id", &[&["1"]]),
        ("SELECT id FROM cht WHERE ch = 'abcdefg' ORDER BY id", &[]),
        (
            "SELECT id FROM cht WHERE ch <> 'ab' ORDER BY id",
            &[&["2"], &["4"]],
        ),
        (
            "SELECT id FROM cht WHERE ch < 'ab' || chr(9) ORDER BY id",
            &[&["4"]],
        ),
        (
            "SELECT id FROM cht WHERE ch > 'ab' || chr(9) ORDER BY id",
            &[&["1"], &["2"]],
        ),
        (
            "SELECT id FROM cht WHERE ch >= 'ab' ORDER BY id",
            &[&["1"], &["2"]],
        ),
        ("SELECT id FROM cht WHERE ch <= 'a' ORDER BY id", &[&["4"]]),
        (
            "SELECT id FROM cht WHERE ch IN ('ab', 'zz ') ORDER BY id",
            &[&["1"], &["2"]],
        ),
        (
            "SELECT id FROM cht WHERE ch NOT IN ('ab', 'abcdefgh') ORDER BY id",
            &[&["2"], &["4"]],
        ),
        (
            "SELECT id FROM cht WHERE ch BETWEEN 'a' AND 'ab' ORDER BY id",
            &[&["1"], &["4"]],
        ),
        (
            "SELECT id FROM cht WHERE ch NOT BETWEEN 'a' AND 'ab' ORDER BY id",
            &[&["2"]],
        ),
        (
            "SELECT id FROM cht WHERE ch IS DISTINCT FROM 'ab' ORDER BY id",
            &[&["2"], &["3"], &["4"]],
        ),
        ("SELECT id FROM cht WHERE ch LIKE 'ab' ORDER BY id", &[]),
        ("SELECT id FROM cht WHERE ch LIKE 'ab%' ORDER BY id", &[&["1"]]),
        (
            "SELECT cht.id, vct.id FROM cht JOIN vct ON cht.ch = vct.s ORDER BY 1, 2",
            &[&["1", "1"], &["1", "2"], &["4", "5"]],
        ),
        (
            "SELECT cht.id, vct.id FROM cht JOIN vct ON cht.ch < vct.s ORDER BY 1, 2",
            &[&["1", "3"], &["4", "1"], &["4", "2"], &["4", "3"]],
        ),
        (
            "SELECT cht.id, c3.id FROM cht JOIN c3 ON cht.ch = c3.c ORDER BY 1, 2",
            &[&["1", "1"], &["2", "3"], &["4", "2"]],
        ),
        (
            "SELECT cht.id, v3.id FROM cht JOIN v3 ON cht.ch = v3.s ORDER BY 1, 2",
            &[&["1", "1"], &["4", "2"]],
        ),
        (
            "SELECT a.id, b.id FROM cht a JOIN cht b ON a.ch = b.ch ORDER BY 1, 2",
            &[&["1", "1"], &["2", "2"], &["4", "4"]],
        ),
        (
            "SELECT count(*), max(length(ch)) FROM cht JOIN (SELECT s AS ch FROM vct) w USING (ch)",
            &[&["3", "65536"]],
        ),
        (
            "SELECT typeof(ch), length(ch), CAST(ch AS varchar) FROM cht JOIN (SELECT c AS ch FROM c3) w USING (ch) ORDER BY 3",
            &[
                &["char(5)", "5", "a    "],
                &["char(5)", "5", "ab   "],
                &["char(5)", "5", "zz   "],
            ],
        ),
        (
            "SELECT id FROM vct WHERE s IN (SELECT ch FROM cht) ORDER BY id",
            &[&["1"], &["2"], &["5"]],
        ),
        (
            "SELECT id FROM vct WHERE s NOT IN (SELECT ch FROM cht WHERE ch IS NOT NULL) ORDER BY id",
            &[&["3"]],
        ),
        (
            "SELECT id FROM vct WHERE s = ANY (SELECT ch FROM cht) ORDER BY id",
            &[&["1"], &["2"], &["5"]],
        ),
        (
            "SELECT id FROM cht WHERE ch IN (SELECT s FROM vct) ORDER BY id",
            &[&["1"], &["4"]],
        ),
        (
            "SELECT id FROM cht WHERE ch IN (SELECT c FROM c3) ORDER BY id",
            &[&["1"], &["2"], &["4"]],
        ),
        (
            "SELECT id FROM vct WHERE s IN (SELECT max(ch) FROM cht GROUP BY id) ORDER BY id",
            &[&["1"], &["2"], &["5"]],
        ),
        (
            "SELECT id FROM vct WHERE s = (SELECT max(ch) FROM cht WHERE id = 1) ORDER BY id",
            &[&["1"], &["2"]],
        ),
        (
            "SELECT (SELECT max(ch) FROM cht WHERE id = 1) = 'ab'",
            &[&["true"]],
        ),
        (
            "SELECT typeof((SELECT max(ch) FROM cht)), (SELECT min(ch) FROM cht)",
            &[&["char(5)", "a    "]],
        ),
        (
            "SELECT count(*) FROM (SELECT (SELECT CAST('ab' AS char(5))) UNION SELECT 'ab ')",
            &[&["1"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE 'ab' END, typeof(CASE WHEN id = 1 THEN ch ELSE 'ab' END) FROM cht ORDER BY id",
            &[
                &["1", "ab   ", "char(5)"],
                &["2", "ab   ", "char(5)"],
                &["3", "ab   ", "char(5)"],
                &["4", "ab   ", "char(5)"],
            ],
        ),
        (
            "SELECT count(DISTINCT CASE WHEN id = 1 THEN ch ELSE 'ab' END) FROM cht",
            &[&["1"]],
        ),
        (
            "SELECT count(DISTINCT coalesce(CASE WHEN id = 2 THEN NULL ELSE ch END, 'ab')) FROM cht",
            &[&["2"]],
        ),
        (
            "SELECT id, coalesce(ch, 'x'), typeof(coalesce(ch, 'x')) FROM cht ORDER BY id",
            &[
                &["1", "ab   ", "char(5)"],
                &["2", "zz   ", "char(5)"],
                &["3", "x    ", "char(5)"],
                &["4", "a    ", "char(5)"],
            ],
        ),
        (
            "SELECT id, CASE ch WHEN 'ab' THEN 1 WHEN 'zz ' THEN 2 ELSE 0 END FROM cht ORDER BY id",
            &[&["1", "1"], &["2", "2"], &["3", "0"], &["4", "0"]],
        ),
        (
            "SELECT id, nullif(ch, 'ab'), greatest(ch, 'b'), least(ch, 'b') FROM cht ORDER BY id",
            &[
                &["1", "NULL", "b    ", "ab   "],
                &["2", "zz   ", "zz   ", "b    "],
                &["3", "NULL", "NULL", "NULL"],
                &["4", "a    ", "b    ", "a    "],
            ],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE c END FROM cht JOIN c3 USING (id) ORDER BY id",
            &[&["1", "ab   "], &["2", "a    "], &["3", "zz   "]],
        ),
        (
            "SELECT id, if(id = 1, ch, 'abcdefg') FROM cht ORDER BY id",
            &[
                &["1", "ab     "],
                &["2", "abcdefg"],
                &["3", "abcdefg"],
                &["4", "abcdefg"],
            ],
        ),
        (
            "SELECT id, ch || 'x', typeof(ch || 'x'), ch || c FROM cht JOIN c3 USING (id) ORDER BY id",
            &[
                &["1", "ab   x", "char(6)", "ab   ab "],
                &["2", "zz   x", "char(6)", "zz   a  "],
                &["3", "NULL", "char(6)", "NULL"],
            ],
        ),
        (
            "SELECT id, concat(ch, s) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[&["1", "ab   ab "], &["2", "zz   a  "]],
        ),
        (
            "SELECT id, upper(ch), typeof(upper(ch)), rtrim(ch), substr(ch, 2) FROM cht ORDER BY id",
            &[
                &["1", "AB   ", "char(5)", "ab", "b   "],
                &["2", "ZZ   ", "char(5)", "zz", "z   "],
                &["3", "NULL", "char(5)", "NULL", "NULL"],
                &["4", "A    ", "char(5)", "a", "    "],
            ],
        ),
        (
            "SELECT CAST(CAST('ab' AS char(5)) AS varchar) = 'ab   '",
            &[&["true"]],
        ),
        (
            "SELECT CAST('ab' AS char(5)) = 'ab ', 'ab ' = CAST('ab' AS char(5)), CAST('ab' AS char(3)) = CAST('ab' AS char(5))",
            &[&["true", "true", "true"]],
        ),
        (
            "SELECT TRY_CAST('ab' AS char(5)) = 'ab ', TRY_CAST('abcdef' AS varchar(3)), TRY_CAST('abcdef' AS char(3))",
            &[&["true", "abc", "abc"]],
        ),
        (
            "SELECT count(*) FROM (SELECT 'ab ' UNION SELECT CAST('ab' AS char(5)))",
            &[&["1"]],
        ),
        (
            "SELECT x, typeof(x) FROM (SELECT ch AS x FROM cht UNION SELECT c FROM c3) ORDER BY 1",
            &[
                &["a    ", "char(5)"],
                &["ab   ", "char(5)"],
                &["zz   ", "char(5)"],
                &["NULL", "char(5)"],
            ],
        ),
        (
            "SELECT count(*) FROM (SELECT ch FROM cht UNION SELECT s FROM v3)",
            &[&["4"]],
        ),
        (
            "SELECT count(*) FROM (SELECT ch FROM cht UNION SELECT s FROM vct)",
            &[&["5"]],
        ),
        (
            "SELECT count(DISTINCT x) FROM (SELECT ch AS x FROM cht UNION ALL SELECT CAST('ab' AS char(5)))",
            &[&["3"]],
        ),
        (
            "SELECT x FROM (SELECT ch AS x FROM cht INTERSECT SELECT c FROM c3) ORDER BY 1",
            &[&["a    "], &["ab   "], &["zz   "]],
        ),
        (
            "SELECT count(*) FROM (SELECT ch FROM cht INTERSECT SELECT s FROM vct)",
            &[&["3"]],
        ),
        (
            "SELECT count(*) FROM (SELECT ch FROM cht EXCEPT SELECT s FROM vct)",
            &[&["1"]],
        ),
        (
            "SELECT x FROM (SELECT ch AS x FROM cht EXCEPT SELECT c FROM c3) ORDER BY 1",
            &[&["NULL"]],
        ),
        (
            "SELECT count(DISTINCT x) FROM (VALUES ('ab '), (CAST('ab' AS char(5)))) AS t(x)",
            &[&["1"]],
        ),
        (
            "SELECT x, typeof(x) FROM (VALUES (CAST('ab' AS char(3))), (CAST('a' AS char(5))), (NULL)) AS t(x) ORDER BY 1",
            &[&["a    ", "char(5)"], &["ab   ", "char(5)"], &["NULL", "char(5)"]],
        ),
        (
            "SELECT id, max(ch), min(ch) FROM cht GROUP BY id ORDER BY id",
            &[
                &["1", "ab   ", "ab   "],
                &["2", "zz   ", "zz   "],
                &["3", "NULL", "NULL"],
                &["4", "a    ", "a    "],
            ],
        ),
        (
            "SELECT ch, count(*) FROM (SELECT ch FROM cht UNION ALL SELECT ch FROM cht) GROUP BY ch ORDER BY 1",
            &[&["a    ", "2"], &["ab   ", "2"], &["zz   ", "2"], &["NULL", "2"]],
        ),
        (
            "SELECT id FROM (SELECT id, max(ch) AS m FROM cht GROUP BY id) WHERE m = 'ab' ORDER BY 1",
            &[&["1"]],
        ),
        (
            "SELECT id FROM (SELECT id, lag(ch) OVER (ORDER BY id) AS p FROM cht) WHERE p = 'ab' ORDER BY 1",
            &[&["2"]],
        ),
        ("SELECT id FROM cht WHERE upper(ch) = 'AB ' ORDER BY 1", &[&["1"]]),
        (
            "SELECT id FROM cht WHERE (ch, id) = ('ab', 1) ORDER BY 1",
            &[&["1"]],
        ),
        (
            "SELECT id FROM vct WHERE EXISTS (SELECT 1 FROM cht WHERE cht.ch = vct.s) ORDER BY 1",
            &[&["1"], &["2"], &["5"]],
        ),
        (
            "SELECT id, ch FROM cht ORDER BY ch, id",
            &[&["4", "a    "], &["1", "ab   "], &["2", "zz   "], &["3", "NULL"]],
        ),
        (
            "SELECT id FROM vct WHERE s < ALL (SELECT ch FROM cht) ORDER BY id",
            &[],
        ),
        (
            "SELECT id FROM vct WHERE s > ANY (SELECT ch FROM cht) ORDER BY id",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "SELECT id FROM cht WHERE ch <= ALL (SELECT c FROM c3) ORDER BY id",
            &[&["4"]],
        ),
        (
            "SELECT id FROM vct WHERE NOT (s = ANY (SELECT ch FROM cht)) ORDER BY id",
            &[],
        ),
        (
            "SELECT id FROM vct WHERE s <> ALL (SELECT ch FROM cht) ORDER BY id",
            &[],
        ),
        (
            "SELECT id FROM vct WHERE s < ALL (SELECT ch FROM cht WHERE ch IS NOT NULL) ORDER BY id",
            &[],
        ),
        (
            "SELECT id FROM vct WHERE id = 3 OR s IN (SELECT ch FROM cht) ORDER BY id",
            &[&["1"], &["2"], &["3"], &["5"]],
        ),
        (
            "SELECT id FROM cht WHERE ch <> ALL (SELECT s FROM vct WHERE s IS NOT NULL) ORDER BY id",
            &[&["2"]],
        ),
        (
            "SELECT id FROM cht WHERE NOT (ch > ANY (SELECT s FROM v3)) ORDER BY id",
            &[&["4"]],
        ),
        (
            "SELECT id FROM cht WHERE ch = ALL (SELECT s FROM vct WHERE id IN (1, 2)) ORDER BY id",
            &[&["1"]],
        ),
        (
            "SELECT id FROM cht WHERE ARRAY[ch] = ARRAY['ab'] ORDER BY id",
            &[&["1"]],
        ),
        (
            "SELECT id FROM cht WHERE contains(ARRAY['ab', 'x'], ch) ORDER BY id",
            &[&["1"]],
        ),
        (
            "SELECT id, array_position(ARRAY['zz', 'ab'], ch) FROM cht ORDER BY id",
            &[&["1", "2"], &["2", "1"], &["3", "NULL"], &["4", "0"]],
        ),
        (
            "SELECT id FROM cht WHERE ROW(ch, id) = ROW('ab', 1) ORDER BY id",
            &[&["1"]],
        ),
        (
            "SELECT id, cardinality(array_distinct(ARRAY[ch, 'ab'])) FROM cht ORDER BY id",
            &[&["1", "1"], &["2", "2"], &["3", "2"], &["4", "2"]],
        ),
        (
            "SELECT id FROM cht WHERE ch = CAST('ab' AS char(3)) ORDER BY id",
            &[&["1"]],
        ),
        (
            "SELECT id FROM c3 WHERE c = (SELECT ch FROM cht WHERE id = 4) ORDER BY id",
            &[&["2"]],
        ),
        (
            "SELECT id FROM c3 WHERE c IN (SELECT ch FROM cht) ORDER BY id",
            &[&["1"], &["2"], &["3"]],
        ),
        (
            "SELECT id, lag(ch, 1, 'x') OVER (ORDER BY id) FROM cht ORDER BY id",
            &[&["1", "x    "], &["2", "ab   "], &["3", "zz   "], &["4", "NULL"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch END FROM cht ORDER BY id",
            &[&["1", "ab   "], &["2", "NULL"], &["3", "NULL"], &["4", "NULL"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE replace(s, 'a', 'bc') END, length(CASE WHEN id = 1 THEN ch ELSE replace(s, 'a', 'bc') END), typeof(CASE WHEN id = 1 THEN ch ELSE replace(s, 'a', 'bc') END) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[
                &["1", "ab         ", "11", "char(11)"],
                &["2", "bc         ", "11", "char(11)"],
            ],
        ),
        (
            "SELECT id, length(CASE WHEN id = 1 THEN ch ELSE regexp_replace(s, 'a', 'bc') END), typeof(CASE WHEN id = 1 THEN ch ELSE regexp_replace(s, 'a', 'bc') END) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[&["1", "15", "char(15)"], &["2", "15", "char(15)"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE soundex(s) END, typeof(CASE WHEN id = 1 THEN ch ELSE soundex(s) END), length(coalesce(ch, url_encode(s))), typeof(coalesce(ch, url_encode(s))) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[
                &["1", "ab   ", "char(5)", "36", "char(36)"],
                &["2", "A000 ", "char(5)", "36", "char(36)"],
            ],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE element_at(split(s, ','), 1) END, typeof(CASE WHEN id = 1 THEN ch ELSE element_at(split(s, ','), 1) END) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[&["1", "ab   ", "char(5)"], &["2", "a    ", "char(5)"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE split(s, ',')[1] END, typeof(CASE WHEN id = 1 THEN ch ELSE split(s, ',')[1] END) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[&["1", "ab   ", "char(5)"], &["2", "a    ", "char(5)"]],
        ),
        (
            "SELECT id, length(coalesce(ch, regexp_extract_all(s, '.')[1])), typeof(coalesce(ch, regexp_extract_all(s, '.')[1])) FROM cht JOIN v3 USING (id) ORDER BY id",
            &[&["1", "5", "char(5)"], &["2", "5", "char(5)"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE to_iso8601(DATE '2020-01-01') END, typeof(CASE WHEN id = 1 THEN ch ELSE to_iso8601(DATE '2020-01-01') END) FROM cht ORDER BY id",
            &[
                &["1", "ab              ", "char(16)"],
                &["2", "2020-01-01      ", "char(16)"],
                &["3", "2020-01-01      ", "char(16)"],
                &["4", "2020-01-01      ", "char(16)"],
            ],
        ),
        (
            "SELECT id, length(CASE WHEN id = 1 THEN ch ELSE human_readable_seconds(id) END), typeof(CASE WHEN id = 1 THEN ch ELSE human_readable_seconds(id) END) FROM cht ORDER BY id",
            &[
                &["1", "65536", "char(65536)"],
                &["2", "65536", "char(65536)"],
                &["3", "65536", "char(65536)"],
                &["4", "65536", "char(65536)"],
            ],
        ),
        (
            "SELECT id, length(coalesce(ch, replace(s, 'a', 'bc'))), typeof(coalesce(ch, replace(s, 'a', 'bc'))) FROM cht JOIN vct USING (id) ORDER BY id",
            &[
                &["1", "65536", "char(65536)"],
                &["2", "65536", "char(65536)"],
                &["3", "65536", "char(65536)"],
                &["4", "65536", "char(65536)"],
            ],
        ),
        (
            "SELECT id FROM cht JOIN v3 USING (id) WHERE ch = replace(s, 'b', 'b  ') ORDER BY id",
            &[&["1"]],
        ),
        (
            "SELECT v, length(v) FROM (SELECT ch AS v FROM cht UNION SELECT replace(s, 'a', 'x') FROM v3) ORDER BY 1",
            &[
                &["a      ", "7"],
                &["ab     ", "7"],
                &["x      ", "7"],
                &["xb     ", "7"],
                &["zz     ", "7"],
                &["NULL", "NULL"],
            ],
        ),
        (
            "SELECT id, coalesce(ch, regexp_replace(s, 'b', 'yy')), length(coalesce(ch, regexp_replace(s, 'b', 'yy'))) FROM (SELECT id, ch, 'abc' AS s FROM cht) ORDER BY id",
            &[
                &["1", "ab             ", "15"],
                &["2", "zz             ", "15"],
                &["3", "ayyc           ", "15"],
                &["4", "a              ", "15"],
            ],
        ),
        (
            "SELECT count(*) FROM (SELECT ch FROM cht UNION SELECT url_decode(s) FROM v3)",
            &[&["4"]],
        ),
        (
            "SELECT id, CASE WHEN id = 1 THEN ch ELSE replace(NULL, 'a', 'bc') END, typeof(CASE WHEN id = 1 THEN ch ELSE replace(NULL, 'a', 'bc') END) FROM cht ORDER BY id",
            &[
                &["1", "ab   ", "char(5)"],
                &["2", "NULL", "char(5)"],
                &["3", "NULL", "char(5)"],
                &["4", "NULL", "char(5)"],
            ],
        ),
    ];

    #[tokio::test]
    async fn char_values_answer_like_trino() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path()).await;
        let mut wrong = Vec::new();
        for (sql, answer) in CASES {
            let got = rows(&engine, sql).await;
            if got.as_ref() != Ok(&expected(answer)) {
                wrong.push(format!("{sql}: got {got:?}, Trino 431 {answer:?}"));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// Where Trino 431 fails or answers wrong rows, the engine answers what
    /// SQL means: `EXISTS` never repeats a row (Trino joins on the distinct
    /// varchar values before converting them to char, and repeats `1`), and a
    /// scalar subquery in `VALUES` is a value (Trino: "Unexpected subquery
    /// expression in logical plan").
    #[tokio::test]
    async fn char_values_where_the_reference_fails() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path()).await;
        for (sql, answer) in [
            (
                "SELECT id FROM cht WHERE EXISTS (SELECT 1 FROM vct WHERE vct.s = cht.ch) ORDER BY 1",
                &[&["1"][..], &["4"]][..],
            ),
            (
                "SELECT count(*) FROM (VALUES ((SELECT CAST('ab' AS char(5)))), ('ab ')) AS t(x) GROUP BY x",
                &[&["2"]],
            ),
        ] {
            assert_eq!(rows(&engine, sql).await, Ok(expected(answer)), "{sql}");
        }
    }

    /// Char values stored by the engine itself: an in-memory table, `UPDATE`,
    /// `CREATE TABLE … AS`, a view.
    #[tokio::test]
    async fn char_values_the_engine_stores_are_padded() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path()).await;
        for statement in [
            "UPDATE cht SET ch = 'b' WHERE id = 4",
            "CREATE TABLE made AS SELECT id, ch, CAST('x' AS char(2)) AS two FROM cht",
            "CREATE VIEW chv AS SELECT id, ch FROM cht",
        ] {
            rows(&engine, statement)
                .await
                .unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
        for (sql, answer) in [
            (
                "SELECT id, max(ch) FROM cht GROUP BY id ORDER BY id",
                &[
                    &["1", "ab   "][..],
                    &["2", "zz   "],
                    &["3", "NULL"],
                    &["4", "b    "],
                ][..],
            ),
            ("SELECT CAST(ch AS varchar) FROM cht WHERE id = 4", &[&["b    "]]),
            (
                "SELECT id, typeof(two), two = 'x', typeof(ch) FROM made WHERE ch = 'ab' ORDER BY id",
                &[&["1", "char(2)", "true", "char(5)"]],
            ),
            (
                "SELECT id FROM chv WHERE ch = 'zz' OR ch = 'b ' ORDER BY id",
                &[&["2"], &["4"]],
            ),
        ] {
            assert_eq!(rows(&engine, sql).await, Ok(expected(answer)), "{sql}");
        }
    }

    /// Refused: what Trino 431 refuses, and a common type whose length the
    /// engine cannot tell.
    #[tokio::test]
    async fn refusals() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path()).await;
        for (sql, message) in [
            (
                "SELECT count(*) FROM cht NATURAL JOIN c3",
                "Natural join not supported",
            ),
            (
                "SELECT count(*) FROM cht a NATURAL LEFT JOIN (SELECT * FROM cht) b",
                "Natural join not supported",
            ),
            (
                "SELECT ch || s FROM cht JOIN vct USING (id)",
                "concatenating a char(n) value with text of unbounded or unknown length",
            ),
            (
                "SELECT CASE WHEN id = 1 THEN ch ELSE element_at(x.a, 1) END FROM cht CROSS JOIN (SELECT split('a,b', ',') AS a) x",
                "the length of the char(n) type the values share cannot be determined",
            ),
            (
                "SELECT cht.id FROM cht CROSS JOIN (SELECT array_agg(s) AS a FROM vct) x WHERE contains(x.a, ch)",
                "comparing a char(n) value with the elements of an array that is not written out",
            ),
            (
                "SELECT cht.id FROM cht CROSS JOIN (SELECT array_agg(ch) AS a FROM cht) x WHERE contains(x.a, 'ab')",
                "comparing a char(n) value with the elements of an array that is not written out",
            ),
            (
                "SELECT id FROM cht CROSS JOIN (SELECT array_agg(s) AS a FROM vct) x WHERE ARRAY[ch] = x.a",
                "a comparison of an array of char(n) values with another array is not supported yet",
            ),
            (
                "SELECT count(*) FROM (SELECT ARRAY[ch] FROM cht UNION SELECT ARRAY['ab'])",
                "UNION of an array of char(n) values with another array is not supported yet",
            ),
        ] {
            let err = rows(&engine, sql).await.unwrap_err();
            assert!(err.contains(message), "{sql}: {err}");
        }
    }

    #[test]
    fn padded_comparison() {
        assert_eq!(compare_padded("ab", "ab   "), Ordering::Equal);
        assert_eq!(compare_padded("ab\t", "ab"), Ordering::Less);
        assert_eq!(compare_padded("ab!", "ab"), Ordering::Greater);
        assert_eq!(compare_padded("", "   "), Ordering::Equal);
        assert_eq!(compare_padded("a", "b"), Ordering::Less);
        assert_eq!(padded("ç", 3), "ç  ");
        assert_eq!(padded("abcd", 3), "abcd");
    }
}
