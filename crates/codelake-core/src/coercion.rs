//! Trino's common types of numbers.
//!
//! Where values of different types meet — the rows of `VALUES`, the inputs
//! of `UNION`, the branches of `CASE`, the arguments of `coalesce`,
//! `nullif`, `greatest` and `least`, the elements of an array, an `IN` list,
//! `BETWEEN` and a comparison — Trino 431 converts every value to one common
//! type. For numbers:
//!
//! - `tinyint < smallint < integer < bigint`: the wider integer;
//! - a decimal with an integer: a decimal, the integer counting as
//!   `decimal(3,0)`, `decimal(5,0)`, `decimal(10,0)` or `decimal(19,0)`;
//! - two decimals: scale `max(s1,s2)`, precision
//!   `max(p1-s1, p2-s2) + max(s1,s2)` (at most 38);
//! - `real` or `double` with any other number: the floating point type
//!   (`double` when there is a `double`).
//!
//! `DataFusion` prefers the decimal instead (`VALUES (1E-20), (1.5)` became
//! `decimal(30,15)`, the first value `0`, and `1E300` failed) and counts a
//! `bigint` as `decimal(20,0)`. [`TrinoCommonTypes`] (analyzer, right before
//! type coercion) casts each value to Trino's common type, so `DataFusion`'s
//! coercion finds nothing left to do.
//!
//! Integer literals take their type (`integer` when the value fits 32 bits)
//! from the function library's literal rewrite (`crate::trino_functions`).
//!
//! `DataFusion` settles the type of a `VALUES` column when it plans the SQL,
//! before any analyzer rule. [`mark_values`] wraps every value of a `VALUES`
//! list in `codelake_values_cell(…)` (an identity), so the rule can tell the
//! values as written from the casts `DataFusion` added around them, and
//! types the column again. The `VALUES` of an `INSERT` is left alone: the
//! table's types decide there.

use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::expr_rewriter::{NamePreserver, coerce_plan_expr_for_schema};
use datafusion::logical_expr::{
    Between, BinaryExpr, Case, Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Union, Values, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    self as sql, Query, SetExpr, Statement as SqlStatement, VisitMut, VisitorMut,
};

use crate::overflow::decimal::{MAX_PRECISION, as_decimal};
use crate::trino_types::Declared;

/// Name of the identity that marks a value of a `VALUES` list.
pub(crate) const VALUES_CELL: &str = "codelake_values_cell";

/// `codelake_values_cell(x)`: `x` (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ValuesCell {
    signature: Signature,
}

impl ScalarUDFImpl for ValuesCell {
    fn name(&self) -> &'static str {
        VALUES_CELL
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [only] = arg_types else {
            return exec_err!("{VALUES_CELL} expects one argument");
        };
        Ok(only.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [only] = args.arg_fields else {
            return exec_err!("{VALUES_CELL} expects one argument");
        };
        Ok(Arc::new(Field::new(
            VALUES_CELL,
            only.data_type().clone(),
            only.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [only] = args.args.as_slice() else {
            return exec_err!("{VALUES_CELL} expects one argument");
        };
        Ok(only.clone())
    }
}

static VALUES_CELL_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(ValuesCell {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

/// The function [`mark_values`] calls; the engine registers it so a marked
/// statement plans.
pub(crate) fn values_cell() -> ScalarUDF {
    VALUES_CELL_UDF.as_ref().clone()
}

fn cell_call(expr: sql::Expr) -> sql::Expr {
    use sql::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    };
    sql::Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(VALUES_CELL)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn mark_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for cell in row.iter_mut() {
                    *cell = cell_call(std::mem::replace(cell, sql::Expr::value(sql::Value::Null)));
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            mark_set_expr(left);
            mark_set_expr(right);
        }
        // Nested queries are visited on their own.
        _ => {}
    }
}

/// Positional names for the unnamed items of every branch of a set
/// operation but the first (see [`name_later_branches`]).
fn name_branches(body: &mut SetExpr, first: bool) {
    match body {
        SetExpr::SetOperation {
            left,
            right,
            set_quantifier,
            ..
        } if !matches!(
            set_quantifier,
            sql::SetQuantifier::ByName | sql::SetQuantifier::AllByName | sql::SetQuantifier::DistinctByName
        ) =>
        {
            name_branches(left, first);
            name_branches(right, false);
        }
        SetExpr::Select(select) if !first => {
            for (position, item) in select.projection.iter_mut().enumerate() {
                if let sql::SelectItem::UnnamedExpr(expr) = item {
                    *item = sql::SelectItem::ExprWithAlias {
                        expr: std::mem::replace(expr, sql::Expr::value(sql::Value::Null)),
                        alias: sql::Ident::new(format!("_codelake_col{}", position + 1)),
                    };
                }
            }
        }
        _ => {}
    }
}

struct NameLaterBranches;

impl VisitorMut for NameLaterBranches {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        name_branches(&mut query.body, true);
        ControlFlow::Continue(())
    }
}

/// A set operation takes its column names from its first branch; the names
/// in the others mean nothing, and Trino accepts one written twice there
/// (`SELECT ct."__$start_lsn", …, ct.* FROM …` after `UNION ALL`).
/// `DataFusion` plans each branch as a projection and refuses the repeated
/// name ("Projections require unique expression names"). Every unnamed item
/// of a later branch gets a positional name (`BY NAME` operations excepted).
pub(crate) fn name_later_branches(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = inner.as_mut().visit(&mut NameLaterBranches);
}

/// Marks every `VALUES` list but the one an `INSERT` reads.
#[derive(Default)]
struct MarkValues {
    /// Address of the query an `INSERT` reads.
    insert_source: Option<usize>,
}

impl VisitorMut for MarkValues {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut SqlStatement) -> ControlFlow<()> {
        if let SqlStatement::Insert(insert) = statement
            && let Some(source) = &insert.source
        {
            self.insert_source = Some(std::ptr::from_ref::<Query>(source) as usize);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if self.insert_source != Some(std::ptr::from_ref::<Query>(query) as usize) {
            mark_set_expr(&mut query.body);
        }
        ControlFlow::Continue(())
    }
}

/// Wraps every value of a `VALUES` list in `codelake_values_cell` (see the
/// module documentation).
pub(crate) fn mark_values(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = inner.as_mut().visit(&mut MarkValues::default());
}

// ---------------------------------------------------------------- cast names

/// Name of the identity that names a cast in an aggregate's arguments.
pub(crate) const NAMED_CAST: &str = "codelake_named_cast";

/// `codelake_named_cast(CAST(x AS T), 'T')`: the cast, named with its type.
///
/// `DataFusion` leaves casts out of expression names, so `sum(n)` and
/// `sum(CAST(n AS SMALLINT))` are both named `sum(n)`, and a query with both
/// fails ("Projections require unique expression names", "Schema contains
/// duplicate unqualified field name"); in Trino they are two columns.
/// [`name_casts_in_aggregates`] wraps the casts of such functions before
/// planning; the analyzer removes the wrapper ([`unname_cast`]), the names it
/// gave staying as aliases.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NamedCast {
    signature: Signature,
}

impl ScalarUDFImpl for NamedCast {
    fn name(&self) -> &'static str {
        NAMED_CAST
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [cast, spelled] = args else {
            return exec_err!("{NAMED_CAST} expects two arguments");
        };
        let spelled = match spelled {
            Expr::Literal(ScalarValue::Utf8(Some(text)), _) => text.clone(),
            other => other.schema_name().to_string(),
        };
        Ok(format!("CAST({} AS {spelled})", cast.schema_name()))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let Some(first) = arg_types.first() else {
            return exec_err!("{NAMED_CAST} expects two arguments");
        };
        Ok(first.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let Some(first) = args.arg_fields.first() else {
            return exec_err!("{NAMED_CAST} expects two arguments");
        };
        Ok(Arc::new(first.as_ref().clone().with_name(NAMED_CAST)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return exec_err!("{NAMED_CAST} expects two arguments");
        };
        Ok(first.clone())
    }
}

static NAMED_CAST_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(NamedCast {
        signature: Signature::any(2, Volatility::Immutable),
    }))
});

/// The function [`name_casts_in_aggregates`] calls; the engine registers it
/// so a marked statement plans.
pub(crate) fn named_cast() -> ScalarUDF {
    NAMED_CAST_UDF.as_ref().clone()
}

/// `codelake_named_cast(x, '…')` → `x` (see [`NamedCast`]).
pub(crate) fn unname_cast(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::ScalarFunction(ScalarFunction { func, mut args })
            if func.name() == NAMED_CAST && args.len() == 2 =>
        {
            args.truncate(1);
            Transformed::yes(args.pop().unwrap_or_default())
        }
        other => Transformed::no(other),
    }
}

/// Name of the identity around a timestamp cast that must stay exact.
pub const EXACT_CAST: &str = "codelake_exact_cast";

/// `codelake_exact_cast(CAST(x AS timestamp(finer)))`: the cast itself.
///
/// `DataFusion`'s simplifier unwraps `CAST(col AS T) <op> literal` into
/// `col <op> CAST(literal AS type of col)` and converts a timestamp literal
/// to a coarser unit by truncation (`expr-common` `cast_between_timestamp`),
/// so `CAST(ms AS µs) >= .123100` became `ms >= .123` (true for `.123`).
/// The wrapper hides the cast from that rewrite; the comparison is made in
/// the finer unit, as [`unify_timestamps`] decided.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExactCast {
    signature: Signature,
}

impl ScalarUDFImpl for ExactCast {
    fn name(&self) -> &'static str {
        EXACT_CAST
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [only] = args else {
            return exec_err!("{EXACT_CAST} expects one argument");
        };
        Ok(only.schema_name().to_string())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [only] = arg_types else {
            return exec_err!("{EXACT_CAST} expects one argument");
        };
        Ok(only.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let [only] = args.arg_fields else {
            return exec_err!("{EXACT_CAST} expects one argument");
        };
        Ok(Arc::new(only.as_ref().clone().with_name(EXACT_CAST)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(only) = args.args.first() else {
            return exec_err!("{EXACT_CAST} expects one argument");
        };
        Ok(only.clone())
    }
}

static EXACT_CAST_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(ExactCast {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

/// The identity [`unify_timestamps`] calls; the engine registers it so plans
/// that carry it can be printed and planned again.
pub(crate) fn exact_cast() -> ScalarUDF {
    EXACT_CAST_UDF.as_ref().clone()
}

/// A timestamp cast to a finer unit, kept from the simplifier (see
/// [`ExactCast`]).
fn widen_timestamp(expr: Expr, to: DataType) -> Expr {
    EXACT_CAST_UDF.call(vec![Expr::Cast(Cast::new(Box::new(expr), to))])
}

/// `f(…)` with every cast removed, as `DataFusion` names it.
fn without_casts(expr: &sql::Expr) -> String {
    let mut stripped = expr.clone();
    let _ = sql::visit_expressions_mut(&mut stripped, |e| {
        if let sql::Expr::Cast { expr: inner, .. } = e {
            *e = std::mem::replace(inner.as_mut(), sql::Expr::value(sql::Value::Null));
        }
        ControlFlow::<()>::Continue(())
    });
    stripped.to_string()
}

/// Every function call of `expr` (at any depth): (name without casts, text).
fn function_names(
    expr: &sql::Expr,
    names: &mut std::collections::HashMap<String, std::collections::HashSet<String>>,
) {
    let _ = sql::visit_expressions(expr, |e| {
        if let sql::Expr::Function(_) = e {
            names.entry(without_casts(e)).or_default().insert(e.to_string());
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Wraps the casts inside the functions whose name is in `colliding`.
struct NameCasts<'a> {
    colliding: &'a std::collections::HashSet<String>,
    /// Enclosing colliding functions.
    active: usize,
}

impl VisitorMut for NameCasts<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut sql::Expr) -> ControlFlow<()> {
        if matches!(expr, sql::Expr::Function(_)) && self.colliding.contains(&without_casts(expr)) {
            self.active += 1;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut sql::Expr) -> ControlFlow<()> {
        match expr {
            sql::Expr::Function(f) if f.name.to_string() != NAMED_CAST => {
                // The name as it was when visited first: its casts are
                // already wrapped now.
                let original = {
                    let mut unwrapped = expr.clone();
                    let _ = sql::visit_expressions_mut(&mut unwrapped, |e| {
                        if let sql::Expr::Function(g) = e
                            && g.name.to_string() == NAMED_CAST
                            && let sql::FunctionArguments::List(list) = &mut g.args
                            && let Some(sql::FunctionArg::Unnamed(sql::FunctionArgExpr::Expr(inner))) =
                                list.args.first_mut()
                        {
                            *e = std::mem::replace(inner, sql::Expr::value(sql::Value::Null));
                        }
                        ControlFlow::<()>::Continue(())
                    });
                    without_casts(&unwrapped)
                };
                if self.colliding.contains(&original) {
                    self.active = self.active.saturating_sub(1);
                }
            }
            sql::Expr::Cast { data_type, .. } if self.active > 0 => {
                let spelled = data_type.to_string();
                let cast = std::mem::replace(expr, sql::Expr::value(sql::Value::Null));
                *expr = named_cast_call(cast, spelled);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn named_cast_call(cast: sql::Expr, spelled: String) -> sql::Expr {
    use sql::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    };
    let arg = |e: sql::Expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(e));
    sql::Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(NAMED_CAST)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                arg(cast),
                arg(sql::Expr::value(sql::Value::SingleQuotedString(spelled))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// The expressions of a query whose aggregates meet in one plan node: the
/// select list, `HAVING` and `ORDER BY`.
fn aggregate_expressions(query: &mut Query) -> Vec<&mut sql::Expr> {
    let mut exprs: Vec<&mut sql::Expr> = Vec::new();
    if let SetExpr::Select(select) = query.body.as_mut() {
        let select = select.as_mut();
        for item in &mut select.projection {
            if let sql::SelectItem::UnnamedExpr(e) | sql::SelectItem::ExprWithAlias { expr: e, .. } = item {
                exprs.push(e);
            }
        }
        if let Some(having) = &mut select.having {
            exprs.push(having);
        }
    } else {
        return exprs;
    }
    if let Some(order_by) = &mut query.order_by
        && let sql::OrderByKind::Expressions(items) = &mut order_by.kind
    {
        for item in items {
            exprs.push(&mut item.expr);
        }
    }
    exprs
}

struct NameCastsInQueries;

impl VisitorMut for NameCastsInQueries {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        let mut names = std::collections::HashMap::new();
        for expr in aggregate_expressions(query) {
            function_names(expr, &mut names);
        }
        let colliding: std::collections::HashSet<String> = names
            .into_iter()
            .filter(|(_, texts)| texts.len() > 1)
            .map(|(name, _)| name)
            .collect();
        if colliding.is_empty() {
            return ControlFlow::Continue(());
        }
        for expr in aggregate_expressions(query) {
            let _ = expr.visit(&mut NameCasts {
                colliding: &colliding,
                active: 0,
            });
        }
        ControlFlow::Continue(())
    }
}

/// Names the casts of functions that `DataFusion` would name alike (see
/// [`NamedCast`]): only where two calls differ by their casts alone.
pub(crate) fn name_casts_in_aggregates(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = inner.as_mut().visit(&mut NameCastsInQueries);
}

/// `DECIMAL` without precision is `decimal(38,0)` in Trino (`DataFusion`:
/// `decimal(38,10)`).
fn trino_decimal(data_type: &mut sql::DataType) {
    use sql::{DataType as T, ExactNumberInfo};
    if let T::Decimal(info @ ExactNumberInfo::None)
    | T::Numeric(info @ ExactNumberInfo::None)
    | T::Dec(info @ ExactNumberInfo::None) = data_type
    {
        *info = ExactNumberInfo::PrecisionAndScale(38, 0);
    }
}

/// Gives `DECIMAL` without precision Trino's `decimal(38,0)`, in casts and
/// column definitions.
pub(crate) fn default_decimal_precision(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    if let SqlStatement::CreateTable(create) = inner.as_mut() {
        for column in &mut create.columns {
            trino_decimal(&mut column.data_type);
        }
    }
    let _ = sql::visit_expressions_mut(inner.as_mut(), |expr| {
        if let sql::Expr::Cast { data_type, .. } = expr {
            trino_decimal(data_type);
        }
        ControlFlow::<()>::Continue(())
    });
}

fn is_values_cell(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(f) if f.func.name() == VALUES_CELL)
}

// ---------------------------------------------------------------- types

/// Rank of an integer type, `None` for anything else.
fn integer_rank(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Int8 => Some(0),
        DataType::Int16 => Some(1),
        DataType::Int32 => Some(2),
        DataType::Int64 => Some(3),
        _ => None,
    }
}

fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn is_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..)
    )
}

/// A number Trino has: not an unsigned integer, nor a decimal above 38
/// digits.
fn is_number(data_type: &DataType) -> bool {
    integer_rank(data_type).is_some()
        || is_float(data_type)
        || (is_decimal(data_type) && as_decimal(data_type).is_some())
}

/// Trino's common type of two numbers (see the module documentation), or
/// `None` when one is not a number Trino has (unsigned integers, decimals
/// above 38 digits).
fn common_number(a: &DataType, b: &DataType) -> Option<DataType> {
    if a == b {
        return Some(a.clone());
    }
    if let (Some(ra), Some(rb)) = (integer_rank(a), integer_rank(b)) {
        return Some(if ra >= rb { a.clone() } else { b.clone() });
    }
    if !is_number(a) || !is_number(b) {
        return None;
    }
    if is_float(a) || is_float(b) {
        let double = matches!(a, DataType::Float64) || matches!(b, DataType::Float64);
        return Some(if double {
            DataType::Float64
        } else {
            DataType::Float32
        });
    }
    let ((p1, s1), (p2, s2)) = (as_decimal(a)?, as_decimal(b)?);
    let scale = s1.max(s2);
    let digits = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
    let precision = (digits + i16::from(scale)).min(i16::from(MAX_PRECISION));
    Some(DataType::Decimal128(u8::try_from(precision).ok()?, scale))
}

/// Trino's common type of numbers (`NULL`s ignored); `None` when a type is
/// not a number, when all are equal, or when there is nothing to unify.
fn common_numeric_type(types: &[DataType]) -> Option<DataType> {
    let mut known = types.iter().filter(|t| !t.is_null());
    let first = known.next()?.clone();
    let mut all_equal = true;
    let mut common = first;
    for t in known {
        all_equal &= *t == common;
        common = common_number(&common, t)?;
    }
    (!all_equal || types.iter().any(DataType::is_null)).then_some(common)
}

fn cast_if_needed(expr: Expr, from: &DataType, to: &DataType) -> Expr {
    if from == to {
        expr
    } else {
        Expr::Cast(Cast::new(Box::new(expr), to.clone()))
    }
}

/// Fractional digits of a timestamp unit, to order units by precision.
fn unit_digits(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// Where timestamps meet, Trino converts them to the higher precision
/// (`TIMESTAMP '… 00.123' = TIMESTAMP '… 00.123400'` is false);
/// `DataFusion` converts them to the lower one, truncating the other side
/// (it was true). Every timestamp is cast to the finest unit among them.
///
/// Where timestamps with and without time zone meet, a timestamp without
/// time zone is also the local time in the session zone (Trino casts it to
/// `timestamp with time zone`); `DataFusion` reads it as UTC.
fn unify_timestamps(exprs: Vec<Expr>, types: &[DataType], session_zone: &str) -> Transformed<Vec<Expr>> {
    let zoned = types.iter().any(|t| matches!(t, DataType::Timestamp(_, Some(_))));
    let Some(finest) = types
        .iter()
        .filter_map(|t| match t {
            DataType::Timestamp(unit, _) => Some(*unit),
            _ => None,
        })
        .max_by_key(|unit| unit_digits(*unit))
    else {
        return Transformed::no(exprs);
    };
    let target = |t: &DataType| match t {
        DataType::Timestamp(_, None) if zoned => Some(DataType::Timestamp(finest, Some(session_zone.into()))),
        DataType::Timestamp(_, zone) => Some(DataType::Timestamp(finest, zone.clone())),
        _ => None,
    };
    if !types.iter().any(|t| target(t).is_some_and(|to| to != *t)) {
        return Transformed::no(exprs);
    }
    Transformed::yes(
        exprs
            .into_iter()
            .zip(types)
            .map(|(e, t)| match (target(t), t) {
                (Some(to), DataType::Timestamp(unit, _)) if *unit != finest => widen_timestamp(e, to),
                (Some(to), _) => cast_if_needed(e, t, &to),
                (None, _) => e,
            })
            .collect(),
    )
}

/// Casts every expression to Trino's common type of their numbers (or of
/// their timestamps, see [`unify_timestamps`]).
fn unify(exprs: Vec<Expr>, schema: &DFSchema, session_zone: &str) -> Result<Transformed<Vec<Expr>>> {
    let types = exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let Some(common) = common_numeric_type(&types) else {
        return Ok(unify_timestamps(exprs, &types, session_zone));
    };
    if types.iter().all(|t| t.is_null() || *t == common) {
        return Ok(Transformed::no(exprs));
    }
    Ok(Transformed::yes(
        exprs
            .into_iter()
            .zip(&types)
            .map(|(e, t)| {
                if t.is_null() {
                    e
                } else {
                    cast_if_needed(e, t, &common)
                }
            })
            .collect(),
    ))
}

fn is_comparison(op: datafusion::logical_expr::Operator) -> bool {
    use datafusion::logical_expr::Operator::{
        Eq, Gt, GtEq, IsDistinctFrom, IsNotDistinctFrom, Lt, LtEq, NotEq,
    };
    matches!(
        op,
        Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom
    )
}

/// Functions whose arguments share one type (`make_array`: `ARRAY[…]`).
fn unifies_arguments(name: &str) -> bool {
    matches!(
        name,
        "coalesce" | "greatest" | "least" | "nvl" | "ifnull" | "make_array"
    )
}

/// `nullif(a, b)`: compared in the common type, typed as `a`.
fn nullif(args: Vec<Expr>, func: &Arc<ScalarUDF>, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let [a, b] = <[Expr; 2]>::try_from(args)
        .map_err(|_| datafusion::error::DataFusionError::Plan("nullif expects two arguments".into()))?;
    let (ta, tb) = (a.get_type(schema)?, b.get_type(schema)?);
    let Some(common) = common_numeric_type(&[ta.clone(), tb.clone()]).filter(|_| !ta.is_null()) else {
        return Ok(Transformed::no(func.call(vec![a, b])));
    };
    if common == ta {
        return Ok(Transformed::yes(
            func.call(vec![a, cast_if_needed(b, &tb, &common)]),
        ));
    }
    // `a` is not of the common type: compare a copy, answer `a` itself.
    let equal = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(cast_if_needed(a.clone(), &ta, &common)),
        datafusion::logical_expr::Operator::Eq,
        Box::new(cast_if_needed(b, &tb, &common)),
    ));
    Ok(Transformed::yes(Expr::Case(Case::new(
        None,
        vec![(
            Box::new(equal),
            Box::new(Expr::Literal(ScalarValue::try_from(&ta)?, None)),
        )],
        Some(Box::new(a)),
    ))))
}

/// Adds the declared text type of one value where text values meet: a
/// literal is `varchar(<its length>)`, a typed expression its
/// `varchar(n)`/`char(n)`, other text unbounded (`None`); a `NULL` does not
/// count.
fn push_text_type(expr: &Expr, schema: &DFSchema, declared: &mut Vec<Option<Declared>>) -> Result<()> {
    if let Expr::Literal(value, _) = expr
        && value.is_null()
    {
        return Ok(());
    }
    if let Some(text) = text_literal(expr) {
        declared.push(Some(Declared::Varchar(
            u32::try_from(text.chars().count()).unwrap_or(u32::MAX),
        )));
        return Ok(());
    }
    let (_, field) = expr.to_field(schema)?;
    declared.push(crate::trino_types::declared(&field));
    Ok(())
}

/// A `CASE` or `coalesce`-like text result declares Trino's common type of
/// its values (`CASE … THEN 'Fisica' … THEN 'Juridica' END` is `varchar(8)`,
/// `coalesce(varchar(100) column, '')` is `varchar(100)`), which
/// `CREATE TABLE AS` stores; `DataFusion` gave unbounded text.
fn typed_text_result(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let values: Vec<&Expr> = match &expr {
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .collect(),
        Expr::ScalarFunction(ScalarFunction { func, args }) if unifies_arguments(func.name()) => {
            args.iter().collect()
        }
        _ => return Ok(Transformed::no(expr)),
    };
    if !is_text_type(&expr.get_type(schema)?) {
        return Ok(Transformed::no(expr));
    }
    let mut declared = Vec::with_capacity(values.len());
    for value in values {
        push_text_type(value, schema, &mut declared)?;
    }
    let Some(common) = common_text(&declared) else {
        return Ok(Transformed::no(expr));
    };
    Ok(Transformed::yes(crate::declared::trino_typed().call(vec![
        expr,
        Expr::Literal(ScalarValue::Utf8(Some(typed_name(Some(common)))), None),
    ])))
}

/// Every construct of one expression node where values meet (see the module
/// documentation).
fn common_types(expr: Expr, schema: &DFSchema, session_zone: &str) -> Result<Transformed<Expr>> {
    let unified = common_values(expr, schema, session_zone)?;
    let transformed = unified.transformed;
    let typed = typed_text_result(unified.data, schema)?;
    Ok(Transformed::new_transformed(
        typed.data,
        transformed || typed.transformed,
    ))
}

fn common_values(expr: Expr, schema: &DFSchema, session_zone: &str) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            unify(vec![*left, *right], schema, session_zone)?.update_data(|mut pair| {
                let right = pair.pop().unwrap_or_default();
                let left = pair.pop().unwrap_or_default();
                Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
            })
        }
        Expr::Case(Case {
            expr: operand,
            when_then_expr,
            else_expr,
        }) => {
            let (whens, thens): (Vec<Expr>, Vec<Expr>) =
                when_then_expr.into_iter().map(|(w, t)| (*w, *t)).unzip();
            let mut branches = thens;
            let has_else = else_expr.is_some();
            branches.extend(else_expr.map(|e| *e));
            unify(branches, schema, session_zone)?.update_data(|mut branches| {
                let else_expr = has_else.then(|| Box::new(branches.pop().unwrap_or_default()));
                Expr::Case(Case::new(
                    operand,
                    whens
                        .into_iter()
                        .zip(branches)
                        .map(|(w, t)| (Box::new(w), Box::new(t)))
                        .collect(),
                    else_expr,
                ))
            })
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) if func.name() == "nullif" && args.len() == 2 => {
            return nullif(args, &func, schema);
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) if unifies_arguments(func.name()) => {
            unify(args, schema, session_zone)?.update_data(|args| func.call(args))
        }
        Expr::InList(InList {
            expr: value,
            list,
            negated,
        }) => {
            let mut all = vec![*value];
            all.extend(list);
            unify(all, schema, session_zone)?.update_data(|mut all| {
                let value = all.remove(0);
                Expr::InList(InList::new(Box::new(value), all, negated))
            })
        }
        Expr::Between(Between {
            expr: value,
            negated,
            low,
            high,
        }) => unify(vec![*value, *low, *high], schema, session_zone)?.update_data(|mut all| {
            let high = all.pop().unwrap_or_default();
            let low = all.pop().unwrap_or_default();
            let value = all.pop().unwrap_or_default();
            Expr::Between(Between::new(
                Box::new(value),
                negated,
                Box::new(low),
                Box::new(high),
            ))
        }),
        other => Transformed::no(other),
    })
}

/// A day-to-second interval literal as a plain duration: its days as
/// 24-hour periods, the months (a year-to-month interval) kept.
fn as_duration(value: &ScalarValue) -> Option<ScalarValue> {
    use datafusion::arrow::datatypes::IntervalMonthDayNano;
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    match value {
        ScalarValue::IntervalMonthDayNano(Some(v)) if v.days != 0 => {
            let nanos = i64::from(v.days)
                .checked_mul(NANOS_PER_DAY)?
                .checked_add(v.nanoseconds)?;
            Some(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(v.months, 0, nanos),
            )))
        }
        ScalarValue::IntervalDayTime(Some(v)) if v.days != 0 => {
            let millis = i64::from(v.days)
                .checked_mul(86_400_000)?
                .checked_add(i64::from(v.milliseconds))?;
            Some(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 0, millis.checked_mul(1_000_000)?),
            )))
        }
        _ => None,
    }
}

/// `<timestamp with time zone> ± INTERVAL 'n' DAY`: Trino 431 adds `n` times
/// 24 hours (a day-to-second interval is a duration), so across a
/// daylight-saving change the local time moves by an hour; `DataFusion` adds
/// calendar days in the value's zone. The interval literal's days become
/// nanoseconds.
fn zoned_day_interval(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    use datafusion::logical_expr::Operator;
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(op, Operator::Plus | Operator::Minus) {
        return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })));
    }
    let zoned =
        |e: &Expr| -> Result<bool> { Ok(matches!(e.get_type(schema)?, DataType::Timestamp(_, Some(_)))) };
    let duration = |e: &Expr| match e {
        Expr::Literal(value, metadata) => as_duration(value).map(|v| Expr::Literal(v, metadata.clone())),
        _ => None,
    };
    if zoned(&left)?
        && let Some(interval) = duration(&right)
    {
        return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            left,
            op,
            Box::new(interval),
        ))));
    }
    if op == Operator::Plus
        && zoned(&right)?
        && let Some(interval) = duration(&left)
    {
        return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(interval),
            op,
            right,
        ))));
    }
    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })))
}

/// Schema that resolves the columns of `plan`'s expressions.
fn expression_schema(plan: &LogicalPlan) -> Result<DFSchema> {
    let mut schema = datafusion::logical_expr::utils::merge_schema(&plan.inputs());
    if let LogicalPlan::TableScan(scan) = plan {
        let source = DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())?;
        schema.merge(&source);
    }
    Ok(schema)
}

/// `VALUES` with marked values: each column typed again (see the module
/// documentation).
fn retype_values(values: Values) -> Result<Transformed<LogicalPlan>> {
    let marked = values.values.iter().flatten().any(|cell| match cell {
        Expr::Cast(Cast { expr, .. }) => is_values_cell(expr),
        other => is_values_cell(other),
    });
    if !marked {
        return Ok(Transformed::no(LogicalPlan::Values(values)));
    }
    let empty = DFSchema::empty();
    // Each value as written, without `DataFusion`'s cast.
    let cells: Vec<Vec<Expr>> = values
        .values
        .into_iter()
        .map(|row| row.into_iter().map(unmark).collect())
        .collect();
    let mut rows: Vec<Vec<Expr>> = vec![Vec::new(); cells.len()];
    let mut fields = Vec::with_capacity(values.schema.fields().len());
    for (j, field) in values.schema.fields().iter().enumerate() {
        let types = cells
            .iter()
            .map(|row| row[j].get_type(&empty))
            .collect::<Result<Vec<_>>>()?;
        let target = common_numeric_type(&types)
            .or_else(|| only_type(&types))
            .unwrap_or_else(|| field.data_type().clone());
        for ((row, cells), t) in rows.iter_mut().zip(&cells).zip(&types) {
            row.push(cast_if_needed(cells[j].clone(), t, &target));
        }
        // Text literals only: `varchar(n)`, `n` the longest (Trino).
        let lengths: Option<Vec<usize>> = cells
            .iter()
            .filter(|row| !matches!(&row[j], Expr::Literal(v, _) if v.is_null()))
            .map(|row| text_literal(&row[j]).map(|s| s.chars().count()))
            .collect();
        let metadata = match lengths.and_then(|l| l.into_iter().max()) {
            Some(longest) if is_text_type(&target) => {
                Declared::Varchar(u32::try_from(longest).unwrap_or(u32::MAX)).metadata()
            }
            _ => field.metadata().clone(),
        };
        fields.push(Arc::new(
            Field::new(field.name(), target, true).with_metadata(metadata),
        ));
    }
    let schema = DFSchema::from_unqualified_fields(fields.into(), values.schema.metadata().clone())?;
    Ok(Transformed::yes(LogicalPlan::Values(Values {
        schema: Arc::new(schema),
        values: rows,
    })))
}

/// The text of a text literal.
fn text_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::Utf8View(Some(text))
            | ScalarValue::LargeUtf8(Some(text)),
            _,
        ) => Some(text),
        _ => None,
    }
}

/// A text literal selected as is (`SELECT 'abc'`) is `varchar(3)`, as in
/// Trino; the type is recorded in the literal's metadata.
fn typed_text_literal(expr: Expr) -> Expr {
    let typed = |literal: Expr| -> Expr {
        match literal {
            Expr::Literal(value, None) => match text_literal(&Expr::Literal(value.clone(), None)) {
                Some(text) => {
                    let length = u32::try_from(text.chars().count()).unwrap_or(u32::MAX);
                    let metadata = Declared::Varchar(length).metadata().into_iter().collect();
                    Expr::Literal(
                        value,
                        Some(datafusion::common::metadata::FieldMetadata::new(metadata)),
                    )
                }
                None => Expr::Literal(value, None),
            },
            other => other,
        }
    };
    match expr {
        Expr::Alias(mut alias) => {
            alias.expr = Box::new(typed(*alias.expr));
            Expr::Alias(alias)
        }
        other => typed(other),
    }
}

/// The one type of all non-`NULL` values.
fn only_type(types: &[DataType]) -> Option<DataType> {
    let mut known = types.iter().filter(|t| !t.is_null());
    let first = known.next()?;
    known.all(|t| t == first).then(|| first.clone())
}

/// A cell of a planned `VALUES` → the value as written.
fn unmark(cell: Expr) -> Expr {
    let cell = match cell {
        Expr::Cast(Cast { expr, .. }) if is_values_cell(&expr) => *expr,
        other => other,
    };
    match cell {
        Expr::ScalarFunction(ScalarFunction { func, mut args })
            if func.name() == VALUES_CELL && args.len() == 1 =>
        {
            args.pop().unwrap_or_default()
        }
        other => other,
    }
}

/// `UNION`: each input's columns cast to Trino's common type.
fn is_text_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Trino's common bounded text type: `varchar(max n)` of `varchar(n)`s, the
/// one `char(n)` they all are, unbounded otherwise.
fn common_text(declared: &[Option<Declared>]) -> Option<Declared> {
    let first = (*declared.first()?)?;
    if declared.iter().all(|d| *d == Some(first)) {
        return Some(first);
    }
    let mut longest = 0;
    for d in declared {
        match d {
            Some(Declared::Varchar(n)) => longest = longest.max(*n),
            _ => return None,
        }
    }
    Some(Declared::Varchar(longest))
}

/// The name [`crate::declared::TrinoTyped`] reads (`''`: no declared type).
fn typed_name(declared: Option<Declared>) -> String {
    declared.map_or_else(String::new, |d| d.name(&DataType::Utf8))
}

/// `field` declaring `declared` (nothing when `None`), its other metadata
/// kept.
fn with_declared(field: &Field, declared: Option<Declared>) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.remove(crate::trino_types::TRINO_TYPE);
    if let Some(declared) = declared {
        metadata.extend(declared.metadata());
    }
    field.clone().with_metadata(metadata)
}

/// Text columns of a `UNION` declare one type in every input (`SELECT 'a'
/// UNION ALL SELECT 'bcd'` is `varchar(3)`); `DataFusion` would keep the
/// last input's.
///
/// The union's own schema is `DataFusion`'s, computed when the SQL was
/// planned: before this rule typed its inputs' text literals, so without
/// their types. Each column declares its inputs' type again, even where
/// the inputs agree: `a UNION ALL b UNION ALL c` is a union of `a UNION ALL
/// b` and `c`, and the outer one would see unbounded text in the inner one
/// and drop the type of every input (`'CRITICO'`/`'OK'` in three inputs
/// stored as `varchar`, not `varchar(7)`).
fn unify_union_text(union: Union) -> Result<Transformed<Union>> {
    let columns = union.schema.fields().len();
    // Per text column: the common type, and whether an input declares
    // another (it is then cast to it).
    let mut targets: Vec<Option<Option<Declared>>> = Vec::with_capacity(columns);
    let mut declared: Vec<Option<Option<Declared>>> = Vec::with_capacity(columns);
    for j in 0..columns {
        let fields: Vec<&Field> = union
            .inputs
            .iter()
            .map(|i| i.schema().field(j).as_ref())
            .collect();
        if !fields.iter().all(|f| is_text_type(f.data_type())) {
            targets.push(None);
            declared.push(None);
            continue;
        }
        let own: Vec<Option<Declared>> = fields.iter().map(|f| crate::trino_types::declared(f)).collect();
        let common = common_text(&own);
        targets.push(own.iter().any(|d| *d != common).then_some(common));
        declared.push(Some(common));
    }
    let fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = union
        .schema
        .iter()
        .zip(&declared)
        .map(|((qualifier, field), declared)| match declared {
            Some(declared) => (qualifier.cloned(), Arc::new(with_declared(field, *declared))),
            None => (qualifier.cloned(), Arc::clone(field)),
        })
        .collect();
    let schema = Arc::new(DFSchema::new_with_metadata(
        fields,
        union.schema.metadata().clone(),
    )?);
    if targets.iter().all(Option::is_none) {
        if schema.fields() == union.schema.fields() {
            return Ok(Transformed::no(union));
        }
        return Ok(Transformed::yes(Union {
            inputs: union.inputs,
            schema,
        }));
    }
    let typed = crate::declared::trino_typed();
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in union.inputs {
        let exprs: Vec<Expr> = input
            .schema()
            .iter()
            .zip(&targets)
            .map(|((qualifier, field), target)| {
                let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
                match target {
                    Some(declared) => typed
                        .call(vec![
                            column,
                            Expr::Literal(ScalarValue::Utf8(Some(typed_name(*declared))), None),
                        ])
                        .alias(field.name()),
                    None => column,
                }
            })
            .collect();
        inputs.push(Arc::new(LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new(exprs, input)?,
        )));
    }
    Ok(Transformed::yes(Union { inputs, schema }))
}

fn unify_union(union: Union) -> Result<Transformed<LogicalPlan>> {
    let text = unify_union_text(union)?;
    let (union, text_changed) = (text.data, text.transformed);
    let numeric = unify_union_numbers(union)?;
    Ok(Transformed::new_transformed(
        numeric.data,
        numeric.transformed || text_changed,
    ))
}

/// The number type every input of a union has, when the union declares
/// another. The union's schema is `DataFusion`'s, computed when the SQL was
/// planned: `a UNION ALL b UNION ALL c` is a union of `a UNION ALL b` and
/// `c`, and the outer one keeps the type `DataFusion` gave `decimal UNION ALL
/// double` (`decimal(35,15)`) after this rule cast the inner one's inputs to
/// `double`; type coercion would then cast every input back to that decimal.
fn agreed_number(types: &[DataType], declared: &DataType) -> Option<DataType> {
    only_type(types).filter(|t| t != declared && is_number(t))
}

/// The finest unit among timestamps of the same kind (all with a zone, or
/// none), when an input or the union has another: Trino 431 unites
/// `timestamp(3)` and `timestamp(0)` as `timestamp(3)`; `DataFusion`'s type
/// coercion took the coarser unit and cut the fraction (`.123` became
/// `.000`, ISS-032 item 8). Every input is cast to it here (widening is
/// exact), so type coercion finds them alike. The zone is the union's.
fn finest_timestamp(types: &[DataType], declared: &DataType) -> Option<DataType> {
    let DataType::Timestamp(_, zone) = declared else {
        return None;
    };
    let rank = |unit: &TimeUnit| match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    };
    let mut units = Vec::with_capacity(types.len());
    for t in types.iter().filter(|t| !t.is_null()) {
        let DataType::Timestamp(unit, own) = t else {
            return None;
        };
        if own.is_some() != zone.is_some() {
            return None;
        }
        units.push(*unit);
    }
    let finest = units.into_iter().max_by_key(rank)?;
    let target = DataType::Timestamp(finest, zone.clone());
    let differs = |t: &DataType| !t.is_null() && t != &target;
    (types.iter().any(differs) || &target != declared).then_some(target)
}

fn unify_union_numbers(union: Union) -> Result<Transformed<LogicalPlan>> {
    let columns = union.schema.fields().len();
    let mut targets: Vec<Option<DataType>> = Vec::with_capacity(columns);
    for j in 0..columns {
        let types: Vec<DataType> = union
            .inputs
            .iter()
            .map(|input| input.schema().field(j).data_type().clone())
            .collect();
        let declared = union.schema.field(j).data_type();
        targets.push(
            common_numeric_type(&types)
                .or_else(|| agreed_number(&types, declared))
                .or_else(|| finest_timestamp(&types, declared)),
        );
    }
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(LogicalPlan::Union(union)));
    }
    let fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = union
        .schema
        .iter()
        .zip(&targets)
        .map(|((qualifier, field), target)| {
            let field = match target {
                Some(t) => Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(t.clone())
                        .with_nullable(true),
                ),
                None => Arc::clone(field),
            };
            (qualifier.cloned(), field)
        })
        .collect();
    let schema = DFSchema::new_with_metadata(fields, union.schema.metadata().clone())?;
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in union.inputs {
        // Each input keeps its own column names; only the types change.
        let own: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = input
            .schema()
            .iter()
            .zip(&targets)
            .map(|((qualifier, field), target)| {
                let field = match target {
                    Some(t) => Arc::new(field.as_ref().clone().with_data_type(t.clone())),
                    None => Arc::clone(field),
                };
                (qualifier.cloned(), field)
            })
            .collect();
        let own = DFSchema::new_with_metadata(own, input.schema().metadata().clone())?;
        inputs.push(Arc::new(coerce_plan_expr_for_schema(
            Arc::unwrap_or_clone(input),
            &own,
        )?));
    }
    Ok(Transformed::yes(LogicalPlan::Union(Union {
        inputs,
        schema: Arc::new(schema),
    })))
}

/// The join `INTERSECT` or `EXCEPT` is planned as: a left semi (anti) join
/// of every column, `NULL` equal to `NULL`, answering the left input.
pub(crate) fn is_set_operation(join: &datafusion::logical_expr::Join) -> bool {
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    let (left, right) = (join.left.schema(), join.right.schema());
    matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
        && join.null_equality == NullEquality::NullEqualsNull
        && join.filter.is_none()
        && join.on.len() == left.fields().len()
        && join.on.len() == right.fields().len()
        && join.on.iter().enumerate().all(|(i, (l, r))| {
            matches!(l, Expr::Column(c) if left.index_of_column(c).ok() == Some(i))
                && matches!(r, Expr::Column(c) if right.index_of_column(c).ok() == Some(i))
        })
}

/// `plan` with each column cast to its target type (same qualifier and
/// name), or `plan` itself when no type changes.
fn cast_columns(plan: Arc<LogicalPlan>, targets: &[Option<DataType>]) -> Result<Arc<LogicalPlan>> {
    if targets.iter().all(Option::is_none) {
        return Ok(plan);
    }
    let exprs: Vec<Expr> = plan
        .schema()
        .iter()
        .zip(targets)
        .map(|((qualifier, field), target)| {
            let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
            match target {
                Some(t) if t != field.data_type() => Expr::Cast(Cast::new(Box::new(column), t.clone()))
                    .alias_qualified(qualifier.cloned(), field.name()),
                _ => column,
            }
        })
        .collect();
    Ok(Arc::new(LogicalPlan::Projection(
        datafusion::logical_expr::Projection::try_new(exprs, plan)?,
    )))
}

/// One Arrow type for text of two different ones (`Utf8` of a hive table,
/// `Utf8View` of an in-memory one): left to type coercion, the key would
/// become a cast, and the join no longer one of columns (see
/// [`is_set_operation`]).
fn common_text_type(left: &DataType, right: &DataType) -> Option<DataType> {
    let text = |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
    if left == right || !text(left) || !text(right) {
        return None;
    }
    Some(if left == &DataType::Utf8View || right == &DataType::Utf8View {
        DataType::Utf8View
    } else {
        DataType::LargeUtf8
    })
}

/// `INTERSECT` and `EXCEPT` answer Trino's common type of each column, like
/// `UNION` (`decimal(5,1) INTERSECT decimal(6,2)` is `decimal(6,2)`,
/// `integer INTERSECT bigint` is `bigint`); `DataFusion` keeps the left
/// input's type and compares the two in their common type. Both inputs are
/// cast to the common type first.
fn unify_set_operation(join: datafusion::logical_expr::Join) -> Result<Transformed<LogicalPlan>> {
    let targets: Vec<Option<DataType>> = join
        .left
        .schema()
        .fields()
        .iter()
        .zip(join.right.schema().fields())
        .map(|(l, r)| {
            common_numeric_type(&[l.data_type().clone(), r.data_type().clone()])
                .or_else(|| common_text_type(l.data_type(), r.data_type()))
        })
        .collect();
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let left = cast_columns(Arc::clone(&join.left), &targets)?;
    let right = cast_columns(Arc::clone(&join.right), &targets)?;
    let on = join.on.clone();
    let plan = LogicalPlan::Join(datafusion::logical_expr::Join::try_new(
        left,
        right,
        on,
        None,
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?);
    Ok(Transformed::yes(plan))
}

/// A subquery's references to the columns of `outer` take their types,
/// which this rule may have changed (an integer literal of a `VALUES`).
fn refresh_outer_references(expr: Expr, outer: &DFSchema) -> Result<Transformed<Expr>> {
    use datafusion::logical_expr::Subquery;
    use datafusion::logical_expr::expr::{Exists, InSubquery};
    let refresh = |subquery: Subquery| -> Result<Subquery> {
        let fix = |e: Expr| -> Result<Transformed<Expr>> {
            e.transform_up(|e| {
                Ok(match e {
                    Expr::OuterReferenceColumn(field, column) => match outer
                        .field_with_name(column.relation.as_ref(), &column.name)
                    {
                        Ok(current) if current.data_type() != field.data_type() => {
                            Transformed::yes(Expr::OuterReferenceColumn(
                                Arc::new(field.as_ref().clone().with_data_type(current.data_type().clone())),
                                column,
                            ))
                        }
                        _ => Transformed::no(Expr::OuterReferenceColumn(field, column)),
                    },
                    other => Transformed::no(other),
                })
            })
        };
        let plan = Arc::unwrap_or_clone(subquery.subquery)
            .transform_up_with_subqueries(|node| {
                node.map_expressions(fix)?.map_data(LogicalPlan::recompute_schema)
            })?
            .data;
        let outer_ref_columns = subquery
            .outer_ref_columns
            .into_iter()
            .map(|e| fix(e).map(|t| t.data))
            .collect::<Result<Vec<_>>>()?;
        Ok(Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns,
            spans: subquery.spans,
        })
    };
    Ok(match expr {
        Expr::ScalarSubquery(subquery) if !subquery.outer_ref_columns.is_empty() => {
            Transformed::yes(Expr::ScalarSubquery(refresh(subquery)?))
        }
        Expr::InSubquery(InSubquery {
            expr: value,
            subquery,
            negated,
        }) if !subquery.outer_ref_columns.is_empty() => Transformed::yes(Expr::InSubquery(InSubquery::new(
            value,
            refresh(subquery)?,
            negated,
        ))),
        Expr::Exists(Exists { subquery, negated }) if !subquery.outer_ref_columns.is_empty() => {
            Transformed::yes(Expr::Exists(Exists::new(refresh(subquery)?, negated)))
        }
        other => Transformed::no(other),
    })
}

fn rewrite_expressions(plan: LogicalPlan, session_zone: &str) -> Result<Transformed<LogicalPlan>> {
    if matches!(plan, LogicalPlan::Limit(_)) {
        return Ok(Transformed::no(plan));
    }
    let schema = expression_schema(&plan)?;
    let names = NamePreserver::new(&plan);
    let projection = matches!(plan, LogicalPlan::Projection(_));
    plan.map_expressions(|expr| {
        let name = names.save(&expr);
        expr.transform_up(|e| {
            let interval = zoned_day_interval(e, &schema)?;
            let unified = common_types(interval.data, &schema, session_zone)?;
            let outer = refresh_outer_references(unified.data, &schema)?;
            Ok(Transformed::new_transformed(
                outer.data,
                interval.transformed || unified.transformed || outer.transformed,
            ))
        })
        .map(|t| {
            t.update_data(|e| {
                // Before the name is restored: the metadata is part of a
                // literal's name.
                let e = if projection { typed_text_literal(e) } else { e };
                name.restore(e)
            })
        })
    })
}

fn common_types_node(plan: LogicalPlan, session_zone: &str) -> Result<Transformed<LogicalPlan>> {
    let plan = match plan {
        LogicalPlan::Values(values) => {
            // A rewrite that preserves expression names (the function
            // library's) may alias a cell; names mean nothing in `VALUES`.
            let narrowed = LogicalPlan::Values(values).map_expressions(|cell| match cell {
                Expr::Alias(_) => Ok(Transformed::yes(cell.unalias())),
                other => Ok(Transformed::no(other)),
            })?;
            let LogicalPlan::Values(values) = narrowed.data else {
                return Ok(narrowed);
            };
            return retype_values(values);
        }
        LogicalPlan::Union(union) => return unify_union(union),
        LogicalPlan::Join(join) if is_set_operation(&join) => return unify_set_operation(join),
        dml @ LogicalPlan::Dml(_) => return crate::declared::check_insert(dml),
        other => other,
    };
    let rewritten = rewrite_expressions(plan, session_zone)?;
    // Types below may have changed: every schema is computed again.
    rewritten.map_data(LogicalPlan::recompute_schema)
}

/// Analyzer rule: see the module documentation.
#[derive(Debug, Default)]
pub(crate) struct TrinoCommonTypes;

impl AnalyzerRule for TrinoCommonTypes {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let session_zone = config.execution.time_zone.as_deref().unwrap_or("UTC");
        Ok(plan
            .transform_up_with_subqueries(|node| common_types_node(node, session_zone))?
            .data)
    }

    fn name(&self) -> &'static str {
        "codelake_trino_common_types"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The finest unit of a union's timestamps (ISS-032 item 8): only when
    /// an input or the union differs from it, only among timestamps of one
    /// kind, `NULL` inputs aside.
    #[test]
    fn unions_take_the_finest_timestamp() {
        let naive = |unit| DataType::Timestamp(unit, None);
        let utc = |unit, zone: &str| DataType::Timestamp(unit, Some(zone.into()));
        let (s, ms, us) = (TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond);
        assert_eq!(
            finest_timestamp(&[naive(ms), naive(s)], &naive(s)),
            Some(naive(ms))
        );
        assert_eq!(
            finest_timestamp(&[naive(ms), naive(s)], &naive(ms)),
            Some(naive(ms))
        );
        assert_eq!(
            finest_timestamp(&[naive(s), naive(us), naive(ms)], &naive(s)),
            Some(naive(us))
        );
        assert_eq!(
            finest_timestamp(&[naive(ms), naive(ms)], &naive(s)),
            Some(naive(ms))
        );
        assert_eq!(finest_timestamp(&[naive(ms), naive(ms)], &naive(ms)), None);
        assert_eq!(finest_timestamp(&[DataType::Null, naive(ms)], &naive(ms)), None);
        assert_eq!(
            finest_timestamp(&[DataType::Null, naive(ms)], &naive(s)),
            Some(naive(ms))
        );
        assert_eq!(
            finest_timestamp(&[utc(s, "UTC"), utc(us, "+00:00")], &utc(s, "UTC")),
            Some(utc(us, "UTC"))
        );
        assert_eq!(finest_timestamp(&[naive(ms), utc(s, "UTC")], &naive(ms)), None);
        assert_eq!(
            finest_timestamp(&[utc(ms, "UTC"), naive(s)], &utc(ms, "UTC")),
            None
        );
        assert_eq!(finest_timestamp(&[DataType::Int64, naive(ms)], &naive(ms)), None);
        assert_eq!(finest_timestamp(&[naive(ms), naive(s)], &DataType::Int64), None);
        assert_eq!(finest_timestamp(&[DataType::Null], &naive(s)), None);
    }

    #[test]
    fn common_numbers_follow_trino() {
        use DataType::{Decimal128 as D, Float32, Float64, Int8, Int16, Int32, Int64};
        let c = |a: DataType, b: DataType| common_number(&a, &b);
        assert_eq!(c(Int32, Int64), Some(Int64));
        assert_eq!(c(Int8, Int16), Some(Int16));
        assert_eq!(c(Int64, D(2, 1)), Some(D(20, 1)));
        assert_eq!(c(Int32, D(2, 1)), Some(D(11, 1)));
        assert_eq!(c(D(5, 1), D(10, 2)), Some(D(10, 2)));
        assert_eq!(c(D(38, 0), D(38, 37)), Some(D(38, 37)));
        assert_eq!(c(Float64, D(2, 1)), Some(Float64));
        assert_eq!(c(Float32, D(2, 1)), Some(Float32));
        assert_eq!(c(Float32, Int64), Some(Float32));
        assert_eq!(c(Float32, Float64), Some(Float64));
        assert_eq!(c(DataType::UInt64, Int64), None);
        assert_eq!(c(DataType::Utf8, Int64), None);
        assert_eq!(common_numeric_type(&[Int32, Int32]), None);
        assert_eq!(common_numeric_type(&[DataType::Null, Int32]), Some(Int32));
    }

    #[test]
    fn a_union_declares_the_number_its_inputs_agree_on() {
        use DataType::{Decimal128 as D, Float64, Int64, Utf8, Utf8View};
        assert_eq!(agreed_number(&[Float64, Float64], &D(35, 15)), Some(Float64));
        assert_eq!(agreed_number(&[D(20, 1), D(20, 1)], &D(21, 1)), Some(D(20, 1)));
        assert_eq!(agreed_number(&[DataType::Null, Int64], &D(20, 0)), Some(Int64));
        assert_eq!(agreed_number(&[Float64, Float64], &Float64), None);
        assert_eq!(agreed_number(&[Float64, D(2, 1)], &D(35, 15)), None);
        assert_eq!(agreed_number(&[Utf8, Utf8], &Utf8View), None);
        assert_eq!(agreed_number(&[DataType::UInt64, DataType::UInt64], &Int64), None);
    }
}
