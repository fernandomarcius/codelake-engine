//! Trino's common types of numbers, dates and times.
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
//! Dates, timestamps and times meet in the highest precision among them
//! (`timestamp(3)` with `timestamp(0)` or a date is `timestamp(3)`, `time(3)`
//! with `time(0)` is `time(3)`), where `DataFusion` takes the coarser Arrow
//! unit and drops the fraction; a precision no Arrow unit has is declared on
//! the result (see [`common_temporal`]).
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

/// Name of [`ExactMicros`].
pub(crate) const EXACT_MICROS: &str = "codelake_exact_micros";

/// `codelake_exact_micros(x)`: nanosecond timestamps as microseconds, the
/// zone kept, failing on a value with a digit past the sixth (never rounded):
/// where nanoseconds meet a coarser timestamp, the common type is carried in
/// microseconds ([`common_temporal`], [`unify_timestamps`], ISS-062).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExactMicros {
    signature: Signature,
}

impl ScalarUDFImpl for ExactMicros {
    fn name(&self) -> &'static str {
        EXACT_MICROS
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [DataType::Timestamp(_, zone)] = arg_types else {
            return exec_err!("{EXACT_MICROS} expects one timestamp");
        };
        Ok(DataType::Timestamp(TimeUnit::Microsecond, zone.clone()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, AsArray};
        use datafusion::arrow::datatypes::{Int64Type, TimestampMicrosecondType};
        let [value] = args.args.as_slice() else {
            return exec_err!("{EXACT_MICROS} expects one argument");
        };
        let array = value.to_array(args.number_rows)?;
        let DataType::Timestamp(unit, zone) = array.data_type() else {
            return exec_err!("{EXACT_MICROS} expects one timestamp");
        };
        let per_micro: i64 = match unit {
            TimeUnit::Nanosecond => 1_000,
            _ => 1,
        };
        let raw = datafusion::arrow::compute::cast(&array, &DataType::Int64)?;
        let mut out = Vec::with_capacity(raw.len());
        for v in raw.as_primitive::<Int64Type>() {
            out.push(match v {
                None => None,
                Some(v) if v.rem_euclid(per_micro) == 0 => Some(v.div_euclid(per_micro)),
                Some(v) => {
                    let text = crate::timestamp_casts::timestamp_text(i128::from(v), 9)?;
                    return exec_err!(
                        "INVALID_CAST_ARGUMENT: TIMESTAMP '{text}' has more than 6 fraction digits where it meets \
                         values kept in microseconds (a nanosecond timestamp holds only the years 1677 to 2262); \
                         cast it to timestamp(6)"
                    );
                }
            });
        }
        let micros = datafusion::arrow::array::PrimitiveArray::<TimestampMicrosecondType>::from(out)
            .with_timezone_opt(zone.clone());
        let micros: datafusion::arrow::array::ArrayRef = Arc::new(micros);
        Ok(match value {
            ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::try_from_array(&micros, 0)?),
            ColumnarValue::Array(_) => ColumnarValue::Array(micros),
        })
    }
}

static EXACT_MICROS_UDF: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::new_from_impl(ExactMicros {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

/// The function [`cast_temporal`] calls; the engine registers it so plans
/// that carry it can be printed and planned again.
pub(crate) fn exact_micros() -> ScalarUDF {
    EXACT_MICROS_UDF.as_ref().clone()
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
///
/// A constant in nanoseconds (`TIMESTAMP '… .1234567'`) meeting a coarser
/// timestamp is compared in microseconds, exactly or refused
/// ([`ExactMicros`]): widening the other side to nanoseconds overflowed on
/// a `9999-12-31` sentinel, and a `datetime2(7)` column (read in
/// microseconds) answered no row where it should have one.
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
    let coarser = types
        .iter()
        .any(|t| matches!(t, DataType::Timestamp(unit, _) if *unit != TimeUnit::Nanosecond));
    // Every nanosecond operand a constant (no column).
    let only_constants = exprs.iter().zip(types).all(|(e, t)| {
        !matches!(t, DataType::Timestamp(TimeUnit::Nanosecond, _)) || e.column_refs().is_empty()
    });
    let narrowed = finest == TimeUnit::Nanosecond && coarser && only_constants;
    let (exprs, types, finest) = if narrowed {
        let (exprs, types): (Vec<Expr>, Vec<DataType>) = exprs
            .into_iter()
            .zip(types)
            .map(|(e, t)| match t {
                DataType::Timestamp(TimeUnit::Nanosecond, zone) => (
                    EXACT_MICROS_UDF.call(vec![e]),
                    DataType::Timestamp(TimeUnit::Microsecond, zone.clone()),
                ),
                other => (e, other.clone()),
            })
            .unzip();
        (exprs, std::borrow::Cow::Owned(types), TimeUnit::Microsecond)
    } else {
        (exprs, std::borrow::Cow::Borrowed(types), finest)
    };
    let types: &[DataType] = &types;
    let target = |t: &DataType| match t {
        DataType::Timestamp(_, None) if zoned => Some(DataType::Timestamp(finest, Some(session_zone.into()))),
        DataType::Timestamp(_, zone) => Some(DataType::Timestamp(finest, zone.clone())),
        _ => None,
    };
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

/// [`unify`] for values that become one result (`CASE`, `coalesce`, …):
/// dates and timestamps meet in their common type ([`common_temporal`]:
/// `CASE … THEN DATE '…' ELSE TIMESTAMP '… .123' END` is `timestamp(3)`,
/// where `DataFusion` answers nanoseconds, and a nanosecond value next to a
/// `datetime2` column is carried in microseconds).
fn unify_results(exprs: Vec<Expr>, schema: &DFSchema, session_zone: &str) -> Result<Transformed<Vec<Expr>>> {
    let fields = exprs
        .iter()
        .map(|e| e.to_field(schema).map(|(_, f)| f))
        .collect::<Result<Vec<_>>>()?;
    let own: Vec<&Field> = fields.iter().map(AsRef::as_ref).collect();
    let Some(target) = common_temporal(&own, None).filter(|t| matches!(t.data_type, DataType::Timestamp(..)))
    else {
        return unify(exprs, schema, session_zone);
    };
    Ok(Transformed::yes(
        exprs
            .into_iter()
            .zip(&own)
            .map(|(e, f)| {
                if f.data_type().is_null() {
                    e
                } else {
                    cast_temporal(e, f.data_type(), &target.data_type, session_zone)
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

/// `expr` declaring the precision [`temporal_result`] found, if any.
fn with_temporal(expr: Transformed<Expr>, declared: Option<Declared>) -> Transformed<Expr> {
    match declared {
        None => expr,
        Some(declared) => Transformed::yes(crate::declared::trino_typed().call(vec![
            expr.data,
            Expr::Literal(ScalarValue::Utf8(Some(typed_name(Some(declared)))), None),
        ])),
    }
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
            unify_results(branches, schema, session_zone)?.update_data(|mut branches| {
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
            unify_results(args, schema, session_zone)?.update_data(|args| func.call(args))
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
fn retype_values(values: Values, session_zone: &str) -> Result<Transformed<LogicalPlan>> {
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
        let own = cells
            .iter()
            .map(|row| row[j].to_field(&empty).map(|(_, f)| f))
            .collect::<Result<Vec<_>>>()?;
        let types: Vec<DataType> = own.iter().map(|f| f.data_type().clone()).collect();
        let numeric = common_numeric_type(&types);
        // Dates, timestamps and times: the type and the precision they
        // declare (`VALUES TIME '12:00:00.250'` is `time(3)`).
        let temporal = if numeric.is_none() {
            let own: Vec<&Field> = own.iter().map(AsRef::as_ref).collect();
            common_temporal(&own, Some(field.data_type()))
        } else {
            None
        };
        let target = numeric
            .or_else(|| temporal.as_ref().map(|t| t.data_type.clone()))
            .or_else(|| only_type(&types))
            .unwrap_or_else(|| field.data_type().clone());
        for ((row, cells), t) in rows.iter_mut().zip(&cells).zip(&types) {
            // A plain cast but where a local time meets a zone.
            row.push(cast_temporal(cells[j].clone(), t, &target, session_zone));
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
        let field = Field::new(field.name(), target, true).with_metadata(metadata);
        fields.push(Arc::new(match temporal {
            Some(t) => with_declared(&field, t.declared),
            None => field,
        }));
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

// ------------------------------------------------------- dates and times

/// Trino's common type of dates, timestamps or times where they meet: its
/// Arrow type and the `timestamp(p)`/`time(p)` it declares when its unit
/// holds more digits (see [`Declared`]).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Temporal {
    data_type: DataType,
    declared: Option<Declared>,
}

/// Fraction digits a field holds: the precision it declares, its unit's
/// otherwise; a date has none. `None` for anything else.
fn fraction_digits(field: &Field) -> Option<u8> {
    match (crate::trino_types::declared(field), field.data_type()) {
        (Some(Declared::Timestamp(p)), DataType::Timestamp(..))
        | (Some(Declared::Time(p)), DataType::Time32(_) | DataType::Time64(_)) => Some(p),
        (_, DataType::Timestamp(unit, _) | DataType::Time32(unit) | DataType::Time64(unit)) => {
            Some(unit_digits(*unit))
        }
        (_, DataType::Date32 | DataType::Date64) => Some(0),
        _ => None,
    }
}

/// Trino's common type of the values of `fields` (`NULL`s ignored) when
/// they are all dates, timestamps or times: the highest precision among
/// them, `timestamp(3)` for `timestamp(3)` with `timestamp(0)` or a date
/// (`DataFusion` takes the coarser unit and drops the fraction), `time(3)`
/// for `time(3)` with `time(0)`. A timestamp is carried by the smallest unit
/// holding its digits, but never in nanoseconds next to a coarser value
/// (ISS-062).
///
/// With a `timestamp with time zone` among them, every value is one: the
/// zone their values share, or `declared`'s (the zone `DataFusion` chose)
/// when they have several (Arrow has one zone per column, ADR 0008); a
/// timestamp without time zone is then the local time in the session zone
/// ([`cast_temporal`]). Times of different Arrow types are left as they
/// are.
fn common_temporal(fields: &[&Field], declared: Option<&DataType>) -> Option<Temporal> {
    let known: Vec<&Field> = fields
        .iter()
        .copied()
        .filter(|f| !f.data_type().is_null())
        .collect();
    let precision = known
        .iter()
        .map(|f| fraction_digits(f))
        .collect::<Option<Vec<u8>>>()?
        .into_iter()
        .max()?;
    let first = known.first()?.data_type();
    if matches!(first, DataType::Time32(_) | DataType::Time64(_)) {
        let (DataType::Time32(unit) | DataType::Time64(unit)) = first else {
            return None;
        };
        if !known.iter().all(|f| f.data_type() == first) {
            return None;
        }
        return Some(Temporal {
            data_type: first.clone(),
            declared: (precision != unit_digits(*unit)).then_some(Declared::Time(precision)),
        });
    }
    let mut zones: Vec<&Arc<str>> = Vec::new();
    for field in &known {
        match field.data_type() {
            DataType::Timestamp(_, zone) => zones.extend(zone),
            DataType::Date32 | DataType::Date64 => {}
            _ => return None,
        }
    }
    // Dates alone are left as they are.
    if !known
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Timestamp(..)))
    {
        return None;
    }
    let zone = match zones.as_slice() {
        [] => None,
        [first, rest @ ..] if rest.iter().all(|z| z == first) => Some(Arc::clone(first)),
        [first, ..] => match declared {
            Some(DataType::Timestamp(_, Some(chosen))) => Some(Arc::clone(chosen)),
            _ => Some(Arc::clone(first)),
        },
    };
    // Never widened to nanoseconds next to a coarser value: a nanosecond
    // timestamp holds only the years 1677 to 2262, and a `datetime2` or a
    // date column holds `9999-12-31` (an open-end sentinel). Microseconds
    // then, the nanosecond values exact or refused ([`cast_temporal`]).
    let nanos = known
        .iter()
        .all(|f| matches!(f.data_type(), DataType::Timestamp(TimeUnit::Nanosecond, _)));
    let precision = if nanos { precision } else { precision.min(6) };
    let unit = crate::declared::unit_for(precision);
    Some(Temporal {
        data_type: DataType::Timestamp(unit, zone),
        declared: (precision != unit_digits(unit)).then_some(Declared::Timestamp(precision)),
    })
}

/// `expr` of type `from` as a value of `to`'s Arrow type. A date or a
/// timestamp without time zone becoming one with a time zone is the local
/// time in the session zone, as Trino casts it (the engine's cast reads the
/// local time in the target zone, `crate::timestamp_casts`).
fn cast_temporal(expr: Expr, from: &DataType, to: &DataType, session_zone: &str) -> Expr {
    if from == to {
        return expr;
    }
    // Nanoseconds carried in microseconds: exact, or refused.
    if let (DataType::Timestamp(TimeUnit::Nanosecond, zone), DataType::Timestamp(unit, _)) = (from, to)
        && *unit != TimeUnit::Nanosecond
    {
        let micros = DataType::Timestamp(TimeUnit::Microsecond, zone.clone());
        return cast_temporal(EXACT_MICROS_UDF.call(vec![expr]), &micros, to, session_zone);
    }
    let local = matches!(
        from,
        DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64
    );
    let expr = match to {
        DataType::Timestamp(unit, Some(zone)) if local && zone.as_ref() != session_zone => {
            Expr::Cast(Cast::new(
                Box::new(expr),
                DataType::Timestamp(*unit, Some(session_zone.into())),
            ))
        }
        _ => expr,
    };
    Expr::Cast(Cast::new(Box::new(expr), to.clone()))
}

/// `expr` (of `field`) as a value of `to`, declaring `to`'s precision (or
/// none) like every other value it meets: a union answers the metadata of
/// all its inputs merged, so one stale `timestamp(1)` would round the
/// others.
fn as_temporal(expr: Expr, field: &Field, to: &Temporal, session_zone: &str) -> Expr {
    let cast = cast_temporal(expr, field.data_type(), &to.data_type, session_zone);
    if to.declared.is_none() && crate::trino_types::declared(field).is_none() {
        return cast;
    }
    crate::declared::trino_typed().call(vec![
        cast,
        Expr::Literal(ScalarValue::Utf8(Some(typed_name(to.declared))), None),
    ])
}

/// Whether a value of `field` must change to be one of `to`.
fn changes(field: &Field, to: &Temporal) -> bool {
    *field.data_type() != to.data_type || crate::trino_types::declared(field) != to.declared
}

/// Dates, timestamps and times of a `UNION` meet in Trino's common type
/// ([`common_temporal`]). Like [`unify_union_text`], the union's own schema
/// is retyped even where its inputs agree: `a UNION ALL b UNION ALL c` is a
/// union of `a UNION ALL b` and `c`, and the outer one would keep the
/// `timestamp(0)` `DataFusion` gave it, casting every input back to it.
fn unify_union_temporal(union: Union, session_zone: &str) -> Result<Transformed<Union>> {
    let targets: Vec<Option<Temporal>> = (0..union.schema.fields().len())
        .map(|j| {
            let fields: Vec<&Field> = union
                .inputs
                .iter()
                .map(|i| i.schema().field(j).as_ref())
                .collect();
            common_temporal(&fields, Some(union.schema.field(j).data_type()))
        })
        .collect();
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(union));
    }
    let fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = union
        .schema
        .iter()
        .zip(&targets)
        .map(|((qualifier, field), target)| match target {
            Some(t) => (
                qualifier.cloned(),
                Arc::new(with_declared(
                    &field.as_ref().clone().with_data_type(t.data_type.clone()),
                    t.declared,
                )),
            ),
            None => (qualifier.cloned(), Arc::clone(field)),
        })
        .collect();
    let schema = Arc::new(DFSchema::new_with_metadata(
        fields,
        union.schema.metadata().clone(),
    )?);
    let mut changed = schema.fields() != union.schema.fields();
    let mut inputs = Vec::with_capacity(union.inputs.len());
    for input in union.inputs {
        let retyped = input
            .schema()
            .fields()
            .iter()
            .zip(&targets)
            .any(|(field, target)| target.as_ref().is_some_and(|t| changes(field, t)));
        if !retyped {
            inputs.push(input);
            continue;
        }
        changed = true;
        let exprs: Vec<Expr> = input
            .schema()
            .iter()
            .zip(&targets)
            .map(|((qualifier, field), target)| {
                let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
                match target {
                    Some(t) => as_temporal(column, field, t, session_zone).alias(field.name()),
                    None => column,
                }
            })
            .collect();
        inputs.push(Arc::new(LogicalPlan::Projection(
            datafusion::logical_expr::Projection::try_new(exprs, input)?,
        )));
    }
    if !changed {
        return Ok(Transformed::no(Union {
            inputs,
            schema: union.schema,
        }));
    }
    Ok(Transformed::yes(Union { inputs, schema }))
}

/// A `CASE` or `coalesce`-like result of timestamps or times declares the
/// highest precision among its values (`CASE … THEN TIMESTAMP '… .1' ELSE
/// TIMESTAMP '… .12' END` is `timestamp(2)`, of `TIME` literals `time(p)`)
/// when its unit holds more digits; the expression itself names no type.
/// Its values meet in the finer unit through [`unify_timestamps`].
fn temporal_result(expr: &Expr, schema: &DFSchema) -> Result<Option<Declared>> {
    let values: Vec<&Expr> = match expr {
        Expr::Case(case) => case
            .when_then_expr
            .iter()
            .map(|(_, then)| then.as_ref())
            .chain(case.else_expr.as_deref())
            .collect(),
        Expr::ScalarFunction(ScalarFunction { func, args }) if unifies_arguments(func.name()) => {
            args.iter().collect()
        }
        _ => return Ok(None),
    };
    let fields = values
        .iter()
        .map(|v| v.to_field(schema).map(|(_, f)| f))
        .collect::<Result<Vec<_>>>()?;
    let fields: Vec<&Field> = fields.iter().map(AsRef::as_ref).collect();
    Ok(common_temporal(&fields, None).and_then(|t| t.declared))
}

fn unify_union(union: Union, session_zone: &str) -> Result<Transformed<LogicalPlan>> {
    let text = unify_union_text(union)?.transform_data(|union| unify_union_temporal(union, session_zone))?;
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

fn unify_union_numbers(union: Union) -> Result<Transformed<LogicalPlan>> {
    let columns = union.schema.fields().len();
    let mut targets: Vec<Option<DataType>> = Vec::with_capacity(columns);
    for j in 0..columns {
        let types: Vec<DataType> = union
            .inputs
            .iter()
            .map(|input| input.schema().field(j).data_type().clone())
            .collect();
        targets.push(
            common_numeric_type(&types).or_else(|| agreed_number(&types, union.schema.field(j).data_type())),
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

/// Where the columns of a set operation meet.
enum Meet {
    /// One Arrow type (numbers, text).
    Type(DataType),
    /// Dates, timestamps and times ([`common_temporal`]).
    Temporal(Temporal),
}

/// `plan` with each column cast to its target type (same qualifier and
/// name), or `plan` itself when no type changes.
fn cast_columns(
    plan: Arc<LogicalPlan>,
    targets: &[Option<Meet>],
    session_zone: &str,
) -> Result<Arc<LogicalPlan>> {
    let changed = plan
        .schema()
        .fields()
        .iter()
        .zip(targets)
        .any(|(field, target)| match target {
            Some(Meet::Type(t)) => t != field.data_type(),
            Some(Meet::Temporal(t)) => changes(field, t),
            None => false,
        });
    if !changed {
        return Ok(plan);
    }
    let exprs: Vec<Expr> = plan
        .schema()
        .iter()
        .zip(targets)
        .map(|((qualifier, field), target)| {
            let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
            let cast = match target {
                Some(Meet::Type(t)) => cast_if_needed(column, field.data_type(), t),
                Some(Meet::Temporal(t)) => as_temporal(column, field, t, session_zone),
                None => return column,
            };
            cast.alias_qualified(qualifier.cloned(), field.name())
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
fn unify_set_operation(
    join: datafusion::logical_expr::Join,
    session_zone: &str,
) -> Result<Transformed<LogicalPlan>> {
    let targets: Vec<Option<Meet>> = join
        .left
        .schema()
        .fields()
        .iter()
        .zip(join.right.schema().fields())
        .map(|(l, r)| {
            common_numeric_type(&[l.data_type().clone(), r.data_type().clone()])
                .or_else(|| common_text_type(l.data_type(), r.data_type()))
                .map(Meet::Type)
                .or_else(|| {
                    common_temporal(&[l.as_ref(), r.as_ref()], None)
                        .filter(|t| changes(l, t) || changes(r, t))
                        .map(Meet::Temporal)
                })
        })
        .collect();
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let left = cast_columns(Arc::clone(&join.left), &targets, session_zone)?;
    let right = cast_columns(Arc::clone(&join.right), &targets, session_zone)?;
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
            // Read before the values meet: a value cast to a finer unit no
            // longer tells its precision.
            let temporal = temporal_result(&interval.data, &schema)?;
            let unified = with_temporal(common_types(interval.data, &schema, session_zone)?, temporal);
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
            return retype_values(values, session_zone);
        }
        LogicalPlan::Union(union) => return unify_union(union, session_zone),
        LogicalPlan::Join(join) if is_set_operation(&join) => return unify_set_operation(join, session_zone),
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

    fn field(data_type: DataType, declared: Option<&str>) -> Field {
        let field = Field::new("x", data_type, true);
        match declared.and_then(Declared::parse) {
            Some(d) => field.with_metadata(d.metadata()),
            None => field,
        }
    }

    fn common(fields: &[Field], declared: Option<&DataType>) -> Option<Temporal> {
        let fields: Vec<&Field> = fields.iter().collect();
        common_temporal(&fields, declared)
    }

    fn stamp(unit: TimeUnit, zone: Option<&str>, declared: Option<Declared>) -> Temporal {
        Temporal {
            data_type: DataType::Timestamp(unit, zone.map(Into::into)),
            declared,
        }
    }

    #[test]
    fn timestamps_meet_in_the_highest_precision() {
        use DataType::{Date32, Int64, Null, Timestamp as Ts, Utf8};
        use TimeUnit::{Microsecond as Us, Millisecond as Ms, Nanosecond as Ns, Second as S};
        let f = |t: DataType| field(t, None);
        assert_eq!(
            common(&[f(Ts(Ms, None)), f(Ts(S, None))], None),
            Some(stamp(Ms, None, None))
        );
        assert_eq!(
            common(&[f(Ts(S, None)), f(Ts(Us, None))], Some(&Ts(S, None))),
            Some(stamp(Us, None, None))
        );
        assert_eq!(
            common(&[f(Ts(Ns, None)), f(Ts(Ms, None))], None),
            Some(stamp(Us, None, None))
        );
        // Inputs that agree, the union typed coarser when planned.
        assert_eq!(
            common(&[f(Ts(Ms, None)), f(Ts(Ms, None))], Some(&Ts(S, None))),
            Some(stamp(Ms, None, None))
        );
        assert_eq!(
            common(&[f(Ts(S, None)), f(Null), f(Ts(Ms, None))], None),
            Some(stamp(Ms, None, None))
        );
        assert_eq!(
            common(&[f(Date32), f(Ts(Ms, None))], None),
            Some(stamp(Ms, None, None))
        );
        assert_eq!(
            common(&[f(Date32), f(Ts(S, None))], None),
            Some(stamp(S, None, None))
        );
        // Dates alone, other types: nothing.
        assert_eq!(common(&[f(Date32), f(Date32)], None), None);
        assert_eq!(common(&[f(Ts(Ms, None)), f(Utf8)], None), None);
        assert_eq!(common(&[f(Ts(Ms, None)), f(Int64)], None), None);
        assert_eq!(common(&[f(Null)], None), None);
        // Declared precisions.
        let ms1 = field(Ts(Ms, None), Some("timestamp(1)"));
        let ms2 = field(Ts(Ms, None), Some("timestamp(2)"));
        let ns7 = field(Ts(Ns, None), Some("timestamp(7)"));
        let us4 = field(Ts(Us, None), Some("timestamp(4)"));
        assert_eq!(
            common(&[ms1.clone(), ms2], None),
            Some(stamp(Ms, None, Some(Declared::Timestamp(2))))
        );
        assert_eq!(
            common(&[ms1.clone(), f(Ts(S, None))], None),
            Some(stamp(Ms, None, Some(Declared::Timestamp(1))))
        );
        assert_eq!(
            common(&[ms1.clone(), f(Ts(Ms, None))], None),
            Some(stamp(Ms, None, None))
        );
        // Nanoseconds next to a coarser value: microseconds, never wider.
        assert_eq!(
            common(&[ns7.clone(), f(Ts(Ms, None))], None),
            Some(stamp(Us, None, None))
        );
        assert_eq!(
            common(&[ns7.clone(), f(Date32)], None),
            Some(stamp(Us, None, None))
        );
        // Nanoseconds only: the highest precision they declare.
        let ns8 = field(Ts(Ns, None), Some("timestamp(8)"));
        assert_eq!(
            common(&[ns7.clone(), ns8], None),
            Some(stamp(Ns, None, Some(Declared::Timestamp(8))))
        );
        assert_eq!(common(&[ns7, f(Ts(Ns, None))], None), Some(stamp(Ns, None, None)));
        assert_eq!(
            common(&[us4, f(Date32), f(Ts(Ms, None))], None),
            Some(stamp(Us, None, Some(Declared::Timestamp(4))))
        );
        assert_eq!(common(&[f(Ts(Ns, None)), ms1], None), Some(stamp(Us, None, None)));
    }

    #[test]
    fn nanoseconds_reach_microseconds_exactly_or_not_at_all() {
        use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::TimestampMicrosecondType;
        let call = |values: Vec<Option<i64>>| {
            let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"));
            let field = Arc::new(Field::new("x", array.data_type().clone(), true));
            exact_micros().invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(array)],
                arg_fields: vec![field],
                number_rows: 3,
                return_field: Arc::new(Field::new(
                    "r",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    true,
                )),
                config_options: Arc::new(ConfigOptions::default()),
            })
        };
        let Ok(ColumnarValue::Array(out)) = call(vec![Some(-2_000), None, Some(1_767_261_600_123_456_000)])
        else {
            panic!("exact values are converted");
        };
        assert_eq!(
            out.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let micros = out.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(
            micros.iter().collect::<Vec<_>>(),
            [Some(-2), None, Some(1_767_261_600_123_456)]
        );
        let error = call(vec![Some(0), Some(1_767_261_600_123_456_700), None])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("TIMESTAMP '2026-01-01 10:00:00.123456700' has more than 6 fraction digits"),
            "{error}"
        );
        assert!(call(vec![Some(-1), None, None]).is_err());
        assert_eq!(
            exact_micros()
                .return_type(&[DataType::Timestamp(TimeUnit::Nanosecond, None)])
                .unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(exact_micros().return_type(&[DataType::Int64]).is_err());
        assert_eq!(exact_micros().name(), "codelake_exact_micros");
    }

    #[test]
    fn zoned_timestamps_meet_in_one_zone() {
        use DataType::{Date32, Timestamp as Ts};
        use TimeUnit::{Millisecond as Ms, Second as S};
        let f = |t: DataType| field(t, None);
        let sp = || Some(Arc::<str>::from("America/Sao_Paulo"));
        let utc = || Some(Arc::<str>::from("UTC"));
        assert_eq!(
            common(&[f(Ts(Ms, sp())), f(Ts(S, sp()))], None),
            Some(stamp(Ms, Some("America/Sao_Paulo"), None))
        );
        assert_eq!(
            common(&[f(Ts(Ms, None)), f(Ts(S, sp())), f(Date32)], None),
            Some(stamp(Ms, Some("America/Sao_Paulo"), None))
        );
        // One zone, whatever the union was planned with.
        assert_eq!(
            common(&[f(Ts(Ms, sp())), f(Ts(S, sp()))], Some(&Ts(S, utc()))),
            Some(stamp(Ms, Some("America/Sao_Paulo"), None))
        );
        // Several zones: the one the union was planned with, else the first.
        assert_eq!(
            common(&[f(Ts(Ms, utc())), f(Ts(S, sp()))], Some(&Ts(S, sp()))),
            Some(stamp(Ms, Some("America/Sao_Paulo"), None))
        );
        assert_eq!(
            common(&[f(Ts(Ms, utc())), f(Ts(S, sp()))], Some(&Ts(S, None))),
            Some(stamp(Ms, Some("UTC"), None))
        );
        assert_eq!(
            common(&[f(Ts(Ms, utc())), f(Ts(S, sp()))], None),
            Some(stamp(Ms, Some("UTC"), None))
        );
    }

    #[test]
    fn times_meet_in_the_highest_declared_precision() {
        use DataType::{Time32, Time64, Timestamp as Ts};
        use TimeUnit::{Millisecond as Ms, Nanosecond as Ns};
        let t3 = field(Time64(Ns), Some("time(3)"));
        let t0 = field(Time64(Ns), Some("time(0)"));
        let time = |declared| {
            Some(Temporal {
                data_type: Time64(Ns),
                declared,
            })
        };
        assert_eq!(
            common(&[t3.clone(), t0.clone()], None),
            time(Some(Declared::Time(3)))
        );
        assert_eq!(
            common(&[t0.clone(), t0.clone()], None),
            time(Some(Declared::Time(0)))
        );
        assert_eq!(common(&[t0.clone(), field(Time64(Ns), None)], None), time(None));
        // Times of different units, a time with a timestamp: nothing.
        assert_eq!(common(&[t0.clone(), field(Time32(Ms), None)], None), None);
        assert_eq!(common(&[t3, field(Ts(Ms, None), None)], None), None);
        assert_eq!(common(&[field(Ts(Ms, None), None), t0], None), None);
    }

    #[test]
    fn a_value_changes_when_its_type_or_declaration_differs() {
        use DataType::Timestamp as Ts;
        use TimeUnit::Millisecond as Ms;
        let to = Temporal {
            data_type: Ts(Ms, None),
            declared: Some(Declared::Timestamp(2)),
        };
        assert!(!changes(&field(Ts(Ms, None), Some("timestamp(2)")), &to));
        assert!(changes(&field(Ts(Ms, None), Some("timestamp(1)")), &to));
        assert!(changes(&field(Ts(Ms, None), None), &to));
        assert!(changes(
            &field(Ts(TimeUnit::Second, None), Some("timestamp(2)")),
            &to
        ));
    }

    #[test]
    fn a_local_time_meeting_a_zone_is_read_in_the_session_zone() {
        use DataType::{Date32, Timestamp as Ts};
        use TimeUnit::Millisecond as Ms;
        let column = || Expr::Column(datafusion::common::Column::from_name("x"));
        let cast = |e: Expr, t: DataType| Expr::Cast(Cast::new(Box::new(e), t));
        let utc = Ts(Ms, Some("UTC".into()));
        let session = Ts(Ms, Some("America/Sao_Paulo".into()));
        assert_eq!(cast_temporal(column(), &utc, &utc, "America/Sao_Paulo"), column());
        assert_eq!(
            cast_temporal(column(), &Ts(Ms, None), &utc, "America/Sao_Paulo"),
            cast(cast(column(), session.clone()), utc.clone())
        );
        assert_eq!(
            cast_temporal(column(), &Date32, &utc, "America/Sao_Paulo"),
            cast(cast(column(), session.clone()), utc.clone())
        );
        assert_eq!(
            cast_temporal(column(), &Ts(Ms, None), &session, "America/Sao_Paulo"),
            cast(column(), session.clone())
        );
        assert_eq!(
            cast_temporal(
                column(),
                &Ts(TimeUnit::Second, Some("America/Sao_Paulo".into())),
                &utc,
                "America/Sao_Paulo"
            ),
            cast(column(), utc)
        );
        assert_eq!(
            cast_temporal(column(), &Ts(TimeUnit::Second, None), &Ts(Ms, None), "UTC"),
            cast(column(), Ts(Ms, None))
        );
        // Nanoseconds to microseconds exactly, then the zone.
        let micros = |zone: Option<&str>| Ts(TimeUnit::Microsecond, zone.map(Into::into));
        let exact = || EXACT_MICROS_UDF.call(vec![column()]);
        assert_eq!(
            cast_temporal(column(), &Ts(TimeUnit::Nanosecond, None), &micros(None), "UTC"),
            exact()
        );
        assert_eq!(
            cast_temporal(
                column(),
                &Ts(TimeUnit::Nanosecond, None),
                &micros(Some("UTC")),
                "America/Sao_Paulo"
            ),
            cast(
                cast(exact(), micros(Some("America/Sao_Paulo"))),
                micros(Some("UTC"))
            )
        );
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
