//! Trino's common types of numbers.
//!
//! Where values of different types meet — the rows of `VALUES`, the inputs
//! of `UNION`, the branches of `CASE`, the arguments of `coalesce`,
//! `nullif`, `greatest` and `least`, an `IN` list, `BETWEEN` and a comparison
//! — Trino 431 converts every value to one common type. For numbers:
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
//! Integer literals take Trino's type too: `integer` when the value fits 32
//! bits, `bigint` otherwise (`DataFusion` reads every one as `bigint`), except
//! as a direct argument of an aggregate or window function, where
//! `DataFusion` expects its own type (`ntile(4)`, `lag(x, 1)`).
//!
//! `DataFusion` settles the type of a `VALUES` column when it plans the SQL,
//! before any analyzer rule. [`mark_values`] wraps every value of a `VALUES`
//! list in `codelake_values_cell(…)` (an identity), so the rule can tell the
//! values as written from the casts `DataFusion` added around them, and
//! types the column again. The `VALUES` of an `INSERT` is left alone: the
//! table's types decide there.

use std::any::Any;
use std::ops::ControlFlow;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::expr::{AggregateFunction, InList, ScalarFunction, WindowFunction};
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
const VALUES_CELL: &str = "codelake_values_cell";

/// `codelake_values_cell(x)`: `x` (see the module documentation).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ValuesCell {
    signature: Signature,
}

impl ScalarUDFImpl for ValuesCell {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    let known =
        |t: &DataType| integer_rank(t).is_some() || is_float(t) || (is_decimal(t) && as_decimal(t).is_some());
    if !known(a) || !known(b) {
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

/// Casts every expression to Trino's common type of their numbers.
fn unify(exprs: Vec<Expr>, schema: &DFSchema) -> Result<Transformed<Vec<Expr>>> {
    let types = exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let Some(common) = common_numeric_type(&types) else {
        return Ok(Transformed::no(exprs));
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

/// Functions whose arguments share one type.
fn unifies_arguments(name: &str) -> bool {
    matches!(name, "coalesce" | "greatest" | "least" | "nvl" | "ifnull")
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

/// Every construct of one expression node where values meet (see the module
/// documentation).
fn common_types(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
            unify(vec![*left, *right], schema)?.update_data(|mut pair| {
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
            unify(branches, schema)?.update_data(|mut branches| {
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
            unify(args, schema)?.update_data(|args| func.call(args))
        }
        Expr::InList(InList {
            expr: value,
            list,
            negated,
        }) => {
            let mut all = vec![*value];
            all.extend(list);
            unify(all, schema)?.update_data(|mut all| {
                let value = all.remove(0);
                Expr::InList(InList::new(Box::new(value), all, negated))
            })
        }
        Expr::Between(Between {
            expr: value,
            negated,
            low,
            high,
        }) => unify(vec![*value, *low, *high], schema)?.update_data(|mut all| {
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

/// A 32-bit integer literal → `integer` (see the module documentation).
fn narrow_literal(expr: Expr) -> Transformed<Expr> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), metadata) if i32::try_from(v).is_ok() => {
            Transformed::yes(Expr::Literal(ScalarValue::Int32(i32::try_from(v).ok()), metadata))
        }
        // Aggregate and window functions keep their own literal arguments.
        Expr::AggregateFunction(AggregateFunction { func, mut params }) => {
            params.args = params.args.into_iter().map(widen_literal).collect();
            Transformed::yes(Expr::AggregateFunction(AggregateFunction { func, params }))
        }
        Expr::WindowFunction(mut window) => {
            let WindowFunction { params, .. } = window.as_mut();
            params.args = std::mem::take(&mut params.args)
                .into_iter()
                .map(widen_literal)
                .collect();
            Transformed::yes(Expr::WindowFunction(window))
        }
        other => Transformed::no(other),
    }
}

fn widen_literal(expr: Expr) -> Expr {
    match expr {
        Expr::Literal(ScalarValue::Int32(v), metadata) => {
            Expr::Literal(ScalarValue::Int64(v.map(i64::from)), metadata)
        }
        other => other,
    }
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

/// Text columns of a `UNION` declare one type in every input (`SELECT 'a'
/// UNION ALL SELECT 'bcd'` is `varchar(3)`); `DataFusion` would keep the
/// last input's.
fn unify_union_text(union: Union) -> Result<Transformed<Union>> {
    let columns = union.schema.fields().len();
    let mut targets: Vec<Option<Option<Declared>>> = Vec::with_capacity(columns);
    for j in 0..columns {
        let fields: Vec<&Field> = union
            .inputs
            .iter()
            .map(|i| i.schema().field(j).as_ref())
            .collect();
        if !fields.iter().all(|f| is_text_type(f.data_type())) {
            targets.push(None);
            continue;
        }
        let declared: Vec<Option<Declared>> =
            fields.iter().map(|f| crate::trino_types::declared(f)).collect();
        let common = common_text(&declared);
        targets.push(declared.iter().any(|d| *d != common).then_some(common));
    }
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(union));
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
    let first = Arc::clone(&inputs[0]);
    let fields: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = union
        .schema
        .iter()
        .zip(first.schema().fields())
        .map(|((qualifier, field), typed)| {
            let metadata = typed.metadata().clone();
            (
                qualifier.cloned(),
                Arc::new(field.as_ref().clone().with_metadata(metadata)),
            )
        })
        .collect();
    let schema = DFSchema::new_with_metadata(fields, union.schema.metadata().clone())?;
    Ok(Transformed::yes(Union {
        inputs,
        schema: Arc::new(schema),
    }))
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

fn unify_union_numbers(union: Union) -> Result<Transformed<LogicalPlan>> {
    let columns = union.schema.fields().len();
    let mut targets: Vec<Option<DataType>> = Vec::with_capacity(columns);
    for j in 0..columns {
        let types: Vec<DataType> = union
            .inputs
            .iter()
            .map(|input| input.schema().field(j).data_type().clone())
            .collect();
        targets.push(common_numeric_type(&types));
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
            let zoned = crate::timestamp_casts::at_time_zone(e, &schema, session_zone)?;
            let function = crate::trino_functions::trino_function(zoned.data, &schema, session_zone)?;
            let narrowed = narrow_literal(function.data);
            let unified = common_types(narrowed.data, &schema)?;
            let outer = refresh_outer_references(unified.data, &schema)?;
            Ok(Transformed::new_transformed(
                outer.data,
                zoned.transformed
                    || function.transformed
                    || narrowed.transformed
                    || unified.transformed
                    || outer.transformed,
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
            let narrowed = LogicalPlan::Values(values).map_expressions(|cell| {
                // Only the values as written, inside their marks.
                if is_values_cell(&cell)
                    || matches!(&cell, Expr::Cast(Cast { expr, .. }) if is_values_cell(expr))
                {
                    cell.transform_up(|e| Ok(narrow_literal(e)))
                } else {
                    Ok(Transformed::no(cell))
                }
            })?;
            let LogicalPlan::Values(values) = narrowed.data else {
                return Ok(narrowed);
            };
            return retype_values(values);
        }
        LogicalPlan::Union(union) => return unify_union(union),
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
}
