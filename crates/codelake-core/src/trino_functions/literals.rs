//! Numeric literals with Trino's types.
//!
//! `DataFusion` reads `1` as `bigint` and `1.5` as `double`; Trino reads `1`
//! as `integer` (`bigint` only beyond 32 bits), `1.5` as `decimal(2,1)` and
//! only `1.5E0` as `double`. The difference is not cosmetic: `x / 3600.0` is a
//! decimal division rounded to one fractional digit in Trino, and
//! `0.1 + 0.2` is exactly `0.3`. Inside queries, literals are rewritten to
//! `CAST(1 AS integer)`, `CAST('1.5' AS decimal(2,1))` and
//! `CAST('1.5E0' AS double)`.
//!
//! Literals whose position means something else keep the parser's reading:
//! `ORDER BY 1` / `GROUP BY 1` (column positions), `LIMIT`, `OFFSET`,
//! `FETCH`, window frame bounds, window function arguments (`ntile(3)`,
//! `lag(x, 2)`) and interval values. DDL options outside queries are not
//! touched.

use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    CastKind, DataType, ExactNumberInfo, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Ident, LimitClause, NamedWindowExpr, OrderByKind, Query, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Value, ValueWithSpan, VisitMut, VisitorMut, WindowFrameBound, WindowSpec, WindowType,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

fn is_number(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: Value::Number(..),
            ..
        })
    )
}

fn cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        array: false,
        format: None,
    }
}

fn text(value: &str) -> Expr {
    Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(value.to_owned())))
}

/// The Trino-typed form of a numeric literal, if it changes.
fn typed(number: &str) -> Option<Expr> {
    if number.contains(['e', 'E']) {
        return Some(cast(text(number), DataType::Double(ExactNumberInfo::None)));
    }
    if number.contains('.') {
        let unsigned = number.strip_prefix('-').unwrap_or(number);
        let (int_part, fraction) = unsigned.split_once('.')?;
        let digits = int_part.trim_start_matches('0').len() + fraction.len();
        let scale = u64::try_from(fraction.len()).ok()?;
        let precision = u64::try_from(digits.max(1)).ok()?.max(scale);
        // Longer literals are not decimals in Trino either.
        if precision > 38 {
            return None;
        }
        let data_type = DataType::Decimal(ExactNumberInfo::PrecisionAndScale(
            precision,
            i64::try_from(scale).ok()?,
        ));
        return Some(cast(text(number), data_type));
    }
    let value: i64 = number.parse().ok()?;
    i32::try_from(value).ok()?;
    let literal = Expr::Value(ValueWithSpan::from(Value::Number(number.to_owned(), false)));
    Some(cast(literal, DataType::Integer(None)))
}

/// An integer literal Trino cannot read: beyond `bigint` (Trino fails with
/// `SYNTAX_ERROR`; the engine would read a `double` or an unsigned integer).
fn out_of_range_integer(number: &str) -> bool {
    let digits = number.strip_prefix('-').unwrap_or(number);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) && number.parse::<i64>().is_err()
}

/// Functions that read numeric literal arguments when planned
/// (`from_unixtime(t, hours, minutes)` takes its zone from them).
const LITERAL_ARGUMENTS: [&str; 1] = ["from_unixtime"];

#[derive(Debug, Default)]
struct LiteralTyper {
    /// Literals whose meaning is positional or structural.
    keep: HashSet<*const Expr>,
    /// The first integer literal out of the `bigint` range.
    invalid: Option<String>,
    /// Depth of queries being visited: only literals in queries are typed.
    queries: usize,
}

impl LiteralTyper {
    fn keep_if_number(&mut self, expr: &Expr) {
        let negative_number = matches!(
            expr,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } if is_number(inner)
        );
        if is_number(expr) || negative_number {
            self.keep.insert(std::ptr::from_ref(expr));
        }
    }

    fn keep_window(&mut self, spec: &WindowSpec) {
        if let Some(frame) = &spec.window_frame {
            for bound in std::iter::once(&frame.start_bound).chain(frame.end_bound.as_ref()) {
                if let WindowFrameBound::Preceding(Some(e)) | WindowFrameBound::Following(Some(e)) = bound {
                    self.keep.insert(std::ptr::from_ref(e.as_ref()));
                }
            }
        }
    }

    fn keep_positions(&mut self, body: &SetExpr) {
        match body {
            SetExpr::Select(select) => {
                if let GroupByExpr::Expressions(items, _) = &select.group_by {
                    for item in items {
                        self.keep_if_number(item);
                    }
                }
                for window in &select.named_window {
                    if let NamedWindowExpr::WindowSpec(spec) = &window.1 {
                        self.keep_window(spec);
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.keep_positions(left);
                self.keep_positions(right);
            }
            _ => {}
        }
    }
}

impl VisitorMut for LiteralTyper {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        self.queries += 1;
        if let Some(order_by) = &query.order_by
            && let OrderByKind::Expressions(items) = &order_by.kind
        {
            for item in items {
                self.keep_if_number(&item.expr);
            }
        }
        match &query.limit_clause {
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                for e in limit
                    .iter()
                    .chain(offset.as_ref().map(|o| &o.value))
                    .chain(limit_by)
                {
                    self.keep.insert(std::ptr::from_ref(e));
                }
            }
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                self.keep.insert(std::ptr::from_ref(offset));
                self.keep.insert(std::ptr::from_ref(limit));
            }
            None => {}
        }
        if let Some(quantity) = query.fetch.as_ref().and_then(|f| f.quantity.as_ref()) {
            self.keep.insert(std::ptr::from_ref(quantity));
        }
        self.keep_positions(&query.body);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<()> {
        self.queries -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        // `-<number>`: the sign belongs to the literal (`-2147483648` is an
        // `integer`, `-9223372036854775808` a `bigint`).
        let folded = match &*expr {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: inner,
            } => match inner.as_ref() {
                Expr::Value(ValueWithSpan {
                    value: Value::Number(number, long),
                    ..
                }) if !number.starts_with('-') => Some((format!("-{number}"), *long)),
                _ => None,
            },
            _ => None,
        };
        if let Some((number, long)) = folded
            && !self.keep.contains(&std::ptr::from_ref(&*expr))
        {
            *expr = Expr::Value(ValueWithSpan::from(Value::Number(number, long)));
        }
        match expr {
            // Functions whose result type depends on a numeric literal
            // argument read it while the query is planned, before casts fold.
            Expr::Function(function)
                if LITERAL_ARGUMENTS
                    .iter()
                    .any(|n| function.name.to_string().eq_ignore_ascii_case(n)) =>
            {
                if let FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.keep_if_number(e);
                        }
                    }
                }
            }
            Expr::Function(function) if function.over.is_some() => {
                if let FunctionArguments::List(list) = &function.args {
                    for arg in &list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.keep_if_number(e);
                        }
                    }
                }
                if let Some(WindowType::WindowSpec(spec)) = &function.over {
                    self.keep_window(spec);
                }
            }
            Expr::Interval(interval) => {
                self.keep.insert(std::ptr::from_ref(interval.value.as_ref()));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if self.queries == 0 || self.keep.contains(&std::ptr::from_ref(&*expr)) {
            return ControlFlow::Continue(());
        }
        if let Expr::Value(ValueWithSpan {
            value: Value::Number(number, _),
            ..
        }) = &*expr
        {
            if out_of_range_integer(number) {
                self.invalid.get_or_insert_with(|| number.clone());
            } else if let Some(replacement) = typed(number) {
                *expr = replacement;
            }
        }
        ControlFlow::Continue(())
    }
}

/// Trino's output names: an expression without an alias is `_col<i>` (its
/// position); a column reference keeps its name. `DataFusion` would name it
/// after its text and reject two items with the same text.
#[derive(Debug, Default)]
struct ColumnNamer;

fn name_select_items(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for (i, item) in select.projection.iter_mut().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item
                    && !matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                {
                    let expr = std::mem::replace(expr, Expr::Value(Value::Null.into()));
                    *item = SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::new(format!("_col{i}")),
                    };
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            name_select_items(left);
            name_select_items(right);
        }
        _ => {}
    }
}

impl VisitorMut for ColumnNamer {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        name_select_items(&mut query.body);
        ControlFlow::Continue(())
    }
}

/// Names unaliased output expressions `_col<i>`, as Trino does.
pub(crate) fn name_columns(statement: &mut Statement) {
    let _ = statement.visit(&mut ColumnNamer);
    if let Statement::Query(query) = statement {
        name_values_columns(&mut query.body);
    }
    let _ = statement.visit(&mut DerivedValuesNamer);
}

const VALUES_ALIAS: &str = "codelake_values";

/// `VALUES …` read as a query (not the rows of an `INSERT`): its columns are
/// `_col0`, `_col1`, … in Trino (`column1`, … in the engine). The body
/// becomes `SELECT column1 AS _col0, … FROM (VALUES …) AS codelake_values`.
fn name_values_columns(body: &mut SetExpr) {
    let SetExpr::Values(values) = &*body else {
        return;
    };
    let Some(width) = values.rows.first().map(Vec::len) else {
        return;
    };
    let items = (0..width)
        .map(|i| format!("column{} AS _col{i}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {items} FROM (VALUES (0)) AS {VALUES_ALIAS}");
    let Ok(mut parsed) = Parser::parse_sql(&GenericDialect {}, &sql) else {
        return;
    };
    let Some(Statement::Query(mut query)) = parsed.pop() else {
        return;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return;
    };
    let Some(TableFactor::Derived { subquery, .. }) = select.from.first_mut().map(|t| &mut t.relation) else {
        return;
    };
    std::mem::swap(subquery.body.as_mut(), body);
    std::mem::swap(body, query.body.as_mut());
}

/// Derived tables `(VALUES …) AS t` without column names get Trino's names.
#[derive(Debug)]
struct DerivedValuesNamer;

impl VisitorMut for DerivedValuesNamer {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Derived { subquery, alias, .. } = factor
            && alias
                .as_ref()
                .is_none_or(|a| a.columns.is_empty() && a.name.value != VALUES_ALIAS)
        {
            name_values_columns(&mut subquery.body);
        }
        ControlFlow::Continue(())
    }
}

/// Types the numeric literals of the queries in `statement` (module docs).
///
/// # Errors
/// `SYNTAX_ERROR` for an integer literal beyond the `bigint` range, as Trino.
pub(crate) fn type_literals(statement: &mut Statement) -> Result<()> {
    let mut typer = LiteralTyper::default();
    let _ = statement.visit(&mut typer);
    match typer.invalid {
        Some(number) => Err(DataFusionError::Plan(format!(
            "SYNTAX_ERROR: Invalid numeric literal: {number}"
        ))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed_sql(sql: &str) -> String {
        let mut statement = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
        type_literals(&mut statement).unwrap();
        statement.to_string()
    }

    #[test]
    fn literals_get_trino_types() {
        assert_eq!(
            typed_sql("SELECT 1, 3000000000, 1.50, 0.05, 1.5E0 FROM t"),
            "SELECT CAST(1 AS INTEGER), 3000000000, CAST('1.50' AS DECIMAL(3,2)), CAST('0.05' AS DECIMAL(2,2)), CAST('1.5E0' AS DOUBLE) FROM t"
        );
    }

    #[test]
    fn positional_and_structural_literals_are_kept() {
        assert_eq!(
            typed_sql("SELECT a, count(*) FROM t WHERE b > 2 GROUP BY 1 ORDER BY 2 DESC LIMIT 10 OFFSET 5"),
            "SELECT a, count(*) FROM t WHERE b > CAST(2 AS INTEGER) GROUP BY 1 ORDER BY 2 DESC LIMIT 10 OFFSET 5"
        );
        assert_eq!(
            typed_sql(
                "SELECT ntile(3) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING), x + INTERVAL '1' DAY FROM t"
            ),
            "SELECT ntile(3) OVER (ORDER BY x ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING), x + INTERVAL '1' DAY FROM t"
        );
        assert_eq!(
            typed_sql("SELECT * FROM (SELECT 1 UNION ALL SELECT 2) ORDER BY 1"),
            "SELECT * FROM (SELECT CAST(1 AS INTEGER) UNION ALL SELECT CAST(2 AS INTEGER)) ORDER BY 1"
        );
    }

    #[test]
    fn ddl_options_are_not_queries() {
        let sql = "CREATE TABLE t (a INT) WITH (bucket_count = 4)";
        let mut statement = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
        let before = statement.to_string();
        type_literals(&mut statement).unwrap();
        assert_eq!(statement.to_string(), before);
    }
}
