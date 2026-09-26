//! Trino 431's reading of SQL text where the parser reads it differently.
//!
//! - An integer literal that does not fit `bigint` is a syntax error in Trino
//!   (`Invalid numeric literal: 9223372036854775808`; the smallest `bigint`,
//!   `-9223372036854775808`, is valid); `DataFusion` reads it as a decimal.
//! - A timestamp precision above 12 is an unknown type in Trino
//!   (`TYPE_MISMATCH`, `Unknown type: timestamp(13)`), not a feature the
//!   engine lacks. [`check_statement`] rejects both before planning.
//! - `IS [NOT] DISTINCT FROM` binds tighter than `AND` and `OR` in Trino (and
//!   PostgreSQL): `a IS NOT DISTINCT FROM b AND c` is `(a IS NOT DISTINCT
//!   FROM b) AND c`. The parser reads the whole rest of the expression as the
//!   right operand, `a IS NOT DISTINCT FROM (b AND c)`: a join condition
//!   `ON a.flag IS NOT DISTINCT FROM b.flag AND a.id = b.id` answered other
//!   rows, and with a non-boolean `b` the statement failed.
//!   [`regroup_distinct_from`] gives the operator its operand back.
//! - `INSERT INTO t SELECT a, a FROM u` inserts by position in Trino;
//!   `DataFusion` refuses a projection that repeats a name ("Projections
//!   require unique expression names"). [`name_insert_columns`] names the
//!   items of such a select list.
//!
//! The case of identifiers is folded for every client protocol by
//! [`crate::identifier_case`].

use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, DataType, Expr, Statement as SqlStatement, TimezoneInfo, UnaryOperator, Value,
    ValueWithSpan, visit_expressions, visit_expressions_mut,
};
use datafusion::sql::sqlparser::parser::ParserError;

/// Largest timestamp precision Trino has (picoseconds).
const MAX_TIMESTAMP_PRECISION: u64 = 12;

fn invalid_literal(text: &str) -> DataFusionError {
    DataFusionError::SQL(
        Box::new(ParserError::ParserError(format!(
            "Invalid numeric literal: {text}"
        ))),
        None,
    )
}

/// An integer literal as written (digits only).
fn integer_text(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::Number(text, _),
            ..
        }) if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) => Some(text),
        _ => None,
    }
}

fn unknown_timestamp(precision: u64, tz: TimezoneInfo) -> DataFusionError {
    let zone = if matches!(tz, TimezoneInfo::WithTimeZone | TimezoneInfo::Tz) {
        " with time zone"
    } else {
        ""
    };
    DataFusionError::Plan(format!("Unknown type: timestamp({precision}){zone}"))
}

fn check_type(data_type: &DataType) -> Result<(), DataFusionError> {
    match data_type {
        DataType::Timestamp(Some(p), tz) if *p > MAX_TIMESTAMP_PRECISION => Err(unknown_timestamp(*p, *tz)),
        _ => Ok(()),
    }
}

/// Rejects what Trino rejects before planning (see the module
/// documentation).
pub(crate) fn check_statement(statement: &DFStatement) -> Result<(), DataFusionError> {
    let DFStatement::Statement(inner) = statement else {
        return Ok(());
    };
    if let SqlStatement::CreateTable(create) = inner.as_ref() {
        for column in &create.columns {
            check_type(&column.data_type)?;
        }
    }
    // Literals already checked (the operand of a unary minus) or exempt.
    let mut negated: HashSet<usize> = HashSet::new();
    let flow = visit_expressions(inner.as_ref(), |expr| {
        let checked = match expr {
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: operand,
            } if !negated.contains(&(std::ptr::from_ref::<Expr>(expr) as usize)) => {
                match integer_text(operand) {
                    Some(text) => {
                        negated.insert(std::ptr::from_ref::<Expr>(operand) as usize);
                        format!("-{text}")
                            .parse::<i64>()
                            .map(|_| ())
                            .map_err(|_| invalid_literal(&format!("-{text}")))
                    }
                    None => Ok(()),
                }
            }
            Expr::Cast { data_type, .. } => check_type(data_type),
            // `arrow_cast(<literal>, '<Arrow type>')`, an engine extension,
            // reads its literal as the Arrow type it names (`UInt64`).
            Expr::Function(function) if function.name.to_string().eq_ignore_ascii_case("arrow_cast") => {
                if let datafusion::sql::sqlparser::ast::FunctionArguments::List(list) = &function.args
                    && let Some(datafusion::sql::sqlparser::ast::FunctionArg::Unnamed(
                        datafusion::sql::sqlparser::ast::FunctionArgExpr::Expr(first),
                    )) = list.args.first()
                {
                    let literal = match first {
                        Expr::UnaryOp { expr: operand, .. } => operand.as_ref(),
                        other => other,
                    };
                    negated.insert(std::ptr::from_ref::<Expr>(literal) as usize);
                    negated.insert(std::ptr::from_ref::<Expr>(first) as usize);
                }
                Ok(())
            }
            other => match integer_text(other) {
                Some(text) if !negated.contains(&(std::ptr::from_ref::<Expr>(other) as usize)) => {
                    text.parse::<i64>().map(|_| ()).map_err(|_| invalid_literal(text))
                }
                _ => Ok(()),
            },
        };
        match checked {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    });
    match flow {
        ControlFlow::Break(error) => Err(error),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// `statement` with the operands of its `IS [NOT] DISTINCT FROM` regrouped
/// (see [`regroup_distinct_from`]), or `None` when they need no change. For
/// the PostgreSQL wire, whose statements come parsed.
pub fn regrouped_statement(statement: &SqlStatement) -> Option<SqlStatement> {
    let mut regrouped = DFStatement::Statement(Box::new(statement.clone()));
    regroup_distinct_from(&mut regrouped);
    match regrouped {
        DFStatement::Statement(regrouped) if *regrouped != *statement => Some(*regrouped),
        _ => None,
    }
}

/// Regroups the operands of every `IS [NOT] DISTINCT FROM` (see the module
/// documentation). A parenthesized operand is left as written.
pub fn regroup_distinct_from(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let _ = visit_expressions_mut(inner.as_mut(), |expr| {
        if is_operator(expr) && holds_distinct_from(expr) {
            let taken = std::mem::replace(expr, Expr::Value(Value::Null.into()));
            let mut pieces = Vec::new();
            flatten(taken, &mut pieces);
            pieces.reverse();
            *expr = regroup(&mut pieces, 0);
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Precedences of the operators the parser can misgroup around `IS [NOT]
/// DISTINCT FROM` (the parser's own values): anything else is an operand.
const OR: u8 = 5;
const AND: u8 = 10;
const NOT: u8 = 15;
const IS: u8 = 17;

/// Builds an `IS NULL`, `IS TRUE`, … node around its operand.
type Postfix = fn(Box<Expr>) -> Expr;

/// The source order of an operator tree: operands and operators as written.
enum Piece {
    Operand(Box<Expr>),
    /// `AND`, `OR`, `IS DISTINCT FROM` (false) or `IS NOT DISTINCT FROM` (true).
    Binary(Option<BinaryOperator>, bool),
    /// `IS NULL`, `IS TRUE`, …
    Postfix(Postfix),
    Not,
}

fn is_operator(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp {
            op: BinaryOperator::And | BinaryOperator::Or,
            ..
        } | Expr::IsDistinctFrom(..)
            | Expr::IsNotDistinctFrom(..)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::UnaryOp {
                op: UnaryOperator::Not,
                ..
            }
    )
}

/// An `IS [NOT] DISTINCT FROM` in the operator tree of `expr` (operands
/// excluded: they are regrouped on their own).
fn holds_distinct_from(expr: &Expr) -> bool {
    match expr {
        Expr::IsDistinctFrom(..) | Expr::IsNotDistinctFrom(..) => true,
        Expr::BinaryOp { left, right, .. } if is_operator(expr) => {
            holds_distinct_from(left) || holds_distinct_from(right)
        }
        Expr::IsNull(x)
        | Expr::IsNotNull(x)
        | Expr::IsTrue(x)
        | Expr::IsNotTrue(x)
        | Expr::IsFalse(x)
        | Expr::IsNotFalse(x)
        | Expr::IsUnknown(x)
        | Expr::IsNotUnknown(x)
        | Expr::UnaryOp { expr: x, .. }
            if is_operator(expr) =>
        {
            holds_distinct_from(x)
        }
        _ => false,
    }
}

fn flatten(expr: Expr, pieces: &mut Vec<Piece>) {
    let postfix: (Box<Expr>, Postfix) = match expr {
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            right,
        } => {
            flatten(*left, pieces);
            pieces.push(Piece::Binary(Some(op), false));
            return flatten(*right, pieces);
        }
        Expr::IsDistinctFrom(left, right) => {
            flatten(*left, pieces);
            pieces.push(Piece::Binary(None, false));
            return flatten(*right, pieces);
        }
        Expr::IsNotDistinctFrom(left, right) => {
            flatten(*left, pieces);
            pieces.push(Piece::Binary(None, true));
            return flatten(*right, pieces);
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            pieces.push(Piece::Not);
            return flatten(*expr, pieces);
        }
        Expr::IsNull(x) => (x, Expr::IsNull),
        Expr::IsNotNull(x) => (x, Expr::IsNotNull),
        Expr::IsTrue(x) => (x, Expr::IsTrue),
        Expr::IsNotTrue(x) => (x, Expr::IsNotTrue),
        Expr::IsFalse(x) => (x, Expr::IsFalse),
        Expr::IsNotFalse(x) => (x, Expr::IsNotFalse),
        Expr::IsUnknown(x) => (x, Expr::IsUnknown),
        Expr::IsNotUnknown(x) => (x, Expr::IsNotUnknown),
        operand => {
            pieces.push(Piece::Operand(Box::new(operand)));
            return;
        }
    };
    let (operand, build) = postfix;
    flatten(*operand, pieces);
    pieces.push(Piece::Postfix(build));
}

fn binding(piece: &Piece) -> Option<u8> {
    match piece {
        Piece::Binary(Some(BinaryOperator::Or), _) => Some(OR),
        Piece::Binary(Some(_), _) => Some(AND),
        Piece::Binary(None, _) | Piece::Postfix(_) => Some(IS),
        Piece::Not | Piece::Operand(_) => None,
    }
}

/// Precedence climbing over `pieces` (in reverse source order), as the
/// parser does for every other operator: an operator binds while its
/// precedence is above `floor`, all of them left to right.
fn regroup(pieces: &mut Vec<Piece>, floor: u8) -> Expr {
    let mut left = match pieces.pop() {
        Some(Piece::Not) => Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(regroup(pieces, NOT)),
        },
        Some(Piece::Operand(operand)) => *operand,
        // `flatten` starts and follows every operator with an operand.
        _ => Expr::Value(Value::Null.into()),
    };
    while let Some(precedence) = pieces.last().and_then(binding) {
        if precedence <= floor {
            break;
        }
        left = match pieces.pop() {
            Some(Piece::Binary(Some(op), _)) => Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(regroup(pieces, precedence)),
            },
            Some(Piece::Binary(None, true)) => {
                Expr::IsNotDistinctFrom(Box::new(left), Box::new(regroup(pieces, precedence)))
            }
            Some(Piece::Binary(None, false)) => {
                Expr::IsDistinctFrom(Box::new(left), Box::new(regroup(pieces, precedence)))
            }
            Some(Piece::Postfix(build)) => build(Box::new(left)),
            _ => left,
        };
    }
    left
}

/// The name `DataFusion` gives a select item without an alias, as far as
/// telling two apart goes (a column's last name part, case folded).
fn item_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.to_lowercase(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map_or_else(String::new, |ident| ident.value.to_lowercase()),
        other => other.to_string().to_lowercase(),
    }
}

fn name_select_items(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            let mut seen = HashSet::new();
            let repeats = select
                .projection
                .iter()
                .filter_map(|item| match item {
                    SelectItem::UnnamedExpr(expr) => Some(item_name(expr)),
                    SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_lowercase()),
                    _ => None,
                })
                .any(|name| !seen.insert(name));
            if !repeats {
                return;
            }
            for (i, item) in select.projection.iter_mut().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item {
                    let expr = std::mem::replace(expr, Expr::value(Value::Null));
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
        SetExpr::Query(query) => name_select_items(query.body.as_mut()),
        _ => {}
    }
}

/// `INSERT … SELECT` whose select list repeats a name (`SELECT id, s, s`):
/// its items without an alias are named `_col<i>`, as the reference inserts
/// by position whatever the names are.
pub(crate) fn name_insert_columns(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let SqlStatement::Insert(insert) = inner.as_mut() else {
        return;
    };
    if let Some(source) = insert.source.as_mut() {
        name_select_items(source.body.as_mut());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::SetExpr;

    /// The tree of `e` with every operator in parentheses.
    fn shape(e: &Expr) -> String {
        match e {
            Expr::BinaryOp { left, op, right } => format!("({} {op} {})", shape(left), shape(right)),
            Expr::IsDistinctFrom(l, r) => format!("({} IDF {})", shape(l), shape(r)),
            Expr::IsNotDistinctFrom(l, r) => format!("({} INDF {})", shape(l), shape(r)),
            Expr::IsNull(x) => format!("({} IS NULL)", shape(x)),
            Expr::Nested(x) => format!("[{}]", shape(x)),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => format!("(NOT {})", shape(expr)),
            other => other.to_string(),
        }
    }

    fn regrouped(sql: &str) -> String {
        let mut statement = datafusion::sql::parser::DFParser::parse_sql(sql)
            .unwrap()
            .pop_front()
            .unwrap();
        regroup_distinct_from(&mut statement);
        let DFStatement::Statement(inner) = &statement else {
            panic!("not a statement")
        };
        let SqlStatement::Query(query) = inner.as_ref() else {
            panic!("not a query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("not a select")
        };
        match select.selection.as_ref() {
            Some(selection) => shape(selection),
            None => panic!("no WHERE"),
        }
    }

    /// Trino 431 and PostgreSQL group `IS [NOT] DISTINCT FROM` tighter than
    /// `AND`/`OR`, left to right with the other `IS` forms.
    #[test]
    fn distinct_from_binds_tighter_than_and_or() {
        for (condition, expected) in [
            ("a IS NOT DISTINCT FROM b AND c", "((a INDF b) AND c)"),
            ("a IS DISTINCT FROM b OR c AND d", "((a IDF b) OR (c AND d))"),
            ("a IS DISTINCT FROM b AND c OR d", "(((a IDF b) AND c) OR d)"),
            ("a IS DISTINCT FROM b IS NULL", "((a IDF b) IS NULL)"),
            ("a IS DISTINCT FROM b IS DISTINCT FROM c", "((a IDF b) IDF c)"),
            (
                "a IS NOT DISTINCT FROM b AND c IS DISTINCT FROM d",
                "((a INDF b) AND (c IDF d))",
            ),
            ("x AND a IS DISTINCT FROM b OR c", "((x AND (a IDF b)) OR c)"),
            ("NOT a IS DISTINCT FROM b AND c", "((NOT (a IDF b)) AND c)"),
            (
                "x OR NOT a IS NOT DISTINCT FROM b AND c OR d",
                "((x OR ((NOT (a INDF b)) AND c)) OR d)",
            ),
            ("x = 1 AND y = 2 OR z = 3", "(((x = 1) AND (y = 2)) OR (z = 3))"),
            ("a IS DISTINCT FROM (b AND c)", "(a IDF [(b AND c)])"),
            ("a IS DISTINCT FROM b + 1", "(a IDF (b + 1))"),
            ("a IS DISTINCT FROM b = c", "(a IDF (b = c))"),
            (
                "a.f IS NOT DISTINCT FROM b.f AND a.id = b.id",
                "((a.f INDF b.f) AND (a.id = b.id))",
            ),
        ] {
            let sql = format!("SELECT 1 FROM t WHERE {condition}");
            assert_eq!(regrouped(&sql), expected, "{condition}");
        }
    }

    fn checked(sql: &str) -> Result<(), DataFusionError> {
        let statement = datafusion::sql::parser::DFParser::parse_sql(sql)
            .unwrap()
            .pop_front()
            .unwrap();
        check_statement(&statement)
    }

    /// Answers observed on Trino 431.
    #[test]
    fn integer_literals_beyond_bigint_are_syntax_errors() {
        for sql in [
            "SELECT 9223372036854775807",
            "SELECT -9223372036854775808",
            "SELECT 9223372036854775808.5",
            "SELECT 1e40",
            "SELECT '99999999999999999999'",
            "SELECT arrow_cast(18446744073709551615, 'UInt64')",
        ] {
            assert!(checked(sql).is_ok(), "{sql}");
        }
        for (sql, literal) in [
            ("SELECT 9223372036854775808", "9223372036854775808"),
            ("SELECT -9223372036854775809", "-9223372036854775809"),
            (
                "SELECT 1 + 12345678901234567890123456789012345678",
                "12345678901234567890123456789012345678",
            ),
        ] {
            let error = checked(sql).unwrap_err();
            assert!(matches!(error, DataFusionError::SQL(..)), "{sql}: {error}");
            assert!(
                error
                    .to_string()
                    .contains(&format!("Invalid numeric literal: {literal}")),
                "{error}"
            );
        }
    }

    #[test]
    fn timestamp_precisions_above_12_are_unknown_types() {
        assert!(checked("SELECT CAST(x AS TIMESTAMP(12))").is_ok());
        let error = checked("SELECT CAST(x AS TIMESTAMP(13))").unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error during planning: Unknown type: timestamp(13)"
        );
        let error = checked("SELECT CAST(x AS TIMESTAMP(13) WITH TIME ZONE)").unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error during planning: Unknown type: timestamp(13) with time zone"
        );
        assert!(checked("CREATE TABLE t (ts TIMESTAMP(15))").is_err());
    }
}
