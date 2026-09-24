//! Trino 431's reading of SQL text where the parser reads it differently.
//!
//! - An integer literal that does not fit `bigint` is a syntax error in Trino
//!   (`Invalid numeric literal: 9223372036854775808`; the smallest `bigint`,
//!   `-9223372036854775808`, is valid); `DataFusion` reads it as a decimal.
//! - A timestamp precision above 12 is an unknown type in Trino
//!   (`TYPE_MISMATCH`, `Unknown type: timestamp(13)`), not a feature the
//!   engine lacks. [`check_statement`] rejects both before planning.
//! - Identifiers are case-insensitive in Trino: every catalog has lower-case
//!   columns only, and `"CamelCol"` names the column `camelcol`.
//!   `DataFusion` matches a quoted name exactly. [`fold_quoted_columns`]
//!   folds a quoted column reference to lower case, unless the statement
//!   itself defines that exact name (a column alias, a table or CTE alias, a
//!   derived column list), which then stays reachable as written.

use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::error::DataFusionError;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{
    DataType, Expr, Ident, Query, SelectItem, SetExpr, Statement as SqlStatement, TableFactor, TimezoneInfo,
    UnaryOperator, Value, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut, visit_expressions,
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

/// Names a statement defines: column aliases, table and CTE aliases,
/// derived column lists.
#[derive(Default)]
struct DefinedNames(HashSet<String>);

impl DefinedNames {
    fn add(&mut self, ident: &Ident) {
        self.0.insert(ident.value.clone());
    }
}

impl Visitor for DefinedNames {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.add(&cte.alias.name);
                for column in &cte.alias.columns {
                    self.add(&column.name);
                }
            }
        }
        let mut bodies = vec![query.body.as_ref()];
        while let Some(body) = bodies.pop() {
            match body {
                SetExpr::Select(select) => {
                    for item in &select.projection {
                        if let SelectItem::ExprWithAlias { alias, .. } = item {
                            self.add(alias);
                        }
                    }
                }
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push(left);
                    bodies.push(right);
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        let alias = match factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            _ => None,
        };
        if let Some(alias) = alias {
            self.add(&alias.name);
            for column in &alias.columns {
                self.add(&column.name);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Folds the quoted column references not in `defined`.
struct FoldColumns<'a> {
    defined: &'a HashSet<String>,
}

impl FoldColumns<'_> {
    fn fold(&self, ident: &mut Ident) {
        if ident.quote_style.is_some()
            && ident.value.chars().any(char::is_uppercase)
            && !self.defined.contains(&ident.value)
        {
            ident.value = ident.value.to_lowercase();
        }
    }
}

impl VisitorMut for FoldColumns<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(ident) => self.fold(ident),
            // `t."Col"`: the column; the qualifier names a relation.
            Expr::CompoundIdentifier(parts) => {
                if let Some(last) = parts.last_mut() {
                    self.fold(last);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Folds quoted column references to lower case (see the module
/// documentation).
pub(crate) fn fold_quoted_columns(statement: &mut DFStatement) {
    let DFStatement::Statement(inner) = statement else {
        return;
    };
    let mut defined = DefinedNames::default();
    let _ = inner.as_ref().visit(&mut defined);
    let _ = inner.as_mut().visit(&mut NameFoldedItems { defined: &defined.0 });
    let _ = inner.as_mut().visit(&mut FoldColumns { defined: &defined.0 });
}

/// A select item that is a quoted column reference about to be folded keeps
/// its name as written (Trino names it `CamelCol`, and `SELECT "CamelCol",
/// "CAMELCOL"` is two columns).
struct NameFoldedItems<'a> {
    defined: &'a HashSet<String>,
}

impl VisitorMut for NameFoldedItems<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        let SetExpr::Select(select) = query.body.as_mut() else {
            return ControlFlow::Continue(());
        };
        for item in &mut select.projection {
            let SelectItem::UnnamedExpr(expr) = item else {
                continue;
            };
            let name = match expr {
                Expr::Identifier(ident) => ident,
                Expr::CompoundIdentifier(parts) => match parts.last() {
                    Some(last) => last,
                    None => continue,
                },
                _ => continue,
            };
            let folds = name.quote_style.is_some()
                && name.value.chars().any(char::is_uppercase)
                && !self.defined.contains(&name.value);
            if folds {
                let alias = name.clone();
                *item = SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias,
                };
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn folded(sql: &str) -> String {
        let mut statement = datafusion::sql::parser::DFParser::parse_sql(sql)
            .unwrap()
            .pop_front()
            .unwrap();
        fold_quoted_columns(&mut statement);
        statement.to_string()
    }

    #[test]
    fn quoted_columns_fold_unless_the_statement_defines_them() {
        assert_eq!(
            folded("SELECT \"CamelCol\" AS \"OtherCol\" FROM s.\"MiXeD\" WHERE t.\"Id\" > 1"),
            "SELECT \"camelcol\" AS \"OtherCol\" FROM s.\"MiXeD\" WHERE t.\"id\" > 1"
        );
        assert_eq!(
            folded("SELECT \"CamelCol\", t.\"CAMELCOL\" FROM t"),
            "SELECT \"camelcol\" AS \"CamelCol\", t.\"camelcol\" AS \"CAMELCOL\" FROM t"
        );
        assert_eq!(
            folded(
                "SELECT \"ID\", \"Mixed Case\" FROM (SELECT id AS \"ID\", 1 AS \"Mixed Case\") ORDER BY \"ID\""
            ),
            "SELECT \"ID\", \"Mixed Case\" FROM (SELECT id AS \"ID\", 1 AS \"Mixed Case\") ORDER BY \"ID\""
        );
        assert_eq!(
            folded("SELECT \"T\".\"K\" FROM (VALUES (1)) \"T\"(\"K\")"),
            "SELECT \"T\".\"K\" FROM (VALUES (1)) \"T\" (\"K\")"
        );
        assert_eq!(
            folded("SELECT Foo, \"lower\" FROM t"),
            "SELECT Foo, \"lower\" FROM t"
        );
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
