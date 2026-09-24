//! Output column names as Trino gives them: an aliased item keeps its alias, a
//! column reference its column name, and any other expression is `_col<n>`,
//! `n` being its position in the output. `DataFusion` names such expressions
//! after their text (`count(*)`) and refuses two with the same text
//! (`SELECT v, CAST(v AS VARCHAR)`), so the protocol aliases them in the
//! parsed statement with [`UNNAMED`] and renames them by position once the
//! output schema is known (a `*` before them shifts the position).

use datafusion::sql::parser::{DFParser, Statement as DfStatement};
use datafusion::sql::sqlparser::ast::{Expr, Ident, Query, SelectItem, SetExpr, Statement};

/// Alias prefix of unnamed output expressions; `types::columns` turns it into `_col<n>`.
pub(crate) const UNNAMED: &str = "__codelake_unnamed_";

fn is_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

/// Aliases the unnamed expressions of the query's output; `true` if any.
fn name_query(query: &mut Query) -> bool {
    name_set(&mut query.body)
}

fn name_set(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => {
            let mut changed = false;
            for (index, item) in select.projection.iter_mut().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item
                    && !is_column(expr)
                {
                    *item = SelectItem::ExprWithAlias {
                        expr: expr.clone(),
                        alias: Ident::with_quote('"', format!("{UNNAMED}{index}")),
                    };
                    changed = true;
                }
            }
            changed
        }
        // The first branch names the columns of a set operation.
        SetExpr::SetOperation { left, .. } => name_set(left),
        SetExpr::Query(query) => name_query(query),
        _ => false,
    }
}

/// The SQL to plan so that unnamed output columns can be renamed `_col<n>`:
/// `None` when the statement is not a query or names every column already.
pub(crate) fn name_unnamed_columns(sql: &str) -> Option<String> {
    // The text the engine parses (`VALUES 1, 2` read as `VALUES (1), (2)`).
    let sql = codelake_core::engine::trino_text(sql);
    let mut statements = DFParser::parse_sql(&sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let Some(DfStatement::Statement(statement)) = statements.front_mut() else {
        return None;
    };
    let Statement::Query(query) = statement.as_mut() else {
        return None;
    };
    name_query(query).then(|| statement.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unnamed_expressions_get_positional_aliases() {
        assert_eq!(
            name_unnamed_columns("SELECT value, CAST(value AS VARCHAR) FROM generate_series(1, 3)")
                .as_deref(),
            Some(
                "SELECT value, CAST(value AS VARCHAR) AS \"__codelake_unnamed_1\" FROM generate_series(1, 3)"
            )
        );
        assert_eq!(
            name_unnamed_columns("SELECT count(*) FROM t UNION ALL SELECT 1").as_deref(),
            Some("SELECT count(*) AS \"__codelake_unnamed_0\" FROM t UNION ALL SELECT 1")
        );
        assert_eq!(
            name_unnamed_columns("WITH c AS (SELECT 1 AS a) SELECT a + 1 FROM c").as_deref(),
            Some("WITH c AS (SELECT 1 AS a) SELECT a + 1 AS \"__codelake_unnamed_0\" FROM c")
        );
    }

    #[test]
    fn named_columns_and_other_statements_are_left_alone() {
        assert_eq!(name_unnamed_columns("SELECT a, t.b, c AS d FROM t"), None);
        assert_eq!(name_unnamed_columns("SELECT * FROM t"), None);
        assert_eq!(name_unnamed_columns("INSERT INTO t SELECT 1"), None);
        assert_eq!(name_unnamed_columns("CREATE TABLE t AS SELECT 1"), None);
        assert_eq!(name_unnamed_columns("SELECT 1 +"), None);
        assert_eq!(name_unnamed_columns("SELECT 1; SELECT 2"), None);
    }
}
