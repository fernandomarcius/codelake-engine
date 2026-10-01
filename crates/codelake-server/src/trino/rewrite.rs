//! Output column names as Trino gives them: an aliased item keeps its alias, a
//! column reference its column name, and any other expression is `_col<n>`,
//! `n` being its position in the output. `DataFusion` names such expressions
//! after their text (`count(*)`) and refuses two with the same text
//! (`SELECT v, CAST(v AS VARCHAR)`), so the protocol aliases them in the
//! parsed statement with [`UNNAMED`] and renames them by position once the
//! output schema is known (a `*` before them shifts the position).
//!
//! Names keep the case they are written with: `SELECT 1 AS FirstDay` and
//! `SELECT X FROM t` (column `x`) name their columns `FirstDay` and `X`, quoted
//! or not, where `DataFusion` folds unquoted names to lower case.
//! [`written_names`] reads them from the statement and [`restore_written`]
//! puts them back on the output names they fold to; the names a `*` expands
//! to are the engine's (ISS-018).

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

/// How the statement writes one item of its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Written {
    /// An alias, or a column reference without one: the name as written.
    Name(String),
    /// An expression without alias (named `_col<n>`).
    Unnamed,
    /// `*` or `t.*`: any number of columns.
    Wildcard,
}

fn written_set(body: &SetExpr) -> Vec<Written> {
    match body {
        SetExpr::Select(select) => select
            .projection
            .iter()
            .map(|item| match item {
                SelectItem::ExprWithAlias { alias, .. } => Written::Name(alias.value.clone()),
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => Written::Name(ident.value.clone()),
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => parts
                    .last()
                    .map_or(Written::Unnamed, |ident| Written::Name(ident.value.clone())),
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAliases { .. } => Written::Unnamed,
                SelectItem::QualifiedWildcard(..) | SelectItem::Wildcard(_) => Written::Wildcard,
            })
            .collect(),
        // The first branch names the columns of a set operation.
        SetExpr::SetOperation { left, .. } => written_set(left),
        SetExpr::Query(query) => written_set(&query.body),
        _ => Vec::new(),
    }
}

/// The output items of a query as the statement writes them; empty for
/// other statements.
pub(crate) fn written_names(sql: &str) -> Vec<Written> {
    let sql = codelake_core::engine::trino_text(sql);
    let Ok(mut statements) = DFParser::parse_sql(&sql) else {
        return Vec::new();
    };
    match (statements.len(), statements.pop_front()) {
        (1, Some(DfStatement::Statement(statement))) => match *statement {
            Statement::Query(query) => written_set(&query.body),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Gives each output name its written spelling where the two are the same
/// name but for case. Items before the first `*` match the first names, items
/// after the last `*` the last ones; items between two `*` are not placed.
pub(crate) fn restore_written(mut names: Vec<&mut String>, written: &[Written]) {
    let (head, tail) = match written.iter().position(|w| *w == Written::Wildcard) {
        None if written.len() == names.len() => (written, &[][..]),
        // Not the statement's output: nothing is placed.
        None => return,
        Some(first) => {
            let last = written
                .iter()
                .rposition(|w| *w == Written::Wildcard)
                .unwrap_or(first);
            (&written[..first], &written[last + 1..])
        }
    };
    if head.len() + tail.len() > names.len() {
        return;
    }
    let split = names.len() - tail.len();
    let (front, back) = names.split_at_mut(split);
    let placed = head
        .iter()
        .zip(front.iter_mut())
        .chain(tail.iter().zip(back.iter_mut()));
    for (written, name) in placed {
        if let Written::Name(spelling) = written
            && spelling != &**name
            && spelling.to_lowercase() == name.to_lowercase()
        {
            spelling.clone_into(name);
        }
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

    fn restored(names: &[&str], sql: &str) -> Vec<String> {
        let mut names: Vec<String> = names.iter().map(|&n| n.to_owned()).collect();
        restore_written(names.iter_mut().collect(), &written_names(sql));
        names
    }

    /// Spellings observed on the reference engine.
    #[test]
    fn output_names_keep_the_written_case() {
        assert_eq!(
            restored(
                &["firstday", "valorsistemabase", "Data_Rec", "_col3"],
                "SELECT 1 AS FirstDay, 2 valorSistemaBase, 3 AS \"Data_Rec\", 4 FROM t"
            ),
            ["FirstDay", "valorSistemaBase", "Data_Rec", "_col3"]
        );
        assert_eq!(
            restored(&["x", "x", "y"], "SELECT X, t.X, \"y\" FROM t"),
            ["X", "X", "y"]
        );
        // The first branch names a set operation's columns; parentheses too.
        assert_eq!(
            restored(&["abc"], "(SELECT 1 AS Abc) UNION ALL SELECT 2 AS Xyz"),
            ["Abc"]
        );
        assert_eq!(
            restored(&["foo"], "WITH c AS (SELECT 1 AS Foo) SELECT foo FROM c"),
            ["foo"]
        );
        // Around a `*`: the items before it from the start, after it from the end.
        assert_eq!(
            restored(
                &["primeiro", "x", "y", "ultimo"],
                "SELECT x AS Primeiro, *, y AS Ultimo FROM t"
            ),
            ["Primeiro", "x", "y", "Ultimo"]
        );
        assert_eq!(
            restored(&["a", "b", "c", "d"], "SELECT *, B, t.*, D FROM t"),
            ["a", "b", "c", "D"]
        );
        assert_eq!(
            restored(&["a"], "SELECT A, *, B FROM t"),
            ["a"],
            "more items than names"
        );
        assert_eq!(
            restored(&["a", "b"], "SELECT A, *, B FROM t"),
            ["A", "B"],
            "a * of no column"
        );
        // A name that is not the item's (the engine renamed it) stays.
        assert_eq!(restored(&["other"], "SELECT Name FROM t"), ["other"]);
        assert_eq!(restored(&["ação"], "SELECT 1 AS \"Ação\""), ["Ação"]);
    }

    #[test]
    fn only_a_query_writes_output_names() {
        assert_eq!(
            restored(&["a", "b"], "SELECT A FROM t"),
            ["a", "b"],
            "count differs"
        );
        assert_eq!(restored(&["a"], "INSERT INTO t SELECT 1 AS A"), ["a"]);
        assert_eq!(restored(&["a"], "VALUES (1)"), ["a"]);
        assert_eq!(restored(&["a"], "SELECT 1 AS A; SELECT 2 AS A"), ["a"]);
        assert_eq!(restored(&["a"], "SELECT 1 AS"), ["a"]);
        assert!(written_names("SHOW TABLES").is_empty());
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
