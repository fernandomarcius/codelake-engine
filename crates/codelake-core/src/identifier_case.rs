//! Case of identifiers (ISS-018): names are case-insensitive on every client
//! protocol, and every spelling of a column resolves to it.
//!
//! Every catalog exposes lower-case column names (a hive table keeps the
//! spelling a name was written with in its files and metadata, and shows the
//! folded one). The SQL planner matches a quoted name exactly and folds an
//! unquoted one with ASCII rules only (`DESCRIÇÃO` would become
//! `descriÇÃo`, a spelling nobody wrote). [`fold_identifiers`] closes both
//! gaps before planning:
//!
//! - an unquoted identifier with a non-ASCII capital letter is folded to
//!   lower case (as the planner folds `A`–`Z`), wherever it names something;
//! - a quoted column reference (in an expression, an `INSERT` column list, a
//!   `JOIN … USING` list) is folded to lower case, unless the statement
//!   itself defines that exact name (a column alias, a table or CTE alias, a
//!   derived column list), which then stays reachable as written. A select
//!   item that is such a reference keeps its written name as its header
//!   (`SELECT "CamelCol", "CAMELCOL"` is two columns).
//!
//! Names that differ only by case are refused where they are stored, so
//! folding never makes one name reach another column.

use std::collections::HashSet;
use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr, Ident, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, Visit, VisitMut, Visitor, VisitorMut,
};

/// Folds the identifiers of `statement` (see the module documentation).
/// Returns whether anything changed.
pub fn fold_identifiers(statement: &mut Statement) -> bool {
    let unquoted = fold_unquoted(statement);
    let mut defined = DefinedNames::default();
    let _ = (&*statement).visit(&mut defined);
    let mut items = NameFoldedItems {
        defined: &defined.0,
        changed: false,
    };
    let _ = statement.visit(&mut items);
    let mut columns = FoldColumns {
        defined: &defined.0,
        changed: false,
    };
    let _ = statement.visit(&mut columns);
    unquoted || items.changed || columns.changed
}

/// Folds only the unquoted identifiers with non-ASCII capitals, as the
/// planner folds `A`-`Z`. Returns whether anything changed.
pub fn fold_unquoted(statement: &mut Statement) -> bool {
    let mut unquoted = FoldUnquoted::default();
    let _ = statement.visit(&mut unquoted);
    unquoted.changed
}

/// Whether `ident` is unquoted and has a capital letter the planner's ASCII
/// folding leaves.
fn unquoted_non_ascii_capital(ident: &Ident) -> bool {
    ident.quote_style.is_none() && ident.value.chars().any(|c| !c.is_ascii() && c.is_uppercase())
}

/// Folds unquoted identifiers with non-ASCII capitals.
#[derive(Default)]
struct FoldUnquoted {
    changed: bool,
}

impl FoldUnquoted {
    fn fold(&mut self, ident: &mut Ident) {
        if unquoted_non_ascii_capital(ident) {
            ident.value = ident.value.to_lowercase();
            self.changed = true;
        }
    }

    fn fold_name(&mut self, name: &mut ObjectName) {
        for part in &mut name.0 {
            if let ObjectNamePart::Identifier(ident) = part {
                self.fold(ident);
            }
        }
    }

    fn fold_joins(&mut self, from: &mut [TableWithJoins]) {
        for table in from {
            for join in &mut table.joins {
                if let Some(JoinConstraint::Using(names)) = join_constraint(&mut join.join_operator) {
                    for name in names {
                        self.fold_name(name);
                    }
                }
            }
        }
    }
}

impl VisitorMut for FoldUnquoted {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        match statement {
            Statement::Insert(insert) => {
                for column in &mut insert.columns {
                    self.fold_name(column);
                }
            }
            Statement::CreateTable(create) => {
                for column in &mut create.columns {
                    self.fold(&mut column.name);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.fold(&mut cte.alias.name);
                for column in &mut cte.alias.columns {
                    self.fold(&mut column.name);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        for item in &mut select.projection {
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                self.fold(alias);
            }
        }
        self.fold_joins(&mut select.from);
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        self.fold_name(relation);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::NestedJoin { table_with_joins, .. } = factor {
            self.fold_joins(std::slice::from_mut(table_with_joins.as_mut()));
        }
        if let Some(alias) = table_alias(factor) {
            self.fold(&mut alias.name);
            for column in &mut alias.columns {
                self.fold(&mut column.name);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(ident) => self.fold(ident),
            Expr::CompoundIdentifier(parts) => {
                for part in parts {
                    self.fold(part);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// The constraint of a join, if it has one.
fn join_constraint(operator: &mut JoinOperator) -> Option<&mut JoinConstraint> {
    match operator {
        JoinOperator::Join(constraint)
        | JoinOperator::Inner(constraint)
        | JoinOperator::Left(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::Right(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint)
        | JoinOperator::CrossJoin(constraint)
        | JoinOperator::Semi(constraint)
        | JoinOperator::LeftSemi(constraint)
        | JoinOperator::RightSemi(constraint)
        | JoinOperator::Anti(constraint)
        | JoinOperator::LeftAnti(constraint)
        | JoinOperator::RightAnti(constraint)
        | JoinOperator::StraightJoin(constraint)
        | JoinOperator::AsOf { constraint, .. } => Some(constraint),
        _ => None,
    }
}

fn table_alias(factor: &mut TableFactor) -> Option<&mut datafusion::sql::sqlparser::ast::TableAlias> {
    match factor {
        TableFactor::Table { alias, .. }
        | TableFactor::Derived { alias, .. }
        | TableFactor::UNNEST { alias, .. }
        | TableFactor::Function { alias, .. }
        | TableFactor::NestedJoin { alias, .. } => alias.as_mut(),
        _ => None,
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

/// Whether a quoted column name `ident` folds (see the module
/// documentation).
fn folds(ident: &Ident, defined: &HashSet<String>) -> bool {
    ident.quote_style.is_some()
        && ident.value.chars().any(char::is_uppercase)
        && !defined.contains(&ident.value)
}

/// Folds the quoted column references not in `defined`.
struct FoldColumns<'a> {
    defined: &'a HashSet<String>,
    changed: bool,
}

impl FoldColumns<'_> {
    fn fold(&mut self, ident: &mut Ident) {
        if folds(ident, self.defined) {
            ident.value = ident.value.to_lowercase();
            self.changed = true;
        }
    }

    /// A column named by an object name (`INSERT` and `USING` lists): its
    /// last part.
    fn fold_column(&mut self, name: &mut ObjectName) {
        if let Some(ObjectNamePart::Identifier(ident)) = name.0.last_mut() {
            self.fold(ident);
        }
    }

    fn fold_joins(&mut self, from: &mut [TableWithJoins]) {
        for table in from {
            for join in &mut table.joins {
                if let Some(JoinConstraint::Using(names)) = join_constraint(&mut join.join_operator) {
                    for name in names {
                        self.fold_column(name);
                    }
                }
            }
        }
    }
}

impl VisitorMut for FoldColumns<'_> {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<()> {
        // The columns of the target table: never a name the statement
        // defines, whatever aliases its query has.
        if let Statement::Insert(insert) = statement {
            for column in &mut insert.columns {
                if let Some(ObjectNamePart::Identifier(ident)) = column.0.last_mut()
                    && ident.quote_style.is_some()
                    && ident.value.chars().any(char::is_uppercase)
                {
                    ident.value = ident.value.to_lowercase();
                    self.changed = true;
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        self.fold_joins(&mut select.from);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::NestedJoin { table_with_joins, .. } = factor {
            self.fold_joins(std::slice::from_mut(table_with_joins.as_mut()));
        }
        ControlFlow::Continue(())
    }

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

/// A select item that is a quoted column reference about to be folded keeps
/// its name as written.
struct NameFoldedItems<'a> {
    defined: &'a HashSet<String>,
    changed: bool,
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
            if folds(name, self.defined) {
                let alias = name.clone();
                *item = SelectItem::ExprWithAlias {
                    expr: expr.clone(),
                    alias,
                };
                self.changed = true;
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::{GenericDialect, PostgreSqlDialect};
    use datafusion::sql::sqlparser::parser::Parser;

    fn folded(sql: &str) -> String {
        let mut statement = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
        fold_identifiers(&mut statement);
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
    fn insert_lists_and_using_lists_fold() {
        // The target's columns fold even when the query defines the name.
        assert_eq!(
            folded("INSERT INTO t (\"Valor\", \"CUSTOMERCODE\") SELECT 1 AS \"Valor\", 2"),
            "INSERT INTO t (\"valor\", \"customercode\") SELECT 1 AS \"Valor\", 2"
        );
        assert_eq!(
            folded(
                "SELECT count(*) FROM a JOIN b USING (\"CustomerCode\") LEFT JOIN (c JOIN d USING (\"K\")) ON 1 = 1"
            ),
            "SELECT count(*) FROM a JOIN b USING(\"customercode\") LEFT JOIN (c JOIN d USING(\"k\")) ON 1 = 1"
        );
        // A name the statement defines stays reachable as written.
        assert_eq!(
            folded("SELECT 1 FROM (SELECT 1 AS \"K\") a JOIN (SELECT 1 AS \"K\") b USING (\"K\")"),
            "SELECT 1 FROM (SELECT 1 AS \"K\") a JOIN (SELECT 1 AS \"K\") b USING(\"K\")"
        );
    }

    #[test]
    fn unquoted_non_ascii_capitals_fold() {
        assert_eq!(
            folded("SELECT CÓDIGO, x.DESCRIÇÃO AS NOMÉ FROM s.AÇÃO x JOIN y USING (ÍD)"),
            "SELECT código, x.descrição AS nomé FROM s.ação x JOIN y USING(íd)"
        );
        assert_eq!(
            folded("CREATE TABLE t (DESCRIÇÃO BIGINT, \"CÓDIGO\" BIGINT)"),
            "CREATE TABLE t (descrição BIGINT, \"CÓDIGO\" BIGINT)"
        );
        // ASCII capitals are the planner's to fold.
        assert_eq!(folded("SELECT FOO FROM T"), "SELECT FOO FROM T");
    }

    #[test]
    fn reports_whether_anything_changed() {
        for (sql, changed) in [
            ("SELECT a, \"b\" FROM t", false),
            ("SELECT \"A\" AS \"A\" FROM t", false),
            ("SELECT \"A\" FROM t", true),
            ("SELECT DESCRIÇÃO FROM t", true),
        ] {
            let mut statement = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap().remove(0);
            assert_eq!(fold_identifiers(&mut statement), changed, "{sql}");
        }
    }
}
