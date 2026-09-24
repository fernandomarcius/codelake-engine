//! `CROSS JOIN UNNEST(t.a) AS u(x)` (and `FROM t, UNNEST(t.a) AS u(x)`):
//! Trino's usual way to explode an array column, one output row per
//! element (rows whose array is empty or NULL disappear).
//!
//! `DataFusion` 53 plans `UNNEST` of an outer column as a lateral join it
//! cannot decorrelate. The relation and its `UNNEST` become one derived
//! table that unnests in its projection, which the engine supports with the
//! same semantics (arguments zipped, shorter arrays padded with NULL, NULL
//! and empty arrays dropped):
//!
//! ```sql
//! FROM t CROSS JOIN UNNEST(t.a, t.b) AS u(x, y)
//! -- becomes
//! FROM (SELECT t.*, unnest(t.a) AS x, unnest(t.b) AS y FROM t) AS t
//! ```
//!
//! and references `u.x` in the query become `t.x`. Only a single relation
//! with a name (a table or an aliased derived table) followed by the
//! `UNNEST` is rewritten; other shapes keep the engine's error.

use std::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    JoinOperator, ObjectName, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    TableAlias, TableFactor, TableWithJoins, VisitMut, VisitorMut, WildcardAdditionalOptions,
    visit_expressions_mut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

fn references_columns(expr: &Expr) -> bool {
    let mut found = false;
    let mut probe = expr.clone();
    let _ = visit_expressions_mut(&mut probe, |e| {
        if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
            found = true;
        }
        ControlFlow::<()>::Continue(())
    });
    found
}

/// The name that qualifies the columns of a relation.
fn qualifier(relation: &TableFactor) -> Option<Ident> {
    match relation {
        TableFactor::Table { name, alias, .. } => match alias {
            Some(alias) => Some(alias.name.clone()),
            None => name.0.last().and_then(|part| part.as_ident()).cloned(),
        },
        TableFactor::Derived {
            alias: Some(alias), ..
        } => Some(alias.name.clone()),
        _ => None,
    }
}

/// An `UNNEST` of outer columns this rewrite handles: its alias and arguments.
fn lateral_unnest(relation: &TableFactor) -> Option<(&TableAlias, &[Expr])> {
    match relation {
        TableFactor::UNNEST {
            alias: Some(alias),
            array_exprs,
            with_offset: false,
            with_ordinality: false,
            ..
        } if alias.columns.len() == array_exprs.len() && array_exprs.iter().any(references_columns) => {
            Some((alias, array_exprs))
        }
        _ => None,
    }
}

fn unnest_call(argument: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("unnest")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// `(SELECT q.*, unnest(e1) AS c1, … FROM relation) AS q`.
fn unnesting_relation(
    relation: TableFactor,
    qualifier: &Ident,
    alias: &TableAlias,
    arguments: &[Expr],
) -> Option<TableFactor> {
    let mut parsed = Parser::parse_sql(&GenericDialect {}, "SELECT 1 FROM codelake_lateral").ok()?;
    let datafusion::sql::sqlparser::ast::Statement::Query(mut query) = parsed.pop()? else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    let mut projection = vec![SelectItem::QualifiedWildcard(
        SelectItemQualifiedWildcardKind::ObjectName(ObjectName::from(vec![qualifier.clone()])),
        WildcardAdditionalOptions::default(),
    )];
    for (argument, column) in arguments.iter().zip(&alias.columns) {
        projection.push(SelectItem::ExprWithAlias {
            expr: unnest_call(argument.clone()),
            alias: column.name.clone(),
        });
    }
    select.projection = projection;
    select.from = vec![TableWithJoins {
        relation,
        joins: vec![],
    }];
    Some(TableFactor::Derived {
        lateral: false,
        subquery: query,
        alias: Some(TableAlias {
            explicit: true,
            name: qualifier.clone(),
            columns: vec![],
            at: None,
        }),
        sample: None,
    })
}

fn placeholder() -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![Ident::new("codelake_lateral")]),
        alias: None,
        args: None,
        with_hints: vec![],
        version: None,
        with_ordinality: false,
        partitions: vec![],
        json_path: None,
        sample: None,
        index_hints: vec![],
    }
}

/// Rewrites one lateral `UNNEST` of `select`; returns the `UNNEST` alias and
/// the qualifier its columns now have.
fn rewrite_one(select: &mut Select) -> Option<(Ident, Ident)> {
    // `t CROSS JOIN UNNEST(…)`: the first join of a relation.
    for item in &mut select.from {
        let Some(join) = item.joins.first() else { continue };
        if !matches!(join.join_operator, JoinOperator::CrossJoin(_)) {
            continue;
        }
        let Some((alias, arguments)) = lateral_unnest(&join.relation) else {
            continue;
        };
        let Some(name) = qualifier(&item.relation) else {
            continue;
        };
        let (alias, arguments) = (alias.clone(), arguments.to_vec());
        let relation = std::mem::replace(&mut item.relation, placeholder());
        item.relation = unnesting_relation(relation, &name, &alias, &arguments)?;
        item.joins.remove(0);
        return Some((alias.name, name));
    }
    // `FROM t, UNNEST(…)`: the next item of the list.
    for i in 1..select.from.len() {
        let (before, after) = select.from.split_at_mut(i);
        let (Some(left), Some(right)) = (before.last_mut(), after.first()) else {
            continue;
        };
        if !left.joins.is_empty() || !right.joins.is_empty() {
            continue;
        }
        let Some((alias, arguments)) = lateral_unnest(&right.relation) else {
            continue;
        };
        let Some(name) = qualifier(&left.relation) else {
            continue;
        };
        let (alias, arguments) = (alias.clone(), arguments.to_vec());
        let relation = std::mem::replace(&mut left.relation, placeholder());
        left.relation = unnesting_relation(relation, &name, &alias, &arguments)?;
        select.from.remove(i);
        return Some((alias.name, name));
    }
    None
}

#[derive(Debug)]
struct LateralUnnest;

impl VisitorMut for LateralUnnest {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        let mut renames = Vec::new();
        if let SetExpr::Select(select) = query.body.as_mut() {
            while let Some(rename) = rewrite_one(select) {
                renames.push(rename);
            }
        }
        if !renames.is_empty() {
            let _ = visit_expressions_mut(query, |e| {
                if let Expr::CompoundIdentifier(parts) = e
                    && parts.len() == 2
                    && let Some((_, qualifier)) = renames
                        .iter()
                        .find(|(from, _)| from.value.eq_ignore_ascii_case(&parts[0].value))
                {
                    parts[0] = qualifier.clone();
                }
                ControlFlow::<()>::Continue(())
            });
        }
        ControlFlow::Continue(())
    }
}

/// Applies the rewrite to every query of `statement` (module docs).
pub(crate) fn rewrite(statement: &mut datafusion::sql::sqlparser::ast::Statement) {
    let _ = statement.visit(&mut LateralUnnest);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewritten(sql: &str) -> String {
        let mut statement = Parser::parse_sql(&GenericDialect {}, sql).unwrap().remove(0);
        rewrite(&mut statement);
        statement.to_string()
    }

    #[test]
    fn cross_join_unnest_of_a_column_unnests_in_a_projection() {
        assert_eq!(
            rewritten("SELECT g, u.x FROM t CROSS JOIN UNNEST(t.a) AS u(x) ORDER BY 1"),
            "SELECT g, t.x FROM (SELECT t.*, unnest(t.a) AS x FROM t) AS t ORDER BY 1"
        );
        assert_eq!(
            rewritten("SELECT * FROM (VALUES (1, ARRAY[2])) AS v(g, a), UNNEST(a, a) AS u(x, y)"),
            "SELECT * FROM (SELECT v.*, unnest(a) AS x, unnest(a) AS y FROM (VALUES (1, ARRAY[2])) AS v (g, a)) AS v"
        );
    }

    #[test]
    fn unnest_of_constants_is_left_alone() {
        let sql = "SELECT * FROM UNNEST(ARRAY[1]) AS a (i) CROSS JOIN UNNEST(ARRAY[2]) AS b (j)";
        assert_eq!(rewritten(sql), sql);
    }
}
