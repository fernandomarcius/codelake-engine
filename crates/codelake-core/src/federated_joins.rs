//! Joins sent to a source as SQL that means what the plan means.
//!
//! `datafusion-federation` renders a federated sub-plan with the `DataFusion`
//! unparser, which writes a join from the shapes of its inputs and gets three
//! of them wrong (`datafusion-sql` 54.1, `unparser/plan.rs`):
//!
//! - **Filters of a table folded into `ON`.** A filtered table under a join
//!   (`FROM t AS r … WHERE r.x = 1` reaches it as filters of the scan) has
//!   its filters moved into the join: `WHERE` for an inner join, `ON` for an
//!   outer one (`split_join_on_and_where_filters`). For the side an outer
//!   join preserves that changes the result: `r LEFT JOIN p ON … AND r.x = 1`
//!   returns every row of `r`. A reporting application's list of payers
//!   returned 150 974 rows instead of 158 (D1).
//! - **A filter above a join input goes to the shared `WHERE`.** A filter
//!   over a derived table or over other joins becomes part of the `WHERE` of
//!   the whole `FROM`, which drops the rows an outer join added for the side
//!   it does not preserve.
//! - **The select list of a join is built side by side.** Without a
//!   projection above it, each side contributes its own list: a table its
//!   columns, an aliased table `*`, a derived table nothing. The source then
//!   returns other columns than the plan's, read by position: an error when
//!   the types differ, another column's values when they agree (D2).
//!
//! [`for_unparser`] rewrites a sub-plan about to be federated so the
//! unparser writes it right: a filtered table a LEFT or RIGHT join preserves
//! has its filters lifted above the join (its `WHERE`), a filtered table of
//! a FULL join becomes a derived table of its own, and so does an aliased
//! input the unparser would merge into the enclosing `SELECT` (the filter of
//! a side the join adds rows for, under its alias);
//! every `SELECT` whose list a join would build gets an explicit
//! list of the plan's columns, in order (which
//! [`crate::federation_policy::PruneFederatedColumns`] then narrows to the
//! columns the engine reads, D3). [`renders_faithfully`] tells whether every
//! join of a sub-plan has a shape the unparser writes right; one that does
//! not stays in the engine, its inputs federated on their own.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, TableReference};
use datafusion::error::Result;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, Filter, Join, JoinType, LogicalPlan, Projection, SubqueryAlias};

/// How the unparser renders a join input into the `FROM` of the enclosing
/// `SELECT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// A table without filters, aliased or not, or a derived table: nothing
    /// of it reaches `ON` or `WHERE`.
    Plain,
    /// A table under filters (`Filter`s or filters of its scan, aliased or
    /// not): the unparser moves them into the join, `ON` when it is outer.
    FilteredScan,
    /// A derived table under a filter: the filter goes to the `WHERE` shared
    /// by the whole `FROM`.
    FilteredDerived,
    /// Joins written into the same `FROM`; `shared_where` when a filter of
    /// theirs goes to its `WHERE`.
    Joins { shared_where: bool },
    /// Anything else: the unparser may name a relation it did not write, or
    /// mix this input's clauses into the enclosing `SELECT`.
    Unknown,
}

impl Shape {
    /// Whether some filter of this input ends up in the shared `WHERE`.
    fn shared_where(self) -> bool {
        matches!(self, Self::FilteredDerived | Self::Joins { shared_where: true })
    }
}

/// The unparser writes these under an alias as a derived table
/// (`requires_derived_subquery`, and a projection once the enclosing
/// `SELECT` has its list, which [`for_unparser`] makes sure of).
fn is_derived_body(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Union(_)
    )
}

/// A table scan the unparser can write as a table (no row limit of its own).
fn plain_scan(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::TableScan(scan) if scan.fetch.is_none())
}

/// `Filter*` over a table scan under at most one alias (the chain
/// `try_transform_to_simple_table_scan_with_filters` folds into a join; of
/// two aliases it would write the inner one).
fn scan_under_filters(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => scan_under_filters(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => filters_over_scan(&alias.input),
        other => plain_scan(other),
    }
}

/// `Filter*` over a table scan, without an alias.
fn filters_over_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => filters_over_scan(&filter.input),
        other => plain_scan(other),
    }
}

fn shape(plan: &LogicalPlan) -> Shape {
    match plan {
        LogicalPlan::TableScan(scan) if scan.fetch.is_none() => {
            if scan.filters.is_empty() {
                Shape::Plain
            } else {
                Shape::FilteredScan
            }
        }
        LogicalPlan::SubqueryAlias(alias) => match alias.input.as_ref() {
            LogicalPlan::TableScan(_) => shape(&alias.input),
            LogicalPlan::Filter(_) if filters_over_scan(&alias.input) => Shape::FilteredScan,
            body if is_derived_body(body) => Shape::Plain,
            _ => Shape::Unknown,
        },
        LogicalPlan::Filter(filter) => {
            if scan_under_filters(&filter.input) {
                return Shape::FilteredScan;
            }
            match shape(&filter.input) {
                Shape::Plain | Shape::FilteredDerived => Shape::FilteredDerived,
                Shape::Joins { .. } => Shape::Joins { shared_where: true },
                Shape::FilteredScan | Shape::Unknown => Shape::Unknown,
            }
        }
        LogicalPlan::Join(join) => {
            let (left, right) = (shape(&join.left), shape(&join.right));
            if left == Shape::Unknown || right == Shape::Unknown {
                return Shape::Unknown;
            }
            let inner_filters = join.join_type == JoinType::Inner
                && (left == Shape::FilteredScan || right == Shape::FilteredScan);
            Shape::Joins {
                shared_where: inner_filters || left.shared_where() || right.shared_where(),
            }
        }
        _ => Shape::Unknown,
    }
}

/// Whether the unparser writes `join` as the plan means it, its inputs as
/// [`for_unparser`] leaves them.
fn join_renders_faithfully(join: &Join) -> bool {
    let (left, right) = (shape(&join.left), shape(&join.right));
    if left == Shape::Unknown || right == Shape::Unknown {
        return false;
    }
    // A join as the right input is written into the enclosing `FROM`, after
    // its left input's joins: another join.
    let right_joins = matches!(right, Shape::Joins { .. });
    match join.join_type {
        JoinType::Inner => !right_joins,
        // The side kept whole must not have its filters folded into `ON`;
        // the other must not put its own into the shared `WHERE` (they would
        // drop the rows the join adds for it).
        JoinType::Left => !right_joins && left != Shape::FilteredScan && !right.shared_where(),
        JoinType::Right => !right_joins && right != Shape::FilteredScan && !left.shared_where(),
        JoinType::Full => {
            matches!(left, Shape::Plain | Shape::Joins { shared_where: false }) && right == Shape::Plain
        }
        // Semi, anti and mark joins render as `EXISTS`, their inputs apart.
        _ => true,
    }
}

/// Whether every join of `plan` (as [`for_unparser`] leaves it) renders as
/// the plan means it.
pub(crate) fn renders_faithfully(plan: &LogicalPlan) -> bool {
    let mut faithful = true;
    // Visiting never fails: the closure only reads.
    let _ = plan.apply(|node| {
        if let LogicalPlan::Join(join) = node
            && !join_renders_faithfully(join)
        {
            faithful = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    faithful
}

/// `plan` rewritten so the unparser renders its joins and select lists as
/// the plan means them (see the module documentation). What it cannot fix
/// is left as is, for [`renders_faithfully`] to refuse.
pub(crate) fn for_unparser(plan: LogicalPlan) -> Result<LogicalPlan> {
    let plan = plan.transform_up(isolate_join_inputs).data()?;
    let plan = plan
        .transform_up(|node| {
            let LogicalPlan::Union(union) = &node else {
                return Ok(Transformed::no(node));
            };
            if !union.inputs.iter().any(|input| lists_from_join(input)) {
                return Ok(Transformed::no(node));
            }
            let inputs = union
                .inputs
                .iter()
                .map(|input| explicit_select_list(input.as_ref().clone()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Transformed::yes(node.with_new_exprs(node.expressions(), inputs)?))
        })
        .data()?;
    explicit_select_list(plan)
}

/// A `SELECT` whose list the unparser would build from a join (a join
/// under filters, sorts and limits) gets a projection of the join's
/// columns: above its filters, below sorts and limits (a projection over a
/// limit renders the limit as a derived table, and the list would name the
/// tables inside it).
fn explicit_select_list(plan: LogicalPlan) -> Result<LogicalPlan> {
    match &plan {
        LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => {
            let Some(input) = plan.inputs().into_iter().next().cloned() else {
                return Ok(plan);
            };
            if !lists_from_join(&input) && !matches!(input, LogicalPlan::Sort(_) | LogicalPlan::Limit(_)) {
                return Ok(plan);
            }
            let input = explicit_select_list(input)?;
            plan.with_new_exprs(plan.expressions(), vec![input])
        }
        _ if lists_from_join(&plan) => {
            let columns = plan.schema().columns().into_iter().map(Expr::Column).collect();
            Ok(LogicalPlan::Projection(Projection::try_new(
                columns,
                Arc::new(plan),
            )?))
        }
        _ => Ok(plan),
    }
}

fn lists_from_join(plan: &LogicalPlan) -> bool {
    match plan {
        // A semi, anti or mark join lists its left input's columns.
        LogicalPlan::Join(join) => matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        ),
        LogicalPlan::Filter(filter) => lists_from_join(&filter.input),
        _ => false,
    }
}

/// The inputs of a join that the unparser would write wrong, isolated: a
/// filtered table a LEFT or RIGHT join preserves has its filters lifted
/// above the join, out of `ON` (a FULL join's becomes a derived table), and
/// an aliased input the unparser would merge into the enclosing `SELECT`
/// (a filter or a join under the alias, such as the filter of a side the
/// join adds rows for) becomes a derived table of its own. One that needs a
/// derived table but has no alias is left as is: the join then stays in
/// the engine.
fn isolate_join_inputs(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Join(join) = &node else {
        return Ok(Transformed::no(node));
    };
    let (outer, keeps_left, keeps_right) = match join.join_type {
        JoinType::Inner => (false, true, true),
        JoinType::Left => (true, true, false),
        JoinType::Right => (true, false, true),
        JoinType::Full => (true, true, true),
        _ => return Ok(Transformed::no(node)),
    };
    let full = join.join_type == JoinType::Full;
    let mut lifted = Vec::new();
    let mut changed = false;
    let mut inputs = Vec::with_capacity(2);
    for (input, keeps) in [(&join.left, keeps_left), (&join.right, keeps_right)] {
        let side = shape(input);
        // Folded into `ON`: wrong when the join keeps this side's rows
        // anyway. A filter that would reach the shared `WHERE` from a side
        // the join adds rows for sits above the side's alias, if any: the
        // join stays in the engine (`renders_faithfully`).
        let folded = outer && keeps && side == Shape::FilteredScan;
        let merged = side == Shape::Unknown;
        if !folded && !merged {
            inputs.push(input.as_ref().clone());
            continue;
        }
        if folded
            && !full
            && let Some(table) = single_relation(input)
        {
            let (bare, filters) = without_filters(input.as_ref().clone())?;
            inputs.push(bare);
            for filter in filters {
                lifted.push(requalified(filter, &table)?);
            }
            changed = true;
        } else if let Some(derived) = as_derived_table(input)? {
            inputs.push(derived);
            changed = true;
        } else {
            inputs.push(input.as_ref().clone());
        }
    }
    if !changed {
        return Ok(Transformed::no(node));
    }
    let rebuilt = node.with_new_exprs(node.expressions(), inputs)?;
    Ok(Transformed::yes(match conjunction(lifted) {
        Some(predicate) => LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(rebuilt))?),
        None => rebuilt,
    }))
}

/// An aliased input as a derived table of its own (`(SELECT … WHERE …) AS
/// r`): the projection of its columns under the alias, which the unparser
/// writes as a derived table once the enclosing `SELECT` has its list.
/// `None` for an input without an alias, which a derived table would rename.
fn as_derived_table(input: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::SubqueryAlias(aliased) = input else {
        return Ok(None);
    };
    // A projection over `DISTINCT` renders as one `SELECT DISTINCT` of
    // fewer columns, which keeps fewer rows (the optimizer turns `DISTINCT`
    // into an aggregation before federation anyway).
    if is_derived_body(&aliased.input) || matches!(aliased.input.as_ref(), LogicalPlan::Distinct(_)) {
        return Ok(None);
    }
    let columns = aliased
        .input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let body = LogicalPlan::Projection(Projection::try_new(columns, Arc::clone(&aliased.input))?);
    Ok(Some(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(body),
        aliased.alias.clone(),
    )?)))
}

/// A table under filters (aliased or not) without them, and the filters,
/// as they name the table: by its own name or by an alias below the
/// outermost one.
fn without_filters(plan: LogicalPlan) -> Result<(LogicalPlan, Vec<Expr>)> {
    match plan {
        LogicalPlan::SubqueryAlias(aliased) => {
            let (bare, filters) = without_filters(Arc::unwrap_or_clone(aliased.input))?;
            let rebuilt = SubqueryAlias::try_new(Arc::new(bare), aliased.alias)?;
            Ok((LogicalPlan::SubqueryAlias(rebuilt), filters))
        }
        LogicalPlan::Filter(filter) => {
            let (bare, mut filters) = without_filters(Arc::unwrap_or_clone(filter.input))?;
            filters.push(filter.predicate);
            Ok((bare, filters))
        }
        LogicalPlan::TableScan(mut scan) => {
            let filters = std::mem::take(&mut scan.filters);
            Ok((LogicalPlan::TableScan(scan), filters))
        }
        other => Ok((other, Vec::new())),
    }
}

/// The one relation every column of `plan` names, as the join sees it.
fn single_relation(plan: &LogicalPlan) -> Option<TableReference> {
    let mut relations = plan.schema().iter().map(|(qualifier, _)| qualifier);
    let first = relations.next()??;
    relations.all(|q| q == Some(first)).then(|| first.clone())
}

/// `expr`, whose columns all belong to one table, naming them as `table`.
fn requalified(expr: Expr, table: &TableReference) -> Result<Expr> {
    expr.transform(|e| match e {
        Expr::Column(column) => Ok(Transformed::yes(Expr::Column(Column::new(
            Some(table.clone()),
            column.name,
        )))),
        other => Ok(Transformed::no(other)),
    })
    .data()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::logical_expr::logical_plan::builder::table_scan_with_filters;
    use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, col, lit, table_scan};
    use datafusion::sql::unparser::Unparser;
    use datafusion::sql::unparser::dialect::DefaultDialect;

    use super::*;
    use crate::federation_policy::PushdownPolicy;
    use crate::testing::{int64, strings};

    fn schema(columns: &[&str]) -> Schema {
        Schema::new(
            columns
                .iter()
                .map(|c| Field::new(*c, DataType::Int64, true))
                .collect::<Vec<_>>(),
        )
    }

    fn recebidas(filters: Vec<Expr>) -> LogicalPlanBuilder {
        table_scan_with_filters(
            Some("recebidas"),
            &schema(&["cliente_rec", "empresa_rec", "valor_rec"]),
            None,
            filters,
        )
        .unwrap()
    }

    fn pessoas(filters: Vec<Expr>) -> LogicalPlanBuilder {
        table_scan_with_filters(
            Some("pessoas"),
            &schema(&["cod_pes", "tipo_pes", "senha_pes"]),
            None,
            filters,
        )
        .unwrap()
    }

    fn keys(left: &str, right: &str) -> (Vec<Column>, Vec<Column>) {
        (
            vec![Column::from_qualified_name(left)],
            vec![Column::from_qualified_name(right)],
        )
    }

    /// `left JOIN right ON left_key = right_key`.
    fn join(
        left: LogicalPlanBuilder,
        kind: JoinType,
        right: LogicalPlanBuilder,
        on: (&str, &str),
    ) -> LogicalPlan {
        left.join(right.build().unwrap(), kind, keys(on.0, on.1), None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn sql(plan: &LogicalPlan) -> String {
        Unparser::new(&DefaultDialect {})
            .plan_to_sql(plan)
            .unwrap()
            .to_string()
    }

    fn rendered(plan: LogicalPlan) -> String {
        let plan = for_unparser(plan).unwrap();
        assert!(renders_faithfully(&plan), "{plan}");
        sql(&plan)
    }

    /// The defect this module works around, still in the unparser: when it
    /// is fixed upstream this test fails, and the rewrite can go.
    #[test]
    fn the_unparser_folds_a_preserved_filter_into_on() {
        let plan = join(
            recebidas(vec![col("recebidas.empresa_rec").eq(lit(1_i64))])
                .alias("r")
                .unwrap(),
            JoinType::Left,
            pessoas(vec![]).alias("p").unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        let plan = LogicalPlanBuilder::from(plan)
            .project(vec![col("r.cliente_rec"), col("p.cod_pes")])
            .unwrap()
            .build()
            .unwrap();
        assert!(
            sql(&plan).ends_with("ON r.cliente_rec = p.cod_pes AND (r.empresa_rec = 1)"),
            "{}",
            sql(&plan)
        );
        assert!(!renders_faithfully(&plan));
    }

    #[test]
    fn a_left_join_keeps_the_filter_of_its_preserved_side_in_where() {
        let plan = join(
            recebidas(vec![col("recebidas.empresa_rec").eq(lit(1_i64))])
                .alias("r")
                .unwrap(),
            JoinType::Left,
            pessoas(vec![]).alias("p").unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        assert_eq!(
            rendered(plan),
            "SELECT r.cliente_rec, r.empresa_rec, r.valor_rec, p.cod_pes, p.tipo_pes, p.senha_pes \
             FROM recebidas AS r LEFT OUTER JOIN pessoas AS p ON r.cliente_rec = p.cod_pes WHERE (r.empresa_rec = 1)"
        );
    }

    #[test]
    fn a_right_join_keeps_the_filter_of_its_preserved_side_in_where() {
        let plan = join(
            recebidas(vec![col("recebidas.empresa_rec").eq(lit(1_i64))])
                .alias("r")
                .unwrap(),
            JoinType::Right,
            pessoas(vec![col("pessoas.tipo_pes").eq(lit(2_i64))])
                .alias("p")
                .unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        assert_eq!(
            rendered(plan),
            "SELECT r.cliente_rec, r.empresa_rec, r.valor_rec, p.cod_pes, p.tipo_pes, p.senha_pes \
             FROM recebidas AS r RIGHT OUTER JOIN pessoas AS p ON r.cliente_rec = p.cod_pes AND (r.empresa_rec = 1) \
             WHERE (p.tipo_pes = 2)"
        );
    }

    #[test]
    fn a_table_without_alias_has_its_filters_lifted_as_they_name_it() {
        let plan = join(
            recebidas(vec![col("empresa_rec").eq(lit(1_i64))])
                .filter(col("recebidas.valor_rec").gt(lit(5_i64)))
                .unwrap(),
            JoinType::Left,
            pessoas(vec![]),
            ("recebidas.cliente_rec", "pessoas.cod_pes"),
        );
        let rendered = rendered(plan);
        assert!(
            rendered.ends_with(
                "FROM recebidas LEFT OUTER JOIN pessoas ON recebidas.cliente_rec = pessoas.cod_pes \
                 WHERE ((recebidas.empresa_rec = 1) AND (recebidas.valor_rec > 5))"
            ),
            "{rendered}"
        );
    }

    #[test]
    fn filters_under_an_alias_are_lifted_naming_the_alias() {
        let plan = join(
            recebidas(vec![])
                .filter(col("recebidas.empresa_rec").eq(lit(85_i64)))
                .unwrap()
                .alias("r")
                .unwrap(),
            JoinType::Left,
            pessoas(vec![]).alias("p").unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        assert!(rendered(plan).ends_with("WHERE (r.empresa_rec = 85)"));
    }

    #[test]
    fn a_full_join_reads_each_filtered_side_as_a_derived_table() {
        let plan = join(
            recebidas(vec![col("recebidas.empresa_rec").eq(lit(1_i64))])
                .alias("r")
                .unwrap(),
            JoinType::Full,
            pessoas(vec![col("pessoas.tipo_pes").eq(lit(2_i64))])
                .alias("p")
                .unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        assert_eq!(
            rendered(plan),
            "SELECT r.cliente_rec, r.empresa_rec, r.valor_rec, p.cod_pes, p.tipo_pes, p.senha_pes \
             FROM (SELECT r.cliente_rec, r.empresa_rec, r.valor_rec FROM recebidas AS r \
             WHERE (r.empresa_rec = 1)) AS r FULL JOIN (SELECT p.cod_pes, p.tipo_pes, \
             p.senha_pes FROM pessoas AS p WHERE (p.tipo_pes = 2)) AS p ON r.cliente_rec = p.cod_pes"
        );
    }

    #[test]
    fn a_full_join_of_a_filtered_table_without_alias_stays_in_the_engine() {
        let plan = join(
            recebidas(vec![col("empresa_rec").eq(lit(1_i64))]),
            JoinType::Full,
            pessoas(vec![]),
            ("recebidas.cliente_rec", "pessoas.cod_pes"),
        );
        assert!(!renders_faithfully(&for_unparser(plan).unwrap()));
    }

    /// The side a join does not preserve under a filter of a derived
    /// table: in the shared `WHERE` the filter would drop the rows the
    /// join adds for it.
    #[test]
    fn a_filter_the_join_adds_rows_against_moves_into_a_derived_table() {
        let counts = pessoas(vec![])
            .aggregate(vec![col("pessoas.cod_pes")], Vec::<Expr>::new())
            .unwrap();
        // Under the alias: becomes a derived table of its own.
        let aliased = counts
            .clone()
            .filter(col("pessoas.cod_pes").gt(lit(1_i64)))
            .unwrap()
            .alias("c")
            .unwrap();
        let plan = join(
            recebidas(vec![]).alias("r").unwrap(),
            JoinType::Left,
            aliased,
            ("r.cliente_rec", "c.cod_pes"),
        );
        assert_eq!(
            rendered(plan),
            "SELECT r.cliente_rec, r.empresa_rec, r.valor_rec, c.cod_pes FROM recebidas AS r LEFT OUTER JOIN \
             (SELECT pessoas.cod_pes FROM pessoas GROUP BY pessoas.cod_pes HAVING (pessoas.cod_pes > 1)) AS c \
             ON r.cliente_rec = c.cod_pes"
        );
        // Above the alias: no alias to keep, the join stays in the engine.
        let above = counts
            .alias("c")
            .unwrap()
            .filter(col("c.cod_pes").gt(lit(1_i64)))
            .unwrap();
        let plan = join(
            recebidas(vec![]).alias("r").unwrap(),
            JoinType::Left,
            above.clone(),
            ("r.cliente_rec", "c.cod_pes"),
        );
        assert!(!renders_faithfully(&for_unparser(plan).unwrap()));
        // The side a LEFT join preserves may keep it in `WHERE`.
        let plan = join(
            above,
            JoinType::Left,
            recebidas(vec![]).alias("r").unwrap(),
            ("c.cod_pes", "r.cliente_rec"),
        );
        assert!(rendered(plan).ends_with("WHERE (c.cod_pes > 1)"));
    }

    /// D2: the select list of a join over an aggregated derived table.
    #[test]
    fn a_join_without_a_select_list_gets_the_plans_columns_in_order() {
        let derived = recebidas(vec![])
            .aggregate(
                vec![col("recebidas.cliente_rec"), col("recebidas.empresa_rec")],
                Vec::<Expr>::new(),
            )
            .unwrap()
            .alias("c")
            .unwrap();
        let plan = join(
            derived,
            JoinType::Left,
            pessoas(vec![]).alias("p").unwrap(),
            ("c.cliente_rec", "p.cod_pes"),
        );
        assert!(sql(&plan).starts_with("SELECT * FROM (SELECT"), "{}", sql(&plan));
        assert_eq!(
            rendered(plan),
            "SELECT c.cliente_rec, c.empresa_rec, p.cod_pes, p.tipo_pes, p.senha_pes FROM (SELECT \
             recebidas.cliente_rec, recebidas.empresa_rec FROM recebidas GROUP BY recebidas.cliente_rec, \
             recebidas.empresa_rec) AS c LEFT OUTER JOIN pessoas AS p ON c.cliente_rec = p.cod_pes"
        );
    }

    #[test]
    fn the_select_list_goes_above_filters_and_below_sorts_and_limits() {
        let joined = join(
            recebidas(vec![]).alias("r").unwrap(),
            JoinType::Inner,
            pessoas(vec![]).alias("p").unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        let plan = LogicalPlanBuilder::from(joined)
            .filter(col("p.tipo_pes").eq(lit(1_i64)))
            .unwrap()
            .sort(vec![col("r.cliente_rec").sort(true, false)])
            .unwrap()
            .limit(0, Some(3))
            .unwrap()
            .build()
            .unwrap();
        let rewritten = for_unparser(plan).unwrap();
        let LogicalPlan::Limit(limit) = &rewritten else {
            panic!("{rewritten}")
        };
        let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
            panic!("{rewritten}")
        };
        let LogicalPlan::Projection(projection) = sort.input.as_ref() else {
            panic!("{rewritten}")
        };
        assert!(matches!(projection.input.as_ref(), LogicalPlan::Filter(_)));
        assert_eq!(projection.expr.len(), 6);
        // Nothing to add over a projection, an aggregation or a table.
        let scan = recebidas(vec![]).build().unwrap();
        assert_eq!(for_unparser(scan.clone()).unwrap(), scan);
        let sorted = recebidas(vec![])
            .sort(vec![col("cliente_rec").sort(true, false)])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(for_unparser(sorted.clone()).unwrap(), sorted);
    }

    #[test]
    fn each_input_of_a_union_lists_its_columns() {
        let joined = || {
            join(
                recebidas(vec![]).alias("r").unwrap(),
                JoinType::Inner,
                pessoas(vec![]).alias("p").unwrap(),
                ("r.cliente_rec", "p.cod_pes"),
            )
        };
        let plan = LogicalPlanBuilder::from(joined())
            .union(joined())
            .unwrap()
            .build()
            .unwrap();
        let rewritten = for_unparser(plan).unwrap();
        let LogicalPlan::Union(union) = &rewritten else {
            panic!("{rewritten}")
        };
        assert!(
            union
                .inputs
                .iter()
                .all(|input| matches!(input.as_ref(), LogicalPlan::Projection(_)))
        );
        let scans = LogicalPlanBuilder::from(recebidas(vec![]).build().unwrap())
            .union(recebidas(vec![]).build().unwrap())
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(for_unparser(scans.clone()).unwrap(), scans);
    }

    #[test]
    fn a_join_as_the_right_input_stays_in_the_engine_unless_aliased() {
        let inner = join(
            pessoas(vec![]).alias("p").unwrap(),
            JoinType::Inner,
            recebidas(vec![]).alias("c").unwrap(),
            ("p.cod_pes", "c.cliente_rec"),
        );
        let plan = recebidas(vec![])
            .alias("r")
            .unwrap()
            .join(
                inner.clone(),
                JoinType::Inner,
                keys("r.cliente_rec", "p.cod_pes"),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(!renders_faithfully(&for_unparser(plan).unwrap()));
        // Aliased, it becomes a derived table with its own list.
        let aliased = LogicalPlanBuilder::from(inner)
            .project(vec![col("p.cod_pes"), col("c.valor_rec")])
            .unwrap()
            .alias("x")
            .unwrap()
            .build()
            .unwrap();
        let plan = recebidas(vec![])
            .alias("r")
            .unwrap()
            .join(aliased, JoinType::Left, keys("r.cliente_rec", "x.cod_pes"), None)
            .unwrap()
            .build()
            .unwrap();
        assert!(rendered(plan).contains("LEFT OUTER JOIN (SELECT p.cod_pes, c.valor_rec FROM pessoas AS p"));
    }

    #[test]
    fn an_inner_join_whose_filters_go_to_where_may_not_be_the_side_an_outer_join_completes() {
        let inner = join(
            recebidas(vec![col("recebidas.empresa_rec").eq(lit(1_i64))])
                .alias("r")
                .unwrap(),
            JoinType::Inner,
            pessoas(vec![]).alias("p").unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        let completed = LogicalPlanBuilder::from(inner.clone())
            .join(
                table_scan(Some("c"), &schema(&["k"]), None)
                    .unwrap()
                    .build()
                    .unwrap(),
                JoinType::Right,
                keys("r.cliente_rec", "c.k"),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(!renders_faithfully(&for_unparser(completed).unwrap()));
        let kept = LogicalPlanBuilder::from(inner)
            .join(
                table_scan(Some("c"), &schema(&["k"]), None)
                    .unwrap()
                    .build()
                    .unwrap(),
                JoinType::Left,
                keys("r.cliente_rec", "c.k"),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(rendered(kept).ends_with("WHERE (r.empresa_rec = 1)"));
    }

    #[test]
    fn shapes_the_unparser_does_not_write_as_tables_stay_in_the_engine() {
        // A scan with a row limit of its own.
        let limited = LogicalPlanBuilder::from(recebidas(vec![]).build().unwrap())
            .limit(0, Some(5))
            .unwrap();
        let LogicalPlan::Limit(limit) = limited.clone().build().unwrap() else {
            panic!()
        };
        let mut scan = match limit.input.as_ref() {
            LogicalPlan::TableScan(scan) => scan.clone(),
            other => panic!("{other}"),
        };
        scan.fetch = Some(5);
        let fetched = LogicalPlanBuilder::from(LogicalPlan::TableScan(scan));
        assert_eq!(shape(&fetched.clone().build().unwrap()), Shape::Unknown);
        let plan = join(
            fetched,
            JoinType::Inner,
            pessoas(vec![]),
            ("recebidas.cliente_rec", "pessoas.cod_pes"),
        );
        assert!(!renders_faithfully(&for_unparser(plan).unwrap()));
        // A projection without an alias.
        let projected = recebidas(vec![]).project(vec![col("cliente_rec")]).unwrap();
        assert_eq!(shape(&projected.build().unwrap()), Shape::Unknown);
        // Semi joins render as `EXISTS`, their inputs apart.
        let semi = join(
            recebidas(vec![]),
            JoinType::LeftSemi,
            pessoas(vec![]),
            ("recebidas.cliente_rec", "pessoas.cod_pes"),
        );
        assert!(renders_faithfully(&semi));
        assert_eq!(for_unparser(semi.clone()).unwrap(), semi);
    }

    #[test]
    fn shapes_of_join_inputs() {
        assert_eq!(shape(&recebidas(vec![]).build().unwrap()), Shape::Plain);
        assert_eq!(
            shape(&recebidas(vec![lit(true)]).build().unwrap()),
            Shape::FilteredScan
        );
        assert_eq!(
            shape(&recebidas(vec![]).alias("r").unwrap().build().unwrap()),
            Shape::Plain
        );
        assert_eq!(
            shape(
                &recebidas(vec![])
                    .filter(lit(true))
                    .unwrap()
                    .alias("r")
                    .unwrap()
                    .build()
                    .unwrap()
            ),
            Shape::FilteredScan
        );
        assert_eq!(
            shape(
                &recebidas(vec![])
                    .alias("r")
                    .unwrap()
                    .filter(lit(true))
                    .unwrap()
                    .build()
                    .unwrap()
            ),
            Shape::FilteredScan
        );
        let derived = recebidas(vec![])
            .aggregate(vec![col("cliente_rec")], Vec::<Expr>::new())
            .unwrap()
            .alias("d")
            .unwrap();
        let nested = recebidas(vec![])
            .alias("a")
            .unwrap()
            .filter(lit(true))
            .unwrap()
            .alias("b")
            .unwrap();
        assert_eq!(shape(&nested.build().unwrap()), Shape::Unknown);
        assert_eq!(shape(&derived.clone().build().unwrap()), Shape::Plain);
        assert_eq!(
            shape(&derived.filter(lit(true)).unwrap().build().unwrap()),
            Shape::FilteredDerived
        );
        let joined = |kind, filters| {
            join(
                recebidas(filters).alias("r").unwrap(),
                kind,
                pessoas(vec![]).alias("p").unwrap(),
                ("r.cliente_rec", "p.cod_pes"),
            )
        };
        assert_eq!(
            shape(&joined(JoinType::Inner, vec![])),
            Shape::Joins { shared_where: false }
        );
        assert_eq!(
            shape(&joined(JoinType::Inner, vec![lit(true)])),
            Shape::Joins { shared_where: true }
        );
        assert_eq!(
            shape(&joined(JoinType::Left, vec![lit(true)])),
            Shape::Joins { shared_where: false }
        );
        let filtered = LogicalPlanBuilder::from(joined(JoinType::Left, vec![]))
            .filter(lit(true))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(shape(&filtered), Shape::Joins { shared_where: true });
        assert!(
            !Shape::Plain.shared_where()
                && !Shape::FilteredScan.shared_where()
                && !Shape::Unknown.shared_where()
        );
    }

    #[test]
    fn shapes_propagate_through_filters_and_joins() {
        let derived = || {
            recebidas(vec![])
                .aggregate(vec![col("cliente_rec")], Vec::<Expr>::new())
                .unwrap()
                .alias("d")
                .unwrap()
        };
        let twice = derived()
            .filter(lit(true))
            .unwrap()
            .filter(lit(false))
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(shape(&twice), Shape::FilteredDerived);
        let over_projection = recebidas(vec![])
            .project(vec![col("cliente_rec")])
            .unwrap()
            .filter(lit(true))
            .unwrap();
        assert_eq!(shape(&over_projection.clone().build().unwrap()), Shape::Unknown);
        // A join with an input of unknown shape is unknown.
        let unknown = over_projection.join(
            pessoas(vec![]).build().unwrap(),
            JoinType::Inner,
            keys("recebidas.cliente_rec", "pessoas.cod_pes"),
            None,
        );
        assert_eq!(shape(&unknown.unwrap().build().unwrap()), Shape::Unknown);
        let right_unknown = pessoas(vec![])
            .join(
                recebidas(vec![])
                    .project(vec![col("cliente_rec")])
                    .unwrap()
                    .build()
                    .unwrap(),
                JoinType::Inner,
                keys("pessoas.cod_pes", "recebidas.cliente_rec"),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(shape(&right_unknown), Shape::Unknown);
        assert!(!renders_faithfully(&right_unknown));
        // A filter of the preserved side reaches the shared `WHERE` of the
        // joins above, from either input.
        let filtered = derived().filter(lit(true)).unwrap();
        let left = join(
            filtered.clone(),
            JoinType::Left,
            pessoas(vec![]).alias("p").unwrap(),
            ("d.cliente_rec", "p.cod_pes"),
        );
        assert_eq!(shape(&left), Shape::Joins { shared_where: true });
        let right = join(
            pessoas(vec![]).alias("p").unwrap(),
            JoinType::Right,
            filtered,
            ("p.cod_pes", "d.cliente_rec"),
        );
        assert_eq!(shape(&right), Shape::Joins { shared_where: true });
    }

    #[test]
    fn each_join_type_refuses_the_shapes_it_would_write_wrong() {
        let filtered = || recebidas(vec![lit(true)]).alias("r").unwrap();
        let plain = || pessoas(vec![]).alias("p").unwrap();
        let on = ("r.cliente_rec", "p.cod_pes");
        // As left unchanged by the rewrite: the filter of the preserved side
        // folded into `ON`.
        assert!(!renders_faithfully(&join(
            plain(),
            JoinType::Right,
            filtered(),
            ("p.cod_pes", "r.cliente_rec")
        )));
        assert!(!renders_faithfully(&join(
            filtered(),
            JoinType::Left,
            plain(),
            on
        )));
        assert!(!renders_faithfully(&join(
            filtered(),
            JoinType::Full,
            plain(),
            on
        )));
        assert!(!renders_faithfully(&join(
            plain(),
            JoinType::Full,
            filtered(),
            ("p.cod_pes", "r.cliente_rec")
        )));
        // The side the join completes folds into `ON`: right.
        assert!(renders_faithfully(&join(
            filtered(),
            JoinType::Right,
            plain(),
            on
        )));
        assert!(renders_faithfully(&join(
            plain(),
            JoinType::Left,
            filtered(),
            ("p.cod_pes", "r.cliente_rec")
        )));
        assert!(renders_faithfully(&join(
            filtered(),
            JoinType::Inner,
            plain(),
            on
        )));
        // FULL over joins without a shared `WHERE` on the left.
        let joined = join(
            plain(),
            JoinType::Inner,
            recebidas(vec![]).alias("r").unwrap(),
            ("p.cod_pes", "r.cliente_rec"),
        );
        let full = LogicalPlanBuilder::from(joined)
            .join(
                table_scan(Some("c"), &schema(&["k"]), None)
                    .unwrap()
                    .build()
                    .unwrap(),
                JoinType::Full,
                keys("r.cliente_rec", "c.k"),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        assert!(renders_faithfully(&full));
        // A join as the right input of a LEFT one.
        let nested = join(
            plain(),
            JoinType::Inner,
            recebidas(vec![]).alias("r").unwrap(),
            ("p.cod_pes", "r.cliente_rec"),
        );
        let left = table_scan(Some("c"), &schema(&["k"]), None)
            .unwrap()
            .join(nested, JoinType::Left, keys("c.k", "p.cod_pes"), None)
            .unwrap()
            .build()
            .unwrap();
        assert!(!renders_faithfully(&left));
    }

    #[test]
    fn an_aliased_input_merged_into_the_select_becomes_a_derived_table() {
        // A filter over an aggregation under an alias, in an inner join and a
        // FULL one: a derived table of its own.
        let having = || {
            pessoas(vec![])
                .aggregate(vec![col("pessoas.cod_pes")], Vec::<Expr>::new())
                .unwrap()
                .filter(col("pessoas.cod_pes").gt(lit(1_i64)))
                .unwrap()
                .alias("c")
                .unwrap()
        };
        assert_eq!(shape(&having().build().unwrap()), Shape::Unknown);
        for kind in [JoinType::Inner, JoinType::Full] {
            let plan = join(
                recebidas(vec![]).alias("r").unwrap(),
                kind,
                having(),
                ("r.cliente_rec", "c.cod_pes"),
            );
            assert!(
                rendered(plan)
                    .contains("(SELECT pessoas.cod_pes FROM pessoas GROUP BY pessoas.cod_pes HAVING")
            );
        }
        // Not over `DISTINCT`: the projection would make it compare fewer columns.
        let distinct = LogicalPlanBuilder::from(LogicalPlan::Distinct(
            datafusion::logical_expr::Distinct::All(Arc::new(pessoas(vec![]).build().unwrap())),
        ))
        .alias("d")
        .unwrap();
        let plan = join(
            recebidas(vec![]).alias("r").unwrap(),
            JoinType::Inner,
            distinct,
            ("r.cliente_rec", "d.cod_pes"),
        );
        assert!(!renders_faithfully(&for_unparser(plan).unwrap()));
    }

    #[test]
    fn a_relation_is_single_only_when_every_column_names_it() {
        assert_eq!(
            single_relation(&recebidas(vec![]).alias("r").unwrap().build().unwrap()),
            Some(TableReference::bare("r"))
        );
        let joined = join(
            recebidas(vec![]).alias("r").unwrap(),
            JoinType::Inner,
            pessoas(vec![]).alias("p").unwrap(),
            ("r.cliente_rec", "p.cod_pes"),
        );
        assert_eq!(single_relation(&joined), None);
        let unqualified = recebidas(vec![])
            .project(vec![col("cliente_rec").alias("k")])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(single_relation(&unqualified), None);
    }

    /// Through the engine's optimizer: the statements of the shadow run's
    /// minimal cases, as the source receives them.
    #[derive(Debug)]
    struct EverythingButAggregates;

    impl PushdownPolicy for EverythingButAggregates {
        fn node_is_pushable(&self, plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::TableScan(scan) => scan.filters.is_empty(),
                LogicalPlan::Aggregate(a) => a.aggr_expr.is_empty(),
                LogicalPlan::Extension(_) => false,
                _ => true,
            }
        }
    }

    async fn sent(sql: &str) -> Vec<String> {
        let ctx = crate::testing::federated_session();
        let (tables, statements) = crate::testing::remote_tables_recording(
            "erp",
            vec![
                (
                    "recebidas",
                    vec![
                        ("cliente_rec", int64(&[1, 2, 3])),
                        ("empresa_rec", int64(&[1, 2, 1])),
                        ("valor_rec", int64(&[10, 20, 30])),
                    ],
                ),
                (
                    "contasreceber",
                    vec![
                        ("cliente_prc", int64(&[1, 4, 5])),
                        ("empresa_prc", int64(&[1, 2, 1])),
                        ("valor_prc", int64(&[5, 6, 7])),
                    ],
                ),
                (
                    "pessoas",
                    vec![
                        ("cod_pes", int64(&[1, 2, 3])),
                        ("nome_pes", strings(&["a", "b", "c"])),
                        ("senha_pes", strings(&["x", "y", "z"])),
                    ],
                ),
            ],
            Some(Arc::new(EverythingButAggregates)),
        );
        for (name, table) in ["recebidas", "contasreceber", "pessoas"].into_iter().zip(tables) {
            ctx.register_table(name, table).unwrap();
        }
        // The fake source cannot join: only what it was sent matters.
        let _ = ctx.sql(sql).await.unwrap().collect().await;
        let mut sent = statements.lock().unwrap().clone();
        sent.dedup();
        sent
    }

    #[tokio::test]
    async fn the_payers_list_reaches_the_source_with_its_filter_in_where() {
        assert_eq!(
            sent(
                "SELECT r.cliente_rec, max(p.nome_pes) FROM recebidas r LEFT JOIN pessoas p ON p.cod_pes = r.cliente_rec \
                 WHERE r.empresa_rec IN (1, 85) AND r.cliente_rec IS NOT NULL GROUP BY r.cliente_rec"
            )
            .await,
            ["SELECT r.cliente_rec, p.nome_pes FROM recebidas AS r LEFT OUTER JOIN pessoas AS p ON (p.cod_pes = r.cliente_rec) \
              WHERE (((r.empresa_rec = 1) OR (r.empresa_rec = 85)) AND r.cliente_rec IS NOT NULL)"]
        );
    }

    #[tokio::test]
    async fn a_join_over_a_derived_aggregation_lists_the_columns_the_engine_reads() {
        assert_eq!(
            sent(
                "SELECT c.k, max(p.nome_pes) AS nome FROM (SELECT cliente_rec AS k, empresa_rec AS e FROM recebidas \
                 WHERE empresa_rec = 1 UNION SELECT cliente_prc, empresa_prc FROM contasreceber WHERE empresa_prc = 1) c \
                 LEFT JOIN pessoas p ON p.cod_pes = c.k GROUP BY c.k"
            )
            .await,
            ["SELECT c.k, p.nome_pes FROM (SELECT k, e FROM (SELECT recebidas.cliente_rec AS k, recebidas.empresa_rec AS e \
              FROM recebidas WHERE (recebidas.empresa_rec = 1) UNION ALL SELECT contasreceber.cliente_prc AS k, \
              contasreceber.empresa_prc AS e FROM contasreceber WHERE (contasreceber.empresa_prc = 1)) GROUP BY k, e) AS c \
              LEFT OUTER JOIN pessoas AS p ON (p.cod_pes = c.k)"]
        );
    }
}
