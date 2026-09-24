//! Scalar subqueries: pre-evaluation for pushdown and the one-row rule.
//!
//! The reference incremental models filter a remote CDC table by a watermark read
//! from another catalog:
//!
//! ```sql
//! WHERE "__$start_lsn" > (SELECT COALESCE(MAX(lsn), X'00') FROM hive.raw.t)
//! ```
//!
//! `DataFusion` turns the subquery into a join with a non-equi predicate, which
//! produces no dynamic filter: the whole remote table would be read on every
//! run. Evaluating the subquery first and substituting its value as a literal
//! lets the predicate reach the source (`WHERE [__$start_lsn] > 0x…`).
//!
//! Every deterministic subquery without outer references is pre-evaluated, in
//! any expression of the statement (`SELECT` list, `WHERE`, `HAVING`, `JOIN …
//! ON`, …), except in a view definition, whose value must not be frozen.
//!
//! A scalar subquery follows SQL semantics: no row is NULL, more than one row
//! is an error ([`SubqueryMultipleRows`], Trino's `SUBQUERY_MULTIPLE_ROWS`).
//! `DataFusion` does not enforce it: it plans the subquery as a join, which
//! repeats the outer row once per subquery row and drops it when there is
//! none. [`EnforceScalarSubqueryRows`] wraps every scalar subquery whose plan
//! does not already guarantee this in an aggregation by [`single_row`], which
//! yields the row's value, NULL for no row, and fails on a second row. For a
//! correlated subquery the aggregation is grouped by the correlation (one
//! check per outer row), which also lets `DataFusion` decorrelate subqueries it
//! would otherwise reject as not aggregated.

use std::any::Any;
use std::mem::size_of_val;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, UInt64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, DdlStatement, Expr, LogicalPlan, LogicalPlanBuilder,
    Signature, Subquery, Volatility, cast, lit,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::functions::binary_literal_expr;

/// Upper bound on substitutions per statement, against runaway rewrites.
const MAX_SUBSTITUTIONS: usize = 64;

/// A scalar subquery returned more than one row. The error the engine raises
/// for Trino's `SUBQUERY_MULTIPLE_ROWS`, carried as
/// [`DataFusionError::External`] so protocols can recognise it by type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Scalar sub-query has returned multiple rows")]
pub struct SubqueryMultipleRows;

impl SubqueryMultipleRows {
    pub fn error() -> DataFusionError {
        DataFusionError::External(Box::new(Self))
    }

    /// Whether `error` (its root cause) is this error.
    pub fn is(error: &DataFusionError) -> bool {
        matches!(error.find_root(), DataFusionError::External(e) if e.is::<Self>())
    }
}

/// A subquery calling `now()`, `random()` or any non-immutable function would
/// see a different value when evaluated separately from the main query.
fn is_deterministic(plan: &LogicalPlan) -> bool {
    let mut deterministic = true;
    let _ = plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if let Expr::ScalarFunction(f) = e
                    && f.func.signature().volatility != Volatility::Immutable
                {
                    deterministic = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    });
    deterministic
}

/// First uncorrelated, deterministic scalar subquery in any expression of `plan`.
fn find(plan: &LogicalPlan) -> Result<Option<Subquery>> {
    let mut found = None;
    plan.apply(|node| {
        node.apply_expressions(|expression| {
            expression.apply(|expr| {
                if let Expr::ScalarSubquery(subquery) = expr
                    && subquery.outer_ref_columns.is_empty()
                    && is_deterministic(&subquery.subquery)
                {
                    found = Some(subquery.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    Ok(found)
}

async fn evaluate(ctx: &SessionContext, subquery: &Subquery) -> Result<ScalarValue> {
    let plan = subquery.subquery.as_ref().clone();
    let field_type = plan
        .schema()
        .fields()
        .first()
        .map(|f| f.data_type().clone())
        .ok_or_else(|| DataFusionError::Plan("scalar subquery returns no column".into()))?;
    // Two rows are enough to know the answer.
    let plan = LogicalPlanBuilder::from(plan).limit(0, Some(2))?.build()?;
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let mut rows = batches.iter().filter(|b| b.num_rows() > 0);
    let Some(batch) = rows.next() else {
        return ScalarValue::try_from(&field_type);
    };
    if batch.num_rows() > 1 || rows.next().is_some() {
        return Err(SubqueryMultipleRows::error());
    }
    ScalarValue::try_from_array(batch.column(0), 0)
}

fn literal_for(value: ScalarValue) -> Expr {
    match value {
        ScalarValue::Binary(Some(bytes)) => binary_literal_expr(&bytes),
        ScalarValue::LargeBinary(Some(bytes)) => cast(
            binary_literal_expr(&bytes),
            datafusion::arrow::datatypes::DataType::LargeBinary,
        ),
        ScalarValue::FixedSizeBinary(size, Some(bytes)) => cast(
            binary_literal_expr(&bytes),
            datafusion::arrow::datatypes::DataType::FixedSizeBinary(size),
        ),
        // A cast keeps the value `bigint`: a bare literal that fits 32 bits
        // would become `integer` (see `crate::coercion`).
        ScalarValue::Int64(value) => cast(
            lit(ScalarValue::Int64(value)),
            datafusion::arrow::datatypes::DataType::Int64,
        ),
        other => lit(other),
    }
}

/// Replaces `target` by `replacement` everywhere in `plan`; output columns
/// keep their names (a projected subquery is named after its column).
fn substitute(plan: LogicalPlan, target: &Subquery, replacement: &Expr) -> Result<LogicalPlan> {
    plan.transform(|node| {
        let names = NamePreserver::new(&node);
        let rewritten = node.map_expressions(|expr| {
            let name = names.save(&expr);
            expr.transform(|e| match &e {
                Expr::ScalarSubquery(s) if s == target => Ok(Transformed::yes(replacement.clone())),
                _ => Ok(Transformed::no(e)),
            })
            .map(|t| t.update_data(|e| name.restore(e)))
        })?;
        if rewritten.transformed {
            rewritten.map_data(LogicalPlan::recompute_schema)
        } else {
            Ok(rewritten)
        }
    })
    .data()
}

/// Replaces every uncorrelated, deterministic scalar subquery in `plan` by its
/// value (see the module documentation).
pub async fn pre_evaluate_scalar_subqueries(
    ctx: &SessionContext,
    mut plan: LogicalPlan,
) -> Result<LogicalPlan> {
    if matches!(plan, LogicalPlan::Ddl(DdlStatement::CreateView(_))) {
        return Ok(plan);
    }
    for _ in 0..MAX_SUBSTITUTIONS {
        let Some(subquery) = find(&plan)? else {
            return Ok(plan);
        };
        let value = evaluate(ctx, &subquery).await?;
        plan = substitute(plan, &subquery, &literal_for(value))?;
    }
    Err(DataFusionError::Plan(format!(
        "more than {MAX_SUBSTITUTIONS} scalar subqueries in one statement"
    )))
}

/// `codelake_single_row(x)`: the value of the only input row, NULL without
/// rows, [`SubqueryMultipleRows`] on a second row. Counts rows, not values, so
/// two NULL rows are two rows.
pub fn single_row() -> Arc<AggregateUDF> {
    static UDAF: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| {
        Arc::new(AggregateUDF::new_from_impl(SingleRow {
            signature: Signature::any(1, Volatility::Immutable),
        }))
    });
    Arc::clone(&UDAF)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SingleRow {
    signature: Signature,
}

impl AggregateUDFImpl for SingleRow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "codelake_single_row"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types
            .first()
            .cloned()
            .ok_or_else(|| DataFusionError::Plan("codelake_single_row expects one argument".into()))
    }

    fn accumulator(&self, args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SingleRowAccumulator {
            value: ScalarValue::try_from(args.return_type())?,
            rows: 0,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs<'_>) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "value"),
                args.return_type().clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(args.name, "rows"),
                DataType::UInt64,
                false,
            )),
        ])
    }
}

#[derive(Debug)]
struct SingleRowAccumulator {
    value: ScalarValue,
    rows: u64,
}

impl SingleRowAccumulator {
    fn add(&mut self, values: &ArrayRef, index: usize, rows: u64) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        if self.rows + rows > 1 {
            return Err(SubqueryMultipleRows::error());
        }
        self.value = ScalarValue::try_from_array(values, index)?;
        self.rows += rows;
        Ok(())
    }
}

impl Accumulator for SingleRowAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let rows = u64::try_from(values.len()).unwrap_or(u64::MAX);
        self.add(values, 0, rows)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = states[1].as_primitive::<UInt64Type>();
        for index in 0..counts.len() {
            let rows = if counts.is_valid(index) {
                counts.value(index)
            } else {
                0
            };
            self.add(&states[0], index, rows)?;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.value.clone(), ScalarValue::UInt64(Some(self.rows))])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.value.size() - size_of_val(&self.value)
    }
}

fn strip_projections(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Projection(p) => strip_projections(&p.input),
        LogicalPlan::SubqueryAlias(a) => strip_projections(&a.input),
        other => other,
    }
}

/// Whether `DataFusion`'s plan for `subquery` already has scalar semantics.
///
/// Uncorrelated: only an aggregation without `GROUP BY` (exactly one row);
/// `DataFusion` joins any other subquery, so no row drops the outer row and
/// two rows repeat it. Correlated: what `DataFusion` accepts, an aggregation
/// grouped only by correlated columns (outer join, one row per outer row) or a
/// plan of at most one row.
fn is_scalar(subquery: &Subquery) -> bool {
    let plan = subquery.subquery.as_ref();
    let inner = strip_projections(plan);
    if subquery.outer_ref_columns.is_empty() {
        return matches!(inner, LogicalPlan::Aggregate(aggregate) if aggregate.group_expr.is_empty());
    }
    plan.max_rows().is_some_and(|rows| rows <= 1)
        || match inner {
            LogicalPlan::Aggregate(_) => true,
            LogicalPlan::Filter(filter) => matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)),
            _ => false,
        }
}

/// `(SELECT x …)` → `(SELECT codelake_single_row(x) AS x FROM (SELECT x …))`.
fn single_row_subquery(subquery: &Subquery) -> Result<Subquery> {
    let plan = subquery.subquery.as_ref().clone();
    let (qualifier, field) = plan.schema().qualified_field(0);
    let (qualifier, name) = (qualifier.cloned(), field.name().clone());
    let value = Expr::Column(Column::new(qualifier.clone(), name.clone()));
    let aggregated = LogicalPlanBuilder::from(plan)
        .aggregate(Vec::<Expr>::new(), vec![single_row().call(vec![value])])?
        .build()?;
    let (agg_qualifier, agg_field) = aggregated.schema().qualified_field(0);
    let output =
        Expr::Column(Column::new(agg_qualifier.cloned(), agg_field.name())).alias_qualified(qualifier, name);
    let projected = LogicalPlanBuilder::from(aggregated)
        .project(vec![output])?
        .build()?;
    Ok(subquery.with_plan(Arc::new(projected)))
}

/// Analyzer rule: see the module documentation.
#[derive(Debug, Default)]
pub struct EnforceScalarSubqueryRows;

impl AnalyzerRule for EnforceScalarSubqueryRows {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| {
            let rewritten = node.map_expressions(|expr| {
                expr.transform_up(|e| match e {
                    Expr::ScalarSubquery(subquery) if !is_scalar(&subquery) => Ok(Transformed::yes(
                        Expr::ScalarSubquery(single_row_subquery(&subquery)?),
                    )),
                    other => Ok(Transformed::no(other)),
                })
            })?;
            if rewritten.transformed {
                rewritten.map_data(LogicalPlan::recompute_schema)
            } else {
                Ok(rewritten)
            }
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "codelake_enforce_scalar_subquery_rows"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{AsArray, Int64Array};
    use datafusion::arrow::datatypes::Int64Type;

    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(crate::functions::varbinary_literal());
        ctx.sql("CREATE TABLE src AS VALUES (1, X'0001'), (2, X'0002'), (3, X'0003')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE wm AS VALUES (X'0001', 10), (X'0002', 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE empty_wm (lsn BYTEA)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx
    }

    async fn prepared(ctx: &SessionContext, sql: &str) -> Result<LogicalPlan> {
        let plan = ctx.state().create_logical_plan(sql).await?;
        pre_evaluate_scalar_subqueries(ctx, plan).await
    }

    async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let plan = prepared(ctx, sql).await.unwrap();
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
            .collect()
    }

    #[tokio::test]
    async fn watermark_subquery_becomes_a_binary_literal() {
        let ctx = ctx().await;
        let sql = "SELECT column1 FROM src WHERE column2 > (SELECT max(column1) FROM wm) ORDER BY column1";
        let plan = prepared(&ctx, sql).await.unwrap();
        let text = plan.display_indent().to_string();
        assert!(
            !text.contains("ScalarSubquery") && !text.contains("Subquery:"),
            "{text}"
        );
        assert!(text.contains("codelake_varbinary(Utf8(\"0002\"))"), "{text}");
        assert_eq!(ids(&ctx, sql).await, vec![3]);
    }

    #[tokio::test]
    async fn coalesce_default_applies_when_the_watermark_is_empty() {
        let ctx = ctx().await;
        let sql = "SELECT column1 FROM src WHERE column2 > (SELECT COALESCE(max(lsn), X'00') FROM empty_wm) ORDER BY column1";
        assert_eq!(ids(&ctx, sql).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn empty_subquery_is_null_and_matches_nothing() {
        let ctx = ctx().await;
        let sql = "SELECT column1 FROM src WHERE column2 > (SELECT lsn FROM empty_wm)";
        assert!(ids(&ctx, sql).await.is_empty());
    }

    #[tokio::test]
    async fn non_binary_values_become_plain_literals() {
        let ctx = ctx().await;
        let sql =
            "SELECT column1 FROM src WHERE column1 * 10 >= (SELECT max(column2) FROM wm) ORDER BY column1";
        let plan = prepared(&ctx, sql).await.unwrap();
        assert!(
            plan.display_indent().to_string().contains("Int64(20)"),
            "{}",
            plan.display_indent()
        );
        assert_eq!(ids(&ctx, sql).await, vec![2, 3]);
    }

    #[tokio::test]
    async fn more_than_one_row_is_an_error() {
        let ctx = ctx().await;
        let err = prepared(
            &ctx,
            "SELECT column1 FROM src WHERE column2 > (SELECT column1 FROM wm)",
        )
        .await
        .unwrap_err();
        assert!(SubqueryMultipleRows::is(&err), "{err}");
        assert_eq!(
            err.find_root().to_string(),
            "External error: Scalar sub-query has returned multiple rows"
        );
    }

    #[tokio::test]
    async fn non_deterministic_subqueries_are_left_alone() {
        let ctx = ctx().await;
        let plan = prepared(
            &ctx,
            "SELECT column1 FROM src WHERE column1 > (SELECT max(column2) FROM wm WHERE now() IS NOT NULL)",
        )
        .await
        .unwrap();
        assert!(
            plan.display_indent().to_string().contains("Subquery"),
            "{}",
            plan.display_indent()
        );
    }

    #[tokio::test]
    async fn correlated_subqueries_are_left_alone() {
        let ctx = ctx().await;
        let sql = "SELECT column1 FROM src s WHERE column2 >= (SELECT max(w.column1) FROM wm w WHERE w.column2 = s.column1 * 10) ORDER BY column1";
        let plan = prepared(&ctx, sql).await.unwrap();
        assert!(
            plan.display_indent().to_string().contains("Subquery"),
            "{}",
            plan.display_indent()
        );
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);
    }

    /// A session with the engine's rules, running statements like the engine:
    /// pre-evaluation first, then `DataFusion`.
    async fn engine_values(sql: &str) -> Result<Vec<Option<i64>>> {
        let ctx = crate::testing::federated_session();
        let plan = ctx.state().create_logical_plan(sql).await?;
        let plan = pre_evaluate_scalar_subqueries(&ctx, plan).await?;
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        Ok(batches
            .iter()
            .flat_map(|b| {
                let column = datafusion::arrow::compute::cast(
                    b.column(0),
                    &datafusion::arrow::datatypes::DataType::Int64,
                )
                .unwrap();
                column.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
            })
            .collect())
    }

    /// Trino 431: `SUBQUERY_MULTIPLE_ROWS` wherever the subquery is.
    #[tokio::test]
    async fn two_rows_fail_in_every_position() {
        let two = "(SELECT a FROM (VALUES (1), (2)) s(a))";
        for sql in [
            format!("SELECT {two} AS y"),
            format!("SELECT x FROM (VALUES (1), (2)) t(x) WHERE x = {two}"),
            format!("SELECT count(*) AS c FROM (VALUES (1)) t(x) HAVING count(*) > {two}"),
            format!("SELECT x FROM (VALUES (1)) t(x) ORDER BY {two}"),
            format!("SELECT x, {two} + x AS y FROM (VALUES (1)) t(x)"),
            format!("SELECT * FROM (SELECT {two} AS y) d"),
            format!("SELECT x FROM (VALUES (1)) t(x) WHERE x IN (SELECT {two})"),
            // Non-deterministic: not pre-evaluated, enforced at execution.
            "SELECT (SELECT a FROM (VALUES (1), (2)) s(a) WHERE random() >= 0) AS y".to_owned(),
            // Two NULL rows are still two rows.
            "SELECT (SELECT a FROM (VALUES (CAST(NULL AS BIGINT)), (NULL)) s(a)) AS y".to_owned(),
            // Correlated: two rows for x = 1.
            "SELECT x, (SELECT b FROM (VALUES (1, 10), (1, 11), (2, 20)) s(k, b) WHERE s.k = t.x) AS y \
             FROM (VALUES (1), (2)) t(x)"
                .to_owned(),
            // Uncorrelated aggregation with several groups.
            "SELECT (SELECT max(b) FROM (VALUES (1, 10), (2, 20)) s(k, b) GROUP BY k) AS y".to_owned(),
        ] {
            let err = engine_values(&sql).await.expect_err(&sql);
            assert!(SubqueryMultipleRows::is(&err), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn zero_or_one_row_is_a_value() {
        let cases: [(&str, Vec<Option<i64>>); 9] = [
            ("SELECT (SELECT a FROM (VALUES (1)) s(a)) AS y", vec![Some(1)]),
            // No row is NULL (Trino: [[null]]), not an empty result.
            (
                "SELECT (SELECT a FROM (VALUES (1)) s(a) WHERE a > 5) AS y",
                vec![None],
            ),
            (
                "SELECT (SELECT a FROM (VALUES (1)) s(a) WHERE a > 5 AND random() >= 0) AS y",
                vec![None],
            ),
            (
                "SELECT (SELECT a FROM (VALUES (1), (1)) s(a) LIMIT 1) AS y",
                vec![Some(1)],
            ),
            (
                "SELECT (SELECT a FROM (VALUES (1), (2)) s(a) WHERE a = 2 AND random() >= 0) AS y",
                vec![Some(2)],
            ),
            // Correlated, not aggregated (Trino: 10, 20, NULL).
            (
                "SELECT (SELECT b FROM (VALUES (1, 10), (2, 20)) s(k, b) WHERE s.k = t.x) AS y \
                 FROM (VALUES (1), (2), (3)) t(x) ORDER BY x",
                vec![Some(10), Some(20), None],
            ),
            // Correlated aggregation: unchanged.
            (
                "SELECT (SELECT sum(b) FROM (VALUES (1, 10), (1, 11), (2, 20)) s(k, b) WHERE s.k = t.x) AS y \
                 FROM (VALUES (1), (2), (3)) t(x) ORDER BY x",
                vec![Some(21), Some(20), None],
            ),
            (
                "SELECT x FROM (VALUES (1), (2)) t(x) WHERE x = (SELECT a FROM (VALUES (2)) s(a))",
                vec![Some(2)],
            ),
            (
                "SELECT x FROM (VALUES (1), (2)) t(x) WHERE x IN (SELECT a FROM (VALUES (1), (2)) s(a)) ORDER BY x",
                vec![Some(1), Some(2)],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                engine_values(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")),
                expected,
                "{sql}"
            );
        }
    }

    #[tokio::test]
    async fn subquery_columns_keep_their_names() {
        let ctx = crate::testing::federated_session();
        for sql in [
            "SELECT (SELECT a FROM (VALUES (1)) s(a))",
            "SELECT (SELECT a FROM (VALUES (1)) s(a) WHERE random() >= 0)",
        ] {
            let plan = ctx.state().create_logical_plan(sql).await.unwrap();
            let expected = plan.schema().field(0).name().clone();
            let plan = pre_evaluate_scalar_subqueries(&ctx, plan).await.unwrap();
            let batches = ctx
                .execute_logical_plan(plan)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(batches[0].schema().field(0).name(), &expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn view_definitions_are_not_frozen() {
        let ctx = crate::testing::federated_session();
        let plan = ctx
            .state()
            .create_logical_plan("CREATE VIEW v AS SELECT (SELECT max(a) FROM (VALUES (1)) s(a)) AS m")
            .await
            .unwrap();
        let plan = pre_evaluate_scalar_subqueries(&ctx, plan).await.unwrap();
        let LogicalPlan::Ddl(DdlStatement::CreateView(view)) = plan else {
            panic!("not a view: {plan}");
        };
        assert!(
            find(&view.input).unwrap().is_some(),
            "{}",
            view.input.display_indent()
        );
    }
}
