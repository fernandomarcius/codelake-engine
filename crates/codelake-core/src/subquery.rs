//! Pre-evaluation of uncorrelated scalar subqueries in predicates.
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
//! Only subqueries without outer references in `WHERE`/`JOIN … ON` predicates
//! are pre-evaluated. The result follows SQL semantics: no row is NULL, more
//! than one row is an error.

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, LogicalPlan, Subquery, cast, lit};
use datafusion::prelude::SessionContext;

use crate::functions::binary_literal_expr;

/// Upper bound on substitutions per statement, against runaway rewrites.
const MAX_SUBSTITUTIONS: usize = 64;

fn predicates(plan: &LogicalPlan) -> Vec<&Expr> {
    match plan {
        LogicalPlan::Filter(filter) => vec![&filter.predicate],
        LogicalPlan::Join(join) => join.filter.iter().collect(),
        _ => vec![],
    }
}

/// First uncorrelated scalar subquery found in a predicate of `plan`.
fn find(plan: &LogicalPlan) -> Result<Option<Subquery>> {
    let mut found = None;
    plan.apply(|node| {
        for predicate in predicates(node) {
            predicate.apply(|expr| {
                if let Expr::ScalarSubquery(subquery) = expr
                    && subquery.outer_ref_columns.is_empty()
                {
                    found = Some(subquery.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            if found.is_some() {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
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
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let mut rows = batches.iter().filter(|b| b.num_rows() > 0);
    let Some(batch) = rows.next() else {
        return ScalarValue::try_from(&field_type);
    };
    if batch.num_rows() > 1 || rows.next().is_some() {
        return Err(DataFusionError::Execution(
            "scalar subquery returned more than one row".into(),
        ));
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
        other => lit(other),
    }
}

fn substitute(plan: LogicalPlan, target: &Subquery, replacement: &Expr) -> Result<LogicalPlan> {
    plan.transform(|node| {
        if predicates(&node).is_empty() {
            return Ok(Transformed::no(node));
        }
        node.map_expressions(|expr| {
            expr.transform(|e| match &e {
                Expr::ScalarSubquery(s) if s == target => Ok(Transformed::yes(replacement.clone())),
                _ => Ok(Transformed::no(e)),
            })
        })
    })
    .data()
}

/// Replaces every uncorrelated scalar subquery in `plan`'s predicates by its value.
pub async fn pre_evaluate_scalar_subqueries(
    ctx: &SessionContext,
    mut plan: LogicalPlan,
) -> Result<LogicalPlan> {
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
        assert!(err.to_string().contains("more than one row"), "{err}");
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
}
