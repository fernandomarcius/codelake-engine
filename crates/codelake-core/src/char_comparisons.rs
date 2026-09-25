//! Comparisons of `char(n)` values, refused wherever they are in a plan.
//!
//! A `char(n)` column is text to the engine (Arrow has no char type), with
//! `char(n)` in its field metadata ([`crate::trino_types::TRINO_TYPE`]). The
//! reference semantics compare char values padded with spaces: for a
//! `char(5)` holding `'ab'`, `ch = 'ab'` and `ch = 'ab  '` are both true, and
//! `ch < 'ab!'` compares `'ab   '` with `'ab!  '`. The engine would compare
//! the text as it holds it (padded by a catalog that shows the padding,
//! unpadded after a `CAST … AS char(n)`) and drop or keep other rows, without
//! an error. Until char values compare padded, such a comparison is refused,
//! whether a filter above a `LIMIT` or a window, a join condition, a `CASE`
//! or a projection holds it; a catalog's scan cannot see those. So are the
//! comparisons a plan makes without a comparison expression:
//!
//! - the keys of a join (`USING`, `NATURAL`, and `INTERSECT` and `EXCEPT`,
//!   which are joins on every column);
//! - a `UNION` (with or without `ALL`) holding a char column: it puts values
//!   of other sources in one column (a varchar, or a `CAST … AS char(n)`
//!   the engine holds unpadded), which a `DISTINCT`, a `GROUP BY` or the
//!   `UNION` itself then compares;
//! - the functions that compare their arguments (`nullif`, `greatest`,
//!   `least`).
//!
//! The rule runs before type coercion, so every cast it sees is one the
//! statement wrote. An operand is a char value when its field declares
//! `char(n)` after its aliases are removed; an explicit `CAST(ch AS varchar)`
//! is text, compared as text (the padded value), and is allowed, as are
//! `IS [NOT] NULL`, `ORDER BY` and `GROUP BY`, which compare no char value
//! with other text. Not seen: a char value passed through a function or an
//! aggregate whose result does not declare the type (`max(ch)`,
//! `coalesce(ch, …)`).

use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{
    Alias, Between, Case, InList, InSubquery, Like, ScalarFunction, SetComparison,
};
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

use crate::trino_types::{Declared, declared};

/// Analyzer rule: see the module documentation.
#[derive(Debug, Default)]
pub struct RefuseCharComparisons;

impl AnalyzerRule for RefuseCharComparisons {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.apply_with_subqueries(|node| {
            let schema = input_schema(node);
            for expr in node.expressions() {
                check(&expr, &schema)?;
            }
            if let Some(name) = compared_by_node(node, &schema) {
                return Err(refusal(&name));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "codelake_refuse_char_comparisons"
    }
}

/// The columns a node's expressions can refer to: its inputs' together (a
/// join's condition refers to both sides), or its own for a leaf.
fn input_schema(node: &LogicalPlan) -> Arc<DFSchema> {
    let inputs = node.inputs();
    let Some((first, rest)) = inputs.split_first() else {
        return Arc::clone(node.schema());
    };
    if rest.is_empty() {
        return Arc::clone(first.schema());
    }
    let mut merged = first.schema().as_ref().clone();
    for input in rest {
        merged.merge(input.schema());
    }
    Arc::new(merged)
}

/// A char column `node` compares without a comparison expression: a join
/// key, or a column of a `UNION`.
fn compared_by_node(node: &LogicalPlan, schema: &DFSchema) -> Option<String> {
    match node {
        LogicalPlan::Join(join) => join
            .on
            .iter()
            .flat_map(|(left, right)| [left, right])
            .find_map(|key| char_operand(key, schema)),
        LogicalPlan::Union(union) => union.inputs.iter().find_map(|input| char_column(input)),
        _ => None,
    }
}

/// A char column of `plan`'s output: a projection's expression that is a
/// char value (an explicit cast is not), or a field that declares `char(n)`.
fn char_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Projection(projection) => projection
            .expr
            .iter()
            .find_map(|expr| char_operand(expr, projection.input.schema())),
        _ => plan
            .schema()
            .fields()
            .iter()
            .find(|field| matches!(declared(field), Some(Declared::Char(_))))
            .map(|field| field.name().clone()),
    }
}

fn refusal(name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "comparing the char column {name} is not supported yet: char values compare padded with \
         spaces; compare CAST({name} AS varchar) with the padded value instead"
    ))
}

fn check(expr: &Expr, schema: &DFSchema) -> Result<()> {
    let mut refused = None;
    expr.apply(|e| {
        refused = operands(e)
            .into_iter()
            .find_map(|operand| char_operand(operand, schema));
        Ok(if refused.is_some() {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    match refused {
        None => Ok(()),
        Some(name) => Err(refusal(&name)),
    }
}

/// The values `expr` compares with each other, if it compares values.
fn operands(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if compares(*op) => vec![left, right],
        Expr::Like(Like { expr, pattern, .. }) | Expr::SimilarTo(Like { expr, pattern, .. }) => {
            vec![expr, pattern]
        }
        Expr::InList(InList { expr, list, .. }) => std::iter::once(expr.as_ref()).chain(list).collect(),
        Expr::Between(Between { expr, low, high, .. }) => vec![expr, low, high],
        Expr::InSubquery(InSubquery { expr, .. }) | Expr::SetComparison(SetComparison { expr, .. }) => {
            vec![expr]
        }
        Expr::ScalarFunction(ScalarFunction { func, args })
            if matches!(func.name(), "nullif" | "greatest" | "least") =>
        {
            args.iter().collect()
        }
        Expr::Case(Case {
            expr: Some(base),
            when_then_expr,
            ..
        }) => std::iter::once(base.as_ref())
            .chain(when_then_expr.iter().map(|(when, _)| when.as_ref()))
            .collect(),
        _ => Vec::new(),
    }
}

fn compares(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
            | Operator::LikeMatch
            | Operator::ILikeMatch
            | Operator::NotLikeMatch
            | Operator::NotILikeMatch
            | Operator::RegexMatch
            | Operator::RegexIMatch
            | Operator::RegexNotMatch
            | Operator::RegexNotIMatch
    )
}

/// The name of the char field `operand` is, if it is one (see the module
/// documentation). An operand whose type cannot be settled here (an outer
/// reference) is left to the rest of planning.
fn char_operand(operand: &Expr, schema: &DFSchema) -> Option<String> {
    let mut operand = operand;
    while let Expr::Alias(Alias { expr, .. }) = operand {
        operand = expr;
    }
    if matches!(operand, Expr::Cast(_) | Expr::TryCast(_)) {
        return None;
    }
    let (_, field) = operand.to_field(schema).ok()?;
    matches!(declared(&field), Some(Declared::Char(_))).then(|| field.name().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trino_types::TRINO_TYPE;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;

    /// `t(id bigint, ch char(5))` holding `(1, 'ab   ')`, as a catalog that
    /// shows the padding holds it, and `v(s varchar)` holding `'ab'`.
    fn context() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(crate::engine::analyzer_rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ch", DataType::Utf8, true)
                .with_metadata(HashMap::from([(TRINO_TYPE.to_owned(), "char(5)".to_owned())])),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["ab   "])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        let text = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&text), vec![Arc::new(StringArray::from(vec!["ab"]))]).unwrap();
        ctx.register_table("v", Arc::new(MemTable::try_new(text, vec![vec![batch]]).unwrap()))
            .unwrap();
        ctx
    }

    async fn error(ctx: &SessionContext, sql: &str) -> Option<String> {
        match ctx.sql(sql).await {
            Ok(frame) => frame.collect().await.err().map(|e| e.to_string()),
            Err(err) => Some(err.to_string()),
        }
    }

    #[tokio::test]
    async fn comparisons_of_char_values_are_refused_wherever_they_are() {
        let ctx = context();
        for sql in [
            "SELECT id FROM t WHERE ch = 'ab'",
            "SELECT id FROM t WHERE ch = 'ab' OR random() > 2",
            "SELECT id FROM (SELECT id, ch FROM t ORDER BY id LIMIT 10) x WHERE ch = 'ab'",
            "SELECT id FROM (SELECT id, ch, row_number() OVER (ORDER BY id) AS n FROM t) x WHERE ch = 'ab'",
            "SELECT ch = 'ab' FROM t",
            "SELECT CASE WHEN ch < 'b' THEN 1 END FROM t",
            "SELECT CASE ch WHEN 'ab' THEN 1 END FROM t",
            "SELECT id FROM t WHERE ch IN ('ab', 'cd')",
            "SELECT id FROM t WHERE ch BETWEEN 'a' AND 'b'",
            "SELECT id FROM t WHERE ch LIKE 'ab'",
            "SELECT id FROM t JOIN v ON t.ch = v.s",
            "SELECT id FROM t WHERE id IN (SELECT 1 FROM v WHERE v.s = 'x') AND ch <> 'x'",
            "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t AS u WHERE u.ch = 'ab')",
            "SELECT count(*) FROM t JOIN (SELECT s AS ch FROM v) w USING (ch)",
            "SELECT count(*) FROM t NATURAL JOIN (SELECT s AS ch FROM v) w",
            "SELECT ch FROM t INTERSECT SELECT s FROM v",
            "SELECT ch FROM t EXCEPT SELECT s FROM v",
            "SELECT s FROM v EXCEPT ALL SELECT ch FROM t",
            "SELECT ch FROM t UNION SELECT s FROM v",
            "SELECT count(DISTINCT c) FROM (SELECT ch AS c FROM t UNION ALL SELECT s FROM v)",
            "SELECT id FROM t WHERE nullif(ch, 'ab') IS NULL",
            "SELECT greatest(ch, 'b') FROM t",
            "SELECT least('a', ch) FROM t",
        ] {
            let err = error(&ctx, sql).await;
            assert!(
                err.as_deref().is_some_and(|e| {
                    e.contains("comparing the char column ") && e.contains(" is not supported yet")
                }),
                "{sql}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn char_values_that_are_not_compared_stay_allowed() {
        let ctx = context();
        for sql in [
            "SELECT id, ch FROM t ORDER BY ch",
            "SELECT ch, count(*) FROM t GROUP BY ch",
            "SELECT id FROM t WHERE ch IS NULL OR ch IS NOT NULL",
            "SELECT id FROM t WHERE length(ch) = 5",
            "SELECT id FROM t WHERE CAST(ch AS varchar) = 'ab   '",
            "SELECT id FROM v, t WHERE v.s = 'ab'",
            "SELECT count(*) FROM t JOIN v ON CAST(t.ch AS varchar) = v.s",
            "SELECT CAST(ch AS varchar) FROM t UNION SELECT s FROM v",
            "SELECT id FROM t UNION ALL SELECT 2",
            "SELECT nullif(CAST(ch AS varchar), 'ab   ') FROM t",
        ] {
            assert_eq!(error(&ctx, sql).await, None, "{sql}");
        }
    }
}
