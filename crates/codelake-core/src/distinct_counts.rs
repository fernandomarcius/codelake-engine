//! `COUNT(DISTINCT x)` next to other aggregates, in two spillable phases.
//!
//! An aggregation that counts distinct values keeps them all in one
//! accumulator that cannot spill: a `COUNT(DISTINCT (a, b, c))` over a few
//! million rows next to a `COUNT(*)` exhausted the memory limit where the
//! reference engine answered (shadow run of 2026-09-28, `sensor_distrib`).
//! `DataFusion`'s `single_distinct_aggregation_to_group_by` rewrites it into two
//! aggregations, but only when the other aggregates are `sum`, `min` or
//! `max`: a `COUNT(*)` beside it keeps the single accumulator.
//!
//! This rule rewrites the same shape with counts allowed: the first
//! aggregation groups by the query's keys and the distinct value (a hash
//! aggregation, which spills) and computes each other aggregate per group;
//! the second groups by the query's keys and combines them — the distinct
//! count counts the non-NULL values, a count sums its partial counts (0 when
//! there is no row), `sum`, `min` and `max` apply again. Only `count`,
//! `sum`, `min` and `max`, without `FILTER` or `ORDER BY`, and one distinct
//! argument: anything else is left as planned.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, Result};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::{AggregateFunction, Alias};
use datafusion::logical_expr::{Aggregate, AggregateUDF, Expr, LogicalPlan, Projection, lit};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Name of [`DistinctCountToGroupBy`], for rule ordering.
pub(crate) const DISTINCT_COUNT_TO_GROUP_BY: &str = "codelake_distinct_count_to_group_by";

/// See the module documentation.
#[derive(Debug, Default)]
pub(crate) struct DistinctCountToGroupBy;

impl OptimizerRule for DistinctCountToGroupBy {
    fn name(&self) -> &'static str {
        DISTINCT_COUNT_TO_GROUP_BY
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = &plan else {
            return Ok(Transformed::no(plan));
        };
        match two_phases(aggregate)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// One aggregate of the query.
enum Part {
    /// The `COUNT(DISTINCT x)`, with its `count` function.
    Distinct(Arc<AggregateUDF>),
    /// `count(args)`.
    Count(Arc<AggregateUDF>, Vec<Expr>),
    /// `sum`, `min` or `max` of `args`: applied again to its partials.
    Reapplied(Arc<AggregateUDF>, Vec<Expr>),
}

fn aggregate_call(func: &Arc<AggregateUDF>, args: Vec<Expr>) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        Arc::clone(func),
        args,
        false,
        None,
        vec![],
        None,
    ))
}

/// The query's aggregates as [`Part`]s and its distinct argument, when the
/// aggregation has the shape the rule rewrites.
fn parts(aggregate: &Aggregate) -> Option<(Vec<Part>, Expr)> {
    if aggregate
        .group_expr
        .iter()
        .any(|e| matches!(e, Expr::GroupingSet(_)))
    {
        return None;
    }
    let mut distinct: Option<Expr> = None;
    let mut parts = Vec::with_capacity(aggregate.aggr_expr.len());
    for expr in &aggregate.aggr_expr {
        let expr = match expr {
            Expr::Alias(Alias { expr, .. }) => expr.as_ref(),
            other => other,
        };
        let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
            return None;
        };
        if params.filter.is_some() || !params.order_by.is_empty() || params.null_treatment.is_some() {
            return None;
        }
        let name = func.name();
        if params.distinct {
            let [arg] = params.args.as_slice() else {
                return None;
            };
            if name != "count" || distinct.as_ref().is_some_and(|d| d != arg) {
                return None;
            }
            distinct = Some(arg.clone());
            parts.push(Part::Distinct(Arc::clone(func)));
        } else {
            match name {
                "count" => parts.push(Part::Count(Arc::clone(func), params.args.clone())),
                "sum" | "min" | "max" => parts.push(Part::Reapplied(Arc::clone(func), params.args.clone())),
                _ => return None,
            }
        }
    }
    // Without a count beside it, `single_distinct_aggregation_to_group_by` already applies.
    let counts = parts.iter().any(|p| matches!(p, Part::Count(..)));
    let distinct = distinct?;
    counts.then_some((parts, distinct))
}

const DISTINCT_KEY: &str = "codelake_distinct_key";

fn partial_name(index: usize) -> String {
    format!("codelake_partial_{index}")
}

fn two_phases(aggregate: &Aggregate) -> Result<Option<LogicalPlan>> {
    let Some((parts, distinct)) = parts(aggregate) else {
        return Ok(None);
    };
    let keys = aggregate.group_expr.len();

    let mut first_keys = aggregate.group_expr.clone();
    first_keys.push(distinct.alias(DISTINCT_KEY));
    let mut partials = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        match part {
            Part::Distinct(_) => {}
            Part::Count(func, args) | Part::Reapplied(func, args) => {
                partials.push(aggregate_call(func, args.clone()).alias(partial_name(index)));
            }
        }
    }
    let first = Aggregate::try_new(Arc::clone(&aggregate.input), first_keys, partials)?;
    let first_schema = Arc::clone(&first.schema);

    let second_keys: Vec<Expr> = (0..keys)
        .map(|i| Expr::Column(Column::from(first_schema.qualified_field(i))))
        .collect();
    let partial = |index: usize| Expr::Column(Column::from_name(partial_name(index)));
    let combined: Vec<Expr> = parts
        .iter()
        .enumerate()
        .map(|(index, part)| match part {
            Part::Distinct(count) => {
                aggregate_call(count, vec![Expr::Column(Column::from_name(DISTINCT_KEY))])
            }
            Part::Count(..) => aggregate_call(&sum_udaf(), vec![partial(index)]),
            Part::Reapplied(func, _) => aggregate_call(func, vec![partial(index)]),
        })
        .collect();
    let second = Aggregate::try_new(Arc::new(LogicalPlan::Aggregate(first)), second_keys, combined)?;
    let second_schema = Arc::clone(&second.schema);

    // The query's columns, names and types, as the single aggregation had them.
    let original = &aggregate.schema;
    let columns = (0..original.fields().len())
        .map(|i| {
            let (qualifier, field) = original.qualified_field(i);
            let value = Expr::Column(Column::from(second_schema.qualified_field(i)));
            let value = match i.checked_sub(keys).map(|p| &parts[p]) {
                Some(Part::Count(..)) => coalesce(vec![value, lit(0_i64)]),
                _ => value,
            };
            value.alias_qualified(qualifier.cloned(), field.name())
        })
        .collect();
    let projection = Projection::try_new(columns, Arc::new(LogicalPlan::Aggregate(second)))?;
    Ok(Some(LogicalPlan::Projection(projection)))
}

#[cfg(test)]
mod tests {
    use crate::bytes::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;
    use datafusion::arrow::util::pretty::pretty_format_batches;

    fn engine(limit: ByteSize, dir: &std::path::Path) -> Engine {
        let memory = MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(512),
            metadata_cache_limit: ByteSize::mib(1),
        };
        Engine::new(&memory, &ExecutionConfig::default()).unwrap()
    }

    async fn text(engine: &Engine, sql: &str) -> String {
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    const SETUP: &str = "CREATE TABLE t AS SELECT CAST(v % 3 AS bigint) AS g, CAST(v % 50 AS bigint) AS k, \
        CASE WHEN v % 10 = 0 THEN NULL ELSE CAST(v AS bigint) END AS x FROM generate_series(1, 200) AS s(v)";

    #[tokio::test]
    async fn answers_match_the_single_aggregation() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(ByteSize::mib(64), dir.path());
        engine.sql(SETUP).await.unwrap().collect().await.unwrap();
        // Expected values written by hand from the data: 200 rows, x NULL on
        // every tenth, k in 0..50, g in 0..3.
        let global = text(
            &engine,
            "SELECT COUNT(*) AS n, COUNT(x) AS xs, COUNT(DISTINCT k) AS ks, SUM(x) AS total, MIN(x) AS lo, \
             MAX(x) AS hi, COUNT(DISTINCT (g, k)) AS pairs FROM t",
        )
        .await;
        assert!(global.contains("| 200 | 180 | 50 "), "{global}");
        let empty = text(
            &engine,
            "SELECT COUNT(*) AS n, COUNT(DISTINCT k) AS ks, SUM(x) AS total FROM t WHERE g > 5",
        )
        .await;
        assert!(
            empty.contains("| 0 | 0  |       |"),
            "no row: counts are 0, sum NULL: {empty}"
        );
        let grouped = text(
            &engine,
            "SELECT g, COUNT(*) AS n, COUNT(DISTINCT k) AS ks FROM t GROUP BY g ORDER BY g",
        )
        .await;
        assert!(grouped.contains("| 0 | 66 | 50 |"), "{grouped}");
    }

    #[tokio::test]
    async fn the_distinct_values_are_grouped_where_they_can_spill() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(ByteSize::mib(64), dir.path());
        engine.sql(SETUP).await.unwrap().collect().await.unwrap();
        let plan = text(
            &engine,
            "EXPLAIN SELECT COUNT(*) AS n, COUNT(DISTINCT (g, k)) AS pairs FROM t",
        )
        .await;
        assert!(plan.contains("codelake_distinct_key"), "{plan}");
        assert!(!plan.contains("count(DISTINCT"), "{plan}");
    }

    #[tokio::test]
    async fn many_distinct_values_fit_the_memory_limit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(ByteSize::mib(96), dir.path());
        engine
            .sql(
                "CREATE TABLE big AS SELECT CAST(v % 2 AS bigint) AS g, CAST(v AS bigint) AS lsn, \
                 CAST(v % 5 AS bigint) AS op FROM generate_series(1, 1000000) AS s(v)",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let counts = text(
            &engine,
            "SELECT COUNT(*) AS n, COUNT(DISTINCT (g, lsn, op)) AS pk FROM big",
        )
        .await;
        assert!(counts.contains("| 1000000 | 1000000 |"), "{counts}");
    }

    #[tokio::test]
    async fn other_shapes_are_left_as_planned() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(ByteSize::mib(64), dir.path());
        engine.sql(SETUP).await.unwrap().collect().await.unwrap();
        for sql in [
            "EXPLAIN SELECT COUNT(*), COUNT(DISTINCT k), COUNT(DISTINCT g) FROM t",
            "EXPLAIN SELECT COUNT(*), AVG(x), COUNT(DISTINCT k) FROM t",
            "EXPLAIN SELECT COUNT(*) FILTER (WHERE x > 1), COUNT(DISTINCT k) FROM t",
        ] {
            let plan = text(&engine, sql).await;
            assert!(!plan.contains("codelake_distinct_key"), "{sql}: {plan}");
        }
    }
}
