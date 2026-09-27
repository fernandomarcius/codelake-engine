//! `INTERSECT ALL` and `EXCEPT ALL` as the multiset operations they are
//! (Trino 431): a row found `m` times on the left and `n` times on the right
//! is in `INTERSECT ALL` `min(m, n)` times and in `EXCEPT ALL`
//! `max(m - n, 0)` times, `NULL` equal to `NULL`.
//!
//! `DataFusion` 54 plans both as a semi (anti) join of the left rows against
//! the right ones, which is right only when the left rows are distinct: it
//! keeps every left copy of a row found on the right (`VALUES 1, 1, 1
//! INTERSECT ALL SELECT 1` answered `[1, 1, 1]`, Trino `[1]`) and drops every
//! copy of it (`VALUES 1, 1, 2 EXCEPT ALL SELECT 1` answered `[2]`, Trino
//! `[1, 2]`). [`MultisetOperations`] numbers the copies of each row on both
//! sides (`row_number()` over a partition by every column) and joins on the
//! number too: the `k`-th copy on the left matches the `k`-th copy on the
//! right. `INTERSECT` and `EXCEPT` (distinct) are left as they are: their
//! left rows are distinct already.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, Result};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    Distinct, Expr, ExprFunctionExt, Join, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datafusion::optimizer::AnalyzerRule;

/// The copy number of a left row.
const LEFT_COPY: &str = "codelake_left_copy";
/// The copy number of a right row.
const RIGHT_COPY: &str = "codelake_right_copy";

/// Whether `plan`'s rows are distinct: the `DISTINCT` the planner puts on
/// the left of `INTERSECT` and `EXCEPT`, or the grouping it becomes.
fn distinct_rows(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Distinct(Distinct::All(_)) => true,
        LogicalPlan::Aggregate(aggregate) => {
            aggregate.aggr_expr.is_empty() && aggregate.group_expr.len() == aggregate.schema.fields().len()
        }
        _ => false,
    }
}

/// `plan` with its rows numbered within each group of equal rows, in a
/// column named `name`.
fn numbered(plan: Arc<LogicalPlan>, name: &str) -> Result<LogicalPlan> {
    let columns: Vec<Expr> = plan.schema().columns().into_iter().map(Expr::Column).collect();
    let number = row_number().partition_by(columns).build()?.alias(name);
    LogicalPlanBuilder::from(plan).window(vec![number])?.build()
}

fn multiset(join: Join) -> Result<Transformed<LogicalPlan>> {
    if !crate::coercion::is_set_operation(&join)
        || distinct_rows(&join.left)
        || join.left.schema().fields().iter().any(|f| f.name() == LEFT_COPY)
    {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }
    let output: Vec<Expr> = join
        .left
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let left = numbered(Arc::clone(&join.left), LEFT_COPY)?;
    let right = numbered(Arc::clone(&join.right), RIGHT_COPY)?;
    let mut on = join.on;
    on.push((
        Expr::Column(Column::from_name(LEFT_COPY)),
        Expr::Column(Column::from_name(RIGHT_COPY)),
    ));
    let numbered_join = Join::try_new(
        Arc::new(left),
        Arc::new(right),
        on,
        join.filter,
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?;
    Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(
        output,
        Arc::new(LogicalPlan::Join(numbered_join)),
    )?)))
}

/// Analyzer rule: see the module documentation.
#[derive(Debug, Default)]
pub(crate) struct MultisetOperations;

impl AnalyzerRule for MultisetOperations {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan
            .transform_up_with_subqueries(|node| match node {
                LogicalPlan::Join(join) => multiset(join),
                other => Ok(Transformed::no(other)),
            })?
            .data)
    }

    fn name(&self) -> &'static str {
        "codelake_multiset_operations"
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

    use crate::bytes::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;

    /// Each row of `sql`'s answer, its values joined by `|`.
    async fn rows(engine: &Engine, sql: &str) -> Vec<String> {
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let options = FormatOptions::default().with_null("NULL");
        let mut out = Vec::new();
        for batch in &batches {
            let formatters: Vec<ArrayFormatter<'_>> = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                let values: Vec<String> = formatters.iter().map(|f| f.value(row).to_string()).collect();
                out.push(values.join("|"));
            }
        }
        out
    }

    /// Rows Trino 431 answers to the same SQL.
    const CASES: &[(&str, &[&str])] = &[
        (
            "SELECT x FROM (VALUES 1, 1, 2) t(x) EXCEPT ALL SELECT 1 ORDER BY 1",
            &["1", "2"],
        ),
        (
            "SELECT x FROM (VALUES 1, 1, 1) t(x) INTERSECT ALL SELECT 1 ORDER BY 1",
            &["1"],
        ),
        (
            "SELECT x FROM (VALUES 1, 1, 1, NULL, NULL, 2) t(x) INTERSECT ALL \
             SELECT x FROM (VALUES 1, 1, NULL, 3) t(x) ORDER BY 1",
            &["1", "1", "NULL"],
        ),
        (
            "SELECT x FROM (VALUES 1, 1, 1, NULL, NULL, 2) t(x) EXCEPT ALL \
             SELECT x FROM (VALUES 1, NULL, 3) t(x) ORDER BY 1",
            &["1", "1", "2", "NULL"],
        ),
        (
            "SELECT a, b FROM (VALUES (1, 'x'), (1, 'x'), (1, NULL), (1, NULL)) t(a, b) EXCEPT ALL \
             SELECT * FROM (VALUES (1, NULL), (1, 'x')) t(a, b) ORDER BY 1, 2",
            &["1|x", "1|NULL"],
        ),
        (
            "SELECT x FROM (VALUES 1, 1, 2, 2) t(x) INTERSECT ALL SELECT x FROM (VALUES 1, 2, 2, 2) t(x) \
             EXCEPT ALL SELECT 2 ORDER BY 1",
            &["1", "2"],
        ),
        (
            "SELECT x FROM (VALUES 1, 1, 2) t(x) EXCEPT ALL SELECT x FROM (VALUES 1, 2) t(x) \
             INTERSECT ALL SELECT 1 ORDER BY 1",
            &["1", "2"],
        ),
        (
            "SELECT count(*) FROM (SELECT x FROM (VALUES 1, 1, 2) t(x) EXCEPT ALL SELECT 1)",
            &["2"],
        ),
        // Distinct: unchanged.
        (
            "SELECT x FROM (VALUES 1, 1, 2) t(x) EXCEPT SELECT 1 ORDER BY 1",
            &["2"],
        ),
        (
            "SELECT x FROM (VALUES 1, 1, 1) t(x) INTERSECT SELECT 1 ORDER BY 1",
            &["1"],
        ),
    ];

    fn engine(dir: &std::path::Path) -> Engine {
        let memory = MemoryConfig {
            limit: ByteSize::mib(64),
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(64),
            metadata_cache_limit: ByteSize::mib(1),
        };
        Engine::new(&memory, &ExecutionConfig::default()).unwrap()
    }

    /// Floats: `-0.0` equals `0.0`, a filter `x = <constant>` leaves a
    /// window partitioned by `x` (the numbering of the multiset operations
    /// too) working. Rows of Trino 431 (its memory catalog).
    const FLOAT_CASES: &[(&str, &[&str])] = &[
        ("SELECT count(*) FROM f WHERE x = 0", &["2"]),
        ("SELECT count(*) FROM f WHERE x = -0.0E0", &["2"]),
        ("SELECT count(*) FROM f WHERE x <> 0", &["4"]),
        ("SELECT count(*) FROM f WHERE x < 0", &["0"]),
        ("SELECT count(*) FROM f WHERE x <= 0", &["2"]),
        ("SELECT count(*) FROM f WHERE x > -0.0E0", &["2"]),
        ("SELECT count(*) FROM f WHERE x <> 1", &["4"]),
        (
            "SELECT x, row_number() OVER (PARTITION BY x) FROM f WHERE x = 1 ORDER BY 2",
            &["1.0|1", "1.0|2"],
        ),
        (
            "SELECT count(*) OVER (PARTITION BY x) FROM f WHERE x = 0",
            &["2", "2"],
        ),
        (
            "SELECT count(*), count_if(x = 0) FROM (SELECT x FROM f INTERSECT ALL SELECT x FROM f WHERE x = 0)",
            &["2|2"],
        ),
        (
            "SELECT count(*), count_if(x = 0) FROM (SELECT x FROM f WHERE x = 0 INTERSECT ALL SELECT x FROM f)",
            &["2|2"],
        ),
        (
            "SELECT x FROM f INTERSECT ALL SELECT x FROM f WHERE x = 1",
            &["1.0", "1.0"],
        ),
        (
            "SELECT count(*), count_if(x = 1), count_if(is_nan(x)), count_if(x IS NULL) \
             FROM (SELECT x FROM f EXCEPT ALL SELECT x FROM f WHERE x = 0)",
            &["5|2|2|1"],
        ),
        (
            "SELECT count(*) FROM (SELECT x FROM f EXCEPT ALL SELECT -0.0E0)",
            &["6"],
        ),
    ];

    #[tokio::test]
    async fn float_zeros_and_constant_filters() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path());
        for statement in [
            "CREATE TABLE f (x double)",
            "INSERT INTO f VALUES (0.0E0), (-0.0E0), (nan()), (nan()), (1.0E0), (1.0E0), (NULL)",
        ] {
            rows(&engine, statement).await;
        }
        let mut wrong = Vec::new();
        for (sql, answer) in FLOAT_CASES {
            let got = rows(&engine, sql).await;
            if got != *answer {
                wrong.push(format!("{sql}: got {got:?}, Trino 431 {answer:?}"));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[tokio::test]
    async fn all_set_operations_are_multiset_operations() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path());
        let mut wrong = Vec::new();
        for (sql, answer) in CASES {
            let got = rows(&engine, sql).await;
            if got != *answer {
                wrong.push(format!("{sql}: got {got:?}, Trino 431 {answer:?}"));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}
