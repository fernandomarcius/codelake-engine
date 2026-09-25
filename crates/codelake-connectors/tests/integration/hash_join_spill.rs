//! Hash joins whose build side does not fit the memory pool run as
//! sort-merge joins, which spill (`codelake_core::join_spill`, ADR 0003).
//! Rows checked against Trino 431 reading the same Parquet files, the pool's
//! peak measured by the pool itself.
//!
//! `hj.orders` (200 000 rows of ~210 bytes of text, three files) and
//! `hj.items` (250 000) are larger than the 24 MiB pool; the build side of
//! every join is over it. As a hash join
//! the query fails with a resources error (the session property off); as a
//! sort-merge join it completes, both inputs spill, and the pool never holds
//! more than its limit. Keys hold NULLs on both sides; `hot` repeats a key
//! on more rows than the join may hold, which it spills.

use std::collections::BTreeMap;

use codelake_core::config::ExecutionConfig;
use codelake_core::session_properties::SORT_MERGE_OVERSIZED_JOINS;
use codelake_core::{ByteSize, Engine, QueryOptions};

use crate::join_regressions::{
    Rows, Trino, assert_pool_given_back, explain, note, open, physical_plan, shared, sorted, try_cl,
};

const POOL: ByteSize = ByteSize::mib(24);

/// Trino's `sequence` stops at 10 000 values: rows come from a cross join
/// (`x = h * 1000 + l`). Keys of `orders` in 0..149 999 (NULL every 97th
/// row), of `items` in 50 000..199 999 (NULL every 89th): some keys match
/// once, some twice, some not at all on either side. `kt` is the same key as
/// ~100 characters of text, so that a join reading only the key still has a
/// build side over the pool. `hot` (300 000 rows) repeats three keys that
/// `orders` holds on 75 000 rows each (~9 MB per key with its text), and a
/// fourth that it does not hold.
const SETUP: [&str; 6] = [
    "CREATE SCHEMA hive.hj",
    "CREATE TABLE hive.hj.orders AS \
     SELECT CAST(x AS bigint) AS id, k, rpad(CAST(k AS varchar), 100, '#') AS kt, \
            rpad(CAST(x AS varchar), 110, 'o') AS pad \
     FROM (SELECT x, CASE WHEN x % 97 = 0 THEN NULL ELSE CAST(x % 150000 AS bigint) END AS k \
           FROM (SELECT h * 1000 + l AS x FROM UNNEST(sequence(0, 69)) a(h) \
                 CROSS JOIN UNNEST(sequence(1, 1000)) b(l))) t",
    "INSERT INTO hive.hj.orders \
     SELECT CAST(x AS bigint), k, rpad(CAST(k AS varchar), 100, '#'), rpad(CAST(x AS varchar), 110, 'o') \
     FROM (SELECT x, CASE WHEN x % 97 = 0 THEN NULL ELSE CAST(x % 150000 AS bigint) END AS k \
           FROM (SELECT h * 1000 + l AS x FROM UNNEST(sequence(70, 139)) a(h) \
                 CROSS JOIN UNNEST(sequence(1, 1000)) b(l))) t",
    "INSERT INTO hive.hj.orders \
     SELECT CAST(x AS bigint), k, rpad(CAST(k AS varchar), 100, '#'), rpad(CAST(x AS varchar), 110, 'o') \
     FROM (SELECT x, CASE WHEN x % 97 = 0 THEN NULL ELSE CAST(x % 150000 AS bigint) END AS k \
           FROM (SELECT h * 1000 + l AS x FROM UNNEST(sequence(140, 199)) a(h) \
                 CROSS JOIN UNNEST(sequence(1, 1000)) b(l))) t",
    "CREATE TABLE hive.hj.items AS \
     SELECT k, rpad(CAST(k AS varchar), 100, '#') AS kt, CAST(x AS bigint) AS w, \
            rpad(CAST(x AS varchar), 110, 'i') AS pad \
     FROM (SELECT x, CASE WHEN x % 89 = 0 THEN NULL ELSE CAST(50000 + x % 150000 AS bigint) END AS k \
           FROM (SELECT h * 1000 + l AS x FROM UNNEST(sequence(0, 249)) a(h) \
                 CROSS JOIN UNNEST(sequence(1, 1000)) b(l))) t",
    "CREATE TABLE hive.hj.hot AS \
     SELECT k, rpad(CAST(k AS varchar), 100, '#') AS kt, rpad(CAST(x AS varchar), 110, 'h') AS pad \
     FROM (SELECT x, CASE WHEN x % 4 = 3 THEN CAST(999999 AS bigint) ELSE CAST(x % 4 AS bigint) END AS k \
           FROM (SELECT h * 1000 + l AS x FROM UNNEST(sequence(0, 299)) a(h) \
                 CROSS JOIN UNNEST(sequence(1, 1000)) b(l))) t",
];

/// Every join type the rule rewrites, on an integer key (the build side
/// carries the text) or on the text key, and joins whose buffered side
/// repeats a key on more rows than the join may hold. NULL-free outputs:
/// Trino's TSV and the engine print NULL differently.
const CASES: [(&str, &str); 9] = [
    (
        "inner",
        "SELECT o.k % 13 AS g, count(*), sum(o.id), sum(i.w), sum(length(o.pad)), sum(length(i.pad)) \
         FROM hive.hj.orders o JOIN hive.hj.items i ON o.k = i.k GROUP BY 1",
    ),
    (
        "left",
        "SELECT coalesce(o.k % 13, -1) AS g, count(*), sum(o.id), sum(length(o.pad)), count(i.w), \
                coalesce(sum(i.w), 0), coalesce(sum(length(i.pad)), 0) \
         FROM hive.hj.orders o LEFT JOIN hive.hj.items i ON o.k = i.k GROUP BY 1",
    ),
    (
        "right",
        "SELECT coalesce(i.k % 13, -1) AS g, count(*), count(o.id), coalesce(sum(o.id), 0), \
                coalesce(sum(length(o.pad)), 0), sum(i.w), sum(length(i.pad)) \
         FROM hive.hj.orders o RIGHT JOIN hive.hj.items i ON o.k = i.k GROUP BY 1",
    ),
    (
        "full, text key",
        "SELECT coalesce(coalesce(o.k, i.k) % 13, -1) AS g, count(*), count(o.id), count(i.w), \
                coalesce(sum(o.id), 0), coalesce(sum(i.w), 0) \
         FROM hive.hj.orders o FULL JOIN hive.hj.items i ON o.kt = i.kt GROUP BY 1",
    ),
    (
        "semi, text key",
        "SELECT count(*), sum(id), count(k) FROM hive.hj.orders o \
         WHERE EXISTS (SELECT 1 FROM hive.hj.items i WHERE i.kt = o.kt)",
    ),
    (
        "anti, text key",
        "SELECT count(*), sum(id), count(k) FROM hive.hj.orders o \
         WHERE NOT EXISTS (SELECT 1 FROM hive.hj.items i WHERE i.kt = o.kt)",
    ),
    (
        "inner, repeated keys",
        "SELECT o.k, count(*), sum(o.id), sum(length(o.pad)), sum(length(h.pad)) \
         FROM hive.hj.orders o JOIN hive.hj.hot h ON o.k = h.k GROUP BY 1",
    ),
    (
        "right semi, repeated keys",
        "SELECT h.k, count(*), sum(length(h.pad)) FROM hive.hj.hot h \
         WHERE h.kt IN (SELECT kt FROM hive.hj.orders) GROUP BY 1",
    ),
    (
        "right anti, repeated keys",
        "SELECT h.k, count(*), sum(length(h.pad)) FROM hive.hj.hot h \
         WHERE NOT EXISTS (SELECT 1 FROM hive.hj.orders o WHERE o.kt = h.kt) GROUP BY 1",
    ),
];

/// The join type each case's sort-merge join must have, when it matters.
fn join_type(label: &str) -> Option<&'static str> {
    if label.starts_with("right semi") {
        Some("join_type=RightSemi")
    } else if label.starts_with("right anti") {
        Some("join_type=RightAnti")
    } else {
        None
    }
}

/// Every sort-merge join of the analyzed plan spilled rows of its buffered
/// side.
fn joins_spilled(analyzed: &str) -> bool {
    let joins: Vec<&str> = analyzed
        .lines()
        .filter(|l| l.trim_start().starts_with("SortMergeJoinExec"))
        .collect();
    !joins.is_empty()
        && joins.iter().all(|line| {
            line.split("spill_count=")
                .nth(1)
                .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| n > 0)
        })
}

fn with_property(engine: &Engine, value: &str) -> QueryOptions {
    let mut options = QueryOptions::for_user("test");
    options.properties = engine
        .session_properties()
        .session(BTreeMap::from([(
            SORT_MERGE_OVERSIZED_JOINS.to_owned(),
            value.to_owned(),
        )]))
        .unwrap();
    options
}

async fn try_with(engine: &Engine, options: &QueryOptions, sql: &str) -> Result<Rows, String> {
    let frame = engine
        .sql_with(options, sql)
        .await
        .map_err(|e| format!("planning: {e}"))?;
    let plan = frame
        .clone()
        .create_physical_plan()
        .await
        .map_err(|e| format!("planning: {e}"))?;
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    if !text.contains("HashJoinExec") || text.contains("SortMergeJoinExec") {
        return Err(format!("not a hash join plan:\n{text}"));
    }
    frame
        .collect()
        .await
        .map(|_| Rows::new())
        .map_err(|e| format!("running: {e}"))
}

/// Every external sort of the analyzed plan that feeds a sort-merge join
/// spilled.
fn sorts_spilled(analyzed: &str) -> bool {
    let sorts: Vec<&str> = analyzed
        .lines()
        .filter(|l| l.trim_start().starts_with("SortExec"))
        .collect();
    !sorts.is_empty()
        && sorts.iter().all(|line| {
            line.split("spill_count=")
                .nth(1)
                .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| n > 0)
        })
}

#[tokio::test]
async fn joins_over_the_pool_complete_as_sort_merge_joins_with_trinos_rows() {
    let shared = shared().await;
    let trino = Trino::start(&shared, "hj-spill").await;
    trino.batch(&SETUP).await;
    let statements: Vec<&str> = CASES.iter().map(|(_, sql)| *sql).collect();
    let references = trino.batch(&statements).await;
    let spill = tempfile::tempdir().unwrap();
    // Batches of 2048 rows (~300 kB): under 24 MiB a sort's fair share,
    // with the grouped aggregations above it registered, is ~2 MB; 8192
    // rows of this width are one batch too many (ADR 0003, sizing).
    let execution = ExecutionConfig {
        target_partitions: Some(4),
        batch_size: 2048,
        ..ExecutionConfig::default()
    };
    let engine = open(&shared.warehouse(), spill.path(), POOL, &execution, &[]).await;
    let mut failures = Vec::new();

    // Before: the same join as a hash join does not fit and fails cleanly.
    let (_, inner) = CASES[0];
    match try_with(&engine, &with_property(&engine, "false"), inner).await {
        Err(error) if error.contains("Resources exhausted") => {
            note("hash join", format!("refused as expected: {error}"));
        }
        other => failures.push(format!(
            "hash join under {POOL:?}: expected a resources error, got {other:?}"
        )),
    }
    assert_pool_given_back(&engine).await;

    for ((label, sql), reference) in CASES.iter().zip(references) {
        let reference = sorted(reference);
        let plan = physical_plan(&engine, sql).await;
        note(label, format!("plan:\n{plan}"));
        if !plan.contains("SortMergeJoinExec") || plan.contains("HashJoinExec") {
            failures.push(format!(
                "{label}: vacuous, not planned as a sort-merge join:\n{plan}"
            ));
            continue;
        }
        if let Some(join_type) = join_type(label)
            && !plan.contains(join_type)
        {
            failures.push(format!("{label}: vacuous, no `{join_type}`:\n{plan}"));
            continue;
        }
        engine.reset_memory_peak();
        let rows = try_cl(&engine, sql).await.map(sorted);
        let peak = engine.memory_peak();
        note(
            label,
            format!(
                "pool peak {peak} B (limit {} B), trino={} rows, codelake={:?}",
                POOL.as_u64(),
                reference.len(),
                rows.as_ref().map(Vec::len)
            ),
        );
        match rows {
            Ok(rows) if rows == reference => {}
            Ok(rows) => failures.push(format!("{label}: trino={reference:?} codelake={rows:?}")),
            Err(error) => failures.push(format!("{label}: trino={reference:?} codelake failed: {error}")),
        }
        if peak > POOL.as_u64() {
            failures.push(format!("{label}: the pool held {peak} B, over its {POOL:?}"));
        }
        assert_pool_given_back(&engine).await;
    }

    // The inputs spilled: the plan fits the pool by spilling, not by luck.
    match explain(&engine, "EXPLAIN ANALYZE", inner, "Plan with Metrics").await {
        Ok(analyzed) if sorts_spilled(&analyzed) => note("inner", format!("analyzed:\n{analyzed}")),
        Ok(analyzed) => failures.push(format!("inner: vacuous, the sorts did not spill:\n{analyzed}")),
        Err(error) => failures.push(format!("inner: EXPLAIN ANALYZE failed: {error}")),
    }
    assert_pool_given_back(&engine).await;
    // A key on more rows than the join may hold: the join spilled them.
    let (label, repeated) = CASES[6];
    match explain(&engine, "EXPLAIN ANALYZE", repeated, "Plan with Metrics").await {
        Ok(analyzed) if joins_spilled(&analyzed) => note(label, format!("analyzed:\n{analyzed}")),
        Ok(analyzed) => failures.push(format!("{label}: vacuous, the join did not spill:\n{analyzed}")),
        Err(error) => failures.push(format!("{label}: EXPLAIN ANALYZE failed: {error}")),
    }
    assert_pool_given_back(&engine).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
