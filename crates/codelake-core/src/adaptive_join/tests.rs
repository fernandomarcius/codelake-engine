use super::*;
use datafusion::arrow::array::Array;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::physical_plan::displayable;

fn engine(dir: &std::path::Path, limit: crate::ByteSize) -> crate::Engine {
    let memory = crate::config::MemoryConfig {
        limit,
        spill_dir: dir.to_owned(),
        spill_limit: crate::ByteSize::gib(2),
        metadata_cache_limit: crate::ByteSize::mib(1),
    };
    let execution = crate::config::ExecutionConfig {
        target_partitions: Some(4),
        ..crate::config::ExecutionConfig::default()
    };
    crate::Engine::new(&memory, &execution).unwrap()
}

fn options() -> crate::QueryOptions {
    crate::QueryOptions::for_user("test")
}

async fn physical(engine: &crate::Engine, sql: &str) -> Arc<dyn ExecutionPlan> {
    engine
        .sql_with(&options(), sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

/// The rows of `plan`, sorted, as text.
async fn run(engine: &crate::Engine, plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<Vec<String>>> {
    let batches = datafusion::physical_plan::collect(Arc::clone(plan), engine.context().task_ctx()).await?;
    let mut rows: Vec<Vec<String>> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(row) {
                            "NULL".to_owned()
                        } else {
                            array_value_to_string(c, row).unwrap()
                        }
                    })
                    .collect()
            })
        })
        .collect();
    rows.sort();
    Ok(rows)
}

/// What the decided joins of `plan` wrote to disk, in bytes.
fn spilled(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let own = plan
        .downcast_ref::<AdaptiveJoinExec>()
        .and_then(ExecutionPlan::metrics)
        .and_then(|metrics| metrics.spilled_bytes())
        .unwrap_or(0);
    own + plan.children().into_iter().map(spilled).sum::<usize>()
}

async fn reserved_goes_to_zero(engine: &crate::Engine) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while engine.memory().reserved_bytes != 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(engine.memory().reserved_bytes, 0);
}

/// A table that reports no statistics, as a federated source: a memory
/// table whose scan says nothing of its size.
#[derive(Debug)]
struct Unsized(Arc<dyn datafusion::catalog::TableProvider>);

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for Unsized {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan = self.0.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(crate::constant_keys::ForgetOrderExec::new(scan)))
    }
}

/// The rows of `sql` as table `name`, of unknown size, in `partitions`
/// partitions.
async fn unsized_table(engine: &crate::Engine, name: &str, sql: &str) {
    let frame = datafusion::prelude::SessionContext::new().sql(sql).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.unwrap();
    let table = datafusion::datasource::MemTable::try_new(schema, vec![batches]).unwrap();
    engine
        .context()
        .register_table(name, Arc::new(Unsized(Arc::new(table))))
        .unwrap();
}

/// `k` in 1..=rows with ~`width` bytes of text.
fn series(rows: usize, width: usize) -> String {
    format!(
        "SELECT value AS k, lpad(CAST(value AS VARCHAR), {width}, 'z') AS s \
         FROM generate_series(1, {rows})"
    )
}

/// A memory table of `rows` keyed rows (exact statistics), as `name`; past
/// 128 Ki rows, too large to be collected on one partition as the build side.
async fn keyed(engine: &crate::Engine, name: &str, rows: usize) {
    let sql = format!(
        "SELECT CASE WHEN value % 97 = 0 THEN NULL ELSE value END AS k, \
                lpad(CAST(value AS VARCHAR), 40, 'x') AS s FROM generate_series(1, {rows})"
    );
    let frame = datafusion::prelude::SessionContext::new()
        .sql(&sql)
        .await
        .unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.unwrap();
    let table = datafusion::datasource::MemTable::try_new(schema, vec![batches]).unwrap();
    engine.context().register_table(name, Arc::new(table)).unwrap();
}

/// A build side of unknown size, ~30 MB once read, under a 16 MiB pool:
/// the hash join failed with a resources error. Decided at run time, the
/// build side goes to disk and the join runs as a sort-merge join, with
/// the rows of a pool large enough for the hash join; the pool never holds
/// more than its limit and gives everything back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unsized_build_side_larger_than_the_pool_completes() {
    const ROWS: i64 = 300_000;
    let dir = tempfile::tempdir().unwrap();
    let limit = crate::ByteSize::mib(16);
    let small = engine(dir.path(), limit);
    let large = engine(dir.path(), crate::ByteSize::gib(2));
    for engine in [&small, &large] {
        keyed(engine, "l", 350_000).await;
        unsized_table(engine, "g", &series(300_000, 70)).await;
    }
    let sql = "SELECT count(*), sum(l.k), sum(length(g.s) + length(l.s)) FROM l JOIN g ON l.k = g.k";
    let plan = physical(&small, sql).await;
    let shown = text(&plan);
    assert!(
        shown.contains("AdaptiveJoinExec")
            && shown.contains("BufferedBuildExec")
            && shown.contains("HashJoinExec"),
        "{shown}"
    );
    small.reset_memory_peak();
    let rows = run(&small, &plan).await.unwrap();
    let nulls = ROWS / 97;
    let matched = ROWS - nulls;
    let sum: i64 = (1..=ROWS).filter(|k| k % 97 != 0).sum();
    assert_eq!(
        rows,
        [[matched.to_string(), sum.to_string(), (matched * 110).to_string()]]
    );
    assert!(spilled(&plan) > 0, "the build side did not go to disk");
    assert!(
        small.memory_peak() <= limit.as_u64(),
        "peak {}",
        small.memory_peak()
    );
    reserved_goes_to_zero(&small).await;
    // The same answer from the hash join, where it fits.
    let fits = physical(&large, sql).await;
    assert_eq!(run(&large, &fits).await.unwrap(), rows);
    assert_eq!(spilled(&fits), 0);
    reserved_goes_to_zero(&large).await;
}

/// Every join type the sort-merge join runs, NULL and repeated keys on both
/// sides: decided either way, the rows are the hash join's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_outcomes_answer_the_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let small = engine(dir.path(), crate::ByteSize::mib(8));
    let large = engine(dir.path(), crate::ByteSize::gib(2));
    for engine in [&small, &large] {
        keyed(engine, "l", 150_000).await;
        unsized_table(
            engine,
            "g",
            "SELECT CASE WHEN value % 89 = 0 THEN NULL ELSE value % 50000 END AS k, \
                    lpad(CAST(value AS VARCHAR), 60, 'y') AS s, \
                    lpad(CAST(value % 50000 AS VARCHAR), 40, 'x') AS t \
             FROM generate_series(1, 90000)",
        )
        .await;
    }
    // Each build side holds text, past the buffer's ~1.4 MB under 8 MiB.
    let cases = [
        "SELECT count(*), sum(l.k), sum(g.k), sum(length(g.s)) FROM l JOIN g ON l.k = g.k",
        "SELECT count(*), count(g.t), sum(l.k) FROM l LEFT JOIN g ON l.s = g.t",
        "SELECT count(*), count(l.k), sum(g.k) FROM l RIGHT JOIN g ON l.s = g.t",
        "SELECT count(*), count(l.k), count(g.k), sum(l.k), sum(g.k), sum(length(g.s)) \
         FROM l FULL JOIN g ON l.k = g.k",
        "SELECT count(*), sum(k) FROM l WHERE EXISTS (SELECT 1 FROM g WHERE g.t = l.s)",
        "SELECT count(*), sum(k) FROM l WHERE NOT EXISTS (SELECT 1 FROM g WHERE g.t = l.s)",
        "SELECT count(*), sum(l.k) FROM l JOIN g ON l.s = g.t",
    ];
    for sql in cases {
        let decided = physical(&small, sql).await;
        assert!(
            text(&decided).contains("AdaptiveJoinExec"),
            "{sql}\n{}",
            text(&decided)
        );
        let rows = run(&small, &decided)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert!(spilled(&decided) > 0, "{sql}: vacuous, the hash join ran");
        let hashed = physical(&large, sql).await;
        assert_eq!(rows, run(&large, &hashed).await.unwrap(), "{sql}");
        assert_eq!(spilled(&hashed), 0, "{sql}");
    }
    reserved_goes_to_zero(&small).await;
    reserved_goes_to_zero(&large).await;
}

/// A stream given up early (a `LIMIT` reached) stops the reading tasks and
/// gives the buffer back to the pool, either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_limit_reached_gives_the_buffer_back() {
    let dir = tempfile::tempdir().unwrap();
    for limit in [crate::ByteSize::mib(16), crate::ByteSize::gib(1)] {
        let engine = engine(dir.path(), limit);
        keyed(&engine, "l", 150_000).await;
        unsized_table(&engine, "g", &series(150_000, 70)).await;
        let plan = physical(&engine, "SELECT l.k, g.s FROM l JOIN g ON l.k = g.k LIMIT 5").await;
        assert!(text(&plan).contains("AdaptiveJoinExec"), "{}", text(&plan));
        assert_eq!(run(&engine, &plan).await.unwrap().len(), 5);
        reserved_goes_to_zero(&engine).await;
    }
}

/// Joins the planner sizes, and joins that must stay hash joins, are left
/// as they were; a view's plan, optimized again in the plan that reads it,
/// is wrapped once; the session property turns it off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_unsized_joins_that_can_run_both_ways_are_decided_at_run_time() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), crate::ByteSize::mib(64));
    keyed(&engine, "l", 150_000).await;
    keyed(&engine, "r", 2000).await;
    unsized_table(&engine, "g", &series(3000, 10)).await;
    let count = |plan: &Arc<dyn ExecutionPlan>, name: &str| text(plan).matches(name).count();
    let sized = physical(&engine, "SELECT count(*) FROM l JOIN r ON l.k = r.k").await;
    assert_eq!(count(&sized, "AdaptiveJoinExec"), 0, "{}", text(&sized));
    let residual = physical(
        &engine,
        "SELECT count(*) FROM l JOIN g ON l.k = g.k AND l.s < g.s",
    )
    .await;
    assert_eq!(count(&residual, "AdaptiveJoinExec"), 0, "{}", text(&residual));
    let not_in = physical(&engine, "SELECT count(*) FROM l WHERE k NOT IN (SELECT k FROM g)").await;
    assert_eq!(count(&not_in, "AdaptiveJoinExec"), 0, "{}", text(&not_in));
    let plain = "SELECT l.k FROM l JOIN g ON l.k = g.k";
    let decided = physical(&engine, plain).await;
    assert_eq!(count(&decided, "AdaptiveJoinExec"), 1, "{}", text(&decided));
    assert_eq!(count(&decided, "BufferedBuildExec"), 1, "{}", text(&decided));
    engine
        .context()
        .sql(&format!("CREATE VIEW decided AS {plain}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let viewed = physical(&engine, "SELECT count(*) FROM decided").await;
    assert_eq!(count(&viewed, "AdaptiveJoinExec"), 1, "{}", text(&viewed));
    assert_eq!(count(&viewed, "BufferedBuildExec"), 1, "{}", text(&viewed));
    assert_eq!(run(&engine, &viewed).await.unwrap(), [["2970"]]);
    let mut off = options();
    off.properties = engine
        .session_properties()
        .session(std::collections::BTreeMap::from([(
            crate::session_properties::SORT_MERGE_OVERSIZED_JOINS.to_owned(),
            "false".to_owned(),
        )]))
        .unwrap();
    let kept = engine
        .sql_with(&off, plain)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert_eq!(count(&kept, "AdaptiveJoinExec"), 0, "{}", text(&kept));
    // Put back as the hash join it was (inside an input of a rewritten join).
    let back = plain_hash_joins(Arc::clone(&decided)).unwrap();
    assert_eq!(count(&back, "AdaptiveJoinExec"), 0, "{}", text(&back));
    assert_eq!(count(&back, "BufferedBuildExec"), 0, "{}", text(&back));
    assert_eq!(count(&back, "HashJoinExec"), 1, "{}", text(&back));
}

/// Both nodes cross a process boundary (distributed execution) with their
/// settings, and the decoded join finds its buffered build side.
#[cfg(feature = "portable")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_nodes_round_trip_through_the_plan_codec() {
    use crate::portable::CoreCodec;
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;

    fn find(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.is::<AdaptiveJoinExec>() {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(find)
    }
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), crate::ByteSize::mib(64));
    keyed(&engine, "l", 150_000).await;
    unsized_table(&engine, "g", &series(3000, 10)).await;
    let plan = physical(&engine, "SELECT l.k FROM l JOIN g ON l.k = g.k").await;
    let adaptive = find(&plan).unwrap();
    let join = Arc::clone(adaptive.children()[0]);
    let buffered = Arc::clone(join.children()[0]);
    let ctx = engine.context().task_ctx();
    let mut buf = Vec::new();
    CoreCodec.try_encode(Arc::clone(&buffered), &mut buf).unwrap();
    let back = CoreCodec
        .try_decode(&buf, &[Arc::clone(buffered.children()[0])], &ctx)
        .unwrap();
    assert!(back.is::<BufferedBuildExec>());
    let probe = Arc::clone(join.children()[1]);
    let join = join.with_new_children(vec![back, probe]).unwrap();
    buf.clear();
    CoreCodec.try_encode(Arc::clone(&adaptive), &mut buf).unwrap();
    let back = CoreCodec.try_decode(&buf, &[join], &ctx).unwrap();
    let decoded = back.downcast_ref::<AdaptiveJoinExec>().unwrap();
    let original = adaptive.downcast_ref::<AdaptiveJoinExec>().unwrap();
    assert_eq!(
        (decoded.cap(), decoded.side_budget()),
        (original.cap(), original.side_budget())
    );
    assert!(decoded.parts().is_some());
}

/// The Trino protocol wraps every node of the plan to make it yield to the
/// scheduler: the join still finds its buffered build side and decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrapped_plan_still_decides() {
    use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion::physical_plan::coop::CooperativeExec;
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), crate::ByteSize::mib(16));
    keyed(&engine, "l", 150_000).await;
    unsized_table(&engine, "g", &series(300_000, 70)).await;
    let plan = physical(
        &engine,
        "SELECT count(*), sum(g.k), sum(length(g.s)) FROM l JOIN g ON l.k = g.k",
    )
    .await;
    let wrapped = plan
        .transform_up(|node| {
            Ok(Transformed::yes(
                Arc::new(CooperativeExec::new(node)) as Arc<dyn ExecutionPlan>
            ))
        })
        .data()
        .unwrap();
    let rows = run(&engine, &wrapped).await.unwrap();
    let sum: i64 = (1..=150_000_i64).filter(|k| k % 97 != 0).sum();
    let matched = 150_000 - 150_000 / 97;
    assert_eq!(
        rows,
        [[matched.to_string(), sum.to_string(), (matched * 70).to_string()]]
    );
    assert!(
        spilled(&wrapped) > 0,
        "the hash join ran over the whole build side"
    );
    reserved_goes_to_zero(&engine).await;
}
