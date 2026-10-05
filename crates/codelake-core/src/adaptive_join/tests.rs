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

// -- The buffer and the run, in isolation ------------------------------------

fn numbers(rows: i64) -> RecordBatch {
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new("n", datafusion::arrow::datatypes::DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(datafusion::arrow::array::Int64Array::from_iter_values(
            0..rows,
        ))],
    )
    .unwrap()
}

/// A buffer of `cap` bytes over a ceiling pool of `pool_bytes`, one
/// partition.
fn buffer(
    cap: usize,
    pool_bytes: usize,
) -> (
    Arc<Buffer>,
    Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
) {
    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
        Arc::new(crate::pool::CeilingPool::new(pool_bytes));
    let runtime = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .unwrap();
    let buffer = Buffer {
        cap,
        reservation: Mutex::new(MemoryConsumer::new(BUILD_BUFFER).register(&pool)),
        spilling: AtomicBool::new(false),
        spill: SpillManager::new(
            runtime,
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            numbers(1).schema(),
        ),
        partitions: Mutex::new(vec![None]),
    };
    (Arc::new(buffer), pool)
}

/// Batches are held while the buffer fits and given back on release; past
/// the cap every batch is refused, small ones included.
#[test]
fn the_buffer_holds_while_it_fits() {
    let batch = numbers(1000);
    let bytes = batch_bytes(&batch);
    let (buffer, pool) = buffer(2 * bytes, 1 << 30);
    assert!(buffer.hold(&batch));
    assert!(buffer.hold(&batch));
    assert!(!buffer.spilling());
    assert_eq!(pool.reserved(), 2 * bytes);
    assert!(!buffer.hold(&batch));
    assert!(buffer.spilling());
    assert!(
        !buffer.hold(&numbers(1)),
        "once spilling, every batch goes to disk"
    );
    assert_eq!(pool.reserved(), 2 * bytes);
    buffer.release(bytes);
    assert_eq!(pool.reserved(), bytes);
    // Refused by the pool, within the cap.
    let (tight, _) = buffer_with_pool(&batch);
    assert!(!tight.hold(&batch));
    assert!(tight.spilling());
}

fn buffer_with_pool(
    batch: &RecordBatch,
) -> (
    Arc<Buffer>,
    Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
) {
    buffer(10 * batch_bytes(batch), batch_bytes(batch) / 2)
}

/// Batches held in memory go to a file of their own and give their memory
/// back; the rows read back are the same; settling a spilled buffer does
/// that for every partition.
#[tokio::test]
async fn held_batches_go_to_disk() {
    let batch = numbers(1000);
    let bytes = batch_bytes(&batch);
    let (buffer, pool) = buffer(10 * bytes, 1 << 30);
    assert_eq!(buffer.settle().unwrap(), Outcome::HashJoin);
    assert!(buffer.hold(&batch) && buffer.hold(&batch));
    let mut held = Buffered::default();
    held.memory.extend([batch.clone(), batch.clone()]);
    buffer.spill_held(&mut held).unwrap();
    assert!(held.memory.is_empty());
    assert_eq!(held.files.len(), 1);
    assert_eq!(pool.reserved(), 0);
    let file = held.files.pop().unwrap();
    let rows: usize = buffer
        .spill
        .read_spill_as_stream_unbuffered(file, None)
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .collect::<Vec<_>>()
        .await
        .iter()
        .sum();
    assert_eq!(rows, 2000);
    // Nothing held: no file.
    buffer.spill_held(&mut held).unwrap();
    assert!(held.files.is_empty());
    // Settled after spilling: the partitions' held batches go to disk too.
    assert!(buffer.hold(&batch));
    let mut partition = Buffered::default();
    partition.memory.push_back(batch.clone());
    *lock(&buffer.partitions) = vec![Some(partition)];
    buffer.spilling.store(true, Ordering::Release);
    assert_eq!(buffer.settle().unwrap(), Outcome::SortMerge);
    let partitions = lock(&buffer.partitions);
    let settled = partitions[0].as_ref().unwrap();
    assert!(settled.memory.is_empty());
    assert_eq!(settled.files.len(), 1);
    assert_eq!(pool.reserved(), 0);
}

fn ready_run(buffer: Arc<Buffer>) -> Run {
    Run {
        buffer,
        decision: futures::future::ready(Ok(Outcome::HashJoin)).boxed().shared(),
        sort_merge: OnceLock::new(),
    }
}

/// A run is shared by the output partitions that start while it lives; the
/// slot holds it until all of them have started, then only its streams do.
#[test]
fn a_run_lives_until_every_partition_started_then_with_its_streams() {
    let slot = Slot::default();
    let (buffer, _) = buffer(1 << 20, 1 << 30);
    let first = slot.run(2, || ready_run(Arc::clone(&buffer)));
    assert!(format!("{first:?}").starts_with("Run"));
    drop(first);
    // Not every partition started: the slot keeps the run.
    let second = slot.run(2, || panic!("the run is shared"));
    assert!(slot.current().is_some());
    drop(second);
    // Both started and their streams are gone: the next execution starts
    // a run of its own.
    assert!(slot.current().is_none());
    let mut started = false;
    let third = slot.run(1, || {
        started = true;
        ready_run(Arc::clone(&buffer))
    });
    assert!(started);
    assert!(slot.current().is_some());
    drop(third);
    assert!(slot.current().is_none());
}

/// Reading a partition back hands each held batch on and gives it back to
/// the pool when the next one is asked for (or the stream is dropped).
#[tokio::test]
async fn read_back_gives_each_batch_back_once_the_next_is_asked() {
    let batch = numbers(1000);
    let bytes = batch_bytes(&batch);
    let (buffer, pool) = buffer(10 * bytes, 1 << 30);
    for _ in 0..3 {
        assert!(buffer.hold(&batch));
    }
    let mut partition = Buffered::default();
    partition
        .memory
        .extend([batch.clone(), batch.clone(), batch.clone()]);
    *lock(&buffer.partitions) = vec![Some(partition)];
    let run = Arc::new(ready_run(Arc::clone(&buffer)));
    let mut stream = read_back(&run, 0).unwrap();
    assert!(read_back(&run, 0).is_err(), "a partition is read back once");
    assert_eq!(pool.reserved(), 3 * bytes);
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1000);
    assert_eq!(pool.reserved(), 3 * bytes, "the batch handed on is still held");
    stream.next().await.unwrap().unwrap();
    assert_eq!(pool.reserved(), 2 * bytes);
    drop(stream);
    assert_eq!(pool.reserved(), 0);
}

/// Both nodes pass their input's properties and statistics through and
/// leave the plan's partitioning alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_nodes_are_transparent_to_the_planner() {
    fn find(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == name {
            return Some(Arc::clone(plan));
        }
        plan.children().into_iter().find_map(|child| find(child, name))
    }
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), crate::ByteSize::mib(64));
    keyed(&engine, "l", 150_000).await;
    unsized_table(&engine, "g", &series(3000, 10)).await;
    let plan = physical(&engine, "SELECT l.k FROM l JOIN g ON l.k = g.k").await;
    let adaptive = find(&plan, "AdaptiveJoinExec").unwrap();
    let buffered = find(&plan, "BufferedBuildExec").unwrap();
    assert!(adaptive.is::<AdaptiveJoinExec>() && buffered.is::<BufferedBuildExec>());
    for node in [&adaptive, &buffered] {
        assert_eq!(node.maintains_input_order(), [true]);
        assert_eq!(node.benefits_from_input_partitioning(), [false]);
        let child = node.children()[0];
        assert_eq!(
            node.properties().output_partitioning(),
            child.properties().output_partitioning()
        );
    }
    let rows = numbers(2000);
    let scan: Arc<dyn ExecutionPlan> = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![rows.clone()]],
        rows.schema(),
        None,
    )
    .unwrap();
    let sized = BufferedBuildExec::new(scan);
    assert_eq!(
        sized.partition_statistics(None).unwrap().num_rows,
        datafusion::common::stats::Precision::Exact(2000)
    );
    let decided = adaptive.downcast_ref::<AdaptiveJoinExec>().unwrap();
    assert!(decided.cap() > 1 && decided.side_budget() > decided.cap());
    assert_eq!(
        displayable(adaptive.as_ref()).one_line().to_string().trim(),
        format!(
            "AdaptiveJoinExec: hash_join_build_bytes={}, sort_merge_side_bytes={}",
            decided.cap(),
            decided.side_budget()
        )
    );
    let hash_join = Arc::clone(adaptive.children()[0]);
    assert!(is_buffered(hash_join.downcast_ref::<HashJoinExec>().unwrap()));
    let rows = numbers(10);
    let side: Arc<dyn ExecutionPlan> = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![rows.clone()]],
        rows.schema(),
        None,
    )
    .unwrap();
    let on: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = vec![(
        Arc::new(datafusion::physical_expr::expressions::Column::new("n", 0)),
        Arc::new(datafusion::physical_expr::expressions::Column::new("n", 0)),
    )];
    let unbuffered = HashJoinExec::try_new(
        Arc::clone(&side),
        side,
        on,
        None,
        &datafusion::common::JoinType::Inner,
        None,
        datafusion::physical_plan::joins::PartitionMode::CollectLeft,
        datafusion::common::NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap();
    assert!(!is_buffered(&unbuffered));
    // The sort-merge form built at run time merges through accounted merges.
    let merged = text(&sort_merge(&hash_join, decided.side_budget()).unwrap());
    assert!(merged.contains("SortMergeJoinExec"), "{merged}");
    assert!(merged.contains("AccountedMergeExec"), "{merged}");
    assert!(!merged.contains("CoalescePartitionsExec"), "{merged}");
    // 64 MiB less the exchange slice, halved; less the hash table's 5/2.
    let slice = crate::join_spill::join_slice(64 << 20, 1);
    assert_eq!(decided.cap(), crate::join_spill::build_rows_within(slice));
    assert_eq!(decided.side_budget(), crate::join_spill::side_budget(64 << 20));
    assert_eq!(
        displayable(buffered.as_ref()).one_line().to_string().trim(),
        "BufferedBuildExec"
    );
}
