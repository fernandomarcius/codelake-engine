//! Memory of a query's output: every batch between the plan and a client is
//! reserved in the pool (ADR 0003).
//!
//! `DataFusion` runs a plan of several partitions for a client through
//! `CoalescePartitionsExec` (which `execute_stream` adds on top): one task per
//! partition pushes its batches into a channel, and neither the channel nor the
//! batch that a blocked task holds is reserved. A client that reads slowly
//! leaves 2 × `target_partitions` batches per query outside the ceiling. The
//! same node merges partitions inside plans (below a final aggregation, the
//! build side of a hash join).
//!
//! [`AccountedOutput`], the last physical rule, closes that gap:
//! - every `CoalescePartitionsExec` becomes an [`AccountedMergeExec`] (under
//!   a `GlobalLimitExec` when it had a limit), and so does the root of a plan
//!   with several partitions. It runs the partitions in parallel as well,
//!   through a channel of the same size, but every batch it holds — queued, or
//!   in the hands of a task waiting for room — is reserved in the pool or, when
//!   the pool refuses, **spilled** to disk and dropped before the task waits;
//! - the root is wrapped in [`ReservedOutputExec`], which keeps the batch the
//!   client is being sent reserved until the client asks for the next one
//!   (the PostgreSQL wire encodes a batch row by row as the socket drains).
//!
//! `DataFusion`'s own `RepartitionExec` is not used for this: when the pool
//! refuses a batch it spills it but keeps it in memory until the channel takes
//! the "spilled" marker, which for a paused client is never.
//!
//! What stays outside the pool is the batch an operator is producing at this
//! instant, before its consumer reserves it: at most one per running
//! partition, about 2 MiB when it holds computed values
//! (`crate::computed_batches`; see ADR 0003, "What the pool does not cover").

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties, SpillManager,
};
use futures::{FutureExt, Stream, StreamExt};
use tokio::sync::mpsc;

/// Name of [`AccountedOutput`]; a plan planned inside another one (a view)
/// leaves the rule out, the outer plan applies it.
pub const ACCOUNTED_OUTPUT: &str = "codelake_accounted_output";

/// Consumer name of [`AccountedMergeExec`]; the pool counts it among the
/// exchange buffers (`crate::pool`).
pub const MERGE_CONSUMER: &str = "AccountedMerge";

fn merge(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(AccountedMergeExec::new(input))
}

/// Physical rule, last in the list: see the module documentation.
#[derive(Debug, Default)]
pub struct AccountedOutput;

impl PhysicalOptimizerRule for AccountedOutput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if plan.is::<ReservedOutputExec>() {
            return Ok(plan);
        }
        let plan = accounted_merges(plan)?;
        let plan = if plan.output_partitioning().partition_count() > 1 {
            merge(plan)
        } else {
            plan
        };
        Ok(Arc::new(ReservedOutputExec::new(plan)))
    }

    fn name(&self) -> &'static str {
        ACCOUNTED_OUTPUT
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `plan` with every `CoalescePartitionsExec` of several partitions as an
/// [`AccountedMergeExec`] (under a `GlobalLimitExec` when it had a limit),
/// and without the root of a plan optimized inside it (a view's
/// `ReservedOutputExec`). Also used for plans built while a query runs
/// (`crate::adaptive_join`).
pub(crate) fn accounted_merges(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        // The root of a plan optimized inside this one (a view's):
        // its batches go to the operator above, not to a client, and
        // reserving each of them again held a slice's whole buffers
        // once per partition (an aggregation's output under a view
        // ordered with `LIMIT`: 41.7 MB per partition, refused).
        if let Some(nested) = node.downcast_ref::<ReservedOutputExec>() {
            return Ok(Transformed::yes(Arc::clone(&nested.input)));
        }
        let Some(coalesce) = node.downcast_ref::<CoalescePartitionsExec>() else {
            return Ok(Transformed::no(node));
        };
        let input = coalesce.input();
        if input.output_partitioning().partition_count() < 2 {
            return Ok(Transformed::no(node));
        }
        let merged = merge(Arc::clone(input));
        Ok(Transformed::yes(match coalesce.fetch() {
            Some(fetch) => Arc::new(GlobalLimitExec::new(merged, 0, Some(fetch))),
            None => merged,
        }))
    })
    .data()
}

/// Merges the partitions of its input into one, in no order, like
/// `CoalescePartitionsExec`, holding every batch inside the pool: see the
/// module documentation.
#[derive(Debug)]
pub struct AccountedMergeExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl AccountedMergeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        // Same output properties as the node it stands for.
        let properties = Arc::clone(CoalescePartitionsExec::new(Arc::clone(&input)).properties());
        Self {
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for AccountedMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AccountedMergeExec: input_partitions={}",
            self.input.output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for AccountedMergeExec {
    fn name(&self) -> &'static str {
        "AccountedMergeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::new(input))),
            _ => Err(DataFusionError::Internal(
                "AccountedMergeExec has exactly one child".into(),
            )),
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "AccountedMergeExec has one partition, not {partition}"
            )));
        }
        let schema = self.input.schema();
        let inputs = self.input.output_partitioning().partition_count();
        let spill = SpillManager::new(
            context.runtime_env(),
            SpillMetrics::new(&self.metrics, partition),
            Arc::clone(&schema),
        );
        let reservation = MemoryConsumer::new(format!("{MERGE_CONSUMER}[{partition}]"))
            .with_can_spill(true)
            .register(context.memory_pool());
        // As many queued batches as `CoalescePartitionsExec` allows.
        let (sender, receiver) = mpsc::channel(inputs.max(1));
        let tasks = (0..inputs)
            .map(|input_partition| {
                SpawnedTask::spawn(pull_or_fail(
                    Arc::clone(&self.input),
                    input_partition,
                    Arc::clone(&context),
                    sender.clone(),
                    reservation.new_empty(),
                    spill.clone(),
                ))
            })
            .collect();
        Ok(Box::pin(MergeStream {
            schema,
            receiver,
            spill,
            reading: None,
            _tasks: tasks,
        }))
    }
}

/// A batch in the merge: reserved, or on disk.
enum Held {
    Memory(RecordBatch, MemoryReservation),
    Spilled(RefCountedTempFile),
}

/// Reserves `batch` or, when the pool refuses, writes it to disk and drops it.
fn hold(batch: RecordBatch, reservation: &MemoryReservation, spill: &SpillManager) -> Result<Option<Held>> {
    let held = reservation.new_empty();
    if held.try_grow(batch.get_array_memory_size()).is_ok() {
        return Ok(Some(Held::Memory(batch, held)));
    }
    Ok(spill_in_chunks(&batch, spill)?.map(Held::Spilled))
}

/// Bytes of one spilled chunk.
const SPILL_CHUNK_BYTES: usize = 1 << 20;

/// Writes `batch` to disk in chunks of about [`SPILL_CHUNK_BYTES`]. The pool
/// has just refused the batch, and the IPC writer encodes what it writes into a
/// buffer of its own first: a 32 MiB batch written at once needed another
/// 32 MiB, outside the pool, at the moment memory ran out.
///
/// A slice of a string view keeps every data buffer of the batch, which the
/// writer would copy into each chunk: string and binary views are compacted
/// per chunk, and a batch with a view nested in another type is written whole.
fn spill_in_chunks(batch: &RecordBatch, spill: &SpillManager) -> Result<Option<RefCountedTempFile>> {
    let mut file = spill.create_in_progress_file(MERGE_CONSUMER)?;
    for chunk in spill_chunks(batch)? {
        file.append_batch(&chunk)?;
    }
    file.finish()
}

/// `batch` in the chunks [`spill_in_chunks`] writes: about
/// [`SPILL_CHUNK_BYTES`] each, string views compacted.
pub(crate) fn spill_chunks(batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
    let rows = batch.num_rows();
    let nested = batch.schema().fields().iter().any(|f| f.data_type().is_nested());
    let bytes = batch.get_array_memory_size().max(1);
    let per_chunk = if nested {
        rows
    } else {
        rows.saturating_mul(SPILL_CHUNK_BYTES)
            .div_ceil(bytes)
            .clamp(1, rows.max(1))
    };
    let mut chunks = Vec::with_capacity(rows.div_ceil(per_chunk.max(1)));
    let mut start = 0;
    while start < rows {
        let len = per_chunk.min(rows - start);
        chunks.push(compact_views(&batch.slice(start, len))?);
        start += len;
    }
    Ok(chunks)
}

/// `batch` with its string and binary views holding only the bytes they use.
fn compact_views(batch: &RecordBatch) -> Result<RecordBatch> {
    use datafusion::arrow::array::{Array, ArrayRef, AsArray};
    use datafusion::arrow::datatypes::DataType;
    if !batch
        .columns()
        .iter()
        .any(|c| matches!(c.data_type(), DataType::Utf8View | DataType::BinaryView))
    {
        return Ok(batch.clone());
    }
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|column| match column.data_type() {
            DataType::Utf8View => Arc::new(column.as_string_view().gc()) as ArrayRef,
            DataType::BinaryView => Arc::new(column.as_binary_view().gc()) as ArrayRef,
            _ => Arc::clone(column),
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// [`pull`], turning a panic of the input into an error of the merge: a task
/// that ended without a word would end the merged stream as if every
/// partition had finished, and the query would return part of its rows.
async fn pull_or_fail(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    sender: mpsc::Sender<Result<Held>>,
    reservation: MemoryReservation,
    spill: SpillManager,
) {
    let report = sender.clone();
    let pulled = std::panic::AssertUnwindSafe(pull(input, partition, context, sender, reservation, spill))
        .catch_unwind()
        .await;
    if let Err(payload) = pulled {
        let message = payload
            .downcast_ref::<&str>()
            .map(ToString::to_string)
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "no message".to_owned());
        let error = DataFusionError::Execution(format!(
            "partition {partition} of {MERGE_CONSUMER} panicked: {message}"
        ));
        let _ = report.send(Err(error)).await;
    }
}

/// Runs one input partition into the channel.
async fn pull(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    sender: mpsc::Sender<Result<Held>>,
    reservation: MemoryReservation,
    spill: SpillManager,
) {
    let mut stream = match input.execute(partition, context) {
        Ok(stream) => stream,
        Err(error) => {
            let _ = sender.send(Err(error)).await;
            return;
        }
    };
    while let Some(item) = stream.next().await {
        // The batch is reserved or on disk before this task waits for room.
        let held = match item.and_then(|batch| hold(batch, &reservation, &spill)) {
            Ok(Some(held)) => Ok(held),
            Ok(None) => continue,
            Err(error) => Err(error),
        };
        let failed = held.is_err();
        if sender.send(held).await.is_err() || failed {
            return;
        }
    }
}

struct MergeStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<Result<Held>>,
    spill: SpillManager,
    /// A spilled batch being read back.
    reading: Option<SendableRecordBatchStream>,
    /// Aborted when the stream is dropped.
    _tasks: Vec<SpawnedTask<()>>,
}

impl Stream for MergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(reading) = self.reading.as_mut() {
                match ready!(reading.poll_next_unpin(cx)) {
                    Some(item) => return Poll::Ready(Some(item)),
                    None => self.reading = None,
                }
            }
            match ready!(self.receiver.poll_recv(cx)) {
                None => return Poll::Ready(None),
                Some(Err(error)) => return Poll::Ready(Some(Err(error))),
                // The consumer reserves what it keeps.
                Some(Ok(Held::Memory(batch, _reservation))) => return Poll::Ready(Some(Ok(batch))),
                Some(Ok(Held::Spilled(file))) => match self.spill.read_spill_as_stream_unbuffered(file, None)
                {
                    Ok(stream) => self.reading = Some(stream),
                    Err(error) => return Poll::Ready(Some(Err(error))),
                },
            }
        }
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Root of every client plan: reserves the batch last handed to the consumer
/// until it asks for the next one (or drops the stream). A batch the pool
/// cannot hold fails the query with a resources error.
#[derive(Debug)]
pub struct ReservedOutputExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl ReservedOutputExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::clone(input.properties());
        Self { input, properties }
    }

    /// The plan below; a client that reserves its own buffers (the Trino
    /// protocol) runs this one instead.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for ReservedOutputExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReservedOutputExec")
    }
}

impl ExecutionPlan for ReservedOutputExec {
    fn name(&self) -> &'static str {
        "ReservedOutputExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self::new(input))),
            _ => Err(DataFusionError::Internal(
                "ReservedOutputExec has exactly one child".into(),
            )),
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let reservation =
            MemoryConsumer::new(format!("ClientOutput[{partition}]")).register(context.memory_pool());
        Ok(Box::pin(ReservedStream { input, reservation }))
    }
}

struct ReservedStream {
    input: SendableRecordBatchStream,
    /// The batch last returned.
    reservation: MemoryReservation,
}

impl Stream for ReservedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Asking for the next batch means the previous one is no longer needed.
        self.reservation.free();
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(
                self.reservation
                    .try_grow(batch.get_array_memory_size())
                    .map(|()| batch),
            )),
            other => other,
        }
    }
}

impl RecordBatchStream for ReservedStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bytes::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;
    use datafusion::arrow::array::Int64Array;
    use datafusion::physical_plan::displayable;

    fn engine(dir: &std::path::Path, limit: ByteSize, partitions: usize) -> Engine {
        let memory = MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(64),
            metadata_cache_limit: ByteSize::mib(1),
        };
        let execution = ExecutionConfig {
            target_partitions: Some(partitions),
            ..ExecutionConfig::default()
        };
        Engine::new(&memory, &execution).unwrap()
    }

    async fn plan_text(engine: &Engine, sql: &str) -> String {
        let plan = engine
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        displayable(plan.as_ref()).indent(false).to_string()
    }

    #[tokio::test]
    async fn partitions_merge_through_the_accounted_merge() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        let text = plan_text(
            &engine,
            "SELECT value * 2 AS v FROM generate_series(1, 100000) WHERE value % 3 = 0",
        )
        .await;
        let lines: Vec<&str> = text.lines().map(str::trim).collect();
        assert!(lines[0].starts_with("ReservedOutputExec"), "{text}");
        assert!(
            lines[1].starts_with("AccountedMergeExec: input_partitions=4"),
            "{text}"
        );
        assert!(!text.contains("CoalescePartitionsExec"), "{text}");
        // A final aggregation merges partial states the same way.
        let text = plan_text(&engine, "SELECT count(*) FROM generate_series(1, 100000)").await;
        assert!(!text.contains("CoalescePartitionsExec"), "{text}");
        assert!(text.contains("AccountedMergeExec"), "{text}");
    }

    /// A view's plan, optimized with the engine's rules, ends in its own
    /// `ReservedOutputExec`; inside the plan that reads it only the root one
    /// stays.
    #[tokio::test]
    async fn only_the_root_reserves_the_output() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        engine
            .context()
            .sql("CREATE VIEW numbers AS SELECT value AS v FROM generate_series(1, 1000) WHERE value % 3 = 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let text = plan_text(&engine, "SELECT count(*) FROM numbers WHERE v > 10").await;
        assert_eq!(text.matches("ReservedOutputExec").count(), 1, "{text}");
        assert!(text.trim_start().starts_with("ReservedOutputExec"), "{text}");
        let nested: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            Arc::new(ReservedOutputExec::new(Arc::new(
                datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                    datafusion::arrow::datatypes::Schema::empty(),
                )),
            ))),
            0,
            Some(1),
        ));
        let plan = AccountedOutput
            .optimize(nested, &ConfigOptions::default())
            .unwrap();
        let text = displayable(plan.as_ref()).indent(false).to_string();
        assert_eq!(text.matches("ReservedOutputExec").count(), 1, "{text}");
    }

    /// A merge of two partitions or more becomes the accounted merge; one of
    /// a single partition has nothing to merge and stays.
    #[test]
    fn merges_of_several_partitions_are_accounted() {
        use datafusion::physical_plan::repartition::RepartitionExec;
        let leaf: Arc<dyn ExecutionPlan> = Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
        ));
        let spread = |n| -> Arc<dyn ExecutionPlan> {
            Arc::new(
                RepartitionExec::try_new(
                    Arc::clone(&leaf),
                    datafusion::physical_plan::Partitioning::RoundRobinBatch(n),
                )
                .unwrap(),
            )
        };
        for (partitions, accounted) in [(2, true), (3, true), (1, false)] {
            let merge: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(spread(partitions)));
            let text = displayable(accounted_merges(merge).unwrap().as_ref())
                .indent(false)
                .to_string();
            assert_eq!(
                text.contains("AccountedMergeExec"),
                accounted,
                "{partitions}: {text}"
            );
            assert_eq!(
                text.contains("CoalescePartitionsExec"),
                !accounted,
                "{partitions}: {text}"
            );
        }
    }

    /// A merge with a limit keeps it, above the accounted merge.
    #[tokio::test]
    async fn a_limited_merge_keeps_its_limit() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        let sql = "SELECT value FROM generate_series(1, 100000) WHERE value % 3 = 0 LIMIT 5";
        let text = plan_text(&engine, sql).await;
        assert!(!text.contains("CoalescePartitionsExec"), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 5, "{text}");
        assert!(values(&batches).iter().all(|v| v % 3 == 0));
    }

    fn values(batches: &[RecordBatch]) -> Vec<i64> {
        let mut values: Vec<i64> = batches
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
        values.sort_unstable();
        values
    }

    /// A paused consumer: the merge holds its batches in the pool or on disk,
    /// never more than the pool, and every row still arrives once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batches_the_pool_refuses_are_spilled_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let limit = ByteSize::mib(16);
        let engine = engine(dir.path(), limit, 4);
        // Batches of up to 2 MiB (`computed_batches`); the merge's exchange
        // slice is 2 MiB.
        let frame = engine
            .sql("SELECT value, lpad('', 4000, 'x') AS pad FROM generate_series(1, 10000) WHERE value > 0")
            .await
            .unwrap();
        let plan = frame.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(false).to_string();
        assert!(text.contains("AccountedMergeExec: input_partitions=4"), "{text}");
        let mut stream = frame.execute_stream().await.unwrap();
        let mut batches = vec![stream.next().await.unwrap().unwrap()];
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let paused = engine.memory();
        assert!(paused.spill_used_bytes > 0, "{paused:?}");
        assert!(paused.reserved_bytes <= limit.as_u64(), "{paused:?}");
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        assert_eq!(values(&batches), (1..=10_000).collect::<Vec<i64>>());
        drop(stream);
        assert_eq!(engine.memory().reserved_bytes, 0);
    }

    /// A refused batch is written in chunks of about 1 MiB, string views
    /// compacted, and read back whole and in order.
    #[tokio::test]
    async fn a_refused_batch_is_spilled_in_chunks() {
        use datafusion::arrow::array::{Array, AsArray, StringViewArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::runtime_env::RuntimeEnv;

        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let rows: i64 = 8192;
        let text = |v: i64| format!("{v:0>500}");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(1..=rows)),
                Arc::new(StringViewArray::from_iter_values((1..=rows).map(text))),
            ],
        )
        .unwrap();
        let runtime = Arc::new(RuntimeEnv::default());
        let metrics = ExecutionPlanMetricsSet::new();
        let spill = SpillManager::new(runtime, SpillMetrics::new(&metrics, 0), schema);
        let file = spill_in_chunks(&batch, &spill).unwrap().unwrap();
        // ~4 MiB of strings: written once, not once per chunk.
        let size = std::fs::metadata(file.path()).unwrap().len();
        assert!(size < 6 << 20, "{size} bytes spilled");
        let read: Vec<RecordBatch> = spill
            .read_spill_as_stream_unbuffered(file, None)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();
        // ~4.2 MiB in chunks of ~1 MiB: 5 chunks of at most ~2000 rows.
        let sizes: Vec<usize> = read.iter().map(RecordBatch::num_rows).collect();
        assert!(sizes.len() >= 4 && sizes.iter().all(|n| *n <= 2100), "{sizes:?}");
        let mut next = 1;
        for chunk in &read {
            let v = chunk.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = chunk.column(1).as_string_view();
            for row in 0..chunk.num_rows() {
                assert_eq!(v.value(row), next);
                assert_eq!(s.value(row), text(next));
                next += 1;
            }
        }
        assert_eq!(next, rows + 1);
        assert!(read.iter().all(|b| b.column(1).len() == b.num_rows()));
    }

    #[tokio::test]
    async fn merged_results_are_complete() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        let batches = engine
            .sql("SELECT value FROM generate_series(1, 100000) WHERE value % 7 = 0")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected: Vec<i64> = (1..=100_000).filter(|v| v % 7 == 0).collect();
        assert_eq!(values(&batches), expected);
        assert_eq!(engine.memory().reserved_bytes, 0);
    }

    /// The batch handed to the client stays reserved until the next one is
    /// asked for, and nothing is left once the stream is dropped.
    #[tokio::test]
    async fn the_batch_being_sent_is_reserved() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 1);
        let mut stream = engine
            .sql("SELECT value FROM generate_series(1, 100000)")
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 8192);
        let reserved = engine.memory().reserved_bytes;
        assert_eq!(reserved, u64::try_from(batch.get_array_memory_size()).unwrap());
        drop(stream);
        assert_eq!(engine.memory().reserved_bytes, 0);
    }

    /// A batch larger than the pool fails the query instead of growing it.
    #[tokio::test]
    async fn a_batch_the_pool_cannot_hold_fails_the_query() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(1), 1);
        let error = engine
            .sql("SELECT lpad('', 1024, 'x') AS s FROM generate_series(1, 8192)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
            "{error}"
        );
        assert!(error.to_string().contains("ClientOutput[0]"), "{error}");
        assert_eq!(engine.memory().reserved_bytes, 0);
    }

    /// Two partitions of one row each; the second panics after its row.
    #[derive(Debug)]
    struct PanicsAfterOneRow {
        properties: Arc<PlanProperties>,
    }

    impl PanicsAfterOneRow {
        fn new() -> Self {
            use datafusion::arrow::datatypes::{DataType, Field, Schema};
            use datafusion::physical_plan::empty::EmptyExec;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            Self {
                properties: Arc::clone(EmptyExec::new(schema).with_partitions(2).properties()),
            }
        }
    }

    impl DisplayAs for PanicsAfterOneRow {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "PanicsAfterOneRow")
        }
    }

    impl ExecutionPlan for PanicsAfterOneRow {
        fn name(&self) -> &'static str {
            "PanicsAfterOneRow"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
            let schema = self.schema();
            let value = i64::try_from(partition).unwrap();
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![value]))])?;
            let row = futures::stream::iter(vec![Ok(batch)]);
            let stream = if partition == 1 {
                row.chain(futures::stream::poll_fn(
                    |_| -> Poll<Option<Result<RecordBatch>>> { panic!("boom") },
                ))
                .boxed()
            } else {
                row.boxed()
            };
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }
    }

    /// A partition whose task panics fails the merged stream: it used to end
    /// the stream as if every partition had finished (rows lost in silence).
    #[tokio::test]
    async fn a_partition_that_panics_fails_the_merge() {
        let merge = AccountedMergeExec::new(Arc::new(PanicsAfterOneRow::new()));
        let stream = merge.execute(0, Arc::new(TaskContext::default())).unwrap();
        let error = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("panicked: boom"), "{error}");
    }
}
