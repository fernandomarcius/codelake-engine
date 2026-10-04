//! Hash joins whose build side the planner cannot size choose their operator
//! once that side is read (ADR 0003, amended 2026-10-02).
//!
//! A `HashJoinExec` holds its whole build side in a reservation that cannot
//! spill. [`crate::join_spill`] plans a sort-merge join instead when the
//! statistics say the build side does not fit the join's slice of the pool,
//! and keeps the hash join when they say nothing: a federated source reports
//! no statistics, so a remote table of millions of rows, or anything joined,
//! aggregated or windowed above one, was held whole and the query failed with
//! `EXCEEDED_LOCAL_MEMORY_LIMIT` where the reference engine answered.
//!
//! Such a join now reads its build side before deciding. [`BufferedBuildExec`]
//! takes the place of the build input and [`AdaptiveJoinExec`] the place of
//! the join, which stays in the plan as it was (its dynamic filter, its
//! partitioning, the rules after this one all see the hash join). When the
//! first output partition runs, every partition of the build input is read
//! at once into a buffer reserved in the pool, up to the join's slice
//! divided by the hash table overhead the planner assumes
//! ([`crate::join_spill::build_estimate`]):
//! - the whole build side fits: the hash join runs over the buffer, which
//!   gives each batch back to the pool as the join takes it;
//! - it does not (past the slice, or the pool refuses a batch): the buffer
//!   goes to disk, and the join runs as the sort-merge join
//!   [`crate::join_spill`] would have planned, its build side read back
//!   from the spill files. The probe side is read once either way.
//!
//! The memory the decision holds is the buffer, reserved in the pool (not
//! spillable: it spills itself), so a build side larger than the pool
//! completes as a sort-merge join instead of failing. A join that cannot run
//! as a sort-merge join ([`crate::join_spill::keeps_hash_join`]) is left as it
//! was.

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter_pushdown::{FilterDescription, FilterPushdownPhase};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet, SpillMetrics};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties, SpillManager,
};
use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};

use crate::output::AccountedMergeExec;

/// Consumer name of the buffered build side.
pub const BUILD_BUFFER: &str = "HashJoinBuildBuffer";

/// What a join runs as, once its build side is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The build side fit: the hash join, over the buffer.
    HashJoin,
    /// It did not: the sort-merge join, its build side read back from disk.
    SortMerge,
}

/// The rows of one build partition: in memory, then on disk.
#[derive(Debug, Default)]
struct Buffered {
    memory: VecDeque<RecordBatch>,
    files: Vec<RefCountedTempFile>,
}

/// The build side of one run of a join, shared by the tasks that read it and
/// the streams that read it back.
#[derive(Debug)]
struct Buffer {
    /// Bytes of build rows the hash join may get.
    cap: usize,
    reservation: Mutex<MemoryReservation>,
    /// Set once the build side does not fit: every batch goes to disk.
    spilling: AtomicBool,
    spill: SpillManager,
    /// Each build partition once it has been read; taken when read back.
    partitions: Mutex<Vec<Option<Buffered>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Plain data: a panic elsewhere leaves it consistent.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Bytes `batch` holds, as the hash join reserves them.
fn batch_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

impl Buffer {
    /// Reserves `batch` while the build side still fits; `false` (and from
    /// then on every batch is spilled) once it does not.
    fn hold(&self, batch: &RecordBatch) -> bool {
        let bytes = batch_bytes(batch);
        let reservation = lock(&self.reservation);
        if self.spilling.load(Ordering::Acquire) {
            return false;
        }
        if reservation.size().saturating_add(bytes) <= self.cap && reservation.try_grow(bytes).is_ok() {
            return true;
        }
        self.spilling.store(true, Ordering::Release);
        false
    }

    fn release(&self, bytes: usize) {
        lock(&self.reservation).shrink(bytes);
    }

    fn spilling(&self) -> bool {
        self.spilling.load(Ordering::Acquire)
    }

    /// Writes the batches of `buffered` held in memory to a spill file of
    /// its own and gives their memory back.
    fn spill_held(&self, buffered: &mut Buffered) -> Result<()> {
        if buffered.memory.is_empty() {
            return Ok(());
        }
        let mut file = self.spill.create_in_progress_file(BUILD_BUFFER)?;
        while let Some(batch) = buffered.memory.pop_front() {
            for chunk in crate::output::spill_chunks(&batch)? {
                file.append_batch(&chunk)?;
            }
            self.release(batch_bytes(&batch));
        }
        buffered.files.extend(file.finish()?);
        Ok(())
    }

    /// Once every partition is read: the hash join when nothing went to disk,
    /// else the sort-merge join, with every partition's rows on disk.
    fn settle(&self) -> Result<Outcome> {
        if !self.spilling() {
            return Ok(Outcome::HashJoin);
        }
        let mut partitions = lock(&self.partitions);
        for buffered in partitions.iter_mut().flatten() {
            self.spill_held(buffered)?;
        }
        Ok(Outcome::SortMerge)
    }
}

/// Reads build partition `partition` into `buffer`.
async fn read_partition(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    context: Arc<TaskContext>,
    buffer: Arc<Buffer>,
) -> Result<()> {
    let mut stream = input.execute(partition, context)?;
    let mut buffered = Buffered::default();
    let mut file = None;
    while let Some(batch) = stream.next().await.transpose()? {
        if batch.num_rows() == 0 {
            continue;
        }
        if buffer.hold(&batch) {
            buffered.memory.push_back(batch);
            continue;
        }
        let file = match &mut file {
            Some(file) => file,
            None => file.insert(buffer.spill.create_in_progress_file(BUILD_BUFFER)?),
        };
        while let Some(held) = buffered.memory.pop_front() {
            for chunk in crate::output::spill_chunks(&held)? {
                file.append_batch(&chunk)?;
            }
            buffer.release(batch_bytes(&held));
        }
        for chunk in crate::output::spill_chunks(&batch)? {
            file.append_batch(&chunk)?;
        }
    }
    if let Some(mut file) = file {
        buffered.files.extend(file.finish()?);
    }
    if let Some(slot) = lock(&buffer.partitions).get_mut(partition) {
        *slot = Some(buffered);
    }
    Ok(())
}

type Decision = Shared<BoxFuture<'static, Result<Outcome, Arc<DataFusionError>>>>;

/// One execution of a join: its buffer, the decision, and the sort-merge
/// join built for it when needed.
struct Run {
    buffer: Arc<Buffer>,
    decision: Decision,
    sort_merge: OnceLock<Result<Arc<dyn ExecutionPlan>, Arc<DataFusionError>>>,
}

impl fmt::Debug for Run {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Run")
            .field("buffer", &self.buffer)
            .finish_non_exhaustive()
    }
}

impl Run {
    /// Starts reading every partition of `input` at once.
    fn start(
        input: &Arc<dyn ExecutionPlan>,
        context: &Arc<TaskContext>,
        cap: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let partitions = input.output_partitioning().partition_count();
        let reservation = MemoryConsumer::new(BUILD_BUFFER).register(context.memory_pool());
        let buffer = Arc::new(Buffer {
            cap,
            reservation: Mutex::new(reservation),
            spilling: AtomicBool::new(false),
            spill: SpillManager::new(
                context.runtime_env(),
                SpillMetrics::new(metrics, 0),
                input.schema(),
            ),
            partitions: Mutex::new((0..partitions).map(|_| None).collect()),
        });
        let tasks: Vec<SpawnedTask<Result<()>>> = (0..partitions)
            .map(|partition| {
                SpawnedTask::spawn(read_partition(
                    Arc::clone(input),
                    partition,
                    Arc::clone(context),
                    Arc::clone(&buffer),
                ))
            })
            .collect();
        let settling = Arc::clone(&buffer);
        // Dropped with the last stream of the run: the tasks are aborted.
        let decision = async move {
            for task in tasks {
                task.join_unwind()
                    .await
                    .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))??;
            }
            let outcome = settling.settle()?;
            tracing::debug!(
                ?outcome,
                cap = settling.cap,
                "hash join without a build size decided"
            );
            Ok(outcome)
        }
        .map(|result: Result<Outcome>| result.map_err(Arc::new))
        .boxed()
        .shared();
        Self {
            buffer,
            decision,
            sort_merge: OnceLock::new(),
        }
    }
}

/// The run of the plan being executed, shared by the join and its buffered
/// build side. Held strongly until every output partition of the join has
/// started, then only by the streams.
#[derive(Debug, Default)]
struct Slot(Mutex<SlotState>);

#[derive(Debug, Default)]
struct SlotState {
    strong: Option<Arc<Run>>,
    weak: Weak<Run>,
    started: usize,
}

impl Slot {
    /// The current run, or one `start` makes; `partitions` output partitions
    /// share it.
    fn run(&self, partitions: usize, start: impl FnOnce() -> Run) -> Arc<Run> {
        let mut state = lock(&self.0);
        let run = if let Some(run) = state.weak.upgrade() {
            run
        } else {
            let run = Arc::new(start());
            state.weak = Arc::downgrade(&run);
            state.strong = Some(Arc::clone(&run));
            state.started = 0;
            run
        };
        state.started += 1;
        if state.started >= partitions {
            state.strong = None;
        }
        run
    }

    fn current(&self) -> Option<Arc<Run>> {
        lock(&self.0).weak.upgrade()
    }
}

/// The build input of a hash join that [`AdaptiveJoinExec`] decides: reads
/// back what the run buffered, or its input itself when no run is under way
/// (a plan that moved it off the build side).
#[derive(Debug)]
pub struct BufferedBuildExec {
    input: Arc<dyn ExecutionPlan>,
    slot: Arc<Slot>,
    properties: Arc<PlanProperties>,
}

impl BufferedBuildExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self::with_slot(input, Arc::new(Slot::default()))
    }

    fn with_slot(input: Arc<dyn ExecutionPlan>, slot: Arc<Slot>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            slot,
            properties,
        }
    }
}

impl DisplayAs for BufferedBuildExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BufferedBuildExec")
    }
}

impl ExecutionPlan for BufferedBuildExec {
    fn name(&self) -> &'static str {
        "BufferedBuildExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "BufferedBuildExec expects one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::with_slot(input, Arc::clone(&self.slot))))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<datafusion::common::Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let Some(run) = self.slot.current() else {
            return self.input.execute(partition, context);
        };
        let buffered = lock(&run.buffer.partitions)
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "BufferedBuildExec: build partition {partition} read back before it was read, or twice"
                ))
            })?;
        let files = buffered
            .files
            .into_iter()
            .map(|file| run.buffer.spill.read_spill_as_stream_unbuffered(file, None))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::pin(ReadBack {
            schema: self.input.schema(),
            buffer: Arc::clone(&run.buffer),
            memory: buffered.memory,
            handed: 0,
            files: files.into(),
        }))
    }
}

/// Reads one buffered build partition back: the batches held in memory,
/// each given back to the pool once the consumer asks for the next one,
/// then the spill files.
struct ReadBack {
    schema: SchemaRef,
    buffer: Arc<Buffer>,
    memory: VecDeque<RecordBatch>,
    /// Bytes of the batch last handed on, still reserved.
    handed: usize,
    files: VecDeque<SendableRecordBatchStream>,
}

impl ReadBack {
    fn give_back(&mut self) {
        if self.handed > 0 {
            self.buffer.release(std::mem::take(&mut self.handed));
        }
    }
}

impl Drop for ReadBack {
    fn drop(&mut self) {
        self.give_back();
        let bytes: usize = self.memory.iter().map(batch_bytes).sum();
        self.buffer.release(bytes);
    }
}

impl Stream for ReadBack {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.give_back();
        if let Some(batch) = self.memory.pop_front() {
            self.handed = batch_bytes(&batch);
            return Poll::Ready(Some(Ok(batch)));
        }
        while let Some(file) = self.files.front_mut() {
            match file.poll_next_unpin(cx) {
                Poll::Ready(None) => {
                    self.files.pop_front();
                }
                other => return other,
            }
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for ReadBack {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// A hash join (its child) whose build side is a [`BufferedBuildExec`]: see
/// the module documentation.
#[derive(Debug)]
pub struct AdaptiveJoinExec {
    join: Arc<dyn ExecutionPlan>,
    /// Bytes of build rows the hash join may get.
    cap: usize,
    /// Bytes each input of the sort-merge join may hold.
    side_budget: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl AdaptiveJoinExec {
    pub fn new(join: Arc<dyn ExecutionPlan>, cap: usize, side_budget: usize) -> Self {
        let properties = Arc::clone(join.properties());
        Self {
            join,
            cap,
            side_budget,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// `join` (a hash join) with its build input read through a
    /// [`BufferedBuildExec`], under an [`AdaptiveJoinExec`].
    pub fn wrap(
        node: &Arc<dyn ExecutionPlan>,
        join: &HashJoinExec,
        cap: usize,
        side_budget: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let buffered: Arc<dyn ExecutionPlan> = Arc::new(BufferedBuildExec::new(Arc::clone(join.left())));
        let join = Arc::clone(node).with_new_children(vec![buffered, Arc::clone(join.right())])?;
        Ok(Arc::new(Self::new(join, cap, side_budget)))
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn side_budget(&self) -> usize {
        self.side_budget
    }

    /// The hash join and its buffered build input, when the plan still
    /// has them where [`AdaptiveJoinExec::wrap`] put them.
    ///
    /// Nodes of one input that only wrap their input may stand between them
    /// (the Trino protocol makes every node yield to the scheduler).
    fn parts(&self) -> Option<(&HashJoinExec, &BufferedBuildExec)> {
        let join = through_wrappers(&self.join).downcast_ref::<HashJoinExec>()?;
        let buffered = through_wrappers(join.left()).downcast_ref::<BufferedBuildExec>()?;
        Some((join, buffered))
    }
}

/// `plan`, or the first node below it that is not a node of one input
/// wrapping it (a hash join, a buffered build side, anything with other
/// inputs), down a chain of single-input nodes.
fn through_wrappers(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    let mut node = plan;
    while !node.is::<HashJoinExec>() && !node.is::<BufferedBuildExec>() {
        match node.children().as_slice() {
            [only] => node = only,
            _ => break,
        }
    }
    node
}

/// `plan` with every join decided at run time put back as the hash join
/// it was.
pub fn plain_hash_joins(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(adaptive) = node.downcast_ref::<AdaptiveJoinExec>() else {
            return Ok(Transformed::no(node));
        };
        let unwrapped = Arc::clone(&adaptive.join)
            .transform_up(|inner| match inner.downcast_ref::<BufferedBuildExec>() {
                Some(buffered) => Ok(Transformed::yes(Arc::clone(&buffered.input))),
                None => Ok(Transformed::no(inner)),
            })
            .data()?;
        Ok(Transformed::yes(unwrapped))
    })
    .data()
}

/// Whether `join`'s build input is already buffered (a plan optimized
/// again: a view's).
pub fn is_buffered(join: &HashJoinExec) -> bool {
    join.left().is::<BufferedBuildExec>()
}

impl DisplayAs for AdaptiveJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AdaptiveJoinExec: hash_join_build_bytes={}, sort_merge_side_bytes={}",
            self.cap, self.side_budget
        )
    }
}

impl ExecutionPlan for AdaptiveJoinExec {
    fn name(&self) -> &'static str {
        "AdaptiveJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.join]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [join] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "AdaptiveJoinExec expects one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(join, self.cap, self.side_budget)))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let Some((_, buffered)) = self.parts() else {
            // A rule moved the buffer off the build side: a plain hash join.
            return self.join.execute(partition, context);
        };
        let partitions = self.properties.output_partitioning().partition_count();
        let run = buffered.slot.run(partitions, || {
            Run::start(&buffered.input, &context, self.cap, &self.metrics)
        });
        let hash_join = Arc::clone(&self.join);
        let side_budget = self.side_budget;
        let kept = Arc::clone(&run);
        let stream = async move {
            let outcome = run.decision.clone().await.map_err(DataFusionError::Shared)?;
            match outcome {
                Outcome::HashJoin => hash_join.execute(partition, context),
                Outcome::SortMerge => {
                    let plan = run
                        .sort_merge
                        .get_or_init(|| sort_merge(&hash_join, side_budget).map_err(Arc::new))
                        .clone()
                        .map_err(DataFusionError::Shared)?;
                    plan.execute(partition, context)
                }
            }
        };
        let schema = self.schema();
        let stream = futures::stream::once(stream).try_flatten().map(move |batch| {
            // The run lives as long as any of its streams.
            let _ = &kept;
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// `join` as the sort-merge join [`crate::join_spill`] plans, or an error
/// when it cannot be rebuilt (checked when the join was wrapped).
///
/// Built when the run needs it, after the physical rules ran: its merges
/// are the engine's accounted ones, as the last rule makes every merge
/// (`crate::output`).
fn sort_merge(join: &Arc<dyn ExecutionPlan>, side_budget: usize) -> Result<Arc<dyn ExecutionPlan>> {
    let join = through_wrappers(join)
        .downcast_ref::<HashJoinExec>()
        .ok_or_else(|| DataFusionError::Internal("AdaptiveJoinExec: its child is not a hash join".into()))?;
    let plan = crate::join_spill::sort_merge_of(join, side_budget)?.ok_or_else(|| {
        DataFusionError::Internal("AdaptiveJoinExec: the hash join has no sort-merge form".into())
    })?;
    plan.transform_up(|node| match node.downcast_ref::<CoalescePartitionsExec>() {
        Some(merge) if merge.fetch().is_none() => Ok(Transformed::yes(Arc::new(AccountedMergeExec::new(
            Arc::clone(merge.input()),
        )) as Arc<dyn ExecutionPlan>)),
        _ => Ok(Transformed::no(node)),
    })
    .data()
}

#[cfg(test)]
mod tests;
