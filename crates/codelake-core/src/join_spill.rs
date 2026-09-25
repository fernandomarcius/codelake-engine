/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Derived from `CayenneAntiJoinSortMergeRewriter` in
// `crates/cayenne/src/optimizer_rules.rs` of spiceai/spiceai (commit 10bf93e):
// the join types sort-merge supports, the null-equality check, the build-side
// size estimate (`estimated_arrow_width`, the 5/2 hash table overhead, the
// saturating arithmetic), the fair share of the pool across the hash joins of
// a plan, and the projection rebuilt from the hash join's indices.
//
// Modified by CodeLake contributors, 2026-09-24:
// - every rewritten join merges each input into one stream sorted once, and
//   spreads its output again as the hash join's was (the upstream rule did
//   this for some join shapes only);
// - the slice is taken from `CeilingPool`'s limit less its exchange buffers
//   (ADR 0003); the per-row-count floor and the source-specific paths
//   (Cayenne scans, CTE scans, aggregate builds) are gone;
// - a join is rewritten only when the sort-merge join answers the same rows:
//   no null-aware anti join, no residual (non-equi) condition, no computed
//   key, no key type the sort-merge comparator orders differently or does
//   not support, no limit on the join, no ordering a parent may rely on;
// - `Inexact` estimates must exceed the slice twice over; unknown ones keep
//   the hash join; column byte sizes from statistics replace the width guess
//   when the source reports them;
// - switched by a session property (`sort_merge_oversized_joins`).

//! Hash joins whose build side does not fit the memory pool run as
//! sort-merge joins, which spill (ADR 0003).
//!
//! `DataFusion`'s `HashJoinExec` holds its whole build side (the left input)
//! in memory, in a reservation that cannot spill: a build side larger than
//! the pool fails the query (`EXCEEDED_LOCAL_MEMORY_LIMIT`). A
//! `SortMergeJoinExec` over sorted inputs holds only the rows of one key
//! value, and spills them too; its sorts spill as well.
//!
//! [`SortMergeOversizedJoins`] runs right after `EnforceSorting` and replaces
//! a hash join with a sort-merge join when its estimated build side exceeds
//! the join's slice of the pool. Each input is merged into one stream and
//! sorted by one external sort, the join runs as one stream, and its output
//! is spread again over the hash join's partitions: the join trades
//! parallelism for a plan that fits the pool. A sort per partition keeps its
//! own merge headroom, and 2 × N of them starve a small pool before any holds
//! a batch; each side runs within a budget ([`JoinSideBudget`]), so that the
//! sort that finishes first cannot take, to merge its spilled runs, the
//! memory the other needs.
//!
//! A join stays a hash join whenever the sort-merge join could answer
//! different rows ([`keeps_hash_join`]):
//! - a null-aware anti join (`x NOT IN (subquery)`): the sort-merge join
//!   ignores NULLs (apache/datafusion#22810, ADR 0014);
//! - a join type the sort-merge join does not run (mark joins), or keys that
//!   compare NULLs as equal;
//! - a residual condition besides the equalities (`a.k = b.k AND a.x < b.y`):
//!   kept on the hash join, whose filtered outer and anti joins are the
//!   path the rest of the engine is tested on. A join with no equality at
//!   all is a nested loop join and never reaches this rule (its spill
//!   fallback stays off, ADR 0014: apache/datafusion#24675, #24746, #24677);
//! - a key computed by an expression (`a.x + 1 = b.y`): the engine checks
//!   arithmetic and evaluates `try` in hash join keys only (`TrinoTry`);
//! - a key whose type the sort-merge comparator does not support or orders
//!   differently from the hash join's equality: floating point (`NaN`,
//!   `-0.0`), timestamps with a time zone, `decimal` other than 128-bit,
//!   times, intervals, nested types;
//! - a limit pushed into the join, or an output ordering (a parent may rely
//!   on it).
//!
//! The estimate ([`build_estimate`]) is rows × bytes per row × 5/2 (the hash
//! table and the batches it concatenates). A row count the source knows
//! exactly is used as it is; an estimate (`Inexact`: a filter's selectivity,
//! a metastore count) must exceed the slice twice over; a build side of
//! unknown size keeps the hash join, which fails cleanly if it does not fit.
//! Statistics steer the choice of operator only: both operators answer the
//! same rows.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, IntervalUnit, Schema};
use datafusion::common::config::{ConfigExtension, ConfigOptions};
use datafusion::common::extensions_options;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{JoinType, NullEquality, Statistics};
use datafusion::common::{human_readable_size, resources_datafusion_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning, PlanProperties,
};

use crate::pool::{BUFFERS_DIVISOR, is_exchange_buffer};

/// Name of [`SortMergeOversizedJoins`].
pub const SORT_MERGE_OVERSIZED_JOINS: &str = "codelake_sort_merge_oversized_joins";

/// Name of the rule this one follows: the plan's exchanges and sorts are in
/// place, and the rule replaces those of the joins it rewrites.
pub const AFTER_RULE: &str = "EnforceSorting";

/// A join's slice is at most `1 / SLICE_DIVISOR` of what the pool leaves
/// after the exchange buffers, so the rest of the plan keeps room.
const SLICE_DIVISOR: usize = 2;

/// Each input of a rewritten join holds at most `1 / SIDE_DIVISOR` of what
/// the pool leaves after the exchange buffers.
const SIDE_DIVISOR: usize = 3;

/// An `Inexact` estimate must exceed the slice this many times over.
const INEXACT_MARGIN: usize = 2;

/// Hash table bytes per byte of build rows: the rows, the batch they are
/// concatenated into and the hash map (spiceai: 2-3× in practice).
const OVERHEAD_NUM: usize = 5;
const OVERHEAD_DEN: usize = 2;

/// Bytes per value assumed for a variable-width column the statistics do
/// not size.
const VARIABLE_WIDTH_GUESS: usize = 64;

extensions_options! {
    /// Planner options of [`SortMergeOversizedJoins`], set by the engine.
    pub struct JoinSpillOptions {
        /// Plan a hash join whose build side exceeds its slice of the pool as a sort-merge join.
        pub sort_merge_oversized_joins: bool, default = true
        /// The engine's memory pool, in bytes; without it the rule does nothing.
        pub memory_pool_bytes: Option<usize>, default = None
    }
}

impl ConfigExtension for JoinSpillOptions {
    const PREFIX: &'static str = "codelake_join_spill";
}

impl JoinSpillOptions {
    /// The options of an engine whose pool holds `pool_bytes`.
    pub fn for_pool(pool_bytes: usize) -> Self {
        Self {
            sort_merge_oversized_joins: true,
            memory_pool_bytes: Some(pool_bytes),
        }
    }
}

/// What the planner knows of a build side's size in memory, hash table
/// included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildEstimate {
    /// From an exact row count and exact or fixed widths.
    Exact(usize),
    /// From an estimated row count or guessed widths.
    Inexact(usize),
    Unknown,
}

/// A join's slice of a pool of `pool_bytes` when the plan holds `hash_joins`
/// hash joins: the smaller of half of what the exchange buffers leave and an
/// even share of it.
pub fn join_slice(pool_bytes: usize, hash_joins: usize) -> usize {
    let usable = pool_bytes.saturating_sub(pool_bytes / BUFFERS_DIVISOR);
    (usable / SLICE_DIVISOR).min(usable / hash_joins.max(1))
}

/// Bytes each input of a rewritten join may hold: a third of what the
/// exchange buffers leave, so both sides and the join's own buffers fit.
pub fn side_budget(pool_bytes: usize) -> usize {
    pool_bytes.saturating_sub(pool_bytes / BUFFERS_DIVISOR) / SIDE_DIVISOR
}

/// Whether a build side of `estimate` does not fit `slice`.
pub fn exceeds_slice(estimate: BuildEstimate, slice: usize) -> bool {
    match estimate {
        BuildEstimate::Exact(bytes) => bytes > slice,
        BuildEstimate::Inexact(bytes) => bytes > slice.saturating_mul(INEXACT_MARGIN),
        BuildEstimate::Unknown => false,
    }
}

/// Bytes of one value of `data_type`, `None` for a variable width.
fn fixed_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Null => Some(0),
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth)
        | DataType::Decimal32(_, _) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _)
        | DataType::Interval(IntervalUnit::DayTime) => Some(8),
        DataType::Interval(IntervalUnit::MonthDayNano) | DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        DataType::FixedSizeBinary(size) => usize::try_from(*size).ok(),
        DataType::FixedSizeList(field, length) => {
            fixed_width(field.data_type())?.checked_mul(usize::try_from(*length).ok()?)
        }
        DataType::Struct(fields) => fields.iter().try_fold(0_usize, |sum, field| {
            sum.checked_add(fixed_width(field.data_type())?)
        }),
        _ => None,
    }
}

/// Estimated memory of `plan`'s rows once held by a hash join.
pub fn build_estimate(plan: &dyn ExecutionPlan) -> BuildEstimate {
    match plan.partition_statistics(None) {
        Ok(statistics) => estimate(&statistics, &plan.schema()),
        Err(_) => BuildEstimate::Unknown,
    }
}

/// [`build_estimate`] of rows of `schema` described by `statistics`.
pub fn estimate(statistics: &Statistics, schema: &Schema) -> BuildEstimate {
    let (rows, mut exact) = match statistics.num_rows {
        Precision::Exact(rows) => (rows, true),
        Precision::Inexact(rows) => (rows, false),
        Precision::Absent => return BuildEstimate::Unknown,
    };
    let mut sized = 0_usize;
    let mut unsized_columns = 0_usize;
    for (index, field) in schema.fields().iter().enumerate() {
        let known = statistics.column_statistics.get(index).map(|c| c.byte_size);
        let bytes = match (fixed_width(field.data_type()), known) {
            (Some(width), _) => width.saturating_mul(rows),
            (None, Some(Precision::Exact(bytes))) => bytes,
            (None, Some(Precision::Inexact(bytes))) => {
                exact = false;
                bytes
            }
            (None, _) => {
                unsized_columns += 1;
                0
            }
        };
        sized = sized.saturating_add(bytes);
    }
    // A column of unknown size: the whole rows' size when the source knows
    // it, a guess per value otherwise.
    let payload = if unsized_columns == 0 {
        sized
    } else {
        match statistics.total_byte_size {
            Precision::Exact(total) => total,
            Precision::Inexact(total) => {
                exact = false;
                total
            }
            Precision::Absent => {
                exact = false;
                let guessed = VARIABLE_WIDTH_GUESS.saturating_mul(unsized_columns);
                sized.saturating_add(guessed.saturating_mul(rows))
            }
        }
    };
    // u128: a saturated payload times 5 must not wrap before the division.
    let estimate = payload as u128 * OVERHEAD_NUM as u128 / OVERHEAD_DEN as u128;
    let estimate = usize::try_from(estimate).unwrap_or(usize::MAX);
    if exact {
        BuildEstimate::Exact(estimate)
    } else {
        BuildEstimate::Inexact(estimate)
    }
}

/// Key types the sort-merge join compares as the hash join does: the sort
/// and the join's comparator agree with the hash join's equality, NULL
/// handling apart (`NullEqualsNothing` in both).
fn sort_merge_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, None)
    )
}

/// Why `join` must stay a hash join whatever its size, or `None`.
pub fn keeps_hash_join(join: &HashJoinExec) -> Option<&'static str> {
    if join.null_aware {
        return Some("null-aware anti join (apache/datafusion#22810)");
    }
    if !matches!(
        join.join_type(),
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftSemi
            | JoinType::RightSemi
            | JoinType::LeftAnti
            | JoinType::RightAnti
    ) {
        return Some("join type the sort-merge join does not run");
    }
    if join.null_equality() != NullEquality::NullEqualsNothing {
        return Some("keys that compare NULLs as equal");
    }
    if join.filter().is_some() {
        return Some("residual condition besides the equalities");
    }
    if join.on().is_empty() {
        return Some("no equality");
    }
    if join.fetch().is_some() {
        return Some("limit pushed into the join");
    }
    if join.properties().output_ordering().is_some() {
        return Some("output ordering a parent may rely on");
    }
    // `TrinoTry` (the last rule) installs checked arithmetic and `try` in
    // hash join keys, not in sort-merge join keys: a computed key would be
    // sorted one way and compared another.
    if !join
        .on()
        .iter()
        .all(|(l, r)| l.downcast_ref::<Column>().is_some() && r.downcast_ref::<Column>().is_some())
    {
        return Some("key computed by an expression");
    }
    let (left, right) = (join.left().schema(), join.right().schema());
    let supported =
        join.on().iter().all(
            |(l, r)| match (l.data_type(left.as_ref()), r.data_type(right.as_ref())) {
                (Ok(l), Ok(r)) => l == r && sort_merge_key(&l),
                _ => false,
            },
        );
    if !supported {
        return Some("key type the sort-merge join compares differently");
    }
    None
}

/// `plan` without the exchange `EnforceDistribution` put on top of it
/// (a hash exchange or a merge), which the sort-merge join replaces.
fn below_exchange(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(exchange) = plan.downcast_ref::<RepartitionExec>()
        && !exchange.preserve_order()
    {
        return Arc::clone(exchange.input());
    }
    if let Some(merge) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return Arc::clone(merge.input());
    }
    Arc::clone(plan)
}

/// `input` merged into one stream and sorted on `keys` by one external sort,
/// which with everything below it holds at most `budget` bytes.
fn sorted_stream(
    input: Arc<dyn ExecutionPlan>,
    keys: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
    budget: usize,
) -> Option<Arc<dyn ExecutionPlan>> {
    let ordering = LexOrdering::new(keys.map(|expr| PhysicalSortExpr {
        expr,
        options: SortOptions::default(),
    }))?;
    let input: Arc<dyn ExecutionPlan> = if input.output_partitioning().partition_count() > 1 {
        Arc::new(CoalescePartitionsExec::new(input))
    } else {
        input
    };
    let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));
    Some(Arc::new(JoinSideBudget::new(sort, budget)))
}

/// `join` as a sort-merge join with the same output schema and partitioning,
/// or `None` when that cannot be rebuilt. Each side holds at most
/// `side_budget` bytes.
///
/// Each input is merged into one stream and sorted once: a sort per
/// partition keeps its own merge headroom (`sort_spill_reservation_bytes`)
/// outside what it may spill, and two inputs over N partitions starve a
/// small pool before any of them holds a batch (N = 4 under 16 MiB). Each
/// side runs within a budget ([`JoinSideBudget`]): the join reads its inputs
/// together, and the sort that finishes first merges its spilled runs with
/// as many files as the pool has room for, leaving the other sort less than
/// one batch. The join then runs as one stream and its output is spread again
/// as the hash join's was: by the same hash, which a partitioned parent pairs
/// with its other input, or round robin.
fn sort_merge(join: &HashJoinExec, side_budget: usize) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let left = below_exchange(join.left());
    let right = below_exchange(join.right());
    let (Some(left), Some(right)) = (
        sorted_stream(left, join.on().iter().map(|(l, _)| Arc::clone(l)), side_budget),
        sorted_stream(right, join.on().iter().map(|(_, r)| Arc::clone(r)), side_budget),
    ) else {
        return Ok(None);
    };
    let merge: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        left,
        right,
        join.on().to_vec(),
        None,
        *join.join_type(),
        vec![SortOptions::default(); join.on().len()],
        join.null_equality(),
    )?);
    let Some(output) = same_output(merge, join)? else {
        return Ok(None);
    };
    let partitioning = match join.properties().output_partitioning() {
        Partitioning::Hash(exprs, n) if *n > 1 => Partitioning::Hash(exprs.clone(), *n),
        other if other.partition_count() > 1 => Partitioning::RoundRobinBatch(other.partition_count()),
        _ => return Ok(Some(output)),
    };
    Ok(Some(Arc::new(RepartitionExec::try_new(output, partitioning)?)))
}

/// `merge` with the columns `join` outputs: the hash join's projection, when
/// it has one, rebuilt by index. The hash join's partitioning expressions then
/// apply to the result unchanged.
fn same_output(merge: Arc<dyn ExecutionPlan>, join: &HashJoinExec) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let source = merge.schema();
    let target = join.schema();
    let Some(indices) = join.projection.as_ref() else {
        return Ok(same_types(&source, &target).then_some(merge));
    };
    // The projection indexes the hash join's join schema, which is the
    // sort-merge join's schema (`build_join_schema`): rebuild it by index,
    // never by name (a self-join repeats names).
    if indices.len() != target.fields().len() {
        return Ok(None);
    }
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(indices.len());
    for (out, &index) in indices.iter().enumerate() {
        let Some(field) = source.fields().get(index) else {
            return Ok(None);
        };
        if field.data_type() != target.field(out).data_type() {
            return Ok(None);
        }
        exprs.push((
            Arc::new(Column::new(field.name(), index)),
            target.field(out).name().clone(),
        ));
    }
    Ok(Some(Arc::new(ProjectionExec::try_new(exprs, merge)?)))
}

fn same_types(source: &Schema, target: &Schema) -> bool {
    source.fields().len() == target.fields().len()
        && source
            .fields()
            .iter()
            .zip(target.fields())
            .all(|(s, t)| s.data_type() == t.data_type())
}

fn count_hash_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
    usize::from(plan.is::<HashJoinExec>()) + plan.children().into_iter().map(count_hash_joins).sum::<usize>()
}

/// Memory pool of one join input: forwards every reservation to the
/// engine's pool (which keeps the ceiling and names the consumers in its
/// errors) and refuses what would take the input's operators past `budget`.
/// An operator refused spills, as when the engine's pool refuses it.
/// Exchange channels are not counted: they have their own slice of the
/// engine's pool and spill what it refuses.
#[derive(Debug)]
struct BudgetPool {
    inner: Arc<dyn MemoryPool>,
    budget: usize,
    used: Mutex<usize>,
}

impl BudgetPool {
    fn used(&self) -> MutexGuard<'_, usize> {
        // A plain counter: a panic elsewhere leaves it consistent.
        self.used.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl fmt::Display for BudgetPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "join_side_budget(used: {}, budget: {}) over {}",
            human_readable_size(*self.used()),
            human_readable_size(self.budget),
            self.inner
        )
    }
}

impl MemoryPool for BudgetPool {
    fn name(&self) -> &'static str {
        "join_side_budget"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        if !is_exchange_buffer(reservation.consumer()) {
            *self.used() += additional;
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        if !is_exchange_buffer(reservation.consumer()) {
            let mut used = self.used();
            *used = used.saturating_sub(shrink);
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if is_exchange_buffer(reservation.consumer()) {
            return self.inner.try_grow(reservation, additional);
        }
        let mut used = self.used();
        let room = self.budget.saturating_sub(*used);
        if additional > room {
            return Err(resources_datafusion_err!(
                "Failed to allocate additional {} for {} with {} already allocated for this reservation - {} remain available for this join input",
                human_readable_size(additional),
                reservation.consumer().name(),
                human_readable_size(reservation.size()),
                human_readable_size(room)
            ));
        }
        self.inner.try_grow(reservation, additional)?;
        *used += additional;
        Ok(())
    }

    fn reserved(&self) -> usize {
        *self.used()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.budget)
    }
}

/// Runs its input (one sorted side of a rewritten join) with a memory pool
/// in front of the engine's that refuses what would take the input's
/// operators past `budget` bytes (exchange channels apart). A refused
/// operator spills, as when the engine's pool refuses it.
#[derive(Debug)]
pub struct JoinSideBudget {
    input: Arc<dyn ExecutionPlan>,
    budget: usize,
    properties: Arc<PlanProperties>,
}

impl JoinSideBudget {
    pub fn new(input: Arc<dyn ExecutionPlan>, budget: usize) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            budget,
            properties,
        }
    }
}

impl DisplayAs for JoinSideBudget {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinSideBudget: bytes={}", self.budget)
    }
}

impl ExecutionPlan for JoinSideBudget {
    fn name(&self) -> &'static str {
        "JoinSideBudget"
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
                "JoinSideBudget expects one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(input, self.budget)))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let runtime = context.runtime_env();
        let pool: Arc<dyn MemoryPool> = Arc::new(BudgetPool {
            inner: Arc::clone(&runtime.memory_pool),
            budget: self.budget,
            used: Mutex::new(0),
        });
        let budgeted = RuntimeEnvBuilder::from_runtime_env(&runtime)
            .with_memory_pool(pool)
            .build_arc()?;
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            context.session_config().clone(),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            budgeted,
        ));
        self.input.execute(partition, context)
    }
}

/// Physical rule, right after `EnforceSorting`: see the module documentation.
#[derive(Debug, Default)]
pub struct SortMergeOversizedJoins;

impl PhysicalOptimizerRule for SortMergeOversizedJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(options) = config.extensions.get::<JoinSpillOptions>() else {
            return Ok(plan);
        };
        let Some(pool_bytes) = options.memory_pool_bytes else {
            return Ok(plan);
        };
        if !options.sort_merge_oversized_joins {
            return Ok(plan);
        }
        let slice = join_slice(pool_bytes, count_hash_joins(&plan));
        let side_budget = side_budget(pool_bytes);
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if let Some(reason) = keeps_hash_join(join) {
                tracing::trace!(reason, "hash join kept");
                return Ok(Transformed::no(node));
            }
            let estimate = build_estimate(below_exchange(join.left()).as_ref());
            if !exceeds_slice(estimate, slice) {
                return Ok(Transformed::no(node));
            }
            match sort_merge(join, side_budget)? {
                Some(merge) => {
                    tracing::debug!(?estimate, slice, "hash join planned as a sort-merge join");
                    Ok(Transformed::yes(merge))
                }
                None => Ok(Transformed::no(node)),
            }
        })
        .data()
    }

    fn name(&self) -> &'static str {
        SORT_MERGE_OVERSIZED_JOINS
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// `rules` with [`SortMergeOversizedJoins`] right after `EnforceSorting`, or
/// last when that rule is missing.
pub fn with_sort_merge_oversized_joins(
    mut rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let position = rules
        .iter()
        .position(|rule| rule.name() == AFTER_RULE)
        .map_or(rules.len(), |index| index + 1);
    rules.insert(position, Arc::new(SortMergeOversizedJoins));
    rules
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::common::ColumnStatistics;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::execution::config::SessionConfig;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    const MIB: usize = 1 << 20;

    // -- Decision ------------------------------------------------------------

    #[test]
    fn the_slice_is_half_of_the_pool_less_buffers_or_an_even_share() {
        // 128 MiB less 1/8 for the exchange buffers: 112 MiB usable.
        assert_eq!(join_slice(128 * MIB, 0), 56 * MIB);
        assert_eq!(join_slice(128 * MIB, 1), 56 * MIB);
        assert_eq!(join_slice(128 * MIB, 2), 56 * MIB);
        assert_eq!(join_slice(128 * MIB, 4), 28 * MIB);
        assert_eq!(join_slice(0, 3), 0);
        assert_eq!(join_slice(usize::MAX, 1), (usize::MAX - usize::MAX / 8) / 2);
    }

    #[test]
    fn exact_estimates_count_as_they_are_inexact_ones_twice_over() {
        let slice = 10 * MIB;
        assert!(!exceeds_slice(BuildEstimate::Exact(slice), slice));
        assert!(exceeds_slice(BuildEstimate::Exact(slice + 1), slice));
        assert!(!exceeds_slice(BuildEstimate::Inexact(slice + 1), slice));
        assert!(!exceeds_slice(BuildEstimate::Inexact(2 * slice), slice));
        assert!(exceeds_slice(BuildEstimate::Inexact(2 * slice + 1), slice));
        assert!(!exceeds_slice(BuildEstimate::Unknown, 0));
        assert!(!exceeds_slice(BuildEstimate::Inexact(usize::MAX), usize::MAX));
        assert!(exceeds_slice(BuildEstimate::Exact(1), 0));
    }

    fn column(byte_size: Precision<usize>) -> ColumnStatistics {
        ColumnStatistics {
            byte_size,
            ..ColumnStatistics::new_unknown()
        }
    }

    fn stats(rows: Precision<usize>, columns: Vec<ColumnStatistics>) -> Statistics {
        Statistics {
            num_rows: rows,
            total_byte_size: Precision::Absent,
            column_statistics: columns,
        }
    }

    fn schema(types: &[DataType]) -> Schema {
        Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn the_estimate_is_rows_times_width_times_five_halves() {
        let fixed = schema(&[DataType::Int64, DataType::Int32, DataType::Decimal128(38, 2)]);
        let unknown = || vec![column(Precision::Absent); 3];
        // (8 + 4 + 16) × 1000 × 5 / 2.
        assert_eq!(
            estimate(&stats(Precision::Exact(1000), unknown()), &fixed),
            BuildEstimate::Exact(70_000)
        );
        assert_eq!(
            estimate(&stats(Precision::Inexact(1000), unknown()), &fixed),
            BuildEstimate::Inexact(70_000)
        );
        assert_eq!(
            estimate(&stats(Precision::Absent, unknown()), &fixed),
            BuildEstimate::Unknown
        );
    }

    #[test]
    fn variable_widths_come_from_statistics_or_a_guess() {
        let text = schema(&[DataType::Int64, DataType::Utf8View]);
        // An exact byte size keeps the estimate exact: (8 × 100 + 5000) × 5 / 2.
        let sized = stats(
            Precision::Exact(100),
            vec![column(Precision::Absent), column(Precision::Exact(5000))],
        );
        assert_eq!(estimate(&sized, &text), BuildEstimate::Exact(14_500));
        let estimated = stats(
            Precision::Exact(100),
            vec![column(Precision::Absent), column(Precision::Inexact(5000))],
        );
        assert_eq!(estimate(&estimated, &text), BuildEstimate::Inexact(14_500));
        // No size: 64 bytes a value, and the estimate is a guess.
        let guessed = stats(
            Precision::Exact(100),
            vec![column(Precision::Absent), column(Precision::Absent)],
        );
        assert_eq!(estimate(&guessed, &text), BuildEstimate::Inexact(18_000));
        // Missing column statistics count as unknown sizes.
        assert_eq!(
            estimate(&stats(Precision::Exact(100), Vec::new()), &text),
            BuildEstimate::Inexact(18_000)
        );
        // A column of unknown size: the source's total size of the rows.
        let total = |total| Statistics {
            total_byte_size: total,
            ..stats(Precision::Exact(100), Vec::new())
        };
        assert_eq!(
            estimate(&total(Precision::Exact(40_000)), &text),
            BuildEstimate::Exact(100_000)
        );
        assert_eq!(
            estimate(&total(Precision::Inexact(40_000)), &text),
            BuildEstimate::Inexact(100_000)
        );
        // Every column sized: the total is not needed.
        let fixed = schema(&[DataType::Int64]);
        assert_eq!(
            estimate(&total(Precision::Exact(1)), &fixed),
            BuildEstimate::Exact(2000)
        );
    }

    #[test]
    fn huge_estimates_saturate_instead_of_wrapping() {
        let wide = schema(&[DataType::Decimal256(76, 0), DataType::Decimal256(76, 0)]);
        let unknown = vec![column(Precision::Absent); 2];
        assert_eq!(
            estimate(&stats(Precision::Exact(usize::MAX / 4), unknown), &wide),
            BuildEstimate::Exact(usize::MAX)
        );
    }

    #[test]
    fn widths_of_fixed_types() {
        assert_eq!(fixed_width(&DataType::Boolean), Some(1));
        assert_eq!(fixed_width(&DataType::Int16), Some(2));
        assert_eq!(fixed_width(&DataType::Date32), Some(4));
        assert_eq!(
            fixed_width(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            Some(8)
        );
        assert_eq!(fixed_width(&DataType::Decimal128(10, 2)), Some(16));
        assert_eq!(fixed_width(&DataType::Decimal256(10, 2)), Some(32));
        assert_eq!(fixed_width(&DataType::FixedSizeBinary(12)), Some(12));
        let list = DataType::FixedSizeList(Arc::new(Field::new("x", DataType::Int32, true)), 3);
        assert_eq!(fixed_width(&list), Some(12));
        let pair = DataType::Struct(
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int8, true),
            ]
            .into(),
        );
        assert_eq!(fixed_width(&pair), Some(9));
        let named = DataType::Struct(vec![Field::new("a", DataType::Utf8, true)].into());
        assert_eq!(fixed_width(&named), None);
        assert_eq!(fixed_width(&DataType::Utf8), None);
        assert_eq!(fixed_width(&DataType::Null), Some(0));
    }

    #[test]
    fn keys_the_sort_merge_join_compares_like_the_hash_join() {
        for data_type in [
            DataType::Int64,
            DataType::UInt32,
            DataType::Utf8,
            DataType::Utf8View,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::Decimal128(10, 2),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Boolean,
        ] {
            assert!(sort_merge_key(&data_type), "{data_type}");
        }
        for data_type in [
            DataType::Float64,
            DataType::Float32,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            DataType::Decimal256(40, 2),
            DataType::Decimal64(10, 2),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Null,
            DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into()),
        ] {
            assert!(!sort_merge_key(&data_type), "{data_type}");
        }
    }

    #[test]
    fn the_rule_follows_enforce_sorting() {
        let names: Vec<String> = crate::engine::physical_rules()
            .iter()
            .map(|r| r.name().to_owned())
            .collect();
        let sorting = names.iter().position(|n| n == AFTER_RULE).unwrap();
        assert_eq!(names[sorting + 1], SORT_MERGE_OVERSIZED_JOINS, "{names:?}");
        // Before the rules that read or finish the plan's exchanges and joins.
        for later in [
            "HashJoinBuffering",
            "SanityCheckPlan",
            crate::output::ACCOUNTED_OUTPUT,
        ] {
            let index = names.iter().position(|n| n == later).unwrap();
            assert!(sorting + 1 < index, "{later}: {names:?}");
        }
        // Without `EnforceSorting` the rule goes last.
        let alone = with_sort_merge_oversized_joins(Vec::new());
        assert_eq!(alone[0].name(), SORT_MERGE_OVERSIZED_JOINS);
    }

    #[test]
    fn a_side_budget_refuses_past_its_bytes_and_forwards_the_rest() {
        let engine: Arc<dyn MemoryPool> = Arc::new(crate::pool::CeilingPool::new(64 * MIB));
        let budget: Arc<dyn MemoryPool> = Arc::new(BudgetPool {
            inner: Arc::clone(&engine),
            budget: MIB,
            used: Mutex::new(0),
        });
        let sort = MemoryConsumer::new("ExternalSorter[0]")
            .with_can_spill(true)
            .register(&budget);
        sort.try_grow(MIB / 2).unwrap();
        let error = sort.try_grow(MIB).unwrap_err().to_string();
        assert!(error.contains("Resources exhausted"), "{error}");
        assert!(error.contains("this join input"), "{error}");
        assert_eq!((budget.reserved(), engine.reserved()), (MIB / 2, MIB / 2));
        // Unchecked growth is recorded in both.
        sort.grow(MIB);
        assert_eq!(
            (budget.reserved(), engine.reserved()),
            (MIB + MIB / 2, MIB + MIB / 2)
        );
        sort.shrink(MIB);
        assert_eq!((budget.reserved(), engine.reserved()), (MIB / 2, MIB / 2));
        // Exchange channels use the engine's pool only.
        let exchange = MemoryConsumer::new("RepartitionExec[0]")
            .with_can_spill(true)
            .register(&budget);
        exchange.try_grow(2 * MIB).unwrap();
        exchange.grow(MIB);
        exchange.shrink(MIB);
        assert_eq!(
            (budget.reserved(), engine.reserved()),
            (MIB / 2, MIB / 2 + 2 * MIB)
        );
        drop(exchange);
        drop(sort);
        assert_eq!((budget.reserved(), engine.reserved()), (0, 0));
        assert!(matches!(budget.memory_limit(), MemoryLimit::Finite(MIB)));
        assert!(budget.to_string().contains("budget: 1024.0 KB"), "{budget}");
        // The engine's pool still refuses what exceeds it.
        let greedy = MemoryConsumer::new("HashJoinInput").register(&budget);
        let small: Arc<dyn MemoryPool> = Arc::new(BudgetPool {
            inner: Arc::new(crate::pool::CeilingPool::new(MIB)),
            budget: 4 * MIB,
            used: Mutex::new(0),
        });
        let inside = MemoryConsumer::new("HashJoinInput").register(&small);
        assert!(inside.try_grow(2 * MIB).is_err());
        assert_eq!(small.reserved(), 0);
        drop(greedy);
    }

    #[test]
    fn the_side_budget_is_a_third_of_the_pool_less_buffers() {
        assert_eq!(side_budget(96 * MIB), 28 * MIB);
        assert_eq!(side_budget(0), 0);
        let node = JoinSideBudget::new(
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                schema(&[DataType::Int64]),
            ))),
            1234,
        );
        let text = displayable(&node).one_line().to_string();
        assert_eq!(text.trim(), "JoinSideBudget: bytes=1234");
    }

    // -- Plans and rows ------------------------------------------------------

    /// `a.k` in 0..2999 and `b.k` in 0..3999, both with NULLs; `s` text keys
    /// that match on multiples of 3.
    const SETUP: [&str; 2] = [
        "CREATE TABLE a AS SELECT value AS id, \
                CASE WHEN value % 50 = 0 THEN NULL ELSE value % 3000 END AS k, \
                value * 2 AS v, \
                CASE WHEN value % 45 = 0 THEN NULL ELSE 'key-' || CAST(value % 900 AS VARCHAR) END AS s \
         FROM generate_series(1, 20000)",
        "CREATE TABLE b AS SELECT value AS id, \
                CASE WHEN value % 70 = 0 THEN NULL ELSE value % 4000 END AS k, \
                value AS w, \
                'key-' || CAST(value * 3 % 1200 AS VARCHAR) AS s \
         FROM generate_series(1, 30000)",
    ];

    /// A session with the engine's rules whose planner believes the pool
    /// holds `pool` bytes: only the inputs of a rewritten join run within
    /// their budget (a third of it), in batches of 512 rows.
    async fn session(pool: usize) -> SessionContext {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(512)
            .with_sort_spill_reservation_bytes(16 << 10)
            .with_option_extension(JoinSpillOptions::for_pool(pool));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_analyzer_rules(crate::engine::analyzer_rules())
            .with_optimizer_rules(crate::engine::optimizer_rules())
            .with_physical_optimizer_rules(crate::engine::physical_rules())
            .with_query_planner(crate::engine::query_planner())
            .build();
        let ctx = SessionContext::new_with_state(state);
        for sql in SETUP {
            ctx.sql(sql).await.unwrap().collect().await.unwrap();
        }
        ctx
    }

    async fn plan(ctx: &SessionContext, sql: &str) -> String {
        let plan = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        displayable(plan.as_ref()).indent(true).to_string()
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
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
        rows
    }

    /// Every join shape the rule rewrites, NULL keys included: the
    /// sort-merge plan answers the hash join's rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rewritten_joins_answer_the_hash_join_rows() {
        let small = session(512 << 10).await;
        let large = session(1 << 40).await;
        for sql in [
            "SELECT a.k % 7 AS g, count(*), sum(a.id), sum(b.id), count(b.k) \
             FROM a JOIN b ON a.k = b.k GROUP BY 1",
            "SELECT a.k % 7 AS g, count(*), sum(a.id), count(b.id), count(a.k) \
             FROM a LEFT JOIN b ON a.k = b.k GROUP BY 1",
            "SELECT b.k % 7 AS g, count(*), count(a.id), sum(b.id), count(b.k) \
             FROM a RIGHT JOIN b ON a.k = b.k GROUP BY 1",
            "SELECT coalesce(a.k, b.k) % 7 AS g, count(*), count(a.id), count(b.id), sum(a.id), sum(b.id) \
             FROM a FULL JOIN b ON a.k = b.k GROUP BY 1",
            "SELECT count(*), sum(id), count(k) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
            "SELECT count(*), sum(id), count(k) FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
            "SELECT count(*), sum(id), count(k) FROM b WHERE k IN (SELECT k FROM a)",
            "SELECT count(*), sum(id), count(k) FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.k = b.k)",
            "SELECT a.s, count(*), sum(a.id), sum(b.id) FROM a JOIN b ON a.s = b.s GROUP BY 1",
            "SELECT count(*), count(a.s), count(b.s), sum(a.id), sum(b.id) \
             FROM a FULL JOIN b ON a.s = b.s AND a.k = b.k",
            "SELECT count(*), sum(x.id), sum(y.id) FROM a x JOIN a y ON x.k = y.k",
            // A computed key is projected below the join, checked.
            "SELECT count(*), sum(a.id), sum(b.id) FROM a JOIN b ON a.k + 1 = b.k",
        ] {
            let merged = plan(&small, sql).await;
            assert!(merged.contains("SortMergeJoinExec"), "{sql}\n{merged}");
            assert!(!merged.contains("HashJoinExec"), "{sql}\n{merged}");
            let hashed = plan(&large, sql).await;
            assert!(hashed.contains("HashJoinExec"), "{sql}\n{hashed}");
            assert!(!hashed.contains("SortMergeJoinExec"), "{sql}\n{hashed}");
            let expected = rows(&large, sql).await;
            assert!(!expected.is_empty(), "{sql}");
            assert_eq!(rows(&small, sql).await, expected, "{sql}");
        }
    }

    /// Joins the sort-merge join could answer differently stay hash joins
    /// however small the pool, and still answer their rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn guarded_joins_stay_hash_joins() {
        let small = session(1024).await;
        let large = session(1 << 40).await;
        for (label, sql) in [
            (
                "null-aware anti join",
                "SELECT count(*), sum(id) FROM a WHERE k NOT IN (SELECT k FROM b WHERE k > 100)",
            ),
            (
                "residual condition",
                "SELECT count(*), sum(a.id) FROM a JOIN b ON a.k = b.k AND a.v < b.w",
            ),
            (
                "residual condition, outer",
                "SELECT count(*), count(b.id) FROM a LEFT JOIN b ON a.k = b.k AND a.v < b.w",
            ),
            (
                "floating point keys",
                "SELECT count(*) FROM a JOIN b ON CAST(a.k AS DOUBLE) = CAST(b.k AS DOUBLE)",
            ),
            (
                "NULLs compare equal",
                "SELECT count(*) FROM a JOIN b ON a.k IS NOT DISTINCT FROM b.k",
            ),
            (
                "mark join",
                "SELECT count(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR a.v > 100",
            ),
            (
                "unknown size",
                "SELECT count(*) FROM (SELECT value AS k FROM generate_series(1, 20000)) x \
                 JOIN (SELECT value AS k FROM generate_series(1, 30000)) y ON x.k = y.k",
            ),
        ] {
            let text = plan(&small, sql).await;
            assert!(!text.contains("SortMergeJoinExec"), "{label}: {sql}\n{text}");
            assert!(
                text.contains("HashJoinExec") || text.contains("NestedLoopJoinExec"),
                "{label}: vacuous, no join in\n{text}"
            );
            assert_eq!(rows(&small, sql).await, rows(&large, sql).await, "{label}");
        }
    }

    /// Guards no SQL reaches after the engine's planner (null-aware joins
    /// with sort-merge preferred, right mark joins, a pushed limit), on
    /// hand-built joins.
    #[test]
    fn hand_built_joins_the_rule_keeps() {
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::joins::PartitionMode;
        let schema = Arc::new(schema(&[DataType::Int64]));
        let side = || Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let key = || Arc::new(Column::new("c0", 0)) as Arc<dyn PhysicalExpr>;
        let join = |join_type, null_aware, null_equality| {
            HashJoinExec::try_new(
                side(),
                side(),
                vec![(key(), key())],
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                null_equality,
                null_aware,
            )
            .unwrap()
        };
        let plain = NullEquality::NullEqualsNothing;
        assert_eq!(keeps_hash_join(&join(JoinType::LeftAnti, false, plain)), None);
        assert!(keeps_hash_join(&join(JoinType::LeftAnti, true, plain)).is_some());
        assert!(keeps_hash_join(&join(JoinType::LeftMark, false, plain)).is_some());
        assert!(keeps_hash_join(&join(JoinType::RightMark, false, plain)).is_some());
        assert!(keeps_hash_join(&join(JoinType::Inner, false, NullEquality::NullEqualsNull)).is_some());
        let computed = HashJoinExec::try_new(
            side(),
            side(),
            vec![(
                Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                    key(),
                    datafusion::logical_expr::Operator::Plus,
                    Arc::new(datafusion::physical_expr::expressions::Literal::new(
                        datafusion::common::ScalarValue::Int64(Some(1)),
                    )),
                )) as Arc<dyn PhysicalExpr>,
                key(),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            plain,
            false,
        )
        .unwrap();
        assert_eq!(keeps_hash_join(&computed), Some("key computed by an expression"));
        let limited = join(JoinType::Inner, false, plain)
            .builder()
            .with_fetch(Some(10))
            .build()
            .unwrap();
        assert!(keeps_hash_join(&limited).is_some());
    }

    /// Off by session: the planner keeps the hash join.
    #[tokio::test]
    async fn the_session_can_turn_it_off() {
        let ctx = session(1024).await;
        let sql = "SELECT count(*) FROM a JOIN b ON a.k = b.k";
        assert!(plan(&ctx, sql).await.contains("SortMergeJoinExec"));
        ctx.sql("SET codelake_join_spill.sort_merge_oversized_joins = false")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let text = plan(&ctx, sql).await;
        assert!(
            !text.contains("SortMergeJoinExec") && text.contains("HashJoinExec"),
            "{text}"
        );
    }

    /// Without the options (a session the engine did not configure) the rule
    /// does nothing.
    #[tokio::test]
    async fn without_a_pool_nothing_changes() {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_physical_optimizer_rules(crate::engine::physical_rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        for sql in SETUP {
            ctx.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let text = plan(&ctx, "SELECT count(*) FROM a JOIN b ON a.k = b.k").await;
        assert!(!text.contains("SortMergeJoinExec"), "{text}");
        let mut options = JoinSpillOptions::for_pool(1024);
        options.memory_pool_bytes = None;
        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(4)
                    .with_option_extension(options),
            )
            .with_default_features()
            .with_physical_optimizer_rules(crate::engine::physical_rules())
            .build();
        let ctx = SessionContext::new_with_state(state);
        for sql in SETUP {
            ctx.sql(sql).await.unwrap().collect().await.unwrap();
        }
        let text = plan(&ctx, "SELECT count(*) FROM a JOIN b ON a.k = b.k").await;
        assert!(!text.contains("SortMergeJoinExec"), "{text}");
    }

    // -- Under the engine's pool --------------------------------------------

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

    /// `rows` rows of a unique key and ~70 bytes of text, in memory.
    async fn keyed_table(rows: usize, fill: char) -> Arc<dyn datafusion::catalog::TableProvider> {
        let sql = format!(
            "SELECT value AS k, lpad(CAST(value AS VARCHAR), 70, '{fill}') AS s \
             FROM generate_series(1, {rows})"
        );
        let frame = SessionContext::new().sql(&sql).await.unwrap();
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame.collect().await.unwrap();
        Arc::new(datafusion::datasource::MemTable::try_new(schema, vec![batches]).unwrap())
    }

    fn options(engine: &crate::Engine, value: &str) -> crate::QueryOptions {
        let mut options = crate::QueryOptions::for_user("test");
        options.properties = engine
            .session_properties()
            .session(std::collections::BTreeMap::from([(
                crate::session_properties::SORT_MERGE_OVERSIZED_JOINS.to_owned(),
                value.to_owned(),
            )]))
            .unwrap();
        options
    }

    /// A build side of ~20 MB under a 16 MiB pool: the hash join fails with
    /// a resources error; planned as a sort-merge join, the query completes
    /// with the right numbers and the pool never holds more than its limit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_build_side_larger_than_the_pool_completes_as_a_sort_merge_join() {
        const ROWS: i64 = 200_000;
        let dir = tempfile::tempdir().unwrap();
        let limit = crate::ByteSize::mib(16);
        let engine = engine(dir.path(), limit);
        engine
            .context()
            .register_table("l", keyed_table(200_000, 'x').await)
            .unwrap();
        engine
            .context()
            .register_table("r", keyed_table(200_000, 'y').await)
            .unwrap();
        let sql = "SELECT count(*), sum(l.k), sum(length(l.s) + length(r.s)) \
                   FROM l JOIN r ON l.k = r.k";

        let off = options(&engine, "false");
        let frame = engine.sql_with(&off, sql).await.unwrap();
        let text = displayable(frame.clone().create_physical_plan().await.unwrap().as_ref())
            .indent(true)
            .to_string();
        assert!(text.contains("HashJoinExec"), "{text}");
        let error = frame.collect().await.unwrap_err().to_string();
        assert!(error.contains("Resources exhausted"), "{error}");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while engine.memory().reserved_bytes != 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(engine.memory().reserved_bytes, 0);

        engine.reset_memory_peak();
        let on = options(&engine, "true");
        let frame = engine.sql_with(&on, sql).await.unwrap();
        let text = displayable(frame.clone().create_physical_plan().await.unwrap().as_ref())
            .indent(true)
            .to_string();
        assert!(
            text.contains("SortMergeJoinExec") && !text.contains("HashJoinExec"),
            "{text}"
        );
        let batches = frame.collect().await.unwrap();
        let row: Vec<String> = batches[0]
            .columns()
            .iter()
            .map(|c| array_value_to_string(c, 0).unwrap())
            .collect();
        assert_eq!(
            row,
            [
                ROWS.to_string(),
                (ROWS * (ROWS + 1) / 2).to_string(),
                (ROWS * 140).to_string()
            ]
        );
        let peak = engine.memory_peak();
        assert!(peak > 0 && peak <= limit.as_u64(), "pool peak {peak}");
        assert_eq!(engine.memory().reserved_bytes, 0);
        // The default is on.
        let default = crate::QueryOptions::for_user("test");
        let text = displayable(
            engine
                .sql_with(&default, sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap()
                .as_ref(),
        )
        .indent(true)
        .to_string();
        assert!(text.contains("SortMergeJoinExec"), "{text}");
    }
}
