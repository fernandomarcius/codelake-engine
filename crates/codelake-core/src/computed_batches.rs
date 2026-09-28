//! Batches of computed values sized in bytes, not only in rows (ADR 0003).
//!
//! `DataFusion` sizes a batch in rows (`execution.batch_size`), so a projection
//! that computes wide values produces batches of `batch_size` × their width:
//! `lpad('', 4096, 'x')` over 8192 rows is a 32 MiB batch, allocated whole before
//! any consumer can reserve it. One such batch per running worker is outside
//! the pool; 12 Trino clients of that query grew a server with
//! `memory.limit = 128 MiB` by 187-213 MiB (DEFECT-MEM8, MEM9).
//!
//! [`ComputedBatches`], a physical rule, puts every projection that computes a
//! variable-width value (a string, binary, list, map, struct) between a
//! [`SliceExec`] below and a [`SizedBatchesExec`] above. The node above measures
//! the bytes per row of each batch the projection produces, and sets how many
//! rows the node below hands it next so that a batch is about
//! [`TARGET_BATCH_BYTES`]: it starts at [`INITIAL_ROWS`], grows at most
//! fourfold per batch and never passes `batch_size`. Narrow rows reach
//! `batch_size` after a few batches; 4 KiB rows run in batches of 512.
//!
//! A slice shares the buffers of the batch it was cut from, and consumers
//! reserve a batch by the memory of its buffers: a column passed through (or a
//! string view into the input's strings) would charge the whole input to every
//! slice. A column of a produced batch that holds more than twice the bytes of
//! its own rows is copied to them first.
//!
//! The rows, their order and the partitioning are the projection's; only the
//! batch boundaries change.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, ready};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::{Stream, StreamExt};

/// Name of [`ComputedBatches`].
pub const COMPUTED_BATCHES: &str = "codelake_computed_batches";

/// Bytes a batch of computed values aims at.
pub const TARGET_BATCH_BYTES: usize = 2 << 20;

/// Rows of the first batch of a partition, before any width is known.
pub const INITIAL_ROWS: usize = 64;

/// Slack under which a column is not copied to its own rows.
const COMPACT_SLACK: usize = 64 << 10;

/// Physical rule: see the module documentation.
#[derive(Debug, Default)]
pub struct ComputedBatches;

impl PhysicalOptimizerRule for ComputedBatches {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(projection) = node.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            let input = projection.input();
            // Already sized (a view's plan is optimized again inside the outer one).
            if input.is::<SliceExec>() || !computes_wide_values(projection)? {
                return Ok(Transformed::no(node));
            }
            let sizing = Arc::new(Sizing::new(input.output_partitioning().partition_count()));
            let slice: Arc<dyn ExecutionPlan> =
                Arc::new(SliceExec::new(Arc::clone(input), Arc::clone(&sizing)));
            let projection = Arc::clone(&node).with_new_children(vec![slice])?;
            Ok(Transformed::yes(Arc::new(SizedBatchesExec::new(
                projection, sizing,
            ))))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        COMPUTED_BATCHES
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether `projection` computes (not just passes through) a value whose width
/// varies per row.
fn computes_wide_values(projection: &ProjectionExec) -> Result<bool> {
    let input_schema = projection.input().schema();
    for item in projection.expr() {
        if item.expr.is::<Column>() {
            continue;
        }
        if variable_width(&item.expr.data_type(&input_schema)?) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn variable_width(data_type: &DataType) -> bool {
    !(data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Null | DataType::Boolean | DataType::FixedSizeBinary(_)
        ))
}

/// Rows per slice of each partition, shared by a [`SliceExec`] and the
/// [`SizedBatchesExec`] above it. Two runs of the same plan at once share it
/// too; that only changes their batch boundaries.
#[derive(Debug)]
pub struct Sizing {
    rows: Vec<AtomicUsize>,
}

impl Sizing {
    fn new(partitions: usize) -> Self {
        Self {
            rows: (0..partitions.max(1))
                .map(|_| AtomicUsize::new(INITIAL_ROWS))
                .collect(),
        }
    }

    fn rows(&self, partition: usize) -> usize {
        self.rows
            .get(partition)
            .map_or(INITIAL_ROWS, |rows| rows.load(Ordering::Relaxed))
            .max(1)
    }

    fn set(&self, partition: usize, rows: usize) {
        if let Some(slot) = self.rows.get(partition) {
            slot.store(rows.max(1), Ordering::Relaxed);
        }
    }

    /// Rows of the next slice after `batch`: what makes [`TARGET_BATCH_BYTES`]
    /// at its bytes per row, at most four times the current count and at most
    /// `batch_size`.
    fn observe(&self, partition: usize, batch: &RecordBatch, batch_size: usize) {
        let rows = batch.num_rows();
        if rows == 0 {
            return;
        }
        let bytes: usize = batch.columns().iter().map(|c| own_bytes(c.as_ref())).sum();
        let per_row = bytes.div_ceil(rows).max(1);
        let wanted = (TARGET_BATCH_BYTES / per_row).max(1);
        let next = wanted
            .min(self.rows(partition).saturating_mul(4))
            .min(batch_size.max(1));
        self.set(partition, next);
    }
}

/// Bytes the rows of `array` need on their own (what a copy of just them
/// would hold), unlike `get_array_memory_size`, which counts every buffer the
/// array shares with the batch it was cut from.
fn own_bytes(array: &dyn Array) -> usize {
    let nulls = array.nulls().map_or(0, |n| n.len().div_ceil(8));
    match array.data_type() {
        DataType::Utf8View | DataType::BinaryView => {
            let lengths: usize = match array.data_type() {
                DataType::Utf8View => array
                    .as_string_view()
                    .lengths()
                    .map(|l| l as usize)
                    .filter(|l| *l > 12)
                    .sum(),
                _ => array
                    .as_binary_view()
                    .lengths()
                    .map(|l| l as usize)
                    .filter(|l| *l > 12)
                    .sum(),
            };
            array.len() * 16 + lengths + nulls
        }
        _ => array
            .to_data()
            .get_slice_memory_size()
            .unwrap_or_else(|_| array.get_array_memory_size()),
    }
}

/// `batch` with every variable-width column that holds more than twice the
/// bytes of its rows copied to them.
fn compact(batch: RecordBatch) -> Result<RecordBatch> {
    let oversized = |column: &ArrayRef| {
        variable_width(column.data_type())
            && column.get_array_memory_size() > own_bytes(column.as_ref()).saturating_mul(2) + COMPACT_SLACK
    };
    if !batch.columns().iter().any(oversized) {
        return Ok(batch);
    }
    let rows = batch.num_rows();
    let indices = UInt32Array::from_iter_values(
        0..u32::try_from(rows).map_err(|e| DataFusionError::External(Box::new(e)))?,
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            if !oversized(column) {
                return Ok(Arc::clone(column));
            }
            Ok(match column.data_type() {
                DataType::Utf8View => Arc::new(column.as_string_view().gc()) as ArrayRef,
                DataType::BinaryView => Arc::new(column.as_binary_view().gc()) as ArrayRef,
                _ => take(column.as_ref(), &indices, None)?,
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Hands its input's batches on in slices of the rows [`Sizing`] sets.
#[derive(Debug)]
pub struct SliceExec {
    input: Arc<dyn ExecutionPlan>,
    sizing: Arc<Sizing>,
    properties: Arc<PlanProperties>,
}

impl SliceExec {
    fn new(input: Arc<dyn ExecutionPlan>, sizing: Arc<Sizing>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            sizing,
            properties,
        }
    }
}

impl DisplayAs for SliceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SliceExec")
    }
}

impl ExecutionPlan for SliceExec {
    fn name(&self) -> &'static str {
        "SliceExec"
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
            (Some(input), true) => Ok(Arc::new(Self::new(input, Arc::clone(&self.sizing)))),
            _ => Err(DataFusionError::Internal(
                "SliceExec has exactly one child".into(),
            )),
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(SliceStream {
            input: self.input.execute(partition, context)?,
            sizing: Arc::clone(&self.sizing),
            partition,
            current: None,
        }))
    }
}

struct SliceStream {
    input: SendableRecordBatchStream,
    sizing: Arc<Sizing>,
    partition: usize,
    /// The input batch being sliced, and the offset of its next slice.
    current: Option<(RecordBatch, usize)>,
}

impl Stream for SliceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some((batch, offset)) = self.current.take() {
                let total = batch.num_rows();
                if offset < total {
                    let len = self.sizing.rows(self.partition).min(total - offset);
                    let slice = if offset == 0 && len == total {
                        batch.clone()
                    } else {
                        batch.slice(offset, len)
                    };
                    if offset + len < total {
                        self.current = Some((batch, offset + len));
                    }
                    return Poll::Ready(Some(Ok(slice)));
                }
            }
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) if batch.num_rows() > 0 => self.current = Some((batch, 0)),
                other => return Poll::Ready(other),
            }
        }
    }
}

impl RecordBatchStream for SliceStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Above a projection whose input is a [`SliceExec`]: compacts what the
/// projection produces and sizes its next slice.
#[derive(Debug)]
pub struct SizedBatchesExec {
    input: Arc<dyn ExecutionPlan>,
    sizing: Arc<Sizing>,
    properties: Arc<PlanProperties>,
}

impl SizedBatchesExec {
    fn new(input: Arc<dyn ExecutionPlan>, sizing: Arc<Sizing>) -> Self {
        let properties = Arc::clone(input.properties());
        Self {
            input,
            sizing,
            properties,
        }
    }
}

impl DisplayAs for SizedBatchesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SizedBatchesExec: target_bytes={TARGET_BATCH_BYTES}")
    }
}

impl ExecutionPlan for SizedBatchesExec {
    fn name(&self) -> &'static str {
        "SizedBatchesExec"
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
            (Some(input), true) => Ok(Arc::new(Self::new(input, Arc::clone(&self.sizing)))),
            _ => Err(DataFusionError::Internal(
                "SizedBatchesExec has exactly one child".into(),
            )),
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        self.sizing.set(partition, INITIAL_ROWS.min(batch_size));
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SizedStream {
            input,
            sizing: Arc::clone(&self.sizing),
            partition,
            batch_size,
        }))
    }
}

struct SizedStream {
    input: SendableRecordBatchStream,
    sizing: Arc<Sizing>,
    partition: usize,
    batch_size: usize,
}

impl Stream for SizedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = ready!(self.input.poll_next_unpin(cx));
        Poll::Ready(item.map(|batch| {
            let batch = compact(batch?)?;
            self.sizing.observe(self.partition, &batch, self.batch_size);
            Ok(batch)
        }))
    }
}

impl RecordBatchStream for SizedStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

/// Rebuilds of [`SliceExec`] and [`SizedBatchesExec`] for plans that cross a
/// process boundary (`crate::portable`). The pair shares one [`Sizing`]; they
/// are always in the same stage (no exchange between a projection and its
/// input), so the node above finds the slice below it and shares its sizing.
#[cfg(feature = "portable")]
pub(crate) mod portable {
    use std::sync::Arc;

    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

    use super::{SizedBatchesExec, Sizing, SliceExec};

    pub(crate) fn slice(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let sizing = Arc::new(Sizing::new(input.output_partitioning().partition_count()));
        Arc::new(SliceExec::new(input, sizing))
    }

    pub(crate) fn sized(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let mut below = Some(Arc::clone(&input));
        let mut sizing = None;
        while let Some(plan) = below {
            if let Some(slice) = plan.downcast_ref::<SliceExec>() {
                sizing = Some(Arc::clone(&slice.sizing));
                break;
            }
            below = match plan.children().as_slice() {
                [only] => Some(Arc::clone(only)),
                _ => None,
            };
        }
        let sizing =
            sizing.unwrap_or_else(|| Arc::new(Sizing::new(input.output_partitioning().partition_count())));
        Arc::new(SizedBatchesExec::new(input, sizing))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, StringViewArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::displayable;

    use super::*;
    use crate::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;

    fn engine(dir: &std::path::Path, partitions: usize) -> Engine {
        let memory = MemoryConfig {
            limit: ByteSize::mib(256),
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::gib(1),
            metadata_cache_limit: ByteSize::mib(1),
        };
        let execution = ExecutionConfig {
            target_partitions: Some(partitions),
            ..ExecutionConfig::default()
        };
        Engine::new(&memory, &execution).unwrap()
    }

    async fn plan(engine: &Engine, sql: &str) -> String {
        let plan = engine
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn batch_bytes(batch: &RecordBatch) -> usize {
        batch.columns().iter().map(|c| own_bytes(c.as_ref())).sum()
    }

    /// 4 KiB computed rows come in batches of about 2 MiB, starting at 64 rows,
    /// with every row, in order.
    #[tokio::test]
    async fn wide_computed_values_come_in_batches_of_about_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), 1);
        let sql = "SELECT value, lpad('', 4096, 'x') AS s FROM generate_series(1, 20000)";
        let text = plan(&engine, sql).await;
        assert!(text.contains("SizedBatchesExec"), "{text}");
        assert!(text.contains("SliceExec"), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), INITIAL_ROWS);
        let mut next = 1;
        for batch in &batches {
            // 512 rows of 4 KiB and their offsets and values.
            assert!(
                batch_bytes(batch) <= TARGET_BATCH_BYTES + (64 << 10),
                "{}",
                batch_bytes(batch)
            );
            assert!(
                batch.get_array_memory_size() <= 3 * TARGET_BATCH_BYTES,
                "{}",
                batch.get_array_memory_size()
            );
            let v = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(v.value(row), next);
                next += 1;
            }
        }
        assert_eq!(next, 20_001);
        for batch in &batches {
            let s = cast(batch.column(1), &DataType::Utf8).unwrap();
            let s = s.as_string::<i32>();
            assert!((0..batch.num_rows()).all(|row| s.value(row) == "x".repeat(4096)));
        }
        // 2 MiB / ~4 KiB per row: never more than 512 rows after the first ones.
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(sizes[1], 4 * INITIAL_ROWS, "{sizes:?}");
        assert!(sizes.iter().all(|n| *n <= 512), "{sizes:?}");
        assert!(sizes.len() >= 20_000 / 512, "{sizes:?}");
    }

    /// Narrow computed values grow to `batch_size` rows.
    #[tokio::test]
    async fn narrow_computed_values_reach_the_batch_size() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), 1);
        let sql = "SELECT value, CAST(value AS VARCHAR) AS s FROM generate_series(1, 100000)";
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(sizes[..5], [64, 256, 1024, 4096, 2752], "{sizes:?}");
        assert!(sizes[5..sizes.len() - 1].iter().all(|n| *n == 8192), "{sizes:?}");
        assert_eq!(sizes.iter().sum::<usize>(), 100_000);
        let mut next: i64 = 1;
        for batch in &batches {
            let v = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = batch.column(1).as_string_view();
            for row in 0..batch.num_rows() {
                assert_eq!(v.value(row), next);
                assert_eq!(s.value(row), next.to_string());
                next += 1;
            }
        }
    }

    /// A projection of columns and fixed-width values is left as it is.
    #[tokio::test]
    async fn other_projections_are_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), 2);
        for sql in [
            "SELECT value + 1 AS v, value > 3 AS b FROM generate_series(1, 10)",
            "SELECT v FROM (SELECT value AS v, value * 2 AS w FROM generate_series(1, 10))",
        ] {
            let text = plan(&engine, sql).await;
            assert!(!text.contains("SliceExec"), "{text}");
        }
    }

    /// A column passed through from a wide input batch is copied to the rows of
    /// each slice, so no batch charges the whole input: 8192 rows of ~1 KiB
    /// string views (8 MiB) pass through a projection that computes a string.
    #[tokio::test]
    async fn columns_passed_through_hold_only_their_rows() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), 1);
        let rows: i64 = 8192;
        let text = |v: i64| format!("{v:0>1000}");
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, false),
        ]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(1..=rows)),
                Arc::new(StringViewArray::from_iter_values((1..=rows).map(text))),
            ],
        )
        .unwrap();
        let whole = input.get_array_memory_size();
        engine.context().register_batch("t", input).unwrap();
        let sql = "SELECT v, s, upper(s) AS u FROM t";
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(batches.len() > 4, "{}", batches.len());
        let mut next = 1;
        for batch in &batches {
            assert!(
                batch.get_array_memory_size() < whole / 2,
                "a batch of {} rows holds {} of the {whole} input bytes",
                batch.num_rows(),
                batch.get_array_memory_size()
            );
            let v = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = cast(batch.column(1), &DataType::Utf8).unwrap();
            let s = s.as_string::<i32>();
            let u = cast(batch.column(2), &DataType::Utf8).unwrap();
            let u = u.as_string::<i32>();
            for row in 0..batch.num_rows() {
                assert_eq!(v.value(row), next);
                assert_eq!(s.value(row), text(next));
                assert_eq!(u.value(row), text(next));
                next += 1;
            }
        }
        assert_eq!(next, rows + 1);
    }

    /// Several partitions: every row once, with its own value.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partitioned_wide_projection_keeps_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), 4);
        let sql = "SELECT value, lpad('', 3000, 'y') || CAST(value AS VARCHAR) AS s \
                   FROM generate_series(1, 50000)";
        let text = plan(&engine, sql).await;
        assert!(text.contains("SizedBatchesExec"), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let mut seen = Vec::new();
        for batch in &batches {
            let v = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = cast(batch.column(1), &DataType::Utf8).unwrap();
            let s = s.as_string::<i32>();
            for row in 0..batch.num_rows() {
                assert_eq!(s.value(row), format!("{}{}", "y".repeat(3000), v.value(row)));
                seen.push(v.value(row));
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, (1..=50_000).collect::<Vec<i64>>());
    }
}
