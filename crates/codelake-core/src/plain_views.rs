//! Plain strings in the operators that keep rows (ADR 0003).
//!
//! A string view (`Utf8View`, `DataFusion`'s type for `VARCHAR`; `BinaryView`)
//! points into data buffers it shares: `take`, `slice` and `concat` copy the
//! views and keep *every* buffer. Two operators of `DataFusion` 53 keep rows that
//! way and so hold far more memory than they account:
//!
//! - **A hash aggregation that spills** emits its whole state as one batch and
//!   writes it sorted, in slices taken with `take`: every slice keeps all the
//!   data buffers of the state, and the spill file records the memory of the
//!   whole state as the size of its largest batch. Merging the spill files
//!   reserves two of those "batches" per file, and at least two files:
//!   1,000,000 groups of ~210 B keys under 128 MiB spilled 800 MB, then asked
//!   for 31 MB to merge slices of 1.7 MB each, more than the operator's share,
//!   and the query failed (DEFECT-MEM6).
//! - **A window** (`BoundedWindowAggExec`) appends each input batch to what it
//!   still needs with `concat` and drops what it no longer needs with `slice`:
//!   the buffers of every batch stay until the window ends, unreserved. A
//!   `row_number()` over a sort of ~330 MiB grew the process by 358 MiB under
//!   `memory.limit = 128 MiB` (DEFECT-MEM4).
//!
//! [`PlainViews`], a physical rule, runs those operators on `LargeUtf8`
//! (`LargeBinary`) instead, whose `take` and `concat` copy only the rows they
//! keep: an aggregation groups by its view keys cast (with the partial
//! aggregation that feeds it, so that the exchange between them carries plain
//! strings), and a window reads its view columns cast by a [`RetypeExec`]
//! below it. A [`RetypeExec`] above casts
//! back and reports the operator's original properties, so the rest of the
//! plan does not change. Each cast copies the strings once.
//!
//! Left as they are: aggregations over input sorted on their keys (they never
//! spill) or with grouping sets; aggregate states that are views (`min`/`max` of a string) and
//! window columns that a function reads or that a computed `PARTITION BY` or
//! `ORDER BY` uses, whose types are part of the expressions.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr::projection::ProjectionMapping;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, InputOrderMode, PlanProperties,
};
use futures::StreamExt;

/// Name of [`PlainViews`].
pub const PLAIN_VIEWS: &str = "codelake_plain_views";

/// Physical rule: see the module documentation.
#[derive(Debug, Default)]
pub struct PlainViews;

impl PhysicalOptimizerRule for PlainViews {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        rewrite(plan, false)
    }

    fn name(&self) -> &'static str {
        PLAIN_VIEWS
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Rewrites `node` and its descendants, bottom up. `shared` says whether the
/// way `node`'s rows are spread over partitions must match another input of an
/// operator above it (a partitioned join): a hash exchange of `LargeUtf8`
/// strings spreads them unlike one of views (`DataFusion` hashes short views by
/// their 16 bytes), so an aggregation whose output is shared keeps views on its
/// exchange.
fn rewrite(node: Arc<dyn ExecutionPlan>, shared: bool) -> Result<Arc<dyn ExecutionPlan>> {
    let any = node.as_any();
    let children_shared = if node.children().len() > 1 {
        true
    } else if any.is::<RepartitionExec>()
        || any.is::<CoalescePartitionsExec>()
        || any.is::<SortPreservingMergeExec>()
    {
        // Their output is spread their own way, whatever their input's was.
        false
    } else {
        shared
    };
    let children: Vec<Arc<dyn ExecutionPlan>> = node.children().into_iter().cloned().collect();
    let mut changed = false;
    let mut rewritten = Vec::with_capacity(children.len());
    for child in children {
        let new = rewrite(Arc::clone(&child), children_shared)?;
        changed |= !Arc::ptr_eq(&new, &child);
        rewritten.push(new);
    }
    let node = if changed {
        node.with_new_children(rewritten)?
    } else {
        node
    };
    let plain = if let Some(aggregate) = node.as_any().downcast_ref::<AggregateExec>() {
        plain_keys(aggregate, shared)?
    } else {
        plain_window(&node)?
    };
    Ok(plain.unwrap_or(node))
}

/// The plain type a view is kept as, if `data_type` is a view.
fn plain(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Utf8View => Some(DataType::LargeUtf8),
        DataType::BinaryView => Some(DataType::LargeBinary),
        _ => None,
    }
}

/// Whether `new` is `old` with some views made plain: same names, same
/// nullability, and the same types or their plain ones.
fn aligned(old: &Schema, new: &Schema) -> bool {
    old.fields().len() == new.fields().len()
        && old.fields().iter().zip(new.fields().iter()).all(|(o, n)| {
            o.name() == n.name()
                && o.is_nullable() == n.is_nullable()
                && (o.data_type() == n.data_type() || plain(o.data_type()).as_ref() == Some(n.data_type()))
        })
}

/// Whether [`PlainViews`] handles `aggregate`: plain grouping (no grouping
/// sets) by some key, over input not sorted on it.
fn simple(aggregate: &AggregateExec) -> bool {
    let group_by = aggregate.group_expr();
    group_by.is_single()
        && !group_by.expr().is_empty()
        && matches!(aggregate.input_order_mode(), InputOrderMode::Linear)
}

/// `aggregate` over `input`, grouping by its keys cast to their plain types
/// where `input` has views; `None` when no key is a view.
fn cast_keys(aggregate: &AggregateExec, input: Arc<dyn ExecutionPlan>) -> Result<Option<AggregateExec>> {
    let input_schema = input.schema();
    let mut changed = false;
    let mut keys = Vec::with_capacity(aggregate.group_expr().expr().len());
    for (expr, alias) in aggregate.group_expr().expr() {
        let key: Arc<dyn PhysicalExpr> = match plain(&expr.data_type(&input_schema)?) {
            Some(target) => {
                changed = true;
                Arc::new(CastExpr::new(Arc::clone(expr), target, None))
            }
            None => Arc::clone(expr),
        };
        keys.push((key, alias.clone()));
    }
    let input_changed = !Arc::ptr_eq(&input, aggregate.input());
    if !changed && !input_changed {
        return Ok(None);
    }
    Ok(Some(
        AggregateExec::try_new(
            *aggregate.mode(),
            PhysicalGroupBy::new_single(keys),
            aggregate.aggr_expr().to_vec(),
            aggregate.filter_expr().to_vec(),
            input,
            aggregate.input_schema(),
        )?
        .with_limit_options(aggregate.limit_options()),
    ))
}

/// `input` with the partial aggregation it exchanges grouping by plain keys,
/// so that the exchange carries plain strings too (a hash repartition `take`s
/// every batch into one per partition, and each of them kept all the buffers
/// of the batch); `None` when `input` is not partial aggregation under
/// exchanges, or it has no view key.
fn plain_partial(input: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(partial) = input.as_any().downcast_ref::<AggregateExec>() {
        if *partial.mode() != AggregateMode::Partial || !simple(partial) {
            return Ok(None);
        }
        let plain = cast_keys(partial, Arc::clone(partial.input()))?;
        return Ok(plain.map(|plain| Arc::new(plain) as Arc<dyn ExecutionPlan>));
    }
    let exchange = input.as_any().is::<RepartitionExec>() || input.as_any().is::<CoalescePartitionsExec>();
    let [child] = input.children()[..] else {
        return Ok(None);
    };
    if !exchange {
        return Ok(None);
    }
    match plain_partial(child)? {
        Some(child) => Ok(Some(Arc::clone(input).with_new_children(vec![child])?)),
        None => Ok(None),
    }
}

/// `aggregate`, and the partial aggregation below it unless its output is
/// `shared` (see [`rewrite`]), grouped by plain keys, under a [`RetypeExec`]
/// back to its own schema; `None` for a partial aggregation (its final one
/// rewrites it) or when no key is a view.
fn plain_keys(aggregate: &AggregateExec, shared: bool) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if *aggregate.mode() == AggregateMode::Partial || !simple(aggregate) {
        return Ok(None);
    }
    let partial = if shared {
        None
    } else {
        plain_partial(aggregate.input())?
    };
    let input = partial.unwrap_or_else(|| Arc::clone(aggregate.input()));
    let Some(grouped) = cast_keys(aggregate, input)? else {
        return Ok(None);
    };
    if !aligned(&aggregate.schema(), &grouped.schema()) {
        return Ok(None);
    }
    Ok(Some(Arc::new(RetypeExec {
        input: Arc::new(grouped),
        properties: Arc::clone(aggregate.properties()),
    })))
}

/// The window in `node` reading its view columns plain, under a
/// [`RetypeExec`] back to its own schema; `None` when it has no view column it
/// can cast.
fn plain_window(node: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(window) = node.as_any().downcast_ref::<BoundedWindowAggExec>() else {
        return Ok(None);
    };
    let input = window.input();
    let input_schema = input.schema();
    // Columns whose type is part of an expression: function arguments, and
    // anything a computed PARTITION BY or ORDER BY reads.
    let mut typed: HashSet<usize> = HashSet::new();
    for expr in window.window_expr() {
        let computed = expr
            .partition_by()
            .iter()
            .chain(expr.order_by().iter().map(|sort| &sort.expr))
            .filter(|e| !e.as_any().is::<Column>());
        for e in expr.expressions().iter().chain(computed) {
            typed.extend(collect_columns(e).iter().map(Column::index));
        }
    }
    let mut changed = false;
    let fields: Vec<Field> = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| match plain(field.data_type()) {
            Some(target) if !typed.contains(&index) => {
                changed = true;
                field.as_ref().clone().with_data_type(target)
            }
            _ => field.as_ref().clone(),
        })
        .collect();
    if !changed {
        return Ok(None);
    }
    let schema = Arc::new(Schema::new_with_metadata(fields, input_schema.metadata().clone()));
    let below = RetypeExec::below(Arc::clone(input), schema)?;
    let plain = Arc::clone(node).with_new_children(vec![below])?;
    if !aligned(&node.schema(), &plain.schema()) {
        return Ok(None);
    }
    Ok(Some(Arc::new(RetypeExec {
        input: plain,
        properties: Arc::clone(node.properties()),
    })))
}

/// Casts the columns of its input to the types of its schema; the rows, their
/// order and their partitioning are its input's.
#[derive(Debug)]
pub struct RetypeExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl RetypeExec {
    /// `input` as `schema` (same columns, other types), with the equivalences,
    /// orderings and partitioning of `input` carried over column by column.
    fn below(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let columns = input_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().clone(),
                )
            })
            .collect::<Vec<_>>();
        let mapping = ProjectionMapping::try_new(columns, &input_schema)?;
        let equivalences = input.equivalence_properties();
        let properties = PlanProperties::new(
            equivalences.project(&mapping, schema),
            input.output_partitioning().project(&mapping, equivalences),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Ok(Arc::new(Self {
            input,
            properties: Arc::new(properties),
        }))
    }
}

impl DisplayAs for RetypeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RetypeExec")
    }
}

impl ExecutionPlan for RetypeExec {
    fn name(&self) -> &'static str {
        "RetypeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match (children.pop(), children.is_empty()) {
            (Some(input), true) => Ok(Arc::new(Self {
                input,
                properties: Arc::clone(&self.properties),
            })),
            _ => Err(DataFusionError::Internal(
                "RetypeExec has exactly one child".into(),
            )),
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let input = self.input.execute(partition, context)?;
        let target = Arc::clone(&schema);
        let stream = input.map(move |batch| retype(&batch?, &target));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn retype(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                Ok(cast(column, field.data_type())?)
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, Int64Array};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;

    fn engine(dir: &std::path::Path, limit: ByteSize, partitions: usize) -> Engine {
        let memory = MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::gib(4),
            metadata_cache_limit: ByteSize::mib(1),
        };
        let execution = ExecutionConfig {
            target_partitions: Some(partitions),
            ..ExecutionConfig::default()
        };
        Engine::new(&memory, &execution).unwrap()
    }

    async fn plan(context: &SessionContext, sql: &str) -> String {
        let plan = context
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        displayable(plan.as_ref()).indent(true).to_string()
    }

    /// The final aggregation groups by `LargeUtf8`, the partial one keeps its
    /// views, and the query's schema and rows are those of the view keys.
    #[tokio::test]
    async fn string_keys_of_a_spilling_aggregation_are_plain() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        let sql = "SELECT k, count(*) AS n, max(v) AS hi FROM (\
                     SELECT CAST(value % 3 AS VARCHAR) AS k, value AS v FROM generate_series(1, 30)) \
                   GROUP BY k ORDER BY k";
        let text = plan(engine.context(), sql).await;
        assert!(text.contains("RetypeExec"), "{text}");
        // The partial aggregation groups by plain keys, and the exchange and
        // the final aggregation read them as they are.
        assert!(
            text.contains("mode=Partial, gby=[CAST(k@0 AS LargeUtf8) as k]"),
            "{text}"
        );
        assert!(text.contains("mode=FinalPartitioned, gby=[k@0 as k]"), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Utf8View);
        let rows: Vec<(String, i64, i64)> = batches
            .iter()
            .flat_map(|b| {
                let k = b.column(0).as_string_view();
                let n = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let hi = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..b.num_rows())
                    .map(|i| (k.value(i).to_owned(), n.value(i), hi.value(i)))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("0".to_owned(), 10, 30),
                ("1".to_owned(), 10, 28),
                ("2".to_owned(), 10, 29)
            ]
        );
    }

    /// An aggregation whose partitions a partitioned join matches with its
    /// other input keeps views on its exchange (a hash of `LargeUtf8` spreads
    /// keys unlike a hash of views), and the join finds every row.
    #[tokio::test]
    async fn an_aggregation_joined_by_partition_keeps_its_exchange() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        for set in [
            "SET datafusion.optimizer.hash_join_single_partition_threshold = 0",
            "SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
        ] {
            engine.context().sql(set).await.unwrap().collect().await.unwrap();
        }
        let sql = "SELECT b.v, a.n FROM (\
                     SELECT CAST(value % 50 AS VARCHAR) AS k, count(*) AS n \
                     FROM generate_series(1, 1000) GROUP BY 1) a \
                   JOIN (SELECT CAST(value AS VARCHAR) AS k, value AS v FROM generate_series(0, 49)) b \
                   ON a.k = b.k ORDER BY b.v";
        let text = plan(engine.context(), sql).await;
        assert!(text.contains("mode=Partitioned"), "{text}");
        assert!(text.contains("RetypeExec"), "{text}");
        // Only the final aggregation groups plain keys; the partial one and
        // the exchange keep views.
        assert_eq!(text.matches("AS LargeUtf8)").count(), 1, "{text}");
        assert!(text.contains("mode=FinalPartitioned, gby=[CAST("), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let v = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let n = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            rows.extend((0..b.num_rows()).map(|i| (v.value(i), n.value(i))));
        }
        assert_eq!(rows, (0..50).map(|v| (v, 20)).collect::<Vec<_>>());
    }

    /// Non-view keys, and aggregations without keys, are left alone.
    #[tokio::test]
    async fn other_aggregations_are_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        for sql in [
            "SELECT value % 3 AS k, count(*) FROM generate_series(1, 30) GROUP BY value % 3",
            "SELECT count(*), max(CAST(value AS VARCHAR)) FROM generate_series(1, 30)",
        ] {
            let text = plan(engine.context(), sql).await;
            assert!(!text.contains("RetypeExec"), "{text}");
        }
    }

    /// NULL keys and several keys (one of them a view) group as before.
    #[tokio::test]
    async fn null_and_composite_keys_group_right() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        let sql = "SELECT k, g, count(*) AS n FROM (\
                     SELECT CASE WHEN value % 4 = 0 THEN NULL ELSE CAST(value % 2 AS VARCHAR) END AS k, \
                            value % 3 AS g FROM generate_series(1, 24)) \
                   GROUP BY k, g ORDER BY k NULLS FIRST, g";
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let k = b.column(0).as_string_view();
            let g = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let n = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                let key = (!k.is_null(i)).then(|| k.value(i).to_owned());
                rows.push((key, g.value(i), n.value(i)));
            }
        }
        // value % 4 = 0 → NULL (6 rows: 4, 8, …, 24), g = value % 3 → 2 each.
        // Odd values (12) → "1", 4 per g; even non-multiples of 4 (6) → "0", 2 per g.
        let expected: Vec<(Option<String>, i64, i64)> = [None, Some("0"), Some("1")]
            .into_iter()
            .flat_map(|k| {
                let n = match k {
                    None | Some("0") => 2,
                    _ => 4,
                };
                (0..3).map(move |g| (k.map(str::to_owned), g, n))
            })
            .collect();
        assert_eq!(rows, expected);
    }

    /// A window reads its view columns plain, and gives the rows and types of
    /// the query unchanged; a column a window function reads keeps its view.
    #[tokio::test]
    async fn a_window_reads_its_view_columns_plain() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 4);
        let sql = "SELECT s, t, row_number() OVER (PARTITION BY t ORDER BY s DESC) AS r, \
                          first_value(t) OVER (PARTITION BY t ORDER BY s DESC) AS f FROM (\
                     SELECT CAST(value AS VARCHAR) AS s, CAST(value % 2 AS VARCHAR) AS t \
                     FROM generate_series(1, 12)) ORDER BY t, r";
        let text = plan(engine.context(), sql).await;
        assert!(text.contains("RetypeExec"), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8View);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8View);
        let mut rows = Vec::new();
        for b in &batches {
            let s = b.column(0).as_string_view();
            let t = b.column(1).as_string_view();
            let r = b
                .column(2)
                .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
            let f = b.column(3).as_string_view();
            for i in 0..b.num_rows() {
                rows.push((
                    s.value(i).to_owned(),
                    t.value(i).to_owned(),
                    r.value(i),
                    f.value(i).to_owned(),
                ));
            }
        }
        // Per t, s descending as text: "8" > "6" > "4" > "2" > "12" > "10".
        let order = |t: &str, list: &[&str]| -> Vec<(String, String, i64, String)> {
            list.iter()
                .zip(1..)
                .map(|(s, r)| ((*s).to_owned(), t.to_owned(), r, t.to_owned()))
                .collect()
        };
        let mut expected = order("0", &["8", "6", "4", "2", "12", "10"]);
        expected.extend(order("1", &["9", "7", "5", "3", "11", "1"]));
        assert_eq!(rows, expected);
    }

    /// Only the columns no expression types are cast below the window.
    #[tokio::test]
    async fn a_window_keeps_the_views_its_functions_read() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(64), 1);
        let sql = "SELECT lag(s) OVER (ORDER BY upper(s)) AS l FROM (\
                     SELECT CAST(value AS VARCHAR) AS s FROM generate_series(1, 3))";
        let text = plan(engine.context(), sql).await;
        assert!(!text.contains("RetypeExec"), "{text}");
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let l = batches[0].column(0).as_string_view();
        assert!(l.is_null(0));
        assert_eq!((l.value(1), l.value(2)), ("1", "2"));
    }

    /// 1,000,000 groups of ~210 B keys, each seen twice, under 128 MiB with 4
    /// partitions: the aggregation spills and merges its spill files within
    /// its share (DEFECT-MEM6 failed merging them), and counts right.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_high_cardinality_aggregation_spills_and_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(128), 4);
        let sql = "SELECT count(*) AS groups, sum(c) AS n, min(c) AS lo, max(c) AS hi FROM (\
                     SELECT p, count(*) AS c FROM (\
                       SELECT repeat('x', 200) || CAST(value % 1000000 AS VARCHAR) AS p \
                       FROM generate_series(1, 2000000)) GROUP BY p)";
        let batches = engine.sql(sql).await.unwrap().collect().await.unwrap();
        let value = |c: usize| {
            batches[0]
                .column(c)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(
            (value(0), value(1), value(2), value(3)),
            (1_000_000, 2_000_000, 2, 2)
        );
        assert_eq!(engine.memory().reserved_bytes, 0);
    }
}
