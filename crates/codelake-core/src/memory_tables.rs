//! Tables of `DataFusion`'s in-memory catalogs, kept inside the memory pool.
//!
//! `CREATE TABLE` in an in-memory catalog keeps every row in server memory
//! after the statement. `DataFusion` holds them outside the pool: any client
//! could fill the server past its ceiling (ADR 0003). [`PooledTable`] holds a
//! reservation of what its rows take, for as long as the table exists:
//! `CREATE TABLE … AS` reserves each batch as it arrives and fails, with
//! nothing registered, once the pool cannot hold more; `INSERT` reserves
//! what it appends and gives it back if it fails. `DELETE` and `UPDATE` run
//! when their plan executes (never while it is planned or explained), over
//! the whole table at once: `UPDATE` reserves the values it computes before
//! the table changes, so one the pool cannot hold fails with the rows as
//! they were; then the reservation is resized to what the table holds.
//! Dropping or replacing the table frees it. The conditions `DELETE` and
//! `UPDATE` receive are the whole `WHERE`: a statement whose rows they would
//! not select (a subquery, a join) is refused before it reaches the table
//! (`crate::dml_guard`).

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::compute::{and, filter_record_batch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, DFSchema};
use datafusion::datasource::memory::PartitionData;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;

/// A reservation several statements grow and shrink.
type Shared = Arc<Mutex<MemoryReservation>>;

fn lock(reservation: &Shared) -> MutexGuard<'_, MemoryReservation> {
    reservation.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A new reservation for the rows of table `name`.
pub(crate) fn reservation(name: &str, context: &TaskContext) -> Shared {
    Arc::new(Mutex::new(
        MemoryConsumer::new(format!("MemoryTable[{name}]")).register(context.memory_pool()),
    ))
}

/// Runs `plan` and keeps its rows, by partition, each batch reserved on
/// `reservation` as it arrives.
pub(crate) async fn collect_reserved(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    reservation: &Shared,
) -> Result<Vec<Vec<RecordBatch>>> {
    let partitions = plan.output_partitioning().partition_count();
    let reads = (0..partitions).map(|partition| {
        let stream = plan.execute(partition, Arc::clone(&context));
        async move {
            let mut stream = stream?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                lock(reservation).try_grow(batch.get_array_memory_size())?;
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(batches)
        }
    });
    futures::future::try_join_all(reads).await
}

/// A [`MemTable`] whose rows are reserved in the memory pool.
#[derive(Debug)]
pub(crate) struct PooledTable {
    inner: MemTable,
    reservation: Shared,
}

impl PooledTable {
    /// `inner`, whose rows `reservation` already holds.
    pub(crate) fn new(inner: MemTable, reservation: Shared) -> Self {
        Self { inner, reservation }
    }

    /// A plan that runs `change` over the rows `filters` select (every row
    /// without filters) when it executes, and answers how many it changed.
    fn rewrite(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
        change: Change,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(self.inner.schema())?;
        let filters = filters
            .into_iter()
            .map(|filter| state.create_physical_expr(filter, &df_schema))
            .collect::<Result<_>>()?;
        let sink = RewriteSink {
            partitions: self.inner.batches.clone(),
            schema: self.inner.schema(),
            reservation: Arc::clone(&self.reservation),
            filters,
            change,
        };
        let input = Arc::new(EmptyExec::new(self.inner.schema()));
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }
}

#[async_trait]
impl TableProvider for PooledTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The table's own checks (schema, append only); its plan is replaced
        // by one that reserves what it appends.
        self.inner
            .insert_into(state, Arc::clone(&input), insert_op)
            .await?;
        if self.inner.batches.is_empty() {
            return Err(DataFusionError::Plan(
                "Cannot insert into MemTable with zero partitions".into(),
            ));
        }
        let sink = PooledSink {
            partitions: self.inner.batches.clone(),
            schema: self.inner.schema(),
            reservation: Arc::clone(&self.reservation),
        };
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    async fn delete_from(&self, state: &dyn Session, filters: Vec<Expr>) -> Result<Arc<dyn ExecutionPlan>> {
        self.rewrite(state, filters, Change::Delete)
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = self.inner.schema();
        let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
        let mut set = Vec::with_capacity(assignments.len());
        for (name, expr) in assignments {
            let Ok(index) = schema.index_of(&name) else {
                let available: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
                return Err(DataFusionError::Plan(format!(
                    "UPDATE failed: column '{name}' does not exist. Available columns: {}",
                    available.join(", ")
                )));
            };
            set.push((index, state.create_physical_expr(expr, &df_schema)?));
        }
        self.rewrite(state, filters, Change::Update(set))
    }
}

/// What a [`RewriteSink`] does to the rows its filters select.
#[derive(Debug)]
enum Change {
    Delete,
    /// New values of these columns (by index).
    Update(Vec<(usize, Arc<dyn PhysicalExpr>)>),
}

/// Rows of `batch` every filter holds for (NULL does not hold); `None`
/// without filters (every row).
fn selected(filters: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> Result<Option<BooleanArray>> {
    let mut mask: Option<BooleanArray> = None;
    for filter in filters {
        let values = filter.evaluate(batch)?.into_array(batch.num_rows())?;
        let values = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| DataFusionError::Internal("a filter did not evaluate to boolean".into()))?
            .clone();
        mask = Some(match mask {
            Some(mask) => and(&mask, &values)?,
            None => values,
        });
    }
    // Only TRUE selects a row.
    Ok(mask.map(|mask| mask.iter().map(|v| Some(v == Some(true))).collect()))
}

/// Runs a `DELETE` or `UPDATE` of a [`PooledTable`] over every partition at
/// once, holding them all: the table changes only if the whole statement
/// succeeds (see the module documentation).
struct RewriteSink {
    partitions: Vec<PartitionData>,
    schema: SchemaRef,
    reservation: Shared,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    change: Change,
}

impl fmt::Debug for RewriteSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RewriteSink")
            .field("partitions", &self.partitions.len())
            .field("filters", &self.filters)
            .field("change", &self.change)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RewriteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.change {
            Change::Delete => "DELETE",
            Change::Update(_) => "UPDATE",
        };
        write!(f, "MemoryTable {what} (partitions={})", self.partitions.len())
    }
}

impl RewriteSink {
    /// The new rows of `batch` and how many rows changed; values an `UPDATE`
    /// computes are reserved on `grown` first.
    fn rewrite(&self, batch: &RecordBatch, grown: &mut usize) -> Result<(RecordBatch, u64)> {
        let mask = selected(&self.filters, batch)?;
        let selected = mask.as_ref().map_or(batch.num_rows(), BooleanArray::true_count);
        let changed = u64::try_from(selected).unwrap_or(u64::MAX);
        if selected == 0 {
            return Ok((batch.clone(), 0));
        }
        match &self.change {
            Change::Delete => {
                let keep = match mask {
                    Some(mask) => datafusion::arrow::compute::not(&mask)?,
                    None => BooleanArray::from(vec![false; batch.num_rows()]),
                };
                Ok((filter_record_batch(batch, &keep)?, changed))
            }
            Change::Update(set) => {
                let mask = mask.unwrap_or_else(|| BooleanArray::from(vec![true; batch.num_rows()]));
                let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
                for (index, expr) in set {
                    // Only the selected rows are evaluated (no error from a row
                    // left as it is).
                    let values = expr
                        .evaluate_selection(batch, &mask)?
                        .into_array(batch.num_rows())?;
                    let original: &dyn Array = batch.column(*index).as_ref();
                    let new: &dyn Array = values.as_ref();
                    let column = zip(&mask, &new, &original)?;
                    let bytes = column.get_array_memory_size();
                    lock(&self.reservation).try_grow(bytes)?;
                    *grown += bytes;
                    columns[*index] = column;
                }
                Ok((RecordBatch::try_new(Arc::clone(&self.schema), columns)?, changed))
            }
        }
    }
}

#[async_trait]
impl DataSink for RewriteSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(&self, _data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> Result<u64> {
        // Every partition, in order (as `INSERT` takes them): no statement
        // sees the table half changed.
        let mut held = Vec::with_capacity(self.partitions.len());
        for partition in &self.partitions {
            held.push(partition.write().await);
        }
        let mut grown = 0;
        let mut changed = 0_u64;
        let mut rewritten = Vec::with_capacity(held.len());
        let computed = (|| {
            for partition in &held {
                let mut batches = Vec::with_capacity(partition.len());
                for batch in partition.iter().filter(|batch| batch.num_rows() > 0) {
                    let (batch, count) = self.rewrite(batch, &mut grown)?;
                    changed += count;
                    if batch.num_rows() > 0 {
                        batches.push(batch);
                    }
                }
                rewritten.push(batches);
            }
            Ok::<_, DataFusionError>(())
        })();
        if let Err(err) = computed {
            lock(&self.reservation).shrink(grown);
            return Err(err);
        }
        let mut bytes = 0;
        for (partition, batches) in held.iter_mut().zip(rewritten) {
            bytes += batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
            **partition = batches;
        }
        // At most what was reserved: the old rows and the new values.
        lock(&self.reservation).resize(bytes);
        Ok(changed)
    }
}

/// Appends to a [`MemTable`]'s partitions, round robin, as `DataFusion`'s
/// own sink does, reserving each batch; nothing is appended (and nothing
/// stays reserved) unless every batch is.
struct PooledSink {
    partitions: Vec<PartitionData>,
    schema: SchemaRef,
    reservation: Shared,
}

impl fmt::Debug for PooledSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledSink")
            .field("partitions", &self.partitions.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for PooledSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryTable (partitions={})", self.partitions.len())
    }
}

#[async_trait]
impl DataSink for PooledSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let count = self.partitions.len().max(1);
        let mut appended = vec![Vec::new(); count];
        let mut reserved = 0;
        let mut rows = 0_u64;
        let mut next = 0;
        let read = async {
            while let Some(batch) = data.next().await {
                let batch = batch?;
                let bytes = batch.get_array_memory_size();
                lock(&self.reservation).try_grow(bytes)?;
                reserved += bytes;
                rows += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
                appended[next].push(batch);
                next = (next + 1) % count;
            }
            Ok::<_, DataFusionError>(())
        }
        .await;
        if let Err(err) = read {
            lock(&self.reservation).shrink(reserved);
            return Err(err);
        }
        for (partition, mut batches) in self.partitions.iter().zip(appended) {
            partition.write().await.append(&mut batches);
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::ByteSize;
    use crate::config::{ExecutionConfig, MemoryConfig};
    use crate::engine::Engine;

    fn engine(dir: &std::path::Path, limit: ByteSize) -> Engine {
        let memory = MemoryConfig {
            limit,
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(64),
            metadata_cache_limit: ByteSize::mib(1),
        };
        Engine::new(&memory, &ExecutionConfig::default()).unwrap()
    }

    async fn run(engine: &Engine, sql: &str) -> Result<String, String> {
        let frame = engine.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = frame.collect().await.map_err(|e| e.to_string())?;
        Ok(datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .map_err(|e| e.to_string())?
            .to_string())
    }

    fn reserved(engine: &Engine) -> u64 {
        engine.memory().reserved_bytes
    }

    const WIDE: &str = "SELECT CAST(value AS BIGINT) AS id, array_join(repeat('x', 1000), '') AS s \
                        FROM generate_series(1, {rows})";

    #[tokio::test]
    async fn tables_hold_their_rows_inside_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(48));
        let big = WIDE.replace("{rows}", "120000");
        let err = run(&engine, &format!("CREATE TABLE datafusion.public.big AS {big}"))
            .await
            .unwrap_err();
        assert!(err.contains("Resources exhausted"), "{err}");
        assert!(
            run(&engine, "SELECT count(*) FROM datafusion.public.big")
                .await
                .is_err()
        );
        assert_eq!(reserved(&engine), 0, "a failed CREATE keeps nothing");

        let small = WIDE.replace("{rows}", "4000");
        run(
            &engine,
            &format!("CREATE TABLE datafusion.public.small AS {small}"),
        )
        .await
        .unwrap();
        let held = reserved(&engine);
        assert!(held >= 4_000_000, "the rows stay reserved: {held}");
        let err = run(&engine, &format!("INSERT INTO datafusion.public.small {big}"))
            .await
            .unwrap_err();
        assert!(err.contains("Resources exhausted"), "{err}");
        assert_eq!(
            reserved(&engine),
            held,
            "a failed INSERT gives back what it reserved"
        );
        run(&engine, &format!("INSERT INTO datafusion.public.small {small}"))
            .await
            .unwrap();
        assert!(reserved(&engine) >= 2 * held - held / 10);
        run(&engine, "DELETE FROM datafusion.public.small WHERE id > 0")
            .await
            .unwrap();
        assert!(reserved(&engine) < held / 10, "{}", reserved(&engine));
        run(&engine, "DROP TABLE datafusion.public.small").await.unwrap();
        assert_eq!(reserved(&engine), 0);
    }

    #[tokio::test]
    async fn an_update_the_pool_cannot_hold_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(48));
        let rows = WIDE.replace("{rows}", "4000");
        run(&engine, &format!("CREATE TABLE datafusion.public.t AS {rows}"))
            .await
            .unwrap();
        let sizes = "SELECT count(*), sum(length(s)) FROM datafusion.public.t";
        let mut answers = Vec::new();
        for _ in 0..5 {
            let before = (reserved(&engine), run(&engine, sizes).await.unwrap());
            let updated = run(&engine, "UPDATE datafusion.public.t SET s = concat(s, s)").await;
            let after = (reserved(&engine), run(&engine, sizes).await.unwrap());
            assert!(
                after.0 <= ByteSize::mib(48).as_u64(),
                "{} bytes reserved",
                after.0
            );
            match &updated {
                Ok(_) => assert_ne!(after, before, "an UPDATE that answered changed the rows"),
                Err(err) => {
                    assert!(err.contains("Resources exhausted"), "{err}");
                    assert_eq!(after, before, "an UPDATE that failed kept its rows");
                }
            }
            answers.push(updated.is_ok());
        }
        assert!(answers.contains(&true) && answers.contains(&false), "{answers:?}");
    }

    #[tokio::test]
    async fn delete_and_update_change_rows_only_when_they_run() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(48));
        run(
            &engine,
            "CREATE TABLE datafusion.public.t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS v(id, s)",
        )
        .await
        .unwrap();
        let all = "SELECT id, s FROM datafusion.public.t ORDER BY id";
        let before = run(&engine, all).await.unwrap();
        run(&engine, "EXPLAIN UPDATE datafusion.public.t SET s = 'z'")
            .await
            .unwrap();
        run(&engine, "EXPLAIN DELETE FROM datafusion.public.t")
            .await
            .unwrap();
        assert_eq!(
            run(&engine, all).await.unwrap(),
            before,
            "EXPLAIN changed the rows"
        );

        let updated = run(&engine, "UPDATE datafusion.public.t SET s = 'z' WHERE s <> 'a'")
            .await
            .unwrap();
        assert!(updated.contains("| 1     |"), "{updated}");
        let deleted = run(&engine, "DELETE FROM datafusion.public.t WHERE id = 1")
            .await
            .unwrap();
        assert!(deleted.contains("| 1     |"), "{deleted}");
        assert_eq!(
            run(&engine, all).await.unwrap(),
            "+----+---+\n| id | s |\n+----+---+\n| 2  | z |\n| 3  |   |\n+----+---+"
        );
        let err = run(&engine, "UPDATE datafusion.public.t SET nope = 1")
            .await
            .unwrap_err();
        assert!(err.contains("No field named nope"), "{err}");
    }

    /// `t(id, s)` holding `(1, a), (2, b), (3, c)` and `u(id, s)` holding
    /// `(2, B)`, created again.
    async fn fresh(engine: &Engine) {
        for sql in [
            "DROP TABLE IF EXISTS datafusion.public.t",
            "DROP TABLE IF EXISTS datafusion.public.u",
            "CREATE TABLE datafusion.public.t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS v(id, s)",
            "CREATE TABLE datafusion.public.u AS SELECT * FROM (VALUES (2, 'B')) AS v(id, s)",
        ] {
            run(engine, sql).await.unwrap();
        }
    }

    fn rows(rows: &[(i32, &str)]) -> String {
        use std::fmt::Write as _;
        let mut table = String::from("+----+---+\n| id | s |\n+----+---+\n");
        for (id, s) in rows {
            writeln!(table, "| {id}  | {s} |").unwrap();
        }
        table.push_str("+----+---+");
        table
    }

    fn count(n: u64) -> String {
        format!("+-------+\n| count |\n+-------+\n| {n:<5} |\n+-------+")
    }

    #[tokio::test]
    async fn update_and_delete_change_exactly_the_rows_their_where_selects() {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path(), ByteSize::mib(48));
        let all = "SELECT id, s FROM datafusion.public.t ORDER BY id";
        let before = rows(&[(1, "a"), (2, "b"), (3, "c")]);
        // A subquery or a join the table would not see: refused, nothing changed.
        for sql in [
            "UPDATE datafusion.public.t SET s = 'z' WHERE id IN (SELECT 2)",
            "DELETE FROM datafusion.public.t WHERE id IN (SELECT 2)",
            "UPDATE datafusion.public.t SET s = 'z' WHERE id NOT IN (SELECT id FROM datafusion.public.u)",
            "DELETE FROM datafusion.public.t WHERE EXISTS (SELECT 1 FROM datafusion.public.u WHERE u.id = t.id)",
            "DELETE FROM datafusion.public.t WHERE id = 2 OR id IN (SELECT 3)",
            "UPDATE datafusion.public.t SET s = (SELECT u.s FROM datafusion.public.u WHERE u.id = t.id)",
            "DELETE FROM datafusion.public.t WHERE id = (SELECT max(u.id) FROM datafusion.public.u WHERE u.s = t.s)",
        ] {
            fresh(&engine).await;
            let err = run(&engine, sql).await.unwrap_err();
            assert!(
                err.contains("is not supported yet: the rows to change can only be selected by conditions on the columns of"),
                "{sql}: {err}"
            );
            assert_eq!(run(&engine, all).await.unwrap(), before, "{sql} changed the rows");
        }
        // Exactly the rows the WHERE selects.
        for (sql, changed, after) in [
            ("DELETE FROM datafusion.public.t WHERE 1 = 0", 0, before.clone()),
            ("DELETE FROM datafusion.public.t WHERE false", 0, before.clone()),
            (
                "DELETE FROM datafusion.public.t WHERE id = 1 AND id = 2",
                0,
                before.clone(),
            ),
            (
                "UPDATE datafusion.public.t SET s = 'z' WHERE 1 = 0",
                0,
                before.clone(),
            ),
            (
                "UPDATE datafusion.public.t SET s = 'z' WHERE id = (SELECT max(id) FROM datafusion.public.u)",
                1,
                rows(&[(1, "a"), (2, "z"), (3, "c")]),
            ),
            (
                "UPDATE datafusion.public.t AS x SET s = 'w' WHERE x.id = 3",
                1,
                rows(&[(1, "a"), (2, "b"), (3, "w")]),
            ),
            (
                "DELETE FROM datafusion.public.t WHERE id + 1 > 2 AND id + 1 < 4",
                1,
                rows(&[(1, "a"), (3, "c")]),
            ),
            (
                "DELETE FROM datafusion.public.t WHERE id IN (1, 3)",
                2,
                rows(&[(2, "b")]),
            ),
            (
                "UPDATE datafusion.public.t SET s = 'q'",
                3,
                rows(&[(1, "q"), (2, "q"), (3, "q")]),
            ),
            (
                "UPDATE datafusion.public.t SET id = id * 2 + 1 WHERE id * 2 + 1 > 3",
                2,
                rows(&[(1, "a"), (5, "b"), (7, "c")]),
            ),
        ] {
            fresh(&engine).await;
            assert_eq!(run(&engine, sql).await.unwrap(), count(changed), "{sql}");
            assert_eq!(run(&engine, all).await.unwrap(), after, "{sql}");
        }
    }
}
