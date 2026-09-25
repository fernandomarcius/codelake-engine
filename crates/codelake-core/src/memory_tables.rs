//! Tables of `DataFusion`'s in-memory catalogs, kept inside the memory pool.
//!
//! `CREATE TABLE` in an in-memory catalog keeps every row in server memory
//! after the statement. `DataFusion` holds them outside the pool: any client
//! could fill the server past its ceiling (ADR 0003). [`PooledTable`] holds a
//! reservation of what its rows take, for as long as the table exists:
//! `CREATE TABLE … AS` reserves each batch as it arrives and fails, with
//! nothing registered, once the pool cannot hold more; `INSERT` reserves
//! what it appends and gives it back if it fails; `DELETE` and `UPDATE`
//! resize the reservation to what the table then holds. Dropping or
//! replacing the table frees it.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Constraints;
use datafusion::datasource::memory::PartitionData;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
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

    /// Resizes the reservation to what the table holds now. Rows already
    /// changed are recorded even beyond the ceiling (the change cannot be
    /// undone), so later statements find the pool full.
    async fn account(&self) {
        let mut bytes = 0;
        for partition in &self.inner.batches {
            bytes += partition
                .read()
                .await
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
        }
        lock(&self.reservation).resize(bytes);
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
        let deleted = self.inner.delete_from(state, filters).await;
        self.account().await;
        deleted
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let updated = self.inner.update(state, assignments, filters).await;
        self.account().await;
        updated
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
}
