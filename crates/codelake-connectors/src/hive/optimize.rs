//! `ALTER TABLE … EXECUTE optimize[(file_size_threshold => '100MB')]`, Trino's
//! Hive table procedure (ADR 0009).
//!
//! Data files no larger than the threshold, as listed when the procedure
//! starts, are rewritten into new files of at most `target_max_file_size`,
//! then removed. Files written afterwards (a concurrent `INSERT`) are never
//! touched. As in Trino, the procedure only runs when the session enables
//! `<catalog>.non_transactional_optimize_enabled`.
//!
//! Crash safety: new files are written under hidden `.…inprogress` names. The
//! swap starts by creating a hidden marker (`.codelake-optimize.json`) that
//! lists the files to add and to remove; only then are the new files renamed
//! into place and the old ones deleted, and the marker goes last. A process
//! that dies at any point leaves either no marker (nothing visible changed)
//! or a marker from which [`recover`] finishes or undoes the swap, before the
//! next `OPTIMIZE` of the table and when the catalog is opened.
//!
//! The swap and every recovery hold the table's commit lock: in this process
//! a `tokio` lock, across processes on a local warehouse an exclusive `flock`
//! (`FileMetastore::table_file_lock`). So a recovery never inspects a swap
//! that is still running (another node opening the catalog in the middle of
//! a swap waits for it), and two swaps of one table never interleave. Before
//! publishing the marker, the swap checks that the table and every file it
//! compacted are still there with the same size. The marker is created with
//! "create if absent" as a last guard. On an object store there is no
//! cross-process lock; see ADR 0009.

use std::sync::Arc;
use std::time::SystemTime;

use codelake_core::session_properties::parse_data_size;
use codelake_core::{
    CatalogSession, Effect, ProcedureArgument, ProcedureCall, PropertyType, SessionProperty, TableProcedure,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::arrow::async_writer::ParquetObjectWriter;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::exact_timestamps;
use super::metastore::MetastoreError;
use super::table::{HiveTable, conform, data_file_name, external};
use super::trino_parquet::{ROW_GROUP_ROWS, TrinoParquetWriter};
use super::types::unsupported;

pub(crate) const OPTIMIZE: &str = "OPTIMIZE";
pub(crate) const FILE_SIZE_THRESHOLD: &str = "file_size_threshold";
pub(crate) const ENABLED: &str = "non_transactional_optimize_enabled";
pub(crate) const TARGET_MAX_FILE_SIZE: &str = "target_max_file_size";
/// Swap marker. The leading dot hides it from Trino, the sink and scans.
pub(crate) const MARKER: &str = ".codelake-optimize.json";
const MARKER_VERSION: u32 = 1;

/// Table procedures of the hive catalog.
pub(crate) fn procedures() -> Vec<TableProcedure> {
    vec![TableProcedure::new(
        OPTIMIZE,
        vec![ProcedureArgument::new(
            FILE_SIZE_THRESHOLD,
            PropertyType::DataSize,
            "100MB",
            "Only compact files smaller than given threshold in bytes",
        )],
    )]
}

/// Catalog session properties the procedure honors (Trino 431 names and defaults).
pub(crate) fn session_properties() -> Vec<SessionProperty> {
    vec![
        SessionProperty::new(
            ENABLED,
            PropertyType::Boolean,
            "false",
            Effect::Enforced,
            "Enable OPTIMIZE table procedure",
        ),
        SessionProperty::new(
            TARGET_MAX_FILE_SIZE,
            PropertyType::DataSize,
            "1GB",
            Effect::Enforced,
            "Target maximum size of files written by OPTIMIZE; the actual size may be larger \
             (INSERT always writes one file per statement)",
        ),
    ]
}

/// Validated parameters of one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Settings {
    /// Files larger than this are left alone.
    pub(crate) threshold: u64,
    /// A new output file starts once the current one reaches this size.
    pub(crate) target: u64,
}

impl Settings {
    pub(crate) fn new(call: &ProcedureCall, session: &CatalogSession) -> Result<Self> {
        if !session.is_enabled(ENABLED) {
            return Err(DataFusionError::Plan(format!(
                "OPTIMIZE procedure must be explicitly enabled via {ENABLED} session property"
            )));
        }
        let size = |value: Option<&str>, default: &str| {
            parse_data_size(value.unwrap_or(default)).map_err(DataFusionError::Plan)
        };
        Ok(Self {
            threshold: size(call.get(FILE_SIZE_THRESHOLD), "100MB")?,
            target: size(session.get(TARGET_MAX_FILE_SIZE), "1GB")?,
        })
    }
}

/// What a run did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub(crate) files_removed: usize,
    pub(crate) files_written: usize,
    pub(crate) rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Removed {
    pub(crate) name: String,
    pub(crate) size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Added {
    /// Hidden name the file was written under.
    pub(crate) temporary: String,
    /// Visible name after the swap.
    pub(crate) name: String,
    pub(crate) rows: u64,
}

/// Content of the swap marker. File names are relative to the table directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    pub(crate) version: u32,
    pub(crate) id: String,
    pub(crate) remove: Vec<Removed>,
    pub(crate) add: Vec<Added>,
}

fn file(table: &HiveTable, name: &str) -> Path {
    table.dir().join(name)
}

fn store(table: &HiveTable) -> &Arc<dyn ObjectStore> {
    table.metastore.store()
}

async fn exists(table: &HiveTable, name: &str) -> Result<bool> {
    match store(table).head(&file(table, name)).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(err) => Err(external(err)),
    }
}

async fn delete_if_exists(table: &HiveTable, name: &str) -> Result<()> {
    match store(table).delete(&file(table, name)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(err) => Err(external(err)),
    }
}

/// Hadoop keeps `.<name>.crc` next to files written by Trino.
fn checksum_name(name: &str) -> String {
    format!(".{name}.crc")
}

/// Runs `OPTIMIZE` on one table.
pub(crate) async fn optimize(
    table: &HiveTable,
    settings: Settings,
    context: &Arc<TaskContext>,
) -> Result<Outcome> {
    // Compaction rewrites every column: one `CodeLake` cannot read is refused
    // before anything is written.
    if let Some(reason) = table
        .schema
        .fields()
        .iter()
        .filter(|field| field.data_type() == &DataType::Null)
        .find_map(|field| unsupported(field))
    {
        return Err(DataFusionError::NotImplemented(format!(
            "OPTIMIZE of {}.{}: {reason}",
            table.schema_name, table.table
        )));
    }
    let locks = table.metastore.table_locks(&table.schema_name, &table.table);
    let _one_at_a_time = locks.optimize.lock().await;
    recover(table).await?;

    // Files first, columns second: `ADD COLUMN` writes the metadata before
    // any file with the new column exists, so every file listed here is
    // readable with the columns read after it. Columns read before waiting
    // for the lock above could miss one and drop its values.
    let listed = table.data_files().await?;
    let table = &table.reload().await?;
    let candidates: Vec<ObjectMeta> = listed
        .into_iter()
        .filter(|meta| meta.size <= settings.threshold)
        .collect();
    // Rewriting a single file gains nothing and renames data under the sink.
    if candidates.len() < 2 {
        return Ok(Outcome::default());
    }
    let expected = footer_rows(table, &candidates).await?;
    let added = write_compacted(table, &candidates, settings.target, context).await?;
    let written: u64 = added.iter().map(|a| a.rows).sum();
    if written != expected {
        discard(table, &added).await;
        return Err(DataFusionError::Internal(format!(
            "OPTIMIZE of {}.{} read {written} rows but the files hold {expected}; nothing was changed",
            table.schema_name, table.table
        )));
    }
    let manifest = Manifest {
        version: MARKER_VERSION,
        id: uuid::Uuid::new_v4().to_string(),
        remove: candidates
            .iter()
            .map(|meta| Removed {
                name: meta.location.filename().unwrap_or_default().to_owned(),
                size: meta.size,
            })
            .collect(),
        add: added,
    };
    commit(table, &manifest).await?;
    Ok(Outcome {
        files_removed: manifest.remove.len(),
        files_written: manifest.add.len(),
        rows: written,
    })
}

/// Row count of `files` according to their Parquet footers.
async fn footer_rows(table: &HiveTable, files: &[ObjectMeta]) -> Result<u64> {
    let mut total = 0_u64;
    for meta in files {
        let reader = ParquetObjectReader::new(Arc::clone(store(table)), meta.location.clone())
            .with_file_size(meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let rows = u64::try_from(builder.metadata().file_metadata().num_rows())
            .map_err(|_| DataFusionError::Internal(format!("{}: negative row count", meta.location)))?;
        total += rows;
    }
    Ok(total)
}

/// Output file being written.
struct Open {
    writer: TrinoParquetWriter<ParquetObjectWriter>,
    added: Added,
}

/// Streams rows into hidden files, rolling over at the target size.
struct Compactor<'a> {
    table: &'a HiveTable,
    /// Columns of the rows read: the table's, timestamps as exact
    /// nanoseconds (`exact_timestamps.rs`).
    schema: SchemaRef,
    /// Columns of the files written ([`file_columns`] of the table's).
    file_schema: SchemaRef,
    target: u64,
    reservation: MemoryReservation,
    open: Option<Open>,
    done: Vec<Added>,
}

impl Compactor<'_> {
    fn start(&self) -> Result<Open> {
        let name = data_file_name(SystemTime::now());
        let temporary = format!(".{name}.inprogress");
        let writer = TrinoParquetWriter::try_new(
            ParquetObjectWriter::new(Arc::clone(store(self.table)), file(self.table, &temporary)),
            Arc::clone(&self.file_schema),
            ROW_GROUP_ROWS,
        )?
        .recording_time_zone(self.table.metastore.parquet_time_zone());
        Ok(Open {
            writer,
            added: Added {
                temporary,
                name,
                rows: 0,
            },
        })
    }

    async fn push(&mut self, batch: &datafusion::arrow::record_batch::RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut open = match self.open.take() {
            Some(open) => open,
            None => self.start()?,
        };
        let written = async {
            open.writer.write(&conform(&self.schema, batch)?).await?;
            open.added.rows += batch.num_rows() as u64;
            // The encoder buffers the open row group: account for it.
            self.reservation.try_resize(open.writer.memory_size())?;
            Ok::<_, DataFusionError>(())
        }
        .await;
        let size = (open.writer.bytes_written() + open.writer.in_progress_size()) as u64;
        self.open = Some(open);
        written?;
        if size >= self.target {
            self.close().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let added = open.added.clone();
        // Registered before closing so a failed close still cleans the file up.
        self.done.push(added.clone());
        let rows = open.writer.close().await?;
        self.reservation.free();
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        if rows != added.rows {
            return Err(DataFusionError::Internal(format!(
                "{} holds {rows} rows, {} were written",
                added.temporary, added.rows
            )));
        }
        Ok(())
    }
}

/// The table's columns as the files written store them: timestamps as
/// `INT96`, which holds the exact nanoseconds read (never rounded to the
/// catalog's precision: compaction keeps what the files hold).
fn file_columns(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Timestamp(..) => field
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => field.as_ref().clone(),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Writes the rows of `candidates` into new hidden files. On failure every
/// file it started is removed.
pub(crate) async fn write_compacted(
    table: &HiveTable,
    candidates: &[ObjectMeta],
    target: u64,
    context: &Arc<TaskContext>,
) -> Result<Vec<Added>> {
    // One group, in listing order: the files are read one after another.
    let group = candidates
        .iter()
        .cloned()
        .map(PartitionedFile::new_from_meta)
        .collect();
    // Timestamps are read and rewritten exactly, whatever the catalog's
    // precision: compaction must neither round what another catalog wrote
    // (a catalog at MICROSECONDS or NANOSECONDS) nor wrap an instant
    // outside what Arrow nanoseconds hold (`exact_timestamps.rs`).
    let plan = exact_timestamps::exact_scan(table, &context.runtime_env(), vec![group])?;
    let schema = plan.schema();
    let mut compactor = Compactor {
        table,
        schema,
        // The files keep each column's stored spelling (ISS-018).
        file_schema: file_columns(&table.stored_schema()),
        target,
        reservation: MemoryConsumer::new(format!("HiveOptimize[{}]", table.table))
            .register(context.memory_pool()),
        open: None,
        done: Vec::new(),
    };
    let result = async {
        let mut rows = plan.execute(0, Arc::clone(context))?;
        while let Some(batch) = rows.next().await {
            compactor.push(&batch?).await?;
        }
        compactor.close().await
    }
    .await;
    if let Err(err) = result {
        let mut started = compactor.done;
        started.extend(compactor.open.map(|open| open.added));
        discard(table, &started).await;
        return Err(err);
    }
    Ok(compactor.done)
}

/// Best-effort removal of hidden files that never became visible.
async fn discard(table: &HiveTable, added: &[Added]) {
    for file in added {
        if let Err(err) = delete_if_exists(table, &file.temporary).await {
            warn!(file = %file.temporary, error = %err, "could not remove an OPTIMIZE temporary file");
        }
    }
}

/// Checks the snapshot still holds, publishes the marker and swaps the files.
///
/// Once the marker exists the swap only moves forward (here or in
/// [`recover`]): nothing that published a marker ever deletes the new files.
/// The whole swap holds the table's commit lock, in-process and across
/// processes, so no recovery looks at it half done.
pub(crate) async fn commit(table: &HiveTable, manifest: &Manifest) -> Result<()> {
    let locks = table.metastore.table_locks(&table.schema_name, &table.table);
    let _swap = locks.commit.write().await;
    let _other_processes = match table
        .metastore
        .table_file_lock(&table.schema_name, &table.table, true)
        .await
    {
        Ok(lock) => lock,
        Err(err) => {
            discard(table, &manifest.add).await;
            return Err(external(err));
        }
    };
    if let Err(err) = verify(table, manifest).await {
        discard(table, &manifest.add).await;
        return Err(err);
    }
    let marker = file(table, MARKER);
    let body = serde_json::to_vec_pretty(manifest).map_err(external)?;
    let created = store(table)
        .put_opts(&marker, PutPayload::from(body), PutOptions::from(PutMode::Create))
        .await;
    match created {
        Ok(_) => {}
        Err(object_store::Error::AlreadyExists { .. }) => {
            discard(table, &manifest.add).await;
            return Err(DataFusionError::Plan(format!(
                "another OPTIMIZE is swapping files of {}.{}; nothing was changed, run it again",
                table.schema_name, table.table
            )));
        }
        Err(err) => {
            discard(table, &manifest.add).await;
            return Err(external(err));
        }
    }
    roll_forward(table, manifest).await.map_err(|err| {
        DataFusionError::Execution(format!(
            "OPTIMIZE of {}.{} stopped while swapping files ({err}); the next OPTIMIZE of the table \
             or a restart finishes it from {MARKER}",
            table.schema_name, table.table
        ))
    })
}

/// The table still exists and every file to replace is still there, unchanged.
async fn verify(table: &HiveTable, manifest: &Manifest) -> Result<()> {
    let target = format!("{}.{}", table.schema_name, table.table);
    let current = table
        .metastore
        .table(&table.schema_name, &table.table)
        .await
        .map_err(external)?;
    let Some(current) = current else {
        return Err(DataFusionError::Plan(format!(
            "table {target} was dropped while OPTIMIZE ran"
        )));
    };
    if current.is_view() || current.table_id() != table.table_id.as_deref() {
        return Err(DataFusionError::Plan(format!(
            "table {target} was replaced while OPTIMIZE ran; nothing was changed, run it again"
        )));
    }
    for removed in &manifest.remove {
        let unchanged = match store(table).head(&file(table, &removed.name)).await {
            Ok(meta) => meta.size == removed.size,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(err) => return Err(external(err)),
        };
        if !unchanged {
            return Err(DataFusionError::Plan(format!(
                "data file {} of {target} changed while OPTIMIZE ran; nothing was changed, run it again",
                removed.name
            )));
        }
    }
    Ok(())
}

/// Makes the new files visible, removes the old ones, then the marker.
/// Idempotent: every step tolerates having been done already.
async fn roll_forward(table: &HiveTable, manifest: &Manifest) -> Result<()> {
    for added in &manifest.add {
        let renamed = store(table)
            .rename(&file(table, &added.temporary), &file(table, &added.name))
            .await;
        match renamed {
            Ok(()) => {}
            Err(object_store::Error::NotFound { .. }) if exists(table, &added.name).await? => {}
            Err(err) => return Err(external(err)),
        }
    }
    for removed in &manifest.remove {
        delete_if_exists(table, &removed.name).await?;
        delete_if_exists(table, &checksum_name(&removed.name)).await?;
    }
    match table
        .metastore
        .invalidate_statistics(&table.schema_name, &table.table)
        .await
    {
        Ok(()) | Err(MetastoreError::NotFound(_)) => {}
        Err(err) => return Err(external(err)),
    }
    delete_if_exists(table, MARKER).await
}

/// Removes the new files and the marker; the old files were never touched.
async fn roll_back(table: &HiveTable, manifest: &Manifest) -> Result<()> {
    for added in &manifest.add {
        delete_if_exists(table, &added.temporary).await?;
        delete_if_exists(table, &added.name).await?;
    }
    delete_if_exists(table, MARKER).await
}

/// Finishes or undoes a swap interrupted by a crash. Returns whether a marker
/// was found.
///
/// The marker is published only after the snapshot was verified, and old
/// files are deleted only after every new file is visible. So:
/// - every new file present (hidden or renamed): finish the swap;
/// - a new file gone while every old file is still there (someone removed
///   it): undo, keeping the old files;
/// - anything else was changed by someone else: stop and say so.
pub(crate) async fn recover(table: &HiveTable) -> Result<bool> {
    // Read and act on the marker only while no swap runs, here or in another
    // process: a marker of a live swap is not a crash.
    let locks = table.metastore.table_locks(&table.schema_name, &table.table);
    let _swap = locks.commit.write().await;
    let _other_processes = table
        .metastore
        .table_file_lock(&table.schema_name, &table.table, true)
        .await
        .map_err(external)?;
    let marker = file(table, MARKER);
    let bytes = match store(table).get(&marker).await {
        Ok(found) => found.bytes().await.map_err(external)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(false),
        Err(err) => return Err(external(err)),
    };
    let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|err| {
        DataFusionError::Plan(format!(
            "OPTIMIZE marker {marker} is unreadable ({err}); inspect the table directory"
        ))
    })?;
    let mut lost = false;
    for added in &manifest.add {
        // Hidden name first: a rename between the two checks (a writer this
        // lock does not reach) then finds the file under its final name.
        if !exists(table, &added.temporary).await? && !exists(table, &added.name).await? {
            lost = true;
        }
    }
    let mut originals = true;
    for removed in &manifest.remove {
        originals &= exists(table, &removed.name).await?;
    }
    let target = format!("{}.{}", table.schema_name, table.table);
    if !lost {
        warn!(table = %target, id = %manifest.id, "finishing an interrupted OPTIMIZE");
        roll_forward(table, &manifest).await?;
    } else if lost && originals {
        warn!(table = %target, id = %manifest.id, "undoing an interrupted OPTIMIZE");
        roll_back(table, &manifest).await?;
    } else {
        return Err(DataFusionError::Plan(format!(
            "OPTIMIZE marker {marker} lists files that are gone; inspect the table directory"
        )));
    }
    info!(table = %target, id = %manifest.id, "interrupted OPTIMIZE recovered");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declarations_match_trino_431() {
        let procedures = procedures();
        assert_eq!(procedures.len(), 1);
        assert_eq!(procedures[0].name, "OPTIMIZE");
        assert_eq!(procedures[0].arguments[0].name, "file_size_threshold");
        assert_eq!(procedures[0].arguments[0].default, "100MB");
        let properties = session_properties();
        assert_eq!(properties[0].name, "non_transactional_optimize_enabled");
        assert_eq!(properties[0].default, "false");
        assert_eq!(properties[1].name, "target_max_file_size");
        assert_eq!(properties[1].default, "1GB");
    }

    #[test]
    fn settings_require_the_session_property() {
        let procedure = &procedures()[0];
        let call = codelake_core::table_procedure::bind("hive", procedure, vec![]).unwrap();
        let disabled = CatalogSession::default();
        assert_eq!(
            Settings::new(&call, &disabled).unwrap_err().to_string(),
            "Error during planning: OPTIMIZE procedure must be explicitly enabled via \
             non_transactional_optimize_enabled session property"
        );
        let enabled = CatalogSession::new([(ENABLED.to_owned(), "true".to_owned())].into_iter().collect());
        assert_eq!(
            Settings::new(&call, &enabled).unwrap(),
            Settings {
                threshold: 100 << 20,
                target: 1 << 30
            }
        );
        let small = CatalogSession::new(
            [
                (ENABLED.to_owned(), "true".to_owned()),
                (TARGET_MAX_FILE_SIZE.to_owned(), "1kB".to_owned()),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(Settings::new(&call, &small).unwrap().target, 1024);
    }

    #[test]
    fn checksum_side_files() {
        assert_eq!(checksum_name("20260101_x"), ".20260101_x.crc");
    }
}
