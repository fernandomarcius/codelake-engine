//! Catalog and schema providers over a Trino file metastore, and the DDL the
//! engine routes here (`WritableCatalog`).
//!
//! Listing (`schema_names`, `table_names`) comes from a cache refreshed after
//! every DDL and at startup; `table()` always reads the metastore, so tables
//! created by another writer (Trino during a canary) resolve immediately.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use codelake_core::{CatalogSession, ProcedureCall, TableProcedure, ViewDefinition, WritableCatalog};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, cast, col};
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};

use super::metastore::{ColumnMetadata, FileMetastore, MetastoreError, TableMetadata};
use super::optimize::{self, OPTIMIZE, Settings};
use super::table::{HiveSink, HiveTable};
use super::types::{arrow_type, hive_type};

const VIEW_PREFIX: &str = "/* Presto View: ";
const VIEW_SUFFIX: &str = " */";

fn metastore_error(err: MetastoreError) -> DataFusionError {
    match err {
        MetastoreError::AlreadyExists(what) => DataFusionError::Plan(format!("{what} already exists")),
        MetastoreError::NotFound(what) => DataFusionError::Plan(format!("{what} does not exist")),
        other => DataFusionError::External(Box::new(other)),
    }
}

/// View definition as Trino stores it (base64 JSON inside `viewOriginalText`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TrinoView {
    pub(crate) original_sql: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) catalog: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) schema: Option<String>,
    pub(crate) columns: Vec<TrinoViewColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(default)]
    pub(crate) run_as_invoker: bool,
    #[serde(default)]
    pub(crate) path: Vec<serde_json::Value>,
    /// Fields this version does not model (`comment`, …), kept on rewrite.
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TrinoViewColumn {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) trino_type: String,
    #[serde(flatten)]
    pub(crate) extra: BTreeMap<String, serde_json::Value>,
}

impl TrinoView {
    pub(crate) fn decode(text: &str) -> Result<Self> {
        let encoded = text
            .strip_prefix(VIEW_PREFIX)
            .and_then(|rest| rest.strip_suffix(VIEW_SUFFIX))
            .ok_or_else(|| DataFusionError::Plan("not a Presto view definition".into()))?;
        let json = BASE64
            .decode(encoded.trim())
            .map_err(|e| DataFusionError::Plan(format!("invalid view encoding: {e}")))?;
        serde_json::from_slice(&json)
            .map_err(|e| DataFusionError::Plan(format!("invalid view definition: {e}")))
    }

    pub(crate) fn encode(&self) -> Result<String> {
        let json = serde_json::to_vec(self).map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(format!("{VIEW_PREFIX}{}{VIEW_SUFFIX}", BASE64.encode(json)))
    }
}

/// Trino type name (view columns) → Arrow.
pub(crate) fn trino_to_arrow(trino: &str) -> Result<DataType> {
    let lower = trino.trim().to_ascii_lowercase();
    if let Some(precision) = lower.strip_prefix("timestamp(").and_then(|r| r.strip_suffix(')')) {
        let unit = match precision.parse::<u8>().unwrap_or(3) {
            0..=3 => TimeUnit::Millisecond,
            4..=6 => TimeUnit::Microsecond,
            _ => TimeUnit::Nanosecond,
        };
        return Ok(DataType::Timestamp(unit, None));
    }
    match lower.split('(').next().unwrap_or_default() {
        "integer" => Ok(DataType::Int32),
        "real" => Ok(DataType::Float32),
        "varbinary" => Ok(DataType::Binary),
        _ => arrow_type(&lower),
    }
}

/// Arrow → Trino type name (view columns).
pub(crate) fn arrow_to_trino(data_type: &DataType) -> Result<String> {
    Ok(match data_type {
        DataType::Int32 => "integer".into(),
        DataType::Float32 => "real".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "varbinary".into(),
        DataType::Timestamp(TimeUnit::Second | TimeUnit::Millisecond, None) => "timestamp(3)".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp(6)".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp(9)".into(),
        other => hive_type(other)?,
    })
}

/// A view stored in the metastore. Planned from its SQL at scan time, in the
/// catalog and schema stored with it (Trino resolves a view's unqualified
/// names that way, not in the reader's session), then cast to the declared
/// column types.
#[derive(Debug)]
struct HiveView {
    definition: TrinoView,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for HiveView {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.definition.original_sql)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| DataFusionError::Internal("hive views need a SessionState".into()))?;
        let mut view_state = state.clone();
        let defaults = &mut view_state.config_mut().options_mut().catalog;
        if let Some(catalog) = &self.definition.catalog {
            defaults.default_catalog.clone_from(catalog);
        }
        if let Some(schema) = &self.definition.schema {
            defaults.default_schema.clone_from(schema);
        }
        let plan = view_state
            .create_logical_plan(&self.definition.original_sql)
            .await?;
        let output = plan.schema().clone();
        if output.fields().len() != self.schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "view returns {} columns but declares {}",
                output.fields().len(),
                self.schema.fields().len()
            )));
        }
        let casts: Vec<Expr> = output
            .columns()
            .into_iter()
            .zip(self.schema.fields())
            .map(|(column, field)| cast(col(column), field.data_type().clone()).alias(field.name()))
            .collect();
        let mut builder = LogicalPlanBuilder::from(plan).project(casts)?;
        if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            builder = builder.filter(filter)?;
        }
        if let Some(indices) = projection {
            let fields = self.schema.fields();
            builder = builder.project(indices.iter().map(|i| col(fields[*i].name())).collect::<Vec<_>>())?;
        }
        if let Some(limit) = limit {
            builder = builder.limit(0, Some(limit))?;
        }
        state.create_physical_plan(&builder.build()?).await
    }
}

#[derive(Debug, Default)]
struct Listing {
    tables: BTreeMap<String, BTreeSet<String>>,
}

/// Catalog over one warehouse root.
#[derive(Debug)]
pub struct HiveCatalog {
    name: String,
    metastore: FileMetastore,
    store_url: ObjectStoreUrl,
    listing: RwLock<Listing>,
}

impl HiveCatalog {
    pub(crate) fn new(name: &str, metastore: FileMetastore) -> Result<Self> {
        let store_url = ObjectStoreUrl::parse(format!("codelake://{name}"))?;
        Ok(Self {
            name: name.to_owned(),
            metastore,
            store_url,
            listing: RwLock::new(Listing::default()),
        })
    }

    pub(crate) fn metastore(&self) -> &FileMetastore {
        &self.metastore
    }

    /// Reloads schema and table names from the metastore.
    pub(crate) async fn refresh(&self) -> Result<()> {
        let mut tables = BTreeMap::new();
        for schema in self.metastore.list_schemas().await.map_err(metastore_error)? {
            let names = self
                .metastore
                .list_tables(&schema)
                .await
                .map_err(metastore_error)?;
            tables.insert(schema, names.into_iter().collect());
        }
        if let Ok(mut listing) = self.listing.write() {
            listing.tables = tables;
        }
        Ok(())
    }

    fn schema_provider(self: &Arc<Self>, schema: &str) -> Arc<dyn SchemaProvider> {
        Arc::new(HiveSchema {
            catalog: Arc::clone(self),
            schema: hive_name(schema),
        })
    }

    async fn resolve(&self, schema: &str, table: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(metadata) = self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?
        else {
            return Ok(None);
        };
        if metadata.is_view() {
            let text = metadata
                .view_original_text
                .as_deref()
                .ok_or_else(|| DataFusionError::Plan(format!("view {schema}.{table} has no definition")))?;
            let definition = TrinoView::decode(text)?;
            let fields = definition
                .columns
                .iter()
                .map(|c| Ok(Field::new(&c.name, trino_to_arrow(&c.trino_type)?, true)))
                .collect::<Result<Vec<_>>>()?;
            return Ok(Some(Arc::new(HiveView {
                definition,
                schema: Arc::new(Schema::new(fields)),
            })));
        }
        Ok(Some(Arc::new(self.table_provider(schema, table, &metadata)?)))
    }

    pub(crate) fn table_provider(
        &self,
        schema: &str,
        table: &str,
        metadata: &TableMetadata,
    ) -> Result<HiveTable> {
        HiveTable::from_metadata(
            self.metastore.clone(),
            self.store_url.clone(),
            schema,
            table,
            metadata,
        )
    }

    /// Finishes or undoes `OPTIMIZE` runs interrupted by a crash, in every
    /// table. Called when the catalog is opened, before any query can list a
    /// table whose files were half swapped. A table whose swap cannot be
    /// settled (unreadable or inconsistent marker) is logged and left as it
    /// is: the other tables stay available, and that table refuses scans and
    /// `OPTIMIZE` until someone inspects it (its marker stays in place).
    pub(crate) async fn recover_interrupted_optimize(&self) -> Result<usize> {
        let tables: Vec<(String, String)> = self
            .listing
            .read()
            .map(|l| {
                l.tables
                    .iter()
                    .flat_map(|(schema, tables)| tables.iter().map(move |t| (schema.clone(), t.clone())))
                    .collect()
            })
            .unwrap_or_default();
        let mut recovered = 0;
        for (schema, table) in tables {
            match self.recover_table(&schema, &table).await {
                Ok(true) => recovered += 1,
                Ok(false) => {}
                Err(err) => tracing::error!(
                    catalog = %self.name,
                    %schema,
                    %table,
                    error = %err,
                    "an interrupted OPTIMIZE could not be settled; the table refuses scans until it is inspected"
                ),
            }
        }
        Ok(recovered)
    }

    /// [`optimize::recover`] for one table; `false` when it has no marker.
    async fn recover_table(&self, schema: &str, table: &str) -> Result<bool> {
        let marker = self.metastore.table_dir(schema, table).join(optimize::MARKER);
        match self.metastore.store().head(&marker).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        }
        let Some(metadata) = self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?
        else {
            return Ok(false);
        };
        let hive = self.table_provider(schema, table, &metadata)?;
        let locks = self.metastore.table_locks(schema, table);
        let _one_at_a_time = locks.optimize.lock().await;
        optimize::recover(&hive).await
    }

    async fn require_schema(&self, schema: &str) -> Result<()> {
        if self
            .metastore
            .schema_exists(schema)
            .await
            .map_err(metastore_error)?
        {
            Ok(())
        } else {
            Err(DataFusionError::Plan(format!(
                "schema {}.{schema} does not exist",
                self.name
            )))
        }
    }
}

/// `CatalogProvider` wrapper: providers need an `Arc` of the catalog.
#[derive(Debug)]
pub(crate) struct HiveCatalogProvider(pub(crate) Arc<HiveCatalog>);

impl CatalogProvider for HiveCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.0
            .listing
            .read()
            .map(|l| l.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Any name resolves: a schema created by another writer must be usable
        // before the next refresh. Missing tables simply resolve to None.
        Some(self.0.schema_provider(name))
    }
}

#[derive(Debug)]
struct HiveSchema {
    catalog: Arc<HiveCatalog>,
    schema: String,
}

#[async_trait]
impl SchemaProvider for HiveSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.catalog
            .listing
            .read()
            .ok()
            .and_then(|l| l.tables.get(&self.schema).map(|t| t.iter().cloned().collect()))
            .unwrap_or_default()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.catalog.resolve(&self.schema, &hive_name(name)).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.catalog.listing.read().is_ok_and(|l| {
            l.tables
                .get(&self.schema)
                .is_some_and(|t| t.contains(&hive_name(name)))
        })
    }
}

/// Name as the Hive connector stores it. Trino folds identifiers to lower
/// case, so its Hive catalog only knows lower-case schemas, tables and
/// columns: a quoted `"MixedCase"` is the table `mixedcase`. Every entry of
/// this catalog folds, so directory, metadata and lookup always agree.
pub(crate) fn hive_name(name: &str) -> String {
    name.to_lowercase()
}

fn column(name: &str, data_type: &DataType) -> Result<ColumnMetadata> {
    Ok(ColumnMetadata {
        name: hive_name(name),
        hive_type: hive_type(data_type)?,
        comment: None,
        properties: BTreeMap::new(),
        extra: BTreeMap::new(),
    })
}

#[async_trait]
impl WritableCatalog for HiveCatalog {
    async fn create_schema(&self, schema: &str, if_not_exists: bool, owner: &str) -> Result<()> {
        let schema = &hive_name(schema);
        match self.metastore.create_schema(schema, owner).await {
            Ok(()) => {}
            Err(MetastoreError::AlreadyExists(_)) if if_not_exists => return Ok(()),
            Err(err) => return Err(metastore_error(err)),
        }
        self.refresh().await
    }

    async fn create_table_as(
        &self,
        schema: &str,
        table: &str,
        if_not_exists: bool,
        owner: &str,
        rows: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<u64> {
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        self.require_schema(schema).await?;
        if self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?
            .is_some()
        {
            if if_not_exists {
                return Ok(0);
            }
            return Err(DataFusionError::Plan(format!(
                "table {}.{schema}.{table} already exists",
                self.name
            )));
        }
        let columns = rows
            .schema()
            .fields()
            .iter()
            .map(|f| column(f.name(), f.data_type()))
            .collect::<Result<Vec<_>>>()?;
        let mut names = BTreeSet::new();
        if let Some(twice) = columns.iter().find(|c| !names.insert(c.name.as_str())) {
            return Err(DataFusionError::Plan(format!(
                "duplicate column name `{}` in {schema}.{table} (hive column names are case-insensitive)",
                twice.name
            )));
        }
        let metadata = TableMetadata::managed_parquet(owner, columns);
        let hive = self.table_provider(schema, table, &metadata)?;
        // Trino refuses a target directory that exists without a table (left
        // by another writer); taking it over could mix foreign files in.
        if self
            .metastore
            .table_location_exists(schema, table)
            .await
            .map_err(metastore_error)?
        {
            return Err(DataFusionError::Plan(format!(
                "target directory for table {}.{schema}.{table} already exists",
                self.name
            )));
        }
        // Data first, metadata last: a failure leaves no visible table, and
        // the directory this statement created is removed, as Trino does.
        let created = async {
            let written = HiveSink { table: hive }.write_data(rows, &context).await?;
            self.metastore
                .write_table(schema, table, &metadata)
                .await
                .map_err(metastore_error)?;
            self.metastore
                .write_owner_permissions(schema, table, owner)
                .await
                .map_err(metastore_error)?;
            Ok::<_, DataFusionError>(written)
        }
        .await;
        let written = match created {
            Ok(written) => written,
            Err(err) => {
                if let Err(cleanup) = self.metastore.remove_table_location(schema, table).await {
                    tracing::warn!(%schema, %table, error = %cleanup, "failed CTAS left files behind");
                }
                return Err(err);
            }
        };
        self.refresh().await?;
        Ok(written)
    }

    async fn drop_table(&self, schema: &str, table: &str, if_exists: bool) -> Result<()> {
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        match self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?
        {
            None if if_exists => return Ok(()),
            None => {
                return Err(DataFusionError::Plan(format!(
                    "table {}.{schema}.{table} does not exist",
                    self.name
                )));
            }
            Some(metadata) if metadata.is_view() => {
                return Err(DataFusionError::Plan(format!(
                    "{schema}.{table} is a view: use DROP VIEW"
                )));
            }
            Some(_) => {}
        }
        // Never in the middle of an OPTIMIZE swap of this table.
        let locks = self.metastore.table_locks(schema, table);
        let _swap = locks.commit.write().await;
        self.metastore
            .drop_table(schema, table)
            .await
            .map_err(metastore_error)?;
        self.refresh().await
    }

    async fn create_view(
        &self,
        schema: &str,
        view: &str,
        or_replace: bool,
        owner: &str,
        definition: ViewDefinition,
    ) -> Result<()> {
        let (schema, view) = (&hive_name(schema), &hive_name(view));
        self.require_schema(schema).await?;
        if let Some(existing) = self
            .metastore
            .table(schema, view)
            .await
            .map_err(metastore_error)?
        {
            if !existing.is_view() {
                return Err(DataFusionError::Plan(format!("{schema}.{view} is a table")));
            }
            if !or_replace {
                return Err(DataFusionError::Plan(format!(
                    "view {}.{schema}.{view} already exists",
                    self.name
                )));
            }
        }
        let trino_view = TrinoView {
            original_sql: definition.sql,
            // The session the SQL was planned in, not the view's own schema:
            // unqualified names must keep resolving to the same tables.
            catalog: Some(definition.catalog),
            schema: Some(definition.schema),
            columns: definition
                .output
                .fields()
                .iter()
                .map(|f| {
                    Ok(TrinoViewColumn {
                        name: f.name().clone(),
                        trino_type: arrow_to_trino(f.data_type())?,
                        extra: BTreeMap::new(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            owner: Some(owner.to_owned()),
            run_as_invoker: false,
            path: Vec::new(),
            extra: BTreeMap::new(),
        };
        let metadata = TableMetadata::trino_view(owner, trino_view.encode()?);
        self.metastore
            .write_table(schema, view, &metadata)
            .await
            .map_err(metastore_error)?;
        self.metastore
            .write_owner_permissions(schema, view, owner)
            .await
            .map_err(metastore_error)?;
        self.refresh().await
    }

    async fn drop_view(&self, schema: &str, view: &str, if_exists: bool) -> Result<()> {
        let (schema, view) = (&hive_name(schema), &hive_name(view));
        match self
            .metastore
            .table(schema, view)
            .await
            .map_err(metastore_error)?
        {
            None if if_exists => return Ok(()),
            None => {
                return Err(DataFusionError::Plan(format!(
                    "view {}.{schema}.{view} does not exist",
                    self.name
                )));
            }
            Some(metadata) if !metadata.is_view() => {
                return Err(DataFusionError::Plan(format!(
                    "{schema}.{view} is a table: use DROP TABLE"
                )));
            }
            Some(_) => {}
        }
        self.metastore
            .drop_table(schema, view)
            .await
            .map_err(metastore_error)?;
        self.refresh().await
    }

    async fn add_column(&self, schema: &str, table: &str, name: &str, data_type: &DataType) -> Result<()> {
        let (schema, table, name) = (&hive_name(schema), &hive_name(table), &hive_name(name));
        let mut metadata = self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?
            .ok_or_else(|| {
                DataFusionError::Plan(format!("table {}.{schema}.{table} does not exist", self.name))
            })?;
        if metadata.is_view() {
            return Err(DataFusionError::Plan(format!("{schema}.{table} is a view")));
        }
        if metadata.data_columns.iter().any(|c| hive_name(&c.name) == *name) {
            return Err(DataFusionError::Plan(format!(
                "column {name} already exists in {schema}.{table}"
            )));
        }
        metadata.data_columns.push(column(name, data_type)?);
        self.metastore
            .write_table(schema, table, &metadata)
            .await
            .map_err(metastore_error)
    }

    fn table_procedures(&self) -> Vec<TableProcedure> {
        optimize::procedures()
    }

    fn check_table_procedure(&self, call: &ProcedureCall, session: &CatalogSession) -> Result<()> {
        Settings::new(call, session).map(|_| ())
    }

    async fn execute_table_procedure(
        &self,
        schema: &str,
        table: &str,
        call: &ProcedureCall,
        session: &CatalogSession,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        if !call.procedure.eq_ignore_ascii_case(OPTIMIZE) {
            return Err(DataFusionError::Plan(
                codelake_core::table_procedure::not_registered(&call.procedure),
            ));
        }
        let settings = Settings::new(call, session)?;
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        let metadata = self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Table '{}.{schema}.{table}' does not exist", self.name))
            })?;
        if metadata.is_view() {
            return Err(DataFusionError::Plan(
                "ALTER TABLE EXECUTE is not supported for views".into(),
            ));
        }
        let hive = self.table_provider(schema, table, &metadata)?;
        let outcome = optimize::optimize(&hive, settings, &context).await?;
        tracing::info!(
            catalog = %self.name,
            schema,
            table,
            files_removed = outcome.files_removed,
            files_written = outcome.files_written,
            rows = outcome.rows,
            "OPTIMIZE finished"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_VIEW: &str = include_str!("../../tests/fixtures/trino431/warehouse/pilot/v1/.trinoSchema");

    #[test]
    fn decodes_the_view_trino_wrote_and_round_trips() {
        let metadata: TableMetadata = serde_json::from_str(FIXTURE_VIEW).unwrap();
        let view = TrinoView::decode(metadata.view_original_text.as_deref().unwrap()).unwrap();
        assert!(
            view.original_sql.contains("hive.pilot.t1"),
            "{}",
            view.original_sql
        );
        assert_eq!(view.catalog.as_deref(), Some("hive"));
        assert_eq!(
            view.columns,
            vec![
                TrinoViewColumn {
                    name: "id".into(),
                    trino_type: "bigint".into(),
                    extra: BTreeMap::new(),
                },
                TrinoViewColumn {
                    name: "nome".into(),
                    trino_type: "varchar".into(),
                    extra: BTreeMap::new(),
                },
            ]
        );
        assert_eq!(TrinoView::decode(&view.encode().unwrap()).unwrap(), view);
        assert!(TrinoView::decode("SELECT 1").is_err());
    }

    #[test]
    fn view_definition_keeps_session_and_unknown_fields() {
        let json = serde_json::json!({
            "originalSql": "SELECT id FROM base",
            "catalog": "hive",
            "schema": "vw",
            "columns": [{"name": "id", "type": "bigint", "comment": "chave"}],
            "comment": "relative view",
            "owner": "trino",
            "runAsInvoker": false,
            "path": []
        });
        let text = format!("{VIEW_PREFIX}{}{VIEW_SUFFIX}", BASE64.encode(json.to_string()));
        let view = TrinoView::decode(&text).unwrap();
        assert_eq!(
            (view.catalog.as_deref(), view.schema.as_deref()),
            (Some("hive"), Some("vw"))
        );
        let back: serde_json::Value = serde_json::from_slice(
            &BASE64
                .decode(
                    view.encode()
                        .unwrap()
                        .strip_prefix(VIEW_PREFIX)
                        .unwrap()
                        .strip_suffix(VIEW_SUFFIX)
                        .unwrap(),
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn names_fold_like_trino() {
        assert_eq!(hive_name("MixedCase"), "mixedcase");
        assert_eq!(hive_name("Pedidos_2026"), "pedidos_2026");
        assert_eq!(hive_name("AÇÃO"), "ação");
        assert_eq!(column("Id", &DataType::Int64).unwrap().name, "id");
    }

    #[test]
    fn view_column_types_round_trip() {
        for (trino, arrow) in [
            ("bigint", DataType::Int64),
            ("integer", DataType::Int32),
            ("varchar", DataType::Utf8),
            ("varchar(20)", DataType::Utf8),
            ("double", DataType::Float64),
            ("real", DataType::Float32),
            ("boolean", DataType::Boolean),
            ("date", DataType::Date32),
            ("decimal(18,2)", DataType::Decimal128(18, 2)),
            ("varbinary", DataType::Binary),
            ("timestamp(3)", DataType::Timestamp(TimeUnit::Millisecond, None)),
            ("timestamp(6)", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ] {
            assert_eq!(trino_to_arrow(trino).unwrap(), arrow, "{trino}");
        }
        assert_eq!(arrow_to_trino(&DataType::Int32).unwrap(), "integer");
        assert_eq!(arrow_to_trino(&DataType::Utf8View).unwrap(), "varchar");
        assert_eq!(
            arrow_to_trino(&DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap(),
            "timestamp(3)"
        );
    }
}
