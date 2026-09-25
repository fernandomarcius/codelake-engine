//! Catalog and schema providers over a Trino file metastore, and the DDL the
//! engine routes here (`WritableCatalog`).
//!
//! Listing (`schema_names`, `table_names`) reads a local warehouse's
//! directories each time, so a schema or table another writer created (Trino
//! during a canary, another `CodeLake` node) is listed at once; an object
//! store warehouse lists from a cache refreshed after every DDL and at
//! startup. `table()` always reads the metastore.
//!
//! A table the catalog cannot read (another storage format, partitions,
//! unreadable metadata) fails only when it is read; columns of a type
//! `CodeLake` does not read yet fail only when the query reads them (see
//! `types.rs`). Listings (`information_schema`) stay whole.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use codelake_core::statement_policy::{ObjectKind, check_object_name, parse_view_query};
use codelake_core::trino_types::Declared;
use codelake_core::{
    COLUMN_COMMENT, CatalogSession, ProcedureCall, TableProcedure, ViewDefinition, ViewSecurity,
    WritableCatalog,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::Column;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::functions::expr_fn;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, TableProviderFilterPushDown, cast};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::dialect::dialect_from_str;
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};

use codelake_core::config::HiveTimestampPrecision;

use super::metastore::{
    ColumnMetadata, FileMetastore, MetastoreError, Staging, TABLE_ID, TableMetadata, WRITTEN_NAME,
};
use super::optimize::{self, OPTIMIZE, Settings};
use super::table::{HiveSink, HiveTable};
use super::types::{UNSUPPORTED, arrow_type, char_column, hive_type, stored_type, unsupported};

const VIEW_PREFIX: &str = "/* Presto View: ";
const VIEW_SUFFIX: &str = " */";

fn metastore_error(err: MetastoreError) -> DataFusionError {
    match err {
        MetastoreError::AlreadyExists(what) => DataFusionError::Plan(format!("{what} already exists")),
        MetastoreError::NotFound(what) => DataFusionError::Plan(format!("{what} does not exist")),
        MetastoreError::Conflict(what) => DataFusionError::Plan(what),
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

/// Arrow unit of a Trino timestamp precision.
fn precision_unit(precision: &str) -> TimeUnit {
    // The smallest unit that holds the precision; 1 and 2 (like 4, 5, 7 and
    // 8) also declare it in the field (see `view_declared`).
    match precision.trim().parse::<u8>().unwrap_or(3) {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

/// Zone of the Arrow type of Trino's `timestamp(p) with time zone`: values
/// keep their instant; each value's own zone is not kept (Arrow has one zone
/// per column), so they read in UTC.
const VIEW_ZONE: &str = "UTC";

/// Trino type name (view columns) → Arrow.
pub(crate) fn trino_to_arrow(trino: &str) -> Result<DataType> {
    let lower = trino.trim().to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("timestamp") {
        let (precision, zoned) = match rest.trim().strip_suffix("with time zone") {
            Some(before) => (before.trim(), true),
            None => (rest.trim(), false),
        };
        let precision = precision
            .strip_prefix('(')
            .and_then(|p| p.strip_suffix(')'))
            .unwrap_or("3");
        let zone = zoned.then(|| VIEW_ZONE.into());
        return Ok(DataType::Timestamp(precision_unit(precision), zone));
    }
    match lower.split('(').next().unwrap_or_default() {
        "integer" => Ok(DataType::Int32),
        "real" => Ok(DataType::Float32),
        "varbinary" => Ok(DataType::Binary),
        _ => arrow_type(&lower),
    }
}

/// The field of a view column: a type `CodeLake` does not read yet fails
/// only when a query reads the column (see `types.rs`).
fn view_field(name: &str, trino: &str) -> Field {
    let lower = trino.trim().to_ascii_lowercase();
    let refuse = |data_type: DataType, reason: String| {
        Field::new(name, data_type, true).with_metadata(std::collections::HashMap::from([(
            UNSUPPORTED.to_owned(),
            reason,
        )]))
    };
    if lower == "char" || lower.starts_with("char(") {
        // Padded like a table's char column (see `types.rs`).
        return super::types::column_field(name, &lower, HiveTimestampPrecision::default());
    }
    match trino_to_arrow(&lower) {
        Ok(data_type) => {
            let field = Field::new(name, data_type, true);
            match view_declared(&lower) {
                Some(declared) => field.with_metadata(declared.metadata()),
                None => field,
            }
        }
        Err(_) => refuse(
            DataType::Null,
            format!("view column {name}: type `{trino}` is not supported"),
        ),
    }
}

/// The type a view column declares beyond its Arrow type: `varchar(n)`, and
/// a timestamp precision no Arrow unit has (`timestamp(1)`); `char(n)` is a
/// table's char column (see `view_field`).
fn view_declared(trino: &str) -> Option<Declared> {
    Declared::parse(trino).filter(|d| match d {
        Declared::Varchar(_) => true,
        Declared::Timestamp(p) => ![0, 3, 6, 9].contains(p),
        Declared::Char(_) => false,
    })
}

/// Arrow → Trino type name (view columns).
pub(crate) fn arrow_to_trino(data_type: &DataType) -> Result<String> {
    let digits = |unit: &TimeUnit| match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    };
    Ok(match data_type {
        DataType::Int32 => "integer".into(),
        DataType::Float32 => "real".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "varbinary".into(),
        DataType::Timestamp(TimeUnit::Second, None) => "timestamp(0)".into(),
        DataType::Timestamp(TimeUnit::Millisecond, None) => "timestamp(3)".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp(6)".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "timestamp(9)".into(),
        DataType::Timestamp(unit, Some(_)) => format!("timestamp({}) with time zone", digits(unit)),
        other => hive_type(other)?,
    })
}

/// A view stored in the metastore. Planned from its SQL at scan time, in the
/// catalog and schema stored with it (Trino resolves a view's unqualified
/// names that way, not in the reader's session), then cast to the declared
/// column types. The stored SQL must be a query: whoever wrote the metadata,
/// reading a view never runs anything else.
#[derive(Debug)]
struct HiveView {
    name: String,
    definition: TrinoView,
    schema: SchemaRef,
}

/// Refuses reading columns `CodeLake` cannot return (see `types.rs`).
pub(crate) fn refuse_unsupported(schema: &Schema, projection: Option<&Vec<usize>>) -> Result<()> {
    let indices: Vec<usize> = projection.map_or_else(|| (0..schema.fields().len()).collect(), Clone::clone);
    for index in &indices {
        if let Some(reason) = schema.fields().get(*index).and_then(|f| unsupported(f)) {
            return Err(DataFusionError::NotImplemented(reason.to_owned()));
        }
    }
    Ok(())
}

#[async_trait]
impl TableProvider for HiveView {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.definition.original_sql)
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        // Seen by `scan` (to refuse char comparisons), applied again above.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
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
        refuse_unsupported(&self.schema, projection)?;
        // The view's plan becomes part of the outer one, which merges and
        // reserves the output of the whole query (`codelake_core::output`).
        let rules = state
            .physical_optimizers()
            .iter()
            .filter(|rule| rule.name() != codelake_core::output::ACCOUNTED_OUTPUT)
            .cloned()
            .collect();
        let mut view_state = SessionStateBuilder::new_from_existing(state.clone())
            .with_physical_optimizer_rules(rules)
            .build();
        let defaults = &mut view_state.config_mut().options_mut().catalog;
        if let Some(catalog) = &self.definition.catalog {
            defaults.default_catalog.clone_from(catalog);
        }
        if let Some(schema) = &self.definition.schema {
            defaults.default_schema.clone_from(schema);
        }
        let dialect_name = view_state.config().options().sql_parser.dialect;
        let zone = view_state.config().options().execution.time_zone.clone();
        let query = {
            // `Box<dyn Dialect>` is not `Send`: gone before any `.await`.
            let dialect = dialect_from_str(dialect_name)
                .ok_or_else(|| DataFusionError::Plan(format!("unsupported SQL dialect {dialect_name}")))?;
            // The same statement rewrites as any query the engine plans.
            parse_view_query(
                &self.definition.original_sql,
                dialect.as_ref(),
                zone.as_deref().unwrap_or("UTC"),
            )
            .map_err(|err| DataFusionError::Plan(format!("view {}: {err}", self.name)))?
        };
        let plan = view_state.statement_to_plan(query).await?;
        let output = plan.schema().clone();
        if output.fields().len() != self.schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "view returns {} columns but declares {}",
                output.fields().len(),
                self.schema.fields().len()
            )));
        }
        // Only the columns read: one of a type `CodeLake` does not read yet
        // must not fail a query that does not read it.
        let fields = self.schema.fields();
        let indices: Vec<usize> = projection.cloned().unwrap_or_else(|| (0..fields.len()).collect());
        let columns = output.columns();
        let mut read: Vec<usize> = indices.clone();
        for filter in filters {
            for column in filter.column_refs() {
                if let Some(index) = fields.iter().position(|f| f.name() == &column.name)
                    && !read.contains(&index)
                {
                    read.push(index);
                }
            }
        }
        refuse_unsupported(&self.schema, Some(&read))?;
        let casts: Vec<Expr> = read
            .iter()
            .map(|index| {
                let field = &fields[*index];
                let value = cast(Expr::Column(columns[*index].clone()), field.data_type().clone());
                // A char(n) column holds its values padded, as Trino shows
                // them (`codelake_core::char_semantics`).
                let value = match char_column(field).and_then(|n| u32::try_from(n).ok()) {
                    Some(length) => codelake_core::char_semantics::pad(expr_fn::rtrim(vec![value]), length),
                    None => value,
                };
                value.alias(field.name())
            })
            .collect();
        let mut builder = LogicalPlanBuilder::from(plan);
        if !casts.is_empty() {
            builder = builder.project(casts)?;
        }
        if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            builder = builder.filter(filter)?;
        }
        builder = builder.project(
            indices
                .iter()
                .map(|i| Expr::Column(Column::from_name(fields[*i].name())))
                .collect::<Vec<_>>(),
        )?;
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
            let fields: Vec<Field> = definition
                .columns
                .iter()
                .map(|c| view_field(&c.name, &c.trino_type))
                .collect();
            return Ok(Some(Arc::new(HiveView {
                name: format!("{schema}.{table}"),
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
    fn schema_names(&self) -> Vec<String> {
        if let Some(schemas) = self.0.metastore.local_schemas() {
            return schemas;
        }
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
    fn table_names(&self) -> Vec<String> {
        if let Some(tables) = self.catalog.metastore.local_tables(&self.schema) {
            return tables;
        }
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
        let name = hive_name(name);
        if let Some(tables) = self.catalog.metastore.local_tables(&self.schema) {
            return tables.contains(&name);
        }
        self.catalog
            .listing
            .read()
            .is_ok_and(|l| l.tables.get(&self.schema).is_some_and(|t| t.contains(&name)))
    }
}

/// A name folded to lower case: how the engine sees every schema, table and
/// column of this catalog (identifiers are case-insensitive), how the
/// metastore names them and the directory of a schema or table. Trino folds
/// identifiers too: it finds a table only in its lower-case directory and
/// writes only into lower-case columns. The name as written is kept in the
/// metadata (`codelake.name`) and, for columns, in the files (ISS-018).
pub(crate) fn hive_name(name: &str) -> String {
    name.to_lowercase()
}

/// Records `written` in `parameters` when it is not already its folded
/// form (ISS-018: the metastore keeps names as they were written).
pub(crate) fn record_written_name(parameters: &mut BTreeMap<String, String>, written: &str) {
    if written == hive_name(written) {
        parameters.remove(WRITTEN_NAME);
    } else {
        parameters.insert(WRITTEN_NAME.into(), written.to_owned());
    }
}

/// The error for a name taken by one that differs from it only by case.
fn taken(kind: &str, catalog: &str, written: &str, folded: &str) -> String {
    if written == folded {
        format!("{kind} {catalog}.{folded} already exists")
    } else {
        format!(
            "{kind} {catalog}.{written} cannot be created: {catalog}.{folded} already exists, and names \
             that differ only by case are the same {kind}"
        )
    }
}

/// Refuses two column names that differ only by case (ISS-018): readers
/// that ignore case (the engine, the reference engine, SQL Server
/// `OPENROWSET`) would read one for the other.
fn refuse_case_duplicates<'a>(names: impl IntoIterator<Item = &'a str>, relation: &str) -> Result<()> {
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for name in names {
        if let Some(other) = seen.insert(hive_name(name), name) {
            return Err(DataFusionError::Plan(if other == name {
                format!("Column name '{name}' specified more than once in {relation}")
            } else {
                format!(
                    "columns `{other}` and `{name}` of {relation} differ only by case: column names are \
                     case-insensitive, so they would be one column"
                )
            }));
        }
    }
    Ok(())
}

#[cfg(test)]
fn column(name: &str, data_type: &DataType) -> Result<ColumnMetadata> {
    Ok(ColumnMetadata {
        name: hive_name(name),
        hive_type: hive_type(data_type)?,
        comment: None,
        properties: BTreeMap::new(),
        extra: BTreeMap::new(),
    })
}

/// `ALTER TABLE|VIEW … RENAME TO` in the metastore, after checking that the
/// source is of the kind the statement names.
async fn rename_relation(
    catalog: &HiveCatalog,
    view: bool,
    (schema, name): (&str, &str),
    (new_schema, written): (&str, &str),
) -> Result<()> {
    let (new_schema, new_name) = (&hive_name(new_schema), &hive_name(written));
    let (noun, other) = if view {
        ("View", "table")
    } else {
        ("Table", "view")
    };
    let metadata = catalog
        .metastore
        .table(schema, name)
        .await
        .map_err(metastore_error)?
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{noun} '{}.{schema}.{name}' does not exist",
                catalog.name
            ))
        })?;
    if metadata.is_view() != view {
        return Err(DataFusionError::Plan(format!(
            "{noun} '{}.{schema}.{name}' does not exist, but a {other} with that name exists",
            catalog.name
        )));
    }
    catalog.require_schema(new_schema).await?;
    match catalog
        .metastore
        .rename_table(schema, name, new_schema, new_name)
        .await
    {
        Ok(()) => {}
        Err(MetastoreError::AlreadyExists(_)) => {
            return Err(DataFusionError::Plan(taken(
                if view { "view" } else { "table" },
                &format!("{}.{new_schema}", catalog.name),
                written,
                new_name,
            )));
        }
        Err(err) => return Err(metastore_error(err)),
    }
    // The name as the statement wrote it (ISS-018); cosmetic, so a failure
    // here leaves the rename done.
    if let Err(err) = catalog
        .metastore
        .record_written_name(new_schema, new_name, written)
        .await
    {
        tracing::warn!(catalog = %catalog.name, table = %new_name, error = %err, "could not record the written name");
    }
    catalog.refresh().await
}

/// How a new column is stored (see [`stored_type`]), with its comment.
fn new_column(
    field: &Field,
    precision: HiveTimestampPrecision,
    check_precision: bool,
) -> Result<ColumnMetadata> {
    check_object_name(ObjectKind::Column, field.name())?;
    // Folded in the metastore, as written in its property and in the files
    // (ISS-018).
    let mut properties = BTreeMap::new();
    record_written_name(&mut properties, field.name());
    Ok(ColumnMetadata {
        name: hive_name(field.name()),
        hive_type: stored_type(field, precision, check_precision)?,
        comment: field.metadata().get(COLUMN_COMMENT).cloned(),
        properties,
        extra: BTreeMap::new(),
    })
}

/// Checks a schema and table name before they become paths.
fn check_names(schema: &str, table: &str, kind: ObjectKind) -> Result<()> {
    check_object_name(ObjectKind::Schema, schema)?;
    check_object_name(kind, table)
}

/// What a `CREATE TABLE` does with an existing table of the same name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Existing {
    Fail,
    Keep,
    /// `CREATE OR REPLACE` (ISS-017).
    Replace,
}

impl HiveCatalog {
    /// Makes the table staged in `staging` visible, under the table's locks.
    /// `false` when `IF NOT EXISTS` found a table another statement created
    /// meanwhile.
    async fn publish(
        &self,
        staging: &Staging,
        schema: &str,
        table: &str,
        existing: Existing,
    ) -> Result<bool> {
        if existing == Existing::Replace {
            return self.swap_in(staging, schema, table).await.map(|()| true);
        }
        let _metadata = self
            .metastore
            .lock_metadata(schema, table)
            .await
            .map_err(metastore_error)?;
        match self.metastore.publish_table(staging, schema, table).await {
            Ok(()) => Ok(true),
            // Another statement created it meanwhile: `IF NOT EXISTS` is
            // satisfied, anything else fails as if that one came first.
            Err(MetastoreError::AlreadyExists(_)) if existing == Existing::Keep => Ok(false),
            Err(MetastoreError::AlreadyExists(_)) => Err(DataFusionError::Plan(taken(
                "table",
                &format!("{}.{schema}", self.name),
                table,
                table,
            ))),
            Err(err) => Err(metastore_error(err)),
        }
    }

    /// `CREATE OR REPLACE`: the staged table takes the name in one step
    /// ([`FileMetastore::replace_table`]), under the same locks as
    /// `DROP TABLE` (no `OPTIMIZE` swap, commit of `INSERT` files, or scan
    /// listing of this table, in any process, sees it half done).
    async fn swap_in(&self, staging: &Staging, schema: &str, table: &str) -> Result<()> {
        let locks = self.metastore.table_locks(schema, table);
        let _swap = locks.commit.write().await;
        let _other_processes = self
            .metastore
            .table_file_lock(schema, table, true)
            .await
            .map_err(metastore_error)?;
        let _metadata = self
            .metastore
            .lock_metadata(schema, table)
            .await
            .map_err(metastore_error)?;
        match self.metastore.replace_table(staging, schema, table).await {
            Ok(()) => Ok(()),
            Err(MetastoreError::Conflict(message)) => Err(DataFusionError::Plan(message)),
            Err(err) => Err(metastore_error(err)),
        }
    }

    /// `CREATE [OR REPLACE] TABLE [AS]`: rows and metadata are written into a
    /// private staging directory, then published (or swapped in, for `OR
    /// REPLACE`) under the table's locks (see `metastore.rs`). A failure
    /// before that removes only the staging directory: an existing table, or
    /// one another statement created meanwhile, is never touched. After a
    /// replacement the staging directory holds the old table, removed last.
    async fn write_table_as(
        &self,
        (schema, table): (&str, &str),
        existing: Existing,
        owner: &str,
        rows: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<u64> {
        let if_not_exists = existing == Existing::Keep;
        let written = table;
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        check_names(schema, table, ObjectKind::Table)?;
        self.require_schema(schema).await?;
        let found = self
            .metastore
            .table(schema, table)
            .await
            .map_err(metastore_error)?;
        match &found {
            Some(_) if if_not_exists => return Ok(0),
            Some(metadata) if existing == Existing::Replace && metadata.is_view() => {
                return Err(DataFusionError::Plan(format!(
                    "{schema}.{table} is a view: use CREATE OR REPLACE VIEW, or DROP VIEW first"
                )));
            }
            Some(_) if existing == Existing::Replace => {}
            Some(_) => {
                return Err(DataFusionError::Plan(taken(
                    "table",
                    &format!("{}.{schema}", self.name),
                    written,
                    table,
                )));
            }
            None => {}
        }
        let precision = self.metastore.timestamp_precision();
        let columns = rows
            .schema()
            .fields()
            .iter()
            .map(|f| new_column(f, precision, true))
            .collect::<Result<Vec<_>>>()?;
        refuse_case_duplicates(
            columns.iter().map(ColumnMetadata::written_name),
            &format!("{schema}.{table}"),
        )?;
        // Trino refuses a target directory that exists without a table (left
        // by another writer); taking it over could mix foreign files in, and
        // replacing it would delete them (red-team W1-1).
        if found.is_none()
            && self
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
        let mut metadata = TableMetadata::managed_parquet(owner, columns);
        record_written_name(&mut metadata.parameters, written);
        if existing == Existing::Replace {
            metadata
                .parameters
                .insert(TABLE_ID.into(), uuid::Uuid::new_v4().simple().to_string());
        }
        let hive = self.table_provider(schema, table, &metadata)?;
        let staging = self.metastore.staging();
        let committed = async {
            let written = HiveSink::new(hive)
                .write_into(rows, &context, &staging.dir)
                .await?;
            self.metastore
                .stage_table(&staging, &metadata, owner)
                .await
                .map_err(metastore_error)?;
            let published = self.publish(&staging, schema, table, existing).await?;
            Ok::<_, DataFusionError>(if published { written } else { 0 })
        }
        .await;
        // Published or not, nothing of this statement stays in staging
        // (after a replacement: the old table, no longer visible).
        self.metastore.discard(&staging).await;
        let written = committed?;
        self.refresh().await?;
        Ok(written)
    }
}

#[async_trait]
impl WritableCatalog for HiveCatalog {
    async fn create_schema(&self, schema: &str, if_not_exists: bool, owner: &str) -> Result<()> {
        let written = schema;
        let schema = &hive_name(schema);
        check_object_name(ObjectKind::Schema, schema)?;
        match self.metastore.create_schema(schema, written, owner).await {
            Ok(()) => {}
            Err(MetastoreError::AlreadyExists(_)) if if_not_exists => return Ok(()),
            Err(MetastoreError::AlreadyExists(_)) => {
                return Err(DataFusionError::Plan(taken(
                    "schema", &self.name, written, schema,
                )));
            }
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
        let existing = if if_not_exists {
            Existing::Keep
        } else {
            Existing::Fail
        };
        self.write_table_as((schema, table), existing, owner, rows, context)
            .await
    }

    /// `CREATE OR REPLACE TABLE`, which Trino 431's hive connector refuses
    /// (ISS-017, deliberate divergence): the new table is staged, then its
    /// directory and the old table's are exchanged in one step (see
    /// `HiveCatalog::write_table_as` and `FileMetastore::replace_table`).
    async fn replace_table_as(
        &self,
        schema: &str,
        table: &str,
        owner: &str,
        rows: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> Result<u64> {
        self.write_table_as((schema, table), Existing::Replace, owner, rows, context)
            .await
    }

    async fn drop_table(&self, schema: &str, table: &str, if_exists: bool) -> Result<()> {
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        check_names(schema, table, ObjectKind::Table)?;
        // Never in the middle of an OPTIMIZE swap of this table, nor of a
        // commit of its files.
        let locks = self.metastore.table_locks(schema, table);
        let _swap = locks.commit.write().await;
        // Scans of other processes list files under this lock, shared.
        let _other_processes = self
            .metastore
            .table_file_lock(schema, table, true)
            .await
            .map_err(metastore_error)?;
        let _metadata = self
            .metastore
            .lock_metadata(schema, table)
            .await
            .map_err(metastore_error)?;
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
        let written = view;
        let (schema, view) = (&hive_name(schema), &hive_name(view));
        check_names(schema, view, ObjectKind::View)?;
        // Trino stores no owner for SECURITY INVOKER views (`runAsInvoker`).
        let invoker = definition.security == ViewSecurity::Invoker;
        self.require_schema(schema).await?;
        {
            let dialect = dialect_from_str("generic")
                .ok_or_else(|| DataFusionError::Internal("generic SQL dialect".into()))?;
            parse_view_query(&definition.sql, dialect.as_ref(), "UTC")?;
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
                    // Trino folds view column names, as every other name.
                    check_object_name(ObjectKind::Column, f.name())?;
                    Ok(TrinoViewColumn {
                        name: hive_name(f.name()),
                        trino_type: match codelake_core::trino_types::declared(f) {
                            Some(
                                declared
                                @ (Declared::Varchar(_) | Declared::Char(_) | Declared::Timestamp(_)),
                            ) if !matches!(f.data_type(), DataType::Timestamp(_, Some(_))) => {
                                declared.name(f.data_type())
                            }
                            _ => arrow_to_trino(f.data_type())?,
                        },
                        extra: BTreeMap::new(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            // Trino stores no owner for SECURITY INVOKER views (`runAsInvoker`).
            owner: (!invoker).then(|| owner.to_owned()),
            run_as_invoker: invoker,
            path: Vec::new(),
            extra: BTreeMap::new(),
        };
        let mut metadata = TableMetadata::trino_view(owner, trino_view.encode()?);
        record_written_name(&mut metadata.parameters, written);
        let _metadata = self
            .metastore
            .lock_metadata(schema, view)
            .await
            .map_err(metastore_error)?;
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
                return Err(DataFusionError::Plan(taken(
                    "view",
                    &format!("{}.{schema}", self.name),
                    written,
                    view,
                )));
            }
        }
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
        check_names(schema, view, ObjectKind::View)?;
        let _metadata = self
            .metastore
            .lock_metadata(schema, view)
            .await
            .map_err(metastore_error)?;
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

    async fn rename_table(&self, schema: &str, table: &str, new_schema: &str, new_table: &str) -> Result<()> {
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        rename_relation(self, false, (schema, table), (new_schema, new_table)).await
    }

    async fn rename_view(&self, schema: &str, view: &str, new_schema: &str, new_view: &str) -> Result<()> {
        let (schema, view) = (&hive_name(schema), &hive_name(view));
        rename_relation(self, true, (schema, view), (new_schema, new_view)).await
    }

    async fn add_column(&self, schema: &str, table: &str, column: &Field) -> Result<()> {
        let (schema, table) = (&hive_name(schema), &hive_name(table));
        check_names(schema, table, ObjectKind::Table)?;
        // Trino records any timestamp precision as `timestamp` here.
        let column = new_column(column, self.metastore.timestamp_precision(), false)?;
        match self.metastore.add_column(schema, table, column).await {
            Ok(()) => Ok(()),
            Err(MetastoreError::NotFound(_)) => Err(DataFusionError::Plan(format!(
                "table {}.{schema}.{table} does not exist",
                self.name
            ))),
            Err(err) => Err(metastore_error(err)),
        }
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
            ("timestamp(0)", DataType::Timestamp(TimeUnit::Second, None)),
            ("timestamp(1)", DataType::Timestamp(TimeUnit::Millisecond, None)),
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
        assert_eq!(
            arrow_to_trino(&DataType::Timestamp(TimeUnit::Second, None)).unwrap(),
            "timestamp(0)"
        );
    }
}
