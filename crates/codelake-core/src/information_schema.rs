//! One `information_schema` per catalog.
//!
//! In Trino every catalog has its own `information_schema`, listing only that
//! catalog's schemas, tables, views and columns; dbt lists relations through
//! `<catalog>.information_schema` filtering by schema only, so rows of another
//! catalog with a schema of the same name would show up as its relations.
//! `DataFusion`'s `information_schema` (the `information_schema` session
//! option) answers every `<catalog>.information_schema` with all catalogs.
//!
//! The engine turns that option off and registers its catalogs in a
//! [`CatalogList`] instead: each catalog it hands out also has an
//! `information_schema` schema restricted to that catalog. Statements that
//! `DataFusion` plans over `information_schema` (`SHOW TABLES`, `SHOW
//! COLUMNS`) resolve it in the session's catalog.
//!
//! The tables are Trino 431's, column for column (dbt reads them):
//!
//! - `schemata(catalog_name, schema_name)`;
//! - `tables(table_catalog, table_schema, table_name, table_type)`, with
//!   `BASE TABLE` or `VIEW` (`information_schema`'s own tables are
//!   `BASE TABLE`, as in Trino);
//! - `views(table_catalog, table_schema, table_name, view_definition)`, the
//!   query printed as Trino prints it (`crate::trino_sql`);
//! - `columns(table_catalog, table_schema, table_name, column_name,
//!   ordinal_position, column_default, is_nullable, data_type)`, the position
//!   counted from 1 and the type named as Trino names it
//!   ([`crate::trino_types::field_display_name`]: `bigint`, `decimal(10,2)`,
//!   `varchar(10)`);
//! - `table_privileges`, `roles`, `enabled_roles`, `applicable_roles`, empty
//!   (no privileges nor roles).
//!
//! `DataFusion`'s own tables (`df_settings` for `SHOW ALL`, `routines`,
//! `parameters`) still resolve, but are not listed.
//!
//! A listing never fails because of one table: a table whose provider cannot
//! be built (unreadable metadata, a layout the connector does not read) is
//! listed without columns and logged; reading it still fails with its own
//! error. Trino lists such tables too, and dbt lists a whole schema at once.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::information_schema::{INFORMATION_SCHEMA, InformationSchemaProvider};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList, SchemaProvider,
    Session, TableProvider,
};
use datafusion::common::ScalarValue;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;

/// Catalogs of the engine, each with its own `information_schema`.
#[derive(Debug, Default)]
pub struct CatalogList {
    catalogs: MemoryCatalogProviderList,
}

impl CatalogList {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CatalogProviderList for CatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalog = self.catalogs.catalog(name)?;
        // The engine's in-memory catalog folds names like Trino's `memory`.
        let catalog: Arc<dyn CatalogProvider> = if catalog.as_any().is::<MemoryCatalogProvider>() {
            Arc::new(LowerCaseCatalog { inner: catalog })
        } else {
            catalog
        };
        Some(Arc::new(WithInformationSchema {
            name: name.to_owned(),
            catalog,
        }))
    }
}

/// Trino folds identifiers to lower case, so its catalogs only have
/// lower-case schemas and tables (`CREATE TABLE "MiXeD"` creates `mixed`,
/// which `mixed` and `"MiXeD"` both name). `DataFusion`'s in-memory catalog
/// keeps names as written; this wrapper folds them at every entry
/// (registration and lookup). Its columns are folded where tables and views
/// are created (`crate::engine`).
#[derive(Debug)]
struct LowerCaseCatalog {
    inner: Arc<dyn CatalogProvider>,
}

impl CatalogProvider for LowerCaseCatalog {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let inner = self.inner.schema(&name.to_lowercase())?;
        Some(Arc::new(LowerCaseSchema { inner }))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(&name.to_lowercase(), schema)
    }

    fn deregister_schema(&self, name: &str, cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.deregister_schema(&name.to_lowercase(), cascade)
    }
}

/// A schema of [`LowerCaseCatalog`].
#[derive(Debug)]
struct LowerCaseSchema {
    inner: Arc<dyn SchemaProvider>,
}

#[async_trait]
impl SchemaProvider for LowerCaseSchema {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.table(&name.to_lowercase()).await
    }

    async fn table_type(&self, name: &str) -> Result<Option<TableType>> {
        self.inner.table_type(&name.to_lowercase()).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name.to_lowercase(), table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(&name.to_lowercase())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(&name.to_lowercase())
    }
}

/// A catalog plus its own `information_schema`.
#[derive(Debug)]
struct WithInformationSchema {
    name: String,
    catalog: Arc<dyn CatalogProvider>,
}

impl CatalogProvider for WithInformationSchema {
    /// The wrapped catalog, so callers can still downcast to its type.
    fn as_any(&self) -> &dyn Any {
        self.catalog.as_any()
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = self.catalog.schema_names();
        if !names.iter().any(|n| n == INFORMATION_SCHEMA) {
            names.push(INFORMATION_SCHEMA.to_owned());
        }
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA) {
            return Some(Arc::new(TrinoInformationSchema {
                name: self.name.clone(),
                catalog: Arc::new(Tolerant {
                    name: self.name.clone(),
                    catalog: Arc::clone(&self.catalog),
                }),
            }));
        }
        self.catalog.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA) {
            return Err(DataFusionError::Plan(format!("schema {name} is reserved")));
        }
        self.catalog.register_schema(name, schema)
    }

    fn deregister_schema(&self, name: &str, cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.catalog.deregister_schema(name, cascade)
    }
}

/// The catalog list an `information_schema` sees: its own catalog only.
#[derive(Debug)]
struct SingleCatalog {
    name: String,
    catalog: Arc<dyn CatalogProvider>,
}

impl CatalogProviderList for SingleCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None
    }

    fn catalog_names(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        (name == self.name).then(|| Arc::clone(&self.catalog))
    }
}

/// Trino 431's `information_schema` tables, in listing order.
const TABLES: [&str; 8] = [
    "applicable_roles",
    "columns",
    "enabled_roles",
    "roles",
    "schemata",
    "table_privileges",
    "tables",
    "views",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    ApplicableRoles,
    Columns,
    EnabledRoles,
    Roles,
    Schemata,
    TablePrivileges,
    Tables,
    Views,
}

impl Kind {
    fn of(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "applicable_roles" => Self::ApplicableRoles,
            "columns" => Self::Columns,
            "enabled_roles" => Self::EnabledRoles,
            "roles" => Self::Roles,
            "schemata" => Self::Schemata,
            "table_privileges" => Self::TablePrivileges,
            "tables" => Self::Tables,
            "views" => Self::Views,
            _ => return None,
        })
    }

    /// Column names and types observed on Trino 431; every column nullable.
    fn schema(self) -> SchemaRef {
        let text = |names: &[&str]| -> Vec<Field> {
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Utf8, true))
                .collect()
        };
        let fields = match self {
            Self::ApplicableRoles => text(&["grantee", "grantee_type", "role_name", "is_grantable"]),
            Self::EnabledRoles | Self::Roles => text(&["role_name"]),
            Self::Schemata => text(&["catalog_name", "schema_name"]),
            Self::TablePrivileges => text(&[
                "grantor",
                "grantor_type",
                "grantee",
                "grantee_type",
                "table_catalog",
                "table_schema",
                "table_name",
                "privilege_type",
                "is_grantable",
                "with_hierarchy",
            ]),
            Self::Tables => text(&["table_catalog", "table_schema", "table_name", "table_type"]),
            Self::Views => text(&["table_catalog", "table_schema", "table_name", "view_definition"]),
            Self::Columns => {
                let mut fields = text(&["table_catalog", "table_schema", "table_name", "column_name"]);
                fields.push(Field::new("ordinal_position", DataType::Int64, true));
                fields.extend(text(&["column_default", "is_nullable", "data_type"]));
                fields
            }
        };
        Arc::new(Schema::new(fields))
    }
}

/// A catalog's `information_schema`, shaped like Trino's (see the module
/// documentation).
#[derive(Debug)]
struct TrinoInformationSchema {
    name: String,
    catalog: Arc<dyn CatalogProvider>,
}

impl TrinoInformationSchema {
    /// `DataFusion`'s own tables (`df_settings`, …), for its `SHOW` statements.
    fn datafusion(&self) -> InformationSchemaProvider {
        InformationSchemaProvider::new(Arc::new(SingleCatalog {
            name: self.name.clone(),
            catalog: Arc::clone(&self.catalog),
        }))
    }
}

#[async_trait]
impl SchemaProvider for TrinoInformationSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        TABLES.iter().map(|t| (*t).to_owned()).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(kind) = Kind::of(name) else {
            return self.datafusion().table(name).await;
        };
        let rows = Rows {
            kind,
            schema: kind.schema(),
            catalog_name: self.name.clone(),
            catalog: Arc::clone(&self.catalog),
        };
        let table = StreamingTable::try_new(kind.schema(), vec![Arc::new(rows)])?;
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        Kind::of(name).is_some() || self.datafusion().table_exist(name)
    }
}

type Row = Vec<ScalarValue>;

/// Called with each relation's schema, name and provider (see
/// [`Rows::relations`]).
type Visit<'a> = dyn FnMut(&str, &str, Arc<dyn TableProvider>) -> Result<()> + Send + 'a;

fn text(value: &str) -> ScalarValue {
    ScalarValue::Utf8(Some(value.to_owned()))
}

const NULL_TEXT: ScalarValue = ScalarValue::Utf8(None);

/// The rows of one `information_schema` table, computed when scanned.
#[derive(Debug)]
struct Rows {
    kind: Kind,
    schema: SchemaRef,
    catalog_name: String,
    catalog: Arc<dyn CatalogProvider>,
}

impl PartitionStream for Rows {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> SendableRecordBatchStream {
        let rows = Self {
            kind: self.kind,
            schema: Arc::clone(&self.schema),
            catalog_name: self.catalog_name.clone(),
            catalog: Arc::clone(&self.catalog),
        };
        let schema = Arc::clone(&self.schema);
        let batch = futures::stream::once(async move { rows.batch(&context).await });
        Box::pin(RecordBatchStreamAdapter::new(schema, batch))
    }
}

/// `table_type` as Trino spells it.
fn table_type(schema: &str, table: &dyn TableProvider) -> &'static str {
    if schema != INFORMATION_SCHEMA && table.table_type() == TableType::View {
        "VIEW"
    } else {
        "BASE TABLE"
    }
}

impl Rows {
    /// Accumulates the rows, reserving their memory from the query's pool,
    /// and hands them over as one batch.
    async fn batch(&self, context: &TaskContext) -> Result<RecordBatch> {
        let reservation = MemoryConsumer::new(format!("information_schema.{:?}", self.kind))
            .register(context.memory_pool());
        let mut rows: Vec<Row> = Vec::new();
        self.collect(&mut |row: Row| {
            reservation.try_grow(row.iter().map(ScalarValue::size).sum())?;
            rows.push(row);
            Ok(())
        })
        .await?;
        let columns = (0..self.schema.fields().len())
            .map(|index| {
                let field = self.schema.field(index);
                if rows.is_empty() {
                    return Ok(datafusion::arrow::array::new_empty_array(field.data_type()));
                }
                ScalarValue::iter_to_array(rows.iter().map(|row| row[index].clone()))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    /// Schemas of the catalog, sorted, `information_schema` included.
    fn schemas(&self) -> Vec<(String, Arc<dyn SchemaProvider>)> {
        let mut schemas: Vec<(String, Arc<dyn SchemaProvider>)> = self
            .catalog
            .schema_names()
            .into_iter()
            .filter(|name| name != INFORMATION_SCHEMA)
            .filter_map(|name| self.catalog.schema(&name).map(|schema| (name, schema)))
            .collect();
        schemas.push((
            INFORMATION_SCHEMA.to_owned(),
            Arc::new(TrinoInformationSchema {
                name: self.catalog_name.clone(),
                catalog: Arc::clone(&self.catalog),
            }),
        ));
        schemas.sort_by(|a, b| a.0.cmp(&b.0));
        schemas
    }

    /// Every relation of the catalog, sorted, with its provider (a table the
    /// engine cannot read comes without columns, see `Tolerant`).
    async fn relations(&self, each: &mut Visit<'_>) -> Result<()> {
        for (schema_name, schema) in self.schemas() {
            let mut tables = schema.table_names();
            tables.sort();
            for table_name in tables {
                if let Some(table) = schema.table(&table_name).await? {
                    each(&schema_name, &table_name, table)?;
                }
            }
        }
        Ok(())
    }

    async fn collect(&self, emit: &mut (dyn FnMut(Row) -> Result<()> + Send)) -> Result<()> {
        let catalog = self.catalog_name.as_str();
        match self.kind {
            Kind::Schemata => {
                for (schema, _) in self.schemas() {
                    emit(vec![text(catalog), text(&schema)])?;
                }
            }
            Kind::Tables => {
                self.relations(&mut |schema, table, provider| {
                    let kind = table_type(schema, provider.as_ref());
                    emit(vec![text(catalog), text(schema), text(table), text(kind)])
                })
                .await?;
            }
            Kind::Views => {
                self.relations(&mut |schema, table, provider| {
                    if table_type(schema, provider.as_ref()) != "VIEW" {
                        return Ok(());
                    }
                    let definition = provider
                        .get_table_definition()
                        .map_or(NULL_TEXT, |sql| text(&crate::trino_sql::view_definition(sql)));
                    emit(vec![text(catalog), text(schema), text(table), definition])
                })
                .await?;
            }
            Kind::Columns => {
                self.relations(&mut |schema, table, provider| {
                    let view = table_type(schema, provider.as_ref()) == "VIEW";
                    for (position, field) in provider.schema().fields().iter().enumerate() {
                        // View columns are nullable in Trino.
                        let nullable = view || field.is_nullable();
                        emit(vec![
                            text(catalog),
                            text(schema),
                            text(table),
                            text(field.name()),
                            ScalarValue::Int64(i64::try_from(position + 1).ok()),
                            NULL_TEXT,
                            text(if nullable { "YES" } else { "NO" }),
                            text(&crate::trino_types::field_display_name(field)),
                        ])?;
                    }
                    Ok(())
                })
                .await?;
            }
            // No privileges nor roles: empty, as Trino's memory catalog.
            Kind::ApplicableRoles | Kind::EnabledRoles | Kind::Roles | Kind::TablePrivileges => {}
        }
        Ok(())
    }
}

/// The catalog as its `information_schema` sees it: a table whose provider
/// fails is listed as [`Unreadable`] instead of failing the listing.
#[derive(Debug)]
struct Tolerant {
    name: String,
    catalog: Arc<dyn CatalogProvider>,
}

impl CatalogProvider for Tolerant {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schema = self.catalog.schema(name)?;
        Some(Arc::new(TolerantSchema {
            catalog: self.name.clone(),
            name: name.to_owned(),
            schema,
        }))
    }
}

#[derive(Debug)]
struct TolerantSchema {
    catalog: String,
    name: String,
    schema: Arc<dyn SchemaProvider>,
}

#[async_trait]
impl SchemaProvider for TolerantSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.schema.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        match self.schema.table(name).await {
            Ok(table) => Ok(table),
            Err(err) => {
                tracing::warn!(
                    catalog = %self.catalog,
                    schema = %self.name,
                    table = %name,
                    error = %err,
                    "table listed without columns: its definition cannot be read"
                );
                Ok(Some(Arc::new(Unreadable {
                    reason: err.to_string(),
                    schema: Arc::new(Schema::empty()),
                })))
            }
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.schema.table_exist(name)
    }
}

/// A listed table whose definition cannot be read: no columns, and reading
/// it fails with the original error.
#[derive(Debug)]
struct Unreadable {
    reason: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for Unreadable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(self.reason.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int64Type;
    use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn ctx() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_information_schema(false))
            .with_catalog_list(Arc::new(CatalogList::new()))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        let other = MemoryCatalogProvider::new();
        other
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        ctx.register_catalog("other", Arc::new(other));
        ctx
    }

    async fn count(ctx: &SessionContext, sql: &str) -> i64 {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    }

    #[tokio::test]
    async fn each_catalog_lists_only_its_own_objects() {
        let ctx = ctx();
        ctx.sql("CREATE TABLE datafusion.public.a AS VALUES (1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        ctx.sql("CREATE TABLE other.public.b AS VALUES (1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for catalog in ["datafusion", "other"] {
            for (table, column) in [
                ("tables", "table_catalog"),
                ("columns", "table_catalog"),
                ("views", "table_catalog"),
                ("schemata", "catalog_name"),
            ] {
                let sql = format!(
                    "SELECT count(*) FROM {catalog}.information_schema.{table} WHERE {column} <> '{catalog}'"
                );
                assert_eq!(count(&ctx, &sql).await, 0, "{sql}");
            }
        }
        let own = "SELECT count(*) FROM other.information_schema.tables WHERE table_schema = 'public'";
        assert_eq!(count(&ctx, own).await, 1);
        let default = "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('a', 'b')";
        assert_eq!(
            count(&ctx, default).await,
            1,
            "unqualified: the session's catalog"
        );
        let columns = "SELECT count(*) FROM other.information_schema.columns WHERE table_name = 'b'";
        assert_eq!(count(&ctx, columns).await, 1);
    }

    async fn text_rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
        use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let options = FormatOptions::default().with_null("NULL");
            let formatters: Vec<ArrayFormatter<'_>> = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                rows.push(formatters.iter().map(|f| f.value(row).to_string()).collect());
            }
        }
        rows
    }

    /// Shapes and values observed on Trino 431 (`memory` catalog).
    #[tokio::test]
    async fn tables_are_shaped_like_trino() {
        let ctx = ctx();
        ctx.sql(
            "CREATE TABLE other.public.t (a BIGINT NOT NULL, h DECIMAL(10,2), j VARCHAR, \
             n TIMESTAMP(3), l INT[])",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        ctx.sql("CREATE VIEW other.public.v AS SELECT a, h FROM other.public.t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let columns = text_rows(
            &ctx,
            "SELECT * FROM other.information_schema.columns WHERE table_schema = 'public' \
             ORDER BY table_name, ordinal_position",
        )
        .await;
        let expected = [
            ["other", "public", "t", "a", "1", "NULL", "NO", "bigint"],
            ["other", "public", "t", "h", "2", "NULL", "YES", "decimal(10,2)"],
            ["other", "public", "t", "j", "3", "NULL", "YES", "varchar"],
            ["other", "public", "t", "n", "4", "NULL", "YES", "timestamp(3)"],
            ["other", "public", "t", "l", "5", "NULL", "YES", "array(integer)"],
            ["other", "public", "v", "a", "1", "NULL", "YES", "bigint"],
            ["other", "public", "v", "h", "2", "NULL", "YES", "decimal(10,2)"],
        ];
        assert_eq!(columns, expected);
        let names = |sql: &str| {
            let ctx = &ctx;
            let sql = sql.to_owned();
            async move {
                let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                batches[0]
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            }
        };
        assert_eq!(
            names("SELECT * FROM other.information_schema.columns").await,
            [
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "column_default",
                "is_nullable",
                "data_type"
            ]
        );
        assert_eq!(
            names("SELECT * FROM other.information_schema.views").await,
            ["table_catalog", "table_schema", "table_name", "view_definition"]
        );
        assert_eq!(
            names("SELECT * FROM other.information_schema.schemata").await,
            ["catalog_name", "schema_name"]
        );
        let tables = text_rows(
            &ctx,
            "SELECT * FROM other.information_schema.tables ORDER BY 2, 3",
        )
        .await;
        let expected: Vec<Vec<String>> = [
            ["other", "information_schema", "applicable_roles", "BASE TABLE"],
            ["other", "information_schema", "columns", "BASE TABLE"],
            ["other", "information_schema", "enabled_roles", "BASE TABLE"],
            ["other", "information_schema", "roles", "BASE TABLE"],
            ["other", "information_schema", "schemata", "BASE TABLE"],
            ["other", "information_schema", "table_privileges", "BASE TABLE"],
            ["other", "information_schema", "tables", "BASE TABLE"],
            ["other", "information_schema", "views", "BASE TABLE"],
            ["other", "public", "t", "BASE TABLE"],
            ["other", "public", "v", "VIEW"],
        ]
        .iter()
        .map(|r| r.iter().map(|c| (*c).to_owned()).collect())
        .collect();
        assert_eq!(tables, expected);
        let schemata = text_rows(&ctx, "SELECT * FROM other.information_schema.schemata ORDER BY 2").await;
        assert_eq!(schemata, [["other", "information_schema"], ["other", "public"]]);
        let views = text_rows(&ctx, "SELECT table_name FROM other.information_schema.views").await;
        assert_eq!(views, [["v"]]);
        for empty in ["table_privileges", "roles", "enabled_roles", "applicable_roles"] {
            let sql = format!("SELECT count(*) FROM other.information_schema.{empty}");
            assert_eq!(count(&ctx, &sql).await, 0, "{sql}");
        }
    }

    #[derive(Debug)]
    struct Broken;

    #[async_trait]
    impl SchemaProvider for Broken {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn table_names(&self) -> Vec<String> {
            vec!["bad".into(), "good".into()]
        }
        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            if name == "bad" {
                return Err(DataFusionError::NotImplemented(
                    "hive type `array<int>` is not supported".into(),
                ));
            }
            let schema = Arc::new(Schema::new(vec![datafusion::arrow::datatypes::Field::new(
                "x",
                datafusion::arrow::datatypes::DataType::Int64,
                true,
            )]));
            Ok(Some(Arc::new(datafusion::datasource::empty::EmptyTable::new(
                schema,
            ))))
        }
        fn table_exist(&self, name: &str) -> bool {
            name == "bad" || name == "good"
        }
    }

    #[tokio::test]
    async fn one_unreadable_table_does_not_break_the_listing() {
        let ctx = ctx();
        let lake = MemoryCatalogProvider::new();
        lake.register_schema("s", Arc::new(Broken)).unwrap();
        ctx.register_catalog("lake", Arc::new(lake));
        let tables = "SELECT count(*) FROM lake.information_schema.tables WHERE table_schema = 's'";
        assert_eq!(count(&ctx, tables).await, 2, "both tables are listed");
        let columns = "SELECT count(*) FROM lake.information_schema.columns WHERE table_schema = 's'";
        assert_eq!(count(&ctx, columns).await, 1, "the readable table's column");
        let err = ctx
            .sql("SELECT * FROM lake.s.bad")
            .await
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("array<int>"),
            "reading it keeps its own error: {err}"
        );
    }

    #[tokio::test]
    async fn show_statements_and_schema_listing_still_work() {
        let ctx = ctx();
        ctx.sql("SHOW TABLES").await.unwrap().collect().await.unwrap();
        ctx.sql("SHOW ALL").await.unwrap().collect().await.unwrap();
        let catalog = ctx.catalog("other").unwrap();
        assert!(catalog.schema_names().contains(&"information_schema".to_owned()));
        assert!(catalog.as_any().is::<MemoryCatalogProvider>());
        assert!(
            catalog
                .register_schema("information_schema", Arc::new(MemorySchemaProvider::new()))
                .is_err()
        );
    }
}
