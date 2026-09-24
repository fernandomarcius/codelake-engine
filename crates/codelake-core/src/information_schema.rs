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
//! `information_schema` schema, `DataFusion`'s own provider restricted to that
//! catalog. Statements that `DataFusion` plans over `information_schema`
//! (`SHOW TABLES`, `SHOW COLUMNS`) resolve it in the session's catalog.
//!
//! A listing never fails because of one table: a table whose provider cannot
//! be built (unreadable metadata, a layout the connector does not read) is
//! listed without columns and logged; reading it still fails with its own
//! error. Trino lists such tables too, and dbt lists a whole schema at once.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::information_schema::{INFORMATION_SCHEMA, InformationSchemaProvider};
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

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
        Some(Arc::new(WithInformationSchema {
            name: name.to_owned(),
            catalog,
        }))
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
            let only_this = Arc::new(SingleCatalog {
                name: self.name.clone(),
                catalog: Arc::new(Tolerant {
                    name: self.name.clone(),
                    catalog: Arc::clone(&self.catalog),
                }),
            });
            return Some(Arc::new(InformationSchemaProvider::new(only_this)));
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
