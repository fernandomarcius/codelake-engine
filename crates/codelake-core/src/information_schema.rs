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

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::information_schema::{INFORMATION_SCHEMA, InformationSchemaProvider};
use datafusion::catalog::{CatalogProvider, CatalogProviderList, MemoryCatalogProviderList, SchemaProvider};
use datafusion::error::{DataFusionError, Result};

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
                catalog: Arc::clone(&self.catalog),
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
