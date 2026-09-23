//! Catalog adapter that keeps listing metadata but resolves tables through a
//! connector-specific [`TableResolver`].
//!
//! Why it exists: generic catalog providers from `datafusion-table-providers`
//! hand out plain `SqlTable`s, which are *not* federated, so a join between two
//! tables of the same Postgres would pull both tables over the network. Routing
//! resolution through the connector's table factory returns federated providers,
//! and the whole sub-plan is pushed down to the source instead.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result;

/// Builds the provider for one table of a source.
#[async_trait]
pub trait TableResolver: Send + Sync + Debug {
    async fn resolve(&self, schema: &str, table: &str) -> Result<Option<Arc<dyn TableProvider>>>;
}

/// Catalog whose names come from `listing` and whose tables come from `resolver`.
#[derive(Debug)]
pub struct FederatedCatalog {
    listing: Arc<dyn CatalogProvider>,
    resolver: Arc<dyn TableResolver>,
}

impl FederatedCatalog {
    pub fn new(listing: Arc<dyn CatalogProvider>, resolver: Arc<dyn TableResolver>) -> Self {
        Self { listing, resolver }
    }
}

impl CatalogProvider for FederatedCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.listing.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.listing.schema(name).map(|listing| {
            Arc::new(FederatedSchema {
                name: name.to_owned(),
                listing,
                resolver: Arc::clone(&self.resolver),
            }) as Arc<dyn SchemaProvider>
        })
    }
}

/// Listing built from plain names, for sources without a native catalog provider
/// (`MongoDB` collections, for instance). It only answers "what exists"; tables are
/// always produced by the resolver of the wrapping [`FederatedCatalog`].
#[derive(Debug, Default)]
pub struct NameListing {
    schemas: BTreeMap<String, Arc<NameSchema>>,
}

impl NameListing {
    pub fn new(schemas: BTreeMap<String, Vec<String>>) -> Self {
        Self {
            schemas: schemas
                .into_iter()
                .map(|(name, tables)| (name, Arc::new(NameSchema { tables })))
                .collect(),
        }
    }
}

impl CatalogProvider for NameListing {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .map(|schema| Arc::clone(schema) as Arc<dyn SchemaProvider>)
    }
}

#[derive(Debug)]
struct NameSchema {
    tables: Vec<String>,
}

#[async_trait]
impl SchemaProvider for NameSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.clone()
    }

    async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(None)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t == name)
    }
}

#[derive(Debug)]
struct FederatedSchema {
    name: String,
    listing: Arc<dyn SchemaProvider>,
    resolver: Arc<dyn TableResolver>,
}

#[async_trait]
impl SchemaProvider for FederatedSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.listing.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if !self.listing.table_exist(name) {
            return Ok(None);
        }
        self.resolver.resolve(&self.name, name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.listing.table_exist(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::datasource::empty::EmptyTable;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct RecordingResolver {
        calls: Mutex<Vec<(String, String)>>,
    }

    fn table_with_column(name: &str) -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Int64,
            false,
        )]))))
    }

    #[async_trait]
    impl TableResolver for RecordingResolver {
        async fn resolve(&self, schema: &str, table: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            if let Ok(mut calls) = self.calls.lock() {
                calls.push((schema.to_owned(), table.to_owned()));
            }
            Ok(Some(table_with_column("resolved")))
        }
    }

    fn listing() -> Arc<dyn CatalogProvider> {
        let schema = MemorySchemaProvider::new();
        schema
            .register_table("orders".into(), table_with_column("listed"))
            .unwrap();
        let catalog = MemoryCatalogProvider::new();
        catalog.register_schema("public", Arc::new(schema)).unwrap();
        Arc::new(catalog)
    }

    #[tokio::test]
    async fn tables_come_from_the_resolver_not_the_listing() {
        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(listing(), resolver.clone());

        assert_eq!(catalog.schema_names(), vec!["public"]);
        let schema = catalog.schema("public").unwrap();
        assert_eq!(schema.table_names(), vec!["orders"]);
        assert!(schema.table_exist("orders"));

        let table = schema.table("orders").await.unwrap().unwrap();
        assert_eq!(table.schema().field(0).name(), "resolved");
        assert_eq!(
            *resolver.calls.lock().unwrap(),
            vec![("public".to_owned(), "orders".to_owned())]
        );
    }

    #[tokio::test]
    async fn name_listing_feeds_the_resolver() {
        let listing = NameListing::new(BTreeMap::from([(
            "public".to_owned(),
            vec!["events".to_owned(), "users".to_owned()],
        )]));
        assert!(
            listing
                .schema("public")
                .unwrap()
                .table("events")
                .await
                .unwrap()
                .is_none()
        );

        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(Arc::new(listing), resolver.clone());
        assert_eq!(catalog.schema_names(), vec!["public"]);
        let schema = catalog.schema("public").unwrap();
        assert_eq!(schema.table_names(), vec!["events", "users"]);
        assert!(schema.table("users").await.unwrap().is_some());
        assert!(schema.table("ghost").await.unwrap().is_none());
        assert_eq!(resolver.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unknown_tables_never_reach_the_source() {
        let resolver = Arc::new(RecordingResolver::default());
        let catalog = FederatedCatalog::new(listing(), resolver.clone());
        let schema = catalog.schema("public").unwrap();

        assert!(schema.table("missing").await.unwrap().is_none());
        assert!(!schema.table_exist("missing"));
        assert!(catalog.schema("nope").is_none());
        assert!(resolver.calls.lock().unwrap().is_empty());
    }
}
