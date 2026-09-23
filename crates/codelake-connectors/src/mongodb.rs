//! `MongoDB`: collections are exposed as tables of schema `public`; the schema of
//! each collection is inferred by sampling documents. Filters and projections are
//! pushed down; joins run in the engine.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use codelake_core::catalog::{FederatedCatalog, NameListing, TableResolver};
use codelake_core::{Connector, ConnectorError};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::sql::TableReference;
use datafusion_table_providers::mongodb::MongoDBTableFactory;
use datafusion_table_providers::mongodb::connection_pool::MongoDBConnectionPool;
use secrecy::SecretString;
use tokio::sync::OnceCell;

use crate::params::map;

/// Schema name under which collections are listed.
pub const SCHEMA: &str = "public";

#[derive(Debug)]
pub struct MongoConnector {
    name: String,
    connection_string: SecretString,
    pool: OnceCell<Arc<MongoDBConnectionPool>>,
}

impl MongoConnector {
    pub fn new(name: String, connection_string: SecretString) -> Self {
        Self {
            name,
            connection_string,
            pool: OnceCell::new(),
        }
    }

    fn unreachable(&self, cause: impl Into<codelake_core::BoxError>) -> ConnectorError {
        ConnectorError::Unreachable {
            source_name: self.name.clone(),
            cause: cause.into(),
        }
    }

    async fn pool(&self) -> Result<Arc<MongoDBConnectionPool>, ConnectorError> {
        self.pool
            .get_or_try_init(|| async {
                let params = map([("connection_string", self.connection_string.clone())]);
                MongoDBConnectionPool::new(params)
                    .await
                    .map(Arc::new)
                    .map_err(|e| self.unreachable(e))
            })
            .await
            .cloned()
    }

    async fn collections(&self) -> Result<Vec<String>, ConnectorError> {
        let pool = self.pool().await?;
        let connection = pool.connect().await.map_err(|e| self.unreachable(e))?;
        connection.tables().await.map_err(|e| ConnectorError::Metadata {
            source_name: self.name.clone(),
            cause: e.into(),
        })
    }
}

struct MongoResolver(MongoDBTableFactory);

impl fmt::Debug for MongoResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MongoResolver")
    }
}

#[async_trait]
impl TableResolver for MongoResolver {
    async fn resolve(&self, _schema: &str, table: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        self.0
            .table_provider(TableReference::bare(table))
            .await
            .map(Some)
            .map_err(DataFusionError::External)
    }
}

#[async_trait]
impl Connector for MongoConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "mongodb"
    }

    async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
        let collections = self.collections().await?;
        let listing = NameListing::new(BTreeMap::from([(SCHEMA.to_owned(), collections)]));
        let resolver = MongoResolver(MongoDBTableFactory::new(self.pool().await?));
        Ok(Arc::new(FederatedCatalog::new(
            Arc::new(listing),
            Arc::new(resolver),
        )))
    }

    async fn ping(&self) -> Result<(), ConnectorError> {
        self.collections().await.map(|_| ())
    }
}
