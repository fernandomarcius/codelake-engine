//! PostgreSQL (and PostgreSQL-wire sources such as Redshift, with dialect caveats).

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use codelake_core::catalog::{FederatedCatalog, TableResolver};
use codelake_core::{Connector, ConnectorError};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::sql::TableReference;
use datafusion_table_providers_common::common::DatabaseCatalogProvider;
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_postgres::PostgresTableFactory;
use datafusion_table_providers_postgres::pool::PostgresConnectionPool;
use secrecy::SecretString;
use tokio::sync::OnceCell;

use crate::params::{map, plain};

#[derive(Debug, Clone)]
pub struct PostgresParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: SecretString,
    pub sslmode: String,
}

#[derive(Debug)]
pub struct PostgresConnector {
    name: String,
    params: PostgresParams,
    pool: OnceCell<Arc<PostgresConnectionPool>>,
}

impl PostgresConnector {
    pub fn new(name: String, params: PostgresParams) -> Self {
        Self {
            name,
            params,
            pool: OnceCell::new(),
        }
    }

    async fn pool(&self) -> Result<Arc<PostgresConnectionPool>, ConnectorError> {
        self.pool
            .get_or_try_init(|| async {
                let p = &self.params;
                let params = map([
                    ("host", plain(&p.host)),
                    ("port", plain(p.port.to_string())),
                    ("db", plain(&p.database)),
                    ("user", plain(&p.user)),
                    ("pass", p.password.clone()),
                    ("sslmode", plain(&p.sslmode)),
                    ("application_name", plain("codelake")),
                ]);
                PostgresConnectionPool::new(params)
                    .await
                    .map(Arc::new)
                    .map_err(|e| ConnectorError::Unreachable {
                        source_name: self.name.clone(),
                        cause: e.into(),
                    })
            })
            .await
            .cloned()
    }
}

struct PostgresResolver(PostgresTableFactory);

impl fmt::Debug for PostgresResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresResolver")
    }
}

#[async_trait]
impl TableResolver for PostgresResolver {
    async fn resolve(&self, schema: &str, table: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        self.0
            .table_provider(TableReference::partial(schema, table))
            .await
            .map(Some)
            .map_err(DataFusionError::External)
    }
}

#[async_trait]
impl Connector for PostgresConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "postgres"
    }

    async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
        let pool = self.pool().await?;
        let listing = DatabaseCatalogProvider::try_new(pool.clone())
            .await
            .map_err(|cause| ConnectorError::Metadata {
                source_name: self.name.clone(),
                cause,
            })?;
        let resolver = PostgresResolver(PostgresTableFactory::new(pool));
        Ok(Arc::new(FederatedCatalog::new(
            Arc::new(listing),
            Arc::new(resolver),
        )))
    }

    async fn ping(&self) -> Result<(), ConnectorError> {
        let pool = self.pool().await?;
        pool.connect()
            .await
            .map(|_| ())
            .map_err(|cause| ConnectorError::Unreachable {
                source_name: self.name.clone(),
                cause,
            })
    }
}
