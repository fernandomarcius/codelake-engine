//! PostgreSQL (and PostgreSQL-wire sources such as Redshift, with dialect caveats).
//!
//! Tables are `datafusion-table-providers` SQL tables, federated through the
//! pushdown policy of `postgres_pushdown`, over this connector's own pool
//! (`pool`), names and column types (`catalog`, `types`). The catalog
//! follows the source: a relation created after boot is found when a query
//! names it (`codelake_core::catalog::LiveListing`).

mod catalog;
mod pool;
mod types;

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use codelake_core::catalog::{FederatedCatalog, LiveListing, TableResolver};
use codelake_core::{Connector, ConnectorError};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::error::Result as DfResult;
use datafusion::sql::TableReference;
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use secrecy::SecretString;
use tokio::sync::OnceCell;

use self::catalog::PgNames;
use self::pool::{Client, Param, PgPool};

/// How long a listing of the source stands before a listing of the catalog
/// (`information_schema`, `SHOW TABLES`) reads it again.
const LISTING_TTL: Duration = Duration::from_secs(1);

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
    pool: OnceCell<Arc<PgPool>>,
}

impl PostgresConnector {
    pub fn new(name: String, params: PostgresParams) -> Self {
        Self {
            name,
            params,
            pool: OnceCell::new(),
        }
    }

    async fn pool(&self) -> Result<Arc<PgPool>, ConnectorError> {
        self.pool
            .get_or_try_init(|| async {
                PgPool::open(&self.params)
                    .await
                    .map(Arc::new)
                    .map_err(|cause| ConnectorError::Unreachable {
                        source_name: self.name.clone(),
                        cause,
                    })
            })
            .await
            .cloned()
    }
}

struct PostgresResolver(Arc<PgPool>);

impl fmt::Debug for PostgresResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PostgresResolver")
    }
}

#[async_trait]
impl TableResolver for PostgresResolver {
    async fn resolve(&self, schema: &str, table: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let pool: Arc<dyn DbConnectionPool<Client, Param> + Send + Sync> = self.0.clone();
        crate::postgres_pushdown::table_provider(&pool, TableReference::partial(schema, table))
            .await
            .map(Some)
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
        let listing = LiveListing::read(Arc::new(PgNames::new(pool.clone())), LISTING_TTL)
            .await
            .map_err(|cause| ConnectorError::Metadata {
                source_name: self.name.clone(),
                cause: cause.into(),
            })?;
        Ok(Arc::new(FederatedCatalog::live(
            listing,
            Arc::new(PostgresResolver(pool)),
        )))
    }

    async fn ping(&self) -> Result<(), ConnectorError> {
        let pool = self.pool().await?;
        pool.client()
            .await
            .map(|_| ())
            .map_err(|cause| ConnectorError::Unreachable {
                source_name: self.name.clone(),
                cause,
            })
    }
}
