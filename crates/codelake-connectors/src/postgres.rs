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
pub use codelake_core::config::{DecimalMapping, DecimalRoundingMode};
use codelake_core::{Connector, ConnectorError};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
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
    pub decimals: Decimals,
}

/// What a `numeric` without precision, or with more than 38 digits, is, and
/// how a value is rounded to its decimal's scale: the reference engine's
/// `decimal-mapping`, `decimal-rounding-mode` and `decimal-default-scale`
/// (by default such a column is left out, as there; ISS-060).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Decimals {
    pub mapping: DecimalMapping,
    pub rounding_mode: DecimalRoundingMode,
    /// 0 to 38.
    pub default_scale: u8,
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
        let source: Arc<dyn crate::postgres_pushdown::ZonedSource> = self.0.clone();
        crate::postgres_pushdown::table_provider(&pool, source, TableReference::partial(schema, table))
            .await
            .map(Some)
            .map_err(unsupported_as_not_implemented)
    }
}

/// A relation with a column of a type the connector does not read
/// (`jsonb`, `uuid`) is a feature `CodeLake` lacks, not a failing source:
/// `NotImplemented`, which a catalog listing (`system.jdbc`,
/// `information_schema`) lists without columns instead of failing whole. So
/// is a relation whose every column is left out, named `NOT_FOUND` as the
/// reference engine names it.
fn unsupported_as_not_implemented(error: DataFusionError) -> DataFusionError {
    use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::Error as DbError;
    let DataFusionError::External(inner) = &error else {
        return error;
    };
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(inner.as_ref());
    while let Some(current) = cause {
        if let Some(unsupported @ DbError::UnsupportedDataType { .. }) = current.downcast_ref::<DbError>() {
            return DataFusionError::NotImplemented(unsupported.to_string());
        }
        if let Some(empty) = current.downcast_ref::<catalog::NoSupportedColumns>() {
            return DataFusionError::NotImplemented(empty.to_string());
        }
        cause = current.source();
    }
    error
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::Error as DbError;

    /// The error a relation with a `jsonb` column raised: wrapped by the SQL table.
    #[derive(Debug)]
    struct Wrapped(DbError);

    impl fmt::Display for Wrapped {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Unable to get schema: {}", self.0)
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    fn unsupported() -> DbError {
        DbError::UnsupportedDataType {
            data_type: "jsonb".into(),
            field_name: "j".into(),
        }
    }

    #[test]
    fn an_unsupported_column_type_is_not_implemented() {
        for error in [
            DataFusionError::External(Box::new(unsupported())),
            DataFusionError::External(Box::new(Wrapped(unsupported()))),
        ] {
            match unsupported_as_not_implemented(error) {
                DataFusionError::NotImplemented(message) => {
                    assert!(message.contains("jsonb"), "{message}");
                }
                other => panic!("{other}"),
            }
        }
        // Any other failure stays what it is.
        let refused = DataFusionError::External(Box::new(DbError::UndefinedTable {
            table_name: "s.t".into(),
            source: "relation does not exist".into(),
        }));
        assert!(matches!(
            unsupported_as_not_implemented(refused),
            DataFusionError::External(_)
        ));
        assert!(matches!(
            unsupported_as_not_implemented(DataFusionError::Plan("x".into())),
            DataFusionError::Plan(_)
        ));
    }

    #[test]
    fn a_relation_without_a_supported_column_is_not_found() {
        let empty = DbError::UnableToGetSchema {
            source: Box::new(catalog::NoSupportedColumns::new("hub.totals", 2)),
        };
        match unsupported_as_not_implemented(DataFusionError::External(Box::new(Wrapped(empty)))) {
            DataFusionError::NotImplemented(message) => assert_eq!(
                message,
                "NOT_FOUND: Table 'hub.totals' has no supported columns (all 2 columns are not supported)"
            ),
            other => panic!("{other}"),
        }
    }
}
