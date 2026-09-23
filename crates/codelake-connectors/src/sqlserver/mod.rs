//! SQL Server connector (TDS 7.3+, SQL Server 2008 and later).
//!
//! Names follow Trino's convention: schemas, tables and columns are exposed in
//! lower case and mapped back to their real spelling in the SQL sent to the
//! server. Two objects whose names differ only by case are ambiguous and are
//! not exposed (logged at `warn`).

mod dialect;
mod executor;
mod pool;
mod types;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use codelake_core::catalog::{FederatedCatalog, NameListing, TableResolver};
use codelake_core::{BoxError, Connector, ConnectorError};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::sql::TableReference;
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::{RemoteTableRef, SQLFederationProvider, SQLTableSource};
use secrecy::{ExposeSecret, SecretString};
use tiberius::{AuthMethod, Config, EncryptionLevel, Query};
use tokio::sync::OnceCell;

use self::executor::SqlServerExecutor;
use self::pool::{Manager, Pool};

/// Rows per Arrow batch read from SQL Server.
const BATCH_SIZE: usize = 8192;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

const LIST_TABLES: &str = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
     WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA') \
     AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')";

const LIST_COLUMNS: &str = "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE \
     FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 \
     ORDER BY ORDINAL_POSITION";

#[derive(Debug, Clone)]
pub struct SqlServerParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: SecretString,
    /// `required`, `on` or `off`.
    pub encrypt: String,
    pub trust_server_certificate: bool,
    pub pool_size: u32,
}

impl SqlServerParams {
    fn tiberius_config(&self) -> Config {
        let mut config = Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.application_name("codelake");
        config.authentication(AuthMethod::sql_server(&self.user, self.password.expose_secret()));
        config.encryption(match self.encrypt.as_str() {
            "off" => EncryptionLevel::Off,
            "on" => EncryptionLevel::On,
            _ => EncryptionLevel::Required,
        });
        if self.trust_server_certificate {
            config.trust_cert();
        }
        config
    }

    /// Identifies the remote database: sub-plans of sources with the same
    /// context may be pushed down together as one statement.
    fn compute_context(&self) -> String {
        format!(
            "sqlserver://{}@{}:{}/{}",
            self.user, self.host, self.port, self.database
        )
    }
}

/// Real names of the exposed (lower-case) schemas and tables.
#[derive(Debug, Default)]
struct NameMap {
    tables: HashMap<(String, String), (String, String)>,
}

pub struct SqlServerConnector {
    name: String,
    params: SqlServerParams,
    pool: OnceCell<Pool>,
}

impl fmt::Debug for SqlServerConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlServerConnector")
            .field("name", &self.name)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl SqlServerConnector {
    pub fn new(name: String, params: SqlServerParams) -> Self {
        Self {
            name,
            params,
            pool: OnceCell::new(),
        }
    }

    fn unreachable(&self, cause: impl Into<BoxError>) -> ConnectorError {
        ConnectorError::Unreachable {
            source_name: self.name.clone(),
            cause: cause.into(),
        }
    }

    fn metadata(&self, cause: impl Into<BoxError>) -> ConnectorError {
        ConnectorError::Metadata {
            source_name: self.name.clone(),
            cause: cause.into(),
        }
    }

    async fn pool(&self) -> Result<Pool, ConnectorError> {
        self.pool
            .get_or_try_init(|| async {
                bb8::Pool::builder()
                    .max_size(self.params.pool_size)
                    .connection_timeout(CONNECT_TIMEOUT)
                    .test_on_check_out(true)
                    .build(Manager::new(self.params.tiberius_config()))
                    .await
                    .map_err(|e| self.unreachable(e))
            })
            .await
            .cloned()
    }

    async fn list_tables(&self, pool: &Pool) -> Result<NameMap, ConnectorError> {
        let mut conn = pool.get().await.map_err(|e| self.unreachable(format!("{e:?}")))?;
        let rows = conn
            .client
            .simple_query(LIST_TABLES)
            .await
            .map_err(|e| self.metadata(e))?
            .into_first_result()
            .await
            .map_err(|e| self.metadata(e))?;
        let mut seen: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
        for row in rows {
            let schema: &str = row.get(0).unwrap_or_default();
            let table: &str = row.get(1).unwrap_or_default();
            seen.entry((schema.to_lowercase(), table.to_lowercase()))
                .or_default()
                .push((schema.to_owned(), table.to_owned()));
        }
        let mut names = NameMap::default();
        for (lower, mut real) in seen {
            if real.len() == 1 {
                names.tables.insert(lower, real.remove(0));
            } else {
                tracing::warn!(source = %self.name, ?real, "tables differ only by case; not exposed");
            }
        }
        Ok(names)
    }
}

struct SqlServerResolver {
    source: String,
    pool: Pool,
    provider: Arc<SQLFederationProvider>,
    names: NameMap,
}

impl fmt::Debug for SqlServerResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlServerResolver")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

fn metadata_error(source: &str, cause: impl fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("sqlserver `{source}`: {cause}"))
}

impl SqlServerResolver {
    async fn schema_of(&self, schema: &str, table: &str) -> DfResult<Schema> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| metadata_error(&self.source, format!("{e:?}")))?;
        let mut query = Query::new(LIST_COLUMNS);
        query.bind(schema);
        query.bind(table);
        let rows = query
            .query(&mut conn.client)
            .await
            .map_err(|e| metadata_error(&self.source, e))?
            .into_first_result()
            .await
            .map_err(|e| metadata_error(&self.source, e))?;
        let mut fields = Vec::with_capacity(rows.len());
        for row in rows {
            let column: &str = row.get(0).unwrap_or_default();
            let data_type: &str = row.get(1).unwrap_or_default();
            let precision: Option<u8> = row.get(2);
            let scale: Option<i32> = row.get(3);
            let nullable = row.get::<&str, _>(4) != Some("NO");
            let scale = scale.and_then(|s| i8::try_from(s).ok());
            if let Some(arrow) = types::arrow_type(data_type, precision, scale) {
                fields.push(Field::new(column.to_lowercase(), arrow, nullable));
            } else {
                tracing::warn!(
                    source = %self.source, %schema, %table, %column, %data_type,
                    "unsupported column type; column not exposed"
                );
            }
        }
        Ok(Schema::new(fields))
    }
}

#[async_trait]
impl TableResolver for SqlServerResolver {
    async fn resolve(&self, schema: &str, table: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some((real_schema, real_table)) = self.names.tables.get(&(schema.to_owned(), table.to_owned()))
        else {
            return Ok(None);
        };
        let arrow_schema = Arc::new(self.schema_of(real_schema, real_table).await?);
        let remote = RemoteTableRef::from(TableReference::partial(real_schema.as_str(), real_table.as_str()));
        let source = SQLTableSource::new_with_schema(Arc::clone(&self.provider), remote, arrow_schema);
        Ok(Some(Arc::new(FederatedTableProviderAdaptor::new(Arc::new(
            source,
        )))))
    }
}

#[async_trait]
impl Connector for SqlServerConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "sqlserver"
    }

    async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
        let pool = self.pool().await?;
        let names = self.list_tables(&pool).await?;
        let mut listing: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (schema, table) in names.tables.keys() {
            listing.entry(schema.clone()).or_default().push(table.clone());
        }
        for tables in listing.values_mut() {
            tables.sort();
        }
        let executor = SqlServerExecutor::new(self.params.compute_context(), pool.clone(), BATCH_SIZE);
        let resolver = SqlServerResolver {
            source: self.name.clone(),
            pool,
            provider: Arc::new(SQLFederationProvider::new(Arc::new(executor))),
            names,
        };
        Ok(Arc::new(FederatedCatalog::new(
            Arc::new(NameListing::new(listing)),
            Arc::new(resolver),
        )))
    }

    async fn ping(&self) -> Result<(), ConnectorError> {
        let pool = self.pool().await?;
        let mut conn = pool.get().await.map_err(|e| self.unreachable(format!("{e:?}")))?;
        conn.client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| self.unreachable(e))?
            .into_row()
            .await
            .map_err(|e| self.unreachable(e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> SqlServerParams {
        SqlServerParams {
            host: "erp.internal".into(),
            port: 1433,
            database: "ERP".into(),
            user: "reader".into(),
            password: SecretString::from("super-secret-pw"),
            encrypt: "required".into(),
            trust_server_certificate: false,
            pool_size: 4,
        }
    }

    #[test]
    fn compute_context_identifies_server_database_and_user() {
        assert_eq!(
            params().compute_context(),
            "sqlserver://reader@erp.internal:1433/ERP"
        );
    }

    #[test]
    fn config_carries_address() {
        assert_eq!(params().tiberius_config().get_addr(), "erp.internal:1433");
    }

    #[test]
    fn debug_never_prints_the_password() {
        let rendered = format!("{:?}", SqlServerConnector::new("erp1".into(), params()));
        assert!(!rendered.contains("super-secret-pw"), "{rendered}");
        assert!(rendered.contains("erp1"));
    }
}
