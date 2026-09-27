//! SQL Server connector (TDS 7.3+, SQL Server 2008 and later).
//!
//! Names follow Trino's convention: schemas, tables and columns are exposed in
//! lower case and mapped back to their real spelling in the SQL sent to the
//! server. Two objects whose names differ only by case are ambiguous and are
//! not exposed (logged at `warn`).

mod dialect;
mod executor;
mod pool;
mod table;
mod types;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use self::table::SqlServerTable;
use async_trait::async_trait;
use codelake_core::catalog::{FederatedCatalog, NameListing, TableResolver};
use codelake_core::{BoxError, Connector, ConnectorError};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion_federation::FederatedTableProviderAdaptor;
use datafusion_federation::sql::{SQLFederationProvider, SQLTableSource};
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

const LIST_COLUMNS: &str = "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, \
     CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION \
     FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2 \
     ORDER BY ORDINAL_POSITION";

/// Field metadata key holding the column's SQL Server type, e.g. `binary(10)`
/// or `datetime2(7)`. The pushdown policy relies on it.
pub(crate) const SQL_TYPE: &str = "sqlserver.type";

/// SQL Server type as written in DDL, from `INFORMATION_SCHEMA.COLUMNS`.
fn sql_type(data_type: &str, max_length: Option<i32>, datetime_precision: Option<i16>) -> String {
    let base = data_type.to_ascii_lowercase();
    match base.as_str() {
        "binary" | "varbinary" | "char" | "varchar" | "nchar" | "nvarchar" => match max_length {
            Some(-1) => format!("{base}(max)"),
            Some(n) => format!("{base}({n})"),
            None => base,
        },
        "datetime2" | "datetimeoffset" | "time" => match datetime_precision {
            Some(p) => format!("{base}({p})"),
            None => base,
        },
        _ => base,
    }
}

/// The bounded text type a column shows in results and keeps through
/// `CREATE TABLE AS`, as the reference maps SQL Server: `varchar(n)` and
/// `nvarchar(n)` are `varchar(n)`; `(max)`, `text` and `ntext` are unbounded.
fn declared_text(data_type: &str, max_length: Option<i32>) -> Option<String> {
    match (data_type.to_ascii_lowercase().as_str(), max_length) {
        ("varchar" | "nvarchar", Some(n)) if n > 0 => Some(format!("varchar({n})")),
        _ => None,
    }
}

/// Object names are rendered in `[...]`; `]` would end the quote. Such names
/// are not exposed instead of being escaped at every use site.
fn safe_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(']')
}

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
        if params.encrypt == "off" {
            tracing::warn!(source = %name, "encrypt = off: credentials and data travel in clear text");
        }
        if params.trust_server_certificate {
            tracing::warn!(source = %name, "trust_server_certificate = true: server identity is not verified");
        }
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
            if !safe_name(schema) || !safe_name(table) {
                tracing::warn!(source = %self.name, %schema, %table, "name contains `]`; not exposed");
                continue;
            }
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
    /// Arrow schema (lower-case names, SQL type in field metadata) and the real
    /// spelling of each exposed column.
    async fn schema_of(&self, schema: &str, table: &str) -> DfResult<(Schema, BTreeMap<String, String>)> {
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
        let mut fields: Vec<Field> = Vec::new();
        let mut real_names = BTreeMap::new();
        for row in rows {
            let column: &str = row.get(0).unwrap_or_default();
            let data_type: &str = row.get(1).unwrap_or_default();
            let precision: Option<u8> = row.get(2);
            let scale = row.get::<i32, _>(3).and_then(|s| i8::try_from(s).ok());
            let nullable = row.get::<&str, _>(4) != Some("NO");
            let max_length: Option<i32> = row.get(5);
            let datetime_precision: Option<i16> = row.get(6);
            if !safe_name(column) {
                tracing::warn!(source = %self.source, %schema, %table, %column, "name contains `]`; column not exposed");
                continue;
            }
            let Some(arrow) = types::arrow_type(data_type, precision, scale, datetime_precision) else {
                tracing::warn!(
                    source = %self.source, %schema, %table, %column, %data_type,
                    "unsupported column type; column not exposed"
                );
                continue;
            };
            let mut metadata = HashMap::from([(
                SQL_TYPE.to_owned(),
                sql_type(data_type, max_length, datetime_precision),
            )]);
            if let Some(declared) = declared_text(data_type, max_length) {
                metadata.insert(codelake_core::trino_types::TRINO_TYPE.to_owned(), declared);
            }
            let field = Field::new(column.to_lowercase(), arrow, nullable).with_metadata(metadata);
            real_names.insert(column.to_lowercase(), column.to_owned());
            fields.push(field);
        }
        let (fields, dropped) = unique_in_order(fields);
        for lower in dropped {
            real_names.remove(&lower);
            tracing::warn!(source = %self.source, %schema, %table, column = %lower, "columns differ only by case; not exposed");
        }
        Ok((Schema::new(fields), real_names))
    }
}

/// The fields in the order given (`ORDINAL_POSITION`: `SELECT *` and a
/// positional `UNION` depend on it), without the lower-case names that occur
/// more than once; those names are returned, sorted.
fn unique_in_order(fields: Vec<Field>) -> (Vec<Field>, Vec<String>) {
    let mut count: BTreeMap<String, usize> = BTreeMap::new();
    for field in &fields {
        *count.entry(field.name().clone()).or_default() += 1;
    }
    let kept = fields
        .into_iter()
        .filter(|field| count.get(field.name()) == Some(&1))
        .collect();
    let dropped = count
        .into_iter()
        .filter(|(_, n)| *n > 1)
        .map(|(name, _)| name)
        .collect();
    (kept, dropped)
}

#[async_trait]
impl TableResolver for SqlServerResolver {
    async fn resolve(&self, schema: &str, table: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some((real_schema, real_table)) = self.names.tables.get(&(schema.to_owned(), table.to_owned()))
        else {
            return Ok(None);
        };
        let (arrow_schema, real_names) = self.schema_of(real_schema, real_table).await?;
        let table = SqlServerTable::new(real_schema, real_table, Arc::new(arrow_schema), real_names);
        let source = SQLTableSource::new_with_table(Arc::clone(&self.provider), Arc::new(table));
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
        let target = executor::Target {
            host: self.params.host.clone(),
            port: self.params.port,
            database: self.params.database.clone(),
        };
        let executor =
            SqlServerExecutor::new(self.params.compute_context(), pool.clone(), BATCH_SIZE, target);
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

    #[test]
    fn bounded_text_declares_its_length() {
        assert_eq!(declared_text("varchar", Some(20)).as_deref(), Some("varchar(20)"));
        assert_eq!(
            declared_text("NVARCHAR", Some(50)).as_deref(),
            Some("varchar(50)")
        );
        assert_eq!(declared_text("varchar", Some(-1)), None, "varchar(max)");
        assert_eq!(declared_text("ntext", Some(1_073_741_823)), None);
        assert_eq!(
            declared_text("char", Some(3)),
            None,
            "char(n) keeps its own semantics"
        );
        assert_eq!(declared_text("int", None), None);
    }

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
    fn sql_types_carry_length_and_precision() {
        assert_eq!(sql_type("binary", Some(10), None), "binary(10)");
        assert_eq!(sql_type("VARBINARY", Some(-1), None), "varbinary(max)");
        assert_eq!(sql_type("datetime2", None, Some(7)), "datetime2(7)");
        assert_eq!(sql_type("int", None, None), "int");
    }

    #[test]
    fn columns_keep_the_source_order_without_case_duplicates() {
        use datafusion::arrow::datatypes::DataType;
        let field = |name: &str| Field::new(name, DataType::Int32, true);
        let (kept, dropped) = unique_in_order(vec![
            field("__$start_lsn"),
            field("zeta"),
            field("dup"),
            field("alpha"),
            field("dup"),
            field("__$command_id"),
        ]);
        let names: Vec<&str> = kept.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["__$start_lsn", "zeta", "alpha", "__$command_id"]);
        assert_eq!(dropped, ["dup"]);
    }

    #[test]
    fn bracket_in_names_is_refused() {
        assert!(safe_name("Contratos"));
        assert!(!safe_name("x]; DROP TABLE t;--"));
        assert!(!safe_name(""));
    }

    #[test]
    fn debug_never_prints_the_password() {
        let rendered = format!("{:?}", SqlServerConnector::new("erp1".into(), params()));
        assert!(!rendered.contains("super-secret-pw"), "{rendered}");
        assert!(rendered.contains("erp1"));
    }
}
