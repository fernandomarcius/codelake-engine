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

/// The schemas the login sees, as the reference lists them (the JDBC
/// driver's `getSchemas`: `sys.schemas`): the database roles' schemas
/// (`db_owner`, `db_datareader`, …), `guest` and `sys` included, and empty
/// ones too. `INFORMATION_SCHEMA` is the catalog's own (`codelake_core`).
const LIST_SCHEMAS: &str = "SELECT name FROM sys.schemas WHERE name <> 'INFORMATION_SCHEMA'";

/// The tables and views the login sees, as the reference lists them (the
/// JDBC driver's `getTables` for `TABLE` and `VIEW`, i.e. `sp_tables`):
/// `sys.all_objects` holds the system views of `sys` besides the user's
/// objects (`sys.objects`, what `INFORMATION_SCHEMA.TABLES` reads).
const LIST_TABLES: &str = "SELECT SCHEMA_NAME(schema_id), name FROM sys.all_objects \
     WHERE type IN ('U', 'V') AND SCHEMA_NAME(schema_id) <> 'INFORMATION_SCHEMA'";

/// The columns of a table or view, as `INFORMATION_SCHEMA.COLUMNS` describes
/// them (its definition, `OBJECT_DEFINITION(OBJECT_ID('INFORMATION_SCHEMA.COLUMNS'))`,
/// with what its internal `COLUMNPROPERTY` and `ODBCSCALE` compute written
/// out) over `sys.all_columns`: that view only reads `sys.columns`, which has
/// no column of a system view. For a user table it answers what that view
/// answers, type by type (compared on SQL Server 2022 over a table of every
/// type; the connector's integration suite reads every type through it).
const LIST_COLUMNS: &str = "SELECT c.name, ISNULL(TYPE_NAME(c.system_type_id), t.name), \
     CONVERT(tinyint, CASE WHEN c.system_type_id IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127) THEN c.precision END), \
     CONVERT(int, CASE WHEN c.system_type_id IN (48, 52, 56, 127) THEN 0 \
       WHEN c.system_type_id IN (60, 106, 108, 122) THEN c.scale END), \
     CONVERT(varchar(3), CASE c.is_nullable WHEN 1 THEN 'YES' ELSE 'NO' END), \
     CONVERT(int, CASE WHEN c.system_type_id IN (165, 167, 173, 175) THEN c.max_length \
       WHEN c.system_type_id IN (231, 239) THEN CASE WHEN c.max_length = -1 THEN -1 ELSE c.max_length / 2 END \
       WHEN c.system_type_id IN (34, 35) THEN 2147483647 WHEN c.system_type_id = 99 THEN 1073741823 \
       WHEN c.system_type_id = 241 THEN -1 END), \
     CONVERT(smallint, CASE WHEN c.system_type_id IN (40, 58) THEN 0 WHEN c.system_type_id = 61 THEN 3 \
       WHEN c.system_type_id IN (41, 42, 43) THEN c.scale END), \
     c.collation_name \
     FROM sys.all_columns c JOIN sys.all_objects o ON o.object_id = c.object_id \
     LEFT JOIN sys.types t ON t.user_type_id = c.user_type_id \
     WHERE o.type IN ('U', 'V') AND SCHEMA_NAME(o.schema_id) = @P1 AND o.name = @P2 \
     ORDER BY c.column_id";

/// Field metadata key holding the column's SQL Server type, e.g. `binary(10)`
/// or `datetime2(7)`. The pushdown policy relies on it.
pub(crate) const SQL_TYPE: &str = "sqlserver.type";

/// Field metadata key holding the collation of a text column, e.g.
/// `SQL_Latin1_General_CP1_CI_AS`. The pushdown policy relies on it.
pub(crate) const SQL_COLLATION: &str = "sqlserver.collation";

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

/// The type a column shows in results beyond its Arrow type, as the
/// reference maps SQL Server: `varchar(n)` and `nvarchar(n)` are
/// `varchar(n)` (kept through `CREATE TABLE AS`; `(max)`, `text` and
/// `ntext` are unbounded); `time(p)` is `time(p)` (7 without `p`), carried in
/// nanoseconds: it was `time(9)`, nine digits a client may refuse.
fn declared_text(
    data_type: &str,
    max_length: Option<i32>,
    datetime_precision: Option<i16>,
) -> Option<String> {
    match (data_type.to_ascii_lowercase().as_str(), max_length) {
        ("varchar" | "nvarchar", Some(n)) if n > 0 => Some(format!("varchar({n})")),
        ("time", _) => Some(format!("time({})", datetime_precision.unwrap_or(7).clamp(0, 7))),
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
    /// Every exposed schema, with tables or not.
    schemas: std::collections::BTreeSet<String>,
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
        let schemas = conn
            .client
            .simple_query(LIST_SCHEMAS)
            .await
            .map_err(|e| self.metadata(e))?
            .into_first_result()
            .await
            .map_err(|e| self.metadata(e))?;
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
        for row in schemas {
            let schema: &str = row.get(0).unwrap_or_default();
            if safe_name(schema) {
                names.schemas.insert(schema.to_lowercase());
            } else {
                tracing::warn!(source = %self.name, %schema, "name contains `]`; not exposed");
            }
        }
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
            let collation: Option<&str> = row.get(7);
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
            if let Some(declared) = declared_text(data_type, max_length, datetime_precision) {
                metadata.insert(codelake_core::trino_types::TRINO_TYPE.to_owned(), declared);
            }
            if let Some(collation) = collation {
                metadata.insert(SQL_COLLATION.to_owned(), collation.to_owned());
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
        let mut listing: BTreeMap<String, Vec<String>> = names
            .schemas
            .iter()
            .map(|schema| (schema.clone(), Vec::new()))
            .collect();
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
        assert_eq!(
            declared_text("varchar", Some(20), None).as_deref(),
            Some("varchar(20)")
        );
        assert_eq!(
            declared_text("NVARCHAR", Some(50), None).as_deref(),
            Some("varchar(50)")
        );
        assert_eq!(declared_text("varchar", Some(-1), None), None, "varchar(max)");
        assert_eq!(declared_text("ntext", Some(1_073_741_823), None), None);
        assert_eq!(
            declared_text("char", Some(3), None),
            None,
            "char(n) keeps its own semantics"
        );
        assert_eq!(declared_text("int", None, None), None);
        assert_eq!(declared_text("datetime2", None, Some(7)), None);
    }

    #[test]
    fn times_declare_their_precision() {
        assert_eq!(declared_text("time", None, Some(3)).as_deref(), Some("time(3)"));
        assert_eq!(declared_text("TIME", None, Some(0)).as_deref(), Some("time(0)"));
        assert_eq!(declared_text("time", None, None).as_deref(), Some("time(7)"));
        assert_eq!(declared_text("time", None, Some(9)).as_deref(), Some("time(7)"));
        assert_eq!(declared_text("time", None, Some(-1)).as_deref(), Some("time(0)"));
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
