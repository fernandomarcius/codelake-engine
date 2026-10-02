//! Connections to PostgreSQL for the table providers of
//! `datafusion-table-providers`.
//!
//! The upstream pool runs `SET json_serialization_enable …` (a Redshift
//! setting) on every connection it opens; PostgreSQL refuses it and logs an
//! `ERROR` on the server for each connection (ISS-060). This pool opens plain
//! sessions; its connections read rows with the upstream row decoder.
//!
//! Every statement runs in the time zone of the engine session it answers
//! (the client's `X-Trino-Time-Zone` or `SET TIME ZONE`, UTC without one),
//! never in the server's or the role's default: a view that reads
//! `current_date` or `now()` answers the client's day. The connection sets
//! its `TimeZone` before each statement, so a pooled connection carries no
//! zone from one statement to the next. The reference engine runs its
//! sessions in its JVM's zone (`Etc/UTC`) whatever the client's: ISS-060.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use bb8_postgres::tokio_postgres::config::SslMode;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::{Config, Row};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::TableReference;
use datafusion_table_providers_common::sql::db_connection_pool::dbconnection::{
    AsyncDbConnection, DbConnection, Error as DbError, GenericError,
};
use datafusion_table_providers_common::sql::db_connection_pool::{DbConnectionPool, JoinPushDown};
use datafusion_table_providers_postgres::arrow_sql_gen::rows_to_arrow;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt, stream};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use secrecy::ExposeSecret;

use super::PostgresParams;
use super::catalog;
use super::types;

type Manager = PostgresConnectionManager<MakeTlsConnector>;

/// A pooled connection.
pub(super) type Client = bb8::PooledConnection<'static, Manager>;

/// A statement parameter.
pub(super) type Param = &'static (dyn ToSql + Sync);

/// Rows decoded per batch.
const ROWS_PER_BATCH: usize = 4_000;

/// Connections the pool keeps at most (the upstream pool's size).
const POOL_SIZE: u32 = 10;

#[derive(Clone)]
pub(super) struct PgPool {
    pool: bb8::Pool<Manager>,
    /// Sources with the same server, database and user may be joined at the
    /// source (the upstream pool's rule).
    join_context: String,
}

impl fmt::Debug for PgPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgPool")
            .field("join_context", &self.join_context)
            .finish_non_exhaustive()
    }
}

/// TLS as `sslmode` asks: `disable` none; `prefer` when the server offers it;
/// `require` without checking the certificate; `verify-ca` checks the
/// certificate, `verify-full` also the host name.
fn tls(sslmode: &str) -> Result<(SslMode, MakeTlsConnector), GenericError> {
    let mode = match sslmode.to_ascii_lowercase().as_str() {
        "disable" => SslMode::Disable,
        "prefer" => SslMode::Prefer,
        "require" | "verify-ca" | "verify-full" => SslMode::Require,
        other => {
            return Err(format!(
                "sslmode {other:?} is not one of disable, prefer, require, verify-ca, verify-full"
            )
            .into());
        }
    };
    let verify_full = sslmode.eq_ignore_ascii_case("verify-full");
    let verify_ca = verify_full || sslmode.eq_ignore_ascii_case("verify-ca");
    let connector = TlsConnector::builder()
        .danger_accept_invalid_hostnames(!verify_full)
        .danger_accept_invalid_certs(!verify_ca)
        .build()?;
    Ok((mode, MakeTlsConnector::new(connector)))
}

impl PgPool {
    /// Opens the pool and one connection, so a source that cannot be reached
    /// fails here.
    pub(super) async fn open(params: &PostgresParams) -> Result<Self, GenericError> {
        let (mode, connector) = tls(&params.sslmode)?;
        let mut config = Config::new();
        config
            .host(&params.host)
            .port(params.port)
            .dbname(&params.database)
            .user(&params.user)
            .password(params.password.expose_secret())
            .application_name("codelake")
            .ssl_mode(mode);
        let pool = bb8::Pool::builder()
            .max_size(POOL_SIZE)
            .build(PostgresConnectionManager::new(config, connector))
            .await?;
        pool.get()
            .await
            .map_err(pool_error)?
            .simple_query("SELECT 1")
            .await?;
        Ok(Self {
            pool,
            join_context: format!(
                "host={},port={},db={},user={},",
                params.host, params.port, params.database, params.user
            ),
        })
    }

    pub(super) async fn client(&self) -> Result<Client, GenericError> {
        self.pool.get_owned().await.map_err(pool_error)
    }
}

fn pool_error(error: bb8::RunError<bb8_postgres::tokio_postgres::Error>) -> GenericError {
    match error {
        bb8::RunError::User(error) => Box::new(error),
        bb8::RunError::TimedOut => "timed out waiting for a PostgreSQL connection".into(),
    }
}

impl crate::postgres_pushdown::ZonedSource for PgPool {
    fn query(
        &self,
        time_zone: Option<String>,
        sql: String,
        schema: SchemaRef,
    ) -> BoxFuture<'static, Result<SendableRecordBatchStream, GenericError>> {
        let pool = self.clone();
        Box::pin(async move {
            let connection = PgConnection {
                client: pool.client().await?,
                time_zone,
            };
            connection.query_arrow(&sql, &[], Some(schema)).await
        })
    }
}

/// The statement setting a session's `TimeZone` to `zone`, the engine
/// session's zone as Trino spells it (`UTC`, `-03:00`, `America/Sao_Paulo`;
/// `None`: the engine's, UTC). An offset is an interval: as text PostgreSQL
/// reads it as a POSIX zone, east and west swapped. UTC is `Etc/UTC`, the
/// name the reference engine's sessions use.
pub(super) fn zone_statement(zone: Option<&str>) -> String {
    match zone.unwrap_or("UTC") {
        "UTC" => "SET TIME ZONE 'Etc/UTC'".to_owned(),
        offset
            if offset.starts_with(['+', '-'])
                && offset[1..].bytes().all(|b| b.is_ascii_digit() || b == b':') =>
        {
            format!("SET TIME ZONE INTERVAL '{offset}' HOUR TO MINUTE")
        }
        name => format!("SET TIME ZONE '{}'", name.replace('\'', "''")),
    }
}

#[async_trait]
impl DbConnectionPool<Client, Param> for PgPool {
    async fn connect(&self) -> Result<Box<dyn DbConnection<Client, Param>>, GenericError> {
        Ok(Box::new(PgConnection::new(self.client().await?)))
    }

    fn join_push_down(&self) -> JoinPushDown {
        JoinPushDown::AllowedFor(self.join_context.clone())
    }
}

pub(super) struct PgConnection {
    client: Client,
    /// The engine session's zone its statements run in ([`zone_statement`]).
    time_zone: Option<String>,
}

impl PgConnection {
    /// Sets the session's `TimeZone` for the next statement.
    async fn in_session_zone(&self) -> Result<(), GenericError> {
        self.client
            .batch_execute(&zone_statement(self.time_zone.as_deref()))
            .await
            .map_err(db_error)
    }
}

impl DbConnection<Client, Param> for PgConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Client, Param>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<Client, Param> for PgConnection {
    fn new(client: Client) -> Self {
        Self {
            client,
            time_zone: None,
        }
    }

    async fn tables(&self, schema: &str) -> Result<Vec<String>, DbError> {
        let mut listing = catalog::list(&self.client)
            .await
            .map_err(|source| DbError::UnableToGetTables { source })?;
        Ok(listing.remove(schema).unwrap_or_default())
    }

    async fn schemas(&self) -> Result<Vec<String>, DbError> {
        let listing = catalog::list(&self.client)
            .await
            .map_err(|source| DbError::UnableToGetSchemas { source })?;
        Ok(listing.into_keys().collect())
    }

    async fn get_schema(&self, table: &TableReference) -> Result<SchemaRef, DbError> {
        let schema = table.schema().unwrap_or("public");
        catalog::schema_of(&self.client, schema, table.table())
            .await
            .map(Arc::new)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[Param],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream, GenericError> {
        self.in_session_zone().await?;
        let sql = types::readable(sql, projected_schema.as_deref());
        let rows = self
            .client
            .query_raw(sql.as_ref(), params.iter().copied())
            .await
            .map_err(db_error)?;
        let decode_schema = projected_schema.clone();
        let mut batches = rows
            .chunks(ROWS_PER_BATCH)
            .map(move |rows| {
                let rows = rows
                    .into_iter()
                    .collect::<Result<Vec<Row>, _>>()
                    .map_err(db_error)?;
                let batch = rows_to_arrow(&rows, &decode_schema).map_err(|e| Box::new(e) as GenericError)?;
                types::restore(batch, decode_schema.as_deref()).map_err(GenericError::from)
            })
            .boxed();
        let Some(first) = batches.next().await else {
            let schema = projected_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty())));
        };
        let first = first?;
        let schema = first.schema();
        let rest = batches.map_err(|e| DataFusionError::Execution(format!("Failed to fetch batch: {e}")));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(async move { Ok(first) }).chain(rest),
        )))
    }

    async fn execute(&self, sql: &str, params: &[Param]) -> Result<u64, GenericError> {
        self.in_session_zone().await?;
        self.client.execute(sql, params).await.map_err(db_error)
    }
}

/// The server's own error when there is one (`permission denied for table
/// t`), else the client's.
pub(super) fn db_error(error: bb8_postgres::tokio_postgres::Error) -> GenericError {
    match error.as_db_error() {
        Some(db) => Box::new(db.clone()),
        None => Box::new(error),
    }
}

#[cfg(test)]
mod tests {
    use super::zone_statement;

    #[test]
    fn statements_run_in_the_engine_sessions_zone() {
        assert_eq!(zone_statement(None), "SET TIME ZONE 'Etc/UTC'");
        assert_eq!(zone_statement(Some("UTC")), "SET TIME ZONE 'Etc/UTC'");
        assert_eq!(
            zone_statement(Some("America/Sao_Paulo")),
            "SET TIME ZONE 'America/Sao_Paulo'"
        );
        assert_eq!(
            zone_statement(Some("-03:00")),
            "SET TIME ZONE INTERVAL '-03:00' HOUR TO MINUTE"
        );
        assert_eq!(
            zone_statement(Some("+05:45")),
            "SET TIME ZONE INTERVAL '+05:45' HOUR TO MINUTE"
        );
        assert_eq!(zone_statement(Some("-03'x")), "SET TIME ZONE '-03''x'");
        assert_eq!(zone_statement(Some("O'Zone")), "SET TIME ZONE 'O''Zone'");
    }
}
