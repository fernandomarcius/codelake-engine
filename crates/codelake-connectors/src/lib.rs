//! Source connectors. Each connector is behind a Cargo feature so a deployment
//! compiles (and audits) only the drivers it uses.
//!
//! Adding a connector: implement [`codelake_core::Connector`], add a feature, add a
//! variant to `SourceKind` and wire it in [`build`]. Integration tests against a
//! real instance (Testcontainers) are mandatory; see `tests/`.

use std::sync::Arc;

use codelake_core::config::{Secrets, SourceConfig};
use codelake_core::{Connector, ConnectorError, SourceKind};

#[cfg(feature = "hive")]
mod file_statistics;
#[cfg(feature = "hive")]
pub mod hive;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "mongodb"))]
mod params;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlserver")]
pub mod sqlserver;

/// Builds the connector for one configured source. Does not connect yet.
#[allow(
    clippy::too_many_lines,
    reason = "one match arm per connector kind keeps the config-to-connector mapping in one place"
)]
pub fn build(source: &SourceConfig, secrets: &Secrets) -> Result<Arc<dyn Connector>, ConnectorError> {
    let name = source.name.clone();
    let secret = |var: &str| {
        secrets
            .get(var)
            .cloned()
            .ok_or_else(|| ConnectorError::Misconfigured {
                source_name: name.clone(),
                reason: format!("secret {var} was not resolved"),
            })
    };
    let _ = &secret;
    match &source.kind {
        #[cfg(feature = "postgres")]
        SourceKind::Postgres {
            host,
            port,
            database,
            user,
            password_env,
            sslmode,
        } => Ok(Arc::new(postgres::PostgresConnector::new(
            name.clone(),
            postgres::PostgresParams {
                host: host.clone(),
                port: *port,
                database: database.clone(),
                user: user.clone(),
                password: secret(password_env)?,
                sslmode: sslmode.clone(),
            },
        ))),
        #[cfg(feature = "mysql")]
        SourceKind::Mysql {
            host,
            port,
            database,
            user,
            password_env,
            sslmode,
        } => Ok(Arc::new(mysql::MysqlConnector::new(
            name.clone(),
            mysql::MysqlParams {
                host: host.clone(),
                port: *port,
                database: database.clone(),
                user: user.clone(),
                password: secret(password_env)?,
                sslmode: sslmode.clone(),
            },
        ))),
        #[cfg(feature = "sqlserver")]
        SourceKind::Sqlserver {
            host,
            port,
            database,
            user,
            password_env,
            encrypt,
            trust_server_certificate,
            pool_size,
        } => Ok(Arc::new(sqlserver::SqlServerConnector::new(
            name.clone(),
            sqlserver::SqlServerParams {
                host: host.clone(),
                port: *port,
                database: database.clone(),
                user: user.clone(),
                password: secret(password_env)?,
                encrypt: encrypt.clone(),
                trust_server_certificate: *trust_server_certificate,
                pool_size: *pool_size,
            },
        ))),
        #[cfg(feature = "mongodb")]
        SourceKind::Mongodb {
            connection_string_env,
        } => Ok(Arc::new(mongodb::MongoConnector::new(
            name.clone(),
            secret(connection_string_env)?,
        ))),
        #[cfg(feature = "hive")]
        SourceKind::Hive {
            warehouse,
            s3_endpoint,
            s3_region,
            access_key_id_env,
            secret_access_key_env,
            s3_path_style,
            timestamp_precision,
        } => {
            let credentials = match (access_key_id_env, secret_access_key_env) {
                (Some(key), Some(secret_key)) => Some((secret(key)?, secret(secret_key)?)),
                _ => None,
            };
            Ok(Arc::new(
                hive::HiveConnector::new(
                    name.clone(),
                    hive::HiveParams {
                        warehouse: warehouse.clone(),
                        s3_endpoint: s3_endpoint.clone(),
                        s3_region: s3_region.clone(),
                        credentials,
                        path_style: *s3_path_style,
                    },
                )
                .with_timestamp_precision(*timestamp_precision),
            ))
        }
        #[allow(
            unreachable_patterns,
            reason = "reachable only when a connector feature is disabled"
        )]
        other => Err(ConnectorError::Misconfigured {
            source_name: name.clone(),
            reason: format!(
                "connector `{}` is not compiled into this build (enable the Cargo feature)",
                other.label()
            ),
        }),
    }
}
