//! Source connectors. Each connector is behind a Cargo feature so a deployment
//! compiles (and audits) only the drivers it uses.
//!
//! Adding a connector: implement [`codelake_core::Connector`], add a feature, add a
//! variant to `SourceKind` and wire it in [`build`]. Integration tests against a
//! real instance (Testcontainers) are mandatory; see `tests/`.

use std::sync::Arc;

use codelake_core::config::{Secrets, SourceConfig};
use codelake_core::{Connector, ConnectorError, SourceKind};

#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "mongodb"))]
mod params;
#[cfg(feature = "postgres")]
pub mod postgres;

/// Builds the connector for one configured source. Does not connect yet.
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
        #[cfg(feature = "mongodb")]
        SourceKind::Mongodb {
            connection_string_env,
        } => Ok(Arc::new(mongodb::MongoConnector::new(
            name.clone(),
            secret(connection_string_env)?,
        ))),
        #[cfg(feature = "iceberg")]
        SourceKind::Iceberg {
            catalog_uri,
            warehouse,
            s3_endpoint,
            s3_region,
            access_key_id_env,
            secret_access_key_env,
            s3_path_style,
        } => {
            let credentials = match (access_key_id_env, secret_access_key_env) {
                (Some(key), Some(secret_key)) => Some((secret(key)?, secret(secret_key)?)),
                _ => None,
            };
            Ok(Arc::new(iceberg::IcebergConnector::new(
                name.clone(),
                iceberg::IcebergParams {
                    catalog_uri: catalog_uri.clone(),
                    warehouse: warehouse.clone(),
                    s3_endpoint: s3_endpoint.clone(),
                    s3_region: s3_region.clone(),
                    credentials,
                    path_style: *s3_path_style,
                },
            )))
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
