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
            parquet_time_zone,
            metastore_names,
        } => {
            let zone = parquet_time_zone
                .parse()
                .map_err(|reason: String| ConnectorError::Misconfigured {
                    source_name: name.clone(),
                    reason: format!("parquet_time_zone {reason}"),
                })?;
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
                .with_timestamp_precision(*timestamp_precision)
                .with_parquet_time_zone(zone)
                .with_metastore_names(metastore_names.clone()),
            ))
        }
        // No Iceberg integration is published for the DataFusion train (ADR 0013).
        SourceKind::Iceberg { .. } => Err(ConnectorError::Misconfigured {
            source_name: name.clone(),
            reason: "connector `iceberg` is not compiled into this build: no Iceberg integration is \
                     published for its DataFusion version (ADR 0013)"
                .to_owned(),
        }),
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

#[cfg(test)]
mod tests {
    use codelake_core::config::{Secrets, SourceConfig};
    use codelake_core::{ConnectorError, SourceKind};

    #[test]
    fn an_iceberg_source_names_why_it_is_missing() {
        let source = SourceConfig {
            name: "lake".to_owned(),
            required: true,
            kind: SourceKind::Iceberg {
                catalog_uri: "http://iceberg-rest:8181".to_owned(),
                warehouse: None,
                s3_endpoint: None,
                s3_region: "us-east-1".to_owned(),
                access_key_id_env: None,
                secret_access_key_env: None,
                s3_path_style: false,
            },
        };
        let Err(ConnectorError::Misconfigured { source_name, reason }) =
            super::build(&source, &Secrets::default())
        else {
            panic!("an iceberg source must not build");
        };
        assert_eq!(source_name, "lake");
        assert!(
            reason.starts_with("connector `iceberg` is not compiled into this build"),
            "{reason}"
        );
        assert!(
            reason.contains("ADR 0013") && !reason.contains("Cargo feature"),
            "{reason}"
        );
    }
}
