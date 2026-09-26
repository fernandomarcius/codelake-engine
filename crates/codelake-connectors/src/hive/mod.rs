//! Hive catalog compatible with Trino's file metastore (ADR 0007): reads and
//! writes the same `.trinoSchema` layout as Trino's `hive.metastore=file`, on a
//! local directory or S3/MinIO.

mod catalog;
mod columns;
mod crc;
mod exact_timestamps;
#[cfg(test)]
mod f2_tests;
#[cfg(test)]
mod limits_tests;
mod metastore;
mod optimize;
#[cfg(test)]
mod optimize_tests;
mod statistics;
mod table;
mod trino_parquet;
mod types;

use std::sync::Arc;

use async_trait::async_trait;
use codelake_core::config::HiveTimestampPrecision;
use codelake_core::parquet_time_zone::ParquetTimeZone;
use codelake_core::{BoxError, Connector, ConnectorError, SessionProperty, WritableCatalog};
use datafusion::catalog::CatalogProvider;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
#[cfg(feature = "hive-s3")]
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tokio::sync::OnceCell;

pub use self::catalog::HiveCatalog;
use self::catalog::HiveCatalogProvider;
use self::metastore::FileMetastore;

#[derive(Debug, Clone)]
pub struct HiveParams {
    /// Local directory (`/data/warehouse` or `file:/data/warehouse`) or
    /// `s3://bucket/prefix`.
    pub warehouse: String,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub credentials: Option<(SecretString, SecretString)>,
    pub path_style: bool,
}

/// Where the warehouse lives, parsed from `warehouse`.
#[derive(Debug, PartialEq, Eq)]
enum Location {
    Local(std::path::PathBuf),
    S3 { bucket: String, prefix: String },
}

fn parse_location(warehouse: &str) -> Result<Location, String> {
    if let Some(rest) = warehouse.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(format!("warehouse `{warehouse}` has no bucket"));
        }
        return Ok(Location::S3 {
            bucket: bucket.to_owned(),
            prefix: prefix.trim_end_matches('/').to_owned(),
        });
    }
    let path = warehouse.strip_prefix("file:").unwrap_or(warehouse);
    let path = path.strip_prefix("//").unwrap_or(path);
    if !path.starts_with('/') {
        return Err(format!(
            "warehouse `{warehouse}` must be an absolute path or s3:// URL"
        ));
    }
    Ok(Location::Local(path.into()))
}

pub struct HiveConnector {
    name: String,
    params: HiveParams,
    /// Trino's `hive.timestamp-precision` for this catalog.
    precision: HiveTimestampPrecision,
    /// The reference engine's `hive.parquet.time-zone` for this catalog.
    zone: ParquetTimeZone,
    catalog: OnceCell<Arc<HiveCatalog>>,
}

impl std::fmt::Debug for HiveConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HiveConnector")
            .field("name", &self.name)
            .field("warehouse", &self.params.warehouse)
            .finish_non_exhaustive()
    }
}

impl HiveConnector {
    pub fn new(name: String, params: HiveParams) -> Self {
        Self {
            name,
            params,
            precision: HiveTimestampPrecision::default(),
            zone: ParquetTimeZone::default(),
            catalog: OnceCell::new(),
        }
    }

    /// Sets the precision of the catalog's timestamps (Trino's
    /// `hive.timestamp-precision`, default `MILLISECONDS`).
    #[must_use]
    pub fn with_timestamp_precision(mut self, precision: HiveTimestampPrecision) -> Self {
        self.precision = precision;
        self
    }

    /// Sets the zone of the catalog's Parquet `INT96` timestamps (the
    /// reference engine's `hive.parquet.time-zone`, default
    /// `America/Sao_Paulo`): writes convert wall-clock times from it to UTC,
    /// reads convert back.
    #[must_use]
    pub fn with_parquet_time_zone(mut self, zone: ParquetTimeZone) -> Self {
        self.zone = zone;
        self
    }

    fn misconfigured(&self, reason: impl Into<String>) -> ConnectorError {
        ConnectorError::Misconfigured {
            source_name: self.name.clone(),
            reason: reason.into(),
        }
    }

    fn metastore(&self) -> Result<FileMetastore, ConnectorError> {
        let location = parse_location(&self.params.warehouse).map_err(|e| self.misconfigured(e))?;
        let (store, root, local): (Arc<dyn ObjectStore>, Path, _) = match location {
            Location::Local(path) => {
                std::fs::create_dir_all(&path)
                    .map_err(|e| self.misconfigured(format!("{}: {e}", path.display())))?;
                let store =
                    LocalFileSystem::new_with_prefix(&path).map_err(|e| self.misconfigured(e.to_string()))?;
                (Arc::new(store), Path::default(), Some(path))
            }
            #[cfg(not(feature = "hive-s3"))]
            Location::S3 { .. } => {
                return Err(self.misconfigured(
                    "s3:// warehouses need the `hive-s3` feature, not shipped yet (ROADMAP F11)",
                ));
            }
            #[cfg(feature = "hive-s3")]
            Location::S3 { bucket, prefix } => {
                let mut builder = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(&self.params.s3_region)
                    .with_virtual_hosted_style_request(!self.params.path_style);
                if let Some(endpoint) = &self.params.s3_endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_allow_http(endpoint.starts_with("http://"));
                }
                if let Some((key, secret)) = &self.params.credentials {
                    builder = builder
                        .with_access_key_id(key.expose_secret())
                        .with_secret_access_key(secret.expose_secret());
                }
                let store = builder.build().map_err(|e| self.misconfigured(e.to_string()))?;
                (Arc::new(store), Path::from(prefix), None)
            }
        };
        let metastore = FileMetastore::new(store, root)
            .with_timestamp_precision(self.precision)
            .with_parquet_time_zone(self.zone);
        Ok(match local {
            Some(dir) => metastore.with_local_dir(dir),
            None => metastore,
        })
    }

    async fn hive(&self) -> Result<Arc<HiveCatalog>, ConnectorError> {
        self.catalog
            .get_or_try_init(|| async {
                let catalog = HiveCatalog::new(&self.name, self.metastore()?)
                    .map_err(|e| self.misconfigured(e.to_string()))?;
                Ok(Arc::new(catalog))
            })
            .await
            .cloned()
    }

    fn unreachable(&self, cause: impl Into<BoxError>) -> ConnectorError {
        ConnectorError::Unreachable {
            source_name: self.name.clone(),
            cause: cause.into(),
        }
    }
}

#[async_trait]
impl Connector for HiveConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "hive"
    }

    async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
        let hive = self.hive().await?;
        hive.refresh().await.map_err(|e| self.unreachable(e))?;
        // A half-swapped OPTIMIZE would show duplicated rows: settle it first.
        hive.recover_interrupted_optimize()
            .await
            .map_err(|e| ConnectorError::Metadata {
                source_name: self.name.clone(),
                cause: e.into(),
            })?;
        Ok(Arc::new(HiveCatalogProvider(hive)))
    }

    async fn ping(&self) -> Result<(), ConnectorError> {
        let hive = self.hive().await?;
        hive.metastore()
            .list_schemas()
            .await
            .map(|_| ())
            .map_err(|e| self.unreachable(e))
    }

    fn writable(&self) -> Option<Arc<dyn WritableCatalog>> {
        self.catalog
            .get()
            .map(|c| Arc::clone(c) as Arc<dyn WritableCatalog>)
    }

    fn session_properties(&self) -> Vec<SessionProperty> {
        optimize::session_properties()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warehouse_locations() {
        assert_eq!(parse_location("/data/wh"), Ok(Location::Local("/data/wh".into())));
        assert_eq!(
            parse_location("file:/data/wh"),
            Ok(Location::Local("/data/wh".into()))
        );
        assert_eq!(
            parse_location("file:///data/wh"),
            Ok(Location::Local("/data/wh".into()))
        );
        assert_eq!(
            parse_location("s3://lake/warehouse/"),
            Ok(Location::S3 {
                bucket: "lake".into(),
                prefix: "warehouse".into()
            })
        );
        assert_eq!(
            parse_location("s3://lake"),
            Ok(Location::S3 {
                bucket: "lake".into(),
                prefix: String::new()
            })
        );
        assert!(parse_location("relative/dir").is_err());
        assert!(parse_location("s3:///x").is_err());
    }
}
