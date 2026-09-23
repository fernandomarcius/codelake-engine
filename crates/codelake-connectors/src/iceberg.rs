//! Apache Iceberg through a REST catalog, with data on S3 or an S3-compatible
//! store (`MinIO`). Read path only; writes are tracked in ROADMAP.md.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use codelake_core::{Connector, ConnectorError};
use datafusion::catalog::CatalogProvider;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::OnceCell;

#[derive(Debug, Clone)]
pub struct IcebergParams {
    pub catalog_uri: String,
    pub warehouse: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    /// `(access_key_id, secret_access_key)`; `None` uses the ambient AWS chain.
    pub credentials: Option<(SecretString, SecretString)>,
    pub path_style: bool,
}

pub struct IcebergConnector {
    name: String,
    params: IcebergParams,
    catalog: OnceCell<Arc<dyn Catalog>>,
}

impl std::fmt::Debug for IcebergConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergConnector")
            .field("name", &self.name)
            .field("params", &self.params)
            .finish_non_exhaustive()
    }
}

impl IcebergConnector {
    pub fn new(name: String, params: IcebergParams) -> Self {
        Self {
            name,
            params,
            catalog: OnceCell::new(),
        }
    }

    /// Properties handed to the REST catalog and to `FileIO`. Contains secrets:
    /// never log the returned map.
    fn properties(&self) -> HashMap<String, String> {
        let p = &self.params;
        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_owned(), p.catalog_uri.clone()),
            (S3_REGION.to_owned(), p.s3_region.clone()),
            (S3_PATH_STYLE_ACCESS.to_owned(), p.path_style.to_string()),
        ]);
        if let Some(warehouse) = &p.warehouse {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_owned(), warehouse.clone());
        }
        if let Some(endpoint) = &p.s3_endpoint {
            props.insert(S3_ENDPOINT.to_owned(), endpoint.clone());
        }
        if let Some((key, secret)) = &p.credentials {
            props.insert(S3_ACCESS_KEY_ID.to_owned(), key.expose_secret().to_owned());
            props.insert(S3_SECRET_ACCESS_KEY.to_owned(), secret.expose_secret().to_owned());
        }
        props
    }

    fn unreachable(&self, cause: impl Into<codelake_core::BoxError>) -> ConnectorError {
        ConnectorError::Unreachable {
            source_name: self.name.clone(),
            cause: cause.into(),
        }
    }

    async fn rest_catalog(&self) -> Result<Arc<dyn Catalog>, ConnectorError> {
        self.catalog
            .get_or_try_init(|| async {
                RestCatalogBuilder::default()
                    .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                        customized_credential_load: None,
                    }))
                    .load(self.name.clone(), self.properties())
                    .await
                    .map(|catalog| Arc::new(catalog) as Arc<dyn Catalog>)
                    .map_err(|e| self.unreachable(e))
            })
            .await
            .cloned()
    }
}

#[async_trait]
impl Connector for IcebergConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &'static str {
        "iceberg"
    }

    async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError> {
        let catalog = self.rest_catalog().await?;
        let provider =
            IcebergCatalogProvider::try_new(catalog)
                .await
                .map_err(|e| ConnectorError::Metadata {
                    source_name: self.name.clone(),
                    cause: e.into(),
                })?;
        Ok(Arc::new(provider))
    }

    async fn ping(&self) -> Result<(), ConnectorError> {
        let catalog = self.rest_catalog().await?;
        catalog
            .list_namespaces(None)
            .await
            .map(|_| ())
            .map_err(|e| self.unreachable(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(credentials: Option<(SecretString, SecretString)>) -> IcebergParams {
        IcebergParams {
            catalog_uri: "http://rest:8181".into(),
            warehouse: Some("s3://lake".into()),
            s3_endpoint: Some("http://minio:9000".into()),
            s3_region: "us-east-1".into(),
            credentials,
            path_style: true,
        }
    }

    #[test]
    fn properties_map_config_to_iceberg_keys() {
        let creds = Some((SecretString::from("AK"), SecretString::from("SK")));
        let props = IcebergConnector::new("lake".into(), params(creds)).properties();
        assert_eq!(props[REST_CATALOG_PROP_URI], "http://rest:8181");
        assert_eq!(props[REST_CATALOG_PROP_WAREHOUSE], "s3://lake");
        assert_eq!(props[S3_ENDPOINT], "http://minio:9000");
        assert_eq!(props[S3_PATH_STYLE_ACCESS], "true");
        assert_eq!(props[S3_ACCESS_KEY_ID], "AK");
        assert_eq!(props[S3_SECRET_ACCESS_KEY], "SK");
    }

    #[test]
    fn ambient_credentials_leave_keys_unset() {
        let props = IcebergConnector::new("lake".into(), params(None)).properties();
        assert!(!props.contains_key(S3_ACCESS_KEY_ID));
        assert!(!props.contains_key(S3_SECRET_ACCESS_KEY));
    }

    #[test]
    fn debug_never_prints_credentials() {
        let creds = Some((SecretString::from("AKIA-LEAK"), SecretString::from("TOP-SECRET")));
        let rendered = format!("{:?}", IcebergConnector::new("lake".into(), params(creds)));
        assert!(
            !rendered.contains("AKIA-LEAK") && !rendered.contains("TOP-SECRET"),
            "{rendered}"
        );
    }
}
