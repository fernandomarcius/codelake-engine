//! Contract every source connector implements.
//!
//! The engine only knows this trait. Connectors live in `codelake-connectors` and
//! the dependency points one way: connectors depend on core, never the reverse
//! (enforced by `cargo xtask arch`).

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;

/// Boxed error from a driver, kept opaque so connectors can wrap any client library.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("source `{source_name}` is unreachable: {cause}")]
    Unreachable {
        source_name: String,
        #[source]
        cause: BoxError,
    },
    #[error("source `{source_name}` rejected the configuration: {reason}")]
    Misconfigured { source_name: String, reason: String },
    #[error("source `{source_name}` failed while listing metadata: {cause}")]
    Metadata {
        source_name: String,
        #[source]
        cause: BoxError,
    },
}

/// A federated data source exposed to SQL as one catalog.
#[async_trait]
pub trait Connector: Send + Sync + Debug {
    /// Catalog name used in SQL (`SELECT * FROM <name>.<schema>.<table>`).
    fn name(&self) -> &str;

    /// Connector family (`postgres`, `mongodb`, `iceberg`), used as a metric label.
    fn kind(&self) -> &'static str;

    /// Builds the catalog. Called once at startup; implementations should connect,
    /// list schemas and return providers that push work down to the source.
    async fn catalog(&self) -> Result<Arc<dyn CatalogProvider>, ConnectorError>;

    /// Cheap reachability check used by the readiness probe. Must not scan data.
    async fn ping(&self) -> Result<(), ConnectorError>;
}
