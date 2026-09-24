//! Contract every source connector implements.
//!
//! The engine only knows this trait. Connectors live in `codelake-connectors` and
//! the dependency points one way: connectors depend on core, never the reverse
//! (enforced by `cargo xtask arch`).

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::CatalogProvider;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::session_properties::CatalogSession;
use crate::table_procedure::{ProcedureCall, TableProcedure, not_registered};

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

    /// DDL and write support, for sources that accept `CREATE`, `DROP`, views
    /// and `ALTER TABLE ADD COLUMN`. `INSERT` goes through the table provider.
    fn writable(&self) -> Option<Arc<dyn WritableCatalog>> {
        None
    }

    /// Catalog session properties (`<catalog>.<name>`) this connector honors.
    fn session_properties(&self) -> Vec<crate::session_properties::SessionProperty> {
        Vec::new()
    }
}

/// Statement-level writes a catalog accepts. The engine routes the matching
/// statements here instead of `DataFusion`'s in-memory DDL.
#[async_trait]
pub trait WritableCatalog: Send + Sync + Debug {
    async fn create_schema(&self, schema: &str, if_not_exists: bool, owner: &str) -> DfResult<()>;

    /// Creates `schema.table` from a stream of rows. The table becomes visible
    /// only after every row is durably written.
    async fn create_table_as(
        &self,
        schema: &str,
        table: &str,
        if_not_exists: bool,
        owner: &str,
        rows: SendableRecordBatchStream,
        context: Arc<TaskContext>,
    ) -> DfResult<u64>;

    async fn drop_table(&self, schema: &str, table: &str, if_exists: bool) -> DfResult<()>;

    /// Stores a view; `sql` is the definition as written by the client.
    async fn create_view(
        &self,
        schema: &str,
        view: &str,
        or_replace: bool,
        owner: &str,
        sql: &str,
        output: SchemaRef,
    ) -> DfResult<()>;

    async fn drop_view(&self, schema: &str, view: &str, if_exists: bool) -> DfResult<()>;

    async fn add_column(&self, schema: &str, table: &str, column: &str, data_type: &DataType)
    -> DfResult<()>;

    /// Procedures this catalog runs with `ALTER TABLE … EXECUTE` (Trino's table
    /// procedures, e.g. `OPTIMIZE`).
    fn table_procedures(&self) -> Vec<TableProcedure> {
        Vec::new()
    }

    /// Runs a procedure from [`WritableCatalog::table_procedures`] on an existing
    /// table. The engine has already checked that the table exists and is not a
    /// view, and bound `call`'s arguments (defaults applied). `session` holds
    /// this catalog's session properties; `context` gives access to the memory
    /// pool, which procedures that move data must reserve from.
    async fn execute_table_procedure(
        &self,
        schema: &str,
        table: &str,
        call: &ProcedureCall,
        session: &CatalogSession,
        context: Arc<TaskContext>,
    ) -> DfResult<()> {
        let _ = (schema, table, session, context);
        Err(DataFusionError::Plan(not_registered(&call.procedure)))
    }
}
