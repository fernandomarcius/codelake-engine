//! `CodeLake` core: configuration, memory governance, federated catalog and the
//! connector contract. This crate must not depend on any concrete connector or on
//! the server (see `cargo xtask arch`).

pub mod bytes;
pub mod catalog;
pub mod config;
pub mod connector;
mod ddl;
pub mod engine;
pub mod federation_guard;
pub mod federation_policy;
pub mod functions;
pub mod information_schema;
mod literals;
pub mod overflow;
pub mod runtime;
pub mod session_properties;
pub mod subquery;
pub mod table_procedure;
#[cfg(test)]
mod testing;
pub mod trace;

pub use bytes::ByteSize;
pub use config::{Config, SourceConfig, SourceKind, TrinoProtocolConfig, ValidatedConfig};
pub use connector::{BoxError, Connector, ConnectorError, ViewDefinition, WritableCatalog};
pub use engine::{Attach, Engine, EngineError, Query, QueryOptions, SourceStatus};
pub use runtime::MemoryStats;
pub use session_properties::{
    CatalogSession, Effect, PropertyRegistry, PropertyType, SessionProperties, SessionProperty,
};
pub use table_procedure::{ProcedureArgument, ProcedureCall, TableProcedure};
pub use trace::{QueryTrace, TraceParent};
