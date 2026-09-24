//! `CodeLake` core: configuration, memory governance, federated catalog and the
//! connector contract. This crate must not depend on any concrete connector or on
//! the server (see `cargo xtask arch`).

pub mod bytes;
mod casts;
pub mod catalog;
mod coercion;
pub mod config;
pub mod connector;
mod ddl;
mod declared;
pub mod engine;
pub mod federation_guard;
pub mod federation_policy;
pub mod functions;
pub mod information_schema;
mod literals;
pub mod overflow;
pub mod runtime;
#[cfg(test)]
mod semantics_tests;
pub mod session_properties;
pub mod subquery;
#[cfg(test)]
mod testing;
mod timestamp_casts;
mod timestamps;
mod trino_functions;
pub mod trino_types;

pub use bytes::ByteSize;
pub use config::{Config, SourceConfig, SourceKind, TrinoProtocolConfig, ValidatedConfig};
pub use connector::{BoxError, Connector, ConnectorError, ViewDefinition, ViewSecurity, WritableCatalog};
pub use ddl::{RelationKind, update_type as ddl_update_type};
pub use engine::{Attach, Engine, EngineError, QueryOptions, SourceStatus};
pub use runtime::MemoryStats;
pub use session_properties::{Effect, PropertyRegistry, PropertyType, SessionProperties, SessionProperty};
pub use timestamps::trino_time_zone;
