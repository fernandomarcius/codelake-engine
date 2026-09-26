//! `CodeLake` core: configuration, memory governance, federated catalog and the
//! connector contract. This crate must not depend on any concrete connector or on
//! the server (see `cargo xtask arch`).

pub mod bytes;
mod casts;
pub mod catalog;
pub mod char_semantics;
mod coercion;
pub mod computed_batches;
pub mod config;
pub mod connector;
mod ddl;
mod declared;
mod dml_guard;
pub mod engine;
pub mod env_refs;
pub mod federation_guard;
pub mod federation_policy;
pub mod functions;
pub mod governance;
pub mod identifier_case;
pub mod information_schema;
pub mod join_guard;
pub mod join_spill;
mod literals;
mod memory_tables;
mod nested_values;
pub mod operand_checks;
pub mod output;
pub mod overflow;
pub mod parquet_time_zone;
pub mod plain_views;
pub mod pool;
pub mod runtime;
#[cfg(test)]
mod semantics_tests;
pub mod session_properties;
mod set_operations;
pub mod statement_policy;
pub mod statistics_guard;
pub mod subquery;
pub mod table_procedure;
#[cfg(test)]
mod testing;
mod timestamp_casts;
mod timestamps;
pub mod trace;
pub mod trino_functions;
mod trino_sql;
mod trino_sql_checks;
pub mod trino_types;
mod window_frames;

pub use bytes::ByteSize;
pub use config::{
    Config, GovernanceConfig, ResourceGroupConfig, SourceConfig, SourceKind, TrinoProtocolConfig,
    ValidatedConfig,
};
pub use connector::{
    BoxError, COLUMN_COMMENT, Connector, ConnectorError, DECLARED_TYPE, ViewDefinition, ViewSecurity,
    WritableCatalog,
};
pub use ddl::{RelationKind, update_type as ddl_update_type};
pub use declared::{PostgresStatement, postgres_statement, trino_type_syntax};
pub use engine::{Attach, Engine, EngineError, Query, QueryOptions, SourceStatus};
pub use governance::Governance;
pub use runtime::MemoryStats;
pub use session_properties::{
    CatalogSession, Effect, PropertyRegistry, PropertyType, SessionProperties, SessionProperty,
};
pub use table_procedure::{ProcedureArgument, ProcedureCall, TableProcedure};
pub use timestamps::trino_time_zone;
pub use trace::{QueryTrace, TraceParent};
pub use trino_sql_checks::{regroup_distinct_from, regrouped_statement};
