//! `CodeLake` core: configuration, memory governance, federated catalog and the
//! connector contract. This crate must not depend on any concrete connector or on
//! the server (see `cargo xtask arch`).

pub mod bytes;
pub mod catalog;
pub mod config;
pub mod connector;
pub mod engine;
pub mod federation_guard;
pub mod runtime;

pub use bytes::ByteSize;
pub use config::{Config, SourceConfig, SourceKind, ValidatedConfig};
pub use connector::{BoxError, Connector, ConnectorError};
pub use engine::{Attach, Engine, EngineError, SourceStatus};
pub use runtime::MemoryStats;
