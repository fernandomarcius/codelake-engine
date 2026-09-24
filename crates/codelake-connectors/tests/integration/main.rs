//! Integration tests against real, disposable databases (Testcontainers).
//! One test binary on purpose: each binary links the whole engine, and several
//! of them in parallel exhaust the linker's memory.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test crate: a failed setup must abort the test loudly"
)]

mod common;

#[cfg(feature = "hive")]
mod hive_acceptance;
#[cfg(feature = "hive")]
mod hive_smoke;

#[cfg(feature = "mongodb")]
mod mongodb;
#[cfg(all(feature = "mysql", feature = "postgres"))]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
#[cfg(all(feature = "sqlserver", feature = "postgres"))]
mod sqlserver_acceptance;
#[cfg(all(feature = "sqlserver", feature = "postgres"))]
mod sqlserver_audit;
#[cfg(all(feature = "sqlserver", feature = "postgres"))]
mod sqlserver_smoke;
