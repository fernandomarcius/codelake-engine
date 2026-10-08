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
mod f4_acceptance;
#[cfg(feature = "hive")]
mod f9_acceptance;
#[cfg(all(feature = "hive", feature = "postgres"))]
mod float_zeros;
#[cfg(feature = "hive")]
mod hash_join_spill;
#[cfg(feature = "hive")]
mod hive_acceptance;
#[cfg(feature = "hive")]
mod hive_optimize;
#[cfg(feature = "hive")]
mod hive_redteam;
#[cfg(feature = "hive")]
mod hive_redteam_w1;
#[cfg(feature = "hive")]
mod hive_smoke;
#[cfg(feature = "hive")]
mod hive_statistics;
#[cfg(feature = "hive")]
mod hive_statistics_acceptance;
#[cfg(feature = "hive")]
mod join_regressions;

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
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_case_functions;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_column_pruning;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_dynamic_filter;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_federated_joins;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_join_build_side;
#[cfg(all(feature = "sqlserver", feature = "postgres"))]
mod sqlserver_smoke;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_timestamp_sentinels;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_union_text_types;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_union_timestamp_types;
#[cfg(all(feature = "sqlserver", feature = "hive"))]
mod sqlserver_views_memory;
