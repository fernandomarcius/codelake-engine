//! Trino client protocol (`/v1/statement`), so Trino clients — the JDBC driver,
//! the CLI, the Python client, dbt-trino — and plain HTTP scripts work against
//! `CodeLake` unchanged. See `docs/adr/0012-trino-client-protocol.md`.

mod errors;
mod prepared;
mod queries;
mod rewrite;
mod session;
mod statement;
mod types;

pub(crate) use queries::Limits;
pub(crate) use statement::{TrinoState, router};

#[cfg(test)]
mod tests;
