# ADR 0002 — One DataFusion "version train"

- Status: Accepted
- Date: 2026-09-23

## Context

DataFusion ships a major roughly monthly. `datafusion-federation`,
`datafusion-table-providers`, `iceberg-datafusion` and `datafusion-postgres` each
target a specific major, and they lag each other. Two majors in one binary do not
interoperate (distinct `TableProvider` traits) and double the binary.

## Decision

Pin the newest major that **all** dependencies support, as one block in
`[workspace.dependencies]`:

| Crate | Version | DataFusion |
|---|---|---|
| datafusion | 53.1 | 53 |
| datafusion-federation | =0.5.3 | 53 |
| datafusion-table-providers | =0.11.2 | 53 |
| iceberg / iceberg-datafusion | =0.10.1 | 53 |
| datafusion-postgres | =0.17.0 | 53 |

The `train` gate fails if the graph ever contains two majors of DataFusion, Arrow,
Parquet or object_store. Dependabot groups these crates so they move together.

## Consequences

Upgrading means waiting for the slowest crate (today `iceberg-datafusion`). A bump
PR must pass every gate, including the integration suite, because pushdown
behaviour changes between majors (see the federation guard).
