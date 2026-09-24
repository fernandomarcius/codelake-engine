# ADR 0002 — One DataFusion "version train"

- Status: Accepted
- Date: 2026-09-23; amended 2026-09-24 (DataFusion 54 train)

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
| datafusion | 54.1 | 54 |
| datafusion-federation | =0.5.5 | 54 |
| datafusion-table-providers-{common,postgres,mysql,mongodb} | =0.13.1 | 54 |
| datafusion-postgres | =0.18.0 | 54 |

Pins that are easy to get wrong:

- `datafusion-federation` is pinned exactly: the table-provider crates ask for
  `^0.5.5`, only one 0.5.x can be in the graph, and 0.5.7 already needs
  DataFusion 55.
- Since 0.13 `datafusion-table-providers` is one crate per connector; the
  facade lost the `*-federation` features. The workspace declares the
  per-connector crates with `default-features = false` and
  `codelake-connectors` enables `federation` per connector (PostgreSQL and
  MySQL yes, MongoDB no, as before).
- Iceberg is out of this train: no Iceberg integration is published for
  DataFusion 54 (ADR 0013).

History: DataFusion 53 (federation 0.5.3, table-providers 0.11.2, iceberg
0.10.1, datafusion-postgres 0.17.0) until 2026-09-24. DataFusion 55 was
skipped: neither `datafusion-table-providers` nor `datafusion-postgres` has a
release for it. DataFusion 56 is the next candidate: it is the first train on
`object_store` 0.14.2, which the `hive-s3` feature needs.

The `train` gate fails if the graph ever contains two majors of DataFusion, Arrow,
Parquet or object_store. Dependabot groups these crates so they move together.

## Consequences

Upgrading means waiting for the slowest crate, or leaving a feature out of the
build as ADR 0013 does for Iceberg. A bump PR must pass every gate, including
the integration suite, because pushdown and function behaviour change between
majors (see the federation guard and the DataFusion upgrade guides).
