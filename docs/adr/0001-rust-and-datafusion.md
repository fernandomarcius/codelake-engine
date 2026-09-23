# ADR 0001 — Rust and Apache DataFusion instead of Trino (JVM)

- Status: Accepted
- Date: 2026-09-23

## Context

Trino serves federated queries (SQL Server CDC, Hive/Parquet, dbt) for the reference
pipelines. The cost driver is memory: a JVM needs heap plus GC headroom, and
exceeding it kills queries or the node. We want a hard, observable memory ceiling
and lower cost per node. The corporate stack catalog defaults to TypeScript/NestJS;
a query engine is outside that catalog's intent, so this ADR records the exception.

## Decision

Build CodeLake in Rust on Apache DataFusion, reusing `datafusion-federation` and
`datafusion-table-providers` instead of forking Spice.ai or porting Trino.

## Alternatives

- **Port Trino to Rust:** ~1M lines of Java relying on GC, runtime bytecode
  generation and classloader plugins; not translatable, only rewritable.
- **Fork Spice.ai OSS:** carries LLM/search/acceleration features we do not need and
  releases weekly; a diverging fork means constant merge pain.
- **Tune Trino:** lowers but does not remove JVM overhead; no hard ceiling.

## Consequences

- Single node first; distributed execution is a later milestone (ADR pending, M4).
- We inherit DataFusion's release cadence (see ADR 0002).
- We own connector gaps: SQL Server, Hive catalog, Trino protocol and functions.
