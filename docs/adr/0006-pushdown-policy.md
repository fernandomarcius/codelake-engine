# ADR 0006 — Per-source pushdown policy and scalar subquery pre-evaluation

- Status: Accepted
- Date: 2026-09-23

## Context

`datafusion-federation` pushes the largest sub-plan it can to a SQL source.
Databases do not evaluate every operator like the engine: SQL Server compares
text with case-insensitive collations and ignores trailing spaces, truncates
`AVG(int)`, sums `int` in 32 bits and sorts NULLs first. Pushing those operators
changes results without an error.

Separately, the reference incremental models filter remote CDC tables by a watermark
computed in another catalog (`__$start_lsn > (SELECT COALESCE(MAX(lsn), X'00')
FROM hive...)`). DataFusion 53 plans that as a non-equi join, which has no
dynamic filter, so the whole remote table would be read on every run.

## Decision

1. `codelake_core::federation_policy`: each SQL connector supplies a
   `PushdownPolicy`; `restrict_federation` (plugged into the executor's
   `logical_optimizer` hook) keeps only policy-approved subtrees federated and
   runs the rest in the engine. Table scans are always federated; rejected scan
   filters are lifted into a local `Filter`, never dropped.
2. SQL Server policy (see `sqlserver/dialect.rs` for the full table): text
   comparisons, text grouping, `AVG`, `SUM` of `int`/`smallint`, decimal `/` and
   `%`, `ORDER BY`, unfaithful literals, functions and non-widening casts stay local.
3. Optimizer order: an extra `push_down_filter` runs right before the
   federation rule (kept at upstream's position, after scalar subquery
   decorrelation), so filters above cross-source joins reach their source.
   Federating after *all* rules was tried and rejected: `optimize_projections`
   then creates derived tables the unparser renders as invalid SQL.
4. Before unparsing, `PushdownPolicy::prepare` adapts the federated subtree
   (SQL Server: re-wrap folded binary literals, flatten column-only projections
   under aggregates). A rejected conjunctive filter pushes its safe terms and
   keeps the rest local.
5. SQL Server tables expose lower-case names but restore the real spelling in
   the remote SQL, so case-sensitive collations work.
6. `Engine::sql` pre-evaluates uncorrelated scalar subqueries in `WHERE`/`ON`
   predicates (no row = NULL, more than one row = error) and substitutes the
   value. Binary values travel as `codelake_varbinary('<hex>')`, which dialects
   render natively (T-SQL `0x…`), because the DataFusion unparser cannot render
   binary literals.

## Consequences

- Correctness first: some operators Trino pushes down run locally here, moving
  more rows. The shadow run (F8) measures the cost.
- Clients must go through `Engine::sql` to benefit from pre-evaluation. The
  Trino protocol endpoint (F3) does; the PostgreSQL wire path still calls
  `SessionContext::sql` directly (correct, slower) until it is routed through
  the engine.
- MySQL has the same collation hazard and still uses the unrestricted
  `datafusion-table-providers` federation: tracked for M1 before any reference use.
- Binary comparison caveat: SQL Server pads the shorter operand with zeros; the
  engine treats a prefix as smaller. They differ only when an all-zero value is
  compared with a shorter one, which does not happen for LSNs.
