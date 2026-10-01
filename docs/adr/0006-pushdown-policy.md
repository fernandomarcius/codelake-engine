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
6. `Engine::sql` pre-evaluates uncorrelated, deterministic scalar subqueries
   in any expression of a statement except a view definition (no row = NULL,
   more than one row = `SUBQUERY_MULTIPLE_ROWS`) and substitutes the value.
   Binary values travel as `codelake_varbinary('<hex>')`, which dialects
   render natively (T-SQL `0x…`), because the DataFusion unparser cannot render
   binary literals. DataFusion plans the other scalar subqueries (correlated
   or non-deterministic) as joins, which repeat the outer row once per
   subquery row and drop it when there is none; unless the plan already
   yields one row (per outer row), the subquery is wrapped in an aggregation
   by `codelake_single_row`: NULL without rows, `SUBQUERY_MULTIPLE_ROWS` on a
   second row (`codelake_core::subquery`).
7. Integer and decimal arithmetic is checked (`codelake_core::overflow`):
   DataFusion wraps around on overflow (`+ - *`, unary minus, `sum`) and lets
   decimals exceed their precision; Trino fails with
   `NUMERIC_VALUE_OUT_OF_RANGE`. Casts to an integer round half away from
   zero, as in Trino, instead of truncating. An analyzer rule replaces the
   operators with checked functions; a rule right before `push_down_filter`
   and federation turns them back into operators, so federated sub-plans
   render as plain SQL and the source applies its own overflow checks, and a
   rule right after federation checks again what runs in the engine. `restrict_federation` re-checks the
   nodes it keeps local.

8. On DataFusion 54, every `push_down_filter` of the list runs wrapped so
   that nothing is inferred from the keys of a null-aware anti join, and a
   `NOT IN` compared with a constant or over a correlated subquery is
   rewritten before decorrelation (ADR 0014).

9. A hash join's dynamic filter reaches a federated scan on its probe side
   (`codelake_core::federated_dynamic_filters`). The federation barrier keeps
   the dynamic filters offered to it (reporting them unsupported: the join
   still filters exactly) and, when the remote query starts, after the
   build side, adds to it what they say about plain columns of the table:
   the keys as an `IN` list while the build side has at most
   `execution.dynamic_filter_max_keys` of them (150), their min/max always.
   Each term goes through the source's policy as a superset filter: sent as
   it is, as the superset the policy offers, or not at all (SQL Server: text
   keys as an `IN` superset under its collation, never text min/max). A
   null-safe join's filter (`(bounds AND keys) OR key IS NULL`, ADR 0014) is
   split term by term (`(keys OR key IS NULL) AND ...`) so its keys still go
   when the bounds do not. Only
   over a table scan under filters, plain column projections and aliases;
   a source without a policy gets nothing. SQL Server compares a
   `datetime`/`datetime2(7)` column with a literal inside a second through
   whole-second bounds (`>=` the second at or before it, `<` the first
   second past what it keeps), a superset of what the engine keeps.

10. `lower(column)`/`upper(column)` compared with ASCII literals (`=`, `IN`)
    reach SQL Server as a superset when the column's collation (read from
    `INFORMATION_SCHEMA.COLUMNS`) is a Latin one: the column itself under a
    case-insensitive collation, `LOWER`/`UPPER` of it otherwise (both pair
    `A`-`Z` with `a`-`z` as the engine; a Turkish or Azeri collation does
    not, and gets nothing). A letter a character beyond ASCII also becomes
    under the engine's case mapping (`İ` → `i`, Kelvin sign → `k`, `ı` →
    `I`, `ſ` → `S`) is `%` in a `LIKE`. `<>` and `NOT IN` are not supersets
    and stay local.
11. `ORDER BY … LIMIT n [OFFSET m]` over a sub-plan the source evaluates
    exactly (no superset below it) asks the source for its first `m + n`
    rows (`PushdownPolicy::first_rows`, `TOP … ORDER BY` in T-SQL); the
    engine sorts them again. SQL Server: keys that are plain columns of
    integer, decimal, `date`, `smalldatetime`, `datetime2(0..6)` or
    `datetime` type (ordered value for value as the engine orders them), each
    with its NULLs where SQL Server puts them (first ascending, last
    descending) or without NULLs.

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
