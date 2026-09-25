# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- Engine core with hard memory ceiling (`FairSpillPool`) and bounded spill.
- Fail-fast configuration validation reporting every problem at once; secrets via environment variables only.
- Connectors: PostgreSQL and MySQL (federated pushdown), MongoDB.
- Server: PostgreSQL wire protocol, `/livez`, `/readyz`, `/healthz`, `/metrics`, graceful drain on SIGTERM.
- Quality gates: clippy pedantic, nextest, Testcontainers integration, architecture and version-train checks,
  coverage, mutation testing, criterion regression gate, cargo-deny, CodeQL, Trivy, fuzzing, signed releases with SBOM.

- SQL Server connector (2008+ via TDS 7.3): CDC change tables, LSN watermarks pushed as T-SQL
  binary constants, real column spelling for case-sensitive collations.
- Per-source pushdown policy: operators whose semantics differ at the source (text collation,
  `AVG`, `int` `SUM`, decimal division, NULL ordering) run in the engine (ADR 0006).
- `Engine::sql` pre-evaluates uncorrelated scalar subqueries in predicates so their values can
  be pushed down.

- Trino client protocol (`[server.trino]`): `/v1/statement` with paging, idempotent retries and
  cancel, `X-Trino-*` session headers, `USE`/`SET SESSION`/`RESET SESSION`/`SHOW SESSION`, Trino
  error codes, `query_max_run_time`, `system.runtime.queries` (ADR 0012).
- Session-property registry: system properties plus catalog properties declared by connectors;
  undeclared names are rejected, performance-only Trino knobs are accepted with a warning.

### Changed
- A hash join whose estimated build side does not fit its slice of the memory pool
  runs as a sort-merge join, which spills, instead of failing with
  `EXCEEDED_LOCAL_MEMORY_LIMIT` (ADR 0003; session property `sort_merge_oversized_joins`,
  default on). Derived from Spice.ai's rule (Apache-2.0, NOTICE). A key value repeated on
  more rows than the pool holds still fails with `EXCEEDED_LOCAL_MEMORY_LIMIT`: the
  sort-merge join keeps the keys of its spilled rows in memory, and fails once they take
  the pool past its limit.
- DataFusion 54.1 train: `datafusion-federation` 0.5.5, per-connector
  `datafusion-table-providers` 0.13.1 crates, `datafusion-postgres` 0.18.0 (ADR 0002).

### Removed
- Experimental Iceberg connector and its `iceberg` feature, out of the build until an Iceberg
  integration is published for DataFusion 54 or later (ADR 0013). It was never shipped.

### Fixed
- DataFusion 54 correctness guards (ADR 0014): `NOT IN (subquery)` as a condition of
  `WHERE`/`HAVING` joined by `AND` keeps its NULL semantics (dynamic filter, filter
  inference, sort-merge join, constant and correlated forms); nested loop joins over the
  memory limit fail instead of running the spill fallback that repeated, dropped or lost
  rows.
- `x [NOT] IN (subquery)` elsewhere (under `OR`, in `CASE`, `coalesce`, `IS NULL`) answered
  FALSE where SQL says UNKNOWN (DataFusion 53 and 54); when either side may be NULL it now
  fails with `NOT_SUPPORTED` (ADR 0014, defect 12).
- Text compared with a number now fails with `TYPE_MISMATCH` and `time ± interval`
  with `NOT_SUPPORTED` on the engine's client protocols, instead of answering rows.
- A partition that panicked inside the output merge truncated the result in silence;
  the query now fails.
- `CREATE SCHEMA` without a catalog used the engine's default catalog instead of the session's.
- `codelake-connectors` without the `hive-s3` feature failed to build under `-D warnings`.
- Filters above cross-source joins now reach their source (extra `push_down_filter` before federation).
- Wrong results when a filter sat above a federated scan in cross-source joins (datafusion-federation 0.5.3); see ADR 0005.
