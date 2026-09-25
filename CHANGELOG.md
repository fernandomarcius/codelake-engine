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
- `parquet_time_zone` per hive catalog (IANA name, validated at load, default
  `America/Sao_Paulo`): `INT96` timestamps are written as the instant of the wall-clock
  time in that zone and read back, as Trino's `hive.parquet.time-zone` (ADR 0007,
  ISS-003). **Risk:** files another engine wrote in UTC read three hours earlier; set it
  to the zone of every engine that shares the warehouse.

- Operations runbooks (`docs/operations/`): install (Compose, Kubernetes), configure, upgrade and
  rollback, sizing, incidents, compaction, backup and restore, observability, migration from the
  reference engine and back, go-live checklist.
- Canary (`scripts/ops/canary.sh`): recorded answers (value, row count, sha256, rows, bounds) or
  equality with a reference engine, latency and freshness objectives; one JSON line per run, exit
  status for alerting, optional node_exporter textfile metrics. Self-test:
  `scripts/ops/canary-selftest.sh`.
- `deploy/compose/codelake.yml` (the engine alone on one host, ports on loopback) and a generic
  production configuration example (`docs/examples/codelake.production.toml`).

- Query governance for every client protocol (`[governance]`, ADR 0016): a bounded admission
  queue (`max_running_queries`, `max_queued_queries`, `queue_timeout_secs`), per-user limits
  (`max_running_per_user`, `max_queued_per_user`) and resource groups
  (`[[governance.resource_group]]`, selected by user and source); a full queue is refused with
  `QUERY_QUEUE_FULL` over HTTP and SQLSTATE `53400` on the PostgreSQL wire. Time limits
  (`query_max_run_time` from submission, `query_max_execution_time` from admission, configured
  defaults, PostgreSQL `statement_timeout`) stop the query's operators and return their memory
  to the pool, also for a PostgreSQL portal the client stopped reading. Metrics
  `codelake_queries_{running,queued,rejected_total,timed_out_total,completed_total}` and
  `codelake_resource_group_{running,queued}`.
- Stopping a query from outside, on both client protocols: `CALL system.runtime.kill_query(query_id
  => '…', message => '…')` (`ADMINISTRATIVELY_KILLED`, owner only, no admission slot needed),
  PostgreSQL cancel requests (`psql` Ctrl-C, JDBC `Statement.cancel`) while a statement waits,
  plans or sends rows (SQLSTATE `57014`), and `DELETE /v1/query/{id}` for PostgreSQL queries
  too. The memory returns to the pool. The PostgreSQL statement span carries `codelake.query_id`.

### Changed
- The query registry moved to the core: `system.runtime.queries` lists the queries of both
  protocols and exists on the PostgreSQL wire too; `resource_group_id` is `["global"]` (or
  `["global", "<group>"]`) instead of NULL. `[server.trino] history_size` and
  `history_retention_secs` are deprecated in favour of `[governance]` (still honored when set).
- PostgreSQL wire: `statement_timeout` now bounds execution and the sending of rows, not only
  planning; a timed-out statement answers SQLSTATE `57014` with
  `Query exceeded maximum time limit of …`.
- A hash join whose estimated build side does not fit its slice of the memory pool
  runs as a sort-merge join, which spills, instead of failing with
  `EXCEEDED_LOCAL_MEMORY_LIMIT` (ADR 0003; session property `sort_merge_oversized_joins`,
  default on). Derived from Spice.ai's rule (Apache-2.0, NOTICE). A key value repeated on
  more rows than the pool holds still fails with `EXCEEDED_LOCAL_MEMORY_LIMIT`: the
  sort-merge join keeps the keys of its spilled rows in memory, and fails once they take
  the pool past its limit.
- DataFusion 54.1 train: `datafusion-federation` 0.5.5, per-connector
  `datafusion-table-providers` 0.13.1 crates, `datafusion-postgres` 0.18.0 (ADR 0002).
- **Breaking:** hive `INT96` timestamps convert through the catalog's `parquet_time_zone`,
  `America/Sao_Paulo` by default; they were stored and read as UTC. Set
  `parquet_time_zone = "UTC"` to keep the former behavior (ADR 0007, ISS-003).
- **Breaking:** a comparison of a `char(n)` value (`=`, `<`, `IN`, `LIKE`, `BETWEEN`,
  `CASE x WHEN`, join conditions) fails with `NOT_SUPPORTED` wherever it is in the plan,
  until the engine compares char values padded; `CAST(ch AS varchar)` compares text
  (ADR 0007, red-team W1-4). So do char values as join keys (`USING`, `NATURAL`,
  `INTERSECT`, `EXCEPT`), in a `UNION` (with or without `ALL`) and as arguments of
  `nullif`, `greatest` and `least` (DEFECT-W1T-2), and in a `VALUES` list that puts
  them with other text; `TRY_CAST(x AS char(n))` fails with `NOT_SUPPORTED`. A value
  computed from a char value (`max(ch)`, `lag(ch)`, `coalesce(ch, …)`, `upper(ch)`,
  `ch || 'x'`) counts as one.
- **Breaking:** text compared with a number (`varchar_col > 9`, `x IN ('1.0')`) fails with
  `TYPE_MISMATCH` on the engine's client protocols, as on Trino; cast explicitly
  (ADR 0014). It answered rows before, through DataFusion's coercion.
- **Breaking:** on the PostgreSQL wire, typed text compared with a number
  (`varchar_col > 9`, `'7'::varchar = 7`, `varchar_col IN (1, 2)`) fails with SQLSTATE
  `42883` ("operator does not exist: text > integer"), as on PostgreSQL, and
  `time ± interval` with `0A000` (ADR 0014, ISS-006). It answered rows before, through
  DataFusion's coercion, whose direction changed between DataFusion 53 (compared text) and
  54 (converts the text to the number). An untyped quoted literal still takes the other
  side's type (`int_col > '20'`), as on PostgreSQL.

### Removed
- Experimental Iceberg connector and its `iceberg` feature, out of the build until an Iceberg
  integration is published for DataFusion 54 or later (ADR 0013). It was never shipped.

### Fixed
- Red-team W1 (ADR 0007): a NANOSECONDS hive catalog read `INT96` timestamps outside
  1677–2262 as other instants (1500-06-01 as 2084-12-18) and its `OPTIMIZE` rewrote them
  into the files; `OPTIMIZE` in any other catalog rounded stored nanoseconds to
  microseconds. Timestamps are now read and compacted exactly; an instant Arrow
  nanoseconds cannot hold is an error naming it (ADR 0009).
- `char(n)` comparisons above the hive scan (over `LIMIT`, a window, a volatile predicate)
  dropped rows the reference engine returns; they are refused (see Changed).
- `TRY_CAST('ab' AS char(5)) = 'ab '` answered false and `VALUES ('ab '),
  (CAST('ab' AS char(5)))` held two distinct values (the reference: true, one value):
  the engine held the char value as unpadded text. Both are refused (see Changed).
- A char value passed through an aggregate, a window function, a function, a `CASE` or
  `||` (`max(ch)`, `lag(ch)`, `coalesce(ch, …)`, `upper(ch)`) was compared as plain text:
  `SELECT max(ch) FROM t UNION SELECT s FROM v` held two rows where the reference holds
  one. Such values are followed through the plan and refused like the column (see
  Changed).
- Tables of the in-memory catalog (`CREATE TABLE datafusion.…`) kept their rows outside
  the memory pool; they are reserved in it for as long as the table exists. `UPDATE` and
  `DELETE` of such a table run when the plan executes (they changed the rows while
  planning, `EXPLAIN` included), and an `UPDATE` the pool cannot hold fails with the rows
  unchanged instead of growing the table past the ceiling (DEFECT-W1T-3).
- `UPDATE` and `DELETE` changed every row of the table when their `WHERE` held a
  subquery (`id IN (SELECT 2)`, `EXISTS`, a correlated value) or was false
  (`WHERE 1 = 0`, `id = 1 AND id = 2`): DataFusion 54.1 passes the table only the
  conditions it finds on filters. Every condition now reaches the table; a `WHERE`
  with a subquery or a join fails with `NOT_SUPPORTED` before a row changes, and a
  false one changes no row.
- `OPTIMIZE` in a catalog whose `parquet_time_zone` is not UTC rewrote `INT64`
  timestamps of other writers (Spark, pandas) as `INT96` without converting them: they
  read shifted by the zone offset afterwards. They are now kept; a wall-clock time the
  zone skips makes `OPTIMIZE` fail instead of changing it (DEFECT-W1T-1). A wall-clock
  time whose instant nanoseconds cannot hold (after 2262-04-11 23:47:16 UTC) fails
  naming the range, not as a time the zone skips.
- Test helpers and the golden generator wait until the reference server lists an active
  worker, instead of sending statements that fail with "No nodes available to run query".
- DataFusion 54 correctness guards (ADR 0014): `NOT IN (subquery)` as a condition of
  `WHERE`/`HAVING` joined by `AND` keeps its NULL semantics (dynamic filter, filter
  inference, sort-merge join, constant and correlated forms); nested loop joins over the
  memory limit fail instead of running the spill fallback that repeated, dropped or lost
  rows.
- `x [NOT] IN (subquery)` elsewhere (under `OR`, in `CASE`, `coalesce`, `IS NULL`) answered
  FALSE where SQL says UNKNOWN (DataFusion 53 and 54); when either side may be NULL it now
  fails with `NOT_SUPPORTED` (ADR 0014, defect 12).
- Text compared with a number now fails with `TYPE_MISMATCH` and `time ± interval`
  with `NOT_SUPPORTED` on the engine's client protocols, and with `42883`/`0A000` on the
  PostgreSQL wire, instead of answering rows.
- A partition that panicked inside the output merge truncated the result in silence;
  the query now fails.
- `CREATE SCHEMA` without a catalog used the engine's default catalog instead of the session's.
- `codelake-connectors` without the `hive-s3` feature failed to build under `-D warnings`.
- Filters above cross-source joins now reach their source (extra `push_down_filter` before federation).
- Wrong results when a filter sat above a federated scan in cross-source joins (datafusion-federation 0.5.3); see ADR 0005.
