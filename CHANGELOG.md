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
- `${ENV:NAME}` references in any string value of `codelake.toml`, resolved when the file is
  loaded; missing variables are named with their keys, all at once, and resolved values never
  appear in errors (ADR 0015). Secrets keep their `*_env` keys.
- `codelake config import --from <dir> --out codelake.toml --env-out codelake.env`: catalog
  `.properties` files (`sqlserver`, `postgresql`, `hive` with a file metastore) become sources;
  secrets go to an env file with mode 0600, never to the TOML or the terminal; other catalogs
  are skipped with a reason (exit 3); `hive.parquet.time-zone` becomes `parquet_time_zone`, and a
  hive catalog without it needs `--reference-time-zone` (never guessed) (ADR 0015, ISS-027).
- Shadow-run harness (`scripts/shadow-run.sh`, `tools/shadow`): runs a dbt project against a
  reference and a candidate engine with the same cutoff, compares every output table with
  DuckDB (row count, order-insensitive checksum, per-column checksums, differing rows by hash),
  samples peak RSS per side, and writes JSON and Markdown reports per cycle and cumulative;
  declared-type and name-case differences are divergent unless an expected-differences rule
  documents them (a rule file named by the run; ISS-018 and ISS-019 shipped), and no rule
  covers values;
  self-test with SQL Server CDC and mutation scenarios (`tools/shadow/selftest.sh`,
  docs/shadow-run.md).

- Shared compilation cache for local builds: the toolbelt image ships sccache 0.18.0 (pinned,
  checked by SHA-256) and `scripts/dev.sh` mounts one `codelake-sccache` volume for every
  checkout (`CODELAKE_SCCACHE_SIZE`, 20G by default; `CODELAKE_SCCACHE=0` turns it off; off in
  CI unless asked). Targets stay one per checkout. `scripts/sccache-check.sh` proves entries
  are keyed by content, not mtime; `gates.sh fast` prints the run's hits (docs/gates.md).
- Differential gate `gates.sh differential` (`tools/differential`): declarative TOML cases run
  against CodeLake and an oracle (Trino 431 over HTTP, PostgreSQL 17 over the wire, or fixed
  rows), CodeLake reached over its Trino protocol and its PostgreSQL wire; outcomes `WRONG`,
  `RECUSA`, `EXTRA`, `ERRO`, `ORDEM`, `TIPO`, `TRAVA`; JSON report; exit 1 on any outcome a case
  does not expect. Divergences are expected only with an issue, ADR or front named (`known`
  open defect or `accepted` divergence) and are listed, never counted green; a known defect
  that is fixed fails until its marker goes. The engines run as sibling containers on a private
  network, no port published, removed at the end. First cases: char(n) (CH1–CH16, ISS-025
  shapes, W1 verdict probes), NULL-only Parquet row groups under null-safe joins, `-0.0`,
  PostgreSQL cursors and transaction blocks, PostgreSQL wire against Trino
  (docs/differential.md); divergences no front fixes are recorded in ISS-032. Part of the
  fronts' Definition of Done (docs/gates.md).

### Changed
- Session properties and table-procedure arguments of type data size
  (`file_size_threshold`, `target_max_file_size`) accept `KiB MiB GiB TiB PiB`
  besides `kB MB GB TB PB`, all powers of 1024 (`MB` keeps its meaning); the
  reference engine refuses the IEC names. Size errors keep the reference
  engine's message and add the accepted units (ADR 0009, ISS-020).
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
- SQL Server `datetime`, `smalldatetime`, `datetime2(p)` and `datetimeoffset(p)` keep the precision they declare
  (`datetime` rounded to the millisecond); a CTAS into a millisecond hive catalog no longer fails (ISS-042).
- Timestamps of different precisions compare at the finer one; the simplifier no longer truncates the literal (ISS-042).
- Quoted mixed-case names of federated sources resolve without regard to case (ISS-043).
- Windows ordered by a binary key with the default frame plan (`RANGE` without offsets as `GROUPS`) (ISS-045).
- A name repeated in a later branch of a set operation is accepted (ISS-046).
- `varchar(n)` is kept from SQL Server, through `lower`/`upper`/`trim`/`substr`, and as the common type of
  `CASE`/`coalesce` of text; CTAS stores it (ISS-044).
- Shadow harness: `replaced_characters` expected-difference rule (ISS-047).
- PostgreSQL wire, memory outside the pool: a `FETCH` among other statements of a message
  (`FETCH ALL FROM c; CLOSE c`) collected its rows in memory outside the pool before sending
  them (6 M rows took the server from 114 MB to 815 MB of RSS with a 256 MiB pool). The rows
  are now held in the pool until sent; past what it gives the `FETCH` fails with `53200`, its
  cursor's statement with `EXCEEDED_LOCAL_MEMORY_LIMIT` (slot and memory given back) and the
  cursor cannot be read again (`55000`). A `FETCH` sent alone still streams (ADR 0016).
- PostgreSQL wire, statements queued behind their own cursor: with a cursor or a suspended
  portal open, a `SELECT`, a second `DECLARE`, `ABORT` or a query beside a JDBC fetch-size
  portal waited in the queue for the connection's own slot (the whole queue timeout with a
  per-user limit of one, then `57014`). A connection now holds one admission slot at most, and
  its other statements run in it at once, each registered with its own deadline; the per-user
  and per-group limits count connections with work. `ABORT` is read as `ROLLBACK` on both
  protocols (ADR 0016, ISS-031).
- PostgreSQL wire, `DISCARD ALL` failed with `XX000` and left every cursor open, held ones
  included, with their slots and memory (a connection pool resetting a connection kept them
  for the next client). It now closes every cursor and portal and resets `statement_timeout`,
  and fails with `25001` inside a transaction block, as PostgreSQL; `DISCARD PLANS`,
  `SEQUENCES` and `TEMP` answer their tags (ADR 0016, ISS-031).
- PostgreSQL wire, cursor names: `DECLARE Big` declared `Big`, so `FETCH FROM big` failed with
  `34000`; unquoted names are now folded to lower case, quoted ones kept, as PostgreSQL does.
  A cursor's query is planned as the session's queries are (operands checked, ADR 0014; the
  session's format options) (ADR 0016).
- PostgreSQL wire, `FETCH` and `MOVE` on the extended protocol: `FETCH FROM c`, `FETCH c`,
  `FETCH -1` and every `MOVE` failed at `Parse` with `XX000`; they are answered as on the
  simple protocol (rows, `55000` backward). `MOVE` is implemented on both protocols (it was
  `0A000`): it moves the cursor as the `FETCH` of the same direction would and answers
  `MOVE n` as PostgreSQL. A count beyond PostgreSQL's `int4` (`FETCH 9223372036854775807`)
  fails with `42601` instead of returning every row left (ADR 0016).
- Joins that match NULL with NULL lost the rows whose key is NULL when those NULLs sat in a
  row group of their own (what an `INSERT` of a NULL row leaves): `t EXCEPT t` returned the
  NULL row, and `t INTERSECT t`, `JOIN … ON a IS NOT DISTINCT FROM b`, `LEFT JOIN` on it and
  `[NOT] EXISTS (… WHERE b IS NOT DISTINCT FROM a)` lost it (DataFusion 54.1 pushed the join's
  dynamic filter, built from the non-NULL keys, into the probe side's scan). The filter now
  keeps NULL keys; rows match Trino 431 over tables written by Trino and by the engine
  (ADR 0014, defect 13).
- `a IS [NOT] DISTINCT FROM b AND c` was parsed as `a IS [NOT] DISTINCT FROM (b AND c)`:
  over booleans it answered other rows (`JOIN … ON a.f IS NOT DISTINCT FROM b.f AND a.id =
  b.id`), otherwise it failed with `Utf8 AND Boolean`. It now binds tighter than `AND`,
  `OR` and `NOT`, as in Trino and PostgreSQL, on both client protocols (ADR 0014, defect 14).
- `0.0` and `-0.0` compared as different values: `x = 0` dropped `-0.0` rows, `x < 0`,
  `x <> 0` and `x IS DISTINCT FROM 0` kept them, and so did `IN (0, …)`, `BETWEEN 0 AND …`,
  `CASE x WHEN 0`, `nullif(x, 0)`, comparisons of two columns and `GROUP BY ROLLUP`/`CUBE`/
  `GROUPING SETS`; a scan could skip a row group holding only `-0.0`. The two zeros are now
  one value everywhere, for `double` and `real`. **Deliberate divergence:** joins,
  `IN (subquery)` and `IN` lists of `real` match `0.0` with `-0.0`, as PostgreSQL does and
  Trino 431 does not (ADR 0017, ISS-050).
- PostgreSQL wire, transactions: `COMMIT` and `ROLLBACK` did not close cursors and portals,
  which kept their admission slot and pool memory (for good on a pooled connection) and
  answered `FETCH` after the `COMMIT`. The end of a transaction (`COMMIT`, `ROLLBACK`, a
  message or an extended-protocol `Sync` outside a transaction block) now closes them, as in
  PostgreSQL; `FETCH` afterwards fails with `34000`. **Breaking:** a `DECLARE` without
  `WITH HOLD` outside a transaction block fails with `25P01`, as in PostgreSQL; a `WITH HOLD`
  cursor outlives the transaction that commits it (ADR 0016, ISS-030).
- PostgreSQL wire, `FETCH` (wrong rows): `FETCH 0` and `FETCH FORWARD 0` returned every row
  left and emptied the cursor, and so did a count that could not be read (`FETCH 1.5`). They
  now return the row the cursor stands on, as PostgreSQL does, and an unreadable count fails
  with `42601`. A `FETCH` the SQL parser does not read (`FETCH -1 FROM c`, `FETCH c`) waited
  60 s in the queue behind its own cursor's slot; it is answered at once (a move backward
  fails with `55000`). After a failed `FETCH` in a transaction block the
  next one returned no row and no error; the transaction is now aborted (`25P02` for anything
  but `COMMIT`/`ROLLBACK`). `FETCH` answers the tag `FETCH n` (it said `SELECT n`). A
  `BINARY` cursor (rows would be read as binary) fails with `0A000`; a `DECLARE` of an open
  name with `42P03`; `CLOSE` of a missing cursor with `34000` (ADR 0016).
- PostgreSQL wire, cursors (`DECLARE … CURSOR`: psycopg2 named cursors, pandas chunked
  reads): the `DECLARE` finished at once and the cursor's stream lived in the connection with
  no admission slot, time limit or registry entry, holding its memory until `CLOSE`, and
  `FETCH ALL` collected every row left before sending the first. A cursor now runs as its
  `DECLARE`'s governed statement until it is read to the end or closed (cancel requests,
  `kill_query` and its time limit stop it); `FETCH` and `CLOSE` take no slot of their own, and
  `FETCH` streams its rows (ADR 0016).
- PostgreSQL wire, extended protocol (JDBC, psycopg3, `tokio-postgres`' `query`): an
  `Execute` without a row limit collected every row of the result in server memory, outside
  the pool, before sending the first; the statement then ended (slot freed, time limit over)
  while its client had read nothing. Rows now stream to the client as they are produced, so
  the statement holds its slot and its time limit until they are read (ADR 0016).
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
