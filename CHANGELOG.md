# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Performance
- Freed memory is returned to the system within a second while the memory pool is below an eighth of its
  limit, and at once above it (it was always at once). TPC-H SF10 on 16 vCPU: 2.6x faster and 65% less
  CPU (sum of p50 51.6 s → 19.8 s, CPU 507 s → 177 s; Trino 431: 24.4 s, 243 s). The memory ceiling holds.
- `avg` of `decimal(p,s)` with `p <= 19` sums in 128 bits instead of 256; decimal operators compute their
  powers of ten once per batch.
- Found by the shadow run on production routines (2026-09-28), where CodeLake was far slower than Trino 431:
  - SQL Server: a comparison of a `date`, `smalldatetime` or `datetime2(0..6)` column with a date or
    timestamp literal is pushed down, the literal rendered as `CAST('…' AS datetime2(7))` (exact for those
    types); `datetime` and `datetime2(7)` still compare in the engine. A time window over a sink table
    (`_sink_loaded_at > now - 60 min`) read the whole table (459 s for 28 tables).
  - A federated sub-plan below a local operator returns only the columns the engine uses; it returned
    every column of its tables.
  - `<catalog>.information_schema` filtered by `table_schema`/`table_name` (or `schema_name`) opens only
    the matching tables; it opened every table of the catalog, one metadata query each on a remote
    source (56 lookups of one table each: 288 s).
  - `COUNT(DISTINCT x)` next to `COUNT(*)` runs in two spillable aggregations; one million distinct
    tuples exhausted the memory limit.
  - The nightly reconciliation (dbt, 921 s against 75 s) filters SQL Server tables by the keys of a local
    replica (`id IN (SELECT id FROM hive…)`) and a window (`CAST(dataregistro AS timestamp(3)) >=
    date_add('day', -7, TIMESTAMP '… .133')`); neither reached SQL Server, which returned whole tables:
    - A hash join's dynamic filter reaches the federated scan it probes: its keys as an `IN` list
      (`execution.dynamic_filter_max_keys`, 150), their min/max always, each term sent as the source's
      policy accepts it (SQL Server: text keys as an `IN` superset under its collation, never a text
      min/max; ADR 0006).
    - SQL Server: a `datetime` or `datetime2(7)` column compared with a literal inside a second is sent
      whole-second bounds (`>=` the second before, `<` the second after) as a superset. Upper bounds are
      always strict: a `datetime2(7)` value 100 ns past a whole-second `<=` bound was lost at the source.
    - Reproduction (2 million rows in SQL Server 2022, 13 936 local keys): 11.4 s → 0.50 s, SQL Server
      returns 58 521 rows instead of 2 000 000; same result.

### Added
- Distributed execution (M4, ADR 0011), behind the `distributed` feature: a `[distributed]` coordinator
  plans every query and sends the stages of those that read only Hive tables to its workers
  (datafusion-distributed 3.0.0, patched in `vendor/`); each node keeps its memory ceiling. Single node
  stays the default.
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
- **Breaking (ISS-011):** CodeLake names by default. The HTTP protocol's headers are `X-CodeLake-*`
  (`[server.trino] header_prefix`) and the reported version is CodeLake's (`reported_version`); hive
  metastores write `.codelakeSchema`, `.codelakePermissions` and `CodeLake View` markers
  (`[source.metastore_names]`). Clients and warehouses of the reference engine set its names in the
  configuration, or `codelake hive migrate-names` renames a warehouse (docs/operations/migration.md).
- **Breaking:** hive tables keep the case a column name was written with: Parquet files written
  by `CREATE TABLE AS`, `INSERT` and `OPTIMIZE` name a quoted `"CustomerCode"` column
  `CustomerCode` (it was `customercode`), so readers of the files (SQL Server `OPENROWSET`,
  pandas) see the name as written; a consumer that expects lower-case Parquet column names under
  a case-sensitive comparison must follow. The metastore keeps folded names and records the
  written one as `codelake.name` (column property; table, view and schema parameter), which the
  reference engine reads and keeps. Queries resolve any spelling. Two names of one level that
  differ only by case are refused with an error naming both; statistics and reads match file
  columns without regard to case, and a file with two spellings of one column fails when that
  column is read (ADR 0007, ISS-018).
- **Breaking:** `CREATE OR REPLACE TABLE [… AS]` on a hive catalog replaces the table instead
  of failing with "This connector does not support replacing tables" (the reference engine's
  answer). The new table is written aside and exchanged with the old one's directory in one
  step: readers see the old or the new table, never a mix or nothing; a failure or a crash
  before the swap leaves the old table. Views, external tables, directories without a table
  and warehouses that cannot exchange two directories are refused with nothing changed. A
  plan made against the replaced table fails and the statement must be planned again; a
  PostgreSQL prepared statement is planned again at each `Execute`, as PostgreSQL does, so it
  reads the new table, and is refused with SQLSTATE `0A000` ("cached plan must not change
  result type") when the new table changes its columns (ADR 0007, ISS-017).
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
- **Breaking:** `char(n)` values follow the reference engine's semantics instead of being
  refused (ADR 0015, ISS-024): every char value is held padded to `n` (so
  `length(CAST('ab' AS char(5)))` is 5 and `CAST(CAST('ab' AS char(5)) AS varchar)` is
  `'ab   '`), and where it meets other text both are converted to the common char type:
  comparisons (`=`, `<`, `IN`, `BETWEEN`, `LIKE`, `CASE x WHEN`, `nullif`, `greatest`,
  `least`, join conditions, `IN`/`ANY`/`ALL` subqueries, scalar subqueries) answer what
  Trino 431 answers, and `CASE`, `coalesce`, `UNION`, `INTERSECT`, `EXCEPT`, `VALUES`,
  `USING` and `||` output the common type, padded (`char(65536)` against unbounded
  `varchar`). The refusals of W1-4 and DEFECT-W1T-2 are gone; `TRY_CAST(x AS char(n))`
  is answered. `typeof`, the client protocol and `CREATE TABLE … AS` see `char(n)` for
  computed char values (`max(ch)`, `coalesce(ch, 'x')`, `ch || 'x'`).
- **Breaking:** refused like the reference engine refuses them: `NATURAL JOIN` over any
  columns ("Natural join not supported"), `char(n) || <unbounded varchar>` and
  `char(n) || NULL`. Refused because the engine cannot guarantee the reference's answer
  (ADR 0015): a char value output together with text of a length it does not derive
  (`CASE … THEN ch ELSE element_at(t.arr, 1) END` over an array column,
  `to_iso8601(timestamp)`), and char values in arrays that are not written out
  (`contains(array_agg(ch), 'ab')`). The result lengths of `replace`, `regexp_replace`,
  `url_encode`, `soundex`, `json_extract_scalar`, `url_decode`, the `url_extract_*`
  functions, `from_utf8`, `to_iso8601(date)`, an element of `split`, `regexp_split` or
  `regexp_extract_all` (`element_at`, `[i]`), `to_base64url`, `human_readable_seconds`,
  `current_timezone` and `version` are derived as the reference engine derives them, so
  a char value output with them is answered.
- **Breaking:** `TRY_CAST(x AS varchar(n))` cuts to `n` characters and
  `TRY_CAST(x AS timestamp(p))` rounds to `p` digits, as `CAST` does (`NULL` where `CAST`
  fails); they returned the value as it was.
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
- Found by the shadow run on production routines (2026-09-29):
  - `max_by`/`min_by` (and the `n` form) accept a row or an array as the key (`FUNCTION_NOT_FOUND`
    before), as the reference orders them: rows field by field, arrays element by element with a prefix
    first; a NULL field or element the comparison reaches is `NOT_SUPPORTED`. A NaN key ranks below every
    number for `max_by` and above for `min_by`, so it is picked only when every key is NaN (such a group
    answered NULL, and the `n` form dropped NaN keys).
  - `CREATE TABLE AS` (and `CREATE OR REPLACE`) of a `UNION ALL` whose branches differ in field metadata
    failed (`target schema is not superset of current schema`): SQL Server columns record their type
    (`numeric` in one branch, `decimal` in another). A batch now takes the announced schema when only
    metadata and nullability differ; another difference is still an error.
- `SHOW CREATE TABLE` failed for every table (`COLUMN_NOT_FOUND: No field named definition`) and `SHOW CREATE
  VIEW` was refused. Both answer the statement as the reference engine prints it, one `varchar(n)` row: types
  in SQL syntax, `NOT NULL` where the source refuses nulls, reserved and irregular names quoted, a hive
  table's `WITH (format = '…')` and a view's `SECURITY` mode. A view is not a table (`NOT_SUPPORTED`), nor
  a table a view; a missing one is `TABLE_NOT_FOUND`.
- A session state rebuilt with another configuration could resolve `extract(...)` to DataFusion's
  `date_part` (an `int`) instead of the engine's (`bigint`): `rebuild_state` keeps the engine's functions.
- SQL Server `datetime`, `smalldatetime`, `datetime2(p)` and `datetimeoffset(p)` keep the precision they declare
  (`datetime` rounded to the millisecond); a CTAS into a millisecond hive catalog no longer fails (ISS-042).
- Timestamps of different precisions compare at the finer one; the simplifier no longer truncates the literal (ISS-042).
- Quoted mixed-case names of federated sources resolve without regard to case (ISS-043).
- Windows ordered by a binary key with the default frame plan (`RANGE` without offsets as `GROUPS`) (ISS-045).
- A name repeated in a later branch of a set operation is accepted (ISS-046).
- `varchar(n)` is kept from SQL Server, through `lower`/`upper`/`trim`/`substr`, and as the common type of
  `CASE`/`coalesce` of text; CTAS stores it (ISS-044).
- Shadow harness: `replaced_characters` expected-difference rule (ISS-047).
- PostgreSQL wire, `FETCH` (wrong rows): a `FETCH n` executed as a portal with a row limit
  (JDBC's fetch size) read its cursor as it sent, so a `FETCH` of the same cursor run while the
  portal was suspended took the portal's rows (PostgreSQL: the portal 3–6 and the `FETCH` 7;
  CodeLake: the `FETCH` 4 and the portal 5–7). It now reads its rows at its first `Execute`, held
  in the pool until sent, as PostgreSQL does (ADR 0016, ISS-032).
- PostgreSQL wire, cancel requests: canceling a statement failed every cursor of the
  connection, so a `WITH HOLD` cursor read afterwards answered `57014`. A cancel now stops the
  statements the message runs (the one executed, the cursor a `FETCH` reads, the portal an
  `Execute` resumes) and the other cursors read on, as in PostgreSQL.
- PostgreSQL wire, savepoints: `SAVEPOINT` and `RELEASE` failed with `XX000`, `ROLLBACK TO
  SAVEPOINT` ended the whole transaction and closed every cursor, and `COMMIT AND CHAIN`
  began no transaction. They now behave as PostgreSQL for cursors and the transaction status;
  a `ROLLBACK TO SAVEPOINT` after a statement that may write fails with `0A000` (the engine's
  writes are not transactional; ISS-032).
- PostgreSQL wire, cursors: a `DECLARE` with parameters on the extended protocol (psycopg3's
  server-side cursors) failed with `XX000`; `DISCARD ALL` ran inside an extended-protocol
  pipeline (PostgreSQL: `25001`); `FETCH FROM c`, `FETCH c` and `MOVE` among other statements
  of a message failed with `42601`; cursor names were not cut to 63 bytes; `SHOW` answered the
  tag `SELECT 1` and `START TRANSACTION` the tag `BEGIN`. All now answer as PostgreSQL.
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
- Column names resolve in any spelling wherever a statement names a column: a quoted
  `"CustomerCode"` in an `INSERT` column list or a `JOIN … USING` list, and in any statement on
  the PostgreSQL wire (simple and extended protocols), failed with `No field named`; an unquoted
  name with a non-ASCII capital (`DESCRIÇÃO`) was folded to ASCII lower case only
  (`descriÇÃo`, also the name written to the files) and did not resolve (ADR 0007, ISS-018).
- Hive catalog: `DROP VIEW` deleted every file in the view's directory, including files of
  another writer when the view had been created over an existing directory (as the reference
  engine does); `ALTER VIEW … RENAME` moved them. Both now touch only the view's metadata
  (ADR 0007, ISS-023, deliberate divergence).
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
- Rows and arrays not written out (a subquery's or a CTE's column, `array_agg`, a `CASE`
  of rows) were compared by DataFusion as one value: `ROW(1, NULL) = ROW(1, NULL)` was
  true where Trino 431 answers `NULL`, a join on such keys matched `NULL` with `NULL`, and
  char values inside were compared unpadded. They are now compared value by value (a
  join on them runs as a nested-loop join), and char values inside rows and arrays of
  different lengths are padded to their common type (`arrays_overlap`, `array_intersect`,
  `array_union`, `array_except`, `flatten`, `CASE`/`coalesce`/`if` of rows or arrays,
  `UNION` columns) (ADR 0015). A row column compared with a row of other field names
  (`r = ROW(1, 'a')` over `row(x bigint, y varchar)`) failed ("Unsupported CAST"); rows
  compare by position.
- `rtrim(ch, x)`, `trim(ch, x)` and `trim(TRAILING|BOTH x FROM ch)` of a `char(n)` value
  kept the spaces that ended up last; `typeof` of a constant answered `varchar`
  (`typeof('a')` is `varchar(1)`, `typeof(CAST('ab' AS char(3)))` `char(3)`).
- `INTERSECT ALL` and `EXCEPT ALL` between a hive column and an in-memory one (Arrow
  `Utf8` and `Utf8View`) kept, or dropped, every copy of a row again: the multiset rewrite
  did not see the set operation behind type coercion's cast.
- Floats: `WHERE x = 0` of a `double` dropped `-0.0` and `x < 0` kept it (Trino 431:
  `-0.0 = 0.0`); a window partitioned by a float column under a filter `x = <constant>`
  failed ("Expects PARTITION BY expression to be ordered"), also in `INTERSECT ALL`
  (ADR 0008).
- On the PostgreSQL wire `varchar(n)` was unbounded text: `CAST(v AS varchar(1))` did not
  cut, a `varchar(2)` column created there took `'abcdef'`, and `CREATE TABLE … AS SELECT
  CAST(v AS varchar(2))` kept whole values. Casts now cut and the column's `INSERT`s are
  checked, as on the Trino protocol (ADR 0015).
- `INSERT INTO t SELECT id, s, s FROM u` failed ("Projections require unique expression
  names"); it inserts by position, as Trino 431.
- `char(n)` values still answered other rows than Trino 431 in shapes the matrix did not
  cover (ADR 0015): a view over a char column dropped a trailing tab or line feed before
  padding; `ltrim(ch)` kept the padding; rows written out (`ROW(ch, c3) = ROW('ab',
  'ab')`, `(ch, id) IN (...)`) compared without char semantics in a select list and made
  a `NULL` field equal; elements of arrays of char values (`ARRAY[ch, c3][2]`,
  `array_max`, `array_agg(ch)[1]`), `array_remove`, array concatenation, `min_by`,
  `max_by`, `map_agg`, `histogram` lookups and `try` lost the char type; `INTERSECT` and
  `EXCEPT` kept values equal as char apart. They now answer Trino's rows; a cast to an
  array, map or row of `char(n)`/`varchar(n)` and `char(n)` on the PostgreSQL wire are
  refused instead of answering unpadded values. Text computed from a char value is
  typed `varchar(n)` (clients, `typeof`, `CREATE TABLE AS`).
- `ARRAY[1] || CAST(NULL AS integer)` answered `[1, NULL]`; Trino 431 answers `NULL`.
- `INTERSECT ALL` kept every left copy of a row found on the right (`VALUES 1, 1, 1
  INTERSECT ALL SELECT 1` answered three rows) and `EXCEPT ALL` removed every copy of
  it (`VALUES 1, 1, 2 EXCEPT ALL SELECT 1` answered `[2]`); they are now the multiset
  operations Trino 431 answers (ADR 0014, defect 13).
- Red-team W1 (ADR 0007): a NANOSECONDS hive catalog read `INT96` timestamps outside
  1677–2262 as other instants (1500-06-01 as 2084-12-18) and its `OPTIMIZE` rewrote them
  into the files; `OPTIMIZE` in any other catalog rounded stored nanoseconds to
  microseconds. Timestamps are now read and compacted exactly; an instant Arrow
  nanoseconds cannot hold is an error naming it (ADR 0009).
- `char(n)` values answered other rows than the reference engine wherever the refusals did
  not see them (P1–P16 of the W1 verification): `IN`/`NOT IN`/`= ANY` with the char value
  in the subquery, scalar subqueries of char values, `CASE`/`coalesce` mixing char and
  text, `CAST … AS char(n)` held unpadded; and over the hive scan (under `LIMIT`, a
  window). They now answer the reference's rows (ADR 0015).
- `max(ch)` over a char column of an in-memory table panicked (`Utf8`×`Utf8View`) and
  `CAST(ch AS varchar)` failed ("expected Utf8View but found Utf8"): `INSERT` wrote
  `Utf8` values into a `Utf8View` column. Values keep the column's Arrow type.
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
