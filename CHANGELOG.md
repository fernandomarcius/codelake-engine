# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Performance
- SQL Server: `lower(column)`/`upper(column)` compared with ASCII literals (`=`, `IN`) reach SQL Server as a
  filter that keeps at least the engine's rows (the column under a case-insensitive Latin collation, its
  `LOWER`/`UPPER` under another Latin one; `%` in a `LIKE` where a character beyond ASCII also maps to the
  letter, as `İ` to `i`); the engine narrows them. The monitoring screen's `WHERE lower(tabela) =
  'recebidas' ORDER BY started_at DESC LIMIT 100` read the whole table: 2.1 million rows in SQL Server
  2022, 0.717 s → 0.047 s (p50 of 7); `upper(tabela) IN ('RECEBIDAS', 'PAGAS')` 0.487 s → 0.050 s.
- SQL Server: `ORDER BY … LIMIT n [OFFSET m]` over a sub-plan it evaluates exactly asks it for its first
  `m + n` rows (`TOP … ORDER BY`), by plain columns of integer, decimal, `date`, `smalldatetime`,
  `datetime2(0..6)` or `datetime` type with NULLs where it puts them; the engine sorts them again.
  `ORDER BY started_at DESC LIMIT 100` over the same table: 0.455 s → 0.186 s.
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
    - A null-safe join's dynamic filter (`(bounds AND keys) OR key IS NULL`) is sent term by term, so its keys
      still go when the source refuses its bounds (text); a build-side literal is never cast to a type that
      changes its value.
    - Reproduction (15 million rows in SQL Server 2022, 14 850 local text keys): 93–108 s → 1.9–2.5 s,
      SQL Server returns 453 522 rows instead of 15 000 000; same result. The keys are past the `IN` limit,
      so the window does the work; listing them all (`dynamic_filter_max_keys = 20000`) returns 14 570 rows
      but takes 3.1 s (SQL Server compiles the 14 850-literal `IN`).
- Every federated scan of a query returns only the columns the engine uses; only the first one found in
  each optimizer pass did, three for a query. Found by the shadow run (2026-09-30): the nightly row-level
  reconciliation (eleven scans of SQL Server, each under `SELECT *, ROW_NUMBER() OVER (…)`) asked eight of
  them for every column, 380 s against Trino 431's 197 s. Reproduction (SQL Server 2022, 70 columns, three
  tables of 5 million rows and four of 500 000): 211–213 s → 38–39 s, Trino 431 36–55 s; same rows.
- A partitioned hash join's dynamic filter reaches a federated scan as `IN` lists only while the lists of
  every partition hold `execution.dynamic_filter_max_keys` keys together; past it, only the rest of the filter
  (the bounds) is sent. Each partition listed up to that many, so the source got up to that many times the
  partitions. Found by the shadow run (2026-10-01): once a hash join without sizes built its right input,
  the Warehouse Comissões screen (a 5.8-million-row SQL Server view joined with three small views) sent the
  view the 432 keys of one of them, which kept nearly every row, and SQL Server compared each row with every
  key: 157–178 s against 21–25 s before and Trino 431's 29–48 s. Now 26–33 s, same rows; the build sides
  stay the small views (1 MB of hash tables instead of 1.5 GB).

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
- **Breaking (ISS-059):** `char(n)` values are never padded, from any origin: a hive table (and a file
  another tool wrote with trailing spaces), an in-memory table or view, `CAST`/`TRY_CAST`, a literal,
  and the common char type of `COALESCE`, `CASE`, `UNION`, `INTERSECT`, `EXCEPT`, `VALUES` and `USING`
  (`COALESCE(varchar, char(36))` held 65 536 characters per value, as Trino 431). A char value is held,
  returned on both protocols and written without trailing spaces; its type stays `char(n)`. Comparisons
  keep Trino 431's answers: equality, `GROUP BY` and joins ignore trailing spaces; `<`, `BETWEEN`,
  `ORDER BY`, `min`/`max`, `min_by`/`max_by` by a char key and `greatest`/`least` order char values as
  if padded (`'a' || chr(9)` below `'a'`); `LIKE` matches the value padded to `n`. What follows from
  the value differs from Trino 431 on purpose: `length(CAST('ab' AS char(5)))` is 2 (Trino: 5),
  `CAST(CAST('ab' AS char(5)) AS varchar)` is `'ab'` (Trino: `'ab   '`), `CAST('ab' AS char(3)) || 'x'`
  is `'abx'` (Trino: `'ab x'`), `substr` of the value. The differential cases that showed Trino's
  padding are accepted divergences (ISS-059). A map keyed by `char(n)` values looked up with a char
  value of another length is answered (it was refused).
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
- `now()`, `current_timestamp`, `current_timestamp(p)` and `from_unixtime` are `timestamp with time zone`
  values in the session's zone (`X-Trino-Time-Zone`), as in the reference: they were in UTC, the engine's,
  whatever the client sent (the same instant shown at another hour: `hour(now())` 12 instead of 9 in
  `America/Sao_Paulo`, `CAST(now() AS timestamp)` the UTC wall clock). The session's functions were built
  for the engine's zone and never updated for the client's. `current_timestamp(p)` and `localtimestamp(p)`
  failed with "precision must be a constant".
- `SET TIME ZONE 'zone'` and `SET TIME ZONE LOCAL` (they failed with "SET variant not implemented"): the
  zone is sent back as the session property `time_zone_id`, which wins over `X-Trino-Time-Zone`, as the
  reference does.
- PostgreSQL, found by the shadow run of the Warehouse boletos panel (2026-10-01; its 28 queries failed;
  ISS-060):
  - Views, materialized views and foreign tables did not exist (`TABLE_NOT_FOUND`): relations were listed from
    `pg_tables`. They are listed from `pg_class` as the reference lists them (tables, partitioned tables and
    their partitions, views, materialized views, foreign tables; `pg_catalog` listed without relations; not
    filtered by privilege: what the user may not read fails at the source). Columns are read from the catalog
    for every kind (a foreign or partitioned table was typed from its first row, an empty one had none).
  - A relation created after boot needed a restart. A name the listing lacks is looked up at the source when
    a query names it; `information_schema` and `system.jdbc` read the listing again once it is older than one
    second.
  - An array of dates refused its whole relation (`unsupported data type: _date`). Arrays follow the
    reference's `array-mapping=AS_ARRAY`: `date`, `time`, `timestamp`, `timestamptz`, `numeric(p,s)`, text
    types, `uuid`, `json`, `jsonb`, `money`, enums, numbers and booleans, nested per declared dimension;
    arrays of other types are left out of the relation, as the reference leaves them.
  - Every connection ran `SET json_serialization_enable` (a Redshift parameter), one `ERROR` in the server's
    log per connection. The connector opens plain sessions.
- PostgreSQL, found by the second shadow run of the Warehouse boletos panel (2026-10-01 22:06, ISS-060):
  - **Wrong result**: statements ran in the server's (or the role's) default `TimeZone`, so a view reading
    `current_date` or `now()` answered another day between 21:00 and 24:00 in `America/Sao_Paulo`
    (`saude_fontes`: `dias_sem_recebimento` 23 against 22, `recebimentos_30d` 22 against 30). Each
    statement now runs in the client session's zone (`X-Trino-Time-Zone`, `SET TIME ZONE`; `Etc/UTC`
    without one), set on its connection before it runs, so a pooled connection carries no zone from one
    statement to the next. The reference engine runs its sessions in `Etc/UTC` whatever the client's zone:
    with a client zone CodeLake answers the client's day (accepted divergence, ISS-060).
  - `timestamp`, `timestamptz` and `time` columns were `timestamp(9)`, `timestamp(9) with time zone` and
    `time(9)`, sent with nine fraction digits, which the .NET driver (NH.Trino.Data.ADO) refuses ("Timestamp
    with time zone has more than 7 digits of precision"): the Warehouse boletos API answered 503 on every route
    reading a `timestamptz`. They have the precision PostgreSQL declares, as in the reference (6 without one:
    `timestamp(6) with time zone`; `timestamp(3)`, `timestamp(1)`, `time(4)`…), also as array elements;
    `typeof` names a declared `timestamp(p)` / `time(p)` too (it answered the Arrow unit's precision).
  - **Wrong result**: a `timestamptz` outside 1677–2262 (a `9999-12-31` sentinel) was read as
    `1970-01-01 00:00:00`, and a `timestamp` before 1970 failed the batch; timestamps travel as their
    microseconds since 1970 and keep their whole range.
  - A `numeric` without precision (`decimal(38,20)`) was decoded through a 96-bit decimal
    (`rust_decimal`), which rounds past 28 significant digits, and a value above `decimal(38,20)`'s 18
    integer digits but within 128 bits was kept as an invalid decimal. It travels as its text; a value that does not fit `decimal(38,20)` exactly (more than 18 integer or 20 fraction digits,
    `NaN`, `±Infinity`) fails the query naming the column (`the PostgreSQL value NaN of column
    "vl_recebido" does not fit decimal(38,20)`). The reference leaves such a column out of the relation
    (`decimal-mapping=STRICT`): accepted divergence, ISS-060.
- Timestamps and times never reach a client with more digits than the reference declares (a protocol test
  checks that every value is sent with the digits its column's type declares, for every unit, zone and
  declared precision; `timestamp_precision.toml` compares functions, casts, literals, Hive and PostgreSQL
  columns with Trino 431):
  - **Wrong result**: a `UNION` of timestamps of different precisions took the coarser one and cut the
    fraction (`timestamp(3)` with `timestamp(0)` was `timestamp(0)`: `.123` became `.000`); it takes the
    finest, as the reference (ISS-032 item 8).
  - `current_time` was a `time(9)`; it is the session's wall time at the millisecond, `time(3)` (the
    reference: `time(3) with time zone`; the engine has no time with a zone, ISS-032 item 7).
  - SQL Server `time(p)` columns were `time(9)`; they are `time(p)` (7 without `p`), as the reference.
- PostgreSQL sources get a pushdown policy, as SQL Server has: only what PostgreSQL evaluates exactly like
  the engine is sent to it. Found on a dashboard reading PostgreSQL views (2026-10-01):
  - `count_if`, `arrays_overlap`, array subscripts and `count(*) FILTER (WHERE …)` were sent as written
    and failed ("function count_if(boolean) does not exist"); the engine's functions now run in the engine,
    and the other terms of the same filter are still sent.
  - `SELECT DISTINCT c` was sent as `SELECT t.c FROM (…) AS derived_projection GROUP BY t.c` ("missing
    FROM-clause entry for table").
  - **Wrong result**: `ORDER BY`, `<`/`>`/`BETWEEN` and `min`/`max` on text were sent and followed the
    column's collation (`pt-BR-x-icu`: `_x, a, A, Ábaco, b, B`); they run in the engine, by code point
    (`A, B, _x, a, b, Ábaco`). Text equality, `IN`, `GROUP BY`, `DISTINCT` and join keys are still sent.
  - **Wrong result**: a `TIMESTAMP '…'` literal compared with a `timestamp with time zone` column was sent
    as `CAST('…+00:00' AS TIMESTAMP)`, which drops the offset and reads the text in PostgreSQL's
    `TimeZone` (a database in `America/Sao_Paulo` shifted the window by three hours); it is sent as
    `CAST('…+00' AS TIMESTAMP WITH TIME ZONE)`.
  - Kept local as well: floats in comparisons, grouping and ordering (`NaN`, `-0`), `avg`, `sum` of
    `bigint` or floats, decimal arithmetic, `CASE`, `LIKE` and every other function.
- Hive views another engine stored with a capital letter in a column name (`Descr_obr`; 40 of the 84
  production views) are read: their columns resolve by any spelling, as a table's do (ISS-018). A view over
  such a view (`ob.descr_obr`) failed with `COLUMN_NOT_FOUND`. A view whose columns differ only by case is
  refused with an error naming both.
- `codelake hive migrate-names`: hidden directories inside a schema (a backup copy `.bkp_<date>…` of a
  table) are skipped like hidden schemas; one with an empty metadata file refused the whole plan. Column
  statistics keep every digit (`13591.293610249173` became `…171`: JSON numbers are now parsed exactly,
  which also fixes every other metadata rewrite). `--apply` checks that each directory it changes can be
  written before changing anything, and a write that fails halfway removes what the run wrote. A
  partitioned table refuses the plan with a clear message (its partition metadata is not migrated).
- Found running the Warehouse API's .NET driver (`NH.Trino.Data.ADO` 2025.11.15.6) against the HTTP statement
  protocol (2026-10-01):
  - Every submission is answered `QUEUED` with the first `nextUri`, as the reference engine answers;
    statements answered at once (`USE`, `SET`/`RESET SESSION`, `PREPARE`, `DEALLOCATE`, a kill) and failures
    found before execution (a bad session property, parameters that do not bind, a full queue, a text over
    the limit) come on that page, with their `X-Trino-Set-*`/`Added-Prepare` headers. They came in the
    submission's own answer, without `nextUri`, and the driver failed with `NullReferenceException`.
  - Result columns keep the case they are written with: `SELECT 1 AS FirstDay` and `SELECT X FROM t` name
    them `FirstDay` and `X`, quoted or not (they were `firstday`, `x`); the names a `*` expands to are still
    folded (ISS-018).
  - A page takes the size the client asks for (`targetResultSize`, up to 128 MB; 1 MB by default) and, once
    it has rows, keeps taking the batches that follow closely: execution producing small batches (a cross
    join of `UNNEST`s) sent two batches a page. 300 000 rows over a link of 8 ms: 5 001 requests, 57.5 s →
    4 requests (the driver asks for 5 MB), 0.53 s (reference engine: 0.83 s).
  - A `TIME` literal is `time(p)` for its fraction digits (`TIME '12:34:56.789'` is `time(3)`), as in the
    reference engine; it was `time(9)`, whose nine digits the driver cannot read.
- Found by the shadow run on production routines (2026-10-01):
  - **Wrong result**: a `UNION ALL` (or `UNION`) of three or more inputs mixing a decimal and a `double`
    answered `decimal(35,15)` where the reference answers `double` (the go/no-go report model stored
    `pior_valor`, a `max(bigint / 3600.0)` and two `max(CAST(… AS DOUBLE))`, as `decimal(36,15)`, values
    rounded to 15 digits). Such a union is planned as a union of unions; the outer one kept the decimal the
    SQL planner gave it after the inner one's inputs met in `double`, and every input was cast back to it.
    The outer union now takes the number type its inputs agree on (`real` likewise, where a decimal meets
    a `real`).
  - The elements of an array meet in the same common type: `ARRAY[decimal, double]` is `array(double)`,
    `ARRAY[decimal, real]` is `array(real)` (both were a decimal).
- Found by the shadow run's nightly validation (2026-09-30):
  - Two monitoring models failed with `EXCEEDED_LOCAL_MEMORY_LIMIT` under a 4 GiB pool (`HashJoinInput[0..5]`,
    532 MB each, `can spill: false`) where the reference answers within 8 GB: a SQL Server table of 15
    million rows filtered by `lower(cast(id AS varchar)) IN (SELECT … FROM hive…)` was the build side of its
    hash join and held whole. A federated source reports no statistics, and a hash join whose inputs the
    statistics do not both size built the query's left input. It builds the right one now (the subquery of
    `IN`, the right side of a join), as the reference does without statistics; an input small enough for
    one partition is still collected, inputs both sized are still ordered by size, `NOT IN` keeps its sides
    and `datafusion.optimizer.join_reordering = false` keeps the query's order. Reproduction (15 million
    rows in SQL Server 2022, a replica of 109 401 rows, a `UNION ALL` of two reconciliations, 6 partitions,
    4 GiB): failed at a pool peak of 4094 MiB → 321 MiB, same rows; one reconciliation alone: 2886 MiB →
    166 MiB.
- Found running the daily receipts reports against the reference engine on the same SQL Server catalogs
  (2026-09-29):
  - **Wrong result**: a grouping federated alone (below a join the engine keeps, as the decorrelated
    scalar subquery of an aggregated `SELECT` puts above it) was sent as `SELECT count(1), k … GROUP BY k`,
    aggregates before groups, and read by position: every group came back as `(count, key)`
    (`count(DISTINCT k)` 99 instead of 184). It is now sent with its groups first.
  - An aggregation over a derived table that computes a column (`SELECT count(DISTINCT o.k) FROM (SELECT
    o.k, count(*) c … GROUP BY o.k)`) failed on SQL Server (`The multi-part identifier "o.k" could not be
    bound`): it runs in the engine, the inner grouping still reaches the source.
  - A filter of an aliased table (`FROM t o WHERE o.c = 1`) sent alone or under a grouping named the table
    instead of the alias (`could not be bound`); it names the alias.
  - `UNNEST(...)` joined with a federated table failed (`Assertion failed: expr.is_empty()`).
  - A scalar subquery correlated by an expression (`DATE(r.paid_at) = date(o.day)`), in `SELECT`, `WHERE` or
    `HAVING`, failed (`Physical plan does not support logical expression ScalarSubquery`): the expression is
    computed as a column of the subquery and decorrelated like a plain one.
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
  - A view (Hive, or any state rebuilt from the engine's: distributed tasks) could run a `DataFusion`
    built-in instead of the engine's function of the same name, in about half the processes:
    `date_format(ts, '%Y-%m-%d %H:%i:%s')` failed (`Cast error: Format error`, `to_char`) while the same
    `SELECT` outside the view answered; `length`, `element_at`, `array_join`, `arrays_overlap` were exposed
    the same way. Every name now belongs to one function in every state, whatever order a rebuild registers
    them in. The names only a replaced built-in answered to are gone (`datepart`, `datetrunc`, `today`,
    `instr`, `mean`, `covar`, `array_indexof`, `list_position`, `list_indexof`, `list_distinct`,
    `list_except`, `list_intersect`, `list_max`, `list_remove`, `list_sort`, `list_union`); the reference has
    none of them.
  - `CREATE TABLE AS` of a `UNION ALL` of three or more inputs stored unbounded `varchar` (`string` in the
    metastore) for text built from literals (`'vendas' AS tabela`, a `CASE` of literals) where the reference
    stores `varchar(longest)` (`varchar(14)`, `varchar(7)`: four monitoring models, same values). Such a union
    is planned as a union of unions; one whose inputs agreed kept the type `DataFusion` gave it before the
    literals were typed, and the union above it dropped the type of every input. A column also took a
    bounded `varchar(n)` where an input was unbounded; it is unbounded `varchar`, as in the reference.
    A view of an in-memory catalog lists the `varchar(n)` of its text literals in `information_schema.columns`
    (it listed `varchar`), as the table created from the same query stores it.
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
