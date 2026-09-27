# ADR 0007 — Hive catalog compatible with Trino's file metastore

- Status: Proposed (F2)
- Date: 2026-09-23

## Context

The reference pipelines keep their warehouse in Trino's Hive connector with
`hive.metastore=file`: one directory per schema and per table under
`/data/DATAWAREHOUSE`, table metadata in `.trinoSchema` (JSON) with Hadoop
`..trinoSchema.crc` checksums, data in Parquet. A SQL Server 2022 sink lists the
directory (`sys.dm_os_enumerate_filesystem`), skips dotfiles, `__dbt_tmp`,
`trinoSchema` and files younger than 45 s, and loads each Parquet file with
`OPENROWSET(BULK …, FORMAT='PARQUET')`, recording its path.

## Decision

- CodeLake's `hive` catalog reads and writes Trino's file-metastore layout and
  JSON, so it can take over the existing warehouse without migration and Trino
  can read what CodeLake writes (rollback path for the M2 canary).
- `INSERT` appends exactly one new Parquet file per statement per writer and
  never rewrites existing files. Files are written under a dot-prefixed temporary
  name and renamed when complete, so the sink never sees a partial file.
- Views are stored as Trino views (`VIRTUAL_VIEW` with the original SQL) and
  survive restarts.
- Storage: local filesystem first (production today), S3/MinIO through
  `object_store` with the same layout.

## Format decisions (F2 corrections)

Each one was found by the independent acceptance suite
(`tests/integration/hive_acceptance.rs`) against a real Trino 431 and SQL
Server 2022, and each is the behavior of Trino 431 itself.

- **`writerVersion` is `431`.** Trino's file metastore refuses any metadata
  file whose `writerVersion` differs from its own version. New metadata
  (schemas, tables, views) is written as `431`, the Trino this warehouse is
  shared with; a rewrite keeps the version already in the file. Moving to
  another Trino version means changing `TRINO_VERSION`.
- **Complete table metadata, lossless rewrites.** Trino 431 requires
  `serdeParameters` and `columnStatistics` (non-null maps) in every table and
  view file. Both are always written; views also carry
  `originalStorageFormat: {}` and `presto_view`, `presto_version` parameters.
  Fields `CodeLake` does not model are kept unchanged on every rewrite, at
  table and column level.
- **Statistics after `INSERT` become unknown, not absent.** `CodeLake` does not
  compute statistics. After appending a file it records them as Trino records
  unknown statistics when it merges an insert without them: `numFiles`,
  `numRows`, `rawDataSize`, `totalSize` removed, `columnStatistics` emptied
  (the field stays). Every other parameter is kept. Stale counts would mislead
  Trino's optimizer; an absent field makes Trino fail.
- **Parquet laid out like Trino's.** Hive `timestamp` is written as `INT96`
  (Julian day + nanoseconds of day) holding the instant of the wall-clock
  value in the catalog's Parquet time zone, with that zone under
  `writer.time.zone` in the footer, exactly as Trino 431 writes it.
  SQL Server 2022 `OPENROWSET` maps `INT96` to `datetime2`; an `INT64`
  timestamp without the legacy `TIMESTAMP_MILLIS` annotation reaches it as
  `bigint`. Decimals are `FIXED_LEN_BYTE_ARRAY` of the smallest width for the
  precision (Trino's layout); the root group is `trino_schema`. arrow-rs cannot
  write `INT96`, so the writer encodes those columns itself and leaves them
  without min/max statistics (undefined sort order must never prune).
- **Managed tables own their directory.** `DROP TABLE` removes the table
  directory with its data; `EXTERNAL_TABLE` keeps its data and loses only
  what the metastore wrote. **A view owns nothing but its metadata**
  (amended 2026-09-26, ISS-023, deliberate divergence): `DROP VIEW` removes
  `.trinoSchema`, its checksum and `.trinoPermissions`, and the directory
  only when nothing else is left in it; `ALTER VIEW … RENAME` moves only
  those files (new name written whole, `.trinoSchema` last, then the old
  one removed). Trino 431 lets `CREATE VIEW` take a directory that already
  holds another writer's files and deletes them all on `DROP VIEW`;
  `CodeLake` keeps them, byte for byte (`hive::limits_tests::drop_view_*`,
  `view_rename_and_replace_never_move_foreign_files`). The view's rows are
  the same in both engines, and after `DROP VIEW` neither finds it
  (`hive_acceptance::drop_view_keeps_foreign_files_and_trino_reads_the_view`,
  Trino 431). Any table type other than `MANAGED_TABLE`
  loses only its metadata on `DROP TABLE`. A `CREATE TABLE AS`
  that fails removes everything it wrote, directory included. A CTAS whose
  target directory already exists fails, as in Trino ("Target directory for
  table … already exists"). `CREATE SCHEMA` creates the schema directory.
- **Views resolve names in their own session.** Trino stores the session
  catalog and schema of `CREATE VIEW` in the view definition and resolves the
  view's unqualified names with them, whatever the reader's session is.
  `CodeLake` plans stored views the same way and records its own session
  catalog/schema when it creates a view. Unknown fields of the view
  definition (`comment`, …) survive a rewrite.
- **Names resolve folded; the case written is kept** (amended 2026-09-26,
  ISS-018, removed limitation, no compatibility mode). Trino folds
  identifiers to lower case, so its Hive connector only has lower-case
  schemas, tables and columns, and loses the case a client wrote.
  `CodeLake` now keeps it where that loses nothing:
  - **Files:** every Parquet file `CodeLake` writes (`CREATE TABLE AS`,
    `INSERT`, `OPTIMIZE`) names each column as it was written: a quoted
    `"CustomerCode"` is the Parquet column `CustomerCode` (an unquoted
    name is folded, as SQL reads it). Readers of the files see it: SQL
    Server 2022 `OPENROWSET` (case-insensitive collation) reads the right
    columns under either spelling, and Trino 431 reads them (its Parquet
    reader matches names ignoring case).
  - **Metastore:** names stay folded (`dataColumns[].name`, the directories
    of schemas and tables); the name as written is recorded in the
    `codelake.name` property of the column (`properties`) and in the
    `codelake.name` parameter of the table, view and schema (only when it
    differs from the folded name; updated by a rename). Checked on Trino
    431: with a mixed-case name in `dataColumns` it reads the table but its
    `INSERT` fails (`NullPointerException: "type" is null`), and it finds a
    table only in its lower-case directory; it keeps the column properties
    through its own `INSERT`. A `codelake.name` that no longer folds to the
    column's name (another engine renamed it) is ignored.
  - **Resolution:** the engine sees every schema, table and column by its
    folded name, so any spelling resolves (`customercode`, `CUSTOMERCODE`,
    `"CustomerCode"`), on both client protocols (HTTP and the PostgreSQL
    wire, simple and extended), wherever a statement names a column: in
    expressions, `INSERT` column lists and `JOIN … USING` lists
    (`codelake_core::identifier_case`; a name the statement itself defines,
    an alias, stays as written). An unquoted name with a non-ASCII capital
    folds as `A`–`Z` do (`DESCRIÇÃO` is `descrição`; the planner alone
    folds ASCII only, which recorded `descriÇÃo`, a spelling nobody
    wrote). Result headers, `information_schema` and `DESCRIBE`
    show the folded name, as Trino does. Showing the written case there
    needs case-insensitive column resolution in the planner, which
    `DataFusion` 54 does not have (ISS-018, awaiting confirmation).
  - **Ambiguity is refused:** two names of one level that differ only by
    case (columns of a table, a column added next to another, tables,
    views, schemas) fail with an error that names both ("differ only by
    case"). A table whose metadata (another writer's) holds such columns
    fails when read; a data file with several spellings of one column is
    read by the exact written spelling, then the folded one, and otherwise
    fails when the column is read, never NULL nor a guess. Footer
    statistics match file columns the same way (a column spelled otherwise
    in the file is no longer taken for absent, "all NULL").
  - Tables written before (lower-case files and metadata) read and write
    as before; files of other writers (`ID` for `id`) too.
  - Evidence: `hive::limits_tests` (`column_names_keep_their_case_…`,
    `names_that_differ_only_by_case_are_refused`, `schema_and_table_names_…`,
    stale and ambiguous names), `hive::columns` and `file_statistics` unit
    tests, `hive_acceptance::case_preserved_columns_are_read_by_trino_and_openrowset`
    (Trino 431 reads the rows `CodeLake` reads and appends rows `CodeLake`
    then reads; SQL Server 2022 reads each file's columns by both spellings
    and reports the written names).
- **`INT96` time zone per catalog: `parquet_time_zone`** (amended
  2026-09-25). Trino 431 converts `INT96` values with
  `hive.parquet.time-zone` (default: the JVM zone, UTC in the
  `trinodb/trino` image) on write and on read, and ignores the
  `writer.time.zone` footer key it writes itself (checked: a Trino 431 with
  `hive.parquet.time-zone=America/Sao_Paulo` shows its own fixture file and a
  `CodeLake` file both 3 h earlier). Each hive catalog has the same setting,
  `parquet_time_zone`, an IANA zone name validated when the configuration
  loads (default `America/Sao_Paulo`, owner decision): writes convert the
  wall-clock time from it to UTC, reads convert back, with Joda-Time's rules
  as Trino (a time the clocks skip moves forward; a repeated time is the
  earlier instant west of Greenwich), over the IANA database
  (`codelake_core::parquet_time_zone`). Only `INT96` columns convert: the
  Parquet reader marks them (`coerce_int96_tz`) so `INT64` timestamps keep
  their values, as in Trino. `OPTIMIZE` rewrites stored instants unconverted
  (ADR 0009) and writes every timestamp as `INT96`, so it reads an `INT64`
  timestamp (another writer's wall-clock time) as the instant of the zone,
  which reads back unchanged; a wall-clock time the zone skips has no such
  instant, and `OPTIMIZE` refuses it instead of changing it (DEFECT-W1T-1,
  `w1t_optimize_keeps_int64_timestamps_of_other_writers`, `…_like_trino_431`). Proven row by row against a Trino 431 whose catalogs set
  `hive.parquet.time-zone=America/Sao_Paulo`
  (`hive_redteam_w1::w1_parquet_time_zone_reads_and_writes_like_the_reference_engine`:
  each engine reads what the other wrote, the instants stored agree through
  UTC catalogs, including a repeated local time, the local mean time before
  1914, nanoseconds through NANOSECONDS catalogs and `OPTIMIZE`; a skipped
  local time written by `CodeLake` reads moved forward in both engines. Trino
  itself fails to write a skipped time now and then: its Parquet writer
  re-reads a sample of its files and the value comes back moved).
  **Risk:** the zone must be the one every engine sharing the warehouse
  uses. A file written in UTC (Trino with its default image zone, Spark,
  pandas writing `INT96`) reads 3 h earlier in `America/Sao_Paulo`, silently,
  as it does in a Trino set to that zone; SQL Server `OPENROWSET` shows the
  stored instant (UTC), not the wall-clock time. Check the production
  Trino's `hive.parquet.time-zone` (or its JVM zone) before the M2 canary
  and set the catalog to it.

## Red-team corrections

An independent red-team (probes in `tests/integration/hive_redteam.rs`) found
data loss, concurrency, type and security defects. Each rule below is either
Trino 431's behavior (checked on `trinodb/trino:431`) or what data safety
requires where Trino has no equivalent.

- **Client statements reach files only through catalogs.** `COPY … TO`,
  `CREATE EXTERNAL TABLE` and `SET/RESET datafusion.runtime.*` are refused with
  `NOT_SUPPORTED` in the core (`statement_policy`), on the statement and again
  on the plan, for every protocol (Trino, and the PostgreSQL wire through a
  hook that runs before any other). Trino has none of them.
- **Names.** `""` is a syntax error anywhere, as Trino's lexer ("Zero-length
  delimited identifier not allowed"). Schema, table and view names that would
  not be a directory of their own (`.`, `..`, `/`, `\`, control characters)
  are refused before any writable catalog turns them into a path.
- **Writers are serialized per table.** Each change of a table's metadata and
  each commit of new files holds the table's metadata lock (in-process mutex
  plus `flock` across processes on a local warehouse). New tables and `INSERT`
  files are written under `.codelake/staging/<id>/` and published by one
  rename under that lock after re-checking the target: two CTAS of one name
  never mix rows (the loser fails), an `ADD COLUMN` is never lost to a
  concurrent `INSERT` or `ADD COLUMN`, an `INSERT` whose metadata update fails
  leaves no file visible, and a failed CTAS removes only its own staging.
- **`CREATE OR REPLACE TABLE`** (and `… AS`) replaces the table atomically
  (amended 2026-09-26, ISS-017, deliberate divergence, no compatibility
  mode; it was refused as Trino 431's Hive connector refuses it, "This
  connector does not support replacing tables"):
  - the new table (data files, `.trinoSchema`, permissions) is written in
    the statement's staging directory, like a CTAS; a query that fails, or
    a process that dies, leaves the old table untouched (the orphan staging
    is hidden, disk only);
  - under the table's commit lock, cross-process file lock and metadata
    lock (those `DROP TABLE` takes), the staging directory and the table
    directory are **exchanged in one step** (`renameat2(RENAME_EXCHANGE)`,
    `renamex_np(RENAME_SWAP)` on macOS): every reader that takes those
    locks (this process, another `CodeLake` node on the warehouse, the
    sink) finds the whole old table or the whole new one under the name,
    never a mix and never nothing. Trino reads the table's `.trinoSchema`
    and lists its directory in separate steps, without those locks: the
    exchange guarantees it never finds the table missing, but a Trino query
    that overlaps the swap may read the new table's files with the old
    table's metadata (not reproduced; do not replace a table Trino is
    reading). The old table is then in the staging directory and is removed
    last; a crash before that leaves the new table and a hidden orphan in
    `.codelake/staging`, which nothing removes yet (disk only);
  - a free name is a CTAS (a directory without a table is refused, never
    deleted: W1-1); a view, or a table of another type than
    `MANAGED_TABLE` (an `EXTERNAL_TABLE` does not own its data), is
    refused with nothing changed; a file system or platform that cannot
    exchange two directories, and object-store warehouses, refuse the
    statement with the table unchanged;
  - the new table's metadata carries a `codelake.table_id` parameter. A
    scan lists files only after checking, under the locks the swap takes,
    that the table still has the identity it was planned with; an `INSERT`
    commits its file under the same check; `OPTIMIZE` checks it before its
    swap. A plan made against the replaced table fails ("was replaced
    (CREATE OR REPLACE) after this statement was planned; plan the statement
    again"): it never reads the new table's files with the old table's
    columns, nor puts rows planned for one into the other. A query that
    listed the old files just before the swap fails reading them (file not
    found: loud, never partial), as a query planned before an `OPTIMIZE`
    swap does (ADR 0009);
  - a client protocol that keeps plans plans the statement again before
    running it. The PostgreSQL wire keeps the plan of a prepared statement
    from `Parse` (JDBC after five runs, psycopg3, Npgsql): each `Execute`
    plans the statement again against the catalogs as they are, as
    PostgreSQL plans a prepared statement again when a table it reads
    changed, so a pool keeps working across a dbt run that replaces its
    tables. When the new plan would answer other columns (names or types)
    than `Describe` announced, or take other parameter types, the statement
    is refused as PostgreSQL refuses it (SQLSTATE `0A000`, "cached plan must
    not change result type") until it is prepared again. The HTTP protocol
    plans every statement it runs;
  - evidence: `hive::limits_tests` (replacement and self-reading
    replacement, free name, view, foreign directory and external table
    refused, failed query, a crash while writing, a crash before and after
    the swap with a reopened catalog, statements planned before the swap,
    readers of two processes racing twelve replacements, two processes
    replacing at once), `hive::limits_redteam_tests` (a lock-free reader
    always finds the table, a kept plan), e2e `limits_redteam` (`SIGKILL` of
    the binary at 14 points of a replacement; prepared statements across a
    replacement with the same and with other columns), `hive_redteam_w1` (`OR REPLACE` racing `OPTIMIZE`
    on another node; Trino 431 reads the replaced table with the same rows
    as `CodeLake`).
- **Stored views are queries.** The SQL of a view is parsed and must be one
  `SELECT`/`WITH`/`VALUES`, when it is created and every time it is read.
- **Timestamps.** The catalog has Trino's `hive.timestamp-precision`
  (`timestamp_precision`, default `MILLISECONDS`). Values are rounded half up
  to it on write and on read (a file of another writer may hold
  nanoseconds), never cut. `CREATE TABLE` and CTAS refuse a timestamp column
  of another precision with Trino's message ("Incorrect timestamp precision
  for timestamp(6); the configured precision is MILLISECONDS; column name:
  ts"); `ADD COLUMN` accepts any and records `timestamp`, as Trino does.
  `OPTIMIZE` keeps what files hold, to the nanosecond (ADR 0009).
- **`varchar(n)`** is stored as declared; an `INSERT` of a longer value fails
  as in Trino (`INVALID_CAST_ARGUMENT`, only trailing spaces are cut) and reads
  cut values to `n` characters, as Trino's reader. **`char(n)`** is stored as
  declared, written without trailing spaces (as Trino stores it; a longer
  value fails like Trino's `INSERT`) and read padded to `n` (`'ab   '`,
  length 5, as Trino shows it). Trino also compares char values padded
  (`ch = 'ab'` is true); the engine has no char type, so a comparison of a
  char column is refused instead of answered as a varchar comparison,
  wherever the plan holds it (see "Red-team W1 corrections").
- **Checksums are verified.** A metadata file whose `.crc` does not match is
  an error ("Checksum error"), as in Trino, never a table read with other
  columns. A mismatch is re-read briefly first: writers replace the checksum
  just before the file.
- **Listing never goes stale nor breaks.** On a local warehouse schema and
  table names are read from the directories at every listing, so tables of
  another writer appear at once. A table the catalog cannot read (another
  format, partitions, unreadable metadata) is listed and fails only when read;
  a column of a type the engine cannot read (`array`, `map`, `struct`) fails
  only when a query reads it.
- **Column options** the catalog cannot keep (`NOT NULL`, `DEFAULT`, …) are
  refused with Trino's messages; `COMMENT` is stored.
- **Views** pad their `char(n)` columns and fold their column names (a quoted `"MixedCol"` is `mixedcol`; a view writes no file, so ISS-018 keeps nothing else), and
  `timestamp(p) with time zone` columns keep their instant (read in UTC: Arrow
  has one zone per column, so each value's own zone is not kept).
- **Pending, F5.** Owner-based security (R11): Trino with
  `hive.security=sql-standard` refuses `DROP`/`INSERT` on another user's
  table; `CodeLake` writes `.trinoPermissions` but does not check them yet.

## Red-team W1 corrections

A second red-team (`tests/integration/hive_redteam_w1.rs`, after the F9
`OPTIMIZE` integration) found seven defects; each probe below failed before
its correction and passes after it (2026-09-25).

- **W1-1 `CREATE OR REPLACE` over a foreign directory** deleted it. The
  statement was then refused as Trino 431 refuses it; since ISS-017 it
  replaces tables, and a name without a table whose directory exists is
  still refused, nothing deleted (see above;
  `w1_or_replace_never_deletes_a_foreign_directory`).
- **W1-2 NANOSECONDS scans of `INT96` outside 1677–2262** answered another
  instant (the reader's nanoseconds wrap: 1500-06-01 read as 2084-12-18).
  A NANOSECONDS catalog now reads its timestamps exactly
  (`hive/exact_timestamps.rs`: microseconds and nanoseconds read over the
  same files, paired row by row); an instant Arrow nanoseconds cannot hold
  is an error naming the value and column, never another instant. Writing
  one fails the statement instead of storing NULL
  (`w1_nanosecond_catalog_never_reads_a_wrong_instant`). Each of the two
  reads keeps every file in its partition and in order: `DataFusion` 54 lets
  an idle partition take files queued for another, which paired rows of
  different files (found by the time zone test, 2026-09-25;
  `w1_nanosecond_catalog_pairs_rows_of_many_files`).
- **W1-3 (critical) `OPTIMIZE` in a NANOSECONDS catalog** rewrote those
  values as other instants, and **W1-7** `OPTIMIZE` in another catalog
  rounded nanoseconds to microseconds: compaction rewrites the exact stored
  instants, whatever the catalog's precision (ADR 0009;
  `w1_nanosecond_optimize_never_rewrites_other_instants`,
  `w1_optimize_keeps_the_nanoseconds_trino_wrote`).
- **W1-4 `char(n)` comparisons above the scan** (under `LIMIT`, a window, a
  volatile predicate) compared unpadded text and dropped rows. An analyzer
  rule (`codelake_core::char_comparisons`) refuses every comparison of a
  char value, wherever it is in the plan (filters, joins, `CASE`,
  projections, subqueries), until the engine has a char type; an explicit
  `CAST(ch AS varchar)` compares text (owner decision: char(n) as
  implemented) (`w1_char_comparisons_above_the_scan_never_drop_rows`).
  The comparisons a plan makes without a comparison expression are refused
  too (DEFECT-W1T-2, which answered other counts than Trino 431): join keys
  (`USING`, `NATURAL`, `INTERSECT`, `EXCEPT`), a `UNION` holding a char
  column (it mixes padded values with unpadded ones, a varchar or a
  `CAST … AS char(n)`, which `DISTINCT` and `GROUP BY` then compare), and
  `nullif`, `greatest` and `least`
  (`w1t_char_values_compared_outside_predicates_like_trino_431`).
- **W1-5 tables of the in-memory catalog** (`CREATE TABLE datafusion.…`)
  held their rows outside the memory pool. Their rows are reserved in the
  pool for as long as the table exists (`codelake_core::memory_tables`):
  `CREATE TABLE … AS` and `INSERT` fail with nothing kept once the pool is
  full; `DELETE`, `UPDATE` and `DROP` give memory back
  (`w1_in_memory_tables_never_escape_the_pool`). `DELETE` and `UPDATE` run
  when the plan executes, not while it is planned (`DataFusion`'s own
  tables change their rows while planning, `EXPLAIN` included), and
  `UPDATE` reserves the values it computes before the table changes: one
  the pool cannot hold fails with the rows as they were, instead of
  growing the table past the ceiling (DEFECT-W1T-3,
  `w1t_in_memory_update_never_escapes_the_pool`). Pending, F5: those tables
  are visible to every user, like the rest of the catalogs until ownership
  (R11) is checked.
- **W1-6** owner checks (R11): pending F5, the probe stays ignored.

## Acceptance (F2 DoD)

- Round trip with a real Trino 431 container sharing the warehouse volume:
  tables created by Trino are read by CodeLake and vice versa, rows equal.
- Old files keep their checksum after later inserts; the file list only grows.
- SQL Server 2022 loads CodeLake-written Parquet with `OPENROWSET`.
