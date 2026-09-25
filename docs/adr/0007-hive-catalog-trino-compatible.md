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
  directory with its data (a view's directory holds only metadata); `EXTERNAL_TABLE`
  keeps its data and loses only what the metastore wrote. A `CREATE TABLE AS`
  that fails removes everything it wrote, directory included. A CTAS whose
  target directory already exists fails, as in Trino ("Target directory for
  table … already exists"). `CREATE SCHEMA` creates the schema directory.
- **Views resolve names in their own session.** Trino stores the session
  catalog and schema of `CREATE VIEW` in the view definition and resolves the
  view's unqualified names with them, whatever the reader's session is.
  `CodeLake` plans stored views the same way and records its own session
  catalog/schema when it creates a view. Unknown fields of the view
  definition (`comment`, …) survive a rewrite.
- **Names are lower case.** Trino folds identifiers to lower case, so the Hive
  connector only has lower-case schemas, tables and columns. `CodeLake` folds
  them at every entry of the catalog (DDL, lookup, listing), so a quoted
  `"MixedCase"` is the table `mixedcase` in the directory, the metadata and
  every query, from both engines. Two columns that differ only in case are an
  error.
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
  (ADR 0009). Proven row by row against a Trino 431 whose catalogs set
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
- **`CREATE OR REPLACE TABLE`** is refused as Trino 431's Hive connector
  refuses it (`NOT_SUPPORTED`, "This connector does not support replacing
  tables"), whether the name exists or not (owner decision, 2026-09-24: the
  catalog answers what Trino answers on the same metastore). The existing
  table and the staging area are untouched.
- **Stored views are queries.** The SQL of a view is parsed and must be one
  `SELECT`/`WITH`/`VALUES`, when it is created and every time it is read.
- **Timestamps.** The catalog has Trino's `hive.timestamp-precision`
  (`timestamp_precision`, default `MILLISECONDS`). Values are rounded half up
  to it on write and on read (a file of another writer may hold
  nanoseconds), never cut. `CREATE TABLE` and CTAS refuse a timestamp column
  of another precision with Trino's message ("Incorrect timestamp precision
  for timestamp(6); the configured precision is MILLISECONDS; column name:
  ts"); `ADD COLUMN` accepts any and records `timestamp`, as Trino does.
  `OPTIMIZE` keeps what files hold (microseconds, or nanoseconds for a
  `NANOSECONDS` catalog).
- **`varchar(n)`** is stored as declared; an `INSERT` of a longer value fails
  as in Trino (`INVALID_CAST_ARGUMENT`, only trailing spaces are cut) and reads
  cut values to `n` characters, as Trino's reader. **`char(n)`** is stored as
  declared, written without trailing spaces (as Trino stores it; a longer
  value fails like Trino's `INSERT`) and read padded to `n` (`'ab   '`,
  length 5, as Trino shows it). Trino also compares char values padded
  (`ch = 'ab'` is true); the engine has no char type, so a filter on a char
  column (other than `IS [NOT] NULL`) is refused instead of answered as a
  varchar comparison. Comparisons outside a filter on the table or view (a
  join with a varchar, a `CASE`) still compare the padded text: pending an
  engine char type.
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
- **Views** pad their `char(n)` columns and fold their column names (a quoted `"MixedCol"` is `mixedcol`), and
  `timestamp(p) with time zone` columns keep their instant (read in UTC: Arrow
  has one zone per column, so each value's own zone is not kept).
- **Pending, F5.** Owner-based security (R11): Trino with
  `hive.security=sql-standard` refuses `DROP`/`INSERT` on another user's
  table; `CodeLake` writes `.trinoPermissions` but does not check them yet.

## Acceptance (F2 DoD)

- Round trip with a real Trino 431 container sharing the warehouse volume:
  tables created by Trino are read by CodeLake and vice versa, rows equal.
- Old files keep their checksum after later inserts; the file list only grows.
- SQL Server 2022 loads CodeLake-written Parquet with `OPENROWSET`.
