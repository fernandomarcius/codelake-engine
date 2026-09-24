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
  (Julian day + nanoseconds of day) holding the wall-clock value, with
  `writer.time.zone = Etc/UTC` in the footer, exactly as Trino 431 writes it.
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
- **`INT96` time zone: UTC, as the official Trino image.** Trino 431 converts
  `INT96` values with `hive.parquet.time-zone` (default: the JVM zone, UTC in
  the `trinodb/trino` image) on write and on read, and ignores the
  `writer.time.zone` footer key it writes itself (checked: a Trino 431 with
  `hive.parquet.time-zone=America/Sao_Paulo` shows its own fixture file and a
  `CodeLake` file both 3 h earlier). `CodeLake` stores and reads the wall
  clock as UTC, so it agrees with a Trino whose Parquet time zone is UTC. A
  Trino configured with another zone needs the same setting in `CodeLake`
  (not implemented): check the production Trino before the M2 canary.

## Acceptance (F2 DoD)

- Round trip with a real Trino 431 container sharing the warehouse volume:
  tables created by Trino are read by CodeLake and vice versa, rows equal.
- Old files keep their checksum after later inserts; the file list only grows.
- SQL Server 2022 loads CodeLake-written Parquet with `OPENROWSET`.
