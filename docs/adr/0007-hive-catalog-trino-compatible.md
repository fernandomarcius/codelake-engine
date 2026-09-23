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

## Acceptance (F2 DoD)

- Round trip with a real Trino 431 container sharing the warehouse volume:
  tables created by Trino are read by CodeLake and vice versa, rows equal.
- Old files keep their checksum after later inserts; the file list only grows.
- SQL Server 2022 loads CodeLake-written Parquet with `OPENROWSET`.
