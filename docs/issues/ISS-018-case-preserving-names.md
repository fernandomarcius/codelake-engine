# ISS-018 — Case-preserving names, case-insensitive resolution

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: awaiting-confirmation (implemented 2026-09-26; choices below applied by recommendation)
- Context: Hive names are folded to lower case because the reference engine does.
- Accepted: Preserve the case a name was written with, resolve names without regard to case, and refuse two names that differ only by case; no compatibility mode.
- Risk: Readers with case-insensitive collations (SQL Server OPENROWSET) would otherwise read the wrong column.
- Revisit when: Done when implemented with tests over hive and OPENROWSET readers. Implemented 2026-09-26 (ADR 0007): Parquet files carry each column as written; the metastore keeps folded names and records the written one as `codelake.name` (column property; table, view and schema parameter); every spelling resolves; names that differ only by case are refused with an error naming both; old tables unchanged. Tested with Trino 431 (reads, and appends rows `CodeLake` reads) and SQL Server 2022 `OPENROWSET` (right columns under both spellings).
- Choices applied by recommendation (confirm or overturn):
  1. Metastore names stay folded, the written name goes to `codelake.name`: Trino 431 fails its `INSERT` into a table whose `dataColumns` hold a mixed-case name (checked: `NullPointerException: "type" is null`), which would break the reference engine writing shared tables (canary, rollback).
  2. Schema and table directories stay folded: Trino looks a table up only in its lower-case directory, and a case-sensitive file system would otherwise hold two directories for one name.
  3. The SQL surface (result headers, `information_schema`, `DESCRIBE`) shows folded names: showing the written case needs case-insensitive column resolution in the planner (DataFusion 54 matches names exactly); open as follow-up.
  4. View column names stay folded (a view writes no file).
- References: ADR 0007.
