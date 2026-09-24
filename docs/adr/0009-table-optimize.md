# ADR 0009 — Table compaction with Trino's `ALTER TABLE … EXECUTE optimize`

- Status: Proposed (F9)
- Date: 2026-09-23

## Context

Tables that receive one file per `INSERT` (ADR 0007) accumulate thousands of
small files; reads slow down with the file count. Trino's Hive connector
compacts them with a table procedure:

```sql
SET SESSION hive.non_transactional_optimize_enabled = true;
ALTER TABLE hive.s.t EXECUTE optimize(file_size_threshold => '100MB');
```

Clients written for Trino must keep working unchanged, so the syntax, the
session property, the answer and the error messages are Trino's. Behaviour
observed on a real `trinodb/trino:431` with a file metastore (2026-09-23):

| Case | Trino 431 |
|---|---|
| success | `updateType` `ALTER TABLE EXECUTE`; one column `rows` (`bigint`), **no rows**, no `updateCount` |
| property off (default) | `OPTIMIZE procedure must be explicitly enabled via non_transactional_optimize_enabled session property` |
| unknown procedure | `Table procedure not registered: FOO` (name upper-cased; lookup case-insensitive, quoted or not) |
| unknown argument | `Catalog 'hive' table procedure 'OPTIMIZE' property 'foo' does not exist` |
| bad size | `Unable to set catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['abc']: size is not a valid data size string: abc` (`'10mb'` → `Unknown unit: mb`) |
| non-string value | `Invalid value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold': Cannot convert [10] to varchar` |
| `NULL` | `Invalid null value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' from [null]` |
| arguments | named or positional, not mixed; constant expressions (`'1' \|\| 'MB'`); a subquery → `Constant expression cannot contain a subquery` |
| `WHERE` on a non-partitioned table | `Unexpected FilterNode found in plan; probably connector was not able to handle provided WHERE expression` |
| view / missing table | `ALTER TABLE EXECUTE is not supported for views` / `Table 'hive.s.nope' does not exist` |
| sizes | airlift `DataSize`: units `B kB MB GB TB PB`, case-sensitive, powers of 1024 |
| threshold | files with size **≤** threshold are rewritten (a file exactly at the threshold is) |
| checks order | table, procedure, arguments, then the session property |
| old files | deleted together with their Hadoop `.crc` side files |

## Decision

### Generic in the core

- `codelake_core::table_procedure`: a catalog declares `TableProcedure`s with
  typed `ProcedureArgument`s (the same `PropertyType`s as session properties).
  `bind` applies defaults, names and positions, types and parsing, with the
  messages above. Nothing in the core knows `OPTIMIZE`.
- `WritableCatalog::table_procedures` and
  `WritableCatalog::execute_table_procedure(schema, table, call, session, context)`.
  `session` is a `CatalogSession`: that catalog's session properties without
  the prefix, defaults applied (`PropertyRegistry::catalog_session`).
- `sqlparser` 0.61 cannot parse `ALTER TABLE … EXECUTE`. `ddl::parse_table_execute`
  recognizes it with `sqlparser`'s own tokenizer and `parse_object_name` /
  `parse_expr`, so quoting, comments, case, 1–3 part names and whitespace
  follow the dialect. Any other statement is left to the regular parser.
- `Engine` evaluates each argument as `SELECT <expr>` (after refusing
  subqueries), checks table, view and procedure, binds, rejects `WHERE`, then
  calls the catalog, and answers with an empty result with one `rows` column.
- `parse_data_size` implements airlift `DataSize`; session properties of type
  `DataSize` now validate with it too (Trino rejects `512MiB`, so do we).

### Hive implementation (`hive/optimize.rs`)

- Session properties declared by `HiveConnector::session_properties`:
  `non_transactional_optimize_enabled` (boolean, `false`) and
  `target_max_file_size` (`1GB`; honored by `OPTIMIZE` only — `INSERT` keeps
  writing exactly one file per statement, ADR 0007).
- Snapshot: the visible data files listed when the procedure starts, filtered
  by `size ≤ file_size_threshold`. Files written later (concurrent `INSERT`)
  are never read nor deleted.
- Rows are read with the table's schema, so files written before an
  `ADD COLUMN` produce `NULL` for it and the new file carries the column;
  `INT96` timestamps are read as milliseconds, as for scans.
- Streaming: one Parquet scan over the snapshot (one partition, files in
  listing order) feeding writers that roll over at `target_max_file_size`.
  The open row group is reserved in the memory pool
  (`HiveOptimize[<table>]`); a short pool fails the procedure with
  `Resources exhausted` and leaves the table untouched.
- Correctness check before any visible change: the rows written must equal
  the row counts in the snapshot files' Parquet footers, and each closed file's
  footer must match the rows written into it.

### Crash safety

1. Write new files as `.<name>.inprogress` (hidden; readers, Trino and the
   SQL Server sink skip dot files).
2. Under the table's in-process commit lock, verify the table still exists and
   every snapshot file is still there with the same size.
3. Publish the intent: create `.codelake-optimize.json` with *create if
   absent*. It lists the files to add (hidden and final names, rows) and to
   remove (name, size). If it already exists, another swap is in progress:
   discard and fail.
4. Rename each new file to its final name; delete the old files and their
   `.crc`; drop the table statistics (like `INSERT`); delete the marker.

Once the marker exists, the swap only moves forward. Every step is
idempotent, so `recover` can finish it at any point. `recover` runs before
each `OPTIMIZE` of the table and for every table when the catalog is opened
(`Connector::catalog`), before any query:

- every new file present (hidden or renamed) → finish the swap;
- a new file gone while all old files remain (removed by someone else) → undo;
- anything else → stop with an error naming the marker; never guess.

A crash before step 3 leaves only hidden `.inprogress` files: no reader sees
them, no row is lost or duplicated.

Tests cover a crash after the marker, in the middle of the renames, in the
middle of the deletes, a lost new file, an inconsistent marker, an unreadable
marker, a concurrent `INSERT`, a snapshot changed by another compaction, a
second marker, and concurrent `OPTIMIZE` + reads in one process
(`hive/optimize_tests.rs`).

### Deliberate differences from Trino

- Fewer than two files under the threshold: nothing is rewritten. Trino
  rewrites a single small file into a new name; that gains nothing and makes
  downstream consumers that track file names (the reference sink) load the same rows
  again. The rows returned by queries are the same.
- Trino has no recovery: a failure while deleting leaves duplicates for a
  human to clean up. CodeLake finishes the swap from the marker.

## Residual windows (documented, not fixed)

- **Readers during the swap (step 4, milliseconds).** A CodeLake scan in the
  same process never lists during a swap (it takes the commit lock shared
  while listing), but a query planned before the swap can fail with a missing
  file (loud, not wrong). Trino, the sink or another process listing the
  directory during step 4 can see old and new files together (duplicates) or
  miss some; same as Trino's own `OPTIMIZE`. Schedule compaction outside the
  sink's window.
- **Several processes compacting the same table at once** (two CodeLake nodes,
  or CodeLake and Trino) is not supported, as in Trino. The marker refuses a
  second swap while one is in progress and the snapshot check refuses a swap
  whose files are gone, but a swap whose check ran just before another
  process's swap deleted the same files can still duplicate rows. One node
  compacts; in-process runs are serialized per table.
- **Orphans of a crash before step 3**: hidden `.inprogress` files (and, on a
  local disk, `object_store` staging files) stay until removed by hand. They
  cost disk only.
- **Power loss**: renames and deletes are not `fsync`ed; durability across a
  power failure is the file system's (process crashes are covered above).
- `DROP TABLE` in the same process waits for a swap; from another process it
  can race with one (the snapshot check catches a dropped table before the
  swap, not during it).

## Interoperability findings (hive catalog, ADR 0007)

Proving that Trino reads compacted tables exposed two gaps in the metadata
CodeLake writes, found with a real Trino 431:

- Trino 431 refuses `.trinoSchema` without the keys `serdeParameters` and
  `columnStatistics` ("Could not read table schema"), even when empty.
  CodeLake dropped `columnStatistics` when invalidating statistics (every
  `INSERT` into a table with statistics, and every `OPTIMIZE`) and never wrote
  either key for tables it creates. **Fixed here**: both are modelled and
  always written; invalidation empties the statistics instead of removing
  the key.
- Trino's file metastore refuses metadata whose `writerVersion` differs from
  its own version (`hive.metastore.version-compatibility=NOT_SUPPORTED`, the
  default). Tables and schemas *created* by CodeLake carry
  `writerVersion: codelake`, so Trino reads them only with
  `UNSAFE_ASSUME_COMPATIBILITY`. **Not fixed here** (it is a decision for the
  hive catalog: which version to claim, and whether to make it configurable);
  metadata of tables created by Trino keeps its version when CodeLake rewrites
  it, so compacting a Trino table keeps it readable by a strict Trino.

## Not done

- `WHERE` (partition filters): partitioned hive tables are not supported yet.
- Re-seeding the reference sink's processed-file checkpoint after a compaction
  (ROADMAP M2, R9) belongs to the pipeline, not to the engine.
- Other procedures (`drop_extended_stats`, Iceberg `expire_snapshots`, …): the
  registry supports them; none is implemented.

## Evidence

- `codelake-core`: `table_procedure` (binding, messages),
  `ddl::parse_table_execute` (quoting, case, comments, 1–3 parts, malformed
  statements), `engine` (routing, Trino's checks, argument expressions,
  session defaults), `session_properties::parse_data_size`.
- `codelake-connectors`: `hive::optimize_tests` (rows, `ADD COLUMN`, threshold,
  roll-over, Trino-written files with `.crc`, crash points, concurrency,
  memory).
- Integration (`tests/integration/hive_optimize.rs`, real `trinodb/trino:431`):
  a table written by Trino in four statements with an `ADD COLUMN` is
  compacted by CodeLake and a strict Trino returns exactly its original rows
  from a single file; a table written and compacted by CodeLake is read by
  Trino (lenient catalog, see above) with the same rows as its uncompacted
  copy.
