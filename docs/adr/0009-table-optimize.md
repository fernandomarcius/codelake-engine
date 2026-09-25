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
| unknown procedure | `Table procedure not registered: FOO`: an unquoted name is upper-cased, a quoted one is kept, and the lookup is exact (`EXECUTE "optimize"` → `… not registered: optimize`; `"OPTIMIZE"` runs) |
| unknown argument | `Catalog 'hive' table procedure 'OPTIMIZE' property 'foo' does not exist` |
| bad size | `Unable to set catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' to ['abc']: size is not a valid data size string: abc` (`'10mb'` → `Unknown unit: mb`) |
| non-string value | `Invalid value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold': Cannot convert [10] to varchar`; the value prints as a SQL literal of its type (`[DECIMAL '1.5']`, `[DATE '2020-01-01']`) |
| `NULL` | `Invalid null value for catalog 'hive' table procedure 'OPTIMIZE' property 'file_size_threshold' from [null]` |
| arguments | named or positional, not mixed; constant expressions (`'1' \|\| 'MB'`); a subquery → `Constant expression cannot contain a subquery` |
| `WHERE` on a non-partitioned table | `Unexpected FilterNode found in plan; probably connector was not able to handle provided WHERE expression` (after the session property check) |
| quoted names | `hive."S"."T"` is the table `hive.s.t` (the hive catalog folds names) |
| view / missing table | `ALTER TABLE EXECUTE is not supported for views` / `Table 'hive.s.nope' does not exist` |
| sizes | airlift `DataSize`: units `B kB MB GB TB PB`, case-sensitive, powers of 1024; a fraction rounds to the nearest byte, half up (`374.5B` is 375 bytes); more than 2^63 − 1 bytes → `size is too large to be represented in bytes` |
| threshold | files with size **≤** threshold are rewritten (a file exactly at the threshold is) |
| checks order | table, procedure, arguments, the session property, then `WHERE` |
| zero-byte file | skipped by reads and by `OPTIMIZE` (left in place) |
| old files | deleted together with their Hadoop `.crc` side files |

## Decision

### Generic in the core

- `codelake_core::table_procedure`: a catalog declares `TableProcedure`s with
  typed `ProcedureArgument`s (the same `PropertyType`s as session properties).
  `bind` applies defaults, names and positions, types and parsing, with the
  messages above. Nothing in the core knows `OPTIMIZE`.
- `WritableCatalog::check_table_procedure(call, session)` lets the catalog
  validate its session properties before the engine rejects `WHERE`
  (Trino's order); the hive catalog checks `non_transactional_optimize_enabled`
  there.
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
- The files are listed once the table's `OPTIMIZE` lock is held, and the
  table's columns are read from the metastore *after* the listing: `ADD
  COLUMN` writes the metadata before any file has the column, so every
  listed file is read with all of its columns (a queued `OPTIMIZE` must not
  compact with the columns it saw when it was submitted).
- Rows are read with the table's columns, matched like Trino's Hive
  connector does (`hive/columns.rs`, also used by every scan): by name,
  ignoring case (a file written with `ID` feeds `id`); a column a file lacks
  reads `NULL`; a column stored with a type the table's cannot hold without
  loss (a column dropped and added again as another type) fails the read
  with `Unsupported Trino column type` instead of being cast. Zero-byte
  files are not data (Trino skips them).
- Timestamps are rewritten **exactly as stored**, whatever the precision
  and Parquet time zone of the catalog that compacts (amended 2026-09-25,
  red-team W1-3/W1-7; owner decision: OPTIMIZE keeps the original
  precision): the instants `INT96` holds, to the nanosecond, over its whole
  range (years 1 to 9999). The Parquet reader decodes `INT96` in one Arrow
  unit, and nanoseconds wrap outside 1677–2262, so the files are read twice
  in the same order, in microseconds (every instant fits) and in
  nanoseconds (the digits below a microsecond), and paired row by row into
  exact `Decimal128` nanoseconds (`hive/exact_timestamps.rs`). The files are
  written by the same Trino-compatible writer as `INSERT` (`INT96`
  timestamps, ADR 0007), without any time zone conversion: the instants are
  kept, and `writer.time.zone` records the catalog's zone.
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
2. Under the table's commit lock (in-process, and across processes, see
   below), verify the table still exists and every snapshot file is still
   there with the same size.
3. Publish the intent: create `.codelake-optimize.json` with *create if
   absent*. It lists the files to add (hidden and final names, rows) and to
   remove (name, size). If it already exists, another swap is in progress:
   discard and fail.
4. Rename each new file to its final name; delete the old files and their
   `.crc`; drop the table statistics (like `INSERT`); delete the marker.

Once the marker exists, the swap only moves forward. Every step is
idempotent, so `recover` can finish it at any point. `recover` runs before
each `OPTIMIZE` of the table and for every table when the catalog is opened
(`Connector::catalog`), before any query.

**Cross-process lock.** In-process `tokio` locks do not reach other
`CodeLake` processes (nodes) on the same warehouse, and a node opening the
catalog while another swapped once took the live swap for a crash, undid it
and lost every compacted row. So steps 2–4 and every `recover` also hold an
exclusive advisory `flock` on `.codelake/locks/<schema>/<table>.lock` under a
local warehouse root (hidden from Trino, outside the table directory), and
scans hold it shared while they list the files. The marker is read only
under that lock, so a recovery never sees a live swap; the OS releases the
lock of a process that dies, so a crash never leaves it held. `recover`
checks each new file's hidden name before its final name, so even without
the lock a concurrent rename cannot make a file look lost.

`recover` decides:

- every new file present (hidden or renamed) → finish the swap;
- a new file gone while all old files remain (removed by someone else) → undo;
- anything else → stop with an error naming the marker; never guess.

A table whose marker cannot be settled (unreadable, or listing files that
are gone) does not stop the catalog from opening: the error is logged, the
other tables stay available, and that table refuses scans and `OPTIMIZE`
until someone inspects it. Scans refuse any table whose listing, taken under
the commit locks, still shows a marker: under those locks a marker is never
a swap in progress, only one that failed and awaits recovery.

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

- **Readers during the swap (step 4, milliseconds).** A CodeLake scan on a
  local warehouse never lists during a swap, in this process or another (it
  takes the commit locks shared while listing), but a query planned before
  the swap can fail with a missing file (loud, not wrong). Trino or the sink
  listing the directory during step 4 can see old and new files together
  (duplicates) or miss some; same as Trino's own `OPTIMIZE`. Schedule
  compaction outside the sink's window.
- **Object-store warehouses** (`hive-s3`, not shipped) have no cross-process
  lock: there, two CodeLake nodes compacting one table, or a recovery racing
  a live swap, are not excluded (the hidden-name-first check and the
  snapshot check narrow the window but do not close it). One node compacts.
- **CodeLake and Trino compacting the same table at once** is not
  supported, as in Trino: Trino does not take the lock.
- **Sub-microsecond timestamps** (a Trino catalog at `NANOSECONDS`
  precision) are kept by `OPTIMIZE` since 2026-09-25 (they were rounded to
  microseconds before); compaction reads each file twice for that (see
  above).
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
  default). Fixed by the F2 interoperability work merged from `main`
  (ADR 0007: CodeLake writes `writerVersion` `431`).

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
  memory), `hive::columns` (name and type matching).
- Independent acceptance suite (`tests/integration/f9_acceptance.rs`, real
  `trinodb/trino:431`): statements and thresholds answered like Trino, every
  type and extreme value, microsecond timestamps, case-insensitive columns,
  retyped columns, zero-byte files, volume, memory pool, concurrency, `DROP
  TABLE`, a node restarting during another node's swap, crash states, an
  unreadable marker.
- Integration (`tests/integration/hive_optimize.rs`, real `trinodb/trino:431`):
  a table written by Trino in four statements with an `ADD COLUMN` is
  compacted by CodeLake and a strict Trino returns exactly its original rows
  from a single file; a table written and compacted by CodeLake is read by
  Trino (lenient catalog, see above) with the same rows as its uncompacted
  copy.
