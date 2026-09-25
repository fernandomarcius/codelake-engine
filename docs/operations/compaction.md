# Compaction (OPTIMIZE)

Every `INSERT` into a hive table writes one new file (ADR 0007), so tables fed
by micro-batches collect thousands of small files and reads slow down.
`OPTIMIZE` rewrites the small files into fewer large ones, preserving rows and
columns, crash-safe (ADR 0009).

```sql
SET SESSION <lake>.non_transactional_optimize_enabled = true;
ALTER TABLE <lake>.<schema>.<table> EXECUTE optimize(file_size_threshold => '100MB');
```

Files of at most `file_size_threshold` are rewritten into files of up to
`<lake>.target_max_file_size` (default `1GB`). Fewer than two small files:
nothing is rewritten. Sizes use the units `B kB MB GB TB` (case-sensitive,
powers of 1024).

## One engine compacts

Two engines (or two CodeLake nodes on a warehouse without working file locks)
compacting the **same table at the same time** is not supported: the
reference engine takes no lock, and each would delete files the other is
reading or rewriting. While another engine shares the warehouse (for example
during a [migration](migration.md)):

- choose **one** engine that compacts, for every table; disable compaction
  jobs on the other (its schedule, maintenance scripts, cron);
- CodeLake nodes on one local warehouse exclude each other with advisory file
  locks (`.codelake/locks/…`); on a network file system confirm that `flock`
  works across hosts before letting two nodes compact, otherwise compact from
  one node only;
- a table written by one engine can be compacted by the other: CodeLake
  compacts files the reference engine wrote, and the reference engine reads
  what CodeLake compacted (tested against version 431).

## Window

- Outside the pipelines' writes to that table: an `INSERT` running during the
  compaction is safe (its new file is not touched), but a consumer that lists
  the directory during the swap (milliseconds) can see old and new files
  together or miss some. Consumers that track **file names** (for example a
  sink that loads each new file once) see the compacted files as new: reset
  their checkpoint for that table after compacting, or exclude compacted
  tables from them.
- Free disk: the new files are written before the old ones are deleted, so
  the table needs its size again in free space.
- Memory: one scan, one writer; the open row group is reserved in the pool.
  A short pool fails the procedure and leaves the table untouched.

## Procedure

1. Pick the tables: many files, small average size.

   ```bash
   find <warehouse>/<schema>/<table> -maxdepth 1 -type f ! -name '.*' | wc -l
   du -sh <warehouse>/<schema>/<table>
   ```

2. Record the row count (and a checksum of a key column for critical tables):

   ```sql
   SELECT count(*), checksum(<key>) FROM <lake>.<schema>.<table>;
   ```

3. Run `OPTIMIZE` (statement above) through the HTTP protocol, from a job that
   runs in the window. It answers when the swap is done.
4. Verify: the same count and checksum as step 2; the file count fell; no
   hidden `.*.inprogress` files and no `.codelake-optimize.json` remain in the
   table directory.
5. Reset the checkpoints of file-name consumers of that table, if any.

## When it fails

- `OPTIMIZE procedure must be explicitly enabled…`: the session property is
  missing in that session.
- `Resources exhausted`: memory; rerun with more room, or with a lower
  `target_max_file_size`.
- Crash or kill in the middle: the next `OPTIMIZE` of the table, or the next
  start of the catalog, finishes or undoes the swap from its marker. Hidden
  `.*.inprogress` files left by a crash **before** the swap cost only disk;
  remove them with the process stopped. See
  [incidents.md](incidents.md#missing-file-during-optimize) when a table
  refuses scans.

Nothing to roll back: the rows are the same. To return to the small files,
restore the table from a [backup](backup-restore.md) taken before.
