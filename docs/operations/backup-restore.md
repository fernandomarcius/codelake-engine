# Backup and restore

CodeLake keeps no state of its own besides hive catalogs: federated sources
are backed up by their own databases, and a node can be rebuilt from its image
and configuration. What needs a backup:

| What | Where | Size | How often |
|---|---|---|---|
| Configuration | the TOML file (ConfigMap / host file), without secrets | KB | every change (version it) |
| Metastore of each hive catalog | metadata files inside the warehouse (below) | KB–MB | before every upgrade, compaction campaign or schema change; daily |
| Warehouse data | Parquet files inside the warehouse | the data | as the data's owners require |

Secrets are backed up by the secret manager, not here.

## Layout

The metastore is a set of files in the warehouse, in the reference engine's
file-metastore layout (ADR 0007); data files sit next to their table's
metadata:

```text
<warehouse>/.<schema>.trinoSchema               schema metadata (+ ..<schema>.trinoSchema.crc)
<warehouse>/<schema>/<table>/.trinoSchema        table or view metadata (+ ..trinoSchema.crc)
<warehouse>/<schema>/<table>/.trinoPermissions/  owner
<warehouse>/<schema>/<table>/<data files>        Parquet, no extension
<warehouse>/<schema>/<table>/.codelake-optimize.json   only while an OPTIMIZE swap is unfinished
<warehouse>/.codelake/                           locks and staging of running writes: never back up or restore
```

The metadata file names are the reference engine's; if a later version makes
them configurable (docs/issues/ISS-011), use the names the installation
configures in the commands below.

Data files are immutable: `INSERT` adds files, `OPTIMIZE` replaces small files
with new ones, `DROP TABLE` removes the directory. An incremental copy (rsync,
restic, file system snapshots) only moves new files.

## Consistency

A table is its metadata plus the files in its directory at one moment. For a
consistent copy, pause what writes: the pipelines' writes, `OPTIMIZE` jobs and
DDL (in CodeLake and in any other engine sharing the warehouse). Reads may go
on. When the file system supports snapshots (LVM, ZFS, a CSI
`VolumeSnapshot`), take one while writes are paused and copy from the
snapshot: the pause lasts seconds.

## Metastore

```bash
# Backup: metadata only, small and fast (writers paused).
cd <warehouse>
find . -path ./.codelake -prune -o \
  \( -name '*.trinoSchema' -o -name '*.trinoSchema.crc' -o -path '*/.trinoPermissions/*' \
     -o -name '.codelake-optimize.json' \) -type f -print0 \
  | tar --null -czf /backup/metastore-$(date -u +%Y%m%dT%H%M%SZ).tar.gz -T -

# Verify the archive lists every schema and table you expect.
tar -tzf /backup/metastore-<stamp>.tar.gz | grep -c '/.trinoSchema$'
find . -path ./.codelake -prune -o -name '.trinoSchema' -type f -print | wc -l
```

The two counts must match.

## Warehouse

```bash
# Full or incremental copy (writers paused, or from a snapshot).
rsync -a --delete --exclude '/.codelake/' --exclude '.*.inprogress' \
  <warehouse>/ <backup-host>:/backup/warehouse/
```

Verify with row counts of a sample of tables in both copies (restore into a
scratch warehouse and query it with a second CodeLake configured on it), or
at least with file counts per table directory.

## Restore

1. Stop every writer of the warehouse (CodeLake nodes and any other engine),
   or all of CodeLake if the whole warehouse is restored.
2. Keep what is there: move the current directory aside, to a directory
   outside the warehouse on the same file system
   (`mv <warehouse>/<schema>/<table> <aside>/<schema>.<table>.before-restore`),
   instead of deleting it.
3. Restore:
   - one table: copy its directory back from the warehouse backup, metadata
     and data together;
   - metadata only (data intact, metadata damaged):
     `tar -xzf /backup/metastore-<stamp>.tar.gz -C <warehouse>`; a table whose
     data files changed since the backup (inserts, compaction) must be restored
     with its data too, or it will show the files present now under the
     metadata of then;
   - everything: `rsync -a /backup/warehouse/ <warehouse>/`.
4. Ownership: `chown -R 65532:65532 <warehouse>/<restored path>` (or the
   shared group used with the other engine).
5. Start CodeLake; the catalog opens every table and recovers any unfinished
   `OPTIMIZE` from its marker.
6. Verify: `SHOW TABLES FROM <lake>.<schema>`, row counts against the numbers
   recorded before, the canary.
7. Replay the writes made after the backup from the pipelines (their
   watermarks usually do this on the next run), then remove the
   `.before-restore` directories once the result is confirmed.

Going back from a restore: stop writers, move the restored directory aside and
the `.before-restore` one back into the warehouse.
