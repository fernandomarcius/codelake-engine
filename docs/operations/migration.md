# Migration from the reference engine

CodeLake speaks the client protocol, SQL dialect, functions, session
properties and error codes of the reference engine (Trino 431) and reads and
writes its hive file-metastore layout, so clients move by changing an address.
This runbook moves one installation: inventory, catalogs, clients, a period of
running side by side, cut-over, and the way back. Both engines can share one
warehouse during the move.

## 1. Inventory and blockers

List what the installation uses and check it against what CodeLake has today:

| Item | Check |
|---|---|
| Catalogs and their connectors | the mapping below; `iceberg` catalogs, S3/MinIO hive warehouses, partitioned hive tables, ORC/Avro tables and `array`/`map`/`row` columns in hive tables are **not** supported yet |
| Client authentication | CodeLake has none yet: the listeners must stay on a private network ([configuration.md](configuration.md#listeners-and-security)). If the reference installation authenticates its clients, keep it for those clients |
| Table ownership rules (`hive.security=sql-standard`) | written, not enforced yet |
| TLS on the client HTTP protocol | not available; PostgreSQL wire only |
| SQL the clients send | run the clients' queries against CodeLake before the cut-over (shadow run below); a refusal (`NOT_SUPPORTED`, `TYPE_MISMATCH`) is visible, a different answer is caught only by comparison |
| Session properties the clients set | undeclared names fail with `INVALID_SESSION_PROPERTY`; performance-only knobs of the reference engine are accepted with a warning |
| Parquet INT96 time zone (`hive.parquet.time-zone`) | docs/issues/ISS-003: not configurable in this build; confirm how timestamps written by the other engine read back before the cut-over |

## 2. Catalogs

Each catalog file of the reference engine (`etc/catalog/<name>.properties`)
becomes one `[[source]]` with the **same name**, so SQL keeps its three-part
names. Never copy a password or key from those files into the TOML: put it in
the secret store and reference the variable (`password_env`). If the
reference installation keeps plaintext secrets in its catalog files, rotate
them as part of the migration.

| Reference `connector.name` | CodeLake `kind` | Mapping |
|---|---|---|
| `sqlserver` | `sqlserver` | `connection-url=jdbc:sqlserver://<host>:<port>;databaseName=<db>;encrypt=…` → `host`, `port`, `database`, `encrypt`; `connection-user` → `user`; `connection-password` → a secret named by `password_env` |
| `postgresql` | `postgres` | `jdbc:postgresql://<host>:<port>/<db>` → `host`, `port`, `database`; TLS → `sslmode` |
| `mysql` | `mysql` | `jdbc:mysql://<host>:<port>` → `host`, `port`; the database → `database` |
| `mongodb` | `mongodb` | `mongodb.connection-url` → a secret named by `connection_string_env` |
| `hive` with `hive.metastore=file` | `hive` | `hive.metastore.catalog.dir=file:///<path>` → `warehouse = "<path>"`; `hive.timestamp-precision` → `timestamp_precision` |

```toml
[[source]]
name = "<same name as the catalog file>"
kind = "sqlserver"
host = "<host>"
database = "<db>"
user = "<read-only user>"
password_env = "<CATALOG>_PASSWORD"
```

Validate with `check-config` ([install.md](install.md#before-you-start)).

## 3. Clients

| Client | Change |
|---|---|
| dbt (`dbt-trino` adapter) | a new target in `profiles.yml` pointing at CodeLake; keep the old one |
| JDBC / CLI of the reference engine | host and port of `[server.trino] listen` |
| Scripts on `POST /v1/statement` | base URL; the protocol and `nextUri` paging are the same; the header prefix is CodeLake's unless configured (below) |

### Header prefix and version

Clients of the reference engine send `X-Trino-*` headers and some check the
server version. Until each client is ported to CodeLake's names
(`X-CodeLake-*`), configure the listener with theirs (ISS-011):

```toml
[server.trino]
listen = "0.0.0.0:8081"
header_prefix = "X-Trino-"
reported_version = "431"
```
| BI tools on PostgreSQL wire | `server.pg_listen` (new capability, not a migration) |

dbt profile, with the previous engine kept as a second target for the way
back:

```yaml
<project>:
  target: codelake
  outputs:
    codelake:
      type: trino
      method: none            # no authentication yet
      http_scheme: http
      host: <codelake-host>
      port: <http-port>       # [server.trino] listen
      user: <pipeline-user>
      database: <catalog>     # catalog, e.g. the hive catalog the models write to
      schema: <schema>
      threads: 4
      session_properties:
        query_max_run_time: 1h
    reference:
      type: trino
      method: none
      http_scheme: http
      host: <reference-host>
      port: <reference-port>
      user: <pipeline-user>
      database: <catalog>
      schema: <schema>
      threads: 4
```

Switching is `dbt build --target codelake` (or `target:` in the profile);
switching back is `--target reference`.

Keep the listener on the same port the clients already use when the host
changes only by DNS: set `[server.trino] listen` to that port and move the
DNS name. The examples put the ops listener on 8080; move it when the clients
already use that port.

### Hive warehouse names

A warehouse the reference engine wrote holds its metastore names
(`.trinoSchema`, `.trinoPermissions`, `Presto View`). Either keep them while
both engines share the warehouse:

```toml
[[source]]
name = "hive"
kind = "hive"
warehouse = "/data/warehouse"

[source.metastore_names]
metadata_file = ".trinoSchema"
permissions_dir = ".trinoPermissions"
view_marker = "Presto View"
view_parameter = "presto_view"
version_parameter = "presto_version"
writer_version = "431"
```

or, once CodeLake alone uses it (after the cut-over and a
[metastore backup](backup-restore.md#metastore)), rename it to the source's
names with the servers stopped:

```bash
codelake hive migrate-names --config codelake.toml --source hive \
  --from-metadata-file .trinoSchema --from-permissions-dir .trinoPermissions \
  --from-view-marker "Presto View" --from-view-parameter presto_view \
  --from-version-parameter presto_version            # prints the plan
# the same with --apply changes the files
```

The command rewrites every schema, table and view metadata file under the
new name with its checksum (writer version, version and view parameters,
view markers), removes the old one, and renames the permissions
directories; data files are not touched. It refuses the whole plan, changing
nothing, when a new name exists or a file does not parse. The reference
engine cannot read the warehouse afterwards.

## 4. Side by side

1. **Shadow run.** Point a copy of the pipeline at CodeLake with the same
   inputs, writing to a separate schema (or a separate warehouse), and compare
   every output table with the reference engine's: row count and a checksum
   per table (`SELECT count(*), checksum(<col>) …` for each column, or the
   canary's `expect.reference`). Repeat over several days, including the
   heaviest runs. Every divergence is investigated before the cut-over.
2. **Canary.** Configure [the canary](canary.md) with `reference` pointing at
   the reference engine and checks over the tables the pipelines own, plus
   freshness and latency checks. Run it periodically; it is the evidence for
   the go-live gate.
3. **Shared warehouse rules** while both engines point at the same hive
   warehouse:
   - one writer per table at a time: each pipeline writes through one engine;
   - one engine compacts ([compaction.md](compaction.md#one-engine-compacts));
   - DDL (`CREATE`, `DROP`, `ALTER`) through the engine that owns the table;
   - both engines run with users that can read and write the warehouse files
     (a shared group, or the same uid).

## 5. Cut-over

In a window agreed with the pipelines' owners, outside heavy runs:

1. Canary green against CodeLake and against the reference comparison.
2. Pause the pipelines (let the running cycle finish).
3. Take a [metastore backup](backup-restore.md#metastore).
4. Switch the clients: dbt target, JDBC/CLI addresses, script base URLs, or
   the DNS name. Stop the reference engine's own maintenance jobs on the
   warehouse (compaction, cleanup).
5. Resume the pipelines; watch the first cycles: the canary, `/healthz`,
   errors in the pipelines' logs, freshness.
6. Keep the reference engine installed and able to start, with its
   configuration unchanged, until the go-live period ends.

## Going back

Trigger: any wrong result, a failure the team cannot fix within the
freshness objective, or the go-live gate failing.

1. Pause the pipelines.
2. Switch the clients back (dbt `--target reference`, addresses, DNS).
3. Restart the reference engine's maintenance jobs; stop CodeLake's
   compaction jobs.
4. Tables CodeLake wrote stay readable by the reference engine: same layout,
   `writerVersion` and INT96 Parquet writer (ADR 0007, ADR 0009). No data
   conversion is needed. If a table was damaged, restore it from the backup of
   step 5.3 and replay from the pipelines' watermarks.
5. Resume the pipelines; confirm freshness and the pipelines' own checks.
6. Record why, with the evidence (canary lines, the queries, both answers).
