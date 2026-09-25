# Configure

One TOML file (`--config <config>` or `CODELAKE_CONFIG`) plus environment
variables for secrets. Unknown keys are refused. Validate every change before
it reaches a running node:

```bash
codelake check-config --config <config>    # exit 0 valid, 2 invalid (every problem listed)
```

The server reads the file once at startup: a change takes effect on restart
(Compose: `docker compose … up -d --force-recreate`; Kubernetes: the
ConfigMap generator changes its name, so `kubectl apply -k` rolls the pods).
Rollback = the previous file and the same restart.

## Memory: `[memory]`

| Key | Default | Meaning |
|---|---|---|
| `limit` | required | ceiling of query memory (joins, sorts, aggregations, buffers to clients). Minimum 64 MiB |
| `spill_dir` | required | absolute path for spill files; a dedicated volume |
| `spill_limit` | required | disk all queries together may spill; `> 0` |
| `metadata_cache_limit` | 64 MiB | Parquet metadata cache; must be below `limit` |

How the ceiling behaves (ADR 0003):

- every operator reserves from one pool of `limit` bytes; over it, operators
  that can spill (sort, aggregation, the sort-merge join a large hash join
  becomes) write to `spill_dir`;
- when neither memory nor spill is left, **only that query** fails with
  `EXCEEDED_LOCAL_MEMORY_LIMIT`; the process keeps serving;
- the process uses more than `limit`: the allocator, connection buffers,
  the batch each worker is producing and source drivers are outside the pool.
  Keep `limit` at about **75% of the container limit** (the remaining 25% is
  that headroom). Measured gaps and their bounds are in ADR 0003.

Tuning, in order of preference:

1. Queries fail with `EXCEEDED_LOCAL_MEMORY_LIMIT` while the container has
   room: raise `limit` (and the container limit with it, keeping 75%).
2. The container is OOM-killed while the pool is below its limit: lower
   `limit`, or lower `execution.batch_size` when sources return wide rows
   (documents, long text): at 1024 rows, 4 KiB rows make 4 MiB batches.
3. Many concurrent queries: set `execution.target_partitions` below the CPU
   count; it is also the number of runtime workers, which bounds the batches
   being produced outside the pool.

## Execution: `[execution]`

| Key | Default | Meaning |
|---|---|---|
| `batch_size` | 8192 | rows per batch (1 to 1 048 576) |
| `target_partitions` | CPUs | parallelism per query and runtime workers |
| `source_startup_timeout_secs` | 60 | time to attach every source at startup |

## Catalogs: `[[source]]`

Each source is a SQL catalog named `name` (`[a-z_][a-z0-9_]*`, not
`datafusion`, `information_schema` or `system`). `required = false` lets the
server start without it (the source is skipped with a warning and stays
absent until the next restart); a required source that cannot be attached
stops startup.

| `kind` | Keys (defaults) | Notes |
|---|---|---|
| `sqlserver` | `host`, `port` (1433), `database`, `user`, `password_env`, `encrypt` (`required`), `trust_server_certificate` (false), `pool_size` (10) | `encrypt = "off"` sends credentials in clear text; `trust_server_certificate = true` skips server identity; both are logged as warnings and are for test networks only |
| `postgres` | `host`, `port` (5432), `database`, `user`, `password_env`, `sslmode` (`verify-full`) | |
| `mysql` | `host`, `port` (3306), `database`, `user`, `password_env`, `sslmode` (`required`) | |
| `mongodb` | `connection_string_env` | the whole URI is the secret |
| `hive` | `warehouse` (absolute path or `file:` URL), `timestamp_precision` (`MILLISECONDS`, `MICROSECONDS`, `NANOSECONDS`) | Parquet in the file-metastore layout (ADR 0007). S3/MinIO warehouses are not in this build |

Use a read-only database account for federated sources. A hive warehouse must
be writable by uid 65532.

## Listeners and security

| Key | Meaning |
|---|---|
| `server.pg_listen` | PostgreSQL wire (psql, BI tools) |
| `server.http_listen` | ops HTTP: `/livez`, `/readyz`, `/healthz`, `/metrics` |
| `[server.trino] listen` | client HTTP protocol (`/v1/statement`; the CLI, JDBC, Python and dbt clients of the reference engine). Off when the table is absent |
| `server.max_connections` | PostgreSQL connections (default 256) |
| `server.shutdown_grace_secs` | drain time after SIGTERM (default 30); keep it below the orchestrator's kill timeout |
| `[server.trino] max_active_queries` | accepted and unfinished queries (default 100); more are refused with `QUERY_QUEUE_FULL` |
| `[server.trino] client_timeout_secs` | a query whose client stops polling this long is canceled (`ABANDONED_QUERY`, default 300) |
| `[server.trino] history_size`, `history_retention_secs` | finished queries kept in `system.runtime.queries` (100, 900 s) |
| `[server.trino] max_query_length` | longest SQL text (1 000 000 characters) |
| `server.tls` | `cert_path`, `key_path` for the PostgreSQL listener; a missing file stops startup |

**There is no client authentication yet** on either query listener (ROADMAP
M1 item 5). Anyone who reaches the port can query every catalog with the
sources' credentials. Therefore:

- The server refuses to bind a query listener beyond loopback unless
  `server.allow_unauthenticated_network = true`.
- Set it only when the network layer restricts who reaches the port: the
  `NetworkPolicy` of `deploy/k8s`, ports published on the host's `127.0.0.1`
  (Compose), or a firewall/security group that admits only the client hosts.
- The ops listener has no secrets but `/healthz` shows source host names and
  errors: expose it to the monitoring network only.
- The identity a client sends (`user`) is recorded in
  `system.runtime.queries` and traces; it is **not** authenticated and grants
  nothing.

## Telemetry: `[telemetry]`

Off unless `otlp_endpoint` is set. `otlp_endpoint` is a plain `http://`
collector address (an agent on the node or pod handles TLS upstream);
`sampling_ratio` (0.0–1.0) applies to new traces, a caller's `traceparent`
decides for its own. See [observability.md](observability.md).

Logs are JSON on stdout; `RUST_LOG` sets the level (default `info`).

## Session properties

Clients set them per session (`SET SESSION name = value` on the HTTP protocol):

| Property | Default | Use |
|---|---|---|
| `query_max_run_time`, `query_max_execution_time` | `100d` | per-query time limit (`EXCEEDED_TIME_LIMIT`); set them in the clients' profiles (for example `1h`) |
| `sort_merge_oversized_joins` | `true` | a hash join whose build side exceeds its slice of the pool runs as a spilling sort-merge join |
| `<lake>.non_transactional_optimize_enabled` | `false` | required for `OPTIMIZE` ([compaction.md](compaction.md)) |
| `<lake>.target_max_file_size` | `1GB` | file size `OPTIMIZE` writes |

## Change procedure

1. Copy the running configuration (`<config>.bak-<date>`).
2. Edit; `check-config`; review the diff.
3. Restart one node (Kubernetes rolls one pod at a time: `maxUnavailable: 0`).
4. Verify `/readyz`, `/healthz`, the canary.
5. Revert: put the copy back and restart.
