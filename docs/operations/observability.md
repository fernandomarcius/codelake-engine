# Observability

Four signals: Prometheus metrics and health endpoints on the ops listener,
OTLP traces (optional), JSON logs on stdout, and the canary. What exists today
and what does not:

| Question | Signal |
|---|---|
| Is the process up? Should it get traffic? | `/livez`, `/readyz` |
| How full are memory and spill? | `codelake_memory_*`, `codelake_spill_*` |
| Is a source reachable? | `codelake_source_up`, `/healthz` |
| Which queries run now, who sent them, how did they end? | `system.runtime.queries` (HTTP protocol only) |
| Where did a slow query spend its time? | traces |
| Are answers right and fresh, is latency within the objective? | [canary](canary.md) |
| Query rate, latency percentiles, errors per code | **not exported yet** as metrics: use the canary, traces and the clients' own metrics |

## Health endpoints (`server.http_listen`)

| Endpoint | Answers | Use |
|---|---|---|
| `/livez` | `ok` while the process responds; never checks sources | liveness probe |
| `/readyz` | 200 `{"ready":true}`; 503 when draining or a **required** source is down (`reason`) | readiness probe, load balancer |
| `/healthz` | version, draining, memory (`limit_bytes`, `reserved_bytes`, `spill_used_bytes`, `spill_limit_bytes`), queries (`running`, `queued`), every source with `up` and its error | humans, incident start |

Source checks are cached for 5 s and time out after 2 s, so probes and
scrapes do not load the sources.

## Metrics (`/metrics`, Prometheus text)

| Metric | Type | Meaning |
|---|---|---|
| `codelake_memory_limit_bytes` | gauge | `memory.limit` |
| `codelake_memory_reserved_bytes` | gauge | memory reserved by running queries now |
| `codelake_spill_used_bytes` | gauge | spill on disk now |
| `codelake_spill_limit_bytes` | gauge | `memory.spill_limit` |
| `codelake_source_up{source,kind}` | gauge | 1 reachable, 0 not |
| `codelake_queries_running`, `codelake_queries_queued` | gauge | admission slots in use on the node |
| `codelake_queries_suspended` | gauge | HTTP queries that gave their running slot back while their client does not read (`max_suspended_queries`) |
| `codelake_resource_group_running{resource_group}`, `codelake_resource_group_queued{resource_group}` | gauge | the same per configured group |
| `codelake_queries_rejected_total{scope}` | counter | refused with a full queue; `scope`: `node`, `resource_group`, `user` |
| `codelake_queries_timed_out_total{phase}` | counter | time limit reached; `phase`: `queued`, `running` |
| `codelake_queries_completed_total{protocol,state}` | counter | governed queries ended, `FINISHED` or `FAILED` |
| `codelake_statements_total{protocol}` | counter | statements received on the PostgreSQL wire (`simple`, `extended`) |
| `codelake_connections_open{listener}`, `codelake_connections_max{listener}` | gauge | open connections and the limit of each listener (`ops`, `pg`, `trino`; ADR 0018) |
| `codelake_connections_refused_total{listener,reason}` | counter | answered with an error and closed: `limit` (listener full), `descriptors` (no descriptor left) |
| `codelake_connections_closed_total{listener,reason}` | counter | HTTP connections closed by a timeout: `idle`, `header_read` |
| `codelake_accept_errors_total{listener,error}` | counter | failed `accept`: `descriptors` (`EMFILE`/`ENFILE`), `other` |
| `codelake_open_files`, `codelake_open_files_limit` | gauge | descriptors open now and the soft limit |

The canary adds, when run with `--textfile`: `codelake_canary_run_ok`,
`codelake_canary_check_ok{check,status}`, `codelake_canary_latency_seconds`,
`codelake_canary_freshness_lag_seconds`, `codelake_canary_last_run_timestamp_seconds`.

Container metrics come from the platform (cAdvisor / kubelet, or
`docker stats`): working set, CPU throttling, restarts, OOM kills.

Suggested alerts (tune thresholds per installation):

```yaml
groups:
  - name: codelake
    rules:
      - alert: CodeLakeCanaryFailing
        expr: codelake_canary_run_ok == 0
        for: 0m
        labels: { severity: page }
      - alert: CodeLakeCanaryNotRunning
        expr: time() - codelake_canary_last_run_timestamp_seconds > 900
        labels: { severity: page }
      - alert: CodeLakeSourceDown
        expr: codelake_source_up == 0
        for: 5m
        labels: { severity: page }
      - alert: CodeLakeMemoryPoolSaturated
        expr: codelake_memory_reserved_bytes / codelake_memory_limit_bytes > 0.9
        for: 15m
        labels: { severity: ticket }
      - alert: CodeLakeSpillFilling
        expr: codelake_spill_used_bytes / codelake_spill_limit_bytes > 0.8
        for: 10m
        labels: { severity: ticket }
      - alert: CodeLakeConnectionsRefused
        expr: increase(codelake_connections_refused_total[10m]) > 0
        labels: { severity: ticket }
      - alert: CodeLakeDescriptorsExhausted
        expr: increase(codelake_accept_errors_total{error="descriptors"}[5m]) > 0
        labels: { severity: page }
      - alert: CodeLakeContainerNearLimit
        expr: >
          container_memory_working_set_bytes{container="codelake"}
          / container_spec_memory_limit_bytes{container="codelake"} > 0.9
        for: 10m
        labels: { severity: ticket }
      - alert: CodeLakeRestarting
        expr: increase(kube_pod_container_status_restarts_total{container="codelake"}[30m]) > 0
        labels: { severity: ticket }
```

A wrong result has no metric of its own: it shows only as a canary mismatch.
Page on it.

## Traces (OTLP)

Enable with `[telemetry] otlp_endpoint` (ADR 0010). One trace per query:

```text
codelake.query            query id, user, catalog, schema, correlation id, redacted SQL + hash, rows, status
├─ codelake.parse / codelake.plan / codelake.execute
│  ├─ hive.scan           files selected: count, bytes, names
│  └─ codelake.remote     one per federated scan
│     └─ sqlserver.query  redacted T-SQL, server, database, pool wait, retries, rows, error number
├─ codelake.fetch         rows sent to the client; `cancelled` when the client left
hive.write                file name and rows of each CTAS / INSERT
```

- A client that sends a W3C `traceparent` gets the query in its own trace
  (dbt or the scheduler can correlate a model run with its queries); the
  correlation header of the client protocol is kept as
  `codelake.correlation_id`.
- SQL text and error messages are **redacted**: literals become `?`. Group
  queries by `codelake.sql.hash`.
- PostgreSQL wire statements appear as `codelake.pg.statement` spans in one
  trace per session; their execution spans are separate traces for now.
- Look for: the longest child of a slow `codelake.query`; a `sqlserver.query`
  with long pool wait (raise `pool_size`), long duration (the source) or
  `codelake.retries` above zero (the source dropped a connection: see the
  `WARN` "sqlserver connection lost while reading" lines in the logs); a
  `hive.scan` with thousands of files (compact); `codelake.fetch` long while
  execution is short (a slow client).

## Logs

JSON, one object per line on stdout; `RUST_LOG` sets the level (`info` by
default, `codelake_core=debug` for one crate). Fields: `timestamp`, `level`,
`target`, `fields.message` and the event's fields, and `span` with the
current query's fields (`codelake.query_id`, `codelake.trace_id`).

The engine does not log every query; logs carry lifecycle and warnings. Lines
worth an alert or a look:

| Message | Meaning |
|---|---|
| `engine ready` / `source attached` | startup finished / one per source |
| `optional source skipped` | an optional source was down at startup; absent until restart |
| `shutdown requested, draining` | SIGTERM received |
| `encrypt = off: credentials and data travel in clear text` | a SQL Server source without TLS |
| `trust_server_certificate = true: …` | a SQL Server source without certificate validation |
| `finishing an interrupted OPTIMIZE` / `undoing an interrupted OPTIMIZE` | crash recovery of a compaction |
| `an interrupted OPTIMIZE could not be settled; …` | a table refuses scans: [incidents.md](incidents.md#missing-file-during-optimize) |
| `table listed but unreadable` | metadata of a table could not be read while listing |
| any `ERROR` | startup failure (configuration, a required source) or a runtime error |

```bash
kubectl -n <ns> logs deploy/codelake --since=1h | jq -c 'select(.level == "WARN" or .level == "ERROR") | {timestamp, level, msg: .fields.message}'
```
