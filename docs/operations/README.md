# Operations runbooks

Runbooks for running CodeLake in production. Each one is a procedure: what to
run, how to check that it worked, and how to undo it. They describe the engine
as it is in this repository; where a capability is missing (client
authentication, query governance, per-query metrics), the runbook says so and
gives the workaround.

Every installation is its own: its own hosts, catalogs, users and data. Nothing
here names a customer system. Values in angle brackets are placeholders:

| Placeholder | Meaning | Example |
|---|---|---|
| `<ns>` | Kubernetes namespace | `codelake` |
| `<image>` | image by digest | `registry.example.com/codelake@sha256:…` |
| `<config>` | path of the TOML configuration | `/etc/codelake/codelake.toml` |
| `<warehouse>` | root of a `hive` catalog on disk | `/data/warehouse` |
| `<lake>` | name of a `hive` catalog | `lake` |
| `<ops>` | ops HTTP listener (`server.http_listen`) | `127.0.0.1:8080` |
| `<http>` | client HTTP protocol listener (`[server.trino] listen`) | `127.0.0.1:8081` |
| `<pg>` | PostgreSQL wire listener (`server.pg_listen`) | `127.0.0.1:5432` |

## Runbooks

| Runbook | When |
|---|---|
| [Install](install.md) | first installation with Docker Compose or Kubernetes |
| [Configure](configuration.md) | memory ceiling, spill, catalogs, listeners, telemetry, security |
| [Upgrade and rollback](upgrade-rollback.md) | a new image, and going back |
| [Sizing](sizing.md) | CPU, memory and spill disk for a workload |
| [Incidents](incidents.md) | stuck query, out of memory, spill full, source down, missing file, queue full, latency |
| [Compaction](compaction.md) | `OPTIMIZE` of hive tables, with one or two engines on a warehouse |
| [Backup and restore](backup-restore.md) | file metastore and warehouse |
| [Observability](observability.md) | metrics, traces, logs and alerts |
| [Migration](migration.md) | moving clients and catalogs from the reference engine, cut-over and back |
| [Canary](canary.md) | `scripts/ops/canary.sh`: correctness, latency and freshness probes |
| [Go-live checklist](go-live.md) | the gate before serving production |

## Service levels

The canary measures two objectives; each installation sets its own numbers in
the canary configuration ([docs/examples/canary.json](../examples/canary.json)):

- **Correctness:** every canary check returns the expected answer. One wrong
  answer is an incident, whatever the latency.
- **Latency / freshness:** each check answers within its `max_latency_ms`, and
  the newest row of each monitored table is at most `freshness.max_lag_secs`
  old (the pipelines' end-to-end objective, for example 5 minutes).

## Ground rules

- A wrong result is the worst failure. When correctness and availability
  conflict, stop serving (roll back or route clients to the previous engine)
  rather than serve a suspect answer.
- Secrets live in environment variables or files the process reads, never in
  the TOML file, a command line, a ticket or a chat.
- Every change is reversible: note the current image digest and a copy of the
  configuration before changing either.
