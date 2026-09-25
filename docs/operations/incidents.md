# Incidents

Each entry: how it shows, how to confirm, what to do, how to check it is over,
and how to undo what you did. Start every incident the same way:

```bash
curl -fsS http://<ops>/healthz | jq .                  # version, memory, every source
curl -s  http://<ops>/metrics | grep '^codelake_'      # pool, spill, sources
# Kubernetes: kubectl -n <ns> logs deploy/codelake --since=30m | jq -c 'select(.level != "INFO")'
# Compose:    docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake logs --since 30m codelake
```

and, for the HTTP protocol, list what is running (any HTTP client works; the
reference engine's CLI too):

```sql
SELECT query_id, state, "user", source, created, last_heartbeat, substr(query, 1, 120) AS query
FROM system.runtime.queries
WHERE state IN ('QUEUED', 'PLANNING', 'RUNNING', 'FINISHING')
ORDER BY created;
```

A **wrong result** reported by a user or by the canary outranks every other
incident: capture the query, both answers and the version, then route the
affected clients back to the previous engine or version
([migration.md](migration.md#going-back), [upgrade-rollback.md](upgrade-rollback.md#rollback))
before investigating.

## A query is stuck or too long

**Shows as:** a pipeline step never ends; `system.runtime.queries` shows the
query `RUNNING` for longer than usual; CPU stays high.

**Act (HTTP protocol):** cancel it as its owner (the `user` it was submitted
with; another user gets `403`):

```bash
curl -sS -X DELETE -H 'X-Trino-User: <user>' http://<http>/v1/query/<query_id>   # 204 = canceled
```

The query fails with `USER_CANCELED`, its memory returns to the pool and its
worker stops (operators yield cooperatively, ADR 0012). A client that simply
stops polling is canceled after `client_timeout_secs` (`ABANDONED_QUERY`).

**Act (either protocol, from `psql` or any client):** kill it by id as its
owner (connect with the `user` it runs as); the call needs no free slot, so
it works on a full node:

```sql
CALL system.runtime.kill_query(query_id => '<query_id>', message => 'incident 123');
```

The query fails with `ADMINISTRATIVELY_KILLED` (`Query killed. Message: …`;
SQLSTATE `57014` on the PostgreSQL wire). Another user gets `Access Denied`
(`42501`). From a PostgreSQL client, a cancel request (`psql` Ctrl-C, JDBC
`Statement.cancel`) stops that connection's statement the same way, and
`DELETE /v1/query/<query_id>` above works for PostgreSQL queries too. If CPU
and `codelake_memory_reserved_bytes` stay up after the kill, drain and restart
the node (Kubernetes: `kubectl -n <ns> delete pod <pod>`, the other replica
keeps serving; Compose: `docker compose … restart codelake`).

**Verify:** the query shows `FAILED` with `USER_CANCELED` or
`ADMINISTRATIVELY_KILLED`;
`codelake_memory_reserved_bytes` falls; CPU drops.

**Prevent:** set `[governance] query_max_run_time` (the default for every
client), or `query_max_run_time` in the clients' session (for example `1h` in
the dbt profile's `session_properties`; `SET statement_timeout` on the
PostgreSQL wire); it fails the query with `EXCEEDED_TIME_LIMIT` instead of
letting it run.

## Out of memory

Two different failures:

**1. A query fails with `EXCEEDED_LOCAL_MEMORY_LIMIT`** ("Resources
exhausted"). The pool and the spill budget were not enough for that query; the
process and the other queries are fine. The error names the top memory
consumers.

- Confirm: `codelake_memory_reserved_bytes` near `codelake_memory_limit_bytes`
  at the time, or `codelake_spill_used_bytes` near its limit.
- Act: rerun it when fewer queries run; if it fails alone, raise
  `memory.limit` together with the container limit
  ([configuration.md](configuration.md#memory-memory)); if it is a join,
  check that `sort_merge_oversized_joins` is not turned off in the session. A
  join key repeated on more rows than the pool holds fails by design
  (docs/issues/ISS-007): rewrite the query or raise the limit.
- Verify: the query completes; the canary stays green.

**2. The container is OOM-killed** (exit 137, `OOMKilled: true`, pod
restarts). Memory outside the pool outgrew the 25% headroom.

- Confirm: `kubectl -n <ns> describe pod <pod>` (`Last State: Terminated,
  Reason: OOMKilled`) or `docker inspect --format '{{.State.OOMKilled}}' <container>`.
  Compare the container's working set with `codelake_memory_reserved_bytes`
  just before: working set far above the pool means memory outside it.
- Act, in order: lower `memory.limit` (keep ~75% of the container); lower
  `execution.batch_size` if sources return wide rows; lower
  `execution.target_partitions`; give the container more memory. Note which
  query ran (traces, `system.runtime.queries` of the other replica, client
  logs): an OOM kill with the pool below its limit is a defect to report with
  that query.
- Verify: no restart over the next heavy run; working set peak below 90% of
  the limit.
- Revert: the previous configuration file.

## Spill disk full

**Shows as:** queries fail with a resources error while memory is not
exhausted; `codelake_spill_used_bytes` at `codelake_spill_limit_bytes`; on
Kubernetes, a pod evicted for exceeding the `emptyDir` `sizeLimit` (if
`sizeLimit` is below `spill_limit`, which it must not be).

**Act:** spill files are temporary and removed when their query ends; nothing
needs cleaning by hand while the process runs. Cancel the query that is
spilling (the largest running one), then raise `memory.spill_limit` and the
volume (`emptyDir.sizeLimit`, or the host directory's file system) together,
or move the heavy runs apart. After a crash, files left in `spill_dir` belong
to no query: with the process stopped, delete them with the literal path
of `memory.spill_dir` written out, never through a variable or placeholder
that could be empty (for example `rm -rf -- /srv/codelake/spill/*` on
Compose). On Kubernetes the `emptyDir` survives a container restart and is
emptied only when the pod is deleted: `kubectl -n <ns> delete pod <pod>`.

**Verify:** `codelake_spill_used_bytes` returns to 0 after the heavy run.

## A source is down

**Shows as:** queries on one catalog fail (connection refused, timeout,
login failure); `codelake_source_up{source="<name>"} 0`; `/readyz` answers
`503` with `required sources down: <name>` and the load balancer stops sending
traffic to the pod.

Important consequences:

- `/readyz` fails for a **required** source, so every replica leaves the
  load balancer when a shared source is down: queries on the other catalogs
  stop being served too. Liveness does not look at sources, so pods are not
  restarted.
- A required source unreachable at **startup** stops the process (pods in
  `CrashLoopBackOff`, Compose container restarting).
- An optional source (`required = false`) that was down at startup is skipped
  and stays absent until the next restart.

**Act:**

1. Confirm from the node: `curl -s http://<ops>/healthz | jq '.sources[] | select(.up | not)'`
   shows the driver's error (DNS, TCP, TLS, login).
2. Fix the source or the path to it (network, firewall, credentials rotated
   without updating the Secret: update it and restart).
3. If other catalogs must keep serving during a long outage, set
   `required = false` for that source and restart: readiness then ignores it
   and its queries fail on their own. Revert once the source is back.

**Verify:** `codelake_source_up` back to 1, `/readyz` 200, canary green.

## Missing file during OPTIMIZE

**Shows as:** a read of a hive table fails with a missing or not-found file
while that table was being compacted; or a table refuses scans after a crash
with an error that names an `OPTIMIZE` marker.

- A query planned just before the file swap can fail once with a missing
  file: the error is loud, never a wrong result. **Rerun it.**
- After a process crash in the middle of a swap, the swap is finished or
  undone automatically when the catalog opens and before the next `OPTIMIZE`
  (log lines `finishing an interrupted OPTIMIZE` / `undoing an interrupted
  OPTIMIZE`).
- If recovery cannot decide (log line `an interrupted OPTIMIZE could not be
  settled; the table refuses scans until it is inspected`), the other tables
  keep working and that table stays refused. Inspect it:

  ```bash
  ls -la <warehouse>/<schema>/<table>/            # data files, hidden .*.inprogress files, the marker
  jq . <warehouse>/<schema>/<table>/.codelake-optimize.json   # files to add and to remove
  ```

  Decide with the marker: if every file it lists to add exists (hidden or
  renamed), the swap can be finished; if the old files are all there, it can
  be undone. Restore the table from [backup](backup-restore.md) when neither
  holds. Record what you did in the incident.

See [compaction.md](compaction.md) for the procedure that avoids these cases.

## Queue full

**Shows as:** clients get `QUERY_QUEUE_FULL` on the HTTP protocol or SQLSTATE
`53400` (`Too many queued queries for <node | resource group | user> (limit
N)`) on the PostgreSQL wire; `codelake_queries_rejected_total{scope}` grows;
`codelake_queries_queued` stays high.

The message names the limit reached: the node's `[governance]
max_queued_queries`, a resource group's `max_queued`, or the user's
`max_queued_per_user`. Queries wait while the node runs `max_running_queries`
(or the group / user its own maximum). On the HTTP protocol,
`[server.trino] max_active_queries` (default 100) also caps accepted and
unfinished queries, usually clients that submit and never read (canceled
after `client_timeout_secs`). A PostgreSQL client that stops reading a
portal keeps its running slot until its time limit.

**Act:** list them (query above, grouped by `"user"` and `source`); kill
abandoned or runaway ones (`kill_query` above); fix the client that leaks
queries; give a noisy workload its own `[[governance.resource_group]]`.
Raise the limits only with the memory to back them (each running query holds
memory in the pool).

**Verify:** new submissions succeed; `codelake_queries_queued` falls.

## Latency above the objective

**Shows as:** canary `status: "slow"` or `"stale"`; pipelines late.

1. Is it one query or all? Compare canary checks; look at the query in traces
   (`codelake.query` → `codelake.remote` / `sqlserver.query` / `hive.scan`):
   time in the source (pushdown missing, source busy), in the engine, or
   waiting for the client.
2. Saturation: CPU throttling of the container, `codelake_memory_reserved_bytes`
   at the limit (queries spilling), `codelake_spill_used_bytes` rising.
3. Many small files in a hive table read by the slow query: compact it
   ([compaction.md](compaction.md)).
4. A source slow on its own: its own monitoring; the engine cannot make a
   remote statement faster.
5. After an upgrade: roll back and compare
   ([upgrade-rollback.md](upgrade-rollback.md)).

**Verify:** canary latency and freshness back under their limits for a full
pipeline cycle.
