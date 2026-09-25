# ADR 0016 — Query governance in the core: registry, admission, time limits

- Status: Accepted
- Date: 2026-09-25

## Context

Requirement R12 (ROADMAP M1 item 6) asks for a per-query timeout, an
admission queue and per-user limits. Before this decision:

- the query registry (`system.runtime.queries`) lived in the HTTP statement
  protocol (ADR 0012) and never saw PostgreSQL wire queries;
- `query_max_run_time` was enforced on the HTTP path only; on the PostgreSQL
  path `datafusion-postgres` applied `statement_timeout` to planning only,
  not to execution or to the rows being sent;
- the only bound on concurrency was `max_active_queries` of the HTTP
  listener (accepted and unfinished queries) and `max_connections` of the
  PostgreSQL one: a node accepted as many concurrent queries as clients sent,
  all competing for one memory pool.

Governance must hold for every client protocol, without authentication (M1
item 5 is separate): it uses the user each client names.

## Decision

`codelake_core::governance` owns four things; each protocol calls them.

1. **Registry** (`QueryRegistry`, `QueryRecord`): every governed query from
   submission to its end (`QUEUED`, `PLANNING`, `RUNNING`, `FINISHING`,
   `FINISHED`, `FAILED`), with protocol, user, source, resource group,
   times, rows and failure (name, type, message, plus the protocol's own
   error as an opaque detail). Completed queries are kept for
   `history_retention_secs`, at most `history_size`, trimmed on every
   registration; SQL text is kept up to `max_recorded_query_length`
   characters. `system.runtime.queries` is registered by the engine for
   every protocol, with the columns fixed by ADR 0012; `resource_group_id`
   is `["global"]` or `["global", "<group>"]`. The HTTP protocol keeps only a
   paging index (id → buffered pages), trimmed to what the registry lists.
2. **Admission** (`Admission`, `Ticket`, `Permit`): a query runs at once when
   the node (`max_running_queries`), its resource group (`max_running`) and
   its user (`max_running_per_user`) all have a free slot; otherwise it
   waits if the node (`max_queued_queries`), the group (`max_queued`) and the
   user (`max_queued_per_user`) have a free queued place; otherwise it is
   refused with `QUERY_QUEUE_FULL`, naming the limit reached. Waiting
   queries are admitted in arrival order among those that fit (no
   head-of-line blocking by a user's or a group's own limit). A waiting
   query fails with `EXCEEDED_TIME_LIMIT` after `queue_timeout_secs`, or
   when its run time ends first; a canceled one leaves the queue. Slots are
   RAII (`Permit`, `Ticket`), every counter changes under one lock and
   per-user entries are removed at zero, so no path leaks a slot. Resource
   groups are a flat, ordered list in `[[governance.resource_group]]`
   selected by user list, user regex and source regex (whole-text matches);
   no hierarchy, weights or memory shares.
3. **Time limits** (`TimeLimits`): `query_max_run_time` counts from
   submission (queueing included), `query_max_execution_time` from
   admission; the query must end by the earlier deadline. The configured
   values are the session properties' defaults (and show in `SHOW
   SESSION`). A protocol enforces the deadline by dropping the query's
   streams: the operators stop and their reservations return to the pool.
4. **Stopping a query from outside** (`Governance::kill`,
   `Governance::cancel`): `CALL system.runtime.kill_query(query_id => '…',
   message => '…')` (or by position) fails the query with
   `ADMINISTRATIVELY_KILLED` (`Query killed. Message: …`), whatever protocol
   runs it; `DELETE /v1/query/{id}` cancels one (`USER_CANCELED`). Only the
   query's owner — the user its client named — may do either (another user
   gets `Access Denied: Cannot kill query owned by <owner>`). The reference
   engine lets anyone kill any query with its default access control
   (checked on 431); users are not authenticated yet, so the owner rule
   keeps one client from stopping another's work by mistake, and the
   authentication front (M1 item 5) decides who else may (ISS-026). The call answers
   without an admission slot, so it works on a full node. Failing the record
   cancels its token; each protocol then drops the query's streams as for a
   deadline.

Per protocol:

| | HTTP statement protocol | PostgreSQL wire |
|---|---|---|
| user / source | `X-Trino-User` / `X-Trino-Source` | startup `user` / `application_name` |
| refusal | `QUERY_QUEUE_FULL` in the `POST` response itself | SQLSTATE `53400` (`configuration_limit_exceeded`) |
| time limit | `EXCEEDED_TIME_LIMIT` | SQLSTATE `57014` (`query_canceled`) |
| session limit | `query_max_run_time`, `query_max_execution_time` | `SET statement_timeout` (as run time) |
| slot held | from admission to the end of execution (the last batch produced; ≤ 2 buffered batches, reserved, may still wait for the client) | until every row is sent or the stream is dropped |
| not governed | `USE`, `SET/RESET SESSION`, `PREPARE`, `DEALLOCATE`, `kill_query` | `SET`, `SHOW <variable>`, transaction control, `DISCARD`, `DEALLOCATE`, `kill_query` |
| client cancel | `DELETE` of the query's `nextUri` | cancel request (process id + secret key): every governed statement of the connection, queued, planning or sending rows; SQLSTATE `57014` |
| `kill_query` errors | as the reference engine 431 (checked against it): `NOT_FOUND`, `NOT_SUPPORTED` (not running), `INVALID_ARGUMENTS`, `INVALID_PROCEDURE_ARGUMENT` (`NULL` id), `TYPE_MISMATCH`; plus `PERMISSION_DENIED` (not the owner) | `42704`, `55000`, `22023`, `22023`, `42804`; `42501` |

On the PostgreSQL wire the server wraps `datafusion-postgres`'s query
handlers (`crates/codelake-server/src/pg.rs`) instead of adding a hook: a
hook cannot hold a slot across the execution that follows it, and does not
see the portal's result formats. The result stream sits in a slot that a
deadline watcher empties, so a portal suspended by a row limit (a client
that stopped reading) is stopped at its deadline too, and the reader, when
waiting, is woken with the error. The same watcher wakes on the record's
cancellation token, which a cancel request or `kill_query` fires: the
server's cancel handler maps the request's process id and secret key to the
connection's live statements (a wrong secret key cancels nothing), then lets
`datafusion-postgres` interrupt what it is planning. A hook plans
`kill_query` as an empty statement on the extended protocol, where
`DataFusion` could not plan `CALL`. The server also replaces `pgwire`'s
`Execute` handler (`crates/codelake-server/src/pg/execute.rs`): the default
collects every row of a portal executed without a row limit (JDBC,
psycopg3) before sending the first, which would end the statement — slot
freed, time limit over — while its rows wait in memory outside the pool;
the replacement sends rows as the stream yields them.

Metrics (Prometheus, `metrics` facade in the core): gauges
`codelake_queries_running`, `codelake_queries_queued`,
`codelake_resource_group_running{resource_group}`,
`codelake_resource_group_queued{resource_group}`; counters
`codelake_queries_rejected_total{scope}`,
`codelake_queries_timed_out_total{phase}`,
`codelake_queries_completed_total{protocol,state}`. Users are never a label.
Traces: the `codelake.query` span of an HTTP query carries
`codelake.resource_group` and `codelake.queued_ms`, and its
`codelake.query_id` is the id clients and `system.runtime.queries` show;
the `codelake.pg.statement` span of a governed PostgreSQL statement carries
the same `codelake.query_id`. No metric is labelled by query (unbounded
cardinality): per-query figures are in `system.runtime.queries`.

Defaults: 32 running and 1000 queued per node, 16 running and 500 queued per
user, 600 s in the queue, 100 days for both time limits (the properties'
previous defaults). `[server.trino] history_size` and
`history_retention_secs` are deprecated in favour of `[governance]`; when
set, they still apply.

## Consequences

- One registry for both protocols: a PostgreSQL client sees HTTP queries in
  `system.runtime.queries` and the other way round.
- A slow HTTP client no longer holds a running slot once execution ends; a
  slow PostgreSQL client does (its rows are produced as they are sent), up to
  its deadline.
- Admission is per node. The cluster-wide admission budget of M4 builds on
  it; so would authentication-based groups (M1 item 5).
- Not covered: memory-based admission (queries are admitted by count; the
  pool still fails or spills a query past the ceiling, ADR 0003), resource
  group hierarchy and scheduling weights, a queued-time session property,
  `pg_cancel_backend(pid)` (the cancel request and `kill_query` cover it),
  and `CALL runtime.kill_query` without the catalog.
- Pending, not decided here: **query execution shares the Tokio workers with
  the protocols.** One runtime of `target_partitions` workers runs the
  listeners (HTTP, PostgreSQL, ops) and every operator; `DataFusion`
  operators yield cooperatively, so a CPU-heavy query delays protocol
  requests (ISS-009 saw 120 s under the coverage build) but does not block
  them. A dedicated CPU runtime is not a local change: on the PostgreSQL wire
  the result stream is created and polled by `datafusion-postgres` in the
  connection's task, and operators that spawn at `execute()` (coalesce,
  repartition) spawn on the runtime of that task, so moving execution means
  forwarding rows through a channel between runtimes (outside the pool unless
  reserved, ADR 0003) and re-deciding the worker count that bounds the
  batches in flight. The HTTP protocol already runs each query in a task of
  its own and could move first. Measure protocol latency under CPU
  saturation before choosing (ROADMAP M1 item 6).
- Tests: unit tests of admission, registry, limits and `kill_query` in the
  core; the HTTP protocol in process
  (`crates/codelake-server/src/trino/tests/governance.rs`, pool read at zero
  after a timeout); both protocols with real clients (`tokio-postgres`, the
  Python client) against the binary
  (`crates/codelake-server/tests/governance.rs`: time limits, queue and
  per-user limits, cancel request, `kill_query` across protocols, `DELETE`
  of a PostgreSQL query, pool back at zero after each); the query id on the
  PostgreSQL span (`tests/e2e/otlp.rs`).
