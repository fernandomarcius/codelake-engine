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
| slot held | from admission to the end of execution (the last batch produced; ≤ 2 buffered batches, reserved, may still wait for the client) | until every row is sent or the stream is dropped; one per connection: while a cursor or suspended portal holds it, the connection's other statements run in it |
| not governed | `USE`, `SET/RESET SESSION`, `PREPARE`, `DEALLOCATE`, `kill_query` | `SET`, `SHOW <variable>`, transaction control, `DISCARD`, `DEALLOCATE`, `kill_query`; `FETCH` and `CLOSE` (governed as their cursor's `DECLARE`, which ends with its transaction unless `WITH HOLD`) |
| client cancel | `DELETE` of the query's `nextUri` | cancel request (process id + secret key), while the connection executes a message: the governed statements that message runs, queued, planning or sending rows — those it admits and those whose rows it reads (the cursor a `FETCH` reads, the portal an `Execute` resumes); the connection's other cursors and suspended portals read on, as in PostgreSQL; between messages it does nothing, as in PostgreSQL; SQLSTATE `57014` |
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
statements the message the connection executes runs — the ones it admits and
the ones whose rows it reads (a governed stream joins the message that reads
it) — so a cancel stops the statement being executed, never a `WITH HOLD`
cursor or a suspended portal the message does not read (amended after the
third F6 verification, ISS-032: the handler failed every live statement of
the connection, which PostgreSQL's aborted transaction does not do to a held
cursor). A wrong secret key cancels nothing, and so does a request that
arrives while the connection executes no message; the handler then lets
`datafusion-postgres` interrupt what it is planning. A hook plans
`kill_query` as an empty statement on the extended protocol, where
`DataFusion` could not plan `CALL`. The server also replaces `pgwire`'s
`Execute` handler (`crates/codelake-server/src/pg/execute.rs`): the default
collects every row of a portal executed without a row limit (JDBC,
psycopg3) before sending the first, which would end the statement — slot
freed, time limit over — while its rows wait in memory outside the pool;
the replacement sends rows as the stream yields them.

A connection holds one admission slot at most (amended after the second F6
re-verification, ISS-031). While a statement of the connection holds one —
an open cursor, a portal suspended by a row limit — the connection's next
governed statements are registered, each with its own deadline, and run in
that slot at once (`Governance::admit_alongside`, in the slot's resource
group) instead of queueing behind it; the slot is given back when the last
of them ends. Before, a `SELECT`, a second `DECLARE` or a query beside a
JDBC fetch-size portal waited behind the connection's own cursor, which with
a per-user limit of one lasted the whole queue timeout and ended in `57014`.
The connection runs one statement at a time, so the slot still bounds what
it executes at once, and memory stays bounded by the pool; the per-user and
per-group limits count connections with work, not their cursors.

A cursor (`DECLARE … CURSOR`, `crates/codelake-server/src/pg/cursor.rs`)
is its `DECLARE`'s statement: the server's cursor hook plans the cursor's
query as a query of the session (operands checked, ADR 0014; rows encoded
with the session's format options) and keeps its stream in a portal of the
connection, under the name PostgreSQL reads (folded to lower case unless
quoted: `DECLARE Big` declares `big`; `datafusion-postgres` kept the name as
written), and the server governs that stream as it does a query's, so the `DECLARE` stays `RUNNING`, holding its slot, until
the cursor is read to the end, closed, stopped (cancel request,
`kill_query`) or past its deadline — its time limit counts from the
`DECLARE`, where PostgreSQL applies `statement_timeout` to each `FETCH`.
`FETCH` and `CLOSE` take no slot: a `FETCH` queued behind its own cursor's
slot would never run. `FETCH` is answered by the server, sending rows as the
cursor yields them (`datafusion-postgres` collects the rows a `FETCH` asks
for, all that are left for `FETCH ALL`, before sending the first). A `FETCH`
among other statements of a simple-protocol message must read its rows
before the next statement runs: they are held in the memory pool until
sent, and rows beyond what the pool gives fail the `FETCH` with `53200`
(`out_of_memory`), fail its cursor's statement
(`EXCEEDED_LOCAL_MEMORY_LIMIT`, slot and memory given back) and leave the
cursor unreadable (`55000`), never held outside the pool (before, `FETCH
ALL FROM c; CLOSE c` over 6 M rows took the server from 114 MB to 815 MB of
RSS with a 256 MiB pool). So does a `FETCH` executed as a protocol portal
with a row limit (JDBC's fetch size), which reads all its rows at its first
`Execute`, as PostgreSQL does, and sends them as the client asks, tagged
with every row it read (`FETCH 4`); read as they were sent, a `FETCH` run
while the portal was suspended took rows that were the portal's (a wrong
result found by the third F6 verification, ISS-032). A message whose `FETCH`
or `MOVE` the SQL parser does not read runs statement by statement, each
read on its own (a statement neither reads fails the message before any of
it runs). On the
extended protocol (psycopg3's server-side cursors) a `FETCH` is described
with its cursor's columns, as PostgreSQL does (`datafusion-postgres`
describes it with none: the client failed on the first row, after the
server had consumed the rows it asked for). A cursor's rows are encoded as
text when it is declared, so a `FETCH` bound for binary results
(`tokio-postgres`' `query`) is refused with SQLSTATE `0A000` before it reads
a row, where PostgreSQL would send them in binary (ISS-029), and so is a
`BINARY` cursor. A statement that fails after declaring a cursor drops it.
On the extended protocol a `DECLARE` of one cursor is planned as its query,
so its parameters (`$1`, psycopg3's server-side cursors) are typed and
bound, and described with no columns, as PostgreSQL does. Cursor names are
cut to 63 bytes, as PostgreSQL cuts identifiers.

A cursor lives in its transaction, as in PostgreSQL (amended after the F6
re-verification, ISS-030). The end of a transaction — `COMMIT`, `ROLLBACK`,
the end of a simple-protocol message outside a transaction block, an
extended-protocol `Sync` outside one (a rollback when a message before it
failed) — closes the connection's cursors and portals, which ends their
statements and frees their slots and their memory; a `FETCH` afterwards
fails with `34000`. Only a `WITH HOLD` cursor outlives the transaction that
commits it (a rollback closes one it declared), holding its slot until it
is read to the end, closed or stopped. A `DECLARE` without `WITH HOLD`
outside a transaction block fails with `25P01` (a message of several
statements is an implicit block, whose cursor closes with the message).
Before, a cursor survived its transaction and a pooled connection kept its
slot and memory for as long as it lived. `ABORT`, which the SQL parser does
not read, is read as `ROLLBACK` on both protocols. `DISCARD ALL` closes
every cursor and portal, held ones too, and resets `statement_timeout`, as a
connection pool expects when it resets a connection; inside a transaction
block (explicit, or a message of several statements) it fails with
`25001`, as in PostgreSQL, and so it does in an extended-protocol pipeline
after an `Execute` that completed (PostgreSQL 17's pipelining rule).
Savepoints hold what the engine's transactions hold (ISS-032): `ROLLBACK TO
SAVEPOINT` closes the cursors declared since the savepoint, keeps the others
where they stand and ends the failed state, `RELEASE` keeps them, `AND
CHAIN` begins the next transaction at once, with PostgreSQL's errors
(`25P01` outside a block, `3B001` for a missing savepoint). The engine's
writes are not transactional: a `ROLLBACK TO SAVEPOINT` after a statement
that may write since the savepoint fails with `0A000` rather than leave the
write as if undone. `SHOW` answers the tag `SHOW` and `START TRANSACTION`
its own, as PostgreSQL. `DISCARD PLANS`, `SEQUENCES` and `TEMP` have
nothing to discard. A hook ahead of
`datafusion-postgres`'s cursor and transaction hooks
(`crates/codelake-server/src/pg/cursor.rs`) tracks the transaction block and
the held cursors while a message's statements run, so a `COMMIT` among them
closes the cursors before the next statement.

`FETCH` follows PostgreSQL's reading of a cursor that only scans forward:
`FETCH n` returns up to n rows and leaves the cursor on the last one it
returned (after the end when fewer were left); `FETCH 0`, `FETCH FORWARD 0`
and `FETCH RELATIVE 0` return that row again without moving (none before the
first row or after the end; `55000` for a `NO SCROLL` cursor on a row, as
PostgreSQL); a move backward (`FETCH -1`, `PRIOR`, `BACKWARD n`) fails with
`55000` ("cursor can only scan forward", PostgreSQL's answer for a `NO
SCROLL` cursor); `ABSOLUTE`, `RELATIVE n`, `FIRST` and `LAST` fail with
`0A000`; a count that is not a whole number, or beyond PostgreSQL's `int4`
(±2147483647), fails with `42601`, never "every row" (`datafusion-postgres`
read `0` and unreadable counts as `ALL`). `MOVE` moves the cursor as the
`FETCH` of the same direction would, dropping the rows it reads at once,
and answers `MOVE n` as PostgreSQL does (`MOVE 0` answers 1 on a row, 0
elsewhere, with no error on a `NO SCROLL` cursor). A `FETCH` or `MOVE` the
SQL parser does not read (a signed count, no count, no `FROM`, every
`MOVE`) is read by the server with PostgreSQL's grammar on both protocols —
on the extended one its parser falls back to that reading, keeping the
statement's text, which tells a `MOVE` from a `FETCH` — so it is answered
at once (it failed at `Parse` with `XX000`). Its tag is `FETCH n` on both
protocols. In a failed transaction block only `COMMIT` (a rollback)
and `ROLLBACK` run; anything else fails with `25P02` before it reads a
cursor or takes a slot (`datafusion-postgres` answered `25P01`, and a
`FETCH` after a canceled one returned no row and no error). A cursor whose
stream failed cannot be read again (`55000`, PostgreSQL's failed portal). A
`DECLARE` of an open name fails with `42P03` and `CLOSE` of a missing cursor
with `34000`, as in PostgreSQL; a `DECLARE` sent alone is checked before it
takes a slot. The integration tests run the same statements against
PostgreSQL 17 and the engine and check both against the rows written down.

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
  of a PostgreSQL query, pool back at zero after each; cursors across
  transactions, slot and pool freed at their end; statements beside an
  open cursor or suspended portal under a per-user limit of one; a `FETCH`
  among other statements held in the pool; the cancel request on the
  extended protocol, sparing the cursors the message does not read; cursors,
  savepoints and suspended `FETCH` portals differentially against
  PostgreSQL 17, message by message on both protocols); the
  query id on the PostgreSQL span (`tests/e2e/otlp.rs`).
