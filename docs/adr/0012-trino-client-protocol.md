# ADR 0012 — Trino client protocol

Status: Accepted (2026-09-23)

## Context

Trino clients — the JDBC driver, the CLI, the Python `trino` package (used by
dbt-trino) and plain HTTP scripts — talk to Trino's REST protocol
(`POST /v1/statement`, then `GET nextUri` until it disappears). Replacing Trino
without touching those clients requires CodeLake to speak that protocol. ADR
0004 lists the client protocols; this ADR fixes how the Trino one behaves.

## Decision

A separate listener (`[server.trino]`, off by default) serves:

| endpoint | behavior |
|---|---|
| `POST /v1/statement` | registers the query, starts it in its own task, answers at once (`QUEUED`, `nextUri` token 1) |
| `GET /v1/statement/executing/{id}/{slug}/{token}` | next page (~1 MiB of JSON, waits ≤ 1 s for rows); the current token repeated returns the same bytes, any other token → `410` |
| `DELETE …/{token}` | cancels (`USER_CANCELED`) |
| `DELETE /v1/query/{id}` | cancels by id — only when `X-Trino-User` is the query's owner (Trino's default allows anyone) |
| `GET /v1/info` | version `431` |

- **Execution moves ahead of the client by at most two record batches** (bounded
  channel), like Trino's output buffer: memory stays bounded whatever the
  client's pace, and DML finishes without waiting for the client.
- **Results** follow Trino's encoding: type names and `typeSignature`
  (`decimal(p,s)`, `timestamp(p)`, `varbinary` in base64, decimals as strings,
  `NaN`/`Infinity` as strings). `INSERT` and `CREATE TABLE AS` answer one `rows`
  bigint column plus `updateCount`; other DDL answers `result = true`, with
  Trino's `updateType`.
- **Session** is client-side, as in Trino: `X-Trino-User/Catalog/Schema/Source/Session`
  in; `USE`, `SET SESSION`, `RESET SESSION` answer `X-Trino-Set-Catalog/Schema`,
  `X-Trino-Set-Session`, `X-Trino-Clear-Session`. `SHOW SESSION` lists every
  property.
- **Session properties** come from the engine's registry
  (`codelake_core::PropertyRegistry`): system properties plus catalog
  properties declared by each connector. Undeclared names fail with
  `INVALID_SESSION_PROPERTY`, as in Trino. Trino tuning knobs that only change
  performance are accepted and answered with a warning. `query_max_run_time`
  and `query_max_execution_time` are enforced (`EXCEEDED_TIME_LIMIT`).
- **Errors** carry Trino's `errorCode/errorName/errorType` (`SYNTAX_ERROR` with
  location, `TABLE_NOT_FOUND`, `DIVISION_BY_ZERO`, `EXCEEDED_LOCAL_MEMORY_LIMIT`,
  `QUERY_QUEUE_FULL`, …), because clients branch on them.
- **`system.runtime.queries`** exposes the registry with Trino 431's columns.
  A client that stops polling for `client_timeout_secs` (default 300, Trino's
  `query.client.timeout`) abandons its query, which is canceled
  (`ABANDONED_QUERY`). Completed queries are kept for `history_retention_secs`,
  at most `history_size`. Accepted-and-unfinished queries are capped by
  `max_active_queries` (`QUERY_QUEUE_FULL`).
- The listener has **no client authentication yet**: like the PostgreSQL one, it
  must bind to loopback unless `allow_unauthenticated_network = true`.

## Consequences

- Proven end to end with the real Trino 431 CLI and the Python `trino` client
  (`crates/codelake-server/tests/trino_clients.rs`), and protocol details in
  `crates/codelake-server/src/trino/tests.rs`.
- Queries through the PostgreSQL wire do not appear in `system.runtime.queries`
  yet; query governance (ROADMAP M1 item 6) moves the registry into the core.
- Open: TLS on this listener, authentication (M1 item 5), prepared statements
  (`X-Trino-Prepared-Statement`, `EXECUTE IMMEDIATE`) used by the Python client
  when parameters are bound, `X-Trino-Trace-Token`/`traceparent` (OTLP work),
  spooled/segment result encoding of newer Trino versions.
