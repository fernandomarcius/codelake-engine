# ADR 0012 — Trino client protocol

Status: Accepted (2026-09-23); amended 2026-09-24 after the acceptance and
red-team comparison against a real Trino 431 (DDL results, types, errors,
prepared statements, limits, cancellation).

## Context

Trino clients — the JDBC driver, the CLI, the Python `trino` package (used by
dbt-trino) and plain HTTP scripts — talk to Trino's REST protocol
(`POST /v1/statement`, then `GET nextUri` until it disappears). Replacing Trino
without touching those clients requires CodeLake to speak that protocol. ADR
0004 lists the client protocols; this ADR fixes how the Trino one behaves.

The contract is Trino 431's observed behavior, not its documentation:
`crates/codelake-server/tests/f3_acceptance.rs` runs every expectation against a
real `trinodb/trino:431` as well (`trino_431_reference_confirms_every_check`).

## Decision

A separate listener (`[server.trino]`, off by default) serves:

| endpoint | behavior |
|---|---|
| `POST /v1/statement` | registers the query, starts it in its own task, answers at once (`QUEUED`, `nextUri` token 1); no `X-Trino-User` → `401`; malformed headers (schema without catalog, bad `X-Trino-Session` / `X-Trino-Prepared-Statement`) → `400` |
| `GET /v1/statement/executing/{id}/{slug}/{token}` | next page (~1 MiB of JSON, waits ≤ 1 s for rows); the current token repeated returns the same bytes, an older token → `410`, a later one → `404` |
| `DELETE …/{token}` | cancels (`USER_CANCELED`) |
| `DELETE /v1/query/{id}` | cancels by id — only when `X-Trino-User` is the query's owner (Trino's default allows anyone) |
| `GET /v1/info` | version `431` |

- **Execution moves ahead of the client by at most two record batches** (bounded
  channel), like Trino's output buffer: memory stays bounded whatever the
  client's pace, and DML finishes without waiting for the client. The buffered
  batches are reserved in the engine's memory pool, and a query that fails
  (canceled, abandoned, timed out) drops them at once — only its record stays
  in the history. The rows of the page being built and the body kept for a
  retry of the current token are reserved too (ADR 0003). A failed query
  keeps that body only while requests for it are in flight, so they all get
  the same bytes; a finished one does not keep its final page. A later retry
  of those tokens is `410`.
- **Cancellation stops the work.** Every operator of the physical plan is
  wrapped in `DataFusion`'s `CooperativeExec`, so it yields to the scheduler
  after a budget of batches. Without it an operator that emits many batches per
  input batch (a cross join) never yields, and a canceled query kept a worker
  at 100% CPU until the process restarted.
- **Results** follow Trino's encoding: type names with Trino's spacing
  (`decimal(10, 2)`) and `typeSignature`; `varbinary` in base64; decimals as
  strings; `NaN`/`Infinity` as strings; `real` as Java's `Float` prints it
  (`0.1`); `NULL` of type `unknown` as JSON `null`; `array`, `map` (a JSON
  object) and `row` (a JSON array, `NAMED_TYPE` fields); `interval day to
  second` / `year to month` in Trino's text; `timestamp with time zone` as the
  local time in its own zone (`… America/Sao_Paulo`); `UInt64` as
  `decimal(20, 0)`; negative-scale decimals as integral decimals. A client
  without the `PARAMETRIC_DATETIME` capability gets the legacy `timestamp` /
  `time` types in milliseconds. A value Trino cannot represent faithfully fails
  with `NOT_SUPPORTED`; nothing travels as Arrow's display text.
- **Column names**: an unnamed expression is `_col<position>`, as in Trino
  (the statement is planned with a generated alias, renamed on output).
- **Statement results**: `INSERT`, `CREATE TABLE AS`, `DELETE`, `UPDATE` and
  `MERGE` answer one `rows` bigint column plus `updateCount` (the in-memory
  catalog counts the rows a `CREATE TABLE AS` writes); other DDL answers
  **only `updateType` with an empty column list** — no `result` column, no
  rows — as Trino 431 does (the JDBC driver requires the empty list).
- **Session** is client-side, as in Trino: `X-Trino-User/Catalog/Schema/Source/Session`
  in; `USE`, `SET SESSION`, `RESET SESSION` answer `X-Trino-Set-Catalog/Schema`,
  `X-Trino-Set-Session`, `X-Trino-Clear-Session`. `USE` of a catalog or schema
  that does not exist fails (`NOT_FOUND`). `SHOW SESSION [LIKE …]` lists the
  properties, durations in airlift's text (`100.00d`). `DESCRIBE <table>`
  answers Trino's `Column, Type, Extra, Comment`.
- **Prepared statements** travel in headers, as in Trino: `PREPARE name FROM
  sql` answers `X-Trino-Added-Prepare`, the client sends them back in
  `X-Trino-Prepared-Statement`, `DEALLOCATE PREPARE` answers
  `X-Trino-Deallocated-Prepare`. `EXECUTE name USING …` and `EXECUTE IMMEDIATE
  'sql' USING …` bind parameters by replacing each `?` in the parsed statement
  with the parsed parameter expression — never by concatenating text.
- **Session properties** come from the engine's registry
  (`codelake_core::PropertyRegistry`): system properties plus catalog
  properties declared by each connector. Undeclared names fail with
  `INVALID_SESSION_PROPERTY`, as in Trino. Trino tuning knobs that only change
  performance are accepted and answered with a warning. `query_max_run_time`
  and `query_max_execution_time` are enforced (`EXCEEDED_TIME_LIMIT`);
  `sort_merge_oversized_joins` (CodeLake's own) turns off the sort-merge plan
  of hash joins over the pool (ADR 0003).
- **Errors** carry Trino's `errorCode/errorName/errorType`, because clients
  branch on them: structural mappings (syntax, missing column, ambiguous name,
  cast out of range) plus a message rule table (`errors.rs`, `MESSAGE_RULES`)
  for engine and connector messages (`TABLE_ALREADY_EXISTS`,
  `SCHEMA_ALREADY_EXISTS`, `FUNCTION_NOT_FOUND`, `TYPE_MISMATCH`, …). A missing
  table is reported as `CATALOG_NOT_FOUND`, `SCHEMA_NOT_FOUND` or
  `TABLE_NOT_FOUND` after checking which part is missing. `errorLocation` is
  given for syntax errors (including at the end of the input) and for semantic
  errors the planner locates.
- **`system.runtime.queries`** exposes the registry with Trino 431's columns.
  A client that stops polling for `client_timeout_secs` (default 300, Trino's
  `query.client.timeout`) abandons its query, which is canceled
  (`ABANDONED_QUERY`). Completed queries are kept for `history_retention_secs`,
  at most `history_size`, enforced on every submission. Accepted-and-unfinished
  queries are capped by `max_active_queries` (`QUERY_QUEUE_FULL`), and SQL text
  by `max_query_length` characters (default 1,000,000, Trino's
  `query.max-length`; `QUERY_TEXT_TOO_LARGE`): the registry holds at most
  `max_active_queries + history_size` texts of bounded size.
- **`system.metadata` and `system.jdbc`** have Trino 431's tables and columns
  (`crates/codelake-server/src/trino/system_tables.rs`), filled at scan time
  from the catalogs registered in the session: dbt-trino lists relations
  through `system.metadata.materialized_views`, the JDBC driver answers
  `DatabaseMetaData` (`getCatalogs`, `getTables`, `getColumns`, …) from
  `system.jdbc.*`. What CodeLake does not have — materialized views,
  procedures, user-defined types, comments, `WITH` properties — is listed
  empty, as Trino lists it for a connector without the feature.
  `SELECT version()` answers `431`, the version on `/v1/info`, on this
  listener only (the PostgreSQL listener keeps DataFusion's).
- **DDL Trino clients issue** beyond what `sqlparser` accepts:
  `ALTER TABLE … RENAME TO` and `ALTER VIEW … RENAME TO` (dbt's table
  materialization swaps names this way; the target resolves against the
  session like in Trino, and must be in the same catalog), and
  `CREATE VIEW … SECURITY DEFINER | INVOKER`. CodeLake has no access control
  yet, so both security modes read with the querying user's rights; the mode
  is stored with the view where the catalog persists views (Hive:
  `runAsInvoker`, no owner for `INVOKER`, as Trino writes it), so Trino,
  reading the same metastore, applies it.
- The listener has **no client authentication yet**: like the PostgreSQL one, it
  must bind to loopback unless `allow_unauthenticated_network = true`.

## Consequences

- Proven end to end with the real Trino 431 CLI, JDBC driver and Python
  `trino` client (`crates/codelake-server/tests/trino_clients.rs`,
  `f3_acceptance.rs`), and protocol details in
  `crates/codelake-server/src/trino/tests.rs`.
- Queries through the PostgreSQL wire do not appear in `system.runtime.queries`
  yet; query governance (ROADMAP M1 item 6) moves the registry into the core.
- The engine has no `char(n)`, bounded `varchar(n)`, `json` or `uuid` types:
  such columns travel as `varchar`, and `JSON '…'` / `UUID '…'` literals fail
  with `NOT_SUPPORTED`, until the engine carries those types.
- Execution shares the Tokio runtime with the HTTP handlers; cooperative
  operators keep it responsive, a dedicated CPU runtime is not needed yet.
- Open: TLS on this listener, authentication (M1 item 5),
  `X-Trino-Trace-Token`/`traceparent` (OTLP work), spooled/segment result
  encoding of newer Trino versions.
