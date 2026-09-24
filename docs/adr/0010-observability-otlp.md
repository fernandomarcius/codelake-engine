# ADR 0010 — Distributed tracing: OTLP export, W3C propagation, redacted SQL

- Status: Accepted
- Date: 2026-09-23

## Context

A slow or failed query crosses the client (dbt, scripts, BI tools), the engine
and one or more sources (SQL Server, Hive files, PostgreSQL, MySQL). Logs alone
do not tell which remote statement made a dbt model slow, or which files an
`INSERT` wrote. Trino emits OpenTelemetry traces and accepts a caller's
context; clients that move to CodeLake expect the same.

Constraints:

- The core must stay light and vendor-neutral (`cargo xtask arch`).
- Traces must never carry the values a query filtered on: SQL and error
  messages quote literals (customer names, document numbers, LSNs).
- Tracing is off by default and must cost ~nothing when off.
- No gRPC/`tonic` tree and no second TLS stack for an optional feature.

## Decision

1. **Split.** `codelake_core::trace` emits plain `tracing` spans under the
   target `codelake::trace` and defines a `TraceBackend` trait (set a remote
   parent, read a trace id). The server installs the OpenTelemetry backend
   (`tracing-opentelemetry` 0.34, `opentelemetry`/`_sdk`/`-otlp` 0.33). The
   core has no OpenTelemetry dependency.
2. **Export.** OTLP/HTTP with protobuf and the blocking `reqwest` client, batch
   span processor. `[telemetry] otlp_endpoint` enables it; the value is the
   collector base URL (`/v1/traces` is appended). Only `http://` is accepted:
   export to a collector agent on the node/pod, which handles TLS upstream.
   `protocol = "grpc"` and `https://` are refused at startup rather than
   silently degraded. Only the `codelake::trace` target (plus WARN+ events
   from CodeLake crates) goes to the exporter; `RUST_LOG` keeps governing logs.
3. **Sampling.** `ParentBased(TraceIdRatioBased(sampling_ratio))`: a caller's
   `traceparent` decides for its own trace; new traces use the ratio.
4. **Propagation in.** `QueryTrace::from_headers(traceparent, tracestate,
   trace_token)` implements W3C Trace Context (invalid `traceparent` ⇒ new
   trace and `tracestate` dropped; future versions tolerated). The Trino
   protocol front passes `traceparent`/`tracestate` and `X-Trino-Trace-Token`
   (kept as `codelake.correlation_id`) through `QueryOptions::with_trace`.
   `QueryOptions::for_user` is unchanged (the new field defaults to "new trace").
5. **Span tree.** `Engine::query` returns a `Query` whose span lives until the
   results are fetched:

   ```text
   codelake.query (server)            query id, user, catalog, schema, correlation id,
   │                                  redacted SQL + hash, rows, status
   ├─ codelake.parse
   ├─ codelake.plan                   (includes scalar-subquery pre-evaluation)
   ├─ codelake.execute                physical planning; DDL/CTAS work
   │  ├─ hive.scan                    files selected: count, bytes, names (≤ 50)
   │  └─ codelake.remote (client)     one per federated scan, any SQL source: executor, rows
   │     └─ sqlserver.query (client)  redacted T-SQL + hash, server, database,
   │                                  pool wait, rows, SQL Server error number
   └─ codelake.fetch                  rows streamed to the client; `cancelled` if dropped
      └─ … (remote/scan spans started while fetching)
   hive.write                         under the CTAS/INSERT execute: file name, rows
   ```

   Tasks spawned by DataFusion inherit the current span through
   `datafusion::common::runtime::set_join_set_tracer`, so source spans stay in
   the query's tree. `Engine::sql_with` keeps its signature and traces parse and
   plan only.
6. **Redaction.** SQL is tokenized with `sqlparser` (dialect of the source) and
   every literal token — strings, numbers, hex/byte/national/dollar strings —
   becomes `?`; comments are removed; text is capped at 8 KiB. If the SQL does
   not tokenize, no text is recorded. `codelake.sql.hash` is FNV-1a 64 of the
   *redacted* text, so equal shapes group together and no low-entropy literal
   can be confirmed by hashing guesses. Error descriptions drop quoted
   segments and `0x…` values.
7. **PostgreSQL wire.** The listener still runs statements inside
   `datafusion-postgres`, so a hook can only observe. Each session gets a
   synthetic trace parent (kept in the connection metadata, sampled once with
   the ratio); every statement is a `codelake.pg.statement` span in that trace
   with the backend pid as `codelake.session_id`, user, client address and
   redacted SQL.

## Consequences

- Cost when off: the `tracing` spans the JSON logs already see, one `Span`
  enter/exit per batch in the fetch and remote streams, and a tokenizer pass
  per query for the redacted SQL on the query span. No exporter thread, no
  OpenTelemetry layer, no PostgreSQL hook.
- Log lines emitted inside a query carry `codelake.query_id` and, when the
  caller sent one, `codelake.trace_id` — even with export off.
- `hive.scan` reports the files a scan *selected*; bytes actually read after
  row-group pruning are not measured.

## Known gaps

- **PostgreSQL wire execution is not linked.** Statement spans mark arrival;
  their execution spans (remote, Hive) are separate root traces until the
  listener runs statements through `Engine::query` (ROADMAP M1 item 1
  follow-up). PostgreSQL has no header for a caller's `traceparent`.
- **SQL text for PostgreSQL/MySQL sources.** Their executors live inside
  `datafusion-table-providers`; they get the generic `codelake.remote` span
  (executor, rows, errors, duration) but not the pushed SQL.
- **TLS to the collector** is not supported (by design, see 2).
- No metrics or logs over OTLP; Prometheus `/metrics` and JSON logs stay.

## Evidence

- `crates/codelake-server/tests/e2e/otlp.rs` (integration lane): Jaeger v2 as
  the collector, SQL Server 2022 and a Hive warehouse as sources. A query sent
  with `traceparent` is exported in the caller's trace id with the query span
  as child of the caller's span; phases, `codelake.remote` → `sqlserver.query`,
  `hive.write` and `hive.scan` hang where described above with the right row
  counts; no attribute contains the filter literal; a query without
  `traceparent` under `sampling_ratio = 0` is not exported. A second test runs
  the binary with `[telemetry]` and checks that two PostgreSQL statements of
  one session share one trace.
- Unit tests: W3C parsing (including property tests), redaction per literal
  kind, span tree and error/cancel recording with an in-process capture layer
  (`engine::tests::traces`), config validation.
