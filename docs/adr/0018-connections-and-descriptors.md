# ADR 0018 — Connections and file descriptors

- Status: Accepted
- Date: 2026-10-02
- Related: ADR 0004 (client protocols), ADR 0012 (Trino client protocol),
  ISS-063 (defaults applied by recommendation)

## Context

The production-query replay on the shadow (2026-10-02) found the server
unavailable to every new client — `SELECT 1` and the health checks included —
while ~1000 HTTP/1.1 keep-alive connections sat idle:

- `codelake serve` ran with the container's soft `RLIMIT_NOFILE` of 1024
  (hard 524288; the reference engine runs with 524288).
- Neither HTTP listener (ops, Trino protocol) ever closed an idle keep-alive
  connection: `axum::serve` sets no timeout on the connections it hands to
  hyper.
- With the descriptors used up, `accept` failed with `EMFILE`. The kernel
  still completed the TCP handshakes, so clients waited for an answer that
  never came. axum retried each second and logged nothing (its `tracing`
  feature is off); datafusion-postgres' loop retried at once, a busy loop.
  Queries in flight kept running; everything recovered as soon as clients
  closed.

Measured with `fdprobe` (N idle keep-alive connections, each after one
answered `GET /v1/info`, then a new `SELECT 1` with a 5 s timeout): CodeLake
served 1003 connections and the 1004th timed out; the reference served 1100.
Idle connections the server closed within 75 s: CodeLake 0, reference 79 —
but those 79 are the probe's: it checks each socket with `select()`, which
raises for a descriptor at or above 1024, and counts the error as a close.
Counted with `poll()`, the reference (idle timeout 200 s) closed none within
75 s either.

## Decision

1. **Open-files limit.** At startup the server sets its soft `RLIMIT_NOFILE`
   to the hard limit, or to `server.open_files_limit` when set (never above
   the hard limit), and logs both. A refusal is logged and the server keeps
   the limit it had.
2. **A budget per listener.** `server.reserved_files` (256) descriptors are
   never given to client connections: source pools, spill and data files,
   logs. The ops listener holds at most 32 connections, PostgreSQL
   `server.max_connections` (256), and the Trino protocol listener what is
   left (or `[server.trino] max_connections`). A limit that cannot hold them
   stops startup with the arithmetic in the error. With the shadow's hard
   limit (524288) the Trino listener may hold ~523 000 connections; with a
   soft limit of 1024 that cannot be raised, 480.
3. **Fail fast at the limit.** Past its budget a listener accepts the
   connection, answers at once and closes: HTTP `503` with `retry-after: 1`
   and a text body (Trino clients retry `503`), PostgreSQL an `ErrorResponse`
   `FATAL 53300 sorry, too many clients already` (datafusion-postgres closed
   it without a word). Within the budget there is always a descriptor to
   do so.
4. **When descriptors still run out** (something outside the budget used the
   reserve): each listener keeps one spare descriptor (`/dev/null`). On
   `EMFILE`/`ENFILE` it frees it, accepts the waiting client with it, answers
   (query listeners: the refusal, `503` / `53000`; the ops listener: the
   request itself, one per connection, so probes get their real answer),
   and opens the spare again. Other `accept` errors pause 5 ms, doubling to
   1 s. Every error is counted; the log has one line per listener per 10 s
   with the count of those it suppressed.
5. **HTTP timeouts** (both HTTP listeners), in the server's own connection
   loop on hyper (`axum::serve` cannot set them):
   - idle: a connection with no request in flight is closed
     `server.http_idle_timeout_secs` (200) after its last response —
     Trino's `http-server.net.max-idle-time` default (airlift
     `HttpServerConfig.networkMaxIdleTime`, 200 s);
   - header read: a request head must arrive in full within
     `server.http_header_read_timeout_secs` (30), counted from the opening
     of the connection for the first request (a client that connects and
     sends nothing) and from its first byte for the next ones (hyper's
     default for its own header timeout). hyper's own header timer is not
     used: it also runs during the wait between requests.
   - A request in flight is never cut, however long it takes.
6. **PostgreSQL sessions: no idle-session timeout**, as PostgreSQL's default
   (`idle_session_timeout = 0`); the reference engine has no PostgreSQL
   listener to follow. BI tools keep pooled sessions open on purpose, and a
   closed session is an error in the middle of someone's work. Their number
   is bounded by `server.max_connections`, inside the budget, and TCP
   keepalive is on (as PostgreSQL sets it) so the sessions of vanished
   clients end. A client that never completes its startup is dropped after
   60 s (pgwire). The server now accepts PostgreSQL connections itself
   (pgwire's `process_socket`), with the same TLS (`server.tls`; an
   unreadable certificate stops startup instead of a plaintext fallback).
7. **Metrics**: `codelake_connections_open{listener}`,
   `codelake_connections_max{listener}`,
   `codelake_connections_refused_total{listener,reason}` (`limit`,
   `descriptors`), `codelake_connections_closed_total{listener,reason}`
   (`idle`, `header_read`), `codelake_accept_errors_total{listener,error}`
   (`descriptors`, `other`), `codelake_open_files`,
   `codelake_open_files_limit`.

## Consequences

- The defect's scenario (1100 idle keep-alive connections) is served like the
  reference: each connection costs one descriptor of a limit of 524288, and
  the idle ones are closed after 200 s. `fdprobe` locally (soft 1024, hard
  524288, as on the shadow): before, 1012 served, the 1013th and every
  `SELECT 1` timed out after 5 s, 0 closed; after, 1100 served, `SELECT 1`
  in 4 ms while they are held, 0 closed after 75 s and 1100 after 205 s.
- A host whose hard limit stays at 1024 serves fewer Trino connections than
  before (480 instead of ~1000), but the 481st gets a `503` at once instead of
  a timeout, and probes keep their own share.
- An HTTP client that keeps a connection idle for more than 200 s finds it
  closed, as with the reference; clients reconnect (a pooled connection the
  server closed is retried by the Trino clients and `requests`).
- The distributed worker's execution listener (gRPC, feature `distributed`)
  is outside this ADR; the raised limit applies to it, its connections are
  the coordinators'.
