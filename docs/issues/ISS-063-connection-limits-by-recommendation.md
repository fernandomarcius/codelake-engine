# ISS-063 — Connection and descriptor limits: defaults applied by recommendation

- Kind: behavior · Recorded by the availability front (`fix/fd-limit-idle-connections`), 2026-10-02 ·
  Status: awaiting-confirmation
- Context: the shadow's production-query replay (2026-10-02, `fdprobe`) left every new client hanging
  behind ~1000 idle keep-alive connections (ADR 0018). The fix needed values nobody had chosen; each was
  applied by recommendation:
  - HTTP idle timeout **200 s**: the reference engine's `http-server.net.max-idle-time` default (read
    from airlift 237's `HttpServerConfig`, the version Trino 431 ships).
  - HTTP header-read timeout **30 s** (hyper's default for its own), counted from the connection's
    opening for the first request: a client that connects and sends nothing is closed after 30 s, not
    200 s. The reference has no such distinction (Jetty's idle timeout covers both).
  - **256 reserved descriptors** for everything that is not a client connection, and **32** for the
    ops listener; the Trino listener gets the rest unless `[server.trino] max_connections` says less.
  - At the limit, an **immediate error** (HTTP `503` with `retry-after: 1`, PostgreSQL `53300`) instead
    of queueing the connection: the reference queues it in the kernel and its Jetty threads.
  - **No idle-session timeout for PostgreSQL**, as PostgreSQL's default; TCP keepalive on.
- Risk: a client that keeps an HTTP connection idle beyond 200 s must reconnect (the Trino clients and
  `requests` do); with a hard limit of 1024 the Trino listener holds 480 connections instead of ~1000
  (more are refused at once, not left hanging).
- Revisit when: a client fails on a server-closed keep-alive connection, or `reserved_files` proves too
  small (`codelake_accept_errors_total{error="descriptors"}` grows).
- References: ADR 0018; `docs/operations/configuration.md` (Connections and file descriptors).
