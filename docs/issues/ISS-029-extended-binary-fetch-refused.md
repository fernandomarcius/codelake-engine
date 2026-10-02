# ISS-029 — FETCH in binary format on the PostgreSQL extended protocol is refused

- Kind: limitation · Applied: 2026-09-26 (F6 close) · Accepted by: Fernando Farias, 2026-10-02 · Status: closed
- Context: declared cursors encode rows as text at `DECLARE`; a `FETCH` whose portal asks for binary results would hand text to a client decoding binary (wrong values).
- Applied: such a `FETCH` fails with SQLSTATE `0A000` before any row is consumed; text `FETCH` works on both protocols. PostgreSQL itself supports binary `FETCH`.
- Risk: drivers that request binary results for cursors (e.g. some `tokio-postgres`/JDBC paths) must use text or plain queries.
- Revisit when: the cursor keeps Arrow batches and encodes at `FETCH` time.
- References: ADR 0016; commit ddf9ef7.
