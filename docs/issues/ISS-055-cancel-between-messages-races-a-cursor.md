# ISS-055 — A cancel request between messages sometimes cancels the next FETCH

- Kind: defect · Recorded: 2026-09-27 · Status: open
- Context: `governance::declared_cursor_runs_under_governance` (step
  `cancel_request_between_messages_does_nothing`) sends a PostgreSQL cancel
  request while the connection executes nothing, waits 500 ms, and fetches
  from an open cursor. PostgreSQL ignores such a cancel. CodeLake answers the
  next `FETCH` with `57014 canceling statement due to user request` in some
  runs: 2 of 8 on `main` at 8c981c8, 1 of 5 on `front/m4-distributed`.
- Effect: a late Ctrl-C (or JDBC's `Statement.cancel` of a statement that
  already finished) can fail the client's next fetch from a cursor.
- Suspected: the cancel is recorded for the connection and consumed by the
  next statement instead of being dropped when nothing runs (a race with the
  end of the previous `FETCH`). Not investigated yet.
- References: the F6 re-verification fixed the deterministic version of this
  (test comment in `crates/codelake-server/tests/governance.rs`).
