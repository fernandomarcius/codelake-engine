# ISS-054 — SQL Server `datetime` compared at the value the engine shows

- Kind: deliberate divergence · Accepted by: Fernando Farias, 2026-09-27 · Status: open
- Context: SQL Server `datetime` stores time in 1/300 s steps, so a value written as
  `17:37:54.173` is stored as `17:37:54.17333…`. Both engines read the column as
  `timestamp(3)` and show `17:37:54.173`. Trino 431 then compares a filter on the column
  with the stored value (its SQL Server connector pushes the predicate down): on the shadow
  run of 2026-09-27 its `WHERE tran_end_time = TIMESTAMP '2026-09-27 17:37:54.173'` and
  `<=` the same literal returned no row, although the row shows exactly that value. CodeLake
  compares with the value it shows, with or without pushdown, and returns the row.
- Accepted: CodeLake keeps comparing at the value it shows. A row always matches its own
  displayed value, and the answer does not depend on whether the filter was pushed down.
  Not inherited from Trino 431: its behavior loses rows at the boundary and protects no data.
- Risk: a filter whose literal is the displayed value of a SQL Server `datetime` that is
  not exact in 1/300 s steps returns rows Trino 431 does not (`=`, `<=`, `>=`), or does not
  return rows it does (`<`, `>`). Only milliseconds that are a multiple of 10 are exact
  (`.680` is 204 steps; `.173` is not), and those compare the same in both engines.
- Shadow run: an incremental cutoff taken as `MAX(tran_end_time)` lands on such a value
  and made one CDC mapping table differ by the boundary row. The cutoff is now truncated
  to the second (`date_trunc('second', …)`), which `datetime` represents exactly
  (docs/shadow-run.md).
- Revisit when: a user needs Trino 431's result for such a filter.
- References: shadow run `k3s-f8` cycle 1 (2026-09-27 17:45), ISS-042 (timestamps of different precisions).
