# ISS-061 — A `TIME` literal's precision is lost through expressions

- Kind: known defect · Recorded by the protocol front (`fix/protocol-driver-compat`), 2026-10-01 · Status: open
- Context: Trino 431 types `TIME 'HH:MM:SS.f…'` as `time(p)`, `p` its fraction digits, and keeps
  `time(p)` through `VALUES`, `CASE`, `UNION` and functions. CodeLake now gives a literal its `time(p)`
  (it was always `time(9)`), declared on a nanosecond Arrow time: `DataFusion` has no common type for
  two time units, so literals of other units would no longer meet each other or a column (`VALUES`
  refuses "inconsistent data type", comparisons "cannot infer common argument type"). An expression
  over the literal does not carry the declaration and is `time(9)` again, nine digits included:
  `SELECT t FROM (VALUES TIME '12:00:00.250') v(t)` answers `12:00:00.250000000`, `time(9)`
  (reference: `12:00:00.250`, `time(3)`); `CAST(x AS time)` is `time(9)` too (reference: `time(3)`).
- Risk: the .NET driver (`NH.Trino.Data.ADO`) reads only `time(3)` (it fails on other precisions on
  the reference engine too); a `time(9)` value fails to parse there. Values are equal.
- Fix: a time unit per precision with the engine's own coercion between time units (planning and
  `VALUES`), or the declaration carried through the expressions that keep their input's type.
- Partly fixed 2026-10-01 (`fix/union-timestamp-precision`, ISS-032 item 8): `VALUES`, `UNION`,
  `INTERSECT`/`EXCEPT`, `CASE` and `coalesce`/`greatest`/`least` declare the highest `time(p)` of their
  values (`VALUES TIME '12:00:00.250'` is `time(3)`, `CT8` is `OK`). Still `time(9)`: functions and other
  expressions over a time, `CAST(x AS time)`, and `typeof`/`CAST(x AS varchar)` of a time.
- Revisit when: a client reads times computed by an expression.
- References: differential case `client_protocol` CT8.
