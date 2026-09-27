# ADR 0017 — `0.0` and `-0.0` are one value

- Status: Accepted
- Date: 2026-09-26
- Related: ADR 0008 (Trino function library), ADR 0014 (correctness guards),
  ISS-050 (the decision)

## Context

The engine compares floats (`double`, `real`) with a total order on their
bits: `-0.0` sorts below `0.0` and is a different value. `WHERE x = 0` dropped
the `-0.0` rows, `x < 0` and `x IS DISTINCT FROM 0` kept them, `x IN (0, 1)`,
`BETWEEN 0 AND 1`, `CASE x WHEN 0` and `nullif(x, 0)` answered the same way,
`-0.0E0 IS DISTINCT FROM 0.0E0` folded to true, and a scan pruned row groups
with those comparisons (a bit order `x >= 0.0` skips a row group holding only
`-0.0`). Joins, `IN (subquery)`, `GROUP BY`, `DISTINCT` and set operations
already compared normalized keys (`-0.0` as `0.0`), but `GROUP BY ROLLUP`,
`CUBE` and `GROUPING SETS` did not.

IEEE 754, the SQL standard and PostgreSQL have `-0.0 = 0.0` everywhere.
Trino 431 is not consistent: its comparisons and `IS DISTINCT FROM` treat the
zeros as equal, but its hash joins (`JOIN … ON a.x = b.y`, `x IN (SELECT …)`)
and its `IN` list of `real` values do not match `0.0` with `-0.0`, while a
`WHERE a.x = b.y` over a cross join does, and `x = 0 AND x >= 0 AND x <=
-0.0E0` answers no row (its optimizer intersects the ranges ordering `-0.0`
below `0.0`).

## Decision

`0.0` and `-0.0` are one value everywhere (user decision, ISS-050):
comparisons, `IS [NOT] DISTINCT FROM`, `IN` lists and `IN (subquery)`, joins,
`GROUP BY` (grouping sets included), `DISTINCT` and set operations, for
`double` and `real`. Where Trino 431 disagrees (its joins and `IN` over
`real`), the engine follows PostgreSQL, deliberately.

- `FloatZeros` (logical, right before `simplify_expressions`,
  `trino_functions/zeros.rs`) rewrites a comparison of a float with a zero
  constant into one that the bit order answers as IEEE 754 does over both
  zeros: `x = 0` is `x = -0.0 OR x = 0.0`, `x >= 0` is `x >= -0.0`, `x <= -0.0`
  is `x <= 0.0`, `x < 0` is `x < -0.0`, `x > -0.0` is `x > 0.0`,
  `x IS NOT DISTINCT FROM 0` is `x IS NOT NULL AND (x = -0.0 OR x = 0.0)` and
  `x IS DISTINCT FROM 0` its negation. An `IN` list with a zero gets the other
  zero; `BETWEEN` with a zero bound, `CASE x WHEN` and `nullif` are spelled
  out as comparisons; a comparison of two constants is folded with IEEE
  semantics before the simplifier folds it with the bit order. The forms are
  plain comparisons: scans prune with them, remote sources read the same
  condition, the simplifier's negations stay exact (`NOT (x = 0)` becomes
  `x <> -0.0 AND x <> 0.0`), and `x = 0` no longer makes the planner treat
  `x` as a constant (a window partitioned by `x` would be left unsorted).
- `NanAwareComparison` (physical, `trino_functions/nan.rs`), which already
  gave comparisons IEEE NaN semantics, also answers two zeros as equal and
  covers `IS [NOT] DISTINCT FROM` (zeros one value, every NaN one value):
  comparisons of two columns and `<>` are corrected where they run. A
  comparison with a literal that is neither a zero nor NaN (`x = 1.5`) is left
  unwrapped: the bit order answers it right, and the planner keeps reading it
  as a constant.
- `TrinoKeys` (`keys.rs`) also normalizes float expressions inside
  `GROUPING SETS`, `ROLLUP` and `CUBE`.

This replaces the approach of `c4a9c35` (`front/w1-df54`), which rewrote only
`x = 0` in a filter into the IEEE function: it failed for `real`
("expected Float64 argument, got Float32"), left `IS [NOT] DISTINCT FROM`,
`>=`/`<=` pruning and `IN` lists wrong, and kept a filter on `x = 0` from
pruning at all.

## Consequences

- Joins and `IN (subquery)` over floats match `0.0` with `-0.0`, and
  `real IN (0, …)` keeps `-0.0`: other rows than Trino 431 (as PostgreSQL
  17); so does a conjunction of ranges that meet at the zeros. The integration test checks these against PostgreSQL 17 and the rest
  against Trino 431.
- A group of `0.0` and `-0.0` shows `0.0` (the normalized key), where
  Trino shows the first value it met; the rows and counts match.
- NaN keeps the semantics it had: comparisons are IEEE (every one false
  but `<>`), `IS [NOT] DISTINCT FROM` and grouping put every NaN in one
  group. The simplifier still negates an ordering comparison with the bit
  order (`NOT (x > 1)` into `x <= 1`, `x NOT BETWEEN a AND b` into
  `x < a OR x > b`), which drops NaN rows IEEE keeps; that is a NaN defect
  of its own, not of the zeros.
- A filter `x = 0` is an `OR` of two equalities; a scan prunes with it as
  before.
