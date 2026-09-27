# ISS-050 — `0.0` and `-0.0` are one value everywhere

- Kind: deliberate divergence · Accepted by: Fernando Farias, 2026-09-26 · Status: open
- Context: The engine compared floats by their bits (`-0.0` below `0.0`), so `x = 0` dropped
  `-0.0` rows and `x IS DISTINCT FROM 0` kept them. Trino 431 is not consistent: its
  comparisons and `IS DISTINCT FROM` treat the zeros as equal, but its joins, `IN (subquery)`
  and `IN` lists of `real` values do not match `0.0` with `-0.0`.
- Accepted: `0.0` and `-0.0` are equal everywhere — comparisons, `IS [NOT] DISTINCT FROM`,
  `JOIN`, `IN (subquery)`, `IN` lists, `GROUP BY`/`DISTINCT`, set operations — for `double`
  and `real`, as the SQL standard and PostgreSQL. Where Trino 431 differs (joins, `IN` over
  `real`) the engine follows PostgreSQL 17 (ADR 0017).
- Risk: A query joining floats on a zero returns rows Trino 431 does not; a group of the two
  zeros shows `0.0`.
- Revisit when: a consumer needs Trino's join behavior on float zeros.
- References: ADR 0017; re-verification of the char(n) round 5 (item 4 of its verdict).
