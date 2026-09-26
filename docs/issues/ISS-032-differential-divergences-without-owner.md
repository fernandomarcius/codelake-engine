# ISS-032 — Divergences the first differential run found with no front fixing them

- Kind: known defect · Found: 2026-09-26 (first run of `gates.sh differential` on main
  f7349d1) · Status: open
- Context: the differential gate's first cases (the queries that caught defects in the W1,
  null-safe join and F6 verifications) were run against main and against the binaries of
  `front/w1-df54` (4a9ad90), `front/nullsafe-join` (403e89e) and `front/f6-fixes` (d06b278).
  A case that is right in one of those fronts is marked `known = "front/<branch>"`; the cases
  below are wrong in main **and** in all three, so no front owns them. Each is marked
  `known = "ISS-032"` in `tools/differential/cases/` and listed by every run of the gate.
- Found:
  1. **Wrong results with `-0.0`** (`float_zero.toml`, `pg_wire.toml`): over Parquet files
     Trino wrote, `-0.0` is not equal to `0` in `=`, `>=`, `<=`, `IN`, `BETWEEN`,
     `IS [NOT] DISTINCT FROM`, `nullif`, `CASE x WHEN 0`, `ROLLUP`/`GROUPING SETS`; also for
     `real` and in memory (`-0.0E0 IS DISTINCT FROM 0.0E0` is true; joins on `x = y` with
     `-0.0` on one side miss rows). `front/w1-df54` (c4a9c35) fixes part of them; these remain
     wrong there. On the PostgreSQL wire `-0.0` is printed `0` (PostgreSQL prints `-0`).
  2. **SQLSTATE `XX000` for every planning and execution error on the PostgreSQL wire**
     (`cursors.toml`, `sqlstate-*`): undefined table (`42P01`), division by zero (`22012`),
     invalid text for a number (`22P02`), syntax error (`42601`), undefined function
     (`42883`). Clients that branch on the SQLSTATE cannot.
  3. **`SAVEPOINT` and `RELEASE SAVEPOINT`** fail with `XX000`
     (`cursors.toml` `savepoints`); **`COMMIT AND CHAIN`** does not open the chained block
     (`commit-and-chain`).
  4. **char(n) beyond comparisons** (`char.toml`, `V*`): internal errors where Trino answers
     (`Utf8View`/`Utf8` mismatches, `make_array` assertion, `MIN/MAX` of char in memory,
     `ORDER BY ch … LIMIT`), `typeof` of char expressions, and functions over char values
     that Trino refuses with `FUNCTION_NOT_FOUND`/`TYPE_MISMATCH` answered with unpadded values
     (`lpad`, `rpad`, `reverse`, `split`, `strpos`, `to_utf8`, `CAST(ch AS json|double|bigint)`,
     `concat` of char values). ISS-024 is the char(n) work; these shapes are not in its fixes.
  5. **SQL the char probes reached that is not about char**: `x = ANY (VALUES …)` is a syntax
     error; `EXISTS` in a projection and a correlated scalar subquery in `WHERE` are
     `NOT_SUPPORTED`; `lag`/`lead` with a column as default fail; `format()` is missing;
     `JOIN … USING (k)` over subqueries fails with an ambiguous or duplicate name; a column
     neither grouped nor aggregated fails with `GENERIC_USER_ERROR` instead of
     `EXPRESSION_NOT_AGGREGATE`.
  6. **Precedence of `IS [NOT] DISTINCT FROM` against `=`**: `true IS DISTINCT FROM b = false`
     and `x = 0 IS NOT DISTINCT FROM true` answer rows where Trino fails with a syntax error.
- Risk: items 1 and 4 are wrong or unpadded rows; the others are refusals or wrong error
  codes.
- Revisit when: a front takes an item: it fixes it, drops `known = "ISS-032"` from the cases it
  turns `OK` (the gate fails until it does) and updates this list.
- References: `tools/differential/cases/`; docs/differential.md; ISS-024, ISS-025.
