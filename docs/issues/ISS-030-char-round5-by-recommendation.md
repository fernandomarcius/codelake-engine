# ISS-030 — char(n) round 5: choices applied by recommendation

- Kind: behavior · Accepted by: recommendation (implementer, round 5 of front/w1-df54), 2026-09-26 · Status: awaiting-confirmation
- Context: The fifth verification of the char(n) semantics (HEAD e20fe57) found wrong rows in rows and arrays not written out, in arrays of char values of different lengths, in trims, in `INTERSECT ALL`/`EXCEPT ALL` across hive and in-memory columns and in `varchar(n)` on the PostgreSQL wire. Fixing them took decisions beyond char(n).
- Applied:
  1. Equality of rows and arrays not written out (`=`, `<>`, `IS [NOT] DISTINCT FROM`, `IN` lists, join conditions) runs as `codelake_nested_test`, value by value, for every query, char or not: `ROW(1, NULL) = ROW(1, NULL)` is now `NULL` (was true), and a join on row or array keys no longer matches `NULL` with `NULL`. The cost: such a join is planned as a nested-loop join (no hash join on row/array keys).
  2. Floats compare `-0.0` equal to `0.0` in filters (`WHERE x = 0` keeps `-0.0`; `x < 0` drops it), as Trino 431; `x = 0` in a filter is planned as the IEEE function, so the planner no longer takes `x` for a constant there.
  3. On the PostgreSQL wire, casts to `varchar(n)` are rewritten as the engine does and a `CREATE TABLE` records its `varchar(n)` columns (checked on `INSERT` on both protocols); `char(n)` stays refused there (ADR 0015).
  4. `INSERT … SELECT` whose select list repeats a name has its unnamed items named `_col<i>` before planning.
  5. Where Trino 431 fails comparing stored rows or arrays holding a `NULL` ("ROW comparison not supported for fields with null elements", also in joins), the engine answers with the value-by-value semantics Trino applies to rows it builds itself (`NULL` when no pair differs but one is `NULL`).
- Left as they are: an `INSERT` of a `ROW(…)` into a `row(x …, y …)` column is refused ("Unsupported CAST", DataFusion casts rows by field name; write `CAST(ROW(…) AS row(x …, y …))`); array and row types of char values are reported as `array(varchar)`/`row(varchar)`; ordering (`<`) of rows holding char values keeps DataFusion's order where Trino refuses rows with `NULL` fields.
- Risk: Item 1 changes answers of queries that relied on DataFusion's row equality (now Trino's), and can slow joins on row or array keys.
- Revisit when: A human confirms or rejects each item; a join on row/array keys shows up in a workload (hash on a normalized key would restore the hash join).
- References: ADR 0015, ADR 0008, ISS-024, `scratchpad/reverify/char` (verifier harness, probes s1–s7).
