# ISS-065 — `*` over several Hive tables read with their hidden columns, joined by `USING`

- Kind: known defect · Recorded by the hidden-columns front (`feat/hive-hidden-columns`), 2026-10-02 · Status: open
- Context: the planner has no hidden columns: every column of a relation is in its `*`. A `SELECT`
  that names a hidden column (`"$path"`, `"$file_size"`, `"$file_modified_time"`) reads its Hive tables
  with them, and its wildcards leave them out (`codelake_core::hidden_columns`): `*` and `t.*` get
  `EXCLUDE (…)`, and a `*` over several such tables becomes one `x.*` per relation of its `FROM`. That
  rewrite cannot keep the meaning of `JOIN … USING` (a `*` lists a `USING` column once), nor name a
  relation that has no name (`(SELECT 1)` without an alias), so CodeLake refuses those:
  `SELECT * FROM t x JOIN t y USING (a) WHERE x."$path" = y."$path"` answers "qualify the wildcard
  (t.*) or list the columns" (Trino 431 answers the rows).
- Risk: none for the production views (one table per `SELECT`); a client query of that shape is
  refused, never answered wrong.
- Fix: the planner leaving hidden columns out of `*` (an upstream metadata-column feature), or the
  rewrite listing the `USING` columns once.
- Revisit when: a client or view needs that shape.
- References: differential case `hidden_columns` HC13; unit test
  `star_over_several_tables_that_cannot_be_listed_is_refused`.
