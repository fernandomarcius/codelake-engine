# ISS-019 — CTAS of a literal infers unbounded varchar

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: closed (2026-10-02: superseded, not implemented)
- Context: `CREATE TABLE AS SELECT 'um'` makes a `varchar(2)` column, as in the reference engine; a later longer INSERT fails.
- Accepted: Infer unbounded `varchar` for literals; explicitly declared `varchar(n)` stays enforced.
- Risk: Column types differ from the reference engine's (expected, declared difference in the shadow run).
- Revisit when: Done when implemented.
- Superseded: the code does the opposite, as Trino 431 does. A text literal is typed `varchar(length)` (`typed_text_literal`, `crates/codelake-core/src/coercion.rs`) and `CREATE TABLE AS SELECT 'um'` records `varchar(2)`; the inputs of a `UNION`, `CASE` or `coalesce` meet at `varchar(longest)`, or unbounded `varchar` when one input is (`unify_union_text`, PR #75, merge 23ae047). The shadow run's nightly validation (2026-09-29) found tables where the engine stored unbounded text and Trino 431 `varchar(n)`, and the fix followed the reference so both engines declare the same column types. Implementing this issue would bring that divergence back.
- References: ISS-010; PR #75; `crates/codelake-core/src/coercion.rs`.
