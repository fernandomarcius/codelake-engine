# ISS-019 — CTAS of a literal infers unbounded varchar

- Kind: removed limitation · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: `CREATE TABLE AS SELECT 'um'` makes a `varchar(2)` column, as in the reference engine; a later longer INSERT fails.
- Accepted: Infer unbounded `varchar` for literals; explicitly declared `varchar(n)` stays enforced.
- Risk: Column types differ from the reference engine's (expected, declared difference in the shadow run).
- Revisit when: Done when implemented.
- References: ISS-010.
