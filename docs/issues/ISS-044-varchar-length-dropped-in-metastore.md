# ISS-044 — `varchar(n)` from a source is declared `string` in the hive metastore

- Kind: defect · Recorded: 2026-09-26 · Status: open
- Context: Every table written by the candidate declares character columns as `string`; the reference keeps `varchar(n)` from the source. Row values are equal; the declared schema is not.
- Evidence: Cycle 2: 62 of 63 tables written by both sides diverge only on declared types (`varchar(30)` → `string` 106 times, `varchar(5)` 92, `varchar(175)` 90, …).
- Recommendation: Keep the declared length through `CREATE TABLE AS` (it is part of the schema contract readers of the warehouse depend on, e.g. the SQL Server sink through OPENROWSET). If the length is dropped on purpose, it needs its own accepted issue and an expected-difference rule in the shadow harness. Related: ISS-019.
- Revisit when: Done when declared types match or the difference is accepted and declared.
- References: Shadow run on OrbStack, 2026-09-26 (dbt_pilot of reference-dbt-project, selection `+path:models/pilot +path:models/streaming`, 120 models, full refresh; reference Trino 431 with the production catalog settings, candidate `codelake:shadow-a1c2f23`). Reports: `~/Documents/Github.nosync/CodeLake-shadow/out` (cycle 1) and `out-2` (cycle 2, project copy with the workarounds below).
