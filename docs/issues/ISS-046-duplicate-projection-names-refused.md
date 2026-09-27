# ISS-046 — Duplicate output names in an inner projection are refused

- Kind: removed limitation · Recorded: 2026-09-26 · Status: open
- Context: "Projections require unique expression names but the expression \"ct.__$start_lsn\" at position 1 and \"ct.__$start_lsn\" at position 6 have the same name". The reference accepts the same column twice in a subquery or CTE select list; only a table or view definition needs unique names.
- Evidence: Cycle 2: 6 models failed.
- Recommendation: Accept duplicate names where the reference does (inner queries); refuse only where the name would become ambiguous in a stored schema. The refusal does not protect data.
- Revisit when: Done when the 6 models run.
- References: Shadow run on OrbStack, 2026-09-26 (dbt_pilot of reference-dbt-project, selection `+path:models/pilot +path:models/streaming`, 120 models, full refresh; reference Trino 431 with the production catalog settings, candidate `codelake:shadow-a1c2f23`). Reports: `~/Documents/Github.nosync/CodeLake-shadow/out` (cycle 1) and `out-2` (cycle 2, project copy with the workarounds below).
- Fixed on branch `front/shadow-fixes` (4edb88d); shadow run cycle 4 (2026-09-26, `~/Documents/Github.nosync/CodeLake-shadow/out-4`): the unmodified project (only the LSN cutoff) runs 120/120 models on both engines. Pending: independent verification and the integration gate before merge.
