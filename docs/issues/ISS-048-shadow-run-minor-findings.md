# ISS-048 — Shadow run: minor findings

- Kind: defect · Recorded: 2026-09-26 · Status: open
- Context: (1) `U&'\FFFD'` Unicode string literals are refused ("Unsupported Value UnicodeStringLiteral"). (2) `SHOW TABLES FROM erp1.cdc LIKE ...` took about 100 s on the candidate, instant on the reference. (3) The hive metastore files are still named `.trinoSchema` by default (rename front, ISS-038). (4) Peak RSS not comparable yet: the candidate failed early (46–88 MB); reference 3.7–3.9 GB for about 65 s of dbt.
- Evidence: Found while diagnosing cycles 1 and 2.
- Recommendation: Fix (1) and (2) in their fronts; (3) in the rename front; measure RSS again once the models run.
- Revisit when: Close per item.
- References: Shadow run on OrbStack, 2026-09-26 (dbt_pilot of reference-dbt-project, selection `+path:models/pilot +path:models/streaming`, 120 models, full refresh; reference Trino 431 with the production catalog settings, candidate `codelake:shadow-a1c2f23`). Reports: `~/Documents/Github.nosync/CodeLake-shadow/out` (cycle 1) and `out-2` (cycle 2, project copy with the workarounds below).
