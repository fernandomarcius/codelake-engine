# ISS-045 — Internal error "Cannot run range queries on datatype: Binary"

- Kind: defect · Recorded: 2026-09-26 · Status: open
- Context: Queries of the `pilot` layer that filter or join hive Parquet tables on `varbinary` LSN columns (`__$start_lsn`) fail with a DataFusion internal error ("Cannot run range queries on datatype: Binary … likely caused by a bug in DataFusion's code"). Likely the Parquet pruning or statistics path on `Binary`.
- Evidence: Cycle 2: 21 models failed (`pilot.aplicacaoprodinsumo`, `bancos`, `extrato`, `recebidas`, `vendas`, …), and every `streaming` model after them was skipped.
- Recommendation: Reproduce with a minimal Parquet table and a `varbinary` range predicate; fix in CodeLake (disable pruning for that type or give it correct statistics) and, if it is DataFusion's, record it in docs/upstream (upstream PRs are a human gate).
- Revisit when: Done when the `pilot` models run.
- References: Shadow run on OrbStack, 2026-09-26 (dbt_pilot of reference-dbt-project, selection `+path:models/pilot +path:models/streaming`, 120 models, full refresh; reference Trino 431 with the production catalog settings, candidate `codelake:shadow-a1c2f23`). Reports: `~/Documents/Github.nosync/CodeLake-shadow/out` (cycle 1) and `out-2` (cycle 2, project copy with the workarounds below).
