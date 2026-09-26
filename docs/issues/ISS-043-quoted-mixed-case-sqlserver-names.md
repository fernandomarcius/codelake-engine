# ISS-043 — Quoted mixed-case names are not resolved against SQL Server

- Kind: defect · Recorded: 2026-09-26 · Status: open
- Context: `SELECT ... FROM erp1.cdc."dbo_AplicacaoProdInsumo_CT"` fails with TABLE_NOT_FOUND on CodeLake; the unquoted form and the lower-case form work. dbt quotes every identifier, so every source declared with a mixed-case `identifier:` fails. ISS-018 accepted resolution without regard to case; main does not do it for quoted names of a federated source.
- Evidence: Cycle 1: 12 models failed (`dbo_AplicacaoProdInsumo_CT`, `dbo_CategoriasDeTipoDeMovimentacao_CT`, `dbo_EntSaiEmpAplic_CT`, `PesEndereco`, `PesFis`, `PesJur` on both bases). Cycle 2 lower-cased the identifiers in the project copy.
- Recommendation: Resolve quoted and unquoted names of SQL Server objects without regard to case (the source's collation decides; refuse when two objects differ only by case), as ISS-018 accepted.
- Revisit when: Done when `_sources.yml` runs unmodified.
- References: Shadow run on OrbStack, 2026-09-26 (dbt_pilot of reference-dbt-project, selection `+path:models/pilot +path:models/streaming`, 120 models, full refresh; reference Trino 431 with the production catalog settings, candidate `codelake:shadow-a1c2f23`). Reports: `~/Documents/Github.nosync/CodeLake-shadow/out` (cycle 1) and `out-2` (cycle 2, project copy with the workarounds below).
