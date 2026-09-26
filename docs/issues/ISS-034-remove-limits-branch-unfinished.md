# ISS-034 — front/remove-limits: verification failed, fix round unfinished

- Kind: pending work · Recorded: 2026-09-26 · Status: open
- Branch: `front/remove-limits` (worktree `CodeLake-wt/limits`), HEAD `1109201`, 7 commits over main `f7349d1` + uncommitted work (saved in `scratchpad/pausa-final/limits.patch`).
- Approved by the verifier: ISS-020 (MiB/GiB) and ISS-023 (DROP VIEW keeps files).
- Failed: ISS-017 — a prepared statement on the PostgreSQL wire kept failing after `CREATE OR REPLACE` (the fix `1109201` re-plans it; not re-verified); ISS-018 — "any spelling resolves" fails outside SELECT (`INSERT INTO t ("Valor", "CUSTOMERCODE")`, `JOIN … USING ("CustomerCode")`, other paths in the verdict); the metastore keeps lower-case names with the written name in `codelake.name` (awaiting confirmation).
- Open decision: `MB` means 1000² in server config (`ByteSize`) and 1024² in session properties; unchanged.
- Next: finish the ISS-018 fixes from the patch, re-verify, merge; ISS-019 waits for the char work (ISS-037).
