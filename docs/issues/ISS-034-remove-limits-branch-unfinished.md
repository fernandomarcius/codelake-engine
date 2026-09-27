# ISS-034 — front/remove-limits: verification failed, fix round unfinished

- Kind: pending work · Recorded: 2026-09-26 · Status: closed
- Branch: `front/remove-limits` (worktree `CodeLake-wt/limits`), HEAD `1109201`, 7 commits over main `f7349d1` + uncommitted work (saved in `scratchpad/pausa-final/limits.patch`).
- Approved by the verifier: ISS-020 (MiB/GiB) and ISS-023 (DROP VIEW keeps files).
- Failed: ISS-017 — a prepared statement on the PostgreSQL wire kept failing after `CREATE OR REPLACE` (the fix `1109201` re-plans it; not re-verified); ISS-018 — "any spelling resolves" fails outside SELECT (`INSERT INTO t ("Valor", "CUSTOMERCODE")`, `JOIN … USING ("CustomerCode")`, other paths in the verdict); the metastore keeps lower-case names with the written name in `codelake.name` (awaiting confirmation).
- Open decision: `MB` means 1000² in server config (`ByteSize`) and 1024² in session properties; unchanged.
- Next: finish the ISS-018 fixes from the patch, re-verify, merge; ISS-019 waits for the char work (ISS-037).
- Closed 2026-09-27: the saved patch (identifier_case: every spelling resolves in INSERT column lists, USING lists and on the PostgreSQL wire; unquoted non-ASCII capitals folded) committed, rebased on main and merged with the null-safe branch's regrouping of IS DISTINCT FROM (both applied on both protocols). Evidence: fast gate 849/849, integration 300/300 (the branch's red-team probes included), shadow run 2 cycles verdict equal (120/120 tables). Still open for the owner: the metastore stores folded names with the written one in `codelake.name` (ISS-018, awaiting confirmation); `MB` means 1000² in server config and 1024² in session properties (unchanged).
