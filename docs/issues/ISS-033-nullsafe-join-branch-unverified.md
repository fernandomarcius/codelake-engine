# ISS-033 — front/nullsafe-join finished but not independently verified

- Kind: pending work · Recorded: 2026-09-26 (work stopped by the owner; weekly agent quota exhausted) · Status: open
- Branch: `front/nullsafe-join` (worktree `CodeLake-wt/nullsafe`), 4 commits over main `f7349d1`, HEAD `22fb66b`: null-safe join keeps NULL keys against the hash-join dynamic filter (ADR 0014 defects 13/14), `IS [NOT] DISTINCT FROM` precedence, `0.0` and `-0.0` as one value everywhere (ADR 0017, its own ISS-031 must be renumbered on merge). Gates at HEAD: fast 800, integration 293/293.
- Not merged: the independent verification was cut by the quota. Partial verifier notes (`scratchpad/nullsafe/progress.txt`): 17/19 mutants caught; remaining wrong rows still seen — `IS NOT DISTINCT FROM` combined with another join condition still loses NULL (VALUES/memory/hive), `EXCEPT ALL`/`INTERSECT ALL` multiplicity, window peers with `-0.0`/`0.0`; pre-existing: `NaN` with `NOT BETWEEN`/`NOT (x > 1)` drops rows.
- Risk: main still has the NULL row-group wrong result (EXCEPT/INTERSECT/IS NOT DISTINCT FROM over hive) and the `-0.0` divergences until this branch lands.
- Next: finish the verification, fix its findings, rebase on main, merge.
