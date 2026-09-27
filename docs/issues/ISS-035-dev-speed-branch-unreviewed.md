# ISS-035 — front/dev-speed (sccache + differential gate) not reviewed

- Kind: pending work · Recorded: 2026-09-26 · Status: closed
- Branch: `front/dev-speed` (worktree `CodeLake-wt/devspeed`), HEAD `06e44ef`, fast green.
- Content: shared `sccache` in the dev image (clean fast 313 s → 107 s with a warm cache, 100 % hits; content-keyed, no mtime false green), `tools/differential/` harness and `scripts/gates.sh differential` (284 cases vs Trino 431 / PostgreSQL 17, ~30 s; 210 known open defects listed, not green), its own ISS-032.
- Not merged: the independent review was cut by the quota.
- Image swap (after merge, with no front running): `heavy.sh docker build -f docker/dev.Dockerfile -t codelake-dev:local docker`.
- Next: review, merge, swap the image, make `differential` part of every front's DoD.
- Closed 2026-09-27: reviewed (build scripts, pinned sccache, CI default off), rebased on main `cad0c62`. Evidence: `scripts/sccache-check.sh` OK (different content missed, same content hit), fast gate 803/803, differential gate 284 cases as expected after dropping four stale `front/f6-fixes` markers. No engine change, so the shadow run result of `cad0c62` stands.
