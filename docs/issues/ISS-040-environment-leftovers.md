# ISS-040 — Local environment leftovers after the iCloud move

- Kind: environment · Recorded: 2026-09-26 · Status: open
- The repository moved to `~/Documents/Github.nosync`; files that were only in iCloud became unreadable placeholders (reads block). The main worktree (`~/Documents/Github.nosync/CodeLake`) still has ~500 such files; its checkout is behind `main` (the ref was advanced from other worktrees with `update-ref`). Do not run git commands that read its working tree until it is repaired.
- 143 loose objects were moved to `.git/quarantine-dataless/` (git fsck is clean without them; they are unreachable). Put them back into `.git/objects` once materialized.
- A background download (`scratchpad/icloud/baixar.sh`) reads every placeholder under `Github.nosync` (all repositories).
- Stale worktrees and large build volumes (e.g. `codelake-target-w1-df54`, 146 GB) to prune when their branches are merged.
- Containers of other projects stopped around the VM restart (agent-control-plane orchestrator/egress-proxy, planoacao-qa-local, cflow-g0): not restarted.
