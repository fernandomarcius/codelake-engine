# ISS-008 — Commit 9ef792e not marked breaking in its header

- Kind: history · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Accepted: keep the history; the breaking change is recorded in the CHANGELOG and ADR 0014 instead of rewriting `fix(core):` to `fix(core)!:`.
- Risk: tools that derive versions from commit headers miss the breaking change.
- Revisit when: release notes or versioning are generated from commits.
