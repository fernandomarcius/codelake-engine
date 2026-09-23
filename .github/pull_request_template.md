## What and why

<!-- Link the issue. Title must follow Conventional Commits (feat:, fix:, perf:, ...). -->

Closes #

## Criticality

<!-- LOW / MEDIUM / HIGH / CRITICAL — calibrates mutation and performance gates. -->

## Definition of Done

Deterministic gates run in CI; a human reviewer does not override a red gate.

- [ ] Requirement satisfied, with the acceptance criteria verified
- [ ] Unit tests describe behaviour (would fail if the implementation were wrong)
- [ ] Integration test against a real instance for any connector or SQL change
- [ ] Wrong-result risk considered: new pushdown paths are covered by a test that checks rows, not only plans
- [ ] Memory: new operators reserve from the pool; no unbounded buffering
- [ ] Observability: logs/metrics for new failure modes
- [ ] Docs updated (README, ROADMAP, ADR if architecture changed)
- [ ] No gate suppressed without an entry in `docs/exceptions.md`
