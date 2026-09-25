# Accepted issues

Every decision a human accepts that trades correctness, compatibility, security or
scope for something else is recorded here, one file per issue, so it can be
revisited. Gate exceptions stay in [`../exceptions.md`](../exceptions.md); an issue
may point to one.

Status: `open` (accepted, still to revisit or implement), `awaiting-confirmation`
(applied by recommendation without an explicit human answer), `closed`.

| ID | Title | Kind | Accepted | Status |
|---|---|---|---|---|
| [ISS-001](ISS-001-jemalloc-allocator.md) | jemalloc as the global allocator | dependency | 2026-09-24 | open |
| [ISS-002](ISS-002-test-crate-lint-allow.md) | `unwrap`/`expect`/`panic` allowed in integration test crates (EX-001) | gate exception | 2026-09-24 | open |
| [ISS-003](ISS-003-int96-default-time-zone.md) | INT96 time zone configurable per hive catalog, default `America/Sao_Paulo` | compatibility risk | 2026-09-24 | open |
| [ISS-004](ISS-004-iceberg-out-of-df54.md) | DataFusion 54.1 train without Iceberg | scope | 2026-09-24 | open |
| [ISS-005](ISS-005-statistics-canary-flipped.md) | Statistics canary test flipped after DataFusion 54 fixed the defect | test change | 2026-09-24 | open |
| [ISS-006](ISS-006-text-number-comparison-refused.md) | Text compared with a number refused on both client protocols (breaking) | breaking change | 2026-09-25 | closed |
| [ISS-007](ISS-007-sort-merge-join-hot-keys.md) | Sort-merge join with hot keys fails at the ceiling instead of avoiding the rewrite | memory limit | 2026-09-25 | open |
| [ISS-008](ISS-008-breaking-commit-without-bang.md) | Commit 9ef792e not marked breaking in its header | history | 2026-09-25 | open |
| [ISS-009](ISS-009-flaky-tests-rewritten.md) | Two unstable tests fixed to match their ADRs, not loosened | test change | 2026-09-25 | closed |
| [ISS-010](ISS-010-red-team-decisions.md) | Ten red-team/semantics decisions applied by recommendation | behavior | 2026-09-24 | awaiting-confirmation |
| [ISS-011](ISS-011-configurable-compat-names.md) | Compatibility names configurable, CodeLake names by default | compatibility | 2026-09-24 | open |
| [ISS-012](ISS-012-spice-code-reuse.md) | Code copied from Spice.ai under Apache-2.0 | licensing | 2026-09-24 | open |
| [ISS-013](ISS-013-reference-engine-credentials-in-git.md) | Reference engine catalog credentials committed in an external repository | security (external) | 2026-09-24 | open |
| [ISS-014](ISS-014-w1-corrections-by-recommendation.md) | W1 corrections: choices applied by recommendation | behavior | 2026-09-25 | awaiting-confirmation |
