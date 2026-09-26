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
| [ISS-010](ISS-010-red-team-decisions.md) | Ten red-team/semantics decisions applied by recommendation | behavior | 2026-09-24 | open |
| [ISS-011](ISS-011-configurable-compat-names.md) | Compatibility names configurable, CodeLake names by default | compatibility | 2026-09-24 | open |
| [ISS-012](ISS-012-spice-code-reuse.md) | Code copied from Spice.ai under Apache-2.0 | licensing | 2026-09-24 | open |
| [ISS-013](ISS-013-reference-engine-credentials-in-git.md) | Reference engine catalog credentials committed in an external repository | security (external) | 2026-09-24 | open |
| [ISS-014](ISS-014-w1-corrections-by-recommendation.md) | W1 corrections: choices applied by recommendation | behavior | 2026-09-25 | open |
| [ISS-015](ISS-015-bench-harness-sub-millisecond.md) | Bench harness measures sub-millisecond cases in bursts | measurement | 2026-09-25 | open |
| [ISS-016](ISS-016-erp-network-shadow-run.md) | Access to the source network authorized for the F8 shadow run | security | 2026-09-25 | open |
| [ISS-017](ISS-017-create-or-replace-atomic.md) | CREATE OR REPLACE on hive tables, atomic | removed limitation | 2026-09-25 | open |
| [ISS-018](ISS-018-case-preserving-names.md) | Case-preserving names, case-insensitive resolution | removed limitation | 2026-09-25 | open |
| [ISS-019](ISS-019-ctas-literal-unbounded-varchar.md) | CTAS of a literal infers unbounded varchar | removed limitation | 2026-09-25 | open |
| [ISS-020](ISS-020-datasize-iec-units.md) | Data sizes accept MiB/GiB | removed limitation | 2026-09-25 | open |
| [ISS-021](ISS-021-arrow-flight-sql.md) | New front: Arrow Flight SQL | scope | 2026-09-25 | open |
| [ISS-022](ISS-022-keep-dbt-trino-adapter.md) | Keep the existing dbt adapter until a native one is validated | dependency | 2026-09-25 | open |
| [ISS-023](ISS-023-drop-view-keeps-files.md) | DROP VIEW never deletes data files | deliberate divergence | 2026-09-25 | open |
| [ISS-024](ISS-024-char-semantics.md) | char(n) semantics instead of refusals | removed limitation | 2026-09-25 | open |
| [ISS-025](ISS-025-char-wrong-results-merged.md) | Known char(n) wrong results merged into main | known defect | 2026-09-25 | open |
| [ISS-026](ISS-026-f6-governance-by-recommendation.md) | F6 query governance: owner-only kill, SQLSTATE 53400, default limits, shared runtime | behavior | 2026-09-25 | awaiting-confirmation |
| [ISS-027](ISS-027-shadow-run-choices-by-recommendation.md) | Shadow-run harness: choices applied by recommendation | behavior | 2026-09-25 | awaiting-confirmation |
| [ISS-028](ISS-028-memory-ceiling-rss-test-flaky.md) | Memory-ceiling hash-join test measures process RSS with a thin margin | test reliability | 2026-09-26 | open |
| [ISS-029](ISS-029-extended-binary-fetch-refused.md) | FETCH in binary format on the PostgreSQL extended protocol is refused | limitation | 2026-09-26 | awaiting-confirmation |
| [ISS-030](ISS-030-cursor-transactions-by-recommendation.md) | PostgreSQL cursors and transactions: choices applied by recommendation | behavior | 2026-09-26 | awaiting-confirmation |
| [ISS-031](ISS-031-cursor-slots-by-recommendation.md) | PostgreSQL cursors, second round: one slot per connection, FETCH rows in the pool, DISCARD, MOVE | behavior | 2026-09-26 | awaiting-confirmation |
| [ISS-033](ISS-033-nullsafe-join-branch-unverified.md) | front/nullsafe-join finished but not independently verified | pending work | 2026-09-26 | open |
| [ISS-034](ISS-034-remove-limits-branch-unfinished.md) | front/remove-limits: verification failed, fix round unfinished | pending work | 2026-09-26 | open |
| [ISS-035](ISS-035-dev-speed-branch-unreviewed.md) | front/dev-speed (sccache + differential gate) not reviewed | pending work | 2026-09-26 | open |
| [ISS-036](ISS-036-f6-round3-unverified.md) | F6 round 3 (cursors) implemented, not verified | pending work | 2026-09-26 | open |
| [ISS-037](ISS-037-char-branch-unmerged.md) | char(n) semantics branch not merged | pending work | 2026-09-26 | open |
| [ISS-038](ISS-038-roadmap-fronts-not-started.md) | Roadmap fronts not started | pending work | 2026-09-26 | open |
| [ISS-039](ISS-039-benchmarks-and-global-dod.md) | Benchmarks and global Definition of Done pending | pending work | 2026-09-26 | open |
| [ISS-040](ISS-040-environment-leftovers.md) | Local environment leftovers after the iCloud move | environment | 2026-09-26 | open |
| [ISS-041](ISS-041-human-gates-pending.md) | Human gates pending | human decision | 2026-09-26 | open |
| [ISS-042](ISS-042-sqlserver-datetime-microseconds.md) | SQL Server `datetime` read as a microsecond timestamp; hive write refuses it | defect | 2026-09-26 | open |
| [ISS-043](ISS-043-quoted-mixed-case-sqlserver-names.md) | Quoted mixed-case names are not resolved against SQL Server | defect | 2026-09-26 | open |
| [ISS-044](ISS-044-varchar-length-dropped-in-metastore.md) | `varchar(n)` from a source is declared `string` in the hive metastore | defect | 2026-09-26 | open |
| [ISS-045](ISS-045-binary-range-pruning-internal-error.md) | Internal error "Cannot run range queries on datatype: Binary" | defect | 2026-09-26 | open |
| [ISS-046](ISS-046-duplicate-projection-names-refused.md) | Duplicate output names in an inner projection are refused | removed limitation | 2026-09-26 | open |
| [ISS-047](ISS-047-sqlserver-undefined-cp1252-bytes.md) | Bytes undefined in code page 1252 keep their value (U+0081), the reference replaces them with U+FFFD | expected difference | 2026-09-26 | awaiting-confirmation |
| [ISS-048](ISS-048-shadow-run-minor-findings.md) | Shadow run: minor findings | defect | 2026-09-26 | open |
