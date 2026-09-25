# Gate exceptions

Every suppressed or relaxed gate is listed here. An entry without an expiry date is
invalid. Expired entries fail review until renewed or removed.

| ID | Rule | Scope | Justification | Owner | Approved by | Expires |
|---|---|---|---|---|---|---|
| EX-001 | `clippy::unwrap_used`, `clippy::expect_used`, `clippy::panic` allowed (`#![allow(..., reason = "test crate")]`) | integration test crates under `crates/*/tests/` (e.g. `memory_ceiling.rs`, `e2e.rs`, `trino_clients.rs`) | A failed setup or assertion in an integration test must abort the test; propagating errors there adds noise without safety. Production code keeps the lints. | Fernando Farias | Fernando Farias, 2026-09-24 | 2027-03-24 |

## Known risks that are *not* exceptions

- `iceberg` connector: out of the build (ADR 0013); its source is parked in
  `crates/codelake-connectors/parked/`. No Iceberg crate is in the dependency graph.
  Bringing it back requires an Iceberg integration published for the train's
  DataFusion major and `opendal ≥ 0.58.1` (quick-xml ^0.41, clears
  RUSTSEC-2026-0194/0195), not an exception. The DataFusion integration moves to
  `apache/datafusion-iceberg`.
- `hive-s3` feature (S3/MinIO warehouses for the hive catalog): `object_store`
  0.13's S3 client pulls the same `quick-xml` 0.39. Local warehouses (the reference
  production case) ship; S3 waits for DataFusion 56, the first train on
  `object_store` 0.14.2 (quick-xml ^0.41) (ROADMAP M3, dependency train).
- `datafusion-federation` lost-filter defect (0.5.3, still present in 0.5.5):
  mitigated in code by `federation_guard.rs`, covered by an integration test. Not
  suppressed; fixed.
- DataFusion 54.1 join defects (null-aware anti join: dynamic filter
  apache/datafusion#23104, filter inference, sort-merge join #22810, constant and
  correlated `NOT IN`; nested loop join spill fallback #24675, #24746, #24677):
  mitigated in code by `join_guard.rs` (ADR 0014), each with a canary unit test
  and an integration test against Trino 431 (`join_regressions.rs`). A nested
  loop join over the memory limit fails instead of spilling. Not suppressed;
  guarded.
- Text compared with a number and `time ± interval`: refused by
  `operand_checks.rs` on the engine's client protocols (ADR 0014); the
  PostgreSQL wire path keeps DataFusion's coercion.
