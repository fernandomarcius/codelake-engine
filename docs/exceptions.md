# Gate exceptions

Every suppressed or relaxed gate is listed here. An entry without an expiry date is
invalid. Expired entries fail review until renewed or removed.

| ID | Rule | Scope | Justification | Owner | Approved by | Expires |
|---|---|---|---|---|---|---|
| EX-001 | `clippy::unwrap_used`, `clippy::expect_used`, `clippy::panic` allowed (`#![allow(..., reason = "test crate")]`) | integration test crates under `crates/*/tests/` (e.g. `memory_ceiling.rs`, `e2e.rs`, `trino_clients.rs`) | A failed setup or assertion in an integration test must abort the test; propagating errors there adds noise without safety. Production code keeps the lints. | Fernando Farias | Fernando Farias, 2026-09-24 | 2027-03-24 |

## Known risks that are *not* exceptions

- `iceberg` connector: depends on `quick-xml` 0.39 (RUSTSEC-2026-0194, -0195) via
  `opendal` 0.57, pinned by `iceberg-storage-opendal` 0.10. The feature is not in
  `default`, not in the release image and not audited by the `deny` gate. Shipping
  it requires the upstream upgrade (ROADMAP M3), not an exception.
- `hive-s3` feature (S3/MinIO warehouses for the hive catalog): `object_store`
  0.13's S3 client pulls the same `quick-xml` 0.39. Local warehouses (the reference
  production case) ship; S3 waits for the DataFusion train with
  `object_store >= 0.14` (ROADMAP F11).
- `datafusion-federation` 0.5.3 lost-filter defect: mitigated in code by
  `federation_guard.rs`, covered by an integration test. Not suppressed; fixed.
