# ADR 0013 — Iceberg stays out of the build on the DataFusion 54 train

- Status: Accepted
- Date: 2026-09-24
- Related: ADR 0002 (version train), ROADMAP M3

## Context

The workspace moves to DataFusion 54.1 (ADR 0002). Every crate of the train has
a published release for DataFusion 54 except the Iceberg integration:

- `iceberg-datafusion` 0.10.1, the newest release, links DataFusion 53. Two
  DataFusion majors in one binary do not interoperate and break the `train` gate.
- `iceberg` 0.11.0 (DataFusion 54, opendal 0.58) is still a release candidate;
  RC1 and RC2 failed their votes.
- `iceberg-rust` removed its DataFusion integration from `main`. The successor,
  `apache/datafusion-iceberg`, is not on crates.io yet (the crate name
  `datafusion-iceberg` belongs to another project).

The connector never shipped: it was behind the non-default `iceberg` feature,
outside the release image and outside the `deny` gate, because `opendal` 0.57
pulls `quick-xml` 0.39 (RUSTSEC-2026-0194/0195). Staying on DataFusion 53 for
it would hold back every other connector.

The gates build with `--all-features`, so a feature that cannot compile on this
train cannot stay declared.

## Decision

1. Remove `iceberg`, `iceberg-datafusion`, `iceberg-catalog-rest` and
   `iceberg-storage-opendal` from the dependency graph, and the `iceberg`
   Cargo feature from `codelake-connectors` and `codelake-server`.
2. Keep the connector's source, unchanged, in
   `crates/codelake-connectors/parked/iceberg.rs`, outside the module tree:
   it is not compiled, formatted or linted. `parked/README.md` explains how to
   bring it back.
3. The `iceberg` source kind stays in the configuration schema. A source of
   that kind fails at startup with "connector `iceberg` is not compiled into
   this build", as it already did in every shipped build.

## Condition to return

Bring the connector back when a crate that links DataFusion 54 or later and
reads Iceberg tables through a REST catalog is published on crates.io, on the
DataFusion major of the train at that time: `iceberg-datafusion` 0.11.x final,
or the first `apache/datafusion-iceberg` release. It must also clear
RUSTSEC-2026-0194/0195 (`opendal` ≥ 0.58.1, `quick-xml` ≥ 0.41) before it can
enter `default` or the release image. Returning it means a PR that moves the
file back to `src/`, restores the feature and the dependencies in the ADR 0002
block, adapts the code to the new API and adds an integration test that asserts
rows against a REST catalog.

## Consequences

- No build on this train can read Iceberg. No shipped build could before.
- `deploy/` and `examples/` configurations that declare an `iceberg` source
  behave as they did with any shipped build, which never compiled the kind.
- The parked file drifts from the engine's APIs while it waits. The return PR
  pays that cost once, against whichever integration crate is published.
- M3 (lakehouse) still depends on this return.
