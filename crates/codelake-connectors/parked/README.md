# Parked connectors

Source files here are **not compiled**: no `mod` declaration reaches them, so
`cargo build`, `clippy`, `fmt` and the tests ignore them. They wait for a
dependency that does not exist yet on the current DataFusion train.

| File | Waiting for | Decision |
|---|---|---|
| `iceberg.rs` | an Iceberg integration crate published for the train's DataFusion major (`iceberg-datafusion` 0.11.x final or `apache/datafusion-iceberg`) | [ADR 0013](../../../docs/adr/0013-iceberg-out-of-the-datafusion-54-train.md) |

To bring one back: move it to `src/`, restore its Cargo feature, its
dependencies (in the ADR 0002 block of the workspace `Cargo.toml`) and its arm
in `lib.rs::build`, port it to the current API, and add an integration test
that asserts rows.
