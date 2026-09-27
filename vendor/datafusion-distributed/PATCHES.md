# datafusion-distributed 3.0.0, patched for CodeLake

Source: the published crate `datafusion-distributed` 3.0.0 (crates.io, tag
`v3.0.0`, 518f25b), Apache-2.0 (`LICENSE.txt`). Only the library is kept:
examples, tests, benchmarks and dev-dependencies are removed from
`Cargo.toml`. The DataFusion 54 line of the library gets no more releases
(ADR 0011), so fixes CodeLake needs live here until the DataFusion 56 train.

## 1. User codecs before the library's (`src/codec/distributed_codec.rs`)

`DistributedCodec::new_combined_with_user` composed `[DistributedCodec, user
codecs…]`. `ComposedPhysicalExtensionCodec` encodes a function with the first
codec whose `try_encode_udf`/`try_encode_udaf` succeeds, and `DistributedCodec`
inherits the defaults, which succeed without writing anything. A user codec
therefore never encoded a function: a function that is not in the worker's
registry, or whose instance carries parameters, failed to decode or decoded as
the registered function of the same name (CodeLake's overflow-checked `sum`
and `avg` would have become the unchecked ones).

The patch puts the user codecs first. A user codec that writes nothing for a
function it does not own keeps the encoded definition empty (position 0, empty
blob), so the worker resolves the function by name in its registry, as before.


## 2. Broadcast build sides reserved within the limit (`src/execution_plans/broadcast.rs`)

`BroadcastExec` holds the whole build side of a broadcast join, for every
consumer task, and reserved it with `MemoryReservation::grow`, which no pool
refuses: a large build side took the worker past its memory ceiling.
It reserves with `try_grow`, so a build side the pool cannot hold fails the
query with a resources error, as a single-node hash join's build side does
(ADR 0003: fail the query, not the node).

The library's other buffers stay on `grow` because they are bounded by
construction: `NetworkBoundary` (two messages per input stream),
`WorkerConnection` (the per-connection budget, which CodeLake sizes from the
node's memory limit) and `PartitionSampler` (only with dynamic task counts,
off by default).

Upstream: neither patch is reported (ADR 0005); drafts in docs/upstream/.
