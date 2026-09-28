# Upstream reports: datafusion-distributed 3.0.0 and DataFusion 54.1

Drafts found while integrating distributed execution (M4, 2026-09-27).
**Not filed**: filing needs maintainer approval (ADR 0005). CodeLake carries
the first two as patches in `vendor/datafusion-distributed/PATCHES.md` and
works around the third in `codelake_core::rebuild_state`.

## Draft 1 — datafusion-distributed: user codecs never encode functions

**Title:** `DistributedCodec::new_combined_with_user` puts `DistributedCodec`
first, so user codecs never see `try_encode_udf`/`try_encode_udaf`

`ComposedPhysicalExtensionCodec` keeps the first codec whose encode
succeeds. `DistributedCodec` inherits the default `try_encode_udf`, which
succeeds without writing anything, so a user codec placed after it is never
asked to encode a function. A function outside the worker's registry fails
to decode ("PhysicalExtensionCodec is not provided for scalar function …");
a function whose instance carries parameters, or that shares a built-in's
name, decodes as the registered one.

Expected: user codecs first (an empty encoding from them still resolves by
name), or `DistributedCodec` returning `NotImplemented` for functions.

## Draft 2 — datafusion-distributed: broadcast build sides bypass the pool

**Title:** `BroadcastExec` reserves the build side with `grow`, so a
worker's memory pool cannot refuse it

`BroadcastExec` holds every batch of the build side for all consumer tasks
and reserves them with `MemoryReservation::grow`. No pool refuses `grow`; a
large build side takes a worker past `memory_pool` limits instead of
failing the query as a single-node `CollectLeft` hash join does.

Expected: `try_grow`, and the error surfaced to the query.

## Draft 3 — DataFusion: `SessionStateBuilder::new_from_existing` can undo a function override

**Title:** rebuilding a state re-registers stale alias entries, so a
built-in can replace a UDF registered under its name

`new_from_existing` collects `scalar_functions.into_values()`, which
includes alias entries still pointing to a built-in that a UDF replaced
under its canonical name, and `build()` registers them again in hash order
(re-applying `with_updated_config`). With a different configuration
(another time zone or batch size) the built-in can win: after
`register_udf` of a `date_part` returning `Int64`, a rebuilt state resolved
`date_part` to the built-in (`Int32`) in some runs.

Expected: a rebuild preserves the name → function mapping it started from.
