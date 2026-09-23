# ADR 0003 — Memory model: one fair pool, bounded spill, fail the query not the node

- Status: Accepted
- Date: 2026-09-23

## Decision

- All query memory is reserved from a single `FairSpillPool(memory.limit)` wrapped
  in `TrackConsumersPool`, so out-of-memory errors name the top 5 consumers.
- Spill goes to a dedicated directory bounded by `memory.spill_limit`; the
  Kubernetes volume has its own `sizeLimit` above that value.
- When memory and spill are exhausted, the query fails with a resources error;
  the process keeps serving other queries.
- `memory.limit` is set to ~75% of the container limit, leaving room for the
  allocator, connection buffers and metadata caches.

## Evidence

`engine::tests::query_larger_than_memory_spills_instead_of_failing` sorts ~40 MiB
under a 16 MiB ceiling; `query_fails_cleanly_when_memory_and_spill_are_exhausted`
proves the failure mode.

## Known gaps

Memory held by source drivers while decoding (e.g. MongoDB documents) and by
in-memory tables is not tracked by the pool. Admission control per query and per
user is M1.
