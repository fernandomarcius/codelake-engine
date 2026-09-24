# ADR 0003 — Memory model: one fair pool, bounded spill, fail the query not the node

- Status: Accepted
- Date: 2026-09-23; amended 2026-09-24 after the memory ceiling red team
  (DEFECT-MEM1..MEM9)

## Decision

- All query memory is reserved from a single `CeilingPool(memory.limit)`
  (`codelake_core::pool`) wrapped in `TrackConsumersPool`, so out-of-memory
  errors name the top 5 consumers. It keeps `FairSpillPool`'s policy (each
  reservation of an operator that can spill gets its share of what the
  operators that cannot spill left) with two changes:
  - every refusable growth keeps the **total** under the limit
    (`FairSpillPool` checked a spilling reservation only against its share,
    and an operator may hold several reservations);
  - exchange channels (`RepartitionExec`, the output merge below) are not
    counted among the operators sharing the pool: they get 1/8 of it, set
    aside, and spill what they are refused. Before, a plan over 18 partitions
    registered dozens of idle exchange consumers, and a sort's share of a
    64 MiB pool fell below the reservation of one batch: the sort failed
    without spilling.
- Every buffer between the plan and a client is reserved (`codelake_core::output`):
  - the last physical rule replaces `CoalescePartitionsExec` — also the one
    `execute_stream` puts on top of a multi-partition plan — with
    `AccountedMergeExec`: every batch it holds, queued or in the hands of a task
    waiting for room, is reserved or spilled to disk and dropped before the
    task waits. `CoalescePartitionsExec` held 2 × partitions batches per query
    outside the pool (DEFECT-MEM1/MEM2);
  - `ReservedOutputExec`, at the root, keeps the batch being sent reserved
    until the client asks for the next one (the PostgreSQL wire encodes it row
    by row as the socket drains; DEFECT-MEM9);
  - the Trino protocol reserves its channel, the batch being paged, the rows of
    the page being built and the body kept for a retry. A failed, canceled or
    abandoned query drops all of it at once, and so does a finished one once its
    final page is sent (that page is no longer served again: `410`; DEFECT-MEM3).

  A reservation the pool refuses fails the query with a resources error
  (`EXCEEDED_LOCAL_MEMORY_LIMIT`); a Trino page that already has rows is sent
  short instead.
- Sorts: a global `ORDER BY` runs one external sort (`repartition_sorts =
  false`), which still sorts its batches in parallel tasks, and keeps 1/16 of
  the pool (at most `DataFusion`'s 10 MiB) as merge headroom. Per-partition
  sorts each reserved their final merge greedily from the shared pool: the
  first one took what the others needed and they failed although spill was
  left (DEFECT-MEM4/MEM5).
- Spill goes to a dedicated directory bounded by `memory.spill_limit`; the
  Kubernetes volume has its own `sizeLimit` above that value.
- When memory and spill are exhausted, the query fails with a resources error;
  the process keeps serving other queries.
- The server allocator is jemalloc, built to purge freed pages at once
  (`JEMALLOC_SYS_WITH_MALLOC_CONF` in `.cargo/config.toml`). With glibc's
  per-thread arenas the process stayed hundreds of MiB above what the pool
  held after a spill (aggregation of 1M groups: 636 MiB of growth with glibc,
  168 MiB with jemalloc; the pool held at most 128). The cost is a page fault
  when freed memory is reused.
- The runtime runs `execution.target_partitions` workers when it is set: the
  parallelism a query asks for, and the bound of the batches being produced at
  one instant (next section). Trino clients, 12 at once: 267 MiB of growth with
  18 workers, 187-206 MiB with 4.
- `memory.limit` is set to ~75% of the container limit, leaving room for the
  allocator, connection buffers, metadata caches and the gaps below.

## What the pool does not cover (DataFusion 53)

The pool accounts what operators *hold*. The following are outside it. Numbers:
`crates/codelake-server/tests/memory_ceiling.rs`, peak `VmHWM` of the real
binary over its warmed-up idle footprint (~81 MiB), `memory.limit = 128 MiB`,
4 partitions; the test's budget is `limit + limit / 3` = 170.6 MiB.

| Gap | Bound | Measured |
|---|---|---|
| The batch an operator is producing, before its consumer reserves it, and the IPC copy of a batch being spilled | about two batches per worker; a batch is `batch_size` rows, so wide rows make it large (4 KiB rows: 32 MiB) | 12 Trino clients of 4 KiB rows (slow, abandoned, canceled): 187-206 MiB, 10-11 queries refused with `EXCEEDED_LOCAL_MEMORY_LIMIT`, nothing reserved after; 8 PostgreSQL connections (slow, stalled): 155-227 MiB, 5 refused |
| `BoundedWindowAggExec` buffers its input without reserving it; with `Utf8View` strings (DataFusion's type for `VARCHAR`) its concat + slice keeps every data buffer of the window partition | the window partition | sort of ~330 MiB under `row_number()`: 354 MiB (the sort spilled ~310 MiB, rows right) |
| An aggregation that spilled merges its spill files in "report error" mode with the hash table of the first phase; under its fair share it can fail | — | 1M groups of ~210 B: spills ~1.4 GiB, 168-170 MiB growth, then `EXCEEDED_LOCAL_MEMORY_LIMIT` |
| `RepartitionExec` keeps a batch it spilled in memory until its channel takes the marker; `SortPreservingMergeExec` and spill readers buffer a batch per input | one batch per input partition | — |
| Memory held by source drivers while decoding (e.g. MongoDB documents) and by in-memory tables | per source | — |

Sizing: keep `workers × 2 × batch bytes` inside the 25% headroom — lower
`execution.batch_size` for wide rows, or `memory.limit`. A hash join whose
build side does not fit fails cleanly (220 MiB build under 128 MiB: 116-120 MiB
of growth). Admission control per query and per user is M1.

## Evidence

- Exact accounting in process (`trino::tests::ceiling`, payload buffers counted
  when dropped): a paused client holds only reserved payload (MEM1: 48 MiB live,
  65.5 MiB reserved); 12 paused queries under 64 MiB hold at most the limit
  (MEM2); canceled queries keep no page body (MEM3: 0 B served, 0 B reserved); a
  canceled, killed or abandoned query leaves 0 B of payload and 0 B reserved
  (RT10); the probe itself returns to 0/0 after a completed query.
- Sorts of ~100 MiB spill ~99 MiB and return the right rows under 64 MiB with 2
  partitions (peak payload 28 MiB), 32 MiB with 2 (12 MiB) and 64 MiB with 8
  (26 MiB). Their tests (MEM4, MEM5) stay ignored: `row_number()` is sent as
  `decimal(20, 0)` instead of Trino's `bigint`, fixed on front/f4-functions.
- `engine::tests::query_larger_than_memory_spills_instead_of_failing` sorts
  ~40 MiB under 16 MiB; `query_fails_cleanly_when_memory_and_spill_are_exhausted`
  proves the failure mode; `output::tests` and `pool::tests` cover the merge
  (spill and read back under a paused consumer) and the pool.
