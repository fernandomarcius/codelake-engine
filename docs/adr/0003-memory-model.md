# ADR 0003 — Memory model: one fair pool, bounded spill, fail the query not the node

- Status: Accepted
- Date: 2026-09-23; amended 2026-09-24 after the memory ceiling red team
  (DEFECT-MEM1..MEM9), and on 2026-09-24 for hash joins over the pool
  (DataFusion 54 train)

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
    outside the pool (DEFECT-MEM1/MEM2). A refused batch is written in chunks of
    ~1 MiB (string views compacted per chunk): the IPC writer encodes what it
    writes into a buffer of its own, and a 32 MiB batch written at once needed
    another 32 MiB, outside the pool, at the moment memory ran out;
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
- Operators that keep rows run on plain strings (`codelake_core::plain_views`,
  a physical rule before the output one). A string view (`Utf8View`,
  `DataFusion`'s type for `VARCHAR`) shares data buffers, and `take`, `slice` and
  `concat` keep all of them:
  - a hash aggregation spills its state in slices taken from it, and each slice
    recorded the memory of the whole state; merging the spill files reserves
    two such "batches" per file, so the merge did not fit the operator's share
    (1M groups of ~210 B under 128 MiB: 800 MB spilled, then 31 MB asked to
    merge slices of 1.7 MB, `EXCEEDED_LOCAL_MEMORY_LIMIT` — DEFECT-MEM6). Both
    phases now group by the keys cast to `LargeUtf8` (`LargeBinary`), so the
    hash exchange between them copies strings instead of sharing every batch's
    buffers with four partitions: the same query finishes in 3.6 s (was 12 s,
    then failed) with 119 MiB of growth;
  - a window appends each batch with `concat` and drops what it no longer
    needs with `slice`, so it kept the buffers of every batch it read (a sort of
    ~330 MiB under `row_number()`: 358 MiB of growth — DEFECT-MEM4). It now
    reads the view columns no expression types as `LargeUtf8`: 115 MiB.

  A node above each rewritten operator casts back and reports the original
  properties, so the rest of the plan is unchanged; each cast copies the
  strings once.
- Batches of computed values are sized in bytes (`codelake_core::computed_batches`,
  a physical rule after the plain-strings one). `DataFusion` counts a batch in
  rows, so a projection computing wide values made batches of `batch_size` ×
  their width (`repeat('x', 4096)`: 32 MiB), allocated whole before any
  consumer could reserve them, one per running worker (DEFECT-MEM8/MEM9). Every
  projection computing a string, binary, list, map or struct now reads its
  input in slices whose row count follows the bytes per row it produced: about
  2 MiB per batch, starting at 64 rows, growing at most fourfold per batch, never
  above `batch_size` (narrow rows reach it after four batches). A column of such
  a batch that still shares the input's buffers (a column passed through, a
  string view into the input) is copied to its own rows when it holds more than
  twice their bytes, so consumers do not reserve the whole input once per slice.
  The same 12 Trino clients grow the process by 73 MiB (was 187-213), 8
  PostgreSQL connections by 23 MiB (was 155-227).
- Hash joins whose build side does not fit run as sort-merge joins
  (`codelake_core::join_spill`, derived from Spice.ai's
  `CayenneAntiJoinSortMergeRewriter`, Apache-2.0, see NOTICE). A
  `HashJoinExec` holds its whole build side in a reservation that cannot
  spill, in DataFusion 53 and 54 alike. A physical rule right after
  `EnforceSorting` estimates the build side (rows × bytes per row × 5/2 for
  the hash table; column sizes from statistics, else the rows' total size,
  else 64 bytes per variable-width value) and compares it with the join's
  slice: the smaller of half of the pool less the exchange slice and an even
  share of that among the plan's hash joins. Past the slice the join becomes a
  `SortMergeJoinExec`: each input merged into one stream and sorted by one
  external sort (a sort per partition keeps its own merge headroom, and 2 × N
  of them starve a small pool before any holds a batch), each side run within
  a budget of a third of the pool less the exchange slice (`JoinSideBudget`:
  the sort that finishes first merges its spilled runs with as many files as
  the pool has room for and left the other sort without a batch), and the
  output spread again over the hash join's partitioning. Rewritten joins that
  run side by side (a `UNION ALL` of two) share that budget, down to twice
  the sort's merge headroom; a join inside an input of another already runs
  within that input's budget. The budget bounds the sorts, merges and
  aggregations of an input; the joins inside it (a hash join's build side,
  which cannot spill) are bounded by the engine's pool only, as before the
  rewrite (`join_spill::tests::a_hash_join_inside_an_input_is_bounded_by_the_pool_not_the_budget`:
  a ~15 MB build under a 32 MiB pool completes; counted against the ~9 MB
  budget it was refused). The join trades
  parallelism for a plan that fits.
  - Rows of one key value: the sort-merge join holds the rows of the current
    key value of its buffered side and spills them past what the pool
    grants, but keeps their key columns in memory, and reads a spilled batch
    back to emit it, with `MemoryReservation::grow`, which the pool records
    and cannot refuse (DataFusion 54.1 `sort_merge_join/materializing_stream.rs`).
    That memory grows with the number of rows sharing a key: 1M rows of one
    100-byte key hold ~100 MB outside the pool's say. `SortMergeWithinCeiling`
    wraps each rewritten join in a pool that holds its buffered rows within
    the budget of one input (past it they spill, while the pool still has
    room for their keys) and watches the unchecked growth; once the
    engine's pool holds more than its limit, the join and its inputs fail
    with `Resources exhausted` at their next batch, one batch's keys over the
    limit at most. A key repeated past the pool fails, as the hash join did;
    it never holds more than the ceiling in silence.
  - Statistics: an exact row count is used as it is; an estimate (`Inexact`:
    a filter's selectivity, the hive metastore or footers) must exceed the
    slice twice over; a build side of unknown size (no statistics: a
    `generate_series`, a source without them) stays a hash join and fails
    cleanly if it does not fit. Either operator answers the same rows, so a
    wrong estimate costs speed or a refusal, never rows.
  - The join stays a hash join whenever the sort-merge join could answer
    other rows: a null-aware anti join (`NOT IN`, apache/datafusion#22810,
    ADR 0014), mark joins, keys that compare NULLs as equal, a residual
    (non-equi) condition, a key computed by an expression (checked arithmetic
    and `try` are installed on hash join keys only), a key type the
    sort-merge comparator does not support or orders differently (floating
    point, zoned timestamps, decimals other than 128-bit, times, intervals,
    nested types), a pushed limit, an output ordering. Joins with no equality
    are nested loop joins, whose spill fallback stays off (ADR 0014).
  - Session property `sort_merge_oversized_joins` (default `true`) turns it
    off per query.
- Spill goes to a dedicated directory bounded by `memory.spill_limit`; the
  Kubernetes volume has its own `sizeLimit` above that value.
- When memory and spill are exhausted, the query fails with a resources error;
  the process keeps serving other queries.
- The server allocator is jemalloc (`tikv-jemallocator`; dependency approved by the
  owner on 2026-09-24), built to purge freed pages within a second, from a
  background thread (`dirty_decay_ms:1000,muzzy_decay_ms:0` in
  `JEMALLOC_SYS_WITH_MALLOC_CONF`, `.cargo/config.toml`). With glibc's
  per-thread arenas the process stayed hundreds of MiB above what the pool
  held after a spill (aggregation of 1M groups: 636 MiB of growth with glibc,
  168 MiB with jemalloc; the pool held at most 128).
  Amended 2026-09-27: purging at once (`dirty_decay_ms:0`) turned every free
  into an `madvise` and the next allocation into page faults. On TPC-H SF10
  (16 vCPU, x86) it took 2.6× the time and 2.8× the CPU of a one-second decay
  (sum of p50 51.6 s against 19.8 s, CPU 507 s against 177 s); the one-second
  decay raised the mean per-query peak from 0.94 to 1.23 GiB (maximum 2.5 to
  2.9 GiB). A fixed short decay does not hold the ceiling near the limit (a
  1M-group aggregation under 128 MiB: 188 MiB of growth at 100 ms against a
  171 MiB budget), so the decay follows the pool: `codelake_core::pool`
  signals when it holds more than an eighth of its limit (and again under a
  twentieth), and the server sets every jemalloc arena's `dirty_decay_ms` to 0
  while memory is scarce and back to 1000 ms after (`tikv-jemalloc-ctl`, the
  allocator's safe control interface). The memory-ceiling tests hold, 5 of 5
  runs. See `bench/reports/2026-09-27-k3s-sf10.md`.
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
| The batch an operator is producing, before its consumer reserves it | about two batches per worker. A computed value: ~2 MiB per batch (above). Rows that arrive wide from a source (a scan, a connector) or that an operator assembles from wide inputs (sort, join and aggregation output) are still `batch_size` rows: 4 KiB rows make 32 MiB | 12 Trino clients of 4 KiB computed rows (slow, abandoned, canceled): 73 MiB, nothing reserved after; 8 PostgreSQL connections (slow, stalled): 23 MiB. Before the computed-batch rule: 187-213 MiB and 155-227 MiB |
| Values an expression computes on its way to a narrower result (`length(repeat(...))`) | `batch_size` rows of the intermediate width, one per worker, while the expression runs | — |
| String views where their type is part of an expression: aggregate states that are views (`min`/`max` of a string), window columns a function reads or a computed `PARTITION BY`/`ORDER BY` uses, and the exchange of an aggregation whose partitions a partitioned join matches (its final phase still groups plain keys) | the buffers of the batches such an operator keeps | — |
| `RepartitionExec` keeps a batch it spilled in memory until its channel takes the marker; `SortPreservingMergeExec` and spill readers buffer a batch per input | one batch per input partition | — |
| Memory held by source drivers while decoding (e.g. MongoDB documents) and by in-memory tables | per source | — |

Sizing: keep `workers × 2 × batch bytes` inside the 25% headroom. Computed
values take care of themselves; for sources whose rows are wide (documents,
long text), lower `execution.batch_size` (at 1024, 4 KiB rows make 4 MiB
batches), or `memory.limit`. A sort gets its fair share among every spilling
operator the plan registered, idle grouped aggregations included: under a
small pool a sort of wide rows needs smaller batches (24 MiB, 4 partitions,
rows of ~210 bytes: 2048 rows per batch). A hash join whose build side does
not fit runs as a sort-merge join when the planner can size it (above); one it
cannot size fails cleanly (220 MiB `generate_series` build under 128 MiB:
116-120 MiB of growth). Admission control per query and per user is M1.

## Evidence

- Exact accounting in process (`trino::tests::ceiling`, payload buffers counted
  when dropped): a paused client holds only reserved payload (MEM1: 8.1 MiB
  live, 8.5 MiB reserved); 12 paused queries under 64 MiB hold at most the
  limit (MEM2: 15 MiB live, 61.7 MiB reserved); canceled queries keep no page body (MEM3: 0 B served, 0 B reserved); a
  canceled, killed or abandoned query leaves 0 B of payload and 0 B reserved
  (RT10); the probe itself returns to 0/0 after a completed query.
- Sorts of ~100 MiB spill ~99 MiB and return the right rows under 64 MiB with 2
  partitions (peak payload 30 MiB), 32 MiB with 2 (14 MiB) and 64 MiB with 8
  (24 MiB). Their tests (MEM4, MEM5) stay ignored: `row_number()` is sent as
  `decimal(20, 0)` instead of Trino's `bigint`, fixed on front/f4-functions.
- `engine::tests::query_larger_than_memory_spills_instead_of_failing` sorts
  ~40 MiB under 16 MiB; `query_fails_cleanly_when_memory_and_spill_are_exhausted`
  proves the failure mode; `output::tests` and `pool::tests` cover the merge
  (spill and read back under a paused consumer) and the pool;
  `computed_batches::tests` the batch sizes, the rows and their order.
- End to end (`tests/memory_ceiling.rs`, budget 170.6 MiB over idle): 12 Trino
  clients 73 MiB, 8 PostgreSQL connections 23 MiB, a sort of ~330 MiB 115 MiB
  (spilled 311 MiB, rows right; its test stays ignored for the `row_number()`
  type), an aggregation of 1M groups 121 MiB (spilled 424 MiB, counts right), a
  220 MiB hash join build 116 MiB (`EXCEEDED_LOCAL_MEMORY_LIMIT`: its
  `generate_series` inputs have no statistics, so it stays a hash join).
- Hash joins over the pool (`join_spill`): in process
  (`join_spill::tests::a_build_side_larger_than_the_pool_completes_as_a_sort_merge_join`),
  a ~20 MB build under 16 MiB fails as a hash join (`Resources exhausted`) and
  completes as a sort-merge join with the right numbers, the pool's own peak
  under its limit. Against Trino 431
  (`crates/codelake-connectors/tests/integration/hash_join_spill.rs`, hive
  tables of 200 000 and 250 000 rows of ~210 bytes, pool 24 MiB): the hash
  join is refused; inner, left, right, full, semi and anti joins (integer
  and text keys, NULLs on both sides) return Trino's rows, both sorts spill,
  the pool peaks at 11-16 MB. Inner, right semi and right anti joins whose
  buffered side repeats a key on 75 000 rows (`hive.hj.hot`) return Trino's
  rows; the inner one spills rows of that key. The guarded shapes stay hash joins with the
  same rows (`join_spill::tests::guarded_joins_stay_hash_joins`). One key on
  400 000 rows (~40 MB of keys) under 16 MiB
  (`join_spill::tests::a_key_repeated_past_the_pool_fails_instead_of_passing_the_ceiling`)
  took the pool past its limit (17.0 MB reserved at the first growth past
  it) and completed without the watch; with it the query fails with
  `Resources exhausted` and the pool returns to zero. Keys repeated on 100 000 rows (~9 MB per key) under 16 MiB
  (`join_spill::tests::repeated_keys_spill_the_buffered_side_and_keep_the_hash_join_rows`):
  inner, left, right, full, semi, right semi and right anti joins answer
  the hash join's rows with the pool under its limit; the inner join spills
  rows of its buffered side and reads them back.
