# Benchmarks and correctness eval

Two layers, both machine-checked:

| Layer | What | Where | Gate |
|---|---|---|---|
| Micro | criterion benches of the engine core: aggregation and sort (roomy vs spilling), Parquet scan, hash join | `crates/codelake-core/benches/query_memory.rs` | `cargo xtask bench-gate target/criterion base head` on every PR |
| End to end | the same workload against CodeLake and Trino 431, same data, same container limits, **results compared before any timing** | `scripts/bench/run.sh`, `crates/codelake-bench`, `bench/` | `cargo xtask bench-gate --e2e <base>.json <head>.json` |

## Running the end-to-end benchmark

Requirements: Docker (cgroup v2), about 2 × `BENCH_MEMORY_MIB` of free memory,
and a **reserved machine**: nothing else building or querying while it measures.
Numbers taken on a busy machine are not published.

```bash
scripts/bench/run.sh                                       # TPC-H SF1, all workloads
BENCH_SCALE=tiny BENCH_REPS=1 BENCH_WARMUP=0 scripts/bench/run.sh   # 2-minute smoke run
BENCH_SCALE=10 BENCH_CPUS=8 BENCH_MEMORY_MIB=16384 scripts/bench/run.sh
```

| Variable | Default | Meaning |
|---|---|---|
| `BENCH_SCALE` | `1` | TPC-H scale: `tiny` (0.01), `1`, `10`, `100`, … |
| `BENCH_CPUS` / `BENCH_MEMORY_MIB` | `4` / `4096` | `docker --cpus/--memory` of **each** engine container (swap off) |
| `BENCH_REPS` / `BENCH_WARMUP` | `5` / `1` | measured / discarded repetitions (cases may override, e.g. micro-batch 30) |
| `BENCH_CASE_REPS` | unset | per-case `<id>=<reps>[/<warmup>]`, comma separated, over the workload files (e.g. `ctas_lineitem=1/0` at SF100); recorded in the report |
| `BENCH_TIMEOUT_SECS` | `1800` | per-statement timeout of the bench client (a CTAS of `lineitem` at SF100 may need more) |
| `BENCH_WORKLOADS` | `tpch,microbatch,metadata,pipeline` | files in `bench/workloads/` |
| `BENCH_PHASES` | `build generate trino codelake embedded compare` | subset to re-run a step |
| `CODELAKE_TARGET_VOLUME` | `codelake-target` | cargo target volume holding the release binaries |
| `BENCH_CLEAN` | unset | set to drop the warehouse volume (the data set is reused otherwise) |
| `BENCH_WAREHOUSE_VOLUME` | `codelake-bench-warehouse` | Docker volume holding the Parquet data set and the Hive metastore |
| `BENCH_PREFIX` | `codelake-bench` | name prefix of the containers and network, for parallel checkouts on one Docker host |

What the script does, in order:

1. **build** — `cargo build --release` of `codelake` and `codelake-bench` in the
   toolbelt container, one at a time. By default `BENCH_BUILD_ENV` turns off LTO
   and debug info (opt-level 3 and codegen-units 1 kept): the thin-LTO link of
   the engine does not fit a 12 GB Docker VM. Set `BENCH_BUILD_ENV=""` on a
   larger machine to measure the exact release profile; the report records
   which one was used.
2. **generate** — Trino 431 copies its `tpch` connector (the reference TPC-H
   generator) into the warehouse volume with CTAS: Parquet, Hive file metastore,
   money columns as `decimal(15,2)` as in the specification. Both engines then
   read the very same files, in the layout CodeLake must take over (ADR 0007).
   Idempotent: existing tables are kept after their row counts are re-checked.
3. **trino** — a fresh `trinodb/trino:431` container runs every case over the
   Trino REST protocol. JVM heap as the image sets it (80% of the container);
   query memory in the proportion production uses (`-Xmx8G`,
   `query.max-memory=6GB`, 75% of the heap): `query.max-memory` =
   `query.max-memory-per-node` = 75% of the heap, `memory.heap-headroom-per-node`
   = 20% (the sum must fit in the heap). With 8192 MiB: heap 6553 MiB, query
   memory 4914 MB. The image default is 30% of the heap per node (1.92 GB at
   8192 MiB), which fails SF100 joins and aggregations that production runs
   while CodeLake gets 75% of the container; that favored CodeLake. The file
   is generated into the output directory (`trino-etc/config.properties`) and
   the report's *Environment* records the JVM options and these limits.
4. **codelake** — a fresh `codelake serve` container (`memory.limit` = 75% of
   the container, `target_partitions` = CPUs, non-root uid 65532) runs the read
   cases over the PostgreSQL wire. The binaries run on the toolbelt image they
   were built in (same Debian and shared libraries).
5. **embedded** — the write cases (CTAS, INSERT) run with the CodeLake engine
   inside the bench process, in a container with the same limits, because the
   PostgreSQL wire path does not route DDL to the Hive catalog yet (ROADMAP M1.1
   follow-up). This is **not** a like-for-like protocol comparison and the
   report says so per case.
6. **compare** — the eval and the report.

If an engine container dies during a phase (for example killed by the kernel
for exceeding its memory limit), the script records it (exit code,
`OOMKilled`, time) under `engine_container_deaths` in the report's
*Environment* and starts the same container again; the bench client waits for
the engine, marks the case that was running as **ERROR** ("engine connection
lost during this case") and goes on with the next case on the restarted
engine. The restarted process starts with empty caches, and `memory.peak` of
the phase covers only the time since the last restart. Without this, one crash
turned every later case of the phase into an error that said nothing about
that case. A failed `embedded` phase is recorded the same way; its write cases
then show as **MISSING** and the read cases are still compared.

Every engine phase starts a fresh container and measures:

- **time to ready**: container start (`docker inspect .State.StartedAt`) → first
  successful `SELECT count(*) FROM hive.<tpch>.region` through the client
  protocol (a query that reads data: Trino answers `SELECT 1` on the
  coordinator before a worker has registered);
- **idle memory**: anonymous memory of the container 5 s after ready;
- per case: **latency** of every repetition (client wall time until the last
  row), **p50/p95** (nearest rank), **container CPU time** per repetition
  (cgroup `cpu.stat`), **peak anonymous memory** (cgroup `memory.stat` `anon`
  sampled every 20 ms: RSS without the page cache of the Parquet files, the
  same probe for both engines), **spill** peak (CodeLake's spill gauge; Trino's
  default configuration has spill disabled).

## Workloads

| File | Cases | Pattern |
|---|---|---|
| `tpch.toml` | 22 | TPC-H with the specification's validation parameters; Q15 as a CTE |
| `microbatch.toml` | 3 × 30 reps | short queries a 2-minute pipeline pays every cycle: key-window aggregate, key-window rows, point lookup; the window moves every repetition |
| `metadata.toml` | 4 × 10 reps | `information_schema` schemata/tables/columns, as dbt and BI tools read them |
| `pipeline.toml` | 4 (writes) | CTAS of `lineitem`, CTAS of a join+aggregate mart, repeated incremental `INSERT` (one file each), micro-batch `INSERT` of ~1000 rows × 30 |

Statements are templates: `{cat}` (catalog), `{tpch}` (data set schema),
`{work}` (per-engine scratch schema, `bench_trino` / `bench_codelake`) and `{i}`
(repetition index). A case may declare `setup`, `before_each`, `verify` and
`teardown` statements (untimed) and `writes = true` when it creates or drops
tables. Adding a workload is adding a TOML file; no code change.

## The eval: correctness before speed

Trino 431 is the reference. For every case, CodeLake's result (the `verify`
query's result for writes, the first measured repetition otherwise) is
compared with Trino's:

- same column count and row count;
- rows as multisets, sorted canonically; every cell equal: integers and
  decimals **by exact value** (`10.50` = `10.5`, `25.51` ≠ `25.507251`), text,
  dates and timestamps exactly, only `double`/`real` within a relative
  tolerance of 1e-9 (absolute 1e-12 near zero);
- a checksum (FNV-1a over the canonical rows) is recorded; results above
  50 000 rows are compared by checksum and count only.

Status per case: **OK**, **FAIL** (rows differ: a wrong result or a Trino
incompatibility — for a drop-in replacement both block), **ERROR** (the
statement failed on an engine), **MISSING** (did not run on one side). The
report shows timing only for OK cases: a fast wrong answer is not a result.
Column names and number types that differ while values match are listed as
notes, not failures.

## Reading the report

`bench/reports/<date>-<commit>.md`, generated from `bench/baselines/<commit>.json`
(scales other than SF1 add a suffix: `<commit>-sf10.json`, `<date>-<commit>-sf10.md`):

- **Correctness**: counts and every divergence (row, column, both values).
- **Summary**: per workload, geometric mean of `CodeLake / Trino` for p50
  latency and peak memory over the OK cases. **Below 1.00 CodeLake is faster
  or smaller.**
- **Process**: time to ready, idle memory, container `memory.peak`.
- **Per case**: p50/p95 of both engines and their ratio, peak memory and
  ratio, CPU time, CodeLake spill, and the CodeLake driver used.
- **Environment**: host CPU, Docker VM, limits, image digests, versions, and
  how many unrelated containers were running.

Caveats that apply to every number:

- p95 over 5 repetitions is the slowest repetition; use `BENCH_REPS` ≥ 20 for
  tail latency.
- Memory is sampled every 20 ms: spikes shorter than that are missed, for both
  engines alike.
- Peak memory is the container's, not the query's: memory an engine keeps
  after earlier cases (JVM heap committed by Trino, allocator caches in
  CodeLake) is included, so later cases read higher. It is what the host must
  provision, which is the question the ROADMAP's RSS target asks.
- Cases run through the `embedded` driver also carry the bench client in the
  measured container (small: it only collects the rows).
- Trino runs with image defaults except query memory (75% of the heap, the
  production proportion, see step 3); CodeLake with the documented defaults
  (query memory 75% of the container). No other tuning on either side, same
  container limits.
- On macOS, Docker runs in a VM: absolute numbers include virtualization cost;
  the ratios are the comparable quantity.

## Baselines and the regression gate

`bench/baselines/<commit>.json` is versioned: one file per measured commit,
with the eval, both engines' metrics and the environment. To check a change:

```bash
scripts/bench/run.sh                                   # on the change, same machine and settings
scripts/dev.sh cargo xtask bench-gate --e2e bench/baselines/<base>.json bench/baselines/<head>.json
```

The gate compares CodeLake's p50 per case with the same thresholds as the
micro benches (**< 10% PASS · 10–25% WARNING · > 25% BLOCK**), blocks any case
that matched Trino in the base and no longer does, and refuses baselines taken
with different scale, limits or repetitions.

## Client protocols

`codelake-bench` has three drivers: `pg` (PostgreSQL wire), `trino` (Trino REST
protocol: `POST /v1/statement`, follow `nextUri`, `DELETE` to cancel) and
`embedded`. When CodeLake serves the Trino protocol (ROADMAP M1.3), point the
`trino` driver at it — `run --engine codelake --driver trino --url
http://<codelake>:<port>` — and every case, writes included, runs through the
same protocol as Trino; the `embedded` phase then goes away.

## Not covered yet

- **Federated SQL Server → Hive** (the reference ingestion pattern): needs a
  `mcr.microsoft.com/mssql/server:2022-latest` container seeded with the TPC-H
  `orders`/`lineitem` tables, a `sqlserver` source in both engines' catalogs,
  and `INSERT INTO hive… SELECT … FROM sqlserver…` cases in a new workload file.
  On Apple Silicon that image runs under x86 emulation, which dominates its
  timings; measure it on x86 hardware.
- TPC-H SF10/SF100 need proportionally more memory and disk than a laptop VM.
