# Sizing

Starting points come from the measured benchmark reports in
[`bench/reports`](../../bench/reports) (method: [docs/benchmarks.md](../benchmarks.md));
the final numbers come from the installation's own workload, measured as
described at the end of this page. Nothing below is extrapolated beyond what
was measured.

## What was measured

TPC-H in Parquet (hive layout), the reference engine next to CodeLake, same
container limits, results compared before timing (at scale 10 all 33 cases
matched the reference engine).

| Data set | Container | Pool (`memory.limit`) | CodeLake idle | Highest per-case peak | Spill | Report |
|---|---|---|---|---|---|---|
| TPC-H scale 1 (lineitem 6M rows), earlier engine version | 4 CPUs, 4 GiB | 3 GiB | 5.5 MiB | 1.4 GiB (TPC-H Q18) | 0 | `2026-09-24-94f0ba28b016.md` |
| TPC-H scale 10 (lineitem 60M rows) | 8 CPUs, 8 GiB | 6 GiB | ~10 MiB | 2.5 GiB (CTAS of a join + aggregate) | 0 | `2026-09-25-df53-vs-df54-cf34a2d-sf10.md` |

Per-case peak anonymous memory at scale 10 (DataFusion 54 run), rounded:

| Workload | Peak memory | p50 latency | CPU time per run |
|---|---|---|---|
| TPC-H reads (22 queries) | 120 MiB – 2.1 GiB; most under 700 MiB | 0.3 – 2.2 s | 0.7 – 14 s |
| Micro-batch reads (key window, point lookup) | 110 – 350 MiB | 0.12 – 0.26 s | 0.2 – 1.7 s |
| Metadata (`information_schema`) | ~45 MiB | ~1-2 ms | ~1 ms |
| `CREATE TABLE AS` of lineitem (60M rows) | 2.1 GiB | 29 s | 46 s |
| `INSERT` (incremental / ~1000-row micro-batch) | 1.9 – 2.0 GiB | 0.7 – 0.9 s | 1.2 – 3.9 s |

Write cases ran in-process in the bench client container, not over the
network; their memory includes the client.

Readings that matter for sizing:

- **CPU time is several times wall time**: a query uses every
  `target_partitions` worker (default: all CPUs). One heavy query keeps 8 CPUs
  busy; concurrent heavy queries share them and each gets slower.
- **Small queries do not get faster with more CPUs**: the micro-batch cases
  were slower than the reference engine (p50 ratio 1.3–1.5) while the TPC-H
  reads were about twice as fast. Latency objectives for 2-minute pipelines
  depend on these, so measure them with the canary.
- **No case spilled** with a 6 GiB pool at scale 10. Spill is the safety net for
  larger inputs and concurrency, not the normal path.
- Container `memory.peak` including page cache reached 3.9 GiB at scale 10: the
  kernel caches Parquet files in the container's memory accounting; that cache
  is reclaimable and does not cause an OOM kill by itself.

## Starting point

| Workload | CPUs | Container memory | `memory.limit` | `spill_limit` |
|---|---|---|---|---|
| Federation and micro-batches, data like TPC-H scale 1 (largest table ~6M rows), few concurrent queries | 4 | 4 GiB | 3 GiB | 20–40 GiB |
| Data sets like TPC-H scale 10 (largest table ~60M rows), pipeline runs with CTAS/INSERT of tens of millions of rows | 8 | 8 GiB | 6 GiB | 50–100 GiB |
| Larger data or several heavy runs at once | scale CPUs with concurrency; memory so that the heavy runs that overlap fit (~2.5 GiB each at scale 10) | ≥ 16 GiB | 75% | ≥ 5 × `limit` |

Rules:

- `memory.limit` ≈ 75% of the container limit; `requests.memory` =
  `limits.memory` on Kubernetes (no overcommit of the ceiling).
- Spill on local SSD/NVMe. `spill_limit` ≥ the largest intermediate state a
  query may need to put on disk (a sort, aggregation or join input larger than
  its share of the pool); the Kubernetes `emptyDir.sizeLimit` (or the volume)
  above `spill_limit`.
- Hive warehouse disk: the data, plus free space for the largest table being
  compacted (`OPTIMIZE` writes the new files before it deletes the old ones),
  plus the growth between compactions.
- `execution.target_partitions`: the CPU count for few heavy queries; fewer
  (for example half) when many queries run at once, to bound the memory
  outside the pool.

## Measure your own workload

Run the real pipelines against the installation (shadow run or canary period)
for at least one full cycle of the heaviest runs, then read:

| Question | Where |
|---|---|
| How close does the pool get to its limit? | max of `codelake_memory_reserved_bytes` / `codelake_memory_limit_bytes` |
| Does the process stay inside the container? | max of the container's working set (`container_memory_working_set_bytes`) / its limit; OOM kills (`kube_pod_container_status_last_terminated_reason{reason="OOMKilled"}` or `docker inspect … .State.OOMKilled`) |
| Is spill used, and how much? | max of `codelake_spill_used_bytes` / `codelake_spill_limit_bytes` |
| Are queries refused for memory? | `EXCEEDED_LOCAL_MEMORY_LIMIT` in clients' errors and `system.runtime.queries.error_code` |
| Is CPU the bottleneck? | container CPU throttling (`container_cpu_cfs_throttled_periods_total`) and canary latency during heavy runs |

Then:

- pool max < 50% and no spill: the container can shrink;
- pool at its limit with spill used, latency within the objective: fine;
- queries refused for memory: raise `limit` with the container, or move the
  heavy runs apart;
- working set near the container limit while the pool is not: lower
  `batch_size` or `target_partitions` (memory outside the pool, ADR 0003).

To compare candidate sizes on the installation's hardware, run the benchmark
harness with its limits (`BENCH_CPUS`, `BENCH_MEMORY_MIB`, `BENCH_SCALE`) on a
reserved machine; see [docs/benchmarks.md](../benchmarks.md).
