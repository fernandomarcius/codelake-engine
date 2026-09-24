# ADR 0011 — Distributed execution: datafusion-distributed vs Ballista

- Status: Proposed
- Date: 2026-09-24
- Supersedes: —
- Related: ADR 0002 (version train), ADR 0003 (memory model), ADR 0005 (federation filter guard), ADR 0006 (pushdown policy), ROADMAP M4

## Context

ROADMAP M4 says a single node with 32–64 cores and NVMe spill handles hundreds of GB per query, and that TB-scale joins need scale-out. The DoD is: TPC-H SF1000 completes on N workers with at least 0.7 scaling efficiency from 1 to 4 workers, and no worker exceeds its memory ceiling.

Constraints any option must respect:

1. **Wrong results are the worst defect.** A distributed plan must return the same rows as single-node CodeLake and as Trino 431 on the same data.
2. **One DataFusion train (ADR 0002).** The distributed layer must have a release for the DataFusion major the rest of the train uses. It must not become the slowest crate in the train.
3. **Memory model (ADR 0003).** CodeLake uses one `FairSpillPool(memory.limit)` per node, wrapped in `TrackConsumersPool`, with bounded spill. When memory runs out, the query fails and the node keeps running. M4 also needs a cluster-wide admission budget.
4. **Federation (ADR 0005/0006).** Logical pushdown into remote SQL (SQL Server via tiberius, PostgreSQL, MySQL) must survive distribution. `FederationFilterGuard` must keep running. `datafusion-federation` still reports `PushedDown::Yes` for every parent filter on `main` ([sql/mod.rs](https://github.com/datafusion-contrib/datafusion-federation/blob/main/datafusion-federation/src/sql/mod.rs)), so the guard stays mandatory.
5. **Generic platform.** No customer-specific topology. It must deploy on Kubernetes with Apache-2.0-compatible licensing.

Custom plan pieces that would have to cross the wire (checked in the repo): the Hive `TableProvider` (`codelake-connectors/src/hive/table.rs`), the federation `VirtualExecutionPlan` backed by our SQL executors (SQL Server `bb8` pool over tiberius), `FederationFilterGuard` (`codelake-core/src/federation_guard.rs`) and the Trino-compatible UDFs.

## Options

**0. Stay single-node (scale up).** No new dependency. This is the baseline every benchmark is measured against. By the ROADMAP premise it does not meet the M4 DoD at SF1000.

**A. [datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed)** (datafusion-contrib, Apache-2.0).
- It is a library, not an engine. There is no split between coordinator and worker: any node can coordinate ([README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/README.md)).
- A distributed plan is an ordinary DataFusion physical plan plus network-boundary nodes: `NetworkShuffleExec`, `NetworkCoalesceExec` and `NetworkBroadcastExec`. Those nodes stream Arrow batches between workers over Arrow Flight/gRPC in a pipelined way ([concepts](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/learn/01-concepts.md), [plan building](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/learn/02-how-a-distributed-plan-is-built.md), [custom plans](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/advanced/05-custom-distributed-plans.md)).

**B. [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista)** (ASF, Apache-2.0).
- It is an engine: scheduler and executor processes, with clients that submit plans.
- It uses a blocking shuffle like Spark. Every stage writes Arrow IPC files to the executor work dir, and readers fetch them locally or over Arrow Flight ([FAQ](https://github.com/apache/datafusion-ballista/blob/main/docs/source/user-guide/faq.md), [shuffle design](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/shuffle.md), [#2318](https://github.com/apache/datafusion-ballista/issues/2318)).

**Discarded alternatives:**
- [Sail](https://github.com/lakehq/sail) is a drop-in Spark replacement: an engine with its own front end, not a library we can embed under a Trino front end. It is on DataFusion 55.1 ([Cargo.toml](https://github.com/lakehq/sail/blob/main/Cargo.toml)).
- [datafusion-ray](https://github.com/apache/datafusion-ray) is stale: it is pinned to DataFusion 45 and its last commit was 2025-10-05.
- [Comet](https://github.com/apache/datafusion-comet) is a Spark accelerator, not a standalone engine.

## Comparison

| Criterion | A. datafusion-distributed | B. Ballista |
|---|---|---|
| Release for DF 53 (our train) | `1.0.0` (2026-04-16) needs `datafusion ^53` ([crates.io](https://crates.io/crates/datafusion-distributed/1.0.0/dependencies)) | `53.0.0` (2026-05-25) needs `datafusion ^53` ([crates.io](https://crates.io/crates/ballista-core/53.0.0/dependencies)) |
| Newest release | `4.0.0` (2026-08-20) on DF 55. `2.0.0` and `3.0.0` are on DF 54. `main` already documents a 5.0 break ([upgrade/5.0.0](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/upgrade/5.0.0.md)) | `54.1.0` (2026-08-09) on DF 54. `main` is on DF 55 but not released ([Cargo.toml](https://github.com/apache/datafusion-ballista/blob/main/Cargo.toml)) |
| Lag after a DataFusion major | DF53 (03-23): 24 days. DF54 (06-08): 6 days (2.0.0). DF55 (08-18): 2 days (4.0.0) ([crates.io](https://crates.io/crates/datafusion-distributed/versions)) | DF53: 63 days. DF54: 37 days. DF55: no release after 37 days ([crates.io](https://crates.io/crates/ballista/versions)) |
| API churn | High: four majors in five months, each with breaking changes ([3.0](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/upgrade/3.0.0.md), [4.0](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/upgrade/4.0.0.md), [5.0](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/upgrade/5.0.0.md)) | Version follows the DataFusion major. Extension points are codecs and producers ([extending](https://github.com/apache/datafusion-ballista/blob/main/docs/source/user-guide/extending-components.md)) |
| Activity (GitHub API, 2026-09-24) | 139★, 38 contributors, 80 commits in 90 days, 63 open issues | 2142★, 352 contributors, 345 commits in 90 days, 144 open issues, 19 open `bug` |
| Governance | datafusion-contrib, "not part of Apache DataFusion" ([README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/README.md)) | Apache DataFusion sub-project |
| Shuffle | Pipelined streaming over Arrow Flight. Broadcast joins supported. AQE adapts task counts only ([AQE](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/advanced/08-adaptive-query-execution.md)) | Blocking and materialized to local disk (Arrow IPC), read over Flight. AQE is default-on in recent `main` ([#2092](https://github.com/apache/datafusion-ballista/issues/2092)) |
| Fault tolerance | None during a query. A failing worker fails the whole query ([#640](https://github.com/datafusion-contrib/datafusion-distributed/issues/640)). Retry before side effects is still an open PR ([#728](https://github.com/datafusion-contrib/datafusion-distributed/pull/728)) | Re-runs only the lost map tasks, with a retry budget per stage ([shuffle.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/shuffle.md)). "Job hangs when all executors are lost" is still open ([#2029](https://github.com/apache/datafusion-ballista/issues/2029)), although fix [#2212](https://github.com/apache/datafusion-ballista/pull/2212) merged 2026-08-03. A scheduler restart fails in-flight queries ([kubernetes.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/user-guide/deployment/kubernetes.md)) |
| Memory per worker | The worker builds its `SessionState` through `WorkerSessionBuilder` and can share our `RuntimeEnv` via `with_runtime_env` ([worker docs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/user-guide/03-worker.md)). That maps 1:1 onto ADR 0003 (one fair pool per node). Unverified: whether network and broadcast buffers are reserved in the pool. Broadcast eviction is still an open PR ([#726](https://github.com/datafusion-contrib/datafusion-distributed/pull/726)) | Each task gets its own `FairSpillPool` of `memory_pool_size / vcores`, and idle slots do not lend memory ([tuning guide](https://github.com/apache/datafusion-ballista/blob/main/docs/source/user-guide/tuning-guide.md)). That does not match ADR 0003; a custom runtime producer could replace it. Shuffle files in the work dir are outside the pool. Defaults to sort-merge join because hash join does not spill. A spilling hash join is an open PR ([#2108](https://github.com/apache/datafusion-ballista/pull/2108)) |
| Cluster admission budget | Not provided; CodeLake builds it on the coordinator | Not provided as a memory budget; scheduling is by task slots |
| Kubernetes | Bring your own: a `WorkerResolver` example with kube-rs ([worker resolver](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/user-guide/02-worker-resolver.md)), worker versioning for rolling deploys ([versioning](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/advanced/07-worker-versioning.md)), custom channels via a channel resolver | Documented Deployments for scheduler and executor, health port, rolling upgrade, KEDA autoscaling ([kubernetes.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/user-guide/deployment/kubernetes.md)). Needs a PVC per executor for shuffle: the SF1000 run used 1000 GiB gp3 per executor ([benchmarking.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/benchmarking.md)) |
| Known correctness issues | Closed: [#563](https://github.com/datafusion-contrib/datafusion-distributed/issues/563) (CollectLeft/NLJ returned incomplete results over several tasks; fixed 2026-08-08). Open: [#310](https://github.com/datafusion-contrib/datafusion-distributed/issues/310) (the default task estimator can break partitioning) | Open: [#2046](https://github.com/apache/datafusion-ballista/issues/2046) (wrong TPC-DS results with the static planner), [#2065](https://github.com/apache/datafusion-ballista/issues/2065) (broadcast promotion drops a required sort order and silently gives wrong results), [#1055](https://github.com/apache/datafusion-ballista/issues/1055) (outer join wrong with CollectLeft/broadcast) |
| Published performance | Project's own numbers, 12× c5n.2xlarge (8 vCPU, 21 GiB): TPC-H SF100 total 42 s, against Trino 93 s, Ballista 237 s and Spark 261 s ([README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/README.md)) | SF1000 on 32 executors of 8 vCPU / 64 GiB: 817 s against Spark 3.4 at 845 s. Row counts agree. Q8 wrote 313 GB of shuffle ([benchmarking.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/benchmarking.md)) |
| Federation (SQL Server pushdown) | Not documented. A custom leaf without a `DesiredTaskCountHandler` defaults to one task ([work-unit feeds](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/docs/source/advanced/04-work-unit-feeds.md)), so a federated scan reads on one worker. Planning happens only on the coordinator ("a worker only *executes* fragments"), so our logical pushdown and the guard stay in the coordinator's optimizer. Workers need a codec for `VirtualExecutionPlan` plus source credentials and pools | Not documented. The scheduler plans, so the guard and federation rules must be registered in the scheduler's session builder. Executors need physical codecs, credentials and pools |
| Iceberg | Read-only integration crate in the repo, not published ([iceberg/README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/iceberg/README.md), epic [#599](https://github.com/datafusion-contrib/datafusion-distributed/issues/599)) | Requested, open ([#2174](https://github.com/apache/datafusion-ballista/issues/2174)) |
| Integration shape for CodeLake | Embed in `codelake-server`. The coordinator is the existing Trino-protocol server with the distributed planner enabled; the worker is the same binary in worker mode | CodeLake becomes a Ballista client, plus custom scheduler and executor binaries that embed our codecs, UDFs and rules. The client fetches results from the executors over Flight ([scheduler.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/user-guide/scheduler.md)) |
| Integration cost (estimate, not measured) | About 3–5 engineer-weeks: codecs (Hive, federation, guard, UDFs), K8s resolver, mTLS and auth on the channels, admission budget | About 6–10 engineer-weeks: the above plus two new binaries, a state backend, result-fetch path through our protocol server, and shuffle PVC operations |
| License | Apache-2.0 | Apache-2.0 |

Both projects need the same thing from federation: a federated scan runs as one partition on one worker. Distribution speeds up the joins and aggregations after the scan, not the read from SQL Server. With N workers, each keeping its own pool per source, the number of connections to the source multiplies. We need a connection cap per source across the cluster, whichever option we pick.

## Preliminary recommendation

Adopt **datafusion-distributed** as the M4 candidate and keep **Ballista** as the documented fallback. The benchmark below confirms or reverses this; nothing merges before it runs.

Why A:
- It is a library. It runs inside our existing server and planner, which keeps the core stateless per node as the M4 plan expects.
- Our `RuntimeEnv` (one `FairSpillPool` per node) plugs in directly.
- It has followed every recent DataFusion major within days, so it will not become the slowest crate in the train.
- Pipelined execution suits the interactive, Trino-like workload we are replacing.
- Its correctness record is better today: one closed wrong-result bug. Ballista has three open wrong-result bugs.

Risks of A:
1. **No fault tolerance during a query.** Losing one worker fails the query. The mitigation is a query-level retry in the coordinator, safe only for read-only statements. Writes (CTAS, INSERT, MERGE) need idempotent commits first. Worker admission and retry before side effects is still an open PR ([#728](https://github.com/datafusion-contrib/datafusion-distributed/pull/728)).
2. **Pipelined memory pressure.** All stages hold memory at the same time. Unverified whether network and broadcast buffers are reserved in the pool. The benchmark must measure the worst-case peak memory (RSS) per worker.
3. **Governance and churn.** The project lives in datafusion-contrib with a small maintainer base, and ships one breaking major per DataFusion major. We pin it exactly, it joins the ADR 0002 train block, and each bump is a train PR.
4. **Federated scans do not parallelize.** Covered by the per-source connection cap.
5. **Its benchmark is self-published** ([dev-tools](https://github.com/gabotechs/datafusion-distributed-dev-tools)). We reproduce, we do not trust.

When to choose B instead: long batch or ETL jobs at SF1000 on preemptible nodes, where losing an executor must not restart the query. This holds only once [#2046](https://github.com/apache/datafusion-ballista/issues/2046) and [#2065](https://github.com/apache/datafusion-ballista/issues/2065) are closed.

## Benchmark plan

We extend the existing harness: `scripts/bench/run.sh`, `crates/codelake-bench`, and results compared with Trino 431 before any timing ([docs/benchmarks.md](../benchmarks.md)). Data comes from Trino's `tpch` connector, as today, or from [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) 3.0.0. No production data.

**Hardware.** The local 12 GB Docker VM can only run the smoke phase (SF1–SF10, 2 workers). It cannot host SF100.

| Phase | Nodes | Per node | Storage | Network |
|---|---|---|---|---|
| P0 correctness | laptop or CI | — | local | — |
| P1 SF100 | 1 coordinator (8 vCPU / 32 GiB) + 4 workers. Single-node baseline: 1 × 64 vCPU / 256 GiB | 16 vCPU / 64 GiB / ≥ 500 GB local NVMe (spill and, for B, shuffle) | S3-compatible object store on a separate node, or cloud S3 | ≥ 10 Gbps, 25 Gbps preferred |
| P2 SF1000 | 1 coordinator + 8 workers (runs at 1, 2, 4, 8 workers) | 32 vCPU / 128 GiB / ≥ 1.5 TB local NVMe | object store with ≥ 1 TB usable | ≥ 25 Gbps |

Minimum for the M4 DoD (efficiency from 1 to 4 workers): 4 workers of 32 vCPU / 128 GiB / 2 TB NVMe. The 1-worker SF1000 point is spill-bound; if it cannot finish, report efficiency from 2 to 8 workers and say so explicitly. For reference, Ballista's SF1000 run used 32 × (8 vCPU / 64 GiB / 1 TB gp3); a dedicated disk was needed because shuffle writes saturated the shared EBS ([benchmarking.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/benchmarking.md)).

**Phases:**

- **P0 — correctness gate, SF1 and SF10.**
  - TPC-H 22 and TPC-DS 99 on options 0, A and B. Results are compared with Trino 431 and with single-node CodeLake.
  - A federated suite joins a SQL Server test container (`codelake-f13-*`) with Parquet. It checks that the remote SQL text is identical to single-node and that the guard is active.
  - Failure injection: SIGKILL one worker in the middle of a query. Expected outcome: B completes; A fails cleanly within the timeout, with no hang.
  - Memory-ceiling test: cgroup `memory.peak` of each worker must stay at or below its limit.
- **P1 — SF100.** 1 warmup and 5 measured repetitions (harness defaults). Collect p50/p95 per query, total time, spill bytes, shuffle/network bytes, and peak RSS per worker.
- **P2 — SF1000.** Scaling runs. Efficiency is `E(n) = T(1) / (n · T(n))`, or the 2→8 variant.

**Decision criteria.** The first five are gates; any failure disqualifies the option. The rest break ties.

1. Zero result mismatches in P0 and P1 (Trino 431 and single-node).
2. No worker exceeds its memory ceiling. Exhaustion shows up as a clean resources error (ADR 0003 semantics).
3. SF1000 completes with E ≥ 0.7 from 1 to 4 workers (M4 DoD).
4. Federated pushdown is unchanged from single-node, and source connections stay under the configured cap.
5. A release exists for the current train major, pinned exactly.
6. Tie-breaks, in order: fault-tolerance outcome, SF100 and SF1000 totals, integration cost measured in the spike, operating burden (processes, PVCs, state).

## Consequences

- The chosen crate joins the ADR 0002 train block and Dependabot group. The `train` gate covers it.
- The distributed layer lives in the server and connector crates, never in `codelake-core` (`cargo xtask arch`). Codecs belong to the connectors that own the plans.
- A follow-up ADR records the numbers and moves this one to Accepted or Rejected.
