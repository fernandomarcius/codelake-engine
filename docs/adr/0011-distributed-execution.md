# ADR 0011 — Distributed execution: datafusion-distributed vs Ballista

- Status: Proposed (revised 2026-09-27)
- Date: 2026-09-24, revised 2026-09-27
- Supersedes: —
- Related: ADR 0002 (version train), ADR 0003 (memory model), ADR 0005 (federation filter guard), ADR 0006 (pushdown policy), ADR 0007 (Hive catalog), ADR 0010 (OTLP, no gRPC tree), ADR 0016 (query governance), ROADMAP M4 (F13, F14), ISS-038

Revision of 2026-09-27: the first version was written against the DataFusion 53 train and the `FairSpillPool` memory model. This revision replaces it in place (it is in the git history); the recommendation is unchanged, the evidence and the conditions are new.

## Context

ROADMAP M4 has two fronts:

- **F13**: choose between Apache DataFusion Ballista and `datafusion-distributed`, measured on TPC-H SF100 and SF1000.
- **F14**: coordinator and workers, Arrow Flight shuffle, a memory ceiling per worker and a cluster-wide budget. DoD: SF1000 completes, scaling efficiency is at least 0.7 from 1 to 4 workers, and no worker exceeds its ceiling.

The first version of this ADR made `datafusion-distributed` the candidate. Three things changed since:

1. The train moved to DataFusion 54.1 (ADR 0002, amended 2026-09-24). The first version's compatibility row was about DataFusion 53.
2. The node pool is now `CeilingPool` (`codelake_core::pool`), not `FairSpillPool` (ADR 0003, amended). `CeilingPool` checks the total on every refusable growth. It gives exchange buffers (`RepartitionExec[`, `SortPreservingRepartitionExec[`, `AccountedMerge[`) 1/8 of the limit. It records unchecked `MemoryReservation::grow` calls but does not refuse them.
3. Reading the source of both candidates at their DataFusion 54 tags showed facts that the first version could not see from the documentation (see "Findings from the source").

Constraints (unchanged):

1. Wrong results are the worst defect.
2. One DataFusion train (ADR 0002).
3. The memory model of ADR 0003: fail the query, not the node.
4. Federation pushdown survives, and `FederationFilterGuard` keeps running (ADR 0005/0006).
5. Generic platform: Kubernetes, Apache-2.0-compatible licenses.

### What would cross the wire (counted in the repository at `origin/main` 8db75e2)

- **13 custom `ExecutionPlan`s in production code.**
  - `codelake-core`: `AccountedMergeExec`, `ReservedOutputExec`, `FederationFilterBarrier`, `SliceExec`, `SizedBatchesExec`, `JoinSideBudget`, `SortMergeWithinCeiling`, `RetypeExec`, `NullAwareProbeBarrier`, `NestedLoopJoinWithoutFallback`, `JoinInputRuntime`, `NullKeysKept`.
  - `codelake-connectors`: `ExactTimestampsExec`.
  - Also crossing: `datafusion_federation::sql::VirtualExecutionPlan`, which holds our SQL executors (SQL Server through a `bb8` pool over tiberius; PostgreSQL and MySQL through `datafusion-table-providers`).
- **10 `PhysicalOptimizerRule`s and 15 `OptimizerRule`/`AnalyzerRule`s** (char semantics, coercion, overflow, join guards, subqueries, Trino functions, window frames, and others).
- **About 40 UDF/UDAF/UDWF implementations** (the count is `impl *UDFImpl`).
- **`TableProvider`s:** `HiveTable`, `HiveView`, `SystemTable`, `QueriesTable`, `PooledTable`, plus the federation adaptors.
- **No `datafusion-proto`, `arrow-flight` or production `tonic` dependency today.** `tonic` 0.14.6 is in `Cargo.lock` only through `bollard` (testcontainers, dev only). ADR 0010 deliberately avoided a gRPC tree.
- **The Hive scan is a plain `DataSourceExec` over `ParquetSource`** (`hive/table.rs`), which both candidates distribute natively.
- **The warehouse is `object_store` `fs` only.** `hive-s3` is not shipped until the DataFusion 56 train (ADR 0002, ROADMAP M3). Commits take an in-process mutex plus `flock` (ADR 0007).

## Options

**0. Stay single-node (scale up).** No new dependency. It is the baseline for every number. By the ROADMAP premise it does not meet the M4 DoD at SF1000 (unverified; P2 below measures it).

**A. [datafusion-distributed](https://github.com/datafusion-contrib/datafusion-distributed) 3.0.0** (datafusion-contrib, Apache-2.0).
- It is a library. A distributed plan is a DataFusion physical plan plus network boundaries (`NetworkShuffleExec`, `NetworkCoalesceExec`, `NetworkBroadcastExec`), and data streams between workers over Arrow Flight/gRPC.
- There is no coordinator/worker split: any node can coordinate ([README at v3.0.0](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/README.md)).

**B. [Apache DataFusion Ballista](https://github.com/apache/datafusion-ballista) 54.1.0** (ASF, Apache-2.0).
- It is an engine: a scheduler, executors, and clients that submit logical plans or SQL.
- The shuffle is blocking. Stages write Arrow IPC files to the executor work dir, and readers fetch them locally or over Flight ([architecture](https://github.com/apache/datafusion-ballista/blob/54.1.0/docs/source/contributors-guide/architecture.md)).

**C. Build our own on `datafusion-proto` + `arrow-flight`.** This is considered and rejected. Option A is exactly this: the same crates plus a stage planner, and it is Apache-2.0. Building our own would re-implement boundary injection, task-count estimation, and the correctness work A already did ([#563](https://github.com/datafusion-contrib/datafusion-distributed/issues/563)). It stays the exit if A is abandoned upstream: fork A at the pinned tag.

**Rejected in the first version:** Sail (its own Spark front end), datafusion-ray (stale), Comet (Spark accelerator). Not re-checked for this revision.

## Findings from the source

Sources: crates.io API, GitHub API, and shallow clones at tags `v3.0.0` (518f25b) and `54.1.0` (f4e6652), all read on 2026-09-27.

### Version train

| | A. datafusion-distributed | B. Ballista |
|---|---|---|
| Releases on DF 54 | `2.0.0` (2026-06-14) and `3.0.0` (2026-08-14): `datafusion ^54.0.0`, `datafusion-proto ^54.0.0`, `arrow-flight ^58`, `object_store ^0.13`, `tonic ^0.14.1` ([crates.io](https://crates.io/crates/datafusion-distributed/3.0.0/dependencies)) | `54.0.0` (2026-07-15) and `54.1.0` (2026-08-09): `datafusion ^54`, `datafusion-proto ^54`, `arrow-flight ^58.3`, `tonic ^0.14` ([crates.io](https://crates.io/crates/ballista-core/54.1.0/dependencies)) |
| Fits our lock (arrow 58.4.0, object_store 0.13.2, prost 0.14.4)? | Yes by semver; not built (no cargo run for this revision) | Yes by semver; not built |
| Newest release | `4.0.0` (2026-08-20) on DF 55. `main` is on DF 55 at version 4.0.0 and documents a 5.0 break | `54.1.0`. `main` is on DF 55, unreleased |
| Maintenance of the DF 54 line | No `3.x` branch or tag after `v3.0.0` (tags checked). Fixes after 2026-08-14 are not backported (unverified whether they would be on request) | No `54.x` after `54.1.0` (unverified whether a 54.2 is planned) |
| Feature gates | Since 3.0.0, `grpc` (arrow-flight, tonic, tower) is a default feature, with `parquet` and `sql` | Not applicable |

Both candidates have a DataFusion 54 release, so neither blocks F14 on the train. Both have moved `main` to DataFusion 55, which our train skips (ADR 0002). Whichever we pick, we run a **frozen release** until the DataFusion 56 train. A correctness fix we need in the meantime means a patch on our side (`[patch.crates-io]` to a tag-based fork), not an upgrade.

### Activity and governance (GitHub API, 2026-09-27)

- **A.** 139★, 81 open issues and PRs. 88 commits since 2026-06-29. One maintainer dominates: `gabotechs` has 218 commits against 31 for the next contributor.
  - Its README says "not part of Apache DataFusion".
  - The DataDog origin is recorded only as a repository fork ([DataDog/datafusion-distributed](https://github.com/DataDog/datafusion-distributed)). No public statement of production use was found (unverified).
- **B.** 2142★, 191 open issues and PRs, ≥ 100 commits since 2026-06-29. It is an Apache DataFusion sub-project.
  - 54.0.0 had 171 commits from 11 contributors ([blog](https://datafusion.apache.org/blog/output/2026/07/12/datafusion-ballista-54.0.0/)). 54.1.0 had 10 commits from 3 contributors ([blog](https://datafusion.apache.org/blog/output/2026/08/09/datafusion-ballista-54.1.0/)).
  - Listed adopters: Spice AI and Coralogix ([README](https://github.com/apache/datafusion-ballista/blob/54.1.0/README.md)). Spice runs a fork ([spiceai/datafusion-ballista](https://github.com/spiceai/datafusion-ballista)). It adds multi-active scheduler HA, bidirectional control streams, catalog and UDF sync to executors, mandatory mTLS and custom codecs, and uses it for batch and analytical work, not sub-second queries ([Spice blog, 2026-04-09](https://spice.ai/blog/apache-ballista-at-spice-ai)).

### Architecture and where our rules run

**A.**
- `DistributedQueryPlanner` wraps the query planner. It runs the default physical planner, with every registered physical rule including ours. Then it rewrites the finished plan (`src/distributed_planner/distributed_query_planner.rs` at v3.0.0) in four steps:
  1. wrap a `CoalescePartitionsExec`;
  2. insert `BroadcastExec` under `CollectLeft` hash joins;
  3. inject boundaries above hash `RepartitionExec`s and under `CoalescePartitionsExec`/`SortPreservingMergeExec`;
  4. prepare the boundaries.
- A worker only executes: it decodes the stage with `datafusion-proto` plus user codecs and runs it ([worker guide](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/docs/source/user-guide/03-worker.md)). Our analyzer, logical and physical rules therefore run once, in our own coordinator process.
- UDFs go into the worker's `SessionStateBuilder`, through `Worker::from_session_builder`.
- Worker plan rewrite handlers allow worker-local, topology-preserving rewrites ([plan hooks](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/docs/source/advanced/03-plan-hooks.md)).

**B.**
- Clients submit a logical plan or SQL to the scheduler. The scheduler builds the execution graph and stages ([architecture](https://github.com/apache/datafusion-ballista/blob/54.1.0/docs/source/contributors-guide/architecture.md)).
- Our analyzer and optimizer rules must therefore be registered in the scheduler's `override_session_builder`, and the executors need the physical codec, the function registry and the runtime producer (`ExecutorProcessConfig`) ([extending components](https://github.com/apache/datafusion-ballista/blob/54.1.0/docs/source/user-guide/extending-components.md)).
- The logical plan crosses the wire, so every custom `TableProvider` needs a `LogicalExtensionCodec::try_encode_table_provider` (`ballista/core/src/serde/mod.rs`). So does every logical extension node, such as federation's.
- Unverified: whether our logical rules still give the same result when a client optimizes the plan and the scheduler optimizes it again.

### Memory per worker

**A.**
- `Worker::with_runtime_env` accepts our `RuntimeEnv` (`src/worker/worker_service.rs`), so one `CeilingPool` per worker maps 1:1 onto ADR 0003.
- The library's own buffers use **unchecked** `MemoryReservation::grow`, all at v3.0.0. `CeilingPool` records that growth and never refuses it:

  | Consumer | Source file | What it holds |
  |---|---|---|
  | `WorkerConnection` | `protocol/grpc/worker_client.rs` | Bounded by `worker_connection_buffer_budget_bytes`, default 64 MiB **per connection**. The config says the actual peak is budget + one message |
  | `NetworkBoundary` | `protocol/grpc/spawn_select_all.rs` | Channel of 2 batches per stream |
  | `PartitionSampler[..]` | `execution_plans/sampler.rs` | Samples for dynamic task counts |
  | `BroadcastExec[..]` | `execution_plans/broadcast.rs` | The **whole build side** of a broadcast join, for every consumer task, until they finish. `broadcast_joins` is `true` by default. Eviction is an open PR ([#726](https://github.com/datafusion-contrib/datafusion-distributed/pull/726)) |

- None of these names is in `CeilingPool::EXCHANGE_BUFFERS`. They register as unspillable, so they would also shrink the fair shares of sorts and aggregations.
- **As shipped, A can take a worker past its ceiling.** It does so through broadcast build sides and through the per-connection buffers, whose sum grows with the number of peer workers × concurrent queries.

**B.**
- By default each task gets its own `FairSpillPool` of `memory_pool_size / concurrent_tasks`, and idle slots do not lend memory ([tuning guide 54.1](https://github.com/apache/datafusion-ballista/blob/54.1.0/docs/source/user-guide/tuning-guide.md)).
- The executor calls a `RuntimeProducer` closure per task (`ballista/executor/src/executor.rs`). A closure that captures one `Arc<CeilingPool>` would give one pool per executor. This is inferred from the code and not tested.
- In 54.1.0 the sort-based shuffle writer uses `try_grow` and spills when refused ([54.1 blog](https://datafusion.apache.org/blog/output/2026/08/09/datafusion-ballista-54.1.0/)). It also has a separate 256 MiB `ballista.shuffle.sort_based.memory_limit`. Unverified: whether the hash shuffle writer reserves.
- Shuffle files live on the work dir, outside the pool and outside our `DiskManager` spill limit (unverified that they bypass it; the work dir is Ballista's own).
- Ballista defaults to sort-merge join because DataFusion's hash join does not spill. We already bound both joins with our own guards (`join_guard`, `join_spill`), and those guards would have to be registered in the scheduler.

### Custom plans and federation

**A.**
- Custom nodes need a `PhysicalExtensionCodec` registered on both the coordinator and the workers ([custom plans](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/docs/source/user-guide/04-distribute-custom-plan.md)). A custom leaf is split with `DesiredTaskCountHandler` and `ScaleUpLeafNodeHandler`; `maximum(1)` means "cannot be distributed". `RouteTasksHandler` pins tasks to workers.
- A federated `VirtualExecutionPlan` therefore stays one task. Routing it to the worker co-located with the coordinator would keep SQL Server credentials and pools on one node (unverified that routing to the coordinator's own worker is supported).
- `datafusion-federation` 0.5.5 does not depend on `datafusion-proto` ([crates.io](https://crates.io/crates/datafusion-federation/0.5.5/dependencies)), so it ships no codec for `VirtualExecutionPlan`. We would write one.

**B.** It needs the same physical codecs, plus logical codecs for every `TableProvider` and logical extension node. Spice's fork shows this is feasible, and also that it is sizeable.

### Correctness record

**A.**
- [#563](https://github.com/datafusion-contrib/datafusion-distributed/issues/563): incomplete results for `CollectLeft`, cross and nested-loop joins over several tasks. Closed on 2026-08-08 by [#583](https://github.com/datafusion-contrib/datafusion-distributed/pull/583) (normalize collect joins), which is present in v3.0.0.
- Still open: plan-time validation of the invariant "every stage, run once per task and unioned, equals one run over all data" ([#745](https://github.com/datafusion-contrib/datafusion-distributed/issues/745); the earlier attempt [#582](https://github.com/datafusion-contrib/datafusion-distributed/pull/582) was abandoned).
- Still open: [#310](https://github.com/datafusion-contrib/datafusion-distributed/issues/310) (the default task estimator may break partitioning).
- Consequence for us: our 13 nodes, several of which wrap joins, are not validated by the library. Only our differential gate catches a violation.
- The boundary injector recognizes nodes by `downcast_ref` to `CoalescePartitionsExec`, `SortPreservingMergeExec`, `RepartitionExec` and `HashJoinExec`. Our output rule replaces `CoalescePartitionsExec` with `AccountedMergeExec`, and our join guards wrap joins. Unverified what boundaries A injects for such plans. This is spike item S1.

**B.** Open wrong-result issues:
- [#2046](https://github.com/apache/datafusion-ballista/issues/2046): TPC-DS with the static planner.
- [#2065](https://github.com/apache/datafusion-ballista/issues/2065): broadcast promotion drops a required sort.
- [#1055](https://github.com/apache/datafusion-ballista/issues/1055): outer join with `CollectLeft`/broadcast.

In 54.1.0 the sort-merge-join broadcast optimization is opt-in "while outstanding issues are resolved" ([54.1 blog](https://datafusion.apache.org/blog/output/2026/08/09/datafusion-ballista-54.1.0/)).

### Fault tolerance

- **A.** No retry code at v3.0.0 (grep for `retry` in `src/`). A lost worker fails the query. The request for workers to reject work before side effects ([#640](https://github.com/datafusion-contrib/datafusion-distributed/issues/640)) was closed on 2026-09-23 without a merged fix, and its PR [#728](https://github.com/datafusion-contrib/datafusion-distributed/pull/728) is still open.
- **B.** The static planner has `max_task_failures` and `max_stage_failures`, re-runs stages after a `FetchPartitionError`, and resets stages on a lost executor (`scheduler/src/state/execution_graph.rs`).
  - AQE is off by default in 54.x, and its path has no executor-failure handling ([#1986](https://github.com/apache/datafusion-ballista/issues/1986)).
  - "Job hangs when all executors are lost" ([#2029](https://github.com/apache/datafusion-ballista/issues/2029)) is still open, although fix [#2212](https://github.com/apache/datafusion-ballista/pull/2212) merged on 2026-08-03. Unverified whether that fix is in 54.1.0.
  - The scheduler is a single point of failure (Spice built HA on top).

### Kubernetes

- **A.** Bring your own: a `WorkerResolver` (a kube-rs example exists), a `ChannelResolver` for TLS and auth, and worker versioning for rolling deploys ([docs index](https://github.com/datafusion-contrib/datafusion-distributed/tree/v3.0.0/docs/source)).
- **B.** Documented manifests with a PV/PVC and optional KEDA autoscaling ([kubernetes.md 54.1](https://github.com/apache/datafusion-ballista/blob/54.1.0/docs/source/user-guide/deployment/kubernetes.md)). Each executor needs a shuffle volume.

### Published performance (vendor numbers; we reproduce, we do not trust)

- **A.** 12 × c5n.2xlarge (8 vCPU, 21 GiB, up to 25 Gbps), Parquet on S3, sum of p50 over the queries every engine completed. TPC-H SF100 (19 queries): A 42 s, Trino 93 s, Ballista 237 s, Spark 261 s ([README v3.0.0](https://github.com/datafusion-contrib/datafusion-distributed/blob/v3.0.0/README.md); tooling in [datafusion-distributed-dev-tools](https://github.com/gabotechs/datafusion-distributed-dev-tools)). It publishes no SF1000 result (none found). Which release produced the numbers is unverified.
- **B.** TPC-H SF1000, 32 executors × (8 vCPU, 64 GiB, 1000 GiB gp3 PVC for shuffle), single iteration: 817.07 s, against Spark 3.4 at 845.21 s. Measured at `main` 67b3a19b, refreshed 2026-09-03 ([benchmarking.md](https://github.com/apache/datafusion-ballista/blob/main/docs/source/contributors-guide/benchmarking.md)). That commit postdates 54.1.0 and may be on DF 55 (unverified). The 54.0 blog acknowledges a scaling gap between SF100, where Ballista is competitive, and SF1000.

## Decision drivers

CodeLake's goal is the most performance for the least compute. Distribution costs compute (serialization, network, duplicated build sides), so it is justified only where one node cannot do the work or cannot do it in time. Every number below is therefore reported with the compute it took, and the distributed mode is compared with single-node CodeLake on the same total cores and memory.

In priority order:

1. Zero wrong results against Trino 431 and single-node CodeLake.
2. The per-worker ceiling holds, measured as cgroup `memory.peak` ≤ container limit, with exhaustion as a clean `EXCEEDED_LOCAL_MEMORY_LIMIT`.
3. The M4 DoD: SF1000, E(1→4) ≥ 0.7.
4. Fits the DF 54 train without a second major, and does not become the slowest crate.
5. Our rules, guards and codecs run in as few places as possible.
6. Compute per query: CPU-seconds and GiB-seconds of peak memory, next to wall time.
7. Integration and operating cost.
8. Fault tolerance.

## Decision (recommended, pending the benchmark)

Adopt **datafusion-distributed 3.0.0** (`=3.0.0`, joining the ADR 0002 train block together with `datafusion-proto =54.1.0`) as the F14 candidate. Keep **Ballista 54.1.0** as the documented fallback. Nothing merges to `main` before the benchmark gates below pass.

Why A:
- **Rules and plans stay in one process.** Our planner and all 25 logical and physical rules keep running once, in the coordinator we already have. Workers only decode and execute. With B, the rules move into a scheduler, and every `TableProvider` also needs a logical codec.
- **The memory model fits.** One `CeilingPool` per worker plugs in through `with_runtime_env`. B's default (a pool per task slot) contradicts ADR 0003 and needs a custom runtime producer to undo.
- **Streaming shuffle.** It suits the interactive Trino-like workload, needs no shuffle volumes, and keeps all intermediate data inside a pool we control, once the gaps below are closed.
- **Fewer open wrong-result issues.** A has no open wrong-result issue for its shipped planner (it has #310 and the unenforced invariant of #745). B has three open.

Single-node stays the default. The distributed planner is behind a `distributed` cargo feature and a configuration switch, and a query whose estimated input fits one node runs on one node: the coordinator distributes only above a threshold that S1 and P1 measure. A cluster that is idle costs no more than one node.

Conditions attached to A. These are defects we fix, not reasons to reject A:
1. **Account for the library's buffers.** Add `WorkerConnection`, `NetworkBoundary`, `BroadcastExec[` and `PartitionSampler[` to `CeilingPool`'s classification (`EXCHANGE_BUFFERS` or a new class), so their unchecked growth counts in the buffer slice and does not dilute the shares. Size `worker_connection_buffer_budget_bytes` from the worker's limit.
2. **Broadcast joins off** (`broadcast_joins = false`) until broadcast build sides are reserved with `try_grow`, which needs an upstream PR or a patch, and evicted ([#726](https://github.com/datafusion-contrib/datafusion-distributed/pull/726)).
3. **Rule ordering (S1).** `AccountedMergeExec` and `ReservedOutputExec` must end up at the head stage, and the join guards must survive boundary injection. If the injector does not see through our nodes, we move our output rule after distribution (a query planner that wraps A's).
4. **Federated scans stay one task, pinned to the coordinator's node.** A cluster-wide connection cap per source is required either way.
5. **Writes commit on the coordinator only.** The Hive commit (`flock`, rename) never runs on a worker.
6. **Reads retry, writes do not.** A lost worker fails the query. The coordinator retries read-only statements once; writes do not retry until [#728](https://github.com/datafusion-contrib/datafusion-distributed/pull/728) or an idempotent commit exists.

### What overturns the decision

- **Choose B instead if:**
  - A fails gate 1 or 2 in a way that conditions 1–3 cannot fix without forking the planner;
  - or B reaches E(1→4) ≥ 0.7 at SF1000 and A does not;
  - or the target workload turns out to be long ETL on preemptible nodes, where query-level failure is unacceptable. In that case, only once [#2046](https://github.com/apache/datafusion-ballista/issues/2046) and [#2065](https://github.com/apache/datafusion-ballista/issues/2065) are closed or proven irrelevant to our plans.
- **Stay single-node (option 0) and revisit on the DataFusion 56 train if:** both fail.

## Cluster-wide budget (F14 design, both options)

Neither candidate has a memory-aware admission. The design:

- **Per worker, hard.** `CeilingPool(memory.limit)` per worker process. `memory.limit` = 0.75 × container limit − network headroom H. H = `worker_connection_buffer_budget_bytes` × (peer workers × max running queries per worker) + one max message. The formula is a starting point, and P0 measures whether it holds (unverified). The worker's container limit is the ceiling the DoD refers to.
- **Per cluster, admission.** Extend `governance::Admission` (ADR 0016) with a memory scope next to node, resource group and user.
  - Each query declares `query_max_memory` (session property, default = cluster budget ÷ max running queries).
  - The coordinator admits a query only while Σ declared ≤ cluster budget. The cluster budget is Σ worker `memory.limit` × a configurable fraction.
  - The worker pools stay the enforcement. Admission only keeps concurrent queries from failing each other.
  - `query_max_memory` per worker is enforced by a per-query child reservation on each worker: a query-scoped wrapper over `CeilingPool`. This needs design and is not in either library (unverified feasibility).
- **Visibility.** Every worker exports `peak_reserved`, the dd `max_mem_used` metric, spill bytes and cgroup `memory.peak` to the coordinator's `system.runtime` tables.

## Benchmark plan (F13 evidence, F14 DoD)

Harness: extend `scripts/bench/run.sh` and `crates/codelake-bench` ([docs/benchmarks.md](../benchmarks.md)). Results are compared with Trino 431 before any timing, and so are CodeLake's own single-node results. Data comes from [tpchgen-cli](https://crates.io/crates/tpchgen-cli) 3.0.0 or Trino's `tpch` connector. It is written as Parquet in the Hive layout, with `decimal(15,2)` money columns as today. No production data.

**Storage (a blocker specific to the DF 54 train).** `hive-s3` does not exist before DataFusion 56, so workers cannot read the warehouse from an object store. For the benchmark, use one of two layouts:

- **L1 (primary, measures the engine).** A read-only copy of the TPC-H data set on every worker's local NVMe, at the identical path. It is valid for read-only TPC-H only.
- **L2 (secondary, production-like).** One NFS (or CephFS) share mounted on all nodes, with a ReadWriteMany PVC on Kubernetes. Its numbers are reported separately, because they include the file server.

Production scale-out on DataFusion 54 therefore needs a shared POSIX file system. Whether `flock` holds across NFS clients is unverified, which is why condition 5 keeps commits on one node.

**Hardware.** The project's k3s test cluster: 2 nodes of 32 vCPU / 128 GiB, local disks of 5.8 TB and 2.9 TB, 0.9 ms between them, pod network over WireGuard (flannel `wireguard-native`). The benchmark runs in its own namespace with Guaranteed QoS pods and never takes more than 24 vCPU / 96 GiB per node, so the other workloads keep their share. The local 12 GB Docker VM runs P0 at SF1 only.

| Phase | Layout | Per worker | Disk | Network |
|---|---|---|---|---|
| P0 correctness | 1 node, coordinator + 2–3 workers | 4 vCPU / 8 GiB | local | pod network, same node |
| P1 SF100 | coordinator 4 vCPU / 16 GiB + 4 workers, 2 per node; single-node baseline 1 × 16 vCPU / 64 GiB, then 1 × 24 vCPU / 96 GiB | 8 vCPU / 32 GiB | data copy on each node (L1) + spill | pod network across nodes |
| P2 SF1000 | coordinator + 1, 2, 4 workers, 2 per node | 12 vCPU / 48 GiB | data copy on each node (L1) + spill; for B, shuffle | pod network across nodes |

The bandwidth between the nodes is measured (iperf3 pod to pod) before P1 and recorded in the report; WireGuard encryption is part of the measured cost. If SF1000 at 4 workers of 12 vCPU does not fit, the report says so, and the missing points need machines that ROADMAP's owner provides. The size of SF1000 as Parquet is measured with `du` after generation (unverified estimate: several hundred GB). The same pods run A, B and single-node, one after another.

**Phases:**

- **S — spike (before any timing), A and B.** Build with the candidate behind a `distributed` cargo feature of `codelake-server` (never in `codelake-core`; `cargo xtask arch`).
  - S1: the physical plans of the 22 TPC-H queries with our rules on. Record where the boundaries go and whether `AccountedMergeExec`, `ReservedOutputExec` and the join guards survive.
  - S2: codecs for the 13 nodes, plus UDF registration on workers.
  - S3: a federated scan pinned to one node, with `FederationFilterGuard` still active.
  - Exit: the plans are recorded, and the integration cost is measured in engineer-days, not estimated.
- **P0 — correctness and ceiling gates, SF1 and SF10.**
  - Tests:
    - TPC-H 22 and the existing differential corpus ([docs/differential.md](../differential.md)) on options 0, A and B. The comparison is against Trino 431 and single-node CodeLake.
    - A federated suite: a SQL Server test container joined with Hive Parquet. The remote SQL text must be byte-identical to single-node, and the guard must be active.
    - Ceiling tests:
      - Each worker's container limit is set so that single-node CodeLake with the same `memory.limit` spills on Q9/Q18/Q21 (sizes found empirically).
      - Run with `broadcast_joins` both off and on for A. The on run is expected to fail the gate as shipped; it records by how much.
      - Pass: cgroup `memory.peak` ≤ limit on every worker, no `OOMKilled`, exhaustion only as `EXCEEDED_LOCAL_MEMORY_LIMIT`.
    - Admission test: 8 concurrent SF10 queries with a cluster budget that fits 2. Expected: 2 running, the rest queued or refused with `INSUFFICIENT_RESOURCES`, none failing with a memory error.
    - Failure injection: SIGKILL one worker in the middle of Q9. Expected: A fails cleanly within the timeout and a read retry succeeds; B completes. A hang fails the gate.
  - Ballista configuration: executor `--concurrent-tasks` = vCPU; a runtime producer that shares one `CeilingPool`, plus one run with stock `--memory-pool-size` for comparison; sort-based shuffle; work dir on NVMe.
- **P1 — SF100, layout L1, then L2.**
  - 1 warmup and 5 measured repetitions, the harness defaults.
  - Collected: p50/p95 per query, total, CPU-seconds per query (cgroup `cpu.stat` of every pod, coordinator included), peak anonymous memory per worker (20 ms cgroup sampling), `peak_reserved`, spill bytes, network bytes (dd `bytes_transferred`) and shuffle bytes on disk (B).
- **P2 — SF1000, layout L1.**
  - Runs at 1, 2 and 4 workers, same per-worker size. E(n) = T(1) / (n · T(n)) on the total of the queries that completed at every n; per-query E is reported too. T(1) is the distributed binary with one worker, not single-node CodeLake, which is reported separately.
  - If the 1-worker run cannot finish within `BENCH_TIMEOUT_SECS` (spill-bound), the report says so. It then gives E(2→8) = 2·T(2) / (8 · T(8)) as a documented deviation from the DoD, which the ROADMAP owner must accept explicitly.
  - Same metrics as P1.

**Gates.** Any failure disqualifies the option:
1. Zero mismatches in P0/P1/P2.
2. No worker above its ceiling in any phase, and exhaustion is clean.
3. SF1000 completes with E(1→4) ≥ 0.7.
4. Federated pushdown is unchanged, and source connections stay under the cap.
5. The release is pinned exactly on the current train major.

Tie-breaks, in order: CPU-seconds per query at the same wall time, the P1 and P2 totals, the fault-injection outcome, the spike's measured integration cost, and operating burden (processes, volumes, state).

Single-node is also judged: where single-node CodeLake finishes a query within 1.2× the distributed wall time, the report recommends it for that query class, because it uses less compute.

## Results so far (2026-09-27)

Spike and F14 implementation on `front/m4-distributed`; numbers in
`bench/reports/2026-09-27-k3s-sf10.md`.

- **Integration (S1–S3):** datafusion-distributed 3.0.0 builds against the
  train's lock with no duplicate crate. Two library defects and one DataFusion
  defect were found and are patched or worked around
  (`vendor/datafusion-distributed/PATCHES.md`, `docs/upstream/datafusion-distributed-3.md`):
  user codecs never saw functions (the checked `sum`/`avg` would have come back
  unchecked), broadcast build sides bypassed the pool (condition 1, now
  `try_grow`), and a rebuilt session state could lose the engine's functions.
  Condition 3 (rule ordering) was real: the output rule and the nested loop join
  guard hide what the library cuts stages at and normalizes, so the coordinator
  runs them after distribution; before that fix two and four workers returned
  twice the rows on TPC-H Q11, Q15 and Q22.
- **Correctness gate:** 22/22 TPC-H queries match single-node and Trino 431
  (SF 0.01 in the integration tests over Parquet and Hive tables, SF10 on the
  cluster with one and two workers). Federated sources, views and writes stay
  on the coordinator (conditions 4 and 5).
- **Ceiling gate:** each node keeps its `CeilingPool`; broadcast build sides
  are reserved; connection buffers are sized to 1/16 of `memory.limit`. The
  memory-ceiling tests hold.
- **Scaling gate: not measurable on this cluster.** Its two nodes are joined
  by 0.33 Gbit/s (pod to pod, WireGuard), against the ≥ 10 Gbit/s this ADR's
  plan requires. Scans scale with a second worker (Q01 3.3 s → 1.5 s); joins
  that shuffle between the nodes get several times slower; four workers did
  not fit the cluster's free CPU. E(1→4) stays open until a machine set with
  the planned network exists.
- **Compute:** the same work surfaced a single-node cost that dwarfs what
  distribution could give here: returning every freed page at once cost 2.6×
  the time and 2.8× the CPU (ADR 0003, amended). Single-node CodeLake now runs
  TPC-H SF10 in 19.8 s with 178 s of CPU on 16 vCPU, against Trino 431's 24.4 s
  and 243 s.

Status stays **Proposed**: the scaling gate has not run. Single node remains
the default; the `distributed` feature and `[distributed]` configuration ship
for deployments whose data does not fit one node and whose network meets the
plan.

## Consequences

- The chosen crate, `datafusion-proto` and a production `tonic`/`arrow-flight` tree join the ADR 0002 block and the `train` gate. This reverses ADR 0010's "no gRPC tree" for the distributed feature only. mTLS on worker channels brings a TLS stack for tonic. A second TLS stack next to tiberius's `native-tls` is likely; which one is unverified.
- Both candidates' DF 54 lines are frozen. Fixes we need before the DataFusion 56 train are carried as a tag-based patch and listed in `docs/upstream/`.
- The M4 DoD can be measured on DF 54 with layout L1. Production multi-node use on DF 54 needs a shared POSIX warehouse. S3 warehouses wait for DataFusion 56 (`hive-s3`).
- `CeilingPool` gains a classification for the distributed library's buffers. `governance::Admission` gains a memory scope. The distributed layer lives in `codelake-server` and `codelake-connectors`, never in `codelake-core`. Codecs live with the crate that owns each plan node.
- A follow-up ADR records the numbers and moves this one to Accepted or Rejected.
