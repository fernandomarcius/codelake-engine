# Roadmap

The target: **a drop-in replacement for Trino** — same client protocol, SQL
dialect, functions, session properties, error codes and table formats — with a
hard memory ceiling instead of a JVM heap, then scale-out.

CodeLake is a general-purpose engine. The two production pipelines described in
[docs/requirements/trino-replacement.md](docs/requirements/trino-replacement.md)
are **acceptance cases**: CodeLake must run them unchanged, but they are a floor,
not the specification. Features follow Trino's contract, with registries that
connectors extend (catalogs, session properties, functions), never shapes
hard-coded for one user.

Every milestone ends with an executable Definition of Done. A milestone is done
when its DoD commands pass in CI, not when the code "looks finished".

## M0 — Skeleton ✅

- Core: config with fail-fast validation, memory ceiling (`FairSpillPool` + bounded
  spill), federated catalogs, connector contract.
- Connectors: PostgreSQL (federated), MySQL (federated), MongoDB; Iceberg REST
  (experimental, not shipped).
- Server: PostgreSQL wire, `/livez` `/readyz` `/healthz` `/metrics`, graceful drain.
- Every gate in [docs/gates.md](docs/gates.md) wired locally and in CI.
- Correctness guard for a silent wrong-result defect in `datafusion-federation`
  0.5.3 (see `crates/codelake-core/src/federation_guard.rs`).

**DoD:** `scripts/dev.sh scripts/gates.sh fast` and `… integration` pass.

## M1 — Trino drop-in: read, write, clients

Blocking items, in order:

1. ✅ **SQL Server connector** (R1, F1): `tiberius` (TDS 7.3+), per-source
   pushdown policy (ADR 0006), T-SQL dialect, `varbinary(10)` LSNs, `$`
   identifiers, real column spelling for case-sensitive collations, watermark
   subqueries pre-evaluated and pushed as `0x…`. Independent acceptance suite
   green against SQL Server 2019 and 2022 with real CDC.
   Follow-up: route the PostgreSQL wire path through `Engine::sql`; apply a
   pushdown policy to MySQL (same collation hazard) before any reference use.
2. 🔧 **Hive-style Parquet catalog** (R4–R7, F2) — implemented; the red-team's
   critical and high findings are fixed (ADR 0007, "Red-team corrections";
   probes in `hive_redteam.rs`), and so are the second red-team's (W1: exact
   `INT96` timestamps in NANOSECONDS scans and `OPTIMIZE`, `char(n)`
   comparisons refused wherever they are, in-memory tables inside the pool;
   "Red-team W1 corrections", `hive_redteam_w1.rs`), except owner-based
   security (R11, W1-6), which waits on authentication (item 5, F5).
   `INT96` time zone per catalog (`parquet_time_zone`, default
   `America/Sao_Paulo`, ISS-003), proven row by row against Trino 431 set to
   the same zone: directory per table on local disk or
   S3/MinIO, file-based metastore; `CREATE SCHEMA`, CTAS `WITH (format='PARQUET')`,
   `INSERT INTO … SELECT` appending **one new file per insert**, `DROP TABLE`,
   `CREATE/DROP VIEW`, `ALTER TABLE ADD COLUMN`. Output readable by SQL Server 2022
   `OPENROWSET` (verified in an integration test).
3. 🔧 **Trino client protocol** (R10, F3) — implemented; independent verification against
   Trino 431 found 40 defects (dbt-trino, prepared statements, JDBC metadata, cancel of
   CPU-bound queries, value encoding), being fixed: `POST /v1/statement`, `nextUri` paging
   with idempotent retries, cancel, `X-Trino-*` session headers, `USE` / `SET
   SESSION` / `SHOW SESSION`, Trino error codes, `system.runtime.queries`, and a
   session-property registry that connectors extend (ADR 0012). Proven with the
   Trino 431 CLI and the Python `trino` client (dbt-trino's DB-API).
   Follow-up: prepared statements (`EXECUTE IMMEDIATE`), TLS on the listener.
4. ✅ **Trino function library** (R-SQL, F4): Trino 431's functions by category
   (date/time, string, binary, math, conditional, aggregate, array/map, JSON,
   URL) with Trino semantics, each with a golden test against Trino 431. Every
   function the acceptance pipelines use is the minimum, not the scope (ADR 0008).
   Evidence: `scripts/gates.sh integration` on integ/wave1 9f8950a →
   `f4_acceptance` 39/39 passed; `scripts/gates.sh test` → every
   `trino_functions` golden and unit test passed.
5. **Client authentication** on both listeners (SCRAM-SHA-256 for PostgreSQL wire,
   password/JWT for the Trino protocol) and table ownership semantics (R11).
   **Stopped (2026-09-27, owner):** authentication will come from the external identity service; this item is not implemented in CodeLake (ISS-053).
6. ✅ **Query governance** (R12, F6) — implemented in the core for both client
   protocols (ADR 0016): one query registry (`system.runtime.queries` lists
   PostgreSQL wire and HTTP queries), `query_max_run_time` /
   `query_max_execution_time` with configured defaults (`statement_timeout` on
   the PostgreSQL wire) that stop the operators and return their memory to the
   pool, a bounded admission queue (`QUERY_QUEUE_FULL`, SQLSTATE `53400`), a
   queue timeout, per-user and resource-group concurrency, Prometheus metrics,
   and stopping a query from outside (`CALL system.runtime.kill_query` on both
   protocols, PostgreSQL cancel requests, `DELETE /v1/query/{id}`).
   Proven with `tokio-postgres` and the Python client against the binary
   (`tests/governance.rs`: pool back at zero after a timeout, a cancel
   request, a kill and a `DELETE`). Evidence: `scripts/gates.sh integration`
   on front/f6-governance 259dbf1 → 285/285 passed, `governance` 1/1;
   `scripts/gates.sh fast` → 714 passed. Not covered: memory-based admission, group
   hierarchy and weights. Pending: execution shares the Tokio workers with
   the protocol listeners, so a CPU-heavy query delays protocol requests
   (ISS-009); a dedicated CPU runtime needs a row channel between runtimes on
   the PostgreSQL wire and a new worker budget (ADR 0016, Consequences) —
   measure protocol latency under CPU saturation first.
7. ✅ **OTLP traces** (F7) with correlation ID across client → engine → source
   (ADR 0010). Evidence: `scripts/gates.sh integration` on integ/wave1 9f8950a →
   `f7_acceptance` 9/9 and `otlp` 2/2 passed.

**DoD:**
- `gates.sh fast` + `integration` green, including SQL Server and Hive catalog suites.
- **Shadow run:** the full dbt project of `reference-dbt-project` (213 models, 82 tests)
  runs with `dbt build` through dbt-trino against CodeLake, and every output table
  matches the Trino run (row count + `checksum` per table), three consecutive days.
- Peak RSS of CodeLake during the hourly full run ≤ 50% of Trino's on the same host.
- 🔧 Tooling for both (F8, ADR 0015, [docs/shadow-run.md](docs/shadow-run.md)):
  `codelake config import` (catalog `.properties` → `codelake.toml`, secrets to a
  0600 env file), `${ENV:NAME}` references in `codelake.toml`, and the harness
  `scripts/shadow-run.sh` (same cutoff on both sides, DuckDB checksums per table
  and column, declared-type and name-case differences classified by documented
  rules without ever masking a value, peak RSS per side, reports per cycle and
  cumulative). Evidence:
  `tools/shadow/selftest.sh` (SQL Server 2022 CDC, Trino 431, CodeLake) — two
  cycles without divergence, every injected mutation detected. Findings that
  block the real run (docs/shadow-run.md): SQL Server `datetime` values are
  read as the exact 1/300 s tick instead of the millisecond the reference
  reads (wrong values in filters and `date_diff`), `datetime2(7)` is
  truncated, and every one arrives as `timestamp(6)`, which a `MILLISECONDS`
  hive CTAS refuses (F8-1); SQL Server `varchar(n)` loses its length in CTAS
  (F8-2); window `ORDER BY` on varbinary fails (F8-3); a SQL Server table
  created after start is not found until a restart (F8-5). Fixed: SQL Server
  columns in name order instead of the source's (F8-4). Pending: those fixes,
  then the run itself in homologation (a person, three days).

## M2 — Production cut-over

- Canary: CodeLake serves the 2-minute micro-batch while Trino keeps the heavy
  21h/03h runs; compare end-to-end latency (target ≤ 5 min, median today 2.5 min).
  Before it: set each hive catalog's `parquet_time_zone` to the production
  Trino's `hive.parquet.time-zone` (or its JVM zone), or every `INT96`
  timestamp reads shifted (ADR 0007, ISS-003).
  The tooling is ready (F10, below); running it in production is the human gate.
- ✅ Compaction without `docker exec` (R9, F9): Trino's `ALTER TABLE … EXECUTE
  optimize`, rewriting with the same Trino 431 Parquet writer (INT96) as
  `INSERT`, preserving row count and columns, crash-safe (ADR 0009).
  Evidence: `scripts/gates.sh integration` on integ/wave1 9f8950a →
  `hive_optimize` 2/2 (Trino 431 round trip) and `f9_acceptance` 10/16 passed;
  the 6 failures are test SQL Trino 431 rejects since F4 (`md5(varchar)`,
  `to_timestamp_millis` stored in hive), not OPTIMIZE, and wait on the
  testers (with Trino SQL, 15/16 passed). Not in F9: re-seeding the sink
  checkpoint after a compaction. Since red-team W1 (W1-3/W1-7), `OPTIMIZE`
  keeps every stored instant to the nanosecond, whatever the catalog's
  precision (ADR 0009).
- ✅ Runbooks and canary (F10): [docs/operations](docs/operations/README.md)
  covers installation (Compose and Kubernetes), configuration (memory ceiling,
  spill, catalogs, listeners without client authentication), upgrade and
  rollback, sizing from the benchmark reports, incidents (stuck query,
  `EXCEEDED_LOCAL_MEMORY_LIMIT` and OOM kills, spill full, source down, missing
  file during `OPTIMIZE`, queue full, latency), compaction with one compacting
  engine per warehouse, backup and restore of the file metastore and
  warehouse, observability, migration from Trino with cut-over and the way
  back, and the go-live checklist. `scripts/ops/canary.sh` checks recorded
  answers (value, row count, sha256, rows, bounds) or equality with a
  reference engine, latency and freshness objectives, prints one JSON line per
  run and exits ≠ 0 on any failure. Evidence: `scripts/ops/canary-selftest.sh`
  24/24 (bash 3.2 and 5.2); one run against a local engine: 7 checks ok, a
  deliberate mismatch and a timeout reported (exit 1), the timed-out query
  `USER_CANCELED` on the server and the pool back to 0 bytes. Pending
  (human): review of the runbooks, deployment in HML/PRD, the cut-over window
  and the DoD period below.

**DoD:** 14 days serving production with zero wrong-result incidents and SLO met
(procedure and evidence: [go-live checklist](docs/operations/go-live.md);
production only, not done by CI).

## M3 — Lakehouse (Iceberg) and dbt merge

- Iceberg write path: CTAS, append, **overwrite/row-delta (MERGE)** — upstream
  contribution to [`apache/datafusion-iceberg`](https://github.com/apache/datafusion-iceberg),
  which takes over the DataFusion integration that `iceberg-rust` removed from
  `main` (0.11.x is its last release with `iceberg-datafusion`), or a maintained fork.
- Ship the Iceberg connector: out of the build since 2026-09-24 (ADR 0013).
  Returns with an Iceberg integration published for the train's DataFusion
  major (`iceberg` 0.11.0 final, still at RC on 2026-09-24, or
  `apache/datafusion-iceberg`) on `opendal ≥ 0.58.1` (quick-xml ^0.41, clears
  RUSTSEC-2026-0194/0195).
- Dependency train (F11): on DataFusion 54.1 since 2026-09-24, with
  `datafusion-federation = "=0.5.5"`, the per-connector
  `datafusion-table-providers` 0.13.1 crates and `datafusion-postgres` 0.18.0,
  Iceberg left out (ADR 0002, ADR 0013). The join defects the train brought
  (null-aware anti join, nested loop join spill fallback) and the string/number
  coercion change are guarded in the engine (ADR 0014). A hash join whose
  estimated build side exceeds its slice of the pool runs as a spilling
  sort-merge join (ADR 0003, `sort_merge_oversized_joins`); one without
  statistics still fails cleanly. Codepoint
  `lpad`/`rpad`/`translate` and `approx_percentile` match Trino 431. Pending:
  correlated scalar subqueries (`scalar_subquery_to_join` fails, a refusal, not
  a wrong result). Skip DataFusion 55 (no `datafusion-table-providers` or
  `datafusion-postgres` for it).
- `hive-s3` (S3/MinIO warehouses) waits for DataFusion 56: DataFusion 53–55 need
  `object_store` ^0.13 (quick-xml ^0.39); DataFusion 56 moves to
  `object_store` 0.14.2 (quick-xml ^0.41).
- Persistent views in the catalog; `information_schema` completeness for dbt.
- dbt incremental `merge` and `delete+insert` strategies.

**DoD:** dbt project with `merge` models passes the same shadow-run comparison.

## M4 — Distributed execution

A single node with 32–64 cores and NVMe spill handles hundreds of GB per query;
TB-scale joins need scale-out.

- Evaluate Ballista vs `datafusion-distributed` (Arrow Flight shuffle) with TPC-H
  SF100/SF1000; ADR with the numbers. ADR 0011 (Proposed, revised for the
  DataFusion 54 train) makes `datafusion-distributed` 3.0.0 the candidate and
  Ballista 54.1 the fallback, and sets the benchmark plan and decision gates;
  single-node stays the default, and nothing merges before the benchmark runs.
  Spike S1 done (branch `front/m4-distributed`): the 22 TPC-H queries on a
  coordinator and three workers match single-node; two library defects and
  one DataFusion defect found and patched (docs/upstream/datafusion-distributed-3.md).
- Coordinator/worker split; the core is already stateless per node.
- Memory ceiling per worker plus a cluster-wide admission budget.

**DoD:** TPC-H SF1000 completes on N workers with linear-ish scaling (≥ 0.7
efficiency from 1 → 4 workers) and no worker exceeding its memory ceiling.

## Later

Oracle (ODBC or `oracle` crate over ODPI-C), Redshift dialect, Hive Metastore
(Thrift), ORC reader, Arrow Flight SQL endpoint, SQLite.
