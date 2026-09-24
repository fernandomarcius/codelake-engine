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
2. **Hive-style Parquet catalog** (R4–R7): directory per table on local disk or
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
4. **Trino function library** (R-SQL): Trino 431's functions by category
   (date/time, string, binary, math, conditional, aggregate, array/map, JSON,
   URL) with Trino semantics, each with a golden test against Trino 431. Every
   function the acceptance pipelines use is the minimum, not the scope.
5. **Client authentication** on both listeners (SCRAM-SHA-256 for PostgreSQL wire,
   password/JWT for the Trino protocol) and table ownership semantics (R11).
6. **Query governance** (R12): per-query timeout, admission queue, per-user limits.
7. **OTLP traces** with correlation ID across client → engine → source.

**DoD:**
- `gates.sh fast` + `integration` green, including SQL Server and Hive catalog suites.
- **Shadow run:** the full dbt project of `reference-dbt-project` (213 models, 82 tests)
  runs with `dbt build` through dbt-trino against CodeLake, and every output table
  matches the Trino run (row count + `checksum` per table), three consecutive days.
- Peak RSS of CodeLake during the hourly full run ≤ 50% of Trino's on the same host.

## M2 — Production cut-over

- Canary: CodeLake serves the 2-minute micro-batch while Trino keeps the heavy
  21h/03h runs; compare end-to-end latency (target ≤ 5 min, median today 2.5 min).
- Compaction without `docker exec` (R9): built-in `ALTER TABLE … EXECUTE compact`
  that preserves row count and columns, and re-seeds the sink checkpoint.
- Runbooks: memory tuning, spill disk sizing, source outage, rollback to Trino.

**DoD:** 14 days serving production with zero wrong-result incidents and SLO met.

## M3 — Lakehouse (Iceberg) and dbt merge

- Iceberg write path: CTAS, append, **overwrite/row-delta (MERGE)** — upstream
  contribution to `iceberg-rust` or a maintained fork.
- Ship the Iceberg connector: blocked on an `iceberg-rust` release with
  `opendal ≥ 0.59` (clears RUSTSEC-2026-0194/0195 from `quick-xml`).
- Persistent views in the catalog; `information_schema` completeness for dbt.
- dbt incremental `merge` and `delete+insert` strategies.

**DoD:** dbt project with `merge` models passes the same shadow-run comparison.

## M4 — Distributed execution

A single node with 32–64 cores and NVMe spill handles hundreds of GB per query;
TB-scale joins need scale-out.

- Evaluate Ballista vs `datafusion-distributed` (Arrow Flight shuffle) with TPC-H
  SF100/SF1000; ADR with the numbers.
- Coordinator/worker split; the core is already stateless per node.
- Memory ceiling per worker plus a cluster-wide admission budget.

**DoD:** TPC-H SF1000 completes on N workers with linear-ish scaling (≥ 0.7
efficiency from 1 → 4 workers) and no worker exceeding its memory ceiling.

## Later

Oracle (ODBC or `oracle` crate over ODPI-C), Redshift dialect, Hive Metastore
(Thrift), ORC reader, Arrow Flight SQL endpoint, SQLite.
