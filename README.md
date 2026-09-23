# CodeLake

Federated SQL engine in Rust with a **hard memory ceiling**. Query SQL Server,
PostgreSQL, MySQL, MongoDB and (experimental) Iceberg on S3/MinIO as one database, join
across them, and never let the process grow past the memory you gave it.

Built on [Apache DataFusion](https://datafusion.apache.org/) 53,
[`datafusion-federation`](https://github.com/datafusion-contrib/datafusion-federation) and
[`datafusion-table-providers`](https://github.com/datafusion-contrib/datafusion-table-providers).
Goal: replace Trino in production workloads at a fraction of the memory
(see [ROADMAP.md](ROADMAP.md)).

> **Status: 0.1 skeleton.** Read path for SQL Server, PostgreSQL, MySQL and
> MongoDB works and is tested against real databases. The PostgreSQL wire listener has **no client
> authentication yet**, so the server refuses to bind it beyond loopback unless you
> opt in explicitly (see [Security](#security)).

## Why

| | Trino (JVM) | CodeLake |
|---|---|---|
| Memory ceiling | heap + off-heap + GC headroom | one pool, `memory.limit`, enforced per operator |
| Over the limit | query killed or node OOM | operators spill to disk; if spill is exhausted, **only that query** fails |
| Idle footprint | GBs of heap | to be measured in M1 (shadow run compares peak RSS) |
| Execution | vectorized JVM | vectorized Arrow, native code, no GC pauses |

## How it works

```
 clients (psql, BI, dbt*)             ops: /livez /readyz /healthz /metrics
        │ PostgreSQL wire                          │ HTTP
 ┌──────▼──────────────────────────────────────────▼─────┐
 │ codelake-server                                        │
 ├────────────────────────────────────────────────────────┤
 │ codelake-core   SQL → logical plan → federation        │
 │                 ─ sub-plans pushed down to each source │
 │                 ─ cross-source joins run here, inside  │
 │                   FairSpillPool(memory.limit) + spill  │
 ├───────────┬───────────┬────────────┬───────────────────┤
 │ sqlserver · postgres · mysql · mongodb · iceberg (exp.) │  codelake-connectors
 └───────────┴───────────┴────────────┴───────────────────┘
```

Each source is a SQL catalog: `SELECT … FROM erp.public.customers c JOIN crm.app.contracts k …`.
Joins inside one SQL source travel to that source as a single statement; joins
across sources run in the engine under the memory ceiling.

## Quick start

Everything runs in a pinned toolbelt container. You only need Docker.

```bash
scripts/dev.sh cargo build
```

```bash
scripts/dev.sh scripts/gates.sh fast
```

```bash
docker compose -f deploy/compose/docker-compose.yml up -d
```

Configuration is a TOML file plus environment variables for secrets
([examples/codelake.toml](examples/codelake.toml)). Validate it without starting
anything; every problem is reported at once:

```bash
scripts/dev.sh cargo run -p codelake-server -- check-config --config examples/codelake.toml
```

## Quality gates

Every PR must pass all gates **before** human review; a human never overrides a
red gate. Same script locally and in CI: [`scripts/gates.sh`](scripts/gates.sh).
Thresholds and the exception process: [docs/gates.md](docs/gates.md).

| Gate | Tool | Blocks when |
|---|---|---|
| Format / lint | rustfmt, clippy pedantic, `-D warnings`, no `unwrap`/`panic` | any finding |
| Unit tests | nextest | any failure |
| Integration | Testcontainers: real SQL Server 2019/2022, PostgreSQL, MySQL, MongoDB | any failure |
| Architecture | `cargo xtask arch` | core depends on a connector/server |
| Version train | `gates.sh train` | two DataFusion/Arrow majors in the graph |
| Coverage | cargo-llvm-cov | lines < 75% |
| Mutation | cargo-mutants on `codelake-core` (HIGH) | score < 80% |
| Performance | criterion + `cargo xtask bench-gate` | > 25% slower than base (10–25% warns) |
| Supply chain | cargo-deny (RustSec, licenses, sources), dependency-review | vulnerable/unmaintained crate, disallowed license |
| Unused deps | cargo-machete | any |
| SAST | CodeQL (Rust + Actions) | finding |
| Secrets / IaC | Trivy fs | CRITICAL/HIGH |
| Image | Trivy image, non-root check | CRITICAL/HIGH, root user |
| MSRV | Rust 1.94 | does not compile |
| Fuzzing (nightly) | cargo-fuzz: config parser, byte sizes | crash |
| Release | SBOM (CycloneDX), cosign keyless, SLSA provenance | — |

## Security

- Secrets never live in the config file: sources name the environment variable
  that holds them, values are wrapped in `SecretString` and redacted from `Debug`.
- TLS to sources is the default (`verify-full` for PostgreSQL, `required` for MySQL).
- Server TLS files are checked at startup; a missing file stops the process
  instead of silently falling back to plaintext.
- No client authentication on the PostgreSQL listener yet (ROADMAP M1). Until
  then `pg_listen` must be loopback, or you set `allow_unauthenticated_network = true`
  and restrict access at the network layer (see `deploy/k8s/networkpolicy.yaml`).
- Container: distroless, non-root (65532), read-only root filesystem, all
  capabilities dropped, base images pinned by digest.

Report vulnerabilities privately: [SECURITY.md](SECURITY.md).

## Repository layout

| Path | Content |
|---|---|
| `crates/codelake-core` | config, memory governance, federation guard, engine |
| `crates/codelake-connectors` | one Cargo feature per source |
| `crates/codelake-server` | `codelake` binary: PostgreSQL wire + ops HTTP |
| `xtask` | architecture and performance gates |
| `fuzz` | cargo-fuzz targets |
| `deploy/k8s`, `deploy/compose` | Kubernetes manifests, local stack |
| `docs/adr` | architecture decisions |
| `docs/requirements` | what replacing Trino actually requires |

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
