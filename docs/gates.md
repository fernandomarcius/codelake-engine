# Quality gates

Trust in CodeLake comes from machine-verified evidence, not from reviewers reading
every line. Therefore:

> **Every gate passes before human review. A human does not override a red gate.**
> If a gate is wrong, fix the gate or register an exception in
> [exceptions.md](exceptions.md). Never suppress it locally.

All gates run through one script, locally and in CI:

```bash
scripts/dev.sh scripts/gates.sh <gate>
```

## Build caches

`scripts/dev.sh` keeps two caches in named Docker volumes:

| Volume | Shared | Holds | Why |
|---|---|---|---|
| `codelake-target` (`CODELAKE_TARGET_VOLUME`) | **no**, one per checkout | cargo's `target/` | cargo decides freshness by mtime; two worktrees sharing a target (both mounted at `/work`) reused each other's binaries and tested code that was not on disk (a false green) |
| `codelake-sccache` (`CODELAKE_SCCACHE_VOLUME`) | yes, every checkout | [sccache](https://github.com/mozilla/sccache) compiler outputs | an entry is keyed by the hash of the compiler, its arguments, its environment and the **content** of every input file, never by mtime; a new worktree compiles its dependencies from the cache |

sccache wraps `rustc` for library crates (dependencies and workspace libraries);
binaries, test harnesses and incremental builds are not cached (`CARGO_INCREMENTAL=0`
in the image). Proof that the cache keys by content:
`scripts/dev.sh scripts/sccache-check.sh` builds two versions of the same file at the
same path, each in its own target, and fails if the second reuses the first's
output (it also checks that the same content, with a new mtime, is a hit). `gates.sh
fast` ends by printing the run's hits and misses.

| Variable | Default | Effect |
|---|---|---|
| `CODELAKE_SCCACHE` | `1` locally, `0` in CI (`CODELAKE_CACHE_DIR` set) | `0` builds without the cache |
| `CODELAKE_SCCACHE_VOLUME` | `codelake-sccache` | volume holding the cache (`$CODELAKE_CACHE_DIR/sccache` in CI) |
| `CODELAKE_SCCACHE_SIZE` | `20G` | ceiling; sccache evicts least recently used entries past it |

Clean it with `docker volume rm codelake-sccache` (nothing may be running with it);
its size: `docker system df -v | grep codelake-sccache`. An image built before
sccache was added keeps working, uncached, with a warning on stderr; rebuild it with
`docker build -f docker/dev.Dockerfile -t codelake-dev:local docker`.

## Gates and thresholds

| Gate | Command | Threshold | When |
|---|---|---|---|
| fmt | `gates.sh fmt` | no diff | every PR |
| clippy | `gates.sh clippy` | zero warnings; `pedantic` on; `unwrap`/`expect`/`panic`/`todo` denied outside tests; `unsafe` forbidden | every PR |
| typos | `gates.sh typos` | zero | every PR |
| arch | `gates.sh arch` | core never depends on connectors/server/drivers | every PR |
| train | `gates.sh train` | one DataFusion/Arrow/Parquet/object_store major | every PR |
| test | `gates.sh test` | 100% pass | every PR |
| integration | `gates.sh integration` | 100% pass against real databases | every PR |
| doc | `gates.sh doc` | rustdoc without warnings | every PR |
| msrv | `gates.sh msrv` | compiles on Rust 1.94 | every PR |
| deny | `gates.sh deny` | no RustSec advisory (vulnerable, unmaintained, yanked), only allowed licenses, crates.io only | every PR + weekly |
| machete | `gates.sh machete` | no unused dependency | every PR |
| differential | `gates.sh differential` (containers through the Docker socket; under `heavy.sh` on a shared machine) | every case as its file expects; known defects listed, never green ([differential.md](differential.md)) | before delivering a front (DoD, below) |
| shadow-run harness | `tools/shadow/test.sh` (Docker: the harness image; not in `gates.sh`, which runs inside the toolbelt) | 100% pass | every PR |
| coverage | `gates.sh coverage` | lines ≥ 75% | every PR |
| mutation | `gates.sh mutants` | by criticality, below | PR (changed code), nightly (full) |
| performance | `cargo xtask bench-gate` | < 10% PASS · 10–25% WARNING · > 25% **BLOCK** | every PR |
| performance vs Trino | `scripts/bench/run.sh` + `cargo xtask bench-gate --e2e` ([benchmarks.md](benchmarks.md)) | same thresholds on CodeLake p50 per case; a case that stops matching Trino **BLOCKS** | before a release, on a reserved machine |
| SAST | CodeQL Rust + Actions | no finding | every PR |
| secrets / IaC | Trivy fs | no CRITICAL/HIGH | every PR + weekly |
| image | Trivy image, `USER 65532` check | no CRITICAL/HIGH fixable CVE | every PR + release |
| dependency review | GitHub | no new HIGH advisory, no copyleft license | every PR |
| fuzz | cargo-fuzz, 10 min per target | no crash | nightly |
| PR title | Conventional Commits | valid | every PR |

## Mutation testing by criticality

| Crate / module | Criticality | Minimum score |
|---|---|---|
| `codelake-core` (memory, config, federation guard, join guards) | HIGH | 80% |
| `codelake-connectors` | MEDIUM | 70% (enforced once connector unit tests exist; integration tests cover today) |
| `codelake-server` | MEDIUM | 70% (same) |

Score = (caught + timeout) / (caught + missed + timeout). Unviable mutants do not count.
Mutants are built and tested with every feature, as the unit-test gate builds the crate: code behind a
feature (`portable`) would otherwise be neither compiled nor tested, and each of its mutants would count as missed.

## Wrong results are the worst defect

A crash is loud; a wrong answer is silent. Any change to planning, pushdown or a
connector needs an integration test that asserts **rows**, not only plans. The
federation guard exists because such a test caught a lost `WHERE` clause.

## Differential cases in the Definition of Done

A front that touches planning, pushdown, a connector, a client protocol or SQL
semantics runs, before delivering:

```bash
heavy.sh scripts/dev.sh scripts/gates.sh differential
```

It adds a case to `tools/differential/cases/` for every wrong result it finds or
fixes, drops the `known` marker of the cases its fix turns `OK` (the gate fails
until it does) and never marks a divergence `known`/`accepted` without the issue or
ADR that records it. The implementer pastes the gate's summary in the delivery; the
verifier reruns it. Format and outcomes: [differential.md](differential.md).

## Supply chain

- Critical/High vulnerabilities block release unless a formal exception exists.
- Release artifacts are produced only by CI, signed (cosign keyless), with SBOM
  (CycloneDX) and SLSA provenance. The same digest is promoted HML → PRD.
- GitHub Actions are pinned by commit SHA; base images by digest.
- Features that fail a supply-chain gate are kept out of `default` and out of the
  release image (today: `hive-s3`, see ROADMAP M3). A connector with no release
  for the current DataFusion train is kept out of the build (today: `iceberg`,
  ADR 0013).

## Exceptions

Allowed only if explicit, approved, traceable, scoped and **time-boxed**. Register
in [exceptions.md](exceptions.md) with: rule, justification, owner, approver,
expiry. `#[allow]`, `ignore` or `skip` without a matching entry is a defect.
Code-level `#[allow]`/`#[expect]` must carry a `reason = "…"`.
