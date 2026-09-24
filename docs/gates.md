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
| `codelake-core` (memory, config, federation guard) | HIGH | 80% |
| `codelake-connectors` | MEDIUM | 70% (enforced once connector unit tests exist; integration tests cover today) |
| `codelake-server` | MEDIUM | 70% (same) |

Score = (caught + timeout) / (caught + missed + timeout). Unviable mutants do not count.

## Wrong results are the worst defect

A crash is loud; a wrong answer is silent. Any change to planning, pushdown or a
connector needs an integration test that asserts **rows**, not only plans. The
federation guard exists because such a test caught a lost `WHERE` clause.

## Supply chain

- Critical/High vulnerabilities block release unless a formal exception exists.
- Release artifacts are produced only by CI, signed (cosign keyless), with SBOM
  (CycloneDX) and SLSA provenance. The same digest is promoted HML → PRD.
- GitHub Actions are pinned by commit SHA; base images by digest.
- Features that fail a supply-chain gate are kept out of `default` and out of the
  release image (today: `iceberg`, see ROADMAP M3).

## Exceptions

Allowed only if explicit, approved, traceable, scoped and **time-boxed**. Register
in [exceptions.md](exceptions.md) with: rule, justification, owner, approver,
expiry. `#[allow]`, `ignore` or `skip` without a matching entry is a defect.
Code-level `#[allow]`/`#[expect]` must carry a `reason = "…"`.
