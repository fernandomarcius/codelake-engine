# Changelog

All notable changes are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- Engine core with hard memory ceiling (`FairSpillPool`) and bounded spill.
- Fail-fast configuration validation reporting every problem at once; secrets via environment variables only.
- Connectors: PostgreSQL and MySQL (federated pushdown), MongoDB; Iceberg REST (experimental, not shipped).
- Server: PostgreSQL wire protocol, `/livez`, `/readyz`, `/healthz`, `/metrics`, graceful drain on SIGTERM.
- Quality gates: clippy pedantic, nextest, Testcontainers integration, architecture and version-train checks,
  coverage, mutation testing, criterion regression gate, cargo-deny, CodeQL, Trivy, fuzzing, signed releases with SBOM.

- SQL Server connector (2008+ via TDS 7.3): CDC change tables, LSN watermarks pushed as T-SQL
  binary constants, real column spelling for case-sensitive collations.
- Per-source pushdown policy: operators whose semantics differ at the source (text collation,
  `AVG`, `int` `SUM`, decimal division, NULL ordering) run in the engine (ADR 0006).
- `Engine::sql` pre-evaluates uncorrelated scalar subqueries in predicates so their values can
  be pushed down.

### Fixed
- Filters above cross-source joins now reach their source (extra `push_down_filter` before federation).
- Wrong results when a filter sat above a federated scan in cross-source joins (datafusion-federation 0.5.3); see ADR 0005.
