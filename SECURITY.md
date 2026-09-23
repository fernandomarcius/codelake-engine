# Security policy

## Reporting a vulnerability

Report privately through GitHub Security Advisories
(<https://github.com/fernandomarcius/CodeLake/security/advisories/new>).
Do not open public issues for vulnerabilities. We acknowledge within 3 business
days and agree on a disclosure date with the reporter.

## Supported versions

Only the latest minor release receives fixes while the project is pre-1.0.

## Known limitations (pre-1.0)

- The PostgreSQL wire listener has no client authentication yet. The server
  refuses to listen beyond loopback unless explicitly allowed; restrict access at
  the network layer (see `deploy/k8s/networkpolicy.yaml`).
- The experimental `iceberg` feature pulls a vulnerable `quick-xml` and is not
  shipped (see `docs/exceptions.md`).

## Hardening in place

Secrets only via environment variables and redacted from logs and `Debug`; TLS to
sources by default; fail-fast on missing TLS files; distroless non-root image with
read-only root filesystem; images pinned by digest and signed; SBOM and SLSA
provenance per release; CodeQL, Trivy, cargo-deny and dependency review on every PR.
