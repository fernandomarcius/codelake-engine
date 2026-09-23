# Governance

- **Maintainers** (`.github/CODEOWNERS`) merge PRs, cut releases and approve gate
  exceptions. Architecture changes require an ADR in `docs/adr`.
- **Decisions** are made in public issues/PRs; when consensus fails, maintainers
  decide and record the reasoning in the ADR.
- **Releases** follow SemVer. Pre-1.0, minor versions may break compatibility; the
  changelog lists every breaking change.
- **Branch protection on `main`:** PR required, all CI gates required, at least one
  maintainer approval, linear history, signed commits recommended.
