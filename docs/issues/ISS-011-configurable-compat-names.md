# ISS-011 — Compatibility names configurable, CodeLake names by default

- Kind: compatibility · Accepted by: Fernando Farias, 2026-09-24 · Status: open (rename front pending)
- Accepted: protocol header prefix (default `X-CodeLake-`), reported version, hive metastore file names (default `.codelakeSchema`/`.codelakePermissions`) and view marker configurable; old names only in user config; a migration command renames existing metadata.
- Risk: existing clients and warehouses need configuration or migration; a missed rename breaks a client silently.
- Revisit when: the rename front merges; each client system is ported.
- Done 2026-09-27 (branch `front/rename-names`): `[server.trino] header_prefix` (default `X-CodeLake-`) and `reported_version` (default the CodeLake version); hive `[source.metastore_names]` (metadata file, permissions directory, view marker and parameter, version parameter, writer version; CodeLake names by default); `codelake hive migrate-names` (dry run by default, refuses the whole plan on a conflict); the shadow harness reads each side's metadata file name. The reference engine's names appear only in configuration, tests and docs. Remaining for this issue: the mechanical rename of modules, types and messages that still say Trino in `src/` (second pull request).
