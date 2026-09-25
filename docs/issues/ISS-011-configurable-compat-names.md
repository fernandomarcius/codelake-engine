# ISS-011 — Compatibility names configurable, CodeLake names by default

- Kind: compatibility · Accepted by: Fernando Farias, 2026-09-24 · Status: open (rename front pending)
- Accepted: protocol header prefix (default `X-CodeLake-`), reported version, hive metastore file names (default `.codelakeSchema`/`.codelakePermissions`) and view marker configurable; old names only in user config; a migration command renames existing metadata.
- Risk: existing clients and warehouses need configuration or migration; a missed rename breaks a client silently.
- Revisit when: the rename front merges; each client system is ported.
