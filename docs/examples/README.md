# Examples

Configuration examples for operators. Every host, catalog, table and user in
them is a placeholder; secrets are referenced by environment variable, never
written.

| File | What |
|---|---|
| [codelake.production.toml](codelake.production.toml) | one production node: listeners, memory ceiling, telemetry, a hive warehouse and federated sources |
| [canary.json](canary.json) | the canary's checks: fixed counts and checksums, rows, bounds, comparison with a reference engine, freshness ([docs/operations/canary.md](../operations/canary.md)) |

The development configuration is [examples/codelake.toml](../../examples/codelake.toml).
