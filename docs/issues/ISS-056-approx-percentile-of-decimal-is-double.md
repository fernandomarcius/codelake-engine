# ISS-056 — `approx_percentile` of a decimal is `double`

- Kind: deliberate divergence · Decided by the owner, 2026-09-28 · Status: open
- Context: Trino 431 has no `approx_percentile` for `decimal`; it coerces the argument and returns
  `real` (32-bit float). CodeLake returns `double`. Found by the shadow run on production's
  observability models: `kpi_freshness.cdc_age_p50_horas` and `cdc_age_p95_horas` are
  `approx_percentile(idade_horas, …)` of `date_diff(…) / 3600.0`, a decimal, and were the only
  difference of the routine once every run compared only its own rows (values equal, types
  `real` and `double`).
- Decided: CodeLake keeps `double`. A `real` keeps about 7 significant digits and protects no
  data; the `double` holds every value the `real` does.
- Risk: a consumer that declares the column as `real` (a typed `CREATE TABLE` receiving an
  `INSERT … SELECT`) converts it; a reader of the Parquet sees `DOUBLE` where the reference wrote
  `FLOAT`.
- Shadow run: declared as an expected `declared_type` difference for those two columns
  (`real` → `double`); their values are still compared.
- Revisit when: a consumer depends on the `real` type.
- References: shadow run on `.47`, routine `obs`, cycle of 2026-09-28 15:17.
