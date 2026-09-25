# ADR 0015 — Shadow-run harness and configuration references

- Status: Proposed (F8)
- Date: 2026-09-25
- Related: ROADMAP M1 (DoD: shadow run), ADR 0007 (hive layout), ADR 0012
  (client protocol), docs/shadow-run.md

## Context

The M1 Definition of Done is a shadow run: the same dbt project runs through
dbt-trino against the reference engine and against CodeLake, every output
table matches by row count and checksum on three consecutive days, and
CodeLake's peak RSS stays at or below half of the reference's. The run itself
happens in a homologation environment and is a human's job; this front
delivers the tool it needs and proves the tool on disposable containers.

Three things were missing:

1. A way to configure CodeLake from what operators already have: catalog
   `.properties` files (`connector.name=…`), where passwords are often inline
   or written as `${ENV:NAME}`.
2. A way for `codelake.toml` to take non-secret values (hosts, listen
   addresses, sizes, paths) from the environment, so one file serves every
   environment. Other fronts need the same mechanism.
3. A harness that runs both sides with the same inputs, compares every table
   without trusting either engine, and measures memory the same way on both.

## Decision

### `${ENV:NAME}` in `codelake.toml` (`codelake_core::env_refs`)

- Any string value may hold `${ENV:NAME}` references (whole or embedded);
  they are resolved once, when the file is loaded (`Config::load`, used by
  `serve` and `check-config`). Every `${ENV:` starts a reference, with no
  escape, as in catalog files. Only string values:
  numbers and booleans stay literals, keys are never resolved, resolution is
  not recursive.
- An unset or empty variable is an error naming the key and the variable
  (`source[0].host references environment variable ERP_HOST, which is not
  set`); every missing variable is reported at once, as the rest of the
  validation does. A resolved value never appears in an error: parse errors
  after resolution have resolved values replaced by their reference.
- A document without references is parsed exactly as before, so its errors
  keep their line numbers.
- Secrets keep their `*_env` keys, whose values become `SecretString`.
  References are for non-secret values: a resolved value is ordinary
  configuration (it can appear in `Debug` output of the config).

This is the one mechanism for environment references in the file; new
configuration sections use it instead of inventing their own.

### `codelake config import`

- Maps `sqlserver`, `postgresql` and `hive` with `hive.metastore=file` (local
  paths and S3) catalogs to `[[source]]` entries. Anything else (thrift
  metastore, iceberg, mongodb, other connectors, files that do not parse) is
  skipped with a reason that names settings, never values; the files are still
  written, and the exit code is 3.
- Sensitive settings (password, secret, access key, token, credentials,
  private key, key store) and credentials embedded in a JDBC URL go to an env
  file written atomically with mode 0600; the TOML names the variable. A
  secret written `${ENV:NAME}` becomes `…_env = "NAME"` and is not copied.
  Credentials embedded in a host or URL (`user:pass@host`) are refused.
- Non-secret `${ENV:NAME}` references are written as they are and resolved by
  the server at load. The import never reads the environment. Where the
  import must read a value to map it (a port, a boolean, the scheme of a
  warehouse), a reference is refused with a reason.
- `hive.parquet.time-zone` becomes `parquet_time_zone`. A hive catalog
  without it takes the zone the operator names with `--reference-time-zone`
  (the reference engine's JVM zone, which decides its `INT96` timestamps);
  without that flag the catalog is skipped. The import never guesses a zone:
  CodeLake's default (`America/Sao_Paulo`) and the stock reference image's
  (UTC) differ, and a wrong one shifts timestamps silently.
- The generated document is parsed and validated before anything is written.

### Harness (`tools/shadow`, `scripts/shadow-run.sh`)

- Python in its own image: dbt-core/dbt-trino 1.8 (the minor the acceptance
  pipelines run) and DuckDB, every distribution pinned by hash; base image by
  digest. The comparison does not go through either engine: DuckDB reads the
  Parquet files of both warehouses directly.
- **Same inputs.** Each cycle reads one cutoff (a watermark such as the
  highest CDC LSN, from `SHADOW_CUTOFF_SQL` on the reference or a fixed
  value) and passes the same value to both sides as a dbt variable; the
  project bounds its reads with it. The project is copied once per
  invocation, so both sides run the same files. Sides run one after the other
  (lockstep); a cycle compares only after both finished. The two sides must
  write to different destinations (refused otherwise).
- **Comparison.** Per table: row count; a checksum that ignores row order and
  counts duplicates (sum of a 128-bit hash of each row's canonical text);
  a checksum per column to say which columns differ. Canonical values:
  decimals and integers by value, timestamps as microseconds (naive read as
  UTC), strings exact (no trimming), binary as hex, NULL distinct from `''`.
  Declared types (each table's metastore file, or the Parquet schema when
  either side has none) that differ are divergences, as are table or column
  names that differ only by case, unless a rule of the expected-differences
  file documents them (below). Storage-type differences under equal declared
  types (INT96 vs INT64 timestamps) are reported as warnings. Table
  directories are found without regard to case; partitioned tables are
  refused, never compared as empty.
- **Expected differences.** The engine removed limitations copied from the
  reference engine (ISS-017..020, ISS-023); their effect on output is
  declared in a rule file (`SHADOW_EXPECTED_DIFFERENCES`, JSON; the harness
  ships ISS-018 names and ISS-019 unbounded `varchar`, applied only when a
  run names it: a rule is a decision about a run, not a default). A rule cites the
  accepted issue and a reason, and classifies only a declared-type or
  name-case difference as "expected and documented"; it is listed per table
  and column and counted in the cumulative report. Values are always
  compared: no rule covers a row count, a checksum or a column present on
  one side only. A `declared_type` rule that would accept any type change is
  refused.
  Differing rows are shown as hashes with their count per side; values only
  with `SHADOW_SHOW_ROWS` (opt-in). Volatile columns (`uuid()`,
  `localtimestamp`) are excluded by pattern (`SHADOW_IGNORE_COLUMNS`) and
  listed in the report.
- **Memory.** Peak RSS per side, sampled while that side's dbt run lasts:
  `pid:` (process tree `VmRSS`), `container:` (cgroup v2 `anon`, as the
  benchmark), or `cmd:` (a command printing bytes). Optional
  `SHADOW_RSS_MAX_RATIO` turns the M1 ratio into a failure.
- **Verdict.** JSON and Markdown per cycle and cumulative (cycles accumulate
  across invocations in the same output directory, one per day for the M1
  run). Exit 1 on any divergent table, model status that differs between
  sides, dbt failure or ratio above the limit.
- DuckDB runs with a memory limit and spills to a temporary directory; the
  harness buffers nothing without bound.

### Self-test (`tools/shadow/selftest.sh`)

SQL Server 2022 with CDC, `trinodb/trino:431` as the reference, CodeLake
configured by `codelake config import`, and a fixture dbt project with the
acceptance pipelines' patterns (CDC source, incremental `append` over an LSN
watermark, `table` with `on_table_exists=drop`, a view, decimal, timestamp,
varchar, varbinary). Two cycles with CDC changes between them must report no
divergence; mutations of the candidate's output (a row more or less, a
decimal, a timestamp +3h, a trailing space, NULL to `''`) must each be
detected on the right column, and a layout-only rewrite must not.

### Findings

The self-test surfaced three candidate defects on the acceptance pipelines'
patterns (docs/shadow-run.md, "Findings"): SQL Server `datetime`/`datetime2(3)`
columns arrive as `timestamp(6)` and a `MILLISECONDS` hive CTAS refuses them
(F8-1); SQL Server `varchar(n)` columns lose their length in CTAS (F8-2); a
window `ORDER BY` on varbinary fails (F8-3). They belong to the connector and
planner, not to this front. The fixture works around them behind a dbt
variable so the harness is proven; `SHADOW_SELFTEST_STRICT=1` shows them.

## Consequences

- Operators can bring their catalog files and keep secrets out of the TOML
  and out of terminals and logs.
- Other fronts reuse `${ENV:…}`; the rule "secrets in `*_env`" stays.
- The harness trusts neither engine and needs no change to CodeLake, but it
  needs the dbt project to bound its reads with the cutoff variable; without
  it, CDC rows arriving between the two runs make the sides differ.
- Views are not compared (no data files); tables built from them are.
- S3 warehouses are supported by the harness (DuckDB `httpfs`) but are not
  exercised by the self-test, as CodeLake's `hive-s3` feature is out of the
  release build (ROADMAP M3).
