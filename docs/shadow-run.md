# Shadow run

The M1 acceptance ([ROADMAP](../ROADMAP.md)): the same dbt project runs
against the reference engine and against CodeLake with the same inputs; every
output table must match (row count and checksum) on three consecutive days,
and CodeLake's peak RSS must stay at or below half of the reference's.

This page covers the harness that runs and judges it
([ADR 0015](adr/0015-shadow-run-harness.md)). The run in homologation is done
by a person, with the steps below.

## 1. Configure the candidate from the existing catalogs

```bash
codelake config import --from catalog/ --out codelake.toml --env-out codelake.env \
    [--base base.toml] [--env-prefix CODELAKE_] [--force]
```

- `catalog/` holds one `<name>.properties` per catalog (`connector.name=…`).
  Imported: `sqlserver`, `postgresql`, `hive` with `hive.metastore=file`
  (local directory or `s3://`). Anything else is skipped with a reason; the
  files are still written and the exit code is 3 (0 when every catalog was
  imported, 2 on an error that wrote nothing).
- `--base` is an existing configuration whose sections (`[server]`,
  `[memory]`, sources) are kept; imported sources are appended. Without it, a
  loopback-only header is written.
- Secrets (passwords, keys, tokens, credentials in a JDBC URL) go to the env
  file only, mode `0600`, one `NAME=value` per line (`docker --env-file`,
  Compose `env_file`, systemd `EnvironmentFile`, `kubectl create secret generic
  --from-env-file`). The TOML names the variable (`password_env = "…"`).
  Nothing is printed: the report names catalogs, settings and variables.
- A secret already written `${ENV:NAME}` becomes `password_env = "NAME"` and
  is not copied. Other `${ENV:NAME}` values stay as they are in the TOML and
  are resolved when the server loads it (below).

Review the file, then validate it where the variables are set:

```bash
codelake check-config --config codelake.toml
```

### `${ENV:NAME}` in `codelake.toml`

Any string value may reference environment variables, resolved once when the
file is loaded (`serve`, `check-config`):

```toml
[server]
pg_listen = "${ENV:CODELAKE_PG_LISTEN}"

[memory]
limit = "${ENV:CODELAKE_MEMORY_LIMIT}"
spill_dir = "${ENV:DATA_DIR}/spill"

[[source]]
name = "erp"
kind = "postgres"
host = "${ENV:ERP_HOST}"
database = "erp"
user = "reader"
password_env = "ERP_PASSWORD"   # secrets: keep the *_env keys
```

- `NAME` is `[A-Za-z_][A-Za-z0-9_]*`. Every `${ENV:` starts a reference (no escape, as in catalog files); any other `$` is literal.
- An unset or empty variable stops the load with the key and the variable
  named, all of them at once (exit 2). Values never appear in errors.
- Only string values (a size or an address is a string); numbers and booleans
  are literals. Not recursive.
- Use it for non-secret values. Secrets stay in `*_env` keys, held as
  `SecretString` and never printed.

## 2. Prepare the two sides

| | Reference | Candidate |
|---|---|---|
| Engine | the current engine, unchanged | CodeLake with the imported configuration |
| Sources | the same databases | the same databases |
| Output warehouse | its own directory | **another** directory (the harness refuses the same) |
| dbt target | `reference` (any name, `SHADOW_REFERENCE_TARGET`) | `candidate` |

Both warehouses must start in the same state: both empty (then use
`SHADOW_FULL_REFRESH_FIRST=true`) or one a copy of the other.

The profile can point both targets at the side being run: the harness exports
`SHADOW_SIDE`, `SHADOW_ENGINE_URL`, `SHADOW_ENGINE_HOST`, `SHADOW_ENGINE_PORT`
and `SHADOW_ENGINE_HTTP_SCHEME` to dbt (see `tools/shadow/fixture/profiles.yml`).

### Same inputs: the cutoff

Sources keep changing (CDC). Each cycle reads **one** cutoff and gives the
same value to both sides as the dbt variable `shadow_cutoff`
(`SHADOW_CUTOFF_VAR`). Models that read changing sources must bound their
reads with it, for example:

```sql
WHERE "__$start_lsn" <= X'{{ var("shadow_cutoff") }}'
```

The cutoff comes from `SHADOW_CUTOFF_SQL` (one value, run on the reference;
varbinary as upper-case hex, timestamps in ISO 8601) or `SHADOW_CUTOFF_VALUE`.
Without a cutoff, rows arriving between the two runs show up as divergences.

## 3. Run

```bash
scripts/shadow-run.sh shadow.env          # SHADOW_CYCLES cycles
scripts/shadow-run.sh shadow.env summary  # cumulative report again
scripts/shadow-run.sh shadow.env test     # the harness's own tests
scripts/shadow-run.sh shadow.env compare --reference-warehouse /a --candidate-warehouse /b \
    --table schema.table [--ignore-column schema.table.col] [--show-rows]
```

The harness runs in its own image (`tools/shadow/Dockerfile`, built on first
use; dbt-core/dbt-trino 1.8 and DuckDB pinned by hash, regenerate with
`tools/shadow/lock.sh`). Local paths of the configuration are mounted at the
same path inside it.

`shadow.env` holds `SHADOW_*=value` lines only (no quotes, no expansion):

| Variable | Meaning | Default |
|---|---|---|
| `SHADOW_REFERENCE_URL`, `SHADOW_CANDIDATE_URL` | `http(s)://host:port` of each engine's client protocol | required |
| `SHADOW_REFERENCE_TARGET`, `SHADOW_CANDIDATE_TARGET` | dbt targets | `reference`, `candidate` |
| `SHADOW_DBT_PROJECT_DIR`, `SHADOW_DBT_PROFILES_DIR` | the dbt project and its profiles | required |
| `SHADOW_DBT_COMMAND` | `run` or `build` (models and tests) | `run` |
| `SHADOW_SELECT`, `SHADOW_EXCLUDE` | dbt selectors | all |
| `SHADOW_THREADS` | dbt `--threads` | profile |
| `SHADOW_DBT_VARS` | extra dbt variables, a JSON object | — |
| `SHADOW_REFERENCE_WAREHOUSE`, `SHADOW_CANDIDATE_WAREHOUSE` | where each side's output is read: absolute path or `s3://bucket/prefix` | required, different |
| `SHADOW_OUTPUT_CATALOGS` | catalogs whose models are compared | `hive` |
| `SHADOW_TABLES` | compare these `schema.table` instead of the models that ran | — |
| `SHADOW_IGNORE_COLUMNS` | volatile columns (`uuid()`, `localtimestamp`): `schema.table.col`, `table.col` or `col`, globs allowed | — |
| `SHADOW_CUTOFF_SQL` / `SHADOW_CUTOFF_VALUE` / `SHADOW_CUTOFF_VAR` | the cutoff (above) | — / — / `shadow_cutoff` |
| `SHADOW_CUTOFF_USER` | user for the cutoff query | `shadow` |
| `SHADOW_FULL_REFRESH_FIRST` | `--full-refresh` on the first cycle | `false` |
| `SHADOW_CYCLES`, `SHADOW_INTERVAL_SECS` | cycles per invocation, pause between them | `1`, `0` |
| `SHADOW_REFERENCE_RSS`, `SHADOW_CANDIDATE_RSS` | memory meter: `pid:<pid>`, `container:<name>`, `cmd:<command printing bytes>` | none |
| `SHADOW_RSS_INTERVAL_MS` | sampling period | `200` |
| `SHADOW_RSS_MAX_RATIO` | fail when candidate/reference peak exceeds it (M1: `0.5`) | report only |
| `SHADOW_EXPECTED_DIFFERENCES` | rule file of expected, documented differences (below); `none` for no rule | the shipped `tools/shadow/codelake_shadow/expected-differences.json` |
| `SHADOW_SAMPLE`, `SHADOW_SHOW_ROWS` | differing row hashes listed; print their values (opt-in) | `10`, `false` |
| `SHADOW_DUCKDB_MEMORY` | DuckDB memory limit (it spills beyond it) | `2GB` |
| `SHADOW_S3_ENDPOINT`, `SHADOW_S3_REGION`, `SHADOW_S3_PATH_STYLE` | S3 warehouses; keys from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via `SHADOW_PASS_ENV`, or the credential chain | — |
| `SHADOW_OUT_DIR` | reports and dbt artifacts | required |
| `SHADOW_PASS_ENV` | names of variables to pass through (profile passwords): values come from the calling environment | — |
| `SHADOW_DOCKER_NETWORK`, `SHADOW_DOCKER_VOLUMES` | network to reach the engines; volumes to mount (`vol:/path:ro`, comma-separated) | — |

Memory meters: `container:` reads the container's cgroup v2 `anon` (RSS without
page cache), the benchmark's probe; `pid:` needs the harness to see the
process (`--pid=host`, added automatically); `cmd:` runs a command each sample
(e.g. `ssh host ps -o rss= -p 1234` with a `k` suffix), and the report never
shows the command.

For the M1 run: one invocation per day with the same `SHADOW_OUT_DIR`
(cycles accumulate), `SHADOW_DBT_COMMAND=build`, the full project, and
`SHADOW_RSS_MAX_RATIO=0.5` measured during the hourly full run.

## 4. Read the verdict

Per cycle `cycle-NNNN.json` / `.md`, and cumulative `summary.json` / `.md` in
`SHADOW_OUT_DIR`. Exit code: 0 all equal; 1 divergence (a table, a model
status that differs between sides, a dbt failure, or the RSS ratio); 2
configuration; 3 harness failure.

Per table:

- **rows** and **checksum**: the checksum is the sum of a 128-bit hash of each
  row's canonical text, so it ignores row order and counts duplicates.
- **columns differing**: per-column checksums, with NULL counts.
- **sample**: hashes of rows whose count differs, with the count on each side.
  Values only with `SHADOW_SHOW_ROWS=true` (they are production data).
- **schema**: columns on one side only and value-class mismatches
  (divergences); **declared types** that differ (divergences unless a rule
  documents them, below), read from each table's metastore file, or from the
  Parquet schema when either side has none; Parquet storage types that differ
  under equal declared types (warnings, e.g. INT96 vs INT64 timestamps).
- **name case**: a table directory or a column whose name differs only by
  letter case (divergence unless a rule documents it). Directories are found
  without regard to case.
- Partitioned tables (files in subdirectories, partition columns in the
  metastore) are refused with an error, never compared as empty.

Canonical values: integers and decimals by value (`12.50` = `12.5`),
timestamps as microseconds since the epoch (naive values read as UTC), strings
exactly (trailing spaces count), binary as hex, NULL distinct from `''`.

Views have no data files and are listed as not compared; compare what is
built from them.

### Expected and documented differences

CodeLake removed limitations it would otherwise copy from the reference engine
(accepted issues in [docs/issues](issues/README.md)); the difference they make
in the output is declared, not hidden. A rule file (JSON) lists them:

```json
{"differences": [
  {"id": "ISS-019", "kind": "declared_type",
   "reason": "CTAS of a literal infers unbounded varchar",
   "reference": ["varchar(*)"], "candidate": ["varchar", "string"]},
  {"id": "ISS-018", "kind": "name_case", "reason": "Case-preserving names"}
]}
```

- `kind`: `declared_type` (a column's declared type) or `name_case` (a table or
  column name that differs only by case). `id` (the accepted issue) and
  `reason` are required; `table` (`schema.table`) and `column` are globs
  (default `*`); `reference`/`candidate` are globs over the declared type,
  lower case without spaces. A `declared_type` rule must constrain at least
  one side.
- A matching difference is reported under *Expected and documented
  differences* with its rule, per table and column, and counted per rule in
  the cumulative report; it does not make the table divergent.
- A rule never covers values: row count, checksum, per-column checksums and
  columns present on one side only are always compared. A table whose only
  difference is a documented type is equal; the same table with one changed
  value is divergent.
- The shipped file declares ISS-019 and ISS-018. The other removed limitations
  (ISS-017 atomic `CREATE OR REPLACE`, ISS-020 size units, ISS-023 `DROP VIEW`)
  change no table content: where they change a dbt run (a model that fails on
  one side only), the harness reports it as a model status difference, which
  no rule covers. ISS-019's rule accepts any `varchar(n)` → unbounded change,
  including a length lost from a source column; review the listed columns.

## Self-test

```bash
CODELAKE_TARGET_VOLUME=… heavy.sh tools/shadow/selftest.sh
```

SQL Server 2022 with CDC, `trinodb/trino:431` as the reference and CodeLake
configured by `codelake config import` from `tools/shadow/fixture/catalog`,
running `tools/shadow/fixture` (CDC source, incremental `append` over an LSN
watermark, tables, a view; decimal, timestamp, varchar, varbinary). Checks:
the import (secrets as references, the thrift catalog skipped with its reason,
no secret value anywhere, env file `0600`), two cycles with CDC changes between
them and zero divergences, and each injected mutation detected on the right
column (a row more or less, decimal, timestamp +3h, trailing space, NULL to
`''`) while a layout-only rewrite is not. Reports stay in
`target/shadow-selftest/out`.
