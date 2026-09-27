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
    [--base base.toml] [--env-prefix CODELAKE_] [--reference-time-zone UTC] [--force]
```

- `catalog/` holds one `<name>.properties` per catalog (`connector.name=…`).
  Imported: `sqlserver`, `postgresql`, `hive` with `hive.metastore=file`
  (local directory or `s3://`). Anything else is skipped with a reason; the
  files are still written and the exit code is 3 (0 when every catalog was
  imported, 2 on an error that wrote nothing).
- A hive catalog's `hive.parquet.time-zone` becomes `parquet_time_zone`.
  Without it, the reference engine converts `INT96` timestamps with its JVM
  time zone, which the catalog file does not show: pass it as
  `--reference-time-zone <IANA zone>` (`UTC` in the stock container image),
  or the catalog is skipped. A wrong zone would shift every `INT96`
  timestamp silently (ADR 0007).
- `--base` is an existing configuration whose sections (`[server]`,
  `[memory]`, sources) are kept; imported sources are appended. Without it, a
  loopback-only header is written.
- Secrets (passwords, keys, tokens, credentials in a JDBC URL) go to the env
  file only, mode `0600`, one `NAME=value` per line, unquoted: read literally
  by `docker --env-file` and `kubectl create secret generic --from-env-file`.
  Compose `env_file` and systemd `EnvironmentFile` interpret `$`, `#`, quotes,
  backslashes and edge whitespace; the report names every variable whose value
  holds one of them (use a literal reader or a `${ENV:…}` reference for it).
  The TOML names the variable (`password_env = "…"`). Nothing is printed: the
  report names catalogs, settings and variables.
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

Take a cutoff both engines represent exactly. A SQL Server `datetime` stores
1/300 s steps, and the reference compares a pushed-down filter with the stored
value while showing milliseconds (ISS-054): truncate a timestamp cutoff to the
second, for example
`SELECT CAST(date_trunc('second', MAX(tran_end_time)) AS varchar) FROM src.cdc.lsn_time_mapping`.

## 3. Run

```bash
scripts/shadow-run.sh shadow.env          # SHADOW_CYCLES cycles
scripts/shadow-run.sh shadow.env summary  # cumulative report again
scripts/shadow-run.sh shadow.env test     # the harness's own tests
scripts/shadow-run.sh shadow.env compare --reference-warehouse /a --candidate-warehouse /b \
    --table schema.table [--ignore-column schema.table.col] [--parquet-time-zone ZONE] \
    [--expected FILE] [--show-rows]
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
| `SHADOW_REFERENCE_METASTORE_FILE`, `SHADOW_CANDIDATE_METASTORE_FILE` | each side's table metadata file (the candidate's `metastore_names.metadata_file`) | `.trinoSchema`, `.codelakeSchema` |
| `SHADOW_OUTPUT_CATALOGS` | catalogs whose models are compared | `hive` |
| `SHADOW_TABLES` | compare these `schema.table` instead of the models that ran (any other entry is refused) | — |
| `SHADOW_PARQUET_TIME_ZONE` | IANA zone the hive catalogs of **both** engines convert `INT96` timestamps with (`hive.parquet.time-zone` / `parquet_time_zone`); an `INT96` column is not compared without it | none |
| `SHADOW_IGNORE_COLUMNS` | volatile columns (`uuid()`, `localtimestamp`): `schema.table.col`, `table.col` or `col`, globs allowed | — |
| `SHADOW_CUTOFF_SQL` / `SHADOW_CUTOFF_VALUE` / `SHADOW_CUTOFF_VAR` | the cutoff (above) | — / — / `shadow_cutoff` |
| `SHADOW_CUTOFF_USER` | user for the cutoff query | `shadow` |
| `SHADOW_FULL_REFRESH_FIRST` | `--full-refresh` on the first cycle | `false` |
| `SHADOW_CYCLES`, `SHADOW_INTERVAL_SECS` | cycles per invocation, pause between them | `1`, `0` |
| `SHADOW_REFERENCE_RSS`, `SHADOW_CANDIDATE_RSS` | memory meter: `pid:<pid>`, `container:<name>`, `cmd:<command printing bytes>` | none |
| `SHADOW_RSS_INTERVAL_MS` | sampling period | `200` |
| `SHADOW_RSS_MAX_RATIO` | fail when candidate/reference peak exceeds it (M1: `0.5`); needs both meters, and a cycle where a side was not measured fails | report only |
| `SHADOW_EXPECTED_DIFFERENCES` | rule file of expected, documented differences (below): a path, or `default` for the shipped `tools/shadow/codelake_shadow/expected-differences.json` | none (no rule) |
| `SHADOW_SAMPLE`, `SHADOW_SHOW_ROWS` | differing row hashes listed; print their values (opt-in) | `10`, `false` |
| `SHADOW_DUCKDB_MEMORY` | DuckDB memory limit (it spills beyond it) | `2GB` |
| `SHADOW_S3_ENDPOINT`, `SHADOW_S3_REGION`, `SHADOW_S3_PATH_STYLE` | S3 warehouses; keys from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via `SHADOW_PASS_ENV`, or the credential chain | — |
| `SHADOW_OUT_DIR` | reports and dbt artifacts | required |
| `SHADOW_PASS_ENV` | names of variables to pass through (profile passwords): values come from the calling environment | — |
| `SHADOW_TZ` | time zone of report timestamps and logs | `TZ`, else the host's `/etc/localtime` |
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
status that differs between sides, a dbt failure, the RSS ratio, or a side
the configured RSS gate could not measure); 2 configuration; 3 harness
failure.

Per table:

- **rows** and **checksum**: the checksum is the sum of a 128-bit hash of each
  row's canonical text, so it ignores row order and counts duplicates.
- **columns differing**: per-column checksums, with NULL counts.
- **sample**: hashes of rows whose count differs, with the count on each side.
  Values only with `SHADOW_SHOW_ROWS=true` (they are production data).
- **schema**, every difference a divergence:
  - the metastore file (each side's `SHADOW_<SIDE>_METASTORE_FILE`, `dataColumns`) on one side only;
  - declared columns: names, **order** (`SELECT *` and a positional `UNION`
    depend on it) and **types** (unless a rule documents a type, below),
    from the metastore file of both sides, or from the Parquet schema of both
    sides when neither has one. A table without data files is still compared
    on them;
  - Parquet columns on one side only and value-class mismatches;
  - Parquet **storage**: the type DuckDB reads (a decimal's precision and
    scale, a timestamp's unit) and a timestamp's encoding (`INT96`, or
    `INT64` with its unit and UTC adjustment). Both engines may read equal
    values from files that differ there; another reader of the files does
    not, so it is not equal.
- **name case**: a table directory or a column whose name differs only by
  letter case (divergence unless a rule documents it). Directories are found
  without regard to case.
- Partitioned tables (files in subdirectories, partition columns in the
  metastore) are refused with an error, never compared as empty.

Canonical values: integers and decimals by value (`12.50` = `12.5`),
timestamps as nanoseconds since the epoch of the wall-clock value the engines
read (an `INT64` timestamp as stored, naive values read as UTC; an `INT96`
timestamp converted from the UTC instant it stores to
`SHADOW_PARQUET_TIME_ZONE`, as the hive catalogs do), strings exactly
(trailing spaces count), binary as hex, NULL distinct from `''`. DuckDB reads
`INT96` at microsecond precision: sub-microsecond digits of an `INT96` column
are not compared.

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
   "table": "marts.orders_by_status", "column": "label",
   "reference": ["varchar(*)"], "candidate": ["varchar", "string"]},
  {"id": "ISS-018", "kind": "name_case", "reason": "Case-preserving names"}
]}
```

- `kind`: `declared_type` (a column's declared type), `name_case` (a table or
  column name that differs only by case) or `replaced_characters` (the
  reference replaced characters of a text column with U+FFFD; `characters`:
  `c1`, the C1 controls U+0080–U+009F, ISS-047). `id` (the accepted issue) and
  `reason` are required. `table` (`schema.table`) and `column`: globs for
  `name_case` (default `*`); for `declared_type`, one exact table and column,
  required: a type change is expected only on a column someone checked (the
  change ISS-019 makes on a literal is also what a length lost from a source
  column looks like, finding F8-2). `reference`/`candidate` are globs over
  the declared type, lower case without spaces; a `declared_type` rule must
  constrain at least one side.
- A matching difference is reported under *Expected and documented
  differences* with its rule, per table and column, and counted per rule in
  the cumulative report; it does not make the table divergent.
- `replaced_characters` is the one rule about values, for one exact table and
  column (required, no glob): the candidate's values of that column are
  compared after the same replacement, every other value and column exactly;
  the report shows the rule and how many candidate rows it changed. Example:
  `{"id": "ISS-047", "kind": "replaced_characters", "reason": "…",
  "table": "erp_raw_b1.pesfis_b1", "column": "cargo_pf", "characters": "c1"}`.
- No other rule covers values: row count, checksum, per-column checksums and
  columns present on one side only are always compared, and so are column
  order and storage. A table whose only difference is a documented type is
  equal; the same table with one changed value is divergent.
- No rule applies unless the run names a file: a rule is a decision about a
  run, not a default. The shipped file (`default`) declares ISS-018 only;
  ISS-019 rules are written per column of the project, once the column is
  known to come from a literal. The other removed limitations
  (ISS-017 atomic `CREATE OR REPLACE`, ISS-020 size units, ISS-023 `DROP VIEW`)
  change no table content: where they change a dbt run (a model that fails on
  one side only), the harness reports it as a model status difference, which
  no rule covers.

## Findings of the self-test

The self-test runs the acceptance pipelines' patterns; they show candidate
defects, not harness choices, and each blocks the M1 shadow run on the real
project. The fixture works around F8-1..F8-3 behind the dbt variable
`shadow_workarounds` (default true; each workaround is a no-op on the
reference engine), so the harness itself is proven; `SHADOW_SELFTEST_STRICT=1`
runs the fixture as the pipelines write it and fails until they are fixed.
The strict run stops at F8-1 (the models after it do not run); F8-2 and F8-3
were confirmed by querying both engines directly.

| ID | Pattern | Reference engine | CodeLake today | Workaround in the fixture |
|---|---|---|---|---|
| F8-1 | SQL Server `datetime`, `smalldatetime`, `datetime2(n)` columns | `datetime` read as SQL Server shows it, rounded to the millisecond (`.003`, `.007`, `.997`); `datetime2(7)` kept to 100 ns (`.9999995`, and `1.000000` once cast to `timestamp(6)`); precision declared (`smalldatetime` `timestamp(0)`, `datetime` `timestamp(3)`, `datetime2(n)` `timestamp(n)`) | **wrong values**: `datetime` read as the exact 1/300 s tick (`.003333`, `.006667`, `.996667`), so `date_diff('millisecond', …)` gives 6 where the reference gives 7 and `WHERE dt = TIMESTAMP '….003'` matches a row the reference does not; `datetime2(7)` truncated to the microsecond (`.999999`, a value the reference never produces); every type arrives as `timestamp(6)`, which a `MILLISECONDS` hive CTAS refuses (`Incorrect timestamp precision for timestamp(6)`) | `CAST(… AS timestamp(3))`, which rounds like the reference and so also hides the value difference: the workaround hides a wrong result, not only a precision |
| F8-2 | CTAS of a SQL Server `varchar(n)` column | declares `varchar(n)` | declares unbounded `string` (values equal) | `CAST(… AS varchar(n))` with the source's declared type |
| F8-3 | `ROW_NUMBER() OVER (PARTITION BY … ORDER BY "__$seqval")` (varbinary), from the source or from a hive table | ordered by bytes | `Cannot run range queries on datatype: Binary` | `ORDER BY to_hex(…)` |
| F8-4 | `SELECT *` on a SQL Server table | the source's column order | columns sorted by name, so `SELECT *` and a positional `UNION` or `INSERT` gave other results | none: fixed (the connector keeps `ORDINAL_POSITION`) |
| F8-5 | a SQL Server table created after the server started (a new CDC capture instance) | visible at once | `TABLE_NOT_FOUND` until CodeLake restarts: the table list is read once, at start | none (the fixture creates its tables before the candidate starts) |

Values were equal on every table and cycle once the workarounds let both sides
run.

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
`''`, a declared type, `INT96` copied as the stored instant). The copies are
written by DuckDB, which cannot write `INT96`: each shows the timestamps'
storage difference, and a rewrite that keeps the values shows nothing else.
A command that fails stops the self-test with a `FAIL` line naming it.
Reports stay in `target/shadow-selftest/out`. The harness's unit tests run on
their own with `tools/shadow/test.sh` (CI job `shadow-harness`).
