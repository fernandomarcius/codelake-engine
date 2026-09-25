# Canary

[`scripts/ops/canary.sh`](../../scripts/ops/canary.sh) runs a fixed list of
queries against an installation and checks, for each one:

- **correctness**: the answer equals what was recorded (a value, the row
  count, a sha256 of the rows, the rows themselves, numeric bounds), or equals
  the answer of a reference engine for the same SQL;
- **latency**: the query answered within `max_latency_ms`;
- **freshness** (optional): the newest row of a table is at most
  `max_lag_secs` old.

It prints **one JSON line per run** on stdout (diagnostics on stderr) and
exits `0` when every check passed, `1` when any failed, `2` on a usage or
configuration error. It runs once (cron, systemd timer, Kubernetes CronJob)
or forever with `--interval`.

Requirements: bash, curl, jq; `psql` for checks over the PostgreSQL wire;
`sha256sum`, `shasum` or `openssl`. The engine's image has none of these (it is
distroless): run the canary from a host or a small tools image in the clients'
network.

## Configure

A JSON file; example: [docs/examples/canary.json](../examples/canary.json).

| Key | Meaning |
|---|---|
| `name` | label in every line and metric |
| `user` | user sent with each query (default `canary`); give it its own name so its queries are easy to find |
| `http.url`, `http.catalog`, `http.schema`, `http.time_zone` | client HTTP protocol endpoint and session. The canary follows `nextUri` only to this URL's host and port, always with this URL's scheme (behind a TLS proxy the engine answers `http://`); a proxy in front must pass the client's `Host` header through |
| `http.password_env` | variable holding a password, once the engine authenticates (sent through curl's stdin, never on the command line) |
| `http.header_prefix` | protocol header prefix (default the reference engine's) |
| `pg.url` | `postgresql://user@host:port/db`, **without** password (use `PGPASSWORD` or a pgpass file; a URL with a password, as `user:pw@` or a `password=` parameter, is refused) |
| `reference.url`, `.user`, `.catalog`, `.schema`, `.password_env` | a second HTTP endpoint (the reference engine, or the previous CodeLake) for `expect.reference` |
| `defaults.protocol` | `http` or `pg` (default: `http` when configured) |
| `defaults.max_latency_ms` | latency objective per check (default 300000) |
| `defaults.timeout_secs` | a query still running after this is canceled and fails (default 600) |
| `checks[]` | `name`, `sql`, optional `protocol`, `max_latency_ms`, `timeout_secs`, `expect`, `freshness` |

`expect` (all given keys must hold):

| Key | Passes when |
|---|---|
| `value` | exactly one row, whose canonical text equals it (one column: the value, e.g. `"18342"`; a string, number or boolean, a `NULL` cell is `"\\N"`) |
| `row_count` | the number of rows |
| `sha256` | sha256 of the canonical rows |
| `rows` | the rows, e.g. `[["closed", 18000], ["returned", 342]]` |
| `min`, `max` | one numeric value within the bounds |
| `reference: true` | the same rows as the reference endpoint for the same SQL |
| `ordered: true` | compare rows in the order returned (default: sorted, order-insensitive) |

`--check-config` (and every run) refuses an expectation that could never
fail: `value: null`, or `ordered`/`reference` that is not `true` or `false`.

`freshness.max_lag_secs`: the query returns one number, the newest timestamp
in **seconds since the epoch**, and the check fails when now minus it exceeds
the limit. Convert in SQL, e.g.
`SELECT to_unixtime(with_timezone(max(updated_at), 'UTC')) FROM …` for a
timestamp column stored in UTC (use the zone the column's values are written
in). The canary host's clock must be synchronized (NTP).

Environment overrides: `CANARY_CONFIG`, `CANARY_NAME`, `CANARY_USER`,
`CANARY_HTTP_URL`, `CANARY_PG_URL`, `CANARY_REFERENCE_URL`,
`CANARY_HEADER_PREFIX`, `CANARY_INTERVAL_SECS`, `CANARY_MAX_RUNS`,
`CANARY_TEXTFILE`.

### Choosing checks

- **Answers that cannot change**: counts and checksums over closed periods
  (yesterday's partition, a finished month), historical snapshots, dimension
  tables that are never updated. A check over data that still grows gives
  false alarms; use `min` for those.
- **Every critical output table** of the pipelines: at least one closed-period
  count and one checksum (`checksum(col)` is the aggregate the reference
  engine has; combine several columns in one row).
- **Each catalog** once (a cheap query per source) so a source outage shows as
  a failed check, not only as a metric.
- **Reference comparisons** during the migration: the same SQL on both
  engines. Make the result independent of formatting: cast decimals and
  timestamps to `varchar` or aggregate them, so a difference is a data
  difference.
- **Freshness** of each table the pipelines write, with the pipelines'
  end-to-end objective (for example 300 s).
- Latency limits from the objective, not from today's timing plus a margin.

### Recording expected values

Write the checks without `expect`, run once, check the answer by hand or
against the reference engine, then record it:

```bash
scripts/ops/canary.sh --config canary.json --once | jq '.checks[] | {name, rows, value, sha256}'
```

What each field of the line can become:

| Line field | Is | Record it as |
|---|---|---|
| `rows` | the **number** of rows, not the rows | `expect.row_count` |
| `sha256` | sha256 of all the canonical rows | `expect.sha256` |
| `value` | the **first cell** of the first (sorted) row, cut at 200 characters | `expect.value` only for a one-row, one-column answer shorter than that; `min`/`max` bounds around it |

For a multi-column answer, `expect.value` must be the whole row's canonical
text (cells joined by a tab), which the line does not show: prefer
`expect.sha256`, or write `expect.rows` from the answer of the SQL run by
hand (e.g. `[["closed", 18000], ["returned", 342]]`).

The canonical text of a row is its cells joined by tabs, `NULL` as `\N`,
tabs/newlines/backslashes escaped; rows are sorted (bytewise) unless
`ordered`. Values arrive as the protocol encodes them, so a `sha256` recorded
over HTTP is not valid over the PostgreSQL wire: record per protocol.

Over HTTP, the canonical text of a **number** depends on the jq version:
jq 1.7 and later keep the literal the server sent (`42.0`, `1E+3`), jq 1.6
normalizes it and loses precision above 2^53. A `sha256` or `value` recorded
on one host can therefore fail on another, and two engines that write the
same double differently differ from each other. So:

- pin the jq version in the canary's host or image, and record expected
  values with that same version;
- cast `double`, `real`, `decimal` and integers that can exceed 2^53 to
  `varchar` in the SQL (or aggregate them into an integer), always in a check
  with `expect.reference`, and in every `sha256` or `value` check that is
  meant to survive a change of host.

## Run

```bash
scripts/ops/canary.sh --config canary.json --check-config     # validate only
scripts/ops/canary.sh --config canary.json                    # once; exit code = verdict
scripts/ops/canary.sh --config canary.json --interval 60 --textfile /var/lib/node_exporter/codelake_canary.prom
```

Output of one run (one line, shown indented):

```json
{"time": "2026-09-25T14:12:50Z", "canary": "prod-canary", "run": 1, "status": "fail",
 "duration_ms": 1381, "checks_total": 3, "checks_failed": 2,
 "checks": [
   {"name": "orders_closed_day_count", "protocol": "http", "status": "mismatch", "latency_ms": 160,
    "max_latency_ms": 5000, "rows": 1, "sha256": "…", "value": "18341", "expected": "18342",
    "message": "value 18341, expected 18342"},
   {"name": "orders_freshness", "protocol": "http", "status": "stale", "latency_ms": 128,
    "max_latency_ms": 10000, "rows": 1, "sha256": "…", "value": "1790345559.67",
    "freshness_lag_secs": 912.1, "message": "newest row is 912.1s old, SLO 300s"},
   {"name": "smoke", "protocol": "http", "status": "ok", "latency_ms": 247, "max_latency_ms": 300000,
    "rows": 1, "sha256": "…", "value": "42"}]}
```

Check statuses: `ok`, `mismatch` (wrong result), `slow` (latency above the
objective), `stale` (freshness above the objective), `error` (query failed,
timed out, endpoint unreachable, or an HTTP result that ended without the
`FINISHED` state; `message` has the engine's error name). A check that
reports no result counts as failed, so a run is never "ok" with fewer
checks than the configuration has.
`latency_ms` is measured by the canary around the query and includes the
client's own overhead (tens of milliseconds).

Where to run it:

- **cron / systemd timer**: one run per minute, output appended to a log the
  log pipeline ships; alert on `status != "ok"` or on the exit code.
- **Long-running** (`--interval`) under a supervisor, with `--textfile` for
  node_exporter; alert rules in [observability.md](observability.md).
- **Kubernetes CronJob** in a client namespace (labelled
  `codelake.io/client=true`), configuration from a ConfigMap, image with
  bash, curl and jq. The base `NetworkPolicy` admits only the PostgreSQL port
  `5432`; checks over the client HTTP protocol need the overlay's rule for
  its port ([install.md](install.md#kubernetes)).

Self-test of the script (no engine needed):
`scripts/ops/canary-selftest.sh`.

## When it fails

| Status | First step |
|---|---|
| `mismatch` | treat as a wrong result until proven otherwise: capture the line, rerun the SQL by hand on CodeLake and on the reference, then follow [incidents.md](incidents.md) |
| `slow` | [incidents.md](incidents.md#latency-above-the-objective) |
| `stale` | the pipeline that writes the table: is it running, failing, or slow? then latency as above |
| `error` | the message: source down, engine down, queue full, timeout ([incidents.md](incidents.md)) |

A check that is wrong because the data legitimately changed (a closed period
reopened, a backfill) is fixed in the configuration with the reason in its
change history, never silenced.
