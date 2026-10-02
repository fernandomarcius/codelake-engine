# ISS-060 — PostgreSQL names, column types and connections outside the upstream provider

- Kind: upstream defect (worked around) · Applied by recommendation, 2026-10-01 · Status: open
- Context: the boletos panel of Warehouse reads the `hub` catalog (PostgreSQL) through the
  reference engine; in the shadow run all 28 of its queries failed on CodeLake. The schema
  `boletos` has 112 relations on the reference engine and 32 on CodeLake. Each cause below is in
  `datafusion-table-providers-postgres` 0.13.1; the connector works around each one without a
  fork (`crates/codelake-connectors/src/postgres/`), covered by the integration suite
  `postgres` and the differential files `postgres_catalog` and `postgres_types_zone`.

| # | Upstream behavior | Symptom | Workaround |
|---|---|---|---|
| 1 | Relations are listed from `pg_tables` (`conn.rs`, `TABLES_QUERY`): tables and partitioned tables only | views, materialized views and foreign tables do not exist (`TABLE_NOT_FOUND`): the panel's views | `postgres::catalog` lists `pg_class` with `relkind` `r`, `p`, `v`, `m`, `f`, as the reference engine (partitions included, no privilege filter: reading what the user may not read fails at the source, as there) |
| 2 | Columns are read for `relkind IN ('r','v','m')` (`conn.rs`, `SCHEMA_QUERY`); otherwise the schema is guessed from one row (`infer_schema_from_data`) | a foreign or partitioned table had its columns typed from its first row, and none when empty | `postgres::catalog::schema_of` reads the columns of every listed kind |
| 3 | The row decoder reads arrays of numbers, booleans and `text` only, and only without `NULL` elements (`arrow_sql_gen::rows_to_arrow`); the column mapping does not know their element types either | `The field 'x' has an unsupported data type: _date` (`vw_dash_ciclo`); `error deserializing column` for `ARRAY[1, NULL]` | `postgres::types`: arrays follow the reference engine's `array-mapping=AS_ARRAY` (table in the module); every list is sent as JSON (`to_json`), which the decoder turns into the declared list |
| 4 | Every connection of the pool runs `SET json_serialization_enable TO true` (Redshift) and ignores PostgreSQL's refusal (`pool.rs`, `configure_session`) | one `ERROR: unrecognized configuration parameter` in the server log per connection, in production | `postgres::pool`: the connector's own pool (`bb8-postgres`) opens plain sessions; the upstream row decoder and SQL table are kept |
| 5 | `timestamp(p) with time zone` was mapped to a timestamp without zone (the precision was cut before matching the name) | a `timestamptz(3)` column was declared without zone while its rows came with one | `postgres::types` matches the name without its precision |
| 6 | Dates and times are nanoseconds (`timestamp(9)`, `time(9)`: nine fraction digits, which the Warehouse API's .NET driver refuses, "Timestamp with time zone has more than 7 digits of precision", 503 on every hub boletos route); the row decoder reads a timestamp as nanoseconds since 1970: a `timestamp` before 1970 is skipped (`duration_since(UNIX_EPOCH)` fails, the batch then fails), a `timestamptz` outside 1677–2262 becomes 0 (`timestamp_nanos_opt().unwrap_or_default()`) | `timestamp(9) with time zone` where the reference declares `timestamp(6) with time zone` (hub panel, 2026-10-01 22:06: every `timestamptz` column, values equal); `9999-12-31` read as `1970-01-01` | `postgres::types`: the precision PostgreSQL declares (6 without one), carried by the smallest Arrow unit holding it, recorded in the field when it is not 0, 3 or 6 (`timestamp(1)`, `time(4)`); a timestamp travels as its microseconds since 1970 in UTC (`bigint`, exact on any server version) and is restored to its unit |
| 7 | The pool leaves each session in the server's (or the role's) `TimeZone` | a view reading `current_date` / `now()` answered another day between 21:00 and 24:00 in `America/Sao_Paulo` (hub `saude_fontes`: `dias_sem_recebimento` 23 against 22, `recebimentos_30d` 22 against 30) | `postgres::pool`: every statement runs in the engine session's zone, set before it (see the divergence below) |
| 8 | A `numeric` without precision is `decimal(38,20)`, decoded through `rust_decimal` (96 bits: rounds past 28 significant digits) and appended without a precision check | a value of 19 or 20 integer digits became an invalid `decimal(38,20)`; longer ones could be rounded | `postgres::types`: a decimal travels as its text and is restored exactly; a value that does not fit fails the query naming the column (see below) |

Besides, the listing followed the boot only (`codelake_core::catalog`): a relation created
afterwards needed a restart. It is now a `LiveListing`: an unlisted name is looked up at the
source when a query names it; the listing is read again by a listing of the catalog
(`information_schema`, `system.jdbc`) once older than one second, and by a lookup of an unknown
schema (on its own task: that query still fails, the next one finds the schema).

## Deliberate divergences (accepted)

- **Session time zone** (item 7). The reference engine's JDBC sessions run in its JVM's zone,
  `Etc/UTC`, whatever the client's: through it, `current_setting('TimeZone')` is `Etc/UTC` and
  `current_date` is UTC's date (verified on the shadow run and on `trinodb/trino:431` with
  `postgres:17`, with and without `--timezone`). Removing nothing protects data here, and the answer a
  client expects is its own day (rule: do not inherit the reference's limitations), so CodeLake runs
  each statement in the client session's zone: `X-Trino-Time-Zone` or `SET TIME ZONE`
  (`codelake_core::federation_guard::remote_time_zone`, the zone of the session whose remote node the
  barrier starts), sent as `SET TIME ZONE 'America/Sao_Paulo'`, an offset as `SET TIME ZONE INTERVAL
  '-03:00' HOUR TO MINUTE` (PostgreSQL reads a text offset as a POSIX zone, east and west swapped);
  without a client zone, the engine's UTC as `Etc/UTC`, the reference's spelling. Never the server's
  or the role's default. The connection sets it before each statement, so a pooled connection carries
  no zone into the next one (the integration test `statements_run_in_the_client_time_zone` runs 40
  queries in four zones at once over the pool of 10). Cases: differential `postgres_types_zone` PZ1
  (no client zone: both `Etc/UTC`), PZ2 (`WRONG`, accepted: `America/Sao_Paulo` against `Etc/UTC`), PZ3
  (`current_date` of a view in `Pacific/Kiritimati` is that zone's day).
- **`numeric` without precision** (item 8). The reference (`decimal-mapping=STRICT`, its default)
  leaves the column out of the relation: the hub's `vl_recebido` and `ultima_rodada_pct_aceite` do not
  exist there (why the Warehouse API lists `vl_recebido` as sortable while it "does not exist"). Leaving
  it out protects no data, so CodeLake keeps it as `decimal(38,20)`, exactly: a value with more than
  18 integer or 20 fraction digits (other than trailing zeros), `NaN`, `Infinity` or `-Infinity`
  (PostgreSQL's `numeric` holds them) fails the query with `the PostgreSQL value NaN of column
  "vl_recebido" does not fit decimal(38,20): a decimal holds only finite numbers` (or `more than 18
  integer digits`, `more than 20 fraction digits`); it is never rounded nor cut. Cases: integration
  `unbounded_numerics_are_exact_or_fail_naming_the_column` (`1e20`, `1e18`,
  `0.123456789012345678901`, `NaN`, `±Infinity`, and 38 exact digits read whole); differential
  `postgres_types_zone` NUM2 (`EXTRA`), NUM3–NUM7 (`ERRO`, CodeLake's error pinned).

## Still different from the reference engine

- Arrays of `oid`, `timetz` and `bit` are left out (the reference engine maps them to
  `bigint`, `time(6)` and `boolean`); arrays of a domain or of more dimensions in a view
  (which keeps none) as in the reference engine.
- Element types are named as CodeLake names the same scalar types from PostgreSQL today:
  `varchar(10)[]`, `char(3)[]`, `uuid[]` and `json[]` are `array(varchar)` (the reference
  engine: `array(varchar(10))`, `array(char(3))`, `array(uuid)`, `array(json)`). An array of
  `timestamp(p)` / `time(p)` with `p` 1, 2, 4 or 5 is named after its Arrow unit
  (`array(timestamp(3))` for `timestamp(2)[]`; the reference: `array(timestamp(2))`): the
  declared precision is recorded on top-level fields only.
- Scalar types keep the upstream mapping, including its refusals (`jsonb`, `timetz`): a
  relation with such a column cannot be read, where the reference engine leaves `jsonb` out
  and declares `timetz` as `time(6)` but fails to read any value of it ("Cannot convert the
  column of type TIMETZ to requested type java.time.LocalTime", Trino 431).
- Values that JSON spells differently from what the decoder reads (`infinity` dates and
  timestamps, years before 1 AD) fail when read inside an array; a scalar `infinity` timestamp
  fails at the source (`cannot subtract infinite dates`, the microseconds of item 6).

- Revisit when: `datafusion-table-providers` is upgraded; drop each workaround whose upstream
  behavior is fixed (the tests above keep the behavior).
- References: branches `fix/postgres-catalog-objects`, `fix/postgres-session-zone-precision`.
