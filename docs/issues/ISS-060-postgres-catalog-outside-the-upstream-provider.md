# ISS-060 — PostgreSQL names, column types and connections outside the upstream provider

- Kind: upstream defect (worked around) · Applied by recommendation, 2026-10-01 · Status: open;
  item 8 (`numeric` without precision) changed 2026-10-02 by recommendation: awaiting-confirmation
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
| 8 | A `numeric` without precision is `decimal(38,20)`, decoded through `rust_decimal` (96 bits: rounds past 28 significant digits) and appended without a precision check; one with more than 38 digits an invalid `decimal(p,s)` | a value of 19 or 20 integer digits became an invalid `decimal(38,20)`; longer ones could be rounded | `postgres::types`: such a column follows the catalog's `decimal_mapping` as the reference engine's `decimal-mapping` (default `strict`: left out); a decimal travels as its text and is restored exactly, or rounded only by the catalog's `decimal_rounding_mode`; a value that does not fit fails the query naming the column (see below) |

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
## Item 8: `numeric` without precision, as the reference engine (2026-10-02)

Applied by recommendation, awaiting confirmation. Until then (PR #88) CodeLake kept such a column
as `decimal(38,20)`, exact or failing (an accepted divergence: "leaving it out protects no data").
It does not hold:

- Evidence (2026-10-02, the Warehouse .NET API against the shadow run, `conf/api-test` on
  the staging host): the API's driver (NH.Trino.Data.ADO) cannot hold precision 38 in a
  `System.Decimal` and hands the API an object serialized `{}`. `/api/boletos/saude` answered
  `ciclo.dados.ultima_rodada_pct_aceite` = `98.5` on production and on the shadow's Trino 431 (the
  API computes it when the column is absent) and `{}` on CodeLake; `/api/boletos/fila` items gained
  `"vl_recebido": {}`; the exports `/api/boletos/fila/xlsx`, `/gerados/xlsx` and `/conciliacao/xlsx`
  answered 406 on CodeLake and 200 on Trino 431 and production; `/api/boletos/fila?ordenar=vl_recebido`
  answered 200 on CodeLake and 503 on production (the API lists `vl_recebido` as sortable, the
  reference engine hides it). Exposing the column changed the API's JSON contract and broke values.
  The hub panel's 28 queries (`out/hub/hub-queries.json`, run of 2026-10-01 22:06) declare no
  decimal column on Trino 431; on CodeLake the only decimals were six columns of this kind, all
  `decimal(38,20)`: `vl_recebido`, `vl_baixado`, `ultima_rodada_pct_aceite`, `valor_erp`,
  `valor_cedente`, `dif_valor_informativa`. Without them both engines declare the same columns.
- Decision: by default CodeLake leaves the column out, as the reference engine's
  `decimal-mapping=STRICT` (its default) does: from the relation, `information_schema`,
  `system.jdbc`, `DESCRIBE`/`SHOW COLUMNS` and `SELECT *`; naming it fails with `COLUMN_NOT_FOUND`
  (the panel's `ordenar=vl_recebido` fails as in production). The same for a `numeric(p,s)` with
  `p` above 38 and for arrays of either; a relation whose every column is left out is refused,
  `NOT_FOUND: Table 's.t' has no supported columns (all n columns are not supported)`, as there,
  and listed without columns. A catalog can expose them on purpose with the reference's other
  setting (`docs/operations/configuration.md`): `decimal_mapping = "allow_overflow"`,
  `decimal_rounding_mode`, `decimal_default_scale` (`codelake config import` maps the reference's
  `decimal-mapping`, `decimal-rounding-mode` and `decimal-default-scale`).
- The reference engine's behavior, verified on `trinodb/trino:431` with `postgres:17` (catalogs
  with each setting, session properties for the eight rounding modes and scales 0 to 38):
  `STRICT` leaves out `numeric`, `numeric(50,10)`, `numeric(40)`, `numeric[]` and
  `numeric(50,10)[]`; `ALLOW_OVERFLOW` maps `numeric` to `decimal(38, default-scale)`,
  `numeric(p > 38, s)` to `decimal(38, min(s, default-scale))`, `numeric[]` to
  `array(decimal(38,0))` whatever the default scale, `numeric(p > 38, s)[]` to
  `array(decimal(38, min(s, default-scale)))`. A value is rounded to the scale by the rounding mode
  (`UNNECESSARY`, the default, fails: "Rounding necessary"), then fails when it needs more than 38
  digits ("Decimal overflow"); `NaN` and `±Infinity` fail ("Bad value for type BigDecimal : NaN").
  CodeLake does the same; the rounding table of each mode is pinned by the unit test
  `decimals_round_as_the_reference_engine` with the answers Trino 431 gave.
- With `allow_overflow` a value is never rounded unless the catalog's rounding mode says so (the
  default `unnecessary` keeps PR #88's behavior: exact or the query fails naming the value and the
  column). `allow_overflow` with `decimal_default_scale = 20` is the `decimal(38,20)` CodeLake had.
- Cases: differential `postgres_decimals` (catalogs `pg`, `pg_overflow` with the reference's
  defaults, `pg_rounded` with `HALF_EVEN` and scale 2, on both engines; 25 cases) and
  `postgres_types_zone` NUM2–NUM4; integration `unbounded_numerics_are_left_out_by_default` and
  `allow_overflow_maps_unbounded_numerics_and_rounds_only_when_asked`; unit tests in
  `postgres::types` (`numerics_follow_the_decimal_mapping`, `decimals_round_as_the_reference_engine`,
  `lists_of_decimals_travel_as_texts`).

## Still different from the reference engine

- A `numeric` value that does not fit fails with CodeLake's own error (`GENERIC_INTERNAL_ERROR`, `the
  PostgreSQL value 1.5 of column "n" does not fit decimal(38,0): more than 0 fraction digits`); the
  reference engine's is its JDBC driver's (`JDBC_ERROR`, "Rounding necessary", "Decimal overflow",
  "Bad value for type BigDecimal : NaN"). Accepted: the message names the value and the column
  (differential `postgres_decimals` PO3–PO7, PR3, PR4).
- The settings are per catalog only: the reference's session properties `decimal_mapping`,
  `decimal_rounding_mode` and `decimal_default_scale` are not read.
- A composite column with a `numeric` field without precision (or above 38 digits) is left out with
  either setting (the reference engine leaves every composite out).
- `numeric(p, -s)` (PostgreSQL 15+): the reference engine fails every listing of the catalog
  ("DECIMAL scale must be in range [0, precision (5)]: 2046"); CodeLake lists it as
  `decimal(p,-s)`, whose values fail to read.

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
