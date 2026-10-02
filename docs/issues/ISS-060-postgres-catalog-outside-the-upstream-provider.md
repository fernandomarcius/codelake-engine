# ISS-060 — PostgreSQL names, column types and connections outside the upstream provider

- Kind: upstream defect (worked around) · Applied by recommendation, 2026-10-01 · Status: open
- Context: the boletos panel of Warehouse reads the `hub` catalog (PostgreSQL) through the
  reference engine; in the shadow run all 28 of its queries failed on CodeLake. The schema
  `boletos` has 112 relations on the reference engine and 32 on CodeLake. Each cause below is in
  `datafusion-table-providers-postgres` 0.13.1; the connector works around each one without a
  fork (`crates/codelake-connectors/src/postgres/`), covered by the integration suite
  `postgres` and the differential file `postgres_catalog`.

| # | Upstream behavior | Symptom | Workaround |
|---|---|---|---|
| 1 | Relations are listed from `pg_tables` (`conn.rs`, `TABLES_QUERY`): tables and partitioned tables only | views, materialized views and foreign tables do not exist (`TABLE_NOT_FOUND`): the panel's views | `postgres::catalog` lists `pg_class` with `relkind` `r`, `p`, `v`, `m`, `f`, as the reference engine (partitions included, no privilege filter: reading what the user may not read fails at the source, as there) |
| 2 | Columns are read for `relkind IN ('r','v','m')` (`conn.rs`, `SCHEMA_QUERY`); otherwise the schema is guessed from one row (`infer_schema_from_data`) | a foreign or partitioned table had its columns typed from its first row, and none when empty | `postgres::catalog::schema_of` reads the columns of every listed kind |
| 3 | The row decoder reads arrays of numbers, booleans and `text` only, and only without `NULL` elements (`arrow_sql_gen::rows_to_arrow`); the column mapping does not know their element types either | `The field 'x' has an unsupported data type: _date` (`vw_dash_ciclo`); `error deserializing column` for `ARRAY[1, NULL]` | `postgres::types`: arrays follow the reference engine's `array-mapping=AS_ARRAY` (table in the module); every list is sent as JSON (`to_json`), which the decoder turns into the declared list |
| 4 | Every connection of the pool runs `SET json_serialization_enable TO true` (Redshift) and ignores PostgreSQL's refusal (`pool.rs`, `configure_session`) | one `ERROR: unrecognized configuration parameter` in the server log per connection, in production | `postgres::pool`: the connector's own pool (`bb8-postgres`) opens plain sessions; the upstream row decoder and SQL table are kept |
| 5 | `timestamp(p) with time zone` was mapped to a timestamp without zone (the precision was cut before matching the name) | a `timestamptz(3)` column was declared without zone while its rows came with one | `postgres::types` matches the name without its precision |

Besides, the listing followed the boot only (`codelake_core::catalog`): a relation created
afterwards needed a restart. It is now a `LiveListing`: an unlisted name is looked up at the
source when a query names it; the listing is read again by a listing of the catalog
(`information_schema`, `system.jdbc`) once older than one second, and by a lookup of an unknown
schema (on its own task: that query still fails, the next one finds the schema).

## Still different from the reference engine

- Arrays of `oid`, `timetz` and `bit` are left out (the reference engine maps them to
  `bigint`, `time(6)` and `boolean`); arrays of a domain or of more dimensions in a view
  (which keeps none) as in the reference engine.
- Element types are named as CodeLake names the same scalar types from PostgreSQL today:
  `varchar(10)[]`, `char(3)[]`, `uuid[]` and `json[]` are `array(varchar)` (the reference
  engine: `array(varchar(10))`, `array(char(3))`, `array(uuid)`, `array(json)`); timestamps
  carry nanoseconds.
- Scalar types keep the upstream mapping, including its refusals (`jsonb`, `timetz`): a
  relation with such a column cannot be read, where the reference engine leaves the column out.
- Values that JSON spells differently from what the decoder reads (`infinity` dates and
  timestamps, years before 1 AD) fail when read inside an array.

- Revisit when: `datafusion-table-providers` is upgraded; drop each workaround whose upstream
  behavior is fixed (the tests above keep the behavior).
- References: branch `fix/postgres-catalog-objects`.
