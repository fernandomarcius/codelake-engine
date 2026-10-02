# ISS-064 — JDBC/ODBC metadata as the reference lists it: choices and what still differs

- Kind: behavior · Recorded by the metadata front (`fix/jdbc-metadata`), 2026-10-02 · Status:
  awaiting-confirmation (items 1–3 applied by recommendation; items 4 and 5 open)
- Context: the shadow's production-query replay (2026-10-02) showed what DBeaver, the JDBC driver and
  ODBC tools read differently from the reference engine (Trino 431): `system.jdbc.types` had 18 rows
  against 48, the engine's own `datafusion` catalog was listed (`system.jdbc.catalogs`,
  `system.metadata.catalogs`, `DatabaseMetaData.getCatalogs`), and a SQL Server catalog lacked the
  `db_*` schemas. Every `system.jdbc` and `system.metadata` table, `information_schema`, `DESCRIBE` of
  the system tables, `SHOW CATALOGS` / `SHOW SCHEMAS` and every `DatabaseMetaData` listing of the JDBC
  driver 431 were then compared row by row, by column names and types, with `trinodb/trino:431` over
  the same SQL Server 2022, PostgreSQL 17 and hive warehouse: integration test `jdbc_metadata`
  (SQL Server, JDBC driver) and differential `jdbc_metadata` (hive, PostgreSQL).

## Fixed: now as the reference

- `system.jdbc.types`: Trino 431's 48 rows, values and order (the same on every start; `integer`
   is listed twice there), including types `CodeLake` does not produce (`Geometry`, `HyperLogLog`,
   `ipaddress`, …): a client reads this list as the reference engine's.
- `system.jdbc.columns` names a column's type as `information_schema.columns` does (the declared
   `varchar(20)`, `char(3)`, `time(3)`; it was `varchar` and `time(9)`), and derives code, size,
   digits, radix and octet length from that name, as observed on Trino 431 for every type.
- SQL Server schemas: every schema the login sees (`sys.schemas`, what the JDBC driver's `getSchemas`
   reads): `db_owner`, `db_datareader`, …, `guest`, `sys` and empty schemas. Tables and views from
   `sys.all_objects` (`sp_tables`' answer): the 589 system views and tables of `sys` are listed and
   readable, as on the reference; their columns are read with `INFORMATION_SCHEMA.COLUMNS`'s own
   definition over `sys.all_columns` (that view only reads `sys.columns`; same answer for every type of
   a user table).
- `system.metadata.catalogs.connector_name` is `postgresql` for a PostgreSQL catalog (was `postgres`).
- A PostgreSQL relation with a column of a type the connector does not read (`jsonb`) failed every
   listing of its catalog (`system.jdbc.tables`, `system.jdbc.columns`, `table_comments`:
   `GENERIC_INTERNAL_ERROR`); it is listed without columns, and reading it fails with `NOT_SUPPORTED`.
- `SHOW CATALOGS [LIKE … [ESCAPE …]]` (one `Catalog` column of type `varchar(n)`, `n` the longest
   listed name) and `SHOW SCHEMAS [FROM | IN catalog] [LIKE … [ESCAPE …]]` (`CATALOG_NOT_FOUND`;
   `MISSING_CATALOG_NAME` without a session catalog) were refused (`NOT_SUPPORTED`).

## Choices (applied by recommendation)

1. **Item 1 — `system.jdbc.types` whole.** The 48 rows include types `CodeLake` does not produce:
   the list is the reference's, which a client reads as the server's; a type missing from it would be
   one a client cannot pick, not one it could use.
2. **Item 2 — the engine's in-memory catalog.** `datafusion` is `DataFusion`'s default catalog: every
   session has it and sources may not take its name. It is not listed (`system.jdbc.*`,
   `system.metadata.*`, `SHOW CATALOGS`, `getCatalogs`) while it holds only its empty `public` schema,
   which is the engine's internal state and has no counterpart in the reference. It is listed as soon as
   a client creates a schema or a table in it: hiding what a client stored there would protect no data
   and make the listing lie. It is still addressable by name, and the PostgreSQL listener is unchanged.
   Differential cases compare catalog listings on the catalogs both engines have.
3. **Item 3 — catalog properties.** `system.metadata.table_properties`, `schema_properties`,
   `column_properties` and `analyze_properties` stay empty: the reference lists its connectors'
   properties (28 hive table properties, SQL Server's `data_compression`), most of which `CodeLake`
   refuses in `WITH (…)` (it accepts `format = 'PARQUET'` only, and does not write SQL Server). Listing
   them would advertise options that fail. Differential `jdbc_metadata` JM27 (`accepted`).

## Still different (open)

4. **Item 4 — `system.runtime`.** The reference has `nodes`, `tasks`, `transactions` and
   `optimizer_rule_stats` besides `queries`; they are missing from `system.runtime` and so from the
   listings of the `system` catalog. Differential JM22 (`known`).
5. **Item 5 — connector type mappings and names**, which the listings show faithfully (they are what a
   query answers); each is a connector front of its own:
   - SQL Server: `char(n)`/`nchar(n)` are `varchar` (reference `char(n)`); `uniqueidentifier` is
     `varchar` (`char(36)`); `datetime2(7)` and `datetimeoffset(7)` carry 6 digits (`timestamp(7)`,
     `timestamp(7) with time zone`). Integration test `jdbc_metadata` pins them (`dbo.divergentes`).
   - PostgreSQL: `varchar(n)` and `char(n)` are `varchar`; `jsonb` is not read (`json`); `uuid` is
     `varchar` (`uuid`); a mixed-case schema keeps its case (`"JmMixed"`; the reference folds it to
     `jmmixed`, which then also names it in queries). Differential JM10, JM11, JM13, JM36 (`known`).
   - Hive: `array`, `map` and `row` columns are not read (`unknown`). Differential JM12 (`known`).
   - The SQL Server catalog is listed once, when the server starts (a schema or table created later
     needs a restart); the PostgreSQL one follows its source (ISS-060).
6. **Item 6 — outside the comparison.** `SHOW SESSION` lists `CodeLake`'s properties (another set), `SHOW
   FUNCTIONS` answers `DataFusion`'s columns, and `current_catalog` / `current_schema` are not
   supported; `DatabaseMetaData` methods the driver itself does not implement (`getFunctions`,
   `getIndexInfo`, …) fail alike on both engines.

- Risk: a client that relied on seeing `datafusion` while empty (none known), and SQL Server catalogs
  list 589 more relations (a whole-catalog `information_schema.columns` reads their columns too, one
  query per relation, as the reference does).
- Revisit when: a connector front fixes part of item 5 (drop the `known` marker of its cases); a client
  needs a catalog property listed.
- References: `crates/codelake-server/src/trino/system_tables.rs`, `crates/codelake-server/src/trino/statement.rs`
  (`listing`), `crates/codelake-connectors/src/sqlserver/mod.rs` (`LIST_SCHEMAS`, `LIST_TABLES`,
  `LIST_COLUMNS`), `crates/codelake-connectors/src/postgres.rs`; integration test
  `crates/codelake-server/tests/jdbc_metadata.rs`; differential `tools/differential/cases/jdbc_metadata.toml`.
