//! `information_schema` after DDL, in `hive` with arrays, CTAS, computed
//! views and ADD COLUMN, and under filters, compared row by row with Trino 431.

use super::{Res, expect_rows};

/// Tables and views renamed, dropped and replaced, and a quoted name
/// (Trino's memory connector cannot add columns; ADD COLUMN is in hive).
const EVOLUTION_SETUP: &[&str] = &[
    "CREATE SCHEMA {c}.bx_ev",
    "CREATE TABLE {c}.bx_ev.t (a bigint, b decimal(7,2), c timestamp(6), d varchar)",
    "INSERT INTO {c}.bx_ev.t VALUES (1, 2.50, TIMESTAMP '2026-01-01 10:00:00.123456', 'x')",
    "CREATE TABLE {c}.bx_ev.old (x integer)",
    "ALTER TABLE {c}.bx_ev.old RENAME TO {c}.bx_ev.renamed",
    "CREATE TABLE {c}.bx_ev.gone (x integer)",
    "DROP TABLE {c}.bx_ev.gone",
    "CREATE VIEW {c}.bx_ev.v AS SELECT a FROM {c}.bx_ev.t",
    "CREATE OR REPLACE VIEW {c}.bx_ev.v AS SELECT a, b * b AS bb, c, sum(b) OVER () AS sb FROM {c}.bx_ev.t",
    "CREATE VIEW {c}.bx_ev.dropped AS SELECT 1 AS one",
    "DROP VIEW {c}.bx_ev.dropped",
    "CREATE TABLE {c}.bx_ev.\"x y\" (\"x y\" integer)",
];

const EVOLUTION_QUERIES: &[&str] = &[
    "SELECT table_name, column_name, ordinal_position, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema = 'bx_ev' ORDER BY table_name, ordinal_position",
    "SELECT table_name, table_type FROM {c}.information_schema.tables WHERE table_schema = 'bx_ev' ORDER BY table_name",
    "SELECT table_name FROM {c}.information_schema.views WHERE table_schema = 'bx_ev' ORDER BY table_name",
    "SELECT * FROM {c}.bx_ev.v",
    "SELECT * FROM {c}.bx_ev.t",
    "SELECT \"x y\" FROM {c}.bx_ev.\"x y\"",
    "SELECT count(*) FROM {c}.information_schema.tables WHERE table_schema = 'bx_ev' AND table_name IN ('gone', 'dropped', 'old')",
];

#[tokio::test]
async fn information_schema_after_ddl_matches_trino() -> Res<()> {
    expect_rows(EVOLUTION_SETUP, EVOLUTION_QUERIES, None).await
}

/// A Parquet table, a CTAS of aggregates, a view with computed columns and
/// a view over that view (partitioned tables and arrays are a documented
/// `NOT_SUPPORTED` of the hive connector, not tested here).
const HIVE_SETUP: &[&str] = &[
    "CREATE SCHEMA hive.bx_h",
    "CREATE TABLE hive.bx_h.p (id bigint, dc decimal(20,4), ts timestamp(3), v varchar, dt date) WITH (format = 'PARQUET')",
    "INSERT INTO hive.bx_h.p VALUES (1, 1.5, TIMESTAMP '2026-01-01 10:00:00.123', 'x', DATE '2026-01-01'), (2, -2.25, TIMESTAMP '1969-12-31 23:59:59.999', NULL, DATE '2026-01-02'), (3, 7, NULL, 'zz', DATE '2026-01-01')",
    "CREATE TABLE hive.bx_h.agg WITH (format = 'PARQUET') AS SELECT dt, sum(dc) AS s, avg(dc) AS a, count(*) AS n, max(ts) AS mts, min(id) AS mid FROM hive.bx_h.p GROUP BY dt",
    "CREATE VIEW hive.bx_h.vv AS SELECT id, dc / 3 AS third, CAST(ts AS timestamp(6)) AS ts6, CAST(length(v) AS bigint) AS len FROM hive.bx_h.p",
    "CREATE VIEW hive.bx_h.vv2 AS SELECT id, third * 2 AS t2, ts6 FROM hive.bx_h.vv WHERE len IS NOT NULL",
];

const HIVE_QUERIES: &[&str] = &[
    "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type FROM hive.information_schema.columns WHERE table_schema = 'bx_h' ORDER BY table_name, ordinal_position",
    "SELECT table_catalog, table_schema, table_name, table_type FROM hive.information_schema.tables WHERE table_schema = 'bx_h' ORDER BY table_name",
    "SELECT table_catalog, table_schema, table_name FROM hive.information_schema.views WHERE table_schema = 'bx_h' ORDER BY table_name",
    "SELECT * FROM hive.bx_h.p ORDER BY id",
    "SELECT * FROM hive.bx_h.agg ORDER BY dt",
    "SELECT * FROM hive.bx_h.vv ORDER BY id",
    "SELECT * FROM hive.bx_h.vv2 ORDER BY id",
];

#[tokio::test]
async fn hive_ctas_and_views_match_trino() -> Res<()> {
    expect_rows(HIVE_SETUP, HIVE_QUERIES, None).await
}

/// Filters over the metadata tables: case of names, `LIKE` wildcards,
/// `OR`, `NOT IN`, a join between `tables` and `columns`, and the
/// catalog column.
const FILTER_SETUP: &[&str] = &[
    "CREATE SCHEMA {c}.bx_f",
    "CREATE TABLE {c}.bx_f.alpha (id bigint, amount decimal(20,4), at timestamp(3))",
    "CREATE TABLE {c}.bx_f.a_ha (id bigint)",
    "CREATE TABLE {c}.bx_f.beta (flag boolean, amount decimal(20,4))",
    "CREATE VIEW {c}.bx_f.alpha_v AS SELECT id, amount FROM {c}.bx_f.alpha",
    "CREATE SCHEMA hive.bx_f",
    "CREATE TABLE hive.bx_f.alpha (id bigint, amount decimal(20,4), at timestamp(3)) WITH (format = 'PARQUET')",
    "CREATE TABLE hive.bx_f.a_ha (id bigint) WITH (format = 'PARQUET')",
];

const FILTER_QUERIES: &[&str] = &[
    "SELECT table_name FROM {c}.information_schema.tables WHERE table_schema = 'bx_f' AND table_name = 'ALPHA'",
    "SELECT table_name FROM {c}.information_schema.tables WHERE table_schema = 'bx_f' AND table_name LIKE 'a_ha' ORDER BY table_name",
    "SELECT table_name FROM {c}.information_schema.tables WHERE table_schema = 'bx_f' AND table_name LIKE 'a\\_ha' ESCAPE '\\' ORDER BY table_name",
    "SELECT table_name, column_name FROM {c}.information_schema.columns WHERE table_schema = 'bx_f' AND (column_name = 'flag' OR data_type = 'timestamp(3)') ORDER BY table_name, column_name",
    "SELECT table_name, column_name FROM {c}.information_schema.columns WHERE table_schema = 'bx_f' AND data_type = 'decimal(20,4)' ORDER BY table_name",
    "SELECT table_name FROM {c}.information_schema.tables WHERE table_schema = 'bx_f' AND table_name NOT IN ('alpha', 'beta') ORDER BY table_name",
    "SELECT t.table_name, t.table_type, count(c.column_name) FROM {c}.information_schema.tables t JOIN {c}.information_schema.columns c ON c.table_schema = t.table_schema AND c.table_name = t.table_name WHERE t.table_schema = 'bx_f' GROUP BY t.table_name, t.table_type ORDER BY t.table_name",
    "SELECT column_name, ordinal_position FROM {c}.information_schema.columns WHERE table_schema = 'bx_f' AND table_name = 'alpha' ORDER BY ordinal_position DESC",
    "SELECT table_catalog, table_name, column_name, data_type FROM hive.information_schema.columns WHERE table_catalog = 'hive' AND table_schema = 'bx_f' AND table_name LIKE 'a%' ORDER BY table_name, ordinal_position",
    "SELECT count(*) FROM hive.information_schema.tables WHERE table_catalog = 'nope'",
    "SELECT schema_name FROM hive.information_schema.schemata WHERE schema_name LIKE 'bx%'",
];

#[tokio::test]
async fn information_schema_filters_match_trino() -> Res<()> {
    expect_rows(FILTER_SETUP, FILTER_QUERIES, None).await
}

/// Columns added to a hive table after rows were written: positions,
/// types, and NULL for the old rows.
const HIVE_ADD_COLUMN: &[&str] = &[
    "CREATE SCHEMA hive.bx_add",
    "CREATE TABLE hive.bx_add.t (id bigint, dc decimal(10,2)) WITH (format = 'PARQUET')",
    "INSERT INTO hive.bx_add.t VALUES (1, 1.25)",
    "ALTER TABLE hive.bx_add.t ADD COLUMN ts timestamp(3)",
    "ALTER TABLE hive.bx_add.t ADD COLUMN big decimal(38,10)",
    "INSERT INTO hive.bx_add.t VALUES (2, -2.5, TIMESTAMP '1969-12-31 23:59:59.999', 12345678901234567890.0123456789)",
    "CREATE VIEW hive.bx_add.v AS SELECT id, big * 2 AS big2, dc + big AS mix FROM hive.bx_add.t",
];

const HIVE_ADD_COLUMN_QUERIES: &[&str] = &[
    "SELECT table_name, column_name, ordinal_position, is_nullable, data_type FROM hive.information_schema.columns WHERE table_schema = 'bx_add' ORDER BY table_name, ordinal_position",
    "SELECT * FROM hive.bx_add.t ORDER BY id",
    "SELECT * FROM hive.bx_add.v ORDER BY id",
];

#[tokio::test]
async fn hive_add_column_matches_trino() -> Res<()> {
    expect_rows(HIVE_ADD_COLUMN, HIVE_ADD_COLUMN_QUERIES, None).await
}

// ---------------------------------------------------------------- defects

#[tokio::test]
#[ignore = "DEFECT-SEM43: quoted mixed-case names keep their case in the in-memory catalog; \
    Trino folds every identifier to lower case (hive already does, ADR 0007). Expected: CREATE \
    TABLE \"MiXeD\" (\"CamelCol\" bigint) is listed as mixed/camelcol, SELECT camelcol FROM mixed \
    works, table_name IN ('MiXeD') matches nothing. Obtained: listed as MiXeD/CamelCol, \
    TABLE_NOT_FOUND for mixed, 'MiXeD' matches one table"]
async fn defect_sem43_memory_catalog_folds_names_to_lower_case() -> Res<()> {
    expect_rows(
        &[
            "CREATE SCHEMA {c}.bx_mc",
            "CREATE TABLE {c}.bx_mc.\"MiXeD\" (\"CamelCol\" bigint)",
            "CREATE VIEW {c}.bx_mc.\"ViewOne\" AS SELECT \"CamelCol\" AS \"OtherCol\" FROM {c}.bx_mc.\"MiXeD\"",
        ],
        &[
            "SELECT table_name, column_name FROM {c}.information_schema.columns WHERE table_schema = 'bx_mc' ORDER BY table_name, column_name",
            "SELECT table_name FROM {c}.information_schema.tables WHERE table_schema = 'bx_mc' AND table_name IN ('MiXeD', 'ViewOne')",
            "SELECT camelcol FROM {c}.bx_mc.mixed",
            "SELECT othercol FROM {c}.bx_mc.viewone",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM44: hive view columns lose timestamp precisions other than 3. Expected \
    (Trino): a view column CAST(ts AS timestamp(0)) has data_type 'timestamp(0)' and answers \
    timestamp(0) '2026-01-01 10:00:01' (also through a view over the view); CAST(ts AS \
    timestamp(1)) has data_type 'timestamp(1)'. Obtained: data_type 'timestamp(3)' for both, and \
    SELECT of ts0 answers timestamp(3) '2026-01-01 10:00:01.000' (the same cast outside a view \
    is right)"]
async fn defect_sem44_hive_view_keeps_timestamp_precision() -> Res<()> {
    expect_rows(
        &[
            "CREATE SCHEMA hive.bx_tp",
            "CREATE TABLE hive.bx_tp.p (id bigint, ts timestamp(3)) WITH (format = 'PARQUET')",
            "INSERT INTO hive.bx_tp.p VALUES (1, TIMESTAMP '2026-01-01 10:00:00.623')",
            "CREATE VIEW hive.bx_tp.v AS SELECT id, CAST(ts AS timestamp(0)) AS ts0, CAST(ts AS timestamp(1)) AS ts1 FROM hive.bx_tp.p",
            "CREATE VIEW hive.bx_tp.v2 AS SELECT ts0 FROM hive.bx_tp.v",
        ],
        &[
            "SELECT CAST(ts AS timestamp(0)), CAST(ts AS timestamp(1)) FROM hive.bx_tp.p",
            "SELECT table_name, column_name, data_type FROM hive.information_schema.columns WHERE table_schema = 'bx_tp' ORDER BY table_name, ordinal_position",
            "SELECT * FROM hive.bx_tp.v",
            "SELECT * FROM hive.bx_tp.v2",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM45: length(varchar) is bigint in Trino. Expected: length('abc') bigint 3, \
    and a view column length(v) has data_type 'bigint'. Obtained: integer"]
async fn defect_sem45_length_is_bigint() -> Res<()> {
    expect_rows(
        &[
            "CREATE SCHEMA hive.bx_len",
            "CREATE TABLE hive.bx_len.p (v varchar) WITH (format = 'PARQUET')",
            "CREATE VIEW hive.bx_len.v AS SELECT length(v) AS len FROM hive.bx_len.p",
        ],
        &[
            "SELECT length('abc'), length(CAST(NULL AS varchar))",
            "SELECT column_name, data_type FROM hive.information_schema.columns WHERE table_schema = 'bx_len' AND table_name = 'v'",
        ],
        None,
    )
    .await
}
