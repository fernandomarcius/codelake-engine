//! `information_schema` in the in-memory catalog and in `hive`: `tables`,
//! `columns` (`ordinal_position`, `data_type`, `is_nullable`), `schemata`
//! and `views`, compared row by row with Trino 431.

use serde_json::json;

use super::{Pair, Res, expect_same, verdict};

/// A table with every type both engines create in the in-memory catalog,
/// a view over it and a CTAS.
const MEMORY_SETUP: &[&str] = &[
    "CREATE SCHEMA {c}.sem_m",
    "CREATE TABLE {c}.sem_m.t (id bigint, n integer, s smallint, ti tinyint, b boolean, r real, d double, dc decimal(12,3), dcl decimal(38,5), v varchar, dt date, ts timestamp, ts0 timestamp(0), ts6 timestamp(6))",
    "CREATE VIEW {c}.sem_m.v AS SELECT id, dc, ts, v FROM {c}.sem_m.t",
    "CREATE SCHEMA {c}.sem_m2",
    "CREATE TABLE {c}.sem_m2.e (x bigint)",
];

const MEMORY_QUERIES: &[&str] = &[
    "SELECT table_catalog = '{c}', table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_m' ORDER BY table_schema, table_name, ordinal_position",
    "SELECT table_schema, table_name, column_name, ordinal_position, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema LIKE 'sem\\_m%' ESCAPE '\\' ORDER BY table_schema, table_name, ordinal_position",
    "SELECT table_catalog = '{c}', table_schema, table_name, table_type FROM {c}.information_schema.tables WHERE table_schema IN ('sem_m', 'sem_m2', 'information_schema') ORDER BY table_schema, table_name",
    "SELECT catalog_name = '{c}', schema_name FROM {c}.information_schema.schemata WHERE schema_name IN ('sem_m', 'sem_m2', 'information_schema', 'nope') ORDER BY schema_name",
    "SELECT table_catalog = '{c}', table_schema, table_name FROM {c}.information_schema.views WHERE table_schema = 'sem_m'",
    "SELECT * FROM {c}.information_schema.columns WHERE table_schema = 'no_such_schema'",
    "SELECT * FROM {c}.information_schema.tables WHERE table_schema = 'no_such_schema'",
    "SELECT * FROM {c}.information_schema.views WHERE table_schema = 'no_such_schema'",
    "SELECT * FROM {c}.information_schema.schemata WHERE schema_name = 'no_such_schema'",
    "SELECT count(*), max(ordinal_position), min(ordinal_position) FROM {c}.information_schema.columns WHERE table_schema = 'sem_m' AND table_name = 't'",
    "SELECT column_name FROM {c}.information_schema.columns WHERE table_schema LIKE 'sem\\_m' ESCAPE '\\' AND table_name LIKE 'v%' AND column_name LIKE '%d%' ORDER BY 1",
    "SELECT table_name, column_name, ordinal_position, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema = 'information_schema' ORDER BY table_name, ordinal_position",
    "SELECT (SELECT count(*) FROM {c}.information_schema.table_privileges), (SELECT count(*) FROM {c}.information_schema.roles), (SELECT count(*) FROM {c}.information_schema.enabled_roles), (SELECT count(*) FROM {c}.information_schema.applicable_roles)",
];

#[tokio::test]
async fn memory_catalog_information_schema_matches_trino() -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(MEMORY_SETUP).await?;
    let mut wrong = pair.mismatches(MEMORY_QUERIES, None).await?;
    // Trino 431's memory connector lists every column once per schema of an
    // `IN` list on `table_schema` (38 rows for 19 columns). `CodeLake`
    // answers each column once: deliberate divergence, not a defect.
    let (trino, codelake) = pair
        .both(
            "SELECT count(*) FROM {c}.information_schema.columns WHERE table_schema IN ('sem_m', 'sem_m2')",
            None,
        )
        .await?;
    if trino["rows"] != json!([[38]]) || codelake["rows"] != json!([[19]]) {
        wrong.push(format!(
            "columns with table_schema IN (...): trino {trino}, codelake {codelake} (expected 38 and 19)"
        ));
    }
    verdict(&wrong)
}

/// The same shapes in `hive` (Trino file metastore on both sides), plus a
/// CTAS and a view whose columns are computed.
const HIVE_SETUP: &[&str] = &[
    "CREATE SCHEMA hive.sem_h",
    "CREATE TABLE hive.sem_h.t (id bigint, n integer, s smallint, ti tinyint, b boolean, r real, d double, dc decimal(12,3), dcl decimal(30,5), v varchar, dt date, ts timestamp) WITH (format = 'PARQUET')",
    "CREATE TABLE hive.sem_h.c WITH (format = 'PARQUET') AS SELECT CAST(1.5 AS decimal(2,1)) AS x, CAST(1 AS integer) AS y, CAST(1.25 AS decimal(20,2)) * 2 AS w, 1E0 AS dd, CAST(2 AS bigint) AS z",
    "CREATE VIEW hive.sem_h.v AS SELECT id, dc * 2 AS dc2, avg(dc) OVER () AS a, ts, v FROM hive.sem_h.t",
];

const HIVE_QUERIES: &[&str] = &[
    "SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, column_default, is_nullable, data_type FROM hive.information_schema.columns WHERE table_schema = 'sem_h' ORDER BY table_name, ordinal_position",
    "SELECT table_catalog, table_schema, table_name, table_type FROM hive.information_schema.tables WHERE table_schema IN ('sem_h', 'information_schema') ORDER BY table_schema, table_name",
    "SELECT catalog_name, schema_name FROM hive.information_schema.schemata ORDER BY schema_name",
    "SELECT table_catalog, table_schema, table_name FROM hive.information_schema.views WHERE table_schema = 'sem_h'",
    "SELECT count(*) FROM hive.information_schema.columns WHERE table_catalog = 'hive' AND table_schema = 'sem_h' AND table_name = 't'",
    "SELECT * FROM hive.sem_h.c",
    "SELECT * FROM hive.sem_h.v",
];

#[tokio::test]
async fn hive_catalog_information_schema_matches_trino() -> Res<()> {
    expect_same(HIVE_SETUP, HIVE_QUERIES, None).await
}

// ---------------------------------------------------------------- defects

#[tokio::test]
async fn defect_sem7_varchar_length_is_enforced() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA {c}.sem_vc",
            "CREATE TABLE {c}.sem_vc.t (id bigint, v varchar, v10 varchar(10))",
            "INSERT INTO {c}.sem_vc.t (id, v) VALUES (1, 'abcdefgh')",
            "CREATE VIEW {c}.sem_vc.w AS SELECT CAST(v AS varchar(5)) AS v5 FROM {c}.sem_vc.t",
        ],
        &[
            "SELECT CAST('abcdefgh' AS varchar(5)), CAST(v AS varchar(5)) FROM {c}.sem_vc.t",
            "SELECT v5 FROM {c}.sem_vc.w",
            "INSERT INTO {c}.sem_vc.t (id, v10) VALUES (2, 'abcdefghijKLM')",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem23_bounded_character_types_in_data_type() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA {c}.sem_ch",
            "CREATE TABLE {c}.sem_ch.t (v10 varchar(10), ch char(3), v varchar)",
            "CREATE VIEW {c}.sem_ch.w AS SELECT CAST(v AS varchar(5)) AS v5 FROM {c}.sem_ch.t",
            "CREATE SCHEMA hive.sem_ch",
            "CREATE TABLE hive.sem_ch.t (v10 varchar(10), ch char(3), v varchar) WITH (format = 'PARQUET')",
            "CREATE VIEW hive.sem_ch.w AS SELECT CAST(v AS varchar(5)) AS v5 FROM hive.sem_ch.t",
        ],
        &[
            "SELECT table_name, column_name, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_ch' ORDER BY table_name, ordinal_position",
            "SELECT table_name, column_name, data_type FROM hive.information_schema.columns WHERE table_schema = 'sem_ch' ORDER BY table_name, ordinal_position",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem24_ctas_columns_are_nullable() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA {c}.sem_ct",
            "CREATE TABLE {c}.sem_ct.c AS SELECT 1.5 AS x, CAST(1 AS INTEGER) AS y, 'abc' AS z",
        ],
        &["SELECT column_name, ordinal_position, is_nullable, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_ct' ORDER BY ordinal_position"],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem25_memory_view_column_types() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA {c}.sem_vt",
            "CREATE TABLE {c}.sem_vt.t (id bigint, dc decimal(12,3))",
            "INSERT INTO {c}.sem_vt.t VALUES (1, 1.5)",
            "CREATE VIEW {c}.sem_vt.v AS SELECT id, dc * 2 AS dc2, avg(dc) OVER () AS a FROM {c}.sem_vt.t",
        ],
        &[
            "SELECT column_name, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_vt' AND table_name = 'v' ORDER BY ordinal_position",
            "SELECT * FROM {c}.sem_vt.v",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem26_view_definition_is_the_formatted_query() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA {c}.sem_vd",
            "CREATE TABLE {c}.sem_vd.t (id bigint, dc decimal(12,3))",
            "CREATE VIEW {c}.sem_vd.v AS SELECT id, dc * 2 AS dc2 FROM {c}.sem_vd.t",
            "CREATE SCHEMA hive.sem_vd",
            "CREATE TABLE hive.sem_vd.t (id bigint, dc decimal(12,3)) WITH (format = 'PARQUET')",
            "CREATE VIEW hive.sem_vd.v AS SELECT id, dc * 2 AS dc2 FROM hive.sem_vd.t",
        ],
        &[
            "SELECT replace(view_definition, '{c}', 'CATALOG') FROM {c}.information_schema.views WHERE table_schema = 'sem_vd'",
            "SELECT view_definition FROM hive.information_schema.views WHERE table_schema = 'sem_vd'",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem27_show_columns_and_describe() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA hive.sem_sc",
            "CREATE TABLE hive.sem_sc.t (id bigint, dc decimal(12,3), ts timestamp) WITH (format = 'PARQUET')",
        ],
        &["SHOW COLUMNS FROM hive.sem_sc.t", "DESCRIBE hive.sem_sc.t"],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem28_create_table_with_trino_type_names() -> Res<()> {
    expect_same(
        &["CREATE SCHEMA {c}.sem_ty"],
        &[
            "CREATE TABLE {c}.sem_ty.a (arr array(integer))",
            "CREATE TABLE {c}.sem_ty.m (mp map(varchar, bigint))",
            "CREATE TABLE {c}.sem_ty.r (rw row(x integer, y varchar))",
            "CREATE TABLE {c}.sem_ty.b (vb varbinary)",
            "SELECT table_name, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_ty' ORDER BY table_name",
        ],
        None,
    )
    .await
}
