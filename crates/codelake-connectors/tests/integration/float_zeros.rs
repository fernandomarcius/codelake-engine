//! `0.0` and `-0.0` are one value (ADR 0017, ISS-050), checked by rows over
//! Hive tables whose zeros sit in files of their own (one `INSERT` each, so a
//! scan that prunes row groups by statistics would skip them). Trino 431 is
//! the reference for comparisons, `IS [NOT] DISTINCT FROM`, `IN` lists of
//! `double`, grouping and set operations; PostgreSQL 17 for joins,
//! `IN (subquery)` and `IN` lists of `real`, where Trino 431 does not match
//! `0.0` with `-0.0` and the engine follows PostgreSQL (NaN aside: the
//! engine keeps IEEE 754's NaN, as Trino).

use crate::join_regressions::{Rows, Trino, note, open, shared, sorted, try_cl};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// `z`: the two zeros, one and minus one, NaN and NULL, as `double` (`x`)
/// and `real` (`r`); `g`: the two zeros and NaN with a tag.
const TRINO_SETUP: [&str; 10] = [
    "CREATE SCHEMA hive.fz",
    "CREATE TABLE hive.fz.z (id bigint, x double, r real) WITH (format = 'PARQUET')",
    "INSERT INTO hive.fz.z VALUES (1, 0.0E0, REAL '0.0')",
    "INSERT INTO hive.fz.z VALUES (2, -0.0E0, REAL '-0.0')",
    "INSERT INTO hive.fz.z VALUES (3, 1.0E0, REAL '1.0'), (4, -1.0E0, REAL '-1.0')",
    "INSERT INTO hive.fz.z VALUES (5, nan(), CAST(nan() AS real)), (6, NULL, NULL)",
    "CREATE TABLE hive.fz.g (x double, tag varchar) WITH (format = 'PARQUET')",
    "INSERT INTO hive.fz.g VALUES (0.0E0, 'pos')",
    "INSERT INTO hive.fz.g VALUES (-0.0E0, 'neg')",
    "INSERT INTO hive.fz.g VALUES (nan(), 'nan')",
];

const POSTGRES_SETUP: &str = "CREATE TABLE z (id bigint, x double precision, r real);
     INSERT INTO z VALUES (1, 0.0, 0.0), (2, '-0', '-0'), (3, 1, 1), (4, -1, -1), (5, 'NaN', 'NaN'), (6, NULL, NULL);
     CREATE TABLE g (x double precision, tag text);
     INSERT INTO g VALUES (0.0, 'pos'), ('-0', 'neg'), ('NaN', 'nan');";

/// Answered as Trino 431 answers them (ids and counts only, never a float).
const TRINO_CASES: [&str; 33] = [
    "SELECT id FROM hive.fz.z WHERE x = 0",
    "SELECT id FROM hive.fz.z WHERE x = -0.0E0",
    "SELECT id FROM hive.fz.z WHERE 0 = x",
    "SELECT id FROM hive.fz.z WHERE x <> 0",
    "SELECT id FROM hive.fz.z WHERE NOT (x = 0)",
    "SELECT id FROM hive.fz.z WHERE x < 0",
    "SELECT id FROM hive.fz.z WHERE x <= -0.0E0",
    "SELECT id FROM hive.fz.z WHERE x > -0.0E0",
    "SELECT id FROM hive.fz.z WHERE x >= 0",
    "SELECT id FROM hive.fz.z WHERE x IN (0, 1)",
    "SELECT id FROM hive.fz.z WHERE x NOT IN (0, 1)",
    "SELECT id FROM hive.fz.z WHERE x IN (-0.0E0, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)",
    "SELECT id FROM hive.fz.z WHERE x BETWEEN 0 AND 1",
    "SELECT id FROM hive.fz.z WHERE x IS NOT DISTINCT FROM -0.0E0",
    "SELECT id FROM hive.fz.z WHERE x IS DISTINCT FROM 0",
    "SELECT id FROM hive.fz.z WHERE r = 0",
    "SELECT id FROM hive.fz.z WHERE r = REAL '-0.0'",
    "SELECT id FROM hive.fz.z WHERE r >= 0",
    "SELECT id FROM hive.fz.z WHERE r IS NOT DISTINCT FROM REAL '-0.0'",
    "SELECT count(*) FROM (VALUES REAL '0.0', REAL '-0.0', REAL '1.0') v(r) WHERE r = 0",
    "SELECT a.id, b.id FROM hive.fz.z a, hive.fz.z b WHERE a.x IS DISTINCT FROM b.x AND a.id < 3 AND b.id < 3",
    "SELECT a.id, b.id FROM hive.fz.z a, hive.fz.z b WHERE a.x < b.x",
    "SELECT count(*) FROM hive.fz.z a, hive.fz.z b WHERE a.x IS NOT DISTINCT FROM b.x",
    "SELECT count(*) FROM (SELECT x FROM hive.fz.z GROUP BY x)",
    "SELECT count(*) FROM (SELECT DISTINCT r FROM hive.fz.z)",
    "SELECT count(*) FROM (SELECT x FROM hive.fz.z GROUP BY ROLLUP (x))",
    "SELECT count(DISTINCT x), count(DISTINCT r) FROM hive.fz.z",
    "SELECT count(*) FROM (SELECT x FROM hive.fz.z UNION SELECT -x FROM hive.fz.z)",
    "SELECT count(*) FROM (SELECT x FROM hive.fz.z INTERSECT SELECT -x FROM hive.fz.z)",
    "SELECT count(*) FROM (SELECT x FROM hive.fz.z EXCEPT SELECT -x FROM hive.fz.z)",
    "SELECT id FROM hive.fz.z WHERE CASE x WHEN 0 THEN true ELSE false END",
    "SELECT id FROM hive.fz.z WHERE nullif(x, 0) IS NULL",
    "SELECT id, count(*) OVER (PARTITION BY x) FROM hive.fz.z WHERE x = 0",
];

/// Answered as PostgreSQL 17 answers them; `{t}` is the table's schema
/// prefix (`hive.fz.` in the engine, none in PostgreSQL). Without the NaN
/// rows: PostgreSQL has NaN equal to itself and above every number, the
/// engine keeps IEEE 754's NaN, as Trino does.
const POSTGRES_CASES: [&str; 10] = [
    // Trino 431 answers no row: it intersects the ranges ordering -0.0
    // below 0.0.
    "SELECT id FROM {t}z WHERE x = 0 AND x >= 0 AND x <= -0.0E0",
    "SELECT count(*) FROM {t}z a JOIN {t}z b ON a.x = b.x WHERE a.id <> 5",
    "SELECT count(*) FROM {t}z a JOIN {t}g b ON a.x = b.x WHERE b.tag <> 'nan'",
    "SELECT b.tag, count(*) FROM {t}z a JOIN {t}g b ON a.x = b.x WHERE b.tag <> 'nan' GROUP BY b.tag",
    "SELECT count(*) FROM {t}z a JOIN {t}z b ON a.r = b.r WHERE a.id <> 5",
    "SELECT id FROM {t}z WHERE x IN (SELECT x FROM {t}g WHERE tag <> 'nan')",
    "SELECT id FROM {t}z WHERE x NOT IN (SELECT x FROM {t}g WHERE tag <> 'nan') AND id <> 5",
    "SELECT id FROM {t}z a WHERE EXISTS (SELECT 1 FROM {t}g b WHERE b.x = a.x AND b.tag <> 'nan')",
    "SELECT id FROM {t}z WHERE r IN (0, 1)",
    "SELECT id FROM {t}z WHERE r = 0 OR r = 1",
];

async fn postgres_rows(client: &tokio_postgres::Client, sql: &str) -> Rows {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("PostgreSQL `{sql}`: {e}"))
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn the_two_zeros_are_one_value() {
    let shared = shared().await;
    let (trino, postgres) = tokio::join!(
        Trino::start(&shared, "zeros"),
        Postgres::default().with_tag("17-alpine").start()
    );
    let postgres = postgres.unwrap();
    trino.batch(&TRINO_SETUP).await;
    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!(
        "host={} port={port} user=postgres password=postgres dbname=postgres",
        crate::common::host()
    );
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client.batch_execute(POSTGRES_SETUP).await.unwrap();
    let spill = tempfile::tempdir().unwrap();
    let engine = open(
        &shared.warehouse(),
        spill.path(),
        ByteSize::mib(256),
        &ExecutionConfig::default(),
        &[],
    )
    .await;
    let mut failures = Vec::new();
    let references = trino.batch(&TRINO_CASES).await;
    for (sql, reference) in TRINO_CASES.iter().zip(references) {
        let reference = sorted(reference);
        let rows = try_cl(&engine, sql).await.map(sorted);
        note(
            "zeros vs Trino",
            format!("trino={reference:?} codelake={rows:?}: {sql}"),
        );
        if rows.as_ref() != Ok(&reference) {
            failures.push(format!("trino={reference:?} codelake={rows:?}\n  {sql}"));
        }
    }
    for template in POSTGRES_CASES {
        let reference = sorted(postgres_rows(&client, &template.replace("{t}", "")).await);
        let sql = template.replace("{t}", "hive.fz.");
        let rows = try_cl(&engine, &sql).await.map(sorted);
        note(
            "zeros vs PostgreSQL",
            format!("postgres={reference:?} codelake={rows:?}: {sql}"),
        );
        if rows.as_ref() != Ok(&reference) {
            failures.push(format!("postgres={reference:?} codelake={rows:?}\n  {sql}"));
        }
    }
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
}
