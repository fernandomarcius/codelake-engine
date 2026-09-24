//! Trino 431 semantics through the whole engine: every type and value below
//! was observed on Trino 431 with the same SQL (the black-box suite
//! `trino_semantics_acceptance` compares both engines side by side).
//!
//! Types are Trino's names ([`crate::trino_types::field_display_name`]);
//! values are Arrow's text (decimals exact, timestamps ISO 8601).

use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::bytes::ByteSize;
use crate::config::{ExecutionConfig, MemoryConfig};
use crate::engine::{Engine, QueryOptions};

fn engine(dir: &std::path::Path) -> Engine {
    let memory = MemoryConfig {
        limit: ByteSize::mib(64),
        spill_dir: dir.to_owned(),
        spill_limit: ByteSize::mib(64),
        metadata_cache_limit: ByteSize::mib(1),
    };
    let execution = ExecutionConfig {
        target_partitions: Some(1),
        ..ExecutionConfig::default()
    };
    Engine::new(&memory, &execution).unwrap()
}

type Answer = (Vec<String>, Vec<Vec<String>>);

async fn run_in(engine: &Engine, zone: Option<&str>, sql: &str) -> Result<Answer, String> {
    let options = QueryOptions {
        time_zone: zone.map(str::to_owned),
        ..QueryOptions::for_user("test")
    };
    let frame = engine.sql_with(&options, sql).await.map_err(|e| e.to_string())?;
    let batches = frame.collect().await.map_err(|e| e.to_string())?;
    let Some(first) = batches.first() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let types = first
        .schema()
        .fields()
        .iter()
        .map(|f| crate::trino_types::field_display_name(f))
        .collect();
    let mut rows = Vec::new();
    for batch in &batches {
        let options = FormatOptions::default().with_null("NULL");
        let formatters: Vec<ArrayFormatter<'_>> = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(formatters.iter().map(|f| f.value(row).to_string()).collect());
        }
    }
    Ok((types, rows))
}

async fn run(engine: &Engine, sql: &str) -> Answer {
    run_in(engine, None, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn answer(types: &[&str], rows: &[&[&str]]) -> Answer {
    (
        types.iter().map(|t| (*t).to_owned()).collect(),
        rows.iter()
            .map(|r| r.iter().map(|v| (*v).to_owned()).collect())
            .collect(),
    )
}

async fn assert_same(engine: &Engine, cases: &[(&str, Answer)]) {
    for (sql, expected) in cases {
        assert_eq!(&run(engine, sql).await, expected, "{sql}");
    }
}

async fn assert_error(engine: &Engine, sql: &str, message: &str) {
    let error = run_in(engine, None, sql).await.expect_err(sql);
    assert!(error.contains(message), "{sql}: {error}");
}

#[tokio::test]
async fn numbers_meet_in_trino_s_common_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[
            (
                "SELECT x FROM (VALUES (1E-20), (1.5)) t(x) ORDER BY x",
                answer(&["double"], &[&["1e-20"], &["1.5"]]),
            ),
            (
                "SELECT x FROM (VALUES (1E300), (1.5)) t(x) ORDER BY x",
                answer(&["double"], &[&["1.5"], &["1e300"]]),
            ),
            (
                "SELECT x FROM (SELECT CAST(1.5 AS REAL) AS x UNION ALL SELECT 2.5) ORDER BY x",
                answer(&["real"], &[&["1.5"], &["2.5"]]),
            ),
            (
                "SELECT CASE WHEN x = 1 THEN 1.23456789012345678E0 ELSE 0.5 END FROM (VALUES (1)) t(x)",
                answer(&["double"], &[&["1.2345678901234567"]]),
            ),
            ("SELECT nullif(1E-20, 1.5)", answer(&["double"], &[&["1e-20"]])),
            (
                "SELECT nullif(1.5, 1E-20)",
                answer(&["decimal(2,1)"], &[&["1.5"]]),
            ),
            (
                "SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (1.5)) t(x) ORDER BY x",
                answer(&["decimal(20,1)"], &[&["1.0"], &["1.5"]]),
            ),
            (
                "SELECT coalesce(CAST(NULL AS BIGINT), 1.5)",
                answer(&["decimal(20,1)"], &[&["1.5"]]),
            ),
            (
                "SELECT 1 / 2, 7, 2147483647, 2147483648",
                answer(
                    &["integer", "integer", "integer", "bigint"],
                    &[&["0", "7", "2147483647", "2147483648"]],
                ),
            ),
            (
                "SELECT k FROM (VALUES (1), (2)) t(k) ORDER BY k",
                answer(&["integer"], &[&["1"], &["2"]]),
            ),
            (
                "SELECT 0.2 * NULL, NULL * 1.5, 1.5 / NULL, NULL % 1.5",
                answer(
                    &["decimal(2,1)", "decimal(3,1)", "decimal(2,1)", "decimal(2,1)"],
                    &[&["NULL", "NULL", "NULL", "NULL"]],
                ),
            ),
        ],
    )
    .await;
}

/// TPC-H Q17's shape: `0.2 * avg(…)` in a correlated scalar subquery.
#[tokio::test]
async fn decimal_product_in_a_correlated_subquery() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let lineitem = "(VALUES (1, CAST(1.00 AS DECIMAL(15,2)), CAST(10.00 AS DECIMAL(15,2))), \
        (1, CAST(20.00 AS DECIMAL(15,2)), CAST(20.00 AS DECIMAL(15,2))), \
        (2, CAST(3.00 AS DECIMAL(15,2)), CAST(5.00 AS DECIMAL(15,2)))) l(k, q, pr)";
    let query = |brand: &str| {
        format!(
            "SELECT sum(pr) / 7.0 AS avg_yearly FROM {lineitem}, (VALUES (1, 'a'), (2, 'a')) p(pk, b) \
             WHERE pk = k AND b = '{brand}' AND q < (SELECT 0.2 * avg(q) FROM {lineitem} WHERE k = pk)"
        )
    };
    assert_same(
        &engine,
        &[
            (&query("a"), answer(&["decimal(38,2)"], &[&["1.43"]])),
            (&query("z"), answer(&["decimal(38,2)"], &[&["NULL"]])),
        ],
    )
    .await;
}

#[tokio::test]
async fn casts_to_decimal_follow_trino() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[
            (
                "SELECT CAST(1.005E0 AS DECIMAL(5,2)), CAST(CAST(1.005 AS REAL) AS DECIMAL(5,2)), \
                 CAST(CAST(0.1 AS REAL) AS DECIMAL(20,18))",
                answer(
                    &["decimal(5,2)", "decimal(5,2)", "decimal(20,18)"],
                    &[&["1.01", "1.01", "0.100000000000000000"]],
                ),
            ),
            (
                "SELECT CAST('1e2' AS DECIMAL(5,1)), CAST('-1.5E-1' AS DECIMAL(5,2)), CAST('12.345' AS DECIMAL)",
                answer(
                    &["decimal(5,1)", "decimal(5,2)", "decimal(38,0)"],
                    &[&["100.0", "-0.15", "12"]],
                ),
            ),
            (
                "SELECT round(CAST(1.2345 AS DECIMAL(10,4)), 2), round(CAST(9.99 AS DECIMAL(3,2)), 1), \
                 round(CAST(155 AS DECIMAL(5,0)), -1), round(15, -1)",
                answer(
                    &["decimal(11,4)", "decimal(4,2)", "decimal(6,0)", "integer"],
                    &[&["1.2300", "10.00", "160", "20"]],
                ),
            ),
        ],
    )
    .await;
    assert_error(
        &engine,
        "SELECT CAST(' 12.5 ' AS DECIMAL(5,1))",
        "Cannot cast VARCHAR ' 12.5 ' to DECIMAL(5, 1). Value is not a number.",
    )
    .await;
    assert_error(
        &engine,
        "SELECT CAST(1E300 AS DECIMAL(38,0))",
        "Cannot cast DOUBLE '1.0E300' to DECIMAL(38, 0)",
    )
    .await;
}

#[tokio::test]
async fn window_ranks_and_extract_are_bigint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[(
            "SELECT row_number() OVER (ORDER BY x), rank() OVER (ORDER BY x), \
             dense_rank() OVER (ORDER BY x), extract(YEAR FROM DATE '2020-05-01') \
             FROM (VALUES (1), (2)) t(x) ORDER BY x",
            answer(
                &["bigint", "bigint", "bigint", "bigint"],
                &[&["1", "1", "1", "2020"], &["2", "2", "2", "2020"]],
            ),
        )],
    )
    .await;
}

#[tokio::test]
async fn timestamps_convert_like_trino() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[
            // The local time in the value's zone, per row.
            (
                "SELECT CAST(CAST(x AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP) FROM (VALUES \
                 (1, '2026-01-01 10:00:00 America/Sao_Paulo'), (2, '2026-01-01 10:00:00 Asia/Tokyo')) t(k, x) \
                 ORDER BY k",
                answer(
                    &["timestamp(3)"],
                    &[&["2026-01-01T10:00:00"], &["2026-01-01T10:00:00"]],
                ),
            ),
            (
                "SELECT CAST(CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP)",
                answer(&["timestamp(3)"], &[&["2026-01-01T10:00:00"]]),
            ),
            // A daylight-saving gap of the named zone moves forward.
            (
                "SELECT CAST('2018-11-04 00:30:00 America/Sao_Paulo' AS TIMESTAMP)",
                answer(&["timestamp(3)"], &[&["2018-11-04T01:30:00"]]),
            ),
            (
                "SELECT to_unixtime(CAST('1969-12-31 23:59:59.999 UTC' AS TIMESTAMP WITH TIME ZONE))",
                answer(&["double"], &[&["-0.001"]]),
            ),
            (
                "SELECT CAST(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS VARCHAR), \
                 CAST(TIMESTAMP '2026-01-01 10:00:00.123' AS VARCHAR)",
                answer(
                    &["varchar", "varchar"],
                    &[&["2019-02-16 23:30:00.000 America/Sao_Paulo", "2026-01-01 10:00:00.123"]],
                ),
            ),
            (
                "SELECT TRY_CAST('garbage' AS TIMESTAMP WITH TIME ZONE)",
                answer(&["timestamp(3) with time zone"], &[&["NULL"]]),
            ),
            (
                "SELECT CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'UTC'",
                answer(
                    &["timestamp(3) with time zone"],
                    &[&["2019-02-17T01:30:00Z"]],
                ),
            ),
            (
                "SELECT TIMESTAMP '2026-01-01 10:00:00.123', TIMESTAMP '1969-12-31 23:59:59.5', \
                 CAST('2026-01-01 10:00:00.12345' AS TIMESTAMP(4))",
                answer(
                    &["timestamp(3)", "timestamp(1)", "timestamp(4)"],
                    &[&["2026-01-01T10:00:00.123", "1969-12-31T23:59:59.500", "2026-01-01T10:00:00.123500"]],
                ),
            ),
            (
                "SELECT CAST(CAST('2026-01-01 10:00:00.1235' AS TIMESTAMP(6)) AS TIMESTAMP(3))",
                answer(&["timestamp(3)"], &[&["2026-01-01T10:00:00.124"]]),
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn the_session_zone_places_local_times() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let sql = "SELECT CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE) = \
               CAST('2026-01-01 13:00:00 UTC' AS TIMESTAMP WITH TIME ZONE), \
               CAST(TIMESTAMP '2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE), \
               CAST(DATE '2026-01-01' AS TIMESTAMP WITH TIME ZONE)";
    let (types, rows) = run_in(&engine, Some("America/Sao_Paulo"), sql).await.unwrap();
    assert_eq!(
        types,
        [
            "boolean",
            "timestamp(3) with time zone",
            "timestamp(3) with time zone"
        ]
    );
    assert_eq!(
        rows,
        [["true", "2026-01-01T10:00:00-03:00", "2026-01-01T00:00:00-03:00"]]
            .map(|r| r.map(str::to_owned).to_vec())
            .to_vec()
    );
}

#[tokio::test]
async fn bounded_text_types_are_kept_and_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    for statement in [
        "CREATE SCHEMA datafusion.vc",
        "CREATE TABLE datafusion.vc.t (id bigint, v varchar, v10 varchar(10), c3 char(3))",
        "INSERT INTO datafusion.vc.t (id, v) VALUES (1, 'abcdefgh')",
    ] {
        run(&engine, statement).await;
    }
    assert_same(
        &engine,
        &[
            (
                "SELECT CAST(v AS varchar(5)), CAST(v AS char(3)), CAST(12 AS varchar(5)) FROM datafusion.vc.t",
                answer(&["varchar(5)", "char(3)", "varchar(5)"], &[&["abcde", "abc", "12"]]),
            ),
            (
                "SELECT column_name, data_type FROM datafusion.information_schema.columns \
                 WHERE table_schema = 'vc' ORDER BY ordinal_position",
                answer(
                    &["varchar", "varchar"],
                    &[
                        &["id", "bigint"],
                        &["v", "varchar"],
                        &["v10", "varchar(10)"],
                        &["c3", "char(3)"],
                    ],
                ),
            ),
        ],
    )
    .await;
    assert_error(
        &engine,
        "INSERT INTO datafusion.vc.t (id, v10) VALUES (2, 'abcdefghijKLM')",
        "Cannot truncate non-space characters when casting from varchar(13) to varchar(10) on INSERT",
    )
    .await;
    assert_error(
        &engine,
        "SELECT CAST(123 AS VARCHAR(2))",
        "Value 123 cannot be represented as varchar(2)",
    )
    .await;
}

#[tokio::test]
async fn relations_describe_themselves_like_trino() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    for statement in [
        "CREATE SCHEMA datafusion.vw",
        "CREATE TABLE datafusion.vw.t (id bigint, dc decimal(12,3))",
        "INSERT INTO datafusion.vw.t VALUES (1, 1.5)",
        "CREATE VIEW datafusion.vw.v AS SELECT id, dc * 2 AS dc2, avg(dc) OVER () AS a FROM datafusion.vw.t",
        "CREATE TABLE datafusion.vw.c AS SELECT 1.5 AS x, CAST(1 AS INTEGER) AS y, 'abc' AS z",
        "CREATE TABLE datafusion.vw.k (arr array(integer), mp map(varchar, bigint), rw row(x integer, y varchar), vb varbinary)",
    ] {
        run(&engine, statement).await;
    }
    let columns = |table: &str| {
        format!(
            "SELECT column_name, is_nullable, data_type FROM datafusion.information_schema.columns \
             WHERE table_schema = 'vw' AND table_name = '{table}' ORDER BY ordinal_position"
        )
    };
    let text = &["varchar", "varchar", "varchar"];
    assert_same(
        &engine,
        &[
            (
                &columns("v"),
                answer(
                    text,
                    &[
                        &["id", "YES", "bigint"],
                        &["dc2", "YES", "decimal(22,3)"],
                        &["a", "YES", "decimal(12,3)"],
                    ],
                ),
            ),
            (
                "SELECT * FROM datafusion.vw.v",
                answer(
                    &["bigint", "decimal(22,3)", "decimal(12,3)"],
                    &[&["1", "3.000", "1.500"]],
                ),
            ),
            (
                "SELECT view_definition FROM datafusion.information_schema.views WHERE table_schema = 'vw'",
                answer(
                    &["varchar"],
                    &[&["SELECT\n  id\n, (dc * 2) dc2\n, avg(dc) OVER () a\nFROM\n  datafusion.vw.t\n"]],
                ),
            ),
            (
                &columns("c"),
                answer(
                    text,
                    &[
                        &["x", "YES", "decimal(2,1)"],
                        &["y", "YES", "integer"],
                        &["z", "YES", "varchar(3)"],
                    ],
                ),
            ),
            (
                &columns("k"),
                answer(
                    text,
                    &[
                        &["arr", "YES", "array(integer)"],
                        &["mp", "YES", "map(varchar, bigint)"],
                        &["rw", "YES", "row(x integer, y varchar)"],
                        &["vb", "YES", "varbinary"],
                    ],
                ),
            ),
            (
                "SELECT 'abc', x FROM (VALUES ('a'), ('bc')) t(x) ORDER BY x",
                answer(&["varchar(3)", "varchar(2)"], &[&["abc", "a"], &["abc", "bc"]]),
            ),
            (
                "SELECT 'a' AS x UNION ALL SELECT 'bcd'",
                answer(&["varchar(3)"], &[&["a"], &["bcd"]]),
            ),
        ],
    )
    .await;
}
