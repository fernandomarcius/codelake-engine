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

/// Answers of the second black-box suite (`trino_semantics_blackbox`),
/// observed on Trino 431.
#[tokio::test]
async fn second_suite_answers_follow_trino() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    for sql in [
        "CREATE SCHEMA datafusion.bx",
        "CREATE TABLE datafusion.bx.g AS SELECT CAST(x AS INTEGER) AS n FROM (VALUES 1, 2, 3) t(x)",
        "CREATE TABLE datafusion.bx.\"MiXeD\" (\"CamelCol\" bigint)",
        "CREATE VIEW datafusion.bx.\"ViewOne\" AS SELECT \"CamelCol\" AS \"OtherCol\" FROM datafusion.bx.\"MiXeD\"",
    ] {
        run(&engine, sql).await;
    }
    assert_same(
        &engine,
        &[
            // Rows of VALUES without parentheses.
            (
                "SELECT x FROM (VALUES CAST(NULL AS DECIMAL(5,2)), 1.5, -2.25) t(x) ORDER BY x",
                answer(&["decimal(5,2)"], &[&["-2.25"], &["1.50"], &["NULL"]]),
            ),
            // INTERSECT and EXCEPT answer the common type.
            (
                "SELECT v FROM (SELECT CAST(1.5 AS DECIMAL(5,1)) AS v INTERSECT SELECT CAST(1.50 AS DECIMAL(6,2)))",
                answer(&["decimal(6,2)"], &[&["1.50"]]),
            ),
            (
                "SELECT v FROM (SELECT CAST(1 AS INTEGER) AS v EXCEPT SELECT CAST(2 AS BIGINT))",
                answer(&["bigint"], &[&["1"]]),
            ),
            // ceil, floor, round, truncate and sign of decimals.
            (
                "SELECT ceil(CAST(9.5 AS DECIMAL(2,1))), floor(CAST(-9.5 AS DECIMAL(2,1))), round(CAST(9.5 AS DECIMAL(2,1))), ceil(CAST(5 AS DECIMAL(3,0)))",
                answer(
                    &["decimal(2,0)", "decimal(2,0)", "decimal(2,0)", "decimal(3,0)"],
                    &[&["10", "-10", "10", "5"]],
                ),
            ),
            (
                "SELECT ceil(CAST(99999999.9999 AS DECIMAL(12,4))), floor(CAST(-0.0005 AS DECIMAL(12,4))), round(CAST(-2.5 AS DECIMAL(12,4))), ceil(5)",
                answer(
                    &["decimal(9,0)", "decimal(9,0)", "decimal(9,0)", "integer"],
                    &[&["100000000", "-1", "-3", "5"]],
                ),
            ),
            (
                "SELECT sign(CAST(-2.5 AS DECIMAL(12,4))), truncate(CAST(-2.5 AS DECIMAL(12,4))), sign(-3), sign(-2.5E0), truncate(2.7E0), truncate(CAST(12.345 AS DECIMAL(5,3)), 1)",
                answer(
                    &["decimal(1,0)", "decimal(8,0)", "integer", "double", "double", "decimal(5,3)"],
                    &[&["-1", "-2", "-1", "-1.0", "2.0", "12.300"]],
                ),
            ),
            // Zoned values: fields, offsets, date_diff, days of 24 hours.
            (
                "SELECT hour(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), timezone_hour(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), extract(TIMEZONE_MINUTE FROM CAST('2026-06-01 12:00:00 Asia/Kathmandu' AS TIMESTAMP WITH TIME ZONE))",
                answer(&["bigint", "bigint", "bigint"], &[&["23", "-2", "45"]]),
            ),
            (
                "SELECT date_diff('hour', CAST('2018-11-03 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), CAST('2018-11-04 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), date_diff('day', DATE '2026-01-01', DATE '2025-12-01')",
                answer(&["bigint", "bigint"], &[&["23", "-31"]]),
            ),
            (
                "SELECT CAST(CAST('2018-11-03 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) + INTERVAL '1' DAY AS VARCHAR)",
                answer(&["varchar"], &[&["2018-11-04 13:00:00.000 America/Sao_Paulo"]]),
            ),
            (
                "SELECT k, CAST(CAST(x AS TIMESTAMP WITH TIME ZONE) AS DATE) FROM (VALUES (1, '1970-01-01 00:00:00 Europe/London'), (2, '2026-06-01 12:00:00 -12:00')) t(k, x) ORDER BY k",
                answer(&["integer", "date"], &[&["1", "1970-01-01"], &["2", "2026-06-01"]]),
            ),
            (
                "SELECT date_trunc('month', DATE '1995-09-17'), length('abc')",
                answer(&["date", "bigint"], &[&["1995-09-01", "3"]]),
            ),
            // NULL last in both directions.
            (
                "SELECT v FROM (VALUES (CAST(NULL AS DECIMAL(5,2))), (1.5), (10)) t(v) ORDER BY v DESC",
                answer(&["decimal(12,2)"], &[&["10.00"], &["1.50"], &["NULL"]]),
            ),
            // Aggregates that differ by a cast; real aggregates are real.
            (
                "SELECT sum(n), sum(CAST(n AS SMALLINT)), sum(CAST(n AS REAL)), avg(CAST(n AS REAL)) FROM datafusion.bx.g",
                answer(&["bigint", "bigint", "real", "real"], &[&["6", "6", "6.0", "2.0"]]),
            ),
            // Names fold to lower case in the in-memory catalog.
            (
                "SELECT table_name, column_name FROM datafusion.information_schema.columns WHERE table_schema = 'bx' AND table_name <> 'g' ORDER BY table_name",
                answer(
                    &["varchar", "varchar"],
                    &[&["mixed", "camelcol"], &["viewone", "othercol"]],
                ),
            ),
        ],
    )
    .await;
}

/// A timestamp meets a zoned value in the session zone; what Trino 431
/// rejects fails.
#[tokio::test]
async fn second_suite_session_zone_and_errors_follow_trino() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let zoned = run_in(
        &engine,
        Some("Asia/Kathmandu"),
        "SELECT k FROM (VALUES (1, TIMESTAMP '2026-01-01 10:00:00'), (2, TIMESTAMP '2026-01-01 16:00:00')) t(k, ts) WHERE ts >= CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)",
    )
    .await
    .unwrap();
    assert_eq!(zoned, answer(&["integer"], &[&["2"]]));
    assert_error(&engine, "SELECT 9223372036854775808", "Invalid numeric literal").await;
    assert_error(
        &engine,
        "SELECT CAST(x AS TIMESTAMP(13)) FROM datafusion.bx.g",
        "Unknown type: timestamp(13)",
    )
    .await;
    assert_error(
        &engine,
        "SELECT sum(x) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
        "Decimal overflow",
    )
    .await;
}

/// Text and numbers are not comparable: each of these fails on Trino 431
/// ("Cannot apply operator: integer < varchar(2)", …). `DataFusion` 53
/// compared them as text and 54 as numbers, each answering rows.
#[tokio::test]
async fn text_and_numbers_are_not_comparable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    for (sql, message) in [
        (
            "SELECT x FROM (VALUES 1, 5, 100) t(x) WHERE x > '20' ORDER BY x",
            "Cannot apply operator: ",
        ),
        (
            "SELECT CAST(5.0 AS double) = '5'",
            "Cannot apply operator: double = varchar",
        ),
        (
            "SELECT CAST('7' AS varchar) = 7",
            "Cannot apply operator: varchar = ",
        ),
        (
            "SELECT x FROM (VALUES '10', '9') t(x) WHERE x > 9 ORDER BY x",
            "Cannot apply operator: varchar > ",
        ),
        (
            "SELECT x FROM (VALUES 1, 2) t(x) WHERE x = 'a'",
            "Cannot apply operator",
        ),
        (
            "SELECT x FROM (VALUES 10, 9) t(x) WHERE x > '9.5'",
            "Cannot apply operator",
        ),
        (
            "SELECT x FROM (VALUES 1.0e0, 2.0e0) t(x) WHERE x IN ('1.0')",
            "Cannot find common type between double and varchar",
        ),
        (
            "SELECT 'a' IN ('a', 1)",
            "Cannot find common type between varchar and ",
        ),
        (
            "SELECT CASE 'x' WHEN 1.0 THEN 'a' ELSE 'b' END",
            "CASE operand type does not match WHEN clause operand type",
        ),
        ("SELECT 5 BETWEEN '1' AND 9", "is BETWEEN"),
        (
            "SELECT x FROM (VALUES 1, 2) t(x) WHERE x IN (SELECT y FROM (VALUES '1') u(y))",
            "must be of the same type",
        ),
        (
            "SELECT x FROM (VALUES 1, 2) t(x) WHERE EXISTS (SELECT 1 FROM (VALUES '1') u(y) WHERE y = t.x)",
            "Cannot apply operator: varchar = ",
        ),
    ] {
        assert_error(&engine, sql, "TYPE_MISMATCH").await;
        assert_error(&engine, sql, message).await;
    }
    // Same-kind comparisons, NULLs and explicit casts still answer.
    assert_same(
        &engine,
        &[
            (
                "SELECT 1 = 1.0, 'a' < 'b', 1 = NULL",
                answer(&["boolean", "boolean", "boolean"], &[&["true", "true", "NULL"]]),
            ),
            (
                "SELECT x FROM (VALUES 1, 5, 100) t(x) WHERE CAST(x AS varchar) > '20' ORDER BY x",
                answer(&["integer"], &[&["5"]]),
            ),
            (
                "SELECT x FROM (VALUES 1, 5, 100) t(x) WHERE x > CAST('20' AS integer) ORDER BY x",
                answer(&["integer"], &[&["100"]]),
            ),
        ],
    )
    .await;
}

/// A view whose body compares text with a number, stored without the
/// engine's checks (by an older build, or straight through `DataFusion`),
/// is refused when a statement reads it.
#[tokio::test]
async fn a_stored_view_comparing_text_with_a_number_is_refused_when_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let body = engine
        .context()
        .sql("SELECT x FROM (VALUES ('10'), ('9')) t(x) WHERE x > 9")
        .await
        .unwrap()
        .into_unoptimized_plan();
    engine
        .context()
        .register_table(
            "text_view",
            std::sync::Arc::new(datafusion::datasource::ViewTable::new(body, None)),
        )
        .unwrap();
    assert_error(&engine, "SELECT count(*) FROM text_view", "TYPE_MISMATCH").await;
    assert_error(
        &engine,
        "SELECT count(*) FROM text_view",
        "Cannot apply operator: varchar > ",
    )
    .await;
    // A view whose body is well typed still answers.
    let body = engine
        .context()
        .sql("SELECT x FROM (VALUES ('10'), ('9')) t(x) WHERE x > '9'")
        .await
        .unwrap()
        .into_unoptimized_plan();
    engine
        .context()
        .register_table(
            "typed_view",
            std::sync::Arc::new(datafusion::datasource::ViewTable::new(body, None)),
        )
        .unwrap();
    assert_same(
        &engine,
        &[("SELECT count(*) FROM typed_view", answer(&["bigint"], &[&["0"]]))],
    )
    .await;
}

/// `time ± interval` is refused: `DataFusion` 54 answers an interval
/// (`24 hours 30 mins`) where Trino 431 answers the time `00:30:00`.
#[tokio::test]
async fn time_plus_interval_is_refused_instead_of_answering_an_interval() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    for sql in [
        "SELECT TIME '23:30:00' + INTERVAL '1' HOUR",
        "SELECT TIME '00:30:00' - INTERVAL '1' HOUR",
        "SELECT INTERVAL '1' HOUR + TIME '23:30:00'",
    ] {
        assert_error(&engine, sql, "NOT_SUPPORTED").await;
    }
    // Dates and timestamps keep their arithmetic.
    assert_same(
        &engine,
        &[(
            "SELECT DATE '2024-01-31' + INTERVAL '1' DAY",
            answer(&["date"], &[&["2024-02-01"]]),
        )],
    )
    .await;
}

/// `NOT IN` with a NULL in the subquery (or a NULL value) is UNKNOWN: the
/// row is filtered out. Every answer below is Trino 431's; `DataFusion`
/// answered the constant and correlated forms without the NULL semantics.
/// The forms that could read UNKNOWN as FALSE are refused instead
/// (`in_subquery_is_refused_where_unknown_would_read_as_false`).
#[tokio::test]
async fn not_in_keeps_null_semantics_where_it_answers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[
            (
                "SELECT count(*) FROM (VALUES 1, 2) t(x) WHERE 3 NOT IN (SELECT y FROM (VALUES 1, NULL) u(y))",
                answer(&["bigint"], &[&["0"]]),
            ),
            (
                "SELECT x FROM (VALUES 1, 2) t(x) WHERE 3 NOT IN (SELECT y FROM (VALUES 1, 2) u(y)) ORDER BY x",
                answer(&["integer"], &[&["1"], &["2"]]),
            ),
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE x NOT IN (SELECT y FROM (VALUES 1, NULL) u(y))",
                answer(&["bigint"], &[&["0"]]),
            ),
            (
                "SELECT id FROM (VALUES (1, 1), (2, 2)) o(id, g) \
                 WHERE 3 NOT IN (SELECT i FROM (VALUES (1, 1), (NULL, 2)) n(i, g) WHERE n.g = o.g) ORDER BY id",
                answer(&["integer"], &[&["1"]]),
            ),
            (
                "SELECT id FROM (VALUES (1, 1), (2, 2), (3, 3)) o(id, g) \
                 WHERE id NOT IN (SELECT i FROM (VALUES (1, 1), (NULL, 2), (5, 3)) n(i, g) WHERE n.g = o.g) \
                 ORDER BY id",
                answer(&["integer"], &[&["3"]]),
            ),
            (
                "SELECT id FROM (VALUES (1, CAST(NULL AS integer)), (2, 5)) o(id, k) \
                 WHERE k NOT IN (SELECT i FROM (VALUES (1, 7)) n(g, i) WHERE n.g = o.id) ORDER BY id",
                answer(&["integer"], &[&["2"]]),
            ),
            // A negated membership joined by AND, spelled NOT (x IN …), over
            // a computed value, in HAVING, over an outer join's NULLs.
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) \
                 WHERE x NOT IN (SELECT y FROM (VALUES 1, NULL) u(y)) AND x > 1",
                answer(&["bigint"], &[&["0"]]),
            ),
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE NOT (x IN (SELECT y FROM (VALUES 1, NULL) u(y)))",
                answer(&["bigint"], &[&["0"]]),
            ),
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE (x + 0) NOT IN (SELECT y FROM (VALUES 1, NULL) u(y))",
                answer(&["bigint"], &[&["0"]]),
            ),
            (
                "SELECT g FROM (VALUES 1, 2) t(g) GROUP BY g HAVING g NOT IN (SELECT y FROM (VALUES 1, NULL) u(y))",
                (Vec::new(), Vec::new()),
            ),
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) LEFT JOIN (VALUES 2) w(v) ON t.x = w.v \
                 WHERE w.v NOT IN (SELECT y FROM (VALUES 7) u(y))",
                answer(&["bigint"], &[&["1"]]),
            ),
            // Where FALSE and UNKNOWN filter the same rows the mark join
            // answers: a membership under OR, or a NOT IN under two NOTs.
            (
                "SELECT x FROM (VALUES 1, 2, 3) t(x) \
                 WHERE x IN (SELECT y FROM (VALUES 1, NULL) u(y)) OR x = 3 ORDER BY x",
                answer(&["integer"], &[&["1"], &["3"]]),
            ),
            (
                "SELECT x FROM (VALUES 1, 2, 3) t(x) \
                 WHERE NOT (x NOT IN (SELECT y FROM (VALUES 1, NULL) u(y)) AND x < 3) ORDER BY x",
                answer(&["integer"], &[&["1"], &["3"]]),
            ),
            // NOT over OR is a conjunction once simplified: `x NOT IN … AND x <> 99`.
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) \
                 WHERE NOT (x IN (SELECT y FROM (VALUES 1, NULL) u(y)) OR x = 99)",
                answer(&["bigint"], &[&["0"]]),
            ),
        ],
    )
    .await;
}

/// Outside a conjunct of a `WHERE` or `HAVING`, `DataFusion` answers
/// `x [NOT] IN (subquery)` with a mark join, which reads UNKNOWN as FALSE:
/// the first two statements answered 2 where Trino 431 answers 0. Every form
/// where that matters is refused when a NULL is possible on either side
/// (`join_guard`, guard 5); without a possible NULL it answers.
#[tokio::test]
async fn in_subquery_is_refused_where_unknown_would_read_as_false() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let with_null = "(SELECT y FROM (VALUES 1, NULL) u(y))";
    for sql in [
        format!("SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE x NOT IN {with_null} OR x = 99"),
        format!(
            "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE CASE WHEN x NOT IN {with_null} THEN true ELSE false END"
        ),
        format!("SELECT x, x NOT IN {with_null} FROM (VALUES 1, 2) t(x)"),
        format!("SELECT x, x IN {with_null} FROM (VALUES 1, 2) t(x)"),
        format!("SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE coalesce(x NOT IN {with_null}, false)"),
        format!("SELECT count(*) FROM (VALUES 1, 2, 3) t(x) WHERE (x IN {with_null}) IS NULL"),
        // A NULL value of `x` is UNKNOWN too, whatever the subquery holds.
        "SELECT count(*) FROM (VALUES 1, CAST(NULL AS integer)) t(x) \
         WHERE x NOT IN (SELECT y FROM (VALUES 5) u(y)) OR x = 99"
            .to_owned(),
        // Inside a subquery as well.
        format!(
            "SELECT count(*) FROM (VALUES 1, 2) o(x) \
             WHERE EXISTS (SELECT 1 FROM (VALUES 5) i(z) WHERE i.z > 0 AND (o.x NOT IN {with_null} OR i.z = 9))"
        ),
    ] {
        assert_error(&engine, &sql, "NOT_SUPPORTED").await;
        assert_error(&engine, &sql, "may be NULL").await;
    }
    // No NULL possible on either side (VALUES columns may hold one; coalesce
    // cannot): the mark join is right.
    assert_same(
        &engine,
        &[
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) \
                 WHERE coalesce(x, 0) NOT IN (SELECT coalesce(y, 0) FROM (VALUES 1, 2) u(y)) OR x = 1",
                answer(&["bigint"], &[&["2"]]),
            ),
            (
                "SELECT count(*) FROM (VALUES 1, 2, 3) t(x) \
                 WHERE CASE WHEN coalesce(x, 0) NOT IN (SELECT coalesce(y, 0) FROM (VALUES 1, 2) u(y)) \
                       THEN true ELSE false END",
                answer(&["bigint"], &[&["1"]]),
            ),
        ],
    )
    .await;
}

/// Timestamps of different precisions meet at the higher one, as in Trino
/// 431; `DataFusion` truncated the finer side (the equality was true).
#[tokio::test]
async fn timestamps_of_different_precisions_compare_at_the_finer_one() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[
            (
                "SELECT TIMESTAMP '2020-01-01 00:00:00.123' = TIMESTAMP '2020-01-01 00:00:00.123400', \
                 TIMESTAMP '2020-01-01 00:00:00.123' >= TIMESTAMP '2020-01-01 00:00:00.123500', \
                 TIMESTAMP '2020-01-01 00:00:00.123' < TIMESTAMP '2020-01-01 00:00:00.123001', \
                 TIMESTAMP '2020-01-01 00:00:00' = TIMESTAMP '2020-01-01 00:00:00.000'",
                answer(
                    &["boolean", "boolean", "boolean", "boolean"],
                    &[&["false", "false", "true", "true"]],
                ),
            ),
            (
                "SELECT k FROM (VALUES (1, TIMESTAMP '2020-01-01 00:00:00.123'), (2, TIMESTAMP '2020-01-01 00:00:00.124')) t(k, ts) \
                 WHERE ts BETWEEN TIMESTAMP '2020-01-01 00:00:00.123100' AND TIMESTAMP '2020-01-01 00:00:00.124000'",
                answer(&["integer"], &[&["2"]]),
            ),
            (
                "SELECT k FROM (VALUES (1, TIMESTAMP '2020-01-01 00:00:00.123'), (2, TIMESTAMP '2020-01-01 00:00:00.124')) t(k, ts) \
                 WHERE ts IN (TIMESTAMP '2020-01-01 00:00:00.123400', TIMESTAMP '2020-01-01 00:00:00.124000')",
                answer(&["integer"], &[&["2"]]),
            ),
            (
                "SELECT coalesce(CAST(NULL AS TIMESTAMP(3)), TIMESTAMP '2020-01-01 00:00:00.123456')",
                answer(&["timestamp(6)"], &[&["2020-01-01T00:00:00.123456"]]),
            ),
        ],
    )
    .await;
}

/// A window ordered by a `varbinary` key without a frame (the default
/// `RANGE … CURRENT ROW`, peers included) plans as in Trino 431.
#[tokio::test]
async fn windows_ordered_by_varbinary_keep_range_peers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    assert_same(
        &engine,
        &[(
            "SELECT k, rank() OVER (ORDER BY b), count(*) OVER (ORDER BY b), \
             count(*) OVER (ORDER BY b RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
             FROM (VALUES (1, X'02'), (2, X'01'), (3, X'01'), (4, X'0100')) t(k, b) ORDER BY k",
            answer(
                &["integer", "bigint", "bigint", "bigint"],
                &[
                    &["1", "4", "4", "1"],
                    &["2", "1", "2", "4"],
                    &["3", "1", "2", "4"],
                    &["4", "3", "3", "2"],
                ],
            ),
        )],
    )
    .await;
}
