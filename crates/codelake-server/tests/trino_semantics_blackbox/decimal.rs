//! Decimal arithmetic and aggregation through table rows: signs and
//! rounding of every operator and function, decimals mixed with every
//! integer type, aggregates over generated rows, joins, ordering, casts,
//! literal typing and 38-digit overflow.

use super::{Pair, Res, expect_errors, expect_rows, verdict};

/// Rows of `decimal(12,4)` and `decimal(6,2)` operands: signs, halves,
/// tiny and extreme magnitudes, and NULL.
const SIGNS_SETUP: &[&str] = &[
    "CREATE SCHEMA {c}.bx_dec",
    "CREATE TABLE {c}.bx_dec.s AS SELECT CAST(k AS BIGINT) AS k, CAST(a AS DECIMAL(12,4)) AS a, CAST(b AS DECIMAL(6,2)) AS b FROM (VALUES \
        (1, '10.0000', '3.00'), (2, '-10.0000', '3.00'), (3, '1.0000', '-0.03'), (4, '-0.0005', '0.02'), \
        (5, '12345678.9999', '-0.07'), (6, '2.5000', '2.00'), (7, '-2.5000', '2.00'), (8, '0.0000', '1.00'), \
        (9, NULL, '1.00'), (10, '99999999.9999', '9999.99'), (11, '-99999999.9999', '0.01'), (12, '7.5000', '-2.00'), \
        (13, '-0.0050', '-0.01'), (14, '0.0149', '0.03')) t(k, a, b)",
];

/// `+ - * / %`, both ways, with division and modulo rounding per sign, and
/// the functions that keep or change the scale.
#[tokio::test]
async fn operators_and_functions_per_row_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(SIGNS_SETUP).await?;
    let from = "FROM {c}.bx_dec.s ORDER BY k";
    let groups: [&[&str]; 4] = [
        &["a + b", "a - b", "a * b", "a / b", "a % b", "b - a"],
        &["b / nullif(a, 0)", "b % nullif(a, 0)", "-a", "-b", "abs(a)"],
        &["round(a, 1)", "round(a, -1)", "round(b, 1)"],
        &[
            "a / 3", "a * 100", "2 * b", "b * 1.0", "1 - a", "a + 1.5", "a / 0.3", "a % 3",
        ],
    ];
    let mut wrong = Vec::new();
    for group in groups {
        let exprs: Vec<String> = group.iter().map(|e| (*e).to_owned()).collect();
        wrong.extend(pair.batch(&exprs, from, None).await?);
    }
    verdict(&wrong)
}

/// Decimals of two widths mixed with `tinyint`, `smallint`, `integer` and
/// `bigint` columns at their extremes.
#[tokio::test]
async fn decimal_with_integer_types_matches_trino() -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(&[
        "CREATE SCHEMA {c}.bx_int",
        "CREATE TABLE {c}.bx_int.i AS SELECT CAST(k AS BIGINT) AS k, CAST(t AS TINYINT) AS t, CAST(s AS SMALLINT) AS s, CAST(n AS INTEGER) AS n, CAST(g AS BIGINT) AS g, CAST(d5 AS DECIMAL(5,2)) AS d5, CAST(d20 AS DECIMAL(20,5)) AS d20 FROM (VALUES \
            (1, 127, 32767, 2147483647, 9223372036854775807, '999.99', '12345.12345'), \
            (2, -128, -32768, -2147483648, -9223372036854775807, '-0.01', '-0.00001'), \
            (3, 3, 7, -9, 11, '2.50', '1.50000')) x(k, t, s, n, g, d5, d20)",
    ])
    .await?;
    let from = "FROM {c}.bx_int.i ORDER BY k";
    let mut wrong = Vec::new();
    for int in ["t", "s", "n", "g"] {
        for dec in ["d5", "d20"] {
            let exprs = vec![
                format!("{int} + {dec}"),
                format!("{dec} - {int}"),
                format!("{int} * {dec}"),
                format!("{dec} / {int}"),
                format!("{int} / {dec}"),
                format!("{dec} % {int}"),
                format!("{int} % {dec}"),
            ];
            wrong.extend(pair.batch(&exprs, from, None).await?);
        }
    }
    verdict(&wrong)
}

/// A thousand generated rows: `n` 0..999 as integer, `v = (n - 500) / 7`
/// with six decimals, `z` the last digit as tinyint.
const GENERATED: &[&str] = &[
    "CREATE SCHEMA {c}.bx_agg",
    "CREATE TABLE {c}.bx_agg.g AS SELECT CAST(x * 100 + y * 10 + z AS INTEGER) AS n, \
        CAST(CAST(x * 100 + y * 10 + z - 500 AS DECIMAL(10,2)) / 7 AS DECIMAL(12,6)) AS v, \
        CAST(z AS TINYINT) AS z, CAST(x * 100 + y * 10 + z AS DECIMAL(3,0)) AS d \
        FROM (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) a(x) \
        CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) b(y) \
        CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) c(z)",
];

const GENERATED_QUERIES: &[&str] = &[
    "SELECT count(*), sum(v), avg(v), min(v), max(v), sum(d), avg(d), sum(v * v), avg(v / 7), avg(-v) FROM {c}.bx_agg.g",
    "SELECT z, sum(v), avg(v), sum(d), avg(d), avg(d / 3), count(DISTINCT v) FROM {c}.bx_agg.g GROUP BY z ORDER BY z",
    "SELECT n % 3 AS m, sum(v) FILTER (WHERE v < 0), avg(v) FILTER (WHERE v > 0), avg(v) FILTER (WHERE n < 0) FROM {c}.bx_agg.g GROUP BY n % 3 ORDER BY m",
    // Aggregates of the other numeric types (Trino: sum of integers is
    // bigint, avg of integers and bigint is double; real is DEFECT-SEM46).
    // One cast of `n` per query: see DEFECT-SEM41.
    "SELECT sum(n), avg(n), sum(z), avg(z) FROM {c}.bx_agg.g",
    "SELECT sum(CAST(n AS SMALLINT)), avg(CAST(n AS SMALLINT)) FROM {c}.bx_agg.g",
    "SELECT sum(CAST(n AS BIGINT)), avg(CAST(n AS BIGINT)) FROM {c}.bx_agg.g",
    "SELECT sum(CAST(n AS DOUBLE)), avg(CAST(n AS DOUBLE)) FROM {c}.bx_agg.g",
    "SELECT sum(v), avg(v), sum(n), avg(n), sum(z), count(v), count(*) FROM {c}.bx_agg.g WHERE n < 0",
    "SELECT min(v), max(v), min(d), max(d) FROM {c}.bx_agg.g WHERE z = 7",
    // Windows: running sums, a partition average, and frames that are
    // empty at the end of the partition.
    "SELECT * FROM (SELECT n, sum(v) OVER (PARTITION BY z ORDER BY n ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s3, avg(v) OVER (PARTITION BY z) AS pz, avg(v) OVER (PARTITION BY z ORDER BY n ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS nxt, sum(d) OVER (ORDER BY n) AS run FROM {c}.bx_agg.g) WHERE n % 97 = 0 OR n >= 990 ORDER BY n",
    "SELECT * FROM (SELECT n, avg(d) OVER (ORDER BY n RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a, min(v) OVER (PARTITION BY n % 10) AS mn FROM {c}.bx_agg.g) WHERE n % 111 = 0 ORDER BY n",
];

#[tokio::test]
async fn aggregates_over_generated_rows_match_trino() -> Res<()> {
    expect_rows(GENERATED, GENERATED_QUERIES, None).await
}

/// `avg` rounds half away from zero on the exact sum, for every scale.
const AVG_ROUNDING: &[&str] = &[
    "SELECT avg(x) FROM (VALUES (CAST(1 AS DECIMAL(10,0))), (CAST(2 AS DECIMAL(10,0)))) t(x)",
    "SELECT avg(x) FROM (VALUES (CAST(-1 AS DECIMAL(10,0))), (CAST(-2 AS DECIMAL(10,0)))) t(x)",
    "SELECT avg(x) FROM (VALUES (CAST(0.01 AS DECIMAL(5,2))), (CAST(0.02 AS DECIMAL(5,2)))) t(x)",
    "SELECT avg(x) FROM (VALUES (CAST(-0.01 AS DECIMAL(5,2))), (CAST(-0.02 AS DECIMAL(5,2)))) t(x)",
    "SELECT avg(x) FROM (VALUES (CAST('0.00000000000000000000000000000000000001' AS DECIMAL(38,38))), (CAST('0.00000000000000000000000000000000000002' AS DECIMAL(38,38)))) t(x)",
    "SELECT avg(x), sum(x) FROM (VALUES (CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0))), (CAST('12345678901234567890123456789012345679' AS DECIMAL(38,0)))) t(x)",
    "SELECT sum(DISTINCT x), count(DISTINCT x), avg(DISTINCT x) FROM (VALUES (CAST(1.5 AS DECIMAL(5,2))), (CAST(1.50 AS DECIMAL(5,2))), (CAST(2.25 AS DECIMAL(5,2)))) t(x)",
    "SELECT k, count(*) FROM (SELECT CAST(1.5 AS DECIMAL(5,1)) AS k UNION ALL SELECT CAST(1.50 AS DECIMAL(6,2)) UNION ALL SELECT CAST(-0.0 AS DECIMAL(3,1))) GROUP BY k ORDER BY k",
    "SELECT stddev(x), var_samp(x), stddev_pop(x), var_pop(x) FROM (VALUES (CAST(1 AS DECIMAL(5,2))), (CAST(2 AS DECIMAL(5,2))), (CAST(3 AS DECIMAL(5,2))), (CAST(4 AS DECIMAL(5,2)))) t(x)",
];

#[tokio::test]
async fn avg_rounding_and_distinct_match_trino() -> Res<()> {
    expect_rows(&[], AVG_ROUNDING, None).await
}

/// Comparisons, joins and set operations across scales, integers and
/// doubles; ordering with NULLs in both directions.
const COMPARISONS: &[&str] = &[
    "SELECT a.k, b.k FROM (VALUES (CAST(1.5 AS DECIMAL(5,1)), 1), (CAST(2 AS DECIMAL(5,1)), 2), (CAST(-0.1 AS DECIMAL(5,1)), 3)) a(v, k) JOIN (VALUES (CAST(1.50 AS DECIMAL(10,2)), 10), (CAST(2.00 AS DECIMAL(10,2)), 20), (CAST(-0.10 AS DECIMAL(10,2)), 30), (CAST(1.51 AS DECIMAL(10,2)), 40)) b(v, k) ON a.v = b.v ORDER BY a.k, b.k",
    "SELECT a.k, b.k FROM (VALUES (CAST(2 AS DECIMAL(5,1)), 1), (CAST(2.5 AS DECIMAL(5,1)), 2)) a(v, k) JOIN (VALUES (CAST(2 AS BIGINT), 10), (CAST(3 AS BIGINT), 20)) b(v, k) ON a.v = b.v ORDER BY a.k, b.k",
    "SELECT a.k, b.k FROM (VALUES (CAST(0.1 AS DECIMAL(5,1)), 1), (CAST(0.5 AS DECIMAL(5,1)), 2)) a(v, k) JOIN (VALUES (0.1E0, 10), (0.5E0, 20)) b(v, k) ON a.v = b.v ORDER BY a.k, b.k",
    "SELECT v FROM (VALUES (CAST(NULL AS DECIMAL(5,2))), (1.5), (-2.25), (0), (10)) t(v) ORDER BY v",
    "SELECT v FROM (VALUES (CAST(NULL AS DECIMAL(5,2))), (1.5), (-2.25)) t(v) ORDER BY v NULLS FIRST",
    "SELECT DISTINCT v FROM (SELECT CAST(1.5 AS DECIMAL(5,1)) AS v UNION ALL SELECT CAST(1.50 AS DECIMAL(6,2)) UNION ALL SELECT 1.500) ORDER BY v",
    "SELECT v FROM (SELECT CAST(1.5 AS DECIMAL(5,1)) AS v UNION SELECT CAST(1.50 AS DECIMAL(6,2))) ORDER BY v",
    "SELECT k FROM (VALUES (1, CAST(1.5 AS DECIMAL(5,2))), (2, CAST(2.25 AS DECIMAL(5,2))), (3, CAST(1 AS DECIMAL(5,2)))) t(k, v) WHERE v = 1.5E0 OR v > 2 OR v IN (1, 7.77) ORDER BY k",
    "SELECT k FROM (VALUES (1, CAST(1.5 AS DECIMAL(5,2))), (2, CAST(2.25 AS DECIMAL(5,2))), (3, CAST(1 AS DECIMAL(5,2)))) t(k, v) WHERE v BETWEEN 1.4999 AND 2.2500 ORDER BY k",
    "SELECT CASE k WHEN 1 THEN CAST(1.5 AS DECIMAL(5,1)) WHEN 2 THEN CAST(2.255 AS DECIMAL(10,3)) ELSE NULL END FROM (VALUES (1), (2), (3)) t(k) ORDER BY k",
    "SELECT CASE WHEN k = 1 THEN CAST(1.5 AS DECIMAL(5,1)) ELSE CAST(k AS BIGINT) END FROM (VALUES (1), (2)) t(k) ORDER BY k",
    "SELECT greatest(CAST(1.5 AS DECIMAL(5,1)), 2), least(CAST(1.5 AS DECIMAL(5,1)), CAST(1 AS BIGINT)), greatest(CAST(1.5 AS DECIMAL(5,1)), 2.5E0)",
];

#[tokio::test]
async fn comparisons_joins_and_ordering_match_trino() -> Res<()> {
    expect_rows(&[], COMPARISONS, None).await
}

/// Casts from and to decimal.
const CASTS: &[&str] = &[
    "SELECT CAST(CAST(-1.45 AS DECIMAL(5,2)) AS DECIMAL(5,1)), CAST(CAST(-1.44 AS DECIMAL(5,2)) AS DECIMAL(5,1)), CAST(CAST(-0.05 AS DECIMAL(3,2)) AS DECIMAL(3,1)), CAST(CAST(9.95 AS DECIMAL(3,2)) AS DECIMAL(3,1))",
    "SELECT CAST(CAST(2.5 AS DECIMAL(5,1)) AS TINYINT), CAST(CAST(-2.5 AS DECIMAL(5,1)) AS SMALLINT), CAST(CAST(-0.5 AS DECIMAL(5,1)) AS INTEGER), CAST(CAST(0.49 AS DECIMAL(5,2)) AS BIGINT)",
    "SELECT CAST(CAST(1.1 AS DECIMAL(5,1)) AS REAL), CAST(CAST(1.1 AS DECIMAL(5,1)) AS DOUBLE), CAST(CAST('0.12345678901234567890123456789012345678' AS DECIMAL(38,38)) AS DOUBLE)",
    "SELECT CAST(CAST(1.10 AS DECIMAL(5,2)) AS VARCHAR), CAST(CAST(0 AS DECIMAL(5,2)) AS VARCHAR), CAST(CAST('-0.00000000000000000000000000000000000001' AS DECIMAL(38,38)) AS VARCHAR), CAST(CAST(-5 AS DECIMAL(3,0)) AS VARCHAR)",
    "SELECT CAST('.5' AS DECIMAL(3,2)), CAST('-.5' AS DECIMAL(3,2)), CAST('+1.5' AS DECIMAL(3,2)), CAST('1.' AS DECIMAL(3,2)), CAST('007.50' AS DECIMAL(5,2)), CAST('-0' AS DECIMAL(3,1))",
    "SELECT TRY_CAST('abc' AS DECIMAL(5,2)), TRY_CAST('12345' AS DECIMAL(5,2)), TRY_CAST(CAST(123.45 AS DECIMAL(5,2)) AS DECIMAL(4,2))",
    "SELECT CAST(CAST(9223372036854775807 AS BIGINT) AS DECIMAL(19,0)), CAST(CAST(-2147483648 AS INTEGER) AS DECIMAL(10,0)), CAST(CAST(127 AS TINYINT) AS DECIMAL(3,0)), CAST(CAST(5 AS BIGINT) AS DECIMAL(38,37))",
    "SELECT CAST(1E37 AS DECIMAL(38,0)), CAST(-1.5E-5 AS DECIMAL(10,5)), CAST(0.1E0 AS DECIMAL(38,37)), CAST(CAST(0.1 AS REAL) AS DECIMAL(10,9))",
    "SELECT CAST(x AS DECIMAL(6,2)) FROM (VALUES (1, '1.005'), (2, '-1.005'), (3, '  '), (4, NULL)) t(k, x) WHERE k <> 3 ORDER BY k",
    "SELECT CAST(CAST(x AS DECIMAL(10,4)) AS DECIMAL(6,1)) FROM (VALUES (1, '12.3456'), (2, '-12.3456'), (3, '-12.3500'), (4, '12.3500')) t(k, x) ORDER BY k",
];

#[tokio::test]
async fn casts_from_and_to_decimal_match_trino() -> Res<()> {
    expect_rows(&[], CASTS, None).await
}

/// Literal typing: digits give precision and scale; up to 38 digits.
const LITERALS: &[&str] = &[
    "SELECT 1.50, 0.000, 00.10, .001, 12345678901234567890.123, 1234567890123456789012345678901234567.8",
    "SELECT 9223372036854775807, -9223372036854775807, 2147483647 + CAST(1 AS BIGINT), 1.0E0, 1E-3",
    "SELECT 0.1 + 0.2 = 0.3, 0.1E0 + 0.2E0 = 0.3E0, 1 / 3.0, 2 / 3.00, 10.0 / 4",
];

#[tokio::test]
async fn literal_typing_matches_trino() -> Res<()> {
    expect_rows(&[], LITERALS, None).await
}

/// Errors at the 38-digit boundary and on invalid input.
const ERRORS: &[&str] = &[
    "SELECT 2147483647 + 1",
    "SELECT x * 2 FROM (VALUES (2147483647)) t(x)",
    "SELECT -9223372036854775807 - 2",
    "SELECT x - y FROM (VALUES (CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0)), CAST(1 AS DECIMAL(38,0)))) t(x, y)",
    "SELECT x * y FROM (VALUES (CAST('99999999999999999999' AS DECIMAL(20,0)), CAST('9999999999999999999' AS DECIMAL(19,0)))) t(x, y)",
    "SELECT CAST(CAST(128 AS DECIMAL(5,0)) AS TINYINT)",
    "SELECT CAST(CAST('99999999999999999999' AS DECIMAL(20,0)) AS BIGINT)",
    "SELECT CAST('12345' AS DECIMAL(5,2))",
    "SELECT CAST('1.2.3' AS DECIMAL(5,2))",
    "SELECT CAST(CAST('NaN' AS DOUBLE) AS DECIMAL(5,2))",
    "SELECT CAST(CAST('-Infinity' AS DOUBLE) AS DECIMAL(5,2))",
    "SELECT CAST(1E38 AS DECIMAL(38,0))",
    "SELECT x % y FROM (VALUES (CAST(1.5 AS DECIMAL(5,1)), CAST(0 AS DECIMAL(3,2)))) t(x, y)",
    "SELECT avg(x / 0) FROM (VALUES (CAST(1.5 AS DECIMAL(5,1)))) t(x)",
];

#[tokio::test]
async fn decimal_errors_match_trino() -> Res<()> {
    expect_errors(ERRORS, None).await
}

// ---------------------------------------------------------------- defects

#[tokio::test]
#[ignore = "DEFECT-SEM30: VALUES rows without parentheses are a syntax error. Expected (Trino): \
    VALUES 1, 2 is two rows of integer; VALUES CAST(1 AS DECIMAL(10,0)), CAST(2 AS DECIMAL(10,0)) \
    two rows of decimal(10,0). Obtained: SYNTAX_ERROR 'Expected: identifier, found: 1' (and \
    'Expected: ), found: DECIMAL')"]
async fn defect_sem30_values_rows_without_parentheses() -> Res<()> {
    expect_rows(
        &[],
        &[
            "SELECT x FROM (VALUES 1, 2) t(x) ORDER BY x",
            "SELECT avg(x) FROM (VALUES CAST(1 AS DECIMAL(10,0)), CAST(2 AS DECIMAL(10,0))) t(x)",
            "SELECT x FROM (VALUES CAST(NULL AS DECIMAL(5,2)), 1.5, -2.25) t(x) ORDER BY x",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM31: INTERSECT and EXCEPT keep the left type instead of the common \
    supertype. Expected (Trino): SELECT CAST(1.5 AS DECIMAL(5,1)) INTERSECT SELECT CAST(1.50 AS \
    DECIMAL(6,2)) is decimal(6,2) '1.50', integer INTERSECT bigint is bigint. Obtained: \
    decimal(5,1) '1.5' and integer (UNION is right)"]
async fn defect_sem31_intersect_and_except_use_the_supertype() -> Res<()> {
    expect_rows(
        &[],
        &[
            "SELECT v FROM (SELECT CAST(1.5 AS DECIMAL(5,1)) AS v INTERSECT SELECT CAST(1.50 AS DECIMAL(6,2)))",
            "SELECT v FROM (SELECT CAST(1.5 AS DECIMAL(5,1)) AS v EXCEPT SELECT CAST(2.25 AS DECIMAL(6,2)))",
            "SELECT v FROM (SELECT CAST(1 AS INTEGER) AS v INTERSECT SELECT CAST(1 AS BIGINT))",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM32: an integer literal beyond bigint is a syntax error in Trino. Expected: \
    SELECT 9223372036854775808, SELECT 12345678901234567890123456789012345678 and SELECT \
    1234567890123456789012345678901234567890 fail with SYNTAX_ERROR 'Invalid numeric literal'. \
    Obtained: the first two are accepted as decimal(20,0) and decimal(38,0); the 40-digit one fails with NOT_SUPPORTED 'decimal(40, 0) (precision above 38) \
    cannot be sent over the Trino protocol'"]
async fn defect_sem32_integer_literal_beyond_bigint_is_rejected() -> Res<()> {
    expect_errors(
        &[
            "SELECT 9223372036854775808",
            "SELECT 12345678901234567890123456789012345678",
            "SELECT 1234567890123456789012345678901234567890",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM33: ceil, floor and round(decimal(p,s)) are decimal(p-s+1,0) in Trino \
    (decimal(p,0) when s = 0). Expected for decimal(12,4): decimal(9,0); ceil(99999999.9999) = \
    100000000, floor(-99999999.9999) = -100000000. Obtained: round is decimal(12,0) (values \
    right); ceil and floor fail with GENERIC_INTERNAL_ERROR 'Decimal overflow while applying ceil' \
    on the whole column"]
async fn defect_sem33_ceil_floor_round_result_type() -> Res<()> {
    expect_rows(
        SIGNS_SETUP,
        &[
            "SELECT ceil(a) FROM {c}.bx_dec.s ORDER BY k",
            "SELECT floor(a) FROM {c}.bx_dec.s ORDER BY k",
            "SELECT round(a) FROM {c}.bx_dec.s ORDER BY k",
            "SELECT ceil(CAST(9.5 AS DECIMAL(2,1))), floor(CAST(-9.5 AS DECIMAL(2,1))), round(CAST(9.5 AS DECIMAL(2,1))), ceil(CAST(5 AS DECIMAL(3,0)))",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM34: Trino functions over decimals, doubles and zoned timestamps are \
    missing. Expected: sign(decimal) decimal(1,0), truncate(decimal(12,4)) decimal(8,0), nan(), \
    infinity(), current_timezone(), hour(), timezone_hour(), timezone_minute(), date_diff(), \
    variance(), year(), month(), extract(TIMEZONE_HOUR/TIMEZONE_MINUTE FROM ...). Obtained: \
    FUNCTION_NOT_FOUND for each ('Invalid function 'sign'', ...); extract fails with \
    GENERIC_INTERNAL_ERROR 'Date part 'TIMEZONE_HOUR' not supported'"]
async fn defect_sem34_trino_functions_exist() -> Res<()> {
    expect_rows(
        SIGNS_SETUP,
        &[
            "SELECT sign(a), truncate(a) FROM {c}.bx_dec.s ORDER BY k",
            "SELECT is_nan(nan()), infinity() > 1E308",
            "SELECT current_timezone()",
            "SELECT hour(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), timezone_hour(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), timezone_minute(CAST('2026-06-01 12:00:00 Asia/Kathmandu' AS TIMESTAMP WITH TIME ZONE))",
            "SELECT date_diff('hour', CAST('2018-11-03 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), CAST('2018-11-04 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE))",
            "SELECT variance(x), year(DATE '1995-09-17'), month(DATE '1995-09-17') FROM (VALUES (CAST(1 AS DECIMAL(5,2))), (CAST(2 AS DECIMAL(5,2)))) t(x)",
            "SELECT extract(TIMEZONE_HOUR FROM CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), extract(TIMEZONE_MINUTE FROM CAST('2026-06-01 12:00:00 Asia/Kathmandu' AS TIMESTAMP WITH TIME ZONE))",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM40: ORDER BY ... DESC puts NULL first; Trino puts NULL last in both \
    directions. Expected: ORDER BY v DESC over (NULL, 1.5, -2.25, 0, 10) = 10.00, 1.50, 0.00, \
    -2.25, NULL. Obtained: NULL, 10.00, 1.50, 0.00, -2.25"]
async fn defect_sem40_order_by_desc_puts_nulls_last() -> Res<()> {
    expect_rows(
        &[],
        &[
            "SELECT v FROM (VALUES (CAST(NULL AS DECIMAL(5,2))), (1.5), (-2.25), (0), (10)) t(v) ORDER BY v DESC",
            "SELECT k FROM (VALUES (1, 'b'), (2, NULL), (3, 'a')) t(k, s) ORDER BY s DESC, k",
            "SELECT k, row_number() OVER (ORDER BY v DESC) FROM (VALUES (1, 2.5), (2, NULL), (3, 1.5)) t(k, v) ORDER BY k",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM41: two aggregates of the same column that differ by a cast fail. \
    Expected (Trino): SELECT sum(n), sum(CAST(n AS SMALLINT)) answers bigint 499500, 499500. \
    Obtained: GENERIC_USER_ERROR 'Projections require unique expression names but the \
    expression \"sum(datafusion.bx_agg.g.n)\" at position 0 and \"sum(CAST(datafusion.bx_agg.g.n \
    AS Int16))\" at position 1 have the same name' (also avg(n) with avg(CAST(n AS REAL))); \
    with aliases: 'Schema contains duplicate unqualified field name'"]
async fn defect_sem41_aggregates_differing_by_a_cast() -> Res<()> {
    expect_rows(
        GENERATED,
        &[
            "SELECT sum(n), sum(CAST(n AS SMALLINT)) FROM {c}.bx_agg.g",
            "SELECT avg(n), avg(CAST(n AS REAL)) FROM {c}.bx_agg.g",
            "SELECT sum(z), sum(CAST(z AS BIGINT)) FROM {c}.bx_agg.g",
            "SELECT sum(n) AS a, sum(CAST(n AS SMALLINT)) AS b FROM {c}.bx_agg.g",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM42: the overflow of sum(decimal) has another error name. Expected (Trino): \
    GENERIC_INTERNAL_ERROR 'Decimal overflow' for sum of two decimal(38,0) of 38 nines. \
    Obtained: NUMERIC_VALUE_OUT_OF_RANGE 'Arrow error: Arithmetic overflow: Decimal overflow'. \
    Low severity: both fail; the name Trino uses is an internal-error name"]
async fn defect_sem42_sum_overflow_error_name() -> Res<()> {
    expect_errors(
        &[
            "SELECT sum(x) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
            "SELECT sum(x) FROM (VALUES (CAST('999999999999999999999999999999999999.99' AS DECIMAL(38,2))), (CAST('0.01' AS DECIMAL(38,2)))) t(x)",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM46: sum and avg of real are real in Trino. Expected: sum(CAST(n AS REAL)) \
    real 499500.0, avg real 499.5 (NULL real over no rows). Obtained: double for both"]
async fn defect_sem46_real_aggregates_are_real() -> Res<()> {
    expect_rows(
        GENERATED,
        &[
            "SELECT sum(CAST(n AS REAL)), avg(CAST(n AS REAL)) FROM {c}.bx_agg.g",
            "SELECT avg(CAST(n AS REAL)) FROM {c}.bx_agg.g WHERE n < 0",
        ],
        None,
    )
    .await
}
