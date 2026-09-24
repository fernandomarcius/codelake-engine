//! Decimal arithmetic and aggregation: result type and value of every
//! operator over a matrix of precisions and scales, aggregates, windows,
//! supertypes and overflow.

use super::{Pair, Res, expect_same, verdict};

/// Precision and scale pairs of the matrix: scale 0, scale = precision, the
/// 18/19 and 38 boundaries of the 64- and 128-bit representations.
const SPECS: [(u32, u32); 12] = [
    (1, 0),
    (3, 1),
    (5, 2),
    (10, 0),
    (10, 10),
    (15, 2),
    (18, 4),
    (19, 0),
    (20, 10),
    (28, 6),
    (38, 0),
    (38, 10),
];

const OPS: [&str; 5] = ["+", "-", "*", "/", "%"];

/// A value that fits `decimal(precision, scale)`. Kind 0: `777.3535…`;
/// kind 1: `11.666…` (repeating, rounding in division); kind 2: `2.5000…`
/// (exact halves).
fn value(precision: u32, scale: u32, negative: bool, kind: u8) -> String {
    let int_digits = precision - scale;
    let scale = scale as usize;
    let (int_part, frac) = match kind {
        0 => ("777", "35".repeat(scale)),
        1 => ("11", "6".repeat(scale)),
        _ => ("2", format!("5{}", "0".repeat(scale))),
    };
    let int_part = if int_digits == 0 {
        "0".to_owned()
    } else {
        int_part[..int_part.len().min(int_digits as usize)].to_owned()
    };
    let frac = &frac[..scale];
    let sign = if negative { "-" } else { "" };
    if scale == 0 {
        format!("{sign}{int_part}")
    } else {
        format!("{sign}{int_part}.{frac}")
    }
}

fn literal(spec: (u32, u32), negative: bool, kind: u8) -> String {
    let (p, s) = spec;
    format!("CAST('{}' AS DECIMAL({p},{s}))", value(p, s, negative, kind))
}

/// Constant expressions: the planner folds them, a path of its own.
#[tokio::test]
async fn literal_operator_matrix_matches_trino() -> Res<()> {
    let pair = Pair::start().await?;
    let mut wrong = Vec::new();
    for left in SPECS {
        for right in SPECS {
            for kind in [0, 2] {
                let (a, b) = (literal(left, false, kind), literal(right, true, 0));
                let exprs: Vec<String> = OPS.iter().map(|op| format!("{a} {op} {b}")).collect();
                wrong.extend(pair.batch(&exprs, "").await?);
            }
        }
    }
    verdict(&wrong)
}

/// Rows of every spec: positive `777.35…`, negative `-11.66…`, negative
/// `-2.50…` and positive `11.66…`.
fn matrix_table() -> String {
    let rows = [(false, 0), (true, 1), (true, 2), (false, 1)];
    let casts: Vec<String> = SPECS
        .iter()
        .enumerate()
        .map(|(i, (p, s))| format!("CAST(x{i} AS DECIMAL({p},{s})) AS c{i}"))
        .collect();
    let values: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(rid, &(negative, kind))| {
            let cells: Vec<String> = SPECS
                .iter()
                .map(|&(p, s)| format!("'{}'", value(p, s, negative, kind)))
                .collect();
            format!("({}, {})", rid + 1, cells.join(", "))
        })
        .collect();
    let names: Vec<String> = (0..SPECS.len()).map(|i| format!("x{i}")).collect();
    format!(
        "CREATE TABLE {{c}}.sem_dec.d AS SELECT CAST(rid AS BIGINT) AS rid, {} FROM (VALUES {}) t(rid, {})",
        casts.join(", "),
        values.join(", "),
        names.join(", ")
    )
}

/// The same matrix through table columns: the kernels run per row.
#[tokio::test]
async fn column_operator_matrix_matches_trino() -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(&["CREATE SCHEMA {c}.sem_dec", &matrix_table()])
        .await?;
    let mut wrong = Vec::new();
    for i in 0..SPECS.len() {
        for j in 0..SPECS.len() {
            let exprs: Vec<String> = OPS.iter().map(|op| format!("c{i} {op} c{j}")).collect();
            wrong.extend(pair.batch(&exprs, "FROM {c}.sem_dec.d ORDER BY rid").await?);
        }
    }
    verdict(&wrong)
}

/// `sum`, `avg`, `min`, `max`, `count`, `FILTER`, `GROUP BY` and windows
/// over every spec.
#[tokio::test]
async fn aggregates_over_the_matrix_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    pair.setup(&["CREATE SCHEMA {c}.sem_dec", &matrix_table()])
        .await?;
    let mut wrong = Vec::new();
    for i in 0..SPECS.len() {
        let queries = [
            format!(
                "SELECT sum(c{i}), avg(c{i}), min(c{i}), max(c{i}), count(c{i}), sum(c{i}) FILTER (WHERE rid > 1), avg(c{i}) FILTER (WHERE rid > 2) FROM {{c}}.sem_dec.d"
            ),
            format!(
                "SELECT sum(c{i}), avg(c{i}), avg(-c{i}) FROM {{c}}.sem_dec.d GROUP BY rid % 2 ORDER BY rid % 2"
            ),
            format!(
                "SELECT sum(c{i}) OVER (ORDER BY rid), avg(c{i}) OVER (ORDER BY rid ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {{c}}.sem_dec.d ORDER BY rid"
            ),
            format!(
                "SELECT sum(c{i} * c{i}), avg(c{i} / 3), sum(DISTINCT c{i}), avg(DISTINCT c{i}) FROM {{c}}.sem_dec.d"
            ),
        ];
        for query in &queries {
            if let Some(m) = pair.mismatch(query, None).await? {
                wrong.push(m);
            }
        }
    }
    verdict(&wrong)
}

/// Supertypes, mixed operands, casts, rounding and overflow errors that
/// match Trino 431.
const CASES: &[&str] = &[
    "SELECT CAST(1.5 AS DECIMAL(5,1)) + CAST(2 AS BIGINT), CAST(1.5 AS DECIMAL(5,1)) + CAST(2 AS INTEGER), CAST(1.5 AS DECIMAL(5,1)) + CAST(2 AS SMALLINT), CAST(1.5 AS DECIMAL(5,1)) + 2.0E0, CAST(1.5 AS DECIMAL(5,1)) + CAST(2 AS REAL), CAST(1.5 AS DECIMAL(5,1)) * 2",
    "SELECT CASE WHEN true THEN CAST(1.5 AS DECIMAL(5,1)) ELSE CAST(2.25 AS DECIMAL(10,2)) END, coalesce(CAST(NULL AS DECIMAL(5,1)), CAST(2.25 AS DECIMAL(10,2))), greatest(CAST(1.5 AS DECIMAL(5,1)), CAST(2.25 AS DECIMAL(10,2))), least(CAST(1.5 AS DECIMAL(5,1)), CAST(2.25 AS DECIMAL(10,2)))",
    "SELECT CASE WHEN true THEN CAST(1 AS SMALLINT) ELSE CAST(2.25 AS DECIMAL(4,2)) END, CASE WHEN false THEN CAST(1 AS INTEGER) ELSE 1.5 END",
    "SELECT CAST(1.5 AS DECIMAL(5,1)) = CAST(1.50 AS DECIMAL(10,2)), CAST(1.5 AS DECIMAL(5,1)) < 2, CAST(1.5 AS DECIMAL(5,1)) IN (1.5, 2), CAST(1.5 AS DECIMAL(10,2)) BETWEEN 1 AND 1.99",
    "SELECT CAST(CAST(123.456 AS DECIMAL(6,3)) AS VARCHAR), CAST(CAST(-0.5 AS DECIMAL(3,2)) AS VARCHAR), CAST(CAST(0.001 AS DECIMAL(4,3)) AS DOUBLE), CAST('1.235' AS DECIMAL(5,2)), CAST('-1.235' AS DECIMAL(5,2))",
    "SELECT CAST(2.5 AS BIGINT), CAST(-2.5 AS BIGINT), CAST(2.4 AS INTEGER), CAST(CAST(1.45 AS DECIMAL(5,2)) AS DECIMAL(5,1)), CAST(CAST(-1.45 AS DECIMAL(5,2)) AS DECIMAL(5,1)), CAST(2.675E0 AS DECIMAL(5,2)), CAST(1E20 AS DECIMAL(38,0)), CAST(123.456E0 AS DECIMAL(38,10))",
    "SELECT round(CAST(2.5 AS DECIMAL(5,1))), round(CAST(-2.5 AS DECIMAL(5,1))), round(CAST(99.5 AS DECIMAL(3,1))), abs(CAST(-1.5 AS DECIMAL(5,1))), -CAST(1.5 AS DECIMAL(5,1))",
    "SELECT 7 / 2.0, 1.0 / 3, 10 % 3.0, 2147483648 * 1.5, 123456789012 * 1.0, 1.5, 100.00, 0.00, .5, -1.5, 1e5, 2.5E-1, 0.1 + 0.2",
    "SELECT x FROM (SELECT CAST(1 AS DECIMAL(5,1)) AS x UNION ALL SELECT CAST(2.25 AS DECIMAL(10,2))) ORDER BY x",
    "SELECT x FROM (SELECT CAST(1 AS DECIMAL(38,0)) AS x UNION ALL SELECT CAST(0.5 AS DECIMAL(38,37))) ORDER BY x",
    "SELECT x FROM (VALUES (CAST(1 AS INTEGER)), (1.5)) t(x) ORDER BY x",
    // avg rounds half away from zero on the exact sum.
    "SELECT avg(v) FROM (VALUES (CAST(1 AS DECIMAL(10,2))), (CAST(0 AS DECIMAL(10,2))), (CAST(0 AS DECIMAL(10,2)))) t(v)",
    "SELECT avg(v), avg(-v) FROM (VALUES (CAST(0.01 AS DECIMAL(10,2))), (CAST(0 AS DECIMAL(10,2))), (CAST(0 AS DECIMAL(10,2))), (CAST(0.01 AS DECIMAL(10,2))), (CAST(0.01 AS DECIMAL(10,2))), (CAST(0 AS DECIMAL(10,2)))) t(v)",
    "SELECT sum(x), avg(x) FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('-99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
    "SELECT sum(v), avg(v) FROM (VALUES (1, CAST(NULL AS DECIMAL(10,2))), (1, CAST(NULL AS DECIMAL(10,2))), (2, CAST(1.10 AS DECIMAL(10,2)))) t(k, v) GROUP BY k ORDER BY k",
    "SELECT sum(v), avg(v), count(v) FROM (VALUES (CAST(1 AS DECIMAL(10,2)))) t(v) WHERE v > 5",
    // Overflow and division by zero are errors in both.
    "SELECT x + x FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)))) t(x)",
    "SELECT x * x FROM (VALUES (CAST('9999999999999999999' AS DECIMAL(19,0)))) t(x)",
    "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + 1",
    "SELECT CAST(0 AS DECIMAL(38,0)) / CAST(3 AS DECIMAL(38,19))",
    "SELECT x / (x - x) FROM (VALUES (CAST(1.5 AS DECIMAL(5,1)))) t(x)",
    "SELECT x % (x - x) FROM (VALUES (CAST(1.5 AS DECIMAL(5,1)))) t(x)",
    "SELECT CAST(1 AS DECIMAL(5,0)) / CAST(0 AS DECIMAL(5,0))",
];

#[tokio::test]
async fn supertypes_casts_and_overflow_match_trino() -> Res<()> {
    expect_same(&[], CASES, None).await
}

/// Trino 431 divides `decimal(38,10)` by `decimal(10,10)` without checking
/// the result against precision 38: it returns a 28-digit positive number
/// for a positive dividend over a negative divisor. The true quotient needs
/// 29 integer digits; `CodeLake` fails with `Decimal overflow` instead of
/// copying the wrong value. Deliberate divergence, not a defect.
#[tokio::test]
async fn division_overflow_is_an_error_where_trino_431_returns_a_wrong_value() -> Res<()> {
    let pair = Pair::start().await?;
    let sql = "SELECT CAST('9999999999999999999999999999.9999999999' AS DECIMAL(38,10)) / CAST('-0.3535353535' AS DECIMAL(10,10))";
    let (trino, codelake) = pair.both(sql, None).await?;
    let trino_value = trino["rows"][0][0].as_str().unwrap_or_default();
    if trino_value.starts_with('-') || trino_value.is_empty() {
        return Err(format!("Trino 431 no longer answers the wrong value: {trino}"));
    }
    if codelake["error"] != "NUMERIC_VALUE_OUT_OF_RANGE" {
        return Err(format!("expected Decimal overflow, got {codelake}"));
    }
    Ok(())
}

// ---------------------------------------------------------------- defects

#[tokio::test]
async fn defect_sem1_double_and_decimal_unify_to_double() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT x FROM (VALUES (1E-20), (1.5)) t(x) ORDER BY x",
            "SELECT x FROM (VALUES (1E300), (1.5)) t(x) ORDER BY x",
            "SELECT CASE WHEN x = 1 THEN 1.23456789012345678E0 ELSE 0.5 END FROM (VALUES (1)) t(x)",
            "SELECT coalesce(x, 0.0) FROM (VALUES (CAST(1E-9 AS DOUBLE))) t(x)",
            "SELECT nullif(1E-20, 1.5)",
            "SELECT x FROM (SELECT CAST(1.5 AS REAL) AS x UNION ALL SELECT 2.5) ORDER BY x",
            "SELECT x FROM (VALUES (CAST(3.4028235E38 AS REAL)), (1.5)) t(x) ORDER BY x",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem6_double_to_decimal_rounds_like_trino() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST(1.005E0 AS DECIMAL(5,2)), CAST(CAST(1.005 AS REAL) AS DECIMAL(5,2))",
            "SELECT CAST(x AS DECIMAL(5,2)) FROM (VALUES (1, 1.005E0), (2, 2.675E0), (3, 0.125E0), (4, -0.125E0), (5, 1.115E0)) t(k, x) ORDER BY k",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem16_round_with_digits_keeps_the_scale() -> Res<()> {
    expect_same(
        &[],
        &["SELECT round(CAST(1.2345 AS DECIMAL(10,4)), 2), round(CAST(1.2355 AS DECIMAL(10,4)), 3), round(CAST(-1.2355 AS DECIMAL(10,4)), 3), round(CAST(155 AS DECIMAL(5,0)), -1), round(CAST(9.99 AS DECIMAL(3,2)), 1)"],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem17_ranking_windows_are_bigint() -> Res<()> {
    expect_same(
        &[],
        &["SELECT row_number() OVER (ORDER BY x), rank() OVER (ORDER BY x), dense_rank() OVER (ORDER BY x) FROM (VALUES (1), (2)) t(x) ORDER BY x"],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem19_small_integer_literals_are_integer() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT 1 / 2, 7, -3, 2147483647, 2147483648",
            "SELECT k FROM (VALUES (1), (2)) t(k) ORDER BY k",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem20_bigint_in_a_decimal_supertype_is_decimal_19() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (1.5)) t(x) ORDER BY x",
            "SELECT coalesce(CAST(NULL AS BIGINT), 1.5)",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem21_null_literal_in_decimal_arithmetic() -> Res<()> {
    expect_same(
        &[],
        &["SELECT 0.2 * NULL, NULL * 1.5, CAST(1.5 AS DECIMAL(5,1)) + NULL, 1.5 / NULL, NULL % 1.5"],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem22_decimal_error_names() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST(1.5 AS DECIMAL(5,2)) * CAST('0.35' AS DECIMAL(38,38))",
            "SELECT CAST(1.5 AS DECIMAL(5,2)) / CAST('0.35' AS DECIMAL(38,38))",
            "SELECT CAST(x AS DECIMAL(3,1)) FROM (VALUES (CAST(777.35 AS DECIMAL(18,4)))) t(x)",
        ],
        None,
    )
    .await
}

#[tokio::test]
async fn defect_sem29_text_to_decimal_accepts_what_trino_accepts() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST(' 12.5 ' AS DECIMAL(5,1))",
            "SELECT CAST('1e2' AS DECIMAL(5,1)), CAST('-1.5E-1' AS DECIMAL(5,2))",
        ],
        None,
    )
    .await
}
