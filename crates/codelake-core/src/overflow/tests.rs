//! Edge values of checked arithmetic. Every expected value and message was
//! observed on a real Trino 431 server (`trinodb/trino:431`), except where a
//! comment says otherwise.

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::testing::{federated_session, int64, remote_table_recording};

async fn run(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>, DataFusionError> {
    ctx.sql(sql).await?.collect().await
}

/// First column of the result as text, one entry per row.
async fn values(ctx: &SessionContext, sql: &str) -> Result<Vec<String>, DataFusionError> {
    let batches = run(ctx, sql).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let column = batch.column(0);
        let formatter =
            ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default().with_null("NULL"))?;
        for row in 0..column.len() {
            out.push(formatter.value(row).to_string());
        }
    }
    Ok(out)
}

fn overflow_message(error: &DataFusionError) -> Option<String> {
    match error.find_root() {
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::ArithmeticOverflow(message) => Some(message.clone()),
            _ => None,
        },
        _ => None,
    }
}

async fn expect_overflow(ctx: &SessionContext, sql: &str, message: &str) {
    match values(ctx, sql).await {
        Ok(rows) => panic!("{sql}: expected `{message}`, got rows {rows:?}"),
        Err(error) => assert_eq!(
            overflow_message(&error).as_deref(),
            Some(message),
            "{sql}: {error}"
        ),
    }
}

async fn expect_rows(ctx: &SessionContext, sql: &str, expected: &[&str]) {
    let rows = values(ctx, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(rows, expected, "{sql}");
}

const MAX: &str = "9223372036854775807";
const MIN: &str = "(-9223372036854775807 - 1)";

#[tokio::test]
async fn bigint_operators_fail_on_overflow() {
    let ctx = federated_session();
    let cases = [
        (
            format!("SELECT {MAX} + 1"),
            "bigint addition overflow: 9223372036854775807 + 1",
        ),
        (
            format!("SELECT {MIN} - 1"),
            "bigint subtraction overflow: -9223372036854775808 - 1",
        ),
        (
            "SELECT -9223372036854775807 - 2".to_owned(),
            "bigint subtraction overflow: -9223372036854775807 - 2",
        ),
        (
            format!("SELECT {MAX} * 2"),
            "bigint multiplication overflow: 9223372036854775807 * 2",
        ),
        (
            format!("SELECT {MIN} * -1"),
            "bigint multiplication overflow: -9223372036854775808 * -1",
        ),
        (
            format!("SELECT {MIN} / -1"),
            "bigint division overflow: -9223372036854775808 / -1",
        ),
        (
            format!("SELECT -{MIN}"),
            "bigint negation overflow: -9223372036854775808",
        ),
        (
            format!("SELECT abs({MIN})"),
            "Value -9223372036854775808 is out of range for abs(bigint)",
        ),
    ];
    for (sql, message) in cases {
        expect_overflow(&ctx, &sql, message).await;
    }
}

#[tokio::test]
async fn bigint_boundaries_that_fit_are_exact() {
    let ctx = federated_session();
    let cases = [
        (format!("SELECT {MAX} + 0"), MAX),
        (format!("SELECT {MIN} + 0"), "-9223372036854775808"),
        (format!("SELECT {MAX} - {MAX}"), "0"),
        (format!("SELECT {MIN} + {MAX}"), "-1"),
        (format!("SELECT {MIN} / 1"), "-9223372036854775808"),
        // Java's `%`: no overflow.
        (format!("SELECT {MIN} % -1"), "0"),
        (format!("SELECT -{MAX}"), "-9223372036854775807"),
        (format!("SELECT abs(-{MAX})"), MAX),
        ("SELECT 4611686018427387903 * 2".to_owned(), "9223372036854775806"),
        ("SELECT -7 / 2".to_owned(), "-3"),
        ("SELECT -7 % 2".to_owned(), "-1"),
        ("SELECT 7 % -2".to_owned(), "1"),
    ];
    for (sql, expected) in cases {
        expect_rows(&ctx, &sql, &[expected]).await;
    }
}

#[tokio::test]
async fn narrower_integers_overflow_at_their_own_range() {
    let ctx = federated_session();
    let cases = [
        (
            "SELECT CAST(2147483647 AS INTEGER) + CAST(1 AS INTEGER)",
            "integer addition overflow: 2147483647 + 1",
        ),
        (
            "SELECT CAST(32767 AS SMALLINT) + CAST(1 AS SMALLINT)",
            "smallint addition overflow: 32767 + 1",
        ),
        (
            "SELECT CAST(127 AS TINYINT) + CAST(1 AS TINYINT)",
            "tinyint addition overflow: 127 + 1",
        ),
        (
            "SELECT CAST(127 AS TINYINT) * CAST(2 AS TINYINT)",
            "tinyint multiplication overflow: 127 * 2",
        ),
        ("SELECT -CAST(-128 AS TINYINT)", "tinyint negation overflow: -128"),
        (
            "SELECT abs(CAST(-128 AS TINYINT))",
            "Value -128 is out of range for abs(tinyint)",
        ),
        (
            "SELECT abs(CAST(-2147483648 AS INTEGER))",
            "Value -2147483648 is out of range for abs(integer)",
        ),
    ];
    for (sql, message) in cases {
        expect_overflow(&ctx, sql, message).await;
    }
    expect_rows(&ctx, "SELECT CAST(-128 AS TINYINT) % CAST(-1 AS TINYINT)", &["0"]).await;
    expect_rows(&ctx, "SELECT CAST(126 AS TINYINT) + CAST(1 AS TINYINT)", &["127"]).await;
}

#[tokio::test]
async fn columns_nulls_and_predicates_are_checked() {
    let ctx = federated_session();
    let one_row = format!("(VALUES ({MAX})) t(x)");
    expect_overflow(
        &ctx,
        &format!("SELECT x + 1 AS y FROM {one_row}"),
        "bigint addition overflow: 9223372036854775807 + 1",
    )
    .await;
    expect_overflow(
        &ctx,
        &format!("SELECT 1 + x AS y FROM {one_row}"),
        "bigint addition overflow: 1 + 9223372036854775807",
    )
    .await;
    expect_overflow(
        &ctx,
        &format!("SELECT x FROM {one_row} WHERE x + 1 > 0"),
        "bigint addition overflow: 9223372036854775807 + 1",
    )
    .await;
    expect_overflow(
        &ctx,
        &format!("SELECT x * y AS z FROM (VALUES ({MAX}, 2), (1, 1)) t(x, y)"),
        "bigint multiplication overflow: 9223372036854775807 * 2",
    )
    .await;
    expect_overflow(
        &ctx,
        "SELECT -x AS y FROM (VALUES (-9223372036854775808)) t(x)",
        "bigint negation overflow: -9223372036854775808",
    )
    .await;
    expect_overflow(
        &ctx,
        &format!("SELECT x FROM {one_row} ORDER BY x + 1"),
        "bigint addition overflow: 9223372036854775807 + 1",
    )
    .await;
    expect_overflow(
        &ctx,
        &format!("SELECT x + 1 AS k, count(*) AS n FROM {one_row} GROUP BY x + 1"),
        "bigint addition overflow: 9223372036854775807 + 1",
    )
    .await;
    // NULL operands and rows with NULL never fail.
    expect_rows(
        &ctx,
        &format!("SELECT x + y AS z FROM (VALUES ({MAX}, CAST(NULL AS BIGINT)), (1, 2)) t(x, y)"),
        &["NULL", "3"],
    )
    .await;
    expect_rows(&ctx, &format!("SELECT {MAX} + NULL"), &["NULL"]).await;
    // A branch that is not taken is not evaluated.
    expect_rows(
        &ctx,
        &format!("SELECT CASE WHEN x > 0 THEN 1 ELSE x + 1 END AS y FROM {one_row}"),
        &["1"],
    )
    .await;
    expect_rows(
        &ctx,
        &format!("SELECT {MAX} + 1 AS y FROM (VALUES (1)) t(x) WHERE false"),
        &[],
    )
    .await;
}

#[tokio::test]
async fn sum_fails_on_overflow_everywhere() {
    let ctx = federated_session();
    for sql in [
        format!("SELECT sum(x) AS s FROM (VALUES ({MAX}), (1)) t(x)"),
        format!("SELECT k, sum(x) AS s FROM (VALUES (1, {MAX}), (1, 1), (2, 5)) t(k, x) GROUP BY k"),
        format!("SELECT sum(DISTINCT x) AS s FROM (VALUES ({MAX}), (1), (1)) t(x)"),
        format!(
            "SELECT sum(x) OVER (ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM (VALUES (1, {MAX}), (2, 1)) t(o, x)"
        ),
        format!("SELECT sum(x) OVER (ORDER BY o) AS s FROM (VALUES (1, {MAX}), (2, 1)) t(o, x)"),
    ] {
        let error = values(&ctx, &sql).await.expect_err(&sql);
        let found = overflow_message(&error).unwrap_or_default();
        // The operands depend on the order rows are added in.
        assert!(found.starts_with("bigint addition overflow"), "{sql}: {error}");
    }
    // `sum` of integers is a bigint (Trino: 2147483648).
    expect_rows(
        &ctx,
        "SELECT sum(CAST(x AS INTEGER)) AS s FROM (VALUES (2147483647), (1)) t(x)",
        &["2147483648"],
    )
    .await;
    expect_rows(
        &ctx,
        &format!("SELECT sum(x) AS s FROM (VALUES ({MAX}), (CAST(NULL AS BIGINT))) t(x)"),
        &[MAX],
    )
    .await;
    expect_rows(
        &ctx,
        "SELECT sum(x) AS s FROM (VALUES (CAST(NULL AS BIGINT))) t(x)",
        &["NULL"],
    )
    .await;
    expect_rows(
        &ctx,
        "SELECT sum(x) FILTER (WHERE x > 1) AS s FROM (VALUES (1), (2), (3)) t(x)",
        &["5"],
    )
    .await;
    expect_rows(
        &ctx,
        "SELECT k, sum(x) AS s FROM (VALUES (1, 1), (1, 2), (2, 5), (3, CAST(NULL AS BIGINT))) t(k, x) GROUP BY k ORDER BY k",
        &["1", "2", "3"],
    )
    .await;
    let grouped = values(
        &ctx,
        "SELECT s FROM (SELECT k, sum(x) AS s FROM (VALUES (1, 1), (1, 2), (2, 5), (3, CAST(NULL AS BIGINT))) t(k, x) GROUP BY k) ORDER BY k",
    )
    .await
    .unwrap();
    assert_eq!(grouped, ["3", "5", "NULL"]);
    expect_rows(
        &ctx,
        "SELECT sum(x) OVER (ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM (VALUES (1, 1), (2, 2), (3, 4)) t(o, x) ORDER BY s",
        &["1", "3", "6"],
    )
    .await;
    expect_rows(
        &ctx,
        "SELECT sum(x) AS s FROM (VALUES (1.5), (2.25)) t(x)",
        &["3.75"],
    )
    .await;
}

#[tokio::test]
async fn decimals_fail_beyond_their_precision() {
    let ctx = federated_session();
    let nines = "CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))";
    for sql in [
        format!("SELECT {nines} + CAST(1 AS DECIMAL(38,0))"),
        format!("SELECT -{nines} - CAST(1 AS DECIMAL(38,0))"),
        "SELECT CAST('9999999999999999999999999999999999999' AS DECIMAL(38,0)) * CAST(100 AS DECIMAL(3,0))"
            .to_owned(),
        format!("SELECT sum(x) AS s FROM (VALUES ({nines}), (CAST(1 AS DECIMAL(38,0)))) t(x)"),
        format!(
            "SELECT k, sum(x) AS s FROM (VALUES (1, {nines}), (1, CAST(1 AS DECIMAL(38,0)))) t(k, x) GROUP BY k"
        ),
    ] {
        expect_overflow(&ctx, &sql, "Decimal overflow").await;
    }
    expect_rows(
        &ctx,
        "SELECT CAST(9.99 AS DECIMAL(3,2)) + CAST(9.99 AS DECIMAL(3,2))",
        &["19.98"],
    )
    .await;
    expect_rows(
        &ctx,
        &format!("SELECT {nines} - CAST(1 AS DECIMAL(38,0))"),
        &["99999999999999999999999999999999999998"],
    )
    .await;
    expect_rows(&ctx, "SELECT CAST(12.5 AS DECIMAL(3,1)) * 2", &["25.0"]).await;
    let batches = run(
        &ctx,
        "SELECT CAST(9.99 AS DECIMAL(3,2)) + CAST(9.99 AS DECIMAL(3,2)) AS d",
    )
    .await
    .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Decimal128(4, 2)
    );
}

#[tokio::test]
async fn casts_to_integers_round_half_away_from_zero() {
    let ctx = federated_session();
    let cases = [
        ("SELECT CAST(2.5 AS BIGINT)", "3"),
        ("SELECT CAST(-2.5 AS BIGINT)", "-3"),
        ("SELECT CAST(2.4 AS BIGINT)", "2"),
        ("SELECT CAST(3.5e0 AS INTEGER)", "4"),
        ("SELECT CAST(CAST(-0.5 AS DOUBLE) AS BIGINT)", "-1"),
        ("SELECT CAST(CAST(0.49999999999999994 AS DOUBLE) AS BIGINT)", "0"),
        ("SELECT CAST(CAST(2.5 AS REAL) AS INTEGER)", "3"),
        ("SELECT CAST(CAST(127.4 AS DOUBLE) AS TINYINT)", "127"),
        ("SELECT CAST(CAST(2.5 AS DECIMAL(3,1)) AS BIGINT)", "3"),
        ("SELECT CAST(CAST(-2.5 AS DECIMAL(3,1)) AS INTEGER)", "-3"),
        ("SELECT CAST(CAST(2.4 AS DECIMAL(3,1)) AS BIGINT)", "2"),
        ("SELECT CAST(CAST(-2.45 AS DECIMAL(3,2)) AS TINYINT)", "-2"),
        ("SELECT CAST(CAST(9.5 AS DECIMAL(2,1)) AS TINYINT)", "10"),
        ("SELECT CAST(CAST(0.5 AS DECIMAL(1,1)) AS TINYINT)", "1"),
        ("SELECT TRY_CAST(CAST(2.5 AS DOUBLE) AS BIGINT)", "3"),
        (
            "SELECT CAST(x AS INTEGER) FROM (VALUES (CAST(2.5 AS DOUBLE)), (CAST(-2.5 AS DOUBLE))) t(x)",
            "3",
        ),
    ];
    for (sql, expected) in cases {
        let rows = values(&ctx, sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows.first().map(String::as_str), Some(expected), "{sql}");
    }
    expect_rows(
        &ctx,
        "SELECT CAST(x AS INTEGER) FROM (VALUES (CAST(2.5 AS DOUBLE)), (CAST(-2.5 AS DOUBLE))) t(x)",
        &["3", "-3"],
    )
    .await;
    // Out of range after rounding, NaN: errors, as in Trino.
    for sql in [
        "SELECT CAST(CAST(127.5 AS DOUBLE) AS TINYINT)",
        "SELECT CAST(CAST(127.5 AS DECIMAL(4,1)) AS TINYINT)",
        "SELECT CAST(CAST('NaN' AS DOUBLE) AS BIGINT)",
        "SELECT CAST(9.3e18 AS BIGINT)",
    ] {
        assert!(values(&ctx, sql).await.is_err(), "{sql}");
    }
}

#[tokio::test]
async fn rewrites_keep_column_names() {
    let plain = SessionContext::new();
    let checked = federated_session();
    let sql = "SELECT x + 1, -x, x * y - 2, abs(x), sum(x), CAST(y AS INTEGER), CAST(d AS BIGINT) \
               FROM (VALUES (1, 2.5, CAST(1.5 AS DECIMAL(3,1)))) t(x, y, d) GROUP BY x, y, d";
    let names = |batches: Vec<RecordBatch>| -> Vec<String> {
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    let expected = names(run(&plain, sql).await.unwrap());
    assert_eq!(names(run(&checked, sql).await.unwrap()), expected);
}

#[tokio::test]
async fn federated_sub_plans_keep_plain_operators() {
    let ctx = federated_session();
    let (table, statements) = remote_table_recording("remote", "r", vec![("x", int64(&[1, 2]))], None);
    ctx.register_table("r", table).unwrap();
    // The fake source only returns plain columns: what matters is the
    // statement it received.
    let _ = values(&ctx, "SELECT x + 1 AS y FROM r WHERE x * 2 > 2").await;
    let sent = statements.lock().unwrap().join("\n");
    assert!(
        sent.contains("SELECT (r.x + 1) AS y FROM r WHERE ((r.x * 2) > 2)"),
        "{sent}"
    );
    assert!(!sent.contains("codelake_checked"), "{sent}");
}

#[tokio::test]
async fn check_local_node_checks_lifted_nodes() {
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};
    let plan = LogicalPlanBuilder::values(vec![vec![lit(i64::MAX)]])
        .unwrap()
        .project(vec![col("column1") + lit(1_i64)])
        .unwrap()
        .build()
        .unwrap();
    let checked = super::check_local_node(plan).unwrap();
    assert!(
        checked
            .display_indent()
            .to_string()
            .contains("codelake_checked_add"),
        "{}",
        checked.display_indent()
    );
}
