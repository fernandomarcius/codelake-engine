//! Casts from text with time zones. Every text is checked as the zoned
//! value, as its instant (`to_unixtime`), as text, as a local timestamp and
//! as a date, through literals and through table rows.

use super::{Pair, Res, expect_errors, expect_rows, verdict};

/// Local times in a `America/Sao_Paulo` gap that begins at midnight before
/// 1970. Trino 431 casts them to the day after (see
/// `trino_431_dates_a_pre_1970_midnight_gap_a_day_late`).
const MIDNIGHT_GAPS: [&str; 3] = [
    "1914-01-01 00:00:00",
    "1932-10-03 00:30:00",
    "1963-10-23 00:30:00",
];

/// Local times at the DST transitions of `America/Sao_Paulo` from the first
/// one (1931) to the last (2019), gaps and overlaps, the LMT boundary of
/// 1914 and a date after DST was abolished.
const SAO_PAULO: &[&str] = &[
    "1913-12-31 23:59:59.999",
    "1914-01-01 00:00:00",
    "1931-10-03 11:30:00",
    "1932-03-31 23:30:00",
    "1932-10-03 00:30:00",
    "1963-10-23 00:30:00",
    "1964-02-29 23:30:00",
    "1965-06-15 12:00:00",
    "1985-11-02 00:30:00",
    "1986-03-14 23:30:00",
    "1999-10-03 00:30:00",
    "2000-02-26 23:30:00",
    "2008-10-19 00:30:00",
    "2009-02-14 23:30:00",
    "2018-11-04 00:30:00",
    "2019-02-16 23:30:00",
    "2019-11-03 00:30:00",
];

/// Odd zones and links, other DST rules, offsets, and instants before 1970
/// (Europe/London stayed at +01:00 from 1968 to 1971).
const ZONED_TEXTS: &[&str] = &[
    "2026-10-25 01:30:00 Europe/London",
    "2026-03-29 01:30:00 Europe/London",
    "1970-01-01 00:00:00 Europe/London",
    "1968-02-18 02:30:00 Europe/London",
    "2026-10-04 02:30:00 Australia/Sydney",
    "2026-04-05 02:30:00 Australia/Sydney",
    "2011-12-30 12:00:00 Pacific/Apia",
    "2011-12-29 23:59:59 Pacific/Apia",
    "2026-01-01 00:00:00 Pacific/Kiritimati",
    "2026-06-01 12:00:00 America/St_Johns",
    "2026-06-01 12:00:00 Asia/Tehran",
    "2026-03-01 12:00:00 Africa/Casablanca",
    "2026-06-01 12:00:00 America/Manaus",
    "2026-06-01 12:00:00 America/Noronha",
    "2026-06-01 12:00:00 America/Rio_Branco",
    "2026-06-01 12:00:00 Brazil/East",
    "2026-06-01 12:00:00 US/Pacific",
    "2026-06-01 12:00:00 GMT",
    "2026-06-01 12:00:00 Etc/UTC",
    "2026-06-01 12:00:00 Etc/GMT-14",
    "2026-06-01 12:00:00 +14:00",
    "2026-06-01 12:00:00 -12:00",
    "2026-06-01 12:00:00 +00:00",
    "1941-06-01 12:00:00 Asia/Kolkata",
    "1937-06-01 12:00:00 Europe/Amsterdam",
    "1901-12-13 20:45:52 UTC",
    "1800-01-01 00:00:00 America/Sao_Paulo",
    "0001-01-01 00:00:00 UTC",
    "1969-07-20 20:17:40.123456 UTC",
    "1969-12-31 23:59:59.9995 UTC",
    "1969-12-31 21:00:00.0005 America/Sao_Paulo",
];

fn zoned_exprs(text: &str) -> Vec<String> {
    let tz = format!("CAST('{text}' AS TIMESTAMP WITH TIME ZONE)");
    let mut exprs = vec![
        tz.clone(),
        format!("to_unixtime({tz})"),
        format!("CAST({tz} AS VARCHAR)"),
        format!("CAST('{text}' AS TIMESTAMP)"),
        format!("CAST('{text}' AS TIMESTAMP(6))"),
        format!("{tz} AT TIME ZONE 'America/Sao_Paulo'"),
        format!("CAST('{text}' AS TIMESTAMP(6) WITH TIME ZONE)"),
    ];
    if !MIDNIGHT_GAPS.iter().any(|gap| text.starts_with(gap)) {
        exprs.push(format!("CAST({tz} AS DATE)"));
    }
    exprs
}

/// Literal text naming `America/Sao_Paulo`, at every transition.
#[tokio::test]
async fn sao_paulo_transitions_from_literals_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    let mut wrong = Vec::new();
    for local in SAO_PAULO {
        let text = format!("{local} America/Sao_Paulo");
        wrong.extend(pair.batch(&zoned_exprs(&text), "", None).await?);
    }
    verdict(&wrong)
}

/// Literal text naming other zones and offsets.
#[tokio::test]
async fn other_zones_from_literals_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    let mut wrong = Vec::new();
    for text in ZONED_TEXTS {
        wrong.extend(pair.batch(&zoned_exprs(text), "", None).await?);
    }
    verdict(&wrong)
}

fn values(texts: &[String]) -> String {
    let rows: Vec<String> = texts
        .iter()
        .enumerate()
        .map(|(k, t)| format!("({k}, '{t}')"))
        .collect();
    format!("(VALUES {}) t(k, x)", rows.join(", "))
}

/// Every text of both lists, `America/Sao_Paulo` ones included, without
/// the midnight gaps Trino 431 dates a day late.
fn all_texts() -> Vec<String> {
    let mut texts: Vec<String> = SAO_PAULO
        .iter()
        .filter(|l| !MIDNIGHT_GAPS.contains(l))
        .map(|l| format!("{l} America/Sao_Paulo"))
        .collect();
    texts.extend(ZONED_TEXTS.iter().map(|t| (*t).to_owned()));
    texts
}

/// The same texts through a varchar column: the instant and the local time
/// of each row (the zone of each row is DEFECT-SEM9 of the first suite, so
/// the zoned value itself is not compared here; the date is DEFECT-SEM38).
#[tokio::test]
async fn zoned_texts_from_rows_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    let from = format!("FROM {} ORDER BY k", values(&all_texts()));
    let exprs = vec![
        "to_unixtime(CAST(x AS TIMESTAMP WITH TIME ZONE))".to_owned(),
        "CAST(x AS TIMESTAMP)".to_owned(),
        "CAST(x AS TIMESTAMP(0))".to_owned(),
        "CAST(x AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'UTC'".to_owned(),
    ];
    verdict(&pair.batch(&exprs, &from, None).await?)
}

/// Trino 431 casts a zoned value whose local time fell in a gap that began
/// at midnight before 1970 to the next day: `1932-10-03 00:30` in
/// `America/Sao_Paulo` is `1932-10-03 01:30 -02:00`, and Trino dates it
/// `1932-10-04`. `CodeLake` answers the local date instead of copying the
/// wrong day. Deliberate divergence, not a defect.
#[tokio::test]
async fn trino_431_dates_a_pre_1970_midnight_gap_a_day_late() -> Res<()> {
    let pair = Pair::start().await?;
    let mut wrong = Vec::new();
    for (local, trino_day, local_day) in [
        ("1914-01-01 00:00:00", "1914-01-02", "1914-01-01"),
        ("1932-10-03 00:30:00", "1932-10-04", "1932-10-03"),
        ("1963-10-23 00:30:00", "1963-10-24", "1963-10-23"),
    ] {
        let tz = format!("CAST('{local} America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)");
        let sql = format!("SELECT CAST({tz} AS DATE), CAST(CAST({tz} AS TIMESTAMP) AS DATE)");
        let (trino, codelake) = pair.both(&sql, None).await?;
        let trino_ok = trino["rows"] == serde_json::json!([[trino_day, local_day]]);
        let codelake_ok = codelake["rows"] == serde_json::json!([[local_day, local_day]]);
        if !trino_ok || !codelake_ok {
            wrong.push(format!("{sql}\n  trino    {trino}\n  codelake {codelake}"));
        }
    }
    verdict(&wrong)
}

/// Values without a zone take the session zone; values with a zone keep
/// theirs. Gaps and overlaps of the session zone, before and after 1970.
const SESSION_QUERIES: &[&str] = &[
    "SELECT CAST('2018-11-04 00:30:00' AS TIMESTAMP WITH TIME ZONE), to_unixtime(CAST('2018-11-04 00:30:00' AS TIMESTAMP WITH TIME ZONE))",
    "SELECT CAST(TIMESTAMP '2019-02-16 23:30:00' AS TIMESTAMP WITH TIME ZONE), to_unixtime(CAST(TIMESTAMP '2019-02-16 23:30:00' AS TIMESTAMP WITH TIME ZONE))",
    "SELECT CAST(DATE '2018-11-04' AS TIMESTAMP WITH TIME ZONE), to_unixtime(CAST(DATE '2018-11-04' AS TIMESTAMP WITH TIME ZONE))",
    "SELECT CAST(TIMESTAMP '1965-01-15 12:00:00.123' AS TIMESTAMP WITH TIME ZONE), to_unixtime(CAST(TIMESTAMP '1965-01-15 12:00:00.123' AS TIMESTAMP WITH TIME ZONE))",
    "SELECT CAST(CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP), CAST(CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE) AS DATE)",
    "SELECT TIMESTAMP '2026-01-01 10:00:00' = CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT k, CAST(x AS TIMESTAMP WITH TIME ZONE), to_unixtime(CAST(x AS TIMESTAMP WITH TIME ZONE)) FROM (VALUES (1, '2018-11-04 00:30:00'), (2, '2026-07-01 12:00:00'), (3, '1950-01-01 00:00:00')) t(k, x) ORDER BY k",
    "SELECT to_unixtime(CAST(DATE '1970-01-01' AS TIMESTAMP WITH TIME ZONE)), CAST(CAST(DATE '1970-01-01' AS TIMESTAMP WITH TIME ZONE) AS VARCHAR)",
];

#[tokio::test]
async fn session_zones_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    let mut wrong = Vec::new();
    for zone in [
        "America/Sao_Paulo",
        "Asia/Kathmandu",
        "+05:30",
        "America/New_York",
        "Pacific/Apia",
    ] {
        wrong.extend(pair.rows(SESSION_QUERIES, Some(zone)).await?);
    }
    verdict(&wrong)
}

/// Arithmetic on zoned values crosses DST in the value's zone.
const ARITHMETIC: &[&str] = &[
    "SELECT CAST('2018-11-03 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) + INTERVAL '24' HOUR, CAST('2019-02-16 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) + INTERVAL '90' MINUTE",
    "SELECT date_trunc('day', CAST('2018-11-04 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), date_trunc('hour', CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE))",
    "SELECT extract(HOUR FROM CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), extract(DAY FROM CAST('2026-01-01 01:00:00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE)), extract(YEAR FROM CAST('1969-12-31 22:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE))",
    "SELECT CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('2026-01-01 22:00:00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE)",
    // Instants only: a column keeps one zone (DEFECT-SEM9 of the first suite).
    "SELECT count(DISTINCT x), to_unixtime(min(x)), to_unixtime(max(x)) FROM (VALUES (CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)), (CAST('2026-01-01 13:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)), (CAST('2026-01-01 22:00:00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE)), (CAST('2026-01-01 12:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)), (CAST('2026-01-01 23:00:00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE))) t(x)",
];

#[tokio::test]
async fn zoned_arithmetic_matches_trino() -> Res<()> {
    expect_rows(&[], ARITHMETIC, None).await
}

/// Text Trino rejects.
const INVALID: &[&str] = &[
    "SELECT CAST('2026-01-01 10:00:00 America/Sao Paulo' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('2026-01-01 10:00:00 +25:00' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('2026-01-01 10:00:00 UTC garbage' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('2026-01-01 24:00:00' AS TIMESTAMP)",
    "SELECT CAST('2026-01-01 23:59:60' AS TIMESTAMP)",
    "SELECT CAST(x AS TIMESTAMP WITH TIME ZONE) FROM (VALUES ('2026-01-01 10:00:00 Mars/Olympus')) t(x)",
];

#[tokio::test]
async fn invalid_zoned_text_fails_like_trino() -> Res<()> {
    expect_errors(INVALID, None).await
}

// ---------------------------------------------------------------- defects

#[tokio::test]
#[ignore = "DEFECT-SEM35: an unknown timestamp precision is TYPE_MISMATCH in Trino. Expected: \
    CAST(... AS TIMESTAMP(13)) fails with TYPE_MISMATCH 'Unknown type: timestamp(13)'. Obtained: \
    NOT_SUPPORTED 'Unsupported SQL type TIMESTAMP(13)'"]
async fn defect_sem35_unknown_timestamp_precision_error_name() -> Res<()> {
    expect_errors(
        &[
            "SELECT CAST('2026-01-01 10:00:00' AS TIMESTAMP(13))",
            "SELECT CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP(13) WITH TIME ZONE)",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM36: comparing a timestamp with a timestamp with time zone ignores the \
    session zone: the timestamp is read as UTC, a wrong answer in WHERE. Expected with session \
    Asia/Kathmandu: TIMESTAMP '2026-01-01 10:00:00' < CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP \
    WITH TIME ZONE) is true, and WHERE ts >= that value keeps only 16:00; with America/Sao_Paulo, \
    > is true. Obtained: < and > both false (equal), WHERE keeps 10:00 and 16:00"]
async fn defect_sem36_timestamp_meets_zoned_value_in_the_session_zone() -> Res<()> {
    let pair = Pair::start().await?;
    let mut wrong = Vec::new();
    for zone in ["Asia/Kathmandu", "+05:30", "Pacific/Apia", "America/Sao_Paulo"] {
        wrong.extend(
            pair.rows(
                &[
                    "SELECT TIMESTAMP '2026-01-01 10:00:00' < CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE), TIMESTAMP '2026-01-01 10:00:00' > CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)",
                    "SELECT k FROM (VALUES (1, TIMESTAMP '2026-01-01 10:00:00'), (2, TIMESTAMP '2026-01-01 16:00:00')) t(k, ts) WHERE ts >= CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE) ORDER BY k",
                ],
                Some(zone),
            )
            .await?,
        );
    }
    verdict(&wrong)
}

#[tokio::test]
#[ignore = "DEFECT-SEM37: timestamp with time zone + INTERVAL 'n' DAY adds n * 24 hours in \
    Trino 431. Expected: CAST('2018-11-03 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME \
    ZONE) + INTERVAL '1' DAY = '2018-11-04 13:00:00.000 America/Sao_Paulo' (DST began in \
    between), '2019-02-16 12:00:00' + 1 day = '2019-02-17 11:00:00.000'. Obtained: 12:00:00.000 \
    for both (calendar day)"]
async fn defect_sem37_zoned_plus_day_interval() -> Res<()> {
    expect_rows(
        &[],
        &[
            "SELECT CAST('2018-11-03 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) + INTERVAL '1' DAY, CAST('2019-02-16 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) + INTERVAL '1' DAY, CAST('2018-11-05 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) - INTERVAL '2' DAY",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM38: CAST(CAST(varchar column AS TIMESTAMP WITH TIME ZONE) AS DATE) is the \
    UTC date, not the date where the value is. Expected (Trino): '1913-12-31 23:59:59.999 \
    America/Sao_Paulo' -> 1913-12-31, '1970-01-01 00:00:00 Europe/London' -> 1970-01-01, \
    '2026-01-01 00:00:00 Pacific/Kiritimati' -> 2026-01-01, '2026-06-01 12:00:00 -12:00' -> \
    2026-06-01. Obtained: 1914-01-01, 1969-12-31, 2025-12-31, 2026-06-02 (the same literals are \
    right)"]
async fn defect_sem38_date_of_a_zoned_value_from_rows() -> Res<()> {
    let pair = Pair::start().await?;
    let from = format!("FROM {} ORDER BY k", values(&all_texts()));
    verdict(
        &pair
            .rows(
                &[&format!(
                    "SELECT k, CAST(CAST(x AS TIMESTAMP WITH TIME ZONE) AS DATE) {from}"
                )],
                None,
            )
            .await?,
    )
}
