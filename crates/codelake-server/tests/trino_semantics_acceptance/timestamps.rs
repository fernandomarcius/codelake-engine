//! Casts from text with time zones: many zones, the historical DST of
//! `America/Sao_Paulo`, instants before 1970 and LMT offsets.

use super::{Res, expect_same};

/// `timestamp(p) with time zone` from text keeps the zone; the instant is
/// checked against the same instant written in UTC.
const WITH_ZONE: &[&str] = &[
    // America/Sao_Paulo DST, 2018/2019: 2018-11-04 00:00 jumped to 01:00
    // (gap), 2019-02-16 23:00-23:59 happened twice (overlap).
    "SELECT CAST('2018-11-04 00:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), CAST('2019-02-17 00:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), CAST('2018-06-01 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('2018-11-04 00:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('2018-11-04 03:30:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('2019-02-17 01:30:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2019-02-17 00:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('2019-02-17 03:30:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2018-01-15 12:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('2018-01-15 14:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)",
    // Other zones: DST gap/overlap in New York and Lisbon, half and
    // three-quarter hour offsets, the inverted Etc/GMT sign, fixed offsets.
    "SELECT CAST('2026-03-08 02:30:00 America/New_York' AS TIMESTAMP WITH TIME ZONE), CAST('2026-11-01 01:30:00 America/New_York' AS TIMESTAMP WITH TIME ZONE), CAST('2026-03-29 01:30:00 Europe/Lisbon' AS TIMESTAMP WITH TIME ZONE), CAST('2026-06-01 12:00:00 Asia/Kolkata' AS TIMESTAMP WITH TIME ZONE), CAST('2026-06-01 12:00:00 Asia/Kathmandu' AS TIMESTAMP WITH TIME ZONE), CAST('2026-06-01 12:00:00 Australia/Lord_Howe' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 12:00:00 Pacific/Chatham' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('2026-03-08 02:30:00 America/New_York' AS TIMESTAMP WITH TIME ZONE) = CAST('2026-03-08 07:30:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2026-11-01 01:30:00 America/New_York' AS TIMESTAMP WITH TIME ZONE) = CAST('2026-11-01 05:30:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2026-06-01 12:00:00 Asia/Kathmandu' AS TIMESTAMP WITH TIME ZONE) = CAST('2026-06-01 06:15:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 12:00:00 Pacific/Chatham' AS TIMESTAMP WITH TIME ZONE) = CAST('2025-12-31 22:15:00 UTC' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('2026-01-01 10:00:00 Etc/GMT+3' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 10:00:00 +02:00' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 10:00:00 -03:00' AS TIMESTAMP(0) WITH TIME ZONE), CAST('2026-01-01 10:00:00.123456 +05:30' AS TIMESTAMP(6) WITH TIME ZONE), CAST('2026-01-01 10:00:00.123456789 UTC' AS TIMESTAMP(9) WITH TIME ZONE), CAST('2026-01-01 10:00:00.5 UTC' AS TIMESTAMP(0) WITH TIME ZONE)",
    // Before 1970, and before standard time: Sao Paulo used LMT -03:06:28
    // until 1914.
    "SELECT CAST('1969-12-31 23:59:59.999 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('1900-01-01 00:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), CAST('1960-06-15 12:00:00.5 -05:00' AS TIMESTAMP(3) WITH TIME ZONE), CAST('1932-10-03 00:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST('1900-01-01 00:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('1900-01-01 03:06:28 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('1969-12-31 21:00:00.001 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('1970-01-01 00:00:00.001 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('1914-01-01 00:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('1914-01-01 03:00:00 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('1913-12-31 23:59:59 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) = CAST('1914-01-01 03:06:27 UTC' AS TIMESTAMP WITH TIME ZONE)",
    "SELECT CAST(CAST('2026-01-01 10:00:00 +05:30' AS TIMESTAMP WITH TIME ZONE) AS DATE), CAST(CAST('2026-01-01 23:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS DATE), CAST(CAST('1969-12-31 22:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS DATE)",
    "SELECT CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) < CAST('2026-01-01 12:59:59 UTC' AS TIMESTAMP WITH TIME ZONE), CAST('2026-01-01 10:00:00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE) < CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)",
];

/// `timestamp(p)` from text ignores the zone the text names; layout,
/// precision and half-up rounding, before and after 1970.
const WITHOUT_ZONE: &[&str] = &[
    "SELECT CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP), CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP), CAST('2026-06-01 12:00:00 Asia/Kathmandu' AS TIMESTAMP), CAST('2026-01-01 10:00:00 +05:30' AS TIMESTAMP), CAST('1900-01-01 00:00:00 America/Sao_Paulo' AS TIMESTAMP)",
    "SELECT CAST('2026-01-01 10:00:00.1235' AS TIMESTAMP), CAST('2026-01-01 23:59:59.9995' AS TIMESTAMP), CAST('2026-01-01 10:00:00.5' AS TIMESTAMP(0)), CAST('2026-1-1 1:2:3' AS TIMESTAMP), CAST(' 2026-01-01 10:00 ' AS TIMESTAMP(6)), CAST('2026-01-01 10:00:00.123456789' AS TIMESTAMP(9))",
    "SELECT CAST('1969-12-31 23:59:59.999' AS TIMESTAMP), CAST('1969-12-31 23:59:59.9995' AS TIMESTAMP), CAST('1969-12-31 23:59:59.4' AS TIMESTAMP(0)), CAST('1969-12-31 23:59:59.5' AS TIMESTAMP(0)), CAST('1900-02-28 12:00:00.0005' AS TIMESTAMP), CAST('0001-01-01 00:00:00' AS TIMESTAMP), CAST('1582-10-10 00:00:00' AS TIMESTAMP)",
    "SELECT CAST(x AS TIMESTAMP) FROM (VALUES (1, '2026-01-01 10:00:00 America/Sao_Paulo'), (2, '2019-02-16 23:30:00.5 America/Sao_Paulo'), (3, '1960-06-15 12:00:00 -05:00'), (4, '2026-01-01 10:00:00'), (5, NULL)) t(k, x) ORDER BY k",
    "SELECT TRY_CAST('garbage' AS TIMESTAMP), TRY_CAST('2026-13-01 00:00:00' AS TIMESTAMP)",
    "SELECT CAST('2026-01-01T10:00:00' AS TIMESTAMP)",
    "SELECT CAST('2026-01-01 10:00:00 Mars/Olympus' AS TIMESTAMP)",
    "SELECT CAST('2026-02-30 10:00:00' AS TIMESTAMP)",
    "SELECT CAST(x AS TIMESTAMP) FROM (VALUES ('garbage')) t(x)",
];

#[tokio::test]
async fn casts_to_timestamp_with_time_zone_match_trino() -> Res<()> {
    expect_same(&[], WITH_ZONE, None).await
}

#[tokio::test]
async fn casts_to_timestamp_match_trino() -> Res<()> {
    expect_same(&[], WITHOUT_ZONE, None).await
}

/// A session zone does not change a cast to `timestamp(p)`, nor a text that
/// names its own zone.
#[tokio::test]
async fn session_zone_does_not_move_zoned_text() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP), CAST('2026-01-01 10:00:00' AS TIMESTAMP), CAST('2026-01-01 10:00:00 Asia/Tokyo' AS TIMESTAMP WITH TIME ZONE)",
            "SELECT CAST('2018-11-04 00:30:00 UTC' AS TIMESTAMP WITH TIME ZONE) = CAST('2018-11-03 22:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE)",
        ],
        Some("America/Sao_Paulo"),
    )
    .await
}

// ---------------------------------------------------------------- defects

#[tokio::test]
#[ignore = "DEFECT-SEM3: CAST(timestamp with time zone AS timestamp) takes the local time in \
    UTC, not in the value's zone. Expected: CAST(CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS \
    TIMESTAMP WITH TIME ZONE) AS TIMESTAMP) = 2026-01-01 10:00:00.000; the same from a column of \
    Sao_Paulo and Tokyo rows = 10:00, 10:00. Obtained: 13:00:00.000; 01:00 and 13:00"]
async fn defect_sem3_zoned_to_local_timestamp_uses_the_value_zone() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST(CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP), CAST(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP)",
            "SELECT CAST(CAST(x AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP) FROM (VALUES (1, '2026-01-01 10:00:00 America/Sao_Paulo'), (2, '2026-01-01 10:00:00 Asia/Tokyo')) t(k, x) ORDER BY k",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM4: the session time zone (X-Trino-Time-Zone) is ignored. Expected with \
    session America/Sao_Paulo: CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE) = \
    2026-01-01 10:00:00.000 America/Sao_Paulo (13:00 UTC); CAST(TIMESTAMP ... AS TIMESTAMP WITH \
    TIME ZONE) and CAST(DATE ...) likewise. Obtained: 2026-01-01 10:00:00.000 UTC, an instant 3 \
    hours off"]
async fn defect_sem4_session_time_zone_applies_to_unzoned_values() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE), CAST(TIMESTAMP '2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE), CAST(DATE '2026-01-01' AS TIMESTAMP WITH TIME ZONE)",
            "SELECT CAST(x AS TIMESTAMP WITH TIME ZONE) FROM (VALUES ('2026-01-01 10:00:00')) t(x)",
            "SELECT CAST('2026-01-01 10:00:00' AS TIMESTAMP WITH TIME ZONE) = CAST('2026-01-01 13:00:00 UTC' AS TIMESTAMP WITH TIME ZONE)",
        ],
        Some("America/Sao_Paulo"),
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM5: a timestamp(3) with time zone column created by CREATE TABLE loses the \
    zone. Expected: information_schema data_type 'timestamp(3) with time zone'; inserting \
    TIMESTAMP '2026-01-01 10:00:00 America/Sao_Paulo' reads back '2026-01-01 10:00:00.000 \
    America/Sao_Paulo'. Obtained: data_type 'timestamp(3)', value timestamp(3) '2026-01-01 \
    13:00:00.000'"]
async fn defect_sem5_zoned_column_keeps_its_zone() -> Res<()> {
    expect_same(
        &[
            "CREATE SCHEMA {c}.sem_tz",
            "CREATE TABLE {c}.sem_tz.t (id bigint, tstz timestamp(3) with time zone)",
            "INSERT INTO {c}.sem_tz.t VALUES (1, TIMESTAMP '2026-01-01 10:00:00 America/Sao_Paulo')",
        ],
        &[
            "SELECT column_name, data_type FROM {c}.information_schema.columns WHERE table_schema = 'sem_tz' ORDER BY ordinal_position",
            "SELECT id, tstz FROM {c}.sem_tz.t",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM8: text in a DST gap cast to timestamp(p): Trino resolves the local time in \
    the named zone first. Expected: CAST('2018-11-04 00:30:00 America/Sao_Paulo' AS TIMESTAMP) = \
    2018-11-04 01:30:00.000. Obtained: 2018-11-04 00:30:00.000"]
async fn defect_sem8_dst_gap_text_to_timestamp() -> Res<()> {
    expect_same(
        &[],
        &["SELECT CAST('2018-11-04 00:30:00 America/Sao_Paulo' AS TIMESTAMP), CAST('2026-03-08 02:30:00 America/New_York' AS TIMESTAMP)"],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM9: CAST(varchar column AS timestamp with time zone) drops each row's zone. \
    Expected: '2026-01-01 10:00:00.000 Asia/Tokyo', '... UTC', '... America/Sao_Paulo'. Obtained: \
    every row in UTC ('2026-01-01 01:00:00.000 UTC', ..., '2026-01-01 13:00:00.000 UTC')"]
async fn defect_sem9_column_cast_keeps_each_rows_zone() -> Res<()> {
    expect_same(
        &[],
        &["SELECT CAST(x AS TIMESTAMP WITH TIME ZONE) FROM (VALUES (1, '2026-01-01 10:00:00 America/Sao_Paulo'), (2, '2026-01-01 10:00:00 Asia/Tokyo'), (3, '2026-01-01 10:00:00')) t(k, x) ORDER BY k"],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM10: to_unixtime is double with the fraction of a second. Expected: \
    to_unixtime(CAST('1969-12-31 23:59:59.999 UTC' AS TIMESTAMP WITH TIME ZONE)) = -0.001 (double). \
    Obtained: bigint 0"]
async fn defect_sem10_to_unixtime_keeps_milliseconds() -> Res<()> {
    expect_same(
        &[],
        &["SELECT to_unixtime(CAST('1969-12-31 23:59:59.999 UTC' AS TIMESTAMP WITH TIME ZONE)), to_unixtime(CAST('2018-11-04 00:30:00.250 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE))"],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM11: CAST(timestamp with time zone AS varchar) uses RFC 3339. Expected: \
    '2019-02-16 23:30:00.000 America/Sao_Paulo'. Obtained: '2019-02-16T23:30:00-02:00' (and \
    '2026-01-01T10:00:00Z' for UTC)"]
async fn defect_sem11_zoned_timestamp_to_varchar() -> Res<()> {
    expect_same(
        &[],
        &["SELECT CAST(CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AS VARCHAR), CAST(CAST('2026-01-01 10:00:00 UTC' AS TIMESTAMP WITH TIME ZONE) AS VARCHAR)"],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM12: only precisions 0, 3, 6 and 9 exist. Expected: CAST('2026-01-01 \
    10:00:00.5' AS TIMESTAMP(1)) = timestamp(1) '2026-01-01 10:00:00.5' (also 2, 4, 5, 7, 8, with \
    and without time zone). Obtained: NOT_SUPPORTED 'Unsupported SQL type TIMESTAMP(1)'"]
async fn defect_sem12_every_timestamp_precision() -> Res<()> {
    expect_same(
        &[],
        &[
            "SELECT CAST('2026-01-01 10:00:00.5' AS TIMESTAMP(1)), CAST('2026-01-01 10:00:00.12' AS TIMESTAMP(2)), CAST('2026-01-01 10:00:00.12345' AS TIMESTAMP(4)), CAST('2026-01-01 10:00:00.1234567' AS TIMESTAMP(7))",
            "SELECT CAST('1960-06-15 12:00:00.5 -05:00' AS TIMESTAMP(1) WITH TIME ZONE)",
        ],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM13: TRY_CAST(... AS TIMESTAMP WITH TIME ZONE) has no zone. Expected: \
    timestamp(3) with time zone '2026-01-01 10:00:00.000 America/Sao_Paulo' and NULL. Obtained: \
    timestamp(9) '2026-01-01 10:00:00.000000000' and NULL"]
async fn defect_sem13_try_cast_with_time_zone() -> Res<()> {
    expect_same(
        &[],
        &["SELECT TRY_CAST('2026-01-01 10:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE), TRY_CAST('garbage' AS TIMESTAMP WITH TIME ZONE)"],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM14: AT TIME ZONE widens the precision. Expected: timestamp(3) with time \
    zone '2019-02-17 01:30:00.000 UTC'. Obtained: timestamp(9) with time zone '2019-02-17 \
    01:30:00.000000000 UTC'"]
async fn defect_sem14_at_time_zone_keeps_precision() -> Res<()> {
    expect_same(
        &[],
        &["SELECT CAST('2019-02-16 23:30:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'UTC', CAST('1900-01-01 00:00:00 America/Sao_Paulo' AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'UTC'"],
        None,
    )
    .await
}

#[tokio::test]
#[ignore = "DEFECT-SEM15: a TIMESTAMP literal takes the precision of its digits. Expected: \
    TIMESTAMP '2026-01-01 10:00:00.123' is timestamp(3) '2026-01-01 10:00:00.123'; without \
    fraction timestamp(0). Obtained: timestamp(9) '2026-01-01 10:00:00.123000000'"]
async fn defect_sem15_timestamp_literal_precision() -> Res<()> {
    expect_same(
        &[],
        &["SELECT TIMESTAMP '2026-01-01 10:00:00.123', TIMESTAMP '2026-01-01 10:00:00', TIMESTAMP '1969-12-31 23:59:59.5'"],
        None,
    )
    .await
}
