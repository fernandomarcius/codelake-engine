//! SQL Server's open-end sentinels (`9999-12-31 23:59:59.9999999`,
//! `0001-01-01`) in a `datetime2` column (`datetime2(7)` by default), read
//! alone and where they meet other timestamps, against Trino 431 reading the
//! same SQL Server.
//!
//! The engine reads `datetime2(7)` in microseconds (`timestamp(6)`, the 7th
//! digit cut: a nanosecond timestamp holds only the years 1677 to 2262), so
//! its answer is the reference's at six digits, never another value: a
//! common type with a coarser timestamp no longer rounded the sentinel to
//! `+10000-01-01` nor overflowed widening it to nanoseconds. A constant with
//! a 7th digit meeting such a column is refused, never compared or united
//! rounded (`where = ts(7)` answered no row; the reference answers row 3).

use crate::common;
use crate::join_regressions::{Trino, note, open, shared, sorted, try_cl};
use crate::sqlserver_dynamic_filter::{SA_PASSWORD, exec, sqlserver, start_mssql};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;

const SEED: &str = "CREATE TABLE dbo.vigencia (
         id INT NOT NULL, d7 DATETIME2 NULL, d3 DATETIME2(3) NULL, dt DATETIME NULL);
     INSERT INTO dbo.vigencia VALUES
         (1, '9999-12-31 23:59:59.9999999', '9999-12-31 23:59:59.999', '9999-12-31 23:59:59.997'),
         (2, '0001-01-01 00:00:00', '0001-01-01 00:00:00', '1753-01-01 00:00:00'),
         (3, '2026-01-01 10:00:00.1234567', '2026-01-01 10:00:00.123', '2026-01-01 10:00:00.123'),
         (4, NULL, '2026-01-02 00:00:00', NULL);";

const T: &str = "warehouse.dbo.vigencia";

/// Queries the engine refuses, with what its message says: a constant with a
/// 7th digit (exact or refused, never rounded), and a sentinel cast to
/// `timestamp(9)` (no nanosecond timestamp holds it).
const REFUSED: [(&str, &str); 5] = [
    ("where = ts(7) literal", "has more than 6 fraction digits"),
    ("compare ts(7) literal", "has more than 6 fraction digits"),
    ("union ts(7) literal", "has more than 6 fraction digits"),
    ("coalesce ts(7) literal", "has more than 6 fraction digits"),
    ("union ts(9) cast", "Overflow"),
];

/// The reference's answer at the engine's precision: `timestamp(7..9)` is
/// `timestamp(6)` and its values keep six fraction digits; `NULL` as the
/// engine prints it.
fn six_digits(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|v| match v.as_str() {
                    "" => "NULL".to_owned(),
                    "timestamp(7)" | "timestamp(8)" | "timestamp(9)" => "timestamp(6)".to_owned(),
                    text => match text.split_once('.') {
                        Some((whole, fraction)) if whole.len() == 19 && fraction.len() > 6 => {
                            format!("{whole}.{}", &fraction[..6])
                        }
                        _ => text.to_owned(),
                    },
                })
                .collect()
        })
        .collect()
}

fn queries() -> Vec<(&'static str, String)> {
    let shown = |x: &str, from: &str| format!("SELECT typeof({x}), CAST({x} AS varchar) FROM {from}");
    vec![
        (
            "alone",
            format!("SELECT id, typeof(d7), CAST(d7 AS varchar) FROM {T}"),
        ),
        (
            "where >",
            format!("SELECT id FROM {T} WHERE d7 > TIMESTAMP '9000-01-01 00:00:00'"),
        ),
        (
            "where <",
            format!("SELECT id FROM {T} WHERE d7 < TIMESTAMP '1000-01-01 00:00:00'"),
        ),
        (
            "where = ts(3)",
            format!("SELECT id FROM {T} WHERE d7 >= TIMESTAMP '9999-12-31 23:59:59.999'"),
        ),
        (
            "where = ts(7) literal",
            format!("SELECT id FROM {T} WHERE d7 = TIMESTAMP '2026-01-01 10:00:00.1234567'"),
        ),
        (
            "compare ts(7) literal",
            format!(
                "SELECT id, CAST(d7 = TIMESTAMP '2026-01-01 10:00:00.1234567' AS varchar), \
                 CAST(d7 > TIMESTAMP '2026-01-01 10:00:00.1234567' AS varchar) FROM {T}"
            ),
        ),
        (
            "union datetime2(3)",
            shown(
                "x",
                &format!("(SELECT d7 AS x FROM {T} UNION ALL SELECT d3 FROM {T})"),
            ),
        ),
        (
            "union datetime",
            shown(
                "x",
                &format!("(SELECT d7 AS x FROM {T} UNION ALL SELECT dt FROM {T})"),
            ),
        ),
        (
            "union ts(3) literal",
            shown(
                "x",
                &format!("(SELECT d7 AS x FROM {T} UNION ALL SELECT TIMESTAMP '2026-01-01 10:00:00.123')"),
            ),
        ),
        (
            "union ts(7) literal",
            shown(
                "x",
                &format!(
                    "(SELECT d7 AS x FROM {T} UNION ALL SELECT TIMESTAMP '2026-01-01 10:00:00.1234567')"
                ),
            ),
        ),
        (
            "union ts(9) cast",
            shown(
                "x",
                &format!("(SELECT d7 AS x FROM {T} UNION ALL SELECT CAST(d3 AS timestamp(9)) FROM {T})"),
            ),
        ),
        (
            "case datetime2(3)",
            shown("CASE WHEN id = 4 THEN d3 ELSE d7 END", T),
        ),
        ("case datetime", shown("CASE WHEN id = 4 THEN dt ELSE d7 END", T)),
        ("coalesce datetime2(3)", shown("coalesce(d7, d3)", T)),
        ("coalesce datetime", shown("coalesce(d7, dt)", T)),
        (
            "coalesce ts(7) literal",
            shown("coalesce(d7, TIMESTAMP '2026-01-01 10:00:00.1234567')", T),
        ),
        (
            "min/max",
            format!("SELECT typeof(max(d7)), CAST(min(d7) AS varchar), CAST(max(d7) AS varchar) FROM {T}"),
        ),
        ("greatest", shown("greatest(d7, d3)", T)),
    ]
}

#[tokio::test]
async fn datetime2_sentinels_against_the_reference() {
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE warehouse").await;
    exec(&host, port, "warehouse", SEED).await;
    let shared = shared().await;
    let catalog = format!(
        "connector.name=sqlserver\n\
         connection-url=jdbc:sqlserver://{host}:{port};database=warehouse;encrypt=false\n\
         connection-user=sa\n\
         connection-password={SA_PASSWORD}\n"
    );
    let trino = Trino::start_with(&shared, "sentinels", &[("warehouse", catalog)]).await;
    let spill = tempfile::tempdir().unwrap();
    let mut engine = open(
        &shared.warehouse(),
        spill.path(),
        ByteSize::mib(256),
        &ExecutionConfig::default(),
        &[],
    )
    .await;
    common::attach(&mut engine, sqlserver(&host, port)).await;

    let mut failures = Vec::new();
    for (label, query) in queries() {
        let theirs = sorted(trino.batch(&[&query]).await.remove(0));
        let ours = try_cl(&engine, &query).await.map(sorted);
        note(label, format!("trino {theirs:?}\n      engine {ours:?}"));
        let refused = REFUSED.iter().find(|(l, _)| *l == label).map(|(_, m)| *m);
        match (refused, ours) {
            (Some(message), Err(error)) if error.contains(message) => {}
            (None, Ok(ours)) if ours == sorted(six_digits(&theirs)) => {}
            (_, other) => failures.push(format!("{label}: the engine {other:?}, trino {theirs:?}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
