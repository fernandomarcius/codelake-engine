//! `lower(column)`/`upper(column)` compared with text literals reach SQL
//! Server as a filter that keeps at least the engine's rows, which the engine
//! then narrows: the monitoring screen's `WHERE lower(tabela) = 'recebidas'`
//! read every row of the table. A query ordered by a column of the table and
//! limited reaches it as `TOP n … ORDER BY` when its whole filter is exact.
//!
//! Every answer is compared with the engine's own over a copy of the table in
//! memory (no source to push to), on columns of a case-insensitive, a
//! case-sensitive, a binary and a Turkish collation, with values whose case
//! mapping leaves ASCII (`İ`, `ı`, the Kelvin sign, `ſ`, `ß`), accents and
//! trailing spaces.

use std::fmt::Write as _;

use crate::common;
use crate::join_regressions::{note, physical_plan, sorted, try_cl};
use crate::sqlserver_dynamic_filter::{exec, sqlserver, start_mssql};

/// Text columns of each collation, all holding the same value per row.
const COLUMNS: [&str; 6] = ["ci", "ci_n", "cs", "cs_n", "bin_n", "tr_n"];

const SEED: &str = "CREATE TABLE dbo.textos (
       id INT NOT NULL PRIMARY KEY,
       ci VARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
       ci_n NVARCHAR(30) COLLATE Latin1_General_100_CI_AS NULL,
       cs VARCHAR(30) COLLATE Latin1_General_CS_AS NULL,
       cs_n NVARCHAR(30) COLLATE Latin1_General_CS_AS NULL,
       bin_n NVARCHAR(30) COLLATE Latin1_General_100_BIN2 NULL,
       tr_n NVARCHAR(30) COLLATE Turkish_CI_AS NULL,
       n INT NULL,
       ts DATETIME2(3) NULL);
     INSERT INTO dbo.textos (id, ci_n, n, ts) VALUES
       (1, N'recebidas', 5, '2026-01-01T10:00:00'),
       (2, N'Recebidas', NULL, '2026-01-02T10:00:00'),
       (3, N'RECEBIDAS', 7, NULL),
       (4, N'recebidas ', 1, '2026-01-04T10:00:00'),
       (5, N'recébidas', 2, '2026-01-05T10:00:00'),
       (6, N'RECEB' + NCHAR(304) + N'DAS', 3, '2026-01-06T10:00:00'),
       (7, N'receb' + NCHAR(305) + N'das', 4, '2026-01-07T10:00:00'),
       (8, NULL, 8, '2026-01-08T10:00:00'),
       (9, N'kit', 9, '2026-01-09T10:00:00'),
       (10, NCHAR(8490) + N'IT', 10, '2026-01-10T10:00:00'),
       (11, N'KIT', 11, '2026-01-11T10:00:00'),
       (12, NCHAR(383) + N'im', 12, '2026-01-12T10:00:00'),
       (13, N'SIM', 13, '2026-01-13T10:00:00'),
       (14, N'stra' + NCHAR(223) + N'e', 14, '2026-01-14T10:00:00'),
       (15, N'STRASSE', 15, '2026-01-15T10:00:00'),
       (16, N'Isim', 16, '2026-01-16T10:00:00'),
       (17, NCHAR(305) + N'sim', 17, '2026-01-17T10:00:00'),
       (18, N'a_b%c', 18, '2026-01-18T10:00:00'),
       (19, N'AXB%C', 19, '2026-01-19T10:00:00'),
       (20, N'Pagas', 20, '2026-01-20T10:00:00');
     UPDATE dbo.textos SET ci = ci_n, cs = ci_n, cs_n = ci_n, bin_n = ci_n, tr_n = ci_n;";

/// Predicates over `{c}`, one of [`COLUMNS`].
const PREDICATES: [&str; 12] = [
    "lower({c}) = 'recebidas'",
    "'recebidas' = lower({c})",
    "upper({c}) = 'RECEBIDAS'",
    "lower({c}) = 'Recebidas'",
    "lower({c}) IN ('recebidas', 'pagas')",
    "upper({c}) IN ('KIT', 'SIM', 'ISIM')",
    "lower({c}) = 'kit'",
    "upper({c}) = 'STRASSE'",
    "lower({c}) = 'recebidas ' OR lower({c}) = 'a_b%c'",
    "lower({c}) = 'a_b%c' AND n > 0",
    "lower({c}) = 'recebidas' AND n > 4",
    "lower({c}) <> 'recebidas'",
];

/// The `WHERE` of every query sent to SQL Server for `sql`.
async fn remote_where(engine: &codelake_core::Engine, sql: &str) -> Vec<String> {
    physical_plan(engine, sql)
        .await
        .lines()
        .filter(|l| l.contains("VirtualExecutionPlan") && l.contains("name=sqlserver"))
        .filter_map(|l| l.split_once(" WHERE "))
        .map(|(_, w)| w.split(" rewritten_").next().unwrap_or(w).to_owned())
        .collect()
}

/// The T-SQL sent to SQL Server for `sql`.
async fn remote_sql(engine: &codelake_core::Engine, sql: &str) -> Vec<String> {
    physical_plan(engine, sql)
        .await
        .lines()
        .filter(|l| l.contains("VirtualExecutionPlan") && l.contains("name=sqlserver"))
        .filter_map(|l| l.split_once("rewritten_executor_sql=").map(|(_, s)| s.to_owned()))
        .collect()
}

/// One SQL Server for both checks.
#[tokio::test]
async fn case_functions_and_first_rows_reach_sql_server_and_keep_the_engines_rows() {
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE warehouse").await;
    exec(&host, port, "warehouse", SEED).await;
    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, sqlserver(&host, port)).await;
    try_cl(
        &engine,
        "CREATE TABLE datafusion.public.textos AS SELECT * FROM warehouse.dbo.textos",
    )
    .await
    .unwrap();
    case_functions(&engine).await;
    first_rows(&engine).await;
}

async fn case_functions(engine: &codelake_core::Engine) {
    let mut failures = String::new();
    let mut pushed = Vec::new();
    for column in COLUMNS {
        for predicate in PREDICATES {
            let predicate = predicate.replace("{c}", column);
            let query = |table: &str| format!("SELECT id FROM {table} WHERE {predicate}");
            let expected = try_cl(engine, &query("datafusion.public.textos"))
                .await
                .map(sorted);
            let got = try_cl(engine, &query("warehouse.dbo.textos")).await.map(sorted);
            if got != expected {
                let _ = writeln!(failures, "{predicate}: {got:?}, the engine alone {expected:?}");
            }
            let sent = remote_where(engine, &query("warehouse.dbo.textos")).await;
            if !sent.is_empty() {
                pushed.push((column, predicate.clone(), sent));
            }
        }
    }
    assert!(failures.is_empty(), "{failures}");
    let sent = |column: &str, predicate: &str| {
        pushed
            .iter()
            .find(|(c, p, _)| *c == column && p == &predicate.replace("{c}", column))
            .map(|(_, _, sent)| sent.join(" | "))
    };
    // A case-insensitive Latin collation compares the column itself;
    // a letter another character also becomes (`İ` → `i`) is `%` in a `LIKE`.
    let has = |column: &str, predicate: &str, parts: &[&str]| {
        let sent = sent(column, predicate).unwrap_or_default();
        assert!(
            !sent.is_empty() && parts.iter().all(|p| sent.contains(p)),
            "{column} {predicate}: {sent}"
        );
    };
    for column in ["ci", "ci_n"] {
        let c = format!("[{column}]");
        has(
            column,
            "lower({c}) = 'recebidas'",
            &[&format!("{c} LIKE 'receb%das'")],
        );
        has(column, "upper({c}) = 'STRASSE'", &[&format!("{c} LIKE '%TRA%E'")]);
        has(
            column,
            "lower({c}) = 'a_b%c' AND n > 0",
            &[&format!("{c} = 'a_b%c'"), "[n] > 0"],
        );
    }
    // Case-sensitive and binary ones compare its lower (upper) case.
    for column in ["cs", "cs_n", "bin_n"] {
        let c = format!("[{column}]");
        has(
            column,
            "lower({c}) IN ('recebidas', 'pagas')",
            &[
                &format!("lower([textos].{c}) LIKE 'receb%das'"),
                &format!("lower([textos].{c}) = 'pagas'"),
            ],
        );
    }
    // Turkish pairs `I` with `ı`: none of its text terms is sent (the
    // numeric one is); `<>` is never sent.
    for predicate in PREDICATES {
        let sent = sent("tr_n", predicate).unwrap_or_default();
        assert!(
            !sent.contains("tr_n] ") && !sent.contains("tr_n])"),
            "{predicate}: {sent}"
        );
    }
    for column in COLUMNS {
        assert_eq!(sent(column, "lower({c}) <> 'recebidas'"), None, "{column}");
    }
    for (column, predicate, sent) in &pushed {
        note(&format!("[{column}] {predicate}"), sent.join(" | "));
    }
}

/// `ORDER BY … LIMIT n` reaches SQL Server as `TOP` only under an exact
/// filter and by keys it orders as the engine.
async fn first_rows(engine: &codelake_core::Engine) {
    let mut failures = String::new();
    // (query over {t}, whether TOP reaches SQL Server)
    for (query, top) in [
        ("SELECT id, ts FROM {t} ORDER BY ts DESC LIMIT 3", true),
        ("SELECT id FROM {t} WHERE n > 4 ORDER BY ts DESC LIMIT 3", true),
        ("SELECT id, n FROM {t} ORDER BY n DESC, id LIMIT 4", true),
        // NULLs last ascending: SQL Server puts them first.
        ("SELECT id, ts FROM {t} ORDER BY ts LIMIT 3", false),
        ("SELECT id, ts FROM {t} ORDER BY ts NULLS FIRST LIMIT 3", true),
        (
            "SELECT id, ts FROM {t} ORDER BY ts DESC NULLS FIRST LIMIT 3",
            false,
        ),
        // A superset filter: the engine drops rows of SQL Server's first n.
        (
            "SELECT id FROM {t} WHERE lower(ci) = 'recebidas' ORDER BY ts DESC LIMIT 2",
            false,
        ),
        (
            "SELECT id FROM {t} WHERE ci = 'recebidas' ORDER BY ts DESC LIMIT 2",
            false,
        ),
        // Text orders by the collation.
        ("SELECT id FROM {t} ORDER BY ci DESC LIMIT 2", false),
        ("SELECT id FROM {t} ORDER BY n + 1 DESC LIMIT 2", false),
        ("SELECT id, ts FROM {t} ORDER BY ts DESC LIMIT 2 OFFSET 1", true),
    ] {
        let expected = try_cl(engine, &query.replace("{t}", "datafusion.public.textos")).await;
        let sql = query.replace("{t}", "warehouse.dbo.textos");
        let got = try_cl(engine, &sql).await;
        // Ties may come in another order; the rows must be the same.
        if got.clone().map(sorted) != expected.clone().map(sorted) {
            let _ = writeln!(failures, "{query}: {got:?}, the engine alone {expected:?}");
        }
        let sent = remote_sql(engine, &sql).await.join(" | ");
        note(query, &sent);
        if sent.contains("TOP") != top {
            let _ = writeln!(failures, "{query}: TOP expected {top}, sent {sent}");
        }
    }
    assert!(failures.is_empty(), "{failures}");
}
