//! Common type of SQL Server `datetime`, `smalldatetime` and `datetime2(n)`
//! columns where they meet, against Trino 431 reading the same SQL Server:
//! the reference maps them to `timestamp(3)`, `timestamp(0)` and
//! `timestamp(n)`, and a `UNION ALL` (or a `CASE`) of two of them is the
//! higher precision, the fraction kept. The engine typed a union of
//! `datetime2(3)` and `datetime2(0)` as `timestamp(0)` and dropped the
//! fraction, and a `CREATE TABLE AS` of `datetime` with `smalldatetime` into
//! the hive catalog (millisecond precision) was refused. Each engine stores
//! its own table in the shared metastore; the types the reference stored, the
//! types each reads from the other's, and the rows must agree.

use crate::common;
use crate::join_regressions::{Trino, note, open, shared, sorted, try_cl};
use crate::sqlserver_dynamic_filter::{SA_PASSWORD, exec, sqlserver, start_mssql};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;

/// `datetime` values are exact in its 1/300 s steps only for some
/// milliseconds (ISS-054): `.250` is.
const SEED: &str = "CREATE TABLE dbo.eventos (
         id INT NOT NULL, dt DATETIME NULL, sdt SMALLDATETIME NULL,
         d0 DATETIME2(0) NULL, d3 DATETIME2(3) NULL, d6 DATETIME2(6) NULL);
     INSERT INTO dbo.eventos VALUES
         (1, '2026-01-01 10:00:00.250', '2026-01-01 10:01:00', '2026-01-01 10:00:01',
             '2026-01-01 10:00:00.123', '2026-01-01 10:00:00.123456'),
         (2, '2026-01-02 23:59:59.997', '2026-01-02 23:59:00', '2026-01-02 23:59:59',
             '2026-01-02 23:59:59.999', '2026-01-02 23:59:59.999999');";

fn from(column: &str) -> String {
    format!("SELECT {column} AS x FROM warehouse.dbo.eventos")
}

/// `(label, query)`: the type and the text of every value, compared row by
/// row.
fn queries() -> Vec<(&'static str, String)> {
    let shown = |inputs: &[&str]| {
        let union: Vec<String> = inputs.iter().map(|c| from(c)).collect();
        format!(
            "SELECT typeof(x), CAST(x AS varchar) FROM ({})",
            union.join(" UNION ALL ")
        )
    };
    vec![
        ("datetime2(3) with datetime2(0)", shown(&["d3", "d0"])),
        ("datetime2(0) with datetime2(3)", shown(&["d0", "d3"])),
        ("datetime with smalldatetime", shown(&["dt", "sdt"])),
        (
            "smalldatetime, datetime2(0), datetime2(6)",
            shown(&["sdt", "d0", "d6"]),
        ),
        (
            "smalldatetime, datetime2(0), datetime",
            shown(&["sdt", "d0", "dt"]),
        ),
        (
            "UNION (distinct) of datetime2(0) and datetime2(3)",
            format!(
                "SELECT typeof(x), CAST(x AS varchar) FROM ({} UNION {})",
                from("d0"),
                from("d3")
            ),
        ),
        (
            "CASE of smalldatetime and datetime2(3)",
            "SELECT typeof(x), CAST(x AS varchar) FROM (SELECT CASE WHEN id = 1 THEN sdt ELSE d3 END AS x \
             FROM warehouse.dbo.eventos)"
                .to_owned(),
        ),
    ]
}

fn columns(table: &str) -> String {
    format!(
        "SELECT column_name, data_type FROM hive.information_schema.columns \
         WHERE table_schema = 'uts' AND table_name = '{table}' ORDER BY ordinal_position"
    )
}

#[tokio::test]
async fn datetime_columns_meet_in_the_higher_precision() {
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
    let trino = Trino::start_with(&shared, "timestamp-types", &[("warehouse", catalog)]).await;
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
        note(label, format!("trino {theirs:?}"));
        match try_cl(&engine, &query).await.map(sorted) {
            Ok(ours) if ours == theirs && !ours.is_empty() => {}
            other => failures.push(format!("{label}: the engine {other:?}, trino {theirs:?}")),
        }
    }

    // `CREATE TABLE AS` into hive, as dbt writes it: `datetime` with
    // `smalldatetime` is `timestamp(3)`, which the catalog stores.
    trino.batch(&["CREATE SCHEMA hive.uts"]).await;
    let union = format!("{} UNION ALL {}", from("dt"), from("sdt"));
    let create = |engine: &str| {
        format!(
            "create table \"hive\".\"uts\".\"mixed_{engine}\"\n WITH (format = 'PARQUET')\n as (\n{union}\n )"
        )
    };
    trino.batch(&[&create("tr")]).await;
    if let Err(error) = try_cl(&engine, &create("cl")).await {
        failures.push(format!("CREATE TABLE AS: {error}"));
        panic!("{}", failures.join("\n"));
    }
    let stored_by_trino = trino.batch(&[&columns("mixed_tr")]).await.remove(0);
    let read_by_trino = trino.batch(&[&columns("mixed_cl")]).await.remove(0);
    note(
        "stored",
        format!("trino {stored_by_trino:?}, read the engine's {read_by_trino:?}"),
    );
    if read_by_trino != stored_by_trino {
        failures.push(format!(
            "trino stored {stored_by_trino:?}, reads the engine's table as {read_by_trino:?}"
        ));
    }
    for (what, table) in [("stored", "mixed_cl"), ("read trino's", "mixed_tr")] {
        match try_cl(&engine, &columns(table)).await {
            Ok(rows) if rows == stored_by_trino => {}
            other => failures.push(format!("the engine {what} {other:?}, trino {stored_by_trino:?}")),
        }
    }
    let rows = |engine: &str| format!("SELECT CAST(x AS varchar) FROM hive.uts.mixed_{engine}");
    let theirs = sorted(trino.batch(&[&rows("tr")]).await.remove(0));
    match try_cl(&engine, &rows("cl")).await.map(sorted) {
        Ok(ours) if ours == theirs && !ours.is_empty() => {}
        other => failures.push(format!("rows {other:?}, trino {theirs:?}")),
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
