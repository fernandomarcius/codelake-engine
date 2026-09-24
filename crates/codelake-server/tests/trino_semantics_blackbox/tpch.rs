//! TPC-H Q01, Q08, Q14 and Q17 over `tpch.tiny` (SF 0.01) with money as
//! `decimal(12,2)`, stored as Parquet in `hive` on both sides: Trino copies
//! its `tpch` connector with CTAS, `CodeLake` receives the same rows through
//! `INSERT ... VALUES`.

use serde_json::Value;

use super::{Pair, Res, expect_rows, run, verdict};

/// A column: name, type, and the Trino expression over `tpch.tiny`
/// producing it.
type Column = (&'static str, &'static str, &'static str);

/// Every table with its columns.
const TABLES: &[(&str, &[Column])] = &[
    (
        "region",
        &[
            ("r_regionkey", "bigint", "regionkey"),
            ("r_name", "varchar", "CAST(name AS varchar)"),
        ],
    ),
    (
        "nation",
        &[
            ("n_nationkey", "bigint", "nationkey"),
            ("n_name", "varchar", "CAST(name AS varchar)"),
            ("n_regionkey", "bigint", "regionkey"),
        ],
    ),
    (
        "supplier",
        &[
            ("s_suppkey", "bigint", "suppkey"),
            ("s_nationkey", "bigint", "nationkey"),
            ("s_acctbal", "decimal(12,2)", "CAST(acctbal AS decimal(12,2))"),
        ],
    ),
    (
        "customer",
        &[
            ("c_custkey", "bigint", "custkey"),
            ("c_nationkey", "bigint", "nationkey"),
        ],
    ),
    (
        "part",
        &[
            ("p_partkey", "bigint", "partkey"),
            ("p_brand", "varchar", "CAST(brand AS varchar)"),
            ("p_type", "varchar", "CAST(type AS varchar)"),
            ("p_container", "varchar", "CAST(container AS varchar)"),
        ],
    ),
    (
        "orders",
        &[
            ("o_orderkey", "bigint", "orderkey"),
            ("o_custkey", "bigint", "custkey"),
            ("o_orderdate", "date", "orderdate"),
        ],
    ),
    (
        "lineitem",
        &[
            ("l_orderkey", "bigint", "orderkey"),
            ("l_partkey", "bigint", "partkey"),
            ("l_suppkey", "bigint", "suppkey"),
            ("l_quantity", "decimal(12,2)", "CAST(quantity AS decimal(12,2))"),
            (
                "l_extendedprice",
                "decimal(12,2)",
                "CAST(extendedprice AS decimal(12,2))",
            ),
            ("l_discount", "decimal(12,2)", "CAST(discount AS decimal(12,2))"),
            ("l_tax", "decimal(12,2)", "CAST(tax AS decimal(12,2))"),
            ("l_returnflag", "varchar", "CAST(returnflag AS varchar)"),
            ("l_linestatus", "varchar", "CAST(linestatus AS varchar)"),
            ("l_shipdate", "date", "shipdate"),
        ],
    ),
];

const ROWS_PER_INSERT: usize = 5000;

fn ok(answer: &Value, what: &str) -> Res<()> {
    match answer.get("error") {
        Some(_) => Err(format!("{what}: {answer}")),
        None => Ok(()),
    }
}

/// A SQL literal of a Trino JSON cell for a column of `sql_type`.
fn literal(cell: &Value, sql_type: &str) -> Res<String> {
    Ok(match cell {
        Value::Null => "NULL".to_owned(),
        Value::Number(n) => n.to_string(),
        Value::String(s) if sql_type == "date" => format!("DATE '{s}'"),
        Value::String(s) if sql_type.starts_with("decimal") => s.clone(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => return Err(format!("unexpected cell {other}")),
    })
}

/// Loads `tpch.tiny` into `hive.bx_tpch` on both engines, then checks row
/// counts and money totals.
async fn load(pair: &Pair) -> Res<()> {
    pair.setup(&["CREATE SCHEMA hive.bx_tpch"]).await?;
    let codelake = pair.codelake.base();
    for (table, columns) in TABLES {
        let select: Vec<String> = columns
            .iter()
            .map(|(name, _, expr)| format!("{expr} AS {name}"))
            .collect();
        let ctas = format!(
            "CREATE TABLE hive.bx_tpch.{table} WITH (format = 'PARQUET') AS SELECT {} FROM tpch.tiny.{table}",
            select.join(", ")
        );
        ok(&run(&pair.trino, "hive", &ctas, None).await?, &ctas)?;
        let ddl: Vec<String> = columns
            .iter()
            .map(|(name, ty, _)| format!("{name} {ty}"))
            .collect();
        let ddl = format!(
            "CREATE TABLE hive.bx_tpch.{table} ({}) WITH (format = 'PARQUET')",
            ddl.join(", ")
        );
        ok(&run(&codelake, "hive", &ddl, None).await?, &ddl)?;
        let rows = run(
            &pair.trino,
            "hive",
            &format!("SELECT * FROM hive.bx_tpch.{table}"),
            None,
        )
        .await?;
        ok(&rows, table)?;
        let rows = rows["rows"].as_array().cloned().unwrap_or_default();
        for chunk in rows.chunks(ROWS_PER_INSERT) {
            let mut tuples = Vec::with_capacity(chunk.len());
            for row in chunk {
                let cells = row.as_array().cloned().unwrap_or_default();
                let literals: Res<Vec<String>> = cells
                    .iter()
                    .zip(columns.iter())
                    .map(|(cell, (_, ty, _))| literal(cell, ty))
                    .collect();
                tuples.push(format!("({})", literals?.join(", ")));
            }
            let insert = format!("INSERT INTO hive.bx_tpch.{table} VALUES {}", tuples.join(", "));
            ok(
                &run(&codelake, "hive", &insert, None).await?,
                &format!("INSERT INTO {table}"),
            )?;
        }
    }
    let check = "SELECT (SELECT count(*) FROM hive.bx_tpch.lineitem), (SELECT sum(l_extendedprice) FROM hive.bx_tpch.lineitem), (SELECT sum(l_quantity) FROM hive.bx_tpch.lineitem), (SELECT count(*) FROM hive.bx_tpch.orders), (SELECT count(*) FROM hive.bx_tpch.part), (SELECT count(*) FROM hive.bx_tpch.customer), (SELECT sum(s_acctbal) FROM hive.bx_tpch.supplier), (SELECT count(*) FROM hive.bx_tpch.nation), (SELECT count(*) FROM hive.bx_tpch.region)";
    verdict(&pair.rows(&[check], None).await?)
}

const Q01: &str = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM hive.bx_tpch.lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";

const Q08: &str = "SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM (SELECT extract(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM hive.bx_tpch.part, hive.bx_tpch.supplier, hive.bx_tpch.lineitem, hive.bx_tpch.orders, hive.bx_tpch.customer, hive.bx_tpch.nation n1, hive.bx_tpch.nation n2, hive.bx_tpch.region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations GROUP BY o_year ORDER BY o_year";

/// Q08 over every part type and region, so every year and nation has
/// volume and the shares are not all zero.
const Q08_ALL: &str = "SELECT o_year, nation, sum(volume) AS vol, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS br_share FROM (SELECT extract(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM hive.bx_tpch.part, hive.bx_tpch.supplier, hive.bx_tpch.lineitem, hive.bx_tpch.orders, hive.bx_tpch.customer, hive.bx_tpch.nation n1, hive.bx_tpch.nation n2, hive.bx_tpch.region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31') AS all_nations GROUP BY o_year, nation ORDER BY o_year, nation";

const Q14: &str = "SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM hive.bx_tpch.lineitem, hive.bx_tpch.part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH";

/// Q14 per month over the whole history.
const Q14_MONTHS: &str = "SELECT extract(YEAR FROM l_shipdate) * 100 + extract(MONTH FROM l_shipdate) AS m, 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM hive.bx_tpch.lineitem, hive.bx_tpch.part WHERE l_partkey = p_partkey GROUP BY extract(YEAR FROM l_shipdate) * 100 + extract(MONTH FROM l_shipdate) ORDER BY m";

const Q17: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM hive.bx_tpch.lineitem, hive.bx_tpch.part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM hive.bx_tpch.lineitem WHERE l_partkey = p_partkey)";

/// Q17 per brand and container, so the subquery threshold filters rows.
const Q17_ALL: &str = "SELECT p_brand, p_container, sum(l_extendedprice) / 7.0 AS avg_yearly FROM hive.bx_tpch.lineitem, hive.bx_tpch.part WHERE p_partkey = l_partkey AND p_container LIKE 'MED%' AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM hive.bx_tpch.lineitem WHERE l_partkey = p_partkey) GROUP BY p_brand, p_container ORDER BY p_brand, p_container";

#[tokio::test]
async fn tpch_on_hive_parquet_matches_trino() -> Res<()> {
    let pair = Pair::start().await?;
    load(&pair).await?;
    verdict(
        &pair
            .rows(&[Q01, Q08, Q08_ALL, Q14, Q14_MONTHS, Q17, Q17_ALL], None)
            .await?,
    )
}

// ---------------------------------------------------------------- defects

#[tokio::test]
async fn defect_sem39_date_trunc_of_a_date_is_a_date() -> Res<()> {
    expect_rows(
        &[],
        &["SELECT date_trunc('month', DATE '1995-09-17'), date_trunc('year', DATE '1995-09-17'), date_trunc('day', DATE '1969-12-31'), date_trunc('week', DATE '2026-01-01')"],
        None,
    )
    .await
}
