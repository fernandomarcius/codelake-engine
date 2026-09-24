//! TPC-H Q01, Q08, Q14 and Q17 on SF 0.01 (`tpch.tiny`). Trino copies its
//! `tpch` connector into the `memory` catalog with the TPC-H decimal types;
//! the same rows go into `CodeLake` through `INSERT ... VALUES`, so both
//! engines answer over identical data.

use serde_json::Value;

use super::{Pair, Res, run, verdict};

/// Table, `CodeLake` column list, and the Trino select over `tpch.tiny`
/// producing those columns (money as `decimal(15,2)`, text as `varchar`).
const TABLES: &[(&str, &str, &str)] = &[
    (
        "region",
        "r_regionkey bigint, r_name varchar",
        "regionkey, CAST(name AS varchar)",
    ),
    (
        "nation",
        "n_nationkey bigint, n_name varchar, n_regionkey bigint",
        "nationkey, CAST(name AS varchar), regionkey",
    ),
    (
        "supplier",
        "s_suppkey bigint, s_nationkey bigint",
        "suppkey, nationkey",
    ),
    (
        "customer",
        "c_custkey bigint, c_nationkey bigint",
        "custkey, nationkey",
    ),
    (
        "part",
        "p_partkey bigint, p_brand varchar, p_type varchar, p_container varchar",
        "partkey, CAST(brand AS varchar), CAST(type AS varchar), CAST(container AS varchar)",
    ),
    (
        "orders",
        "o_orderkey bigint, o_custkey bigint, o_orderdate date",
        "orderkey, custkey, orderdate",
    ),
    (
        "lineitem",
        "l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_quantity decimal(15,2), l_extendedprice decimal(15,2), l_discount decimal(15,2), l_tax decimal(15,2), l_returnflag varchar, l_linestatus varchar, l_shipdate date",
        "orderkey, partkey, suppkey, CAST(quantity AS decimal(15,2)), CAST(extendedprice AS decimal(15,2)), CAST(discount AS decimal(15,2)), CAST(tax AS decimal(15,2)), CAST(returnflag AS varchar), CAST(linestatus AS varchar), shipdate",
    ),
];

const ROWS_PER_INSERT: usize = 5000;

/// Splits a select list at top-level commas.
fn items(list: &str) -> Vec<String> {
    let (mut out, mut depth, mut current) = (Vec::new(), 0_i32, String::new());
    for ch in list.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if ch == ',' && depth == 0 {
            out.push(current.trim().to_owned());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    out.push(current.trim().to_owned());
    out
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

/// Loads `tpch.tiny` into `{c}.sem_tpch` on both engines and checks that
/// both hold the same row counts and money totals.
async fn load(pair: &Pair) -> Res<()> {
    pair.setup(&["CREATE SCHEMA {c}.sem_tpch"]).await?;
    let codelake = pair.codelake.base();
    for (table, columns, select) in TABLES {
        let columns: Vec<String> = items(columns);
        let aliased: Vec<String> = items(select)
            .iter()
            .zip(&columns)
            .map(|(expr, column)| {
                let name = column.split_whitespace().next().unwrap_or_default();
                format!("{expr} AS {name}")
            })
            .collect();
        let ctas = format!(
            "CREATE TABLE memory.sem_tpch.{table} AS SELECT {} FROM tpch.tiny.{table}",
            aliased.join(", ")
        );
        ok(&run(&pair.trino, "memory", &ctas, None).await?, &ctas)?;
        let ddl = format!(
            "CREATE TABLE datafusion.sem_tpch.{table} ({})",
            columns.join(", ")
        );
        ok(&run(&codelake, "datafusion", &ddl, None).await?, &ddl)?;
        let rows = run(
            &pair.trino,
            "memory",
            &format!("SELECT * FROM memory.sem_tpch.{table}"),
            None,
        )
        .await?;
        let rows = rows["rows"].as_array().cloned().unwrap_or_default();
        let types: Vec<&str> = columns
            .iter()
            .map(|c| c.split_once(' ').map_or("", |(_, t)| t))
            .collect();
        for chunk in rows.chunks(ROWS_PER_INSERT) {
            let mut tuples = Vec::with_capacity(chunk.len());
            for row in chunk {
                let cells = row.as_array().cloned().unwrap_or_default();
                let literals: Res<Vec<String>> = cells
                    .iter()
                    .zip(&types)
                    .map(|(cell, t)| literal(cell, t))
                    .collect();
                tuples.push(format!("({})", literals?.join(", ")));
            }
            let insert = format!(
                "INSERT INTO datafusion.sem_tpch.{table} VALUES {}",
                tuples.join(", ")
            );
            ok(
                &run(&codelake, "datafusion", &insert, None).await?,
                &format!("INSERT INTO {table}"),
            )?;
        }
    }
    let check = "SELECT (SELECT count(*) FROM {c}.sem_tpch.lineitem), (SELECT sum(l_extendedprice) FROM {c}.sem_tpch.lineitem), (SELECT count(*) FROM {c}.sem_tpch.orders), (SELECT count(*) FROM {c}.sem_tpch.part), (SELECT count(*) FROM {c}.sem_tpch.customer), (SELECT count(*) FROM {c}.sem_tpch.supplier), (SELECT count(*) FROM {c}.sem_tpch.nation), (SELECT count(*) FROM {c}.sem_tpch.region)";
    verdict(&pair.mismatch(check, None).await?.into_iter().collect::<Vec<_>>())
}

fn ok(answer: &Value, what: &str) -> Res<()> {
    match answer.get("error") {
        Some(_) => Err(format!("{what}: {answer}")),
        None => Ok(()),
    }
}

const Q01: &str = "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM {c}.sem_tpch.lineitem WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";

const Q08: &str = "SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM (SELECT extract(YEAR FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM {c}.sem_tpch.part, {c}.sem_tpch.supplier, {c}.sem_tpch.lineitem, {c}.sem_tpch.orders, {c}.sem_tpch.customer, {c}.sem_tpch.nation n1, {c}.sem_tpch.nation n2, {c}.sem_tpch.region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations GROUP BY o_year ORDER BY o_year";

/// Q08 with the year cast to `bigint` (see DEFECT-SEM18) and nations that
/// have volume at SF 0.01, so the shares are not all zero.
const Q08_SHARES: &str = "SELECT o_year, sum(CASE WHEN nation = 'UNITED STATES' THEN volume ELSE 0 END) / sum(volume) AS us_share, sum(CASE WHEN nation = 'CANADA' THEN volume ELSE 0 END) / sum(volume) AS ca_share, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM (SELECT CAST(extract(YEAR FROM o_orderdate) AS bigint) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation FROM {c}.sem_tpch.part, {c}.sem_tpch.supplier, {c}.sem_tpch.lineitem, {c}.sem_tpch.orders, {c}.sem_tpch.customer, {c}.sem_tpch.nation n1, {c}.sem_tpch.nation n2, {c}.sem_tpch.region WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA' AND s_nationkey = n2.n_nationkey AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type LIKE 'ECONOMY%') AS all_nations GROUP BY o_year ORDER BY o_year";

const Q14: &str = "SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM {c}.sem_tpch.lineitem, {c}.sem_tpch.part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH";

/// Q17 as written (`Brand#23`, `MED BOX`: no rows at SF 0.01, the answer is
/// NULL) and with `Brand#11`, which has parts in `MED BOX`.
const Q17: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM {c}.sem_tpch.lineitem, {c}.sem_tpch.part WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM {c}.sem_tpch.lineitem WHERE l_partkey = p_partkey)";
const Q17_BRAND11: &str = "SELECT sum(l_extendedprice) / 7.0 AS avg_yearly FROM {c}.sem_tpch.lineitem, {c}.sem_tpch.part WHERE p_partkey = l_partkey AND p_brand = 'Brand#11' AND p_container = 'MED BOX' AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM {c}.sem_tpch.lineitem WHERE l_partkey = p_partkey)";

#[tokio::test]
async fn tpch_q01_q08_q14_match_trino() -> Res<()> {
    let pair = Pair::start().await?;
    load(&pair).await?;
    verdict(&pair.mismatches(&[Q01, Q08_SHARES, Q14], None).await?)
}

// ---------------------------------------------------------------- defects

#[tokio::test]
async fn defect_sem2_tpch_q17() -> Res<()> {
    let pair = Pair::start().await?;
    load(&pair).await?;
    verdict(&pair.mismatches(&[Q17, Q17_BRAND11], None).await?)
}

#[tokio::test]
async fn defect_sem18_tpch_q08_extract_is_bigint() -> Res<()> {
    let pair = Pair::start().await?;
    load(&pair).await?;
    verdict(&pair.mismatches(&[Q08], None).await?)
}
