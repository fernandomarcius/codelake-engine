//! Reconciliation of a replica (Hive) against a SQL Server table larger than
//! the memory pool, as the nightly monitoring models write it: the remote
//! table filtered by `lower(cast(key AS varchar)) IN (keys of the replica)`,
//! each side deduplicated by `ROW_NUMBER`, then a `FULL OUTER JOIN` on the
//! lowered key. Rows checked against Trino 431 reading the same SQL Server
//! and the same Parquet files, the pool's peak measured by the pool itself.
//!
//! SQL Server reports no statistics: the size of the remote table is
//! unknown to the planner. A hash join holds its build side in memory, and
//! with one side's size unknown the build side is the one the query names
//! last (the subquery of `IN`, the right side of a join), never the remote
//! table the query reads first.

use std::time::{Duration, Instant};

use crate::common;
use crate::join_regressions::{
    Trino, assert_pool_given_back, note, open, physical_plan, shared, sorted, try_cl,
};
use crate::sqlserver_dynamic_filter::{SA_PASSWORD, exec, sqlserver, start_mssql};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// Rows of `dbo.recebidas` (~290 bytes each once read: over the pool).
const REMOTE_ROWS: u64 = 1_000_000;
const POOL: ByteSize = ByteSize::mib(256);

/// `rows` versions of `rows * 9 / 10` upper-case keys over two years, one
/// version in ten repeated, one in 53 deleted.
fn seed_sql(rows: u64) -> [String; 2] {
    let keys = rows * 9 / 10;
    [
        "CREATE TABLE dbo.recebidas (
           id BIGINT IDENTITY PRIMARY KEY,
           recebidasid VARCHAR(50) NOT NULL, obraid VARCHAR(10) NOT NULL,
           dataregistro DATETIME NOT NULL, seqval BINARY(10) NOT NULL, deleted BIT NOT NULL,
           status_rec INT NULL, valor_rec DECIMAL(18,2) NULL, vlmulta_rec DECIMAL(18,2) NULL,
           vljuros_rec DECIMAL(18,2) NULL, vldesconto_rec DECIMAL(18,2) NULL,
           vlacres_rec DECIMAL(18,2) NULL, vlcorrecao_rec DECIMAL(18,2) NULL,
           historico_rec VARCHAR(120) NULL);"
            .to_owned(),
        format!(
            "WITH n AS (SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                        FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
             INSERT INTO dbo.recebidas WITH (TABLOCK)
               (recebidasid, obraid, dataregistro, seqval, deleted, status_rec, valor_rec, vlmulta_rec,
                vljuros_rec, vldesconto_rec, vlacres_rec, vlcorrecao_rec, historico_rec)
             SELECT CONCAT('REC-', RIGHT(CONCAT('0000000000', i % {keys}), 10)),
                    CONCAT('OB', i % 700),
                    DATEADD(second, -CAST(({rows} - i) * (730 * 86400 / {rows}) AS INT), '2026-09-29T01:00:00'),
                    CAST(i AS BINARY(10)),
                    CASE WHEN i % 53 = 0 THEN 1 ELSE 0 END,
                    CAST(i % 4 AS INT),
                    CAST((i % 100000) / 100.0 AS DECIMAL(18,2)), CAST((i % 700) / 100.0 AS DECIMAL(18,2)),
                    CAST((i % 900) / 100.0 AS DECIMAL(18,2)), CAST((i % 300) / 100.0 AS DECIMAL(18,2)),
                    CAST((i % 200) / 100.0 AS DECIMAL(18,2)), CAST((i % 500) / 100.0 AS DECIMAL(18,2)),
                    REPLICATE(CONCAT('parcela ', i % 97, ' '), 8)
             FROM n;"
        ),
    ]
}

/// The replica: the versions of the last ~55 days (over 150 000 rows, so
/// the planner cannot hold it whole on one partition), keys in lower case,
/// some values and statuses changed, and keys SQL Server does not hold.
const PILOT: [&str; 3] = [
    "CREATE SCHEMA hive.pilot",
    "CREATE TABLE hive.pilot.recebidas AS
     SELECT lower(recebidasid) AS recebidasid, obraid, dataregistro, seqval AS __seqval,
            CAST(deleted AS integer) AS deleted,
            CASE WHEN id % 17 = 0 THEN status_rec + 1 ELSE status_rec END AS status_rec,
            valor_rec + CASE WHEN id % 11 = 0 THEN 1 ELSE 0 END AS valor_rec,
            vlmulta_rec, vljuros_rec, vldesconto_rec, vlacres_rec, vlcorrecao_rec, historico_rec
     FROM warehouse.dbo.recebidas WHERE dataregistro >= TIMESTAMP '2026-08-05 00:00:00'",
    "INSERT INTO hive.pilot.recebidas
     SELECT concat('rec-only-', CAST(n AS varchar)), 'OB1', TIMESTAMP '2026-09-28 00:00:00',
            CAST(to_utf8('x') AS varbinary), 0, 1, CAST(1 AS decimal(18,2)), CAST(0 AS decimal(18,2)),
            CAST(0 AS decimal(18,2)), CAST(0 AS decimal(18,2)), CAST(0 AS decimal(18,2)),
            CAST(0 AS decimal(18,2)), 'only in the replica'
     FROM UNNEST(sequence(1, 500)) t(n)",
];

/// One branch of the nightly `sensor_valor_status` model, as dbt compiled
/// it (its measurement timestamp aside), with its output as text.
const RECONCILE: &str = "
SELECT tabela, grupo, CAST(destino_faltando AS varchar), CAST(destino_sobrando AS varchar),
       CAST(valor_delta AS varchar), CAST(valor_linhas_diff AS varchar), CAST(status_diff AS varchar), status
FROM (
WITH pilot_dedup AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY recebidasid
                ORDER BY dataregistro DESC, __seqval DESC, deleted ASC
            ) AS rn
        FROM \"hive\".\"pilot\".\"recebidas\"
    ) z WHERE rn = 1 AND deleted = 0
),
subset_ids AS (SELECT DISTINCT lower(cast(recebidasid AS varchar)) AS k FROM \"hive\".\"pilot\".\"recebidas\"),
prod_dedup AS (
    SELECT * FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY recebidasid
                ORDER BY dataregistro DESC, seqval DESC, deleted ASC
            ) AS rn
        FROM warehouse.dbo.recebidas
        WHERE lower(cast(recebidasid AS varchar)) IN (SELECT k FROM subset_ids)
    ) z WHERE rn = 1 AND CAST(deleted AS integer) = 0
),
joined AS (
    SELECT
        COALESCE(p.obraid, q.obraid) AS grupo,
        SUM(CASE WHEN p.recebidasid IS NOT NULL AND q.recebidasid IS NULL     THEN 1 ELSE 0 END) AS destino_faltando,
        SUM(CASE WHEN p.recebidasid IS NULL     AND q.recebidasid IS NOT NULL THEN 1 ELSE 0 END) AS destino_sobrando,
        COALESCE(SUM(CASE WHEN p.recebidasid IS NOT NULL AND q.recebidasid IS NOT NULL
                          THEN CAST(q.valor_rec AS decimal(18,6)) - CAST(p.valor_rec AS decimal(18,6)) END), 0) AS valor_delta,
        SUM(CASE WHEN p.recebidasid IS NOT NULL AND q.recebidasid IS NOT NULL
                  AND (ROUND(CAST(p.valor_rec AS decimal(18,6)),2) <> ROUND(CAST(q.valor_rec AS decimal(18,6)),2) OR ROUND(CAST(p.vlmulta_rec AS decimal(18,6)),2) <> ROUND(CAST(q.vlmulta_rec AS decimal(18,6)),2) OR ROUND(CAST(p.vljuros_rec AS decimal(18,6)),2) <> ROUND(CAST(q.vljuros_rec AS decimal(18,6)),2) OR ROUND(CAST(p.vldesconto_rec AS decimal(18,6)),2) <> ROUND(CAST(q.vldesconto_rec AS decimal(18,6)),2) OR ROUND(CAST(p.vlacres_rec AS decimal(18,6)),2) <> ROUND(CAST(q.vlacres_rec AS decimal(18,6)),2) OR ROUND(CAST(p.vlcorrecao_rec AS decimal(18,6)),2) <> ROUND(CAST(q.vlcorrecao_rec AS decimal(18,6)),2))
                 THEN 1 ELSE 0 END) AS valor_linhas_diff,
        SUM(CASE WHEN p.recebidasid IS NOT NULL AND q.recebidasid IS NOT NULL
                  AND p.status_rec IS DISTINCT FROM q.status_rec THEN 1 ELSE 0 END) AS status_diff,
        CAST(0 AS bigint) AS cancel_orig_ativo_dest,
        CAST(0 AS bigint) AS cancel_dest_ativo_orig
    FROM pilot_dedup p
    FULL OUTER JOIN prod_dedup q ON lower(cast(p.recebidasid AS varchar)) = lower(cast(q.recebidasid AS varchar))
    GROUP BY COALESCE(p.obraid, q.obraid)
)
SELECT
    'recebidas' AS tabela,
    grupo,
    destino_faltando, destino_sobrando,
    valor_delta, valor_linhas_diff,
    status_diff, cancel_orig_ativo_dest, cancel_dest_ativo_orig,
    CASE
        WHEN destino_faltando > 0 THEN 'CRITICO'
        WHEN cancel_orig_ativo_dest > 0 OR cancel_dest_ativo_orig > 0 THEN 'CRITICO'
        WHEN destino_sobrando > 0 OR valor_linhas_diff > 0 OR status_diff > 0 THEN 'ALERTA'
        ELSE 'OK'
    END AS status
FROM joined
) t";

/// The hash joins of `plan` that read the remote table on their build side
/// (the first input): each line from the join to its first input's leaf.
fn remote_build_sides(plan: &str) -> Vec<String> {
    let lines: Vec<&str> = plan.lines().collect();
    let depth = |line: &str| line.len() - line.trim_start().len();
    let mut found = Vec::new();
    for (at, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("HashJoinExec") {
            continue;
        }
        // The build side is the join's first child: every line below the
        // join, deeper than it, up to the second child (the next line at the
        // first child's depth).
        let Some(first) = lines.get(at + 1) else { continue };
        let child = depth(first);
        let build: Vec<&str> = lines[at + 1..]
            .iter()
            .enumerate()
            .take_while(|(i, l)| depth(l) > depth(line) && (*i == 0 || depth(l) != child))
            .map(|(_, l)| *l)
            .collect();
        if build
            .iter()
            .any(|l| l.contains("[recebidas]") || l.contains("dbo.recebidas"))
        {
            found.push(format!("{line}\n{}", build.join("\n")));
        }
    }
    found
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one scenario, read top to bottom")]
async fn a_remote_table_larger_than_the_pool_is_never_the_build_side() {
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE warehouse").await;
    for statement in seed_sql(REMOTE_ROWS) {
        exec(&host, port, "warehouse", &statement).await;
    }
    let shared = shared().await;
    let catalog = format!(
        "connector.name=sqlserver\n\
         connection-url=jdbc:sqlserver://{host}:{port};database=warehouse;encrypt=false\n\
         connection-user=sa\n\
         connection-password={SA_PASSWORD}\n"
    );
    let trino = Trino::start_with(&shared, "build-side", &[("warehouse", catalog)]).await;
    trino.batch(&PILOT).await;
    let reference = sorted(trino.batch(&[RECONCILE]).await.remove(0));
    note("trino", format!("{} groups", reference.len()));

    let spill = tempfile::tempdir().unwrap();
    let execution = ExecutionConfig {
        target_partitions: Some(6),
        ..ExecutionConfig::default()
    };
    let mut engine = open(&shared.warehouse(), spill.path(), POOL, &execution, &[]).await;
    common::attach(&mut engine, sqlserver(&host, port)).await;
    let mut failures = Vec::new();

    let plan = physical_plan(&engine, RECONCILE).await;
    note("plan", &plan);
    if !plan.contains("HashJoinExec") {
        failures.push(format!("vacuous, no hash join:\n{plan}"));
    }
    let remote = remote_build_sides(&plan);
    if !remote.is_empty() {
        failures.push(format!(
            "the remote table is the build side of {} hash join(s):\n{}",
            remote.len(),
            remote.join("\n---\n")
        ));
    }

    engine.reset_memory_peak();
    let rows = try_cl(&engine, RECONCILE).await.map(sorted);
    let peak = engine.memory_peak();
    note(
        "reconcile",
        format!(
            "pool peak {peak} B (limit {} B), trino={} rows, codelake={:?}",
            POOL.as_u64(),
            reference.len(),
            rows.as_ref().map(Vec::len)
        ),
    );
    match rows {
        Ok(rows) if rows == reference && !rows.is_empty() => {}
        Ok(rows) => failures.push(format!("trino={reference:?}\ncodelake={rows:?}")),
        Err(error) => failures.push(format!("codelake failed: {error}")),
    }
    if peak > POOL.as_u64() {
        failures.push(format!("the pool held {peak} B, over its {POOL:?}"));
    }
    assert_pool_given_back(&engine).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Hand-run benchmark settings: `CODELAKE_BENCH_ROWS` rows in SQL Server
/// (15 000 000, the production table), a pool of `CODELAKE_BENCH_POOL_MIB`
/// (4096, the shadow run's), the replica holding the versions since
/// `CODELAKE_BENCH_SINCE` (the last five days), the model a `UNION ALL` of
/// `CODELAKE_BENCH_BRANCHES` reconciliations (2; the nightly model has 7,
/// one per table).
fn bench_setting(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::test]
#[ignore = "benchmark: CODELAKE_BENCH_ROWS rows in SQL Server, run by hand"]
#[allow(clippy::print_stdout, reason = "the measurement is the output")]
async fn reconciliation_peak_benchmark() {
    let rows: u64 = bench_setting("CODELAKE_BENCH_ROWS", "15000000").parse().unwrap();
    let pool = ByteSize::mib(bench_setting("CODELAKE_BENCH_POOL_MIB", "4096").parse().unwrap());
    let since = bench_setting("CODELAKE_BENCH_SINCE", "2026-09-24 00:00:00");
    let branches: usize = bench_setting("CODELAKE_BENCH_BRANCHES", "2").parse().unwrap();
    let model = vec![RECONCILE; branches].join(" UNION ALL ");
    // SQL Server takes most of the machine's memory unless told otherwise.
    let mssql = MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_env_var("MSSQL_MEMORY_LIMIT_MB", "3072")
        .with_tag("2022-latest")
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .unwrap();
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    let started = Instant::now();
    exec(&host, port, "master", "CREATE DATABASE warehouse").await;
    for statement in seed_sql(rows) {
        exec(&host, port, "warehouse", &statement).await;
    }
    println!("seeded {rows} rows in {:?}", started.elapsed());
    let shared = shared().await;
    let spill = tempfile::tempdir().unwrap();
    let execution = ExecutionConfig {
        target_partitions: Some(6),
        ..ExecutionConfig::default()
    };
    let mut engine = open(&shared.warehouse(), spill.path(), pool, &execution, &[]).await;
    common::attach(&mut engine, sqlserver(&host, port)).await;
    let pilot = PILOT[1].replace("TIMESTAMP '2026-08-05 00:00:00'", &format!("TIMESTAMP '{since}'"));
    for statement in [PILOT[0], pilot.as_str(), PILOT[2]] {
        try_cl(&engine, statement).await.unwrap();
    }
    let counts = try_cl(
        &engine,
        "SELECT count(*), count(DISTINCT recebidasid) FROM hive.pilot.recebidas",
    )
    .await
    .unwrap();
    println!("replica rows, keys: {counts:?}");
    // Without join reordering the plan is the one before the build side
    // rule: `JoinSelection` swaps nothing either.
    let mut query_order = open(
        &shared.warehouse(),
        spill.path(),
        pool,
        &execution,
        &[("datafusion.optimizer.join_reordering", "false")],
    )
    .await;
    common::attach(&mut query_order, sqlserver(&host, port)).await;
    for (name, engine) in [("join_reordering = false", &query_order), ("default", &engine)] {
        let plan = physical_plan(engine, &model).await;
        let joins: Vec<&str> = plan.lines().filter(|l| l.contains("HashJoinExec")).collect();
        engine.reset_memory_peak();
        let started = Instant::now();
        let answer = try_cl(engine, &model).await;
        println!(
            "===== {name} =====\n{}\npool {} MiB, peak {} MiB, {:?}: {}",
            joins.join("\n"),
            pool.as_u64() >> 20,
            engine.memory_peak() >> 20,
            started.elapsed(),
            match &answer {
                Ok(rows) => format!("{} rows", rows.len()),
                Err(error) => error.chars().take(900).collect(),
            }
        );
        assert_pool_given_back(engine).await;
    }
}
