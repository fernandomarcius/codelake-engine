//! The `Warehouse` screens' views over a SQL Server table larger than the
//! memory pool, as the shadow run read them on 2026-10-02 (`CodeLake` 3997959
//! against Trino 431, same sources): Hive views written by Trino, each a
//! remote table of millions of rows joined with small tables deduplicated
//! by `ROW_NUMBER` (remote, or their Hive replicas), one of them a
//! `UNION ALL` of two remote reads under a `NOT EXISTS`, and an aggregation
//! of 17 keys over one of them.
//!
//! In the shadow run, `SELECT * FROM <view> LIMIT 50` failed after ~51 s
//! with six `HashJoinInput` partitions of 450-680 MB under a 4 GiB pool
//! (Trino: 4.2 s), the aggregation ordered by every column failed with
//! `GroupedHashAggregateStream` refused while the hash joins held the pool,
//! and the `LIMIT 50` that did answer took 30.7 s against Trino's 4.2 s:
//! a view's plan is optimized twice and the second pass swapped the hash
//! joins back to building the remote table (`codelake_core::build_side`).
//!
//! Here, at 1/6 of the scale under 1/16 of the pool (256 MiB, 6
//! partitions): no hash join builds the remote table, every view answers
//! Trino's rows (aggregated fingerprints of the whole view, the ordered
//! aggregation row by row), the `LIMIT 50` reads are a fraction of a whole
//! read, and the pool never passes its limit.

use std::time::{Duration, Instant};

use crate::common;
use crate::join_regressions::{
    Trino, assert_pool_given_back, note, open, physical_plan, shared, sorted, try_cl,
};
use crate::sqlserver_dynamic_filter::{SA_PASSWORD, exec, sqlserver, start_mssql};
use codelake_core::ByteSize;
use codelake_core::config::ExecutionConfig;

/// Rows of the remote table (~330 bytes each once read).
const REMOTE_ROWS: u64 = 1_000_000;
const POOL: ByteSize = ByteSize::mib(256);
const OBRAS: u64 = 700;

/// The remote table and the small versioned ones, as the `Warehouse` database
/// and the ERP view hold them: every obra in two operations, each version
/// of a row repeated with an older `dataregistro`, one row in 13 deleted.
#[allow(clippy::too_many_lines, reason = "the seed, one statement per table")]
fn seed_sql() -> Vec<String> {
    let rows = REMOTE_ROWS;
    vec![
        "CREATE TABLE dbo.vd_recebidas (
           id BIGINT IDENTITY PRIMARY KEY,
           obraid VARCHAR(44) NOT NULL, nomecliente VARCHAR(150) NULL, identificador_unid VARCHAR(30) NULL,
           numero_venda INT NULL, numnovavend INT NULL, data_rec DATETIME NULL, data_vencimento DATETIME NULL,
           empresaid VARCHAR(23) NULL, obra_rec VARCHAR(5) NULL, totconf DECIMAL(28,6) NULL,
           totparcel DECIMAL(34,6) NULL, valortot_vrec DECIMAL(18,6) NULL, datacessao_vrec DATETIME NULL,
           datacancel_vrec DATETIME NULL, bancoid VARCHAR(23) NULL, conta_corrente VARCHAR(20) NULL,
           descricaoparcela VARCHAR(15) NULL, numparcger_rec INT NULL, numparcela INT NULL,
           codobra VARCHAR(5) NULL, nomeobra VARCHAR(255) NULL, vendaid VARCHAR(65) NULL,
           status_venda VARCHAR(9) NULL, tipoparcela VARCHAR(19) NULL);"
            .to_owned(),
        format!(
            "WITH n AS (SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                        FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
             INSERT INTO dbo.vd_recebidas WITH (TABLOCK)
               (obraid, nomecliente, identificador_unid, numero_venda, numnovavend, data_rec, data_vencimento,
                empresaid, obra_rec, totconf, totparcel, valortot_vrec, datacessao_vrec, datacancel_vrec,
                bancoid, conta_corrente, descricaoparcela, numparcger_rec, numparcela, codobra, nomeobra,
                vendaid, status_venda, tipoparcela)
             SELECT CONCAT('OBRA-', i % {OBRAS}), CONCAT('Customer ', i % 50000, ' of the Example family'),
                    CONCAT('UN-', i % 9000), CAST(i % 200000 AS INT),
                    CASE WHEN i % 7 = 0 THEN CAST(i % 1000 AS INT) END,
                    DATEADD(minute, CAST(i % 500000 AS INT), '2025-01-01'),
                    DATEADD(day, CAST(i % 900 AS INT), '2024-06-01'),
                    CONCAT('EMP-', i % 30), CONCAT('R', i % 9000 % 1000), CAST((i % 77777) / 100.0 AS DECIMAL(28,6)),
                    CAST((i % 99991) / 10.0 AS DECIMAL(34,6)), CAST((i % 500000) + 1000 AS DECIMAL(18,6)),
                    CASE WHEN i % 11 = 0 THEN DATEADD(day, CAST(i % 300 AS INT), '2025-02-01') END,
                    CASE WHEN i % 17 = 0 THEN DATEADD(day, CAST(i % 300 AS INT), '2025-03-01') END,
                    CONCAT('BANCO-', i % 4), CONCAT('CC-', i % 3), CONCAT('PARC ', i % 12),
                    CAST(i % 360 AS INT), CAST(i % 120 AS INT), CONCAT('C', i % {OBRAS}),
                    CONCAT('Empreendimento ', i % {OBRAS}, ' Tower'), CONCAT('VENDA-', i % 200000),
                    CASE WHEN i % 9 = 0 THEN 'Cancelada' ELSE 'Ativa' END,
                    CASE i % 4 WHEN 0 THEN 'Parcela' WHEN 1 THEN 'Deposit' WHEN 2 THEN 'Chaves' ELSE 'Intermediaria' END
             FROM n;"
        ),
        "CREATE TABLE dbo.operacaoobra (operacaoobraid INT, obraid VARCHAR(44), operacaoid INT,
           percentualcomissao DECIMAL(5,2), percentualoperacao DECIMAL(18,4), dataregistro DATETIME, deleted BIT);
         CREATE TABLE dbo.obras (obraid VARCHAR(44), descr_obr VARCHAR(255), dataregistro DATETIME, deleted BIT);
         CREATE TABLE dbo.bancooperacaoobra (bancooperacaoobraid INT, operacaoobraid INT, bancoid VARCHAR(23),
           numconta VARCHAR(20), bancotipoid INT, dataregistro DATETIME, deleted BIT);
         CREATE TABLE dbo.operacao (operacaoid INT, numero VARCHAR(255), estruturador VARCHAR(50),
           apelidoop VARCHAR(50), statusoperacao VARCHAR(50), dataregistro DATETIME, deleted BIT);
         CREATE TABLE dbo.depara (deparaid INT, operacaoobraid INT, de VARCHAR(50), para VARCHAR(50),
           tipo VARCHAR(20), dataregistro DATETIME, deleted BIT);
         CREATE TABLE dbo.produtosgeraisparametros (identificador VARCHAR(30), obraid VARCHAR(44),
           operacaoid INT, dataregistro DATETIME, deleted BIT);"
            .to_owned(),
        // Two versions of everything: the newer one wins (`rn = 1`).
        format!(
            "WITH v AS (SELECT 0 AS age UNION ALL SELECT 1),
                  o AS (SELECT TOP ({OBRAS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects),
                  p AS (SELECT 1 AS j UNION ALL SELECT 2)
             INSERT INTO dbo.operacaoobra
             SELECT CAST(o.i * 2 + p.j AS INT), CONCAT('OBRA-', o.i), CAST((o.i % 40) * 2 + p.j AS INT),
                    CAST(1.5 + (o.i % 5) + v.age AS DECIMAL(5,2)), CAST(50 + (o.i % 30) + v.age AS DECIMAL(18,4)),
                    DATEADD(day, -v.age, '2026-09-01'), CASE WHEN o.i % 13 = 0 AND v.age = 0 THEN 1 ELSE 0 END
             FROM o CROSS JOIN p CROSS JOIN v;"
        ),
        format!(
            "WITH v AS (SELECT 0 AS age UNION ALL SELECT 1),
                  o AS (SELECT TOP ({OBRAS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects),
                  p AS (SELECT 1 AS j UNION ALL SELECT 2)
             INSERT INTO dbo.obras
             SELECT CONCAT('OBRA-', o.i), CONCAT('Obra ', o.i, CASE WHEN v.age = 1 THEN ' (antiga)' ELSE '' END),
                    DATEADD(day, -v.age, '2026-09-01'), 0
             FROM o CROSS JOIN v;"
        ),
        format!(
            "WITH v AS (SELECT 0 AS age UNION ALL SELECT 1),
                  o AS (SELECT TOP ({OBRAS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects),
                  p AS (SELECT 1 AS j UNION ALL SELECT 2)
             INSERT INTO dbo.bancooperacaoobra
             SELECT CAST((o.i * 2 + p.j) * 16 + b.k * 4 + c.k AS INT), CAST(o.i * 2 + p.j AS INT),
                    CONCAT('BANCO-', b.k), CONCAT('CC-', c.k), CASE WHEN (o.i + b.k + c.k) % 3 = 0 THEN 3 ELSE 2 END,
                    DATEADD(day, -v.age, '2026-09-01'), 0
             FROM o CROSS JOIN p CROSS JOIN v
                  CROSS JOIN (SELECT 0 AS k UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) b
                  CROSS JOIN (SELECT 0 AS k UNION ALL SELECT 1 UNION ALL SELECT 2) c
             WHERE (o.i + b.k) % 2 = 0;"
        ),
        format!(
            "WITH v AS (SELECT 0 AS age UNION ALL SELECT 1),
                  o AS (SELECT TOP ({OBRAS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects),
                  p AS (SELECT 1 AS j UNION ALL SELECT 2)
             INSERT INTO dbo.operacao
             SELECT CAST(o.i AS INT), CONCAT('OP-', o.i), CONCAT('Estruturador ', o.i % 7), CONCAT('Apelido ', o.i),
                    CASE WHEN o.i % 5 = 0 THEN 'Ativa' ELSE 'Liquidada' END, DATEADD(day, -v.age, '2026-09-01'), 0
             FROM o CROSS JOIN v WHERE o.i <= 81;"
        ),
        format!(
            "WITH v AS (SELECT 0 AS age UNION ALL SELECT 1),
                  o AS (SELECT TOP ({OBRAS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects),
                  p AS (SELECT 1 AS j UNION ALL SELECT 2)
             INSERT INTO dbo.depara
             SELECT CAST(o.i * 2 + p.j AS INT), CAST(o.i * 2 + p.j AS INT), 'Deposit',
                    CASE WHEN o.i % 3 = 0 THEN 'Fora da operacao' ELSE 'Dentro' END, ' Parcela ',
                    DATEADD(day, -v.age, '2026-09-01'), 0
             FROM o CROSS JOIN p CROSS JOIN v;"
        ),
        format!(
            "WITH v AS (SELECT 0 AS age UNION ALL SELECT 1),
                  o AS (SELECT TOP ({OBRAS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS i FROM sys.all_objects),
                  p AS (SELECT 1 AS j UNION ALL SELECT 2)
             INSERT INTO dbo.produtosgeraisparametros
             SELECT CONCAT('UN-', o.i * 7), CONCAT('OBRA-', o.i), CAST((o.i % 40) * 2 + 1 AS INT),
                    DATEADD(day, -v.age, '2026-09-01'), 0
             FROM o CROSS JOIN v;"
        ),
    ]
}

/// Latest version of a remote table, as the views write it.
fn latest(table: &str, partition: &str, alias: &str) -> String {
    format!(
        "SELECT * FROM (SELECT {alias}.*, row_number() OVER (PARTITION BY {partition} ORDER BY {alias}.dataregistro DESC) rn \
         FROM warehouse.dbo.{table} {alias} WHERE {alias}.dataregistro <= current_timestamp AND {alias}.deleted = false) t \
         WHERE rn = 1"
    )
}

/// The Hive replicas of `base1` and the views, written by Trino.
fn views() -> Vec<String> {
    let operacaoobra = latest("operacaoobra", "o.obraid, o.operacaoid", "o");
    let obras = latest("obras", "o.obraid", "o");
    let banco = latest("bancooperacaoobra", "bo.bancooperacaoobraid", "bo");
    let banco_by_account = latest(
        "bancooperacaoobra",
        "bo.operacaoobraid, bo.bancoid, bo.numconta",
        "bo",
    );
    let operacao = latest("operacao", "p.operacaoid", "p");
    let produtos = latest(
        "produtosgeraisparametros",
        "pgp.identificador, pgp.obraid, pgp.operacaoid",
        "pgp",
    );
    let depara = latest("depara", "d.deparaid", "d");
    let base_columns = "r.obraid, r.nomecliente, r.identificador_unid, r.numero_venda, r.numnovavend, r.data_rec, \
         r.data_vencimento, r.empresaid, r.obra_rec, r.totconf, r.totparcel, r.valortot_vrec valortotaldocontrato, \
         (r.valortot_vrec * (p.percentualcomissao / DECIMAL '100.0')) valortotalrestantedescontar, \
         ((r.totparcel / NULLIF(r.valortot_vrec, 0)) * DECIMAL '100.0') percentualvalortotalrecebido, \
         r.datacessao_vrec, r.datacancel_vrec, p.percentualcomissao";
    let receipts_columns = "r.descricaoparcela, r.bancoid, r.data_rec, r.numparcger_rec, r.numparcela, r.codobra, \
         r.identificador_unid, r.nomeobra, r.vendaid, r.obraid, r.conta_corrente, r.status_venda, r.tipoparcela, \
         CAST(r.totparcel AS DECIMAL(18, 2)) totparcel";
    vec![
        "CREATE SCHEMA hive.analytics".to_owned(),
        format!(
            "CREATE TABLE hive.analytics.operacaoobra_latest AS SELECT operacaoobraid, obraid, operacaoid, \
             percentualcomissao, percentualoperacao, dataregistro FROM ({operacaoobra})"
        ),
        format!("CREATE TABLE hive.analytics.obras_latest AS SELECT obraid, descr_obr FROM ({obras})"),
        format!(
            "CREATE TABLE hive.analytics.bancooperacaoobra_latest AS SELECT operacaoobraid, bancoid, numconta, \
             bancotipoid FROM ({banco})"
        ),
        format!(
            "CREATE VIEW hive.analytics.vw_recebidas_comissoes_base2 AS SELECT ob.descr_obr nomeobra, {base_columns}, \
             format_datetime(r.data_rec, 'yyyy-MM') mesano, \
             (CASE WHEN (r.numnovavend IS NOT NULL) THEN r.numnovavend ELSE r.numero_venda END) vendabaseid \
             FROM warehouse.dbo.vd_recebidas r \
             INNER JOIN ({operacaoobra}) p ON p.obraid = r.obraid \
             INNER JOIN (SELECT obraid, descr_obr FROM ({obras})) ob ON ob.obraid = r.obraid \
             INNER JOIN ({banco}) bo ON bo.operacaoobraid = p.operacaoobraid AND bo.bancoid = r.bancoid \
               AND bo.numconta = r.conta_corrente AND bo.bancotipoid = 2"
        ),
        format!(
            "CREATE VIEW hive.analytics.vw_recebidas_comissoes_base1 AS SELECT ob.descr_obr nomeobra, {base_columns}, \
             bo.bancotipoid, date_trunc('month', r.data_rec) mesano, COALESCE(r.numnovavend, r.numero_venda) vendabaseid \
             FROM warehouse.dbo.vd_recebidas r \
             INNER JOIN hive.analytics.operacaoobra_latest p ON p.obraid = r.obraid \
             INNER JOIN hive.analytics.obras_latest ob ON ob.obraid = r.obraid \
             INNER JOIN hive.analytics.bancooperacaoobra_latest bo ON bo.operacaoobraid = p.operacaoobraid \
               AND bo.bancoid = r.bancoid AND bo.numconta = r.conta_corrente AND bo.bancotipoid = 2"
        ),
        "CREATE VIEW hive.analytics.vw_recebidas_comissoes_daily_base1 AS SELECT obraid, numero_venda, obra_rec, \
         nomeobra, nomecliente, bancotipoid, totparcel, data_vencimento, data_rec, totconf, mesano, \
         valortotaldocontrato, identificador_unid, percentualcomissao, valortotalrestantedescontar, \
         percentualvalortotalrecebido, vendabaseid, SUM(percentualvalortotalrecebido) percentualrecebidodia \
         FROM hive.analytics.vw_recebidas_comissoes_base1 \
         GROUP BY obraid, numero_venda, obra_rec, nomeobra, nomecliente, bancotipoid, totparcel, data_vencimento, \
         data_rec, totconf, mesano, valortotaldocontrato, identificador_unid, percentualcomissao, \
         valortotalrestantedescontar, percentualvalortotalrecebido, vendabaseid"
            .to_owned(),
        format!(
            "CREATE VIEW hive.analytics.vw_recebimentos_base AS WITH \
             base AS (SELECT {receipts_columns} FROM warehouse.dbo.vd_recebidas r WHERE r.id % 2 = 0 \
                          UNION ALL SELECT {receipts_columns} FROM warehouse.dbo.vd_recebidas r WHERE r.id % 2 = 1), \
             operacaoobra AS ({operacaoobra}), bancooperacao AS ({banco_by_account}), operacao AS ({operacao}), \
             produtosgeraisparametros AS ({produtos}), \
             depara_operacao AS (SELECT oo.obraid, oo.operacaoid, dp.de, dp.para, dp.tipo FROM operacaoobra oo \
                                 LEFT JOIN ({depara}) dp ON dp.operacaoobraid = oo.operacaoobraid) \
             SELECT c.obraid, c.codobra, c.nomeobra, c.identificador_unid, c.vendaid, c.data_rec, \
                    DATE(c.data_rec) data_recebimento, c.bancoid, c.conta_corrente, c.numparcger_rec, c.numparcela, \
                    c.descricaoparcela, c.tipoparcela, c.status_venda, c.totparcel, oo.operacaoid, oo.operacaoobraid, \
                    CAST(oo.percentualoperacao AS DECIMAL(18, 2)) percentualoperacao, op.numero, op.estruturador, \
                    op.apelidoop, op.statusoperacao, bo.bancotipoid, \
                    CAST(((c.totparcel * CAST(oo.percentualoperacao AS DECIMAL(18, 2))) / 100) AS DECIMAL(18, 2)) \
                      valor_recebido_operacao \
             FROM base c \
             INNER JOIN operacaoobra oo ON oo.obraid = c.obraid \
             INNER JOIN operacao op ON op.operacaoid = oo.operacaoid \
             LEFT JOIN bancooperacao bo ON bo.operacaoobraid = oo.operacaoobraid AND bo.bancoid = c.bancoid \
               AND bo.numconta = c.conta_corrente \
             LEFT JOIN produtosgeraisparametros pgp ON pgp.obraid = c.obraid AND pgp.operacaoid = oo.operacaoid \
               AND pgp.identificador = c.identificador_unid \
             WHERE bo.bancotipoid IN (2, 3) AND op.statusoperacao = 'Liquidada' AND pgp.identificador IS NULL \
               AND NOT EXISTS (SELECT 1 FROM depara_operacao d WHERE d.operacaoid = oo.operacaoid \
                 AND d.obraid = c.obraid AND lower(trim(d.tipo)) = 'parcela' \
                 AND upper(trim(d.de)) = upper(trim(c.tipoparcela)) AND upper(d.para) LIKE '%FORA%OPERA%')"
        ),
    ]
}

/// One row summing every column of `view`, as text: equal fingerprints mean
/// the whole view was read and joined the same way.
fn fingerprint(view: &str) -> String {
    let sums: &[&str] = match view {
        "vw_recebimentos_base" => &[
            "count(DISTINCT vendaid)",
            "sum(totparcel)",
            "sum(valor_recebido_operacao)",
            "sum(operacaoid)",
            "sum(bancotipoid)",
            "count(DISTINCT data_recebimento)",
            "count(DISTINCT numero)",
        ],
        "vw_recebidas_comissoes_daily_base1" => &[
            "sum(totparcel)",
            "sum(percentualrecebidodia)",
            "sum(valortotalrestantedescontar)",
            "count(DISTINCT mesano)",
        ],
        _ => &[
            "count(DISTINCT nomeobra)",
            "sum(totparcel)",
            "sum(valortotalrestantedescontar)",
            "sum(percentualvalortotalrecebido)",
            "count(DISTINCT mesano)",
            "sum(vendabaseid)",
        ],
    };
    let casts: Vec<String> = std::iter::once("count(*)")
        .chain(sums.iter().copied())
        .map(|sum| format!("CAST({sum} AS varchar)"))
        .collect();
    format!("SELECT {} FROM hive.analytics.{view}", casts.join(", "))
}

/// Columns of `vw_recebidas_comissoes_daily_base1`, in order.
const DAILY: [&str; 18] = [
    "obraid",
    "numero_venda",
    "obra_rec",
    "nomeobra",
    "nomecliente",
    "bancotipoid",
    "totparcel",
    "data_vencimento",
    "data_rec",
    "totconf",
    "mesano",
    "valortotaldocontrato",
    "identificador_unid",
    "percentualcomissao",
    "valortotalrestantedescontar",
    "percentualvalortotalrecebido",
    "vendabaseid",
    "percentualrecebidodia",
];

/// The hash joins of `plan` that read the remote table on their build side.
fn remote_builds(plan: &str) -> usize {
    let lines: Vec<&str> = plan.lines().collect();
    let depth = |line: &str| line.len() - line.trim_start().len();
    let mut found = 0;
    for (at, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("HashJoinExec") {
            continue;
        }
        let Some(first) = lines.get(at + 1) else { continue };
        let child = depth(first);
        let remote = lines[at + 1..]
            .iter()
            .enumerate()
            .take_while(|(i, l)| depth(l) > depth(line) && (*i == 0 || depth(l) != child))
            .any(|(_, l)| l.contains("vd_recebidas"));
        found += usize::from(remote);
    }
    found
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one scenario, read top to bottom")]
async fn screen_views_over_a_remote_table_larger_than_the_pool() {
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE warehouse").await;
    let started = Instant::now();
    for statement in seed_sql() {
        exec(&host, port, "warehouse", &statement).await;
    }
    note("seed", format!("{REMOTE_ROWS} rows in {:?}", started.elapsed()));
    let shared = shared().await;
    let catalog = format!(
        "connector.name=sqlserver\n\
         connection-url=jdbc:sqlserver://{host}:{port};database=warehouse;encrypt=false\n\
         connection-user=sa\n\
         connection-password={SA_PASSWORD}\n"
    );
    let trino = Trino::start_with(&shared, "views-memory", &[("warehouse", catalog)]).await;
    let statements = views();
    let statements: Vec<&str> = statements.iter().map(String::as_str).collect();
    trino.batch(&statements).await;

    let spill = tempfile::tempdir().unwrap();
    let execution = ExecutionConfig {
        target_partitions: Some(6),
        ..ExecutionConfig::default()
    };
    let mut engine = open(&shared.warehouse(), spill.path(), POOL, &execution, &[]).await;
    common::attach(&mut engine, sqlserver(&host, port)).await;
    let mut failures = Vec::new();
    let timed = |label: &'static str, elapsed: Duration, outcome: &Result<Vec<Vec<String>>, String>| {
        note(
            label,
            format!(
                "{elapsed:?}: {}",
                match outcome {
                    Ok(rows) => format!("{} rows", rows.len()),
                    Err(error) => error.chars().take(400).collect(),
                }
            ),
        );
    };

    for view in [
        "vw_recebidas_comissoes_base2",
        "vw_recebidas_comissoes_base1",
        "vw_recebimentos_base",
    ] {
        let first = format!("SELECT * FROM hive.analytics.{view} LIMIT 50");
        let plan = physical_plan(&engine, &first).await;
        let remote = remote_builds(&plan);
        if remote > 0 || !plan.contains("HashJoinExec") {
            failures.push(format!(
                "{view}: {remote} hash join(s) build the remote table:\n{plan}"
            ));
        }
        engine.reset_memory_peak();
        let started = Instant::now();
        let limited = try_cl(&engine, &first).await;
        let limit_time = started.elapsed();
        timed(view, limit_time, &limited);
        match &limited {
            Ok(rows) if rows.len() == 50 => {}
            other => failures.push(format!("{view} LIMIT 50: {other:?}")),
        }
        assert_pool_given_back(&engine).await;

        let whole = fingerprint(view);
        let reference = trino.batch(&[whole.as_str()]).await.remove(0);
        let started = Instant::now();
        let answered = try_cl(&engine, &whole).await;
        let whole_time = started.elapsed();
        timed("whole view", whole_time, &answered);
        match answered {
            Ok(rows) if rows == reference && !rows.is_empty() => {}
            Ok(rows) => failures.push(format!("{view}: trino={reference:?} codelake={rows:?}")),
            Err(error) => failures.push(format!("{view} whole: {error}")),
        }
        // Without an order, the first 50 rows stop the read of the remote
        // table: a fraction of a whole read.
        if limited.is_ok() && limit_time * 3 > whole_time {
            failures.push(format!(
                "{view}: LIMIT 50 took {limit_time:?}, the whole view {whole_time:?}"
            ));
        }
        assert_pool_given_back(&engine).await;
    }

    // The aggregation of 17 keys over a view, ordered by every column.
    let ordered = format!(
        "SELECT {} FROM hive.analytics.vw_recebidas_comissoes_daily_base1 ORDER BY {} LIMIT 50",
        DAILY
            .iter()
            .map(|c| format!("CAST({c} AS varchar)"))
            .collect::<Vec<_>>()
            .join(", "),
        DAILY.join(", ")
    );
    let reference = trino.batch(&[ordered.as_str()]).await.remove(0);
    engine.reset_memory_peak();
    let started = Instant::now();
    let rows = try_cl(&engine, &ordered).await;
    timed("daily ordered", started.elapsed(), &rows);
    match rows {
        Ok(rows) if rows == reference && rows.len() == 50 => {}
        Ok(rows) => failures.push(format!("daily ordered: trino={reference:?}\ncodelake={rows:?}")),
        Err(error) => failures.push(format!("daily ordered: {error}")),
    }
    let whole = fingerprint("vw_recebidas_comissoes_daily_base1");
    let reference = sorted(trino.batch(&[whole.as_str()]).await.remove(0));
    let started = Instant::now();
    let rows = try_cl(&engine, &whole).await.map(sorted);
    timed("daily whole", started.elapsed(), &rows);
    match rows {
        Ok(rows) if rows == reference => {}
        Ok(rows) => failures.push(format!("daily whole: trino={reference:?} codelake={rows:?}")),
        Err(error) => failures.push(format!("daily whole: {error}")),
    }
    let peak = engine.memory_peak();
    note("pool", format!("peak {peak} B, limit {} B", POOL.as_u64()));
    if peak > POOL.as_u64() {
        failures.push(format!("the pool held {peak} B, over its {POOL:?}"));
    }
    assert_pool_given_back(&engine).await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
