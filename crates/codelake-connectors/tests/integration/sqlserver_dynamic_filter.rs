//! Reconciliation of a local table against a large SQL Server table: the
//! local side (a few thousand keys) must reach the remote query, or SQL
//! Server returns the whole table.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::{Connector, Engine};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use secrecy::SecretString;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub(crate) const SA_PASSWORD: &str = "Semi-Join-Only-1";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);

pub(crate) async fn start_mssql() -> ContainerAsync<MssqlServer> {
    MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag("2022-latest")
        .with_startup_timeout(STARTUP_TIMEOUT)
        .start()
        .await
        .unwrap()
}

async fn client(host: &str, port: u16, database: &str) -> Client<Compat<TcpStream>> {
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server("sa", SA_PASSWORD));
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
    Client::connect(config, tcp.compat_write()).await.unwrap()
}

pub(crate) async fn exec(host: &str, port: u16, database: &str, batch: &str) {
    let mut c = client(host, port, database).await;
    let done = match c.simple_query(batch).await {
        Ok(stream) => stream.into_results().await.map(|_| ()),
        Err(e) => Err(e),
    };
    if let Err(e) = done {
        panic!("`{batch}` failed: {e}");
    }
}

/// First column of every row of `sql`, as text.
async fn direct_texts(host: &str, port: u16, database: &str, sql: &str) -> Vec<String> {
    let mut c = client(host, port, database).await;
    let rows = c
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("direct `{sql}` failed: {e}"))
        .into_first_result()
        .await
        .unwrap_or_else(|e| panic!("direct `{sql}` failed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| match row.try_get::<&str, _>(i) {
                    Ok(Some(s)) => s.to_owned(),
                    _ => match row.try_get::<i64, _>(i) {
                        Ok(Some(v)) => v.to_string(),
                        _ => String::from("?"),
                    },
                })
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect()
}

/// `rows` versions of `rows * 9 / 10` keys over two years, one version in
/// ten repeated; the last days hold what a daily replica would copy.
async fn seed(host: &str, port: u16, rows: u64) {
    exec(host, port, "master", "CREATE DATABASE warehouse").await;
    exec(
        host,
        port,
        "warehouse",
        "CREATE TABLE dbo.recebidas (
           id BIGINT IDENTITY PRIMARY KEY,
           recebidasid VARCHAR(50) NOT NULL, obraid VARCHAR(10) NOT NULL,
           dataregistro DATETIME NOT NULL, seqval BINARY(10) NOT NULL, deleted BIT NOT NULL,
           empresa_rec INT NULL, obra_rec VARCHAR(10) NULL, numvend_rec INT NULL, numparc_rec INT NULL,
           parctype_rec VARCHAR(2) NULL, tipo_rec INT NULL, numparcger_rec INT NULL,
           valor_rec DECIMAL(18,2) NULL);",
    )
    .await;
    let keys = rows * 9 / 10;
    exec(
        host,
        port,
        "warehouse",
        &format!(
            "WITH n AS (SELECT TOP ({rows}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
                        FROM sys.all_objects a CROSS JOIN sys.all_objects b CROSS JOIN sys.all_objects c)
             INSERT INTO dbo.recebidas WITH (TABLOCK)
               (recebidasid, obraid, dataregistro, seqval, deleted, empresa_rec, obra_rec, numvend_rec,
                numparc_rec, parctype_rec, tipo_rec, numparcger_rec, valor_rec)
             SELECT CONCAT('REC-', RIGHT(CONCAT('0000000000', i % {keys}), 10)),
                    CONCAT('OB', i % 700),
                    DATEADD(millisecond, -CAST(i % 1000 AS INT) * 7,
                      DATEADD(second, -CAST(({rows} - i) * (730 * 86400 / {rows}) AS INT), '2026-09-29T01:00:00')),
                    CAST(i AS BINARY(10)),
                    CASE WHEN i % 53 = 0 THEN 1 ELSE 0 END,
                    CAST(i % 7 AS INT), CONCAT('O', i % 97), CAST(i % 5000 AS INT), CAST(i % 120 AS INT),
                    CASE WHEN i % 3 = 0 THEN 'P' ELSE 'R' END, CAST(i % 4 AS INT), CAST(i % 360 AS INT),
                    CAST((i % 100000) / 100.0 AS DECIMAL(18,2))
             FROM n;
             CREATE INDEX ix_recebidasid ON dbo.recebidas (recebidasid);
             CREATE INDEX ix_dataregistro ON dbo.recebidas (dataregistro);"
        ),
    )
    .await;
}

pub(crate) fn sqlserver(host: &str, port: u16) -> Arc<dyn Connector> {
    Arc::new(SqlServerConnector::new(
        "warehouse".into(),
        SqlServerParams {
            host: host.to_owned(),
            port,
            database: "warehouse".to_owned(),
            user: "sa".into(),
            password: SecretString::from(SA_PASSWORD),
            encrypt: "required".into(),
            trust_server_certificate: true,
            pool_size: 4,
        },
    ))
}

fn hive(warehouse: &std::path::Path) -> Arc<dyn Connector> {
    Arc::new(HiveConnector::new(
        "hive".into(),
        HiveParams {
            warehouse: warehouse.display().to_string(),
            s3_endpoint: None,
            s3_region: "us-east-1".into(),
            credentials: None,
            path_style: false,
        },
    ))
}

/// An engine with room for the reconciliation's sorts (the shared test
/// engine has 256 MiB) whose dynamic filters list up to `keys` keys.
fn engine(spill: &std::path::Path, keys: usize) -> Engine {
    let memory = codelake_core::config::MemoryConfig {
        limit: codelake_core::ByteSize::gib(2),
        spill_dir: spill.to_owned(),
        spill_limit: codelake_core::ByteSize::gib(4),
        metadata_cache_limit: codelake_core::ByteSize::mib(8),
    };
    let execution = codelake_core::config::ExecutionConfig {
        dynamic_filter_max_keys: keys,
        dynamic_filter_max_key_bytes: codelake_core::ByteSize::mib(16),
        ..codelake_core::config::ExecutionConfig::default()
    };
    Engine::new(&memory, &execution).expect("engine")
}

async fn attached(
    spill: &std::path::Path,
    warehouse: &std::path::Path,
    host: &str,
    port: u16,
    keys: usize,
) -> Engine {
    let mut engine = engine(spill, keys);
    common::attach(&mut engine, sqlserver(host, port)).await;
    common::attach(&mut engine, hive(warehouse)).await;
    engine
}

async fn run(engine: &Engine, sql: &str) -> Vec<RecordBatch> {
    engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("running `{sql}`: {e}"))
}

fn text(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            let row: Vec<String> = batch
                .columns()
                .iter()
                .map(|c| array_value_to_string(c, i).unwrap())
                .collect();
            out.push(row.join(" | "));
        }
    }
    out
}

const RECONCILE: &str = "
WITH pilot_dedup AS (
  SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY recebidasid ORDER BY dataregistro DESC, __seqval DESC, deleted ASC) AS rn
                 FROM hive.pilot.recebidas) t
  WHERE rn = 1 AND deleted = 0),
subset_ids AS (SELECT DISTINCT recebidasid FROM pilot_dedup),
prod_subset AS (
  SELECT r.recebidasid, r.obraid, r.dataregistro, r.seqval, CAST(r.deleted AS integer) AS deleted,
         r.empresa_rec, r.obra_rec, r.numvend_rec, r.numparc_rec, r.parctype_rec, r.tipo_rec, r.numparcger_rec, r.valor_rec
  FROM warehouse.dbo.recebidas r
  WHERE r.recebidasid IN (SELECT recebidasid FROM subset_ids)
    AND CAST(dataregistro AS timestamp(3)) >= date_add('day', -7, TIMESTAMP '2026-09-29 01:01:40.133')),
prod_dedup AS (
  SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY recebidasid ORDER BY dataregistro DESC, seqval DESC, deleted ASC) AS rn
                 FROM prod_subset) t
  WHERE rn = 1 AND deleted = 0),
joined AS (
  SELECT COALESCE(p.obraid, q.obraid) AS obraid,
         SUM(CASE WHEN p.recebidasid IS NOT NULL THEN 1 ELSE 0 END) AS n_pilot,
         SUM(CASE WHEN q.recebidasid IS NOT NULL THEN 1 ELSE 0 END) AS n_prod,
         SUM(CASE WHEN p.recebidasid IS NOT NULL AND q.recebidasid IS NOT NULL
                   AND p.valor_rec = q.valor_rec AND p.numparc_rec = q.numparc_rec THEN 1 ELSE 0 END) AS n_equal,
         SUM(p.valor_rec) AS v_pilot, SUM(q.valor_rec) AS v_prod,
         checksum(q.recebidasid) AS ck_prod
  FROM pilot_dedup p FULL OUTER JOIN prod_dedup q ON p.recebidasid = q.recebidasid
  GROUP BY 1)
SELECT * FROM joined ORDER BY obraid";

/// What SQL Server ran reading `table`, from its plan cache: each
/// statement as the engine sent it (an ad hoc plan keeps the text; a plan
/// SQL Server parameterized shows `@1` for its literals), and the rows each
/// returned.
fn remote_statements(table: &str) -> String {
    let reads =
        format!("t.text LIKE '%FROM \\[dbo\\].\\[{table}\\]%' ESCAPE '\\' AND t.text NOT LIKE '%dm_exec_%'");
    format!(
        "SELECT CONCAT(cp.objtype, ': ', t.text)
         FROM sys.dm_exec_cached_plans cp CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) t WHERE {reads}
         UNION ALL
         SELECT CONCAT(s.total_rows, ' rows in ', s.total_elapsed_time / 1000, ' ms: ', LEFT(t.text, 200))
         FROM sys.dm_exec_query_stats s CROSS APPLY sys.dm_exec_sql_text(s.sql_handle) t WHERE {reads}"
    )
}

/// Runs `sql` on a cold plan cache and returns its rows and the statements
/// SQL Server ran reading `table`.
async fn traced(
    engine: &Engine,
    host: &str,
    port: u16,
    table: &str,
    sql: &str,
) -> (Vec<String>, Vec<String>) {
    exec(host, port, "master", "DBCC FREEPROCCACHE").await;
    let rows = text(&run(engine, sql).await);
    let sent = direct_texts(host, port, "warehouse", &remote_statements(table)).await;
    (rows, sent)
}

/// FNV-1a of the rows, to compare whole results in a line.
fn digest(rows: &[String]) -> String {
    let hash = rows
        .iter()
        .flat_map(|r| r.bytes().chain(*b"\n"))
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
    format!("{} rows, fnv {hash:016x}", rows.len())
}

/// The pilot a daily replica holds: every version written since `since`.
async fn copy_pilot(engine: &Engine, since: &str) {
    run(engine, "CREATE SCHEMA hive.pilot").await;
    run(
        engine,
        &format!(
            "CREATE TABLE hive.pilot.recebidas AS
             SELECT recebidasid, obraid, dataregistro, seqval AS __seqval, CAST(deleted AS integer) AS deleted,
                    empresa_rec, obra_rec, numvend_rec, numparc_rec, parctype_rec, tipo_rec, numparcger_rec,
                    valor_rec + CASE WHEN numvend_rec % 11 = 0 THEN 1 ELSE 0 END AS valor_rec
             FROM warehouse.dbo.recebidas WHERE dataregistro >= TIMESTAMP '{since}'"
        ),
    )
    .await;
}

async fn seed_edges(host: &str, port: u16) {
    exec(
        host,
        port,
        "warehouse",
        "CREATE TABLE dbo.bordas (id INT NOT NULL PRIMARY KEY, registro DATETIME NULL);
         INSERT INTO dbo.bordas VALUES
           (1, '2026-09-22T01:01:40.130'), (2, '2026-09-22T01:01:40.133'),
           (3, '2026-09-22T01:01:40.137'), (4, '2026-09-22T01:01:39.997'),
           (5, '2026-09-22T01:01:40.000'), (6, '2026-09-22T01:01:41.000'), (7, NULL);
         CREATE TABLE dbo.bordas7 (id INT NOT NULL PRIMARY KEY, registro DATETIME2(7) NULL);
         INSERT INTO dbo.bordas7 VALUES
           (1, '2026-09-22T01:01:40.0000001'), (2, '2026-09-22T01:01:40'), (3, '2026-09-22T01:01:41');
         CREATE TABLE dbo.chaves (id INT NOT NULL PRIMARY KEY, chave VARCHAR(20) NULL);
         INSERT INTO dbo.chaves VALUES (1, 'K-1'), (2, 'k-1'), (3, 'K-1 '), (4, 'K-2'), (5, NULL), (6, 'K-3');
         INSERT INTO dbo.chaves SELECT TOP (2000) 100 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
           CONCAT('F-', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
         CREATE TABLE dbo.numeros (n INT NOT NULL PRIMARY KEY, par BIT NOT NULL);
         INSERT INTO dbo.numeros SELECT TOP (5000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), 0
           FROM sys.all_objects a CROSS JOIN sys.all_objects b;",
    )
    .await;
}

/// Soft checks: every one is logged, the test fails once at the end.
#[derive(Default)]
struct Checks {
    log: String,
    failed: Vec<String>,
}

impl Checks {
    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        let mark = if ok { "PASS" } else { "FAIL" };
        let _ = writeln!(self.log, "[{mark}] {name}\n        {detail}");
        if !ok {
            self.failed.push(name.to_owned());
        }
    }

    #[allow(
        clippy::print_stdout,
        reason = "the check log is the evidence shown with --no-capture"
    )]
    fn finish(self) {
        println!(
            "===== dynamic filters and time windows at SQL Server =====\n{}",
            self.log
        );
        assert!(
            self.failed.is_empty(),
            "failed checks: {:?}\n{}",
            self.failed,
            self.log
        );
    }
}

fn sent_any(sent: &[String], all: &[&str]) -> bool {
    sent.iter().any(|s| all.iter().all(|part| s.contains(part)))
}

#[tokio::test]
#[allow(clippy::too_many_lines, reason = "one scenario per block, read top to bottom")]
async fn local_keys_and_time_windows_reach_sql_server() {
    let mut checks = Checks::default();
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    seed(&host, port, 60_000).await;
    seed_edges(&host, port).await;
    let spill = tempfile::tempdir().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let engine = attached(spill.path(), warehouse.path(), &host, port, 150).await;
    // One day of versions: ~80 keys, listed by the dynamic filter.
    copy_pilot(&engine, "2026-09-28 01:00:00").await;
    run(
        &engine,
        "CREATE TABLE hive.pilot.chaves AS SELECT * FROM (VALUES ('K-1'), ('K-3'), (CAST(NULL AS varchar))) t(chave)",
    )
    .await;
    run(
        &engine,
        "CREATE TABLE hive.pilot.numeros AS SELECT CAST(n AS integer) AS n FROM UNNEST(sequence(1000, 1049)) t(n)",
    )
    .await;

    // A bound inside a second on a `datetime` column: whole seconds at the
    // source, the exact bound in the engine (1/300 s steps shown rounded).
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "bordas",
        "SELECT id FROM warehouse.dbo.bordas
         WHERE CAST(registro AS timestamp(3)) >= date_add('day', -7, TIMESTAMP '2026-09-29 01:01:40.133') ORDER BY id",
    )
    .await;
    checks.check(
        "datetime >= .133: rows",
        rows == ["2", "3", "6"],
        format!("{rows:?}"),
    );
    checks.check(
        "datetime >= .133: whole second sent",
        sent_any(
            &sent,
            &["[registro] >= CAST('2026-09-22 01:01:40' AS datetime2(7))"],
        ),
        format!("{sent:?}"),
    );
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "bordas",
        "SELECT id FROM warehouse.dbo.bordas WHERE registro <= TIMESTAMP '2026-09-22 01:01:40.133' ORDER BY id",
    )
    .await;
    checks.check(
        "datetime <= .133: rows",
        rows == ["1", "2", "4", "5"],
        format!("{rows:?}"),
    );
    checks.check(
        "datetime <= .133: next second sent",
        sent_any(
            &sent,
            &["[registro] < CAST('2026-09-22 01:01:41' AS datetime2(7))"],
        ),
        format!("{sent:?}"),
    );

    // datetime2(7) shows 100 ns past a second as that second: an upper
    // bound at the source is strict, past the second.
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "bordas7",
        "SELECT id FROM warehouse.dbo.bordas7 WHERE registro <= TIMESTAMP '2026-09-22 01:01:40' ORDER BY id",
    )
    .await;
    checks.check(
        "datetime2(7) <= whole second: rows",
        rows == ["1", "2"],
        format!("{rows:?}"),
    );
    checks.check(
        "datetime2(7) <= whole second: next second sent",
        sent_any(
            &sent,
            &["[registro] < CAST('2026-09-22 01:01:41' AS datetime2(7))"],
        ),
        format!("{sent:?}"),
    );

    // Text keys of a local table: an IN list at the source, which compares
    // under its case-insensitive collation; the join keeps the exact ones.
    let semi =
        "SELECT id FROM warehouse.dbo.chaves WHERE chave IN (SELECT chave FROM hive.pilot.chaves) ORDER BY id";
    let (rows, sent) = traced(&engine, &host, port, "chaves", semi).await;
    checks.check(
        "text keys, IN subquery: rows",
        rows == ["1", "6"],
        format!("{rows:?}"),
    );
    checks.check(
        "text keys, IN subquery: IN list sent, collation superset returned",
        sent_any(&sent, &["[chave] IN (", "'K-1'", "'K-3'"]) && sent_any(&sent, &["4 rows in"]),
        format!("{sent:?}"),
    );
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "chaves",
        "SELECT chave FROM hive.pilot.chaves INTERSECT SELECT chave FROM warehouse.dbo.chaves ORDER BY 1",
    )
    .await;
    checks.check(
        "text keys, INTERSECT: NULL matches NULL",
        rows == ["K-1", "K-3", ""],
        format!("{rows:?}"),
    );
    checks.check(
        "text keys, INTERSECT: the NULL key is not filtered out at the source",
        !sent_any(&sent, &[" IN ("]) || sent_any(&sent, &[" IN (", " IS NULL"]),
        format!("{sent:?}"),
    );
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "chaves",
        "SELECT c.id FROM hive.pilot.chaves p JOIN warehouse.dbo.chaves c ON c.chave IS NOT DISTINCT FROM p.chave
         ORDER BY 1",
    )
    .await;
    checks.check(
        "text keys, IS NOT DISTINCT FROM: NULL matches NULL",
        rows == ["1", "5", "6"],
        format!("{rows:?}"),
    );
    // Its dynamic filter is `(bounds AND keys) OR chave IS NULL`: the keys go
    // with the NULL key, the text bounds stay out.
    checks.check(
        "text keys, IS NOT DISTINCT FROM: keys and the NULL key sent",
        sent_any(&sent, &["[chave] IN (", "[chave] IS NULL"])
            && !sent_any(&sent, &["[chave] >="])
            && sent_any(&sent, &["5 rows in"]),
        format!("{sent:?}"),
    );
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "chaves",
        "SELECT c.id FROM hive.pilot.chaves p JOIN warehouse.dbo.chaves c ON c.chave = p.chave ORDER BY 1",
    )
    .await;
    checks.check("text keys, join: rows", rows == ["1", "6"], format!("{rows:?}"));
    checks.check(
        "text keys, join: IN list sent",
        sent_any(&sent, &["[chave] IN ("]),
        format!("{sent:?}"),
    );
    let (rows, sent) = traced(
        &engine,
        &host,
        port,
        "chaves",
        "SELECT count(*), count(p.chave) FROM hive.pilot.chaves p RIGHT JOIN warehouse.dbo.chaves c ON c.chave = p.chave",
    )
    .await;
    checks.check(
        "text keys, outer join: rows",
        rows == ["2006 | 2"],
        format!("{rows:?}"),
    );
    checks.check(
        "text keys, outer join: no key sent for the preserved side",
        !sent_any(&sent, &[" IN ("]),
        format!("{sent:?}"),
    );

    // A partitioned join lists its keys per partition; the source gets the
    // lists while they hold the limit together, else nothing for text keys
    // (no text bounds): a source compares each row with every key.
    run(
        &engine,
        "CREATE TABLE hive.pilot.textos AS SELECT CONCAT('F-', CAST(n AS varchar)) AS chave FROM UNNEST(sequence(1, 40)) t(n)",
    )
    .await;
    let join = "SELECT count(*), min(c.id), max(c.id) FROM warehouse.dbo.chaves c JOIN hive.pilot.textos t ON c.chave = t.chave";
    for (keys, sent_keys) in [(40, true), (39, false)] {
        let engine = attached(spill.path(), warehouse.path(), &host, port, keys).await;
        for setting in [
            "SET datafusion.execution.target_partitions = 16",
            "SET datafusion.optimizer.hash_join_single_partition_threshold = 0",
            "SET datafusion.optimizer.hash_join_single_partition_threshold_rows = 0",
        ] {
            engine.context().sql(setting).await.unwrap();
        }
        let (rows, sent) = traced(&engine, &host, port, "chaves", join).await;
        checks.check(
            &format!("40 text keys in 16 partitions, limit {keys}: rows"),
            rows == ["40 | 101 | 140"],
            format!("{rows:?}"),
        );
        checks.check(
            &format!("40 text keys in 16 partitions, limit {keys}: IN lists sent: {sent_keys}"),
            sent_any(&sent, &["[chave] IN ("]) == sent_keys,
            format!("{sent:?}"),
        );
    }

    // Past the IN-list limit, integer keys go as their min/max.
    let few = attached(spill.path(), warehouse.path(), &host, port, 10).await;
    let (rows, sent) = traced(
        &few,
        &host,
        port,
        "numeros",
        "SELECT count(*) FROM warehouse.dbo.numeros WHERE n IN (SELECT n FROM hive.pilot.numeros)",
    )
    .await;
    checks.check(
        "integer keys past the limit: rows",
        rows == ["50"],
        format!("{rows:?}"),
    );
    checks.check(
        "integer keys past the limit: bounds sent",
        sent_any(&sent, &["[n] >= 1000", "[n] <= 1049"]) && sent_any(&sent, &["50 rows in"]),
        format!("{sent:?}"),
    );

    // The reconciliation: the pilot's keys and the time window at the
    // source, and the same result as without dynamic filters.
    let (rows, sent) = traced(&engine, &host, port, "recebidas", RECONCILE).await;
    checks.check(
        "reconciliation: keys and window sent",
        sent_any(
            &sent,
            &[
                "[recebidasid] IN (",
                "[dataregistro] >= CAST('2026-09-22 01:01:40' AS datetime2(7))",
            ],
        ),
        format!("{sent:?}"),
    );
    engine
        .context()
        .sql("SET datafusion.optimizer.enable_join_dynamic_filter_pushdown = false")
        .await
        .unwrap();
    let (reference, sent) = traced(&engine, &host, port, "recebidas", RECONCILE).await;
    checks.check(
        "reconciliation: without dynamic filters, no key sent",
        !sent_any(&sent, &[" IN ("]),
        format!("{sent:?}"),
    );
    checks.check(
        "reconciliation: same rows with and without dynamic filters",
        rows == reference && !rows.is_empty(),
        format!("{} vs {}", digest(&rows), digest(&reference)),
    );
    checks.finish();
}

/// Hand-run benchmark settings: `CODELAKE_BENCH_ROWS` rows in SQL Server
/// (2 000 000), the pilot holds the versions since `CODELAKE_BENCH_SINCE`
/// (five days) and each configuration runs `CODELAKE_BENCH_RUNS` times (3).
fn bench_setting(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

#[tokio::test]
#[ignore = "benchmark: CODELAKE_BENCH_ROWS rows in SQL Server, run by hand"]
#[allow(clippy::print_stdout, reason = "the measurement is the output")]
async fn reconciliation_benchmark() {
    let rows: u64 = bench_setting("CODELAKE_BENCH_ROWS", "2000000").parse().unwrap();
    let since = bench_setting("CODELAKE_BENCH_SINCE", "2026-09-24 01:00:00");
    let runs: u32 = bench_setting("CODELAKE_BENCH_RUNS", "3").parse().unwrap();
    let mssql = start_mssql().await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    let started = Instant::now();
    seed(&host, port, rows).await;
    println!("seeded {rows} rows in {:?}", started.elapsed());

    let spill = tempfile::tempdir().unwrap();
    let warehouse = tempfile::tempdir().unwrap();
    let engine = attached(spill.path(), warehouse.path(), &host, port, 150).await;
    // Five days of 2 million rows: ~14 000 keys, as the nightly routine's pilot.
    copy_pilot(&engine, &since).await;
    let pilot = text(
        &run(
            &engine,
            "SELECT count(*), count(DISTINCT recebidasid) FROM hive.pilot.recebidas",
        )
        .await,
    );
    println!("pilot rows, keys: {pilot:?}");
    let explain = text(&run(&engine, &format!("EXPLAIN {RECONCILE}")).await);
    println!("===== EXPLAIN =====\n{}", explain.join("\n"));
    let listing = attached(spill.path(), warehouse.path(), &host, port, 20_000).await;
    let unfiltered = attached(spill.path(), warehouse.path(), &host, port, 150).await;
    unfiltered
        .context()
        .sql("SET datafusion.optimizer.enable_join_dynamic_filter_pushdown = false")
        .await
        .unwrap();
    for (name, engine) in [
        ("no dynamic filter", &unfiltered),
        ("150 keys listed (default)", &engine),
        ("20 000 keys listed", &listing),
    ] {
        let mut report = String::new();
        let mut result = Vec::new();
        exec(&host, port, "master", "DBCC FREEPROCCACHE").await;
        for attempt in 0..runs {
            let started = Instant::now();
            result = text(&run(engine, RECONCILE).await);
            let _ = writeln!(report, "run {attempt}: {:?}", started.elapsed());
        }
        let sent = direct_texts(&host, port, "warehouse", &remote_statements("recebidas")).await;
        let sent: Vec<String> = sent.iter().map(|s| s.chars().take(700).collect()).collect();
        println!(
            "===== {name} =====\n{report}result: {}\nsent to SQL Server:\n{}",
            digest(&result),
            sent.join("\n")
        );
    }
    let analyze = text(&run(&engine, &format!("EXPLAIN ANALYZE {RECONCILE}")).await);
    println!("===== EXPLAIN ANALYZE =====\n{}", analyze.join("\n"));
}
