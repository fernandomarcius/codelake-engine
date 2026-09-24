//! Independent red-team audit suite for F1 (SQL Server connector).
//!
//! Written by a tester who did not implement the connector. Each item in the
//! red-team report is proven or refuted against a real, disposable SQL Server:
//! for every check the `CodeLake` result is compared with the value that is
//! correct by the *engine's* semantics (computed by running the identical
//! expression over a local `VALUES` table, or read directly through tiberius),
//! and pushdown decisions are verified through `EXPLAIN`.
//!
//! Items are grouped so one container serves several related checks. All soft
//! checks are recorded and the suite fails once at the end, so a single run
//! shows the whole picture.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use crate::common;
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::{Connector, Engine};
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::util::display::array_value_to_string;
use secrecy::SecretString;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tiberius::{AuthMethod, Client, ColumnData, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const SA_PASSWORD: &str = "Audit-Only-1x";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);
const CONCURRENCY_TIMEOUT: Duration = Duration::from_secs(90);

type Rows = Vec<Vec<String>>;

// ---------------------------------------------------------------------------
// Soft checks: record everything, fail once at the end.
// ---------------------------------------------------------------------------

struct Checks {
    log: String,
    failed: Vec<String>,
}

impl Checks {
    fn new() -> Self {
        Self {
            log: String::new(),
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        let mark = if ok { "PASS" } else { "FAIL" };
        let _ = writeln!(self.log, "[{mark}] {name}\n        {detail}");
        if !ok {
            self.failed.push(name.to_owned());
        }
    }

    fn note(&mut self, text: impl std::fmt::Display) {
        let _ = writeln!(self.log, "[INFO] {text}");
    }

    #[allow(
        clippy::print_stdout,
        reason = "the check log is the evidence shown with --no-capture"
    )]
    fn finish(self, suite: &str) {
        println!("===== {suite} =====\n{}", self.log);
        assert!(
            self.failed.is_empty(),
            "{suite}: failed checks: {:?}\n{}",
            self.failed,
            self.log
        );
    }
}

// ---------------------------------------------------------------------------
// Containers and direct access to SQL Server.
// ---------------------------------------------------------------------------

async fn start_mssql(tag: &str) -> ContainerAsync<MssqlServer> {
    MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag(tag)
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

async fn exec(host: &str, port: u16, database: &str, batch: &str) {
    let mut c = client(host, port, database).await;
    if let Err(e) = c.simple_query(batch).await {
        panic!("`{batch}` failed: {e}");
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Renders a TDS value roughly the way Arrow's display renders the mapped type.
fn cell(value: &ColumnData<'_>) -> String {
    match value {
        ColumnData::U8(Some(v)) => v.to_string(),
        ColumnData::I16(Some(v)) => v.to_string(),
        ColumnData::I32(Some(v)) => v.to_string(),
        ColumnData::I64(Some(v)) => v.to_string(),
        ColumnData::F32(Some(v)) => v.to_string(),
        ColumnData::F64(Some(v)) => v.to_string(),
        ColumnData::Bit(Some(v)) => v.to_string(),
        ColumnData::String(Some(s)) => s.to_string(),
        ColumnData::Binary(Some(b)) => hex(b),
        other => format!("{other:?}"),
    }
}

async fn direct(host: &str, port: u16, database: &str, sql: &str) -> Rows {
    let mut c = client(host, port, database).await;
    let rows = c
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("direct `{sql}` failed: {e}"))
        .into_first_result()
        .await
        .unwrap_or_else(|e| panic!("direct `{sql}` failed: {e}"));
    rows.into_iter()
        .map(|row| row.into_iter().map(|v| cell(&v)).collect())
        .collect()
}

// ---------------------------------------------------------------------------
// Engine access (always through `Engine::sql`, the path clients use).
// ---------------------------------------------------------------------------

fn arrow_rows(batches: &[RecordBatch]) -> Rows {
    let mut out = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(i) {
                            "NULL".to_owned()
                        } else {
                            array_value_to_string(c, i).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

async fn try_rows(engine: &Engine, sql: &str) -> Result<Rows, String> {
    let df = engine.sql(sql).await.map_err(|e| format!("planning: {e}"))?;
    let batches = df.collect().await.map_err(|e| format!("executing: {e}"))?;
    Ok(arrow_rows(&batches))
}

/// Arrow type of the first column of `sql`.
async fn first_type(engine: &Engine, sql: &str) -> Result<String, String> {
    let df = engine.sql(sql).await.map_err(|e| format!("planning: {e}"))?;
    df.schema()
        .fields()
        .first()
        .map(|f| f.data_type().to_string())
        .ok_or_else(|| "no column".to_owned())
}

/// Physical plan text of `EXPLAIN <sql>`.
async fn physical_plan(engine: &Engine, sql: &str) -> Result<String, String> {
    let df = engine
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .map_err(|e| format!("planning EXPLAIN: {e}"))?;
    let batches = df.collect().await.map_err(|e| format!("EXPLAIN: {e}"))?;
    let mut text = String::new();
    for batch in &batches {
        for i in 0..batch.num_rows() {
            if array_value_to_string(batch.column(0), i).map_err(|e| e.to_string())? == "physical_plan" {
                text.push_str(&array_value_to_string(batch.column(1), i).map_err(|e| e.to_string())?);
            }
        }
    }
    Ok(text)
}

/// The SQL actually sent to SQL Server by each federated scan in `plan`.
fn remote_sqls(plan: &str) -> Vec<String> {
    const MARKERS: [&str; 5] = [
        " rewritten_sql_query=",
        " rewritten_ast_analyzer=",
        " rewritten_executor_sql=",
        " rewritten_logical_sql=",
        " base_sql=",
    ];
    plan.lines()
        .filter(|l| l.contains("VirtualExecutionPlan") && l.contains("name=sqlserver"))
        .filter_map(|l| {
            MARKERS
                .iter()
                .find_map(|m| l.find(m).map(|at| l[at + m.len()..].to_owned()))
        })
        .collect()
}

fn sqlserver_connector(host: &str, port: u16, database: &str, pool_size: u32) -> Arc<SqlServerConnector> {
    Arc::new(SqlServerConnector::new(
        "erp1".into(),
        SqlServerParams {
            host: host.to_owned(),
            port,
            database: database.to_owned(),
            user: "sa".into(),
            password: SecretString::from(SA_PASSWORD),
            encrypt: "required".into(),
            trust_server_certificate: true,
            pool_size,
        },
    ))
}

fn rows(expected: &[&[&str]]) -> Rows {
    expected
        .iter()
        .map(|r| r.iter().map(|c| (*c).to_owned()).collect())
        .collect()
}

// ---------------------------------------------------------------------------
// Items 1, 2, 3, 4, 8 and extras: one container, one engine.
// ---------------------------------------------------------------------------

async fn seed_wrong_results(host: &str, port: u16) {
    exec(host, port, "master", "CREATE DATABASE Audit").await;
    exec(
        host,
        port,
        "Audit",
        // Small readable values used for arithmetic; the large/maximum money
        // values live apart so an arithmetic scan never trips on them.
        "CREATE TABLE dbo.Dinheiro (Id INT NOT NULL PRIMARY KEY, Valor MONEY NULL);
         INSERT INTO dbo.Dinheiro VALUES (1, 10.0000), (2, 30.0000);
         CREATE TABLE dbo.DinheiroBig (Id INT NOT NULL PRIMARY KEY, Valor MONEY NULL);
         INSERT INTO dbo.DinheiroBig VALUES (3, 900000000000.1234), (4, 922337203685477.5807);
         CREATE TABLE dbo.Precisos (
           Id INT NOT NULL PRIMARY KEY, A DECIMAL(38,10) NULL, B DECIMAL(38,10) NULL,
           P DECIMAL(19,4) NULL, Q DECIMAL(19,4) NULL);
         INSERT INTO dbo.Precisos VALUES
           (1, 12345678901234567890.1234567891, 87654321098765432109.8765432109,
               1234567.8900, 9876.5432);
         CREATE TABLE dbo.Binarios (Id INT NOT NULL PRIMARY KEY, Vb VARBINARY(16) NULL, Fx BINARY(10) NULL);
         INSERT INTO dbo.Binarios VALUES
           (1, 0x01, 0x01000000000000000000),
           (2, 0x0100, 0x02000000000000000000);
         CREATE TABLE dbo.Temporal (
           Id INT NOT NULL PRIMARY KEY, Dt2 DATETIME2(7) NULL, Dtm DATETIME NULL, DtmB DATETIME NULL);
         INSERT INTO dbo.Temporal VALUES
           (1, '2026-01-01T00:00:00.1234567', '2020-01-01T00:00:00', '2019-01-01T00:00:00'),
           (2, '2026-01-01T00:00:00.1234561', '2020-01-01T00:00:00', '2021-01-01T00:00:00');
         CREATE TABLE dbo.Sessao (Id INT NOT NULL PRIMARY KEY);
         INSERT INTO dbo.Sessao VALUES (2), (3), (4);",
    )
    .await;
}

#[allow(
    clippy::too_many_lines,
    reason = "one audit item per block, read top to bottom"
)]
async fn wrong_results(tag: &str) {
    let mut checks = Checks::new();
    let mssql = start_mssql(tag).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    seed_wrong_results(&host, port).await;
    let version = direct(&host, port, "master", "SELECT CAST(@@VERSION AS NVARCHAR(200))").await;
    checks.note(format!("server: {}", version[0][0].lines().next().unwrap_or("")));

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Audit", 4) as Arc<dyn Connector>,
    )
    .await;

    // --- Item 1: money ---------------------------------------------------
    // valor/3 and valor*valor: exact decimal, computed locally (not pushed).
    let div = try_rows(&engine, "SELECT valor / 3 FROM erp1.dbo.dinheiro WHERE id = 1").await;
    let div_ref = try_rows(&engine, "SELECT CAST(10.0000 AS DECIMAL(19,4)) / 3").await;
    let div_type = first_type(&engine, "SELECT valor / 3 FROM erp1.dbo.dinheiro WHERE id = 1").await;
    checks.check(
        "1a money valor/3 equals the engine's own decimal division (10.0000/3)",
        div.is_ok() && div == div_ref,
        format!("got={div:?} engine_ref={div_ref:?}"),
    );
    checks.check(
        "1b money valor/3 is a Decimal, never a float",
        div_type.as_deref().is_ok_and(|t| t.starts_with("Decimal")),
        format!("type={div_type:?}"),
    );
    let mul = try_rows(
        &engine,
        "SELECT valor * valor FROM erp1.dbo.dinheiro WHERE id = 2",
    )
    .await;
    let mul_ref = try_rows(
        &engine,
        "SELECT CAST(30.0000 AS DECIMAL(19,4)) * CAST(30.0000 AS DECIMAL(19,4))",
    )
    .await;
    checks.check(
        "1c money valor*valor equals the engine's own decimal product (30*30=900)",
        mul.is_ok() && mul == mul_ref,
        format!("got={mul:?} engine_ref={mul_ref:?}"),
    );
    let div_plan = physical_plan(&engine, "SELECT valor / 3 FROM erp1.dbo.dinheiro WHERE id = 1").await;
    let div_remote = div_plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "1d money arithmetic is NOT pushed (remote SQL has no division)",
        div_remote.iter().all(|s| !s.contains('/')),
        format!("remote={div_remote:?}"),
    );
    // WHERE valor/3 > 3.33333: same rows as the engine computes locally.
    let filt = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.dinheiro WHERE valor / 3 > 3.33333 ORDER BY id",
    )
    .await;
    let filt_ref = try_rows(
        &engine,
        "SELECT id FROM (VALUES (1, CAST(10.0000 AS DECIMAL(19,4))), (2, CAST(30.0000 AS DECIMAL(19,4)))) \
         AS t(id, valor) WHERE valor / 3 > 3.33333 ORDER BY id",
    )
    .await;
    checks.note(format!(
        "SQL Server itself (money/int) would answer WHERE valor/3 > 3.33333 differently; engine ref={filt_ref:?}"
    ));
    checks.check(
        "1e WHERE money/3 > 3.33333 matches the engine's local decimal semantics",
        filt.is_ok() && filt == filt_ref,
        format!("got={filt:?} engine_ref={filt_ref:?}"),
    );
    // Large money read exactly; the true maximum is exact OR an explicit error.
    let big = try_rows(&engine, "SELECT valor FROM erp1.dbo.dinheirobig WHERE id = 3").await;
    checks.check(
        "1f money 900000000000.1234 read exactly (no silent drift)",
        big.as_ref() == Ok(&rows(&[&["900000000000.1234"]])),
        format!("got={big:?}"),
    );
    let max = try_rows(&engine, "SELECT valor FROM erp1.dbo.dinheirobig WHERE id = 4").await;
    checks.check(
        "1g money maximum 922337203685477.5807 is exact OR an explicit error, never wrong",
        match &max {
            Ok(r) => *r == rows(&[&["922337203685477.5807"]]),
            Err(_) => true,
        },
        format!("got={max:?}"),
    );

    // --- Item 2: decimal(38,10) and decimal(19,4) arithmetic -------------
    // The two addends were chosen to sum EXACTLY to 10^20: the integer parts add
    // to 99999999999999999999 and the fractions to 1.0000000000. A float would
    // never land there, so this pins the exact-decimal path.
    let add = try_rows(&engine, "SELECT a + b FROM erp1.dbo.precisos WHERE id = 1").await;
    let add_type = first_type(&engine, "SELECT a + b FROM erp1.dbo.precisos WHERE id = 1").await;
    checks.check(
        "2a decimal(38,10)+decimal(38,10) is the exact sum 100000000000000000000.0000000000",
        add.as_ref() == Ok(&rows(&[&["100000000000000000000.0000000000"]])),
        format!("got={add:?}"),
    );
    checks.check(
        "2b decimal(38,10) sum keeps the engine's scale (Decimal128, scale 10)",
        add_type.as_deref().is_ok_and(|t| t.contains(", 10)")),
        format!("type={add_type:?}"),
    );
    // 1234567.8900 * 9876.5432 = 12193263098.917848 exactly (scale 8).
    let prod = try_rows(&engine, "SELECT p * q FROM erp1.dbo.precisos WHERE id = 1").await;
    let prod_type = first_type(&engine, "SELECT p * q FROM erp1.dbo.precisos WHERE id = 1").await;
    checks.check(
        "2c decimal(19,4)*decimal(19,4) is the exact product 12193263098.91784800 (scale 8)",
        prod.as_ref() == Ok(&rows(&[&["12193263098.91784800"]])),
        format!("got={prod:?} type={prod_type:?}"),
    );

    // --- Item 3: varbinary(16) and binary(10) ----------------------------
    let vb_eq = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.binarios WHERE vb = X'01' ORDER BY id",
    )
    .await;
    checks.check(
        "3a varbinary WHERE vb = X'01' returns only 0x01, not 0x0100 (no zero-padding)",
        vb_eq.as_ref() == Ok(&rows(&[&["1"]])),
        format!("got={vb_eq:?}"),
    );
    let vb_plan = physical_plan(&engine, "SELECT id FROM erp1.dbo.binarios WHERE vb = X'01'").await;
    let vb_remote = vb_plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "3b variable-width varbinary comparison is NOT pushed",
        vb_remote.iter().all(|s| !s.to_lowercase().contains("0x01")),
        format!("remote={vb_remote:?}"),
    );
    let vb_group = try_rows(
        &engine,
        "SELECT vb, count(*) FROM erp1.dbo.binarios GROUP BY vb ORDER BY vb",
    )
    .await;
    checks.check(
        "3c GROUP BY varbinary keeps 0x01 and 0x0100 apart",
        vb_group.as_ref() == Ok(&rows(&[&["01", "1"], &["0100", "1"]])),
        format!("got={vb_group:?}"),
    );
    let vb_distinct = try_rows(&engine, "SELECT count(DISTINCT vb) FROM erp1.dbo.binarios").await;
    checks.check(
        "3d DISTINCT varbinary = 2",
        vb_distinct.as_ref() == Ok(&rows(&[&["2"]])),
        format!("got={vb_distinct:?}"),
    );
    let vb_minmax = try_rows(&engine, "SELECT min(vb), max(vb) FROM erp1.dbo.binarios").await;
    checks.check(
        "3e MIN/MAX(varbinary) follow the engine's byte order (0x01 / 0x0100)",
        vb_minmax.as_ref() == Ok(&rows(&[&["01", "0100"]])),
        format!("got={vb_minmax:?}"),
    );
    let fx_plan = physical_plan(
        &engine,
        "SELECT id FROM erp1.dbo.binarios WHERE fx = X'01000000000000000000'",
    )
    .await;
    let fx_remote = fx_plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "3f fixed binary(10) equality with a 10-byte literal stays PUSHED",
        fx_remote
            .iter()
            .any(|s| s.to_lowercase().contains("0x01000000000000000000")),
        format!("remote={fx_remote:?}"),
    );
    let fx_eq = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.binarios WHERE fx = X'01000000000000000000'",
    )
    .await;
    checks.check(
        "3g fixed binary(10) equality returns the right row",
        fx_eq.as_ref() == Ok(&rows(&[&["1"]])),
        format!("got={fx_eq:?}"),
    );
    // Extra (not in the audit): variable-width IN-list stays local and exact.
    let vb_in = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.binarios WHERE vb IN (X'01', X'0100') ORDER BY id",
    )
    .await;
    checks.check(
        "X1 varbinary IN (X'01', X'0100') returns both rows exactly",
        vb_in.as_ref() == Ok(&rows(&[&["1"], &["2"]])),
        format!("got={vb_in:?}"),
    );

    // --- Item 4: datetime2(7) --------------------------------------------
    let dt2_group = try_rows(
        &engine,
        "SELECT count(*) FROM (SELECT dt2 FROM erp1.dbo.temporal GROUP BY dt2) t",
    )
    .await;
    checks.check(
        "4a rows differing only in the 7th datetime2 digit collapse to one group (µs truncation)",
        dt2_group.as_ref() == Ok(&rows(&[&["1"]])),
        format!("got={dt2_group:?}"),
    );
    let dt2_group_plan = physical_plan(&engine, "SELECT dt2 FROM erp1.dbo.temporal GROUP BY dt2").await;
    let dt2_group_remote = dt2_group_plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "4b GROUP BY datetime2(7) is NOT pushed (engine truncates 100 ns)",
        dt2_group_remote
            .iter()
            .all(|s| !s.to_lowercase().contains("group by")),
        format!("remote={dt2_group_remote:?}"),
    );
    let cross = physical_plan(&engine, "SELECT id FROM erp1.dbo.temporal WHERE dt2 > dtm").await;
    let cross_remote = cross.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "4c comparison datetime2(7) > datetime (different SQL types) is NOT pushed",
        cross_remote.iter().all(|s| !s.contains("WHERE")),
        format!("remote={cross_remote:?}"),
    );
    let cross_rows = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.temporal WHERE dt2 > dtm ORDER BY id",
    )
    .await;
    checks.check(
        "4d dt2 > dtm still returns the correct rows (both, year 2026 > 2020)",
        cross_rows.as_ref() == Ok(&rows(&[&["1"], &["2"]])),
        format!("got={cross_rows:?}"),
    );
    let same = physical_plan(&engine, "SELECT id FROM erp1.dbo.temporal WHERE dtm > dtmb").await;
    let same_remote = same.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "4e comparison between two datetime columns (same type) IS pushed",
        same_remote
            .iter()
            .any(|s| s.contains("WHERE") && s.to_lowercase().contains("[dtm]")),
        format!("remote={same_remote:?}"),
    );
    let same_rows = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.temporal WHERE dtm > dtmb ORDER BY id",
    )
    .await;
    checks.check(
        "4f dtm > dtmb returns only row 1",
        same_rows.as_ref() == Ok(&rows(&[&["1"]])),
        format!("got={same_rows:?}"),
    );

    // --- Item 8: session options -----------------------------------------
    let not_in = try_rows(
        &engine,
        "SELECT count(*) FROM erp1.dbo.sessao WHERE id NOT IN (1, NULL)",
    )
    .await;
    checks.check(
        "8a WHERE id NOT IN (1, NULL) returns 0 rows (three-valued logic)",
        not_in.as_ref() == Ok(&rows(&[&["0"]])),
        format!("got={not_in:?}"),
    );
    let divzero_plan = physical_plan(&engine, "SELECT id / 0 FROM erp1.dbo.sessao").await;
    let divzero_remote = divzero_plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.note(format!("id/0 remote SQL: {divzero_remote:?}"));
    let div0 = try_rows(&engine, "SELECT id / 0 FROM erp1.dbo.sessao").await;
    checks.check(
        "8b integer division by zero is an error, not a silent NULL (ARITHABORT/ANSI_WARNINGS ON)",
        div0.is_err(),
        format!("got={div0:?}"),
    );

    checks.finish(&format!("F1 audit: wrong results on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_audit_wrong_results_2022() {
    wrong_results("2022-latest").await;
}

// ---------------------------------------------------------------------------
// Item 5: injection through object and alias names.
// ---------------------------------------------------------------------------

async fn injection(tag: &str) {
    let mut checks = Checks::new();
    let mssql = start_mssql(tag).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE Inj").await;
    // A sacrificial table an injection would try to drop, plus a table and a
    // column whose real names contain `]`.
    exec(
        &host,
        port,
        "Inj",
        "CREATE TABLE dbo.alvo (Id INT NOT NULL PRIMARY KEY);
         INSERT INTO dbo.alvo VALUES (1);
         CREATE TABLE dbo.[x]]; DROP TABLE dbo.alvo;--] (Id INT NOT NULL PRIMARY KEY);
         INSERT INTO dbo.[x]]; DROP TABLE dbo.alvo;--] VALUES (7);
         CREATE TABLE dbo.ColBracket (Id INT NOT NULL PRIMARY KEY, [c]]d] INT NULL);
         INSERT INTO dbo.ColBracket VALUES (1, 42);",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Inj", 4) as Arc<dyn Connector>,
    )
    .await;

    // The `]`-bearing table and column are not exposed by the catalog.
    let tables = try_rows(
        &engine,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'erp1'",
    )
    .await;
    checks.note(format!(
        "engine-visible catalog probe (may be unsupported): {tables:?}"
    ));
    // Selecting the bracketed table by its lower-case name must not resolve.
    let bad_table = try_rows(&engine, "SELECT id FROM erp1.dbo.\"x]; drop table dbo.alvo;--\"").await;
    checks.check(
        "5a the `]`-bearing table is not exposed (query does not resolve)",
        bad_table.is_err(),
        format!("got={bad_table:?}"),
    );
    // The safe column of ColBracket is readable; the `]` column is absent.
    let colbracket = try_rows(&engine, "SELECT id FROM erp1.dbo.colbracket ORDER BY id").await;
    checks.check(
        "5b table with a `]` column is still usable through its safe columns",
        colbracket.as_ref() == Ok(&rows(&[&["1"]])),
        format!("got={colbracket:?}"),
    );
    let bad_col = try_rows(&engine, "SELECT \"c]d\" FROM erp1.dbo.colbracket").await;
    checks.check(
        "5c the `]` column is not exposed (query does not resolve)",
        bad_col.is_err(),
        format!("got={bad_col:?}"),
    );

    // A user alias carrying `]` and a DROP: must run at most one SELECT.
    let aliased = try_rows(
        &engine,
        "SELECT id AS \"a]; DROP TABLE dbo.alvo;--b\" FROM erp1.dbo.colbracket ORDER BY 1",
    )
    .await;
    checks.check(
        "5d alias containing `]` and a DROP either errors or returns only a SELECT result",
        match &aliased {
            Ok(r) => *r == rows(&[&["1"]]),
            Err(_) => true,
        },
        format!("got={aliased:?}"),
    );

    // After every attempt, dbo.alvo must still exist with its row.
    let alvo = direct(&host, port, "Inj", "SELECT Id FROM dbo.alvo").await;
    checks.check(
        "5e dbo.alvo still exists after all injection attempts",
        alvo == rows(&[&["1"]]),
        format!("got={alvo:?}"),
    );

    checks.finish(&format!("F1 audit: injection on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_audit_injection_2022() {
    injection("2022-latest").await;
}

// ---------------------------------------------------------------------------
// Items 6 and 10: pooling, concurrency and consumer cancellation.
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines, reason = "two pool scenarios sharing one container")]
async fn pool_and_cancel(tag: &str) {
    let mut checks = Checks::new();
    let mssql = start_mssql(tag).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE Pool").await;
    exec(
        &host,
        port,
        "Pool",
        "CREATE TABLE dbo.A (Id INT NOT NULL PRIMARY KEY, K NVARCHAR(10) NOT NULL);
         CREATE TABLE dbo.B (Id INT NOT NULL PRIMARY KEY, K NVARCHAR(10) NOT NULL);
         CREATE TABLE dbo.C (Id INT NOT NULL PRIMARY KEY, K NVARCHAR(10) NOT NULL);
         INSERT INTO dbo.A VALUES (1, N'x'), (2, N'y');
         INSERT INTO dbo.B VALUES (1, N'x'), (2, N'y');
         INSERT INTO dbo.C VALUES (1, N'x'), (2, N'y');
         CREATE TABLE dbo.Muitas (Id INT NOT NULL PRIMARY KEY);
         WITH n AS (SELECT TOP (3000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS r
                    FROM sys.all_objects a CROSS JOIN sys.all_objects b)
         INSERT INTO dbo.Muitas SELECT r FROM n;",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    // Pool of only 2 connections: three text joins force three local scans.
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Pool", 2) as Arc<dyn Connector>,
    )
    .await;

    // Item 6: a query with three joins on text (each side a separate remote
    // scan, joined locally) plus several such queries concurrently.
    let join_sql = "SELECT count(*) FROM erp1.dbo.a JOIN erp1.dbo.b ON a.k = b.k \
         JOIN erp1.dbo.c ON b.k = c.k";
    let single = tokio::time::timeout(CONCURRENCY_TIMEOUT, try_rows(&engine, join_sql)).await;
    checks.check(
        "6a three text joins (3 local scans) finish on a pool of 2, no deadlock",
        matches!(&single, Ok(Ok(r)) if r == &rows(&[&["2"]])),
        format!("got={single:?}"),
    );
    let concurrent = tokio::time::timeout(CONCURRENCY_TIMEOUT, async {
        tokio::join!(
            try_rows(&engine, join_sql),
            try_rows(&engine, join_sql),
            try_rows(&engine, join_sql),
            try_rows(&engine, join_sql),
        )
    })
    .await;
    checks.check(
        "6b four concurrent 3-join queries all succeed on a pool of 2 (no timeout/deadlock)",
        match &concurrent {
            Ok((a, b, c, d)) => [a, b, c, d]
                .iter()
                .all(|r| r.as_ref().map(Vec::as_slice) == Ok(rows(&[&["2"]]).as_slice())),
            Err(_) => false,
        },
        format!("got={concurrent:?}"),
    );

    // Item 10: consume a big result only in part (local LIMIT ... OFFSET, which
    // is not pushed), then run another query on the same connector.
    let partial = try_rows(&engine, "SELECT id FROM erp1.dbo.muitas LIMIT 5 OFFSET 1").await;
    checks.check(
        "10a partial consumption of a 3000-row scan returns 5 rows",
        partial.as_ref().is_ok_and(|r| r.len() == 5),
        format!("got_len={:?}", partial.as_ref().map(Vec::len)),
    );
    let partial_plan = physical_plan(&engine, "SELECT id FROM erp1.dbo.muitas LIMIT 5 OFFSET 1").await;
    let partial_remote = partial_plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.note(format!(
        "partial-consume remote SQL (must NOT carry TOP, so the stream is abandoned mid-way): {partial_remote:?}"
    ));
    checks.check(
        "10b the abandoned scan was not limited remotely (TOP absent → connection poisoned)",
        partial_remote.iter().all(|s| !s.to_uppercase().contains("TOP")),
        format!("remote={partial_remote:?}"),
    );
    // Run several more queries: a poisoned connection would be handed out and fail.
    let after = try_rows(&engine, "SELECT count(*) FROM erp1.dbo.muitas").await;
    checks.check(
        "10c a follow-up query on the same connector works (poisoned conn discarded)",
        after.as_ref() == Ok(&rows(&[&["3000"]])),
        format!("got={after:?}"),
    );
    let after2 = tokio::time::timeout(CONCURRENCY_TIMEOUT, async {
        tokio::join!(
            try_rows(&engine, "SELECT count(*) FROM erp1.dbo.muitas"),
            try_rows(&engine, "SELECT id FROM erp1.dbo.muitas LIMIT 3 OFFSET 1"),
            try_rows(&engine, "SELECT count(*) FROM erp1.dbo.a"),
        )
    })
    .await;
    checks.check(
        "10d repeated partial + full queries keep working (pool not exhausted by poisoning)",
        matches!(&after2, Ok((a, _, c)) if a.as_ref() == Ok(&rows(&[&["3000"]])) && c.as_ref() == Ok(&rows(&[&["2"]]))),
        format!("got={after2:?}"),
    );

    checks.finish(&format!("F1 audit: pool and cancellation on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_audit_pool_and_cancel_2022() {
    pool_and_cancel("2022-latest").await;
}

// ---------------------------------------------------------------------------
// Item 7: unsupported types alongside normal columns.
// ---------------------------------------------------------------------------

async fn unsupported_types(tag: &str) {
    let mut checks = Checks::new();
    let mssql = start_mssql(tag).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE Unsup").await;
    exec(
        &host,
        port,
        "Unsup",
        "CREATE TABLE dbo.Mistos (
           Id INT NOT NULL PRIMARY KEY, Nome NVARCHAR(30) NULL,
           Regiao GEOGRAPHY NULL, Qualquer SQL_VARIANT NULL);
         INSERT INTO dbo.Mistos (Id, Nome, Regiao, Qualquer) VALUES
           (1, N'alpha', geography::Point(1, 1, 4326), CAST(10 AS SQL_VARIANT)),
           (2, N'beta',  NULL, NULL);",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Unsup", 4) as Arc<dyn Connector>,
    )
    .await;

    // The exposed schema must contain only the normal columns.
    let schema = engine.sql("SELECT * FROM erp1.dbo.mistos").await.map(|df| {
        df.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    });
    checks.check(
        "7a geography and sql_variant are not exposed; only id and nome remain",
        schema.as_ref().is_ok_and(|s| *s == ["id", "nome"]),
        format!("schema={schema:?}"),
    );
    let all = try_rows(&engine, "SELECT id, nome FROM erp1.dbo.mistos ORDER BY id").await;
    checks.check(
        "7b SELECT of the normal columns works despite the unsupported ones",
        all.as_ref() == Ok(&rows(&[&["1", "alpha"], &["2", "beta"]])),
        format!("got={all:?}"),
    );
    let scan = try_rows(&engine, "SELECT id FROM erp1.dbo.mistos WHERE nome = 'beta'").await;
    checks.check(
        "7c a text WHERE (isolated scan) works over the table with unsupported columns",
        scan.as_ref() == Ok(&rows(&[&["2"]])),
        format!("got={scan:?}"),
    );

    checks.finish(&format!("F1 audit: unsupported types on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_audit_unsupported_types_2022() {
    unsupported_types("2022-latest").await;
}

// ---------------------------------------------------------------------------
// Item 9: case-sensitive collation, join with alias and derived subquery.
// ---------------------------------------------------------------------------

async fn case_sensitive(tag: &str) {
    let mut checks = Checks::new();
    let mssql = start_mssql(tag).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(
        &host,
        port,
        "master",
        "CREATE DATABASE Cs COLLATE Latin1_General_CS_AS",
    )
    .await;
    // Join key columns differ only in case between the two tables (Id vs ID):
    // both are exposed as lower-case `id` and mapped back per table.
    exec(
        &host,
        port,
        "Cs",
        "CREATE TABLE dbo.A (Id INT NOT NULL PRIMARY KEY, Nome NVARCHAR(20) NOT NULL);
         CREATE TABLE dbo.B (ID INT NOT NULL PRIMARY KEY, Valor INT NOT NULL);
         INSERT INTO dbo.A VALUES (1, N'Ana'), (2, N'Bruno'), (3, N'Caio');
         INSERT INTO dbo.B VALUES (1, 100), (2, 200);",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Cs", 4) as Arc<dyn Connector>,
    )
    .await;

    let joined = try_rows(
        &engine,
        "SELECT a.id AS aid, a.nome, s.valor \
         FROM erp1.dbo.a AS a \
         JOIN (SELECT id, valor FROM erp1.dbo.b) AS s ON s.id = a.id \
         ORDER BY a.id",
    )
    .await;
    checks.check(
        "9a case-sensitive DB: join (alias + derived subquery) on Id/ID returns the matching rows",
        joined.as_ref() == Ok(&rows(&[&["1", "Ana", "100"], &["2", "Bruno", "200"]])),
        format!("got={joined:?}"),
    );
    let count = try_rows(
        &engine,
        "SELECT count(*) FROM erp1.dbo.a AS a JOIN erp1.dbo.b AS b ON a.id = b.id",
    )
    .await;
    checks.check(
        "9b case-sensitive DB: row count of the join is 2",
        count.as_ref() == Ok(&rows(&[&["2"]])),
        format!("got={count:?}"),
    );

    checks.finish(&format!("F1 audit: case-sensitive database on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_audit_case_sensitive_2022() {
    case_sensitive("2022-latest").await;
}

// ---------------------------------------------------------------------------
// Item 11: non-deterministic scalar subqueries are not pre-evaluated.
// ---------------------------------------------------------------------------

async fn pre_evaluation(tag: &str) {
    let mut checks = Checks::new();
    let mssql = start_mssql(tag).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE Pre").await;
    exec(
        &host,
        port,
        "Pre",
        "CREATE TABLE dbo.Numeros (Id INT NOT NULL PRIMARY KEY);
         INSERT INTO dbo.Numeros VALUES (1), (2), (3), (4), (5);",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Pre", 4) as Arc<dyn Connector>,
    )
    .await;
    // A local watermark table for the subquery.
    engine
        .context()
        .sql("CREATE TABLE wm AS VALUES (2)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Deterministic subquery: pre-evaluated, so EXPLAIN shows no subquery.
    let det_sql = "SELECT id FROM erp1.dbo.numeros \
         WHERE id > (SELECT max(column1) FROM wm) ORDER BY id";
    let det_plan = physical_plan(&engine, det_sql).await;
    checks.check(
        "11a deterministic scalar subquery is pre-evaluated (no Subquery/nested loop in EXPLAIN)",
        det_plan
            .as_deref()
            .is_ok_and(|p| !p.contains("SubqueryExec") && !p.to_lowercase().contains("subquery")),
        format!("plan={det_plan:?}"),
    );
    let det = try_rows(&engine, det_sql).await;
    checks.check(
        "11b deterministic subquery result is correct (id > 2 → 3,4,5)",
        det.as_ref() == Ok(&rows(&[&["3"], &["4"], &["5"]])),
        format!("got={det:?}"),
    );

    // Non-deterministic subquery (now()): must NOT be pre-evaluated.
    let nondet_sql = "SELECT id FROM erp1.dbo.numeros \
         WHERE id > (SELECT max(column1) FROM wm WHERE now() IS NOT NULL) ORDER BY id";
    let nondet_plan = physical_plan(&engine, nondet_sql).await;
    checks.check(
        "11c non-deterministic subquery (now()) is left in the plan (still a subquery/join)",
        nondet_plan.as_deref().is_ok_and(|p| {
            let p = p.to_lowercase();
            p.contains("subquery")
                || p.contains("nestedloopjoin")
                || p.contains("hashjoin")
                || p.contains("join")
        }),
        format!("plan={nondet_plan:?}"),
    );
    let nondet = try_rows(&engine, nondet_sql).await;
    checks.check(
        "11d non-deterministic subquery result is still correct (id > 2 → 3,4,5)",
        nondet.as_ref() == Ok(&rows(&[&["3"], &["4"], &["5"]])),
        format!("got={nondet:?}"),
    );

    checks.finish(&format!("F1 audit: subquery pre-evaluation on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_audit_pre_evaluation_2022() {
    pre_evaluation("2022-latest").await;
}
