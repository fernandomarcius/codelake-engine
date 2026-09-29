//! Independent acceptance suite for F1 (SQL Server connector).
//!
//! Written by a tester who did not implement the connector. Every check asserts
//! rows, not only plans: expected values come either from literals written here
//! or from reading the same table directly through `tiberius`.
//!
//! Each acceptance test runs against one SQL Server version and records every check
//! before failing, so one run shows the whole picture.

use std::fmt::Write as _;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common;
use codelake_connectors::postgres::{PostgresConnector, PostgresParams};
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use codelake_core::{Connector, Engine};
use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::util::display::array_value_to_string;
use secrecy::SecretString;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};
use tiberius::{AuthMethod, Client, ColumnData, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const SA_PASSWORD: &str = "Acceptance-Only-1";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);
const CDC_TIMEOUT: Duration = Duration::from_secs(120);
const OUTAGE_TIMEOUT: Duration = Duration::from_secs(60);

type Rows = Vec<Vec<String>>;

// ---------------------------------------------------------------------------
// Soft checks: record everything, fail once at the end.
// ---------------------------------------------------------------------------

/// Which checks a run enforces: result rows (and the text-safety plan checks)
/// or pushdown of safe predicates.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Rows,
    Pushdown,
}

struct Checks {
    scope: Scope,
    log: String,
    failed: Vec<String>,
}

impl Checks {
    fn new(scope: Scope) -> Self {
        Self {
            scope,
            log: String::new(),
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        self.record(Scope::Rows, name, ok, detail);
    }

    fn pushdown(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        self.record(Scope::Pushdown, name, ok, detail);
    }

    fn record(&mut self, scope: Scope, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if scope != self.scope {
            return;
        }
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

async fn start_mssql(tag: &str, agent: bool) -> ContainerAsync<MssqlServer> {
    let request = MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag(tag)
        .with_startup_timeout(STARTUP_TIMEOUT);
    let request = if agent {
        request.with_env_var("MSSQL_AGENT_ENABLED", "true")
    } else {
        request
    };
    request.start().await.unwrap()
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

async fn try_exec(host: &str, port: u16, database: &str, batch: &str) -> Result<(), tiberius::error::Error> {
    let mut c = client(host, port, database).await;
    c.simple_query(batch).await?.into_results().await?;
    Ok(())
}

async fn exec(host: &str, port: u16, database: &str, batch: &str) {
    try_exec(host, port, database, batch)
        .await
        .unwrap_or_else(|e| panic!("`{batch}` failed: {e}"));
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn decimal(value: i128, scale: u8) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    if scale == 0 {
        return format!("{sign}{abs}");
    }
    let pow = 10_u128.pow(u32::from(scale));
    format!("{sign}{}.{:0w$}", abs / pow, abs % pow, w = usize::from(scale))
}

/// Renders a TDS value the way Arrow's display renders the mapped type.
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
        ColumnData::Guid(Some(g)) => g.to_string().to_uppercase(),
        ColumnData::Binary(Some(b)) => hex(b),
        ColumnData::Numeric(Some(n)) => decimal(n.value(), n.scale()),
        ColumnData::U8(None)
        | ColumnData::I16(None)
        | ColumnData::I32(None)
        | ColumnData::I64(None)
        | ColumnData::F32(None)
        | ColumnData::F64(None)
        | ColumnData::Bit(None)
        | ColumnData::String(None)
        | ColumnData::Guid(None)
        | ColumnData::Binary(None)
        | ColumnData::Numeric(None)
        | ColumnData::Xml(None)
        | ColumnData::DateTime(None)
        | ColumnData::SmallDateTime(None)
        | ColumnData::Time(None)
        | ColumnData::Date(None)
        | ColumnData::DateTime2(None)
        | ColumnData::DateTimeOffset(None) => "NULL".to_owned(),
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

/// Polls `probe` until it returns true or `timeout` elapses.
async fn wait_until<F, Fut>(timeout: Duration, what: &str, mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if probe().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
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

/// The SQL actually sent to SQL Server by each federated scan in `plan`
/// (the last rewrite shown on each `VirtualExecutionPlan` line).
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

fn sqlserver_connector(host: &str, port: u16, database: &str) -> Arc<SqlServerConnector> {
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
            pool_size: 4,
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
// DoD (items 1–6).
// ---------------------------------------------------------------------------

const CT_COLUMNS: &str =
    "\"__$start_lsn\", \"__$seqval\", \"__$operation\", \"__$update_mask\", id, situacao, valor";
const CT_COLUMNS_TSQL: &str =
    "[__$start_lsn], [__$seqval], [__$operation], [__$update_mask], [Id], [Situacao], [Valor]";

async fn seed_sqlserver(host: &str, port: u16) {
    exec(host, port, "master", "CREATE DATABASE Aceite").await;
    exec(host, port, "Aceite", "EXEC sys.sp_cdc_enable_db").await;
    exec(
        host,
        port,
        "Aceite",
        "CREATE TABLE dbo.Contratos (Id INT NOT NULL PRIMARY KEY, Situacao NVARCHAR(20) NULL, \
           Valor DECIMAL(18,2) NULL);",
    )
    .await;
    // Enabling a table adds the capture job, refused while the Agent is still
    // starting (error 14258): retry until it is accepted.
    wait_until(CDC_TIMEOUT, "SQL Server Agent to accept the CDC job", || async {
        try_exec(
            host,
            port,
            "Aceite",
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Contratos', \
               @role_name = NULL, @supports_net_changes = 0;",
        )
        .await
        .is_ok()
    })
    .await;
    // One statement per transaction: every change gets its own LSN.
    exec(
        host,
        port,
        "Aceite",
        "INSERT INTO dbo.Contratos VALUES (1, N'ativo', 10.50);
         INSERT INTO dbo.Contratos VALUES (2, N'Pendente', NULL);
         INSERT INTO dbo.Contratos VALUES (3, N'ação', -0.01);
         UPDATE dbo.Contratos SET Situacao = N'ATIVO', Valor = 99.99 WHERE Id = 2;
         DELETE FROM dbo.Contratos WHERE Id = 3;",
    )
    .await;
    exec(
        host,
        port,
        "Aceite",
        "CREATE TABLE dbo.Parceiros (Id INT NOT NULL PRIMARY KEY, Situacao NVARCHAR(20) NULL, Saldo INT NOT NULL);
         INSERT INTO dbo.Parceiros VALUES
           (1, N'ativo', 100), (2, N'ATIVO', 200), (3, N'ativo ', 300),
           (4, N'ativo', 400), (5, N'inativo', 500), (6, NULL, 600);
         CREATE TABLE dbo.Tipos (
           Id INT NOT NULL PRIMARY KEY, I INT NULL, B BIGINT NULL, D DECIMAL(18,2) NULL,
           S NVARCHAR(50) NULL, Dt DATETIME2(7) NULL, Dd DATE NULL, Bt BIT NULL,
           G UNIQUEIDENTIFIER NULL, Vb VARBINARY(16) NULL);
         INSERT INTO dbo.Tipos VALUES
           (1, -2147483648, 9223372036854775807, 1234567890123456.78, N'ação São Paulo — ü ñ 日本',
            '9999-12-31T23:59:59.9999999', '9999-12-31', 1, '6F9619FF-8B86-D011-B42D-00C04FC964FF', 0xDEADBEEF00),
           (2, 2147483647, -9223372036854775808, -0.01, N'',
            '0001-01-01T00:00:00', '0001-01-01', 0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 0x),
           (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
           (4, 0, 0, 0.00, N'x', '2026-01-01T10:00:00.1234567', '2026-02-28', 1,
            '00000000-0000-0000-0000-000000000001', 0x00);",
    )
    .await;
}

async fn seed_postgres(host: &str, port: u16) {
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (pg, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    pg.batch_execute(
        "CREATE TABLE pedidos (id INT PRIMARY KEY, parceiro_id INT NOT NULL, total NUMERIC(12,2) NOT NULL, status TEXT NOT NULL);
         INSERT INTO pedidos VALUES
           (100, 1, 10.00, 'pago'), (101, 2, 20.00, 'pago'), (102, 3, 30.00, 'pago'),
           (103, 4, 40.00, 'pago'), (104, 4, 41.00, 'PAGO'), (105, 5, 50.00, 'pago'),
           (106, 6, 60.00, 'pago');",
    )
    .await
    .unwrap();
}

#[allow(
    clippy::too_many_lines,
    reason = "one acceptance scenario per SQL Server version, read top to bottom"
)]
async fn dod(tag: &str, scope: Scope) {
    let mut checks = Checks::new(scope);
    let (mssql, pg) = tokio::join!(
        start_mssql(tag, true),
        Postgres::default().with_tag("17-alpine").start()
    );
    let pg = pg.unwrap();
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();
    seed_sqlserver(&host, port).await;
    seed_postgres(&host, pg_port).await;
    let version = direct(&host, port, "master", "SELECT CAST(@@VERSION AS NVARCHAR(200))").await;
    checks.note(format!("server: {}", version[0][0].lines().next().unwrap_or("")));

    // Wait for the CDC capture job: 3 inserts + update (2 rows) + delete.
    wait_until(CDC_TIMEOUT, "CDC capture", || async {
        direct(
            &host,
            port,
            "Aceite",
            "SELECT COUNT_BIG(*) FROM cdc.dbo_Contratos_CT",
        )
        .await[0][0]
            == "6"
    })
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    let source = sqlserver_connector(&host, port, "Aceite");
    common::attach(&mut engine, Arc::clone(&source) as Arc<dyn Connector>).await;
    common::attach(
        &mut engine,
        Arc::new(PostgresConnector::new(
            "erp".into(),
            PostgresParams {
                host: host.clone(),
                port: pg_port,
                database: "postgres".into(),
                user: "postgres".into(),
                password: SecretString::from("postgres"),
                sslmode: "disable".into(),
            },
        )),
    )
    .await;

    // 1. CDC rows through the engine match the change table read directly.
    let expected_ct = direct(
        &host,
        port,
        "Aceite",
        &format!("SELECT {CT_COLUMNS_TSQL} FROM cdc.dbo_Contratos_CT ORDER BY [__$start_lsn], [__$seqval], [__$operation]"),
    )
    .await;
    let got_ct = try_rows(
        &engine,
        &format!(
            "SELECT {CT_COLUMNS} FROM erp1.cdc.dbo_contratos_ct \
             ORDER BY \"__$start_lsn\", \"__$seqval\", \"__$operation\""
        ),
    )
    .await;
    checks.check(
        "1a CDC change table rows equal direct tiberius read",
        got_ct.as_ref().is_ok_and(|r| *r == expected_ct && r.len() == 6),
        format!("engine={got_ct:?}\n        direct={expected_ct:?}"),
    );
    let ops = try_rows(
        &engine,
        "SELECT \"__$operation\" AS op, count(*) AS n FROM erp1.cdc.dbo_contratos_ct GROUP BY 1 ORDER BY 1",
    )
    .await;
    checks.check(
        "1b CDC count per __$operation (1=delete,2=insert,3=before,4=after)",
        ops.as_ref() == Ok(&rows(&[&["1", "1"], &["2", "3"], &["3", "1"], &["4", "1"]])),
        format!("{ops:?}"),
    );
    let values = try_rows(
        &engine,
        "SELECT \"__$operation\", id, situacao, valor FROM erp1.cdc.dbo_contratos_ct \
         ORDER BY \"__$start_lsn\", \"__$seqval\", \"__$operation\"",
    )
    .await;
    checks.check(
        "1c CDC column values equal what was written",
        values.as_ref()
            == Ok(&rows(&[
                &["2", "1", "ativo", "10.50"],
                &["2", "2", "Pendente", "NULL"],
                &["2", "3", "ação", "-0.01"],
                &["3", "2", "Pendente", "NULL"],
                &["4", "2", "ATIVO", "99.99"],
                &["1", "3", "ação", "-0.01"],
            ])),
        format!("{values:?}"),
    );

    // 2. LSN watermark pre-evaluated and pushed as a T-SQL binary constant.
    let lsns = direct(
        &host,
        port,
        "Aceite",
        "SELECT DISTINCT [__$start_lsn] FROM cdc.dbo_Contratos_CT ORDER BY [__$start_lsn]",
    )
    .await;
    checks.check(
        "2a one LSN per committed statement (setup)",
        lsns.len() == 5,
        format!("{lsns:?}"),
    );
    let watermark = lsns[2][0].clone(); // third insert
    engine
        .context()
        .sql(&format!("CREATE TABLE wm AS VALUES (X'{watermark}')"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let incremental = format!(
        "SELECT {CT_COLUMNS} FROM erp1.cdc.dbo_contratos_ct \
         WHERE \"__$start_lsn\" > (SELECT COALESCE(max(column1), X'00') FROM wm)"
    );
    let mut got_inc = try_rows(&engine, &incremental).await;
    if let Ok(r) = got_inc.as_mut() {
        r.sort();
    }
    let mut expected_inc = direct(
        &host,
        port,
        "Aceite",
        &format!("SELECT {CT_COLUMNS_TSQL} FROM cdc.dbo_Contratos_CT WHERE [__$start_lsn] > 0x{watermark}"),
    )
    .await;
    expected_inc.sort();
    checks.check(
        "2b watermark returns exactly the rows with a greater LSN",
        got_inc.as_ref().is_ok_and(|r| *r == expected_inc && r.len() == 3),
        format!("engine={got_inc:?}\n        direct={expected_inc:?}"),
    );
    let plan = physical_plan(&engine, &incremental).await;
    let remote = plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.pushdown(
        "2c EXPLAIN: remote SQL carries the watermark as a 0x… constant",
        remote.len() == 1
            && remote[0].to_lowercase().contains(&format!("> 0x{watermark}"))
            && remote[0].contains("[cdc].[dbo_Contratos_CT]"),
        format!("remote={remote:?}\n        plan={plan:?}"),
    );
    let by_operation = "SELECT id FROM erp1.cdc.dbo_contratos_ct WHERE \"__$operation\" = 2";
    let plan = physical_plan(&engine, by_operation).await;
    let remote = plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.pushdown(
        "2e EXPLAIN: numeric filter on the change table is in the remote SQL",
        remote.len() == 1 && remote[0].contains("WHERE") && remote[0].contains("= 2"),
        format!("remote={remote:?}"),
    );
    let empty_wm = try_rows(
        &engine,
        "SELECT count(*) FROM erp1.cdc.dbo_contratos_ct \
         WHERE \"__$start_lsn\" > (SELECT COALESCE(max(column1), X'00') FROM wm WHERE false)",
    )
    .await;
    checks.check(
        "2d empty watermark (COALESCE X'00') reads everything",
        empty_wm.as_ref() == Ok(&rows(&[&["6"]])),
        format!("{empty_wm:?}"),
    );

    // 3. JOIN SQL Server × PostgreSQL with numeric and text WHERE.
    let join = |situacao: &str| {
        format!(
            "SELECT c.id AS parceiro, p.id AS pedido, p.total \
             FROM erp1.dbo.parceiros c JOIN erp.public.pedidos p ON p.parceiro_id = c.id \
             WHERE c.saldo >= 200 AND c.situacao = '{situacao}' AND p.status = 'pago' \
             ORDER BY p.id"
        )
    };
    let ci = direct(
        &host,
        port,
        "Aceite",
        "SELECT Id FROM dbo.Parceiros WHERE Saldo >= 200 AND Situacao = N'ativo' ORDER BY Id",
    )
    .await;
    checks.note(format!(
        "SQL Server itself answers Situacao = N'ativo' with ids {ci:?} (case/space-insensitive collation)"
    ));
    let exact = try_rows(&engine, &join("ativo")).await;
    checks.check(
        "3a join, text = 'ativo' is case- and space-sensitive",
        exact.as_ref() == Ok(&rows(&[&["4", "103", "40.00"]])),
        format!("{exact:?}"),
    );
    let upper = try_rows(&engine, &join("ATIVO")).await;
    checks.check(
        "3b join, text = 'ATIVO' matches only the upper-case row",
        upper.as_ref() == Ok(&rows(&[&["2", "101", "20.00"]])),
        format!("{upper:?}"),
    );
    let spaced = try_rows(&engine, &join("ativo ")).await;
    checks.check(
        "3c join, text = 'ativo ' matches only the row with the trailing space",
        spaced.as_ref() == Ok(&rows(&[&["3", "102", "30.00"]])),
        format!("{spaced:?}"),
    );

    // 4. EXPLAIN of the join: numeric WHERE pushed as T-SQL; text WHERE sent as a
    //    superset and applied exactly in the engine (3a-3c check the rows).
    let plan = physical_plan(&engine, &join("ativo")).await;
    let remote = plan.as_deref().map(remote_sqls).unwrap_or_default();
    let parceiros = remote.iter().find(|s| s.contains("[dbo].[Parceiros]"));
    checks.pushdown(
        "4a EXPLAIN: remote T-SQL uses [dbo].[Parceiros] with the numeric WHERE",
        // Column spelling in the remote SQL is the real one (`[Saldo]`).
        parceiros.is_some_and(|s| {
            s.contains("WHERE") && s.contains(">= 200") && s.to_lowercase().contains("[saldo]")
        }),
        format!("remote={remote:?}"),
    );
    checks.check(
        "4b EXPLAIN: text filter sent as a superset (SQL Server's collation keeps more rows), evaluated locally",
        parceiros.is_some_and(|s| s.to_lowercase().contains("ativo"))
            && plan.as_deref().is_ok_and(|p| {
                p.lines()
                    .any(|l| l.contains("FilterExec") && l.contains("situacao") && l.contains("ativo"))
            }),
        format!("plan={plan:?}"),
    );

    let numeric_only = "SELECT c.id AS parceiro, p.id AS pedido \
         FROM erp1.dbo.parceiros c JOIN erp.public.pedidos p ON p.parceiro_id = c.id \
         WHERE c.saldo >= 200 ORDER BY p.id";
    let plan = physical_plan(&engine, numeric_only).await;
    let remote = plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.pushdown(
        "4c EXPLAIN: join with only the numeric WHERE pushes it to [dbo].[Parceiros]",
        remote
            .iter()
            .any(|s| s.contains("[dbo].[Parceiros]") && s.contains("WHERE") && s.contains(">= 200")),
        format!("remote={remote:?}"),
    );

    // 6. Types, exact values and NULL in each.
    let types = try_rows(
        &engine,
        "SELECT id, i, b, d, s, dt, dd, bt, g, vb FROM erp1.dbo.tipos ORDER BY id",
    )
    .await;
    let expected_types = rows(&[
        &[
            "1",
            "-2147483648",
            "9223372036854775807",
            "1234567890123456.78",
            "ação São Paulo — ü ñ 日本",
            "9999-12-31T23:59:59.999999",
            "9999-12-31",
            "true",
            "6F9619FF-8B86-D011-B42D-00C04FC964FF",
            "deadbeef00",
        ],
        &[
            "2",
            "2147483647",
            "-9223372036854775808",
            "-0.01",
            "",
            "0001-01-01T00:00:00",
            "0001-01-01",
            "false",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
            "",
        ],
        &[
            "3", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
        ],
        &[
            "4",
            "0",
            "0",
            "0.00",
            "x",
            "2026-01-01T10:00:00.123456",
            "2026-02-28",
            "true",
            "00000000-0000-0000-0000-000000000001",
            "00",
        ],
    ]);
    checks.check(
        "6a types: int, bigint, decimal(18,2), nvarchar, datetime2(7), date, bit, guid, varbinary, NULL",
        types.as_ref() == Ok(&expected_types),
        format!("{types:?}"),
    );
    let schema = engine
        .sql("SELECT i, b, d, s, dt, dd, bt, g, vb FROM erp1.dbo.tipos")
        .await
        .map(|df| {
            df.schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        });
    checks.check(
        "6b types: Arrow types",
        schema.as_ref().is_ok_and(|s| {
            *s == [
                "i:Int32",
                "b:Int64",
                "d:Decimal128(18, 2)",
                "s:Utf8",
                "dt:Timestamp(µs)",
                "dd:Date32",
                "bt:Boolean",
                "g:Utf8",
                "vb:Binary",
            ]
        }),
        format!("{schema:?}"),
    );

    // 5. Outage: stop the server with the connector attached.
    source.ping().await.expect("ping while up");
    mssql.stop().await.unwrap();
    let ping = tokio::time::timeout(OUTAGE_TIMEOUT, source.ping()).await;
    checks.check(
        "5a ping fails (and does not hang) once SQL Server is down",
        matches!(ping, Ok(Err(_))),
        format!("{ping:?}"),
    );
    let started = Instant::now();
    let query = tokio::time::timeout(
        OUTAGE_TIMEOUT,
        try_rows(&engine, "SELECT count(*) FROM erp1.dbo.parceiros"),
    )
    .await;
    checks.check(
        "5b query fails with an error (no hang) once SQL Server is down",
        matches!(query, Ok(Err(_))),
        format!("after {:?}: {query:?}", started.elapsed()),
    );

    let suite = match scope {
        Scope::Rows => "F1 DoD (rows)",
        Scope::Pushdown => "F1 DoD (pushdown)",
    };
    checks.finish(&format!("{suite} on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_acceptance_dod_2019() {
    dod("2019-latest", Scope::Rows).await;
}

#[tokio::test]
async fn sqlserver_acceptance_dod_2022() {
    dod("2022-latest", Scope::Rows).await;
}

#[tokio::test]
async fn sqlserver_acceptance_pushdown_2019() {
    dod("2019-latest", Scope::Pushdown).await;
}

#[tokio::test]
async fn sqlserver_acceptance_pushdown_2022() {
    dod("2022-latest", Scope::Pushdown).await;
}

// ---------------------------------------------------------------------------
// Attempts to break the connector.
// ---------------------------------------------------------------------------

#[allow(
    clippy::too_many_lines,
    reason = "one scenario per SQL Server version, read top to bottom"
)]
async fn break_attempts(tag: &str) {
    let mut checks = Checks::new(Scope::Rows);
    let mssql = start_mssql(tag, false).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE Quebra").await;
    exec(
        &host,
        port,
        "Quebra",
        "CREATE TABLE dbo.Grupos (Id INT NOT NULL PRIMARY KEY, K NVARCHAR(10) NOT NULL, V INT NOT NULL);
         INSERT INTO dbo.Grupos VALUES (1, N'A', 1), (2, N'a', 2), (3, N'a ', 3), (4, N'A', 4);
         CREATE TABLE dbo.Grandes (Id INT NOT NULL PRIMARY KEY, N INT NOT NULL);
         INSERT INTO dbo.Grandes VALUES (1, 2147483647), (2, 2147483647), (3, 10);
         CREATE TABLE dbo.Latin (Id INT NOT NULL PRIMARY KEY, V VARCHAR(20) COLLATE Latin1_General_CI_AS NULL);
         INSERT INTO dbo.Latin VALUES (1, 'ação'), (2, 'Pão');",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        sqlserver_connector(&host, port, "Quebra") as Arc<dyn Connector>,
    )
    .await;

    let groups = try_rows(
        &engine,
        "SELECT k, count(*) AS n, sum(v) AS s FROM erp1.dbo.grupos GROUP BY k ORDER BY k",
    )
    .await;
    checks.check(
        "Q1 GROUP BY text keeps 'A', 'a' and 'a ' apart",
        groups.as_ref() == Ok(&rows(&[&["A", "2", "5"], &["a", "1", "2"], &["a ", "1", "3"]])),
        format!("{groups:?}"),
    );
    let distinct = try_rows(&engine, "SELECT count(DISTINCT k) FROM erp1.dbo.grupos").await;
    checks.check(
        "Q2 COUNT(DISTINCT text) = 3",
        distinct.as_ref() == Ok(&rows(&[&["3"]])),
        format!("{distinct:?}"),
    );
    let avg = try_rows(&engine, "SELECT avg(v) FROM erp1.dbo.grupos").await;
    checks.check(
        "Q3 AVG(int) keeps decimals (2.5, not 2)",
        avg.as_ref() == Ok(&rows(&[&["2.5"]])),
        format!("{avg:?}"),
    );
    let sum = try_rows(&engine, "SELECT sum(n) FROM erp1.dbo.grandes").await;
    checks.check(
        "Q4 SUM(int) beyond 2^31 = 4294967304",
        sum.as_ref() == Ok(&rows(&[&["4294967304"]])),
        format!("{sum:?}"),
    );
    let sum_filtered = try_rows(&engine, "SELECT sum(n) FROM erp1.dbo.grandes WHERE id <= 2").await;
    checks.check(
        "Q5 SUM(int) with numeric WHERE = 4294967294",
        sum_filtered.as_ref() == Ok(&rows(&[&["4294967294"]])),
        format!("{sum_filtered:?}"),
    );
    let limit = try_rows(&engine, "SELECT id FROM erp1.dbo.grupos LIMIT 2").await;
    checks.check(
        "Q6 LIMIT 2 returns 2 rows",
        limit.as_ref().is_ok_and(|r| r.len() == 2),
        format!("{limit:?}"),
    );
    let plan = physical_plan(&engine, "SELECT id FROM erp1.dbo.grupos LIMIT 2").await;
    let remote = plan.as_deref().map(remote_sqls).unwrap_or_default();
    checks.check(
        "Q7 EXPLAIN LIMIT 2 is pushed as TOP (2)",
        remote.iter().any(|s| s.contains("TOP (2)")),
        format!("remote={remote:?}"),
    );
    let ordered = try_rows(
        &engine,
        "SELECT id FROM erp1.dbo.grupos ORDER BY v DESC LIMIT 2 OFFSET 1",
    )
    .await;
    checks.check(
        "Q8 ORDER BY … LIMIT 2 OFFSET 1",
        ordered.as_ref() == Ok(&rows(&[&["3"], &["2"]])),
        format!("{ordered:?}"),
    );
    let count = try_rows(&engine, "SELECT count(*) FROM erp1.dbo.grupos WHERE v > 1").await;
    checks.check(
        "Q9 COUNT(*) with numeric WHERE = 3",
        count.as_ref() == Ok(&rows(&[&["3"]])),
        format!("{count:?}"),
    );
    let max_text = try_rows(&engine, "SELECT max(k), min(k) FROM erp1.dbo.grupos").await;
    checks.check(
        "Q10 MAX/MIN(text) follow binary order ('a ' / 'A')",
        max_text.as_ref() == Ok(&rows(&[&["a ", "A"]])),
        format!("{max_text:?}"),
    );
    let latin = try_rows(&engine, "SELECT id, v FROM erp1.dbo.latin ORDER BY id").await;
    checks.check(
        "Q11 varchar (code page 1252) with accents",
        latin.as_ref() == Ok(&rows(&[&["1", "ação"], &["2", "Pão"]])),
        format!("{latin:?}"),
    );

    checks.finish(&format!("F1 break attempts on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_acceptance_break_2019() {
    break_attempts("2019-latest").await;
}

#[tokio::test]
async fn sqlserver_acceptance_break_2022() {
    break_attempts("2022-latest").await;
}

/// Column names are exposed in lower case; the SQL sent to the server must use
/// their real spelling, or a database with a case-sensitive collation cannot
/// be read at all.
async fn case_sensitive_database(tag: &str) {
    let mut checks = Checks::new(Scope::Rows);
    let mssql = start_mssql(tag, false).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(
        &host,
        port,
        "master",
        "CREATE DATABASE Sensivel COLLATE Latin1_General_CS_AS",
    )
    .await;
    exec(
        &host,
        port,
        "Sensivel",
        "CREATE TABLE dbo.Pessoas (Id INT NOT NULL PRIMARY KEY, NomeCompleto NVARCHAR(20) NOT NULL);
         INSERT INTO dbo.Pessoas VALUES (1, N'Ana'), (2, N'Bruno');",
    )
    .await;
    let spill = tempfile::tempdir().unwrap();
    let mut cs_engine = common::engine(spill.path());
    let cs_attach = cs_engine
        .attach(
            sqlserver_connector(&host, port, "Sensivel") as Arc<dyn Connector>,
            true,
            common::ATTACH_TIMEOUT,
        )
        .await;
    let cs = match cs_attach {
        Ok(_) => {
            try_rows(
                &cs_engine,
                "SELECT id, nomecompleto FROM erp1.dbo.pessoas WHERE id >= 1 ORDER BY id",
            )
            .await
        }
        Err(e) => Err(format!("attach: {e}")),
    };
    checks.check(
        "CS1 case-sensitive database: mixed-case columns are readable",
        cs.as_ref() == Ok(&rows(&[&["1", "Ana"], &["2", "Bruno"]])),
        format!("{cs:?}"),
    );

    checks.finish(&format!("F1 case-sensitive database on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_acceptance_case_sensitive_2019() {
    case_sensitive_database("2019-latest").await;
}

#[tokio::test]
async fn sqlserver_acceptance_case_sensitive_2022() {
    case_sensitive_database("2022-latest").await;
}

/// `SHOW CREATE TABLE` of a SQL Server table: columns as the catalog exposes
/// them, `NOT NULL` where SQL Server refuses nulls, and no `WITH` clause (the
/// source has no table properties). A `CHAR(n)` column is exposed as
/// `varchar`, in `DESCRIBE` as here, so the key is a `VARCHAR(36)`.
async fn show_create_table(tag: &str) {
    let mut checks = Checks::new(Scope::Rows);
    let mssql = start_mssql(tag, false).await;
    let host = common::host();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    exec(&host, port, "master", "CREATE DATABASE Cadastro").await;
    exec(
        &host,
        port,
        "Cadastro",
        "CREATE TABLE dbo.Bancos (Id VARCHAR(36) NOT NULL PRIMARY KEY, BancoId VARCHAR(50) NOT NULL, \
         GestorId INT NULL, DataRegistro DATETIME2(3) NOT NULL, Deleted BIT NULL, _sink_loaded_at DATETIME2(3) NULL);",
    )
    .await;
    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    let attached = engine
        .attach(
            sqlserver_connector(&host, port, "Cadastro") as Arc<dyn Connector>,
            true,
            common::ATTACH_TIMEOUT,
        )
        .await;
    let shown = match attached {
        Ok(_) => try_rows(&engine, "SHOW CREATE TABLE erp1.dbo.bancos").await,
        Err(e) => Err(format!("attach: {e}")),
    };
    let expected = "CREATE TABLE erp1.dbo.bancos (\n   id varchar(36) NOT NULL,\n   bancoid varchar(50) NOT NULL,\n   \
                    gestorid integer,\n   dataregistro timestamp(3) NOT NULL,\n   deleted boolean,\n   \
                    _sink_loaded_at timestamp(3)\n)";
    checks.check(
        "SC1 SHOW CREATE TABLE marks NOT NULL columns and has no WITH clause",
        shown.as_ref() == Ok(&rows(&[&[expected]])),
        format!("{shown:?}"),
    );
    let missing = try_rows(&engine, "SHOW CREATE TABLE erp1.dbo.nope").await;
    checks.check(
        "SC2 SHOW CREATE TABLE of a missing table fails",
        missing
            .as_ref()
            .is_err_and(|e| e.contains("Table 'erp1.dbo.nope' does not exist")),
        format!("{missing:?}"),
    );
    checks.finish(&format!("SHOW CREATE TABLE on SQL Server {tag}"));
}

#[tokio::test]
async fn sqlserver_acceptance_show_create_table_2022() {
    show_create_table("2022-latest").await;
}
