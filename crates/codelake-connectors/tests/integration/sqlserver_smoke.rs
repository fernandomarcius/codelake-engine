//! Implementer's smoke test for the SQL Server connector. The acceptance suite
//! for F1 is written separately by an independent tester.

use std::sync::Arc;

use crate::common;
use codelake_connectors::sqlserver::{SqlServerConnector, SqlServerParams};
use secrecy::SecretString;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const SA_PASSWORD: &str = "Smoke-Test-Only-1";

async fn sql(host: &str, port: u16, database: &str, batch: &str) {
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(AuthMethod::sql_server("sa", SA_PASSWORD));
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
    let mut client = Client::connect(config, tcp.compat_write()).await.unwrap();
    client
        .simple_query(batch)
        .await
        .unwrap()
        .into_results()
        .await
        .unwrap();
}

#[tokio::test]
async fn reads_types_and_keeps_text_semantics_local() {
    let container = MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag("2022-latest")
        .start()
        .await
        .unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(1433).await.unwrap();
    sql(&host, port, "master", "CREATE DATABASE smoke").await;
    sql(
        &host,
        port,
        "smoke",
        "CREATE TABLE dbo.Contratos (Id INT PRIMARY KEY, Situacao NVARCHAR(20) NOT NULL, \
           Valor DECIMAL(18,2) NULL, [__$start_lsn] BINARY(10) NOT NULL, CriadoEm DATETIME2(7) NOT NULL);
         INSERT INTO dbo.Contratos VALUES
           (1, N'ativo', 10.50, 0x00000000000000000001, '2026-01-01T10:00:00.1234567'),
           (2, N'ATIVO', NULL,  0x00000000000000000002, '9999-12-31T23:59:59.9999999'),
           (3, N'ativo ', 7.25, 0x00000000000000000003, '1900-01-01T00:00:00');",
    )
    .await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        Arc::new(SqlServerConnector::new(
            "erp1".into(),
            SqlServerParams {
                host,
                port,
                database: "smoke".into(),
                user: "sa".into(),
                password: SecretString::from(SA_PASSWORD),
                encrypt: "required".into(),
                trust_server_certificate: true,
                pool_size: 4,
            },
        )),
    )
    .await;

    let all = common::query(
        &engine,
        "SELECT id, situacao, valor, \"__$start_lsn\", criadoem FROM erp1.dbo.contratos ORDER BY id",
    )
    .await;
    assert!(
        all.contains("| 1  | ativo    | 10.50 | 00000000000000000001 | 2026-01-01T10:00:00.123456 |"),
        "{all}"
    );
    assert!(all.contains("9999-12-31T23:59:59.999999"), "{all}");

    // Case- and trailing-space-sensitive, like the engine (SQL Server's CI
    // collation would return all three rows if this filter were pushed).
    let ativo = common::query(
        &engine,
        "SELECT id FROM erp1.dbo.contratos WHERE situacao = 'ativo'",
    )
    .await;
    assert!(
        ativo.contains("| 1  |") && !ativo.contains("| 2  |") && !ativo.contains("| 3  |"),
        "{ativo}"
    );

    // Numeric filter is pushed to SQL Server as T-SQL with bracket quoting.
    let plan = common::query(&engine, "EXPLAIN SELECT id FROM erp1.dbo.contratos WHERE id > 1").await;
    assert!(
        plan.contains("[dbo].[Contratos]") && plan.contains("WHERE"),
        "{plan}"
    );
    let top = common::query(
        &engine,
        "SELECT count(*) AS n FROM erp1.dbo.contratos WHERE id >= 2",
    )
    .await;
    assert!(top.contains("| 2 |"), "{top}");
}
