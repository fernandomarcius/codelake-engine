//! MySQL connector against a real MySQL, including a cross-source join with
//! PostgreSQL: the core promise of federation.

use std::sync::Arc;

use crate::common;
use codelake_connectors::mysql::{MysqlConnector, MysqlParams};
use codelake_connectors::postgres::{Decimals, PostgresConnector, PostgresParams};
use mysql_async::prelude::Queryable;
use secrecy::SecretString;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn joins_across_mysql_and_postgres() {
    let host = common::host();
    let (mysql, pg) = tokio::join!(
        Mysql::default().with_tag("8.4").start(),
        Postgres::default().with_tag("17-alpine").start()
    );
    let (mysql, pg) = (mysql.unwrap(), pg.unwrap());
    let mysql_port = mysql.get_host_port_ipv4(3306).await.unwrap();
    let pg_port = pg.get_host_port_ipv4(5432).await.unwrap();

    let url = format!("mysql://root@{host}:{mysql_port}/test");
    let mut conn = mysql_async::Conn::from_url(url).await.unwrap();
    conn.query_drop(
        "CREATE TABLE contracts (id INT PRIMARY KEY, customer_id INT NOT NULL, status VARCHAR(20) NOT NULL);
         INSERT INTO contracts VALUES (1, 1, 'active'), (2, 2, 'cancelled'), (3, 1, 'active');",
    )
    .await
    .unwrap();
    conn.disconnect().await.unwrap();

    let dsn = format!("host={host} port={pg_port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
        .batch_execute(
            "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO customers VALUES (1, 'Ana'), (2, 'Bruno');",
        )
        .await
        .unwrap();

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        Arc::new(MysqlConnector::new(
            "crm".into(),
            MysqlParams {
                host: host.clone(),
                port: mysql_port,
                database: "test".into(),
                user: "root".into(),
                password: SecretString::from(""),
                sslmode: "disabled".into(),
            },
        )),
    )
    .await;
    common::attach(
        &mut engine,
        Arc::new(PostgresConnector::new(
            "erp".into(),
            PostgresParams {
                host,
                port: pg_port,
                database: "postgres".into(),
                user: "postgres".into(),
                password: SecretString::from("postgres"),
                sslmode: "disable".into(),
                decimals: Decimals::default(),
            },
        )),
    )
    .await;

    // Regression test for the lost-filter defect (see codelake_core::federation_guard).
    let plan = common::query(
        &engine,
        "EXPLAIN SELECT c.name FROM erp.public.customers c JOIN crm.test.contracts k \
         ON k.customer_id = c.id WHERE k.status = 'active'",
    )
    .await;
    assert!(plan.contains("FederationFilterBarrier"), "{plan}");

    let result = common::query(
        &engine,
        "SELECT c.name, count(*) AS active_contracts \
         FROM erp.public.customers c JOIN crm.test.contracts k ON k.customer_id = c.id \
         WHERE k.status = 'active' GROUP BY c.name ORDER BY c.name",
    )
    .await;
    assert!(result.contains("| Ana  | 2"), "{result}");
    assert!(!result.contains("Bruno"), "{result}");
}
