//! PostgreSQL connector against a real PostgreSQL.

use std::sync::Arc;

use crate::common;
use codelake_connectors::postgres::{PostgresConnector, PostgresParams};
use codelake_core::Connector;
use secrecy::SecretString;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn seed(host: &str, port: u16) {
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
        .batch_execute(
            "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT NOT NULL, total NUMERIC(12,2) NOT NULL);
             INSERT INTO customers VALUES (1, 'Ana'), (2, 'Bruno'), (3, 'Carla');
             INSERT INTO orders VALUES (10, 1, 100.50), (11, 1, 20.00), (12, 3, 7.25);",
        )
        .await
        .unwrap();
}

fn connector(host: &str, port: u16) -> PostgresConnector {
    PostgresConnector::new(
        "erp".into(),
        PostgresParams {
            host: host.into(),
            port,
            database: "postgres".into(),
            user: "postgres".into(),
            password: SecretString::from("postgres"),
            sslmode: "disable".into(),
        },
    )
}

#[tokio::test]
async fn joins_inside_one_source_are_pushed_down_and_correct() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    seed(&host, port).await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;

    let sql = "SELECT c.name, sum(o.total) AS total \
               FROM erp.public.customers c JOIN erp.public.orders o ON o.customer_id = c.id \
               GROUP BY c.name ORDER BY c.name";
    let result = common::query(&engine, sql).await;
    assert!(result.contains("Ana") && result.contains("120.50"), "{result}");
    assert!(result.contains("Carla") && result.contains("7.25"), "{result}");
    assert!(!result.contains("Bruno"), "{result}");

    // Federation: the whole join must travel to PostgreSQL as one statement,
    // not as two table scans joined in memory.
    let plan = common::query(&engine, &format!("EXPLAIN {sql}")).await;
    assert!(
        plan.contains("VirtualExecutionPlan"),
        "join was not federated:\n{plan}"
    );
    assert!(plan.to_lowercase().contains("join"), "{plan}");
}

#[tokio::test]
async fn information_schema_lists_remote_tables() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    seed(&host, port).await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;

    let tables = common::query(
        &engine,
        "SELECT table_name FROM erp.information_schema.tables \
         WHERE table_catalog = 'erp' AND table_schema = 'public' ORDER BY table_name",
    )
    .await;
    assert!(
        tables.contains("customers") && tables.contains("orders"),
        "{tables}"
    );
}

#[tokio::test]
async fn ping_fails_fast_when_the_database_is_gone() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let source = connector(&host, port);
    source.ping().await.expect("ping while up");

    container.stop().await.unwrap();
    assert!(
        source.ping().await.is_err(),
        "ping must fail once the database is down"
    );
}
