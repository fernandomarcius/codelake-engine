//! PostgreSQL connector against a real PostgreSQL.

use std::sync::Arc;

use crate::common;
use codelake_connectors::postgres::{PostgresConnector, PostgresParams};
use codelake_core::{Connector, Engine};
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use datafusion::arrow::util::display::array_value_to_string;
use secrecy::SecretString;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

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

/// The statement each federated scan of `plan` sends.
fn remote_sqls(plan: &str) -> Vec<String> {
    plan.lines()
        .filter(|l| l.contains("VirtualExecutionPlan"))
        .filter_map(|l| l.rsplit_once("_sql=").map(|(_, sql)| sql.trim().to_owned()))
        .collect()
}

/// Five aliased tables, each under a window over `SELECT *` and a semi join
/// with keys another table provider holds: more federated scans than the
/// optimizer has passes. Each reads only the columns the query uses, the
/// projection below its alias (above it, `PostgreSQL` answers "missing
/// FROM-clause entry for table").
#[tokio::test]
async fn every_aliased_scan_of_a_union_reads_only_the_columns_it_uses() {
    const TABLES: [&str; 5] = ["t1", "t2", "t3", "t4", "t5"];
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    for table in TABLES {
        client
            .batch_execute(&format!(
                "CREATE TABLE {table} (k TEXT NOT NULL, seq INT NOT NULL, deleted INT NOT NULL,
                                       v INT NOT NULL, notes TEXT NOT NULL, extra NUMERIC(12,2));
                 INSERT INTO {table} VALUES ('A', 1, 0, 10, 'w', 1), ('A', 2, 0, 11, 'x', 2),
                                            ('B', 1, 0, 20, 'y', 3), ('C', 1, 1, 30, 'z', 4);"
            ))
            .await
            .unwrap();
    }

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "c"]));
    engine
        .context()
        .register_batch("keys", RecordBatch::try_from_iter([("x", keys)]).unwrap())
        .unwrap();

    let sql = TABLES
        .map(|table| {
            format!(
                "SELECT k, v FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY r.k ORDER BY r.seq DESC) AS rn \
                 FROM erp.public.{table} AS r \
                 WHERE lower(r.k) IN (SELECT lower(x) FROM keys)) z \
                 WHERE rn = 1 AND deleted = 0"
            )
        })
        .join(" UNION ALL ");
    let result = common::query(&engine, &sql).await;
    assert_eq!(result.matches("| A | 11 |").count(), 5, "{result}");
    assert_eq!(
        result.lines().count(),
        9,
        "only the newest row of A, five times:\n{result}"
    );

    let plan = common::query(&engine, &format!("EXPLAIN {sql}")).await;
    let remote = remote_sqls(&plan);
    assert_eq!(remote.len(), 5, "one scan per table:\n{plan}");
    for table in TABLES {
        let sent: Vec<&String> = remote
            .iter()
            .filter(|s| s.contains(&format!("\"{table}\"")))
            .collect();
        assert_eq!(sent.len(), 1, "{table}: {remote:?}");
        for used in ["\"k\"", "\"seq\"", "\"deleted\"", "\"v\""] {
            assert!(sent[0].contains(used), "{table} without {used}: {}", sent[0]);
        }
        for unused in ["notes", "extra"] {
            assert!(!sent[0].contains(unused), "{table} reads {unused}: {}", sent[0]);
        }
    }
}

/// Answer of `sql` through the engine's SQL path, every cell as text (`NULL`).
async fn rows(engine: &Engine, sql: &str) -> Vec<Vec<String>> {
    let batches = engine
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}` failed: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("executing `{sql}` failed: {e}"));
    let mut out = Vec::new();
    for batch in &batches {
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

/// The statements `sql` sends to PostgreSQL.
async fn sent(engine: &Engine, sql: &str) -> Vec<String> {
    let plan: Vec<String> = rows(engine, &format!("EXPLAIN {sql}"))
        .await
        .into_iter()
        .filter_map(|row| row.last().cloned())
        .collect();
    remote_sqls(&plan.join("\n"))
}

fn column(rows: &[Vec<String>]) -> Vec<&str> {
    rows.iter().map(|r| r[0].as_str()).collect()
}

/// A table whose text column sorts by an ICU collation (not by code point),
/// read by a session whose `TimeZone` is not UTC: the two settings under
/// which PostgreSQL answers differently from the engine.
async fn collated_source() -> (ContainerAsync<Postgres>, Engine, tempfile::TempDir) {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
        .batch_execute(
            r#"CREATE TABLE t (id INT, s TEXT COLLATE "pt-BR-x-icu", ops INT[], ok BOOLEAN,
                              at TIMESTAMPTZ, d DATE);
             INSERT INTO t VALUES
               (1, 'b', '{18}', true, '2026-09-14 00:30:00+00', '2026-09-01'),
               (2, 'B', '{6,18}', false, '2026-09-13 23:30:00+00', '2026-09-02'),
               (3, 'a', '{88}', false, '2026-09-14 02:59:00+00', '2026-09-03'),
               (4, 'A', NULL, NULL, '2026-09-14 03:00:00+00', '2026-09-04'),
               (5, 'é', '{89,6}', true, NULL, NULL),
               (6, 'e', '{6}', true, '2026-09-15 00:00:00+00', '2026-09-05'),
               (7, '_x', '{13}', false, '2026-09-14 01:00:00+00', '2026-09-06'),
               (8, 'Z', '{169}', false, '2026-09-14 06:00:00+00', '2026-09-07'),
               (9, 'Ábaco', '{18,88}', NULL, '2026-09-14 05:59:00+00', '2026-09-08'),
               (10, 'ação', '{88}', false, '2026-09-16 00:00:00+00', '2026-09-09'),
               (11, NULL, '{18}', false, NULL, NULL);
             ALTER DATABASE postgres SET timezone TO 'America/Sao_Paulo';"#,
        )
        .await
        .unwrap();
    // The collation really orders differently from code points.
    let icu: Vec<String> = client
        .query("SELECT s FROM t WHERE s IS NOT NULL ORDER BY s", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(icu, ["_x", "a", "A", "Ábaco", "ação", "b", "B", "e", "é", "Z"]);

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;
    (container, engine, spill)
}

#[tokio::test]
async fn text_is_ordered_by_code_point_whatever_the_collation() {
    let (_container, engine, _spill) = collated_source().await;
    let by_code_point = ["A", "B", "Z", "_x", "a", "ação", "b", "e", "Ábaco", "é", "NULL"];

    let sql = "SELECT s FROM erp.public.t ORDER BY s";
    assert_eq!(column(&rows(&engine, sql).await), by_code_point);
    let remote = sent(&engine, sql).await;
    assert!(remote.iter().all(|s| !s.contains("ORDER BY")), "{remote:?}");

    let sql = "SELECT s FROM erp.public.t ORDER BY s DESC LIMIT 3";
    assert_eq!(column(&rows(&engine, sql).await), ["é", "Ábaco", "e"]);

    let sql = "SELECT min(s), max(s) FROM erp.public.t";
    assert_eq!(rows(&engine, sql).await, [["A", "é"]]);

    // Ranges on text: [A, B, Z, _x] precede 'a' by code point ([_x] under pt-BR).
    let sql = "SELECT count(*) FROM erp.public.t WHERE s < 'a'";
    assert_eq!(rows(&engine, sql).await, [["4"]]);
    let sql = "SELECT id FROM erp.public.t WHERE s BETWEEN 'a' AND 'b' ORDER BY id";
    assert_eq!(column(&rows(&engine, sql).await), ["1", "3", "10"]);
    let remote = sent(&engine, sql).await;
    assert!(remote.iter().all(|s| !s.contains("WHERE")), "{remote:?}");

    // Equality does not depend on the collation: it is still sent.
    let sql = "SELECT id FROM erp.public.t WHERE s = 'ação' OR s IN ('Z', 'e') ORDER BY id";
    assert_eq!(column(&rows(&engine, sql).await), ["6", "8", "10"]);
    let remote = sent(&engine, sql).await;
    assert!(remote.iter().any(|s| s.contains("'ação'")), "{remote:?}");
}

#[tokio::test]
async fn distinct_and_grouping_on_text_are_sent_as_valid_sql() {
    let (_container, engine, _spill) = collated_source().await;
    let sql = "SELECT DISTINCT s FROM erp.public.t";
    let mut distinct = column(&rows(&engine, sql).await)
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    distinct.sort();
    assert_eq!(distinct.len(), 11, "{distinct:?}");
    let remote = sent(&engine, sql).await;
    assert!(
        remote
            .iter()
            .any(|s| s.contains("GROUP BY") && !s.contains("derived_projection")),
        "{remote:?}"
    );

    let sql =
        "SELECT d, count(*), sum(id) FROM erp.public.t WHERE d >= DATE '2026-09-05' GROUP BY d ORDER BY d";
    assert_eq!(
        rows(&engine, sql).await,
        [
            ["2026-09-05", "1", "6"],
            ["2026-09-06", "1", "7"],
            ["2026-09-07", "1", "8"],
            ["2026-09-08", "1", "9"],
            ["2026-09-09", "1", "10"],
        ]
    );
    let remote = sent(&engine, sql).await;
    assert!(
        remote.iter().any(|s| s.contains("GROUP BY")
            && s.contains("CAST('2026-09-05' AS DATE)")
            && s.contains("ORDER BY")),
        "{remote:?}"
    );
}

#[tokio::test]
async fn functions_the_engine_defines_are_evaluated_in_the_engine() {
    let (_container, engine, _spill) = collated_source().await;
    let cases = [
        ("SELECT count_if(ok) FROM erp.public.t", "3", "count_if"),
        (
            "SELECT count(*) FROM erp.public.t WHERE arrays_overlap(ops, ARRAY[6, 13])",
            "4",
            "arrays_overlap",
        ),
        (
            "SELECT count(*) FILTER (WHERE id > 5) FROM erp.public.t",
            "6",
            "FILTER",
        ),
        ("SELECT count(*) FROM erp.public.t WHERE ops[1] = 18", "3", "[1]"),
    ];
    for (sql, expected, local) in cases {
        assert_eq!(rows(&engine, sql).await, [[expected]], "{sql}");
        let remote = sent(&engine, sql).await;
        assert!(
            !remote.is_empty() && remote.iter().all(|s| !s.contains(local)),
            "{sql}: {remote:?}"
        );
    }
    // The safe terms of the same filter still reach PostgreSQL.
    let sql = "SELECT id FROM erp.public.t WHERE arrays_overlap(ops, ARRAY[6, 13]) AND id > 2 ORDER BY id";
    assert_eq!(column(&rows(&engine, sql).await), ["5", "6", "7"]);
    let remote = sent(&engine, sql).await;
    assert!(remote.iter().any(|s| s.contains("> 2")), "{remote:?}");
}

#[tokio::test]
async fn zoned_timestamp_literals_reach_postgresql_as_instants() {
    let (_container, engine, _spill) = collated_source().await;
    // The session zone is UTC: [00:00Z, 03:00Z). Read in the database's
    // America/Sao_Paulo, the same text would be [03:00Z, 06:00Z): ids 4 and 9.
    let sql = "SELECT id FROM erp.public.t \
               WHERE at >= TIMESTAMP '2026-09-14 00:00:00' AND at < TIMESTAMP '2026-09-14 03:00:00' \
               ORDER BY id";
    assert_eq!(column(&rows(&engine, sql).await), ["1", "3", "7"]);
    let remote = sent(&engine, sql).await;
    assert!(
        remote
            .iter()
            .any(|s| s.contains("CAST('2026-09-14 00:00:00+00' AS TIMESTAMP WITH TIME ZONE)")),
        "{remote:?}"
    );
    let sql = "SELECT max(at) FROM erp.public.t WHERE at < TIMESTAMP '2026-09-14 06:00:00.000001'";
    assert_eq!(rows(&engine, sql).await, [["2026-09-14T06:00:00Z"]]);
}
