//! PostgreSQL connector against a real PostgreSQL.

use std::sync::Arc;

use crate::common;
use codelake_connectors::postgres::{Decimals, PostgresConnector, PostgresParams};
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
            decimals: Decimals::default(),
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

/// `IN`/`NOT IN` lists the simplifier splits into `OR`/`AND` chains reach
/// PostgreSQL as lists again (it plans the chain apart, up to forty times
/// slower over a view), with the same rows: NULLs are in neither answer.
#[tokio::test]
async fn value_lists_reach_postgresql_as_lists() {
    let (_container, engine, _spill) = collated_source().await;
    let cases = [
        (
            "SELECT id FROM erp.public.t WHERE s IN ('Z', 'e', 'ação') ORDER BY id",
            vec!["6", "8", "10"],
            r#""t"."s" IN ('Z', 'e', 'ação')"#,
        ),
        (
            "SELECT id FROM erp.public.t WHERE s NOT IN ('Z', 'e', 'ação') ORDER BY id",
            vec!["1", "2", "3", "4", "5", "7", "9"],
            r#""t"."s" NOT IN ('Z', 'e', 'ação')"#,
        ),
        (
            "SELECT id FROM erp.public.t WHERE s = 'Z' OR id = 3 OR s = 'e' ORDER BY id",
            vec!["3", "6", "8"],
            r#""t"."s" IN ('Z', 'e')"#,
        ),
        (
            "SELECT id FROM erp.public.t WHERE id IN (1, 4, 11) AND ok IS NOT NULL ORDER BY id",
            vec!["1", "11"],
            r#""t"."id" IN (1, 4, 11)"#,
        ),
        // The session zone is UTC; the database's is America/Sao_Paulo.
        (
            "SELECT id FROM erp.public.t \
             WHERE at IN (TIMESTAMP '2026-09-14 00:30:00', TIMESTAMP '2026-09-14 03:00:00') \
               AND d IN (DATE '2026-09-01', DATE '2026-09-04') ORDER BY id",
            vec!["1", "4"],
            "IN (CAST('2026-09-14 00:30:00+00' AS TIMESTAMP WITH TIME ZONE), \
             CAST('2026-09-14 03:00:00+00' AS TIMESTAMP WITH TIME ZONE))",
        ),
    ];
    for (sql, expected, list) in cases {
        assert_eq!(column(&rows(&engine, sql).await), expected, "{sql}");
        let remote = sent(&engine, sql).await;
        assert!(remote.iter().any(|s| s.contains(list)), "{sql}: {remote:?}");
        // Only the term on another column is left joined by `OR`.
        let ors = usize::from(sql.contains("id = 3"));
        assert!(
            remote.iter().all(|s| s.matches(" OR ").count() == ors),
            "{sql}: {remote:?}"
        );
    }
}

async fn client(host: &str, port: u16, user: &str, password: &str) -> tokio_postgres::Client {
    let dsn = format!("host={host} port={port} user={user} password={password} dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(connection);
    client
}

fn connector_as(host: &str, port: u16, user: &str, password: &str) -> PostgresConnector {
    PostgresConnector::new(
        "erp".into(),
        PostgresParams {
            host: host.into(),
            port,
            database: "postgres".into(),
            user: user.into(),
            password: SecretString::from(password.to_owned()),
            sslmode: "disable".into(),
            decimals: Decimals::default(),
        },
    )
}

/// One relation of each kind a query reads, the hub's views among them, all
/// over the same rows; and objects that are not relations.
const RELATIONS: &str = "
    CREATE SCHEMA cat;
    CREATE TABLE cat.base_t (id INT NOT NULL, label TEXT, ts TIMESTAMPTZ);
    INSERT INTO cat.base_t VALUES (1, 'a', '2026-09-01 12:00:00+00'), (2, 'b', NULL);
    CREATE VIEW cat.a_view AS SELECT * FROM cat.base_t;
    CREATE MATERIALIZED VIEW cat.a_matview AS SELECT * FROM cat.base_t;
    CREATE TABLE cat.a_part (id INT, label TEXT, ts TIMESTAMPTZ) PARTITION BY RANGE (id);
    CREATE TABLE cat.a_part_1 PARTITION OF cat.a_part FOR VALUES FROM (0) TO (100);
    INSERT INTO cat.a_part SELECT * FROM cat.base_t;
    CREATE TABLE cat.empty_part (id INT, label TEXT) PARTITION BY RANGE (id);
    CREATE EXTENSION postgres_fdw;
    CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname 'postgres');
    CREATE USER MAPPING FOR PUBLIC SERVER loopback OPTIONS (user 'postgres', password 'postgres');
    CREATE FOREIGN TABLE cat.a_foreign (id INT, label TEXT, ts TIMESTAMPTZ)
        SERVER loopback OPTIONS (schema_name 'cat', table_name 'base_t');
    CREATE SEQUENCE cat.a_seq;
    CREATE INDEX a_idx ON cat.base_t (id);
    CREATE TYPE cat.pair AS (x INT, y TEXT);
    CREATE TABLE cat.no_grant (id INT);
    CREATE SCHEMA hidden;
    CREATE TABLE hidden.h (id INT);
    CREATE ROLE limited LOGIN PASSWORD 'limited';
    GRANT USAGE ON SCHEMA cat TO limited;
    GRANT SELECT ON cat.base_t, cat.a_view TO limited;";

#[tokio::test]
async fn views_materialized_views_foreign_and_partitioned_tables_are_relations() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(RELATIONS)
        .await
        .unwrap();

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;

    // Every relation kind, partitions too; not sequences, indexes nor types.
    let tables = common::query(
        &engine,
        "SELECT table_name, table_type FROM erp.information_schema.tables \
         WHERE table_schema = 'cat' ORDER BY table_name",
    )
    .await;
    let names: Vec<&str> = tables
        .lines()
        .filter_map(|l| l.strip_prefix("| "))
        .filter_map(|l| l.split(' ').next())
        .filter(|n| *n != "table_name")
        .collect();
    assert_eq!(
        names,
        [
            "a_foreign",
            "a_matview",
            "a_part",
            "a_part_1",
            "a_view",
            "base_t",
            "empty_part",
            "no_grant"
        ],
        "{tables}"
    );
    assert!(
        !tables.contains("VIEW |"),
        "listed as tables, as the reference engine does:\n{tables}"
    );

    for relation in ["base_t", "a_view", "a_matview", "a_part", "a_part_1", "a_foreign"] {
        let rows = common::query(
            &engine,
            &format!("SELECT id, label, ts FROM erp.cat.{relation} ORDER BY id"),
        )
        .await;
        assert!(
            rows.contains("| 1  | a     | 2026-09-01T12:00:00Z |") && rows.contains("| 2  | b     |"),
            "{relation}:\n{rows}"
        );
    }
    // Columns come from the catalog, not from a row: an empty partitioned
    // table has them.
    let columns = common::query(
        &engine,
        "SELECT column_name, data_type FROM erp.information_schema.columns \
         WHERE table_schema = 'cat' AND table_name IN ('empty_part', 'a_foreign') \
         ORDER BY table_name, ordinal_position",
    )
    .await;
    for line in [
        "| id          | integer                     |",
        "| label       | varchar                     |",
        "| ts          | timestamp(6) with time zone |",
    ] {
        assert!(columns.contains(line), "{line}:\n{columns}");
    }
    assert_eq!(columns.matches("| id ").count(), 2, "{columns}");
    let empty = common::query(&engine, "SELECT count(*) AS n FROM erp.cat.empty_part").await;
    assert!(empty.contains("| 0 |"), "{empty}");

    // The source's system schemas: pg_catalog is listed, without relations;
    // its relations and the source's information_schema are not readable.
    let schemas = common::query(
        &engine,
        "SELECT schema_name FROM erp.information_schema.schemata ORDER BY schema_name",
    )
    .await;
    for schema in ["cat", "hidden", "information_schema", "pg_catalog", "public"] {
        assert!(schemas.contains(&format!("| {schema}")), "{schema}:\n{schemas}");
    }
    assert!(!schemas.contains("pg_toast"), "{schemas}");
    for missing in [
        "erp.pg_catalog.pg_class",
        "erp.cat.a_seq",
        "erp.cat.a_idx",
        "erp.cat.pair",
    ] {
        let err = engine
            .context()
            .sql(&format!("SELECT * FROM {missing}"))
            .await
            .expect_err(missing);
        assert!(err.to_string().contains("not found"), "{missing}: {err}");
    }
}

/// Like the reference engine, the listing does not depend on privileges:
/// what the user may not read is listed and fails at the source when read.
#[tokio::test]
async fn relations_the_user_may_not_read_are_listed_and_fail_at_the_source() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(RELATIONS)
        .await
        .unwrap();

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(
        &mut engine,
        Arc::new(connector_as(&host, port, "limited", "limited")),
    )
    .await;
    let tables = common::query(
        &engine,
        "SELECT table_schema, table_name FROM erp.information_schema.tables \
         WHERE table_schema IN ('cat', 'hidden') ORDER BY 1, 2",
    )
    .await;
    assert!(tables.contains("| cat          | no_grant"), "{tables}");
    assert!(tables.contains("| hidden       | h "), "{tables}");
    let granted = common::query(&engine, "SELECT label FROM erp.cat.a_view WHERE id = 2").await;
    assert!(granted.contains("| b "), "{granted}");
    for denied in ["erp.cat.no_grant", "erp.hidden.h"] {
        let err = engine
            .context()
            .sql(&format!("SELECT * FROM {denied}"))
            .await
            .unwrap()
            .collect()
            .await
            .expect_err(denied);
        assert!(err.to_string().contains("permission denied"), "{denied}: {err}");
    }
}

#[tokio::test]
async fn relations_created_after_boot_are_found_without_a_restart() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let admin = client(&host, port, "postgres", "postgres").await;
    admin
        .batch_execute("CREATE TABLE public.t (id INT); INSERT INTO t VALUES (1), (2);")
        .await
        .unwrap();

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;

    // A view created after boot is read by name at once, in any case.
    admin
        .batch_execute("CREATE VIEW public.late_view AS SELECT id * 10 AS v FROM t;")
        .await
        .unwrap();
    let rows = common::query(&engine, "SELECT sum(v) AS s FROM erp.public.LATE_VIEW").await;
    assert!(rows.contains("| 30 |"), "{rows}");
    let listed = common::query(
        &engine,
        "SELECT count(*) AS n FROM erp.information_schema.tables WHERE table_name = 'late_view'",
    )
    .await;
    assert!(listed.contains("| 1 |"), "{listed}");

    // Relations and schemas created after boot are listed once the listing
    // is older than its time to live; a schema then resolves.
    admin
        .batch_execute(
            "CREATE MATERIALIZED VIEW public.late_mat AS SELECT 7 AS v;
             CREATE SCHEMA late; CREATE TABLE late.t2 (x INT); INSERT INTO late.t2 VALUES (5);",
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let tables = common::query(
        &engine,
        "SELECT table_schema, table_name FROM erp.information_schema.tables \
         WHERE table_schema IN ('public', 'late') ORDER BY 1, 2",
    )
    .await;
    for line in [
        "| late         | t2 ",
        "| public       | late_mat ",
        "| public       | t ",
    ] {
        assert!(tables.contains(line), "{line}:\n{tables}");
    }
    let rows = common::query(&engine, "SELECT x FROM erp.late.t2").await;
    assert!(rows.contains("| 5 |"), "{rows}");

    // Dropped at the source: no longer listed once read again.
    admin.batch_execute("DROP VIEW public.late_view;").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let listed = common::query(
        &engine,
        "SELECT count(*) AS n FROM erp.information_schema.tables WHERE table_name = 'late_view'",
    )
    .await;
    assert!(listed.contains("| 0 |"), "{listed}");
}

const ARRAYS: &str = "
    CREATE TYPE mood AS ENUM ('ok', 'bad');
    CREATE TABLE arrays (
      id INT,
      a_date DATE[], a_ts TIMESTAMP[], a_ts3 TIMESTAMP(3)[], a_tstz TIMESTAMPTZ[], a_time TIME[],
      a_num2 NUMERIC(10,2)[], a_text TEXT[], a_varchar VARCHAR[], a_varchar10 VARCHAR(10)[],
      a_char CHAR(3)[], a_ichar \"char\"[], a_name NAME[], a_i2 SMALLINT[], a_i4 INT[], a_i8 BIGINT[],
      a_f4 REAL[], a_f8 DOUBLE PRECISION[], a_bool BOOLEAN[], a_uuid UUID[], a_json JSON[],
      a_jsonb JSONB[], a_money MONEY[], a_mood mood[], a_2d INT[][],
      a_num NUMERIC[], a_bytea BYTEA[], a_interval INTERVAL[], a_inet INET[], a_oid OID[]
    );
    INSERT INTO arrays VALUES (1,
      ARRAY[DATE '2026-09-01', NULL, DATE '2026-09-30'], ARRAY[TIMESTAMP '2026-09-01 10:11:12.123456'],
      ARRAY[TIMESTAMP '2026-09-01 10:11:12.123'], ARRAY[TIMESTAMPTZ '2026-09-01 10:11:12.5+00'],
      ARRAY[TIME '10:11:12'], ARRAY[1.5, NULL, -2.25]::NUMERIC(10,2)[], ARRAY['a', NULL, 'c'],
      ARRAY['v']::VARCHAR[], ARRAY['v10']::VARCHAR(10)[], ARRAY['ab']::CHAR(3)[], ARRAY['x']::\"char\"[],
      ARRAY['n']::NAME[], ARRAY[1, 2]::SMALLINT[], ARRAY[1, NULL, 3], ARRAY[9223372036854775807, -1],
      ARRAY[1.5]::REAL[], ARRAY[2.5, 'NaN', '-Infinity']::FLOAT8[], ARRAY[true, NULL],
      ARRAY['a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID], ARRAY['{\"a\":1}'::JSON],
      ARRAY['{\"a\": 1}'::JSONB], ARRAY[1.5::MONEY], ARRAY['ok'::mood], ARRAY[[1, 2], [3, 4]],
      ARRAY[1.5], ARRAY['\\x01'::BYTEA], ARRAY[INTERVAL '1 day'], ARRAY['10.0.0.1'::INET], ARRAY[1::OID]),
      (2, NULL, '{}', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    CREATE VIEW arrays_view AS SELECT * FROM arrays;";

async fn assert_contains(engine: &codelake_core::Engine, sql: &str, values: &[&str]) {
    let rows = common::query(engine, sql).await;
    for value in values {
        assert!(rows.contains(value), "{value}:\n{rows}");
    }
}

/// The columns of `ARRAYS` read, with their types (the others are left out).
const ARRAY_COLUMNS: [(&str, &str); 24] = [
    ("id", "integer"),
    ("a_date", "array(date)"),
    ("a_ts", "array(timestamp(6))"),
    ("a_ts3", "array(timestamp(3))"),
    ("a_tstz", "array(timestamp(6) with time zone)"),
    ("a_time", "array(time(6))"),
    ("a_num2", "array(decimal(10,2))"),
    ("a_text", "array(varchar)"),
    ("a_varchar", "array(varchar)"),
    ("a_varchar10", "array(varchar)"),
    ("a_char", "array(varchar)"),
    ("a_ichar", "array(varchar)"),
    ("a_name", "array(varchar)"),
    ("a_i2", "array(smallint)"),
    ("a_i4", "array(integer)"),
    ("a_i8", "array(bigint)"),
    ("a_f4", "array(real)"),
    ("a_f8", "array(double)"),
    ("a_bool", "array(boolean)"),
    ("a_uuid", "array(varchar)"),
    ("a_json", "array(varchar)"),
    ("a_jsonb", "array(varchar)"),
    ("a_money", "array(varchar)"),
    ("a_mood", "array(varchar)"),
];

/// Arrays as the reference engine maps them (`array-mapping=AS_ARRAY`): the
/// types it maps are lists, the others are left out.
#[tokio::test]
async fn arrays_of_the_types_the_reference_engine_maps_are_read() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(ARRAYS)
        .await
        .unwrap();

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;

    for relation in ["arrays", "arrays_view"] {
        let columns = common::query(
            &engine,
            &format!(
                "SELECT column_name, data_type FROM erp.information_schema.columns \
                 WHERE table_name = '{relation}' ORDER BY ordinal_position"
            ),
        )
        .await;
        let found: Vec<(String, String)> = columns
            .lines()
            .skip(3)
            .filter_map(|l| {
                let cells: Vec<&str> = l.split('|').map(str::trim).collect();
                (cells.len() == 4).then(|| (cells[1].to_owned(), cells[2].to_owned()))
            })
            .collect();
        let mut want: Vec<(String, String)> = ARRAY_COLUMNS
            .iter()
            .map(|(c, t)| ((*c).to_owned(), (*t).to_owned()))
            .collect();
        // Declared dimensions: a view keeps none, read as one.
        want.push((
            "a_2d".to_owned(),
            if relation == "arrays" {
                "array(array(integer))"
            } else {
                "array(integer)"
            }
            .to_owned(),
        ));
        assert_eq!(found, want, "{relation}:\n{columns}");
    }

    assert_contains(
        &engine,
        "SELECT a_date, a_ts, a_ts3, a_tstz, a_time, a_num2, a_text, a_varchar, a_varchar10, a_char, \
         a_ichar, a_name FROM erp.public.arrays ORDER BY id",
        &[
            "[2026-09-01, , 2026-09-30]",
            "[2026-09-01T10:11:12.123456]",
            "[2026-09-01T10:11:12.123]",
            "[2026-09-01T10:11:12.500Z]",
            "[10:11:12]",
            "[1.50, , -2.25]",
            "[a, , c]",
            "[v]",
            "[v10]",
            "[ab]",
            "[x]",
            "[n]",
        ],
    )
    .await;
    assert_contains(
        &engine,
        "SELECT a_i2, a_i4, a_i8, a_f4, a_f8, a_bool, a_uuid, a_json, a_jsonb, a_money, a_mood, a_ts AS empty \
         FROM erp.public.arrays_view ORDER BY id",
        &[
        "[1, 2]",
        "[1, , 3]",
        "[9223372036854775807, -1]",
        "[1.5]",
        "[2.5, NaN, -inf]",
        "[true, ]",
        "[a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11]",
        "[{\"a\":1}]",
        "[{\"a\": 1}]",
        "[$1.50]",
        "[ok]",
        "| [] ",
        ],
    )
    .await;
    let grid = common::query(&engine, "SELECT a_2d FROM erp.public.arrays WHERE id = 1").await;
    assert!(grid.contains("[[1, 2], [3, 4]]"), "{grid}");
    // An array next to other columns, filtered and projected at the source.
    let filtered = common::query(
        &engine,
        "SELECT id, a_date FROM erp.public.arrays_view WHERE a_date IS NOT NULL",
    )
    .await;
    assert!(
        filtered.contains("| 1  | [2026-09-01, , 2026-09-30] |") && !filtered.contains("| 2 "),
        "{filtered}"
    );
}

/// Each connection opens a plain session: no statement the server refuses,
/// so its log has no `ERROR` from the engine.
#[tokio::test]
async fn sessions_leave_no_error_in_the_server_log() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    seed(&host, port).await;

    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;
    // Several connections of the pool at once.
    let queries = (0..4).map(|_| {
        common::query(
            &engine,
            "SELECT c.name, o.total FROM erp.public.customers c JOIN erp.public.orders o ON o.customer_id = c.id",
        )
    });
    for rows in futures::future::join_all(queries).await {
        assert!(rows.contains("Ana"), "{rows}");
    }
    let log = String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap()).into_owned();
    assert!(log.contains("database system is ready"), "{log}");
    assert!(!log.contains("ERROR"), "{log}");
}

/// Rows of `sql` in a session with time zone `zone` (`None`: the engine's).
async fn rows_in(engine: &Engine, zone: Option<&str>, sql: &str) -> Vec<Vec<String>> {
    let options = codelake_core::QueryOptions {
        time_zone: zone.map(str::to_owned),
        ..codelake_core::QueryOptions::for_user("u")
    };
    let batches = engine
        .sql_with(&options, sql)
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
                    .map(|c| array_value_to_string(c, i).unwrap())
                    .collect(),
            );
        }
    }
    out
}

/// Dates and times of every precision, inside and outside 1677–2262.
const DATETIMES: &str = "CREATE TABLE dt (id INT,
       ts TIMESTAMP, ts0 TIMESTAMP(0), ts1 TIMESTAMP(1), ts3 TIMESTAMP(3), ts5 TIMESTAMP(5),
       tz TIMESTAMPTZ, tz0 TIMESTAMPTZ(0), tz2 TIMESTAMPTZ(2), tz3 TIMESTAMPTZ(3),
       t TIME, t0 TIME(0), t3 TIME(3), t4 TIME(4),
       a_ts3 TIMESTAMP(3)[], a_tz TIMESTAMPTZ[], a_t TIME[]);
     INSERT INTO dt VALUES (1,
       '2026-10-01 12:34:56.123456', '2026-10-01 12:34:56', '2026-10-01 12:34:56.1',
       '2026-10-01 12:34:56.123', '2026-10-01 12:34:56.12345',
       '2026-10-01 12:34:56.123456+00', '2026-10-01 12:34:56+00', '2026-10-01 12:34:56.12+00',
       '2026-10-01 12:34:56.123+00',
       '12:34:56.123456', '12:34:56', '12:34:56.123', '12:34:56.1234',
       ARRAY['2026-10-01 12:34:56.123'::TIMESTAMP(3)], ARRAY['2026-10-01 12:34:56.123456+00'::TIMESTAMPTZ],
       ARRAY['12:34:56.123456'::TIME]);
     INSERT INTO dt (id, ts, tz, ts3) VALUES
       (2, '1960-01-01 00:00:00.5', '1960-01-01 00:00:00.5+00', '1969-12-31 23:59:59.999'),
       (3, '0001-01-01 00:00:00', '9999-12-31 23:59:59.999999+00', '2300-01-01 00:00:00.001');
     ALTER DATABASE postgres SET timezone TO 'America/Sao_Paulo';";

/// The types of `DATETIMES`, as the reference engine declares them.
const DATETIME_TYPES: [(&str, &str); 17] = [
    ("id", "integer"),
    ("ts", "timestamp(6)"),
    ("ts0", "timestamp(0)"),
    ("ts1", "timestamp(1)"),
    ("ts3", "timestamp(3)"),
    ("ts5", "timestamp(5)"),
    ("tz", "timestamp(6) with time zone"),
    ("tz0", "timestamp(0) with time zone"),
    ("tz2", "timestamp(2) with time zone"),
    ("tz3", "timestamp(3) with time zone"),
    ("t", "time(6)"),
    ("t0", "time(0)"),
    ("t3", "time(3)"),
    ("t4", "time(4)"),
    ("a_ts3", "array(timestamp(3))"),
    ("a_tz", "array(timestamp(6) with time zone)"),
    ("a_t", "array(time(6))"),
];

/// Dates and times have the reference engine's precision: PostgreSQL's
/// microsecond, or the precision a column declares; their values are read
/// whole, also before 1970 and after 2262 (the upstream decoder dropped
/// the first and turned the second into 1970-01-01).
#[tokio::test]
async fn datetimes_have_the_reference_precision_and_their_whole_range() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(DATETIMES)
        .await
        .unwrap();
    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;

    let types = rows(
        &engine,
        "SELECT column_name, data_type FROM erp.information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'dt' ORDER BY ordinal_position",
    )
    .await;
    let types: Vec<(&str, &str)> = types.iter().map(|r| (r[0].as_str(), r[1].as_str())).collect();
    assert_eq!(types, DATETIME_TYPES);

    let values = rows(
        &engine,
        "SELECT ts, ts0, ts1, ts3, ts5, tz, tz0, tz2, tz3, t, t0, t3, t4, a_ts3, a_tz, a_t \
         FROM erp.public.dt WHERE id = 1",
    )
    .await;
    assert_eq!(
        values,
        [[
            "2026-10-01T12:34:56.123456",
            "2026-10-01T12:34:56",
            "2026-10-01T12:34:56.100",
            "2026-10-01T12:34:56.123",
            "2026-10-01T12:34:56.123450",
            "2026-10-01T12:34:56.123456Z",
            "2026-10-01T12:34:56Z",
            "2026-10-01T12:34:56.120Z",
            "2026-10-01T12:34:56.123Z",
            "12:34:56.123456",
            "12:34:56",
            "12:34:56.123",
            "12:34:56.123400",
            "[2026-10-01T12:34:56.123]",
            "[2026-10-01T12:34:56.123456Z]",
            "[12:34:56.123456]",
        ]]
    );
    let range = rows(
        &engine,
        "SELECT id, ts, tz, ts3 FROM erp.public.dt WHERE id > 1 ORDER BY id",
    )
    .await;
    assert_eq!(
        range,
        [
            [
                "2",
                "1960-01-01T00:00:00.500",
                "1960-01-01T00:00:00.500Z",
                "1969-12-31T23:59:59.999",
            ],
            [
                "3",
                "0001-01-01T00:00:00",
                "9999-12-31T23:59:59.999999Z",
                "2300-01-01T00:00:00.001",
            ],
        ]
    );
    // Compared at the source with literals of other precisions.
    let found = rows(
        &engine,
        "SELECT id FROM erp.public.dt WHERE ts < TIMESTAMP '1970-01-01 00:00:00' ORDER BY id",
    )
    .await;
    assert_eq!(found, [["2"], ["3"]]);
    let found = rows(
        &engine,
        "SELECT id FROM erp.public.dt WHERE tz > TIMESTAMP '9999-12-31 23:59:59.999998 UTC'",
    )
    .await;
    assert_eq!(found, [["3"]]);
}

fn connector_with(host: &str, port: u16, decimals: Decimals) -> PostgresConnector {
    PostgresConnector::new(
        "erp".into(),
        PostgresParams {
            decimals,
            ..connector_params(host, port)
        },
    )
}

fn connector_params(host: &str, port: u16) -> PostgresParams {
    PostgresParams {
        host: host.into(),
        port,
        database: "postgres".into(),
        user: "postgres".into(),
        password: SecretString::from("postgres"),
        sslmode: "disable".into(),
        decimals: Decimals::default(),
    }
}

/// `numeric` columns of every kind, the hub's view among them.
const NUMERICS: &str = "
    CREATE TABLE nums (id INT, n NUMERIC, n2 NUMERIC(10,2), w NUMERIC(50,10), a NUMERIC[]);
    INSERT INTO nums (id, n, n2, w, a) VALUES (1, 1.5, 1.50, 1.5, '{1.5,NULL,-2.5}'),
      (2, 123456789012345678.12345678901234567890, NULL, 0.12345, NULL),
      (3, -999999999999999999.99999999999999999999, NULL, NULL, '{}'),
      (4, 0.1000000000000000000000000, NULL, NULL, NULL), (5, NULL, NULL, NULL, NULL);
    INSERT INTO nums (id, n) VALUES (10, 1e20), (11, 0.123456789012345678901), (12, 'NaN'),
      (13, 'Infinity'), (14, '-Infinity'), (15, 1000000000000000000),
      (16, 12345678901234.123456789012345678901);
    CREATE VIEW recebimentos AS SELECT id, n AS vl_recebido, n2 FROM nums;
    CREATE VIEW totals AS SELECT sum(n) AS total FROM nums WHERE id < 10;";

async fn numerics_engine(host: &str, port: u16, decimals: Decimals) -> (Engine, tempfile::TempDir) {
    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector_with(host, port, decimals))).await;
    (engine, spill)
}

async fn error_of(engine: &Engine, sql: &str) -> String {
    match engine.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(frame) => frame.collect().await.expect_err(sql).to_string(),
    }
}

/// By default a `numeric` without precision, or with more than 38 digits,
/// and an array of them, is left out of its relation, as the reference
/// engine leaves it (`decimal-mapping=STRICT`, ISS-060): the `Warehouse`
/// API's .NET driver cannot hold the `decimal(38,20)` it was, and the API's
/// JSON changed (`"vl_recebido": {}`). Naming it fails as there.
#[tokio::test]
async fn unbounded_numerics_are_left_out_by_default() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(NUMERICS)
        .await
        .unwrap();
    let (engine, _spill) = numerics_engine(&host, port, Decimals::default()).await;

    let columns = rows(
        &engine,
        "SELECT table_name, column_name, data_type FROM erp.information_schema.columns \
         WHERE table_schema = 'public' AND table_name IN ('nums', 'recebimentos') \
         ORDER BY table_name, ordinal_position",
    )
    .await;
    assert_eq!(
        columns,
        [
            ["nums", "id", "integer"],
            ["nums", "n2", "decimal(10,2)"],
            ["recebimentos", "id", "integer"],
            ["recebimentos", "n2", "decimal(10,2)"],
        ]
    );
    let all = rows(
        &engine,
        "SELECT * FROM erp.public.recebimentos WHERE id < 3 ORDER BY id",
    )
    .await;
    assert_eq!(all, [["1", "1.50"], ["2", "NULL"]]);
    // Every value is read, even those no decimal holds: the column is not.
    let count = rows(&engine, "SELECT count(*) FROM erp.public.nums").await;
    assert_eq!(count, [["12"]]);
    for (sql, column) in [
        ("SELECT vl_recebido FROM erp.public.recebimentos", "vl_recebido"),
        (
            "SELECT id FROM erp.public.recebimentos ORDER BY vl_recebido DESC NULLS LAST LIMIT 51",
            "vl_recebido",
        ),
        ("SELECT id FROM erp.public.nums WHERE n > 1", "n"),
        ("SELECT a FROM erp.public.nums", "a"),
        ("SELECT w FROM erp.public.nums", "w"),
    ] {
        let error = error_of(&engine, sql).await;
        assert!(
            error.contains(&format!("No field named {column}")),
            "{sql}: {error}"
        );
    }
    // A relation whose every column is left out is refused, as there; a
    // listing of the catalog still lists it.
    let error = error_of(&engine, "SELECT * FROM erp.public.totals").await;
    assert!(
        error.contains(
            "NOT_FOUND: Table 'public.totals' has no supported columns (all 1 columns are not supported)"
        ),
        "{error}"
    );
    let listed = rows(
        &engine,
        "SELECT table_name FROM erp.information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'totals'",
    )
    .await;
    assert_eq!(listed, [["totals"]]);
}

/// `decimal_mapping = "allow_overflow"` exposes such a column as
/// `decimal(38, decimal_default_scale)`. With the default rounding mode
/// (`unnecessary`) a value is exact or fails the query, naming its column:
/// scale 20 is the `decimal(38,20)` it was before.
#[tokio::test]
async fn allow_overflow_keeps_values_exact_without_a_rounding_mode() {
    use codelake_connectors::postgres::{DecimalMapping, DecimalRoundingMode};
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(NUMERICS)
        .await
        .unwrap();
    let exact = Decimals {
        mapping: DecimalMapping::AllowOverflow,
        rounding_mode: DecimalRoundingMode::Unnecessary,
        default_scale: 20,
    };
    let (engine, _spill) = numerics_engine(&host, port, exact).await;
    let types = rows(
        &engine,
        "SELECT column_name, data_type FROM erp.information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'nums' ORDER BY ordinal_position",
    )
    .await;
    assert_eq!(
        types,
        [
            ["id", "integer"],
            ["n", "decimal(38,20)"],
            ["n2", "decimal(10,2)"],
            ["w", "decimal(38,10)"],
            ["a", "array(decimal(38,0))"],
        ]
    );
    let exact_rows = rows(
        &engine,
        "SELECT id, n FROM erp.public.nums WHERE id < 10 ORDER BY id",
    )
    .await;
    assert_eq!(
        exact_rows,
        [
            ["1", "1.50000000000000000000"],
            ["2", "123456789012345678.12345678901234567890"],
            ["3", "-999999999999999999.99999999999999999999"],
            ["4", "0.10000000000000000000"],
            ["5", "NULL"],
        ]
    );
    let sum = rows(&engine, "SELECT sum(n) FROM erp.public.nums WHERE id IN (1, 4)").await;
    assert_eq!(sum, [["1.60000000000000000000"]]);
    let total = rows(&engine, "SELECT total FROM erp.public.totals").await;
    assert_eq!(total, [["-876543210987654320.27654321098765432109"]]);
    for (id, value, why) in [
        (10, "100000000000000000000", "more than 18 integer digits"),
        (11, "0.123456789012345678901", "more than 20 fraction digits"),
        (12, "NaN", "a decimal holds only finite numbers"),
        (13, "Infinity", "a decimal holds only finite numbers"),
        (14, "-Infinity", "a decimal holds only finite numbers"),
        (15, "1000000000000000000", "more than 18 integer digits"),
        (
            16,
            "12345678901234.123456789012345678901",
            "more than 20 fraction digits",
        ),
    ] {
        let sql = format!("SELECT vl_recebido FROM erp.public.recebimentos WHERE id = {id}");
        let error = error_of(&engine, &sql).await;
        let expected = format!(
            "the PostgreSQL value {value} of column \"vl_recebido\" does not fit decimal(38,20): {why}"
        );
        assert!(error.contains(&expected), "{sql}: {error}");
    }
    // The elements of a `numeric[]` are `decimal(38,0)`: 1.5 needs rounding.
    let error = error_of(&engine, "SELECT a FROM erp.public.nums WHERE id = 1").await;
    assert!(
        error.contains(
            "the PostgreSQL value 1.5 of column \"a\" does not fit decimal(38,0): more than 0 fraction digits"
        ),
        "{error}"
    );
}

/// `allow_overflow` with a rounding mode rounds as the reference engine
/// rounds (Trino 431: `HALF_UP` to the default scale; the elements of a
/// `numeric[]` to 0 digits); a value no decimal holds still fails.
#[tokio::test]
async fn allow_overflow_rounds_with_the_catalogs_rounding_mode() {
    use codelake_connectors::postgres::{DecimalMapping, DecimalRoundingMode};
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(NUMERICS)
        .await
        .unwrap();
    let rounded = Decimals {
        mapping: DecimalMapping::AllowOverflow,
        rounding_mode: DecimalRoundingMode::HalfUp,
        default_scale: 2,
    };
    let (engine, _spill) = numerics_engine(&host, port, rounded).await;
    let values = rows(
        &engine,
        "SELECT id, n, w, a FROM erp.public.nums WHERE id < 6 ORDER BY id",
    )
    .await;
    assert_eq!(
        values,
        [
            ["1", "1.50", "1.50", "[2, , -3]"],
            ["2", "123456789012345678.12", "0.12", "NULL"],
            ["3", "-1000000000000000000.00", "NULL", "[]"],
            ["4", "0.10", "NULL", "NULL"],
            ["5", "NULL", "NULL", "NULL"],
        ]
    );
    for id in [12, 13, 14] {
        let sql = format!("SELECT n FROM erp.public.nums WHERE id = {id}");
        let error = error_of(&engine, &sql).await;
        assert!(
            error.contains("a decimal holds only finite numbers"),
            "{sql}: {error}"
        );
    }
    let wide = rows(
        &engine,
        "SELECT id, n FROM erp.public.nums WHERE id IN (10, 15, 16) ORDER BY id",
    )
    .await;
    assert_eq!(
        wide,
        [
            ["10", "100000000000000000000.00"],
            ["15", "1000000000000000000.00"],
            ["16", "12345678901234.12"],
        ]
    );
}

/// Every statement runs in the client session's time zone, never in the
/// server's or the role's default (`America/Sao_Paulo` here): a view that
/// reads `current_date` answers the client's day. The reference engine runs
/// its sessions in `Etc/UTC` whatever the client's (ISS-060); without a
/// client zone the engine's is UTC, as `Etc/UTC`. A pooled connection
/// carries no zone from one query to the next.
#[tokio::test]
async fn statements_run_in_the_client_time_zone() {
    let container = Postgres::default().with_tag("17-alpine").start().await.unwrap();
    let host = common::host();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    client(&host, port, "postgres", "postgres")
        .await
        .batch_execute(
            "CREATE VIEW today AS SELECT current_setting('TimeZone') AS zone, current_date AS today,
               (now() AT TIME ZONE 'UTC')::date AS utc_today,
               (now() AT TIME ZONE 'Pacific/Kiritimati')::date AS kiritimati_today,
               (now() AT TIME ZONE 'Etc/GMT+12')::date AS minus12_today,
               extract(hour FROM now() AT TIME ZONE 'UTC')::int AS utc_hour;
             ALTER DATABASE postgres SET timezone TO 'America/Sao_Paulo';",
        )
        .await
        .unwrap();
    let spill = tempfile::tempdir().unwrap();
    let mut engine = common::engine(spill.path());
    common::attach(&mut engine, Arc::new(connector(&host, port))).await;
    let sql =
        "SELECT zone, today, utc_today, kiritimati_today, minus12_today, utc_hour FROM erp.public.today";

    // A zone whose date differs from UTC's now: +14 from 10:00 UTC, -12
    // before 12:00 UTC.
    let utc_hour: u32 = rows_in(&engine, None, sql).await[0][5].parse().unwrap();
    let (zone, shown, column) = if utc_hour >= 11 {
        ("Pacific/Kiritimati", "Pacific/Kiritimati", 3)
    } else {
        ("-12:00", "<-12>+12", 4)
    };
    let answer = &rows_in(&engine, Some(zone), sql).await[0];
    assert_eq!(answer[0], shown);
    assert_eq!(
        answer[1], answer[column],
        "current_date is the client's day: {answer:?}"
    );
    assert_ne!(answer[1], answer[2], "and not UTC's: {answer:?}");

    let zone_of = |zone: Option<&'static str>| {
        let engine = &engine;
        async move { rows_in(engine, zone, "SELECT zone FROM erp.public.today").await[0][0].clone() }
    };
    assert_eq!(zone_of(None).await, "Etc/UTC");
    assert_eq!(zone_of(Some("UTC")).await, "Etc/UTC");
    assert_eq!(zone_of(Some("+05:45")).await, "<+05:45>-05:45");
    // More queries at once than the pool has connections, in turn.
    let zones = [
        Some("Asia/Tokyo"),
        None,
        Some("America/Sao_Paulo"),
        Some("-03:00"),
    ];
    let shown = ["Asia/Tokyo", "Etc/UTC", "America/Sao_Paulo", "<-03>+03"];
    let answers = futures::future::join_all((0..40).map(|i| zone_of(zones[i % 4]))).await;
    for (i, answer) in answers.iter().enumerate() {
        assert_eq!(answer, shown[i % 4], "query {i}");
    }
}
