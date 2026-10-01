//! `char(n)` on the PostgreSQL wire: a column or a cast of that type is
//! refused (`0A000`) on both protocols, where it would create unbounded text
//! or a value compared as text (ADR 0015); the session keeps working.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

const REFUSED: [&str; 4] = [
    "CREATE TABLE c (id bigint, ch char(5))",
    "SELECT CAST('ab' AS char(5))",
    "SELECT 'ab'::char(4)",
    "ALTER TABLE ok ADD COLUMN ch char(3)",
];

#[tokio::test]
async fn postgres_wire_refuses_char_types() {
    let dir = tempfile::tempdir().unwrap();
    let (pg, http) = (super::free_port(), super::free_port());
    let config = super::write_config(dir.path(), pg, http);
    let _server = super::Server(
        Command::new(super::BIN)
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::net::TcpStream::connect(("127.0.0.1", pg)).is_err() {
        assert!(Instant::now() < deadline, "server did not listen");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
        .batch_execute("CREATE TABLE ok (id bigint, v varchar(5)); INSERT INTO ok VALUES (1, 'ab')")
        .await
        .unwrap();
    for sql in REFUSED {
        for err in [
            client.simple_query(sql).await.unwrap_err(),
            client.query(sql, &[]).await.unwrap_err(),
        ] {
            let db = err
                .as_db_error()
                .unwrap_or_else(|| panic!("{sql}: not a server error: {err:?}"));
            assert_eq!(db.code(), &SqlState::FEATURE_NOT_SUPPORTED, "{sql}: {db:?}");
            assert!(
                db.message()
                    .contains("char(n) type is not supported on the PostgreSQL wire"),
                "{sql}: {db:?}"
            );
        }
    }
    let rows: Vec<Option<String>> = client
        .simple_query("SELECT v FROM ok")
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
            _ => None,
        })
        .collect();
    assert_eq!(rows, [Some("ab".to_owned())]);
}

/// Rows of `sql` over the simple protocol, `NULL` as `None`.
async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                Some((0..row.len()).map(|i| row.get(i).map(str::to_owned)).collect())
            }
            _ => None,
        })
        .collect()
}

/// `varchar(n)` on the PostgreSQL wire keeps its length, as on the Trino
/// protocol (rows of Trino 431): a cast cuts, a column checks what an
/// `INSERT` writes into it, and a `CREATE TABLE … AS` keeps the cut values.
#[tokio::test]
async fn postgres_wire_keeps_varchar_lengths() {
    let dir = tempfile::tempdir().unwrap();
    let (pg, http) = (super::free_port(), super::free_port());
    let config = super::write_config(dir.path(), pg, http);
    let _server = super::Server(
        Command::new(super::BIN)
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::net::TcpStream::connect(("127.0.0.1", pg)).is_err() {
        assert!(Instant::now() < deadline, "server did not listen");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(connection);
    client
        .batch_execute(
            "CREATE TABLE t (id bigint, v varchar); INSERT INTO t VALUES (1, 'ab'), (2, 'b'), (3, 'abc')",
        )
        .await
        .unwrap();
    let some = |v: &str| Some(v.to_owned());
    // Casts, on both protocols.
    let sql = "SELECT CAST('abc' AS varchar(2)), 'ab'::varchar(1), TRY_CAST('abc' AS varchar(1))";
    assert_eq!(
        simple_rows(&client, sql).await,
        [vec![some("ab"), some("a"), some("a")]]
    );
    let row = client.query_one(sql, &[]).await.unwrap();
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        ("ab".to_owned(), "a".to_owned())
    );
    let sql = "SELECT id FROM t WHERE CAST(v AS varchar(1)) = 'a' ORDER BY id";
    assert_eq!(
        simple_rows(&client, sql).await,
        [vec![some("1")], vec![some("3")]]
    );
    let ids: Vec<i64> = client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(ids, [1, 3]);
    // A `varchar(2)` column checks each value an `INSERT` writes.
    client
        .batch_execute("CREATE TABLE w (w varchar(2))")
        .await
        .unwrap();
    for (sql, refused) in [
        ("INSERT INTO w VALUES ('ab')", false),
        ("INSERT INTO w VALUES ('abcdef')", true),
        ("INSERT INTO w VALUES ('a   ')", false),
    ] {
        match client.simple_query(sql).await {
            Ok(_) => assert!(!refused, "{sql} was accepted"),
            Err(err) => {
                assert!(refused, "{sql}: {err}");
                let db = err.as_db_error().unwrap();
                assert!(
                    db.message().contains(
                        "Cannot truncate non-space characters when casting from varchar(6) to varchar(2) on INSERT"
                    ),
                    "{sql}: {db:?}"
                );
            }
        }
    }
    let err = client
        .execute("INSERT INTO w VALUES ('xyz')", &[])
        .await
        .unwrap_err();
    let db = err.as_db_error().unwrap();
    assert!(
        db.message().contains("Cannot truncate non-space characters"),
        "{db:?}"
    );
    assert_eq!(
        simple_rows(&client, "SELECT w, length(w) FROM w ORDER BY w").await,
        [vec![some("a "), some("2")], vec![some("ab"), some("2")]]
    );
    // A `CREATE TABLE … AS` keeps what the cast cut.
    client
        .batch_execute("CREATE TABLE c AS SELECT id, CAST(v AS varchar(2)) AS v2 FROM t")
        .await
        .unwrap();
    assert_eq!(
        simple_rows(&client, "SELECT id, v2, length(v2) FROM c ORDER BY id").await,
        [
            vec![some("1"), some("ab"), some("2")],
            vec![some("2"), some("b"), some("1")],
            vec![some("3"), some("ab"), some("2")],
        ]
    );
}
