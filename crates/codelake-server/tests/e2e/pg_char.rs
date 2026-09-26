//! `char(n)` on the PostgreSQL wire: a column or a cast of that type is
//! refused (`0A000`) on both protocols, where it would create unbounded text
//! or answer an unpadded value (ADR 0015); the session keeps working.

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
