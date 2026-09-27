//! Text against a number and `time ± interval` on the PostgreSQL wire
//! (ADR 0014, ISS-006): refused with PostgreSQL's SQLSTATE and wording on
//! both the simple and the extended protocol, while what PostgreSQL accepts
//! (an untyped quoted literal, explicit casts, numbers against numbers)
//! still answers the right rows; and `IS [NOT] DISTINCT FROM` grouped as
//! PostgreSQL groups it.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

async fn serve(dir: &std::path::Path) -> (super::Server, Client) {
    let (pg, http) = (super::free_port(), super::free_port());
    let config = super::write_config(dir, pg, http);
    let server = super::Server(
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
            "CREATE TABLE ops (s varchar, n int); INSERT INTO ops VALUES ('10', 5), ('9', 100), ('a', 1)",
        )
        .await
        .unwrap();
    (server, client)
}

/// First column of every row, as text, through the simple protocol.
async fn simple(client: &Client, sql: &str) -> Vec<Option<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
            _ => None,
        })
        .collect()
}

fn assert_refused(err: &tokio_postgres::Error, state: &SqlState, message: &str, sql: &str) {
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("{sql}: not a server error: {err:?}"));
    assert_eq!(db.code(), state, "{sql}: {db:?}");
    assert_eq!(db.message(), message, "{sql}");
    assert_eq!(db.severity(), "ERROR", "{sql}");
}

const REFUSED: [(&str, &str); 5] = [
    (
        "SELECT n FROM ops WHERE s > 9",
        "operator does not exist: text > integer",
    ),
    (
        "SELECT '7'::varchar = 7",
        "operator does not exist: text = integer",
    ),
    (
        "SELECT n FROM ops WHERE s IN (1, 2)",
        "operator does not exist: text = integer",
    ),
    (
        "SELECT n FROM ops WHERE s BETWEEN 1 AND 9",
        "operator does not exist: text >= integer",
    ),
    (
        "SELECT n FROM ops WHERE n = s",
        "operator does not exist: integer = text",
    ),
];

#[tokio::test]
async fn postgres_wire_refuses_text_against_a_number_like_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, client) = serve(dir.path()).await;
    for (sql, message) in REFUSED {
        let err = client.simple_query(sql).await.unwrap_err();
        assert_refused(&err, &SqlState::UNDEFINED_FUNCTION, message, sql);
        let hint = err.as_db_error().unwrap().hint().unwrap_or_default();
        assert!(hint.contains("explicit type casts"), "{sql}: {hint}");
        let err = client.query(sql, &[]).await.unwrap_err();
        assert_refused(&err, &SqlState::UNDEFINED_FUNCTION, message, sql);
    }
    let sql = "SELECT TIME '10:00:00' + INTERVAL '1 hour'";
    let message = "time without time zone + interval is not supported";
    let err = client.simple_query(sql).await.unwrap_err();
    assert_refused(&err, &SqlState::FEATURE_NOT_SUPPORTED, message, sql);
    let err = client.query(sql, &[]).await.unwrap_err();
    assert_refused(&err, &SqlState::FEATURE_NOT_SUPPORTED, message, sql);
    // The session is still usable after a refusal.
    assert_eq!(
        simple(&client, "SELECT count(*) FROM ops").await,
        [Some("3".to_owned())]
    );
}

#[tokio::test]
async fn postgres_wire_keeps_untyped_literals_casts_and_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, client) = serve(dir.path()).await;
    let text = |values: &[&str]| values.iter().map(|v| Some((*v).to_owned())).collect::<Vec<_>>();
    // Every answer is projected as text so both protocols read it alike.
    for (sql, rows) in [
        // A quoted literal takes the column's type, as in PostgreSQL.
        (
            "SELECT CAST(n AS varchar) AS v FROM ops WHERE n > '6' ORDER BY n",
            text(&["100"]),
        ),
        ("SELECT s FROM ops WHERE s = '10'", text(&["10"])),
        (
            "SELECT CAST(n AS varchar) AS v FROM ops WHERE n IN ('1', '5') ORDER BY n",
            text(&["1", "5"]),
        ),
        // Explicit casts: text compares as text ('10' < '9' < 'a').
        (
            "SELECT s FROM ops WHERE s > CAST(9 AS varchar) ORDER BY s",
            text(&["a"]),
        ),
        (
            "SELECT CAST(n AS varchar) AS v FROM ops WHERE CAST(n AS varchar) = '100'",
            text(&["100"]),
        ),
        // Numbers against numbers.
        (
            "SELECT CAST(n AS varchar) AS v FROM ops WHERE n > 4.5 ORDER BY n",
            text(&["5", "100"]),
        ),
    ] {
        assert_eq!(simple(&client, sql).await, rows, "{sql}");
        let extended: Vec<Option<String>> = client
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(extended, rows, "extended: {sql}");
    }
    // A parameter takes the column's type too.
    let rows = client
        .query("SELECT s FROM ops WHERE n = $1", &[&100_i32])
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|r| r.get(0)).collect::<Vec<String>>(), ["9"]);
}

/// `IS [NOT] DISTINCT FROM` binds tighter than `AND` and `OR` in PostgreSQL;
/// the parser took the rest of the condition as its right operand, and over
/// booleans answered other rows. Rows as PostgreSQL 17 answers them.
#[tokio::test]
async fn postgres_wire_groups_is_distinct_from_like_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, client) = serve(dir.path()).await;
    client
        .batch_execute("CREATE TABLE flags (id int, f boolean); INSERT INTO flags VALUES (1, true), (2, false), (3, NULL)")
        .await
        .unwrap();
    let text = |values: &[&str]| values.iter().map(|v| Some((*v).to_owned())).collect::<Vec<_>>();
    for (sql, rows) in [
        (
            "SELECT CAST(a.id AS varchar) AS v FROM flags a JOIN flags b \
             ON a.f IS NOT DISTINCT FROM b.f AND a.id = b.id ORDER BY a.id",
            text(&["1", "2", "3"]),
        ),
        (
            "SELECT CAST(id AS varchar) AS v FROM flags WHERE f IS DISTINCT FROM true AND id > 1 ORDER BY id",
            text(&["2", "3"]),
        ),
        (
            "SELECT CAST(id AS varchar) AS v FROM flags WHERE NOT f IS DISTINCT FROM false OR id = 3 ORDER BY id",
            text(&["2", "3"]),
        ),
    ] {
        assert_eq!(simple(&client, sql).await, rows, "{sql}");
        let extended: Vec<Option<String>> = client
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(extended, rows, "extended: {sql}");
    }
}
