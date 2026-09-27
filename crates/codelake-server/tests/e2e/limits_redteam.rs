//! Independent red-team probe of ISS-017 through the real binary: a
//! `SIGKILL` at points spread over a `CREATE OR REPLACE TABLE`, the server
//! restarted each time over the same warehouse.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

fn config(dir: &Path, warehouse: &Path, pg: u16, http: u16, trino: u16) -> PathBuf {
    let path = super::write_config(dir, pg, http);
    let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    write!(
        file,
        "\n[server.trino]\nlisten = \"127.0.0.1:{trino}\"\n\n[[source]]\nname = \"hive\"\nkind = \"hive\"\nwarehouse = \"{}\"\n",
        warehouse.display()
    )
    .unwrap();
    path
}

struct Running {
    server: super::Server,
    client: Client,
    http: u16,
}

async fn start(dir: &Path, warehouse: &Path) -> Running {
    let (pg, admin, http) = (super::free_port(), super::free_port(), super::free_port());
    let config = config(dir, warehouse, pg, admin, http);
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
    let info = format!("http://127.0.0.1:{http}/v1/info");
    while reqwest::get(&info).await.is_err() {
        assert!(Instant::now() < deadline, "Trino listener did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let dsn = format!("host=127.0.0.1 port={pg} user=codelake dbname=codelake");
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
    tokio::spawn(connection);
    Running { server, client, http }
}

async fn simple(client: &Client, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let messages = client.simple_query(sql).await.map_err(|e| format!("{e:?}"))?;
    Ok(messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => {
                Some((0..row.len()).map(|i| row.get(i).map(str::to_owned)).collect())
            }
            _ => None,
        })
        .collect())
}

fn one(values: &[&str]) -> Vec<Vec<Option<String>>> {
    vec![values.iter().map(|v| Some((*v).to_owned())).collect()]
}

fn snapshot(dir: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let name = path.strip_prefix(root).unwrap().display().to_string();
                out.push((name, std::fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// Staging directories left under the warehouse, and how many of them hold
/// a whole table's metadata (a replaced table left behind).
fn leftovers(warehouse: &Path) -> (usize, usize) {
    let staging = warehouse.join(".codelake/staging");
    let Ok(entries) = std::fs::read_dir(&staging) else {
        return (0, 0);
    };
    let dirs: Vec<PathBuf> = entries.map(|e| e.unwrap().path()).collect();
    let tables = dirs.iter().filter(|d| d.join(".codelakeSchema").exists()).count();
    (dirs.len(), tables)
}

/// A `SIGKILL` at any point of `CREATE OR REPLACE`: after a restart the
/// table is the whole old one or the whole new one, the table directory
/// holds exactly one version's files, and nothing of a replaced table
/// stays behind forever.
#[tokio::test]
async fn sigkill_during_or_replace_leaves_the_old_or_the_new_table() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let old = one(&["10", "55"]);
    let new = one(&["400000", "80000200000"]);
    let replace = "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(value AS BIGINT) AS v, \
                   rpad(CAST(value AS varchar), 64, 'x') AS pad FROM generate_series(1, 400000)";
    let query = "SELECT count(*), sum(v) FROM hive.s.t";

    // How long a replacement takes, to spread the kills over it.
    let running = start(dir.path(), &warehouse).await;
    http_ok(running.http, "CREATE SCHEMA hive.s").await;
    let started = Instant::now();
    http_ok(running.http, replace).await;
    let took = started.elapsed();
    assert_eq!(simple(&running.client, query).await.unwrap(), new);
    drop(running);

    let mut outcomes = Vec::new();
    for round in 0..14_u32 {
        let running = start(dir.path(), &warehouse).await;
        http_ok(
            running.http,
            "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(x AS BIGINT) AS v, 'p' AS pad \
             FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) v(x)",
        )
        .await;
        assert_eq!(simple(&running.client, query).await.unwrap(), old);
        let before = snapshot(&warehouse.join("s/t"));
        let port = running.http;
        let writer = tokio::spawn(async move { http(port, replace).await });
        // Most kills near the end, where the swap and the cleanup happen.
        let delay = took.mul_f64(0.5 + f64::from(round) * 0.05);
        tokio::time::sleep(delay).await;
        let mut server = running.server;
        server.0.kill().unwrap();
        let _ = server.0.wait();
        let _ = writer.await;

        let after = start(dir.path(), &warehouse).await;
        let answer = simple(&after.client, query).await.unwrap();
        let state = if answer == old {
            assert_eq!(
                snapshot(&warehouse.join("s/t")),
                before,
                "round {round}: the old table changed"
            );
            "old"
        } else {
            assert_eq!(answer, new, "round {round}: neither version");
            "new"
        };
        outcomes.push((round, delay, state, leftovers(&warehouse)));
        drop(after);
    }
    eprintln!("outcomes: {outcomes:#?}");
    let running = start(dir.path(), &warehouse).await;
    simple(&running.client, "SELECT 1").await.unwrap();
    let (dirs, tables) = leftovers(&warehouse);
    drop(running);
    assert_eq!(
        tables, 0,
        "replaced tables left in staging after restarts ({dirs} staging directories): {outcomes:#?}"
    );
}

/// One statement over the Trino HTTP protocol: column names and rows.
async fn http(port: u16, sql: &str) -> (Vec<String>, Vec<serde_json::Value>, Option<String>) {
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let mut page: serde_json::Value = serde_json::from_str(
        &client
            .post(format!("{base}/v1/statement"))
            .header("X-Trino-User", "codelake")
            .body(sql.to_owned())
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
    )
    .unwrap();
    let mut names = Vec::new();
    let mut rows = Vec::new();
    loop {
        if names.is_empty()
            && let Some(columns) = page.get("columns").and_then(serde_json::Value::as_array)
        {
            names = columns
                .iter()
                .map(|c| c["name"].as_str().unwrap_or_default().to_owned())
                .collect();
        }
        if let Some(data) = page.get("data").and_then(serde_json::Value::as_array) {
            rows.extend(data.iter().cloned());
        }
        if let Some(error) = page.get("error") {
            return (names, rows, Some(error["message"].to_string()));
        }
        let Some(next) = page.get("nextUri").and_then(serde_json::Value::as_str) else {
            return (names, rows, None);
        };
        let next = next.to_owned();
        page = serde_json::from_str(
            &client
                .get(next)
                .header("X-Trino-User", "codelake")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
        )
        .unwrap();
    }
}

/// A statement over the Trino HTTP protocol that must succeed (DDL and
/// writes go through the engine there).
async fn http_ok(port: u16, sql: &str) {
    let (_, _, error) = http(port, sql).await;
    assert_eq!(error, None, "{sql}");
}

// Each test below failed before its fix and proves a defect of the tester's
// report.

/// A client that prepared a statement (a JDBC driver does it after five
/// runs; psycopg and Npgsql too) runs it again after the table it reads was
/// replaced by one with the same columns. PostgreSQL re-plans it; the
/// statement must answer the new table's rows.
#[tokio::test]
async fn a_prepared_statement_answers_after_or_replace() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let running = start(dir.path(), &warehouse).await;
    let client = &running.client;
    http_ok(running.http, "CREATE SCHEMA hive.s").await;
    http_ok(
        running.http,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    let statement = client
        .prepare("SELECT CAST(sum(v) AS VARCHAR) FROM hive.s.t")
        .await
        .unwrap();
    let first: String = client.query_one(&statement, &[]).await.unwrap().get(0);
    assert_eq!(first, "1");
    http_ok(
        running.http,
        "CREATE OR REPLACE TABLE hive.s.t AS SELECT CAST(2 AS BIGINT) AS v",
    )
    .await;
    let mut answers = Vec::new();
    for _ in 0..3 {
        answers.push(match client.query_one(&statement, &[]).await {
            Ok(row) => Ok(row.get::<_, String>(0)),
            Err(err) => Err(err.as_db_error().map_or_else(
                || format!("{err:?}"),
                |db| format!("{}: {}", db.code().code(), db.message()),
            )),
        });
    }
    assert_eq!(
        answers,
        vec![Ok("2".to_owned()), Ok("2".to_owned()), Ok("2".to_owned())],
        "the prepared statement after CREATE OR REPLACE"
    );
    drop(running.server);
}

/// A prepared statement whose table was replaced by one with other columns
/// is refused as `PostgreSQL` refuses it (`0A000`, the columns `Describe`
/// announced would change), on every run; a statement prepared again
/// answers the new table.
#[tokio::test]
async fn a_prepared_statement_is_refused_when_the_replacement_changes_its_columns() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let running = start(dir.path(), &warehouse).await;
    let client = &running.client;
    http_ok(running.http, "CREATE SCHEMA hive.s").await;
    http_ok(
        running.http,
        "CREATE TABLE hive.s.t AS SELECT CAST(1 AS BIGINT) AS v",
    )
    .await;
    let statement = client.prepare("SELECT * FROM hive.s.t").await.unwrap();
    assert_eq!(
        client.query_one(&statement, &[]).await.unwrap().get::<_, i64>(0),
        1
    );
    http_ok(
        running.http,
        "CREATE OR REPLACE TABLE hive.s.t AS SELECT 'two' AS v",
    )
    .await;
    for _ in 0..2 {
        let err = client.query_one(&statement, &[]).await.unwrap_err();
        let db = err.as_db_error().unwrap_or_else(|| panic!("{err:?}"));
        assert_eq!(db.code().code(), "0A000", "{db:?}");
        assert_eq!(db.message(), "cached plan must not change result type");
    }
    let statement = client.prepare("SELECT * FROM hive.s.t").await.unwrap();
    assert_eq!(
        client
            .query_one(&statement, &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "two"
    );
    drop(running.server);
}

/// Mixed-case names over both protocols: every spelling reads the same
/// rows, and the files carry the written spelling.
#[tokio::test]
async fn mixed_case_names_over_both_protocols() {
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let running = start(dir.path(), &warehouse).await;
    let client = &running.client;
    let mut failures: Vec<String> = Vec::new();
    http_ok(running.http, "CREATE SCHEMA hive.\"Vendas\"").await;
    http_ok(
        running.http,
        "CREATE TABLE hive.\"Vendas\".\"Pedido\" AS SELECT CAST(x AS BIGINT) AS \"CustomerCode\", \
         CAST(x * 10 AS BIGINT) AS \"Valor\" FROM (VALUES (1), (2), (3)) v(x)",
    )
    .await;
    for sql in [
        "SELECT CAST(sum(customercode) AS VARCHAR), CAST(sum(valor) AS VARCHAR) FROM hive.vendas.pedido",
        "SELECT CAST(sum(\"CustomerCode\") AS VARCHAR), CAST(sum(\"Valor\") AS VARCHAR) FROM hive.\"Vendas\".\"Pedido\"",
        "SELECT CAST(sum(CUSTOMERCODE) AS VARCHAR), CAST(sum(VALOR) AS VARCHAR) FROM hive.VENDAS.PEDIDO",
    ] {
        let answer = simple(client, sql).await;
        if answer != Ok(one(&["6", "60"])) {
            failures.push(format!("pg simple: {sql}: {answer:?}"));
        }
        match client.prepare(sql).await {
            Ok(statement) => match client.query_one(&statement, &[]).await {
                Ok(row) => {
                    let got = (row.get::<_, String>(0), row.get::<_, String>(1));
                    if got != ("6".to_owned(), "60".to_owned()) {
                        failures.push(format!("pg extended: {sql}: {got:?}"));
                    }
                }
                Err(err) => failures.push(format!("pg extended: {sql}: {err:?}")),
            },
            Err(err) => failures.push(format!("pg extended prepare: {sql}: {err:?}")),
        }
        let (_, rows, error) = http(running.http, sql).await;
        if error.is_some() || rows != vec![serde_json::json!(["6", "60"])] {
            failures.push(format!("http: {sql}: {rows:?} {error:?}"));
        }
    }
    let quoted = "INSERT INTO hive.vendas.pedido (\"Valor\", \"CustomerCode\") VALUES (50, 5)";
    let (_, _, error) = http(running.http, quoted).await;
    if let Some(error) = error {
        failures.push(format!("http: {quoted}: {error}"));
    }
    assert!(failures.is_empty(), "{failures:#?}");
    let (names, _, error) = http(
        running.http,
        "SELECT \"CustomerCode\", valor FROM hive.vendas.pedido WHERE customercode = 2",
    )
    .await;
    assert_eq!(error, None);
    assert_eq!(names, vec!["CustomerCode".to_owned(), "valor".to_owned()]);
    let (names, rows, error) = http(
        running.http,
        "SELECT column_name FROM hive.information_schema.columns WHERE table_schema = 'vendas' AND table_name = 'pedido'",
    )
    .await;
    assert_eq!(error, None);
    assert_eq!(names, vec!["column_name".to_owned()]);
    assert_eq!(rows.len(), 2, "{rows:?}");
    http_ok(running.http, "INSERT INTO hive.vendas.pedido VALUES (4, 40)").await;
    let files: Vec<PathBuf> = std::fs::read_dir(warehouse.join("vendas/pedido"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| !p.file_name().unwrap().to_string_lossy().starts_with(['.', '_']))
        .collect();
    assert!(files.len() >= 2, "{files:?}");
    drop(running.server);
}
