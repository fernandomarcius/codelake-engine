//! What metadata clients (`DBeaver`, the JDBC driver, ODBC tools) read about a
//! SQL Server catalog, row by row against a real Trino 431 reading the same
//! SQL Server 2022: every `system.jdbc` and `system.metadata` table,
//! `information_schema`, `DESCRIBE` of the system tables and the `SHOW`
//! statements, compared by column names, column types and rows; then every
//! `DatabaseMetaData` listing of the Trino JDBC driver 431 against both
//! (ISS-064). Runs in the integration lane.
//!
//! The reference engine has only the SQL Server catalog (its image's default
//! catalogs are removed), as `CodeLake`, whose own in-memory catalog is not
//! listed while unused.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    reason = "test crate"
)]

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{IpAddr, TcpListener, UdpSocket};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;
use testcontainers::core::wait::ExitWaitStrategy;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const BIN: &str = env!("CARGO_BIN_EXE_codelake");
const SA_PASSWORD: &str = "Jdbc-Metadata-Only-1";

/// The SQL Server database both engines read. `dbo.tipos` has a column of
/// every type both engines type alike; `dbo.divergentes` the types whose
/// mapping still differs (ISS-064 item 5); `vendas` is empty.
const SEED: &str = "
EXEC('CREATE SCHEMA vendas');
EXEC('CREATE SCHEMA Financeiro');
CREATE TABLE Financeiro.Contas (Id INT, Nome VARCHAR(10));
CREATE TABLE dbo.tipos (id INT NOT NULL PRIMARY KEY, b BIT, ti TINYINT, si SMALLINT, bi BIGINT, r REAL,
  f FLOAT, d DECIMAL(10,2), n NUMERIC(38,0), m MONEY, vc VARCHAR(20), vmax VARCHAR(MAX), nvc NVARCHAR(30),
  t TEXT, dt DATE, tm TIME(3), dtm DATETIME, dtm2 DATETIME2(3), sdt SMALLDATETIME, dto DATETIMEOFFSET(3),
  bin BINARY(4), vbin VARBINARY(MAX));
EXEC('CREATE VIEW dbo.v_tipos AS SELECT id, vc FROM dbo.tipos');
CREATE TABLE dbo.divergentes (c CHAR(3), u UNIQUEIDENTIFIER, dtm7 DATETIME2(7));
";

fn host() -> String {
    std::env::var("TESTCONTAINERS_HOST_OVERRIDE").unwrap_or_else(|_| "127.0.0.1".to_owned())
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// This container's address on the Docker bridge, reachable by client
/// containers (no packet is sent: `connect` on UDP only picks a route).
fn own_ip() -> IpAddr {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    socket.local_addr().unwrap().ip()
}

fn name(tag: &str) -> String {
    format!("codelake-jdbc-meta-{tag}-{}", std::process::id())
}

async fn tsql(port: u16, database: &str, batch: &str) {
    let mut config = Config::new();
    config.host(host());
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

async fn sql_server() -> (ContainerAsync<MssqlServer>, u16) {
    let mssql = MssqlServer::default()
        .with_sa_password(SA_PASSWORD)
        .with_accept_eula()
        .with_tag("2022-latest")
        .with_container_name(name("mssql"))
        .start()
        .await
        .unwrap();
    let port = mssql.get_host_port_ipv4(1433).await.unwrap();
    tsql(port, "master", "CREATE DATABASE warehouse").await;
    tsql(port, "warehouse", SEED).await;
    (mssql, port)
}

struct Server {
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    async fn start(mssql_port: u16) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (pg, http, port) = (free_port(), free_port(), free_port());
        let path = dir.path().join("codelake.toml");
        let mut file = std::fs::File::create(&path).unwrap();
        write!(
            file,
            r#"
[server]
pg_listen = "127.0.0.1:{pg}"
http_listen = "127.0.0.1:{http}"
shutdown_grace_secs = 1
# The JDBC client container reaches the Trino listener over the Docker bridge.
allow_unauthenticated_network = true

[server.trino]
listen = "0.0.0.0:{port}"
header_prefix = "X-Trino-"
reported_version = "431"

[memory]
limit = "1GiB"
spill_dir = "{spill}"
spill_limit = "1GiB"
metadata_cache_limit = "8MiB"

[[source]]
name = "warehouse"
kind = "sqlserver"
host = "{host}"
port = {mssql_port}
database = "warehouse"
user = "sa"
password_env = "JDBC_META_SA_PASSWORD"
encrypt = "required"
trust_server_certificate = true
"#,
            spill = dir.path().join("spill").display(),
            host = host(),
        )
        .unwrap();
        let child = Command::new(BIN)
            .args(["serve", "--config"])
            .arg(&path)
            .env("JDBC_META_SA_PASSWORD", SA_PASSWORD)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let server = Self {
            child,
            port,
            _dir: dir,
        };
        wait_info(&server.base()).await;
        server
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

async fn wait_info(base: &str) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Ok(response) = reqwest::get(format!("{base}/v1/info")).await
            && response.status() == 200
            && let Ok(body) = response.text().await
            && body.contains("\"starting\":false")
        {
            return;
        }
        assert!(Instant::now() < deadline, "{base} did not start");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Trino 431 with the SQL Server catalog `warehouse` only.
async fn trino_431(mssql_port: u16) -> (ContainerAsync<GenericImage>, String) {
    const JVM: &str = "-server\n-Xmx1G\n-XX:+UseG1GC\n-XX:G1HeapRegionSize=32M\n\
        -XX:+ExplicitGCInvokesConcurrent\n-XX:+ExitOnOutOfMemoryError\n\
        -XX:-OmitStackTraceInFastThrow\n-XX:ReservedCodeCacheSize=150M\n\
        -Djdk.attach.allowAttachSelf=true\n-Djdk.nio.maxCachedBufferSize=2000000\n";
    let catalog = format!(
        "connector.name=sqlserver\n\
         connection-url=jdbc:sqlserver://{}:{mssql_port};database=warehouse;encrypt=false\n\
         connection-user=sa\n\
         connection-password={SA_PASSWORD}\n",
        host()
    );
    let container = GenericImage::new("trinodb/trino", "431")
        .with_exposed_port(ContainerPort::Tcp(8080))
        .with_wait_for(WaitFor::message_on_either_std("SERVER STARTED"))
        .with_entrypoint("sh")
        .with_cmd([
            "-c",
            "rm -f /etc/trino/catalog/jmx.properties /etc/trino/catalog/memory.properties \
             /etc/trino/catalog/tpch.properties /etc/trino/catalog/tpcds.properties \
             && exec /usr/lib/trino/bin/run-trino",
        ])
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/jvm.config").with_mode(0o644),
            JVM.as_bytes().to_vec(),
        )
        .with_copy_to(
            CopyTargetOptions::new("/etc/trino/catalog/warehouse.properties").with_mode(0o644),
            catalog.into_bytes(),
        )
        .with_container_name(name("trino"))
        .with_startup_timeout(Duration::from_secs(240))
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(8080).await.unwrap();
    let base = format!("http://{}:{port}", container.get_host().await.unwrap());
    wait_info(&base).await;
    // The coordinator logs SERVER STARTED before its worker registers.
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let nodes = run(
            &base,
            "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'",
            None,
        )
        .await;
        if nodes
            .rows
            .first()
            .and_then(|row| row[0].as_u64())
            .is_some_and(|n| n > 0)
        {
            break;
        }
        assert!(Instant::now() < deadline, "Trino never listed an active worker");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    (container, base)
}

/// A statement's answer: column names and types and rows, or its error.
#[derive(Debug, Clone, PartialEq)]
struct Answer {
    names: Vec<String>,
    types: Vec<String>,
    rows: Vec<Vec<Value>>,
    /// `errorName` and `errorCode` of a failure.
    error: Option<(String, i64)>,
    message: String,
}

async fn run(base: &str, sql: &str, catalog: Option<&str>) -> Answer {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "meta")
        .header("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
        .timeout(Duration::from_secs(120))
        .body(sql.to_owned());
    if let Some(catalog) = catalog {
        request = request.header("X-Trino-Catalog", catalog);
    }
    let mut page: Value =
        serde_json::from_slice(&request.send().await.unwrap().bytes().await.unwrap()).unwrap();
    let mut answer = Answer {
        names: Vec::new(),
        types: Vec::new(),
        rows: Vec::new(),
        error: None,
        message: String::new(),
    };
    let deadline = Instant::now() + Duration::from_secs(240);
    loop {
        if answer.names.is_empty()
            && let Some(columns) = page.get("columns").and_then(Value::as_array)
        {
            for column in columns {
                answer.names.push(column["name"].as_str().unwrap().to_owned());
                answer.types.push(column["type"].as_str().unwrap().to_owned());
            }
        }
        if let Some(rows) = page.get("data").and_then(Value::as_array) {
            answer
                .rows
                .extend(rows.iter().map(|row| row.as_array().unwrap().clone()));
        }
        if let Some(error) = page.get("error") {
            answer.error = Some((
                error["errorName"].as_str().unwrap_or_default().to_owned(),
                error["errorCode"].as_i64().unwrap_or_default(),
            ));
            answer.message = error["message"].as_str().unwrap_or_default().to_owned();
        }
        let Some(next) = page.get("nextUri").and_then(Value::as_str).map(str::to_owned) else {
            break;
        };
        assert!(Instant::now() < deadline, "{sql} did not finish");
        let body = client
            .get(next)
            .header("X-Trino-User", "meta")
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        page = serde_json::from_slice(&body).unwrap();
    }
    answer
}

/// The rows of `answer` as sorted text, without those whose key (the first
/// four columns) is in `skip`.
fn sorted_rows(answer: &Answer, skip: &[Vec<Value>]) -> Vec<String> {
    let mut rows: Vec<String> = answer
        .rows
        .iter()
        .filter(|row| !skip.iter().any(|key| row.len() >= 4 && row[..4] == key[..]))
        .map(|row| serde_json::to_string(row).unwrap())
        .collect();
    rows.sort();
    rows
}

/// Whether a column type is one the SQL Server connector still maps
/// differently (ISS-064 item 5: `char(n)`, `uniqueidentifier`
/// as `char(36)`, 7 fraction digits).
fn divergent_type(name: &str) -> bool {
    name.starts_with("char(") || name.contains("(7)")
}

/// One statement on both engines; the differences, if any. `type_column` is
/// where a column listing names its type: rows of a divergent type
/// ([`divergent_type`]) are left out of the comparison, by their key.
async fn compare(
    trino: &str,
    codelake: &str,
    sql: &str,
    catalog: Option<&str>,
    type_column: Option<usize>,
) -> Option<String> {
    let want = run(trino, sql, catalog).await;
    let got = run(codelake, sql, catalog).await;
    if want.error.is_some() || got.error.is_some() {
        return (want.error != got.error).then(|| {
            format!(
                "{sql}\n  trino:    {:?} {}\n  codelake: {:?} {}",
                want.error, want.message, got.error, got.message
            )
        });
    }
    let mut problems = Vec::new();
    if want.names != got.names {
        problems.push(format!("names {:?} vs {:?}", want.names, got.names));
    }
    if want.types != got.types {
        problems.push(format!("types {:?} vs {:?}", want.types, got.types));
    }
    let skip: Vec<Vec<Value>> = type_column
        .map(|index| {
            want.rows
                .iter()
                .filter(|row| row[index].as_str().is_some_and(divergent_type))
                .map(|row| row[..4].to_vec())
                .collect()
        })
        .unwrap_or_default();
    let (want_rows, got_rows) = (sorted_rows(&want, &skip), sorted_rows(&got, &skip));
    if want_rows != got_rows {
        let only = |a: &[String], b: &[String]| -> Vec<String> {
            a.iter().filter(|r| !b.contains(r)).take(10).cloned().collect()
        };
        problems.push(format!(
            "rows {} vs {}; only trino {:?}; only codelake {:?}",
            want_rows.len(),
            got_rows.len(),
            only(&want_rows, &got_rows),
            only(&got_rows, &want_rows)
        ));
    }
    (!problems.is_empty()).then(|| format!("{sql}\n  {}", problems.join("\n  ")))
}

const JDBC: [&str; 13] = [
    "catalogs",
    "schemas",
    "tables",
    "columns",
    "table_types",
    "types",
    "procedures",
    "procedure_columns",
    "pseudo_columns",
    "super_types",
    "super_tables",
    "udts",
    "attributes",
];

const METADATA: [&str; 8] = [
    "catalogs",
    "schema_properties",
    "table_properties",
    "column_properties",
    "materialized_view_properties",
    "analyze_properties",
    "materialized_views",
    "table_comments",
];

/// Every `DatabaseMetaData` listing a metadata client reads, printed per
/// section (`## name | column:type …`, then one line per row).
const META_PROBE: &str = r###"
import java.sql.*;

public class Meta {
    interface Call { ResultSet run() throws SQLException; }

    static void dump(String label, Call call) {
        try (ResultSet rs = call.run()) {
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder h = new StringBuilder("## " + label + " |");
            for (int i = 1; i <= md.getColumnCount(); i++) h.append(' ').append(md.getColumnLabel(i)).append(':').append(md.getColumnTypeName(i));
            System.out.println(h);
            while (rs.next()) {
                StringBuilder sb = new StringBuilder("  ");
                for (int i = 1; i <= md.getColumnCount(); i++) { if (i > 1) sb.append(" | "); sb.append(rs.getString(i)); }
                System.out.println(sb);
            }
        } catch (SQLException | RuntimeException e) {
            System.out.println("## " + label + " ERROR " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        String cat = "warehouse", schema = "dbo", table = "tipos";
        try (Connection c = DriverManager.getConnection(args[0], "meta", null)) {
            DatabaseMetaData dm = c.getMetaData();
            System.out.println("## product " + dm.getDatabaseProductName() + " " + dm.getDatabaseProductVersion());
            dump("getCatalogs", dm::getCatalogs);
            dump("getSchemas()", dm::getSchemas);
            dump("getSchemas(cat)", () -> dm.getSchemas(cat, null));
            dump("getSchemas(cat, db_%)", () -> dm.getSchemas(cat, "db\\_%"));
            dump("getTableTypes", dm::getTableTypes);
            dump("getTypeInfo", dm::getTypeInfo);
            dump("getTables(cat, schema)", () -> dm.getTables(cat, schema, null, null));
            dump("getTables(cat, financeiro, TABLE)", () -> dm.getTables(cat, "financeiro", null, new String[]{"TABLE"}));
            dump("getTables(cat, sys, objects)", () -> dm.getTables(cat, "sys", "objects", null));
            dump("getColumns(cat, schema, table)", () -> dm.getColumns(cat, schema, table, null));
            dump("getColumns(cat, schema, v_tipos)", () -> dm.getColumns(cat, schema, "v_tipos", null));
            dump("getProcedures", () -> dm.getProcedures(cat, schema, null));
            dump("getProcedureColumns", () -> dm.getProcedureColumns(cat, schema, null, null));
            dump("getPseudoColumns", () -> dm.getPseudoColumns(cat, schema, null, null));
            dump("getSuperTypes", () -> dm.getSuperTypes(cat, schema, null));
            dump("getSuperTables", () -> dm.getSuperTables(cat, schema, null));
            dump("getUDTs", () -> dm.getUDTs(cat, schema, null, null));
            dump("getAttributes", () -> dm.getAttributes(cat, schema, null, null));
            dump("getPrimaryKeys", () -> dm.getPrimaryKeys(cat, schema, table));
            dump("getImportedKeys", () -> dm.getImportedKeys(cat, schema, table));
            dump("getExportedKeys", () -> dm.getExportedKeys(cat, schema, table));
            dump("getIndexInfo", () -> dm.getIndexInfo(cat, schema, table, false, true));
            dump("getTablePrivileges", () -> dm.getTablePrivileges(cat, schema, table));
            dump("getClientInfoProperties", dm::getClientInfoProperties);
        }
        System.out.println("## done");
    }
}
"###;

/// `Meta.java` with the Trino JDBC driver 431 (Maven Central) inside the
/// Trino 431 image, which ships a JDK, against `url`; its sections, each
/// with its lines sorted (the driver orders some listings by a column with
/// ties).
async fn jdbc_listings(tag: &str, url: &str) -> BTreeMap<String, Vec<String>> {
    let script = format!(
        "set -e; cd /tmp; \
         curl -fsSL -o trino-jdbc.jar https://repo1.maven.org/maven2/io/trino/trino-jdbc/431/trino-jdbc-431.jar; \
         cp /w/Meta.java . && javac Meta.java; \
         java -cp trino-jdbc.jar:. Meta '{url}'"
    );
    let container = GenericImage::new("trinodb/trino", "431")
        .with_entrypoint("sh")
        .with_wait_for(WaitFor::exit(ExitWaitStrategy::new()))
        .with_cmd(vec!["-c".to_owned(), script])
        .with_copy_to(
            CopyTargetOptions::new("/w/Meta.java").with_mode(0o644),
            META_PROBE.as_bytes().to_vec(),
        )
        .with_container_name(name(tag))
        .with_startup_timeout(Duration::from_secs(280))
        .start()
        .await
        .unwrap();
    let code = container.exit_code().await.unwrap().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&container.stdout_to_vec().await.unwrap()).into_owned();
    let stderr = String::from_utf8_lossy(&container.stderr_to_vec().await.unwrap()).into_owned();
    assert_eq!(code, 0, "{tag}: {stdout}\n{stderr}");
    assert!(stdout.contains("## done"), "{tag}: {stdout}\n{stderr}");
    let mut sections: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current = String::new();
    for line in stdout.lines() {
        if let Some(header) = line.strip_prefix("## ") {
            header.clone_into(&mut current);
            sections.entry(current.clone()).or_default();
        } else {
            sections.entry(current.clone()).or_default().push(line.to_owned());
        }
    }
    for lines in sections.values_mut() {
        lines.sort();
    }
    sections
}

#[tokio::test]
async fn sql_server_catalog_metadata_matches_trino_431() {
    let (_mssql, mssql_port) = sql_server().await;
    let (trino_container, trino) = trino_431(mssql_port).await;
    let server = Server::start(mssql_port).await;
    let codelake = server.base();
    let warehouse = Some("warehouse");

    let mut statements: Vec<(String, Option<&str>, Option<usize>)> = Vec::new();
    for table in JDBC {
        statements.push((format!("DESCRIBE system.jdbc.{table}"), None, None));
    }
    for table in METADATA {
        statements.push((format!("DESCRIBE system.metadata.{table}"), None, None));
    }
    for table in JDBC {
        let sql = match table {
            // The engines' `system.runtime` differ (ISS-064 item 4): left out here.
            "tables" | "columns" => {
                format!("SELECT * FROM system.jdbc.{table} WHERE table_schem <> 'runtime'")
            }
            _ => format!("SELECT * FROM system.jdbc.{table}"),
        };
        statements.push((sql, None, (table == "columns").then_some(5)));
    }
    for table in METADATA {
        if table == "table_properties" {
            // `data_compression`: a write property of a catalog `CodeLake`
            // does not write (ISS-064 item 3).
            continue;
        }
        let sql = if table == "table_comments" {
            "SELECT * FROM system.metadata.table_comments WHERE schema_name <> 'runtime'".to_owned()
        } else {
            format!("SELECT * FROM system.metadata.{table}")
        };
        statements.push((sql, None, None));
    }
    for table in [
        "schemata",
        "tables",
        "views",
        "table_privileges",
        "roles",
        "enabled_roles",
        "applicable_roles",
    ] {
        statements.push((
            format!("SELECT * FROM warehouse.information_schema.{table}"),
            None,
            None,
        ));
    }
    statements.push((
        "SELECT * FROM warehouse.information_schema.columns".to_owned(),
        None,
        Some(7),
    ));
    for sql in [
        "SHOW CATALOGS",
        "SHOW CATALOGS LIKE 'c%'",
        "SHOW CATALOGS LIKE 'nothing%'",
        "SHOW SCHEMAS FROM warehouse",
        "SHOW SCHEMAS IN warehouse LIKE 'db\\_%' ESCAPE '\\'",
        "SHOW SCHEMAS FROM nope",
        "SHOW SCHEMAS",
        "SHOW TABLES FROM warehouse.dbo",
        "SHOW TABLES FROM warehouse.vendas",
        "DESCRIBE warehouse.dbo.tipos",
        "SELECT name FROM warehouse.sys.schemas",
        "SELECT name, type_desc FROM warehouse.sys.objects WHERE schema_id = 1",
    ] {
        statements.push((sql.to_owned(), None, None));
    }
    // With a session catalog, as DBeaver connects.
    statements.push(("SHOW SCHEMAS".to_owned(), warehouse, None));
    statements.push((
        "SELECT table_schem FROM system.jdbc.schemas WHERE table_catalog = 'warehouse' AND table_schem LIKE 'db%'"
            .to_owned(),
        warehouse,
        None,
    ));

    let mut failures = Vec::new();
    for (sql, catalog, type_column) in &statements {
        if let Some(problem) = compare(&trino, &codelake, sql, *catalog, *type_column).await {
            failures.push(problem);
        }
    }

    // The divergences left out above are exactly the known ones.
    let runtime = "SELECT table_name FROM system.jdbc.tables WHERE table_schem = 'runtime' ORDER BY 1";
    let theirs: Vec<Vec<Value>> = run(&trino, runtime, None).await.rows;
    let ours: Vec<Vec<Value>> = run(&codelake, runtime, None).await.rows;
    let missing: Vec<&Value> = theirs
        .iter()
        .filter(|row| !ours.contains(row))
        .map(|row| &row[0])
        .collect();
    assert_eq!(
        missing,
        ["nodes", "optimizer_rule_stats", "tasks", "transactions"],
        "system.runtime tables only the reference has"
    );
    let divergent = "SELECT column_name, type_name FROM system.jdbc.columns \
                     WHERE table_cat = 'warehouse' AND table_schem = 'dbo' AND table_name = 'divergentes'";
    let mut want = run(&trino, divergent, None).await.rows;
    let mut got = run(&codelake, divergent, None).await.rows;
    want.sort_by_key(|row| row[0].to_string());
    got.sort_by_key(|row| row[0].to_string());
    assert_eq!(
        (want, got),
        (
            vec![
                vec![Value::from("c"), Value::from("char(3)")],
                vec![Value::from("dtm7"), Value::from("timestamp(7)")],
                vec![Value::from("u"), Value::from("char(36)")],
            ],
            vec![
                vec![Value::from("c"), Value::from("varchar")],
                vec![Value::from("dtm7"), Value::from("timestamp(6)")],
                vec![Value::from("u"), Value::from("varchar")],
            ]
        ),
        "the SQL Server types still mapped differently (ISS-064 item 5)"
    );
    let properties = "SELECT * FROM system.metadata.table_properties";
    assert_eq!(run(&trino, properties, None).await.rows.len(), 1);
    assert!(run(&codelake, properties, None).await.rows.is_empty());

    // The schemas that were missing: the database roles', guest and sys,
    // and an empty one.
    let schemas = run(
        &codelake,
        "SELECT table_schem FROM system.jdbc.schemas WHERE table_catalog = 'warehouse' ORDER BY 1",
        None,
    )
    .await;
    let schemas: Vec<&str> = schemas.rows.iter().map(|row| row[0].as_str().unwrap()).collect();
    for schema in [
        "db_owner",
        "db_datareader",
        "guest",
        "sys",
        "vendas",
        "financeiro",
        "dbo",
    ] {
        assert!(schemas.contains(&schema), "{schema}: {schemas:?}");
    }
    let catalogs = run(&codelake, "SELECT table_cat FROM system.jdbc.catalogs", None).await;
    assert_eq!(
        catalogs.rows,
        [vec![Value::from("warehouse")], vec![Value::from("system")]]
    );

    // The JDBC driver's DatabaseMetaData, section by section.
    let trino_ip = trino_container.get_bridge_ip_address().await.unwrap();
    let theirs = jdbc_listings("jdbc-trino", &format!("jdbc:trino://{trino_ip}:8080")).await;
    let ours = jdbc_listings(
        "jdbc-codelake",
        &format!("jdbc:trino://{}:{}", own_ip(), server.port),
    )
    .await;
    assert_eq!(theirs.keys().collect::<Vec<_>>(), ours.keys().collect::<Vec<_>>());
    for (section, lines) in &theirs {
        if lines != &ours[section] {
            failures.push(format!(
                "JDBC {section}\n  trino:    {lines:?}\n  codelake: {:?}",
                ours[section]
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
