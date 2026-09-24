//! Protocol tests against a real engine through the real router.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::{Limits, TrinoState, router};

struct Harness {
    state: Arc<TrinoState>,
    engine: Arc<Engine>,
    router: Router,
    _dir: tempfile::TempDir,
}

fn limits() -> Limits {
    Limits {
        max_active: 50,
        client_timeout: Duration::from_mins(5),
        history_retention: Duration::from_mins(15),
        history_size: 100,
        max_query_length: 1_000_000,
    }
}

fn harness_with(limits: Limits) -> Harness {
    harness_config(limits, ByteSize::mib(256), &ExecutionConfig::default())
}

/// A harness with its own memory limit and execution settings.
fn harness_config(limits: Limits, memory_limit: ByteSize, execution: &ExecutionConfig) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let memory = MemoryConfig {
        limit: memory_limit,
        spill_dir: dir.path().to_owned(),
        spill_limit: ByteSize::mib(256),
        metadata_cache_limit: ByteSize::mib(1),
    };
    let engine = Arc::new(Engine::new(&memory, execution).unwrap());
    let state = TrinoState::new(Arc::clone(&engine), limits, "127.0.0.1:8081".parse().unwrap());
    engine
        .context()
        .register_catalog("system", state.system_catalog().unwrap());
    Harness {
        router: router(Arc::clone(&state)),
        state,
        engine,
        _dir: dir,
    }
}

fn harness() -> Harness {
    harness_with(limits())
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
    raw: Vec<u8>,
}

impl Harness {
    async fn call(&self, method: Method, uri: &str, headers: &[(&str, &str)], body: &str) -> Reply {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("host", "codelake.test:8081");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = self
            .router
            .clone()
            .oneshot(request.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let raw = response.into_body().collect().await.unwrap().to_bytes().to_vec();
        let body = serde_json::from_slice(&raw).unwrap_or(Value::Null);
        Reply {
            status,
            headers,
            body,
            raw,
        }
    }

    async fn submit(&self, sql: &str, headers: &[(&str, &str)]) -> Reply {
        let mut all = vec![("x-trino-user", "tester")];
        all.extend_from_slice(headers);
        self.call(Method::POST, "/v1/statement", &all, sql).await
    }

    /// Follows `nextUri` to the end; returns every response body.
    async fn follow(&self, first: Reply) -> Vec<Value> {
        let mut pages = vec![first.body];
        while let Some(next) = pages.last().unwrap()["nextUri"].as_str().map(path_of) {
            let reply = self.call(Method::GET, &next, &[], "").await;
            assert_eq!(reply.status, StatusCode::OK, "{next}");
            pages.push(reply.body);
            assert!(pages.len() < 10_000, "no end");
        }
        pages
    }

    async fn run(&self, sql: &str, headers: &[(&str, &str)]) -> Outcome {
        let first = self.submit(sql, headers).await;
        assert_eq!(first.status, StatusCode::OK, "{sql}");
        Outcome::from(self.follow(first).await)
    }
}

fn path_of(uri: &str) -> String {
    let rest = uri
        .strip_prefix("http://codelake.test:8081")
        .expect("absolute URI on the request host");
    rest.to_owned()
}

#[derive(Debug)]
struct Outcome {
    columns: Vec<(String, String)>,
    rows: Vec<Vec<Value>>,
    last: Value,
    pages_with_data: usize,
}

impl From<Vec<Value>> for Outcome {
    fn from(pages: Vec<Value>) -> Self {
        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut pages_with_data = 0;
        for page in &pages {
            if let Some(cols) = page["columns"].as_array()
                && columns.is_empty()
            {
                columns = cols
                    .iter()
                    .map(|c| {
                        (
                            c["name"].as_str().unwrap().to_owned(),
                            c["type"].as_str().unwrap().to_owned(),
                        )
                    })
                    .collect();
            }
            if let Some(data) = page["data"].as_array() {
                assert!(!page["columns"].is_null(), "data without columns: {page}");
                pages_with_data += 1;
                rows.extend(data.iter().map(|r| r.as_array().unwrap().clone()));
            }
        }
        Self {
            columns,
            rows,
            last: pages.last().unwrap().clone(),
            pages_with_data,
        }
    }
}

impl Outcome {
    fn state(&self) -> &str {
        self.last["stats"]["state"].as_str().unwrap()
    }

    fn error_name(&self) -> Option<&str> {
        self.last["error"]["errorName"].as_str()
    }
}

#[tokio::test]
async fn select_returns_typed_rows() {
    let h = harness();
    let out = h
        .run(
            "SELECT 1 AS a, 'ação' AS b, CAST(NULL AS BIGINT) AS c, CAST(12.50 AS DECIMAL(10,2)) AS d, true AS e",
            &[],
        )
        .await;
    assert_eq!(out.state(), "FINISHED");
    assert_eq!(
        out.columns,
        [
            // Trino 431: an integer literal that fits 32 bits is `integer`, a
            // text literal `varchar(<its length>)`.
            ("a", "integer"),
            ("b", "varchar(4)"),
            ("c", "bigint"),
            ("d", "decimal(10, 2)"),
            ("e", "boolean")
        ]
        .map(|(n, t)| (n.to_owned(), t.to_owned()))
    );
    assert_eq!(
        out.rows,
        vec![
            serde_json::json!([1, "ação", null, "12.50", true])
                .as_array()
                .unwrap()
                .clone()
        ]
    );
    assert!(out.last.get("error").is_none());
    assert!(out.last["id"].as_str().unwrap().contains('_'));
}

#[tokio::test]
async fn large_results_are_paged_and_complete() {
    let h = harness();
    let out = h
        .run(
            "SELECT value, lpad('', 40, 'x') AS pad FROM generate_series(1, 200000) ORDER BY value",
            &[],
        )
        .await;
    assert_eq!(out.state(), "FINISHED");
    assert_eq!(out.rows.len(), 200_000);
    assert!(out.pages_with_data > 3, "pages: {}", out.pages_with_data);
    // Every value exactly once, in order.
    for (index, row) in out.rows.iter().enumerate() {
        assert_eq!(row[0].as_i64().unwrap(), i64::try_from(index).unwrap() + 1);
    }
    assert_eq!(out.last["stats"]["processedRows"], 200_000);
}

#[tokio::test]
async fn repeating_a_token_returns_the_same_page_and_old_tokens_are_gone() {
    let h = harness();
    let first = h.submit("SELECT value FROM generate_series(1, 50000)", &[]).await;
    let one = path_of(first.body["nextUri"].as_str().unwrap());
    let a = h.call(Method::GET, &one, &[], "").await;
    let b = h.call(Method::GET, &one, &[], "").await;
    assert_eq!(a.raw, b.raw, "retry must be idempotent");
    let Some(two) = a.body["nextUri"].as_str().map(path_of) else {
        return;
    };
    let _ = h.call(Method::GET, &two, &[], "").await;
    let stale = h.call(Method::GET, &one, &[], "").await;
    assert_eq!(stale.status, StatusCode::GONE);
    // A known id with a wrong slug is not found.
    let parts: Vec<&str> = one.split('/').collect();
    let forged = format!("/v1/statement/executing/{}/{}/1", parts[4], "0".repeat(32));
    let bad = h.call(Method::GET, &forged, &[], "").await;
    assert_eq!(bad.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn errors_follow_trino_codes() {
    let h = harness();
    let syntax = h.run("SELECT 1 +", &[]).await;
    assert_eq!(syntax.state(), "FAILED");
    assert_eq!(syntax.error_name(), Some("SYNTAX_ERROR"));
    assert_eq!(syntax.last["error"]["errorType"], "USER_ERROR");
    assert!(syntax.last.get("nextUri").is_none());

    let missing = h.run("SELECT * FROM nowhere.nothing.none", &[]).await;
    assert_eq!(missing.state(), "FAILED");
    assert!(missing.error_name().is_some(), "{}", missing.last);

    let zero = h.run("SELECT 1 / 0", &[]).await;
    assert_eq!(zero.error_name(), Some("DIVISION_BY_ZERO"), "{}", zero.last);
}

#[tokio::test]
async fn missing_user_and_empty_statement_are_rejected() {
    let h = harness();
    let no_user = h.call(Method::POST, "/v1/statement", &[], "SELECT 1").await;
    assert_eq!(no_user.status, StatusCode::UNAUTHORIZED);
    let empty = h.submit("   ", &[]).await;
    assert_eq!(empty.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cancel_stops_the_query_and_is_recorded() {
    let h = harness();
    let first = h
        .submit("SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    let id = first.body["id"].as_str().unwrap().to_owned();
    let next = path_of(first.body["nextUri"].as_str().unwrap());
    let canceled = h.call(Method::DELETE, &next, &[], "").await;
    assert_eq!(canceled.status, StatusCode::NO_CONTENT);
    let after = h.call(Method::GET, &next, &[], "").await;
    assert_eq!(after.body["error"]["errorName"], "USER_CANCELED");
    assert_eq!(after.body["stats"]["state"], "FAILED");

    let listed = h
        .run(
            &format!(
                "SELECT state, error_code, \"user\" FROM system.runtime.queries WHERE query_id = '{id}'"
            ),
            &[],
        )
        .await;
    assert_eq!(
        listed.rows,
        vec![
            serde_json::json!(["FAILED", "USER_CANCELED", "tester"])
                .as_array()
                .unwrap()
                .clone()
        ]
    );
}

#[tokio::test]
async fn kill_by_id_requires_the_owner() {
    let h = harness();
    let first = h
        .submit("SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    let id = first.body["id"].as_str().unwrap().to_owned();
    let other = h
        .call(
            Method::DELETE,
            &format!("/v1/query/{id}"),
            &[("x-trino-user", "intruder")],
            "",
        )
        .await;
    assert_eq!(other.status, StatusCode::FORBIDDEN);
    let owner = h
        .call(
            Method::DELETE,
            &format!("/v1/query/{id}"),
            &[("x-trino-user", "tester")],
            "",
        )
        .await;
    assert_eq!(owner.status, StatusCode::NO_CONTENT);
    let unknown = h
        .call(
            Method::DELETE,
            "/v1/query/nope",
            &[("x-trino-user", "tester")],
            "",
        )
        .await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    let out = Outcome::from(h.follow(first).await);
    assert_eq!(out.error_name(), Some("USER_CANCELED"));
}

#[tokio::test]
async fn session_statements_answer_with_headers() {
    let h = harness();
    let use_ = h.submit("USE datafusion.public", &[]).await;
    assert_eq!(use_.body["updateType"], "USE");
    assert_eq!(use_.headers["x-trino-set-catalog"], "datafusion");
    assert_eq!(use_.headers["x-trino-set-schema"], "public");
    assert!(use_.body.get("nextUri").is_none());

    let set = h.submit("SET SESSION query_max_run_time = '2h'", &[]).await;
    assert_eq!(set.body["updateType"], "SET SESSION");
    assert_eq!(set.headers["x-trino-set-session"], "query_max_run_time=2h");

    let reset = h.submit("RESET SESSION query_max_run_time", &[]).await;
    assert_eq!(reset.headers["x-trino-clear-session"], "query_max_run_time");

    let bad = h.run("SET SESSION no_such_thing = 1", &[]).await;
    assert_eq!(bad.error_name(), Some("INVALID_SESSION_PROPERTY"));

    let bad_header = h.run("SELECT 1", &[("x-trino-session", "no_such_thing=1")]).await;
    assert_eq!(bad_header.error_name(), Some("INVALID_SESSION_PROPERTY"));
}

#[tokio::test]
async fn show_session_reflects_client_values_and_warns_on_ignored() {
    let h = harness();
    let out = h
        .run(
            "SHOW SESSION",
            &[("x-trino-session", "query_max_run_time=1h,scale_writers=false")],
        )
        .await;
    assert_eq!(out.columns[0].0, "Name");
    let row = out
        .rows
        .iter()
        .find(|r| r[0] == "query_max_run_time")
        .expect("listed");
    assert_eq!(row[1], "1h");
    let scale = out.rows.iter().find(|r| r[0] == "scale_writers").unwrap();
    assert_eq!(scale[1], "false");
    let warnings = out.last["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w["message"].as_str().unwrap().contains("scale_writers")),
        "{warnings:?}"
    );
}

#[tokio::test]
async fn catalog_and_schema_headers_resolve_unqualified_names() {
    let h = harness();
    let created = h
        .run("CREATE TABLE datafusion.public.t AS SELECT 1 AS a", &[])
        .await;
    assert_eq!(created.state(), "FINISHED");
    assert_eq!(created.last["updateType"], "CREATE TABLE");
    let out = h
        .run(
            "SELECT a FROM t",
            &[("x-trino-catalog", "datafusion"), ("x-trino-schema", "public")],
        )
        .await;
    assert_eq!(out.rows, vec![vec![serde_json::json!(1)]]);
}

#[tokio::test]
async fn dml_reports_update_count() {
    let h = harness();
    let _ = h
        .run("CREATE TABLE datafusion.public.items (id BIGINT)", &[])
        .await;
    let insert = h
        .run("INSERT INTO datafusion.public.items VALUES (1), (2), (3)", &[])
        .await;
    assert_eq!(insert.state(), "FINISHED", "{}", insert.last);
    assert_eq!(insert.last["updateType"], "INSERT");
    assert_eq!(insert.last["updateCount"], 3);
    assert_eq!(insert.columns, vec![("rows".to_owned(), "bigint".to_owned())]);
    assert_eq!(insert.rows, vec![vec![serde_json::json!(3)]]);
    let count = h.run("SELECT count(*) FROM datafusion.public.items", &[]).await;
    assert_eq!(count.rows, vec![vec![serde_json::json!(3)]]);

    let drop = h.run("DROP TABLE datafusion.public.items", &[]).await;
    assert_eq!(drop.last["updateType"], "DROP TABLE");
    // Trino answers DDL with the update type only.
    assert!(drop.columns.is_empty() && drop.rows.is_empty(), "{drop:?}");
}

#[tokio::test]
async fn query_max_run_time_is_enforced() {
    let h = harness();
    let out = h
        .run(
            "SELECT count(*) FROM generate_series(1, 5000000000)",
            &[("x-trino-session", "query_max_run_time=200ms")],
        )
        .await;
    assert_eq!(out.error_name(), Some("EXCEEDED_TIME_LIMIT"), "{}", out.last);
    assert!(
        out.last["error"]["message"]
            .as_str()
            .unwrap()
            .contains("200.00ms")
    );
}

#[tokio::test]
async fn abandoned_queries_are_canceled() {
    let h = harness_with(Limits {
        client_timeout: Duration::from_millis(100),
        ..limits()
    });
    let first = h
        .submit("SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    h.state.sweep();
    let out = Outcome::from(h.follow(first).await);
    assert_eq!(out.error_name(), Some("ABANDONED_QUERY"));
}

#[tokio::test]
async fn active_query_limit_refuses_with_queue_full() {
    let h = harness_with(Limits {
        max_active: 1,
        ..limits()
    });
    let first = h
        .submit("SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    let second = h.submit("SELECT 1", &[]).await;
    assert_eq!(second.body["error"]["errorName"], "QUERY_QUEUE_FULL");
    let next = path_of(first.body["nextUri"].as_str().unwrap());
    let _ = h.call(Method::DELETE, &next, &[], "").await;
}

#[tokio::test]
async fn draining_refuses_new_statements() {
    let h = harness();
    h.state.start_draining();
    let reply = h.submit("SELECT 1", &[]).await;
    assert_eq!(reply.status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn info_reports_protocol_version() {
    let h = harness();
    let reply = h.call(Method::GET, "/v1/info", &[], "").await;
    assert_eq!(reply.body["nodeVersion"]["version"], "431");
    assert_eq!(reply.body["coordinator"], true);
}

#[tokio::test]
async fn tokens_ahead_are_unknown_and_malformed_sessions_are_bad_requests() {
    let h = harness();
    let first = h.submit("SELECT value FROM generate_series(1, 50000)", &[]).await;
    let one = path_of(first.body["nextUri"].as_str().unwrap());
    let ahead = format!("{}/5", one.rsplit_once('/').unwrap().0);
    assert_eq!(
        h.call(Method::GET, &ahead, &[], "").await.status,
        StatusCode::NOT_FOUND
    );
    let _ = h.call(Method::DELETE, &one, &[], "").await;
    let schema_only = h.submit("SELECT 1", &[("x-trino-schema", "public")]).await;
    assert_eq!(schema_only.status, StatusCode::BAD_REQUEST);
    let bad_session = h
        .submit("SELECT 1", &[("x-trino-session", "no_equals_sign")])
        .await;
    assert_eq!(bad_session.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn statements_report_like_trino() {
    let h = harness();
    let schema = h.run("CREATE SCHEMA datafusion.st", &[]).await;
    assert_eq!(schema.last["updateType"], "CREATE SCHEMA");
    assert_eq!(schema.last["columns"], serde_json::json!([]), "{}", schema.last);
    assert!(schema.last.get("data").is_none(), "{}", schema.last);
    let ctas = h
        .run(
            "CREATE TABLE datafusion.st.t AS SELECT * FROM (VALUES (1, 'x'), (2, 'y'), (3, 'z')) v(a, b)",
            &[],
        )
        .await;
    assert_eq!(ctas.last["updateType"], "CREATE TABLE");
    assert_eq!(ctas.last["updateCount"], 3);
    assert_eq!(ctas.rows, vec![vec![serde_json::json!(3)]]);
    let exists = h.run("CREATE TABLE datafusion.st.t (a BIGINT)", &[]).await;
    assert_eq!(
        exists.error_name(),
        Some("TABLE_ALREADY_EXISTS"),
        "{}",
        exists.last
    );
    let schema_exists = h.run("CREATE SCHEMA datafusion.st", &[]).await;
    assert_eq!(schema_exists.error_name(), Some("SCHEMA_ALREADY_EXISTS"));
    let missing = h.run("DROP TABLE datafusion.st.nope", &[]).await;
    assert_eq!(missing.error_name(), Some("TABLE_NOT_FOUND"));
    let unnamed = h
        .run("SELECT count(*), max(a) AS m FROM datafusion.st.t", &[])
        .await;
    let names: Vec<&str> = unnamed.columns.iter().map(|c| c.0.as_str()).collect();
    assert_eq!(names, ["_col0", "m"]);
    let located = h.run("SELECT 1\n  FROM\n  datafusion.st.missing", &[]).await;
    assert_eq!(located.error_name(), Some("TABLE_NOT_FOUND"));
    assert_eq!(
        located.last["error"]["errorLocation"],
        serde_json::json!({"lineNumber": 3, "columnNumber": 3})
    );
    let described = h.run("DESCRIBE datafusion.st.t", &[]).await;
    let names: Vec<&str> = described.columns.iter().map(|c| c.0.as_str()).collect();
    assert_eq!(names, ["Column", "Type", "Extra", "Comment"]);
    assert_eq!(described.rows[0][0], "a");
    // Trino 431: `VALUES (1, …)` is `integer`; `Extra` and `Comment` empty.
    assert_eq!(described.rows[0][1], "integer");
    assert_eq!(described.rows[0][2], "");
    let shown = h.run("SHOW COLUMNS FROM datafusion.st.t LIKE 'b'", &[]).await;
    let names: Vec<&str> = shown.columns.iter().map(|c| c.0.as_str()).collect();
    assert_eq!(names, ["Column", "Type", "Extra", "Comment"]);
    assert_eq!(shown.rows.len(), 1);
    assert_eq!(shown.rows[0][0], "b");
    assert_eq!(shown.rows[0][1], "varchar(1)");
}

#[tokio::test]
async fn show_session_filters_and_formats_defaults() {
    let h = harness();
    let out = h.run("SHOW SESSION LIKE 'query_max_run%'", &[]).await;
    assert_eq!(out.rows.len(), 1, "{:?}", out.rows);
    assert_eq!(out.rows[0][0], "query_max_run_time");
    assert_eq!(out.rows[0][1], "100.00d");
    assert_eq!(out.rows[0][2], "100.00d");
}

#[tokio::test]
async fn prepared_statements_travel_in_headers() {
    let h = harness();
    let prepare = h.submit("PREPARE st1 FROM SELECT ? + 1 AS y, ? AS s", &[]).await;
    assert_eq!(prepare.body["updateType"], "PREPARE");
    let added = prepare.headers["x-trino-added-prepare"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(added.starts_with("st1="), "{added}");
    let executed = h
        .run(
            "EXECUTE st1 USING 41, 'it''s'",
            &[("x-trino-prepared-statement", added.as_str())],
        )
        .await;
    assert_eq!(executed.state(), "FINISHED", "{}", executed.last);
    assert_eq!(
        executed.rows,
        vec![vec![serde_json::json!(42), serde_json::json!("it's")]]
    );
    let unknown = h.run("EXECUTE nope USING 1", &[]).await;
    assert_eq!(unknown.error_name(), Some("NOT_FOUND"));
    let wrong_count = h
        .run(
            "EXECUTE st1 USING 1",
            &[("x-trino-prepared-statement", added.as_str())],
        )
        .await;
    assert_eq!(wrong_count.error_name(), Some("INVALID_PARAMETER_USAGE"));
    let deallocated = h
        .submit(
            "DEALLOCATE PREPARE st1",
            &[("x-trino-prepared-statement", added.as_str())],
        )
        .await;
    assert_eq!(deallocated.body["updateType"], "DEALLOCATE");
    assert_eq!(deallocated.headers["x-trino-deallocated-prepare"], "st1");
    let immediate = h
        .run(
            "EXECUTE IMMEDIATE 'SELECT ?, ? AS d' USING 'a''b OR 1=1', DATE '1969-07-20'",
            &[("x-trino-client-capabilities", "PARAMETRIC_DATETIME")],
        )
        .await;
    assert_eq!(immediate.state(), "FINISHED", "{}", immediate.last);
    assert_eq!(immediate.columns[0].0, "_col0");
    assert_eq!(
        immediate.rows,
        vec![vec![
            serde_json::json!("a'b OR 1=1"),
            serde_json::json!("1969-07-20")
        ]]
    );
}

#[tokio::test]
async fn statements_over_the_length_limit_fail_like_trino() {
    let h = harness_with(Limits {
        max_query_length: 100,
        ..limits()
    });
    let out = h.run(&format!("SELECT 1 /*{}*/", "x".repeat(200)), &[]).await;
    assert_eq!(out.error_name(), Some("QUERY_TEXT_TOO_LARGE"));
    assert_eq!(
        out.last["error"]["message"],
        "Query text length (213) exceeds the maximum length (100)"
    );
    let ok = h.run("SELECT 1 AS x", &[]).await;
    assert_eq!(ok.state(), "FINISHED");
}

async fn median_submit_time(h: &Harness) -> Duration {
    let mut times = Vec::new();
    for _ in 0..15 {
        let start = std::time::Instant::now();
        let reply = h.submit("SELECT 1", &[]).await;
        times.push(start.elapsed());
        let _ = h.follow(reply).await;
    }
    times.sort();
    times[times.len() / 2]
}

/// Submitting stays cheap however much SQL the history holds (the history
/// shares the text instead of copying it on every request).
#[tokio::test]
async fn submit_cost_does_not_grow_with_history_text() {
    let h = harness_with(Limits {
        max_active: 200,
        ..limits()
    });
    let baseline = median_submit_time(&h).await;
    let big = format!("SELECT 1 /*{}*/", "x".repeat(990_000));
    for _ in 0..100 {
        assert_eq!(h.run(&big, &[]).await.state(), "FINISHED");
    }
    let loaded = median_submit_time(&h).await;
    assert!(
        loaded < baseline * 4 + Duration::from_millis(2),
        "POST SELECT 1 went from {baseline:?} to {loaded:?}"
    );
}

mod ceiling;
mod redteam;
