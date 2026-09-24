//! Red-team probes of the Trino protocol (front F3). Each `DEFECT-RT<n>` test
//! asserts Trino 431's behavior (observed on a real `trinodb/trino:431`
//! server) and fails against `CodeLake` today; the others are attacks that
//! did not break anything. Run: `cargo nextest run -p codelake-server
//! trino::tests::redteam --run-ignored all`.

use std::time::{Duration, Instant};

use axum::http::{Method, StatusCode};
use serde_json::{Value, json};

use super::{Limits, Outcome, harness, harness_with, limits, path_of};

fn rss_bytes() -> u64 {
    // Linux only (the dev container); resident pages * 4 KiB.
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    statm
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse::<u64>().ok())
        .unwrap_or(0)
        * 4096
}

const MIB: u64 = 1 << 20;

// ---------------------------------------------------------------- session ---

/// Trino 431: `USE memory.nope` → FAILED, `NOT_FOUND` "Schema does not exist";
/// `USE nocat.x` → `NOT_FOUND` "Catalog does not exist". `CodeLake` answers
/// success and sends `X-Trino-Set-Catalog/Schema` for objects that do not
/// exist, so the client's next unqualified query fails far from the cause.
#[tokio::test]
async fn rt1_use_of_missing_schema_fails_like_trino() {
    let h = harness();
    let reply = h.submit("USE nocat.nope", &[]).await;
    let out = Outcome::from(h.follow(reply).await);
    assert_eq!(out.state(), "FAILED", "{}", out.last);
    let reply = h.submit("USE datafusion.nope", &[]).await;
    assert!(
        reply.headers.get("x-trino-set-schema").is_none(),
        "session switched to a missing schema: {:?}",
        reply.headers
    );
}

/// Non-ASCII schema names reach `X-Trino-Set-Schema` (`HeaderValue` accepts
/// obs-text bytes), as Trino 431 sends them.
#[tokio::test]
async fn rt_pass_use_of_non_ascii_schema_sets_the_header() {
    let h = harness();
    let created = h.run("CREATE SCHEMA datafusion.\"ação\"", &[]).await;
    assert_eq!(created.state(), "FINISHED", "{}", created.last);
    let reply = h.submit("USE datafusion.\"ação\"", &[]).await;
    assert_eq!(reply.body["updateType"], "USE");
    assert!(reply.body.get("error").is_none());
    assert!(
        reply.headers.get("x-trino-set-schema").is_some(),
        "USE reported success but no X-Trino-Set-Schema was sent: {:?}",
        reply.headers
    );
}

/// `SET SESSION` values with CR/LF, commas, `=` and non-ASCII travel
/// percent-encoded; nothing reaches the response headers raw.
#[tokio::test]
async fn rt_pass_set_session_values_cannot_inject_headers() {
    let h = harness();
    for value in ["'1h\r\nX-Evil: 1'", "'a,b=c'", "'ação'"] {
        let reply = h
            .submit(&format!("SET SESSION query_max_run_time = {value}"), &[])
            .await;
        // Invalid durations are refused; nothing may leak into headers.
        assert!(reply.headers.get("x-evil").is_none());
        for (name, v) in &reply.headers {
            assert!(!v.as_bytes().contains(&b'\n'), "{name}: {v:?}");
        }
    }
    // Quoted property names with CR/LF are not properties.
    let bad = h.run("SET SESSION \"a\r\nX-Evil: 1\" = '1'", &[]).await;
    assert_eq!(bad.error_name(), Some("INVALID_SESSION_PROPERTY"));
}

// ------------------------------------------------------------ query text ---

/// Trino 431 (`query.max-length` = 1,000,000): 1,000,100 characters and
/// 3,000,000 characters both answer HTTP 200 with a FAILED query,
/// `QUERY_TEXT_TOO_LARGE` (35, `USER_ERROR`). `CodeLake` runs the first and
/// answers the second with a bare `413` from axum's 2 MB body limit.
#[tokio::test]
async fn rt3_query_text_limit_matches_trino() {
    let h = harness();
    let mut wrong = Vec::new();
    for n in [1_000_100_usize, 3_000_000] {
        let sql = format!("SELECT 1 /*{}*/", "x".repeat(n));
        let reply = h.submit(&sql, &[]).await;
        if reply.status != StatusCode::OK {
            let text = String::from_utf8_lossy(&reply.raw).into_owned();
            wrong.push(format!("{n} chars: HTTP {} {text:?}", reply.status));
            continue;
        }
        let out = Outcome::from(h.follow(reply).await);
        if out.error_name() != Some("QUERY_TEXT_TOO_LARGE") {
            wrong.push(format!("{n} chars: {} {:?}", out.state(), out.error_name()));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

// --------------------------------------------------------------- encoding ---

/// Trino 431: `SELECT CAST(0.1 AS REAL), CAST(1.0E0/3 AS REAL)` → `[0.1, 0.33333334]`
/// (Java `Float` in JSON). `CodeLake` widens to f64 first:
/// `0.10000000149011612`, `0.3333333432674408`.
#[tokio::test]
async fn rt4_real_values_encode_like_trino() {
    let h = harness();
    let out = h
        .run("SELECT CAST(0.1 AS REAL) AS a, CAST(1.0 / 3 AS REAL) AS b", &[])
        .await;
    assert_eq!(out.columns[0].1, "real");
    assert_eq!(out.rows[0][0].to_string(), "0.1", "{:?}", out.rows);
    assert_eq!(out.rows[0][1].to_string(), "0.33333334", "{:?}", out.rows);
}

/// Trino 431 answers DDL (`CREATE TABLE` without AS, `DROP TABLE`, `CREATE
/// SCHEMA`, `CREATE/DROP VIEW`) with `updateType` and **no** `columns`/`data`.
/// `CodeLake` adds a `result boolean` column with `[[true]]`, so a DB-API
/// `cursor.fetchall()` after `DROP TABLE` returns `[[True]]` instead of `[]`
/// and `cursor.description` is set.
#[tokio::test]
async fn rt5_ddl_has_no_result_columns() {
    let h = harness();
    let created = h.run("CREATE TABLE datafusion.public.rt5 (a BIGINT)", &[]).await;
    assert_eq!(created.last["updateType"], "CREATE TABLE");
    let dropped = h.run("DROP TABLE datafusion.public.rt5", &[]).await;
    assert_eq!(dropped.last["updateType"], "DROP TABLE");
    assert!(
        created.columns.is_empty() && created.rows.is_empty(),
        "CREATE TABLE: {:?} {:?}",
        created.columns,
        created.rows
    );
    assert!(
        dropped.columns.is_empty() && dropped.rows.is_empty(),
        "DROP TABLE: {:?} {:?}",
        dropped.columns,
        dropped.rows
    );
}

/// Trino 431 error names for common user mistakes (all `USER_ERROR`):
/// overflow → `NUMERIC_VALUE_OUT_OF_RANGE` (19), unknown function →
/// `FUNCTION_NOT_FOUND` (6), unknown schema → `SCHEMA_NOT_FOUND` (45), scalar
/// subquery with 2 rows → `SUBQUERY_MULTIPLE_ROWS` (28). The Python client raises
/// `TrinoUserError` only for `USER_ERROR`; dbt shows `INTERNAL_ERROR` as a
/// server bug.
#[tokio::test]
async fn rt6_user_errors_keep_trino_names_and_type() {
    let h = harness();
    let cases = [
        ("SELECT CAST(300 AS TINYINT)", "NUMERIC_VALUE_OUT_OF_RANGE"),
        ("SELECT nofunc(1)", "FUNCTION_NOT_FOUND"),
        ("SELECT * FROM datafusion.noschema.t", "SCHEMA_NOT_FOUND"),
    ];
    let mut wrong = Vec::new();
    for (sql, expected) in cases {
        let out = h.run(sql, &[]).await;
        let name = out.error_name().unwrap_or("<none>").to_owned();
        let kind = out.last["error"]["errorType"].as_str().unwrap_or("").to_owned();
        if name != expected || kind != "USER_ERROR" {
            wrong.push(format!(
                "{sql}: {name}/{kind} rows={:?} (Trino: {expected}/USER_ERROR)",
                out.rows
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Statements that fail in Trino 431 return a row here: bigint overflow wraps
/// around (Trino: `NUMERIC_VALUE_OUT_OF_RANGE` "bigint addition overflow") and a
/// scalar subquery with two rows yields a value (Trino: `SUBQUERY_MULTIPLE_ROWS`).
/// A dbt model gets a wrong number instead of an error. (Engine semantics,
/// observed through the Trino protocol.)
#[tokio::test]
async fn rt15_statements_trino_rejects_do_not_return_rows() {
    let h = harness();
    let mut wrong = Vec::new();
    for sql in [
        "SELECT 9223372036854775807 + 1",
        "SELECT (SELECT x FROM (VALUES (1), (2)) t(x))",
    ] {
        let out = h.run(sql, &[]).await;
        if out.state() != "FAILED" {
            wrong.push(format!("{sql}: {} rows={:?}", out.state(), out.rows));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Types `types.rs` does not map travel as `varchar` holding Arrow's display
/// text. Trino 431: `ROW(1,'a')` → `row(...)` `[1,"a"]`; `MAP(...)` → `map(...)`
/// `{"k":1}`; `INTERVAL '1' DAY` → `interval day to second` `"1 00:00:00.000"`.
#[tokio::test]
async fn rt7_struct_map_interval_are_typed() {
    let h = harness();
    let out = h
        .run(
            "SELECT named_struct('a', 1, 'b', 'x') AS s, map(['k'], [1]) AS m, INTERVAL '1' DAY AS i",
            &[],
        )
        .await;
    assert_eq!(out.state(), "FINISHED", "{}", out.last);
    let types: Vec<&str> = out.columns.iter().map(|c| c.1.as_str()).collect();
    assert!(types[0].starts_with("row("), "{types:?} {:?}", out.rows);
    assert!(types[1].starts_with("map("), "{types:?} {:?}", out.rows);
    assert!(types[2].starts_with("interval"), "{types:?} {:?}", out.rows);
    assert_eq!(out.rows[0][0], json!([1, "x"]));
    assert_eq!(out.rows[0][1], json!({"k": 1}));
}

/// Values that do not fit the type named in `columns`: a `UInt64` above
/// `i64::MAX` is announced as `bigint` (Java clients overflow), and a decimal
/// with negative scale prints its unscaled value (12300 → "123").
#[tokio::test]
async fn rt8_values_fit_their_announced_type() {
    let h = harness();
    let out = h
        .run(
            "SELECT arrow_cast(18446744073709551615, 'UInt64') AS u, \
             arrow_cast(12300, 'Decimal128(5, -2)') AS d",
            &[],
        )
        .await;
    assert_eq!(out.state(), "FINISHED", "{}", out.last);
    let mut wrong = Vec::new();
    if out.columns[0].1 == "bigint" && out.rows[0][0].as_i64().is_none() {
        wrong.push(format!("bigint column carries {}", out.rows[0][0]));
    }
    let d = out.rows[0][1].as_str().unwrap_or_default().to_owned();
    if d.parse::<f64>().map_or(true, |v| (v - 12300.0).abs() > 0.5) {
        wrong.push(format!("decimal 12300 sent as {d:?} ({})", out.columns[1].1));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Arrow types that `trino_type` names but `value` does not encode fall to
/// Arrow's display text: `Date64` is announced as `date`, `Time32` as `time(p)`.
/// Trino 431 sends `date` as `YYYY-MM-DD` and `time(3)` as `HH:MM:SS.fff`.
#[tokio::test]
#[ignore = "DEFECT-RT8: Date64/Time32 values do not match their announced type"]
async fn rt8_date64_and_time32_match_their_type() {
    let h = harness();
    let out = h
        .run(
            "SELECT arrow_cast(arrow_cast('2026-01-01', 'Date32'), 'Date64') AS d, \
             arrow_cast(arrow_cast('10:00:00.120', 'Time64(Microsecond)'), 'Time32(Millisecond)') AS t",
            &[],
        )
        .await;
    assert_eq!(out.state(), "FINISHED", "{}", out.last);
    assert_eq!(
        (out.columns[0].1.as_str(), out.rows[0][0].clone()),
        ("date", json!("2026-01-01")),
        "{:?}",
        out.rows
    );
    assert_eq!(
        (out.columns[1].1.as_str(), out.rows[0][1].clone()),
        ("time(3)", json!("10:00:00.120")),
        "{:?}",
        out.rows
    );
}

/// Trino 431 writes parametric type names with ", " (`decimal(10, 2)`,
/// `row(integer, varchar(1))`, `map(varchar(1), integer)`) — observed on the
/// real server — and, for a client that does not send
/// `X-Trino-Client-Capabilities: PARAMETRIC_DATETIME`, legacy names without
/// precision (`timestamp`, `time`). `CodeLake` sends `decimal(10,2)` and always
/// `timestamp(3)`. JDBC's `getColumnTypeName` shows the `type` text.
#[tokio::test]
async fn rt14_type_text_matches_trino() {
    let h = harness();
    let out = h
        .run(
            "SELECT CAST(1 AS DECIMAL(10,2)) AS d, CAST('2026-01-01 10:00:00' AS TIMESTAMP) AS t",
            &[],
        )
        .await;
    assert_eq!(out.columns[0].1, "decimal(10, 2)", "{:?}", out.columns);
    // No X-Trino-Client-Capabilities header was sent.
    assert!(!out.columns[1].1.contains('('), "{:?}", out.columns);
}

/// A `timestamp with time zone` in a zone other than UTC is re-labeled: the
/// instant is kept but the zone is always "UTC". Trino 431:
/// `TIMESTAMP '2026-01-01 10:00:00.000 America/Sao_Paulo'` →
/// `"2026-01-01 10:00:00.000 America/Sao_Paulo"`.
#[tokio::test]
async fn rt9_timestamp_keeps_its_zone() {
    let h = harness();
    let out = h
        .run(
            "SELECT arrow_cast(arrow_cast(1767272400000, 'Timestamp(Millisecond, None)'), \
             'Timestamp(Millisecond, Some(\"America/Sao_Paulo\"))') AS t",
            &[],
        )
        .await;
    assert_eq!(out.state(), "FINISHED", "{}", out.last);
    let text = out.rows[0][0].as_str().unwrap_or_default().to_owned();
    assert!(
        text.ends_with("America/Sao_Paulo"),
        "{text:?} ({})",
        out.columns[0].1
    );
}

// ------------------------------------------------------- memory / CPU DoS ---

/// A query canceled (or abandoned) after its first page keeps its buffered
/// record batches (`Exec.events` + `Exec.pending`) for as long as it stays in
/// the history (`history_retention_secs`, default 900 s, up to `history_size`
/// = 100 queries). Those batches are outside `DataFusion`'s memory pool. Trino
/// frees a query's output buffers when it fails.
#[tokio::test]
async fn rt10_canceled_queries_release_their_buffers() {
    let h = harness_with(Limits {
        history_retention: Duration::from_secs(2),
        ..limits()
    });
    let before = rss_bytes();
    // 8192-row batches of 8 KiB strings: 64 MiB per batch.
    let sql = "SELECT repeat('x', 8192) || CAST(value AS VARCHAR) AS s FROM generate_series(1, 1000000)";
    for _ in 0..3 {
        let first = h.submit(sql, &[]).await;
        let one = path_of(first.body["nextUri"].as_str().unwrap());
        let page = h.call(Method::GET, &one, &[], "").await;
        assert_eq!(page.status, StatusCode::OK);
        // Let execution fill the channel, then cancel.
        let mut next = page.body["nextUri"].as_str().map(path_of);
        if next.as_deref().is_some_and(|n| !n.is_empty()) {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let _ = h.call(Method::DELETE, &one, &[], "").await;
        if let Some(n) = next.take() {
            let after = h.call(Method::GET, &n, &[], "").await;
            assert_eq!(after.body["stats"]["state"], "FAILED");
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let while_in_history = rss_bytes();
    tokio::time::sleep(Duration::from_millis(2200)).await;
    h.state.sweep();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_sweep = rss_bytes();
    let held = while_in_history.saturating_sub(after_sweep);
    eprintln!(
        "RSS before {} MiB, canceled-in-history {} MiB, after sweep {} MiB, held by 3 canceled queries {} MiB",
        before / MIB,
        while_in_history / MIB,
        after_sweep / MIB,
        held / MIB
    );
    assert!(
        held < 32 * MIB,
        "3 FAILED queries still held {} MiB until they left the history",
        held / MIB
    );
}

async fn median_submit(h: &super::Harness) -> Duration {
    let mut times = Vec::new();
    for _ in 0..15 {
        let start = Instant::now();
        let reply = h.submit("SELECT 1", &[]).await;
        times.push(start.elapsed());
        let _ = h.follow(reply).await;
    }
    times.sort();
    times[times.len() / 2]
}

/// Every `POST` clones every registered query's `Info` — including its full
/// SQL text — under the registry's `std::sync::Mutex` to count the active
/// ones (`Registry::insert`), and every page does it again for its own query
/// (`TrinoState::body` → `snapshot`). With the history full of large
/// statements (100 × 1.9 MB, which Trino would not even accept), a trivial
/// `SELECT 1` pays ~190 MB of copying.
#[tokio::test]
#[ignore = "DEFECT-RT11: submit cost grows with the SQL text held in the history"]
async fn rt11_submit_cost_does_not_grow_with_history_text() {
    let h = harness_with(Limits {
        max_active: 200,
        ..limits()
    });
    let baseline = median_submit(&h).await;
    let big = format!("SELECT 1 /*{}*/", "x".repeat(1_900_000));
    for _ in 0..100 {
        let out = h.run(&big, &[]).await;
        assert_eq!(out.state(), "FINISHED");
    }
    let loaded = median_submit(&h).await;
    eprintln!("median POST: empty history {baseline:?}, history of 100 x 1.9 MB SQL {loaded:?}");
    assert!(
        loaded < baseline * 4 + Duration::from_millis(2),
        "POST SELECT 1 went from {baseline:?} to {loaded:?}"
    );
}

/// Between two sweeps (5 s) nothing bounds the registry: finished and failed
/// queries do not count toward `max_active_queries`, and `history_size` is only
/// applied by the sweeper. A burst of failing statements accumulates without
/// limit — each keeping its SQL text.
#[tokio::test]
async fn rt12_registry_is_bounded_between_sweeps() {
    let h = harness_with(Limits {
        max_active: 5,
        history_size: 10,
        ..limits()
    });
    for _ in 0..200 {
        let out = h.run("SELECT 1 +", &[]).await;
        assert_eq!(out.error_name(), Some("SYNTAX_ERROR"));
    }
    let listed = h.run("SELECT count(*) FROM system.runtime.queries", &[]).await;
    let n = listed.rows[0][0].as_i64().unwrap();
    assert!(
        n <= 5 + 10 + 1,
        "{n} queries registered with max_active = 5 and history_size = 10"
    );
}

// ------------------------------------------------------------- security ---

/// `DELETE /v1/query/{id}` "only the owner": the owner is whatever
/// `X-Trino-User` says, and `system.runtime.queries` lists every query's id and
/// user to anyone. Any client can cancel anyone's query in two requests — the
/// same as Trino's default allow-all access control, so the ADR's owner check
/// protects nothing until authentication exists. (Trino 431 answers 204 to any
/// user; this passes and documents the parity.)
#[tokio::test]
async fn rt_pass_owner_check_is_spoofable_like_trino_allow_all() {
    let h = harness();
    let victim = h
        .call(
            Method::POST,
            "/v1/statement",
            &[("x-trino-user", "victim")],
            "SELECT count(*) FROM generate_series(1, 5000000000)",
        )
        .await;
    let id = victim.body["id"].as_str().unwrap().to_owned();
    let seen = h
        .run(
            &format!("SELECT \"user\", query FROM system.runtime.queries WHERE query_id = '{id}'"),
            &[],
        )
        .await;
    let owner = seen.rows[0][0].as_str().unwrap().to_owned();
    assert!(seen.rows[0][1].as_str().unwrap().contains("generate_series"));
    let killed = h
        .call(
            Method::DELETE,
            &format!("/v1/query/{id}"),
            &[("x-trino-user", owner.as_str())],
            "",
        )
        .await;
    assert_eq!(killed.status, StatusCode::NO_CONTENT);
}

/// Slugs are random `UUIDv4` (122 bits): a known id with any other slug is 404,
/// and ids are only readable through `system.runtime.queries`.
#[tokio::test]
async fn rt_pass_slug_is_required_for_statement_uris() {
    let h = harness();
    let first = h
        .submit("SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    let next = path_of(first.body["nextUri"].as_str().unwrap());
    let parts: Vec<&str> = next.split('/').collect();
    let slug = parts[5];
    assert_eq!(slug.len(), 32);
    let forged = format!("/v1/statement/executing/{}/{}/1", parts[4], "f".repeat(32));
    assert_eq!(
        h.call(Method::DELETE, &forged, &[], "").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        h.call(Method::GET, &forged, &[], "").await.status,
        StatusCode::NOT_FOUND
    );
    let _ = h.call(Method::DELETE, &next, &[], "").await;
}

/// The `Host` header is reflected into `nextUri`/`infoUri` only when it is a
/// plain host[:port]; anything with `/`, `@`, spaces or CR/LF falls back to
/// the listen address. (Trino reflects `Host` too; a client only sees URIs
/// built from its own request.)
#[tokio::test]
async fn rt_pass_host_header_cannot_inject_paths() {
    let h = harness();
    for host in ["evil.example/x", "a@evil.example", "evil example"] {
        let reply = post_with_host(&h.router, host).await;
        let next = reply["nextUri"].as_str().unwrap_or_default().to_owned();
        assert!(next.starts_with("http://127.0.0.1:8081/"), "{host}: {next}");
    }
}

// ------------------------------------------------------------- races ---

/// The same token fetched in parallel returns identical bytes; DELETE racing
/// GETs always ends `FAILED`/`USER_CANCELED`; never a panic or a 5xx.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rt_pass_parallel_token_reuse_and_cancel_race() {
    let h = std::sync::Arc::new(harness());
    for _ in 0..20 {
        let first = h
            .submit("SELECT value FROM generate_series(1, 3000000)", &[])
            .await;
        let one = path_of(first.body["nextUri"].as_str().unwrap());
        let mut tasks = Vec::new();
        for i in 0..6 {
            let h = std::sync::Arc::clone(&h);
            let one = one.clone();
            tasks.push(tokio::spawn(async move {
                if i == 5 {
                    let r = h.call(Method::DELETE, &one, &[], "").await;
                    (r.status, Vec::new())
                } else {
                    let r = h.call(Method::GET, &one, &[], "").await;
                    (r.status, r.raw)
                }
            }));
        }
        let mut bodies = Vec::new();
        for t in tasks {
            let (status, raw) = t.await.unwrap();
            assert!(status.is_success(), "{status}");
            if !raw.is_empty() {
                bodies.push(raw);
            }
        }
        assert!(
            bodies.windows(2).all(|w| w[0] == w[1]),
            "same token, different pages"
        );
        let _ = h.call(Method::DELETE, &one, &[], "").await;
    }
}

async fn post_with_host(router: &axum::Router, host: &str) -> Value {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    let request = Request::builder()
        .method(Method::POST)
        .uri("/v1/statement")
        .header("host", host)
        .header("x-trino-user", "tester")
        .body(Body::from("SELECT 1"))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let raw = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&raw).unwrap_or(Value::Null)
}
