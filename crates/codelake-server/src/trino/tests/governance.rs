//! Query governance through the HTTP statement protocol: time limits that
//! stop the work and give the memory back, the admission queue, per-user and
//! resource-group limits. Memory is read in the engine's pool (what operators
//! reserved), never as process RSS.

use std::time::{Duration, Instant};

use axum::http::Method;
use codelake_core::config::{GovernanceConfig, ResourceGroupConfig};
use serde_json::{Value, json};

use super::{Harness, Limits, Outcome, harness_governed, limits, path_of};

/// Rows enough that the producer blocks on the client after two batches; it
/// keeps its running slot while the client does not page only with
/// suspension off ([`held_slots`]).
const HOLDS_A_SLOT: &str = "SELECT value FROM generate_series(1, 100000000)";

fn governed(config: &GovernanceConfig) -> Harness {
    harness_governed(limits(), config)
}

/// Suspension off: a query whose client does not page keeps its running slot,
/// as the tests of the queue need a holder (suspension has its own tests,
/// at the end).
fn held_slots() -> GovernanceConfig {
    GovernanceConfig {
        max_suspended_queries: 0,
        ..GovernanceConfig::default()
    }
}

impl Harness {
    async fn submit_as(&self, user: &str, sql: &str, headers: &[(&str, &str)]) -> Value {
        let mut all = vec![("x-trino-user", user)];
        all.extend_from_slice(headers);
        self.call(Method::POST, "/v1/statement", &all, sql).await.body
    }

    /// The first page of a statement: where a refusal at submission is.
    async fn first_page_as(&self, user: &str, sql: &str) -> Value {
        let submitted = self.submit_as(user, sql, &[]).await;
        assert_eq!(submitted["stats"]["state"], "QUEUED", "{submitted}");
        let next = path_of(submitted["nextUri"].as_str().unwrap());
        self.call(Method::GET, &next, &[], "").await.body
    }

    async fn cancel(&self, submitted: &Value) {
        let next = path_of(submitted["nextUri"].as_str().unwrap());
        let _ = self.call(Method::DELETE, &next, &[], "").await;
    }

    async fn follow_body(&self, first: Value) -> Outcome {
        let mut pages = vec![first];
        while let Some(next) = pages.last().unwrap()["nextUri"].as_str().map(path_of) {
            pages.push(self.call(Method::GET, &next, &[], "").await.body);
            assert!(pages.len() < 10_000, "no end");
        }
        Outcome::from(pages)
    }

    /// Waits (at most 10 s) until admission shows `(running, queued)`.
    async fn load_becomes(&self, expected: (usize, usize)) {
        let admission = self.engine.governance().admission();
        let deadline = Instant::now() + Duration::from_secs(10);
        while admission.load() != expected {
            assert!(
                Instant::now() < deadline,
                "admission load {:?}, expected {expected:?}",
                admission.load()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn reserved(&self) -> u64 {
        self.engine.memory().reserved_bytes
    }

    /// Waits (at most 10 s) for the pool to hold nothing; returns what it holds.
    async fn pool_drains(&self) -> u64 {
        for _ in 0..200 {
            if self.reserved() == 0 {
                return 0;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.reserved()
    }
}

/// Definition of done: a query past `query_max_run_time` is stopped for real — its operators
/// drop their reservations — and the pool returns to zero. The join's build
/// side (2 M rows) is reserved while the probe side (3 G rows) streams, so
/// the query holds memory when the limit fires.
#[tokio::test]
async fn timeout_stops_the_query_and_the_pool_returns_to_zero() {
    let h = governed(&GovernanceConfig::default());
    assert_eq!(h.reserved(), 0);
    h.engine.reset_memory_peak();
    let started = Instant::now();
    let first = h
        .submit_as(
            "ana",
            "SELECT count(*) FROM generate_series(1, 3000000000) b \
             JOIN generate_series(1, 2000000) a ON a.value = b.value",
            &[("x-trino-session", "query_max_run_time=2s")],
        )
        .await;
    let out = h.follow_body(first).await;
    let elapsed = started.elapsed();
    assert_eq!(out.error_name(), Some("EXCEEDED_TIME_LIMIT"), "{}", out.last);
    assert_eq!(
        out.last["error"]["message"],
        "Query exceeded maximum time limit of 2.00s"
    );
    assert!(elapsed < Duration::from_secs(8), "stopped after {elapsed:?}");
    let peak = h.engine.memory_peak();
    assert!(
        peak > 1 << 20,
        "the query held {peak} bytes: too little to prove anything"
    );
    assert_eq!(
        h.pool_drains().await,
        0,
        "reservations left after the timeout (peak {peak})"
    );
    h.load_becomes((0, 0)).await;
}

/// `query_max_execution_time` counts from admission, not from submission;
/// the configured default applies when the session sets nothing.
#[tokio::test]
async fn configured_default_time_limit_applies() {
    let h = governed(&GovernanceConfig {
        query_max_execution_time: "300ms".into(),
        ..GovernanceConfig::default()
    });
    let first = h
        .submit_as("ana", "SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    let out = h.follow_body(first).await;
    assert_eq!(out.error_name(), Some("EXCEEDED_TIME_LIMIT"), "{}", out.last);
    assert!(
        out.last["error"]["message"]
            .as_str()
            .unwrap()
            .contains("300.00ms")
    );
    // SHOW SESSION reports the configured default.
    let shown = h
        .follow_body(
            h.submit_as("ana", "SHOW SESSION LIKE 'query_max_execution_time'", &[])
                .await,
        )
        .await;
    assert_eq!(shown.rows[0][2], "300.00ms", "{:?}", shown.rows);
}

/// Beyond the running slots queries wait in the queue (`QUEUED`), and run
/// when a slot frees.
#[tokio::test]
async fn queued_queries_run_when_a_slot_frees() {
    let h = governed(&GovernanceConfig {
        max_running_queries: 1,
        ..held_slots()
    });
    let holder = h.submit_as("ana", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let waiting = h.submit_as("bob", "SELECT 7 AS x", &[]).await;
    assert_eq!(waiting["stats"]["state"], "QUEUED", "{waiting}");
    h.load_becomes((1, 1)).await;
    // A page of a queued query answers without rows, still queued.
    let page = h
        .call(
            Method::GET,
            &path_of(waiting["nextUri"].as_str().unwrap()),
            &[],
            "",
        )
        .await
        .body;
    assert_eq!(page["stats"]["state"], "QUEUED", "{page}");
    assert!(page["nextUri"].is_string());

    h.cancel(&holder).await;
    let out = h.follow_body(page).await;
    assert_eq!(out.state(), "FINISHED", "{}", out.last);
    assert_eq!(out.rows, [vec![json!(7)]]);
    assert!(out.last["stats"]["queuedTimeMillis"].as_u64().unwrap() > 0);
    h.load_becomes((0, 0)).await;
}

/// A full queue refuses at once, on the statement's first page.
#[tokio::test]
async fn a_full_queue_refuses_with_query_queue_full() {
    let h = governed(&GovernanceConfig {
        max_running_queries: 1,
        max_queued_queries: 0,
        ..held_slots()
    });
    let holder = h.submit_as("ana", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let refused = h.first_page_as("bob", "SELECT 1").await;
    assert!(refused.get("nextUri").is_none(), "{refused}");
    assert_eq!(refused["error"]["errorName"], "QUERY_QUEUE_FULL", "{refused}");
    assert_eq!(refused["error"]["errorType"], "INSUFFICIENT_RESOURCES");
    assert_eq!(refused["error"]["errorCode"], 131_074);
    assert_eq!(
        refused["error"]["message"],
        "Too many queued queries for this node (limit 0)"
    );
    assert_eq!(refused["stats"]["state"], "FAILED");
    h.cancel(&holder).await;
    h.load_becomes((0, 0)).await;
}

/// One user's queries are limited; another user's are not held back.
#[tokio::test]
async fn per_user_limit_refuses_that_user_only() {
    let h = governed(&GovernanceConfig {
        max_running_per_user: 1,
        max_queued_per_user: 0,
        ..held_slots()
    });
    let holder = h.submit_as("alice", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let refused = h.first_page_as("alice", "SELECT 1").await;
    assert_eq!(refused["error"]["errorName"], "QUERY_QUEUE_FULL", "{refused}");
    assert_eq!(
        refused["error"]["message"],
        "Too many queued queries for user alice (limit 0)"
    );
    let other = h
        .follow_body(h.submit_as("bob", "SELECT 2 AS y", &[]).await)
        .await;
    assert_eq!(other.rows, [vec![json!(2)]], "{}", other.last);

    // The refusal is listed, with the user's own name.
    let listed = h
        .follow_body(
            h.submit_as(
                "bob",
                "SELECT user, state, error_code FROM system.runtime.queries \
                 WHERE error_code = 'QUERY_QUEUE_FULL'",
                &[],
            )
            .await,
        )
        .await;
    assert_eq!(
        listed.rows,
        [vec![json!("alice"), json!("FAILED"), json!("QUERY_QUEUE_FULL")]]
    );
    h.cancel(&holder).await;
    h.load_becomes((0, 0)).await;
}

/// A query that waits longer than the queue allows fails, and gives its
/// place back.
#[tokio::test]
async fn queue_timeout_fails_the_waiting_query() {
    let h = governed(&GovernanceConfig {
        max_running_queries: 1,
        queue_timeout_secs: 1,
        ..held_slots()
    });
    let holder = h.submit_as("ana", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let waiting = h.submit_as("bob", "SELECT 1", &[]).await;
    let out = h.follow_body(waiting).await;
    assert_eq!(out.error_name(), Some("EXCEEDED_TIME_LIMIT"), "{}", out.last);
    assert_eq!(
        out.last["error"]["message"],
        "Query exceeded maximum queued time of 1.00s"
    );
    h.load_becomes((1, 0)).await;
    h.cancel(&holder).await;
    h.load_becomes((0, 0)).await;
}

/// Canceling a queued query takes it out of the queue.
#[tokio::test]
async fn canceling_a_queued_query_leaves_the_queue() {
    let h = governed(&GovernanceConfig {
        max_running_queries: 1,
        ..held_slots()
    });
    let holder = h.submit_as("ana", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let waiting = h.submit_as("bob", "SELECT 1", &[]).await;
    h.load_becomes((1, 1)).await;
    h.cancel(&waiting).await;
    h.load_becomes((1, 0)).await;
    h.cancel(&holder).await;
    h.load_becomes((0, 0)).await;
    assert_eq!(h.pool_drains().await, 0);
}

/// Resource groups: a user's group caps its queries; the group shows in
/// `system.runtime.queries`.
#[tokio::test]
async fn resource_group_limits_its_members() {
    let h = governed(&GovernanceConfig {
        resource_groups: vec![ResourceGroupConfig {
            name: "etl".into(),
            users: vec!["dbt".into()],
            user_pattern: None,
            source_pattern: None,
            max_running: 1,
            max_queued: 0,
        }],
        ..held_slots()
    });
    let holder = h.submit_as("dbt", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let refused = h.first_page_as("dbt", "SELECT 1").await;
    assert_eq!(
        refused["error"]["message"], "Too many queued queries for resource group etl (limit 0)",
        "{refused}"
    );
    let listed = h
        .follow_body(
            h.submit_as(
                "ana",
                "SELECT \"user\", resource_group_id FROM system.runtime.queries \
                 WHERE state IN ('PLANNING', 'RUNNING') ORDER BY \"user\"",
                &[],
            )
            .await,
        )
        .await;
    assert_eq!(
        listed.rows,
        [
            vec![json!("ana"), json!(["global"])],
            vec![json!("dbt"), json!(["global", "etl"])],
        ],
        "{}",
        listed.last
    );
    h.cancel(&holder).await;
    h.load_becomes((0, 0)).await;
}

/// Statements answered at once (session control) do not take a slot.
#[tokio::test]
async fn session_statements_do_not_queue() {
    let h = governed(&GovernanceConfig {
        max_running_queries: 1,
        max_queued_queries: 0,
        ..held_slots()
    });
    let holder = h.submit_as("ana", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let set = h
        .first_page_as("ana", "SET SESSION query_max_run_time = '1h'")
        .await;
    assert_eq!(set["stats"]["state"], "FINISHED", "{set}");
    h.cancel(&holder).await;
    h.load_becomes((0, 0)).await;
}

/// `CALL system.runtime.kill_query`: only the owner kills; it answers at once
/// on a full node (no slot); the killed query fails with
/// `ADMINISTRATIVELY_KILLED` and its memory and slot are given back.
#[tokio::test]
async fn kill_query_call_stops_a_running_query() {
    let h = governed(&GovernanceConfig {
        max_running_queries: 1,
        max_queued_queries: 0,
        ..held_slots()
    });
    let holder = h.submit_as("ana", HOLDS_A_SLOT, &[]).await;
    h.load_becomes((1, 0)).await;
    let id = holder["id"].as_str().unwrap().to_owned();

    let error = |outcome: &Outcome| outcome.last["error"].clone();
    let stranger = h
        .follow_body(
            h.submit_as("bob", &format!("CALL system.runtime.kill_query('{id}')"), &[])
                .await,
        )
        .await;
    assert_eq!(error(&stranger)["errorName"], "PERMISSION_DENIED");
    assert_eq!(
        error(&stranger)["message"],
        "Access Denied: Cannot kill query owned by ana"
    );
    let missing = h
        .follow_body(
            h.submit_as("ana", "CALL system.runtime.kill_query(query_id => 'nope')", &[])
                .await,
        )
        .await;
    assert_eq!(error(&missing)["errorName"], "NOT_FOUND");
    assert_eq!(error(&missing)["message"], "Target query not found: nope");

    let killed = h
        .follow_body(
            h.submit_as(
                "ana",
                &format!("CALL system.runtime.kill_query(query_id => '{id}', message => 'runaway')"),
                &[],
            )
            .await,
        )
        .await;
    assert!(killed.last.get("error").is_none(), "{}", killed.last);
    assert_eq!(killed.last["updateType"], "CALL");
    let outcome = h.follow_body(holder).await;
    assert_eq!(error(&outcome)["errorName"], "ADMINISTRATIVELY_KILLED");
    assert_eq!(error(&outcome)["errorType"], "USER_ERROR");
    assert_eq!(error(&outcome)["message"], "Query killed. Message: runaway");
    h.load_becomes((0, 0)).await;
    let again = h
        .follow_body(
            h.submit_as("ana", &format!("CALL system.runtime.kill_query('{id}')"), &[])
                .await,
        )
        .await;
    assert_eq!(error(&again)["errorName"], "NOT_SUPPORTED");
    assert_eq!(
        error(&again)["message"],
        format!("Target query is not running: {id}")
    );
    assert_eq!(h.pool_drains().await, 0);
}

impl Harness {
    /// Submits as `user` and reads the first page with rows (or the end), as
    /// a client that reads one page and goes away.
    async fn first_rows_as(&self, user: &str, sql: &str) -> Value {
        let mut page = self.submit_as(user, sql, &[]).await;
        for _ in 0..200 {
            if page.get("data").is_some() || page.get("nextUri").is_none() {
                return page;
            }
            page = self
                .call(Method::GET, &path_of(page["nextUri"].as_str().unwrap()), &[], "")
                .await
                .body;
        }
        panic!("no rows: {page}");
    }

    /// Waits (at most 10 s) until `count` queries are suspended.
    async fn suspended_becomes(&self, count: usize) {
        let admission = self.engine.governance().admission();
        let deadline = Instant::now() + Duration::from_secs(10);
        while admission.suspended() != count {
            assert!(
                Instant::now() < deadline,
                "suspended {}, expected {count} (load {:?})",
                admission.suspended(),
                admission.load()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// D4 (shadow replay of 2026-10-07): an application killed by a deploy
/// abandons 18 queries with large results after their first page, without
/// canceling them. Their executions wait for a client that is gone: they give
/// their running slots back, so the same user's next query runs at once and
/// another user's too; they hold only their buffered batches in the pool; and
/// the abandonment still cancels them, giving everything back.
#[tokio::test]
async fn abandoned_large_results_give_their_slots_back() {
    let h = harness_governed(
        Limits {
            client_timeout: Duration::from_secs(4),
            ..limits()
        },
        &GovernanceConfig::default(),
    );
    let mut abandoned = Vec::new();
    for _ in 0..18 {
        let page = h.first_rows_as("financeiro", HOLDS_A_SLOT).await;
        assert!(page["data"].is_array(), "{page}");
        abandoned.push(page);
    }
    // 16 run (the user's limit) and 2 wait; within the suspension delay the
    // first ones give their slots back, the waiting ones run and suspend too.
    h.suspended_becomes(18).await;
    h.load_becomes((0, 0)).await;
    // Each holds its last page (kept for a retry) and at most the batches
    // buffered for its client, all in the pool.
    let held = h.reserved();
    assert!(held < 18 * (4 << 20), "18 suspended queries hold {held} bytes");

    let started = Instant::now();
    let same_user = h
        .follow_body(h.submit_as("financeiro", "SELECT 1 AS x", &[]).await)
        .await;
    let same_user_secs = started.elapsed();
    assert_eq!(same_user.rows, [vec![json!(1)]], "{}", same_user.last);
    let started = Instant::now();
    let other = h
        .follow_body(h.submit_as("warehouse", "SELECT 2 AS y", &[]).await)
        .await;
    assert_eq!(other.rows, [vec![json!(2)]], "{}", other.last);
    assert!(
        same_user_secs < Duration::from_secs(2) && started.elapsed() < Duration::from_secs(2),
        "same user {same_user_secs:?}, other user {:?}",
        started.elapsed()
    );

    // Listed as running, as the reference engine lists a query blocked on
    // its output.
    let listed = h
        .follow_body(
            h.submit_as(
                "ops",
                "SELECT count(*) FROM system.runtime.queries \
                 WHERE \"user\" = 'financeiro' AND state = 'RUNNING'",
                &[],
            )
            .await,
        )
        .await;
    assert_eq!(listed.rows, [vec![json!(18)]], "{}", listed.last);

    // The client is gone: the sweep after `client_timeout` cancels them.
    tokio::time::sleep(Duration::from_millis(4200)).await;
    h.state.sweep();
    for page in abandoned {
        let out = h.follow_body(page).await;
        assert_eq!(out.error_name(), Some("ABANDONED_QUERY"), "{}", out.last);
    }
    h.suspended_becomes(0).await;
    h.load_becomes((0, 0)).await;
    assert_eq!(
        h.pool_drains().await,
        0,
        "reservations left after the abandonment"
    );
}

/// A suspended query whose client reads again takes a running slot back
/// before producing more, and returns every row: with a per-user limit of
/// one, another query of the user runs while it is suspended, and it waits
/// for that query's slot before it goes on.
#[tokio::test]
async fn a_suspended_query_resumes_with_every_row() {
    let h = governed(&GovernanceConfig {
        max_running_per_user: 1,
        ..GovernanceConfig::default()
    });
    let rows = 300_000;
    let first = h
        .first_rows_as("ana", &format!("SELECT value FROM generate_series(1, {rows})"))
        .await;
    h.suspended_becomes(1).await;
    assert_eq!(h.engine.governance().admission().user_load("ana"), (0, 0));

    // Her next query runs in the slot given back, and holds it: it works
    // (nothing to send until it ends), so it never waits for its client.
    let holder = h
        .submit_as("ana", "SELECT count(*) FROM generate_series(1, 5000000000)", &[])
        .await;
    h.load_becomes((1, 0)).await;

    // Paged again, the first query waits for her slot: no rows beyond what
    // was buffered while it was suspended.
    let page = h
        .call(Method::GET, &path_of(first["nextUri"].as_str().unwrap()), &[], "")
        .await
        .body;
    h.load_becomes((1, 1)).await;
    h.cancel(&holder).await;
    let mut out = h.follow_body(page).await;
    let read_first = first["data"].as_array().unwrap();
    out.rows
        .splice(0..0, read_first.iter().map(|r| r.as_array().unwrap().clone()));
    assert_eq!(out.state(), "FINISHED", "{}", out.last);
    assert_eq!(out.rows.len(), rows);
    assert_eq!(out.rows.last().unwrap()[0], json!(rows));
    h.suspended_becomes(0).await;
    h.load_becomes((0, 0)).await;
    assert_eq!(h.pool_drains().await, 0);
}
