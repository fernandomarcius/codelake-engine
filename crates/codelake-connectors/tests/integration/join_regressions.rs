//! Rows of joins that `DataFusion` 54.1.0 is known to get wrong, checked
//! against Trino 431 reading the same Parquet files. Written by a tester who
//! did not do the port: each test fails while the defect it names is present
//! in the engine, and passes only when the rows match Trino's.
//!
//! - `NOT IN (subquery)` whose subquery holds a NULL (a null-aware anti join)
//!   must return nothing:
//!   - the hash join's dynamic filter is pushed into the scan of the subquery
//!     and drops its NULL before the join can see it. `DataFusion` 53 pushed
//!     the filter only below inner joins; 54 pushes it below every join whose
//!     probe side is preserved, anti joins included. Upstream fix:
//!     apache/datafusion#23104 (`OR key IS NULL`), not in 54.1.0;
//!   - with `datafusion.optimizer.prefer_hash_join = false` the planner routes
//!     it to `SortMergeJoinExec`, which is not null-aware
//!     (apache/datafusion#22810, not in 54.1.0).
//! - A null-safe join (`IS NOT DISTINCT FROM`, `INTERSECT`, `EXCEPT`) must
//!   match a NULL key with a NULL key although the NULLs sit in a row group of
//!   their own: the join's dynamic filter (bounds of the build side's
//!   non-NULL keys) pruned that row group from the probe side's scan.
//! - `x [NOT] IN (subquery)` outside a conjunct of the `WHERE` is a mark join,
//!   which answers FALSE where SQL says UNKNOWN: the engine must answer
//!   Trino's rows where FALSE and UNKNOWN filter the same rows, and refuse
//!   the other forms.
//! - `NestedLoopJoinExec` over a memory limit falls back to spilling its build
//!   side (new in `DataFusion` 54), and the fallback:
//!   - emits a LEFT / LEFT ANTI join's unmatched rows once per probe partition,
//!     from a bitmap that saw only that partition (apache/datafusion#24675);
//!   - drops the unmatched probe rows of a RIGHT / RIGHT ANTI join (a swapped
//!     LEFT JOIN or NOT EXISTS) once the build side runs out
//!     (apache/datafusion#24746);
//!   - executes the build side a second time to spill it
//!     (apache/datafusion#24677). In 54 a file scan shares one queue of
//!     unopened files per plan instance, so the second execution reads only
//!     the files the first left unopened; an exchange (below an aggregation)
//!     hands each partition out once.
//!
//! A test only counts when it reaches the path it is about: the plan must
//! hold the operator, and a nested loop join must have spilled. Otherwise the
//! test fails as vacuous.
//!
//! The warehouse is shared with the Trino container as in
//! `hive_statistics_acceptance.rs` (same absolute path in both containers).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common;
use codelake_connectors::hive::{HiveConnector, HiveParams};
use codelake_core::config::{ExecutionConfig, MemoryConfig};
use codelake_core::{ByteSize, Engine};
use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::util::display::array_value_to_string;
use testcontainers::bollard::Docker;
use testcontainers::bollard::models::MountPointTypeEnum;
use testcontainers::core::{CmdWaitFor, ExecCommand, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);
const SHARED_MOUNT: &str = "/work/target";
/// Prefix of the Trino containers; a random suffix keeps parallel runs apart.
const TRINO_CONTAINER: &str = "codelake-jr-trino";

pub(crate) type Rows = Vec<Vec<String>>;

#[allow(clippy::print_stderr, reason = "evidence for the report")]
pub(crate) fn note(what: &str, detail: impl std::fmt::Display) {
    eprintln!("[JR] {what}: {detail}");
}

// ---------------------------------------------------------------------------
// Shared warehouse directory (same absolute path here and in Trino).
// ---------------------------------------------------------------------------

pub(crate) struct Shared {
    dir: PathBuf,
    mount: Mount,
}

impl Shared {
    pub(crate) fn warehouse(&self) -> PathBuf {
        self.dir.join("warehouse")
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn own_mount(destination: &str) -> Option<Mount> {
    let id = std::fs::read_to_string("/etc/hostname").ok()?.trim().to_owned();
    let docker = Docker::connect_with_local_defaults().ok()?;
    let info = docker.inspect_container(&id, None).await.ok()?;
    let point = info
        .mounts?
        .into_iter()
        .find(|m| m.destination.as_deref() == Some(destination))?;
    match point.typ? {
        MountPointTypeEnum::VOLUME => Some(Mount::volume_mount(point.name?, destination)),
        MountPointTypeEnum::BIND => Some(Mount::bind_mount(point.source?, destination)),
        _ => None,
    }
}

pub(crate) async fn shared() -> Shared {
    let run = format!("run-{}", uuid::Uuid::new_v4().simple());
    let (base, mount) = if let Some(mount) = own_mount(SHARED_MOUNT).await {
        (Path::new(SHARED_MOUNT).join("join-regressions"), mount)
    } else {
        let base = std::env::temp_dir().join("codelake-join-regressions");
        let path = base.display().to_string();
        (base, Mount::bind_mount(path.clone(), path))
    };
    let dir = base.join(run);
    std::fs::create_dir_all(dir.join("warehouse")).unwrap();
    Shared { dir, mount }
}

// ---------------------------------------------------------------------------
// Trino 431, the reference.
// ---------------------------------------------------------------------------

pub(crate) struct Trino(ContainerAsync<GenericImage>);

impl Trino {
    pub(crate) async fn start(shared: &Shared, name: &str) -> Self {
        let properties = format!(
            "connector.name=hive\n\
             hive.metastore=file\n\
             hive.metastore.catalog.dir=file:{}\n\
             hive.parquet.time-zone=America/Sao_Paulo\n\
             hive.storage-format=PARQUET\n\
             hive.allow-drop-table=true\n",
            shared.warehouse().display()
        );
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let container = GenericImage::new("trinodb/trino", "431")
            .with_wait_for(WaitFor::message_on_stderr("SERVER STARTED"))
            .with_user("root")
            .with_container_name(format!("{TRINO_CONTAINER}-{name}-{}", &suffix[..8]))
            .with_mount(shared.mount.clone())
            .with_copy_to("/etc/trino/catalog/hive.properties", properties.into_bytes())
            .with_startup_timeout(STARTUP_TIMEOUT)
            .start()
            .await
            .unwrap();
        let trino = Self(container);
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        // Ready once the coordinator lists an active worker.
        loop {
            match trino.try_sql(common::ACTIVE_WORKERS).await {
                Ok(rows)
                    if common::counts_a_worker(
                        rows.first().and_then(|row| row.first()).map(String::as_str),
                    ) =>
                {
                    break;
                }
                Ok(rows) => assert!(Instant::now() < deadline, "no active worker in time: {rows:?}"),
                Err(e) if Instant::now() < deadline && e.contains("No nodes available") => {}
                Err(e) => panic!("Trino did not accept queries: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        trino
    }

    async fn try_sql(&self, sql: &str) -> Result<Rows, String> {
        let args = vec![
            "trino",
            "--debug",
            "--output-format",
            "TSV",
            "--catalog",
            "hive",
            "--execute",
            sql,
        ];
        let mut result = self
            .0
            .exec(ExecCommand::new(args).with_cmd_ready_condition(CmdWaitFor::exit()))
            .await
            .unwrap();
        let stdout = String::from_utf8(result.stdout_to_vec().await.unwrap()).unwrap();
        let stderr = String::from_utf8_lossy(&result.stderr_to_vec().await.unwrap()).into_owned();
        let mut code = None;
        for _ in 0..50 {
            code = result.exit_code().await.unwrap();
            if code.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let errors: Vec<&str> = stderr
            .lines()
            .filter(|l| {
                (l.starts_with("Query ") && l.contains(" failed"))
                    || l.starts_with("Caused by:")
                    || (!l.starts_with(char::is_whitespace) && l.contains("Exception: "))
            })
            .collect();
        if code != Some(0) || !errors.is_empty() {
            return Err(format!("exit {code:?}: {}", errors.join(" | ")));
        }
        Ok(stdout
            .lines()
            .map(|line| line.split('\t').map(str::to_owned).collect())
            .collect())
    }

    async fn sql(&self, sql: &str) -> Rows {
        self.try_sql(sql)
            .await
            .unwrap_or_else(|e| panic!("Trino `{sql}` failed: {e}"))
    }

    /// Runs `statements` in one CLI call (each start of the CLI costs
    /// seconds) and splits the output by a marker row.
    pub(crate) async fn batch(&self, statements: &[&str]) -> Vec<Rows> {
        const MARK: &str = "@@end-of-statement@@";
        let mut script = String::new();
        for statement in statements {
            script.push_str(statement);
            script.push_str(";\nSELECT '");
            script.push_str(MARK);
            script.push_str("';\n");
        }
        let mut results = vec![Vec::new()];
        for row in self.sql(&script).await {
            if row == [MARK] {
                results.push(Vec::new());
            } else {
                results.last_mut().unwrap().push(row);
            }
        }
        results.pop();
        assert_eq!(results.len(), statements.len(), "Trino batch output");
        results
    }
}

// ---------------------------------------------------------------------------
// CodeLake side.
// ---------------------------------------------------------------------------

/// An engine over the shared warehouse with `settings` applied
/// (`datafusion.*` session options).
pub(crate) async fn open(
    warehouse: &Path,
    spill: &Path,
    limit: ByteSize,
    execution: &ExecutionConfig,
    settings: &[(&str, &str)],
) -> Engine {
    let memory = MemoryConfig {
        limit,
        spill_dir: spill.to_owned(),
        spill_limit: ByteSize::gib(2),
        metadata_cache_limit: ByteSize::mib(8),
    };
    let mut engine = Engine::new(&memory, execution).expect("engine");
    common::attach(
        &mut engine,
        Arc::new(
            HiveConnector::new(
                "hive".into(),
                HiveParams {
                    warehouse: warehouse.display().to_string(),
                    s3_endpoint: None,
                    s3_region: "us-east-1".into(),
                    credentials: None,
                    path_style: false,
                },
            )
            .with_metastore_names(crate::common::reference_names()),
        ),
    )
    .await;
    for (key, value) in settings {
        set(&engine, key, value).await;
    }
    engine
}

async fn set(engine: &Engine, key: &str, value: &str) {
    engine
        .context()
        .sql(&format!("SET {key} = {value}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

fn to_rows(batches: &[RecordBatch]) -> Rows {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(row) {
                            "NULL".to_owned()
                        } else {
                            array_value_to_string(column, row).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

/// The rows `sql` answers, or the error it fails with.
pub(crate) async fn try_cl(engine: &Engine, sql: &str) -> Result<Rows, String> {
    let frame = engine.sql(sql).await.map_err(|e| format!("planning: {e}"))?;
    let batches = frame.collect().await.map_err(|e| format!("running: {e}"))?;
    Ok(to_rows(&batches))
}

/// The text of every row `EXPLAIN [ANALYZE] sql` answers for `kind`
/// (`physical_plan`, `Plan with Metrics`).
pub(crate) async fn explain(engine: &Engine, prefix: &str, sql: &str, kind: &str) -> Result<String, String> {
    let batches = engine
        .sql(&format!("{prefix} {sql}"))
        .await
        .map_err(|e| format!("planning: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("running: {e}"))?;
    let mut text = String::new();
    for batch in &batches {
        let kinds = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let plans = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == kind {
                text.push_str(plans.value(row));
            }
        }
    }
    if text.is_empty() {
        return Err(format!("no `{kind}` row"));
    }
    Ok(text)
}

pub(crate) async fn physical_plan(engine: &Engine, sql: &str) -> String {
    explain(engine, "EXPLAIN", sql, "physical_plan")
        .await
        .unwrap_or_else(|e| panic!("EXPLAIN `{sql}`: {e}"))
}

pub(crate) fn sorted(mut rows: Rows) -> Rows {
    rows.sort();
    rows
}

/// A query, the operator line its plan must contain for the test to be about
/// what it says, and whether that operator must have spilled. With the
/// engine's guard (ADR 0014) a nested loop join over the limit is refused
/// before it spills: the nested loop cases pass through that refusal, and
/// check the rows only if the join ever runs within the pool.
///
/// A case that must spill passes with Trino's rows, or when the engine refuses
/// the query with a resources error (what `DataFusion` 53 did, having no
/// fallback, and what the upstream fix of #24675 does): a loud refusal is not
/// a wrong answer. Other rows or any other error fail.
struct Case {
    label: &'static str,
    sql: &'static str,
    operator: &'static str,
    must_spill: bool,
}

/// Every nested loop join of the analyzed plan spilled at least once.
fn nested_loop_joins_spilled(analyzed: &str) -> bool {
    let joins: Vec<&str> = analyzed
        .lines()
        .filter(|l| l.trim_start().starts_with("NestedLoopJoinExec"))
        .collect();
    !joins.is_empty()
        && joins.iter().all(|line| {
            line.split("spill_count=")
                .nth(1)
                .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<u64>().ok())
                .is_some_and(|n| n > 0)
        })
}

/// The pool is back to zero once the tasks a failed query aborted are gone.
pub(crate) async fn assert_pool_given_back(engine: &Engine) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while engine.memory().reserved_bytes != 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(engine.memory().reserved_bytes, 0, "pool given back");
}

/// Runs every case on `engine` and on Trino; returns what differs.
async fn compare(engine: &Engine, trino: &Trino, cases: &[Case]) -> Vec<String> {
    let statements: Vec<&str> = cases.iter().map(|c| c.sql).collect();
    let references = trino.batch(&statements).await;
    let mut failures = Vec::new();
    for (case, reference) in cases.iter().zip(references) {
        let reference = sorted(reference);
        let plan = physical_plan(engine, case.sql).await;
        note(case.label, format!("plan:\n{plan}"));
        if !plan.contains(case.operator) {
            failures.push(format!(
                "{}: vacuous, the plan has no `{}`:\n{plan}",
                case.label, case.operator
            ));
            continue;
        }
        let rows = try_cl(engine, case.sql).await.map(sorted);
        note(case.label, format!("trino={reference:?} codelake={rows:?}"));
        match rows {
            Ok(rows) if rows == reference => {}
            Ok(rows) => {
                failures.push(format!(
                    "{}: trino={reference:?} codelake={rows:?}\n  {}",
                    case.label, case.sql
                ));
                continue;
            }
            Err(error) if case.must_spill && error.contains("Resources exhausted") => {
                note(case.label, "refused for lack of memory (accepted)");
                continue;
            }
            Err(error) => {
                failures.push(format!(
                    "{}: trino={reference:?} codelake failed: {error}\n  {}",
                    case.label, case.sql
                ));
                continue;
            }
        }
        if case.must_spill {
            match explain(engine, "EXPLAIN ANALYZE", case.sql, "Plan with Metrics").await {
                Ok(analyzed) if nested_loop_joins_spilled(&analyzed) => {}
                Ok(analyzed) => failures.push(format!(
                    "{}: vacuous, the nested loop join did not spill:\n{analyzed}",
                    case.label
                )),
                Err(error) => failures.push(format!("{}: EXPLAIN ANALYZE failed: {error}", case.label)),
            }
        }
    }
    failures
}

// ---------------------------------------------------------------------------
// NOT IN with a NULL in the subquery.
// ---------------------------------------------------------------------------

/// `outer_rows.k` in 0..29 with 30 NULLs; the subquery tables hold 0..24,
/// plus a NULL in a file of its own (`sub_null_file`, what an `INSERT`
/// leaves) or in the same file as the keys (`sub_null_row`).
const NOT_IN_SETUP: [&str; 6] = [
    "CREATE SCHEMA hive.jr",
    "CREATE TABLE hive.jr.outer_rows AS \
     SELECT CAST(x AS bigint) AS id, \
            CASE WHEN x % 100 = 7 THEN NULL ELSE CAST(x % 30 AS bigint) END AS k \
     FROM UNNEST(sequence(1, 3000)) t(x)",
    "CREATE TABLE hive.jr.sub_keys AS SELECT CAST(x AS bigint) AS n FROM UNNEST(sequence(0, 24)) t(x)",
    "CREATE TABLE hive.jr.sub_null_file AS SELECT n FROM hive.jr.sub_keys",
    "INSERT INTO hive.jr.sub_null_file VALUES (CAST(NULL AS bigint))",
    "CREATE TABLE hive.jr.sub_null_row AS \
     SELECT CASE WHEN x = 25 THEN NULL ELSE CAST(x AS bigint) END AS n \
     FROM UNNEST(sequence(0, 25)) t(x)",
];

const NOT_IN_CASES: [Case; 5] = [
    Case {
        label: "NOT IN, NULL in its own file",
        sql: "SELECT count(*) FROM hive.jr.outer_rows WHERE k NOT IN (SELECT n FROM hive.jr.sub_null_file)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN, NULL in the keys' file",
        sql: "SELECT count(*) FROM hive.jr.outer_rows WHERE k NOT IN (SELECT n FROM hive.jr.sub_null_row)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN, rows, NULL in its own file",
        sql: "SELECT id, k FROM hive.jr.outer_rows WHERE k NOT IN (SELECT n FROM hive.jr.sub_null_file)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN, filtered subquery with the NULL",
        sql: "SELECT count(*) FROM hive.jr.outer_rows \
              WHERE k NOT IN (SELECT n FROM hive.jr.sub_null_file WHERE n IS NULL OR n < 10)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN, no NULL in the subquery",
        sql: "SELECT count(*), count(k) FROM hive.jr.outer_rows WHERE k NOT IN (SELECT n FROM hive.jr.sub_keys)",
        operator: "",
        must_spill: false,
    },
];

/// Which of `cases` give Trino's rows once the join's dynamic filter is off:
/// evidence of the cause, printed with the failure.
async fn without_dynamic_filter(warehouse: &Path, spill: &Path, trino: &Trino, cases: &[Case]) -> String {
    let engine = open(
        warehouse,
        spill,
        ByteSize::mib(256),
        &ExecutionConfig::default(),
        &[(
            "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
            "false",
        )],
    )
    .await;
    let failures = compare(&engine, trino, cases).await;
    if failures.is_empty() {
        "every case matches Trino with the join dynamic filter off".to_owned()
    } else {
        format!(
            "still wrong with the join dynamic filter off:\n{}",
            failures.join("\n")
        )
    }
}

/// `x NOT IN (subquery)` is UNKNOWN for every row once the subquery holds a
/// NULL, so the query returns nothing (Trino and the SQL standard). Default
/// settings, and with row-level Parquet filtering, where the pushed filter
/// also reaches a NULL that shares a file with the keys.
#[tokio::test]
async fn not_in_with_a_null_in_the_subquery_returns_nothing() {
    let shared = shared().await;
    let warehouse = shared.warehouse();
    let trino = Trino::start(&shared, "notin").await;
    trino.batch(&NOT_IN_SETUP).await;
    let spill = tempfile::tempdir().unwrap();
    let mut failures = Vec::new();
    for settings in [
        &[][..],
        &[("datafusion.execution.parquet.pushdown_filters", "true")][..],
    ] {
        let engine = open(
            &warehouse,
            spill.path(),
            ByteSize::mib(256),
            &ExecutionConfig::default(),
            settings,
        )
        .await;
        for failure in compare(&engine, &trino, &NOT_IN_CASES).await {
            failures.push(format!("settings {settings:?}: {failure}"));
        }
    }
    if !failures.is_empty() {
        let cause = without_dynamic_filter(&warehouse, spill.path(), &trino, &NOT_IN_CASES).await;
        panic!("rows differ:\n{}\n\n{cause}", failures.join("\n"));
    }
}

/// apache/datafusion#22810: with `prefer_hash_join = false` and several
/// partitions, a null-aware anti join is planned as `SortMergeJoinExec`,
/// which knows nothing of NULLs: neither a NULL in the subquery (no row
/// qualifies) nor a NULL outer value (`NULL NOT IN (non-empty)` is UNKNOWN).
/// A session may set it (`SET datafusion.optimizer.prefer_hash_join = false`
/// is not refused).
#[tokio::test]
async fn not_in_keeps_null_semantics_when_hash_join_is_not_preferred() {
    let shared = shared().await;
    let warehouse = shared.warehouse();
    let trino = Trino::start(&shared, "smj").await;
    trino.batch(&NOT_IN_SETUP).await;
    let spill = tempfile::tempdir().unwrap();
    let execution = ExecutionConfig {
        target_partitions: Some(4),
        ..ExecutionConfig::default()
    };
    let engine = open(
        &warehouse,
        spill.path(),
        ByteSize::mib(256),
        &execution,
        &[
            ("datafusion.optimizer.prefer_hash_join", "false"),
            // The dynamic filter defect is the other test's; keep it out.
            (
                "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
                "false",
            ),
        ],
    )
    .await;
    // The rows must match whichever join the planner picks.
    let failures = compare(&engine, &trino, &NOT_IN_CASES).await;
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
}

/// `x [NOT] IN (subquery)` outside a conjunct of the `WHERE`: planned as a
/// mark join, which answers FALSE where SQL says UNKNOWN. Where FALSE and
/// UNKNOWN filter the same rows the engine answers, and the rows must be
/// Trino's; elsewhere, with a NULL possible on either side, it refuses.
const MEMBERSHIP_CASES: [Case; 6] = [
    Case {
        label: "IN under OR, NULL in the subquery",
        sql: "SELECT id, k FROM hive.jr.outer_rows \
              WHERE k IN (SELECT n FROM hive.jr.sub_null_file) OR id > 2990",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN under NOT and OR, NULL in the subquery",
        sql: "SELECT id, k FROM hive.jr.outer_rows \
              WHERE NOT (k NOT IN (SELECT n FROM hive.jr.sub_null_row) OR id < 2990)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN joined by AND, with a condition under OR",
        sql: "SELECT count(*) FROM hive.jr.outer_rows \
              WHERE k NOT IN (SELECT n FROM hive.jr.sub_keys) AND (id < 100 OR id > 2900)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT (k IN …), NULL in the subquery",
        sql: "SELECT count(*) FROM hive.jr.outer_rows WHERE NOT (k IN (SELECT n FROM hive.jr.sub_null_file))",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT over OR (a conjunction once simplified), NULL in the subquery",
        sql: "SELECT count(*) FROM hive.jr.outer_rows \
              WHERE NOT (k IN (SELECT n FROM hive.jr.sub_null_file) OR id > 2990)",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT IN in HAVING, NULL in the subquery",
        sql: "SELECT k, count(*) FROM hive.jr.outer_rows GROUP BY k \
              HAVING k NOT IN (SELECT n FROM hive.jr.sub_null_row)",
        operator: "",
        must_spill: false,
    },
];

/// Forms whose mark join answered rows Trino filters out: refused.
const REFUSED_MEMBERSHIPS: [&str; 3] = [
    "SELECT count(*) FROM hive.jr.outer_rows WHERE k NOT IN (SELECT n FROM hive.jr.sub_null_file) OR id > 2990",
    "SELECT count(*) FROM hive.jr.outer_rows \
     WHERE CASE WHEN k NOT IN (SELECT n FROM hive.jr.sub_null_row) THEN true ELSE false END",
    "SELECT count(*) FROM hive.jr.outer_rows WHERE (k IN (SELECT n FROM hive.jr.sub_keys)) IS NULL",
];

#[tokio::test]
async fn in_subquery_answers_trinos_rows_or_is_refused() {
    let shared = shared().await;
    let trino = Trino::start(&shared, "member").await;
    trino.batch(&NOT_IN_SETUP).await;
    let spill = tempfile::tempdir().unwrap();
    let engine = open(
        &shared.warehouse(),
        spill.path(),
        ByteSize::mib(256),
        &ExecutionConfig::default(),
        &[],
    )
    .await;
    let mut failures = compare(&engine, &trino, &MEMBERSHIP_CASES).await;
    // Trino answers the refused forms; what the engine would have answered
    // differs from them, so it must not answer at all.
    let references = trino.batch(&REFUSED_MEMBERSHIPS).await;
    for (sql, reference) in REFUSED_MEMBERSHIPS.iter().zip(references) {
        match try_cl(&engine, sql).await {
            Err(error) if error.contains("NOT_SUPPORTED") && error.contains("may be NULL") => {
                note("refused", format!("trino={reference:?}: {sql}"));
            }
            other => failures.push(format!(
                "expected a refusal, got {other:?} (trino={reference:?}): {sql}"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

// ---------------------------------------------------------------------------
// Null-safe joins over a row group that holds only NULL keys.
// ---------------------------------------------------------------------------

/// `ns_tr`, written by Trino: (1, 'a', 10) and (2, 'b', 20) in one file, then
/// (3, NULL, NULL) in a file of its own (a second `INSERT`), so the NULL keys
/// are a row group of their own.
const NULL_SAFE_SETUP: [&str; 6] = [
    "CREATE SCHEMA hive.jr",
    "CREATE TABLE hive.jr.ns_tr (id bigint, v varchar, k bigint) WITH (format = 'PARQUET')",
    "INSERT INTO hive.jr.ns_tr VALUES (1, 'a', 10), (2, 'b', 20)",
    "INSERT INTO hive.jr.ns_tr VALUES (3, NULL, NULL)",
    "CREATE TABLE hive.jr.flags (id bigint, f boolean) WITH (format = 'PARQUET')",
    "INSERT INTO hive.jr.flags VALUES (1, true), (2, false), (3, NULL)",
];

/// `IS [NOT] DISTINCT FROM` binds tighter than `AND` and `OR`; the parser
/// took the rest of the condition as its right operand, and over booleans
/// that answered other rows.
const DISTINCT_FROM_PRECEDENCE: [Case; 4] = [
    Case {
        label: "IS NOT DISTINCT FROM … AND, join condition",
        sql: "SELECT a.id, b.id FROM hive.jr.flags a JOIN hive.jr.flags b \
              ON a.f IS NOT DISTINCT FROM b.f AND a.id = b.id",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "IS DISTINCT FROM … OR",
        sql: "SELECT a.id, b.id FROM hive.jr.flags a, hive.jr.flags b \
              WHERE a.f IS DISTINCT FROM b.f OR a.id = b.id",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "IS DISTINCT FROM … AND",
        sql: "SELECT id FROM hive.jr.flags WHERE f IS DISTINCT FROM true AND id > 1",
        operator: "",
        must_spill: false,
    },
    Case {
        label: "NOT … IS DISTINCT FROM … OR",
        sql: "SELECT id FROM hive.jr.flags WHERE NOT f IS DISTINCT FROM false OR id = 3",
        operator: "",
        must_spill: false,
    },
];

/// `ns_cl`: the same rows, written by the engine the same way.
const NULL_SAFE_ENGINE_SETUP: [&str; 3] = [
    "CREATE TABLE hive.jr.ns_cl (id bigint, v varchar, k bigint)",
    "INSERT INTO hive.jr.ns_cl VALUES (1, 'a', 10), (2, 'b', 20)",
    "INSERT INTO hive.jr.ns_cl VALUES (3, NULL, NULL)",
];

/// Every join below matches NULL with NULL; the NULL row must match itself.
/// Each must run through the guard (`NullKeysKept` in the plan). Only the
/// `id`s are compared, which are never NULL.
macro_rules! null_safe_cases {
    ($t:literal, $u:literal) => {
        [
            Case {
                label: concat!("EXCEPT, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT id FROM (SELECT * FROM hive.jr.",
                    $t,
                    " EXCEPT SELECT * FROM hive.jr.",
                    $u,
                    ")"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
            Case {
                label: concat!("INTERSECT, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT id FROM (SELECT * FROM hive.jr.",
                    $t,
                    " INTERSECT SELECT * FROM hive.jr.",
                    $u,
                    ")"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
            Case {
                label: concat!("JOIN ON IS NOT DISTINCT FROM, varchar, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT a.id, b.id FROM hive.jr.",
                    $t,
                    " a JOIN hive.jr.",
                    $u,
                    " b ON a.v IS NOT DISTINCT FROM b.v"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
            Case {
                label: concat!("JOIN ON IS NOT DISTINCT FROM, bigint, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT a.id, b.id FROM hive.jr.",
                    $t,
                    " a JOIN hive.jr.",
                    $u,
                    " b ON a.k IS NOT DISTINCT FROM b.k"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
            Case {
                label: concat!("LEFT JOIN ON IS NOT DISTINCT FROM, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT a.id, b.id FROM hive.jr.",
                    $t,
                    " a LEFT JOIN hive.jr.",
                    $u,
                    " b ON a.v IS NOT DISTINCT FROM b.v"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
            Case {
                label: concat!("NOT EXISTS with IS NOT DISTINCT FROM, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT id FROM hive.jr.",
                    $t,
                    " a WHERE NOT EXISTS (SELECT 1 FROM hive.jr.",
                    $u,
                    " b WHERE b.v IS NOT DISTINCT FROM a.v)"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
            Case {
                label: concat!("EXISTS with IS NOT DISTINCT FROM, ", $t, " and ", $u),
                sql: concat!(
                    "SELECT id FROM hive.jr.",
                    $t,
                    " a WHERE EXISTS (SELECT 1 FROM hive.jr.",
                    $u,
                    " b WHERE b.v IS NOT DISTINCT FROM a.v)"
                ),
                operator: "NullKeysKept",
                must_spill: false,
            },
        ]
    };
}

/// apache/datafusion 54.1: a null-safe hash join pushes its dynamic filter
/// (bounds of the build side's non-NULL keys) into the scan of its probe
/// side, which prunes the row group holding only the NULL keys: `t EXCEPT t`
/// returned the NULL row, `t INTERSECT t` and the joins lost it. Tables
/// written by Trino and by the engine, alone and against each other, with
/// default settings and with row-level Parquet filtering; and the grouping of
/// `IS [NOT] DISTINCT FROM` with `AND`/`OR` (`DISTINCT_FROM_PRECEDENCE`).
#[tokio::test]
async fn null_safe_joins_keep_the_rows_whose_key_is_null() {
    let shared = shared().await;
    let trino = Trino::start(&shared, "nullsafe").await;
    trino.batch(&NULL_SAFE_SETUP).await;
    let spill = tempfile::tempdir().unwrap();
    let writer = open(
        &shared.warehouse(),
        spill.path(),
        ByteSize::mib(256),
        &ExecutionConfig::default(),
        &[],
    )
    .await;
    for sql in NULL_SAFE_ENGINE_SETUP {
        try_cl(&writer, sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}`: {e}"));
    }
    drop(writer);
    let cases: Vec<Case> = [
        null_safe_cases!("ns_tr", "ns_tr"),
        null_safe_cases!("ns_cl", "ns_cl"),
        null_safe_cases!("ns_tr", "ns_cl"),
        null_safe_cases!("ns_cl", "ns_tr"),
    ]
    .into_iter()
    .flatten()
    .chain(DISTINCT_FROM_PRECEDENCE)
    .collect();
    let mut failures = Vec::new();
    for settings in [
        &[][..],
        &[("datafusion.execution.parquet.pushdown_filters", "true")][..],
    ] {
        let engine = open(
            &shared.warehouse(),
            spill.path(),
            ByteSize::mib(256),
            &ExecutionConfig::default(),
            settings,
        )
        .await;
        for failure in compare(&engine, &trino, &cases).await {
            failures.push(format!("settings {settings:?}: {failure}"));
        }
    }
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
}

// ---------------------------------------------------------------------------
// Nested loop joins over the memory limit.
// ---------------------------------------------------------------------------

/// Wide rows (16 000 characters each): `wide_small` is about 19 MB in memory,
/// written as six files of 200 rows (one CTAS, then five inserts), and `wide_big`
/// about 32 MB; both are far over the 12 MiB pool, so a nested loop join
/// built on either cannot hold its build side and spills it. `s.v > b.w + 90`
/// matches few pairs, leaving most rows unmatched.
const WIDE_SETUP: [&str; 8] = [
    "CREATE SCHEMA hive.jr",
    "CREATE TABLE hive.jr.wide_small AS \
     SELECT CAST(x AS bigint) AS id, CAST(x % 97 AS bigint) AS v, \
            rpad(CAST(x AS varchar), 16000, '*') AS pad \
     FROM UNNEST(sequence(1, 200)) t(x)",
    "INSERT INTO hive.jr.wide_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 16000, '*') FROM UNNEST(sequence(201, 400)) t(x)",
    "INSERT INTO hive.jr.wide_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 16000, '*') FROM UNNEST(sequence(401, 600)) t(x)",
    "INSERT INTO hive.jr.wide_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 16000, '*') FROM UNNEST(sequence(601, 800)) t(x)",
    "INSERT INTO hive.jr.wide_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 16000, '*') FROM UNNEST(sequence(801, 1000)) t(x)",
    "INSERT INTO hive.jr.wide_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 16000, '*') FROM UNNEST(sequence(1001, 1200)) t(x)",
    "CREATE TABLE hive.jr.wide_big AS \
     SELECT CAST(x AS bigint) AS id, CAST(x % 101 AS bigint) AS w, \
            rpad(CAST(x AS varchar), 16000, '#') AS pad \
     FROM UNNEST(sequence(1, 2000)) t(x)",
];

const POOL: ByteSize = ByteSize::mib(12);

/// `DataFusion` 54's file scans share one queue of unopened files per plan
/// instance (`enable_file_stream_work_stealing`, on by default), so a second
/// execution of the same scan reads only what the first left unopened: the
/// fallback's second execution (apache/datafusion#24677) then misses build
/// rows. Tests about the other fallback defects turn it off, so each test
/// fails on its own defect.
const REPLAYABLE_SCANS: (&str, &str) = ("datafusion.execution.enable_file_stream_work_stealing", "false");

async fn wide_engine(warehouse: &Path, spill: &Path, batch_size: usize, settings: &[(&str, &str)]) -> Engine {
    let execution = ExecutionConfig {
        batch_size,
        target_partitions: Some(4),
        ..ExecutionConfig::default()
    };
    open(warehouse, spill, POOL, &execution, settings).await
}

async fn wide_trino(shared: &Shared, name: &str) -> Trino {
    let trino = Trino::start(shared, name).await;
    trino.batch(&WIDE_SETUP).await;
    trino
}

/// apache/datafusion#24675: the preserved side (`wide_small`, the smaller)
/// builds a LEFT or LEFT ANTI nested loop join probed by several partitions;
/// after the fallback each partition emits the build rows that its own probe
/// rows did not match, so a row is emitted once per partition, or as
/// unmatched although another partition matched it.
#[tokio::test]
async fn left_nested_loop_join_over_the_limit_emits_each_unmatched_row_once() {
    let shared = shared().await;
    let trino = wide_trino(&shared, "nlj-left").await;
    let spill = tempfile::tempdir().unwrap();
    let engine = wide_engine(&shared.warehouse(), spill.path(), 32, &[REPLAYABLE_SCANS]).await;
    let cases = [
        Case {
            label: "LEFT JOIN, build = preserved side",
            sql: "SELECT count(*), count(b.id), sum(length(s.pad)), coalesce(sum(length(b.pad)), 0) \
                  FROM hive.jr.wide_small s LEFT JOIN hive.jr.wide_big b ON s.v > b.w + 90",
            operator: "NestedLoopJoinExec: join_type=Left,",
            must_spill: true,
        },
        Case {
            label: "NOT EXISTS, build = preserved side",
            sql: "SELECT count(*), sum(length(s.pad)) FROM hive.jr.wide_small s \
                  WHERE NOT EXISTS (SELECT 1 FROM hive.jr.wide_big b \
                                    WHERE s.v > b.w + 90 AND b.pad <> s.pad)",
            operator: "NestedLoopJoinExec: join_type=LeftAnti,",
            must_spill: true,
        },
    ];
    let failures = compare(&engine, &trino, &cases).await;
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
    assert_pool_given_back(&engine).await;
}

/// Rows of 40 000 characters: `huge_small` is six files of 200 rows (8 MB
/// per file, one batch each with `batch_size = 256`), `huge_big` 2 000 rows.
/// Under the 12 MiB pool a chunk of the spilled build side holds exactly two
/// batches (the second is the one refused), so the last chunk ends on a
/// refusal: the case apache/datafusion#24746 mishandles.
const HUGE_SETUP: [&str; 8] = [
    "CREATE SCHEMA hive.jr",
    "CREATE TABLE hive.jr.huge_small AS \
     SELECT CAST(x AS bigint) AS id, CAST(x % 97 AS bigint) AS v, \
            rpad(CAST(x AS varchar), 40000, '*') AS pad \
     FROM UNNEST(sequence(1, 200)) t(x)",
    "INSERT INTO hive.jr.huge_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 40000, '*') FROM UNNEST(sequence(201, 400)) t(x)",
    "INSERT INTO hive.jr.huge_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 40000, '*') FROM UNNEST(sequence(401, 600)) t(x)",
    "INSERT INTO hive.jr.huge_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 40000, '*') FROM UNNEST(sequence(601, 800)) t(x)",
    "INSERT INTO hive.jr.huge_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 40000, '*') FROM UNNEST(sequence(801, 1000)) t(x)",
    "INSERT INTO hive.jr.huge_small SELECT CAST(x AS bigint), CAST(x % 97 AS bigint), \
     rpad(CAST(x AS varchar), 40000, '*') FROM UNNEST(sequence(1001, 1200)) t(x)",
    "CREATE TABLE hive.jr.huge_big AS \
     SELECT CAST(x AS bigint) AS id, CAST(x % 101 AS bigint) AS w, \
            rpad(CAST(x AS varchar), 40000, '#') AS pad \
     FROM UNNEST(sequence(1, 2000)) t(x)",
];

/// apache/datafusion#24746: the larger side is written first, so the join is
/// swapped into a RIGHT / RIGHT ANTI nested loop join built on `huge_small`;
/// when the last chunk of the spilled build side ends on a refused batch,
/// the next load finds nothing and the join ends without emitting the probe
/// rows that no chunk matched.
#[tokio::test]
async fn swapped_nested_loop_join_over_the_limit_keeps_unmatched_probe_rows() {
    let shared = shared().await;
    let trino = Trino::start(&shared, "nlj-right").await;
    trino.batch(&HUGE_SETUP).await;
    let spill = tempfile::tempdir().unwrap();
    let engine = wide_engine(&shared.warehouse(), spill.path(), 256, &[REPLAYABLE_SCANS]).await;
    let cases = [
        Case {
            label: "LEFT JOIN swapped to RIGHT",
            sql: "SELECT count(*), count(s.id), sum(length(b.pad)), coalesce(sum(length(s.pad)), 0) \
                  FROM hive.jr.huge_big b LEFT JOIN hive.jr.huge_small s ON s.v > b.w + 90",
            operator: "NestedLoopJoinExec: join_type=Right,",
            must_spill: true,
        },
        Case {
            label: "NOT EXISTS swapped to RIGHT ANTI",
            sql: "SELECT count(*), sum(length(b.pad)) FROM hive.jr.huge_big b \
                  WHERE NOT EXISTS (SELECT 1 FROM hive.jr.huge_small s \
                                    WHERE s.v > b.w + 90 AND s.pad <> b.pad)",
            operator: "NestedLoopJoinExec: join_type=RightAnti,",
            must_spill: true,
        },
    ];
    let failures = compare(&engine, &trino, &cases).await;
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
    assert_pool_given_back(&engine).await;
}

/// apache/datafusion#24677: the fallback executes the build side a second
/// time to spill it. Default settings: a scan of a table in several files
/// hands the second execution only the files the first left unopened, and an
/// exchange (below an aggregation) hands each partition out once.
#[tokio::test]
async fn nested_loop_join_over_the_limit_reads_its_whole_build_side() {
    let shared = shared().await;
    let trino = wide_trino(&shared, "nlj-once").await;
    let spill = tempfile::tempdir().unwrap();
    let engine = wide_engine(&shared.warehouse(), spill.path(), 32, &[]).await;
    let cases = [
        Case {
            label: "INNER JOIN built on a table of six files",
            sql: "SELECT count(*), sum(length(s.pad)), sum(length(b.pad)) \
                  FROM hive.jr.wide_small s JOIN hive.jr.wide_big b ON s.v > b.w + 90",
            operator: "NestedLoopJoinExec: join_type=Inner,",
            must_spill: true,
        },
        Case {
            label: "INNER JOIN built on an aggregation",
            sql: "SELECT count(*), sum(length(s.pad)), sum(length(b.pad)) \
                  FROM (SELECT id, max(v) AS v, max(pad) AS pad FROM hive.jr.wide_small GROUP BY id) s \
                  JOIN hive.jr.wide_big b ON s.v > b.w + 90",
            operator: "NestedLoopJoinExec: join_type=Inner,",
            must_spill: true,
        },
    ];
    let failures = compare(&engine, &trino, &cases).await;
    assert!(failures.is_empty(), "rows differ:\n{}", failures.join("\n"));
    assert_pool_given_back(&engine).await;
}
