//! One engine phase: wait until the engine answers, measure it idle, then run
//! every case (setup → warmup → measured repetitions → verify → teardown).

use std::io::Write as _;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::driver::{Driver, DriverError, PgClient, TrinoClient};
use crate::model::{CaseRun, EngineRun, ProcessMetrics, RunStatus, SCHEMA_VERSION, percentile};
use crate::probe::{Cgroup, Sampler, SpillProbe};
use crate::workload::{Case, Vars};

/// Which cases a phase runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Selection {
    All,
    /// Cases that do not create or drop tables.
    Reads,
    /// Cases that do (`writes = true`).
    Writes,
}

impl Selection {
    fn includes(self, case: &Case) -> bool {
        match self {
            Self::All => true,
            Self::Reads => !case.spec.writes,
            Self::Writes => case.spec.writes,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Settings {
    pub(crate) engine: String,
    pub(crate) engine_version: Option<String>,
    pub(crate) scale: String,
    pub(crate) reps: usize,
    pub(crate) warmup: usize,
    pub(crate) timeout: Duration,
    pub(crate) vars: Vars,
    pub(crate) selection: Selection,
    pub(crate) cgroup: Option<Cgroup>,
    pub(crate) spill: SpillProbe,
}

pub(crate) fn log(message: &str) {
    let _ = writeln!(std::io::stderr(), "[codelake-bench] {message}");
}

fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Milliseconds since the epoch of a UTC RFC 3339 timestamp as printed by
/// `docker inspect` (`2026-09-24T01:26:31.123456789Z`).
pub(crate) fn parse_rfc3339_ms(text: &str) -> Option<u64> {
    let text = text.trim().strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let mut date_parts = date.splitn(3, '-').map(str::parse::<i64>);
    let (y, mo, d) = (
        date_parts.next()?.ok()?,
        date_parts.next()?.ok()?,
        date_parts.next()?.ok()?,
    );
    let (clock, frac) = time.split_once('.').unwrap_or((time, "0"));
    let mut clock_parts = clock.splitn(3, ':').map(str::parse::<i64>);
    let (h, mi, s) = (
        clock_parts.next()?.ok()?,
        clock_parts.next()?.ok()?,
        clock_parts.next()?.ok()?,
    );
    let millis: i64 = format!("{frac:0<3}").get(..3)?.parse().ok()?;
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s;
    u64::try_from(secs * 1000 + millis).ok()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// How to reach a network engine.
#[derive(Debug, Clone)]
pub(crate) enum Endpoint {
    Postgres(String),
    Trino(String),
}

/// Polls with `probe_sql` until the engine answers, then returns a connected
/// driver and the time from container start (if known) to ready. The probe
/// should read data: Trino answers `SELECT 1` on the coordinator before any
/// worker has registered.
pub(crate) async fn connect_when_ready(
    endpoint: &Endpoint,
    probe_sql: &str,
    started_at_ms: Option<u64>,
    deadline: Duration,
) -> Result<(Driver, Option<u64>), DriverError> {
    let begin = Instant::now();
    let probe = Duration::from_secs(10);
    loop {
        let attempt = match endpoint {
            Endpoint::Postgres(conninfo) => match PgClient::connect(conninfo).await {
                Ok(pg) => pg.execute(probe_sql, probe).await.map(|_| Driver::Postgres(pg)),
                Err(err) => Err(err),
            },
            Endpoint::Trino(url) => {
                let trino = TrinoClient::new(url, "bench");
                trino
                    .execute(probe_sql, probe)
                    .await
                    .map(|_| Driver::Trino(trino))
            }
        };
        match attempt {
            Ok(driver) => {
                let ready = started_at_ms.map(|start| now_ms().saturating_sub(start));
                return Ok((driver, ready));
            }
            Err(err) if begin.elapsed() > deadline => return Err(err),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

/// How to reach a network engine again after it went away mid-run (the
/// orchestrator restarts a container that died, e.g. killed for exceeding its
/// memory limit). Without it one crash would turn every later case into an
/// error that says nothing about that case.
#[derive(Debug, Clone)]
pub(crate) struct Reconnect {
    pub(crate) endpoint: Endpoint,
    /// Readiness probe, the same one the phase started with.
    pub(crate) probe_sql: String,
    pub(crate) deadline: Duration,
}

/// Appended to the error of the case during which the engine went away.
pub(crate) const ENGINE_LOST: &str =
    "engine connection lost during this case; the next case ran on a restarted engine";

struct Recorder<'a> {
    driver: &'a mut Driver,
    settings: &'a Settings,
}

impl Recorder<'_> {
    async fn untimed(&self, sql: &str) -> Result<(), String> {
        self.driver
            .execute(sql, self.settings.timeout)
            .await
            .map(|_| ())
            .map_err(|e| format!("`{sql}`: {e}"))
    }

    async fn case(&mut self, case: &Case) -> CaseRun {
        let settings = self.settings;
        let reps = case.spec.reps.unwrap_or(settings.reps).max(1);
        let warmup = case.spec.warmup.unwrap_or(settings.warmup);
        let mut run = CaseRun {
            id: case.spec.id.clone(),
            workload: case.workload.clone(),
            driver: self.driver.label().to_owned(),
            status: RunStatus::Ok,
            error: None,
            warmup,
            latencies_us: Vec::with_capacity(reps),
            p50_us: None,
            p95_us: None,
            cpu_usec_p50: None,
            peak_anon_bytes: None,
            spill_peak_bytes: None,
            result: None,
        };
        let outcome = self.measure(case, reps, warmup, &mut run).await;
        for statement in &case.spec.teardown {
            let cleanup = match settings.vars.render(statement, 0) {
                Ok(sql) => self.untimed(&sql).await,
                Err(e) => Err(e.to_string()),
            };
            if let Err(err) = cleanup {
                log(&format!("{}: teardown failed: {err}", case.spec.id));
            }
        }
        if let Err(err) = outcome {
            log(&format!("{}: ERROR {err}", case.spec.id));
            run.status = RunStatus::Error;
            run.error = Some(err);
        }
        run.p50_us = percentile(&run.latencies_us, 50);
        run.p95_us = percentile(&run.latencies_us, 95);
        run
    }

    async fn measure(
        &mut self,
        case: &Case,
        reps: usize,
        warmup: usize,
        run: &mut CaseRun,
    ) -> Result<(), String> {
        let settings = self.settings;
        let render = |template: &str, i: usize| settings.vars.render(template, i).map_err(|e| e.to_string());
        for statement in &case.spec.setup {
            self.untimed(&render(statement, 0)?).await?;
        }
        if case.spec.verify.is_none() {
            self.driver.prepare(&render(&case.sql, warmup)?).await;
        }
        let mut cpu = Vec::with_capacity(reps);
        for i in 0..warmup + reps {
            for statement in &case.spec.before_each {
                self.untimed(&render(statement, i)?).await?;
            }
            let sql = render(&case.sql, i)?;
            let cgroup = settings.cgroup.clone();
            let cpu_before = cgroup.as_ref().and_then(Cgroup::cpu_usec);
            let sampler = Sampler::start(cgroup.clone(), settings.spill.clone());
            let started = Instant::now();
            let result = self.driver.execute(&sql, settings.timeout).await;
            let elapsed = started.elapsed();
            let peaks = sampler.stop();
            let cpu_after = cgroup.as_ref().and_then(Cgroup::cpu_usec);
            let raw = result.map_err(|e| format!("repetition {i}: {e}"))?;
            if i < warmup {
                continue;
            }
            run.latencies_us.push(micros(elapsed));
            if let (Some(before), Some(after)) = (cpu_before, cpu_after) {
                cpu.push(after.saturating_sub(before));
            }
            run.peak_anon_bytes = run.peak_anon_bytes.max(peaks.anon_bytes);
            run.spill_peak_bytes = run.spill_peak_bytes.max(peaks.spill_bytes);
            if i == warmup && case.spec.verify.is_none() {
                run.result = Some(raw.canonicalize().map_err(|e| e.to_string())?);
            }
        }
        run.cpu_usec_p50 = percentile(&cpu, 50);
        if let Some(verify) = &case.spec.verify {
            let sql = render(verify, 0)?;
            self.driver.prepare(&sql).await;
            let raw = self
                .driver
                .execute(&sql, settings.timeout)
                .await
                .map_err(|e| format!("verify: {e}"))?;
            run.result = Some(raw.canonicalize().map_err(|e| e.to_string())?);
        }
        Ok(())
    }
}

impl Recorder<'_> {
    /// After a failed case: if the engine no longer answers, waits for it to
    /// come back and replaces the connection. Returns whether it reconnected.
    async fn recover(&mut self, reconnect: &Reconnect) -> bool {
        let alive = self
            .driver
            .execute(&reconnect.probe_sql, Duration::from_secs(10))
            .await
            .is_ok();
        if alive {
            return false;
        }
        log("engine unreachable; waiting for it to restart");
        match connect_when_ready(
            &reconnect.endpoint,
            &reconnect.probe_sql,
            None,
            reconnect.deadline,
        )
        .await
        {
            Ok((driver, _)) => {
                *self.driver = driver;
                log("engine back; continuing with the next case");
                true
            }
            Err(err) => {
                log(&format!("engine did not come back: {err}"));
                false
            }
        }
    }
}

/// Runs the selected cases on a connected driver. With `reconnect`, a network
/// engine that dies during a case is waited for and reconnected before the
/// next case (the dead case stays an error, marked [`ENGINE_LOST`]).
pub(crate) async fn run_cases(
    mut driver: Driver,
    settings: &Settings,
    cases: &[Case],
    mut process: ProcessMetrics,
    reconnect: Option<&Reconnect>,
) -> EngineRun {
    let label = driver.label().to_owned();
    let selected: Vec<&Case> = cases.iter().filter(|c| settings.selection.includes(c)).collect();
    let mut runs = Vec::with_capacity(selected.len());
    let mut recorder = Recorder {
        driver: &mut driver,
        settings,
    };
    for (n, case) in selected.iter().enumerate() {
        let mut run = recorder.case(case).await;
        if run.status == RunStatus::Error
            && let Some(reconnect) = reconnect
            && recorder.recover(reconnect).await
        {
            let error = run.error.take().unwrap_or_default();
            run.error = Some(format!("{error} ({ENGINE_LOST})"));
        }
        log(&format!(
            "{}/{} {} {}: p50={}us p95={}us",
            n + 1,
            selected.len(),
            settings.engine,
            run.id,
            run.p50_us.map_or_else(|| "-".into(), |v| v.to_string()),
            run.p95_us.map_or_else(|| "-".into(), |v| v.to_string()),
        ));
        runs.push(run);
    }
    process.container_peak_bytes = settings.cgroup.as_ref().and_then(Cgroup::peak_bytes);
    EngineRun {
        schema_version: SCHEMA_VERSION,
        engine: settings.engine.clone(),
        driver: label,
        engine_version: settings.engine_version.clone(),
        scale: settings.scale.clone(),
        reps: settings.reps,
        warmup: settings.warmup,
        process,
        cases: runs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_timestamps_parse() {
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:01.5Z"), Some(1500));
        assert_eq!(
            parse_rfc3339_ms("2026-09-24T01:26:31.123456789Z"),
            Some(1_790_213_191_123)
        );
        assert_eq!(parse_rfc3339_ms("2024-02-29T12:00:00Z"), Some(1_709_208_000_000));
        assert_eq!(parse_rfc3339_ms("not a date"), None);
        assert_eq!(parse_rfc3339_ms("2026-09-24T01:26:31"), None);
    }

    fn settings() -> Settings {
        Settings {
            engine: "trino".into(),
            engine_version: None,
            scale: "tiny".into(),
            reps: 1,
            warmup: 0,
            timeout: Duration::from_secs(1),
            vars: Vars {
                catalog: "hive".into(),
                tpch: "tpch_tiny".into(),
                work: "bench_trino".into(),
            },
            selection: Selection::All,
            cgroup: None,
            spill: SpillProbe::None,
        }
    }

    #[tokio::test]
    async fn an_engine_that_does_not_come_back_is_not_reconnected() {
        // Nothing listens on port 1: every probe fails at once.
        let url = "http://127.0.0.1:1";
        let mut driver = Driver::Trino(TrinoClient::new(url, "bench"));
        let settings = settings();
        let mut recorder = Recorder {
            driver: &mut driver,
            settings: &settings,
        };
        let reconnect = Reconnect {
            endpoint: Endpoint::Trino(url.into()),
            probe_sql: "SELECT 1".into(),
            deadline: Duration::from_millis(200),
        };
        assert!(!recorder.recover(&reconnect).await);
    }

    #[test]
    fn selection_filters_by_writes() {
        let case = |writes| Case {
            workload: "w".into(),
            sql: "SELECT 1".into(),
            spec: toml::from_str(&format!("id = \"x\"\nsql = \"SELECT 1\"\nwrites = {writes}")).unwrap(),
        };
        assert!(Selection::All.includes(&case(true)));
        assert!(Selection::Reads.includes(&case(false)));
        assert!(!Selection::Reads.includes(&case(true)));
        assert!(Selection::Writes.includes(&case(true)));
        assert!(!Selection::Writes.includes(&case(false)));
    }
}
