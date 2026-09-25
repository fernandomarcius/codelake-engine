//! `codelake-bench`: the same workload against `CodeLake` and Trino 431, with a
//! correctness eval before any timing. Orchestrated by `scripts/bench/run.sh`;
//! see `docs/benchmarks.md`.
//!
//! ```text
//! codelake-bench generate --trino http://trino:8080 --scale 1 --out dataset.json
//! codelake-bench run --engine trino --driver trino --url http://trino:8080 ... --out trino.json
//! codelake-bench run --engine codelake --driver pg --url "host=codelake ..." ... --out codelake-pg.json
//! codelake-bench run --engine codelake --driver embedded --config codelake.toml ... --out codelake-embedded.json
//! codelake-bench compare --trino trino.json --codelake codelake-pg.json --codelake codelake-embedded.json \
//!     --env env.json --commit abc1234 --date 2026-09-24 --baseline-out b.json --report-out r.md
//! ```

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};

mod canonical;
mod compare;
mod driver;
mod generate;
mod model;
mod probe;
mod report;
mod run;
mod workload;

use driver::{Driver, EmbeddedClient, TrinoClient};
use model::{EngineRun, ProcessMetrics};
use probe::{Cgroup, SpillProbe};
use run::{Endpoint, Reconnect, Selection, Settings, log};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DriverKind {
    /// PostgreSQL wire protocol (`CodeLake` today).
    Pg,
    /// Trino client REST protocol (Trino; `CodeLake` once it serves it).
    Trino,
    /// `CodeLake` engine inside this process (writes only; not like-for-like).
    Embedded,
}

#[derive(Debug, Parser)]
#[command(
    name = "codelake-bench",
    about = "Benchmark and correctness eval: CodeLake vs Trino"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Args)]
struct RunArgs {
    /// Engine label: `codelake` or `trino` (also names the scratch schema).
    #[arg(long)]
    engine: String,
    #[arg(long, value_enum)]
    driver: DriverKind,
    /// PostgreSQL conninfo or Trino base URL.
    #[arg(long)]
    url: Option<String>,
    /// `CodeLake` configuration (embedded driver).
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value = "bench/workloads")]
    workloads_dir: PathBuf,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "tpch,microbatch,metadata,pipeline"
    )]
    workloads: Vec<String>,
    #[arg(long, value_enum, default_value = "all")]
    cases: Selection,
    #[arg(long, default_value = "hive")]
    catalog: String,
    /// TPC-H scale: `tiny`, `1`, `10`, `100`, ...
    #[arg(long, default_value = "1")]
    scale: String,
    #[arg(long, default_value_t = 5)]
    reps: usize,
    #[arg(long, default_value_t = 1)]
    warmup: usize,
    /// Per-case repetitions that win over the workload files:
    /// `<case id>=<reps>[/<warmup>]`, comma separated.
    #[arg(long, value_delimiter = ',')]
    case_reps: Vec<String>,
    #[arg(long, default_value_t = 1800)]
    timeout_secs: u64,
    /// Mounted cgroup v2 root of the Docker host, with `--container-id`.
    #[arg(long)]
    cgroup_root: Option<PathBuf>,
    #[arg(long)]
    container_id: Option<String>,
    /// Measure this process's own cgroup (`/sys/fs/cgroup`), for embedded runs.
    #[arg(long)]
    cgroup_self: bool,
    /// Readiness probe; should read data (default: the TPC-H `region` table).
    #[arg(long)]
    ready_sql: Option<String>,
    /// Container start time (`docker inspect .State.StartedAt`), for time to ready.
    #[arg(long)]
    started_at: Option<String>,
    /// Seconds to wait after ready before sampling idle memory.
    #[arg(long, default_value_t = 5)]
    idle_secs: u64,
    /// `CodeLake` ops listener (`host:port`) for the spill gauge.
    #[arg(long)]
    metrics: Option<String>,
    #[arg(long)]
    engine_version: Option<String>,
    #[arg(long)]
    out: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create the TPC-H tables in the warehouse through Trino (idempotent).
    Generate {
        #[arg(long)]
        trino: String,
        #[arg(long, default_value = "hive")]
        catalog: String,
        #[arg(long, default_value = "1")]
        scale: String,
        #[arg(long, default_value_t = 7200)]
        timeout_secs: u64,
        #[arg(long)]
        out: PathBuf,
    },
    /// Run the workloads against one engine.
    Run(Box<RunArgs>),
    /// Eval + baseline JSON + markdown report.
    Compare {
        #[arg(long)]
        trino: PathBuf,
        #[arg(long, required = true)]
        codelake: Vec<PathBuf>,
        /// JSON object with facts about the machine (`cpus`, `memory_mib`, ...).
        #[arg(long)]
        env: PathBuf,
        #[arg(long)]
        commit: String,
        #[arg(long)]
        date: String,
        #[arg(long)]
        baseline_out: PathBuf,
        #[arg(long)]
        report_out: PathBuf,
    },
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, BoxError> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?)
}

fn write_file(path: &Path, content: &str) -> Result<(), BoxError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(())
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), BoxError> {
    let mut text = serde_json::to_string_pretty(value)?;
    text.push('\n');
    write_file(path, &text)
}

fn cgroup(args: &RunArgs) -> Result<Option<Cgroup>, BoxError> {
    if args.cgroup_self {
        return Ok(Some(Cgroup::at(PathBuf::from("/sys/fs/cgroup"))));
    }
    match (&args.cgroup_root, &args.container_id) {
        (Some(root), Some(id)) => Cgroup::find(root, id)
            .map(Some)
            .ok_or_else(|| format!("no cgroup for container {id} under {}", root.display()).into()),
        (None, None) => Ok(None),
        _ => Err("--cgroup-root and --container-id go together".into()),
    }
}

async fn run(args: RunArgs) -> Result<(), BoxError> {
    let mut cases = workload::load(&args.workloads_dir, &args.workloads)?;
    workload::apply_overrides(&mut cases, &args.case_reps)?;
    let cgroup = cgroup(&args)?;
    let timeout = Duration::from_secs(args.timeout_secs);
    let (_, tpch) = workload::tpch_schemas(&args.scale);
    let started_at_ms = args.started_at.as_deref().and_then(run::parse_rfc3339_ms);
    let url = || args.url.clone().ok_or("--url is required for this driver");
    let (driver, ready_ms, spill, reconnect) = match args.driver {
        DriverKind::Pg | DriverKind::Trino => {
            let endpoint = if args.driver == DriverKind::Pg {
                Endpoint::Postgres(url()?)
            } else {
                Endpoint::Trino(url()?)
            };
            let probe = args
                .ready_sql
                .clone()
                .unwrap_or_else(|| format!("SELECT count(*) FROM {}.{tpch}.region", args.catalog));
            let deadline = Duration::from_secs(600);
            let (driver, ready) = run::connect_when_ready(&endpoint, &probe, started_at_ms, deadline).await?;
            let spill = args.metrics.clone().map_or(SpillProbe::None, SpillProbe::Metrics);
            let reconnect = Reconnect {
                endpoint,
                probe_sql: probe,
                deadline,
            };
            (driver, ready, spill, Some(reconnect))
        }
        DriverKind::Embedded => {
            let config = args
                .config
                .as_deref()
                .ok_or("--config is required for the embedded driver")?;
            let embedded = EmbeddedClient::start(config).await?;
            let spill = SpillProbe::Engine(std::sync::Arc::clone(&embedded.engine));
            (Driver::Embedded(embedded), None, spill, None)
        }
    };
    log(&format!(
        "{} ({}) ready{}",
        args.engine,
        driver.label(),
        ready_ms.map_or_else(String::new, |ms| format!(" in {ms} ms"))
    ));
    let idle_anon_bytes = if cgroup.is_some() && ready_ms.is_some() {
        tokio::time::sleep(Duration::from_secs(args.idle_secs)).await;
        cgroup.as_ref().and_then(Cgroup::anon_bytes)
    } else {
        None
    };
    let settings = Settings {
        engine: args.engine.clone(),
        engine_version: args.engine_version.clone(),
        scale: args.scale.clone(),
        reps: args.reps,
        warmup: args.warmup,
        timeout,
        vars: workload::Vars {
            catalog: args.catalog.clone(),
            tpch,
            work: format!("bench_{}", args.engine),
        },
        selection: args.cases,
        cgroup,
        spill,
    };
    let process = ProcessMetrics {
        ready_ms,
        idle_anon_bytes,
        container_peak_bytes: None,
    };
    let result: EngineRun = run::run_cases(driver, &settings, &cases, process, reconnect.as_ref()).await;
    write_json(&args.out, &result)
}

async fn main_async(cli: Cli) -> Result<(), BoxError> {
    match cli.command {
        Command::Generate {
            trino,
            catalog,
            scale,
            timeout_secs,
            out,
        } => {
            let (source, target) = workload::tpch_schemas(&scale);
            // Needs a worker: the generator connector is scanned by workers.
            let probe = format!("SELECT count(*) FROM tpch.{source}.region");
            let (driver, _) = run::connect_when_ready(
                &Endpoint::Trino(trino.clone()),
                &probe,
                None,
                Duration::from_secs(600),
            )
            .await?;
            drop(driver);
            let client = TrinoClient::new(&trino, "bench");
            let counts = generate::generate(
                &client,
                &catalog,
                &source,
                &target,
                Duration::from_secs(timeout_secs),
            )
            .await?;
            log(&format!("{catalog}.{target} ready: {counts:?}"));
            write_json(
                &out,
                &serde_json::json!({ "scale": scale, "schema": target, "rows": counts }),
            )
        }
        Command::Run(args) => run(*args).await,
        Command::Compare {
            trino,
            codelake,
            env,
            commit,
            date,
            baseline_out,
            report_out,
        } => {
            let trino: EngineRun = read_json(&trino)?;
            let codelake = codelake
                .iter()
                .map(|p| read_json(p))
                .collect::<Result<Vec<EngineRun>, _>>()?;
            let env: serde_json::Map<String, serde_json::Value> = read_json(&env)?;
            let baseline = compare::merge(&trino, &codelake, env, &commit, &date);
            write_json(&baseline_out, &baseline)?;
            write_file(&report_out, &report::render(&baseline))?;
            let failed = baseline
                .cases
                .iter()
                .filter(|c| c.correctness.status != model::Verdict::Ok)
                .count();
            log(&format!(
                "eval: {} of {} cases match Trino; report {}",
                baseline.cases.len() - failed,
                baseline.cases.len(),
                report_out.display()
            ));
            Ok(())
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(err) => {
            log(&format!("cannot start runtime: {err}"));
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(main_async(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            log(&format!("error: {err}"));
            ExitCode::FAILURE
        }
    }
}
