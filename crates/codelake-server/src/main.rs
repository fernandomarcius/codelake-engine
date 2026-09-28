//! `codelake` binary.
//!
//! ```text
//! codelake check-config --config codelake.toml   # validate and exit (CI / deploy gate)
//! codelake serve --config codelake.toml          # run the server
//! codelake config import --from catalog/ --out codelake.toml --env-out codelake.env
//! codelake hive migrate-names --config codelake.toml --source hive --from-metadata-file … [--apply]
//! ```

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use codelake_server::telemetry;

/// ADR 0003: the memory ceiling is a limit on the process, so memory the
/// engine frees must go back to the OS. glibc's allocator keeps freed memory in
/// one arena per thread; after a spilling sort or aggregation the process
/// stayed hundreds of MiB above what the pool held. jemalloc returns freed
/// pages within a second (`JEMALLOC_SYS_WITH_MALLOC_CONF`, `.cargo/config.toml`),
/// and at once while the pool is short of memory ([`follow_memory_pressure`]).
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Decay of freed pages while memory is plentiful: jemalloc keeps them for up
/// to a second, so a query that frees and allocates again reuses them instead
/// of faulting them back in (ADR 0003).
const LAZY_DECAY_MS: isize = 1000;

/// Follows the memory pool's pressure (`codelake_core::pool::on_memory_pressure`):
/// while memory is scarce every freed page goes back to the system at once
/// (decay 0, which also purges what each arena keeps), so the process stays
/// within its ceiling; otherwise pages decay over [`LAZY_DECAY_MS`].
fn follow_memory_pressure(scarce: bool) {
    use tikv_jemalloc_ctl::{Access as _, AsName as _};
    let decay = if scarce { 0 } else { LAZY_DECAY_MS };
    // Arenas created later start with this value.
    let _ = b"arenas.dirty_decay_ms\0".name().write(decay);
    let Ok(count) = tikv_jemalloc_ctl::arenas::narenas::read() else {
        return;
    };
    let Ok(mut mib) = b"arena.0.dirty_decay_ms\0".name().mib::<[usize; 3]>() else {
        return;
    };
    for arena in 0..count {
        mib[1] = arena as usize;
        let _ = mib.write(decay);
    }
}

mod hooks;
mod import;
mod ops;
mod pg;
mod serve;
mod trino;

#[derive(Debug, Parser)]
#[command(
    name = "codelake",
    version,
    about = "Federated SQL engine with a hard memory ceiling"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validate configuration and secrets, print every problem, then exit.
    CheckConfig {
        #[arg(long, env = "CODELAKE_CONFIG")]
        config: PathBuf,
    },
    /// Run the server.
    Serve {
        #[arg(long, env = "CODELAKE_CONFIG")]
        config: PathBuf,
    },
    /// Configuration tools.
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Hive warehouse tools.
    #[command(subcommand)]
    Hive(HiveCommand),
}

#[derive(Debug, Subcommand)]
enum HiveCommand {
    /// Rename a local warehouse's metastore files and markers from the names
    /// another engine wrote to the source's `metastore_names` (ISS-011).
    /// Prints the plan; changes files only with `--apply`.
    MigrateNames(MigrateNamesArgs),
}

#[derive(Debug, clap::Args)]
struct MigrateNamesArgs {
    #[arg(long, env = "CODELAKE_CONFIG")]
    config: PathBuf,
    /// The hive source whose warehouse is migrated (its `metastore_names`
    /// are the new names).
    #[arg(long)]
    source: String,
    /// Names the warehouse has now (the engine that wrote it).
    #[arg(long)]
    from_metadata_file: String,
    #[arg(long)]
    from_permissions_dir: String,
    #[arg(long)]
    from_view_marker: String,
    #[arg(long)]
    from_view_parameter: String,
    #[arg(long)]
    from_version_parameter: String,
    /// Change the files (without it, only the plan is printed).
    #[arg(long)]
    apply: bool,
}

fn migrate_names(args: &MigrateNamesArgs) -> ExitCode {
    let mut err = std::io::stderr();
    let loaded = match codelake_core::Config::load(&args.config) {
        Ok(validated) => validated,
        Err(e) => {
            let _ = writeln!(err, "codelake hive migrate-names: {e}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let Some((warehouse, names)) = loaded
        .config
        .sources
        .iter()
        .find(|s| s.name == args.source)
        .and_then(|s| match &s.kind {
            codelake_core::config::SourceKind::Hive {
                warehouse,
                metastore_names,
                ..
            } => Some((warehouse.clone(), metastore_names.clone())),
            _ => None,
        })
    else {
        let _ = writeln!(
            err,
            "codelake hive migrate-names: no hive source `{}`",
            args.source
        );
        return ExitCode::from(EXIT_CONFIG);
    };
    let Some(root) = warehouse
        .strip_prefix("file://")
        .or(Some(warehouse.as_str()))
        .filter(|w| w.starts_with('/'))
    else {
        let _ = writeln!(
            err,
            "codelake hive migrate-names: only local warehouses are migrated, not `{warehouse}`"
        );
        return ExitCode::from(EXIT_CONFIG);
    };
    let old = codelake_core::config::MetastoreNames {
        metadata_file: args.from_metadata_file.clone(),
        permissions_dir: args.from_permissions_dir.clone(),
        view_marker: args.from_view_marker.clone(),
        view_parameter: args.from_view_parameter.clone(),
        version_parameter: args.from_version_parameter.clone(),
        writer_version: names.writer_version.clone(),
    };
    if let Some(problem) = old.problems().first() {
        let _ = writeln!(err, "codelake hive migrate-names: old names: {problem}");
        return ExitCode::from(EXIT_CONFIG);
    }
    match codelake_connectors::hive::migrate::migrate_names(
        std::path::Path::new(root),
        &old,
        &names,
        args.apply,
    ) {
        Ok(steps) => {
            let mut out = std::io::stdout().lock();
            for step in &steps {
                let _ = match step {
                    codelake_connectors::hive::migrate::Step::Rewrite { from, to } => {
                        writeln!(out, "rewrite {} -> {}", from.display(), to.display())
                    }
                    codelake_connectors::hive::migrate::Step::Rename { from, to } => {
                        writeln!(out, "rename  {} -> {}", from.display(), to.display())
                    }
                };
            }
            let verb = if args.apply {
                "applied"
            } else {
                "planned (dry run: add --apply)"
            };
            let _ = writeln!(out, "{} change(s) {verb}", steps.len());
            ExitCode::SUCCESS
        }
        Err(e) => {
            let _ = writeln!(err, "codelake hive migrate-names: {e}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Generate `[[source]]` entries from a directory of catalog-properties
    /// files (`connector.name=...`). Secrets go to an env file (mode 0600),
    /// never to the TOML file or the terminal.
    Import(import::ImportArgs),
}

/// Exit code for configuration errors, distinct from runtime failures (1).
const EXIT_CONFIG: u8 = 2;
/// `config import` wrote its files but skipped at least one catalog.
const EXIT_PARTIAL: u8 = 3;

fn config_import(args: &import::ImportArgs) -> ExitCode {
    match import::run(args, &mut std::io::stdout().lock()) {
        Ok(outcome) if outcome.skipped == 0 => ExitCode::SUCCESS,
        Ok(_) => ExitCode::from(EXIT_PARTIAL),
        Err(err) => {
            let _ = writeln!(std::io::stderr(), "codelake config import: {err}");
            ExitCode::from(EXIT_CONFIG)
        }
    }
}

fn main() -> ExitCode {
    codelake_core::pool::on_memory_pressure(follow_memory_pressure);
    let cli = Cli::parse();
    let config = match &cli.command {
        Command::CheckConfig { config } | Command::Serve { config } => config,
        Command::Config(ConfigCommand::Import(args)) => return config_import(args),
        Command::Hive(HiveCommand::MigrateNames(args)) => return migrate_names(args),
    };
    // Load first: the exporter is configured by the file. Errors are logged
    // once the subscriber exists.
    let loaded = codelake_core::Config::load(config);
    let exporter = match (&cli.command, &loaded) {
        (Command::Serve { .. }, Ok(validated)) => Some(&validated.config.telemetry),
        _ => None,
    };
    let telemetry = match telemetry::init(exporter) {
        Ok(telemetry) => telemetry,
        Err(err) => {
            // No subscriber could be installed: stderr is all there is.
            let _ = writeln!(std::io::stderr(), "cannot start telemetry: {err}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let validated = match loaded {
        Ok(validated) => validated,
        Err(err) => {
            tracing::error!(config = %config.display(), "{err}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    let code = match cli.command {
        // Handled before the configuration is loaded.
        Command::Config(_) | Command::Hive(_) => ExitCode::SUCCESS,
        Command::CheckConfig { .. } => {
            tracing::info!(sources = validated.config.sources.len(), "configuration is valid");
            ExitCode::SUCCESS
        }
        Command::Serve { .. } => match serve::run(validated, telemetry.sampling_ratio()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                tracing::error!("{err}");
                ExitCode::FAILURE
            }
        },
    };
    telemetry.shutdown();
    code
}
