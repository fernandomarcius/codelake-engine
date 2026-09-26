//! `codelake` binary.
//!
//! ```text
//! codelake check-config --config codelake.toml   # validate and exit (CI / deploy gate)
//! codelake serve --config codelake.toml          # run the server
//! codelake config import --from catalog/ --out codelake.toml --env-out codelake.env
//! ```

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use codelake_server::telemetry;

/// ADR 0003: the memory ceiling is a limit on the process, so memory the
/// engine frees must go back to the OS. glibc's allocator keeps freed memory in
/// one arena per thread; after a spilling sort or aggregation the process
/// stayed hundreds of MiB above what the pool held. jemalloc is built to
/// purge freed pages at once (`JEMALLOC_SYS_WITH_MALLOC_CONF`, `.cargo/config.toml`).
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
    let cli = Cli::parse();
    let config = match &cli.command {
        Command::CheckConfig { config } | Command::Serve { config } => config,
        Command::Config(ConfigCommand::Import(args)) => return config_import(args),
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
        Command::Config(_) => ExitCode::SUCCESS,
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
