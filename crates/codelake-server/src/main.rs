//! `codelake` binary.
//!
//! ```text
//! codelake check-config --config codelake.toml   # validate and exit (CI / deploy gate)
//! codelake serve --config codelake.toml          # run the server
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
}

/// Exit code for configuration errors, distinct from runtime failures (1).
const EXIT_CONFIG: u8 = 2;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let (Command::CheckConfig { config } | Command::Serve { config }) = &cli.command;
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
