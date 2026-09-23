//! `codelake` binary.
//!
//! ```text
//! codelake check-config --config codelake.toml   # validate and exit (CI / deploy gate)
//! codelake serve --config codelake.toml          # run the server
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod hooks;
mod ops;
mod serve;
mod telemetry;

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
    telemetry::init();
    let (Command::CheckConfig { config } | Command::Serve { config }) = &cli.command;
    let validated = match codelake_core::Config::load(config) {
        Ok(validated) => validated,
        Err(err) => {
            tracing::error!(config = %config.display(), "{err}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    match cli.command {
        Command::CheckConfig { .. } => {
            tracing::info!(sources = validated.config.sources.len(), "configuration is valid");
            ExitCode::SUCCESS
        }
        Command::Serve { .. } => match serve::run(validated) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                tracing::error!("{err}");
                ExitCode::FAILURE
            }
        },
    }
}
