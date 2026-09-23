//! Structured JSON logs to stdout. `RUST_LOG` controls verbosity
//! (default `info`). Traces over OTLP are tracked in ROADMAP.md (M1).

use tracing_subscriber::EnvFilter;

pub(crate) fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(true)
        .with_target(true)
        .init();
}
