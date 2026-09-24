//! Server lifecycle: validate → build engine → attach sources → serve → drain.

use std::sync::Arc;
use std::time::Duration;

use codelake_core::{Engine, EngineError, ValidatedConfig};
use datafusion_postgres::ServerOptions;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{hooks, ops};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ServeError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Connector(#[from] codelake_core::ConnectorError),
    #[error("metrics: {0}")]
    Metrics(#[from] metrics_exporter_prometheus::BuildError),
    #[error("PostgreSQL listener stopped: {0}")]
    PgListener(String),
}

/// `session_sampling`: share of PostgreSQL sessions traced (see
/// [`hooks::TracingHook`]); `None` when traces are not exported.
pub(crate) fn run(validated: ValidatedConfig, session_sampling: Option<f64>) -> Result<(), ServeError> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("codelake")
        .build()?
        .block_on(serve(validated, session_sampling))
}

async fn serve(validated: ValidatedConfig, session_sampling: Option<f64>) -> Result<(), ServeError> {
    let ValidatedConfig { config, secrets } = validated;
    let prometheus = PrometheusBuilder::new().install_recorder()?;

    std::fs::create_dir_all(&config.memory.spill_dir)?;
    let mut engine = Engine::new(&config.memory, &config.execution)?;
    let timeout = Duration::from_secs(config.execution.source_startup_timeout_secs);
    for source in &config.sources {
        let connector = codelake_connectors::build(source, &secrets)?;
        engine.attach(connector, source.required, timeout).await?;
    }
    let engine = Arc::new(engine);
    info!(memory_limit = %config.memory.limit, sources = config.sources.len(), "engine ready");

    let state = Arc::new(ops::OpsState::new(Arc::clone(&engine), prometheus));
    let shutdown = CancellationToken::new();
    let http_listener = TcpListener::bind(config.server.http_listen).await?;
    info!(addr = %config.server.http_listen, "ops HTTP listening");
    let http = tokio::spawn(
        axum::serve(http_listener, ops::router(Arc::clone(&state)))
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future(),
    );

    let server = &config.server;
    let tls = server.tls.as_ref();
    let options = ServerOptions::new()
        .with_host(server.pg_listen.ip().to_string())
        .with_port(server.pg_listen.port())
        .with_max_connections(server.max_connections)
        .with_tls_cert_path(tls.map(|t| t.cert_path.display().to_string()))
        .with_tls_key_path(tls.map(|t| t.key_path.display().to_string()));
    let session = Arc::new(engine.context().clone());

    tokio::select! {
        result = datafusion_postgres::serve_with_hooks(session, &options, hooks::all(session_sampling)) => {
            shutdown.cancel();
            let reason = result.err().map_or_else(|| "exited".to_owned(), |e| e.to_string());
            return Err(ServeError::PgListener(reason));
        }
        () = shutdown_signal() => {}
    }

    // Stop accepting new PostgreSQL connections (the accept loop was dropped
    // above), fail readiness so the load balancer drains us, and give in-flight
    // queries the grace period to finish.
    state.start_draining();
    let grace = Duration::from_secs(server.shutdown_grace_secs);
    warn!(?grace, "shutdown requested, draining");
    tokio::time::sleep(grace).await;
    shutdown.cancel();
    if let Ok(Err(err)) = http.await {
        warn!(error = %err, "ops HTTP server ended with error");
    }
    info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!(error = %err, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                warn!(error = %err, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
