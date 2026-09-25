//! Server lifecycle: validate → build engine → attach sources → serve → drain.

use std::sync::Arc;
use std::time::Duration;

use codelake_core::{Engine, EngineError, TrinoProtocolConfig, ValidatedConfig};
use datafusion_postgres::ServerOptions;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{hooks, ops, trino};

/// How often abandoned queries are canceled and old ones forgotten.
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);

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
    let mut runtime = tokio::runtime::Builder::new_multi_thread();
    // One worker per partition a query runs: the parallelism the
    // configuration asks for. It also bounds the batches being produced at any
    // instant (before their consumer reserves them), which the memory pool
    // cannot see (ADR 0003).
    if let Some(partitions) = validated.config.execution.target_partitions {
        runtime.worker_threads(partitions);
    }
    runtime
        .enable_all()
        .thread_name("codelake")
        .build()?
        .block_on(serve(validated, session_sampling))
}

async fn serve(validated: ValidatedConfig, session_sampling: Option<f64>) -> Result<(), ServeError> {
    let ValidatedConfig { config, secrets } = validated;
    let prometheus = PrometheusBuilder::new().install_recorder()?;

    std::fs::create_dir_all(&config.memory.spill_dir)?;
    let mut engine = Engine::with_governance(
        &config.memory,
        &config.execution,
        &config.governance,
        Some(config.query_history()),
    )?;
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

    let trino = match &config.server.trino {
        Some(settings) => Some(start_trino(&engine, settings, &shutdown).await?),
        None => None,
    };

    let server = &config.server;
    let tls = server.tls.as_ref();
    let options = ServerOptions::new()
        .with_host(server.pg_listen.ip().to_string())
        .with_port(server.pg_listen.port())
        .with_max_connections(server.max_connections)
        .with_tls_cert_path(tls.map(|t| t.cert_path.display().to_string()))
        .with_tls_key_path(tls.map(|t| t.key_path.display().to_string()));
    let session = Arc::new(engine.context().clone());
    spawn_sweeper(&engine, trino.clone(), &shutdown);

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
    if let Some(trino) = &trino {
        trino.start_draining();
    }
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

/// Starts the HTTP statement protocol listener; its catalog `system` adds the
/// protocol's metadata tables to the engine's `runtime.queries`.
async fn start_trino(
    engine: &Arc<Engine>,
    settings: &TrinoProtocolConfig,
    shutdown: &CancellationToken,
) -> Result<Arc<trino::TrinoState>, ServeError> {
    let limits = trino::Limits {
        max_active: settings.max_active_queries,
        client_timeout: Duration::from_secs(settings.client_timeout_secs),
        max_query_length: settings.max_query_length,
    };
    let state = trino::TrinoState::new(Arc::clone(engine), limits, settings.listen);
    engine
        .context()
        .register_catalog("system", state.system_catalog().map_err(EngineError::from)?);
    let listener = TcpListener::bind(settings.listen).await?;
    info!(addr = %settings.listen, "Trino protocol listening");
    tokio::spawn(
        axum::serve(listener, trino::router(Arc::clone(&state)))
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future(),
    );
    Ok(state)
}

/// Periodically forgets old completed queries, and cancels the HTTP
/// protocol's abandoned ones.
fn spawn_sweeper(engine: &Arc<Engine>, http: Option<Arc<trino::TrinoState>>, shutdown: &CancellationToken) {
    let engine = Arc::clone(engine);
    let stop = shutdown.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = tick.tick() => {
                    if let Some(http) = &http {
                        http.sweep();
                    }
                    engine.governance().sweep();
                }
            }
        }
    });
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
