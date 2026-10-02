//! Server lifecycle: validate → build engine → attach sources → serve → drain.

use std::sync::Arc;
use std::time::Duration;

use codelake_core::{Engine, EngineError, TrinoProtocolConfig, ValidatedConfig};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::net::listener::{GuardedListener, OnExhausted};
use crate::net::{http, limits};
use crate::{hooks, ops, pg, trino};

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
    #[error("connection limits: {0}")]
    Limits(String),
    #[cfg(not(feature = "distributed"))]
    #[error("[distributed] is configured but this build has no `distributed` feature")]
    NotDistributed,
    #[cfg(feature = "distributed")]
    #[error("distributed: {0}")]
    Distributed(String),
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
    // First: source pools, spill and data files open descriptors too.
    let (budget, timeouts) = connection_limits(&config.server)?;

    std::fs::create_dir_all(&config.memory.spill_dir)?;
    let mut engine = Engine::with_governance(
        &config.memory,
        &config.execution,
        &config.governance,
        Some(config.query_history()),
    )?;
    let timeout = Duration::from_secs(config.execution.source_startup_timeout_secs);
    let worker = config
        .distributed
        .as_ref()
        .is_some_and(|d| d.role == codelake_core::config::DistributedRole::Worker);
    for source in &config.sources {
        // A worker only reads the files of the stages it runs: federated
        // sources stay on the coordinator (no pools or credentials here).
        if worker && !matches!(source.kind, codelake_core::SourceKind::Hive { .. }) {
            continue;
        }
        let connector = codelake_connectors::build(source, &secrets)?;
        engine.attach(connector, source.required, timeout).await?;
    }
    if let Some(distributed) = &config.distributed {
        #[cfg(feature = "distributed")]
        {
            if worker {
                return serve_worker(engine, distributed.listen, &config, prometheus, timeouts).await;
            }
            coordinate(&mut engine, distributed, &config)?;
        }
        #[cfg(not(feature = "distributed"))]
        {
            let _ = distributed;
            return Err(ServeError::NotDistributed);
        }
    }
    let engine = Arc::new(engine);
    info!(memory_limit = %config.memory.limit, sources = config.sources.len(), "engine ready");

    let state = Arc::new(ops::OpsState::new(Arc::clone(&engine), prometheus));
    let shutdown = CancellationToken::new();
    let http = tokio::spawn(
        start_ops(
            config.server.http_listen,
            ops::router(Arc::clone(&state)),
            timeouts,
            shutdown.clone(),
        )
        .await?,
    );

    let trino = match (&config.server.trino, budget.trino) {
        (Some(settings), Some(max)) => Some(start_trino(&engine, settings, max, timeouts, &shutdown).await?),
        _ => None,
    };

    let server = &config.server;
    let tls = server
        .tls
        .as_ref()
        .map(|t| crate::net::pg::tls_acceptor(&t.cert_path, &t.key_path))
        .transpose()?;
    let pg_listener = GuardedListener::new(
        TcpListener::bind(server.pg_listen).await?,
        "pg",
        budget.pg,
        OnExhausted::Refuse,
        crate::net::pg::refusal,
    );
    info!(addr = %server.pg_listen, tls = tls.is_some(), "PostgreSQL listening");
    let session = Arc::new(engine.context().clone());
    let handlers = Arc::new(pg::Handlers::new(
        Arc::clone(&engine),
        session,
        hooks::all(session_sampling),
    ));
    spawn_sweeper(&engine, trino.clone(), &shutdown);

    tokio::select! {
        () = crate::net::pg::serve(pg_listener, handlers, tls) => {
            shutdown.cancel();
            return Err(ServeError::PgListener("exited".to_owned()));
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
    if let Err(err) = http.await {
        warn!(error = %err, "ops HTTP server ended with error");
    }
    info!("shutdown complete");
    Ok(())
}

/// Raises the open-files limit and shares it between the listeners
/// (ADR 0018).
fn connection_limits(
    server: &codelake_core::config::ServerConfig,
) -> Result<(limits::Budget, http::Timeouts), ServeError> {
    let open_files = limits::raise_open_files(server.open_files_limit);
    let budget = limits::budget(
        open_files.soft,
        server.reserved_files,
        server.max_connections,
        limits::TrinoShare::of(server.trino.as_ref()),
    )
    .map_err(ServeError::Limits)?;
    info!(
        ops = budget.ops,
        pg = budget.pg,
        trino = ?budget.trino,
        reserved = server.reserved_files,
        "connection limits"
    );
    let timeouts = http::Timeouts {
        idle: Duration::from_secs(server.http_idle_timeout_secs),
        header_read: Duration::from_secs(server.http_header_read_timeout_secs),
    };
    Ok((budget, timeouts))
}

/// Makes `engine` the coordinator of `distributed.workers`. A connection to a
/// worker buffers at most 1/16 of the memory limit (the library's default is
/// a fixed 64 MiB per connection).
#[cfg(feature = "distributed")]
fn coordinate(
    engine: &mut Engine,
    distributed: &codelake_core::config::DistributedConfig,
    config: &codelake_core::Config,
) -> Result<(), ServeError> {
    use datafusion_distributed::DistributedExt as _;
    let urls = distributed
        .workers
        .iter()
        .map(|url| url::Url::parse(url).map_err(|e| ServeError::Distributed(format!("{url}: {e}"))))
        .collect::<Result<Vec<_>, _>>()?;
    let budget = (config.memory.limit.as_usize() / 16).max(8 << 20);
    codelake_server::distributed::coordinate(
        engine,
        codelake_server::distributed::StaticWorkers(urls),
        |builder| builder.with_distributed_worker_connection_buffer_budget_bytes(budget),
    )
    .map_err(|e| ServeError::Distributed(e.to_string()))?;
    info!(workers = distributed.workers.len(), "distributed coordinator");
    Ok(())
}

/// A worker: its execution service and the operational HTTP listener
/// (probes, metrics), until SIGTERM. No client listeners.
#[cfg(feature = "distributed")]
async fn serve_worker(
    engine: Engine,
    listen: std::net::SocketAddr,
    config: &codelake_core::Config,
    prometheus: metrics_exporter_prometheus::PrometheusHandle,
    timeouts: http::Timeouts,
) -> Result<(), ServeError> {
    let engine = Arc::new(engine);
    let state = Arc::new(ops::OpsState::new(Arc::clone(&engine), prometheus));
    let shutdown = CancellationToken::new();
    let http = tokio::spawn(
        start_ops(
            config.server.http_listen,
            ops::router(Arc::clone(&state)),
            timeouts,
            shutdown.clone(),
        )
        .await?,
    );
    info!(addr = %listen, "distributed worker listening");
    let stop = shutdown.clone();
    let served = codelake_server::distributed::serve_worker(&engine, listen, async move {
        tokio::select! {
            () = shutdown_signal() => {}
            () = stop.cancelled() => {}
        }
    })
    .await;
    state.start_draining();
    shutdown.cancel();
    if let Err(err) = http.await {
        warn!(error = %err, "ops HTTP server ended with error");
    }
    served.map_err(|e| ServeError::Distributed(e.to_string()))
}

/// Binds the ops listener (probes, `/metrics`) and returns its serving loop.
/// On descriptor exhaustion it serves the client it accepts on its spare
/// descriptor: a probe gets its real answer (ADR 0018).
async fn start_ops(
    addr: std::net::SocketAddr,
    router: axum::Router,
    timeouts: http::Timeouts,
    shutdown: CancellationToken,
) -> Result<impl std::future::Future<Output = ()> + Send + 'static, ServeError> {
    let listener = GuardedListener::new(
        TcpListener::bind(addr).await?,
        "ops",
        limits::OPS_MAX_CONNECTIONS,
        OnExhausted::Serve,
        http::refusal,
    );
    info!(%addr, "ops HTTP listening");
    Ok(http::serve(listener, router, timeouts, shutdown))
}

/// Starts the HTTP statement protocol listener; its catalog `system` adds the
/// protocol's metadata tables to the engine's `runtime.queries`.
async fn start_trino(
    engine: &Arc<Engine>,
    settings: &TrinoProtocolConfig,
    max_connections: usize,
    timeouts: http::Timeouts,
    shutdown: &CancellationToken,
) -> Result<Arc<trino::TrinoState>, ServeError> {
    let limits = trino::Limits {
        max_active: settings.max_active_queries,
        client_timeout: Duration::from_secs(settings.client_timeout_secs),
        max_query_length: settings.max_query_length,
    };
    let state = trino::TrinoState::new(
        Arc::clone(engine),
        limits,
        settings.listen,
        &settings.header_prefix,
        settings
            .reported_version
            .as_deref()
            .unwrap_or(env!("CARGO_PKG_VERSION")),
    );
    engine
        .context()
        .register_catalog("system", state.system_catalog().map_err(EngineError::from)?);
    let listener = GuardedListener::new(
        TcpListener::bind(settings.listen).await?,
        "trino",
        max_connections,
        OnExhausted::Refuse,
        http::refusal,
    );
    info!(addr = %settings.listen, max_connections, "Trino protocol listening");
    tokio::spawn(http::serve(
        listener,
        trino::router(Arc::clone(&state)),
        timeouts,
        shutdown.clone(),
    ));
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
