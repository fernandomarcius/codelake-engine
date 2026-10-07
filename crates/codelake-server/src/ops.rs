//! Operational HTTP endpoints. The three probes answer different questions and
//! must never share a handler:
//!
//! | endpoint  | question                          | on failure                     |
//! |-----------|-----------------------------------|--------------------------------|
//! | `/livez`  | is the process responsive?        | Kubernetes restarts the pod    |
//! | `/readyz` | should this pod receive traffic?  | pod leaves the load balancer   |
//! | `/healthz`| full picture for a human          | nothing (informational)        |
//!
//! Liveness never touches a source: a database outage must not restart the fleet.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use codelake_core::{Engine, SourceStatus};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tokio::sync::Mutex;

/// Source checks are cached so probes and scrapes do not hammer the sources.
const SOURCE_CHECK_TTL: Duration = Duration::from_secs(5);
const SOURCE_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct OpsState {
    engine: Arc<Engine>,
    prometheus: PrometheusHandle,
    draining: AtomicBool,
    cache: Mutex<Option<(Instant, Vec<SourceStatus>)>>,
}

impl OpsState {
    pub(crate) fn new(engine: Arc<Engine>, prometheus: PrometheusHandle) -> Self {
        Self {
            engine,
            prometheus,
            draining: AtomicBool::new(false),
            cache: Mutex::new(None),
        }
    }

    /// Readiness turns false immediately; in-flight work keeps running.
    pub(crate) fn start_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    async fn sources(&self) -> Vec<SourceStatus> {
        let mut cache = self.cache.lock().await;
        if let Some((at, statuses)) = cache.as_ref()
            && at.elapsed() < SOURCE_CHECK_TTL
        {
            return statuses.clone();
        }
        let statuses = self.engine.check_sources(SOURCE_CHECK_TIMEOUT).await;
        *cache = Some((Instant::now(), statuses.clone()));
        statuses
    }
}

pub(crate) fn router(state: Arc<OpsState>) -> Router {
    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics_endpoint))
        .with_state(state)
}

async fn livez() -> &'static str {
    "ok"
}

#[derive(Debug, Serialize)]
struct Readiness {
    ready: bool,
    reason: Option<String>,
}

async fn readyz(State(state): State<Arc<OpsState>>) -> Response {
    let (status, reason) = if state.is_draining() {
        (StatusCode::SERVICE_UNAVAILABLE, Some("draining".to_owned()))
    } else {
        let down: Vec<String> = state
            .sources()
            .await
            .into_iter()
            .filter(|s| s.required && !s.up)
            .map(|s| s.name)
            .collect();
        if down.is_empty() {
            (StatusCode::OK, None)
        } else {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some(format!("required sources down: {}", down.join(", "))),
            )
        }
    };
    let body = Readiness {
        ready: status == StatusCode::OK,
        reason,
    };
    (status, Json(body)).into_response()
}

#[derive(Debug, Serialize)]
struct Health {
    version: &'static str,
    draining: bool,
    memory: codelake_core::MemoryStats,
    queries: QueryLoad,
    sources: Vec<SourceStatus>,
}

/// Admission slots in use (see `codelake_core::governance`).
#[derive(Debug, Serialize)]
struct QueryLoad {
    running: usize,
    queued: usize,
    /// Gave their running slot back while their client does not read.
    suspended: usize,
}

async fn healthz(State(state): State<Arc<OpsState>>) -> Json<Health> {
    let admission = state.engine.governance().admission();
    let (running, queued) = admission.load();
    let suspended = admission.suspended();
    Json(Health {
        version: env!("CARGO_PKG_VERSION"),
        draining: state.is_draining(),
        memory: state.engine.memory(),
        queries: QueryLoad {
            running,
            queued,
            suspended,
        },
        sources: state.sources().await,
    })
}

#[allow(
    clippy::cast_precision_loss,
    reason = "gauges are f64 by protocol; byte counts below 2^53 are exact"
)]
async fn metrics_endpoint(State(state): State<Arc<OpsState>>) -> impl IntoResponse {
    let memory = state.engine.memory();
    metrics::gauge!("codelake_memory_limit_bytes").set(memory.limit_bytes as f64);
    metrics::gauge!("codelake_memory_reserved_bytes").set(memory.reserved_bytes as f64);
    metrics::gauge!("codelake_spill_used_bytes").set(memory.spill_used_bytes as f64);
    metrics::gauge!("codelake_spill_limit_bytes").set(memory.spill_limit_bytes as f64);
    if let Some(open) = crate::net::limits::open_descriptors() {
        metrics::gauge!("codelake_open_files").set(open as f64);
    }
    if let Some(soft) = crate::net::limits::current_soft() {
        metrics::gauge!("codelake_open_files_limit").set(soft as f64);
    }
    for source in state.sources().await {
        metrics::gauge!("codelake_source_up", "source" => source.name, "kind" => source.kind)
            .set(if source.up { 1.0 } else { 0.0 });
    }
    (
        [("content-type", "text/plain; version=0.0.4")],
        state.prometheus.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use codelake_core::ByteSize;
    use codelake_core::config::{ExecutionConfig, MemoryConfig};
    use http_body_util::BodyExt;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use std::sync::OnceLock;
    use tower::ServiceExt;

    fn prometheus() -> PrometheusHandle {
        static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
        HANDLE
            .get_or_init(|| PrometheusBuilder::new().install_recorder().unwrap())
            .clone()
    }

    fn state(dir: &std::path::Path) -> Arc<OpsState> {
        let memory = MemoryConfig {
            limit: ByteSize::mib(64),
            spill_dir: dir.to_owned(),
            spill_limit: ByteSize::mib(64),
            metadata_cache_limit: ByteSize::mib(1),
        };
        let engine = Engine::new(&memory, &ExecutionConfig::default()).unwrap();
        Arc::new(OpsState::new(Arc::new(engine), prometheus()))
    }

    async fn get(state: &Arc<OpsState>, path: &str) -> (StatusCode, String) {
        let response = router(Arc::clone(state))
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn liveness_is_always_ok() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        state.start_draining();
        assert_eq!(get(&state, "/livez").await, (StatusCode::OK, "ok".to_owned()));
    }

    #[tokio::test]
    async fn readiness_flips_when_draining() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let (status, body) = get(&state, "/readyz").await;
        assert_eq!(status, StatusCode::OK, "{body}");

        state.start_draining();
        let (status, body) = get(&state, "/readyz").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("draining"), "{body}");
    }

    #[tokio::test]
    async fn health_reports_memory_ceiling() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let (status, body) = get(&state, "/healthz").await;
        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["memory"]["limit_bytes"], ByteSize::mib(64).as_u64());
        assert_eq!(json["draining"], false);
    }

    #[tokio::test]
    async fn metrics_expose_memory_gauges() {
        let dir = tempfile::tempdir().unwrap();
        let state = state(dir.path());
        let (status, body) = get(&state, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("codelake_memory_limit_bytes"), "{body}");
        assert!(body.contains("codelake_spill_limit_bytes"), "{body}");
        assert!(body.contains("codelake_open_files "), "{body}");
        assert!(body.contains("codelake_open_files_limit "), "{body}");
    }
}
