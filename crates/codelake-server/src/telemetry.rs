//! Logs and traces.
//!
//! - **Logs:** structured JSON on stdout; `RUST_LOG` controls verbosity
//!   (default `info`). Log lines inside a query carry its span fields
//!   (`codelake.query_id`, `codelake.trace_id`, …).
//! - **Traces:** when `[telemetry] otlp_endpoint` is set, spans of the
//!   `codelake::trace` target (see `codelake_core::trace`) are exported over
//!   OTLP/HTTP (protobuf) with a batch processor. Sampling is parent-based: a
//!   query that arrives with a W3C `traceparent` follows the caller's
//!   decision; new traces are kept with probability `sampling_ratio`.
//!
//! Disabled tracing costs nothing beyond the `tracing` spans the logs already
//! use: no exporter thread, no OpenTelemetry layer, no backend.

use std::time::Duration;

use codelake_core::config::TelemetryConfig;
use codelake_core::trace::{self, TraceBackend, TraceParent};
use opentelemetry::trace::{
    SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator, Sampler, SdkTracerProvider};
use tracing::Span;
use tracing::level_filters::LevelFilter;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Instrumentation scope reported with every span.
const SCOPE: &str = "codelake";

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("telemetry is enabled but otlp_endpoint is not set")]
    NoEndpoint,
    #[error("OTLP exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
    #[error("tracing subscriber: {0}")]
    Subscriber(#[from] tracing_subscriber::util::TryInitError),
}

/// Keeps the exporter alive; [`Telemetry::shutdown`] flushes pending spans.
#[derive(Debug)]
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
    sampling_ratio: f64,
}

impl Telemetry {
    /// `true` when spans are exported.
    pub fn is_exporting(&self) -> bool {
        self.provider.is_some()
    }

    /// Ratio for new traces when spans are exported, `None` otherwise. The
    /// PostgreSQL listener uses it to sample whole sessions.
    pub fn sampling_ratio(&self) -> Option<f64> {
        self.is_exporting().then_some(self.sampling_ratio)
    }

    /// Exports every finished span now. Blocks until the collector answered
    /// or the export timeout passed.
    pub fn flush(&self) {
        if let Some(provider) = &self.provider
            && let Err(err) = provider.force_flush()
        {
            tracing::warn!(error = %err, "flushing spans failed");
        }
    }

    /// Flushes and stops the exporter.
    pub fn shutdown(self) {
        if let Some(provider) = self.provider
            && let Err(err) = provider.shutdown()
        {
            tracing::warn!(error = %err, "stopping the span exporter failed");
        }
    }
}

/// Builds the OTLP pipeline for `config`. No global state is touched.
pub fn tracer_provider(config: &TelemetryConfig) -> Result<SdkTracerProvider, TelemetryError> {
    let url = config.traces_url().ok_or(TelemetryError::NoEndpoint)?;
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(url)
        .with_timeout(Duration::from_secs(config.export_timeout_secs))
        .build()?;
    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
        .build();
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sampling_ratio,
        ))))
        .with_resource(resource)
        .build())
}

/// Spans exported over OTLP: the engine's trace target, plus warnings and
/// errors from `CodeLake` crates (they become span events).
fn exported() -> Targets {
    Targets::new()
        .with_target(trace::TARGET, LevelFilter::TRACE)
        .with_target("codelake_core", LevelFilter::WARN)
        .with_target("codelake_connectors", LevelFilter::WARN)
        .with_target("codelake_server", LevelFilter::WARN)
        .with_target("codelake", LevelFilter::WARN)
}

/// Installs the global subscriber: JSON logs always, OTLP export when
/// `config` enables it. Call once, before the async runtime starts.
pub fn init(config: Option<&TelemetryConfig>) -> Result<Telemetry, TelemetryError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let logs = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_target(true)
        .with_filter(filter);
    let Some(config) = config.filter(|c| c.otlp_endpoint.is_some()) else {
        tracing_subscriber::registry().with(logs).try_init()?;
        return Ok(Telemetry {
            provider: None,
            sampling_ratio: 0.0,
        });
    };
    let provider = tracer_provider(config)?;
    let traces = tracing_opentelemetry::layer()
        .with_tracer(provider.tracer(SCOPE))
        .with_filter(exported());
    tracing_subscriber::registry()
        .with(logs)
        .with(traces)
        .try_init()?;
    trace::install_backend(Box::new(OtelBackend));
    tracing::info!(
        endpoint = config.traces_url().as_deref(),
        service = %config.service_name,
        sampling_ratio = config.sampling_ratio,
        "exporting traces over OTLP"
    );
    Ok(Telemetry {
        provider: Some(provider),
        sampling_ratio: config.sampling_ratio,
    })
}

/// Bridges `codelake_core::trace` to `tracing-opentelemetry`.
#[derive(Debug)]
struct OtelBackend;

impl TraceBackend for OtelBackend {
    fn set_remote_parent(&self, span: &Span, parent: &TraceParent, tracestate: Option<&str>) {
        let state = tracestate
            .and_then(|s| s.parse::<TraceState>().ok())
            .unwrap_or_default();
        let remote = SpanContext::new(
            TraceId::from_bytes(parent.trace_id),
            SpanId::from_bytes(parent.parent_id),
            // Only the sampled bit has a defined meaning in version 00.
            TraceFlags::new(parent.flags & TraceParent::SAMPLED),
            true,
            state,
        );
        if let Err(err) = span.set_parent(Context::new().with_remote_span_context(remote)) {
            tracing::debug!(error = %err, "remote parent not applied");
        }
    }

    fn trace_id(&self, span: &Span) -> Option<String> {
        let context = span.context();
        let span_context = context.span().span_context().clone();
        // An unsampled span has ids too, but nothing will ever show them.
        (span_context.is_valid() && span_context.is_sampled()).then(|| span_context.trace_id().to_string())
    }
}

/// A synthetic parent for one PostgreSQL wire session: every statement of the
/// session becomes a child of it, so a tracing backend shows the session as
/// one trace. The sampling decision is taken once per session with `ratio`.
pub fn session_parent(ratio: f64) -> TraceParent {
    let ids = RandomIdGenerator::default();
    let trace_id = ids.new_trace_id().to_bytes();
    let parent_id = ids.new_span_id().to_bytes();
    let flags = if ratio_samples(&trace_id, ratio) {
        TraceParent::SAMPLED
    } else {
        0
    };
    TraceParent {
        trace_id,
        parent_id,
        flags,
    }
}

/// The same rule as OpenTelemetry's `TraceIdRatioBased`: the lower 63 bits of
/// the trace id's last 8 bytes against `ratio × 2^63`.
fn ratio_samples(trace_id: &[u8; 16], ratio: f64) -> bool {
    if ratio >= 1.0 {
        return true;
    }
    if ratio.is_nan() || ratio <= 0.0 {
        return false;
    }
    let mut low = [0_u8; 8];
    low.copy_from_slice(&trace_id[8..]);
    let value = u64::from_be_bytes(low) >> 1;
    // Compared in floating point, built from exact 32-bit halves: rounding
    // at one part in 2^52 is irrelevant for a sampling decision.
    let high = f64::from(u32::try_from(value >> 32).unwrap_or(u32::MAX));
    let low = f64::from(u32::try_from(value & 0xffff_ffff).unwrap_or(u32::MAX));
    let two_32 = f64::from(u32::MAX) + 1.0;
    high * two_32 + low < ratio * two_32 * two_32 / 2.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratio_extremes_are_exact() {
        let id = [0xff; 16];
        assert!(ratio_samples(&id, 1.0));
        assert!(!ratio_samples(&[0; 16], 0.0));
        assert!(!ratio_samples(&[0; 16], f64::NAN));
        let mut low = [0_u8; 16];
        low[15] = 2; // value 1 after the shift
        assert!(ratio_samples(&low, 0.5));
        assert!(!ratio_samples(&id, 0.5));
    }

    #[test]
    fn ratio_samples_about_the_right_share() {
        let sampled = (0..10_000).filter(|_| session_parent(0.25).is_sampled()).count();
        assert!((2_000..3_000).contains(&sampled), "{sampled}");
        assert!(session_parent(1.0).is_sampled());
        assert!(!session_parent(0.0).is_sampled());
        let parent = session_parent(1.0);
        assert_ne!(parent.trace_id, [0; 16]);
        assert_ne!(parent.parent_id, [0; 8]);
        assert_eq!(TraceParent::parse(&parent.to_string()), Ok(parent));
    }

    #[test]
    fn provider_needs_an_endpoint() {
        let config = TelemetryConfig::default();
        assert!(matches!(
            tracer_provider(&config),
            Err(TelemetryError::NoEndpoint)
        ));
    }

    #[test]
    fn exported_targets_are_the_trace_target_and_codelake_warnings() {
        let targets = exported();
        let enabled = |target: &str, level| targets.would_enable(target, &level);
        assert!(enabled(trace::TARGET, tracing::Level::INFO));
        assert!(enabled(trace::TARGET, tracing::Level::TRACE));
        assert!(enabled("codelake_connectors::sqlserver", tracing::Level::WARN));
        assert!(!enabled("codelake_connectors::sqlserver", tracing::Level::INFO));
        assert!(!enabled("tiberius", tracing::Level::ERROR));
        assert!(!enabled("datafusion", tracing::Level::WARN));
    }
}
