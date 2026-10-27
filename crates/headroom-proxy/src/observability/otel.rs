//! OpenTelemetry SDK integration for headroom-proxy — Phase G PR-G4.
//!
//! Bridges the existing Prometheus metrics and structured request logs to OTLP.
//! The goal is not to replace the Prometheus `/metrics` endpoint but to add an
//! OTLP side-channel so fleet-wide compression, latency, and trace data can be
//! ingested by the cluster's otel-collector without scraping every pod.
//!
//! # Signals
//!
//! * **Metrics** — exported via `opentelemetry_sdk::metrics::SdkMeterProvider` to
//!   `HEADROOM_OTEL_METRICS_ENDPOINT` (default `http://localhost:4318/v1/metrics`).
//! * **Traces** — exported via `opentelemetry_sdk::trace::SdkTracerProvider` to
//!   `HEADROOM_OTEL_TRACES_ENDPOINT` (default `http://localhost:4318/v1/traces`).
//!
//! # Configuration
//!
//! All OTLP export is gated by `HEADROOM_OTEL_ENABLED=1`. Individual signals
//! can be disabled with `HEADROOM_OTEL_METRICS_ENABLED=0` or
//! `HEADROOM_OTEL_TRACES_ENABLED=0`. Endpoint and headers are read from the
//! standard OTLP environment variables with `HEADROOM_OTEL_*` overrides so the
//! proxy can coexist with other applications running OTEL in the same pod.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::trace::{Tracer, TraceContextExt};
use opentelemetry::KeyValue;
use opentelemetry_http::{HttpClient, HttpError};
use opentelemetry_otlp::{HasExportConfig, HasHttpConfig, WithExportConfig};
use opentelemetry_otlp::WithHttpConfig;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::resource::Resource;
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Phase G PR-G4 remediation: the default `opentelemetry-otlp`
/// `reqwest-client` metrics exporter spawns an async `reqwest::Client`
/// inside a dedicated std thread created by `PeriodicReader`. That
/// thread calls `futures_executor::block_on(exporter.export())`, but
/// an async `reqwest::Client` requires a Tokio reactor, which doesn't
/// exist there. The symptom is a panic:
///
///   thread 'OpenTelemetry.Metrics.PeriodicReader' panicked:
///   there is no reactor running, must be called from the context of a Tokio 1.x runtime
///
/// We provide a synchronous HTTP client that uses `reqwest::blocking`
/// for the metrics exporter. `reqwest::blocking` carries its own
/// runtime internally, so it is safe to call from the PeriodicReader
/// thread. The `HttpClient` trait is `async_trait`, but we resolve the
/// future immediately in the body.
#[derive(Debug)]
struct BlockingHttpClient {
    inner: reqwest::blocking::Client,
}

impl BlockingHttpClient {
    fn new(timeout: Duration) -> Self {
        let inner = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_default();
        Self { inner }
    }
}

#[async_trait::async_trait]
impl HttpClient for BlockingHttpClient {
    async fn send_bytes(
        &self,
        request: http::Request<bytes::Bytes>,
    ) -> Result<http::Response<bytes::Bytes>, HttpError> {
        let reqwest_request: reqwest::blocking::Request = request.try_into()?;
        let mut response = self.inner.execute(reqwest_request)?.error_for_status()?;
        let status = response.status();
        let mut http_response = http::Response::builder().status(status);
        let headers = std::mem::take(response.headers_mut());
        for (key, value) in headers {
            if let Some(key) = key {
                http_response = http_response.header(key, value);
            }
        }
        let body = response.bytes()?.into();
        Ok(http_response.body(body).map_err(|e: http::Error| {
            Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()) as HttpError
        })?)
    }
}

static OTEL_INIT: tokio::sync::OnceCell<Providers> = tokio::sync::OnceCell::const_new();
static OTEL_METER: OnceLock<Meter> = OnceLock::new();

const OTEL_SERVICE_NAME: &str = "service.name";
const OTEL_SERVICE_VERSION: &str = "service.version";

#[derive(Debug, Clone)]
pub struct OtelConfig {
    pub enabled: bool,
    pub metrics_enabled: bool,
    pub traces_enabled: bool,
    pub service_name: String,
    pub service_version: String,
    pub metrics_endpoint: String,
    pub traces_endpoint: String,
    pub headers: HashMap<String, String>,
    pub compression: Option<String>,
    pub timeout: Duration,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            metrics_enabled: true,
            traces_enabled: true,
            service_name: "headroom-proxy".into(),
            service_version: env!("CARGO_PKG_VERSION").into(),
            metrics_endpoint: "http://localhost:4318/v1/metrics".into(),
            traces_endpoint: "http://localhost:4318/v1/traces".into(),
            headers: HashMap::new(),
            compression: None,
            timeout: Duration::from_secs(10),
        }
    }
}

impl OtelConfig {
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        cfg.enabled = parse_bool_env("HEADROOM_OTEL_ENABLED", false);
        cfg.metrics_enabled = cfg.enabled && parse_bool_env("HEADROOM_OTEL_METRICS_ENABLED", true);
        cfg.traces_enabled = cfg.enabled && parse_bool_env("HEADROOM_OTEL_TRACES_ENABLED", true);

        if let Ok(v) = std::env::var("HEADROOM_OTEL_SERVICE_NAME") {
            if !v.is_empty() { cfg.service_name = v; }
        }
        if let Ok(v) = std::env::var("HEADROOM_OTEL_SERVICE_VERSION") {
            if !v.is_empty() { cfg.service_version = v; }
        }

        if let Ok(v) = std::env::var("HEADROOM_OTEL_METRICS_ENDPOINT") {
            if !v.is_empty() { cfg.metrics_endpoint = v; }
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") {
            if !v.is_empty() { cfg.metrics_endpoint = v; }
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            if !v.is_empty() { cfg.metrics_endpoint = format!("{}/v1/metrics", v.trim_end_matches('/')); }
        }

        if let Ok(v) = std::env::var("HEADROOM_OTEL_TRACES_ENDPOINT") {
            if !v.is_empty() { cfg.traces_endpoint = v; }
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
            if !v.is_empty() { cfg.traces_endpoint = v; }
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            if !v.is_empty() { cfg.traces_endpoint = format!("{}/v1/traces", v.trim_end_matches('/')); }
        }

        if let Ok(v) = std::env::var("HEADROOM_OTEL_HEADERS") {
            cfg.headers = parse_key_value_pairs(&v);
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_HEADERS") {
            cfg.headers = parse_key_value_pairs(&v);
        }

        if let Ok(v) = std::env::var("HEADROOM_OTEL_COMPRESSION") {
            if !v.is_empty() { cfg.compression = Some(v); }
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_COMPRESSION") {
            if !v.is_empty() { cfg.compression = Some(v); }
        }

        if let Ok(v) = std::env::var("HEADROOM_OTEL_TIMEOUT_MS") {
            if let Ok(ms) = v.parse::<u64>() { cfg.timeout = Duration::from_millis(ms); }
        } else if let Ok(v) = std::env::var("OTEL_EXPORTER_OTLP_TIMEOUT") {
            if let Ok(ms) = v.parse::<u64>() { cfg.timeout = Duration::from_millis(ms); }
        }

        cfg
    }
}

struct Providers {
    meter_provider: SdkMeterProvider,
    tracer_provider: SdkTracerProvider,
    meter: Meter,
    tracer: opentelemetry::global::BoxedTracer,
}

pub fn init(
    config: &OtelConfig,
) -> Pin<Box<dyn Future<Output = Option<(&'static Meter, &'static opentelemetry::global::BoxedTracer)>> + Send + '_>> {
    Box::pin(async move {
        if !config.enabled || (!config.metrics_enabled && !config.traces_enabled) {
            return None;
        }

        let providers = OTEL_INIT
            .get_or_init(|| build_providers_future(config))
            .await;

        Some((&providers.meter, &providers.tracer))
    })
}

fn build_providers_future(
    config: &OtelConfig,
) -> Pin<Box<dyn Future<Output = Providers> + Send + '_>> {
    Box::pin(async move { build_providers(config).await })
}

async fn build_providers(config: &OtelConfig) -> Providers {
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new(OTEL_SERVICE_NAME, config.service_name.clone()),
            KeyValue::new(OTEL_SERVICE_VERSION, config.service_version.clone()),
        ])
        .build();

    let meter_provider = if config.metrics_enabled {
        let exporter = build_metric_exporter(config);
        let reader = PeriodicReader::builder(exporter)
            .with_interval(
                std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .unwrap_or(Duration::from_secs(60)),
            )
            .build();
        SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_reader(reader)
            .build()
    } else {
        SdkMeterProvider::builder().build()
    };

    let tracer_provider = if config.traces_enabled {
        let traces_builder = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(&config.traces_endpoint)
            .with_timeout(config.timeout);
        let traces_builder = apply_headers(traces_builder, &config.headers);
        let traces_builder = apply_compression(traces_builder, config.compression.as_deref());
        let exporter = traces_builder
            .build()
            .expect("HEADROOM_OTEL traces exporter builds");
        // Use the async-runtime batch processor so spans are exported from a
        // background task rather than blocking the request handler thread.
        let batch = opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor::builder(
            exporter,
            runtime::Tokio,
        )
        .build();
        SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_span_processor(batch)
            .build()
    } else {
        SdkTracerProvider::builder().build()
    };

    global::set_meter_provider(meter_provider.clone());
    global::set_tracer_provider(tracer_provider.clone());

    let meter = global::meter("headroom_proxy");
    let tracer = global::tracer("headroom_proxy");
    let _ = OTEL_METER.set(meter.clone());

    Providers {
        meter_provider,
        tracer_provider,
        meter,
        tracer,
    }
}

fn build_metric_exporter(config: &OtelConfig) -> opentelemetry_otlp::MetricExporter {
    // `reqwest::blocking::Client::new()` must not be called from an async
    // context because it drops a Tokio runtime when the client goes out of
    // scope, which panics when blocking is disallowed. Constructing it on a
    // dedicated std thread avoids the issue entirely.
    let client = std::thread::spawn({
        let timeout = config.timeout;
        move || BlockingHttpClient::new(timeout)
    })
    .join()
    .expect("HEADROOM_OTEL blocking HTTP client thread builds");

    let mut metrics_builder = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_http_client(client)
        .with_endpoint(&config.metrics_endpoint)
        .with_timeout(config.timeout);
    metrics_builder = apply_headers(metrics_builder, &config.headers);
    metrics_builder = apply_compression(metrics_builder, config.compression.as_deref());
    metrics_builder
        .build()
        .expect("HEADROOM_OTEL metrics exporter builds")
}

fn meter() -> Option<&'static Meter> {
    OTEL_METER.get()
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

pub fn record_request_latency(method: &str, path: &str, status: u16, seconds: f64) {
    if let Some(m) = meter() {
        static HIST: OnceLock<Histogram<f64>> = OnceLock::new();
        let hist = HIST.get_or_init(|| {
            m.f64_histogram(super::metric_names::METRIC_OTEL_REQUEST_LATENCY_SECONDS)
                .with_description(super::metric_names::METRIC_OTEL_REQUEST_LATENCY_SECONDS_HELP)
                .with_unit("s")
                .build()
        });
        hist.record(
            seconds,
            &[
                KeyValue::new(super::metric_names::LABEL_METHOD, method.to_string()),
                KeyValue::new(super::metric_names::LABEL_PATH, sanitize_path(path)),
                KeyValue::new(super::metric_names::LABEL_STATUS_CLASS, status_class(status)),
            ],
        );
    }
}

pub fn record_request_count(method: &str, path: &str, status: u16) {
    if let Some(m) = meter() {
        static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
        let counter = COUNTER.get_or_init(|| {
            m.u64_counter(super::metric_names::METRIC_OTEL_REQUEST_COUNT_TOTAL)
                .with_description(super::metric_names::METRIC_OTEL_REQUEST_COUNT_TOTAL_HELP)
                .build()
        });
        counter.add(
            1,
            &[
                KeyValue::new(super::metric_names::LABEL_METHOD, method.to_string()),
                KeyValue::new(super::metric_names::LABEL_PATH, sanitize_path(path)),
                KeyValue::new(super::metric_names::LABEL_STATUS_CLASS, status_class(status)),
            ],
        );
    }
}

pub fn record_compression_bytes(endpoint: &str, strategy: &str, bytes_saved: u64) {
    if bytes_saved == 0 {
        return;
    }
    if let Some(m) = meter() {
        static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
        let counter = COUNTER.get_or_init(|| {
            m.u64_counter(super::metric_names::METRIC_OTEL_COMPRESSION_BYTES_TOTAL)
                .with_description(super::metric_names::METRIC_OTEL_COMPRESSION_BYTES_TOTAL_HELP)
                .build()
        });
        counter.add(
            bytes_saved,
            &[
                KeyValue::new(super::metric_names::LABEL_ENDPOINT, endpoint.to_string()),
                KeyValue::new(super::metric_names::LABEL_STRATEGY, strategy.to_string()),
            ],
        );
    }
}

pub fn record_compression_decision(endpoint: &str, decision: &str, reason: &str) {
    if let Some(m) = meter() {
        static COUNTER: OnceLock<Counter<u64>> = OnceLock::new();
        let counter = COUNTER.get_or_init(|| {
            m.u64_counter(super::metric_names::METRIC_OTEL_COMPRESSION_DECISION_COUNT_TOTAL)
                .with_description(
                    super::metric_names::METRIC_OTEL_COMPRESSION_DECISION_COUNT_TOTAL_HELP,
                )
                .build()
        });
        counter.add(
            1,
            &[
                KeyValue::new(super::metric_names::LABEL_ENDPOINT, endpoint.to_string()),
                KeyValue::new(super::metric_names::LABEL_DECISION, decision.to_string()),
                KeyValue::new(super::metric_names::LABEL_REASON, reason.to_string()),
            ],
        );
    }
}

/// Bound cardinality on `path` by collapsing known dynamic segments.
fn sanitize_path(path: &str) -> String {
    // Collapse UUID-ish and numeric path segments so a single misbehaving
    // client cannot blow up OTLP metric cardinality. Keep the LLM endpoint
    // names literal.
    path.split('/')
        .map(|seg| {
            if seg.is_empty() {
                seg.to_string()
            } else if seg.parse::<u64>().is_ok() {
                ":id".to_string()
            } else if seg.len() == 32 && seg.chars().all(|c| c.is_ascii_hexdigit()) {
                ":hex".to_string()
            } else {
                seg.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn apply_headers<T>(builder: T, headers: &HashMap<String, String>) -> T
where
    T: HasHttpConfig,
{
    if headers.is_empty() {
        builder
    } else {
        builder.with_headers(headers.clone())
    }
}

fn apply_compression<T>(builder: T, compression: Option<&str>) -> T
where
    T: HasExportConfig,
{
    // opentelemetry-otlp 0.28s HTTP builder does not expose `with_compression` generically through
    // the traits we can import. Compression is therefore ignored; the collector accepts plain
    // protobuf by default.
    if let Some(other) = compression {
        if !matches!(other.to_ascii_lowercase().as_str(), "none" | "false" | "off") {
            tracing::warn!(
                event = "otel_unknown_compression",
                compression = %other,
                "HTTP OTLP compression is not supported by this build; sending uncompressed"
            );
        }
    }
    builder
}

pub fn shutdown() -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        if let Some(providers) = OTEL_INIT.get() {
            let meter_res = providers.meter_provider.shutdown();
            let tracer_res = providers.tracer_provider.shutdown();
            if let Err(e) = meter_res {
                tracing::warn!(event = "otel_meter_shutdown_failed", error = %e, "OTEL meter provider shutdown failed");
            }
            if let Err(e) = tracer_res {
                tracing::warn!(event = "otel_tracer_shutdown_failed", error = %e, "OTEL tracer provider shutdown failed");
            }
        }
    })
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => default,
    }
}

fn parse_key_value_pairs(raw: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for item in raw.split(',') {
        let item = item.trim();
        if let Some((k, v)) = item.split_once('=') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if !k.is_empty() && !v.is_empty() {
                map.insert(k, v);
            }
        }
    }
    map
}

pub fn in_request_span<F, T>(
    name: &'static str,
    request_id: String,
    method: String,
    path: String,
    fut: F,
) -> Pin<Box<dyn Future<Output = T> + Send>>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    Box::pin(async move {
        if let Some(providers) = OTEL_INIT.get() {
            let span = providers
                .tracer
                .span_builder(name)
                .with_attributes([
                    KeyValue::new("http.request.method", method),
                    KeyValue::new("url.path", path),
                    KeyValue::new("request_id", request_id),
                ])
                .start(&providers.tracer);
            let cx = opentelemetry::Context::current().with_span(span);
            let result = opentelemetry::trace::FutureExt::with_context(fut, cx.clone()).await;
            // End the span explicitly; FutureExt does not end it on drop.
            cx.span().end();
            result
        } else {
            fut.await
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_disabled() {
        let cfg = OtelConfig::default();
        assert!(!cfg.enabled);
    }

    #[test]
    fn parse_bool_env_honours_true_values() {
        std::env::set_var("_HR_OTEL_TEST_BOOL", "true");
        assert!(parse_bool_env("_HR_OTEL_TEST_BOOL", false));
        std::env::set_var("_HR_OTEL_TEST_BOOL", "1");
        assert!(parse_bool_env("_HR_OTEL_TEST_BOOL", false));
        std::env::set_var("_HR_OTEL_TEST_BOOL", "false");
        assert!(!parse_bool_env("_HR_OTEL_TEST_BOOL", false));
    }

    #[test]
    fn sanitize_path_collapses_numeric_segments() {
        assert_eq!(sanitize_path("/v1/conversations/123/items/456"), "/v1/conversations/:id/items/:id");
    }

    #[test]
    fn sanitize_path_keeps_known_endpoints() {
        assert_eq!(sanitize_path("/v1/chat/completions"), "/v1/chat/completions");
        assert_eq!(sanitize_path("/v1/messages"), "/v1/messages");
        assert_eq!(sanitize_path("/v1/responses"), "/v1/responses");
    }

    #[test]
    fn status_class_buckets() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
        assert_eq!(status_class(999), "unknown");
    }
}
