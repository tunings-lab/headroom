//! End-to-end smoke test for OTLP export — confirm collector receives batches.

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use common::start_proxy_with_state;
use headroom_proxy::observability::otel::OtelConfig;
use headroom_proxy::observability::init_otel;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use tokio::sync::oneshot;

#[derive(Clone)]
struct CollectorState {
    metrics_count: Arc<AtomicUsize>,
    spans_count: Arc<AtomicUsize>,
}

async fn collect_metrics(
    State(state): State<CollectorState>,
    body: Bytes,
) -> impl IntoResponse {
    eprintln!("COLLECTOR /v1/metrics received {} bytes", body.len());
    if let Ok(req) = ExportMetricsServiceRequest::decode(body) {
        let mut count = 0usize;
        for rm in &req.resource_metrics {
            for sm in &rm.scope_metrics {
                count += sm.metrics.len();
            }
        }
        state.metrics_count.fetch_add(count, Ordering::SeqCst);
    }
    StatusCode::OK
}

async fn collect_traces(
    State(state): State<CollectorState>,
    body: Bytes,
) -> impl IntoResponse {
    eprintln!("COLLECTOR /v1/traces received {} bytes", body.len());
    if let Ok(req) = ExportTraceServiceRequest::decode(body) {
        let mut count = 0usize;
        for rs in &req.resource_spans {
            for ss in &rs.scope_spans {
                count += ss.spans.len();
            }
        }
        state.spans_count.fetch_add(count, Ordering::SeqCst);
    }
    StatusCode::OK
}

async fn start_collector() -> (SocketAddr, CollectorState, oneshot::Sender<()>) {
    let state = CollectorState {
        metrics_count: Arc::new(AtomicUsize::new(0)),
        spans_count: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/v1/metrics", post(collect_metrics))
        .route("/v1/traces", post(collect_traces))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind collector");
    let addr = listener.local_addr().expect("collector addr");
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { let _ = rx.await; })
            .await;
    });
    (addr, state, tx)
}

#[tokio::test(flavor = "multi_thread")]
async fn otlp_smoke_receives_metrics_and_traces() {
    // Normalise env so the previous test run cannot leak providers.
    unsafe {
        std::env::remove_var("HEADROOM_OTEL_ENABLED");
        std::env::remove_var("HEADROOM_OTEL_METRICS_ENDPOINT");
        std::env::remove_var("HEADROOM_OTEL_TRACES_ENDPOINT");
    }

    let (collector_addr, counts, _collector_shutdown) = start_collector().await;
    let metrics_url = format!("http://{}/v1/metrics", collector_addr);
    // let traces_url = format!("http://{}/v1/traces", collector_addr);

    let traces_url = format!("http://{}/v1/traces", collector_addr);

    unsafe {
        std::env::set_var("HEADROOM_OTEL_ENABLED", "1");
        std::env::set_var("OTEL_METRIC_EXPORT_INTERVAL", "1000");
        std::env::set_var("HEADROOM_OTEL_METRICS_ENDPOINT", &metrics_url);
        std::env::set_var("HEADROOM_OTEL_TRACES_ENDPOINT", &traces_url);
    }

    // PR-G4: initialize the OTEL providers explicitly because the test harness
    // does not go through `main.rs`.
    let otel_config = OtelConfig::from_env();
    let _ = init_otel(&otel_config).await;
    eprintln!("test OTEL init done");

    // Use a real HTTP upstream because the catch-all path will forward a GET.
    let upstream = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/ping"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&upstream)
        .await;

    let proxy = start_proxy_with_state(&upstream.uri(), |_| {}, |s| s).await;

    // Send a request through the proxy.
    let resp = reqwest::get(format!("{}/ping", proxy.url()))
        .await
        .expect("proxy reachable");
    assert_eq!(resp.status(), 200);

    // Metrics/traces export; wait up to 15s.
    let mut received_metrics = false;
    let mut received_traces = false;
    for _ in 0..30 {
        let m = counts.metrics_count.load(Ordering::SeqCst);
        let s = counts.spans_count.load(Ordering::SeqCst);
        eprintln!("loop metrics={} traces={}", m, s);
        if m > 0 { received_metrics = true; }
        if s > 0 { received_traces = true; }
        if received_metrics && received_traces { break; }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    tokio::time::timeout(tokio::time::Duration::from_secs(5), proxy.shutdown())
        .await
        .expect("proxy shutdown timed out");
    eprintln!("proxy shutdown done");
    
    assert!(
        received_metrics,
        "expected at least one OTLP metric batch to reach the collector"
    );
    assert!(
        received_traces,
        "expected at least one OTLP trace span to reach the collector"
    );
}
