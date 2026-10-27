//! Minimal OTLP export sanity test without the proxy.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use headroom_proxy::observability::otel::{OtelConfig, init, record_request_count, record_request_latency};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
use tokio::sync::oneshot;

#[derive(Clone)]
struct CollectorState { count: Arc<AtomicUsize> }

async fn collect(State(state): State<CollectorState>, body: Bytes) -> impl IntoResponse {
    eprintln!("MINI COLLECTOR bytes={}", body.len());
    if let Ok(req) = ExportMetricsServiceRequest::decode(body) {
        let c: usize = req.resource_metrics.iter().map(|rm| rm.scope_metrics.iter().map(|sm| sm.metrics.len()).sum::<usize>()).sum();
        state.count.fetch_add(c, Ordering::SeqCst);
        eprintln!("MINI COLLECTOR metrics={}", c);
    }
    StatusCode::OK
}

async fn start_collector() -> (SocketAddr, CollectorState, oneshot::Sender<()>) {
    let state = CollectorState { count: Arc::new(AtomicUsize::new(0)) };
    let app = Router::new().route("/v1/metrics", post(collect)).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move { let _ = axum::serve(listener, app).with_graceful_shutdown(async move { let _ = rx.await; }).await; });
    (addr, state, tx)
}

#[tokio::test(flavor = "multi_thread")]
async fn otlp_minimal_export() {
    unsafe {
        std::env::remove_var("HEADROOM_OTEL_ENABLED");
        std::env::remove_var("HEADROOM_OTEL_METRICS_ENDPOINT");
    }
    let (addr, counts, _tx) = start_collector().await;
    let metrics_url = format!("http://{}/v1/metrics", addr);
    unsafe {
        std::env::set_var("HEADROOM_OTEL_ENABLED", "1");
        std::env::set_var("OTEL_METRIC_EXPORT_INTERVAL", "1000");
        std::env::set_var("HEADROOM_OTEL_METRICS_ENDPOINT", &metrics_url);
        std::env::set_var("HEADROOM_OTEL_TRACES_ENABLED", "0");
    }
    let cfg = OtelConfig::from_env();
    let _ = init(&cfg).await;
    record_request_count("GET", "/test", 200);
    record_request_latency("GET", "/test", 200, 0.005);
    eprintln!("recorded");
    for _ in 0..30 {
        let c = counts.count.load(Ordering::SeqCst);
        eprintln!("count={}", c);
        if c > 0 { return; }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    panic!("no metrics received");
}
