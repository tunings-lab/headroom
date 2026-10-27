//! Trace export minimal sanity test.
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use headroom_proxy::observability::otel::{OtelConfig, init, in_request_span};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use tokio::sync::oneshot;

#[derive(Clone)]
struct CollectorState { count: Arc<AtomicUsize> }

async fn collect(State(state): State<CollectorState>, body: Bytes) -> impl IntoResponse {
    eprintln!("TRACE COLLECTOR bytes={}", body.len());
    if let Ok(req) = ExportTraceServiceRequest::decode(body) {
        let c: usize = req.resource_spans.iter().map(|rs| rs.scope_spans.iter().map(|ss| ss.spans.len()).sum::<usize>()).sum();
        state.count.fetch_add(c, Ordering::SeqCst);
        eprintln!("TRACE COLLECTOR spans={}", c);
    }
    StatusCode::OK
}

async fn start_collector() -> (SocketAddr, CollectorState, oneshot::Sender<()>) {
    let state = CollectorState { count: Arc::new(AtomicUsize::new(0)) };
    let app = Router::new().route("/v1/traces", post(collect)).with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move { let _ = axum::serve(listener, app).with_graceful_shutdown(async move { let _ = rx.await; }).await; });
    (addr, state, tx)
}

#[tokio::test(flavor = "multi_thread")]
async fn trace_export_minimal() {
    unsafe {
        std::env::remove_var("HEADROOM_OTEL_ENABLED");
        std::env::remove_var("HEADROOM_OTEL_TRACES_ENDPOINT");
    }
    let (addr, counts, _tx) = start_collector().await;
    let traces_url = format!("http://{}/v1/traces", addr);
    unsafe {
        std::env::set_var("HEADROOM_OTEL_ENABLED", "1");
        std::env::set_var("HEADROOM_OTEL_METRICS_ENABLED", "0");
        std::env::set_var("HEADROOM_OTEL_TRACES_ENDPOINT", &traces_url);
    }
    let cfg = OtelConfig::from_env();
    let _ = init(&cfg).await;
    eprintln!("trace init done");
    in_request_span("test", "req-1".into(), "GET".into(), "/ping".into(), async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await
    }).await;
    eprintln!("span ended");
    for _ in 0..30 {
        let c = counts.count.load(Ordering::SeqCst);
        eprintln!("trace count={}", c);
        if c > 0 { return; }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
    panic!("no traces received");
}
