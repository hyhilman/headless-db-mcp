use std::sync::Arc;

use axum::middleware;
use axum::routing::{get, post};
use axum::Router;
use db_headless_mcp_server::McpSession;

use crate::auth::require_bearer_token;
use crate::handlers::handle_mcp;
use crate::rate_limit::{build_limiter, enforce_rate_limit};
use crate::sse_demo::stream_demo;
use crate::state::AppState;
use crate::HttpTransportConfig;

/// Builds the full router: `POST /mcp` behind bearer auth, `GET
/// /mcp/stream` as the SSE mechanism demo (see `crate::sse_demo`), and a
/// per-IP rate limiter wrapped around both (see `crate::rate_limit`) so it
/// runs before auth, the cheapest check first.
pub(crate) fn build_router(session: Arc<McpSession>, config: &HttpTransportConfig) -> Router {
    let state = AppState {
        session,
        bearer_token: Arc::from(config.bearer_token.as_str()),
        limiter: build_limiter(config.rate_limit_per_minute),
    };

    let authenticated_mcp =
        Router::new()
            .route("/mcp", post(handle_mcp))
            .route_layer(middleware::from_fn_with_state(
                state.clone(),
                require_bearer_token,
            ));

    Router::new()
        .merge(authenticated_mcp)
        .route("/mcp/stream", get(stream_demo))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_rate_limit,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::{Request, StatusCode};
    use db_headless_mcp_server::{AuditEvent, AuditLogger, McpSession, McpToolRegistry, PingTool};
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use super::*;

    const TEST_TOKEN: &str = "correct-horse-battery-staple";
    const TEST_ADDR: SocketAddr =
        SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 55555);

    #[derive(Default)]
    struct CountingAuditLogger {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl AuditLogger for CountingAuditLogger {
        async fn record(&self, _event: AuditEvent) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn build_session(audit: Arc<CountingAuditLogger>) -> Arc<McpSession> {
        let mut registry = McpToolRegistry::new();
        registry.register(Arc::new(PingTool));
        Arc::new(McpSession::new(Arc::new(registry), audit))
    }

    fn test_config(rate_limit_per_minute: u32) -> HttpTransportConfig {
        HttpTransportConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            bearer_token: TEST_TOKEN.to_string(),
            rate_limit_per_minute,
        }
    }

    fn test_router(session: Arc<McpSession>, rate_limit_per_minute: u32) -> Router {
        let config = test_config(rate_limit_per_minute);
        build_router(session, &config).layer(MockConnectInfo(TEST_ADDR))
    }

    fn json_body(value: Value) -> Body {
        Body::from(serde_json::to_vec(&value).expect("value serializes"))
    }

    async fn body_to_bytes(response: axum::response::Response) -> Vec<u8> {
        response
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes()
            .to_vec()
    }

    fn initialize_request() -> Value {
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    }

    fn authorized_post(uri: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .header("content-type", "application/json")
            .body(body)
            .expect("request builds")
    }

    #[tokio::test]
    async fn valid_token_reaches_session_and_returns_json_rpc_response() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit.clone()), 1000);

        let response = router
            .oneshot(authorized_post("/mcp", json_body(initialize_request())))
            .await
            .expect("router responds");
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_bytes(response).await;
        let parsed: Value = serde_json::from_slice(&body).expect("valid json body");
        assert_eq!(parsed["result"]["serverInfo"]["name"], "db-headless-mcp");
        assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missing_authorization_header_is_rejected_before_session() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit.clone()), 1000);

        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(json_body(initialize_request()))
            .expect("request builds");

        let response = router.oneshot(request).await.expect("router responds");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wrong_token_is_rejected_before_session() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit.clone()), 1000);

        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("authorization", "Bearer not-the-right-token")
            .header("content-type", "application/json")
            .body(json_body(initialize_request()))
            .expect("request builds");

        let response = router.oneshot(request).await.expect("router responds");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(audit.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn malformed_body_with_correct_token_is_a_json_rpc_error_response() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit), 1000);

        let response = router
            .oneshot(authorized_post("/mcp", Body::from("not json at all {{{")))
            .await
            .expect("router responds");
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_to_bytes(response).await;
        let parsed: Value = serde_json::from_slice(&body).expect("valid json body");
        assert!(parsed["id"].is_null());
        assert!(parsed["error"]["code"].is_i64());
    }

    #[tokio::test]
    async fn notification_returns_202_with_empty_body() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit), 1000);

        let notification = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
        let response = router
            .oneshot(authorized_post("/mcp", json_body(notification)))
            .await
            .expect("router responds");
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let body = body_to_bytes(response).await;
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn requests_over_the_rate_limit_get_429() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit), 3);

        let mut statuses = Vec::new();
        for _ in 0..4 {
            let response = router
                .clone()
                .oneshot(authorized_post("/mcp", json_body(initialize_request())))
                .await
                .expect("router responds");
            statuses.push(response.status());
        }

        assert_eq!(
            &statuses[0..3],
            &[StatusCode::OK, StatusCode::OK, StatusCode::OK]
        );
        assert_eq!(statuses[3], StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn stream_endpoint_returns_sse_formatted_body() {
        let audit = Arc::new(CountingAuditLogger::default());
        let router = test_router(build_session(audit), 1000);

        let request = Request::builder()
            .method("GET")
            .uri("/mcp/stream")
            .body(Body::empty())
            .expect("request builds");

        let response = router.oneshot(request).await.expect("router responds");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("content-type header present"),
            "text/event-stream"
        );

        let mut body = response.into_body();
        let frame = tokio::time::timeout(Duration::from_secs(5), body.frame())
            .await
            .expect("first sse frame arrives before the timeout")
            .expect("stream yields at least one frame")
            .expect("frame is not an error");
        let bytes = frame.into_data().expect("frame carries data");
        let text = String::from_utf8(bytes.to_vec()).expect("frame is utf8");

        assert!(text.contains("event: ping"));
        assert!(text.contains("data:"));
    }
}
