#![forbid(unsafe_code)]

//! Streamable HTTP+SSE MCP transport.
//!
//! Wraps `db_headless_mcp_server::McpSession` behind an authenticated
//! network listener. Unlike the stdio transport (trusted the way any
//! locally-spawned process is), this one accepts input from a real socket,
//! so every request that would reach `McpSession::handle` first passes a
//! per-source-IP rate limit ([`rate_limit`]) and a constant-time
//! bearer-token check ([`auth`]), in that order (cheapest check first).
//!
//! Two endpoints:
//! - `POST /mcp` ([`handlers`]): one JSON-RPC message per request/response,
//!   decoded with `db_headless_mcp_wire::decode_message` directly against
//!   the raw body.
//! - `GET /mcp/stream` ([`sse_demo`]): a fixed demo SSE stream proving the
//!   wire crate's SSE framing works over a real `axum` streaming body.
//!   Placeholder for real per-tool-call streaming once a tool needs it;
//!   see that module's doc comment.
//!
//! See [`HttpTransportConfig`] and [`run_http`] for the entry point.

mod auth;
mod handlers;
mod rate_limit;
mod router;
mod sse_demo;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use db_headless_mcp_server::McpSession;
use thiserror::Error;

/// Configuration for [`run_http`].
///
/// Deliberately has no `Default` impl: `bind_addr` and `bearer_token` must
/// come from whoever constructs the server (the `db-headless-mcp` binary),
/// never assumed by this crate. If a convenience default is ever added
/// here, its bind address must be `127.0.0.1` (loopback), never
/// `0.0.0.0` — this crate never widens its own bind surface.
pub struct HttpTransportConfig {
    /// Address to listen on, passed straight to `TcpListener::bind` with
    /// no rewriting. Logged in full at startup ([`run_http`]), with an
    /// explicit warning if it is not loopback.
    pub bind_addr: SocketAddr,
    /// Shared secret clients must present as `Authorization: Bearer
    /// <token>`. Compared in constant time ([`auth`]); never logged, at
    /// any level, on any path.
    pub bearer_token: String,
    /// Requests allowed per source IP per minute before `429 Too Many
    /// Requests`. See [`rate_limit`] for why this is per-IP rather than
    /// one global counter.
    pub rate_limit_per_minute: u32,
}

impl std::fmt::Debug for HttpTransportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransportConfig")
            .field("bind_addr", &self.bind_addr)
            .field("bearer_token", &"<redacted>")
            .field("rate_limit_per_minute", &self.rate_limit_per_minute)
            .finish()
    }
}

/// Failure modes of [`run_http`]: binding the listener, and the accept
/// loop itself exiting with an I/O error.
#[derive(Debug, Error)]
pub enum HttpTransportError {
    #[error("failed to bind http listener on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("http server exited with an error: {0}")]
    Serve(#[source] std::io::Error),
}

fn log_bind_address(addr: SocketAddr) {
    tracing::info!(%addr, "http transport listening");
    if !addr.ip().is_loopback() {
        tracing::warn!(
            %addr,
            "http transport bound to a non-loopback address; the MCP server is reachable beyond localhost"
        );
    }
}

/// Runs the HTTP+SSE MCP transport until the listener exits.
///
/// Binds `config.bind_addr`, builds the router (`POST /mcp`, `GET
/// /mcp/stream`), and serves it with `axum::serve` using
/// `into_make_service_with_connect_info` so [`rate_limit`]'s per-IP
/// limiter sees real client addresses. Runs until the accept loop itself
/// errors; there is no built-in shutdown signal here, that is the
/// caller's concern (e.g. wiring `axum::serve(..).with_graceful_shutdown`
/// at the call site once one is needed).
pub async fn run_http(
    session: Arc<McpSession>,
    config: HttpTransportConfig,
) -> Result<(), HttpTransportError> {
    log_bind_address(config.bind_addr);

    let app = router::build_router(session, &config);

    let listener = tokio::net::TcpListener::bind(config.bind_addr)
        .await
        .map_err(|source| HttpTransportError::Bind {
            addr: config.bind_addr,
            source,
        })?;

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(HttpTransportError::Serve)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_the_bearer_token() {
        let config = HttpTransportConfig {
            bind_addr: "127.0.0.1:8080".parse().expect("valid addr"),
            bearer_token: "super-secret-value".to_string(),
            rate_limit_per_minute: 60,
        };
        let debug_output = format!("{config:?}");
        assert!(!debug_output.contains("super-secret-value"));
        assert!(debug_output.contains("<redacted>"));
    }
}
