use std::net::IpAddr;
use std::sync::Arc;

use db_headless_mcp_server::McpSession;
use governor::DefaultKeyedRateLimiter;

/// Shared state handed to every axum handler and middleware in this
/// crate's router. Cheap to clone: everything inside is already behind an
/// `Arc`.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) session: Arc<McpSession>,
    /// The configured bearer token, held only long enough to be compared
    /// in constant time by [`crate::auth`]. Never formatted, never logged.
    pub(crate) bearer_token: Arc<str>,
    pub(crate) limiter: Arc<DefaultKeyedRateLimiter<IpAddr>>,
}
