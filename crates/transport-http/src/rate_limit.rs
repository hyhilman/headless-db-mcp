//! Per-source rate limiting, applied to the whole router before auth or
//! `McpSession` ever see a request (cheapest check first).
//!
//! # Design choice: per-IP, not a single global bucket
//!
//! This crate keys the limiter on the client's socket address IP
//! (`governor`'s keyed rate limiter, one GCRA bucket per `IpAddr`) rather
//! than a single global counter for the whole listener. The reason: this
//! check runs *before* auth. A global bucket would let one unauthenticated
//! client exhaust the entire listener's budget and lock out every other
//! caller, including ones with a valid token, just by sending garbage
//! requests as fast as possible. A per-IP bucket bounds the blast radius
//! of that to the offending source.
//!
//! The IP comes from `axum::extract::ConnectInfo<SocketAddr>`, populated
//! by serving the router through
//! `Router::into_make_service_with_connect_info` (see `crate::run_http`).
//! Tests populate it with `axum::extract::connect_info::MockConnectInfo`,
//! axum's documented mechanism for this exact case.
//!
//! Known limitation carried forward rather than solved in this pass: the
//! keyed state store grows one entry per distinct IP seen and is never
//! evicted. Fine for the expected deployment shape (a handful of clients
//! on a loopback or private network); a long-lived listener exposed to
//! many distinct source IPs would want periodic cleanup (`governor`
//! exposes `retain_recent` on its keyed state store for this).

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};

use crate::state::AppState;

/// Builds a per-IP rate limiter allowing `requests_per_minute` requests
/// per source address, with a burst equal to the same figure (so a client
/// well under its budget can still send a short burst rather than being
/// smoothed to a strict one-per-`60/n`-seconds cadence).
///
/// A configured `0` is not representable as `governor`'s `NonZeroU32`
/// burst size; it is clamped up to `1` rather than panicking; the
/// documented behavior for "0 requests per minute" is "practically
/// nothing gets through", which `1` delivers close enough to.
pub(crate) fn build_limiter(requests_per_minute: u32) -> Arc<DefaultKeyedRateLimiter<IpAddr>> {
    let burst = NonZeroU32::new(requests_per_minute).unwrap_or(NonZeroU32::MIN);
    Arc::new(RateLimiter::keyed(Quota::per_minute(burst)))
}

/// `axum` middleware: the outermost layer of the whole router. Returns
/// `429 Too Many Requests` for a source IP over its budget before the
/// request reaches auth or `McpSession`.
pub(crate) async fn enforce_rate_limit(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    match state.limiter.check_key(&addr.ip()) {
        Ok(()) => next.run(request).await,
        Err(_not_until) => {
            tracing::debug!(client_ip = %addr.ip(), "http transport: rate limit exceeded");
            (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_requests_per_minute_does_not_panic() {
        let limiter = build_limiter(0);
        let ip: IpAddr = "127.0.0.1".parse().expect("valid ip");
        // Whatever the outcome, building and checking the limiter must not
        // panic; a `0` config still enforces *some* limit rather than
        // crashing the server.
        let _ = limiter.check_key(&ip);
    }

    #[test]
    fn burst_up_to_the_configured_limit_is_allowed_then_denied() {
        let limiter = build_limiter(3);
        let ip: IpAddr = "127.0.0.1".parse().expect("valid ip");

        assert!(limiter.check_key(&ip).is_ok());
        assert!(limiter.check_key(&ip).is_ok());
        assert!(limiter.check_key(&ip).is_ok());
        assert!(limiter.check_key(&ip).is_err());
    }

    #[test]
    fn different_ips_have_independent_budgets() {
        let limiter = build_limiter(1);
        let a: IpAddr = "127.0.0.1".parse().expect("valid ip");
        let b: IpAddr = "127.0.0.2".parse().expect("valid ip");

        assert!(limiter.check_key(&a).is_ok());
        assert!(limiter.check_key(&a).is_err());
        assert!(limiter.check_key(&b).is_ok());
    }
}
