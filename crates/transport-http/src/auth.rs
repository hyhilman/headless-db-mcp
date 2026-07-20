//! Bearer-token authentication for every endpoint that reaches
//! [`db_headless_mcp_server::McpSession`].
//!
//! Applied as `axum` middleware in front of `POST /mcp` only (see
//! `crate::router`); `GET /mcp/stream` is a fixed demo stream that never
//! touches `McpSession` (see `crate::sse_demo`) and so is out of scope for
//! this guard.
//!
//! The token comparison uses [`subtle::ConstantTimeEq`] rather than `==`
//! so a client guessing the token cannot learn how many leading bytes it
//! got right from response timing. Neither the presented token nor the
//! configured one is ever logged, on either the success or failure path.

use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

use crate::state::AppState;

const BEARER_PREFIX: &str = "Bearer ";

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    raw.strip_prefix(BEARER_PREFIX)
}

fn tokens_match(provided: &str, expected: &str) -> bool {
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

/// `axum` middleware: rejects with `401 Unauthorized` before the wrapped
/// service (ultimately, `McpSession::handle`) ever runs, unless the
/// request carries `Authorization: Bearer <token>` matching
/// `AppState::bearer_token`.
pub(crate) async fn require_bearer_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    match extract_bearer_token(&headers) {
        Some(token) if tokens_match(token, &state.bearer_token) => next.run(request).await,
        _ => {
            tracing::warn!(
                "http transport: rejected request with a missing or invalid bearer token"
            );
            (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_token_from_a_well_formed_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer secret-token".parse().expect("valid header value"),
        );
        assert_eq!(extract_bearer_token(&headers), Some("secret-token"));
    }

    #[test]
    fn missing_header_yields_no_token() {
        let headers = HeaderMap::new();
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn wrong_scheme_yields_no_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Basic dXNlcjpwYXNz".parse().expect("valid header value"),
        );
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn non_utf8_header_does_not_panic_and_yields_no_token() {
        let mut headers = HeaderMap::new();
        let opaque = axum::http::HeaderValue::from_bytes(&[0xff, 0xfe, 0xfd])
            .expect("HeaderValue permits opaque non-UTF-8 bytes");
        headers.insert(header::AUTHORIZATION, opaque);
        assert_eq!(extract_bearer_token(&headers), None);
    }

    #[test]
    fn tokens_match_requires_exact_equality() {
        assert!(tokens_match("abc", "abc"));
        assert!(!tokens_match("abc", "abcd"));
        assert!(!tokens_match("abc", "xyz"));
        assert!(!tokens_match("", "abc"));
    }
}
