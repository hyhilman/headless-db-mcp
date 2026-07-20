//! Maps transport-level (`reqwest::Error`) and application-level
//! (ClickHouse's own HTTP error responses) failures onto
//! `db_headless_core::DriverError`.
//!
//! Neither path ever has a credential to leak: `reqwest::Error`'s
//! `Display` only ever reports transport state (DNS, connect, timeout,
//! TLS), never the `Authorization` header this driver builds itself, and
//! ClickHouse's own exception text only ever echoes back the query it
//! rejected, never the Basic Auth credentials used to reach it. This is
//! exercised by `connect_failure_with_wrong_password_does_not_leak_password`
//! in `tests/integration.rs`.

use db_headless_core::{DriverError, DriverErrorKind};
use reqwest::StatusCode;

/// Converts a transport-level `reqwest::Error` (the request never got a
/// response at all: DNS failure, connection refused, timed out, TLS
/// handshake failure) into a `DriverError`.
pub fn map_reqwest_error(err: reqwest::Error) -> DriverError {
    let kind = if err.is_timeout() {
        DriverErrorKind::Timeout
    } else if err.is_connect() {
        DriverErrorKind::Connection
    } else {
        DriverErrorKind::Protocol
    };
    DriverError::new(kind, err.to_string())
}

/// Converts a ClickHouse HTTP-level error response (a non-2xx status,
/// whose body is a plain-text `Code: N. DB::Exception: ...` message) into
/// a `DriverError`, extracting the numeric exception code when present.
pub fn map_http_error(status: StatusCode, body: &str) -> DriverError {
    let kind = if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        DriverErrorKind::Auth
    } else {
        DriverErrorKind::Query
    };

    let message = if body.trim().is_empty() {
        format!("ClickHouse request failed with HTTP status {status}")
    } else {
        body.trim().to_string()
    };

    let mut error = DriverError::new(kind, message);
    if let Some(code) = extract_exception_code(body) {
        error = error.with_code(code);
    }
    error
}

/// Pulls the numeric code out of ClickHouse's `Code: 81. DB::Exception: ...`
/// exception text, if the body follows that convention.
fn extract_exception_code(body: &str) -> Option<String> {
    let rest = body.trim().strip_prefix("Code: ")?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        Some(digits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unauthorized_status_maps_to_auth_kind() {
        let err = map_http_error(StatusCode::UNAUTHORIZED, "Authentication failed");
        assert_eq!(err.kind, DriverErrorKind::Auth);
    }

    #[test]
    fn forbidden_status_maps_to_auth_kind() {
        let err = map_http_error(StatusCode::FORBIDDEN, "Access denied");
        assert_eq!(err.kind, DriverErrorKind::Auth);
    }

    #[test]
    fn other_error_status_maps_to_query_kind() {
        let err = map_http_error(
            StatusCode::BAD_REQUEST,
            "Code: 62. DB::Exception: Syntax error",
        );
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn extracts_numeric_exception_code_when_present() {
        let err = map_http_error(
            StatusCode::NOT_FOUND,
            "Code: 81. DB::Exception: Database foo does not exist.",
        );
        assert_eq!(err.code.as_deref(), Some("81"));
    }

    #[test]
    fn missing_exception_code_is_none_not_an_error() {
        let err = map_http_error(StatusCode::BAD_REQUEST, "some other failure text");
        assert_eq!(err.code, None);
    }

    #[test]
    fn empty_body_still_produces_a_readable_message() {
        let err = map_http_error(StatusCode::INTERNAL_SERVER_ERROR, "");
        assert!(err.message.contains("500"));
    }
}
