//! Maps `redis::RedisError` onto `db_headless_core::DriverError`.
//!
//! `redis::RedisError`'s `Display`/`Debug` (used verbatim below via
//! `err.to_string()`) never echo back connection parameters: the
//! variants that carry a message (`WithDescription`,
//! `WithDescriptionAndDetail`, `ExtensionError`) are all built from a
//! server reply or a client-side classification, never from the
//! `ConnectionInfo`/password this driver supplied at connect time. This
//! is exercised by `connect_failure_with_wrong_password_does_not_leak_password`
//! in `tests/integration.rs`, which asserts the password never appears
//! anywhere in the resulting `DriverError`.

use db_headless_core::{DriverError, DriverErrorKind};

/// Converts a `redis::RedisError` encountered while establishing a
/// connection into a `DriverError`, defaulting unclassified failures to
/// `Connection` (the failure happened before any command could run).
pub fn map_connect_error(err: redis::RedisError) -> DriverError {
    map_error(err, DriverErrorKind::Connection)
}

/// Converts a `redis::RedisError` encountered while running a command
/// into a `DriverError`, defaulting unclassified failures to `Query`.
pub fn map_query_error(err: redis::RedisError) -> DriverError {
    map_error(err, DriverErrorKind::Query)
}

fn map_error(err: redis::RedisError, fallback_kind: DriverErrorKind) -> DriverError {
    let kind = match err.kind() {
        redis::ErrorKind::AuthenticationFailed => DriverErrorKind::Auth,
        redis::ErrorKind::IoError => DriverErrorKind::Connection,
        _ => fallback_kind,
    };

    let mut driver_error = DriverError::new(kind, err.to_string());
    if let Some(code) = err.code() {
        driver_error = driver_error.with_code(code.to_string());
    }
    if let Some(detail) = err.detail() {
        driver_error = driver_error.with_detail(detail.to_string());
    }
    driver_error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authentication_failed_maps_to_auth_kind() {
        let err: redis::RedisError = (
            redis::ErrorKind::AuthenticationFailed,
            "Password authentication failed",
        )
            .into();
        assert_eq!(map_query_error(err).kind, DriverErrorKind::Auth);
    }

    #[test]
    fn io_error_maps_to_connection_kind() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err: redis::RedisError = io_err.into();
        assert_eq!(map_query_error(err).kind, DriverErrorKind::Connection);
    }

    #[test]
    fn unclassified_error_falls_back_to_the_caller_supplied_kind_for_query() {
        let err: redis::RedisError = (redis::ErrorKind::ResponseError, "boom").into();
        assert_eq!(map_query_error(err).kind, DriverErrorKind::Query);
    }

    #[test]
    fn unclassified_error_falls_back_to_the_caller_supplied_kind_for_connect() {
        let err: redis::RedisError = (redis::ErrorKind::ResponseError, "boom").into();
        assert_eq!(map_connect_error(err).kind, DriverErrorKind::Connection);
    }

    #[test]
    fn code_and_detail_are_carried_through_when_present() {
        let err: redis::RedisError = (
            redis::ErrorKind::ExecAbortError,
            "aborted",
            "transaction discarded".to_string(),
        )
            .into();
        let mapped = map_query_error(err);
        assert_eq!(mapped.code.as_deref(), Some("EXECABORT"));
        assert_eq!(mapped.detail.as_deref(), Some("transaction discarded"));
    }
}
