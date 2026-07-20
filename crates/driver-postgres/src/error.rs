//! Maps `tokio_postgres::Error` onto `db_headless_core::DriverError`.
//!
//! `tokio_postgres::Error`'s `Display`/`Debug` never echo back connection
//! parameters (the client library only ever reports protocol/server state,
//! not the credentials used to establish the session), so no redaction is
//! needed here beyond simply not constructing a message from anything we
//! were handed at connect time. This is exercised by
//! `tests::connect_failure_wrong_password_does_not_leak_password` in
//! `driver.rs`, which asserts the password never appears anywhere in the
//! resulting `DriverError`.

use db_headless_core::{DriverError, DriverErrorKind};

fn kind_for_sql_state(sql_state: &str) -> DriverErrorKind {
    match sql_state {
        "28000" | "28P01" => DriverErrorKind::Auth,
        "57014" => DriverErrorKind::Cancelled,
        s if s.starts_with("08") => DriverErrorKind::Connection,
        _ => DriverErrorKind::Query,
    }
}

/// Converts a `tokio_postgres::Error` encountered while a connection is
/// (or was expected to be) established into a `DriverError`.
pub fn map_connect_error(err: tokio_postgres::Error) -> DriverError {
    map_error(err, DriverErrorKind::Connection)
}

/// Converts a `tokio_postgres::Error` encountered while running a query
/// into a `DriverError`, classifying it from the server's `SQLSTATE` when
/// one is present.
pub fn map_query_error(err: tokio_postgres::Error) -> DriverError {
    map_error(err, DriverErrorKind::Query)
}

fn map_error(err: tokio_postgres::Error, fallback_kind: DriverErrorKind) -> DriverError {
    let sql_state = err.code().map(|c| c.code().to_string());
    let kind = sql_state
        .as_deref()
        .map(kind_for_sql_state)
        .unwrap_or(fallback_kind);

    if let Some(db_error) = err.as_db_error() {
        let mut driver_error = DriverError::new(kind, db_error.message().to_string());
        if let Some(sql_state) = sql_state {
            driver_error = driver_error
                .with_sql_state(sql_state.clone())
                .with_code(sql_state);
        }
        let mut detail_parts = Vec::new();
        if let Some(detail) = db_error.detail() {
            detail_parts.push(detail.to_string());
        }
        if let Some(hint) = db_error.hint() {
            detail_parts.push(format!("hint: {hint}"));
        }
        if !detail_parts.is_empty() {
            driver_error = driver_error.with_detail(detail_parts.join("; "));
        }
        return driver_error;
    }

    let mut driver_error = DriverError::new(kind, err.to_string());
    if let Some(sql_state) = sql_state {
        driver_error = driver_error
            .with_sql_state(sql_state.clone())
            .with_code(sql_state);
    }
    driver_error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_sql_state_falls_back_to_query_kind() {
        assert_eq!(kind_for_sql_state("42601"), DriverErrorKind::Query);
    }

    #[test]
    fn connection_class_sql_state_maps_to_connection_kind() {
        assert_eq!(kind_for_sql_state("08006"), DriverErrorKind::Connection);
    }

    #[test]
    fn invalid_password_sql_state_maps_to_auth_kind() {
        assert_eq!(kind_for_sql_state("28P01"), DriverErrorKind::Auth);
    }

    #[test]
    fn query_canceled_sql_state_maps_to_cancelled_kind() {
        assert_eq!(kind_for_sql_state("57014"), DriverErrorKind::Cancelled);
    }
}
