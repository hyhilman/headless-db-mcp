use thiserror::Error;

/// Coarse classification of a driver error, used by the MCP tool layer to
/// decide how to present a failure (e.g. `Cancelled` is not surfaced as a
/// user-facing failure the way `Query` is).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverErrorKind {
    Connection,
    Auth,
    Query,
    Timeout,
    Cancelled,
    Protocol,
    Internal,
}

/// Uniform error surface across every driver, mirroring the source
/// project's `PluginDriverError` protocol.
///
/// `message` must never contain a credential. Drivers that wrap an
/// underlying client library's error must strip connection strings/DSNs
/// from the message before constructing this (guardrail #2 applies to
/// error paths, not just the happy path).
#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct DriverError {
    pub kind: DriverErrorKind,
    pub message: String,
    pub code: Option<String>,
    pub sql_state: Option<String>,
    pub detail: Option<String>,
}

impl DriverError {
    pub fn new(kind: DriverErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
            sql_state: None,
            detail: None,
        }
    }

    pub fn cancelled() -> Self {
        Self::new(DriverErrorKind::Cancelled, "operation was cancelled")
    }

    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn with_sql_state(mut self, sql_state: impl Into<String>) -> Self {
        self.sql_state = Some(sql_state.into());
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_has_cancelled_kind() {
        let err = DriverError::cancelled();
        assert_eq!(err.kind, DriverErrorKind::Cancelled);
    }

    #[test]
    fn builder_methods_set_optional_fields() {
        let err = DriverError::new(DriverErrorKind::Query, "syntax error")
            .with_code("42601")
            .with_sql_state("42601")
            .with_detail("near \"SELCT\"");
        assert_eq!(err.code.as_deref(), Some("42601"));
        assert_eq!(err.sql_state.as_deref(), Some("42601"));
        assert_eq!(err.detail.as_deref(), Some("near \"SELCT\""));
    }

    #[test]
    fn implements_std_error() {
        fn assert_error<E: std::error::Error>(_: &E) {}
        assert_error(&DriverError::new(DriverErrorKind::Internal, "boom"));
    }
}
