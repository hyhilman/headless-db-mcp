use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use db_headless_mcp_server::McpToolError;

use crate::manager::ConnectionManagerError;

/// Deserializes a tool's `arguments` into its typed argument struct,
/// mapping both "arguments missing entirely" and "arguments present but
/// don't match the schema" onto `McpToolError::InvalidArguments`.
pub(crate) fn parse_arguments<T: DeserializeOwned>(
    arguments: Option<Value>,
) -> Result<T, McpToolError> {
    let value =
        arguments.ok_or_else(|| McpToolError::InvalidArguments("missing arguments".to_string()))?;
    serde_json::from_value(value).map_err(|err| McpToolError::InvalidArguments(err.to_string()))
}

/// Parses a `connection_id` argument as a `Uuid`. An invalid format is an
/// argument-shape problem, not a lookup failure, so it maps to
/// `InvalidArguments` rather than `Failed`.
pub(crate) fn parse_connection_id(raw: &str) -> Result<Uuid, McpToolError> {
    Uuid::parse_str(raw)
        .map_err(|err| McpToolError::InvalidArguments(format!("invalid connection_id: {err}")))
}

/// Maps a `ConnectionManagerError` onto the right `McpToolError` variant.
///
/// Every arm here is `McpToolError::Failed`: by the time a tool has a
/// `ConnectionManagerError` in hand, the arguments themselves were well
/// formed (a malformed `connection_id` string is caught earlier by
/// `parse_connection_id`) — the operation just did not succeed. Driver
/// error messages are passed through verbatim; `DriverError`'s own
/// contract guarantees they are already free of credentials, so no
/// additional context is added here that might reintroduce a leak.
pub(crate) fn map_manager_error(err: ConnectionManagerError) -> McpToolError {
    match err {
        ConnectionManagerError::NotFound(id) => {
            McpToolError::Failed(format!("no such connection: {id}"))
        }
        ConnectionManagerError::UnknownDatabaseType(database_type) => {
            McpToolError::Failed(format!("unknown database type: {database_type}"))
        }
        ConnectionManagerError::Superseded(id) => McpToolError::Failed(format!(
            "connection attempt for {id} was superseded by a newer attempt"
        )),
        ConnectionManagerError::Driver(driver_error) => {
            McpToolError::Failed(driver_error.to_string())
        }
    }
}
