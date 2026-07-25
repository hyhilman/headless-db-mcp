use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use db_headless_core::{CellValue, QueryResult, QueryTimeouts};
use db_headless_mcp_server::McpToolError;

use crate::manager::{ConnectionManager, ConnectionManagerError};

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

/// Runs a user-supplied query on a live connection, wrapped in the same
/// client-side backstop timeout and cancellation every query-running tool
/// shares (`execute_query`, `export_query_jsonl`).
///
/// The server-side engine timeout (`apply_query_timeout`, set at connect)
/// is the normal path; this backstop only trips when the connection is not
/// communicating at all, at which point it calls `cancel_query` out of band
/// and returns a clear `Failed` rather than hanging. Factored here so the
/// three tools that run a query cannot drift apart on this behavior.
pub(crate) async fn run_user_query(
    manager: &ConnectionManager,
    connection_id: Uuid,
    query: &str,
    parameters: Option<&[CellValue]>,
    row_cap: Option<usize>,
) -> Result<QueryResult, McpToolError> {
    let driver = manager.get(connection_id).map_err(map_manager_error)?;

    let query_future = driver.execute_user_query(query, row_cap, parameters);
    let backstop = Duration::from_secs(QueryTimeouts::CLIENT_BACKSTOP_SECS);

    match tokio::time::timeout(backstop, query_future).await {
        Ok(query_result) => query_result.map_err(|err| McpToolError::Failed(err.to_string())),
        Err(_elapsed) => {
            if let Err(err) = driver.cancel_query() {
                tracing::warn!(
                    error = %err,
                    "failed to cancel a query that exceeded the client-side backstop timeout"
                );
            }
            Err(McpToolError::Failed(format!(
                "query exceeded the {}s client-side timeout and was cancelled; this connection's link may be unstable, or the query itself may be missing an index",
                QueryTimeouts::CLIENT_BACKSTOP_SECS
            )))
        }
    }
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
