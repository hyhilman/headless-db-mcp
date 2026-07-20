#![forbid(unsafe_code)]

//! Connection manager and MCP tool surface for `db-headless-mcp`.
//!
//! [`ConnectionManager`] owns the driver-factory registry and the
//! generation-fenced connect/disconnect lifecycle, built on top of
//! `db_headless_registry`'s `ConnectionAttemptRegistry` and
//! `SessionRegistry` — see that crate's module doc comment for why a
//! generation token has to guard every session insert. The `tools`
//! module wraps the manager in `db_headless_mcp_server::McpTool`
//! implementations; see its module doc comment for the Phase 2 tool
//! surface and what is deliberately deferred to a later phase.

mod manager;
#[cfg(test)]
mod test_support;
mod tools;

pub use manager::{ConnectionManager, ConnectionManagerError, ConnectionSummary};
pub use tools::{
    CellValueArg, ConnectTool, DeleteConnectionProfileTool, DescribeTableTool, DisconnectTool,
    ExecuteQueryTool, GetConnectionStatusTool, ListConnectionProfilesTool, ListConnectionsTool,
    ListDatabasesTool, ListSchemasTool, ListTablesTool, SaveConnectionProfileTool,
};
