//! MCP tools exposing [`crate::ConnectionManager`] to MCP clients.
//!
//! This is a deliberately narrower tool surface than the full
//! migration-plan tool list. `GetTableDdl`, `SwitchDatabase`,
//! `SwitchSchema`, and similar already have a `DatabaseDriver` method
//! backing them (`fetch_table_ddl`, `switch_database`, `switch_schema`)
//! but are not wired up as tools yet — that is a follow-up once the
//! connect/execute/list loop this phase proves out is wired into
//! `crates/server`, not a gap in the driver contract.

mod connect;
mod describe_table;
mod disconnect;
mod execute_query;
mod get_connection_status;
mod list_connections;
mod list_databases;
mod list_schemas;
mod list_tables;
mod support;

pub use connect::ConnectTool;
pub use describe_table::DescribeTableTool;
pub use disconnect::DisconnectTool;
pub use execute_query::{CellValueArg, ExecuteQueryTool};
pub use get_connection_status::GetConnectionStatusTool;
pub use list_connections::ListConnectionsTool;
pub use list_databases::ListDatabasesTool;
pub use list_schemas::ListSchemasTool;
pub use list_tables::ListTablesTool;
