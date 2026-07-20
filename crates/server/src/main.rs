//! Binary entry point: wires the MCP session (tool registry + dispatch)
//! into either the stdio transport or the HTTP+SSE transport.
//!
//! Registers the `ping`/`echo` transport smoke-test tools from Phase 1,
//! plus the Phase 2 database tool surface backed by a
//! `db_headless_connections::ConnectionManager` with PostgreSQL
//! (`db_headless_driver_postgres`) as its only registered driver so far.
//! `GetTableDdl`/`SwitchDatabase`/`SwitchSchema` and beyond are deferred;
//! see `db_headless_connections`'s crate docs for the current tool list.
//!
//! ## Usage
//!
//! ```text
//! db-headless-mcp                 # stdio transport (default)
//! db-headless-mcp --http          # HTTP+SSE transport
//! ```
//!
//! HTTP mode reads configuration from the environment, never from a
//! built-in default that could ship an unauthenticated or non-loopback
//! listener by accident:
//!
//! - `DB_HEADLESS_MCP_TOKEN` (required): bearer token clients must
//!   present. Starting `--http` without this set is a hard error, not a
//!   fallback to no auth.
//! - `DB_HEADLESS_MCP_BIND` (optional, default `127.0.0.1:8787`): listen
//!   address. A non-loopback value is accepted but logged loudly by
//!   `db-headless-transport-http` at startup.
//! - `DB_HEADLESS_MCP_RATE_LIMIT` (optional, default `120`): requests per
//!   source IP per minute before `429`.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use db_headless_connections::{
    ConnectTool, ConnectionManager, DescribeTableTool, DisconnectTool, ExecuteQueryTool,
    GetConnectionStatusTool, ListConnectionsTool, ListDatabasesTool, ListSchemasTool,
    ListTablesTool,
};
use db_headless_driver_postgres::PostgresDriverFactory;
use db_headless_mcp_server::{EchoTool, McpSession, McpToolRegistry, PingTool, TracingAuditLogger};
use db_headless_transport_http::{run_http, HttpTransportConfig};
use db_headless_transport_stdio::run_stdio;

const DEFAULT_HTTP_BIND: &str = "127.0.0.1:8787";
const DEFAULT_RATE_LIMIT_PER_MINUTE: u32 = 120;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let use_http = std::env::args().any(|arg| arg == "--http");
    let session = Arc::new(build_session());

    if use_http {
        let config = match build_http_config() {
            Ok(config) => config,
            Err(message) => {
                tracing::error!("{message}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = run_http(session, config).await {
            tracing::error!(%error, "http transport exited with an error");
            return ExitCode::FAILURE;
        }
    } else if let Err(error) = run_stdio(session).await {
        tracing::error!(%error, "stdio transport exited with an error");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

fn build_session() -> McpSession {
    let mut connection_manager = ConnectionManager::new();
    connection_manager.register_driver_factory(
        db_headless_driver_postgres::DATABASE_TYPE_ID,
        Arc::new(PostgresDriverFactory),
    );
    let connection_manager = Arc::new(connection_manager);

    let mut registry = McpToolRegistry::new();
    registry.register(Arc::new(PingTool));
    registry.register(Arc::new(EchoTool));
    registry.register(Arc::new(ConnectTool::new(connection_manager.clone())));
    registry.register(Arc::new(DisconnectTool::new(connection_manager.clone())));
    registry.register(Arc::new(ExecuteQueryTool::new(connection_manager.clone())));
    registry.register(Arc::new(ListDatabasesTool::new(connection_manager.clone())));
    registry.register(Arc::new(ListSchemasTool::new(connection_manager.clone())));
    registry.register(Arc::new(ListTablesTool::new(connection_manager.clone())));
    registry.register(Arc::new(DescribeTableTool::new(connection_manager.clone())));
    registry.register(Arc::new(ListConnectionsTool::new(
        connection_manager.clone(),
    )));
    registry.register(Arc::new(GetConnectionStatusTool::new(connection_manager)));

    McpSession::new(Arc::new(registry), Arc::new(TracingAuditLogger))
}

/// Reads HTTP transport config from the environment. Never falls back to
/// a default bearer token — a missing `DB_HEADLESS_MCP_TOKEN` is a
/// startup error, not an unauthenticated server.
fn build_http_config() -> Result<HttpTransportConfig, String> {
    let bearer_token = std::env::var("DB_HEADLESS_MCP_TOKEN").map_err(|_| {
        "DB_HEADLESS_MCP_TOKEN must be set to run the HTTP transport; refusing to start \
         an unauthenticated server"
            .to_string()
    })?;
    if bearer_token.is_empty() {
        return Err("DB_HEADLESS_MCP_TOKEN must not be empty".to_string());
    }

    let bind_addr = match std::env::var("DB_HEADLESS_MCP_BIND") {
        Ok(value) => value.parse::<SocketAddr>().map_err(|error| {
            format!("DB_HEADLESS_MCP_BIND={value:?} is not a valid address: {error}")
        })?,
        Err(_) => DEFAULT_HTTP_BIND
            .parse()
            .expect("DEFAULT_HTTP_BIND is a valid socket address"),
    };

    let rate_limit_per_minute = match std::env::var("DB_HEADLESS_MCP_RATE_LIMIT") {
        Ok(value) => value.parse::<u32>().map_err(|error| {
            format!("DB_HEADLESS_MCP_RATE_LIMIT={value:?} is not a valid number: {error}")
        })?,
        Err(_) => DEFAULT_RATE_LIMIT_PER_MINUTE,
    };

    Ok(HttpTransportConfig {
        bind_addr,
        bearer_token,
        rate_limit_per_minute,
    })
}
