//! Binary entry point: wires the MCP session (tool registry + dispatch)
//! into either the stdio transport or the HTTP+SSE transport.
//!
//! Registers the `ping`/`echo` transport smoke-test tools from Phase 1,
//! plus the database tool surface backed by a
//! `db_headless_connections::ConnectionManager` with PostgreSQL, Redis,
//! and ClickHouse (Phase 2/3) registered as drivers, plus (when
//! `DB_HEADLESS_MASTER_KEY` is set) the connection-profile tool surface
//! backed by a `db_headless_connection_profiles::ConnectionProfileManager`
//! — see `build_profile_manager`.
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
//!
//! Connection profile storage (saved, named credentials — see
//! `db_headless_connection_profiles`'s crate docs) is a separate opt-in,
//! available on both transports:
//!
//! - `DB_HEADLESS_MASTER_KEY` (optional): 64 hex characters (32 bytes),
//!   the AES-256-GCM key protecting stored passwords. Unset means
//!   profile storage is disabled entirely (`save_connection_profile`,
//!   `list_connection_profiles`, `delete_connection_profile`, and
//!   `connect`'s `profile_name` argument are not registered) — the
//!   ad-hoc `connect` path keeps working either way. Set but malformed
//!   is a hard startup error, never a silent fallback. This key must
//!   stay stable across restarts: losing it makes every stored password
//!   unrecoverable.
//! - `DB_HEADLESS_DATA_DIR` (optional, default `.`): directory holding
//!   `secrets.json` (encrypted) and `profiles.json` (metadata only, no
//!   passwords).

use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use db_headless_connection_profiles::ConnectionProfileManager;
use db_headless_connections::{
    ConnectTool, ConnectionManager, DeleteConnectionProfileTool, DescribeTableTool, DisconnectTool,
    ExecuteQueryTool, GetConnectionStatusTool, ListConnectionProfilesTool, ListConnectionsTool,
    ListDatabasesTool, ListSchemasTool, ListTablesTool, SaveConnectionProfileTool,
};
use db_headless_driver_clickhouse::ClickHouseDriverFactory;
use db_headless_driver_postgres::PostgresDriverFactory;
use db_headless_driver_redis::RedisDriverFactory;
use db_headless_mcp_server::{EchoTool, McpSession, McpToolRegistry, PingTool, TracingAuditLogger};
use db_headless_secrets::EncryptedFileSecretStore;
use db_headless_transport_http::{run_http, HttpTransportConfig};
use db_headless_transport_stdio::run_stdio;

const DEFAULT_HTTP_BIND: &str = "127.0.0.1:8787";
const DEFAULT_RATE_LIMIT_PER_MINUTE: u32 = 120;
const MASTER_KEY_ENV_VAR: &str = "DB_HEADLESS_MASTER_KEY";
const DATA_DIR_ENV_VAR: &str = "DB_HEADLESS_DATA_DIR";
const DEFAULT_DATA_DIR: &str = ".";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let use_http = std::env::args().any(|arg| arg == "--http");

    let profiles = match build_profile_manager() {
        Ok(profiles) => profiles,
        Err(message) => {
            tracing::error!("{message}");
            return ExitCode::FAILURE;
        }
    };
    let session = Arc::new(build_session(profiles));

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

fn build_session(profiles: Option<Arc<ConnectionProfileManager>>) -> McpSession {
    let mut connection_manager = ConnectionManager::new();
    connection_manager.register_driver_factory(
        db_headless_driver_postgres::DATABASE_TYPE_ID,
        Arc::new(PostgresDriverFactory),
    );
    connection_manager.register_driver_factory(
        db_headless_driver_redis::DATABASE_TYPE_ID,
        Arc::new(RedisDriverFactory),
    );
    connection_manager.register_driver_factory(
        db_headless_driver_clickhouse::DATABASE_TYPE_ID,
        Arc::new(ClickHouseDriverFactory),
    );
    let connection_manager = Arc::new(connection_manager);

    let mut registry = McpToolRegistry::new();
    registry.register(Arc::new(PingTool));
    registry.register(Arc::new(EchoTool));

    let connect_tool = match &profiles {
        Some(profiles) => {
            ConnectTool::with_profiles(connection_manager.clone(), Arc::clone(profiles))
        }
        None => ConnectTool::new(connection_manager.clone()),
    };
    registry.register(Arc::new(connect_tool));

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

    if let Some(profiles) = profiles {
        registry.register(Arc::new(SaveConnectionProfileTool::new(Arc::clone(
            &profiles,
        ))));
        registry.register(Arc::new(ListConnectionProfilesTool::new(Arc::clone(
            &profiles,
        ))));
        registry.register(Arc::new(DeleteConnectionProfileTool::new(profiles)));
    }

    McpSession::new(Arc::new(registry), Arc::new(TracingAuditLogger))
}

/// Builds the connection-profile manager when the operator has opted in
/// via `DB_HEADLESS_MASTER_KEY`. A missing key disables profile storage
/// entirely (`Ok(None)`) rather than falling back to some default key —
/// the ad-hoc `connect` path (raw credentials per call) is unaffected. A
/// *present but malformed* key is `Err`, a hard startup failure: silently
/// treating a typo'd key as "disabled" would hide a real misconfiguration
/// from the operator instead of failing loudly.
fn build_profile_manager() -> Result<Option<Arc<ConnectionProfileManager>>, String> {
    if std::env::var(MASTER_KEY_ENV_VAR).is_err() {
        tracing::info!(
            "{MASTER_KEY_ENV_VAR} not set: connection profile storage is disabled \
             (save_connection_profile, list_connection_profiles, delete_connection_profile, \
             and connect's profile_name argument are unavailable)"
        );
        return Ok(None);
    }

    let data_dir = std::env::var(DATA_DIR_ENV_VAR).unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    let secrets_path = Path::new(&data_dir).join("secrets.json");
    let profiles_path = Path::new(&data_dir).join("profiles.json");

    let secret_store = EncryptedFileSecretStore::from_env(secrets_path).map_err(|error| {
        format!("failed to initialize connection profile secret store: {error}")
    })?;

    Ok(Some(Arc::new(ConnectionProfileManager::new(
        profiles_path,
        Arc::new(secret_store),
    ))))
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
