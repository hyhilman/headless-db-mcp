use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use db_headless_core::{ConnectionConfig, SslConfig, SslMode};
use db_headless_mcp_server::{McpTool, McpToolError};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments};

#[derive(Debug, Deserialize)]
struct ConnectArgs {
    database_type: String,
    host: String,
    port: u16,
    username: String,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    ssl_mode: Option<String>,
}

fn parse_ssl_mode(raw: Option<&str>) -> Result<SslMode, McpToolError> {
    match raw {
        None => Ok(SslMode::VerifyIdentity),
        Some("disabled") => Ok(SslMode::Disabled),
        Some("preferred") => Ok(SslMode::Preferred),
        Some("required") => Ok(SslMode::Required),
        Some("verify_ca") => Ok(SslMode::VerifyCa),
        Some("verify_identity") => Ok(SslMode::VerifyIdentity),
        Some(other) => Err(McpToolError::InvalidArguments(format!(
            "unrecognized ssl_mode: {other}"
        ))),
    }
}

/// Opens a new connection and returns its `connection_id`.
///
/// Phase 2 deliberately accepts the password as a plain call argument.
/// There is no persisted, named-connection store with secret-store-backed
/// credentials yet: `db-headless-secrets` exists as a crate, but wiring
/// it into a named-connection flow (resolve credentials by a stored
/// connection id rather than a caller-supplied password) is a real
/// feature, not just missing polish, and is out of scope for proving the
/// driver + connection-manager loop this phase delivers. Every `connect`
/// call here is a fresh, ephemeral, in-memory connection.
///
/// `ssl_mode` defaults to `verify_identity` when omitted, not
/// `disabled` or `preferred` — downgrading certificate verification must
/// always be an explicit opt-out from the caller, never this tool's
/// default.
pub struct ConnectTool {
    manager: Arc<ConnectionManager>,
}

impl ConnectTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ConnectTool {
    fn name(&self) -> &str {
        "connect"
    }

    fn description(&self) -> &str {
        "Opens a new database connection and returns its connection_id."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "database_type": {
                    "type": "string",
                    "description": "Driver id the connection manager was registered under, e.g. \"PostgreSQL\"."
                },
                "host": { "type": "string" },
                "port": { "type": "integer", "minimum": 0, "maximum": 65535 },
                "username": { "type": "string" },
                "password": { "type": "string" },
                "database": { "type": "string" },
                "ssl_mode": {
                    "type": "string",
                    "enum": ["disabled", "preferred", "required", "verify_ca", "verify_identity"],
                    "description": "Defaults to verify_identity when omitted."
                }
            },
            "required": ["database_type", "host", "port", "username"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ConnectArgs = parse_arguments(arguments)?;
        let ssl_mode = parse_ssl_mode(args.ssl_mode.as_deref())?;

        let config = ConnectionConfig {
            host: args.host,
            port: args.port,
            username: args.username,
            password: args.password.map(SecretString::from),
            database: args.database,
            ssl: SslConfig {
                mode: Some(ssl_mode),
                ca_path: None,
                client_cert_path: None,
                client_key_path: None,
            },
            additional_fields: HashMap::new(),
        };

        let connection_id = self
            .manager
            .connect(&args.database_type, config)
            .await
            .map_err(map_manager_error)?;

        Ok(json!({ "connection_id": connection_id.to_string() }))
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::test_support::{MockDriverConfig, MockFactory};

    fn manager_with_mock() -> Arc<ConnectionManager> {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(MockDriverConfig::default())));
        Arc::new(manager)
    }

    #[tokio::test]
    async fn unrecognized_ssl_mode_is_invalid_arguments_and_never_connects() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(Arc::clone(&manager));

        let err = tool
            .call(Some(json!({
                "database_type": "Mock",
                "host": "localhost",
                "port": 5432,
                "username": "u",
                "ssl_mode": "not-a-real-mode"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::InvalidArguments(_)));
        assert!(manager.list().is_empty());
    }

    #[tokio::test]
    async fn connect_round_trips_to_a_connection_id() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(Arc::clone(&manager));

        let result = tool
            .call(Some(json!({
                "database_type": "Mock",
                "host": "localhost",
                "port": 5432,
                "username": "u",
                "password": "secret"
            })))
            .await
            .expect("connect succeeds");

        let connection_id = result["connection_id"]
            .as_str()
            .expect("connection_id is a string");
        assert!(Uuid::parse_str(connection_id).is_ok());
        assert_eq!(manager.list().len(), 1);
    }

    #[tokio::test]
    async fn missing_required_field_is_invalid_arguments() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(manager);

        let err = tool
            .call(Some(
                json!({ "host": "localhost", "port": 5432, "username": "u" }),
            ))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn unknown_database_type_is_failed_not_invalid_arguments() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(manager);

        let err = tool
            .call(Some(json!({
                "database_type": "DoesNotExist",
                "host": "localhost",
                "port": 5432,
                "username": "u"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::Failed(_)));
    }
}
