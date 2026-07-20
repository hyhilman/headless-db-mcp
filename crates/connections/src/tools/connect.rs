use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use db_headless_connection_profiles::ConnectionProfileManager;
use db_headless_core::{ConnectionConfig, SslConfig, SslMode};
use db_headless_mcp_server::{McpTool, McpToolError};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments};

#[derive(Debug, Deserialize)]
struct ConnectArgs {
    #[serde(default)]
    profile_name: Option<String>,
    #[serde(default)]
    database_type: Option<String>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    ssl_mode: Option<String>,
    #[serde(default)]
    read_only: Option<bool>,
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
/// Two mutually exclusive ways to call this: pass `profile_name` to
/// connect with credentials saved earlier via `save_connection_profile`
/// (the password never appears in this call at all), or pass
/// `database_type`/`host`/`port`/`username`/`password` directly for a
/// one-off, ephemeral connection the way Phase 2 originally worked.
/// Mixing the two in one call is rejected rather than silently picking
/// one, since a caller that sets both almost certainly has a bug.
///
/// `profiles` is `None` when the server was started without
/// `DB_HEADLESS_MASTER_KEY` — `ConnectionProfileManager` needs a working
/// `SecretStore`, which needs that key, so profile storage is an opt-in
/// feature rather than a hard requirement for the ad-hoc connect path.
///
/// `ssl_mode` defaults to `verify_identity` when omitted, not
/// `disabled` or `preferred` — downgrading certificate verification must
/// always be an explicit opt-out from the caller, never this tool's
/// default.
pub struct ConnectTool {
    manager: Arc<ConnectionManager>,
    profiles: Option<Arc<ConnectionProfileManager>>,
}

impl ConnectTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self {
            manager,
            profiles: None,
        }
    }

    pub fn with_profiles(
        manager: Arc<ConnectionManager>,
        profiles: Arc<ConnectionProfileManager>,
    ) -> Self {
        Self {
            manager,
            profiles: Some(profiles),
        }
    }
}

#[async_trait]
impl McpTool for ConnectTool {
    fn name(&self) -> &str {
        "connect"
    }

    fn description(&self) -> &str {
        "Opens a new database connection and returns its connection_id. \
         Pass profile_name to use credentials saved via \
         save_connection_profile, or pass database_type/host/port/username/\
         password directly for a one-off connection."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "profile_name": {
                    "type": "string",
                    "description": "Name of a profile saved via save_connection_profile. Cannot be combined with database_type/host/port/username/password/database."
                },
                "database_type": {
                    "type": "string",
                    "description": "Driver id the connection manager was registered under, e.g. \"PostgreSQL\". Required when profile_name is not given."
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
                },
                "read_only": {
                    "type": "boolean",
                    "description": "When true, the connection rejects any write the underlying engine can be made to refuse. Only applies to the ad-hoc credential path; a profile_name connection uses the read_only value saved on that profile. Defaults to false."
                }
            },
            "oneOf": [
                { "required": ["profile_name"] },
                { "required": ["database_type", "host", "port", "username"] }
            ],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ConnectArgs = parse_arguments(arguments)?;

        let (database_type, config) = match args.profile_name {
            Some(profile_name) => {
                if args.database_type.is_some()
                    || args.host.is_some()
                    || args.port.is_some()
                    || args.username.is_some()
                    || args.password.is_some()
                    || args.database.is_some()
                    || args.ssl_mode.is_some()
                    || args.read_only.is_some()
                {
                    return Err(McpToolError::InvalidArguments(
                        "profile_name cannot be combined with database_type/host/port/username/\
                         password/database/ssl_mode/read_only"
                            .to_string(),
                    ));
                }

                let profiles = self.profiles.as_ref().ok_or_else(|| {
                    McpToolError::Failed(
                        "connection profiles are not enabled on this server (set \
                         DB_HEADLESS_MASTER_KEY to enable save_connection_profile)"
                            .to_string(),
                    )
                })?;

                let resolved = profiles
                    .resolve(&profile_name)
                    .await
                    .map_err(|err| McpToolError::Failed(err.to_string()))?;
                (resolved.database_type, resolved.config)
            }
            None => {
                let database_type = args.database_type.ok_or_else(|| {
                    McpToolError::InvalidArguments(
                        "database_type is required when profile_name is not given".to_string(),
                    )
                })?;
                let host = args.host.ok_or_else(|| {
                    McpToolError::InvalidArguments(
                        "host is required when profile_name is not given".to_string(),
                    )
                })?;
                let port = args.port.ok_or_else(|| {
                    McpToolError::InvalidArguments(
                        "port is required when profile_name is not given".to_string(),
                    )
                })?;
                let username = args.username.ok_or_else(|| {
                    McpToolError::InvalidArguments(
                        "username is required when profile_name is not given".to_string(),
                    )
                })?;
                let ssl_mode = parse_ssl_mode(args.ssl_mode.as_deref())?;

                let config = ConnectionConfig {
                    host,
                    port,
                    username,
                    password: args.password.map(SecretString::from),
                    database: args.database,
                    ssl: SslConfig {
                        mode: Some(ssl_mode),
                        ca_path: None,
                        client_cert_path: None,
                        client_key_path: None,
                    },
                    read_only: args.read_only.unwrap_or(false),
                    additional_fields: HashMap::new(),
                };
                (database_type, config)
            }
        };

        let connection_id = self
            .manager
            .connect(&database_type, config)
            .await
            .map_err(map_manager_error)?;

        Ok(json!({ "connection_id": connection_id.to_string() }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use secrecy::SecretString as SecretStringForTest;
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

    #[tokio::test]
    async fn profile_name_without_a_configured_profile_manager_is_a_clear_error() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(manager);

        let err = tool
            .call(Some(json!({ "profile_name": "prod" })))
            .await
            .unwrap_err();

        assert!(
            matches!(err, McpToolError::Failed(message) if message.contains("DB_HEADLESS_MASTER_KEY"))
        );
    }

    #[tokio::test]
    async fn profile_name_combined_with_read_only_is_invalid_arguments() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(manager);

        let err = tool
            .call(Some(json!({
                "profile_name": "prod",
                "read_only": true
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn profile_name_combined_with_host_is_invalid_arguments() {
        let manager = manager_with_mock();
        let tool = ConnectTool::new(manager);

        let err = tool
            .call(Some(json!({
                "profile_name": "prod",
                "host": "localhost"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }

    struct InMemorySecretStore {
        values: Mutex<HashMap<String, SecretStringForTest>>,
    }

    impl InMemorySecretStore {
        fn new() -> Self {
            Self {
                values: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl db_headless_secrets::SecretStore for InMemorySecretStore {
        async fn get(
            &self,
            key: &str,
        ) -> Result<Option<SecretStringForTest>, db_headless_secrets::SecretError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        async fn set(
            &self,
            key: &str,
            value: SecretStringForTest,
        ) -> Result<(), db_headless_secrets::SecretError> {
            self.values.lock().unwrap().insert(key.to_string(), value);
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<(), db_headless_secrets::SecretError> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
    }

    #[tokio::test]
    async fn connect_via_profile_name_resolves_credentials_and_connects() {
        let manager = manager_with_mock();
        let dir = tempfile::TempDir::new().expect("tempdir");
        let profile_manager = Arc::new(ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        ));
        profile_manager
            .save(db_headless_connection_profiles::SaveProfileParams {
                name: "prod".to_string(),
                database_type: "Mock".to_string(),
                host: "localhost".to_string(),
                port: 5432,
                username: "u".to_string(),
                password: Some(SecretStringForTest::from("secret".to_string())),
                database: None,
                ssl_mode: None,
                read_only: None,
            })
            .await
            .expect("save succeeds");

        let tool = ConnectTool::with_profiles(Arc::clone(&manager), profile_manager);

        let result = tool
            .call(Some(json!({ "profile_name": "prod" })))
            .await
            .expect("connect via profile succeeds");

        assert!(result["connection_id"].as_str().is_some());
        assert_eq!(manager.list().len(), 1);
    }
}
