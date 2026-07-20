use std::sync::Arc;

use async_trait::async_trait;
use db_headless_connection_profiles::{ConnectionProfileManager, SaveProfileParams};
use db_headless_core::SslMode;
use db_headless_mcp_server::{McpTool, McpToolError};
use secrecy::SecretString;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::tools::support::parse_arguments;

#[derive(Debug, Deserialize)]
struct SaveConnectionProfileArgs {
    name: String,
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
    #[serde(default)]
    read_only: Option<bool>,
}

/// Mirrors `connect.rs`'s default: a profile saved without an explicit
/// `ssl_mode` stores `verify_identity`, not `None`. Downgrading
/// verification must always be an explicit opt-out (guardrail #6), and
/// storing the default explicitly keeps that true regardless of how the
/// stored `SslMode` is read back later.
fn parse_ssl_mode(raw: Option<&str>) -> Result<Option<SslMode>, McpToolError> {
    match raw {
        None => Ok(Some(SslMode::VerifyIdentity)),
        Some("disabled") => Ok(Some(SslMode::Disabled)),
        Some("preferred") => Ok(Some(SslMode::Preferred)),
        Some("required") => Ok(Some(SslMode::Required)),
        Some("verify_ca") => Ok(Some(SslMode::VerifyCa)),
        Some("verify_identity") => Ok(Some(SslMode::VerifyIdentity)),
        Some(other) => Err(McpToolError::InvalidArguments(format!(
            "unrecognized ssl_mode: {other}"
        ))),
    }
}

/// Saves credentials once, under a name, so future `connect` calls can
/// pass `profile_name` instead of a raw password. See
/// `db_headless_connection_profiles`'s crate doc comment for the full
/// rationale: the password is written straight to the encrypted
/// `SecretStore` and never appears in this tool's response.
///
/// Omitting `password` on an update to an existing profile keeps
/// whatever password is already stored — it does not clear it. There is
/// no separate "clear the password" tool yet; delete and recreate the
/// profile for that. Omitting `read_only` behaves the same way: it keeps
/// whatever was stored (false for a brand new profile), rather than
/// silently turning write access back on on an unrelated update.
pub struct SaveConnectionProfileTool {
    manager: Arc<ConnectionProfileManager>,
}

impl SaveConnectionProfileTool {
    pub fn new(manager: Arc<ConnectionProfileManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for SaveConnectionProfileTool {
    fn name(&self) -> &str {
        "save_connection_profile"
    }

    fn description(&self) -> &str {
        "Saves database credentials once under a name. Use `connect` with \
         profile_name afterward instead of passing the password again."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Unique name for this profile, e.g. \"prod-db\". Saving again with the same name updates it."
                },
                "database_type": {
                    "type": "string",
                    "description": "Driver id the connection manager was registered under, e.g. \"PostgreSQL\"."
                },
                "host": { "type": "string" },
                "port": { "type": "integer", "minimum": 0, "maximum": 65535 },
                "username": { "type": "string" },
                "password": {
                    "type": "string",
                    "description": "Stored encrypted-at-rest. Omit on an update to keep the existing password unchanged."
                },
                "database": { "type": "string" },
                "ssl_mode": {
                    "type": "string",
                    "enum": ["disabled", "preferred", "required", "verify_ca", "verify_identity"],
                    "description": "Defaults to verify_identity when omitted."
                },
                "read_only": {
                    "type": "boolean",
                    "description": "When true, connections opened via this profile reject any write the underlying engine can be made to refuse. Omit on an update to keep the currently stored value unchanged; defaults to false for a brand new profile."
                }
            },
            "required": ["name", "database_type", "host", "port", "username"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: SaveConnectionProfileArgs = parse_arguments(arguments)?;
        let ssl_mode = parse_ssl_mode(args.ssl_mode.as_deref())?;

        self.manager
            .save(SaveProfileParams {
                name: args.name.clone(),
                database_type: args.database_type,
                host: args.host,
                port: args.port,
                username: args.username,
                password: args.password.map(SecretString::from),
                database: args.database,
                ssl_mode,
                read_only: args.read_only,
            })
            .await
            .map_err(|err| McpToolError::Failed(err.to_string()))?;

        Ok(json!({ "name": args.name, "saved": true }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use secrecy::ExposeSecret;

    use super::*;

    struct InMemorySecretStore {
        values: Mutex<HashMap<String, SecretString>>,
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
        ) -> Result<Option<SecretString>, db_headless_secrets::SecretError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        async fn set(
            &self,
            key: &str,
            value: SecretString,
        ) -> Result<(), db_headless_secrets::SecretError> {
            self.values.lock().unwrap().insert(key.to_string(), value);
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<(), db_headless_secrets::SecretError> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn tool_with_temp_store() -> (SaveConnectionProfileTool, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let manager = ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        );
        (SaveConnectionProfileTool::new(Arc::new(manager)), dir)
    }

    #[tokio::test]
    async fn save_never_echoes_password_back() {
        let (tool, _dir) = tool_with_temp_store();

        let result = tool
            .call(Some(json!({
                "name": "prod",
                "database_type": "PostgreSQL",
                "host": "db.internal",
                "port": 5432,
                "username": "app",
                "password": "hunter2"
            })))
            .await
            .expect("save succeeds");

        assert_eq!(result["name"], json!("prod"));
        assert_eq!(result["saved"], json!(true));
        let response_text = result.to_string();
        assert!(!response_text.contains("hunter2"));
    }

    #[tokio::test]
    async fn unrecognized_ssl_mode_is_invalid_arguments() {
        let (tool, _dir) = tool_with_temp_store();

        let err = tool
            .call(Some(json!({
                "name": "prod",
                "database_type": "PostgreSQL",
                "host": "db.internal",
                "port": 5432,
                "username": "app",
                "ssl_mode": "not-a-real-mode"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn saved_password_round_trips_through_the_manager() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let store = Arc::new(InMemorySecretStore::new());
        let manager = Arc::new(ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            store,
        ));
        let tool = SaveConnectionProfileTool::new(Arc::clone(&manager));

        tool.call(Some(json!({
            "name": "prod",
            "database_type": "PostgreSQL",
            "host": "db.internal",
            "port": 5432,
            "username": "app",
            "password": "hunter2"
        })))
        .await
        .expect("save succeeds");

        let resolved = manager.resolve("prod").await.expect("resolve succeeds");
        assert_eq!(
            resolved
                .config
                .password
                .as_ref()
                .map(|s| s.expose_secret().to_string()),
            Some("hunter2".to_string())
        );
    }

    #[tokio::test]
    async fn read_only_true_is_saved_and_resolves_onto_the_config() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let manager = Arc::new(ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        ));
        let tool = SaveConnectionProfileTool::new(Arc::clone(&manager));

        tool.call(Some(json!({
            "name": "prod",
            "database_type": "PostgreSQL",
            "host": "db.internal",
            "port": 5432,
            "username": "app",
            "read_only": true
        })))
        .await
        .expect("save succeeds");

        let resolved = manager.resolve("prod").await.expect("resolve succeeds");
        assert!(resolved.config.read_only);
    }
}
