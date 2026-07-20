use std::sync::Arc;

use async_trait::async_trait;
use db_headless_connection_profiles::{ConnectionProfileManager, ProfileError};
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::tools::support::parse_arguments;

#[derive(Debug, Deserialize)]
struct DeleteConnectionProfileArgs {
    name: String,
}

/// Deletes a saved connection profile's metadata and its stored password.
/// Deleting an unknown name is an error, not a silent no-op — this is an
/// explicit operator action on a named resource, so a typo should be
/// caught rather than swallowed.
pub struct DeleteConnectionProfileTool {
    manager: Arc<ConnectionProfileManager>,
}

impl DeleteConnectionProfileTool {
    pub fn new(manager: Arc<ConnectionProfileManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for DeleteConnectionProfileTool {
    fn name(&self) -> &str {
        "delete_connection_profile"
    }

    fn description(&self) -> &str {
        "Deletes a saved connection profile and its stored password."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" }
            },
            "required": ["name"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: DeleteConnectionProfileArgs = parse_arguments(arguments)?;

        self.manager
            .delete(&args.name)
            .await
            .map_err(|err| match err {
                ProfileError::NotFound { name } => {
                    McpToolError::Failed(format!("no such connection profile: {name}"))
                }
                other => McpToolError::Failed(other.to_string()),
            })?;

        Ok(json!({ "name": args.name, "deleted": true }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use secrecy::SecretString;

    use super::*;
    use db_headless_connection_profiles::SaveProfileParams;

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

    fn manager_in(dir: &tempfile::TempDir) -> Arc<ConnectionProfileManager> {
        Arc::new(ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        ))
    }

    #[tokio::test]
    async fn deletes_an_existing_profile() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);
        manager
            .save(SaveProfileParams {
                name: "prod".to_string(),
                database_type: "PostgreSQL".to_string(),
                host: "db.internal".to_string(),
                port: 5432,
                username: "app".to_string(),
                password: Some(SecretString::from("hunter2".to_string())),
                database: None,
                ssl_mode: None,
                read_only: None,
            })
            .await
            .expect("save succeeds");

        let tool = DeleteConnectionProfileTool::new(Arc::clone(&manager));
        let result = tool
            .call(Some(json!({ "name": "prod" })))
            .await
            .expect("delete succeeds");

        assert_eq!(result["deleted"], json!(true));
        assert!(manager.list().expect("list").is_empty());
    }

    #[tokio::test]
    async fn deleting_unknown_profile_is_an_error() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);
        let tool = DeleteConnectionProfileTool::new(manager);

        let err = tool
            .call(Some(json!({ "name": "does-not-exist" })))
            .await
            .unwrap_err();
        assert!(matches!(err, McpToolError::Failed(_)));
    }
}
