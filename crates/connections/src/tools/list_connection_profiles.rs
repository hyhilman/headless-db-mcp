use std::sync::Arc;

use async_trait::async_trait;
use db_headless_connection_profiles::ConnectionProfileManager;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde_json::{json, Value};

/// Lists saved connection profiles. Never returns a password: profile
/// metadata (`db_headless_connection_profiles::ConnectionProfile`) does
/// not carry one, only a `has_password` flag.
pub struct ListConnectionProfilesTool {
    manager: Arc<ConnectionProfileManager>,
}

impl ListConnectionProfilesTool {
    pub fn new(manager: Arc<ConnectionProfileManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ListConnectionProfilesTool {
    fn name(&self) -> &str {
        "list_connection_profiles"
    }

    fn description(&self) -> &str {
        "Lists saved connection profiles (name, host, database_type, etc). \
         Never includes passwords."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn call(&self, _arguments: Option<Value>) -> Result<Value, McpToolError> {
        let profiles = self
            .manager
            .list()
            .map_err(|err| McpToolError::Failed(err.to_string()))?;
        let profiles = serde_json::to_value(profiles).map_err(|err| {
            McpToolError::Failed(format!("failed to serialize connection profiles: {err}"))
        })?;

        Ok(json!({ "profiles": profiles }))
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

    #[tokio::test]
    async fn lists_saved_profiles_without_passwords() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let manager = Arc::new(ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        ));
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
            })
            .await
            .expect("save succeeds");

        let tool = ListConnectionProfilesTool::new(manager);
        let result = tool.call(None).await.expect("list succeeds");

        assert_eq!(result["profiles"][0]["name"], json!("prod"));
        assert_eq!(result["profiles"][0]["has_password"], json!(true));
        assert!(!result.to_string().contains("hunter2"));
    }

    #[tokio::test]
    async fn empty_when_nothing_saved() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let manager = Arc::new(ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        ));

        let tool = ListConnectionProfilesTool::new(manager);
        let result = tool.call(None).await.expect("list succeeds");
        assert_eq!(result["profiles"], json!([]));
    }
}
