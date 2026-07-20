use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct ListDatabasesArgs {
    connection_id: String,
}

pub struct ListDatabasesTool {
    manager: Arc<ConnectionManager>,
}

impl ListDatabasesTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ListDatabasesTool {
    fn name(&self) -> &str {
        "list_databases"
    }

    fn description(&self) -> &str {
        "Lists databases visible on a live connection."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "connection_id": { "type": "string" }
            },
            "required": ["connection_id"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ListDatabasesArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;
        let driver = self.manager.get(connection_id).map_err(map_manager_error)?;

        let databases = driver
            .fetch_databases()
            .await
            .map_err(|err| McpToolError::Failed(err.to_string()))?;

        Ok(json!({ "databases": databases }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    #[tokio::test]
    async fn lists_the_databases_the_driver_returns() {
        let config =
            MockDriverConfig::with_databases(vec!["app".to_string(), "app_test".to_string()]);

        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(config)));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ListDatabasesTool::new(manager);
        let result = tool
            .call(Some(json!({ "connection_id": id.to_string() })))
            .await
            .expect("list_databases succeeds");

        assert_eq!(result["databases"], json!(["app", "app_test"]));
    }

    #[tokio::test]
    async fn unknown_connection_id_is_failed() {
        let manager = Arc::new(ConnectionManager::new());
        let tool = ListDatabasesTool::new(manager);

        let err = tool
            .call(Some(
                json!({ "connection_id": uuid::Uuid::new_v4().to_string() }),
            ))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::Failed(_)));
    }
}
