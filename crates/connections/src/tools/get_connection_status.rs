use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct GetConnectionStatusArgs {
    connection_id: String,
}

pub struct GetConnectionStatusTool {
    manager: Arc<ConnectionManager>,
}

impl GetConnectionStatusTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for GetConnectionStatusTool {
    fn name(&self) -> &str {
        "get_connection_status"
    }

    fn description(&self) -> &str {
        "Returns the status of a single connection by id."
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
        let args: GetConnectionStatusArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;

        let status = self
            .manager
            .status(connection_id)
            .map_err(map_manager_error)?;

        serde_json::to_value(status).map_err(|err| {
            McpToolError::Failed(format!("failed to serialize connection status: {err}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    #[tokio::test]
    async fn returns_the_status_of_a_live_connection() {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(MockDriverConfig::default())));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = GetConnectionStatusTool::new(manager);
        let result = tool
            .call(Some(json!({ "connection_id": id.to_string() })))
            .await
            .expect("get_connection_status succeeds");

        assert_eq!(result["connection_id"], json!(id.to_string()));
        assert_eq!(result["database_type"], json!("Mock"));
    }

    #[tokio::test]
    async fn unknown_connection_id_is_failed_not_invalid_arguments() {
        let manager = Arc::new(ConnectionManager::new());
        let tool = GetConnectionStatusTool::new(manager);

        let err = tool
            .call(Some(
                json!({ "connection_id": uuid::Uuid::new_v4().to_string() }),
            ))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::Failed(_)));
    }
}
