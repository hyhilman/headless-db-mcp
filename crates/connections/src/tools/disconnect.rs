use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct DisconnectArgs {
    connection_id: String,
}

pub struct DisconnectTool {
    manager: Arc<ConnectionManager>,
}

impl DisconnectTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for DisconnectTool {
    fn name(&self) -> &str {
        "disconnect"
    }

    fn description(&self) -> &str {
        "Closes a live connection."
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
        let args: DisconnectArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;

        self.manager
            .disconnect(connection_id)
            .await
            .map_err(map_manager_error)?;

        Ok(json!({ "connection_id": connection_id.to_string(), "disconnected": true }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    fn manager_with_mock() -> Arc<ConnectionManager> {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(MockDriverConfig::default())));
        Arc::new(manager)
    }

    #[tokio::test]
    async fn disconnect_removes_the_connection() {
        let manager = manager_with_mock();
        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = DisconnectTool::new(Arc::clone(&manager));
        let result = tool
            .call(Some(json!({ "connection_id": id.to_string() })))
            .await
            .expect("disconnect succeeds");

        assert_eq!(result["disconnected"], json!(true));
        assert!(manager.get(id).is_err());
        assert!(manager.list().is_empty());
    }

    #[tokio::test]
    async fn malformed_connection_id_is_invalid_arguments() {
        let manager = manager_with_mock();
        let tool = DisconnectTool::new(manager);

        let err = tool
            .call(Some(json!({ "connection_id": "not-a-uuid" })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }
}
