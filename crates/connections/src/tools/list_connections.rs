use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde_json::{json, Value};

use crate::manager::ConnectionManager;

pub struct ListConnectionsTool {
    manager: Arc<ConnectionManager>,
}

impl ListConnectionsTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ListConnectionsTool {
    fn name(&self) -> &str {
        "list_connections"
    }

    fn description(&self) -> &str {
        "Lists every connection currently held open by this server."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        })
    }

    async fn call(&self, _arguments: Option<Value>) -> Result<Value, McpToolError> {
        let connections = self.manager.list();
        let connections = serde_json::to_value(connections).map_err(|err| {
            McpToolError::Failed(format!("failed to serialize connections: {err}"))
        })?;

        Ok(json!({ "connections": connections }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    #[tokio::test]
    async fn lists_open_connections() {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(MockDriverConfig::default())));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ListConnectionsTool::new(manager);
        let result = tool.call(None).await.expect("list_connections succeeds");

        assert_eq!(
            result["connections"][0]["connection_id"],
            json!(id.to_string())
        );
        assert_eq!(result["connections"][0]["database_type"], json!("Mock"));
    }

    #[tokio::test]
    async fn empty_when_nothing_connected() {
        let manager = Arc::new(ConnectionManager::new());
        let tool = ListConnectionsTool::new(manager);

        let result = tool.call(None).await.expect("list_connections succeeds");
        assert_eq!(result["connections"], json!([]));
    }
}
