use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct DescribeTableArgs {
    connection_id: String,
    table: String,
    #[serde(default)]
    schema: Option<String>,
}

pub struct DescribeTableTool {
    manager: Arc<ConnectionManager>,
}

impl DescribeTableTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for DescribeTableTool {
    fn name(&self) -> &str {
        "describe_table"
    }

    fn description(&self) -> &str {
        "Returns columns, indexes, foreign keys, and triggers for a table."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "connection_id": { "type": "string" },
                "table": { "type": "string" },
                "schema": { "type": "string" }
            },
            "required": ["connection_id", "table"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: DescribeTableArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;
        let driver = self.manager.get(connection_id).map_err(map_manager_error)?;

        let metadata = driver
            .fetch_table_metadata(&args.table, args.schema.as_deref())
            .await
            .map_err(|err| McpToolError::Failed(err.to_string()))?;

        serde_json::to_value(metadata).map_err(|err| {
            McpToolError::Failed(format!("failed to serialize table metadata: {err}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    #[tokio::test]
    async fn describes_the_requested_table() {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(MockDriverConfig::default())));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = DescribeTableTool::new(manager);
        let result = tool
            .call(Some(json!({
                "connection_id": id.to_string(),
                "table": "users",
                "schema": "public"
            })))
            .await
            .expect("describe_table succeeds");

        assert_eq!(result["info"]["name"], json!("users"));
        assert_eq!(result["info"]["schema"], json!("public"));
    }

    #[tokio::test]
    async fn unknown_connection_id_is_failed() {
        let manager = Arc::new(ConnectionManager::new());
        let tool = DescribeTableTool::new(manager);

        let err = tool
            .call(Some(json!({
                "connection_id": uuid::Uuid::new_v4().to_string(),
                "table": "users"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::Failed(_)));
    }
}
