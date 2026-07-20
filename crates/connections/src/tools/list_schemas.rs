use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct ListSchemasArgs {
    connection_id: String,
}

pub struct ListSchemasTool {
    manager: Arc<ConnectionManager>,
}

impl ListSchemasTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ListSchemasTool {
    fn name(&self) -> &str {
        "list_schemas"
    }

    fn description(&self) -> &str {
        "Lists schemas visible on a live connection. Returns an empty list for a driver that does not support schemas."
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
        let args: ListSchemasArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;
        let driver = self.manager.get(connection_id).map_err(map_manager_error)?;

        if !driver.supports_schemas() {
            return Ok(json!({ "schemas": Vec::<String>::new() }));
        }

        let schemas = driver
            .fetch_schemas()
            .await
            .map_err(|err| McpToolError::Failed(err.to_string()))?;

        Ok(json!({ "schemas": schemas }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    #[tokio::test]
    async fn returns_empty_when_driver_does_not_support_schemas() {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(MockDriverConfig::default())));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ListSchemasTool::new(manager);
        let result = tool
            .call(Some(json!({ "connection_id": id.to_string() })))
            .await
            .expect("list_schemas succeeds");

        assert_eq!(result["schemas"], json!([]));
    }

    #[tokio::test]
    async fn returns_the_schemas_the_driver_reports() {
        let config = MockDriverConfig::with_schemas(vec!["public".to_string()]);

        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(config)));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ListSchemasTool::new(manager);
        let result = tool
            .call(Some(json!({ "connection_id": id.to_string() })))
            .await
            .expect("list_schemas succeeds");

        assert_eq!(result["schemas"], json!(["public"]));
    }
}
