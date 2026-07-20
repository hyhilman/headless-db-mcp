use std::sync::Arc;

use async_trait::async_trait;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

#[derive(Debug, Deserialize)]
struct ListTablesArgs {
    connection_id: String,
    #[serde(default)]
    schema: Option<String>,
}

pub struct ListTablesTool {
    manager: Arc<ConnectionManager>,
}

impl ListTablesTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ListTablesTool {
    fn name(&self) -> &str {
        "list_tables"
    }

    fn description(&self) -> &str {
        "Lists tables (and views) visible on a live connection, optionally scoped to a schema."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "connection_id": { "type": "string" },
                "schema": { "type": "string" }
            },
            "required": ["connection_id"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ListTablesArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;
        let driver = self.manager.get(connection_id).map_err(map_manager_error)?;

        let tables = driver
            .fetch_tables(args.schema.as_deref())
            .await
            .map_err(|err| McpToolError::Failed(err.to_string()))?;

        let tables = serde_json::to_value(tables)
            .map_err(|err| McpToolError::Failed(format!("failed to serialize tables: {err}")))?;

        Ok(json!({ "tables": tables }))
    }
}

#[cfg(test)]
mod tests {
    use db_headless_core::{TableInfo, TableKind};

    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    #[tokio::test]
    async fn lists_the_tables_the_driver_returns() {
        let config = MockDriverConfig::with_tables(vec![TableInfo {
            name: "users".to_string(),
            schema: Some("public".to_string()),
            kind: TableKind::Table,
            comment: None,
            row_count_estimate: None,
        }]);

        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(config)));
        let manager = Arc::new(manager);

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ListTablesTool::new(manager);
        let result = tool
            .call(Some(
                json!({ "connection_id": id.to_string(), "schema": "public" }),
            ))
            .await
            .expect("list_tables succeeds");

        assert_eq!(result["tables"][0]["name"], json!("users"));
    }
}
