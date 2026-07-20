use std::sync::Arc;

use async_trait::async_trait;
use db_headless_core::CellValue;
use db_headless_mcp_server::{McpTool, McpToolError};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::manager::ConnectionManager;
use crate::tools::support::{map_manager_error, parse_arguments, parse_connection_id};

/// JSON-friendly representation of a single bound query parameter.
///
/// `CellValue` is not the natural JSON shape a human or LLM caller would
/// type, so this tool accepts only plain JSON `null` (-> `CellValue::Null`)
/// or a JSON string (-> `CellValue::Text`). Binary (`CellValue::Bytes`)
/// parameters are not supported through this JSON tool interface in
/// Phase 2: there is no established base64-marker convention for them
/// yet, and no Phase 2 driver needs to bind a binary parameter to prove
/// the connect/execute loop. A future tool revision can add one (e.g.
/// `{"base64": "..."}`) once a driver actually needs it.
pub type CellValueArg = Option<String>;

fn to_cell_value(arg: CellValueArg) -> CellValue {
    match arg {
        Some(text) => CellValue::Text(text),
        None => CellValue::Null,
    }
}

#[derive(Debug, Deserialize)]
struct ExecuteQueryArgs {
    connection_id: String,
    query: String,
    #[serde(default)]
    parameters: Option<Vec<CellValueArg>>,
    #[serde(default)]
    row_cap: Option<usize>,
}

pub struct ExecuteQueryTool {
    manager: Arc<ConnectionManager>,
}

impl ExecuteQueryTool {
    pub fn new(manager: Arc<ConnectionManager>) -> Self {
        Self { manager }
    }
}

#[async_trait]
impl McpTool for ExecuteQueryTool {
    fn name(&self) -> &str {
        "execute_query"
    }

    fn description(&self) -> &str {
        "Executes a SQL query against a live connection and returns the result set."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "connection_id": { "type": "string" },
                "query": { "type": "string" },
                "parameters": {
                    "type": "array",
                    "items": { "type": ["string", "null"] },
                    "description": "Bound parameters in order. null binds SQL NULL; a string binds a text value. Binary parameters are not supported through this tool."
                },
                "row_cap": { "type": "integer", "minimum": 0 }
            },
            "required": ["connection_id", "query"],
            "additionalProperties": false
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args: ExecuteQueryArgs = parse_arguments(arguments)?;
        let connection_id = parse_connection_id(&args.connection_id)?;
        let driver = self.manager.get(connection_id).map_err(map_manager_error)?;

        let parameters: Option<Vec<CellValue>> = args
            .parameters
            .map(|params| params.into_iter().map(to_cell_value).collect());

        let result = driver
            .execute_user_query(&args.query, args.row_cap, parameters.as_deref())
            .await
            .map_err(|err| McpToolError::Failed(err.to_string()))?;

        serde_json::to_value(result)
            .map_err(|err| McpToolError::Failed(format!("failed to serialize query result: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use db_headless_core::QueryResult;

    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    fn manager_with_mock(config: MockDriverConfig) -> Arc<ConnectionManager> {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(config)));
        Arc::new(manager)
    }

    #[tokio::test]
    async fn unknown_connection_id_is_failed_not_invalid_arguments() {
        let manager = manager_with_mock(MockDriverConfig::default());
        let tool = ExecuteQueryTool::new(manager);

        let err = tool
            .call(Some(json!({
                "connection_id": uuid::Uuid::new_v4().to_string(),
                "query": "SELECT 1"
            })))
            .await
            .unwrap_err();

        assert!(matches!(err, McpToolError::Failed(_)));
    }

    #[tokio::test]
    async fn round_trips_a_query_result_end_to_end() {
        let mut canned = QueryResult::empty();
        canned.columns.push("id".to_string());
        canned.column_type_names.push("int4".to_string());
        canned.rows.push(vec![CellValue::Text("1".to_string())]);
        canned.rows_affected = 1;

        let manager = manager_with_mock(MockDriverConfig::with_query_result(canned));
        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        let tool = ExecuteQueryTool::new(manager);
        let result = tool
            .call(Some(json!({
                "connection_id": id.to_string(),
                "query": "SELECT * FROM t WHERE id = ?",
                "parameters": ["1", null],
                "row_cap": 100
            })))
            .await
            .expect("execute_query succeeds");

        assert_eq!(result["columns"], json!(["id"]));
        assert_eq!(result["rows"], json!([[{"kind": "text", "value": "1"}]]));
        assert_eq!(result["rows_affected"], json!(1));
    }
}
