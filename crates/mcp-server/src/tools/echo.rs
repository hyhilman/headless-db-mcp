use async_trait::async_trait;
use serde_json::{json, Value};

use crate::tool::{McpTool, McpToolError};

/// Echoes the `message` argument back. Exists to exercise the
/// `tools/call` argument-passing and validation path end to end.
pub struct EchoTool;

#[async_trait]
impl McpTool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }

    fn description(&self) -> &str {
        "Echoes back the \"message\" argument."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "message": { "type": "string" } },
            "required": ["message"],
            "additionalProperties": false,
        })
    }

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
        let args = arguments
            .ok_or_else(|| McpToolError::InvalidArguments("missing arguments".to_string()))?;
        let message = args.get("message").and_then(Value::as_str).ok_or_else(|| {
            McpToolError::InvalidArguments("\"message\" must be a string".to_string())
        })?;
        Ok(json!({"message": message}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echoes_the_message() {
        let result = EchoTool
            .call(Some(json!({"message": "hi"})))
            .await
            .expect("echo succeeds");
        assert_eq!(result, json!({"message": "hi"}));
    }

    #[tokio::test]
    async fn missing_message_is_invalid_arguments() {
        let err = EchoTool.call(Some(json!({}))).await.unwrap_err();
        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn missing_arguments_entirely_is_invalid_arguments() {
        let err = EchoTool.call(None).await.unwrap_err();
        assert!(matches!(err, McpToolError::InvalidArguments(_)));
    }
}
