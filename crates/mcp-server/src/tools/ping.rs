use async_trait::async_trait;
use serde_json::{json, Value};

use crate::tool::{McpTool, McpToolError};

/// Health check. Returns server identity, nothing else.
pub struct PingTool;

#[async_trait]
impl McpTool for PingTool {
    fn name(&self) -> &str {
        "ping"
    }

    fn description(&self) -> &str {
        "Health check; returns \"pong\" and the server version."
    }

    fn input_schema(&self) -> Value {
        json!({"type": "object", "properties": {}, "additionalProperties": false})
    }

    async fn call(&self, _arguments: Option<Value>) -> Result<Value, McpToolError> {
        Ok(json!({"status": "pong", "version": env!("CARGO_PKG_VERSION")}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_pong() {
        let result = PingTool.call(None).await.expect("ping succeeds");
        assert_eq!(result["status"], "pong");
    }
}
