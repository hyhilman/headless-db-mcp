use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use crate::tool::{McpTool, McpToolError, ToolDescriptor};

/// A tool call was made against a name no `McpTool` was registered under.
/// Kept distinct from [`McpToolError`] so the session layer can map it to
/// `JsonRpcError::METHOD_NOT_FOUND` instead of `INTERNAL_ERROR`.
#[derive(Debug, thiserror::Error)]
#[error("no such tool: {0}")]
pub struct UnknownTool(pub String);

/// The set of tools this server exposes over `tools/list` and `tools/call`.
#[derive(Default)]
pub struct McpToolRegistry {
    tools: HashMap<String, Arc<dyn McpTool>>,
}

impl McpToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, tool: Arc<dyn McpTool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    /// Descriptors sorted by name, so `tools/list` responses are
    /// deterministic regardless of registration order.
    pub fn descriptors(&self) -> Vec<ToolDescriptor> {
        let mut descriptors: Vec<_> = self.tools.values().map(|tool| tool.descriptor()).collect();
        descriptors.sort_by(|a, b| a.name.cmp(&b.name));
        descriptors
    }

    pub async fn call(&self, name: &str, arguments: Option<Value>) -> Result<Value, ToolCallError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| ToolCallError::Unknown(UnknownTool(name.to_string())))?;
        tool.call(arguments).await.map_err(ToolCallError::Failed)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ToolCallError {
    #[error(transparent)]
    Unknown(#[from] UnknownTool),
    #[error(transparent)]
    Failed(#[from] McpToolError),
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;

    struct StubTool;

    #[async_trait]
    impl McpTool for StubTool {
        fn name(&self) -> &str {
            "stub"
        }

        fn description(&self) -> &str {
            "a stub tool"
        }

        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }

        async fn call(&self, _arguments: Option<Value>) -> Result<Value, McpToolError> {
            Ok(json!({"ok": true}))
        }
    }

    #[tokio::test]
    async fn registered_tool_is_callable_by_name() {
        let mut registry = McpToolRegistry::new();
        registry.register(Arc::new(StubTool));

        let result = registry.call("stub", None).await.expect("call succeeds");
        assert_eq!(result, json!({"ok": true}));
    }

    #[tokio::test]
    async fn unregistered_tool_name_is_unknown_not_failed() {
        let registry = McpToolRegistry::new();
        let err = registry.call("does-not-exist", None).await.unwrap_err();
        assert!(matches!(err, ToolCallError::Unknown(_)));
    }

    #[test]
    fn descriptors_are_sorted_by_name() {
        struct AnotherTool;
        #[async_trait]
        impl McpTool for AnotherTool {
            fn name(&self) -> &str {
                "aaa_first"
            }
            fn description(&self) -> &str {
                "sorts before stub"
            }
            fn input_schema(&self) -> Value {
                json!({"type": "object"})
            }
            async fn call(&self, _arguments: Option<Value>) -> Result<Value, McpToolError> {
                Ok(Value::Null)
            }
        }

        let mut registry = McpToolRegistry::new();
        registry.register(Arc::new(StubTool));
        registry.register(Arc::new(AnotherTool));

        let names: Vec<_> = registry.descriptors().into_iter().map(|d| d.name).collect();
        assert_eq!(names, vec!["aaa_first".to_string(), "stub".to_string()]);
    }
}
