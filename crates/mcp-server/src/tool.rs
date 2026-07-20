use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// Why a tool call failed, distinct enough for the session layer to map
/// each variant onto the right JSON-RPC error code without guessing.
#[derive(Debug, Error)]
pub enum McpToolError {
    /// The caller supplied arguments that don't match `input_schema`
    /// (missing required field, wrong type, ...). Maps to
    /// `JsonRpcError::INVALID_PARAMS`.
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    /// The tool ran but the underlying operation failed (e.g. a database
    /// error once real drivers exist). Maps to
    /// `JsonRpcError::INTERNAL_ERROR`.
    ///
    /// The message here is returned to the MCP client verbatim — tool
    /// authors are responsible for keeping it free of credentials,
    /// connection strings, or other secrets (guardrail #2 applies to
    /// error paths, not just the happy path).
    #[error("{0}")]
    Failed(String),
}

/// Describes a tool for the `tools/list` response. `input_schema` is a
/// JSON Schema object describing the shape `call`'s `arguments` must
/// satisfy.
#[derive(Debug, Clone, Serialize)]
pub struct ToolDescriptor {
    pub name: String,
    pub description: String,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// A single MCP tool. Implementations must validate `arguments` against
/// their own `input_schema` themselves — nothing upstream does it for
/// them.
#[async_trait]
pub trait McpTool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn input_schema(&self) -> Value;

    async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError>;

    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: self.name().to_string(),
            description: self.description().to_string(),
            input_schema: self.input_schema(),
        }
    }
}
