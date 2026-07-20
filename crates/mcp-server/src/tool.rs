use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// Why a tool call failed, distinct enough for the session layer to map
/// each variant into the `tools/call` result correctly. Both variants are
/// reported inside a [`CallToolResult`] with `isError: true`, per the MCP
/// spec, so the calling model actually sees the message instead of the
/// call silently returning nothing.
#[derive(Debug, Error)]
pub enum McpToolError {
    /// The caller supplied arguments that don't match `input_schema`
    /// (missing required field, wrong type, ...).
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    /// The tool ran but the underlying operation failed (e.g. a database
    /// error once real drivers exist).
    ///
    /// The message here is returned to the MCP client verbatim — tool
    /// authors are responsible for keeping it free of credentials,
    /// connection strings, or other secrets (guardrail #2 applies to
    /// error paths, not just the happy path).
    #[error("{0}")]
    Failed(String),
}

/// One block of a `tools/call` result's `content` array. Only the `text`
/// variant exists here — no tool in this workspace returns images or
/// embedded resources.
#[derive(Debug, Clone, Serialize)]
pub struct ContentBlock {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub text: String,
}

impl ContentBlock {
    fn text(text: String) -> Self {
        Self { kind: "text", text }
    }
}

/// The MCP spec's actual `tools/call` result shape. A bare tool `Value`
/// returned as the JSON-RPC `result` looks reasonable in raw JSON-RPC but
/// renders as nothing in a real MCP client (Claude Code included) — every
/// client reads `result.content`, not `result` itself. `structured_content`
/// carries the same data untouched for clients that read it, but `content`
/// is what every client is guaranteed to render.
#[derive(Debug, Clone, Serialize)]
pub struct CallToolResult {
    pub content: Vec<ContentBlock>,
    #[serde(rename = "isError")]
    pub is_error: bool,
    #[serde(rename = "structuredContent", skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
}

impl CallToolResult {
    pub fn success(value: Value) -> Self {
        let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
        Self {
            content: vec![ContentBlock::text(text)],
            is_error: false,
            structured_content: Some(value),
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            content: vec![ContentBlock::text(message)],
            is_error: true,
            structured_content: None,
        }
    }

    pub fn into_value(self) -> Value {
        serde_json::to_value(self).expect("CallToolResult's fields are all JSON-safe")
    }
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
