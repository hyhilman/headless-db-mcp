#![forbid(unsafe_code)]

//! Transport-agnostic MCP session logic: tool registry, `initialize`/
//! `tools/list`/`tools/call` dispatch, audit logging.
//!
//! Deliberately has no socket, no HTTP, no stdio here — [`McpSession`]
//! takes one decoded `db_headless_mcp_wire::JsonRpcMessage` in and
//! produces zero or one out. Transports (stdio, HTTP+SSE) wrap this and
//! own the concerns that differ between them: stdio is implicitly
//! trusted the way any locally-spawned process is, HTTP is not and must
//! apply auth/rate-limiting before a message ever reaches this type.

mod audit;
mod registry;
mod session;
mod tool;
mod tools;

pub use audit::{AuditEvent, AuditLogger, AuditOutcome, TracingAuditLogger};
pub use registry::{McpToolRegistry, ToolCallError, UnknownTool};
pub use session::{McpSession, PROTOCOL_VERSION};
pub use tool::{McpTool, McpToolError, ToolDescriptor};
pub use tools::{EchoTool, PingTool};
