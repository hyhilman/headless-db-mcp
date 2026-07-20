use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use db_headless_mcp_wire::{
    JsonRpcError, JsonRpcErrorResponse, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest,
    JsonRpcSuccessResponse,
};
use serde::Deserialize;
use serde_json::Value;

use crate::audit::{AuditEvent, AuditLogger, AuditOutcome};
use crate::registry::{McpToolRegistry, ToolCallError};
use crate::tool::{CallToolResult, McpToolError};

/// A placeholder protocol version string for this Phase 1 implementation.
/// Not yet validated against a client-supplied version — full
/// version negotiation is deferred until this server needs to support
/// more than one MCP protocol revision.
pub const PROTOCOL_VERSION: &str = "2025-06-18";

/// Transport-agnostic MCP request/notification dispatch.
///
/// Owns no socket, no auth, no rate limiting — those are transport
/// concerns (stdio is implicitly trusted the way a locally-spawned
/// process is; HTTP is not, and must apply auth/rate-limiting before a
/// message ever reaches this type). This type's only job is: given one
/// decoded [`JsonRpcMessage`], decide what happens and what (if
/// anything) to send back.
pub struct McpSession {
    registry: Arc<McpToolRegistry>,
    audit: Arc<dyn AuditLogger>,
    initialized: AtomicBool,
}

impl McpSession {
    pub fn new(registry: Arc<McpToolRegistry>, audit: Arc<dyn AuditLogger>) -> Self {
        Self {
            registry,
            audit,
            initialized: AtomicBool::new(false),
        }
    }

    /// Handles one incoming message. Returns `Some` for a request (always
    /// reply, success or error) and `None` for a notification (per spec,
    /// never reply to a notification) or for an incoming response (this
    /// server does not currently send requests of its own, so it has
    /// nothing to correlate an incoming response against; it is logged
    /// and dropped rather than treated as an error, since receiving one
    /// is a client-side protocol quirk, not this server's failure).
    pub async fn handle(&self, message: JsonRpcMessage) -> Option<JsonRpcMessage> {
        match message {
            JsonRpcMessage::Request(request) => Some(self.handle_request(request).await),
            JsonRpcMessage::Notification(notification) => {
                self.handle_notification(notification);
                None
            }
            JsonRpcMessage::SuccessResponse(_) | JsonRpcMessage::ErrorResponse(_) => {
                tracing::debug!("ignoring unexpected response-shaped message from client");
                None
            }
        }
    }

    async fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcMessage {
        let method = request.method.clone();
        let (result, tool_name, outcome) = self.dispatch_request(&request).await;

        self.audit
            .record(AuditEvent {
                method,
                tool_name,
                outcome,
            })
            .await;

        match result {
            Ok(value) => {
                JsonRpcMessage::SuccessResponse(JsonRpcSuccessResponse::new(request.id, value))
            }
            Err(error) => {
                JsonRpcMessage::ErrorResponse(JsonRpcErrorResponse::new(Some(request.id), error))
            }
        }
    }

    /// Returns the handler's result, the tool name (if any), and the audit
    /// outcome, so the caller can log without re-parsing params or
    /// re-deriving success/failure from the JSON-RPC-level result (which,
    /// for `tools/call`, is *not* the same thing — see
    /// [`AuditOutcome::from_result`]).
    async fn dispatch_request(
        &self,
        request: &JsonRpcRequest,
    ) -> (Result<Value, JsonRpcError>, Option<String>, AuditOutcome) {
        match request.method.as_str() {
            "initialize" => {
                let result = self.handle_initialize();
                let outcome = AuditOutcome::from_result(&result);
                (result, None, outcome)
            }
            "tools/list" => {
                let result = self.handle_tools_list();
                let outcome = AuditOutcome::from_result(&result);
                (result, None, outcome)
            }
            "tools/call" => self.handle_tools_call(request.params.clone()).await,
            other => {
                let result = Err(JsonRpcError::method_not_found(format!(
                    "unknown method: {other}"
                )));
                let outcome = AuditOutcome::from_result(&result);
                (result, None, outcome)
            }
        }
    }

    fn handle_initialize(&self) -> Result<Value, JsonRpcError> {
        self.initialized.store(true, Ordering::SeqCst);
        Ok(serde_json::json!({
            "protocolVersion": PROTOCOL_VERSION,
            "serverInfo": {
                "name": "db-headless-mcp",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": { "tools": {} },
        }))
    }

    fn require_initialized(&self) -> Result<(), JsonRpcError> {
        if self.initialized.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(JsonRpcError::invalid_request(
                "session is not initialized; call \"initialize\" first",
            ))
        }
    }

    fn handle_tools_list(&self) -> Result<Value, JsonRpcError> {
        self.require_initialized()?;
        Ok(serde_json::json!({ "tools": self.registry.descriptors() }))
    }

    async fn handle_tools_call(
        &self,
        params: Option<Value>,
    ) -> (Result<Value, JsonRpcError>, Option<String>, AuditOutcome) {
        if let Err(err) = self.require_initialized() {
            let outcome = AuditOutcome::Error { code: err.code };
            return (Err(err), None, outcome);
        }

        let params = match params.map(serde_json::from_value::<ToolCallParams>) {
            Some(Ok(params)) => params,
            Some(Err(err)) => {
                let error =
                    JsonRpcError::invalid_params(format!("invalid \"tools/call\" params: {err}"));
                let outcome = AuditOutcome::Error { code: error.code };
                return (Err(error), None, outcome);
            }
            None => {
                let error = JsonRpcError::invalid_params("\"tools/call\" requires params");
                let outcome = AuditOutcome::Error { code: error.code };
                return (Err(error), None, outcome);
            }
        };

        let tool_name = params.name.clone();
        let result = self.registry.call(&params.name, params.arguments).await;
        let (mapped, outcome) = match result {
            Ok(value) => (
                Ok(CallToolResult::success(value).into_value()),
                AuditOutcome::Ok,
            ),
            Err(ToolCallError::Unknown(unknown)) => {
                let error = JsonRpcError::method_not_found(unknown.to_string());
                let outcome = AuditOutcome::Error { code: error.code };
                (Err(error), outcome)
            }
            Err(ToolCallError::Failed(err)) => {
                let code = match &err {
                    McpToolError::InvalidArguments(_) => JsonRpcError::INVALID_PARAMS,
                    McpToolError::Failed(_) => JsonRpcError::INTERNAL_ERROR,
                };
                (
                    Ok(CallToolResult::error(err.to_string()).into_value()),
                    AuditOutcome::Error { code },
                )
            }
        };

        (mapped, Some(tool_name), outcome)
    }

    fn handle_notification(&self, notification: JsonRpcNotification) {
        match notification.method.as_str() {
            "notifications/initialized" => {
                tracing::debug!("client acknowledged initialization");
            }
            other => {
                tracing::debug!(method = other, "ignoring unrecognized notification");
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Option<Value>,
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use db_headless_mcp_wire::{JsonRpcId, JsonRpcNotification as WireNotification};
    use serde_json::json;
    use std::sync::Mutex as StdMutex;

    use crate::tool::McpTool;

    use super::*;

    struct EchoStub;

    #[async_trait]
    impl McpTool for EchoStub {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "echoes arguments back"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn call(&self, arguments: Option<Value>) -> Result<Value, McpToolError> {
            Ok(arguments.unwrap_or(Value::Null))
        }
    }

    #[derive(Default)]
    struct RecordingAudit {
        events: StdMutex<Vec<AuditEvent>>,
    }

    #[async_trait]
    impl AuditLogger for RecordingAudit {
        async fn record(&self, event: AuditEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn session_with_echo() -> (Arc<McpSession>, Arc<RecordingAudit>) {
        let mut registry = McpToolRegistry::new();
        registry.register(Arc::new(EchoStub));
        let audit = Arc::new(RecordingAudit::default());
        let session = Arc::new(McpSession::new(Arc::new(registry), audit.clone()));
        (session, audit)
    }

    fn request(id: i64, method: &str, params: Option<Value>) -> JsonRpcRequest {
        JsonRpcRequest::new(JsonRpcId::Number(id), method, params)
    }

    #[tokio::test]
    async fn tools_call_before_initialize_is_rejected() {
        let (session, _audit) = session_with_echo();
        let reply = session
            .handle(JsonRpcMessage::Request(request(1, "tools/list", None)))
            .await
            .expect("requests always get a reply");

        match reply {
            JsonRpcMessage::ErrorResponse(err) => {
                assert_eq!(err.error.code, JsonRpcError::INVALID_REQUEST);
            }
            other => panic!("expected an error response, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn initialize_then_tools_list_returns_registered_tool() {
        let (session, _audit) = session_with_echo();
        session
            .handle(JsonRpcMessage::Request(request(1, "initialize", None)))
            .await;

        let reply = session
            .handle(JsonRpcMessage::Request(request(2, "tools/list", None)))
            .await
            .expect("reply");

        let JsonRpcMessage::SuccessResponse(success) = reply else {
            panic!("expected success response");
        };
        let tools = success.result["tools"].as_array().expect("tools array");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "echo");
    }

    #[tokio::test]
    async fn tools_call_dispatches_to_the_named_tool() {
        let (session, audit) = session_with_echo();
        session
            .handle(JsonRpcMessage::Request(request(1, "initialize", None)))
            .await;

        let call_params = json!({"name": "echo", "arguments": {"message": "hi"}});
        let reply = session
            .handle(JsonRpcMessage::Request(request(
                2,
                "tools/call",
                Some(call_params),
            )))
            .await
            .expect("reply");

        let JsonRpcMessage::SuccessResponse(success) = reply else {
            panic!("expected success response");
        };
        assert_eq!(success.result["isError"], json!(false));
        assert_eq!(
            success.result["structuredContent"],
            json!({"message": "hi"})
        );
        let content = success.result["content"].as_array().expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        let expected_text = serde_json::to_string_pretty(&json!({"message": "hi"})).unwrap();
        assert_eq!(content[0]["text"], json!(expected_text));

        let events = audit.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].tool_name.as_deref(), Some("echo"));
    }

    struct FailingStub;

    #[async_trait]
    impl McpTool for FailingStub {
        fn name(&self) -> &str {
            "failing"
        }
        fn description(&self) -> &str {
            "always fails"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn call(&self, _arguments: Option<Value>) -> Result<Value, McpToolError> {
            Err(McpToolError::Failed("boom".to_string()))
        }
    }

    #[tokio::test]
    async fn tools_call_failure_is_reported_in_band_not_as_a_protocol_error() {
        let mut registry = McpToolRegistry::new();
        registry.register(Arc::new(FailingStub));
        let audit = Arc::new(RecordingAudit::default());
        let session = Arc::new(McpSession::new(Arc::new(registry), audit.clone()));
        session
            .handle(JsonRpcMessage::Request(request(1, "initialize", None)))
            .await;

        let call_params = json!({"name": "failing"});
        let reply = session
            .handle(JsonRpcMessage::Request(request(
                2,
                "tools/call",
                Some(call_params),
            )))
            .await
            .expect("reply");

        let JsonRpcMessage::SuccessResponse(success) = reply else {
            panic!("a tool execution failure must still be a JSON-RPC success response, with isError set inside the result, so the client actually renders it");
        };
        assert_eq!(success.result["isError"], json!(true));
        let content = success.result["content"].as_array().expect("content array");
        assert_eq!(content[0]["text"], json!("boom"));

        let events = audit.events.lock().unwrap();
        assert!(
            matches!(events[1].outcome, AuditOutcome::Error { .. }),
            "a failed tool call must still audit-log as an error even though the JSON-RPC \
             envelope itself is a success response, or failures become invisible to audit logs"
        );
    }

    #[tokio::test]
    async fn tools_call_for_unknown_tool_is_method_not_found() {
        let (session, _audit) = session_with_echo();
        session
            .handle(JsonRpcMessage::Request(request(1, "initialize", None)))
            .await;

        let call_params = json!({"name": "does-not-exist"});
        let reply = session
            .handle(JsonRpcMessage::Request(request(
                2,
                "tools/call",
                Some(call_params),
            )))
            .await
            .expect("reply");

        let JsonRpcMessage::ErrorResponse(err) = reply else {
            panic!("expected error response");
        };
        assert_eq!(err.error.code, JsonRpcError::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_method_is_method_not_found() {
        let (session, _audit) = session_with_echo();
        session
            .handle(JsonRpcMessage::Request(request(1, "initialize", None)))
            .await;

        let reply = session
            .handle(JsonRpcMessage::Request(request(
                2,
                "nonexistent/method",
                None,
            )))
            .await
            .expect("reply");

        let JsonRpcMessage::ErrorResponse(err) = reply else {
            panic!("expected error response");
        };
        assert_eq!(err.error.code, JsonRpcError::METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn notification_never_produces_a_reply() {
        let (session, _audit) = session_with_echo();
        let reply = session
            .handle(JsonRpcMessage::Notification(WireNotification::new(
                "notifications/initialized",
                None,
            )))
            .await;
        assert!(reply.is_none());
    }
}
