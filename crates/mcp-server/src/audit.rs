use async_trait::async_trait;

/// The outcome of one JSON-RPC request handled by [`crate::McpSession`].
#[derive(Debug, Clone)]
pub enum AuditOutcome {
    Ok,
    Error { code: i64 },
}

/// One audit record.
///
/// Deliberately excludes tool call arguments and results: those may
/// contain user data or, once real database tools land, query text and
/// row values. `method`/`tool_name`/`outcome` are enough to reconstruct
/// "who called what, and did it succeed" without risking a secret or a
/// user's data landing in a log file (guardrail #2).
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub method: String,
    pub tool_name: Option<String>,
    pub outcome: AuditOutcome,
}

/// Records one [`AuditEvent`] per handled request. Implementations must
/// not block the caller for long — this is called inline on the request
/// path.
#[async_trait]
pub trait AuditLogger: Send + Sync {
    async fn record(&self, event: AuditEvent);
}

/// Emits audit events as structured `tracing` events rather than to a
/// dedicated store. Sufficient while the server has no persistent audit
/// log of its own; swap in a file/database-backed `AuditLogger` once one
/// exists without touching call sites.
#[derive(Debug, Default)]
pub struct TracingAuditLogger;

#[async_trait]
impl AuditLogger for TracingAuditLogger {
    async fn record(&self, event: AuditEvent) {
        match event.outcome {
            AuditOutcome::Ok => {
                tracing::info!(method = %event.method, tool = ?event.tool_name, "mcp call ok");
            }
            AuditOutcome::Error { code } => {
                tracing::warn!(method = %event.method, tool = ?event.tool_name, code, "mcp call failed");
            }
        }
    }
}
