//! Placeholder tools proving the MCP transport works end to end without a
//! real database driver.
//!
//! These are not part of the tool surface documented in the migration
//! plan (`Connect`, `ExecuteQuery`, `ListTables`, ...) — that surface
//! needs a real [`db_headless_core::DatabaseDriver`] and a connection
//! manager to back it, which land in Phase 2. [`PingTool`] and
//! [`EchoTool`] exist only to exercise `tools/list`/`tools/call` over a
//! real transport (stdio, then HTTP+SSE) before there's anything real to
//! call.

mod echo;
mod ping;

pub use echo::EchoTool;
pub use ping::PingTool;
