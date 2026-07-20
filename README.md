# db-headless-mcp

A headless, backend-runnable MCP server exposing database connections
(PostgreSQL, MySQL, SQLite, and more) as MCP tools. Rewritten in Rust from
[TablePro](https://github.com/TableProApp/TablePro)'s driver/MCP layer,
dropping every GUI/AppKit dependency so it can run on any server, not just
macOS.

## Status

Phase 0 (foundations) in progress. See `crates/`:

- `core` — the `DatabaseDriver` trait every database backend implements,
  plus the shared transfer types (`QueryResult`, `CellValue`, etc).
- `secrets` — credential storage abstraction, encrypted-at-rest by default.
- `registry` — connection lifecycle bookkeeping with generation-fenced
  cancellation (a cancelled connect/query attempt can never clobber a
  newer one, or leak a connection/thread when it loses the race).
- `mcp-wire` — JSON-RPC 2.0 message types and SSE event framing.
- `server` — binary crate wiring the above into a running MCP server.

No database driver is implemented yet — PostgreSQL (via `tokio-postgres`)
is next.

## Non-negotiable guardrails

These apply to every crate in this workspace, not just the current ones:

1. Never build SQL by string-interpolating parameter values. Always use
   the driver's native bind API.
2. Every credential goes through the `secrets` abstraction. Never log,
   error-message, or audit-log a secret in full.
3. The server requires auth by default and binds to loopback unless an
   operator explicitly opts into a wider listen address.
4. Destructive operations (DROP/TRUNCATE/DELETE-without-WHERE) require an
   explicit confirmation step, never execute on first call.
5. Cancellation must not leak. Every in-flight connect/query attempt owns
   a monotonic generation token (see `registry`); a stale attempt tears
   down its own resources instead of touching shared state.
6. TLS/host-key verification defaults to strict; downgrading is an
   explicit, logged opt-out, never a default.
7. A hard server-side row cap applies independent of client-requested
   limits (`core::RowLimits::EMERGENCY_MAX`).
8. Adding a database driver must not require changing the `DatabaseDriver`
   trait other drivers depend on.

## Development

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

`#![forbid(unsafe_code)]` applies to every crate except future FFI-backed
driver crates, which must scope `unsafe` narrowly and justify each block.
