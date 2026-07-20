# db-headless-mcp

A headless, backend-runnable MCP server exposing database connections
(PostgreSQL, Redis, ClickHouse today; more planned) as MCP tools.
Rewritten in Rust from [TablePro](https://github.com/TableProApp/TablePro)'s
driver/MCP layer, dropping every GUI/AppKit dependency so it can run on
any server, not just macOS.

## Status

See `crates/`:

- `core` — the `DatabaseDriver` trait every database backend implements,
  plus the shared transfer types (`QueryResult`, `CellValue`, etc).
- `secrets` — credential storage abstraction, encrypted-at-rest by default.
- `registry` — connection lifecycle bookkeeping with generation-fenced
  cancellation (a cancelled connect/query attempt can never clobber a
  newer one, or leak a connection/thread when it loses the race).
- `mcp-wire` — JSON-RPC 2.0 message types and SSE event framing.
- `mcp-server` — tool registry, session/dispatch, audit logging.
- `transport-stdio` / `transport-http` — the two ways an MCP client talks
  to this server.
- `driver-postgres` / `driver-redis` / `driver-clickhouse` — the three
  implemented `DatabaseDriver`s.
- `connections` — `ConnectionManager` (driver-agnostic connection
  lifecycle) and the MCP tools that expose it (`connect`,
  `execute_query`, `list_tables`, ...).
- `connection-profiles` — named, persisted connection credentials (see
  "Credential storage" below).
- `server` — binary crate wiring all of the above into a running MCP
  server.

MySQL, SQLite, and MongoDB are not implemented yet.

## Credential storage

There are two ways to open a connection:

1. **Ad-hoc** — `connect` with `database_type`/`host`/`port`/`username`/
   `password` directly. Ephemeral and in-memory only; nothing is
   persisted. The password has to be passed on every call.
2. **Saved profile** — `save_connection_profile` once with a name and the
   full credentials, then `connect` with just `profile_name` from then
   on. The password is never passed to `connect` again, and never
   appears in `list_connection_profiles`' output — only a `has_password`
   flag does.

Saved profiles split storage across two tiers: non-secret metadata
(host/port/username/database/ssl_mode) lives in a plain, atomically
written JSON file; the password lives in `db-headless-secrets`, an
AES-256-GCM-encrypted file, keyed by profile name. Enable it by setting
`DB_HEADLESS_MASTER_KEY` (64 hex characters — generate one with
`openssl rand -hex 32`) before starting the server. Unset, the three
profile tools (`save_connection_profile`, `list_connection_profiles`,
`delete_connection_profile`) aren't registered at all and `connect`'s
`profile_name` argument returns a clear error — the ad-hoc path above
keeps working regardless.

**The master key must stay stable across restarts.** Losing it makes
every stored password permanently unrecoverable (there's no key
rotation or recovery path yet) — keep it in a password manager or secret
store, not just in your shell history.

`DB_HEADLESS_DATA_DIR` (default `.`) sets where `secrets.json` and
`profiles.json` are written; point it at a persistent volume in Docker
(see below).

## Running with Docker

```bash
docker build -t db-headless-mcp .
docker run -d --name db-headless-mcp \
  -p 127.0.0.1:8787:8787 \
  -e DB_HEADLESS_MCP_TOKEN="$(openssl rand -hex 32)" \
  -e DB_HEADLESS_MASTER_KEY="$(openssl rand -hex 32)" \
  -v db-headless-data:/data \
  -e DB_HEADLESS_DATA_DIR=/data \
  db-headless-mcp
```

Or `docker compose up -d` with `DB_HEADLESS_MCP_TOKEN` set in a `.env`
file (never commit it). `DB_HEADLESS_MCP_TOKEN` authenticates *MCP
clients to this server* — it has nothing to do with database
credentials, which are supplied per-connection via `connect` or
`save_connection_profile` as described above.

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
