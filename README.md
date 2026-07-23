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

## Read-only connections

Pass `read_only: true` to `connect` (ad-hoc) or `save_connection_profile`
(persisted) to make a connection reject any write. This is enforced by
the database engine itself, not by this server parsing SQL:

- **PostgreSQL**: `execute_user_query`/`stream_rows` open their
  transaction with `BEGIN READ ONLY`, so Postgres itself refuses a write,
  including one hidden behind a function call or CTE a client-side
  statement check could miss.
- **ClickHouse**: every request carries the HTTP interface's own
  `readonly=1` setting, which the server enforces for writes and setting
  changes alike.
- **Redis**, which has no engine-level read-only mode, uses an explicit
  allow-list of known read commands (`crates/driver-redis/src/command.rs`);
  anything not on that list is rejected, including commands the list does
  not yet recognize — a false rejection just costs a retry, a false
  permission would defeat the point of the flag.

Omitting `read_only` on a `save_connection_profile` update keeps
whatever was already stored, the same way omitting `password` does — it
never silently turns write access back on.

## PostgreSQL TLS

`connect`'s `ssl_mode` is fully implemented for PostgreSQL via `rustls`
(`tokio-postgres-rustls`), not just accepted and ignored:

- `disabled` — plaintext, no TLS.
- `preferred` / `required` — encrypts the connection but does not verify
  the server's certificate at all, matching libpq's own `sslmode=require`
  semantics exactly. `preferred` additionally falls back to plaintext if
  the server refuses TLS; `required` does not.
- `verify_ca` — verifies the certificate chains to the CA at `ca_path`,
  but not the hostname. Requires `ca_path`.
- `verify_identity`, and a missing `ssl_mode` (guardrail #6: never
  silently downgrade) — full chain and hostname verification, against
  `ca_path` if given or the platform's native trust store otherwise.

Every mode still verifies the TLS handshake signature for real; only the
chain-of-trust and hostname checks differ between modes. ClickHouse's own
TLS support (via `reqwest`) predates this and covers the same mode set
through `reqwest`'s HTTPS handling instead.

## Redis TLS

`connect`'s `ssl_mode` is implemented for Redis too, via `redis-rs`'s own
`rustls` integration rather than a hand-rolled connector:

- `disabled` — plaintext, no TLS.
- `preferred` / `required` — encrypts the connection without verifying
  the certificate at all, same semantics as the PostgreSQL modes above.
- `verify_ca`, `verify_identity`, and a missing `ssl_mode` — full chain
  and hostname verification, against `ssl.ca_path` if given or the
  platform's native trust store otherwise.

Unlike PostgreSQL, `verify_ca` and `verify_identity` are the same mode
here: `redis-rs`'s public TLS API only exposes a binary "verify
everything" / "verify nothing" switch, with no hook for a custom
certificate verifier, so "trust the chain but skip the hostname check"
can't be expressed through it the way the PostgreSQL connector does.
Collapsing `verify_ca` into the stricter `verify_identity` behavior is
deliberate: it never checks less than the mode asks for, only more.

## Query timeouts

Every connection gets two layers of query timeout, neither of which a
caller has to opt into:

- **Server-side** — right after connecting, `connect` pushes a 30s
  timeout into the database engine itself via
  `DatabaseDriver::apply_query_timeout` (Postgres: `SET
  statement_timeout`; ClickHouse: `max_execution_time` on every
  request). The engine cancels the query on its own and returns a clean
  error; the connection stays usable for the next query. This is the
  normal path.
- **Client-side backstop** — `execute_query` also wraps the query call
  itself in a 45s timeout, deliberately longer than the server-side one
  so the engine gets the first chance to fail cleanly. The backstop only
  trips when the connection isn't communicating at all: the same
  packet-loss case where the engine's own timeout enforcement can't get
  a cancellation request through either. On trip, it calls
  `DatabaseDriver::cancel_query` (Postgres `PQcancel`, ClickHouse
  cancel-by-query-id, Redis `CLIENT KILL`) and returns a clear
  `isError: true` result instead of hanging indefinitely.

A driver with no real `apply_query_timeout` (Redis, whose commands
aren't long-running "statements" the same way SQL is) still gets the
client-side backstop, since that layer lives in `execute_query`, not in
the driver — adding it required no driver-specific code and applies to
every current and future driver uniformly.

Before this, a slow, unindexed query and a genuinely dead connection
were indistinguishable from the caller's side: both just hung until
something outside this server (the MCP client itself) gave up, with no
error and no way to tell which one had happened.

## MCP protocol compliance

`tools/call` responses follow the MCP spec's actual result shape:
`{ content: [{ type: "text", text: "..." }], isError, structuredContent }`.
A tool's return value is JSON-stringified into `content[0].text` and also
carried untouched in `structuredContent`. A tool that fails (invalid
arguments, or a driver error) still comes back as a successful JSON-RPC
response with `isError: true`, never a JSON-RPC-level error, so the
calling model actually sees the failure instead of an empty result. Only
calling a tool name that isn't registered is a JSON-RPC-level error
(`METHOD_NOT_FOUND`).

Returning the bare tool value as `result` (this server's original shape)
is valid JSON-RPC but renders as nothing in a real MCP client, including
Claude Code, since every client reads `result.content`.

## Running with Docker

Build locally:

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

Or pull a tagged release from GHCR instead of building
(`.github/workflows/docker-publish.yml` pushes one for every `v*` tag):

```bash
docker pull ghcr.io/hyhilman/headless-db-mcp:<version>
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
