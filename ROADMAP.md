# Roadmap

Working notes for continuing `db-headless-mcp`. See `README.md` for the
non-negotiable guardrails and day-to-day dev commands; this document is
about what's done, what's next, and what to watch out for.

## Done so far

**Phase 0-1 — foundations**
- `core`: `DatabaseDriver`/`DriverFactory` async traits, transfer types
  (`QueryResult`, `CellValue`, `ConnectionConfig`, ...).
- `secrets`: `EncryptedFileSecretStore` (AES-256-GCM, master key from
  `DB_HEADLESS_MASTER_KEY`, fails closed).
- `registry`: generation-fenced `ConnectionAttemptRegistry` /
  `SessionRegistry` — a cancelled connect/query attempt can never clobber
  a newer one.
- `mcp-wire`, `mcp-server`: JSON-RPC 2.0 + SSE framing, tool registry,
  session dispatch, audit logging.
- `transport-stdio`, `transport-http`: both wired into `crates/server`.
  HTTP requires a bearer token and rate-limits per source IP.

**Phase 2 — PostgreSQL**
- `driver-postgres` via `tokio-postgres`: cursor/portal streaming, row
  caps, real `information_schema`/`pg_catalog` introspection,
  `CancelToken`-based cancellation, safe identifier quoting.
- `connections`: `ConnectionManager` + 9 MCP tools (`connect`,
  `disconnect`, `execute_query`, `list_databases`, `list_schemas`,
  `list_tables`, `describe_table`, `list_connections`,
  `get_connection_status`).

**Phase 3 — Redis and ClickHouse**
- `driver-redis`: Redis modeled as six pseudo-tables
  (string/hash/list/set/zset/stream); real `SCAN ... TYPE ...` streaming;
  parameters bound as distinct RESP arguments.
- `driver-clickhouse`: HTTP interface (TSV for buffered queries,
  JSONEachRow for streaming); parameters bound via `unhex({pN:String})`
  with hex-encoded values (plain `{pN:String}` was verified to corrupt
  tabs and not decode a `0x` prefix — see `driver-clickhouse/src/params.rs`).

**Credential storage (this session)**
- `connection-profiles`: named, persisted connection credentials.
  Non-secret metadata (host/port/username/database/ssl_mode) in a plain
  atomic-write JSON file; the password in the existing encrypted
  `SecretStore`, keyed by profile name.
- New tools: `save_connection_profile`, `list_connection_profiles`
  (never returns a password, only `has_password`), `delete_connection_profile`.
  `connect` accepts `profile_name` as an alternative to raw credentials.
- Gated on `DB_HEADLESS_MASTER_KEY`: unset means the three tools aren't
  even registered and `connect`'s `profile_name` gives a clear error, but
  the ad-hoc `connect` path is unaffected. A malformed key is a hard
  startup failure.

**Deployment**
- `Dockerfile` (multi-stage, non-root user, persistent `/data` volume)
  and `docker-compose.yml`.

**Read-only connections**
- `connect`/`save_connection_profile` accept `read_only: bool`. Enforced
  by the engine itself on every driver, not by this server parsing SQL:
  Postgres opens `BEGIN READ ONLY` transactions, ClickHouse sends the
  HTTP interface's `readonly=1` setting on every request, Redis checks
  the command verb against an explicit allow-list (deny-by-default).
  Live-smoke-tested against real Postgres/Redis/ClickHouse containers —
  a write is rejected and never persists; a read still works.
- A persisted profile's `read_only` follows the same "omit means keep
  what's stored" rule as `password`, never silently flipping back to
  writable on an unrelated update.

**PostgreSQL TLS**
- `driver-postgres` now implements every `SslMode` for real via `rustls`
  (`tokio-postgres-rustls`): `preferred`/`required` encrypt without
  verifying the certificate (matches libpq's `sslmode=require`);
  `verify_ca` verifies the chain against `ssl.ca_path` but not the
  hostname (built directly on `rustls-webpki`'s `EndEntityCert`, since
  rustls's own `WebPkiServerVerifier` always couples the two checks);
  `verify_identity` (and a missing mode, per guardrail #6) does full
  chain + hostname verification against `ca_path` or the platform's
  native trust store. Every mode still verifies the handshake signature
  for real. Live-tested against a real Postgres container with a
  self-signed, deliberately hostname-mismatched cert: `verify_ca` accepts
  it (chain trusted, hostname unchecked), `verify_identity` rejects the
  *same* cert (hostname checked), a wrong CA is rejected under
  `verify_ca`, and `required` connects regardless.

**Redis TLS**
- `driver-redis` now implements every `SslMode` via `redis-rs`'s own
  `rustls` integration (`tls-rustls`/`tls-rustls-insecure` features)
  rather than a hand-rolled connector: `preferred`/`required` use
  `redis-rs`'s `insecure` connection mode (encrypts, verifies nothing);
  `verify_ca`, `verify_identity`, and a missing mode all use its
  non-`insecure` mode (full chain + hostname check against `ca_path` or
  the native trust store). `verify_ca` and `verify_identity` are
  identical here, unlike PostgreSQL: `redis-rs`'s public TLS API has no
  hook for a custom certificate verifier, so "chain trusted, hostname
  unchecked" can't be built the way `driver-postgres`'s connector does.
  Collapsing upward (stricter, never more lenient) is the deliberate
  choice. Live-tested against a real Redis container with a self-signed
  cert: `required` connects regardless of trust; a cert matching the
  real connection host and trusted via `ca_path` is accepted under
  `verify_identity`; a hostname-mismatched cert is rejected under both
  `verify_ca` and `verify_identity`; a cert from an unrelated CA is
  rejected under both.
- Fixed a real bug surfaced by writing those tests: `RedisDriver`
  established its `ConnectionManager` with `redis-rs`'s default retry
  config, whose `factor` (100) and unset `max_delay` (backon's own
  60s default applies) combine to make every failed connect attempt
  retry for several minutes before surfacing an error, regardless of
  cause (wrong password, rejected TLS cert, anything permanent). Now
  bounded to 2 retries with a 1s max delay and a 5s connection timeout,
  so a connect failure surfaces in seconds. This affected every driver
  connect failure, not just TLS.

**MCP protocol compliance**
- `tools/call` now returns the actual MCP spec result shape
  (`{content: [{type: "text", text}], isError, structuredContent}`)
  instead of the bare tool `Value` as `result`. The bare shape was valid
  JSON-RPC but rendered as nothing in a real MCP client, Claude Code
  included, since every client reads `result.content`. A tool failure
  (invalid arguments or a driver error) now comes back as a successful
  JSON-RPC response with `isError: true` rather than a JSON-RPC-level
  error, so the calling model sees it instead of getting an empty result;
  only an unregistered tool name is still a protocol-level error. Audit
  logging tracks this distinction explicitly (`AuditOutcome` is now
  computed per-branch in `McpSession`, not derived from the JSON-RPC
  result) so a failed tool call still logs as a failure.

**Docker image publishing**
- `.github/workflows/docker-publish.yml` builds and pushes the image to
  `ghcr.io/hyhilman/headless-db-mcp` on every `v*` tag, using the
  workflow's own `GITHUB_TOKEN` (no separate registry credential to
  manage) and `docker/metadata-action` to derive `{version}`,
  `{major}.{minor}`, and `{major}` tags plus `latest`. A new GHCR
  package defaults to private on first push even for a public repo —
  needs a one-time manual visibility change in the package's own
  settings after the first tag is pushed.

Full workspace (`cargo test --workspace`, `cargo clippy --workspace
--all-targets -- -D warnings`, `cargo fmt --check`) is green as of the
last commit on `main`.

## Not implemented yet

Real database drivers, in the order TablePro shipped them (see that
project's `CLAUDE.md` for the bundled/registry split this list mirrors):

1. **MySQL** — via `mysql_async` or similar pure-Rust client. Was
   bundled in the source app alongside Postgres/SQLite/ClickHouse/Redis,
   so probably the most-requested next driver.
2. **SQLite** — via `rusqlite` or `sqlx`'s sqlite feature. No network
   auth/TLS concerns, but the driver contract's `ConnectionConfig`
   (host/port/username) doesn't fit an embedded file DB well — expect to
   lean on `additional_fields` for a file path, the same escape hatch
   the trait docs describe for driver-specific knobs.
3. **MongoDB** — via the official `mongodb` crate. First of the
   "registry-only" set; document store, so `fetch_tables`/`fetch_columns`
   will need real thought about what a "table"/"column" even means here
   (same category of problem Redis's pseudo-table model solved).
4. After that, roughly by expected demand: **Oracle**, **DuckDB**,
   **MSSQL**, **Cassandra**, **Etcd**, **CloudflareD1**, **DynamoDB**,
   **BigQuery**, **LibSQL**, **Snowflake**, **Elasticsearch**.

Each new driver should follow the Phase 2/3 pattern: build it in its own
crate, real integration tests via `testcontainers` (or the equivalent
hosted/emulator setup for cloud-only backends like DynamoDB/BigQuery),
register its `DriverFactory` in `crates/server/src/main.rs`.

## Known gaps worth closing before relying on this in production

- **No credential rotation/clear path.** `save_connection_profile`
  without a password preserves the existing one; there's no way to
  explicitly clear a stored password short of `delete_connection_profile`
  + recreate. Fine for now, but worth a real `clear_password` op if this
  gets used for anything long-lived.
- **No master-key rotation.** Losing `DB_HEADLESS_MASTER_KEY` makes every
  stored password permanently unrecoverable — there's no re-encrypt-under-
  a-new-key tool yet.
- **No test/lint CI.** `.github/workflows/docker-publish.yml` builds and
  pushes the Docker image to `ghcr.io/hyhilman/headless-db-mcp` on `v*`
  tags, but nothing runs `cargo test --workspace` / `clippy` /
  `fmt --check` on a push or PR yet — those still only run locally.
  Worth adding before this has more than one contributor.
- **Deferred tools**: `GetTableDdl`, `SwitchDatabase`, `SwitchSchema`
  already have driver methods (`fetch_table_ddl`, `switch_database`,
  `switch_schema`) but aren't wired up as MCP tools yet — same situation
  Phase 2's own doc comments flagged.
- **No integration test hits the HTTP transport's SSE stream endpoint
  with a real long-running query** — `sse_demo` in `transport-http` is a
  demo, not exercised by a driver end-to-end yet.

## How to resume

- Repo: `~/projects/me/db-headless-mcp`, pushed to
  `github.com/hyhilman/headless-db-mcp`, `main` branch only so far.
- Git identity for commits here is configured globally as `hyhilman` /
  `hilman.nihri@gmail.com`.
- Before starting new work, run `git status --short` — don't assume this
  document's "Done so far" section is still accurate; check first.
- Standard loop for a new driver: build the crate with real
  `testcontainers`-backed integration tests, run
  `cargo test -p <crate>` and `cargo clippy -p <crate> --all-targets --
  -D warnings` on it in isolation, wire it into `crates/server/src/main.rs`,
  then re-run the full workspace suite before considering it done. For
  anything touching wire-format quirks (parameter binding, streaming,
  cancellation), do a live smoke test against a real running binary and a
  real database, not just unit tests — that's what caught the two real
  bugs (a Postgres commit-vs-rollback bug and a ClickHouse DDL-breaking
  bug) that automated tests alone missed during Phase 2/3.
