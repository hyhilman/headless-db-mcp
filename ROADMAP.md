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

- **Postgres TLS is not actually implemented.** `driver-postgres` only
  connects via `NoTls` today (see the comment in
  `crates/driver-postgres/src/driver.rs`); `ssl_mode` is accepted and
  validated but not enforced. Wire up `tokio-postgres-rustls` before
  connecting to anything over an untrusted network with `verify_identity`.
- **No credential rotation/clear path.** `save_connection_profile`
  without a password preserves the existing one; there's no way to
  explicitly clear a stored password short of `delete_connection_profile`
  + recreate. Fine for now, but worth a real `clear_password` op if this
  gets used for anything long-lived.
- **No master-key rotation.** Losing `DB_HEADLESS_MASTER_KEY` makes every
  stored password permanently unrecoverable — there's no re-encrypt-under-
  a-new-key tool yet.
- **No CI.** There's no `.github/workflows` in this repo yet — `cargo
  test --workspace` / `clippy` / `fmt --check` only run locally today.
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
