#![forbid(unsafe_code)]

//! Named, persisted connection profiles for `db-headless-mcp`.
//!
//! `crates/connections`' `connect` tool (Phase 2) only ever opened
//! ephemeral, in-memory connections: every call required the caller to
//! pass a plaintext password as a tool argument. That means an MCP
//! client (an AI agent, an orchestrator, anything driving this protocol)
//! had to be handed a real database password on every single connect
//! call — the credential travels through the model's context every time
//! it's used, not just once when it's provisioned.
//!
//! This crate closes that gap: [`ConnectionProfileManager`] lets an
//! operator save a named profile once via `save_connection_profile`
//! (host/port/username/password/database/ssl_mode, keyed by a
//! human-chosen `name`), after which every future `connect` call can
//! pass `profile_name` instead of raw credentials. The password is
//! never held by this crate directly — it round-trips through
//! `db_headless_secrets::SecretStore` (encrypted-at-rest, keyed
//! `"profile:<name>:password"`) on every `save`/`resolve`/`delete` call.
//! Everything else about the profile (host, port, username, database,
//! ssl_mode) is not a credential, so it lives in a plain (but
//! atomically-written, owner-only-permissions) JSON file instead.

mod error;
mod manager;
mod metadata_store;
mod profile;

pub use error::ProfileError;
pub use manager::{ConnectionProfileManager, ResolvedProfile, SaveProfileParams};
pub use profile::ConnectionProfile;
