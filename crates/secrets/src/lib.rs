#![forbid(unsafe_code)]

//! Encrypted-at-rest credential storage for `db-headless-mcp`.
//!
//! The source project (TablePro) stores connection secrets in macOS
//! Keychain, which assumes an interactive login session. A headless
//! server has no such session, so this crate replaces Keychain with a
//! local, encrypted-at-rest store keyed by an opaque string — mirroring
//! the source project's per-connection-UUID + field-type Keychain keying
//! (see [`SecretStore`] for the convention).
//!
//! [`EncryptedFileSecretStore`] is the only backend today: a single JSON
//! file, AES-256-GCM per entry, master key from the
//! `DB_HEADLESS_MASTER_KEY` environment variable. There is deliberately no
//! default, generated, or zero fallback key — construction fails closed.

mod error;
mod file_store;
mod master_key;
mod store;

pub use error::SecretError;
pub use file_store::EncryptedFileSecretStore;
pub use store::SecretStore;
