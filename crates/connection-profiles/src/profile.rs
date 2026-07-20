use db_headless_core::SslMode;
use serde::{Deserialize, Serialize};

/// Non-secret metadata for one named, persisted connection.
///
/// Deliberately excludes the password: it never exists in memory as part
/// of this type, only as a [`secrecy::SecretString`] fetched on demand
/// from the `SecretStore` by [`crate::ConnectionProfileManager::resolve`].
/// `has_password` records whether a password is currently stored, so
/// `list_connection_profiles` can tell an operator "this profile has a
/// saved credential" without ever touching the secret store or coming
/// close to exposing the value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub name: String,
    pub database_type: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub database: Option<String>,
    pub ssl_mode: Option<SslMode>,
    #[serde(default)]
    pub read_only: bool,
    pub has_password: bool,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}
