use thiserror::Error;

use db_headless_secrets::SecretError;

/// Uniform error surface for [`crate::ConnectionProfileManager`].
///
/// No variant here may embed a password or other secret value: the only
/// string data these carry is a profile name or an I/O/parse failure
/// reason, mirroring the same guarantee `SecretError` makes.
#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("connection profile metadata store I/O failure: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection profile metadata store is corrupted: {reason}")]
    Corrupted { reason: String },

    #[error("connection profile name must not be empty")]
    EmptyName,

    #[error("no such connection profile: {name}")]
    NotFound { name: String },

    #[error(transparent)]
    Secret(#[from] SecretError),
}
