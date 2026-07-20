use thiserror::Error;

use crate::master_key::MASTER_KEY_ENV_VAR;

/// Uniform error surface for every [`crate::SecretStore`] backend.
///
/// None of these variants may carry a secret value, a raw master key, or
/// ciphertext — only enough context (a storage key name, an I/O path
/// error, a parse failure) to debug the failure. This is checked by
/// `tests::debug_output_never_contains_plaintext` and by the crypto
/// failure variants below only ever embedding the storage key, never the
/// key material or the (de/en)crypted bytes.
#[derive(Debug, Error)]
pub enum SecretError {
    #[error("secret store I/O failure: {0}")]
    Io(#[from] std::io::Error),

    #[error("missing master key: set the {MASTER_KEY_ENV_VAR} environment variable to 64 hex characters (32 bytes)")]
    MissingMasterKey,

    #[error("invalid master key: {reason}")]
    InvalidMasterKey { reason: String },

    #[error("secret key must not be empty")]
    EmptyKey,

    #[error("encryption failed for key '{key}'")]
    EncryptionFailed { key: String },

    #[error("decryption failed for key '{key}'")]
    DecryptionFailed { key: String },

    #[error("secret store file is corrupted: {reason}")]
    Corrupted { reason: String },
}
