use std::env;

use zeroize::Zeroizing;

use crate::error::SecretError;

/// Environment variable read by [`crate::EncryptedFileSecretStore::from_env`].
///
/// Expected value: 64 hex characters, decoding to exactly 32 bytes for
/// AES-256-GCM. There is no default — a missing or malformed value is
/// always an error, never a fallback key.
pub const MASTER_KEY_ENV_VAR: &str = "DB_HEADLESS_MASTER_KEY";

const KEY_LEN_BYTES: usize = 32;

/// A validated 32-byte AES-256-GCM key, held only long enough to build a
/// cipher instance. The backing bytes are zeroized on drop.
///
/// Deliberately has no `Debug`/`Display` impl: printing it is a compile
/// error, not a runtime discipline problem.
pub(crate) struct MasterKey {
    bytes: Zeroizing<[u8; KEY_LEN_BYTES]>,
}

impl MasterKey {
    /// Parses a master key from a 64-character hex string. This is the
    /// injectable entry point tests should use instead of mutating process
    /// environment variables.
    pub(crate) fn from_hex(hex_str: &str) -> Result<Self, SecretError> {
        let decoded = hex::decode(hex_str.trim()).map_err(|_| SecretError::InvalidMasterKey {
            reason: "master key must be valid hex".to_string(),
        })?;

        let bytes: [u8; KEY_LEN_BYTES] =
            decoded
                .as_slice()
                .try_into()
                .map_err(|_| SecretError::InvalidMasterKey {
                    reason: format!(
                        "master key must decode to {KEY_LEN_BYTES} bytes, got {}",
                        decoded.len()
                    ),
                })?;

        Ok(Self {
            bytes: Zeroizing::new(bytes),
        })
    }

    /// Reads and parses the master key from `DB_HEADLESS_MASTER_KEY`.
    /// Returns `Err` if the variable is unset or malformed — never falls
    /// back to a default, generated, or zero key.
    pub(crate) fn from_env() -> Result<Self, SecretError> {
        let raw = env::var(MASTER_KEY_ENV_VAR).map_err(|_| SecretError::MissingMasterKey)?;
        Self::from_hex(&raw)
    }

    pub(crate) fn as_bytes(&self) -> &[u8; KEY_LEN_BYTES] {
        &self.bytes
    }
}
