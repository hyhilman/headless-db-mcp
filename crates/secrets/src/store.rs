use secrecy::SecretString;

use crate::error::SecretError;

/// Credential storage, keyed by an opaque string.
///
/// Mirrors the source project's per-connection-UUID + field-type Keychain
/// keying, but with the key format left to the caller. The convention used
/// by [`crate::EncryptedFileSecretStore`]'s callers is expected to be
/// `"<connection-uuid>:<field>"`, e.g. `"3f2a1c4e-...:password"` or
/// `"3f2a1c4e-...:ssh_passphrase"` — this crate does not parse or validate
/// that shape, it only requires the key to be non-empty.
#[async_trait::async_trait]
pub trait SecretStore: Send + Sync {
    /// Returns the stored secret for `key`, or `Ok(None)` if nothing has
    /// been stored under that key. A missing key is not an error.
    async fn get(&self, key: &str) -> Result<Option<SecretString>, SecretError>;

    /// Stores `value` under `key`, overwriting any existing value.
    async fn set(&self, key: &str, value: SecretString) -> Result<(), SecretError>;

    /// Removes any secret stored under `key`. Deleting a key that was
    /// never set is not an error.
    async fn delete(&self, key: &str) -> Result<(), SecretError>;
}
