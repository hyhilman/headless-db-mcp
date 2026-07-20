use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use rand::RngCore;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::error::SecretError;
use crate::master_key::MasterKey;
use crate::store::SecretStore;

const NONCE_LEN_BYTES: usize = 12;

#[derive(Debug, Serialize, Deserialize)]
struct StoredSecret {
    nonce: String,
    ciphertext: String,
}

type StoreFile = BTreeMap<String, StoredSecret>;

/// Encrypted-at-rest [`SecretStore`] backed by a single JSON file.
///
/// Each entry is AES-256-GCM encrypted with a fresh random 96-bit nonce
/// per write; the nonce and ciphertext are stored base64-encoded next to
/// the key they belong to. The file itself is never written in place —
/// every write lands in a temp file in the same directory that is then
/// renamed over the target, so a crash mid-write cannot leave a torn file.
///
/// # File shape
///
/// ```json
/// {
///   "3f2a1c4e-...:password": {
///     "nonce": "<base64, 12 bytes>",
///     "ciphertext": "<base64>"
///   }
/// }
/// ```
///
/// The master key never touches disk and is not part of this file; it is
/// supplied at construction time via [`EncryptedFileSecretStore::from_env`]
/// or [`EncryptedFileSecretStore::with_master_key_hex`].
pub struct EncryptedFileSecretStore {
    path: PathBuf,
    cipher: Aes256Gcm,
    write_lock: Mutex<()>,
}

impl EncryptedFileSecretStore {
    /// Builds a store backed by `path`, reading the master key from the
    /// `DB_HEADLESS_MASTER_KEY` environment variable. Returns `Err` if the
    /// variable is missing or is not 64 hex characters — this constructor
    /// never falls back to a default, generated, or zero key.
    pub fn from_env(path: impl Into<PathBuf>) -> Result<Self, SecretError> {
        Self::with_master_key(path, MasterKey::from_env()?)
    }

    /// Builds a store backed by `path`, using `master_key_hex` (64 hex
    /// characters, decoding to 32 bytes) as the AES-256-GCM key.
    ///
    /// This is the injectable entry point: production code should prefer
    /// [`Self::from_env`], and tests should use this instead of mutating
    /// process environment variables, so they stay deterministic under
    /// `cargo test`'s parallel execution.
    pub fn with_master_key_hex(
        path: impl Into<PathBuf>,
        master_key_hex: &str,
    ) -> Result<Self, SecretError> {
        Self::with_master_key(path, MasterKey::from_hex(master_key_hex)?)
    }

    fn with_master_key(
        path: impl Into<PathBuf>,
        master_key: MasterKey,
    ) -> Result<Self, SecretError> {
        let cipher = Aes256Gcm::new_from_slice(master_key.as_bytes()).map_err(|_| {
            SecretError::InvalidMasterKey {
                reason: "master key could not be loaded into the cipher".to_string(),
            }
        })?;

        Ok(Self {
            path: path.into(),
            cipher,
            write_lock: Mutex::new(()),
        })
    }

    fn read_file(&self) -> Result<StoreFile, SecretError> {
        if !self.path.exists() {
            return Ok(StoreFile::new());
        }

        let bytes = std::fs::read(&self.path)?;
        if bytes.is_empty() {
            return Ok(StoreFile::new());
        }

        serde_json::from_slice(&bytes).map_err(|source| SecretError::Corrupted {
            reason: format!("failed to parse secret store file: {source}"),
        })
    }

    fn write_file(&self, file: &StoreFile) -> Result<(), SecretError> {
        let json = serde_json::to_vec_pretty(file).map_err(|source| SecretError::Corrupted {
            reason: format!("failed to serialize secret store file: {source}"),
        })?;
        write_atomic(&self.path, &json)
    }

    fn encrypt(&self, key: &str, plaintext: &[u8]) -> Result<StoredSecret, SecretError> {
        let mut nonce_bytes = [0u8; NONCE_LEN_BYTES];
        rand::rngs::OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);

        let ciphertext =
            self.cipher
                .encrypt(&nonce, plaintext)
                .map_err(|_| SecretError::EncryptionFailed {
                    key: key.to_string(),
                })?;

        Ok(StoredSecret {
            nonce: BASE64.encode(nonce_bytes),
            ciphertext: BASE64.encode(ciphertext),
        })
    }

    fn decrypt(&self, key: &str, stored: &StoredSecret) -> Result<Vec<u8>, SecretError> {
        let nonce_bytes = BASE64
            .decode(&stored.nonce)
            .map_err(|_| SecretError::Corrupted {
                reason: format!("nonce for key '{key}' is not valid base64"),
            })?;
        let ciphertext = BASE64
            .decode(&stored.ciphertext)
            .map_err(|_| SecretError::Corrupted {
                reason: format!("ciphertext for key '{key}' is not valid base64"),
            })?;

        let nonce_bytes: [u8; NONCE_LEN_BYTES] =
            nonce_bytes
                .as_slice()
                .try_into()
                .map_err(|_| SecretError::Corrupted {
                    reason: format!("nonce for key '{key}' has the wrong length"),
                })?;
        let nonce = Nonce::from(nonce_bytes);

        self.cipher
            .decrypt(&nonce, ciphertext.as_slice())
            .map_err(|_| SecretError::DecryptionFailed {
                key: key.to_string(),
            })
    }
}

fn validate_key(key: &str) -> Result<(), SecretError> {
    if key.is_empty() {
        return Err(SecretError::EmptyKey);
    }
    Ok(())
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<(), SecretError> {
    let dir = path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir)?;

    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(contents)?;
    tmp.as_file().sync_all()?;

    set_owner_only_permissions(tmp.as_file())?;

    tmp.persist(path)
        .map_err(|persist_error| persist_error.error)?;
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_permissions(file: &std::fs::File) -> Result<(), SecretError> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_file: &std::fs::File) -> Result<(), SecretError> {
    Ok(())
}

#[async_trait::async_trait]
impl SecretStore for EncryptedFileSecretStore {
    async fn get(&self, key: &str) -> Result<Option<SecretString>, SecretError> {
        validate_key(key)?;

        let file = {
            let _guard = self
                .write_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.read_file()?
        };

        let Some(stored) = file.get(key) else {
            return Ok(None);
        };

        let plaintext = self.decrypt(key, stored)?;
        let text = String::from_utf8(plaintext).map_err(|_| SecretError::DecryptionFailed {
            key: key.to_string(),
        })?;

        Ok(Some(SecretString::from(text)))
    }

    async fn set(&self, key: &str, value: SecretString) -> Result<(), SecretError> {
        validate_key(key)?;

        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut file = self.read_file()?;
        let stored = self.encrypt(key, value.expose_secret().as_bytes())?;
        file.insert(key.to_string(), stored);
        self.write_file(&file)
    }

    async fn delete(&self, key: &str) -> Result<(), SecretError> {
        validate_key(key)?;

        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut file = self.read_file()?;
        file.remove(key);
        self.write_file(&file)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use secrecy::SecretString;
    use tempfile::TempDir;

    use super::*;
    use crate::master_key::MASTER_KEY_ENV_VAR;

    const TEST_KEY_HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const _: () = assert!(
        TEST_KEY_HEX.len() == 64,
        "test master key must be 64 hex characters"
    );

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn store_in(dir: &TempDir) -> EncryptedFileSecretStore {
        let path = dir.path().join("secrets.json");
        EncryptedFileSecretStore::with_master_key_hex(path, TEST_KEY_HEX)
            .expect("valid test master key must construct a store")
    }

    #[tokio::test]
    async fn round_trip_set_get_delete() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_in(&dir);
        let key = "3f2a1c4e-uuid:password";

        store
            .set(key, SecretString::from("hunter2".to_string()))
            .await
            .expect("set should succeed");

        let fetched = store.get(key).await.expect("get should succeed");
        assert_eq!(
            fetched.map(|secret| secret.expose_secret().to_string()),
            Some("hunter2".to_string())
        );

        store.delete(key).await.expect("delete should succeed");
        let after_delete = store
            .get(key)
            .await
            .expect("get after delete should succeed");
        assert!(after_delete.is_none());
    }

    #[tokio::test]
    async fn get_on_never_set_key_returns_none_not_error() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_in(&dir);

        let result = store.get("never-set:password").await;
        assert!(matches!(result, Ok(None)));
    }

    #[tokio::test]
    async fn empty_key_is_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_in(&dir);

        let result = store.set("", SecretString::from("value".to_string())).await;
        assert!(matches!(result, Err(SecretError::EmptyKey)));
    }

    #[tokio::test]
    async fn from_env_missing_var_returns_err() {
        let _guard = ENV_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::env::var(MASTER_KEY_ENV_VAR).ok();
        std::env::remove_var(MASTER_KEY_ENV_VAR);

        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("secrets.json");
        let result = EncryptedFileSecretStore::from_env(path);

        if let Some(value) = previous {
            std::env::set_var(MASTER_KEY_ENV_VAR, value);
        }

        assert!(matches!(result, Err(SecretError::MissingMasterKey)));
    }

    #[test]
    fn malformed_master_key_wrong_length_errs() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("secrets.json");

        let result = EncryptedFileSecretStore::with_master_key_hex(path, "abcd");
        assert!(matches!(result, Err(SecretError::InvalidMasterKey { .. })));
    }

    #[test]
    fn malformed_master_key_non_hex_errs() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("secrets.json");

        let not_hex = "zz".repeat(32);
        let result = EncryptedFileSecretStore::with_master_key_hex(path, &not_hex);
        assert!(matches!(result, Err(SecretError::InvalidMasterKey { .. })));
    }

    #[tokio::test]
    async fn identical_plaintext_produces_different_ciphertext_each_write() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_in(&dir);

        store
            .set(
                "uuid-a:password",
                SecretString::from("same-secret".to_string()),
            )
            .await
            .expect("set a");
        store
            .set(
                "uuid-b:password",
                SecretString::from("same-secret".to_string()),
            )
            .await
            .expect("set b");

        let raw = std::fs::read(dir.path().join("secrets.json")).expect("read raw store file");
        let file: StoreFile = serde_json::from_slice(&raw).expect("parse raw store file");

        let a = &file["uuid-a:password"];
        let b = &file["uuid-b:password"];

        assert_ne!(
            a.nonce, b.nonce,
            "nonce must be freshly randomized per write"
        );
        assert_ne!(
            a.ciphertext, b.ciphertext,
            "ciphertext must differ when the nonce differs"
        );
    }

    #[tokio::test]
    async fn secret_string_debug_output_never_contains_plaintext() {
        let dir = TempDir::new().expect("tempdir");
        let store = store_in(&dir);
        let plaintext = "super-secret-value";

        store
            .set("uuid:password", SecretString::from(plaintext.to_string()))
            .await
            .expect("set should succeed");

        let secret = store
            .get("uuid:password")
            .await
            .expect("get should succeed")
            .expect("secret must be present");

        let debug_output = format!("{secret:?}");
        assert!(
            !debug_output.contains(plaintext),
            "Debug output leaked the plaintext secret: {debug_output}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn store_file_has_owner_only_permissions_after_write() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("tempdir");
        let store = store_in(&dir);

        store
            .set("uuid:password", SecretString::from("value".to_string()))
            .await
            .expect("set should succeed");

        let metadata = std::fs::metadata(dir.path().join("secrets.json")).expect("metadata");
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected owner-only permissions, got {mode:o}");
    }
}
