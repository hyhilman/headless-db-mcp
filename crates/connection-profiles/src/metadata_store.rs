use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::ProfileError;
use crate::profile::ConnectionProfile;

type StoreFile = BTreeMap<String, ConnectionProfile>;

/// Plain-JSON, atomic-write store for [`ConnectionProfile`] metadata.
///
/// This file holds no secrets (host/port/username/database/ssl_mode are
/// not credentials), so unlike `db_headless_secrets::EncryptedFileSecretStore`
/// it is not encrypted. It still writes atomically (temp file + rename in
/// the same directory) and restricts file permissions on Unix, since the
/// contents still reveal connection topology and usernames worth keeping
/// off a shared filesystem.
///
/// Reads and writes go straight to disk on every call rather than caching
/// in memory, the same tradeoff `EncryptedFileSecretStore` makes: this
/// process is not expected to be a high-QPS hot path, and staying
/// stateless avoids ever serving a stale profile after an out-of-band
/// edit to the file.
pub(crate) struct ProfileMetadataStore {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl ProfileMetadataStore {
    pub(crate) fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Mutex::new(()),
        }
    }

    fn read_file(&self) -> Result<StoreFile, ProfileError> {
        if !self.path.exists() {
            return Ok(StoreFile::new());
        }

        let bytes = std::fs::read(&self.path)?;
        if bytes.is_empty() {
            return Ok(StoreFile::new());
        }

        serde_json::from_slice(&bytes).map_err(|source| ProfileError::Corrupted {
            reason: format!("failed to parse connection profile store file: {source}"),
        })
    }

    fn write_file(&self, file: &StoreFile) -> Result<(), ProfileError> {
        let json = serde_json::to_vec_pretty(file).map_err(|source| ProfileError::Corrupted {
            reason: format!("failed to serialize connection profile store file: {source}"),
        })?;
        write_atomic(&self.path, &json)
    }

    pub(crate) fn list(&self) -> Result<Vec<ConnectionProfile>, ProfileError> {
        let file = {
            let _guard = self
                .write_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.read_file()?
        };
        Ok(file.into_values().collect())
    }

    pub(crate) fn get(&self, name: &str) -> Result<Option<ConnectionProfile>, ProfileError> {
        let file = {
            let _guard = self
                .write_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.read_file()?
        };
        Ok(file.get(name).cloned())
    }

    pub(crate) fn upsert(&self, profile: ConnectionProfile) -> Result<(), ProfileError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut file = self.read_file()?;
        file.insert(profile.name.clone(), profile);
        self.write_file(&file)
    }

    /// Returns whether an entry was actually removed, so the caller can
    /// distinguish "deleted" from "was already gone" and surface a clear
    /// `NotFound` rather than silently succeeding on a typo'd name.
    pub(crate) fn remove(&self, name: &str) -> Result<bool, ProfileError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut file = self.read_file()?;
        let removed = file.remove(name).is_some();
        self.write_file(&file)?;
        Ok(removed)
    }
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<(), ProfileError> {
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
fn set_owner_only_permissions(file: &std::fs::File) -> Result<(), ProfileError> {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    file.set_permissions(permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_file: &std::fs::File) -> Result<(), ProfileError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn sample(name: &str) -> ConnectionProfile {
        ConnectionProfile {
            name: name.to_string(),
            database_type: "PostgreSQL".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            username: "app".to_string(),
            database: Some("appdb".to_string()),
            ssl_mode: None,
            has_password: true,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
        }
    }

    #[test]
    fn round_trip_upsert_get_list_remove() {
        let dir = TempDir::new().expect("tempdir");
        let store = ProfileMetadataStore::new(dir.path().join("profiles.json"));

        store.upsert(sample("prod")).expect("upsert");
        assert_eq!(
            store.get("prod").expect("get").map(|p| p.name),
            Some("prod".to_string())
        );
        assert_eq!(store.list().expect("list").len(), 1);

        assert!(store.remove("prod").expect("remove"));
        assert!(store.get("prod").expect("get after remove").is_none());
    }

    #[test]
    fn removing_unknown_name_returns_false_not_error() {
        let dir = TempDir::new().expect("tempdir");
        let store = ProfileMetadataStore::new(dir.path().join("profiles.json"));

        assert!(!store.remove("never-existed").expect("remove"));
    }

    #[test]
    fn get_on_empty_store_returns_none() {
        let dir = TempDir::new().expect("tempdir");
        let store = ProfileMetadataStore::new(dir.path().join("profiles.json"));

        assert!(store.get("missing").expect("get").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn store_file_has_owner_only_permissions_after_write() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("profiles.json");
        let store = ProfileMetadataStore::new(&path);

        store.upsert(sample("prod")).expect("upsert");

        let metadata = std::fs::metadata(&path).expect("metadata");
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "expected owner-only permissions, got {mode:o}");
    }
}
