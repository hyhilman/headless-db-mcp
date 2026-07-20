use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use db_headless_core::{ConnectionConfig, SslConfig, SslMode};
use db_headless_secrets::SecretStore;
use secrecy::SecretString;

use crate::error::ProfileError;
use crate::metadata_store::ProfileMetadataStore;
use crate::profile::ConnectionProfile;

/// Arguments to [`ConnectionProfileManager::save`].
///
/// `password: None` means "don't change the stored credential" on an
/// update to an existing profile (so an operator can fix a typo'd host
/// without re-typing the password), and "no password stored" for a brand
/// new profile (some backends, e.g. a passwordless local Redis, have
/// none). There is deliberately no separate "clear the password" flag in
/// this phase — delete and recreate the profile if a stored password
/// needs to be removed without replacing it.
#[derive(Debug, Clone)]
pub struct SaveProfileParams {
    pub name: String,
    pub database_type: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<SecretString>,
    pub database: Option<String>,
    pub ssl_mode: Option<SslMode>,
}

/// A profile resolved back into a connection-ready shape: the driver-type
/// id [`crate::ConnectionProfileManager::connect`] callers pass to
/// `ConnectionManager::connect`, plus a fully populated `ConnectionConfig`
/// (password included, fetched fresh from the secret store on every
/// call — never cached).
#[derive(Debug, Clone)]
pub struct ResolvedProfile {
    pub database_type: String,
    pub config: ConnectionConfig,
}

fn secret_key(name: &str) -> String {
    format!("profile:{name}:password")
}

fn now_unix_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn validate_name(name: &str) -> Result<(), ProfileError> {
    if name.is_empty() {
        return Err(ProfileError::EmptyName);
    }
    Ok(())
}

/// Named, persisted connection credentials.
///
/// Splits storage across two tiers, same separation `db-headless-secrets`
/// documents as guardrail #2: [`ProfileMetadataStore`] holds everything
/// that is safe to read in plaintext (host, port, username, database,
/// ssl_mode), and the password — the only field that actually needs
/// encryption-at-rest — lives in the injected `SecretStore`, keyed by
/// `"profile:<name>:password"`.
///
/// This is what lets an MCP client `connect` by `profile_name` without
/// the caller (an AI agent, a script, anything driving the MCP protocol)
/// ever needing to see, hold, or pass the password again after the one
/// `save_connection_profile` call that stored it.
pub struct ConnectionProfileManager {
    metadata: ProfileMetadataStore,
    secrets: Arc<dyn SecretStore>,
}

impl ConnectionProfileManager {
    pub fn new(metadata_path: impl Into<PathBuf>, secrets: Arc<dyn SecretStore>) -> Self {
        Self {
            metadata: ProfileMetadataStore::new(metadata_path),
            secrets,
        }
    }

    /// Creates a new profile or updates an existing one by name.
    pub async fn save(&self, params: SaveProfileParams) -> Result<(), ProfileError> {
        validate_name(&params.name)?;

        let existing = self.metadata.get(&params.name)?;

        let has_password = if let Some(password) = params.password {
            self.secrets
                .set(&secret_key(&params.name), password)
                .await?;
            true
        } else {
            existing.as_ref().is_some_and(|p| p.has_password)
        };

        let now = now_unix_ms();
        let created_at_unix_ms = existing.as_ref().map_or(now, |p| p.created_at_unix_ms);

        let profile = ConnectionProfile {
            name: params.name,
            database_type: params.database_type,
            host: params.host,
            port: params.port,
            username: params.username,
            database: params.database,
            ssl_mode: params.ssl_mode,
            has_password,
            created_at_unix_ms,
            updated_at_unix_ms: now,
        };

        self.metadata.upsert(profile)
    }

    pub fn list(&self) -> Result<Vec<ConnectionProfile>, ProfileError> {
        let mut profiles = self.metadata.list()?;
        profiles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(profiles)
    }

    /// Fetches a stored password by profile name and builds a fully
    /// populated `ConnectionConfig`, ready to hand to
    /// `ConnectionManager::connect` alongside `database_type`.
    pub async fn resolve(&self, name: &str) -> Result<ResolvedProfile, ProfileError> {
        validate_name(name)?;
        let profile = self
            .metadata
            .get(name)?
            .ok_or_else(|| ProfileError::NotFound {
                name: name.to_string(),
            })?;

        let password = self.secrets.get(&secret_key(name)).await?;

        let config = ConnectionConfig {
            host: profile.host,
            port: profile.port,
            username: profile.username,
            password,
            database: profile.database,
            ssl: SslConfig {
                mode: profile.ssl_mode,
                ca_path: None,
                client_cert_path: None,
                client_key_path: None,
            },
            additional_fields: HashMap::new(),
        };

        Ok(ResolvedProfile {
            database_type: profile.database_type,
            config,
        })
    }

    /// Deletes a profile's metadata and its stored password (if any).
    /// Deleting an unknown name is `ProfileError::NotFound`, not a silent
    /// no-op — unlike `ConnectionManager::disconnect`, this is an explicit
    /// operator action on a named resource, so a typo'd name should be
    /// caught, not swallowed.
    pub async fn delete(&self, name: &str) -> Result<(), ProfileError> {
        validate_name(name)?;

        let removed = self.metadata.remove(name)?;
        if !removed {
            return Err(ProfileError::NotFound {
                name: name.to_string(),
            });
        }

        self.secrets.delete(&secret_key(name)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;
    use tempfile::TempDir;

    use super::*;

    struct InMemorySecretStore {
        values: std::sync::Mutex<HashMap<String, SecretString>>,
    }

    impl InMemorySecretStore {
        fn new() -> Self {
            Self {
                values: std::sync::Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl SecretStore for InMemorySecretStore {
        async fn get(
            &self,
            key: &str,
        ) -> Result<Option<SecretString>, db_headless_secrets::SecretError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        async fn set(
            &self,
            key: &str,
            value: SecretString,
        ) -> Result<(), db_headless_secrets::SecretError> {
            self.values.lock().unwrap().insert(key.to_string(), value);
            Ok(())
        }

        async fn delete(&self, key: &str) -> Result<(), db_headless_secrets::SecretError> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }
    }

    fn manager_in(dir: &TempDir) -> ConnectionProfileManager {
        ConnectionProfileManager::new(
            dir.path().join("profiles.json"),
            Arc::new(InMemorySecretStore::new()),
        )
    }

    fn sample_params(name: &str) -> SaveProfileParams {
        SaveProfileParams {
            name: name.to_string(),
            database_type: "PostgreSQL".to_string(),
            host: "localhost".to_string(),
            port: 5432,
            username: "app".to_string(),
            password: Some(SecretString::from("hunter2".to_string())),
            database: Some("appdb".to_string()),
            ssl_mode: None,
        }
    }

    #[tokio::test]
    async fn save_then_resolve_round_trips_password() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        manager.save(sample_params("prod")).await.expect("save");
        let resolved = manager.resolve("prod").await.expect("resolve");

        assert_eq!(resolved.database_type, "PostgreSQL");
        assert_eq!(resolved.config.host, "localhost");
        assert_eq!(
            resolved
                .config
                .password
                .as_ref()
                .map(|s| s.expose_secret().to_string()),
            Some("hunter2".to_string())
        );
    }

    #[tokio::test]
    async fn list_never_exposes_password_field() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        manager.save(sample_params("prod")).await.expect("save");
        let profiles = manager.list().expect("list");

        assert_eq!(profiles.len(), 1);
        assert!(profiles[0].has_password);
        let json = serde_json::to_string(&profiles[0]).expect("serialize");
        assert!(
            !json.contains("hunter2"),
            "serialized profile must never contain the actual secret value"
        );
    }

    #[tokio::test]
    async fn updating_without_a_new_password_keeps_the_old_one() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        manager.save(sample_params("prod")).await.expect("save");

        let mut update = sample_params("prod");
        update.password = None;
        update.host = "new-host".to_string();
        manager.save(update).await.expect("update");

        let resolved = manager.resolve("prod").await.expect("resolve");
        assert_eq!(resolved.config.host, "new-host");
        assert_eq!(
            resolved
                .config
                .password
                .as_ref()
                .map(|s| s.expose_secret().to_string()),
            Some("hunter2".to_string())
        );

        let profiles = manager.list().expect("list");
        assert!(profiles[0].has_password);
    }

    #[tokio::test]
    async fn new_profile_without_password_has_no_password_and_resolves_none() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        let mut params = sample_params("local-redis");
        params.password = None;
        manager.save(params).await.expect("save");

        let profiles = manager.list().expect("list");
        assert!(!profiles[0].has_password);

        let resolved = manager.resolve("local-redis").await.expect("resolve");
        assert!(resolved.config.password.is_none());
    }

    #[tokio::test]
    async fn resolve_unknown_profile_is_not_found() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        let err = manager.resolve("does-not-exist").await.unwrap_err();
        assert!(matches!(err, ProfileError::NotFound { .. }));
    }

    #[tokio::test]
    async fn delete_removes_metadata_and_password() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        manager.save(sample_params("prod")).await.expect("save");
        manager.delete("prod").await.expect("delete");

        assert!(manager.list().expect("list").is_empty());
        let err = manager.resolve("prod").await.unwrap_err();
        assert!(matches!(err, ProfileError::NotFound { .. }));
    }

    #[tokio::test]
    async fn deleting_unknown_profile_is_not_found_not_a_silent_success() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        let err = manager.delete("never-existed").await.unwrap_err();
        assert!(matches!(err, ProfileError::NotFound { .. }));
    }

    #[tokio::test]
    async fn empty_name_is_rejected_on_save_resolve_delete() {
        let dir = TempDir::new().expect("tempdir");
        let manager = manager_in(&dir);

        let mut params = sample_params("");
        params.name = String::new();
        assert!(matches!(
            manager.save(params).await,
            Err(ProfileError::EmptyName)
        ));
        assert!(matches!(
            manager.resolve("").await,
            Err(ProfileError::EmptyName)
        ));
        assert!(matches!(
            manager.delete("").await,
            Err(ProfileError::EmptyName)
        ));
    }
}
