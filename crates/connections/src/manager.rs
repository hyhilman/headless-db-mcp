use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use db_headless_core::{
    ConnectionConfig, DatabaseDriver, DriverError, DriverFactory, QueryTimeouts,
};
use db_headless_registry::{AttemptToken, ConnectionAttemptRegistry, SessionRegistry};
use serde::Serialize;
use uuid::Uuid;

/// Everything the manager keeps about one live connection, beyond the
/// live driver handle itself. `SessionRegistry` (see its module doc
/// comment) intentionally exposes no "peek" accessor — only
/// `insert_if_current`, `remove`, and `contains` — because its only job
/// is gating the generation-fenced insert. Reads (`get`/`list`/`status`)
/// are served from this map instead, kept in lockstep with `sessions` at
/// every mutation point.
struct ConnectionEntry {
    driver: Arc<dyn DatabaseDriver>,
    database_type: String,
    connected_at: SystemTime,
}

/// A point-in-time snapshot of one connection, safe to hand back to an
/// MCP client as JSON.
///
/// `connected_at_unix_ms` is milliseconds since the Unix epoch. A
/// dedicated date/time crate is not worth adding to this crate just to
/// format a timestamp; a caller that wants a human-readable string can
/// convert from Unix millis with whatever formatting library the server
/// binary already depends on.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectionSummary {
    pub connection_id: Uuid,
    pub database_type: String,
    pub connected_at_unix_ms: u64,
}

impl ConnectionSummary {
    fn from_entry(connection_id: Uuid, entry: &ConnectionEntry) -> Self {
        let connected_at_unix_ms = entry
            .connected_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let connected_at_unix_ms = u64::try_from(connected_at_unix_ms).unwrap_or(u64::MAX);

        Self {
            connection_id,
            database_type: entry.database_type.clone(),
            connected_at_unix_ms,
        }
    }
}

/// Why a `ConnectionManager` operation failed.
///
/// `Display` never includes connection credentials: `ConnectionConfig`'s
/// `password` field is a `secrecy::SecretString` that is already excluded
/// from `Debug`/`Display`, and none of these variants manually
/// re-serialize a `ConnectionConfig` into a message.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    #[error("unknown database type: {0}")]
    UnknownDatabaseType(String),

    #[error("no such connection: {0}")]
    NotFound(Uuid),

    /// The connect attempt lost a generation-fencing race: a newer
    /// `connect()` (or other future attempt) for the same connection id
    /// started before this one finished. The driver this attempt built
    /// has already been disconnected by the manager itself — see
    /// `ConnectionManager::finish_connect`.
    #[error("connection attempt for {0} was superseded by a newer attempt for the same id")]
    Superseded(Uuid),

    #[error(transparent)]
    Driver(#[from] DriverError),
}

/// Driver-agnostic connection lifecycle manager.
///
/// Every `connect()` call mints a brand-new random connection id — this
/// is Phase 2 scope: there is no persisted, named-connection store yet
/// (that lands once `db-headless-secrets` is wired in), so "reconnect to
/// an existing logical connection" does not exist. Every connection this
/// manager knows about is ephemeral and in-memory only.
pub struct ConnectionManager {
    factories: HashMap<String, Arc<dyn DriverFactory>>,
    attempts: ConnectionAttemptRegistry,
    sessions: SessionRegistry<Arc<dyn DatabaseDriver>>,
    entries: Mutex<HashMap<Uuid, ConnectionEntry>>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
            attempts: ConnectionAttemptRegistry::new(),
            sessions: SessionRegistry::new(),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Registers a `DriverFactory` under a database-type id string (the
    /// source project's `DatabaseType.pluginTypeId` convention, e.g.
    /// `"PostgreSQL"`). Intended to be called a handful of times while
    /// building the manager, before it is shared behind an `Arc` — hence
    /// `&mut self` rather than interior mutability.
    pub fn register_driver_factory(
        &mut self,
        database_type: impl Into<String>,
        factory: Arc<dyn DriverFactory>,
    ) {
        self.factories.insert(database_type.into(), factory);
    }

    /// See [`ConnectionAttemptRegistry`]'s and `SessionRegistry`'s own
    /// lock-poisoning notes: every critical section under this lock is a
    /// plain, infallible `HashMap` operation with no `.await` in between,
    /// so recovering a poisoned guard cannot observe or produce an
    /// inconsistent map.
    fn lock_entries(&self) -> MutexGuard<'_, HashMap<Uuid, ConnectionEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Opens a new connection and returns its freshly minted id.
    pub async fn connect(
        &self,
        database_type: &str,
        config: ConnectionConfig,
    ) -> Result<Uuid, ConnectionManagerError> {
        let factory = self.factories.get(database_type).cloned().ok_or_else(|| {
            ConnectionManagerError::UnknownDatabaseType(database_type.to_string())
        })?;

        let id = Uuid::new_v4();
        let token = self.attempts.begin(id);

        let driver = factory.create_driver(config);
        if let Err(err) = driver.connect().await {
            self.attempts.invalidate(id);
            return Err(ConnectionManagerError::Driver(err));
        }

        // Best-effort: a driver that can't apply it (or doesn't support
        // one at all, like Redis) still gets `ExecuteQueryTool`'s
        // client-side backstop timeout, so this never blocks the connect.
        if let Err(err) = driver.apply_query_timeout(QueryTimeouts::SERVER_SECS).await {
            tracing::warn!(
                %id,
                error = %err,
                "failed to apply the default server-side query timeout to a new connection"
            );
        }

        let driver: Arc<dyn DatabaseDriver> = Arc::from(driver);
        let stored_driver = Arc::clone(&driver);

        self.finish_connect(token, driver).await?;

        let entry = ConnectionEntry {
            driver: stored_driver,
            database_type: database_type.to_string(),
            connected_at: SystemTime::now(),
        };
        self.lock_entries().insert(id, entry);

        Ok(id)
    }

    /// Installs `driver` into `sessions` only if `token` is still the
    /// current attempt for its connection id.
    ///
    /// This is the single most important correctness property in this
    /// crate: `SessionRegistry::insert_if_current` never tears down a
    /// session it refuses (see its doc comment) — on `Err`, the session
    /// it hands back belongs to the caller, not the registry. A caller
    /// that forgets this leaks a live driver connection every time a
    /// connect attempt loses this race.
    async fn finish_connect(
        &self,
        token: AttemptToken,
        driver: Arc<dyn DatabaseDriver>,
    ) -> Result<(), ConnectionManagerError> {
        let connection_id = token.connection_id();

        match self
            .sessions
            .insert_if_current(&self.attempts, token, driver)
        {
            Ok(None) => Ok(()),
            Ok(Some(previous)) => {
                if let Err(err) = previous.disconnect().await {
                    tracing::warn!(
                        %connection_id,
                        error = %err,
                        "failed to disconnect a session previously stored under the same connection id"
                    );
                }
                Ok(())
            }
            Err(orphaned) => {
                if let Err(err) = orphaned.disconnect().await {
                    tracing::warn!(
                        %connection_id,
                        error = %err,
                        "failed to disconnect an orphaned driver after losing the connect race"
                    );
                }
                Err(ConnectionManagerError::Superseded(connection_id))
            }
        }
    }

    /// Closes a live connection. Disconnecting an unknown id is treated
    /// as a no-op success rather than `NotFound`: callers commonly call
    /// `disconnect` defensively during cleanup, and "already gone" is not
    /// meaningfully different from "gone now" to that caller.
    pub async fn disconnect(&self, connection_id: Uuid) -> Result<(), ConnectionManagerError> {
        let entry = self.lock_entries().remove(&connection_id);
        self.sessions.remove(connection_id);
        self.attempts.forget(connection_id);

        match entry {
            Some(entry) => entry
                .driver
                .disconnect()
                .await
                .map_err(ConnectionManagerError::Driver),
            None => Ok(()),
        }
    }

    /// Looks up the live driver for a connection id, for tool
    /// implementations to call driver methods on.
    pub fn get(
        &self,
        connection_id: Uuid,
    ) -> Result<Arc<dyn DatabaseDriver>, ConnectionManagerError> {
        self.lock_entries()
            .get(&connection_id)
            .map(|entry| Arc::clone(&entry.driver))
            .ok_or(ConnectionManagerError::NotFound(connection_id))
    }

    pub fn list(&self) -> Vec<ConnectionSummary> {
        let mut summaries: Vec<ConnectionSummary> = self
            .lock_entries()
            .iter()
            .map(|(id, entry)| ConnectionSummary::from_entry(*id, entry))
            .collect();
        summaries.sort_by_key(|summary| summary.connection_id);
        summaries
    }

    pub fn status(&self, connection_id: Uuid) -> Result<ConnectionSummary, ConnectionManagerError> {
        self.lock_entries()
            .get(&connection_id)
            .map(|entry| ConnectionSummary::from_entry(connection_id, entry))
            .ok_or(ConnectionManagerError::NotFound(connection_id))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::test_support::{sample_config, MockDriverConfig, MockFactory};

    fn manager_with_mock(config: MockDriverConfig) -> ConnectionManager {
        let mut manager = ConnectionManager::new();
        manager.register_driver_factory("Mock", Arc::new(MockFactory(config)));
        manager
    }

    #[tokio::test]
    async fn connect_with_unknown_database_type_is_rejected() {
        let manager = manager_with_mock(MockDriverConfig::default());

        let err = manager
            .connect("DoesNotExist", sample_config())
            .await
            .unwrap_err();

        assert!(
            matches!(err, ConnectionManagerError::UnknownDatabaseType(ref t) if t == "DoesNotExist")
        );
        assert!(manager.list().is_empty());
    }

    #[tokio::test]
    async fn connect_success_is_visible_via_get_status_list() {
        let manager = manager_with_mock(MockDriverConfig::default());

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        assert!(manager.get(id).is_ok());

        let status = manager.status(id).expect("status");
        assert_eq!(status.connection_id, id);
        assert_eq!(status.database_type, "Mock");

        let list = manager.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].connection_id, id);
    }

    #[tokio::test]
    async fn connect_where_driver_connect_fails_leaves_nothing_registered() {
        let manager = manager_with_mock(MockDriverConfig::failing_connect());

        let err = manager.connect("Mock", sample_config()).await.unwrap_err();

        assert!(matches!(err, ConnectionManagerError::Driver(_)));
        assert!(manager.list().is_empty());
    }

    #[tokio::test]
    async fn orphaned_driver_from_lost_race_is_disconnected_by_manager() {
        let manager = manager_with_mock(MockDriverConfig::default());
        let id = Uuid::new_v4();

        let stale_token = manager.attempts.begin(id);
        let _current_token = manager.attempts.begin(id);

        let torn_down = Arc::new(AtomicBool::new(false));
        let driver: Arc<dyn DatabaseDriver> = Arc::new(crate::test_support::MockDriver::new(
            MockDriverConfig::with_disconnect_flag(Arc::clone(&torn_down)),
        ));

        let result = manager.finish_connect(stale_token, driver).await;

        assert!(
            matches!(result, Err(ConnectionManagerError::Superseded(superseded_id)) if superseded_id == id)
        );
        assert!(
            torn_down.load(Ordering::SeqCst),
            "ConnectionManager must disconnect a driver whose insert_if_current lost the race"
        );
        assert!(!manager.sessions.contains(id));
        assert!(manager.get(id).is_err());
    }

    #[tokio::test]
    async fn disconnect_calls_driver_disconnect_and_removes_session() {
        let torn_down = Arc::new(AtomicBool::new(false));
        let manager = manager_with_mock(MockDriverConfig::with_disconnect_flag(Arc::clone(
            &torn_down,
        )));

        let id = manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");
        assert!(manager.get(id).is_ok());

        manager.disconnect(id).await.expect("disconnect succeeds");

        assert!(torn_down.load(Ordering::SeqCst));
        assert!(manager.get(id).is_err());
        assert!(manager.list().is_empty());
    }

    #[tokio::test]
    async fn disconnecting_an_unknown_id_is_a_no_op() {
        let manager = manager_with_mock(MockDriverConfig::default());
        manager
            .disconnect(Uuid::new_v4())
            .await
            .expect("no-op success");
    }

    #[tokio::test]
    async fn connect_applies_the_default_server_side_query_timeout() {
        let recorder = Arc::new(std::sync::Mutex::new(None));
        let manager = manager_with_mock(MockDriverConfig::with_applied_query_timeout_recorder(
            Arc::clone(&recorder),
        ));

        manager
            .connect("Mock", sample_config())
            .await
            .expect("connect succeeds");

        assert_eq!(
            *recorder.lock().expect("recorder"),
            Some(QueryTimeouts::SERVER_SECS)
        );
    }
}
