use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use crate::{AttemptToken, ConnectionAttemptRegistry};

/// Session storage keyed by connection id, gated by a
/// [`ConnectionAttemptRegistry`] so that only the current attempt for a
/// connection id may install (or replace) its session.
///
/// Generic over the session type so this crate stays free of any
/// particular driver trait: callers plug in whatever "connected" state
/// their layer owns (a live driver handle, a socket, a mock in tests).
pub struct SessionRegistry<S> {
    sessions: Mutex<HashMap<Uuid, S>>,
}

impl<S> Default for SessionRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> SessionRegistry<S> {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// See [`ConnectionAttemptRegistry`]'s lock-poisoning note: every
    /// critical section here is a plain, infallible `HashMap` operation,
    /// so recovering a poisoned guard cannot surface an inconsistent map.
    fn lock(&self) -> MutexGuard<'_, HashMap<Uuid, S>> {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Inserts `session` for `token.connection_id()` only if `token` is
    /// still the current attempt per `registry`.
    ///
    /// On success, returns whatever session was previously stored for
    /// that connection id, if any — the caller may still need to tear
    /// that down (e.g. an old driver being replaced by a new one). On
    /// failure the token was stale: `session` is handed back unchanged in
    /// `Err`, because a stale attempt's resources belong to that attempt,
    /// not to this registry. `SessionRegistry` never adopts or silently
    /// drops a session it did not accept.
    ///
    /// `token` is re-checked against `registry` both before and after
    /// acquiring the session-map lock. That narrows, without fully
    /// closing, the window in which a concurrent `begin`/`invalidate` on
    /// `registry` could race this call, since the two registries are
    /// guarded by separate mutexes and there is no single lock that
    /// covers "check the generation" and "install the session" as one
    /// step. `ConnectionAttemptRegistry`'s counter remains the single
    /// source of truth for which attempt is current; a caller that loses
    /// this narrow race still observes `is_current` return `false` on its
    /// own next check and tears itself down there.
    pub fn insert_if_current(
        &self,
        registry: &ConnectionAttemptRegistry,
        token: AttemptToken,
        session: S,
    ) -> Result<Option<S>, S> {
        if !registry.is_current(&token) {
            return Err(session);
        }

        let mut sessions = self.lock();

        if !registry.is_current(&token) {
            return Err(session);
        }

        Ok(sessions.insert(token.connection_id(), session))
    }

    pub fn remove(&self, connection_id: Uuid) -> Option<S> {
        self.lock().remove(&connection_id)
    }

    pub fn contains(&self, connection_id: Uuid) -> bool {
        self.lock().contains_key(&connection_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use uuid::Uuid;

    use super::*;

    struct TrackedSession {
        torn_down: Arc<AtomicBool>,
    }

    impl TrackedSession {
        fn teardown(self) {
            self.torn_down.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn insert_if_current_with_stale_token_hands_session_back_for_teardown() {
        let attempts = ConnectionAttemptRegistry::new();
        let sessions: SessionRegistry<TrackedSession> = SessionRegistry::new();
        let id = Uuid::new_v4();

        let stale_token = attempts.begin(id);
        let _current_token = attempts.begin(id);

        let torn_down = Arc::new(AtomicBool::new(false));
        let session = TrackedSession {
            torn_down: Arc::clone(&torn_down),
        };

        let result = sessions.insert_if_current(&attempts, stale_token, session);

        match result {
            Ok(_) => panic!("a stale token must never be accepted"),
            Err(returned_session) => returned_session.teardown(),
        }

        assert!(torn_down.load(Ordering::SeqCst));
        assert!(!sessions.contains(id));
    }

    #[test]
    fn insert_if_current_with_current_token_succeeds() {
        let attempts = ConnectionAttemptRegistry::new();
        let sessions: SessionRegistry<&'static str> = SessionRegistry::new();
        let id = Uuid::new_v4();

        let token = attempts.begin(id);

        let first_insert = sessions.insert_if_current(&attempts, token, "first");
        assert_eq!(first_insert, Ok(None));

        let second_insert = sessions.insert_if_current(&attempts, token, "second");
        assert_eq!(second_insert, Ok(Some("first")));

        assert_eq!(sessions.remove(id), Some("second"));
    }
}
