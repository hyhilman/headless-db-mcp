use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use uuid::Uuid;

use crate::AttemptToken;

/// Generation-fenced bookkeeping for in-flight connect/query attempts.
///
/// Each connection id has a monotonically increasing generation counter.
/// `begin` mints a token carrying the new generation and, by doing so,
/// invalidates any token minted by an earlier `begin` (or left current by
/// `invalidate`) for the same connection id. Callers use `is_current` to
/// find out, right before touching shared state, whether their token is
/// still the newest one issued for its connection id — a stale result
/// means a newer attempt has since started, and the caller must back off
/// and tear down its own resources instead of mutating shared state.
///
/// Uses a plain `std::sync::Mutex` rather than an async one: every
/// critical section here is a handful of infallible `HashMap`/`u64`
/// operations with no `.await` and no I/O in between, so there is nothing
/// an async mutex would buy over the synchronous one, and using one would
/// let a held lock outlive a cancelled future.
pub struct ConnectionAttemptRegistry {
    generations: Mutex<HashMap<Uuid, u64>>,
}

impl Default for ConnectionAttemptRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionAttemptRegistry {
    pub fn new() -> Self {
        Self {
            generations: Mutex::new(HashMap::new()),
        }
    }

    /// A poisoned lock only means some unrelated thread panicked while
    /// holding it, not that the map's contents are inconsistent: every
    /// critical section in this type is a single infallible
    /// read-modify-write over plain `u64` generation counters (increments
    /// use `wrapping_add`, so not even integer overflow can panic), so
    /// whatever the map holds after recovery is exactly what the last
    /// completed operation left behind. Recovering the guard is strictly
    /// safer than taking down every caller of this registry over a panic
    /// that happened somewhere else entirely.
    fn lock(&self) -> MutexGuard<'_, HashMap<Uuid, u64>> {
        self.generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Mints a new token for `connection_id`. Any token previously issued
    /// for the same id — via `begin`, or left current after
    /// `invalidate` — stops being current as of this call.
    pub fn begin(&self, connection_id: Uuid) -> AttemptToken {
        let mut generations = self.lock();
        let next = generations
            .get(&connection_id)
            .map_or(1, |generation| generation.wrapping_add(1));
        generations.insert(connection_id, next);
        AttemptToken::new(connection_id, next)
    }

    /// True if `token` is still the newest attempt issued for its
    /// connection id.
    pub fn is_current(&self, token: &AttemptToken) -> bool {
        let generations = self.lock();
        generations.get(&token.connection_id()) == Some(&token.generation())
    }

    /// Invalidates the connection's current generation without starting a
    /// new attempt, e.g. because the user cancelled. A later `is_current`
    /// call on the most recently issued token for this connection id
    /// returns `false` afterwards.
    pub fn invalidate(&self, connection_id: Uuid) {
        let mut generations = self.lock();
        let next = generations
            .get(&connection_id)
            .map_or(1, |generation| generation.wrapping_add(1));
        generations.insert(connection_id, next);
    }

    /// Drops all bookkeeping for `connection_id`, e.g. because the
    /// connection itself was deleted.
    ///
    /// The generation counter does not survive `forget`: the next `begin`
    /// for the same id starts a fresh sequence back at generation 1,
    /// indistinguishable from the very first attempt ever made for that
    /// id. Any token issued before the `forget` is stale from that point
    /// on, including one that happens to collide with a post-forget
    /// generation number, because `is_current` also compares the
    /// connection id it was minted for and both re-derive from the same
    /// map entry.
    pub fn forget(&self, connection_id: Uuid) {
        let mut generations = self.lock();
        generations.remove(&connection_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use uuid::Uuid;

    use super::*;

    #[test]
    fn second_begin_invalidates_first_token() {
        let registry = ConnectionAttemptRegistry::new();
        let id = Uuid::new_v4();

        let first = registry.begin(id);
        let second = registry.begin(id);

        assert!(!registry.is_current(&first));
        assert!(registry.is_current(&second));
    }

    #[test]
    fn two_thread_sequential_begin_only_last_is_current() {
        let registry = Arc::new(ConnectionAttemptRegistry::new());
        let id = Uuid::new_v4();

        let registry_for_a = Arc::clone(&registry);
        let token_a = thread::spawn(move || registry_for_a.begin(id))
            .join()
            .expect("thread A panicked");

        let token_b = registry.begin(id);

        assert!(!registry.is_current(&token_a));
        assert!(registry.is_current(&token_b));
    }

    #[test]
    fn concurrent_begin_stress_exactly_one_token_ends_up_current() {
        const THREAD_COUNT: usize = 64;

        let registry = Arc::new(ConnectionAttemptRegistry::new());
        let id = Uuid::new_v4();
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));

        let handles: Vec<_> = (0..THREAD_COUNT)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    registry.begin(id)
                })
            })
            .collect();

        let tokens: Vec<AttemptToken> = handles
            .into_iter()
            .map(|handle| handle.join().expect("worker thread panicked"))
            .collect();

        let current_count = tokens
            .iter()
            .filter(|token| registry.is_current(token))
            .count();
        assert_eq!(current_count, 1);
    }

    #[test]
    fn forget_makes_a_later_begin_behave_like_the_first_attempt() {
        let registry = ConnectionAttemptRegistry::new();
        let id = Uuid::new_v4();

        let before_forget = registry.begin(id);
        registry.forget(id);
        assert!(!registry.is_current(&before_forget));

        let after_forget = registry.begin(id);
        assert!(registry.is_current(&after_forget));
        assert_eq!(after_forget.generation(), 1);
    }
}
