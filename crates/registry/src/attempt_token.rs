use uuid::Uuid;

/// Proof that a specific attempt was (or still is) the current attempt for
/// its connection id.
///
/// An `AttemptToken` is minted by [`crate::ConnectionAttemptRegistry::begin`]
/// and is only meaningful together with the registry that issued it: pass
/// it to [`crate::ConnectionAttemptRegistry::is_current`] (or let
/// [`crate::SessionRegistry::insert_if_current`] do that for you) to find
/// out whether a newer attempt has since superseded it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptToken {
    connection_id: Uuid,
    generation: u64,
}

impl AttemptToken {
    pub(crate) fn new(connection_id: Uuid, generation: u64) -> Self {
        Self {
            connection_id,
            generation,
        }
    }

    pub fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}
