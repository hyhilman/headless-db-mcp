//! TCP keepalive policy for driver transports.
//!
//! Why this exists: this server is designed to run behind VPN/NAT paths
//! (WireGuard split tunnels, Docker NAT, stateful cloud firewalls) that
//! cull TCP flows they consider idle — observed in practice culling
//! flows after only ~2 seconds of silence. A database connection is
//! exactly the kind of flow that goes silent while still healthy: the
//! server computes a slow query (a full scan, a big aggregate, even
//! `pg_sleep`) and sends nothing until the result is ready, so the
//! middlebox drops the flow's state and the result can never come back.
//! The client then observes not an error but *silence* — which is why
//! these failures surface as `QueryTimeouts::CLIENT_BACKSTOP_SECS`
//! cancellations rather than clean connection errors.
//!
//! Kernel TCP keepalive probes fix this from the client side alone: a
//! probe elicits an ACK from the database, and that round trip refreshes
//! the flow's state in every stateful device on the path, in both
//! directions. The policy here is aggressive on purpose — probes must
//! flow faster than the tightest culling window observed (~2s).

use std::time::Duration;

/// The one keepalive policy every socket-owning driver applies.
pub struct TransportKeepalive;

impl TransportKeepalive {
    /// Silence on the connection before the first probe is sent.
    pub const IDLE: Duration = Duration::from_secs(1);
    /// Interval between probes once probing has started.
    pub const INTERVAL: Duration = Duration::from_secs(1);
    /// Unanswered probes before the kernel declares the peer dead —
    /// which surfaces as a fast, clean I/O error (~4s total) instead of
    /// a hang that only the client backstop timeout can end.
    pub const RETRIES: u32 = 3;
}

/// How a driver relates to [`TransportKeepalive`]. Returned by
/// `DatabaseDriver::keepalive_posture`.
///
/// This is a *declaration*, deliberately separate from the socket work
/// itself: keepalive is a socket option set wherever each driver builds
/// its transport (`tokio_postgres::Config`, `reqwest::ClientBuilder`,
/// ...), so no trait method could apply it uniformly. The declaration's
/// job is to make the policy impossible to forget — the trait method has
/// no default implementation, so a new driver does not compile until its
/// author either applies the policy or writes down, in code, why the
/// underlying client cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeepalivePosture {
    /// The driver applies [`TransportKeepalive`] when it builds its
    /// transport.
    Applied,
    /// The underlying client library exposes no keepalive control. The
    /// reason must say why that is acceptable for this driver.
    NotSupported { reason: &'static str },
}
