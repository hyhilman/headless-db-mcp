#![forbid(unsafe_code)]

//! Generation-fenced connection lifecycle bookkeeping.
//!
//! Async cancellation is cooperative: cancelling a task cannot interrupt a
//! blocking call already underneath it, so a "cancelled" connect or query
//! attempt can keep running and complete later, after a newer attempt has
//! already taken its place. This crate is the mechanism that keeps such a
//! late-finishing attempt from clobbering shared state.
//!
//! [`ConnectionAttemptRegistry`] hands each in-flight attempt a
//! monotonically increasing generation token ([`AttemptToken`]) keyed by
//! connection id. Starting a new attempt for the same connection id
//! invalidates whatever token was issued before it. Before an attempt
//! mutates shared state it re-checks its token; a stale token means a
//! newer attempt has since started, and the stale attempt must back off
//! and tear down whatever it allocated instead of touching shared state.
//!
//! [`SessionRegistry`] is the shared state this pattern protects: a
//! generic, driver-agnostic session map that only accepts an insert from
//! the still-current attempt, handing the session straight back to a
//! stale caller instead of silently dropping (and leaking) it.
//!
//! This crate is pure in-memory bookkeeping — no I/O, no actual driver or
//! network code. It is the concurrency-safe primitive other code builds
//! cancel-safe connect/query logic on top of.

mod attempt_token;
mod connection_registry;
mod session_registry;

pub use attempt_token::AttemptToken;
pub use connection_registry::ConnectionAttemptRegistry;
pub use session_registry::SessionRegistry;
