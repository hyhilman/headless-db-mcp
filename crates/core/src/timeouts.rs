//! Query timeouts applied automatically to every connection, independent
//! of whether a caller asks for one.

/// Two deliberately different durations, not one:
///
/// - [`QueryTimeouts::SERVER_SECS`] is pushed into the database engine
///   itself via `DatabaseDriver::apply_query_timeout`, called once right
///   after every successful connect (Postgres: `SET statement_timeout`;
///   ClickHouse: `max_execution_time` on every request). This is the
///   normal path: the engine cancels the query on its own and returns a
///   clean error, and the connection stays usable for the next query.
/// - [`QueryTimeouts::CLIENT_BACKSTOP_SECS`] wraps the query call itself
///   (`ExecuteQueryTool`) and is longer than `SERVER_SECS` on purpose, so
///   the engine's own timeout gets the first chance to fire. It only
///   trips when the connection isn't communicating at all -- the same
///   packet-loss case where the engine's own timeout enforcement can't
///   get a cancellation request through either. On trip it calls
///   `DatabaseDriver::cancel_query` (a real out-of-band cancel per
///   driver: Postgres `PQcancel`, ClickHouse cancel-by-query-id, Redis
///   `CLIENT KILL`) and returns a clear error instead of hanging forever.
///
/// A driver with no real `apply_query_timeout` (Redis, whose commands
/// are not long-running "statements" the same way SQL is) still gets the
/// client-side backstop, since that layer lives in `ExecuteQueryTool`,
/// not in the driver.
pub struct QueryTimeouts;

impl QueryTimeouts {
    pub const SERVER_SECS: u64 = 30;
    pub const CLIENT_BACKSTOP_SECS: u64 = 45;
}

const _: () = assert!(
    QueryTimeouts::CLIENT_BACKSTOP_SECS > QueryTimeouts::SERVER_SECS,
    "the client backstop must stay longer than the server-side timeout, or it fires first \
     and the engine never gets a chance to cancel the query cleanly on its own"
);
