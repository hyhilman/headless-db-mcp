#![forbid(unsafe_code)]

//! Redis `db_headless_core::DatabaseDriver` implementation, built on
//! `redis-rs`'s async, auto-reconnecting `redis::aio::ConnectionManager`.
//!
//! Redis is not SQL: there are no tables, schemas, or SQL-style
//! transactions. This crate is organized around that:
//!
//! - `driver.rs` — [`RedisDriver`] and [`RedisDriverFactory`], and the
//!   module-level doc comment there explains why `ping()`,
//!   `supports_transactions()`, and the transaction methods override the
//!   trait's SQL-shaped defaults rather than inheriting them, plus the
//!   `cancel_query` design (synchronous trait method, out-of-band
//!   `CLIENT KILL`).
//! - `schema.rs` — the **pseudo-table** model: Redis has no real schema,
//!   so this driver reports a fixed six-entry table list, one per Redis
//!   data type (`string`, `hash`, `list`, `set`, `zset`, `stream`), each
//!   with a fixed synthetic column list. This is a deliberate browsing
//!   convenience, not a real schema, and is documented as such at the
//!   top of that module.
//! - `config.rs` — builds a `redis::ConnectionInfo` directly (a struct
//!   literal, not a hand-formatted `redis://` URL) from
//!   `db_headless_core::ConnectionConfig`, with the same TLS-mode
//!   scoping precedent `driver-postgres` uses.
//! - `command.rs` — tokenizes a raw Redis command line and binds `?`
//!   placeholders as distinct RESP arguments (never by string
//!   concatenation).
//! - `query.rs` — `execute`/`execute_parameterized`/`execute_user_query`:
//!   run one arbitrary Redis command and convert the reply into a
//!   generic, un-opinionated `QueryResult`.
//! - `stream.rs` — `stream_rows`: real `SCAN ... TYPE ...` cursor-based
//!   iteration over one pseudo-table, yielding properly-shaped rows
//!   (real hash field/value pairs, real zset member/score pairs, ...).
//! - `convert.rs` — `redis::Value` → `CellValue`/JSON conversions shared
//!   by `query.rs` and `stream.rs`.
//! - `error.rs` — `redis::RedisError` → `db_headless_core::DriverError`.

mod command;
mod config;
mod convert;
mod driver;
mod error;
mod query;
mod schema;
mod stream;

pub use driver::{RedisDriver, RedisDriverFactory, DATABASE_TYPE_ID};
