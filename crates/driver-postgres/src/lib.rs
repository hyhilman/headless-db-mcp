#![forbid(unsafe_code)]

//! PostgreSQL `db_headless_core::DatabaseDriver` implementation, built on
//! `tokio_postgres`.
//!
//! [`PostgresDriver`] holds an optional `tokio_postgres::Client` behind a
//! `tokio::sync::RwLock`, constructed not-yet-connected by
//! [`PostgresDriverFactory::create_driver`] and brought up by a separate
//! `connect()` call, per the trait's lifecycle contract. See `driver.rs`
//! for the concurrency design (why some methods take a read lock and two
//! — `execute_user_query`, `stream_rows` — take a write lock).

mod config;
mod convert;
mod driver;
mod error;
mod ident;
mod params;
mod query;
mod schema;
mod stream;
mod tls;

pub use driver::{PostgresDriver, PostgresDriverFactory, DATABASE_TYPE_ID};
