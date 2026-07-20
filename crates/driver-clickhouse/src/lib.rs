#![forbid(unsafe_code)]

//! ClickHouse `db_headless_core::DatabaseDriver` implementation, talking
//! to ClickHouse's HTTP interface (default port 8123) via `reqwest` —
//! not the native binary protocol (port 9000).
//!
//! [`ClickHouseDriver`] holds an optional [`request::ConnectedClient`]
//! behind a `tokio::sync::RwLock`, constructed not-yet-connected by
//! [`ClickHouseDriverFactory::create_driver`] and brought up by a
//! separate `connect()` call, per the trait's lifecycle contract. See
//! `driver.rs` for why this driver needs none of `driver-postgres`'s
//! read/write-lock split between plain queries and portal-based ones:
//! ClickHouse's HTTP interface is stateless per request, so there is no
//! shared mutable session to serialize against.

mod driver;
mod error;
mod ident;
mod jsonl;
mod params;
mod query;
mod request;
mod schema;
mod statement;
mod stream;
mod tls;
mod tsv;

pub use driver::{ClickHouseDriver, ClickHouseDriverFactory, DATABASE_TYPE_ID};
