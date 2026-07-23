#![forbid(unsafe_code)]

//! Core driver contract and transfer types shared by every database
//! backend and the MCP server. See `README.md` at the workspace root for
//! the guardrails this crate and its implementations must uphold.

pub mod config;
pub mod driver;
pub mod error;
pub mod limits;
pub mod result;
pub mod schema;
pub mod timeouts;
pub mod transport;
pub mod value;

pub use config::{ConnectionConfig, SslConfig, SslMode};
pub use driver::{DatabaseDriver, DriverFactory, DriverResult, RowStream};
pub use error::{DriverError, DriverErrorKind};
pub use limits::RowLimits;
pub use result::QueryResult;
pub use schema::{
    ColumnInfo, CreateDatabaseRequest, DatabaseMetadata, ForeignKeyInfo, IdentityKind, IndexInfo,
    ParameterStyle, StreamElement, StreamHeader, TableInfo, TableKind, TableMetadata, TriggerInfo,
};
pub use timeouts::QueryTimeouts;
pub use transport::{KeepalivePosture, TransportKeepalive};
pub use value::CellValue;
