//! Schema-introspection transfer types.
//!
//! Field lists here are a reasonable starting shape translated from the
//! source project's `PluginTableInfo`/`PluginIndexInfo`/etc., but the
//! source's exact field lists were not fully enumerated during the
//! architecture survey. Treat these as provisional: the Phase 2
//! PostgreSQL driver is expected to validate (and adjust) them against
//! real `information_schema`/`pg_catalog` introspection queries before
//! other drivers rely on the shape being final.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::value::CellValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityKind {
    Always,
    ByDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
    pub extra: Option<String>,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub comment: Option<String>,
    pub identity_kind: Option<IdentityKind>,
    pub is_generated: bool,
    pub allowed_values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    Table,
    View,
    MaterializedView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub kind: TableKind,
    pub comment: Option<String>,
    pub row_count_estimate: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub method: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_schema: Option<String>,
    pub referenced_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub name: String,
    pub event: String,
    pub timing: String,
    pub definition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub info: TableInfo,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub triggers: Vec<TriggerInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetadata {
    pub name: String,
    pub schemas: Vec<String>,
    pub size_bytes: Option<u64>,
}

/// How a driver expects bound-parameter placeholders to be written in
/// query text (`?` vs `$1`). Purely descriptive — the driver itself is
/// responsible for the actual binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterStyle {
    QuestionMark,
    Dollar,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHeader {
    pub columns: Vec<String>,
    pub column_type_names: Vec<String>,
}

/// An element of a `DatabaseDriver::stream_rows` stream. Every stream
/// starts with exactly one `Header`, followed by zero or more `Rows`
/// batches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamElement {
    Header(StreamHeader),
    Rows(Vec<Vec<CellValue>>),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
    pub owner: Option<String>,
    pub encoding: Option<String>,
    #[serde(default)]
    pub additional_fields: HashMap<String, String>,
}
