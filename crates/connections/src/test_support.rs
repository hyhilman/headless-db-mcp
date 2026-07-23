//! Shared test doubles for the connection manager and tool tests.
//!
//! Mirrors `db_headless_core::driver`'s own `NullDriver` test pattern
//! (a minimal, fully-defaulted `DatabaseDriver`) but adds configurable
//! behavior — a failing `connect()`, canned table/database/query
//! results, and a disconnect-observed flag — so manager- and tool-level
//! tests can exercise real code paths without a real database. Only
//! compiled under `#[cfg(test)]`, so this never ships in the crate's
//! public API or a release build.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use db_headless_core::{
    CellValue, ColumnInfo, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver,
    DatabaseMetadata, DriverError, DriverErrorKind, DriverFactory, DriverResult, ForeignKeyInfo,
    IndexInfo, ParameterStyle, QueryResult, RowStream, SslConfig, TableInfo, TableKind,
    TableMetadata,
};
use futures_util::stream;
use secrecy::SecretString;

/// Configuration for a [`MockDriver`], cloned fresh into each driver a
/// [`MockFactory`] produces so a single config can be reused across
/// `connect()` calls in a test while still letting each produced driver
/// be inspected (e.g. via `disconnected_flag`) independently.
#[derive(Clone, Default)]
pub(crate) struct MockDriverConfig {
    pub fail_connect: bool,
    pub disconnected_flag: Option<Arc<AtomicBool>>,
    pub tables: Vec<TableInfo>,
    pub databases: Vec<String>,
    pub schemas: Vec<String>,
    pub query_result: Option<QueryResult>,
    pub query_delay: Option<Duration>,
    pub cancel_query_flag: Option<Arc<AtomicBool>>,
    pub applied_query_timeout: Option<Arc<Mutex<Option<u64>>>>,
}

impl MockDriverConfig {
    pub fn failing_connect() -> Self {
        Self {
            fail_connect: true,
            ..Self::default()
        }
    }

    pub fn with_disconnect_flag(flag: Arc<AtomicBool>) -> Self {
        Self {
            disconnected_flag: Some(flag),
            ..Self::default()
        }
    }

    pub fn with_tables(tables: Vec<TableInfo>) -> Self {
        Self {
            tables,
            ..Self::default()
        }
    }

    pub fn with_databases(databases: Vec<String>) -> Self {
        Self {
            databases,
            ..Self::default()
        }
    }

    pub fn with_schemas(schemas: Vec<String>) -> Self {
        Self {
            schemas,
            ..Self::default()
        }
    }

    pub fn with_query_result(query_result: QueryResult) -> Self {
        Self {
            query_result: Some(query_result),
            ..Self::default()
        }
    }

    pub fn with_query_delay_and_cancel_flag(delay: Duration, flag: Arc<AtomicBool>) -> Self {
        Self {
            query_delay: Some(delay),
            cancel_query_flag: Some(flag),
            ..Self::default()
        }
    }

    pub fn with_applied_query_timeout_recorder(recorder: Arc<Mutex<Option<u64>>>) -> Self {
        Self {
            applied_query_timeout: Some(recorder),
            ..Self::default()
        }
    }
}

pub(crate) struct MockDriver(MockDriverConfig);

impl MockDriver {
    pub fn new(config: MockDriverConfig) -> Self {
        Self(config)
    }
}

#[async_trait]
impl DatabaseDriver for MockDriver {
    async fn connect(&self) -> DriverResult<()> {
        if self.0.fail_connect {
            return Err(DriverError::new(
                DriverErrorKind::Connection,
                "mock connect failure",
            ));
        }
        Ok(())
    }

    async fn disconnect(&self) -> DriverResult<()> {
        if let Some(flag) = &self.0.disconnected_flag {
            flag.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn execute(&self, _query: &str) -> DriverResult<QueryResult> {
        if let Some(delay) = self.0.query_delay {
            tokio::time::sleep(delay).await;
        }
        Ok(self
            .0
            .query_result
            .clone()
            .unwrap_or_else(QueryResult::empty))
    }

    async fn execute_parameterized(
        &self,
        query: &str,
        _parameters: &[CellValue],
    ) -> DriverResult<QueryResult> {
        self.execute(query).await
    }

    async fn execute_user_query(
        &self,
        query: &str,
        _row_cap: Option<usize>,
        _parameters: Option<&[CellValue]>,
    ) -> DriverResult<QueryResult> {
        self.execute(query).await
    }

    async fn fetch_tables(&self, _schema: Option<&str>) -> DriverResult<Vec<TableInfo>> {
        Ok(self.0.tables.clone())
    }

    async fn fetch_columns(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<ColumnInfo>> {
        Ok(Vec::new())
    }

    async fn fetch_indexes(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<IndexInfo>> {
        Ok(Vec::new())
    }

    async fn fetch_foreign_keys(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<ForeignKeyInfo>> {
        Ok(Vec::new())
    }

    async fn fetch_table_ddl(&self, _table: &str, _schema: Option<&str>) -> DriverResult<String> {
        Ok(String::new())
    }

    async fn fetch_view_definition(
        &self,
        _view: &str,
        _schema: Option<&str>,
    ) -> DriverResult<String> {
        Ok(String::new())
    }

    async fn fetch_table_metadata(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<TableMetadata> {
        Ok(TableMetadata {
            info: TableInfo {
                name: table.to_string(),
                schema: schema.map(str::to_string),
                kind: TableKind::Table,
                comment: None,
                row_count_estimate: None,
            },
            columns: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            triggers: Vec::new(),
        })
    }

    async fn fetch_databases(&self) -> DriverResult<Vec<String>> {
        Ok(self.0.databases.clone())
    }

    async fn fetch_database_metadata(&self, database: &str) -> DriverResult<DatabaseMetadata> {
        Ok(DatabaseMetadata {
            name: database.to_string(),
            schemas: Vec::new(),
            size_bytes: None,
        })
    }

    fn supports_schemas(&self) -> bool {
        !self.0.schemas.is_empty()
    }

    async fn fetch_schemas(&self) -> DriverResult<Vec<String>> {
        Ok(self.0.schemas.clone())
    }

    fn parameter_style(&self) -> ParameterStyle {
        ParameterStyle::Dollar
    }

    async fn create_database(&self, _request: &CreateDatabaseRequest) -> DriverResult<()> {
        Ok(())
    }

    async fn drop_database(&self, _name: &str) -> DriverResult<()> {
        Ok(())
    }

    async fn switch_database(&self, _database: &str) -> DriverResult<()> {
        Ok(())
    }

    fn stream_rows<'a>(&'a self, _query: &'a str) -> RowStream<'a> {
        Box::pin(stream::empty())
    }

    fn cancel_query(&self) -> DriverResult<()> {
        if let Some(flag) = &self.0.cancel_query_flag {
            flag.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn apply_query_timeout(&self, seconds: u64) -> DriverResult<()> {
        if let Some(recorder) = &self.0.applied_query_timeout {
            *recorder.lock().unwrap_or_else(|p| p.into_inner()) = Some(seconds);
        }
        Ok(())
    }
}

pub(crate) struct MockFactory(pub MockDriverConfig);

impl DriverFactory for MockFactory {
    fn create_driver(&self, _config: ConnectionConfig) -> Box<dyn DatabaseDriver> {
        Box::new(MockDriver::new(self.0.clone()))
    }
}

pub(crate) fn sample_config() -> ConnectionConfig {
    ConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        password: Some(SecretString::from("hunter2".to_string())),
        database: Some("app".to_string()),
        ssl: SslConfig::default(),
        read_only: false,
        additional_fields: HashMap::new(),
    }
}
