use std::pin::Pin;

use async_trait::async_trait;
use futures_core::Stream;

use crate::config::ConnectionConfig;
use crate::error::{DriverError, DriverErrorKind};
use crate::result::QueryResult;
use crate::schema::{
    ColumnInfo, CreateDatabaseRequest, DatabaseMetadata, ForeignKeyInfo, IndexInfo, ParameterStyle,
    StreamElement, TableInfo, TableMetadata, TriggerInfo,
};
use crate::transport::KeepalivePosture;
use crate::value::CellValue;

pub type DriverResult<T> = Result<T, DriverError>;
pub type RowStream<'a> = Pin<Box<dyn Stream<Item = DriverResult<StreamElement>> + Send + 'a>>;

/// The contract every database backend implements.
///
/// This is the seam between the MCP tool layer and a specific database's
/// wire protocol, translated from the source project's
/// `PluginDatabaseDriver` protocol. Adding a driver must never require
/// changing this trait for existing drivers (guardrail #8) — extend via
/// a new default method instead of a breaking signature change.
///
/// Every method takes `&self`, not `&mut self`: implementations hold
/// their connection/session state behind interior mutability (a mutex or
/// the underlying client library's own synchronization), because the
/// registry (`db-headless-registry`) shares a driver instance across
/// concurrent MCP tool calls and cancellation paths.
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    async fn connect(&self) -> DriverResult<()>;
    async fn disconnect(&self) -> DriverResult<()>;

    async fn ping(&self) -> DriverResult<()> {
        self.execute("SELECT 1").await.map(|_| ())
    }

    async fn execute(&self, query: &str) -> DriverResult<QueryResult>;

    async fn execute_parameterized(
        &self,
        query: &str,
        parameters: &[CellValue],
    ) -> DriverResult<QueryResult>;

    /// Executes a user-supplied query with a row cap.
    ///
    /// Implementations must apply `crate::limits::RowLimits::EMERGENCY_MAX`
    /// as an absolute ceiling even if `row_cap` requests more (guardrail
    /// #7), and should prefer capping at the source (e.g. `LIMIT`,
    /// cursor `FETCH` sizing) over fetching the full result set into
    /// memory and truncating afterward.
    async fn execute_user_query(
        &self,
        query: &str,
        row_cap: Option<usize>,
        parameters: Option<&[CellValue]>,
    ) -> DriverResult<QueryResult>;

    async fn fetch_tables(&self, schema: Option<&str>) -> DriverResult<Vec<TableInfo>>;

    async fn fetch_columns(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<ColumnInfo>>;

    async fn fetch_indexes(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<IndexInfo>>;

    async fn fetch_foreign_keys(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<ForeignKeyInfo>>;

    async fn fetch_triggers(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<TriggerInfo>> {
        let _ = (table, schema);
        Ok(Vec::new())
    }

    async fn fetch_table_ddl(&self, table: &str, schema: Option<&str>) -> DriverResult<String>;

    async fn fetch_view_definition(&self, view: &str, schema: Option<&str>)
        -> DriverResult<String>;

    async fn fetch_table_metadata(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<TableMetadata>;

    async fn fetch_databases(&self) -> DriverResult<Vec<String>>;

    async fn fetch_database_metadata(&self, database: &str) -> DriverResult<DatabaseMetadata>;

    fn supports_schemas(&self) -> bool {
        false
    }

    async fn fetch_schemas(&self) -> DriverResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn switch_schema(&self, schema: &str) -> DriverResult<()> {
        let _ = schema;
        Err(DriverError::new(
            DriverErrorKind::Query,
            "this driver does not support schemas",
        ))
    }

    fn current_schema(&self) -> Option<String> {
        None
    }

    fn supports_transactions(&self) -> bool {
        true
    }

    async fn begin_transaction(&self) -> DriverResult<()> {
        self.execute("BEGIN").await.map(|_| ())
    }

    async fn commit_transaction(&self) -> DriverResult<()> {
        self.execute("COMMIT").await.map(|_| ())
    }

    async fn rollback_transaction(&self) -> DriverResult<()> {
        self.execute("ROLLBACK").await.map(|_| ())
    }

    /// Interrupts a query already in flight on this driver, out-of-band
    /// (i.e. usable from a different task than the one awaiting the
    /// query). Default is a no-op for drivers with no such mechanism;
    /// overriding this is strongly preferred over relying on task
    /// cancellation alone — see guardrail #5 and the source project's
    /// MySQL `KILL QUERY` / PostgreSQL `PQcancel` approaches, both of
    /// which use a side channel rather than hoping the blocking call
    /// notices cancellation.
    fn cancel_query(&self) -> DriverResult<()> {
        Ok(())
    }

    async fn apply_query_timeout(&self, seconds: u64) -> DriverResult<()> {
        let _ = seconds;
        Ok(())
    }

    /// Declares how this driver handles the transport keepalive policy
    /// (see `crate::transport` for why the policy exists).
    ///
    /// Deliberately has **no default implementation**, unlike
    /// `apply_query_timeout` above — a silent no-op default is exactly
    /// how a driver ships without a timeout, and keepalive must not
    /// repeat that. This is a knowing exception to the "extend via a
    /// new default method" guidance in this trait's doc (guardrail #8):
    /// a default posture would defeat the method's whole purpose.
    fn keepalive_posture(&self) -> KeepalivePosture;

    fn server_version(&self) -> Option<String> {
        None
    }

    fn parameter_style(&self) -> ParameterStyle;

    async fn create_database(&self, request: &CreateDatabaseRequest) -> DriverResult<()>;
    async fn drop_database(&self, name: &str) -> DriverResult<()>;
    async fn switch_database(&self, database: &str) -> DriverResult<()>;

    /// True row streaming, in chunks, without buffering the full result
    /// set. Unlike the source project (whose default implementation
    /// silently degraded to one `execute()` call plus a single batch),
    /// there is no default here — every driver must implement genuine
    /// chunked streaming or explicitly document why it cannot, rather
    /// than inherit a fake-streaming default that looks correct until a
    /// large result set OOMs the process.
    fn stream_rows<'a>(&'a self, query: &'a str) -> RowStream<'a>;
}

/// Constructs a driver instance for a resolved connection config. One
/// implementation per database type; the server's driver registry maps a
/// database-type string id (e.g. `"PostgreSQL"`) to a `DriverFactory`.
pub trait DriverFactory: Send + Sync {
    fn create_driver(&self, config: ConnectionConfig) -> Box<dyn DatabaseDriver>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    /// A minimal in-memory driver whose only purpose is to prove
    /// `DatabaseDriver` is object-safe (`Box<dyn DatabaseDriver>`
    /// compiles and is callable) and that the default method bodies
    /// (ping, transactions, cancel_query, ...) behave as documented.
    struct NullDriver;

    #[async_trait]
    impl DatabaseDriver for NullDriver {
        async fn connect(&self) -> DriverResult<()> {
            Ok(())
        }

        async fn disconnect(&self) -> DriverResult<()> {
            Ok(())
        }

        async fn execute(&self, _query: &str) -> DriverResult<QueryResult> {
            Ok(QueryResult::empty())
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
            Ok(Vec::new())
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

        async fn fetch_table_ddl(
            &self,
            _table: &str,
            _schema: Option<&str>,
        ) -> DriverResult<String> {
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
                    kind: crate::schema::TableKind::Table,
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
            Ok(Vec::new())
        }

        async fn fetch_database_metadata(&self, database: &str) -> DriverResult<DatabaseMetadata> {
            Ok(DatabaseMetadata {
                name: database.to_string(),
                schemas: Vec::new(),
                size_bytes: None,
            })
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

        fn keepalive_posture(&self) -> KeepalivePosture {
            KeepalivePosture::NotSupported {
                reason: "in-memory test driver; there is no socket to tune",
            }
        }

        fn stream_rows<'a>(&'a self, _query: &'a str) -> RowStream<'a> {
            Box::pin(stream::empty())
        }
    }

    fn boxed_driver() -> Box<dyn DatabaseDriver> {
        Box::new(NullDriver)
    }

    #[tokio::test]
    async fn trait_object_is_constructible_and_callable() {
        let driver = boxed_driver();
        driver.connect().await.expect("connect");
        driver.ping().await.expect("ping defaults to SELECT 1");
    }

    #[tokio::test]
    async fn default_transaction_methods_delegate_to_execute() {
        let driver = boxed_driver();
        driver.begin_transaction().await.expect("begin");
        driver.commit_transaction().await.expect("commit");
        driver.rollback_transaction().await.expect("rollback");
    }

    #[test]
    fn default_cancel_query_is_ok_noop() {
        let driver = boxed_driver();
        assert!(driver.cancel_query().is_ok());
    }

    #[test]
    fn default_supports_schemas_is_false() {
        let driver = boxed_driver();
        assert!(!driver.supports_schemas());
    }

    #[tokio::test]
    async fn default_switch_schema_errors_when_unsupported() {
        let driver = boxed_driver();
        let err = driver.switch_schema("public").await.unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[tokio::test]
    async fn default_fetch_triggers_is_empty() {
        let driver = boxed_driver();
        let triggers = driver.fetch_triggers("t", None).await.expect("ok");
        assert!(triggers.is_empty());
    }
}
