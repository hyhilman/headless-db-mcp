//! `RedisDriver`: the `db_headless_core::DatabaseDriver` implementation
//! backed by `redis-rs`'s `redis::aio::ConnectionManager` — an
//! auto-reconnecting, cheaply `Clone`-able async connection handle, which
//! matches this driver's actual concurrency need: the registry shares one
//! `RedisDriver` instance across concurrent MCP tool calls (every trait
//! method takes `&self`), and `ConnectionManager` is explicitly documented
//! by `redis-rs` as safe to use that way. Every async method here reads
//! the current manager out from behind a `tokio::sync::RwLock`, clones it
//! (an `Arc` bump, not a new connection), and drops the lock before
//! issuing any command — so plain command execution never contends with
//! `connect`/`disconnect`/`switch_database`, which take the write lock
//! only for the instant it takes to swap the stored connection.
//!
//! # Why several trait defaults are overridden here
//!
//! Every other driver in this workspace is SQL-shaped, and three of
//! `DatabaseDriver`'s default method bodies assume SQL syntax that is
//! actively dangerous against a real Redis server if left un-overridden:
//!
//! - The default `ping()` runs `execute("SELECT 1")`. Redis has a real
//!   command literally named `SELECT`, and `SELECT 1` is valid Redis
//!   syntax that **switches the connection to database index 1** — an
//!   un-overridden `ping()` would silently change which database every
//!   subsequent command runs against, on every health check. `ping()`
//!   below issues Redis's real `PING` command instead and never touches
//!   `execute`.
//! - The default `begin_transaction`/`commit_transaction`/
//!   `rollback_transaction` run `execute("BEGIN"/"COMMIT"/"ROLLBACK")`,
//!   none of which are valid Redis commands (Redis's equivalent,
//!   `MULTI`/`EXEC`/`DISCARD`, has different semantics — no partial
//!   rollback — and is out of scope for this pass). `supports_transactions`
//!   returns `false` and the three methods return a clear `DriverError`
//!   instead of sending literal, invalid commands to the server.
//!
//! # `cancel_query`: a real design constraint, not a shortcut
//!
//! `cancel_query` is **synchronous** in the trait (callable without
//! `.await`, from a different task than the one running the blocked
//! command). Redis's real cancellation mechanism, `CLIENT KILL ID
//! <id>`, must be issued from a *different* connection — a connection
//! cannot kill its own in-flight blocking command from inside itself.
//! `establish` queries `CLIENT ID` once, right after connecting, and
//! caches it in a `std::sync::Mutex<Option<i64>>` (mirroring
//! `driver-postgres`'s cached `CancelToken`). `cancel_query` then
//! `tokio::spawn`s a **detached, best-effort** task (valid here — the
//! whole server runs under a `#[tokio::main]` runtime already) that opens
//! a short-lived new connection and issues `CLIENT KILL ID` against the
//! cached id, logging failure via `tracing::warn!` rather than
//! surfacing it (the synchronous caller already got `Ok(())` back — there
//! is no channel back from the detached task). This is blunt (it kills
//! the whole connection, not just the one command) but it is the only
//! shape available given the trait's synchronous signature.

use std::sync::Mutex;

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use tokio::sync::RwLock;

use db_headless_core::{
    CellValue, ColumnInfo, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver,
    DatabaseMetadata, DriverError, DriverErrorKind, DriverFactory, DriverResult, ForeignKeyInfo,
    IndexInfo, ParameterStyle, QueryResult, RowStream, TableInfo, TableKind, TableMetadata,
    TriggerInfo,
};

use crate::{config, error, query, schema, stream};

pub struct RedisDriver {
    config: ConnectionConfig,
    pub(crate) connection: RwLock<Option<ConnectionManager>>,
    client_id: Mutex<Option<i64>>,
}

pub(crate) fn not_connected_error() -> DriverError {
    DriverError::new(DriverErrorKind::Connection, "not connected")
}

fn unsupported_transaction_error() -> DriverError {
    DriverError::new(
        DriverErrorKind::Query,
        "Redis transactions are not supported through this interface yet",
    )
}

fn fixed_database_pool_error(verb: &str) -> DriverError {
    DriverError::new(
        DriverErrorKind::Query,
        format!(
            "Redis's numbered databases are a fixed pool sized by the server's `databases` \
             config directive (which requires a server restart to resize); there is no \
             per-connection way to {verb} one"
        ),
    )
}

impl RedisDriver {
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            connection: RwLock::new(None),
            client_id: Mutex::new(None),
        }
    }

    async fn manager(&self) -> DriverResult<ConnectionManager> {
        self.connection
            .read()
            .await
            .clone()
            .ok_or_else(not_connected_error)
    }

    async fn establish(&self, db_override: Option<i64>) -> DriverResult<(ConnectionManager, i64)> {
        let mut info = config::build_connection_info(&self.config)?;
        if let Some(db) = db_override {
            info.redis.db = db;
        }

        let client = redis::Client::open(info).map_err(error::map_connect_error)?;
        let mut manager = client
            .get_connection_manager()
            .await
            .map_err(error::map_connect_error)?;
        let client_id = redis::cmd("CLIENT")
            .arg("ID")
            .query_async::<i64>(&mut manager)
            .await
            .map_err(error::map_connect_error)?;

        Ok((manager, client_id))
    }

    async fn adopt(&self, manager: ConnectionManager, client_id: i64) {
        let mut guard = self.connection.write().await;
        *guard = Some(manager);
        drop(guard);
        *self.client_id.lock().unwrap_or_else(|p| p.into_inner()) = Some(client_id);
    }
}

#[async_trait]
impl DatabaseDriver for RedisDriver {
    async fn connect(&self) -> DriverResult<()> {
        let (manager, client_id) = self.establish(None).await?;
        self.adopt(manager, client_id).await;
        Ok(())
    }

    async fn disconnect(&self) -> DriverResult<()> {
        let mut guard = self.connection.write().await;
        *guard = None;
        drop(guard);
        *self.client_id.lock().unwrap_or_else(|p| p.into_inner()) = None;
        Ok(())
    }

    async fn ping(&self) -> DriverResult<()> {
        let mut manager = self.manager().await?;
        redis::cmd("PING")
            .query_async::<redis::Value>(&mut manager)
            .await
            .map(|_| ())
            .map_err(error::map_query_error)
    }

    async fn execute(&self, query: &str) -> DriverResult<QueryResult> {
        let mut manager = self.manager().await?;
        query::execute(&mut manager, query).await
    }

    async fn execute_parameterized(
        &self,
        query: &str,
        parameters: &[CellValue],
    ) -> DriverResult<QueryResult> {
        let mut manager = self.manager().await?;
        query::execute_parameterized(&mut manager, query, parameters).await
    }

    async fn execute_user_query(
        &self,
        query: &str,
        row_cap: Option<usize>,
        parameters: Option<&[CellValue]>,
    ) -> DriverResult<QueryResult> {
        let mut manager = self.manager().await?;
        query::execute_user_query(
            &mut manager,
            query,
            row_cap,
            parameters,
            self.config.read_only,
        )
        .await
    }

    async fn fetch_tables(&self, _schema: Option<&str>) -> DriverResult<Vec<TableInfo>> {
        self.manager().await?;
        Ok(schema::fetch_tables())
    }

    async fn fetch_columns(
        &self,
        table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<ColumnInfo>> {
        self.manager().await?;
        schema::columns_for(table)
    }

    async fn fetch_indexes(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<IndexInfo>> {
        self.manager().await?;
        Ok(Vec::new())
    }

    async fn fetch_foreign_keys(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<ForeignKeyInfo>> {
        self.manager().await?;
        Ok(Vec::new())
    }

    async fn fetch_triggers(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<TriggerInfo>> {
        self.manager().await?;
        Ok(Vec::new())
    }

    async fn fetch_table_ddl(&self, _table: &str, _schema: Option<&str>) -> DriverResult<String> {
        self.manager().await?;
        Err(DriverError::new(
            DriverErrorKind::Query,
            "DDL is not applicable to Redis: pseudo-tables are a browsing convenience over a \
             data type, not real schema objects with a definition to reconstruct",
        ))
    }

    async fn fetch_view_definition(
        &self,
        _view: &str,
        _schema: Option<&str>,
    ) -> DriverResult<String> {
        self.manager().await?;
        Err(DriverError::new(
            DriverErrorKind::Query,
            "views are not applicable to Redis",
        ))
    }

    async fn fetch_table_metadata(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<TableMetadata> {
        let columns = self.fetch_columns(table, schema).await?;
        Ok(TableMetadata {
            info: TableInfo {
                name: table.to_string(),
                schema: None,
                kind: TableKind::Table,
                comment: Some(
                    "Redis pseudo-table: a synthetic view over one Redis data type, not a real \
                     schema object."
                        .to_string(),
                ),
                row_count_estimate: None,
            },
            columns,
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            triggers: Vec::new(),
        })
    }

    async fn fetch_databases(&self) -> DriverResult<Vec<String>> {
        let mut manager = self.manager().await?;
        schema::fetch_databases(&mut manager).await
    }

    async fn fetch_database_metadata(&self, database: &str) -> DriverResult<DatabaseMetadata> {
        self.manager().await?;
        Ok(DatabaseMetadata {
            name: database.to_string(),
            schemas: Vec::new(),
            size_bytes: None,
        })
    }

    fn supports_schemas(&self) -> bool {
        false
    }

    fn supports_transactions(&self) -> bool {
        false
    }

    async fn begin_transaction(&self) -> DriverResult<()> {
        Err(unsupported_transaction_error())
    }

    async fn commit_transaction(&self) -> DriverResult<()> {
        Err(unsupported_transaction_error())
    }

    async fn rollback_transaction(&self) -> DriverResult<()> {
        Err(unsupported_transaction_error())
    }

    fn cancel_query(&self) -> DriverResult<()> {
        let client_id = self
            .client_id
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .ok_or_else(not_connected_error)?;
        let config = self.config.clone();

        tokio::spawn(async move {
            if let Err(err) = kill_client(&config, client_id).await {
                tracing::warn!(
                    error = %err,
                    "failed to send redis CLIENT KILL for query cancellation"
                );
            }
        });

        Ok(())
    }

    fn parameter_style(&self) -> ParameterStyle {
        ParameterStyle::QuestionMark
    }

    async fn create_database(&self, _request: &CreateDatabaseRequest) -> DriverResult<()> {
        Err(fixed_database_pool_error("create"))
    }

    async fn drop_database(&self, _name: &str) -> DriverResult<()> {
        Err(fixed_database_pool_error("drop"))
    }

    async fn switch_database(&self, database: &str) -> DriverResult<()> {
        let db = config::parse_db_index(Some(database))?;
        let (manager, client_id) = self.establish(Some(db)).await?;
        self.adopt(manager, client_id).await;
        Ok(())
    }

    fn stream_rows<'a>(&'a self, query: &'a str) -> RowStream<'a> {
        stream::stream_rows(self, query)
    }
}

/// Opens a short-lived, unmanaged connection (not a `ConnectionManager` —
/// no reconnect logic is wanted for a one-shot admin command) purely to
/// send `CLIENT KILL ID` against the driver's own cached client id.
async fn kill_client(config: &ConnectionConfig, client_id: i64) -> DriverResult<()> {
    let info = config::build_connection_info(config)?;
    let client = redis::Client::open(info).map_err(error::map_connect_error)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(error::map_connect_error)?;

    redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(client_id)
        .query_async::<redis::Value>(&mut connection)
        .await
        .map_err(error::map_query_error)?;

    Ok(())
}

pub struct RedisDriverFactory;

impl DriverFactory for RedisDriverFactory {
    fn create_driver(&self, config: ConnectionConfig) -> Box<dyn DatabaseDriver> {
        Box::new(RedisDriver::new(config))
    }
}

pub const DATABASE_TYPE_ID: &str = "Redis";
