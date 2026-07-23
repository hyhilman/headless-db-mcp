//! `ClickHouseDriver`: the `db_headless_core::DatabaseDriver`
//! implementation talking to ClickHouse's HTTP interface (default port
//! 8123), not the native binary protocol (port 9000). Every request is a
//! plain `POST /` with the SQL as the request body; ClickHouse's HTTP
//! interface is stateless per request, so unlike `driver-postgres` there
//! is no persistent session/socket to hold — `connect()` builds a
//! `reqwest::Client` (with TLS configured per `crate::tls`) and proves it
//! works with one lightweight probe request, and every other method just
//! reuses that client.
//!
//! Every method still takes `&self`, per the trait's contract (the
//! registry shares one driver instance across concurrent MCP tool calls).
//! Because HTTP requests carry no shared mutable session the way a single
//! Postgres TCP connection does, there is no need for `driver-postgres`'s
//! read/write-lock split between plain queries and portal-based ones —
//! every method here only ever needs read access to the connected
//! client, and concurrent requests run as fully independent HTTP calls.
//!
//! `cancel_query` is synchronous per the trait, so — exactly like
//! `driver-postgres`'s cached `CancelToken` — the pieces it needs
//! (client, base URL, credentials) are cached in a plain
//! `std::sync::Mutex<Option<CancelContext>>`, refreshed on every
//! successful `connect`, and read without touching the async `RwLock`.
//! `active_query_id` is a second, similarly synchronous-friendly
//! `std::sync::Mutex<Option<String>>` naming *the* query currently in
//! flight on this driver so `cancel_query` knows which `query_id` to
//! `KILL QUERY` — a single slot, matching the trait's framing of
//! cancelling "a query already in flight on this driver", not a
//! multiplexed table of every concurrent call.

use std::sync::Mutex;

use async_trait::async_trait;
use db_headless_core::{
    CellValue, ColumnInfo, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver,
    DatabaseMetadata, DriverError, DriverErrorKind, DriverFactory, DriverResult, ForeignKeyInfo,
    IndexInfo, KeepalivePosture, ParameterStyle, QueryResult, RowStream, TableInfo, TableMetadata,
};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::ident::quote_literal;
use crate::request::{send, CancelContext, ConnectedClient};
use crate::{query, schema, stream, tls};

pub struct ClickHouseDriver {
    pub(crate) config: ConnectionConfig,
    pub(crate) client: RwLock<Option<ConnectedClient>>,
    pub(crate) current_database: Mutex<String>,
    server_version: Mutex<Option<String>>,
    active_query_id: Mutex<Option<String>>,
    query_timeout_seconds: Mutex<Option<u64>>,
    cancel_context: Mutex<Option<CancelContext>>,
}

pub(crate) fn not_connected_error() -> DriverError {
    DriverError::new(DriverErrorKind::Connection, "not connected")
}

/// Clears `ClickHouseDriver::active_query_id` when dropped, so every exit
/// path out of a query (success, error via `?`, or an early `break` once
/// `execute_user_query`'s row cap is hit) reliably releases the "query
/// currently in flight" slot, without every call site having to remember
/// to do it manually.
pub(crate) struct ActiveQueryGuard<'a> {
    driver: &'a ClickHouseDriver,
    query_id: String,
}

impl<'a> ActiveQueryGuard<'a> {
    pub(crate) fn new(driver: &'a ClickHouseDriver, query_id: String) -> Self {
        Self { driver, query_id }
    }
}

impl Drop for ActiveQueryGuard<'_> {
    fn drop(&mut self) {
        self.driver.clear_active_query_id(&self.query_id);
    }
}

impl ClickHouseDriver {
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            client: RwLock::new(None),
            current_database: Mutex::new("default".to_string()),
            server_version: Mutex::new(None),
            active_query_id: Mutex::new(None),
            query_timeout_seconds: Mutex::new(None),
            cancel_context: Mutex::new(None),
        }
    }

    pub(crate) fn current_database(&self) -> String {
        self.current_database
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn set_current_database(&self, database: String) {
        *self
            .current_database
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = database;
    }

    pub(crate) fn query_timeout_seconds(&self) -> Option<u64> {
        *self
            .query_timeout_seconds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    fn set_active_query_id(&self, query_id: String) {
        *self
            .active_query_id
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(query_id);
    }

    fn clear_active_query_id(&self, query_id: &str) {
        let mut guard = self
            .active_query_id
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if guard.as_deref() == Some(query_id) {
            *guard = None;
        }
    }

    /// Sends `body` against the connected client, tagging the request
    /// with a fresh `query_id` (recorded as *the* in-flight query for
    /// `cancel_query`) plus `database=`/an optional `max_execution_time=`
    /// derived from `apply_query_timeout`. Returns the response (already
    /// checked for a successful status) and an `ActiveQueryGuard` that
    /// releases the in-flight slot when the caller is done with it.
    pub(crate) async fn send_request<'a>(
        &'a self,
        connected: &ConnectedClient,
        body: String,
        extra_params: &[(String, String)],
    ) -> DriverResult<(reqwest::Response, ActiveQueryGuard<'a>)> {
        let query_id = Uuid::new_v4().to_string();
        self.set_active_query_id(query_id.clone());

        let mut params: Vec<(String, String)> = vec![
            ("database".to_string(), self.current_database()),
            ("query_id".to_string(), query_id.clone()),
        ];
        if let Some(seconds) = self.query_timeout_seconds() {
            params.push(("max_execution_time".to_string(), seconds.to_string()));
        }
        // `readonly=1` is ClickHouse's own HTTP interface setting: the
        // server itself rejects any write or setting change, on every
        // request this driver sends (introspection queries are already
        // `SELECT`s, so this is a no-op for them). Applied here, in the
        // one place every query path funnels through, rather than in
        // each of `execute`/`execute_parameterized`/`execute_user_query`
        // separately.
        if self.config.read_only {
            params.push(("readonly".to_string(), "1".to_string()));
        }
        params.extend_from_slice(extra_params);

        let username = tls::resolve_username(&self.config);
        let response = send(
            &connected.client,
            &connected.base_url,
            username,
            self.config.password.as_ref(),
            body,
            &params,
        )
        .await;

        match response {
            Ok(response) => Ok((response, ActiveQueryGuard::new(self, query_id))),
            Err(err) => {
                self.clear_active_query_id(&query_id);
                Err(err)
            }
        }
    }
}

#[async_trait]
impl DatabaseDriver for ClickHouseDriver {
    async fn connect(&self) -> DriverResult<()> {
        let base_url = tls::resolve_base_url(&self.config);
        let http_client = tls::build_http_client(&self.config)?;
        let database = self
            .config
            .database
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let username = tls::resolve_username(&self.config);

        let probe_response = send(
            &http_client,
            &base_url,
            username,
            self.config.password.as_ref(),
            "SELECT version()".to_string(),
            &[
                ("database".to_string(), database.clone()),
                (
                    "default_format".to_string(),
                    "TabSeparatedWithNamesAndTypes".to_string(),
                ),
                ("wait_end_of_query".to_string(), "1".to_string()),
            ],
        )
        .await?;
        let body = probe_response
            .text()
            .await
            .map_err(crate::error::map_reqwest_error)?;
        let (_, _, rows) = crate::tsv::parse_full(&body)?;
        let version = rows
            .first()
            .and_then(|row| row.first())
            .and_then(|cell| cell.as_text())
            .map(str::to_string);

        self.set_current_database(database);
        *self
            .server_version
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = version;
        *self
            .cancel_context
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(CancelContext {
            client: http_client.clone(),
            base_url: base_url.clone(),
            username: username.to_string(),
            password: self.config.password.clone(),
        });

        let mut guard = self.client.write().await;
        *guard = Some(ConnectedClient {
            client: http_client,
            base_url,
        });
        Ok(())
    }

    async fn disconnect(&self) -> DriverResult<()> {
        let mut guard = self.client.write().await;
        *guard = None;
        *self
            .server_version
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        *self
            .active_query_id
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        *self
            .cancel_context
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        self.set_current_database("default".to_string());
        Ok(())
    }

    async fn execute(&self, sql: &str) -> DriverResult<QueryResult> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        query::execute(self, connected, sql).await
    }

    async fn execute_parameterized(
        &self,
        sql: &str,
        parameters: &[CellValue],
    ) -> DriverResult<QueryResult> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        query::execute_parameterized(self, connected, sql, parameters).await
    }

    async fn execute_user_query(
        &self,
        sql: &str,
        row_cap: Option<usize>,
        parameters: Option<&[CellValue]>,
    ) -> DriverResult<QueryResult> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        query::execute_user_query_capped(self, connected, sql, row_cap, parameters).await
    }

    async fn fetch_tables(&self, schema: Option<&str>) -> DriverResult<Vec<TableInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let database = schema
            .map(str::to_string)
            .unwrap_or_else(|| self.current_database());
        schema::fetch_tables(self, connected, &database).await
    }

    async fn fetch_columns(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<ColumnInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let database = schema
            .map(str::to_string)
            .unwrap_or_else(|| self.current_database());
        schema::fetch_columns(self, connected, table, &database).await
    }

    async fn fetch_indexes(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<IndexInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let database = schema
            .map(str::to_string)
            .unwrap_or_else(|| self.current_database());
        schema::fetch_indexes(self, connected, table, &database).await
    }

    async fn fetch_foreign_keys(
        &self,
        _table: &str,
        _schema: Option<&str>,
    ) -> DriverResult<Vec<ForeignKeyInfo>> {
        // ClickHouse has no foreign key constraints at all — it is a
        // columnar OLAP store with no referential-integrity engine. This
        // is a permanent, real "no such thing here", not a gap.
        Ok(Vec::new())
    }

    async fn fetch_table_ddl(&self, table: &str, schema: Option<&str>) -> DriverResult<String> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let database = schema
            .map(str::to_string)
            .unwrap_or_else(|| self.current_database());
        schema::fetch_create_query(self, connected, table, &database).await
    }

    async fn fetch_view_definition(
        &self,
        view: &str,
        schema: Option<&str>,
    ) -> DriverResult<String> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let database = schema
            .map(str::to_string)
            .unwrap_or_else(|| self.current_database());
        schema::fetch_create_query(self, connected, view, &database).await
    }

    async fn fetch_table_metadata(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<TableMetadata> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let database = schema
            .map(str::to_string)
            .unwrap_or_else(|| self.current_database());
        schema::fetch_table_metadata(self, connected, table, &database).await
    }

    async fn fetch_databases(&self) -> DriverResult<Vec<String>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::fetch_databases(self, connected).await
    }

    async fn fetch_database_metadata(&self, database: &str) -> DriverResult<DatabaseMetadata> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::fetch_database_metadata(self, connected, database).await
    }

    fn cancel_query(&self) -> DriverResult<()> {
        let context = self
            .cancel_context
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(context) = context else {
            return Ok(());
        };

        let query_id = self
            .active_query_id
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(query_id) = query_id else {
            return Ok(());
        };

        tokio::spawn(async move {
            let sql = format!(
                "KILL QUERY WHERE query_id = {} SYNC",
                quote_literal(&query_id)
            );
            let result = send(
                &context.client,
                &context.base_url,
                &context.username,
                context.password.as_ref(),
                sql,
                &[],
            )
            .await;
            if let Err(err) = result {
                tracing::warn!(error = %err, "failed to send ClickHouse KILL QUERY cancel request");
            }
        });
        Ok(())
    }

    async fn apply_query_timeout(&self, seconds: u64) -> DriverResult<()> {
        *self
            .query_timeout_seconds
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(seconds);
        Ok(())
    }

    fn keepalive_posture(&self) -> KeepalivePosture {
        // Applied in `tls::build_http_client` on every connect.
        KeepalivePosture::Applied
    }

    fn server_version(&self) -> Option<String> {
        self.server_version
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn parameter_style(&self) -> ParameterStyle {
        ParameterStyle::QuestionMark
    }

    fn supports_transactions(&self) -> bool {
        false
    }

    async fn begin_transaction(&self) -> DriverResult<()> {
        Err(unsupported_transactions_error())
    }

    async fn commit_transaction(&self) -> DriverResult<()> {
        Err(unsupported_transactions_error())
    }

    async fn rollback_transaction(&self) -> DriverResult<()> {
        Err(unsupported_transactions_error())
    }

    async fn create_database(&self, request: &CreateDatabaseRequest) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::create_database(self, connected, request).await
    }

    async fn drop_database(&self, name: &str) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::drop_database(self, connected, name).await
    }

    async fn switch_database(&self, database: &str) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::validate_database_exists(self, connected, database).await?;
        self.set_current_database(database.to_string());
        Ok(())
    }

    fn stream_rows<'a>(&'a self, query: &'a str) -> RowStream<'a> {
        stream::stream_rows(self, query)
    }
}

fn unsupported_transactions_error() -> DriverError {
    DriverError::new(
        DriverErrorKind::Query,
        "ClickHouse does not reliably support multi-statement ACID transactions; this driver \
         does not send BEGIN/COMMIT/ROLLBACK",
    )
}

pub struct ClickHouseDriverFactory;

impl DriverFactory for ClickHouseDriverFactory {
    fn create_driver(&self, config: ConnectionConfig) -> Box<dyn DatabaseDriver> {
        Box::new(ClickHouseDriver::new(config))
    }
}

pub const DATABASE_TYPE_ID: &str = "ClickHouse";
