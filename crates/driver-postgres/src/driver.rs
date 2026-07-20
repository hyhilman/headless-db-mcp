//! `PostgresDriver`: the `db_headless_core::DatabaseDriver` implementation
//! backed by `tokio_postgres`.
//!
//! Connection state lives behind `tokio::sync::RwLock<Option<ConnectedClient>>`
//! rather than the `&mut self` a naive implementation might reach for,
//! because the trait requires every method to take `&self` (the registry
//! shares one driver instance across concurrent MCP tool calls). Plain
//! queries (`execute`, `execute_parameterized`, every `fetch_*`) only need
//! `&Client`, so they take a **read** lock and run concurrently with each
//! other. `execute_user_query` and `stream_rows` open a real
//! transaction/portal, which needs `&mut Client` (`tokio_postgres::Client::
//! transaction` requires it) and would otherwise let unrelated concurrent
//! queries run *inside* that open transaction on the same session — so
//! those two take a **write** lock instead, serializing against every
//! other query on this connection for the lifetime of the transaction.
//! This is a deliberate, documented deviation from "every method takes a
//! read lock": it is required by `tokio_postgres`'s API and matches real
//! Postgres session semantics (one session has at most one open
//! transaction at a time).
//!
//! `cancel_query` is synchronous per the trait, so it cannot `.await` the
//! read lock above. A `CancelToken` is cheap (two `i32`s) and immutable
//! for the lifetime of a session, so it is cached separately behind a
//! plain `std::sync::Mutex`, refreshed on every successful `connect`/
//! `switch_database`, and read synchronously from `cancel_query` without
//! touching the async lock at all.

use std::sync::Mutex;

use async_trait::async_trait;
use db_headless_core::{
    CellValue, ColumnInfo, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver,
    DatabaseMetadata, DriverError, DriverErrorKind, DriverFactory, DriverResult, ForeignKeyInfo,
    IndexInfo, ParameterStyle, QueryResult, RowStream, SslMode, TableInfo, TableMetadata,
    TriggerInfo,
};
use tokio::sync::RwLock;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{CancelToken, Client, NoTls, Socket};

use crate::{config, error, query, schema, stream, tls};

pub(crate) struct ConnectedClient {
    pub(crate) client: Client,
    connection_task: tokio::task::JoinHandle<()>,
}

impl Drop for ConnectedClient {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

pub struct PostgresDriver {
    pub(crate) config: ConnectionConfig,
    pub(crate) client: RwLock<Option<ConnectedClient>>,
    cancel_token: Mutex<Option<CancelToken>>,
    server_version: Mutex<Option<String>>,
    current_schema: Mutex<Option<String>>,
}

pub(crate) fn not_connected_error() -> DriverError {
    DriverError::new(DriverErrorKind::Connection, "not connected")
}

impl PostgresDriver {
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            config,
            client: RwLock::new(None),
            cancel_token: Mutex::new(None),
            server_version: Mutex::new(None),
            current_schema: Mutex::new(None),
        }
    }

    async fn connect_with_config(
        &self,
        config: &ConnectionConfig,
    ) -> DriverResult<(Client, tokio::task::JoinHandle<()>, CancelToken)> {
        let pg_config = config::build_config(config);

        // `Disabled` is the only mode that connects with `NoTls`. Every
        // other mode -- including a missing `ssl.mode`, which guardrail
        // #6 requires treating as `VerifyIdentity` -- gets a real
        // `rustls`-backed connector from `crate::tls`, differing only in
        // how much of the certificate it actually verifies.
        if matches!(config.ssl.mode, Some(SslMode::Disabled)) {
            Self::connect_via(&pg_config, NoTls).await
        } else {
            let connector = tls::build_connector(config)?;
            Self::connect_via(&pg_config, connector).await
        }
    }

    async fn connect_via<T>(
        pg_config: &tokio_postgres::Config,
        tls: T,
    ) -> DriverResult<(Client, tokio::task::JoinHandle<()>, CancelToken)>
    where
        T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
        T::Stream: Send,
        T::TlsConnect: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let (client, connection) = pg_config
            .connect(tls)
            .await
            .map_err(error::map_connect_error)?;

        let cancel_token = client.cancel_token();

        let task = tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::error!(error = %err, "postgres connection task ended with an error");
            }
        });

        Ok((client, task, cancel_token))
    }

    async fn refresh_session_caches(&self, client: &Client) {
        let version = client
            .simple_query("SHOW server_version")
            .await
            .ok()
            .and_then(|messages| first_simple_query_value(&messages));
        *self
            .server_version
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = version;

        let schema = client
            .query_one("SELECT current_schema()", &[])
            .await
            .ok()
            .and_then(|row| row.try_get::<_, Option<String>>(0).ok().flatten());
        *self
            .current_schema
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = schema;
    }

    fn set_cancel_token(&self, token: CancelToken) {
        *self.cancel_token.lock().unwrap_or_else(|p| p.into_inner()) = Some(token);
    }
}

fn first_simple_query_value(messages: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
        _ => None,
    })
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    async fn connect(&self) -> DriverResult<()> {
        let (client, task, cancel_token) = self.connect_with_config(&self.config).await?;
        self.refresh_session_caches(&client).await;
        self.set_cancel_token(cancel_token);

        let mut guard = self.client.write().await;
        *guard = Some(ConnectedClient {
            client,
            connection_task: task,
        });
        Ok(())
    }

    async fn disconnect(&self) -> DriverResult<()> {
        let mut guard = self.client.write().await;
        *guard = None;
        *self.cancel_token.lock().unwrap_or_else(|p| p.into_inner()) = None;
        *self
            .server_version
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        *self
            .current_schema
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = None;
        Ok(())
    }

    async fn execute(&self, sql: &str) -> DriverResult<QueryResult> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        query::execute(&connected.client, sql).await
    }

    async fn execute_parameterized(
        &self,
        sql: &str,
        parameters: &[CellValue],
    ) -> DriverResult<QueryResult> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        query::execute_parameterized(&connected.client, sql, parameters).await
    }

    async fn execute_user_query(
        &self,
        sql: &str,
        row_cap: Option<usize>,
        parameters: Option<&[CellValue]>,
    ) -> DriverResult<QueryResult> {
        let mut guard = self.client.write().await;
        let connected = guard.as_mut().ok_or_else(not_connected_error)?;
        query::execute_user_query(
            &mut connected.client,
            sql,
            row_cap,
            parameters,
            self.config.read_only,
        )
        .await
    }

    async fn fetch_tables(&self, schema: Option<&str>) -> DriverResult<Vec<TableInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_tables(&connected.client, &schema).await
    }

    async fn fetch_columns(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<ColumnInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_columns(&connected.client, table, &schema).await
    }

    async fn fetch_indexes(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<IndexInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_indexes(&connected.client, table, &schema).await
    }

    async fn fetch_foreign_keys(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<ForeignKeyInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_foreign_keys(&connected.client, table, &schema).await
    }

    async fn fetch_triggers(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<Vec<TriggerInfo>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_triggers(&connected.client, table, &schema).await
    }

    async fn fetch_table_ddl(&self, table: &str, schema: Option<&str>) -> DriverResult<String> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_table_ddl(&connected.client, table, &schema).await
    }

    async fn fetch_view_definition(
        &self,
        view: &str,
        schema: Option<&str>,
    ) -> DriverResult<String> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_view_definition(&connected.client, view, &schema).await
    }

    async fn fetch_table_metadata(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> DriverResult<TableMetadata> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let schema = self.resolve_schema(schema);
        schema::fetch_table_metadata(&connected.client, table, &schema).await
    }

    async fn fetch_databases(&self) -> DriverResult<Vec<String>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::fetch_databases(&connected.client).await
    }

    async fn fetch_database_metadata(&self, database: &str) -> DriverResult<DatabaseMetadata> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::fetch_database_metadata(&connected.client, database).await
    }

    fn supports_schemas(&self) -> bool {
        true
    }

    async fn fetch_schemas(&self) -> DriverResult<Vec<String>> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::fetch_schemas(&connected.client).await
    }

    async fn switch_schema(&self, schema: &str) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::switch_schema(&connected.client, schema).await?;
        *self
            .current_schema
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(schema.to_string());
        Ok(())
    }

    fn current_schema(&self) -> Option<String> {
        self.current_schema
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn cancel_query(&self) -> DriverResult<()> {
        let token = self
            .cancel_token
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or_else(not_connected_error)?;

        tokio::spawn(async move {
            if let Err(err) = token.cancel_query(NoTls).await {
                tracing::error!(error = %err, "failed to send postgres cancel request");
            }
        });
        Ok(())
    }

    async fn apply_query_timeout(&self, seconds: u64) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        let millis = seconds.saturating_mul(1000);
        let sql = format!("SET statement_timeout = {millis}");
        connected
            .client
            .simple_query(&sql)
            .await
            .map_err(error::map_query_error)?;
        Ok(())
    }

    fn server_version(&self) -> Option<String> {
        self.server_version
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    fn parameter_style(&self) -> ParameterStyle {
        ParameterStyle::Dollar
    }

    async fn create_database(&self, request: &CreateDatabaseRequest) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::create_database(&connected.client, request).await
    }

    async fn drop_database(&self, name: &str) -> DriverResult<()> {
        let guard = self.client.read().await;
        let connected = guard.as_ref().ok_or_else(not_connected_error)?;
        schema::drop_database(&connected.client, name).await
    }

    async fn switch_database(&self, database: &str) -> DriverResult<()> {
        let mut new_config = self.config.clone();
        new_config.database = Some(database.to_string());

        let (client, task, cancel_token) = self.connect_with_config(&new_config).await?;
        self.refresh_session_caches(&client).await;

        let mut guard = self.client.write().await;
        *guard = Some(ConnectedClient {
            client,
            connection_task: task,
        });
        drop(guard);

        self.set_cancel_token(cancel_token);
        Ok(())
    }

    fn stream_rows<'a>(&'a self, query: &'a str) -> RowStream<'a> {
        stream::stream_rows(self, query)
    }
}

impl PostgresDriver {
    fn resolve_schema(&self, schema: Option<&str>) -> String {
        schema
            .map(str::to_string)
            .or_else(|| self.current_schema())
            .unwrap_or_else(|| "public".to_string())
    }
}

pub struct PostgresDriverFactory;

impl DriverFactory for PostgresDriverFactory {
    fn create_driver(&self, config: ConnectionConfig) -> Box<dyn DatabaseDriver> {
        Box::new(PostgresDriver::new(config))
    }
}

pub const DATABASE_TYPE_ID: &str = "PostgreSQL";
