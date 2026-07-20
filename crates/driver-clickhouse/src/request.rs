//! Low-level HTTP plumbing shared by every query path: building the
//! request (Basic Auth, `database=`/`query_id=` query parameters),
//! checking the response status, and reading ClickHouse's own
//! `X-ClickHouse-Summary` response header for statement outcomes
//! (`TabSeparated` gives no row count for a bare `INSERT`/DDL statement,
//! since it returns no result set at all).

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use db_headless_core::DriverResult;

use crate::error::{map_http_error, map_reqwest_error};

/// The reqwest client plus resolved base URL for an established
/// connection. Held behind `ClickHouseDriver::client`'s `RwLock`.
pub(crate) struct ConnectedClient {
    pub(crate) client: reqwest::Client,
    pub(crate) base_url: String,
}

/// The small, cheap-to-clone slice of connection state `cancel_query`
/// needs. `cancel_query` is synchronous per the `DatabaseDriver` trait, so
/// it cannot await `ClickHouseDriver::client`'s async `RwLock` — this is
/// cached separately behind a plain `std::sync::Mutex` and refreshed on
/// every successful `connect`, mirroring the cached `CancelToken` pattern
/// `crates/driver-postgres/src/driver.rs` uses for the same constraint.
#[derive(Clone)]
pub(crate) struct CancelContext {
    pub(crate) client: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) username: String,
    pub(crate) password: Option<SecretString>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct ClickHouseSummary {
    #[serde(default)]
    written_rows: Option<String>,
}

impl ClickHouseSummary {
    pub(crate) fn written_rows(&self) -> Option<u64> {
        self.written_rows.as_deref().and_then(|s| s.parse().ok())
    }
}

/// Reads and parses the `X-ClickHouse-Summary` response header, if
/// present. Never fails: a missing or malformed header just means "no
/// summary available", not an error worth surfacing to the caller.
pub(crate) fn extract_summary(response: &reqwest::Response) -> Option<ClickHouseSummary> {
    let header = response.headers().get("x-clickhouse-summary")?;
    let text = header.to_str().ok()?;
    serde_json::from_str(text).ok()
}

pub(crate) fn basic_auth_value(
    username: &str,
    password: Option<&SecretString>,
) -> (String, Option<String>) {
    (
        username.to_string(),
        password.map(|p| p.expose_secret().to_string()),
    )
}

/// Sends `body` as a query against `base_url`, applying Basic Auth and
/// the given query-string parameters, and maps a non-2xx response into a
/// `DriverError` before returning. On success, the caller still owns the
/// `reqwest::Response` and is responsible for reading its body (as text
/// for the buffered TSV path, or as a byte stream for the streaming
/// JSONEachRow path) — this function never buffers the body itself.
pub(crate) async fn send(
    client: &reqwest::Client,
    base_url: &str,
    username: &str,
    password: Option<&SecretString>,
    body: String,
    params: &[(String, String)],
) -> DriverResult<reqwest::Response> {
    let (user, pass) = basic_auth_value(username, password);
    let response = client
        .post(format!("{base_url}/"))
        .basic_auth(user, pass)
        .query(params)
        .body(body)
        .send()
        .await
        .map_err(map_reqwest_error)?;

    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    Err(map_http_error(status, &text))
}
