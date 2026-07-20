//! Integration tests against a real, ephemeral ClickHouse container.
//!
//! Every test spins up its own container via `testcontainers`/
//! `testcontainers-modules`'s `clickhouse` module (built on the official
//! `clickhouse/clickhouse-server` image), so this file never assumes a
//! pre-existing running ClickHouse. Docker must be reachable
//! (`docker info`). `testcontainers-modules::clickhouse::ClickHouse`'s
//! own `ready_conditions` already wait on a real HTTP `200` from `/`
//! before `start()` returns, so no extra polling is needed here.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use db_headless_core::{
    CellValue, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver, DriverErrorKind, SslConfig,
    StreamElement,
};
use db_headless_driver_clickhouse::ClickHouseDriver;
use futures_util::StreamExt;
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

const CLICKHOUSE_PASSWORD: &str = "real-s3cret-value";

async fn start_container() -> ContainerAsync<ClickHouse> {
    ClickHouse::default()
        .with_env_var("CLICKHOUSE_PASSWORD", CLICKHOUSE_PASSWORD)
        .start()
        .await
        .expect("start clickhouse container")
}

async fn config_for(container: &ContainerAsync<ClickHouse>, password: &str) -> ConnectionConfig {
    let host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("container port");

    ConnectionConfig {
        host,
        port,
        username: "default".to_string(),
        password: Some(secrecy::SecretString::from(password.to_string())),
        database: Some("default".to_string()),
        ssl: SslConfig::disabled(),
        read_only: false,
        additional_fields: HashMap::new(),
    }
}

async fn connected_driver(container: &ContainerAsync<ClickHouse>) -> ClickHouseDriver {
    let config = config_for(container, CLICKHOUSE_PASSWORD).await;
    let driver = ClickHouseDriver::new(config);
    driver.connect().await.expect("connect");
    driver
}

#[tokio::test]
async fn connect_disconnect_and_ping_succeed() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver.ping().await.expect("ping");
    driver.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn connect_failure_with_wrong_password_does_not_leak_password() {
    let container = start_container().await;
    let wrong_password = "definitely-not-the-real-password";
    let config = config_for(&container, wrong_password).await;
    let driver = ClickHouseDriver::new(config);

    let err = driver
        .connect()
        .await
        .expect_err("wrong password must fail to connect");

    assert_eq!(err.kind, DriverErrorKind::Auth);
    assert!(!err.message.contains(wrong_password));
    assert!(!err.message.contains(CLICKHOUSE_PASSWORD));
    if let Some(detail) = &err.detail {
        assert!(!detail.contains(wrong_password));
        assert!(!detail.contains(CLICKHOUSE_PASSWORD));
    }
}

#[tokio::test]
async fn execute_parameterized_round_trips_null_tab_and_backslash_exactly() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE round_trip (id String, val Nullable(String)) ENGINE = Memory")
        .await
        .expect("create table");

    let tricky_value = "has\ta tab, a backslash \\ and a newline\nhere";
    driver
        .execute_parameterized(
            "INSERT INTO round_trip (id, val) VALUES (?, ?)",
            &[
                CellValue::Text("1".to_string()),
                CellValue::Text(tricky_value.to_string()),
            ],
        )
        .await
        .expect("insert tricky value");

    // Binding a `CellValue::Null` through `execute_parameterized` is a
    // documented, deliberate gap (see `crate::params`'s module doc
    // comment): making a placeholder correctly `Nullable` needs type
    // information this driver does not have. Insert the NULL row through
    // plain `execute` instead, and separately assert the parameterized
    // path really does reject a `Null` binding with a clear error.
    let null_binding_err = driver
        .execute_parameterized(
            "INSERT INTO round_trip (id, val) VALUES (?, ?)",
            &[CellValue::Text("2".to_string()), CellValue::Null],
        )
        .await
        .expect_err("binding a NULL parameter must be a clear, documented error");
    assert_eq!(null_binding_err.kind, DriverErrorKind::Query);
    assert!(null_binding_err.message.to_lowercase().contains("null"));

    driver
        .execute("INSERT INTO round_trip (id, val) VALUES ('2', NULL)")
        .await
        .expect("insert null value via plain execute");

    let result = driver
        .execute("SELECT id, val FROM round_trip ORDER BY id")
        .await
        .expect("select back");

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], CellValue::Text("1".to_string()));
    assert_eq!(result.rows[0][1], CellValue::Text(tricky_value.to_string()));
    assert_eq!(result.rows[1][0], CellValue::Text("2".to_string()));
    assert_eq!(result.rows[1][1], CellValue::Null);

    let tables = driver
        .execute("SELECT name FROM system.tables WHERE name = 'round_trip'")
        .await
        .expect("check table still exists");
    assert_eq!(
        tables.rows.len(),
        1,
        "round_trip table must not have been dropped"
    );
}

#[tokio::test]
async fn sql_injection_shaped_parameter_is_stored_as_literal_text() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE foo (id String, note String) ENGINE = Memory")
        .await
        .expect("create foo table");

    let payload = "'; DROP TABLE foo; --";
    driver
        .execute_parameterized(
            "INSERT INTO foo (id, note) VALUES (?, ?)",
            &[
                CellValue::Text("1".to_string()),
                CellValue::Text(payload.to_string()),
            ],
        )
        .await
        .expect("insert injection-shaped payload");

    let still_there = driver
        .execute("SELECT name FROM system.tables WHERE name = 'foo'")
        .await
        .expect("check foo still exists");
    assert_eq!(
        still_there.rows.len(),
        1,
        "foo table must survive a bound injection-shaped value"
    );

    let result = driver
        .execute_parameterized(
            "SELECT note FROM foo WHERE id = ?",
            &[CellValue::Text("1".to_string())],
        )
        .await
        .expect("select back payload");
    assert_eq!(result.rows[0][0], CellValue::Text(payload.to_string()));
}

#[tokio::test]
async fn bytes_parameter_round_trips_exactly_through_unhex() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE blobs (id String, data String) ENGINE = Memory")
        .await
        .expect("create blobs table");

    let bytes = vec![0x00u8, 0xDE, 0xAD, 0xBE, 0xEF, 0x0A, 0xFF];
    driver
        .execute_parameterized(
            "INSERT INTO blobs (id, data) VALUES (?, ?)",
            &[
                CellValue::Text("1".to_string()),
                CellValue::Bytes(bytes.clone()),
            ],
        )
        .await
        .expect("insert bytes");

    let result = driver
        .execute("SELECT hex(data), length(data) FROM blobs WHERE id = '1'")
        .await
        .expect("select back hex");
    let expected_hex = bytes.iter().map(|b| format!("{b:02X}")).collect::<String>();
    assert_eq!(result.rows[0][0], CellValue::Text(expected_hex));
    assert_eq!(result.rows[0][1], CellValue::Text(bytes.len().to_string()));
}

/// Regression test for the bug a live end-to-end smoke test against the
/// running MCP server caught: `execute_user_query` is the one method the
/// generic `execute_query` MCP tool calls for *any* SQL a client sends,
/// including DDL. Before the fix, `execute_user_query`'s capped path
/// unconditionally wrapped the statement as `SELECT * FROM (<sql>)
/// LIMIT 0 FORMAT ...` to probe a typed header, which is a real
/// ClickHouse syntax error (`Code: 62`) for a `CREATE TABLE`. This must
/// now succeed by routing through the buffered path instead.
#[tokio::test]
async fn execute_user_query_runs_create_table_ddl_successfully() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute_user_query(
            "CREATE TABLE widgets (id UInt32, name String) ENGINE = MergeTree ORDER BY id",
            None,
            None,
        )
        .await
        .expect("CREATE TABLE through execute_user_query must succeed, not a syntax error");

    let tables = driver
        .execute("SELECT name FROM system.tables WHERE name = 'widgets'")
        .await
        .expect("check widgets exists");
    assert_eq!(tables.rows.len(), 1);
}

/// Same class of bug as `execute_user_query_insert_actually_persists` in
/// `driver-postgres`'s integration suite: a method reporting `Ok` is not
/// proof the write actually happened. Runs both a plain and a
/// parameterized `INSERT` through `execute_user_query` (the exact method
/// the DDL bug above lived in), then reconnects entirely — a fresh
/// `ClickHouseDriver` and a fresh HTTP client, not just a fresh query on
/// the same driver — before reading the rows back, to rule out any
/// session-local artifact.
#[tokio::test]
async fn execute_user_query_insert_actually_persists_verified_via_fresh_connection() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute_user_query(
            "CREATE TABLE persisted (id UInt32, note String) ENGINE = MergeTree ORDER BY id",
            None,
            None,
        )
        .await
        .expect("create table via execute_user_query");

    driver
        .execute_user_query(
            "INSERT INTO persisted (id, note) VALUES (1, 'plain insert')",
            None,
            None,
        )
        .await
        .expect("plain insert via execute_user_query");

    driver
        .execute_user_query(
            "INSERT INTO persisted (id, note) VALUES (?, ?)",
            None,
            Some(&[
                CellValue::Text("2".to_string()),
                CellValue::Text("parameterized insert".to_string()),
            ]),
        )
        .await
        .expect("parameterized insert via execute_user_query");

    let fresh_driver = connected_driver(&container).await;
    let readback = fresh_driver
        .execute("SELECT id, note FROM persisted ORDER BY id")
        .await
        .expect("read back inserted rows from a fresh connection");

    assert_eq!(readback.rows.len(), 2);
    assert_eq!(readback.rows[0][0], CellValue::Text("1".to_string()));
    assert_eq!(
        readback.rows[0][1],
        CellValue::Text("plain insert".to_string())
    );
    assert_eq!(readback.rows[1][0], CellValue::Text("2".to_string()));
    assert_eq!(
        readback.rows[1][1],
        CellValue::Text("parameterized insert".to_string())
    );
}

/// A `read_only` connection sends ClickHouse's own `readonly=1` HTTP
/// setting on every request, so the server itself rejects a write —
/// real engine-level enforcement, not a client-side statement check
/// this driver's SQL dialect awareness could get wrong.
#[tokio::test]
async fn read_only_connection_rejects_writes_but_allows_selects() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute_user_query(
            "CREATE TABLE read_only_guard (id UInt32) ENGINE = MergeTree ORDER BY id",
            None,
            None,
        )
        .await
        .expect("create table");

    let mut read_only_config = config_for(&container, CLICKHOUSE_PASSWORD).await;
    read_only_config.read_only = true;
    let read_only_driver = ClickHouseDriver::new(read_only_config);
    read_only_driver.connect().await.expect("connect");

    let select = read_only_driver
        .execute_user_query("SELECT id FROM read_only_guard", None, None)
        .await
        .expect("select still works on a read-only connection");
    assert_eq!(select.rows.len(), 0);

    read_only_driver
        .execute_user_query("INSERT INTO read_only_guard (id) VALUES (1)", None, None)
        .await
        .expect_err("insert must be rejected on a read-only connection");

    let fresh_driver = connected_driver(&container).await;
    let readback = fresh_driver
        .execute("SELECT id FROM read_only_guard")
        .await
        .expect("read back");
    assert_eq!(
        readback.rows.len(),
        0,
        "the rejected insert must not have persisted anything"
    );
}

/// Confirms the DDL/DML fix did not accidentally route real `SELECT`s
/// through the buffered fallback path too: a `SELECT` with a `row_cap`
/// smaller than the real result set must still report `is_truncated:
/// true` with exactly `row_cap` rows, the same capped-streaming
/// behavior as before the fix.
#[tokio::test]
async fn execute_user_query_select_with_small_cap_still_uses_streaming_path_and_truncates() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;
    seed_numbered_table(&driver, "cap_regression", 500).await;

    let result = driver
        .execute_user_query("SELECT n FROM cap_regression ORDER BY n", Some(7), None)
        .await
        .expect("capped select");
    assert_eq!(result.rows.len(), 7);
    assert!(result.is_truncated);
    assert_eq!(result.columns, vec!["n".to_string()]);
    assert_eq!(result.column_type_names, vec!["UInt32".to_string()]);
}

/// A statement preceded by a SQL comment must still be classified
/// correctly: a commented `INSERT` must not be routed through the
/// header-probing streaming path (which would break exactly like the
/// uncommented `CREATE TABLE` bug did), and a commented `SELECT` must
/// still stream and cap normally.
#[tokio::test]
async fn execute_user_query_classifies_statements_behind_a_leading_comment() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute_user_query(
            "-- seed the table\nCREATE TABLE commented (id UInt32) ENGINE = MergeTree ORDER BY id",
            None,
            None,
        )
        .await
        .expect("commented CREATE TABLE must succeed");

    driver
        .execute_user_query(
            "-- insert a row\nINSERT INTO commented (id) VALUES (1)",
            None,
            None,
        )
        .await
        .expect("commented INSERT must succeed");

    let capped = driver
        .execute_user_query("-- read it back\nSELECT id FROM commented", Some(1), None)
        .await
        .expect("commented SELECT must still stream and cap");
    assert_eq!(capped.rows.len(), 1);
    assert_eq!(capped.rows[0][0], CellValue::Text("1".to_string()));
}

async fn seed_numbered_table(driver: &ClickHouseDriver, table: &str, row_count: usize) {
    driver
        .execute(&format!("CREATE TABLE {table} (n UInt32) ENGINE = Memory"))
        .await
        .expect("create numbered table");
    driver
        .execute(&format!(
            "INSERT INTO {table} SELECT number FROM numbers({row_count})"
        ))
        .await
        .expect("seed rows");
}

#[tokio::test]
async fn execute_user_query_caps_rows_and_reports_truncation() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;
    seed_numbered_table(&driver, "capped", 50).await;

    let capped = driver
        .execute_user_query("SELECT n FROM capped ORDER BY n", Some(10), None)
        .await
        .expect("capped query");
    assert_eq!(capped.rows.len(), 10);
    assert!(capped.is_truncated);

    let uncapped = driver
        .execute_user_query("SELECT n FROM capped ORDER BY n", Some(1000), None)
        .await
        .expect("uncapped query");
    assert_eq!(uncapped.rows.len(), 50);
    assert!(!uncapped.is_truncated);
}

/// A capped `execute_user_query` must stop consuming the HTTP response as
/// soon as it has `cap` rows, never reading (let alone buffering) the
/// rest of a large result. Directly observing "bytes not read" from
/// outside the driver isn't practical, so this is a behavioral proof
/// instead: a small cap against a result set with many multi-batch's
/// worth of rows (`JSON_BATCH_SIZE` is 5,000; this seeds 20x that) must
/// return almost immediately, because a version of this method that
/// buffered the whole response first would instead pay for serializing
/// and transferring hundreds of thousands of JSON lines before ever
/// looking at `cap`. The threshold is generous (a real full read of this
/// result set is measured separately, below, and takes far longer) to
/// keep this test from being flaky on a loaded CI box while still failing
/// decisively if buffering regresses.
#[tokio::test]
async fn execute_user_query_capped_path_does_not_buffer_the_full_result_set() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;
    let total_rows = 100_000;
    seed_numbered_table(&driver, "huge", total_rows).await;

    let full_started = Instant::now();
    let full = driver
        .execute_user_query("SELECT n FROM huge ORDER BY n", Some(total_rows), None)
        .await
        .expect("full query");
    let full_elapsed = full_started.elapsed();
    assert_eq!(full.rows.len(), total_rows);

    let capped_started = Instant::now();
    let capped = driver
        .execute_user_query("SELECT n FROM huge ORDER BY n", Some(5), None)
        .await
        .expect("capped query");
    let capped_elapsed = capped_started.elapsed();

    assert_eq!(capped.rows.len(), 5);
    assert!(capped.is_truncated);
    assert!(
        capped_elapsed < full_elapsed,
        "capped read ({capped_elapsed:?}) should be faster than reading all {total_rows} rows \
         ({full_elapsed:?}); a buffer-then-truncate implementation would take about as long as \
         the full read regardless of the cap"
    );
}

#[tokio::test]
async fn stream_rows_yields_multiple_batches_matching_total_count() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;
    seed_numbered_table(&driver, "streamed", 12_000).await;

    let mut stream = driver.stream_rows("SELECT n FROM streamed ORDER BY n");
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;
    let mut saw_header = false;

    while let Some(element) = stream.next().await {
        match element.expect("stream element") {
            StreamElement::Header(header) => {
                assert!(!saw_header, "header must be sent exactly once");
                assert_eq!(header.columns, vec!["n".to_string()]);
                assert_eq!(header.column_type_names, vec!["UInt32".to_string()]);
                saw_header = true;
            }
            StreamElement::Rows(rows) => {
                batch_count += 1;
                total_rows += rows.len();
            }
        }
    }

    assert!(saw_header);
    assert!(
        batch_count > 1,
        "expected more than one batch, got {batch_count}"
    );
    assert_eq!(total_rows, 12_000);
}

#[tokio::test]
async fn schema_introspection_reports_real_columns_primary_key_and_skip_index() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute(
            "CREATE TABLE events (
                id UInt64,
                created_at DateTime,
                payload String,
                INDEX payload_idx payload TYPE bloom_filter GRANULARITY 4
            ) ENGINE = MergeTree
            ORDER BY (id, created_at)",
        )
        .await
        .expect("create events table");

    let columns = driver
        .fetch_columns("events", Some("default"))
        .await
        .expect("fetch columns");
    let id_col = columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column present");
    assert_eq!(id_col.data_type, "UInt64");
    assert!(id_col.is_primary_key);
    let payload_col = columns
        .iter()
        .find(|c| c.name == "payload")
        .expect("payload column present");
    assert!(!payload_col.is_nullable);

    let indexes = driver
        .fetch_indexes("events", Some("default"))
        .await
        .expect("fetch indexes");
    let pk_index = indexes
        .iter()
        .find(|i| i.is_primary)
        .expect("synthetic primary key index present");
    assert_eq!(
        pk_index.columns,
        vec!["id".to_string(), "created_at".to_string()]
    );
    let skip_index = indexes
        .iter()
        .find(|i| i.name == "payload_idx")
        .expect("data skipping index present");
    assert!(!skip_index.is_primary);
    assert_eq!(skip_index.method.as_deref(), Some("bloom_filter"));

    let foreign_keys = driver
        .fetch_foreign_keys("events", Some("default"))
        .await
        .expect("fetch fks");
    assert!(
        foreign_keys.is_empty(),
        "ClickHouse has no foreign key constraints"
    );

    let tables = driver
        .fetch_tables(Some("default"))
        .await
        .expect("fetch tables");
    assert!(tables.iter().any(|t| t.name == "events"));
}

#[tokio::test]
async fn fetch_table_ddl_and_view_definition_return_exact_create_statements() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE viewed_t (id UInt32, active UInt8) ENGINE = MergeTree ORDER BY id")
        .await
        .expect("create table");
    driver
        .execute("CREATE VIEW active_viewed_t AS SELECT id FROM viewed_t WHERE active = 1")
        .await
        .expect("create view");

    let table_ddl = driver
        .fetch_table_ddl("viewed_t", Some("default"))
        .await
        .expect("fetch table ddl");
    assert!(table_ddl.to_uppercase().contains("CREATE TABLE"));
    assert!(table_ddl.contains("viewed_t"));

    let view_definition = driver
        .fetch_view_definition("active_viewed_t", Some("default"))
        .await
        .expect("fetch view definition");
    assert!(view_definition.to_uppercase().contains("CREATE VIEW"));
    assert!(view_definition.to_lowercase().contains("viewed_t"));
    assert!(view_definition.contains("active"));
}

#[tokio::test]
async fn create_and_drop_database_round_trip() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let request = CreateDatabaseRequest {
        name: "roundtrip_db".to_string(),
        owner: None,
        encoding: None,
        additional_fields: HashMap::new(),
    };
    driver
        .create_database(&request)
        .await
        .expect("create database");

    let databases = driver.fetch_databases().await.expect("fetch databases");
    assert!(databases.contains(&"roundtrip_db".to_string()));

    driver
        .drop_database("roundtrip_db")
        .await
        .expect("drop database");

    let databases_after = driver
        .fetch_databases()
        .await
        .expect("fetch databases after drop");
    assert!(!databases_after.contains(&"roundtrip_db".to_string()));
}

#[tokio::test]
async fn create_database_with_backtick_in_name_round_trips_and_drop_removes_it() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let request = CreateDatabaseRequest {
        name: "weird`db".to_string(),
        owner: None,
        encoding: None,
        additional_fields: HashMap::new(),
    };
    driver
        .create_database(&request)
        .await
        .expect("create database with backtick in name");

    let databases = driver.fetch_databases().await.expect("fetch databases");
    assert!(databases.contains(&"weird`db".to_string()));

    driver
        .drop_database("weird`db")
        .await
        .expect("drop database with backtick in name");

    let databases_after = driver
        .fetch_databases()
        .await
        .expect("fetch databases after drop");
    assert!(!databases_after.contains(&"weird`db".to_string()));
}

#[tokio::test]
async fn switch_database_to_unknown_name_fails_clearly() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let err = driver
        .switch_database("does_not_exist_at_all")
        .await
        .expect_err("switching to an unknown database must fail");
    assert_eq!(err.kind, DriverErrorKind::Query);

    driver
        .execute("CREATE DATABASE switch_target")
        .await
        .expect("create switch target");
    driver
        .switch_database("switch_target")
        .await
        .expect("switch to a real database");

    driver
        .execute("CREATE TABLE in_target (id UInt32) ENGINE = Memory")
        .await
        .expect("create table in switched database");
    let tables = driver
        .execute("SELECT name FROM system.tables WHERE database = 'switch_target'")
        .await
        .expect("list tables in switch_target");
    assert!(tables
        .rows
        .iter()
        .any(|row| row[0] == CellValue::Text("in_target".to_string())));
}

#[tokio::test]
async fn cancel_query_interrupts_a_long_running_query() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let start = Instant::now();

    let query_fut = driver.execute("SELECT sleep(3) FROM numbers(100)");
    let cancel_fut = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        driver.cancel_query().expect("cancel query");
    };

    let (result, ()) = tokio::join!(query_fut, cancel_fut);
    let elapsed = start.elapsed();

    assert!(result.is_err(), "cancelled query must return an error");
    assert!(
        elapsed < Duration::from_secs(20),
        "cancellation should interrupt the query well before its natural ~300s completion, took {elapsed:?}"
    );
}

#[tokio::test]
async fn supports_transactions_is_false_and_transaction_methods_return_clear_errors() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    assert!(!driver.supports_transactions());
    assert_eq!(
        driver.begin_transaction().await.unwrap_err().kind,
        DriverErrorKind::Query
    );
    assert_eq!(
        driver.commit_transaction().await.unwrap_err().kind,
        DriverErrorKind::Query
    );
    assert_eq!(
        driver.rollback_transaction().await.unwrap_err().kind,
        DriverErrorKind::Query
    );
}
