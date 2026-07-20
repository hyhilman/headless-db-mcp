//! Integration tests against a real, ephemeral PostgreSQL container.
//!
//! Every test spins up its own container via `testcontainers`/
//! `testcontainers-modules`, so this file never assumes a pre-existing
//! running Postgres. Docker must be reachable (`docker info`).

use std::collections::HashMap;
use std::time::Duration;

use db_headless_core::{
    CellValue, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver, DriverErrorKind, SslConfig,
    SslMode, StreamElement,
};
use db_headless_driver_postgres::PostgresDriver;
use futures_util::StreamExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

const POSTGRES_PASSWORD: &str = "real-s3cret-value";

async fn start_container() -> ContainerAsync<Postgres> {
    Postgres::default()
        .with_password(POSTGRES_PASSWORD)
        .start()
        .await
        .expect("start postgres container")
}

async fn config_for(container: &ContainerAsync<Postgres>, password: &str) -> ConnectionConfig {
    let host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");

    ConnectionConfig {
        host,
        port,
        username: "postgres".to_string(),
        password: Some(secrecy::SecretString::from(password.to_string())),
        database: Some("postgres".to_string()),
        ssl: SslConfig::disabled(),
        read_only: false,
        additional_fields: HashMap::new(),
    }
}

async fn connected_driver(container: &ContainerAsync<Postgres>) -> PostgresDriver {
    let config = config_for(container, POSTGRES_PASSWORD).await;
    let driver = PostgresDriver::new(config);
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
    let driver = PostgresDriver::new(config);

    let err = driver
        .connect()
        .await
        .expect_err("wrong password must fail to connect");

    assert_eq!(err.kind, DriverErrorKind::Auth);
    assert!(!err.message.contains(wrong_password));
    assert!(!err.message.contains(POSTGRES_PASSWORD));
    if let Some(detail) = &err.detail {
        assert!(!detail.contains(wrong_password));
        assert!(!detail.contains(POSTGRES_PASSWORD));
    }
    if let Some(sql_state) = &err.sql_state {
        assert!(!sql_state.contains(wrong_password));
    }
}

#[tokio::test]
async fn execute_parameterized_round_trips_null_quote_and_semicolon_exactly() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE round_trip (id INTEGER PRIMARY KEY, val TEXT)")
        .await
        .expect("create table");

    let tricky_value = "hello ' ; DROP TABLE round_trip; --";
    driver
        .execute_parameterized(
            "INSERT INTO round_trip (id, val) VALUES ($1, $2)",
            &[
                CellValue::Text("1".to_string()),
                CellValue::Text(tricky_value.to_string()),
            ],
        )
        .await
        .expect("insert tricky value");

    driver
        .execute_parameterized(
            "INSERT INTO round_trip (id, val) VALUES ($1, $2)",
            &[CellValue::Text("2".to_string()), CellValue::Null],
        )
        .await
        .expect("insert null value");

    let result = driver
        .execute_parameterized("SELECT id, val FROM round_trip ORDER BY id", &[])
        .await
        .expect("select back");

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], CellValue::Text("1".to_string()));
    assert_eq!(result.rows[0][1], CellValue::Text(tricky_value.to_string()));
    assert_eq!(result.rows[1][0], CellValue::Text("2".to_string()));
    assert_eq!(result.rows[1][1], CellValue::Null);

    let tables = driver
        .execute("SELECT relname FROM pg_class WHERE relname = 'round_trip'")
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
        .execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, note TEXT)")
        .await
        .expect("create foo table");

    let payload = "'; DROP TABLE foo; --";
    driver
        .execute_parameterized(
            "INSERT INTO foo (id, note) VALUES ($1, $2)",
            &[
                CellValue::Text("1".to_string()),
                CellValue::Text(payload.to_string()),
            ],
        )
        .await
        .expect("insert injection-shaped payload");

    let still_there = driver
        .execute("SELECT relname FROM pg_class WHERE relname = 'foo'")
        .await
        .expect("check foo still exists");
    assert_eq!(
        still_there.rows.len(),
        1,
        "foo table must survive a bound injection-shaped value"
    );

    let result = driver
        .execute_parameterized(
            "SELECT note FROM foo WHERE id = $1",
            &[CellValue::Text("1".to_string())],
        )
        .await
        .expect("select back payload");
    assert_eq!(result.rows[0][0], CellValue::Text(payload.to_string()));
}

async fn seed_numbered_table(driver: &PostgresDriver, table: &str, row_count: usize) {
    driver
        .execute(&format!("CREATE TABLE {table} (n INTEGER PRIMARY KEY)"))
        .await
        .expect("create numbered table");
    driver
        .execute(&format!(
            "INSERT INTO {table} (n) SELECT generate_series(1, {row_count})"
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

/// Regression test: `execute_user_query` runs its fetch loop inside a
/// transaction (needed for the portal), and the transaction must be
/// committed, not rolled back, once the fetch completes successfully.
/// This was caught by a live manual smoke test, not by the original test
/// suite: every prior `execute_user_query` test only exercised `SELECT`
/// statements, which never surfaced that the transaction was being rolled
/// back regardless of what it ran — an `INSERT` through this same method
/// silently reported success while persisting nothing.
#[tokio::test]
async fn execute_user_query_insert_actually_persists() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE persisted (id INT PRIMARY KEY, note TEXT)")
        .await
        .expect("create table");

    let result = driver
        .execute_user_query(
            "INSERT INTO persisted (id, note) VALUES ($1, $2)",
            None,
            Some(&[
                CellValue::Text("1".to_string()),
                CellValue::Text("hi".to_string()),
            ]),
        )
        .await
        .expect("insert via execute_user_query");
    // The extended-protocol portal path does not expose the DML command
    // tag's affected-row count the way the simple query protocol does
    // (see `query::execute_user_query`'s doc comment for why); an INSERT
    // with no `RETURNING` clause fetches zero rows from the portal. This
    // assertion documents that known gap rather than silently ignoring it.
    assert_eq!(result.rows.len(), 0);

    // The bug this test guards against: a prior version of this method
    // rolled back its transaction unconditionally, so the row below would
    // be absent after a fresh connection re-reads the table.
    let verify = connected_driver(&container).await;
    let readback = verify
        .execute_user_query("SELECT id, note FROM persisted", None, None)
        .await
        .expect("read back the inserted row");
    assert_eq!(readback.rows.len(), 1);
    assert_eq!(
        readback.rows[0],
        vec![
            CellValue::Text("1".to_string()),
            CellValue::Text("hi".to_string())
        ]
    );
}

/// A `read_only` connection must let Postgres itself reject a write, not
/// merely rely on this server declining to call a write method: the
/// server opens `execute_user_query`'s transaction with `BEGIN READ
/// ONLY`, so the write fails with Postgres's own error rather than a
/// client-side string check that a CTE or function call could slip past.
#[tokio::test]
async fn read_only_connection_rejects_writes_but_allows_selects() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE read_only_guard (id INT PRIMARY KEY)")
        .await
        .expect("create table");

    let mut read_only_config = config_for(&container, POSTGRES_PASSWORD).await;
    read_only_config.read_only = true;
    let read_only_driver = PostgresDriver::new(read_only_config);
    read_only_driver.connect().await.expect("connect");

    let select = read_only_driver
        .execute_user_query("SELECT id FROM read_only_guard", None, None)
        .await
        .expect("select still works on a read-only connection");
    assert_eq!(select.rows.len(), 0);

    let insert_err = read_only_driver
        .execute_user_query(
            "INSERT INTO read_only_guard (id) VALUES ($1)",
            None,
            Some(&[CellValue::Text("1".to_string())]),
        )
        .await
        .expect_err("insert must be rejected on a read-only connection");
    assert_eq!(insert_err.kind, DriverErrorKind::Query);

    let verify = connected_driver(&container).await;
    let readback = verify
        .execute_user_query("SELECT id FROM read_only_guard", None, None)
        .await
        .expect("read back");
    assert_eq!(
        readback.rows.len(),
        0,
        "the rejected insert must not have persisted anything"
    );
}

#[tokio::test]
async fn stream_rows_yields_multiple_batches_matching_total_count() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;
    seed_numbered_table(&driver, "streamed", 2500).await;

    let mut stream = driver.stream_rows("SELECT n FROM streamed ORDER BY n");
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;
    let mut saw_header = false;

    while let Some(element) = stream.next().await {
        match element.expect("stream element") {
            StreamElement::Header(header) => {
                assert!(!saw_header, "header must be sent exactly once");
                assert_eq!(header.columns, vec!["n".to_string()]);
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
    assert_eq!(total_rows, 2500);
}

#[tokio::test]
async fn schema_introspection_reports_real_columns_indexes_and_foreign_keys() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE parent_t (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
        .await
        .expect("create parent_t");
    driver
        .execute(
            "CREATE TABLE child_t (
                id SERIAL PRIMARY KEY,
                parent_id INTEGER NOT NULL REFERENCES parent_t(id),
                email TEXT
            )",
        )
        .await
        .expect("create child_t");
    driver
        .execute("CREATE UNIQUE INDEX child_email_idx ON child_t(email)")
        .await
        .expect("create index");

    let columns = driver
        .fetch_columns("child_t", Some("public"))
        .await
        .expect("fetch columns");
    let id_col = columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column present");
    assert!(id_col.is_primary_key);
    let parent_id_col = columns
        .iter()
        .find(|c| c.name == "parent_id")
        .expect("parent_id column present");
    assert!(!parent_id_col.is_nullable);
    let email_col = columns
        .iter()
        .find(|c| c.name == "email")
        .expect("email column present");
    assert!(email_col.is_nullable);

    let indexes = driver
        .fetch_indexes("child_t", Some("public"))
        .await
        .expect("fetch indexes");
    let pk_index = indexes
        .iter()
        .find(|i| i.is_primary)
        .expect("primary key index present");
    assert_eq!(pk_index.columns, vec!["id".to_string()]);
    let email_index = indexes
        .iter()
        .find(|i| i.name == "child_email_idx")
        .expect("email index present");
    assert!(email_index.is_unique);
    assert!(!email_index.is_primary);
    assert_eq!(email_index.columns, vec!["email".to_string()]);

    let foreign_keys = driver
        .fetch_foreign_keys("child_t", Some("public"))
        .await
        .expect("fetch fks");
    assert_eq!(foreign_keys.len(), 1);
    let fk = &foreign_keys[0];
    assert_eq!(fk.columns, vec!["parent_id".to_string()]);
    assert_eq!(fk.referenced_table, "parent_t");
    assert_eq!(fk.referenced_columns, vec!["id".to_string()]);

    let tables = driver
        .fetch_tables(Some("public"))
        .await
        .expect("fetch tables");
    assert!(tables.iter().any(|t| t.name == "child_t"));
    assert!(tables.iter().any(|t| t.name == "parent_t"));
}

#[tokio::test]
async fn fetch_view_definition_returns_real_view_body() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("CREATE TABLE viewed_t (id INTEGER PRIMARY KEY, active BOOLEAN)")
        .await
        .expect("create table");
    driver
        .execute("CREATE VIEW active_viewed_t AS SELECT id FROM viewed_t WHERE active = true")
        .await
        .expect("create view");

    let definition = driver
        .fetch_view_definition("active_viewed_t", Some("public"))
        .await
        .expect("fetch view definition");

    assert!(definition.to_lowercase().contains("viewed_t"));
    assert!(definition.to_lowercase().contains("active"));
}

#[tokio::test]
async fn create_database_with_quote_in_name_round_trips_and_drop_removes_it() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let request = CreateDatabaseRequest {
        name: "weird\"db".to_string(),
        owner: None,
        encoding: None,
        additional_fields: HashMap::new(),
    };
    driver
        .create_database(&request)
        .await
        .expect("create database with quoted name");

    let databases = driver.fetch_databases().await.expect("fetch databases");
    assert!(databases.contains(&"weird\"db".to_string()));

    driver
        .drop_database("weird\"db")
        .await
        .expect("drop database with quoted name");

    let databases_after = driver
        .fetch_databases()
        .await
        .expect("fetch databases after drop");
    assert!(!databases_after.contains(&"weird\"db".to_string()));
}

#[tokio::test]
async fn create_and_drop_database_round_trip() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let request = CreateDatabaseRequest {
        name: "roundtrip_db".to_string(),
        owner: None,
        encoding: Some("UTF8".to_string()),
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
async fn cancel_query_interrupts_a_long_running_query() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let start = std::time::Instant::now();

    let query_fut = driver.execute("SELECT pg_sleep(30)");
    let cancel_fut = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        driver.cancel_query().expect("cancel query");
    };

    let (result, ()) = tokio::join!(query_fut, cancel_fut);
    let elapsed = start.elapsed();

    assert!(result.is_err(), "cancelled query must return an error");
    assert!(
        elapsed < Duration::from_secs(20),
        "cancellation should interrupt the query well before its natural 30s completion, took {elapsed:?}"
    );
}

/// The official Postgres image runs every `/docker-entrypoint-initdb.d/*`
/// script (sourced, not executed, since the copied-in file is not
/// marked executable) after `initdb` but before the real server starts,
/// running as the `postgres` user against `$PGDATA`. Copying the
/// generated cert/key in elsewhere first and letting this script `cp`
/// them into place sidesteps entirely having to make a host-copied file
/// land inside the container already owned by `postgres` with `0600`
/// permissions -- the `cp` here creates a brand new file owned by
/// whoever ran it, which is the postgres user script executes as.
const ENABLE_TLS_INIT_SCRIPT: &str = r#"#!/bin/bash
set -e
cp /tmp/tls/server.crt "$PGDATA/server.crt"
cp /tmp/tls/server.key "$PGDATA/server.key"
chmod 600 "$PGDATA/server.key"
cat >> "$PGDATA/postgresql.conf" <<'EOF'
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
EOF
"#;

/// Generates a self-signed cert/key pair for `dns_name`. Deliberately
/// used with a `dns_name` that can never match the container's real
/// connection address (an IP, or `localhost`), so `VerifyIdentity`'s
/// hostname check has something real to reject regardless of how this
/// environment's Docker daemon happens to be reachable.
fn self_signed_cert_pem(dns_name: &str) -> (Vec<u8>, Vec<u8>) {
    let mut params = rcgen::CertificateParams::default();
    params.subject_alt_names = vec![rcgen::SanType::DnsName(
        dns_name.try_into().expect("valid dns name"),
    )];
    let key_pair = rcgen::KeyPair::generate().expect("generate key pair");
    let cert = params.self_signed(&key_pair).expect("self-sign cert");
    (
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes(),
    )
}

async fn start_tls_container(cert_pem: Vec<u8>, key_pem: Vec<u8>) -> ContainerAsync<Postgres> {
    Postgres::default()
        .with_password(POSTGRES_PASSWORD)
        .with_copy_to("/tmp/tls/server.crt", cert_pem)
        .with_copy_to("/tmp/tls/server.key", key_pem)
        .with_copy_to(
            "/docker-entrypoint-initdb.d/00-enable-ssl.sh",
            ENABLE_TLS_INIT_SCRIPT.as_bytes().to_vec(),
        )
        .start()
        .await
        .expect("start postgres container with TLS enabled")
}

fn write_ca_file(pem: &[u8]) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().expect("create temp file for ca_path");
    file.write_all(pem).expect("write ca_path contents");
    file.flush().expect("flush ca_path contents");
    file
}

async fn tls_config(
    container: &ContainerAsync<Postgres>,
    port: u16,
    mode: SslMode,
    ca_path: Option<&std::path::Path>,
) -> ConnectionConfig {
    ConnectionConfig {
        host: container
            .get_host()
            .await
            .expect("container host")
            .to_string(),
        port,
        username: "postgres".to_string(),
        password: Some(secrecy::SecretString::from(POSTGRES_PASSWORD.to_string())),
        database: Some("postgres".to_string()),
        ssl: SslConfig {
            mode: Some(mode),
            ca_path: ca_path.map(|p| p.to_path_buf()),
            client_cert_path: None,
            client_key_path: None,
        },
        read_only: false,
        additional_fields: HashMap::new(),
    }
}

/// `VerifyCa` trusts the server's own self-signed cert (given as its own
/// CA) and never checks the hostname, so this connects successfully even
/// though the cert's only SAN (`postgres-tls-test.invalid`) can never
/// match this container's real connection address.
#[tokio::test]
async fn verify_ca_with_correct_ca_path_accepts_connection_despite_hostname_mismatch() {
    let (cert_pem, key_pem) = self_signed_cert_pem("postgres-tls-test.invalid");
    let container = start_tls_container(cert_pem.clone(), key_pem).await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let ca_file = write_ca_file(&cert_pem);

    let config = tls_config(&container, port, SslMode::VerifyCa, Some(ca_file.path())).await;
    let driver = PostgresDriver::new(config);
    driver.connect().await.expect("connect over TLS");
    driver
        .execute("SELECT 1")
        .await
        .expect("query over verified TLS connection");
}

/// `VerifyCa` against a CA that never signed the presented certificate
/// must reject the connection: chain verification is real, not a no-op.
#[tokio::test]
async fn verify_ca_with_wrong_ca_path_rejects_connection() {
    let (cert_pem, key_pem) = self_signed_cert_pem("postgres-tls-test.invalid");
    let (wrong_ca_pem, _unused_key) = self_signed_cert_pem("unrelated-ca.invalid");
    let container = start_tls_container(cert_pem, key_pem).await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let ca_file = write_ca_file(&wrong_ca_pem);

    let config = tls_config(&container, port, SslMode::VerifyCa, Some(ca_file.path())).await;
    let driver = PostgresDriver::new(config);
    driver
        .connect()
        .await
        .expect_err("a cert not signed by the configured CA must be rejected");
}

/// The property that matters most: `VerifyIdentity` must reject exactly
/// the connection `VerifyCa` (correctly) accepted above, proving the
/// hostname check is real and actually distinguishes the two modes
/// rather than `VerifyIdentity` silently behaving like `VerifyCa`.
#[tokio::test]
async fn verify_identity_rejects_hostname_mismatch_even_with_correct_ca_path() {
    let (cert_pem, key_pem) = self_signed_cert_pem("postgres-tls-test.invalid");
    let container = start_tls_container(cert_pem.clone(), key_pem).await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let ca_file = write_ca_file(&cert_pem);

    let config = tls_config(
        &container,
        port,
        SslMode::VerifyIdentity,
        Some(ca_file.path()),
    )
    .await;
    let driver = PostgresDriver::new(config);
    driver
        .connect()
        .await
        .expect_err("a hostname mismatch must be rejected under verify_identity");
}

/// `Required` (and `Preferred`) match libpq's own `sslmode=require`
/// semantics: encrypt the connection, verify nothing about the
/// certificate at all. Must succeed against the same untrusted,
/// hostname-mismatched self-signed cert the two tests above reject or
/// accept selectively.
#[tokio::test]
async fn required_mode_connects_despite_untrusted_self_signed_cert() {
    let (cert_pem, key_pem) = self_signed_cert_pem("postgres-tls-test.invalid");
    let container = start_tls_container(cert_pem, key_pem).await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");

    let config = tls_config(&container, port, SslMode::Required, None).await;
    let driver = PostgresDriver::new(config);
    driver
        .connect()
        .await
        .expect("required mode connects without verifying the certificate");
    driver
        .execute("SELECT 1")
        .await
        .expect("query over the unverified TLS connection");
}
