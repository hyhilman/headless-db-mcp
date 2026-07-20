//! Integration tests against a real, ephemeral Redis container.
//!
//! Every test spins up its own container via `testcontainers`/
//! `testcontainers-modules`, so this file never assumes a pre-existing
//! running Redis. Docker must be reachable (`docker info`).
//!
//! The image tag is pinned to `7.2` rather than the module's default
//! (`5.0`): `SCAN`'s server-side `TYPE` filter, which `stream_rows` (see
//! `src/stream.rs`) relies on to avoid ever scanning-then-filtering
//! client-side, was only added in Redis 6.0.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use db_headless_core::{
    CellValue, ConnectionConfig, CreateDatabaseRequest, DatabaseDriver, DriverErrorKind, SslConfig,
    SslMode, StreamElement,
};
use db_headless_driver_redis::RedisDriver;
use futures_util::StreamExt;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::core::wait::LogWaitStrategy;
use testcontainers_modules::testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

const REDIS_TAG: &str = "7.2";
const REDIS_PASSWORD: &str = "real-s3cret-value";

async fn start_container() -> ContainerAsync<Redis> {
    Redis::default()
        .with_tag(REDIS_TAG)
        .start()
        .await
        .expect("start redis container")
}

async fn start_container_with_password() -> ContainerAsync<Redis> {
    Redis::default()
        .with_tag(REDIS_TAG)
        .with_cmd(["--requirepass", REDIS_PASSWORD])
        .start()
        .await
        .expect("start redis container with password")
}

async fn config_for(container: &ContainerAsync<Redis>, password: Option<&str>) -> ConnectionConfig {
    let host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("container port");

    ConnectionConfig {
        host,
        port,
        username: String::new(),
        password: password.map(|p| secrecy::SecretString::from(p.to_string())),
        database: None,
        ssl: SslConfig::disabled(),
        read_only: false,
        additional_fields: HashMap::new(),
    }
}

async fn connected_driver(container: &ContainerAsync<Redis>) -> RedisDriver {
    let config = config_for(container, None).await;
    let driver = RedisDriver::new(config);
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

/// Regression test for the documented `ping()` override: the trait's
/// default body runs `execute("SELECT 1")`, and Redis has a real `SELECT`
/// command that would switch the connection to database index 1. Merely
/// asserting `ping()` returns `Ok` would not catch that bug (the default
/// body's `execute` call also succeeds) — this test proves the *side
/// effect* did not happen by checking a key set on database 0 is still
/// visible after `ping()`.
#[tokio::test]
async fn ping_does_not_change_the_selected_database() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("SET marker db0value")
        .await
        .expect("seed marker on db 0");

    for _ in 0..3 {
        driver.ping().await.expect("ping");
    }

    let result = driver
        .execute("GET marker")
        .await
        .expect("read marker back");
    assert_eq!(
        result.rows,
        vec![vec![CellValue::Text("db0value".to_string())]],
        "ping() must never issue a literal SELECT and change the selected database"
    );
}

#[tokio::test]
async fn connect_failure_with_wrong_password_does_not_leak_password() {
    let container = start_container_with_password().await;
    let wrong_password = "definitely-not-the-real-password";
    let config = config_for(&container, Some(wrong_password)).await;
    let driver = RedisDriver::new(config);

    let err = driver
        .connect()
        .await
        .expect_err("wrong password must fail to connect");

    assert_eq!(err.kind, DriverErrorKind::Auth);
    assert!(!err.message.contains(wrong_password));
    assert!(!err.message.contains(REDIS_PASSWORD));
    if let Some(detail) = &err.detail {
        assert!(!detail.contains(wrong_password));
        assert!(!detail.contains(REDIS_PASSWORD));
    }
}

#[tokio::test]
async fn execute_parameterized_round_trips_a_value_with_spaces_and_a_literal_question_mark() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let tricky_value = "value ? with spaces and another ?";
    driver
        .execute_parameterized("SET mykey ?", &[CellValue::Text(tricky_value.to_string())])
        .await
        .expect("set with bound parameter");

    let result = driver
        .execute_parameterized("GET mykey", &[])
        .await
        .expect("get back");
    assert_eq!(
        result.rows,
        vec![vec![CellValue::Text(tricky_value.to_string())]]
    );
}

#[tokio::test]
async fn null_parameter_is_rejected_rather_than_becoming_an_empty_string() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let err = driver
        .execute_parameterized("SET mykey ?", &[CellValue::Null])
        .await
        .expect_err("null must not silently bind as empty string");
    assert_eq!(err.kind, DriverErrorKind::Query);

    let result = driver
        .execute("EXISTS mykey")
        .await
        .expect("check key was never created");
    assert_eq!(result.rows, vec![vec![CellValue::Text("0".to_string())]]);
}

/// A `read_only` connection must reject a write command through
/// `execute_user_query` (the only user-reachable arbitrary-command path)
/// while still allowing a read, and the rejected write must not have
/// reached the server at all.
#[tokio::test]
async fn read_only_connection_rejects_writes_but_allows_reads() {
    let container = start_container().await;
    let mut read_only_config = config_for(&container, None).await;
    read_only_config.read_only = true;
    let driver = RedisDriver::new(read_only_config);
    driver.connect().await.expect("connect");

    let get_result = driver
        .execute_user_query("GET mykey", None, None)
        .await
        .expect("a read command is allowed on a read-only connection");
    assert_eq!(get_result.rows, vec![vec![CellValue::Null]]);

    let set_err = driver
        .execute_user_query("SET mykey myvalue", None, None)
        .await
        .expect_err("a write command must be rejected on a read-only connection");
    assert_eq!(set_err.kind, DriverErrorKind::Query);

    let verify = connected_driver(&container).await;
    let readback = verify
        .execute("EXISTS mykey")
        .await
        .expect("check the rejected write never reached the server");
    assert_eq!(readback.rows, vec![vec![CellValue::Text("0".to_string())]]);
}

#[tokio::test]
async fn fetch_tables_always_returns_exactly_the_six_pseudo_tables_regardless_of_keys() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("SET onlykey onlyvalue")
        .await
        .expect("seed one string key");

    let tables = driver.fetch_tables(None).await.expect("fetch tables");
    let mut names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec!["hash", "list", "set", "stream", "string", "zset"]
    );
}

#[tokio::test]
async fn fetch_columns_matches_the_documented_shape_for_every_pseudo_table() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let expected: &[(&str, &[&str])] = &[
        ("string", &["key", "value"]),
        ("hash", &["key", "field", "value"]),
        ("list", &["key", "index", "value"]),
        ("set", &["key", "member"]),
        ("zset", &["key", "member", "score"]),
        ("stream", &["key", "id", "fields"]),
    ];

    for (table, columns) in expected {
        let fetched = driver
            .fetch_columns(table, None)
            .await
            .unwrap_or_else(|_| panic!("fetch columns for {table}"));
        let names: Vec<&str> = fetched.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names, *columns,
            "column shape mismatch for pseudo-table {table}"
        );
    }

    let err = driver
        .fetch_columns("bogus", None)
        .await
        .expect_err("unknown pseudo-table must error");
    assert_eq!(err.kind, DriverErrorKind::Query);
}

#[tokio::test]
async fn stream_rows_over_hash_pseudo_table_yields_multiple_batches_matching_total_row_count() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    const KEY_COUNT: usize = 60;
    const FIELDS_PER_KEY: usize = 3;

    for i in 0..KEY_COUNT {
        for field in 0..FIELDS_PER_KEY {
            driver
                .execute(&format!(
                    "HSET streamtest:{i} field{field} value{i}-{field}"
                ))
                .await
                .expect("seed hash field");
        }
    }

    let mut stream = driver.stream_rows("hash MATCH streamtest:* COUNT 5");
    let mut batch_count = 0usize;
    let mut total_rows = 0usize;
    let mut saw_header = false;

    while let Some(element) = stream.next().await {
        match element.expect("stream element") {
            StreamElement::Header(header) => {
                assert!(!saw_header, "header must be sent exactly once");
                assert_eq!(header.columns, vec!["key", "field", "value"]);
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
        "expected more than one SCAN batch, got {batch_count}"
    );
    assert_eq!(total_rows, KEY_COUNT * FIELDS_PER_KEY);
}

#[tokio::test]
async fn switch_database_actually_changes_which_database_commands_run_against() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    driver
        .execute("SET onlydb0 hello")
        .await
        .expect("seed key on db 0");

    driver.switch_database("1").await.expect("switch to db 1");

    let missing_on_db1 = driver.execute("GET onlydb0").await.expect("get on db 1");
    assert_eq!(missing_on_db1.rows, vec![vec![CellValue::Null]]);

    driver
        .execute("SET onlydb1 world")
        .await
        .expect("seed key on db 1");

    driver
        .switch_database("0")
        .await
        .expect("switch back to db 0");

    let missing_on_db0 = driver.execute("GET onlydb1").await.expect("get on db 0");
    assert_eq!(missing_on_db0.rows, vec![vec![CellValue::Null]]);

    let still_on_db0 = driver
        .execute("GET onlydb0")
        .await
        .expect("get original key back on db 0");
    assert_eq!(
        still_on_db0.rows,
        vec![vec![CellValue::Text("hello".to_string())]]
    );
}

#[tokio::test]
async fn create_and_drop_database_return_clear_errors_rather_than_attempting_anything() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let request = CreateDatabaseRequest {
        name: "wontwork".to_string(),
        owner: None,
        encoding: None,
        additional_fields: HashMap::new(),
    };
    let create_err = driver
        .create_database(&request)
        .await
        .expect_err("create_database must error");
    assert_eq!(create_err.kind, DriverErrorKind::Query);

    let drop_err = driver
        .drop_database("wontwork")
        .await
        .expect_err("drop_database must error");
    assert_eq!(drop_err.kind, DriverErrorKind::Query);
}

/// Regression test for the documented `begin_transaction` override: the
/// trait's default body runs `execute("BEGIN")`, which is not a valid
/// Redis command. This asserts the override rejects the call outright,
/// never reaching the server with a literal `BEGIN`.
#[tokio::test]
async fn begin_transaction_returns_a_clear_error_instead_of_sending_a_literal_begin_command() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    assert!(!driver.supports_transactions());

    let err = driver
        .begin_transaction()
        .await
        .expect_err("begin_transaction must not be supported");
    assert_eq!(err.kind, DriverErrorKind::Query);
}

#[tokio::test]
async fn cancel_query_interrupts_a_long_running_blocking_command() {
    let container = start_container().await;
    let driver = connected_driver(&container).await;

    let start = std::time::Instant::now();

    let query_fut = driver.execute("BLPOP somekey 30");
    let cancel_fut = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        driver.cancel_query().expect("cancel query");
    };

    let (result, ()) = tokio::join!(query_fut, cancel_fut);
    let elapsed = start.elapsed();

    assert!(result.is_err(), "cancelled command must return an error");
    assert!(
        elapsed < Duration::from_secs(20),
        "cancellation should interrupt the blocking command well before its natural 30s completion, took {elapsed:?}"
    );
}

const REDIS_TLS_PORT: u16 = 6380;

/// Generates a self-signed cert/key pair whose only SAN is `name`, either
/// a DNS name or an IP address. `redis-rs`'s non-`insecure` TLS path
/// always checks the hostname (see `src/config.rs`'s module doc comment
/// for why it can't be split from the chain check the way
/// `driver-postgres`'s `VerifyCa` does), so unlike that crate's own
/// `self_signed_cert_pem`, this one is also used to build a certificate
/// that genuinely matches the real connection host, not just mismatched
/// ones.
fn self_signed_cert_pem(name: &str) -> (Vec<u8>, Vec<u8>) {
    let san = match name.parse::<IpAddr>() {
        Ok(ip) => rcgen::SanType::IpAddress(ip),
        Err(_) => rcgen::SanType::DnsName(name.try_into().expect("valid dns name")),
    };
    let mut params = rcgen::CertificateParams::default();
    params.subject_alt_names = vec![san];
    let key_pair = rcgen::KeyPair::generate().expect("generate key pair");
    let cert = params.self_signed(&key_pair).expect("self-sign cert");
    (
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes(),
    )
}

/// The host testcontainers will actually connect through, discovered by
/// starting a throwaway plain container and asking it: `get_host()`
/// reflects the Docker client's own configuration (a plain per-daemon
/// property, e.g. `127.0.0.1` for a local Docker socket), not anything
/// container-specific, but the API is only reachable from a running
/// container. This container is torn down as soon as the function
/// returns.
async fn docker_host_string() -> String {
    let probe = start_container().await;
    probe.get_host().await.expect("container host").to_string()
}

/// Starts a real Redis server with its TLS listener enabled on
/// [`REDIS_TLS_PORT`], alongside its plaintext port (left enabled so the
/// well-established `"Ready to accept connections"` readiness message is
/// never solely dependent on the exact wording Redis uses for its TLS
/// listener specifically). `--tls-auth-clients no` means this is
/// server-authentication-only TLS, matching what `driver-redis` actually
/// implements (mTLS is out of scope, same as `driver-postgres`), so the
/// server's own `--tls-ca-cert-file` (needed only to authenticate
/// clients) is never exercised; `cert_pem` doubles as that value purely
/// so the server has *something* valid to load there.
async fn start_tls_container(cert_pem: Vec<u8>, key_pem: Vec<u8>) -> ContainerAsync<GenericImage> {
    GenericImage::new("redis", REDIS_TAG)
        .with_exposed_port(REDIS_TLS_PORT.tcp())
        .with_wait_for(WaitFor::log(
            LogWaitStrategy::stdout("Ready to accept connections").with_times(2),
        ))
        .with_copy_to("/tls/redis.crt", cert_pem.clone())
        .with_copy_to("/tls/redis.key", key_pem)
        .with_copy_to("/tls/ca.crt", cert_pem)
        .with_cmd([
            "--tls-port",
            "6380",
            "--tls-cert-file",
            "/tls/redis.crt",
            "--tls-key-file",
            "/tls/redis.key",
            "--tls-ca-cert-file",
            "/tls/ca.crt",
            "--tls-auth-clients",
            "no",
        ])
        .start()
        .await
        .expect("start redis container with TLS enabled")
}

fn write_ca_file(pem: &[u8]) -> tempfile::NamedTempFile {
    use std::io::Write;
    let mut file = tempfile::NamedTempFile::new().expect("create temp file for ca_path");
    file.write_all(pem).expect("write ca_path contents");
    file.flush().expect("flush ca_path contents");
    file
}

async fn tls_config(
    container: &ContainerAsync<GenericImage>,
    mode: SslMode,
    ca_path: Option<&std::path::Path>,
) -> ConnectionConfig {
    let host = container
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(REDIS_TLS_PORT)
        .await
        .expect("container tls port");

    ConnectionConfig {
        host,
        port,
        username: String::new(),
        password: None,
        database: None,
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

/// `Required` (and `Preferred`) match libpq's own `sslmode=require`
/// semantics: encrypt the connection, verify nothing about the
/// certificate at all. Must succeed against a self-signed, untrusted,
/// hostname-mismatched cert.
#[tokio::test]
async fn required_mode_connects_despite_untrusted_self_signed_cert() {
    let (cert_pem, key_pem) = self_signed_cert_pem("redis-tls-test.invalid");
    let container = start_tls_container(cert_pem, key_pem).await;

    let config = tls_config(&container, SslMode::Required, None).await;
    let driver = RedisDriver::new(config);
    driver
        .connect()
        .await
        .expect("required mode connects without verifying the certificate");
    driver
        .ping()
        .await
        .expect("command over the unverified TLS connection");
}

/// The positive case for the mode `driver-redis` can actually support in
/// full: a certificate whose SAN genuinely matches the real connection
/// host, trusted via its own PEM as `ssl.ca_path`. Both the chain and the
/// hostname check must pass.
#[tokio::test]
async fn verify_identity_with_correct_ca_path_and_matching_hostname_accepts_connection() {
    let host = docker_host_string().await;
    let (cert_pem, key_pem) = self_signed_cert_pem(&host);
    let container = start_tls_container(cert_pem.clone(), key_pem).await;
    let ca_file = write_ca_file(&cert_pem);

    let config = tls_config(&container, SslMode::VerifyIdentity, Some(ca_file.path())).await;
    let driver = RedisDriver::new(config);
    driver.connect().await.expect("connect over verified TLS");
    driver
        .ping()
        .await
        .expect("command over verified TLS connection");
}

/// `driver-redis` cannot express `driver-postgres`'s `VerifyCa` (chain
/// trusted, hostname unchecked) because `redis-rs`'s TLS API has no hook
/// for a custom certificate verifier — see `src/config.rs`'s module doc
/// comment. `VerifyCa` collapses onto `VerifyIdentity`'s stricter
/// behavior instead of erroring or silently skipping the hostname check,
/// so both modes must reject the exact same hostname-mismatched cert
/// here, proving `VerifyCa` never ends up more lenient than intended.
#[tokio::test]
async fn verify_ca_rejects_hostname_mismatch_just_like_verify_identity() {
    for mode in [SslMode::VerifyCa, SslMode::VerifyIdentity] {
        let (cert_pem, key_pem) = self_signed_cert_pem("redis-tls-test.invalid");
        let container = start_tls_container(cert_pem.clone(), key_pem).await;
        let ca_file = write_ca_file(&cert_pem);

        let config = tls_config(&container, mode, Some(ca_file.path())).await;
        let driver = RedisDriver::new(config);
        driver.connect().await.expect_err(&format!(
            "a hostname mismatch must be rejected under {mode:?}"
        ));
    }
}

/// Chain verification is real, not a no-op: a CA that never signed the
/// presented certificate must be rejected under both modes.
#[tokio::test]
async fn verify_ca_and_verify_identity_reject_a_cert_from_an_unrelated_ca() {
    for mode in [SslMode::VerifyCa, SslMode::VerifyIdentity] {
        let host = docker_host_string().await;
        let (cert_pem, key_pem) = self_signed_cert_pem(&host);
        let (wrong_ca_pem, _unused_key) = self_signed_cert_pem("unrelated-ca.invalid");
        let container = start_tls_container(cert_pem, key_pem).await;
        let ca_file = write_ca_file(&wrong_ca_pem);

        let config = tls_config(&container, mode, Some(ca_file.path())).await;
        let driver = RedisDriver::new(config);
        driver.connect().await.expect_err(&format!(
            "a cert not signed by the configured CA must be rejected under {mode:?}"
        ));
    }
}
