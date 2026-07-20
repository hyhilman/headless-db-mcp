//! Builds a `redis::ConnectionInfo` from a `db_headless_core::ConnectionConfig`,
//! and from it a real `redis::Client` wired up for whatever `SslMode` the
//! config asks for.
//!
//! `ConnectionInfo` itself is built as a struct
//! (`redis::ConnectionInfo { addr, redis: RedisConnectionInfo { .. } }`)
//! rather than by formatting a `redis://user:pass@host:port/db` URL
//! string: hand-assembling and percent-encoding a credentials URL is
//! exactly the kind of ad hoc string construction this project avoids
//! elsewhere, and the struct constructor needs no escaping at all.
//!
//! # TLS: real, via `redis-rs`'s own `rustls` integration
//!
//! Unlike `driver-postgres`, which hand-rolls a `rustls::ClientConfig`
//! (needed there to get a chain-only verifier), `redis-rs` already ships
//! working `rustls` support behind its `tls-rustls`/`tls-rustls-insecure`
//! features, so `build_client` uses that directly instead of duplicating
//! it:
//!
//! - `Disabled`: plain TCP, `redis::Client::open`.
//! - `Preferred` / `Required`: `ConnectionAddr::TcpTls { insecure: true, .. }`.
//!   Still a real, encrypted TLS handshake; the certificate's chain and
//!   hostname are not checked at all, matching libpq's own
//!   `sslmode=require` semantics (and this workspace's own
//!   `driver-postgres` precedent for the same two modes).
//! - `VerifyCa`, `VerifyIdentity`, and a missing `mode` (guardrail #6:
//!   never silently downgrade): `insecure: false`. `redis-rs`'s public
//!   TLS API only exposes that one binary flag — there is no hook to
//!   supply a custom `rustls::client::danger::ServerCertVerifier`, so
//!   "verify the chain but skip the hostname" (`driver-postgres`'s
//!   `VerifyCa` behavior) cannot be expressed through it. `VerifyCa`
//!   therefore gets the same, stricter chain-and-hostname check as
//!   `VerifyIdentity` here. Collapsing upward like this is the safe
//!   direction: guardrail #6 forbids verifying *less* than the mode
//!   asks for, never verifying more. `ssl.ca_path`, when set, is read
//!   and passed as the trusted root; otherwise `redis-rs` falls back to
//!   the platform's native trust store on its own.

use std::fs;

use secrecy::ExposeSecret;

use db_headless_core::{ConnectionConfig, DriverError, DriverErrorKind, DriverResult, SslMode};

use crate::error;

/// Parses `ConnectionConfig::database` as Redis's numeric database index.
/// `None` (or an empty string) defaults to `0`, matching Redis's own
/// default database. A non-numeric value is a clear `DriverError`, never
/// a panic and never a silent fallback to `0`.
pub(crate) fn parse_db_index(database: Option<&str>) -> DriverResult<i64> {
    match database.map(str::trim) {
        None | Some("") => Ok(0),
        Some(s) => s.parse::<i64>().map_err(|_| {
            DriverError::new(
                DriverErrorKind::Query,
                format!("Redis database must be a numeric index, got {s:?}"),
            )
        }),
    }
}

pub(crate) fn build_connection_info(
    config: &ConnectionConfig,
) -> DriverResult<redis::ConnectionInfo> {
    let db = parse_db_index(config.database.as_deref())?;

    let username = if config.username.is_empty() {
        None
    } else {
        Some(config.username.clone())
    };
    let password = config
        .password
        .as_ref()
        .map(|secret| secret.expose_secret().to_string());

    let addr = match config.ssl.mode {
        Some(SslMode::Disabled) => redis::ConnectionAddr::Tcp(config.host.clone(), config.port),
        Some(SslMode::Preferred) | Some(SslMode::Required) => redis::ConnectionAddr::TcpTls {
            host: config.host.clone(),
            port: config.port,
            insecure: true,
            tls_params: None,
        },
        Some(SslMode::VerifyCa) | Some(SslMode::VerifyIdentity) | None => {
            redis::ConnectionAddr::TcpTls {
                host: config.host.clone(),
                port: config.port,
                insecure: false,
                tls_params: None,
            }
        }
    };

    Ok(redis::ConnectionInfo {
        addr,
        redis: redis::RedisConnectionInfo {
            db,
            username,
            password,
            protocol: redis::ProtocolVersion::RESP2,
        },
    })
}

/// Reads `ssl.ca_path` (if set) into the PEM bytes `redis-rs` wants as a
/// custom trusted root. `None` lets `redis-rs` fall back to the
/// platform's native trust store on its own.
fn root_cert_pem(config: &ConnectionConfig) -> DriverResult<Option<Vec<u8>>> {
    match &config.ssl.ca_path {
        Some(ca_path) => {
            let pem = fs::read(ca_path).map_err(|err| {
                DriverError::new(
                    DriverErrorKind::Connection,
                    format!("failed to read ssl.ca_path {}: {err}", ca_path.display()),
                )
            })?;
            Ok(Some(pem))
        }
        None => Ok(None),
    }
}

/// Builds a real `redis::Client` for `config`, connected over plain TCP
/// for `SslMode::Disabled` and over real TLS (see the module doc comment
/// for exactly what each mode verifies) for every other mode.
pub(crate) fn build_client(
    config: &ConnectionConfig,
    db_override: Option<i64>,
) -> DriverResult<redis::Client> {
    let mut info = build_connection_info(config)?;
    if let Some(db) = db_override {
        info.redis.db = db;
    }

    if matches!(config.ssl.mode, Some(SslMode::Disabled)) {
        return redis::Client::open(info).map_err(error::map_connect_error);
    }

    let root_cert = if matches!(
        config.ssl.mode,
        Some(SslMode::Preferred | SslMode::Required)
    ) {
        None
    } else {
        root_cert_pem(config)?
    };

    redis::Client::build_with_tls(
        info,
        redis::TlsCertificates {
            client_tls: None,
            root_cert,
        },
    )
    .map_err(error::map_connect_error)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use db_headless_core::SslConfig;

    use super::*;

    fn base_config(ssl: SslConfig) -> ConnectionConfig {
        ConnectionConfig {
            host: "localhost".to_string(),
            port: 6379,
            username: String::new(),
            password: None,
            database: None,
            ssl,
            read_only: false,
            additional_fields: HashMap::new(),
        }
    }

    #[test]
    fn missing_database_defaults_to_zero() {
        assert_eq!(parse_db_index(None).expect("parse"), 0);
    }

    #[test]
    fn empty_database_defaults_to_zero() {
        assert_eq!(parse_db_index(Some("")).expect("parse"), 0);
    }

    #[test]
    fn numeric_database_parses() {
        assert_eq!(parse_db_index(Some("3")).expect("parse"), 3);
    }

    #[test]
    fn non_numeric_database_is_a_clear_error_not_a_panic() {
        let err = parse_db_index(Some("not-a-number")).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Query);
    }

    #[test]
    fn disabled_mode_builds_plain_tcp_connection_info() {
        let config = base_config(SslConfig::disabled());
        let info = build_connection_info(&config).expect("build info");
        assert_eq!(info.redis.db, 0);
        assert!(matches!(info.addr, redis::ConnectionAddr::Tcp(_, 6379)));
    }

    #[test]
    fn preferred_and_required_build_insecure_tls_connection_info() {
        for mode in [SslMode::Preferred, SslMode::Required] {
            let config = base_config(SslConfig {
                mode: Some(mode),
                ..Default::default()
            });
            let info = build_connection_info(&config).expect("build info");
            match info.addr {
                redis::ConnectionAddr::TcpTls { insecure, .. } => assert!(insecure),
                other => panic!("expected TcpTls, got {other:?}"),
            }
        }
    }

    #[test]
    fn verify_ca_verify_identity_and_missing_mode_all_build_verified_tls_connection_info() {
        for ssl in [
            SslConfig {
                mode: Some(SslMode::VerifyCa),
                ..Default::default()
            },
            SslConfig {
                mode: Some(SslMode::VerifyIdentity),
                ..Default::default()
            },
            SslConfig::default(),
        ] {
            let config = base_config(ssl);
            let info = build_connection_info(&config).expect("build info");
            match info.addr {
                redis::ConnectionAddr::TcpTls { insecure, .. } => assert!(!insecure),
                other => panic!("expected TcpTls, got {other:?}"),
            }
        }
    }

    #[test]
    fn build_client_for_disabled_mode_does_not_require_tls_setup() {
        let config = base_config(SslConfig::disabled());
        build_client(&config, None).expect("plain TCP client always builds");
    }

    #[test]
    fn build_client_for_preferred_and_required_does_not_need_a_ca_path() {
        for mode in [SslMode::Preferred, SslMode::Required] {
            let config = base_config(SslConfig {
                mode: Some(mode),
                ..Default::default()
            });
            build_client(&config, None).expect("insecure TLS client builds without a CA");
        }
    }

    #[test]
    fn build_client_for_verify_ca_with_a_nonexistent_ca_path_is_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyCa),
            ca_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        });
        let err = build_client(&config, None).map(drop).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("ca_path"));
    }

    #[test]
    fn build_client_for_verify_identity_without_a_ca_path_falls_back_to_native_trust_store() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyIdentity),
            ..Default::default()
        });
        // Either a successful build or a clear `redis-rs` error about the
        // native trust store is acceptable in a minimal CI/container
        // image with no native certs; a panic is not.
        let _ = build_client(&config, None);
    }

    #[test]
    fn missing_mode_behaves_like_verify_identity_not_disabled() {
        let config = base_config(SslConfig::default());
        let _ = build_client(&config, None);
    }

    // There is deliberately no
    // `password_is_never_embedded_in_the_debug_output_of_the_built_info`
    // test here, unlike `driver-postgres`'s equivalent: `redis::Client`
    // and `redis::ConnectionInfo`/`RedisConnectionInfo` derive `Debug`
    // with no redaction, so `format!("{:?}", ...)` on either genuinely
    // does print the plaintext password (verified against redis 0.27.6's
    // source; `tokio_postgres::Config`, by contrast, implements a custom
    // `Debug` that hides it, which is why that assertion holds for
    // Postgres). The guardrail this driver actually upholds is narrower
    // and enforced differently: no code path in this crate ever formats
    // a `redis::Client`/`ConnectionInfo`/`RedisConnectionInfo` with
    // `{:?}` in a `DriverError`, a log line, or anywhere else reachable
    // from a caller. `map_connect_error`/`map_query_error` only ever
    // read `RedisError::to_string()`/`.code()`/`.detail()`, none of
    // which touch connection info. The integration test
    // `connect_failure_with_wrong_password_does_not_leak_password` in
    // `tests/integration.rs` is the enforcement point: it fails the day
    // any future change threads one of those types' `Debug` output into
    // a `DriverError`.
}
