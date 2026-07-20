//! Builds a `redis::ConnectionInfo` from a `db_headless_core::ConnectionConfig`.
//!
//! Built as a struct (`redis::ConnectionInfo { addr, redis: RedisConnectionInfo { .. } }`)
//! rather than by formatting a `redis://user:pass@host:port/db` URL string:
//! hand-assembling and percent-encoding a credentials URL is exactly the
//! kind of ad hoc string construction this project avoids elsewhere, and
//! the struct constructor needs no escaping at all.
//!
//! TLS support is intentionally scoped down for this pass, mirroring
//! `driver-postgres`'s precedent: only `SslMode::Disabled` and
//! `SslMode::Preferred` connect for real (both as plain TCP — `Preferred`
//! always falls back to plaintext, there is no TLS connector wired up
//! yet). Guardrail #6 requires a missing `mode` to be treated as
//! `VerifyIdentity`, never silently downgraded to `Disabled`, so an unset
//! mode and the three verified modes (`Required`, `VerifyCa`,
//! `VerifyIdentity`) all return a clear `DriverError` instead of
//! connecting.

use secrecy::ExposeSecret;

use db_headless_core::{ConnectionConfig, DriverError, DriverErrorKind, DriverResult, SslMode};

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
    resolve_ssl_mode(config)?;
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

    Ok(redis::ConnectionInfo {
        addr: redis::ConnectionAddr::Tcp(config.host.clone(), config.port),
        redis: redis::RedisConnectionInfo {
            db,
            username,
            password,
            protocol: redis::ProtocolVersion::RESP2,
        },
    })
}

fn resolve_ssl_mode(config: &ConnectionConfig) -> DriverResult<()> {
    match config.ssl.mode {
        Some(SslMode::Disabled) | Some(SslMode::Preferred) => Ok(()),
        None => Err(unverified_tls_error(
            "no ssl.mode was set; guardrail #6 treats a missing mode as VerifyIdentity, \
             which this driver build does not yet implement",
        )),
        Some(SslMode::Required) => Err(unverified_tls_error(
            "ssl.mode = required is not yet implemented by this driver build",
        )),
        Some(SslMode::VerifyCa) => Err(unverified_tls_error(
            "ssl.mode = verify_ca is not yet implemented by this driver build",
        )),
        Some(SslMode::VerifyIdentity) => Err(unverified_tls_error(
            "ssl.mode = verify_identity is not yet implemented by this driver build",
        )),
    }
}

fn unverified_tls_error(message: &str) -> DriverError {
    DriverError::new(
        DriverErrorKind::Connection,
        format!(
            "{message}; pass ssl.mode = disabled or preferred explicitly, or wait for verified \
             TLS support, connection refused rather than silently downgrading verification"
        ),
    )
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
    fn disabled_mode_builds_connection_info() {
        let config = base_config(SslConfig::disabled());
        let info = build_connection_info(&config).expect("build info");
        assert_eq!(info.redis.db, 0);
        assert!(matches!(info.addr, redis::ConnectionAddr::Tcp(_, 6379)));
    }

    #[test]
    fn preferred_mode_builds_connection_info() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::Preferred),
            ..Default::default()
        });
        assert!(build_connection_info(&config).is_ok());
    }

    #[test]
    fn missing_mode_is_rejected_rather_than_silently_downgraded() {
        let config = base_config(SslConfig::default());
        let err = build_connection_info(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("VerifyIdentity"));
    }

    #[test]
    fn required_mode_is_rejected_with_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::Required),
            ..Default::default()
        });
        let err = build_connection_info(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("required"));
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
