//! Builds a `tokio_postgres::Config` from a `db_headless_core::ConnectionConfig`.
//!
//! TLS support in this driver is intentionally scoped down for this pass:
//! only `SslMode::Disabled` and `SslMode::Preferred` connect for real
//! (`Preferred` negotiates with a `NoTls` connector, so it always falls
//! back to plaintext — there is no TLS connector wired up yet). Guardrail
//! #6 in `db-headless-core` requires a missing `mode` to be treated as
//! `VerifyIdentity`, never silently downgraded to `Disabled`, so an unset
//! mode and the three verified modes (`Required`, `VerifyCa`,
//! `VerifyIdentity`) all return a clear `DriverError` instead of
//! connecting. Wiring up `tokio-postgres-rustls` (or `postgres-native-tls`)
//! for those modes is tracked as follow-up work, not implemented here.

use db_headless_core::{ConnectionConfig, DriverError, DriverErrorKind, SslMode};
use secrecy::ExposeSecret;

pub fn build_config(config: &ConnectionConfig) -> Result<tokio_postgres::Config, DriverError> {
    let pg_ssl_mode = resolve_ssl_mode(config)?;

    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .host(&config.host)
        .port(config.port)
        .user(&config.username)
        .ssl_mode(pg_ssl_mode);

    if let Some(password) = &config.password {
        pg_config.password(password.expose_secret());
    }

    if let Some(database) = &config.database {
        pg_config.dbname(database);
    }

    Ok(pg_config)
}

fn resolve_ssl_mode(
    config: &ConnectionConfig,
) -> Result<tokio_postgres::config::SslMode, DriverError> {
    match config.ssl.mode {
        Some(SslMode::Disabled) => Ok(tokio_postgres::config::SslMode::Disable),
        Some(SslMode::Preferred) => Ok(tokio_postgres::config::SslMode::Prefer),
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
            port: 5432,
            username: "postgres".to_string(),
            password: None,
            database: None,
            ssl,
            additional_fields: HashMap::new(),
        }
    }

    #[test]
    fn disabled_mode_resolves_to_disable() {
        let config = base_config(SslConfig::disabled());
        let pg_config = build_config(&config).expect("build config");
        assert_eq!(
            pg_config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable
        );
    }

    #[test]
    fn preferred_mode_resolves_to_prefer() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::Preferred),
            ..Default::default()
        });
        let pg_config = build_config(&config).expect("build config");
        assert_eq!(
            pg_config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Prefer
        );
    }

    #[test]
    fn missing_mode_is_rejected_rather_than_silently_downgraded() {
        let config = base_config(SslConfig::default());
        let err = build_config(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("VerifyIdentity"));
    }

    #[test]
    fn required_mode_is_rejected_with_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::Required),
            ..Default::default()
        });
        let err = build_config(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("required"));
    }

    #[test]
    fn verify_ca_mode_is_rejected_with_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyCa),
            ..Default::default()
        });
        let err = build_config(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("verify_ca"));
    }

    #[test]
    fn verify_identity_mode_is_rejected_with_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyIdentity),
            ..Default::default()
        });
        let err = build_config(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("verify_identity"));
    }

    #[test]
    fn password_is_never_embedded_in_the_debug_output_of_the_built_config() {
        let mut config = base_config(SslConfig::disabled());
        config.password = Some(secrecy::SecretString::from("hunter2".to_string()));
        let pg_config = build_config(&config).expect("build config");
        assert!(!format!("{pg_config:?}").contains("hunter2"));
    }
}
