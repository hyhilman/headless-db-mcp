//! Builds a `tokio_postgres::Config` from a `db_headless_core::ConnectionConfig`.
//!
//! Every `SslMode` is implemented for real: `resolve_ssl_mode` here only
//! decides the *wire-protocol negotiation mode* tokio_postgres itself
//! understands (whether SSL is attempted, and whether a refusal falls
//! back to plaintext or fails outright). The actual certificate
//! verification behavior (unverified / chain-only / chain+hostname) lives
//! in `crate::tls`, which builds the real `rustls`-based connector
//! `driver.rs` hands to `tokio_postgres::Config::connect` for every mode
//! except `Disabled`. Guardrail #6 in `db-headless-core` requires a
//! missing `mode` to be treated as `VerifyIdentity`, never silently
//! downgraded to `Disabled` — this resolves it to the same wire mode
//! (`Require`) as `VerifyIdentity`, and `crate::tls::build_connector`
//! independently applies the same default for the verifier it builds.

use db_headless_core::{ConnectionConfig, SslMode};
use secrecy::ExposeSecret;

pub fn build_config(config: &ConnectionConfig) -> tokio_postgres::Config {
    let pg_ssl_mode = resolve_ssl_mode(config);

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

    pg_config
}

fn resolve_ssl_mode(config: &ConnectionConfig) -> tokio_postgres::config::SslMode {
    match config.ssl.mode {
        Some(SslMode::Disabled) => tokio_postgres::config::SslMode::Disable,
        Some(SslMode::Preferred) => tokio_postgres::config::SslMode::Prefer,
        Some(SslMode::Required)
        | Some(SslMode::VerifyCa)
        | Some(SslMode::VerifyIdentity)
        | None => tokio_postgres::config::SslMode::Require,
    }
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
            read_only: false,
            additional_fields: HashMap::new(),
        }
    }

    #[test]
    fn disabled_mode_resolves_to_disable() {
        let config = base_config(SslConfig::disabled());
        let pg_config = build_config(&config);
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
        let pg_config = build_config(&config);
        assert_eq!(
            pg_config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Prefer
        );
    }

    #[test]
    fn missing_mode_resolves_to_require_not_disable() {
        let config = base_config(SslConfig::default());
        let pg_config = build_config(&config);
        assert_eq!(
            pg_config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );
    }

    #[test]
    fn required_verify_ca_and_verify_identity_all_resolve_to_require() {
        for mode in [
            SslMode::Required,
            SslMode::VerifyCa,
            SslMode::VerifyIdentity,
        ] {
            let config = base_config(SslConfig {
                mode: Some(mode),
                ..Default::default()
            });
            let pg_config = build_config(&config);
            assert_eq!(
                pg_config.get_ssl_mode(),
                tokio_postgres::config::SslMode::Require
            );
        }
    }

    #[test]
    fn password_is_never_embedded_in_the_debug_output_of_the_built_config() {
        let mut config = base_config(SslConfig::disabled());
        config.password = Some(secrecy::SecretString::from("hunter2".to_string()));
        let pg_config = build_config(&config);
        assert!(!format!("{pg_config:?}").contains("hunter2"));
    }
}
