//! Resolves a `db_headless_core::ConnectionConfig`'s TLS settings into a
//! scheme (`http`/`https`) and a `reqwest::Client` built accordingly.
//!
//! Unlike `driver-postgres`/`driver-redis` (which negotiate TLS at the
//! wire-protocol level and therefore defer most modes), this driver is
//! just an HTTP(S) client, so `reqwest`'s own TLS handling covers real
//! verified TLS with very little extra code:
//!
//! - `SslMode::Disabled` -> plain `http://`.
//! - `SslMode::Preferred` / `SslMode::Required` / `SslMode::VerifyIdentity`
//!   (and a missing `mode`, which guardrail #6 requires treating as
//!   `VerifyIdentity`) -> `https://` using `reqwest`'s default strict
//!   verification (chain + hostname) against the Mozilla root store
//!   bundled by the `rustls-tls` feature. `Preferred` does not fall back
//!   to plaintext on failure — ClickHouse's HTTP interface has no
//!   in-band upgrade handshake to negotiate that against, so "preferred"
//!   here means "use TLS, verified", the same strict behavior as
//!   `Required`/`VerifyIdentity`.
//! - `SslMode::VerifyCa` -> `https://` with the default (built-in) root
//!   store replaced by a single custom CA loaded from `ssl.ca_path`
//!   (`reqwest::Certificate::from_pem` +
//!   `ClientBuilder::add_root_certificate`, `tls_built_in_root_certs(false)`
//!   so only that CA is trusted). This still verifies the server's
//!   hostname against the certificate, which is stricter than Postgres's
//!   own `verify-ca` (chain only, no hostname check) — reqwest has no
//!   middle ground between "verify everything" and
//!   `danger_accept_invalid_hostnames`, and loosening hostname
//!   verification for a mode named "verify" was not a trade-off worth
//!   making silently.
//!
//! Every mode in `SslMode` is implemented for real; none are deferred.

use std::fs;

use db_headless_core::{ConnectionConfig, DriverError, DriverErrorKind, SslMode};

pub fn resolve_scheme(config: &ConnectionConfig) -> &'static str {
    match config.ssl.mode {
        Some(SslMode::Disabled) => "http",
        _ => "https",
    }
}

pub fn resolve_base_url(config: &ConnectionConfig) -> String {
    format!(
        "{}://{}:{}",
        resolve_scheme(config),
        config.host,
        config.port
    )
}

pub fn build_http_client(config: &ConnectionConfig) -> Result<reqwest::Client, DriverError> {
    let mut builder = reqwest::Client::builder();

    match config.ssl.mode {
        Some(SslMode::Disabled) => {}
        Some(SslMode::VerifyCa) => {
            let ca_path = config.ssl.ca_path.as_ref().ok_or_else(|| {
                DriverError::new(
                    DriverErrorKind::Connection,
                    "ssl.mode = verify_ca requires ssl.ca_path to be set",
                )
            })?;
            let pem = fs::read(ca_path).map_err(|err| {
                DriverError::new(
                    DriverErrorKind::Connection,
                    format!("failed to read ssl.ca_path {}: {err}", ca_path.display()),
                )
            })?;
            let cert = reqwest::Certificate::from_pem(&pem).map_err(|err| {
                DriverError::new(
                    DriverErrorKind::Connection,
                    format!("ssl.ca_path did not contain a valid PEM certificate: {err}"),
                )
            })?;
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(cert);
        }
        _ => {}
    }

    builder.build().map_err(|err| {
        DriverError::new(
            DriverErrorKind::Connection,
            format!("failed to build the ClickHouse HTTP client: {err}"),
        )
    })
}

pub fn resolve_username(config: &ConnectionConfig) -> &str {
    if config.username.is_empty() {
        "default"
    } else {
        &config.username
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
            port: 8123,
            username: String::new(),
            password: None,
            database: None,
            ssl,
            additional_fields: HashMap::new(),
        }
    }

    #[test]
    fn disabled_mode_uses_plain_http() {
        let config = base_config(SslConfig::disabled());
        assert_eq!(resolve_scheme(&config), "http");
        assert_eq!(resolve_base_url(&config), "http://localhost:8123");
    }

    #[test]
    fn missing_mode_defaults_to_https_not_plaintext() {
        let config = base_config(SslConfig::default());
        assert_eq!(resolve_scheme(&config), "https");
    }

    #[test]
    fn required_and_verify_identity_use_https() {
        for mode in [
            SslMode::Required,
            SslMode::VerifyIdentity,
            SslMode::Preferred,
        ] {
            let config = base_config(SslConfig {
                mode: Some(mode),
                ..Default::default()
            });
            assert_eq!(resolve_scheme(&config), "https");
        }
    }

    #[test]
    fn verify_ca_without_ca_path_is_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyCa),
            ..Default::default()
        });
        let err = build_http_client(&config).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("ca_path"));
    }

    #[test]
    fn empty_username_defaults_to_default() {
        let config = base_config(SslConfig::disabled());
        assert_eq!(resolve_username(&config), "default");
    }

    #[test]
    fn non_empty_username_is_used_verbatim() {
        let mut config = base_config(SslConfig::disabled());
        config.username = "alice".to_string();
        assert_eq!(resolve_username(&config), "alice");
    }
}
