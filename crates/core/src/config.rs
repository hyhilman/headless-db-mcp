use std::collections::HashMap;
use std::path::PathBuf;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// TLS/SSL negotiation mode for a database connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SslMode {
    Disabled,
    Preferred,
    Required,
    VerifyCa,
    VerifyIdentity,
}

/// TLS configuration for a database connection.
///
/// Guardrail #6: a driver must treat a missing `mode` as
/// `VerifyIdentity`, not `Disabled` — downgrading verification is an
/// explicit, logged opt-out, never a default.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SslConfig {
    pub mode: Option<SslMode>,
    pub ca_path: Option<PathBuf>,
    pub client_cert_path: Option<PathBuf>,
    pub client_key_path: Option<PathBuf>,
}

impl SslConfig {
    pub fn disabled() -> Self {
        Self {
            mode: Some(SslMode::Disabled),
            ..Default::default()
        }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self.mode, None | Some(SslMode::Disabled))
    }
}

/// Connection parameters passed to `DriverFactory::create_driver`.
///
/// `additional_fields` is how driver-specific knobs (Kerberos realm, IAM
/// role, socket path, read preference, ...) thread through without every
/// new driver forcing a change to this shared struct — the same pattern
/// the source project used to scale to 20+ drivers without a kitchen-sink
/// struct.
///
/// `password` is intentionally excluded from `Serialize`: connection
/// metadata (this struct) and secrets are different lifetimes and
/// different storage tiers (guardrail #2). Callers resolve the password
/// from a `SecretStore` and attach it after deserializing the rest of the
/// config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    #[serde(skip_serializing, default)]
    pub password: Option<SecretString>,
    pub database: Option<String>,
    #[serde(default)]
    pub ssl: SslConfig,
    #[serde(default)]
    pub additional_fields: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_disabled_is_not_enabled() {
        assert!(!SslConfig::disabled().is_enabled());
    }

    #[test]
    fn ssl_missing_mode_is_not_reported_enabled_but_callers_must_not_treat_as_disabled() {
        // `is_enabled()` is conservative (None -> false) so callers doing
        // "do we need to do TLS setup" checks behave safely, but this must
        // never be read as "None means the driver may skip verification" -
        // that decision belongs to each driver's own default, which must
        // default to strict verification per guardrail #6.
        let cfg = SslConfig::default();
        assert!(!cfg.is_enabled());
        assert!(cfg.mode.is_none());
    }

    #[test]
    fn connection_config_password_is_not_serialized() {
        let cfg = ConnectionConfig {
            host: "localhost".into(),
            port: 5432,
            username: "postgres".into(),
            password: Some(SecretString::from("hunter2".to_string())),
            database: None,
            ssl: SslConfig::default(),
            additional_fields: HashMap::new(),
        };
        let json = serde_json::to_string(&cfg).expect("serialize");
        assert!(!json.contains("hunter2"));
        assert!(!json.contains("password"));
    }
}
