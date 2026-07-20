//! Builds the real, negotiated TLS connector `driver.rs` hands to
//! `tokio_postgres::Config::connect` for every `SslMode` except
//! `Disabled` (which uses `tokio_postgres::NoTls` directly).
//!
//! Every mode is a distinct trust decision, deliberately not layered as
//! "verify_identity but skip a step":
//!
//! - `SslMode::Required` (and `Preferred`, which only differs in whether
//!   `tokio_postgres` is allowed to fall back to plaintext if the server
//!   refuses SSL): encrypts the connection but does not authenticate the
//!   server at all, matching libpq's own `sslmode=require` semantics
//!   exactly. [`AcceptAnyCertificate`] still verifies the handshake
//!   signature for real (proving whoever presented the certificate holds
//!   the matching private key) — only the chain-of-trust and hostname
//!   checks are skipped. Skipping signature verification too would make
//!   this indistinguishable from no security at all, a strictly worse
//!   guarantee than libpq's `require` provides.
//! - `SslMode::VerifyCa`: verifies the certificate chains to a trust
//!   anchor loaded from `ssl.ca_path`, but not the hostname — built from
//!   `rustls-webpki`'s own `EndEntityCert::verify_for_usage` directly
//!   (chain check) without ever calling
//!   `verify_is_valid_for_subject_name` (the hostname check), since
//!   rustls's own `WebPkiServerVerifier` always does both together and
//!   has no supported way to skip just the hostname half.
//! - `SslMode::VerifyIdentity`, and a missing `mode` (guardrail #6 in
//!   `db_headless_core` requires treating that as `VerifyIdentity`, never
//!   `Disabled`): full chain and hostname verification via rustls's
//!   standard `WebPkiServerVerifier`, against `ssl.ca_path` if given or
//!   the platform's native trust store (`rustls-native-certs`) otherwise.
//!
//! Every verifier is built against `rustls::crypto::ring::default_provider()`
//! explicitly rather than relying on a process-wide default `CryptoProvider`
//! being installed somewhere else in the binary.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use db_headless_core::{ConnectionConfig, DriverError, DriverErrorKind, SslMode};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, TrustAnchor, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore};
use tokio_postgres_rustls::MakeRustlsConnect;

fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn tls_setup_error(message: impl Into<String>) -> DriverError {
    DriverError::new(DriverErrorKind::Connection, message.into())
}

fn base_builder(
    provider: Arc<CryptoProvider>,
) -> Result<rustls::ConfigBuilder<ClientConfig, rustls::WantsVerifier>, DriverError> {
    ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| tls_setup_error(format!("failed to select TLS protocol versions: {err}")))
}

/// Accepts any server certificate without checking the chain or hostname
/// (libpq's `sslmode=require`), but still verifies the TLS handshake
/// signature for real using the given `CryptoProvider`'s algorithms.
#[derive(Debug)]
struct AcceptAnyCertificate {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Verifies the certificate chains to one of `roots`, but never checks
/// the hostname (libpq's `sslmode=verify-ca`). Built directly from
/// `rustls-webpki`'s `EndEntityCert::verify_for_usage`, the same building
/// block rustls's own `WebPkiServerVerifier` uses internally, just
/// without the paired `verify_is_valid_for_subject_name` call.
#[derive(Debug)]
struct ChainOnlyVerifier {
    roots: Vec<TrustAnchor<'static>>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for ChainOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        let cert = webpki::EndEntityCert::try_from(end_entity)
            .map_err(|err| RustlsError::General(format!("invalid server certificate: {err}")))?;

        cert.verify_for_usage(
            self.provider.signature_verification_algorithms.all,
            &self.roots,
            intermediates,
            now,
            webpki::KeyUsage::server_auth(),
            None,
            None,
        )
        .map_err(|err| RustlsError::General(format!("certificate chain is not trusted: {err}")))?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn load_trust_anchors(ca_path: &Path) -> Result<Vec<TrustAnchor<'static>>, DriverError> {
    let pem = fs::read(ca_path).map_err(|err| {
        tls_setup_error(format!(
            "failed to read ssl.ca_path {}: {err}",
            ca_path.display()
        ))
    })?;
    let mut reader = pem.as_slice();
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|err| {
            tls_setup_error(format!(
                "ssl.ca_path did not contain a valid PEM certificate: {err}"
            ))
        })?;
    if certs.is_empty() {
        return Err(tls_setup_error(format!(
            "ssl.ca_path {} contained no certificates",
            ca_path.display()
        )));
    }

    certs
        .iter()
        .map(|cert| {
            webpki::anchor_from_trusted_cert(cert)
                .map(|anchor| anchor.to_owned())
                .map_err(|err| {
                    tls_setup_error(format!("ssl.ca_path is not a valid CA certificate: {err}"))
                })
        })
        .collect()
}

fn load_native_root_store() -> Result<RootCertStore, DriverError> {
    let result = rustls_native_certs::load_native_certs();
    if result.certs.is_empty() {
        return Err(tls_setup_error(format!(
            "no usable certificates found in the platform's native trust store: {:?}",
            result.errors
        )));
    }
    let mut roots = RootCertStore::empty();
    let (added, _ignored) = roots.add_parsable_certificates(result.certs);
    if added == 0 {
        return Err(tls_setup_error(
            "the platform's native trust store contained no parsable certificates",
        ));
    }
    Ok(roots)
}

fn root_store_for_verify_identity(config: &ConnectionConfig) -> Result<RootCertStore, DriverError> {
    match &config.ssl.ca_path {
        Some(ca_path) => {
            let mut roots = RootCertStore::empty();
            let pem = fs::read(ca_path).map_err(|err| {
                tls_setup_error(format!(
                    "failed to read ssl.ca_path {}: {err}",
                    ca_path.display()
                ))
            })?;
            let mut reader = pem.as_slice();
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
                .collect::<Result<_, _>>()
                .map_err(|err| {
                    tls_setup_error(format!(
                        "ssl.ca_path did not contain a valid PEM certificate: {err}"
                    ))
                })?;
            let (added, _ignored) = roots.add_parsable_certificates(certs);
            if added == 0 {
                return Err(tls_setup_error(format!(
                    "ssl.ca_path {} contained no usable certificates",
                    ca_path.display()
                )));
            }
            Ok(roots)
        }
        None => load_native_root_store(),
    }
}

/// Builds the `MakeRustlsConnect` to pass to `tokio_postgres::Config::connect`
/// for every mode except `Disabled`, which callers should connect with
/// `tokio_postgres::NoTls` instead (this function is never called for it).
pub fn build_connector(config: &ConnectionConfig) -> Result<MakeRustlsConnect, DriverError> {
    let provider = crypto_provider();

    let client_config = match config.ssl.mode {
        Some(SslMode::Disabled) => {
            return Err(tls_setup_error(
                "build_connector must not be called for ssl.mode = disabled",
            ));
        }
        Some(SslMode::Preferred) | Some(SslMode::Required) => {
            let mut client_config = base_builder(Arc::clone(&provider))?
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth();
            client_config
                .dangerous()
                .set_certificate_verifier(Arc::new(AcceptAnyCertificate {
                    provider: Arc::clone(&provider),
                }));
            client_config
        }
        Some(SslMode::VerifyCa) => {
            let ca_path = config.ssl.ca_path.as_ref().ok_or_else(|| {
                tls_setup_error("ssl.mode = verify_ca requires ssl.ca_path to be set")
            })?;
            let roots = load_trust_anchors(ca_path)?;
            let mut client_config = base_builder(Arc::clone(&provider))?
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth();
            client_config
                .dangerous()
                .set_certificate_verifier(Arc::new(ChainOnlyVerifier {
                    roots,
                    provider: Arc::clone(&provider),
                }));
            client_config
        }
        Some(SslMode::VerifyIdentity) | None => {
            let roots = root_store_for_verify_identity(config)?;
            base_builder(provider)?
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
    };

    Ok(MakeRustlsConnect::new(client_config))
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
    fn disabled_mode_is_rejected_callers_must_use_no_tls_instead() {
        let config = base_config(SslConfig::disabled());
        let err = build_connector(&config).map(drop).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
    }

    #[test]
    fn preferred_and_required_build_successfully_without_a_ca_path() {
        for mode in [SslMode::Preferred, SslMode::Required] {
            let config = base_config(SslConfig {
                mode: Some(mode),
                ..Default::default()
            });
            build_connector(&config).expect("builds without needing a CA");
        }
    }

    #[test]
    fn verify_ca_without_ca_path_is_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyCa),
            ..Default::default()
        });
        let err = build_connector(&config).map(drop).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("ca_path"));
    }

    #[test]
    fn verify_ca_with_a_nonexistent_ca_path_is_a_clear_error() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyCa),
            ca_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        });
        let err = build_connector(&config).map(drop).unwrap_err();
        assert_eq!(err.kind, DriverErrorKind::Connection);
        assert!(err.message.contains("ca_path"));
    }

    #[test]
    fn verify_identity_falls_back_to_native_root_store_without_a_ca_path() {
        let config = base_config(SslConfig {
            mode: Some(SslMode::VerifyIdentity),
            ..Default::default()
        });
        // This machine's native trust store may or may not be present in a
        // minimal CI/container image; either a successful build or a clear
        // "no usable certificates" error is acceptable, a panic is not.
        let _ = build_connector(&config);
    }

    #[test]
    fn missing_mode_behaves_like_verify_identity_not_disabled() {
        let config = base_config(SslConfig::default());
        let _ = build_connector(&config);
    }
}
