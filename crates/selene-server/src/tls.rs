//! TLS configuration for QUIC.
//!
//! Dev mode: self-signed certificate, no client auth.
//! Production: load cert/key from disk, optional mTLS with CA verification.

use std::sync::Arc;

use quinn::ServerConfig;
#[cfg(feature = "dev-tls")]
use rustls::pki_types::PrivatePkcs8KeyDer;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pki_types::pem::PemObject;

use crate::config::TlsConfig;

/// Create a QUIC `ServerConfig` for dev mode with a self-signed certificate.
#[cfg(feature = "dev-tls")]
pub fn dev_server_config() -> anyhow::Result<(ServerConfig, Vec<CertificateDer<'static>>)> {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert_der = CertificateDer::from(certified.cert.der().to_vec());
    let key_der = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::Pkcs8(key_der))?;

    tls_config.alpn_protocols = vec![b"selene/1".to_vec()];

    let server_config = build_quinn_config(tls_config);
    Ok((server_config, vec![cert_der]))
}

/// Create a QUIC `ServerConfig` for production with certificates from disk.
///
/// If `tls_config.ca_cert_path` is set, mTLS is enabled: clients must
/// present a certificate signed by the specified CA.
pub fn prod_server_config(
    tls_config: &TlsConfig,
) -> anyhow::Result<(ServerConfig, Vec<CertificateDer<'static>>)> {
    // Load server certificate chain
    let cert_pem = std::fs::read(&tls_config.cert_path).map_err(|e| {
        anyhow::anyhow!(
            "failed to read cert {}: {e}",
            tls_config.cert_path.display()
        )
    })?;
    let server_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(cert_pem.as_slice())
            .collect::<Result<_, _>>()
            .map_err(|e| anyhow::anyhow!("failed to parse cert PEM: {e}"))?;

    if server_certs.is_empty() {
        anyhow::bail!(
            "no certificates found in {}",
            tls_config.cert_path.display()
        );
    }

    // Load server private key
    let key_pem = std::fs::read(&tls_config.key_path).map_err(|e| {
        anyhow::anyhow!("failed to read key {}: {e}", tls_config.key_path.display())
    })?;
    let server_key: PrivateKeyDer<'static> = PrivateKeyDer::from_pem_slice(key_pem.as_slice())
        .map_err(|e| anyhow::anyhow!("failed to parse key PEM: {e}"))?;

    // Build TLS config — with or without mTLS
    let rustls_config = if let Some(ca_path) = &tls_config.ca_cert_path {
        // mTLS: require client certificates signed by this CA
        let ca_pem = std::fs::read(ca_path)
            .map_err(|e| anyhow::anyhow!("failed to read CA cert {}: {e}", ca_path.display()))?;
        let ca_certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(ca_pem.as_slice())
                .collect::<Result<_, _>>()
                .map_err(|e| anyhow::anyhow!("failed to parse CA cert PEM: {e}"))?;

        let mut root_store = rustls::RootCertStore::empty();
        for ca_cert in ca_certs {
            root_store.add(ca_cert)?;
        }

        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build client verifier: {e}"))?;

        let mut config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(server_certs.clone(), server_key)?;

        config.alpn_protocols = vec![b"selene/1".to_vec()];

        tracing::info!(
            cert = %tls_config.cert_path.display(),
            ca = %ca_path.display(),
            "production TLS with mTLS enabled"
        );

        config
    } else {
        // No mTLS — just server cert verification
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(server_certs.clone(), server_key)?;

        config.alpn_protocols = vec![b"selene/1".to_vec()];

        tracing::info!(
            cert = %tls_config.cert_path.display(),
            "production TLS without mTLS"
        );

        config
    };

    let server_config = build_quinn_config(rustls_config);
    Ok((server_config, server_certs))
}

/// Build a QUIC ServerConfig from a rustls config with standard transport settings.
fn build_quinn_config(tls_config: rustls::ServerConfig) -> ServerConfig {
    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config).expect("valid TLS config"),
    ));

    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(256u32.into());
    transport.max_concurrent_uni_streams(64u32.into());
    server_config.transport_config(Arc::new(transport));

    server_config
}
