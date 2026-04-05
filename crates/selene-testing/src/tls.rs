use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, IsCa, Issuer, KeyPair, SanType,
};

/// Test certificates for mTLS QUIC connections.
pub struct TestCerts {
    /// DER-encoded CA certificate (for adding to root stores).
    pub ca_cert_der: Vec<u8>,
    /// PEM-encoded CA certificate chain (for writing to disk).
    pub ca_cert_pem: Vec<u8>,
    /// PEM-encoded server certificate chain.
    pub server_cert_chain_pem: Vec<u8>,
    /// PEM-encoded server private key.
    pub server_key_pem: Vec<u8>,
    /// PEM-encoded client certificate chain.
    pub client_cert_chain_pem: Vec<u8>,
    /// PEM-encoded client private key.
    pub client_key_pem: Vec<u8>,
}

/// Generate a full set of test certificates: CA, server, and client.
pub fn generate_test_certs() -> TestCerts {
    let ca_key = KeyPair::generate().expect("generate CA key");
    let mut ca_params = CertificateParams::new(vec![]).expect("CA cert params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Test CA");
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign CA cert");
    let ca_issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().expect("generate server key");
    let mut server_params =
        CertificateParams::new(vec!["localhost".into()]).expect("server cert params");
    server_params.distinguished_name = DistinguishedName::new();
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    server_params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().expect("dns name")),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        SanType::IpAddress(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)),
    ];
    let server_cert = server_params
        .signed_by(&server_key, &ca_issuer)
        .expect("sign server cert");

    let client_key = KeyPair::generate().expect("generate client key");
    let mut client_params = CertificateParams::new(vec![]).expect("client cert params");
    client_params.distinguished_name = DistinguishedName::new();
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "test-client");
    let client_cert = client_params
        .signed_by(&client_key, &ca_issuer)
        .expect("sign client cert");

    TestCerts {
        ca_cert_der: ca_cert.der().to_vec(),
        ca_cert_pem: ca_cert.pem().into_bytes(),
        server_cert_chain_pem: server_cert.pem().into_bytes(),
        server_key_pem: server_key.serialize_pem().into_bytes(),
        client_cert_chain_pem: client_cert.pem().into_bytes(),
        client_key_pem: client_key.serialize_pem().into_bytes(),
    }
}

/// Build a rustls `ServerConfig` for testing (mTLS with test certs).
pub fn server_tls_config(certs: &TestCerts) -> rustls::ServerConfig {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls_pki_types::pem::PemObject;

    let server_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(certs.server_cert_chain_pem.as_slice())
            .collect::<Result<_, _>>()
            .expect("parse server cert PEM");

    let server_key: PrivateKeyDer<'static> =
        PrivateKeyDer::from_pem_slice(certs.server_key_pem.as_slice())
            .expect("parse server key PEM");

    let ca_cert = CertificateDer::from(certs.ca_cert_der.clone());
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(ca_cert).expect("add CA to root store");

    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .expect("build client verifier");

    let mut tls_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(server_certs, server_key)
        .expect("build server TLS config");

    tls_config.alpn_protocols = vec![b"selene/1".to_vec()];
    tls_config
}

/// Build a rustls `ClientConfig` for testing (mTLS with test certs).
pub fn client_tls_config(certs: &TestCerts) -> rustls::ClientConfig {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls_pki_types::pem::PemObject;

    let client_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(certs.client_cert_chain_pem.as_slice())
            .collect::<Result<_, _>>()
            .expect("parse client cert PEM");

    let client_key: PrivateKeyDer<'static> =
        PrivateKeyDer::from_pem_slice(certs.client_key_pem.as_slice())
            .expect("parse client key PEM");

    let ca_cert = CertificateDer::from(certs.ca_cert_der.clone());
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(ca_cert).expect("add CA to root store");

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(client_certs, client_key)
        .expect("build client TLS config")
}
