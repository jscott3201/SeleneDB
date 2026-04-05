//! Client configuration.

use std::net::SocketAddr;
use std::path::PathBuf;

/// Auth credentials for the handshake.
#[derive(Clone)]
pub struct AuthCredentials {
    /// Auth type: "dev", "token", "psk".
    pub auth_type: String,
    /// Principal identity (username) for lookup.
    pub identity: String,
    /// Secret credential for verification.
    pub credentials: String,
}

impl std::fmt::Debug for AuthCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthCredentials")
            .field("auth_type", &self.auth_type)
            .field("identity", &self.identity)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

/// Configuration for connecting to a Selene server.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server address (default: 127.0.0.1:4510).
    pub server_addr: SocketAddr,
    /// Server name for TLS verification (default: "localhost").
    pub server_name: String,
    /// Skip TLS certificate verification (dev mode only).
    pub insecure: bool,
    /// TLS settings for production connections.
    pub tls: Option<ClientTlsConfig>,
    /// Auth credentials. If set, handshake is performed automatically on connect.
    pub auth: Option<AuthCredentials>,
}

/// Client TLS configuration for verified/mTLS connections.
#[derive(Debug, Clone)]
pub struct ClientTlsConfig {
    /// Path to PEM-encoded CA certificate for server verification.
    pub ca_cert_path: PathBuf,
    /// Path to PEM-encoded client certificate (for mTLS).
    pub cert_path: Option<PathBuf>,
    /// Path to PEM-encoded client private key (for mTLS).
    pub key_path: Option<PathBuf>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:4510".parse().unwrap(),
            server_name: "localhost".into(),
            insecure: true,
            tls: None,
            auth: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let cfg = ClientConfig::default();
        assert_eq!(
            cfg.server_addr,
            "127.0.0.1:4510".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(cfg.server_name, "localhost");
        assert!(cfg.insecure);
        assert!(cfg.tls.is_none());
        assert!(cfg.auth.is_none());
    }

    #[test]
    fn default_config_server_addr_is_ipv4_loopback_port_4510() {
        let cfg = ClientConfig::default();
        assert!(cfg.server_addr.ip().is_loopback());
        assert_eq!(cfg.server_addr.port(), 4510);
        assert!(cfg.server_addr.is_ipv4());
    }

    #[test]
    fn auth_credentials_debug_redacts_credentials() {
        let creds = AuthCredentials {
            auth_type: "token".into(),
            identity: "admin".into(),
            credentials: "super-secret-key-12345".into(),
        };
        let debug_output = format!("{creds:?}");
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED], got: {debug_output}"
        );
        assert!(
            !debug_output.contains("super-secret-key-12345"),
            "Debug output must not leak the credential value"
        );
    }

    #[test]
    fn auth_credentials_debug_shows_type_and_identity() {
        let creds = AuthCredentials {
            auth_type: "psk".into(),
            identity: "sensor-gateway".into(),
            credentials: "secret".into(),
        };
        let debug_output = format!("{creds:?}");
        assert!(
            debug_output.contains("psk"),
            "Debug output should include auth_type"
        );
        assert!(
            debug_output.contains("sensor-gateway"),
            "Debug output should include identity"
        );
    }

    #[test]
    fn auth_credentials_clone_is_independent() {
        let original = AuthCredentials {
            auth_type: "dev".into(),
            identity: "user1".into(),
            credentials: "pass1".into(),
        };
        let mut cloned = original.clone();
        cloned.auth_type = "token".into();
        cloned.identity = "user2".into();
        cloned.credentials = "pass2".into();

        // Original is unchanged.
        assert_eq!(original.auth_type, "dev");
        assert_eq!(original.identity, "user1");
        assert_eq!(original.credentials, "pass1");
    }

    #[test]
    fn client_config_clone_is_independent() {
        let original = ClientConfig {
            server_addr: "10.0.0.1:9999".parse().unwrap(),
            server_name: "prod-server".into(),
            insecure: false,
            tls: Some(ClientTlsConfig {
                ca_cert_path: "/tmp/ca.pem".into(),
                cert_path: None,
                key_path: None,
            }),
            auth: Some(AuthCredentials {
                auth_type: "token".into(),
                identity: "admin".into(),
                credentials: "secret".into(),
            }),
        };
        let mut cloned = original.clone();
        cloned.server_name = "other-server".into();
        cloned.insecure = true;

        assert_eq!(original.server_name, "prod-server");
        assert!(!original.insecure);
    }

    #[test]
    fn client_tls_config_stores_ca_only() {
        let tls = ClientTlsConfig {
            ca_cert_path: "/etc/selene/ca.pem".into(),
            cert_path: None,
            key_path: None,
        };
        assert_eq!(tls.ca_cert_path, PathBuf::from("/etc/selene/ca.pem"));
        assert!(tls.cert_path.is_none());
        assert!(tls.key_path.is_none());
    }

    #[test]
    fn client_tls_config_stores_mtls_paths() {
        let tls = ClientTlsConfig {
            ca_cert_path: "/certs/ca.pem".into(),
            cert_path: Some("/certs/client.pem".into()),
            key_path: Some("/certs/client-key.pem".into()),
        };
        assert_eq!(tls.ca_cert_path, PathBuf::from("/certs/ca.pem"));
        assert_eq!(
            tls.cert_path.as_deref(),
            Some(std::path::Path::new("/certs/client.pem"))
        );
        assert_eq!(
            tls.key_path.as_deref(),
            Some(std::path::Path::new("/certs/client-key.pem"))
        );
    }
}
