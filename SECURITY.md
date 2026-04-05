# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | Yes       |
| 0.1.x   | No        |

## Reporting a Vulnerability

If you discover a security vulnerability in Selene, please report it responsibly.
**Do not open a public GitHub issue.**

Email **jscott3201@gmail.com** with:

- A description of the vulnerability
- Steps to reproduce
- The affected component (GQL engine, HTTP server, auth, persistence, etc.)
- Any suggested fix, if you have one

You should receive an acknowledgment within 72 hours. We will work with you to
understand the issue, develop a fix, and coordinate disclosure.

## Scope

The following are in scope for security reports:

- Authentication and authorization bypasses (Cedar policies, credential handling)
- Injection vulnerabilities in the GQL parser or HTTP endpoints
- Memory safety issues
- Vault encryption weaknesses (XChaCha20-Poly1305 envelope encryption)
- WAL or snapshot integrity issues that could lead to data corruption
- TLS configuration weaknesses
- Denial of service through crafted queries or inputs

## Security Features

Selene includes several security mechanisms documented in
[docs/operations/security.md](docs/operations/security.md):

- TLS for QUIC and HTTP transports (rustls, no OpenSSL)
- Cedar policy engine for fine-grained authorization
- Argon2id credential hashing with rate limiting
- Encrypted vault graph (XChaCha20-Poly1305, envelope encryption)
- Distroless Docker image (no shell, no package manager, nonroot UID)
- Container hardening (read-only root FS, dropped capabilities, no-new-privileges)

## Disclosure Policy

We follow coordinated disclosure. Once a fix is available, we will:

1. Release a patched version
2. Publish a security advisory on GitHub
3. Credit the reporter (unless they prefer anonymity)
