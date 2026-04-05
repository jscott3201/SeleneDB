# syntax=docker/dockerfile:1
# ── Stage 1: Build ──────────────────────────────────────────────────────
# Uses the official Rust image on Alpine for musl-static builds.
# The resulting binary has zero runtime dependencies.

# TODO: pin by digest in CI (e.g., rust:alpine@sha256:<digest>)
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev pkgconfig

WORKDIR /build

# Compile-time feature flags (default: all).
#
# Most services are always compiled in and toggled at runtime via config
# or environment variables (SELENE_PROFILE, SELENE_VECTOR_ENABLED, etc.).
# Features that gate actual dependencies:
#   vector         - candle (BERT embedding inference)
#   search         - tantivy (full-text BM25 indexes)
#   cloud-storage  - object_store (S3/GCS/Azure cloud offload)
#   rdf            - oxrdf/oxttl (RDF import/export)
#   rdf-sparql     - spareval (SPARQL query, implies rdf)
#
# The `federation` flag is retained for compatibility but compiles
# unconditionally.
#
# To build a minimal image without search/vector:
#   docker build --build-arg FEATURES=federation .
ARG FEATURES=federation,vector,search,cloud-storage,rdf,rdf-sparql

COPY . .

# BuildKit cache mounts: cargo registry, git index, and target directory
# persist across builds. No fragile dummy-file dependency caching.
# The cp at the end extracts binaries — cache mounts are not in the layer.
# Server and CLI are built separately because --features only applies to -p target.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release -p selene-server --features "${FEATURES}" \
    && cargo build --release -p selene-cli \
    && mkdir -p /tmp/out \
    && cp target/release/selene-server target/release/selene /tmp/out/

# ── Stage 2: Runtime ────────────────────────────────────────────────────
# Distroless static image: CA certs, tzdata, nonroot user (UID 65532).
# No shell, no package manager, near-zero CVE surface.
# The Rust binary is statically linked (musl) and runs as PID 1.

# TODO: pin by digest in CI (e.g., gcr.io/distroless/static@sha256:<digest>)
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /tmp/out/selene-server /selene-server
COPY --from=builder /tmp/out/selene /selene

VOLUME /data
WORKDIR /data

# Default ports:
#   4510 — QUIC (UDP)
#   8080 — HTTP (TCP)
EXPOSE 4510/udp
EXPOSE 8080/tcp

# ── Runtime Configuration ───────────────────────────────────────────────
# Services are toggled at runtime via TOML config or environment variables.
# Key environment variables:
#   SELENE_PROFILE          — edge | cloud | standalone (default: edge)
#   SELENE_DEV_MODE         — true to enable dev mode (no auth, self-signed TLS)
#   SELENE_VECTOR_ENABLED   — true/false (default: profile-dependent)
#   SELENE_SEARCH_ENABLED   — true/false (default: profile-dependent)
#   SELENE_TEMPORAL_ENABLED — true/false (default: true)
#   SELENE_MCP_ENABLED      — true/false (default: false in production)
#   SELENE_MEMORY_BUDGET_MB — memory budget in MB (default: profile-dependent)
#   RUST_LOG                — log level (default: selene_server=info)

ENTRYPOINT ["/selene-server"]

# Default: start the server with /data as the data directory.
# When overriding CMD (e.g., --dev), include /data explicitly:
#   docker run selene:latest --dev /data
CMD ["/data"]
