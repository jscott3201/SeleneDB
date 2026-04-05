# Contributing to Selene

Thank you for your interest in contributing to Selene. This document explains how to
get involved, from reporting bugs to submitting code changes.

## Reporting Issues

**Bugs:** Open a [bug report](https://github.com/jscott3201/SeleneDB/issues/new?template=bug_report.md)
with a minimal reproduction case. Include your Selene version, platform, and the GQL
query or HTTP request that triggered the issue.

**Feature requests:** Open a [feature request](https://github.com/jscott3201/SeleneDB/issues/new?template=feature_request.md)
describing the use case, not just the desired API. Understanding *why* helps us design
the right solution.

**Security vulnerabilities:** Do not open a public issue. See [SECURITY.md](SECURITY.md)
for responsible disclosure instructions.

## Development Setup

Selene requires Rust 1.94+ and has zero C/C++ dependencies.

```bash
git clone https://github.com/jscott3201/SeleneDB.git
cd SeleneDB
cargo test --workspace --all-features   # run all ~2,600 tests
cargo run -p selene-server -- --dev --seed /tmp/selene-data
```

Optional feature flags (compile-time, all opt-in):

| Flag | What it enables |
|------|-----------------|
| `federation` | Peer-to-peer graph federation over QUIC |
| `vector` | Embedding inference via candle BERT |
| `search` | Full-text BM25 search via tantivy |
| `cloud-storage` | S3/GCS/Azure cold-tier offload |
| `rdf` | RDF import/export (Turtle, N-Triples) |
| `rdf-sparql` | SPARQL query support (implies `rdf`) |
| `dev-tls` | Self-signed TLS certificates for dev mode |

## Pull Request Process

1. **Fork and branch** from `main`. Use a descriptive branch name
   (`fix/wal-recovery-timestamp`, `feat/graph-slice-pagination`).

2. **Write tests first.** Selene enforces test coverage for new functionality. Add
   tests in the same crate as your change. For GQL features, add tests in
   `selene-gql`; for server behavior, add tests in `selene-server`.

3. **Run the full CI suite locally** before pushing:

   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-features --all-targets -- -D warnings
   cargo test --workspace --all-features
   cargo doc --workspace --all-features --no-deps
   ```

4. **Keep commits focused.** One logical change per commit. Use conventional commit
   messages: `fix:`, `feat:`, `refactor:`, `docs:`, `chore:`, `perf:`, `bench:`.

5. **Open a PR** against `main`. Fill out the PR template. Link any related issues.

6. **Respond to review.** All PRs require review before merge.

## Coding Standards

Selene enforces these automatically through CI:

- **Formatting:** `cargo fmt` (rustfmt.toml: edition 2024, 100-char lines, 4-space indent)
- **Linting:** `cargo clippy -- -D warnings` (zero warnings policy)
- **Documentation:** `cargo doc` must produce zero warnings
- **License:** `cargo deny check` for dependency license compliance

Additional conventions:

- **GQL is the sole query interface.** All transports (HTTP, QUIC, MCP) route through
  GQL. Do not add transport-specific query paths.
- **Ops layer pattern.** Business logic lives in `selene-server/src/ops/`. Transports
  are thin adapters. New features go in ops, not in transport handlers.
- **`pub(crate)` by default.** Only expose what the public API requires. Internal
  modules use `pub(crate)` with explicit top-level re-exports.
- **No unsafe.** 10 crates enforce `#![forbid(unsafe_code)]`; the remaining 2
  use `#![deny(unsafe_code)]` with targeted allows for candle mmap and test-only
  env var removal. Keep it that way unless there is a compelling, documented reason.
- **Errors, not panics.** Use `Result` for fallible operations. Reserve `panic!` and
  `unwrap()` for true invariant violations, never for input validation.

## Architecture Overview

Selene is a workspace of 13 crates. Before contributing, read the
[Architecture](docs/internals/architecture.md) doc to understand crate boundaries
and data flow.

Key design decisions:

- **In-memory graph** with dense `Vec<Option<Node>>` storage and ArcSwap for
  lock-free reads
- **Multi-tier time-series** (hot/warm/cold/cloud) with schema-driven encoding hints
- **Plan cache** for parsed GQL with generation-based invalidation
- **13 optimizer rules** including selectivity-aware predicate reordering and WCO join detection
- **persist_or_die** WAL policy (retry 3x then abort, SQLite philosophy)

## Testing

- `cargo test --workspace --all-features` runs all tests including feature-gated code
- `cargo bench -p <crate>` runs benchmarks (run crates sequentially, not in parallel)

The `selene-testing` crate provides test factories for nodes, edges, schemas, and a
scalable reference building model for deterministic benchmarking.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE-MIT) OR [Apache License 2.0](LICENSE-APACHE), at the
user's option.
