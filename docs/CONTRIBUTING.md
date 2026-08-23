# Contributing

Thanks for improving TemporalStore Rust.

## How to contribute

This repository is open for contributions — you do **not** need write access.

1. **Fork** `matrixarkai/TemporalStore` to your own account.
2. Create a branch: `git checkout -b my-change`.
3. Make your change and run the checks in *Development Setup* below.
4. Push to your fork and **open a Pull Request** against `main`.
5. A maintainer (**@bjmeetsfo** or **@superhaiou**) reviews and approves before it is merged.

Every PR requires at least one maintainer approval (enforced by branch protection + CODEOWNERS). Fork-and-PR is the normal path for everyone, including first-time contributors.


## Development Setup

Install a stable Rust toolchain and run:

```bash
cargo check -p temporalstore-rust --all-targets
cargo test -p temporalstore-rust --lib --tests -- --test-threads=1
```

For a faster first pass:

```bash
cargo fmt --all -- --check
python3 tools/validate_open_source_readiness.py
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

## Test Expectations

- Product behavior should be represented in
  `compat/unified_temporalstore_cases.json`.
- New Rust product tests must include `shared-corpus: <case_id>`.
- Rust-only implementation tests must include `rust-internal: <reason>`.
- Keep conformance claims tied to shared corpus cases, harness output, or docs
  that describe the remaining blocker.

## Scope Boundaries

Rust production surfaces are HTTP/JSON, RESP, tonic/gRPC, Rust SDKs, harnesses,
and MatrixArk MCP integration. Do not add brpc or thrift to Rust unless the
project explicitly re-scopes that work.

Rust storage keeps its native page/log format. compatibility is proven by
migration/replay and shared logical reads, not byte-for-byte internal layout.

## Pull Request Checklist

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo check -p temporalstore-rust --all-targets`
- [ ] Relevant unit/integration tests or harnesses
- [ ] `python3 tools/run_temporalstore_unified_tests.py --validate-only`
- [ ] Docs updated for behavior, readiness, or scope changes
- [ ] No generated build output, credentials, local caches, or benchmark dumps
      committed

## Generated And Local Files

Do not commit `target/`, `build-ubuntu22/`, `output/`, `target-rust-bench/`,
`.local/`, local benchmark reports, credentials, or third-party dependency
caches.
