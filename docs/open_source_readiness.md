# Open Source Readiness

Last validated: 2026-06-23

This page records the repository-level open-source readiness contract for the
Rust TemporalStore worktree.

## Required Public Files

- `README.md`
- `crates/temporalstore-rust/README.md`
- `LICENSE`
- `NOTICE`
- `CONTRIBUTING.md`
- `SECURITY.md`
- `CODE_OF_CONDUCT.md`
- `.gitignore`

CI is tracked in `.github/workflows/matrixark-context-backfill-ci.yml`. The
`MatrixArk Context Backfill CI` workflow runs the Ubuntu 22 readiness gate and
uploads `matrixark_context_backfill_readiness.json` plus
`matrixark_context_backfill_evidence/` so reviewers can verify checksums,
Prometheus text output, and nested dual-write evidence. Publishing workflow
changes still requires a GitHub credential with `workflow` scope.

## Scope Statement

Rust TemporalStore is open-source ready as a Rust-native implementation path.
The Rust repo should not imply that it is a brpc/thrift wire-compatible clone or
a byte-for-byte C++ storage-layout clone.

The Rust crate README and crate-level rustdoc now make the Raft library contract
explicit: TemporalRaft/raft-rs is the production process path, local Raft models
are test-only fixtures, and production readiness requires spawned process
evidence with independent WAL/snapshot stores.

Current Rust compatibility positioning:

- C++ behavior parity is tracked through shared tests, migration corpus, harness
  reports, and readiness docs.
- Rust public surfaces are Rust APIs, HTTP/JSON, RESP, tonic/gRPC, Codex MCP,
  and harness contracts.
- brpc/thrift compatibility remains explicitly out of scope for Rust.
- live ByteStore/S3 production integration remains explicitly out of scope until
  re-scoped and security-reviewed.

## Validation

Run:

```bash
python3 tools/validate_open_source_readiness.py
tools/run_matrixark_context_backfill_ci_gate_ubuntu22.sh
cargo fmt --all -- --check
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

The open-source readiness validator checks required files, license alignment,
scope language, generated-output ignore rules, absence of tracked local Codex
hook config, absence of personal absolute checkout paths, the backfill CI shell
gate, and the GitHub Actions workflow that publishes readiness evidence.

## Release Hygiene

Before publishing or accepting external contributions:

- keep secrets out of git
- keep generated build outputs out of git
- keep third-party dependency caches out of git
- keep `.codex/` local-only; publish hook templates in docs instead of tracked
  machine-specific hook files
- use placeholders such as `<repo-root>` and `<cpp-temporalstore-checkout>` in
  docs instead of absolute local paths
- keep production-readiness claims tied to evidence
- update readiness docs when scope changes
