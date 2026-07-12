# TemporalStore Rust

TemporalStore Rust is a Rust-native implementation of TemporalStore storage,
context, cache, Raft, control-plane, proxy/client, ingestion, and benchmark
readiness work.

The Rust code is not a brpc/thrift clone of the C++ service. Its public
migration contract is Rust-native APIs, HTTP/JSON, RESP, tonic/gRPC, shared
test corpora, and documented behavioral parity evidence.

Open-source boundary: no brpc/thrift in Rust. Rust production-readiness claims
must be backed by readiness reports, shared corpus runs, or harness evidence.

## Current Status

- License: Apache-2.0.
- Primary Rust branch marker: rust-main.
- Rust workspace crates:
  - `crates/temporalstore-rust`
  - `crates/temporalstore-snapshot`
- C++ parity target: behavior, durability, migration corpus, shared tests, and
  operational evidence.
- Explicitly out of scope unless separately re-added:
  - brpc/thrift wire compatibility in Rust
  - byte-for-byte C++ page/log layout compatibility
  - live MatrixObjectStore/S3 production integration

Production-readiness claims should be made from passing readiness reports, not
from this README alone. See:

- [Rust vs C++ parity report](docs/rust_vs_cpp_temporalstore_parity_report.md)
- [Benchmark and readiness evidence](docs/benchmark_readiness_evidence_20260629.md)
- [Storage/Raft readiness plan](docs/storage_raft_production_readiness_plan.md)
- [Unified test inventory](docs/unified_test_case_inventory.md)
- [Rust MatrixArk query/index debug flow](docs/rust_matrixark_query_index_debug_flow.md)

## Build

```bash
cargo check -p temporalstore-rust --all-targets
cargo test -p temporalstore-rust --lib --tests -- --test-threads=1
```

Useful focused harnesses:

```bash
cargo run -p temporalstore-rust --bin readiness_gate -- --service-reports
cargo run -p temporalstore-rust --bin context_workflow_harness
cargo run -p temporalstore-rust --bin storage_modes_harness
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness
```

## Validation

Fast repository checks:

```bash
cargo fmt --all -- --check
python3 tools/validate_open_source_readiness.py
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_no_duplicate_tests.py
python3 tools/validate_rust_product_test_guard.py
```

The workflow template in
[docs/ci/rust-production-readiness.workflow.yml](docs/ci/rust-production-readiness.workflow.yml)
covers format, corpus, readiness, unit/integration, storage, scale, and Raft
checks.

## Codex And MatrixArk MCP

Rust and C++ Codex integration use the same MatrixArk MCP tool surface with
backend selection:

- `temporalstore-direct`: C++ TemporalStore SDK backend
- `temporalstore-rust`: Rust TemporalStore record-log backend

See [Rust/C++ Codex MCP integration](docs/rust_cpp_codex_mcp_integration.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

New product behavior tests should reference a shared corpus case with
`shared-corpus: <case_id>`. Rust-only implementation tests should be marked
`rust-internal: <reason>`.
