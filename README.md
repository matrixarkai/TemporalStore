# TemporalStore Rust

TemporalStore Rust is a Rust-native implementation of TemporalStore storage,
context, cache, Raft, control-plane, proxy/client, ingestion, and benchmark
readiness work.

The Rust code is not a brpc/thrift clone of the C++ service. Its public
migration contract is Rust-native APIs, HTTP/JSON, RESP, tonic/gRPC, shared
test corpora, and documented behavioral parity evidence.

Open-source boundary: no brpc/thrift in Rust. Rust production-readiness claims
must be backed by readiness reports, shared corpus runs, or harness evidence.

## Quick Start (single node in Docker)

The fastest way to a working local TemporalStore is one container running the
Rust metaserver + datanode. You need [Docker](docs/INSTALL.md#step-0-install-docker-if-you-dont-have-it)
and a clone of this repo — nothing else installed on the host.

```bash
git clone https://github.com/bjmeetsfo/TemporalStore.git
cd TemporalStore
docker compose -f docker-compose.single-node.yml up --build
```

The first run builds a lean image (Rust toolchain lives inside the build stage,
not on your machine) and starts a node listening on:

- `http://127.0.0.1:17101` — metaserver: cluster metadata and health
- `http://127.0.0.1:17102` — datanode: health, plus writes/reads via `POST /execute`

From another terminal, health-check it and do a write/read round trip:

```bash
curl http://127.0.0.1:17102/health

# write: key "hello" = bytes for "world"
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_set","key":"hello","value":[119,111,114,108,100]}}'

# read it back
curl -sS http://127.0.0.1:17102/execute -H 'content-type: application/json' \
  -d '{"shard_id":1,"command":{"kind":"string_get","key":"hello"}}'
```

Data persists in the `temporalstore-data` Docker volume across restarts. Stop the
node with `Ctrl-C`; remove the node and its data with
`docker compose -f docker-compose.single-node.yml down -v`.

Running on macOS or Windows, or prefer a native (non-Docker) build? The
[Install Guide](docs/INSTALL.md) walks through Docker setup and every supported
path step by step.

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
  - live ByteStore/S3 production integration

Production-readiness claims should be made from passing readiness reports, not
from this README alone. See:

- [Rust vs C++ parity report](docs/rust_vs_cpp_temporalstore_parity_report.md)
- [Benchmark and readiness evidence](docs/benchmark_readiness_evidence_20260629.md)
- [Benchmarks: token & quality vs full local replay (3-arm)](docs/benchmarks/README.md)
- [Storage/Raft readiness plan](docs/storage_raft_production_readiness_plan.md)
- [Context Management on TemporalStore](docs/context_management_on_temporalstore.md)
- [Context Management technical blog](docs/blog_context_management_temporalstore.md)
- [Control State technical blog](docs/blog_control_state_frequency_caps.md)
- [Feature sequences and aggregates technical blog](docs/blog_feature_sequences_and_aggregates.md)
- [Windows Docker installation manual](docs/windows_docker_install.md)
- [Linux build and deploy manual](docs/linux_deploy.md)
- [macOS build and deploy manual](docs/macos_deploy.md)
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
