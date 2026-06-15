# Rust Test LOC and CI Status

Measured on `rust-main` on June 12, 2026 using tracked Rust files plus the
current working changes.

## Test-specific LOC

- Rust source files: 40
- Raw Rust LOC: 47,034
- Test-specific Rust LOC: 23,063 across 30 files
- Test validation scripts: 524 LOC across 7 files

Breakdown:

| Area | Files | LOC |
| --- | ---: | ---: |
| `#[cfg(test)]` Rust modules | 22 | 18,267 |
| Harness binaries | 6 | 3,466 |
| `tests/` integration tests | 2 | 1,330 |

Largest test-specific files:

| LOC | Area | File |
| ---: | --- | --- |
| 3,872 | `#[cfg(test)]` | `crates/temporalstore-rust/src/raft.rs` |
| 3,140 | `#[cfg(test)]` | `crates/temporalstore-rust/src/engine.rs` |
| 2,937 | `#[cfg(test)]` | `crates/temporalstore-rust/src/client.rs` |
| 1,556 | `#[cfg(test)]` | `crates/temporalstore-rust/src/data_node.rs` |
| 1,318 | harness binary | `crates/temporalstore-rust/src/bin/raft_secondary_replication_harness.rs` |
| 1,256 | `#[cfg(test)]` | `crates/temporalstore-rust/src/bin/server.rs` |
| 1,180 | integration test | `crates/temporalstore-rust/tests/temporalstore_compat.rs` |

## CI/CD Status

No tracked GitHub Actions, GitLab CI, CircleCI, Jenkins, Buildkite, or Azure
Pipeline config is present in this repo at this revision. The repo does have
local validation and deployment scripts:

- `tools/run_temporalstore_parity_gate.sh`
- `tools/run_temporalstore_cpp_like_tests.sh`
- `tools/run_temporalstore_scale_harness.sh`
- `tools/deploy_and_test_aws_existing_eks.sh`
- `tools/validate_aws_existing_eks.sh`
- `tools/scale_test_aws_existing_eks.sh`
- `tools/validate_aws_validation_log.py`

A GitHub Actions workflow was prepared locally for `rust-main`, but pushing a
workflow file requires an OAuth token with GitHub `workflow` scope. The current
push token rejected `.github/workflows/rust-ci.yml`, so the pushable change keeps
the Rust test enhancement and this CI status record.

Recommended CI gate once workflow-scope credentials are available:

```bash
cargo fmt --all -- --check
cargo check -p temporalstore-rust --all-targets
cargo run -p temporalstore-rust --bin readiness_gate
cargo test -p temporalstore-rust --lib --tests -- --test-threads=1
for run in 1 2 3; do
  cargo test -p temporalstore-rust tiny_memory_cache --lib -- --test-threads=1
  cargo test -p temporalstore-rust restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache --lib -- --test-threads=1
done
TS_SCALE_PROFILE=debug \
TS_SCALE_NODES=3 \
TS_SCALE_STRING_OPS=12 \
TS_SCALE_HASH_OPS=3 \
TS_SCALE_SEQUENCE_KEYS=1 \
TS_SCALE_SEQUENCE_LEN=8 \
TS_SCALE_EVENTS=1 \
TS_SCALE_COMPARE_SHARED_STORE=true \
TS_SCALE_SHARED_STORE_OPS=12 \
TS_SCALE_SHARED_STORE_FLUSH_EVERY=4 \
  tools/run_temporalstore_scale_harness.sh | tee /tmp/temporalstore-scale-ci.log
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-scale-validation \
  --log /tmp/temporalstore-scale-ci.log
```

The readiness gate JSON now includes `blocker_count`, `failed_areas`, and
`failed_capabilities[]` entries with exact area/capability text. The CLI also prints the first
failed capabilities to stderr before exiting non-zero, so CI logs identify the remaining production
gaps without requiring a separate JSON parser.
