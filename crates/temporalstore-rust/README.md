# TemporalStore Rust

Rust-native TemporalStore library surfaces for the storage engine, client/proxy
contracts, context pipeline, ingestion, and production Raft readiness.

This crate is designed as an open-source Rust implementation path. It is not a
legacy wire-compatible clone: brpc and Thrift are intentionally out of scope.
The production migration contract is Rust APIs plus HTTP/JSON, RESP, tonic/gRPC,
and executable shared conformance test cases.

## Raft Library Contract

TemporalStore consumes the pinned MatrixRaft library through the default
`temporal-raft-engine` feature. The public production Raft path is the
TemporalRaft/raft-rs process path for both data-node and metaserver runtimes.
Local in-process Raft models remain available only as test fixtures and cannot
satisfy production readiness.

Important entry points:

- `distributed_raft_readiness()`
- `validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed)`
- `require_production_raft_ready()`
- `matrixraft_parity_report_from_current_readiness()`
- `ProductionRaftRuntimeOptions`
- `ProductionRaftRuntime`
- `ProductionMetaRaftRuntime`
- `TemporalRaftDataNodeProcessRolloutReport`
- `TemporalRaftMetaProcessRolloutReport`
- `MetaOwnedDataRaftMembershipReport`

The no-argument readiness API fails closed unless process rollout evidence is
provided by harnesses. Production evidence must come from spawned data-node and
metaserver processes with independent WAL and snapshot directories, observed
read-index responses, restart recovery, per-node log-store inspection,
membership changes, failover, follower lag, and secondary-read checks.

## What Readiness Means

`production_ready` is an evidence-backed claim, not a feature-name claim. A
passing report must prove:

- process-path validation for data-node and metaserver Raft;
- durable WAL/log-store state and restart recovery;
- storage apply fences for applied Raft index plus storage mutations;
- snapshot build/install/restart behavior;
- read-index and lease-read safety;
- lagging follower read rejection and stale follower write rejection;
- learner catch-up, promotion, leader transfer, and voter removal;
- per-peer progress, WAL first/last index, snapshot state, and admin metrics.

If those fields are missing, readiness reports should keep the service blocked
with concrete missing evidence rather than claiming conformance from local fixtures.

## Quick Validation

From the repository root:

```bash
cargo fmt --all -- --check
cargo check -p temporalstore-rust --lib --bins
cargo test -p temporalstore-rust matrixraft_admin_reports_witness_auto_promote_and_pending_joint_consensus --lib -- --test-threads=1
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_rust_product_test_guard.py
python3 tools/validate_no_duplicate_tests.py
```

## Storage Tuning Conformance

Rust exposes the same public production tuning surface used by benchmark and
deployment profiles. `StorageTuningConfig::from_env()` reads:

- `TS_CONTEXT_PAGE_TARGET_BYTES`: target bytes for packed context timestamp pages.
- `TS_BLOCK_SLAB_TARGET_BYTES`: target local block segment size before rolling.
- `TS_STORAGE_ZONE_SIZE`: storage zone target used by deployment/lifecycle wiring.
- `TS_STREAM_MAX_BLOB_SIZE`: stream blob cap; the block store rolls at the lower
  of this value and `TS_BLOCK_SLAB_TARGET_BYTES`.
- `TS_COMPACTION_WATERMARK_BYTES`: compaction scheduling watermark.
- `TS_COLD_SCAN_NO_CACHE_FILL`: default cold-scan behavior for lifecycle work.
- `TS_PAGE_INDEX_CACHE_BYTES`: page-index cache budget for serving/range lookups.
- `TS_BLOCK_INDEX_CACHE_BYTES`: block-index cache budget for physical address
  lookups.

The block store consumes the segment/blob knobs directly for append rolling, and
context packed pages consume `TS_CONTEXT_PAGE_TARGET_BYTES`. The remaining knobs
are part of the shared lifecycle/cold-scan config surface so and Rust
benchmarks can run with the same named production profile.

launchers consume the same names through `tools/temporalstore_runtime_env.sh`.
The storage-facing subset maps into existing gflags:
`TS_STORAGE_ZONE_SIZE -> --storage_zone_size` and
`TS_STREAM_MAX_BLOB_SIZE -> --stream_max_blob_size`. Existing
`TEMPORALSTORE_STORAGE_ZONE_SIZE` and `TEMPORALSTORE_STREAM_MAX_BLOB_SIZE`
overrides still work for compatibility.

Keep the and Rust surfaces synchronized with:

```bash
python3 tools/validate_storage_tuning_conformance.py
```

For process-path Raft evidence, use the harnesses:

```bash
cargo run -p temporalstore-rust --bin distributed_raft_harness -- --root /tmp/temporalstore-raft/data
cargo run -p temporalstore-rust --bin metaserver_raft_harness -- --root /tmp/temporalstore-raft/meta
cargo run -p temporalstore-rust --bin raft_secondary_replication_harness -- --root /tmp/temporalstore-raft/secondary
```

## Documentation

- `docs/distributed_raft_readiness.md`

Keep benchmark and production-readiness documents strict: deterministic
engineering evidence, live-reader evidence, and paper-comparable evidence are
different labels and should not be collapsed into one claim.
