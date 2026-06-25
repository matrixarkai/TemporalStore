# Unified Test And Scale Validation - 2026-06-25

This note records the current Rust-side unified C++/Rust test and local scale validation pass.
It is intentionally scoped to local evidence: the shared corpus, parity evidence validators,
context workflow, and a compact 3-node scale/shared-store run.

## What Is Unified Now

- Shared corpus schema: `temporalstore-unified-cpp-rust-corpus`
- Shared cases: `150`
- Shared steps: `312`
- C++ existing-test surfaces tracked: `185`
- Rust shared-corpus marked tests: `47`
- Rust grandfathered product-test backlog: `507`

The active Rust integration target is now:

```bash
cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1
```

The older Rust-local `temporalstore_compat` integration target is retired from the active
unified runners. Product behavior should move into `compat/unified_temporalstore_cases.json`
and be exercised by `unified_temporalstore_corpus`.

## Commands Run

```bash
python3 tools/run_temporalstore_unified_tests.py --validate-only
python3 tools/validate_no_duplicate_tests.py
python3 tools/validate_raft_storage_parity_evidence.py
python3 tools/validate_control_plane_parity_evidence.py
cargo test -p temporalstore-rust --test unified_temporalstore_corpus -- --test-threads=1
cargo run -p temporalstore-rust --bin context_workflow_harness
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-context-workflow-validation \
  --log /tmp/temporalstore-context-workflow-validation.log
cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes 3 \
  --string-ops 12 \
  --hash-ops 4 \
  --sequence-keys 1 \
  --sequence-len 12 \
  --scale-events 1 \
  --failover-every 6 \
  --read-sample-every 3 \
  --compare-shared-store true \
  --shared-store-ops 12 \
  --shared-store-flush-every 4
python3 tools/validate_aws_validation_log.py \
  --job temporalstore-scale-validation \
  --log /tmp/temporalstore-unified-scale-validation.log
```

## Results

| Gate | Result |
| --- | --- |
| Shared corpus schema validation | passed |
| Duplicate/product-test guard | passed |
| Raft/storage parity evidence | passed |
| Control-plane parity evidence | passed |
| Rust shared corpus integration test | passed: `2` tests |
| Context workflow harness | passed |
| Compact scale/shared-store harness | passed |

## Compact Scale Metrics

| Metric | Value |
| --- | ---: |
| Initial nodes | 3 |
| Final nodes | 4 |
| Leader | 2 |
| Commit index | 17 |
| String ops | 12 |
| Hash ops | 4 |
| Sequence rows | 12 |
| Sampled reads | 5 |
| Failovers | 1 |
| Scale events | 1 |
| Replication healthy | true |
| Max Raft replica lag | 0 |
| Write QPS | 7.585899152164212 |
| Read QPS | 2.2311468094600624 |
| Raft write p50/p95/p99 | 44,711 / 68,641 / 68,641 us |
| Replica read p50/p95/p99 | 5,636 / 8,977 / 8,977 us |

Shared-store comparison:

| Metric | Sync | Async |
| --- | ---: | ---: |
| Primary write ops | 12 | 12 |
| Replica read ops | 12 | 3 |
| Primary write p50 | 14,555 us | 15,753 us |
| Storage write p50 | 540 us | 513 us |
| Replica read p50 | 4,463 us | 5,453 us |
| Max lag | 0 | 3 |

The scale SLO report marked these readiness fields true: Docker/AWS SLO evidence, storage
deployment scale SLO, metaserver/proxy/client/data-node process readiness, Raft failover,
storage pressure, cache pressure, proxy convergence, and workload replay. CPU, memory, disk,
and network collectors remain explicitly marked as pending collectors in the local run.

## Readiness Note

The shared readiness corpus now stays honest: `data_node` and `raft_replication` are expected to
fail closed until OpenRaft data-node/metaserver real multi-process rollout evidence is complete.
The unified test verifies those concrete blockers instead of claiming all production gates are
ready.
