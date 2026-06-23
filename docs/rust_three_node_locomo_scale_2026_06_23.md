# Rust Three-Node LOCOMO Scale Validation

Date: 2026-06-23

This run validates the Rust TemporalStore context pipeline and the Rust three-data-node
replication path from a clean Ubuntu-local checkout:

- checkout: `/tmp/temporalstore-rust-scale`
- commit: `3c37e4b`
- branch source: `origin/rust-main`
- LOCOMO input: `/mnt/c/root/matrixark_benchmarks/data/locomo10.json`
- LOCOMO SHA-256: `79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4`

The LOCOMO benchmark used Rust TemporalStore for ingestion, extraction, retrieval, and
full replay. The three-data-node scale and secondary-replication harnesses validate the
Rust Raft/data-node path separately. The repo does not yet have a single runner that
serves LOCOMO queries through three external data-node processes, so this evidence should
be read as combined pipeline plus three-node scale validation, not as a single distributed
LOCOMO service benchmark.

## Combined Gate Rerun

After the first manual run, a repeatable combined gate was added:

```bash
python3 tools/run_three_node_locomo_scale_gate.py \
  --worktree /tmp/temporalstore-rust-scale \
  --input /mnt/c/root/matrixark_benchmarks/data/locomo10.json \
  --out-dir benchmark_reports/three_node_locomo_scale_gate_20260623 \
  --skip-build \
  --nodes 3 \
  --shared-store-flush-every 25 \
  --locomo-timeout-seconds 2400 \
  --report benchmark_reports/three_node_locomo_scale_gate_20260623/combined_gate.json
```

Combined report:

- `/tmp/temporalstore-rust-scale/benchmark_reports/three_node_locomo_scale_gate_20260623/combined_gate.json`
- schema: `temporalstore_three_node_locomo_scale_gate_v1`
- ready: `true`
- blockers: `[]`
- elapsed: `497.17 s`

The combined gate fails closed unless all of the following are true:

- LOCOMO uses Rust TemporalStore and full Rust replay.
- LOCOMO is not a Python-only diagnostic.
- LOCOMO full threshold passes.
- three-data-node scale replication is healthy.
- Raft max replica lag is `0`.
- sync shared-store lag is `0`.
- async shared-store lag is bounded by `flush_every - 1`.
- secondary replication failover, OpenRaft process rollout, multi-process log-store validation,
  restart recovery, and applied-fence validation all pass.

## Commands

Build:

```bash
cd /tmp/temporalstore-rust-scale
cargo build --release -p temporalstore-rust --bins
```

Three-data-node scale run:

```bash
./target/release/scale_harness \
  --nodes 3 \
  --string-ops 1000 \
  --hash-ops 250 \
  --sequence-keys 4 \
  --sequence-len 500 \
  --scale-events 2 \
  --failover-every 250 \
  --read-sample-every 100 \
  --compare-shared-store true \
  --shared-store-ops 1000 \
  --shared-store-flush-every 25 \
  --shared-store-root /tmp/ts-three-node-shared-store \
  > benchmark_reports/three_node_locomo_scale/scale_harness_3nodes.json
```

Three-process secondary replication run:

```bash
./target/release/raft_secondary_replication_harness \
  --root /tmp/ts-three-node-secondary-raft \
  --heartbeat-ms 25 \
  > benchmark_reports/three_node_locomo_scale/raft_secondary_replication_3nodes.json
```

LOCOMO full Rust replay:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --input /mnt/c/root/matrixark_benchmarks/data/locomo10.json \
  --threshold-profile locomo_full \
  --require-rust-temporalstore \
  --require-full-rust-temporalstore-replay \
  --rust-temporalstore-release \
  --rust-temporalstore-max-cases 0 \
  --rust-temporalstore-source-limit 0 \
  --rust-temporalstore-batch-size 16 \
  --rust-temporalstore-source-pack-size 24 \
  --rust-temporalstore-timeout-seconds 2400 \
  --report benchmark_reports/three_node_locomo_scale/locomo_full_rust_report.json \
  --misses benchmark_reports/three_node_locomo_scale/locomo_full_rust_misses.jsonl \
  --rust-temporalstore-jsonl benchmark_reports/three_node_locomo_scale/locomo_full_rust_context.jsonl \
  --rust-temporalstore-report benchmark_reports/three_node_locomo_scale/locomo_full_rust_backend.json
```

## LOCOMO Pipeline Metrics

| Metric | Value |
| --- | ---: |
| Conversations | 10 |
| Cases | 1542 |
| Source records | 9363 |
| Rust TemporalStore backend ready | true |
| Full Rust replay ready | true |
| Python-only diagnostic | false |
| All pipelines use Rust TemporalStore | true |
| Hit@K | 0.9474708171 |
| Reader hit rate | 0.8767833982 |
| MRR | 0.5273212064 |
| Token reduction | 83.7139817097% |
| Retrieval p50 | 21.375 ms |
| Retrieval p95 | 61.242 ms |
| Reader p50 | 4.619 ms |
| Reader p95 | 19.211 ms |
| Retrieval zero-hit queries | 81 |
| Reader zero-hit queries | 190 |
| Threshold passed | true |
| Threshold violations | 0 |
| Reader mode | deterministic |
| Live OSS reader calls | 0 |

Latest combined gate rerun metrics:

| Metric | Value |
| --- | ---: |
| Hit@K | 0.9474708171 |
| Reader hit rate | 0.8767833982 |
| MRR | 0.5273212064 |
| Token reduction | 83.7139817097% |
| Retrieval p95 | 32.080 ms |
| Reader p95 | 12.653 ms |
| Zero-hit queries | 81 |
| Rust source sets ingested | 10 |
| Rust source sets retrieved | 10 |
| Rust retrieved blocks | 186,606 |

Source packing preserved all source text while reducing replay rows:

| Metric | Value |
| --- | ---: |
| Original source rows | 1,475,584 |
| Packed source rows | 62,202 |
| Reduction | 1,413,382 |
| Max original sources per case | 1113 |
| Max packed sources per case | 47 |
| Pack size | 24 |
| Rust ingested source sets | 10 |
| Rust retrieved source sets | 10 |
| Rust retrieved blocks | 186,606 |
| Rust replay elapsed | 306.464 s |

Category breakdown:

| Category | Cases | Hit@K | Reader hit | Answer coverage | Zero-hit queries |
| --- | ---: | ---: | ---: | ---: | ---: |
| category_1 | 282 | 0.9397163121 | 0.8652482270 | 0.8473053892 | 17 |
| category_2 | 321 | 0.9626168224 | 0.8442367601 | 0.8563218391 | 12 |
| category_3 | 96 | 0.9479166667 | 0.7187500000 | 0.7209302326 | 5 |
| category_4 | 841 | 0.9441141498 | 0.9108204518 | 0.9096109840 | 47 |
| category_5 | 2 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 0 |

## Three-Data-Node Scale Metrics

| Metric | Value |
| --- | ---: |
| Initial nodes | 3 |
| Final voters | 2, 3, 4 |
| String ops | 1000 |
| Hash ops | 250 |
| Sequence rows | 2000 |
| Sampled reads | 14 |
| Scale events | 2 |
| Failovers | 3 |
| Commit index | 1258 |
| Elapsed | 143.547 s |
| Write throughput | 8.7358 ops/s |
| Max Raft replica lag | 0 |
| Replication healthy | true |

Latest combined gate rerun:

| Metric | Value |
| --- | ---: |
| Final voters | 2, 3, 4 |
| Commit index | 1258 |
| Failovers | 3 |
| Scale events | 2 |
| Write throughput | 9.0100 ops/s |
| Max Raft replica lag | 0 |
| Replication healthy | true |

Raft latency:

| Metric | p50 | p95 | p99 | Max | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| Write | 45.725 ms | 60.685 ms | 70.587 ms | 286.877 ms | 1254 |
| Replica read | 4.382 ms | 12.037 ms | 12.037 ms | 12.457 ms | 14 |

Latest combined gate rerun latency:

| Metric | p50 | p95 | p99 | Max | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| Write | 41.473 ms | 55.843 ms | 60.049 ms | 284.159 ms | 1254 |
| Replica read | 3.948 ms | 11.988 ms | 11.988 ms | 13.130 ms | 14 |

The SLO report marked the local multi-node scale evidence ready with:

- `storage_deployment_scale_slo_ready=true`
- `raft_failover_ready=true`
- `replication_healthy=true`
- `max_replica_lag=0`
- `error_budget_remaining_percent=100.0`
- `scale_events=2`
- `failovers=3`

## Shared-Store And Secondary Replication

The scale harness compared sync and async shared-store paths:

| Metric | Sync | Async |
| --- | ---: | ---: |
| Ops | 1000 | 1000 |
| Primary write p50 | 14.125 ms | 13.844 ms |
| Primary write p95 | 19.134 ms | 18.711 ms |
| Storage write/enqueue p50 | 0.280 ms | 0.001 ms |
| Storage write/enqueue p95 | 0.451 ms | 0.001 ms |
| Replica read p50 | 3.379 ms | 3.371 ms |
| Replica read p95 | 4.427 ms | 4.114 ms |
| Max lag | 0 | 24 |
| Async flush every | n/a | 25 |
| Async flush p95 | n/a | 5.526 ms |

The async max lag of 24 is expected with `--shared-store-flush-every 25`; it remained bounded
below the flush interval and did not block the LOCOMO Rust pipeline or the three-data-node
scale harness.

Latest combined gate rerun shared-store metrics:

| Metric | Sync | Async |
| --- | ---: | ---: |
| Primary write p50 | 14.611 ms | 14.161 ms |
| Primary write p95 | 18.628 ms | 18.168 ms |
| Replica read p50 | 3.435 ms | 3.345 ms |
| Replica read p95 | 4.418 ms | 3.809 ms |
| Max lag | 0 | 24 |
| Lag bound | 0 | 24 |

The dedicated secondary replication harness passed these checks:

- secondary node restart and catch-up
- lagging follower observed lag of 3 and caught up all three writes
- membership scale down to voters `[1, 2]`
- membership scale up to voters `[1, 2, 3]`
- partition rejected isolated stale read, then healed and read the committed value
- leader failover returned `ok`
- OpenRaft process rollout report returned `ready=true`
- multi-process log-store validation returned `true`
- restart recovery, snapshot install, leader transfer, and applied fence validation returned `true`

Final surviving nodes after the deliberate leader crash were nodes `2` and `3`, both with
`commit_index=15`, `applied_index=15`, and validated WAL directories under
`/tmp/ts-three-node-secondary-raft`.

## Raw Reports

Raw reports were written in the Ubuntu-local checkout:

- `/tmp/temporalstore-rust-scale/benchmark_reports/three_node_locomo_scale/scale_harness_3nodes.json`
- `/tmp/temporalstore-rust-scale/benchmark_reports/three_node_locomo_scale/raft_secondary_replication_3nodes.json`
- `/tmp/temporalstore-rust-scale/benchmark_reports/three_node_locomo_scale/locomo_full_rust_report.json`
- `/tmp/temporalstore-rust-scale/benchmark_reports/three_node_locomo_scale/locomo_full_rust_backend.json`
- `/tmp/temporalstore-rust-scale/benchmark_reports/three_node_locomo_scale/locomo_full_rust_misses.jsonl`

## Result

The run is good for current Rust context-pipeline and three-data-node scale evidence:

- LOCOMO ingestion, extraction, retrieval, and replay used Rust TemporalStore and passed the
  full deterministic LOCOMO gate.
- Three Rust data-node scale validation passed with Raft max replica lag `0`.
- Secondary replication fault coverage passed; the intentionally lagging follower caught up.
- Async shared-store replication lag was bounded and did not impact the pipeline evidence.

Remaining gap: a single end-to-end LOCOMO runner that routes the benchmark queries through three
external data-node processes is still not present. Current evidence combines full Rust LOCOMO
pipeline replay with separate three-node Rust Raft/data-node scale validation.
