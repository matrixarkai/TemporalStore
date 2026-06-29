# Benchmark And Readiness Evidence - 2026-06-29

This page is the clean evidence index for the current Rust TemporalStore benchmark and readiness posture. It intentionally separates deterministic engineering evidence from live-reader and paper-comparable claims.

## Claim Labels

| Label | Meaning | Current status |
| --- | --- | --- |
| Deterministic engineering evidence | Full Rust TemporalStore backend replay with deterministic reader/scorer and archived threshold fields. | Available for LOCOMO and LongMemEval_s. |
| Live-reader evidence | Full Rust replay plus a configured OpenAI-compatible reader endpoint and `reader_open_source_calls > 0`. | Not available in the committed evidence below. |
| Paper-comparable evidence | Real dataset, live reader, full Rust replay, provider/model/prompt metadata, category breakdown, and passing thresholds in one archive. | Not claimed. |

## Benchmark Evidence

Source summaries:

- [`locomo_rust_backend_readiness_20260626_summary.json`](benchmark_archives/locomo_rust_backend_readiness_20260626_summary.json)
- [`longmemeval_s_rust_backend_readiness_20260626_summary.json`](benchmark_archives/longmemeval_s_rust_backend_readiness_20260626_summary.json)

| Dataset | Evidence label | Cases | Hit@K | Reader hit | Token reduction | Retrieval p95 | Full Rust replay | Paper-comparable |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| LOCOMO | deterministic engineering evidence | 1542 | 94.75% | 87.68% | 83.71% | 34.30 ms | `true` | `false` |
| LongMemEval_s | deterministic engineering evidence | 500 | 100.00% | 99.60% | 81.39% | 24.74 ms | `true` | `false` |

Both summaries report:

- `all_pipelines_use_rust_temporalstore=true`
- `python_only_diagnostic=false`
- `rust_temporalstore_backend_ready=true`
- `rust_temporalstore_context_event_ingest_ready=true`
- `rust_temporalstore_direct_source_scoring=false`

The blocker for paper-comparable benchmark claims is explicit in both summaries: `reader_open_source_calls=0` and `reader_mode_effective=deterministic`.

## Readiness Evidence

Command run on 2026-06-29:

```bash
cargo run -p temporalstore-rust --bin readiness_gate -- --service-reports
```

Machine-readable snapshot:

- [`service_reports_20260629.json`](readiness/service_reports_20260629.json)

Result: `9` ready services, `3` blocked services. The command correctly failed closed while emitting service reports.

| Service | Status | Severity | Blockers | Primary blocker capability | Evidence field |
| --- | --- | --- | ---: | --- | --- |
| `client` | `ready` | `ready` | `0` | ready | `none` |
| `proxy` | `ready` | `ready` | `0` | ready | `none` |
| `ingestion` | `ready` | `ready` | `0` | ready | `none` |
| `data_node` | `blocked` | `critical` | `3` | provide passing TemporalRaft data-node multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API writes, real log-store validation, snapshot install, restart recovery, crash-window recovery after storage mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, and RustRaft-derived operational semantics evidence | `raft_rollout.temporal_raft_data_node_process_rollout_ready` |
| `metaserver` | `ready` | `ready` | `0` | ready | `none` |
| `storage_cache` | `blocked` | `warning` | `2` | mtcache-class async writeback and backpressure | `storage_cache_mtcache.async_writeback_backpressure_ready` |
| `feature_modules` | `ready` | `ready` | `0` | ready | `none` |
| `context_workflow` | `ready` | `ready` | `0` | ready | `none` |
| `fault_tolerance` | `ready` | `ready` | `0` | ready | `none` |
| `deployment_ops` | `ready` | `ready` | `0` | ready | `none` |
| `scale_testing` | `ready` | `ready` | `0` | ready | `none` |
| `raft_replication` | `blocked` | `critical` | `3` | provide passing TemporalRaft data-node multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API writes, real log-store validation, snapshot install, restart recovery, crash-window recovery after storage mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, and RustRaft-derived operational semantics evidence | `raft_rollout.temporal_raft_data_node_process_rollout_ready` |

## Remaining Blockers

| Service | Owner | Severity | Primary blocker capability | Evidence field | Next action |
| --- | --- | --- | --- | --- | --- |
| `data_node` | `data_node_runtime` | `critical` | provide passing TemporalRaft data-node multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API writes, real log-store validation, snapshot install, restart recovery, crash-window recovery after storage mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, and RustRaft-derived operational semantics evidence | `raft_rollout.temporal_raft_data_node_process_rollout_ready` | finish metaserver-driven membership against real data-node Raft groups |
| `storage_cache` | `storage_runtime` | `warning` | mtcache-class async writeback and backpressure | `storage_cache_mtcache.async_writeback_backpressure_ready` | finish mtcache-class async writeback/backpressure and mature latency metrics |
| `raft_replication` | `consensus_runtime` | `critical` | provide passing TemporalRaft data-node multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API writes, real log-store validation, snapshot install, restart recovery, crash-window recovery after storage mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, and RustRaft-derived operational semantics evidence | `raft_rollout.temporal_raft_data_node_process_rollout_ready` | finish durable real-process TemporalRaft rollout, production mTLS transport, and external chaos coverage |

## What This Evidence Supports

- Rust TemporalStore benchmark replay is valid deterministic engineering evidence for the archived LOCOMO and LongMemEval_s summaries.
- The readiness gate is strict: it does not claim production readiness while data-node, storage-cache, and Raft process-path evidence remain blocked.
- The current evidence is suitable for regression tracking, shared C++/Rust corpus validation, and readiness triage.

## What This Evidence Does Not Claim

- No VikingMem paper-comparable score is claimed from deterministic-reader reports.
- No live GPT-4o-mini/OpenAI-compatible reader result is claimed without archived model calls.
- No full production readiness claim is made while `readiness_gate --service-reports` reports blocked services.
