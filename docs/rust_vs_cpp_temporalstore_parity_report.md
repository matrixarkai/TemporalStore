# Rust Vs C++ TemporalStore Parity Report

Report date: 2026-06-22

## Summary

Rust TemporalStore is a Rust-native production path, not a legacy C++ wire-compatible clone. The
current parity target is behavioral compatibility, durability, storage/Raft safety, unified shared
tests, and benchmark evidence versus C++ TemporalStore.

The implementation decision remains explicit:

- no brpc or Thrift in Rust
- no byte-for-byte C++ page/log layout requirement
- Rust-native HTTP/JSON, RESP, and tonic are the production migration surfaces
- Rust-native page/log formats are accepted when migration/replay corpus evidence passes
- live ByteStore/S3 remains out of scope unless separately reintroduced

## Current Evidence Snapshot

| Area | Current status | Evidence |
| --- | --- | --- |
| Storage/cache | Local/shared-store parity evidence exists for dump/load, recovery, corrupt pages, follower-safe GC, cache refill, shared-store sync/async replay, and Rust-native migration corpus. | `docs/storage_raft_production_readiness_plan.md`, `compat/unified_temporalstore_cases.json` |
| Raft | OpenRaft production mode is the readiness-eligible path. Local Raft fixtures are test-only. Local harness evidence covers snapshots, membership, failover, follower lag, restart, and secondary reads. | `docs/storage_raft_production_readiness_plan.md`, `docs/distributed_raft_readiness.md` |
| Client/proxy | Rust-native migration contract is HTTP/JSON, RESP, and tonic. Topology sync, retry budgets, route invalidation, quarantine, admission, and aliases are tracked. | `docs/client_vs_cpp.md`, `docs/client_sdk_contract.md` |
| Data-node/metaserver | Rust has lifecycle, scheduler, heartbeat, topology, membership, and readiness evidence, but global production claims still depend on real deployment-scale evidence. | `docs/data_node_vs_cpp.md`, `docs/metaserver_vs_cpp.md` |
| Ingestion | Shared cases cover Kafka offsets, rebalance/backpressure, Flink checkpoints, dead letters, lag metrics, and restart idempotence. C++ execution still needs broader native shared-runner coverage. | `docs/unified_test_case_inventory.md` |
| Context/benchmarks | LOCOMO and LongMemEval_s full deterministic runs use Rust TemporalStore for ingestion, event storage, retrieval, and replay. Live GPT-4o-mini/OpenAI-compatible reader evidence is still required for VikingMem paper-comparable claims. | `docs/rust_temporalstore_locomo_longmemeval_benchmark_metrics.md`, `docs/benchmark_reproducibility_evidence.md` |
| Unified tests | The shared corpus has 79 cases and 166 steps. Rust executes 26 shared behavior cases. C++ still has many static surface gates that should become native executable shared cases. | `docs/unified_test_case_inventory.md`, `compat/unified_temporalstore_cases.json` |
| Ops/scale | Local readiness evidence exists, but broad production readiness needs a Docker/AWS multi-service SLO package. | `docs/storage_raft_production_readiness_plan.md`, `docs/aws_existing_eks_deployment.md` |

## Readiness Evidence Fields

Each major blocker must map to a concrete evidence field before it can be treated as ready:

| Service/area | Required evidence |
| --- | --- |
| `raft_replication` | OpenRaft process rollout, multi-process log-store validation, snapshots, membership changes, leader transfer, failover, restart recovery, follower lag, and secondary reads. |
| `storage_cache` | Slot dump/load, manifest rejection, recovery faults, follower-safe GC, cache refill, local/shared-store replay, and C++ migration-corpus replay into Rust-native storage. |
| `client` / `proxy` | Rust-native migration contract, typed table client, topology sync, retry budgets, route invalidation, route quarantine/recovery, admission policy, RESP aliases, HTTP/JSON aliases, and tonic contract evidence. |
| `data_node` | Lifecycle persistence, load/reload/unload barriers, readonly catch-up, metaserver-owned membership execution, and restart evidence. |
| `metaserver` | Networked Raft mutation path evidence, scheduler execution, durable task/retry replay, stale token/generation rejection, and data-node membership coupling. |
| `benchmarks` | Dataset hash, full Rust TemporalStore replay, reader mode/provider/model, category breakdown, p50/p95 latencies, token reduction, per-query rows, and report paths. |
| `scale_testing` | Real multi-process Docker/AWS SLO evidence with metaserver, proxy, client, data-node, Raft failover, storage/cache pressure, proxy convergence, workload replay, and resource collectors. |

## Unified Test Migration Backlog

The shared corpus is the canonical location for product behavior. Rust-local and C++-local tests
should remain only for implementation internals.

Current migration focus:

1. Promote remaining Rust-local product tests into shared corpus cases or mark them as Rust-only
   internals when they only validate local helpers.
2. Convert C++ static parity gates into executable shared cases family by family:
   storage/Raft, control plane, ingestion, Context, Redis/admin, Feature, IPS, and Risk.
3. Keep C++ local tests for C++ transport, fixture, allocator, build, and ByteRaft integration
   mechanics that are not cross-language product contracts.
4. Keep Rust local tests for parser/helper/provider mock mechanics that are not TemporalStore
   product behavior.

## Benchmark Claim Levels

Benchmark claims must stay separated:

| Claim level | Requirements |
| --- | --- |
| Deterministic engineering evidence | Real dataset, deterministic reader, Rust TemporalStore backend, full replay when required, threshold profile pass. |
| Live-reader evidence | Deterministic requirements plus OpenAI-compatible reader endpoint, real GPT-4o-mini or configured model calls, no fallback, and `reader_open_source_calls > 0`. |
| VikingMem paper-comparable evidence | Live-reader evidence plus archived provider/model/prompt metadata, full Rust replay, per-category results, p50/p95, token reduction, and `paper_comparable_claim_ready=true`. |

The current LOCOMO and LongMemEval_s full deterministic reports are accepted engineering evidence.
They are not VikingMem paper-comparable until a live reader endpoint run succeeds.

## Remaining Gaps

- Broader Docker/AWS multi-service SLO evidence for global production readiness.
- Live GPT-4o-mini/OpenAI-compatible reader evidence for VikingMem-comparable benchmark claims.
- Native C++ execution for many shared corpus cases that are currently C++ static surface gates.
- Continued migration of Rust-local product tests into the shared corpus.
- Any future live ByteStore/S3 requirement, if brought back into scope, needs separate follower-cursor
  and Raft-snapshot retention evidence.
