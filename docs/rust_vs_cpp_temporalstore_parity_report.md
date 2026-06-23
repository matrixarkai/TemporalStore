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
| Context/benchmarks | LOCOMO and LongMemEval_s full deterministic runs use Rust TemporalStore for ingestion, event storage, retrieval, and replay. Recent Context parity covers first-class `ContextEntityModel`, tree child refs, embeddings, summaries, compression events, node-context query, `ContextSegment` event blocks, source secondary-index routing, L0/L1/L2 prompt injection, and packed LOCOMO source ingestion through bounded Context metadata. Live GPT-4o-mini/OpenAI-compatible reader evidence is still required for VikingMem paper-comparable claims. | `docs/rust_temporalstore_locomo_longmemeval_benchmark_metrics.md`, `docs/benchmark_reproducibility_evidence.md`, `docs/context_benchmark_entity_segment_index_contract.md` |
| Unified tests | The shared corpus has 82 cases. Rust executes the recent Context benchmark-injection, tree/embedding/summary/compression, and temporal-compression cases directly. C++ still has many static surface gates that should become native executable shared cases. | `docs/unified_test_case_inventory.md`, `compat/unified_temporalstore_cases.json` |
| Ops/scale | Local readiness evidence exists, but broad production readiness needs a Docker/AWS multi-service SLO package. | `docs/storage_raft_production_readiness_plan.md`, `docs/aws_existing_eks_deployment.md` |

## Recent Context Parity Pass

The latest Rust Context work closes the recent benchmark/pipeline gaps against the shared C++/Rust
contract:

- Rust now has first-class `ContextEntityModel` storage and commands for extracted entity
  attributes. Benchmark entity blocks still read L0/L1 prompt material from `ContextNodeModel`,
  while `ContextSegment` remains the timestamp-keyed event/segment vocabulary.
- Rust now translates the C++ tree/embedding/summary/compression Context test into executable
  shared cases: child refs, embedding lookup, tree traversal, summary as-of query, compression
  event query, node-context query, and temporal compression that preserves raw source events.
  Shared case ids: `context_tree_embedding_summary_compression` and
  `context_temporal_compression_replayable_summary`.
- `context_benchmark_injection_entity_segment_index` is now a shared corpus case. Rust executes it
  directly: extract a LOCOMO-style turn, write entity/segment/index records, query the source
  secondary index, retrieve L0/L1/L2 blocks, inject them into `<context>`, and verify
  `ContextPackAudit` selected refs.
- Packed LOCOMO full-source replay no longer fails Rust ingestion on oversized packed source
  titles. The harness compacts node metadata to C++/Rust Context validation limits while preserving
  full segment text and source refs for retrieval scoring.

The newer C++ Context code at `/root/src/github-services/TemporalStore` goes further than the
current Rust implementation:

| C++ Context surface | C++ model/function evidence | Rust status |
| --- | --- | --- |
| `ContextChildModel` | model id `14`, `UPSERT_CHILD_REF`, `QUERY_CHILDREN` | Implemented as `ContextChildRef` with duplicate-safe upsert and query. |
| `ContextEmbeddingModel` | model id `15`, `UPSERT_EMBEDDING`, `QUERY_EMBEDDINGS` | Implemented as `ContextEmbedding` with finite-vector validation and query by ref hash. |
| `ContextSummaryModel` | model id `16`, `UPSERT_SUMMARY`, `QUERY_SUMMARIES`, L0/L1 summary retrieval | Implemented as timestamped `ContextSummary` with as-of query and latest-summary node-context lookup. |
| `ContextCompressionModel` | model id `17`, `WRITE_COMPRESSION_EVENT`, `QUERY_COMPRESSION_EVENTS`, `COMPRESS_EVENTS` | Implemented as `ContextCompressionEvent`, direct write/query, and deterministic source-event compression. |
| `ContextEntityModel` | model id `18`, `UPSERT_ENTITY`, `GET_ENTITY`, `QUERY_ENTITIES` | Implemented as Rust `ContextEntity` plus `ContextUpsertEntity`, `ContextGetEntity`, and `ContextQueryEntities`; covered by translated C++ round-trip and validation tests. |
| Integrated node context | `QUERY_NODE_CONTEXT` returns node, latest summary, and cold-window compression summaries | Implemented as `ContextQueryNodeContext`. |
| Extracted event fanout | `WRITE_EXTRACTED_EVENT` writes events plus internal indexes for entity/status/source/time bucket | Rust writes event plus source index in the benchmark workflow, but does not yet expose the full C++ extracted-index fanout. |

So the honest current state is: Rust is benchmark-compatible for recent Context ingestion/retrieval
evidence and shared corpus validation, but it is not yet C++ feature-complete for the newest
Context storage models. The next parity slice should add Rust-native `WRITE_EXTRACTED_EVENT`
fanout for entity/status/source/time-bucket internal indexes and convert more C++ static gates into
native executable shared cases.

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
- Native C++ execution for many shared corpus cases that are currently C++ static surface gates,
  including the recent ContextEntity/ContextSegment benchmark-injection contract.
- Rust implementation of C++ `WRITE_EXTRACTED_EVENT` default/disabled internal-index fanout.
- Continued migration of Rust-local product tests into the shared corpus.
- Any future live ByteStore/S3 requirement, if brought back into scope, needs separate follower-cursor
  and Raft-snapshot retention evidence.
