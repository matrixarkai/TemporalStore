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
| Raft | TemporalRaft production mode is the readiness-eligible path. Local Raft fixtures are test-only. Local harness evidence covers snapshots, membership, failover, follower lag, restart, and secondary reads. | `docs/storage_raft_production_readiness_plan.md`, `docs/distributed_raft_readiness.md` |
| Client/proxy | Rust-native migration contract is HTTP/JSON, RESP, and tonic. Topology sync, retry budgets, route invalidation, quarantine, admission, and aliases are tracked. | `docs/client_vs_cpp.md`, `docs/client_sdk_contract.md` |
| Data-node/metaserver | Rust has lifecycle, scheduler, heartbeat, topology, membership, and readiness evidence, but global production claims still depend on real deployment-scale evidence. | `docs/data_node_vs_cpp.md`, `docs/metaserver_vs_cpp.md` |
| Ingestion | Shared cases cover Kafka offsets, rebalance/backpressure, Flink checkpoints, dead letters, lag metrics, and restart idempotence. C++ execution still needs broader native shared-runner coverage. | `docs/unified_test_case_inventory.md` |
| Context/benchmarks | LOCOMO and LongMemEval_s full deterministic runs use Rust TemporalStore for ingestion, event storage, retrieval, and replay. Recent Context parity covers first-class `ContextEntityModel`, tree child refs, embeddings, summaries, compression events, node-context query, `ContextSegment` event blocks, source secondary-index routing, L0/L1/L2 prompt injection, and packed LOCOMO source ingestion through bounded Context metadata. Live GPT-4o-mini/OpenAI-compatible reader evidence is still required for VikingMem paper-comparable claims. | `docs/rust_temporalstore_locomo_longmemeval_benchmark_metrics.md`, `docs/benchmark_reproducibility_evidence.md`, `docs/context_benchmark_entity_segment_index_contract.md` |
| Cross-subsystem parity | `cross_storage_control_agent_parity` ties storage dump/load/cache recovery, client/proxy topology refresh and admission policy, data-node lifecycle barriers, metaserver scheduler tokens, and Context agent resource/skill parser workflow evidence into one Rust-executable/C++-static shared contract. | `docs/cross_storage_control_agent_parity.md`, `docs/unified_test_case_inventory.md` |
| Unified tests | The shared corpus has 82 cases. Rust executes the recent Context benchmark-injection, tree/embedding/summary/compression, temporal-compression, and cross storage/control/agent parity cases directly. C++ still has many static surface gates that should become native executable shared cases. | `docs/unified_test_case_inventory.md`, `compat/unified_temporalstore_cases.json` |
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
- `context_extracted_event_default_index_fanout` now translates the C++
  `WRITE_EXTRACTED_EVENT` debug tests: Rust writes the event and fans out default internal
  `event_kind`, `entity`, `status`, `source`, and `event_time_bucket` indexes, while disabled
  indexes do not return query refs.
- Recent C++ Context optimization changes are now represented in Rust behavior:
  timestamp-keyed `ContextEvent` records carry `event_time_key` and parent-aware
  `context_event_key` for node and segment parents; `ContextEmbedding` carries compact
  `model_hash` metadata; Rust context nodes/child refs stay compact by storing tenant scope in
  command/object keys rather than a bulky `scope` payload. Covered by
  `context_events_segments_entities_child_refs` and
  `context_cpp_wire_model_descriptor_roundtrip`.
- `context_resource_skill_parser_openviking_parity` covers the OpenViking/C++ resource parser
  gap: Rust parses markdown/text resources and `SKILL.md` front matter into stable source-ref
  chunks with embedding refs, persists chunk embeddings, then feeds those chunks through Rust
  context ingestion/extraction/retrieval.
- Packed LOCOMO full-source replay no longer fails Rust ingestion on oversized packed source
  titles. The harness compacts node metadata to C++/Rust Context validation limits while preserving
  full segment text and source refs for retrieval scoring.

The newer C++ Context code at `<cpp-temporalstore-checkout>` is tracked against these
Rust behavior-parity surfaces:

| C++ Context surface | C++ model/function evidence | Rust status |
| --- | --- | --- |
| `ContextChildModel` | model id `14`, `UPSERT_CHILD_REF`, `QUERY_CHILDREN` | Implemented as `ContextChildRef` with duplicate-safe upsert and query. |
| `ContextEmbeddingModel` | model id `15`, `UPSERT_EMBEDDING`, `QUERY_EMBEDDINGS` | Implemented as `ContextEmbedding` with finite-vector validation and query by ref hash. |
| `ContextSummaryModel` | model id `16`, `UPSERT_SUMMARY`, `QUERY_SUMMARIES`, L0/L1 summary retrieval | Implemented as timestamped `ContextSummary` with as-of query and latest-summary node-context lookup. |
| `ContextCompressionModel` | model id `17`, `WRITE_COMPRESSION_EVENT`, `QUERY_COMPRESSION_EVENTS`, `COMPRESS_EVENTS` | Implemented as `ContextCompressionEvent`, direct write/query, and deterministic source-event compression. |
| `ContextEntityModel` | model id `18`, `UPSERT_ENTITY`, `GET_ENTITY`, `QUERY_ENTITIES` | Implemented as Rust `ContextEntity` plus `ContextUpsertEntity`, `ContextGetEntity`, and `ContextQueryEntities`; covered by translated C++ round-trip and validation tests. |
| Integrated node context | `QUERY_NODE_CONTEXT` returns node, latest summary, and cold-window compression summaries | Implemented as `ContextQueryNodeContext`. |
| Extracted event fanout | `WRITE_EXTRACTED_EVENT` writes events plus internal indexes for entity/status/source/time bucket | Implemented as Rust `ContextWriteExtractedEvent` with default `event_kind`, `entity`, `status`, `source`, and `event_time_bucket` fanout plus disabled-index support. |
| Timestamp-keyed event storage | recent C++ changesets `dbab966b`/`e86e26eb` add parent-aware `context_event_key` and `event_time_key` | Implemented in Rust `ContextEvent` and normalized on `ContextWriteEvent` / `ContextWriteExtractedEvent`; node-parent and segment-parent cases are asserted in `context_models_match_cpp_keys_timeline_pages_and_filters`. |
| Compact embedding metadata | C++ changeset `df54f71d` replaces repeated embedding model strings with `model_hash` when possible | Implemented in Rust `ContextEmbedding` and resource/context embedding producers; C++ wire round-trip preserves `model_hash`. |

So the honest current state is: Rust is benchmark-compatible for recent Context ingestion/retrieval
evidence and shared corpus validation, and the newest C++ Context storage/debug models now have
Rust-native equivalents through `ContextWriteExtractedEvent`, entity, child, embedding, summary,
compression, and node-context commands. Remaining parity work is mostly broader executable C++
runner coverage, live-reader benchmark evidence, and deployment-scale production gates.

## Readiness Evidence Fields

Each major blocker must map to a concrete evidence field before it can be treated as ready:

| Service/area | Required evidence |
| --- | --- |
| `raft_replication` | TemporalRaft process rollout, multi-process log-store validation, snapshots, membership changes, leader transfer, failover, restart recovery, follower lag, and secondary reads. |
| `storage_cache` | Slot dump/load, manifest rejection, recovery faults, follower-safe GC, cache refill, local/shared-store replay, and C++ migration-corpus replay into Rust-native storage. |
| `client` / `proxy` | Rust-native migration contract, typed table client, topology sync, retry budgets, route invalidation, route quarantine/recovery, admission policy, RESP aliases, HTTP/JSON aliases, and tonic contract evidence. |
| `data_node` | Lifecycle persistence, load/reload/unload barriers, readonly catch-up, metaserver-owned membership execution, and restart evidence. |
| `metaserver` | Networked Raft mutation path evidence, scheduler execution, durable task/retry replay, stale token/generation rejection, and data-node membership coupling. |
| `benchmarks` | Dataset hash, full Rust TemporalStore replay, reader mode/provider/model, category breakdown, p50/p95 latencies, token reduction, per-query rows, and report paths. |
| `scale_testing` | Real multi-process Docker/AWS SLO evidence with metaserver, proxy, client, data-node, Raft failover, storage/cache pressure, proxy convergence, workload replay, and resource collectors. |

The cross-subsystem readiness guard is the shared `cross_storage_control_agent_parity` case. It is
the explicit evidence join for storage/cache, client/proxy, data-node, metaserver, and Context agent
workflow parity: storage dump/load/cache recovery must agree with client/proxy topology and
admission behavior; data-node lifecycle barriers must agree with metaserver scheduler tokens; and
the Context agent resource/skill parser must feed Rust ingestion, extraction, secondary indexing,
and retrieval through the same shared case. C++ still treats this as a static surface gate until the
native shared runner executes the case.

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
- Continued migration of Rust-local product tests into the shared corpus.
- Any future live ByteStore/S3 requirement, if brought back into scope, needs separate follower-cursor
  and Raft-snapshot retention evidence.
