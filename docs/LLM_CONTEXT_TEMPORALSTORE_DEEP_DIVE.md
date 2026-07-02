# TemporalStore LLM Context Deep Dive

This document consolidates the MatrixArk / TemporalStore context design, serving
schemas, ingestion/query workflows, parity gates, and local validation steps.

## Goal

TemporalStore is the serving-time store for LLM context engineering. MatrixArk sits
above it and owns extraction, canonicalization, model calls, resource parsing, and
context-pack construction. TemporalStore stores compact, queryable serving records:
nodes, events, indexes, embeddings, summaries, compression windows, dirty markers,
and replay audits.

The design is intentionally schema-light for customers:

```text
customer or agent sends raw query/event/resource + optional hints
-> MatrixArk extracts intent, time, filters, entities, and node path
-> TemporalStore stores compact serving records
-> MatrixArk retrieves a token-budgeted context pack
-> final LLM combines local context + MatrixArk context pack
-> feedback/final answer can be ingested as future memory
```

Customers should not define deep schemas in v1. They can send raw text, resource
references, optional team/project/time hints, and optional model-understanding
output if their agent already has it.

## Architecture

```text
AI harness / vertical Cursor / enterprise agent
  | raw query, hints, resources, feedback hooks
  v
MatrixArk context runtime
  | extraction, canonicalization, embedding, filters, token budget, replay
  v
TemporalStore context extension
  | compact serving records over HashModel and FeatureModel page primitives
  v
TemporalStore storage / raft / cache path
```

TemporalStore does not call LLMs or embedding models. MatrixArk or the caller
generates vectors and structured intent. TemporalStore stores normalized vectors
and computes bounded cosine scores during tree traversal.

## Context Record Schemas

The C++ proto lives at `src/extension/context/interface.proto`.

### ContextNode

```text
node_hash            uint64  stable canonical node id
parent_hash          uint64  parent node id, 0 for root
kind                 uint32  caller-defined compact node type
canonical_name       string  compact name such as approvals or incident_77
l0                   string  short traversal summary
last_event_time_ms   uint64  newest event time known for this node
```

Use for tenant/team/project/resource/entity/collection nodes. Keep the number of
nodes low; prefer one strong canonical node over many near-duplicate entity nodes.

### ContextChildRef

```text
parent_hash          uint64
child_hash           uint64
updated_at_ms        uint64
```

Use for tree edges. Names and kinds are derived by reading the child node, so edge
writes stay tiny. `UPSERT_CHILD_REF` deduplicates by `child_hash`, returns
`created=false` for existing edges, and avoids duplicate timeline writes.

### ContextEvent

```text
event_id_hash        uint64
event_time_ms        uint64
type                 uint32
confidence           float   0..1
importance           float   0..1
text                 string  compact serving text
```

Use for extracted memories, business events, confirmations, feedback, and useful
conversation facts. Actor/team/project/status/amount/freshness should live in
bounded indexes, node path, or compact text instead of duplicating fields on every
event. Do not store large raw documents here.

### IndexRef

```text
primary_node_hash        uint64
primary_event_time_ms    uint64
event_id_hash            uint64
```

Use for small secondary indexes such as status, event type, project, approval
person, or incident id. Indexes should be compact and bounded. Serving can
combine filters by intersecting several `ContextIndex` result sets before reading
the referenced event timelines. Keep the indexed fields declared; do not turn
this into arbitrary JSON filtering.

### ContextEmbedding

```text
ref_hash             uint64  node, resource chunk, or summary id
level                uint32  L0/L1/L2 or caller-defined level
vector               float[]
updated_at_ms        uint64
```

Use for L0 node summaries and selected resource chunks. For MVP, vectors are
stored inside TemporalStore. Vector dimension is derived from vector length.
Milvus/S3 can be added later for larger deployments.

### ContextSummary

```text
node_hash            uint64
level                uint32
text                 string
valid_from_ms        uint64
```

Use for async L0/L1 summary refresh. L0 is the required traversal summary. L1 is
optional for larger or high-value nodes. L2 should usually remain as resource
chunks or raw object references, not a giant prompt payload.

Summary embeddings are stored as `ContextEmbedding` records, not inside
`ContextSummary`. The summary worker writes:

```text
UPSERT_SUMMARY(tenant_hash, node_hash, level, text, valid_from_ms)
UPSERT_EMBEDDING(tenant_hash, ref_hash = node_hash, level, summary_vector, updated_at_ms)
```

This keeps summary text versioned by time while keeping the latest traversal
vector cheap to load by node/ref hash.

### ContextCompressionEvent

```text
compression_id_hash  uint64
node_hash            uint64
source_start_ms      uint64
source_end_ms        uint64
compressed_time_ms   uint64
summary              string
```

Use for non-destructive temporal compression of older windows. Fresh events stay
queryable; compressed summaries help old context fit token budgets.
See `docs/CONTEXT_COMPRESSION_WORKFLOW.md` for the detailed write/query/debug
flow.

### SummaryDirtyMarker

```text
node_hash            uint64
event_time_ms        uint64
propagate_depth      uint32
```

Use to queue async summary refresh. Event writes should not synchronously update
all parent summaries.

### ContextPackAudit

```text
query_id             string
session_hash         uint64
request_time_ms      uint64
max_prompt_tokens    uint32
selected_tokens      uint32
selected_refs[]      AuditRef(node_hash, event_time_ms)
```

Use for replay, governance, and debugging why a context pack was returned.

## Storage Mapping

TemporalStore context models reuse existing page primitives.

```text
ContextNodeModel         Hash page object, field = meta
ContextEventModel        Feature page object, key = event_time_ms + hash suffix
ContextIndexModel        Feature page object, key = event_time_ms + hash suffix
ContextAuditModel        Feature page object, key = request_time_ms + hash suffix
ContextDirtyModel        Feature page object, key = event_time_ms + hash suffix
ContextChildModel        Feature page object, key = updated_at_ms + child hash suffix
ContextEmbeddingModel    Hash page object, field = embedding
ContextEntityModel       Hash page object, field = entity
ContextSummaryModel      Feature page object, key = valid_from_ms + summary hash suffix
ContextCompressionModel  Feature page object, key = compressed_time_ms + compression hash suffix
```

Object keys:

```text
ctx:node:{tenant_hash}:{node_hash}
ctx:event:{tenant_hash}:{node_hash}
ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}
ctx:audit:{tenant_hash}:{session_hash}
ctx:dirty:{tenant_hash}:{node_hash}
ctx:child:{tenant_hash}:{parent_hash}
ctx:emb:{tenant_hash}:{ref_hash}
ctx:entity:{tenant_hash}:{node_hash}:{entity_hash}
ctx:summary:{tenant_hash}:{node_hash}:{level}
ctx:compress:{tenant_hash}:{node_hash}
```

Tenant isolation is carried in every object key. Payloads avoid duplicating tenant
and scope identity unless needed at serving time.

## Minimal Record Policy

TemporalStore serving records should carry only the fields needed for bounded
lookup, scoring, filtering, and replay:

```text
ContextNode          identity + parent + type + name + L0 + last event time
ContextChildRef      parent + child + update time only
ContextEvent         id + time + type + confidence + importance + compact text
ContextEmbedding     ref + level + vector + update time only
ContextEntity        entity + node + compact state + validity/confidence + source event ids
ContextSummary       node + level + text + valid_from only
IndexRef             pointer back to the primary event only
ContextPackAudit     query/session/time/token budget + selected refs only
```

`ContextEntity` is the VikingMem-style evolving state record in this MVP. It is
not a directory and it is not a replacement for `ContextEvent`. Events remain the
immutable replayable timeline; entity state is the compact current view that
retrieval can use before spending tokens on raw evidence. MatrixArk updates it
from raw ingest, batch ingest, stream ingest, resource-derived events, and
feedback confirmations when extraction identifies a stable entity.

See `docs/CONTEXT_ENTITY_AND_CHILDREF_WORKFLOW.md` for the concrete extraction
decision tree, API/batch/stream/resource/feedback coverage, and a detailed
`ContextChildRef` traversal example.

Entity retrieval is bounded: MatrixArk first narrows scope through the context
tree and query plan, then asks for known entity hashes or scans the small selected
node set in the runtime layer. TemporalStore does not run arbitrary JSON filters
over all entities.

Useful but non-essential serving attributes should move out of the hot record:

```text
actor/team/project/status/amount -> node path, compact text, or bounded indexes
freshness/validity               -> query time range, BLOCK_IF_STALE operator, or index policy
child name/kind                  -> child node
embedding dimension              -> vector length
summary id/valid_until           -> timeline key and next summary version
resource metadata                -> chunk ref, raw_uri, or optional resource store
```

## Public APIs

### Node And Tree

```text
UPSERT_NODE(tenant_hash, ContextNode) -> object_key
GET_NODE(tenant_hash, node_hash) -> exist, object_key, ContextNode
UPSERT_CHILD_REF(tenant_hash, ContextChildRef) -> object_key, created, parent_child_count
QUERY_CHILDREN(tenant_hash, parent_hash, limit) -> child refs
```

### Event And Index

```text
WRITE_EVENT(tenant_hash, node_hash, ContextEvent, first_write_only) -> object_key
QUERY_EVENTS(tenant_hash, node_hash, time range, limit, freshness, type/confidence/importance filters)
WRITE_INDEX_REF(tenant_hash, index_name, index_value_hash, scope_hash, event_time_ms, IndexRef)
QUERY_INDEX(tenant_hash, index_name, index_value_hash, scope_hash, time range, limit)
```

### Entity State

```text
UPSERT_ENTITY(tenant_hash, ContextEntity) -> object_key
GET_ENTITY(tenant_hash, node_hash, entity_hash) -> entity
QUERY_ENTITIES(tenant_hash, node_hash, entity_hashes, limit) -> entities
```

MatrixArk should use `ContextEntity` when a raw event changes durable current
state, for example `gpu_purchase_request -> approved`, `rollback_incident ->
confirmed`, or `customer_policy -> superseded`. The entity carries only the
compact state and source event ids; detailed evidence stays in `ContextEvent` or
resource chunks.

### Embedding And Traversal

```text
UPSERT_EMBEDDING(tenant_hash, ContextEmbedding) -> object_key
QUERY_EMBEDDINGS(tenant_hash, ref_hashes[], limit) -> embeddings
TRAVERSE_CONTEXT_TREE(tenant_hash, start_node_hash, query_vector, max_depth, top_k_per_depth,
                      max_children_scored_per_parent, max_candidate_nodes, leaf_only)
```

### Summary, Compression, Replay

```text
MARK_SUMMARY_DIRTY(tenant_hash, SummaryDirtyMarker) -> object_key
QUERY_SUMMARY_DIRTY(tenant_hash, node_hash, time range, limit)
UPSERT_SUMMARY(tenant_hash, ContextSummary) -> object_key
QUERY_SUMMARIES(tenant_hash, node_hash, level, as_of_ms, limit)
WRITE_COMPRESSION_EVENT(tenant_hash, ContextCompressionEvent) -> object_key
QUERY_COMPRESSION_EVENTS(tenant_hash, node_hashes[], time range, limit)
WRITE_PACK_AUDIT(tenant_hash, ContextPackAudit) -> object_key
QUERY_PACK_AUDIT(tenant_hash, session_hash, time range, limit)
```

## Ingestion Workflow

### API Ingest

```text
POST /v1/context/ingest
raw_text + tenant + optional hints + idempotency key
-> extract event type/status/time/entity
-> choose canonical node path
-> ensure missing nodes and child refs
-> reject duplicate idempotency keys before writing
-> write ContextEvent
-> write bounded IndexRef records
-> write L0 ContextEmbedding if available
-> mark summary dirty
```

### Streaming Ingest

```text
stream_name + partition + offset + raw_text + hints
-> keep a committed offset checkpoint per stream/partition
-> skip offsets already committed during replay
-> same extraction/canonicalization path as API ingest
-> write compact ContextEvent only once
```

TemporalStore does not need stream offsets in the serving payload. Offsets belong
to the ingestion service. The serving proof still requires replay tests that show
old offsets are skipped and the next offset is accepted.

### Batch Ingest

```text
events[] or resources[]
-> process with the same extraction path
-> keep batch bounded
-> fail item-level validation without expanding TemporalStore schema
```

### Resource Ingest

Resources are stored as parsed serving chunks and references, not raw bytes.

```text
.md/.txt/.pdf/raw_uri
-> parse headings/pages/paragraphs
-> create L0/L1/L2-style chunks
-> generate chunk embeddings
-> write resource node, child refs, embeddings, optional extracted events
-> keep raw bytes behind raw_uri or object storage
```

For TemporalStore-only MVP, raw files can remain local or mounted. For larger
deployments, S3-compatible object storage can hold raw bytes while TemporalStore
keeps source refs and retrieval metadata.

## Query Workflow

```text
raw query + hints + max_prompt_tokens
-> MatrixArk extracts intent, time window, filters, and optional target scope
-> generate query embedding
-> TRAVERSE_CONTEXT_TREE from tenant/team/project root
-> score child L0/L1 embeddings with bounded fanout
-> query selected leaf timelines or secondary indexes
-> apply validity, time, confidence, importance, type filters
-> optionally query resource chunks and compression windows
-> build token-budgeted ContextPack with summary/entity/event/resource refs
-> WRITE_PACK_AUDIT
```

The pack deliberately references three context layers:

```text
summary_refs   L0/L1 node overview selected during traversal
entity_hashes  compact current or evolving state for the selected nodes
event_ids      replayable timestamped facts used as evidence
chunk_hashes   selected L2 resource chunks with citations
```

Summaries are not just background cache records. They become serving-time
overview refs, similar to layered filesystem context, while events remain the
immutable replay source and entities carry the current state.

Default serving limits:

```text
max_depth = 6
top_k_per_depth = 5
max_children_scored_per_parent <= 256
max_candidate_nodes = 24
```

For 9-layer paths, TemporalStore should not scan globally. It should do parent
lookup, bounded child scoring, and query only selected leaf timelines.

## MatrixArk Extraction Responsibilities

MatrixArk should keep customer inputs simple and absorb schema complexity:

```text
general extraction fields:
tenant/team/project hints
time expression and resolved time range
event_type
status or action
actor/entity candidates
confidence
importance
operator/index freshness when stale context must be blocked
canonical node path
small index fields, up to a bounded number
```

If the agent already called an LLM to understand the query, it may pass that
understanding as hints. Otherwise MatrixArk can call its own model provider.

## Context Types

```text
resource        parsed document, runbook, design doc, PDF, markdown, ticket
memory          useful conversation fact or final answer confirmation
business_event  approval, incident, cost update, deployment, purchase, policy change
skill           reusable instruction, playbook, tool preference, coding convention
```

Memory often comes from threads. Business events often come from systems outside
threads. Skills are reusable operational knowledge. Resources are source material
with citations and chunks.

## Summary And Compression Policy

Write path:

```text
event/resource write
-> optional leaf embedding
-> dirty marker
-> return quickly
```

Async worker:

```text
query dirty markers
-> regenerate leaf L0 summary
-> optionally refresh parent L0/L1 summaries
-> upsert ContextSummary
-> upsert ContextEmbedding for the summary vector
-> write compression events for older windows
```

The local E2E validates this for API ingest, batch ingest, stream ingest,
resource-derived extraction, and feedback ingestion by querying both summary
records and summary embedding refs. Retrieval also returns `summary_refs` in the
ContextPack, so the summary layer is validated in the serving path.
It also writes/query compression windows for old approval and incident context,
including source event ids for replay.

Compression:

```text
fresh window       raw ContextEvent records
older hot window   raw events + compression summaries
cold window        prefer summaries, keep raw events by retention policy
```

TemporalStore can serve historical context, but heavy analytics, wide scans, or
long-retention audits should move to MatrixDB or an offline warehouse.

## Unified Test Matrix

The shared corpus is `third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json`.

Important context cases:

```text
context_tree_event_pack_replay
context_raw_extraction_query_pipeline
context_resource_ingestion_chunk_query
context_query_filter_isolation_pipeline
context_incident_time_aware_pipeline
context_resource_event_feedback_loop
context_resource_feedback_second_query_pipeline
context_pack_token_budget_parity
context_layered_resource_parsing_pipeline
context_batch_extraction_query_ingestion_x8
context_stream_batch_api_ingestion_compression
context_eight_parity_gates
context_nine_ingestion_compression_parity_gates
context_module_cpp_surfaces
```

`context_eight_parity_gates` asserts:

```text
1. API ingest accepted event 996001
2. stream ingest accepted events 996002, 996003, 996004, and replay event 996007
3. duplicate API retry 996088 and duplicate stream offsets 996098/996099 are absent
4. batch ingest accepted events 996005, 996006
5. retrieval selected approval events 996001, 996004, 996007
6. selected token count is 54
7. compression id 77001 is queryable
8. resource chunk evidence 30039901 exists and root child fanout is bounded
```

`context_nine_ingestion_compression_parity_gates` repeats the same state and adds
a ninth gate: the compression record must retain source event ids 996001, 996004,
and 996007. This is the production requirement for replay, audit, and explaining
why older context was compressed.

## Monitoring UI Readiness

The monitoring UI under `tools/temporalstore-monitoring-ui/` now exposes two
operator-facing parity sections.

### End-to-End Parity

This table repeats the runtime story across nine complete paths:

```text
raw query to context pack
API idempotency
stream replay checkpoint
batch ingest x8
resource parsing
feedback memory
temporal compression
compression source audit
C++ module parity
```

Every row must show the evidence command or corpus case, the expected output, and
the pass/fail status. This makes parity visible without reading the JSON corpus.

### UI Production Readiness

The UI production-readiness cards repeat nine gates:

```text
accessible controls
responsive layout
overflow guard
empty state safety
refresh resilience
evidence visibility
deterministic fixture
actionable runbook
nine-lane parity
```

Each card carries owner, severity, evidence, and detail. Long model names, commands,
hashes, and source refs must wrap inside their containers on desktop and mobile.

## Local Validation Steps

Run from:

```bash
cd <workspace>/Codex/2026-06-07/what-s-the-topology-for-all/temporalstore-service-fix
```

Schema and corpus:

```bash
python3 -m json.tool third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json >/tmp/temporalstore_external_unified_cases.json
python3 tools/run_temporalstore_unified_tests.py --validate-only
```

C++ unified contract:

```bash
bash tools/run_cpp_unified_context_contract.sh third_party/TemporalStoreTestCorpus/cases/unified_temporalstore_cases.json
```

Monitoring UI:

```bash
python3 -m json.tool tools/temporalstore-monitoring-ui/health.json >/tmp/temporalstore_ui_health.json
node --check tools/temporalstore-monitoring-ui/app.js
python3 -m unittest tools.test_monitoring_ui_context_ops tools.test_render_health_from_results
```

C++ context module:

```bash
cmake --build build-ubuntu22/test-release --target context_module_test -j2
./build-ubuntu22/test-release/src/extension/context/context_module_test --gtest_brief=1
```

Expected current results:

```text
unified corpus validation: passed
C++ unified context contract: passed, 12 context cases / 96 context steps
monitoring UI unit tests: 6 tests passed
context_module_test: 7 tests passed
```

## Production Readiness Notes

- Register every context model in `src/model/model_manager.cc`. Missing
  registrations break storage object creation at runtime.
- Child refs must stay idempotent. Duplicate child refs should not create duplicate
  timeline writes.
- Keep event writes lightweight. Do not synchronously refresh parent summaries.
- Keep payload fields sparse. Use object keys, scope hashes, secondary indexes, and
  embeddings instead of large arbitrary JSON filters.
- Use MatrixDB or an offline warehouse for wide OLAP/HSAP analysis that cannot fit
  serving-time thresholds.
- Use TemporalStore first for low-latency serving state; add external vector DB or
  object storage only when vector scale or raw object size justifies it.

## What This Replaces Or Complements

Filesystem-style context systems are easy to understand, but TemporalStore gives
serving-time primitives that a filesystem does not naturally provide:

```text
time range scans
valid-as-of filtering
secondary index refs
bounded tree traversal
stored embeddings with scoring
token-budget replay audits
non-destructive temporal compression
tenant/table isolation
serving-oriented storage boundaries
```

Graph memory systems are strong for relationship reasoning, but they often require
LLM extraction, graph updates, and semantic retrieval before serving. TemporalStore
is better positioned as the low-latency temporal serving substrate. MatrixArk can
still extract entities and relationships above it when useful.
