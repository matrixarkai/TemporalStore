# Context Extension

This module is the first TemporalStore-native substrate for MatrixArk LLM context serving.

For the full schema, workflow, test matrix, and UI readiness details, see
`docs/LLM_CONTEXT_TEMPORALSTORE_DEEP_DIVE.md`. For the entity extraction/update
flow and a concrete `ContextChildRef` traversal example, see
`docs/CONTEXT_ENTITY_AND_CHILDREF_WORKFLOW.md`. For temporal compression details,
see `docs/CONTEXT_COMPRESSION_WORKFLOW.md`. For the minimal caller format used
by Cursor-like agents, see `AI_AGENT_CONTEXT_ENVELOPE.md`. For the MCP server
that exposes MatrixArk to Codex/Claude/Cursor-like agents, see
`MATRIXARK_MCP_SERVER.md`.

It intentionally keeps the C++ serving schema small:

```text
ContextNodeModel  -> Hash page object with a compact node metadata field
ContextEventModel -> Feature page object keyed by ingestion_time_ms plus a small hash suffix
ContextIndexModel -> Feature page object keyed by event_time_ms plus a small hash suffix
ContextAuditModel -> Feature page object keyed by request_time_ms plus a small hash suffix
ContextDirtyModel -> Feature page object keyed by event_time_ms plus a small hash suffix
ContextChildModel -> Feature page object keyed by updated_at_ms plus child hash suffix
ContextEmbeddingModel -> Hash page object with one compact embedding field
ContextEntityModel -> Hash page object with one compact current-state field
ContextSummaryModel -> Feature page object keyed by valid_from_ms plus summary hash suffix
ContextCompressionModel -> Feature page object keyed by compressed_time_ms plus compression hash suffix
```

MatrixArk should perform LLM extraction, model-based query understanding,
canonicalization, node selection, schema enforcement, and token-budgeted context
packing above this module. The C++ module stores and retrieves already-compiled
context records.

The context model names are first-class C++ model types, but they intentionally reuse
the existing HashModel and FeatureModel page primitives. This keeps the MVP compatible
with TemporalStore's serving path while leaving room to specialize compaction, index
maintenance, and summary refresh behavior behind context-specific model names later.

## Object Keys

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

The object key carries tenant, node, index, scope, and session identity so hot values do not need to duplicate those fields.

Timeline keys use a small fanout suffix to avoid overwriting multiple records that arrive in the same millisecond. Payloads keep only the fields needed by serving:

```text
ContextNode: node_hash, parent_hash, kind, canonical_name, l0, last_event_time_ms
ContextEvent: event_id_hash, ingestion_time_ms, event_time_ms, type, confidence, importance, text
ContextChildRef: parent_hash, child_hash, updated_at_ms
ContextEmbedding: ref_hash, level, vector, updated_at_ms
ContextEntity: entity_hash, node_hash, type, name, value, updated_at_ms, valid_from_ms, confidence, source_event_hashes
ContextSummary: node_hash, level, text, valid_from_ms
ContextCompressionEvent: compression_id_hash, node_hash, source_start_ms, source_end_ms, compressed_time_ms, summary
```

Each `ContextNode` can expose two summary layers:

```text
ContextSummary          current/overall node summary, versioned by valid_from_ms
ContextCompressionEvent cold-window summary for an older source time range
```

Store summary text and summary embeddings as separate records, not as large
fields inside `ContextNode`. The shared key is the node hash:

```text
ContextSummary(node_hash, level, text, valid_from_ms)
ContextEmbedding(ref_hash = node_hash, level, vector, updated_at_ms)
```

This keeps `ContextNode` small enough for hot tree traversal while allowing
MatrixArk to regenerate summary text or embeddings independently. The unified
E2E asserts `context_assert_summary_embeddings` so every node selected for L0
summary traversal has both the readable summary and its vector ref.

`QUERY_NODE_CONTEXT` composes both for serving. It returns node metadata, the
latest overall summary as of the request time, and matching cold-window
compression summaries for the requested historical range. MatrixArk can then pack
that with fresh raw `ContextEvent` rows:

```text
query ContextNode
-> retrieve latest ContextSummary / node L0 for current meaning
-> retrieve fresh raw ContextEvents separately
-> retrieve matching ContextCompressionEvents for old/cold windows
-> pack all selected refs into ContextPack
```

`ContextEventModel` uses ingestion time as the primary write and query timeline.
`event_time_ms` is the optional extracted/source/business time. This keeps late
arriving facts hot and append-only at the moment MatrixArk learns them, while
source-time or business-time lookups can be represented through secondary indexes.
For backward compatibility, event writes that omit `ingestion_time_ms` fall back to
`event_time_ms`, but new MatrixArk ingestion should always set `ingestion_time_ms`.

`WRITE_EXTRACTED_EVENT` is the internal MatrixArk ingestion helper. Customers do
not define schemas in v1; they send raw text plus hints. MatrixArk runs model/rule
extraction and submits a compact `ContextEvent` plus `ExtractedContextIndexes`.
The helper writes the event and default secondary indexes in one serving-oriented
path. Defaults are intentionally small and can be disabled per write:

```text
event_kind         from ContextEvent.type
entity            one ref per extracted entity/topic/resource/person hash
status            approved, blocked, rejected, superseded, current, etc.
source            cursor, codex, slack, jira, github, resource, etc.
event_time_bucket extracted/source/business time bucket
```

This keeps the customer API simple while giving retrieval AND-filterable handles
for scope, kind, entity, status, source, and source-time queries.

`ContextIndexModel` is the secondary-index surface. It stores small `IndexRef`
records under `ctxidx:{tenant}:{index}:{value_hash}:{scope_hash}` so MatrixArk can
jump from declared filters such as status, actor, project, or entity class back
to the primary event timeline without scanning event JSON. Each ref contains only
the primary node, primary ingestion time, and event id. Index rows themselves can
be keyed by extracted/source time, status-change time, actor, entity, or any other
declared compact filter. For AND filters, MatrixArk performs several bounded index
lookups, intersects event ids, then reads the selected `ContextEvent` rows by
node/ingestion-time. Duplicate refs for the same event pointer are
treated as idempotent writes.

`QUERY_EVENTS` can optionally apply serving-time age decay for older events. When
`decay_half_life_ms` is set, TemporalStore computes:

```text
decayed_score = confidence * importance * 0.5 ^ (age_ms / decay_half_life_ms)
```

`as_of_ms` controls the age anchor; if it is omitted, `end_time_ms` is used. Age
is calculated from `ingestion_time_ms`, not extracted/source `event_time_ms`.
Callers can set `rank_by_decayed_score` to return fresher/high-confidence events
first, and `min_decayed_score` to drop stale low-value facts before token packing.
The raw event remains stored and replayable; decay only changes serving ranking and
filtering.

`ContextChildRef` is the tree edge surface. It stores only parent, child, and
update time under `ctx:child:{tenant}:{parent}` so traversal can list a bounded
set of children for one parent, score their summary embeddings, and descend
without scanning every node in the tenant.

`ContextEntity` is compact evolving state for retrieval. MatrixArk extracts or
updates entities from raw API events, batch rows, stream records, resource-derived
events, tool results, and feedback confirmations, then retrieval can include
matching entity state before raw events under the token budget. Entities are not
parent directories; they live beside events at the selected node and point back
to source event ids for replay.

## Write Pattern

MatrixArk should feed TemporalStore through three ingestion lanes that compile into
the same compact serving records:

```text
API ingest      -> one raw event/query/feedback turn with idempotency key
Batch ingest    -> bounded list of raw events/resources with per-item hints
Streaming ingest -> ordered events with stream, partition, offset, and replay checkpoints
```

For AI-agent integrations, these lanes accept a minimal Zep/Mem0-style envelope:
`messages`, `scope`, `metadata`, and optional hook evidence. MatrixArk always
runs extraction and canonicalization. Message, resource, and feedback envelopes
are normalized into the same internal
`ContextEvent`, `ContextEntity`, `ContextSummary`, `ContextIndex`, and replay
records. Customers do not define the extraction schema. MatrixArk can classify
`CONFIRMATION`, `CORRECTION`, `NEW_EVENT`, or `NOISE` only when there is enough
previous context, such as a `context_pack_id`, accepted refs, or previous
assistant answer.

The same envelope can be emitted by a host-agent hook instead of a manual API
call. Supported hook types are `before_llm`, `after_llm`, `tool_result`,
`resource_added`, `feedback`, and `session_commit`. Hook metadata carries source,
hook id, observed time, trigger, and idempotency key; the serving write still goes
through the same ContextEvent/ContextEntity/ContextSummary path.

The repo-local MCP MVP is `tools/matrixark_mcp_server.py`. It exposes
`matrixark_ingest`, `matrixark_retrieve`, `matrixark_feedback`, and
`matrixark_replay` over stdio MCP. Its current adapter is a local JSONL event log
for testing; production should replace that adapter with TemporalStore RPC calls.
The complete public API, schema, MCP tool, and internal TemporalStore context
reference is documented in `MATRIXARK_API_REFERENCE.md`.

All three lanes should use the same extraction and canonicalization path before
writing `ContextEvent`, `ContextChildRef`, `ContextEmbedding`, and secondary index
records. API duplicate keys and replayed stream offsets must be rejected before a
serving write. Stream offsets, API idempotency keys, actor/team/project labels,
freshness policies, child names, and rich resource metadata live in MatrixArk or in
bounded indexes; the TemporalStore payload keeps only serving fields.

The unified parity corpus repeats this as `context_eight_parity_gates` and
`context_nine_ingestion_compression_parity_gates`. Together they assert API ingest,
API idempotency, stream ingest, stream replay checkpointing, batch ingest, tree
retrieval, token budget, compression lookup, compression source ids, and resource
chunk evidence. Keep these cases green when changing either the C++ context module
or the Rust unified runner.

For scaled local validation of the full extraction/ingestion/query loop, run
`tools/run_context_pipeline_scale_e2e.py`. It generates a temporary unified corpus
covering API, batch, stream, resource, feedback, and second-query retrieval paths,
then executes that same corpus through the C++ contract and Rust mock proxy tests.
See `LOCAL_TEST_SETUP.md` for the latest command and observed results.

The scale runner defaults to open-source model names instead of rule vectors:
`sentence-transformers/all-MiniLM-L6-v2` for query and summary embeddings, and
`Salesforce/blip-image-captioning-base` for resource/VLM readiness. Minimal local
CI can fall back to deterministic embeddings, but `--require-models` turns missing
OSS model packages into a hard failure for true local model validation.

The generated E2E tree is intentionally multi-layer:

```text
tenant root -> collection node -> leaf event/resource node
```

MatrixArk generates query, collection L0, leaf L0, and resource summary embeddings
with the configured OSS provider, then stores those vectors through TemporalStore
context embedding records. Retrieval starts at the root, scans every child in the
current layer, scores each child embedding against the query vector, keeps the
global top-k for that depth, and then descends. Top-k is not applied independently
per parent.

The returned `ContextPack` references all selected layers instead of flattening
them into one prompt blob:

```text
summary_refs   collection/leaf L0 or L1 summaries selected by traversal
entity_hashes  compact current state derived from events
event_ids      replayable temporal evidence
chunk_hashes   selected resource chunks for L2 citations
```

This keeps the OpenViking-style L0/L1/L2 idea, but the references live in
TemporalStore data models and can be replayed, audited, and token-budgeted.

A typical useful input writes:

```text
1. one ContextEvent under the primary node, normally through WRITE_EXTRACTED_EVENT
2. default ContextIndexRef records from internal extraction fields
3. parent-child refs for newly created path edges
4. one ContextEmbedding for the leaf L0 summary, plus optional scope/resource embeddings
5. optional ContextEntity updates for current/evolving state
6. one async SummaryDirty marker, if the caller chooses to enqueue summary refresh
7. one ContextPackAudit record after query serving
```

Do not duplicate the same event under many related nodes. Use secondary index refs for alternate lookup paths.

`WRITE_EVENT` intentionally does not update node summaries, parent summaries, or dirty markers. Event writes must stay lightweight. MatrixArk should call `MARK_SUMMARY_DIRTY` asynchronously, or let a background worker derive dirty nodes from event timelines.

`UPSERT_CHILD_REF` is the lightweight tree-maintenance write. It deduplicates children by
`child_hash` and returns whether a new edge was created. It does not rewrite parent
node metadata or synchronously refresh L0/L1 summaries for the parent path; summary
refresh remains an async worker responsibility.

## Query Pattern

Serving queries should prefer:

```text
raw query + hints
-> model query understanding
-> scope/time/filter planning
-> ContextNode traversal with layer-wise child embedding scoring
-> ContextEvent/resource retrieval at selected leaves
-> algorithmic staleness scoring
-> token budgeting
-> ContextPack
```

This module does not run LLM extraction or prompt rendering. MatrixArk or another caller
generates embeddings; TemporalStore stores those vectors and computes bounded cosine
scores during tree traversal.

The query-understanding boundary is intentionally model-owned. MatrixArk can use
an OSS LLM, OpenAI, or an AI-agent-provided plan to produce a compact `query_plan`
with scope, time window, filters, and token policy. Staleness is intentionally not
LLM-owned in serving: retrieval applies a deterministic policy such as
`algorithmic_freshness_v1` over event time, validity, confidence, importance, and
authority so stale blockers and selected evidence are replayable.

## Tree Traversal

`TRAVERSE_CONTEXT_TREE` starts from a tenant/team/project node, lists bounded children
for the current frontier, scores each child embedding against a query vector, keeps
the global top children for that layer, and stops at depth/candidate limits. This is
intentionally not a full filesystem crawl and not a per-parent top-k walk.

Defaults expected by callers:

```text
max_depth = 6
top_k_per_depth = 5
max_children_scored_per_parent <= 256
max_candidate_nodes = 24
```

## Summary Refresh And Compression

`MARK_SUMMARY_DIRTY` remains the lightweight write-time signal. Async workers should
refresh both `ContextSummary` and `ContextEmbedding` records after ingestion. The
summary text is stored through `UPSERT_SUMMARY`; the model-generated summary
vector is stored through `UPSERT_EMBEDDING` using the same node/ref hash. Query
serving can then read the summary version by time and use the summary embedding
for tree traversal. Older raw event windows can be summarized with
`WRITE_COMPRESSION_EVENT`; query serving can then include compressed summaries
alongside fresh raw events.

The local E2E covers summary refresh after API ingest, batch ingest, stream
ingest, resource extraction, and feedback ingestion. It verifies summary records
with `context_query_summaries` and summary vectors with `context_query_embeddings`.
It also writes and queries compression records for approval and incident windows
with source event ids, so compressed context remains replayable.

Compression is time-windowed and non-destructive in the MVP:

```text
fresh window       -> query raw ContextEvent records
older hot window   -> query raw events plus ContextCompressionEvent summaries
cold window        -> prefer compressed summaries, keep raw events queryable by policy
```

`COMPRESS_EVENTS` is the built-in TemporalStore operator for deterministic MVP
compression. It scans a bounded source window on one node, filters by confidence
and importance, writes one `ContextCompressionEvent`, and leaves every source
`ContextEvent` intact. The generated summary is intentionally simple; MatrixArk can
still use `WRITE_COMPRESSION_EVENT` to write richer LLM-generated summaries through
the same storage path.

`QUERY_COMPRESSION_EVENTS` matches by source-window overlap, not by
`compressed_time_ms`. That matters because async compression may run hours or days
after the events being compressed; a query over last week's approval window should
still find the summary even if it was produced today.

Compression records should include the source time range. Source event ids and
larger replay metadata can stay in MatrixArk pack audits or historical storage;
TemporalStore keeps the compact window summary on the serving path. MatrixDB or
object storage can retain deeper historical payloads when customers need
audit-heavy analysis.
