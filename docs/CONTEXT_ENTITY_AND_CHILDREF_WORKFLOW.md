# ContextEntity And ContextChildRef Workflow

This note explains how MatrixArk should extract entities when an event arrives,
how TemporalStore stores the event plus evolving entity state, and how
`ContextChildRef` makes tree traversal fast enough for serving-time context.

## Event To Entity

When an input arrives, MatrixArk should always treat the raw signal as an
immutable event first:

```text
raw input
-> model/rule extraction
-> canonical node path selection
-> append ContextEvent at the selected leaf node
-> optionally upsert ContextEntity at the same leaf node
-> write indexes, embeddings, and dirty summary marker
-> async worker writes ContextSummary and summary ContextEmbedding
```

The event is the replayable source of truth. The entity is a compact current or
evolving state derived from one or more events.

Example input:

```text
Alice approved GPU purchase request 8891 for Project 1.
```

Extraction output:

```json
{
  "event_type": "approval_confirmation",
  "status": "approved",
  "team": "infra_team",
  "project": "project_1",
  "entity": {
    "entity_hash": 60000,
    "entity_type": 1,
    "entity_name": "gpu_purchase_request_8891",
    "entity_value": "approved",
    "confidence": 0.95
  }
}
```

TemporalStore writes:

```text
ctx:event:tenant:approval_leaf
  -> ContextEvent(event_id=50000, time=t, type=approval_confirmation, text=...)

ctx:entity:tenant:approval_leaf:60000
  -> ContextEntity(entity=60000, value=approved, updated_at=t, sources=[50000])
```

If the entity already exists, `UPSERT_ENTITY` replaces the compact state with the
latest extracted state and source pointer. If it does not exist, the same call
creates it. This keeps ingestion idempotent and lightweight.

## When To Create Or Update

Create a new entity when extraction identifies a durable object the system may
need again:

```text
purchase request, incident, customer policy, deployment, approval, contract,
runbook, budget item, claim, ticket, account, workflow state
```

Update an existing entity when the new event talks about the same durable object:

```text
approved -> rejected
open -> mitigated
draft -> active
current budget 42000 -> current budget 48000
runbook attached -> runbook confirmed by user
```

Do not create entities for throwaway utterances:

```text
thanks, ok, yes, retry that, explain again
```

Those can still be written as events if replay or feedback memory needs them, but
they should not become stable entity state unless they confirm or correct a prior
entity.

## Covered Pipelines

The local unified E2E now covers entity creation/update from:

```text
API ingest       -> context_api_ingest_raw_event
batch ingest     -> context_batch_ingest_raw_events
stream ingest    -> context_stream_ingest_raw_events
resource extract -> context_extract_resource_events
feedback ingest  -> context_ingest_feedback
retrieval        -> context_retrieve / context_retrieve_with_resources
```

Retrieval uses entity state only when requested by the query plan:

```text
raw query + hints
-> model query understanding
-> tree traversal with embeddings
-> selected leaf nodes
-> matching ContextEntity records first
-> matching ContextEvent/resource evidence next
-> token-budgeted ContextPack
```

The pack includes `entity_hashes` and an `entity_state` section before raw
events. This is how MatrixArk can save tokens: the model sees compact current
state first, then only the raw events/resources needed for evidence.

## Event, Entity, Summary References

MatrixArk now treats the pack as a set of references, not a blob of copied text:

```json
{
  "entity_hashes": [60000],
  "summary_refs": [4201, 4200],
  "event_ids": [50000, 51000, 51001],
  "chunk_hashes": [53000],
  "context_pack_sections": [
    "entity_state",
    "summary_context",
    "current_state",
    "selected_evidence"
  ]
}
```

The roles are deliberately separate:

```text
ContextSummary  -> L0/L1 navigation and overview context for selected nodes
ContextEntity   -> compact current/evolving state, derived from events
ContextEvent    -> immutable replayable evidence and timeline facts
ResourceChunk   -> selected L2 evidence from parsed files
```

This mirrors the useful part of filesystem-style layered context while keeping
TemporalStore as the serving store. During retrieval, summaries are selected
after tree traversal and before raw event evidence. That lets the model see a
small node overview, then the compact entity state, then only the matching events
and chunks that fit the token budget.

## Summary Embeddings

Summary refresh is asynchronous. Event ingestion should stay light, but every
lane can mark summary state dirty. The refresh worker then uses the configured
model provider to generate summary text and an embedding:

```text
summary_text = summarize(events/resources/feedback for node)
summary_vector = embedding_encoder(summary_text)
UPSERT_SUMMARY(node_hash, level, summary_text, valid_from_ms)
UPSERT_EMBEDDING(ref_hash = node_hash, level, summary_vector, updated_at_ms)
```

The local E2E verifies this after API ingest, batch ingest, stream ingest,
resource extraction, and feedback ingestion. It queries `ContextSummary` records
and summary embedding refs from TemporalStore, so the path is covered by both the
C++ contract and Rust mock proxy. Retrieval also returns `summary_refs`, so
summaries are covered in the serving path rather than only in the background
refresh path.

## ContextChildRef

`ContextChildRef` is the tree edge record:

```text
ctx:child:{tenant_hash}:{parent_hash}
  timeline_key = updated_at_ms + child_hash suffix
  payload = { parent_hash, child_hash, updated_at_ms }
```

It is used to list children for one parent without scanning all nodes.

Example tree:

```text
company_a
└── infra_team
    └── project_1
        ├── approvals
        │   └── gpu_purchase_request_8891
        └── incidents
            └── rollback_incident
```

Stored edges:

```text
ctx:child:tenant:company_a_hash
  -> child_hash=infra_team_hash

ctx:child:tenant:infra_team_hash
  -> child_hash=project_1_hash

ctx:child:tenant:project_1_hash
  -> child_hash=approvals_hash
  -> child_hash=incidents_hash

ctx:child:tenant:approvals_hash
  -> child_hash=gpu_purchase_request_8891_hash
```

Query traversal:

```text
start at company/team/project root
-> QUERY_CHILDREN(parent)
-> load child L0 embeddings
-> score query vector against each child
-> keep top-k children
-> repeat until leaf/candidate/deadline limit
```

This helps because a query only scores siblings under the current frontier. A
tenant can have many nodes, but serving does not scan every node or every event.

## Why Not Put Children In ContextNode

Keeping child edges in `ContextChildRef` avoids rewriting the parent node on every
new child. Parent summaries can be refreshed asynchronously. Event writes remain:

```text
ensure missing node path
upsert new child refs only
append event
upsert entity if needed
mark summary dirty
```

That reduces write amplification for hot parents such as `project_1`,
`approvals`, or `incidents`.

## Local Debug Commands

Fast deterministic run:

```bash
python3 tools/run_context_pipeline_scale_e2e.py \
  --model-provider deterministic \
  --events-per-lane 50 \
  --write-results /tmp/context_entity_e2e_50.json
```

OSS-model Docker run with cached model:

```bash
python3 tools/download_context_oss_models.py --source modelscope --skip-vlm
EVENTS_PER_LANE=5 tools/run_context_pipeline_docker_oss_models.sh
```

Inspect the generated result:

```bash
cat /tmp/context_entity_e2e_50.json
```

The expected result should include:

```json
{
  "entity_records": 2,
  "summary_records": 7,
  "summary_embedding_refs": 6,
  "resource_extracted_events": 1,
  "tree_shape": "root/collection/leaf",
  "layer_traversal": "global_topk_per_depth"
}
```
