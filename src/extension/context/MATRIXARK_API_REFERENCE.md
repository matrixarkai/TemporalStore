# MatrixArk API And Schema Reference

MatrixArk exposes a small agent-facing API and hides the TemporalStore serving
schema. Agents, vertical Cursor products, and enterprise integrations send
messages, resources, feedback, scope, and optional hook evidence. MatrixArk
always extracts, canonicalizes, indexes, summarizes, embeds, compresses, and
packs context internally.

## API Boundary

```text
AI agent / hook / SDK / MCP client
-> MatrixArk public API
-> MatrixArk extraction and context engineering
-> TemporalStore internal context APIs
-> ContextPack returned to the agent
```

Customers should not send `ContextEvent`, `ContextEntity`, `ContextIndex`,
`ContextSummary`, or compression records. Those are internal records optimized
for serving-time retrieval.

## Public HTTP API Contract

The HTTP shape is the production contract. The local MVP currently implements
the same shape through MCP tools and the repo-local Python adapter.

All endpoints should use a common envelope:

```json
{
  "request_id": "optional-client-request-id",
  "idempotency_key": "optional-retry-key",
  "scope": {
    "tenant_id": "company-a",
    "user_id": "alice",
    "session_id": "cursor-thread-123",
    "team": "infra_team",
    "project": "project_1"
  },
  "metadata": {
    "source": "cursor",
    "node_path": ["company_a", "infra_team", "project_1", "approvals"],
    "reply_to_context_pack_id": "pack-123"
  }
}
```

`scope.user_id` or `scope.session_id` is the minimum useful scope. Sending both
is best. `session_id` gives same-thread reasoning; `user_id` gives longer-term
user memory. MatrixArk may derive `tenant_hash`, `scope_hash`, `node_hash`, and
other internal keys from auth and the request envelope.

### POST /v1/context/ingest

Ingest one message, tool output, business event, or already-parsed resource
description.

Request:

```json
{
  "messages": [
    {
      "role": "user",
      "content": "Alice approved the GPU request.",
      "name": "Alice",
      "created_at_ms": 1781500000000
    }
  ],
  "scope": {
    "user_id": "alice",
    "session_id": "thread-gpu",
    "team": "infra_team",
    "project": "project_1"
  },
  "metadata": {
    "source": "cursor"
  },
  "agent_hook": {
    "source": "matrixark-sdk",
    "hook_type": "before_llm",
    "hook_id": "hook-before-gpu",
    "observed_at_ms": 1781500000000,
    "idempotency_key": "gpu-approval-1",
    "trigger": "user_message",
    "auto_captured": true
  }
}
```

Response:

```json
{
  "status": "accepted",
  "event_id_hash": 52001,
  "node_hash": 61001,
  "classification": "NEW_EVENT",
  "extraction_mode": "matrixark_internal",
  "prior_context": "session",
  "prior_refs": [{"ref_type": "event", "ref_hash": 51000}],
  "quality_warning": ""
}
```

Behavior:

```text
normalize envelope
-> pull bounded prior context when useful
-> extract event/entity/status/source/time
-> choose or create ContextNode path
-> write ContextEvent and default ContextIndex rows
-> write/update ContextEntity when current state changes
-> write ContextSummary and ContextEmbedding records
-> mark summary/compression work for async refresh
```

### POST /v1/context/batch_ingest

Ingest multiple independent items. Good rows should be accepted even when bad
rows fail validation.

Request:

```json
{
  "items": [
    {
      "idempotency_key": "approval-1",
      "messages": [{"role": "user", "content": "Alice approved GPU request 8891."}],
      "scope": {"session_id": "thread-gpu", "team": "infra_team"}
    },
    {
      "idempotency_key": "budget-1",
      "messages": [{"role": "tool", "content": "Budget updated to 42000 USD."}],
      "scope": {"session_id": "thread-gpu", "team": "infra_team"}
    }
  ]
}
```

Response:

```json
{
  "status": "partial_success",
  "accepted": [{"index": 0, "event_id_hash": 52001}],
  "rejected": [{"index": 1, "error": {"code": "invalid_message"}}]
}
```

### POST /v1/context/stream_ingest

Ingest ordered stream events from an agent gateway, proxy, or on-prem sidecar.

Request:

```json
{
  "stream": "cursor-thread-events",
  "partition": "thread-gpu",
  "offset": 42,
  "payload": {
    "messages": [{"role": "assistant", "content": "The GPU request is approved."}],
    "scope": {"session_id": "thread-gpu", "team": "infra_team"}
  }
}
```

Response:

```json
{
  "status": "accepted",
  "stream": "cursor-thread-events",
  "partition": "thread-gpu",
  "offset": 42,
  "deduped": false,
  "event_id_hash": 52002
}
```

### POST /v1/context/resource

Register a file, URL, PDF, markdown page, log, or runbook. MatrixArk stores
parsed serving data and references raw bytes by `raw_uri`.

Request:

```json
{
  "raw_uri": "s3://company-a/runbooks/gpu-approval.pdf",
  "resource_type": "pdf",
  "messages": [
    {"role": "tool", "content": "GPU approval runbook attached."}
  ],
  "scope": {"team": "infra_team", "project": "project_1"},
  "metadata": {"source": "resource_parser"}
}
```

Response:

```json
{
  "status": "accepted",
  "resource_ref": "s3://company-a/runbooks/gpu-approval.pdf",
  "chunk_count": 12,
  "summary_ref_hash": 63001,
  "embedding_refs": [63001, 63002]
}
```

### POST /v1/context/retrieve

Return a token-budgeted `ContextPack` for a raw query. The caller should combine
the returned pack with local context before the final LLM call.

Request:

```json
{
  "query": "What GPU approvals are current?",
  "scope": {"session_id": "thread-gpu", "team": "infra_team"},
  "max_context_tokens": 2048
}
```

Response:

```json
{
  "context_pack_id": "pack-thread-gpu-1781500001",
  "used_context_tokens": 312,
  "insufficient_context": false,
  "sections": [
    {
      "title": "Current approvals",
      "items": [
        {
          "ref_type": "event",
          "ref_hash": 52001,
          "node_hash": 61001,
          "text": "Alice approved the GPU request.",
          "score": 0.92,
          "staleness": "fresh"
        }
      ]
    }
  ],
  "selected_refs": [{"ref_type": "event", "ref_hash": 52001}],
  "blocked_refs": [],
  "dropped_refs": [{"ref_hash": 51000, "reason": "token_budget"}],
  "quality_warnings": [],
  "audit_ref": "pack-thread-gpu-1781500001"
}
```

Retrieval pipeline:

```text
raw query + hints
-> query understanding
-> scope/time/filter planning
-> ContextNode traversal by summary embeddings
-> ContextEvent/resource/entity retrieval
-> staleness and authority scoring
-> token budgeting
-> ContextPackAudit
```

### POST /v1/context/feedback

Capture final answer feedback, accepted refs, rejected refs, corrections, and
confirmations.

Request:

```json
{
  "messages": [
    {"role": "user", "content": "Yes, that answer is correct."}
  ],
  "scope": {"user_id": "alice", "session_id": "thread-gpu"},
  "context_pack_id": "pack-thread-gpu-1781500001",
  "accepted_refs": [{"ref_type": "event", "ref_hash": 52001}],
  "rejected_refs": []
}
```

Response:

```json
{
  "status": "accepted",
  "event_id_hash": 52003,
  "classification": "CONFIRMATION",
  "prior_context": "explicit",
  "prior_refs": [{"ref_type": "event", "ref_hash": 52001}],
  "quality_warning": ""
}
```

Short feedback such as `yes`, `correct`, or `wrong` is `AMBIGUOUS` unless
MatrixArk can attach it to a prior context pack, accepted/rejected refs, an
assistant message in the same batch, or same-session history.

### GET /v1/context/audit/{context_pack_id}

Return why a pack selected, blocked, dropped, or compressed each piece of
context.

Response:

```json
{
  "context_pack_id": "pack-thread-gpu-1781500001",
  "request_time_ms": 1781500001000,
  "selected_refs": [{"ref_type": "event", "ref_hash": 52001}],
  "blocked_refs": [],
  "dropped_refs": [{"ref_hash": 51000, "reason": "token_budget"}],
  "layer_scores": [{"node_hash": 61001, "depth": 4, "score": 0.88}],
  "token_budget": {"max": 2048, "used": 312}
}
```

### GET /v1/context/replay/{context_pack_id}

Return replayable local evidence for debugging and governance.

Response:

```json
{
  "context_pack_id": "pack-thread-gpu-1781500001",
  "events": [
    {
      "record_type": "context_event",
      "event_id_hash": 52001,
      "node_hash": 61001,
      "summary_text": "Alice approved the GPU request."
    }
  ]
}
```

## Shared Public Schemas

### Message

```json
{
  "role": "user | assistant | tool | system",
  "content": "required text",
  "name": "optional human/tool/agent label",
  "created_at_ms": 1781500000000
}
```

`created_at_ms` is the source event time. MatrixArk still uses server ingestion
time as the primary write key. Extracted/source time is stored as secondary
context for filters and historical replay.

### Scope

```json
{
  "tenant_id": "company-a",
  "user_id": "alice",
  "session_id": "thread-123",
  "team": "infra_team",
  "project": "project_1"
}
```

`tenant_id` usually comes from auth. `user_id` and `session_id` are optional, but
at least one should be sent by serious integrations.

### Metadata

```json
{
  "source": "cursor",
  "node_path": ["company_a", "infra_team", "project_1", "approvals"],
  "reply_to_message_id": "msg-123",
  "reply_to_context_pack_id": "pack-123",
  "agent_extraction": {
    "event_type": "approval",
    "entity": "GPU request 8891",
    "status": "approved"
  }
}
```

`metadata.agent_extraction` is a hint, not a serving schema. MatrixArk validates
and normalizes hints into its own internal model.

### Agent Hook

```json
{
  "source": "matrixark-sdk",
  "hook_type": "before_llm | after_llm | tool_result | resource_added | feedback | session_commit",
  "hook_id": "hook-before-gpu",
  "observed_at_ms": 1781500000000,
  "idempotency_key": "gpu-approval-1",
  "trigger": "user_message",
  "auto_captured": true
}
```

Hooks let MatrixArk capture events automatically from the host agent loop.

## MCP Server

The local MCP server is:

```bash
python3 tools/matrixark_mcp_server.py \
  --event-log /tmp/matrixark-mcp-events.jsonl
```

Codex config:

```toml
[mcp_servers.matrixark]
command = "python3"
args = [
  "tools/matrixark_mcp_server.py",
  "--event-log",
  "/tmp/matrixark-mcp-events.jsonl"
]
startup_timeout_sec = 10
tool_timeout_sec = 60
enabled = true
```

### matrixark_ingest

Input schema:

```json
{
  "kind": "message | feedback | resource | business_data",
  "messages": [{"role": "user", "content": "Alice approved the GPU request."}],
  "scope": {},
  "metadata": {},
  "agent_hook": {},
  "raw_uri": "optional-resource-uri",
  "resource_type": "md | txt | pdf | url"
}
```

Required: `messages`.

Output: same core response as `/v1/context/ingest`.

### matrixark_retrieve

Input schema:

```json
{
  "query": "what GPU approvals are current?",
  "scope": {},
  "max_context_tokens": 2048
}
```

Required: `query`.

Output: `ContextPack`-like response with `context_pack_id`, selected refs,
layer scores, quality warnings, and token usage.

### matrixark_feedback

Input schema:

```json
{
  "messages": [{"role": "user", "content": "Yes, correct."}],
  "scope": {},
  "metadata": {},
  "context_pack_id": "pack-123",
  "accepted_refs": [],
  "rejected_refs": [],
  "agent_hook": {}
}
```

Required: `messages`.

Output: same core response as `/v1/context/feedback`.

### matrixark_replay

Input schema:

```json
{
  "context_pack_id": "pack-123"
}
```

Required: `context_pack_id`.

Output: replay records from the local event log or production audit store.

## Internal TemporalStore Context APIs

These APIs are not customer-facing. MatrixArk calls them after extraction and
planning.

| Function | Purpose |
| --- | --- |
| `UPSERT_NODE` / `GET_NODE` | Create or read a canonical context tree node. |
| `UPSERT_CHILD_REF` / `QUERY_CHILDREN` | Maintain filesystem-like parent-child edges for bounded tree traversal. |
| `WRITE_EVENT` / `QUERY_EVENTS` | Write and read timestamp-keyed context events. |
| `WRITE_EXTRACTED_EVENT` | Write an event plus extracted secondary indexes in one logical operation. |
| `WRITE_INDEX_REF` / `QUERY_INDEX` | Maintain compact secondary indexes for event kind, entity, status, source, and time bucket. |
| `UPSERT_ENTITY` / `GET_ENTITY` / `QUERY_ENTITIES` | Store evolving current state derived from events. |
| `UPSERT_SUMMARY` / `QUERY_SUMMARIES` | Store L0/L1 node summaries and session summaries. |
| `UPSERT_EMBEDDING` / `QUERY_EMBEDDINGS` | Store summary/event/resource embeddings inside TemporalStore. |
| `TRAVERSE_CONTEXT_TREE` | Score child node embeddings layer by layer under serving-time limits. |
| `MARK_SUMMARY_DIRTY` / `QUERY_SUMMARY_DIRTY` | Queue async summary refresh without blocking ingestion. |
| `WRITE_COMPRESSION_EVENT` / `QUERY_COMPRESSION_EVENTS` | Store cold-window compressed summaries. |
| `COMPRESS_EVENTS` | Build a compression event from an old event window. |
| `QUERY_NODE_CONTEXT` | Fetch node metadata, overall summary, and cold-window summaries together. |
| `WRITE_PACK_AUDIT` / `QUERY_PACK_AUDIT` | Persist retrieval audit and replay evidence. |

Core internal records:

```text
ContextNode              tree node with parent, canonical name, L0, last event time
ContextChildRef          parent_hash -> child_hash edge
ContextEvent             ingestion-time-keyed event with extracted/source time
ContextEntity            evolving state extracted from events
IndexRef                 secondary lookup ref to event/node/time
ContextSummary           L0/L1 or session summary for a node
ContextEmbedding         vector for node/event/resource/summary refs
ContextCompressionEvent  cold-window temporal summary
ContextPackAudit         selected/dropped/blocked refs and token budget evidence
```

## Defaults And Limits

Recommended MVP defaults:

```text
max_context_tokens             2048
prior_raw_events_for_extraction 8
prior_text_budget_bytes        4096
tree_max_depth                 6
top_k_per_depth                5
max_children_scored_per_parent 128
max_candidate_nodes            24
summary_level_l0               required
summary_level_l1               optional
```

## Error Shape

All public APIs should return structured errors:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "messages must be a non-empty array",
    "retryable": false,
    "request_id": "req-123"
  }
}
```

Common codes:

```text
invalid_request
unauthorized
scope_required
idempotency_conflict
resource_unreadable
token_budget_too_small
deadline_exceeded
internal_error
```

## Versioning

MatrixArk should version public APIs independently from internal TemporalStore
models:

```text
Public API:     /v1/context/*
MCP tools:      matrixark_*
Internal proto: bcache2.context.Function
```

Adding internal fields must not require agent callers to change payloads. Public
schema changes should be additive whenever possible.
