# MatrixArk Secondary Index + Tree Retrieval Walkthrough

This doc explains the end-to-end MatrixArk flow for LOCOMO-style long conversation memory: extraction, ContextNode L0/L1 summaries, layer-by-layer traversal, secondary-index filtering, segment/event/entity use, entity updates, and token-budgeted retrieval.

## 1. Big Picture

MatrixArk should not scan every memory and then ask embeddings to save us. The target serving path is:

```text
raw messages or batch
-> one-pass extraction
-> ContextNode path + L0/L1 summaries + summary embeddings
-> ContextEvent / ContextEntity / ContextSegment / ContextIndex
-> query understanding
-> layer-by-layer ContextNode traversal using L0/L1 summary embeddings
-> secondary-index prefilter inside selected subtrees
-> leaf event/entity/segment scoring
-> time decay + business weight + question-type packing
-> ContextPack + audit
```

The key idea: use the hierarchy first, then cheap indexes, then dense similarity only on a much smaller candidate set.

## 2. What Secondary Indexes Are For

Secondary indexes are not the tree path and not access scope. They are compact, general filters that can remove most wrong candidates before expensive scoring.

Current MatrixArk runtime writes general `ContextIndex` terms such as:

```text
event_type:confirmation
entity_type:location
entity_type:preference
classification:batch_memory
status:observed
source_type:message
segment_topic:approval_budget
```

We intentionally do not use `team` or `project` as default secondary indexes. Those are scope/path fields. They are useful for isolation and routing, but not general enough as retrieval filters.

Recommended split:

| Field kind | Used as | Example |
|---|---|---|
| `account_id`, `tenant_id` | security/root scope | account/acme, tenant/eng |
| `user_id`, `session_id`, `team`, `project` | scope and path routing | user/alice, project/orion |
| `event_type` | secondary index | confirmation, correction, status_update |
| `entity_type` | secondary index | location, preference, job_status |
| `status` | secondary index | observed, superseded, stale |
| `source_type` | secondary index | message, feedback, resource |
| `segment_topic` | secondary index | approval_budget, recursion |


## 2.1 One-Message Hooks And Session Commit

Codex, Claude, Cursor, and similar agents usually emit one message or tool result at a time. MatrixArk now supports the VikingMem-style compromise:

```text
single incoming hook message
-> write raw ContextEvent immediately
-> append event id to same-session SessionBuffer
-> when threshold/session boundary happens
-> matrixark_session_commit
-> one-pass extraction over buffered raw events
-> write ContextEntity / ContextSegment / ContextIndex / ContextSummary
```

The grouping key is general and auth-oriented, not company/team/project oriented:

```text
account_id + tenant_id + user_id + session_id
```

If `session_id` is missing, MatrixArk can fall back to the user, but production hooks should send a real session/thread/run id.

### Immediate ingest

`matrixark_ingest` writes:

```text
ContextEvent              raw replayable evidence
ContextEmbedding          event text vector
ContextSummary/Embedding  node_l0/node_l1 refresh for traversal
session_buffer_event      pending event pointer for later commit
```

Optional auto commit:

```json
{
  "messages": [{"role": "user", "content": "I moved to Seattle today."}],
  "scope": {"user_id": "alice", "session_id": "thread_123"},
  "auto_batch_extract": true,
  "session_buffer_threshold": 20
}
```

When the pending same-session raw event count reaches the threshold, MatrixArk calls the same session commit path automatically.

### Explicit session commit

Agents can commit at session end, task completion, feedback, or a topic boundary:

```json
{
  "scope": {"user_id": "alice", "session_id": "thread_123"},
  "force": true,
  "threshold_messages": 20,
  "agent_hook": {
    "source": "codex",
    "hook_type": "session_commit",
    "hook_id": "thread_123_done",
    "observed_at_ms": 1781500000000,
    "auto_captured": true
  }
}
```

The commit path does **not** duplicate raw events. It reads pending source events and writes derived records:

```text
ContextSegment.source_event_ids = [event_1, event_3]
ContextEntity.source_event_ids  = [event_1, event_3]
ContextSummary.source_event_ids = [event_1, event_2, event_3]
ContextIndex.batch_id_hash      = derived batch id
context_batch_commit            = replayable commit audit
```

This matters because `ContextEvent` remains the atomic evidence unit, while segments/entities are derived memory over one or more events.

## 3. How Query Text Becomes Index Filters

MatrixArk owns query understanding. The agent can send only a raw query plus optional scope hints.

Example query:

```text
Where is Alice located now?
```

Query understanding derives:

```json
{
  "question_type": "current_state",
  "secondary_index_filter": [
    ["entity_type:location"]
  ],
  "time_intent": "current/latest",
  "preferred_refs": ["entity", "event"]
}
```

Example query:

```text
Who approved the GPU purchase?
```

Derived filter groups:

```json
{
  "question_type": "fact",
  "secondary_index_filter": [
    [
      "event_type:confirmation",
      "entity_type:confirmation",
      "classification:confirmation",
      "segment_topic:approval_budget"
    ]
  ]
}
```

Filter semantics:

```text
AND across groups
OR within each group
```

So if a query has two groups, a candidate must match at least one term in group A and at least one term in group B.

Today this is implemented conservatively in `infer_secondary_index_filter_groups()`. In production, this can be generated by an OSS/OpenAI-compatible query understanding provider, but the output should remain the same simple internal filter contract.

## 4. LOCOMO-Style Example Conversation

This is a LOCOMO-style memory example, not a claim about one exact official LOCOMO row.

Conversation session:

```text
2026-03-02 user: I moved to Seattle today.
2026-03-02 assistant: I will remember that your current location is Seattle.
2026-04-10 user: I moved to Austin for the new infra project.
2026-04-10 assistant: Got it. Austin is your latest location.
2026-05-01 user: Alice approved the GPU purchase after finance review.
2026-05-01 assistant: The GPU purchase is approved by Alice.
```

Questions:

```text
Q1: Where is the user currently located?
Q2: Where was the user before April 10?
Q3: Who approved the GPU purchase?
```

## 5. Extraction: What Gets Written

### 5.1 ContextNode

The ContextNode owns hierarchy. In MVP, the path comes from `metadata.node_path` when supplied; otherwise MatrixArk uses a simple scope-derived default.

Recommended canonical paths:

```text
account:acme / tenant:eng / user:alice / sessions / locomo_0 / location
account:acme / tenant:eng / user:alice / sessions / locomo_0 / approvals / gpu_purchase
```

Current code generates L0/L1 summary and embedding records for every prefix:

```text
account:acme
account:acme / tenant:eng
account:acme / tenant:eng / user:alice
account:acme / tenant:eng / user:alice / sessions
...
```

Each prefix gets:

```text
ContextSummary(summary_type=node_l0 or node_l1)
ContextEmbedding(embedding_type=node_l0 or node_l1)
```

This makes layer-by-layer traversal possible.

### 5.2 ContextEvent

Every raw message stays replayable.

Example:

```json
{
  "record_type": "context_event",
  "node_path": ["account:acme", "tenant:eng", "user:alice", "sessions", "locomo_0", "location"],
  "text": "user: I moved to Austin for the new infra project.",
  "internal_extraction": {
    "event_type": "dialogue_batch",
    "classification": "BATCH_MEMORY"
  }
}
```

Events answer evidence questions and provide replay proof for entity state.

### 5.3 ContextEntity

Entities are flat typed state attached to a node.

Example current entity:

```json
{
  "record_type": "context_entity",
  "entity_type": "location",
  "entity_name": "location",
  "state": "Austin for the new infra project",
  "previous_state": "Seattle",
  "source_refs": ["message_index:2"],
  "node_path": ["account:acme", "tenant:eng", "user:alice", "sessions", "locomo_0", "location"]
}
```

For current-state questions, entity state should be read before raw events. Raw events remain available as evidence.

### 5.4 ContextSegment

A segment is a coherent memory slice extracted from a batch. It is created when messages are salient enough and share a topic.

Current segment creation:

```text
1. score each message for saliency
2. drop filler like hi/ok/thanks
3. infer topic such as location, approval_budget, preference, correction
4. group related messages by topic
5. store coordinates and summary
```

Example:

```json
{
  "record_type": "context_segment",
  "topic": "approval_budget",
  "coordinate_tuples": [[4, 5]],
  "summary_text": "Alice approved the GPU purchase after finance review.",
  "node_path": ["account:acme", "tenant:eng", "user:alice", "sessions", "locomo_0", "approvals", "gpu_purchase"]
}
```

How retrieval uses segment:

- Segment summaries can satisfy broad planning questions.
- Segment text can route the query to the right topic.
- Raw events remain the final evidence for precise answers.
- Future target: high-saliency segments can be promoted to `ContextNode(node_type=segment)`.

### 5.5 ContextIndex

Extraction writes compact terms:

```json
[
  "entity_type:location",
  "event_type:confirmation",
  "classification:batch_memory",
  "status:observed",
  "source_type:message",
  "segment_topic:approval_budget"
]
```

These terms are used later as prefilters.

## 6. Entity Update During Extraction

When a new entity is extracted, MatrixArk computes a stable entity identity:

```text
entity_hash = hash(node_hash + entity_type + entity_name)
```

Then it finds the latest entity state for that identity and applies the update.

For location:

```text
old entity state: Seattle
new extracted value: Austin for the new infra project
updated entity state: Austin for the new infra project
previous_state: Seattle
```

For production TemporalStore, this should be a true point/index lookup:

```text
(entity_hash) -> latest ContextEntity version
```

Today the Python adapter scans the local record log for the latest matching `entity_hash`. The logical model is ready, but native serving should make this an indexed lookup.

Secondary indexes help entity update in two ways:

1. `entity_type:location` quickly identifies the class of state being updated.
2. The stable `entity_hash` or future `ContextEntityIndex` finds the exact previous state without enumerating all entities.

## 7. Retrieval: Layer-By-Layer L0/L1 Traversal

For query:

```text
Where is the user currently located?
```

MatrixArk first embeds the query. Then it scores node summaries, not all raw events.

Traversal:

```text
Layer 1: account:acme
  score L0/L1 summary embedding

Layer 2: tenant:eng
  score only selected children

Layer 3: user:alice
  score only selected children

Layer 4: sessions / shared / resources
  select top K

Layer 5: locomo_0
  select top K

Layer 6: location vs approvals vs preferences
  choose location for the query
```

Current config examples:

```json
{
  "top_k_per_layer": 8,
  "max_children_scored_per_parent": 10000
}
```

This is why a separate VectorDB is not required for this MVP serving path: the system does not search one global million-vector pool. It recursively narrows the tree, then scores a bounded set of leaf records.

## 8. Secondary-Index Prefilter Before Leaf Scoring

After tree traversal selects subtrees, MatrixArk applies the query-derived secondary-index filters.

For `Where is Alice located now?`:

```text
required group: entity_type:location
```

Candidate behavior:

| Candidate | Terms | Pass? |
|---|---|---|
| Location entity | `entity_type:location` | yes |
| Location event | derived `event_type:dialogue_batch`, source message | maybe no unless no strict entity filter applies |
| Approval segment | `segment_topic:approval_budget` | no |
| Approval event | `event_type:confirmation` | no |

Only after passing the filter does MatrixArk calculate leaf event/entity/segment dense similarity and sparse lexical scores.

Current telemetry in `ContextPack.recall_policy.secondary_index_filter`:

```json
{
  "enabled": true,
  "required_groups": [["entity_type:location"]],
  "matched_candidate_count": 1,
  "dropped_candidate_count": 3,
  "mode": "AND across groups, OR within each group",
  "applied_before_embedding_scoring": true
}
```

## 9. Event, Entity, Segment Ranking

Inside selected subtrees and after index filtering:

### Entity

Used first for current-state questions:

```text
current location -> ContextEntity(location)
current preference -> ContextEntity(preference)
current job/status -> ContextEntity(job_status)
```

### Event

Used for replayable evidence and exact answers:

```text
who approved GPU purchase -> raw approval message
where was user before April 10 -> older location event
```

### Segment

Used for coherent topic recall:

```text
approval_budget segment -> groups approval + budget turns
recursion segment -> merges scattered recursion discussion and excludes unrelated topic
```

Final score combines:

```text
origin score = dense + sparse + node score
final score = origin + time decay + business importance
packing score = final score adjusted by question type
```

## 10. Question-Type Packing

MatrixArk packs different evidence for different question types.

| Question type | First evidence |
|---|---|
| date | session date + exact turn |
| fact | extracted observation or raw answer turn |
| evidence | raw dialogue first |
| multi-hop | multiple sessions/entities |
| current-state | entity state + stale blocker |
| why/emotion | answer-bearing sentence first |

For LOCOMO-style current location:

```text
1. ContextEntity(location=Austin)
2. raw event: user moved to Austin
3. stale blocker: previous Seattle event if useful
```

For historical question before April 10:

```text
1. raw Seattle event
2. Austin event as boundary
3. location entity only if valid_as_of logic needs it
```

## 11. End-To-End Data Flow For The Three Questions

### Q1: Where is the user currently located?

```text
query understanding:
  question_type=current_state
  index filter=entity_type:location
  time intent=current/latest

node traversal:
  account -> tenant -> user -> sessions -> locomo_0 -> location

leaf retrieval:
  ContextEntity(location=Austin) passes index filter
  approval records dropped before scoring

pack:
  latest location entity + raw Austin event
```

### Q2: Where was the user before April 10?

```text
query understanding:
  question_type=date/current_state hybrid
  time filter=before 2026-04-10
  entity type=location

node traversal:
  same location subtree

leaf retrieval:
  raw Seattle event selected
  Austin event may be included as boundary

pack:
  older location event first, current entity not allowed to overwrite historical answer
```

### Q3: Who approved the GPU purchase?

```text
query understanding:
  question_type=fact
  index filter=[event_type:confirmation OR segment_topic:approval_budget]

node traversal:
  account -> tenant -> user -> sessions -> locomo_0 -> approvals/gpu_purchase

leaf retrieval:
  approval segment and approval event pass
  location entity/event dropped

pack:
  raw approval event first, segment summary second if budget context helps
```

## 12. What Is Implemented Today vs Next

Implemented today in MatrixArk Python runtime:

- L0/L1 node summaries and summary embeddings.
- Layer-by-layer tree traversal using L0/L1 node embeddings.
- General `ContextIndex` terms for event/entity/status/source/segment.
- Query-derived secondary-index prefilter before leaf similarity scoring.
- `SessionBuffer` records for one-message-at-a-time hooks.
- `matrixark_session_commit` for session/task-boundary one-pass extraction.
- Source-event-linked `ContextEntity`, `ContextSegment`, and `ContextSummary` records without duplicating raw batch events.
- Flat `ContextEntity` updates attached to the selected node.
- Saliency/topic-based `ContextSegment` creation in batch/session extraction.
- ContextPack audit telemetry for traversal and secondary filters.

Still needed for production TemporalStore-native serving:

- Native `ContextChildRef` APIs instead of inferring children from path prefixes.
- Native `QUERY_CONTEXT_INDEX` and index-intersection APIs.
- Native point lookup for latest `ContextEntity` by `entity_hash`.
- Native SessionBuffer/index APIs for pending event lookup and commit marking.
- Better query understanding with OSS/OpenAI-compatible provider.
- Promote high-value segments into `ContextNode(node_type=segment)`.
- Stronger temporal query planning for `valid_as_of`, before/after, and stale blockers.
- Larger benchmark ablations: no index vs index, dense-only vs hybrid, tree-first vs flat.

## 13. Recommended Standard

Define the MatrixArk context model this way:

```text
ContextNode = hierarchy and traversal
ContextSummary = L0/L1 text for a node
ContextEmbedding = L0/L1/query/event/entity/segment vectors
ContextIndex = cheap filters before similarity
ContextEvent = raw replayable evidence
ContextEntity = evolving current state
ContextSegment = coherent topic memory, optionally promoted to node
ContextPack = selected context returned to the agent/reader
```

This gives MatrixArk a clean position versus filesystem-only context and flat RAG: it is filesystem-like for navigation, VikingMem-like for event/entity evolution, and TemporalStore-native for time, replay, compression, and cheap serving-time filtering.
