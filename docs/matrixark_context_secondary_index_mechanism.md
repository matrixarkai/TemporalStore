# MatrixArk Context Secondary Index Mechanism

## Short Answer

Context secondary indexes are intended to use the same TemporalStore idea as the older non-context feature/sequence implementation: keep the primary data timestamp-keyed, then use compact filter/index structures to find a small candidate set before scoring.

There are two context paths today:

1. Legacy compatibility refs:

```text
ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}
  timestamp_key -> ContextIndexRef
```

2. New compact postings:

```text
ctxidx2:{tenant_hash}:{scope_hash}:{index_name}:{time_bucket_ms}
  timestamp_key -> posting payload with one or more ref hashes and index_value_hash
```

The production direction is the second path, or an even more native field-filter path, so the serving system does not create one verbose product-visible row per event/keyword/chunk.

## Why The Index Count Does Not Match Item Count

Secondary-index rows/postings are not supposed to equal the number of context items.

One context item can emit multiple index families:

```text
ContextEvent 123
  -> source_type:message
  -> event_type:approval
  -> entity_type:approval_state
  -> keyword_id:gpu
  -> keyword_id:aurora
```

That is one event but several index entries or posting memberships.

The reverse is also true: one compact posting can hold many refs:

```text
ctxidx2:tenant:scope:event_type:20260704093000000
  -> index_value_hash=approval
  -> ref_hashes=[123, 456, 789, ...]
```

That is many events but one posting object.

Counts can differ for several normal reasons:

- one item belongs to multiple index families;
- one posting contains many item refs;
- large postings split only after the max refs per posting threshold;
- refs are deduped inside a family;
- empty or low-signal fields do not get indexed;
- resources often index chunks, facts, and resource-level metadata separately;
- session/shared/global visibility can create different scope buckets;
- parent-child tree refs are separate from secondary indexes.

So the useful metrics are not `index_rows == item_count`. Better metrics are:

```text
indexed_item_count
posting_object_count
posting_ref_count
avg_refs_per_posting
max_refs_per_posting
index_family_count
index_postings_read
candidate_refs_returned
candidate_refs_after_filter
```

## Primary Context Storage

Context events remain timestamp-keyed under the node:

```text
ctx:event:{tenant_hash}:{node_hash}
  context_timeline_key(ingestion_time_ms, event_id_hash) -> ContextEvent
```

The timestamp key is the primary ordering. Secondary indexes should point back to this primary row, not duplicate the event body.

Example:

```text
ctx:event:2466697514329931826:2100209595829882121
  00000001782681920521:1121810234980183195 -> compact event

ctxidx2:2466697514329931826:7836037686236352053:event_type:1782681900000
  00000001782681920521 -> {event_id_hash:1121810234980183195, node_hash:2100209595829882121, index_value_hash:approval}
```

Query then does:

```text
query understanding
-> scope filter
-> L0/L1 node traversal
-> compact secondary-index posting lookup
-> candidate ref ids
-> primary row fetch by node + timestamp/event id
-> score/rerank/pack
```

Broad scans should be fallback/debug only.

## Parent-To-Child Refs Are Separate

Parent-to-child refs are not the same thing as secondary indexes.

They are the adjacency index for ContextNode tree traversal:

```text
ctx:child:{tenant_hash}:{parent_hash}
  timestamp/order key -> ContextChildRef {parent_hash, child_hash, child_type, rank/status}
```

This supports efficient child scans:

```text
parent_hash
-> ctx:child:{tenant_hash}:{parent_hash}
-> child node refs
-> fetch child summaries / embeddings / counts
```

Events, segments, entities, summaries, chunks, and skills are attached records under selected nodes. They do not need to be graph children themselves.

Recommended debug graph display:

```text
ContextNode
  children: derived only when a debug UI explicitly queries ContextChildRef
  events: count from ctx:event:{tenant}:{node}
  segments: count from segment records
  entities: count from entity records
  summaries: count from summary records
  chunks: count from resource chunk records
```

Do not persist or return a child count on every node. That count costs an extra
child-list read on writes and becomes stale under concurrent updates. If a UI
needs it, derive it from the narrow `ctx:child:{tenant_hash}:{parent_hash}` lookup.
This avoids a graph with thousands of event children while still making attached
evidence visible.

## How To Scan Children Efficiently

Use the parent adjacency key:

```text
ctx:child:{tenant_hash}:{parent_hash}
```

Then page through children by timestamp/rank/child hash. Do not scan all nodes for `parent_hash`.

Good scan:

```text
tenant_hash + parent_hash -> direct child refs
```

Bad scan:

```text
tenant -> all ContextNode records -> filter where parent_hash matches
```

## How Resource Links Should Resolve

Resource chunks should not repeat absolute file paths in every chunk or every ContextPack ref.

Use a manifest plus compact chunk locator:

```json
{
  "resource_id": "res_aurora_pdf",
  "raw_uri": "/absolute/path/aurora_gpu_approval_packet.pdf",
  "source_type": "pdf"
}
```

```json
{
  "resource_id": "res_aurora_pdf",
  "chunk_id": "p1_c3",
  "locator": {"page": 1, "chunk": 3}
}
```

The serving/debug UI resolves:

```text
res_aurora_pdf#page=1&chunk=3
```

to an absolute local path, internal `/api/resources/...` URL, or signed object-store URL. The compact ref keeps ContextPack tokens small.

## Recommended Rule

For hot serving:

```text
primary records: timestamp-keyed context series
secondary indexes: compact postings or native field filters
tree traversal: parent-child adjacency refs
resource links: manifest + compact chunk locator
debug/audit: optional verbose expansion
```

That is why index counts, child counts, resource chunk counts, and item counts should be expected to differ.
