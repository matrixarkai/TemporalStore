# MatrixArk Secondary Index Design

## Short Answer

TemporalStore already had a primary timestamp-keyed feature model before MatrixArk context management was introduced. That older model stores data under one time-ordered key and applies compact field filters during query.

MatrixArk's compatibility `ContextIndexRef` layer is closer to a native posting list:

```text
ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}
  timestamp_key -> {primary_node_hash, primary_event_time_ms, event_id_hash}
```

New C++ default context-index writes use compact bucket postings:

```text
ctxidx2:{tenant_hash}:{scope_hash}:{index_name}:{time_bucket_ms}
  timestamp_key -> {primary_node_hash, primary_event_time_ms, event_id_hash, index_value_hash}
```

This moves the hot serving key shape closer to `scope + index family + time bucket`: one posting object can hold multiple values for the same index family and time bucket, and query-time filtering checks the stored `index_value_hash`. The old `ctxidx:` key remains readable for compatibility/debug callers that use `WRITE_INDEX_REF` directly.

That is better than duplicating full events or creating product-visible rows, but it can still fan out if every resource chunk, PDF keyword, heading, status, and source emits unbounded refs. The longer-term production direction remains:

```text
timestamped context series + declared compact filter fields
```

not:

```text
unbounded product-visible ContextIndex rows for every term
```

## How TemporalStore Worked Before Context Management

The pre-existing TemporalStore feature API has this shape:

```cpp
struct TemporalFeaturePoint {
    uint64_t timestamp;
    std::string value;
};

struct TemporalFeatureFilter {
    std::string field;
    TemporalFeatureFilterOp op;
    uint64_t value;
};

struct TemporalFeatureQuery {
    uint64_t start_ts;
    uint64_t end_ts;
    uint64_t count;
    std::vector<TemporalFeatureFilter> filters;
};
```

The primary access pattern is:

```text
key + timestamp range + count + optional filters -> matching rows
```

For sequence feature rows, the payload has a fixed compact schema such as:

```text
timestamp, gid, action_type, duration, author_id
```

and query filters can apply to fields like `action_type` or `author_id`. The important part is that timestamp remains the primary timeline key. Secondary filtering is not exposed as a large set of serving records in the prompt or debug output; it is a bounded query predicate over compact row fields.

## How Current MatrixArk Context Indexing Works

Context management added context-native model keys:

```text
ctx:event:{tenant_hash}:{node_hash}
ctx:entity:{tenant_hash}:{node_hash}:{entity_hash}
ctx:summary:{tenant_hash}:{node_hash}:{level}
ctx:embedding:{tenant_hash}:{ref_hash}
ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}        # legacy direct path
ctxidx2:{tenant_hash}:{scope_hash}:{index_name}:{time_bucket_ms}         # compact default path
```

For a `ContextEvent`, ingestion time is the primary timeline:

```text
ctx:event:{tenant_hash}:{node_hash}
  context_timeline_key(ingestion_time_ms, event_id_hash) -> compact ContextEvent
```

For secondary lookup, direct compatibility calls can still write small refs:

```text
ctxidx:{tenant_hash}:event_kind:{event_kind_hash}:{scope_hash}
ctxidx:{tenant_hash}:status:{status_hash}:{scope_hash}
ctxidx:{tenant_hash}:source:{source_hash}:{scope_hash}
ctxidx:{tenant_hash}:entity:{entity_hash}:{scope_hash}
ctxidx:{tenant_hash}:event_time_bucket:{bucket_ms}:{scope_hash}
```

Each ref points back to the primary event row:

```text
{primary_node_hash, primary_event_time_ms, event_id_hash}
```

For example:

```text
ctxidx:2466697514329931826:event_kind:3:7836037686236352053
  00000001782681920521:1121810234980183195
    -> node=2100209595829882121
    -> event_time=1782681920521
    -> event_id=1121810234980183195
```

Retrieval can query several index timelines, intersect event ids, then fetch the exact event rows from `ctx:event:{tenant}:{node}`.

For extracted context events, the C++ native path now writes the same refs into compact bucket posting objects by default:

```text
ctxidx2:{tenant_hash}:{scope_hash}:status:{minute_bucket_ms}
  timestamp_key -> {primary_node_hash, primary_event_time_ms, event_id_hash, index_value_hash=status_hash}
```

Native retrieve follows this path:

```text
query understanding
-> scope filter
-> L0/L1 node traversal
-> compact posting lookup by scope + index_name + time_bucket
-> placement-key candidate fetch from selected node partitions
-> native score / rerank / token-budget pack
```

Broad prefix scan is fallback/debug only and must be visible in telemetry as `broad_scan_used`.

## Why Too Many Indexes Hurt

Index refs are small, but too many still cost real resources:

- More writes per event or resource chunk.
- More pages/blocks to compact and cache.
- More prefix scans and index intersections during retrieval.
- More debug/audit noise.
- More duplicated strings if index names are stored as text in exported records.
- Worse ingestion p95 when PDFs or repos generate many keywords.

For large resources, this becomes `chunks x index_terms_per_chunk`. A 500-chunk PDF with 10 terms per chunk creates 5,000 index refs before embeddings, summaries, and facts are counted.

## Production Target

MatrixArk should use the old TemporalStore style for hot serving:

```text
Context data model = timestamped series
Secondary filters = declared compact fields or native posting families
Query = time range + scope + filter predicates + top-k/candidate cap
```

The serving API should look like:

```text
matrixark_scan_candidates(
  scope_key,
  node_hashes,
  data_models=["event", "entity", "resource_chunk", "skill_section"],
  time_range,
  filters={
    "source_type": ["message", "resource"],
    "event_type": ["approval", "deadline"],
    "resource_type": ["pdf", "md"],
    "entity_type": ["owner", "decision"],
    "keyword_id": [123, 456]
  },
  limit
)
```

and later:

```text
matrixark_retrieve_context_pack(...)
```

so Python sends the query plan and receives a compact candidate list or finished ContextPack.

## Recommended Context Index Families

Keep the hot-path families small and declared:

```text
source_type
event_type
entity_type
resource_type
unit_kind
skill_trigger
skill_tool
keyword_id
relative_path_hash
visibility_scope
```

Do not create unbounded indexes for every arbitrary string. Keywords should be normalized to a capped dictionary and written as `keyword_id`, not long repeated strings.

## How Filtering Should Work

Example query:

```text
"Who approved the Project Aurora GPU request?"
```

Query understanding produces:

```json
{
  "query_type": "current_state",
  "filters": {
    "event_type": ["approval"],
    "entity_type": ["resource_decision", "approval_state"],
    "source_type": ["message", "resource"],
    "keyword_id": ["gpu", "aurora", "approved"]
  },
  "temporal_window": {
    "mode": "latest"
  }
}
```

Native retrieval then does:

```text
1. enforce account/tenant/user/session/shared-resource scope
2. score L0/L1 ContextNode summaries to choose folders
3. query timestamped context series under selected nodes
4. apply compact secondary filters before reading/scoring too many rows
5. score surviving events/entities/chunks/skills
6. pack only selected evidence into ContextPack
```

If using posting refs, retrieval intersects compact refs:

```text
event_type=approval
AND source_type in {message, resource}
AND keyword_id in {gpu, aurora}
```

Then it fetches only those primary rows.

If using feature-row predicates, retrieval calls the feature query path directly:

```text
ctx:event:{tenant}:{node}
  start_ts..end_ts
  filters=[event_type == APPROVAL, source_type == RESOURCE]
```

## What To Change From Today

1. Keep `ContextEvent` timestamp-keyed by ingestion time.
2. Keep `ContextIndexRef` as a native internal posting/ref mechanism, not a verbose serving data model.
3. Stop exporting every index ref into ContextPack/debug pages by default.
4. Cap index fanout per object and per resource import.
5. Add a native C++/Rust candidate scan API that applies scope and filter predicates before returning rows.
6. Move keyword indexing to dictionary ids.
7. Keep detailed index decisions in sampled audit/debug records only when debugging is enabled.

## Compatibility Plan

Do not break existing context tests immediately. Migrate in phases:

1. Current compatibility:
   - keep writing existing `ContextIndexRef` families;
   - keep current query behavior working.
2. Native serving path:
   - add C++ and Rust `matrixark_scan_candidates`;
   - map query-understanding filters to native field ids;
   - use native scan before Python fallback.
3. Compact index representation:
   - store field/value ids rather than repeated strings;
   - hide `ContextIndexRef` from serving ContextPacks.
4. Deprecation:
   - keep verbose `context_index` export only under debug/audit sampling;
   - remove product reliance on `index_name` strings in hot records.

## Decision

Use TemporalStore-native secondary filtering for MatrixArk context management. `ContextIndexRef` should remain an internal compact lookup surface or be replaced by feature-style field predicates where possible. It should not become a large user-visible data model, and it should not scale with arbitrary resource keywords or PDF text.
