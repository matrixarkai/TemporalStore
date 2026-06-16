# Context Extension

This module is the first TemporalStore-native substrate for MatrixArk LLM context serving.

It intentionally keeps the C++ serving schema small:

```text
ContextNodeModel  -> Hash page object with a compact node metadata field
ContextEventModel -> Feature page object keyed by event_time_ms plus a small hash suffix
ContextIndexModel -> Feature page object keyed by event_time_ms plus a small hash suffix
ContextAuditModel -> Feature page object keyed by request_time_ms plus a small hash suffix
ContextDirtyModel -> Feature page object keyed by event_time_ms plus a small hash suffix
```

MatrixArk should perform LLM extraction, canonicalization, node selection, schema enforcement, and token-budgeted context packing above this module. The C++ module stores and retrieves already-compiled context records.

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
```

The object key carries tenant, node, index, scope, and session identity so hot values do not need to duplicate those fields.

Timeline keys use a small fanout suffix to avoid overwriting multiple records that arrive in the same millisecond. The payload still carries the original millisecond timestamp.

## Write Pattern

A typical useful input writes:

```text
1. one ContextEvent under the primary node
2. zero to three ContextIndexRef records, if the caller chooses to index
3. one async SummaryDirty marker, if the caller chooses to enqueue summary refresh
4. one ContextPackAudit record after query serving
```

Do not duplicate the same event under many related nodes. Use `related_node_hashes` or secondary index refs.

`WRITE_EVENT` intentionally does not update node summaries, parent summaries, or dirty markers. Event writes must stay lightweight. MatrixArk should call `MARK_SUMMARY_DIRTY` asynchronously, or let a background worker derive dirty nodes from event timelines.

## Query Pattern

Serving queries should prefer:

```text
scope/time/filter narrowing
-> TemporalStore timestamp scan or small secondary index
-> validity/freshness filtering
-> optional vector/object sidecar lookup outside this module
-> token-budgeted ContextPack
```

This module does not run LLM extraction, graph reasoning, ANN search, or prompt rendering.
