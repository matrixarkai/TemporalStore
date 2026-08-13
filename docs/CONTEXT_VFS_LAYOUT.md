# TemporalStore Context Virtual Filesystem Layout

The context store presents a virtual filesystem — the `tsctx://` URI namespace over the
`ContextNode` tree — as its answer to the baseline memory system's `baseline://`. This note documents the
**current** layout (families, URIs, and the tree) and how physical records back it.

TemporalStore keeps context in first-class Context models rather than a separate
`baseline://` filesystem; refs use `tsctx://`, and L0/L1/L2 layering mirrors the baseline memory system's
abstract / overview / full-content tiers.

## 1. The tree (the "directories") — ContextNode + ContextChildRef

Callers do not create nodes; MatrixArk materializes the path. The default is intentionally
shallow (`user:<id>/session:<id>`); `metadata.node_path` can make it deeper.

```
tenant:<tenant_id>                       # isolation root (scope, not a stored tree level by default)
└── user:<user_id>                       # default materialized root
    ├── session:<session_id>             # default leaf (events / segments / entities attach here)
    │   └── conversation:<id>            # optional deeper level
    ├── profile/...                      # durable cross-session entities (recommended home)
    └── resources/<project>/<doc>        # user-scoped resource chunks
tenant:<id>/shared/resources            # governed shared context
tenant:<id>/shared/skills
global/resources/public_docs            # global
```

Directory **edges** are `ContextChildRef` records (`ctx:child:...`): this adjacency index is
what makes "list children" cheap without scanning every node. Events, entities, summaries,
embeddings, and chunks are *attached records* under a node, not tree children.

## 2. The `tsctx://` URI scheme (the addressable "files")

```
tsctx://tenant/{tenant}/node/{node}                          # a node
tsctx://tenant/{tenant}/node/{node}/l0                       # L0 routing summary   (tier)
tsctx://tenant/{tenant}/node/{node}/l1                       # L1 key-fact summary  (tier)
tsctx://tenant/{tenant}/node/{node}/event/{event_time_ms}    # L2 raw event evidence
tsctx://tenant/{tenant}/node/{node}/source/{source_id}       # extracted-source ref
tsctx://tenant/{tenant}/model/{provider}/source/{source_id}  # model-derived ref
```

L0/L1/L2 correspond to the baseline memory system's abstract / overview / full-content layering.

## 3. Physical backing — the `ctx:*` object-key families ("inodes")

Every key is **tenant-scoped**: `{tenant_hash}` is always the first segment, so tenant
isolation is carried in the key itself.

| family      | key format                                                | model                                   | page type            |
| ----------- | --------------------------------------------------------- | --------------------------------------- | -------------------- |
| node        | `ctx:node:{t}:{node}`                                      | ContextNode (l0/l1 meta)                | hash / object        |
| event       | `ctx:event:{t}:{node}`                                     | ContextEvent timeline                   | feature (ts-keyed)   |
| index       | `ctxidx:{t}:{index_name}:{value_hash}:{scope_hash}`        | secondary-index postings                | feature              |
| audit       | `ctx:audit:{t}:{session_hash}`                             | ContextPackAudit                        | feature              |
| entity      | `ctx:entity:{t}:{node}:{entity_hash}` (coll. `ctx:entity:{t}:{node}`) | ContextEntity                | hash / object        |
| child       | `ctx:child:{t}:{parent}`                                   | ContextChildRef (tree edges)            | feature              |
| embedding   | `ctx:embedding:{t}:{ref_hash}`                             | ContextEmbedding vectors                | hash / object        |
| summary     | `ctx:summary:{t}:{node}:{level}`                           | ContextSummary (L0/L1)                  | feature              |
| compress    | `ctx:compress:{t}:{node}`                                  | ContextCompressionEvent                 | feature              |
| dirty       | `ctx:dirty:{t}:{node}`                                     | SummaryDirtyMarker                      | **in-memory only**   |

Key derivation lives in `crates/temporalstore-rust/src/engine/context.rs`
(`context_node_key`, `context_event_key`, `context_index_key`, `context_audit_key`,
`context_entity_key` / `context_entity_collection_key`, `context_child_key`,
`context_embedding_key`, `context_summary_key`, `context_compression_key`,
`context_dirty_key`). URI construction (`tsctx://...`) lives in
`crates/temporalstore-rust/src/context_workflow.rs`.

## 4. Current notes

- **`ctx:dirty` is no longer a persisted family.** The key is still computed to index an
  in-memory coalescing `DirtyIndex` (`ShardState::context_dirty_index`), but it was removed
  from the storage-page / compaction / page-layout / records paths. It is ephemeral, not a
  VFS "file" on disk. See [`CONTEXT_DIRTY_INMEMORY.md`](CONTEXT_DIRTY_INMEMORY.md).
- **`ctx:compress`** is now populated automatically by the configurable temporal-compression
  trigger (per-node, keep-recent window, coalesced watermark), not only by manual writes.
- Physical placement can be sharded; for locality, records for the same hot node/session use
  the same route key, while shared/global resources can be partitioned and quota-limited
  separately.

## 5. Summary

The context VFS is `tsctx://tenant/{id}/node/{id}/{l0|l1|event/...}` addressing over a
`tenant → user → session (/conversation | /profile | /resources)` tree, physically stored as
~10 tenant-scoped `ctx:*` key families that reuse hash/feature page primitives — with
summary-dirty tracking now held in memory rather than on disk.
