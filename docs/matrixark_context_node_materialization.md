# MatrixArk ContextNode Materialization

Date: 2026-06-23

## Decision

MatrixArk callers do not manually create `ContextNode` objects in v1. They send messages, resources, feedback, and optional hints such as `metadata.node_path`.

MatrixArk now materializes the tree internally:

```text
incoming message or batch
-> normalize or infer node_path
-> ensure every prefix exists as context_node
-> ensure every parent/child edge exists as context_child_ref
-> write event/entity/segment/summary/embedding/index records at the selected node
```

## Default Records Written

If the caller does not provide `metadata.node_path`, MatrixArk now keeps the default path intentionally shallow:

```text
user:<user_id> / session:<session_id>
```

That creates two `context_node` records and one `context_child_ref`:

```text
user:alice
user:alice / session:thread-1
user:alice -> session:thread-1
```

Account and tenant remain access-control fields in `scope`; they are not automatically added as tree levels. This avoids making the default tree too deep and too product-specific.

## Custom Paths

If the caller or hook provides `metadata.node_path`, MatrixArk materializes exactly that path. For example:

```text
project_1 / approvals / gpu_purchase
```

creates three `context_node` records and two `context_child_ref` records. The write is idempotent. A later event on the same path creates zero new nodes and zero new child refs.

## Why This Matters

Previously the Python runtime mostly inferred tree structure from `node_path` prefixes and stored summaries/embeddings for those prefixes. That was useful for traversal, but the tree itself was not an explicit record model.

Now the runtime keeps the same simple customer API while making the tree explicit enough for:

- filesystem-like browsing in the UI;
- native C++ `list children` APIs later;
- layer-by-layer traversal using L0/L1 summary embeddings;
- node metadata and access-control inspection;
- lower write amplification because nodes and edges are only created once.

## API Behavior

`matrixark_ingest` and `matrixark_batch_extract` now include:

```json
{
  "node_materialization": {
    "nodes_created": 2,
    "child_refs_created": 1,
    "node_hashes": [123]
  }
}
```

For repeated writes to an existing path, `nodes_created` and `child_refs_created` are `0`.

## Scope

This is a runtime implementation step. It does not require a new public `create_node` API, and it does not require customers to define schemas or tree rules.
