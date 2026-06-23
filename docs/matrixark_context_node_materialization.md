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

## Records Written

For a node path like:

```text
company_a / infra_team / project_1 / approvals
```

MatrixArk writes four `context_node` records:

```text
company_a
company_a / infra_team
company_a / infra_team / project_1
company_a / infra_team / project_1 / approvals
```

And three `context_child_ref` records:

```text
company_a -> infra_team
infra_team -> project_1
project_1 -> approvals
```

The write is idempotent. A later event on the same path creates zero new nodes and zero new child refs.

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
    "nodes_created": 4,
    "child_refs_created": 3,
    "node_hashes": [123]
  }
}
```

For repeated writes to an existing path, `nodes_created` and `child_refs_created` are `0`.

## Scope

This is a runtime implementation step. It does not require a new public `create_node` API, and it does not require customers to define schemas or tree rules.
