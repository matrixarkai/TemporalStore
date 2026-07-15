#!/usr/bin/env python3
"""ContextNode materialization helpers for MatrixArk MCP adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, stable_hash

try:
    from tools.matrixark_mcp_tree import node_prefixes
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_tree import node_prefixes


def ensure_context_node_path(adapter: object, *, node_path: list[str], scope: Json, updated_at_ms: int) -> Json:
    prefixes = node_prefixes(node_path)
    if not prefixes:
        return {"nodes_created": 0, "child_refs_created": 0, "node_hashes": []}

    adapter._ensure_context_node_cache_loaded()
    existing_nodes = adapter._context_node_hashes
    existing_child_refs = adapter._context_child_ref_hashes
    node_hashes: list[int] = []
    nodes_created = 0
    child_refs_created = 0
    for prefix in prefixes:
        node_hash = stable_hash("/".join(prefix))
        node_hashes.append(node_hash)
        parent_path = prefix[:-1]
        parent_hash = stable_hash("/".join(parent_path)) if parent_path else 0
        if node_hash not in existing_nodes:
            adapter.append(
                {
                    "record_type": "context_node",
                    "node_hash": node_hash,
                    "parent_hash": parent_hash,
                    "node_name": prefix[-1],
                    "node_path": prefix,
                    "depth": len(prefix),
                    "scope": scope,
                    "created_at_ms": updated_at_ms,
                    "updated_at_ms": updated_at_ms,
                }
            )
            existing_nodes.add(node_hash)
            nodes_created += 1
        if parent_path:
            child_ref_hash = stable_hash(f"child:{parent_hash}:{node_hash}")
            if child_ref_hash not in existing_child_refs:
                adapter.append(
                    {
                        "record_type": "context_child_ref",
                        "child_ref_hash": child_ref_hash,
                        "parent_hash": parent_hash,
                        "child_hash": node_hash,
                        "child_name": prefix[-1],
                        "parent_path": parent_path,
                        "child_path": prefix,
                        "depth": len(prefix),
                        "scope": scope,
                        "created_at_ms": updated_at_ms,
                        "updated_at_ms": updated_at_ms,
                    }
                )
                existing_child_refs.add(child_ref_hash)
                child_refs_created += 1
    return {
        "nodes_created": nodes_created,
        "child_refs_created": child_refs_created,
        "node_hashes": node_hashes,
    }
