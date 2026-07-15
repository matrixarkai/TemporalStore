#!/usr/bin/env python3
"""Context summary runtime helpers for MatrixArk MCP adapters."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, scope_matches, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, scope_matches, stable_hash

try:
    from tools.matrixark_mcp_tree import node_path_tuple, node_prefixes, starts_with_path
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_tree import node_path_tuple, node_prefixes, starts_with_path


def node_summary_source_records(
    *,
    records: list[Json],
    node_path: list[str],
    scope: Json,
    node_hash: int | None = None,
    max_events: int = 8,
    max_child_summaries: int = 8,
    max_entity_states: int = 6,
    max_operator_states: int = 4,
) -> tuple[list[Json], list[Json], list[Json], list[Json], Json]:
    prefix = node_path_tuple(node_path)
    target_node_hash = int(node_hash) if node_hash is not None else stable_hash("/".join(node_path))
    direct_child_hashes: set[int] = set()
    for record in records:
        if record.get("record_type") != "context_child_ref":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        try:
            parent_hash = int(record.get("parent_hash") or 0)
            child_hash = int(record.get("child_hash") or 0)
        except (TypeError, ValueError):
            continue
        if parent_hash == target_node_hash and child_hash:
            direct_child_hashes.add(child_hash)

    child_summaries: list[Json] = []
    entity_states: list[Json] = []
    operator_states: list[Json] = []
    same_node_events: list[Json] = []
    seen_summary_keys: set[tuple[int, str]] = set()
    seen_entity_hashes: set[int] = set()
    seen_operator_hashes: set[int] = set()
    summary_types = {"node_l0", "node_l1", "batch_l0", "session_l0", "resource_l0", "skill_l0"}
    operator_record_types = {"context_compression_event"}
    for record in reversed(records):
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        record_type = str(record.get("record_type") or "")
        try:
            record_node_hash = int(record.get("node_hash") or 0)
        except (TypeError, ValueError):
            record_node_hash = 0
        record_path = node_path_tuple(record.get("node_path", []))
        is_same_node = record_node_hash == target_node_hash or (bool(record_path) and record_path == prefix)
        is_direct_child = record_node_hash in direct_child_hashes or (
            bool(record_path) and starts_with_path(record_path, prefix) and len(record_path) == len(prefix) + 1
        )
        if record_type == "context_summary" and record.get("summary_type") in summary_types:
            if len(child_summaries) >= max_child_summaries or not is_direct_child:
                continue
            key = (record_node_hash, str(record.get("summary_type", "")))
            if key in seen_summary_keys:
                continue
            seen_summary_keys.add(key)
            child_summaries.append(record)
            continue
        if record_type == "context_entity" and (is_same_node or is_direct_child):
            if len(entity_states) >= max_entity_states:
                continue
            try:
                entity_hash = int(record.get("entity_hash") or 0)
            except (TypeError, ValueError):
                entity_hash = 0
            if entity_hash and entity_hash in seen_entity_hashes:
                continue
            if entity_hash:
                seen_entity_hashes.add(entity_hash)
            entity_states.append(record)
            continue
        if record_type in operator_record_types and (is_same_node or is_direct_child):
            if len(operator_states) >= max_operator_states:
                continue
            try:
                operator_hash = int(record.get("compression_id_hash") or record.get("ref_hash") or 0)
            except (TypeError, ValueError):
                operator_hash = 0
            if operator_hash and operator_hash in seen_operator_hashes:
                continue
            if operator_hash:
                seen_operator_hashes.add(operator_hash)
            operator_states.append(record)
            continue
        if record_type == "context_event" and is_same_node and len(same_node_events) < max_events:
            same_node_events.append(record)

    use_direct_events = not child_summaries
    events = same_node_events if use_direct_events else []
    policy = {
        "source_policy": "child_summaries_plus_state" if child_summaries else "direct_events_fallback",
        "raw_recursive_leaf_event_scan": False,
        "direct_child_count": len(direct_child_hashes),
        "used_direct_event_count": len(events),
        "used_child_summary_count": len(child_summaries),
        "used_entity_state_count": len(entity_states),
        "used_operator_state_count": len(operator_states),
    }
    return (
        list(reversed(events[:max_events])),
        list(reversed(child_summaries[:max_child_summaries])),
        list(reversed(entity_states[:max_entity_states])),
        list(reversed(operator_states[:max_operator_states])),
        policy,
    )


def node_summary_dirty_records(
    *,
    node_path: list[str],
    scope: Json,
    updated_at_ms: int,
    source_ref_type: str,
    source_hash_field: str,
    source_hash: int,
    dirty_reason: str = "new_event",
    propagate_depth: int | None = None,
) -> tuple[list[int], list[Json]]:
    prefixes = node_prefixes(node_path)
    if propagate_depth is not None and propagate_depth >= 0:
        prefixes = prefixes[max(0, len(prefixes) - propagate_depth - 1) :]
    dirty_hashes: list[int] = []
    records: list[Json] = []
    for prefix in prefixes:
        node_hash = stable_hash("/".join(prefix))
        dirty_hash = stable_hash(
            f"summary_dirty:{node_hash}:{dirty_reason}:{source_ref_type}:{source_hash}:{updated_at_ms}"
        )
        dirty_hashes.append(dirty_hash)
        records.append(
            {
                "record_type": "context_summary_dirty",
                "dirty_hash": dirty_hash,
                "node_hash": node_hash,
                "node_path": prefix,
                "depth": len(prefix),
                "dirty_reason": dirty_reason,
                "source_ref_type": source_ref_type,
                source_hash_field: source_hash,
                "changed_ref_count": 1,
                "propagate_depth": propagate_depth if propagate_depth is not None else len(node_path),
                "scope": scope,
                "status": "pending",
                "created_at_ms": updated_at_ms,
                "updated_at_ms": updated_at_ms,
            }
        )
    return dirty_hashes, records
