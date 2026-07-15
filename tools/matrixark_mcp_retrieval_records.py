#!/usr/bin/env python3
"""Local retrieval record filtering helpers for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        canonical_scope_key,
        scope_matches,
        session_scope_mode,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        canonical_scope_key,
        scope_matches,
        session_scope_mode,
    )


RETRIEVAL_HOT_RECORD_TYPES = {
    "context_compression_event",
    "context_embedding",
    "context_entity",
    "context_event",
    "context_index",
    "context_segment",
    "context_summary",
    "resource_chunk",
    "resource_manifest",
    "skill_registry_update",
    "skill_section",
}

_SCOPE_VIA_CANDIDATE_ACCESS = {
    "context_embedding",
    "context_index",
    "context_summary",
    "resource_manifest",
    "skill_registry_update",
}


def retrieval_records_cache_key(
    *,
    generation: int,
    scope: Json,
    allowed_types: set[str],
    secondary_index_groups: list[set[str]] | None = None,
    selected_node_hashes: set[int] | None = None,
) -> tuple[object, ...]:
    secondary_key = tuple(sorted(tuple(sorted(group)) for group in (secondary_index_groups or [])))
    selected_key = tuple(sorted(int(item) for item in (selected_node_hashes or set())))
    return (
        generation,
        canonical_scope_key(scope),
        session_scope_mode(scope),
        tuple(sorted(allowed_types)),
        secondary_key,
        selected_key,
    )


def filter_retrieval_records(
    records: list[Json],
    *,
    scope: Json,
    allowed_types: set[str],
    selected_node_hashes: set[int] | None = None,
) -> tuple[list[Json], Json]:
    filtered: list[Json] = []
    scanned = 0
    dropped_type = 0
    dropped_scope = 0
    dropped_node = 0
    selected_nodes = selected_node_hashes or set()
    for record in records:
        scanned += 1
        record_type = str(record.get("record_type") or "")
        if record_type not in allowed_types:
            dropped_type += 1
            continue
        if selected_nodes:
            try:
                record_node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                record_node_hash = None
            if record_node_hash is not None and record_node_hash not in selected_nodes:
                dropped_node += 1
                continue
        if record_type in _SCOPE_VIA_CANDIDATE_ACCESS:
            if not scope_matches(candidate_access_scope(record), scope):
                dropped_scope += 1
                continue
        elif not access_scope_matches_before_scoring(record, scope):
            dropped_scope += 1
            continue
        filtered.append(record)
    return filtered, {
        "scanned_records": scanned,
        "returned_records": len(filtered),
        "dropped_by_type": dropped_type,
        "dropped_by_scope": dropped_scope,
        "dropped_by_node": dropped_node,
    }
