#!/usr/bin/env python3
"""Dirty summary scheduling helpers for MatrixArk MCP adapters."""

from __future__ import annotations

from typing import Any, Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        scope_matches,
        stable_hash,
    )
    from tools.matrixark_mcp_tree import node_prefixes
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        scope_matches,
        stable_hash,
    )
    from matrixark_mcp_tree import node_prefixes


ContextEventTime = Callable[[Json, dict[Any, Json] | None], int]


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


def mark_node_summary_dirty(
    *,
    append_many: Callable[[list[Json]], None],
    node_path: list[str],
    scope: Json,
    updated_at_ms: int,
    source_ref_type: str,
    source_hash_field: str,
    source_hash: int,
    dirty_reason: str = "new_event",
    propagate_depth: int | None = None,
) -> list[int]:
    dirty_hashes, records = node_summary_dirty_records(
        node_path=node_path,
        scope=scope,
        updated_at_ms=updated_at_ms,
        source_ref_type=source_ref_type,
        source_hash_field=source_hash_field,
        source_hash=source_hash,
        dirty_reason=dirty_reason,
        propagate_depth=propagate_depth,
    )
    append_many(records)
    return dirty_hashes


def pending_dirty_node_records(
    *,
    records: list[Json],
    scope: Json,
    limit: int,
    refreshed_at_ms: int,
    max_raw_events_per_node: int,
    min_compression_event_age_ms: int,
    context_event_ingestion_time_ms: ContextEventTime,
) -> dict[int, Json]:
    completed_dirty_hashes = {
        int(record.get("dirty_hash"))
        for record in records
        if record.get("record_type") in {"context_summary_refresh_audit", "context_summary_dirty"}
        and record.get("status") in {"refreshed", "completed"}
        and record.get("dirty_hash") is not None
    }
    pending_by_node: dict[int, Json] = {}
    for record in records:
        if record.get("record_type") != "context_summary_dirty":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        try:
            dirty_hash = int(record.get("dirty_hash"))
            node_hash = int(record.get("node_hash"))
        except (TypeError, ValueError):
            continue
        if dirty_hash in completed_dirty_hashes:
            continue
        current = pending_by_node.get(node_hash)
        if current is None or int(record.get("updated_at_ms") or 0) >= int(current.get("updated_at_ms") or 0):
            pending_by_node[node_hash] = record
    if len(pending_by_node) >= limit:
        return pending_by_node

    event_counts_by_node: dict[int, int] = {}
    event_path_by_node: dict[int, list[str]] = {}
    event_scope_by_node: dict[int, Json] = {}
    oldest_event_time_by_node: dict[int, int] = {}
    debug_by_ref = {
        record.get("ref_hash"): record.get("debug_payload", {})
        for record in records
        if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
    }
    for record in records:
        if record.get("record_type") != "context_event":
            continue
        if record.get("source_chunk_hash"):
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        try:
            event_node_hash = int(record.get("node_hash"))
        except (TypeError, ValueError):
            continue
        event_counts_by_node[event_node_hash] = event_counts_by_node.get(event_node_hash, 0) + 1
        event_path_by_node[event_node_hash] = [str(part) for part in record.get("node_path", [])]
        event_scope_by_node[event_node_hash] = candidate_access_scope(record)
        event_time = context_event_ingestion_time_ms(record, debug_by_ref)
        if event_time > 0:
            existing_time = oldest_event_time_by_node.get(event_node_hash)
            if existing_time is None or event_time < existing_time:
                oldest_event_time_by_node[event_node_hash] = event_time
    cold_cutoff_ms = refreshed_at_ms - max(0, int(min_compression_event_age_ms))
    for node_hash, event_count in sorted(event_counts_by_node.items(), key=lambda item: item[1], reverse=True):
        if len(pending_by_node) >= limit:
            break
        if node_hash in pending_by_node:
            continue
        if event_count <= max_raw_events_per_node:
            continue
        if min_compression_event_age_ms > 0 and oldest_event_time_by_node.get(node_hash, refreshed_at_ms) > cold_cutoff_ms:
            continue
        node_path = event_path_by_node.get(node_hash, [])
        if not node_path:
            continue
        synthetic_dirty_hash = stable_hash(
            f"scheduled_time_compression:{node_hash}:{event_count}:{oldest_event_time_by_node.get(node_hash, 0)}:{refreshed_at_ms}"
        )
        if synthetic_dirty_hash in completed_dirty_hashes:
            continue
        pending_by_node[node_hash] = {
            "record_type": "context_summary_dirty",
            "dirty_hash": synthetic_dirty_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "depth": len(node_path),
            "dirty_reason": "scheduled_time_compression",
            "source_ref_type": "event_window",
            "changed_ref_count": event_count,
            "propagate_depth": 0,
            "scope": event_scope_by_node.get(node_hash, scope),
            "status": "pending",
            "created_at_ms": refreshed_at_ms,
            "updated_at_ms": refreshed_at_ms,
        }
    return pending_by_node
