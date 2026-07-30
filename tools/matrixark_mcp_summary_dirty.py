#!/usr/bin/env python3
"""Dirty summary scheduling helpers for MatrixArk MCP adapters."""

from __future__ import annotations

from typing import Any, Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        ENABLE_CONTEXT_DEBUG_RECORDS,
        ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS,
        ENABLE_SUMMARY_REFRESH_AUDIT,
        candidate_access_scope,
        scope_matches,
        stable_hash,
    )
    from tools.matrixark_mcp_tree import node_prefixes
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        ENABLE_CONTEXT_DEBUG_RECORDS,
        ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS,
        ENABLE_SUMMARY_REFRESH_AUDIT,
        candidate_access_scope,
        scope_matches,
        stable_hash,
    )
    from matrixark_mcp_tree import node_prefixes


ContextEventTime = Callable[[Json, dict[Any, Json] | None], int]


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _summary_has_text(record: Json) -> bool:
    if record.get("record_type") != "context_summary":
        return False
    return bool(str(record.get("summary_text") or record.get("text") or "").strip())


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
        record = {
            "record_type": "context_summary_dirty",
            "dirty_hash": dirty_hash,
            "node_hash": node_hash,
            "node_path": prefix,
            "scope": scope,
            "status": "pending",
            "created_at_ms": updated_at_ms,
            "updated_at_ms": updated_at_ms,
        }
        if ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS or ENABLE_SUMMARY_REFRESH_AUDIT or ENABLE_CONTEXT_DEBUG_RECORDS:
            record.update(
                {
                    "depth": len(prefix),
                    "dirty_reason": dirty_reason,
                    "source_ref_type": source_ref_type,
                    source_hash_field: source_hash,
                    "changed_ref_count": 1,
                    "propagate_depth": propagate_depth if propagate_depth is not None else len(node_path),
                }
            )
        records.append(record)
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
    skip_dirty_reasons: set[str] | None = None,
    skipped_dirty_reasons: Json | None = None,
) -> dict[int, Json]:
    skip_dirty_reasons = skip_dirty_reasons or set()
    completed_dirty_hashes: set[int] = set()
    summary_nodes_with_text: set[int] = set()
    summary_nodes_empty: set[int] = set()
    node_path_by_hash: dict[int, list[str]] = {}
    for record in records:
        node_hash = _int_value(record.get("node_hash"))
        if node_hash is not None and isinstance(record.get("node_path"), list) and record.get("node_path"):
            node_path_by_hash[node_hash] = [str(part) for part in record.get("node_path", [])]
        dirty_hash = _int_value(record.get("dirty_hash"))
        if (
            dirty_hash is not None
            and record.get("record_type") in {"context_summary_refresh_audit", "context_summary_dirty"}
            and record.get("status") in {"refreshed", "completed"}
        ):
            completed_dirty_hashes.add(dirty_hash)
        if record.get("record_type") != "context_summary":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        if node_hash is None:
            continue
        if _summary_has_text(record):
            summary_nodes_with_text.add(node_hash)
            summary_nodes_empty.discard(node_hash)
        elif node_hash not in summary_nodes_with_text:
            summary_nodes_empty.add(node_hash)

    pending_by_node: dict[int, Json] = {}
    for record in records:
        if record.get("record_type") != "context_summary_dirty":
            continue
        if not scope_matches(candidate_access_scope(record), scope):
            continue
        dirty_hash = _int_value(record.get("dirty_hash"))
        node_hash = _int_value(record.get("node_hash"))
        if dirty_hash is None or node_hash is None:
            continue
        if dirty_hash in completed_dirty_hashes:
            continue
        dirty_reason = str(record.get("dirty_reason") or "").strip()
        if dirty_reason in skip_dirty_reasons:
            if skipped_dirty_reasons is not None:
                skipped_dirty_reasons[dirty_reason] = int(skipped_dirty_reasons.get(dirty_reason, 0)) + 1
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
        event_path = [str(part) for part in record.get("node_path", [])]
        if not event_path:
            event_path = node_path_by_hash.get(event_node_hash, [])
        event_path_by_node[event_node_hash] = event_path
        event_scope_by_node[event_node_hash] = candidate_access_scope(record)
        event_time = context_event_ingestion_time_ms(record, debug_by_ref)
        if event_time > 0:
            existing_time = oldest_event_time_by_node.get(event_node_hash)
            if existing_time is None or event_time < existing_time:
                oldest_event_time_by_node[event_node_hash] = event_time
    for node_hash, event_count in sorted(event_counts_by_node.items(), key=lambda item: item[1], reverse=True):
        if len(pending_by_node) >= limit:
            break
        if node_hash in pending_by_node or node_hash in summary_nodes_with_text:
            continue
        dirty_reason = "missing_or_empty_summary"
        if dirty_reason in skip_dirty_reasons:
            if skipped_dirty_reasons is not None:
                skipped_dirty_reasons[dirty_reason] = int(skipped_dirty_reasons.get(dirty_reason, 0)) + 1
            continue
        node_path = event_path_by_node.get(node_hash, [])
        if not node_path:
            continue
        oldest_event_time = oldest_event_time_by_node.get(node_hash, 0)
        synthetic_dirty_hash = stable_hash(f"missing_or_empty_summary:{node_hash}:{event_count}:{oldest_event_time}")
        if synthetic_dirty_hash in completed_dirty_hashes:
            continue
        synthetic_dirty = {
            "record_type": "context_summary_dirty",
            "dirty_hash": synthetic_dirty_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": event_scope_by_node.get(node_hash, scope),
            "status": "pending",
            "created_at_ms": refreshed_at_ms,
            "updated_at_ms": refreshed_at_ms,
        }
        if ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS or ENABLE_SUMMARY_REFRESH_AUDIT or ENABLE_CONTEXT_DEBUG_RECORDS:
            synthetic_dirty.update(
                {
                    "depth": len(node_path),
                    "dirty_reason": dirty_reason,
                    "source_ref_type": "summary_state",
                    "changed_ref_count": event_count,
                    "empty_summary_seen": node_hash in summary_nodes_empty,
                    "propagate_depth": 0,
                }
            )
        pending_by_node[node_hash] = synthetic_dirty
    cold_cutoff_ms = refreshed_at_ms - max(0, int(min_compression_event_age_ms))
    for node_hash, event_count in sorted(event_counts_by_node.items(), key=lambda item: item[1], reverse=True):
        if len(pending_by_node) >= limit:
            break
        if node_hash in pending_by_node:
            continue
        dirty_reason = "scheduled_time_compression"
        if dirty_reason in skip_dirty_reasons:
            if skipped_dirty_reasons is not None:
                skipped_dirty_reasons[dirty_reason] = int(skipped_dirty_reasons.get(dirty_reason, 0)) + 1
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
        synthetic_dirty = {
            "record_type": "context_summary_dirty",
            "dirty_hash": synthetic_dirty_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": event_scope_by_node.get(node_hash, scope),
            "status": "pending",
            "created_at_ms": refreshed_at_ms,
            "updated_at_ms": refreshed_at_ms,
        }
        if ENABLE_SUMMARY_DIRTY_DEBUG_FIELDS or ENABLE_SUMMARY_REFRESH_AUDIT or ENABLE_CONTEXT_DEBUG_RECORDS:
            synthetic_dirty.update(
                    {
                        "depth": len(node_path),
                        "dirty_reason": dirty_reason,
                        "source_ref_type": "event_window",
                        "changed_ref_count": event_count,
                        "propagate_depth": 0,
                }
            )
        pending_by_node[node_hash] = synthetic_dirty
    return pending_by_node
