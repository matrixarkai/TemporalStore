#!/usr/bin/env python3
"""Latest-value compaction helpers for MatrixArk local records."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_identity import canonical_scope_key
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import canonical_scope_key

Json = dict[str, Any]


def latest_value_record_key(record: Json) -> tuple[Any, ...] | None:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_node":
        return (record_type, record.get("node_hash"))
    if record_type == "context_child_ref":
        return (record_type, record.get("child_ref_hash"))
    if record_type == "context_summary":
        return (record_type, record.get("summary_type"), record.get("summary_hash") or record.get("node_hash"))
    if record_type == "context_embedding":
        return (record_type, record.get("embedding_type"), record.get("ref_type"), record.get("ref_hash"))
    if record_type == "context_index":
        scope = record.get("scope", {})
        scope_key = (
            record.get("scope_key") or canonical_scope_key(scope)
            if isinstance(scope, dict)
            else record.get("scope_key")
        )
        return (
            record_type,
            record.get("index_name"),
            scope_key,
            record.get("node_hash") or record.get("node_id"),
            record.get("capability") or record.get("ref_type"),
            record.get("timestamp_key_ms") or record.get("updated_at_ms"),
        )
    if record_type == "context_entity":
        return (record_type, record.get("entity_hash"))
    if record_type == "context_summary_dirty":
        return (record_type, record.get("dirty_hash"))
    if record_type == "resource_manifest":
        return (record_type, record.get("resource_hash"))
    if record_type == "skill_registry_update":
        return (record_type, record.get("skill_hash"))
    if record_type == "resource_import_task":
        return (record_type, record.get("resource_import_task_hash"))
    return None


def compact_latest_value_records(records: list[Json]) -> list[Json]:
    latest: dict[tuple[Any, ...], Json] = {}
    output: list[Json] = []
    latest_positions: dict[tuple[Any, ...], int] = {}
    for record in records:
        key = latest_value_record_key(record)
        if key is None or any(part in (None, "") for part in key[1:]):
            output.append(record)
            continue
        existing = latest.get(key)
        if existing is None:
            latest[key] = record
            latest_positions[key] = len(output)
            output.append(record)
            continue
        record_updated_at = int(record.get("updated_at_ms") or record.get("created_at_ms") or 0)
        existing_updated_at = int(existing.get("updated_at_ms") or existing.get("created_at_ms") or 0)
        if record_updated_at >= existing_updated_at:
            latest[key] = record
            output[latest_positions[key]] = record
    return output
