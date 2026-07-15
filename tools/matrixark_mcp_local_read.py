#!/usr/bin/env python3
"""Local MatrixArk read and retrieval-record helpers."""

from __future__ import annotations

import json
from typing import Any

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_latest_values import compact_latest_value_records
    from tools import matrixark_mcp_local_cache as local_cache_helpers
    from tools import matrixark_mcp_retrieval_records as retrieval_record_helpers
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_latest_values import compact_latest_value_records
    import matrixark_mcp_local_cache as local_cache_helpers
    import matrixark_mcp_retrieval_records as retrieval_record_helpers


def recent_records(adapter: Any, limit: int = 128, *, copy_slice: bool = True) -> list[Json]:
    limit = max(1, int(limit or 1))
    records = read_all(adapter)
    if len(records) <= limit:
        return records
    return list(records[-limit:]) if copy_slice else records[-limit:]


def read_all(adapter: Any) -> list[Json]:
    try:
        adapter.event_log.stat()
    except FileNotFoundError:
        local_cache_helpers.clear_read_cache_for_missing_log(adapter)
        return []
    records = []
    with adapter._event_log_lock:
        with adapter.event_log.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return compact_latest_value_records(records)


def retrieval_records(
    adapter: Any,
    *,
    scope: Json,
    record_types: set[str] | None,
    secondary_index_groups: list[set[str]] | None,
    selected_node_hashes: set[int] | None,
    allow_broad_scan_fallback: bool | None,
    hot_record_types: set[str],
) -> Json:
    allowed_types = record_types or hot_record_types
    cache_key = retrieval_record_helpers.retrieval_records_cache_key(
        generation=adapter._retrieval_records_cache_generation,
        scope=scope,
        allowed_types=allowed_types,
        secondary_index_groups=secondary_index_groups,
        selected_node_hashes=selected_node_hashes,
    )
    with adapter._retrieval_records_cache_lock:
        cached = adapter._retrieval_records_cache.get(cache_key)
        if cached is not None:
            scan_stats = dict(cached.get("scan_stats", {}))
            scan_stats["cache_hit"] = True
            return {"records": cached.get("records", []), "scan_stats": scan_stats}
    filtered, filter_stats = retrieval_record_helpers.filter_retrieval_records(
        read_all(adapter),
        scope=scope,
        allowed_types=allowed_types,
        selected_node_hashes=selected_node_hashes,
    )
    result = {
        "records": filtered,
        "scan_stats": {
            "backend": getattr(adapter, "_backend_label", lambda: "local")(),
            "execution_mode": "adapter_prefilter_cached",
            "native_pushdown": False,
            "broad_scan_fallback_allowed": True if allow_broad_scan_fallback is None else bool(allow_broad_scan_fallback),
            "broad_scan_used": True,
            "broad_scan_reason": "local_reference_adapter",
            "record_types": sorted(allowed_types),
            **filter_stats,
            "secondary_index_groups_supplied": len(secondary_index_groups or []),
            "selected_node_hashes_supplied": len(selected_node_hashes or set()),
        },
    }
    with adapter._retrieval_records_cache_lock:
        adapter._retrieval_records_cache[cache_key] = result
    return result
