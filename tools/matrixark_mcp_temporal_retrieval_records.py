#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Direct TemporalStore retrieval-record runtime for MatrixArk adapters."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_direct_cache_state import (
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_direct_cache_state import (
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
    )

try:
    from tools.matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        canonical_scope_key,
        json,
        scope_matches,
    )
    from tools.matrixark_mcp_retrieval_records import RETRIEVAL_HOT_RECORD_TYPES
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        canonical_scope_key,
        json,
        scope_matches,
    )
    from matrixark_mcp_retrieval_records import RETRIEVAL_HOT_RECORD_TYPES


def native_locations_for_selected_nodes(target: Any, *, scope: Json, selected_node_hashes: set[int]) -> Json:
    self = target
    batch_hget = getattr(self._client, "batch_hget", None)
    scope_key = canonical_scope_key(scope)
    if not callable(batch_hget) or not scope_key or not selected_node_hashes:
        return {"locations": [], "locator_rows": 0, "eligible": False, "reason": "missing_scope_or_nodes"}
    entries = [
        {"key": self._context_placement_lookup_key(scope_key), "field": str(node_hash)}
        for node_hash in sorted(selected_node_hashes)
        if node_hash
    ]
    if not entries:
        return {"locations": [], "locator_rows": 0, "eligible": False, "reason": "empty_node_set"}
    try:
        rows = batch_hget(entries)
    except Exception as exc:
        return {"locations": [], "locator_rows": 0, "eligible": False, "reason": f"placement_lookup_failed:{exc}"}
    locations: list[Json] = []
    resource_versions: set[str] = set()
    seen: set[tuple[str, str]] = set()
    locator_rows = 0
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        value = row.get("value")
        if not value:
            continue
        try:
            decoded = json.loads(str(value))
        except Exception:
            continue
        raw_locations = decoded.get("locations", []) if isinstance(decoded, dict) else []
        raw_versions = decoded.get("resource_versions", []) if isinstance(decoded, dict) else []
        if isinstance(raw_versions, list):
            resource_versions.update(str(value) for value in raw_versions if str(value))
        if not isinstance(raw_locations, list):
            continue
        locator_rows += 1
        for location in raw_locations:
            if not isinstance(location, dict):
                continue
            key = str(location.get("key") or "")
            field = str(location.get("field") or "")
            if not key or not field or (key, field) in seen:
                continue
            locations.append({"key": key, "field": field})
            seen.add((key, field))
    return {
        "locations": locations,
        "locator_rows": locator_rows,
        "resource_version_watermark": "|".join(sorted(resource_versions)),
        "eligible": bool(locations),
        "reason": "ok" if locations else "no_matching_placement_rows",
    }


def filter_retrieval_candidates(
    target: Any,
    records: list[Json],
    *,
    scope: Json,
    allowed_types: set[str],
    selected_nodes: set[int],
) -> tuple[list[Json], Json]:
    self = target
    filtered: list[Json] = []
    dropped_type = 0
    dropped_scope = 0
    dropped_node = 0
    for record in records:
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
        if record_type in {"context_embedding", "context_index", "context_summary", "resource_manifest", "skill_registry_update"}:
            if not scope_matches(candidate_access_scope(record), scope):
                dropped_scope += 1
                continue
        elif not access_scope_matches_before_scoring(record, scope):
            dropped_scope += 1
            continue
        filtered.append(record)
    return filtered, {
        "scanned": len(records),
        "returned": len(filtered),
        "dropped_type": dropped_type,
        "dropped_scope": dropped_scope,
        "dropped_node": dropped_node,
    }


def retrieval_records(
    target: Any,
    *,
    scope: Json,
    record_types: set[str] | None = None,
    secondary_index_groups: list[set[str]] | None = None,
    selected_node_hashes: set[int] | None = None,
    allow_broad_scan_fallback: bool | None = None,
) -> Json:
    self = target
    count = self._entry_count_cache if self._entry_count_cache is not None else self._get_count()
    self._ensure_backend_metric_fields()
    placement_result = self._native_locations_for_selected_nodes(scope=scope, selected_node_hashes=selected_node_hashes or set())
    resource_version_watermark = str(placement_result.get("resource_version_watermark") or "")
    cache_key = self._retrieval_candidate_cache_key(
        count=count,
        scope={**scope, "_resource_version_watermark": resource_version_watermark},
        record_types=record_types,
        secondary_index_groups=secondary_index_groups,
        selected_node_hashes=selected_node_hashes,
    )
    with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
        cached = _DIRECT_RETRIEVAL_CANDIDATE_CACHE.get(cache_key)
        if cached is not None:
            result = dict(cached)
            result["records"] = list(cached.get("records", []))
            stats = dict(result.get("scan_stats", {}))
            stats["candidate_cache_hit"] = True
            stats["candidate_cache_scope"] = "process_global"
            result["scan_stats"] = stats
            return result

    allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
    selected_nodes = selected_node_hashes or set()
    broad_scan_allowed = (
        bool(allow_broad_scan_fallback)
        if allow_broad_scan_fallback is not None
        else not bool(selected_nodes or secondary_index_groups)
    )
    index_result = {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "skipped_for_placement_lookup"}
    fallback_reason = ""
    raw_records: list[Json] = []
    native_pushdown = False
    native_mode = ""
    placement_cache_result: Json = {"cache_hit": False, "cache_entries": 0, "loaded_records": 0}
    if bool(placement_result.get("eligible")):
        placement_cache_result = self._placement_candidate_records_from_cache_or_load(
            count=count,
            scope=scope,
            allowed_types=allowed_types,
            selected_nodes=selected_nodes,
            locations=placement_result.get("locations", []),
            resource_version_watermark=resource_version_watermark,
        )
        raw_records = placement_cache_result.get("records", [])
        native_pushdown = bool(raw_records)
        native_mode = "native_placement_prefetch"
        if not raw_records:
            fallback_reason = "native_placement_locations_empty"
    if not native_pushdown:
        index_result = self._native_index_ref_hashes(scope=scope, secondary_index_groups=secondary_index_groups)
    if not native_pushdown and bool(index_result.get("eligible")):
        location_result = self._native_locations_for_refs(index_result.get("ref_hashes", set()))
        raw_records = self._load_records_from_locations(location_result.get("locations", []))
        native_pushdown = bool(raw_records)
        native_mode = "native_secondary_index_prefilter"
        if not raw_records:
            fallback_reason = "native_index_locations_empty"
    else:
        location_result = {"locations": [], "locator_rows": 0}
        if not native_pushdown:
            fallback_reason = str(index_result.get("reason") or placement_result.get("reason") or "native_index_not_eligible")

    if native_pushdown:
        filtered, filter_stats = self._filter_retrieval_candidates(
            raw_records,
            scope=scope,
            allowed_types=allowed_types,
            selected_nodes=selected_nodes,
        )
        if not filtered:
            fallback_reason = "native_index_filtered_empty"
            native_pushdown = False

    broad_scan_used = False
    broad_scan_blocked = False
    if not native_pushdown and broad_scan_allowed:
        raw_records = self.read_all()
        broad_scan_used = True
        filtered, filter_stats = self._filter_retrieval_candidates(
            raw_records,
            scope=scope,
            allowed_types=allowed_types,
            selected_nodes=selected_nodes,
        )
    elif not native_pushdown:
        broad_scan_blocked = True
        raw_records = []
        filtered = []
        filter_stats = {
            "scanned": 0,
            "returned": 0,
            "dropped_type": 0,
            "dropped_scope": 0,
            "dropped_node": 0,
        }
    result = {
        "records": filtered,
        "count": count,
        "scan_stats": {
            "backend": self._backend_label(),
            "execution_mode": (
                native_mode
                if native_pushdown
                else ("broad_prefix_scan_fallback" if broad_scan_used else "native_prefilter_no_match_broad_scan_blocked")
            ),
            "native_pushdown": native_pushdown,
            "phase2_native_first": True,
            "native_placement_nodes": len(selected_nodes),
            "native_placement_locator_rows": placement_result.get("locator_rows", 0),
            "native_placement_locations": len(placement_result.get("locations", [])),
            "native_placement_candidate_cache_hit": bool(placement_cache_result.get("cache_hit")),
            "native_placement_candidate_cache_entries": int(placement_cache_result.get("cache_entries") or 0),
            "native_placement_loaded_records": int(placement_cache_result.get("loaded_records") or 0),
            "native_candidate_cache_key_shape": "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
            "native_candidate_cache_payload": "compact_struct",
            "native_resource_version_watermark": resource_version_watermark,
            "native_index_terms": index_result.get("index_terms", []),
            "native_index_posting_buckets": index_result.get("posting_buckets", []),
            "native_index_postings_found": index_result.get("postings_found", 0),
            "native_index_ref_hash_count": len(index_result.get("ref_hashes", set())),
            "native_locator_rows": location_result.get("locator_rows", 0),
            "native_locations": len(location_result.get("locations", [])),
            "fallback_reason": fallback_reason,
            "broad_scan_fallback_allowed": broad_scan_allowed,
            "broad_scan_used": broad_scan_used,
            "broad_scan_blocked": broad_scan_blocked,
            "broad_scan_policy": "explicit_fallback_or_debug_only",
            "candidate_cache_hit": False,
            "candidate_cache_scope": "process_global",
            "watermark_count": count,
            **filter_stats,
            "record_types": sorted(allowed_types),
        },
    }
    with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE[cache_key] = {
            **result,
            "storage_prefix": self._storage_prefix,
            "records": list(filtered),
        }
        self._prune_retrieval_candidate_cache(count)
    return result

