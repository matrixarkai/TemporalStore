#!/usr/bin/env python3
"""Validate C++/Rust storage lifecycle parity contract and optional reports.

The lightweight default mode checks that the shared docs and scale runner agree
on the canonical StorageManager lifecycle metrics. When --cpp-report and
--rust-report are provided, the tool also normalizes backend reports and fails
closed on missing canonical metrics/config or obvious semantic drift.
"""

from __future__ import annotations

import argparse
import ast
import json
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs" / "temporalstore_page_block_address_contract.md"
SCALE_REPORT = ROOT / "tools" / "run_matrixark_cpp_rust_scale_report.py"
REPORT_PAIR_CORPUS = ROOT / "compat" / "storage_lifecycle_report_pair_corpus.json"

REQUIRED_STORAGE_CACHE_LAYERS = [
    "memory_object_cache",
    "page_index_cache",
    "block_index_cache",
    "disk_block_cache",
    "shared_store_read_through",
]

REQUIRED_STORAGE_CACHE_SEMANTICS = [
    "lookup_hot_to_cold",
    "refill_from_durable_on_miss",
    "invalidate_on_append_watermark",
    "invalidate_on_compaction_watermark",
    "cold_scan_no_promote",
    "writeback_backpressure_reported",
]

REQUIRED_STORAGE_CACHE_METRICS = [
    "memory_cache_hits",
    "memory_cache_misses",
    "page_index_cache_hits",
    "page_index_cache_misses",
    "block_index_cache_hits",
    "block_index_cache_misses",
    "disk_cache_hits",
    "disk_cache_misses",
    "shared_store_read_throughs",
    "cache_refills",
    "cache_invalidations",
    "cache_writeback_queue_depth",
    "cache_writeback_rejections",
]

REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS = [
    "layers",
    "semantics",
    "metrics",
    "hot_to_cold_lookup",
    "durable_refill_on_miss",
    "append_watermark_invalidation",
    "compaction_watermark_invalidation",
    "cold_scan_no_promote",
    "writeback_backpressure_measured",
    "cache_refills",
    "cache_invalidations",
    "cache_writeback_queue_depth",
    "cache_writeback_rejections",
    "hot_cache_promotions",
]

REQUIRED_STORAGE_LIFECYCLE_METRICS = [
    "storage_manager_prepare_count",
    "storage_manager_reclaim_count",
    "storage_manager_evict_count",
    "storage_manager_expire_count",
    "storage_manager_page_gc_count",
    "storage_manager_block_gc_count",
    "storage_manager_compaction_count",
    "storage_manager_index_gc_count",
    "storage_manager_delayed_destroy_count",
    "storage_manager_follower_cursor_safety_count",
    "storage_manager_watermark_progress_count",
    "storage_manager_loop_ms",
    "stream_rollover_count",
    "segment_open_count",
    "segment_sealed_count",
    "storage_zone_total_bytes",
    "storage_zone_used_bytes",
    "storage_zone_stale_bytes",
    "append_log_replay_records",
    "append_log_reclaimed_records",
    "slot_dirty_generation_count",
    "slot_tombstone_count",
    "slot_stale_ref_count",
    "slot_owner_mismatch_count",
    "page_index_rebuild_count",
    "block_index_rebuild_count",
    "object_index_rebuild_count",
    "cache_admissions",
    "cache_evictions",
    "cache_rehydrates",
    *REQUIRED_STORAGE_CACHE_METRICS,
    "cold_scan_no_cache_reads",
    "hot_cache_promotions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "delayed_destroy_backlog",
    "follower_cursor_retention_floor",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
    "append_watermark",
    "compaction_watermark",
]

REQUIRED_STORAGE_READ_SEQUENCE = [
    "logical_key_timestamp_range",
    "object_page_index_lookup",
    "page_address_list",
    "block_index_lookup",
    "page_read",
    "decode_records",
    "return_filtered_result",
]

REQUIRED_STORAGE_COLD_SCAN_SEQUENCE = [
    "timestamp_page_index_scan",
    "no_cache_page_read",
    "bounded_decode",
    "no_hot_cache_promotion",
]

REQUIRED_STORAGE_LIFECYCLE_PHASES = [
    "prepare",
    "reclaim",
    "evict",
    "expire",
    "page_gc",
    "block_gc",
    "compaction",
    "index_gc",
    "delayed_destroy",
    "follower_cursor_safety",
    "watermark_progress",
]

REQUIRED_STORAGE_RECLAIM_SEMANTICS = [
    "cache_eviction_memory_only",
    "logical_tombstone_required",
    "stale_pages_blocks_rewritten_or_skipped",
    "reclaimed_bytes_reported",
    "physical_reclaim_errors_zero",
]

REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS = [
    "cache_eviction_frees_memory_only",
    "logical_gc_marks_expired_deletable",
    "physical_reclaim_requires_compaction_or_safe_skip",
    "cache_evictions",
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "stale_pages_rewritten",
    "stale_pages_skipped",
    "stale_blocks_rewritten",
    "stale_blocks_skipped",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
]

REQUIRED_STORAGE_MANAGER_CONTRACT_FIELDS = [
    "manager_identity",
    "cpp_public_name",
    "rust_public_name",
    "phase_order",
    "phase_metrics",
    "phase_counts",
    "loop_metric",
    "loop_ms",
    "phase_order_enforced",
    "missing_phase_count",
]

REQUIRED_STORAGE_MANAGER_PHASE_METRICS = {
    "prepare": "storage_manager_prepare_count",
    "reclaim": "storage_manager_reclaim_count",
    "evict": "storage_manager_evict_count",
    "expire": "storage_manager_expire_count",
    "page_gc": "storage_manager_page_gc_count",
    "block_gc": "storage_manager_block_gc_count",
    "compaction": "storage_manager_compaction_count",
    "index_gc": "storage_manager_index_gc_count",
    "delayed_destroy": "storage_manager_delayed_destroy_count",
    "follower_cursor_safety": "storage_manager_follower_cursor_safety_count",
    "watermark_progress": "storage_manager_watermark_progress_count",
}

REQUIRED_STORAGE_INDEX_CONTRACT_FIELDS = [
    "page_address_codec",
    "block_address_codec",
    "stable_order",
    "slot_index",
    "object_index_entry",
    "page_index",
    "block_index",
    "required_behaviors",
    "page_address_encode_decode",
    "block_address_encode_decode",
    "stable_order_verified",
    "timestamp_range_lookup_verified",
    "slot_index_entry_count",
    "slot_object_ref_count",
    "slot_page_ref_count",
    "object_index_entry_count",
    "page_index_entry_count",
    "block_index_entry_count",
    "restart_rebuild_verified",
    "unreadable_page_refs",
    "checksum_mismatches",
]

REQUIRED_STORAGE_INDEX_BEHAVIORS = [
    "page_address_encode_decode",
    "page_address_stable_order",
    "timestamp_range_page_lookup",
    "slot_index_maps_slot_to_object_page_refs",
    "object_index_maps_model_table_object_key_to_page_chain",
    "page_index_maps_logical_ranges_to_page_addresses",
    "block_index_maps_page_addresses_to_durable_locations",
    "restart_rebuilds_page_block_object_indexes",
]

REQUIRED_STORAGE_RECLAIM_SCOPE = {
    "owner": "temporalstore_storage_lifecycle",
    "matrixark_context_gc_role": "marks_logical_raw_event_eligibility_only",
    "physical_reclaim_context_specific": False,
}

REQUIRED_CONFIG_FIELDS = [
    "TS_CONTEXT_PAGE_TARGET_BYTES",
    "TS_BLOCK_SEGMENT_TARGET_BYTES",
    "TS_STORAGE_ZONE_SIZE",
    "TS_STREAM_MAX_BLOB_SIZE",
    "TS_COMPACTION_WATERMARK_BYTES",
    "TS_COLD_SCAN_NO_CACHE_FILL",
    "TS_PAGE_INDEX_CACHE_BYTES",
    "TS_BLOCK_INDEX_CACHE_BYTES",
]

REQUIRED_LIFECYCLE_TOP_LEVEL_KEYS = [
    "effective_storage_tuning",
    "public_storage_contract",
    "storage_write_contract",
    "storage_read_contract",
    "storage_cold_scan_contract",
    "storage_manager_contract",
    "storage_index_contract",
    "storage_cache_contract",
    "storage_reclaim_contract",
    "storage_read_sequence",
    "storage_cold_scan_sequence",
    "storage_lifecycle_phases",
    "storage_lifecycle_metrics",
    "storage_cache_layers",
    "storage_cache_semantics",
    "storage_reclaim_semantics",
    "storage_write_sequence",
    "storage_reclaim_scope",
]

REQUIRED_STORAGE_WRITE_SEQUENCE = [
    "append_record",
    "route_shard_slot",
    "choose_page",
    "append_page_buffer",
    "update_page_index",
    "flush_page_block_segment",
    "update_block_index",
    "publish_append_watermark",
]

REQUIRED_STORAGE_WRITE_RESULT_FIELDS = [
    "shard_id",
    "slot",
    "placement_key",
    "page_address",
    "block_address",
    "append_watermark",
    "durability",
    "storage_family",
    "write_mode",
    "index_generation",
    "batch_watermark",
    "records_appended",
]

REQUIRED_STORAGE_WRITE_METRICS = [
    "append_queue_wait_ms",
    "append_engine_ms",
    "append_queue_depth",
    "append_batch_size",
    "append_batch_bytes",
    "append_coalesced_writes",
    "append_durability_failures",
    "append_watermark",
    "page_writes",
    "block_writes",
    "bytes_written",
]

REQUIRED_STORAGE_READ_RESULT_FIELDS = [
    "logical_key",
    "timestamp_range",
    "object_index_entry",
    "page_index_entries",
    "page_addresses",
    "block_index_entries",
    "records_decoded",
    "records_returned",
    "tombstones_filtered",
    "stale_generations_filtered",
    "filter_policy",
]

REQUIRED_STORAGE_READ_METRICS = [
    "object_page_index_lookup_count",
    "object_page_index_lookup_ms",
    "page_address_count",
    "block_index_lookup_count",
    "block_index_lookup_ms",
    "page_reads",
    "decode_records_ms",
    "records_decoded",
    "records_returned",
    "tombstones_filtered",
    "stale_generations_filtered",
]

REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS = [
    "timestamp_range",
    "page_index_scan",
    "no_cache_page_reads",
    "decode_batch_limit",
    "decode_byte_limit",
    "deadline_ms",
    "records_decoded",
    "records_returned",
    "hot_cache_promotions",
    "cache_fill",
    "promotion_policy",
]

REQUIRED_STORAGE_COLD_SCAN_METRICS = [
    "cold_scan_no_cache_reads",
    "cold_scan_page_index_scan_count",
    "cold_scan_page_index_scan_ms",
    "cold_scan_page_reads",
    "cold_scan_decode_records_ms",
    "cold_scan_records_decoded",
    "cold_scan_records_returned",
    "cold_scan_decode_batch_limit",
    "cold_scan_decode_byte_limit",
    "hot_cache_promotions",
]

CANONICAL_PUBLIC_FIELDS = [
    "PageAddress",
    "BlockAddress",
    "PageIndexEntry",
    "BlockIndexEntry",
    "ObjectIndexEntry",
    "StorageZone",
    "Stream",
    "Segment",
    "Extent",
    "Slot",
    "AppendWatermark",
    "CompactionWatermark",
    "Tombstone",
    "GcEligibility",
    "FollowerCursorSafety",
]

CANONICAL_JSON_FIELDS = [
    "page_address",
    "block_address",
    "page_index_entry",
    "block_index_entry",
    "object_index_entry",
    "storage_zone",
    "stream",
    "segment",
    "extent",
    "slot",
    "append_watermark",
    "compaction_watermark",
    "tombstone",
    "gc_eligibility",
    "follower_cursor_safety",
]

LEGACY_ALIAS_MAP = {
    "page_store": "storage_zone",
    "block_store": "storage_zone",
    "page_segment": "segment",
    "page_segment_id": "segment_id",
    "object_index": "object_index_entry",
    "stream_blob": "segment",
    "stream_blob_id": "segment_id",
    "oplog": "append_watermark",
    "oplog_id": "append_watermark",
    "oplog_sequence": "append_watermark",
    "zone": "storage_zone",
    "extent_id": "extent_id",
}

ALLOWED_ALIAS_CONTAINERS = {"compatibility_aliases", "legacy_alias", "legacy_aliases", "migration_aliases"}

REQUIRED_SCALE_REPORT_CONFIG_BINDINGS = {
    "required_storage_lifecycle_top_level_keys": "STORAGE_LIFECYCLE_TOP_LEVEL_KEYS",
    "required_storage_write_sequence": "STORAGE_WRITE_SEQUENCE_STEPS",
    "required_storage_write_result_fields": "STORAGE_WRITE_RESULT_FIELDS",
    "required_storage_write_metrics": "STORAGE_WRITE_METRIC_NAMES",
    "required_storage_read_sequence": "STORAGE_READ_SEQUENCE_STEPS",
    "required_storage_read_result_fields": "STORAGE_READ_RESULT_FIELDS",
    "required_storage_read_metrics": "STORAGE_READ_METRIC_NAMES",
    "required_storage_cold_scan_sequence": "STORAGE_COLD_SCAN_SEQUENCE_STEPS",
    "required_storage_cold_scan_result_fields": "STORAGE_COLD_SCAN_RESULT_FIELDS",
    "required_storage_cold_scan_metrics": "STORAGE_COLD_SCAN_METRIC_NAMES",
    "required_storage_lifecycle_phases": "STORAGE_LIFECYCLE_PHASE_NAMES",
    "required_storage_manager_phase_metrics": "STORAGE_MANAGER_PHASE_METRICS",
    "required_storage_index_behaviors": "STORAGE_INDEX_BEHAVIOR_NAMES",
    "required_storage_reclaim_semantics": "STORAGE_RECLAIM_SEMANTICS",
    "required_storage_reclaim_contract_fields": "STORAGE_RECLAIM_CONTRACT_FIELDS",
    "required_storage_cache_layers": "STORAGE_CACHE_LAYER_NAMES",
    "required_storage_cache_semantics": "STORAGE_CACHE_SEMANTICS",
    "required_storage_cache_metrics": "STORAGE_CACHE_METRIC_NAMES",
    "required_storage_cache_contract_fields": "STORAGE_CACHE_CONTRACT_FIELDS",
    "required_storage_lifecycle_metrics": "STORAGE_LIFECYCLE_METRIC_NAMES",
}


def _extract_runner_list(name: str) -> list[str]:
    tree = ast.parse(SCALE_REPORT.read_text(encoding="utf-8"), filename=str(SCALE_REPORT))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                value = ast.literal_eval(node.value)
                if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                    raise AssertionError(f"{name} must be a list[str]")
                return value
    raise AssertionError(f"{name} not found in scale report runner")


def _extract_runner_dict(name: str) -> dict[str, str]:
    tree = ast.parse(SCALE_REPORT.read_text(encoding="utf-8"), filename=str(SCALE_REPORT))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
                value = ast.literal_eval(node.value)
                if not isinstance(value, dict) or not all(isinstance(key, str) and isinstance(item, str) for key, item in value.items()):
                    raise AssertionError(f"{name} must be a dict[str, str]")
                return value
    raise AssertionError(f"{name} not found in scale report runner")


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _dig_metrics(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_lifecycle_metrics"),
        report.get("storage_lifecycle", {}).get("metrics") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("metrics", {}).get("storage_lifecycle") if isinstance(report.get("metrics"), dict) else None,
        report.get("metrics"),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_config(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("effective_storage_tuning"),
        report.get("config", {}).get("effective_storage_tuning") if isinstance(report.get("config"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_sequence(report: dict[str, Any], key: str) -> list[str]:
    candidates = [
        report.get(key),
        report.get("storage_lifecycle", {}).get(key) if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_sequences", {}).get(key) if isinstance(report.get("storage_sequences"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_write_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_write_contract"),
        report.get("storage_lifecycle", {}).get("write_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("write_path", {}).get("contract") if isinstance(report.get("write_path"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_read_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_read_contract"),
        report.get("storage_lifecycle", {}).get("read_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("read_path", {}).get("contract") if isinstance(report.get("read_path"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_cold_scan_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_cold_scan_contract"),
        report.get("storage_lifecycle", {}).get("cold_scan_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("cold_scan_path", {}).get("contract") if isinstance(report.get("cold_scan_path"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_manager_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_manager_contract"),
        report.get("storage_lifecycle", {}).get("manager_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_manager", {}).get("contract") if isinstance(report.get("storage_manager"), dict) else None,
        report.get("store_manager", {}).get("contract") if isinstance(report.get("store_manager"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_index_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_index_contract"),
        report.get("storage_lifecycle", {}).get("index_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_index", {}).get("contract") if isinstance(report.get("storage_index"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_cache_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_cache_contract"),
        report.get("storage_lifecycle", {}).get("cache_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_cache", {}).get("contract") if isinstance(report.get("storage_cache"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_lifecycle_phases(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_lifecycle_phases"),
        report.get("storage_lifecycle", {}).get("phases") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_reclaim_semantics(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_reclaim_semantics"),
        report.get("storage_lifecycle", {}).get("reclaim_semantics") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_reclaim_scope(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_reclaim_scope"),
        report.get("storage_lifecycle", {}).get("reclaim_scope") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_reclaim_contract(report: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        report.get("storage_reclaim_contract"),
        report.get("storage_lifecycle", {}).get("reclaim_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_reclaim", {}).get("contract") if isinstance(report.get("storage_reclaim"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _dig_cache_layers(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_cache_layers"),
        report.get("storage_lifecycle", {}).get("cache_layers") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_cache", {}).get("layers") if isinstance(report.get("storage_cache"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _dig_cache_semantics(report: dict[str, Any]) -> list[str]:
    candidates = [
        report.get("storage_cache_semantics"),
        report.get("storage_lifecycle", {}).get("cache_semantics") if isinstance(report.get("storage_lifecycle"), dict) else None,
        report.get("storage_cache", {}).get("semantics") if isinstance(report.get("storage_cache"), dict) else None,
    ]
    for candidate in candidates:
        if isinstance(candidate, list) and all(isinstance(item, str) for item in candidate):
            return candidate
    return []


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _walk_public_keys(value: Any, *, in_alias_container: bool = False, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], str]]:
    violations: list[tuple[tuple[str, ...], str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            child_in_alias = in_alias_container or str(key) in ALLOWED_ALIAS_CONTAINERS
            if not child_in_alias and str(key) in LEGACY_ALIAS_MAP:
                violations.append((child_path, str(key)))
            violations.extend(_walk_public_keys(child, in_alias_container=child_in_alias, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            violations.extend(_walk_public_keys(child, in_alias_container=in_alias_container, path=(*path, str(index))))
    return violations


def _normalize_public_storage_shape(report: dict[str, Any]) -> dict[str, Any]:
    """Return canonical public storage fields from a backend report.

    Canonical fields win. Legacy aliases are accepted only from compatibility
    alias containers and normalized for comparison.
    """
    source_candidates = [
        report.get("public_storage_contract"),
        report.get("storage_public_contract"),
        report.get("storage_lifecycle", {}).get("public_contract") if isinstance(report.get("storage_lifecycle"), dict) else None,
    ]
    source = next((candidate for candidate in source_candidates if isinstance(candidate, dict)), {})
    aliases = source.get("compatibility_aliases") if isinstance(source.get("compatibility_aliases"), dict) else {}

    normalized: dict[str, Any] = {}
    for key in CANONICAL_JSON_FIELDS:
        if key in source:
            normalized[key] = source[key]
    for alias, canonical in LEGACY_ALIAS_MAP.items():
        if alias in aliases and canonical not in normalized:
            normalized[canonical] = aliases[alias]
    return normalized


def _metric_number(metrics: dict[str, Any], name: str) -> float:
    value = _as_number(metrics.get(name))
    return 0.0 if value is None else value


def _validate_physical_reclaim_evidence(backend: str, metrics: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cache_evictions = _metric_number(metrics, "cache_evictions")
    physical_reclaimed = _metric_number(metrics, "physical_reclaimed_bytes")
    compaction_reclaimed = _metric_number(metrics, "compaction_reclaimed_bytes")
    physical_errors = _metric_number(metrics, "physical_reclaim_errors")
    tombstone_evidence = (
        _metric_number(metrics, "tombstone_records")
        + _metric_number(metrics, "stale_page_tombstones")
        + _metric_number(metrics, "stale_block_tombstones")
    )
    rewrite_or_skip_evidence = (
        _metric_number(metrics, "stale_pages_rewritten")
        + _metric_number(metrics, "stale_pages_skipped")
        + _metric_number(metrics, "stale_blocks_rewritten")
        + _metric_number(metrics, "stale_blocks_skipped")
    )

    if cache_evictions > 0 and physical_reclaimed == 0 and compaction_reclaimed == 0:
        # This is valid and intentional: cache eviction frees memory only.
        return failures

    if physical_reclaimed > 0:
        if physical_errors != 0:
            failures.append(f"{backend} physical reclaim reported bytes with errors={physical_errors}")
        if tombstone_evidence <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without tombstone evidence")
        if rewrite_or_skip_evidence <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without stale page/block rewrite-or-skip evidence")
        if compaction_reclaimed <= 0:
            failures.append(f"{backend} physical reclaim reported bytes without compaction_reclaimed_bytes")
    return failures


def _validate_stream_slot_index_evidence(backend: str, metrics: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    segment_open = _metric_number(metrics, "segment_open_count")
    segment_sealed = _metric_number(metrics, "segment_sealed_count")
    stream_rollover = _metric_number(metrics, "stream_rollover_count")
    log_replay = _metric_number(metrics, "append_log_replay_records")
    log_reclaimed = _metric_number(metrics, "append_log_reclaimed_records")
    owner_mismatch = _metric_number(metrics, "slot_owner_mismatch_count")

    if segment_sealed > segment_open:
        failures.append(
            f"{backend} segment_sealed_count cannot exceed segment_open_count: "
            f"sealed={segment_sealed} open={segment_open}"
        )
    if stream_rollover > 0 and segment_open <= 0:
        failures.append(f"{backend} stream rollover reported without segment open evidence")
    if log_reclaimed > log_replay:
        failures.append(
            f"{backend} append_log_reclaimed_records cannot exceed append_log_replay_records: "
            f"reclaimed={log_reclaimed} replay={log_replay}"
        )
    if owner_mismatch != 0:
        failures.append(f"{backend} slot_owner_mismatch_count must be zero, got {owner_mismatch}")
    for metric in [
        "slot_dirty_generation_count",
        "slot_tombstone_count",
        "slot_stale_ref_count",
        "page_index_rebuild_count",
        "block_index_rebuild_count",
        "object_index_rebuild_count",
    ]:
        if _metric_number(metrics, metric) < 0:
            failures.append(f"{backend} {metric} must be non-negative")
    return failures


def validate_contract_and_runner() -> list[str]:
    failures: list[str] = []
    contract_text = CONTRACT.read_text(encoding="utf-8")
    runner_text = SCALE_REPORT.read_text(encoding="utf-8")
    runner_metrics = _extract_runner_list("STORAGE_LIFECYCLE_METRIC_NAMES")
    runner_write_sequence = _extract_runner_list("STORAGE_WRITE_SEQUENCE_STEPS")
    runner_read_sequence = _extract_runner_list("STORAGE_READ_SEQUENCE_STEPS")
    runner_cold_scan_sequence = _extract_runner_list("STORAGE_COLD_SCAN_SEQUENCE_STEPS")
    runner_write_result_fields = _extract_runner_list("STORAGE_WRITE_RESULT_FIELDS")
    runner_write_metrics = _extract_runner_list("STORAGE_WRITE_METRIC_NAMES")
    runner_read_result_fields = _extract_runner_list("STORAGE_READ_RESULT_FIELDS")
    runner_read_metrics = _extract_runner_list("STORAGE_READ_METRIC_NAMES")
    runner_cold_scan_result_fields = _extract_runner_list("STORAGE_COLD_SCAN_RESULT_FIELDS")
    runner_cold_scan_metrics = _extract_runner_list("STORAGE_COLD_SCAN_METRIC_NAMES")
    runner_lifecycle_phases = _extract_runner_list("STORAGE_LIFECYCLE_PHASE_NAMES")
    runner_manager_phase_metrics = _extract_runner_dict("STORAGE_MANAGER_PHASE_METRICS")
    runner_index_behaviors = _extract_runner_list("STORAGE_INDEX_BEHAVIOR_NAMES")
    runner_reclaim_semantics = _extract_runner_list("STORAGE_RECLAIM_SEMANTICS")
    runner_reclaim_contract_fields = _extract_runner_list("STORAGE_RECLAIM_CONTRACT_FIELDS")
    runner_cache_layers = _extract_runner_list("STORAGE_CACHE_LAYER_NAMES")
    runner_cache_semantics = _extract_runner_list("STORAGE_CACHE_SEMANTICS")
    runner_cache_metrics = _extract_runner_list("STORAGE_CACHE_METRIC_NAMES")
    runner_cache_contract_fields = _extract_runner_list("STORAGE_CACHE_CONTRACT_FIELDS")
    runner_top_level_keys = _extract_runner_list("STORAGE_LIFECYCLE_TOP_LEVEL_KEYS")
    for name in CANONICAL_PUBLIC_FIELDS + CANONICAL_JSON_FIELDS:
        if f"`{name}`" not in contract_text:
            failures.append(f"contract missing canonical public field `{name}`")
    if runner_metrics != REQUIRED_STORAGE_LIFECYCLE_METRICS:
        failures.append("runner:STORAGE_LIFECYCLE_METRIC_NAMES does not match the canonical lifecycle metric order")
    if runner_write_sequence != REQUIRED_STORAGE_WRITE_SEQUENCE:
        failures.append("runner:STORAGE_WRITE_SEQUENCE_STEPS does not match the canonical write sequence")
    if runner_write_result_fields != REQUIRED_STORAGE_WRITE_RESULT_FIELDS:
        failures.append("runner:STORAGE_WRITE_RESULT_FIELDS does not match the canonical write result fields")
    if runner_write_metrics != REQUIRED_STORAGE_WRITE_METRICS:
        failures.append("runner:STORAGE_WRITE_METRIC_NAMES does not match the canonical write metrics")
    if runner_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append("runner:STORAGE_READ_SEQUENCE_STEPS does not match the canonical read sequence")
    if runner_read_result_fields != REQUIRED_STORAGE_READ_RESULT_FIELDS:
        failures.append("runner:STORAGE_READ_RESULT_FIELDS does not match the canonical read result fields")
    if runner_read_metrics != REQUIRED_STORAGE_READ_METRICS:
        failures.append("runner:STORAGE_READ_METRIC_NAMES does not match the canonical read metrics")
    if runner_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append("runner:STORAGE_COLD_SCAN_SEQUENCE_STEPS does not match the canonical cold scan sequence")
    if runner_cold_scan_result_fields != REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS:
        failures.append("runner:STORAGE_COLD_SCAN_RESULT_FIELDS does not match the canonical cold scan result fields")
    if runner_cold_scan_metrics != REQUIRED_STORAGE_COLD_SCAN_METRICS:
        failures.append("runner:STORAGE_COLD_SCAN_METRIC_NAMES does not match the canonical cold scan metrics")
    if runner_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append("runner:STORAGE_LIFECYCLE_PHASE_NAMES does not match the canonical lifecycle phase order")
    if runner_manager_phase_metrics != REQUIRED_STORAGE_MANAGER_PHASE_METRICS:
        failures.append("runner:STORAGE_MANAGER_PHASE_METRICS does not match the canonical manager phase metrics")
    if runner_index_behaviors != REQUIRED_STORAGE_INDEX_BEHAVIORS:
        failures.append("runner:STORAGE_INDEX_BEHAVIOR_NAMES does not match the canonical index behaviors")
    if runner_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append("runner:STORAGE_RECLAIM_SEMANTICS does not match the canonical reclaim semantics")
    if runner_reclaim_contract_fields != REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS:
        failures.append("runner:STORAGE_RECLAIM_CONTRACT_FIELDS does not match the canonical reclaim contract fields")
    if runner_cache_layers != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append("runner:STORAGE_CACHE_LAYER_NAMES does not match the canonical cache layers")
    if runner_cache_semantics != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append("runner:STORAGE_CACHE_SEMANTICS does not match the canonical cache semantics")
    if runner_cache_metrics != REQUIRED_STORAGE_CACHE_METRICS:
        failures.append("runner:STORAGE_CACHE_METRIC_NAMES does not match the canonical cache metrics")
    if runner_cache_contract_fields != REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS:
        failures.append("runner:STORAGE_CACHE_CONTRACT_FIELDS does not match the canonical cache contract fields")
    if runner_top_level_keys != REQUIRED_LIFECYCLE_TOP_LEVEL_KEYS:
        failures.append("runner:STORAGE_LIFECYCLE_TOP_LEVEL_KEYS does not match the canonical report shape")
    for config_key, constant_name in REQUIRED_SCALE_REPORT_CONFIG_BINDINGS.items():
        expected_binding = f'"{config_key}": {constant_name}'
        if expected_binding not in runner_text:
            failures.append(
                f"runner scale-report config missing canonical binding `{config_key}: {constant_name}`"
            )
    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing lifecycle metric `{metric}`")
        if metric not in runner_metrics:
            failures.append(f"runner missing lifecycle metric `{metric}`")
    for step in REQUIRED_STORAGE_WRITE_SEQUENCE + REQUIRED_STORAGE_READ_SEQUENCE + REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        if f"`{step}`" not in contract_text:
            failures.append(f"contract missing storage sequence step `{step}`")
    for field in REQUIRED_STORAGE_WRITE_RESULT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing write result field `{field}`")
    for metric in REQUIRED_STORAGE_WRITE_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing write metric `{metric}`")
    for field in REQUIRED_STORAGE_READ_RESULT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing read result field `{field}`")
    for metric in REQUIRED_STORAGE_READ_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing read metric `{metric}`")
    for field in REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing cold scan result field `{field}`")
    for metric in REQUIRED_STORAGE_COLD_SCAN_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing cold scan metric `{metric}`")
    for phase in REQUIRED_STORAGE_LIFECYCLE_PHASES:
        if f"`{phase}`" not in contract_text:
            failures.append(f"contract missing lifecycle phase `{phase}`")
    for field in REQUIRED_STORAGE_MANAGER_CONTRACT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing manager contract field `{field}`")
    for phase, metric in REQUIRED_STORAGE_MANAGER_PHASE_METRICS.items():
        if f"`{phase}`" not in contract_text:
            failures.append(f"contract missing manager phase `{phase}`")
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing manager phase metric `{metric}`")
    for field in REQUIRED_STORAGE_INDEX_CONTRACT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing index contract field `{field}`")
    for behavior in REQUIRED_STORAGE_INDEX_BEHAVIORS:
        if f"`{behavior}`" not in contract_text:
            failures.append(f"contract missing index behavior `{behavior}`")
    for semantic in REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        if f"`{semantic}`" not in contract_text:
            failures.append(f"contract missing reclaim semantic `{semantic}`")
    for field in REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing reclaim contract field `{field}`")
    for layer in REQUIRED_STORAGE_CACHE_LAYERS:
        if f"`{layer}`" not in contract_text:
            failures.append(f"contract missing cache layer `{layer}`")
    for semantic in REQUIRED_STORAGE_CACHE_SEMANTICS:
        if f"`{semantic}`" not in contract_text:
            failures.append(f"contract missing cache semantic `{semantic}`")
    for metric in REQUIRED_STORAGE_CACHE_METRICS:
        if f"`{metric}`" not in contract_text:
            failures.append(f"contract missing cache metric `{metric}`")
    for field in REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS:
        if f"`{field}`" not in contract_text:
            failures.append(f"contract missing cache contract field `{field}`")
    for value in REQUIRED_STORAGE_RECLAIM_SCOPE.values():
        if isinstance(value, str) and f"`{value}`" not in contract_text and f'"{value}"' not in contract_text:
            failures.append(f"contract missing reclaim scope value `{value}`")
    return failures


def validate_report_pair(cpp_report: dict[str, Any], rust_report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    cpp_metrics = _dig_metrics(cpp_report)
    rust_metrics = _dig_metrics(rust_report)
    cpp_config = _dig_config(cpp_report)
    rust_config = _dig_config(rust_report)
    cpp_public_shape = _normalize_public_storage_shape(cpp_report)
    rust_public_shape = _normalize_public_storage_shape(rust_report)
    cpp_write_contract = _dig_write_contract(cpp_report)
    rust_write_contract = _dig_write_contract(rust_report)
    cpp_read_contract = _dig_read_contract(cpp_report)
    rust_read_contract = _dig_read_contract(rust_report)
    cpp_cold_scan_contract = _dig_cold_scan_contract(cpp_report)
    rust_cold_scan_contract = _dig_cold_scan_contract(rust_report)
    cpp_manager_contract = _dig_manager_contract(cpp_report)
    rust_manager_contract = _dig_manager_contract(rust_report)
    cpp_index_contract = _dig_index_contract(cpp_report)
    rust_index_contract = _dig_index_contract(rust_report)
    cpp_cache_contract = _dig_cache_contract(cpp_report)
    rust_cache_contract = _dig_cache_contract(rust_report)
    cpp_write_sequence = _dig_sequence(cpp_report, "storage_write_sequence")
    rust_write_sequence = _dig_sequence(rust_report, "storage_write_sequence")
    cpp_read_sequence = _dig_sequence(cpp_report, "storage_read_sequence")
    rust_read_sequence = _dig_sequence(rust_report, "storage_read_sequence")
    cpp_cold_scan_sequence = _dig_sequence(cpp_report, "storage_cold_scan_sequence")
    rust_cold_scan_sequence = _dig_sequence(rust_report, "storage_cold_scan_sequence")
    cpp_lifecycle_phases = _dig_lifecycle_phases(cpp_report)
    rust_lifecycle_phases = _dig_lifecycle_phases(rust_report)
    cpp_reclaim_semantics = _dig_reclaim_semantics(cpp_report)
    rust_reclaim_semantics = _dig_reclaim_semantics(rust_report)
    cpp_reclaim_scope = _dig_reclaim_scope(cpp_report)
    rust_reclaim_scope = _dig_reclaim_scope(rust_report)
    cpp_reclaim_contract = _dig_reclaim_contract(cpp_report)
    rust_reclaim_contract = _dig_reclaim_contract(rust_report)
    cpp_cache_layers = _dig_cache_layers(cpp_report)
    rust_cache_layers = _dig_cache_layers(rust_report)
    cpp_cache_semantics = _dig_cache_semantics(cpp_report)
    rust_cache_semantics = _dig_cache_semantics(rust_report)

    for backend, report in [("cpp", cpp_report), ("rust", rust_report)]:
        for key in REQUIRED_LIFECYCLE_TOP_LEVEL_KEYS:
            if key not in report:
                failures.append(f"{backend} report missing required top-level `{key}`")

    for field in REQUIRED_CONFIG_FIELDS:
        if field not in cpp_config:
            failures.append(f"cpp config missing `{field}`")
        if field not in rust_config:
            failures.append(f"rust config missing `{field}`")
        if field in cpp_config and field in rust_config and cpp_config[field] != rust_config[field]:
            failures.append(f"config drift `{field}`: cpp={cpp_config[field]!r} rust={rust_config[field]!r}")

    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        if metric not in cpp_metrics:
            failures.append(f"cpp metrics missing `{metric}`")
        if metric not in rust_metrics:
            failures.append(f"rust metrics missing `{metric}`")
    for field in REQUIRED_STORAGE_WRITE_RESULT_FIELDS:
        if field not in cpp_write_contract:
            failures.append(f"cpp write contract missing result field `{field}`")
        if field not in rust_write_contract:
            failures.append(f"rust write contract missing result field `{field}`")
    for metric in REQUIRED_STORAGE_WRITE_METRICS:
        if metric not in cpp_write_contract:
            failures.append(f"cpp write contract missing metric `{metric}`")
        if metric not in rust_write_contract:
            failures.append(f"rust write contract missing metric `{metric}`")
    for field in ["durability", "storage_family", "write_mode"]:
        if field in cpp_write_contract and field in rust_write_contract and cpp_write_contract[field] != rust_write_contract[field]:
            failures.append(
                f"write contract drift `{field}`: cpp={cpp_write_contract[field]!r} rust={rust_write_contract[field]!r}"
            )
    for field in ["records_appended", "append_durability_failures"]:
        cpp_value = _as_number(cpp_write_contract.get(field))
        rust_value = _as_number(rust_write_contract.get(field))
        if cpp_value is not None and rust_value is not None and cpp_value != rust_value:
            failures.append(f"write contract drift `{field}`: cpp={cpp_value} rust={rust_value}")
    for field in ["append_watermark", "batch_watermark", "index_generation"]:
        cpp_value = _as_number(cpp_write_contract.get(field))
        rust_value = _as_number(rust_write_contract.get(field))
        if cpp_value is not None and cpp_value < 0:
            failures.append(f"cpp write contract `{field}` must be non-negative")
        if rust_value is not None and rust_value < 0:
            failures.append(f"rust write contract `{field}` must be non-negative")
    for backend, write_contract in [("cpp", cpp_write_contract), ("rust", rust_write_contract)]:
        if _as_number(write_contract.get("append_durability_failures")) not in (0.0, None):
            failures.append(f"{backend} write contract append_durability_failures must be zero")
        if _as_number(write_contract.get("records_appended")) == 0:
            failures.append(f"{backend} write contract records_appended must be positive")
    for field in REQUIRED_STORAGE_READ_RESULT_FIELDS:
        if field not in cpp_read_contract:
            failures.append(f"cpp read contract missing result field `{field}`")
        if field not in rust_read_contract:
            failures.append(f"rust read contract missing result field `{field}`")
    for metric in REQUIRED_STORAGE_READ_METRICS:
        if metric not in cpp_read_contract:
            failures.append(f"cpp read contract missing metric `{metric}`")
        if metric not in rust_read_contract:
            failures.append(f"rust read contract missing metric `{metric}`")
    for field in ["records_decoded", "records_returned", "tombstones_filtered", "stale_generations_filtered"]:
        cpp_value = _as_number(cpp_read_contract.get(field))
        rust_value = _as_number(rust_read_contract.get(field))
        if cpp_value is not None and cpp_value < 0:
            failures.append(f"cpp read contract `{field}` must be non-negative")
        if rust_value is not None and rust_value < 0:
            failures.append(f"rust read contract `{field}` must be non-negative")
    for backend, read_contract in [("cpp", cpp_read_contract), ("rust", rust_read_contract)]:
        decoded = _as_number(read_contract.get("records_decoded"))
        returned = _as_number(read_contract.get("records_returned"))
        if decoded is not None and returned is not None and returned > decoded:
            failures.append(f"{backend} read contract records_returned cannot exceed records_decoded")
        if read_contract.get("filter_policy") not in {"normal", "debug_replay", "cold_scan"}:
            failures.append(f"{backend} read contract filter_policy must be normal/debug_replay/cold_scan")
    for field in REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS:
        if field not in cpp_cold_scan_contract:
            failures.append(f"cpp cold scan contract missing result field `{field}`")
        if field not in rust_cold_scan_contract:
            failures.append(f"rust cold scan contract missing result field `{field}`")
    for metric in REQUIRED_STORAGE_COLD_SCAN_METRICS:
        if metric not in cpp_cold_scan_contract:
            failures.append(f"cpp cold scan contract missing metric `{metric}`")
        if metric not in rust_cold_scan_contract:
            failures.append(f"rust cold scan contract missing metric `{metric}`")
    for backend, cold_scan_contract in [("cpp", cpp_cold_scan_contract), ("rust", rust_cold_scan_contract)]:
        decoded = _as_number(cold_scan_contract.get("records_decoded"))
        returned = _as_number(cold_scan_contract.get("records_returned"))
        if decoded is not None and returned is not None and returned > decoded:
            failures.append(f"{backend} cold scan contract records_returned cannot exceed records_decoded")
        for field in ["no_cache_page_reads", "decode_batch_limit", "decode_byte_limit", "deadline_ms", "hot_cache_promotions"]:
            value = _as_number(cold_scan_contract.get(field))
            if value is not None and value < 0:
                failures.append(f"{backend} cold scan contract `{field}` must be non-negative")
        if cold_scan_contract.get("cache_fill") is not False:
            failures.append(f"{backend} cold scan contract cache_fill must be false")
        if cold_scan_contract.get("promotion_policy") != "no_promote":
            failures.append(f"{backend} cold scan contract promotion_policy must be no_promote")
        if _as_number(cold_scan_contract.get("hot_cache_promotions")) not in (0.0, None):
            failures.append(f"{backend} cold scan contract hot_cache_promotions must be zero")
        if _as_number(cold_scan_contract.get("cold_scan_no_cache_reads")) is not None and _as_number(cold_scan_contract.get("cold_scan_no_cache_reads")) < 0:
            failures.append(f"{backend} cold scan contract cold_scan_no_cache_reads must be non-negative")
    for field in REQUIRED_STORAGE_MANAGER_CONTRACT_FIELDS:
        if field not in cpp_manager_contract:
            failures.append(f"cpp manager contract missing field `{field}`")
        if field not in rust_manager_contract:
            failures.append(f"rust manager contract missing field `{field}`")
    for backend, manager_contract in [("cpp", cpp_manager_contract), ("rust", rust_manager_contract)]:
        if manager_contract.get("manager_identity") != "StorageManager/StoreManager":
            failures.append(f"{backend} manager_identity must be StorageManager/StoreManager")
        if manager_contract.get("cpp_public_name") != "StorageManager":
            failures.append(f"{backend} cpp_public_name must be StorageManager")
        if manager_contract.get("rust_public_name") != "StoreManager":
            failures.append(f"{backend} rust_public_name must be StoreManager")
        if manager_contract.get("phase_order") != REQUIRED_STORAGE_LIFECYCLE_PHASES:
            failures.append(f"{backend} manager phase_order drift: {manager_contract.get('phase_order')!r}")
        if manager_contract.get("phase_metrics") != REQUIRED_STORAGE_MANAGER_PHASE_METRICS:
            failures.append(f"{backend} manager phase_metrics drift: {manager_contract.get('phase_metrics')!r}")
        if manager_contract.get("loop_metric") != "storage_manager_loop_ms":
            failures.append(f"{backend} manager loop_metric must be storage_manager_loop_ms")
        if manager_contract.get("phase_order_enforced") is not True:
            failures.append(f"{backend} manager phase_order_enforced must be true")
        if _as_number(manager_contract.get("missing_phase_count")) not in (0.0, None):
            failures.append(f"{backend} manager missing_phase_count must be zero")
        phase_counts = manager_contract.get("phase_counts")
        if not isinstance(phase_counts, dict):
            failures.append(f"{backend} manager phase_counts must be an object")
        else:
            for phase in REQUIRED_STORAGE_LIFECYCLE_PHASES:
                if phase not in phase_counts:
                    failures.append(f"{backend} manager phase_counts missing `{phase}`")
                elif _as_number(phase_counts.get(phase)) is not None and _as_number(phase_counts.get(phase)) < 0:
                    failures.append(f"{backend} manager phase_counts `{phase}` must be non-negative")
        if _as_number(manager_contract.get("loop_ms")) is not None and _as_number(manager_contract.get("loop_ms")) < 0:
            failures.append(f"{backend} manager loop_ms must be non-negative")
    for field in REQUIRED_STORAGE_INDEX_CONTRACT_FIELDS:
        if field not in cpp_index_contract:
            failures.append(f"cpp index contract missing field `{field}`")
        if field not in rust_index_contract:
            failures.append(f"rust index contract missing field `{field}`")
    for backend, index_contract in [("cpp", cpp_index_contract), ("rust", rust_index_contract)]:
        if index_contract.get("page_address_codec") != "PageAddress":
            failures.append(f"{backend} index contract page_address_codec must be PageAddress")
        if index_contract.get("block_address_codec") != "BlockAddress":
            failures.append(f"{backend} index contract block_address_codec must be BlockAddress")
        if index_contract.get("stable_order") != ["shard_id", "zone_id", "segment_id", "page_id", "offset"]:
            failures.append(f"{backend} index contract stable_order drift: {index_contract.get('stable_order')!r}")
        if index_contract.get("slot_index") != "slot -> object/page refs":
            failures.append(f"{backend} index contract slot_index mapping drift")
        if index_contract.get("object_index_entry") != "{model/table/object_key} -> current page chain":
            failures.append(f"{backend} index contract object_index_entry mapping drift")
        if index_contract.get("page_index") != "logical timestamp/key ranges -> page addresses":
            failures.append(f"{backend} index contract page_index mapping drift")
        if index_contract.get("block_index") != "page addresses -> physical durable locations":
            failures.append(f"{backend} index contract block_index mapping drift")
        if index_contract.get("required_behaviors") != REQUIRED_STORAGE_INDEX_BEHAVIORS:
            failures.append(f"{backend} index contract required_behaviors drift: {index_contract.get('required_behaviors')!r}")
        for flag in [
            "page_address_encode_decode",
            "block_address_encode_decode",
            "stable_order_verified",
            "timestamp_range_lookup_verified",
            "restart_rebuild_verified",
        ]:
            if index_contract.get(flag) is not True:
                failures.append(f"{backend} index contract {flag} must be true")
        for field in [
            "slot_index_entry_count",
            "slot_object_ref_count",
            "slot_page_ref_count",
            "object_index_entry_count",
            "page_index_entry_count",
            "block_index_entry_count",
        ]:
            value = _as_number(index_contract.get(field))
            if value is None or value <= 0:
                failures.append(f"{backend} index contract `{field}` must be positive")
        for field in ["unreadable_page_refs", "checksum_mismatches"]:
            if _as_number(index_contract.get(field)) not in (0.0, None):
                failures.append(f"{backend} index contract `{field}` must be zero")
    for field in REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS:
        if field not in cpp_cache_contract:
            failures.append(f"cpp cache contract missing field `{field}`")
        if field not in rust_cache_contract:
            failures.append(f"rust cache contract missing field `{field}`")
    for backend, cache_contract in [("cpp", cpp_cache_contract), ("rust", rust_cache_contract)]:
        if cache_contract.get("layers") != REQUIRED_STORAGE_CACHE_LAYERS:
            failures.append(f"{backend} cache contract layers drift: {cache_contract.get('layers')!r}")
        if cache_contract.get("semantics") != REQUIRED_STORAGE_CACHE_SEMANTICS:
            failures.append(f"{backend} cache contract semantics drift: {cache_contract.get('semantics')!r}")
        if cache_contract.get("metrics") != REQUIRED_STORAGE_CACHE_METRICS:
            failures.append(f"{backend} cache contract metrics drift: {cache_contract.get('metrics')!r}")
        for flag in [
            "hot_to_cold_lookup",
            "durable_refill_on_miss",
            "append_watermark_invalidation",
            "compaction_watermark_invalidation",
            "cold_scan_no_promote",
            "writeback_backpressure_measured",
        ]:
            if cache_contract.get(flag) is not True:
                failures.append(f"{backend} cache contract {flag} must be true")
        for field in [
            "cache_refills",
            "cache_invalidations",
            "cache_writeback_queue_depth",
            "cache_writeback_rejections",
            "hot_cache_promotions",
        ]:
            value = _as_number(cache_contract.get(field))
            if value is None or value < 0:
                failures.append(f"{backend} cache contract `{field}` must be non-negative")
        if _as_number(cache_contract.get("hot_cache_promotions")) not in (0.0, None):
            failures.append(f"{backend} cache contract hot_cache_promotions must be zero for cold scan no-promote parity")
    for field in REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS:
        if field not in cpp_reclaim_contract:
            failures.append(f"cpp reclaim contract missing field `{field}`")
        if field not in rust_reclaim_contract:
            failures.append(f"rust reclaim contract missing field `{field}`")
    for backend, reclaim_contract in [("cpp", cpp_reclaim_contract), ("rust", rust_reclaim_contract)]:
        for flag in [
            "cache_eviction_frees_memory_only",
            "logical_gc_marks_expired_deletable",
            "physical_reclaim_requires_compaction_or_safe_skip",
        ]:
            if reclaim_contract.get(flag) is not True:
                failures.append(f"{backend} reclaim contract {flag} must be true")
        for field in [
            "cache_evictions",
            "tombstone_records",
            "stale_page_tombstones",
            "stale_block_tombstones",
            "stale_pages_rewritten",
            "stale_pages_skipped",
            "stale_blocks_rewritten",
            "stale_blocks_skipped",
            "reclaimable_bytes",
            "compaction_reclaimed_bytes",
            "physical_reclaimed_bytes",
            "physical_reclaim_errors",
        ]:
            value = _as_number(reclaim_contract.get(field))
            if value is None or value < 0:
                failures.append(f"{backend} reclaim contract `{field}` must be non-negative")
        if _as_number(reclaim_contract.get("physical_reclaim_errors")) not in (0.0, None):
            failures.append(f"{backend} reclaim contract physical_reclaim_errors must be zero")
        physical = _metric_number(reclaim_contract, "physical_reclaimed_bytes")
        if physical > 0:
            tombstones = (
                _metric_number(reclaim_contract, "tombstone_records")
                + _metric_number(reclaim_contract, "stale_page_tombstones")
                + _metric_number(reclaim_contract, "stale_block_tombstones")
            )
            rewrite_or_skip = (
                _metric_number(reclaim_contract, "stale_pages_rewritten")
                + _metric_number(reclaim_contract, "stale_pages_skipped")
                + _metric_number(reclaim_contract, "stale_blocks_rewritten")
                + _metric_number(reclaim_contract, "stale_blocks_skipped")
            )
            if tombstones <= 0:
                failures.append(f"{backend} physical reclaim requires tombstone/logical GC evidence")
            if rewrite_or_skip <= 0:
                failures.append(f"{backend} physical reclaim requires compaction rewrite or safe-skip evidence")
            if _metric_number(reclaim_contract, "compaction_reclaimed_bytes") <= 0:
                failures.append(f"{backend} physical reclaim requires compaction_reclaimed_bytes")

    if cpp_write_sequence != REQUIRED_STORAGE_WRITE_SEQUENCE:
        failures.append(f"cpp storage_write_sequence drift: {cpp_write_sequence!r}")
    if rust_write_sequence != REQUIRED_STORAGE_WRITE_SEQUENCE:
        failures.append(f"rust storage_write_sequence drift: {rust_write_sequence!r}")
    if cpp_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"cpp storage_read_sequence drift: {cpp_read_sequence!r}")
    if rust_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"rust storage_read_sequence drift: {rust_read_sequence!r}")
    if cpp_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"cpp storage_cold_scan_sequence drift: {cpp_cold_scan_sequence!r}")
    if rust_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"rust storage_cold_scan_sequence drift: {rust_cold_scan_sequence!r}")
    if cpp_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"cpp storage_lifecycle_phases drift: {cpp_lifecycle_phases!r}")
    if rust_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"rust storage_lifecycle_phases drift: {rust_lifecycle_phases!r}")
    if cpp_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"cpp storage_reclaim_semantics drift: {cpp_reclaim_semantics!r}")
    if rust_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"rust storage_reclaim_semantics drift: {rust_reclaim_semantics!r}")
    if cpp_reclaim_scope != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"cpp storage_reclaim_scope drift: {cpp_reclaim_scope!r}")
    if rust_reclaim_scope != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"rust storage_reclaim_scope drift: {rust_reclaim_scope!r}")
    if cpp_cache_layers != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append(f"cpp storage_cache_layers drift: {cpp_cache_layers!r}")
    if rust_cache_layers != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append(f"rust storage_cache_layers drift: {rust_cache_layers!r}")
    if cpp_cache_semantics != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append(f"cpp storage_cache_semantics drift: {cpp_cache_semantics!r}")
    if rust_cache_semantics != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append(f"rust storage_cache_semantics drift: {rust_cache_semantics!r}")

    for backend, report in [("cpp", cpp_report), ("rust", rust_report)]:
        for path, key in _walk_public_keys(report):
            failures.append(
                f"{backend} public report exposes legacy alias `{key}` outside compatibility_aliases at {'.'.join(path)}"
            )

    for field in CANONICAL_JSON_FIELDS:
        if field not in cpp_public_shape:
            failures.append(f"cpp public storage shape missing canonical `{field}`")
        if field not in rust_public_shape:
            failures.append(f"rust public storage shape missing canonical `{field}`")
    comparable_fields = [field for field in CANONICAL_JSON_FIELDS if field in cpp_public_shape and field in rust_public_shape]
    for field in comparable_fields:
        if type(cpp_public_shape[field]).__name__ != type(rust_public_shape[field]).__name__:
            failures.append(
                f"public storage shape type drift `{field}`: cpp={type(cpp_public_shape[field]).__name__} "
                f"rust={type(rust_public_shape[field]).__name__}"
            )

    for metric in [
        "cold_scan_no_cache_reads",
        "hot_cache_promotions",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
        "append_watermark",
        "compaction_watermark",
    ]:
        cpp_value = _as_number(cpp_metrics.get(metric))
        rust_value = _as_number(rust_metrics.get(metric))
        if cpp_value is None or rust_value is None:
            continue
        if metric == "physical_reclaim_errors" and (cpp_value != 0 or rust_value != 0):
            failures.append(f"physical reclaim errors must be zero: cpp={cpp_value} rust={rust_value}")
        if metric.endswith("_watermark") and (cpp_value < 0 or rust_value < 0):
            failures.append(f"watermark must be non-negative for `{metric}`")
    failures.extend(_validate_physical_reclaim_evidence("cpp", cpp_metrics))
    failures.extend(_validate_physical_reclaim_evidence("rust", rust_metrics))
    failures.extend(_validate_stream_slot_index_evidence("cpp", cpp_metrics))
    failures.extend(_validate_stream_slot_index_evidence("rust", rust_metrics))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-report", type=pathlib.Path)
    parser.add_argument("--rust-report", type=pathlib.Path)
    parser.add_argument(
        "--skip-report-pair-corpus",
        action="store_true",
        help="Only validate docs/runner contract unless explicit reports are supplied.",
    )
    args = parser.parse_args()

    failures = validate_contract_and_runner()
    validated_pair = False
    if not args.skip_report_pair_corpus and not (args.cpp_report or args.rust_report):
        corpus = _load_json(REPORT_PAIR_CORPUS)
        failures.extend(validate_report_pair(corpus["cpp"], corpus["rust"]))
        validated_pair = True
    if bool(args.cpp_report) != bool(args.rust_report):
        failures.append("--cpp-report and --rust-report must be provided together")
    if args.cpp_report and args.rust_report:
        failures.extend(validate_report_pair(_load_json(args.cpp_report), _load_json(args.rust_report)))
        validated_pair = True

    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    print("storage lifecycle parity contract passed:")
    print("- storage_write_sequence=" + " -> ".join(REQUIRED_STORAGE_WRITE_SEQUENCE))
    print("- storage_read_sequence=" + " -> ".join(REQUIRED_STORAGE_READ_SEQUENCE))
    print("- storage_cold_scan_sequence=" + " -> ".join(REQUIRED_STORAGE_COLD_SCAN_SEQUENCE))
    print("- storage_lifecycle_phases=" + ", ".join(REQUIRED_STORAGE_LIFECYCLE_PHASES))
    print("- storage_reclaim_semantics=" + ", ".join(REQUIRED_STORAGE_RECLAIM_SEMANTICS))
    print(
        "- storage_reclaim_scope="
        + REQUIRED_STORAGE_RECLAIM_SCOPE["owner"]
        + " / context_gc="
        + REQUIRED_STORAGE_RECLAIM_SCOPE["matrixark_context_gc_role"]
    )
    print("- storage_cache_layers=" + ", ".join(REQUIRED_STORAGE_CACHE_LAYERS))
    print("- storage_cache_semantics=" + ", ".join(REQUIRED_STORAGE_CACHE_SEMANTICS))
    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        print(f"- {metric}")
    if validated_pair:
        print("- C++/Rust report pair exposes matching config, lifecycle metrics, and canonical public storage shape")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
