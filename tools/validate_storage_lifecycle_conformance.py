#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Validate conformance storage lifecycle parity contract and optional reports.

The lightweight default mode checks that the shared docs and scale runner agree
on the canonical StorageManager lifecycle metrics. When --native-report and
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
SCALE_REPORT = ROOT / "tools" / "run_matrixark_rust_scale_report.py"
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
    "native_public_name",
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
    "public_storage_feature_shapes",
    "storage_write_contract",
    "storage_read_contract",
    "storage_cold_scan_contract",
    "storage_manager_contract",
    "storage_index_contract",
    "storage_cache_contract",
    "storage_reclaim_contract",
    "storage_safety_snapshot",
    "storage_watermark_snapshot",
    "storage_gc_snapshot",
    "storage_index_snapshot",
    "storage_topology_snapshot",
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

REQUIRED_PUBLIC_STORAGE_FEATURE_SHAPES = {
    "page_address_fields": ["shard_id", "zone_id", "segment_id", "page_id", "offset", "length", "generation"],
    "block_address_fields": ["shard_id", "zone_id", "block_id", "offset", "length", "checksum"],
    "page_index_entry_fields": ["logical_key", "timestamp_range", "page_addresses", "append_watermark", "generation"],
    "block_index_entry_fields": ["page_address", "block_address", "extent", "checksum", "generation"],
    "object_index_entry_fields": ["model", "table", "object_key", "page_chain", "tombstone", "generation"],
    "storage_zone_fields": ["zone_id", "total_bytes", "used_bytes", "stale_bytes", "segments"],
    "stream_fields": ["stream_id", "segments", "rollover_count", "sealed_segment_count"],
    "segment_fields": ["segment_id", "extent", "start_offset", "sealed", "generation"],
    "extent_fields": ["extent", "block_range", "reclaim_state", "generation"],
    "slot_fields": ["slot_id", "dirty_generation", "object_refs", "page_refs", "tombstones", "owner_mismatch_count"],
    "append_watermark_fields": ["shard_id", "slot_id", "log_index", "timestamp_ms"],
    "compaction_watermark_fields": ["shard_id", "safe_generation", "safe_timestamp_ms", "follower_floor"],
    "tombstone_fields": ["ref", "generation", "deleted_at_ms", "reason"],
    "gc_eligibility_fields": ["ref", "eligible_after_ms", "has_tombstone", "follower_safe", "reclaimable_bytes"],
    "follower_cursor_safety_fields": ["min_follower_cursor", "blocked_reclaim_bytes", "safe_to_reclaim"],
}

REQUIRED_STORAGE_SAFETY_FIELDS = [
    "append_watermark",
    "compaction_watermark",
    "tombstone_records",
    "gc_eligible_record_count",
    "reclaimable_bytes",
    "follower_cursor_retention_floor",
    "follower_cursor_blocked_reclaim_count",
    "follower_cursor_safe_to_reclaim",
    "physical_reclaim_errors",
]

REQUIRED_STORAGE_GC_SNAPSHOT_FIELDS = [
    "tombstone_records",
    "stale_page_tombstones",
    "stale_block_tombstones",
    "gc_eligible_record_count",
    "reclaimable_bytes",
    "compaction_reclaimed_bytes",
    "physical_reclaimed_bytes",
    "physical_reclaim_errors",
    "follower_cursor_retention_floor",
    "follower_cursor_blocked_reclaim_count",
    "follower_cursor_safe_to_reclaim",
    "tombstone_samples",
    "gc_eligibility_samples",
    "follower_cursor_safety_samples",
]

REQUIRED_STORAGE_WATERMARK_SNAPSHOT_FIELDS = [
    "append_watermark",
    "compaction_watermark",
    "follower_cursor_retention_floor",
    "follower_cursor_safe_watermark",
    "page_index_rebuild_watermark",
    "block_index_rebuild_watermark",
    "object_index_rebuild_watermark",
    "append_watermark_samples",
    "compaction_watermark_samples",
]

REQUIRED_STORAGE_INDEX_SNAPSHOT_FIELDS = [
    "page_index_entry_count",
    "block_index_entry_count",
    "object_index_entry_count",
    "slot_index_entry_count",
    "slot_object_ref_count",
    "slot_page_ref_count",
    "page_address_count",
    "unreadable_page_refs",
    "checksum_mismatches",
    "missing_owner_ref_count",
    "owner_mismatch_count",
    "restart_rebuild_verified",
    "page_index_entry_samples",
    "block_index_entry_samples",
    "object_index_entry_samples",
]

REQUIRED_STORAGE_TOPOLOGY_SNAPSHOT_FIELDS = [
    "storage_zone_count",
    "active_storage_zones",
    "sealed_storage_zones",
    "stream_segment_count",
    "segment_open_count",
    "segment_sealed_count",
    "delayed_destroy_backlog",
    "storage_zone_total_bytes",
    "storage_zone_used_bytes",
    "storage_zone_stale_bytes",
    "append_log_replay_records",
    "append_log_reclaimed_records",
    "storage_zone_samples",
    "stream_samples",
    "segment_samples",
    "extent_samples",
    "slot_samples",
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
    "required_public_storage_feature_shapes": "PUBLIC_STORAGE_FEATURE_SHAPES",
}




def _scale_report_absent_message() -> str:
    """Why this validator cannot run, in one line an operator can act on.

    This compares a published contract against `tools/run_matrixark_rust_scale_report.py`, and that
    file is not in this repository -- `git log --all` finds no trace of it ever having been here,
    while five files still refer to it. With one side of the comparison missing there is nothing to
    check, so the honest outcome is a stated failure rather than a traceback (which reads as a bug
    in the validator) or a zero exit (which reads as conformance verified).

    Resolving it is a decision, not a fix: either the runner belongs in this repository, or these
    validators are describing a comparison this repository cannot make.
    """
    return (
        "cannot run: %s is absent from this repository, and it is the runner this validates the "
        "published contract against. Five files still refer to it and no version of this "
        "repository has contained it. Either it belongs here, or this validator does not."
        % SCALE_REPORT
    )


def validate_contract_and_runner() -> list[str]:
    if not SCALE_REPORT.exists():
        return [_scale_report_absent_message()]
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
    for shape_name, shape_fields in REQUIRED_PUBLIC_STORAGE_FEATURE_SHAPES.items():
        if f"`{shape_name}`" not in contract_text:
            failures.append(f"contract missing public storage feature shape `{shape_name}`")
        for field in shape_fields:
            if f"`{field}`" not in contract_text:
                failures.append(f"contract missing public storage feature field `{field}`")
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


def validate_report_pair(native_report: dict[str, Any], rust_report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    native_metrics = _dig_metrics(native_report)
    rust_metrics = _dig_metrics(rust_report)
    native_config = _dig_config(native_report)
    rust_config = _dig_config(rust_report)
    native_public_shape = _normalize_public_storage_shape(native_report)
    rust_public_shape = _normalize_public_storage_shape(rust_report)
    native_feature_shapes = _dig_public_storage_feature_shapes(native_report)
    rust_feature_shapes = _dig_public_storage_feature_shapes(rust_report)
    native_write_contract = _dig_write_contract(native_report)
    rust_write_contract = _dig_write_contract(rust_report)
    native_read_contract = _dig_read_contract(native_report)
    rust_read_contract = _dig_read_contract(rust_report)
    native_cold_scan_contract = _dig_cold_scan_contract(native_report)
    rust_cold_scan_contract = _dig_cold_scan_contract(rust_report)
    native_manager_contract = _dig_manager_contract(native_report)
    rust_manager_contract = _dig_manager_contract(rust_report)
    native_index_contract = _dig_index_contract(native_report)
    rust_index_contract = _dig_index_contract(rust_report)
    native_cache_contract = _dig_cache_contract(native_report)
    rust_cache_contract = _dig_cache_contract(rust_report)
    native_write_sequence = _dig_sequence(native_report, "storage_write_sequence")
    rust_write_sequence = _dig_sequence(rust_report, "storage_write_sequence")
    native_read_sequence = _dig_sequence(native_report, "storage_read_sequence")
    rust_read_sequence = _dig_sequence(rust_report, "storage_read_sequence")
    native_cold_scan_sequence = _dig_sequence(native_report, "storage_cold_scan_sequence")
    rust_cold_scan_sequence = _dig_sequence(rust_report, "storage_cold_scan_sequence")
    native_lifecycle_phases = _dig_lifecycle_phases(native_report)
    rust_lifecycle_phases = _dig_lifecycle_phases(rust_report)
    native_reclaim_semantics = _dig_reclaim_semantics(native_report)
    rust_reclaim_semantics = _dig_reclaim_semantics(rust_report)
    native_reclaim_scope = _dig_reclaim_scope(native_report)
    rust_reclaim_scope = _dig_reclaim_scope(rust_report)
    native_reclaim_contract = _dig_reclaim_contract(native_report)
    rust_reclaim_contract = _dig_reclaim_contract(rust_report)
    native_safety_snapshot = _dig_safety_snapshot(native_report)
    rust_safety_snapshot = _dig_safety_snapshot(rust_report)
    native_watermark_snapshot = _dig_watermark_snapshot(native_report)
    rust_watermark_snapshot = _dig_watermark_snapshot(rust_report)
    native_gc_snapshot = _dig_gc_snapshot(native_report)
    rust_gc_snapshot = _dig_gc_snapshot(rust_report)
    native_index_snapshot = _dig_index_snapshot(native_report)
    rust_index_snapshot = _dig_index_snapshot(rust_report)
    native_topology_snapshot = _dig_topology_snapshot(native_report)
    rust_topology_snapshot = _dig_topology_snapshot(rust_report)
    native_cache_layers = _dig_cache_layers(native_report)
    rust_cache_layers = _dig_cache_layers(rust_report)
    native_cache_semantics = _dig_cache_semantics(native_report)
    rust_cache_semantics = _dig_cache_semantics(rust_report)

    for backend, report in [("native", native_report), ("rust", rust_report)]:
        for key in REQUIRED_LIFECYCLE_TOP_LEVEL_KEYS:
            if key not in report:
                failures.append(f"{backend} report missing required top-level `{key}`")

    for field in REQUIRED_CONFIG_FIELDS:
        if field not in native_config:
            failures.append(f"native config missing `{field}`")
        if field not in rust_config:
            failures.append(f"rust config missing `{field}`")
        if field in native_config and field in rust_config and native_config[field] != rust_config[field]:
            failures.append(f"config drift `{field}`: native={native_config[field]!r} rust={rust_config[field]!r}")

    for metric in REQUIRED_STORAGE_LIFECYCLE_METRICS:
        if metric not in native_metrics:
            failures.append(f"native metrics missing `{metric}`")
        if metric not in rust_metrics:
            failures.append(f"rust metrics missing `{metric}`")
    for field in REQUIRED_STORAGE_WRITE_RESULT_FIELDS:
        if field not in native_write_contract:
            failures.append(f"native write contract missing result field `{field}`")
        if field not in rust_write_contract:
            failures.append(f"rust write contract missing result field `{field}`")
    for metric in REQUIRED_STORAGE_WRITE_METRICS:
        if metric not in native_write_contract:
            failures.append(f"native write contract missing metric `{metric}`")
        if metric not in rust_write_contract:
            failures.append(f"rust write contract missing metric `{metric}`")
    for field in ["durability", "storage_family", "write_mode"]:
        if field in native_write_contract and field in rust_write_contract and native_write_contract[field] != rust_write_contract[field]:
            failures.append(
                f"write contract drift `{field}`: native={native_write_contract[field]!r} rust={rust_write_contract[field]!r}"
            )
    for field in ["records_appended", "append_durability_failures"]:
        native_value = _as_number(native_write_contract.get(field))
        rust_value = _as_number(rust_write_contract.get(field))
        if native_value is not None and rust_value is not None and native_value != rust_value:
            failures.append(f"write contract drift `{field}`: native={native_value} rust={rust_value}")
    for field in ["append_watermark", "batch_watermark", "index_generation"]:
        native_value = _as_number(native_write_contract.get(field))
        rust_value = _as_number(rust_write_contract.get(field))
        if native_value is not None and native_value < 0:
            failures.append(f"native write contract `{field}` must be non-negative")
        if rust_value is not None and rust_value < 0:
            failures.append(f"rust write contract `{field}` must be non-negative")
    for backend, write_contract in [("native", native_write_contract), ("rust", rust_write_contract)]:
        if _as_number(write_contract.get("append_durability_failures")) not in (0.0, None):
            failures.append(f"{backend} write contract append_durability_failures must be zero")
        if _as_number(write_contract.get("records_appended")) == 0:
            failures.append(f"{backend} write contract records_appended must be positive")
    for field in REQUIRED_STORAGE_READ_RESULT_FIELDS:
        if field not in native_read_contract:
            failures.append(f"native read contract missing result field `{field}`")
        if field not in rust_read_contract:
            failures.append(f"rust read contract missing result field `{field}`")
    for metric in REQUIRED_STORAGE_READ_METRICS:
        if metric not in native_read_contract:
            failures.append(f"native read contract missing metric `{metric}`")
        if metric not in rust_read_contract:
            failures.append(f"rust read contract missing metric `{metric}`")
    for field in ["records_decoded", "records_returned", "tombstones_filtered", "stale_generations_filtered"]:
        native_value = _as_number(native_read_contract.get(field))
        rust_value = _as_number(rust_read_contract.get(field))
        if native_value is not None and native_value < 0:
            failures.append(f"native read contract `{field}` must be non-negative")
        if rust_value is not None and rust_value < 0:
            failures.append(f"rust read contract `{field}` must be non-negative")
    for backend, read_contract in [("native", native_read_contract), ("rust", rust_read_contract)]:
        decoded = _as_number(read_contract.get("records_decoded"))
        returned = _as_number(read_contract.get("records_returned"))
        if decoded is not None and returned is not None and returned > decoded:
            failures.append(f"{backend} read contract records_returned cannot exceed records_decoded")
        if read_contract.get("filter_policy") not in {"normal", "debug_replay", "cold_scan"}:
            failures.append(f"{backend} read contract filter_policy must be normal/debug_replay/cold_scan")
    for field in REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS:
        if field not in native_cold_scan_contract:
            failures.append(f"native cold scan contract missing result field `{field}`")
        if field not in rust_cold_scan_contract:
            failures.append(f"rust cold scan contract missing result field `{field}`")
    for metric in REQUIRED_STORAGE_COLD_SCAN_METRICS:
        if metric not in native_cold_scan_contract:
            failures.append(f"native cold scan contract missing metric `{metric}`")
        if metric not in rust_cold_scan_contract:
            failures.append(f"rust cold scan contract missing metric `{metric}`")
    for backend, cold_scan_contract in [("native", native_cold_scan_contract), ("rust", rust_cold_scan_contract)]:
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
        if field not in native_manager_contract:
            failures.append(f"native manager contract missing field `{field}`")
        if field not in rust_manager_contract:
            failures.append(f"rust manager contract missing field `{field}`")
    for backend, manager_contract in [("native", native_manager_contract), ("rust", rust_manager_contract)]:
        if manager_contract.get("manager_identity") != "StorageManager/StoreManager":
            failures.append(f"{backend} manager_identity must be StorageManager/StoreManager")
        if manager_contract.get("native_public_name") != "StorageManager":
            failures.append(f"{backend} native_public_name must be StorageManager")
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
        if field not in native_index_contract:
            failures.append(f"native index contract missing field `{field}`")
        if field not in rust_index_contract:
            failures.append(f"rust index contract missing field `{field}`")
    for backend, index_contract in [("native", native_index_contract), ("rust", rust_index_contract)]:
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
        if field not in native_cache_contract:
            failures.append(f"native cache contract missing field `{field}`")
        if field not in rust_cache_contract:
            failures.append(f"rust cache contract missing field `{field}`")
    for backend, cache_contract in [("native", native_cache_contract), ("rust", rust_cache_contract)]:
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
        if field not in native_reclaim_contract:
            failures.append(f"native reclaim contract missing field `{field}`")
        if field not in rust_reclaim_contract:
            failures.append(f"rust reclaim contract missing field `{field}`")
    for field in REQUIRED_STORAGE_SAFETY_FIELDS:
        if field not in native_safety_snapshot:
            failures.append(f"native safety snapshot missing field `{field}`")
        if field not in rust_safety_snapshot:
            failures.append(f"rust safety snapshot missing field `{field}`")
    for backend, safety_snapshot in [("native", native_safety_snapshot), ("rust", rust_safety_snapshot)]:
        for field in REQUIRED_STORAGE_SAFETY_FIELDS:
            if field == "follower_cursor_safe_to_reclaim":
                if not isinstance(safety_snapshot.get(field), bool):
                    failures.append(f"{backend} safety snapshot `{field}` must be boolean")
            else:
                value = _as_number(safety_snapshot.get(field))
                if value is None or value < 0:
                    failures.append(f"{backend} safety snapshot `{field}` must be non-negative")
        if _as_number(safety_snapshot.get("physical_reclaim_errors")) not in (0.0, None):
            failures.append(f"{backend} safety snapshot physical_reclaim_errors must be zero")
    for field in REQUIRED_STORAGE_WATERMARK_SNAPSHOT_FIELDS:
        if field not in native_watermark_snapshot:
            failures.append(f"native watermark snapshot missing field `{field}`")
        if field not in rust_watermark_snapshot:
            failures.append(f"rust watermark snapshot missing field `{field}`")
    for backend, watermark_snapshot in [
        ("native", native_watermark_snapshot),
        ("rust", rust_watermark_snapshot),
    ]:
        for field in REQUIRED_STORAGE_WATERMARK_SNAPSHOT_FIELDS:
            if field.endswith("_samples"):
                if not isinstance(watermark_snapshot.get(field), list):
                    failures.append(f"{backend} watermark snapshot `{field}` must be a list")
                continue
            value = _as_number(watermark_snapshot.get(field))
            if value is None or value < 0:
                failures.append(f"{backend} watermark snapshot `{field}` must be non-negative")
        for field, required_keys in [
            ("append_watermark_samples", ("shard_id", "slot_id", "log_index", "timestamp_ms")),
            (
                "compaction_watermark_samples",
                ("shard_id", "safe_generation", "safe_timestamp_ms", "follower_floor"),
            ),
        ]:
            for index, sample in enumerate(watermark_snapshot.get(field) or []):
                if not isinstance(sample, dict):
                    failures.append(f"{backend} watermark sample `{field}[{index}]` must be an object")
                    continue
                for key in required_keys:
                    if key not in sample:
                        failures.append(f"{backend} watermark sample `{field}[{index}]` missing `{key}`")
                    elif _as_number(sample.get(key)) is None:
                        failures.append(f"{backend} watermark sample `{field}[{index}].{key}` must be numeric")
        safe = _as_number(watermark_snapshot.get("follower_cursor_safe_watermark"))
        compaction = _as_number(watermark_snapshot.get("compaction_watermark"))
        follower_floor = _as_number(watermark_snapshot.get("follower_cursor_retention_floor"))
        if safe is not None and compaction is not None and safe > compaction:
            failures.append(f"{backend} watermark snapshot safe watermark must not exceed compaction watermark")
        if safe is not None and follower_floor is not None and follower_floor > 0 and safe > follower_floor:
            failures.append(f"{backend} watermark snapshot safe watermark must not exceed follower cursor floor")
    for field in REQUIRED_STORAGE_GC_SNAPSHOT_FIELDS:
        if field not in native_gc_snapshot:
            failures.append(f"native gc snapshot missing field `{field}`")
        if field not in rust_gc_snapshot:
            failures.append(f"rust gc snapshot missing field `{field}`")
    for backend, gc_snapshot in [("native", native_gc_snapshot), ("rust", rust_gc_snapshot)]:
        for field in REQUIRED_STORAGE_GC_SNAPSHOT_FIELDS:
            if field == "follower_cursor_safe_to_reclaim":
                if not isinstance(gc_snapshot.get(field), bool):
                    failures.append(f"{backend} gc snapshot `{field}` must be boolean")
            elif field.endswith("_samples"):
                if not isinstance(gc_snapshot.get(field), list):
                    failures.append(f"{backend} gc snapshot `{field}` must be a list")
            else:
                value = _as_number(gc_snapshot.get(field))
                if value is None or value < 0:
                    failures.append(f"{backend} gc snapshot `{field}` must be non-negative")
        if _as_number(gc_snapshot.get("physical_reclaim_errors")) not in (0.0, None):
            failures.append(f"{backend} gc snapshot physical_reclaim_errors must be zero")
        blocked = _as_number(gc_snapshot.get("follower_cursor_blocked_reclaim_count"))
        if blocked is not None and blocked > 0 and gc_snapshot.get("follower_cursor_safe_to_reclaim") is True:
            failures.append(f"{backend} gc snapshot cannot be safe to reclaim while follower cursor blocks exist")
    for field in REQUIRED_STORAGE_INDEX_SNAPSHOT_FIELDS:
        if field not in native_index_snapshot:
            failures.append(f"native index snapshot missing field `{field}`")
        if field not in rust_index_snapshot:
            failures.append(f"rust index snapshot missing field `{field}`")
    for backend, index_snapshot in [("native", native_index_snapshot), ("rust", rust_index_snapshot)]:
        for field in REQUIRED_STORAGE_INDEX_SNAPSHOT_FIELDS:
            if field == "restart_rebuild_verified":
                if not isinstance(index_snapshot.get(field), bool):
                    failures.append(f"{backend} index snapshot `{field}` must be boolean")
            elif field.endswith("_samples"):
                if not isinstance(index_snapshot.get(field), list):
                    failures.append(f"{backend} index snapshot `{field}` must be a list")
            else:
                value = _as_number(index_snapshot.get(field))
                if value is None or value < 0:
                    failures.append(f"{backend} index snapshot `{field}` must be non-negative")
        for field in ["unreadable_page_refs", "checksum_mismatches"]:
            if _as_number(index_snapshot.get(field)) not in (0.0, None):
                failures.append(f"{backend} index snapshot `{field}` must be zero")
    for field in REQUIRED_STORAGE_TOPOLOGY_SNAPSHOT_FIELDS:
        if field not in native_topology_snapshot:
            failures.append(f"native topology snapshot missing field `{field}`")
        if field not in rust_topology_snapshot:
            failures.append(f"rust topology snapshot missing field `{field}`")
    for backend, topology_snapshot in [
        ("native", native_topology_snapshot),
        ("rust", rust_topology_snapshot),
    ]:
        for field in REQUIRED_STORAGE_TOPOLOGY_SNAPSHOT_FIELDS:
            if field.endswith("_samples"):
                if not isinstance(topology_snapshot.get(field), list):
                    failures.append(f"{backend} topology snapshot `{field}` must be a list")
                continue
            value = _as_number(topology_snapshot.get(field))
            if value is None or value < 0:
                failures.append(f"{backend} topology snapshot `{field}` must be non-negative")
    for backend, reclaim_contract in [("native", native_reclaim_contract), ("rust", rust_reclaim_contract)]:
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

    if native_write_sequence != REQUIRED_STORAGE_WRITE_SEQUENCE:
        failures.append(f"native storage_write_sequence drift: {native_write_sequence!r}")
    if rust_write_sequence != REQUIRED_STORAGE_WRITE_SEQUENCE:
        failures.append(f"rust storage_write_sequence drift: {rust_write_sequence!r}")
    if native_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"native storage_read_sequence drift: {native_read_sequence!r}")
    if rust_read_sequence != REQUIRED_STORAGE_READ_SEQUENCE:
        failures.append(f"rust storage_read_sequence drift: {rust_read_sequence!r}")
    if native_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"native storage_cold_scan_sequence drift: {native_cold_scan_sequence!r}")
    if rust_cold_scan_sequence != REQUIRED_STORAGE_COLD_SCAN_SEQUENCE:
        failures.append(f"rust storage_cold_scan_sequence drift: {rust_cold_scan_sequence!r}")
    if native_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"native storage_lifecycle_phases drift: {native_lifecycle_phases!r}")
    if rust_lifecycle_phases != REQUIRED_STORAGE_LIFECYCLE_PHASES:
        failures.append(f"rust storage_lifecycle_phases drift: {rust_lifecycle_phases!r}")
    if native_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"native storage_reclaim_semantics drift: {native_reclaim_semantics!r}")
    if rust_reclaim_semantics != REQUIRED_STORAGE_RECLAIM_SEMANTICS:
        failures.append(f"rust storage_reclaim_semantics drift: {rust_reclaim_semantics!r}")
    if native_reclaim_scope != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"native storage_reclaim_scope drift: {native_reclaim_scope!r}")
    if rust_reclaim_scope != REQUIRED_STORAGE_RECLAIM_SCOPE:
        failures.append(f"rust storage_reclaim_scope drift: {rust_reclaim_scope!r}")
    if native_cache_layers != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append(f"native storage_cache_layers drift: {native_cache_layers!r}")
    if rust_cache_layers != REQUIRED_STORAGE_CACHE_LAYERS:
        failures.append(f"rust storage_cache_layers drift: {rust_cache_layers!r}")
    if native_cache_semantics != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append(f"native storage_cache_semantics drift: {native_cache_semantics!r}")
    if rust_cache_semantics != REQUIRED_STORAGE_CACHE_SEMANTICS:
        failures.append(f"rust storage_cache_semantics drift: {rust_cache_semantics!r}")

    for backend, report in [("native", native_report), ("rust", rust_report)]:
        for path, key in _walk_public_keys(report):
            failures.append(
                f"{backend} public report exposes legacy alias `{key}` outside compatibility_aliases at {'.'.join(path)}"
            )

    for field in CANONICAL_JSON_FIELDS:
        if field not in native_public_shape:
            failures.append(f"native public storage shape missing canonical `{field}`")
        if field not in rust_public_shape:
            failures.append(f"rust public storage shape missing canonical `{field}`")
    comparable_fields = [field for field in CANONICAL_JSON_FIELDS if field in native_public_shape and field in rust_public_shape]
    for field in comparable_fields:
        if type(native_public_shape[field]).__name__ != type(rust_public_shape[field]).__name__:
            failures.append(
                f"public storage shape type drift `{field}`: native={type(native_public_shape[field]).__name__} "
                f"rust={type(rust_public_shape[field]).__name__}"
            )

    for shape_name, expected_fields in REQUIRED_PUBLIC_STORAGE_FEATURE_SHAPES.items():
        native_fields = native_feature_shapes.get(shape_name)
        rust_fields = rust_feature_shapes.get(shape_name)
        if native_fields != expected_fields:
            failures.append(f"native public_storage_feature_shapes.{shape_name} drift: {native_fields!r}")
        if rust_fields != expected_fields:
            failures.append(f"rust public_storage_feature_shapes.{shape_name} drift: {rust_fields!r}")

    for metric in [
        "cold_scan_no_cache_reads",
        "hot_cache_promotions",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
        "append_watermark",
        "compaction_watermark",
    ]:
        native_value = _as_number(native_metrics.get(metric))
        rust_value = _as_number(rust_metrics.get(metric))
        if native_value is None or rust_value is None:
            continue
        if metric == "physical_reclaim_errors" and (native_value != 0 or rust_value != 0):
            failures.append(f"physical reclaim errors must be zero: native={native_value} rust={rust_value}")
        if metric.endswith("_watermark") and (native_value < 0 or rust_value < 0):
            failures.append(f"watermark must be non-negative for `{metric}`")
    failures.extend(_validate_physical_reclaim_evidence("native", native_metrics))
    failures.extend(_validate_physical_reclaim_evidence("rust", rust_metrics))
    failures.extend(_validate_stream_slot_index_evidence("native", native_metrics))
    failures.extend(_validate_stream_slot_index_evidence("rust", rust_metrics))
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--native-report", type=pathlib.Path)
    parser.add_argument("--rust-report", type=pathlib.Path)
    parser.add_argument(
        "--skip-report-pair-corpus",
        action="store_true",
        help="Only validate docs/runner contract unless explicit reports are supplied.",
    )
    args = parser.parse_args()

    failures = validate_contract_and_runner()
    validated_pair = False
    if not args.skip_report_pair_corpus and not (args.native_report or args.rust_report):
        corpus = _load_json(REPORT_PAIR_CORPUS)
        failures.extend(validate_report_pair(corpus["native"], corpus["rust"]))
        validated_pair = True
    if bool(args.native_report) != bool(args.rust_report):
        failures.append("--native-report and --rust-report must be provided together")
    if args.native_report and args.rust_report:
        failures.extend(validate_report_pair(_load_json(args.native_report), _load_json(args.rust_report)))
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
        print("- conformance report pair exposes matching config, lifecycle metrics, and canonical public storage shape")
    return 0



# Re-export helpers split into validate_storage_lifecycle_extractors.py
try:  # package path
    from .validate_storage_lifecycle_extractors import *  # noqa: E402,F401,F403
except ImportError:  # top-level path
    from validate_storage_lifecycle_extractors import *  # noqa: E402,F401,F403


if __name__ == "__main__":
    raise SystemExit(main())
