#!/usr/bin/env python3
"""Metrics state initialization for the MatrixArk Rust proxy client."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any


Json = dict[str, Any]


def initialize_rust_proxy_metrics_state(target: Any) -> None:
    target._metrics_lock = threading.Lock()
    target._commands_total = 0
    target._commands_failed_total = 0
    target._records_written_total = 0
    target._records_read_total = 0
    target._backpressure_rejections_total = 0
    target._timeouts_total = 0
    target._last_latency_ms = 0.0
    target._max_observed_latency_ms = 0.0
    target._latency_samples_ms: list[float] = []
    target._lane_latency_samples_ms: dict[str, list[float]] = {
        lane: [] for lane in target._lane_worker_counts
    }
    target._lane_commands_total: dict[str, int] = {
        lane: 0 for lane in target._lane_worker_counts
    }
    target._lane_wait_ms_total: dict[str, float] = {
        lane: 0.0 for lane in target._lane_worker_counts
    }
    target._lane_wait_ms_max: dict[str, float] = {
        lane: 0.0 for lane in target._lane_worker_counts
    }
    target._op_commands_total: dict[str, int] = {}
    target._op_latency_ms_total: dict[str, float] = {}
    target._op_latency_ms_max: dict[str, float] = {}
    target._serialization_ms_total = 0.0
    target._serialization_ms_max = 0.0
    target._rust_engine_ms_total = 0.0
    target._rust_engine_ms_max = 0.0
    target._scan_count_total = 0
    target._cache_hits_total = 0
    target._cache_misses_total = 0
    target._selected_refs_total = 0
    target._dropped_refs_total = 0
    target._memory_layer_budget_totals: dict[str, Any] = {
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_ref_type": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "final_session_boundary_ref_count": 0,
        "provisional_ref_count": 0,
        "final_ref_count": 0,
        "total_selected_refs": 0,
        "total_selected_tokens": 0,
    }
    target._context_record_counts: dict[str, int] = {}
    target._publish_visibility_calls_total = 0
    target._publish_visibility_keys_total = 0
    target._publish_visibility_full_shard_total = 0
    target._publish_visibility_index_bytes_total = 0
    target._publish_visibility_last_key_count = 0
    target._publish_visibility_last_index_bytes = 0
    target._batch_hset_coalesced_batches_total = 0
    target._batch_hset_coalesced_calls_total = 0
    target._batch_hset_coalesced_records_total = 0
    target._batch_hset_coalesced_wait_ms_total = 0.0
    target._batch_hset_coalesced_wait_ms_max = 0.0
    target._batch_hget_coalesced_batches_total = 0
    target._batch_hget_coalesced_calls_total = 0
    target._batch_hget_coalesced_records_total = 0
    target._batch_hget_coalesced_wait_ms_total = 0.0
    target._batch_hget_coalesced_wait_ms_max = 0.0
    target._append_coalesced_batches_total = 0
    target._append_coalesced_calls_total = 0
    target._append_coalesced_records_total = 0
    target._append_coalesced_wait_ms_total = 0.0
    target._append_coalesced_wait_ms_max = 0.0
    target._string_cache_hits_total = 0
    target._string_cache_misses_total = 0
    target._string_cache_updates_total = 0
    target._scan_hash_cache_hits_total = 0
    target._scan_hash_cache_misses_total = 0
    target._scan_hash_cache_updates_total = 0
    target._scan_hash_cache_invalidations_total = 0
    target._context_pack_response_cache_hits_total = 0
    target._context_pack_response_cache_misses_total = 0
    target._context_pack_response_cache_updates_total = 0
    target._context_pack_response_cache_invalidations_total = 0
    target._context_pack_response_singleflight_waits_total = 0
    target._context_pack_response_singleflight_wait_ms_total = 0.0
    target._context_pack_response_singleflight_wait_ms_max = 0.0


def initialize_rust_proxy_cache_state(target: Any) -> None:
    target._batch_hset_coalesce_lock = threading.Lock()
    target._batch_hset_coalesce_queue: list[Json] = []
    target._batch_hset_coalesce_active = False
    target._batch_hget_coalesce_lock = threading.Lock()
    target._batch_hget_coalesce_queue: list[Json] = []
    target._batch_hget_coalesce_active = False
    target._append_coalesce_lock = threading.Lock()
    target._append_coalesce_queue: list[Json] = []
    target._append_coalesce_active = False
    target._string_cache_lock = threading.Lock()
    target._string_cache: dict[str, str] = {}
    target._scan_hash_cache_lock = threading.Lock()
    target._scan_hash_cache: OrderedDict[str, Json] = OrderedDict()
    target._context_pack_response_cache_lock = threading.Lock()
    target._context_pack_response_cache: OrderedDict[str, Json] = OrderedDict()
    target._context_pack_response_inflight: dict[str, Json] = {}
