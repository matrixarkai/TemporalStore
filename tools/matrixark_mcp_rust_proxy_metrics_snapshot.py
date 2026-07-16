#!/usr/bin/env python3
"""Rust proxy metrics snapshot rendering."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import Json
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    from matrixark_mcp_metrics import MatrixArkServiceMetrics


def metrics_snapshot(client: Any) -> Json:
    self = client
    with self._metrics_lock:
        elapsed_s = max(0.001, time.time() - self._started_at)
        samples = list(self._latency_samples_ms)
        context_counts = dict(sorted(self._context_record_counts.items()))
        lane_samples = {lane: list(values) for lane, values in self._lane_latency_samples_ms.items()}
        lane_metrics = {
            lane: {
                "workers": self._lane_worker_counts.get(lane, 0),
                "commands_total": self._lane_commands_total.get(lane, 0),
                "wait_ms_total": round(self._lane_wait_ms_total.get(lane, 0.0), 3),
                "wait_ms_max": round(self._lane_wait_ms_max.get(lane, 0.0), 3),
                "queue_wait_ms_total": round(self._lane_wait_ms_total.get(lane, 0.0), 3),
                "queue_wait_ms_max": round(self._lane_wait_ms_max.get(lane, 0.0), 3),
                "p95_latency_ms": round(self._percentile(values, 0.95), 3),
                "p99_latency_ms": round(self._percentile(values, 0.99), 3),
            }
            for lane, values in lane_samples.items()
        }
        op_metrics = {
            op: {
                "commands_total": count,
                "latency_ms_total": round(self._op_latency_ms_total.get(op, 0.0), 3),
                "latency_ms_avg": round(self._op_latency_ms_total.get(op, 0.0) / max(1, count), 3),
                "latency_ms_max": round(self._op_latency_ms_max.get(op, 0.0), 3),
            }
            for op, count in sorted(self._op_commands_total.items())
        }
        return {
            "gateway_mode": "rust_native_proxy",
            "sdk_mode": "rust_native_proxy",
            "transport": "stdio",
            "proxy_path": self.proxy_path,
            "cli_path": self.cli_path,
            "shared_process_mode": self._shared_process_mode,
            "max_inflight": sum(self._lane_worker_counts.get(group, 0) for group in ("write", "read", "pack", "control")),
            "lane_pool": {
                "write": self._lane_worker_counts.get("write", 0),
                "read": self._lane_worker_counts.get("read", 0),
                "pack": self._lane_worker_counts.get("pack", 0),
                "control": self._lane_worker_counts.get("control", 0),
            },
            "lanes": lane_metrics,
            "write_pool_size": self._lane_worker_counts.get("write", 0),
            "read_pool_size": self._lane_worker_counts.get("read", 0),
            "pack_pool_size": self._lane_worker_counts.get("pack", 0),
            "control_pool_size": self._lane_worker_counts.get("control", 0),
            "write_pool_enabled": self._lane_worker_counts.get("write", 0) > 1,
            "read_pool_enabled": self._lane_worker_counts.get("read", 0) > 1,
            "pack_pool_enabled": self._lane_worker_counts.get("pack", 0) > 1,
            "backpressure_timeout_ms": int(self._backpressure_timeout_s * 1000),
            "commands_total": self._commands_total,
            "commands_failed_total": self._commands_failed_total,
            "timeouts_total": self._timeouts_total,
            "qps": round(self._commands_total / elapsed_s, 6),
            "records_written_total": self._records_written_total,
            "records_read_total": self._records_read_total,
            "backpressure_rejections_total": self._backpressure_rejections_total,
            "proxy_queue_wait_ms_total": round(sum(self._lane_wait_ms_total.values()), 3),
            "proxy_queue_wait_ms_max": round(max(self._lane_wait_ms_max.values()) if self._lane_wait_ms_max else 0.0, 3),
            "serialization_ms_total": round(self._serialization_ms_total, 3),
            "serialization_ms_max": round(self._serialization_ms_max, 3),
            "rust_engine_ms_total": round(self._rust_engine_ms_total, 3),
            "rust_engine_ms_max": round(self._rust_engine_ms_max, 3),
            "scan_count_total": self._scan_count_total,
            "cache_hits_total": self._cache_hits_total,
            "cache_misses_total": self._cache_misses_total,
            "selected_refs_total": self._selected_refs_total,
            "dropped_refs_total": self._dropped_refs_total,
            "publish_visibility": {
                "calls_total": self._publish_visibility_calls_total,
                "keys_total": self._publish_visibility_keys_total,
                "keys_avg": round(
                    self._publish_visibility_keys_total / max(1, self._publish_visibility_calls_total),
                    3,
                ),
                "full_shard_total": self._publish_visibility_full_shard_total,
                "index_bytes_total": self._publish_visibility_index_bytes_total,
                "index_bytes_avg": round(
                    self._publish_visibility_index_bytes_total / max(1, self._publish_visibility_calls_total),
                    3,
                ),
                "last_key_count": self._publish_visibility_last_key_count,
                "last_index_bytes": self._publish_visibility_last_index_bytes,
            },
            "batch_hset_coalescing": {
                "enabled": self._batch_hset_coalesce_enabled,
                "max_batches": self._batch_hset_coalesce_max_batches,
                "min_records": self._batch_hset_coalesce_min_records,
                "wait_ms": round(self._batch_hset_coalesce_wait_s * 1000.0, 3),
                "batches_total": self._batch_hset_coalesced_batches_total,
                "calls_total": self._batch_hset_coalesced_calls_total,
                "records_total": self._batch_hset_coalesced_records_total,
                "wait_ms_total": round(self._batch_hset_coalesced_wait_ms_total, 3),
                "wait_ms_max": round(self._batch_hset_coalesced_wait_ms_max, 3),
            },
            "batch_hget_coalescing": {
                "enabled": self._batch_hget_coalesce_enabled,
                "max_batches": self._batch_hget_coalesce_max_batches,
                "min_records": self._batch_hget_coalesce_min_records,
                "wait_ms": round(self._batch_hget_coalesce_wait_s * 1000.0, 3),
                "batches_total": self._batch_hget_coalesced_batches_total,
                "calls_total": self._batch_hget_coalesced_calls_total,
                "records_total": self._batch_hget_coalesced_records_total,
                "wait_ms_total": round(self._batch_hget_coalesced_wait_ms_total, 3),
                "wait_ms_max": round(self._batch_hget_coalesced_wait_ms_max, 3),
            },
            "matrixark_append_coalescing": {
                "enabled": self._append_coalesce_enabled,
                "max_batches": self._append_coalesce_max_batches,
                "min_records": self._append_coalesce_min_records,
                "wait_ms": round(self._append_coalesce_wait_s * 1000.0, 3),
                "batches_total": self._append_coalesced_batches_total,
                "calls_total": self._append_coalesced_calls_total,
                "records_total": self._append_coalesced_records_total,
                "wait_ms_total": round(self._append_coalesced_wait_ms_total, 3),
                "wait_ms_max": round(self._append_coalesced_wait_ms_max, 3),
            },
            "string_cache": {
                "enabled": self._string_cache_enabled,
                "entries": len(self._string_cache),
                "hits_total": self._string_cache_hits_total,
                "misses_total": self._string_cache_misses_total,
                "updates_total": self._string_cache_updates_total,
                "scope": "record_count_and_record_index_keys",
            },
            "scan_hash_cache": {
                "enabled": self._scan_hash_cache_enabled,
                "max_entries": self._scan_hash_cache_max_entries,
                "entries": len(self._scan_hash_cache),
                "hits_total": self._scan_hash_cache_hits_total,
                "misses_total": self._scan_hash_cache_misses_total,
                "updates_total": self._scan_hash_cache_updates_total,
                "invalidations_total": self._scan_hash_cache_invalidations_total,
                "scope": "hash_key_with_write_invalidation",
            },
            "context_pack_response_cache": {
                "enabled": self._context_pack_response_cache_enabled,
                "max_entries": self._context_pack_response_cache_max_entries,
                "entries": len(self._context_pack_response_cache),
                "hits_total": self._context_pack_response_cache_hits_total,
                "misses_total": self._context_pack_response_cache_misses_total,
                "updates_total": self._context_pack_response_cache_updates_total,
                "invalidations_total": self._context_pack_response_cache_invalidations_total,
                "singleflight_waits_total": self._context_pack_response_singleflight_waits_total,
                "singleflight_wait_ms_total": round(self._context_pack_response_singleflight_wait_ms_total, 3),
                "singleflight_wait_ms_max": round(self._context_pack_response_singleflight_wait_ms_max, 3),
                "scope": "native_context_pack_request_envelope_with_write_invalidation",
            },
            "last_latency_ms": round(self._last_latency_ms, 3),
            "latency_ms_sum": round(sum(samples), 3),
            "latency_ms_count": len(samples),
            "latency_ms_max": round(max(samples) if samples else 0.0, 3),
            "latency_buckets": {str(int(bucket) if bucket != float("inf") else "+Inf"): sum(1 for value in samples if value <= bucket) for bucket in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS},
            "p95_latency_ms": round(self._percentile(samples, 0.95), 3),
            "p99_latency_ms": round(self._percentile(samples, 0.99), 3),
            "max_observed_latency_ms": round(self._max_observed_latency_ms, 3),
            "matrixark_context_records_total": sum(context_counts.values()),
            "matrixark_context_records_by_type": context_counts,
            "op_metrics": op_metrics,
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only",
            "native_proxy": True,
            "direct_sdk_bridge": False,
            "pure_embedded_direct_sdk": False,
            "supports_health": True,
            "supports_readiness": True,
            "supports_metrics": True,
            "supports_batch_append": True,
            "supports_matrixark_batch_append_records": True,
            "supports_matrixark_retrieve_context_pack": True,
            "supports_compact_secondary_index_lookup": True,
            "supports_placement_key_candidate_fetch": True,
            "supports_context_pack_telemetry": True,
            "supports_native_append_queue": True,
            "supports_coalesced_writes": True,
            "supports_coalesced_reads": True,
            "supports_coalesced_appends": True,
            "supports_placement_key_routing": True,
            "supports_prefix_scan": True,
            "supports_graceful_shutdown": True,
            "structured_errors": True,
            "matrixark_batch_append_wire_format": "entries_compact",
        }
