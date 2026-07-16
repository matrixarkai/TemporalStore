#!/usr/bin/env python3
"""Rust proxy call-metrics primitive helpers."""

from __future__ import annotations

import json
import math
from typing import Any

Json = dict[str, Any]


def nested_float(payload: Json, *paths: str) -> float:
    for path in paths:
        current: Any = payload
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current is None:
            continue
        try:
            return float(current)
        except (TypeError, ValueError):
            continue
    return 0.0


def count_context_record(context_record_counts: dict[str, int], value: Any) -> None:
    if not isinstance(value, str) or not value.startswith("{"):
        return
    if '"record_type"' not in value:
        return
    try:
        payload = json.loads(value)
    except Exception:
        return
    record_type = str(payload.get("record_type") or "")
    if not record_type:
        return
    context_record_counts[record_type] = context_record_counts.get(record_type, 0) + 1


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile_value * len(ordered)) - 1))
    return ordered[index]


def record_call_metrics(
    target: Any,
    op: str,
    kwargs: Json,
    response: Json | None,
    elapsed_ms: float,
    *,
    failed: bool,
    backpressure: bool = False,
    lane: str = "control",
    wait_ms: float = 0.0,
) -> None:
    with target._metrics_lock:
        target._commands_total += 1
        target._lane_commands_total[lane] = target._lane_commands_total.get(lane, 0) + 1
        target._lane_wait_ms_total[lane] = target._lane_wait_ms_total.get(lane, 0.0) + max(0.0, wait_ms)
        target._lane_wait_ms_max[lane] = max(target._lane_wait_ms_max.get(lane, 0.0), max(0.0, wait_ms))
        target._op_commands_total[op] = target._op_commands_total.get(op, 0) + 1
        target._op_latency_ms_total[op] = target._op_latency_ms_total.get(op, 0.0) + max(0.0, elapsed_ms)
        target._op_latency_ms_max[op] = max(target._op_latency_ms_max.get(op, 0.0), max(0.0, elapsed_ms))
        if failed:
            target._commands_failed_total += 1
            if "timed out" in str(response or "").lower() or elapsed_ms >= target.request_timeout_ms:
                target._timeouts_total += 1
        if backpressure:
            target._backpressure_rejections_total += 1
        if response:
            serialization_ms = nested_float(
                response,
                "serialization_time_ms",
                "serialization_ms",
                "serialization_time",
            )
            engine_ms = nested_float(
                response,
                "rust_engine_time_ms",
                "engine_ms",
                "rust_engine_ms",
            )
            target._serialization_ms_total += serialization_ms
            target._serialization_ms_max = max(target._serialization_ms_max, serialization_ms)
            target._rust_engine_ms_total += engine_ms
            target._rust_engine_ms_max = max(target._rust_engine_ms_max, engine_ms)
            scan_count = int(
                nested_float(
                    response,
                    "scan_count",
                    "scan_stats.scanned_records",
                    "context_pack.recall_policy.scan_stats.scanned_records",
                )
                or 0
            )
            target._scan_count_total += scan_count
            cache_hit = bool(response.get("cache_hit") or response.get("cache_hit_used"))
            if cache_hit:
                target._cache_hits_total += 1
            elif op in {"matrixark_scan_candidates", "matrixark_retrieve_context_pack"}:
                target._cache_misses_total += 1
            selected_count = int(
                nested_float(
                    response,
                    "selected_ref_count",
                    "context_pack.selected_ref_count",
                )
                or 0
            )
            if not selected_count and isinstance(response.get("context_pack"), dict):
                refs = response["context_pack"].get("selected_refs") or response["context_pack"].get("remote_context_refs") or []
                if isinstance(refs, list):
                    selected_count = len(refs)
            target._selected_refs_total += selected_count
            dropped_count = int(
                nested_float(
                    response,
                    "dropped_ref_count",
                    "context_pack.dropped_ref_count",
                )
                or 0
            )
            if not dropped_count and isinstance(response.get("context_pack"), dict):
                dropped = response["context_pack"].get("dropped_refs")
                if isinstance(dropped, dict):
                    reasons = dropped.get("reason_counts")
                    if isinstance(reasons, dict):
                        dropped_count = sum(int(value or 0) for value in reasons.values())
            target._dropped_refs_total += dropped_count
        target._last_latency_ms = elapsed_ms
        target._max_observed_latency_ms = max(target._max_observed_latency_ms, elapsed_ms)
        target._latency_samples_ms.append(elapsed_ms)
        if len(target._latency_samples_ms) > 2048:
            del target._latency_samples_ms[: len(target._latency_samples_ms) - 2048]
        lane_samples = target._lane_latency_samples_ms.setdefault(lane, [])
        lane_samples.append(elapsed_ms)
        if len(lane_samples) > 1024:
            del lane_samples[: len(lane_samples) - 1024]
        if response and response.get("ok"):
            count = int(response.get("count") or 0)
            if op in {"put_string", "hset"}:
                target._records_written_total += 1
                target._count_context_record(kwargs.get("value"))
            elif op in {"batch_hset", "matrixark_append_records", "matrixark_batch_append_records"}:
                compact_entries = kwargs.get("entries_compact") or []
                entries_for_key = kwargs.get("entries_for_key") or []
                entries = kwargs.get("entries") or []
                target._records_written_total += count or len(compact_entries) or len(entries_for_key) or len(entries)
                for entry in entries:
                    if isinstance(entry, dict):
                        target._count_context_record(entry.get("value"))
                for entry in compact_entries:
                    if isinstance(entry, (list, tuple)) and len(entry) >= 3:
                        target._count_context_record(entry[2])
                for entry in entries_for_key:
                    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                        target._count_context_record(entry[1])
            elif op in {"get_string", "hget"}:
                target._records_read_total += 1
            elif op in {"batch_hget", "hgetall", "scan_hash"}:
                target._records_read_total += count
            elif op == "matrixark_publish_visibility":
                visibility_keys = kwargs.get("visibility_keys") if isinstance(kwargs, dict) else []
                key_count = len(visibility_keys) if isinstance(visibility_keys, list) else 0
                index_bytes = int(
                    nested_float(
                        response,
                        "matrixark_visibility_index_bytes",
                        "extra.matrixark_visibility_index_bytes",
                        "count",
                    )
                    or 0
                )
                full_shard = bool(
                    response.get("matrixark_visibility_full_shard")
                    or (isinstance(response.get("extra"), dict) and response["extra"].get("matrixark_visibility_full_shard"))
                    or key_count == 0
                )
                target._publish_visibility_calls_total += 1
                target._publish_visibility_keys_total += key_count
                target._publish_visibility_full_shard_total += 1 if full_shard else 0
                target._publish_visibility_index_bytes_total += index_bytes
                target._publish_visibility_last_key_count = key_count
                target._publish_visibility_last_index_bytes = index_bytes
