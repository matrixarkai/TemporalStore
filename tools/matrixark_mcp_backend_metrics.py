#!/usr/bin/env python3
"""Backend metrics helpers for TemporalStore-backed MatrixArk adapters."""

from __future__ import annotations

import threading
from typing import Any

try:
    from tools.matrixark_mcp_core import Json, now_ms
    from tools.matrixark_mcp_backend_metric_state import ensure_backend_metric_state, metric_average
    from tools.matrixark_mcp_direct_write_queue import ensure_direct_write_queue_fields
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_native_helpers import (
        latency_quantile_from_cumulative_buckets as _latency_quantile_from_cumulative_buckets,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, now_ms
    from matrixark_mcp_backend_metric_state import ensure_backend_metric_state, metric_average
    from matrixark_mcp_direct_write_queue import ensure_direct_write_queue_fields
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_native_helpers import (
        latency_quantile_from_cumulative_buckets as _latency_quantile_from_cumulative_buckets,
    )


def ensure_temporal_backend_metric_fields(target: Any) -> None:
    ensure_backend_metric_state(target, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS)
    if not hasattr(target, "_backend_ready"):
        target._backend_ready = False
    if not hasattr(target, "_records_cache"):
        target._records_cache = []
    if not hasattr(target, "_retrieval_candidate_cache"):
        target._retrieval_candidate_cache = {}
    if not hasattr(target, "_retrieval_candidate_cache_lock"):
        target._retrieval_candidate_cache_lock = threading.RLock()
    if not hasattr(target, "_audit_buffer"):
        target._audit_buffer = []
    if not hasattr(target, "_audit_flush_failures"):
        target._audit_flush_failures = 0
    ensure_direct_write_queue_fields(target)


def observe_append_queue_wait(target: Any, elapsed_ms: float) -> None:
    ensure_temporal_backend_metric_fields(target)
    with target._metrics_lock:
        target._append_queue_wait_ms_total += max(0.0, float(elapsed_ms))
        target._append_queue_wait_count += 1


def observe_append_engine(target: Any, elapsed_ms: float) -> None:
    ensure_temporal_backend_metric_fields(target)
    with target._metrics_lock:
        target._append_engine_ms_total += max(0.0, float(elapsed_ms))
        target._append_engine_count += 1


def append_queue_wait_ms_avg(target: Any) -> float:
    return metric_average(
        getattr(target, "_append_queue_wait_ms_total", 0.0),
        getattr(target, "_append_queue_wait_count", 0),
    )


def append_engine_ms_avg(target: Any) -> float:
    return metric_average(
        getattr(target, "_append_engine_ms_total", 0.0),
        getattr(target, "_append_engine_count", 0),
    )


def observe_backend_command(
    target: Any,
    elapsed_ms: float,
    *,
    records_written: int = 0,
    records_read: int = 0,
    failed: bool = False,
) -> None:
    ensure_temporal_backend_metric_fields(target)
    with target._metrics_lock:
        target._commands_total += 1
        if failed:
            target._errors_total += 1
        if elapsed_ms >= 0:
            target._latency_sum_ms += float(elapsed_ms)
            target._latency_max_ms = max(target._latency_max_ms, float(elapsed_ms))
            for index, bucket in enumerate(MatrixArkServiceMetrics.LATENCY_BUCKETS_MS):
                if elapsed_ms <= bucket:
                    target._latency_buckets[index] += 1
        target._records_written_total += int(records_written or 0)
        target._records_read_total += int(records_read or 0)


def backend_prometheus(target: Any) -> str:
    ensure_temporal_backend_metric_fields(target)
    backend = "cpp" if target._backend_label() in {"temporalstore-direct", "temporalstore-cpp"} else target._backend_label()
    with target._metrics_lock:
        elapsed_s = max(0.001, (now_ms() - target._metrics_started_at_ms) / 1000.0)
        lines = [
            "# HELP matrixark_backend_qps MatrixArk storage backend command QPS.",
            "# TYPE matrixark_backend_qps gauge",
            f'matrixark_backend_qps{{backend="{backend}"}} {round(target._commands_total / elapsed_s, 6)}',
            "# HELP matrixark_backend_commands_total MatrixArk storage backend command count.",
            "# TYPE matrixark_backend_commands_total counter",
            f'matrixark_backend_commands_total{{backend="{backend}"}} {target._commands_total}',
            "# HELP matrixark_backend_errors_total MatrixArk storage backend command errors.",
            "# TYPE matrixark_backend_errors_total counter",
            f'matrixark_backend_errors_total{{backend="{backend}"}} {target._errors_total}',
            "# HELP matrixark_backend_timeouts_total MatrixArk storage backend command timeouts.",
            "# TYPE matrixark_backend_timeouts_total counter",
            f'matrixark_backend_timeouts_total{{backend="{backend}"}} {target._timeouts_total}',
            "# HELP matrixark_backend_info MatrixArk storage backend identity and mode.",
            "# TYPE matrixark_backend_info gauge",
            f'matrixark_backend_info{{backend="{backend}",storage_mode="direct-sdk"}} 1',
            "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{backend="{backend}",storage_mode="direct-sdk",status="{"ready" if target._backend_ready else "unknown"}"}} {1 if target._backend_ready else 0}',
            "# HELP matrixark_backend_command_latency_ms MatrixArk storage backend command latency quantiles.",
            "# TYPE matrixark_backend_command_latency_ms gauge",
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.50"}} {round(_latency_quantile_from_cumulative_buckets(target._latency_buckets, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, target._commands_total, 0.50), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.95"}} {round(_latency_quantile_from_cumulative_buckets(target._latency_buckets, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, target._commands_total, 0.95), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.99"}} {round(_latency_quantile_from_cumulative_buckets(target._latency_buckets, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, target._commands_total, 0.99), 3)}',
            "# HELP matrixark_backend_command_latency_ms_bucket MatrixArk storage backend command latency buckets.",
            "# TYPE matrixark_backend_command_latency_ms_bucket counter",
            "# HELP matrixark_backend_command_latency_ms_sum MatrixArk storage backend command latency sum in milliseconds.",
            "# TYPE matrixark_backend_command_latency_ms_sum counter",
            f'matrixark_backend_command_latency_ms_sum{{backend="{backend}"}} {round(target._latency_sum_ms, 3)}',
            "# HELP matrixark_backend_command_latency_ms_count MatrixArk storage backend command latency sample count.",
            "# TYPE matrixark_backend_command_latency_ms_count counter",
            f'matrixark_backend_command_latency_ms_count{{backend="{backend}"}} {target._commands_total}',
            "# HELP matrixark_backend_command_latency_max_ms MatrixArk storage backend maximum command latency in milliseconds.",
            "# TYPE matrixark_backend_command_latency_max_ms gauge",
            f'matrixark_backend_command_latency_max_ms{{backend="{backend}"}} {round(target._latency_max_ms, 3)}',
        ]
        for bucket, count in zip(MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, target._latency_buckets):
            le = "+Inf" if bucket == float("inf") else str(int(bucket))
            lines.append(f'matrixark_backend_command_latency_ms_bucket{{backend="{backend}",le="{le}"}} {int(count)}')
        lines.extend(
            [
                "# HELP matrixark_backend_records_written_total MatrixArk storage backend records written.",
                "# TYPE matrixark_backend_records_written_total counter",
                f'matrixark_backend_records_written_total{{backend="{backend}"}} {target._records_written_total}',
                "# HELP matrixark_backend_records_read_total MatrixArk storage backend records read.",
                "# TYPE matrixark_backend_records_read_total counter",
                f'matrixark_backend_records_read_total{{backend="{backend}"}} {target._records_read_total}',
                "# HELP matrixark_context_records_total MatrixArk context records currently cached by backend.",
                "# TYPE matrixark_context_records_total gauge",
                f'matrixark_context_records_total{{backend="{backend}"}} {len(target._records_cache or [])}',
                "# HELP matrixark_backend_cached_clients MatrixArk storage backend cached clients.",
                "# TYPE matrixark_backend_cached_clients gauge",
                f'matrixark_backend_cached_clients{{backend="{backend}"}} 1',
                "# HELP matrixark_backend_matrixark_native_batch_append_available MatrixArk native batch append C API availability.",
                "# TYPE matrixark_backend_matrixark_native_batch_append_available gauge",
                f'matrixark_backend_matrixark_native_batch_append_available{{backend="{backend}",write_path="{getattr(target, "_matrixark_append_write_path", "unknown")}"}} {1 if bool(getattr(target, "_matrixark_native_batch_append_available", False)) else 0}',
                "# HELP matrixark_backend_matrixark_per_record_hset_fallback MatrixArk write path is using the old per-record HSet fallback.",
                "# TYPE matrixark_backend_matrixark_per_record_hset_fallback gauge",
                f'matrixark_backend_matrixark_per_record_hset_fallback{{backend="{backend}",write_path="{getattr(target, "_matrixark_append_write_path", "unknown")}"}} {1 if bool(getattr(target, "_matrixark_append_uses_per_record_hset", True)) else 0}',
                "# HELP matrixark_backend_context_extension_append_selected MatrixArk writes are using native CONTEXT extension append commands.",
                "# TYPE matrixark_backend_context_extension_append_selected gauge",
                f'matrixark_backend_context_extension_append_selected{{backend="{backend}"}} {1 if bool(getattr(target, "_matrixark_context_extension_append_selected", False)) else 0}',
                "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.",
                "# TYPE matrixark_backend_audit_buffered_records gauge",
                f'matrixark_backend_audit_buffered_records{{backend="{backend}"}} {len(getattr(target, "_audit_buffer", []))}',
                "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.",
                "# TYPE matrixark_backend_audit_flush_failures_total counter",
                f'matrixark_backend_audit_flush_failures_total{{backend="{backend}"}} {int(getattr(target, "_audit_flush_failures", 0) or 0)}',
                "# HELP matrixark_backend_write_queue_depth MatrixArk direct backend queued write batches.",
                "# TYPE matrixark_backend_write_queue_depth gauge",
                f'matrixark_backend_write_queue_depth{{backend="{backend}"}} {getattr(target, "_direct_write_queue", None).qsize() if hasattr(target, "_direct_write_queue") else 0}',
                "# HELP matrixark_backend_write_queue_durable_pending_batches MatrixArk durable TemporalStore-backed write queue pending batches.",
                "# TYPE matrixark_backend_write_queue_durable_pending_batches gauge",
                f'matrixark_backend_write_queue_durable_pending_batches{{backend="{backend}",mode="{getattr(target, "_direct_write_queue_mode", "memory")}"}} {target._direct_write_durable_pending_count() if getattr(target, "_direct_write_queue_mode", "memory") == "temporalstore" else 0}',
                "# HELP matrixark_backend_write_queue_failures_total MatrixArk direct backend background write failures.",
                "# TYPE matrixark_backend_write_queue_failures_total counter",
                f'matrixark_backend_write_queue_failures_total{{backend="{backend}"}} {int(getattr(target, "_direct_write_failures", 0) or 0)}',
                "# HELP matrixark_backend_write_queue_enqueued_records_total MatrixArk direct backend records accepted into the async write queue.",
                "# TYPE matrixark_backend_write_queue_enqueued_records_total counter",
                f'matrixark_backend_write_queue_enqueued_records_total{{backend="{backend}"}} {int(getattr(target, "_direct_write_enqueued_records", 0) or 0)}',
                "# HELP matrixark_backend_write_queue_flushed_records_total MatrixArk direct backend queued records flushed to TemporalStore.",
                "# TYPE matrixark_backend_write_queue_flushed_records_total counter",
                f'matrixark_backend_write_queue_flushed_records_total{{backend="{backend}"}} {int(getattr(target, "_direct_write_flushed_records", 0) or 0)}',
                "# HELP matrixark_backend_write_queue_dead_letter_batches_total MatrixArk durable direct write queue batches moved to dead letter.",
                "# TYPE matrixark_backend_write_queue_dead_letter_batches_total counter",
                f'matrixark_backend_write_queue_dead_letter_batches_total{{backend="{backend}"}} {int(getattr(target, "_direct_write_dead_letter_batches", 0) or 0)}',
                "# HELP matrixark_backend_append_queue_wait_ms MatrixArk append queue wait time average in milliseconds.",
                "# TYPE matrixark_backend_append_queue_wait_ms gauge",
                f'matrixark_backend_append_queue_wait_ms{{backend="{backend}"}} {round(append_queue_wait_ms_avg(target), 3)}',
                "# HELP matrixark_backend_append_engine_ms MatrixArk append engine execution time average in milliseconds.",
                "# TYPE matrixark_backend_append_engine_ms gauge",
                f'matrixark_backend_append_engine_ms{{backend="{backend}"}} {round(append_engine_ms_avg(target), 3)}',
            ]
        )
        return "\n".join(lines) + "\n"


def backend_metrics(target: Any) -> Json:
    return {
        "backend": target._backend_label(),
        "metrics_format": "prometheus",
        "prometheus": backend_prometheus(target),
        "capabilities": {
            "health_endpoint": True,
            "readiness_endpoint": True,
            "metrics_endpoint": True,
            "matrixark_batch_append_records": True,
            "matrixark_retrieve_context_pack": callable(getattr(target._client, "matrixark_retrieve_context_pack", None)),
            "compact_secondary_index_lookup": True,
            "placement_key_candidate_fetch": True,
            "context_pack_telemetry": True,
        },
        "metrics": {
            "mode": "cpp-proxy" if getattr(target, "_matrixark_proxy_mode", False) else "direct-sdk",
            "cpp_proxy_endpoint": getattr(target, "_cpp_proxy_endpoint", ""),
            "metaserver": target._metaserver,
            "namespace": target._namespace,
            "table": target._table,
            "storage_prefix": target._storage_prefix,
            "raw_ingestion_backend": target._normalize_raw_storage_backend(
                getattr(target, "_raw_storage_backend", "temporalstore")
            ),
            "raw_ingestion_prefix": getattr(
                target,
                "_raw_ingestion_prefix",
                f"{target._storage_prefix}:raw_ingestion",
            ),
            "audit_mode": target._audit_mode,
            "audit_buffered_records": len(target._audit_buffer),
            "audit_flush_failures": target._audit_flush_failures,
            "write_queue_enabled": bool(getattr(target, "_direct_write_queue_enabled", False)),
            "write_queue_mode": getattr(target, "_direct_write_queue_mode", "memory"),
            "write_queue_depth": getattr(target, "_direct_write_queue", None).qsize() if hasattr(target, "_direct_write_queue") else 0,
            "write_queue_durable_pending_batches": target._direct_write_durable_pending_count() if getattr(target, "_direct_write_queue_mode", "memory") == "temporalstore" else 0,
            "write_queue_failures": int(getattr(target, "_direct_write_failures", 0) or 0),
            "write_queue_enqueued_records": int(getattr(target, "_direct_write_enqueued_records", 0) or 0),
            "write_queue_flushed_records": int(getattr(target, "_direct_write_flushed_records", 0) or 0),
            "write_queue_enqueued_batches": int(getattr(target, "_direct_write_enqueued_batches", 0) or 0),
            "write_queue_flushed_batches": int(getattr(target, "_direct_write_flushed_batches", 0) or 0),
            "write_queue_dead_letter_batches": int(getattr(target, "_direct_write_dead_letter_batches", 0) or 0),
            "append_queue_wait_ms": round(append_queue_wait_ms_avg(target), 3),
            "append_queue_wait_count": int(getattr(target, "_append_queue_wait_count", 0) or 0),
            "append_engine_ms": round(append_engine_ms_avg(target), 3),
            "append_engine_count": int(getattr(target, "_append_engine_count", 0) or 0),
            "entry_count_cache": target._entry_count_cache,
            "python_hot_cache_allowed": target.python_hot_cache_enabled(),
            "records_cache_ready": target._records_cache is not None,
            "commands_total": target._commands_total,
            "errors_total": target._errors_total,
            "timeouts_total": target._timeouts_total,
            "records_written_total": target._records_written_total,
            "records_read_total": target._records_read_total,
        },
    }
