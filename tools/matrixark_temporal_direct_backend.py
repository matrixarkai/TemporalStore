# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_TemporalDirectBackendMixin methods split from matrixark_mcp_temporal_adapters.MatrixArkTemporalStoreDirectAdapter (mixin)."""
from __future__ import annotations

try:
    from tools.matrixark_mcp_env import env_bool
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool


try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
    from tools.matrixark_mcp_temporal_append import slim_persisted_record
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403
    from matrixark_mcp_temporal_append import slim_persisted_record

try:  # package path
    from tools.matrixark_temporal_location_codec import (
        compact_location,
        compact_location_list,
        expand_location,
    )
except ImportError:
    from matrixark_temporal_location_codec import (
        compact_location,
        compact_location_list,
        expand_location,
    )

try:  # names owned by the parent module
    from tools.matrixark_mcp_temporal_adapters import (
    MatrixArkLocalAdapter,
    MatrixArkServiceMetrics,
    TEMPORAL_COMPRESSED_OLD_RECORD_TYPES,
    _durable_recovery_record_identity,
    _latency_quantile_from_cumulative_buckets,
    _mcp_debug_log,
    _records_with_matrixark_write_debug,
    matrixark_record_retention_filtered,
    normalize_raw_ingestion_record,
    queue,
    time,
)
except ImportError:
    from matrixark_mcp_temporal_adapters import (
    MatrixArkLocalAdapter,
    MatrixArkServiceMetrics,
    TEMPORAL_COMPRESSED_OLD_RECORD_TYPES,
    _durable_recovery_record_identity,
    _latency_quantile_from_cumulative_buckets,
    _mcp_debug_log,
    _records_with_matrixark_write_debug,
    matrixark_record_retention_filtered,
    normalize_raw_ingestion_record,
    queue,
    time,
)


class _TemporalDirectBackendMixin:
    def _matrixark_batch_append_records_with_options(
        self,
        append_records: Any,
        entries: list[Json],
        *,
        count_key: str,
        count_value: str,
        append_options: Json,
    ) -> None:
        try:
            append_records(
                entries,
                count_key=count_key,
                count_value=count_value,
                append_options=append_options,
            )
        except TypeError as exc:
            if "append_options" not in str(exc):
                raise
            append_records(entries, count_key=count_key, count_value=count_value)
            calls = getattr(getattr(append_records, "__self__", None), "calls", None)
            if isinstance(calls, list) and calls and isinstance(calls[-1], dict):
                calls[-1].setdefault("append_options", dict(append_options or {}))

    def _ensure_direct_write_queue_fields(self) -> None:
        if not hasattr(self, "_direct_write_queue_enabled"):
            self._direct_write_queue_enabled = env_bool("MATRIXARK_DIRECT_WRITE_QUEUE", False)
        if not hasattr(self, "_direct_write_queue_max_records"):
            self._direct_write_queue_max_records = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", "10000")))
        if not hasattr(self, "_direct_write_queue_put_timeout_s"):
            self._direct_write_queue_put_timeout_s = max(0.01, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", "1000")) / 1000.0)
        if not hasattr(self, "_direct_write_queue_mode"):
            self._direct_write_queue_mode = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory").strip().lower() or "memory"
        if self._direct_write_queue_mode not in {"memory", "temporalstore"}:
            self._direct_write_queue_mode = "memory"
        if not hasattr(self, "_direct_write_queue_drain_max_batches"):
            self._direct_write_queue_drain_max_batches = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES", "64")))
        if not hasattr(self, "_direct_write_queue_allow_sync_context"):
            self._direct_write_queue_allow_sync_context = env_bool("MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT", False)
        if not hasattr(self, "_direct_write_queue_autostart"):
            self._direct_write_queue_autostart = True
        if not hasattr(self, "_native_side_index_assume_fresh"):
            self._native_side_index_assume_fresh = env_bool("MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH", False)
        if not hasattr(self, "_direct_raw_ingestion_queue_enabled"):
            self._direct_raw_ingestion_queue_enabled = env_bool("MATRIXARK_DIRECT_RAW_INGESTION_QUEUE", False)
        if not hasattr(self, "_direct_write_queue_key"):
            self._direct_write_queue_key = f"{self._storage_prefix}:direct_write_queue"
        if not hasattr(self, "_direct_write_queue_done_key"):
            self._direct_write_queue_done_key = f"{self._storage_prefix}:direct_write_queue_done"
        if not hasattr(self, "_direct_write_queue_dead_key"):
            self._direct_write_queue_dead_key = f"{self._storage_prefix}:direct_write_queue_dead"
        if not hasattr(self, "_direct_write_queue"):
            self._direct_write_queue = queue.Queue(maxsize=int(self._direct_write_queue_max_records))
        if not hasattr(self, "_direct_write_worker_started"):
            self._direct_write_worker_started = False
        if not hasattr(self, "_direct_write_worker_lock"):
            self._direct_write_worker_lock = threading.RLock()
        if not hasattr(self, "_direct_write_stop"):
            self._direct_write_stop = threading.Event()
        if not hasattr(self, "_direct_write_failures"):
            self._direct_write_failures = 0
        if not hasattr(self, "_direct_write_enqueued_records"):
            self._direct_write_enqueued_records = 0
        if not hasattr(self, "_direct_write_flushed_records"):
            self._direct_write_flushed_records = 0
        if not hasattr(self, "_direct_write_enqueued_batches"):
            self._direct_write_enqueued_batches = 0
        if not hasattr(self, "_direct_write_flushed_batches"):
            self._direct_write_flushed_batches = 0
        if not hasattr(self, "_direct_write_dead_letter_batches"):
            self._direct_write_dead_letter_batches = 0

    def _observe_append_queue_wait(self, elapsed_ms: float) -> None:
        self._ensure_backend_metric_fields()
        with self._metrics_lock:
            self._append_queue_wait_ms_total += max(0.0, float(elapsed_ms))
            self._append_queue_wait_count += 1

    def _observe_append_engine(self, elapsed_ms: float) -> None:
        self._ensure_backend_metric_fields()
        with self._metrics_lock:
            self._append_engine_ms_total += max(0.0, float(elapsed_ms))
            self._append_engine_count += 1

    def _append_queue_wait_ms_avg(self) -> float:
        count = int(getattr(self, "_append_queue_wait_count", 0) or 0)
        return float(getattr(self, "_append_queue_wait_ms_total", 0.0) or 0.0) / count if count else 0.0

    def _append_engine_ms_avg(self) -> float:
        count = int(getattr(self, "_append_engine_count", 0) or 0)
        return float(getattr(self, "_append_engine_ms_total", 0.0) or 0.0) / count if count else 0.0

    def _observe_backend_command(self, elapsed_ms: float, *, records_written: int = 0, records_read: int = 0, failed: bool = False) -> None:
        self._ensure_backend_metric_fields()
        with self._metrics_lock:
            self._commands_total += 1
            if failed:
                self._errors_total += 1
            if elapsed_ms >= 0:
                self._latency_sum_ms += float(elapsed_ms)
                self._latency_max_ms = max(self._latency_max_ms, float(elapsed_ms))
                for index, bucket in enumerate(MatrixArkServiceMetrics.LATENCY_BUCKETS_MS):
                    if elapsed_ms <= bucket:
                        self._latency_buckets[index] += 1
            self._records_written_total += int(records_written or 0)
            self._records_read_total += int(records_read or 0)

    def _backend_prometheus(self) -> str:
        self._ensure_backend_metric_fields()
        backend = "native" if self._backend_label() in {"temporalstore-direct", "temporalstore-native"} else self._backend_label()
        with self._metrics_lock:
            elapsed_s = max(0.001, (now_ms() - self._metrics_started_at_ms) / 1000.0)
            lines = [
                "# HELP matrixark_backend_qps MatrixArk storage backend command QPS.",
                "# TYPE matrixark_backend_qps gauge",
                f'matrixark_backend_qps{{backend="{backend}"}} {round(self._commands_total / elapsed_s, 6)}',
                "# HELP matrixark_backend_commands_total MatrixArk storage backend command count.",
                "# TYPE matrixark_backend_commands_total counter",
                f'matrixark_backend_commands_total{{backend="{backend}"}} {self._commands_total}',
                "# HELP matrixark_backend_errors_total MatrixArk storage backend command errors.",
                "# TYPE matrixark_backend_errors_total counter",
                f'matrixark_backend_errors_total{{backend="{backend}"}} {self._errors_total}',
                "# HELP matrixark_backend_timeouts_total MatrixArk storage backend command timeouts.",
                "# TYPE matrixark_backend_timeouts_total counter",
                f'matrixark_backend_timeouts_total{{backend="{backend}"}} {self._timeouts_total}',
                "# HELP matrixark_backend_info MatrixArk storage backend identity and mode.",
                "# TYPE matrixark_backend_info gauge",
                f'matrixark_backend_info{{backend="{backend}",storage_mode="direct-sdk"}} 1',
                "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
                "# TYPE matrixark_backend_ready gauge",
                f'matrixark_backend_ready{{backend="{backend}",storage_mode="direct-sdk",status="{"ready" if self._backend_ready else "unknown"}"}} {1 if self._backend_ready else 0}',
                "# HELP matrixark_backend_command_latency_ms MatrixArk storage backend command latency quantiles.",
                "# TYPE matrixark_backend_command_latency_ms gauge",
                f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.50"}} {round(_latency_quantile_from_cumulative_buckets(self._latency_buckets, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, self._commands_total, 0.50), 3)}',
                f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.95"}} {round(_latency_quantile_from_cumulative_buckets(self._latency_buckets, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, self._commands_total, 0.95), 3)}',
                f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.99"}} {round(_latency_quantile_from_cumulative_buckets(self._latency_buckets, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, self._commands_total, 0.99), 3)}',
                "# HELP matrixark_backend_command_latency_ms_bucket MatrixArk storage backend command latency buckets.",
                "# TYPE matrixark_backend_command_latency_ms_bucket counter",
                "# HELP matrixark_backend_command_latency_ms_sum MatrixArk storage backend command latency sum in milliseconds.",
                "# TYPE matrixark_backend_command_latency_ms_sum counter",
                f'matrixark_backend_command_latency_ms_sum{{backend="{backend}"}} {round(self._latency_sum_ms, 3)}',
                "# HELP matrixark_backend_command_latency_ms_count MatrixArk storage backend command latency sample count.",
                "# TYPE matrixark_backend_command_latency_ms_count counter",
                f'matrixark_backend_command_latency_ms_count{{backend="{backend}"}} {self._commands_total}',
                "# HELP matrixark_backend_command_latency_max_ms MatrixArk storage backend maximum command latency in milliseconds.",
                "# TYPE matrixark_backend_command_latency_max_ms gauge",
                f'matrixark_backend_command_latency_max_ms{{backend="{backend}"}} {round(self._latency_max_ms, 3)}',
            ]
            for bucket, count in zip(MatrixArkServiceMetrics.LATENCY_BUCKETS_MS, self._latency_buckets):
                le = "+Inf" if bucket == float("inf") else str(int(bucket))
                lines.append(f'matrixark_backend_command_latency_ms_bucket{{backend="{backend}",le="{le}"}} {int(count)}')
            lines.extend(
                [
                    "# HELP matrixark_backend_records_written_total MatrixArk storage backend records written.",
                    "# TYPE matrixark_backend_records_written_total counter",
                    f'matrixark_backend_records_written_total{{backend="{backend}"}} {self._records_written_total}',
                    "# HELP matrixark_backend_records_read_total MatrixArk storage backend records read.",
                    "# TYPE matrixark_backend_records_read_total counter",
                    f'matrixark_backend_records_read_total{{backend="{backend}"}} {self._records_read_total}',
                    "# HELP matrixark_context_records_total MatrixArk context records currently cached by backend.",
                    "# TYPE matrixark_context_records_total gauge",
                    f'matrixark_context_records_total{{backend="{backend}"}} {len(self._records_cache or [])}',
                    "# HELP matrixark_backend_cached_clients MatrixArk storage backend cached clients.",
                    "# TYPE matrixark_backend_cached_clients gauge",
                    f'matrixark_backend_cached_clients{{backend="{backend}"}} 1',
                    "# HELP matrixark_backend_matrixark_native_batch_append_available MatrixArk native batch append C API availability.",
                    "# TYPE matrixark_backend_matrixark_native_batch_append_available gauge",
                    f'matrixark_backend_matrixark_native_batch_append_available{{backend="{backend}",write_path="{getattr(self, "_matrixark_append_write_path", "unknown")}"}} {1 if bool(getattr(self, "_matrixark_native_batch_append_available", False)) else 0}',
                    "# HELP matrixark_backend_matrixark_per_record_hset_fallback MatrixArk write path is using the old per-record HSet fallback.",
                    "# TYPE matrixark_backend_matrixark_per_record_hset_fallback gauge",
                    f'matrixark_backend_matrixark_per_record_hset_fallback{{backend="{backend}",write_path="{getattr(self, "_matrixark_append_write_path", "unknown")}"}} {1 if bool(getattr(self, "_matrixark_append_uses_per_record_hset", True)) else 0}',
                    "# HELP matrixark_backend_context_extension_append_selected MatrixArk writes are using native CONTEXT extension append commands.",
                    "# TYPE matrixark_backend_context_extension_append_selected gauge",
                    f'matrixark_backend_context_extension_append_selected{{backend="{backend}"}} {1 if bool(getattr(self, "_matrixark_context_extension_append_selected", False)) else 0}',
                    "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.",
                    "# TYPE matrixark_backend_audit_buffered_records gauge",
                    f'matrixark_backend_audit_buffered_records{{backend="{backend}"}} {len(getattr(self, "_audit_buffer", []))}',
                    "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.",
                    "# TYPE matrixark_backend_audit_flush_failures_total counter",
                    f'matrixark_backend_audit_flush_failures_total{{backend="{backend}"}} {int(getattr(self, "_audit_flush_failures", 0) or 0)}',
                    "# HELP matrixark_backend_write_queue_depth MatrixArk direct backend queued write batches.",
                    "# TYPE matrixark_backend_write_queue_depth gauge",
                    f'matrixark_backend_write_queue_depth{{backend="{backend}"}} {getattr(self, "_direct_write_queue", None).qsize() if hasattr(self, "_direct_write_queue") else 0}',
                    "# HELP matrixark_backend_write_queue_durable_pending_batches MatrixArk durable TemporalStore-backed write queue pending batches.",
                    "# TYPE matrixark_backend_write_queue_durable_pending_batches gauge",
                    f'matrixark_backend_write_queue_durable_pending_batches{{backend="{backend}",mode="{getattr(self, "_direct_write_queue_mode", "memory")}"}} {self._direct_write_durable_pending_count() if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore" else 0}',
                    "# HELP matrixark_backend_write_queue_failures_total MatrixArk direct backend background write failures.",
                    "# TYPE matrixark_backend_write_queue_failures_total counter",
                    f'matrixark_backend_write_queue_failures_total{{backend="{backend}"}} {int(getattr(self, "_direct_write_failures", 0) or 0)}',
                    "# HELP matrixark_backend_write_queue_enqueued_records_total MatrixArk direct backend records accepted into the async write queue.",
                    "# TYPE matrixark_backend_write_queue_enqueued_records_total counter",
                    f'matrixark_backend_write_queue_enqueued_records_total{{backend="{backend}"}} {int(getattr(self, "_direct_write_enqueued_records", 0) or 0)}',
                    "# HELP matrixark_backend_write_queue_flushed_records_total MatrixArk direct backend queued records flushed to TemporalStore.",
                    "# TYPE matrixark_backend_write_queue_flushed_records_total counter",
                    f'matrixark_backend_write_queue_flushed_records_total{{backend="{backend}"}} {int(getattr(self, "_direct_write_flushed_records", 0) or 0)}',
                    "# HELP matrixark_backend_write_queue_dead_letter_batches_total MatrixArk durable direct write queue batches moved to dead letter.",
                    "# TYPE matrixark_backend_write_queue_dead_letter_batches_total counter",
                    f'matrixark_backend_write_queue_dead_letter_batches_total{{backend="{backend}"}} {int(getattr(self, "_direct_write_dead_letter_batches", 0) or 0)}',
                    "# HELP matrixark_backend_append_queue_wait_ms MatrixArk append queue wait time average in milliseconds.",
                    "# TYPE matrixark_backend_append_queue_wait_ms gauge",
                    f'matrixark_backend_append_queue_wait_ms{{backend="{backend}"}} {round(self._append_queue_wait_ms_avg(), 3)}',
                    "# HELP matrixark_backend_append_engine_ms MatrixArk append engine execution time average in milliseconds.",
                    "# TYPE matrixark_backend_append_engine_ms gauge",
                    f'matrixark_backend_append_engine_ms{{backend="{backend}"}} {round(self._append_engine_ms_avg(), 3)}',
                ]
            )
            return "\n".join(lines) + "\n"

    def backend_metrics(self) -> Json:
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "prometheus": self._backend_prometheus(),
            "capabilities": {
                "health_endpoint": True,
                "readiness_endpoint": True,
                "metrics_endpoint": True,
                "matrixark_batch_append_records": True,
                "matrixark_retrieve_context_pack": callable(getattr(self._client, "matrixark_retrieve_context_pack", None)),
                "compact_secondary_index_lookup": True,
                "placement_key_candidate_fetch": True,
                "context_pack_telemetry": True,
            },
            "metrics": {
                "mode": "native-proxy" if getattr(self, "_matrixark_proxy_mode", False) else "direct-sdk",
                "native_proxy_endpoint": getattr(self, "_proxy_endpoint", ""),
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "raw_ingestion_backend": self._normalize_raw_storage_backend(
                    getattr(self, "_raw_storage_backend", "temporalstore")
                ),
                "raw_ingestion_prefix": getattr(
                    self,
                    "_raw_ingestion_prefix",
                    f"{self._storage_prefix}:raw_ingestion",
                ),
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
                "write_queue_enabled": bool(getattr(self, "_direct_write_queue_enabled", False)),
                "write_queue_mode": getattr(self, "_direct_write_queue_mode", "memory"),
                "write_queue_depth": getattr(self, "_direct_write_queue", None).qsize() if hasattr(self, "_direct_write_queue") else 0,
                "write_queue_durable_pending_batches": self._direct_write_durable_pending_count() if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore" else 0,
                "write_queue_failures": int(getattr(self, "_direct_write_failures", 0) or 0),
                "write_queue_enqueued_records": int(getattr(self, "_direct_write_enqueued_records", 0) or 0),
                "write_queue_flushed_records": int(getattr(self, "_direct_write_flushed_records", 0) or 0),
                "write_queue_enqueued_batches": int(getattr(self, "_direct_write_enqueued_batches", 0) or 0),
                "write_queue_flushed_batches": int(getattr(self, "_direct_write_flushed_batches", 0) or 0),
                "write_queue_dead_letter_batches": int(getattr(self, "_direct_write_dead_letter_batches", 0) or 0),
                "append_queue_wait_ms": round(self._append_queue_wait_ms_avg(), 3),
                "append_queue_wait_count": int(getattr(self, "_append_queue_wait_count", 0) or 0),
                "append_engine_ms": round(self._append_engine_ms_avg(), 3),
                "append_engine_count": int(getattr(self, "_append_engine_count", 0) or 0),
                "entry_count_cache": self._entry_count_cache,
                "python_hot_cache_allowed": self.python_hot_cache_enabled(),
                "records_cache_ready": self._records_cache is not None,
                "async_context_warmup_enabled": bool(getattr(self, "_async_context_warmup_enabled", False)),
                "async_context_warmup_in_progress": bool(getattr(self, "_async_context_warmup_in_progress", False)),
                "async_context_warmup_started_total": int(getattr(self, "_async_context_warmup_started_total", 0) or 0),
                "async_context_warmup_completed_total": int(getattr(self, "_async_context_warmup_completed_total", 0) or 0),
                "async_context_warmup_failed_total": int(getattr(self, "_async_context_warmup_failed_total", 0) or 0),
                "async_context_warmup": dict(getattr(self, "_async_context_warmup_status", {"status": "unknown"})),
                "disk_fallback_enabled": bool(getattr(self, "_disk_fallback_enabled", False)),
                "disk_fallback_path": str(getattr(self, "_disk_fallback_path", "") or ""),
                "disk_fallback_recovery": dict(getattr(self, "_disk_fallback_recovery_status", {"status": "unknown"})),
                "recovery_status": self._recovery_status_snapshot(),
                "cache_state": self._cache_state_snapshot(),
                "commands_total": self._commands_total,
                "errors_total": self._errors_total,
                "timeouts_total": self._timeouts_total,
                "records_written_total": self._records_written_total,
                "records_read_total": self._records_read_total,
            },
        }

    def ensure_backend_ready(self, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
        with self._readiness_lock:
            if self._readiness_cache and self._readiness_cache.get("status") == "ready":
                cached = dict(self._readiness_cache)
                cached["cached"] = True
                cached["reason"] = reason
                return cached
            timeout = max(1, int(timeout_ms or BACKEND_READINESS_TIMEOUT_MS))
            deadline = time.monotonic() + timeout / 1000.0
            attempts: list[Json] = []
            attempt = 0
            warmup_key = f"{self._storage_prefix}:readiness"
            warmup_field = f"{stable_hash(f'{self._storage_prefix}:{reason}'):020d}"
            warmup_value = json.dumps(
                {
                    "probe": "matrixark_backend_ready",
                    "backend": self._backend_label(),
                    "reason": reason,
                    "ts_ms": now_ms(),
                },
                sort_keys=True,
            )
            while True:
                attempt += 1
                checks: Json = {
                    "mcp_process_started": True,
                    "metaserver_reachable": metaserver_reachable(self._metaserver),
                    "namespace_table_opened": False,
                    "slot_coverage_verified_by_warmup_hset_hget": False,
                }
                try:
                    if not checks["metaserver_reachable"].get("ok"):
                        raise MatrixArkError(checks["metaserver_reachable"].get("error", "metaserver is not reachable"))
                    if probe:
                        self._client.hset(warmup_key, warmup_field, warmup_value)
                        checks["namespace_table_opened"] = True
                        readback = self._client.hget(warmup_key, warmup_field)
                        if readback != warmup_value:
                            raise MatrixArkError("readiness warmup readback mismatch")
                        checks["slot_coverage_verified_by_warmup_hset_hget"] = True
                    else:
                        checks["namespace_table_opened"] = True
                    result: Json = {
                        "status": "ready",
                        "backend": self._backend_label(),
                        "reason": reason,
                        "probe": bool(probe),
                        "attempts": attempt,
                        "attempt_log": attempts,
                        "topology": {
                            "metaserver": self._metaserver,
                            "namespace": self._namespace,
                            "table": self._table,
                            "storage_prefix": self._storage_prefix,
                            "warmup_key": warmup_key,
                            "warmup_field": warmup_field,
                        },
                        "checks": checks,
                    }
                    self._readiness_cache = result
                    return dict(result)
                except Exception as exc:
                    retryable = is_retryable_temporalstore_error(exc)
                    attempts.append({"attempt": attempt, "ok": False, "retryable": retryable, "error": str(exc), "checks": checks})
                    if not retryable or time.monotonic() >= deadline:
                        return {
                            "status": "topology_not_ready",
                            "backend": self._backend_label(),
                            "reason": reason,
                            "probe": bool(probe),
                            "attempts": attempt,
                            "attempt_log": attempts,
                            "error": str(exc),
                            "topology": {
                                "metaserver": self._metaserver,
                                "namespace": self._namespace,
                                "table": self._table,
                                "storage_prefix": self._storage_prefix,
                                "warmup_key": warmup_key,
                                "warmup_field": warmup_field,
                            },
                            "checks": checks,
                        }
                    time.sleep(max(0.05, BACKEND_READINESS_BACKOFF_MS / 1000.0))

    def _get_index(self) -> list[str]:
        # A backend that cannot answer right now (shard loading / not loaded, timeout,
        # connection refused) must stay a question: swallowing it into [] served a populated
        # store as vacuously empty for as long as the load took -- and let a write path
        # compute its append position from a lie. An absent index key on a reachable backend
        # is the real "no index yet" and still answers [].
        try:
            raw = self._client.get_string(self._index_key)
        except Exception as exc:
            if is_retryable_temporalstore_error(exc):
                raise
            return []
        if not raw:
            return []
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(value, list):
            return []
        return [str(item) for item in value]

    def _get_count(self) -> int:
        # Same contract as _get_index: only an ABSENT count key on a reachable backend is
        # count 0. A retryable failure raises so readers surface a retryable error instead
        # of an empty-but-successful view, and writers never derive positions from 0.
        try:
            raw = self._client.get_string(self._count_key)
        except Exception as exc:
            if is_retryable_temporalstore_error(exc):
                raise
            return 0
        if not raw:
            return 0
        try:
            value = int(raw)
        except ValueError:
            return 0
        return max(0, value)

    def append(self, record: Json) -> None:
        self._ensure_backend_metric_fields()
        self._append_raw_ingestion_records([record])
        records = materialize_serving_records(record)
        if self._queue_batched_records(records):
            return
        self._append_many_materialized(records)

    def append_many(self, records: list[Json]) -> None:
        self._ensure_backend_metric_fields()
        self._append_raw_ingestion_records(records)
        materialized = materialize_serving_record_batch(records)
        if self._queue_batched_records(materialized):
            return
        self._append_many_materialized(materialized)

    def _append_disk_fallback_records(self, records: list[Json]) -> None:
        if not records or not bool(getattr(self, "_disk_fallback_enabled", False)):
            return
        if bool(getattr(self, "_disk_fallback_recovery_in_progress", False)):
            return
        path = str(getattr(self, "_disk_fallback_path", "") or "").strip()
        if not path:
            return
        try:
            adapter = getattr(self, "_disk_fallback_adapter", None)
            if adapter is None or str(adapter.event_log) != path:
                adapter = MatrixArkLocalAdapter(Path(path))
                self._disk_fallback_adapter = adapter
            adapter.append_many(records)
        except Exception as exc:
            setattr(self, "_disk_fallback_write_failures", int(getattr(self, "_disk_fallback_write_failures", 0) or 0) + 1)
            _mcp_debug_log(f"matrixark disk fallback shadow append failed: {exc}")

    def _cache_state_snapshot(self) -> Json:
        lock = getattr(self, "_retrieval_candidate_cache_lock", None)
        if lock is None:
            local_candidate_entries = len(getattr(self, "_retrieval_candidate_cache", {}) or {})
        else:
            with lock:
                local_candidate_entries = len(getattr(self, "_retrieval_candidate_cache", {}) or {})
        return {
            "backend": self._backend_label(),
            "entry_count_cache": getattr(self, "_entry_count_cache", None),
            "records_cache_ready": getattr(self, "_records_cache", None) is not None,
            "records_cache_count": len(getattr(self, "_records_cache", []) or []),
            "index_cache_ready": getattr(self, "_index_cache", None) is not None,
            "index_cache_count": len(getattr(self, "_index_cache", []) or []),
            "python_hot_cache_allowed": self.python_hot_cache_enabled(),
            "local_candidate_cache_entries": local_candidate_entries,
            "process_global_record_cache_enabled": self.python_hot_cache_enabled(),
        }

    def _recovery_status_snapshot(self, native_metrics: Json | None = None) -> Json:
        native_metrics = native_metrics or {}
        disk_status = dict(getattr(self, "_disk_fallback_recovery_status", {"status": "unknown"}))
        recovery_source = "local_disk_fallback_replay"
        if disk_status.get("status") == "skipped":
            recovery_source = "distributed_replication_or_shared_store"
        return {
            "backend": self._backend_label(),
            "status": disk_status.get("status", "unknown"),
            "recovery_source": recovery_source,
            "disk_fallback_recovery": disk_status,
            "disk_fallback_enabled": bool(getattr(self, "_disk_fallback_enabled", False)),
            "disk_fallback_recovery_enabled": bool(getattr(self, "_disk_fallback_recovery_enabled", False)),
            "read_through_cache_warmup": False,
            "replicated_storage_recovery": False,
            "shared_store_read_throughs": int(native_metrics.get("shared_store_read_throughs") or native_metrics.get("shared_store_read_through_count") or 0),
            "page_store_reads": int(native_metrics.get("page_store_reads") or native_metrics.get("page_reads") or 0),
            "cache_hits_total": int(native_metrics.get("cache_hits_total") or 0),
            "cache_misses_total": int(native_metrics.get("cache_misses_total") or 0),
        }

    def _disk_fallback_replay_gate(self) -> Json:
        override = os.environ.get("MATRIXARK_TEMPORALSTORE_RECOVER_LOCAL_STORE_ANY_MODE", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        storage_family = str(
            getattr(self, "_storage_family", "")
            or os.environ.get("MATRIXARK_STORAGE_FAMILY", "")
            or os.environ.get("MATRIXARK_TEMPORALSTORE_STORAGE_FAMILY", "")
            or "default"
        ).strip().lower().replace("-", "_")
        storage_mode = str(
            getattr(self, "_storage_mode", "")
            or os.environ.get("MATRIXARK_STORAGE_MODE", "")
            or os.environ.get("MATRIXARK_TEMPORALSTORE_STORAGE_MODE", "")
            or "default"
        ).strip().lower().replace("-", "_")
        replication_mode = str(
            getattr(self, "_replication_mode", "")
            or os.environ.get("MATRIXARK_REPLICATION_MODE", "")
            or os.environ.get("MATRIXARK_TEMPORALSTORE_REPLICATION_MODE", "")
            or "default"
        ).strip().lower().replace("-", "_")
        distributed_values = {"multi_node", "shared_store", "raft", "replicated", "distributed"}
        local_values = {"default", "local", "single_node", "none", ""}
        distributed = (
            storage_family in {"shared_store", "raft", "replicated", "distributed"}
            or storage_mode in distributed_values
            or replication_mode in {"shared_store", "raft", "replicated", "distributed"}
        )
        local_direct = storage_family in local_values and storage_mode in local_values and replication_mode in local_values
        allowed = bool(override or (local_direct and not distributed))
        return {
            "allowed": allowed,
            "override": override,
            "storage_family": storage_family or "default",
            "storage_mode": storage_mode or "default",
            "replication_mode": replication_mode or "default",
            "policy": "local_single_node_only",
            "skip_reason": "" if allowed else "distributed_storage_uses_replication_or_shared_store_recovery",
        }

    def _recover_serving_from_disk_fallback_if_needed(self, *, reason: str) -> Json:
        self._ensure_backend_metric_fields()
        if not bool(getattr(self, "_disk_fallback_recovery_enabled", False)):
            status = {"status": "disabled", "reason": reason}
            self._disk_fallback_recovery_status = status
            return status
        if bool(getattr(self, "_disk_fallback_recovery_attempted", False)):
            return dict(getattr(self, "_disk_fallback_recovery_status", {"status": "already_attempted", "reason": reason}))
        self._disk_fallback_recovery_attempted = True
        replay_gate = self._disk_fallback_replay_gate()
        if not bool(replay_gate.get("allowed", False)):
            status = {"status": "skipped", "reason": reason, "replay_gate": replay_gate}
            self._disk_fallback_recovery_status = status
            return status
        path = str(getattr(self, "_disk_fallback_path", "") or "").strip()
        if not path:
            status = {"status": "missing_path", "reason": reason}
            self._disk_fallback_recovery_status = status
            return status
        fallback_path = Path(path)
        if not fallback_path.exists():
            status = {"status": "missing_file", "reason": reason, "path": path}
            self._disk_fallback_recovery_status = status
            return status
        started_perf = time.perf_counter()
        try:
            fallback_adapter = MatrixArkLocalAdapter(fallback_path)
            fallback_records = fallback_adapter.read_all()
            serving_records = materialize_serving_record_batch(fallback_records)
            serving_records = compact_latest_context_state_records(
                [
                    record
                    for record in serving_records
                    if str(record.get("record_type") or "") not in TEMPORAL_COMPRESSED_OLD_RECORD_TYPES
                    and not matrixark_record_retention_filtered(record)
                ]
            )
            existing_records = self.read_all_without_disk_fallback_recovery()
            existing_ids = {_durable_recovery_record_identity(record) for record in existing_records}
            missing_records = [
                record
                for record in serving_records
                if _durable_recovery_record_identity(record) not in existing_ids
            ]
            if missing_records:
                self._disk_fallback_recovery_in_progress = True
                try:
                    self._append_many_materialized(missing_records, allow_queue=False)
                finally:
                    self._disk_fallback_recovery_in_progress = False
                self._records_cache = None
                self._drop_direct_record_cache()
                self._entry_count_cache = self._get_count()
            elapsed_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
            status = {
                "status": "recovered" if missing_records else "up_to_date",
                "reason": reason,
                "path": path,
                "replay_gate": replay_gate,
                "fallback_records": len(fallback_records),
                "recoverable_serving_records": len(serving_records),
                "existing_serving_records": len(existing_records),
                "recovered_records": len(missing_records),
                "entry_count_after": self._entry_count_cache,
                "elapsed_ms": elapsed_ms,
            }
            self._disk_fallback_recovery_status = status
            return status
        except Exception as exc:
            self._disk_fallback_recovery_in_progress = False
            status = {"status": "failed", "reason": reason, "path": path, "error": str(exc)}
            self._disk_fallback_recovery_status = status
            _mcp_debug_log(f"matrixark disk fallback serving recovery failed: {exc}")
            return status

    def _storage_route_for_bundle(self, bundle: list[Json]) -> Json:
        fallback: Json = {}
        for record in bundle:
            route = record.get("storage_route")
            if isinstance(route, dict) and route:
                if route.get("placement_key"):
                    return route
                if not fallback:
                    fallback = route
        return fallback

    def _native_append_options(self) -> Json:
        return {
            "append_path": "native_append_queue",
            "coalesce_writes": True,
            "route_by": "placement_key",
            "persist_from_storage_options": True,
            "hset_lowering": "forbidden_for_parity",
            "count_update": "same_batch",
            "audit_hot_path": "inline_counters_only",
            "full_context_pack_audit": "sample_or_enqueue_async_policy_enabled",
        }

    def _normalize_raw_storage_backend(self, value: Any) -> str:
        backend = str(value or "temporalstore").strip().lower().replace("-", "_")
        if backend in {"", "temporal", "temporal_store", "ts"}:
            backend = "temporalstore"
        if backend in {"matrix_kv", "kv"}:
            backend = "matrixkv"
        if backend in {"object_store", "object", "blob", "blobstore", "blob_store"}:
            backend = "objectstore"
        if backend in {"aws_s3", "s3_object", "s3_objectstore"}:
            backend = "s3"
        if backend not in {"temporalstore", "matrixkv", "s3", "objectstore"}:
            raise MatrixArkError("MATRIXARK_RAW_INGESTION_BACKEND must be temporalstore, matrixkv, s3, or objectstore")
        return backend

    def _raw_ingestion_append_path(self) -> str:
        backend = self._normalize_raw_storage_backend(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )
        if backend == "temporalstore":
            return "matrixark_raw_ingestion_temporalstore_log"
        if backend == "matrixkv":
            return "matrixark_raw_ingestion_matrixkv_log"
        if backend == "s3":
            return "matrixark_raw_ingestion_s3_object_ref"
        return "matrixark_raw_ingestion_objectstore_ref"

    def _raw_ingestion_append_options(self) -> Json:
        backend = self._normalize_raw_storage_backend(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )
        return {
            "append_path": self._raw_ingestion_append_path(),
            "raw_storage_backend": backend,
            "raw_message_store": backend,
            "coalesce_writes": True,
            "route_by": "raw_ingestion_prefix",
            "persist_from_storage_options": True,
            "hset_lowering": "forbidden_for_parity",
            "count_update": "same_batch",
            "source": "matrixark_live_ingestion_dual_write",
        }

    def _ensure_raw_ingestion_fields(self) -> None:
        if not hasattr(self, "_raw_storage_backend"):
            self._raw_storage_backend = self._normalize_raw_storage_backend(
                os.environ.get("MATRIXARK_RAW_INGESTION_BACKEND", "temporalstore")
            )
        else:
            self._raw_storage_backend = self._normalize_raw_storage_backend(self._raw_storage_backend)
        if not hasattr(self, "_raw_ingestion_prefix"):
            storage_prefix = str(getattr(self, "_storage_prefix", "matrixark:mcp")).rstrip(":")
            configured_raw_prefix = os.environ.get("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX", "").strip().rstrip(":")
            self._raw_ingestion_prefix = configured_raw_prefix or f"{storage_prefix}:raw_ingestion"
        if not hasattr(self, "_raw_record_hash_key"):
            self._raw_record_hash_key = f"{self._raw_ingestion_prefix}:records"
        if not hasattr(self, "_raw_count_key"):
            self._raw_count_key = f"{self._raw_ingestion_prefix}:record_count"
        if not hasattr(self, "_raw_entry_count_cache"):
            self._raw_entry_count_cache = None

    def _raw_record_location(self, sequence: int) -> tuple[str, str]:
        self._ensure_raw_ingestion_fields()
        shard = sequence // self._shard_size
        offset = sequence % self._shard_size
        return f"{self._raw_record_hash_key}:{shard:06d}", f"{offset:020d}"

    def _get_raw_count(self) -> int:
        self._ensure_raw_ingestion_fields()
        try:
            raw = self._client.get_string(self._raw_count_key)
        except Exception:
            return 0
        if not raw:
            return 0
        try:
            value = int(raw)
        except ValueError:
            return 0
        return max(0, value)

    def _append_raw_ingestion_records(self, records: list[Json], *, allow_queue: bool = True) -> None:
        if not records:
            return
        self._ensure_backend_metric_fields()
        records = [normalize_raw_ingestion_record(record) for record in records]
        self._ensure_raw_ingestion_fields()
        if self._raw_ingestion_prefix == self._storage_prefix:
            raise MatrixArkError("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX must differ from the serving storage prefix")
        if (
            allow_queue
            and bool(getattr(self, "_direct_raw_ingestion_queue_enabled", False))
            and bool(getattr(self, "_direct_write_queue_enabled", False))
            and getattr(self, "_direct_write_queue_mode", "memory") == "memory"
        ):
            queued_at_ms = now_ms()
            queue_batch_id = f"raw:{queued_at_ms}:{stable_hash(json.dumps(records, sort_keys=True, separators=(',', ':')))}"
            queued_records = _records_with_matrixark_write_debug(
                records,
                raw_storage_prefix=self._raw_ingestion_prefix,
                write_path="async_memory_queue",
                queue_mode="raw_ingestion",
                queue_batch_id=queue_batch_id,
                queued_at_ms=queued_at_ms,
                queue_backend=getattr(self, "_direct_write_queue_mode", "memory"),
                queue_depth_before=getattr(self, "_direct_write_queue", None).qsize() if hasattr(self, "_direct_write_queue") else 0,
            )
            self._enqueue_direct_write_item({"queue_mode": "raw_ingestion", "records": queued_records}, len(queued_records))
            return
        started_perf = time.perf_counter()
        append_started_at_ms = now_ms()
        records_to_write = _records_with_matrixark_write_debug(
            records,
            raw_storage_prefix=self._raw_ingestion_prefix,
            write_path="direct_append" if allow_queue else "async_queue_flush",
            append_started_at_ms=append_started_at_ms,
        )
        self._append_disk_fallback_records(records_to_write)
        with self._records_lock:
            count = self._raw_entry_count_cache if self._raw_entry_count_cache is not None else self._get_raw_count()
            sequence = count
            entries: list[Json] = []
            for record in records_to_write:
                record_key, record_id = self._raw_record_location(sequence)
                debug = record.get("matrixark_write_debug") if isinstance(record.get("matrixark_write_debug"), dict) else {}
                debug.update(
                    {
                        "persist_sequence": sequence,
                        "persist_key": record_key,
                        "persist_field": record_id,
                        "persist_record_built_at_ms": now_ms(),
                    }
                )
                if debug.get("flush_started_at_ms") and debug.get("queued_at_ms"):
                    try:
                        debug["queue_wait_ms"] = int(debug["flush_started_at_ms"]) - int(debug["queued_at_ms"])
                    except (TypeError, ValueError):
                        pass
                record["matrixark_write_debug"] = debug
                payload = json.dumps(slim_persisted_record(record),
                                     sort_keys=True, separators=(",", ":"))
                route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
                entries.append({"key": record_key, "field": record_id, "value": payload, "storage_route": route})
                sequence += 1
            append_records = getattr(self._client, "matrixark_batch_append_records", None)
            if callable(append_records):
                self._write_with_backoff(
                    lambda: self._matrixark_batch_append_records_with_options(
                        append_records,
                        entries,
                        count_key=self._raw_count_key,
                        count_value=str(sequence),
                        append_options=self._raw_ingestion_append_options(),
                    ),
                    op="matrixark_batch_append_raw_ingestion_records",
                )
            else:
                self._hset_many_with_backoff(entries)
                self._put_string_with_backoff(self._raw_count_key, str(sequence))
            if self._raw_ingestion_visibility_required_after_flush():
                self._note_pending_visibility_keys(
                    [self._raw_count_key] + [str(entry.get("key") or "") for entry in entries]
                )
            self._raw_entry_count_cache = sequence
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            self._observe_backend_command(elapsed_ms, records_written=len(records))

    def enqueue_raw_ingestion_records(self, records: list[Json]) -> None:
        """Persist original ingestion envelopes outside the compact serving prefix.

        Fast hooks write a compact ContextEvent for serving, but the original
        agent message envelope must also be fetchable for backfill/recovery.
        Keep the public helper narrow so hook code does not need to reach into
        raw-prefix internals.
        """
        if not records:
            return
        self._ensure_direct_write_queue_fields()
        # Raw agent envelopes are the recovery/backfill source of truth. Do not
        # leave them in a process-local memory queue that can disappear when a
        # short-lived hook process exits.
        self._append_raw_ingestion_records(records, allow_queue=False)

    def _context_index_lookup_key(self, scope_key: str) -> str:
        scope_hash = stable_hash(scope_key) if scope_key else 0
        return f"{self._storage_prefix}:context_index_lookup:{scope_hash}"

    def _context_ref_locator_key(self) -> str:
        return f"{self._storage_prefix}:context_ref_locator"

    def _context_placement_lookup_key(self, scope_key: str) -> str:
        scope_hash = stable_hash(scope_key) if scope_key else 0
        return f"{self._storage_prefix}:context_placement_lookup:{scope_hash}"

    def _merge_ref_hashes(self, existing_value: str, new_refs: list[int]) -> list[int]:
        refs: list[int] = []
        seen: set[int] = set()
        if existing_value:
            try:
                decoded = json.loads(existing_value)
            except Exception:
                decoded = {}
            raw_refs = decoded.get("ref_hashes", []) if isinstance(decoded, dict) else []
            for value in raw_refs if isinstance(raw_refs, list) else []:
                try:
                    ref_hash = int(value)
                except (TypeError, ValueError):
                    continue
                if ref_hash and ref_hash not in seen:
                    refs.append(ref_hash)
                    seen.add(ref_hash)
        for ref_hash in new_refs:
            if ref_hash and ref_hash not in seen:
                refs.append(ref_hash)
                seen.add(ref_hash)
        return refs

    def _location_base(self) -> str:
        """The record log every compact location is relative to.

        This is the RECORD log (`{storage_prefix}:records`), not the raw-ingestion log. They are
        different keys, and using the wrong one is silent: every entry simply fails the prefix
        test and stays in the long form, so the encoding looks live and saves nothing.
        """
        base = str(getattr(self, "_record_hash_key", "") or "")
        if base:
            return base
        prefix = str(getattr(self, "_storage_prefix", "") or "")
        return ("%s:records" % prefix) if prefix else ""

    def _location_token(self, location: Json) -> Json | None:
        """One location in the form it will be STORED in, or None if it is not a location.

        The merge used to expand every stored entry into `{"key", "field"}` and the writer then
        compacted every one of them straight back -- two string formats per entry, on a list that
        runs to four figures per ingest. Nothing in between needed the long form: the callers
        serialize the result, count it, or compare entries to each other. So the merge now works
        in the stored form throughout, and the compact string doubles as its own dedupe key.
        """
        if isinstance(location, str):
            return location or None
        if isinstance(location, dict):
            key = str(location.get("key") or "")
            field = str(location.get("field") or "")
            if not key or not field:
                return None
            compact = compact_location(key, field, self._location_base())
            if isinstance(compact, str):
                return compact
            return (key, field)
        return None

    def _merge_ref_locations(self, existing_value: str, new_locations: list[Json]) -> list[Json]:
        locations: list[Json] = []
        resource_versions: set[str] = set()
        seen: set[tuple[str, str]] = set()
        if existing_value:
            try:
                decoded = json.loads(existing_value)
            except Exception:
                decoded = {}
            raw_locations = decoded.get("locations", []) if isinstance(decoded, dict) else []
            for location in raw_locations if isinstance(raw_locations, list) else []:
                token = self._location_token(location)
                if token is None or token in seen:
                    continue
                locations.append(token)
                seen.add(token)
        for location in new_locations:
            token = self._location_token(location)
            if token is None or token in seen:
                continue
            locations.append(token)
            seen.add(token)
        return locations

    def _merge_resource_versions(self, existing_value: str, new_versions: set[str]) -> list[str]:
        versions: set[str] = set()
        if existing_value:
            try:
                decoded = json.loads(existing_value)
            except Exception:
                decoded = {}
            raw_versions = decoded.get("resource_versions", []) if isinstance(decoded, dict) else []
            if isinstance(raw_versions, list):
                versions.update(str(value) for value in raw_versions if str(value))
        versions.update(str(value) for value in new_versions if str(value))
        return sorted(versions)

    def _read_hash_values_best_effort(
        self, pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        """Read many (key, field) values in ONE call, when the client can.

        Returns only the pairs it actually resolved. The caller falls back to the per-entry read
        for anything missing, so a partial answer is never mistaken for an empty value -- an empty
        value here would silently drop the existing refs a merge is supposed to preserve.
        """
        if bool(getattr(self, "_native_side_index_assume_fresh", False)):
            return {}
        reader = getattr(self._client, "batch_hget", None)
        if not callable(reader) or not pairs:
            return {}
        try:
            rows = reader([{"key": key, "field": field} for key, field in pairs])
        except Exception:  # noqa: BLE001 - fall back to the per-entry reads.
            return {}
        out: dict[tuple[str, str], str] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            out[(str(row.get("key") or ""), str(row.get("field") or ""))] = str(row.get("value") or "")
        return out

    def _read_hash_value_best_effort(self, key: str, field: str) -> str:
        if bool(getattr(self, "_native_side_index_assume_fresh", False)):
            return ""
        reader = getattr(self._client, "hget", None)
        if not callable(reader):
            return ""
        try:
            value = reader(key, field)
        except Exception:
            return ""
        return str(value or "")

    @staticmethod
    def record_pointed_ref_ids(record: Json) -> list[int]:
        """The ids a record points AT without carrying: provenance sources and tombstone/feedback
        targets. Filed into the locator so one id lookup finds the records ABOUT an id, not just
        the records carrying it."""
        values: list = []
        source_ids = record.get("source_event_ids")
        if isinstance(source_ids, list):
            values.extend(source_ids)
        source_refs = record.get("source_refs")
        if isinstance(source_refs, list):
            values.extend(source_refs)
        for field in ("source_event_hash", "target_memory_id", "superseded_by"):
            if record.get(field) is not None:
                values.append(record.get(field))
        out: list[int] = []
        seen: set[int] = set()
        for value in values:
            try:
                ref = int(value)
            except (TypeError, ValueError):
                continue
            if ref and ref not in seen:
                seen.add(ref)
                out.append(ref)
        return out

    def _chunk_tail_pairs(self, prefetched: dict) -> list[tuple[str, str]]:
        """The tail field of every chunked list whose head we just read.

        Three writers chunk, and each names its tail differently: ref postings use
        ``{field}#r{n}`` with the count in ``ref_chunks``; the locator and placement lists use
        ``{field}#{n}`` with the count in ``location_chunks``. A head that is full but has no
        count yet rolls to chunk 1 on this very write, so that is worth fetching too.
        """
        pairs: list[tuple[str, str]] = []
        for (key, field), value in list(prefetched.items()):
            if not value:
                continue
            try:
                head = json.loads(value)
            except Exception:  # noqa: BLE001 - an unreadable head just means no prefetch.
                continue
            if not isinstance(head, dict):
                continue
            try:
                location_chunks = int(head.get("location_chunks") or 0)
            except (TypeError, ValueError):
                location_chunks = 0
            if location_chunks:
                pairs.append((key, f"{field}#{location_chunks}"))
            else:
                locations = head.get("locations")
                if isinstance(locations, list) and len(locations) >= min(
                    self.LOCATOR_CHUNK_LOCATIONS, self.PLACEMENT_CHUNK_LOCATIONS
                ):
                    pairs.append((key, f"{field}#1"))
            try:
                ref_chunks = int(head.get("ref_chunks") or 0)
            except (TypeError, ValueError):
                ref_chunks = 0
            if ref_chunks:
                pairs.append((key, f"{field}#r{ref_chunks}"))
            else:
                refs = head.get("ref_hashes")
                if isinstance(refs, list) and len(refs) >= self.REF_HASH_CHUNK:
                    pairs.append((key, f"{field}#r1"))
        return pairs

    def _native_side_index_entries_for_bundles(self, bundles: list[tuple[list[Json], str, str]]) -> list[Json]:
        """Build sidecar lookup rows so retrieval can avoid broad record scans.

        ContextIndex remains compact and bucketed.  These sidecar hashes make the
        compact postings and ref-to-record locations directly addressable by the
        native conformance hash API.
        """
        lookup_updates: dict[tuple[str, str], Json] = {}
        locator_updates: dict[int, list[Json]] = {}
        placement_updates: dict[tuple[str, str], Json] = {}
        route_by_hash_field: dict[tuple[str, str], Json] = {}
        for bundle, record_key, record_id in bundles:
            location = {"key": record_key, "field": record_id}
            route = self._storage_route_for_bundle(bundle)
            for record in bundle:
                node_hash = record.get("node_hash")
                scope_key_for_placement = str(record.get("scope_key") or "")
                if not scope_key_for_placement:
                    scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
                    scope_key_for_placement = canonical_scope_key(scope) if scope else ""
                if scope_key_for_placement and node_hash is not None:
                    try:
                        placement_node_hash = int(node_hash)
                    except (TypeError, ValueError):
                        placement_node_hash = 0
                    if placement_node_hash:
                        placement_key = (self._context_placement_lookup_key(scope_key_for_placement), str(placement_node_hash))
                        placement_update = placement_updates.setdefault(placement_key, {"locations": [], "resource_versions": set()})
                        placement_update["locations"].append(location)
                        resource_version = str(record.get("resource_version") or "")
                        if resource_version:
                            placement_update["resource_versions"].add(resource_version)
                        if route:
                            route_by_hash_field.setdefault(placement_key, route)
                for ref_hash in context_index_ref_hashes(record):
                    locator_updates.setdefault(ref_hash, []).append(location)
                    if route:
                        route_by_hash_field.setdefault((self._context_ref_locator_key(), str(ref_hash)), route)
                for ref_hash in self.record_pointed_ref_ids(record):
                    locator_updates.setdefault(ref_hash, []).append(location)
                    if route:
                        route_by_hash_field.setdefault((self._context_ref_locator_key(), str(ref_hash)), route)
                if record.get("record_type") != "context_index":
                    continue
                index_name = str(record.get("index_name") or "").strip()
                if not index_name:
                    continue
                scope_key = str(record.get("scope_key") or "")
                if not scope_key:
                    scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
                    scope_key = canonical_scope_key(scope) if scope else ""
                ref_hashes = context_index_ref_hashes(record)
                if scope_key and ref_hashes:
                    lookup_key = (self._context_index_lookup_key(scope_key), index_name)
                    update = lookup_updates.setdefault(lookup_key, {"ref_hashes": [], "posting_buckets": set()})
                    update["ref_hashes"].extend(ref_hashes)
                    update["posting_buckets"].add(context_index_posting_bucket(context_index_timestamp_key(record)))
                    if route:
                        route_by_hash_field.setdefault(lookup_key, route)

        # Every existing value below is addressed by a (key, field) pair that is already known,
        # so they are fetched in ONE call rather than one round trip per merge.
        prefetch_locator_key = self._context_ref_locator_key()
        wanted: list[tuple[str, str]] = list(lookup_updates.keys())
        wanted.extend((prefetch_locator_key, str(ref_hash)) for ref_hash in locator_updates)
        wanted.extend(placement_updates.keys())
        prefetched = self._read_hash_values_best_effort(wanted) if wanted else {}
        # Second phase. The batch above covers the HEAD of each list, but every chunked writer then
        # asks for a TAIL whose field name is only knowable once its head has been read -- so those
        # fell through to a per-entry read. Measured per add: 24.7 single `hget`s at 0.51 ms, 12.5
        # ms, against 1.75 ms for a batch of any size.
        #
        # Now the heads name their tails and those are fetched in one more batch. This is purely a
        # prefetch: `existing_for` still falls back to the single read, so a tail this guesses
        # wrong costs one wasted entry in a batch and never a wrong answer.
        tail_pairs = self._chunk_tail_pairs(prefetched)
        if tail_pairs:
            prefetched.update(self._read_hash_values_best_effort(tail_pairs))

        def existing_for(key: str, field: str) -> str:
            """The stored value: from the batch when it covered this pair, per-entry otherwise."""
            hit = prefetched.get((key, field))
            return hit if hit is not None else self._read_hash_value_best_effort(key, field)

        entries: list[Json] = []
        for (key, field), update in lookup_updates.items():
            new_refs = update.get("ref_hashes", []) if isinstance(update, dict) else []
            new_buckets = update.get("posting_buckets", set()) if isinstance(update, dict) else set()
            existing_value = existing_for(key, field)
            # Same unbounded rewrite the placement list had: a posting's ref set grows with every
            # record filed under that term, and holding it in one value meant re-writing all of it
            # per add. Measured at 87 755 bytes for a subject with 125 memories against 2 170 for a
            # fresh one. Overflow goes to sibling fields; the head keeps its name and shape.
            chunked = self._ref_hash_chunk_entries(
                key, field, new_refs, existing_value, existing_for,
                route_by_hash_field.get((key, field), {}))
            if chunked is not None:
                entries.extend(chunked)
                continue
            merged_refs = self._merge_ref_hashes(existing_value, new_refs)
            existing_buckets: set[int] = set()
            if existing_value:
                try:
                    decoded_existing = json.loads(existing_value)
                except Exception:
                    decoded_existing = {}
                raw_buckets = decoded_existing.get("posting_buckets", []) if isinstance(decoded_existing, dict) else []
                if isinstance(raw_buckets, list):
                    for value in raw_buckets:
                        try:
                            bucket = int(value)
                        except (TypeError, ValueError):
                            continue
                        if bucket:
                            existing_buckets.add(bucket)
            for value in new_buckets if isinstance(new_buckets, set) else set():
                try:
                    bucket = int(value)
                except (TypeError, ValueError):
                    continue
                if bucket:
                    existing_buckets.add(bucket)
            entries.append(
                {
                    "key": key,
                    "field": field,
                    "value": json.dumps(
                        {"ref_hashes": merged_refs, "posting_buckets": sorted(existing_buckets)},
                        separators=(",", ":"),
                    ),
                    "storage_route": route_by_hash_field.get((key, field), {}),
                }
            )
        locator_key = self._context_ref_locator_key()
        # Store birth: the batch placing the very first record (shard 000000, field 000000) also
        # stamps the coverage marker, so readers can trust that pointed-id indexing was active
        # for every record this store has ever held. Idempotent; existing stores never gain it.
        def _is_birth_location(record_key: str, record_id: str) -> bool:
            # Shard 0, field 0 -- the store's very first append. Field ids are zero-padded to a
            # width that differs from the shard width, so compare numerically, not by literal.
            if not record_key.endswith(":000000"):
                return False
            try:
                return int(record_id) == 0
            except (TypeError, ValueError):
                return False

        if any(_is_birth_location(record_key, record_id)
               for _, record_key, record_id in bundles):
            entries.append({
                "key": locator_key + "_meta",
                "field": "provenance_from_start",
                "value": "1",
                "storage_route": {},
            })
        for ref_hash, new_locations in locator_updates.items():
            field = str(ref_hash)
            entries.extend(
                self._locator_entries_for_ref(
                    locator_key,
                    field,
                    new_locations,
                    existing_for,
                    route_by_hash_field.get((locator_key, field), {}),
                )
            )
        for (key, field), update in placement_updates.items():
            new_locations = update.get("locations", []) if isinstance(update, dict) else []
            new_versions = update.get("resource_versions", set()) if isinstance(update, dict) else set()
            entries.extend(
                self._placement_entries_for_node(
                    key,
                    field,
                    new_locations,
                    new_versions if isinstance(new_versions, set) else set(),
                    existing_for,
                    route_by_hash_field.get((key, field), {}),
                )
            )
        return entries

    # A node's placement list is every record location belonging to that node, and a retrieval
    # needs all of it -- so it cannot be capped. It CAN stop being one value. Held whole, each
    # append re-read the list, added one entry and wrote the whole thing back: O(list) bytes per
    # add, O(list^2) over a subject's life. Measured on one store with the same 62-byte message,
    # this write was 3 486 bytes for a fresh subject and 197 106 bytes for a subject with 125
    # memories -- 47% of everything that add wrote.
    #
    # Chunked, an append rewrites only the tail. The head field keeps its original name and shape,
    # so a reader that knows nothing about chunks still finds locations there; overflow lives in
    # sibling fields "{node}#1", "{node}#2", ... and the head records how many exist.
    # An append rewrites its TAIL chunk, so this number is what one add pays to touch a posting,
    # and 256 was a first guess. Measured at three sizes -- 150 adds into ONE subject, so the
    # posting lists actually grow, which is the case that matters for a large corpus:
    #
    #     chunk 256   236.3 KB per add    add median 241.2 ms
    #     chunk  64   207.5 KB per add    add median 266.2 ms
    #     chunk  16   174.7 KB per add    add median 253.8 ms
    #
    # 26% less disk per add at 16, with add latency flat inside the noise and retrieval returning
    # the same items at every size. The cost of going smaller is more chunk fields to fetch, but
    # the reader collects them in ONE batch call whatever the count, so it buys back little below
    # this. Shrinking the placement chunk alongside it changed nothing measurable (174.6 vs 174.7),
    # because a node's location list is far shorter than a term's posting list -- so that one
    # stays where it is rather than being tuned on a number that did not move.
    REF_HASH_CHUNK = 16

    def _ref_hash_chunk_entries(self, key, field, new_refs, existing_value, existing_for,
                                storage_route):
        """Append ref hashes to a posting without rewriting the whole set.

        Returns None when the head still has room, so the caller keeps its original single-value
        path (and the original shape) for small postings -- which is every posting until a term
        has been used a few hundred times.
        """
        head_decoded: Json = {}
        if existing_value:
            try:
                decoded = json.loads(existing_value)
                if isinstance(decoded, dict):
                    head_decoded = decoded
            except Exception:
                head_decoded = {}
        head_refs = head_decoded.get("ref_hashes")
        head_refs = head_refs if isinstance(head_refs, list) else []
        try:
            chunk_count = int(head_decoded.get("ref_chunks") or 0)
        except (TypeError, ValueError):
            chunk_count = 0
        if not chunk_count and len(head_refs) < self.REF_HASH_CHUNK:
            return None

        tail_index = max(chunk_count, 1)
        tail_field = f"{field}#r{tail_index}"
        tail_value = existing_for(key, tail_field)
        merged_tail = self._merge_ref_hashes(tail_value, new_refs)
        # Anything already in the head stays there; only genuinely new hashes ride the tail.
        head_set = {str(ref) for ref in head_refs}
        merged_tail = [ref for ref in merged_tail if str(ref) not in head_set]
        if len(merged_tail) > self.REF_HASH_CHUNK:
            kept = self._merge_ref_hashes(tail_value, [])
            kept = [ref for ref in kept if str(ref) not in head_set]
            overflow = [ref for ref in merged_tail if ref not in kept]
            if overflow:
                tail_index += 1
                tail_field = f"{field}#r{tail_index}"
                merged_tail = overflow

        entries = [{"key": key, "field": tail_field,
                    "value": json.dumps({"ref_hashes": merged_tail}, separators=(",", ":")),
                    "storage_route": storage_route}]
        if tail_index != chunk_count:
            payload = dict(head_decoded)
            payload["ref_hashes"] = head_refs
            payload["ref_chunks"] = tail_index
            entries.append({"key": key, "field": field,
                            "value": json.dumps(payload, separators=(",", ":")),
                            "storage_route": storage_route})
        return entries

    PLACEMENT_CHUNK_LOCATIONS = 64

    @staticmethod
    def _placement_chunk_field(field: str, index: int) -> str:
        return field if index == 0 else f"{field}#{index}"

    # A ref's locator list is every record location carrying that ref, and it had exactly the
    # problem the placement list had before chunking: held whole, each append re-read the list,
    # added an entry, and wrote the whole thing back. O(list) bytes per add, O(list^2) over the
    # life of a term. Measured by walking the page segments over 300 ingests, rows carrying only a
    # `locations` field -- which is what a locator row is -- were **76.7 KB of the 77.6 KB** that
    # every `locations` field cost per add. The placement rows beside them, already chunked, came
    # to 0.4 KB.
    #
    # Same fix, same shape: the head field keeps its original name and contents, so a reader that
    # knows nothing about chunks still finds locations there, overflow lives in "{ref}#1", "{ref}#2",
    # and the head names how many follow.
    LOCATOR_CHUNK_LOCATIONS = 32

    @staticmethod
    def _locator_chunk_field(field: str, index: int) -> str:
        return field if index == 0 else f"{field}#{index}"

    def _locator_entries_for_ref(
        self,
        key: str,
        field: str,
        new_locations: list[Json],
        existing_for,
        storage_route: Json,
    ) -> list[Json]:
        """Append `new_locations` to a ref's locator list, touching only the tail chunk."""
        head_value = existing_for(key, field)
        head_decoded: Json = {}
        if head_value:
            try:
                decoded = json.loads(head_value)
                if isinstance(decoded, dict):
                    head_decoded = decoded
            except Exception:
                head_decoded = {}
        head_locations = head_decoded.get("locations")
        head_locations = head_locations if isinstance(head_locations, list) else []
        try:
            chunk_count = int(head_decoded.get("location_chunks") or 0)
        except (TypeError, ValueError):
            chunk_count = 0

        tail_index = chunk_count if chunk_count else 0
        if tail_index == 0 and len(head_locations) >= self.LOCATOR_CHUNK_LOCATIONS:
            tail_index = 1
        tail_field = self._locator_chunk_field(field, tail_index)
        tail_value = head_value if tail_index == 0 else existing_for(key, tail_field)

        merged_tail = self._merge_ref_locations(tail_value, new_locations)
        # A tail that overflows starts the next chunk rather than growing without bound. A list
        # already over the limit -- written before chunking -- is left where it is: rewriting it
        # would cost exactly the O(list) write this exists to avoid.
        if tail_index > 0 and len(merged_tail) > self.LOCATOR_CHUNK_LOCATIONS:
            keep = self._merge_ref_locations(tail_value, [])
            overflow = [item for item in merged_tail if item not in keep]
            if overflow:
                tail_index += 1
                tail_field = self._locator_chunk_field(field, tail_index)
                merged_tail = overflow
        elif tail_index == 0 and len(merged_tail) > self.LOCATOR_CHUNK_LOCATIONS and head_locations:
            overflow = [item for item in merged_tail if item not in head_locations]
            if overflow:
                tail_index = 1
                tail_field = self._locator_chunk_field(field, tail_index)
                merged_tail = overflow

        base = self._location_base()
        entries: list[Json] = []
        if tail_index == 0:
            payload: Json = {"locations": compact_location_list(merged_tail, base)}
            if chunk_count:
                payload["location_chunks"] = chunk_count
            entries.append({"key": key, "field": field,
                            "value": json.dumps(payload, separators=(",", ":")),
                            "storage_route": storage_route})
            return entries

        entries.append({"key": key, "field": tail_field,
                        "value": json.dumps(
                            {"locations": compact_location_list(merged_tail, base)},
                            separators=(",", ":")),
                        "storage_route": storage_route})
        # The head is rewritten only when the chunk count actually changes -- once per rollover,
        # not once per add. That is the whole point: the common add touches one small tail.
        if tail_index != chunk_count:
            entries.append({"key": key, "field": field,
                            "value": json.dumps(
                                {"locations": compact_location_list(head_locations, base),
                                 "location_chunks": tail_index},
                                separators=(",", ":")),
                            "storage_route": storage_route})
        return entries

    def _placement_entries_for_node(
        self,
        key: str,
        field: str,
        new_locations: list[Json],
        new_versions: set[str],
        existing_for,
        storage_route: Json,
    ) -> list[Json]:
        """Append `new_locations` to a node's placement list, touching only the tail chunk."""
        head_value = existing_for(key, field)
        head_decoded: Json = {}
        if head_value:
            try:
                decoded = json.loads(head_value)
                if isinstance(decoded, dict):
                    head_decoded = decoded
            except Exception:
                head_decoded = {}
        head_locations = head_decoded.get("locations")
        head_locations = head_locations if isinstance(head_locations, list) else []
        try:
            chunk_count = int(head_decoded.get("location_chunks") or 0)
        except (TypeError, ValueError):
            chunk_count = 0

        # Which chunk is the tail: the head while it still has room, otherwise the last overflow.
        tail_index = chunk_count if chunk_count else 0
        if tail_index == 0 and len(head_locations) >= self.PLACEMENT_CHUNK_LOCATIONS:
            tail_index = 1
        tail_field = self._placement_chunk_field(field, tail_index)
        tail_value = head_value if tail_index == 0 else existing_for(key, tail_field)

        merged_tail = self._merge_ref_locations(tail_value, new_locations)
        # A tail that overflows starts the next chunk rather than growing without bound. Anything
        # already over the limit (a list written before chunking) is left where it is: rewriting it
        # would cost exactly the O(list) write this exists to avoid.
        if tail_index > 0 and len(merged_tail) > self.PLACEMENT_CHUNK_LOCATIONS:
            keep = self._merge_ref_locations(tail_value, [])
            overflow = [item for item in merged_tail if item not in keep]
            if overflow:
                tail_index += 1
                tail_field = self._placement_chunk_field(field, tail_index)
                merged_tail = overflow
        elif tail_index == 0 and len(merged_tail) > self.PLACEMENT_CHUNK_LOCATIONS and head_locations:
            overflow = [item for item in merged_tail if item not in head_locations]
            if overflow:
                tail_index = 1
                tail_field = self._placement_chunk_field(field, tail_index)
                merged_tail = overflow

        entries: list[Json] = []
        if tail_index == 0:
            merged_versions = self._merge_resource_versions(head_value, new_versions)
            payload: Json = {
                "locations": compact_location_list(merged_tail, self._location_base()),
                "resource_versions": merged_versions,
            }
            if chunk_count:
                payload["location_chunks"] = chunk_count
            entries.append({"key": key, "field": field, "value": json.dumps(payload, separators=(",", ":")),
                            "storage_route": storage_route})
            return entries

        entries.append({"key": key, "field": tail_field,
                        "value": json.dumps(
                            {"locations": compact_location_list(merged_tail, self._location_base())},
                            separators=(",", ":"),
                        ),
                        "storage_route": storage_route})
        # The head carries the chunk count and the resource versions, and is rewritten only when
        # one of those actually changes -- once per chunk rollover, not once per add.
        merged_versions = self._merge_resource_versions(head_value, new_versions)
        head_versions = head_decoded.get("resource_versions")
        head_versions = head_versions if isinstance(head_versions, list) else []
        if tail_index != chunk_count or merged_versions != head_versions:
            payload = {
                "locations": compact_location_list(head_locations, self._location_base()),
                "resource_versions": merged_versions,
                "location_chunks": tail_index,
            }
            entries.append({"key": key, "field": field, "value": json.dumps(payload, separators=(",", ":")),
                            "storage_route": storage_route})
        return entries

    def _context_event_ingestion_time_ms(self, record: Json) -> int:
        return context_event_timestamp_ms(record)

    def _context_event_time_index_key(self, record: Json) -> str:
        enriched = attach_context_event_time_key(record)
        parent_segment_hash = enriched.get("parent_segment_hash") or enriched.get("segment_hash")
        if parent_segment_hash:
            enriched = {
                **enriched,
                "context_event_parent_type": "context_segment",
                "context_event_parent_hash": parent_segment_hash,
            }
        parent_type = str(enriched.get("context_event_parent_type") or "context_node")
        parent_hash = enriched.get("context_event_parent_hash") or 0
        return f"{self._storage_prefix}:context_event_by_ingestion_time:{parent_type}:{parent_hash}"

    def _context_event_time_index_field(self, record: Json) -> str:
        event_hash = record.get("event_id_hash") or stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":")))
        timestamp_ms = self._context_event_ingestion_time_ms(record)
        return f"{timestamp_ms:020d}:{event_hash}"

    def _context_event_time_index_payload(self, record: Json) -> str:
        """Compact timestamp-index payload.

        The full ContextEvent is already written to the serving record log.  The
        timestamp index is an ordered lookup structure, so it only needs enough
        information to find/filter the canonical event record.  Keeping raw text
        and extraction/debug fields out of this index avoids doubling hot write
        bytes for every event.
        """
        scope_key = str(record.get("scope_key") or "")
        if not scope_key:
            scope = record.get("scope") if isinstance(record.get("scope"), dict) else {}
            scope_key = canonical_scope_key(scope)
        payload: Json = {
            "record_type": "context_event_ref",
            "ref_hash": int(record.get("event_id_hash") or 0),
            "node_hash": int(record.get("node_hash") or 0),
            "scope_key": scope_key,
            "timestamp_key_ms": self._context_event_ingestion_time_ms(record),
        }
        payload["context_event_key"] = context_event_time_key(payload["timestamp_key_ms"], payload["ref_hash"])
        source_chunk_hash = record.get("source_chunk_hash")
        if source_chunk_hash is not None:
            payload["source_chunk_hash"] = source_chunk_hash
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _context_event_time_index_entries(self, records: list[Json]) -> list[Json]:
        entries: list[Json] = []
        full_payload = env_bool("MATRIXARK_CONTEXT_EVENT_TIME_INDEX_FULL_PAYLOAD", False)
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if record.get("event_id_hash") is None:
                continue
            enriched = attach_context_event_time_key(record)
            # Slim unless explicitly asked for the whole record. The `or parent_segment_hash`
            # that used to be here made the common case the expensive one: a segment-parented
            # event stored the ENTIRE event a second time, measured at 6 329 bytes of one add
            # against 252 for the slim form. That is exactly what the payload helper's own
            # docstring warns against -- "the full ContextEvent is already written to the serving
            # record log ... avoids doubling hot write bytes for every event" -- and the other
            # writer of this same index never had the exception, so the two disagreed about what
            # the index is for.
            #
            # It is an ORDERING structure: the field is {timestamp:020d}:{event_hash}, so lexical
            # order is chronological, and the slim payload carries what a reader needs to reach the
            # canonical record (ref_hash, node_hash, scope_key, timestamp).
            # MATRIXARK_CONTEXT_EVENT_TIME_INDEX_FULL_PAYLOAD still restores the full copy.
            payload = (
                json.dumps(enriched, sort_keys=True, separators=(",", ":"))
                if full_payload
                else self._context_event_time_index_payload(enriched)
            )
            entries.append(
                {
                    "key": self._context_event_time_index_key(enriched),
                    "field": self._context_event_time_index_field(enriched),
                    "value": payload,
                    "storage_route": record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {},
                }
            )
        return entries

    def _latest_context_state_key(self) -> str:
        return f"{self._storage_prefix}:context_latest_state"

    def _latest_context_state_field(self, record: Json) -> str | None:
        key = latest_context_state_key(record)
        if key is None:
            return None
        return ":".join(str(part) for part in key)

    def _latest_context_state_entries(self, records: list[Json]) -> list[Json]:
        entries: list[Json] = []
        for record in compact_latest_context_state_records(records):
            field = self._latest_context_state_field(record)
            if not field:
                continue
            payload_record = dict(slim_persisted_record(record))
            payload_record.pop("summary_version_hash", None)
            entries.append(
                {
                    "key": self._latest_context_state_key(),
                    "field": field,
                    "value": json.dumps(payload_record, sort_keys=True, separators=(",", ":")),
                    "storage_route": record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {},
                }
            )
        return entries

    def _append_log_records(self, records: list[Json]) -> list[Json]:
        return [record for record in records if latest_context_state_key(record) is None]

    def _split_compacted_latest_context_state(self, records: list[Json]) -> tuple[list[Json], list[Json]]:
        """Split an already-compacted batch into latest-state writes and append-log rows."""
        latest_state_entries: list[Json] = []
        append_records_for_log: list[Json] = []
        latest_state_key = self._latest_context_state_key()
        for record in records:
            field = self._latest_context_state_field(record)
            if not field:
                append_records_for_log.append(record)
                continue
            payload_record = dict(slim_persisted_record(record))
            payload_record.pop("summary_version_hash", None)
            latest_state_entries.append(
                {
                    "key": latest_state_key,
                    "field": field,
                    "value": json.dumps(payload_record, sort_keys=True, separators=(",", ":")),
                    "storage_route": record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {},
                }
            )
        return latest_state_entries, append_records_for_log

    def _load_latest_context_state_records(self) -> list[Json]:
        scanner = getattr(getattr(self, "_client", None), "scan_hash", None)
        if not callable(scanner):
            return []
        try:
            response = scanner(self._latest_context_state_key())
        except Exception:
            return []
        rows = response.get("records") if isinstance(response, dict) else []
        records: list[Json] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            if not isinstance(value, str) or not value:
                continue
            try:
                decoded = json.loads(value)
            except Exception:
                continue
            if isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _with_latest_context_state_records(self, records: list[Json]) -> list[Json]:
        """Fold the latest-state store into a native read and collapse it for serving.

        This runs on the RETRIEVAL hot path as well as the memory API, so it deliberately does
        only the last of the serving pipeline's three stages. The two the memory API also needs
        -- latest-value collapse and the tombstone sweep -- are applied one level up, in
        `MatrixArkTemporalStoreDirectAdapter._read_all_compacted`.

        The split was originally introduced because applying the stages here appeared to wedge
        the proxy -- a retrieve hanging 120s and every later one rejected on the lane at 40s.
        THAT ATTRIBUTION WAS WRONG, and is recorded here so it is not repeated. The wedge was
        the background summary refresher: it ran a full-store pass on a fixed 1000 ms timer and
        wrote the result back through the same single-permit proxy lane the request path uses,
        so it starved every request as soon as a pass outlasted the interval. A one-variable
        control settled it -- same rig, refresher off, 8/8 retrieves in 179-263 ms; refresher on
        with a fixed interval, the wedge; refresher on with a cost-proportional delay, 8/8 in
        229-760 ms. See `next_summary_refresh_delay_s`.

        So the split is now a COST choice, not a safety one: these two stages are O(n) and this
        method is on the retrieval hot path, where the memory API's needs do not apply. Moving
        them here would also keep deleted content out of the retrieval candidate set, as on the
        JSONL backend, which is the one behavioural argument for doing it -- deleted content can
        still influence retrieval scoring until the memory-API read sweeps it. That is worth
        revisiting on its own merits and with its own measurement; it is no longer blocked by an
        unexplained wedge.
        """
        try:
            from tools.matrixark_mcp_latest_context_state import expand_record_bundles
        except ModuleNotFoundError:  # Direct script execution from tools/.
            from matrixark_mcp_latest_context_state import expand_record_bundles
        return compact_latest_context_state_records(
            expand_record_bundles(records) + self._load_latest_context_state_records()
        )

    def _latest_context_state_records_for_candidate_scan(
        self,
        *,
        scope: Json,
        record_types: set[str],
        selected_node_hashes: set[int] | None,
    ) -> list[Json]:
        selected = {int(item) for item in (selected_node_hashes or set())}
        filtered: list[Json] = []
        for record in self._load_latest_context_state_records():
            record_type = str(record.get("record_type") or "")
            if record_type not in record_types:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            if selected:
                try:
                    node_hash = int(record.get("node_hash") or 0)
                except (TypeError, ValueError):
                    node_hash = 0
                if node_hash and node_hash not in selected:
                    continue
            filtered.append(record)
        return filtered

    def _records_can_use_direct_write_queue(self, records: list[Json]) -> bool:
        self._ensure_direct_write_queue_fields()
        if not bool(getattr(self, "_direct_write_queue_enabled", False)):
            return False
        if not records:
            return False
        if bool(getattr(self, "_direct_write_queue_allow_sync_context", False)):
            return all(isinstance(record, dict) for record in records)
        saw_background_route = False
        for record in records:
            route = record.get("storage_route")
            if not isinstance(route, dict) or not route:
                continue
            if route.get("sync_write") is True or route.get("background_write") is not True:
                return False
            saw_background_route = True
        return saw_background_route

    def _start_direct_write_worker(self) -> None:
        self._ensure_direct_write_queue_fields()
        with self._direct_write_worker_lock:
            if not self._direct_write_worker_started:
                self._direct_write_worker_started = True
                thread = threading.Thread(target=self._direct_write_loop, name="matrixark-direct-write-queue", daemon=True)
                thread.start()

    def _direct_write_durable_payload(self, records: list[Json]) -> Json:
        created_at = now_ms()
        return {
            "queue_version": 1,
            "status": "pending",
            "attempts": 0,
            "created_at_ms": created_at,
            "updated_at_ms": created_at,
            "record_count": len(records),
            "backend": self._backend_label(),
            "storage_prefix": self._storage_prefix,
            "records": records,
        }

