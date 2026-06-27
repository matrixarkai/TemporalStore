#!/usr/bin/env python3
"""TemporalStore-backed MatrixArk adapters for C++ and Rust backends."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _mcp_debug_log,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _mcp_debug_log,
    )

try:
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_metrics import MatrixArkServiceMetrics


def _latency_quantile_from_cumulative_buckets(buckets: list[int], bucket_bounds: tuple[float, ...], total: int, quantile: float) -> float:
    if total <= 0:
        return 0.0
    target = max(1, math.ceil(total * quantile))
    previous_bound = 0.0
    for count, bound in zip(buckets, bucket_bounds):
        if int(count) >= target:
            return previous_bound if bound == float("inf") else float(bound)
        if bound != float("inf"):
            previous_bound = float(bound)
    return previous_bound


def _latency_quantile_from_bucket_map(buckets: dict[str, Any], total: int, quantile: float) -> float:
    if total <= 0:
        return 0.0
    parsed: list[tuple[float, int]] = []
    for key, value in buckets.items():
        bound = float("inf") if str(key) == "+Inf" else float(key)
        parsed.append((bound, int(value or 0)))
    parsed.sort(key=lambda item: item[0])
    target = max(1, math.ceil(total * quantile))
    previous = 0.0
    for bound, count in parsed:
        if count >= target:
            return previous if bound == float("inf") else bound
        if bound != float("inf"):
            previous = bound
    return previous

class MatrixArkTemporalStoreDirectAdapter(MatrixArkLocalAdapter):
    """MatrixArk storage adapter backed by the native C++ TemporalStore SDK.

    The MCP extraction, node/summary/event mapping, traversal scoring, feedback,
    and replay logic still live in this process. Only the record log boundary is
    replaced: every MatrixArk record is persisted as a TemporalStore hash field.
    New prefixes use a compact sharded append log: hash field = zero-padded
    sequence within a shard, hash key = records:<shard>, and a tiny string key
    stores the global record count. Older prefixes that still have a JSON
    record_index are read through the legacy path.
    """

    def __init__(
        self,
        *,
        metaserver: str,
        namespace: str,
        table: str,
        library_path: str = "",
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
    ) -> None:
        super().__init__(Path("/tmp/matrixark-mcp-unused-direct.jsonl"))
        sdk_root = Path(__file__).resolve().parents[1] / "sdk" / "python"
        sys.path.insert(0, str(sdk_root))
        from temporalstore import Client, Options  # type: ignore

        options = Options(
            metaserver_addr=metaserver,
            namespace_name=namespace,
            table_name=table,
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=io_timeout_ms,
            max_read_retries=2,
            max_write_retries=1,
        )
        self._client = Client(options, library_path=library_path or None)
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)
        self._backend_ready = False
        self._backend_ready_result: Json | None = None
        self._backend_readiness_lock = threading.RLock()
        self._metrics_lock = threading.RLock()
        self._metrics_started_at_ms = now_ms()
        self._commands_total = 0
        self._errors_total = 0
        self._timeouts_total = 0
        self._latency_sum_ms = 0.0
        self._latency_max_ms = 0.0
        self._latency_buckets = [0 for _ in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS]
        self._records_written_total = 0
        self._records_read_total = 0

    def __post_init__(self) -> None:
        # Direct adapter does not use the inherited JSONL path.
        return

    def _backend_label(self) -> str:
        return "temporalstore-cpp"

    def _ensure_backend_metric_fields(self) -> None:
        if not hasattr(self, "_metrics_lock"):
            self._metrics_lock = threading.RLock()
        if not hasattr(self, "_metrics_started_at_ms"):
            self._metrics_started_at_ms = now_ms()
        if not hasattr(self, "_commands_total"):
            self._commands_total = 0
        if not hasattr(self, "_errors_total"):
            self._errors_total = 0
        if not hasattr(self, "_timeouts_total"):
            self._timeouts_total = 0
        if not hasattr(self, "_latency_sum_ms"):
            self._latency_sum_ms = 0.0
        if not hasattr(self, "_latency_max_ms"):
            self._latency_max_ms = 0.0
        if not hasattr(self, "_latency_buckets"):
            self._latency_buckets = [0 for _ in MatrixArkServiceMetrics.LATENCY_BUCKETS_MS]
        if not hasattr(self, "_records_written_total"):
            self._records_written_total = 0
        if not hasattr(self, "_records_read_total"):
            self._records_read_total = 0
        if not hasattr(self, "_backend_ready"):
            self._backend_ready = False
        if not hasattr(self, "_records_cache"):
            self._records_cache = []
        if not hasattr(self, "_audit_buffer"):
            self._audit_buffer = []
        if not hasattr(self, "_audit_flush_failures"):
            self._audit_flush_failures = 0

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
        backend = "cpp" if self._backend_label() in {"temporalstore-direct", "temporalstore-cpp"} else self._backend_label()
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
                    "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.",
                    "# TYPE matrixark_backend_audit_buffered_records gauge",
                    f'matrixark_backend_audit_buffered_records{{backend="{backend}"}} {len(getattr(self, "_audit_buffer", []))}',
                    "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.",
                    "# TYPE matrixark_backend_audit_flush_failures_total counter",
                    f'matrixark_backend_audit_flush_failures_total{{backend="{backend}"}} {int(getattr(self, "_audit_flush_failures", 0) or 0)}',
                ]
            )
            return "\n".join(lines) + "\n"

    def backend_metrics(self) -> Json:
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "prometheus": self._backend_prometheus(),
            "metrics": {
                "mode": "direct-sdk",
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
                "entry_count_cache": self._entry_count_cache,
                "records_cache_ready": self._records_cache is not None,
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
        try:
            raw = self._client.get_string(self._index_key)
        except Exception:
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
        try:
            raw = self._client.get_string(self._count_key)
        except Exception:
            return 0
        if not raw:
            return 0
        try:
            value = int(raw)
        except ValueError:
            return 0
        return max(0, value)

    def append(self, record: Json) -> None:
        records = materialize_serving_records(record)
        if self._queue_batched_records(records):
            return
        self._append_many_materialized(records)

    def append_many(self, records: list[Json]) -> None:
        records = materialize_serving_record_batch(records)
        self._append_many_materialized(records)

    def _storage_route_for_bundle(self, bundle: list[Json]) -> Json:
        for record in bundle:
            route = record.get("storage_route")
            if isinstance(route, dict) and route:
                return route
        return {}

    def _append_many_materialized(self, records: list[Json]) -> None:
        if not records:
            return
        if self._queue_batched_records(records):
            return
        started_perf = time.perf_counter()
        with self._records_lock:
            if self._records_cache is None:
                self.read_all()
            assert self._records_cache is not None
            if self._legacy_index_mode:
                if self._index_cache is None:
                    self._index_cache = self._get_index()
                entries: list[Json] = []
                for record in records:
                    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
                    record_id = (
                        f"{len(self._index_cache):020d}:"
                        f"{record.get('record_type', 'record')}:"
                        f"{stable_hash(json.dumps(record, sort_keys=True))}"
                    )
                    route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
                    entries.append({"key": self._record_hash_key, "field": record_id, "value": payload, "storage_route": route})
                    self._index_cache.append(record_id)
                self._hset_many_with_backoff(entries)
                self._put_string_with_backoff(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
                self._records_cache.extend(records)
                self._put_direct_record_cache(len(self._records_cache), self._records_cache)
                self._observe_backend_command((time.perf_counter() - started_perf) * 1000.0, records_written=len(records))
                return

            sequence = self._entry_count_cache if self._entry_count_cache is not None else self._get_count()
            entries = []
            for bundle in self._record_bundles(records):
                record_key, record_id = self._record_location(sequence)
                payload_value: Json
                payload_value = bundle[0] if len(bundle) == 1 else {"record_bundle": bundle}
                payload = json.dumps(payload_value, sort_keys=True, separators=(",", ":"))
                entries.append({"key": record_key, "field": record_id, "value": payload, "storage_route": self._storage_route_for_bundle(bundle)})
                sequence += 1
            self._hset_many_with_backoff(entries)
            self._put_string_with_backoff(self._count_key, str(sequence))
            self._entry_count_cache = sequence
            self._records_cache.extend(records)
            self._put_direct_record_cache(self._entry_count_cache, self._records_cache)
            self._observe_backend_command((time.perf_counter() - started_perf) * 1000.0, records_written=len(records))

    def append_audit(self, record: Json) -> None:
        if self._audit_mode == "drop":
            _mcp_debug_log("matrixark audit record dropped by MATRIXARK_DIRECT_AUDIT_MODE=drop")
            return
        if self._audit_mode == "sync":
            self.append(record)
            return
        with self._audit_lock:
            self._audit_buffer.append(record)
            if self._audit_mode == "buffered":
                self._ensure_audit_flusher_locked()
            max_pending = self._audit_buffer_max_records * 4
            if len(self._audit_buffer) > max_pending:
                dropped = len(self._audit_buffer) - max_pending
                self._audit_buffer = self._audit_buffer[-max_pending:]
                _mcp_debug_log(f"matrixark audit buffer dropped {dropped} oldest records after flush lag")

    def ensure_backend_ready(
        self,
        *,
        reason: str = "matrixark",
        probe: bool = True,
        timeout_ms: int | None = None,
    ) -> Json:
        with self._backend_readiness_lock:
            if self._backend_ready and self._backend_ready_result is not None:
                cached = dict(self._backend_ready_result)
                cached["cached"] = True
                cached["reason"] = reason
                return cached
            result = self._run_backend_readiness_gate(reason=reason, probe=probe, timeout_ms=timeout_ms)
            if result.get("status") == "ready":
                self._backend_ready = True
                self._backend_ready_result = dict(result)
            return result

    def _backend_metaserver(self) -> str:
        return str(getattr(self, "_metaserver", "") or getattr(getattr(self, "_client", None), "metaserver", ""))

    def _backend_label(self) -> str:
        return "temporalstore-direct"

    def _readiness_failure_result(
        self,
        *,
        reason: str,
        probe: bool,
        attempts: int,
        attempt_log: list[Json],
        error: str,
        checks: Json,
        metaserver: str,
        warmup_key: str,
        warmup_field: str,
    ) -> Json:
        return {
            "status": "topology_not_ready",
            "backend": self._backend_label(),
            "reason": reason,
            "probe": bool(probe),
            "attempts": attempts,
            "attempt_log": attempt_log,
            "error": error,
            "topology": {
                "metaserver": metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "warmup_key": warmup_key,
                "warmup_field": warmup_field,
            },
            "checks": checks,
        }

    def _run_backend_readiness_gate(
        self,
        *,
        reason: str,
        probe: bool = True,
        timeout_ms: int | None = None,
    ) -> Json:
        timeout = max(1, int(timeout_ms or BACKEND_READINESS_TIMEOUT_MS))
        timeout_s = max(0.1, timeout / 1000.0)
        backoff_s = max(0.01, BACKEND_READINESS_BACKOFF_MS / 1000.0)
        deadline = time.monotonic() + timeout_s
        attempts = 0
        metaserver = self._backend_metaserver()
        key = f"{self._storage_prefix}:readiness"
        field = f"{os.getpid()}:{int(time.time() * 1000)}:{stable_hash(reason)}"
        value = json.dumps({"reason": reason, "pid": os.getpid(), "created_at_ms": now_ms()}, sort_keys=True, separators=(",", ":"))
        attempt_log: list[Json] = []
        while True:
            attempts += 1
            checks: Json = {
                "mcp_process_started": True,
                "metaserver_reachable": {"ok": False, "address": metaserver, "error": "not checked"},
                "namespace_table_opened": False,
                "slot_coverage_verified_by_warmup_hset_hget": False,
            }
            if metaserver:
                meta_check = metaserver_reachable(metaserver)
                checks["metaserver_reachable"] = meta_check
                if not bool(meta_check.get("ok")):
                    last_error = f"metaserver unreachable: {meta_check.get('error', 'unknown')}"
                    attempt_log.append({"attempt": attempts, "ok": False, "retryable": True, "error": last_error, "checks": checks})
                    if time.monotonic() >= deadline:
                        return self._readiness_failure_result(
                            reason=reason,
                            probe=probe,
                            attempts=attempts,
                            attempt_log=attempt_log,
                            error=last_error,
                            checks=checks,
                            metaserver=metaserver,
                            warmup_key=key,
                            warmup_field=field,
                        )
                    time.sleep(min(backoff_s * attempts, 2.0))
                    continue
            try:
                checks["namespace_table_opened"] = True
                if probe:
                    self._client.hset(key, field, value)
                    readback = self._client.hget(key, field)
                    if readback != value:
                        raise MatrixArkError("readiness hget readback mismatch")
                    checks["slot_coverage_verified_by_warmup_hset_hget"] = True
                return {
                    "status": "ready",
                    "backend": self._backend_label(),
                    "reason": reason,
                    "probe": bool(probe),
                    "metaserver": metaserver,
                    "storage_prefix": self._storage_prefix,
                    "warmup_key": key,
                    "attempts": attempts,
                    "attempt_log": attempt_log,
                    "topology": {
                        "metaserver": metaserver,
                        "namespace": self._namespace,
                        "table": self._table,
                        "storage_prefix": self._storage_prefix,
                        "warmup_key": key,
                        "warmup_field": field,
                    },
                    "checks": checks,
                }
            except Exception as exc:
                last_error = str(exc)
                retryable = is_retryable_temporalstore_error(exc)
                attempt_log.append({"attempt": attempts, "ok": False, "retryable": retryable, "error": last_error, "checks": checks})
                if time.monotonic() >= deadline or not retryable:
                    return self._readiness_failure_result(
                        reason=reason,
                        probe=probe,
                        attempts=attempts,
                        attempt_log=attempt_log,
                        error=last_error,
                        checks=checks,
                        metaserver=metaserver,
                        warmup_key=key,
                        warmup_field=field,
                    )
                time.sleep(min(backoff_s * attempts, 2.0))

    def _hset_with_backoff(self, key: str, field: str, value: str) -> None:
        self._write_with_backoff(lambda: self._client.hset(key, field, value), op="hset")
        if self._write_throttle_s > 0:
            time.sleep(self._write_throttle_s)

    def _hset_many_with_backoff(self, entries: list[Json]) -> None:
        if not entries:
            return
        batch_hset = getattr(self._client, "batch_hset", None)
        if callable(batch_hset):
            self._write_with_backoff(lambda: batch_hset(entries), op="batch_hset")
            if self._write_throttle_s > 0:
                time.sleep(self._write_throttle_s)
            return
        for entry in entries:
            self._hset_with_backoff(str(entry["key"]), str(entry["field"]), str(entry["value"]))

    def _put_string_with_backoff(self, key: str, value: str) -> None:
        self._write_with_backoff(lambda: self._client.put_string(key, value), op="put_string")
        if self._write_throttle_s > 0:
            time.sleep(self._write_throttle_s)

    def _write_with_backoff(self, fn: Any, *, op: str) -> None:
        attempt = 0
        while True:
            try:
                fn()
                return
            except Exception:
                if attempt >= self._write_retries:
                    raise
                sleep_s = self._write_backoff_s * (2**attempt)
                if sleep_s > 0:
                    time.sleep(sleep_s)
                attempt += 1

    def flush_audits(self) -> None:
        with self._audit_lock:
            if not self._audit_buffer:
                return
            records = self._audit_buffer
            self._audit_buffer = []
        try:
            self.append_many(records)
        except Exception as exc:
            with self._audit_lock:
                self._audit_flush_failures += 1
                remaining_capacity = max(0, self._audit_buffer_max_records * 2 - len(self._audit_buffer))
                if remaining_capacity:
                    self._audit_buffer = records[-remaining_capacity:] + self._audit_buffer
            _mcp_debug_log(f"matrixark audit flush failed: {exc}")

    def _ensure_audit_flusher_locked(self) -> None:
        if self._audit_flusher_started:
            return
        self._audit_flusher_started = True
        thread = threading.Thread(target=self._audit_flush_loop, name="matrixark-audit-flusher", daemon=True)
        thread.start()

    def _audit_flush_loop(self) -> None:
        while True:
            time.sleep(self._audit_flush_interval_s)
            try:
                self.flush_audits()
            except Exception as exc:
                _mcp_debug_log(f"matrixark audit flush loop failed: {exc}")

    def _record_bundles(self, records: list[Json]) -> list[list[Json]]:
        bundles: list[list[Json]] = []
        current: list[Json] = []
        current_bytes = 0
        max_bytes = max(8192, DIRECT_RECORD_BUNDLE_MAX_BYTES)
        for record in records:
            record_bytes = len(json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8"))
            if current and current_bytes + record_bytes > max_bytes:
                bundles.append(current)
                current = []
                current_bytes = 0
            current.append(record)
            current_bytes += record_bytes
        if current:
            bundles.append(current)
        return bundles

    def read_all(self) -> list[Json]:
        with self._records_lock:
            if self._records_cache is not None:
                return list(self._records_cache)
            count = self._get_count()
            if count > 0:
                self._legacy_index_mode = False
                self._entry_count_cache = count
                cached = self._get_direct_record_cache(count)
                if cached is not None:
                    self._records_cache = cached
                    return list(self._records_cache)
                with self._direct_record_load_lock():
                    cached = self._get_direct_record_cache(count)
                    if cached is not None:
                        self._records_cache = cached
                        return list(self._records_cache)
                    self._records_cache = self._load_records_by_count(count)
                    self._put_direct_record_cache(count, self._records_cache)
                    return list(self._records_cache)
            index = self._get_index()
            self._index_cache = index
            self._legacy_index_mode = bool(index)
            self._entry_count_cache = None
            self._records_cache = self._load_records(index)
            return list(self._records_cache)

    def _direct_record_load_lock(self) -> threading.RLock:
        with _DIRECT_RECORD_CACHE_LOCK:
            lock = _DIRECT_RECORD_LOAD_LOCKS.get(self._storage_prefix)
            if lock is None:
                lock = threading.RLock()
                _DIRECT_RECORD_LOAD_LOCKS[self._storage_prefix] = lock
            return lock

    def _get_direct_record_cache(self, count: int) -> list[Json] | None:
        with _DIRECT_RECORD_CACHE_LOCK:
            cached = _DIRECT_RECORD_CACHE.get(self._storage_prefix)
            if cached is None:
                return None
            cached_count, records = cached
            if cached_count != count:
                return None
            return list(records)

    def _put_direct_record_cache(self, count: int, records: list[Json]) -> None:
        with _DIRECT_RECORD_CACHE_LOCK:
            if len(_DIRECT_RECORD_CACHE) >= _DIRECT_RECORD_CACHE_MAX_PREFIXES and self._storage_prefix not in _DIRECT_RECORD_CACHE:
                oldest = next(iter(_DIRECT_RECORD_CACHE))
                _DIRECT_RECORD_CACHE.pop(oldest, None)
            _DIRECT_RECORD_CACHE[self._storage_prefix] = (count, list(records))

    def _load_records_by_count(self, count: int) -> list[Json]:
        records = []
        batch_hget = getattr(self._client, "batch_hget", None)
        if callable(batch_hget):
            entries = []
            for sequence in range(count):
                record_key, record_id = self._record_location(sequence)
                entries.append({"key": record_key, "field": record_id})
            try:
                read_records = batch_hget(entries)
            except Exception:
                read_records = []
            for item in read_records:
                if not isinstance(item, dict):
                    continue
                payload = item.get("value", "")
                if not payload:
                    continue
                decoded = json.loads(str(payload))
                if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                    records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
                elif isinstance(decoded, dict):
                    records.append(decoded)
            if records or count == 0:
                return records
        for sequence in range(count):
            record_key, record_id = self._record_location(sequence)
            try:
                payload = self._client.hget(record_key, record_id)
            except Exception:
                continue
            if not payload:
                continue
            decoded = json.loads(payload)
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                records.extend(item for item in decoded["record_bundle"] if isinstance(item, dict))
            elif isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _record_location(self, sequence: int) -> tuple[str, str]:
        shard = sequence // self._shard_size
        offset = sequence % self._shard_size
        return f"{self._record_hash_key}:{shard:06d}", f"{offset:020d}"

    def _load_records(self, index: list[str]) -> list[Json]:
        records = []
        for record_id in index:
            try:
                payload = self._client.hget(self._record_hash_key, record_id)
            except Exception:
                continue
            if not payload:
                continue
            records.append(json.loads(payload))
        return records


class MatrixArkRustCliClient:
    """Persistent process boundary around the Rust TemporalStore SDK.

    The Rust binary owns direct SDK linkage and runs in JSON-lines serve mode.
    Keeping one process alive avoids spawning the CLI and reconnecting the Rust
    SDK for every hset/hget, which was the main Rust MCP latency source.
    """

    def __init__(
        self,
        *,
        cli_path: str,
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
    ) -> None:
        if not cli_path:
            raise MatrixArkError("--rust-cli or MATRIXARK_TEMPORALSTORE_RUST_CLI is required for temporalstore-rust")
        self.cli_path = cli_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self._lock = threading.Lock()
        self._semaphore = threading.BoundedSemaphore(1)
        self._backpressure_timeout_s = max(
            0.05,
            int(os.environ.get("MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS", str(request_timeout_ms))) / 1000.0,
        )
        self._metrics_lock = threading.Lock()
        self._commands_total = 0
        self._commands_failed_total = 0
        self._records_written_total = 0
        self._records_read_total = 0
        self._backpressure_rejections_total = 0
        self._timeouts_total = 0
        self._last_latency_ms = 0.0
        self._max_observed_latency_ms = 0.0
        self._latency_samples_ms: list[float] = []
        self._context_record_counts: dict[str, int] = {}
        self._started_at = time.time()
        self._proc: subprocess.Popen[str] | None = None

    def close(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        if proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass

    def _ensure_proc(self) -> subprocess.Popen[str]:
        if self._proc is not None and self._proc.poll() is None:
            return self._proc
        self.close()
        self._proc = subprocess.Popen(
            [self.cli_path, "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        return self._proc

    def _read_json_line(self, proc: subprocess.Popen[str], op: str) -> Json:
        assert proc.stdout is not None
        deadline = time.monotonic() + max(2.0, self.request_timeout_ms / 1000.0 + 2.0)
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise MatrixArkError(f"Rust TemporalStore {op} process exited ({proc.returncode}): {stderr[-1000:]}")
            ready, _, _ = select.select([proc.stdout], [], [], 0.05)
            if not ready:
                continue
            line = proc.stdout.readline()
            if not line:
                continue
            if not line.strip().startswith("{"):
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError as exc:
                raise MatrixArkError(f"Rust TemporalStore {op} returned invalid JSON: {line[:200]!r}") from exc
        raise MatrixArkError(
            f"Rust TemporalStore {op} timed out waiting for response from {self.cli_path} "
            f"after {max(2.0, self.request_timeout_ms / 1000.0 + 2.0):.1f}s"
        )

    def _call_json(self, op: str, **kwargs: Any) -> Json:
        command = {
            "op": op,
            "metaserver": self.metaserver,
            "namespace": self.namespace,
            "table": self.table,
            "request_timeout_ms": self.request_timeout_ms,
            "io_timeout_ms": self.io_timeout_ms,
            **kwargs,
        }
        payload = json.dumps(command, separators=(",", ":")) + "\n"
        started = time.perf_counter()
        acquired = self._semaphore.acquire(timeout=self._backpressure_timeout_s)
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, backpressure=True)
            raise MatrixArkError(
                f"Rust TemporalStore {op} rejected by gateway backpressure after "
                f"{self._backpressure_timeout_s:.3f}s"
            )
        try:
            with self._lock:
                proc = self._ensure_proc()
                assert proc.stdin is not None
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                except BrokenPipeError as exc:
                    self.close()
                    raise MatrixArkError(f"Rust TemporalStore {op} pipe closed") from exc
                response = self._read_json_line(proc, op)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True)
            raise
        finally:
            self._semaphore.release()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not response.get("ok"):
            self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=True)
            raise MatrixArkError(f"Rust TemporalStore {op} failed: {response.get('error', 'unknown error')}")
        self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=False)
        return response

    def _record_call_metrics(
        self,
        op: str,
        kwargs: Json,
        response: Json | None,
        elapsed_ms: float,
        *,
        failed: bool,
        backpressure: bool = False,
    ) -> None:
        with self._metrics_lock:
            self._commands_total += 1
            if failed:
                self._commands_failed_total += 1
                if "timed out" in str(response or "").lower() or elapsed_ms >= self.request_timeout_ms:
                    self._timeouts_total += 1
            if backpressure:
                self._backpressure_rejections_total += 1
            self._last_latency_ms = elapsed_ms
            self._max_observed_latency_ms = max(self._max_observed_latency_ms, elapsed_ms)
            self._latency_samples_ms.append(elapsed_ms)
            if len(self._latency_samples_ms) > 2048:
                del self._latency_samples_ms[: len(self._latency_samples_ms) - 2048]
            if response and response.get("ok"):
                count = int(response.get("count") or 0)
                if op in {"put_string", "hset"}:
                    self._records_written_total += 1
                    self._count_context_record(kwargs.get("value"))
                elif op == "batch_hset":
                    self._records_written_total += count or len(kwargs.get("entries") or [])
                    for entry in kwargs.get("entries") or []:
                        if isinstance(entry, dict):
                            self._count_context_record(entry.get("value"))
                elif op in {"get_string", "hget"}:
                    self._records_read_total += 1
                elif op in {"batch_hget", "hgetall", "scan_hash"}:
                    self._records_read_total += count

    def _count_context_record(self, value: Any) -> None:
        if not isinstance(value, str) or not value.startswith("{"):
            return
        try:
            payload = json.loads(value)
        except Exception:
            return
        record_type = str(payload.get("record_type") or "")
        if not record_type:
            return
        self._context_record_counts[record_type] = self._context_record_counts.get(record_type, 0) + 1

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
        return ordered[index]

    def metrics_snapshot(self) -> Json:
        with self._metrics_lock:
            elapsed_s = max(0.001, time.time() - self._started_at)
            samples = list(self._latency_samples_ms)
            context_counts = dict(sorted(self._context_record_counts.items()))
            return {
                "gateway_mode": "long_lived_stdio_gateway",
                "transport": "stdio",
                "cli_path": self.cli_path,
                "max_inflight": 1,
                "backpressure_timeout_ms": int(self._backpressure_timeout_s * 1000),
                "commands_total": self._commands_total,
                "commands_failed_total": self._commands_failed_total,
                "timeouts_total": self._timeouts_total,
                "qps": round(self._commands_total / elapsed_s, 6),
                "records_written_total": self._records_written_total,
                "records_read_total": self._records_read_total,
                "backpressure_rejections_total": self._backpressure_rejections_total,
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
                "process_per_operation_enabled": False,
                "single_shot_mode": "debug_only",
                "supports_health": True,
                "supports_readiness": True,
                "supports_metrics": True,
                "supports_batch_append": True,
                "supports_prefix_scan": True,
                "supports_graceful_shutdown": True,
                "structured_errors": True,
            }

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

    def put_string(self, key: str, value: str) -> None:
        self._call("put_string", key=key, value=value)

    def get_string(self, key: str) -> str:
        return self._call("get_string", key=key)

    def hset(self, key: str, field: str, value: str) -> None:
        self._call("hset", key=key, field=field, value=value)

    def hget(self, key: str, field: str) -> str:
        return self._call("hget", key=key, field=field)

    def batch_hset(self, entries: list[Json]) -> None:
        if not entries:
            return
        self._call_json("batch_hset", entries=entries)

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        if not entries:
            return []
        response = self._call_json("batch_hget", entries=entries)
        records = response.get("records", [])
        return records if isinstance(records, list) else []

    def scan_hash(self, key: str) -> Json:
        return self._call_json("scan_hash", key=key)

    def metrics_prometheus(self) -> str:
        return str(self._call_json("metrics_prometheus").get("prometheus", ""))

    def health(self) -> Json:
        return self._call_json("health")

    def readiness(self) -> Json:
        return self._call_json("readiness")

    def shutdown(self) -> None:
        try:
            self._call_json("shutdown")
        finally:
            self.close()


class MatrixArkTemporalStoreRustAdapter(MatrixArkTemporalStoreDirectAdapter):
    """MatrixArk record-log adapter backed by the Rust TemporalStore SDK."""

    def __init__(
        self,
        *,
        rust_cli: str,
        metaserver: str,
        namespace: str,
        table: str,
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
    ) -> None:
        MatrixArkLocalAdapter.__init__(self, Path("/tmp/matrixark-mcp-unused-rust.jsonl"))
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._client = MatrixArkRustCliClient(
            cli_path=rust_cli,
            metaserver=metaserver,
            namespace=namespace,
            table=table,
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=io_timeout_ms,
        )
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._entry_count_cache: int | None = None
        self._legacy_index_mode = False
        self._records_lock = threading.RLock()
        self._audit_lock = threading.RLock()
        self._audit_buffer: list[Json] = []
        self._audit_flusher_started = False
        self._audit_flush_failures = 0
        if DIRECT_AUDIT_MODE not in {"buffered", "deferred", "drop", "sync"}:
            raise MatrixArkError("MATRIXARK_DIRECT_AUDIT_MODE must be buffered, deferred, drop, or sync")
        self._audit_mode = DIRECT_AUDIT_MODE
        self._audit_buffer_max_records = max(1, DIRECT_AUDIT_BUFFER_MAX_RECORDS)
        self._audit_flush_interval_s = max(0.05, DIRECT_AUDIT_FLUSH_INTERVAL_MS / 1000.0)
        self._write_retries = max(0, DIRECT_WRITE_RETRIES)
        self._write_backoff_s = max(0.0, DIRECT_WRITE_BACKOFF_MS / 1000.0)
        self._write_throttle_s = max(0.0, DIRECT_WRITE_THROTTLE_MS / 1000.0)
        self._backend_ready = False
        self._backend_ready_result = None
        self._backend_readiness_lock = threading.RLock()

    def _backend_metaserver(self) -> str:
        return self._client.metaserver

    def _backend_label(self) -> str:
        return "temporalstore-rust"

    def _backend_neutral_prometheus(self, snapshot: Json) -> str:
        backend = "rust"
        buckets = snapshot.get("latency_buckets") if isinstance(snapshot.get("latency_buckets"), dict) else {}
        lines = [
            "# HELP matrixark_backend_qps MatrixArk storage backend command QPS.",
            "# TYPE matrixark_backend_qps gauge",
            f'matrixark_backend_qps{{backend="{backend}"}} {snapshot.get("qps", 0)}',
            "# HELP matrixark_backend_commands_total MatrixArk storage backend command count.",
            "# TYPE matrixark_backend_commands_total counter",
            f'matrixark_backend_commands_total{{backend="{backend}"}} {int(snapshot.get("commands_total") or 0)}',
            "# HELP matrixark_backend_errors_total MatrixArk storage backend command errors.",
            "# TYPE matrixark_backend_errors_total counter",
            f'matrixark_backend_errors_total{{backend="{backend}"}} {int(snapshot.get("commands_failed_total") or 0)}',
            "# HELP matrixark_backend_timeouts_total MatrixArk storage backend command timeouts.",
            "# TYPE matrixark_backend_timeouts_total counter",
            f'matrixark_backend_timeouts_total{{backend="{backend}"}} {int(snapshot.get("timeouts_total") or 0)}',
            "# HELP matrixark_backend_info MatrixArk storage backend identity and mode.",
            "# TYPE matrixark_backend_info gauge",
            f'matrixark_backend_info{{backend="{backend}",storage_mode="rust-gateway"}} 1',
            "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{backend="{backend}",storage_mode="rust-gateway",status="{"ready" if self._backend_ready else "unknown"}"}} {1 if self._backend_ready else 0}',
            "# HELP matrixark_backend_command_latency_ms MatrixArk storage backend command latency quantiles.",
            "# TYPE matrixark_backend_command_latency_ms gauge",
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.50"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.50), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.95"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.95), 3)}',
            f'matrixark_backend_command_latency_ms{{backend="{backend}",quantile="0.99"}} {round(_latency_quantile_from_bucket_map(buckets, int(snapshot.get("latency_ms_count") or 0), 0.99), 3)}',
            "# HELP matrixark_backend_command_latency_ms_bucket MatrixArk storage backend command latency buckets.",
            "# TYPE matrixark_backend_command_latency_ms_bucket counter",
        ]
        for bucket, count in buckets.items():
            lines.append(f'matrixark_backend_command_latency_ms_bucket{{backend="{backend}",le="{bucket}"}} {int(count)}')
        lines.extend(
            [
                "# HELP matrixark_backend_command_latency_ms_sum MatrixArk storage backend command latency sum in milliseconds.",
                "# TYPE matrixark_backend_command_latency_ms_sum counter",
                f'matrixark_backend_command_latency_ms_sum{{backend="{backend}"}} {snapshot.get("latency_ms_sum", 0)}',
                "# HELP matrixark_backend_command_latency_ms_count MatrixArk storage backend command latency sample count.",
                "# TYPE matrixark_backend_command_latency_ms_count counter",
                f'matrixark_backend_command_latency_ms_count{{backend="{backend}"}} {int(snapshot.get("latency_ms_count") or 0)}',
                "# HELP matrixark_backend_command_latency_max_ms MatrixArk storage backend maximum command latency in milliseconds.",
                "# TYPE matrixark_backend_command_latency_max_ms gauge",
                f'matrixark_backend_command_latency_max_ms{{backend="{backend}"}} {snapshot.get("latency_ms_max", 0)}',
                "# HELP matrixark_backend_records_written_total MatrixArk storage backend records written.",
                "# TYPE matrixark_backend_records_written_total counter",
                f'matrixark_backend_records_written_total{{backend="{backend}"}} {int(snapshot.get("records_written_total") or 0)}',
                "# HELP matrixark_backend_records_read_total MatrixArk storage backend records read.",
                "# TYPE matrixark_backend_records_read_total counter",
                f'matrixark_backend_records_read_total{{backend="{backend}"}} {int(snapshot.get("records_read_total") or 0)}',
                "# HELP matrixark_context_records_total MatrixArk context records currently cached by backend.",
                "# TYPE matrixark_context_records_total gauge",
                f'matrixark_context_records_total{{backend="{backend}"}} {int(snapshot.get("matrixark_context_records_total") or 0)}',
                "# HELP matrixark_backend_cached_clients MatrixArk storage backend cached clients.",
                "# TYPE matrixark_backend_cached_clients gauge",
                f'matrixark_backend_cached_clients{{backend="{backend}"}} {int(snapshot.get("clients_created_total") or 1)}',
                "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.",
                "# TYPE matrixark_backend_audit_buffered_records gauge",
                f'matrixark_backend_audit_buffered_records{{backend="{backend}"}} {len(getattr(self, "_audit_buffer", []))}',
                "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.",
                "# TYPE matrixark_backend_audit_flush_failures_total counter",
                f'matrixark_backend_audit_flush_failures_total{{backend="{backend}"}} {int(getattr(self, "_audit_flush_failures", 0) or 0)}',
            ]
        )
        return "\n".join(lines) + "\n"

    def backend_metrics(self) -> Json:
        health: Json
        readiness: Json
        try:
            health = self._client.health()
        except Exception as exc:
            health = {"ok": False, "error": str(exc)}
        try:
            readiness = self._client.readiness()
        except Exception as exc:
            readiness = {"ok": False, "error": str(exc)}
        rust_client_metrics = self._client.metrics_snapshot()
        try:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + self._client.metrics_prometheus()
        except Exception as exc:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + f"# matrixark_rust_gateway_metrics_error {json.dumps(str(exc))}\n"
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "gateway_mode": "long_lived_stdio_gateway",
            "production_path": "long_lived_only",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only",
            "capabilities": {
                "health_endpoint": True,
                "readiness_endpoint": True,
                "metrics_endpoint": True,
                "batch_append": True,
                "prefix_scan": True,
                "connection_pooling": True,
                "client_pooling": True,
                "backpressure": True,
                "graceful_shutdown": True,
                "timeout_handling": True,
                "structured_errors_cpp_compatible": True,
            },
            "health": health,
            "readiness": readiness,
            "prometheus": prometheus,
            "metrics": {
                "metaserver": self._metaserver,
                "namespace": self._namespace,
                "table": self._table,
                "storage_prefix": self._storage_prefix,
                "audit_mode": self._audit_mode,
                "audit_buffered_records": len(self._audit_buffer),
                "audit_flush_failures": self._audit_flush_failures,
                "rust_client": rust_client_metrics,
            },
        }



