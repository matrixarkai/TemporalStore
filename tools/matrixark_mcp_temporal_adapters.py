#!/usr/bin/env python3
"""TemporalStore-backed MatrixArk adapters for C++ and Rust backends."""

from __future__ import annotations

import queue

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
        _mcp_debug_log,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
        _mcp_debug_log,
    )

try:
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_retrieval import native_retrieve_fallback_allowed
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_retrieval import native_retrieve_fallback_allowed


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


def _native_scope_with_hashes(scope: Json) -> Json:
    if not isinstance(scope, dict):
        return {}
    if int(scope.get("tenant_hash") or 0) and canonical_scope_key(scope):
        return dict(scope)
    defaults = local_identity_defaults({}, scope)
    account_id = str(scope.get("account_id") or defaults.get("account_id") or "acct_local")
    tenant_id = str(scope.get("tenant_id") or defaults.get("tenant_id") or "tenant_local_agent")
    user_id = str(scope.get("user_id") or defaults.get("user_id") or "")
    session_id = str(scope.get("session_id") or defaults.get("session_id") or "")
    hashes = identity_hashes(account_id, tenant_id, user_id, session_id)
    explicit_scope_keys = {str(key) for key in scope.get("_explicit_scope_keys", []) if isinstance(key, str)}
    explicit_scope_keys.update(str(key) for key in scope.keys())
    enriched = {
        **scope,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "tenant_hash": hashes["tenant_hash"],
        "scope_key": hashes["scope_key"],
        "_explicit_scope_keys": sorted(explicit_scope_keys),
    }
    if user_id:
        enriched["user_id"] = user_id
        enriched["user_hash"] = hashes["user_hash"]
    if session_id:
        enriched["session_id"] = session_id
        enriched["session_hash"] = hashes["session_hash"]
    return enriched



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
        MatrixArkLocalAdapter._init_local_runtime_state(self)
        self._entity_cache_loaded = True
        self._context_node_cache_loaded = True
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
        self._matrixark_native_batch_append_available = bool(
            getattr(getattr(self._client, "_native", None), "has_matrixark_batch_append_records", False)
        )
        self._matrixark_append_write_path = (
            "native_c_api_hash_mset_grouped"
            if self._matrixark_native_batch_append_available
            else "fallback_python_batch_hset_loop"
        )
        self._matrixark_append_uses_per_record_hset = not self._matrixark_native_batch_append_available
        # C++ has a native CONTEXT extension (WRITE_EVENT / WRITE_EXTRACTED_EVENT)
        # but the generic JSON record-log adapter still persists through the
        # MatrixArk batch hash API. Keep this explicit in metrics/reports so the
        # deeper append-queue optimization is not confused with the API boundary.
        self._matrixark_context_extension_append_selected = False
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._supported_storage_families = self._parse_supported_storage_families()
        self._record_hash_key = f"{self._storage_prefix}:records"
        self._index_key = f"{self._storage_prefix}:record_index"
        self._count_key = f"{self._storage_prefix}:record_count"
        configured_raw_prefix = os.environ.get("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX", "").strip().rstrip(":")
        self._raw_ingestion_prefix = configured_raw_prefix or f"{self._storage_prefix}:raw_ingestion"
        self._raw_record_hash_key = f"{self._raw_ingestion_prefix}:records"
        self._raw_count_key = f"{self._raw_ingestion_prefix}:record_count"
        self._raw_storage_backend = self._normalize_raw_storage_backend(
            os.environ.get("MATRIXARK_RAW_INGESTION_BACKEND", "temporalstore")
        )
        self._raw_entry_count_cache: int | None = None
        self._shard_size = DIRECT_RECORD_LOG_SHARD_SIZE
        self._index_cache: list[str] | None = None
        self._records_cache: list[Json] | None = None
        self._retrieval_candidate_cache: dict[str, Json] = {}
        self._retrieval_candidate_cache_lock = threading.RLock()
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
        self._direct_write_queue_enabled = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_write_queue_max_records = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", "10000")))
        self._direct_write_queue_put_timeout_s = max(0.01, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", "1000")) / 1000.0)
        self._direct_write_queue_mode = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory").strip().lower() or "memory"
        if self._direct_write_queue_mode not in {"memory", "temporalstore"}:
            raise MatrixArkError("MATRIXARK_DIRECT_WRITE_QUEUE_MODE must be memory or temporalstore")
        self._direct_write_queue_key = f"{self._storage_prefix}:direct_write_queue"
        self._direct_write_queue_done_key = f"{self._storage_prefix}:direct_write_queue_done"
        self._direct_write_queue_dead_key = f"{self._storage_prefix}:direct_write_queue_dead"
        self._direct_write_queue: queue.Queue[Any] = queue.Queue(maxsize=self._direct_write_queue_max_records)
        self._direct_write_worker_started = False
        self._direct_write_worker_lock = threading.RLock()
        self._direct_write_stop = threading.Event()
        self._direct_write_failures = 0
        self._direct_write_enqueued_records = 0
        self._direct_write_flushed_records = 0
        self._direct_write_enqueued_batches = 0
        self._direct_write_flushed_batches = 0
        self._direct_write_dead_letter_batches = 0
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
        self._append_queue_wait_ms_total = 0.0
        self._append_queue_wait_count = 0
        self._append_engine_ms_total = 0.0
        self._append_engine_count = 0

    def __post_init__(self) -> None:
        # Direct adapter does not use the inherited JSONL path.
        return

    def _parse_supported_storage_families(self) -> set[str]:
        raw = os.environ.get("MATRIXARK_NATIVE_STORAGE_FAMILIES") or os.environ.get("MATRIXARK_SUPPORTED_STORAGE_FAMILIES") or "default,local,single_node,shared_store"
        families = {part.strip().lower().replace("-", "_") for part in raw.split(",") if part.strip()}
        return families or {"default", "local", "single_node", "shared_store"}

    def _validate_storage_routes_available(self, records: list[Json]) -> None:
        if not hasattr(self, "_supported_storage_families"):
            self._supported_storage_families = self._parse_supported_storage_families()
        requested: set[str] = set()
        for record in records:
            route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
            family = str(route.get("storage_family") or route.get("selected_storage_family") or "default").strip().lower().replace("-", "_")
            if family and family != "default":
                requested.add(family)
        if len(requested) > 1:
            raise MatrixArkError(f"one MatrixArk write batch cannot mix storage families: {sorted(requested)}")
        unsupported = requested - set(getattr(self, "_supported_storage_families", {"default"}))
        if unsupported:
            raise MatrixArkError(
                f"requested storage_family {sorted(unsupported)} is not configured for backend {self._backend_label()}; "
                f"configured families={sorted(getattr(self, '_supported_storage_families', []))}"
            )

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
        if not hasattr(self, "_append_queue_wait_ms_total"):
            self._append_queue_wait_ms_total = 0.0
        if not hasattr(self, "_append_queue_wait_count"):
            self._append_queue_wait_count = 0
        if not hasattr(self, "_append_engine_ms_total"):
            self._append_engine_ms_total = 0.0
        if not hasattr(self, "_append_engine_count"):
            self._append_engine_count = 0
        if not hasattr(self, "_backend_ready"):
            self._backend_ready = False
        if not hasattr(self, "_records_cache"):
            self._records_cache = []
        if not hasattr(self, "_retrieval_candidate_cache"):
            self._retrieval_candidate_cache = {}
        if not hasattr(self, "_retrieval_candidate_cache_lock"):
            self._retrieval_candidate_cache_lock = threading.RLock()
        if not hasattr(self, "_audit_buffer"):
            self._audit_buffer = []
        if not hasattr(self, "_audit_flush_failures"):
            self._audit_flush_failures = 0
        self._ensure_direct_write_queue_fields()

    def _ensure_direct_write_queue_fields(self) -> None:
        if not hasattr(self, "_direct_write_queue_enabled"):
            self._direct_write_queue_enabled = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE", "0").strip().lower() in {"1", "true", "yes"}
        if not hasattr(self, "_direct_write_queue_max_records"):
            self._direct_write_queue_max_records = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", "10000")))
        if not hasattr(self, "_direct_write_queue_put_timeout_s"):
            self._direct_write_queue_put_timeout_s = max(0.01, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", "1000")) / 1000.0)
        if not hasattr(self, "_direct_write_queue_mode"):
            self._direct_write_queue_mode = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory").strip().lower() or "memory"
        if self._direct_write_queue_mode not in {"memory", "temporalstore"}:
            self._direct_write_queue_mode = "memory"
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
                "mode": "direct-sdk",
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
        self._append_raw_ingestion_records([record])
        records = materialize_serving_records(record)
        if self._queue_batched_records(records):
            return
        self._append_many_materialized(records)

    def append_many(self, records: list[Json]) -> None:
        self._append_raw_ingestion_records(records)
        materialized = materialize_serving_record_batch(records)
        self._append_many_materialized(materialized)

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
        if backend not in {"temporalstore", "matrixkv"}:
            raise MatrixArkError("MATRIXARK_RAW_INGESTION_BACKEND must be temporalstore or matrixkv")
        return backend

    def _raw_ingestion_append_path(self) -> str:
        backend = self._normalize_raw_storage_backend(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )
        if backend == "temporalstore":
            return "matrixark_raw_ingestion_temporalstore_log"
        return "matrixark_raw_ingestion_matrixkv_log"

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

    def _append_raw_ingestion_records(self, records: list[Json]) -> None:
        if not records:
            return
        self._ensure_raw_ingestion_fields()
        if self._raw_ingestion_prefix == self._storage_prefix:
            raise MatrixArkError("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX must differ from the serving storage prefix")
        started_perf = time.perf_counter()
        with self._records_lock:
            count = self._raw_entry_count_cache if self._raw_entry_count_cache is not None else self._get_raw_count()
            sequence = count
            entries: list[Json] = []
            for record in records:
                record_key, record_id = self._raw_record_location(sequence)
                payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
                route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
                entries.append({"key": record_key, "field": record_id, "value": payload, "storage_route": route})
                sequence += 1
            append_records = getattr(self._client, "matrixark_batch_append_records", None)
            if callable(append_records):
                self._write_with_backoff(
                    lambda: append_records(
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
            self._raw_entry_count_cache = sequence
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            self._observe_backend_command(elapsed_ms, records_written=len(records))

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
                if not isinstance(location, dict):
                    continue
                key = str(location.get("key") or "")
                field = str(location.get("field") or "")
                if not key or not field or (key, field) in seen:
                    continue
                locations.append({"key": key, "field": field})
                seen.add((key, field))
        for location in new_locations:
            key = str(location.get("key") or "")
            field = str(location.get("field") or "")
            if not key or not field or (key, field) in seen:
                continue
            locations.append({"key": key, "field": field})
            seen.add((key, field))
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

    def _read_hash_value_best_effort(self, key: str, field: str) -> str:
        reader = getattr(self._client, "hget", None)
        if not callable(reader):
            return ""
        try:
            value = reader(key, field)
        except Exception:
            return ""
        return str(value or "")

    def _native_side_index_entries_for_bundles(self, bundles: list[tuple[list[Json], str, str]]) -> list[Json]:
        """Build sidecar lookup rows so retrieval can avoid broad record scans.

        ContextIndex remains compact and bucketed.  These sidecar hashes make the
        compact postings and ref-to-record locations directly addressable by the
        native C++/Rust hash API.
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

        entries: list[Json] = []
        for (key, field), update in lookup_updates.items():
            new_refs = update.get("ref_hashes", []) if isinstance(update, dict) else []
            new_buckets = update.get("posting_buckets", set()) if isinstance(update, dict) else set()
            merged_refs = self._merge_ref_hashes(self._read_hash_value_best_effort(key, field), new_refs)
            existing_value = self._read_hash_value_best_effort(key, field)
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
        for ref_hash, new_locations in locator_updates.items():
            field = str(ref_hash)
            merged_locations = self._merge_ref_locations(self._read_hash_value_best_effort(locator_key, field), new_locations)
            entries.append(
                {
                    "key": locator_key,
                    "field": field,
                    "value": json.dumps({"locations": merged_locations}, separators=(",", ":")),
                    "storage_route": route_by_hash_field.get((locator_key, field), {}),
                }
            )
        for (key, field), update in placement_updates.items():
            new_locations = update.get("locations", []) if isinstance(update, dict) else []
            new_versions = update.get("resource_versions", set()) if isinstance(update, dict) else set()
            existing_value = self._read_hash_value_best_effort(key, field)
            merged_locations = self._merge_ref_locations(existing_value, new_locations)
            merged_versions = self._merge_resource_versions(existing_value, new_versions if isinstance(new_versions, set) else set())
            entries.append(
                {
                    "key": key,
                    "field": field,
                    "value": json.dumps({"locations": merged_locations, "resource_versions": merged_versions}, separators=(",", ":")),
                    "storage_route": route_by_hash_field.get((key, field), {}),
                }
            )
        return entries

    def _context_event_ingestion_time_ms(self, record: Json) -> int:
        return context_event_timestamp_ms(record)

    def _context_event_time_index_key(self, record: Json) -> str:
        scope_key = str(record.get("scope_key") or "")
        scope_hash = stable_hash(scope_key) if scope_key else int(record.get("node_hash") or 0)
        return f"{self._storage_prefix}:context_event_by_ingestion_time:{scope_hash}"

    def _context_event_time_index_field(self, record: Json) -> str:
        event_hash = record.get("event_id_hash") or stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":")))
        timestamp_ms = self._context_event_ingestion_time_ms(record)
        return f"{context_event_time_key(timestamp_ms, event_hash):020d}:{event_hash}"

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
        full_payload = os.environ.get("MATRIXARK_CONTEXT_EVENT_TIME_INDEX_FULL_PAYLOAD", "0").strip().lower() in {"1", "true", "yes"}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if record.get("event_id_hash") is None:
                continue
            payload = (
                json.dumps(record, sort_keys=True, separators=(",", ":"))
                if full_payload
                else self._context_event_time_index_payload(record)
            )
            entries.append(
                {
                    "key": self._context_event_time_index_key(record),
                    "field": self._context_event_time_index_field(record),
                    "value": payload,
                    "storage_route": record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {},
                }
            )
        return entries

    def _records_can_use_direct_write_queue(self, records: list[Json]) -> bool:
        self._ensure_direct_write_queue_fields()
        if not bool(getattr(self, "_direct_write_queue_enabled", False)):
            return False
        if not records:
            return False
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

    def _direct_write_durable_field(self, payload: Json) -> str:
        digest = stable_hash(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return f"{int(payload.get('created_at_ms') or now_ms()):020d}:{digest}"

    def _enqueue_direct_write_durable(self, records: list[Json]) -> str:
        payload = self._direct_write_durable_payload(list(records))
        field = self._direct_write_durable_field(payload)
        self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(payload, separators=(",", ":")))
        return field

    def _enqueue_direct_write(self, records: list[Json]) -> None:
        self._ensure_direct_write_queue_fields()
        self._start_direct_write_worker()
        item: Any = list(records)
        if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
            item = {"queue_mode": "temporalstore", "field": self._enqueue_direct_write_durable(records)}
        wait_started_perf = time.perf_counter()
        try:
            self._direct_write_queue.put(item, timeout=self._direct_write_queue_put_timeout_s)
        except queue.Full as exc:
            self._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
            if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
                _mcp_debug_log("matrixark durable direct write queue accepted batch but local worker queue is full; batch will be recovered by drain")
                self._direct_write_enqueued_records += len(records)
                self._direct_write_enqueued_batches += 1
                return
            raise MatrixArkError("direct TemporalStore write queue is full") from exc
        self._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
        self._direct_write_enqueued_records += len(records)
        self._direct_write_enqueued_batches += 1

    def _direct_write_loop(self) -> None:
        while not self._direct_write_stop.is_set():
            try:
                item = self._direct_write_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                flushed = self._flush_direct_write_item(item)
                self._direct_write_flushed_records += flushed
                self._direct_write_flushed_batches += 1
            except Exception as exc:
                self._direct_write_failures += 1
                _mcp_debug_log(f"matrixark direct write queue flush failed: {exc}")
            finally:
                try:
                    self._direct_write_queue.task_done()
                except Exception:
                    pass

    def _flush_direct_write_item(self, item: Any) -> int:
        if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
            return self._flush_direct_write_durable_field(str(item.get("field") or ""))
        if isinstance(item, list):
            self._append_many_materialized(item, allow_queue=False)
            return len(item)
        raise MatrixArkError("unknown direct write queue item")

    def _load_direct_write_durable_payload(self, field: str) -> Json | None:
        if not field:
            return None
        raw = self._client.hget(self._direct_write_queue_key, field)
        if not raw:
            return None
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None

    def _write_direct_write_durable_status(self, field: str, payload: Json, status: str, error: str | None = None) -> None:
        updated = dict(payload)
        updated["status"] = status
        updated["updated_at_ms"] = now_ms()
        updated["attempts"] = int(updated.get("attempts") or 0) + (1 if status in {"running", "failed", "dead"} else 0)
        if error:
            updated["error"] = error
        key = self._direct_write_queue_done_key if status == "done" else self._direct_write_queue_dead_key if status == "dead" else self._direct_write_queue_key
        self._hset_with_backoff(key, field, json.dumps(updated, separators=(",", ":")))
        if key != self._direct_write_queue_key:
            self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(updated, separators=(",", ":")))

    def _flush_direct_write_durable_field(self, field: str) -> int:
        payload = self._load_direct_write_durable_payload(field)
        if not payload:
            return 0
        status = str(payload.get("status") or "pending")
        if status == "done":
            return 0
        if status == "dead":
            return 0
        records = payload.get("records")
        if not isinstance(records, list):
            self._write_direct_write_durable_status(field, payload, "dead", "durable queue payload has no records list")
            self._direct_write_dead_letter_batches += 1
            return 0
        self._write_direct_write_durable_status(field, payload, "running")
        try:
            self._append_many_materialized(records, allow_queue=False)
        except Exception as exc:
            refreshed = self._load_direct_write_durable_payload(field) or payload
            self._write_direct_write_durable_status(field, refreshed, "failed", str(exc))
            raise
        refreshed = self._load_direct_write_durable_payload(field) or payload
        self._write_direct_write_durable_status(field, refreshed, "done")
        return len(records)

    def drain_durable_direct_write_queue(self, *, limit: int | None = None) -> Json:
        self._ensure_direct_write_queue_fields()
        if getattr(self, "_direct_write_queue_mode", "memory") != "temporalstore":
            return {"status": "skipped", "reason": "queue_mode_not_temporalstore"}
        scanner = getattr(self._client, "scan_hash", None)
        if not callable(scanner):
            return {"status": "skipped", "reason": "backend_has_no_scan_hash"}
        response = scanner(self._direct_write_queue_key)
        records = response.get("records") if isinstance(response, dict) else []
        fields: list[str] = []
        for row in records if isinstance(records, list) else []:
            if not isinstance(row, dict):
                continue
            field = str(row.get("field") or "")
            value = row.get("value")
            if not field or not isinstance(value, str):
                continue
            try:
                payload = json.loads(value)
            except Exception:
                continue
            if isinstance(payload, dict) and str(payload.get("status") or "pending") in {"pending", "failed", "running"}:
                fields.append(field)
            if limit is not None and len(fields) >= limit:
                break
        self._start_direct_write_worker()
        for field in fields:
            self._direct_write_queue.put({"queue_mode": "temporalstore", "field": field}, timeout=self._direct_write_queue_put_timeout_s)
        return {"status": "queued", "pending_batches": len(fields), "queue_key": self._direct_write_queue_key}

    def _direct_write_durable_pending_count(self) -> int:
        self._ensure_direct_write_queue_fields()
        scanner = getattr(getattr(self, "_client", None), "scan_hash", None)
        if not callable(scanner):
            return 0
        try:
            response = scanner(self._direct_write_queue_key)
        except Exception:
            return 0
        rows = response.get("records") if isinstance(response, dict) else []
        count = 0
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            value = row.get("value")
            if not isinstance(value, str):
                continue
            try:
                payload = json.loads(value)
            except Exception:
                continue
            if isinstance(payload, dict) and str(payload.get("status") or "pending") in {"pending", "failed", "running"}:
                count += 1
        return count

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        self._ensure_direct_write_queue_fields()
        if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
            self.drain_durable_direct_write_queue()
        deadline = time.monotonic() + float(timeout_s if timeout_s is not None else 30.0)
        while self._direct_write_queue.unfinished_tasks:
            if time.monotonic() >= deadline:
                raise MatrixArkError("timed out waiting for direct TemporalStore write queue to drain")
            time.sleep(0.01)

    def _append_many_materialized(self, records: list[Json], *, allow_queue: bool = True) -> None:
        if not records:
            return
        self._validate_storage_routes_available(records)
        if self._queue_batched_records(records):
            return
        if allow_queue and self._records_can_use_direct_write_queue(records):
            self._enqueue_direct_write(records)
            return
        started_perf = time.perf_counter()
        with self._records_lock:
            entry_count_cache = getattr(self, "_entry_count_cache", None)
            count = entry_count_cache if entry_count_cache is not None else self._get_count()
            if count <= 0 and self._index_cache is None:
                self._index_cache = self._get_index()
                self._legacy_index_mode = bool(self._index_cache)
            event_time_entries = self._context_event_time_index_entries(records)
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
                self._hset_many_with_backoff(event_time_entries + entries)
                self._put_string_with_backoff(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
                if self._records_cache is not None:
                    self._records_cache.extend(records)
                    self._put_direct_record_cache(len(self._records_cache), self._records_cache)
                self._update_latest_entity_cache(records)
                elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                self._observe_append_engine(elapsed_ms)
                self._observe_backend_command(elapsed_ms, records_written=len(records))
                return

            sequence = count
            entries = []
            located_bundles: list[tuple[list[Json], str, str]] = []
            for bundle in self._record_bundles(records):
                record_key, record_id = self._record_location(sequence)
                payload_value: Json
                payload_value = bundle[0] if len(bundle) == 1 else {"record_bundle": bundle}
                payload = json.dumps(payload_value, sort_keys=True, separators=(",", ":"))
                entries.append({"key": record_key, "field": record_id, "value": payload, "storage_route": self._storage_route_for_bundle(bundle)})
                located_bundles.append((bundle, record_key, record_id))
                sequence += 1
            native_index_entries = self._native_side_index_entries_for_bundles(located_bundles)
            append_records = getattr(self._client, "matrixark_batch_append_records", None)
            if callable(append_records):
                self._write_with_backoff(
                    lambda: append_records(
                        event_time_entries + native_index_entries + entries,
                        count_key=self._count_key,
                        count_value=str(sequence),
                        append_options=self._native_append_options(),
                    ),
                    op="matrixark_batch_append_records",
                )
                if self._write_throttle_s > 0:
                    time.sleep(self._write_throttle_s)
            else:
                self._hset_many_with_backoff(event_time_entries + native_index_entries + entries)
                self._put_string_with_backoff(self._count_key, str(sequence))
            self._entry_count_cache = sequence
            if self._records_cache is not None:
                self._records_cache.extend(records)
                self._put_direct_record_cache(self._entry_count_cache, self._records_cache)
            self._prune_retrieval_candidate_cache(sequence)
            self._update_latest_entity_cache(records)
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            self._observe_append_engine(elapsed_ms)
            self._observe_backend_command(elapsed_ms, records_written=len(records))

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

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
    ) -> Json:
        """Return retrieval candidates with native scan/cache prefiltering.

        C++ direct and Rust proxy/direct SDK should expose a native hash/prefix
        scan so MatrixArk can fetch appended record shards in one storage call
        per shard. The adapter keeps a small hot cache only as a read-through
        fallback; correctness should come from TemporalStore records, not from
        Python-owned cache state. Python still owns reference scoring and
        ContextPack assembly until the native score/pack API lands.
        """

        allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
        native_candidates = self._native_candidate_scan(
            scope=scope,
            record_types=allowed_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )
        if native_candidates is not None:
            return native_candidates

        raw_records = self.read_all()
        filtered: list[Json] = []
        scoped_records: list[Json] = []
        scanned = 0
        dropped_type = 0
        dropped_scope = 0
        for record in raw_records:
            scanned += 1
            record_type = str(record.get("record_type") or "")
            if record_type not in allowed_types:
                dropped_type += 1
                continue
            if record_type in {"context_embedding", "context_index", "context_summary", "resource_manifest", "skill_registry_update"}:
                in_scope = scope_matches(candidate_access_scope(record), scope)
            else:
                in_scope = access_scope_matches_before_scoring(record, scope)
            if not in_scope:
                dropped_scope += 1
                continue
            scoped_records.append(record)

        secondary_index_dropped = 0
        secondary_index_matched = 0
        matched_node_hashes: set[int] = set()
        if secondary_index_groups:
            index_terms_by_batch: dict[Any, list[str]] = {}
            index_terms_by_node: dict[Any, list[str]] = {}
            index_terms_by_ref: dict[Any, list[str]] = {}
            index_terms_by_node_for_prefilter: dict[int, list[str]] = {}
            for record in scoped_records:
                if record.get("record_type") != "context_index":
                    continue
                index_name = str(record.get("index_name") or "")
                if not index_name:
                    continue
                index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                if ref_hash is not None:
                    index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                else:
                    index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
                try:
                    index_terms_by_node_for_prefilter.setdefault(int(record.get("node_hash")), []).append(index_name)
                except (TypeError, ValueError):
                    pass
            matched_node_hashes = {
                node_hash
                for node_hash, terms in index_terms_by_node_for_prefilter.items()
                if passes_secondary_index_filters(set(terms), secondary_index_groups, mode="any_group" if len(secondary_index_groups) > 1 else "all_groups")
            }
            filter_mode = "any_group" if len(secondary_index_groups) > 1 else "all_groups"
            for record in scoped_records:
                terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
                node_hash = record.get("node_hash")
                try:
                    node_matches = int(node_hash) in matched_node_hashes
                except (TypeError, ValueError):
                    node_matches = False
                if terms and not passes_applicable_secondary_index_filters(terms, secondary_index_groups, mode=filter_mode):
                    secondary_index_dropped += 1
                    continue
                if terms or node_matches:
                    secondary_index_matched += 1
                filtered.append(record)
        else:
            filtered = scoped_records

        if selected_node_hashes:
            narrowed: list[Json] = []
            selected = {int(item) for item in selected_node_hashes}
            for record in filtered:
                try:
                    node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    narrowed.append(record)
                    continue
                if node_hash in selected or record.get("record_type") in {"context_index", "context_embedding"}:
                    narrowed.append(record)
            filtered = narrowed

        native_prefix_scan = bool(getattr(self, "_last_read_all_native_shard_scan", False))
        return {
            "records": filtered,
            "scan_stats": {
                "backend": self._backend_label(),
                "execution_mode": "native_temporalstore_shard_scan_prefilter" if native_prefix_scan else "direct_backend_hot_cache_prefilter",
                "backend_pushdown": True,
                "direct_backend_prefilter": True,
                "native_pushdown": native_prefix_scan,
                "native_prefix_scan": native_prefix_scan,
                "native_pack_assembly": False,
                "cache_hit": self._records_cache is not None,
                "record_types": sorted(allowed_types),
                "scanned_records": scanned,
                "returned_records": len(filtered),
                "dropped_by_type": dropped_type,
                "dropped_by_scope": dropped_scope,
                "secondary_index_groups_supplied": len(secondary_index_groups or []),
                "secondary_index_matched_candidate_count": secondary_index_matched,
                "secondary_index_dropped_candidate_count": secondary_index_dropped,
                "secondary_index_matched_node_count": len(matched_node_hashes),
                "selected_node_hashes_supplied": len(selected_node_hashes or set()),
                "pack_assembly_location": "python_reference_packer",
                "next_native_gap": "C++/Rust ContextPack assembly and scoring APIs",
            },
        }


    def supports_native_context_pack(self) -> bool:
        return callable(getattr(getattr(self, "_client", None), "matrixark_retrieve_context_pack", None))

    def native_context_pack(self, request: Json) -> Json | None:
        retriever = getattr(getattr(self, "_client", None), "matrixark_retrieve_context_pack", None)
        if not callable(retriever):
            return None
        try:
            response = retriever(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                request=request,
            )
        except Exception:
            return None
        if not isinstance(response, dict) or not response.get("native_pack_assembly"):
            return None
        pack = response.get("context_pack")
        if not isinstance(pack, dict):
            return None
        pack.setdefault("context_pack_assembly", "native_backend")
        pack.setdefault("backend", self._backend_label())
        return pack

    def _native_candidate_scan(
        self,
        *,
        scope: Json,
        record_types: set[str],
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> Json | None:
        scanner = getattr(getattr(self, "_client", None), "matrixark_scan_candidates", None)
        if not callable(scanner):
            return None
        try:
            response = scanner(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=scope,
                record_types=sorted(record_types),
                secondary_index_groups=[sorted(group) for group in (secondary_index_groups or [])],
                selected_node_hashes=sorted(int(item) for item in (selected_node_hashes or set())),
            )
        except Exception:
            return None
        records = response.get("records") if isinstance(response, dict) else None
        if not isinstance(records, list):
            return None
        scan_stats = dict(response.get("scan_stats") or {})
        scan_stats.setdefault("backend", self._backend_label())
        scan_stats.setdefault("execution_mode", "native_temporalstore_candidate_prefilter")
        scan_stats.setdefault("backend_pushdown", True)
        scan_stats.setdefault("direct_backend_prefilter", True)
        scan_stats.setdefault("native_pushdown", True)
        scan_stats.setdefault("native_prefix_scan", True)
        scan_stats.setdefault("native_secondary_index_prefilter", bool(secondary_index_groups))
        scan_stats.setdefault("native_pack_assembly", False)
        scan_stats.setdefault("cache_hit", False)
        scan_stats.setdefault("record_types", sorted(record_types))
        scan_stats.setdefault("selected_node_hashes_supplied", len(selected_node_hashes or set()))
        scan_stats.setdefault("pack_assembly_location", "python_reference_packer")
        return {"records": records, "scan_stats": scan_stats}

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

    def _retrieval_candidate_cache_key(
        self,
        *,
        count: int,
        scope: Json,
        record_types: set[str] | None,
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> str:
        return json.dumps(
            {
                "count": count,
                "storage_prefix": self._storage_prefix,
                "scope": scope or {},
                "record_types": sorted(record_types or RETRIEVAL_HOT_RECORD_TYPES),
                "secondary_index_groups": [
                    sorted(group)
                    for group in (secondary_index_groups or [])
                ],
                "selected_node_hashes": sorted(selected_node_hashes or []),
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def _prune_retrieval_candidate_cache(self, current_count: int) -> None:
        with _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK:
            stale_keys = [
                key
                for key, cached in _DIRECT_RETRIEVAL_CANDIDATE_CACHE.items()
                if cached.get("storage_prefix") == self._storage_prefix
                and int(cached.get("count") or -1) != int(current_count)
            ]
            for key in stale_keys:
                _DIRECT_RETRIEVAL_CANDIDATE_CACHE.pop(key, None)
            if len(_DIRECT_RETRIEVAL_CANDIDATE_CACHE) > _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES:
                overflow = len(_DIRECT_RETRIEVAL_CANDIDATE_CACHE) - _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES
                for key in list(_DIRECT_RETRIEVAL_CANDIDATE_CACHE)[:overflow]:
                    _DIRECT_RETRIEVAL_CANDIDATE_CACHE.pop(key, None)
        with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
            stale_keys = [
                key
                for key in _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE
                if key.startswith(f"{self._storage_prefix}|")
                and f"|wm={int(current_count)}|" not in key
            ]
            for key in stale_keys:
                _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)
            if len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) > _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES:
                overflow = len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) - _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES
                for key in list(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)[:overflow]:
                    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)

    def _placement_candidate_table_cache_key(
        self,
        *,
        count: int,
        scope_key: str,
        node_hash: int,
        record_type: str,
        resource_version_watermark: str = "",
    ) -> str:
        return (
            f"{self._storage_prefix}|wm={int(count)}|scope={stable_hash(scope_key)}|"
            f"node={int(node_hash)}|type={record_type}|rv={stable_hash(resource_version_watermark)}"
        )

    def _record_primary_hash(self, record: Json) -> int:
        for field in (
            "event_id_hash",
            "entity_hash",
            "segment_hash",
            "compression_id_hash",
            "summary_hash",
            "chunk_hash",
            "section_hash",
            "skill_hash",
            "resource_hash",
            "batch_id_hash",
            "ref_hash",
        ):
            value = record.get(field)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    break
        return stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":")))

    def _placement_candidate_records_from_cache_or_load(
        self,
        *,
        count: int,
        scope: Json,
        allowed_types: set[str],
        selected_nodes: set[int],
        locations: list[Json],
        resource_version_watermark: str = "",
    ) -> Json:
        scope_key = canonical_scope_key(scope)
        if not scope_key or not selected_nodes or not allowed_types:
            return {"records": [], "cache_hit": False, "cache_entries": 0, "loaded_records": 0}

        keys = [
            self._placement_candidate_table_cache_key(
                count=count,
                scope_key=scope_key,
                node_hash=node_hash,
                record_type=record_type,
                resource_version_watermark=resource_version_watermark,
            )
            for node_hash in sorted(selected_nodes)
            for record_type in sorted(allowed_types)
        ]
        with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
            cached_tables = [_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.get(key) for key in keys]
            if keys and all(table is not None for table in cached_tables):
                compact_rows = [
                    row
                    for table in cached_tables
                    for row in (table or [])
                ]
                return {
                    "records": [dict(row[3]) for row in compact_rows],
                    "cache_hit": True,
                    "cache_entries": len(compact_rows),
                    "loaded_records": 0,
                    "resource_version_watermark": resource_version_watermark,
                }

        loaded_records = self._load_records_from_locations(locations)
        grouped: dict[str, list[tuple[str, int, int, Json]]] = {key: [] for key in keys}
        for record in loaded_records:
            record_type = str(record.get("record_type") or "")
            if record_type not in allowed_types:
                continue
            try:
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if node_hash not in selected_nodes:
                continue
            key = self._placement_candidate_table_cache_key(
                count=count,
                scope_key=scope_key,
                node_hash=node_hash,
                record_type=record_type,
                resource_version_watermark=resource_version_watermark,
            )
            if key not in grouped:
                continue
            grouped[key].append((record_type, self._record_primary_hash(record), node_hash, dict(record)))

        with _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK:
            for key, compact_rows in grouped.items():
                _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE[key] = compact_rows
            if len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) > _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES:
                overflow = len(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE) - _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES
                for key in list(_DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE)[:overflow]:
                    _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.pop(key, None)

        compact_rows = [row for table in grouped.values() for row in table]
        return {
            "records": [dict(row[3]) for row in compact_rows],
            "cache_hit": False,
            "cache_entries": len(compact_rows),
            "loaded_records": len(loaded_records),
            "resource_version_watermark": resource_version_watermark,
        }

    def _native_index_ref_hashes(self, *, scope: Json, secondary_index_groups: list[set[str]] | None) -> Json:
        scope_key = canonical_scope_key(scope)
        groups = secondary_index_groups or []
        if not scope_key or not groups:
            return {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "missing_scope_or_filters"}
        batch_hget = getattr(self._client, "batch_hget", None)
        if not callable(batch_hget):
            return {"ref_hashes": set(), "postings_found": 0, "index_terms": [], "posting_buckets": [], "eligible": False, "reason": "backend_has_no_batch_hget"}
        index_terms = sorted({term for group in groups for term in group if term})
        entries = [{"key": self._context_index_lookup_key(scope_key), "field": term} for term in index_terms]
        try:
            rows = batch_hget(entries)
        except Exception as exc:
            return {"ref_hashes": set(), "postings_found": 0, "index_terms": index_terms, "posting_buckets": [], "eligible": False, "reason": f"index_lookup_failed:{exc}"}
        ref_hashes: set[int] = set()
        posting_buckets: set[int] = set()
        postings_found = 0
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
            raw_refs = decoded.get("ref_hashes", []) if isinstance(decoded, dict) else []
            raw_buckets = decoded.get("posting_buckets", []) if isinstance(decoded, dict) else []
            if isinstance(raw_refs, list):
                postings_found += 1
                for value in raw_refs:
                    try:
                        ref_hash = int(value)
                    except (TypeError, ValueError):
                        continue
                    if ref_hash:
                        ref_hashes.add(ref_hash)
            if isinstance(raw_buckets, list):
                for value in raw_buckets:
                    try:
                        bucket = int(value)
                    except (TypeError, ValueError):
                        continue
                    if bucket:
                        posting_buckets.add(bucket)
        return {
            "ref_hashes": ref_hashes,
            "postings_found": postings_found,
            "index_terms": index_terms,
            "posting_buckets": sorted(posting_buckets),
            "eligible": bool(ref_hashes),
            "reason": "ok" if ref_hashes else "no_matching_postings",
        }

    def _native_locations_for_refs(self, ref_hashes: set[int]) -> Json:
        batch_hget = getattr(self._client, "batch_hget", None)
        if not callable(batch_hget) or not ref_hashes:
            return {"locations": [], "locator_rows": 0}
        entries = [{"key": self._context_ref_locator_key(), "field": str(ref_hash)} for ref_hash in sorted(ref_hashes)]
        try:
            rows = batch_hget(entries)
        except Exception:
            return {"locations": [], "locator_rows": 0}
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
        return {"locations": locations, "locator_rows": locator_rows}

    def _load_records_from_locations(self, locations: list[Json]) -> list[Json]:
        batch_hget = getattr(self._client, "batch_hget", None)
        if not callable(batch_hget) or not locations:
            return []
        try:
            rows = batch_hget(locations)
        except Exception:
            return []
        records: list[Json] = []
        for item in rows if isinstance(rows, list) else []:
            if not isinstance(item, dict):
                continue
            payload = item.get("value", "")
            if not payload:
                continue
            try:
                decoded = json.loads(str(payload))
            except Exception:
                continue
            if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                records.extend(row for row in decoded["record_bundle"] if isinstance(row, dict))
            elif isinstance(decoded, dict):
                records.append(decoded)
        return records

    def _native_context_pack_fallback_blocker(self, args: Json, *, reason: str) -> Json:
        scope = optional_object(args, "scope")
        query = str(args.get("query") or "")
        context_pack_id = str(stable_hash(f"native-blocked:{query}:{canonical_scope_key(scope)}:{now_ms()}"))
        pack: Json = {
            "context_pack_id": context_pack_id,
            "status": "timeout_partial",
            "native_context_pack": False,
            "context_pack_assembly": "native_context_pack_blocked",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "remote_context_refs": [],
            "groups": [],
            "quality_warnings": [
                {
                    "code": "native_backend_contract_blocked",
                    "message": "Native matrixark_retrieve_context_pack was available but did not return a valid compact ContextPack; Python broad scan and hot-path pack fallback are disabled for production retrieval.",
                    "reason": reason,
                }
            ],
            "retrieval_metrics": {
                "backend": self._backend_label(),
                "native_api": "matrixark_retrieve_context_pack",
                "native_pack_assembly": False,
                "python_pack_fallback": False,
                "raw_candidate_tables_returned": False,
                "broad_scan_used": False,
                "broad_scan_blocked": True,
                "broad_scan_policy": "explicit_fallback_or_debug_only",
                "fallback_reason": reason,
                "selected_refs": 0,
                "dropped_refs": 0,
                "scanned_records": 0,
                "index_postings_read": 0,
                "placement_partitions_touched": 0,
                "candidate_cache_hit": False,
                "normal_path_stages": [
                    "query_understanding",
                    "scope_filter",
                    "l0_l1_node_traversal",
                    "compact_secondary_index_prefilter",
                    "placement_key_candidate_fetch",
                    "native_score_rerank_pack",
                ],
                "health_readiness_metrics": {
                    "health": True,
                    "readiness": True,
                    "metrics": True,
                },
            },
            "recall_policy": {
                "backend_retrieval_pushdown": {
                    "backend": self._backend_label(),
                    "execution_mode": "native_context_pack_blocked",
                    "python_materialized_records": 0,
                    "broad_scan_blocked": True,
                    "fallback_reason": reason,
                }
            },
        }
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        return pack

    def _try_native_context_pack(self, args: Json) -> Json | None:
        if os.environ.get("MATRIXARK_DISABLE_NATIVE_CONTEXT_PACK", "").strip().lower() in {"1", "true", "yes"}:
            return None
        native_retrieve = getattr(self._client, "matrixark_retrieve_context_pack", None)
        if not callable(native_retrieve):
            return None
        scope = _native_scope_with_hashes(optional_object(args, "scope"))
        query = require_string(args, "query")
        ranking = optional_object(args, "ranking")
        scope_key = canonical_scope_key(scope)
        native_node_path = optional_object(args, "metadata").get("node_path")
        if not isinstance(native_node_path, list) or not native_node_path:
            native_node_path = self.default_session_node_path(scope)
        native_start_node_hash = stable_hash("/".join(str(part) for part in native_node_path))
        reference_time_ms = int(args.get("reference_time_ms", now_ms()) or now_ms())
        local_context = args.get("local_context", [])
        if not isinstance(local_context, list):
            local_context = []
        watermark_count = self._entry_count_cache if self._entry_count_cache is not None else self._get_count()
        resource_version_watermark = str(
            ranking.get("resource_version_watermark")
            or args.get("resource_version_watermark")
            or ""
        )
        skill_status_watermark = str(
            ranking.get("skill_status_watermark")
            or args.get("skill_status_watermark")
            or ""
        )
        request: Json = {
            "api_version": 1,
            "storage_prefix": self._storage_prefix,
            "backend": self._backend_label(),
            "watermark_count": watermark_count,
            "append_watermark": watermark_count,
            "resource_version_watermark": resource_version_watermark,
            "skill_status_watermark": skill_status_watermark,
            "index_posting_watermark": watermark_count,
            "query": query,
            "scope": scope,
            "scope_key": scope_key,
            "tenant_hash": int(scope.get("tenant_hash") or 0),
            "scope_hash": stable_hash(scope_key) if scope_key else 0,
            "start_node_hash": native_start_node_hash,
            "placement_node_hash": native_start_node_hash,
            "placement_key": f"context:{scope_key}:node={native_start_node_hash}",
            "native_start_node_path": [str(part) for part in native_node_path],
            "start_time_ms": 1,
            "end_time_ms": reference_time_ms,
            "as_of_ms": reference_time_ms,
            "max_selected_refs": int(ranking.get("max_selected_refs") or args.get("max_selected_refs") or 24),
            "min_score": float(ranking.get("min_score") or args.get("min_score") or 0.0),
            "decay_half_life_ms": int(ranking.get("half_life_ms") or 0),
            "max_depth": int(ranking.get("max_depth") or 4),
            "top_k_per_depth": int(ranking.get("top_k_per_layer") or ranking.get("top_k_per_depth") or 16),
            "max_children_scored_per_parent": int(ranking.get("max_children_scored_per_parent") or 256),
            "max_candidate_nodes": int(ranking.get("max_candidate_nodes") or 64),
            "shared_resource_max_refs": int(ranking.get("shared_resource_max_refs") or args.get("shared_resource_max_refs") or 4),
            "skill_max_refs": int(ranking.get("skill_max_refs") or args.get("skill_max_refs") or 4),
            "cross_session_max_refs": int(ranking.get("cross_session_max_refs") or args.get("cross_session_max_refs") or 4),
            "cross_session_rerank": bool(ranking.get("cross_session_rerank", True)),
            "same_session_priority": bool(ranking.get("same_session_priority", True)),
            "leaf_only": bool(ranking.get("leaf_only", False)),
            "allow_broad_scan_fallback": bool(native_retrieve_fallback_allowed(args)),
            "ranking": ranking,
            "storage_options": optional_object(args, "storage_options"),
            "max_context_tokens": int(args.get("max_context_tokens") or 2048),
            "local_context": local_context,
            "local_context_tokens": int(args.get("local_context_tokens") or 0),
            "local_context_safety_margin_tokens": args.get("local_context_safety_margin_tokens"),
            "reference_time_ms": reference_time_ms,
            "include_superseded": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "debug_context_pack": bool(args.get("debug_context_pack") or args.get("include_retrieval_debug")),
            "include_retrieval_metrics": bool(args.get("include_retrieval_metrics")),
            "required_native_apis": [
                "health",
                "readiness",
                "metrics",
                "matrixark_batch_append_records",
                "matrixark_retrieve_context_pack",
                "compact_secondary_index_lookup",
                "placement_key_candidate_fetch",
            ],
            "normal_path_stages": [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
            "normalization_requirements": {
                "scope_key": "canonical",
                "node_hash": "integer",
                "placement_key": "context:{scope_key}:node={node_hash}",
                "resource_visibility": "apply_scope_before_scoring",
                "skill_visibility": "apply_scope_before_scoring",
                "shared_resource_scope": "tenant_or_global_visible_before_scoring",
                "stale_superseded_state": "exclude_unless_include_superseded_resources",
            },
            "execution_plan_requirements": {
                "phase": "phase4_native_score_rerank_pack",
                "context_record_route": "context:{scope_key}:node={node_hash}",
                "traversal": "score_l0_l1_then_fetch_selected_node_partitions",
                "candidate_fetch": "selected_node_placement_partitions_only",
                "candidate_cache": "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
                "candidate_cache_payload": "compact_structs_not_json_strings",
                "secondary_index": "compact_postings_by_scope_index_time_bucket",
                "scoring": "native_embedding_similarity_temporal_decay_business_boost_same_session_boost",
                "quotas": "native_shared_resource_quota_cross_session_quota_current_session_priority",
                "rerank": "native_score_fusion_then_budget_aware_rerank",
                "token_budget_pack": "native_budget_pack_with_selected_refs_and_dropped_summary",
                "pack_assembly": "native_score_rank_budget_pack_selected_refs_dropped_summary",
                "python_role": "dispatcher_only_no_candidate_materialization_no_hot_path_pack",
                "write_path": "native_batch_append_records_append_queue_coalesced_persistence",
                "write_route": "placement_key_partition_route_before_persistence",
                "write_coalescing": "native_append_queue_coalesces_by_record_key_field",
                "durability": "storage_options_select_async_sync_shared_store_or_raft",
                "retrieval_hot_path_audit": "inline_counters_only_no_full_audit_blocking",
                "context_pack_audit": "sample_or_enqueue_async_policy_enabled",
                "full_replay_audit_default": "disabled",
                "broad_prefix_scan": "disabled_unless_explicit_debug_fallback",
                "fallback_telemetry_required": True,
                "health_readiness_metrics": "native_backend_must_expose_health_readiness_metrics",
                "normal_path": "query_understanding_scope_filter_l0_l1_traversal_compact_index_placement_fetch_native_score_rerank_pack",
            },
            "required_output": {
                "context_pack": True,
                "selected_refs": True,
                "dropped_summary": True,
                "drop_counters": [
                    "scope",
                    "placement",
                    "index_filter",
                    "stale",
                    "token_budget",
                    "score_threshold",
                ],
                "telemetry": True,
                "retrieval_metrics": bool(args.get("include_retrieval_metrics")),
                "placement_partitions_touched": True,
                "index_postings_read": True,
                "candidate_cache_hit": True,
                "candidate_cache_key_shape": True,
                "native_pack_assembly": True,
                "raw_candidate_tables": False,
                "python_pack_fallback": False,
                "broad_scan_used": True,
                "normal_path_stages": True,
                "health_readiness_metrics": True,
            },
        }
        started_perf = time.perf_counter()
        try:
            response = native_retrieve(request)
        except TypeError:
            response = native_retrieve(json.dumps(request, sort_keys=True, separators=(",", ":")))
        except Exception as exc:
            _mcp_debug_log(f"matrixark native context pack failed: {exc}")
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_error:{exc}")
            return None
        try:
            pack = json.loads(response) if isinstance(response, str) else response
        except Exception as exc:
            _mcp_debug_log(f"matrixark native context pack returned invalid JSON: {exc}")
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_invalid_json:{exc}")
            return None
        if not isinstance(pack, dict):
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_not_object")
            return None
        selected_refs = pack.get("selected_refs", [])
        groups = pack.get("groups", [])
        if not isinstance(selected_refs, list) and not isinstance(groups, (list, dict)):
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_missing_refs_or_groups")
            return None
        raw_candidate_tables = (
            pack.get("candidate_records")
            or pack.get("raw_candidate_records")
            or pack.get("candidate_tables")
            or pack.get("raw_candidate_tables")
        )
        if raw_candidate_tables:
            _mcp_debug_log("matrixark native context pack returned raw candidate tables")
            if not native_retrieve_fallback_allowed(args):
                blocker = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_returned_raw_candidate_tables")
                blocker["retrieval_metrics"]["raw_candidate_tables_returned"] = True
                return blocker
            return None
        pack.setdefault("context_pack_id", str(stable_hash(f"native:{query}:{canonical_scope_key(scope)}:{now_ms()}")))
        pack.setdefault("context_pack_assembly", "native_cpp_direct")
        pack.setdefault("native_context_pack", True)
        pack.setdefault("query_embedding_model", embedding_model_name())
        pack.setdefault("embedding_execution_mode", embedding_execution_mode_name())
        pack.setdefault("embedding_fallback_used", embedding_fallback_used())
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        if selected_refs and "remote_context_refs" not in pack:
            pack["remote_context_refs"] = selected_refs
        if "recall_policy" not in pack:
            pack["recall_policy"] = {}
        if isinstance(pack["recall_policy"], dict):
            native_telemetry = pack.get("retrieval_metrics") if isinstance(pack.get("retrieval_metrics"), dict) else {}
            native_stage_metrics = native_telemetry.get("stages") if isinstance(native_telemetry.get("stages"), dict) else {}
            total_native_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
            selected_count = len(selected_refs) if isinstance(selected_refs, list) else 0
            pack_ms = float(native_telemetry.get("pack_ms") or native_stage_metrics.get("pack_ms") or 0.0)
            if not pack_ms:
                pack_ms = total_native_ms
            index_postings_read = int(
                native_telemetry.get("index_postings_read")
                or native_telemetry.get("index_postings_touched")
                or native_telemetry.get("native_index_postings_found")
                or 0
            )
            candidate_cache_hit = bool(
                native_telemetry.get("candidate_cache_hit", native_telemetry.get("cache_hit", False))
            )
            native_fallback_flags = native_telemetry.get("fallback_flags")
            if isinstance(native_fallback_flags, str):
                fallback_flags = [native_fallback_flags]
            elif isinstance(native_fallback_flags, list):
                fallback_flags = [str(flag) for flag in native_fallback_flags if str(flag)]
            else:
                fallback_flags = []
            retrieval_metrics = {
                "query_plan_ms": round(float(native_telemetry.get("query_plan_ms") or native_stage_metrics.get("query_plan_ms") or 0.0), 3),
                "node_traversal_ms": round(float(native_telemetry.get("node_traversal_ms") or native_stage_metrics.get("node_traversal_ms") or 0.0), 3),
                "index_prefilter_ms": round(float(native_telemetry.get("index_prefilter_ms") or native_stage_metrics.get("index_prefilter_ms") or 0.0), 3),
                "candidate_fetch_ms": round(float(native_telemetry.get("candidate_fetch_ms") or native_stage_metrics.get("candidate_fetch_ms") or 0.0), 3),
                "score_ms": round(float(native_telemetry.get("score_ms") or native_stage_metrics.get("score_ms") or 0.0), 3),
                "pack_ms": round(pack_ms, 3),
                "audit_ms": round(float(native_telemetry.get("audit_ms") or native_stage_metrics.get("audit_ms") or 0.0), 3),
                "append_queue_wait_ms": round(float(native_telemetry.get("append_queue_wait_ms") or self._append_queue_wait_ms_avg()), 3),
                "append_engine_ms": round(float(native_telemetry.get("append_engine_ms") or self._append_engine_ms_avg()), 3),
                "selected_refs": int(native_telemetry.get("selected_refs") or selected_count),
                "dropped_refs": int(native_telemetry.get("dropped_refs") or native_telemetry.get("dropped_ref_count") or 0),
                "scanned_records": int(native_telemetry.get("scanned_records") or 0),
                "candidate_cache_hit": candidate_cache_hit,
                "cache_hit": candidate_cache_hit,
                "placement_partitions_touched": int(native_telemetry.get("placement_partitions_touched") or 0),
                "placement_fetch_count": int(native_telemetry.get("placement_fetch_count") or 0),
                "index_postings_read": index_postings_read,
                "index_postings_touched": index_postings_read,
                "compact_index_bucket_used": bool(native_telemetry.get("compact_index_bucket_used", False)),
                "compact_index_bucket_count": int(native_telemetry.get("compact_index_bucket_count") or 0),
                "candidate_cache_key_shape": str(native_telemetry.get("candidate_cache_key_shape") or "scope_key+node_hash+record_type+append_watermark+resource_version_watermark"),
                "native_pack_assembly": True,
                "python_pack_fallback": False,
                "raw_candidate_tables_returned": False,
                "broad_scan_used": bool(native_telemetry.get("broad_scan_used", False)),
                "broad_scan_blocked": bool(native_telemetry.get("broad_scan_blocked", False)),
                "broad_scan_fallback_allowed": bool(native_telemetry.get("broad_scan_fallback_allowed", False)),
                "timeout_count": int(native_telemetry.get("timeout_count") or 0),
                "fallback_flags": fallback_flags,
                "broad_scan_policy": "explicit_fallback_or_debug_only",
                "fallback_reason": str(native_telemetry.get("fallback_reason") or ""),
                "normal_path_stages": list(request["normal_path_stages"]),
                "health_readiness_metrics": {
                    "health": True,
                    "readiness": True,
                    "metrics": True,
                },
                "native_context_pack_ms": total_native_ms,
                "source": "native_context_pack",
            }
            native_correctness = (
                native_telemetry.get("correctness_evidence")
                if isinstance(native_telemetry.get("correctness_evidence"), dict)
                else {}
            )
            if native_correctness:
                retrieval_metrics["correctness_evidence"] = {
                    "scope_filtering": bool(native_correctness.get("scope_filtering")),
                    "placement_filtering": bool(native_correctness.get("placement_filtering")),
                    "compact_secondary_index_prefilter": bool(
                        native_correctness.get("compact_secondary_index_prefilter")
                    ),
                    "stale_superseded_exclusion": bool(
                        native_correctness.get("stale_superseded_exclusion")
                    ),
                    "shared_resource_skill_quota": bool(
                        native_correctness.get("shared_resource_skill_quota")
                    ),
                    "cross_session_quota_rerank": bool(
                        native_correctness.get("cross_session_quota_rerank")
                    ),
                }
            native_drop_counters = native_telemetry.get("drop_counters") if isinstance(native_telemetry.get("drop_counters"), dict) else {}
            if not native_drop_counters:
                native_drop_counters = pack.get("drop_counters") if isinstance(pack.get("drop_counters"), dict) else {}
            if not native_drop_counters and isinstance(pack.get("dropped_refs"), dict):
                dropped = pack.get("dropped_refs", {})
                native_drop_counters = {
                    "scope": int(dropped.get("scope", 0) or dropped.get("access_denied", 0) or 0),
                    "placement": int(dropped.get("placement", 0) or dropped.get("placement_filter", 0) or 0),
                    "index_filter": int(dropped.get("index_filter", 0) or dropped.get("secondary_index_filter", 0) or 0),
                    "stale": int(dropped.get("stale", 0) or dropped.get("superseded", 0) or 0),
                    "token_budget": int(dropped.get("over_budget", 0) or dropped.get("max_selected_refs", 0) or 0),
                    "score_threshold": int(dropped.get("low_score", 0) or dropped.get("score_threshold", 0) or 0),
                }
            if native_drop_counters:
                retrieval_metrics["drop_counters"] = native_drop_counters
                if not int(retrieval_metrics.get("dropped_refs") or 0):
                    dropped_total = 0
                    for value in native_drop_counters.values():
                        try:
                            dropped_total += int(value or 0)
                        except (TypeError, ValueError):
                            continue
                    retrieval_metrics["dropped_refs"] = dropped_total
            pack["retrieval_metrics"] = retrieval_metrics
            pack["recall_policy"].setdefault(
                "backend_retrieval_pushdown",
                {
                    "backend": self._backend_label(),
                    "execution_mode": "native_context_pack",
                    "native_pack_assembly": True,
                    "watermark_count": request["watermark_count"],
                    "python_materialized_records": 0,
                },
            )
            pack["recall_policy"].setdefault(
                "stage_latency_budgets",
                {
                    "native_context_pack_ms": total_native_ms,
                    "metrics": retrieval_metrics,
                },
            )
        dropped_refs = pack.get("dropped_refs")
        if isinstance(dropped_refs, list):
            pack["dropped_refs"] = {"refs": dropped_refs, "native_summary": True}
        elif not isinstance(dropped_refs, dict):
            pack["dropped_refs"] = {"refs": [], "native_summary": True}
        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
            return pack
        if isinstance(selected_refs, list) and selected_refs:
            return compact_context_pack_for_serving(pack)
        return pack

    def retrieve(self, args: Json) -> Json:
        native_pack = self._try_native_context_pack(args)
        if native_pack is not None:
            return native_pack
        return super().retrieve(args)

    def _native_locations_for_selected_nodes(self, *, scope: Json, selected_node_hashes: set[int]) -> Json:
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

    def _filter_retrieval_candidates(
        self,
        records: list[Json],
        *,
        scope: Json,
        allowed_types: set[str],
        selected_nodes: set[int],
    ) -> tuple[list[Json], Json]:
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
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
        allow_broad_scan_fallback: bool | None = None,
    ) -> Json:
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

    def _load_records_by_count(self, count: int) -> list[Json]:
        records = []
        self._last_read_all_native_shard_scan = False
        scan_records = self._load_records_by_native_shard_scan(count)
        if scan_records is not None:
            self._last_read_all_native_shard_scan = True
            return scan_records
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

    def _load_records_by_native_shard_scan(self, count: int) -> list[Json] | None:
        scanner = getattr(getattr(self, "_client", None), "scan_hash", None)
        if not callable(scanner) or count <= 0:
            return None
        max_shard = (count - 1) // self._shard_size
        records_by_sequence: list[tuple[int, Json]] = []
        for shard in range(max_shard + 1):
            key = f"{self._record_hash_key}:{shard:06d}"
            try:
                response = scanner(key)
            except Exception:
                return None
            rows = response.get("records") if isinstance(response, dict) else None
            if not isinstance(rows, list):
                return None
            for row in rows:
                if not isinstance(row, dict):
                    continue
                field = str(row.get("field") or "")
                value = row.get("value")
                if not field or not isinstance(value, str):
                    continue
                try:
                    offset = int(field)
                    decoded = json.loads(value)
                except Exception:
                    continue
                sequence = shard * self._shard_size + offset
                if sequence >= count:
                    continue
                if isinstance(decoded, dict) and isinstance(decoded.get("record_bundle"), list):
                    for item in decoded["record_bundle"]:
                        if isinstance(item, dict):
                            records_by_sequence.append((sequence, item))
                elif isinstance(decoded, dict):
                    records_by_sequence.append((sequence, decoded))
        records_by_sequence.sort(key=lambda item: item[0])
        return [record for _, record in records_by_sequence]

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


class MatrixArkRustProxyClient:
    """Persistent Rust proxy boundary around the Rust TemporalStore SDK.

    The Rust binary owns SDK linkage and runs in JSON-lines ``--serve`` mode as
    a Rust proxy. MatrixArk production and benchmark paths should use this
    proxy or the Rust direct SDK path, never process-per-operation CLI calls.
    """

    def __init__(
        self,
        *,
        proxy_path: str = "",
        cli_path: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
        sdk_mode: str = "proxy",
    ) -> None:
        proxy_path = proxy_path or cli_path
        if not proxy_path:
            raise MatrixArkError("--rust-proxy or MATRIXARK_TEMPORALSTORE_RUST_PROXY is required for temporalstore-rust")
        self.cli_path = proxy_path
        self.proxy_path = proxy_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self._legacy_lock = threading.Lock()
        self._legacy_semaphore = threading.BoundedSemaphore(1)
        self._backpressure_timeout_s = max(
            0.05,
            int(
                os.environ.get(
                    "MATRIXARK_RUST_PROXY_BACKPRESSURE_TIMEOUT_MS",
                    os.environ.get("MATRIXARK_RUST_GATEWAY_BACKPRESSURE_TIMEOUT_MS", str(request_timeout_ms)),
                )
            )
            / 1000.0,
        )
        self._write_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_WRITE_LANES", "4")))
        self._read_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_READ_LANES", "4")))
        self._pack_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_PACK_LANES", "2")))
        self._control_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_CONTROL_LANES", "1")))
        self._shared_process_mode = os.environ.get("MATRIXARK_RUST_PROXY_SHARED_PROCESS", "1").strip().lower() not in {"0", "false", "no"}
        if self._shared_process_mode:
            # The local Rust TemporalEngine is embedded in the proxy process. A
            # multi-process lane pool can hide writes from reads until there is
            # a real shared server/proxy behind it, so correctness-first parity
            # uses one process and one stdin/stdout lock by default.
            shared_lanes = self._make_lanes(1)
            self._lanes = {
                "write": shared_lanes,
                "read": shared_lanes,
                "pack": shared_lanes,
                "control": shared_lanes,
            }
        else:
            self._lanes = {
                "write": self._make_lanes(self._write_lane_count),
                "read": self._make_lanes(self._read_lane_count),
                "pack": self._make_lanes(self._pack_lane_count),
                "control": self._make_lanes(self._control_lane_count),
            }
        self._lane_cursors = {name: 0 for name in self._lanes}
        self._lane_select_lock = threading.Lock()
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

    @staticmethod
    def _make_lanes(count: int) -> list[Json]:
        return [
            {
                "proc": None,
                "lock": threading.Lock(),
                "semaphore": threading.BoundedSemaphore(1),
            }
            for _ in range(count)
        ]

    def close(self) -> None:
        seen: set[int] = set()
        for lanes in getattr(self, "_lanes", {}).values():
            for lane in lanes:
                proc = lane.get("proc")
                lane["proc"] = None
                if proc is None or id(proc) in seen:
                    continue
                seen.add(id(proc))
                self._close_proc(proc)
        proc = self._proc
        self._proc = None
        if proc is not None and id(proc) not in seen:
            self._close_proc(proc)

    @staticmethod
    def _close_proc(proc: subprocess.Popen[str]) -> None:
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

    def _ensure_lane_proc(self, lane: Json) -> subprocess.Popen[str]:
        proc = lane.get("proc")
        if proc is not None and proc.poll() is None:
            return proc
        if proc is not None:
            self._close_proc(proc)
        lane["proc"] = subprocess.Popen(
            [self.cli_path, "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        return lane["proc"]

    def _lane_group_for_op(self, op: str) -> str:
        if op in {
            "batch_hset",
            "matrixark_append_records",
            "matrixark_batch_append_records",
            "matrixark_batch_append_raw_ingestion_records",
            "hset",
            "put_string",
            "write_matrixark_record",
            "write_matrixark_records",
        }:
            return "write"
        if op in {"matrixark_retrieve_context_pack"}:
            return "pack"
        if op in {"batch_hget", "hgetall", "scan_hash", "hget", "get_string", "read_matrixark_record", "read_matrixark_records"}:
            return "read"
        return "control"

    def _choose_lane(self, op: str) -> tuple[str, Json]:
        group = self._lane_group_for_op(op)
        lanes = self._lanes.get(group) or self._lanes["control"]
        with self._lane_select_lock:
            index = self._lane_cursors.get(group, 0) % len(lanes)
            self._lane_cursors[group] = index + 1
        return group, lanes[index]

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

    def _call_json(self, op: str, raise_on_error: bool = True, **kwargs: Any) -> Json:
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
        _group, lane = self._choose_lane(op)
        semaphore: threading.BoundedSemaphore = lane["semaphore"]
        acquired = semaphore.acquire(timeout=self._backpressure_timeout_s)
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, backpressure=True)
            raise MatrixArkError(
                f"Rust TemporalStore {op} rejected by proxy backpressure after "
                f"{self._backpressure_timeout_s:.3f}s"
            )
        try:
            lock: threading.Lock = lane["lock"]
            with lock:
                proc = self._ensure_lane_proc(lane)
                assert proc.stdin is not None
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                except BrokenPipeError as exc:
                    lane["proc"] = None
                    self._close_proc(proc)
                    raise MatrixArkError(f"Rust TemporalStore {op} pipe closed") from exc
                response = self._read_json_line(proc, op)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True)
            raise
        finally:
            semaphore.release()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not response.get("ok"):
            self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=True)
            if not raise_on_error:
                return response
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
                elif op in {"batch_hset", "matrixark_append_records", "matrixark_batch_append_records"}:
                    compact_entries = kwargs.get("entries_compact") or []
                    entries = kwargs.get("entries") or []
                    self._records_written_total += count or len(compact_entries) or len(entries)
                    for entry in entries:
                        if isinstance(entry, dict):
                            self._count_context_record(entry.get("value"))
                    for entry in compact_entries:
                        if isinstance(entry, (list, tuple)) and len(entry) >= 3:
                            self._count_context_record(entry[2])
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

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> Json:
        return self._call_json(
            "matrixark_retrieve_context_pack",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            record_types=[
                "context_compression_event",
                "context_embedding",
                "context_entity",
                "context_event",
                "context_index",
                "context_segment",
                "context_summary",
                "resource_chunk",
                "resource_manifest",
                "skill_registry_update",
                "skill_section",
            ],
            scope=request.get("scope", {}),
            secondary_index_groups=request.get("secondary_index_groups", []),
            record=request,
        )

    def metrics_snapshot(self) -> Json:
        with self._metrics_lock:
            elapsed_s = max(0.001, time.time() - self._started_at)
            samples = list(self._latency_samples_ms)
            context_counts = dict(sorted(self._context_record_counts.items()))
            return {
                "gateway_mode": "rust_direct_sdk_bridge",
                "sdk_mode": "rust_direct_sdk_via_long_lived_bridge",
                "transport": "stdio",
                "proxy_path": self.proxy_path,
                "cli_path": self.cli_path,
                "shared_process_mode": self._shared_process_mode,
                "max_inflight": 1
                if self._shared_process_mode
                else self._write_lane_count + self._read_lane_count + self._pack_lane_count + self._control_lane_count,
                "lane_pool": {
                    "write": 1 if self._shared_process_mode else self._write_lane_count,
                    "read": 1 if self._shared_process_mode else self._read_lane_count,
                    "pack": 1 if self._shared_process_mode else self._pack_lane_count,
                    "control": 1 if self._shared_process_mode else self._control_lane_count,
                },
                "write_pool_size": 1 if self._shared_process_mode else self._write_lane_count,
                "read_pool_size": 1 if self._shared_process_mode else self._read_lane_count,
                "pack_pool_size": 1 if self._shared_process_mode else self._pack_lane_count,
                "control_pool_size": 1 if self._shared_process_mode else self._control_lane_count,
                "write_pool_enabled": False if self._shared_process_mode else self._write_lane_count > 1,
                "read_pool_enabled": False if self._shared_process_mode else self._read_lane_count > 1,
                "pack_pool_enabled": False if self._shared_process_mode else self._pack_lane_count > 1,
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
                "direct_sdk_bridge": True,
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
                "supports_placement_key_routing": True,
                "supports_prefix_scan": True,
                "supports_graceful_shutdown": True,
                "structured_errors": True,
                "matrixark_batch_append_wire_format": "entries_compact",
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

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        if not entries and not count_key:
            return
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), str(entry.get("value") or "")]
            for entry in entries
            if isinstance(entry, dict)
        ]
        self._call_json(
            "matrixark_batch_append_records",
            entries_compact=compact_entries,
            key=count_key or "",
            value=count_value or "",
            append_options=append_options or {},
        )

    def matrixark_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        self.matrixark_batch_append_records(
            entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def matrixark_retrieve_context_pack(self, request: Json | str) -> Json:
        if isinstance(request, str):
            decoded = json.loads(request)
            request_payload = decoded if isinstance(decoded, dict) else {}
        else:
            request_payload = dict(request)
        response = self._call_json("matrixark_retrieve_context_pack", **request_payload)
        value = response.get("value")
        if isinstance(value, str) and value:
            decoded = json.loads(value)
            if isinstance(decoded, dict):
                return decoded
        return response

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        if not entries:
            return []
        response = self._call_json("batch_hget", entries=entries)
        records = response.get("records", [])
        return records if isinstance(records, list) else []

    def scan_hash(self, key: str) -> Json:
        return self._call_json("scan_hash", key=key)

    def matrixark_scan_candidates(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        scope: Json,
        record_types: list[str],
        secondary_index_groups: list[list[str]],
        selected_node_hashes: list[int],
    ) -> Json:
        return self._call_json(
            "matrixark_scan_candidates",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )

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


MatrixArkRustCliClient = MatrixArkRustProxyClient


class MatrixArkTemporalStoreRustAdapter(MatrixArkTemporalStoreDirectAdapter):
    """MatrixArk adapter backed by the Rust TemporalStore proxy or direct SDK."""

    def __init__(
        self,
        *,
        rust_cli: str = "",
        rust_proxy: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        storage_prefix: str = "matrixark:mcp",
        request_timeout_ms: int = 20000,
        io_timeout_ms: int = 20000,
        sdk_mode: str = "proxy",
    ) -> None:
        MatrixArkLocalAdapter.__init__(self, Path("/tmp/matrixark-mcp-unused-rust.jsonl"))
        MatrixArkLocalAdapter._init_local_runtime_state(self)
        self._entity_cache_loaded = True
        self._context_node_cache_loaded = True
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        proxy_path = rust_proxy or rust_cli
        self._client = MatrixArkRustProxyClient(
            proxy_path=proxy_path,
            metaserver=metaserver,
            namespace=namespace,
            table=table,
            request_timeout_ms=request_timeout_ms,
            io_timeout_ms=io_timeout_ms,
            sdk_mode=sdk_mode,
        )
        self._metaserver = metaserver
        self._namespace = namespace
        self._table = table
        self._readiness_cache: Json | None = None
        self._readiness_lock = threading.RLock()
        self._storage_prefix = storage_prefix.rstrip(":")
        self._supported_storage_families = self._parse_supported_storage_families()
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

    def _rust_storage_mode_label(self) -> str:
        return "rust-direct-sdk-bridge" if getattr(self._client, "sdk_mode", "") == "direct_sdk" else "rust-proxy"

    def _backend_neutral_prometheus(self, snapshot: Json) -> str:
        backend = "rust"
        storage_mode = self._rust_storage_mode_label()
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
            f'matrixark_backend_info{{backend="{backend}",storage_mode="rust-direct-sdk-bridge"}} 1',
            "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{backend="{backend}",storage_mode="rust-direct-sdk-bridge",status="{"ready" if self._backend_ready else "unknown"}"}} {1 if self._backend_ready else 0}',
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
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + f"# matrixark_rust_proxy_metrics_error {json.dumps(str(exc))}\n"
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "gateway_mode": "rust_direct_sdk_bridge",
            "sdk_mode": "rust_direct_sdk_via_long_lived_bridge",
            "production_path": "rust_direct_sdk_bridge",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only",
            "direct_sdk_bridge": True,
            "pure_embedded_direct_sdk": False,
            "capabilities": {
                "health_endpoint": True,
                "readiness_endpoint": True,
                "metrics_endpoint": True,
                "batch_append": True,
                "matrixark_batch_append_records": True,
                "matrixark_retrieve_context_pack": True,
                "compact_secondary_index_lookup": True,
                "placement_key_candidate_fetch": True,
                "context_pack_telemetry": True,
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


class MatrixArkTemporalStoreRustDirectAdapter(MatrixArkTemporalStoreRustAdapter):
    """MatrixArk adapter backed by a long-lived Rust process using the Rust SDK directly.

    This is the Rust parity counterpart to the C++ direct SDK adapter. The Python
    MCP process still owns protocol/model glue, while the Rust bridge owns the
    TemporalStore SDK client and native storage calls. It is intentionally
    explicit so benchmark reports can distinguish it from the production Rust
    proxy path.
    """

    def __init__(self, **kwargs: Any) -> None:
        kwargs["sdk_mode"] = "direct_sdk"
        super().__init__(**kwargs)

    def _backend_label(self) -> str:
        return "temporalstore-rust-direct"
