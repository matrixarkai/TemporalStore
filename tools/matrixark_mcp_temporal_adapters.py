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
    from tools.matrixark_mcp_env import env_bool, env_int, env_lower
    from tools.matrixark_mcp_backend_metric_state import (
        initialize_backend_metric_state,
    )
    from tools import matrixark_mcp_backend_metrics as backend_metrics_helpers
    from tools import matrixark_mcp_direct_cache as direct_cache_helpers
    from tools import matrixark_mcp_temporal_append as temporal_append_helpers
    from tools.matrixark_mcp_direct_write_queue import (
        direct_write_durable_field,
        direct_write_durable_pending_count,
        direct_write_durable_payload,
        direct_write_loop,
        drain_durable_direct_write_queue,
        enqueue_direct_write,
        enqueue_direct_write_item,
        ensure_direct_write_queue_fields,
        flush_direct_write_durable_field,
        flush_direct_write_item,
        flush_direct_write_items,
        flush_direct_writes,
        load_direct_write_durable_payload,
        records_can_use_direct_write_queue,
        start_direct_write_worker,
        write_direct_write_durable_status,
    )
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_latest_context_state import (
        append_log_records_without_latest_state,
        latest_context_state_entries,
        latest_context_state_field,
        latest_context_state_storage_key,
    )
    from tools.matrixark_mcp_native_side_index import (
        context_index_lookup_key,
        context_placement_lookup_key,
        context_ref_locator_key,
        merge_ref_hashes,
        merge_ref_locations,
        merge_resource_versions,
        native_side_index_entries_for_bundles,
    )
    from tools.matrixark_mcp_native_pack import build_native_context_pack_request
    from tools.matrixark_mcp_raw_ingestion import (
        ensure_raw_ingestion_fields,
        normalize_raw_storage_backend,
        raw_ingestion_append_options,
        raw_ingestion_append_path_for_backend,
        append_raw_ingestion_records,
        get_raw_count,
        raw_record_location,
        raw_record_scope_value,
        raw_record_session_ids,
        raw_session_index_entries,
        raw_session_index_key,
    )
    from tools.matrixark_mcp_retrieval import native_retrieve_fallback_allowed
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool, env_int, env_lower
    from matrixark_mcp_backend_metric_state import (
        initialize_backend_metric_state,
    )
    import matrixark_mcp_backend_metrics as backend_metrics_helpers
    import matrixark_mcp_direct_cache as direct_cache_helpers
    import matrixark_mcp_temporal_append as temporal_append_helpers
    from matrixark_mcp_direct_write_queue import (
        direct_write_durable_field,
        direct_write_durable_pending_count,
        direct_write_durable_payload,
        direct_write_loop,
        drain_durable_direct_write_queue,
        enqueue_direct_write,
        enqueue_direct_write_item,
        ensure_direct_write_queue_fields,
        flush_direct_write_durable_field,
        flush_direct_write_item,
        flush_direct_write_items,
        flush_direct_writes,
        load_direct_write_durable_payload,
        records_can_use_direct_write_queue,
        start_direct_write_worker,
        write_direct_write_durable_status,
    )
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_latest_context_state import (
        append_log_records_without_latest_state,
        latest_context_state_entries,
        latest_context_state_field,
        latest_context_state_storage_key,
    )
    from matrixark_mcp_native_side_index import (
        context_index_lookup_key,
        context_placement_lookup_key,
        context_ref_locator_key,
        merge_ref_hashes,
        merge_ref_locations,
        merge_resource_versions,
        native_side_index_entries_for_bundles,
    )
    from matrixark_mcp_native_pack import build_native_context_pack_request
    from matrixark_mcp_raw_ingestion import (
        ensure_raw_ingestion_fields,
        normalize_raw_storage_backend,
        raw_ingestion_append_options,
        raw_ingestion_append_path_for_backend,
        append_raw_ingestion_records,
        get_raw_count,
        raw_record_location,
        raw_record_scope_value,
        raw_record_session_ids,
        raw_session_index_entries,
        raw_session_index_key,
    )
    from matrixark_mcp_retrieval import native_retrieve_fallback_allowed


try:
    from tools.matrixark_mcp_native_helpers import (
        compact_native_selected_refs as _compact_native_selected_refs,
        float_metric_or_default as _float_metric_or_default,
        latency_quantile_from_bucket_map as _latency_quantile_from_bucket_map,
        selected_ref_class as _selected_ref_class,
        selected_ref_stable_key as _selected_ref_stable_key,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_native_helpers import (
        compact_native_selected_refs as _compact_native_selected_refs,
        float_metric_or_default as _float_metric_or_default,
        latency_quantile_from_bucket_map as _latency_quantile_from_bucket_map,
        selected_ref_class as _selected_ref_class,
        selected_ref_stable_key as _selected_ref_stable_key,
    )


class MatrixArkTemporalStoreDirectAdapter(MatrixArkLocalAdapter):
    """MatrixArk adapter backed by C++ TemporalStore proxy or direct SDK.

    Python stays as API/auth/model orchestration. Production retrieval should
    call native C++ proxy/direct APIs for append, prefix scan, secondary-index
    prefiltering, scoring, and ContextPack assembly. Direct SDK remains the
    embedded/local path; MATRIXARK_TEMPORALSTORE_CPP_PROXY_ENDPOINT selects the
    proxy boundary.
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
        from temporalstore import Client, Options, ProxyClient, ProxyOptions  # type: ignore

        proxy_endpoint = os.environ.get("MATRIXARK_TEMPORALSTORE_CPP_PROXY_ENDPOINT", "").strip()
        force_generic_batch_fallback = os.environ.get("MATRIXARK_FORCE_GENERIC_BATCH_HSET_FALLBACK", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._cpp_proxy_endpoint = proxy_endpoint
        if proxy_endpoint:
            proxy_options = ProxyOptions(
                endpoint=proxy_endpoint,
                namespace_name=namespace,
                table_name=table,
                timeout_seconds=max(1.0, request_timeout_ms / 1000.0),
            )
            self._client = ProxyClient(proxy_options)
            self._matrixark_native_batch_append_available = True
            self._matrixark_append_write_path = "cpp_proxy_matrixark_batch_append_records"
        else:
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
                "cpp_direct_existing_batch_execute_raw_batch_mset"
                if self._matrixark_native_batch_append_available
                else "fallback_python_batch_hset_loop"
            )
        if force_generic_batch_fallback:
            self._matrixark_native_batch_append_available = False
            self._matrixark_append_write_path = "forced_fallback_python_batch_hset_loop"
        self._matrixark_append_uses_per_record_hset = not self._matrixark_native_batch_append_available
        self._matrixark_batch_append_uses_existing_batch_execute = bool(
            self._matrixark_native_batch_append_available
            and self._matrixark_append_write_path == "cpp_direct_existing_batch_execute_raw_batch_mset"
        )
        self._matrixark_proxy_mode = bool(proxy_endpoint)
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
        self._pending_visibility_keys: set[str] = set()
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
        self._direct_write_queue_enabled = env_bool("MATRIXARK_DIRECT_WRITE_QUEUE", False)
        self._direct_write_queue_max_records = max(1, env_int("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", 10000))
        self._direct_write_queue_put_timeout_s = max(0.01, env_int("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", 1000) / 1000.0)
        self._direct_write_queue_mode = env_lower("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory") or "memory"
        if self._direct_write_queue_mode not in {"memory", "temporalstore"}:
            raise MatrixArkError("MATRIXARK_DIRECT_WRITE_QUEUE_MODE must be memory or temporalstore")
        self._direct_write_queue_drain_max_batches = max(1, env_int("MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES", 64))
        self._direct_write_queue_allow_sync_context = env_bool("MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT", False)
        self._direct_write_queue_autostart = env_bool("MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART", True)
        self._native_side_index_assume_fresh = env_bool("MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH", False)
        self._direct_raw_ingestion_queue_enabled = env_bool("MATRIXARK_DIRECT_RAW_INGESTION_QUEUE", False)
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
        initialize_backend_metric_state(self, MatrixArkServiceMetrics.LATENCY_BUCKETS_MS)

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
        return "temporalstore-cpp-proxy" if getattr(self, "_matrixark_proxy_mode", False) else "temporalstore-cpp"

    def python_hot_cache_enabled(self) -> bool:
        return python_hot_cache_allowed(backend_label=self._backend_label())

    def _ensure_backend_metric_fields(self) -> None:
        backend_metrics_helpers.ensure_temporal_backend_metric_fields(self)

    def _ensure_direct_write_queue_fields(self) -> None:
        ensure_direct_write_queue_fields(self)

    def _observe_append_queue_wait(self, elapsed_ms: float) -> None:
        backend_metrics_helpers.observe_append_queue_wait(self, elapsed_ms)

    def _observe_append_engine(self, elapsed_ms: float) -> None:
        backend_metrics_helpers.observe_append_engine(self, elapsed_ms)

    def _append_queue_wait_ms_avg(self) -> float:
        return backend_metrics_helpers.append_queue_wait_ms_avg(self)

    def _append_engine_ms_avg(self) -> float:
        return backend_metrics_helpers.append_engine_ms_avg(self)

    def _observe_backend_command(self, elapsed_ms: float, *, records_written: int = 0, records_read: int = 0, failed: bool = False) -> None:
        backend_metrics_helpers.observe_backend_command(
            self,
            elapsed_ms,
            records_written=records_written,
            records_read=records_read,
            failed=failed,
        )

    def _backend_prometheus(self) -> str:
        return backend_metrics_helpers.backend_prometheus(self)

    def backend_metrics(self) -> Json:
        return backend_metrics_helpers.backend_metrics(self)

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
        if self._queue_batched_records(materialized):
            return
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
        return normalize_raw_storage_backend(value)

    def _raw_ingestion_append_path(self) -> str:
        return raw_ingestion_append_path_for_backend(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )

    def _raw_ingestion_append_options(self) -> Json:
        return raw_ingestion_append_options(
            getattr(self, "_raw_storage_backend", "temporalstore")
        )

    def _ensure_raw_ingestion_fields(self) -> None:
        ensure_raw_ingestion_fields(self)

    def _raw_record_location(self, sequence: int) -> tuple[str, str]:
        self._ensure_raw_ingestion_fields()
        return raw_record_location(self._raw_record_hash_key, self._shard_size, sequence)

    def _raw_session_index_key(self, session_id: str) -> str:
        self._ensure_raw_ingestion_fields()
        return raw_session_index_key(self._raw_ingestion_prefix, session_id)

    def _raw_record_scope_value(self, record: Json, name: str) -> str:
        return raw_record_scope_value(record, name)

    def _raw_record_session_ids(self, record: Json) -> set[str]:
        return raw_record_session_ids(record)

    def _raw_session_index_entries(self, *, sequence: int, record: Json) -> list[Json]:
        self._ensure_raw_ingestion_fields()
        return raw_session_index_entries(
            raw_ingestion_prefix=self._raw_ingestion_prefix,
            shard_size=self._shard_size,
            sequence=sequence,
            record=record,
        )

    def _get_raw_count(self) -> int:
        return get_raw_count(self)

    def _append_raw_ingestion_records(self, records: list[Json], *, allow_queue: bool = True) -> None:
        append_raw_ingestion_records(self, records, allow_queue=allow_queue)

    def _context_index_lookup_key(self, scope_key: str) -> str:
        return context_index_lookup_key(self._storage_prefix, scope_key)

    def _context_ref_locator_key(self) -> str:
        return context_ref_locator_key(self._storage_prefix)

    def _context_placement_lookup_key(self, scope_key: str) -> str:
        return context_placement_lookup_key(self._storage_prefix, scope_key)

    def _merge_ref_hashes(self, existing_value: str, new_refs: list[int]) -> list[int]:
        return merge_ref_hashes(existing_value, new_refs)

    def _merge_ref_locations(self, existing_value: str, new_locations: list[Json]) -> list[Json]:
        return merge_ref_locations(existing_value, new_locations)

    def _merge_resource_versions(self, existing_value: str, new_versions: set[str]) -> list[str]:
        return merge_resource_versions(existing_value, new_versions)

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

    def _native_side_index_entries_for_bundles(self, bundles: list[tuple[list[Json], str, str]]) -> list[Json]:
        return native_side_index_entries_for_bundles(
            storage_prefix=self._storage_prefix,
            bundles=bundles,
            storage_route_for_bundle=self._storage_route_for_bundle,
            read_hash_value=self._read_hash_value_best_effort,
        )

    def _context_event_ingestion_time_ms(self, record: Json) -> int:
        return context_event_timestamp_ms(record)

    def _context_event_time_index_key(self, record: Json) -> str:
        return context_event_time_index_key(self._storage_prefix, record)

    def _context_event_time_index_field(self, record: Json) -> str:
        return context_event_time_index_field(record)

    def _context_event_time_index_payload(self, record: Json) -> str:
        """Compact timestamp-index payload.

        The full ContextEvent is already written to the serving record log.  The
        timestamp index is an ordered lookup structure, so it only needs enough
        information to find/filter the canonical event record.  Keeping raw text
        and extraction/debug fields out of this index avoids doubling hot write
        bytes for every event.
        """
        return context_event_time_index_payload(record)

    def _context_event_time_index_entries(self, records: list[Json]) -> list[Json]:
        return context_event_time_index_entries(self._storage_prefix, records)

    def _latest_context_state_key(self) -> str:
        return latest_context_state_storage_key(self._storage_prefix)

    def _latest_context_state_field(self, record: Json) -> str | None:
        return latest_context_state_field(record)

    def _append_log_records(self, records: list[Json]) -> list[Json]:
        return append_log_records_without_latest_state(records)

    def _split_compacted_latest_context_state(self, records: list[Json]) -> tuple[list[Json], list[Json]]:
        """Split an already-compacted batch into latest-state writes and append-log rows."""
        latest_state_entries: list[Json] = []
        append_records_for_log: list[Json] = []
        for record in records:
            field = self._latest_context_state_field(record)
            if not field:
                append_records_for_log.append(record)
                continue
            latest_state_entries.extend(latest_context_state_entries(self._storage_prefix, [record]))
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
        return compact_latest_context_state_records(list(records) + self._load_latest_context_state_records())

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
        return records_can_use_direct_write_queue(self, records)

    def _start_direct_write_worker(self) -> None:
        start_direct_write_worker(self)

    def _direct_write_durable_payload(self, records: list[Json]) -> Json:
        return direct_write_durable_payload(
            records,
            backend=self._backend_label(),
            storage_prefix=self._storage_prefix,
        )

    def _direct_write_durable_field(self, payload: Json) -> str:
        return direct_write_durable_field(payload)

    def _enqueue_direct_write_durable(self, records: list[Json]) -> str:
        payload = self._direct_write_durable_payload(list(records))
        field = self._direct_write_durable_field(payload)
        self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(payload, separators=(",", ":")))
        return field

    def _enqueue_direct_write_item(self, item: Any, record_count: int) -> None:
        enqueue_direct_write_item(self, item, record_count)

    def _enqueue_direct_write(self, records: list[Json]) -> None:
        enqueue_direct_write(self, records)

    def _direct_write_loop(self) -> None:
        direct_write_loop(self)

    def _flush_direct_write_items(self, items: list[Any]) -> int:
        return flush_direct_write_items(self, items)

    def _flush_direct_write_item(self, item: Any) -> int:
        return flush_direct_write_item(self, item)

    def _load_direct_write_durable_payload(self, field: str) -> Json | None:
        return load_direct_write_durable_payload(self, field)

    def _write_direct_write_durable_status(self, field: str, payload: Json, status: str, error: str | None = None) -> None:
        write_direct_write_durable_status(self, field, payload, status, error)

    def _flush_direct_write_durable_field(self, field: str) -> int:
        return flush_direct_write_durable_field(self, field)

    def drain_durable_direct_write_queue(self, *, limit: int | None = None) -> Json:
        return drain_durable_direct_write_queue(self, limit=limit)

    def _direct_write_durable_pending_count(self) -> int:
        return direct_write_durable_pending_count(self)

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        flush_direct_writes(self, timeout_s=timeout_s)

    def _append_client_for_records(self, records: list[Json]) -> Any:
        return self._client

    def _materialize_appended_records_locked(
        self,
        *,
        prior_entry_count: int,
        new_entry_count: int,
        records: list[Json],
    ) -> None:
        temporal_append_helpers.materialize_appended_records_locked(
            self,
            prior_entry_count=prior_entry_count,
            new_entry_count=new_entry_count,
            records=records,
        )

    def _append_many_materialized(self, records: list[Json], *, allow_queue: bool = True) -> None:
        temporal_append_helpers.append_many_materialized(self, records, allow_queue=allow_queue)

    def _note_pending_visibility_keys(self, keys: Iterable[str]) -> None:
        if not (
            getattr(self, "_publish_visibility_after_flush", False)
            or getattr(self, "_track_pending_visibility_keys", False)
        ):
            return
        pending = getattr(self, "_pending_visibility_keys", None)
        if pending is None:
            self._pending_visibility_keys = set()
            pending = self._pending_visibility_keys
        for key in keys:
            key = str(key or "")
            if key:
                pending.add(key)

    def _raw_ingestion_visibility_required_after_flush(self) -> bool:
        if not getattr(self, "_publish_visibility_after_flush", False):
            return False
        return bool(getattr(self, "_dedicated_proxy_clients_enabled", False))

    def _has_pending_visibility_keys(self) -> bool:
        pending = getattr(self, "_pending_visibility_keys", None)
        return bool(pending)

    def _consume_pending_visibility_keys(self) -> list[str]:
        pending = getattr(self, "_pending_visibility_keys", None)
        if not pending:
            return []
        keys = sorted(pending)
        pending.clear()
        return keys

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
            hot_cache_enabled = self.python_hot_cache_enabled()
            if hot_cache_enabled and self._records_cache is not None:
                self._records_cache = self._with_latest_context_state_records(self._records_cache)
                return list(self._records_cache)
            count = self._get_count()
            if count > 0:
                self._legacy_index_mode = False
                self._entry_count_cache = count
                if not hot_cache_enabled:
                    self._records_cache = None
                    self._drop_direct_record_cache()
                    return self._with_latest_context_state_records(self._load_records_by_count(count))
                cached = self._get_direct_record_cache(count)
                if cached is not None:
                    self._records_cache = self._with_latest_context_state_records(cached)
                    return list(self._records_cache)
                with self._direct_record_load_lock():
                    cached = self._get_direct_record_cache(count)
                    if cached is not None:
                        self._records_cache = self._with_latest_context_state_records(cached)
                        return list(self._records_cache)
                    self._records_cache = self._with_latest_context_state_records(self._load_records_by_count(count))
                    self._put_direct_record_cache(count, self._records_cache)
                    return list(self._records_cache)
            index = self._get_index()
            self._index_cache = index
            self._legacy_index_mode = bool(index)
            self._entry_count_cache = None
            records = self._with_latest_context_state_records(self._load_records(index))
            if hot_cache_enabled:
                self._records_cache = records
            else:
                self._records_cache = None
            return list(records)

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
    ) -> Json:
        """Return retrieval candidates with native scan/cache prefiltering.

        C++ direct and Rust proxy/direct SDK expose native hash/prefix scan for
        debug candidate inspection. Normal TemporalStore retrieval should use
        matrixark_retrieve_context_pack so Python receives a finished ContextPack
        instead of materializing candidates or assembling the hot-path pack.
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
        if native_candidate_prefilter_required(backend_label=self._backend_label()):
            raise MatrixArkError(
                f"backend-native candidate prefilter is required for {self._backend_label()}, "
                "but matrixark_scan_candidates did not return candidates. Python read_all scan/prefilter is disabled."
            )

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
                ref_hashes = context_index_record_ref_hashes(record)
                for legacy_field in ("chunk_hash", "section_hash", "skill_hash"):
                    legacy_value = record.get(legacy_field)
                    if legacy_value is not None:
                        ref_hashes.append(legacy_value)
                ref_hashes = ordered_unique_any(ref_hashes)
                node_hashes_for_index = context_index_record_node_hashes(record)
                for node_hash_for_index in node_hashes_for_index:
                    try:
                        node_hash_int = int(node_hash_for_index)
                    except (TypeError, ValueError):
                        continue
                    index_terms_by_node_for_prefilter.setdefault(node_hash_int, []).append(index_name)
                    index_terms_by_node.setdefault(node_hash_int, []).append(index_name)
                if ref_hashes:
                    for ref_hash in ref_hashes:
                        index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                elif not node_hashes_for_index:
                    index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
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


    def supports_native_candidate_prefilter(self) -> bool:
        return callable(getattr(getattr(self, "_client", None), "matrixark_scan_candidates", None))

    def supports_native_context_pack(self) -> bool:
        return callable(getattr(getattr(self, "_client", None), "matrixark_retrieve_context_pack", None))

    def _ensure_direct_context_pack_response_cache(self) -> None:
        direct_cache_helpers.ensure_direct_context_pack_response_cache(self)

    def _direct_context_pack_response_cache_key(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> str:
        return direct_cache_helpers.direct_context_pack_response_cache_key(
            self,
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            request=request,
        )

    def _direct_context_pack_response_cache_get(self, cache_key: str) -> Json | None:
        return direct_cache_helpers.direct_context_pack_response_cache_get(self, cache_key)

    def _direct_context_pack_response_cache_put(self, cache_key: str, response: Json) -> None:
        direct_cache_helpers.direct_context_pack_response_cache_put(self, cache_key, response)

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
        except Exception as exc:
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly failed for {self._backend_label()}: {exc}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        if not isinstance(response, dict) or not response.get("native_pack_assembly"):
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly returned an invalid response for {self._backend_label()}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
            return None
        if isinstance(response.get("records"), list):
            raise MatrixArkError(
                "native matrixark_retrieve_context_pack must return a finished ContextPack, not raw records"
            )
        pack = response.get("context_pack")
        if not isinstance(pack, dict):
            return None
        pack.setdefault("context_pack_assembly", "native_backend")
        pack.setdefault("backend", self._backend_label())
        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
        contract = recall_policy.get("native_response_contract") if isinstance(recall_policy.get("native_response_contract"), dict) else {}
        contract.setdefault("raw_records_returned_to_python", False)
        contract.setdefault("python_hot_path_records", 0)
        contract.setdefault("python_role", "dispatch_request_receive_context_pack")
        contract.setdefault("backend_role", "scan_filter_score_pack")
        recall_policy["native_response_contract"] = contract
        pack["recall_policy"] = recall_policy
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
        except Exception as exc:
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter failed for {self._backend_label()}: {exc}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        records = response.get("records") if isinstance(response, dict) else None
        if not isinstance(records, list):
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter returned an invalid response for {self._backend_label()}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
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
        latest_state_records = self._latest_context_state_records_for_candidate_scan(
            scope=scope,
            record_types=record_types,
            selected_node_hashes=selected_node_hashes,
        )
        if latest_state_records:
            records = list(records) + latest_state_records
        records = compact_latest_context_state_records(records)
        scan_stats["latest_summary_state_compaction"] = True
        scan_stats["latest_state_records_loaded"] = len(latest_state_records)
        return {"records": records, "scan_stats": scan_stats}

    def _direct_record_load_lock(self) -> threading.RLock:
        return direct_cache_helpers.direct_record_load_lock(self)

    def _get_direct_record_cache(self, count: int) -> list[Json] | None:
        return direct_cache_helpers.get_direct_record_cache(self, count)

    def _put_direct_record_cache(self, count: int, records: list[Json]) -> None:
        direct_cache_helpers.put_direct_record_cache(self, count, records)

    def _drop_direct_record_cache(self) -> None:
        direct_cache_helpers.drop_direct_record_cache(self)

    def _retrieval_candidate_cache_key(
        self,
        *,
        count: int,
        scope: Json,
        record_types: set[str] | None,
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> str:
        return direct_cache_helpers.retrieval_candidate_cache_key(
            self,
            count=count,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )

    def _prune_retrieval_candidate_cache(self, current_count: int) -> None:
        direct_cache_helpers.prune_retrieval_candidate_cache(self, current_count)

    def _placement_candidate_table_cache_key(
        self,
        *,
        count: int,
        scope_key: str,
        node_hash: int,
        record_type: str,
        resource_version_watermark: str = "",
    ) -> str:
        return direct_cache_helpers.placement_candidate_table_cache_key(
            self,
            count=count,
            scope_key=scope_key,
            node_hash=node_hash,
            record_type=record_type,
            resource_version_watermark=resource_version_watermark,
        )

    def _record_primary_hash(self, record: Json) -> int:
        return direct_cache_helpers.record_primary_hash(record)

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
        return direct_cache_helpers.placement_candidate_records_from_cache_or_load(
            self,
            count=count,
            scope=scope,
            allowed_types=allowed_types,
            selected_nodes=selected_nodes,
            locations=locations,
            resource_version_watermark=resource_version_watermark,
        )

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
        if not self.supports_native_context_pack():
            return None
        native_pack_request = build_native_context_pack_request(self, args)
        request = native_pack_request["request"]
        cache_key = str(native_pack_request["cache_key"])
        scope = native_pack_request["scope"]
        query = str(native_pack_request["query"])
        debug_context_pack = bool(native_pack_request["debug_context_pack"])
        cached = self._direct_context_pack_response_cache_get(cache_key)
        if cached is not None:
            return cached
        started_perf = time.perf_counter()
        try:
            response = self.native_context_pack(request)
            if response is None:
                if not native_retrieve_fallback_allowed(args):
                    result = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_unavailable")
                    self._direct_context_pack_response_cache_put(cache_key, result)
                    return result
                return None
        except Exception as exc:
            _mcp_debug_log(f"matrixark native context pack failed: {exc}")
            if not native_retrieve_fallback_allowed(args):
                result = self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_error:{exc}")
                self._direct_context_pack_response_cache_put(cache_key, result)
                return result
            return None
        try:
            pack = json.loads(response) if isinstance(response, str) else response
        except Exception as exc:
            _mcp_debug_log(f"matrixark native context pack returned invalid JSON: {exc}")
            if not native_retrieve_fallback_allowed(args):
                result = self._native_context_pack_fallback_blocker(args, reason=f"native_context_pack_invalid_json:{exc}")
                self._direct_context_pack_response_cache_put(cache_key, result)
                return result
            return None
        if not isinstance(pack, dict):
            if not native_retrieve_fallback_allowed(args):
                result = self._native_context_pack_fallback_blocker(args, reason="native_context_pack_not_object")
                self._direct_context_pack_response_cache_put(cache_key, result)
                return result
            return None
        native_envelope = dict(pack)
        if isinstance(pack.get("context_pack"), dict):
            inner_pack = dict(pack["context_pack"])
            if isinstance(native_envelope.get("scan_stats"), dict):
                recall_policy = inner_pack.get("recall_policy") if isinstance(inner_pack.get("recall_policy"), dict) else {}
                recall_policy.setdefault("scan_stats", native_envelope["scan_stats"])
                inner_pack["recall_policy"] = recall_policy
            if isinstance(native_envelope.get("retrieval_metrics"), dict) and not isinstance(inner_pack.get("retrieval_metrics"), dict):
                inner_pack["retrieval_metrics"] = native_envelope["retrieval_metrics"]
            if native_envelope.get("selected_ref_count") is not None:
                inner_pack.setdefault("selected_ref_count", native_envelope.get("selected_ref_count"))
            if native_envelope.get("dropped_ref_count") is not None:
                inner_pack.setdefault("dropped_ref_count", native_envelope.get("dropped_ref_count"))
            pack = inner_pack
        selected_refs = pack.get("selected_refs", [])
        groups = pack.get("groups", [])
        if not isinstance(selected_refs, list) and not isinstance(groups, (list, dict)):
            if not native_retrieve_fallback_allowed(args):
                return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_missing_refs_or_groups")
            return None
        compact_dropped_refs = 0
        if isinstance(selected_refs, list) and selected_refs:
            compact_refs, compact_dropped_refs = _compact_native_selected_refs(selected_refs)
            if compact_refs and (compact_dropped_refs or len(compact_refs) != len(selected_refs)):
                pack["selected_refs"] = compact_refs
                pack["remote_context_refs"] = compact_refs
                selected_refs = compact_refs
            compact_token_total = 0
            for ref in selected_refs:
                if not isinstance(ref, dict):
                    continue
                try:
                    compact_token_total += int(ref.get("token_estimate") or 0)
                except (TypeError, ValueError):
                    compact_token_total += max(1, (len(str(ref.get("text") or "")) + 3) // 4)
            if compact_token_total > 0:
                pack["used_context_tokens"] = compact_token_total
                pack["used_remote_context_tokens"] = compact_token_total
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
        pack["context_pack_assembly"] = "native_cpp_direct"
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
            scan_stats = pack["recall_policy"].get("scan_stats") if isinstance(pack["recall_policy"].get("scan_stats"), dict) else {}
            if scan_stats:
                merged_native_telemetry = dict(scan_stats)
                merged_native_telemetry.update(native_telemetry)
                native_telemetry = merged_native_telemetry
            native_stage_metrics = native_telemetry.get("stages") if isinstance(native_telemetry.get("stages"), dict) else {}
            total_native_ms = round((time.perf_counter() - started_perf) * 1000.0, 3)
            selected_count = len(selected_refs) if isinstance(selected_refs, list) else 0
            pack_ms = float(native_telemetry.get("pack_ms") or native_stage_metrics.get("pack_ms") or 0.0)
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
                "append_queue_wait_ms": round(_float_metric_or_default(native_telemetry, "append_queue_wait_ms", self._append_queue_wait_ms_avg()), 3),
                "append_engine_ms": round(_float_metric_or_default(native_telemetry, "append_engine_ms", self._append_engine_ms_avg()), 3),
                "selected_refs": selected_count,
                "dropped_refs": int(native_telemetry.get("dropped_refs") or native_telemetry.get("dropped_ref_count") or 0) + compact_dropped_refs,
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
            native_candidate_class_counts = native_telemetry.get("candidate_class_counts")
            if isinstance(native_candidate_class_counts, dict):
                retrieval_metrics["candidate_class_counts"] = native_candidate_class_counts
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
            if compact_dropped_refs:
                native_drop_counters = dict(native_drop_counters or {})
                native_drop_counters["token_budget"] = int(native_drop_counters.get("token_budget") or 0) + compact_dropped_refs
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
            self._direct_context_pack_response_cache_put(cache_key, pack)
            return pack
        if isinstance(selected_refs, list) and selected_refs:
            result = compact_context_pack_for_serving(pack)
            self._direct_context_pack_response_cache_put(cache_key, result)
            return result
        self._direct_context_pack_response_cache_put(cache_key, pack)
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



try:
    from tools.matrixark_mcp_rust_direct_client import MatrixArkRustCdylibClient
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_rust_direct_client import MatrixArkRustCdylibClient


try:
    from tools.matrixark_mcp_rust_proxy_client import MatrixArkRustCliClient, MatrixArkRustProxyClient
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_rust_proxy_client import MatrixArkRustCliClient, MatrixArkRustProxyClient


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
        direct_lib = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", "").strip()
        temporalstore_lib = os.environ.get("TEMPORALSTORE_LIB", os.environ.get("MATRIXARK_TEMPORALSTORE_LIB", "")).strip()
        self._rust_direct_cdylib_enabled = bool(
            sdk_mode in {"direct-sdk", "direct_sdk", "native-binding", "rust-direct"}
            and direct_lib
            and Path(direct_lib).exists()
        )
        if self._rust_direct_cdylib_enabled:
            self._client = MatrixArkRustCdylibClient(
                library_path=direct_lib,
                temporalstore_lib=temporalstore_lib,
                metaserver=metaserver,
                namespace=namespace,
                table=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
            )
        else:
            self._client = MatrixArkRustProxyClient(
                proxy_path=proxy_path,
                metaserver=metaserver,
                namespace=namespace,
                table=table,
                request_timeout_ms=request_timeout_ms,
                io_timeout_ms=io_timeout_ms,
                sdk_mode=sdk_mode,
            )
        self._retrieve_client: Any | None = None
        self._summary_client: Any | None = None
        self._retrieve_client_lock = threading.RLock()
        self._summary_client_lock = threading.RLock()
        self._visibility_publish_lock = threading.RLock()
        self._visibility_publish_thread: threading.Thread | None = None
        self._visibility_publish_error: Exception | None = None
        self._dedicated_proxy_clients_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS",
            "1",
        ).strip().lower() in {"1", "true", "yes"}
        self._dedicated_pack_lanes_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES",
            "1",
        ).strip().lower() in {"1", "true", "yes"}
        self._publish_visibility_after_flush = os.environ.get(
            "MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_ON_FLUSH",
            "0",
        ).strip().lower() in {"1", "true", "yes"}
        self._track_pending_visibility_keys = (
            self._dedicated_proxy_clients_enabled or self._dedicated_pack_lanes_enabled
        )
        self._async_visibility_publish_after_flush = os.environ.get(
            "MATRIXARK_RUST_PROXY_ASYNC_VISIBILITY_PUBLISH_AFTER_FLUSH",
            "1",
        ).strip().lower() in {"1", "true", "yes"}
        self._rust_proxy_path = proxy_path
        self._rust_request_timeout_ms = request_timeout_ms
        self._rust_io_timeout_ms = io_timeout_ms
        self._rust_sdk_mode = sdk_mode
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
        self._matrixark_native_batch_append_available = True
        if self._rust_direct_cdylib_enabled:
            self._matrixark_append_write_path = "rust_direct_cdylib_matrixark_batch_append_records"
        else:
            self._matrixark_append_write_path = (
                "rust_direct_sdk_matrixark_batch_append_records"
                if sdk_mode in {"direct-sdk", "direct_sdk", "native-binding", "rust-direct"}
                else "rust_proxy_matrixark_batch_runtime_default"
            )
        self._matrixark_append_uses_per_record_hset = False
        self._matrixark_batch_append_uses_existing_batch_execute = True
        self._matrixark_batch_append_existing_batch_execute_source = "temporalstore_matrixark_batch_append_records"
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

    def _native_summary_client(self) -> Any:
        """Return a dedicated summary/audit lane when the backend transport needs one."""

        if (
            getattr(self, "_rust_direct_cdylib_enabled", False)
            or not getattr(self, "_dedicated_proxy_clients_enabled", False)
            or self._has_pending_visibility_keys()
            or self._visibility_publish_active()
        ):
            return self._client
        with self._summary_client_lock:
            if self._summary_client is None:
                self._summary_client = MatrixArkRustProxyClient(
                    proxy_path=self._rust_proxy_path,
                    metaserver=self._metaserver,
                    namespace=self._namespace,
                    table=self._table,
                    request_timeout_ms=self._rust_request_timeout_ms,
                    io_timeout_ms=self._rust_io_timeout_ms,
                    sdk_mode=self._rust_sdk_mode,
                )
            return self._summary_client

    def _append_client_for_records(self, records: list[Json]) -> Any:
        hot_record_types = {
            "context_event",
            "context_entity",
            "context_segment",
            "resource_chunk",
            "resource_manifest",
            "resource_registry",
            "skill_manifest",
            "skill_section",
            "skill_registry_update",
            "context_index",
        }
        summary_or_audit_types = {
            "context_child_ref",
            "context_embedding",
            "context_pack_audit",
            "context_summary",
            "context_summary_dirty",
            "context_summary_refresh_audit",
            "matrixark_audit_log",
        }
        record_types = {str(record.get("record_type") or "") for record in records if isinstance(record, dict)}
        if record_types and not (record_types & hot_record_types) and (record_types & summary_or_audit_types):
            return self._native_summary_client()
        return self._client

    def _native_retrieve_client(self) -> Any:
        """Return the native serving read client.

        Rust cdylib direct mode is in-process and does not need a proxy lane;
        the stdio proxy path keeps a dedicated read lane to avoid head-of-line
        blocking behind writes or audit flushes.
        """

        if (
            getattr(self, "_rust_direct_cdylib_enabled", False)
            or not getattr(self, "_dedicated_proxy_clients_enabled", False)
            or self._has_pending_visibility_keys()
            or self._visibility_publish_active()
        ):
            return self._client
        with self._retrieve_client_lock:
            if self._retrieve_client is None:
                self._retrieve_client = MatrixArkRustProxyClient(
                    proxy_path=self._rust_proxy_path,
                    metaserver=self._metaserver,
                    namespace=self._namespace,
                    table=self._table,
                    request_timeout_ms=self._rust_request_timeout_ms,
                    io_timeout_ms=self._rust_io_timeout_ms,
                    sdk_mode=self._rust_sdk_mode,
                )
                warmer = getattr(self._retrieve_client, "warm_lane_group", None)
                if callable(warmer):
                    warmer("pack")
            return self._retrieve_client

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        super().flush_direct_writes(timeout_s=timeout_s)
        if not (
            getattr(self, "_publish_visibility_after_flush", False)
            or (
                getattr(self, "_async_visibility_publish_after_flush", False)
                and getattr(self, "_track_pending_visibility_keys", False)
            )
        ):
            return
        visibility_keys = self._consume_pending_visibility_keys()
        if not visibility_keys:
            return
        if not getattr(self, "_publish_visibility_after_flush", False):
            self._start_async_visibility_publish(visibility_keys)
            return
        self._publish_visibility_keys(visibility_keys)

    def _publish_visibility_keys(self, visibility_keys: list[str]) -> None:
        publisher = getattr(self._client, "matrixark_publish_visibility", None)
        if callable(publisher):
            publisher(visibility_keys=visibility_keys)

    def _visibility_publish_active(self) -> bool:
        with self._visibility_publish_lock:
            thread = self._visibility_publish_thread
            return bool(thread is not None and thread.is_alive())

    def _start_async_visibility_publish(self, visibility_keys: list[str]) -> None:
        with self._visibility_publish_lock:
            thread = self._visibility_publish_thread
            if thread is not None and thread.is_alive():
                self._note_pending_visibility_keys(visibility_keys)
                return
            self._visibility_publish_error = None

            def publish() -> None:
                try:
                    self._publish_visibility_keys(visibility_keys)
                except Exception as exc:  # pragma: no cover - surfaced by read drain.
                    with self._visibility_publish_lock:
                        self._visibility_publish_error = exc

            thread = threading.Thread(target=publish, name="matrixark-rust-visibility-publisher", daemon=True)
            self._visibility_publish_thread = thread
            thread.start()

    def _drain_visibility_publish_for_read(self) -> None:
        while True:
            with self._visibility_publish_lock:
                thread = self._visibility_publish_thread
            if thread is not None:
                thread.join()
            with self._visibility_publish_lock:
                error = self._visibility_publish_error
                if error is not None:
                    self._visibility_publish_error = None
                    raise MatrixArkError(f"rust visibility publish failed before native read: {error}") from error
            visibility_keys = self._consume_pending_visibility_keys()
            if not visibility_keys:
                return
            self._publish_visibility_keys(visibility_keys)

    def supports_native_candidate_prefilter(self) -> bool:
        return True

    def supports_native_context_pack(self) -> bool:
        return True

    def native_context_pack(self, request: Json) -> Json | None:
        self._drain_visibility_publish_for_read()
        retriever = self._native_retrieve_client().matrixark_retrieve_context_pack
        try:
            response = retriever(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                request=request,
            )
        except Exception as exc:
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly failed for {self._backend_label()}: {exc}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        if not isinstance(response, dict) or not response.get("native_pack_assembly"):
            if self.native_context_pack_required():
                raise MatrixArkError(
                    f"backend-native ContextPack assembly returned an invalid response for {self._backend_label()}. "
                    "Python reference packing is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
            return None
        if isinstance(response.get("records"), list):
            raise MatrixArkError("native matrixark_retrieve_context_pack must return a finished ContextPack, not raw records")
        pack = response.get("context_pack")
        if not isinstance(pack, dict):
            return None
        pack.setdefault("context_pack_assembly", "native_backend")
        pack.setdefault("backend", self._backend_label())
        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
        contract = recall_policy.get("native_response_contract") if isinstance(recall_policy.get("native_response_contract"), dict) else {}
        contract.setdefault("raw_records_returned_to_python", False)
        contract.setdefault("python_hot_path_records", 0)
        contract.setdefault("python_role", "dispatch_request_receive_context_pack")
        contract.setdefault("backend_role", "scan_filter_score_pack")
        contract.setdefault("rust_proxy_dedicated_retrieve_lane", bool(getattr(self, "_dedicated_proxy_clients_enabled", False)))
        recall_policy["native_response_contract"] = contract
        pack["recall_policy"] = recall_policy
        return pack

    def _native_candidate_scan(
        self,
        *,
        scope: Json,
        record_types: set[str],
        secondary_index_groups: list[set[str]] | None,
        selected_node_hashes: set[int] | None,
    ) -> Json | None:
        try:
            self._drain_visibility_publish_for_read()
            response = self._native_retrieve_client().matrixark_scan_candidates(
                count_key=self._count_key,
                record_hash_key=self._record_hash_key,
                shard_size=self._shard_size,
                scope=scope,
                record_types=sorted(record_types),
                secondary_index_groups=[sorted(group) for group in (secondary_index_groups or [])],
                selected_node_hashes=sorted(int(item) for item in (selected_node_hashes or set())),
            )
        except Exception as exc:
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter failed for {self._backend_label()}: {exc}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                ) from exc
            return None
        records = response.get("records") if isinstance(response, dict) else None
        if not isinstance(records, list):
            if native_candidate_prefilter_required(backend_label=self._backend_label()):
                raise MatrixArkError(
                    f"backend-native candidate prefilter returned an invalid response for {self._backend_label()}. "
                    "Python read_all scan/prefilter is disabled for TemporalStore serving unless explicitly overridden for local debug."
                )
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
        scan_stats.setdefault("pack_assembly_location", "native_backend_candidate_scan")
        scan_stats.setdefault("rust_proxy_dedicated_retrieve_lane", True)
        latest_state_records = self._latest_context_state_records_for_candidate_scan(
            scope=scope,
            record_types=record_types,
            selected_node_hashes=selected_node_hashes,
        )
        if latest_state_records:
            records = list(records) + latest_state_records
        records = compact_latest_context_state_records(records)
        scan_stats["latest_summary_state_compaction"] = True
        scan_stats["latest_state_records_loaded"] = len(latest_state_records)
        return {"records": records, "scan_stats": scan_stats}

    def _backend_metaserver(self) -> str:
        return self._client.metaserver

    def _backend_label(self) -> str:
        return "temporalstore-rust"

    def _rust_storage_mode_label(self) -> str:
        sdk_mode = getattr(self._client, "sdk_mode", "")
        if sdk_mode == "direct_cdylib":
            return "rust-direct-cdylib"
        if sdk_mode == "direct_sdk":
            return "rust-direct-sdk-bridge"
        return "rust-proxy"

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
            f'matrixark_backend_info{{backend="{backend}",storage_mode="{storage_mode}"}} 1',
            "# HELP matrixark_backend_ready MatrixArk storage backend readiness, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{backend="{backend}",storage_mode="{storage_mode}",status="{"ready" if self._backend_ready else "unknown"}"}} {1 if self._backend_ready else 0}',
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
                "# HELP matrixark_backend_proxy_queue_wait_ms_total Total proxy lane queue wait time in milliseconds.",
                "# TYPE matrixark_backend_proxy_queue_wait_ms_total counter",
                f'matrixark_backend_proxy_queue_wait_ms_total{{backend="{backend}"}} {snapshot.get("proxy_queue_wait_ms_total", 0)}',
                "# HELP matrixark_backend_serialization_time_ms_total Total Rust proxy JSON serialization time in milliseconds.",
                "# TYPE matrixark_backend_serialization_time_ms_total counter",
                f'matrixark_backend_serialization_time_ms_total{{backend="{backend}"}} {snapshot.get("serialization_ms_total", 0)}',
                "# HELP matrixark_backend_rust_engine_time_ms_total Total Rust engine execution time in milliseconds.",
                "# TYPE matrixark_backend_rust_engine_time_ms_total counter",
                f'matrixark_backend_rust_engine_time_ms_total{{backend="{backend}"}} {snapshot.get("rust_engine_ms_total", 0)}',
                "# HELP matrixark_retrieve_scan_count_total Total records scanned by native MatrixArk retrieval.",
                "# TYPE matrixark_retrieve_scan_count_total counter",
                f'matrixark_retrieve_scan_count_total{{backend="{backend}"}} {int(snapshot.get("scan_count_total") or 0)}',
                "# HELP matrixark_retrieve_cache_hits_total Total native MatrixArk retrieval cache hits.",
                "# TYPE matrixark_retrieve_cache_hits_total counter",
                f'matrixark_retrieve_cache_hits_total{{backend="{backend}"}} {int(snapshot.get("cache_hits_total") or 0)}',
                "# HELP matrixark_context_pack_selected_refs_total Total refs selected by native ContextPack assembly.",
                "# TYPE matrixark_context_pack_selected_refs_total counter",
                f'matrixark_context_pack_selected_refs_total{{backend="{backend}"}} {int(snapshot.get("selected_refs_total") or 0)}',
                "# HELP matrixark_context_pack_dropped_refs_total Total refs dropped by native ContextPack assembly.",
                "# TYPE matrixark_context_pack_dropped_refs_total counter",
                f'matrixark_context_pack_dropped_refs_total{{backend="{backend}"}} {int(snapshot.get("dropped_refs_total") or 0)}',
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
        rust_retrieve_metrics: Json | None = None
        rust_summary_metrics: Json | None = None
        if self._retrieve_client is not None:
            rust_retrieve_metrics = self._retrieve_client.metrics_snapshot()
        if self._summary_client is not None:
            rust_summary_metrics = self._summary_client.metrics_snapshot()
        try:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + self._client.metrics_prometheus()
            if self._retrieve_client is not None:
                prometheus += self._retrieve_client.metrics_prometheus()
            if self._summary_client is not None:
                prometheus += self._summary_client.metrics_prometheus()
        except Exception as exc:
            prometheus = self._backend_neutral_prometheus(rust_client_metrics) + f"# matrixark_rust_proxy_metrics_error {json.dumps(str(exc))}\n"
        gateway_mode = str(rust_client_metrics.get("gateway_mode") or "rust_proxy")
        proxy_mode = str(rust_client_metrics.get("proxy_mode") or "rust_proxy_stdio")
        sdk_mode = str(rust_client_metrics.get("sdk_mode") or getattr(self._client, "sdk_mode", "proxy"))
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "gateway_mode": gateway_mode,
            "sdk_mode": sdk_mode,
            "production_path": "rust_native_proxy" if not self._rust_direct_cdylib_enabled else "rust_cpp_c_api_bridge_diagnostic",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only",
            "direct_sdk_bridge": False,
            "cpp_c_api_bridge_diagnostic": bool(self._rust_direct_cdylib_enabled),
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
                "dedicated_retrieve_lane": True,
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
                "rust_write_client": rust_client_metrics,
                "rust_retrieve_client": rust_retrieve_metrics,
                "rust_summary_client": rust_summary_metrics,
                "rust_proxy_lanes": {
                    "write": not self._rust_direct_cdylib_enabled,
                    "retrieve": self._retrieve_client is not None,
                    "summary_audit": self._summary_client is not None,
                    "summary_audit_share_write_lane": False,
                    "retrieve_isolated_from_summary_audit": True,
                    "ingest_isolated_from_summary_audit": True,
                    "direct_cdylib_enabled": self._rust_direct_cdylib_enabled,
                    "transport": rust_client_metrics.get("transport", "rust_proxy_stdio"),
                },
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
