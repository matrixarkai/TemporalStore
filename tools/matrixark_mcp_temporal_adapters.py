#!/usr/bin/env python3
"""TemporalStore-backed MatrixArk adapters for C++ and Rust backends."""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        DIRECT_AUDIT_BUFFER_MAX_RECORDS,
        DIRECT_AUDIT_FLUSH_INTERVAL_MS,
        DIRECT_AUDIT_MODE,
        DIRECT_RECORD_BUNDLE_MAX_BYTES,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        DIRECT_WRITE_BACKOFF_MS,
        DIRECT_WRITE_RETRIES,
        DIRECT_WRITE_THROTTLE_MS,
        Json,
        MatrixArkError,
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
        candidate_access_scope,
        compact_latest_context_state_records,
        context_event_time_index_entries,
        context_event_time_index_field,
        context_event_time_index_key,
        context_event_time_index_payload,
        context_event_timestamp_ms,
        materialize_serving_record_batch,
        materialize_serving_records,
        native_candidate_prefilter_required,
        python_hot_cache_allowed,
        scope_matches,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DIRECT_AUDIT_BUFFER_MAX_RECORDS,
        DIRECT_AUDIT_FLUSH_INTERVAL_MS,
        DIRECT_AUDIT_MODE,
        DIRECT_RECORD_BUNDLE_MAX_BYTES,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        DIRECT_WRITE_BACKOFF_MS,
        DIRECT_WRITE_RETRIES,
        DIRECT_WRITE_THROTTLE_MS,
        Json,
        MatrixArkError,
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
        candidate_access_scope,
        compact_latest_context_state_records,
        context_event_time_index_entries,
        context_event_time_index_field,
        context_event_time_index_key,
        context_event_time_index_payload,
        context_event_timestamp_ms,
        materialize_serving_record_batch,
        materialize_serving_records,
        native_candidate_prefilter_required,
        python_hot_cache_allowed,
        scope_matches,
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
        DirectWriteQueueAdapterMixin,
    )
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_latest_context_state import (
        LatestContextStateAdapterMixin,
    )
    from tools.matrixark_mcp_native_side_index import (
        NativeSideIndexAdapterMixin,
    )
    from tools import matrixark_mcp_native_pack_runtime as native_pack_runtime
    from tools import matrixark_mcp_native_lookup_runtime as native_lookup_runtime
    from tools import matrixark_mcp_temporal_retrieval_records as temporal_retrieval_record_runtime
    from tools import matrixark_mcp_temporal_record_load_runtime as temporal_record_load_runtime
    from tools import matrixark_mcp_temporal_readiness as temporal_readiness
    from tools import matrixark_mcp_temporal_proxy_readiness as temporal_proxy_readiness
    from tools.matrixark_mcp_raw_ingestion import (
        RawIngestionAdapterMixin,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_env import env_bool, env_int, env_lower
    from matrixark_mcp_backend_metric_state import (
        initialize_backend_metric_state,
    )
    import matrixark_mcp_backend_metrics as backend_metrics_helpers
    import matrixark_mcp_direct_cache as direct_cache_helpers
    import matrixark_mcp_temporal_append as temporal_append_helpers
    import matrixark_mcp_native_lookup_runtime as native_lookup_runtime
    import matrixark_mcp_temporal_readiness as temporal_readiness
    import matrixark_mcp_temporal_proxy_readiness as temporal_proxy_readiness
    import matrixark_mcp_temporal_record_load_runtime as temporal_record_load_runtime
    from matrixark_mcp_direct_write_queue import (
        DirectWriteQueueAdapterMixin,
    )
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_local_adapter import RETRIEVAL_HOT_RECORD_TYPES
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_latest_context_state import (
        LatestContextStateAdapterMixin,
    )
    from matrixark_mcp_native_side_index import (
        NativeSideIndexAdapterMixin,
    )
    from matrixark_mcp_raw_ingestion import (
        RawIngestionAdapterMixin,
    )


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


class MatrixArkTemporalStoreDirectAdapter(
    LatestContextStateAdapterMixin,
    NativeSideIndexAdapterMixin,
    RawIngestionAdapterMixin,
    DirectWriteQueueAdapterMixin,
    MatrixArkLocalAdapter,
):
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
        return temporal_proxy_readiness.ensure_backend_ready(
            self,
            reason=reason,
            probe=probe,
            timeout_ms=timeout_ms,
        )

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
        return temporal_readiness.readiness_failure_result(
            self,
            reason=reason,
            probe=probe,
            attempts=attempts,
            attempt_log=attempt_log,
            error=error,
            checks=checks,
            metaserver=metaserver,
            warmup_key=warmup_key,
            warmup_field=warmup_field,
        )

    def _run_backend_readiness_gate(
        self,
        *,
        reason: str,
        probe: bool = True,
        timeout_ms: int | None = None,
    ) -> Json:
        return temporal_readiness.run_backend_readiness_gate(
            self,
            reason=reason,
            probe=probe,
            timeout_ms=timeout_ms,
        )

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
        return temporal_retrieval_record_runtime.retrieval_records(
            self,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
        )

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
        return native_pack_runtime.native_context_pack(self, request)

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
        return native_lookup_runtime.native_index_ref_hashes(
            self,
            scope=scope,
            secondary_index_groups=secondary_index_groups,
        )

    def _native_locations_for_refs(self, ref_hashes: set[int]) -> Json:
        return native_lookup_runtime.native_locations_for_refs(self, ref_hashes)

    def _load_records_from_locations(self, locations: list[Json]) -> list[Json]:
        return native_lookup_runtime.load_records_from_locations(self, locations)

    def _native_context_pack_fallback_blocker(self, args: Json, *, reason: str) -> Json:
        return native_pack_runtime.native_context_pack_fallback_blocker(self, args, reason=reason)

    def _try_native_context_pack(self, args: Json) -> Json | None:
        return native_pack_runtime.try_native_context_pack(self, args)

    def retrieve(self, args: Json) -> Json:
        native_pack = self._try_native_context_pack(args)
        if native_pack is not None:
            return native_pack
        return super().retrieve(args)

    def _native_locations_for_selected_nodes(self, *, scope: Json, selected_node_hashes: set[int]) -> Json:
        return temporal_retrieval_record_runtime.native_locations_for_selected_nodes(
            self,
            scope=scope,
            selected_node_hashes=selected_node_hashes,
        )

    def _filter_retrieval_candidates(
        self,
        records: list[Json],
        *,
        scope: Json,
        allowed_types: set[str],
        selected_nodes: set[int],
    ) -> tuple[list[Json], Json]:
        return temporal_retrieval_record_runtime.filter_retrieval_candidates(
            self,
            records,
            scope=scope,
            allowed_types=allowed_types,
            selected_nodes=selected_nodes,
        )

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
        allow_broad_scan_fallback: bool | None = None,
    ) -> Json:
        return temporal_retrieval_record_runtime.retrieval_records(
            self,
            scope=scope,
            record_types=record_types,
            secondary_index_groups=secondary_index_groups,
            selected_node_hashes=selected_node_hashes,
            allow_broad_scan_fallback=allow_broad_scan_fallback,
        )

    def _load_records_by_count(self, count: int) -> list[Json]:
        return temporal_record_load_runtime.load_records_by_count(self, count)

    def _load_records_by_native_shard_scan(self, count: int) -> list[Json] | None:
        return temporal_record_load_runtime.load_records_by_native_shard_scan(self, count)

    def _record_location(self, sequence: int) -> tuple[str, str]:
        return temporal_record_load_runtime.record_location(self, sequence)

    def _load_records(self, index: list[str]) -> list[Json]:
        return temporal_record_load_runtime.load_records(self, index)


try:
    from tools.matrixark_mcp_temporal_rust_adapters import (
        MatrixArkRustCliClient,
        MatrixArkRustCdylibClient,
        MatrixArkRustCliClient,
        MatrixArkRustProxyClient,
        MatrixArkTemporalStoreRustAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_temporal_rust_adapters import (
        MatrixArkRustCliClient,
        MatrixArkRustCdylibClient,
        MatrixArkRustCliClient,
        MatrixArkRustProxyClient,
        MatrixArkTemporalStoreRustAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
    )
