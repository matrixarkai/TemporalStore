#!/usr/bin/env python3
"""TemporalStore-backed MatrixArk adapters for C++ and Rust backends."""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_mcp_direct_cache_state import (
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_direct_cache_state import (
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_LOCK,
        _DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE_MAX_ENTRIES,
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_LOCK,
        _DIRECT_RETRIEVAL_CANDIDATE_CACHE_MAX_ENTRIES,
    )

try:
    from tools.matrixark_mcp_core import (
        DIRECT_AUDIT_BUFFER_MAX_RECORDS,
        DIRECT_AUDIT_FLUSH_INTERVAL_MS,
        DIRECT_AUDIT_MODE,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        DIRECT_WRITE_BACKOFF_MS,
        DIRECT_WRITE_RETRIES,
        DIRECT_WRITE_THROTTLE_MS,
        Json,
        MatrixArkError,
        candidate_access_scope,
        compact_latest_context_state_records,
        native_candidate_prefilter_required,
        python_hot_cache_allowed,
        scope_matches,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DIRECT_AUDIT_BUFFER_MAX_RECORDS,
        DIRECT_AUDIT_FLUSH_INTERVAL_MS,
        DIRECT_AUDIT_MODE,
        DIRECT_RECORD_LOG_SHARD_SIZE,
        DIRECT_WRITE_BACKOFF_MS,
        DIRECT_WRITE_RETRIES,
        DIRECT_WRITE_THROTTLE_MS,
        Json,
        MatrixArkError,
        candidate_access_scope,
        compact_latest_context_state_records,
        native_candidate_prefilter_required,
        python_hot_cache_allowed,
        scope_matches,
    )

try:
    from tools.matrixark_mcp_env import env_bool, env_int, env_lower
    from tools.matrixark_mcp_backend_metric_state import (
        initialize_backend_metric_state,
    )
    from tools.matrixark_mcp_backend_metrics import BackendMetricsAdapterMixin
    from tools.matrixark_mcp_temporal_audit import TemporalAuditAdapterMixin
    from tools.matrixark_mcp_temporal_direct_cache import TemporalDirectCacheAdapterMixin
    from tools.matrixark_mcp_temporal_write_support import TemporalWriteSupportAdapterMixin
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
    from matrixark_mcp_backend_metrics import BackendMetricsAdapterMixin
    from matrixark_mcp_temporal_audit import TemporalAuditAdapterMixin
    from matrixark_mcp_temporal_direct_cache import TemporalDirectCacheAdapterMixin
    from matrixark_mcp_temporal_write_support import TemporalWriteSupportAdapterMixin
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
    BackendMetricsAdapterMixin,
    LatestContextStateAdapterMixin,
    NativeSideIndexAdapterMixin,
    RawIngestionAdapterMixin,
    DirectWriteQueueAdapterMixin,
    TemporalAuditAdapterMixin,
    TemporalDirectCacheAdapterMixin,
    TemporalWriteSupportAdapterMixin,
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

    def _backend_label(self) -> str:
        return "temporalstore-cpp-proxy" if getattr(self, "_matrixark_proxy_mode", False) else "temporalstore-cpp"

    def python_hot_cache_enabled(self) -> bool:
        return python_hot_cache_allowed(backend_label=self._backend_label())

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
