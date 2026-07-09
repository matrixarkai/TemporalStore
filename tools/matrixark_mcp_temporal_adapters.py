#!/usr/bin/env python3
"""TemporalStore-backed MatrixArk adapters for C++ and Rust backends."""

from __future__ import annotations

import queue
from collections import OrderedDict, defaultdict, deque

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


def _float_metric_or_default(metrics: dict[str, Any], name: str, default: float = 0.0) -> float:
    if name not in metrics or metrics.get(name) is None:
        return float(default)
    try:
        return float(metrics.get(name))
    except (TypeError, ValueError):
        return float(default)



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


def _selected_ref_class(ref: Json) -> str:
    raw = str(ref.get("context_class") or ref.get("ref_type") or ref.get("type") or "").lower()
    if "entity" in raw:
        return "entity"
    if "segment" in raw:
        return "segment"
    if "summary" in raw:
        return "summary"
    if "resource" in raw or "chunk" in raw:
        return "resource"
    if "skill" in raw:
        return "skill"
    if "event" in raw:
        return "event"
    return raw or "ref"


def _selected_ref_stable_key(ref: Json) -> str:
    ref_class = _selected_ref_class(ref)
    stable_id = (
        ref.get("source_ref")
        or ref.get("context_event_key")
        or ref.get("summary_key")
        or ref.get("entity_name")
        or ref.get("resource_id")
        or ref.get("skill_id")
        or ref.get("ref_hash")
        or ref.get("event_id_hash")
        or ref.get("entity_hash")
        or ref.get("chunk_hash")
    )
    if stable_id is not None:
        return f"{ref_class}:{stable_id}"
    text = str(ref.get("text") or ref.get("summary_text") or ref.get("state") or "")
    return f"{ref_class}:text:{stable_hash(text)}"


def _compact_native_selected_refs(selected_refs: list[Json], *, max_total: int = 4, max_text_chars: int = 480) -> tuple[list[Json], int]:
    """Deduplicate and cap already-selected native refs without Python scans."""

    if not selected_refs:
        return [], 0
    per_class_limit = {
        "entity": 1,
        "event": 1,
        "segment": 1,
        "summary": 1,
        "resource": 1,
        "skill": 1,
        "ref": 1,
    }
    selected: list[Json] = []
    seen: set[str] = set()
    class_counts: dict[str, int] = {}
    dropped = 0
    for ref in selected_refs:
        if not isinstance(ref, dict):
            dropped += 1
            continue
        ref_class = _selected_ref_class(ref)
        key = _selected_ref_stable_key(ref)
        limit = per_class_limit.get(ref_class, 1)
        if key in seen or class_counts.get(ref_class, 0) >= limit or len(selected) >= max_total:
            dropped += 1
            continue
        normalized = dict(ref)
        normalized.setdefault("context_class", ref_class)
        text = normalized.get("text")
        if isinstance(text, str) and len(text) > max_text_chars:
            normalized["text"] = text[: max(0, max_text_chars - 1)].rstrip() + "..."
            normalized["token_estimate"] = max(1, (len(str(normalized["text"])) + 3) // 4)
        selected.append(normalized)
        seen.add(key)
        class_counts[ref_class] = class_counts.get(ref_class, 0) + 1
    return selected, dropped



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
        self._direct_write_queue_enabled = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_write_queue_max_records = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MAX_RECORDS", "10000")))
        self._direct_write_queue_put_timeout_s = max(0.01, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_PUT_TIMEOUT_MS", "1000")) / 1000.0)
        self._direct_write_queue_mode = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_MODE", "memory").strip().lower() or "memory"
        if self._direct_write_queue_mode not in {"memory", "temporalstore"}:
            raise MatrixArkError("MATRIXARK_DIRECT_WRITE_QUEUE_MODE must be memory or temporalstore")
        self._direct_write_queue_drain_max_batches = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES", "64")))
        self._direct_write_queue_allow_sync_context = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_write_queue_autostart = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART", "1").strip().lower() not in {"0", "false", "no"}
        self._native_side_index_assume_fresh = os.environ.get("MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH", "0").strip().lower() in {"1", "true", "yes"}
        self._direct_raw_ingestion_queue_enabled = os.environ.get("MATRIXARK_DIRECT_RAW_INGESTION_QUEUE", "0").strip().lower() in {"1", "true", "yes"}
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
        return "temporalstore-cpp-proxy" if getattr(self, "_matrixark_proxy_mode", False) else "temporalstore-cpp"

    def python_hot_cache_enabled(self) -> bool:
        return python_hot_cache_allowed(backend_label=self._backend_label())

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
        if not hasattr(self, "_direct_write_queue_drain_max_batches"):
            self._direct_write_queue_drain_max_batches = max(1, int(os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_DRAIN_MAX_BATCHES", "64")))
        if not hasattr(self, "_direct_write_queue_allow_sync_context"):
            self._direct_write_queue_allow_sync_context = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_ALLOW_SYNC_CONTEXT", "0").strip().lower() in {"1", "true", "yes"}
        if not hasattr(self, "_direct_write_queue_autostart"):
            self._direct_write_queue_autostart = os.environ.get("MATRIXARK_DIRECT_WRITE_QUEUE_AUTOSTART", "1").strip().lower() not in {"0", "false", "no"}
        if not hasattr(self, "_native_side_index_assume_fresh"):
            self._native_side_index_assume_fresh = os.environ.get("MATRIXARK_NATIVE_SIDE_INDEX_ASSUME_FRESH", "0").strip().lower() in {"1", "true", "yes"}
        if not hasattr(self, "_direct_raw_ingestion_queue_enabled"):
            self._direct_raw_ingestion_queue_enabled = os.environ.get("MATRIXARK_DIRECT_RAW_INGESTION_QUEUE", "0").strip().lower() in {"1", "true", "yes"}
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
                "mode": "cpp-proxy" if getattr(self, "_matrixark_proxy_mode", False) else "direct-sdk",
                "cpp_proxy_endpoint": getattr(self, "_cpp_proxy_endpoint", ""),
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
        self._ensure_raw_ingestion_fields()
        if self._raw_ingestion_prefix == self._storage_prefix:
            raise MatrixArkError("MATRIXARK_DIRECT_RAW_STORAGE_PREFIX must differ from the serving storage prefix")
        if (
            allow_queue
            and bool(getattr(self, "_direct_raw_ingestion_queue_enabled", False))
            and bool(getattr(self, "_direct_write_queue_enabled", False))
            and getattr(self, "_direct_write_queue_mode", "memory") == "memory"
        ):
            self._enqueue_direct_write_item({"queue_mode": "raw_ingestion", "records": list(records)}, len(records))
            return
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
            if self._raw_ingestion_visibility_required_after_flush():
                self._note_pending_visibility_keys(
                    [self._raw_count_key] + [str(entry.get("key") or "") for entry in entries]
                )
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
            existing_value = self._read_hash_value_best_effort(key, field)
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
        enriched = attach_context_event_time_key(record)
        parent_type = str(enriched.get("context_event_parent_type") or "context_node")
        parent_hash = enriched.get("context_event_parent_hash") or 0
        return f"{self._storage_prefix}:context_event_by_ingestion_time:{parent_type}:{parent_hash}"

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
            enriched = attach_context_event_time_key(record)
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
            payload_record = dict(record)
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
            payload_record = dict(record)
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

    def _direct_write_durable_field(self, payload: Json) -> str:
        digest = stable_hash(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return f"{int(payload.get('created_at_ms') or now_ms()):020d}:{digest}"

    def _enqueue_direct_write_durable(self, records: list[Json]) -> str:
        payload = self._direct_write_durable_payload(list(records))
        field = self._direct_write_durable_field(payload)
        self._hset_with_backoff(self._direct_write_queue_key, field, json.dumps(payload, separators=(",", ":")))
        return field

    def _enqueue_direct_write_item(self, item: Any, record_count: int) -> None:
        self._ensure_direct_write_queue_fields()
        if bool(getattr(self, "_direct_write_queue_autostart", True)):
            self._start_direct_write_worker()
        wait_started_perf = time.perf_counter()
        try:
            self._direct_write_queue.put(item, timeout=self._direct_write_queue_put_timeout_s)
        except queue.Full as exc:
            self._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
            if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
                _mcp_debug_log("matrixark durable direct write queue accepted batch but local worker queue is full; batch will be recovered by drain")
                self._direct_write_enqueued_records += record_count
                self._direct_write_enqueued_batches += 1
                return
            raise MatrixArkError("direct TemporalStore write queue is full") from exc
        self._observe_append_queue_wait((time.perf_counter() - wait_started_perf) * 1000.0)
        self._direct_write_enqueued_records += record_count
        self._direct_write_enqueued_batches += 1

    def _enqueue_direct_write(self, records: list[Json]) -> None:
        item: Any = list(records)
        if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
            item = {"queue_mode": "temporalstore", "field": self._enqueue_direct_write_durable(records)}
        self._enqueue_direct_write_item(item, len(records))

    def _direct_write_loop(self) -> None:
        while not self._direct_write_stop.is_set():
            try:
                first = self._direct_write_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            items = [first]
            max_batches = max(1, int(getattr(self, "_direct_write_queue_drain_max_batches", 64) or 64))
            while len(items) < max_batches:
                try:
                    items.append(self._direct_write_queue.get_nowait())
                except queue.Empty:
                    break
            try:
                flushed = self._flush_direct_write_items(items)
                self._direct_write_flushed_records += flushed
                self._direct_write_flushed_batches += len(items)
            except Exception as exc:
                self._direct_write_failures += 1
                _mcp_debug_log(f"matrixark direct write queue flush failed: {exc}")
            finally:
                for _item in items:
                    try:
                        self._direct_write_queue.task_done()
                    except Exception:
                        pass

    def _flush_direct_write_items(self, items: list[Any]) -> int:
        memory_records: list[Json] = []
        raw_ingestion_records: list[Json] = []
        flushed = 0
        for item in items:
            if isinstance(item, dict) and item.get("queue_mode") == "temporalstore":
                flushed += self._flush_direct_write_durable_field(str(item.get("field") or ""))
            elif isinstance(item, dict) and item.get("queue_mode") == "raw_ingestion":
                rows = item.get("records")
                if isinstance(rows, list):
                    raw_ingestion_records.extend(row for row in rows if isinstance(row, dict))
            elif isinstance(item, list):
                memory_records.extend(row for row in item if isinstance(row, dict))
            else:
                raise MatrixArkError("unknown direct write queue item")
        if raw_ingestion_records:
            self._append_raw_ingestion_records(raw_ingestion_records, allow_queue=False)
            flushed += len(raw_ingestion_records)
        if memory_records:
            self._append_many_materialized(memory_records, allow_queue=False)
            flushed += len(memory_records)
        return flushed

    def _flush_direct_write_item(self, item: Any) -> int:
        return self._flush_direct_write_items([item])

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
        self._start_direct_write_worker()
        if getattr(self, "_direct_write_queue_mode", "memory") == "temporalstore":
            self.drain_durable_direct_write_queue()
        deadline = time.monotonic() + float(timeout_s if timeout_s is not None else 30.0)
        while self._direct_write_queue.unfinished_tasks:
            if time.monotonic() >= deadline:
                raise MatrixArkError("timed out waiting for direct TemporalStore write queue to drain")
            time.sleep(0.01)

    def _append_client_for_records(self, records: list[Json]) -> Any:
        return self._client

    def _materialize_appended_records_locked(
        self,
        *,
        prior_entry_count: int,
        new_entry_count: int,
        records: list[Json],
    ) -> None:
        """Refresh process-local materialized views after native latest-state writes.

        Some compact context records are written as latest-state HSet entries
        rather than append-log entries. Resource/skill list and retrieval paths
        still need those records visible in the adapter's parsed caches during
        the current process, without forcing the hot write path back through the
        legacy full record log.
        """
        if not records:
            return
        try:
            self._entry_count_cache = max(int(new_entry_count or 0), int(prior_entry_count or 0))
        except Exception:
            pass
        if getattr(self, "_records_cache", None) is not None:
            try:
                self._records_cache.extend(records)
                self._put_direct_record_cache(len(self._records_cache), self._records_cache)
            except Exception:
                pass
        try:
            self._prune_retrieval_candidate_cache(getattr(self, "_entry_count_cache", None) or int(new_entry_count or 0))
        except Exception:
            pass
        try:
            self._update_latest_entity_cache(records)
        except Exception:
            pass

    def _append_many_materialized(self, records: list[Json], *, allow_queue: bool = True) -> None:
        if not records:
            return
        records = compact_latest_context_state_records(records)
        latest_state_entries, append_records_for_log = self._split_compacted_latest_context_state(records)
        self._validate_storage_routes_available(records)
        if latest_state_entries and not append_records_for_log:
            self._hset_many_with_backoff(latest_state_entries)
            self._materialize_appended_records_locked(
                prior_entry_count=getattr(self, "_entry_count_cache", None) or self._get_count(),
                new_entry_count=getattr(self, "_entry_count_cache", None) or self._get_count(),
                records=records,
            )
            return
        records_to_append = append_records_for_log
        if allow_queue and self._records_can_use_direct_write_queue(records_to_append):
            self._enqueue_direct_write(records)
            return
        started_perf = time.perf_counter()
        with self._records_lock:
            entry_count_cache = getattr(self, "_entry_count_cache", None)
            count = entry_count_cache if entry_count_cache is not None else self._get_count()
            if count <= 0 and self._index_cache is None:
                self._index_cache = self._get_index()
                self._legacy_index_mode = bool(self._index_cache)
            event_time_entries = self._context_event_time_index_entries(records_to_append)
            if self._legacy_index_mode:
                if self._index_cache is None:
                    self._index_cache = self._get_index()
                entries: list[Json] = []
                for record in records_to_append:
                    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
                    record_id = (
                        f"{len(self._index_cache):020d}:"
                        f"{record.get('record_type', 'record')}:"
                        f"{stable_hash(json.dumps(record, sort_keys=True))}"
                    )
                    route = record.get("storage_route") if isinstance(record.get("storage_route"), dict) else {}
                    entries.append({"key": self._record_hash_key, "field": record_id, "value": payload, "storage_route": route})
                    self._index_cache.append(record_id)
                self._hset_many_with_backoff(latest_state_entries + event_time_entries + entries)
                self._put_string_with_backoff(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
                self._note_pending_visibility_keys(
                    [self._index_key]
                    + [str(entry.get("key") or "") for entry in latest_state_entries]
                    + [str(entry.get("key") or "") for entry in event_time_entries]
                    + [str(entry.get("key") or "") for entry in entries]
                )
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
            self._note_pending_visibility_keys(
                [self._count_key]
                + [str(entry.get("key") or "") for entry in latest_state_entries]
                + [str(entry.get("key") or "") for entry in event_time_entries]
                + [str(entry.get("key") or "") for entry in native_index_entries]
                + [str(entry.get("key") or "") for entry in entries]
            )
            self._entry_count_cache = sequence
            if self._records_cache is not None:
                self._records_cache.extend(records)
                self._put_direct_record_cache(self._entry_count_cache, self._records_cache)
            self._prune_retrieval_candidate_cache(sequence)
            self._update_latest_entity_cache(records)
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            self._observe_append_engine(elapsed_ms)
            self._observe_backend_command(elapsed_ms, records_written=len(records))

    def _note_pending_visibility_keys(self, keys: Iterable[str]) -> None:
        if not getattr(self, "_publish_visibility_after_flush", False):
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
        with _DIRECT_RECORD_CACHE_LOCK:
            lock = _DIRECT_RECORD_LOAD_LOCKS.get(self._storage_prefix)
            if lock is None:
                lock = threading.RLock()
                _DIRECT_RECORD_LOAD_LOCKS[self._storage_prefix] = lock
            return lock

    def _get_direct_record_cache(self, count: int) -> list[Json] | None:
        if not self.python_hot_cache_enabled():
            return None
        with _DIRECT_RECORD_CACHE_LOCK:
            cached = _DIRECT_RECORD_CACHE.get(self._storage_prefix)
            if cached is None:
                return None
            cached_count, records = cached
            if cached_count != count:
                return None
            return list(records)

    def _put_direct_record_cache(self, count: int, records: list[Json]) -> None:
        if not self.python_hot_cache_enabled():
            return
        with _DIRECT_RECORD_CACHE_LOCK:
            if len(_DIRECT_RECORD_CACHE) >= _DIRECT_RECORD_CACHE_MAX_PREFIXES and self._storage_prefix not in _DIRECT_RECORD_CACHE:
                oldest = next(iter(_DIRECT_RECORD_CACHE))
                _DIRECT_RECORD_CACHE.pop(oldest, None)
            _DIRECT_RECORD_CACHE[self._storage_prefix] = (count, list(records))

    def _drop_direct_record_cache(self) -> None:
        self._entry_count_cache = None
        self._records_cache = None
        self._index_cache = None
        with _DIRECT_RECORD_CACHE_LOCK:
            _DIRECT_RECORD_CACHE.pop(self._storage_prefix, None)
        with self._retrieval_candidate_cache_lock:
            self._retrieval_candidate_cache.clear()

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
        if not self.supports_native_context_pack():
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
            response = self.native_context_pack(request)
            if response is None:
                if not native_retrieve_fallback_allowed(args):
                    return self._native_context_pack_fallback_blocker(args, reason="native_context_pack_unavailable")
                return None
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



class MatrixArkRustCdylibClient:
    """In-process Rust direct SDK binding loaded through the Rust cdylib C ABI."""

    def __init__(
        self,
        *,
        library_path: str,
        temporalstore_lib: str = "",
        metaserver: str,
        namespace: str,
        table: str,
        request_timeout_ms: int,
        io_timeout_ms: int,
    ) -> None:
        import ctypes
        import json as _json

        if not library_path:
            raise MatrixArkError("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB is required for Rust direct cdylib mode")
        self.library_path = library_path
        self.metaserver = metaserver
        self.namespace = namespace
        self.table = table
        self.request_timeout_ms = request_timeout_ms
        self.io_timeout_ms = io_timeout_ms
        self.sdk_mode = "direct_cdylib"
        self._ctypes = ctypes
        load_mode = getattr(ctypes, "RTLD_GLOBAL", None)
        if temporalstore_lib:
            try:
                ctypes.CDLL(temporalstore_lib, mode=load_mode) if load_mode is not None else ctypes.CDLL(temporalstore_lib)
            except OSError:
                pass
        self._lib = ctypes.CDLL(library_path, mode=load_mode) if load_mode is not None else ctypes.CDLL(library_path)
        self._bind()
        self._handle = ctypes.c_void_p()
        self._commands_total = 0
        self._commands_failed_total = 0
        self._records_written_total = 0
        self._records_read_total = 0
        self._latency_samples_ms: list[float] = []
        options = {
            "metaserver_addr": metaserver,
            "namespace_name": namespace,
            "table_name": table,
            "request_timeout_ms": request_timeout_ms,
            "io_timeout_ms": io_timeout_ms,
            "connect_timeout_ms": min(request_timeout_ms, io_timeout_ms),
            "max_read_retries": 2,
            "max_write_retries": 1,
            "retry_backoff_ms": 2,
            "pin_primary": True,
        }
        error = ctypes.c_void_p()
        code = self._lib.temporalstore_rust_connect_json(
            _json.dumps(options, separators=(",", ":")).encode("utf-8"),
            ctypes.byref(self._handle),
            ctypes.byref(error),
        )
        self._check(code, error)

    def _bind(self) -> None:
        c = self._ctypes
        lib = self._lib
        lib.temporalstore_rust_free_string.argtypes = [c.c_void_p]
        lib.temporalstore_rust_free_string.restype = None
        lib.temporalstore_rust_connect_json.argtypes = [c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_connect_json.restype = c.c_int
        lib.temporalstore_rust_close.argtypes = [c.c_void_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_close.restype = c.c_int
        lib.temporalstore_rust_hset.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hset.restype = c.c_int
        lib.temporalstore_rust_hget.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hget.restype = c.c_int
        lib.temporalstore_rust_hgetall_json.argtypes = [c.c_void_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_hgetall_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_batch_append_records_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_batch_append_records_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_scan_candidates_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_size_t, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_scan_candidates_json.restype = c.c_int
        lib.temporalstore_rust_matrixark_retrieve_context_pack_json.argtypes = [c.c_void_p, c.c_char_p, c.c_char_p, c.c_size_t, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(c.c_void_p)]
        lib.temporalstore_rust_matrixark_retrieve_context_pack_json.restype = c.c_int

    def _decode_owned(self, value: Any) -> str:
        try:
            return self._ctypes.cast(value, self._ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            self._lib.temporalstore_rust_free_string(value)

    def _check(self, code: int, error: Any) -> None:
        if code == 0:
            return
        message = "unknown Rust TemporalStore direct binding error"
        if error:
            message = self._decode_owned(error)
        raise MatrixArkError(message)

    def _call(self, op: str, fn: Any, *, records_written: int = 0, records_read: int = 0) -> Any:
        started = time.perf_counter()
        self._commands_total += 1
        try:
            result = fn()
        except Exception:
            self._commands_failed_total += 1
            raise
        finally:
            self._latency_samples_ms.append((time.perf_counter() - started) * 1000.0)
            if len(self._latency_samples_ms) > 2048:
                self._latency_samples_ms = self._latency_samples_ms[-2048:]
        self._records_written_total += records_written
        self._records_read_total += records_read
        return result

    def close(self) -> None:
        if not getattr(self, "_handle", None):
            return
        error = self._ctypes.c_void_p()
        code = self._lib.temporalstore_rust_close(self._handle, self._ctypes.byref(error))
        self._handle = self._ctypes.c_void_p()
        self._check(code, error)

    def put_string(self, key: str, value: str) -> None:
        # MatrixArk direct serving should use batch append; keep this for compatibility through hset-style paths.
        self.hset(key, "", value)

    def get_string(self, key: str) -> str:
        return self.hget(key, "")

    def hset(self, key: str, field: str, value: str) -> None:
        def call() -> None:
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hset(self._handle, key.encode(), field.encode(), value.encode(), self._ctypes.byref(error))
            self._check(code, error)
        self._call("hset", call, records_written=1)

    def hget(self, key: str, field: str) -> str:
        def call() -> str:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hget(self._handle, key.encode(), field.encode(), self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return self._decode_owned(out)
        return self._call("hget", call, records_read=1)

    def hgetall(self, key: str) -> list[Json]:
        return list(self.scan_hash(key).get("records", []))

    def scan_hash(self, key: str) -> Json:
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_hgetall_json(self._handle, key.encode(), self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        result = self._call("scan_hash", call)
        self._records_read_total += int(result.get("count") or 0)
        return result

    def batch_hset(self, entries: list[Json]) -> None:
        self.matrixark_batch_append_records(entries)

    def matrixark_batch_append_records(
        self,
        entries: list[Json],
        *,
        count_key: str | None = None,
        count_value: str | None = None,
        append_options: Json | None = None,
    ) -> None:
        values = [{"key": str(entry.get("key") or ""), "field": str(entry.get("field") or ""), "value": str(entry.get("value") or "")} for entry in entries]
        payload = json.dumps(values, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> None:
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_batch_append_records_json(
                self._handle,
                payload,
                (count_key or "").encode("utf-8"),
                (count_value or "").encode("utf-8"),
                self._ctypes.byref(error),
            )
            self._check(code, error)
        self._call("matrixark_batch_append_records", call, records_written=len(values) + (1 if count_key else 0))

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

    def matrixark_scan_candidates(self, *, count_key: str, record_hash_key: str, shard_size: int, scope: Json, record_types: list[str], secondary_index_groups: list[list[str]], selected_node_hashes: list[int]) -> Json:
        request = json.dumps({"scope": scope, "record_types": record_types, "secondary_index_groups": secondary_index_groups, "selected_node_hashes": selected_node_hashes}, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_scan_candidates_json(self._handle, count_key.encode(), record_hash_key.encode(), int(shard_size), request, self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        return self._call("matrixark_scan_candidates", call)

    def matrixark_retrieve_context_pack(self, *, count_key: str, record_hash_key: str, shard_size: int, request: Json) -> Json:
        payload = json.dumps(request, separators=(",", ":"), sort_keys=True).encode("utf-8")
        def call() -> Json:
            out = self._ctypes.c_void_p()
            error = self._ctypes.c_void_p()
            code = self._lib.temporalstore_rust_matrixark_retrieve_context_pack_json(self._handle, count_key.encode(), record_hash_key.encode(), int(shard_size), payload, self._ctypes.byref(out), self._ctypes.byref(error))
            self._check(code, error)
            return json.loads(self._decode_owned(out))
        return self._call("matrixark_retrieve_context_pack", call)

    def health(self) -> Json:
        return {"ok": True, "status": "ok", "mode": "rust_direct_cdylib"}

    def readiness(self) -> Json:
        return {"ok": True, "status": "ready", "mode": "rust_direct_cdylib", "cached_clients": 1}

    def metrics_snapshot(self) -> Json:
        elapsed = max(0.001, sum(self._latency_samples_ms) / 1000.0) if self._latency_samples_ms else 1.0
        return {
            "gateway_mode": "rust_direct_cdylib",
            "proxy_mode": "none",
            "sdk_mode": "direct_cdylib",
            "transport": "in_process_cdylib_ctypes",
            "process_per_operation_enabled": False,
            "single_shot_mode": "debug_only_disabled_for_hot_path",
            "commands_total": self._commands_total,
            "commands_failed_total": self._commands_failed_total,
            "timeouts_total": 0,
            "qps": round(self._commands_total / elapsed, 6),
            "records_written_total": self._records_written_total,
            "records_read_total": self._records_read_total,
            "latency_ms_sum": round(sum(self._latency_samples_ms), 3),
            "latency_ms_count": len(self._latency_samples_ms),
            "latency_ms_max": round(max(self._latency_samples_ms) if self._latency_samples_ms else 0.0, 3),
            "p95_latency_ms": round(self._percentile(self._latency_samples_ms, 0.95), 3),
            "p99_latency_ms": round(self._percentile(self._latency_samples_ms, 0.99), 3),
            "matrixark_append_write_path": "rust_direct_cdylib_matrixark_batch_append_records",
            "matrixark_native_batch_append_available": True,
            "matrixark_batch_append_uses_existing_batch_execute": True,
            "matrixark_batch_append_existing_batch_execute_source": "temporalstore_rust_cdylib_to_temporalstore_matrixark_batch_append_records",
            "matrixark_append_uses_per_record_hset": False,
            "matrixark_append_uses_generic_batch_hset_fallback": False,
            "supports_batch_append": True,
            "supports_prefix_scan": True,
            "prefix_scan_path": "rust_direct_cdylib_hgetall_json",
            "supports_native_candidate_prefilter": True,
            "candidate_prefilter_path": "rust_direct_cdylib_matrixark_scan_candidates",
            "supports_native_pack_assembly": True,
            "native_pack_assembly_path": "rust_direct_cdylib_matrixark_retrieve_context_pack",
            "requires_c_sdk_hgetall_for_prefix_scan": False,
        }

    def metrics_prometheus(self) -> str:
        metrics = self.metrics_snapshot()
        return "\n".join([
            '# TYPE matrixark_rust_direct_cdylib_commands_total counter',
            f'matrixark_rust_direct_cdylib_commands_total {metrics["commands_total"]}',
            '# TYPE matrixark_rust_direct_cdylib_errors_total counter',
            f'matrixark_rust_direct_cdylib_errors_total {metrics["commands_failed_total"]}',
        ]) + "\n"

    @staticmethod
    def _percentile(values: list[float], ratio: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * ratio))))
        return ordered[index]

    def shutdown(self) -> None:
        self.close()

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
        self._pack_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_PACK_LANES", "16")))
        self._control_lane_count = max(1, int(os.environ.get("MATRIXARK_RUST_PROXY_CONTROL_LANES", "1")))
        self._shared_process_mode = os.environ.get("MATRIXARK_RUST_PROXY_SHARED_PROCESS", "1").strip().lower() not in {"0", "false", "no"}
        self._dedicated_pack_lanes_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._batch_hset_coalesce_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._batch_hset_coalesce_max_batches = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_MAX_BATCHES", "32"))
        )
        self._batch_hset_coalesce_min_records = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_MIN_RECORDS", "16"))
        )
        self._batch_hset_coalesce_wait_s = max(
            0.0,
            float(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HSET_COALESCE_WAIT_MS", "0")) / 1000.0,
        )
        self._batch_hget_coalesce_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._batch_hget_coalesce_max_batches = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_MAX_BATCHES", "32"))
        )
        self._batch_hget_coalesce_min_records = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_MIN_RECORDS", "16"))
        )
        self._batch_hget_coalesce_wait_s = max(
            0.0,
            float(os.environ.get("MATRIXARK_RUST_PROXY_BATCH_HGET_COALESCE_WAIT_MS", "0.0")) / 1000.0,
        )
        self._append_coalesce_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._append_coalesce_max_batches = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE_MAX_BATCHES", "32"))
        )
        self._append_coalesce_min_records = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE_MIN_RECORDS", "16"))
        )
        self._append_coalesce_wait_s = max(
            0.0,
            float(os.environ.get("MATRIXARK_RUST_PROXY_APPEND_COALESCE_WAIT_MS", "0.0")) / 1000.0,
        )
        self._string_cache_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_STRING_CACHE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._scan_hash_cache_enabled = (
            os.environ.get("MATRIXARK_RUST_PROXY_SCAN_HASH_CACHE", "1").strip().lower()
            not in {"0", "false", "no"}
        )
        self._scan_hash_cache_max_entries = max(
            1, int(os.environ.get("MATRIXARK_RUST_PROXY_SCAN_HASH_CACHE_MAX_ENTRIES", "1024"))
        )
        if self._shared_process_mode:
            # The local Rust TemporalEngine is embedded in the proxy process. A
            # multi-process write lane pool can hide writes from reads until
            # there is a real shared server/proxy behind it, so writes/control
            # stay on one process. Retrieve-pack is read-mostly after ingest
            # and may use a warm process pool to avoid stdin/stdout head-of-line
            # blocking in scale tests and production proxy mode.
            shared_lanes = self._make_lanes(1)
            pack_lanes = self._make_lanes(self._pack_lane_count) if self._dedicated_pack_lanes_enabled else shared_lanes
            self._lanes = {
                "write": shared_lanes,
                "read": shared_lanes,
                "pack": pack_lanes,
                "control": shared_lanes,
            }
        else:
            self._lanes = {
                "write": self._make_lanes(self._write_lane_count),
                "read": self._make_lanes(self._read_lane_count),
                "pack": self._make_lanes(self._pack_lane_count),
                "control": self._make_lanes(self._control_lane_count),
            }
        self._lane_worker_counts = {name: len(lanes) for name, lanes in self._lanes.items()}
        self._lane_worker_counts["retrieve"] = self._lane_worker_counts.get("pack", 0)
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
        self._lane_latency_samples_ms: dict[str, list[float]] = {lane: [] for lane in self._lane_worker_counts}
        self._lane_commands_total: dict[str, int] = {lane: 0 for lane in self._lane_worker_counts}
        self._lane_wait_ms_total: dict[str, float] = {lane: 0.0 for lane in self._lane_worker_counts}
        self._lane_wait_ms_max: dict[str, float] = {lane: 0.0 for lane in self._lane_worker_counts}
        self._op_commands_total: dict[str, int] = {}
        self._op_latency_ms_total: dict[str, float] = {}
        self._op_latency_ms_max: dict[str, float] = {}
        self._serialization_ms_total = 0.0
        self._serialization_ms_max = 0.0
        self._rust_engine_ms_total = 0.0
        self._rust_engine_ms_max = 0.0
        self._scan_count_total = 0
        self._cache_hits_total = 0
        self._cache_misses_total = 0
        self._selected_refs_total = 0
        self._dropped_refs_total = 0
        self._context_record_counts: dict[str, int] = {}
        self._publish_visibility_calls_total = 0
        self._publish_visibility_keys_total = 0
        self._publish_visibility_full_shard_total = 0
        self._publish_visibility_index_bytes_total = 0
        self._publish_visibility_last_key_count = 0
        self._publish_visibility_last_index_bytes = 0
        self._batch_hset_coalesce_lock = threading.Lock()
        self._batch_hset_coalesce_queue: list[Json] = []
        self._batch_hset_coalesce_active = False
        self._batch_hset_coalesced_batches_total = 0
        self._batch_hset_coalesced_calls_total = 0
        self._batch_hset_coalesced_records_total = 0
        self._batch_hset_coalesced_wait_ms_total = 0.0
        self._batch_hset_coalesced_wait_ms_max = 0.0
        self._batch_hget_coalesce_lock = threading.Lock()
        self._batch_hget_coalesce_queue: list[Json] = []
        self._batch_hget_coalesce_active = False
        self._batch_hget_coalesced_batches_total = 0
        self._batch_hget_coalesced_calls_total = 0
        self._batch_hget_coalesced_records_total = 0
        self._batch_hget_coalesced_wait_ms_total = 0.0
        self._batch_hget_coalesced_wait_ms_max = 0.0
        self._append_coalesce_lock = threading.Lock()
        self._append_coalesce_queue: list[Json] = []
        self._append_coalesce_active = False
        self._append_coalesced_batches_total = 0
        self._append_coalesced_calls_total = 0
        self._append_coalesced_records_total = 0
        self._append_coalesced_wait_ms_total = 0.0
        self._append_coalesced_wait_ms_max = 0.0
        self._string_cache_lock = threading.Lock()
        self._string_cache: dict[str, str] = {}
        self._string_cache_hits_total = 0
        self._string_cache_misses_total = 0
        self._string_cache_updates_total = 0
        self._scan_hash_cache_lock = threading.Lock()
        self._scan_hash_cache: OrderedDict[str, Json] = OrderedDict()
        self._scan_hash_cache_hits_total = 0
        self._scan_hash_cache_misses_total = 0
        self._scan_hash_cache_updates_total = 0
        self._scan_hash_cache_invalidations_total = 0
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
        env = os.environ.copy()
        proxy_dir = str(Path(self.cli_path).resolve().parent)
        existing_ld_path = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = proxy_dir if not existing_ld_path else f"{proxy_dir}:{existing_ld_path}"
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
                if op == "shutdown" and proc.returncode == 0:
                    return {"ok": True, "status": "shutdown"}
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
        group, lane = self._choose_lane(op)
        semaphore: threading.BoundedSemaphore = lane["semaphore"]
        wait_started = time.perf_counter()
        acquired = semaphore.acquire(timeout=self._backpressure_timeout_s)
        wait_ms = (time.perf_counter() - wait_started) * 1000.0
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, backpressure=True, lane=group, wait_ms=wait_ms)
            raise MatrixArkError(
                f"Rust TemporalStore {op} rejected by {group} proxy lane backpressure after "
                f"{self._backpressure_timeout_s:.3f}s with "
                f"{self._lane_worker_counts.get(group, 1)} workers"
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
            self._record_call_metrics(op, kwargs, None, elapsed_ms, failed=True, lane=group, wait_ms=wait_ms)
            raise
        finally:
            semaphore.release()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not response.get("ok"):
            self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=True, lane=group, wait_ms=wait_ms)
            if not raise_on_error:
                return response
            raise MatrixArkError(f"Rust TemporalStore {op} failed: {response.get('error', 'unknown error')}")
        self._record_call_metrics(op, kwargs, response, elapsed_ms, failed=False, lane=group, wait_ms=wait_ms)
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
        lane: str = "control",
        wait_ms: float = 0.0,
    ) -> None:
        with self._metrics_lock:
            self._commands_total += 1
            self._lane_commands_total[lane] = self._lane_commands_total.get(lane, 0) + 1
            self._lane_wait_ms_total[lane] = self._lane_wait_ms_total.get(lane, 0.0) + max(0.0, wait_ms)
            self._lane_wait_ms_max[lane] = max(self._lane_wait_ms_max.get(lane, 0.0), max(0.0, wait_ms))
            self._op_commands_total[op] = self._op_commands_total.get(op, 0) + 1
            self._op_latency_ms_total[op] = self._op_latency_ms_total.get(op, 0.0) + max(0.0, elapsed_ms)
            self._op_latency_ms_max[op] = max(self._op_latency_ms_max.get(op, 0.0), max(0.0, elapsed_ms))
            if failed:
                self._commands_failed_total += 1
                if "timed out" in str(response or "").lower() or elapsed_ms >= self.request_timeout_ms:
                    self._timeouts_total += 1
            if backpressure:
                self._backpressure_rejections_total += 1
            if response:
                serialization_ms = self._nested_float(
                    response,
                    "serialization_time_ms",
                    "serialization_ms",
                    "serialization_time",
                )
                engine_ms = self._nested_float(
                    response,
                    "rust_engine_time_ms",
                    "engine_ms",
                    "rust_engine_ms",
                )
                self._serialization_ms_total += serialization_ms
                self._serialization_ms_max = max(self._serialization_ms_max, serialization_ms)
                self._rust_engine_ms_total += engine_ms
                self._rust_engine_ms_max = max(self._rust_engine_ms_max, engine_ms)
                scan_count = int(
                    self._nested_float(
                        response,
                        "scan_count",
                        "scan_stats.scanned_records",
                        "context_pack.recall_policy.scan_stats.scanned_records",
                    )
                    or 0
                )
                self._scan_count_total += scan_count
                cache_hit = bool(response.get("cache_hit") or response.get("cache_hit_used"))
                if cache_hit:
                    self._cache_hits_total += 1
                elif op in {"matrixark_scan_candidates", "matrixark_retrieve_context_pack"}:
                    self._cache_misses_total += 1
                selected_count = int(
                    self._nested_float(
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
                self._selected_refs_total += selected_count
                dropped_count = int(
                    self._nested_float(
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
                self._dropped_refs_total += dropped_count
            self._last_latency_ms = elapsed_ms
            self._max_observed_latency_ms = max(self._max_observed_latency_ms, elapsed_ms)
            self._latency_samples_ms.append(elapsed_ms)
            if len(self._latency_samples_ms) > 2048:
                del self._latency_samples_ms[: len(self._latency_samples_ms) - 2048]
            lane_samples = self._lane_latency_samples_ms.setdefault(lane, [])
            lane_samples.append(elapsed_ms)
            if len(lane_samples) > 1024:
                del lane_samples[: len(lane_samples) - 1024]
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
                elif op == "matrixark_publish_visibility":
                    visibility_keys = kwargs.get("visibility_keys") if isinstance(kwargs, dict) else []
                    key_count = len(visibility_keys) if isinstance(visibility_keys, list) else 0
                    index_bytes = int(
                        self._nested_float(
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
                    self._publish_visibility_calls_total += 1
                    self._publish_visibility_keys_total += key_count
                    self._publish_visibility_full_shard_total += 1 if full_shard else 0
                    self._publish_visibility_index_bytes_total += index_bytes
                    self._publish_visibility_last_key_count = key_count
                    self._publish_visibility_last_index_bytes = index_bytes

    @staticmethod
    def _nested_float(payload: Json, *paths: str) -> float:
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

    def _count_context_record(self, value: Any) -> None:
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
                "context_entity",
                "context_event",
                "context_index",
                "context_segment",
                "context_summary",
                "resource_chunk",
                "skill_section",
            ],
            return_index_records=False,
            scope=request.get("scope", {}),
            secondary_index_groups=request.get("secondary_index_groups", []),
            record=request,
        )

    def matrixark_publish_visibility(self, visibility_keys: list[str] | None = None) -> Json:
        return self._call_json("matrixark_publish_visibility", visibility_keys=visibility_keys or [])

    def metrics_snapshot(self) -> Json:
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

    def _call(self, op: str, **kwargs: Any) -> str:
        response = self._call_json(op, **kwargs)
        return str(response.get("value", ""))

    def _string_cache_key_allowed(self, key: str) -> bool:
        return self._string_cache_enabled and str(key).endswith((":record_count", ":record_index"))

    def _string_cache_get(self, key: str) -> str | None:
        if not self._string_cache_key_allowed(key):
            return None
        with self._string_cache_lock:
            value = self._string_cache.get(key)
        with self._metrics_lock:
            if value is None:
                self._string_cache_misses_total += 1
            else:
                self._string_cache_hits_total += 1
        return value

    def _string_cache_put(self, key: str, value: str) -> None:
        if not self._string_cache_key_allowed(key):
            return
        with self._string_cache_lock:
            self._string_cache[key] = str(value)
        with self._metrics_lock:
            self._string_cache_updates_total += 1

    def _scan_hash_cache_get(self, key: str) -> Json | None:
        if not self._scan_hash_cache_enabled:
            return None
        with self._scan_hash_cache_lock:
            cached = self._scan_hash_cache.get(key)
            if cached is None:
                value = None
            else:
                self._scan_hash_cache.move_to_end(key)
                value = json.loads(json.dumps(cached))
        with self._metrics_lock:
            if value is None:
                self._scan_hash_cache_misses_total += 1
            else:
                self._scan_hash_cache_hits_total += 1
        return value

    def _scan_hash_cache_put(self, key: str, response: Json) -> None:
        if not self._scan_hash_cache_enabled:
            return
        with self._scan_hash_cache_lock:
            self._scan_hash_cache[key] = json.loads(json.dumps(response))
            self._scan_hash_cache.move_to_end(key)
            while len(self._scan_hash_cache) > self._scan_hash_cache_max_entries:
                self._scan_hash_cache.popitem(last=False)
        with self._metrics_lock:
            self._scan_hash_cache_updates_total += 1

    def _scan_hash_cache_invalidate_keys(self, keys: Iterable[str]) -> None:
        if not self._scan_hash_cache_enabled:
            return
        removed = 0
        with self._scan_hash_cache_lock:
            for key in set(str(item) for item in keys if str(item)):
                if self._scan_hash_cache.pop(key, None) is not None:
                    removed += 1
        if removed:
            with self._metrics_lock:
                self._scan_hash_cache_invalidations_total += removed

    def put_string(self, key: str, value: str) -> None:
        self._call("put_string", key=key, value=value)
        self._string_cache_put(key, value)

    def get_string(self, key: str) -> str:
        cached = self._string_cache_get(key)
        if cached is not None:
            return cached
        value = self._call("get_string", key=key)
        self._string_cache_put(key, value)
        return value

    def hset(self, key: str, field: str, value: str) -> None:
        self._call("hset", key=key, field=field, value=value)
        self._scan_hash_cache_invalidate_keys([key])

    def hget(self, key: str, field: str) -> str:
        return self._call("hget", key=key, field=field)

    def batch_hset(self, entries: list[Json]) -> None:
        if not entries:
            return
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), str(entry.get("value") or "")]
            for entry in entries
            if isinstance(entry, dict)
        ]
        if (
            self._batch_hset_coalesce_enabled
            and self._shared_process_mode
            and len(compact_entries) >= self._batch_hset_coalesce_min_records
        ):
            self._coalesced_batch_hset(compact_entries)
            return
        self._call_json("batch_hset", entries_compact=compact_entries)
        self._scan_hash_cache_invalidate_keys(entry[0] for entry in compact_entries)

    def _coalesced_batch_hset(self, compact_entries: list[list[str]]) -> None:
        event = threading.Event()
        request: Json = {
            "entries_compact": compact_entries,
            "event": event,
            "error": None,
        }
        became_leader = False
        queued_at = time.perf_counter()
        with self._batch_hset_coalesce_lock:
            self._batch_hset_coalesce_queue.append(request)
            if not self._batch_hset_coalesce_active:
                self._batch_hset_coalesce_active = True
                became_leader = True
        if became_leader:
            self._drain_batch_hset_coalescer()
        else:
            timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
            if not event.wait(timeout=timeout_s):
                raise MatrixArkError(f"Rust TemporalStore batch_hset coalescer timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - queued_at) * 1000.0
        with self._metrics_lock:
            self._batch_hset_coalesced_wait_ms_total += wait_ms
            self._batch_hset_coalesced_wait_ms_max = max(self._batch_hset_coalesced_wait_ms_max, wait_ms)
        error = request.get("error")
        if error:
            raise error

    def _drain_batch_hset_coalescer(self) -> None:
        try:
            if self._batch_hset_coalesce_wait_s > 0:
                time.sleep(self._batch_hset_coalesce_wait_s)
            while True:
                with self._batch_hset_coalesce_lock:
                    pending = self._batch_hset_coalesce_queue[: self._batch_hset_coalesce_max_batches]
                    del self._batch_hset_coalesce_queue[: len(pending)]
                if not pending:
                    with self._batch_hset_coalesce_lock:
                        if not self._batch_hset_coalesce_queue:
                            self._batch_hset_coalesce_active = False
                            return
                    continue
                merged: list[list[str]] = []
                for item in pending:
                    merged.extend(item.get("entries_compact") or [])
                error: BaseException | None = None
                try:
                    self._call_json("batch_hset", entries_compact=merged)
                except BaseException as exc:
                    error = exc
                if error is None:
                    self._scan_hash_cache_invalidate_keys(entry[0] for entry in merged)
                with self._metrics_lock:
                    self._batch_hset_coalesced_batches_total += 1
                    self._batch_hset_coalesced_calls_total += len(pending)
                    self._batch_hset_coalesced_records_total += len(merged)
                for item in pending:
                    item["error"] = error
                    item["event"].set()
                if error is not None:
                    with self._batch_hset_coalesce_lock:
                        remaining = self._batch_hset_coalesce_queue
                        self._batch_hset_coalesce_queue = []
                        self._batch_hset_coalesce_active = False
                    for item in remaining:
                        item["error"] = error
                        item["event"].set()
                    return
        except BaseException as exc:
            with self._batch_hset_coalesce_lock:
                remaining = self._batch_hset_coalesce_queue
                self._batch_hset_coalesce_queue = []
                self._batch_hset_coalesce_active = False
            for item in remaining:
                item["error"] = exc
                item["event"].set()
            raise

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
        append_options = append_options or {}
        if (
            self._append_coalesce_enabled
            and self._shared_process_mode
            and len(compact_entries) >= self._append_coalesce_min_records
        ):
            self._coalesced_matrixark_batch_append_records(
                compact_entries,
                count_key=count_key or "",
                count_value=count_value or "",
                append_options=append_options,
            )
            return
        self._call_json(
            "matrixark_batch_append_records",
            entries_compact=compact_entries,
            key=count_key or "",
            value=count_value or "",
            append_options=append_options,
        )
        self._scan_hash_cache_invalidate_keys(entry[0] for entry in compact_entries)
        if count_key:
            self._string_cache_put(count_key, count_value or "")

    @staticmethod
    def _append_options_signature(append_options: Json) -> str:
        try:
            return json.dumps(append_options or {}, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            return str(sorted((append_options or {}).items())) if isinstance(append_options, dict) else str(append_options)

    @staticmethod
    def _max_count_value(values: list[str]) -> str:
        numeric: list[int] = []
        for value in values:
            try:
                numeric.append(int(str(value)))
            except (TypeError, ValueError):
                continue
        if numeric:
            return str(max(numeric))
        return values[-1] if values else ""

    def _coalesced_matrixark_batch_append_records(
        self,
        compact_entries: list[list[str]],
        *,
        count_key: str,
        count_value: str,
        append_options: Json,
    ) -> None:
        event = threading.Event()
        request: Json = {
            "entries_compact": compact_entries,
            "count_key": count_key,
            "count_value": count_value,
            "append_options": append_options,
            "append_options_signature": self._append_options_signature(append_options),
            "event": event,
            "error": None,
        }
        became_leader = False
        queued_at = time.perf_counter()
        with self._append_coalesce_lock:
            self._append_coalesce_queue.append(request)
            if not self._append_coalesce_active:
                self._append_coalesce_active = True
                became_leader = True
        if became_leader:
            self._drain_append_coalescer()
        else:
            timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
            if not event.wait(timeout=timeout_s):
                raise MatrixArkError(f"Rust TemporalStore matrixark append coalescer timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - queued_at) * 1000.0
        with self._metrics_lock:
            self._append_coalesced_wait_ms_total += wait_ms
            self._append_coalesced_wait_ms_max = max(self._append_coalesced_wait_ms_max, wait_ms)
        error = request.get("error")
        if error:
            raise error

    def _drain_append_coalescer(self) -> None:
        try:
            if self._append_coalesce_wait_s > 0:
                time.sleep(self._append_coalesce_wait_s)
            while True:
                with self._append_coalesce_lock:
                    pending = self._append_coalesce_queue[: self._append_coalesce_max_batches]
                    del self._append_coalesce_queue[: len(pending)]
                if not pending:
                    with self._append_coalesce_lock:
                        if not self._append_coalesce_queue:
                            self._append_coalesce_active = False
                            return
                    continue
                grouped: dict[tuple[str, str], list[Json]] = {}
                for item in pending:
                    signature = (str(item.get("count_key") or ""), str(item.get("append_options_signature") or ""))
                    grouped.setdefault(signature, []).append(item)
                for items in grouped.values():
                    merged: list[list[str]] = []
                    count_values: list[str] = []
                    append_options = items[0].get("append_options") or {}
                    count_key = str(items[0].get("count_key") or "")
                    for item in items:
                        merged.extend(item.get("entries_compact") or [])
                        value = str(item.get("count_value") or "")
                        if value:
                            count_values.append(value)
                    count_value = self._max_count_value(count_values)
                    error: BaseException | None = None
                    try:
                        self._call_json(
                            "matrixark_batch_append_records",
                            entries_compact=merged,
                            key=count_key,
                            value=count_value,
                            append_options=append_options,
                        )
                    except BaseException as exc:
                        error = exc
                    if error is None and count_key:
                        self._string_cache_put(count_key, count_value)
                    if error is None:
                        self._scan_hash_cache_invalidate_keys(entry[0] for entry in merged)
                    with self._metrics_lock:
                        self._append_coalesced_batches_total += 1
                        self._append_coalesced_calls_total += len(items)
                        self._append_coalesced_records_total += len(merged)
                    for item in items:
                        item["error"] = error
                        item["event"].set()
                    if error is not None:
                        with self._append_coalesce_lock:
                            remaining = self._append_coalesce_queue
                            self._append_coalesce_queue = []
                            self._append_coalesce_active = False
                        for item in remaining:
                            item["error"] = error
                            item["event"].set()
                        return
        except BaseException as exc:
            with self._append_coalesce_lock:
                remaining = self._append_coalesce_queue
                self._append_coalesce_queue = []
                self._append_coalesce_active = False
            for item in remaining:
                item["error"] = exc
                item["event"].set()
            raise

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

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Json,
    ) -> Json:
        response = self._call_json(
            "matrixark_retrieve_context_pack",
            count_key=count_key,
            record_hash_key=record_hash_key,
            shard_size=shard_size,
            record_types=[
                "context_compression_event",
                "context_entity",
                "context_event",
                "context_index",
                "context_segment",
                "context_summary",
                "resource_chunk",
                "skill_section",
            ],
            return_index_records=False,
            scope=request.get("scope", {}),
            secondary_index_groups=request.get("secondary_index_groups", []),
            record=request,
            top_level_response=True,
        )
        value = response.get("value")
        if isinstance(value, str) and value:
            decoded = json.loads(value)
            if isinstance(decoded, dict):
                return decoded
        return response

    def batch_hget(self, entries: list[Json]) -> list[Json]:
        if not entries:
            return []
        compact_entries = [
            [str(entry.get("key") or ""), str(entry.get("field") or ""), ""]
            for entry in entries
            if isinstance(entry, dict)
        ]
        if (
            self._batch_hget_coalesce_enabled
            and self._shared_process_mode
            and len(compact_entries) >= self._batch_hget_coalesce_min_records
        ):
            return self._coalesced_batch_hget(compact_entries)
        response = self._call_json("batch_hget", entries_compact=compact_entries)
        records = response.get("records", [])
        return records if isinstance(records, list) else []

    def _coalesced_batch_hget(self, compact_entries: list[list[str]]) -> list[Json]:
        event = threading.Event()
        request: Json = {
            "entries_compact": compact_entries,
            "event": event,
            "error": None,
            "records": None,
        }
        became_leader = False
        queued_at = time.perf_counter()
        with self._batch_hget_coalesce_lock:
            self._batch_hget_coalesce_queue.append(request)
            if not self._batch_hget_coalesce_active:
                self._batch_hget_coalesce_active = True
                became_leader = True
        if became_leader:
            self._drain_batch_hget_coalescer()
        else:
            timeout_s = max(self._backpressure_timeout_s, self.request_timeout_ms / 1000.0 + 2.0)
            if not event.wait(timeout=timeout_s):
                raise MatrixArkError(f"Rust TemporalStore batch_hget coalescer timed out after {timeout_s:.1f}s")
        wait_ms = (time.perf_counter() - queued_at) * 1000.0
        with self._metrics_lock:
            self._batch_hget_coalesced_wait_ms_total += wait_ms
            self._batch_hget_coalesced_wait_ms_max = max(self._batch_hget_coalesced_wait_ms_max, wait_ms)
        error = request.get("error")
        if error:
            raise error
        records = request.get("records")
        return records if isinstance(records, list) else []

    def _drain_batch_hget_coalescer(self) -> None:
        try:
            if self._batch_hget_coalesce_wait_s > 0:
                time.sleep(self._batch_hget_coalesce_wait_s)
            while True:
                with self._batch_hget_coalesce_lock:
                    pending = self._batch_hget_coalesce_queue[: self._batch_hget_coalesce_max_batches]
                    del self._batch_hget_coalesce_queue[: len(pending)]
                if not pending:
                    with self._batch_hget_coalesce_lock:
                        if not self._batch_hget_coalesce_queue:
                            self._batch_hget_coalesce_active = False
                            return
                    continue
                merged: list[list[str]] = []
                for item in pending:
                    merged.extend(item.get("entries_compact") or [])
                error: BaseException | None = None
                rows: list[Json] = []
                try:
                    response = self._call_json("batch_hget", entries_compact=merged)
                    response_rows = response.get("records", [])
                    rows = response_rows if isinstance(response_rows, list) else []
                except BaseException as exc:
                    error = exc
                if error is None:
                    records_by_entry: dict[tuple[str, str], deque[Json]] = defaultdict(deque)
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        records_by_entry[(str(row.get("key") or ""), str(row.get("field") or ""))].append(row)
                    for item in pending:
                        item_records: list[Json] = []
                        for key, field, _ in item.get("entries_compact") or []:
                            bucket = records_by_entry.get((key, field))
                            if bucket:
                                item_records.append(bucket.popleft())
                            else:
                                item_records.append({"key": key, "field": field, "value": ""})
                        item["records"] = item_records
                with self._metrics_lock:
                    self._batch_hget_coalesced_batches_total += 1
                    self._batch_hget_coalesced_calls_total += len(pending)
                    self._batch_hget_coalesced_records_total += len(merged)
                for item in pending:
                    item["error"] = error
                    item["event"].set()
                if error is not None:
                    with self._batch_hget_coalesce_lock:
                        remaining = self._batch_hget_coalesce_queue
                        self._batch_hget_coalesce_queue = []
                        self._batch_hget_coalesce_active = False
                    for item in remaining:
                        item["error"] = error
                        item["event"].set()
                    return
        except BaseException as exc:
            with self._batch_hget_coalesce_lock:
                remaining = self._batch_hget_coalesce_queue
                self._batch_hget_coalesce_queue = []
                self._batch_hget_coalesce_active = False
            for item in remaining:
                item["error"] = exc
                item["event"].set()
            raise

    def scan_hash(self, key: str) -> Json:
        cached = self._scan_hash_cache_get(key)
        if cached is not None:
            return cached
        response = self._call_json("scan_hash", key=key)
        self._scan_hash_cache_put(key, response)
        return response

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
        self._dedicated_proxy_clients_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS",
            "0",
        ).strip().lower() in {"1", "true", "yes"}
        self._dedicated_pack_lanes_enabled = os.environ.get(
            "MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES",
            "0",
        ).strip().lower() in {"1", "true", "yes"}
        self._publish_visibility_after_flush = (
            self._dedicated_proxy_clients_enabled or self._dedicated_pack_lanes_enabled
        )
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

        if getattr(self, "_rust_direct_cdylib_enabled", False) or not getattr(self, "_dedicated_proxy_clients_enabled", False):
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

        if getattr(self, "_rust_direct_cdylib_enabled", False) or not getattr(self, "_dedicated_proxy_clients_enabled", False):
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
            return self._retrieve_client

    def flush_direct_writes(self, timeout_s: float | None = None) -> None:
        super().flush_direct_writes(timeout_s=timeout_s)
        if not getattr(self, "_publish_visibility_after_flush", False):
            return
        visibility_keys = self._consume_pending_visibility_keys()
        if not visibility_keys:
            return
        publisher = getattr(self._client, "matrixark_publish_visibility", None)
        if callable(publisher):
            publisher(visibility_keys=visibility_keys)

    def supports_native_candidate_prefilter(self) -> bool:
        return True

    def supports_native_context_pack(self) -> bool:
        return True

    def native_context_pack(self, request: Json) -> Json | None:
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
