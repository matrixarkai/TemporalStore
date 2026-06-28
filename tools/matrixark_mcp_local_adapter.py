#!/usr/bin/env python3
"""Local MatrixArk adapter and in-memory serving backend."""

from __future__ import annotations

from contextlib import contextmanager
import queue as thread_queue

try:
    from tools.matrixark_mcp_core import *
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *

try:
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_metrics import MatrixArkServiceMetrics

RETRIEVAL_HOT_RECORD_TYPES = {
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
}

RESOURCE_IMPORT_IGNORE_DIRS = {".git", "node_modules", "target", "build", "dist", ".venv", "__pycache__"}
LOCAL_READ_CACHE_COPY = os.environ.get("MATRIXARK_LOCAL_READ_CACHE_COPY", "1").strip().lower() not in {"0", "false", "no"}

_LOCAL_READ_CACHE_LOCK = threading.RLock()
_LOCAL_READ_CACHE: dict[str, tuple[int, int, list[Json]]] = {}



def latest_value_record_key(record: Json) -> tuple[Any, ...] | None:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_node":
        return (record_type, record.get("node_hash"))
    if record_type == "context_child_ref":
        return (record_type, record.get("child_ref_hash"))
    if record_type == "context_summary":
        return (record_type, record.get("summary_type"), record.get("summary_hash") or record.get("node_hash"))
    if record_type == "context_embedding":
        return (record_type, record.get("embedding_type"), record.get("ref_type"), record.get("ref_hash"))
    if record_type == "context_index":
        return (
            record_type,
            record.get("index_name"),
            record.get("scope_key") or canonical_scope_key(record.get("scope", {})) if isinstance(record.get("scope", {}), dict) else record.get("scope_key"),
            record.get("node_hash") or record.get("node_id"),
            record.get("data_model") or record.get("ref_type"),
            record.get("timestamp_key_ms") or record.get("updated_at_ms"),
        )
    if record_type == "context_entity":
        return (record_type, record.get("entity_hash"))
    if record_type == "context_summary_dirty":
        return (record_type, record.get("dirty_hash"))
    if record_type == "resource_manifest":
        return (record_type, record.get("resource_hash"))
    if record_type == "skill_registry_update":
        return (record_type, record.get("skill_hash"))
    if record_type == "resource_import_task":
        return (record_type, record.get("resource_import_task_hash"))
    return None


def compact_latest_value_records(records: list[Json]) -> list[Json]:
    latest: dict[tuple[Any, ...], Json] = {}
    output: list[Json] = []
    latest_positions: dict[tuple[Any, ...], int] = {}
    for record in records:
        key = latest_value_record_key(record)
        if key is None or any(part in (None, "") for part in key[1:]):
            output.append(record)
            continue
        existing = latest.get(key)
        if existing is None:
            latest[key] = record
            latest_positions[key] = len(output)
            output.append(record)
            continue
        if int(record.get("updated_at_ms") or record.get("created_at_ms") or 0) >= int(
            existing.get("updated_at_ms") or existing.get("created_at_ms") or 0
        ):
            latest[key] = record
            output[latest_positions[key]] = record
    return output


@dataclass
class MatrixArkLocalAdapter:
    event_log: Path

    def __post_init__(self) -> None:
        self._init_local_runtime_state()

    def _init_local_runtime_state(self) -> None:
        self.event_log.parent.mkdir(parents=True, exist_ok=True)
        self._write_batch_local = threading.local()
        self._event_log_lock = threading.RLock()
        self._resource_import_worker_count = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_WORKERS", "2")))
        self._resource_import_queue_max = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX", "64")))
        self._resource_import_queue: thread_queue.Queue[Json] = thread_queue.Queue(maxsize=self._resource_import_queue_max)
        self._resource_import_workers_started = False
        self._resource_import_worker_lock = threading.RLock()
        self._resource_import_stop = threading.Event()
        self._resource_import_threads: list[threading.Thread] = []
        self._latest_entity_by_hash: dict[int, Json] = {}
        self._entity_cache_loaded = False
        self._session_buffer_cache_lock = threading.RLock()
        self._context_event_by_hash: dict[int, Json] = {}
        self._session_pending_event_ids_by_key: dict[tuple[str, str, str, str], list[int]] = {}
        self._session_committed_event_ids_by_key: dict[tuple[str, str, str, str], set[int]] = {}
        self._context_node_hashes: set[int] = set()
        self._context_child_ref_hashes: set[int] = set()
        self._context_node_cache_loaded = False
        self._read_cache_lock = threading.RLock()
        self._read_cache_records: list[Json] | None = None
        self._read_cache_size = -1
        self._read_cache_mtime_ns = -1
        self._retrieval_records_cache_lock = threading.RLock()
        self._retrieval_records_cache_generation = 0
        self._retrieval_records_cache: dict[tuple[Any, ...], Json] = {}
        self._context_pack_cache_lock = threading.RLock()
        self._context_pack_cache: dict[tuple[Any, ...], tuple[float, Json]] = {}
        self._context_pack_cache_max_entries = max(0, int(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_MAX_ENTRIES", "256")))
        self._context_pack_cache_ttl_s = max(0.0, float(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_TTL_S", "30")))

    def _write_batch_stack(self) -> list[list[Json]]:
        local = getattr(self, "_write_batch_local", None)
        if local is None:
            self._write_batch_local = threading.local()
            local = self._write_batch_local
        stack = getattr(local, "stack", None)
        if stack is None:
            stack = []
            local.stack = stack
        return stack

    def _current_write_batch(self) -> list[Json] | None:
        stack = self._write_batch_stack()
        return stack[-1] if stack else None

    def _queue_batched_records(self, records: list[Json]) -> bool:
        batch = self._current_write_batch()
        if batch is None:
            return False
        batch.extend(records)
        return True

    @contextmanager
    def write_batch(self, label: str = "hot_path"):
        stack = self._write_batch_stack()
        batch: list[Json] = []
        stack.append(batch)
        try:
            yield batch
        except Exception:
            stack.pop()
            raise
        else:
            stack.pop()
            if batch:
                self.append_many(batch)

    def ensure_backend_ready(self, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
        return {
            "status": "ready",
            "backend": "local",
            "reason": reason,
            "probe": bool(probe),
            "attempts": 1,
            "topology": {"mode": "local-jsonl", "event_log": str(self.event_log)},
            "checks": {
                "mcp_process_started": True,
                "namespace_table_opened": True,
                "slot_coverage_verified_by_warmup_hset_hget": True,
            },
        }

    def backend_metrics(self) -> Json:
        return {
            "backend": getattr(self, "_backend_label", lambda: "local")(),
            "metrics_format": "json",
            "metrics": {
                "mode": "local-jsonl",
                "event_log": str(self.event_log),
            },
        }

    def _observe_model_latency(self, stage: str, elapsed_ms: float) -> None:
        metrics = getattr(self, "_matrixark_service_metrics", None)
        if metrics is not None:
            try:
                metrics.observe_model_latency(stage, elapsed_ms)
            except Exception:
                pass

    def _update_read_cache_after_append(self, records: list[Json]) -> None:
        if not records:
            return
        cache_key = str(self.event_log.resolve())
        with self._read_cache_lock:
            if self._read_cache_records is not None:
                self._read_cache_records.extend(records)
            try:
                stat = self.event_log.stat()
                self._read_cache_size = int(stat.st_size)
                self._read_cache_mtime_ns = int(stat.st_mtime_ns)
            except FileNotFoundError:
                self._read_cache_records = None
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
        with _LOCAL_READ_CACHE_LOCK:
            cached = _LOCAL_READ_CACHE.get(cache_key)
            if cached is not None:
                _, _, cached_records = cached
                cached_records = list(cached_records) + list(records)
                _LOCAL_READ_CACHE[cache_key] = (self._read_cache_size, self._read_cache_mtime_ns, cached_records)
            elif self._read_cache_records is not None:
                _LOCAL_READ_CACHE[cache_key] = (self._read_cache_size, self._read_cache_mtime_ns, list(self._read_cache_records))
        if any(str(record.get("record_type") or "") in RETRIEVAL_HOT_RECORD_TYPES for record in records):
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
                with self._context_pack_cache_lock:
                    self._context_pack_cache.clear()

    def append(self, record: Json) -> None:
        records = materialize_serving_record_batch([record])
        if self._queue_batched_records(records):
            return
        with self._event_log_lock:
            with self.event_log.open("a", encoding="utf-8") as handle:
                for item in records:
                    handle.write(json.dumps(item, separators=(",", ":")) + "\n")
        self._update_latest_entity_cache(records)

    def append_many(self, records: list[Json]) -> None:
        records = materialize_serving_record_batch(records)
        if not records:
            return
        if self._queue_batched_records(records):
            return
        with self._event_log_lock:
            with self.event_log.open("a", encoding="utf-8") as handle:
                for record in records:
                    handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._update_latest_entity_cache(records)

    def _update_latest_entity_cache(self, records: list[Json]) -> None:
        if not hasattr(self, "_session_buffer_cache_lock"):
            self._session_buffer_cache_lock = threading.RLock()
        if not hasattr(self, "_context_event_by_hash"):
            self._context_event_by_hash = {}
        if not hasattr(self, "_session_pending_event_ids_by_key"):
            self._session_pending_event_ids_by_key = {}
        if not hasattr(self, "_session_committed_event_ids_by_key"):
            self._session_committed_event_ids_by_key = {}
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_event":
                try:
                    event_hash = int(record.get("event_id_hash", 0))
                except (TypeError, ValueError):
                    event_hash = 0
                if event_hash:
                    with self._session_buffer_cache_lock:
                        self._context_event_by_hash[event_hash] = record
                continue
            if record_type == "session_buffer_event":
                try:
                    event_hash = int(record.get("event_id_hash", 0))
                except (TypeError, ValueError):
                    event_hash = 0
                raw_key = record.get("buffer_key", [])
                if event_hash and isinstance(raw_key, list) and len(raw_key) == 4:
                    key = tuple(str(item) for item in raw_key)
                    with self._session_buffer_cache_lock:
                        committed = self._session_committed_event_ids_by_key.setdefault(key, set())
                        pending = self._session_pending_event_ids_by_key.setdefault(key, [])
                        if event_hash not in committed and event_hash not in pending:
                            pending.append(event_hash)
                continue
            if record_type == "context_batch_commit":
                key = session_buffer_key_from_scope(record.get("scope", {}))
                source_ids: list[int] = []
                for ref in record.get("source_event_ids", []):
                    try:
                        source_ids.append(int(ref))
                    except (TypeError, ValueError):
                        continue
                if source_ids:
                    with self._session_buffer_cache_lock:
                        committed = self._session_committed_event_ids_by_key.setdefault(key, set())
                        committed.update(source_ids)
                        pending = self._session_pending_event_ids_by_key.setdefault(key, [])
                        if pending:
                            source_set = set(source_ids)
                            self._session_pending_event_ids_by_key[key] = [event_id for event_id in pending if event_id not in source_set]
                continue
            if record_type == "context_node":
                try:
                    node_hash = int(record.get("node_hash", 0))
                except (TypeError, ValueError):
                    node_hash = 0
                if node_hash:
                    self._context_node_hashes.add(node_hash)
                continue
            if record_type == "context_child_ref":
                try:
                    child_ref_hash = int(record.get("child_ref_hash", 0))
                except (TypeError, ValueError):
                    child_ref_hash = 0
                if child_ref_hash:
                    self._context_child_ref_hashes.add(child_ref_hash)
                continue
            if record_type != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash", 0))
            except (TypeError, ValueError):
                continue
            if entity_hash:
                self._latest_entity_by_hash[entity_hash] = record

    def _ensure_context_node_cache_loaded(self) -> None:
        if self._context_node_cache_loaded:
            return
        self._context_node_hashes = set()
        self._context_child_ref_hashes = set()
        for record in self.read_all():
            if record.get("record_type") == "context_node" and record.get("node_hash") is not None:
                try:
                    self._context_node_hashes.add(int(record.get("node_hash")))
                except (TypeError, ValueError):
                    pass
            elif record.get("record_type") == "context_child_ref" and record.get("child_ref_hash") is not None:
                try:
                    self._context_child_ref_hashes.add(int(record.get("child_ref_hash")))
                except (TypeError, ValueError):
                    pass
        self._context_node_cache_loaded = True

    def _ensure_latest_entity_cache_loaded(self) -> None:
        if self._entity_cache_loaded:
            return
        records = self.read_all()
        self._latest_entity_by_hash = {}
        for record in records:
            if record.get("record_type") != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash", 0))
            except (TypeError, ValueError):
                continue
            if entity_hash:
                self._latest_entity_by_hash[entity_hash] = record
        self._entity_cache_loaded = True

    def append_audit(self, record: Json) -> None:
        self.append(record)

    def telemetry_record_for_context_pack(self, pack: Json, *, query: str, scope: Json, audit_mode: str) -> Json:
        recall_policy = pack.get("recall_policy", {}) if isinstance(pack.get("recall_policy"), dict) else {}
        stage_budgets = recall_policy.get("stage_latency_budgets", {}) if isinstance(recall_policy.get("stage_latency_budgets"), dict) else {}
        tree = recall_policy.get("tree_traversal", {}) if isinstance(recall_policy.get("tree_traversal"), dict) else {}
        secondary = recall_policy.get("secondary_index_filter", {}) if isinstance(recall_policy.get("secondary_index_filter"), dict) else {}
        rerank = recall_policy.get("rerank", {}) if isinstance(recall_policy.get("rerank"), dict) else {}
        time_weighted = recall_policy.get("time_weighted_recall", {}) if isinstance(recall_policy.get("time_weighted_recall"), dict) else {}
        dropped_refs = pack.get("dropped_refs", {}) if isinstance(pack.get("dropped_refs"), dict) else {}
        dropped_ref_count = int(dropped_refs.get("dropped_ref_count") or 0)
        if not dropped_ref_count and isinstance(dropped_refs.get("refs"), list):
            dropped_ref_count = len(dropped_refs.get("refs") or [])
        if not dropped_ref_count:
            dropped_ref_count = sum(value for key, value in dropped_refs.items() if isinstance(value, int) and key not in {"deadline_exceeded"})
        return {
            "record_type": "context_pack_telemetry",
            "context_pack_id": pack.get("context_pack_id", ""),
            "query_hash": stable_hash(query),
            "scope": scope,
            "audit_mode": audit_mode,
            "question_type": pack.get("question_type", ""),
            "query_plan": recall_policy.get("query_plan", {}),
            "selected_ref_count": len(pack.get("selected_refs", []) or []),
            "selected_ref_counts": pack.get("selected_ref_counts", {}),
            "dropped_ref_count": dropped_ref_count,
            "dropped_ref_bucket_counts": {k: v for k, v in dropped_refs.items() if isinstance(v, int)},
            "used_local_context_tokens": pack.get("used_local_context_tokens", 0),
            "used_remote_context_tokens": pack.get("used_remote_context_tokens", 0),
            "total_prompt_context_tokens": pack.get("total_prompt_context_tokens", 0),
            "remote_context_budget_tokens": pack.get("remote_context_budget_tokens", 0),
            "requested_max_context_tokens": pack.get("requested_max_context_tokens", 0),
            "partial_context_pack": bool(pack.get("partial_context_pack", False)),
            "insufficient_context": bool(pack.get("insufficient_context", False)),
            "quality_warning_count": len(pack.get("quality_warnings", []) or []),
            "primary_candidate_count": pack.get("primary_candidate_count", 0),
            "auxiliary_candidate_count": pack.get("auxiliary_candidate_count", 0),
            "tree_fallback_to_flat": bool(tree.get("fallback_to_flat", False)),
            "tree_selected_node_count": tree.get("selected_node_count", 0),
            "secondary_index_matched_candidate_count": secondary.get("matched_candidate_count", 0),
            "secondary_index_dropped_candidate_count": secondary.get("dropped_candidate_count", 0),
            "rerank_mode": rerank.get("mode", ""),
            "rerank_candidate_count": rerank.get("reranked_candidate_count", 0),
            "time_weighted_recall": time_weighted,
            "stage_latency_budgets": stage_budgets,
            "created_at_ms": now_ms(),
        }

    def append_context_pack_visibility(
        self,
        *,
        pack: Json,
        audit_record: Json,
        query: str,
        scope: Json,
        audit_mode: str,
        audit_sample_rate: float = 1.0,
    ) -> Json:
        telemetry_write_mode = CONTEXT_TELEMETRY_WRITE_MODE
        if telemetry_write_mode not in {"inline", "async", "sync", "off"}:
            raise MatrixArkError("MATRIXARK_CONTEXT_TELEMETRY_WRITE_MODE must be inline, async, sync, or off")
        force_rich_audit = bool(
            pack.get("partial_context_pack")
            or pack.get("insufficient_context")
            or pack.get("quality_warnings")
        )
        sample_basis = stable_hash(f"{pack.get('context_pack_id', '')}:{query}") % 1_000_000
        sample_value = sample_basis / 1_000_000.0
        rich_audit_sampled = bool(audit_mode == "full" and (force_rich_audit or sample_value < audit_sample_rate))
        telemetry_enabled = audit_mode != "off" and telemetry_write_mode != "off"
        visibility_decision = {
            "audit_mode": audit_mode,
            "audit_sample_rate": round(audit_sample_rate, 6),
            "audit_sample_value": round(sample_value, 6),
            "rich_replay_audit": rich_audit_sampled,
            "full_replay_audit_enabled": audit_mode == "full",
            "rich_replay_audit_force_reason": (
                "partial_or_warning" if force_rich_audit and audit_mode == "full" else "sampled" if rich_audit_sampled else "not_sampled"
            ),
            "telemetry_record": telemetry_enabled,
            "telemetry_write_mode": telemetry_write_mode,
            "serving_blocked_on_full_audit": False,
            "full_replay_audit_requires_full_mode": True,
        }
        telemetry = self.telemetry_record_for_context_pack(pack, query=query, scope=scope, audit_mode=audit_mode)
        telemetry["visibility_decision"] = visibility_decision
        if telemetry_enabled and telemetry_write_mode == "sync":
            self.append(telemetry)
        elif telemetry_enabled and telemetry_write_mode == "async":
            self.append_audit(telemetry)
        if rich_audit_sampled:
            audit_record["operational_visibility_policy"] = visibility_decision
            self.append_audit(compact_context_pack_audit_record(audit_record))
        return visibility_decision

    def flush_audits(self) -> None:
        return

    def find_idempotency_record(self, key_hash: int) -> Json | None:
        for record in reversed(self.read_all()):
            if record.get("record_type") == "matrixark_idempotency" and record.get("key_hash") == key_hash:
                return record
        return None

    def append_idempotency_record(self, *, key_hash: int, tool_name: str, raw_key: str, identity: Json, response: Json) -> None:
        self.append(
            {
                "record_type": "matrixark_idempotency",
                "key_hash": key_hash,
                "tool_name": tool_name,
                "raw_key_hash": stable_hash(raw_key),
                "scope_key": identity.get("scope_key", ""),
                "account_id": identity.get("account_id", ""),
                "tenant_id": identity.get("tenant_id", ""),
                "user_id": identity.get("user_id", ""),
                "session_id": identity.get("session_id", ""),
                "response": response,
                "created_at_ms": now_ms(),
            }
        )

    def ensure_backend_ready(self, *, reason: str = "matrixark") -> Json:
        return {"status": "ready", "backend": "local", "reason": reason}

    def recent_records(self, limit: int = 128) -> list[Json]:
        limit = max(1, int(limit or 1))
        records = self.read_all()
        if len(records) <= limit:
            return records
        return records[-limit:] if LOCAL_READ_CACHE_COPY else list(records[-limit:])

    def read_all(self) -> list[Json]:
        cache_key = str(self.event_log.resolve())
        try:
            stat = self.event_log.stat()
        except FileNotFoundError:
            with self._read_cache_lock:
                self._read_cache_records = []
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
            with _LOCAL_READ_CACHE_LOCK:
                _LOCAL_READ_CACHE.pop(cache_key, None)
            return []
        records = []
        with self._event_log_lock:
            with self.event_log.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        return compact_latest_value_records(records)

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
        allow_broad_scan_fallback: bool | None = None,
    ) -> Json:
        """Return records eligible for retrieval hot-path scan/filter/pack.

        C++/Rust backends override this seam with native prefix scans and
        secondary-index prefiltering. The local adapter keeps the reference
        behavior by filtering the JSONL record log before Python scoring.
        """

        allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
        scope_key = canonical_scope_key(scope)
        secondary_key = tuple(sorted(tuple(sorted(group)) for group in (secondary_index_groups or [])))
        selected_key = tuple(sorted(int(item) for item in (selected_node_hashes or set())))
        cache_key = (
            self._retrieval_records_cache_generation,
            scope_key,
            session_scope_mode(scope),
            tuple(sorted(allowed_types)),
            secondary_key,
            selected_key,
        )
        with self._retrieval_records_cache_lock:
            cached = self._retrieval_records_cache.get(cache_key)
            if cached is not None:
                scan_stats = dict(cached.get("scan_stats", {}))
                scan_stats["cache_hit"] = True
                return {"records": cached.get("records", []), "scan_stats": scan_stats}
        raw_records = self.read_all()
        filtered: list[Json] = []
        scanned = 0
        dropped_type = 0
        dropped_scope = 0
        dropped_node = 0
        selected_nodes = selected_node_hashes or set()
        for record in raw_records:
            scanned += 1
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
        result = {
            "records": filtered,
            "scan_stats": {
                "backend": getattr(self, "_backend_label", lambda: "local")(),
                "execution_mode": "adapter_prefilter_cached",
                "native_pushdown": False,
                "broad_scan_fallback_allowed": True if allow_broad_scan_fallback is None else bool(allow_broad_scan_fallback),
                "broad_scan_used": True,
                "broad_scan_reason": "local_reference_adapter",
                "record_types": sorted(allowed_types),
                "scanned_records": scanned,
                "returned_records": len(filtered),
                "dropped_by_type": dropped_type,
                "dropped_by_scope": dropped_scope,
                "dropped_by_node": dropped_node,
                "secondary_index_groups_supplied": len(secondary_index_groups or []),
                "selected_node_hashes_supplied": len(selected_node_hashes or set()),
            },
        }
        with self._retrieval_records_cache_lock:
            self._retrieval_records_cache[cache_key] = result
        return result

    def find_latest_entity(self, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
        entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
        if entity_hash in self._latest_entity_by_hash:
            return self._latest_entity_by_hash[entity_hash]
        self._ensure_latest_entity_cache_loaded()
        return self._latest_entity_by_hash.get(entity_hash)

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        key = session_buffer_key_from_scope(scope)
        if not hasattr(self, "_session_buffer_cache_lock"):
            self._session_buffer_cache_lock = threading.RLock()
        if not hasattr(self, "_context_event_by_hash"):
            self._context_event_by_hash = {}
        if not hasattr(self, "_session_pending_event_ids_by_key"):
            self._session_pending_event_ids_by_key = {}
        if not hasattr(self, "_session_committed_event_ids_by_key"):
            self._session_committed_event_ids_by_key = {}
        with self._session_buffer_cache_lock:
            if key in self._session_pending_event_ids_by_key:
                pending_ids = list(self._session_pending_event_ids_by_key.get(key, []))
                events = [self._context_event_by_hash[event_hash] for event_hash in pending_ids if event_hash in self._context_event_by_hash]
                return events[:limit] if limit is not None else events
        committed: set[int] = set()
        records = self.read_all()
        for record in records:
            if record.get("record_type") == "context_batch_commit" and session_buffer_key_from_scope(record.get("scope", {})) == key:
                for ref in record.get("source_event_ids", []):
                    try:
                        committed.add(int(ref))
                    except (TypeError, ValueError):
                        continue
        pending_ids: list[int] = []
        for record in records:
            if record.get("record_type") != "session_buffer_event" or tuple(record.get("buffer_key", [])) != key:
                continue
            try:
                event_hash = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            if event_hash not in committed:
                pending_ids.append(event_hash)
        event_by_id: dict[int, Json] = {}
        fallback_events: list[Json] = []
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            try:
                event_hash = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            event_by_id[event_hash] = record
            if not pending_ids and session_buffer_key(record.get("envelope", {})) == key and event_hash not in committed:
                fallback_events.append(record)
        events = [event_by_id[event_hash] for event_hash in pending_ids if event_hash in event_by_id]
        if not events:
            events = fallback_events
        with self._session_buffer_cache_lock:
            self._context_event_by_hash.update(event_by_id)
            self._session_committed_event_ids_by_key[key] = set(committed)
            cached_pending_ids: list[int] = []
            for record in events:
                try:
                    cached_pending_ids.append(int(record.get("event_id_hash")))
                except (TypeError, ValueError):
                    continue
            self._session_pending_event_ids_by_key[key] = cached_pending_ids
        if limit is not None:
            return events[:limit]
        return events

    def append_session_buffer_event(self, *, envelope: Json, event_id_hash: int, node_hash: int, node_path: list[str], hook: Json | None) -> None:
        key = session_buffer_key(envelope)
        self.append(
            {
                "record_type": "session_buffer_event",
                "buffer_key_hash": stable_hash(":".join(key)),
                "buffer_key": list(key),
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "storage_options": envelope.get("storage_options", {}),
                "storage_route": envelope.get("storage_route", {}),
                "node_path": node_path,
                "scope": envelope["scope"],
                "status": "pending",
                "agent_hook": hook,
                "created_at_ms": envelope["ingestion_time_ms"],
            }
        )

    def default_session_node_path(self, scope: Json) -> list[str]:
        tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
        user_id = str(scope.get("user_id") or local_account_user_id())
        session_id = str(scope.get("session_id") or user_id or "default_session")
        return [f"tenant:{tenant_id}", f"user:{user_id}", f"session:{session_id}"]

    def default_shared_context_node_path(self, scope: Json, *, kind: str, sharing_scope: str) -> list[str]:
        collection = "skills" if kind == "skill" else "resources"
        if sharing_scope == "global_shared":
            return ["global", "shared", collection]
        tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
        return [f"tenant:{tenant_id}", "shared", collection]

    def resource_sharing_scope(self, args: Json, envelope: Json, deployment_scope: str) -> str:
        metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
        explicit = str(args.get("sharing_scope") or metadata.get("sharing_scope") or "").strip().lower()
        if explicit in {"tenant_shared", "global_shared", "private_user"}:
            return explicit
        if deployment_scope == "global":
            return "global_shared"
        scope = envelope.get("scope", {}) if isinstance(envelope.get("scope"), dict) else {}
        if not scope.get("user_id") and not scope.get("session_id"):
            return "tenant_shared" if scope.get("tenant_id") else "global_shared"
        return "private_user"

    def default_resource_node_path(self, args: Json, envelope: Json, *, deployment_scope: str, sharing_scope: str) -> list[str]:
        metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
        if metadata.get("node_path"):
            return [str(part) for part in metadata.get("node_path", []) if str(part)]
        if sharing_scope in {"tenant_shared", "global_shared"}:
            return self.default_shared_context_node_path(envelope.get("scope", {}), kind=str(envelope.get("kind") or "resource"), sharing_scope=sharing_scope)
        return self.default_session_node_path(envelope.get("scope", {}))

    def ensure_context_node_path(self, *, node_path: list[str], scope: Json, updated_at_ms: int) -> Json:
        prefixes = node_prefixes(node_path)
        if not prefixes:
            return {"nodes_created": 0, "child_refs_created": 0, "node_hashes": []}

        compact_scope = serving_scope_ref(scope)
        self._ensure_context_node_cache_loaded()
        existing_nodes = self._context_node_hashes
        existing_child_refs = self._context_child_ref_hashes
        node_hashes: list[int] = []
        nodes_created = 0
        child_refs_created = 0
        for prefix in prefixes:
            node_hash = stable_hash("/".join(prefix))
            node_hashes.append(node_hash)
            parent_path = prefix[:-1]
            parent_hash = stable_hash("/".join(parent_path)) if parent_path else 0
            if node_hash not in existing_nodes:
                self.append(
                    {
                        "record_type": "context_node",
                        "node_hash": node_hash,
                        "parent_hash": parent_hash,
                        "node_name": prefix[-1],
                        "node_path": prefix,
                        "depth": len(prefix),
                        "scope": scope,
                        "created_at_ms": updated_at_ms,
                        "updated_at_ms": updated_at_ms,
                    }
                )
                existing_nodes.add(node_hash)
                nodes_created += 1
            if parent_path:
                child_ref_hash = stable_hash(f"child:{parent_hash}:{node_hash}")
                if child_ref_hash not in existing_child_refs:
                    self.append(
                        {
                            "record_type": "context_child_ref",
                            "child_ref_hash": child_ref_hash,
                            "parent_hash": parent_hash,
                            "child_hash": node_hash,
                            "child_name": prefix[-1],
                            "parent_path": parent_path,
                            "child_path": prefix,
                            "depth": len(prefix),
                            "scope": scope,
                            "created_at_ms": updated_at_ms,
                            "updated_at_ms": updated_at_ms,
                        }
                    )
                    existing_child_refs.add(child_ref_hash)
                    child_refs_created += 1
        return {
            "nodes_created": nodes_created,
            "child_refs_created": child_refs_created,
            "node_hashes": node_hashes,
        }

    def session_commit(self, args: Json, *, hook: Json | None = None) -> Json:
        scope = optional_object(args, "scope")
        threshold = args.get("threshold_messages", 20)
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        force = bool(args.get("force", True))
        commit_reason = optional_string(args, "commit_reason") or ("manual_api" if force else "threshold")
        idle_timeout_ms = args.get("idle_timeout_ms")
        if idle_timeout_ms is not None and (not isinstance(idle_timeout_ms, int) or idle_timeout_ms < 0):
            raise MatrixArkError("idle_timeout_ms must be a non-negative integer")
        max_messages = args.get("max_messages")
        if max_messages is not None and (not isinstance(max_messages, int) or max_messages <= 0):
            raise MatrixArkError("max_messages must be a positive integer")
        pending_all = self.pending_session_events(scope)
        pending_event_count = len(pending_all)
        idle_elapsed_ms = 0
        idle_ready = False
        if pending_all and idle_timeout_ms is not None:
            latest_event_time = max(
                int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
                for record in pending_all
            )
            idle_elapsed_ms = max(0, now_ms() - latest_event_time)
            idle_ready = idle_elapsed_ms >= idle_timeout_ms
        threshold_ready = pending_event_count >= threshold
        if not force and not threshold_ready and not idle_ready:
            return {
                "status": "deferred",
                "pending_event_count": pending_event_count,
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "reason": "session buffer below extraction threshold and idle timeout not reached",
            }
        if max_messages is not None:
            commit_limit = max_messages
        elif force or idle_ready:
            commit_limit = None
        else:
            commit_limit = threshold
        pending = pending_all[:commit_limit] if commit_limit is not None else pending_all
        messages = []
        source_event_ids = []
        for record in pending:
            message = message_from_event_record(record)
            if not message:
                continue
            messages.append(message)
            source_event_ids.append(record["event_id_hash"])
        if not messages:
            return {
                "status": "empty",
                "pending_event_count": pending_event_count,
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
            }
        metadata = optional_object(args, "metadata")
        storage_options = normalize_storage_options(args, metadata)
        if "node_path" not in metadata:
            metadata = {**metadata, "node_path": self.default_session_node_path(scope)}
        batch_result = self.batch_extract(
            {
                "messages": messages,
                "scope": scope,
                "metadata": metadata,
                "storage_options": storage_options,
                "threshold_messages": threshold,
                "force": True,
                "derive_from_existing_events": True,
                "source_event_ids": source_event_ids,
                "understanding_provider": args.get("understanding_provider"),
                "extraction_provider": args.get("extraction_provider"),
                "segment_provider": args.get("segment_provider"),
                "segment_model": args.get("segment_model"),
                "segment_model_path": args.get("segment_model_path"),
                "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                "segment_provider_fallback": args.get("segment_provider_fallback"),
                "skip_prior_context": bool(args.get("skip_prior_context", False)),
            },
            hook=hook,
        )
        commit_id_hash = stable_hash(f"commit:{scope}:{source_event_ids}:{now_ms()}")
        self.append(
            {
                "record_type": "context_batch_commit",
                "commit_id_hash": commit_id_hash,
                "batch_id_hash": batch_result.get("batch_id_hash"),
                "node_hash": batch_result.get("node_hash"),
                "node_path": metadata["node_path"],
                "source_event_ids": source_event_ids,
                "scope": scope,
                "message_count": len(messages),
                "threshold_messages": threshold,
                "commit_reason": commit_reason,
                "trigger_policy": "force" if force else "idle_timeout" if idle_ready else "threshold",
                "pending_event_count_before_commit": pending_event_count,
                "committed_event_count": len(source_event_ids),
                "idle_timeout_ms": idle_timeout_ms,
                "idle_elapsed_ms": idle_elapsed_ms,
                "agent_hook": hook,
                "storage_options": storage_options,
                "storage_route": canonical_storage_route(storage_options),
                "created_at_ms": now_ms(),
            }
        )
        return {
            **batch_result,
            "status": "committed",
            "commit_id_hash": commit_id_hash,
            "storage_options": storage_options,
            "storage_route": canonical_storage_route(storage_options),
            "pending_event_count": pending_event_count,
            "committed_event_count": len(source_event_ids),
            "source_event_ids": source_event_ids,
            "commit_reason": commit_reason,
            "trigger_policy": "force" if force else "idle_timeout" if idle_ready else "threshold",
            "idle_timeout_ms": idle_timeout_ms,
            "idle_elapsed_ms": idle_elapsed_ms,
            "raw_events_duplicated": False,
        }

    def node_summary_source_records(
        self,
        *,
        records: list[Json],
        node_path: list[str],
        scope: Json,
        node_hash: int | None = None,
        max_events: int = 8,
        max_child_summaries: int = 8,
    ) -> tuple[list[Json], list[Json]]:
        prefix = node_path_tuple(node_path)
        target_node_hash = int(node_hash) if node_hash is not None else stable_hash("/".join(node_path))
        child_summaries: list[Json] = []
        events: list[Json] = []
        seen_summary_keys: set[tuple[int, str]] = set()
        for record in reversed(records):
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            record_path = node_path_tuple(record.get("node_path", []))
            path_matches = bool(record_path and starts_with_path(record_path, prefix))
            node_matches = record.get("node_hash") == target_node_hash
            if not path_matches and not node_matches:
                continue
            if record.get("record_type") == "context_summary" and record.get("summary_type") in {"node_l0", "node_l1", "batch_l0", "session_l0", "resource_l0", "skill_l0"}:
                if len(child_summaries) >= max_child_summaries:
                    continue
                try:
                    node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    continue
                key = (node_hash, str(record.get("summary_type", "")))
                if key in seen_summary_keys:
                    continue
                if node_path_tuple(record.get("node_path", [])) == prefix:
                    continue
                seen_summary_keys.add(key)
                child_summaries.append(record)
            elif record.get("record_type") == "context_event":
                if len(events) >= max_events:
                    continue
                events.append(record)
        return list(reversed(events[:max_events])), list(reversed(child_summaries[:max_child_summaries]))

    def context_event_ingestion_time_ms(self, record: Json, debug_by_ref: dict[Any, Json] | None = None) -> int:
        event_hash = record.get("event_id_hash")
        debug_payload = (debug_by_ref or {}).get(event_hash, {}) if event_hash is not None else {}
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
        if not isinstance(envelope, dict):
            envelope = {}
        for value in (envelope.get("ingestion_time_ms"), record.get("updated_at_ms"), record.get("created_at_ms")):
            try:
                timestamp = int(value)
            except (TypeError, ValueError):
                continue
            if timestamp > 0:
                return timestamp
        return 0

    def _write_time_compression_from_events(
        self,
        *,
        scope: Json,
        node_hash: int,
        node_path: list[str],
        selected: list[Json],
        event_times: dict[int, int],
        compressed_time_ms: int,
        summary: str = "",
        truncated: bool = False,
        mode: str = "manual",
        raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
        summary_provider_meta: Json | None = None,
    ) -> Json:
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        source_event_ids = [int(record["event_id_hash"]) for record in selected if record.get("event_id_hash") is not None]
        if not source_event_ids:
            raise MatrixArkError("source events need event_id_hash for compression")
        source_times = [event_times.get(event_id, 0) for event_id in source_event_ids if event_times.get(event_id, 0) > 0]
        source_start_ms = min(source_times) if source_times else compressed_time_ms
        source_end_ms = max(source_times) if source_times else compressed_time_ms
        if not summary:
            snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
            suffix = " plus additional source events" if truncated else ""
            summary = (
                f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
                f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
            )
        compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
        record = {
            "record_type": "context_compression_event",
            "compression_id_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "source_start_ms": source_start_ms,
            "source_end_ms": source_end_ms,
            "compressed_time_ms": compressed_time_ms,
            "summary_text": summarize_text(summary, limit=1200),
            "source_event_ids": source_event_ids,
            "source_event_count": len(source_event_ids),
            "truncated_source_events": truncated,
            "operator": "TIME_COMPRESS",
            "compression_mode": mode,
            "summary_provider": summary_provider_meta
            or {
                "provider": "deterministic",
                "model": "",
                "fallback_used": False,
            },
            "compression_safety": {
                "source_event_ids_retained": bool(source_event_ids),
                "source_event_count": len(source_event_ids),
                "summary_non_empty": bool(summary.strip()),
                "raw_events_remain_replayable": True,
                "ttl_marker_only": True,
            },
            "retention_policy": {
                "raw_event_ttl_after_compression_ms": max(0, int(raw_event_ttl_after_compression_ms)),
                "evict_after_ms": compressed_time_ms + max(0, int(raw_event_ttl_after_compression_ms))
                if raw_event_ttl_after_compression_ms > 0
                else 0,
                "requires_no_recent_reinforcement": True,
            },
            "updated_at_ms": compressed_time_ms,
        }
        self.append(record)
        summary_vector = embedding_for_text(record["summary_text"])
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "compression_summary",
                "ref_type": "compression",
                "ref_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(summary_vector),
                "model": embedding_model_name(),
                "vector": summary_vector,
                "scope": scope,
                "updated_at_ms": compressed_time_ms,
            }
        )
        retention_records = []
        evict_after_ms = int(record["retention_policy"]["evict_after_ms"] or 0)
        for event_id in source_event_ids:
            retention_records.append(
                {
                    "record_type": "context_event_retention_marker",
                    "event_id_hash": event_id,
                    "compression_id_hash": compression_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "retention_state": "compressed_retained",
                    "evict_after_ms": evict_after_ms,
                    "raw_events_remain_replayable": True,
                    "requires_no_recent_reinforcement": True,
                    "created_at_ms": compressed_time_ms,
                    "updated_at_ms": compressed_time_ms,
                }
            )
        if retention_records:
            self.append_many(retention_records)
        return record

    def auto_time_compress_node_events(
        self,
        *,
        records: list[Json],
        scope: Json,
        node_hash: int,
        node_path: list[str],
        compressed_time_ms: int,
        max_raw_events_per_node: int = TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        max_source_events: int = TIME_COMPRESSION_WINDOW_EVENTS,
        min_source_events: int = TIME_COMPRESSION_MIN_EVENTS,
        max_windows: int = TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        min_event_age_ms: int = TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
    ) -> Json:
        max_raw_events_per_node = max(1, int(max_raw_events_per_node))
        max_source_events = max(1, int(max_source_events))
        min_source_events = max(1, int(min_source_events))
        max_windows = max(0, int(max_windows))
        if max_windows <= 0:
            return {"status": "disabled", "created_count": 0, "created": []}
        debug_by_ref = {
            record.get("ref_hash"): record.get("debug_payload", {})
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
        }
        compressed_source_ids: set[int] = set()
        reinforced_source_ids: set[int] = set()
        for record in records:
            if record.get("record_type") != "context_compression_event":
                if record.get("record_type") == "context_recall_reinforcement":
                    if int(record.get("node_hash") or 0) != node_hash:
                        continue
                    if not scope_matches(candidate_access_scope(record), scope):
                        continue
                    if int(record.get("protected_until_ms") or 0) < compressed_time_ms:
                        continue
                    try:
                        reinforced_source_ids.add(int(record.get("event_id_hash")))
                    except (TypeError, ValueError):
                        pass
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            for event_id in record.get("source_event_ids", []) or []:
                try:
                    compressed_source_ids.add(int(event_id))
                except (TypeError, ValueError):
                    pass
        events: list[Json] = []
        event_times: dict[int, int] = {}
        event_scopes: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            try:
                event_hash = int(record.get("event_id_hash"))
            except (TypeError, ValueError):
                continue
            event_time = self.context_event_ingestion_time_ms(record, debug_by_ref)
            if event_time <= 0:
                continue
            events.append(record)
            event_times[event_hash] = event_time
            event_scopes[event_hash] = candidate_access_scope(record)
        events.sort(key=lambda record: (event_times.get(int(record.get("event_id_hash") or 0), 0), int(record.get("event_id_hash") or 0)))
        if len(events) <= max_raw_events_per_node:
            return {
                "status": "skipped",
                "reason": "raw_event_count_within_threshold",
                "raw_event_count": len(events),
                "max_raw_events_per_node": max_raw_events_per_node,
                "created_count": 0,
                "created": [],
            }
        newest_raw_ids = {
            int(record.get("event_id_hash"))
            for record in events[-max_raw_events_per_node:]
            if record.get("event_id_hash") is not None
        }
        cold_cutoff_ms = compressed_time_ms - max(0, int(min_event_age_ms))
        old_uncompressed = [
            record
            for record in events
            if int(record.get("event_id_hash") or 0) not in newest_raw_ids
            and int(record.get("event_id_hash") or 0) not in compressed_source_ids
            and int(record.get("event_id_hash") or 0) not in reinforced_source_ids
            and (
                min_event_age_ms <= 0
                or event_times.get(int(record.get("event_id_hash") or 0), compressed_time_ms) <= cold_cutoff_ms
            )
        ]
        created: list[Json] = []
        for window_start in range(0, len(old_uncompressed), max_source_events):
            if len(created) >= max_windows:
                break
            window = old_uncompressed[window_start : window_start + max_source_events]
            if len(window) < min_source_events:
                continue
            first_hash = int(window[0].get("event_id_hash") or 0)
            compression_scope = event_scopes.get(first_hash, scope)
            source_ids = [int(record["event_id_hash"]) for record in window if record.get("event_id_hash") is not None]
            source_times = [event_times.get(event_id, 0) for event_id in source_ids if event_times.get(event_id, 0) > 0]
            summary_result = generate_time_compression_summary(
                node_path=node_path,
                source_start_ms=min(source_times) if source_times else compressed_time_ms,
                source_end_ms=max(source_times) if source_times else compressed_time_ms,
                event_texts=[str(record.get("text", "")) for record in window if record.get("text")],
                max_raw_events_per_node=max_raw_events_per_node,
            )
            created.append(
                self._write_time_compression_from_events(
                    scope=compression_scope,
                    node_hash=node_hash,
                    node_path=node_path,
                    selected=window,
                    event_times=event_times,
                    compressed_time_ms=compressed_time_ms,
                    summary=str(summary_result.get("summary", "")),
                    truncated=len(old_uncompressed) > len(source_ids),
                    mode="automatic",
                    raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
                    summary_provider_meta={
                        "provider": summary_result.get("provider", "deterministic"),
                        "model": summary_result.get("model", ""),
                        "fallback_used": bool(summary_result.get("fallback_used", False)),
                        "warning": summary_result.get("warning", ""),
                    },
                )
            )
        return {
            "status": "ok" if created else "skipped",
            "reason": "" if created else "no_uncompressed_old_window_met_minimum",
            "raw_event_count": len(events),
            "max_raw_events_per_node": max_raw_events_per_node,
            "min_event_age_ms": max(0, int(min_event_age_ms)),
            "cold_cutoff_ms": cold_cutoff_ms,
            "old_uncompressed_event_count": len(old_uncompressed),
            "reinforced_event_count": len(reinforced_source_ids),
            "created_count": len(created),
            "created": [
                {
                    "compression_id_hash": item.get("compression_id_hash"),
                    "source_start_ms": item.get("source_start_ms"),
                    "source_end_ms": item.get("source_end_ms"),
                    "source_event_count": item.get("source_event_count"),
                }
                for item in created
            ],
        }

    def node_summary_dirty_records(
        self,
        *,
        node_path: list[str],
        scope: Json,
        updated_at_ms: int,
        source_ref_type: str,
        source_hash_field: str,
        source_hash: int,
        dirty_reason: str = "new_event",
        propagate_depth: int | None = None,
    ) -> tuple[list[int], list[Json]]:
        prefixes = node_prefixes(node_path)
        if propagate_depth is not None and propagate_depth >= 0:
            prefixes = prefixes[max(0, len(prefixes) - propagate_depth - 1) :]
        dirty_hashes: list[int] = []
        records: list[Json] = []
        for prefix in prefixes:
            node_hash = stable_hash("/".join(prefix))
            dirty_hash = stable_hash(
                f"summary_dirty:{node_hash}:{dirty_reason}:{source_ref_type}:{source_hash}:{updated_at_ms}"
            )
            dirty_hashes.append(dirty_hash)
            records.append(
                {
                    "record_type": "context_summary_dirty",
                    "dirty_hash": dirty_hash,
                    "node_hash": node_hash,
                    "node_path": prefix,
                    "depth": len(prefix),
                    "dirty_reason": dirty_reason,
                    "source_ref_type": source_ref_type,
                    source_hash_field: source_hash,
                    "changed_ref_count": 1,
                    "propagate_depth": propagate_depth if propagate_depth is not None else len(node_path),
                    "scope": scope,
                    "status": "pending",
                    "created_at_ms": updated_at_ms,
                    "updated_at_ms": updated_at_ms,
                }
            )
        return dirty_hashes, records

    def mark_node_summary_dirty(
        self,
        *,
        node_path: list[str],
        scope: Json,
        updated_at_ms: int,
        source_ref_type: str,
        source_hash_field: str,
        source_hash: int,
        dirty_reason: str = "new_event",
        propagate_depth: int | None = None,
    ) -> list[int]:
        dirty_hashes, records = self.node_summary_dirty_records(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_ref_type,
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason=dirty_reason,
            propagate_depth=propagate_depth,
        )
        self.append_many(records)
        return dirty_hashes

    def refresh_dirty_node_summaries(
        self,
        *,
        scope: Json,
        limit: int = 64,
        refreshed_at_ms: int | None = None,
        max_raw_events_per_node: int = TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE,
        compression_window_events: int = TIME_COMPRESSION_WINDOW_EVENTS,
        min_compression_events: int = TIME_COMPRESSION_MIN_EVENTS,
        max_compression_windows_per_node: int = TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
        min_compression_event_age_ms: int = TIME_COMPRESSION_MIN_EVENT_AGE_MS,
        raw_event_ttl_after_compression_ms: int = TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
    ) -> Json:
        refreshed_at_ms = refreshed_at_ms or now_ms()
        records = self.read_all()
        completed_dirty_hashes = {
            int(record.get("dirty_hash"))
            for record in records
            if record.get("record_type") in {"context_summary_refresh_audit", "context_summary_dirty"}
            and record.get("status") in {"refreshed", "completed"}
            and record.get("dirty_hash") is not None
        }
        pending_by_node: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_summary_dirty":
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            try:
                dirty_hash = int(record.get("dirty_hash"))
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if dirty_hash in completed_dirty_hashes:
                continue
            current = pending_by_node.get(node_hash)
            if current is None or int(record.get("updated_at_ms") or 0) >= int(current.get("updated_at_ms") or 0):
                pending_by_node[node_hash] = record
        if len(pending_by_node) < limit:
            event_counts_by_node: dict[int, int] = {}
            event_path_by_node: dict[int, list[str]] = {}
            event_scope_by_node: dict[int, Json] = {}
            oldest_event_time_by_node: dict[int, int] = {}
            debug_by_ref = {
                record.get("ref_hash"): record.get("debug_payload", {})
                for record in records
                if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
            }
            for record in records:
                if record.get("record_type") != "context_event":
                    continue
                if record.get("source_chunk_hash"):
                    continue
                if not scope_matches(candidate_access_scope(record), scope):
                    continue
                try:
                    event_node_hash = int(record.get("node_hash"))
                except (TypeError, ValueError):
                    continue
                event_counts_by_node[event_node_hash] = event_counts_by_node.get(event_node_hash, 0) + 1
                event_path_by_node[event_node_hash] = [str(part) for part in record.get("node_path", [])]
                event_scope_by_node[event_node_hash] = candidate_access_scope(record)
                event_time = self.context_event_ingestion_time_ms(record, debug_by_ref)
                if event_time > 0:
                    existing_time = oldest_event_time_by_node.get(event_node_hash)
                    if existing_time is None or event_time < existing_time:
                        oldest_event_time_by_node[event_node_hash] = event_time
            cold_cutoff_ms = refreshed_at_ms - max(0, int(min_compression_event_age_ms))
            for node_hash, event_count in sorted(event_counts_by_node.items(), key=lambda item: item[1], reverse=True):
                if len(pending_by_node) >= limit:
                    break
                if node_hash in pending_by_node:
                    continue
                if event_count <= max_raw_events_per_node:
                    continue
                if min_compression_event_age_ms > 0 and oldest_event_time_by_node.get(node_hash, refreshed_at_ms) > cold_cutoff_ms:
                    continue
                node_path = event_path_by_node.get(node_hash, [])
                if not node_path:
                    continue
                synthetic_dirty_hash = stable_hash(
                    f"scheduled_time_compression:{node_hash}:{event_count}:{oldest_event_time_by_node.get(node_hash, 0)}:{refreshed_at_ms}"
                )
                if synthetic_dirty_hash in completed_dirty_hashes:
                    continue
                pending_by_node[node_hash] = {
                    "record_type": "context_summary_dirty",
                    "dirty_hash": synthetic_dirty_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "depth": len(node_path),
                    "dirty_reason": "scheduled_time_compression",
                    "source_ref_type": "event_window",
                    "changed_ref_count": event_count,
                    "propagate_depth": 0,
                    "scope": event_scope_by_node.get(node_hash, scope),
                    "status": "pending",
                    "created_at_ms": refreshed_at_ms,
                    "updated_at_ms": refreshed_at_ms,
                }
        refreshed = []
        for dirty in sorted(pending_by_node.values(), key=lambda item: int(item.get("updated_at_ms") or 0))[:limit]:
            node_path = [str(part) for part in dirty.get("node_path", [])]
            if not node_path:
                continue
            node_hash = int(dirty["node_hash"])
            events, child_summaries = self.node_summary_source_records(
                records=records,
                node_path=node_path,
                scope=dirty.get("scope", scope),
                node_hash=node_hash,
            )
            event_texts = [str(record.get("text", "")) for record in events if record.get("text")]
            child_summary_texts = [
                str(record.get("summary_text", ""))
                for record in child_summaries
                if record.get("summary_text")
            ]
            source_text = " ".join(child_summary_texts + event_texts)
            if not source_text:
                source_text = " ".join(node_path)
            prefix_label = " / ".join(node_path)
            l0_summary = summarize_text(f"{prefix_label} :: {source_text}", limit=220)
            source_event_ids = [int(record["event_id_hash"]) for record in events if record.get("event_id_hash") is not None]
            source_summary_hashes = [
                int(record.get("summary_hash") or record.get("node_hash"))
                for record in child_summaries
                if record.get("summary_hash") is not None or record.get("node_hash") is not None
            ]
            l1_policy = node_l1_generation_policy(
                source_text=source_text,
                event_count=len(source_event_ids),
                child_summary_count=len(source_summary_hashes),
            )
            summary_specs = [("node_l0", l0_summary, "node_l0")]
            if l1_policy["generate_l1"]:
                l1_summary = summarize_text(
                    f"Context node {prefix_label}. Rich overview: {source_text}. "
                    f"This node belongs to path {prefix_label} and should be used for tree-first retrieval before leaf event/entity recall.",
                    limit=1200,
                )
                summary_specs.append(("node_l1", l1_summary, "node_l1"))
            version_hash = stable_hash(
                f"summary_version:{node_hash}:{dirty.get('dirty_hash')}:{source_event_ids}:{source_summary_hashes}:{refreshed_at_ms}:{l1_policy}"
            )
            for level, summary_text, embedding_type in summary_specs:
                summary_hash = stable_hash(f"context_summary:{level}:{node_hash}")
                self.append(
                    {
                        "record_type": "context_summary",
                        "summary_type": level,
                        "summary_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "summary_text": summary_text,
                        "source_event_ids": source_event_ids,
                        "source_summary_hashes": source_summary_hashes,
                        "summary_generation_policy": l1_policy,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": embedding_type,
                        "ref_type": "summary",
                        "ref_hash": summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "dim": len(embedding_for_text(summary_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(summary_text),
                        "summary_generation_policy": l1_policy,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
            compression_refresh = self.auto_time_compress_node_events(
                records=records,
                scope=dirty.get("scope", scope),
                node_hash=node_hash,
                node_path=node_path,
                compressed_time_ms=refreshed_at_ms,
                max_raw_events_per_node=max_raw_events_per_node,
                max_source_events=compression_window_events,
                min_source_events=min_compression_events,
                max_windows=max_compression_windows_per_node,
                min_event_age_ms=min_compression_event_age_ms,
                raw_event_ttl_after_compression_ms=raw_event_ttl_after_compression_ms,
            )
            completion_marker = {
                "record_type": "context_summary_dirty",
                "dirty_hash": dirty.get("dirty_hash"),
                "node_hash": node_hash,
                "node_path": node_path,
                "scope": dirty.get("scope", scope),
                "status": "completed",
                "updated_at_ms": refreshed_at_ms,
                "completed_at_ms": refreshed_at_ms,
            }
            self.append(completion_marker)
            if ENABLE_SUMMARY_REFRESH_AUDIT:
                self.append(
                    {
                        "record_type": "context_summary_refresh_audit",
                        "dirty_hash": dirty.get("dirty_hash"),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "summary_version_hash": version_hash,
                        "source_event_ids": source_event_ids,
                        "source_summary_hashes": source_summary_hashes,
                        "source_event_count": len(source_event_ids),
                        "source_summary_count": len(source_summary_hashes),
                        "generated_summary_types": [spec[0] for spec in summary_specs],
                        "summary_generation_policy": l1_policy,
                        "time_compression_policy": {
                            "automatic": True,
                            "max_raw_events_per_node": max_raw_events_per_node,
                            "compression_window_events": compression_window_events,
                            "min_compression_events": min_compression_events,
                            "max_compression_windows_per_node": max_compression_windows_per_node,
                            "min_compression_event_age_ms": min_compression_event_age_ms,
                            "raw_event_ttl_after_compression_ms": raw_event_ttl_after_compression_ms,
                        },
                        "time_compression": compression_refresh,
                        "status": "refreshed",
                        "worker": "matrixark-local-async-summary-worker",
                        "refreshed_at_ms": refreshed_at_ms,
                        "scope": dirty.get("scope", scope),
                    }
                )
            refreshed.append(
                {
                    "dirty_hash": dirty.get("dirty_hash"),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "summary_version_hash": version_hash,
                    "source_event_count": len(source_event_ids),
                    "source_summary_count": len(source_summary_hashes),
                    "generated_summary_types": [spec[0] for spec in summary_specs],
                    "summary_generation_policy": l1_policy,
                    "time_compression": compression_refresh,
                }
            )
        return {
            "status": "ok",
            "refreshed_count": len(refreshed),
            "compression_created_count": sum(int(item.get("time_compression", {}).get("created_count", 0)) for item in refreshed),
            "refreshed": refreshed,
        }

    def append_node_summary_embeddings(
        self,
        *,
        node_path: list[str],
        source_text: str,
        scope: Json,
        updated_at_ms: int,
        source_hash_field: str,
        source_hash: int,
    ) -> Json:
        dirty_hashes = self.mark_node_summary_dirty(
            node_path=node_path,
            scope=scope,
            updated_at_ms=updated_at_ms,
            source_ref_type=source_hash_field.removeprefix("source_").removesuffix("_hash"),
            source_hash_field=source_hash_field,
            source_hash=source_hash,
            dirty_reason="new_event",
        )
        return {
            "status": "dirty_marked",
            "dirty_hashes": dirty_hashes,
            "refresh_result": None,
            "async_required": True,
        }

    def refresh_summaries(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 64)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        refreshed_at_ms = args.get("refreshed_at_ms")
        if refreshed_at_ms is not None and not isinstance(refreshed_at_ms, int):
            raise MatrixArkError("refreshed_at_ms must be an integer")
        return self.refresh_dirty_node_summaries(
            scope=scope,
            limit=limit,
            refreshed_at_ms=refreshed_at_ms,
            max_raw_events_per_node=integer_arg(args, "max_raw_events_per_node", TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE, minimum=1),
            compression_window_events=integer_arg(args, "compression_window_events", TIME_COMPRESSION_WINDOW_EVENTS, minimum=1),
            min_compression_events=integer_arg(args, "min_compression_events", TIME_COMPRESSION_MIN_EVENTS, minimum=1),
            max_compression_windows_per_node=integer_arg(
                args,
                "max_compression_windows_per_node",
                TIME_COMPRESSION_MAX_WINDOWS_PER_REFRESH,
                minimum=0,
            ),
            min_compression_event_age_ms=integer_arg(
                args,
                "min_compression_event_age_ms",
                TIME_COMPRESSION_MIN_EVENT_AGE_MS,
                minimum=0,
            ),
            raw_event_ttl_after_compression_ms=integer_arg(
                args,
                "raw_event_ttl_after_compression_ms",
                TIME_COMPRESSION_RAW_EVENT_TTL_AFTER_COMPRESSION_MS,
                minimum=0,
            ),
        )

    def latest_skill_controls(self, records: list[Json] | None = None) -> dict[int, Json]:
        controls: dict[int, Json] = {}
        for record in reversed(records if records is not None else self.read_all()):
            if record.get("record_type") != "skill_registry_update":
                continue
            try:
                skill_hash = int(record.get("skill_hash"))
            except (TypeError, ValueError):
                continue
            if skill_hash not in controls:
                controls[skill_hash] = record
        return controls

    def _dashboard_record_scope(self, record: Json) -> Json:
        scope = candidate_access_scope(record)
        access_scope = candidate_access_scope(record)
        if isinstance(scope, dict) and isinstance(access_scope, dict):
            merged = {**scope, **access_scope}
            if scope.get("agent_name") and not merged.get("agent_name"):
                merged["agent_name"] = scope["agent_name"]
            explicit = scope.get("_explicit_scope_keys")
            if isinstance(explicit, list):
                merged["_explicit_scope_keys"] = explicit
            return merged
        return access_scope

    def _dashboard_message_rows(self, records: list[Json], scope: Json) -> list[Json]:
        rows: list[Json] = []
        debug_by_ref: dict[Any, Json] = {}
        for record in records:
            if record.get("record_type") != "context_debug_record" or record.get("ref_type") != "event":
                continue
            debug_by_ref[record.get("ref_hash")] = record.get("debug_payload", {}) if isinstance(record.get("debug_payload"), dict) else {}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if not scope_matches(self._dashboard_record_scope(record), scope):
                continue
            debug_payload = debug_by_ref.get(record.get("event_id_hash"), {})
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
            if not isinstance(envelope, dict):
                envelope = {}
            kind = str(envelope.get("kind") or record.get("source_kind") or "")
            if kind not in {"message", "feedback", "business_data"}:
                continue
            messages = envelope.get("messages", []) if isinstance(envelope.get("messages"), list) else []
            if not messages and kind == "message":
                messages = [{"role": "unknown", "content": record.get("text", "")}]
            extraction = record.get("internal_extraction", {}) if isinstance(record.get("internal_extraction"), dict) else debug_payload.get("internal_extraction", {})
            if not isinstance(extraction, dict):
                extraction = {}
            for message in messages:
                if not isinstance(message, dict):
                    continue
                rows.append(
                    {
                        "row_type": "message",
                        "event_id_hash": record.get("event_id_hash", 0),
                        "kind": kind,
                        "role": message.get("role", ""),
                        "name": message.get("name", ""),
                        "content": message.get("content", ""),
                        "summary_text": record.get("summary_text", ""),
                        "classification": non_default_classification(extraction.get("classification", "")),
                        "event_type": extraction.get("event_type", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": envelope.get("scope", scope_from_serving_record(record)),
                        "agent_name": envelope.get("scope", {}).get("agent_name", "") if isinstance(envelope.get("scope"), dict) else "",
                        "created_at_ms": message.get("created_at_ms") or envelope.get("ingestion_time_ms") or record.get("updated_at_ms", 0),
                    }
                )
        return rows

    def _dashboard_rows_for_table(self, records: list[Json], table: str, scope: Json) -> list[Json]:
        rows: list[Json] = []
        if table == "messages":
            return self._dashboard_message_rows(records, scope)
        for record in records:
            record_type = str(record.get("record_type") or "")
            if not scope_matches(self._dashboard_record_scope(record), scope):
                continue
            if table == "resources" and record_type in {"resource_import_task", "resource_manifest", "resource_chunk"}:
                rows.append(
                    {
                        "row_type": record_type,
                        "task_hash": record.get("task_hash", record.get("import_task_hash", 0)),
                        "resource_hash": record.get("resource_hash", 0),
                        "chunk_hash": record.get("chunk_hash", 0),
                        "status": record.get("status", ""),
                        "raw_uri": record.get("raw_uri", ""),
                        "requested_raw_uri": record.get("requested_raw_uri", ""),
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": record.get("resource_version", ""),
                        "raw_uri_hash": record.get("raw_uri_hash", 0),
                        "source_locator": record.get("source_locator", record.get("metadata", {}).get("source_locator", "")),
                        "unit_kind": record.get("unit_kind", record.get("metadata", {}).get("unit_kind", "")),
                        "token_estimate": record.get("token_estimate", 0),
                        "chunk_count": record.get("chunk_count", 0),
                        "parse_warnings": record.get("parse_warnings", []),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                    }
                )
            elif table == "skills" and record_type in {"skill_manifest", "skill_registry", "skill_section"}:
                rows.append(
                    {
                        "row_type": record_type,
                        "skill_hash": record.get("skill_hash", 0),
                        "section_hash": record.get("section_hash", 0),
                        "name": record.get("name", record.get("skill_name", "")),
                        "heading": record.get("heading", ""),
                        "status": record.get("status", ""),
                        "version": record.get("version", ""),
                        "triggers": record.get("triggers", []),
                        "allowed_tools": record.get("allowed_tools", []),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", 0),
                    }
                )
            elif table == "events" and record_type == "context_event":
                rows.append(
                    {
                        "row_type": record_type,
                        "event_id_hash": record.get("event_id_hash", 0),
                        "text": record.get("text", ""),
                        "summary_text": record.get("summary_text", ""),
                        "classification": non_default_classification(record.get("internal_extraction", {}).get("classification", "")),
                        "event_type": record.get("event_type", record.get("internal_extraction", {}).get("event_type", "")),
                        "source_chunk_hash": record.get("source_chunk_hash", 0),
                        "resource_hash": record.get("resource_hash", 0),
                        "source_locator": record.get("source_locator", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record.get("envelope", {}).get("scope", record.get("scope", {})),
                        "updated_at_ms": record.get("envelope", {}).get("ingestion_time_ms", record.get("updated_at_ms", 0)),
                    }
                )
            elif table == "entities" and record_type == "context_entity":
                rows.append(
                    {
                        "row_type": record_type,
                        "entity_hash": record.get("entity_hash", 0),
                        "entity_type": record.get("entity_type", ""),
                        "entity_name": record.get("entity_name", ""),
                        "value": record.get("value", record.get("text", "")),
                        "status": record.get("status", ""),
                        "source_event_hash": record.get("source_event_hash", 0),
                        "source_chunk_hash": record.get("source_chunk_hash", 0),
                        "resource_hash": record.get("resource_hash", 0),
                        "source_locator": record.get("source_locator", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", 0),
                    }
                )
            elif table == "context_packs" and record_type in {"context_pack_audit", "context_pack_telemetry"}:
                dropped_refs = record.get("dropped_refs", {})
                rows.append(
                    {
                        "row_type": record_type,
                        "context_pack_id": record.get("context_pack_id", ""),
                        "query": record.get("query", "") if record_type == "context_pack_audit" else f"hash:{record.get('query_hash', '')}",
                        "used_context_tokens": record.get("used_context_tokens", record.get("used_remote_context_tokens", 0)),
                        "selected_ref_count": len(record.get("selected_refs", [])) if record_type == "context_pack_audit" else record.get("selected_ref_count", 0),
                        "dropped_ref_count": len(dropped_refs.get("refs", [])) if record_type == "context_pack_audit" and isinstance(dropped_refs, dict) else record.get("dropped_ref_count", 0),
                        "quality_warnings": record.get("quality_warnings", []) if record_type == "context_pack_audit" else {"count": record.get("quality_warning_count", 0)},
                        "scope": candidate_access_scope(record),
                        "created_at_ms": record.get("created_at_ms", 0),
                    }
                )
        if table == "resources":
            priority = {"resource_manifest": 0, "resource_chunk": 1, "resource_import_task": 2}
            rows.sort(
                key=lambda row: (
                    priority.get(str(row.get("row_type") or ""), 9),
                    -int(row.get("updated_at_ms") or row.get("created_at_ms") or 0),
                )
            )
        else:
            rows.sort(key=lambda row: int(row.get("updated_at_ms") or row.get("created_at_ms") or 0), reverse=True)
        return rows

    def ingestion_dashboard(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        table = optional_string(args, "table", "messages")
        allowed_tables = {"messages", "resources", "skills", "events", "entities", "context_packs"}
        if table not in allowed_tables:
            raise MatrixArkError(f"table must be one of {sorted(allowed_tables)}")
        page_size = args.get("page_size", 25)
        if not isinstance(page_size, int) or page_size <= 0 or page_size > 200:
            raise MatrixArkError("page_size must be an integer between 1 and 200")
        page_token = args.get("page_token", 0)
        if isinstance(page_token, str) and page_token.isdigit():
            page_token = int(page_token)
        if not isinstance(page_token, int) or page_token < 0:
            raise MatrixArkError("page_token must be a non-negative integer offset")
        records = self.read_all()
        totals = {name: len(self._dashboard_rows_for_table(records, name, scope)) for name in sorted(allowed_tables)}
        rows = self._dashboard_rows_for_table(records, table, scope)
        page = rows[page_token : page_token + page_size]
        next_page_token = page_token + page_size if page_token + page_size < len(rows) else None
        return {
            "status": "ok",
            "scope": scope,
            "table": table,
            "page_size": page_size,
            "page_token": page_token,
            "next_page_token": next_page_token,
            "total": len(rows),
            "totals": totals,
            "rows": page,
            "record_count": len(records),
        }

    def list_resources(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        resource_type_filter = optional_string(args, "resource_type", "")
        resources: dict[int, Json] = {}
        for record in reversed(self.read_all()):
            if record.get("record_type") != "resource_manifest":
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            if resource_type_filter and record.get("resource_type") != resource_type_filter:
                continue
            resource_hash = int(record.get("resource_hash") or 0)
            if resource_hash in resources:
                continue
            resources[resource_hash] = {
                "resource_hash": resource_hash,
                "raw_uri": record.get("raw_uri", ""),
                "requested_raw_uri": record.get("requested_raw_uri", record.get("raw_uri", "")),
                "resource_type": record.get("resource_type", ""),
                "resource_version": record.get("resource_version", ""),
                "content_hash": record.get("content_hash", ""),
                "chunk_count": record.get("chunk_count", 0),
                "original_chunk_count": record.get("original_chunk_count", record.get("chunk_count", 0)),
                "deduped_chunk_count": record.get("deduped_chunk_count", 0),
                "superseded_chunk_count": record.get("superseded_chunk_count", 0),
                "superseded_chunk_hashes": record.get("superseded_chunk_hashes", []),
                "raw_storage_policy": record.get("raw_storage_policy", "raw_uri_only"),
                "raw_storage_mode": record.get("raw_storage_mode", "local"),
                "upload_status": record.get("upload_status", "not_required"),
                "cloud_bucket": record.get("cloud_bucket", ""),
                "cloud_key": record.get("cloud_key", ""),
                "raw_bytes_stored": bool(record.get("raw_bytes_stored", False)),
                "parse_warnings": record.get("parse_warnings", []),
                "parse_warning_count": record.get("parse_warning_count", 0),
                "async_parent_summary_required": bool(record.get("async_parent_summary_required", False)),
                "access_scope": record.get("access_scope", candidate_access_scope(record)),
                "deployment_scope": record.get("deployment_scope", "local"),
                "import_task_hash": record.get("import_task_hash", 0),
                "token_estimate": record.get("token_estimate", 0),
                "node_hash": record.get("node_hash", 0),
                "node_path": record.get("node_path", []),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", 0),
            }
            if len(resources) >= limit:
                break
        return {"status": "ok", "resources": list(resources.values()), "count": len(resources)}

    def list_skills(self, args: Json) -> Json:
        scope = optional_object(args, "scope")
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        include_disabled = bool(args.get("include_disabled", False))
        controls = self.latest_skill_controls()
        skills: dict[int, Json] = {}
        for record in reversed(self.read_all()):
            if record.get("record_type") != "skill_manifest":
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            skill_hash = int(record.get("skill_hash") or 0)
            if skill_hash in skills:
                continue
            control = controls.get(skill_hash, {})
            status = str(control.get("status") or record.get("status") or "active")
            if status == "disabled" and not include_disabled:
                continue
            skills[skill_hash] = {
                "skill_hash": skill_hash,
                "name": record.get("name", ""),
                "description": record.get("description", ""),
                "raw_uri": record.get("raw_uri", ""),
                "requested_raw_uri": record.get("requested_raw_uri", record.get("raw_uri", "")),
                "raw_storage_policy": record.get("raw_storage_policy", "raw_uri_only"),
                "raw_storage_mode": record.get("raw_storage_mode", "local"),
                "upload_status": record.get("upload_status", "not_required"),
                "cloud_bucket": record.get("cloud_bucket", ""),
                "cloud_key": record.get("cloud_key", ""),
                "raw_bytes_stored": bool(record.get("raw_bytes_stored", False)),
                "owner_scope": control.get("owner_scope", record.get("owner_scope", "user")),
                "version": control.get("version", record.get("version", "1")),
                "status": status,
                "precedence": control.get("precedence", record.get("precedence", "normal")),
                "triggers": control.get("triggers", record.get("triggers", [])),
                "allowed_tools": control.get("allowed_tools", record.get("allowed_tools", [])),
                "examples": record.get("examples", record.get("metadata", {}).get("examples", [])),
                "permissions": record.get("permissions", record.get("metadata", {}).get("permissions", [])),
                "inputs": record.get("inputs", record.get("metadata", {}).get("inputs", [])),
                "outputs": record.get("outputs", record.get("metadata", {}).get("outputs", [])),
                "access_scope": record.get("access_scope", candidate_access_scope(record)),
                "deployment_scope": record.get("deployment_scope", "local"),
                "node_hash": record.get("node_hash", 0),
                "node_path": record.get("node_path", []),
                "scope": candidate_access_scope(record),
                "updated_at_ms": control.get("updated_at_ms", record.get("updated_at_ms", 0)),
            }
            if len(skills) >= limit:
                break
        return {"status": "ok", "skills": list(skills.values()), "count": len(skills)}

    def update_skill(self, args: Json) -> Json:
        skill_hash = args.get("skill_hash")
        if not isinstance(skill_hash, int) or skill_hash <= 0:
            raise MatrixArkError("skill_hash must be a positive integer")
        status = optional_string(args, "status", "")
        if status and status not in {"active", "disabled"}:
            raise MatrixArkError("status must be active or disabled")
        precedence = optional_string(args, "precedence", "")
        if precedence and precedence not in {"low", "normal", "high", "critical"}:
            raise MatrixArkError("precedence must be low, normal, high, or critical")
        current = None
        for record in reversed(self.read_all()):
            if record.get("record_type") == "skill_manifest" and record.get("skill_hash") == skill_hash:
                current = record
                break
        if current is None:
            raise MatrixArkError("skill_hash not found")
        update = {
            "record_type": "skill_registry_update",
            "skill_hash": skill_hash,
            "status": status or current.get("status", "active"),
            "precedence": precedence or current.get("precedence", "normal"),
            "owner_scope": optional_string(args, "owner_scope", str(current.get("owner_scope") or "user")),
            "version": optional_string(args, "version", str(current.get("version") or "1")),
            "triggers": optional_string_list(args, "triggers", list(current.get("triggers", []))),
            "allowed_tools": optional_string_list(args, "allowed_tools", list(current.get("allowed_tools", []))),
            "scope": current.get("scope", {}),
            "node_hash": current.get("node_hash", 0),
            "node_path": current.get("node_path", []),
            "updated_at_ms": now_ms(),
        }
        self.append(update)
        return {"status": "updated", **update}

    def _resource_import_pool_status(self) -> Json:
        return {
            "worker_count": self._resource_import_worker_count,
            "queue_max": self._resource_import_queue_max,
            "queue_depth": self._resource_import_queue.qsize(),
            "queue_remaining_capacity": max(0, self._resource_import_queue_max - self._resource_import_queue.qsize()),
            "bounded": True,
        }

    def _ensure_resource_import_workers(self) -> None:
        with self._resource_import_worker_lock:
            if self._resource_import_workers_started:
                return
            self._resource_import_stop.clear()
            for worker_index in range(self._resource_import_worker_count):
                thread = threading.Thread(
                    target=self._resource_import_worker_loop,
                    name=f"matrixark-resource-import-{worker_index}",
                    daemon=True,
                )
                thread.start()
                self._resource_import_threads.append(thread)
            self._resource_import_workers_started = True

    def _resource_import_worker_loop(self) -> None:
        while True:
            item = self._resource_import_queue.get()
            try:
                if item.get("_stop"):
                    return
                args = item.get("args", {})
                hook = item.get("hook")
                self._run_background_resource_import(args, hook if isinstance(hook, dict) else None)
            finally:
                self._resource_import_queue.task_done()

    def close(self, *, timeout_s: float = 5.0) -> None:
        """Drain async import work and stop background workers."""
        deadline = time.monotonic() + max(0.0, timeout_s)
        while getattr(self._resource_import_queue, "unfinished_tasks", 0) and time.monotonic() < deadline:
            time.sleep(0.01)
        self._resource_import_stop.set()
        with self._resource_import_worker_lock:
            if self._resource_import_workers_started:
                for _thread in self._resource_import_threads:
                    remaining = max(0.0, deadline - time.monotonic())
                    try:
                        self._resource_import_queue.put({"_stop": True}, timeout=remaining if remaining > 0 else 0.01)
                    except thread_queue.Full:
                        pass
                for thread in list(self._resource_import_threads):
                    thread.join(timeout=max(0.0, deadline - time.monotonic()))
                self._resource_import_threads = [thread for thread in self._resource_import_threads if thread.is_alive()]
                self._resource_import_workers_started = bool(self._resource_import_threads)

    def _enqueue_resource_import(self, *, args: Json, hook: Json | None, task_hash: int) -> Json:
        self._ensure_resource_import_workers()
        queue_before = self._resource_import_queue.qsize()
        try:
            self._resource_import_queue.put_nowait(
                {
                    "args": args,
                    "hook": hook,
                    "task_hash": task_hash,
                    "queued_at_ms": now_ms(),
                }
            )
        except thread_queue.Full:
            raise MatrixArkError(
                f"resource import queue is full; workers={self._resource_import_worker_count} max_queue={self._resource_import_queue_max}"
            )
        status = self._resource_import_pool_status()
        status["queue_depth_before_enqueue"] = queue_before
        self._observe_model_latency("resource_import_queue_wait", 0.0)
        metrics = getattr(self, "_matrixark_service_metrics", None)
        if metrics is not None:
            metrics.observe_resource_queue_depth(int(status.get("queue_depth") or 0))
        return status

    def _run_background_resource_import(self, args: Json, hook: Json | None) -> None:
        task_hash = args.get("_resource_import_task_hash", 0)
        try:
            self.ingest(args, hook=hook)
        except Exception as exc:  # pragma: no cover - background failure path is validated via records.
            scope = optional_object(args, "scope")
            metadata = optional_object(args, "metadata")
            envelope = normalize_envelope(args, default_kind="resource")
            deployment_scope = deployment_scope_from_args(args, envelope)
            sharing_scope = self.resource_sharing_scope(args, envelope, deployment_scope)
            node_hint = self.default_resource_node_path(args, envelope, deployment_scope=deployment_scope, sharing_scope=sharing_scope)
            node_path = [str(part) for part in node_hint if str(part)]
            try:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": task_hash,
                        "status": "failed",
                        "kind": str(args.get("kind") or "resource"),
                        "raw_uri": str(args.get("raw_uri") or metadata.get("raw_uri") or "inline-resource"),
                        "resource_type": str(args.get("resource_type") or metadata.get("resource_type") or ""),
                        "error": str(exc),
                        "node_hash": stable_hash("/".join(node_path)),
                        "node_path": node_path,
                        "scope": dict(scope),
                        "updated_at_ms": now_ms(),
                    }
                )
            except Exception:
                _mcp_debug_log(f"resource import background failure could not be recorded: {exc}")

    def _resource_import_async_default_reason(self, args: Json, envelope: Json, raw_uri: str) -> str:
        if "wait" in args:
            return ""
        inline_text = "\n\n".join(str(message.get("content", "")) for message in envelope.get("messages", []))
        if len(inline_text) >= RESOURCE_ASYNC_DEFAULT_TEXT_CHARS:
            return f"inline_text_chars>={RESOURCE_ASYNC_DEFAULT_TEXT_CHARS}"
        try:
            path = Path(raw_uri)
            if not path.exists():
                return ""
            if path.is_file():
                size = path.stat().st_size
                if size >= RESOURCE_ASYNC_DEFAULT_BYTES:
                    return f"file_bytes>={RESOURCE_ASYNC_DEFAULT_BYTES}"
            elif path.is_dir():
                file_count = 0
                total_size = 0
                for child in path.rglob("*"):
                    if not child.is_file():
                        continue
                    if any(part in RESOURCE_IMPORT_IGNORE_DIRS for part in child.parts):
                        continue
                    file_count += 1
                    try:
                        total_size += child.stat().st_size
                    except OSError:
                        pass
                    if file_count >= RESOURCE_ASYNC_DEFAULT_PATH_COUNT:
                        return f"path_count>={RESOURCE_ASYNC_DEFAULT_PATH_COUNT}"
                    if total_size >= RESOURCE_ASYNC_DEFAULT_BYTES:
                        return f"directory_bytes>={RESOURCE_ASYNC_DEFAULT_BYTES}"
        except (OSError, ValueError):
            return ""
        return ""

    def ingest(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        backend_readiness: Json | None = None
        if envelope["kind"] in {"resource", "skill"}:
            backend_readiness = self.ensure_backend_ready(reason=f"{envelope['kind']}_ingest")
        idle_commit_result: Json | None = None
        idle_commit_timeout_ms = args.get("idle_commit_timeout_ms")
        if idle_commit_timeout_ms is not None:
            if not isinstance(idle_commit_timeout_ms, int) or idle_commit_timeout_ms < 0:
                raise MatrixArkError("idle_commit_timeout_ms must be a non-negative integer")
            idle_commit_result = self.session_commit(
                {
                    "scope": envelope["scope"],
                    "metadata": envelope["metadata"],
                    "threshold_messages": args.get("session_buffer_threshold", 20),
                    "force": False,
                    "idle_timeout_ms": idle_commit_timeout_ms,
                    "commit_reason": "idle_timeout",
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        lightweight_async_accept = envelope["kind"] in {"message", "business_data", "feedback"} and (
            bool(args.get("async_processing", False)) or args.get("wait") is False
        )
        if lightweight_async_accept:
            text = text_from_messages(envelope["messages"])
            event_id_hash = stable_hash(
                f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
            )
            node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
            node_path = normalized_node_path(envelope, node_hint)
            node_hash = stable_hash("/".join(node_path))
            node_materialization = self.ensure_context_node_path(
                node_path=node_path,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
            )
            with self.write_batch("message_ingest_sync_accept"):
                summary_dirty_hashes = self.mark_node_summary_dirty(
                    node_path=node_path,
                    scope=envelope["scope"],
                    updated_at_ms=envelope["ingestion_time_ms"],
                    source_ref_type="event",
                    source_hash_field="source_event_hash",
                    source_hash=event_id_hash,
                    dirty_reason="new_event",
                )
                self.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": text,
                        "summary_text": summarize_text(text),
                        "envelope": envelope,
                        "internal_extraction": {
                            "mode": "async_pending",
                            "classification": "PENDING_ASYNC_EXTRACTION",
                            "event_type": "pending_async",
                            "status": "pending",
                        },
                        "agent_hook": hook,
                        "storage_options": envelope.get("storage_options", {}),
                        "async_processing": True,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
                self.append(
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": stable_hash(f"async_pipeline:{event_id_hash}"),
                        "event_id_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "status": "pending",
                        "stages": ["extraction", "summary", "compression", "embedding"],
                        "reason": "sync_accept_async_processing",
                        "created_at_ms": envelope["ingestion_time_ms"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            pending_event_count = len(self.pending_session_events(envelope["scope"]))
            return {
                "status": "accepted",
                "sync_write_mode": "lightweight_event",
                "async_processing": True,
                "async_pipeline_status": "pending",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "storage_options": envelope.get("storage_options", {}),
                "storage_route": envelope.get("storage_route", {}),
                "hook_captured": hook is not None,
                "extraction_mode": "async_pending",
                "summary_refresh": {
                    "status": "dirty_marked",
                    "dirty_hashes": summary_dirty_hashes,
                    "async_required": True,
                },
                "node_materialization": node_materialization,
                "session_buffer": {
                    "buffer_key": list(session_buffer_key(envelope)),
                    "pending_event_count": pending_event_count,
                    "threshold_messages": args.get("session_buffer_threshold", 20),
                    "auto_batch_extract": bool(args.get("auto_batch_extract", False)),
                },
                "idle_commit_result": idle_commit_result,
                "quality_warnings": ["async_processing_pending:extraction,summary,compression,embedding"],
            }
        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction_started_perf = time.perf_counter()
        extraction = compact_internal_extraction(
            envelope,
            prior_context=prior_context,
        )
        self._observe_model_latency("extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
        text = text_from_messages(envelope["messages"])
        event_id_hash = stable_hash(
            f"{envelope['kind']}:{text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        if envelope["kind"] in {"resource", "skill"}:
            early_deployment_scope = deployment_scope_from_args(args, envelope)
            early_sharing_scope = self.resource_sharing_scope(args, envelope, early_deployment_scope)
            node_hint = self.default_resource_node_path(args, envelope, deployment_scope=early_deployment_scope, sharing_scope=early_sharing_scope)
        else:
            early_deployment_scope = "local"
            early_sharing_scope = "private_user"
            node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        resource_chunk_hashes: list[int] = []
        resource_dirty_hashes: list[int] = []
        resource_parse_error = ""
        resource_import_task_hash = 0
        resource_import_task_status = "not_applicable"
        resource_import_wait = True
        resource_import_metrics: Json = {}
        resource_fact_event_hashes: list[int] = []
        resource_fact_entity_hashes: list[int] = []
        skill_hash = None
        if envelope["kind"] in {"resource", "skill"}:
            requested_raw_uri = str(envelope.get("raw_uri") or envelope["metadata"].get("raw_uri") or "inline-resource")
            resource_type = str(envelope.get("resource_type") or envelope["metadata"].get("resource_type") or "")
            async_default_reason = self._resource_import_async_default_reason(args, envelope, requested_raw_uri)
            resource_import_wait = bool(args.get("wait", not bool(async_default_reason)))
            resource_import_background = bool(args.get("_background_resource_import", False))
            deployment_scope = early_deployment_scope
            sharing_scope = early_sharing_scope
            access_scope = registry_access_scope(envelope["scope"], sharing_scope=sharing_scope)
            resource_record_scope = access_scope if sharing_scope in {"tenant_shared", "global_shared"} else envelope["scope"]
            provided_task_hash = args.get("_resource_import_task_hash")
            resource_import_task_hash = (
                int(provided_task_hash)
                if isinstance(provided_task_hash, int) and provided_task_hash > 0
                else stable_hash(f"resource_import_task:{envelope['kind']}:{requested_raw_uri}:{node_hash}:{envelope['ingestion_time_ms']}")
            )
            import_started_perf = time.perf_counter()
            raw_uri = requested_raw_uri
            raw_storage_policy = "raw_uri_only"
            storage_resolution: Json = {
                "storage_mode": resource_storage_mode_from_args(args, envelope, deployment_scope),
                "original_raw_uri": requested_raw_uri,
                "stored_raw_uri": requested_raw_uri,
                "parse_uri": requested_raw_uri,
                "parse_text": None,
                "raw_storage_policy": raw_storage_policy,
                "raw_bytes_stored": False,
                "upload_status": "not_started",
                "temp_paths": [],
            }
            if not resource_import_background:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "kind": envelope["kind"],
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "wait": resource_import_wait,
                        "async_default_reason": async_default_reason,
                        "progress": {"stage": "queued", "percent": 0},
                        "created_at_ms": envelope["ingestion_time_ms"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if not resource_import_wait:
                background_args = {
                    **args,
                    "wait": True,
                    "_background_resource_import": True,
                    "_resource_import_task_hash": resource_import_task_hash,
                }
                try:
                    queue_status = self._enqueue_resource_import(
                        args=background_args,
                        hook=hook,
                        task_hash=resource_import_task_hash,
                    )
                except MatrixArkError as exc:
                    self.append(
                        {
                            "record_type": "resource_import_task",
                            "task_hash": resource_import_task_hash,
                            "status": "failed",
                            "kind": envelope["kind"],
                            "raw_uri": requested_raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "resource_type": resource_type,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "raw_bytes_stored": False,
                        "error": str(exc),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                    raise
                return {
                    "status": "queued",
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "resource_import_task": {
                        "task_hash": resource_import_task_hash,
                        "status": "queued",
                        "wait": False,
                        "background_started": True,
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "worker_pool": queue_status,
                        "progress": {"stage": "queued", "percent": 0},
                        "async_default_reason": async_default_reason,
                    },
                    "node_materialization": node_materialization,
                }
            resource_import_task_status = "running"
            resource_text = "\n\n".join(str(message["content"]) for message in envelope["messages"])
            try:
                storage_resolution = resolve_raw_resource_for_ingest(
                    args,
                    envelope,
                    requested_raw_uri,
                    resource_type,
                    deployment_scope,
                    resource_text,
                )
            except MatrixArkError as exc:
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": requested_raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": storage_resolution["raw_storage_policy"],
                        "raw_bytes_stored": False,
                        "error": str(exc),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                raise
            raw_uri = str(storage_resolution["stored_raw_uri"])
            parse_uri = str(storage_resolution.get("parse_uri") or raw_uri)
            parse_text = storage_resolution.get("parse_text")
            raw_storage_policy = str(storage_resolution.get("raw_storage_policy") or "raw_uri_only")
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "running",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "storage_options": envelope.get("storage_options", {}),
                    "progress": {"stage": "running", "percent": 10},
                    "updated_at_ms": now_ms(),
                }
            )
            try:
                if envelope["kind"] == "skill" or (resource_type or "").lower() == "skill":
                    parsed_skill = parse_skill(
                        parse_uri,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                    )
                    parsed_skill_chunks = rewrite_chunk_uris(parsed_skill.chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
                    skill_hash = stable_hash(f"skill:{raw_uri}:{parsed_skill.name}:{parsed_skill.metadata.get('version', '1')}")
                    skill_serving_metadata = serving_resource_metadata(parsed_skill.metadata)
                    self.append(
                        {
                            "record_type": "skill_manifest",
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "raw_uri": raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "upload_status": storage_resolution.get("upload_status", "not_required"),
                            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                            "cloud_key": storage_resolution.get("cloud_key", ""),
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "text_preview": clip_context_text(parsed_skill.text),
                            "token_estimate": parsed_skill.token_estimate,
                            "metadata": skill_serving_metadata,
                            "scope": resource_record_scope,
                            "storage_options": envelope.get("storage_options", {}),
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_debug_metadata = debug_resource_metadata(parsed_skill.metadata)
                    if skill_debug_metadata or parsed_skill.text:
                        self.append(
                            {
                                "record_type": "context_debug_record",
                                "debug_type": "skill_parse_detail",
                                "ref_type": "skill",
                                "ref_hash": skill_hash,
                                "skill_hash": skill_hash,
                                "import_task_hash": resource_import_task_hash,
                                "node_hash": node_hash,
                                "node_path": node_path,
                                "raw_uri": raw_uri,
                                "metadata_debug": skill_debug_metadata,
                                "text_preview": clip_context_text(parsed_skill.text),
                                "scope": resource_record_scope,
                                "updated_at_ms": envelope["ingestion_time_ms"],
                            }
                        )
                    self.append(
                        {
                            "record_type": "skill_registry",
                            "registry_hash": stable_hash(f"skill_registry:{skill_hash}:{deployment_scope}"),
                            "skill_hash": skill_hash,
                            "import_task_hash": resource_import_task_hash,
                            "raw_uri": raw_uri,
                            "requested_raw_uri": requested_raw_uri,
                            "raw_storage_mode": storage_resolution["storage_mode"],
                            "raw_storage_policy": raw_storage_policy,
                            "upload_status": storage_resolution.get("upload_status", "not_required"),
                            "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                            "cloud_key": storage_resolution.get("cloud_key", ""),
                            "name": parsed_skill.name,
                            "description": parsed_skill.description,
                            "owner_scope": parsed_skill.metadata.get("owner_scope", "user"),
                            "version": parsed_skill.metadata.get("version", "1"),
                            "status": parsed_skill.metadata.get("status", "active"),
                            "precedence": parsed_skill.metadata.get("precedence", "normal"),
                            "triggers": parsed_skill.metadata.get("triggers", []),
                            "allowed_tools": parsed_skill.metadata.get("allowed_tools", []),
                            "examples": parsed_skill.metadata.get("examples", []),
                            "permissions": parsed_skill.metadata.get("permissions", []),
                            "inputs": parsed_skill.metadata.get("inputs", []),
                            "outputs": parsed_skill.metadata.get("outputs", []),
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    skill_vector = embedding_for_text(str(parsed_skill.metadata.get("embedding_text") or (parsed_skill.name + " " + parsed_skill.description)))
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_summary",
                            "ref_type": "skill",
                            "ref_hash": skill_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(skill_vector),
                            "model": embedding_model_name(),
                            "vector": skill_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    parsed_chunks = parsed_skill_chunks
                else:
                    parsed_chunks = parse_resource(
                        parse_uri,
                        resource_type=resource_type or None,
                        text=parse_text,
                        chunk_hash_base=args.get("chunk_hash_base") if isinstance(args.get("chunk_hash_base"), int) else None,
                        resource_version=args.get("resource_version") if isinstance(args.get("resource_version"), str) else None,
                        supersedes_chunk_hashes=args.get("supersedes_chunk_hashes") if isinstance(args.get("supersedes_chunk_hashes"), dict) else None,
                    )
                    parsed_chunks = rewrite_chunk_uris(parsed_chunks, parse_uri=parse_uri, stored_raw_uri=raw_uri)
            except ResourceParserError as exc:
                resource_parse_error = str(exc)
                parsed_chunks = []
            finally:
                cleanup_temp_paths([str(path) for path in storage_resolution.get("temp_paths", []) if isinstance(path, str)])
            if not parsed_chunks:
                resource_import_task_status = "failed"
                self.append(
                    {
                        "record_type": "resource_import_task",
                        "task_hash": resource_import_task_hash,
                        "status": "failed",
                        "kind": envelope["kind"],
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "error": resource_parse_error or "resource ingestion produced no chunks",
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "progress": {"stage": "failed", "percent": 100},
                        "updated_at_ms": now_ms(),
                    }
                )
                raise MatrixArkError(resource_parse_error or "resource ingestion produced no chunks")
            original_chunk_count = len(parsed_chunks)
            deduped_source_refs: list[str] = []
            seen_content_hashes: set[str] = set()
            unique_chunks = []
            for chunk in parsed_chunks:
                chunk_content_hash = str(chunk.metadata.get("content_hash") or content_hash(chunk.text))
                if chunk_content_hash in seen_content_hashes:
                    deduped_source_refs.append(chunk.source_ref)
                    continue
                seen_content_hashes.add(chunk_content_hash)
                unique_chunks.append(chunk)
            parsed_chunks = unique_chunks
            deduped_chunk_count = original_chunk_count - len(parsed_chunks)
            if not parsed_chunks:
                raise MatrixArkError("resource ingestion produced only duplicate chunks")
            resource_version_value = str(parsed_chunks[0].metadata.get("resource_version") or "")
            resource_content_hash = content_hash("\n".join(str(chunk.metadata.get("content_hash") or content_hash(chunk.text)) for chunk in parsed_chunks))
            superseded_chunk_count = sum(1 for chunk in parsed_chunks if chunk.metadata.get("supersedes_chunk_hash"))
            superseded_chunk_hashes = [
                int(chunk.metadata["supersedes_chunk_hash"])
                for chunk in parsed_chunks
                if isinstance(chunk.metadata.get("supersedes_chunk_hash"), int)
            ]
            parse_warnings = aggregate_parse_warnings_from_chunks(parsed_chunks)
            chunk_vectors = embeddings_for_texts([embedding_text_for_chunk(chunk) for chunk in parsed_chunks])
            index_write_count = 0
            index_candidate_count = 0
            index_dropped_by_cap_count = 0
            secondary_index_budget = new_secondary_index_budget()
            resource_kind = "skill" if skill_hash is not None else "resource"
            resource_l0_text = summarize_text(
                summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
                limit=700,
            )
            resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
            resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
            self.append(
                {
                    "record_type": "context_summary",
                    "summary_type": f"{resource_kind}_l0",
                    "summary_hash": resource_summary_hash,
                    "import_task_hash": resource_import_task_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "raw_uri": raw_uri,
                    "summary_text": resource_l0_text,
                    "source_chunk_hashes": [chunk.chunk_hash for chunk in parsed_chunks],
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": f"{resource_kind}_l0",
                    "ref_type": "summary",
                    "ref_hash": resource_summary_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(resource_summary_vector),
                    "model": embedding_model_name(),
                    "vector": resource_summary_vector,
                    "scope": resource_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            resource_dirty_hashes = self.mark_node_summary_dirty(
                node_path=node_path,
                scope=envelope["scope"],
                updated_at_ms=envelope["ingestion_time_ms"],
                source_ref_type=f"{resource_kind}_summary",
                source_hash_field="source_summary_hash",
                source_hash=resource_summary_hash,
                dirty_reason=f"{resource_kind}_update",
            )
            raw_resource_indexes = ordered_unique(
                [
                    context_index_name("source_type", envelope["kind"]),
                    context_index_name("resource_type", resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
                ]
                + (
                    [
                        context_index_name("skill_name", parsed_skill.name),
                    ]
                    + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                    + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                    if skill_hash is not None
                    else []
                )
            )
            index_candidate_count += len(raw_resource_indexes)
            resource_indexes = take_secondary_index_terms(raw_resource_indexes, secondary_index_budget)
            for index_name in resource_indexes:
                index_write_count += 1
                self.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "index_hash": stable_hash(f"{index_name}:{resource_summary_hash}"),
                        "summary_hash": resource_summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "storage_options": envelope.get("storage_options", {}),
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            resource_manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
            raw_uri_hash = stable_hash(raw_uri)
            if envelope["kind"] == "resource":
                manifest_hash = resource_manifest_hash
                self.append(
                    {
                        "record_type": "resource_manifest",
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "raw_bytes_stored": False,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "parse_warnings": parse_warnings[:100],
                        "parse_warning_count": len(parse_warnings),
                        "chunk_count": len(parsed_chunks),
                        "original_chunk_count": original_chunk_count,
                        "deduped_chunk_count": deduped_chunk_count,
                        "deduped_source_refs": deduped_source_refs[:50],
                        "superseded_chunk_count": superseded_chunk_count,
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "summary_dirty_hashes": resource_dirty_hashes,
                        "async_parent_summary_required": bool(resource_dirty_hashes),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "token_estimate": sum(chunk.token_estimate for chunk in parsed_chunks),
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "resource_registry",
                        "registry_hash": stable_hash(f"resource_registry:{raw_uri}:{node_hash}:{resource_version_value}:{deployment_scope}"),
                        "resource_hash": manifest_hash,
                        "import_task_hash": resource_import_task_hash,
                        "raw_uri": raw_uri,
                        "requested_raw_uri": requested_raw_uri,
                        "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                        "resource_version": resource_version_value,
                        "content_hash": resource_content_hash,
                        "chunk_count": len(parsed_chunks),
                        "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                        "raw_storage_mode": storage_resolution["storage_mode"],
                        "raw_storage_policy": raw_storage_policy,
                        "upload_status": storage_resolution.get("upload_status", "not_required"),
                        "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                        "cloud_key": storage_resolution.get("cloud_key", ""),
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            for chunk, vector in zip(parsed_chunks, chunk_vectors):
                resource_chunk_hashes.append(chunk.chunk_hash)
                source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
                chunk_metadata_source = {**chunk.metadata, "source_locator": source_locator}
                chunk_metadata = serving_resource_metadata(chunk_metadata_source)
                chunk_debug_metadata = debug_resource_metadata(chunk.metadata)
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "skill_section",
                            "import_task_hash": resource_import_task_hash,
                            "skill_hash": skill_hash,
                            "section_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "resource_hash": skill_hash,
                            "raw_uri_hash": raw_uri_hash,
                            "source_locator": source_locator,
                            "heading": chunk_metadata.get("heading", ""),
                            "text": chunk.text,
                            "token_estimate": chunk.token_estimate,
                            "metadata": chunk_metadata,
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "resource_chunk",
                        "import_task_hash": resource_import_task_hash,
                        "chunk_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                        "raw_uri_hash": raw_uri_hash,
                        "resource_type": chunk_metadata.get("resource_type") or resource_type,
                        "source_locator": source_locator,
                        "text": chunk.text,
                        "token_estimate": chunk.token_estimate,
                        "metadata": chunk_metadata,
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if chunk_debug_metadata:
                    self.append(
                        {
                            "record_type": "context_debug_record",
                            "debug_type": "resource_chunk_parse_detail",
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "import_task_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                            "raw_uri_hash": raw_uri_hash,
                            "raw_uri": raw_uri,
                            "source_locator": source_locator,
                            "source_ref": chunk.source_ref,
                            "metadata_debug": chunk_debug_metadata,
                            "text_preview": clip_context_text(chunk.text),
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "resource_chunk",
                        "ref_type": "resource_chunk",
                        "ref_hash": chunk.chunk_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(vector),
                        "model": embedding_model_name(),
                        "vector": vector,
                        "scope": resource_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                if skill_hash is not None:
                    self.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "skill_section",
                            "ref_type": "skill_section",
                            "ref_hash": chunk.chunk_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(vector),
                            "model": embedding_model_name(),
                            "vector": vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                raw_chunk_index_terms = (
                    [
                        context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                        context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                    ]
                    + metadata_index_terms(chunk.metadata)
                    + (
                        [context_index_name("skill_name", parsed_skill.name)]
                        + [context_index_name("skill_trigger", trigger) for trigger in parsed_skill.metadata.get("triggers", [])]
                        + [context_index_name("skill_tool", tool) for tool in parsed_skill.metadata.get("allowed_tools", [])]
                        if skill_hash is not None and parsed_skill is not None
                        else []
                    )
                )
                index_candidate_count += len([term for term in raw_chunk_index_terms if term])
                chunk_index_terms = limited_index_terms(
                    raw_chunk_index_terms,
                    limit=MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                )
                index_dropped_by_cap_count += max(0, len(ordered_unique([term for term in raw_chunk_index_terms if term])) - len(chunk_index_terms))
                chunk_index_terms = take_secondary_index_terms(chunk_index_terms, secondary_index_budget)
                for index_name in chunk_index_terms:
                    index_write_count += 1
                    self.append(
                        {
                            "record_type": "context_index",
                            "index_name": index_name,
                            "index_hash": stable_hash(f"{index_name}:{chunk.chunk_hash}"),
                            "ref_type": "skill_section" if skill_hash is not None else "resource_chunk",
                            "ref_hash": chunk.chunk_hash,
                            "chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash if skill_hash is None else skill_hash,
                            "source_locator": source_locator,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
            resource_fact_records: list[Json] = []
            fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:MAX_RESOURCE_FACT_CHUNKS]
            remaining_resource_fact_budget = max(0, MAX_RESOURCE_FACTS_PER_RESOURCE)
            for chunk in fact_chunks:
                if remaining_resource_fact_budget <= 0:
                    break
                source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
                chunk_metadata = serving_resource_metadata({**chunk.metadata, "source_locator": source_locator})
                for fact_extraction in extract_resource_facts(
                    chunk,
                    chunk_metadata=chunk_metadata,
                    envelope=envelope,
                    raw_uri=raw_uri,
                    resource_version=resource_version_value,
                )[:remaining_resource_fact_budget]:
                    remaining_resource_fact_budget -= 1
                    fact_event_type = str(fact_extraction["event_type"])
                    fact_entity_type = str(fact_extraction["entity_type"])
                    fact_value = str(fact_extraction.get("value", ""))
                    fact_event_hash = stable_hash(f"resource_fact:{chunk.chunk_hash}:{fact_event_type}:{resource_version_value}")
                    resource_fact_event_hashes.append(fact_event_hash)
                    fact_summary = summarize_text(f"{fact_event_type}: {fact_value}", limit=320)
                    resource_fact_records.append(
                        {
                            "record_type": "context_event",
                            "event_id_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "text": chunk.text,
                            "summary_text": fact_summary,
                            "envelope": {**envelope, "kind": "resource_fact"},
                            "internal_extraction": fact_extraction,
                            "source_chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash,
                            "source_locator": source_locator,
                            "resource_version": resource_version_value,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    fact_vector = embedding_for_text(fact_event_type + " " + fact_value + " " + chunk.text)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "event_text",
                            "ref_type": "event",
                            "ref_hash": fact_event_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(fact_vector),
                            "model": embedding_model_name(),
                            "vector": fact_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_name = str(fact_extraction.get("entity_name") or fact_entity_type)
                    entity_hash = stable_hash(f"{node_hash}:{fact_entity_type}:{entity_name}:{chunk.chunk_hash}")
                    resource_fact_entity_hashes.append(entity_hash)
                    entity_state = summarize_text(f"{fact_event_type}: {fact_value}. Source: {chunk.text}", limit=360)
                    resource_fact_records.append(
                        {
                            "record_type": "context_entity",
                            "entity_hash": entity_hash,
                            "batch_id_hash": resource_import_task_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": resource_record_scope,
                            "entity_type": fact_entity_type,
                            "entity_name": entity_name,
                            "state": entity_state,
                            "confidence": fact_extraction.get("confidence", 0.78),
                            "operator": "LATEST",
                            "source_event_ids": [fact_event_hash],
                            "source_chunk_hash": chunk.chunk_hash,
                            "resource_hash": resource_manifest_hash,
                            "source_locator": source_locator,
                            "resource_version": resource_version_value,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    entity_vector = embedding_for_text(fact_entity_type + " " + entity_name + " " + entity_state)
                    resource_fact_records.append(
                        {
                            "record_type": "context_embedding",
                            "embedding_type": "entity_state",
                            "ref_type": "entity",
                            "ref_hash": entity_hash,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "dim": len(entity_vector),
                            "model": embedding_model_name(),
                            "vector": entity_vector,
                            "scope": resource_record_scope,
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
                    raw_fact_index_terms = [
                        context_index_name("source_type", "resource_fact"),
                        context_index_name("event_type", fact_event_type),
                        context_index_name("entity_type", fact_entity_type),
                        context_index_name("entity_type", "resource_fact"),
                        context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
                    ] + metadata_index_terms(chunk.metadata)
                    index_candidate_count += len([term for term in raw_fact_index_terms if term])
                    fact_index_terms = limited_index_terms(raw_fact_index_terms, limit=MAX_INDEX_TERMS_PER_RESOURCE_FACT)
                    index_dropped_by_cap_count += max(0, len(ordered_unique([term for term in raw_fact_index_terms if term])) - len(fact_index_terms))
                    fact_index_terms = take_secondary_index_terms(fact_index_terms, secondary_index_budget)
                    for index_name in fact_index_terms:
                        index_write_count += 1
                        resource_fact_records.append(
                            {
                                "record_type": "context_index",
                                "index_name": index_name,
                                "index_hash": stable_hash(f"{index_name}:{fact_event_hash}"),
                                "batch_id_hash": resource_import_task_hash,
                                "ref_type": "resource_fact",
                                "ref_hash": fact_event_hash,
                                "chunk_hash": chunk.chunk_hash,
                                "node_hash": node_hash,
                                "node_path": node_path,
                                "scope": resource_record_scope,
                                "updated_at_ms": envelope["ingestion_time_ms"],
                            }
                        )
            if resource_fact_records:
                self.append_many(resource_fact_records)
            resource_import_metrics = {
                "duration_ms": round((time.perf_counter() - import_started_perf) * 1000.0, 3),
                "parser_chunk_count": original_chunk_count,
                "chunk_count": len(parsed_chunks),
                "dedupe_count": deduped_chunk_count,
                "embedding_count": len(chunk_vectors) + 1 + len(resource_fact_event_hashes) + len(resource_fact_entity_hashes),
                "resource_fact_count": len(resource_fact_event_hashes),
                "resource_entity_count": len(resource_fact_entity_hashes),
                "index_candidate_count": index_candidate_count,
                "index_write_count": index_write_count,
                "index_dropped_by_cap_count": index_dropped_by_cap_count,
                **secondary_index_budget_summary(secondary_index_budget),
                "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                "parse_warning_count": len(parse_warnings),
                "parse_warnings": parse_warnings[:100],
                "raw_storage_mode": storage_resolution["storage_mode"],
                "raw_storage_policy": raw_storage_policy,
                "raw_bytes_stored": False,
                "upload_status": storage_resolution.get("upload_status", "not_required"),
                "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                "cloud_key": storage_resolution.get("cloud_key", ""),
                "summary_dirty_count": len(resource_dirty_hashes),
            }
            resource_import_task_status = "completed"
            self.append(
                {
                    "record_type": "resource_import_task",
                    "task_hash": resource_import_task_hash,
                    "status": "completed",
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "requested_raw_uri": requested_raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "resource_version": resource_version_value,
                    "content_hash": resource_content_hash,
                    "raw_storage_mode": storage_resolution["storage_mode"],
                    "raw_storage_policy": raw_storage_policy,
                    "raw_bytes_stored": False,
                    "upload_status": storage_resolution.get("upload_status", "not_required"),
                    "cloud_bucket": storage_resolution.get("cloud_bucket", ""),
                    "cloud_key": storage_resolution.get("cloud_key", ""),
                    "parse_warnings": parse_warnings[:100],
                    "parse_warning_count": len(parse_warnings),
                    "chunk_count": len(parsed_chunks),
                    "original_chunk_count": original_chunk_count,
                    "deduped_chunk_count": deduped_chunk_count,
                    "superseded_chunk_count": superseded_chunk_count,
                    "superseded_chunk_hashes": superseded_chunk_hashes[:200],
                    "resource_fact_count": len(resource_fact_event_hashes),
                    "resource_entity_count": len(resource_fact_entity_hashes),
                    "index_candidate_count": index_candidate_count,
                    "index_write_count": index_write_count,
                    "index_dropped_by_cap_count": index_dropped_by_cap_count,
                    **secondary_index_budget_summary(secondary_index_budget),
                    "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                    "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "progress": {"stage": "completed", "percent": 100},
                    "metrics": resource_import_metrics,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": resource_record_scope,
                    "updated_at_ms": now_ms(),
                }
            )
            self.append(
                {
                    "record_type": "matrixark_metric",
                    "metric_name": "resource_import",
                    "task_hash": resource_import_task_hash,
                    "kind": envelope["kind"],
                    "raw_uri": raw_uri,
                    "resource_type": resource_type or parsed_chunks[0].metadata.get("resource_type", "txt"),
                    "metrics": resource_import_metrics,
                    "progress": {"stage": "completed", "percent": 100},
                    "scope": resource_record_scope,
                    "created_at_ms": now_ms(),
                }
            )
        hot_record_scope = resource_record_scope if envelope["kind"] in {"resource", "skill"} else envelope["scope"]
        summary_text = summarize_text(text)
        embedding_started_perf = time.perf_counter()
        event_embedding = embedding_for_text(text)
        self._observe_model_latency("embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        with self.write_batch("message_ingest_hot_path"):
            session_key_parts = [str(part) for part in context_node_key(envelope)]
            if any(session_key_parts):
                session_summary_source = " ".join(
                    [item.get("text", "") for item in prior_context.get("summaries", [])[:2]]
                    + [item.get("text", "") for item in prior_context.get("messages", [])[:2]]
                    + [text]
                )
                session_summary_text = summarize_text(session_summary_source, limit=512)
                session_summary_hash = stable_hash("session:" + "/".join(session_key_parts))
                self.append(
                    {
                        "record_type": "context_summary",
                        "summary_type": "session_l0",
                        "summary_hash": session_summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "context_node_key": session_key_parts,
                        "summary_text": session_summary_text,
                        "source_event_hash": event_id_hash,
                        "scope": hot_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                self.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "session_l0",
                        "ref_type": "summary",
                        "ref_hash": session_summary_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(embedding_for_text(session_summary_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(session_summary_text),
                        "scope": hot_record_scope,
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            self.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(event_embedding),
                    "model": embedding_model_name(),
                    "vector": event_embedding,
                    "scope": hot_record_scope,
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            record = {
                "record_type": "context_event",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "text": text,
                "envelope": envelope,
                "internal_extraction": extraction,
                "prior_context": prior_context,
                "agent_hook": hook,
                "storage_options": envelope.get("storage_options", {}),
            }
            self.append(record)
            event_index_terms = ordered_unique(
                extraction.get("indexes")
                or [
                    context_index_name("event_type", extraction.get("event_type") or infer_event_type(text)),
                    context_index_name("classification", non_default_classification(extraction.get("classification"))),
                    context_index_name("status", extraction.get("status") or "observed"),
                    context_index_name("source_type", envelope["kind"]),
                ]
            )
            event_index_records: list[Json] = []
            for index_name in event_index_terms:
                event_index_records.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "data_model": "context_event",
                        "ref_type": "event",
                        "ref_hashes": [event_id_hash],
                        "node_hash": node_hash,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if event_index_records:
                self.append_many(event_index_records)
            self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
            summary_refresh = self.append_node_summary_embeddings(
                node_path=node_path,
                source_text=text,
                scope=hot_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
                source_hash_field="source_event_hash",
                source_hash=event_id_hash,
            )
        pending_event_count = len(self.pending_session_events(envelope["scope"]))
        auto_batch_result: Json | None = None
        auto_batch_extract = bool(args.get("auto_batch_extract", False))
        session_buffer_threshold = args.get("session_buffer_threshold", 20)
        if not isinstance(session_buffer_threshold, int) or session_buffer_threshold <= 0:
            raise MatrixArkError("session_buffer_threshold must be a positive integer")
        if auto_batch_extract and pending_event_count >= session_buffer_threshold:
            auto_batch_result = self.session_commit(
                {
                    "scope": hot_record_scope,
                    "metadata": envelope["metadata"],
                    "threshold_messages": session_buffer_threshold,
                    "force": False,
                    "max_messages": session_buffer_threshold,
                    "commit_reason": "threshold",
                    "understanding_provider": args.get("understanding_provider"),
                    "extraction_provider": args.get("extraction_provider"),
                    "segment_provider": args.get("segment_provider"),
                    "segment_model": args.get("segment_model"),
                    "segment_model_path": args.get("segment_model_path"),
                    "segment_max_new_tokens": args.get("segment_max_new_tokens"),
                    "segment_provider_fallback": args.get("segment_provider_fallback"),
                    "skip_prior_context": bool(args.get("skip_prior_context", False)),
                    "storage_options": envelope.get("storage_options", {}),
                },
                hook=hook,
            )
        return {
            "status": "accepted",
            "event_id_hash": event_id_hash,
            "node_hash": record["node_hash"],
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "hook_captured": hook is not None,
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "extraction_mode": extraction["mode"],
            "classification": extraction.get("classification", "UNCLASSIFIED"),
            "prior_context": extraction.get("prior_context", ""),
            "prior_refs": extraction.get("prior_refs", []),
            "prior_message_count": extraction.get("prior_message_count", 0),
            "prior_summary_count": extraction.get("prior_summary_count", 0),
            "quality_warning": extraction.get("quality_warning", ""),
            "summary_refresh": summary_refresh,
            "resource_summary_refresh": {
                "status": "dirty_marked" if resource_dirty_hashes else "not_applicable",
                "dirty_hashes": resource_dirty_hashes,
                "refresh_result": None,
                "async_required": bool(resource_dirty_hashes),
            },
            "resource_import_task": {
                "task_hash": resource_import_task_hash,
                "status": resource_import_task_status,
                "wait": resource_import_wait,
                "metrics": resource_import_metrics,
                "raw_uri": raw_uri if resource_import_task_hash else "",
                "requested_raw_uri": requested_raw_uri if resource_import_task_hash else "",
                "raw_storage_mode": storage_resolution.get("storage_mode", "") if resource_import_task_hash else "",
                "raw_storage_policy": raw_storage_policy if resource_import_task_hash else "",
                "raw_bytes_stored": False if resource_import_task_hash else None,
                "upload_status": storage_resolution.get("upload_status", "") if resource_import_task_hash else "",
                "cloud_bucket": storage_resolution.get("cloud_bucket", "") if resource_import_task_hash else "",
                "cloud_key": storage_resolution.get("cloud_key", "") if resource_import_task_hash else "",
                "progress": {"stage": resource_import_task_status, "percent": 100 if resource_import_task_status == "completed" else 0},
            },
            "node_materialization": node_materialization,
            "resource_chunks": resource_chunk_hashes,
            "resource_chunk_count": len(resource_chunk_hashes),
            "resource_original_chunk_count": original_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_chunk_count": deduped_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_deduped_source_refs": deduped_source_refs[:20] if envelope["kind"] in {"resource", "skill"} else [],
            "resource_version": resource_version_value if envelope["kind"] in {"resource", "skill"} else "",
            "resource_content_hash": resource_content_hash if envelope["kind"] in {"resource", "skill"} else "",
            "resource_parse_warnings": parse_warnings if envelope["kind"] in {"resource", "skill"} else [],
            "resource_parse_warning_count": len(parse_warnings) if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_raw_uri": raw_uri if envelope["kind"] in {"resource", "skill"} else "",
            "resource_requested_raw_uri": requested_raw_uri if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_storage_mode": storage_resolution.get("storage_mode", "") if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_storage_policy": raw_storage_policy if envelope["kind"] in {"resource", "skill"} else "",
            "resource_raw_bytes_stored": False if envelope["kind"] in {"resource", "skill"} else None,
            "backend_readiness": backend_readiness or {},
            "resource_superseded_chunk_count": superseded_chunk_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_superseded_chunk_hashes": superseded_chunk_hashes if envelope["kind"] in {"resource", "skill"} else [],
            "resource_fact_events": resource_fact_event_hashes,
            "resource_fact_event_count": len(resource_fact_event_hashes),
            "resource_fact_entities": resource_fact_entity_hashes,
            "resource_fact_entity_count": len(resource_fact_entity_hashes),
            "resource_index_candidate_count": index_candidate_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_write_count": index_write_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_dropped_by_cap_count": index_dropped_by_cap_count if envelope["kind"] in {"resource", "skill"} else 0,
            "resource_index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
            "resource_index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
            "skill_hash": skill_hash,
            "session_buffer": {
                "buffer_key": list(session_buffer_key(envelope)),
                "pending_event_count": pending_event_count,
                "threshold_messages": session_buffer_threshold,
                "auto_batch_extract": auto_batch_extract,
            },
            "idle_commit_result": idle_commit_result,
            "auto_batch_extract_result": auto_batch_result,
        }

    def batch_extract(self, args: Json, *, hook: Json | None = None) -> Json:
        envelope = normalize_envelope(args, default_kind="message")
        hook = validate_hook(hook)
        threshold = args.get("threshold_messages", 20)
        force = bool(args.get("force", False))
        derive_from_existing_events = bool(args.get("derive_from_existing_events", False))
        source_event_ids = [int(ref) for ref in args.get("source_event_ids", [])] if isinstance(args.get("source_event_ids", []), list) else []
        if not isinstance(threshold, int) or threshold <= 0:
            raise MatrixArkError("threshold_messages must be a positive integer")
        if len(envelope["messages"]) < threshold and not force:
            return {
                "status": "deferred",
                "message_count": len(envelope["messages"]),
                "threshold_messages": threshold,
                "reason": "logical batch below extraction threshold",
            }

        prior_records = [] if args.get("skip_prior_context") else self.read_all()
        prior_context = (
            {"level": "", "refs": [], "messages": [], "summaries": [], "char_count": 0, "limit": MAX_PRIOR_MESSAGES}
            if args.get("skip_prior_context")
            else collect_prior_context(envelope, prior_records)
        )
        extraction_started_perf = time.perf_counter()
        extraction = one_pass_memory_extraction(envelope, prior_context=prior_context)
        self._observe_model_latency("batch_extraction", (time.perf_counter() - extraction_started_perf) * 1000.0)
        batch_text = text_from_messages(envelope["messages"])
        batch_id_hash = stable_hash(
            f"batch:{batch_text}:{envelope['scope']}:{envelope['ingestion_time_ms']}"
        )
        node_hint = envelope["metadata"].get("node_path") or self.default_session_node_path(envelope["scope"])
        node_path = normalized_node_path(envelope, node_hint)
        node_hash = stable_hash("/".join(node_path))
        node_materialization = self.ensure_context_node_path(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )
        batch_summary = extraction["batch_summary"]

        event_hashes: list[int] = list(source_event_ids) if derive_from_existing_events else []
        records_to_append: list[Json] = []
        event_rows: list[tuple[int, Json, str, int]] = []
        segment_hash_by_position: dict[int, int] = {}
        segment_hashes_by_position: dict[int, list[int]] = {}
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            for message_index in segment.get("message_indexes", []):
                if not isinstance(message_index, int):
                    continue
                segment_hashes_by_position.setdefault(message_index, []).append(segment_hash)
                segment_hash_by_position.setdefault(message_index, segment_hash)
        if not derive_from_existing_events:
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                event_hashes.append(event_id_hash)
                event_rows.append((index, message, event_text, event_id_hash))
            event_vectors = embeddings_for_texts([event_text for _index, _message, event_text, _event_id_hash in event_rows])
            for (_index, message, event_text, event_id_hash), event_vector in zip(event_rows, event_vectors):
                records_to_append.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": event_id_hash,
                        "batch_id_hash": batch_id_hash,
                        "parent_segment_hash": segment_hash_by_position.get(_index),
                        "parent_segment_hashes": segment_hashes_by_position.get(_index, []),
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "text": event_text,
                        "summary_text": summarize_text(event_text),
                        "envelope": {
                            **envelope,
                            "messages": [message],
                        },
                        "internal_extraction": {
                            "mode": extraction["mode"],
                            "classification": extraction["classification"],
                            "event_type": extraction["event_type"],
                            "batch_id_hash": batch_id_hash,
                        },
                        "prior_context": prior_context,
                        "agent_hook": hook,
                        "storage_options": envelope.get("storage_options", {}),
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
                records_to_append.append(
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "event_text",
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "dim": len(event_vector),
                        "model": embedding_model_name(),
                        "vector": event_vector,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )

        entity_hashes = []
        for entity in extraction["entities"]:
            entity_hash = stable_hash(
                f"{node_hash}:{entity['entity_type']}:{entity['entity_name']}"
            )
            previous_entity = self.find_latest_entity(
                node_hash=node_hash,
                entity_type=entity["entity_type"],
                entity_name=entity["entity_name"],
            )
            updated_entity = apply_entity_patches(previous_entity, entity)
            entity_hashes.append(entity_hash)
            records_to_append.append(
                {
                    "record_type": "context_entity",
                    "entity_hash": entity_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "entity_type": updated_entity["entity_type"],
                    "entity_name": updated_entity["entity_name"],
                    "state": updated_entity["state"],
                    "previous_state": updated_entity.get("previous_state", ""),
                    "confidence": updated_entity["confidence"],
                    "operator": updated_entity["operator"],
                    "source_refs": updated_entity["source_refs"],
                    "source_event_ids": source_event_ids,
                    "field_patches": updated_entity.get("field_patches", []),
                    "patch_results": updated_entity.get("patch_results", []),
                    "update_mode": updated_entity.get("update_mode", ""),
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            if updated_entity.get("patch_results"):
                records_to_append.append(
                    {
                        "record_type": "context_entity_update_audit",
                        "entity_hash": entity_hash,
                        "batch_id_hash": batch_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "entity_type": updated_entity["entity_type"],
                        "entity_name": updated_entity["entity_name"],
                        "previous_state": updated_entity.get("previous_state", ""),
                        "new_state": updated_entity["state"],
                        "patch_results": updated_entity.get("patch_results", []),
                        "llm_calls": 0,
                        "update_mode": "deterministic_eua",
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            entity_embedding_text = updated_entity["entity_type"] + " " + updated_entity["state"]
            entity_vector = embedding_for_text(entity_embedding_text)
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": entity_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(entity_vector),
                    "model": embedding_model_name(),
                    "vector": entity_vector,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        segment_hashes = []
        for segment in extraction["segments"]:
            segment_hash = stable_hash(f"{batch_id_hash}:segment:{segment['topic']}:{segment['coordinate_tuples']}")
            segment_hashes.append(segment_hash)
            records_to_append.append(
                {
                    "record_type": "context_segment",
                    "segment_hash": segment_hash,
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "topic": segment["topic"],
                    "coordinate_tuples": segment["coordinate_tuples"],
                    "message_indexes": segment["message_indexes"],
                    "source_event_ids": [event_hashes[index] for index in segment["message_indexes"] if index < len(event_hashes)],
                    "saliency_score": segment["saliency_score"],
                    "summary_text": segment["summary_text"],
                    "text": segment["text"],
                    "non_contiguous": segment["non_contiguous"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            segment_embedding_text = segment["topic"] + " " + segment["summary_text"]
            segment_vector = embedding_for_text(segment_embedding_text)
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "segment_text",
                    "ref_type": "segment",
                    "ref_hash": segment_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(segment_vector),
                    "model": embedding_model_name(),
                    "vector": segment_vector,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )

        summary_hash = stable_hash(f"batch_summary:{batch_id_hash}")
        records_to_append.append(
            {
                "record_type": "context_summary",
                "summary_type": "batch_l0",
                "summary_hash": summary_hash,
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "summary_text": batch_summary,
                "source_entity_hashes": entity_hashes,
                "source_segment_hashes": segment_hashes,
                "source_event_ids": event_hashes,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        summary_embedding_text = " ".join(node_path + [batch_summary])
        summary_vector = embedding_for_text(summary_embedding_text)
        records_to_append.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "batch_l0",
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(summary_vector),
                "model": embedding_model_name(),
                "vector": summary_vector,
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        secondary_index_budget = new_secondary_index_budget()
        batch_index_terms = take_secondary_index_terms(list(extraction["indexes"]), secondary_index_budget)
        for index_name in batch_index_terms:
            records_to_append.append(
                {
                    "record_type": "context_index",
                    "index_name": index_name,
                    "index_hash": stable_hash(f"{index_name}:{batch_id_hash}"),
                    "batch_id_hash": batch_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
        records_to_append.append(
            {
                "record_type": "context_extraction_audit",
                "batch_id_hash": batch_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "schema": extraction["schema"],
                "message_count": extraction["message_count"],
                "token_count_estimate": extraction["token_count_estimate"],
                "outputs": {
                    "events": 0 if derive_from_existing_events else len(envelope["messages"]),
                    "source_events": len(event_hashes),
                    "entities": len(entity_hashes),
                    "segments": len(segment_hashes),
                    "summaries": 1,
                    "indexes": len(batch_index_terms),
                    **secondary_index_budget_summary(secondary_index_budget),
                },
                "mode": extraction["mode"],
                "derive_from_existing_events": derive_from_existing_events,
                "source_event_ids": event_hashes,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        dirty_hashes, dirty_records = self.node_summary_dirty_records(
            node_path=node_path,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_ref_type="batch",
            source_hash_field="source_batch_hash",
            source_hash=batch_id_hash,
            dirty_reason="new_event",
        )
        records_to_append.extend(dirty_records)
        self.append_many(records_to_append)
        summary_refresh = {
            "status": "dirty_marked",
            "dirty_hashes": dirty_hashes,
            "refresh_result": None,
            "async_required": True,
            "write_path": "coalesced_with_batch_extract",
        }
        return {
            "status": "accepted",
            "mode": extraction["mode"],
            "segment_provider": extraction.get("segment_provider", {}),
            "classification": extraction["classification"],
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
            "storage_options": envelope.get("storage_options", {}),
            "storage_route": envelope.get("storage_route", {}),
            "embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "message_count": extraction["message_count"],
            "token_count_estimate": extraction["token_count_estimate"],
            "events_written": 0 if derive_from_existing_events else len(envelope["messages"]),
            "source_event_count": len(event_hashes),
            "raw_events_duplicated": not derive_from_existing_events,
            "entities_written": len(entity_hashes),
            "segments_written": len(segment_hashes),
            "summary_hash": summary_hash,
            "summary_refresh": summary_refresh,
            "node_materialization": node_materialization,
            "indexes_written": len(batch_index_terms),
            **secondary_index_budget_summary(secondary_index_budget),
            "one_pass": True,
            "threshold_messages": threshold,
        }

    def write_time_compression(
        self,
        *,
        scope: Json,
        node_hash: int,
        node_path: list[str],
        source_start_ms: int,
        source_end_ms: int,
        compressed_time_ms: int,
        max_source_events: int = 32,
        min_confidence: float = 0.0,
        min_importance: float = 0.0,
        summary: str = "",
    ) -> Json:
        if source_start_ms > source_end_ms:
            raise MatrixArkError("source_start_ms must be <= source_end_ms")
        if max_source_events <= 0:
            raise MatrixArkError("max_source_events must be positive")
        records = self.read_all()
        debug_by_ref = {
            record.get("ref_hash"): record.get("debug_payload", {})
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_type") == "event"
        }
        source_events = []
        event_times: dict[int, int] = {}
        event_scopes: dict[int, Json] = {}
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            event_hash = int(record.get("event_id_hash") or 0)
            debug_payload = debug_by_ref.get(event_hash, {}) if event_hash else {}
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
            if not isinstance(envelope, dict):
                envelope = {}
            event_scope = envelope.get("scope", scope_from_serving_record(record))
            if not scope_matches(event_scope, scope):
                continue
            event_time = int(envelope.get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            if event_time < source_start_ms or event_time > source_end_ms:
                continue
            extraction = record.get("internal_extraction", {}) if isinstance(record.get("internal_extraction"), dict) else debug_payload.get("internal_extraction", {})
            if not isinstance(extraction, dict):
                extraction = {}
            confidence = float(extraction.get("confidence", record.get("confidence", 1.0)) or 1.0)
            metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
            importance = float(metadata.get("importance", record.get("importance", 1.0)) or 1.0)
            if confidence < min_confidence or importance < min_importance:
                continue
            source_events.append(record)
            event_times[event_hash] = event_time
            event_scopes[event_hash] = event_scope
        source_events.sort(key=lambda record: event_times.get(int(record.get("event_id_hash") or 0), 0))
        selected = source_events[:max_source_events]
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        truncated = len(source_events) > len(selected)
        source_event_ids = [int(record["event_id_hash"]) for record in selected]
        compression_scope = event_scopes.get(int(selected[0].get("event_id_hash") or 0), scope)
        if not summary:
            snippets = [summarize_text(str(record.get("text", "")), limit=180) for record in selected[:5]]
            suffix = " plus additional source events" if truncated else ""
            summary = (
                f"Temporal compression window [{source_start_ms}, {source_end_ms}] contains "
                f"{len(selected)} selected events{suffix}. " + " | ".join(snippets)
            )
        compression_id_hash = stable_hash(f"compress:{scope}:{node_hash}:{source_start_ms}:{source_end_ms}:{source_event_ids}")
        record = {
            "record_type": "context_compression_event",
            "compression_id_hash": compression_id_hash,
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": compression_scope,
            "source_start_ms": source_start_ms,
            "source_end_ms": source_end_ms,
            "compressed_time_ms": compressed_time_ms,
            "summary_text": summarize_text(summary, limit=1200),
            "source_event_ids": source_event_ids,
            "source_event_count": len(selected),
            "truncated_source_events": truncated,
            "operator": "TIME_COMPRESS",
            "updated_at_ms": compressed_time_ms,
        }
        self.append(record)
        self.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "compression_summary",
                "ref_type": "compression",
                "ref_hash": compression_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(embedding_for_text(record["summary_text"])),
                "model": embedding_model_name(),
                "vector": embedding_for_text(record["summary_text"]),
                "scope": compression_scope,
                "updated_at_ms": compressed_time_ms,
            }
        )
        return record

    def query_time_compressions(
        self, *, scope: Json, node_hashes: set[int], start_time_ms: int, end_time_ms: int, limit: int = 16
    ) -> list[Json]:
        matches = []
        for record in self.read_all():
            if record.get("record_type") != "context_compression_event":
                continue
            if node_hashes and int(record.get("node_hash") or 0) not in node_hashes:
                continue
            if not scope_matches(candidate_access_scope(record), scope):
                continue
            if int(record.get("source_end_ms") or 0) >= start_time_ms and int(record.get("source_start_ms") or 0) <= end_time_ms:
                matches.append(record)
        matches.sort(key=lambda record: (int(record.get("source_end_ms") or 0), int(record.get("compressed_time_ms") or 0)), reverse=True)
        return matches[:limit]

    def append_recall_reinforcement_markers(
        self,
        *,
        context_pack_id: str,
        selected_refs: list[Json],
        reinforced_at_ms: int,
        protect_ms: int = TIME_COMPRESSION_REINFORCEMENT_PROTECT_MS,
    ) -> Json:
        protect_ms = max(0, int(protect_ms))
        protected_until_ms = reinforced_at_ms + protect_ms if protect_ms else 0
        records: list[Json] = []
        seen: set[tuple[int, int]] = set()
        for ref in selected_refs:
            source_ids: list[int] = []
            if ref.get("ref_type") == "event" and ref.get("ref_hash") is not None:
                try:
                    source_ids.append(int(ref.get("ref_hash")))
                except (TypeError, ValueError):
                    pass
            for event_id in ref.get("source_event_ids", []) or []:
                try:
                    source_ids.append(int(event_id))
                except (TypeError, ValueError):
                    pass
            for event_id in source_ids:
                try:
                    node_hash = int(ref.get("node_hash") or 0)
                except (TypeError, ValueError):
                    node_hash = 0
                key = (event_id, node_hash)
                if key in seen:
                    continue
                seen.add(key)
                records.append(
                    {
                        "record_type": "context_recall_reinforcement",
                        "event_id_hash": event_id,
                        "node_hash": node_hash,
                        "node_path": ref.get("node_path", []),
                        "context_pack_id": context_pack_id,
                        "source_ref_type": ref.get("ref_type"),
                        "source_ref_hash": ref.get("ref_hash"),
                        "scope": ref.get("scope", {}),
                        "reinforced_at_ms": reinforced_at_ms,
                        "protected_until_ms": protected_until_ms,
                        "reason": "selected_in_context_pack",
                        "created_at_ms": reinforced_at_ms,
                        "updated_at_ms": reinforced_at_ms,
                    }
                )
        if records:
            self.append_many(records)
        return {
            "reinforced_event_count": len(records),
            "protect_ms": protect_ms,
            "protected_until_ms": protected_until_ms,
        }

    def deadline_fallback_pack(
        self,
        *,
        query: str,
        scope: Json,
        question_type: str,
        max_context_tokens: int,
        local_budget: Json,
        deadline_ms: int,
        elapsed_ms: float,
        records: list[Json],
        reason: str,
        budget_source: str = "matrixark_default_max_context_tokens",
    ) -> Json:
        selected = []
        used_context_tokens = 0
        local_tokens = int(local_budget.get("token_estimate", 0))
        safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
        remote_budget = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
        for record in reversed(records):
            record_type = record.get("record_type")
            record_scope = candidate_access_scope(record)
            if record_type not in {"context_summary", "context_entity", "context_event", "context_segment"}:
                continue
            if not scope_matches(record_scope, scope):
                continue
            if record_type == "context_summary":
                text = str(record.get("summary_text", ""))
                ref_type = "summary"
                ref_hash = record.get("summary_hash") or record.get("node_hash")
            elif record_type == "context_entity":
                text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
                ref_type = "entity"
                ref_hash = record.get("entity_hash")
            elif record_type == "context_segment":
                text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
                ref_type = "segment"
                ref_hash = record.get("segment_hash")
            else:
                text = str(record.get("summary_text") or record.get("text") or "")
                ref_type = "event"
                ref_hash = record.get("event_id_hash")
            if not text or ref_hash is None:
                continue
            item_tokens = token_count(text)
            if used_context_tokens + item_tokens > remote_budget:
                continue
            selected.append(
                {
                    "ref_type": ref_type,
                    "ref_hash": ref_hash,
                    "node_hash": record.get("node_hash"),
                    "node_path": record.get("node_path", []),
                    "score": 0.0,
                    "recall_path": "deadline_fallback_recent_context",
                    "updated_at_ms": record.get("updated_at_ms", record.get("envelope", {}).get("ingestion_time_ms", now_ms())),
                    "text": clip_context_text(text),
                }
            )
            used_context_tokens += item_tokens
            if len(selected) >= 8:
                break
        context_pack_id = str(stable_hash(f"deadline:{query}:{selected}:{now_ms()}"))
        serving_selected = compact_context_pack_refs(selected, include_debug=False)
        pack = {
            "context_pack_id": context_pack_id,
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "local_context_refs": local_context_refs_for_pack(local_budget),
            "selected_refs": serving_selected,
            "remote_context_refs": serving_selected,
            "layer_scores": [],
            "question_type": question_type,
            "packing_policy": f"deadline_fallback:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "deadline_ms": deadline_ms,
                "elapsed_ms": elapsed_ms,
                "partial_context_pack": True,
                "fallback_reason": reason,
            },
            "primary_candidate_count": 0,
            "auxiliary_candidate_count": 0,
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
            "remote_context_budget_tokens": remote_budget,
            "requested_max_context_tokens": max_context_tokens,
            "local_context_safety_margin_tokens": safety_margin_tokens,
            "budget_source": budget_source,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_tokens,
                "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
                "safety_margin_tokens": safety_margin_tokens,
                "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": {},
            "quality_warnings": [f"retrieval_deadline_exceeded:{reason}"],
            "insufficient_context": not selected,
            "partial_context_pack": True,
        }
        if reason != "service_backpressure":
            self.append_audit(
                compact_context_pack_audit_record({
                    "record_type": "context_pack_audit",
                    "context_pack_id": context_pack_id,
                    "query": query,
                    "scope": scope,
                    "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected), limit=512),
                    "selected_refs": compact_refs_for_audit(selected),
                    "local_context_refs": compact_local_context_refs(local_budget),
                    "context_sources_order": pack["context_sources_order"],
                    "question_type": question_type,
                    "packing_policy": pack["packing_policy"],
                    "recall_policy": pack["recall_policy"],
                    "local_context_policy": pack["local_context_policy"],
                    "used_local_context_tokens": pack["used_local_context_tokens"],
                    "used_remote_context_tokens": pack["used_remote_context_tokens"],
                    "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                    "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                    "requested_max_context_tokens": pack["requested_max_context_tokens"],
                    "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
                    "budget_source": pack["budget_source"],
                    "primary_candidate_count": 0,
                    "auxiliary_candidate_count": 0,
                    "created_at_ms": now_ms(),
                })
            )
        else:
            pack["operational_visibility_policy"] = {
                "audit_mode": "telemetry_only",
                "rich_replay_audit": False,
                "reason": "service_backpressure_uses_access_audit_only",
            }
        )
        return compact_context_pack_for_serving(pack)

    def supports_native_candidate_prefilter(self) -> bool:
        return False

    def supports_native_context_pack(self) -> bool:
        return False

    def native_context_pack_required(self) -> bool:
        if MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK:
            return MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK in {"1", "true", "yes"}
        backend_label = str(getattr(self, "_backend_label", lambda: "local")())
        return backend_label != "local"

    def native_context_pack(self, request: Json) -> Json | None:
        """Return a backend-assembled ContextPack when the native backend supports it.

        Python remains responsible for MCP/auth/model glue and request shaping.
        C++/Rust backends should own scan, secondary-index filtering, scoring, and
        budget-aware pack assembly through this boundary when available.
        """
        return None

    def retrieve(self, args: Json) -> Json:
        started_perf = time.perf_counter()
        query = require_string(args, "query")
        scope = optional_object(args, "scope")
        storage_options = normalize_storage_options(args)
        ranking = optional_object(args, "ranking")
        audit_mode = str(args.get("audit_mode") or os.environ.get("MATRIXARK_CONTEXT_AUDIT_MODE", "telemetry_only")).strip().lower()
        if audit_mode not in {"full", "telemetry_only", "off"}:
            raise MatrixArkError("audit_mode must be full, telemetry_only, or off")
        if "audit_sample_rate" in args:
            raw_audit_sample_rate = args.get("audit_sample_rate")
        elif audit_mode == "full":
            raw_audit_sample_rate = 1.0
        else:
            raw_audit_sample_rate = os.environ.get("MATRIXARK_CONTEXT_AUDIT_SAMPLE_RATE", 0.01)
        try:
            audit_sample_rate = clamp01(float(raw_audit_sample_rate))
        except (TypeError, ValueError):
            raise MatrixArkError("audit_sample_rate must be a number between 0 and 1")
        raw_deadline_ms = args.get("deadline_ms", ranking.get("deadline_ms", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)))
        try:
            deadline_ms = int(raw_deadline_ms or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("deadline_ms must be an integer")

        def deadline_exceeded() -> bool:
            return deadline_ms > 0 and (time.perf_counter() - started_perf) * 1000.0 >= deadline_ms

        stage_names = ["query_understanding", "candidate_fetch", "node_traversal", "rerank_score", "pack", "audit"]
        explicit_stage_budgets = optional_object(args, "stage_budgets_ms") or optional_object(ranking, "stage_budgets_ms")
        if deadline_ms > 0:
            default_stage_budgets = {
                "query_understanding": max(25, int(deadline_ms * 0.15)),
                "candidate_fetch": max(25, int(deadline_ms * 0.20)),
                "node_traversal": max(25, int(deadline_ms * 0.15)),
                "rerank_score": max(25, int(deadline_ms * 0.30)),
                "pack": max(25, int(deadline_ms * 0.15)),
                "audit": max(10, int(deadline_ms * 0.05)),
            }
        else:
            default_stage_budgets = {
                "query_understanding": 500,
                "candidate_fetch": 750,
                "node_traversal": 500,
                "rerank_score": 1000,
                "pack": 500,
                "audit": 250,
            }
        stage_budgets_ms: dict[str, int] = {}
        for stage in stage_names:
            value = explicit_stage_budgets.get(stage, ranking.get(f"{stage}_budget_ms", default_stage_budgets[stage]))
            if not isinstance(value, int) or value < 0:
                raise MatrixArkError(f"stage budget for {stage} must be a non-negative integer")
            stage_budgets_ms[stage] = value
        stage_latencies_ms: dict[str, float] = {}
        stage_started_perf = time.perf_counter()

        def finish_retrieval_stage(stage: str, started: float) -> float:
            elapsed = round((time.perf_counter() - started) * 1000.0, 3)
            stage_latencies_ms[stage] = elapsed
            self._observe_model_latency(f"retrieval_{stage}", elapsed)
            return elapsed

        def stage_budget_snapshot() -> Json:
            stages = {
                stage: {
                    "budget_ms": stage_budgets_ms[stage],
                    "elapsed_ms": round(float(stage_latencies_ms.get(stage, 0.0)), 3),
                    "over_budget": bool(stage_budgets_ms[stage] > 0 and float(stage_latencies_ms.get(stage, 0.0)) > stage_budgets_ms[stage]),
                }
                for stage in stage_names
            }
            return {
                "enabled": True,
                "source": "explicit" if explicit_stage_budgets else ("deadline_derived" if deadline_ms > 0 else "defaults"),
                "stages": stages,
                "over_budget_stages": [stage for stage, row in stages.items() if row["over_budget"]],
            }

        question_type = str(args.get("question_type") or infer_query_type(query))
        retrieval_session_scope = str(args.get("session_scope") or ranking.get("session_scope") or "prefer").strip().lower()
        if retrieval_session_scope not in {"prefer", "only"}:
            raise MatrixArkError("session_scope must be prefer or only")
        retrieval_scope = {**scope, "_session_scope": retrieval_session_scope}
        secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
        secondary_index_filter_mode = "any_group" if len(secondary_index_filter_groups) > 1 else "all_groups"
        secondary_index_dropped_count = 0
        secondary_index_matched_count = 0
        budget_source = "agent_provided_max_context_tokens" if "max_context_tokens" in args else "matrixark_default_max_context_tokens"
        max_context_tokens = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            raise MatrixArkError("max_context_tokens must be a positive integer")
        local_budget = local_context_budget(args)
        local_tokens = int(local_budget.get("token_estimate", 0))
        safety_margin_tokens = int(local_budget.get("safety_margin_tokens", 0))
        remote_context_budget_tokens = max(0, max_context_tokens - local_tokens - safety_margin_tokens)
        local_budget["remote_budget_tokens"] = remote_context_budget_tokens
        cross_session_policy = build_cross_session_policy(
            args,
            ranking,
            question_type=question_type,
            session_scope=retrieval_session_scope,
            remote_budget_tokens=remote_context_budget_tokens,
        )
        shared_context_policy = build_shared_context_policy(
            args,
            ranking,
            remote_budget_tokens=remote_context_budget_tokens,
        )
        query_terms = {term for term in tokens(query) if len(term) > 2}
        raw_reference_time_ms = args.get("reference_time_ms", now_ms())
        if not isinstance(raw_reference_time_ms, int):
            raise MatrixArkError("reference_time_ms must be an integer")
        reference_time_ms = raw_reference_time_ms
        query_plan = build_structured_query_plan(
            query,
            question_type=question_type,
            secondary_index_filter_groups=secondary_index_filter_groups,
            secondary_index_filter_mode=secondary_index_filter_mode,
            reference_time_ms=reference_time_ms,
        )
        pack_cache_enabled = (
            self._context_pack_cache_max_entries > 0
            and self._context_pack_cache_ttl_s > 0
            and python_hot_cache_allowed(backend_label=str(getattr(self, "_backend_label", lambda: "local")()))
        )
        pack_cache_key = (
            self._retrieval_records_cache_generation,
            canonical_scope_key(scope),
            query,
            question_type,
            retrieval_session_scope,
            max_context_tokens,
            int(local_budget.get("token_estimate", 0)),
            tuple(sorted(local_budget.get("text_hashes", set()))),
            json.dumps(ranking, sort_keys=True, separators=(",", ":")),
            bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
        )
        if pack_cache_enabled:
            with self._context_pack_cache_lock:
                cached = self._context_pack_cache.get(pack_cache_key)
                if cached is not None:
                    cached_at, cached_pack = cached
                    if time.monotonic() - cached_at <= self._context_pack_cache_ttl_s:
                        pack = json.loads(json.dumps(cached_pack))
                        pack["context_pack_cache_hit"] = True
                        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
                        recall_policy["context_pack_cache"] = {"hit": True, "ttl_s": self._context_pack_cache_ttl_s}
                        pack["recall_policy"] = recall_policy
                        return compact_context_pack_for_serving(pack, include_debug=debug_refs)
                    self._context_pack_cache.pop(pack_cache_key, None)
        auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
        def annotate_session_continuity(candidate: Json, record: Json) -> Json:
            record_scope = candidate_access_scope(record)
            status = session_continuity_status(record_scope, retrieval_scope)
            boost = session_continuity_boost({**candidate, "session_continuity": status}, question_type)
            reason = (
                "same-session continuity"
                if status == "same_session"
                else "cross-session memory bridge"
                if status == "cross_session"
                else "session-neutral context"
            )
            return {
                **candidate,
                "session_continuity": status,
                "continuity_boost": round(boost, 6),
                "continuity_reason": reason,
                "question_type": question_type,
            }

        finish_retrieval_stage("query_understanding", stage_started_perf)
        native_pack = self.native_context_pack({
            "query": query,
            "scope": retrieval_scope,
            "question_type": question_type,
            "query_plan": query_plan,
            "secondary_index_groups": [sorted(group) for group in secondary_index_filter_groups],
            "secondary_index_filter_mode": secondary_index_filter_mode,
            "max_context_tokens": max_context_tokens,
            "local_budget": {
                "token_estimate": int(local_budget.get("token_estimate", 0)),
                "safety_margin_tokens": int(local_budget.get("safety_margin_tokens", 0)),
                "remote_budget_tokens": int(local_budget.get("remote_budget_tokens", max_context_tokens)),
            },
            "cross_session": cross_session_policy,
            "shared_context": shared_context_policy,
            "ranking": ranking,
            "deadline_ms": deadline_ms,
            "reference_time_ms": reference_time_ms,
            "include_superseded_resources": bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False)),
            "audit_mode": audit_mode,
        })
        if native_pack is not None:
            recall_policy = native_pack.get("recall_policy") if isinstance(native_pack.get("recall_policy"), dict) else {}
            recall_policy.setdefault("native_context_pack", {
                "enabled": True,
                "python_role": "mcp_auth_model_request_shaping_only",
                "backend_role": "scan_filter_score_pack",
            })
            recall_policy.setdefault("stage_latency_budgets", stage_budget_snapshot())
            native_pack["recall_policy"] = recall_policy
            native_pack.setdefault("context_pack_cache_hit", False)
            native_pack.setdefault("context_pack_assembly", "native_backend")
            native_pack.setdefault("remote_context_refs", native_pack.get("selected_refs", []))
            native_pack.setdefault("selected_ref_counts", selected_context_class_counts(native_pack.get("selected_refs", [])))
            selected_refs = native_pack.get("selected_refs", []) if isinstance(native_pack.get("selected_refs"), list) else []
            context_pack_id_text = str(native_pack.get("context_pack_id") or stable_hash(f"native:{query}:{selected_refs}:{now_ms()}"))
            native_pack["context_pack_id"] = context_pack_id_text
            debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
            if audit_mode == "full" and audit_sample_rate > 0 and (audit_sample_rate >= 1.0 or stable_hash(context_pack_id_text) % 10000 < int(audit_sample_rate * 10000)):
                self.append_audit(
                    compact_context_pack_audit_record({
                        "record_type": "context_pack_audit",
                        "context_pack_id": context_pack_id_text,
                        "query": query,
                        "scope": scope,
                        "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected_refs), limit=512),
                        "selected_refs": compact_refs_for_audit(selected_refs),
                        "local_context_refs": compact_local_context_refs(local_budget),
                        "context_sources_order": native_pack.get("context_sources_order", []),
                        "selected_ref_counts": native_pack.get("selected_ref_counts", {}),
                        "dropped_refs": native_pack.get("dropped_refs", {}),
                        "quality_warnings": native_pack.get("quality_warnings", []),
                        "question_type": question_type,
                        "packing_policy": native_pack.get("packing_policy", "native_backend"),
                        "recall_policy": recall_policy,
                        "stage_latency_budgets": recall_policy.get("stage_latency_budgets", {}),
                        "storage_options": storage_options,
                        "used_remote_context_tokens": native_pack.get("used_remote_context_tokens", native_pack.get("used_context_tokens", 0)),
                        "remote_context_budget_tokens": native_pack.get("remote_context_budget_tokens", max_context_tokens),
                        "requested_max_context_tokens": native_pack.get("requested_max_context_tokens", max_context_tokens),
                        "created_at_ms": now_ms(),
                    })
                )
            serving_selected_refs = compact_context_pack_refs(selected_refs, include_debug=debug_refs)
            native_pack["selected_refs"] = serving_selected_refs
            native_pack["remote_context_refs"] = serving_selected_refs
            native_pack["dropped_refs"] = compact_dropped_refs_for_context_pack(native_pack.get("dropped_refs", {}), include_debug=debug_refs)
            native_pack["context_pack_payload_policy"] = {
                "serving_refs": "compact" if not debug_refs else "debug_full",
                "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
                "dropped_ref_details": "audit_only" if not debug_refs else "included",
                "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
            }
            return compact_context_pack_for_serving(native_pack, include_debug=debug_refs)
        if self.native_context_pack_required():
            raise MatrixArkError(
                "backend-native ContextPack assembly is required for TemporalStore serving, "
                "but this backend did not return matrixark_retrieve_context_pack. "
                "Python reference packing is disabled unless explicitly overridden for local debug."
            )
        embedding_started_perf = time.perf_counter()
        query_embedding = embedding_for_text(query)
        self._observe_model_latency("query_embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        stage_started_perf = time.perf_counter()
        retrieval_record_result = self.retrieval_records(
            scope=retrieval_scope,
            secondary_index_groups=secondary_index_filter_groups,
        )
        records = retrieval_record_result["records"]
        retrieval_scan_stats = retrieval_record_result.get("scan_stats", {})

        def deadline_fallback(reason: str, fallback_records: list[Json] | None = None) -> Json:
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records if fallback_records is None else fallback_records,
                reason=reason,
                budget_source=budget_source,
            )
        skill_controls = self.latest_skill_controls(records)
        include_superseded_resources = bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False))
        latest_resource_version_by_hash: dict[int, str] = {}
        resource_uri_by_hash: dict[int, str] = {}
        for manifest in reversed(records):
            if manifest.get("record_type") != "resource_manifest":
                continue
            if not scope_matches(candidate_access_scope(manifest), scope):
                continue
            try:
                resource_hash_key = int(manifest.get("resource_hash") or 0)
            except (TypeError, ValueError):
                resource_hash_key = 0
            raw_uri_key = str(manifest.get("raw_uri") or "")
            resource_version_key = str(manifest.get("resource_version") or "")
            if resource_hash_key:
                if raw_uri_key and resource_hash_key not in resource_uri_by_hash:
                    resource_uri_by_hash[resource_hash_key] = raw_uri_key
                if resource_version_key and resource_hash_key not in latest_resource_version_by_hash:
                    latest_resource_version_by_hash[resource_hash_key] = resource_version_key
        finish_retrieval_stage("candidate_fetch", stage_started_perf)
        stage_started_perf = time.perf_counter()
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_record_load",
                budget_source=budget_source,
            )
        node_scores: dict[int, Json] = {}
        event_embedding_vectors: dict[int, list[float]] = {}
        entity_embedding_vectors: dict[int, list[float]] = {}
        segment_embedding_vectors: dict[int, list[float]] = {}
        compression_embedding_vectors: dict[int, list[float]] = {}
        resource_embedding_vectors: dict[int, list[float]] = {}
        skill_embedding_vectors: dict[int, list[float]] = {}
        index_terms_by_batch: dict[Any, list[str]] = {}
        index_terms_by_node: dict[Any, list[str]] = {}
        index_terms_by_ref: dict[Any, list[str]] = {}
        index_terms_by_node_for_prefilter: dict[int, list[str]] = {}
        node_summary_text_by_hash: dict[int, str] = {}
        for scan_index, record in enumerate(records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_embedding_index_scan")
            record_type = record.get("record_type")
            if record_type == "context_index" and scope_matches(candidate_access_scope(record), retrieval_scope):
                index_name = str(record.get("index_name", ""))
                if index_name:
                    ref_hashes = context_index_ref_hashes(record)
                    if record.get("batch_id_hash") is not None:
                        index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                    node_hash_for_index = record.get("node_hash")
                    try:
                        index_terms_by_node_for_prefilter.setdefault(int(node_hash_for_index), []).append(index_name)
                    except (TypeError, ValueError):
                        pass
                    if ref_hashes:
                        for ref_hash in ref_hashes:
                            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                    else:
                        ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                        if ref_hash is not None:
                            index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                        else:
                            index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
            if record_type == "context_summary" and scope_matches(candidate_access_scope(record), scope):
                summary_type = str(record.get("summary_type", ""))
                if summary_type in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
                    try:
                        node_hash_for_summary = int(record.get("node_hash"))
                    except (TypeError, ValueError):
                        continue
                    existing = node_summary_text_by_hash.get(node_hash_for_summary, "")
                    summary_text = str(record.get("summary_text", ""))
                    if len(summary_text) > len(existing):
                        node_summary_text_by_hash[node_hash_for_summary] = summary_text
        secondary_index_prefilter_node_hashes = {
            node_hash
            for node_hash, terms in index_terms_by_node_for_prefilter.items()
            if passes_secondary_index_filters(set(terms), secondary_index_filter_groups, mode=secondary_index_filter_mode)
        } if secondary_index_filter_groups else set()
        query_plan["secondary_index_prefilter"] = {
            "applied_before_l0_l1_traversal": True,
            "matched_node_count": len(secondary_index_prefilter_node_hashes),
            "fallback_when_no_index_matches": True,
            "strategy": "ContextIndex node hints boost L0/L1 traversal; leaf candidates still verify filters before embedding scoring",
        }
        for scan_index, record in enumerate(records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_embedding_vector_scan")
            record_type = record.get("record_type")
            if record_type == "context_embedding" and not scope_matches(candidate_access_scope(record), scope):
                continue
            if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
                dense_score = cosine(query_embedding, record.get("vector", []))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                index_hint_boost = 0.08 if node_hash in secondary_index_prefilter_node_hashes else 0.0
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score + index_hint_boost), 6)
                current = node_scores.get(node_hash)
                if current is None or score > current["score"]:
                    node_scores[node_hash] = {
                        "node_hash": node_hash,
                        "node_path": record.get("node_path", []),
                        "depth": record.get("depth", len(record.get("node_path", []))),
                        "score": score,
                        "dense_score": dense_score,
                        "sparse_score": sparse_score,
                        "embedding_type": record.get("embedding_type"),
                    }
            elif record_type == "context_embedding" and record.get("embedding_type") == "event_text":
                event_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "entity_state":
                entity_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "segment_text":
                segment_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "compression_summary":
                compression_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "resource_chunk":
                resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_section":
                resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
            elif record_type == "context_embedding" and record.get("embedding_type") == "skill_summary":
                skill_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
        for record in records:
            if record.get("record_type") != "context_node":
                continue
            try:
                node_hash = int(record.get("node_hash"))
            except (TypeError, ValueError):
                continue
            if node_hash not in secondary_index_prefilter_node_hashes or node_hash in node_scores:
                continue
            node_scores[node_hash] = {
                "node_hash": node_hash,
                "node_path": record.get("node_path", []),
                "depth": record.get("depth", len(record.get("node_path", []))),
                "score": 0.58,
                "dense_score": 0.0,
                "sparse_score": 0.0,
                "embedding_type": "secondary_index_hint",
            }
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_embedding_index_scan",
                budget_source=budget_source,
            )

        top_k_per_layer = integer_arg(ranking, "top_k_per_layer", DEFAULT_TOP_K_PER_LAYER, minimum=1)
        max_children_scored_per_parent = bounded_max_children_scored_per_parent(
            integer_arg(
                ranking,
                "max_children_scored_per_parent",
                DEFAULT_MAX_CHILDREN_SCORED_PER_PARENT,
                minimum=1,
            )
        )
        hard_max_children_scored_per_parent = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
        max_candidates_per_node = integer_arg(ranking, "max_candidates_per_node", DEFAULT_MAX_CANDIDATES_PER_NODE, minimum=1)
        max_selected_refs = integer_arg(ranking, "max_selected_refs", DEFAULT_MAX_SELECTED_REFS, minimum=1)
        max_global_candidates = integer_arg(ranking, "max_global_candidates", DEFAULT_MAX_GLOBAL_CANDIDATES, minimum=1)
        min_similarity_score = float_arg(ranking, "min_similarity_score", DEFAULT_RETRIEVAL_MIN_SCORE, minimum=0.0, maximum=1.0)
        budget_fill_policy = str(ranking.get("budget_fill_policy", DEFAULT_BUDGET_FILL_POLICY) or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
        if budget_fill_policy not in {"quality_first", "force_fill"}:
            raise MatrixArkError("budget_fill_policy must be quality_first or force_fill")
        max_raw_events_per_node = integer_arg(ranking, "max_raw_events_per_node", TIME_COMPRESSION_MAX_RAW_EVENTS_PER_NODE, minimum=1)
        traversal = tree_first_traversal(
            node_scores,
            top_k_per_layer=top_k_per_layer,
            max_children_scored_per_parent=max_children_scored_per_parent,
        )
        finish_retrieval_stage("node_traversal", stage_started_perf)
        stage_started_perf = time.perf_counter()
        selected_paths = traversal["selected_paths"]
        selected_leaf_paths = traversal["leaf_paths"]
        selected_node_hashes = traversal["selected_node_hashes"]

        placement_record_result: Json = {}
        placement_candidate_records: list[Json] = []
        if selected_node_hashes and not traversal.get("fallback_to_flat"):
            placement_record_result = self.retrieval_records(
                scope=scope,
                secondary_index_groups=secondary_index_filter_groups,
                selected_node_hashes=selected_node_hashes,
                allow_broad_scan_fallback=False,
            )
            placement_candidate_records = placement_record_result.get("records", [])

            def record_identity(record: Json) -> tuple[str, Any]:
                record_type = str(record.get("record_type") or "")
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
                ):
                    if record.get(field) is not None:
                        return (record_type, record.get(field))
                if record_type == "context_index":
                    return (
                        record_type,
                        (
                            record.get("index_name"),
                            record.get("node_hash"),
                            tuple(context_index_ref_hashes(record)),
                            record.get("timestamp_key_ms"),
                        ),
                    )
                return (record_type, stable_hash(json.dumps(record, sort_keys=True, separators=(",", ":"))))

            seen_record_identities = {record_identity(record) for record in records}
            for record in placement_candidate_records:
                identity = record_identity(record)
                if identity in seen_record_identities:
                    continue
                records.append(record)
                seen_record_identities.add(identity)

            for record in placement_candidate_records:
                record_type = record.get("record_type")
                if record_type == "context_index" and scope_matches(candidate_access_scope(record), scope):
                    index_name = str(record.get("index_name", ""))
                    if index_name:
                        ref_hashes = context_index_ref_hashes(record)
                        if record.get("batch_id_hash") is not None:
                            index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
                        node_hash_for_index = record.get("node_hash")
                        try:
                            index_terms_by_node_for_prefilter.setdefault(int(node_hash_for_index), []).append(index_name)
                        except (TypeError, ValueError):
                            pass
                        if ref_hashes:
                            for ref_hash in ref_hashes:
                                index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                        else:
                            ref_hash = record.get("ref_hash") or record.get("chunk_hash") or record.get("section_hash") or record.get("skill_hash")
                            if ref_hash is not None:
                                index_terms_by_ref.setdefault(ref_hash, []).append(index_name)
                            else:
                                index_terms_by_node.setdefault(record.get("node_hash"), []).append(index_name)
                elif record_type == "context_embedding" and scope_matches(candidate_access_scope(record), scope):
                    embedding_type = record.get("embedding_type")
                    if embedding_type == "event_text":
                        event_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "entity_state":
                        entity_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "segment_text":
                        segment_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "compression_summary":
                        compression_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "resource_chunk":
                        resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "skill_section":
                        resource_embedding_vectors[record["ref_hash"]] = record.get("vector", [])
                    elif embedding_type == "skill_summary":
                        skill_embedding_vectors[record["ref_hash"]] = record.get("vector", [])

        def selected_by_tree(record: Json) -> bool:
            if traversal.get("fallback_to_flat"):
                return True
            path = node_path_tuple(record.get("node_path", []))
            if path and path in selected_paths:
                return True
            if path and any(
                starts_with_path(path, leaf_path) or starts_with_path(leaf_path, path)
                for leaf_path in selected_leaf_paths
            ):
                return True
            try:
                return int(record.get("node_hash")) in selected_node_hashes
            except (TypeError, ValueError):
                return False

        if placement_candidate_records and not traversal.get("fallback_to_flat"):
            tree_candidate_records = [record for record in placement_candidate_records if selected_by_tree(record)]
            tree_prefilter_dropped_count = max(0, len(placement_candidate_records) - len(tree_candidate_records))
            retrieval_scan_stats = {
                **retrieval_scan_stats,
                "leaf_fetch": placement_record_result.get("scan_stats", {}),
                "leaf_fetch_record_count": len(placement_candidate_records),
                "leaf_fetch_strategy": "selected_node_placement",
            }
        else:
            tree_candidate_records = records if traversal.get("fallback_to_flat") else [record for record in records if selected_by_tree(record)]
            tree_prefilter_dropped_count = 0 if traversal.get("fallback_to_flat") else max(0, len(records) - len(tree_candidate_records))
        raw_event_ids_by_node: dict[Any, set[int]] = {}
        raw_event_time_window_dropped_count = 0
        events_by_node: dict[Any, list[Json]] = {}
        nodes_with_compression: set[Any] = set()
        for scan_index, record in enumerate(tree_candidate_records, 1):
            if scan_index % 128 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_tree_candidate_prefilter", records)
            if record.get("record_type") == "context_compression_event":
                node_key_for_compression: Any = record.get("node_hash")
                if node_key_for_compression is None:
                    node_key_for_compression = tuple(record.get("node_path", []))
                nodes_with_compression.add(node_key_for_compression)
                continue
            if record.get("record_type") != "context_event":
                continue
            if record.get("source_chunk_hash"):
                continue
            node_key: Any = record.get("node_hash")
            if node_key is None:
                node_key = tuple(record.get("node_path", []))
            events_by_node.setdefault(node_key, []).append(record)
        for node_key, node_events in events_by_node.items():
            if node_key not in nodes_with_compression:
                continue
            node_events.sort(
                key=lambda item: (
                    self.context_event_ingestion_time_ms(item),
                    int(item.get("event_id_hash") or 0),
                ),
                reverse=True,
            )
            admitted = {
                int(record.get("event_id_hash"))
                for record in node_events[:max_raw_events_per_node]
                if record.get("event_id_hash") is not None
            }
            raw_event_ids_by_node[node_key] = admitted
            raw_event_time_window_dropped_count += max(0, len(node_events) - len(admitted))
        candidate_count_by_node: dict[Any, int] = {}
        fanout_dropped_count = 0

        def admit_candidate_for_node(record: Json) -> bool:
            nonlocal fanout_dropped_count
            node_key: Any = record.get("node_hash")
            if node_key is None:
                node_key = tuple(record.get("node_path", []))
            count = candidate_count_by_node.get(node_key, 0)
            if count >= max_candidates_per_node:
                fanout_dropped_count += 1
                return False
            candidate_count_by_node[node_key] = count + 1
            return True

        layer_scores = sorted(
            traversal["trace"] or node_scores.values(),
            key=lambda item: (item.get("depth", 0), -float(item.get("score", 0.0)), item.get("node_hash", 0)),
        )
        primary_matches = []
        auxiliary_matches = []
        if question_type == "broad_exploration":
            for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
                if scan_index % 64 == 0 and deadline_exceeded():
                    return deadline_fallback("deadline_during_summary_scan", records)
                if record.get("record_type") != "context_summary":
                    continue
                if not access_scope_matches_before_scoring(record, retrieval_scope):
                    continue
                if not selected_by_tree(record):
                    continue
                summary_type = str(record.get("summary_type") or "")
                if summary_type not in {"node_l0", "node_l1", "resource_l0", "batch_l0", "session_l0"}:
                    continue
                index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
                if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                    secondary_index_dropped_count += 1
                    continue
                secondary_index_matched_count += 1
                if not admit_candidate_for_node(record):
                    continue
                text = str(record.get("summary_text", ""))
                if not text:
                    continue
                sparse_score = sparse_lexical_score(query_terms, text)
                keyword_score = len(query_terms.intersection(tokens(text)))
                embedding_score = cosine(query_embedding, embedding_for_text(" ".join(record.get("node_path", []) + [summary_type, text])))
                node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
                origin_score = min(1.0, 0.06 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
                if origin_score <= 0:
                    continue
                primary_matches.append(
                    score_recall_candidate(
                        annotate_session_continuity({
                            "ref_type": "summary",
                            "ref_hash": record.get("summary_hash") or record.get("node_hash"),
                            "node_hash": record.get("node_hash"),
                            "node_path": record.get("node_path", []),
                            "origin_score": origin_score,
                            "keyword_score": keyword_score,
                            "sparse_score": sparse_score,
                            "embedding_score": embedding_score,
                            "node_score": node_score,
                            "matched_index_terms": sorted(index_terms),
                            "selection_reason": "selected by tree path and L0/L1 summary relevance",
                            "event_type": summary_type,
                            "context_class": "summary",
                            "summary_type": summary_type,
                            "access_decision": "allowed_by_registry_scope_before_scoring",
                            "access_scope": candidate_access_scope(record),
                            "scope": candidate_access_scope(record),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "recall_path": "primary_summary",
                        }, record),
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_event_scan", records)
            if record.get("record_type") != "context_event":
                continue
            event_node_key: Any = record.get("node_hash")
            if event_node_key is None:
                event_node_key = tuple(record.get("node_path", []))
            if (
                not record.get("source_chunk_hash")
                and event_node_key in raw_event_ids_by_node
                and int(record.get("event_id_hash") or 0) not in raw_event_ids_by_node[event_node_key]
            ):
                continue
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
            record_scope = candidate_access_scope(record)
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = str(record.get("text", ""))
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
            extraction = record.get("internal_extraction", {}) if isinstance(record.get("internal_extraction"), dict) else {}
            event_type = str(record.get("event_type") or extraction.get("event_type") or record.get("classification") or extraction.get("classification") or "")
            candidate_metadata: Json = {}
            record_metadata = record.get("metadata")
            envelope_metadata = envelope.get("metadata")
            if isinstance(record_metadata, dict):
                candidate_metadata.update(record_metadata)
            if isinstance(envelope_metadata, dict):
                candidate_metadata.update(envelope_metadata)
            candidate = {
                "ref_type": "event",
                "ref_hash": record["event_id_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource fact/event hybrid score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and event hybrid score"
                ),
                "event_type": event_type,
                "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": candidate_metadata,
                "scope": record_scope,
                "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
            graph_score = sparse_lexical_score(query_terms, graph_text)
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_event_scan",
                budget_source=budget_source,
            )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_entity_scan", records)
            if record.get("record_type") != "context_entity":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = f"{record.get('entity_type', '')}: {record.get('entity_name', '')} = {record.get('state', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, entity_embedding_vectors.get(record["entity_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.12 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "entity",
                "ref_hash": record["entity_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": (
                    "selected by tree path, secondary indexes, and resource entity state score"
                    if record.get("source_chunk_hash")
                    else "selected by tree path, secondary indexes, and entity state score"
                ),
                "entity_type": record.get("entity_type", ""),
                "entity_name": record.get("entity_name", ""),
                "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
                "source_chunk_hash": record.get("source_chunk_hash"),
                "source_ref": record.get("source_ref", ""),
                "metadata": record.get("metadata", {}),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_entity_scan",
                budget_source=budget_source,
            )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_segment_scan", records)
            if record.get("record_type") != "context_segment":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            text = f"{record.get('topic', '')}: {record.get('summary_text', '')}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            embedding_score = cosine(query_embedding, segment_embedding_vectors.get(record["segment_hash"], []))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            saliency_score = float(record.get("saliency_score", 0.0))
            origin_score = min(
                1.0,
                0.1 + 0.75 * hybrid_origin_score(query_terms, text, embedding_score, node_score) + 0.15 * saliency_score,
            )
            candidate = {
                "ref_type": "segment",
                "ref_hash": record["segment_hash"],
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "matched_index_terms": sorted(index_terms),
                "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
                "saliency_score": saliency_score,
                "topic": record.get("topic", ""),
                "coordinate_tuples": record.get("coordinate_tuples", []),
                "non_contiguous": record.get("non_contiguous", False),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(str(record.get("summary_text", ""))),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_segment_scan",
                budget_source=budget_source,
            )
        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_resource_skill_scan", records)
            if record.get("record_type") not in {"resource_chunk", "skill_section"}:
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            if record.get("record_type") == "resource_chunk" and record.get("resource_type") == "skill":
                continue
            index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
            if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
                secondary_index_dropped_count += 1
                continue
            secondary_index_matched_count += 1
            if not admit_candidate_for_node(record):
                continue
            if record.get("record_type") == "skill_section":
                ref_type = "skill_section"
                ref_hash = int(record.get("section_hash") or 0)
                parent_skill_hash = int(record.get("skill_hash") or 0)
                control = skill_controls.get(parent_skill_hash, {})
                if str(control.get("status") or "active") != "active":
                    continue
                resource_hash = parent_skill_hash
                raw_uri_value = str(record.get("raw_uri") or "")
                source_locator = str(record.get("source_locator") or "")
                citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
                resource_version_value = str(record.get("metadata", {}).get("resource_version") or record.get("resource_version") or "")
                version_state = "current"
                is_superseded_version = False
                text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = "skill"
                metadata = {**record.get("metadata", {}), "skill_registry": control}
            else:
                ref_type = "resource_chunk"
                ref_hash = int(record.get("chunk_hash") or 0)
                metadata = record.get("metadata", {})
                resource_hash = int(record.get("resource_hash") or 0)
                raw_uri_value = str(record.get("raw_uri") or resource_uri_by_hash.get(resource_hash, ""))
                source_locator = str(record.get("source_locator") or metadata.get("source_locator") or "")
                citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
                resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
                latest_version = latest_resource_version_by_hash.get(resource_hash, resource_version_value)
                is_superseded_version = bool(
                    resource_version_value
                    and latest_version
                    and resource_version_value != latest_version
                )
                if is_superseded_version and not include_superseded_resources:
                    secondary_index_dropped_count += 1
                    continue
                version_state = "historical" if is_superseded_version else "current"
                text = f"resource {source_locator}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = str(record.get("resource_type") or "resource")
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            if origin_score <= 0:
                continue
            primary_matches.append(
                score_recall_candidate(
                    annotate_session_continuity({
                        "ref_type": ref_type,
                        "ref_hash": ref_hash,
                        "node_hash": record.get("node_hash"),
                        "node_path": record.get("node_path", []),
                        "origin_score": origin_score,
                        "keyword_score": keyword_score,
                        "sparse_score": sparse_score,
                        "embedding_score": embedding_score,
                        "node_score": node_score,
                        "matched_index_terms": sorted(index_terms),
                        "selection_reason": (
                            "selected by tree path, secondary indexes, and resource/skill hybrid score"
                            if index_terms
                            else "selected by tree path and resource/skill hybrid score"
                        ),
                        "event_type": business_type,
                        "context_class": ref_type,
                        "resource_hash": resource_hash,
                        "source_locator": source_locator,
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": resource_version_value,
                        "supersedes_chunk_hash": metadata.get("supersedes_chunk_hash"),
                        "version_state": version_state,
                        "stale_or_superseded": is_superseded_version,
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "deployment_scope": record.get("deployment_scope", "local"),
                        "citation": citation,
                        "metadata": metadata,
                        "scope": candidate_access_scope(record),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "primary_resource_skill",
                    }, record),
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )

        for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
            if scan_index % 64 == 0 and deadline_exceeded():
                return deadline_fallback("deadline_during_compression_scan", records)
            if record.get("record_type") != "context_compression_event":
                continue
            if not access_scope_matches_before_scoring(record, retrieval_scope):
                continue
            if not selected_by_tree(record):
                continue
            if not admit_candidate_for_node(record):
                continue
            text = f"TIME_COMPRESS: {summarize_text(str(record.get('summary_text', '')), limit=96)}"
            sparse_score = sparse_lexical_score(query_terms, text)
            keyword_score = len(query_terms.intersection(tokens(text)))
            compression_hash = int(record.get("compression_id_hash") or 0)
            embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
            node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
            origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
            candidate = {
                "ref_type": "compression",
                "ref_hash": compression_hash,
                "node_hash": record["node_hash"],
                "node_path": record.get("node_path", []),
                "origin_score": origin_score,
                "keyword_score": keyword_score,
                "sparse_score": sparse_score,
                "embedding_score": embedding_score,
                "node_score": node_score,
                "event_type": "time_compress",
                "operator": "TIME_COMPRESS",
                "source_event_ids": record.get("source_event_ids", []),
                "source_start_ms": record.get("source_start_ms"),
                "source_end_ms": record.get("source_end_ms"),
                "scope": candidate_access_scope(record),
                "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate(annotate_session_continuity({**candidate, "recall_path": "primary_time_compression"}, record), ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + [text, "time_compress"]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **annotate_session_continuity(candidate, record),
                            "recall_path": "auxiliary_keyword_graph",
                            "origin_score": graph_score,
                            "keyword_graph_score": graph_score,
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        if deadline_exceeded():
            return self.deadline_fallback_pack(
                query=query,
                scope=scope,
                question_type=question_type,
                max_context_tokens=max_context_tokens,
                local_budget=local_budget,
                deadline_ms=deadline_ms,
                elapsed_ms=round((time.perf_counter() - started_perf) * 1000.0, 3),
                records=records,
                reason="deadline_after_compression_scan",
                budget_source=budget_source,
            )
        finish_retrieval_stage("rerank_score", stage_started_perf)
        stage_started_perf = time.perf_counter()
        primary_matches.sort(key=lambda item: item["score"], reverse=True)
        auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
        selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
        rerank_candidate_limit = max(selected_ref_cap, max_global_candidates)
        first_stage_candidate_count = len(primary_matches) + len(auxiliary_matches)
        rerank_policy = {
            "enabled": True,
            "stage": "packing_rerank",
            "mode": "question_type_token_efficiency",
            "input_candidate_count": first_stage_candidate_count,
            "max_candidates": rerank_candidate_limit,
            "reranked_candidate_count": min(first_stage_candidate_count, rerank_candidate_limit),
            "question_type": question_type,
            "signals": [
                "weighted_recall_score",
                "question_type_ref_boost",
                "cross_session_rerank_boost",
                "token_efficiency",
                "multi_hop_node_diversity",
            ],
            "cross_session_rerank_enabled": True,
            "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
            "fallback": "weighted_recall",
            "heavy_rerank_enabled": False,
            "min_similarity_score": min_similarity_score,
            "budget_fill_policy": budget_fill_policy,
        }
        selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
            primary_matches,
            auxiliary_matches,
            max_context_tokens=remote_context_budget_tokens,
            auxiliary_quota=auxiliary_quota,
            question_type=question_type,
            reserved_tokens=0,
            max_selected_refs=max_selected_refs,
            min_score=min_similarity_score,
            max_global_candidates=max_global_candidates,
            budget_fill_policy=budget_fill_policy,
            duplicate_text_hashes=local_budget["text_hashes"],
            deadline_exceeded=deadline_exceeded,
            deadline_reason="deadline_during_context_pack",
            cross_session_policy=cross_session_policy,
            shared_context_policy=shared_context_policy,
        )
        partial_context_pack = bool(dropped_over_budget.get("deadline_exceeded"))
        quality_warnings = []
        if partial_context_pack:
            quality_warnings.append(f"retrieval_deadline_exceeded:{dropped_over_budget.get('deadline_reason', 'deadline_during_context_pack')}")
        context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
        context_pack_id_text = str(context_pack_id)
        recall_reinforcement_enabled = bool(ranking.get("recall_reinforcement", True))
        if recall_reinforcement_enabled:
            reinforcement = self.append_recall_reinforcement_markers(
                context_pack_id=context_pack_id_text,
                selected_refs=selected,
                reinforced_at_ms=now_ms(),
            )
        else:
            reinforcement = {
                "reinforced_event_count": 0,
                "protect_ms": 0,
                "protected_until_ms": 0,
                "skipped": True,
                "reason": "disabled_for_read_only_scale_or_benchmark_run",
            }
        debug_refs = bool(args.get("include_debug_refs") or ranking.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS)
        serving_selected = compact_context_pack_refs(selected, include_debug=debug_refs)
        serving_dropped = compact_dropped_refs_for_context_pack(dropped_over_budget, include_debug=debug_refs)
        pack_summary = summarize_text(
            " ".join(str(item.get("text", "")) for item in selected),
            limit=512,
        )
        selected_context_counts = selected_context_class_counts(selected)
        freshness_tolerance_ms = int(ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS))
        half_life_ms = int(ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS))
        selected_time_scores = [float(item.get("time_score", 0.0)) for item in selected if "time_score" in item]
        selected_age_ms: list[int] = []
        for item in selected:
            try:
                selected_age_ms.append(max(0, int(reference_time_ms) - int(item.get("updated_at_ms") or reference_time_ms)))
            except (TypeError, ValueError):
                continue
        time_weighted_recall = {
            "enabled": True,
            "role": "ranking_prior_not_temporal_compression",
            "score_field": "time_score",
            "formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi",
            "freshness_tolerance_ms": freshness_tolerance_ms,
            "half_life_ms": half_life_ms,
            "selected_ref_count": len(selected),
            "avg_selected_time_score": round(sum(selected_time_scores) / len(selected_time_scores), 6) if selected_time_scores else 0.0,
            "min_selected_time_score": round(min(selected_time_scores), 6) if selected_time_scores else 0.0,
            "max_selected_age_ms": max(selected_age_ms) if selected_age_ms else 0,
            "recent_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms <= freshness_tolerance_ms),
            "older_selected_ref_count": sum(1 for age_ms in selected_age_ms if age_ms > freshness_tolerance_ms),
        }
        pack = {
            "context_pack_id": str(context_pack_id),
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "local_context_refs": local_context_refs_for_pack(local_budget),
            "selected_refs": serving_selected,
            "remote_context_refs": serving_selected,
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": {
                "access_scope_before_scoring": True,
                "skill_selection": "skill_section_only",
                "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
                "recall_reinforcement": "selected event refs and compression source ids receive protection markers before raw-event pruning",
            },
            "layer_scores": layer_scores[:24],
            "question_type": question_type,
            "packing_policy": f"question_type_aware:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "query_plan": query_plan,
                "session_continuity": {
                    "mode": retrieval_session_scope,
                    "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                    "same_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "same_session"),
                    "cross_session_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session"),
                    "entity_bridge_selected_ref_count": sum(1 for item in selected if item.get("session_continuity") == "cross_session" and item.get("ref_type") == "entity"),
                },
                "cross_session": dropped_over_budget.get("cross_session_policy", cross_session_policy),
                "shared_context": dropped_over_budget.get("shared_context_policy", shared_context_policy),
                "backend_retrieval_pushdown": retrieval_scan_stats,
                "ranking": {
                    "min_similarity_score": min_similarity_score,
                    "max_global_candidates": max_global_candidates,
                    "max_selected_refs": max_selected_refs,
                    "budget_fill_policy": budget_fill_policy,
                    "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first",
                },
                "tree_traversal": {
                    "enabled": True,
                    "summary_embeddings": ["node_l0", "node_l1"],
                    "top_k_per_layer": top_k_per_layer,
                    "max_children_scored_per_parent": max_children_scored_per_parent,
                    "hard_max_children_scored_per_parent": hard_max_children_scored_per_parent,
                    "children_scoring_policy": "score_all_children_up_to_hard_cap_then_split_node_layers",
                    "max_candidates_per_node": max_candidates_per_node,
                    "max_raw_events_per_node": max_raw_events_per_node,
                    "max_selected_refs": max_selected_refs,
                    "selected_node_count": len(selected_node_hashes),
                    "selected_path_count": len(selected_paths),
                    "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                    "candidate_records_after_tree": len(tree_candidate_records),
                    "records_dropped_by_tree": tree_prefilter_dropped_count,
                    "records_dropped_by_node_fanout": fanout_dropped_count,
                    "raw_events_dropped_by_time_window": raw_event_time_window_dropped_count,
                    "cold_events_represented_by_compression": raw_event_time_window_dropped_count > 0,
                    "leaf_record_fetch_policy": "events/entities/resources/skills/compressions scanned only inside selected L0/L1 folders",
                    "fallback_to_flat": bool(traversal.get("fallback_to_flat")),
                    "fallback_reason": "missing_or_stale_summary_embeddings" if traversal.get("fallback_to_flat") else "",
                },
                "secondary_index_filter": {
                    "enabled": bool(secondary_index_filter_groups),
                    "required_groups": [sorted(group) for group in secondary_index_filter_groups],
                    "matched_candidate_count": secondary_index_matched_count,
                    "dropped_candidate_count": secondary_index_dropped_count,
                    "mode": "ANY group for multi-intent raw query, otherwise AND across groups; OR within each group",
                    "effective_mode": secondary_index_filter_mode,
                    "applied_before_embedding_scoring": True,
                    "fanout_cap_applied_before_embedding_scoring": True,
                },
                "rerank": rerank_policy,
                "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
                "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
                "time_decay": {
                    "freshness_tolerance_ms": freshness_tolerance_ms,
                    "half_life_ms": half_life_ms,
                },
                "time_weighted_recall": time_weighted_recall,
                "recall_reinforcement": reinforcement,
                "weights": {
                    "time": optional_object(ranking, "weights").get("time", DEFAULT_TIME_WEIGHT),
                    "business": optional_object(ranking, "weights").get("business", DEFAULT_BUSINESS_WEIGHT),
                },
                "auxiliary_quota": auxiliary_quota,
                "storage_options": storage_options,
                "hard_deadline": {
                    "deadline_ms": deadline_ms,
                    "elapsed_ms": round((time.perf_counter() - started_perf) * 1000.0, 3),
                    "partial_context_pack": partial_context_pack,
                    "fallback_reason": dropped_over_budget.get("deadline_reason", "") if partial_context_pack else "",
                },
            },
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "used_context_tokens": used_context_tokens,
            "used_remote_context_tokens": used_context_tokens,
            "used_local_context_tokens": local_tokens,
            "total_prompt_context_tokens": used_context_tokens + local_tokens,
            "remote_context_budget_tokens": remote_context_budget_tokens,
            "requested_max_context_tokens": max_context_tokens,
            "local_context_safety_margin_tokens": safety_margin_tokens,
            "budget_source": budget_source,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_tokens,
                "local_context_token_source": local_budget.get("token_source", "estimated_from_local_context"),
                "safety_margin_tokens": safety_margin_tokens,
                "safety_margin_source": local_budget.get("safety_margin_source", "matrixark_default_5_percent_capped"),
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": serving_dropped,
            "quality_warnings": quality_warnings,
            "insufficient_context": not selected,
            "partial_context_pack": partial_context_pack,
            "context_pack_payload_policy": {
                "serving_refs": "compact" if not debug_refs else "debug_full",
                "hashes_and_matched_indexes": "audit_only" if not debug_refs else "included",
                "dropped_ref_details": "audit_only" if not debug_refs else "included",
                "enable_debug_refs_with": "include_debug_refs=true or MATRIXARK_CONTEXT_PACK_DEBUG_REFS=1",
            },
            "operational_visibility_policy": {
                "audit_mode": audit_mode,
                "audit_sample_rate": audit_sample_rate,
                "telemetry_record": audit_mode != "off",
                "rich_replay_audit": audit_mode == "full" and audit_sample_rate > 0,
                "rich_replay_audit_force_on_partial_or_warning": True,
            },
        }
        finish_retrieval_stage("pack", stage_started_perf)
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages:
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        audit_started_perf = time.perf_counter()
        audit_record = {
            "record_type": "context_pack_audit",
            "context_pack_id": context_pack_id_text,
            "query": query,
            "scope": scope,
            "summary_text": pack_summary,
            "selected_refs": compact_refs_for_audit(selected),
            "local_context_refs": compact_local_context_refs(local_budget),
            "context_sources_order": pack["context_sources_order"],
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": pack["context_assembly_policy"],
            "dropped_refs": dropped_over_budget,
            "quality_warnings": quality_warnings,
            "partial_context_pack": partial_context_pack,
            "layer_scores": layer_scores[:24],
            "tree_traversal": pack["recall_policy"]["tree_traversal"],
            "secondary_index_filter": pack["recall_policy"]["secondary_index_filter"],
            "question_type": question_type,
            "packing_policy": pack["packing_policy"],
            "rerank_policy": rerank_policy,
            "recall_policy": pack["recall_policy"],
            "stage_latency_budgets": pack["recall_policy"]["stage_latency_budgets"],
            "storage_options": storage_options,
            "local_context_policy": pack["local_context_policy"],
            "used_local_context_tokens": pack["used_local_context_tokens"],
            "used_remote_context_tokens": pack["used_remote_context_tokens"],
            "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
            "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
            "requested_max_context_tokens": pack["requested_max_context_tokens"],
            "local_context_safety_margin_tokens": pack["local_context_safety_margin_tokens"],
            "budget_source": pack["budget_source"],
            "operational_visibility_policy": pack["operational_visibility_policy"],
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "tree_candidate_records": len(tree_candidate_records),
            "tree_prefilter_dropped_count": tree_prefilter_dropped_count,
            "fanout_dropped_count": fanout_dropped_count,
            "max_candidates_per_node": max_candidates_per_node,
            "max_selected_refs": max_selected_refs,
            "created_at_ms": now_ms(),
        }
        visibility_decision = self.append_context_pack_visibility(
            pack=pack,
            audit_record=audit_record,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
            audit_sample_rate=audit_sample_rate,
        )
        pack["operational_visibility_policy"] = visibility_decision
        if pack_cache_enabled and not pack.get("partial_context_pack"):
            cached_pack = json.loads(json.dumps(pack))
            cached_recall = cached_pack.get("recall_policy") if isinstance(cached_pack.get("recall_policy"), dict) else {}
            cached_recall["context_pack_cache"] = {"hit": False, "ttl_s": self._context_pack_cache_ttl_s}
            cached_pack["recall_policy"] = cached_recall
            with self._context_pack_cache_lock:
                if len(self._context_pack_cache) >= self._context_pack_cache_max_entries:
                    oldest_key = next(iter(self._context_pack_cache))
                    self._context_pack_cache.pop(oldest_key, None)
                self._context_pack_cache[pack_cache_key] = (time.monotonic(), cached_pack)
        finish_retrieval_stage("audit", audit_started_perf)
        placement = retrieval_scan_stats.get("native_selected_node_locations", {}) if isinstance(retrieval_scan_stats, dict) else {}
        candidate_cache_hit = bool(
            isinstance(retrieval_scan_stats, dict)
            and (
                retrieval_scan_stats.get("cache_hit")
                or retrieval_scan_stats.get("candidate_cache_hit")
                or retrieval_scan_stats.get("native_placement_candidate_cache_hit")
            )
        )
        index_postings_read = (
            int(retrieval_scan_stats.get("index_postings_read") or 0)
            if isinstance(retrieval_scan_stats, dict)
            else 0
        )
        if isinstance(retrieval_scan_stats, dict) and not index_postings_read:
            index_postings_read = int(
                retrieval_scan_stats.get("index_postings_touched")
                or retrieval_scan_stats.get("native_index_postings_found")
                or 0
            )
        pack["retrieval_metrics"] = {
            "query_plan_ms": round(float(stage_latencies_ms.get("query_understanding", 0.0)), 3),
            "node_traversal_ms": round(float(stage_latencies_ms.get("node_traversal", 0.0)), 3),
            "index_prefilter_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
            "candidate_fetch_ms": round(float(stage_latencies_ms.get("candidate_fetch", 0.0)), 3),
            "score_ms": round(float(stage_latencies_ms.get("rerank_score", 0.0)), 3),
            "pack_ms": round(float(stage_latencies_ms.get("pack", 0.0)), 3),
            "audit_ms": round(float(stage_latencies_ms.get("audit", 0.0)), 3),
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": len(selected),
            "dropped_refs": int(len(dropped_over_budget)),
            "scanned_records": int(retrieval_scan_stats.get("loaded_records") or retrieval_scan_stats.get("scanned_records") or len(records)) if isinstance(retrieval_scan_stats, dict) else len(records),
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "index_postings_read": index_postings_read,
            "index_postings_touched": index_postings_read,
            "placement_partitions_touched": len(placement.get("locations", []) or []) if isinstance(placement, dict) else 0,
            "native_pack_assembly": False,
            "python_pack_fallback": True,
            "raw_candidate_tables_returned": False,
            "source": "python_reference_pack",
        }
        if bool(args.get("include_retrieval_metrics")):
            pack["include_retrieval_metrics"] = True
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages and not any(str(warning).startswith("stage_budget_exceeded:") for warning in quality_warnings):
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
            return pack
        return compact_context_pack_for_serving(pack)

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        if not (ENABLE_CONTEXT_REPLAY or bool(args.get("enable_replay"))):
            raise MatrixArkError("context replay is disabled; set MATRIXARK_ENABLE_REPLAY=1 or pass enable_replay=true for explicit debug runs")
        context_pack_id = require_string(args, "context_pack_id")
        include_debug = bool(args.get("include_debug_records") or args.get("include_debug_refs") or CONTEXT_PACK_DEBUG_REFS or AUDIT_DEBUG_PAYLOAD)
        self.flush_audits()
        records = self.read_all()
        if include_debug:
            return {
                "context_pack_id": context_pack_id,
                "events": records,
                "replay_payload_policy": "debug_full_store_scan",
            }
        replay_records: list[Json] = []
        for record in records:
            if str(record.get("context_pack_id") or "") != context_pack_id:
                continue
            record_type = str(record.get("record_type") or "")
            if record_type == "context_pack_audit":
                replay_records.append(compact_context_pack_audit_record(record))
            elif record_type == "context_pack_telemetry":
                replay_records.append(
                    {
                        key: record.get(key)
                        for key in [
                            "record_type",
                            "context_pack_id",
                            "query_hash",
                            "question_type",
                            "selected_ref_count",
                            "selected_ref_counts",
                            "dropped_ref_count",
                            "dropped_ref_bucket_counts",
                            "used_local_context_tokens",
                            "used_remote_context_tokens",
                            "total_prompt_context_tokens",
                            "remote_context_budget_tokens",
                            "partial_context_pack",
                            "insufficient_context",
                            "quality_warning_count",
                            "primary_candidate_count",
                            "auxiliary_candidate_count",
                            "created_at_ms",
                        ]
                        if record.get(key) not in (None, "", [], {})
                    }
                )
            else:
                replay_records.append(
                    {
                        key: record.get(key)
                        for key in ["record_type", "context_pack_id", "source_ref_type", "source_ref_hash", "event_id_hash", "node_hash", "reinforced_at_ms", "protected_until_ms", "reason"]
                        if record.get(key) not in (None, "", [], {})
                    }
                )
        return {
            "context_pack_id": context_pack_id,
            "events": replay_records,
            "replay_payload_policy": "compact_context_pack_scope",
            "debug_records_available_with": "include_debug_records=true",
        }
