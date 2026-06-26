#!/usr/bin/env python3
"""MatrixArk MCP server entrypoint.

The implementation is split into focused modules:
- matrixark_mcp_core: shared primitives, extraction, scoring, traversal helpers
- matrixark_access: account/tenant/user/API-key metadata and governance
- matrixark_mcp_schemas: MCP tool schema catalog
- matrixark_http: management portal HTTP facade

This file keeps the storage adapters, MCP dispatch loop, and process entrypoint
so operational behavior remains stable for existing scripts.
"""

from __future__ import annotations

from contextlib import contextmanager
import queue as thread_queue

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _mcp_debug_log,
    )
    from tools.matrixark_access import MatrixArkAccessManager
    from tools.matrixark_http import make_matrixark_http_handler
    from tools.matrixark_mcp_schemas import TOOLS
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import (
        _DIRECT_RECORD_CACHE,
        _DIRECT_RECORD_CACHE_LOCK,
        _DIRECT_RECORD_CACHE_MAX_PREFIXES,
        _DIRECT_RECORD_LOAD_LOCKS,
        _mcp_debug_log,
    )
    from matrixark_access import MatrixArkAccessManager
    from matrixark_http import make_matrixark_http_handler
    from matrixark_mcp_schemas import TOOLS


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


class MatrixArkServiceMetrics:
    """In-process Prometheus metrics for MatrixArk MCP pipeline work."""

    LATENCY_BUCKETS_MS = (25, 50, 100, 250, 500, 1000, 2000, 5000, 10000, float("inf"))

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._started_at = time.time()
        self._ops: dict[str, Json] = {}
        self._model: dict[str, Json] = {}
        self._timeout_count = 0
        self._backpressure_count = 0
        self._partial_context_pack_count = 0
        self._token_pressure_samples: list[float] = []
        self._last_token_pressure = 0.0
        self._last_backend_ready = 0
        self._last_backend_ready_status = "unknown"
        self._last_resource_queue_depth = 0
        self._last_resource_import_lag_ms = 0
        self._last_dirty_summary_lag_ms = 0
        self._last_audit_write_failures = 0

    def observe_operation(self, operation: str, status: str, elapsed_ms: float, *, timeout: bool = False) -> None:
        with self._lock:
            row = self._ops.setdefault(
                operation,
                {"ok": 0, "error": 0, "latencies": [], "buckets": [0 for _ in self.LATENCY_BUCKETS_MS]},
            )
            row["ok" if status == "ok" else "error"] += 1
            samples = row["latencies"]
            samples.append(float(elapsed_ms))
            if len(samples) > 4096:
                del samples[: len(samples) - 4096]
            for index, bucket in enumerate(self.LATENCY_BUCKETS_MS):
                if elapsed_ms <= bucket:
                    row["buckets"][index] += 1
            if timeout:
                self._timeout_count += 1

    def observe_backpressure(self, operation: str) -> None:
        with self._lock:
            self._backpressure_count += 1
            row = self._ops.setdefault(
                operation,
                {"ok": 0, "error": 0, "latencies": [], "buckets": [0 for _ in self.LATENCY_BUCKETS_MS]},
            )
            row["error"] += 1

    def observe_model_latency(self, stage: str, elapsed_ms: float) -> None:
        with self._lock:
            row = self._model.setdefault(stage, {"count": 0, "latencies": [], "buckets": [0 for _ in self.LATENCY_BUCKETS_MS]})
            row["count"] += 1
            samples = row["latencies"]
            samples.append(float(elapsed_ms))
            if len(samples) > 4096:
                del samples[: len(samples) - 4096]
            for index, bucket in enumerate(self.LATENCY_BUCKETS_MS):
                if elapsed_ms <= bucket:
                    row["buckets"][index] += 1

    def observe_retrieve_result(self, result: Json) -> None:
        with self._lock:
            if result.get("partial_context_pack"):
                self._partial_context_pack_count += 1
            budget = int(result.get("remote_context_budget_tokens") or result.get("max_context_tokens") or 0)
            used = int(result.get("used_remote_context_tokens") or result.get("used_context_tokens") or 0)
            pressure = min(1.0, used / budget) if budget > 0 else 0.0
            self._last_token_pressure = pressure
            self._token_pressure_samples.append(pressure)
            if len(self._token_pressure_samples) > 4096:
                del self._token_pressure_samples[: len(self._token_pressure_samples) - 4096]

    def observe_ingest_result(self, result: Json) -> None:
        task = result.get("resource_import_task") if isinstance(result, dict) else {}
        if isinstance(task, dict) and task.get("metrics"):
            metrics = task.get("metrics") or {}
            try:
                self.observe_model_latency("resource_import", float(metrics.get("duration_ms") or 0.0))
            except (TypeError, ValueError):
                pass

    def observe_resource_queue_depth(self, depth: int) -> None:
        with self._lock:
            self._last_resource_queue_depth = max(0, int(depth))

    def observe_backend_ready(self, ready: bool, status: str = "") -> None:
        with self._lock:
            self._last_backend_ready = 1 if ready else 0
            self._last_backend_ready_status = status or ("ready" if ready else "not_ready")

    def update_gauges(self, *, dirty_summary_lag_ms: int, resource_import_lag_ms: int, queue_depth: int, audit_write_failures: int) -> None:
        with self._lock:
            self._last_dirty_summary_lag_ms = max(0, int(dirty_summary_lag_ms))
            self._last_resource_import_lag_ms = max(0, int(resource_import_lag_ms))
            self._last_resource_queue_depth = max(0, int(queue_depth))
            self._last_audit_write_failures = max(0, int(audit_write_failures))

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
        return round(float(ordered[index]), 3)

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def snapshot(self) -> Json:
        with self._lock:
            return {
                "started_at": self._started_at,
                "ops": json.loads(json.dumps(self._ops)),
                "model": json.loads(json.dumps(self._model)),
                "timeout_count": self._timeout_count,
                "backpressure_count": self._backpressure_count,
                "partial_context_pack_count": self._partial_context_pack_count,
                "last_token_pressure": round(self._last_token_pressure, 6),
                "avg_token_pressure": round(sum(self._token_pressure_samples) / len(self._token_pressure_samples), 6)
                if self._token_pressure_samples
                else 0.0,
                "backend_ready": self._last_backend_ready,
                "backend_ready_status": self._last_backend_ready_status,
                "resource_import_queue_depth": self._last_resource_queue_depth,
                "resource_import_lag_ms": self._last_resource_import_lag_ms,
                "dirty_summary_lag_ms": self._last_dirty_summary_lag_ms,
                "audit_write_failures": self._last_audit_write_failures,
            }

    def render_prometheus(self, *, backend: str, storage_mode: str) -> str:
        snap = self.snapshot()
        backend_label = self._escape(backend)
        storage_label = self._escape(storage_mode)
        base_labels = f'backend="{backend_label}",storage_mode="{storage_label}"'
        elapsed_s = max(0.001, time.time() - float(snap["started_at"]))
        lines = [
            "# HELP matrixark_backend_info MatrixArk backend identity and storage mode.",
            "# TYPE matrixark_backend_info gauge",
            f"matrixark_backend_info{{{base_labels}}} 1",
            "# HELP matrixark_backend_ready MatrixArk backend readiness state, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{{base_labels},status="{self._escape(str(snap["backend_ready_status"]))}"}} {snap["backend_ready"]}',
            "# HELP matrixark_service_requests_total MatrixArk MCP service requests by operation and status.",
            "# TYPE matrixark_service_requests_total counter",
            "# HELP matrixark_service_qps MatrixArk MCP service request QPS by operation.",
            "# TYPE matrixark_service_qps gauge",
            "# HELP matrixark_service_latency_ms MatrixArk MCP service latency quantiles by operation.",
            "# TYPE matrixark_service_latency_ms gauge",
            "# HELP matrixark_service_latency_ms_bucket MatrixArk MCP service latency histogram buckets by operation.",
            "# TYPE matrixark_service_latency_ms_bucket counter",
        ]
        for operation, row in sorted(snap["ops"].items()):
            op_label = self._escape(operation)
            total = int(row.get("ok", 0)) + int(row.get("error", 0))
            for status in ("ok", "error"):
                lines.append(
                    f'matrixark_service_requests_total{{{base_labels},operation="{op_label}",status="{status}"}} {int(row.get(status, 0))}'
                )
            lines.append(f'matrixark_service_qps{{{base_labels},operation="{op_label}"}} {round(total / elapsed_s, 6)}')
            samples = [float(value) for value in row.get("latencies", [])]
            for quantile, percentile in (("0.5", 0.50), ("0.95", 0.95), ("0.99", 0.99)):
                lines.append(
                    f'matrixark_service_latency_ms{{{base_labels},operation="{op_label}",quantile="{quantile}"}} {self._percentile(samples, percentile)}'
                )
            for bucket, count in zip(self.LATENCY_BUCKETS_MS, row.get("buckets", [])):
                le = "+Inf" if bucket == float("inf") else str(int(bucket))
                lines.append(f'matrixark_service_latency_ms_bucket{{{base_labels},operation="{op_label}",le="{le}"}} {int(count)}')

        lines.extend(
            [
                "# HELP matrixark_timeouts_total MatrixArk MCP timeout count.",
                "# TYPE matrixark_timeouts_total counter",
                f"matrixark_timeouts_total{{{base_labels}}} {int(snap['timeout_count'])}",
                "# HELP matrixark_backpressure_rejections_total MatrixArk MCP service backpressure rejection count.",
                "# TYPE matrixark_backpressure_rejections_total counter",
                f"matrixark_backpressure_rejections_total{{{base_labels}}} {int(snap['backpressure_count'])}",
                "# HELP matrixark_partial_context_pack_total MatrixArk partial ContextPack count.",
                "# TYPE matrixark_partial_context_pack_total counter",
                f"matrixark_partial_context_pack_total{{{base_labels}}} {int(snap['partial_context_pack_count'])}",
                "# HELP matrixark_token_pressure_ratio Remote context budget pressure.",
                "# TYPE matrixark_token_pressure_ratio gauge",
                f"matrixark_token_pressure_ratio{{{base_labels},window=\"last\"}} {snap['last_token_pressure']}",
                f"matrixark_token_pressure_ratio{{{base_labels},window=\"avg\"}} {snap['avg_token_pressure']}",
                "# HELP matrixark_dirty_summary_lag_ms Oldest pending dirty summary lag in milliseconds.",
                "# TYPE matrixark_dirty_summary_lag_ms gauge",
                f"matrixark_dirty_summary_lag_ms{{{base_labels}}} {int(snap['dirty_summary_lag_ms'])}",
                "# HELP matrixark_resource_import_lag_ms Oldest queued/running resource import lag in milliseconds.",
                "# TYPE matrixark_resource_import_lag_ms gauge",
                f"matrixark_resource_import_lag_ms{{{base_labels}}} {int(snap['resource_import_lag_ms'])}",
                "# HELP matrixark_resource_import_queue_depth Current MatrixArk resource import queue depth.",
                "# TYPE matrixark_resource_import_queue_depth gauge",
                f"matrixark_resource_import_queue_depth{{{base_labels}}} {int(snap['resource_import_queue_depth'])}",
                "# HELP matrixark_audit_write_failures_total MatrixArk audit write flush failure count.",
                "# TYPE matrixark_audit_write_failures_total counter",
                f"matrixark_audit_write_failures_total{{{base_labels}}} {int(snap['audit_write_failures'])}",
                "# HELP matrixark_model_latency_ms MatrixArk parser/model latency quantiles by stage.",
                "# TYPE matrixark_model_latency_ms gauge",
                "# HELP matrixark_model_latency_ms_bucket MatrixArk parser/model latency buckets by stage.",
                "# TYPE matrixark_model_latency_ms_bucket counter",
            ]
        )
        for stage, row in sorted(snap["model"].items()):
            stage_label = self._escape(stage)
            samples = [float(value) for value in row.get("latencies", [])]
            for quantile, percentile in (("0.5", 0.50), ("0.95", 0.95), ("0.99", 0.99)):
                lines.append(
                    f'matrixark_model_latency_ms{{{base_labels},stage="{stage_label}",quantile="{quantile}"}} {self._percentile(samples, percentile)}'
                )
            for bucket, count in zip(self.LATENCY_BUCKETS_MS, row.get("buckets", [])):
                le = "+Inf" if bucket == float("inf") else str(int(bucket))
                lines.append(f'matrixark_model_latency_ms_bucket{{{base_labels},stage="{stage_label}",le="{le}"}} {int(count)}')
        return "\n".join(lines) + "\n"


@dataclass
class MatrixArkLocalAdapter:
    event_log: Path

    def __post_init__(self) -> None:
        self.event_log.parent.mkdir(parents=True, exist_ok=True)
        self._write_batch_local = threading.local()
        self._resource_import_worker_count = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_WORKERS", "2")))
        self._resource_import_queue_max = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX", "64")))
        self._resource_import_queue: thread_queue.Queue[Json] = thread_queue.Queue(maxsize=self._resource_import_queue_max)
        self._resource_import_workers_started = False
        self._resource_import_worker_lock = threading.RLock()

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

    def append(self, record: Json) -> None:
        records = materialize_serving_records(record)
        if self._queue_batched_records(records):
            return
        with self.event_log.open("a", encoding="utf-8") as handle:
            for item in records:
                handle.write(json.dumps(item, sort_keys=True) + "\n")

    def append_many(self, records: list[Json]) -> None:
        records = materialize_serving_record_batch(records)
        if not records:
            return
        if self._queue_batched_records(records):
            return
        with self.event_log.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, sort_keys=True) + "\n")

    def append_audit(self, record: Json) -> None:
        self.append(record)

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

    def read_all(self) -> list[Json]:
        if not self.event_log.exists():
            return []
        records = []
        with self.event_log.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def retrieval_records(
        self,
        *,
        scope: Json,
        record_types: set[str] | None = None,
        secondary_index_groups: list[set[str]] | None = None,
        selected_node_hashes: set[int] | None = None,
    ) -> Json:
        """Return records eligible for retrieval hot-path scan/filter/pack.

        C++/Rust backends override this seam with native prefix scans and
        secondary-index prefiltering. The local adapter keeps the reference
        behavior by filtering the JSONL record log before Python scoring.
        """

        allowed_types = record_types or RETRIEVAL_HOT_RECORD_TYPES
        raw_records = self.read_all()
        filtered: list[Json] = []
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
                if not scope_matches(candidate_access_scope(record), scope):
                    dropped_scope += 1
                    continue
            elif not access_scope_matches_before_scoring(record, scope):
                dropped_scope += 1
                continue
            filtered.append(record)
        return {
            "records": filtered,
            "scan_stats": {
                "backend": getattr(self, "_backend_label", lambda: "local")(),
                "execution_mode": "adapter_prefilter",
                "native_pushdown": False,
                "record_types": sorted(allowed_types),
                "scanned_records": scanned,
                "returned_records": len(filtered),
                "dropped_by_type": dropped_type,
                "dropped_by_scope": dropped_scope,
                "secondary_index_groups_supplied": len(secondary_index_groups or []),
                "selected_node_hashes_supplied": len(selected_node_hashes or set()),
            },
        }

    def find_latest_entity(self, *, node_hash: int, entity_type: str, entity_name: str) -> Json | None:
        entity_hash = stable_hash(f"{node_hash}:{entity_type}:{entity_name}")
        for record in reversed(self.read_all()):
            if record.get("record_type") == "context_entity" and record.get("entity_hash") == entity_hash:
                return record
        return None

    def pending_session_events(self, scope: Json, *, limit: int | None = None) -> list[Json]:
        key = session_buffer_key_from_scope(scope)
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

    def ensure_context_node_path(self, *, node_path: list[str], scope: Json, updated_at_ms: int) -> Json:
        prefixes = node_prefixes(node_path)
        if not prefixes:
            return {"nodes_created": 0, "child_refs_created": 0, "node_hashes": []}

        records = self.read_all()
        existing_nodes = {
            int(record.get("node_hash"))
            for record in records
            if record.get("record_type") == "context_node" and record.get("node_hash") is not None
        }
        existing_child_refs = {
            int(record.get("child_ref_hash"))
            for record in records
            if record.get("record_type") == "context_child_ref" and record.get("child_ref_hash") is not None
        }
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
                        "status": "active",
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
                            "status": "active",
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
                "created_at_ms": now_ms(),
            }
        )
        return {
            **batch_result,
            "status": "committed",
            "commit_id_hash": commit_id_hash,
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
        max_events: int = 8,
        max_child_summaries: int = 8,
    ) -> tuple[list[Json], list[Json]]:
        prefix = node_path_tuple(node_path)
        child_summaries: list[Json] = []
        events: list[Json] = []
        seen_summary_keys: set[tuple[int, str]] = set()
        for record in reversed(records):
            if not scope_matches(record.get("scope", record.get("envelope", {}).get("scope", {})), scope):
                continue
            record_path = node_path_tuple(record.get("node_path", []))
            if not record_path or not starts_with_path(record_path, prefix):
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
        prefixes = node_prefixes(node_path)
        if propagate_depth is not None and propagate_depth >= 0:
            prefixes = prefixes[max(0, len(prefixes) - propagate_depth - 1) :]
        dirty_hashes: list[int] = []
        for depth, prefix in enumerate(prefixes, start=1):
            node_hash = stable_hash("/".join(prefix))
            dirty_hash = stable_hash(
                f"summary_dirty:{node_hash}:{dirty_reason}:{source_ref_type}:{source_hash}:{updated_at_ms}"
            )
            dirty_hashes.append(dirty_hash)
            self.append(
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
        return dirty_hashes

    def refresh_dirty_node_summaries(
        self,
        *,
        scope: Json,
        limit: int = 64,
        refreshed_at_ms: int | None = None,
    ) -> Json:
        refreshed_at_ms = refreshed_at_ms or now_ms()
        records = self.read_all()
        completed_dirty_hashes = {
            int(record.get("dirty_hash"))
            for record in records
            if record.get("record_type") == "context_summary_refresh_audit"
            and record.get("status") == "refreshed"
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
                self.append(
                    {
                        "record_type": "context_summary",
                        "summary_type": level,
                        "summary_version_hash": version_hash,
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
                        "ref_type": "node",
                        "ref_hash": node_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "depth": len(node_path),
                        "dim": len(embedding_for_text(summary_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(summary_text),
                        "summary_version_hash": version_hash,
                        "summary_generation_policy": l1_policy,
                        "dirty_hash": dirty.get("dirty_hash"),
                        "scope": dirty.get("scope", scope),
                        "updated_at_ms": refreshed_at_ms,
                    }
                )
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
                }
            )
        return {
            "status": "ok",
            "refreshed_count": len(refreshed),
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
        return self.refresh_dirty_node_summaries(scope=scope, limit=limit, refreshed_at_ms=refreshed_at_ms)

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
        for record in records:
            if record.get("record_type") != "context_event":
                continue
            if not scope_matches(self._dashboard_record_scope(record), scope):
                continue
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
            kind = str(envelope.get("kind") or "message")
            if kind not in {"message", "feedback", "business_data"}:
                continue
            messages = envelope.get("messages", []) if isinstance(envelope.get("messages"), list) else []
            for message in messages or [{"role": "unknown", "content": record.get("text", "")}]:
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
                        "classification": record.get("internal_extraction", {}).get("classification", ""),
                        "event_type": record.get("internal_extraction", {}).get("event_type", ""),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": envelope.get("scope", record.get("scope", {})),
                        "agent_name": envelope.get("scope", {}).get("agent_name", ""),
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
                        "source_ref": record.get("source_ref", ""),
                        "unit_kind": record.get("unit_kind", record.get("metadata", {}).get("unit_kind", "")),
                        "token_estimate": record.get("token_estimate", 0),
                        "chunk_count": record.get("chunk_count", 0),
                        "parse_warnings": record.get("parse_warnings", []),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record.get("scope", {}),
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
                        "scope": record.get("scope", {}),
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
                        "classification": record.get("internal_extraction", {}).get("classification", ""),
                        "event_type": record.get("event_type", record.get("internal_extraction", {}).get("event_type", "")),
                        "source_chunk_hash": record.get("source_chunk_hash", 0),
                        "source_ref": record.get("source_ref", ""),
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
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": record.get("scope", {}),
                        "updated_at_ms": record.get("updated_at_ms", 0),
                    }
                )
            elif table == "context_packs" and record_type == "context_pack_audit":
                rows.append(
                    {
                        "row_type": record_type,
                        "context_pack_id": record.get("context_pack_id", ""),
                        "query": record.get("query", ""),
                        "used_context_tokens": record.get("used_context_tokens", 0),
                        "selected_ref_count": len(record.get("selected_refs", [])),
                        "dropped_ref_count": len(record.get("dropped_refs", [])),
                        "quality_warnings": record.get("quality_warnings", []),
                        "scope": record.get("scope", {}),
                        "created_at_ms": record.get("created_at_ms", 0),
                    }
                )
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
                "access_scope": record.get("access_scope", registry_access_scope(record.get("scope", {}))),
                "deployment_scope": record.get("deployment_scope", "local"),
                "import_task_hash": record.get("import_task_hash", 0),
                "token_estimate": record.get("token_estimate", 0),
                "node_hash": record.get("node_hash", 0),
                "node_path": record.get("node_path", []),
                "scope": record.get("scope", {}),
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
                "access_scope": record.get("access_scope", registry_access_scope(record.get("scope", {}))),
                "deployment_scope": record.get("deployment_scope", "local"),
                "node_hash": record.get("node_hash", 0),
                "node_path": record.get("node_path", []),
                "scope": record.get("scope", {}),
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
            for worker_index in range(self._resource_import_worker_count):
                thread = threading.Thread(
                    target=self._resource_import_worker_loop,
                    name=f"matrixark-resource-import-{worker_index}",
                    daemon=True,
                )
                thread.start()
            self._resource_import_workers_started = True

    def _resource_import_worker_loop(self) -> None:
        while True:
            item = self._resource_import_queue.get()
            try:
                args = item.get("args", {})
                hook = item.get("hook")
                self._run_background_resource_import(args, hook if isinstance(hook, dict) else None)
            finally:
                self._resource_import_queue.task_done()

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
            node_hint = metadata.get("node_path") or self.default_session_node_path(scope)
            node_path = [str(part) for part in node_hint if str(part)]
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
            deployment_scope = deployment_scope_from_args(args, envelope)
            access_scope = registry_access_scope(envelope["scope"])
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
                        "scope": envelope["scope"],
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
                        "scope": envelope["scope"],
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
                        "scope": envelope["scope"],
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
                    "scope": envelope["scope"],
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
                            "scope": envelope["scope"],
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
                                "scope": envelope["scope"],
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
                            "scope": envelope["scope"],
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
                            "scope": envelope["scope"],
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
                        "scope": envelope["scope"],
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
                    "scope": envelope["scope"],
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
                    "scope": envelope["scope"],
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
            resource_indexes = ordered_unique(
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
                        "scope": envelope["scope"],
                        "storage_options": envelope.get("storage_options", {}),
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            if envelope["kind"] == "resource":
                manifest_hash = stable_hash(f"resource_manifest:{raw_uri}:{node_hash}")
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
                        "scope": envelope["scope"],
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
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            for chunk, vector in zip(parsed_chunks, chunk_vectors):
                resource_chunk_hashes.append(chunk.chunk_hash)
                chunk_metadata = serving_resource_metadata(chunk.metadata)
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
                            "source_ref": chunk.source_ref,
                            "heading": chunk_metadata.get("heading", ""),
                            "text": chunk.text,
                            "token_estimate": chunk.token_estimate,
                            "metadata": chunk_metadata,
                            "access_scope": access_scope,
                            "deployment_scope": deployment_scope,
                            "scope": envelope["scope"],
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
                        "raw_uri": raw_uri,
                        "resource_type": chunk_metadata.get("resource_type") or resource_type,
                        "source_ref": chunk.source_ref,
                        "text": chunk.text,
                        "token_estimate": chunk.token_estimate,
                        "metadata": chunk_metadata,
                        "access_scope": access_scope,
                        "deployment_scope": deployment_scope,
                        "scope": envelope["scope"],
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
                            "raw_uri": raw_uri,
                            "source_ref": chunk.source_ref,
                            "metadata_debug": chunk_debug_metadata,
                            "text_preview": clip_context_text(chunk.text),
                            "scope": envelope["scope"],
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
                        "scope": envelope["scope"],
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
                            "scope": envelope["scope"],
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
                            "source_ref": chunk.source_ref,
                            "node_hash": node_hash,
                            "node_path": node_path,
                            "scope": envelope["scope"],
                            "updated_at_ms": envelope["ingestion_time_ms"],
                        }
                    )
            resource_fact_records: list[Json] = []
            fact_chunks = [chunk for chunk in parsed_chunks if skill_hash is None and should_extract_resource_fact(chunk.text, chunk.metadata)][:MAX_RESOURCE_FACT_CHUNKS]
            for chunk in fact_chunks:
                chunk_metadata = serving_resource_metadata(chunk.metadata)
                for fact_extraction in extract_resource_facts(
                    chunk,
                    chunk_metadata=chunk_metadata,
                    envelope=envelope,
                    raw_uri=raw_uri,
                    resource_version=resource_version_value,
                ):
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
                            "source_ref": chunk.source_ref,
                            "resource_version": resource_version_value,
                            "scope": envelope["scope"],
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
                            "scope": envelope["scope"],
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
                            "scope": envelope["scope"],
                            "entity_type": fact_entity_type,
                            "entity_name": entity_name,
                            "state": entity_state,
                            "confidence": fact_extraction.get("confidence", 0.78),
                            "operator": "LATEST",
                            "source_refs": [chunk.source_ref],
                            "source_event_ids": [fact_event_hash],
                            "source_chunk_hash": chunk.chunk_hash,
                            "source_ref": chunk.source_ref,
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
                            "scope": envelope["scope"],
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
                                "scope": envelope["scope"],
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
                    "index_cap_per_chunk": MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
                    "index_cap_per_fact": MAX_INDEX_TERMS_PER_RESOURCE_FACT,
                    "summary_dirty_hashes": resource_dirty_hashes,
                    "progress": {"stage": "completed", "percent": 100},
                    "metrics": resource_import_metrics,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": envelope["scope"],
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
                    "scope": envelope["scope"],
                    "created_at_ms": now_ms(),
                }
            )
        summary_text = summarize_text(text)
        embedding_started_perf = time.perf_counter()
        event_embedding = embedding_for_text(text)
        summary_embedding = embedding_for_text(" ".join(node_path + [summary_text]))
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
                        "scope": envelope["scope"],
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
                        "scope": envelope["scope"],
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
                    "scope": envelope["scope"],
                    "updated_at_ms": envelope["ingestion_time_ms"],
                }
            )
            record = {
                "record_type": "context_event",
                "event_id_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "text": text,
                "summary_text": summary_text,
                "summary_embedding": summary_embedding,
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
                    context_index_name("classification", extraction.get("classification")),
                    context_index_name("status", extraction.get("status") or "observed"),
                    context_index_name("source_type", envelope["kind"]),
                ]
            )
            for index_name in event_index_terms:
                self.append(
                    {
                        "record_type": "context_index",
                        "index_name": index_name,
                        "index_hash": stable_hash(f"{index_name}:{event_id_hash}"),
                        "ref_type": "event",
                        "ref_hash": event_id_hash,
                        "event_id_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": envelope["scope"],
                        "updated_at_ms": envelope["ingestion_time_ms"],
                    }
                )
            self.append_session_buffer_event(envelope=envelope, event_id_hash=event_id_hash, node_hash=node_hash, node_path=node_path, hook=hook)
            summary_refresh = self.append_node_summary_embeddings(
                node_path=node_path,
                source_text=text,
                scope=envelope["scope"],
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
                    "scope": envelope["scope"],
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
                },
                hook=hook,
            )
        return {
            "status": "accepted",
            "event_id_hash": event_id_hash,
            "node_hash": record["node_hash"],
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
        if not derive_from_existing_events:
            for index, message in enumerate(envelope["messages"]):
                event_text = f"{message['role']}: {message['content']}"
                event_id_hash = stable_hash(f"{batch_id_hash}:event:{index}:{event_text}")
                event_hashes.append(event_id_hash)
                records_to_append.append(
                    {
                        "record_type": "context_event",
                        "event_id_hash": event_id_hash,
                        "batch_id_hash": batch_id_hash,
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
                        "dim": len(embedding_for_text(event_text)),
                        "model": embedding_model_name(),
                        "vector": embedding_for_text(event_text),
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
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "entity_state",
                    "ref_type": "entity",
                    "ref_hash": entity_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(embedding_for_text(updated_entity["entity_type"] + " " + updated_entity["state"])),
                    "model": embedding_model_name(),
                    "vector": embedding_for_text(updated_entity["entity_type"] + " " + updated_entity["state"]),
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
            records_to_append.append(
                {
                    "record_type": "context_embedding",
                    "embedding_type": "segment_text",
                    "ref_type": "segment",
                    "ref_hash": segment_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "dim": len(embedding_for_text(segment["topic"] + " " + segment["summary_text"])),
                    "model": embedding_model_name(),
                    "vector": embedding_for_text(segment["topic"] + " " + segment["summary_text"]),
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
        records_to_append.append(
            {
                "record_type": "context_embedding",
                "embedding_type": "batch_l0",
                "ref_type": "summary",
                "ref_hash": summary_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(embedding_for_text(" ".join(node_path + [batch_summary]))),
                "model": embedding_model_name(),
                "vector": embedding_for_text(" ".join(node_path + [batch_summary])),
                "scope": envelope["scope"],
                "updated_at_ms": envelope["ingestion_time_ms"],
            }
        )
        for index_name in extraction["indexes"]:
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
                    "indexes": len(extraction["indexes"]),
                },
                "mode": extraction["mode"],
                "derive_from_existing_events": derive_from_existing_events,
                "source_event_ids": event_hashes,
                "agent_hook": hook,
                "created_at_ms": now_ms(),
            }
        )
        self.append_many(records_to_append)
        summary_refresh = self.append_node_summary_embeddings(
            node_path=node_path,
            source_text=batch_summary,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
            source_hash_field="source_batch_hash",
            source_hash=batch_id_hash,
        )
        return {
            "status": "accepted",
            "mode": extraction["mode"],
            "segment_provider": extraction.get("segment_provider", {}),
            "classification": extraction["classification"],
            "batch_id_hash": batch_id_hash,
            "node_hash": node_hash,
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
            "indexes_written": len(extraction["indexes"]),
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
        source_events = []
        for record in self.read_all():
            if record.get("record_type") != "context_event":
                continue
            if int(record.get("node_hash") or 0) != node_hash:
                continue
            event_scope = record.get("envelope", {}).get("scope", {})
            if not scope_matches(event_scope, scope):
                continue
            event_time = int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            if event_time < source_start_ms or event_time > source_end_ms:
                continue
            extraction = record.get("internal_extraction", {})
            confidence = float(extraction.get("confidence", record.get("confidence", 1.0)) or 1.0)
            importance = float(record.get("envelope", {}).get("metadata", {}).get("importance", record.get("importance", 1.0)) or 1.0)
            if confidence < min_confidence or importance < min_importance:
                continue
            source_events.append(record)
        source_events.sort(key=lambda record: int(record.get("envelope", {}).get("ingestion_time_ms") or 0))
        selected = source_events[:max_source_events]
        if not selected:
            raise MatrixArkError("no source events matched compression window")
        truncated = len(source_events) > len(selected)
        source_event_ids = [int(record["event_id_hash"]) for record in selected]
        compression_scope = selected[0].get("envelope", {}).get("scope", scope)
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
    ) -> Json:
        selected = []
        used_context_tokens = 0
        remote_budget = max(0, max_context_tokens - int(local_budget.get("token_estimate", 0)))
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
        pack = {
            "context_pack_id": context_pack_id,
            "selected_refs": selected,
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
            "used_local_context_tokens": local_budget["token_estimate"],
            "total_prompt_context_tokens": used_context_tokens + local_budget["token_estimate"],
            "remote_context_budget_tokens": remote_budget,
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_budget["token_estimate"],
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": [],
            "quality_warnings": [f"retrieval_deadline_exceeded:{reason}"],
            "insufficient_context": not selected,
            "partial_context_pack": True,
        }
        self.append_audit(
            {
                "record_type": "context_pack_audit",
                "context_pack_id": context_pack_id,
                "query": query,
                "scope": scope,
                "summary_text": summarize_text(" ".join(str(item.get("text", "")) for item in selected), limit=512),
                "selected_refs": compact_refs_for_audit(selected),
                "question_type": question_type,
                "packing_policy": pack["packing_policy"],
                "recall_policy": pack["recall_policy"],
                "local_context_policy": pack["local_context_policy"],
                "used_local_context_tokens": pack["used_local_context_tokens"],
                "used_remote_context_tokens": pack["used_remote_context_tokens"],
                "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
                "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
                "primary_candidate_count": 0,
                "auxiliary_candidate_count": 0,
                "created_at_ms": now_ms(),
            }
        )
        return pack

    def retrieve(self, args: Json) -> Json:
        started_perf = time.perf_counter()
        query = require_string(args, "query")
        scope = optional_object(args, "scope")
        storage_options = normalize_storage_options(args)
        ranking = optional_object(args, "ranking")
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
        secondary_index_filter_groups = infer_secondary_index_filter_groups(query, question_type)
        secondary_index_filter_mode = "any_group" if len(secondary_index_filter_groups) > 1 else "all_groups"
        secondary_index_dropped_count = 0
        secondary_index_matched_count = 0
        max_context_tokens = args.get("max_context_tokens", 2048)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            raise MatrixArkError("max_context_tokens must be a positive integer")
        local_budget = local_context_budget(args)
        query_terms = {term for term in tokens(query) if len(term) > 2}
        embedding_started_perf = time.perf_counter()
        query_embedding = embedding_for_text(query)
        self._observe_model_latency("query_embedding", (time.perf_counter() - embedding_started_perf) * 1000.0)
        raw_reference_time_ms = args.get("reference_time_ms", now_ms())
        if not isinstance(raw_reference_time_ms, int):
            raise MatrixArkError("reference_time_ms must be an integer")
        reference_time_ms = raw_reference_time_ms
        auxiliary_quota = integer_arg(ranking, "auxiliary_quota", 2, minimum=0)
        finish_retrieval_stage("query_understanding", stage_started_perf)
        stage_started_perf = time.perf_counter()
        retrieval_record_result = self.retrieval_records(
            scope=scope,
            secondary_index_groups=secondary_index_filter_groups,
        )
        records = retrieval_record_result["records"]
        retrieval_scan_stats = retrieval_record_result.get("scan_stats", {})
        skill_controls = self.latest_skill_controls(records)
        include_superseded_resources = bool(args.get("include_superseded_resources", False) or args.get("historical_replay", False))
        latest_resource_version_by_uri: dict[str, str] = {}
        for manifest in reversed(records):
            if manifest.get("record_type") != "resource_manifest":
                continue
            if not scope_matches(manifest.get("scope", {}), scope):
                continue
            raw_uri_key = str(manifest.get("raw_uri") or "")
            resource_version_key = str(manifest.get("resource_version") or "")
            if raw_uri_key and resource_version_key and raw_uri_key not in latest_resource_version_by_uri:
                latest_resource_version_by_uri[raw_uri_key] = resource_version_key
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
        node_summary_text_by_hash: dict[int, str] = {}
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_index" and scope_matches(candidate_access_scope(record), scope):
                index_name = str(record.get("index_name", ""))
                if index_name:
                    index_terms_by_batch.setdefault(record.get("batch_id_hash"), []).append(index_name)
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
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_embedding" and not scope_matches(candidate_access_scope(record), scope):
                continue
            if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
                dense_score = cosine(query_embedding, record.get("vector", []))
                node_hash = record["node_hash"]
                node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
                sparse_score = sparse_lexical_score(query_terms, node_text)
                score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score), 6)
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
            )

        top_k_per_layer = integer_arg(ranking, "top_k_per_layer", 8, minimum=1)
        max_children_scored_per_parent = integer_arg(ranking, "max_children_scored_per_parent", 10000, minimum=1)
        max_candidates_per_node = integer_arg(ranking, "max_candidates_per_node", 256, minimum=1)
        max_selected_refs = integer_arg(ranking, "max_selected_refs", max(8, min(256, max_context_tokens)), minimum=1)
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

        tree_candidate_records = records if traversal.get("fallback_to_flat") else [record for record in records if selected_by_tree(record)]
        tree_prefilter_dropped_count = 0 if traversal.get("fallback_to_flat") else max(0, len(records) - len(tree_candidate_records))
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
            for record in reversed(tree_candidate_records):
                if record.get("record_type") != "context_summary":
                    continue
                if not access_scope_matches_before_scoring(record, scope):
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
                        {
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
                            "scope": record.get("scope", {}),
                            "updated_at_ms": record.get("updated_at_ms", now_ms()),
                            "text": clip_context_text(text),
                            "recall_path": "primary_summary",
                        },
                        ranking,
                        reference_time_ms=reference_time_ms,
                    )
                )
        for record in reversed(tree_candidate_records):
            if record.get("record_type") != "context_event":
                continue
            envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
            record_scope = candidate_access_scope(record)
            if not access_scope_matches_before_scoring(record, scope):
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
                "metadata": envelope.get("metadata", {}),
                "scope": record_scope,
                "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
            graph_score = sparse_lexical_score(query_terms, graph_text)
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
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
            )
        for record in reversed(tree_candidate_records):
            if record.get("record_type") != "context_entity":
                continue
            if not access_scope_matches_before_scoring(record, scope):
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
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
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
            )
        for record in reversed(tree_candidate_records):
            if record.get("record_type") != "context_segment":
                continue
            if not access_scope_matches_before_scoring(record, scope):
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
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("updated_at_ms", now_ms()),
                "text": clip_context_text(str(record.get("summary_text", ""))),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_hybrid"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [record.get("topic", ""), text]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
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
            )
        for record in reversed(tree_candidate_records):
            if record.get("record_type") not in {"resource_chunk", "skill_section"}:
                continue
            if not access_scope_matches_before_scoring(record, scope):
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
                text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
                embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
                business_type = "skill"
                metadata = {**record.get("metadata", {}), "skill_registry": control}
            else:
                ref_type = "resource_chunk"
                ref_hash = int(record.get("chunk_hash") or 0)
                metadata = record.get("metadata", {})
                raw_uri_value = str(record.get("raw_uri") or "")
                resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
                latest_version = latest_resource_version_by_uri.get(raw_uri_value, resource_version_value)
                is_superseded_version = bool(
                    resource_version_value
                    and latest_version
                    and resource_version_value != latest_version
                )
                if is_superseded_version and not include_superseded_resources:
                    secondary_index_dropped_count += 1
                    continue
                text = f"resource {raw_uri_value} {record.get('source_ref', '')}: {record.get('text', '')}"
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
                    {
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
                        "raw_uri": record.get("raw_uri", ""),
                        "source_ref": record.get("source_ref", ""),
                        "resource_type": record.get("resource_type", ""),
                        "resource_version": metadata.get("resource_version", ""),
                        "supersedes_chunk_hash": metadata.get("supersedes_chunk_hash"),
                        "version_state": "historical" if ref_type == "resource_chunk" and metadata.get("resource_version") != latest_resource_version_by_uri.get(str(record.get("raw_uri") or ""), metadata.get("resource_version", "")) else "current",
                        "stale_or_superseded": bool(ref_type == "resource_chunk" and metadata.get("resource_version") != latest_resource_version_by_uri.get(str(record.get("raw_uri") or ""), metadata.get("resource_version", ""))),
                        "access_decision": "allowed_by_registry_scope_before_scoring",
                        "access_scope": candidate_access_scope(record),
                        "deployment_scope": record.get("deployment_scope", "local"),
                        "citation": record.get("source_ref", ""),
                        "metadata": metadata,
                        "scope": record.get("scope", {}),
                        "updated_at_ms": record.get("updated_at_ms", now_ms()),
                        "text": clip_context_text(text),
                        "recall_path": "primary_resource_skill",
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )

        for record in reversed(tree_candidate_records):
            if record.get("record_type") != "context_compression_event":
                continue
            if not access_scope_matches_before_scoring(record, scope):
                continue
            if not selected_by_tree(record):
                continue
            if not admit_candidate_for_node(record):
                continue
            text = f"TIME_COMPRESS: {record.get('summary_text', '')}"
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
                "scope": record.get("scope", {}),
                "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
                "text": clip_context_text(text),
            }
            if origin_score > 0:
                primary_matches.append(score_recall_candidate({**candidate, "recall_path": "primary_time_compression"}, ranking, reference_time_ms=reference_time_ms))
            graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + [text, "time_compress"]))
            if graph_score > 0:
                auxiliary_matches.append(
                    score_recall_candidate(
                        {
                            **candidate,
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
            )
        finish_retrieval_stage("rerank_score", stage_started_perf)
        stage_started_perf = time.perf_counter()
        primary_matches.sort(key=lambda item: item["score"], reverse=True)
        auxiliary_matches.sort(key=lambda item: item["score"], reverse=True)
        selected, used_context_tokens, dropped_over_budget = select_token_budgeted_refs(
            primary_matches,
            auxiliary_matches,
            max_context_tokens=max_context_tokens,
            auxiliary_quota=auxiliary_quota,
            question_type=question_type,
            reserved_tokens=local_budget["token_estimate"],
            max_selected_refs=max_selected_refs,
            duplicate_text_hashes=local_budget["text_hashes"],
            deadline_exceeded=deadline_exceeded,
            deadline_reason="deadline_during_context_pack",
        )
        partial_context_pack = bool(dropped_over_budget.get("deadline_exceeded"))
        quality_warnings = []
        if partial_context_pack:
            quality_warnings.append(f"retrieval_deadline_exceeded:{dropped_over_budget.get('deadline_reason', 'deadline_during_context_pack')}")
        context_pack_id = stable_hash(f"{query}:{selected}:{now_ms()}")
        context_pack_id_text = str(context_pack_id)
        pack_summary = summarize_text(
            " ".join(str(item.get("text", "")) for item in selected),
            limit=512,
        )
        selected_context_counts = selected_context_class_counts(selected)
        pack = {
            "context_pack_id": str(context_pack_id),
            "selected_refs": selected,
            "selected_ref_counts": selected_context_counts,
            "context_assembly_policy": {
                "access_scope_before_scoring": True,
                "skill_selection": "skill_section_only",
                "resource_selection": "resource_facts_entities_and_chunks_are_ranked_separately",
            },
            "layer_scores": layer_scores[:24],
            "question_type": question_type,
            "packing_policy": f"question_type_aware:{question_type}",
            "query_embedding_model": embedding_model_name(),
            "embedding_execution_mode": embedding_execution_mode_name(),
            "embedding_fallback_used": embedding_fallback_used(),
            "recall_policy": {
                "backend_retrieval_pushdown": retrieval_scan_stats,
                "tree_traversal": {
                    "enabled": True,
                    "summary_embeddings": ["node_l0", "node_l1"],
                    "top_k_per_layer": top_k_per_layer,
                    "max_children_scored_per_parent": max_children_scored_per_parent,
                    "max_candidates_per_node": max_candidates_per_node,
                    "max_selected_refs": max_selected_refs,
                    "selected_node_count": len(selected_node_hashes),
                    "selected_path_count": len(selected_paths),
                    "selected_leaf_count": len(traversal.get("leaf_paths", [])),
                    "candidate_records_after_tree": len(tree_candidate_records),
                    "records_dropped_by_tree": tree_prefilter_dropped_count,
                    "records_dropped_by_node_fanout": fanout_dropped_count,
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
                "primary_path": "tree-first hybrid dense semantic + sparse lexical after secondary-index prefilter",
                "auxiliary_path": "keyword graph inside selected tree after secondary-index prefilter",
                "time_decay": {
                    "freshness_tolerance_ms": ranking.get("freshness_tolerance_ms", DEFAULT_TIME_DECAY_TOLERANCE_MS),
                    "half_life_ms": ranking.get("half_life_ms", DEFAULT_TIME_DECAY_HALFLIFE_MS),
                },
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
            "used_local_context_tokens": local_budget["token_estimate"],
            "total_prompt_context_tokens": used_context_tokens + local_budget["token_estimate"],
            "remote_context_budget_tokens": max(0, max_context_tokens - local_budget["token_estimate"]),
            "local_context_policy": {
                "mode": "shared_budget_dedupe",
                "local_context_count": len(local_budget["items"]),
                "local_context_tokens": local_budget["token_estimate"],
                "dedupe_remote_against_local": True,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "dropped_refs": dropped_over_budget,
            "quality_warnings": quality_warnings,
            "insufficient_context": not selected,
            "partial_context_pack": partial_context_pack,
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
            "recall_policy": pack["recall_policy"],
            "stage_latency_budgets": pack["recall_policy"]["stage_latency_budgets"],
            "storage_options": storage_options,
            "local_context_policy": pack["local_context_policy"],
            "used_local_context_tokens": pack["used_local_context_tokens"],
            "used_remote_context_tokens": pack["used_remote_context_tokens"],
            "total_prompt_context_tokens": pack["total_prompt_context_tokens"],
            "remote_context_budget_tokens": pack["remote_context_budget_tokens"],
            "primary_candidate_count": len(primary_matches),
            "auxiliary_candidate_count": len(auxiliary_matches),
            "tree_candidate_records": len(tree_candidate_records),
            "tree_prefilter_dropped_count": tree_prefilter_dropped_count,
            "fanout_dropped_count": fanout_dropped_count,
            "max_candidates_per_node": max_candidates_per_node,
            "max_selected_refs": max_selected_refs,
            "created_at_ms": now_ms(),
        }
        self.append_audit(audit_record)
        finish_retrieval_stage("audit", audit_started_perf)
        pack["recall_policy"]["stage_latency_budgets"] = stage_budget_snapshot()
        over_budget_stages = pack["recall_policy"]["stage_latency_budgets"].get("over_budget_stages", [])
        if over_budget_stages and not any(str(warning).startswith("stage_budget_exceeded:") for warning in quality_warnings):
            quality_warnings.append("stage_budget_exceeded:" + ",".join(over_budget_stages))
            pack["quality_warnings"] = quality_warnings
        return pack

    def feedback(self, args: Json, *, hook: Json | None = None) -> Json:
        args = {**args, "kind": "feedback"}
        return self.ingest(args, hook=hook)

    def replay(self, args: Json) -> Json:
        context_pack_id = require_string(args, "context_pack_id")
        self.flush_audits()
        return {
            "context_pack_id": context_pack_id,
            "events": self.read_all(),
        }


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

    def _observe_backend_command(self, latency_ms: float, *, error: bool = False, timeout: bool = False, records_written: int = 0, records_read: int = 0) -> None:
        with self._metrics_lock:
            self._commands_total += 1
            if error:
                self._errors_total += 1
            if timeout:
                self._timeouts_total += 1
            self._latency_sum_ms += max(0.0, latency_ms)
            self._latency_max_ms = max(self._latency_max_ms, latency_ms)
            for idx, bucket_ms in enumerate(MatrixArkServiceMetrics.LATENCY_BUCKETS_MS):
                if latency_ms <= bucket_ms:
                    self._latency_buckets[idx] += 1
            self._records_written_total += max(0, records_written)
            self._records_read_total += max(0, records_read)

    def _render_backend_prometheus(self) -> str:
        with self._metrics_lock:
            commands_total = self._commands_total
            errors_total = self._errors_total
            timeouts_total = self._timeouts_total
            latency_sum_ms = self._latency_sum_ms
            latency_max_ms = self._latency_max_ms
            latency_buckets = list(self._latency_buckets)
            records_written_total = self._records_written_total
            records_read_total = self._records_read_total
            started_at_ms = self._metrics_started_at_ms
        elapsed_s = max(0.001, (now_ms() - started_at_ms) / 1000.0)
        qps = commands_total / elapsed_s
        cached_clients = 1
        context_records_total = self._entry_count_cache if self._entry_count_cache is not None else 0
        output = [
            "# HELP matrixark_backend_qps Backend-normalized process-lifetime average command QPS.",
            "# TYPE matrixark_backend_qps gauge",
            f'matrixark_backend_qps{{backend="cpp"}} {qps:.6f}',
            "# HELP matrixark_backend_commands_total Backend-normalized total commands.",
            "# TYPE matrixark_backend_commands_total counter",
            f'matrixark_backend_commands_total{{backend="cpp"}} {commands_total}',
            "# HELP matrixark_backend_errors_total Backend-normalized failed commands.",
            "# TYPE matrixark_backend_errors_total counter",
            f'matrixark_backend_errors_total{{backend="cpp"}} {errors_total}',
            "# HELP matrixark_backend_timeouts_total Backend-normalized timeout count.",
            "# TYPE matrixark_backend_timeouts_total counter",
            f'matrixark_backend_timeouts_total{{backend="cpp"}} {timeouts_total}',
            "# HELP matrixark_backend_cached_clients Backend-normalized cached client/connection count.",
            "# TYPE matrixark_backend_cached_clients gauge",
            f'matrixark_backend_cached_clients{{backend="cpp"}} {cached_clients}',
            "# HELP matrixark_backend_records_written_total Backend-normalized MatrixArk records written.",
            "# TYPE matrixark_backend_records_written_total counter",
            f'matrixark_backend_records_written_total{{backend="cpp"}} {records_written_total}',
            "# HELP matrixark_backend_records_read_total Backend-normalized MatrixArk records read.",
            "# TYPE matrixark_backend_records_read_total counter",
            f'matrixark_backend_records_read_total{{backend="cpp"}} {records_read_total}',
            "# HELP matrixark_context_records_total MatrixArk context records visible through the backend adapter.",
            "# TYPE matrixark_context_records_total gauge",
            f'matrixark_context_records_total{{backend="cpp"}} {context_records_total}',
            "# HELP matrixark_backend_audit_buffered_records Backend-normalized buffered audit records.",
            "# TYPE matrixark_backend_audit_buffered_records gauge",
            f'matrixark_backend_audit_buffered_records{{backend="cpp"}} {len(self._audit_buffer)}',
            "# HELP matrixark_backend_audit_flush_failures_total Backend-normalized audit flush failures.",
            "# TYPE matrixark_backend_audit_flush_failures_total counter",
            f'matrixark_backend_audit_flush_failures_total{{backend="cpp"}} {self._audit_flush_failures}',
            "# HELP matrixark_backend_command_latency_ms Backend-normalized command latency histogram in milliseconds.",
            "# TYPE matrixark_backend_command_latency_ms histogram",
        ]
        for idx, bucket_ms in enumerate(MatrixArkServiceMetrics.LATENCY_BUCKETS_MS):
            le = "+Inf" if bucket_ms == float("inf") else str(int(bucket_ms))
            output.append(f'matrixark_backend_command_latency_ms_bucket{{backend="cpp",le="{le}"}} {latency_buckets[idx]}')
        output.extend(
            [
                f'matrixark_backend_command_latency_ms_sum{{backend="cpp"}} {latency_sum_ms:.6f}',
                f'matrixark_backend_command_latency_ms_count{{backend="cpp"}} {commands_total}',
                "# HELP matrixark_backend_command_latency_max_ms Backend-normalized max observed command latency in milliseconds.",
                "# TYPE matrixark_backend_command_latency_max_ms gauge",
                f'matrixark_backend_command_latency_max_ms{{backend="cpp"}} {latency_max_ms:.6f}',
            ]
        )
        return "\n".join(output) + "\n"

    def backend_metrics(self) -> Json:
        prometheus = self._render_backend_prometheus()
        return {
            "backend": self._backend_label(),
            "metrics_format": "prometheus",
            "prometheus": prometheus,
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
                "qps": self._commands_total / max(0.001, (now_ms() - self._metrics_started_at_ms) / 1000.0),
                "p95_latency_ms": None,
                "p99_latency_ms": None,
                "max_observed_latency_ms": self._latency_max_ms,
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

    def _append_many_materialized(self, records: list[Json]) -> None:
        if not records:
            return
        if self._queue_batched_records(records):
            return
        started = time.monotonic()
        with self._records_lock:
            try:
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
                        entries.append({"key": self._record_hash_key, "field": record_id, "value": payload})
                        self._index_cache.append(record_id)
                    self._hset_many_with_backoff(entries)
                    self._put_string_with_backoff(self._index_key, json.dumps(self._index_cache, separators=(",", ":")))
                    self._records_cache.extend(records)
                    self._put_direct_record_cache(len(self._records_cache), self._records_cache)
                    self._observe_backend_command((time.monotonic() - started) * 1000.0, records_written=len(records))
                    return

                sequence = self._entry_count_cache if self._entry_count_cache is not None else self._get_count()
                entries = []
                for bundle in self._record_bundles(records):
                    record_key, record_id = self._record_location(sequence)
                    payload_value: Json
                    payload_value = bundle[0] if len(bundle) == 1 else {"record_bundle": bundle}
                    payload = json.dumps(payload_value, sort_keys=True, separators=(",", ":"))
                    entries.append({"key": record_key, "field": record_id, "value": payload})
                    sequence += 1
                self._hset_many_with_backoff(entries)
                self._put_string_with_backoff(self._count_key, str(sequence))
                self._entry_count_cache = sequence
                self._records_cache.extend(records)
                self._put_direct_record_cache(self._entry_count_cache, self._records_cache)
                self._observe_backend_command((time.monotonic() - started) * 1000.0, records_written=len(records))
            except Exception as exc:
                self._observe_backend_command((time.monotonic() - started) * 1000.0, error=True, timeout="timeout" in str(exc).lower())
                raise

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
        started = time.monotonic()
        with self._records_lock:
            try:
                if self._records_cache is not None:
                    self._observe_backend_command((time.monotonic() - started) * 1000.0, records_read=len(self._records_cache))
                    return list(self._records_cache)
                count = self._get_count()
                if count > 0:
                    self._legacy_index_mode = False
                    self._entry_count_cache = count
                    cached = self._get_direct_record_cache(count)
                    if cached is not None:
                        self._records_cache = cached
                        self._observe_backend_command((time.monotonic() - started) * 1000.0, records_read=len(self._records_cache))
                        return list(self._records_cache)
                    with self._direct_record_load_lock():
                        cached = self._get_direct_record_cache(count)
                        if cached is not None:
                            self._records_cache = cached
                            self._observe_backend_command((time.monotonic() - started) * 1000.0, records_read=len(self._records_cache))
                            return list(self._records_cache)
                        self._records_cache = self._load_records_by_count(count)
                        self._put_direct_record_cache(count, self._records_cache)
                        self._observe_backend_command((time.monotonic() - started) * 1000.0, records_read=len(self._records_cache))
                        return list(self._records_cache)
                index = self._get_index()
                self._index_cache = index
                self._legacy_index_mode = bool(index)
                self._entry_count_cache = None
                self._records_cache = self._load_records(index)
                self._observe_backend_command((time.monotonic() - started) * 1000.0, records_read=len(self._records_cache))
                return list(self._records_cache)
            except Exception as exc:
                self._observe_backend_command((time.monotonic() - started) * 1000.0, error=True, timeout="timeout" in str(exc).lower())
                raise

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

    def _call_json(self, op: str, *, raise_on_error: bool = True, **kwargs: Any) -> Json:
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
                "process_per_operation_enabled": False,
                "single_shot_mode": "debug_only",
                "supports_health": True,
                "supports_readiness": True,
                "supports_metrics": True,
                "supports_batch_append": True,
                "supports_prefix_scan": True,
                "supports_graceful_shutdown": True,
                "structured_errors": True,
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



class MatrixArkBackpressureError(MatrixArkError):
    pass


class MatrixArkMcpServer:
    IDEMPOTENT_WRITE_TOOLS = {
        "matrixark_ingest",
        "matrixark_batch_extract",
        "matrixark_session_commit",
        "matrixark_refresh_summaries",
        "matrixark_feedback",
        "matrixark_update_skill",
        "matrixark_admin_create_account",
        "matrixark_admin_update_account",
        "matrixark_admin_create_user",
        "matrixark_admin_update_user",
        "matrixark_admin_create_api_key",
        "matrixark_admin_apply_api_key",
        "matrixark_admin_rotate_api_key",
        "matrixark_admin_revoke_api_key",
        "matrixark_admin_map_sso_user",
        "matrixark_auth_signup",
        "matrixark_auth_sso_callback",
    }
    SERVER_NAME = "matrixark-context"
    SERVER_VERSION = "0.2.0"
    DEFAULT_PROTOCOL_VERSION = "2025-06-18"
    DEFAULT_REQUEST_DEADLINES_MS = {
        "matrixark_ingest": int(os.environ.get("MATRIXARK_INGEST_TIMEOUT_MS", "30000")),
        "matrixark_retrieve": int(os.environ.get("MATRIXARK_RETRIEVE_TIMEOUT_MS", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "5000"))),
        "matrixark_feedback": int(os.environ.get("MATRIXARK_FEEDBACK_TIMEOUT_MS", "15000")),
        "matrixark_replay": int(os.environ.get("MATRIXARK_REPLAY_TIMEOUT_MS", "10000")),
    }
    DEFAULT_OPERATION_CONCURRENCY = {
        "ingest": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_INGEST", "32")),
        "retrieve": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_RETRIEVE", "64")),
        "feedback": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_FEEDBACK", "16")),
        "replay": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_REPLAY", "16")),
        "admin": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_ADMIN", "16")),
    }

    def __init__(self, adapter: MatrixArkLocalAdapter, *, line_json: bool = False, access_mode: str = "dev") -> None:
        self.adapter = adapter
        self.line_json = line_json
        self.access = MatrixArkAccessManager(adapter, mode=access_mode)
        self.metrics = MatrixArkServiceMetrics()
        setattr(self.adapter, "_matrixark_service_metrics", self.metrics)
        self._summary_worker_started = False
        self._summary_refresh_interval_s = max(0.0, SUMMARY_REFRESH_INTERVAL_MS / 1000.0)
        self._summary_refresh_limit = max(1, SUMMARY_REFRESH_LIMIT)
        self._operation_backpressure_timeout_ms = max(0, int(os.environ.get("MATRIXARK_BACKPRESSURE_TIMEOUT_MS", "100")))
        self._operation_limiters = {
            group: threading.BoundedSemaphore(max(1, int(capacity)))
            for group, capacity in self.DEFAULT_OPERATION_CONCURRENCY.items()
        }
        self._ensure_summary_worker()

    def _ensure_summary_worker(self) -> None:
        if self._summary_worker_started or self._summary_refresh_interval_s <= 0:
            return
        self._summary_worker_started = True
        thread = threading.Thread(target=self._summary_refresh_loop, name="matrixark-summary-refresher", daemon=True)
        thread.start()
        _mcp_debug_log(
            f"matrixark summary refresher started interval_ms={SUMMARY_REFRESH_INTERVAL_MS} limit={self._summary_refresh_limit}"
        )

    def _summary_refresh_loop(self) -> None:
        while True:
            time.sleep(self._summary_refresh_interval_s)
            try:
                started_perf = time.perf_counter()
                result = self.adapter.refresh_summaries({"scope": {}, "limit": self._summary_refresh_limit})
                self.metrics.observe_operation("summary_refresh", "ok", (time.perf_counter() - started_perf) * 1000.0)
                refreshed_count = int(result.get("refreshed_count") or 0)
                if refreshed_count:
                    self.access.append_audit(
                        "context.refresh_summaries.background",
                        {"account_id": "system", "tenant_id": "system", "user_id": "summary_worker"},
                        status="ok",
                        details={
                            "refreshed_count": refreshed_count,
                            "interval_ms": SUMMARY_REFRESH_INTERVAL_MS,
                            "limit": self._summary_refresh_limit,
                        },
                    )
            except Exception as exc:
                self.metrics.observe_operation("summary_refresh", "error", 0.0, timeout=is_retryable_temporalstore_error(exc))
                _mcp_debug_log(f"matrixark summary refresh loop failed: {exc}")

    def _backend_storage_mode_from_metrics(self, result: Json) -> str:
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        return str(metrics.get("mode") or result.get("gateway_mode") or metrics.get("audit_mode") or "unknown")

    def _refresh_service_metric_gauges(self) -> None:
        now = now_ms()
        dirty_lag_ms = 0
        import_lag_ms = 0
        try:
            records = self.adapter.read_all()
            dirty_times = [int(record.get("updated_at_ms") or record.get("created_at_ms") or now) for record in records if record.get("record_type") == "context_summary_dirty"]
            import_times = [
                int(record.get("updated_at_ms") or record.get("created_at_ms") or now)
                for record in records
                if record.get("record_type") == "resource_import_task" and str(record.get("status") or "") in {"queued", "running"}
            ]
            if dirty_times:
                dirty_lag_ms = max(0, now - min(dirty_times))
            if import_times:
                import_lag_ms = max(0, now - min(import_times))
        except Exception as exc:
            _mcp_debug_log(f"matrixark metrics gauge refresh failed: {exc}")
        queue_depth = 0
        queue_obj = getattr(self.adapter, "_resource_import_queue", None)
        if queue_obj is not None:
            try:
                queue_depth = int(queue_obj.qsize())
            except Exception:
                queue_depth = 0
        audit_write_failures = int(getattr(self.adapter, "_audit_flush_failures", 0) or 0)
        self.metrics.update_gauges(
            dirty_summary_lag_ms=dirty_lag_ms,
            resource_import_lag_ms=import_lag_ms,
            queue_depth=queue_depth,
            audit_write_failures=audit_write_failures,
        )

    def _merge_service_prometheus(self, result: Json) -> Json:
        self._refresh_service_metric_gauges()
        raw_backend = str(result.get("backend") or getattr(self.adapter, "_backend_label", lambda: "local")())
        backend = {
            "temporalstore-cpp": "cpp",
            "temporalstore-direct": "cpp",
            "temporalstore-rust": "rust",
        }.get(raw_backend, raw_backend)
        storage_mode = self._backend_storage_mode_from_metrics(result)
        service_prometheus = self.metrics.render_prometheus(backend=backend, storage_mode=storage_mode)
        combined = str(result.get("prometheus") or "")
        if combined and not combined.endswith("\n"):
            combined += "\n"
        result = dict(result)
        result["metrics_format"] = "prometheus"
        result["prometheus"] = combined + service_prometheus
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        result["metrics"] = {**metrics, "service": self.metrics.snapshot()}
        return result

    def error_response(self, request_id: Any, code: int, message: str, *, data: Json | None = None) -> Json:
        error: Json = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        return {"jsonrpc": "2.0", "id": request_id, "error": error}

    def _validate_jsonrpc_request(self, request: Any) -> tuple[Any, str] | Json:
        if not isinstance(request, dict):
            return self.error_response(None, -32600, "JSON-RPC request must be an object")
        request_id = request.get("id")
        jsonrpc = request.get("jsonrpc", "2.0")
        if jsonrpc != "2.0":
            return self.error_response(request_id, -32600, "jsonrpc must be '2.0'")
        method = request.get("method")
        if not isinstance(method, str) or not method:
            return self.error_response(request_id, -32600, "method must be a non-empty string")
        return request_id, method

    def handle(self, request: Json) -> Json | None:
        validated = self._validate_jsonrpc_request(request)
        if isinstance(validated, dict):
            return validated
        request_id, method = validated
        try:
            if method == "initialize":
                params = request.get("params") or {}
                if not isinstance(params, dict):
                    return self.error_response(request_id, -32602, "initialize params must be an object")
                requested_protocol = params.get("protocolVersion") or self.DEFAULT_PROTOCOL_VERSION
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": requested_protocol,
                        "serverInfo": {
                            "name": self.SERVER_NAME,
                            "version": self.SERVER_VERSION,
                            "serviceMode": "long_lived",
                            "transports": ["stdio-mcp", "http-json"],
                            "requestDeadlines": dict(self.DEFAULT_REQUEST_DEADLINES_MS),
                        },
                        "capabilities": {"tools": {"listChanged": False}},
                    },
                }
            if method == "notifications/initialized":
                return None
            if method == "tools/list":
                return {"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}
            if method == "tools/call":
                params = request.get("params", {})
                if not isinstance(params, dict):
                    return self.error_response(request_id, -32602, "tools/call params must be an object")
                name = params.get("name")
                if not isinstance(name, str) or not name:
                    return self.error_response(request_id, -32602, "tools/call params.name must be a non-empty string")
                args = params.get("arguments", {})
                if not isinstance(args, dict):
                    return self.error_response(request_id, -32602, "tools/call params.arguments must be an object")
                result = self.call_tool(name, args)
                return {"jsonrpc": "2.0", "id": request_id, "result": json_text(result)}
            return self.error_response(request_id, -32601, f"method not found: {method}")
        except MatrixArkError as exc:
            return self.error_response(request_id, -32000, str(exc), data={"error_type": exc.__class__.__name__})
        except Exception as exc:  # MCP errors should stay JSON-RPC shaped.
            _mcp_debug_log(f"handle: internal error for method={method!r}: {exc}")
            return self.error_response(request_id, -32603, "internal MatrixArk MCP server error", data={"error_type": exc.__class__.__name__})

    def _raw_idempotency_key(self, args: Json, hook: Json | None) -> str:
        key = args.get("idempotency_key")
        if not key and isinstance(hook, dict):
            key = hook.get("idempotency_key")
        if key is None:
            return ""
        if not isinstance(key, str) or not key.strip():
            raise MatrixArkError("idempotency_key must be a non-empty string when supplied")
        return key.strip()

    def _idempotency_key_hash(self, name: str, raw_key: str, identity: Json) -> int:
        scope_parts = [
            str(identity.get("account_id") or ""),
            str(identity.get("tenant_id") or ""),
            str(identity.get("user_id") or ""),
            str(identity.get("session_id") or ""),
            str(identity.get("scope_key") or ""),
        ]
        return stable_hash("idempotency:" + name + ":" + ":".join(scope_parts) + ":" + raw_key)

    def _idempotent_replay_response(self, name: str, args: Json, identity: Json, hook: Json | None) -> Json | None:
        if name not in self.IDEMPOTENT_WRITE_TOOLS:
            return None
        raw_key = self._raw_idempotency_key(args, hook)
        if not raw_key:
            return None
        key_hash = self._idempotency_key_hash(name, raw_key, identity)
        record = self.adapter.find_idempotency_record(key_hash)
        if not record:
            return None
        response = dict(record.get("response") or {})
        response["idempotent_replay"] = True
        response["idempotency_key_hash"] = key_hash
        response["access"] = args.get("_matrixark_auth", {})
        self.access.append_audit(
            "idempotency.replay",
            identity,
            status="ok",
            details={"tool_name": name, "idempotency_key_hash": key_hash},
        )
        return response

    def _finalize_write_response(self, name: str, args: Json, identity: Json, hook: Json | None, response: Json) -> Json:
        if name not in self.IDEMPOTENT_WRITE_TOOLS:
            return response
        raw_key = self._raw_idempotency_key(args, hook)
        if not raw_key:
            return response
        key_hash = self._idempotency_key_hash(name, raw_key, identity)
        if not self.adapter.find_idempotency_record(key_hash):
            stored_response = {key: value for key, value in response.items() if key != "access"}
            for secret_key in ("api_key", "new_api_key", "raw_key", "secret"):
                if secret_key in stored_response:
                    stored_response.pop(secret_key, None)
                    stored_response[f"{secret_key}_redacted"] = True
            self.adapter.append_idempotency_record(
                key_hash=key_hash,
                tool_name=name,
                raw_key=raw_key,
                identity=identity,
                response=stored_response,
            )
        response["idempotent_replay"] = False
        response["idempotency_key_hash"] = key_hash
        return response

    def _request_deadline_ms(self, name: str, args: Json) -> int:
        raw_value = args.get("request_deadline_ms", args.get("timeout_ms", self.DEFAULT_REQUEST_DEADLINES_MS.get(name, 0)))
        try:
            deadline_ms = int(raw_value or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("request_deadline_ms/timeout_ms must be an integer")
        if deadline_ms < 0:
            raise MatrixArkError("request_deadline_ms/timeout_ms must be >= 0")
        return deadline_ms

    def _request_timed_out(self, started_perf: float, deadline_ms: int) -> bool:
        return deadline_ms > 0 and (time.perf_counter() - started_perf) * 1000.0 >= deadline_ms

    def _raise_if_request_timed_out(self, name: str, started_perf: float, deadline_ms: int) -> None:
        if self._request_timed_out(started_perf, deadline_ms):
            raise MatrixArkError(f"{name} exceeded request deadline {deadline_ms}ms")

    def _retrieve_timeout_fallback(self, args: Json, *, deadline_ms: int, elapsed_ms: float, reason: str) -> Json:
        query = require_string(args, "query")
        max_context_tokens = args.get("max_context_tokens", 2048)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            max_context_tokens = 2048
        return self.adapter.deadline_fallback_pack(
            query=query,
            scope=optional_object(args, "scope"),
            question_type=str(args.get("question_type") or infer_query_type(query)),
            max_context_tokens=max_context_tokens,
            local_budget=local_context_budget(args),
            deadline_ms=deadline_ms,
            elapsed_ms=round(float(elapsed_ms), 3),
            records=self.adapter.read_all(),
            reason=reason,
        )

    def _operation_group(self, name: str) -> str:
        if name in {"matrixark_ingest", "matrixark_batch_extract", "matrixark_session_commit", "matrixark_refresh_summaries"}:
            return "ingest"
        if name == "matrixark_retrieve":
            return "retrieve"
        if name == "matrixark_feedback":
            return "feedback"
        if name == "matrixark_replay":
            return "replay"
        if name.startswith("matrixark_admin_") or name.startswith("matrixark_auth_") or name in {"matrixark_management_portal", "matrixark_ingestion_dashboard"}:
            return "admin"
        return ""

    @contextmanager
    def _operation_slot(self, name: str, request_deadline_ms: int):
        group = self._operation_group(name)
        limiter = self._operation_limiters.get(group) if group else None
        if limiter is None:
            yield
            return
        wait_ms = self._operation_backpressure_timeout_ms
        if request_deadline_ms > 0:
            wait_ms = min(wait_ms, request_deadline_ms)
        started = time.perf_counter()
        acquired = limiter.acquire(timeout=max(0.0, wait_ms / 1000.0)) if wait_ms > 0 else limiter.acquire(blocking=False)
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self.metrics.observe_backpressure(name)
            raise MatrixArkBackpressureError(f"{name} rejected by service backpressure after {round(elapsed_ms, 3)}ms")
        try:
            yield
        finally:
            limiter.release()

    def call_tool(self, name: str, args: Json) -> Json:
        if not isinstance(name, str) or not name:
            raise MatrixArkError("tool name must be a non-empty string")
        if not isinstance(args, dict):
            raise MatrixArkError("tool arguments must be an object")
        args = dict(args)
        request_deadline_ms = self._request_deadline_ms(name, args)
        hook = args.pop("agent_hook", None)
        identity = self.access.authorize_and_enrich(name, args)
        idempotent_replay = self._idempotent_replay_response(name, args, identity, hook)
        if idempotent_replay is not None:
            return idempotent_replay
        try:
            with self._operation_slot(name, request_deadline_ms):
                return self._call_tool_dispatch(name, args, hook, identity, request_deadline_ms)
        except MatrixArkBackpressureError as exc:
            elapsed_ms = 0.0
            if name == "matrixark_retrieve":
                effective_retrieve_deadline_ms = int(args.get("deadline_ms") or request_deadline_ms or 0)
                result = self._retrieve_timeout_fallback(
                    args,
                    deadline_ms=effective_retrieve_deadline_ms or request_deadline_ms,
                    elapsed_ms=elapsed_ms,
                    reason="service_backpressure",
                )
                result["quality_warnings"] = list(result.get("quality_warnings", [])) + ["service_backpressure"]
                result["request_deadline_ms"] = request_deadline_ms
                result["request_elapsed_ms"] = round(elapsed_ms, 3)
                result["partial_context_pack"] = True
                result["backpressure"] = True
                self.metrics.observe_operation("retrieve", "ok", elapsed_ms, timeout=True)
                self.metrics.observe_retrieve_result(result)
                self.access.append_audit(
                    "context.retrieve",
                    identity,
                    status="backpressure_partial",
                    details={"context_pack_id": result.get("context_pack_id"), "request_deadline_ms": request_deadline_ms},
                )
                return {**result, "access": args.get("_matrixark_auth", {})}
            raise MatrixArkError(str(exc))

    def _call_tool_dispatch(self, name: str, args: Json, hook: Json | None, identity: Json, request_deadline_ms: int) -> Json:
            if name == "matrixark_backend_ready":
                started_perf = time.perf_counter()
                try:
                    result = adapter_ensure_backend_ready(
                        self.adapter,
                        reason=str(args.get("reason") or "manual"),
                        probe=bool(args.get("probe", True)),
                        timeout_ms=args.get("timeout_ms"),
                    )
                except Exception as exc:
                    self.metrics.observe_operation("backend_ready", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    self.metrics.observe_backend_ready(False, "error")
                    raise
                status = "ok" if result.get("status") == "ready" else "topology_not_ready"
                self.metrics.observe_operation("backend_ready", "ok", (time.perf_counter() - started_perf) * 1000.0)
                self.metrics.observe_backend_ready(result.get("status") == "ready", str(result.get("status") or status))
                self.access.append_audit(
                    "backend.ready",
                    identity,
                    status=status,
                    details={"backend": result.get("backend"), "attempts": result.get("attempts")},
                )
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_backend_metrics":
                started_perf = time.perf_counter()
                try:
                    result = self._merge_service_prometheus(self.adapter.backend_metrics())
                except Exception as exc:
                    self.metrics.observe_operation("backend_metrics", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    raise
                self.metrics.observe_operation("backend_metrics", "ok", (time.perf_counter() - started_perf) * 1000.0)
                self.access.append_audit(
                    "backend.metrics",
                    identity,
                    status="ok",
                    details={"backend": result.get("backend"), "metrics_format": result.get("metrics_format")},
                )
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_ingest":
                started_perf = time.perf_counter()
                try:
                    result = self.adapter.ingest(args, hook=hook)
                except Exception as exc:
                    self.metrics.observe_operation("ingest", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    raise
                elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                self.metrics.observe_operation("ingest", "ok", elapsed_ms, timeout=request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
                self._raise_if_request_timed_out(name, started_perf, request_deadline_ms)
                self.metrics.observe_ingest_result(result)
                self.access.append_audit("context.ingest", identity, status="ok", details={"event_id_hash": result.get("event_id_hash"), "request_deadline_ms": request_deadline_ms})
                response = {**result, "access": args.get("_matrixark_auth", {}), "request_deadline_ms": request_deadline_ms, "request_elapsed_ms": round(elapsed_ms, 3)}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_batch_extract":
                started_perf = time.perf_counter()
                try:
                    result = self.adapter.batch_extract(args, hook=hook)
                except Exception as exc:
                    self.metrics.observe_operation("batch_extract", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    raise
                self.metrics.observe_operation("batch_extract", "ok", (time.perf_counter() - started_perf) * 1000.0)
                self.access.append_audit("context.batch_extract", identity, status="ok", details={"batch_id_hash": result.get("batch_id_hash")})
                response = {**result, "access": args.get("_matrixark_auth", {})}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_session_commit":
                result = self.adapter.session_commit(args, hook=hook)
                self.access.append_audit("context.session_commit", identity, status="ok", details={"commit_id_hash": result.get("commit_id_hash"), "batch_id_hash": result.get("batch_id_hash")})
                response = {**result, "access": args.get("_matrixark_auth", {})}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_refresh_summaries":
                started_perf = time.perf_counter()
                try:
                    result = self.adapter.refresh_summaries(args)
                except Exception as exc:
                    self.metrics.observe_operation("summary_refresh", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    raise
                self.metrics.observe_operation("summary_refresh", "ok", (time.perf_counter() - started_perf) * 1000.0)
                self.access.append_audit("context.refresh_summaries", identity, status="ok", details={"refreshed_count": result.get("refreshed_count")})
                response = {**result, "access": args.get("_matrixark_auth", {})}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_retrieve":
                started_perf = time.perf_counter()
                effective_retrieve_deadline_ms = int(args.get("deadline_ms") or request_deadline_ms or 0)
                if effective_retrieve_deadline_ms > 0 and "deadline_ms" not in args:
                    args["deadline_ms"] = effective_retrieve_deadline_ms
                try:
                    result = self.adapter.retrieve(args)
                except Exception as exc:
                    elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                    timeout = is_retryable_temporalstore_error(exc) or (request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
                    self.metrics.observe_operation("retrieve", "error", elapsed_ms, timeout=timeout)
                    if timeout:
                        result = self._retrieve_timeout_fallback(args, deadline_ms=effective_retrieve_deadline_ms or request_deadline_ms, elapsed_ms=elapsed_ms, reason="request_deadline_exception")
                        result["quality_warnings"] = list(result.get("quality_warnings", [])) + ["request_deadline_exception"]
                        result["request_deadline_ms"] = request_deadline_ms
                        result["request_elapsed_ms"] = round(elapsed_ms, 3)
                        result["partial_context_pack"] = True
                        self.metrics.observe_operation("retrieve", "ok", elapsed_ms, timeout=True)
                        self.metrics.observe_retrieve_result(result)
                        self.access.append_audit("context.retrieve", identity, status="timeout_partial", details={"context_pack_id": result.get("context_pack_id"), "request_deadline_ms": request_deadline_ms})
                        return {**result, "access": args.get("_matrixark_auth", {})}
                    raise
                elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                timeout = request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms
                if timeout and not result.get("partial_context_pack"):
                    result = self._retrieve_timeout_fallback(args, deadline_ms=effective_retrieve_deadline_ms or request_deadline_ms, elapsed_ms=elapsed_ms, reason="request_deadline_after_retrieve")
                    result["quality_warnings"] = list(result.get("quality_warnings", [])) + ["request_deadline_after_retrieve"]
                    result["partial_context_pack"] = True
                result["request_deadline_ms"] = request_deadline_ms
                result["request_elapsed_ms"] = round(elapsed_ms, 3)
                self.metrics.observe_operation("retrieve", "ok", elapsed_ms, timeout=timeout)
                self.metrics.observe_retrieve_result(result)
                self.access.append_audit("context.retrieve", identity, status="timeout_partial" if timeout else "ok", details={"context_pack_id": result.get("context_pack_id"), "request_deadline_ms": request_deadline_ms})
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_ingestion_dashboard":
                result = self.adapter.ingestion_dashboard(args)
                self.access.append_audit("context.ingestion_dashboard", identity, status="ok", details={"table": result.get("table"), "total": result.get("total")})
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_auth_signup":
                result = self.access.signup(args, identity)
                response = {**result, "access": args.get("_matrixark_auth", {})}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_auth_sso_login":
                result = self.access.sso_login(args, identity)
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_auth_sso_callback":
                result = self.access.sso_callback(args, identity)
                response = {**result, "access": args.get("_matrixark_auth", {})}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_management_portal":
                result = self.access.management_portal(args, identity)
                self.access.append_audit("admin.management_portal", identity, status="ok", details={"account_id": result.get("account_id"), "tenant_id": result.get("tenant_id")})
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_list_resources":
                result = self.adapter.list_resources(args)
                self.access.append_audit("resource.list", identity, status="ok", details={"count": result.get("count")})
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_list_skills":
                result = self.adapter.list_skills(args)
                self.access.append_audit("skill.list", identity, status="ok", details={"count": result.get("count")})
                return {**result, "access": args.get("_matrixark_auth", {})}
            if name == "matrixark_update_skill":
                result = self.adapter.update_skill(args)
                self.access.append_audit("skill.update", identity, status="ok", details={"skill_hash": result.get("skill_hash"), "skill_status": result.get("status")})
                response = {**result, "access": args.get("_matrixark_auth", {})}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_feedback":
                started_perf = time.perf_counter()
                try:
                    result = self.adapter.feedback(args, hook=hook)
                except Exception as exc:
                    self.metrics.observe_operation("feedback", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    raise
                elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                self.metrics.observe_operation("feedback", "ok", elapsed_ms, timeout=request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
                self._raise_if_request_timed_out(name, started_perf, request_deadline_ms)
                self.access.append_audit("context.feedback", identity, status="ok", details={"event_id_hash": result.get("event_id_hash"), "request_deadline_ms": request_deadline_ms})
                response = {**result, "access": args.get("_matrixark_auth", {}), "request_deadline_ms": request_deadline_ms, "request_elapsed_ms": round(elapsed_ms, 3)}
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_replay":
                started_perf = time.perf_counter()
                try:
                    result = self.adapter.replay(args)
                except Exception as exc:
                    self.metrics.observe_operation("replay", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
                    raise
                elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
                self.metrics.observe_operation("replay", "ok", elapsed_ms, timeout=request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
                self._raise_if_request_timed_out(name, started_perf, request_deadline_ms)
                self.access.append_audit("context.replay", identity, status="ok", details={"context_pack_id": args.get("context_pack_id"), "request_deadline_ms": request_deadline_ms})
                return {**result, "access": args.get("_matrixark_auth", {}), "request_deadline_ms": request_deadline_ms, "request_elapsed_ms": round(elapsed_ms, 3)}
            if name == "matrixark_admin_create_account":
                response = self.access.create_account(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_update_account":
                response = self.access.update_account(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_list_accounts":
                return self.access.list_accounts(args, identity)
            if name == "matrixark_admin_create_user":
                response = self.access.create_user(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_update_user":
                response = self.access.update_user(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_list_users":
                return self.access.list_users(args, identity)
            if name == "matrixark_admin_create_api_key":
                response = self.access.create_api_key(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_apply_api_key":
                response = self.access.apply_api_key(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_list_api_keys":
                return self.access.list_api_keys(args, identity)
            if name == "matrixark_admin_rotate_api_key":
                response = self.access.rotate_api_key(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_revoke_api_key":
                response = self.access.revoke_api_key(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_map_sso_user":
                response = self.access.map_sso_user(args, identity)
                return self._finalize_write_response(name, args, identity, hook, response)
            if name == "matrixark_admin_audit":
                return self.access.audit(args, identity)
            raise MatrixArkError(f"unsupported tool {name!r}")

    def read_message(self) -> Json | None:
        if self.line_json:
            line = sys.stdin.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                return {}
            if not line.lstrip().startswith("{"):
                return {}
            return json.loads(line)

        _mcp_debug_log("read_message: waiting for first header")
        first = sys.stdin.buffer.readline()
        _mcp_debug_log(f"read_message: first={first[:80]!r}")
        if not first:
            return None
        if not first.strip():
            return {}
        if first.lstrip().startswith(b"{"):
            # Codex CLI currently speaks newline-delimited JSON over stdio for
            # configured MCP servers. Auto-detect it so responses use the same
            # framing and do not trigger parse-error ping-pong.
            self.line_json = True
            return json.loads(first.decode("utf-8"))

        headers = [first]
        while True:
            header = sys.stdin.buffer.readline()
            if header in {b"\r\n", b"\n", b""}:
                break
            headers.append(header)

        length = None
        for header in headers:
            if header.lower().startswith(b"content-length:"):
                length = int(header.split(b":", 1)[1].strip())
                break
        if length is None:
            raise MatrixArkError("invalid MCP frame: missing Content-Length header")
        body = sys.stdin.buffer.read(length)
        _mcp_debug_log(f"read_message: body_len={len(body)}")
        return json.loads(body.decode("utf-8"))

    def write_response(self, response: Json) -> None:
        payload = json.dumps(response, sort_keys=True)
        if self.line_json:
            sys.stdout.write(payload + "\n")
            sys.stdout.flush()
            return
        body = payload.encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
        sys.stdout.buffer.write(body)
        sys.stdout.buffer.flush()
        _mcp_debug_log(f"write_response: bytes={len(body)} id={response.get('id')!r} keys={list(response.keys())}")

    def serve(self) -> None:
        while True:
            try:
                request = self.read_message()
            except json.JSONDecodeError as exc:
                _mcp_debug_log(f"serve: parse error: {exc}")
                self.write_response(self.error_response(None, -32700, "parse error", data={"detail": str(exc)}))
                continue
            except Exception as exc:
                _mcp_debug_log(f"serve: invalid request frame: {exc}")
                self.write_response(self.error_response(None, -32600, "invalid request frame", data={"detail": str(exc)}))
                continue
            if request is None:
                return
            if not request:
                continue
            response = self.handle(request)
            if response is not None:
                self.write_response(response)

    def serve_http(self, *, host: str, port: int, static_root: Path) -> None:
        handler = make_matrixark_http_handler(self, static_root)
        httpd = ThreadingHTTPServer((host, port), handler)
        actual_host, actual_port = httpd.server_address
        _mcp_debug_log(f"http: serving management portal on http://{actual_host}:{actual_port} root={static_root}")
        try:
            httpd.serve_forever()
        finally:
            httpd.server_close()


def production_profile_enabled() -> bool:
    return MATRIXARK_MCP_PROFILE in {"prod", "production", "benchmark", "bench", "parity"}


def backend_ready_required(backend: str) -> bool:
    if MATRIXARK_REQUIRE_BACKEND_READY:
        return MATRIXARK_REQUIRE_BACKEND_READY in {"1", "true", "yes"}
    return production_profile_enabled() and backend in {"temporalstore-direct", "temporalstore-rust"}


def validate_mcp_backend_policy(args: argparse.Namespace) -> None:
    local_backends = {"local", "temporalstore-local"}
    if production_profile_enabled() and args.backend in local_backends and not MATRIXARK_ALLOW_LOCAL_BACKEND:
        raise MatrixArkError(
            "MatrixArk MCP production/benchmark profile requires --backend temporalstore-direct "
            "or --backend temporalstore-rust. Set MATRIXARK_ALLOW_LOCAL_BACKEND=1 only for debug."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backend",
        choices=["local", "temporalstore-local", "temporalstore-direct", "temporalstore-rust"],
        default=os.environ.get("MATRIXARK_MCP_BACKEND", "local"),
        help="Storage backend. local uses JSONL; temporalstore-local uses a no-metaserver local TemporalStore-shaped record log; temporalstore-direct uses the native C++ TemporalStore SDK.",
    )
    parser.add_argument(
        "--event-log",
        type=Path,
        default=Path("/tmp/matrixark-mcp-events.jsonl"),
        help="JSONL event log used by the local adapter.",
    )
    parser.add_argument(
        "--local-store",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_TEMPORALSTORE_LOCAL_STORE", "/tmp/matrixark-mcp-temporalstore-local.jsonl")),
        help="Persistent local record log for --backend temporalstore-local. This mode does not require metaserver.",
    )
    parser.add_argument(
        "--line-json",
        action="store_true",
        help="Use newline-delimited JSON for simple shell debugging instead of MCP framing.",
    )
    parser.add_argument(
        "--http-host",
        default=os.environ.get("MATRIXARK_HTTP_HOST", "127.0.0.1"),
        help="Host for the optional HTTP/JSON management portal facade.",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=int(os.environ.get("MATRIXARK_HTTP_PORT", "0")),
        help="If non-zero, serve the browser portal and /api JSON facade instead of stdio MCP.",
    )
    parser.add_argument(
        "--http-root",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_HTTP_ROOT", str(Path(__file__).resolve().parent / "temporalstore-monitoring-ui"))),
        help="Static document root for HTTP portal mode.",
    )
    parser.add_argument(
        "--access-mode",
        choices=["dev", "enforced"],
        default=os.environ.get("MATRIXARK_ACCESS_MODE", "dev"),
        help="dev allows omitted API keys for local testing; enforced requires scoped MatrixArk API keys.",
    )
    parser.add_argument(
        "--metaserver",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"),
        help="C++ TemporalStore metaserver address for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"),
        help="TemporalStore namespace for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"),
        help="TemporalStore table for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--temporalstore-lib",
        default=os.environ.get("TEMPORALSTORE_LIB", ""),
        help="Path to libbcache2.so for --backend temporalstore-direct.",
    )
    parser.add_argument(
        "--storage-prefix",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:mcp"),
        help="TemporalStore key prefix for MatrixArk records.",
    )
    parser.add_argument(
        "--rust-cli",
        default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", ""),
        help="Path to the Rust matrixark_gateway or matrixark_record_log binary for --backend temporalstore-rust.",
    )
    parser.add_argument(
        "--request-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "20000")),
        help="Per-request timeout for the native C++ TemporalStore SDK.",
    )
    parser.add_argument(
        "--io-timeout-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "20000")),
        help="BRPC I/O timeout for the native C++ TemporalStore SDK.",
    )
    args = parser.parse_args()
    _mcp_debug_log(f"main: parsed backend={args.backend} metaserver={args.metaserver}")
    validate_mcp_backend_policy(args)
    if args.backend == "temporalstore-direct":
        adapter = MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    elif args.backend == "temporalstore-rust":
        adapter = MatrixArkTemporalStoreRustAdapter(
            rust_cli=args.rust_cli,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    elif args.backend == "temporalstore-local":
        adapter = MatrixArkLocalAdapter(args.local_store)
    else:
        adapter = MatrixArkLocalAdapter(args.event_log)
    if backend_ready_required(args.backend):
        readiness = adapter_ensure_backend_ready(adapter, reason="mcp_startup", probe=True)
        if readiness.get("status") != "ready":
            raise MatrixArkError(f"MatrixArk MCP backend not ready at startup: {json.dumps(readiness, sort_keys=True)}")
    _mcp_debug_log("main: adapter ready; serving")
    mcp_server = MatrixArkMcpServer(adapter, line_json=args.line_json, access_mode=args.access_mode)
    if args.http_port:
        mcp_server.serve_http(host=args.http_host, port=args.http_port, static_root=args.http_root)
    else:
        mcp_server.serve()
    _mcp_debug_log("main: serve returned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
