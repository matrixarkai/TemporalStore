#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk MCP server entrypoint.

The implementation is split into focused modules:
- matrixark_mcp_core: shared primitives, extraction, scoring, traversal helpers
- matrixark_access: account/tenant/user/API-key metadata and governance
- matrixark_mcp_schemas: MCP tool schema catalog
- matrixark_http: management portal HTTP facade

This file keeps the MCP dispatch loop and process entrypoint while re-exporting
storage adapters for compatibility with existing scripts.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sys
from http.server import ThreadingHTTPServer
from pathlib import Path
import traceback
import threading
import time

try:
    from tools.matrixark_mcp_core import (
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        MATRIXARK_MCP_PROFILE,
        MATRIXARK_REQUIRE_BACKEND_READY,
        MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
        SUMMARY_REFRESH_INTERVAL_MS,
        SUMMARY_REFRESH_LIMIT,
        Json,
        MAX_SECONDARY_INDEX_REFS_PER_POSTING,
        SECONDARY_INDEX_TIME_BUCKET_MS,
        MatrixArkError,
        _mcp_debug_log,
        build_cross_session_policy,
        compact_latest_context_state_records,
        compact_context_pack_refs,
        canonical_entity_name,
        compact_dropped_refs_for_context_pack,
        compact_context_pack_for_serving_flat as compact_context_pack_for_serving,
        candidate_access_scope,
        context_index_posting_record,
        embedding_model_ref_for_name,
        enrich_scope_with_identity,
        is_retryable_temporalstore_error,
        json_text,
        materialize_serving_record_batch,
        now_ms,
        registry_access_scope,
        scope_matches,
        scope_key_from_hashes,
        select_token_budgeted_refs,
    )
    from tools.matrixark_access import MatrixArkAccessManager
    from tools.matrixark_http import make_matrixark_http_handler
    from tools.matrixark_mcp_backends import (
        add_backend_arguments,
        backend_ready_required,
        build_mcp_adapter,
        default_mcp_backend,
        ensure_startup_backend_ready,
        production_profile_enabled,
        validate_mcp_backend_policy,
    )
    from tools.matrixark_mcp_dispatch import dispatch_matrixark_tool
    from tools.matrixark_mcp_admin import is_admin_tool
    from tools.matrixark_mcp_ingestion import is_ingestion_tool
    from tools.matrixark_mcp_requests import normalize_mcp_tool_request
    from tools.matrixark_mcp_retrieval import is_retrieval_tool
    from tools.matrixark_mcp_server_request_policy import (
        MatrixArkBackpressureError,
        MatrixArkServerRequestPolicyMixin,
    )
    from tools.matrixark_mcp_schemas import TOOLS
    from tools.matrixark_mcp_stream_materialize import flush_due_scopes
    from tools.matrixark_mcp_summary_runtime import next_summary_refresh_delay_s
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        MATRIXARK_MCP_PROFILE,
        MATRIXARK_REQUIRE_BACKEND_READY,
        MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
        SUMMARY_REFRESH_INTERVAL_MS,
        SUMMARY_REFRESH_LIMIT,
        Json,
        MAX_SECONDARY_INDEX_REFS_PER_POSTING,
        SECONDARY_INDEX_TIME_BUCKET_MS,
        MatrixArkError,
        _mcp_debug_log,
        build_cross_session_policy,
        compact_latest_context_state_records,
        compact_context_pack_refs,
        canonical_entity_name,
        compact_dropped_refs_for_context_pack,
        compact_context_pack_for_serving_flat as compact_context_pack_for_serving,
        candidate_access_scope,
        context_index_posting_record,
        embedding_model_ref_for_name,
        enrich_scope_with_identity,
        is_retryable_temporalstore_error,
        json_text,
        materialize_serving_record_batch,
        now_ms,
        registry_access_scope,
        scope_matches,
        scope_key_from_hashes,
        select_token_budgeted_refs,
    )
    from matrixark_access import MatrixArkAccessManager
    from matrixark_http import make_matrixark_http_handler
    from matrixark_mcp_backends import (
        add_backend_arguments,
        backend_ready_required,
        build_mcp_adapter,
        default_mcp_backend,
        ensure_startup_backend_ready,
        production_profile_enabled,
        validate_mcp_backend_policy,
    )
    from matrixark_mcp_dispatch import dispatch_matrixark_tool
    from matrixark_mcp_admin import is_admin_tool
    from matrixark_mcp_ingestion import is_ingestion_tool
    from matrixark_mcp_requests import normalize_mcp_tool_request
    from matrixark_mcp_retrieval import is_retrieval_tool
    from matrixark_mcp_server_request_policy import (
        MatrixArkBackpressureError,
        MatrixArkServerRequestPolicyMixin,
    )
    from matrixark_mcp_schemas import TOOLS
    from matrixark_mcp_stream_materialize import flush_due_scopes
    from matrixark_mcp_summary_runtime import next_summary_refresh_delay_s


__all__ = [
    "MatrixArkBackpressureError",
    "MatrixArkMcpServer",
    "MatrixArkLocalAdapter",
    "MatrixArkRustCliClient",
    "MatrixArkTemporalStoreDirectAdapter",
    "MatrixArkTemporalStoreRustAdapter",
    "MAX_SECONDARY_INDEX_REFS_PER_POSTING",
    "SECONDARY_INDEX_TIME_BUCKET_MS",
    "backend_ready_required",
    "build_cross_session_policy",
    "canonical_entity_name",
    "candidate_access_scope",
    "compact_latest_context_state_records",
    "compact_context_pack_refs",
    "compact_dropped_refs_for_context_pack",
    "context_index_posting_record",
    "default_mcp_backend",
    "embedding_model_ref_for_name",
    "enrich_scope_with_identity",
    "main",
    "materialize_serving_record_batch",
    "production_profile_enabled",
    "registry_access_scope",
    "scope_matches",
    "scope_key_from_hashes",
    "select_token_budgeted_refs",
    "validate_mcp_backend_policy",
]


# Background stream-materializer: proactively drains SCHEDULED idle-commit tasks so a
# plain (non-finalized) streaming ingest becomes retrievable on its own after a short
# debounce, instead of waiting for a retrieve-time flush to happen to run. The loop drains
# a per-scope registry (populated at ingest time) with the SAME backend-native flush the
# retrieve path uses (`pre_retrieval_idle_commit_flush` over the rust-native, SCOPED
# `idle_commit_task_records`) -- never Python `read_all`, and never a full-store scan. It is
# started per worker process and wired into the gateway lifespan (see
# matrixark_v1_gateway.create_v1_app / the ASGI lifespan handler). 0 disables the loop.
STREAM_MATERIALIZE_INTERVAL_MS = int(os.environ.get("MATRIXARK_STREAM_MATERIALIZE_INTERVAL_MS", "1500"))
# Hard cap on tracked pending scopes so a slow/stuck backend cannot grow the registry without
# bound; the durable scheduled-task record + retrieve-time flush remain the backstop.
STREAM_MATERIALIZE_MAX_SCOPES = int(os.environ.get("MATRIXARK_STREAM_MATERIALIZE_MAX_SCOPES", "20000"))


try:
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        materialize_serving_record_batch,
    )
    from tools.matrixark_mcp_temporal_adapters import (
        MatrixArkRustCliClient,
        MatrixArkRustProxyClient,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        materialize_serving_record_batch,
    )
    from matrixark_mcp_temporal_adapters import (
        MatrixArkRustCliClient,
        MatrixArkRustProxyClient,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )


class MatrixArkMcpServer(MatrixArkServerRequestPolicyMixin):
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
        "matrixark_auth_sso_login",
        "matrixark_auth_login",
    }
    SCOPED_READ_TOOLS = {"matrixark_retrieve", "matrixark_replay", "matrixark_management_portal", "matrixark_ingestion_dashboard", "matrixark_list_resources", "matrixark_list_skills"}
    SERVER_NAME = "matrixark-context"
    SERVER_VERSION = "0.2.0"
    DEFAULT_PROTOCOL_VERSION = "2025-06-18"
    DEFAULT_REQUEST_DEADLINES_MS = {
        "matrixark_ingest": int(os.environ.get("MATRIXARK_INGEST_TIMEOUT_MS", "30000")),
        # Retrieve deadline default: a cold-start proxy scans the full serving-record
        # set (thousands of records) before scoring, which routinely exceeds the old
        # 5000ms ceiling and made the server discard the real ContextPack for an empty
        # deadline_fallback_pack (silent recall collapse: refs computed but dropped).
        # Raised to match the ingest deadline (30000ms); still fully overridable via
        # MATRIXARK_RETRIEVE_TIMEOUT_MS / MATRIXARK_RETRIEVAL_TIMEOUT_MS (set to 5000 to
        # restore prior behavior). Actual warm/cold retrieve latency stays well under
        # this ceiling; this only prevents premature abort of an in-flight retrieve.
        "matrixark_retrieve": int(os.environ.get("MATRIXARK_RETRIEVE_TIMEOUT_MS", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "30000"))),
        "matrixark_feedback": int(os.environ.get("MATRIXARK_FEEDBACK_TIMEOUT_MS", "15000")),
        "matrixark_replay": int(os.environ.get("MATRIXARK_REPLAY_TIMEOUT_MS", "10000")),
        "matrixark_admin": int(os.environ.get("MATRIXARK_ADMIN_TIMEOUT_MS", "10000")),
    }
    DEFAULT_OPERATION_CONCURRENCY = {
        "ingest": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_INGEST", "32")),
        "retrieve": int(os.environ.get("MATRIXARK_MAX_CONCURRENT_RETRIEVE", str(max(4, min(8, (os.cpu_count() or 8) // 2))))),
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
        self._summary_stop = threading.Event()
        self._summary_thread: threading.Thread | None = None
        self._summary_refresh_interval_s = max(0.0, SUMMARY_REFRESH_INTERVAL_MS / 1000.0)
        self._summary_refresh_limit = max(1, SUMMARY_REFRESH_LIMIT)
        self._stream_materialize_worker_started = False
        self._stream_materialize_stop = threading.Event()
        self._stream_materialize_thread: threading.Thread | None = None
        self._stream_materialize_interval_s = max(0.0, STREAM_MATERIALIZE_INTERVAL_MS / 1000.0)
        # scope_key -> (scope_dict, due_ms). Populated at ingest time; drained by the loop.
        self._stream_materialize_registry: dict[str, tuple[Json, int]] = {}
        self._stream_materialize_registry_lock = threading.Lock()
        self._operation_backpressure_timeout_ms = max(0, int(os.environ.get("MATRIXARK_BACKPRESSURE_TIMEOUT_MS", "100")))
        self._retrieve_shed_cooldown_ms = max(0, int(os.environ.get("MATRIXARK_RETRIEVE_SHED_COOLDOWN_MS", "0")))
        self._retrieve_shed_until_perf = 0.0
        self._retrieve_shed_lock = threading.Lock()
        # Audits default OFF: audit records live in the main record log, so with auditing on a
        # read-heavy workload grows the store without bound. MATRIXARK_AUDIT_MODE=async restores
        # the off-request-path auditing; full/sync restore per-call durability.
        self._audit_mode_default = os.environ.get("MATRIXARK_AUDIT_MODE", "off").strip().lower() or "off"
        self._audit_executor = ThreadPoolExecutor(max_workers=max(1, int(os.environ.get("MATRIXARK_AUDIT_WORKERS", "2"))))
        self._operation_limiters = {
            group: threading.BoundedSemaphore(max(1, int(capacity)))
            for group, capacity in self.DEFAULT_OPERATION_CONCURRENCY.items()
        }
        self._ensure_summary_worker()

    def _ensure_summary_worker(self) -> None:
        if self._summary_worker_started or self._summary_refresh_interval_s <= 0:
            return
        self._summary_worker_started = True
        self._summary_stop.clear()
        self._summary_thread = threading.Thread(target=self._summary_refresh_loop, name="matrixark-summary-refresher", daemon=True)
        self._summary_thread.start()
        _mcp_debug_log(
            f"matrixark summary refresher started interval_ms={SUMMARY_REFRESH_INTERVAL_MS} limit={self._summary_refresh_limit}"
        )

    def _next_summary_refresh_delay_s(self, last_pass_s: float) -> float:
        """Delay before the next refresh pass -- see `next_summary_refresh_delay_s`."""
        return next_summary_refresh_delay_s(self._summary_refresh_interval_s, last_pass_s)

    def _summary_refresh_loop(self) -> None:
        delay_s = self._summary_refresh_interval_s
        while not self._summary_stop.wait(delay_s):
            started_perf = time.perf_counter()
            try:
                result = self.adapter.refresh_summaries({"scope": {}, "limit": self._summary_refresh_limit})
                self.metrics.observe_operation("summary_refresh", "ok", (time.perf_counter() - started_perf) * 1000.0)
                refreshed_count = int(result.get("refreshed_count") or 0)
                if refreshed_count:
                    self.access.append_audit("context.refresh_summaries.background", {"account_id": "system", "tenant_id": "system", "user_id": "summary_worker"}, status="ok", details={"refreshed_count": refreshed_count, "interval_ms": SUMMARY_REFRESH_INTERVAL_MS, "limit": self._summary_refresh_limit})
            except Exception as exc:
                self.metrics.observe_operation("summary_refresh", "error", 0.0, timeout=is_retryable_temporalstore_error(exc))
                _mcp_debug_log(f"matrixark summary refresh loop failed: {exc}")
            delay_s = self._next_summary_refresh_delay_s(time.perf_counter() - started_perf)

    def ensure_stream_materialize_worker(self) -> None:
        """Start the background stream-materializer loop (idempotent, per-process).

        Modelled on `_ensure_summary_worker`. The gateway calls this once per uvicorn worker
        (from `create_v1_app` and on ASGI `lifespan.startup`); the loop proactively drains
        SCHEDULED idle-commit tasks so non-finalized streaming ingests become retrievable on
        their own. It is a no-op when the interval is 0 or the backend adapter cannot serve the
        native `idle_commit_task_records` scan (so we never fall back to Python `read_all`).
        """
        if self._stream_materialize_worker_started or self._stream_materialize_interval_s <= 0:
            return
        if not callable(getattr(self.adapter, "idle_commit_task_records", None)):
            _mcp_debug_log("matrixark stream materializer skipped: adapter lacks native idle_commit_task_records")
            return
        self._stream_materialize_worker_started = True
        self._stream_materialize_stop.clear()
        self._stream_materialize_thread = threading.Thread(
            target=self._stream_materialize_loop, name="matrixark-stream-materializer", daemon=True
        )
        self._stream_materialize_thread.start()
        _mcp_debug_log(f"matrixark stream materializer started interval_ms={STREAM_MATERIALIZE_INTERVAL_MS}")

    def register_stream_materialize_scope(self, scope: Json, due_ms: int) -> None:
        """Record a scope whose streaming-ingest debounce should be flushed by the loop.

        Called by the gateway right after a plain (non-finalize) `/v1/ingest`. Each scope is
        owned by the worker process that handled the ingest, so the loop only ever runs cheap
        SCOPED flushes and no cross-worker coordination is needed. Losing an entry (worker
        crash) is safe: the durable scheduled-task record + retrieve-time flush still commit it.
        """
        if not self._stream_materialize_worker_started:
            return
        try:
            key = json.dumps(scope or {}, sort_keys=True, default=str)
        except (TypeError, ValueError):
            key = str(scope)
        with self._stream_materialize_registry_lock:
            registry = self._stream_materialize_registry
            existing = registry.get(key)
            # Keep the EARLIEST due time so an idle scope is not perpetually deferred.
            if existing is None or int(due_ms) < int(existing[1]):
                registry[key] = (scope or {}, int(due_ms))
            if len(registry) > STREAM_MATERIALIZE_MAX_SCOPES:
                oldest = min(registry, key=lambda k: registry[k][1])
                registry.pop(oldest, None)

    def _stream_materialize_loop(self) -> None:
        while not self._stream_materialize_stop.wait(self._stream_materialize_interval_s):
            try:
                now = now_ms()
                with self._stream_materialize_registry_lock:
                    due_keys = [k for k, (_scope, due_ms) in self._stream_materialize_registry.items() if due_ms <= now]
                    due_scopes = [self._stream_materialize_registry.pop(k)[0] for k in due_keys]
                if not due_scopes:
                    continue
                started_perf = time.perf_counter()
                result = flush_due_scopes(self.adapter, due_scopes)
                self.metrics.observe_operation("stream_materialize", "ok", (time.perf_counter() - started_perf) * 1000.0)
                committed = int(result.get("committed_event_count") or 0)
                if committed:
                    self.access.append_audit(
                        "context.stream_materialize.background",
                        {"account_id": "system", "tenant_id": "system", "user_id": "stream_materializer"},
                        status="ok",
                        details={
                            "committed_event_count": committed,
                            "due_scope_count": int(result.get("due_scope_count") or 0),
                            "interval_ms": STREAM_MATERIALIZE_INTERVAL_MS,
                        },
                    )
            except Exception as exc:
                self.metrics.observe_operation("stream_materialize", "error", 0.0, timeout=is_retryable_temporalstore_error(exc))
                _mcp_debug_log(f"matrixark stream materialize loop failed: {exc}")

    def close(self, *, timeout_s: float = 5.0) -> None:
        self._summary_stop.set()
        self._stream_materialize_stop.set()
        if self._summary_thread is not None:
            self._summary_thread.join(timeout=max(0.0, timeout_s))
        if self._stream_materialize_thread is not None:
            self._stream_materialize_thread.join(timeout=max(0.0, timeout_s))
        adapter_close = getattr(self.adapter, "close", None)
        if callable(adapter_close):
            adapter_close(timeout_s=timeout_s)
        self._audit_executor.shutdown(wait=False, cancel_futures=False)

    def append_audit_policy(self, action: str, identity: Json, *, status: str, details: Json | None = None, args: Json | None = None, hot_path: bool = False) -> None:
        mode = str((args or {}).get("audit_mode") or self._audit_mode_default).strip().lower()
        if mode in {"off", "none", "disabled"}:
            return
        if hot_path and mode not in {"full", "sync", "synchronous"}:
            def write_audit() -> None:
                try:
                    self.access.append_audit(action, identity, status=status, details=details)
                except Exception as exc:
                    setattr(self.adapter, "_audit_flush_failures", int(getattr(self.adapter, "_audit_flush_failures", 0) or 0) + 1)
                    _mcp_debug_log(f"async audit write failed action={action}: {exc}")

            self._audit_executor.submit(write_audit)
            return
        self.access.append_audit(action, identity, status=status, details=details)

    def _backend_storage_mode_from_metrics(self, result: Json) -> str:
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        return str(metrics.get("mode") or result.get("gateway_mode") or metrics.get("audit_mode") or "unknown")

    def _refresh_service_metric_gauges(self) -> None:
        now = now_ms()
        dirty_lag_ms = 0
        import_lag_ms = 0
        try:
            backend_label = str(getattr(self.adapter, "_backend_label", lambda: "local")())
            if python_hot_cache_allowed(backend_label=backend_label):
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
            else:
                _mcp_debug_log("matrixark metrics gauge refresh skipped Python read_all in thin native profile")
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
        audit_buffer = getattr(self.adapter, "_audit_buffer", [])
        try:
            audit_queue_depth = len(audit_buffer)
        except Exception:
            audit_queue_depth = 0
        self.metrics.update_gauges(
            dirty_summary_lag_ms=dirty_lag_ms,
            resource_import_lag_ms=import_lag_ms,
            queue_depth=queue_depth,
            audit_write_failures=audit_write_failures,
            audit_queue_depth=audit_queue_depth,
        )

    def _merge_service_prometheus(self, result: Json) -> Json:
        self._refresh_service_metric_gauges()
        raw_backend = str(result.get("backend") or getattr(self.adapter, "_backend_label", lambda: "local")())
        backend = {
            "temporalstore-native": "native",
            "temporalstore-direct": "native",
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
            _mcp_debug_log(traceback.format_exc())
            return self.error_response(request_id, -32603, "internal MatrixArk MCP server error", data={"error_type": exc.__class__.__name__})

    def _call_tool_dispatch(self, name: str, args: Json, hook: Json | None, identity: Json, request_deadline_ms: int) -> Json:
        return dispatch_matrixark_tool(self, name, args, hook, identity, request_deadline_ms)

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


def python_hot_cache_allowed(*, backend_label: str = "") -> bool:
    configured = os.environ.get("MATRIXARK_ALLOW_PYTHON_HOT_CACHE", "").strip().lower()
    if configured:
        return configured in {"1", "true", "yes"}
    return backend_label == "local"


def backend_ready_required(backend: str) -> bool:
    if MATRIXARK_REQUIRE_BACKEND_READY:
        return MATRIXARK_REQUIRE_BACKEND_READY in {"1", "true", "yes"}
    return production_profile_enabled() and backend in {"temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"}


def native_context_pack_required(backend: str) -> bool:
    if MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK:
        return MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK in {"1", "true", "yes"}
    return backend in {"temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"}


def native_candidate_prefilter_required_for_backend(backend: str) -> bool:
    if backend not in {"temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"}:
        return False
    configured = os.environ.get("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", "").strip().lower()
    if configured:
        return configured in {"1", "true", "yes"}
    return True


def default_mcp_backend() -> str:
    configured = os.environ.get("MATRIXARK_MCP_BACKEND")
    if configured:
        return configured
    return "temporalstore-direct"


def validate_mcp_backend_policy(args: argparse.Namespace) -> None:
    if args.backend not in {"temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"}:
        raise MatrixArkError(
            "MatrixArk MCP no longer supports local JSONL serving backends; "
            "use --backend temporalstore-direct, --backend temporalstore-rust, or --backend temporalstore-rust-direct."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    add_backend_arguments(parser)
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
    args = parser.parse_args()
    if getattr(args, "rust_direct_lib", ""):
        os.environ["MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB"] = args.rust_direct_lib
    _mcp_debug_log(f"main: parsed backend={args.backend} metaserver={args.metaserver}")
    adapter = build_mcp_adapter(args)
    ensure_startup_backend_ready(adapter, args.backend)
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
