#!/usr/bin/env python3
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

from contextlib import contextmanager

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import _mcp_debug_log
    from tools.matrixark_access import MatrixArkAccessManager
    from tools.matrixark_http import make_matrixark_http_handler
    from tools.matrixark_mcp_schemas import TOOLS
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import _mcp_debug_log
    from matrixark_access import MatrixArkAccessManager
    from matrixark_http import make_matrixark_http_handler
    from matrixark_mcp_schemas import TOOLS


try:
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from tools.matrixark_mcp_temporal_adapters import (
        MatrixArkRustCliClient,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_metrics import MatrixArkServiceMetrics
    from matrixark_mcp_local_adapter import MatrixArkLocalAdapter
    from matrixark_mcp_temporal_adapters import (
        MatrixArkRustCliClient,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )


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
        self._summary_stop = threading.Event()
        self._summary_thread: threading.Thread | None = None
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
        self._summary_stop.clear()
        self._summary_thread = threading.Thread(target=self._summary_refresh_loop, name="matrixark-summary-refresher", daemon=True)
        self._summary_thread.start()
        _mcp_debug_log(
            f"matrixark summary refresher started interval_ms={SUMMARY_REFRESH_INTERVAL_MS} limit={self._summary_refresh_limit}"
        )

    def _summary_refresh_loop(self) -> None:
        while not self._summary_stop.wait(self._summary_refresh_interval_s):
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

    def close(self, *, timeout_s: float = 5.0) -> None:
        self._summary_stop.set()
        if self._summary_thread is not None:
            self._summary_thread.join(timeout=max(0.0, timeout_s))
        adapter_close = getattr(self.adapter, "close", None)
        if callable(adapter_close):
            adapter_close(timeout_s=timeout_s)

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
