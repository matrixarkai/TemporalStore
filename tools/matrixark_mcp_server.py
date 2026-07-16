#!/usr/bin/env python3
"""MatrixArk MCP server entrypoint and compatibility re-export layer."""

from __future__ import annotations

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
        MatrixArkError,
        _mcp_debug_log,
        enrich_scope_with_identity,
        is_retryable_temporalstore_error,
        json_text,
        stable_hash,
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
    from tools.matrixark_mcp_schemas import TOOLS
    from tools.matrixark_mcp_server_metrics import MatrixArkServerMetricsMixin
    from tools.matrixark_mcp_server_request_policy import (
        MatrixArkBackpressureError,
        MatrixArkServerRequestPolicyMixin,
        native_context_pack_required,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MATRIXARK_ALLOW_LOCAL_BACKEND,
        MATRIXARK_MCP_PROFILE,
        MATRIXARK_REQUIRE_BACKEND_READY,
        MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
        SUMMARY_REFRESH_INTERVAL_MS,
        SUMMARY_REFRESH_LIMIT,
        Json,
        MatrixArkError,
        _mcp_debug_log,
        enrich_scope_with_identity,
        is_retryable_temporalstore_error,
        json_text,
        stable_hash,
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
    from matrixark_mcp_schemas import TOOLS
    from matrixark_mcp_server_metrics import MatrixArkServerMetricsMixin
    from matrixark_mcp_server_request_policy import (
        MatrixArkBackpressureError,
        MatrixArkServerRequestPolicyMixin,
        native_context_pack_required,
    )


__all__ = [
    "MatrixArkBackpressureError",
    "MatrixArkMcpServer",
    "MatrixArkLocalAdapter",
    "MatrixArkRustCliClient",
    "MatrixArkTemporalStoreDirectAdapter",
    "MatrixArkTemporalStoreRustAdapter",
    "backend_ready_required",
    "default_mcp_backend",
    "enrich_scope_with_identity",
    "main",
    "native_context_pack_required",
    "production_profile_enabled",
    "validate_mcp_backend_policy",
]


try:
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
    from tools.matrixark_mcp_local_adapter import MatrixArkLocalAdapter
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
    from matrixark_mcp_temporal_adapters import (
        MatrixArkRustCliClient,
        MatrixArkRustProxyClient,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )


class MatrixArkMcpServer(MatrixArkServerRequestPolicyMixin, MatrixArkServerMetricsMixin):
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
    }
    SCOPED_READ_TOOLS = {"matrixark_retrieve", "matrixark_replay", "matrixark_management_portal", "matrixark_ingestion_dashboard", "matrixark_list_resources", "matrixark_list_skills"}
    SERVER_NAME = "matrixark-context"
    SERVER_VERSION = "0.2.0"
    DEFAULT_PROTOCOL_VERSION = "2025-06-18"
    DEFAULT_REQUEST_DEADLINES_MS = {
        "matrixark_ingest": int(os.environ.get("MATRIXARK_INGEST_TIMEOUT_MS", "30000")),
        "matrixark_retrieve": int(os.environ.get("MATRIXARK_RETRIEVE_TIMEOUT_MS", os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "5000"))),
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
        self._operation_backpressure_timeout_ms = max(0, int(os.environ.get("MATRIXARK_BACKPRESSURE_TIMEOUT_MS", "100")))
        self._retrieve_shed_cooldown_ms = max(0, int(os.environ.get("MATRIXARK_RETRIEVE_SHED_COOLDOWN_MS", "0")))
        self._retrieve_shed_until_perf = 0.0
        self._retrieve_shed_lock = threading.Lock()
        self._audit_mode_default = os.environ.get("MATRIXARK_AUDIT_MODE", "async").strip().lower() or "async"
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

    def _summary_refresh_loop(self) -> None:
        while not self._summary_stop.wait(self._summary_refresh_interval_s):
            try:
                started_perf = time.perf_counter()
                result = self.adapter.refresh_summaries({"scope": {}, "limit": self._summary_refresh_limit})
                self.metrics.observe_operation("summary_refresh", "ok", (time.perf_counter() - started_perf) * 1000.0)
                refreshed_count = int(result.get("refreshed_count") or 0)
                if refreshed_count:
                    self.access.append_audit("context.refresh_summaries.background", {"account_id": "system", "tenant_id": "system", "user_id": "summary_worker"}, status="ok", details={"refreshed_count": refreshed_count, "interval_ms": SUMMARY_REFRESH_INTERVAL_MS, "limit": self._summary_refresh_limit})
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

    def call_tool(self, name: str, args: Json) -> Json:
        if not isinstance(name, str) or not name:
            raise MatrixArkError("tool name must be a non-empty string")
        if not isinstance(args, dict):
            raise MatrixArkError("tool arguments must be an object")
        args = normalize_mcp_tool_request(name, args, write_tools=self.IDEMPOTENT_WRITE_TOOLS)
        request_deadline_ms = self._request_deadline_ms(name, args)
        hook = args.pop("agent_hook", None)
        identity = self.access.authorize_and_enrich(name, args)
        self._enforce_scope_before_output(name, args, identity)
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
                result["partial_context_pack"] = True
                result["backpressure"] = True
                self.metrics.observe_operation("retrieve", "ok", elapsed_ms, timeout=True)
                self.metrics.observe_retrieve_result(result)
                self.append_audit_policy(
                    "context.retrieve",
                    identity,
                    status="backpressure_partial",
                    details={"context_pack_id": result.get("context_pack_id"), "request_deadline_ms": request_deadline_ms},
                    args=args,
                    hot_path=True,
                )
                return self._retrieve_response(result, args, request_deadline_ms=request_deadline_ms, elapsed_ms=elapsed_ms)
            raise MatrixArkError(str(exc))

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


def main() -> int:
    try:
        from tools.matrixark_mcp_cli import main as cli_main
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_cli import main as cli_main
    return cli_main()


if __name__ == "__main__":
    raise SystemExit(main())
