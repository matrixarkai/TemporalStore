#!/usr/bin/env python3
"""Request policy helpers for the MatrixArk MCP server."""

from __future__ import annotations

from contextlib import contextmanager
import os
import time

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_MAX_CONTEXT_TOKENS,
        MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
        Json,
        MatrixArkError,
        compact_context_pack_for_serving,
        compact_context_pack_for_serving_flat,
        infer_query_type,
        local_context_budget,
        optional_object,
        require_string,
        stable_hash,
    )
    from tools.matrixark_mcp_admin import is_admin_tool
    from tools.matrixark_mcp_ingestion import is_ingestion_tool
    from tools.matrixark_mcp_native_pack_policy import native_context_pack_required_for_backend
    from tools.matrixark_mcp_requests import normalize_mcp_tool_request
    from tools.matrixark_mcp_retrieval import is_retrieval_tool
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_MAX_CONTEXT_TOKENS,
        MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
        Json,
        MatrixArkError,
        compact_context_pack_for_serving,
        compact_context_pack_for_serving_flat,
        infer_query_type,
        local_context_budget,
        optional_object,
        require_string,
        stable_hash,
    )
    from matrixark_mcp_admin import is_admin_tool
    from matrixark_mcp_ingestion import is_ingestion_tool
    from matrixark_mcp_native_pack_policy import native_context_pack_required_for_backend
    from matrixark_mcp_requests import normalize_mcp_tool_request
    from matrixark_mcp_retrieval import is_retrieval_tool


class MatrixArkBackpressureError(MatrixArkError):
    pass


def native_context_pack_required(backend_label: str) -> bool:
    return native_context_pack_required_for_backend(
        backend_label,
        require_flag=MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK,
    )


class MatrixArkServerRequestPolicyMixin:
    """Idempotency, scoping, deadlines, and backpressure for MCP tools."""

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

    @staticmethod
    def _serving_access(args: Json) -> Json:
        auth = args.get("_matrixark_auth", {})
        if not isinstance(auth, dict):
            return {}
        fields = [
            "account_id",
            "tenant_id",
            "user_id",
            "session_id",
            "agent_name",
            "api_key_id",
            "role",
            "mode",
        ]
        return {field: auth.get(field) for field in fields if auth.get(field) not in (None, "", [], {})}

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

    def _enforce_scope_before_output(self, name: str, args: Json, identity: Json) -> None:
        if name not in self.SCOPED_READ_TOOLS:
            return
        scope = optional_object(args, "scope")
        account_id = str(scope.get("account_id") or identity.get("account_id") or "")
        tenant_id = str(scope.get("tenant_id") or identity.get("tenant_id") or "")
        if not account_id or not tenant_id:
            raise MatrixArkError("scoped read requires resolved account_id and tenant_id")
        self.access.ensure_identity_can_read_scope(identity, account_id, tenant_id, scope)

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
        default_deadline = self.DEFAULT_REQUEST_DEADLINES_MS.get(name)
        if default_deadline is None and is_admin_tool(name):
            default_deadline = self.DEFAULT_REQUEST_DEADLINES_MS["matrixark_admin"]
        raw_value = args.get("request_deadline_ms", args.get("timeout_ms", default_deadline or 0))
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
        max_context_tokens = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        if not isinstance(max_context_tokens, int) or max_context_tokens <= 0:
            max_context_tokens = DEFAULT_MAX_CONTEXT_TOKENS
        record_limit = int(os.environ.get("MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT", "0"))
        backend_label = str(getattr(self.adapter, "_backend_label", lambda: "local")())
        pack_required = native_context_pack_required(backend_label)
        if reason == "service_backpressure":
            if pack_required or record_limit <= 0:
                records = []
            elif hasattr(self.adapter, "recent_records"):
                records = self.adapter.recent_records(record_limit)
            else:
                records = self.adapter.read_all()[-record_limit:]
        else:
            records = [] if pack_required else self.adapter.read_all()
        return self.adapter.deadline_fallback_pack(
            query=query,
            scope=optional_object(args, "scope"),
            question_type=str(args.get("question_type") or infer_query_type(query)),
            max_context_tokens=max_context_tokens,
            local_budget=local_context_budget(args),
            deadline_ms=deadline_ms,
            elapsed_ms=round(float(elapsed_ms), 3),
            records=records,
            reason=reason,
            budget_source="agent_provided_max_context_tokens"
            if "max_context_tokens" in args
            else "matrixark_default_max_context_tokens",
        )

    def _operation_group(self, name: str) -> str:
        if is_ingestion_tool(name):
            return "ingest"
        if is_retrieval_tool(name):
            return "retrieve"
        if name == "matrixark_feedback":
            return "feedback"
        if name == "matrixark_replay":
            return "replay"
        if is_admin_tool(name):
            return "admin"
        return ""

    def _retrieve_response(self, result: Json, args: Json, *, request_deadline_ms: int, elapsed_ms: float) -> Json:
        if bool(args.get("debug_context_pack")) or bool(args.get("include_retrieval_debug")):
            return {
                **result,
                "access": args.get("_matrixark_auth", {}),
                "request_deadline_ms": request_deadline_ms,
                "request_elapsed_ms": round(elapsed_ms, 3),
            }
        if str(args.get("audit_mode") or "").strip().lower() in {"full", "sync", "synchronous"}:
            return compact_context_pack_for_serving_flat(result)
        return compact_context_pack_for_serving(result)

    @contextmanager
    def _operation_slot(self, name: str, request_deadline_ms: int):
        group = self._operation_group(name)
        limiter = self._operation_limiters.get(group) if group else None
        if limiter is None:
            yield
            return
        capacity = int(self.DEFAULT_OPERATION_CONCURRENCY.get(group, 0) or 0)
        wait_ms = self._operation_backpressure_timeout_ms
        if request_deadline_ms > 0:
            wait_ms = min(wait_ms, request_deadline_ms)
        started = time.perf_counter()
        if group == "retrieve" and self._retrieve_shed_cooldown_ms > 0:
            with self._retrieve_shed_lock:
                now_perf = time.perf_counter()
                if now_perf < self._retrieve_shed_until_perf:
                    self.metrics.observe_backpressure(name)
                    raise MatrixArkBackpressureError("matrixark_retrieve rejected by adaptive retrieve shed cooldown")
                acquired = limiter.acquire(blocking=False)
        else:
            acquired = (
                limiter.acquire(timeout=max(0.0, wait_ms / 1000.0))
                if wait_ms > 0
                else limiter.acquire(blocking=False)
            )
        if not acquired:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            self.metrics.observe_pool_wait(group, elapsed_ms, capacity=capacity)
            if group == "retrieve" and self._retrieve_shed_cooldown_ms > 0:
                with self._retrieve_shed_lock:
                    self._retrieve_shed_until_perf = max(
                        self._retrieve_shed_until_perf,
                        time.perf_counter() + self._retrieve_shed_cooldown_ms / 1000.0,
                    )
            self.metrics.observe_backpressure(name)
            raise MatrixArkBackpressureError(f"{name} rejected by service backpressure after {round(elapsed_ms, 3)}ms")
        self.metrics.observe_pool_wait(group, (time.perf_counter() - started) * 1000.0, capacity=capacity)
        self.metrics.observe_pool_acquire(group, capacity=capacity)
        try:
            yield
        finally:
            limiter.release()
            self.metrics.observe_pool_release(group)

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
                    details={
                        "context_pack_id": result.get("context_pack_id"),
                        "request_deadline_ms": request_deadline_ms,
                    },
                    args=args,
                    hot_path=True,
                )
                return self._retrieve_response(
                    result,
                    args,
                    request_deadline_ms=request_deadline_ms,
                    elapsed_ms=elapsed_ms,
                )
            raise MatrixArkError(str(exc))
