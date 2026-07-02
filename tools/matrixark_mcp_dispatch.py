#!/usr/bin/env python3
"""Tool dispatch for MatrixArk MCP requests."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        adapter_ensure_backend_ready,
        is_retryable_temporalstore_error,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        adapter_ensure_backend_ready,
        is_retryable_temporalstore_error,
    )


__all__ = ["dispatch_matrixark_tool"]


def dispatch_matrixark_tool(server: Any, name: str, args: Json, hook: Json | None, identity: Json, request_deadline_ms: int) -> Json:
    if name == "matrixark_backend_ready":
        started_perf = time.perf_counter()
        try:
            result = adapter_ensure_backend_ready(
                server.adapter,
                reason=str(args.get("reason") or "manual"),
                probe=bool(args.get("probe", True)),
                timeout_ms=args.get("timeout_ms"),
            )
        except Exception as exc:
            server.metrics.observe_operation("backend_ready", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            server.metrics.observe_backend_ready(False, "error")
            raise
        status = "ok" if result.get("status") == "ready" else "topology_not_ready"
        server.metrics.observe_operation("backend_ready", "ok", (time.perf_counter() - started_perf) * 1000.0)
        server.metrics.observe_backend_ready(result.get("status") == "ready", str(result.get("status") or status))
        server.access.append_audit(
            "backend.ready",
            identity,
            status=status,
            details={"backend": result.get("backend"), "attempts": result.get("attempts")},
        )
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_backend_metrics":
        started_perf = time.perf_counter()
        try:
            result = server._merge_service_prometheus(server.adapter.backend_metrics())
        except Exception as exc:
            server.metrics.observe_operation("backend_metrics", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            raise
        server.metrics.observe_operation("backend_metrics", "ok", (time.perf_counter() - started_perf) * 1000.0)
        server.access.append_audit(
            "backend.metrics",
            identity,
            status="ok",
            details={"backend": result.get("backend"), "metrics_format": result.get("metrics_format")},
        )
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_ingest":
        started_perf = time.perf_counter()
        try:
            result = server.adapter.ingest(args, hook=hook)
        except Exception as exc:
            server.metrics.observe_operation("ingest", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            raise
        elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
        server.metrics.observe_operation("ingest", "ok", elapsed_ms, timeout=request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
        server._raise_if_request_timed_out(name, started_perf, request_deadline_ms)
        server.metrics.observe_ingest_result(result)
        server.access.append_audit("context.ingest", identity, status="ok", details={"event_id_hash": result.get("event_id_hash"), "request_deadline_ms": request_deadline_ms})
        response = {**result, "access": args.get("_matrixark_auth", {}), "request_deadline_ms": request_deadline_ms, "request_elapsed_ms": round(elapsed_ms, 3)}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_batch_extract":
        started_perf = time.perf_counter()
        try:
            result = server.adapter.batch_extract(args, hook=hook)
        except Exception as exc:
            server.metrics.observe_operation("batch_extract", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            raise
        server.metrics.observe_operation("batch_extract", "ok", (time.perf_counter() - started_perf) * 1000.0)
        server.access.append_audit("context.batch_extract", identity, status="ok", details={"batch_id_hash": result.get("batch_id_hash")})
        response = {**result, "access": args.get("_matrixark_auth", {})}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_session_commit":
        result = server.adapter.session_commit(args, hook=hook)
        server.access.append_audit("context.session_commit", identity, status="ok", details={"commit_id_hash": result.get("commit_id_hash"), "batch_id_hash": result.get("batch_id_hash")})
        response = {**result, "access": args.get("_matrixark_auth", {})}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_refresh_summaries":
        started_perf = time.perf_counter()
        try:
            result = server.adapter.refresh_summaries(args)
        except Exception as exc:
            server.metrics.observe_operation("summary_refresh", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            raise
        server.metrics.observe_operation("summary_refresh", "ok", (time.perf_counter() - started_perf) * 1000.0)
        server.access.append_audit("context.refresh_summaries", identity, status="ok", details={"refreshed_count": result.get("refreshed_count")})
        response = {**result, "access": args.get("_matrixark_auth", {})}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_retrieve":
        started_perf = time.perf_counter()
        effective_retrieve_deadline_ms = int(args.get("deadline_ms") or request_deadline_ms or 0)
        if effective_retrieve_deadline_ms > 0 and "deadline_ms" not in args:
            args["deadline_ms"] = effective_retrieve_deadline_ms
        try:
            result = server.adapter.retrieve(args)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
            timeout = is_retryable_temporalstore_error(exc) or (request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
            server.metrics.observe_operation("retrieve", "error", elapsed_ms, timeout=timeout)
            if timeout:
                result = server._retrieve_timeout_fallback(args, deadline_ms=effective_retrieve_deadline_ms or request_deadline_ms, elapsed_ms=elapsed_ms, reason="request_deadline_exception")
                result["quality_warnings"] = list(result.get("quality_warnings", [])) + ["request_deadline_exception"]
                result["request_deadline_ms"] = request_deadline_ms
                result["request_elapsed_ms"] = round(elapsed_ms, 3)
                result["partial_context_pack"] = True
                server.metrics.observe_operation("retrieve", "ok", elapsed_ms, timeout=True)
                server.metrics.observe_retrieve_result(result)
                server.access.append_audit("context.retrieve", identity, status="timeout_partial", details={"context_pack_id": result.get("context_pack_id"), "request_deadline_ms": request_deadline_ms})
                return server._retrieve_response(result, args, request_deadline_ms=request_deadline_ms, elapsed_ms=elapsed_ms)
            raise
        elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
        timeout = request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms
        if timeout and not (result.get("partial_context_pack") or result.get("partial")):
            result = server._retrieve_timeout_fallback(args, deadline_ms=effective_retrieve_deadline_ms or request_deadline_ms, elapsed_ms=elapsed_ms, reason="request_deadline_after_retrieve")
            result["quality_warnings"] = list(result.get("quality_warnings", [])) + ["request_deadline_after_retrieve"]
            result["partial_context_pack"] = True
        result["request_deadline_ms"] = request_deadline_ms
        result["request_elapsed_ms"] = round(elapsed_ms, 3)
        server.metrics.observe_operation("retrieve", "ok", elapsed_ms, timeout=timeout)
        server.metrics.observe_retrieve_result(result)
        server.access.append_audit("context.retrieve", identity, status="timeout_partial" if timeout else "ok", details={"context_pack_id": result.get("context_pack_id"), "request_deadline_ms": request_deadline_ms})
        return server._retrieve_response(result, args, request_deadline_ms=request_deadline_ms, elapsed_ms=elapsed_ms)
    if name == "matrixark_ingestion_dashboard":
        result = server.adapter.ingestion_dashboard(args)
        server.access.append_audit("context.ingestion_dashboard", identity, status="ok", details={"table": result.get("table"), "total": result.get("total")})
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_auth_signup":
        result = server.access.signup(args, identity)
        response = {**result, "access": args.get("_matrixark_auth", {})}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_auth_sso_login":
        result = server.access.sso_login(args, identity)
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_auth_sso_callback":
        result = server.access.sso_callback(args, identity)
        response = {**result, "access": args.get("_matrixark_auth", {})}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_management_portal":
        result = server.access.management_portal(args, identity)
        server.access.append_audit("admin.management_portal", identity, status="ok", details={"account_id": result.get("account_id"), "tenant_id": result.get("tenant_id")})
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_list_resources":
        result = server.adapter.list_resources(args)
        server.access.append_audit("resource.list", identity, status="ok", details={"count": result.get("count")})
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_list_skills":
        result = server.adapter.list_skills(args)
        server.access.append_audit("skill.list", identity, status="ok", details={"count": result.get("count")})
        return {**result, "access": args.get("_matrixark_auth", {})}
    if name == "matrixark_update_skill":
        result = server.adapter.update_skill(args)
        server.access.append_audit("skill.update", identity, status="ok", details={"skill_hash": result.get("skill_hash"), "skill_status": result.get("status")})
        response = {**result, "access": args.get("_matrixark_auth", {})}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_feedback":
        started_perf = time.perf_counter()
        try:
            result = server.adapter.feedback(args, hook=hook)
        except Exception as exc:
            server.metrics.observe_operation("feedback", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            raise
        elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
        server.metrics.observe_operation("feedback", "ok", elapsed_ms, timeout=request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
        server._raise_if_request_timed_out(name, started_perf, request_deadline_ms)
        server.access.append_audit("context.feedback", identity, status="ok", details={"event_id_hash": result.get("event_id_hash"), "request_deadline_ms": request_deadline_ms})
        response = {**result, "access": args.get("_matrixark_auth", {}), "request_deadline_ms": request_deadline_ms, "request_elapsed_ms": round(elapsed_ms, 3)}
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_replay":
        started_perf = time.perf_counter()
        try:
            result = server.adapter.replay(args)
        except Exception as exc:
            server.metrics.observe_operation("replay", "error", (time.perf_counter() - started_perf) * 1000.0, timeout=is_retryable_temporalstore_error(exc))
            raise
        elapsed_ms = (time.perf_counter() - started_perf) * 1000.0
        server.metrics.observe_operation("replay", "ok", elapsed_ms, timeout=request_deadline_ms > 0 and elapsed_ms >= request_deadline_ms)
        server._raise_if_request_timed_out(name, started_perf, request_deadline_ms)
        server.access.append_audit("context.replay", identity, status="ok", details={"context_pack_id": args.get("context_pack_id"), "request_deadline_ms": request_deadline_ms})
        return {**result, "access": args.get("_matrixark_auth", {}), "request_deadline_ms": request_deadline_ms, "request_elapsed_ms": round(elapsed_ms, 3)}
    if name == "matrixark_admin_create_account":
        response = server.access.create_account(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_update_account":
        response = server.access.update_account(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_list_accounts":
        return server.access.list_accounts(args, identity)
    if name == "matrixark_admin_create_user":
        response = server.access.create_user(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_update_user":
        response = server.access.update_user(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_list_users":
        return server.access.list_users(args, identity)
    if name == "matrixark_admin_create_api_key":
        response = server.access.create_api_key(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_apply_api_key":
        response = server.access.apply_api_key(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_list_api_keys":
        return server.access.list_api_keys(args, identity)
    if name == "matrixark_admin_rotate_api_key":
        response = server.access.rotate_api_key(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_revoke_api_key":
        response = server.access.revoke_api_key(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_map_sso_user":
        response = server.access.map_sso_user(args, identity)
        return server._finalize_write_response(name, args, identity, hook, response)
    if name == "matrixark_admin_audit":
        return server.access.audit(args, identity)
    raise MatrixArkError(f"unsupported tool {name!r}")
