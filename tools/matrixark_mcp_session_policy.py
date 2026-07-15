#!/usr/bin/env python3
"""Session buffer and context node path policy helpers for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, local_account_user_id, optional_object
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, local_account_user_id, optional_object


def session_buffer_enabled(args: Json, *, kind: str = "message") -> bool:
    if kind not in {"message", "business_data", "feedback"}:
        return bool(args.get("session_buffer_enabled", False) or args.get("auto_batch_extract", False))
    if args.get("session_buffer_enabled") is False or args.get("auto_batch_extract") is False:
        return False
    return True


def auto_batch_extract_enabled(args: Json, *, kind: str = "message") -> bool:
    if kind not in {"message", "business_data", "feedback"}:
        return bool(args.get("auto_batch_extract", False))
    if args.get("auto_batch_extract") is False or args.get("session_buffer_enabled") is False:
        return False
    return True


def session_boundary_commit_requested(args: Json, *, hook: Json | None = None) -> bool:
    boundary_fields = ["flush_session_buffer", "conversation_done", "session_done", "task_complete"]
    if any(bool(args.get(field, False)) for field in boundary_fields):
        return True
    metadata = optional_object(args, "metadata")
    lifecycle_event = (
        args.get("lifecycle_event_type")
        or args.get("event_type")
        or args.get("event")
        or metadata.get("lifecycle_event_type")
        or metadata.get("event_type")
        or (hook or {}).get("event")
        or (hook or {}).get("event_type")
        or ""
    )
    normalized_event = "".join(ch for ch in str(lifecycle_event).lower() if ch.isalnum())
    return normalized_event in {
        "stop",
        "subagentstop",
        "postcompact",
        "precompact",
        "compact",
        "sessionend",
        "conversationend",
        "conversationdone",
        "sessiondone",
        "taskcomplete",
    }


def default_session_node_path(scope: Json) -> list[str]:
    tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
    user_id = str(scope.get("user_id") or local_account_user_id())
    session_id = str(scope.get("session_id") or user_id or "default_session")
    return [f"tenant:{tenant_id}", f"user:{user_id}", f"session:{session_id}"]


def default_shared_context_node_path(scope: Json, *, kind: str, sharing_scope: str) -> list[str]:
    collection = "skills" if kind == "skill" else "resources"
    if sharing_scope == "global_shared":
        return ["global", "shared", collection]
    tenant_id = str(scope.get("tenant_id") or "tenant_local_agent")
    return [f"tenant:{tenant_id}", "shared", collection]


def resource_sharing_scope(args: Json, envelope: Json, deployment_scope: str) -> str:
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


def default_resource_node_path(args: Json, envelope: Json, *, deployment_scope: str, sharing_scope: str) -> list[str]:
    metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
    if metadata.get("node_path"):
        return [str(part) for part in metadata.get("node_path", []) if str(part)]
    if sharing_scope in {"tenant_shared", "global_shared"}:
        return default_shared_context_node_path(
            envelope.get("scope", {}),
            kind=str(envelope.get("kind") or "resource"),
            sharing_scope=sharing_scope,
        )
    return default_session_node_path(envelope.get("scope", {}))
