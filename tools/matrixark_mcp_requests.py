#!/usr/bin/env python3
"""Request-boundary policy helpers for MatrixArk MCP/HTTP tools."""

from __future__ import annotations

import json
import secrets

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        local_identity_defaults,
        normalize_storage_options,
        now_ms,
        optional_object,
        stable_hash,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        local_identity_defaults,
        normalize_storage_options,
        now_ms,
        optional_object,
        stable_hash,
    )


__all__ = [
    "generated_idempotency_key",
    "normalize_request_scope",
    "normalize_mcp_tool_request",
]


def generated_idempotency_key(tool_name: str, args: Json) -> str:
    material = json.dumps(
        {
            "tool": tool_name,
            "scope": args.get("scope") if isinstance(args.get("scope"), dict) else {},
            "metadata": args.get("metadata") if isinstance(args.get("metadata"), dict) else {},
            "created_at_ms": now_ms(),
            "nonce": secrets.token_urlsafe(16),
        },
        sort_keys=True,
        default=str,
    )
    return f"auto:{tool_name}:{stable_hash(material)}"


def normalize_request_scope(args: Json) -> Json:
    scope = optional_object(args, "scope")
    # For API-key calls, keep the caller's requested scope intact. The access
    # manager validates it against the key and then enriches it exactly once.
    if args.get("api_key"):
        return scope
    defaults = local_identity_defaults(args, scope)
    normalized = dict(scope)
    for key in ("account_id", "tenant_id", "user_id", "session_id", "agent_name"):
        value = defaults.get(key)
        if value and key not in normalized:
            normalized[key] = value
    return normalized


def normalize_mcp_tool_request(tool_name: str, args: Json, *, write_tools: set[str]) -> Json:
    if not isinstance(args, dict):
        raise MatrixArkError("tool arguments must be an object")
    normalized = dict(args)
    normalized["scope"] = normalize_request_scope(normalized)
    if "storage_options" in normalized or any(str(key).startswith("temporalstore_") for key in normalized):
        normalized["storage_options"] = normalize_storage_options(normalized)
    if tool_name in write_tools and not normalized.get("idempotency_key"):
        hook = normalized.get("agent_hook") if isinstance(normalized.get("agent_hook"), dict) else {}
        hook_key = hook.get("idempotency_key") if isinstance(hook, dict) else ""
        if not hook_key:
            normalized["idempotency_key"] = generated_idempotency_key(tool_name, normalized)
    return normalized
