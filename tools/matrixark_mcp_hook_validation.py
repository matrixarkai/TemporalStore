#!/usr/bin/env python3
"""Agent hook validation helpers for MatrixArk MCP."""

from __future__ import annotations

import time
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_validation import require_string
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_validation import require_string


Json = dict[str, Any]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_observed_at_ms(value: Any) -> int:
    if isinstance(value, bool):
        raise MatrixArkError("agent_hook.observed_at_ms must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    raise MatrixArkError("agent_hook.observed_at_ms must be an integer")


def validate_hook(hook: Json | None) -> Json | None:
    if hook is None:
        return None
    if not isinstance(hook, dict):
        raise MatrixArkError("agent_hook must be an object")
    hook_type = str(hook.get("hook_type") or "").strip()
    if hook_type and hook_type not in {
        "before_llm",
        "after_llm",
        "tool_result",
        "resource_added",
        "feedback",
        "session_commit",
    }:
        raise MatrixArkError("agent_hook.hook_type is invalid")
    if "codex_event" in hook and not isinstance(hook["codex_event"], str):
        raise MatrixArkError("agent_hook.codex_event must be a string")
    if "trigger" in hook and not isinstance(hook["trigger"], str):
        raise MatrixArkError("agent_hook.trigger must be a string")
    require_string(hook, "source")
    require_string(hook, "hook_id")
    normalized = dict(hook)
    observed_at_ms = normalized.get("observed_at_ms")
    normalized["observed_at_ms"] = _now_ms() if observed_at_ms is None else _normalize_observed_at_ms(observed_at_ms)
    normalized.setdefault("auto_captured", True)
    if not isinstance(normalized.get("auto_captured"), bool):
        raise MatrixArkError("agent_hook.auto_captured must be a boolean")
    if "idempotency_key" in normalized and not isinstance(normalized["idempotency_key"], str):
        raise MatrixArkError("agent_hook.idempotency_key must be a string")
    return normalized
