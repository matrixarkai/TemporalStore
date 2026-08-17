#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Shared request validation helpers for MatrixArk MCP modules."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


Json = dict[str, Any]


def require_string(data: Json, field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value:
        raise MatrixArkError(f"{field} must be a non-empty string")
    return value


def require_messages(data: Json) -> list[Json]:
    messages = data.get("messages")
    if not isinstance(messages, list) or not messages:
        raise MatrixArkError("messages must be a non-empty list")
    for message in messages:
        if not isinstance(message, dict):
            raise MatrixArkError("messages entries must be objects")
        role = message.get("role")
        content = message.get("content")
        if role not in {"user", "assistant", "tool", "system"}:
            raise MatrixArkError("message role must be user, assistant, tool, or system")
        if not isinstance(content, str) or not content:
            raise MatrixArkError("message content must be a non-empty string")
    return messages


def optional_object(data: Json, field: str) -> Json:
    value = data.get(field, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise MatrixArkError(f"{field} must be an object")
    return value


def _nonempty(value: Any) -> bool:
    """True when ``value`` carries a meaningful identity token (non-blank)."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return bool(str(value).strip())


def fold_mem0_scope_aliases(args: Json, scope: Json) -> Json:
    """Fold mem0-style top-level identity kwargs into a canonical MatrixArk scope.

    mem0 (``from mem0 import Memory``) passes memory identity as TOP-LEVEL kwargs
    on ``add()`` / ``search()`` (``user_id`` / ``agent_id`` / ``run_id``) rather
    than nested under a scope object. This folds those aliases onto the canonical
    scope fields so a mem0 caller needs no rewrite:

      * top-level ``user_id``                      -> ``scope.user_id``
      * top-level ``run_id`` / ``scope.run_id``    -> ``scope.session_id``
      * top-level ``agent_id`` / ``scope.agent_id`` -> ``scope.agent_id``

    Precedence: the CANONICAL scope field ALWAYS wins when BOTH it and an alias
    are supplied (e.g. an explicit ``scope.session_id`` beats ``run_id``). A
    nested ``scope.run_id`` is preferred over a top-level ``run_id`` alias.

    Returns a NEW scope dict; ``scope`` is never mutated. Existing callers that
    pass a full canonical scope and no aliases get a byte-identical scope back,
    so this is purely additive.
    """
    if scope is None:
        scope = {}
    if not isinstance(scope, dict):
        raise MatrixArkError("scope must be an object")
    folded = dict(scope)
    # user_id: canonical scope.user_id wins over the top-level user_id alias.
    if not _nonempty(folded.get("user_id")) and _nonempty(args.get("user_id")):
        folded["user_id"] = args["user_id"]
    # run_id -> session_id: canonical scope.session_id wins; then scope.run_id,
    # then the top-level run_id alias.
    if not _nonempty(folded.get("session_id")):
        if _nonempty(folded.get("run_id")):
            folded["session_id"] = folded["run_id"]
        elif _nonempty(args.get("run_id")):
            folded["session_id"] = args["run_id"]
    # agent_id: canonical scope.agent_id wins over the top-level agent_id alias.
    if not _nonempty(folded.get("agent_id")) and _nonempty(args.get("agent_id")):
        folded["agent_id"] = args["agent_id"]
    return folded


def optional_string(data: Json, field: str, default: str = "") -> str:
    value = data.get(field, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise MatrixArkError(f"{field} must be a string")
    return value


def optional_string_list(data: Json, field: str, default: list[str] | None = None) -> list[str]:
    value = data.get(field, default or [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise MatrixArkError(f"{field} must be a list of strings")
    return list(value)


def integer_arg(data: Json, field: str, default: int, *, minimum: int = 0) -> int:
    value = data.get(field, default)
    if not isinstance(value, int):
        raise MatrixArkError(f"{field} must be an integer")
    if value < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    return value


def float_arg(data: Json, field: str, default: float, *, minimum: float = 0.0, maximum: float | None = None) -> float:
    value = data.get(field, default)
    if not isinstance(value, (int, float)):
        raise MatrixArkError(f"{field} must be a number")
    result = float(value)
    if result < minimum:
        raise MatrixArkError(f"{field} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise MatrixArkError(f"{field} must be <= {maximum}")
    return result
