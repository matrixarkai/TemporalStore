#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Envelope and session key helpers for MatrixArk MCP."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_validation import optional_object
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_validation import optional_object


Json = dict[str, Any]


def has_confirmation_context(envelope: Json) -> bool:
    metadata = optional_object(envelope, "metadata")
    return bool(
        envelope.get("context_pack_id")
        or metadata.get("reply_to_context_pack_id")
        or envelope.get("accepted_refs")
        or envelope.get("rejected_refs")
    )


def explicit_context_pack_id(envelope: Json) -> str:
    metadata = optional_object(envelope, "metadata")
    value = envelope.get("context_pack_id") or metadata.get("reply_to_context_pack_id") or ""
    return str(value) if value else ""


def session_key(envelope: Json) -> tuple[Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
    )


def user_key(envelope: Json) -> tuple[Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("team", ""),
    )


def context_node_key(envelope: Json) -> tuple[Any, Any, Any, Any]:
    scope = envelope.get("scope", {})
    return (
        scope.get("user_id", ""),
        scope.get("session_id", ""),
        scope.get("team", ""),
        scope.get("project", ""),
    )


def session_buffer_key_from_scope(scope: Json) -> tuple[str, str, str, str]:
    user_id = str(scope.get("user_id") or "")
    session_id = str(scope.get("session_id") or user_id or "")
    return (
        str(scope.get("account_id") or "acct_local"),
        str(scope.get("tenant_id") or "tenant_local_agent"),
        user_id,
        session_id,
    )


def session_buffer_key(envelope: Json) -> tuple[str, str, str, str]:
    return session_buffer_key_from_scope(envelope.get("scope", {}))
