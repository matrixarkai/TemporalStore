#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local idempotency records for the MatrixArk MCP adapter."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_identity import Json, now_ms, stable_hash
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_identity import Json, now_ms, stable_hash


def find_idempotency_record(adapter: object, key_hash: int) -> Json | None:
    for record in reversed(adapter.read_all()):
        if record.get("record_type") == "matrixark_idempotency" and record.get("key_hash") == key_hash:
            return record
    return None


def append_idempotency_record(
    adapter: object,
    *,
    key_hash: int,
    tool_name: str,
    raw_key: str,
    identity: Json,
    response: Json,
) -> None:
    adapter.append(
        {
            "record_type": "matrixark_idempotency",
            "key_hash": key_hash,
            "tool_name": tool_name,
            "raw_key_hash": stable_hash(raw_key),
            "scope_key": identity.get("scope_key", ""),
            "account_id": identity.get("account_id", ""),
            "tenant_id": identity.get("tenant_id", ""),
            "user_id": identity.get("user_id", ""),
            "session_id": identity.get("session_id", ""),
            "response": response,
            "created_at_ms": now_ms(),
        }
    )
