#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Common MatrixArk MCP JSON schema fragments."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json


MESSAGE_SCHEMA: Json = {
    "type": "object",
    "description": "One chat/tool/system message. Only role and content are required.",
    "required": ["role", "content"],
    "properties": {
        "role": {"type": "string", "enum": ["user", "assistant", "tool", "system"]},
        "content": {"type": "string", "minLength": 1},
        "name": {"type": "string", "description": "Optional human/tool/agent name."},
        "created_at_ms": {
            "type": "integer",
            "description": "Optional source event time. MatrixArk still uses server ingestion time as the primary write key.",
        },
    },
    "additionalProperties": True,
}

SCOPE_SCHEMA: Json = {
    "type": "object",
    "description": "Optional memory scope. Local mode defaults account_id to acct_local, tenant_id to the agent name, and user_id to the local OS account when omitted. Send user_id or session_id when the host agent knows them; both together give the best user and thread grouping.",
    "properties": {
        "account_id": {"type": "string"},
        "tenant_id": {"type": "string", "description": "Tenant/workspace id. In local mode this defaults to tenant_<agent_name>."},
        "agent_name": {"type": "string", "description": "Optional host agent name such as codex, claude, cursor, or local test. Used to derive the local tenant when tenant_id is omitted."},
        "user_id": {
            "type": "string",
            "description": "Optional user memory scope. In local mode this defaults to the local OS account. Useful alone, and stronger when paired with session_id.",
        },
        "session_id": {
            "type": "string",
            "description": "Optional thread/run/session scope. Useful alone, and strongest when paired with user_id.",
        },
        "team": {"type": "string"},
        "project": {"type": "string"},
    },
    "additionalProperties": True,
}

METADATA_SCHEMA: Json = {
    "type": "object",
    "description": "Optional routing and evidence hints. MatrixArk can infer or fill these internally.",
    "properties": {
        "source": {"type": "string", "description": "Optional source name such as cursor, codex, tool, or resource parser."},
        "node_path": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Optional tree/path hint. MatrixArk may choose its own internal ContextNode path.",
        },
        "reply_to_message_id": {"type": "string", "description": "Optional message linkage for feedback."},
        "reply_to_context_pack_id": {
            "type": "string",
            "description": "Optional ContextPack linkage for confirmation/correction inference.",
        },
    },
    "additionalProperties": True,
}
