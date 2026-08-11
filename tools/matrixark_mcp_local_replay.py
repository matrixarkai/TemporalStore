#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local compact replay helpers for MatrixArk MCP."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_context_pack import compact_context_pack_audit_record
    from tools.matrixark_mcp_core import (
        AUDIT_DEBUG_PAYLOAD,
        CONTEXT_PACK_DEBUG_REFS,
        ENABLE_CONTEXT_REPLAY,
        Json,
    )
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_validation import require_string
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_context_pack import compact_context_pack_audit_record
    from matrixark_mcp_core import (
        AUDIT_DEBUG_PAYLOAD,
        CONTEXT_PACK_DEBUG_REFS,
        ENABLE_CONTEXT_REPLAY,
        Json,
    )
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_validation import require_string


TELEMETRY_REPLAY_FIELDS = [
    "record_type",
    "context_pack_id",
    "query_hash",
    "question_type",
    "selected_ref_count",
    "selected_ref_counts",
    "dropped_ref_count",
    "dropped_ref_bucket_counts",
    "used_local_context_tokens",
    "used_remote_context_tokens",
    "total_prompt_context_tokens",
    "remote_context_budget_tokens",
    "partial_context_pack",
    "insufficient_context",
    "quality_warning_count",
    "primary_candidate_count",
    "auxiliary_candidate_count",
    "created_at_ms",
]

COMPACT_REPLAY_FIELDS = [
    "record_type",
    "context_pack_id",
    "source_ref_type",
    "source_ref_hash",
    "event_id_hash",
    "node_hash",
    "reinforced_at_ms",
    "protected_until_ms",
    "reason",
]


def compact_replay_record(record: Json) -> Json:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_pack_audit":
        return compact_context_pack_audit_record(record)
    if record_type == "context_pack_telemetry":
        return {
            key: record.get(key)
            for key in TELEMETRY_REPLAY_FIELDS
            if record.get(key) not in (None, "", [], {})
        }
    return {
        key: record.get(key)
        for key in COMPACT_REPLAY_FIELDS
        if record.get(key) not in (None, "", [], {})
    }


def replay(adapter: object, args: Json) -> Json:
    if not (ENABLE_CONTEXT_REPLAY or bool(args.get("enable_replay"))):
        raise MatrixArkError(
            "context replay is disabled; set MATRIXARK_ENABLE_REPLAY=1 or pass enable_replay=true for explicit debug runs"
        )
    context_pack_id = require_string(args, "context_pack_id")
    include_debug = bool(
        args.get("include_debug_records")
        or args.get("include_debug_refs")
        or CONTEXT_PACK_DEBUG_REFS
        or AUDIT_DEBUG_PAYLOAD
    )
    adapter.flush_audits()
    records = adapter.read_all()
    if include_debug:
        return {
            "context_pack_id": context_pack_id,
            "events": records,
            "replay_payload_policy": "debug_full_store_scan",
        }
    replay_records = [
        compact_replay_record(record)
        for record in records
        if str(record.get("context_pack_id") or "") == context_pack_id
    ]
    return {
        "context_pack_id": context_pack_id,
        "events": replay_records,
        "replay_payload_policy": "compact_context_pack_scope",
        "debug_records_available_with": "include_debug_records=true",
    }
