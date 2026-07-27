#!/usr/bin/env python3
"""Management dashboard row-shaping helpers for MatrixArk MCP."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        MatrixArkError,
        candidate_access_scope,
        non_default_classification,
        optional_object,
        optional_string,
        scope_from_serving_record,
        scope_matches,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        MatrixArkError,
        candidate_access_scope,
        non_default_classification,
        optional_object,
        optional_string,
        scope_from_serving_record,
        scope_matches,
    )


def dashboard_record_scope(record: Json) -> Json:
    scope = candidate_access_scope(record)
    access_scope = candidate_access_scope(record)
    if isinstance(scope, dict) and isinstance(access_scope, dict):
        merged = {**scope, **access_scope}
        if scope.get("agent_name") and not merged.get("agent_name"):
            merged["agent_name"] = scope["agent_name"]
        explicit = scope.get("_explicit_scope_keys")
        if isinstance(explicit, list):
            merged["_explicit_scope_keys"] = explicit
        return merged
    return access_scope


def dashboard_message_rows(records: list[Json], scope: Json) -> list[Json]:
    rows: list[Json] = []
    debug_by_ref: dict[Any, Json] = {}
    for record in records:
        if record.get("record_type") != "context_debug_record" or record.get("ref_type") != "event":
            continue
        debug_by_ref[record.get("ref_hash")] = record.get("debug_payload", {}) if isinstance(record.get("debug_payload"), dict) else {}
    for record in records:
        if record.get("record_type") != "context_event":
            continue
        if not scope_matches(dashboard_record_scope(record), scope):
            continue
        debug_payload = debug_by_ref.get(record.get("event_id_hash"), {})
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else debug_payload.get("envelope", {})
        if not isinstance(envelope, dict):
            envelope = {}
        kind = str(envelope.get("kind") or record.get("source_kind") or "")
        if kind not in {"message", "feedback", "business_data"}:
            continue
        messages = envelope.get("messages", []) if isinstance(envelope.get("messages"), list) else []
        if not messages and kind == "message":
            messages = [{"role": "unknown", "content": record.get("text", "")}]
        extraction = debug_payload.get("internal_extraction", {}) if isinstance(debug_payload.get("internal_extraction"), dict) else {}
        for message in messages:
            if not isinstance(message, dict):
                continue
            rows.append(
                {
                    "row_type": "message",
                    "event_id_hash": record.get("event_id_hash", 0),
                    "kind": kind,
                    "role": message.get("role", ""),
                    "name": message.get("name", ""),
                    "content": message.get("content", ""),
                    "summary_text": record.get("summary_text", ""),
                    "classification": non_default_classification(extraction.get("classification", "")),
                    "event_type": extraction.get("event_type", ""),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "scope": envelope.get("scope", scope_from_serving_record(record)),
                    "agent_name": envelope.get("scope", {}).get("agent_name", "") if isinstance(envelope.get("scope"), dict) else "",
                    "created_at_ms": message.get("created_at_ms") or envelope.get("ingestion_time_ms") or record.get("updated_at_ms", 0),
                }
            )
    return rows


def dashboard_rows_for_table(records: list[Json], table: str, scope: Json) -> list[Json]:
    rows: list[Json] = []
    if table == "messages":
        return dashboard_message_rows(records, scope)
    for record in records:
        record_type = str(record.get("record_type") or "")
        if not scope_matches(dashboard_record_scope(record), scope):
            continue
        if table == "resources" and record_type in {"resource_import_task", "resource_manifest", "resource_chunk"}:
            rows.append(
                {
                    "row_type": record_type,
                    "task_hash": record.get("task_hash", record.get("import_task_hash", 0)),
                    "resource_hash": record.get("resource_hash", 0),
                    "chunk_hash": record.get("chunk_hash", 0),
                    "status": record.get("status", ""),
                    "raw_uri": record.get("raw_uri", ""),
                    "requested_raw_uri": record.get("requested_raw_uri", ""),
                    "resource_type": record.get("resource_type", ""),
                    "resource_version": record.get("resource_version", ""),
                    "raw_uri_hash": record.get("raw_uri_hash", 0),
                    "source_locator": record.get("source_locator", record.get("metadata", {}).get("source_locator", "")),
                    "unit_kind": record.get("unit_kind", record.get("metadata", {}).get("unit_kind", "")),
                    "token_estimate": record.get("token_estimate", 0),
                    "chunk_count": record.get("chunk_count", 0),
                    "parse_warnings": record.get("parse_warnings", []),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "scope": candidate_access_scope(record),
                    "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                }
            )
        elif table == "skills" and record_type in {"skill_manifest", "skill_registry", "skill_section"}:
            rows.append(
                {
                    "row_type": record_type,
                    "skill_hash": record.get("skill_hash", 0),
                    "section_hash": record.get("section_hash", 0),
                    "name": record.get("name", record.get("skill_name", "")),
                    "heading": record.get("heading", ""),
                    "status": record.get("status", ""),
                    "version": record.get("version", ""),
                    "triggers": record.get("triggers", []),
                    "allowed_tools": record.get("allowed_tools", []),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "scope": candidate_access_scope(record),
                    "updated_at_ms": record.get("updated_at_ms", 0),
                }
            )
        elif table == "events" and record_type == "context_event":
            rows.append(
                {
                    "row_type": record_type,
                    "event_id_hash": record.get("event_id_hash", 0),
                    "text": record.get("text", ""),
                    "summary_text": record.get("summary_text", ""),
                    "classification": non_default_classification(record.get("internal_extraction", {}).get("classification", "")),
                    "event_type": record.get("event_type", record.get("internal_extraction", {}).get("event_type", "")),
                    "source_chunk_hash": record.get("source_chunk_hash", 0),
                    "resource_hash": record.get("resource_hash", 0),
                    "source_locator": record.get("source_locator", ""),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "scope": record.get("envelope", {}).get("scope", record.get("scope", {})),
                    "updated_at_ms": record.get("envelope", {}).get("ingestion_time_ms", record.get("updated_at_ms", 0)),
                }
            )
        elif table == "entities" and record_type == "context_entity":
            rows.append(
                {
                    "row_type": record_type,
                    "entity_hash": record.get("entity_hash", 0),
                    "entity_type": record.get("entity_type", ""),
                    "entity_name": record.get("entity_name", ""),
                    "value": record.get("value", record.get("text", "")),
                    "status": record.get("status", ""),
                    "source_event_hash": record.get("source_event_hash", 0),
                    "source_chunk_hash": record.get("source_chunk_hash", 0),
                    "resource_hash": record.get("resource_hash", 0),
                    "source_locator": record.get("source_locator", ""),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "scope": candidate_access_scope(record),
                    "updated_at_ms": record.get("updated_at_ms", 0),
                }
            )
        elif table == "context_packs" and record_type in {"context_pack_audit", "context_pack_telemetry"}:
            dropped_refs = record.get("dropped_refs", {})
            dropped_ref_bucket_counts = (
                record.get("dropped_ref_bucket_counts")
                if isinstance(record.get("dropped_ref_bucket_counts"), dict)
                else {
                    key: value
                    for key, value in dropped_refs.items()
                    if isinstance(dropped_refs, dict)
                    and isinstance(value, int)
                    and key != "deadline_exceeded"
                    and value > 0
                }
            )
            dropped_ref_count = (
                len(dropped_refs.get("refs", []))
                if record_type == "context_pack_audit" and isinstance(dropped_refs, dict) and isinstance(dropped_refs.get("refs"), list)
                else record.get("dropped_ref_count", 0)
            )
            if not dropped_ref_count:
                dropped_ref_count = sum(int(value) for value in dropped_ref_bucket_counts.values() if isinstance(value, int))
            rows.append(
                {
                    "row_type": record_type,
                    "context_pack_id": record.get("context_pack_id", ""),
                    "query": record.get("query", "") if record_type == "context_pack_audit" else f"hash:{record.get('query_hash', '')}",
                    "used_context_tokens": record.get("used_context_tokens", record.get("used_remote_context_tokens", 0)),
                    "used_local_context_tokens": record.get("used_local_context_tokens", 0),
                    "used_remote_context_tokens": record.get("used_remote_context_tokens", 0),
                    "remote_context_budget_tokens": record.get("remote_context_budget_tokens", 0),
                    "requested_max_context_tokens": record.get("requested_max_context_tokens", 0),
                    "selected_ref_count": len(record.get("selected_refs", [])) if record_type == "context_pack_audit" else record.get("selected_ref_count", 0),
                    "dropped_ref_count": dropped_ref_count,
                    "dropped_ref_bucket_counts": dropped_ref_bucket_counts,
                    "stale_dropped_refs": int(record.get("stale_dropped_refs") or dropped_ref_bucket_counts.get("stale", 0)),
                    "memory_layer_budget": record.get("memory_layer_budget", {}),
                    "quality_warnings": record.get("quality_warnings", []) if record_type == "context_pack_audit" else {"count": record.get("quality_warning_count", 0)},
                    "scope": candidate_access_scope(record),
                    "created_at_ms": record.get("created_at_ms", 0),
                }
            )
        elif table == "summary_refresh" and record_type in {"context_batch_commit", "context_summary_dirty"}:
            if record_type == "context_batch_commit":
                summary_refresh = record.get("summary_refresh", {})
                if not isinstance(summary_refresh, dict):
                    summary_refresh = {}
                profile_promotion_summary = record.get("profile_promotion_summary", [])
                rows.append(
                    {
                        "row_type": record_type,
                        "commit_id_hash": record.get("commit_id_hash", 0),
                        "batch_id_hash": record.get("batch_id_hash", 0),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "commit_reason": record.get("commit_reason", ""),
                        "trigger_policy": record.get("trigger_policy", ""),
                        "extraction_phase": record.get("extraction_phase", ""),
                        "final_session_boundary": bool(record.get("final_session_boundary", False)),
                        "summary_refresh_status": summary_refresh.get("status", ""),
                        "summary_dirty_hash_count": len(summary_refresh.get("dirty_hashes", []))
                        if isinstance(summary_refresh.get("dirty_hashes"), list)
                        else 0,
                        "session_dirty_hash_count": len(summary_refresh.get("session_dirty_hashes", []))
                        if isinstance(summary_refresh.get("session_dirty_hashes"), list)
                        else 0,
                        "profile_dirty_hash_count": len(summary_refresh.get("profile_dirty_hashes", []))
                        if isinstance(summary_refresh.get("profile_dirty_hashes"), list)
                        else 0,
                        "profile_summary_refresh_required": bool(summary_refresh.get("profile_summary_refresh_required", False)),
                        "profile_promotion_count": len(profile_promotion_summary)
                        if isinstance(profile_promotion_summary, list)
                        else 0,
                        "memory_layers_written": record.get("memory_layers_written", {}),
                        "source_roles": record.get("source_roles", []),
                        "source_hook_types": record.get("source_hook_types", []),
                        "source_codex_events": record.get("source_codex_events", []),
                        "created_at_ms": record.get("created_at_ms", 0),
                    }
                )
            else:
                rows.append(
                    {
                        "row_type": record_type,
                        "dirty_node_hash": record.get("dirty_node_hash", record.get("node_hash", 0)),
                        "node_hash": record.get("node_hash", 0),
                        "node_path": record.get("node_path", []),
                        "scope": candidate_access_scope(record),
                        "dirty_reason": record.get("dirty_reason", ""),
                        "source_ref_type": record.get("source_ref_type", ""),
                        "source_batch_hash": record.get("source_batch_hash", 0),
                        "source_entity_hash": record.get("source_entity_hash", 0),
                        "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                    }
                )
        elif table == "async_pipeline" and record_type == "matrixark_async_pipeline_task":
            memory_layers_written = record.get("memory_layers_written", {})
            if not isinstance(memory_layers_written, dict):
                memory_layers_written = {}
            completed_stages = record.get("completed_stages", [])
            if not isinstance(completed_stages, list):
                completed_stages = []
            remaining_stages = record.get("remaining_stages", [])
            if not isinstance(remaining_stages, list):
                remaining_stages = []
            rows.append(
                {
                    "row_type": record_type,
                    "task_hash": record.get("task_hash", 0),
                    "event_id_hash": record.get("event_id_hash", 0),
                    "commit_id_hash": record.get("commit_id_hash", 0),
                    "batch_id_hash": record.get("batch_id_hash", 0),
                    "node_hash": record.get("node_hash", 0),
                    "node_path": record.get("node_path", []),
                    "scope": candidate_access_scope(record),
                    "status": record.get("status", ""),
                    "stages": record.get("stages", []),
                    "completed_stages": completed_stages,
                    "remaining_stages": remaining_stages,
                    "summary_pending": "summary" in remaining_stages,
                    "compression_pending": "compression" in remaining_stages,
                    "embedding_pending": "embedding" in remaining_stages,
                    "trigger_policy": record.get("trigger_policy", ""),
                    "extraction_phase": record.get("extraction_phase", ""),
                    "final_session_boundary": bool(record.get("final_session_boundary", False)),
                    "source_roles": record.get("source_roles", []),
                    "source_hook_types": record.get("source_hook_types", []),
                    "source_codex_events": record.get("source_codex_events", []),
                    "memory_layers_written": memory_layers_written,
                    "summary_refresh_status": record.get("summary_refresh_status", ""),
                    "summary_dirty_nodes": record.get("summary_dirty_nodes", 0),
                    "created_at_ms": record.get("created_at_ms", 0),
                    "updated_at_ms": record.get("updated_at_ms", record.get("created_at_ms", 0)),
                }
            )
    if table == "resources":
        priority = {"resource_manifest": 0, "resource_chunk": 1, "resource_import_task": 2}
        rows.sort(
            key=lambda row: (
                priority.get(str(row.get("row_type") or ""), 9),
                -int(row.get("updated_at_ms") or row.get("created_at_ms") or 0),
            )
        )
    else:
        rows.sort(key=lambda row: int(row.get("updated_at_ms") or row.get("created_at_ms") or 0), reverse=True)
    return rows


def ingestion_dashboard(adapter: Any, args: Json) -> Json:
    scope = optional_object(args, "scope")
    table = optional_string(args, "table", "messages")
    allowed_tables = {
        "messages",
        "resources",
        "skills",
        "events",
        "entities",
        "context_packs",
        "summary_refresh",
        "async_pipeline",
    }
    if table not in allowed_tables:
        raise MatrixArkError(f"table must be one of {sorted(allowed_tables)}")
    page_size = args.get("page_size", 25)
    if not isinstance(page_size, int) or page_size <= 0 or page_size > 200:
        raise MatrixArkError("page_size must be an integer between 1 and 200")
    page_token = args.get("page_token", 0)
    if isinstance(page_token, str) and page_token.isdigit():
        page_token = int(page_token)
    if not isinstance(page_token, int) or page_token < 0:
        raise MatrixArkError("page_token must be a non-negative integer offset")
    records = adapter.read_all()
    totals = {name: len(dashboard_rows_for_table(records, name, scope)) for name in sorted(allowed_tables)}
    rows = dashboard_rows_for_table(records, table, scope)
    page = rows[page_token : page_token + page_size]
    next_page_token = page_token + page_size if page_token + page_size < len(rows) else None
    return {
        "status": "ok",
        "scope": scope,
        "table": table,
        "page_size": page_size,
        "page_token": page_token,
        "next_page_token": next_page_token,
        "total": len(rows),
        "totals": totals,
        "rows": page,
        "record_count": len(records),
    }
