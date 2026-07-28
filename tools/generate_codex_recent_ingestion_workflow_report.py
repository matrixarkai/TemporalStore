#!/usr/bin/env python3
from __future__ import annotations

import collections
import argparse
import html
import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path("/root/src/github-services/TemporalStore-ingestion-workflow-report")
OUT_DIR = ROOT / "docs" / "debug" / "codex_recent_ingestion_workflow_20260724"
OUT_JSON = OUT_DIR / "codex_recent_ingestion_workflow.json"
OUT_MD = OUT_DIR / "codex_recent_ingestion_workflow.md"
OUT_HTML = OUT_DIR / "codex_recent_ingestion_workflow.html"

CPP_LIB = "/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so"
CPP_PREFIX = "matrixark:codex-hook:cpp-live-v2"
RUST_PREFIX = "matrixark:codex-hook:rust-live-v2"


def short(text: Any, limit: int = 220) -> str:
    value = " ".join(str(text or "").split())
    return value if len(value) <= limit else value[: limit - 3] + "..."


def event_ms(record: dict[str, Any]) -> int:
    return int(
        record.get("hook_observed_at_ms")
        or record.get("ingestion_time_ms")
        or record.get("created_at_ms")
        or (record.get("agent_hook") or {}).get("observed_at_ms")
        or 0
    )


def message_text(record: dict[str, Any]) -> str:
    for key in ("text", "content", "summary_text", "entity_name", "state"):
        if isinstance(record.get(key), str) and record[key].strip():
            return record[key]
    messages = record.get("messages")
    if isinstance(messages, list):
        parts = []
        for item in messages[:2]:
            if isinstance(item, dict) and item.get("content"):
                parts.append(str(item.get("content")))
        if parts:
            return " ".join(parts)
    return ""


def compact_count_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    compact: dict[str, int] = {}
    for key, count in value.items():
        name = str(key or "").strip()
        if not name:
            continue
        try:
            amount = int(count or 0)
        except (TypeError, ValueError):
            continue
        if amount > 0:
            compact[name] = compact.get(name, 0) + amount
    return compact


def add_count(target: dict[str, int], key: Any, count: int = 1) -> None:
    name = str(key or "").strip()
    if name and count > 0:
        target[name] = target.get(name, 0) + count


def merge_count_map(target: dict[str, int], value: Any) -> None:
    for key, count in compact_count_map(value).items():
        target[key] = target.get(key, 0) + count


def nested_dict(record: dict[str, Any], *path: str) -> dict[str, Any]:
    value: Any = record
    for key in path:
        if not isinstance(value, dict):
            return {}
        value = value.get(key)
    return value if isinstance(value, dict) else {}


def int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def row(record: dict[str, Any], sequence: int) -> dict[str, Any]:
    hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    retrieval_metrics = nested_dict(record, "retrieval_metrics")
    recall_policy = nested_dict(record, "recall_policy")
    outputs = nested_dict(record, "outputs")
    messages = record.get("messages") if isinstance(record.get("messages"), list) else []
    role = record.get("role")
    if not role and messages and isinstance(messages[0], dict):
        role = messages[0].get("role", "")
    return {
        "sequence": sequence,
        "record_type": record.get("record_type") or record.get("type") or "unknown",
        "role": role or "",
        "session_id": record.get("session_id") or (record.get("scope") or {}).get("session_id", ""),
        "memory_scope": record.get("memory_scope") or "",
        "session_continuity": record.get("session_continuity") or "",
        "data_model": record.get("data_model") or "",
        "node_path": record.get("node_path") if isinstance(record.get("node_path"), list) else [],
        "thread_id": record.get("thread_id") or "",
        "turn_id": record.get("turn_id") or "",
        "codex_api_event": record.get("codex_api_event") or metadata.get("codex_event") or hook.get("trigger") or "",
        "hook_id": record.get("hook_id") or hook.get("hook_id") or "",
        "hook_type": record.get("hook_type") or metadata.get("hook_type") or hook.get("hook_type") or "",
        "hook_observed_at_ms": event_ms(record),
        "synthetic": bool(record.get("synthetic", False)),
        "text": short(message_text(record)),
        "source_roles": record.get("source_roles") if isinstance(record.get("source_roles"), list) else metadata.get("source_roles", []),
        "source_hook_types": record.get("source_hook_types") if isinstance(record.get("source_hook_types"), list) else metadata.get("source_hook_types", []),
        "source_codex_events": record.get("source_codex_events") if isinstance(record.get("source_codex_events"), list) else metadata.get("source_codex_events", []),
        "source_session_ids": record.get("source_session_ids") if isinstance(record.get("source_session_ids"), list) else metadata.get("source_session_ids", []),
        "source_role_counts": compact_count_map(record.get("source_role_counts") or metadata.get("source_role_counts")),
        "source_hook_type_counts": compact_count_map(record.get("source_hook_type_counts") or metadata.get("source_hook_type_counts")),
        "source_codex_event_counts": compact_count_map(record.get("source_codex_event_counts") or metadata.get("source_codex_event_counts")),
        "profile_promotion_policy": record.get("profile_promotion_policy") or outputs.get("profile_promotion_policy") or "",
        "profile_promotion_scope_available": bool_value(
            record.get("profile_promotion_scope_available")
            if "profile_promotion_scope_available" in record
            else outputs.get("profile_promotion_scope_available")
        ),
        "entities_written": int_value(record.get("entities_written") or outputs.get("entities")),
        "profile_entities_written": int_value(record.get("profile_entities_written") or outputs.get("profile_entities")),
        "selected_ref_count": int_value(
            record.get("selected_ref_count")
            or record.get("selected_refs_count")
            or retrieval_metrics.get("selected_refs")
            or record.get("selected_refs_total")
        ),
        "selected_refs": record.get("selected_refs") if isinstance(record.get("selected_refs"), list) else [],
        "memory_layer_budget": (
            record.get("memory_layer_budget")
            if isinstance(record.get("memory_layer_budget"), dict)
            else retrieval_metrics.get("memory_layer_budget")
            if isinstance(retrieval_metrics.get("memory_layer_budget"), dict)
            else recall_policy.get("memory_layer_budget")
            if isinstance(recall_policy.get("memory_layer_budget"), dict)
            else {}
        ),
        "dropped_memory_layer_budget": (
            record.get("dropped_memory_layer_budget")
            if isinstance(record.get("dropped_memory_layer_budget"), dict)
            else retrieval_metrics.get("dropped_memory_layer_budget")
            if isinstance(retrieval_metrics.get("dropped_memory_layer_budget"), dict)
            else recall_policy.get("dropped_memory_layer_budget")
            if isinstance(recall_policy.get("dropped_memory_layer_budget"), dict)
            else {}
        ),
        "memory_layer_pressure": (
            record.get("memory_layer_pressure")
            if isinstance(record.get("memory_layer_pressure"), dict)
            else retrieval_metrics.get("memory_layer_pressure")
            if isinstance(retrieval_metrics.get("memory_layer_pressure"), dict)
            else recall_policy.get("memory_layer_pressure")
            if isinstance(recall_policy.get("memory_layer_pressure"), dict)
            else {}
        ),
        "write_path": ((record.get("matrixark_write_debug") or {}).get("write_path") if isinstance(record.get("matrixark_write_debug"), dict) else ""),
    }


def is_profile_record(item: dict[str, Any]) -> bool:
    node_path = item.get("node_path") if isinstance(item.get("node_path"), list) else []
    return (
        item.get("memory_scope") == "user_profile"
        or item.get("data_model") == "context_profile_entity"
        or "profile:long_term_memory" in node_path
    )


def is_resource_or_skill_record(item: dict[str, Any]) -> bool:
    record_type = str(item.get("record_type") or "")
    return record_type in {
        "resource_chunk",
        "resource_manifest",
        "resource_registry",
        "skill_section",
        "skill_manifest",
        "skill_registry",
        "skill_registry_update",
    }


def serving_visibility_gaps(
    *,
    serving_types: collections.Counter[str],
    context_events: list[dict[str, Any]],
    context_embeddings: list[dict[str, Any]],
    profile_records: list[dict[str, Any]],
    resource_skill_records: list[dict[str, Any]],
    raw_records: list[dict[str, Any]],
) -> list[str]:
    gaps: list[str] = []
    derived_count = sum(
        serving_types.get(record_type, 0)
        for record_type in ("context_entity", "context_index", "context_segment", "context_summary")
    )
    if derived_count:
        if not context_events:
            gaps.append("context_event_missing_while_derived_memory_present")
        if not context_embeddings:
            gaps.append("context_embedding_missing_while_derived_memory_present")
    if serving_types.get("context_index", 0) and not profile_records:
        gaps.append("profile_records_missing_from_recent_serving_window")
    raw_resource_or_skill = any(is_resource_or_skill_record(item) for item in raw_records)
    if raw_resource_or_skill and not resource_skill_records:
        gaps.append("resource_skill_records_missing_from_recent_serving_window")
    return gaps


def profile_promotion_policy_gaps(serving_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for item in serving_records:
        if item.get("record_type") != "context_extraction_audit":
            continue
        entity_count = int_value(item.get("entities_written"))
        profile_entity_count = int_value(item.get("profile_entities_written"))
        if entity_count <= 0:
            continue
        item_gaps: list[str] = []
        if item.get("profile_promotion_scope_available"):
            if item.get("profile_promotion_policy") != "always_when_profile_scope_available":
                item_gaps.append("profile_promotion:policy_not_always")
            if profile_entity_count < entity_count:
                item_gaps.append("profile_promotion:profile_entities_less_than_session_entities")
        elif profile_entity_count > 0:
            item_gaps.append("profile_promotion:profile_entities_written_without_scope")
        if item_gaps:
            gaps.append(
                {
                    "sequence": item.get("sequence"),
                    "gaps": item_gaps,
                    "entities_written": entity_count,
                    "profile_entities_written": profile_entity_count,
                    "profile_promotion_policy": item.get("profile_promotion_policy") or "",
                    "profile_promotion_scope_available": bool(item.get("profile_promotion_scope_available")),
                }
            )
    return gaps


def serving_visibility_summary(report: dict[str, Any]) -> dict[str, Any]:
    gap_backends = []
    for backend in report.get("backends", []):
        gaps = backend.get("serving_visibility_gaps") or []
        if gaps:
            gap_backends.append(
                {
                    "backend": backend.get("backend", ""),
                    "prefix": backend.get("prefix", ""),
                    "gaps": list(gaps),
                }
            )
    return {
        "serving_visibility_pass": not gap_backends,
        "serving_visibility_gap_count": sum(len(item["gaps"]) for item in gap_backends),
        "serving_visibility_gap_backends": gap_backends,
    }


def extraction_input_coverage_summary(report: dict[str, Any]) -> dict[str, Any]:
    gap_backends = []
    for backend in report.get("backends", []):
        gaps = backend.get("extraction_input_coverage_gaps") or []
        if gaps:
            gap_backends.append(
                {
                    "backend": backend.get("backend", ""),
                    "prefix": backend.get("prefix", ""),
                    "gaps": list(gaps),
                }
            )
    return {
        "extraction_input_coverage_pass": not gap_backends,
        "extraction_input_coverage_gap_count": sum(
            len(gap.get("gaps", []))
            for backend in gap_backends
            for gap in backend["gaps"]
            if isinstance(gap, dict)
        ),
        "extraction_input_coverage_gap_backends": gap_backends,
    }


def require_serving_visibility(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "required", "require"}


def require_extraction_input_coverage(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "required", "require"}


def retrieval_memory_coverage_summary(report: dict[str, Any]) -> dict[str, Any]:
    gap_backends = []
    for backend in report.get("backends", []):
        gaps = backend.get("retrieval_memory_coverage_gaps") or []
        if gaps:
            gap_backends.append(
                {
                    "backend": backend.get("backend", ""),
                    "prefix": backend.get("prefix", ""),
                    "gaps": list(gaps),
                }
            )
    return {
        "retrieval_memory_coverage_pass": not gap_backends,
        "retrieval_memory_coverage_gap_count": sum(
            len(gap.get("gaps", []))
            for backend in gap_backends
            for gap in backend["gaps"]
            if isinstance(gap, dict)
        ),
        "retrieval_memory_coverage_gap_backends": gap_backends,
    }


def profile_promotion_policy_summary(report: dict[str, Any]) -> dict[str, Any]:
    gap_backends = []
    for backend in report.get("backends", []):
        gaps = backend.get("profile_promotion_policy_gaps") or []
        if gaps:
            gap_backends.append(
                {
                    "backend": backend.get("backend", ""),
                    "prefix": backend.get("prefix", ""),
                    "gaps": list(gaps),
                }
            )
    return {
        "profile_promotion_policy_pass": not gap_backends,
        "profile_promotion_policy_gap_count": sum(
            len(gap.get("gaps", []))
            for backend in gap_backends
            for gap in backend["gaps"]
            if isinstance(gap, dict)
        ),
        "profile_promotion_policy_gap_backends": gap_backends,
    }


def require_retrieval_memory_coverage(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "required", "require"}


def require_profile_promotion_policy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "required", "require"}


def strict_memory_gate_summary(report: dict[str, Any]) -> dict[str, Any]:
    serving = report.get("serving_visibility") or serving_visibility_summary(report)
    extraction = report.get("extraction_input_coverage") or extraction_input_coverage_summary(report)
    retrieval = report.get("retrieval_memory_coverage") or retrieval_memory_coverage_summary(report)
    profile_promotion = report.get("profile_promotion_policy") or profile_promotion_policy_summary(report)
    gates = {
        "serving_visibility": bool(serving.get("serving_visibility_pass")),
        "extraction_input_coverage": bool(extraction.get("extraction_input_coverage_pass")),
        "retrieval_memory_coverage": bool(retrieval.get("retrieval_memory_coverage_pass")),
        "profile_promotion_policy": bool(profile_promotion.get("profile_promotion_policy_pass")),
    }
    failed = [name for name, passed in gates.items() if not passed]
    return {
        "strict_memory_gate_pass": not failed,
        "strict_memory_gate_failed": failed,
        "strict_memory_gate_status": "pass" if not failed else "gap",
    }


def require_all_memory_gates(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "required", "require", "strict"}


def row_source_roles(item: dict[str, Any]) -> list[str]:
    roles = set()
    role = str(item.get("role") or "").strip()
    if role:
        roles.add(role)
    for role in item.get("source_roles") if isinstance(item.get("source_roles"), list) else []:
        role_name = str(role or "").strip()
        if role_name:
            roles.add(role_name)
    for role in compact_count_map(item.get("source_role_counts")):
        roles.add(role)
    return sorted(roles)


def serving_row_matches_session(item: dict[str, Any], session_id: str) -> bool:
    if not session_id:
        return False
    if item.get("session_id") == session_id:
        return True
    source_session_ids = item.get("source_session_ids") if isinstance(item.get("source_session_ids"), list) else []
    return session_id in {str(value) for value in source_session_ids}


def extraction_input_batches(
    raw_records: list[dict[str, Any]],
    serving_records: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in raw_records:
        if item.get("record_type") != "agent_message" or item.get("synthetic"):
            continue
        session_id = str(item.get("session_id") or item.get("thread_id") or "unknown_session")
        batch = grouped.setdefault(
            session_id,
            {
                "session_id": session_id,
                "source_event_sequences": [],
                "source_role_counts": {},
                "source_hook_type_counts": {},
                "source_codex_event_counts": {},
                "messages": [],
            },
        )
        batch["source_event_sequences"].append(item.get("sequence"))
        roles = row_source_roles(item) or ["unknown"]
        for role in roles:
            add_count(batch["source_role_counts"], role)
        add_count(batch["source_hook_type_counts"], item.get("hook_type") or "unknown")
        add_count(batch["source_codex_event_counts"], item.get("codex_api_event") or "unknown")
        batch["messages"].append(
            {
                "sequence": item.get("sequence"),
                "role": roles[0] if len(roles) == 1 else ",".join(roles),
                "hook_type": item.get("hook_type") or "",
                "codex_api_event": item.get("codex_api_event") or "",
                "text": item.get("text") or "",
            }
        )

    batches: list[dict[str, Any]] = []
    for session_id, batch in grouped.items():
        derived_role_counts: dict[str, int] = {}
        derived_hook_type_counts: dict[str, int] = {}
        derived_codex_event_counts: dict[str, int] = {}
        derived_records = [
            item
            for item in serving_records
            if item.get("record_type")
            in {"context_event", "context_segment", "context_entity", "context_index", "context_summary", "context_embedding"}
            and serving_row_matches_session(item, session_id)
        ]
        for item in derived_records:
            merge_count_map(derived_role_counts, item.get("source_role_counts"))
            merge_count_map(derived_hook_type_counts, item.get("source_hook_type_counts"))
            merge_count_map(derived_codex_event_counts, item.get("source_codex_event_counts"))
            for role in item.get("source_roles") if isinstance(item.get("source_roles"), list) else []:
                add_count(derived_role_counts, role)
            for hook_type in item.get("source_hook_types") if isinstance(item.get("source_hook_types"), list) else []:
                add_count(derived_hook_type_counts, hook_type)
            for codex_event in item.get("source_codex_events") if isinstance(item.get("source_codex_events"), list) else []:
                add_count(derived_codex_event_counts, codex_event)
        expected_roles = sorted(
            role
            for role in batch["source_role_counts"]
            if role in {"user", "assistant", "tool", "system", "llm", "model"}
        )
        derived_roles = sorted(derived_role_counts)
        missing_roles = [role for role in expected_roles if role not in derived_role_counts]
        coverage_gaps = [f"source_role:{role}:missing_from_derived_serving_memory" for role in missing_roles]
        batches.append(
            {
                **batch,
                "message_count": len(batch["messages"]),
                "derived_record_count": len(derived_records),
                "derived_source_role_counts": derived_role_counts,
                "derived_source_hook_type_counts": derived_hook_type_counts,
                "derived_source_codex_event_counts": derived_codex_event_counts,
                "expected_source_roles": expected_roles,
                "derived_source_roles": derived_roles,
                "source_role_coverage_status": "gap" if coverage_gaps else "ok",
                "source_role_coverage_gaps": coverage_gaps,
                "extraction_input_shape": "bounded agent_message records grouped by session and passed to matrixark_session_commit/matrixark_batch_extract",
            }
        )
    return sorted(batches, key=lambda item: max(item["source_event_sequences"] or [-1]), reverse=True)[:limit]


def bucket_ref_count(budget: dict[str, Any], bucket_name: str, key: str) -> int:
    bucket = nested_dict(budget, bucket_name, key)
    return int_value(bucket.get("refs") or bucket.get("selected_refs"))


def retrieval_audit_coverage(item: dict[str, Any]) -> dict[str, Any]:
    budget = item.get("memory_layer_budget") if isinstance(item.get("memory_layer_budget"), dict) else {}
    selected_ref_count = int_value(item.get("selected_ref_count")) or int_value(budget.get("total_selected_refs"))
    selected_refs = item.get("selected_refs") if isinstance(item.get("selected_refs"), list) else []
    if not selected_ref_count and selected_refs:
        selected_ref_count = len(selected_refs)
    session_memory_refs = bucket_ref_count(budget, "by_memory_scope", "session")
    profile_memory_refs = bucket_ref_count(budget, "by_memory_scope", "user_profile")
    same_session_refs = bucket_ref_count(budget, "by_session_continuity", "same_session")
    cross_session_refs = bucket_ref_count(budget, "by_session_continuity", "cross_session")
    if not session_memory_refs:
        session_memory_refs = sum(1 for ref in selected_refs if isinstance(ref, dict) and ref.get("memory_scope") == "session")
    if not profile_memory_refs:
        profile_memory_refs = sum(1 for ref in selected_refs if isinstance(ref, dict) and ref.get("memory_scope") == "user_profile")
    if not same_session_refs:
        same_session_refs = sum(1 for ref in selected_refs if isinstance(ref, dict) and ref.get("session_continuity") == "same_session")
    if not cross_session_refs:
        cross_session_refs = sum(1 for ref in selected_refs if isinstance(ref, dict) and ref.get("session_continuity") == "cross_session")

    gaps = []
    if selected_ref_count <= 0:
        gaps.append("retrieval:no_remote_refs_selected")
    if session_memory_refs <= 0 and profile_memory_refs <= 0:
        gaps.append("retrieval:no_session_or_profile_memory_selected")
    if same_session_refs <= 0 and cross_session_refs <= 0:
        gaps.append("retrieval:no_session_continuity_refs_selected")
    if not budget or int_value(budget.get("total_selected_refs")) <= 0:
        gaps.append("retrieval:memory_layer_budget_missing_selected_refs")
    return {
        "sequence": item.get("sequence"),
        "record_type": item.get("record_type"),
        "context_pack_id": item.get("context_pack_id") or "",
        "query": item.get("text") or "",
        "status": "gap" if gaps else "ok",
        "gaps": gaps,
        "selected_ref_count": selected_ref_count,
        "session_memory_refs": session_memory_refs,
        "profile_memory_refs": profile_memory_refs,
        "same_session_refs": same_session_refs,
        "cross_session_refs": cross_session_refs,
        "memory_layer_budget_selected_refs": int_value(budget.get("total_selected_refs")),
    }


def rust_exec(command: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps({"shard_id": 1, "command": command}).encode("utf-8")
    req = urllib.request.Request("http://127.0.0.1:17100/execute", data=body, headers={"content-type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=5).read().decode("utf-8"))


def bytes_to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def scan_rust(base: str, limit: int = 500) -> tuple[int, int, list[dict[str, Any]]]:
    count = int(bytes_to_str(rust_exec({"kind": "string_get", "key": base + ":record_count"})["response"]["value"]) or 0)
    hot_count = int(bytes_to_str(rust_exec({"kind": "string_get", "key": base + ":hot_record_count"})["response"]["value"]) or 0)
    rows: list[dict[str, Any]] = []
    for sequence in range(count - 1, max(-1, count - limit - 1), -1):
        candidates = (
            (f"{base}:records:{sequence // 256:06d}", f"{sequence % 256:020d}"),
            (f"{base}:records:{sequence // 10000:06d}", f"{sequence:020d}"),
            (f"{base}:records", f"{sequence:020d}"),
        )
        raw = ""
        for key, field in candidates:
            raw = bytes_to_str(rust_exec({"kind": "hash_get", "key": key, "field": field})["response"]["value"])
            if raw:
                break
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except Exception:
            record = {"record_type": "unparsed", "text": raw[:1000]}
        rows.append({"sequence": sequence, "record": record})
    return count, hot_count, rows


def scan_cpp(base: str, limit: int = 500) -> tuple[int, int, list[dict[str, Any]]]:
    sys.path.insert(0, str(ROOT / "sdk" / "python"))
    from temporalstore.client import Client, Options

    client = Client(
        Options(
            metaserver_addr="127.0.0.1:18000",
            namespace_name="deploy_ns",
            table_name="deploy_table",
            request_timeout_ms=5000,
            io_timeout_ms=5000,
            max_read_retries=1,
        ),
        library_path=CPP_LIB,
    )
    try:
        count = int(client.get_string(base + ":record_count") or 0)
    except Exception:
        count = 0
    try:
        hot_count = int(client.get_string(base + ":hot_record_count") or 0)
    except Exception:
        hot_count = 0
    rows = []
    for sequence in range(count - 1, max(-1, count - limit - 1), -1):
        try:
            raw = client.hget(f"{base}:records:{sequence // 256:06d}", f"{sequence % 256:020d}") or ""
        except Exception:
            raw = ""
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except Exception:
            record = {"record_type": "unparsed", "text": raw[:1000]}
        rows.append({"sequence": sequence, "record": record})
    return count, hot_count, rows


def summarize_backend(name: str, prefix: str, raw_count: int, raw_hot_count: int, raw_rows: list[dict[str, Any]], serving_count: int, serving_hot_count: int, serving_rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_records = [row(item["record"], item["sequence"]) for item in raw_rows]
    serving_records = [row(item["record"], item["sequence"]) for item in serving_rows]
    raw_types = collections.Counter(item["record_type"] for item in raw_records)
    serving_types = collections.Counter(item["record_type"] for item in serving_records)
    user_prompts = [item for item in raw_records if item["record_type"] == "agent_message" and item["codex_api_event"] == "UserPromptSubmit" and not item["synthetic"]]
    entities = [item for item in raw_records + serving_records if item["record_type"] in {"context_entity", "context_entity_update_audit"}]
    summaries = [item for item in raw_records + serving_records if item["record_type"] in {"context_summary", "context_summary_dirty", "context_batch_commit"}]
    context_events = [item for item in serving_records if item["record_type"] == "context_event"]
    context_embeddings = [item for item in serving_records if item["record_type"] == "context_embedding"]
    profile_records = [item for item in serving_records if is_profile_record(item)]
    resource_skill_records = [item for item in serving_records if is_resource_or_skill_record(item)]
    retrieval_records = [
        item
        for item in serving_records
        if item["record_type"] in {"context_pack_audit", "context_pack_telemetry"}
    ]
    retrieval_coverages = [retrieval_audit_coverage(item) for item in retrieval_records]
    retrieval_memory_coverage_gaps = [
        {
            "context_pack_id": item["context_pack_id"],
            "sequence": item["sequence"],
            "gaps": item["gaps"],
        }
        for item in retrieval_coverages
        if item["gaps"]
    ]
    extraction_batches = extraction_input_batches(raw_records, serving_records)
    extraction_input_coverage_gaps = [
        {
            "session_id": batch["session_id"],
            "gaps": batch["source_role_coverage_gaps"],
        }
        for batch in extraction_batches
        if batch["source_role_coverage_gaps"]
    ]
    promotion_policy_gaps = profile_promotion_policy_gaps(serving_records)
    visibility_gaps = serving_visibility_gaps(
        serving_types=serving_types,
        context_events=context_events,
        context_embeddings=context_embeddings,
        profile_records=profile_records,
        resource_skill_records=resource_skill_records,
        raw_records=raw_records,
    )
    return {
        "backend": name,
        "prefix": prefix,
        "raw_count": raw_hot_count or raw_count,
        "serving_count": serving_hot_count or serving_count,
        "physical_raw_count": raw_count,
        "physical_serving_count": serving_count,
        "compact_hot_raw_count": raw_hot_count,
        "compact_hot_serving_count": serving_hot_count,
        "recent_raw_type_counts": dict(raw_types),
        "recent_serving_type_counts": dict(serving_types),
        "recent_real_user_prompts": user_prompts[:8],
        "recent_extraction_input_batches": extraction_batches,
        "extraction_input_coverage_status": "gap" if extraction_input_coverage_gaps else "ok",
        "extraction_input_coverage_gaps": extraction_input_coverage_gaps,
        "recent_context_events": context_events[:8],
        "recent_context_embeddings": context_embeddings[:8],
        "recent_profile_records": profile_records[:8],
        "recent_resource_skill_records": resource_skill_records[:8],
        "recent_retrieval_memory_coverages": retrieval_coverages[:8],
        "retrieval_memory_coverage_status": "gap" if retrieval_memory_coverage_gaps else "ok",
        "retrieval_memory_coverage_gaps": retrieval_memory_coverage_gaps,
        "profile_promotion_policy_status": "gap" if promotion_policy_gaps else "ok",
        "profile_promotion_policy_gaps": promotion_policy_gaps,
        "recent_context_event_count": len(context_events),
        "recent_context_embedding_count": len(context_embeddings),
        "recent_profile_record_count": len(profile_records),
        "recent_resource_skill_record_count": len(resource_skill_records),
        "serving_visibility_status": "gap" if visibility_gaps else "ok",
        "serving_visibility_gaps": visibility_gaps,
        "recent_entities": entities[:8],
        "recent_summaries": summaries[:8],
        "recent_raw_records": raw_records[:12],
        "recent_serving_records": serving_records[:12],
    }


def render_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["|" + "|".join(headers) + "|", "|" + "|".join("---" for _ in headers) + "|"]
    for values in rows:
        out.append("|" + "|".join(str(v).replace("|", "\\|") for v in values) + "|")
    return "\n".join(out)


def render_markdown(report: dict[str, Any]) -> str:
    visibility = report.get("serving_visibility") or serving_visibility_summary(report)
    extraction_coverage = report.get("extraction_input_coverage") or extraction_input_coverage_summary(report)
    retrieval_coverage = report.get("retrieval_memory_coverage") or retrieval_memory_coverage_summary(report)
    profile_promotion = report.get("profile_promotion_policy") or profile_promotion_policy_summary(report)
    strict_gate = report.get("strict_memory_gate") or strict_memory_gate_summary(report)
    lines = [
        "# Recent Codex Hook Ingestion Workflow",
        "",
        f"Generated at `{report['generated_at_ms']}`.",
        "",
        "## Strict Memory Gate",
        "",
        f"- Status: `{strict_gate['strict_memory_gate_status']}`",
        f"- Failed gates: `{json.dumps(strict_gate['strict_memory_gate_failed'], sort_keys=True)}`",
        "",
        "## Serving Visibility Gate",
        "",
        f"- Status: `{'pass' if visibility['serving_visibility_pass'] else 'gap'}`",
        f"- Gap count: `{visibility['serving_visibility_gap_count']}`",
        f"- Gap backends: `{json.dumps(visibility['serving_visibility_gap_backends'], sort_keys=True)}`",
        "",
        "## Extraction Input Coverage Gate",
        "",
        f"- Status: `{'pass' if extraction_coverage['extraction_input_coverage_pass'] else 'gap'}`",
        f"- Gap count: `{extraction_coverage['extraction_input_coverage_gap_count']}`",
        f"- Gap backends: `{json.dumps(extraction_coverage['extraction_input_coverage_gap_backends'], sort_keys=True)}`",
        "",
        "## Retrieval Memory Coverage Gate",
        "",
        f"- Status: `{'pass' if retrieval_coverage['retrieval_memory_coverage_pass'] else 'gap'}`",
        f"- Gap count: `{retrieval_coverage['retrieval_memory_coverage_gap_count']}`",
        f"- Gap backends: `{json.dumps(retrieval_coverage['retrieval_memory_coverage_gap_backends'], sort_keys=True)}`",
        "",
        "## Profile Promotion Policy Gate",
        "",
        f"- Status: `{'pass' if profile_promotion['profile_promotion_policy_pass'] else 'gap'}`",
        f"- Gap count: `{profile_promotion['profile_promotion_policy_gap_count']}`",
        f"- Gap backends: `{json.dumps(profile_promotion['profile_promotion_policy_gap_backends'], sort_keys=True)}`",
        "",
        "## What This Report Proves",
        "",
        "- Rust TemporalStore currently captures real Codex `UserPromptSubmit` rows in raw ingestion and exposes matching serving `context_event` rows.",
        "- C++ TemporalStore now keeps the live hot prefix compact; the full hook writes trace/debug output to a separate debug prefix by default.",
        "- Compact extraction/summary is visible through `context_segment`, `context_entity`, `context_index`, and `context_summary` rows in the recent raw window.",
        "- Rust and C++ live hook prefixes use the same compact direct-publish shape for raw prompts, serving events, extracted entities, indexes, segments, and summaries.",
        "",
        "## Workflow",
        "",
        "```mermaid",
        "sequenceDiagram",
        "  participant Codex",
        "  participant Hook as matrixark_codex_dual_hook.sh",
        "  participant Rust as Rust TemporalStore 17100/17101/17102",
        "  participant Cpp as C++ TemporalStore 18000/18001",
        "  participant Async as Async extraction/summary",
        "  Codex->>Hook: UserPromptSubmit JSON payload",
        "  Hook->>Rust: raw agent_message append",
        "  Hook->>Cpp: raw agent_message append",
        "  Rust->>Rust: publish context_event serving projection",
        "  Cpp->>Cpp: publish compact context_event serving projection",
        "  Cpp->>Cpp: publish compact segment/entity/index/summary rows",
        "  Async-->>Cpp: optional debug/audit rows go to debug prefix",
        "```",
        "",
        "## Backend Counts",
        "",
        render_table(
            ["Backend", "Prefix", "Compact hot raw", "Compact hot serving", "Physical raw", "Physical serving", "Events", "Embeddings", "Profile", "Resource/skill", "Visibility", "Gaps", "Recent raw types", "Recent serving types"],
            [[b["backend"], b["prefix"], b["raw_count"], b["serving_count"], b["physical_raw_count"], b["physical_serving_count"], b["recent_context_event_count"], b["recent_context_embedding_count"], b["recent_profile_record_count"], b["recent_resource_skill_record_count"], b["serving_visibility_status"], ",".join(b["serving_visibility_gaps"]), json.dumps(b["recent_raw_type_counts"], sort_keys=True), json.dumps(b["recent_serving_type_counts"], sort_keys=True)] for b in report["backends"]],
        ),
    ]
    for backend in report["backends"]:
        lines.extend(["", f"## {backend['backend']} Recent Real User Prompts", ""])
        lines.append(render_table(["Seq", "Event", "Session", "Turn", "Hook", "Text"], [[r["sequence"], r["codex_api_event"], r["session_id"], r["turn_id"], r["hook_id"] if "hook_id" in r else r["hook_type"], r["text"]] for r in backend["recent_real_user_prompts"]]))
        lines.extend(["", f"## {backend['backend']} Extraction Input Batches", ""])
        lines.append(
            render_table(
                ["Session", "Messages", "Source roles", "Derived roles", "Coverage", "Gaps"],
                [
                    [
                        batch["session_id"],
                        batch["message_count"],
                        json.dumps(batch["source_role_counts"], sort_keys=True),
                        json.dumps(batch["derived_source_role_counts"], sort_keys=True),
                        batch["source_role_coverage_status"],
                        ",".join(batch["source_role_coverage_gaps"]),
                    ]
                    for batch in backend["recent_extraction_input_batches"]
                ],
            )
        )
        for batch in backend["recent_extraction_input_batches"][:3]:
            lines.extend(["", f"### Extraction Input `{batch['session_id']}`", ""])
            lines.append(
                render_table(
                    ["Seq", "Role", "Hook", "Event", "Bounded Text"],
                    [
                        [item["sequence"], item["role"], item["hook_type"], item["codex_api_event"], item["text"]]
                        for item in batch["messages"][:8]
                    ],
                )
            )
        lines.extend(["", f"## {backend['backend']} Retrieval Memory Coverage", ""])
        lines.append(
            render_table(
                ["Seq", "Pack", "Selected", "Session", "Profile", "Same", "Cross", "Coverage", "Gaps"],
                [
                    [
                        item["sequence"],
                        item["context_pack_id"],
                        item["selected_ref_count"],
                        item["session_memory_refs"],
                        item["profile_memory_refs"],
                        item["same_session_refs"],
                        item["cross_session_refs"],
                        item["status"],
                        ",".join(item["gaps"]),
                    ]
                    for item in backend["recent_retrieval_memory_coverages"]
                ],
            )
        )
        lines.extend(["", f"## {backend['backend']} Context Events", ""])
        lines.append(render_table(["Seq", "Event", "Session", "Text"], [[r["sequence"], r["codex_api_event"], r["session_id"], r["text"]] for r in backend["recent_context_events"]]))
        lines.extend(["", f"## {backend['backend']} Embeddings/Profile/Resources", ""])
        rows = [
            [r["sequence"], r["record_type"], r["memory_scope"], r["session_continuity"], r["data_model"], r["text"]]
            for r in backend["recent_context_embeddings"] + backend["recent_profile_records"] + backend["recent_resource_skill_records"]
        ]
        lines.append(render_table(["Seq", "Type", "Memory", "Continuity", "Model", "Text"], rows))
        lines.extend(["", f"## {backend['backend']} Entities And Summaries", ""])
        rows = [[r["sequence"], r["record_type"], r["session_id"], r["text"]] for r in backend["recent_entities"] + backend["recent_summaries"]]
        lines.append(render_table(["Seq", "Type", "Session", "Text"], rows))
    lines.extend([
        "",
        "## Timeline Interpretation",
        "",
        "1. Hook firing is proven when a recent `agent_message` has `codex_api_event=UserPromptSubmit`, `hook_type=before_llm`, `synthetic=false`, and a Codex session/thread id.",
        "2. Raw event ingestion is proven by the `raw_ingestion` append sequence and write path metadata.",
        "3. Serving context-event publication is proven when the same prompt appears as `context_event` in the serving prefix.",
        "4. Async extraction is proven only when entity/summary/index rows appear after the raw prompt or when dirty-summary markers are drained by a worker.",
        "5. Compact hot counts are reported separately from physical historical counts, so old C++ debug/audit rows no longer inflate live traffic parity.",
    ])
    return "\n".join(lines) + "\n"


def render_html(markdown: str) -> str:
    return f"""<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Recent Codex Hook Ingestion Workflow</title>
<style>body{{font-family:Inter,Segoe UI,Arial,sans-serif;background:#f6f8fb;color:#17202a;margin:0}}main{{max-width:1220px;margin:0 auto;padding:32px}}pre{{white-space:pre-wrap;background:#111827;color:#f8fafc;padding:18px;border-radius:8px;overflow:auto}}article{{background:white;border:1px solid #dbe3ee;border-radius:8px;padding:28px}}</style></head>
<body><main><article><pre>{html.escape(markdown)}</pre></article></main></body></html>
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__ or "Generate recent Codex ingestion workflow report.")
    parser.add_argument(
        "--require-serving-visibility",
        action="store_true",
        help="Exit nonzero when serving context_event/context_embedding/profile/resource visibility gaps are present.",
    )
    parser.add_argument(
        "--require-extraction-input-coverage",
        action="store_true",
        help="Exit nonzero when ingested user/assistant/tool input roles are missing from derived serving memory.",
    )
    parser.add_argument(
        "--require-retrieval-memory-coverage",
        action="store_true",
        help="Exit nonzero when recent context-pack audits lack selected session/profile retrieval evidence.",
    )
    parser.add_argument(
        "--require-profile-promotion-policy",
        action="store_true",
        help="Exit nonzero when extraction audits do not prove always-promote profile memory behavior.",
    )
    parser.add_argument(
        "--require-all-memory-gates",
        action="store_true",
        help="Exit nonzero unless serving visibility, extraction input, profile promotion, and retrieval gates all pass.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rust_raw_count, rust_raw_hot_count, rust_raw = scan_rust(RUST_PREFIX + ":raw_ingestion")
    rust_serving_count, rust_serving_hot_count, rust_serving = scan_rust(RUST_PREFIX)
    cpp_raw_count, cpp_raw_hot_count, cpp_raw = scan_cpp(CPP_PREFIX + ":raw_ingestion")
    cpp_serving_count, cpp_serving_hot_count, cpp_serving = scan_cpp(CPP_PREFIX)
    report = {
        "generated_at_ms": int(time.time() * 1000),
        "query_paths": {
            "rust": "HTTP /execute through matrixark_rust_service_proxy on 127.0.0.1:17100",
            "cpp": "TemporalStore Python SDK using libbcache2.so against 127.0.0.1:18000",
        },
        "backends": [
            summarize_backend("Rust TemporalStore", RUST_PREFIX, rust_raw_count, rust_raw_hot_count, rust_raw, rust_serving_count, rust_serving_hot_count, rust_serving),
            summarize_backend("C++ TemporalStore", CPP_PREFIX, cpp_raw_count, cpp_raw_hot_count, cpp_raw, cpp_serving_count, cpp_serving_hot_count, cpp_serving),
        ],
    }
    report["serving_visibility"] = serving_visibility_summary(report)
    report["extraction_input_coverage"] = extraction_input_coverage_summary(report)
    report["retrieval_memory_coverage"] = retrieval_memory_coverage_summary(report)
    report["profile_promotion_policy"] = profile_promotion_policy_summary(report)
    report["strict_memory_gate"] = strict_memory_gate_summary(report)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md = render_markdown(report)
    OUT_MD.write_text(md, encoding="utf-8")
    OUT_HTML.write_text(render_html(md), encoding="utf-8")
    result = {
        "json": str(OUT_JSON),
        "markdown": str(OUT_MD),
        "html": str(OUT_HTML),
        **report["serving_visibility"],
        **report["extraction_input_coverage"],
        **report["retrieval_memory_coverage"],
        **report["profile_promotion_policy"],
        **report["strict_memory_gate"],
    }
    print(json.dumps(result, indent=2))
    if (
        args.require_serving_visibility
        or require_serving_visibility(os.environ.get("MATRIXARK_REQUIRE_SERVING_VISIBILITY"))
    ) and not report["serving_visibility"]["serving_visibility_pass"]:
        raise SystemExit(2)
    if (
        args.require_extraction_input_coverage
        or require_extraction_input_coverage(os.environ.get("MATRIXARK_REQUIRE_EXTRACTION_INPUT_COVERAGE"))
    ) and not report["extraction_input_coverage"]["extraction_input_coverage_pass"]:
        raise SystemExit(3)
    if (
        args.require_retrieval_memory_coverage
        or require_retrieval_memory_coverage(os.environ.get("MATRIXARK_REQUIRE_RETRIEVAL_MEMORY_COVERAGE"))
    ) and not report["retrieval_memory_coverage"]["retrieval_memory_coverage_pass"]:
        raise SystemExit(4)
    if (
        args.require_profile_promotion_policy
        or require_profile_promotion_policy(os.environ.get("MATRIXARK_REQUIRE_PROFILE_PROMOTION_POLICY"))
    ) and not report["profile_promotion_policy"]["profile_promotion_policy_pass"]:
        raise SystemExit(6)
    if (
        args.require_all_memory_gates
        or require_all_memory_gates(os.environ.get("MATRIXARK_REQUIRE_ALL_MEMORY_GATES"))
    ) and not report["strict_memory_gate"]["strict_memory_gate_pass"]:
        raise SystemExit(5)


if __name__ == "__main__":
    main()
