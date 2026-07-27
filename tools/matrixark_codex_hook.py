#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_mcp_core import _mcp_debug_log, memory_hierarchy_contract_from_recall_policy
except ModuleNotFoundError:
    from matrixark_mcp_core import _mcp_debug_log, memory_hierarchy_contract_from_recall_policy


Json = dict[str, Any]


def _default_additional_context_char_limit() -> int:
    try:
        return max(
            1000,
            int(os.environ.get("MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT", "40000")),
        )
    except ValueError:
        return 40000


DEFAULT_ADDITIONAL_CONTEXT_CHAR_LIMIT = _default_additional_context_char_limit()


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default))))
    except ValueError:
        return max(minimum, default)


HOOK_TRACE_APPEND_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_TRACE_APPEND_TIMEOUT_MS", 750, minimum=0)
HOOK_CLOSE_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_CLOSE_TIMEOUT_MS", 750, minimum=0)
HOOK_TOOL_CALL_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_TOOL_CALL_TIMEOUT_MS", 8000, minimum=0)
HOOK_RETRIEVE_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_RETRIEVE_TIMEOUT_MS", 5000, minimum=0)
HOOK_AUTO_BATCH_EXTRACT = os.environ.get("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", "").strip().lower() in {"1", "true", "yes", "on"}
HOOK_FAST_ASYNC_INGEST = os.environ.get("MATRIXARK_HOOK_FAST_ASYNC_INGEST", "").strip().lower() in {"1", "true", "yes", "on"}
HOOK_COMPACT_HOT_PREFIX_ONLY = os.environ.get("MATRIXARK_HOOK_COMPACT_HOT_PREFIX_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}

RESOURCE_TYPE_BY_SUFFIX = {
    ".md": "md",
    ".markdown": "md",
    ".txt": "txt",
    ".log": "log",
    ".html": "html",
    ".htm": "html",
    ".pdf": "pdf",
    ".docx": "docx",
    ".pptx": "pptx",
    ".xlsx": "xlsx",
    ".csv": "csv",
    ".tsv": "tsv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".webp": "image",
}
RESOURCE_EVENTS = {
    "resourceadded",
    "resource_added",
    "addresource",
    "add_resource",
    "resource",
    "fileadded",
    "file_added",
    "documentadded",
    "document_added",
    "resourceimport",
    "resource_import",
    "skilladded",
    "skill_added",
}


def selected_ref_count_from_retrieve(pack: Json | None) -> int:
    if not isinstance(pack, dict):
        return 0
    refs = pack.get("selected_refs")
    if isinstance(refs, list):
        return len(refs)
    refs = pack.get("remote_context_refs")
    if isinstance(refs, list):
        return len(refs)
    groups = pack.get("selected_ref_groups")
    if isinstance(groups, list):
        total = 0
        for group in groups:
            if not isinstance(group, dict):
                continue
            refs_in_group = group.get("refs", [])
            total += int(group.get("count") or (len(refs_in_group) if isinstance(refs_in_group, list) else 0))
        return total
    groups = pack.get("groups")
    if isinstance(groups, list):
        total = 0
        for group in groups:
            if not isinstance(group, dict):
                continue
            refs_in_group = group.get("items", [])
            total += int(group.get("n") or (len(refs_in_group) if isinstance(refs_in_group, list) else 0))
        return total
    local_policy = pack.get("local_context_policy") if isinstance(pack.get("local_context_policy"), dict) else {}
    try:
        return int(local_policy.get("local_context_count") or 0)
    except (TypeError, ValueError):
        return 0


def used_context_tokens_from_retrieve(pack: Json | None) -> int:
    if not isinstance(pack, dict):
        return 0
    tokens = pack.get("tokens")
    if isinstance(tokens, dict):
        try:
            return int(tokens.get("remote") or tokens.get("total") or 0)
        except (TypeError, ValueError):
            return 0
    try:
        return int(pack.get("used_context_tokens") or pack.get("used_remote_context_tokens") or 0)
    except (TypeError, ValueError):
        return 0


def _int_field(payload: Json, field: str) -> int:
    try:
        return int(payload.get(field) or 0)
    except (TypeError, ValueError):
        return 0


def retrieval_budget_summary_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    used_remote_tokens = used_context_tokens_from_retrieve(pack)
    remote_budget_tokens = _int_field(pack, "remote_context_budget_tokens")
    requested_max_context_tokens = _int_field(pack, "requested_max_context_tokens")
    used_local_tokens = _int_field(pack, "used_local_context_tokens")
    total_prompt_tokens = _int_field(pack, "total_prompt_context_tokens") or used_remote_tokens + used_local_tokens
    safety_margin_tokens = _int_field(pack, "local_context_safety_margin_tokens")
    budget: Json = {
        "used_remote_context_tokens": used_remote_tokens,
        "remote_context_budget_tokens": remote_budget_tokens,
        "requested_max_context_tokens": requested_max_context_tokens,
        "used_local_context_tokens": used_local_tokens,
        "total_prompt_context_tokens": total_prompt_tokens,
        "local_context_safety_margin_tokens": safety_margin_tokens,
        "budget_source": str(pack.get("budget_source") or ""),
    }
    if remote_budget_tokens:
        budget["remote_budget_remaining_tokens"] = max(0, remote_budget_tokens - used_remote_tokens)
        budget["remote_budget_overrun"] = used_remote_tokens > remote_budget_tokens
    if requested_max_context_tokens:
        budget["total_prompt_budget_remaining_tokens"] = max(0, requested_max_context_tokens - total_prompt_tokens)
        budget["total_prompt_budget_overrun"] = total_prompt_tokens > requested_max_context_tokens
    if requested_max_context_tokens or remote_budget_tokens or used_local_tokens or safety_margin_tokens:
        budget["budget_contract"] = {
            "mode": "local_first_remote_fill_remaining",
            "local_context_first": True,
            "remote_fills_remaining_budget": True,
            "remote_is_additive_only_within_remaining_budget": True,
            "remote_budget_formula": "requested_max_context_tokens-used_local_context_tokens-local_context_safety_margin_tokens",
            "computed_remote_context_budget_tokens": max(
                0,
                requested_max_context_tokens - used_local_tokens - safety_margin_tokens,
            )
            if requested_max_context_tokens
            else remote_budget_tokens,
            "contract_holds": (
                (not remote_budget_tokens or used_remote_tokens <= remote_budget_tokens)
                and (not requested_max_context_tokens or total_prompt_tokens <= requested_max_context_tokens)
            ),
        }
    return budget


def retrieval_budget_pressure_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    pack_view = _context_pack_view(pack)
    dropped = pack_view.get("dropped_refs") if isinstance(pack_view.get("dropped_refs"), dict) else {}
    if not dropped:
        return {}
    retrieval_metrics = pack_view.get("retrieval_metrics") if isinstance(pack_view.get("retrieval_metrics"), dict) else {}
    recall_policy = pack_view.get("recall_policy") if isinstance(pack_view.get("recall_policy"), dict) else {}
    dropped_memory_layer_budget = retrieval_metrics.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        dropped_memory_layer_budget = pack_view.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        dropped_memory_layer_budget = {}
    budget_reasons = [
        "over_budget",
        "cross_session_budget",
        "cross_session_session_cap",
        "cross_session_candidate_cap",
        "shared_resource_budget",
        "shared_skill_budget",
        "max_selected_refs",
        "deadline",
    ]
    dropped_by_reason: Json = {}
    estimated_tokens: Json = {}
    raw_estimated = dropped.get("estimated_tokens") if isinstance(dropped.get("estimated_tokens"), dict) else {}
    for reason in budget_reasons:
        count = _int_field(dropped, reason)
        if count > 0:
            dropped_by_reason[reason] = count
        token_count = _int_field(raw_estimated, reason)
        if token_count > 0:
            estimated_tokens[reason] = token_count
    summary: Json = {
        "budget_pressure": bool(dropped_by_reason or dropped.get("deadline_exceeded")),
        "dropped_by_reason": dropped_by_reason,
        "estimated_tokens_by_reason": estimated_tokens,
        "deadline_exceeded": bool(dropped.get("deadline_exceeded")),
        "deadline_reason": dropped.get("deadline_reason"),
        "budget_fill_policy": dropped.get("budget_fill_policy"),
    }
    if dropped_memory_layer_budget:
        summary["dropped_memory_layer_budget"] = dropped_memory_layer_budget
    if dropped_by_reason:
        summary["budget_pressure_reason_count"] = sum(int(value) for value in dropped_by_reason.values())
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def retrieval_layer_summary_from_retrieve(pack: Json | None, refs: list[Json] | None = None) -> Json:
    if not isinstance(pack, dict):
        return {}
    pack_view = _context_pack_view(pack)
    all_refs = _selected_refs_from_retrieve(pack_view)
    refs = refs if refs is not None else all_refs
    retrieval_metrics = pack_view.get("retrieval_metrics") if isinstance(pack_view.get("retrieval_metrics"), dict) else {}
    recall_policy = pack_view.get("recall_policy") if isinstance(pack_view.get("recall_policy"), dict) else {}
    memory_layer_budget = retrieval_metrics.get("memory_layer_budget")
    if not isinstance(memory_layer_budget, dict):
        memory_layer_budget = recall_policy.get("memory_layer_budget")
    if not isinstance(memory_layer_budget, dict):
        memory_layer_budget = pack_view.get("memory_layer_budget")
    if not isinstance(memory_layer_budget, dict):
        memory_layer_budget = {}
    if refs and (
        not memory_layer_budget
        or not isinstance(memory_layer_budget.get("by_entity_type"), dict)
        or not memory_layer_budget.get("by_entity_type")
    ):
        inferred_budget = inferred_live_ref_layer_budget(refs)
        if not memory_layer_budget:
            memory_layer_budget = inferred_budget
        else:
            memory_layer_budget = {**inferred_budget, **memory_layer_budget}
            for bucket_name in [
                "by_memory_scope",
                "by_session_continuity",
                "by_extraction_phase",
                "by_ref_type",
                "by_entity_type",
                "by_source_role",
                "by_hook_type",
                "by_codex_event",
            ]:
                if not memory_layer_budget.get(bucket_name):
                    memory_layer_budget[bucket_name] = inferred_budget.get(bucket_name, {})
    raw_counts = pack_view.get("selected_ref_counts")
    selected_ref_counts: Json = {}
    use_pack_counts = refs is all_refs or len(refs) == len(all_refs)
    if use_pack_counts and isinstance(raw_counts, dict):
        for key, value in raw_counts.items():
            try:
                count = int(value)
            except (TypeError, ValueError):
                continue
            if count > 0:
                selected_ref_counts[str(key)] = count
    if not selected_ref_counts:
        for ref in refs:
            ref_class = str(ref.get("context_class") or ref.get("ref_type") or ref.get("type") or "ref")
            selected_ref_counts[ref_class] = int(selected_ref_counts.get(ref_class, 0)) + 1
    continuity = recall_policy.get("session_continuity") if isinstance(recall_policy.get("session_continuity"), dict) else {}
    local_policy = pack_view.get("local_context_policy") if isinstance(pack_view.get("local_context_policy"), dict) else {}
    layer_summary: Json = {"selected_ref_counts": selected_ref_counts}
    for source_key, output_key in [
        ("same_session_selected_ref_count", "same_session_refs"),
        ("cross_session_selected_ref_count", "cross_session_refs"),
        ("entity_bridge_selected_ref_count", "entity_bridge_refs"),
    ]:
        try:
            layer_summary[output_key] = int(continuity.get(source_key) or 0)
        except (TypeError, ValueError):
            layer_summary[output_key] = 0
    if not any(int(layer_summary.get(key) or 0) for key in ["same_session_refs", "cross_session_refs", "entity_bridge_refs"]):
        layer_summary["same_session_refs"] = sum(1 for ref in refs if ref.get("session_continuity") == "same_session")
        layer_summary["cross_session_refs"] = sum(1 for ref in refs if ref.get("session_continuity") == "cross_session")
        layer_summary["entity_bridge_refs"] = sum(
            1
            for ref in refs
            if ref.get("session_continuity") == "cross_session"
            and str(ref.get("ref_type") or ref.get("context_class") or "") == "entity"
        )
    layer_summary["session_memory_refs"] = sum(1 for ref in refs if ref.get("memory_scope") == "session")
    layer_summary["profile_memory_refs"] = sum(1 for ref in refs if ref.get("memory_scope") == "user_profile")
    try:
        layer_summary["local_context_refs"] = int(local_policy.get("local_context_count") or 0)
    except (TypeError, ValueError):
        layer_summary["local_context_refs"] = 0
    if memory_layer_budget:
        layer_summary["memory_layer_budget"] = memory_layer_budget
    memory_layer_pressure = retrieval_memory_layer_pressure_from_retrieve(pack)
    if memory_layer_pressure:
        layer_summary["memory_layer_pressure"] = memory_layer_pressure
    async_readiness = retrieval_async_readiness_from_retrieve(pack)
    if async_readiness:
        layer_summary["async_pipeline_readiness"] = async_readiness
    return layer_summary


def retrieval_memory_layer_pressure_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    pack_view = _context_pack_view(pack)
    for source_name in ["retrieval_metrics", "recall_policy"]:
        source = pack_view.get(source_name)
        if isinstance(source, dict) and isinstance(source.get("memory_layer_pressure"), dict):
            return source["memory_layer_pressure"]
    pressure = pack_view.get("memory_layer_pressure")
    return pressure if isinstance(pressure, dict) else {}


def retrieval_async_readiness_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    pack_view = _context_pack_view(pack)
    for source_name in ["retrieval_metrics", "recall_policy"]:
        source = pack_view.get(source_name)
        if isinstance(source, dict) and isinstance(source.get("async_pipeline_readiness"), dict):
            return source["async_pipeline_readiness"]
    readiness = pack_view.get("async_pipeline_readiness")
    return readiness if isinstance(readiness, dict) else {}


def inferred_live_ref_layer_budget(refs: list[Json]) -> Json:
    budget: Json = {
        "by_memory_scope": {},
        "by_session_continuity": {},
        "by_extraction_phase": {},
        "by_ref_type": {},
        "by_entity_type": {},
        "by_source_role": {},
        "by_hook_type": {},
        "by_codex_event": {},
        "final_session_boundary_ref_count": 0,
        "provisional_ref_count": 0,
        "final_ref_count": 0,
        "total_selected_refs": len(refs),
        "total_selected_tokens": 0,
    }

    def add(bucket_name: str, key: str, token_estimate: int) -> None:
        if not key:
            return
        bucket = budget[bucket_name].setdefault(key, {"refs": 0, "tokens": 0})
        bucket["refs"] += 1
        bucket["tokens"] += token_estimate

    for ref in refs:
        text = _ref_text(ref).lstrip().lower()
        try:
            token_estimate = max(0, int(ref.get("token_estimate") or 0))
        except (TypeError, ValueError):
            token_estimate = 0
        if token_estimate == 0:
            token_estimate = max(1, (len(_ref_text(ref)) + 3) // 4)
        budget["total_selected_tokens"] += token_estimate
        ref_type = str(ref.get("ref_type") or ref.get("context_class") or "ref")
        add("by_ref_type", ref_type, token_estimate)
        memory_scope = str(ref.get("memory_scope") or "session")
        continuity = str(ref.get("session_continuity") or "same_session")
        extraction_phase = str(ref.get("extraction_phase") or "provisional")
        add("by_memory_scope", memory_scope, token_estimate)
        add("by_session_continuity", continuity, token_estimate)
        add("by_extraction_phase", extraction_phase, token_estimate)
        if extraction_phase == "final":
            budget["final_ref_count"] += 1
        else:
            budget["provisional_ref_count"] += 1
        entity_type = str(ref.get("entity_type") or "")
        source_roles = ref.get("source_roles") if isinstance(ref.get("source_roles"), list) else []
        hook_types = ref.get("source_hook_types") if isinstance(ref.get("source_hook_types"), list) else []
        codex_events = ref.get("source_codex_events") if isinstance(ref.get("source_codex_events"), list) else []
        tool_evidence_terms = (
            "tool:",
            "exit code:",
            "head -> main",
            "tests in ",
            " ran ",
            "cargo test",
            "pytest",
            "unittest",
        )
        if not entity_type and any(term in text for term in tool_evidence_terms):
            entity_type = "tool_evidence"
            source_roles = source_roles or ["tool"]
            hook_types = hook_types or ["tool_result"]
            codex_events = codex_events or ["PostToolUse"]
        elif not entity_type and text.startswith("assistant:"):
            entity_type = "assistant_decision"
            source_roles = source_roles or ["assistant"]
            hook_types = hook_types or ["after_llm"]
        add("by_entity_type", entity_type, token_estimate)
        for role in source_roles:
            add("by_source_role", str(role or ""), token_estimate)
        for hook_type in hook_types:
            add("by_hook_type", str(hook_type or ""), token_estimate)
        for codex_event in codex_events:
            add("by_codex_event", str(codex_event or ""), token_estimate)
    return budget


def retrieval_memory_hierarchy_contract_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    return memory_hierarchy_contract_from_recall_policy(recall_policy)


def retrieval_session_identity_from_retrieve(pack: Json | None, *, session_id_source: str = "") -> Json:
    source = str(session_id_source or "").strip()
    if isinstance(pack, dict):
        recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
        session_identity = recall_policy.get("session_identity") if isinstance(recall_policy.get("session_identity"), dict) else {}
        if isinstance(session_identity, dict) and session_identity.get("session_id_source"):
            return session_identity
    if not source:
        return {}
    fallback = source in {"state_file", "state_file_created", "workspace_hash"}
    return {
        "session_id_source": source,
        "strong_session_identity": source in {"explicit", "payload_field", "payload_path_hash"} or source.startswith(("payload.", "env.")),
        "fallback_session_identity": fallback,
        "risk": "workspace_fallback_may_merge_multiple_codex_tasks" if fallback else "",
        "source": "hook_metadata_fallback",
    }


def session_commit_memory_layers_written(commit: Json | None) -> Json:
    if not isinstance(commit, dict) or not commit:
        return {}
    entities_written = _int_field(commit, "entities_written")
    profile_entities_written = _int_field(commit, "profile_entities_written")
    summary_refresh = commit.get("summary_refresh") if isinstance(commit.get("summary_refresh"), dict) else {}
    summary_dirty_hashes = summary_refresh.get("dirty_hashes") if isinstance(summary_refresh.get("dirty_hashes"), list) else []
    memory_layers_written: Json = {
        "context_events": _int_field(commit, "extraction_context_event_count"),
        "segments": _int_field(commit, "segments_written"),
        "session_entities": entities_written,
        "profile_entities": profile_entities_written,
        "same_session_entities": entities_written,
        "cross_session_entities": profile_entities_written,
        "secondary_indexes": _int_field(commit, "indexes_written"),
        "summary_dirty_nodes": len(summary_dirty_hashes),
        "summary_refresh_status": summary_refresh.get("status"),
        "extraction_phase": commit.get("extraction_phase"),
        "final_session_boundary": commit.get("final_session_boundary"),
        "source_roles": commit.get("source_roles"),
        "source_hook_types": commit.get("source_hook_types"),
        "source_codex_events": commit.get("source_codex_events"),
    }
    return {
        key: value
        for key, value in memory_layers_written.items()
        if value not in (None, "", [], {})
    }


def session_commit_summary(commit: Json | None) -> Json:
    if not isinstance(commit, dict) or not commit:
        return {}
    trigger_evidence = commit.get("trigger_evidence") if isinstance(commit.get("trigger_evidence"), dict) else {}
    entities_written = _int_field(commit, "entities_written")
    profile_entities_written = _int_field(commit, "profile_entities_written")
    summary: Json = {
        "status": commit.get("status"),
        "commit_id_hash": commit.get("commit_id_hash"),
        "commit_reason": commit.get("commit_reason"),
        "trigger_policy": commit.get("trigger_policy"),
        "extraction_phase": commit.get("extraction_phase"),
        "final_session_boundary": commit.get("final_session_boundary"),
        "source_event_count": commit.get("committed_event_count", len(commit.get("source_event_ids", []))),
        "extraction_context_event_count": commit.get("extraction_context_event_count", 0),
        "source_roles": commit.get("source_roles"),
        "source_hook_types": commit.get("source_hook_types"),
        "source_codex_events": commit.get("source_codex_events"),
        "profile_promotion_summary": commit.get("profile_promotion_summary"),
        "entity_type_counts": commit.get("entity_type_counts"),
        "source_role_counts": commit.get("source_role_counts"),
        "source_hook_type_counts": commit.get("source_hook_type_counts"),
        "source_codex_event_counts": commit.get("source_codex_event_counts"),
        "segments_written": commit.get("segments_written", 0),
        "entities_written": entities_written,
        "session_entities_written": entities_written,
        "profile_entities_written": profile_entities_written,
        "memory_layers_written": session_commit_memory_layers_written(commit),
        "indexes_written": commit.get("indexes_written", 0),
        "index_total_cap": commit.get("index_total_cap"),
        "index_emitted_count": commit.get("index_emitted_count"),
        "index_dropped_by_total_cap_count": commit.get("index_dropped_by_total_cap_count"),
        "raw_events_duplicated": commit.get("raw_events_duplicated"),
    }
    if trigger_evidence:
        summary["trigger_evidence"] = {
            key: trigger_evidence.get(key)
            for key in [
                "pending_event_count",
                "pending_message_count",
                "threshold_messages",
                "threshold_ready",
                "idle_timeout_ms",
                "idle_elapsed_ms",
                "idle_ready",
                "force",
                "commit_reason",
            ]
            if trigger_evidence.get(key) not in (None, "", [], {})
        }
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def auto_batch_decision_summary(result: Json | None) -> Json:
    if not isinstance(result, dict) or not result:
        return {}
    session_buffer = result.get("session_buffer") if isinstance(result.get("session_buffer"), dict) else {}
    auto_batch = (
        result.get("auto_batch_extract_result")
        if isinstance(result.get("auto_batch_extract_result"), dict)
        else {}
    )
    session_commit = result.get("session_commit") if isinstance(result.get("session_commit"), dict) else {}
    idle_commit = result.get("idle_commit_result") if isinstance(result.get("idle_commit_result"), dict) else {}
    summary: Json = {
        "pending_event_count": session_buffer.get("pending_event_count"),
        "threshold_messages": session_buffer.get("threshold_messages"),
        "threshold_ready": session_buffer.get("threshold_ready"),
        "idle_timeout_ms": session_buffer.get("idle_commit_timeout_ms"),
        "idle_ready": session_buffer.get("idle_ready"),
        "pre_ingest_idle_ready": session_buffer.get("pre_ingest_idle_ready"),
        "pre_ingest_idle_elapsed_ms": session_buffer.get("pre_ingest_idle_elapsed_ms"),
        "pending_before_ingest_count": session_buffer.get("pending_before_ingest_count"),
        "pending_after_ingest_count": session_buffer.get("pending_after_ingest_count"),
        "commit_after_current_ingest": session_buffer.get("commit_after_current_ingest"),
        "auto_batch_extract": session_buffer.get("auto_batch_extract"),
        "boundary_commit_requested": session_buffer.get("boundary_commit_requested"),
    }
    def add_commit_evidence(source: Json) -> None:
        memory_layers = source.get("memory_layers_written")
        if not isinstance(memory_layers, dict) or not memory_layers:
            memory_layers = session_commit_memory_layers_written(source)
        if memory_layers:
            summary["memory_layers_written"] = memory_layers
        summary_refresh = source.get("summary_refresh")
        if isinstance(summary_refresh, dict) and summary_refresh:
            summary["summary_refresh"] = summary_refresh
        for field in [
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "extraction_context_event_count",
            "segments_written",
            "entities_written",
            "profile_entities_written",
            "indexes_written",
            "extraction_phase",
            "final_session_boundary",
        ]:
            value = source.get(field)
            if value not in (None, "", [], {}):
                summary[field] = value
    if auto_batch:
        summary["auto_batch_extract_status"] = auto_batch.get("status")
        summary["decision"] = "committed" if auto_batch.get("status") in {"accepted", "committed"} else "attempted"
        summary["reason"] = auto_batch.get("reason") or auto_batch.get("commit_reason")
        summary["source_roles"] = auto_batch.get("source_roles")
        summary["source_hook_types"] = auto_batch.get("source_hook_types")
        summary["source_codex_events"] = auto_batch.get("source_codex_events")
        summary["profile_promotion_summary"] = auto_batch.get("profile_promotion_summary")
        add_commit_evidence(auto_batch)
    elif session_commit:
        summary["decision"] = "boundary_commit"
        summary["reason"] = session_commit.get("reason") or session_commit.get("commit_reason")
        summary["source_roles"] = session_commit.get("source_roles")
        summary["source_hook_types"] = session_commit.get("source_hook_types")
        summary["source_codex_events"] = session_commit.get("source_codex_events")
        summary["profile_promotion_summary"] = session_commit.get("profile_promotion_summary")
        add_commit_evidence(session_commit)
    elif idle_commit and idle_commit.get("status") in {"accepted", "committed"}:
        summary["auto_batch_extract_status"] = idle_commit.get("status")
        summary["decision"] = "idle_commit"
        summary["reason"] = idle_commit.get("reason") or idle_commit.get("commit_reason") or "idle_timeout"
        summary["source_roles"] = idle_commit.get("source_roles")
        summary["source_hook_types"] = idle_commit.get("source_hook_types")
        summary["source_codex_events"] = idle_commit.get("source_codex_events")
        summary["profile_promotion_summary"] = idle_commit.get("profile_promotion_summary")
        add_commit_evidence(idle_commit)
    elif session_buffer:
        summary["decision"] = "deferred"
        if session_buffer.get("auto_batch_extract") is False:
            summary["reason"] = "auto_batch_extract_disabled"
        elif session_buffer.get("threshold_ready") is False:
            summary["reason"] = "threshold_not_reached"
        elif session_buffer.get("idle_ready") is False:
            summary["reason"] = "idle_timeout_not_reached"
        else:
            summary["reason"] = "no_auto_batch_commit_result"
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def _format_retrieval_layer_summary(layer_summary: Json) -> str:
    if not isinstance(layer_summary, dict) or not layer_summary:
        return ""
    counts = layer_summary.get("selected_ref_counts")
    count_bits = []
    if isinstance(counts, dict):
        for key in sorted(counts):
            try:
                value = int(counts[key])
            except (TypeError, ValueError):
                continue
            if value > 0:
                count_bits.append(f"{key}={value}")
    continuity_bits = []
    for key in [
        "same_session_refs",
        "cross_session_refs",
        "entity_bridge_refs",
        "session_memory_refs",
        "profile_memory_refs",
        "local_context_refs",
    ]:
        try:
            value = int(layer_summary.get(key) or 0)
        except (TypeError, ValueError):
            continue
        if value > 0:
            continuity_bits.append(f"{key}={value}")
    memory_layer_budget = layer_summary.get("memory_layer_budget") if isinstance(layer_summary.get("memory_layer_budget"), dict) else {}
    budget_bits = _format_memory_layer_budget_bits(memory_layer_budget)
    try:
        final_boundary_refs = int(memory_layer_budget.get("final_session_boundary_ref_count") or 0)
    except (TypeError, ValueError):
        final_boundary_refs = 0
    if final_boundary_refs > 0:
        budget_bits.append(f"final_boundary_refs={final_boundary_refs}")
    memory_layer_pressure = (
        layer_summary.get("memory_layer_pressure")
        if isinstance(layer_summary.get("memory_layer_pressure"), dict)
        else {}
    )
    pressure_bits = _format_memory_layer_pressure_bits(memory_layer_pressure)
    readiness = layer_summary.get("async_pipeline_readiness")
    readiness_bits = []
    if isinstance(readiness, dict):
        try:
            task_count = int(readiness.get("task_count") or 0)
        except (TypeError, ValueError):
            task_count = 0
        readiness_bits.append(f"tasks={task_count}")
        if "ready_for_retrieval" in readiness:
            readiness_bits.append(f"ready={str(bool(readiness.get('ready_for_retrieval'))).lower()}")
        remaining = readiness.get("remaining_stages")
        if isinstance(remaining, list) and remaining:
            readiness_bits.append("remaining=" + ",".join(str(item) for item in remaining[:4]))
        for label, field in [
            ("stage_counts", "remaining_stage_counts"),
            ("pending_roles", "pending_source_roles"),
            ("pending_hooks", "pending_source_hook_types"),
            ("pending_codex_events", "pending_source_codex_events"),
            ("pending_scopes", "pending_memory_scopes"),
            ("pending_continuity", "pending_session_continuities"),
            ("pending_phases", "pending_extraction_phases"),
        ]:
            bucket = readiness.get(field)
            if not isinstance(bucket, dict) or not bucket:
                continue
            bucket_bits = []
            for key in sorted(bucket):
                try:
                    count = int(bucket[key])
                except (TypeError, ValueError):
                    continue
                if count > 0:
                    bucket_bits.append(f"{key}={count}")
            if bucket_bits:
                readiness_bits.append(f"{label}[" + ",".join(bucket_bits[:6]) + "]")
        try:
            final_boundary_count = int(readiness.get("pending_final_session_boundary_count") or 0)
        except (TypeError, ValueError):
            final_boundary_count = 0
        if final_boundary_count > 0:
            readiness_bits.append(f"pending_final_boundary={final_boundary_count}")
        warnings = readiness.get("freshness_warnings")
        if isinstance(warnings, list) and warnings:
            readiness_bits.append("warnings=" + ",".join(str(item) for item in warnings[:3]))
    if not count_bits and not continuity_bits and not budget_bits and not pressure_bits and not readiness_bits:
        return ""
    details = []
    if count_bits:
        details.append(", ".join(count_bits))
    if continuity_bits:
        details.append(", ".join(continuity_bits))
    if budget_bits:
        details.append("memory_layer_budget: " + "; ".join(budget_bits))
    if pressure_bits:
        details.append("memory_layer_pressure: " + "; ".join(pressure_bits))
    if readiness_bits:
        details.append("async_pipeline[" + "; ".join(readiness_bits) + "]")
    return "Layer summary: " + "; ".join(details) + "."


def _format_memory_layer_budget_bits(memory_layer_budget: Json) -> list[str]:
    if not isinstance(memory_layer_budget, dict) or not memory_layer_budget:
        return []
    budget_bits = []
    for label, bucket_name in [
        ("scope", "by_memory_scope"),
        ("continuity", "by_session_continuity"),
        ("phase", "by_extraction_phase"),
        ("ref_type", "by_ref_type"),
        ("entity_type", "by_entity_type"),
        ("source_role", "by_source_role"),
        ("hook_type", "by_hook_type"),
        ("codex_event", "by_codex_event"),
    ]:
        buckets = memory_layer_budget.get(bucket_name)
        if not isinstance(buckets, dict):
            continue
        bucket_bits = []
        for bucket_key in sorted(buckets):
            bucket = buckets.get(bucket_key)
            if not isinstance(bucket, dict):
                continue
            try:
                ref_count = int(bucket.get("refs") or 0)
                token_count_estimate = int(bucket.get("tokens") or 0)
            except (TypeError, ValueError):
                continue
            if ref_count > 0:
                bucket_bits.append(f"{bucket_key}={ref_count}/{token_count_estimate}t")
        if bucket_bits:
            budget_bits.append(f"{label}[" + ", ".join(bucket_bits) + "]")
    return budget_bits


def _format_memory_layer_pressure_bits(memory_layer_pressure: Json) -> list[str]:
    if not isinstance(memory_layer_pressure, dict) or not memory_layer_pressure:
        return []
    pressure_bits = []
    for label, field in [
        ("selected", "selected_refs"),
        ("selected_tokens", "selected_tokens"),
        ("dropped", "dropped_refs"),
        ("dropped_tokens", "dropped_tokens"),
        ("pressure_buckets", "pressure_bucket_count"),
        ("dropped_buckets", "dropped_bucket_count"),
    ]:
        try:
            value = int(memory_layer_pressure.get(field) or 0)
        except (TypeError, ValueError):
            continue
        if value > 0:
            pressure_bits.append(f"{label}={value}")
    flag_bits = []
    for label, field in [
        ("profile", "profile_memory_pressure"),
        ("session", "session_memory_pressure"),
        ("cross_session", "cross_session_pressure"),
        ("same_session", "same_session_pressure"),
        ("final", "final_memory_pressure"),
        ("provisional", "provisional_memory_pressure"),
        ("assistant", "assistant_memory_pressure"),
        ("user", "user_memory_pressure"),
        ("tool", "tool_memory_pressure"),
    ]:
        if bool(memory_layer_pressure.get(field)):
            flag_bits.append(label)
    if flag_bits:
        pressure_bits.append("flags[" + ",".join(flag_bits) + "]")
    for label, field in [
        ("pressure_dimensions", "pressure_dimensions"),
        ("dropped_dimensions", "dropped_dimensions"),
    ]:
        values = memory_layer_pressure.get(field)
        if isinstance(values, list) and values:
            pressure_bits.append(f"{label}[" + ",".join(str(value) for value in values[:8]) + "]")
    return pressure_bits


def normalized_event_name(event: str) -> str:
    return "".join(ch for ch in event.lower() if ch.isalnum() or ch == "_")


def _first_string_value(payload: Json, keys: list[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _compact_one_line(value: str, *, max_chars: int = 220) -> str:
    compact = " ".join(str(value).split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 3)].rstrip() + "..."


def is_codex_hook_heartbeat_text(value: str) -> bool:
    text = " ".join(str(value or "").split())
    if not text:
        return False
    if text.startswith("user: "):
        text = text[6:].lstrip()
    return text.startswith("Codex hook heartbeat ") and "C++ TemporalStore is live and accepting MatrixArk hook writes" in text


def _ref_is_codex_hook_heartbeat(ref: Json) -> bool:
    return is_codex_hook_heartbeat_text(_ref_text(ref)) or is_codex_hook_heartbeat_text(str(ref))


def strip_codex_hook_heartbeat_lines(value: str) -> str:
    lines = str(value or "").splitlines()
    kept = [line for line in lines if not is_codex_hook_heartbeat_text(line)]
    return "\n".join(kept).strip()


def sanitized_rendered_context_from_retrieve(pack: Json | None) -> str:
    if not isinstance(pack, dict):
        return ""
    return strip_codex_hook_heartbeat_lines(
        _first_string_value(pack, ["context", "text", "compiled_context", "rendered_context"])
    )


def _context_pack_view(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    nested = pack.get("context_pack")
    if isinstance(nested, dict):
        return nested
    return pack


def _selected_refs_from_retrieve(pack: Json | None) -> list[Json]:
    if not isinstance(pack, dict):
        return []
    pack = _context_pack_view(pack)
    refs = pack.get("selected_refs")
    if not isinstance(refs, list):
        refs = pack.get("remote_context_refs")
    if isinstance(refs, list):
        return [ref for ref in refs if isinstance(ref, dict)]
    flattened: list[Json] = []
    for group_key, ref_key in (("selected_ref_groups", "refs"), ("groups", "items")):
        groups = pack.get(group_key)
        if not isinstance(groups, list):
            continue
        for group in groups:
            if not isinstance(group, dict):
                continue
            refs_in_group = group.get(ref_key)
            if isinstance(refs_in_group, list):
                flattened.extend(ref for ref in refs_in_group if isinstance(ref, dict))
    return flattened


def _ref_text(ref: Json) -> str:
    return _first_string_value(
        ref,
        [
            "text",
            "body",
            "content",
            "summary_text",
            "summary",
            "snippet",
            "chunk_text",
            "event_text",
            "entity_state",
        ],
    )


def _ref_citation(ref: Json) -> str:
    return _first_string_value(
        ref,
        [
            "citation",
            "source_ref",
            "source_locator",
            "raw_uri",
            "resource_uri",
            "node_path_text",
        ],
    )


def additional_context_from_retrieve(
    pack: Json | None,
    *,
    query: str,
    local_context_count: int,
    session_id_source: str = "",
    char_limit: int = DEFAULT_ADDITIONAL_CONTEXT_CHAR_LIMIT,
) -> str:
    """Build Codex hook additionalContext from a MatrixArk ContextPack."""
    if not isinstance(pack, dict):
        return ""
    refs = [ref for ref in _selected_refs_from_retrieve(pack) if not _ref_is_codex_hook_heartbeat(ref)]
    context_text = sanitized_rendered_context_from_retrieve(pack)
    quality_warnings = pack.get("quality_warnings")
    retrieval_metrics = pack.get("retrieval_metrics")
    budget = retrieval_budget_summary_from_retrieve(pack)
    budget_pressure = retrieval_budget_pressure_from_retrieve(pack)
    layer_summary = retrieval_layer_summary_from_retrieve(pack, refs)
    session_identity = retrieval_session_identity_from_retrieve(pack, session_id_source=session_id_source)
    budget_bits = [
        f"used_remote_tokens={budget.get('used_remote_context_tokens', 0)}",
    ]
    if budget.get("remote_context_budget_tokens"):
        budget_bits.append(f"remote_budget={budget.get('remote_context_budget_tokens')}")
        budget_bits.append(f"remote_remaining={budget.get('remote_budget_remaining_tokens', 0)}")
    if budget.get("requested_max_context_tokens"):
        budget_bits.append(f"requested_max={budget.get('requested_max_context_tokens')}")
    if budget.get("budget_source"):
        budget_bits.append(f"budget_source={budget.get('budget_source')}")
    contract = budget.get("budget_contract") if isinstance(budget.get("budget_contract"), dict) else {}
    if contract.get("mode"):
        budget_bits.append(f"contract={contract.get('mode')}")
        budget_bits.append(f"contract_holds={str(bool(contract.get('contract_holds'))).lower()}")
    lines = [
        "MatrixArk/TemporalStore retrieved context for Codex.",
        f"Query: {_compact_one_line(query, max_chars=360)}",
        (
            "Merge this remote memory with the visible local Codex context. "
            "Prefer current local files when they conflict with retrieved memory."
        ),
        (
            "Retrieval summary: "
            f"context_pack_id={pack.get('context_pack_id') or pack.get('pack_id') or ''}, "
            f"selected_refs={len(refs)}, "
            f"used_context_tokens={used_context_tokens_from_retrieve(pack)}, "
            f"local_context_refs_seen={local_context_count}."
        ),
        "Budget summary: " + ", ".join(budget_bits) + ".",
    ]
    if session_identity:
        identity_bits = [
            f"source={session_identity.get('session_id_source', '')}",
            f"strong={str(bool(session_identity.get('strong_session_identity'))).lower()}",
            f"fallback={str(bool(session_identity.get('fallback_session_identity'))).lower()}",
        ]
        if session_identity.get("risk"):
            identity_bits.append(f"risk={session_identity.get('risk')}")
        lines.append("Session identity: " + ", ".join(identity_bits) + ".")
    formatted_layer_summary = _format_retrieval_layer_summary(layer_summary)
    if formatted_layer_summary:
        lines.append(formatted_layer_summary)
    try:
        has_profile_memory = int(layer_summary.get("profile_memory_refs") or 0) > 0
    except (TypeError, ValueError):
        has_profile_memory = False
    try:
        has_cross_session_memory = int(layer_summary.get("cross_session_refs") or 0) > 0
    except (TypeError, ValueError):
        has_cross_session_memory = False
    if has_profile_memory or has_cross_session_memory:
        hierarchy_bits = [
            "session refs are turn/session-local",
            "user_profile/cross_session refs are long-term state and may supersede older session-local entity copies",
        ]
        hierarchy = retrieval_memory_hierarchy_contract_from_retrieve(pack)
        if isinstance(hierarchy, dict):
            floor_status = hierarchy.get("cross_session_budget_floor_status")
            if floor_status:
                hierarchy_bits.append(f"cross_session_budget_floor_status={floor_status}")
            for label, field in [
                ("cross_session_budget", "cross_session_budget_tokens"),
                ("computed", "cross_session_computed_budget_tokens"),
                ("floor", "cross_session_budget_floor_tokens"),
            ]:
                try:
                    value = int(hierarchy.get(field) or 0)
                except (TypeError, ValueError):
                    value = 0
                if value > 0:
                    hierarchy_bits.append(f"{label}={value}")
            if "cross_session_budget_floor_applied" in hierarchy:
                hierarchy_bits.append(
                    "floor_applied="
                    + str(bool(hierarchy.get("cross_session_budget_floor_applied"))).lower()
                )
        lines.append("Memory hierarchy: " + "; ".join(hierarchy_bits) + ".")
    if budget_pressure.get("budget_pressure"):
        dropped_by_reason = budget_pressure.get("dropped_by_reason")
        pressure_bits = []
        if isinstance(dropped_by_reason, dict):
            for reason in sorted(dropped_by_reason):
                try:
                    count = int(dropped_by_reason[reason])
                except (TypeError, ValueError):
                    continue
                if count > 0:
                    pressure_bits.append(f"{reason}={count}")
        if budget_pressure.get("deadline_exceeded"):
            pressure_bits.append("deadline_exceeded=true")
        if budget_pressure.get("budget_fill_policy"):
            pressure_bits.append(f"budget_fill_policy={budget_pressure.get('budget_fill_policy')}")
        dropped_budget = budget_pressure.get("dropped_memory_layer_budget")
        dropped_budget_bits = _format_memory_layer_budget_bits(dropped_budget if isinstance(dropped_budget, dict) else {})
        if dropped_budget_bits:
            pressure_bits.append("dropped_memory_layer_budget: " + "; ".join(dropped_budget_bits))
        if pressure_bits:
            lines.append("Budget pressure: " + ", ".join(pressure_bits) + ".")
    if isinstance(quality_warnings, list) and quality_warnings:
        warnings = []
        for warning in quality_warnings[:4]:
            if isinstance(warning, dict):
                warnings.append(_compact_one_line(str(warning.get("message") or warning.get("code") or warning)))
            else:
                warnings.append(_compact_one_line(str(warning)))
        lines.append("Quality warnings: " + " | ".join(warnings))
    if isinstance(retrieval_metrics, dict):
        fallback = retrieval_metrics.get("fallback_reason")
        if fallback:
            lines.append("Retrieval fallback: " + _compact_one_line(str(fallback), max_chars=400))

    if context_text:
        lines.append("")
        lines.append("Retrieved context:")
        lines.append(context_text.strip())
    elif refs:
        lines.append("")
        lines.append("Retrieved refs:")
        for index, ref in enumerate(refs[:24], start=1):
            ref_type = str(ref.get("ref_type") or ref.get("type") or "ref")
            citation = _ref_citation(ref)
            score = ref.get("score", ref.get("packing_score", ""))
            token_cost = ref.get("token_cost", ref.get("token_estimate", ""))
            header_bits = [f"[{index}] {ref_type}"]
            if citation:
                header_bits.append(_compact_one_line(citation, max_chars=260))
            if score != "":
                header_bits.append(f"score={score}")
            if token_cost != "":
                header_bits.append(f"tokens={token_cost}")
            lines.append(" - " + " | ".join(header_bits))
            text = _ref_text(ref)
            if text:
                lines.append("   " + _compact_one_line(text, max_chars=700))
    else:
        lines.append("")
        lines.append("No remote refs were selected for this prompt.")

    output = "\n".join(lines).strip()
    if len(output) <= char_limit:
        return output
    return output[: max(0, char_limit - 80)].rstrip() + "\n[MatrixArk context truncated by hook char limit]"


def codex_hook_output(
    *,
    args: argparse.Namespace,
    status: str,
    event: str,
    session_id_source: str,
    agent_context: Json,
    ingest: Json | None = None,
    retrieve: Json | None = None,
    commit: Json | None = None,
    raw_uri: str = "",
    resource_type: str = "",
    query: str = "",
    error: str = "",
) -> Json:
    ingest = ingest or {}
    retrieve = retrieve or {}
    commit = commit or {}
    emitted_refs = [
        ref for ref in _selected_refs_from_retrieve(retrieve) if not _ref_is_codex_hook_heartbeat(ref)
    ]
    rendered_context = sanitized_rendered_context_from_retrieve(retrieve)
    output: Json = {
        "status": status,
        "event": event,
        "session_id": args.session_id,
        "session_id_source": session_id_source,
        "agent_context_refs": len(agent_context.get("local_context", [])),
        "workspace_root": agent_context.get("workspace_root", ""),
        "lifecycle_stage": {
            "before_llm_retrieve": event == "UserPromptSubmit",
            "after_llm_ingest_only": event in {"PostToolUse", "PreToolUse", "PermissionRequest"},
            "hook_boundary_commit": event in {"Stop", "PostCompact", "SubagentStop"},
            "idle_timeout_commit": event in {"IdleTimeout", "SessionIdle"},
            "auto_threshold_commit": bool(ingest.get("auto_batch_extract_result")) if ingest else False,
        },
        "ingest": ingest,
        "resource_uri": raw_uri,
        "resource_type": resource_type,
        "retrieve": {
            "context_pack_id": retrieve.get("context_pack_id") or retrieve.get("pack_id"),
            "selected_ref_count": len(emitted_refs),
            "used_context_tokens": used_context_tokens_from_retrieve(retrieve),
            "budget": retrieval_budget_summary_from_retrieve(retrieve),
            "budget_pressure": retrieval_budget_pressure_from_retrieve(retrieve),
            "layers": retrieval_layer_summary_from_retrieve(retrieve, emitted_refs),
            "async_pipeline_readiness": retrieval_async_readiness_from_retrieve(retrieve),
            "session_identity": retrieval_session_identity_from_retrieve(retrieve, session_id_source=session_id_source),
            "memory_hierarchy": retrieval_memory_hierarchy_contract_from_retrieve(retrieve),
            "rendered_context_chars": len(rendered_context),
            "additional_context_emitted": False,
        },
        "session_commit": session_commit_summary(commit),
    }
    if error:
        output["error"] = error
    if event == "UserPromptSubmit":
        retrieve_has_context = bool(
            isinstance(retrieve, dict)
            and not retrieve.get("_hook_tool_timeout")
            and (bool(emitted_refs) or bool(rendered_context))
        )
        if error and not retrieve_has_context:
            additional_context = (
                "MatrixArk/TemporalStore retrieval was attempted for this prompt but failed. "
                "Use visible local Codex context as authoritative for this turn. "
                f"Failure: {_compact_one_line(error, max_chars=700)}"
            )
        elif retrieve_has_context:
            additional_context = additional_context_from_retrieve(
                retrieve,
                query=query,
                local_context_count=len(agent_context.get("local_context", [])),
                session_id_source=session_id_source,
            )
        else:
            additional_context = ""
        if additional_context:
            output["hookSpecificOutput"] = {
                "hookEventName": "UserPromptSubmit",
                "additionalContext": additional_context,
            }
            output["retrieve"]["additional_context_emitted"] = True
    return output


def strict_codex_stdout(output: Json) -> Json:
    """Return only fields accepted by Codex hook stdout parsers."""
    hook_specific = output.get("hookSpecificOutput")
    if isinstance(hook_specific, dict):
        return {"hookSpecificOutput": hook_specific}
    return {}


def hook_trace_payload_keys(payload: Json) -> list[str]:
    return sorted(str(key) for key in payload.keys())[:80] if isinstance(payload, dict) else []


def begin_hook_trace(
    *,
    args: argparse.Namespace,
    payload: Json,
    text: str,
    session_id_source: str,
    raw_uri: str = "",
) -> Json:
    started_at_ms = int(time.time() * 1000)
    return {
        "record_type": "codex_hook_trace",
        "trace_version": 1,
        "trace_id": f"codex:{args.event}:{started_at_ms}:{uuid.uuid4().hex[:12]}",
        "agent": "codex",
        "event": args.event,
        "backend": args.backend,
        "storage_prefix": args.storage_prefix,
        "session_id": args.session_id,
        "session_id_source": session_id_source,
        "account_id": args.account_id,
        "tenant_id": args.tenant_id,
        "user_id": args.user_id,
        "team": args.team,
        "project": args.project,
        "workspace_root": "",
        "raw_uri": raw_uri,
        "payload_keys": hook_trace_payload_keys(payload),
        "text_hash": stable_short_hash(text) if text else "",
        "text_preview": _compact_one_line(text, max_chars=260) if text else "",
        "started_at_ms": started_at_ms,
        "completed_at_ms": 0,
        "elapsed_ms": 0,
        "status": "started",
        "skip_reason": "",
        "tool_calls": [],
        "output_summary": {},
        "error": "",
    }


def trace_tool_call(server: Any, name: str, args: Json, trace: Json) -> Json:
    started_at_ms = int(time.time() * 1000)
    started_perf = time.perf_counter()
    item: Json = {
        "tool": name,
        "started_at_ms": started_at_ms,
        "elapsed_ms": 0,
        "status": "started",
    }
    trace.setdefault("tool_calls", []).append(item)
    try:
        timeout_ms = HOOK_RETRIEVE_TIMEOUT_MS if name == "matrixark_retrieve" else HOOK_TOOL_CALL_TIMEOUT_MS
        call_result = _run_best_effort_with_timeout(name, timeout_ms, call_tool, server, name, args)
        if call_result.get("status") == "timeout":
            result = {
                "status": "timeout",
                "_hook_tool_timeout": True,
                "tool": name,
                "timeout_ms": timeout_ms,
                "warning": (
                    f"{name} exceeded the Codex hook boundary timeout. "
                    "The hook returned partial output instead of blocking the agent."
                ),
            }
            item["status"] = "timeout"
            item["result"] = {"status": "timeout", "timeout_ms": timeout_ms}
            return result
        if call_result.get("status") != "ok":
            raise RuntimeError(str(call_result.get("error") or call_result))
        result_value = call_result.get("value")
        result = result_value if isinstance(result_value, dict) else {}
        item["status"] = "ok"
        if name == "matrixark_ingest":
            auto_batch_extract_result = (
                result.get("auto_batch_extract_result")
                if isinstance(result.get("auto_batch_extract_result"), dict)
                else {}
            )
            idle_commit_result = (
                result.get("idle_commit_result")
                if isinstance(result.get("idle_commit_result"), dict)
                else {}
            )
            item["result"] = {
                "status": result.get("status"),
                "event_id_hash": result.get("event_id_hash"),
                "node_hash": result.get("node_hash"),
                "hook_captured": result.get("hook_captured"),
                "auto_batch_extract_status": auto_batch_extract_result.get("status"),
                "idle_commit_status": idle_commit_result.get("status"),
                "idle_commit": session_commit_summary(idle_commit_result),
                "auto_batch_extract": session_commit_summary(auto_batch_extract_result),
                "auto_batch_extract_decision": auto_batch_decision_summary(result),
            }
        elif name == "matrixark_retrieve":
            emitted_refs = [
                ref for ref in _selected_refs_from_retrieve(result) if not _ref_is_codex_hook_heartbeat(ref)
            ]
            item["result"] = {
                "context_pack_id": result.get("context_pack_id") or result.get("pack_id"),
                "selected_ref_count": len(emitted_refs),
                "used_context_tokens": used_context_tokens_from_retrieve(result),
                "retrieval_budget": retrieval_budget_summary_from_retrieve(result),
                "retrieval_budget_pressure": retrieval_budget_pressure_from_retrieve(result),
                "retrieval_layers": retrieval_layer_summary_from_retrieve(result, emitted_refs),
                "async_pipeline_readiness": retrieval_async_readiness_from_retrieve(result),
                "session_identity": retrieval_session_identity_from_retrieve(
                    result,
                    session_id_source=str((args.get("metadata") if isinstance(args.get("metadata"), dict) else {}).get("session_id_source") or ""),
                ),
                "memory_hierarchy": retrieval_memory_hierarchy_contract_from_retrieve(result),
                "rendered_context_chars": len(sanitized_rendered_context_from_retrieve(result)),
            }
        elif name == "matrixark_session_commit":
            item["result"] = session_commit_summary(result)
        return result
    except Exception as exc:
        item["status"] = "error"
        item["error"] = _compact_one_line(f"{type(exc).__name__}: {exc}", max_chars=700)
        raise
    finally:
        item["elapsed_ms"] = round((time.perf_counter() - started_perf) * 1000.0, 3)


def tool_call_timed_out(result: Json | None) -> bool:
    return bool(isinstance(result, dict) and (result.get("_hook_tool_timeout") or result.get("status") == "timeout"))


def timeout_warning(result: Json | None) -> str:
    if not tool_call_timed_out(result):
        return ""
    tool = str(result.get("tool") or "MatrixArk tool") if isinstance(result, dict) else "MatrixArk tool"
    timeout_ms = result.get("timeout_ms") if isinstance(result, dict) else None
    suffix = f" after {timeout_ms}ms" if timeout_ms else ""
    return f"{tool} timed out at the Codex hook boundary{suffix}; returned partial hook output."


def _run_best_effort_with_timeout(name: str, timeout_ms: int, fn: Any, *args: Any, **kwargs: Any) -> Json:
    if timeout_ms <= 0:
        try:
            value = fn(*args, **kwargs)
            return {"status": "ok", "elapsed_ms": 0.0, "value": value}
        except Exception as exc:
            return {"status": "error", "error": _compact_one_line(f"{type(exc).__name__}: {exc}", max_chars=700)}
    result: Json = {"status": "running"}

    def runner() -> None:
        started = time.perf_counter()
        try:
            value = fn(*args, **kwargs)
            result.update({"status": "ok", "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3), "value": value})
        except Exception as exc:
            result.update(
                {
                    "status": "error",
                    "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
                    "error": _compact_one_line(f"{type(exc).__name__}: {exc}", max_chars=700),
                }
            )

    thread = threading.Thread(target=runner, name=f"matrixark-hook-{name}", daemon=True)
    thread.start()
    thread.join(timeout=max(0.001, timeout_ms / 1000.0))
    if thread.is_alive():
        return {"status": "timeout", "timeout_ms": timeout_ms}
    return result


def append_hook_trace(server: Any, trace: Json, *, output: Json | None = None, status: str = "ok", skip_reason: str = "", error: str = "") -> None:
    if HOOK_COMPACT_HOT_PREFIX_ONLY:
        return
    completed_at_ms = int(time.time() * 1000)
    trace["completed_at_ms"] = completed_at_ms
    try:
        trace["elapsed_ms"] = max(0, completed_at_ms - int(trace.get("started_at_ms") or completed_at_ms))
    except (TypeError, ValueError):
        trace["elapsed_ms"] = 0
    trace["status"] = status
    if skip_reason:
        trace["skip_reason"] = skip_reason
    if error:
        trace["error"] = _compact_one_line(error, max_chars=1000)
    if isinstance(output, dict):
        hook_specific = output.get("hookSpecificOutput") if isinstance(output.get("hookSpecificOutput"), dict) else {}
        retrieve = output.get("retrieve") if isinstance(output.get("retrieve"), dict) else {}
        ingest = output.get("ingest") if isinstance(output.get("ingest"), dict) else {}
        commit = output.get("session_commit") if isinstance(output.get("session_commit"), dict) else {}
        auto_batch_extract = (
            ingest.get("auto_batch_extract")
            if isinstance(ingest.get("auto_batch_extract"), dict)
            else {}
        )
        auto_batch_decision = (
            ingest.get("auto_batch_extract_decision")
            if isinstance(ingest.get("auto_batch_extract_decision"), dict)
            else {}
        )
        idle_commit = ingest.get("idle_commit") if isinstance(ingest.get("idle_commit"), dict) else {}
        trace["output_summary"] = {
            "strict_additional_context_emitted": bool(hook_specific.get("additionalContext")),
            "additional_context_chars": len(str(hook_specific.get("additionalContext") or "")),
            "context_pack_id": retrieve.get("context_pack_id"),
            "selected_ref_count": retrieve.get("selected_ref_count"),
            "retrieval_budget": retrieve.get("budget"),
            "retrieval_budget_pressure": retrieve.get("budget_pressure"),
            "retrieval_layers": retrieve.get("layers"),
            "async_pipeline_readiness": retrieve.get("async_pipeline_readiness"),
            "memory_hierarchy": retrieve.get("memory_hierarchy"),
            "rendered_context_chars": retrieve.get("rendered_context_chars"),
            "ingest_status": ingest.get("status"),
            "auto_batch_extract_status": ingest.get("auto_batch_extract_status"),
            "auto_batch_extract": auto_batch_extract,
            "auto_batch_extract_decision": auto_batch_decision,
            "idle_commit_status": ingest.get("idle_commit_status"),
            "idle_commit": idle_commit,
            "commit_status": commit.get("status"),
            "session_commit": session_commit_summary(commit),
        }
    adapter = getattr(server, "adapter", None)
    append = getattr(adapter, "append", None)
    if callable(append):
        append_result = _run_best_effort_with_timeout("trace-append", HOOK_TRACE_APPEND_TIMEOUT_MS, append, trace)
        if append_result.get("status") != "ok":
            trace["trace_append_status"] = append_result


def close_server_best_effort(server: Any, *, timeout_ms: int = HOOK_CLOSE_TIMEOUT_MS) -> None:
    close = getattr(server, "close", None)
    if not callable(close):
        return
    close_timeout_s = max(0.001, timeout_ms / 1000.0)
    result = _run_best_effort_with_timeout("close", timeout_ms, close, timeout_s=close_timeout_s)
    if result.get("status") != "ok":
        _mcp_debug_log(f"matrixark hook close skipped after {timeout_ms}ms: {result}")


def is_resource_event(event: str) -> bool:
    return normalized_event_name(event) in RESOURCE_EVENTS


def load_matrixark(root: Path):
    sys.path.insert(0, str(root))
    from tools.matrixark_mcp_server import (  # type: ignore
        MatrixArkLocalAdapter,
        MatrixArkMcpServer,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
    )

    return (
        MatrixArkLocalAdapter,
        MatrixArkMcpServer,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
    )


def production_profile_enabled() -> bool:
    return os.environ.get("MATRIXARK_MCP_PROFILE", "").strip().lower() in {"prod", "production", "benchmark", "bench", "parity"}


def local_backend_allowed() -> bool:
    return os.environ.get("MATRIXARK_ALLOW_LOCAL_BACKEND", "").strip().lower() in {"1", "true", "yes", "on"}


def default_hook_backend() -> str:
    configured = os.environ.get("MATRIXARK_MCP_BACKEND")
    if configured:
        return configured
    return "temporalstore-direct"


def validate_hook_backend_policy(backend: str) -> None:
    if backend == "local" and local_backend_allowed():
        return
    if backend not in {"temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"}:
        raise RuntimeError(
            "MatrixArk hooks no longer support local JSONL event logs; "
            "use --backend temporalstore-direct, --backend temporalstore-rust, or --backend temporalstore-rust-direct."
        )


def hook_idempotency_key(payload: Json, *, event: str, session_id: str | None, fallback: str = "") -> str:
    value = payload.get("id") or payload.get("turn_id") or payload.get("message_id") or payload.get("request_id") or fallback
    if value:
        return str(value)
    fingerprint = f"{event}:{session_id or ''}:{time.time_ns()}:{uuid.uuid4().hex}"
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:32]


def hook_lineage_fields(hook: Json | None) -> Json:
    if not isinstance(hook, dict):
        return {}
    lineage: Json = {}
    for field in ("thread_id", "turn_id", "conversation_id"):
        value = hook.get(field)
        if value not in (None, ""):
            lineage[field] = str(value)
    return lineage


def codex_agent_hook(
    *,
    hook_type: str,
    hook_id: str,
    idempotency_key: str,
    trigger: str,
    session_id_source: str,
    identity: Json | None = None,
    observed_at_ms: int | None = None,
) -> Json:
    return {
        "source": "codex",
        "hook_type": hook_type,
        "hook_id": hook_id,
        "observed_at_ms": observed_at_ms if observed_at_ms is not None else int(time.time() * 1000),
        "idempotency_key": idempotency_key,
        "trigger": trigger,
        "auto_captured": True,
        "session_id_source": session_id_source,
        **hook_lineage_fields(identity),
    }


def codex_hook_lineage_from_payload(payload: Json, args: argparse.Namespace, *, session_id_source: str) -> Json:
    thread_id = first_string_at(
        payload,
        [
            ["thread_id"],
            ["threadId"],
            ["conversation_id"],
            ["conversationId"],
            ["transcript_id"],
            ["transcriptId"],
            ["run", "thread_id"],
            ["params", "thread_id"],
            ["turn", "thread_id"],
            ["metadata", "thread_id"],
        ],
    )
    turn_id = first_string_at(
        payload,
        [
            ["turn_id"],
            ["turnId"],
            ["id"],
            ["message_id"],
            ["request_id"],
            ["run", "turn_id"],
            ["params", "turn_id"],
            ["turn", "id"],
            ["turn", "turn_id"],
            ["metadata", "turn_id"],
        ],
    )
    conversation_id = first_string_at(
        payload,
        [
            ["conversation_id"],
            ["conversationId"],
            ["session_id"],
            ["sessionId"],
            ["codex_session_id"],
            ["metadata", "conversation_id"],
        ],
    )
    return {
        "session_id": args.session_id,
        "session_id_source": session_id_source,
        **({"thread_id": thread_id} if thread_id else {}),
        **({"turn_id": turn_id} if turn_id else {}),
        **({"conversation_id": conversation_id} if conversation_id else {}),
    }


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Ingest Codex hook payloads into MatrixArk.")
    parser.add_argument("--event", default=os.environ.get("CODEX_HOOK_EVENT", "UserPromptSubmit"))
    parser.add_argument(
        "--backend",
        choices=["temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct", "local"],
        default=default_hook_backend(),
    )
    parser.add_argument("--event-log", type=Path, default=Path(os.environ.get("MATRIXARK_EVENT_LOG", "")) if os.environ.get("MATRIXARK_EVENT_LOG") else None)
    parser.add_argument("--api-key", default=os.environ.get("MATRIXARK_API_KEY", ""))
    parser.add_argument("--account-id", default=os.environ.get("MATRIXARK_ACCOUNT_ID", "acct_codex"))
    parser.add_argument("--tenant-id", default=os.environ.get("MATRIXARK_TENANT_ID", "tenant_codex"))
    parser.add_argument("--user-id", default=os.environ.get("MATRIXARK_USER_ID", os.environ.get("USERNAME", "codex_user")))
    parser.add_argument("--session-id", default=os.environ.get("MATRIXARK_SESSION_ID"))
    parser.add_argument(
        "--session-state-dir",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_CODEX_SESSION_STATE_DIR", "/tmp/matrixark-codex-sessions")),
        help="Directory used for the fallback generated Codex hook session id.",
    )
    parser.add_argument("--team", default=os.environ.get("MATRIXARK_TEAM", "codex"))
    parser.add_argument("--project", default=os.environ.get("MATRIXARK_PROJECT", "local"))
    parser.add_argument("--query", default="")
    parser.add_argument("--max-context-tokens", type=int, default=int(os.environ.get("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", os.environ.get("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", "128000"))))
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--temporalstore-lib", default=os.environ.get("TEMPORALSTORE_LIB", ""))
    parser.add_argument("--rust-proxy", default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_PROXY", os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", "")))
    parser.add_argument("--rust-direct-sdk", default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK", ""))
    parser.add_argument("--rust-cli", default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", ""))
    parser.add_argument("--storage-prefix", default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:codex-hook"))
    parser.add_argument("--request-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "60000")))
    parser.add_argument("--io-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "60000")))
    parser.add_argument("--session-commit-threshold", type=int, default=int(os.environ.get("MATRIXARK_SESSION_COMMIT_THRESHOLD", "20")))
    parser.add_argument("--idle-commit-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", "0")))
    parser.add_argument("--understanding-provider", default=os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", "rules"))
    parser.add_argument("--segment-provider", default=os.environ.get("MATRIXARK_SEGMENT_PROVIDER", "deterministic"))
    parser.add_argument("--repo-root", type=Path, default=root)
    parser.add_argument("--rollout-backfill-only", action="store_true", default=False)
    parser.add_argument(
        "--rollout-backfill-delay-ms",
        type=int,
        default=int(os.environ.get("MATRIXARK_ROLLOUT_BACKFILL_DELAY_MS", "3500")),
    )
    parser.add_argument(
        "--codex-strict-output",
        action="store_true",
        default=os.environ.get("MATRIXARK_CODEX_STRICT_OUTPUT", "").strip().lower() in {"1", "true", "yes", "on"},
        help="Emit only Codex-supported hook stdout fields; rich audit JSON is for manual diagnostics only.",
    )
    return parser.parse_args()


def read_stdin_payload() -> Json:
    raw = sys.stdin.read()
    raw = raw.lstrip("\ufeff")
    if not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw_text": raw}
    return value if isinstance(value, dict) else {"payload": value}


def first_string_at(payload: Json, paths: list[list[str]]) -> str:
    for path in paths:
        value: Any = payload
        for part in path:
            if not isinstance(value, dict) or part not in value:
                value = None
                break
            value = value[part]
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def text_from_content_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = [text_from_content_value(item) for item in value]
        return "\n".join(part for part in parts if part).strip()
    if not isinstance(value, dict):
        return ""

    direct_parts = [
        text_from_content_value(value.get(key))
        for key in ["text", "output_text", "message", "summary"]
    ]
    direct = "\n".join(part for part in direct_parts if part).strip()
    if direct:
        return direct
    nested_parts = [
        text_from_content_value(value.get(key))
        for key in ["content", "output", "response"]
    ]
    return "\n".join(part for part in nested_parts if part).strip()


def stable_short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def stable_int_hash(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:16], 16) & ((1 << 63) - 1)


def payload_session_candidate(payload: Json) -> tuple[str, str]:
    direct = first_string_at(
        payload,
        [
            ["session_id"],
            ["sessionId"],
            ["codex_session_id"],
            ["thread_id"],
            ["threadId"],
            ["conversation_id"],
            ["conversationId"],
            ["transcript_id"],
            ["transcriptId"],
            ["run", "session_id"],
            ["run", "thread_id"],
            ["params", "session_id"],
            ["params", "thread_id"],
            ["turn", "session_id"],
            ["turn", "thread_id"],
            ["metadata", "session_id"],
            ["metadata", "thread_id"],
        ],
    )
    if direct:
        return f"codex:{direct}", "payload_field"

    path_value = first_string_at(
        payload,
        [
            ["transcript_path"],
            ["transcriptPath"],
            ["conversation_path"],
            ["conversationPath"],
            ["thread_path"],
            ["threadPath"],
            ["log_path"],
            ["logPath"],
        ],
    )
    if path_value:
        return f"codex:path:{stable_short_hash(path_value)}", "payload_path_hash"

    return "", ""


def workspace_fingerprint(payload: Json, args: argparse.Namespace) -> str:
    workspace = first_string_at(
        payload,
        [
            ["workspace_root"],
            ["workspaceRoot"],
            ["cwd"],
            ["params", "cwd"],
            ["metadata", "cwd"],
        ],
    )
    if not workspace:
        workspace = str(args.repo_root)
    seed = "|".join([args.account_id, args.tenant_id, args.user_id, workspace])
    return stable_short_hash(seed)


def generated_session_id(payload: Json, args: argparse.Namespace) -> tuple[str, str]:
    args.session_state_dir.mkdir(parents=True, exist_ok=True)
    state_file = args.session_state_dir / f"{workspace_fingerprint(payload, args)}.session"
    if state_file.exists():
        existing = state_file.read_text(encoding="utf-8").strip()
        if existing:
            return existing, "state_file"
    value = f"codex:local:{uuid.uuid4().hex[:16]}"
    state_file.write_text(value + "\n", encoding="utf-8")
    return value, "state_file_created"


def resolve_session_id(payload: Json, args: argparse.Namespace) -> tuple[str, str]:
    if args.session_id:
        return args.session_id, "explicit"
    candidate, source = payload_session_candidate(payload)
    if candidate:
        return candidate, source
    return generated_session_id(payload, args)



def _windows_codex_sessions_root_from_payload(payload: Json) -> Path | None:
    candidates: list[Path] = []
    home_drive = os.environ.get("USERPROFILE") or os.environ.get("HOME")
    if home_drive:
        candidates.append(Path(home_drive) / ".codex" / "sessions")
    for path_value in [
        first_string_at(payload, [["cwd"], ["workspace_root"], ["workspaceRoot"], ["params", "cwd"]]),
        first_string_at(payload, [["transcript_path"], ["transcriptPath"], ["conversation_path"], ["conversationPath"], ["log_path"], ["logPath"]]),
    ]:
        if not path_value:
            continue
        normalized = path_value.replace("\\", "/")
        if len(normalized) >= 3 and normalized[1:3] == ":/":
            drive = normalized[0].lower()
            parts = normalized[3:].split("/")
            if parts and parts[0].lower() == "users" and len(parts) >= 2:
                candidates.append(Path(f"/mnt/{drive}/Users/{parts[1]}/.codex/sessions"))
    candidates.append(Path("/mnt/c/Users/Deeproute/.codex/sessions"))
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except OSError:
            continue
    return None


def _extract_assistant_text_from_rollout(path: Path) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return ""
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        if payload.get("type") == "event_msg" and isinstance(payload.get("payload"), dict):
            inner = payload["payload"]
            if inner.get("type") == "agent_message" and isinstance(inner.get("message"), str) and inner["message"].strip():
                return inner["message"].strip()
            if inner.get("type") == "task_complete" and isinstance(inner.get("last_agent_message"), str) and inner["last_agent_message"].strip():
                return inner["last_agent_message"].strip()
        if payload.get("type") == "message" and payload.get("role") == "assistant":
            text = text_from_content_value(payload.get("content"))
            if text:
                return text
    return ""


def _extract_tool_text_from_rollout(path: Path) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return ""
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        payload_type = payload.get("type")
        if payload_type == "function_call_output" and isinstance(payload.get("output"), str):
            text = payload["output"].strip()
            if text:
                return text
        if payload_type != "custom_tool_call_output":
            continue
        parts: list[str] = []
        for item in payload.get("output", []) if isinstance(payload.get("output"), list) else []:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        text = "".join(parts).strip()
        if text:
            return text
    return ""


def _latest_rollout_files(payload: Json) -> list[Path]:
    root = _windows_codex_sessions_root_from_payload(payload)
    if root is None:
        return []
    now = datetime.now(timezone.utc)
    day_dir = root / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    search_roots = [day_dir] if day_dir.exists() else [root]
    files: list[Path] = []
    for search_root in search_roots:
        try:
            files.extend(search_root.glob("rollout-*.jsonl"))
        except OSError:
            continue
    files.sort(key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)
    return files[:8]


def latest_codex_tool_output_from_rollout(payload: Json) -> str:
    for path in _latest_rollout_files(payload):
        text = _extract_tool_text_from_rollout(path)
        if text:
            return text
    return ""


TOOL_EVIDENCE_LINE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bexit code:\s*-?\d+",
        r"\bran\s+\d+\s+tests?\b",
        r"\b(?:error|failed|failure|fatal|panic|exception|traceback)\b",
        r"\b(?:passed|ok|success|succeeded|validated|verified)\b",
        r"\b(?:commit|pushed|push|rebase|rebased|branch|origin/main|refs/heads/main)\b",
        r"\b(?:test|tests|unittest|pytest|cargo test|cargo check|bash -n|py_compile)\b",
        r"\b(?:warning|blocked|missing|skipped)\b",
    ]
]


def selected_tool_evidence_text(text: str, *, max_chars: int = 4096, max_lines: int = 80) -> str:
    """Keep memory-worthy tool evidence without storing huge stdout verbatim."""
    compact = str(text or "").strip()
    if not compact:
        return ""
    lines = [" ".join(line.split()) for line in compact.splitlines()]
    selected: list[str] = []
    for line in lines:
        if not line:
            continue
        if any(pattern.search(line) for pattern in TOOL_EVIDENCE_LINE_PATTERNS):
            selected.append(line[:360])
        if len(selected) >= max_lines:
            break
    if not selected:
        selected = [line[:360] for line in lines[: min(len(lines), 12)] if line]
    evidence = "\n".join(selected).strip()
    if len(evidence) > max_chars:
        evidence = evidence[:max_chars].rstrip() + "\n[tool evidence truncated]"
    return evidence



def latest_codex_assistant_message_from_rollout(payload: Json) -> str:
    for path in _latest_rollout_files(payload):
        text = _extract_assistant_text_from_rollout(path)
        if text:
            return text
    return ""


def payload_text(payload: Json) -> str:
    direct = first_string_at(
        payload,
        [
            ["prompt"],
            ["user_prompt"],
            ["input"],
            ["text"],
            ["message"],
            ["last_agent_message"],
            ["lastAssistantMessage"],
            ["assistant_message"],
            ["assistantMessage"],
            ["final_answer"],
            ["finalAnswer"],
            ["response"],
            ["output"],
            ["params", "prompt"],
            ["params", "input"],
            ["params", "text"],
            ["turn", "input"],
            ["raw_text"],
        ],
    )
    if direct:
        return direct
    for key in ["content", "output", "response"]:
        text = text_from_content_value(payload.get(key))
        if text:
            return text
    for key in ["messages", "items", "input"]:
        value = payload.get(key)
        if isinstance(value, list):
            parts = []
            for item in value:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text = text_from_content_value(item.get("content")) or first_string_at(item, [["text"], ["message"]])
                    if text:
                        parts.append(text)
            if parts:
                return "\n".join(parts)
    return json.dumps(payload, sort_keys=True)[:4000] if payload else ""


def payload_resource_uri(payload: Json) -> str:
    return first_string_at(
        payload,
        [
            ["raw_uri"],
            ["rawUri"],
            ["uri"],
            ["url"],
            ["path"],
            ["file_path"],
            ["filePath"],
            ["resource_path"],
            ["resourcePath"],
            ["document_path"],
            ["documentPath"],
            ["params", "raw_uri"],
            ["params", "uri"],
            ["params", "path"],
            ["metadata", "raw_uri"],
            ["metadata", "uri"],
            ["metadata", "path"],
        ],
    )


def payload_resource_type(payload: Json, raw_uri: str) -> str:
    direct = first_string_at(
        payload,
        [
            ["resource_type"],
            ["resourceType"],
            ["type"],
            ["mime_type"],
            ["mimeType"],
            ["params", "resource_type"],
            ["params", "resourceType"],
            ["metadata", "resource_type"],
            ["metadata", "resourceType"],
        ],
    )
    if direct:
        value = direct.strip().lower()
        if "/" in value:
            value = value.split("/")[-1]
        return value
    if Path(raw_uri).name.lower() == "skill.md":
        return "skill"
    return RESOURCE_TYPE_BY_SUFFIX.get(Path(raw_uri).suffix.lower(), "")


def compact_payload_item(item: Any, *, max_text: int = 1200) -> Json:
    if isinstance(item, str):
        return {"text": item[:max_text]}
    if not isinstance(item, dict):
        return {"value": str(item)[:max_text]}
    ref = first_string_at(item, [["ref"], ["path"], ["uri"], ["url"], ["name"], ["file"], ["relative_path"]])
    text = first_string_at(item, [["text"], ["content"], ["summary"], ["snippet"], ["selected_text"], ["output"]])
    kind = first_string_at(item, [["kind"], ["type"], ["mime_type"], ["language"]])
    compact: Json = {}
    if ref:
        compact["ref"] = ref
    if kind:
        compact["kind"] = kind
    if text:
        compact["text"] = text[:max_text]
    for key in ("line", "line_start", "line_end", "start", "end", "modified", "active", "focused"):
        if key in item:
            compact[key] = item[key]
    return compact or {"keys": sorted(str(key) for key in item.keys())[:20]}


def payload_list_items(payload: Json, keys: list[str], *, limit: int = 16) -> list[Json]:
    found: list[Json] = []
    containers = [payload]
    for nested_key in ("params", "turn", "metadata", "context"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            containers.append(nested)
    for container in containers:
        for key in keys:
            value = container.get(key)
            if isinstance(value, list):
                found.extend(compact_payload_item(item) for item in value[:limit])
            elif isinstance(value, dict):
                found.append(compact_payload_item(value))
            elif isinstance(value, str) and value.strip():
                found.append({"ref": key, "text": value[:1200]})
            if len(found) >= limit:
                return found[:limit]
    return found[:limit]


def local_context_from_payload(payload: Json) -> list[Json]:
    refs = payload_list_items(
        payload,
        [
            "local_context",
            "context",
            "open_files",
            "active_files",
            "files",
            "buffers",
            "selected_text",
            "selection",
            "tool_outputs",
            "terminal_output",
        ],
        limit=24,
    )
    return [ref for ref in refs if ref.get("text") or ref.get("ref")]


def agent_context_from_payload(payload: Json, *, event: str, session_id_source: str, args: argparse.Namespace) -> Json:
    workspace = first_string_at(
        payload,
        [
            ["workspace_root"],
            ["workspaceRoot"],
            ["project_root"],
            ["projectRoot"],
            ["cwd"],
            ["params", "cwd"],
            ["metadata", "cwd"],
        ],
    )
    current_url = first_string_at(payload, [["url"], ["current_url"], ["browser_url"], ["metadata", "url"]])
    tool_name = first_string_at(payload, [["tool_name"], ["toolName"], ["tool", "name"], ["params", "tool_name"]])
    tool_status = first_string_at(payload, [["tool_status"], ["status"], ["tool", "status"], ["params", "status"]])
    return {
        "agent": "codex",
        "event": event,
        "session_id_source": session_id_source,
        "workspace_root": workspace or str(args.repo_root),
        "current_url": current_url,
        "tool_name": tool_name,
        "tool_status": tool_status,
        "local_context": local_context_from_payload(payload),
        "files": payload_list_items(payload, ["files", "open_files", "active_files", "changed_files"], limit=24),
        "payload_keys": sorted(str(key) for key in payload.keys())[:80],
    }


def role_for_event(event: str) -> str:
    if event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        return "tool"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        return "assistant"
    return "user"


def hook_type_for_event(event: str) -> str:
    if event == "UserPromptSubmit":
        return "before_llm"
    if event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        return "tool_result"
    if event in {"IdleTimeout", "SessionIdle"}:
        return "session_commit"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        return "after_llm"
    return "before_llm"


def should_commit_session(event: str) -> bool:
    return event in {"Stop", "PostCompact", "SubagentStop", "IdleTimeout", "SessionIdle"}


def should_run_session_commit_after_ingest(event: str, hook_warning: str) -> bool:
    if hook_warning or not should_commit_session(event):
        return False
    return True


def commit_reason_for_event(event: str) -> str:
    if event in {"IdleTimeout", "SessionIdle"}:
        return "idle_timeout"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        return "hook_boundary"
    return "manual_api"


def call_tool(server: Any, name: str, arguments: Json) -> Json:
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000) % 1_000_000,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
    )
    if "error" in response:
        raise RuntimeError(response["error"]["message"])
    return json.loads(response["result"]["content"][0]["text"])


def build_server(args: argparse.Namespace):
    (
        MatrixArkLocalAdapter,
        MatrixArkMcpServer,
        MatrixArkTemporalStoreDirectAdapter,
        MatrixArkTemporalStoreRustAdapter,
        MatrixArkTemporalStoreRustDirectAdapter,
    ) = load_matrixark(args.repo_root)
    if args.backend == "local":
        validate_hook_backend_policy(args.backend)
        if args.event_log is None:
            raise RuntimeError("local hook backend requires --event-log and is only for explicit local tests")
        adapter = MatrixArkLocalAdapter(args.event_log)
    elif args.backend == "temporalstore-direct":
        adapter = MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib or None,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    elif args.backend == "temporalstore-rust":
        rust_proxy = args.rust_proxy or args.rust_cli
        if not rust_proxy:
            for candidate in [
                args.repo_root / "sdk" / "rust" / "temporalstore" / "target" / "release" / "matrixark_rust_proxy",
                args.repo_root / "target" / "release" / "matrixark_rust_proxy",
                args.repo_root / "target" / "debug" / "matrixark_rust_proxy",
                args.repo_root / "sdk" / "rust" / "temporalstore" / "target" / "debug" / "matrixark_rust_proxy",
            ]:
                if candidate.exists() and os.access(candidate, os.X_OK):
                    rust_proxy = str(candidate)
                    break
        adapter = MatrixArkTemporalStoreRustAdapter(
            rust_cli=args.rust_cli,
            rust_proxy=rust_proxy,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    elif args.backend == "temporalstore-rust-direct":
        rust_proxy = args.rust_direct_sdk or args.rust_proxy or args.rust_cli
        if not rust_proxy:
            for candidate in [
                args.repo_root / "sdk" / "rust" / "temporalstore" / "target" / "release" / "matrixark_rust_direct_sdk",
                args.repo_root / "target" / "release" / "matrixark_rust_direct_sdk",
                args.repo_root / "target" / "debug" / "matrixark_rust_direct_sdk",
                args.repo_root / "sdk" / "rust" / "temporalstore" / "target" / "debug" / "matrixark_rust_direct_sdk",
            ]:
                if candidate.exists() and os.access(candidate, os.X_OK):
                    rust_proxy = str(candidate)
                    break
        adapter = MatrixArkTemporalStoreRustDirectAdapter(
            rust_cli=args.rust_cli,
            rust_proxy=rust_proxy,
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    else:
        raise RuntimeError(
            "MatrixArk hooks no longer support local JSONL event logs; "
            "use temporalstore-direct, temporalstore-rust, or temporalstore-rust-direct."
        )
    return MatrixArkMcpServer(adapter)


def scope_from_args(args: argparse.Namespace) -> Json:
    return {
        "account_id": args.account_id,
        "tenant_id": args.tenant_id,
        "user_id": args.user_id,
        "session_id": args.session_id,
        "team": args.team,
        "project": args.project,
    }


def hook_storage_options() -> Json:
    return {"route": os.environ.get("MATRIXARK_HOOK_STORAGE_ROUTE", "shared_store_async")}


SYNTHETIC_HOOK_TEXT_MARKERS = (
    "matrixark synthetic",
    "synthetic probe",
    "codex-live-probe",
    "codex-cpp-live-probe",
    "manual validation",
    "hook verification",
    "reply ok only",
    "manual ingestion",
    "stdin check",
    "cmd stdin check",
    "service publisher",
    "hook fixed raw ingestion probe",
    "registered codex hook config verification",
    "matrixark legacy notify",
    "matrixark node launcher",
    "matrixark utf8 spooled hook",
    "matrixark wsl direct canonical",
    "matrixark app-server",
    "hook capture",
    "queryable row",
)


def is_synthetic_hook_text(text: str) -> bool:
    normalized = " ".join(str(text or "").lower().split())
    if normalized.startswith("user: "):
        normalized = normalized[6:].strip()
    if not normalized:
        return False
    if normalized.startswith(("probe ", "smoke ", "debug ", "test message ")):
        return True
    padded = f" {normalized} "
    if " smoke " in padded or " proof " in padded:
        return True
    if normalized.startswith("you are a helpful assistant. you will be presented with a user prompt, and your job is to provide a short title"):
        return True
    return any(marker in normalized for marker in SYNTHETIC_HOOK_TEXT_MARKERS)


def hook_retention_fields(*, text: str, role: str, now_ms: int) -> Json:
    synthetic = is_synthetic_hook_text(text)
    if not synthetic:
        return {
            "origin": "codex_hook",
            "record_class": f"{role or 'agent'}_message",
            "synthetic": False,
            "retention_class": "normal",
            "expires_at_ms": None,
            "gc_eligible": False,
        }
    return {
        "origin": "codex_hook",
        "record_class": "probe",
        "synthetic": True,
        "retention_class": "debug",
        "expires_at_ms": now_ms,
        "gc_eligible": True,
    }


def fast_hook_summary_dirty_records(
    *,
    node_path: list[str],
    scope: Json,
    event_id_hash: int,
    updated_at_ms: int,
) -> list[Json]:
    records: list[Json] = []
    for depth in range(1, len(node_path) + 1):
        prefix = node_path[:depth]
        prefix_hash = stable_int_hash("/".join(prefix))
        dirty_hash = stable_int_hash(f"summary_dirty:{prefix_hash}:new_event:event:{event_id_hash}:{updated_at_ms}")
        records.append(
            {
                "record_type": "context_summary_dirty",
                "dirty_hash": dirty_hash,
                "node_hash": prefix_hash,
                "node_path": prefix,
                "depth": depth,
                "dirty_reason": "new_event",
                "source_ref_type": "event",
                "source_event_hash": event_id_hash,
                "changed_ref_count": 1,
                "propagate_depth": len(node_path),
                "scope": scope,
                "status": "pending",
                "created_at_ms": updated_at_ms,
                "updated_at_ms": updated_at_ms,
            }
        )
    return records


def fast_async_hook_ingest(server: Any, *, args: argparse.Namespace, text: str, role: str, agent_context: Json, hook: Json | None) -> Json:
    adapter = getattr(server, "adapter", None)
    enqueue = getattr(adapter, "_enqueue_direct_write", None)
    if not callable(enqueue):
        return {}
    now = int(time.time() * 1000)
    scope = scope_from_args(args)
    tenant_id = str(scope.get("tenant_id") or args.tenant_id)
    user_id = str(scope.get("user_id") or args.user_id)
    session_id = str(scope.get("session_id") or args.session_id or "")
    node_path = [
        f"tenant:{tenant_id}",
        f"user:{user_id}",
        f"session:{session_id}",
        "conversation:codex_hook",
    ]
    node_hash = stable_int_hash("/".join(node_path))
    event_id_hash = stable_int_hash(f"{now}:{role}:{session_id}:{text}:{uuid.uuid4().hex}")
    storage_options = hook_storage_options()
    messages = [{"role": role, "content": text}]
    hook_type = hook_type_for_event(args.event)
    lineage = hook_lineage_fields(hook)
    metadata: Json = {
        "source": "codex_hook_fast_async",
        "codex_event": args.event,
        "hook_type": hook_type,
        "source_role": role,
        "agent_context": agent_context,
        **lineage,
    }
    retention = hook_retention_fields(text=text, role=role, now_ms=now)
    raw_record: Json = {
        "record_type": "agent_message",
        "source_kind": "message",
        "source_role": role,
        "hook_type": hook_type,
        "codex_event": args.event,
        "messages": messages,
        "scope": scope,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        **lineage,
        "metadata": metadata,
        "agent_hook": hook,
        "ingestion_time_ms": now,
        "storage_options": storage_options,
        "async_processing": True,
        "created_at_ms": now,
        "updated_at_ms": now,
        **retention,
    }
    record: Json = {
        "record_type": "context_event",
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "text": f"{role}: {text}",
        "summary_text": _compact_one_line(f"{role}: {text}", max_chars=220),
        "classification": "PENDING_ASYNC_EXTRACTION",
        "event_type": "pending_async",
        "status": "pending",
        "source_kind": "message",
        "source_role": role,
        "hook_type": hook_type,
        "codex_event": args.event,
        "scope": scope,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        **lineage,
        "metadata": metadata,
        "envelope": {
            "kind": "message",
            "source_role": role,
            "hook_type": hook_type,
            "codex_event": args.event,
            "messages": messages,
            "scope": scope,
            "metadata": metadata,
            "ingestion_time_ms": now,
            "storage_options": storage_options,
            **lineage,
        },
        "internal_extraction": {
            "mode": "async_pending",
            "classification": "PENDING_ASYNC_EXTRACTION",
            "event_type": "pending_async",
            "status": "pending",
            "source": "codex_hook_fast_async",
        },
        "agent_hook": hook,
        "storage_options": storage_options,
        "async_processing": True,
        "created_at_ms": now,
        "updated_at_ms": now,
        **retention,
    }
    pipeline_task: Json = {
        "record_type": "matrixark_async_pipeline_task",
        "task_hash": stable_int_hash(f"async_pipeline:{event_id_hash}"),
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        **lineage,
        "status": "pending",
        "stages": ["extraction", "summary", "compression", "embedding"],
        "reason": "codex_hook_fast_async_direct_queue",
        "agent_hook": hook,
        "storage_options": storage_options,
        "created_at_ms": now,
        "updated_at_ms": now,
    }
    summary_dirty_records = fast_hook_summary_dirty_records(
        node_path=node_path,
        scope=scope,
        event_id_hash=event_id_hash,
        updated_at_ms=now,
    )
    enqueue_raw = getattr(adapter, "enqueue_raw_ingestion_records", None)
    session_commit_result: Json = {}
    pre_ingest_idle_commit_result: Json = {}
    session_commit = getattr(adapter, "session_commit", None)
    threshold = int(getattr(args, "session_commit_threshold", 20) or 20)
    idle_timeout_ms = int(getattr(args, "idle_commit_timeout_ms", 0) or 0)
    pending_session_events = getattr(adapter, "pending_session_events", None)
    pending_before_ingest: list[Json] = []
    idle_elapsed_before_ingest_ms = 0
    if callable(pending_session_events):
        try:
            pending_before_ingest = list(pending_session_events(scope))
        except Exception:
            pending_before_ingest = []
    if pending_before_ingest and idle_timeout_ms > 0:
        latest_event_time = max(
            int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            for record in pending_before_ingest
        )
        if latest_event_time > 0:
            idle_elapsed_before_ingest_ms = max(0, now - latest_event_time)
    should_pre_ingest_idle_commit = (
        args.event == "UserPromptSubmit"
        and HOOK_AUTO_BATCH_EXTRACT
        and callable(session_commit)
        and bool(pending_before_ingest)
        and idle_timeout_ms > 0
        and idle_elapsed_before_ingest_ms >= idle_timeout_ms
    )
    if should_pre_ingest_idle_commit:
        pre_idle_hook = codex_agent_hook(
            hook_type="session_commit",
            hook_id=f"fast_async_pre_ingest_idle_commit:{args.event}:{now}",
            idempotency_key=f"fast-async-pre-ingest-idle:{session_id}:{event_id_hash}",
            trigger="idle_timeout_before_prompt",
            session_id_source=str((hook or {}).get("session_id_source") or ""),
            identity=hook,
        )
        pre_idle_args: Json = {
            "scope": scope,
            "threshold_messages": threshold,
            "force": False,
            "commit_reason": "idle_timeout",
            "idle_timeout_ms": idle_timeout_ms,
            "understanding_provider": getattr(args, "understanding_provider", None),
            "segment_provider": getattr(args, "segment_provider", None),
            "storage_options": storage_options,
            "agent_hook": pre_idle_hook,
        }
        try:
            pre_ingest_idle_commit_result = session_commit(pre_idle_args, hook=pre_idle_hook)
        except TypeError:
            pre_ingest_idle_commit_result = session_commit(pre_idle_args)
        except Exception as exc:
            pre_ingest_idle_commit_result = {
                "status": "error",
                "reason": "fast_async_pre_ingest_idle_commit_failed",
                "error": str(exc),
            }
        if isinstance(pre_ingest_idle_commit_result, dict):
            memory_layers_written = session_commit_memory_layers_written(pre_ingest_idle_commit_result)
            if memory_layers_written and "memory_layers_written" not in pre_ingest_idle_commit_result:
                pre_ingest_idle_commit_result = {
                    **pre_ingest_idle_commit_result,
                    "memory_layers_written": memory_layers_written,
                }

    if callable(enqueue_raw):
        enqueue_raw([raw_record])
    else:
        append_raw = getattr(adapter, "_append_raw_ingestion_records", None)
        if callable(append_raw):
            append_raw([raw_record])
    enqueue([record, pipeline_task, *summary_dirty_records])
    append_session_buffer = getattr(adapter, "append_session_buffer_event", None)
    if callable(append_session_buffer):
        append_session_buffer(
            envelope=record["envelope"],
            event_id_hash=event_id_hash,
            node_hash=node_hash,
            node_path=node_path,
            hook=hook,
        )
    pending_event_count = 0
    if callable(pending_session_events):
        try:
            pending_event_count = len(pending_session_events(scope))
        except Exception:
            pending_event_count = 0
    should_boundary_commit = should_commit_session(args.event)
    should_threshold_commit = (
        not should_boundary_commit
        and HOOK_AUTO_BATCH_EXTRACT
        and pending_event_count >= threshold
    )
    should_idle_commit = should_boundary_commit and commit_reason_for_event(args.event) == "idle_timeout"
    if callable(session_commit) and (should_threshold_commit or should_boundary_commit):
        commit_reason = commit_reason_for_event(args.event) if should_boundary_commit else "threshold"
        commit_args: Json = {
            "scope": scope,
            "threshold_messages": threshold,
            "force": should_boundary_commit and commit_reason != "idle_timeout",
            "commit_reason": commit_reason,
            "understanding_provider": getattr(args, "understanding_provider", None),
            "segment_provider": getattr(args, "segment_provider", None),
            "storage_options": storage_options,
            "agent_hook": {
                **codex_agent_hook(
                    hook_type="session_commit",
                    hook_id=f"fast_async_session_commit:{args.event}:{int(time.time() * 1000)}",
                    idempotency_key=f"fast-async-session-commit:{args.event}:{session_id}:{event_id_hash}",
                    trigger=args.event,
                    session_id_source=str((hook or {}).get("session_id_source") or ""),
                    identity=hook,
                ),
            },
        }
        if commit_reason == "idle_timeout":
            commit_args["idle_timeout_ms"] = idle_timeout_ms
        if should_threshold_commit:
            commit_args["max_messages"] = threshold
        try:
            session_commit_result = session_commit(commit_args, hook=commit_args["agent_hook"])
        except TypeError:
            session_commit_result = session_commit(commit_args)
        except Exception as exc:
            session_commit_result = {
                "status": "error",
                "reason": "fast_async_session_commit_failed",
                "error": str(exc),
            }
        if isinstance(session_commit_result, dict):
            memory_layers_written = session_commit_memory_layers_written(session_commit_result)
            if memory_layers_written and "memory_layers_written" not in session_commit_result:
                session_commit_result = {
                    **session_commit_result,
                    "memory_layers_written": memory_layers_written,
                }
    return {
        "status": "accepted",
        "sync_write_mode": "hook_fast_async_direct_queue",
        "raw_ingestion_status": "accepted" if callable(enqueue_raw) else "unavailable",
        "async_processing": True,
        "async_pipeline_status": "pending",
        "async_pipeline_task_hash": pipeline_task["task_hash"],
        "summary_dirty_count": len(summary_dirty_records),
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "session_buffer": {
            "registered": callable(append_session_buffer),
            "pending_event_count": pending_event_count,
            "pending_before_ingest_count": len(pending_before_ingest),
            "pending_after_ingest_count": pending_event_count,
            "threshold_messages": threshold,
            "threshold_ready": should_threshold_commit,
            "idle_commit_timeout_ms": idle_timeout_ms,
            "idle_ready": should_idle_commit,
            "pre_ingest_idle_ready": should_pre_ingest_idle_commit,
            "pre_ingest_idle_elapsed_ms": idle_elapsed_before_ingest_ms,
            "commit_after_current_ingest": bool(should_threshold_commit or should_boundary_commit),
            "auto_batch_extract": HOOK_AUTO_BATCH_EXTRACT,
            "boundary_commit_requested": should_boundary_commit,
        },
        "idle_commit_result": pre_ingest_idle_commit_result,
        "auto_batch_extract_result": session_commit_result if should_threshold_commit else {},
        "session_commit": session_commit_result if should_boundary_commit else {},
        "storage_options": storage_options,
        "hook_captured": hook is not None,
        "extraction_mode": "async_pending",
    }


def rollout_role_and_text(event: str, payload: Json) -> tuple[str, str, str, str]:
    if event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        text = selected_tool_evidence_text(latest_codex_tool_output_from_rollout(payload))
        return "tool", text, "PreviousToolOutputBackfill", "previous-tool-output"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        text = latest_codex_assistant_message_from_rollout(payload)
        return "assistant", text, "PreviousAssistantBackfill", "previous-assistant"
    return "", "", "", ""


def spawn_rollout_backfill_child(args: argparse.Namespace) -> None:
    if args.rollout_backfill_only:
        return
    if args.event not in {"PostToolUse", "PreToolUse", "PermissionRequest", "Stop", "PostCompact", "SubagentStop"}:
        return
    cmd = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], "--rollout-backfill-only"]
    try:
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=str(args.repo_root),
        )
    except OSError:
        pass


def run_rollout_backfill_only(args: argparse.Namespace, payload: Json, session_id_source: str) -> int:
    if args.rollout_backfill_delay_ms > 0:
        time.sleep(args.rollout_backfill_delay_ms / 1000.0)
    role, text, codex_event, idempotency_prefix = rollout_role_and_text(args.event, payload)
    if not role or not text:
        return 0
    codex_identity = codex_hook_lineage_from_payload(payload, args, session_id_source=session_id_source)
    agent_context = agent_context_from_payload(payload, event=args.event, session_id_source=session_id_source, args=args)
    server = build_server(args)
    try:
        common: Json = {"scope": scope_from_args(args)}
        if args.api_key:
            common["api_key"] = args.api_key
        call_tool(
            server,
            "matrixark_ingest",
            {
                **common,
                "messages": [{"role": role, "content": text}],
                "wait": False,
                "async_processing": True,
                "understanding_provider": args.understanding_provider,
                "segment_provider": args.segment_provider,
                "storage_options": hook_storage_options(),
                "metadata": {
                    "source": "codex_hook_rollout_async_backfill",
                    "codex_event": codex_event,
                    "backfill_reason": "codex_rollout_is_readable_after_synchronous_hook_boundary",
                    "agent_context": agent_context,
                    "codex_session_id_source": session_id_source,
                },
                "agent_hook": {
                    **codex_agent_hook(
                        hook_type="tool_result" if role == "tool" else "after_llm",
                        hook_id=f"Async{codex_event}:{stable_short_hash(text)}",
                        idempotency_key=f"{idempotency_prefix}:{stable_short_hash(text)}",
                        trigger=f"{args.event}:async_rollout_backfill",
                        session_id_source=session_id_source,
                        identity=codex_identity,
                    ),
                },
            },
        )
        if should_commit_session(args.event):
            call_tool(
                server,
                "matrixark_session_commit",
                {
                    **common,
                    "threshold_messages": 1,
                    "force": True,
                    "commit_reason": "async_rollout_backfill",
                    "understanding_provider": args.understanding_provider,
                    "segment_provider": args.segment_provider,
                    "storage_options": hook_storage_options(),
                    "agent_hook": {
                        **codex_agent_hook(
                            hook_type="session_commit",
                            hook_id=f"async_rollout_session_commit:{args.event}:{stable_short_hash(text)}",
                            idempotency_key=f"async-rollout-session-commit:{args.event}:{stable_short_hash(text)}",
                            trigger=f"{args.event}:async_rollout_backfill",
                            session_id_source=session_id_source,
                            identity=codex_identity,
                        ),
                    },
                },
            )
    finally:
        close_server_best_effort(server)
    return 0


def main() -> int:
    args = parse_args()
    validate_hook_backend_policy(args.backend)
    payload = read_stdin_payload()
    resolved_session_id, session_id_source = resolve_session_id(payload, args)
    args.session_id = resolved_session_id
    codex_identity = codex_hook_lineage_from_payload(payload, args, session_id_source=session_id_source)
    if args.rollout_backfill_only:
        return run_rollout_backfill_only(args, payload, session_id_source)
    text = payload_text(payload) or args.query
    if args.event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        fallback_text = text
        text = ""
        for _attempt in range(12):
            rollout_text = selected_tool_evidence_text(latest_codex_tool_output_from_rollout(payload))
            if rollout_text:
                text = rollout_text
                break
            time.sleep(0.2)
        if not text:
            text = fallback_text
    if args.event in {"Stop", "PostCompact", "SubagentStop"}:
        fallback_text = text
        text = ""
        for _attempt in range(12):
            rollout_text = latest_codex_assistant_message_from_rollout(payload)
            if rollout_text:
                text = rollout_text
                break
            time.sleep(0.2)
        if not text:
            text = fallback_text
    if is_codex_hook_heartbeat_text(text):
        trace = begin_hook_trace(args=args, payload=payload, text=text, session_id_source=session_id_source)
        output = {"status": "skipped", "reason": "codex_hook_heartbeat_not_user_context", "event": args.event}
        server = build_server(args)
        try:
            append_hook_trace(server, trace, output=output, status="skipped", skip_reason="codex_hook_heartbeat_not_user_context")
        finally:
            close_server_best_effort(server)
        print(json.dumps(strict_codex_stdout(output) if args.codex_strict_output else output, sort_keys=True))
        return 0
    raw_uri = payload_resource_uri(payload)
    resource_type = payload_resource_type(payload, raw_uri) if raw_uri else ""
    if not text and not raw_uri and args.event not in {"IdleTimeout", "SessionIdle"}:
        trace = begin_hook_trace(args=args, payload=payload, text=text, session_id_source=session_id_source)
        output = {"status": "skipped", "reason": "empty hook payload", "event": args.event}
        server = build_server(args)
        try:
            append_hook_trace(server, trace, output=output, status="skipped", skip_reason="empty_hook_payload")
        finally:
            close_server_best_effort(server)
        print(json.dumps(strict_codex_stdout(output) if args.codex_strict_output else output, sort_keys=True))
        return 0
    agent_context = agent_context_from_payload(payload, event=args.event, session_id_source=session_id_source, args=args)

    server = build_server(args)
    trace = begin_hook_trace(args=args, payload=payload, text=text, session_id_source=session_id_source, raw_uri=raw_uri)
    trace["workspace_root"] = agent_context.get("workspace_root", "")
    hook_warning = ""
    try:
        scope = scope_from_args(args)
        common: Json = {"scope": scope}
        if args.api_key:
            common["api_key"] = args.api_key

        ingest = {}
        if args.event == "UserPromptSubmit" and not HOOK_FAST_ASYNC_INGEST:
            previous_tool_output = selected_tool_evidence_text(latest_codex_tool_output_from_rollout(payload))
            if previous_tool_output and previous_tool_output != text and not hook_warning:
                backfill_result = trace_tool_call(
                    server,
                    "matrixark_ingest",
                    {
                        **common,
                        "messages": [{"role": "tool", "content": previous_tool_output}],
                        "wait": False,
                        "async_processing": True,
                        "understanding_provider": args.understanding_provider,
                        "segment_provider": args.segment_provider,
                        "storage_options": hook_storage_options(),
                        "metadata": {
                            "source": "codex_hook_rollout_backfill",
                            "codex_event": "PreviousToolOutputBackfill",
                            "backfill_reason": "post_tool_hook_payload_can_arrive_before_rollout_tool_output_is_visible",
                            "agent_context": agent_context,
                            "codex_session_id_source": session_id_source,
                        },
                        "agent_hook": {
                            "source": "codex",
                            "hook_type": "tool_result",
                            "hook_id": f"PreviousToolOutputBackfill:{stable_short_hash(previous_tool_output)}",
                            "observed_at_ms": int(time.time() * 1000),
                            "idempotency_key": f"previous-tool-output:{stable_short_hash(previous_tool_output)}",
                            "trigger": "UserPromptSubmit:previous_tool_output_backfill",
                            "auto_captured": True,
                            "session_id_source": session_id_source,
                        },
                    },
                    trace,
                )
                hook_warning = timeout_warning(backfill_result)
            previous_assistant = latest_codex_assistant_message_from_rollout(payload)
            if previous_assistant and previous_assistant != text and not hook_warning:
                backfill_result = trace_tool_call(
                    server,
                    "matrixark_ingest",
                    {
                        **common,
                        "messages": [{"role": "assistant", "content": previous_assistant}],
                        "wait": False,
                        "async_processing": True,
                        "understanding_provider": args.understanding_provider,
                        "segment_provider": args.segment_provider,
                        "storage_options": hook_storage_options(),
                        "metadata": {
                            "source": "codex_hook_rollout_backfill",
                            "codex_event": "PreviousAssistantBackfill",
                            "backfill_reason": "stop_hook_runs_before_rollout_final_answer_is_visible",
                            "agent_context": agent_context,
                            "codex_session_id_source": session_id_source,
                        },
                        "agent_hook": {
                            "source": "codex",
                            "hook_type": "after_llm",
                            "hook_id": f"PreviousAssistantBackfill:{stable_short_hash(previous_assistant)}",
                            "observed_at_ms": int(time.time() * 1000),
                            "idempotency_key": f"previous-assistant:{stable_short_hash(previous_assistant)}",
                            "trigger": "UserPromptSubmit:previous_assistant_backfill",
                            "auto_captured": True,
                            "session_id_source": session_id_source,
                        },
                    },
                    trace,
                )
                hook_warning = timeout_warning(backfill_result)
        if raw_uri and is_resource_event(args.event):
            kind = "skill" if resource_type == "skill" or Path(raw_uri).name.lower() == "skill.md" else "resource"
            ingest_args = {
                **common,
                "kind": kind,
                "messages": [{"role": "user", "content": text or f"{kind} added: {raw_uri}"}],
                "raw_uri": raw_uri,
                "resource_type": resource_type or kind,
                "metadata": {
                    "source": "codex_hook",
                    "codex_event": args.event,
                    "raw_hook_payload": payload,
                    "agent_context": agent_context,
                    "compacted_session_summary": False,
                    "codex_session_id_source": session_id_source,
                    "raw_uri": raw_uri,
                    "resource_type": resource_type or kind,
                },
                "understanding_provider": args.understanding_provider,
                "segment_provider": args.segment_provider,
                "storage_options": hook_storage_options(),
                "agent_hook": {
                    **codex_agent_hook(
                        hook_type="resource_added",
                        hook_id=f"{args.event}:{raw_uri}:{int(time.time() * 1000)}",
                        idempotency_key=hook_idempotency_key(payload, event=args.event, session_id=args.session_id, fallback=raw_uri),
                        trigger=args.event,
                        session_id_source=session_id_source,
                        identity=codex_identity,
                    ),
                },
                "wait": bool(payload.get("wait", False)),
            }
            if not hook_warning:
                ingest = trace_tool_call(server, "matrixark_ingest", ingest_args, trace)
                hook_warning = timeout_warning(ingest)
        elif text and not hook_warning:
            main_hook = codex_agent_hook(
                hook_type=hook_type_for_event(args.event),
                hook_id=f"{args.event}:{int(time.time() * 1000)}",
                idempotency_key=hook_idempotency_key(payload, event=args.event, session_id=args.session_id),
                trigger=args.event,
                session_id_source=session_id_source,
                identity=codex_identity,
            )
            if HOOK_FAST_ASYNC_INGEST:
                ingest = fast_async_hook_ingest(
                    server,
                    args=args,
                    text=text,
                    role=role_for_event(args.event),
                    agent_context=agent_context,
                    hook=main_hook,
                )
                if not ingest:
                    hook_warning = "fast async hook ingest was requested but the backend has no direct write queue"
            if ingest:
                hook_warning = timeout_warning(ingest)
            if not ingest and not hook_warning:
                ingest_args: Json = {
                    **common,
                    "messages": [{"role": role_for_event(args.event), "content": text}],
                    "wait": False,
                    "async_processing": True,
                    "understanding_provider": args.understanding_provider,
                    "segment_provider": args.segment_provider,
                    "storage_options": hook_storage_options(),
                    "metadata": {
                        "source": "codex_hook",
                        "codex_event": args.event,
                        "raw_hook_payload": payload,
                        "agent_context": agent_context,
                        "compacted_session_summary": args.event == "PostCompact",
                        "codex_session_id_source": session_id_source,
                    },
                    "agent_hook": main_hook,
                }
                if args.event == "UserPromptSubmit" and HOOK_AUTO_BATCH_EXTRACT:
                    ingest_args["auto_batch_extract"] = True
                    ingest_args["session_buffer_threshold"] = args.session_commit_threshold
                    if args.idle_commit_timeout_ms > 0:
                        ingest_args["idle_commit_timeout_ms"] = args.idle_commit_timeout_ms
                ingest = trace_tool_call(server, "matrixark_ingest", ingest_args, trace)
                hook_warning = timeout_warning(ingest)

        commit = {}
        if should_run_session_commit_after_ingest(args.event, hook_warning):
            commit_reason = commit_reason_for_event(args.event)
            commit = trace_tool_call(
                server,
                "matrixark_session_commit",
                {
                    **common,
                    "threshold_messages": args.session_commit_threshold,
                    "force": commit_reason != "idle_timeout",
                    "commit_reason": commit_reason,
                    "understanding_provider": args.understanding_provider,
                    "segment_provider": args.segment_provider,
                    "storage_options": hook_storage_options(),
                    **({"idle_timeout_ms": args.idle_commit_timeout_ms} if commit_reason == "idle_timeout" else {}),
                    "agent_hook": {
                        **codex_agent_hook(
                            hook_type="session_commit",
                            hook_id=f"session_commit:{args.event}:{int(time.time() * 1000)}",
                            idempotency_key=hook_idempotency_key(payload, event=f"session_commit:{args.event}", session_id=args.session_id),
                            trigger=args.event,
                            session_id_source=session_id_source,
                            identity=codex_identity,
                        ),
                    },
                },
                trace,
            )
            hook_warning = timeout_warning(commit)

        retrieve = {}
        query = args.query or text[:500]
        if (args.event == "UserPromptSubmit" or args.query) and not hook_warning and not HOOK_COMPACT_HOT_PREFIX_ONLY:
            retrieve = trace_tool_call(
                server,
                "matrixark_retrieve",
                {
                    **common,
                    "query": query,
                    "max_context_tokens": args.max_context_tokens,
                    "audit_mode": "telemetry_only",
                    "audit_sample_rate": 0.0,
                    "metadata": {
                        "retrieval_source": "codex_hook_retrieve",
                        "codex_event": args.event,
                        "hook_type": hook_type_for_event(args.event),
                        "codex_session_id_source": session_id_source,
                        "session_id_source": session_id_source,
                        "lifecycle_stage": "before_llm_retrieve" if args.event == "UserPromptSubmit" else "explicit_query_retrieve",
                    },
                    **({"local_context": agent_context.get("local_context", [])} if agent_context.get("local_context") else {}),
                },
                trace,
            )
            hook_warning = timeout_warning(retrieve)

        output = codex_hook_output(
            args=args,
            status="warning" if hook_warning else "ok",
            event=args.event,
            session_id_source=session_id_source,
            agent_context=agent_context,
            ingest=ingest,
            retrieve=retrieve,
            commit=commit,
            raw_uri=raw_uri,
            resource_type=resource_type,
            query=query,
            error=hook_warning,
        )
        append_hook_trace(server, trace, output=output, status="ok")
        if args.codex_strict_output:
            output = strict_codex_stdout(output)
        print(json.dumps(output, sort_keys=True))
        if not HOOK_COMPACT_HOT_PREFIX_ONLY:
            spawn_rollout_backfill_child(args)
    except Exception as exc:
        try:
            append_hook_trace(server, trace, status="error", error=f"{type(exc).__name__}: {exc}")
        except Exception:
            pass
        raise
    finally:
        close_server_best_effort(server)
    return 0


def event_from_argv(default: str = "UserPromptSubmit") -> str:
    for index, arg in enumerate(sys.argv):
        if arg == "--event" and index + 1 < len(sys.argv):
            return sys.argv[index + 1]
        if arg.startswith("--event="):
            return arg.split("=", 1)[1]
    return os.environ.get("CODEX_HOOK_EVENT", default)


def fail_open_enabled() -> bool:
    return os.environ.get("MATRIXARK_HOOK_FAIL_OPEN", "1").strip().lower() in {"1", "true", "yes", "on"}


def print_hook_failure(exc: BaseException) -> None:
    event = event_from_argv()
    error = f"{type(exc).__name__}: {exc}"
    output: Json = {
        "status": "warning",
        "event": event,
        "component": "matrixark_codex_hook",
        "reason": "hook_failed_fail_open",
        "error": error,
    }
    if event == "UserPromptSubmit":
        output["hookSpecificOutput"] = {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": (
                "MatrixArk/TemporalStore retrieval was attempted for this prompt but failed before "
                "remote context could be injected. Use visible local Codex context as authoritative "
                f"for this turn. Failure: {_compact_one_line(error, max_chars=700)}"
            ),
        }
    print(json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        if fail_open_enabled():
            print_hook_failure(exc)
            raise SystemExit(0)
        raise
