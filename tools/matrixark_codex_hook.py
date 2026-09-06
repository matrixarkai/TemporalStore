#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

import argparse
import hashlib
import json
try:
    from tools import matrixark_hook_pack_cache as _pack_cache
except ImportError:  # running from inside tools/, as the hooks do
    import matrixark_hook_pack_cache as _pack_cache
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
    from tools.matrixark_mcp_core import (
        _mcp_debug_log,
        canonical_account_id,
        canonical_tenant_id,
        candidate_index_terms,
        candidate_memory_layer_name,
        context_index_posting_record,
        embedding_for_text,
        embedding_model_name,
        EMBEDDING_LINEAGE_DEBUG_FIELDS,
        infer_query_type,
        local_account_user_id,
        memory_hierarchy_contract_from_recall_policy,
        messages_from_event_record,
        new_secondary_index_budget,
        normalize_message_role,
        pending_extraction_memory_layer_intent,
        profile_entity_type_for_memory_text,
        profile_memory_class_for_entity_type,
        profile_memory_kind_for_entity_type,
        serving_memory_layer_budget,
        serving_memory_layer_pressure,
        take_secondary_index_terms,
    )
    from tools.matrixark_mcp_context_pack import (
        serving_async_pipeline_readiness,
    )
except ModuleNotFoundError:
    from matrixark_mcp_core import (
        _mcp_debug_log,
        canonical_account_id,
        canonical_tenant_id,
        candidate_index_terms,
        candidate_memory_layer_name,
        context_index_posting_record,
        embedding_for_text,
        embedding_model_name,
        EMBEDDING_LINEAGE_DEBUG_FIELDS,
        infer_query_type,
        local_account_user_id,
        memory_hierarchy_contract_from_recall_policy,
        messages_from_event_record,
        new_secondary_index_budget,
        normalize_message_role,
        pending_extraction_memory_layer_intent,
        profile_entity_type_for_memory_text,
        profile_memory_class_for_entity_type,
        profile_memory_kind_for_entity_type,
        serving_memory_layer_budget,
        serving_memory_layer_pressure,
        take_secondary_index_terms,
    )
    from matrixark_mcp_context_pack import (
        serving_async_pipeline_readiness,
    )


Json = dict[str, Any]

CODEX_HOOK_CAPTURE_RAW_PAYLOAD = os.environ.get("MATRIXARK_CODEX_HOOK_CAPTURE_RAW_PAYLOAD", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def normalized_role_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return sorted({
        role
        for value in values
        for role in [normalize_message_role(value)]
        if role
    })


def normalized_role_counts(counts: Any, fallback_values: Any = None) -> Json:
    normalized: Json = {}
    counted = False
    if isinstance(counts, dict):
        for key, value in counts.items():
            role = normalize_message_role(key)
            if not role:
                continue
            try:
                amount = int(value or 0)
            except (TypeError, ValueError):
                amount = 0
            if amount > 0:
                normalized[role] = int(normalized.get(role, 0)) + amount
                counted = True
    if counted:
        return normalized
    for role in normalized_role_list(fallback_values):
        normalized[role] = int(normalized.get(role, 0)) + 1
    return normalized


def compact_context_embedding_record(record: Json) -> Json:
    compacted = dict(record)
    for field in EMBEDDING_LINEAGE_DEBUG_FIELDS:
        compacted.pop(field, None)
    return compacted


def attach_memory_layer(record: Json) -> Json:
    layer = candidate_memory_layer_name(record)
    if not layer or layer == "unknown":
        return record
    return {**record, "memory_layer": layer}


def merge_role_bucket(target: Json, role: str, bucket: Any) -> None:
    role = normalize_message_role(role)
    if not role:
        return
    if isinstance(bucket, dict):
        existing = target.setdefault(role, {})
        for field in ["refs", "tokens", "selected_refs", "selected_tokens", "dropped_refs", "dropped_tokens"]:
            try:
                amount = int(bucket.get(field) or 0)
            except (TypeError, ValueError):
                amount = 0
            if amount:
                existing[field] = int(existing.get(field, 0)) + amount
        for field, value in bucket.items():
            if field not in existing and field not in {"refs", "tokens", "selected_refs", "selected_tokens", "dropped_refs", "dropped_tokens"}:
                existing[field] = value
        return
    try:
        amount = int(bucket or 0)
    except (TypeError, ValueError):
        amount = 0
    if amount:
        target[role] = int(target.get(role, 0)) + amount


def normalize_role_bucket_map(bucket_map: Any) -> Json:
    if not isinstance(bucket_map, dict):
        return {}
    normalized: Json = {}
    for role, bucket in bucket_map.items():
        merge_role_bucket(normalized, role, bucket)
    return normalized


def normalize_memory_layer_budget_roles(memory_layer_budget: Json) -> Json:
    normalized = dict(memory_layer_budget)
    for bucket_name in ["by_source_role", "source_message_counts_by_role"]:
        role_bucket = normalize_role_bucket_map(normalized.get(bucket_name))
        if role_bucket:
            normalized[bucket_name] = role_bucket
    return normalized


def normalize_role_lineage_fields(record: Json) -> Json:
    normalized = dict(record)
    scalar_role = normalize_message_role(normalized.get("source_role"))
    roles = set(normalized_role_list(normalized.get("source_roles")))
    if scalar_role:
        roles.add(scalar_role)
    role_counts = normalized_role_counts(normalized.get("source_role_counts"), list(roles))
    if scalar_role and scalar_role not in role_counts:
        role_counts[scalar_role] = 1
    if roles:
        normalized["source_roles"] = sorted(roles)
    if role_counts:
        normalized["source_role_counts"] = role_counts
    if scalar_role:
        normalized["source_role"] = scalar_role
    promotions = normalized.get("profile_promotion_summary")
    if isinstance(promotions, list):
        normalized["profile_promotion_summary"] = [
            normalize_role_lineage_fields(item) if isinstance(item, dict) else item
            for item in promotions
        ]
    return normalized


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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


HOOK_TRACE_APPEND_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_TRACE_APPEND_TIMEOUT_MS", 750, minimum=0)
HOOK_CLOSE_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_CLOSE_TIMEOUT_MS", 750, minimum=0)
HOOK_TOOL_CALL_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_TOOL_CALL_TIMEOUT_MS", 8000, minimum=0)
HOOK_RETRIEVE_TIMEOUT_MS = _env_int("MATRIXARK_HOOK_RETRIEVE_TIMEOUT_MS", 5000, minimum=0)
DEFAULT_IDLE_COMMIT_TIMEOUT_MS = 120_000
HOOK_AUTO_BATCH_EXTRACT = _env_bool("MATRIXARK_HOOK_AUTO_BATCH_EXTRACT", True)
HOOK_FAST_ASYNC_INGEST = _env_bool("MATRIXARK_HOOK_FAST_ASYNC_INGEST", True)
HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH = _env_bool("MATRIXARK_HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH", False)
HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT = _env_int("MATRIXARK_HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT", 2, minimum=1)
HOOK_COMPACT_HOT_PREFIX_ONLY = os.environ.get("MATRIXARK_HOOK_COMPACT_HOT_PREFIX_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
HOOK_TOOL_RESULT_RAW = _env_bool("MATRIXARK_HOOK_TOOL_RESULT_RAW", False)
HOOK_TOOL_RESULT_SERVING = _env_bool("MATRIXARK_HOOK_TOOL_RESULT_SERVING", True)
HOOK_TOOL_RESULT_ROLLOUT_BACKFILL = _env_bool("MATRIXARK_HOOK_TOOL_RESULT_ROLLOUT_BACKFILL", False)
TOOL_HOOK_EVENTS = {"PostToolUse", "PreToolUse", "PermissionRequest"}
SESSION_COMMIT_SUCCESS_STATUSES = {"accepted", "committed", "finalized"}

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
    pack = _context_pack_view(pack)
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
    pack = _context_pack_view(pack)
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
    pack_view = _context_pack_view(pack)
    used_remote_tokens = used_context_tokens_from_retrieve(pack_view)
    remote_budget_tokens = _int_field(pack_view, "remote_context_budget_tokens")
    requested_max_context_tokens = _int_field(pack_view, "requested_max_context_tokens")
    used_local_tokens = _int_field(pack_view, "used_local_context_tokens")
    total_prompt_tokens = _int_field(pack_view, "total_prompt_context_tokens") or used_remote_tokens + used_local_tokens
    safety_margin_tokens = _int_field(pack_view, "local_context_safety_margin_tokens")
    budget: Json = {
        "used_remote_context_tokens": used_remote_tokens,
        "remote_context_budget_tokens": remote_budget_tokens,
        "requested_max_context_tokens": requested_max_context_tokens,
        "used_local_context_tokens": used_local_tokens,
        "total_prompt_context_tokens": total_prompt_tokens,
        "local_context_safety_margin_tokens": safety_margin_tokens,
        "budget_source": str(pack_view.get("budget_source") or ""),
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
    retrieval_metrics = pack_view.get("retrieval_metrics") if isinstance(pack_view.get("retrieval_metrics"), dict) else {}
    recall_policy = pack_view.get("recall_policy") if isinstance(pack_view.get("recall_policy"), dict) else {}
    dropped_memory_layer_budget = retrieval_metrics.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        dropped_memory_layer_budget = recall_policy.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        dropped_memory_layer_budget = pack_view.get("dropped_memory_layer_budget")
    if not isinstance(dropped_memory_layer_budget, dict):
        dropped_memory_layer_budget = {}
    if dropped_memory_layer_budget:
        dropped_memory_layer_budget = normalize_memory_layer_budget_roles(dropped_memory_layer_budget)
    memory_layer_pressure = retrieval_metrics.get("memory_layer_pressure")
    if not isinstance(memory_layer_pressure, dict):
        memory_layer_pressure = recall_policy.get("memory_layer_pressure")
    if not isinstance(memory_layer_pressure, dict):
        memory_layer_pressure = pack_view.get("memory_layer_pressure")
    if not isinstance(memory_layer_pressure, dict):
        memory_layer_pressure = {}
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
    memory_layer_pressure_active = False
    if memory_layer_pressure:
        memory_layer_pressure_active = any(
            [
                _int_field(memory_layer_pressure, "dropped_refs") > 0,
                _int_field(memory_layer_pressure, "dropped_tokens") > 0,
                _int_field(memory_layer_pressure, "dropped_bucket_count") > 0,
                bool(memory_layer_pressure.get("dropped_dimensions")),
                any(
                    bool(value)
                    for key, value in memory_layer_pressure.items()
                    if str(key).endswith("_pressure")
                ),
            ]
        )
    summary: Json = {
        "budget_pressure": bool(
            dropped_by_reason
            or dropped.get("deadline_exceeded")
            or dropped_memory_layer_budget
            or memory_layer_pressure_active
        ),
        "dropped_by_reason": dropped_by_reason,
        "estimated_tokens_by_reason": estimated_tokens,
        "deadline_exceeded": bool(dropped.get("deadline_exceeded")),
        "deadline_reason": dropped.get("deadline_reason"),
        "budget_fill_policy": dropped.get("budget_fill_policy"),
    }
    if dropped_memory_layer_budget:
        summary["dropped_memory_layer_budget"] = serving_memory_layer_budget(
            dropped_memory_layer_budget,
            include_debug=False,
        )
    if memory_layer_pressure:
        summary["memory_layer_pressure"] = serving_memory_layer_pressure(
            memory_layer_pressure,
            include_debug=False,
        )
    if dropped_by_reason:
        summary["budget_pressure_reason_count"] = sum(int(value) for value in dropped_by_reason.values())
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def serving_memory_selection_policy_budget(policy: Any) -> Json:
    if not isinstance(policy, dict):
        return {}
    compact: Json = {}
    if "enabled" in policy:
        compact["enabled"] = bool(policy.get("enabled"))
    for field in [
        "mode",
        "budget_semantics",
        "independent_caps",
        "global_remote_budget_enforced",
    ]:
        value = policy.get(field)
        if value not in (None, "", [], {}):
            compact[field] = value
    try:
        remote_budget = int(policy.get("remote_budget_tokens") or 0)
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget > 0:
        compact["remote_budget_tokens"] = remote_budget
    for field in [
        "budget_tokens",
        "selected_tokens_by_policy",
        "selected_ref_count_by_policy",
    ]:
        values = policy.get(field)
        if not isinstance(values, dict):
            continue
        normalized: Json = {}
        for key, value in values.items():
            label = str(key or "").strip()
            if not label:
                continue
            try:
                amount = int(value or 0)
            except (TypeError, ValueError):
                continue
            if amount > 0:
                normalized[label] = amount
        if normalized:
            compact[field] = normalized
    return {key: value for key, value in compact.items() if value not in (None, "", [], {})}


def retrieval_layer_summary_from_retrieve(
    pack: Json | None,
    refs: list[Json] | None = None,
    *,
    include_budget_lineage: bool = False,
) -> Json:
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
    if memory_layer_budget:
        memory_layer_budget = normalize_memory_layer_budget_roles(memory_layer_budget)
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
                "source_message_counts_by_role",
                "source_hook_counts_by_type",
                "source_codex_event_counts_by_event",
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
            ref_class = str(_ref_value(ref, "context_class", _ref_value(ref, "ref_type", _ref_value(ref, "type", "ref"))) or "ref")
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
        layer_summary["same_session_refs"] = sum(1 for ref in refs if _ref_value(ref, "session_continuity") == "same_session")
        layer_summary["cross_session_refs"] = sum(1 for ref in refs if _ref_value(ref, "session_continuity") == "cross_session")
        layer_summary["entity_bridge_refs"] = sum(
            1
            for ref in refs
            if _ref_value(ref, "session_continuity") == "cross_session"
            and str(_ref_value(ref, "ref_type", _ref_value(ref, "context_class", "")) or "") == "entity"
        )
    layer_summary["session_memory_refs"] = sum(1 for ref in refs if _ref_value(ref, "memory_scope") == "session")
    layer_summary["profile_memory_refs"] = sum(1 for ref in refs if _ref_value(ref, "memory_scope") == "user_profile")
    try:
        layer_summary["local_context_refs"] = int(local_policy.get("local_context_count") or 0)
    except (TypeError, ValueError):
        layer_summary["local_context_refs"] = 0
    if memory_layer_budget:
        # Agent summaries may opt into aggregate budget counters to prove
        # user/assistant/tool coverage without exposing raw refs/text lineage.
        layer_summary["memory_layer_budget"] = serving_memory_layer_budget(
            memory_layer_budget,
            include_debug=include_budget_lineage,
        )
    memory_selection_policy_budget = (
        recall_policy.get("memory_selection_policy_budget_policy")
        if isinstance(recall_policy.get("memory_selection_policy_budget_policy"), dict)
        else retrieval_metrics.get("memory_selection_policy_budget")
        if isinstance(retrieval_metrics.get("memory_selection_policy_budget"), dict)
        else pack_view.get("memory_selection_policy_budget")
        if isinstance(pack_view.get("memory_selection_policy_budget"), dict)
        else {}
    )
    compact_policy_budget = serving_memory_selection_policy_budget(memory_selection_policy_budget)
    if compact_policy_budget:
        layer_summary["memory_selection_policy_budget"] = compact_policy_budget
    memory_layer_pressure = retrieval_memory_layer_pressure_from_retrieve(pack)
    if memory_layer_pressure:
        layer_summary["memory_layer_pressure"] = serving_memory_layer_pressure(
            memory_layer_pressure,
            include_debug=False,
        )
    async_readiness = retrieval_async_readiness_from_retrieve(pack)
    if async_readiness:
        layer_summary["async_pipeline_readiness"] = serving_async_pipeline_readiness(
            async_readiness,
            include_debug=False,
        )
    pre_summary_refresh = retrieval_pre_summary_refresh_from_retrieve(pack)
    if pre_summary_refresh:
        layer_summary["pre_retrieval_summary_refresh"] = pre_summary_refresh
    return layer_summary


def retrieval_pre_summary_refresh_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    pack_view = _context_pack_view(pack)
    for source_name in ["retrieval_metrics", "recall_policy"]:
        source = pack_view.get(source_name)
        if isinstance(source, dict) and isinstance(source.get("pre_retrieval_summary_refresh"), dict):
            return {
                key: value
                for key, value in source["pre_retrieval_summary_refresh"].items()
                if value not in (None, "", [], {})
            }
    refresh = pack_view.get("pre_retrieval_summary_refresh")
    if isinstance(refresh, dict):
        return {key: value for key, value in refresh.items() if value not in (None, "", [], {})}
    return {}


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
            return serving_async_pipeline_readiness(
                normalize_async_readiness_roles(source["async_pipeline_readiness"]),
                include_debug=False,
            )
    readiness = pack_view.get("async_pipeline_readiness")
    if not isinstance(readiness, dict):
        return {}
    return serving_async_pipeline_readiness(
        normalize_async_readiness_roles(readiness),
        include_debug=False,
    )


def normalize_async_readiness_roles(readiness: Json) -> Json:
    normalized = dict(readiness)
    pending_roles = normalized_role_counts(normalized.get("pending_source_roles"))
    if pending_roles:
        normalized["pending_source_roles"] = pending_roles
    return normalized


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
        "source_message_counts_by_role": {},
        "source_hook_counts_by_type": {},
        "source_codex_event_counts_by_event": {},
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

    def add_source_counts(bucket_name: str, counts: Any, fallback_names: list[Any], *, normalize_roles: bool = False) -> None:
        if isinstance(counts, dict) and counts:
            for name, count in counts.items():
                label = normalize_message_role(name) if normalize_roles else str(name or "").strip()
                if not label:
                    continue
                try:
                    amount = max(0, int(count or 0))
                except (TypeError, ValueError):
                    amount = 0
                if amount:
                    budget[bucket_name][label] = int(budget[bucket_name].get(label, 0)) + amount
            return
        for name in fallback_names:
            label = normalize_message_role(name) if normalize_roles else str(name or "").strip()
            if label:
                budget[bucket_name][label] = int(budget[bucket_name].get(label, 0)) + 1

    def source_list(ref: Json, source_key: str, fallback: str) -> list[str]:
        values = _ref_list(ref, source_key)
        if values:
            labels = [str(value or "").strip() for value in values if str(value or "").strip()]
            if labels:
                return labels
        return [fallback] if fallback else []

    for ref in refs:
        text = _ref_text(ref).lstrip().lower()
        try:
            token_estimate = max(0, int(_ref_value(ref, "token_estimate", 0) or 0))
        except (TypeError, ValueError):
            token_estimate = 0
        if token_estimate == 0:
            token_estimate = max(1, (len(_ref_text(ref)) + 3) // 4)
        budget["total_selected_tokens"] += token_estimate
        ref_type = str(_ref_value(ref, "ref_type", _ref_value(ref, "context_class", "ref")) or "ref")
        add("by_ref_type", ref_type, token_estimate)
        memory_scope = str(_ref_value(ref, "memory_scope", "session") or "session")
        continuity = str(_ref_value(ref, "session_continuity", "same_session") or "same_session")
        extraction_phase = str(_ref_value(ref, "extraction_phase", "provisional") or "provisional")
        source_memory_scopes = source_list(ref, "source_memory_scopes", memory_scope)
        source_session_continuities = source_list(ref, "source_session_continuities", continuity)
        source_extraction_phases = source_list(ref, "source_extraction_phases", extraction_phase)
        for source_memory_scope in source_memory_scopes:
            add("by_memory_scope", source_memory_scope, token_estimate)
        for source_continuity in source_session_continuities:
            add("by_session_continuity", source_continuity, token_estimate)
        for source_phase in source_extraction_phases:
            add("by_extraction_phase", source_phase, token_estimate)
        if "final" in source_extraction_phases:
            budget["final_ref_count"] += 1
        if any(phase != "final" for phase in source_extraction_phases):
            budget["provisional_ref_count"] += 1
        entity_type = str(_ref_value(ref, "entity_type") or "")
        source_roles = set(normalized_role_list(_ref_list(ref, "source_roles")))
        scalar_role = normalize_message_role(_ref_value(ref, "source_role"))
        if scalar_role:
            source_roles.add(scalar_role)
        source_roles = sorted(source_roles)
        hook_types = _ref_list(ref, "source_hook_types")
        codex_events = _ref_list(ref, "source_codex_events")
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
        add_source_counts("source_message_counts_by_role", _ref_dict(ref, "source_role_counts"), source_roles, normalize_roles=True)
        add_source_counts("source_hook_counts_by_type", _ref_dict(ref, "source_hook_type_counts"), hook_types)
        add_source_counts("source_codex_event_counts_by_event", _ref_dict(ref, "source_codex_event_counts"), codex_events)
    return budget


def retrieval_memory_hierarchy_contract_from_retrieve(pack: Json | None) -> Json:
    if not isinstance(pack, dict):
        return {}
    pack_view = _context_pack_view(pack)
    recall_policy = pack_view.get("recall_policy") if isinstance(pack_view.get("recall_policy"), dict) else {}
    return memory_hierarchy_contract_from_recall_policy(recall_policy)


def retrieval_session_identity_from_retrieve(pack: Json | None, *, session_id_source: str = "") -> Json:
    source = str(session_id_source or "").strip()
    if isinstance(pack, dict):
        pack_view = _context_pack_view(pack)
        recall_policy = pack_view.get("recall_policy") if isinstance(pack_view.get("recall_policy"), dict) else {}
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
    commit = normalize_role_lineage_fields(commit)
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
        "profile_promotion_policy": commit.get("profile_promotion_policy"),
        "profile_promotion_blocker": commit.get("profile_promotion_blocker"),
    }
    return {
        key: value
        for key, value in memory_layers_written.items()
        if value not in (None, "", [], {})
    }


def session_commit_context_materialization(commit: Json | None) -> Json:
    if not isinstance(commit, dict) or not commit:
        return {}
    commit = normalize_role_lineage_fields(commit)
    source_event_count = _int_field(commit, "committed_event_count")
    context_event_count = _int_field(commit, "extraction_context_event_count")
    segment_count = _int_field(commit, "segments_written")
    session_entity_count = _int_field(commit, "entities_written")
    profile_entity_count = _int_field(commit, "profile_entities_written")
    profile_blocker = str(commit.get("profile_promotion_blocker") or "")
    return {
        "context_event": {
            "count": context_event_count,
            "role": "per-message serving event",
            "source_event_count": source_event_count,
            "scope": "session",
            "session_continuity": "same_session",
        },
        "context_segment": {
            "count": segment_count,
            "role": "derived grouping over context_event rows",
            "derived_from": "context_event",
            "not_a_context_event_alias": True,
        },
        "session_entity": {
            "count": session_entity_count,
            "scope": "session",
            "session_continuity": "same_session",
        },
        "profile_entity": {
            "count": profile_entity_count,
            "scope": "user_profile",
            "session_continuity": "cross_session",
            "policy": commit.get("profile_promotion_policy"),
            "blocker": profile_blocker,
            "scope_available": not bool(profile_blocker),
        },
    }


def session_commit_summary(commit: Json | None) -> Json:
    if not isinstance(commit, dict) or not commit:
        return {}
    commit = normalize_role_lineage_fields(commit)
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
        "prior_commit_count": commit.get("prior_commit_count"),
        "prior_committed_event_count": commit.get("prior_committed_event_count"),
        "boundary_hash": commit.get("boundary_hash"),
        "extraction_context_event_count": commit.get("extraction_context_event_count", 0),
        "source_roles": commit.get("source_roles"),
        "source_hook_types": commit.get("source_hook_types"),
        "source_codex_events": commit.get("source_codex_events"),
        "profile_promotion_summary": commit.get("profile_promotion_summary"),
        "profile_promotion_policy": commit.get("profile_promotion_policy"),
        "profile_promotion_blocker": commit.get("profile_promotion_blocker"),
        "entity_type_counts": commit.get("entity_type_counts"),
        "source_role_counts": commit.get("source_role_counts"),
        "source_hook_type_counts": commit.get("source_hook_type_counts"),
        "source_codex_event_counts": commit.get("source_codex_event_counts"),
        "segments_written": commit.get("segments_written", 0),
        "entities_written": entities_written,
        "session_entities_written": entities_written,
        "profile_entities_written": profile_entities_written,
        "memory_layers_written": session_commit_memory_layers_written(commit),
        "context_materialization": session_commit_context_materialization(commit),
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
                "already_finalized",
            ]
            if trigger_evidence.get(key) not in (None, "", [], {})
        }
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def memory_lineage_summary(*sources: Json | None) -> Json:
    source_role_counts: Json = {}
    source_hook_type_counts: Json = {}
    source_codex_event_counts: Json = {}
    profile_promotion_count = 0
    promoted_source_session_ids: set[str] = set()

    def add_counts(target: Json, counts: Any, values: Any = None, *, normalize_roles: bool = False) -> None:
        counted = False
        if isinstance(counts, dict):
            for key, value in counts.items():
                label = normalize_message_role(key) if normalize_roles else str(key)
                if not label:
                    continue
                try:
                    count = int(value or 0)
                except (TypeError, ValueError):
                    continue
                if count > 0:
                    target[label] = int(target.get(label, 0)) + count
                    counted = True
        if counted:
            return
        if isinstance(values, list):
            for value in values:
                if value in (None, "", [], {}):
                    continue
                key = normalize_message_role(value) if normalize_roles else str(value)
                if not key:
                    continue
                target[key] = int(target.get(key, 0)) + 1

    for source in sources:
        if not isinstance(source, dict) or not source:
            continue
        scalar_role = normalize_message_role(source.get("source_role"))
        role_counts = source.get("source_role_counts")
        role_values = source.get("source_roles")
        if not isinstance(role_counts, dict) or not role_counts:
            role_counts = source.get("budget_source_role_counts")
        if not isinstance(role_values, list) or not role_values:
            role_values = source.get("budget_source_roles")
        if scalar_role:
            role_values = list(role_values) if isinstance(role_values, list) else []
            if scalar_role not in {normalize_message_role(value) for value in role_values}:
                role_values.append(scalar_role)
            if not isinstance(role_counts, dict) or not role_counts:
                role_counts = {scalar_role: 1}
        entity_role = {
            "assistant_decision": "assistant",
            "assistant_response": "assistant",
            "tool_evidence": "tool",
            "user_requirement": "user",
            "user_preference": "user",
        }.get(str(source.get("entity_type") or "").strip().lower())
        if entity_role and not role_counts and not role_values:
            role_values = [entity_role]
        add_counts(source_role_counts, role_counts, role_values, normalize_roles=True)
        add_counts(source_hook_type_counts, source.get("source_hook_type_counts"), source.get("source_hook_types"))
        add_counts(source_codex_event_counts, source.get("source_codex_event_counts"), source.get("source_codex_events"))
        memory_layers = source.get("memory_layers_written") if isinstance(source.get("memory_layers_written"), dict) else {}
        if not role_counts and not role_values:
            add_counts(source_role_counts, None, memory_layers.get("source_roles"), normalize_roles=True)
        if not source.get("source_hook_type_counts") and not source.get("source_hook_types"):
            add_counts(source_hook_type_counts, None, memory_layers.get("source_hook_types"))
        if not source.get("source_codex_event_counts") and not source.get("source_codex_events"):
            add_counts(source_codex_event_counts, None, memory_layers.get("source_codex_events"))
        promotions = source.get("profile_promotion_summary")
        if isinstance(promotions, list):
            profile_promotion_count += len([item for item in promotions if isinstance(item, dict)])
            for item in promotions:
                if not isinstance(item, dict):
                    continue
                session_ids = item.get("source_session_ids")
                if isinstance(session_ids, list):
                    promoted_source_session_ids.update(str(value) for value in session_ids if value not in (None, "", [], {}))
                elif item.get("source_session_id") not in (None, "", [], {}):
                    promoted_source_session_ids.add(str(item.get("source_session_id")))

    if not source_role_counts and not source_hook_type_counts and not source_codex_event_counts and profile_promotion_count <= 0:
        return {}
    summary: Json = {
        "source_role_counts": source_role_counts,
        "source_hook_type_counts": source_hook_type_counts,
        "source_codex_event_counts": source_codex_event_counts,
        "user_prompt_captured": int(source_role_counts.get("user", 0)) > 0,
        "assistant_response_captured": int(source_role_counts.get("assistant", 0)) > 0,
        "tool_evidence_captured": int(source_role_counts.get("tool", 0)) > 0,
        "profile_promotion_count": profile_promotion_count,
        "promoted_source_session_ids": sorted(promoted_source_session_ids),
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
        "pending_message_count": session_buffer.get("pending_message_count"),
        "threshold_messages": session_buffer.get("threshold_messages"),
        "threshold_ready": session_buffer.get("threshold_ready"),
        "idle_timeout_ms": session_buffer.get("idle_commit_timeout_ms"),
        "idle_commit_deadline_ms": session_buffer.get("idle_commit_deadline_ms"),
        "idle_commit_cutoff_ms": session_buffer.get("idle_commit_cutoff_ms"),
        "idle_commit_scheduled": session_buffer.get("idle_commit_scheduled"),
        "idle_ready": session_buffer.get("idle_ready"),
        "pre_ingest_idle_ready": session_buffer.get("pre_ingest_idle_ready"),
        "pre_ingest_idle_elapsed_ms": session_buffer.get("pre_ingest_idle_elapsed_ms"),
        "pending_before_ingest_count": session_buffer.get("pending_before_ingest_count"),
        "pending_before_ingest_message_count": session_buffer.get("pending_before_ingest_message_count"),
        "pending_after_ingest_count": session_buffer.get("pending_after_ingest_count"),
        "pending_after_ingest_message_count": session_buffer.get("pending_after_ingest_message_count"),
        "commit_after_current_ingest": session_buffer.get("commit_after_current_ingest"),
        "auto_batch_extract": session_buffer.get("auto_batch_extract"),
        "boundary_commit_requested": session_buffer.get("boundary_commit_requested"),
    }

    def session_trigger_evidence(source: Json | None = None) -> Json:
        source = source or {}
        trigger_evidence = source.get("trigger_evidence") if isinstance(source.get("trigger_evidence"), dict) else {}
        if isinstance(trigger_evidence, dict) and trigger_evidence:
            raw = {
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
            }
        else:
            raw = {
                "pending_event_count": session_buffer.get("pending_event_count"),
                "pending_message_count": session_buffer.get("pending_message_count"),
                "threshold_messages": session_buffer.get("threshold_messages"),
                "threshold_ready": session_buffer.get("threshold_ready"),
                "idle_timeout_ms": session_buffer.get("idle_commit_timeout_ms"),
                "idle_elapsed_ms": session_buffer.get("idle_elapsed_ms"),
                "idle_ready": session_buffer.get("idle_ready"),
                "force": source.get("force"),
                "commit_reason": source.get("commit_reason") or source.get("reason"),
            }
        return {key: value for key, value in raw.items() if value not in (None, "", [], {})}

    def add_commit_evidence(source: Json) -> None:
        memory_layers = source.get("memory_layers_written")
        if not isinstance(memory_layers, dict) or not memory_layers:
            memory_layers = session_commit_memory_layers_written(source)
        if memory_layers:
            summary["memory_layers_written"] = memory_layers
        summary_refresh = source.get("summary_refresh")
        if isinstance(summary_refresh, dict) and summary_refresh:
            summary["summary_refresh"] = summary_refresh
        trigger_evidence = session_trigger_evidence(source)
        if trigger_evidence:
            summary["trigger_evidence"] = trigger_evidence
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
            "profile_promotion_policy",
            "profile_promotion_blocker",
        ]:
            value = source.get(field)
            if value not in (None, "", [], {}):
                summary[field] = value
    if auto_batch:
        auto_batch = normalize_role_lineage_fields(auto_batch)
        summary["auto_batch_extract_status"] = auto_batch.get("status")
        auto_batch_reason = auto_batch.get("reason") or auto_batch.get("commit_reason") or auto_batch.get("trigger_policy")
        if session_buffer.get("boundary_commit_requested"):
            summary["decision"] = "boundary_commit"
            auto_batch_reason = auto_batch_reason or "hook_boundary"
        elif auto_batch_reason == "idle_timeout":
            summary["decision"] = "idle_commit"
        else:
            summary["decision"] = "committed" if successful_session_commit_status(auto_batch.get("status")) else "attempted"
        summary["reason"] = auto_batch_reason
        summary["source_roles"] = auto_batch.get("source_roles")
        summary["source_hook_types"] = auto_batch.get("source_hook_types")
        summary["source_codex_events"] = auto_batch.get("source_codex_events")
        summary["profile_promotion_summary"] = auto_batch.get("profile_promotion_summary")
        add_commit_evidence(auto_batch)
    elif session_commit:
        session_commit = normalize_role_lineage_fields(session_commit)
        summary["auto_batch_extract_status"] = session_commit.get("status")
        summary["decision"] = "boundary_commit"
        summary["reason"] = session_commit.get("reason") or session_commit.get("commit_reason")
        summary["source_roles"] = session_commit.get("source_roles")
        summary["source_hook_types"] = session_commit.get("source_hook_types")
        summary["source_codex_events"] = session_commit.get("source_codex_events")
        summary["profile_promotion_summary"] = session_commit.get("profile_promotion_summary")
        add_commit_evidence(session_commit)
    elif idle_commit and successful_session_commit_status(idle_commit.get("status")):
        idle_commit = normalize_role_lineage_fields(idle_commit)
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
        trigger_evidence = session_trigger_evidence({"commit_reason": summary.get("reason")})
        if trigger_evidence:
            summary["trigger_evidence"] = trigger_evidence
    return {key: value for key, value in summary.items() if value not in (None, "", [], {})}


def fast_async_boundary_commit_from_ingest(ingest: Json | None) -> Json:
    if not isinstance(ingest, dict):
        return {}
    session_buffer = ingest.get("session_buffer") if isinstance(ingest.get("session_buffer"), dict) else {}
    session_commit = ingest.get("session_commit") if isinstance(ingest.get("session_commit"), dict) else {}
    if not session_commit:
        return {}
    if not session_buffer.get("boundary_commit_requested"):
        return {}
    return session_commit


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
    memory_selection_policy_budget = (
        layer_summary.get("memory_selection_policy_budget")
        if isinstance(layer_summary.get("memory_selection_policy_budget"), dict)
        else {}
    )
    selection_budget_bits = _format_memory_selection_policy_budget_bits(memory_selection_policy_budget)
    memory_layer_pressure = (
        layer_summary.get("memory_layer_pressure")
        if isinstance(layer_summary.get("memory_layer_pressure"), dict)
        else {}
    )
    pressure_bits = _format_memory_layer_pressure_bits(memory_layer_pressure)
    pre_refresh = (
        layer_summary.get("pre_retrieval_summary_refresh")
        if isinstance(layer_summary.get("pre_retrieval_summary_refresh"), dict)
        else {}
    )
    pre_refresh_bits = []
    if pre_refresh:
        pre_refresh_bits.append(f"enabled={str(bool(pre_refresh.get('enabled'))).lower()}")
        if pre_refresh.get("status"):
            pre_refresh_bits.append(f"status={pre_refresh.get('status')}")
        for label, field in [
            ("limit", "requested_limit"),
            ("refreshed", "refreshed_count"),
            ("compression", "compression_created_count"),
            ("skipped", "skipped_dirty_count"),
        ]:
            try:
                value = int(pre_refresh.get(field) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                pre_refresh_bits.append(f"{label}={value}")
        skipped_reasons = pre_refresh.get("skipped_dirty_reasons")
        if isinstance(skipped_reasons, dict) and skipped_reasons:
            reason_bits = []
            for reason, count in sorted(skipped_reasons.items()):
                name = str(reason or "").strip()
                if not name:
                    continue
                try:
                    amount = int(count or 0)
                except (TypeError, ValueError):
                    amount = 0
                if amount > 0:
                    reason_bits.append(f"{name}={amount}")
            if reason_bits:
                pre_refresh_bits.append("skipped_reasons[" + ",".join(reason_bits) + "]")
        try:
            elapsed_ms = float(pre_refresh.get("elapsed_ms") or 0.0)
        except (TypeError, ValueError):
            elapsed_ms = 0.0
        if elapsed_ms > 0:
            pre_refresh_bits.append(f"elapsed_ms={round(elapsed_ms, 3)}")
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
        memory_layer_readiness = readiness.get("memory_layer_readiness")
        if isinstance(memory_layer_readiness, dict):
            if "ready_for_retrieval" in memory_layer_readiness:
                readiness_bits.append(
                    "memory_layers_ready="
                    + str(bool(memory_layer_readiness.get("ready_for_retrieval"))).lower()
                )
            for label, field in [
                ("blocked_layers", "blocked_layers"),
                ("ready_layers", "ready_layers"),
            ]:
                values = memory_layer_readiness.get(field)
                if isinstance(values, list) and values:
                    readiness_bits.append(f"{label}[" + ",".join(str(item) for item in values[:8]) + "]")
            layers = memory_layer_readiness.get("layers")
            if isinstance(layers, dict) and layers:
                layer_bits = []
                for name in sorted(layers):
                    layer = layers.get(name)
                    if not isinstance(layer, dict):
                        continue
                    try:
                        pending_tasks = int(layer.get("pending_task_count") or 0)
                    except (TypeError, ValueError):
                        pending_tasks = 0
                    remaining_stages = layer.get("remaining_stages") if isinstance(layer.get("remaining_stages"), list) else []
                    if pending_tasks > 0 or remaining_stages:
                        suffix = f"{name}={pending_tasks}"
                        if remaining_stages:
                            suffix += ":" + ",".join(str(stage) for stage in remaining_stages[:3])
                        layer_bits.append(suffix)
                if layer_bits:
                    readiness_bits.append("layer_pending[" + ",".join(layer_bits[:8]) + "]")
        readiness_count_specs = [
            ("stage_counts", "remaining_stage_counts"),
            ("pending_scopes", "pending_memory_scopes"),
            ("pending_continuity", "pending_session_continuities"),
            ("pending_phases", "pending_extraction_phases"),
        ]
        for label, field in readiness_count_specs:
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
    if not count_bits and not continuity_bits and not budget_bits and not selection_budget_bits and not pressure_bits and not readiness_bits:
        return ""
    details = []
    if count_bits:
        details.append(", ".join(count_bits))
    if continuity_bits:
        details.append(", ".join(continuity_bits))
    if budget_bits:
        details.append("memory_layer_budget: " + "; ".join(budget_bits))
    if selection_budget_bits:
        details.append("memory_selection_policy_budget: " + "; ".join(selection_budget_bits))
    if pressure_bits:
        details.append("memory_layer_pressure: " + "; ".join(pressure_bits))
    if pre_refresh_bits:
        details.append("summary_refresh[" + "; ".join(pre_refresh_bits) + "]")
    if readiness_bits:
        details.append("async_pipeline[" + "; ".join(readiness_bits) + "]")
    return "Layer summary: " + "; ".join(details) + "."


def _format_memory_layer_budget_bits(memory_layer_budget: Json) -> list[str]:
    if not isinstance(memory_layer_budget, dict) or not memory_layer_budget:
        return []
    budget_bits = []
    bucket_specs = [
        ("scope", "by_memory_scope"),
        ("continuity", "by_session_continuity"),
        ("phase", "by_extraction_phase"),
        ("ref_type", "by_ref_type"),
        ("entity_type", "by_entity_type"),
    ]
    for label, bucket_name in bucket_specs:
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


def _format_memory_selection_policy_budget_bits(policy: Json) -> list[str]:
    if not isinstance(policy, dict) or not policy:
        return []
    bits = []
    if "enabled" in policy:
        bits.append("enabled=" + str(bool(policy.get("enabled"))).lower())
    if policy.get("mode"):
        bits.append(f"mode={policy.get('mode')}")
    try:
        remote_budget = int(policy.get("remote_budget_tokens") or 0)
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget > 0:
        bits.append(f"remote_budget={remote_budget}")
    for label, field in [
        ("caps", "budget_tokens"),
        ("selected_tokens", "selected_tokens_by_policy"),
        ("selected_refs", "selected_ref_count_by_policy"),
    ]:
        values = policy.get(field)
        if not isinstance(values, dict) or not values:
            continue
        value_bits = []
        for key in sorted(values):
            try:
                amount = int(values[key] or 0)
            except (TypeError, ValueError):
                continue
            if amount > 0:
                value_bits.append(f"{key}={amount}")
        if value_bits:
            bits.append(f"{label}[" + ",".join(value_bits) + "]")
    return bits


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
    flag_specs = [
        ("profile", ["profile_memory_pressure"]),
        ("session", ["session_memory_pressure"]),
        ("cross_session", ["cross_session_pressure"]),
        ("same_session", ["same_session_pressure"]),
        ("final", ["final_memory_pressure"]),
        ("provisional", ["provisional_memory_pressure"]),
    ]
    for label, fields in flag_specs:
        if any(bool(memory_layer_pressure.get(field)) for field in fields):
            flag_bits.append(label)
    if flag_bits:
        pressure_bits.append("flags[" + ",".join(flag_bits) + "]")
    for label, field in [
        ("pressure_dimensions", "pressure_dimensions"),
        ("dropped_dimensions", "dropped_dimensions"),
    ]:
        values = memory_layer_pressure.get(field)
        if isinstance(values, list) and values:
            dimensions = []
            for value in values:
                name = str(value)
                if name in {'by_source_role', 'by_hook_type', 'by_codex_event', 'source_message_counts_by_role', 'source_hook_counts_by_type', 'source_codex_event_counts_by_event'}:
                    continue
                dimensions.append(name)
            if dimensions:
                pressure_bits.append(f"{label}[" + ",".join(dimensions[:8]) + "]")
    return pressure_bits


def _format_count_map_bits(counts: Json, *, limit: int = 6) -> str:
    if not isinstance(counts, dict) or not counts:
        return ""
    bits = []
    for key in sorted(counts):
        try:
            count = int(counts[key] or 0)
        except (TypeError, ValueError):
            continue
        if count > 0:
            bits.append(f"{key}={count}")
    return ",".join(bits[:limit])


def _format_memory_lineage_summary(lineage: Json) -> str:
    if not isinstance(lineage, dict) or not lineage:
        return ""
    bits = []
    for label, field in [
        ("roles", "source_role_counts"),
        ("hooks", "source_hook_type_counts"),
        ("codex_events", "source_codex_event_counts"),
    ]:
        formatted = _format_count_map_bits(lineage.get(field))
        if formatted:
            bits.append(f"{label}[{formatted}]")
    flag_bits = []
    for label, field in [
        ("user_prompt", "user_prompt_captured"),
        ("assistant_response", "assistant_response_captured"),
        ("tool_evidence", "tool_evidence_captured"),
    ]:
        if bool(lineage.get(field)):
            flag_bits.append(label)
    if flag_bits:
        bits.append("captured[" + ",".join(flag_bits) + "]")
    try:
        promotion_count = int(lineage.get("profile_promotion_count") or 0)
    except (TypeError, ValueError):
        promotion_count = 0
    if promotion_count > 0:
        bits.append(f"profile_promotions={promotion_count}")
    session_ids = lineage.get("promoted_source_session_ids")
    if isinstance(session_ids, list) and session_ids:
        bits.append("promoted_sessions=" + ",".join(str(value) for value in session_ids[:4]))
    if not bits:
        return ""
    return "Retrieved memory lineage: " + "; ".join(bits) + "."


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
    return text.startswith("Codex hook heartbeat ") and "TemporalStore is live and accepting MatrixArk hook writes" in text


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
    extra = pack.get("extra")
    if isinstance(extra, dict):
        nested = extra.get("context_pack")
        if isinstance(nested, dict):
            return nested
    return pack


def context_pack_id_from_retrieve(pack: Json | None) -> str:
    if not isinstance(pack, dict):
        return ""
    pack_view = _context_pack_view(pack)
    return str(pack_view.get("context_pack_id") or pack_view.get("pack_id") or pack.get("context_pack_id") or pack.get("pack_id") or "")


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


def _ref_metadata(ref: Json) -> Json:
    metadata = ref.get("metadata") if isinstance(ref, dict) else {}
    return metadata if isinstance(metadata, dict) else {}


def _ref_value(ref: Json, key: str, default: object = "") -> object:
    value = ref.get(key) if isinstance(ref, dict) else None
    if value not in (None, "", [], {}):
        return value
    return _ref_metadata(ref).get(key, default)


def _ref_list(ref: Json, key: str) -> list[Any]:
    value = ref.get(key) if isinstance(ref, dict) else None
    if not isinstance(value, list):
        value = _ref_metadata(ref).get(key)
    return value if isinstance(value, list) else []


def _ref_dict(ref: Json, key: str) -> Json:
    value = ref.get(key) if isinstance(ref, dict) else None
    if not isinstance(value, dict):
        value = _ref_metadata(ref).get(key)
    return value if isinstance(value, dict) else {}


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
    lines = [
        "MatrixArk/TemporalStore retrieved context for Codex.",
        f"Query: {_compact_one_line(query, max_chars=360)}",
        (
            "Merge this remote memory with the visible local Codex context. "
            "Prefer current local files when they conflict with retrieved memory."
        ),
        (
            "Retrieval summary: "
            f"context_pack_id={context_pack_id_from_retrieve(pack)}, "
            f"selected_refs={len(refs)}, "
            f"used_context_tokens={used_context_tokens_from_retrieve(pack)}, "
            f"local_context_refs_seen={local_context_count}."
        ),
    ]
    formatted_layer_summary = _format_retrieval_layer_summary(layer_summary)
    try:
        has_profile_memory = int(layer_summary.get("profile_memory_refs") or 0) > 0
    except (TypeError, ValueError):
        has_profile_memory = False
    try:
        has_cross_session_memory = int(layer_summary.get("cross_session_refs") or 0) > 0
    except (TypeError, ValueError):
        has_cross_session_memory = False

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
    if isinstance(ingest.get("auto_batch_extract_result"), dict) and "auto_batch_extract_decision" not in ingest:
        auto_batch_extract_result = ingest.get("auto_batch_extract_result") or {}
        idle_commit_result = ingest.get("idle_commit_result") if isinstance(ingest.get("idle_commit_result"), dict) else {}
        ingest = {
            **ingest,
            "auto_batch_extract_status": auto_batch_extract_result.get("status"),
            "idle_commit_status": idle_commit_result.get("status"),
            "idle_commit": session_commit_summary(idle_commit_result),
            "auto_batch_extract": session_commit_summary(auto_batch_extract_result),
            "auto_batch_extract_decision": auto_batch_decision_summary(ingest),
        }
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
    lineage = memory_lineage_summary(auto_batch_extract or auto_batch_decision, idle_commit, commit)
    emitted_refs = [
        ref for ref in _selected_refs_from_retrieve(retrieve) if not _ref_is_codex_hook_heartbeat(ref)
    ]
    rendered_context = sanitized_rendered_context_from_retrieve(retrieve)
    retrieve_summary: Json = {
        "context_pack_id": context_pack_id_from_retrieve(retrieve),
        "selected_ref_count": len(emitted_refs),
        "used_context_tokens": used_context_tokens_from_retrieve(retrieve),
        "budget": retrieval_budget_summary_from_retrieve(retrieve),
        "budget_pressure": retrieval_budget_pressure_from_retrieve(retrieve),
        "layers": retrieval_layer_summary_from_retrieve(retrieve, emitted_refs),
        "session_identity": retrieval_session_identity_from_retrieve(retrieve, session_id_source=session_id_source),
        "rendered_context_chars": len(rendered_context),
        "additional_context_emitted": False,
    }
    pre_retrieval_summary_refresh = retrieval_pre_summary_refresh_from_retrieve(retrieve)
    if pre_retrieval_summary_refresh:
        retrieve_summary["pre_retrieval_summary_refresh"] = pre_retrieval_summary_refresh
    async_pipeline_readiness = retrieval_async_readiness_from_retrieve(retrieve)
    if async_pipeline_readiness:
        retrieve_summary["async_pipeline_readiness"] = async_pipeline_readiness
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
        "retrieve": retrieve_summary,
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
        # Remember a pack that loaded; serve the last one when this turn could not build any.
        # Codex runs a separate entry point from Claude, so a fallback added to the other hook
        # leaves this one silently unprotected -- which is exactly what was measured.
        try:
            _cache_path = _pack_cache.context_pack_cache_path(
                "codex", str(agent_context.get("workspace_root") or "")
            )
            if additional_context.strip():
                _pack_cache.remember_context_pack(_cache_path, additional_context)
            elif not error and not _pack_cache.store_answered(retrieve):
                _previous, _age_s = _pack_cache.recover_context_pack(
                    _cache_path, max_age_s=_pack_cache.pack_cache_max_age_s()
                )
                if _previous.strip():
                    additional_context = _pack_cache.label_previous_pack(_previous, _age_s)
                    output["retrieve"]["context_pack_source"] = "previous_pack"
                    output["retrieve"]["context_pack_age_s"] = round(_age_s, 1)
        except Exception:  # noqa: BLE001 - the cache must never cost a turn its real answer.
            pass
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
            ingest_result = {
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
            item["result"] = ingest_result
        elif name == "matrixark_retrieve":
            emitted_refs = [
                ref for ref in _selected_refs_from_retrieve(result) if not _ref_is_codex_hook_heartbeat(ref)
            ]
            retrieve_result = {
                "context_pack_id": context_pack_id_from_retrieve(result),
                "selected_ref_count": len(emitted_refs),
                "used_context_tokens": used_context_tokens_from_retrieve(result),
                "retrieval_budget": retrieval_budget_summary_from_retrieve(result),
                "retrieval_budget_pressure": retrieval_budget_pressure_from_retrieve(result),
                "retrieval_layers": retrieval_layer_summary_from_retrieve(result, emitted_refs),
                "session_identity": retrieval_session_identity_from_retrieve(
                    result,
                    session_id_source=str((args.get("metadata") if isinstance(args.get("metadata"), dict) else {}).get("session_id_source") or ""),
                ),
                "rendered_context_chars": len(sanitized_rendered_context_from_retrieve(result)),
            }
            pre_retrieval_summary_refresh = retrieval_pre_summary_refresh_from_retrieve(result)
            if pre_retrieval_summary_refresh:
                retrieve_result["pre_retrieval_summary_refresh"] = pre_retrieval_summary_refresh
            async_pipeline_readiness = retrieval_async_readiness_from_retrieve(result)
            if async_pipeline_readiness:
                retrieve_result["async_pipeline_readiness"] = async_pipeline_readiness
            item["result"] = retrieve_result
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
        memory_lineage = memory_lineage_summary(auto_batch_extract or auto_batch_decision, idle_commit, commit)
        output_summary = {
            "strict_additional_context_emitted": bool(hook_specific.get("additionalContext")),
            "additional_context_chars": len(str(hook_specific.get("additionalContext") or "")),
            "context_pack_id": retrieve.get("context_pack_id"),
            "selected_ref_count": retrieve.get("selected_ref_count"),
            "retrieval_budget": retrieve.get("budget"),
            "retrieval_budget_pressure": retrieve.get("budget_pressure"),
            "retrieval_layers": retrieve.get("layers"),
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
        if retrieve.get("pre_retrieval_summary_refresh"):
            output_summary["pre_retrieval_summary_refresh"] = retrieve.get("pre_retrieval_summary_refresh")
        if retrieve.get("async_pipeline_readiness"):
            output_summary["async_pipeline_readiness"] = retrieve.get("async_pipeline_readiness")
        trace["output_summary"] = output_summary
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
    """Whether a local backend is permitted, spelled the way the guard that enforces it spells it.

    `matrixark_mcp_backends` refuses a local backend under a production profile unless this is set,
    and the refusal says to set it to 1. That guard accepts {1, true, yes}. This function also
    accepted "on", so `MATRIXARK_ALLOW_LOCAL_BACKEND=on` permitted the backend here and was refused
    there: a permission that half-applies, which is worse than one that does not apply, because
    each half looks correct on its own.

    The wider set in `matrixark_mcp_env.TRUE_VALUES` is deliberately NOT used here. A permission
    should not gain accepting spellings by inheriting a general helper -- every spelling it gains
    is another way to turn a production guard off, and the safe direction for a disagreement about
    a guard is the narrower one.
    """
    return os.environ.get("MATRIXARK_ALLOW_LOCAL_BACKEND", "0").strip().lower() in {"1", "true", "yes"}


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
    """Nothing. Hook lineage was only ever emitted under the debug-lineage flag, which is gone.

    Kept as a function because its callers spread the result into a dict; returning an empty one
    keeps every call site unchanged.
    """
    return {}


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
    hook: Json = {
        "source": "codex",
        "hook_id": hook_id,
        "observed_at_ms": observed_at_ms if observed_at_ms is not None else int(time.time() * 1000),
        "idempotency_key": idempotency_key,
        "trigger": trigger,
        "auto_captured": True,
        "session_id_source": session_id_source,
        **hook_lineage_fields(identity),
    }
    if hook_type == "session_commit" or _env_bool("MATRIXARK_HOOK_INCLUDE_LEGACY_HOOK_TYPE", False):
        hook["hook_type"] = hook_type
    return hook


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
    parser.add_argument("--user-id", default=os.environ.get("MATRIXARK_USER_ID") or local_account_user_id())
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
    parser.add_argument("--idle-commit-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", str(DEFAULT_IDLE_COMMIT_TIMEOUT_MS))))
    parser.add_argument("--understanding-provider", default=os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", "rules"))
    parser.add_argument("--segment-provider", default=os.environ.get("MATRIXARK_SEGMENT_PROVIDER", "deterministic"))
    parser.add_argument("--extraction-provider", default=os.environ.get("MATRIXARK_EXTRACTION_PROVIDER", ""))
    parser.add_argument("--segment-model", default=os.environ.get("MATRIXARK_SEGMENT_MODEL", ""))
    parser.add_argument("--segment-model-path", default=os.environ.get("MATRIXARK_SEGMENT_MODEL_PATH", ""))
    parser.add_argument(
        "--segment-max-new-tokens",
        type=int,
        default=int(os.environ["MATRIXARK_SEGMENT_MAX_NEW_TOKENS"])
        if os.environ.get("MATRIXARK_SEGMENT_MAX_NEW_TOKENS")
        else None,
    )
    parser.add_argument("--segment-provider-fallback", default=os.environ.get("MATRIXARK_SEGMENT_PROVIDER_FALLBACK", ""))
    parser.add_argument("--skip-prior-context", action="store_true", default=os.environ.get("MATRIXARK_SKIP_PRIOR_CONTEXT", "").strip().lower() in {"1", "true", "yes", "on"})
    parser.add_argument("--repo-root", type=Path, default=root)
    parser.add_argument("--rollout-backfill-only", action="store_true", default=False)
    parser.add_argument("--idle-commit-worker-only", action="store_true", default=False)
    parser.add_argument("--idle-commit-cutoff-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_CUTOFF_MS", "0") or 0))
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
    candidates.append(Path.home() / ".codex" / "sessions")
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
    direct_files: list[Path] = []
    for path_value in [
        first_string_at(payload, [["transcript_path"], ["transcriptPath"], ["conversation_path"], ["conversationPath"], ["log_path"], ["logPath"]]),
    ]:
        if not path_value:
            continue
        candidate = Path(path_value)
        try:
            if candidate.exists() and candidate.is_file():
                direct_files.append(candidate)
        except OSError:
            continue
    root = _windows_codex_sessions_root_from_payload(payload)
    if root is None:
        return direct_files
    now = datetime.now(timezone.utc)
    day_dir = root / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
    search_roots = [day_dir] if day_dir.exists() else [root]
    files: list[Path] = list(direct_files)
    seen = {str(path.resolve()) for path in files}
    for search_root in search_roots:
        try:
            for candidate in search_root.glob("rollout-*.jsonl"):
                try:
                    key = str(candidate.resolve())
                except OSError:
                    key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                files.append(candidate)
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
        r"\b[0-9a-f]{7,40}\.\.[0-9a-f]{7,40}\s+(?:HEAD|[^\s]+)\s*->\s*main\b",
        r"\b(?:test|tests|unittest|pytest|cargo test|cargo check|bash -n|py_compile)\b",
        r"\b(?:warning|blocked|rejected|missing|skipped|non-fast-forward)\b",
        r"\b(?:benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|ops/s|req/s|writes/s|reads/s)\b",
        r"\b(?:hit[- ]?rate|read[- ]?hit|quality|recall|precision|locomo|longmemeval|memory[- ]?quality)\b",
    ]
]


def benchmark_quality_facts_from_text(text: str, *, max_facts: int = 6) -> list[str]:
    source = " ".join(str(text or "").split())
    if not source:
        return []
    facts: list[str] = []

    def add_fact(value: str) -> None:
        fact = " ".join(str(value or "").split()).strip(" ;,.")
        if fact and fact not in facts:
            facts.append(fact[:180])

    workload_match = re.search(
        r"\b(?:workload|benchmark)\s*[:=]\s*([a-z0-9_./ -]{3,80}?)(?=\s+(?:p50|p90|p95|p99|throughput|qps|ops/s|req/s|hit[- ]?rate|read[- ]?hit|recall|precision|quality)\b|$)",
        source,
        re.IGNORECASE,
    )
    if workload_match:
        add_fact("workload=" + workload_match.group(1).strip())
    for percentile in ["p50", "p90", "p95", "p99"]:
        match = re.search(
            rf"\b{percentile}\b(?:\s+latency)?\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?\s*(?:ms|s|us|µs)?)",
            source,
            re.IGNORECASE,
        )
        if match:
            add_fact(f"{percentile} latency={match.group(1)}")
    throughput_match = re.search(
        r"\b(?:throughput|qps|ops/s|req/s)\b\s*[:=]?\s*([0-9][0-9,]*(?:\.[0-9]+)?\s*(?:ops/s|qps|req/s|writes/s|reads/s)?)",
        source,
        re.IGNORECASE,
    )
    if throughput_match:
        add_fact("throughput=" + throughput_match.group(1).strip())
    hit_rate_match = re.search(
        r"\b(?:hit[- ]?rate|read[- ]?hit(?:[- ]?rate)?|recall|precision|quality)\b\s*[:=]?\s*([0-9]+(?:\.[0-9]+)?\s*%?)",
        source,
        re.IGNORECASE,
    )
    if hit_rate_match:
        add_fact("hit_rate=" + hit_rate_match.group(1).strip())
    benchmark_name_match = re.search(r"\b(locomo|longmemeval)\b", source, re.IGNORECASE)
    if benchmark_name_match:
        add_fact("benchmark=" + benchmark_name_match.group(1).lower())
    return facts[:max_facts]


def pushed_main_commit_from_text(text: str) -> str:
    compact = str(text or "")
    match = re.search(r"\bpushed commit\s+([0-9a-f]{7,40})\b", compact, re.IGNORECASE)
    if match:
        return match.group(1)
    # A `old..new` range names both ends, and the pushed commit is always the SECOND. Ask it
    # before the looser forms below: those capture the first hash they meet after the word
    # "push", which in a range is the commit that was ALREADY there. Git's own output does not
    # contain the word, so it reached the range pattern and read correctly, while a sentence
    # about the same push ("git push output was b223ca8c..4eafaf9c HEAD -> main") reported the
    # old head as the thing that landed.
    match = re.search(r"\b[0-9a-f]{7,40}\.\.([0-9a-f]{7,40})\s+(?:HEAD|[^\s]+)\s*->\s*(?:main|origin/main)\b", compact, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(
        r"\b(?:git\s+push|push|published|publish|pushed)\s+(?:accepted|succeeded|completed|done|sent|landed)?\b.{0,80}?\b(?:main|origin/main)\b.{0,40}?\b(?:at|as|commit)?\s*([0-9a-f]{7,40})\b",
        compact,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    match = re.search(
        r"\b(?:git\s+push|push|published|publish|pushed)\s+(?:accepted|succeeded|completed|done|sent|landed)?\b.{0,80}?\b([0-9a-f]{7,40})\b.{0,40}?\b(?:to\s+)?(?:main|origin/main)\b",
        compact,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    match = re.search(r"\b([0-9a-f]{7,40})\s+(?:refs/heads/main|origin/main)\b", compact, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"\b([0-9a-f]{7,40})\s+(?:HEAD|[^\s]+)\s*->\s*(?:main|origin/main)\b", compact, re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


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
        return ""
    evidence = "\n".join(selected).strip()
    if len(evidence) > max_chars:
        evidence = evidence[:max_chars].rstrip() + "\n[tool evidence truncated]"
    return evidence


def selected_tool_memory_text(text: str, payload: Json | None = None, *, max_chars: int = 1024) -> str:
    """Summarize tool output into a short memory record for live hook ingestion."""
    evidence = selected_tool_evidence_text(text, max_chars=2048, max_lines=20)
    if not evidence:
        return ""
    source = str(evidence)
    facts: list[str] = []
    if isinstance(payload, dict):
        tool_name = first_string_at(payload, [["tool_name"], ["toolName"], ["tool", "name"], ["params", "tool_name"]])
        tool_status = first_string_at(payload, [["tool_status"], ["status"], ["tool", "status"], ["params", "status"]])
        if tool_name:
            facts.append(f"tool_name={tool_name}")
        if tool_status:
            facts.append(f"tool_status={tool_status}")
    exit_match = re.search(r"\bexit code:\s*(-?\d+)", source, re.IGNORECASE)
    if exit_match:
        facts.append(f"Exit code: {exit_match.group(1)}")
    tests_match = re.search(r"\bran\s+(\d+)\s+tests?\b", source, re.IGNORECASE)
    if not tests_match:
        tests_match = re.search(
            r"\b(?:test\s+result:\s+ok\.\s*)?(\d+)\s+passed\b(?:[;,]\s*0\s+failed\b)?",
            source,
            re.IGNORECASE,
        )
    if tests_match:
        if re.search(r"\b(?:passed|test\s+result:\s+ok|0\s+failed)\b", tests_match.group(0), re.IGNORECASE):
            facts.append(f"Validation: {tests_match.group(1)} tests passed")
        else:
            facts.append(f"Ran {tests_match.group(1)} tests")
    elif re.search(r"\b(?:test|tests|validation|py_compile|unittest|pytest|cargo test|cargo check)\b", source, re.IGNORECASE) and re.search(
        r"\b(?:passed|ok|succeeded|success|clean)\b",
        source,
        re.IGNORECASE,
    ):
        facts.append("Validation: tests passed")
    facts.extend(fact for fact in benchmark_quality_facts_from_text(source) if fact not in facts)
    pushed_commit = pushed_main_commit_from_text(source)
    if pushed_commit:
        target = " to origin/main" if re.search(r"\b(?:origin/main|refs/heads/main|HEAD\s*->\s*main)\b", source, re.IGNORECASE) else ""
        facts.append(f"pushed commit {pushed_commit}{target}")
    failure_match = None
    for notable_pattern in [
        r"^.*\b(?:blocked|rejected|failed|failure|fatal|panic|exception|traceback|error)\b.*$",
        r"^.*\b(?:missing|skipped|warning)\b.*$",
    ]:
        failure_match = re.search(notable_pattern, source, re.IGNORECASE | re.MULTILINE)
        if failure_match:
            break
    if failure_match:
        facts.append(f"notable={failure_match.group(0)[:180]}")
    if facts:
        return "; ".join(facts)[:max_chars].rstrip()
    return evidence[:max_chars].rstrip()


ASSISTANT_MEMORY_LINE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\b(?:decision|decided|recommendation|next|todo|follow[- ]?up)\b",
        r"\b(?:done|fixed|implemented|added|removed|changed|updated|validated|verified)\b",
        r"\b(?:commit|pushed|push|rebased|origin/main|refs/heads/main|branch)\b",
        r"\b(?:test|tests|passed|failed|blocked|missing|gap|risk|warning)\b",
        r"\b(?:benchmark|workload|latency|p50|p90|p95|p99|throughput|qps|hit[- ]?rate|quality|locomo|longmemeval)\b",
        r"\b(?:temporalstore|matrixark|codex|context|profile|cross[- ]session|memory)\b",
        r"\b(?:user|you)\b.{0,96}\b(?:prefer|prefers|preference|likes|wants|needs|asked|requires|required|always|never|avoid|remember)\b",
        r"\b(?:i(?:'ll| will)? remember|remembered|noted|got it)\b.{0,140}\b(?:prefer|preference|want|need|always|never|avoid|profile|memory)\b",
        r"\b(?:standing instruction|standing preference|user profile|long[- ]term memor(?:y|ies)|saved preference)\b",
        r"\b(?:call me|my name is|user(?:'s)? name is|user goes by|pronouns?|address (?:me|the user))\b",
        r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|timezone|time zone|locale)\b.{0,120}\b(?:concise|brief|detailed|bullets?|markdown|language|tone|style|format|timezone|locale)\b",
        r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b.{0,140}\b(?:always|prefer|use|keep|must|should|avoid|never|don't|push|build|deploy)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will|going forward|from now on)\b.{0,80}\b(?:use|keep|follow|prefer|avoid|never use|not use|always use|push|build|deploy)\b.{0,140}\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b",
        r"\b(?:mem0|feature parity|feature[- ]focused|functionalit(?:y|ies)|algorithms?|no testing|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no eviden[ct]e|live ingestion|memory ingestion|threshold|idle batch|batch extraction|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b",
    ]
]


ASSISTANT_PROFILE_MEMORY_POLICY_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\b(?:user|you)\b.{0,96}\b(?:prefer|prefers|preference|likes|wants|needs|asked|requires|required|always|never|avoid|remember)\b",
        r"\b(?:i(?:'ll| will)? remember|remembered|noted|got it)\b.{0,140}\b(?:prefer|preference|want|need|always|never|avoid|profile|memory)\b",
        r"\b(?:standing instruction|standing preference|user profile|long[- ]term memor(?:y|ies)|saved preference)\b",
        r"\b(?:call me|my name is|user(?:'s)? name is|user goes by|pronouns?|address (?:me|the user))\b",
        r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|timezone|time zone|locale)\b.{0,120}\b(?:concise|brief|detailed|bullets?|markdown|language|tone|style|format|timezone|locale)\b",
        r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b.{0,140}\b(?:always|prefer|use|keep|must|should|avoid|never|don't|push|build|deploy)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will|going forward|from now on)\b.{0,80}\b(?:use|keep|follow|prefer|avoid|never use|not use|always use|push|build|deploy)\b.{0,140}\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b",
        r"\b(?:mem0|feature parity|feature[- ]focused|functionalit(?:y|ies)|algorithms?|no testing|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no eviden[ct]e|live ingestion|memory ingestion|threshold|idle batch|batch extraction|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b",
    ]
]


FEATURE_SCOPE_EXCLUSION_RE = re.compile(
    r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\s+"
    r"(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b",
    re.IGNORECASE,
)
FEATURE_SCOPE_EXCLUDED_DIMENSION_RE = re.compile(
    r"\b(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b",
    re.IGNORECASE,
)

FEATURE_MEMORY_POLICY_RE = re.compile(
    r"\b(?:mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|memory feature|long[- ]term memory|session memory|profile memory|cross[- ]session memory|live ingestion|memory ingestion|threshold|idle batch|batch extraction|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b",
    re.IGNORECASE,
)


def feature_scope_memory_only_policy(text: str) -> bool:
    compact = " ".join(str(text or "").split())
    if not compact or not FEATURE_MEMORY_POLICY_RE.search(compact):
        return False
    if FEATURE_SCOPE_EXCLUSION_RE.search(compact):
        return True
    return bool(
        re.search(r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\b", compact, re.IGNORECASE)
        and FEATURE_SCOPE_EXCLUDED_DIMENSION_RE.search(compact)
    )


def selected_assistant_profile_fact_policy(text: str) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(
        compact
        and (
            feature_scope_memory_only_policy(compact)
            or any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_MEMORY_POLICY_PATTERNS)
        )
    )


def selected_user_profile_fact_policy(text: str) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(
        compact
        and (
            feature_scope_memory_only_policy(compact)
            or any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_MEMORY_POLICY_PATTERNS)
        )
    )


def fast_hook_profile_memory_fields(*, role: str, text: str, selection_policies: list[str]) -> Json:
    role_name = normalize_message_role(role)
    policy_set = {str(policy or "").strip() for policy in selection_policies if str(policy or "").strip()}
    profile_entity_type = profile_entity_type_for_memory_text(text)
    if profile_entity_type and (
        "selected_user_profile_fact" in policy_set
        or "selected_assistant_profile_fact" in policy_set
        or profile_entity_type == "memory_feature_profile"
    ):
        profile_class = profile_memory_class_for_entity_type(profile_entity_type)
        profile_kind = profile_memory_kind_for_entity_type(profile_entity_type)
        return {
            "profile_memory_class": profile_class,
            "profile_memory_kind": profile_kind,
            "source_profile_memory_classes": [profile_class],
            "source_profile_memory_kinds": [profile_kind],
        }
    if role_name in {"assistant", "tool"}:
        return {
            "profile_memory_class": "codex_outcome",
            "profile_memory_kind": "codex_outcome",
            "source_profile_memory_classes": ["codex_outcome"],
            "source_profile_memory_kinds": ["codex_outcome"],
        }
    return {}


def normalize_assistant_memory_line(line: str) -> str:
    stripped = str(line or "").strip()
    stripped = re.sub(r"^(?:#{1,6}\s+|>\s+)", "", stripped).strip()
    stripped = re.sub(r"^(?:[-*+]\s+|\d+[.)]\s+)", "", stripped).strip()
    stripped = stripped.strip("`").strip()
    return stripped


def assistant_memory_line_too_repetitive(line: str) -> bool:
    tokens = re.findall(r"\b[a-z0-9_/-]{3,}\b", str(line or "").lower())
    if len(tokens) < 24:
        return False
    unique_ratio = len(set(tokens)) / max(1, len(tokens))
    return unique_ratio < 0.35


def selected_assistant_outcome_facts(text: str, *, max_facts: int = 6) -> list[str]:
    """Extract durable assistant outcome facts without keeping full LLM output."""
    compact = " ".join(str(text or "").split())
    if not compact:
        return []
    if feature_scope_memory_only_policy(compact):
        return []
    facts: list[str] = []

    def add_fact(value: str) -> None:
        fact = " ".join(str(value or "").replace(";", ",").split()).strip(" ;,.")
        if not fact:
            return
        if fact not in facts:
            facts.append(fact[:220])

    pushed_commit = pushed_main_commit_from_text(compact)
    if pushed_commit:
        target = " to origin/main" if re.search(r"\b(?:origin/main|refs/heads/main|HEAD\s*->\s*main)\b", compact, re.IGNORECASE) else ""
        add_fact(f"Outcome: pushed commit {pushed_commit}{target}")

    has_test_failure = bool(
        re.search(r"\b[1-9]\d*\s+(?:failed|failures|errors)\b", compact, re.IGNORECASE)
        or re.search(r"\b(?:validation|tests?)\s+failed\b", compact, re.IGNORECASE)
    ) and not re.search(r"\b0\s+failed\b", compact, re.IGNORECASE)
    tests_match = re.search(r"\b(?:ran\s+)?(\d+)\s+tests?\s+(?:passed|ok|succeeded)\b", compact, re.IGNORECASE)
    if not tests_match:
        tests_match = re.search(r"\bran\s+(\d+)\s+tests?\b", compact, re.IGNORECASE)
    if not tests_match:
        tests_match = re.search(
            r"\b(?:test\s+result:\s+ok\.\s*)?(\d+)\s+passed\b(?:[;,]\s*0\s+failed\b)?",
            compact,
            re.IGNORECASE,
        )
    if tests_match and not has_test_failure:
        add_fact(f"Validation: {tests_match.group(1)} tests passed")
    elif not has_test_failure and re.search(r"\b(?:tests?|validation|py_compile|unittest|pytest|cargo test|cargo check)\b.{0,80}\b(?:passed|ok|succeeded|clean)\b", compact, re.IGNORECASE):
        add_fact("Validation: tests passed")

    has_real_blocker = bool(
        re.search(r"\b(?:blocked|blocker|failure|error|missing|rejected)\b", compact, re.IGNORECASE)
        or re.search(r"\b[1-9]\d*\s+(?:failed|failures|errors)\b", compact, re.IGNORECASE)
        or (
            re.search(r"\bfailed\b", compact, re.IGNORECASE)
            and not re.search(r"\b0\s+failed\b", compact, re.IGNORECASE)
        )
    )
    if has_real_blocker:
        blocker_match = re.search(
            r"[^.?!]*(?:blocked|blocker|failed|failure|error|missing|rejected)[^.?!]*[.?!]?",
            compact,
            re.IGNORECASE,
        )
        if blocker_match:
            add_fact("Blocker: " + blocker_match.group(0).strip(" .?!")[:180])

    benchmark_facts = benchmark_quality_facts_from_text(compact, max_facts=6)
    if benchmark_facts:
        add_fact("Benchmark: " + "; ".join(benchmark_facts))

    changed_match = re.search(r"\bchanged:?\s+([^.;]{8,180})", compact, re.IGNORECASE)
    if not changed_match:
        changed_match = re.search(r"\b(?:updated|implemented|added|removed|fixed)\s+([^.;]{8,180})", compact, re.IGNORECASE)
    if changed_match:
        add_fact("Changed: " + changed_match.group(1).strip())

    next_match = re.search(r"\bnext:\s*([^.;]{8,180})", compact, re.IGNORECASE)
    if next_match:
        add_fact("Next: " + next_match.group(1).strip())

    return facts[:max_facts]


def assistant_memory_line_covered_by_outcome_facts(line: str, outcome_facts: list[str]) -> bool:
    normalized_line = " ".join(str(line or "").lower().split())
    if not normalized_line or not outcome_facts:
        return False
    normalized_facts = [" ".join(str(fact or "").lower().split()) for fact in outcome_facts if str(fact or "").strip()]
    if not normalized_facts:
        return False
    has_outcome_fact = any(fact.startswith("outcome:") for fact in normalized_facts)

    line_commit_hashes = set(re.findall(r"\b[0-9a-f]{7,40}\b", normalized_line))
    if line_commit_hashes:
        for fact in normalized_facts:
            fact_commit_hashes = set(re.findall(r"\b[0-9a-f]{7,40}\b", fact))
            if line_commit_hashes & fact_commit_hashes:
                return True

    line_numbers = set(re.findall(r"\b\d+\b", normalized_line))
    if re.search(r"\b(?:test|tests|validation|py_compile|unittest|pytest|cargo)\b", normalized_line):
        for fact in normalized_facts:
            if fact.startswith("validation:"):
                if re.search(r"\b(?:commit|pushed|push|origin/main|refs/heads/main)\b", normalized_line) and not has_outcome_fact:
                    continue
                fact_numbers = set(re.findall(r"\b\d+\b", fact))
                if not line_numbers or line_numbers & fact_numbers:
                    return True

    line_tokens = {
        token
        for token in re.findall(r"\b[a-z0-9_/-]{4,}\b", normalized_line)
        if token not in {"this", "that", "with", "from", "into", "after", "before", "tests", "passed"}
    }
    for fact in normalized_facts:
        fact_tokens = {
            token
            for token in re.findall(r"\b[a-z0-9_/-]{4,}\b", fact)
            if token not in {"this", "that", "with", "from", "into", "after", "before", "tests", "passed"}
        }
        overlap = line_tokens & fact_tokens
        if fact.startswith("next:") and re.search(r"\bnext\b", normalized_line) and len(overlap) >= 2:
            return True
        if fact.startswith("blocker:") and re.search(r"\b(?:blocked|blocker|failed|failure|error|missing|rejected)\b", normalized_line) and len(overlap) >= 2:
            return True
        if fact.startswith("changed:") and re.search(r"\b(?:changed|updated|implemented|added|removed|fixed)\b", normalized_line) and len(overlap) >= 2:
            return True
        if fact.startswith("outcome:") and re.search(r"\b(?:commit|pushed|push|origin/main|refs/heads/main)\b", normalized_line) and len(overlap) >= 2:
            return True
    return False


def selected_assistant_memory_text(text: str, *, max_chars: int = 4096, max_lines: int = 48) -> str:
    """Keep decision/outcome lines from assistant responses without storing large answers verbatim."""
    compact = str(text or "").strip()
    if not compact:
        return ""
    feature_memory_only = feature_scope_memory_only_policy(compact)
    lines = [" ".join(line.split()) for line in compact.splitlines()]
    selected: list[str] = []
    in_code_block = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        normalized = normalize_assistant_memory_line(stripped)
        if assistant_memory_line_too_repetitive(normalized):
            continue
        patterns = ASSISTANT_PROFILE_MEMORY_POLICY_PATTERNS if feature_memory_only else ASSISTANT_MEMORY_LINE_PATTERNS
        if normalized and any(pattern.search(normalized) for pattern in patterns):
            selected.append(normalized[:420])
        if len(selected) >= max_lines:
            break
    if not selected:
        fallback_lines = lines[: min(len(lines), 8)]
        if feature_memory_only:
            fallback_lines = [line for line in lines if FEATURE_MEMORY_POLICY_RE.search(line)][: min(len(lines), 8)]
        selected = [normalize_assistant_memory_line(line)[:420] for line in fallback_lines if line]
    outcome_facts = selected_assistant_outcome_facts(compact)
    if outcome_facts:
        outcome_line = "; ".join(outcome_facts)
        selected = [outcome_line] + [
            line
            for line in selected
            if line not in outcome_facts
            and line != outcome_line
            and not assistant_memory_line_covered_by_outcome_facts(line, outcome_facts)
        ]
    evidence = "\n".join(selected).strip()
    if len(evidence) > max_chars:
        evidence = evidence[:max_chars].rstrip() + "\n[assistant memory truncated]"
    return evidence


USER_PROMPT_MEMORY_LINE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\bgoal\s*:",
        r"\b(?:please\s+)?implement\b",
        r"\b(?:fix|add|remove|replace|move|keep|make\s+sure|ensure)\b",
        r"\b(?:we\s+should|should|need\s+to|must|have\s+to|always|never|do\s+not|don't)\b",
        r"\b(?:ingest|extract|retrieve|profile|cross[- ]session|memory|context|summary|entity|index|budget)\b",
        r"\b(?:mem0|long[- ]term memory|session memory|profile memory|feature parity|features?|functionalit(?:y|ies)|algorithms?|feature[- ]focused|no testing|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no eviden[ct]e|threshold|idle batch|batch extraction)\b",
    ]
]


def selected_user_prompt_memory_text(text: str, *, max_chars: int = 4096, max_lines: int = 64) -> str:
    """Keep durable user intent lines from large prompts without storing full pasted context."""
    compact = str(text or "").strip()
    if not compact:
        return ""
    lines = [" ".join(line.split()).strip() for line in compact.splitlines()]
    selected: list[str] = []
    in_code_block = False
    for line in lines:
        if not line:
            continue
        if line.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        normalized = normalize_assistant_memory_line(line)
        if not normalized:
            continue
        if any(pattern.search(normalized) for pattern in USER_PROMPT_MEMORY_LINE_PATTERNS):
            selected.append(normalized[:420])
        if len(selected) >= max_lines:
            break
    if not selected:
        selected = [normalize_assistant_memory_line(line)[:420] for line in lines[: min(len(lines), 8)] if line]
    evidence = "\n".join(selected).strip()
    if len(evidence) > max_chars:
        evidence = evidence[:max_chars].rstrip() + "\n[user prompt truncated]"
    return evidence


def codex_memory_selection_metadata(*, role: str, event: str, text: str, original_text: str | None = None) -> Json:
    normalized_role = normalize_message_role(role)
    if normalized_role not in {"assistant", "tool", "user"}:
        return {}
    selected_text = str(text or "")
    original = selected_text if original_text is None else str(original_text or "")
    line_count = len([line for line in selected_text.splitlines() if line.strip()])
    original_line_count = len([line for line in original.splitlines() if line.strip()])
    original_text_chars = len(original)
    selected_text_chars = len(selected_text)
    dropped_text_chars = max(0, original_text_chars - selected_text_chars)
    dropped_line_count = max(0, original_line_count - line_count)
    selection_transformed = " ".join(selected_text.split()) != " ".join(original.split())
    if normalized_role == "tool":
        policies = ["selected_tool_evidence_only"]
        truncation_marker = "[tool evidence truncated]"
        max_chars = 4096
        max_lines = 80
    elif normalized_role == "assistant":
        policies = []
        assistant_source_text = selected_text or original
        feature_memory_only = feature_scope_memory_only_policy(assistant_source_text)
        if selected_assistant_profile_fact_policy(assistant_source_text):
            policies.append("selected_assistant_profile_fact")
        if not feature_memory_only and selected_assistant_outcome_facts(assistant_source_text):
            policies.append("selected_assistant_decision_outcome_only")
        if not policies:
            policies.append("selected_assistant_profile_fact" if feature_memory_only else "selected_assistant_decision_outcome_only")
        truncation_marker = "[assistant memory truncated]"
        max_chars = 4096
        max_lines = 48
    else:
        policies = ["selected_user_prompt"]
        if selected_user_profile_fact_policy(selected_text or original):
            policies.append("selected_user_profile_fact")
        truncation_marker = "[user prompt truncated]"
        max_chars = 4096
        max_lines = 64
    policy = policies[0] if policies else ""
    feature_memory_profile = bool(
        feature_scope_memory_only_policy(selected_text or original)
        or (
            normalized_role in {"assistant", "user"}
            and FEATURE_MEMORY_POLICY_RE.search(selected_text or original)
        )
    )
    metadata = {
        "policy": policy,
        "policies": policies,
        "policy_counts": {policy_name: 1 for policy_name in policies},
        "source_role": normalized_role,
        "codex_event": event,
        "selected_text_chars": selected_text_chars,
        "selected_line_count": line_count,
        "original_text_chars": original_text_chars,
        "original_line_count": original_line_count,
        "dropped_text_chars": dropped_text_chars,
        "dropped_line_count": dropped_line_count,
        "retained_text_ratio": min(1.0, round(selected_text_chars / original_text_chars, 6)) if original_text_chars else 1.0,
        "retained_line_ratio": min(1.0, round(line_count / original_line_count, 6)) if original_line_count else 1.0,
        "max_selected_chars": max_chars,
        "max_selected_lines": max_lines,
        "large_payload_verbatim_stored": False,
        "truncated": truncation_marker in selected_text,
        "selection_lossy": bool(
            selection_transformed
            or dropped_text_chars
            or dropped_line_count
            or truncation_marker in selected_text
        ),
        "selection_stage": "codex_hook_before_temporalstore_ingest",
    }
    if feature_memory_profile:
        metadata.update(
            {
                "profile_memory_class": "memory_feature",
                "profile_memory_kind": "memory_feature",
                "source_profile_memory_classes": ["memory_feature"],
                "source_profile_memory_kinds": ["memory_feature"],
            }
        )
    return metadata



def latest_codex_assistant_message_from_rollout_raw(payload: Json) -> str:
    for path in _latest_rollout_files(payload):
        text = _extract_assistant_text_from_rollout(path)
        if text:
            return text
    return ""


def latest_codex_assistant_message_from_rollout(payload: Json) -> str:
    text = latest_codex_assistant_message_from_rollout_raw(payload)
    if text:
        return selected_assistant_memory_text(text)
    return ""


IDENTITY_ONLY_PAYLOAD_KEYS = {
    "account_id",
    "api_key",
    "codex_session_id",
    "codex_thread_id",
    "conversation_id",
    "conversationId",
    "cwd",
    "event",
    "hookEventName",
    "hook_event_name",
    "id",
    "message_id",
    "metadata",
    "params",
    "request_id",
    "run",
    "session_id",
    "sessionId",
    "tenant_id",
    "thread_id",
    "threadId",
    "transcript_id",
    "transcriptId",
    "turn",
    "turn_id",
    "turnId",
    "user_id",
    "workspace_root",
    "workspaceRoot",
}


def identity_only_payload(value: Any) -> bool:
    if isinstance(value, dict):
        if not value:
            return True
        for key, item in value.items():
            if str(key) not in IDENTITY_ONLY_PAYLOAD_KEYS:
                return False
            if isinstance(item, (dict, list)) and not identity_only_payload(item):
                return False
        return True
    if isinstance(value, list):
        return all(identity_only_payload(item) for item in value)
    return True


PAYLOAD_MESSAGE_ROLE_ALIASES = {
    "agent": "assistant",
    "agent_message": "assistant",
    "assistant_response": "assistant",
    "assistant-output": "assistant",
    "assistant_output": "assistant",
    "assistant-message": "assistant",
    "assistant_message": "assistant",
    "final": "assistant",
    "final_answer": "assistant",
    "final_response": "assistant",
    "llm": "assistant",
    "llm_output": "assistant",
    "llm_response": "assistant",
    "model": "assistant",
    "model_output": "assistant",
    "model_response": "assistant",
    "function": "tool",
    "function_call_output": "tool",
    "custom_tool_call_output": "tool",
    "tool-output": "tool",
    "tool_output": "tool",
    "tool-result": "tool",
    "tool_result": "tool",
    "tool_call_output": "tool",
    "human": "user",
    "prompt": "user",
    "user_prompt": "user",
}


def normalized_payload_message_role(role: Any) -> str:
    value = str(role or "").strip().lower()
    return PAYLOAD_MESSAGE_ROLE_ALIASES.get(value, value)


def text_from_payload_messages(value: Any, *, preferred_roles: set[str] | None = None) -> str:
    if not isinstance(value, list):
        return ""
    preferred_roles = preferred_roles or set()
    all_parts: list[str] = []
    preferred_parts: list[str] = []
    for item in value:
        role = ""
        text = ""
        if isinstance(item, str):
            text = item
        elif isinstance(item, dict):
            role = normalized_payload_message_role(item.get("role") or item.get("type"))
            text = text_from_content_value(item.get("content")) or first_string_at(item, [["text"], ["message"], ["output"]])
        if not text:
            continue
        all_parts.append(text)
        if role in preferred_roles:
            preferred_parts.append(text)
    return "\n".join(preferred_parts or all_parts).strip()


def payload_text(payload: Json, *, event: str = "") -> str:
    normalized_event = str(event or payload.get("hook_event_name") or payload.get("hookEventName") or "").strip()

    def select_memory_text(value: str) -> str:
        if normalized_event == "UserPromptSubmit":
            return selected_user_prompt_memory_text(value)
        if normalized_event in {"Stop", "PostCompact", "SubagentStop"}:
            return selected_assistant_memory_text(value)
        if normalized_event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
            return selected_tool_memory_text(value, payload)
        return value

    assistant_paths = [
        ["last_agent_message"],
        ["last_assistant_message"],
        ["last-assistant-message"],
        ["lastAssistantMessage"],
        ["assistant_output"],
        ["assistant_outputs"],
        ["assistant-output"],
        ["assistant-outputs"],
        ["assistantOutput"],
        ["assistantOutputs"],
        ["assistant_message"],
        ["assistant_messages"],
        ["assistant-message"],
        ["assistant-messages"],
        ["assistantMessage"],
        ["assistantMessages"],
        ["llm_response"],
        ["llm_responses"],
        ["llm-response"],
        ["llm-responses"],
        ["llmResponse"],
        ["llmResponses"],
        ["llm_output"],
        ["llm_outputs"],
        ["llm-output"],
        ["llm-outputs"],
        ["llmOutput"],
        ["llmOutputs"],
        ["model_response"],
        ["model_responses"],
        ["model-response"],
        ["model-responses"],
        ["modelResponse"],
        ["modelResponses"],
        ["model_output"],
        ["model_outputs"],
        ["model-output"],
        ["model-outputs"],
        ["modelOutput"],
        ["modelOutputs"],
        ["final_answer"],
        ["final-answer"],
        ["finalAnswer"],
        ["final_response"],
        ["final_responses"],
        ["final-response"],
        ["final-responses"],
        ["finalResponse"],
        ["finalResponses"],
        ["response"],
        ["output"],
        ["params", "last_assistant_message"],
        ["params", "last-assistant-message"],
        ["params", "lastAssistantMessage"],
        ["params", "assistant_output"],
        ["params", "assistant_outputs"],
        ["params", "assistant-output"],
        ["params", "assistant-outputs"],
        ["params", "assistantOutput"],
        ["params", "assistantOutputs"],
        ["params", "assistant_message"],
        ["params", "assistant_messages"],
        ["params", "assistant-message"],
        ["params", "assistant-messages"],
        ["params", "assistantMessage"],
        ["params", "assistantMessages"],
        ["params", "llm_response"],
        ["params", "llm_responses"],
        ["params", "llm-response"],
        ["params", "llm-responses"],
        ["params", "llmResponse"],
        ["params", "llmResponses"],
        ["params", "llm_output"],
        ["params", "llm_outputs"],
        ["params", "llm-output"],
        ["params", "llm-outputs"],
        ["params", "llmOutput"],
        ["params", "llmOutputs"],
        ["params", "model_response"],
        ["params", "model_responses"],
        ["params", "model-response"],
        ["params", "model-responses"],
        ["params", "modelResponse"],
        ["params", "modelResponses"],
        ["params", "model_output"],
        ["params", "model_outputs"],
        ["params", "model-output"],
        ["params", "model-outputs"],
        ["params", "modelOutput"],
        ["params", "modelOutputs"],
        ["params", "final_answer"],
        ["params", "final-answer"],
        ["params", "finalAnswer"],
        ["params", "final_response"],
        ["params", "final_responses"],
        ["params", "final-response"],
        ["params", "final-responses"],
        ["params", "finalResponse"],
        ["params", "finalResponses"],
        ["turn", "last_assistant_message"],
        ["turn", "lastAssistantMessage"],
        ["turn", "assistant_output"],
        ["turn", "assistant_outputs"],
        ["turn", "assistantOutput"],
        ["turn", "assistantOutputs"],
        ["turn", "assistant_message"],
        ["turn", "assistant_messages"],
        ["turn", "assistantMessage"],
        ["turn", "assistantMessages"],
        ["turn", "llm_response"],
        ["turn", "llm_responses"],
        ["turn", "llmResponse"],
        ["turn", "llmResponses"],
        ["turn", "llm_output"],
        ["turn", "llm_outputs"],
        ["turn", "llmOutput"],
        ["turn", "llmOutputs"],
        ["turn", "model_response"],
        ["turn", "model_responses"],
        ["turn", "modelResponse"],
        ["turn", "modelResponses"],
        ["turn", "model_output"],
        ["turn", "model_outputs"],
        ["turn", "modelOutput"],
        ["turn", "modelOutputs"],
        ["turn", "final_answer"],
        ["turn", "finalAnswer"],
        ["turn", "final_response"],
        ["turn", "final_responses"],
        ["turn", "finalResponse"],
        ["turn", "finalResponses"],
    ]
    tool_paths = [
        ["terminal_output"],
        ["terminal-output"],
        ["terminalOutput"],
        ["tool_outputs"],
        ["tool-outputs"],
        ["toolOutputs"],
        ["tool_output"],
        ["tool-output"],
        ["toolOutput"],
        ["tool_result"],
        ["tool-result"],
        ["toolResult"],
        ["result"],
        ["stdout"],
        ["stderr"],
        ["output"],
        ["params", "tool_output"],
        ["params", "tool-output"],
        ["params", "toolOutput"],
        ["params", "terminal_output"],
        ["params", "terminal-output"],
        ["params", "terminalOutput"],
        ["params", "tool_outputs"],
        ["params", "tool-outputs"],
        ["params", "toolOutputs"],
        ["params", "tool_result"],
        ["params", "tool-result"],
        ["params", "toolResult"],
        ["params", "result"],
        ["params", "stdout"],
        ["params", "stderr"],
    ]
    prompt_paths = [
        ["prompt"],
        ["user_prompt"],
        ["input"],
        ["text"],
        ["message"],
        ["params", "prompt"],
        ["params", "input"],
        ["params", "text"],
        ["turn", "input"],
        ["raw_text"],
    ]
    if normalized_event in {"Stop", "PostCompact", "SubagentStop"}:
        direct = first_string_at(payload, assistant_paths + prompt_paths)
    elif normalized_event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        direct = first_string_at(payload, tool_paths + prompt_paths)
    else:
        direct = first_string_at(payload, prompt_paths + assistant_paths + tool_paths)
    if direct:
        return select_memory_text(direct)
    structured_keys = ["content", "output", "response"]
    if normalized_event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        structured_keys = [
            "terminal_output",
            "terminalOutput",
            "tool_outputs",
            "toolOutputs",
            "tool_output",
            "toolOutput",
            "tool_result",
            "toolResult",
            "result",
            "output",
            "stderr",
            "stdout",
            "content",
            "response",
        ]
    elif normalized_event in {"Stop", "PostCompact", "SubagentStop"}:
        structured_keys = [
            "last_agent_message",
            "lastAgentMessage",
            "last_assistant_message",
            "lastAssistantMessage",
            "assistant_output",
            "assistant_outputs",
            "assistantOutput",
            "assistantOutputs",
            "assistant_message",
            "assistant_messages",
            "assistantMessage",
            "assistantMessages",
            "llm_response",
            "llm_responses",
            "llmResponse",
            "llmResponses",
            "llm_output",
            "llm_outputs",
            "llmOutput",
            "llmOutputs",
            "model_response",
            "model_responses",
            "modelResponse",
            "modelResponses",
            "model_output",
            "model_outputs",
            "modelOutput",
            "modelOutputs",
            "final_answer",
            "finalAnswer",
            "final_response",
            "final_responses",
            "finalResponse",
            "finalResponses",
            "response",
            "output",
            "content",
        ]
    for key in structured_keys:
        text = text_from_content_value(payload.get(key))
        if text:
            return select_memory_text(text)
    preferred_roles: set[str] = set()
    if normalized_event in {"Stop", "PostCompact", "SubagentStop"}:
        preferred_roles = {"assistant"}
    elif normalized_event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        preferred_roles = {"tool"}
    elif normalized_event == "UserPromptSubmit":
        preferred_roles = {"user"}
    for key in ["messages", "items", "input"]:
        text = text_from_payload_messages(payload.get(key), preferred_roles=preferred_roles)
        if text:
            return select_memory_text(text)
    if payload and not identity_only_payload(payload):
        return select_memory_text(json.dumps(payload, sort_keys=True)[:4000])
    return ""


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
    context = {
        "agent": "codex",
        "event": event,
        "session_id_source": session_id_source,
        "workspace_root": workspace or str(args.repo_root),
        "current_url": current_url,
        "tool_name": tool_name,
        "tool_status": tool_status,
        "local_context": local_context_from_payload(payload),
        "files": payload_list_items(payload, ["files", "open_files", "active_files", "changed_files"], limit=24),
    }
    return {key: value for key, value in context.items() if value not in ("", None, [], {})}


def codex_hook_metadata(
    *,
    source: str,
    event: str,
    agent_context: Json,
    session_id_source: str,
    payload: Json | None = None,
    **extra: Any,
) -> Json:
    metadata: Json = {
        "source": source,
        "codex_event": event,
        "agent_context": agent_context,
        "codex_session_id_source": session_id_source,
        **extra,
    }
    if CODEX_HOOK_CAPTURE_RAW_PAYLOAD and payload is not None:
        metadata["raw_hook_payload"] = payload
    return {key: value for key, value in metadata.items() if value not in ("", None, [], {})}


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


def should_auto_batch_extract_on_ingest(event: str) -> bool:
    return HOOK_AUTO_BATCH_EXTRACT and not should_commit_session(event)


def apply_hook_auto_batch_ingest_options(
    ingest_args: Json,
    *,
    event: str,
    session_commit_threshold: int,
    idle_commit_timeout_ms: int,
) -> Json:
    auto_batch_extract = should_auto_batch_extract_on_ingest(event)
    ingest_args["auto_batch_extract"] = auto_batch_extract
    ingest_args["session_buffer_threshold"] = session_commit_threshold
    if auto_batch_extract and idle_commit_timeout_ms > 0:
        ingest_args["idle_commit_timeout_ms"] = idle_commit_timeout_ms
    else:
        ingest_args.pop("idle_commit_timeout_ms", None)
    return ingest_args


def hook_session_commit_extraction_options(args: argparse.Namespace) -> Json:
    return {
        "understanding_provider": getattr(args, "understanding_provider", None),
        "extraction_provider": getattr(args, "extraction_provider", None),
        "segment_provider": getattr(args, "segment_provider", None),
        "segment_model": getattr(args, "segment_model", None),
        "segment_model_path": getattr(args, "segment_model_path", None),
        "segment_max_new_tokens": getattr(args, "segment_max_new_tokens", None),
        "segment_provider_fallback": getattr(args, "segment_provider_fallback", None),
        "skip_prior_context": bool(getattr(args, "skip_prior_context", False)),
    }


def codex_retrieve_ranking_options() -> Json:
    return {
        "source_role_budget_mode": "auto",
        "memory_layer_budget_mode": "auto",
        "memory_selection_policy_budget_mode": "auto",
        "extraction_phase_budget_mode": "auto",
    }


def codex_retrieve_question_type(query: str) -> str:
    return str(infer_query_type(str(query or "")) or "fact")


def codex_feature_scope_excludes_audit(query: str) -> bool:
    return bool(
        re.search(
            r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\s+"
            r"(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b",
            str(query or "").lower(),
        )
    )


def codex_retrieve_audit_options(query: str) -> Json:
    if codex_feature_scope_excludes_audit(query):
        return {
            "audit_mode": "off",
            "audit_sample_rate": 0.0,
        }
    return {
        "audit_mode": "telemetry_only",
        "audit_sample_rate": 0.0,
    }


def codex_retrieve_cross_session_options(query: str = "") -> Json:
    question_type = codex_retrieve_question_type(query)
    feature_memory_query = feature_scope_memory_only_policy(query) or bool(FEATURE_MEMORY_POLICY_RE.search(str(query or "")))
    options: Json = {
        "enabled": True,
        "budget_ratio": 0.12,
        "max_budget_ratio": 0.20,
        "max_sessions": 4,
        "max_candidates": 24,
        "min_entity_bridge_refs": 1,
        "preferred_ref_types": ["entity", "summary", "compression", "event", "segment"],
    }
    if question_type == "profile_memory" or feature_memory_query:
        options.update(
            {
                "budget_ratio": 0.30,
                "max_budget_ratio": 0.35,
                "max_sessions": 8,
                "max_candidates": 48,
                "min_entity_bridge_refs": 3,
                "preferred_ref_types": ["entity", "summary", "compression", "segment", "event"],
            }
        )
    elif question_type in {"current_state", "latest", "multi_hop", "date"}:
        options.update(
            {
                "budget_ratio": 0.16,
                "max_sessions": 6,
                "max_candidates": 36,
            }
        )
    return options


def hook_async_message_ingest_args(
    common: Json,
    args: argparse.Namespace,
    *,
    event: str,
    role: str,
    text: str,
    metadata: Json,
    agent_hook: Json,
    original_text: str | None = None,
) -> Json:
    selection_metadata = codex_memory_selection_metadata(role=role, event=event, text=text, original_text=original_text)
    if selection_metadata:
        metadata = {**metadata, "codex_memory_selection": selection_metadata}
    message: Json = {"role": role, "content": text}
    if selection_metadata:
        message["metadata"] = {"codex_memory_selection": selection_metadata}
    ingest_args: Json = {
        **common,
        "messages": [message],
        "wait": False,
        "async_processing": True,
        **hook_session_commit_extraction_options(args),
        "storage_options": hook_storage_options(),
        "metadata": metadata,
        "agent_hook": agent_hook,
    }
    return apply_hook_auto_batch_ingest_options(
        ingest_args,
        event=event,
        session_commit_threshold=args.session_commit_threshold,
        idle_commit_timeout_ms=args.idle_commit_timeout_ms,
    )


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
    user_id = str(args.user_id or local_account_user_id())
    session_id = str(args.session_id or user_id or "")
    return {
        "account_id": canonical_account_id(str(args.account_id or "")),
        "tenant_id": canonical_tenant_id(str(args.tenant_id or "")),
        "user_id": user_id,
        "session_id": session_id,
        "team": args.team,
        "project": args.project,
    }


def hook_storage_options() -> Json:
    return {"route": os.environ.get("MATRIXARK_HOOK_STORAGE_ROUTE", "shared_store_async")}


def is_tool_hook_event(event: str) -> bool:
    normalized = str(event or "").strip()
    return normalized in TOOL_HOOK_EVENTS or normalized.lower() in {"tool_result", "tool-result", "toolresult"}


def should_ingest_tool_result(event: str) -> bool:
    return not is_tool_hook_event(event) or HOOK_TOOL_RESULT_RAW or HOOK_TOOL_RESULT_SERVING


def should_promote_tool_result_to_serving(event: str) -> bool:
    return not is_tool_hook_event(event) or HOOK_TOOL_RESULT_SERVING


def should_rollout_backfill_tool_result() -> bool:
    return HOOK_TOOL_RESULT_ROLLOUT_BACKFILL or HOOK_TOOL_RESULT_RAW or HOOK_TOOL_RESULT_SERVING


SYNTHETIC_HOOK_TEXT_MARKERS = (
    "matrixark synthetic",
    "synthetic probe",
    "codex-live-probe",
    "codex-native-live-probe",
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
    source_role: str = "",
    hook_type: str = "",
    codex_event: str = "",
    source_memory_selection_policies: list[str] | None = None,
    source_memory_selection_policy_counts: Json | None = None,
    source_lineage: Json | None = None,
    profile_memory_class: str = "",
    profile_memory_kind: str = "",
) -> list[Json]:
    records: list[Json] = []
    lineage = source_lineage if isinstance(source_lineage, dict) else {}
    source_roles = [source_role] if source_role else []
    source_hook_types = [hook_type] if hook_type else []
    source_codex_events = [codex_event] if codex_event else []
    selection_policies = [
        str(policy)
        for policy in (source_memory_selection_policies or [])
        if str(policy or "").strip()
    ]
    selection_policy_counts: Json = {}
    for policy, count in (source_memory_selection_policy_counts or {}).items():
        policy_name = str(policy or "").strip()
        if not policy_name:
            continue
        try:
            amount = int(count or 0)
        except (TypeError, ValueError):
            continue
        if amount > 0:
            selection_policy_counts[policy_name] = amount
    source_memory_scopes, source_session_continuities = pending_extraction_memory_layer_intent(scope)
    profile_memory_class = str(profile_memory_class or "").strip()
    profile_memory_kind = str(profile_memory_kind or "").strip()
    profile_fields: Json = {}
    if profile_memory_class:
        profile_fields["profile_memory_class"] = profile_memory_class
        profile_fields["source_profile_memory_classes"] = [profile_memory_class]
    if profile_memory_kind:
        profile_fields["profile_memory_kind"] = profile_memory_kind
        profile_fields["source_profile_memory_kinds"] = [profile_memory_kind]
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
                **lineage,
                "source_roles": source_roles,
                "source_role_counts": {source_role: 1} if source_role else {},
                "source_hook_types": source_hook_types,
                "source_hook_type_counts": {hook_type: 1} if hook_type else {},
                "source_codex_events": source_codex_events,
                "source_codex_event_counts": {codex_event: 1} if codex_event else {},
                "source_memory_scopes": source_memory_scopes,
                "source_session_continuities": source_session_continuities,
                "source_extraction_phases": ["pending_async"],
                "source_memory_selection_policies": selection_policies,
                "source_memory_selection_policy_counts": selection_policy_counts,
                **profile_fields,
                "changed_ref_count": 1,
                "propagate_depth": len(node_path),
                "scope": scope,
                "status": "pending",
                "created_at_ms": updated_at_ms,
                "updated_at_ms": updated_at_ms,
            }
        )
    return records


def fast_hook_event_projection_records(*, event_record: Json, updated_at_ms: int) -> list[Json]:
    text = str(event_record.get("text") or event_record.get("summary_text") or "")
    event_id_hash = int(event_record.get("event_id_hash") or 0)
    node_hash = int(event_record.get("node_hash") or 0)
    node_path = event_record.get("node_path", []) if isinstance(event_record.get("node_path"), list) else []
    scope = event_record.get("scope", {}) if isinstance(event_record.get("scope"), dict) else {}
    serving_lineage: Json = {
        "memory_scope": event_record.get("memory_scope") or "session",
        "session_continuity": event_record.get("session_continuity") or "same_session",
        "extraction_phase": event_record.get("extraction_phase") or "pending_async",
        "final_session_boundary": bool(event_record.get("final_session_boundary", False)),
    }
    semantic_event_type = str(event_record.get("event_type") or "pending_async").strip() or "pending_async"
    projection_lineage: Json = {
        **serving_lineage,
        "event_type": semantic_event_type,
        "classification": event_record.get("classification", ""),
        "extraction_status": event_record.get("extraction_status", ""),
        "extraction_mode": event_record.get("extraction_mode", ""),
        "source_roles": event_record.get("source_roles", []),
        "source_role_counts": event_record.get("source_role_counts", {}),
        "source_hook_types": event_record.get("source_hook_types", []),
        "source_hook_type_counts": event_record.get("source_hook_type_counts", {}),
        "source_codex_events": event_record.get("source_codex_events", []),
        "source_codex_event_counts": event_record.get("source_codex_event_counts", {}),
        "source_memory_scopes": event_record.get("source_memory_scopes", []),
        "source_session_continuities": event_record.get("source_session_continuities", []),
        "source_extraction_phases": event_record.get("source_extraction_phases", []),
        "source_memory_selection_policies": event_record.get("source_memory_selection_policies", []),
        "source_memory_selection_policy_counts": event_record.get("source_memory_selection_policy_counts", {}),
        "profile_memory_class": event_record.get("profile_memory_class", ""),
        "profile_memory_kind": event_record.get("profile_memory_kind", ""),
        "source_profile_memory_classes": event_record.get("source_profile_memory_classes", []),
        "source_profile_memory_kinds": event_record.get("source_profile_memory_kinds", []),
        "source_event_ids": [event_id_hash] if event_id_hash else [],
    }
    projection_lineage = {
        key: value
        for key, value in projection_lineage.items()
        if value not in (None, "", [], {})
    }
    memory_layer = candidate_memory_layer_name(event_record)
    if memory_layer and memory_layer != "unknown":
        projection_lineage["memory_layer"] = memory_layer
    vector = embedding_for_text(text)
    embedding_record = compact_context_embedding_record(
        attach_memory_layer(
            {
                "record_type": "context_embedding",
                "embedding_type": "event_text",
                "ref_type": "event",
                "ref_hash": event_id_hash,
                "node_hash": node_hash,
                "node_path": node_path,
                "dim": len(vector),
                "model": embedding_model_name(),
                "vector": vector,
                "scope": scope,
                "event_type": semantic_event_type,
                "classification": event_record.get("classification", ""),
                "extraction_status": event_record.get("extraction_status", ""),
                "extraction_mode": event_record.get("extraction_mode", ""),
                **projection_lineage,
                "projection_phase": "fast_hook_pending_async",
                "updated_at_ms": updated_at_ms,
            }
        )
    )
    if scope:
        embedding_record["access_scope"] = scope
    records: list[Json] = [embedding_record]
    secondary_index_budget = new_secondary_index_budget()
    index_terms = take_secondary_index_terms(
        sorted(candidate_index_terms(event_record, {}, {})),
        secondary_index_budget,
    )
    for index_name in index_terms:
        index_record = context_index_posting_record(
            index_name=index_name,
            data_model="context_event",
            ref_type="event",
            ref_hashes=[event_id_hash],
            node_hash=node_hash,
            scope=scope,
            updated_at_ms=updated_at_ms,
        )
        index_record["access_scope"] = scope
        index_record.update(projection_lineage)
        index_record["projection_phase"] = "fast_hook_pending_async"
        records.append(index_record)
    return records


def pending_session_message_count(records: list[Json]) -> int:
    return sum(len(messages_from_event_record(record)) for record in records)


def pending_session_buffer_lineage(records: list[Json], *, fallback_role: str, fallback_hook_type: str, fallback_codex_event: str) -> Json:
    source_roles: list[str] = []
    source_hook_types: list[str] = []
    source_codex_events: list[str] = []
    source_memory_selection_policies: list[str] = []
    assistant_text_parts: list[str] = []
    user_text_parts: list[str] = []
    source_role_counts: Json = {}
    source_hook_type_counts: Json = {}
    source_codex_event_counts: Json = {}
    source_memory_selection_policy_counts: Json = {}

    def add_count(bucket: Json, value: Any, *, normalize_role: bool = False, count: int = 1) -> None:
        label = normalize_message_role(value) if normalize_role else str(value or "").strip()
        if not label:
            return
        bucket[label] = int(bucket.get(label, 0) or 0) + max(1, int(count or 1))

    def add_values(values: Any, target: list[str], counts: Json, *, normalize_role: bool = False) -> None:
        if isinstance(values, list):
            for value in values:
                label = normalize_message_role(value) if normalize_role else str(value or "").strip()
                if label:
                    target.append(label)
                    add_count(counts, label, normalize_role=False)

    def add_explicit_counts(values: Any, target: list[str], counts: Json, *, normalize_role: bool = False) -> None:
        if not isinstance(values, dict):
            return
        for value, count in values.items():
            label = normalize_message_role(value) if normalize_role else str(value or "").strip()
            if not label:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                target.append(label)
                counts[label] = max(int(counts.get(label, 0) or 0), amount)

    for record in records:
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
        metadata = envelope.get("metadata", {}) if isinstance(envelope.get("metadata"), dict) else {}
        hook = record.get("agent_hook", {}) if isinstance(record.get("agent_hook"), dict) else {}
        for message in messages_from_event_record(record):
            role = normalize_message_role(message.get("role"))
            if role:
                source_roles.append(role)
                add_count(source_role_counts, role)
            if role == "assistant":
                assistant_text_parts.append(str(message.get("content") or ""))
            elif role == "user":
                user_text_parts.append(str(message.get("content") or ""))
        add_values(metadata.get("source_roles"), source_roles, source_role_counts, normalize_role=True)
        add_values(record.get("source_roles"), source_roles, source_role_counts, normalize_role=True)
        add_explicit_counts(metadata.get("source_role_counts"), source_roles, source_role_counts, normalize_role=True)
        add_explicit_counts(record.get("source_role_counts"), source_roles, source_role_counts, normalize_role=True)
        for value in [record.get("source_role"), envelope.get("source_role"), metadata.get("source_role")]:
            role = normalize_message_role(value)
            if role:
                source_roles.append(role)
                add_count(source_role_counts, role)
        add_values(metadata.get("source_hook_types"), source_hook_types, source_hook_type_counts)
        add_values(record.get("source_hook_types"), source_hook_types, source_hook_type_counts)
        add_explicit_counts(metadata.get("source_hook_type_counts"), source_hook_types, source_hook_type_counts)
        add_explicit_counts(record.get("source_hook_type_counts"), source_hook_types, source_hook_type_counts)
        for value in [record.get("hook_type"), envelope.get("hook_type"), metadata.get("hook_type"), hook.get("hook_type")]:
            hook_type = str(value or "").strip()
            if hook_type:
                source_hook_types.append(hook_type)
                add_count(source_hook_type_counts, hook_type)
        add_values(metadata.get("source_codex_events"), source_codex_events, source_codex_event_counts)
        add_values(record.get("source_codex_events"), source_codex_events, source_codex_event_counts)
        add_explicit_counts(metadata.get("source_codex_event_counts"), source_codex_events, source_codex_event_counts)
        add_explicit_counts(record.get("source_codex_event_counts"), source_codex_events, source_codex_event_counts)
        for value in [record.get("codex_event"), envelope.get("codex_event"), metadata.get("codex_event"), hook.get("codex_event"), hook.get("trigger")]:
            codex_event = str(value or "").strip()
            if codex_event:
                source_codex_events.append(codex_event)
                add_count(source_codex_event_counts, codex_event)
        add_values(
            metadata.get("source_memory_selection_policies"),
            source_memory_selection_policies,
            source_memory_selection_policy_counts,
        )
        add_values(
            record.get("source_memory_selection_policies"),
            source_memory_selection_policies,
            source_memory_selection_policy_counts,
        )
        add_explicit_counts(
            metadata.get("source_memory_selection_policy_counts"),
            source_memory_selection_policies,
            source_memory_selection_policy_counts,
        )
        add_explicit_counts(
            record.get("source_memory_selection_policy_counts"),
            source_memory_selection_policies,
            source_memory_selection_policy_counts,
        )
        selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
        selection_policy_values = []
        if isinstance(selection.get("policies"), list):
            selection_policy_values.extend(selection.get("policies", []))
        selection_policy = str(selection.get("policy") or "").strip()
        if selection_policy:
            selection_policy_values.append(selection_policy)
        seen_selection_policy_values: set[str] = set()
        for selection_policy_value in selection_policy_values:
            selection_policy_name = str(selection_policy_value or "").strip()
            if selection_policy_name and selection_policy_name not in seen_selection_policy_values:
                seen_selection_policy_values.add(selection_policy_name)
                source_memory_selection_policies.append(selection_policy_name)
                add_count(source_memory_selection_policy_counts, selection_policy_name)

    if not source_roles and fallback_role:
        source_roles.append(fallback_role)
        add_count(source_role_counts, fallback_role, normalize_role=True)
    if not source_hook_types and fallback_hook_type:
        source_hook_types.append(fallback_hook_type)
        add_count(source_hook_type_counts, fallback_hook_type)
    if not source_codex_events and fallback_codex_event:
        source_codex_events.append(fallback_codex_event)
        add_count(source_codex_event_counts, fallback_codex_event)

    assistant_lineage_text = "\n".join(assistant_text_parts)
    assistant_policies: list[str] = []
    assistant_feature_memory_only = feature_scope_memory_only_policy(assistant_lineage_text)
    if assistant_lineage_text and selected_assistant_profile_fact_policy(assistant_lineage_text):
        assistant_policies.append("selected_assistant_profile_fact")
    if (assistant_lineage_text or source_role_counts.get("assistant")) and not assistant_feature_memory_only:
        assistant_policies.append("selected_assistant_decision_outcome_only")
    if assistant_feature_memory_only and not assistant_policies:
        assistant_policies.append("selected_assistant_profile_fact")
    user_lineage_text = "\n".join(user_text_parts)
    user_policies = ["selected_user_prompt"]
    if user_lineage_text and selected_user_profile_fact_policy(user_lineage_text):
        user_policies.append("selected_user_profile_fact")
    inferred_policy_by_role = {
        "assistant": assistant_policies,
        "tool": ["selected_tool_evidence_only"],
        "user": user_policies,
    }
    for role, count in source_role_counts.items():
        for policy_name in inferred_policy_by_role.get(role, []):
            if not policy_name or policy_name in source_memory_selection_policy_counts:
                continue
            source_memory_selection_policies.append(policy_name)
            add_count(source_memory_selection_policy_counts, policy_name, count=max(1, int(count or 0)))

    def ordered(values: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            if value and value not in seen:
                seen.add(value)
                result.append(value)
        return result

    return {
        "source_roles": ordered(source_roles),
        "source_role_counts": source_role_counts,
        "source_hook_types": ordered(source_hook_types),
        "source_hook_type_counts": source_hook_type_counts,
        "source_codex_events": ordered(source_codex_events),
        "source_codex_event_counts": source_codex_event_counts,
        "source_memory_selection_policies": ordered(source_memory_selection_policies),
        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
    }


def fast_async_hook_ingest(
    server: Any,
    *,
    args: argparse.Namespace,
    text: str,
    role: str,
    agent_context: Json,
    hook: Json | None,
    original_text: str | None = None,
    codex_event: str | None = None,
) -> Json:
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
    fast_event_type_by_role = {
        "user": "user_prompt",
        "assistant": "assistant_response",
        "tool": "tool_evidence",
    }
    semantic_event_type = fast_event_type_by_role.get(normalize_message_role(role), "conversation_event")
    hook_type = str((hook or {}).get("hook_type") or "").strip() or hook_type_for_event(args.event)
    lineage = hook_lineage_fields(hook)
    tool_name = str(agent_context.get("tool_name") or "").strip()
    tool_status = str(agent_context.get("tool_status") or "").strip()
    tool_fields: Json = {}
    if role == "tool":
        if tool_name:
            tool_fields["tool_name"] = tool_name
        if tool_status:
            tool_fields["tool_status"] = tool_status
    source_memory_scopes, source_session_continuities = pending_extraction_memory_layer_intent(scope)
    event_name = str(codex_event or args.event)
    selection_metadata = codex_memory_selection_metadata(
        role=role,
        event=event_name,
        text=text,
        original_text=original_text,
    )
    if selection_metadata:
        messages[0]["metadata"] = {"codex_memory_selection": selection_metadata}
    selection_policy_values = []
    if isinstance(selection_metadata.get("policies"), list):
        selection_policy_values.extend(selection_metadata.get("policies", []))
    if selection_metadata.get("policy"):
        selection_policy_values.append(selection_metadata.get("policy"))
    source_memory_selection_policies = []
    for selection_policy_value in selection_policy_values:
        selection_policy_name = str(selection_policy_value or "").strip()
        if selection_policy_name and selection_policy_name not in source_memory_selection_policies:
            source_memory_selection_policies.append(selection_policy_name)
    source_memory_selection_policy_counts = {
        policy_name: 1
        for policy_name in source_memory_selection_policies
    }
    profile_memory_fields = fast_hook_profile_memory_fields(
        role=role,
        text=text,
        selection_policies=source_memory_selection_policies,
    )
    metadata: Json = codex_hook_metadata(
        source="codex_hook_fast_async",
        event=event_name,
        agent_context=agent_context,
        session_id_source=str(agent_context.get("session_id_source") or ""),
        source_role=role,
        **lineage,
    )
    tool_policy_event = hook_type if role == "tool" and is_tool_hook_event(hook_type) else args.event
    tool_result_skipped = role == "tool" and not should_ingest_tool_result(tool_policy_event)
    tool_result_raw_only = (
        role == "tool"
        and should_ingest_tool_result(tool_policy_event)
        and not should_promote_tool_result_to_serving(tool_policy_event)
    )
    metadata["serving_projection"] = {
        "record_type": "context_event",
        "event_id_hash": event_id_hash,
        "visibility": "raw_only" if tool_result_raw_only else "serving",
        "source_raw_record_type": "raw_agent_message",
    }
    if tool_result_raw_only:
        metadata["tool_result_ingestion"] = {
            "policy": "raw_only_compact_evidence",
            "reason": "explicit_tool_result_raw_capture",
            "raw_env": "MATRIXARK_HOOK_TOOL_RESULT_RAW",
            "serving_env": "MATRIXARK_HOOK_TOOL_RESULT_SERVING",
        }
    if profile_memory_fields:
        metadata.update(profile_memory_fields)
    if selection_metadata:
        metadata["codex_memory_selection"] = selection_metadata
    retention = hook_retention_fields(text=text, role=role, now_ms=now)
    raw_record: Json = {
        "record_type": "agent_message",
        "raw_record_type": "raw_agent_message",
        "raw_ingestion_visibility": "backfill_only",
        "serving_visible": False,
        "serving_projection_record_type": "context_event",
        "serving_context_event_hash": event_id_hash,
        "serving_event_id_hash": event_id_hash,
        "session_binding": "metadata_only_for_backfill_batching",
        "source_kind": "message",
        "source_role": role,
        "role": role,
        "text": text,
        "codex_event": event_name,
        "codex_api_event": event_name,
        **tool_fields,
        "messages": messages,
        "scope": scope,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        **lineage,
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
        **profile_memory_fields,
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
        "event_type": semantic_event_type,
        "extraction_status": "pending",
        "extraction_mode": "async_pending",
        "status": "pending",
        "source_kind": "message",
        "source_role": role,
        "role": role,
        **tool_fields,
        "source_roles": [role] if role else [],
        "source_role_counts": {role: 1} if role else {},
        "source_hook_types": [hook_type] if hook_type else [],
        "source_hook_type_counts": {hook_type: 1} if hook_type else {},
        "codex_event": event_name,
        "codex_api_event": event_name,
        "source_codex_events": [event_name] if event_name else [],
        "source_codex_event_counts": {event_name: 1} if event_name else {},
        "source_memory_scopes": source_memory_scopes,
        "source_session_continuities": source_session_continuities,
        "source_extraction_phases": ["pending_async"],
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
        **profile_memory_fields,
        "scope": scope,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "memory_scope": "session",
        "session_continuity": "same_session",
        "extraction_phase": "pending_async",
        "final_session_boundary": False,
        **lineage,
        "metadata": metadata,
        "envelope": {
            "kind": "message",
            "source_role": role,
            "codex_event": event_name,
            "event_type": semantic_event_type,
            **tool_fields,
            "messages": messages,
            "scope": scope,
            "metadata": metadata,
            "ingestion_time_ms": now,
            "storage_options": storage_options,
            "source_memory_selection_policies": source_memory_selection_policies,
            "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
            **profile_memory_fields,
            **lineage,
        },
        "internal_extraction": {
            "mode": "async_pending",
            "classification": "PENDING_ASYNC_EXTRACTION",
            "event_type": semantic_event_type,
            "extraction_status": "pending",
            "extraction_mode": "async_pending",
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
    if selection_metadata:
        raw_record["codex_memory_selection"] = selection_metadata
        record["codex_memory_selection"] = selection_metadata
        record["envelope"]["codex_memory_selection"] = selection_metadata
    record = attach_memory_layer(record)
    projection_records = fast_hook_event_projection_records(event_record=record, updated_at_ms=now)
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
        "source_roles": [role] if role else [],
        "source_role_counts": {role: 1} if role else {},
        "source_hook_types": [hook_type] if hook_type else [],
        "source_hook_type_counts": {hook_type: 1} if hook_type else {},
        "source_codex_events": [event_name] if event_name else [],
        "source_codex_event_counts": {event_name: 1} if event_name else {},
        "source_memory_scopes": source_memory_scopes,
        "source_session_continuities": source_session_continuities,
        "source_extraction_phases": ["pending_async"],
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": source_memory_selection_policy_counts,
        **profile_memory_fields,
        "memory_scope": "session",
        "session_continuity": "same_session",
        "extraction_phase": "pending_async",
        "final_session_boundary": False,
        "reason": "codex_hook_fast_async_direct_queue",
        **tool_fields,
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
        source_role=role,
        hook_type=hook_type,
        codex_event=event_name,
        source_memory_selection_policies=source_memory_selection_policies,
        source_memory_selection_policy_counts=source_memory_selection_policy_counts,
        source_lineage=lineage,
        profile_memory_class=str(profile_memory_fields.get("profile_memory_class") or ""),
        profile_memory_kind=str(profile_memory_fields.get("profile_memory_kind") or ""),
    )
    enqueue_raw = getattr(adapter, "enqueue_raw_ingestion_records", None)
    session_commit_result: Json = {}
    pre_ingest_idle_commit_result: Json = {}
    session_commit = getattr(adapter, "session_commit", None)
    threshold = int(getattr(args, "session_commit_threshold", 20) or 20)
    idle_timeout_ms = int(getattr(args, "idle_commit_timeout_ms", 0) or 0)
    pending_session_events = getattr(adapter, "pending_session_events", None)
    pending_before_ingest: list[Json] = []
    pending_before_ingest_message_count = 0
    idle_elapsed_before_ingest_ms = 0
    if callable(pending_session_events):
        try:
            pending_before_ingest = list(pending_session_events(scope))
        except Exception:
            pending_before_ingest = []
    pending_before_ingest_message_count = pending_session_message_count(pending_before_ingest)
    if pending_before_ingest and idle_timeout_ms > 0:
        latest_event_time = max(
            int(record.get("envelope", {}).get("ingestion_time_ms") or record.get("updated_at_ms") or 0)
            for record in pending_before_ingest
        )
        if latest_event_time > 0:
            idle_elapsed_before_ingest_ms = max(0, now - latest_event_time)
        latest_deadline_ms = max(
            int(record.get("envelope", {}).get("idle_commit_deadline_ms") or 0)
            for record in pending_before_ingest
        )
        if latest_deadline_ms > 0 and now >= latest_deadline_ms:
            idle_elapsed_before_ingest_ms = max(idle_elapsed_before_ingest_ms, idle_timeout_ms)
    auto_batch_extract_on_ingest = should_auto_batch_extract_on_ingest(args.event)
    commit_extraction_options: Json = hook_session_commit_extraction_options(args)
    pending_before_ingest_lineage = pending_session_buffer_lineage(
        pending_before_ingest,
        fallback_role="",
        fallback_hook_type="",
        fallback_codex_event="",
    )
    should_pre_ingest_idle_commit = (
        auto_batch_extract_on_ingest
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
            trigger="idle_timeout_before_ingest",
            session_id_source=str((hook or {}).get("session_id_source") or ""),
            identity=hook,
        )
        pre_idle_args: Json = {
            "scope": scope,
            "metadata": {
                **pending_before_ingest_lineage,
                "node_path": node_path,
                "commit_source": "codex_hook_fast_async_pre_ingest_idle",
            },
            "threshold_messages": threshold,
            "force": False,
            "commit_reason": "idle_timeout",
            "idle_timeout_ms": idle_timeout_ms,
            **commit_extraction_options,
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

    if tool_result_skipped:
        return {
            "status": "skipped",
            "reason": "tool_result_ingestion_disabled",
            "raw_ingestion_status": "skipped_tool_result",
            "serving_projection_status": "skipped_tool_result",
            "serving_record_count": 0,
            "async_processing": False,
            "async_pipeline_status": "skipped_tool_result",
            "session_buffer_status": "skipped_tool_result",
            "tool_result_policy": "skip_by_default",
            "tool_result_raw_env": "MATRIXARK_HOOK_TOOL_RESULT_RAW",
            "tool_result_serving_env": "MATRIXARK_HOOK_TOOL_RESULT_SERVING",
            "event_id_hash": event_id_hash,
            "node_hash": node_hash,
            "storage_options": storage_options,
            "hook_captured": hook is not None,
            "extraction_mode": "skipped",
            "idle_commit_result": pre_ingest_idle_commit_result,
            "auto_batch_extract_result": pre_ingest_idle_commit_result if should_pre_ingest_idle_commit else {},
        }
    should_write_raw_record = not (
        role == "tool"
        and is_tool_hook_event(tool_policy_event)
        and not HOOK_TOOL_RESULT_RAW
        and not HOOK_TOOL_RESULT_ROLLOUT_BACKFILL
    )
    raw_ingestion_status = "skipped_tool_result_raw_capture" if not should_write_raw_record else "unavailable"
    if should_write_raw_record and callable(enqueue_raw):
        enqueue_raw([raw_record])
        raw_ingestion_status = "accepted"
    elif should_write_raw_record:
        append_raw = getattr(adapter, "_append_raw_ingestion_records", None)
        if callable(append_raw):
            append_raw([raw_record])
            raw_ingestion_status = "accepted"
    if tool_result_raw_only:
        return {
            "status": "accepted",
            "sync_write_mode": "hook_fast_async_direct_queue",
            "raw_ingestion_status": raw_ingestion_status,
            "serving_projection_status": "skipped_raw_only_tool_result",
            "serving_record_count": 0,
            "async_processing": False,
            "async_pipeline_status": "skipped_raw_only_tool_result",
            "session_buffer_status": "skipped_raw_only_tool_result",
            "tool_result_policy": "raw_only_compact_evidence",
            "event_id_hash": event_id_hash,
            "node_hash": node_hash,
            "storage_options": storage_options,
            "hook_captured": hook is not None,
            "extraction_mode": "raw_only",
            "idle_commit_result": pre_ingest_idle_commit_result,
            "auto_batch_extract_result": pre_ingest_idle_commit_result if should_pre_ingest_idle_commit else {},
        }
    if idle_timeout_ms > 0:
        record["envelope"]["idle_commit_deadline_ms"] = now + idle_timeout_ms
        record["envelope"]["idle_commit_cutoff_ms"] = now
    serving_records = [record, pipeline_task, *projection_records, *summary_dirty_records]
    append_many_materialized = getattr(adapter, "_append_many_materialized", None)
    if callable(append_many_materialized):
        append_many_materialized([record], allow_queue=False)
        remaining_serving_records = serving_records[1:]
        if remaining_serving_records:
            append_many_materialized(remaining_serving_records, allow_queue=False)
    else:
        enqueue(serving_records)
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
    pending_message_count = 0
    pending_after_ingest: list[Json] = []
    if callable(pending_session_events):
        try:
            pending_after_ingest = list(pending_session_events(scope))
            pending_event_count = len(pending_after_ingest)
            pending_message_count = pending_session_message_count(pending_after_ingest)
        except Exception:
            pending_event_count = 0
            pending_message_count = 0
            pending_after_ingest = []
    pending_lineage = pending_session_buffer_lineage(
        pending_after_ingest,
        fallback_role=role,
        fallback_hook_type=hook_type,
        fallback_codex_event=args.event,
    )
    pending_commit_metadata: Json = {
        **pending_lineage,
        "node_path": node_path,
        "commit_source": "codex_hook_fast_async",
    }
    should_boundary_commit = should_commit_session(args.event)
    should_threshold_commit = (
        not should_boundary_commit
        and auto_batch_extract_on_ingest
        and (pending_event_count >= threshold or pending_message_count >= threshold)
    )
    should_immediate_idle_commit = (
        not should_boundary_commit
        and not should_threshold_commit
        and auto_batch_extract_on_ingest
        and idle_timeout_ms == 0
        and pending_event_count > 0
    )
    idle_commit_deadline_ms = now + idle_timeout_ms if idle_timeout_ms > 0 else 0
    idle_commit_cutoff_ms = now if idle_timeout_ms > 0 else 0
    should_schedule_idle_commit = (
        not should_boundary_commit
        and not should_threshold_commit
        and auto_batch_extract_on_ingest
        and idle_timeout_ms > 0
        and pending_event_count > 0
    )
    threshold_commit_scheduled = False
    if should_schedule_idle_commit:
        enqueue(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": stable_int_hash(f"async_pipeline_idle_commit:{event_id_hash}"),
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "status": "idle_commit_scheduled",
                    "stages": ["extraction", "summary", "compression", "embedding"],
                    "reason": "session_buffer_idle_deadline",
                    "trigger_policy": "idle_timeout",
                    "auto_batch_extract": auto_batch_extract_on_ingest,
                    "threshold_messages": threshold,
                    "idle_commit_timeout_ms": idle_timeout_ms,
                    "idle_commit_deadline_ms": idle_commit_deadline_ms,
                    "idle_commit_cutoff_ms": idle_commit_cutoff_ms,
                    "idle_commit_pending_event_count": pending_event_count,
                    "idle_commit_pending_message_count": pending_message_count,
                    **pending_lineage,
                    "source_extraction_phases": ["provisional"],
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "updated_at_ms": now,
                }
            ]
        )
    if should_threshold_commit and not callable(session_commit):
        threshold_commit_scheduled = True
        enqueue(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": stable_int_hash(f"async_pipeline_threshold_commit:{event_id_hash}"),
                    "event_id_hash": event_id_hash,
                    "node_hash": node_hash,
                    "node_path": node_path,
                    "scope": scope,
                    "status": "threshold_commit_scheduled",
                    "stages": ["extraction", "entity", "summary", "compression", "embedding"],
                    "reason": "session_buffer_threshold_reached",
                    "trigger_policy": "threshold",
                    "auto_batch_extract": auto_batch_extract_on_ingest,
                    "threshold_messages": threshold,
                    "threshold_pending_event_count": pending_event_count,
                    "threshold_pending_message_count": pending_message_count,
                    **pending_lineage,
                    "source_extraction_phases": ["provisional"],
                    "extraction_phase": "provisional",
                    "final_session_boundary": False,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "created_at_ms": now,
                    "updated_at_ms": now,
                }
            ]
        )
    should_idle_commit = should_boundary_commit and commit_reason_for_event(args.event) == "idle_timeout"
    if callable(session_commit) and (should_threshold_commit or should_boundary_commit or should_immediate_idle_commit):
        commit_reason = (
            commit_reason_for_event(args.event)
            if should_boundary_commit
            else "idle_timeout"
            if should_immediate_idle_commit
            else "threshold"
        )
        commit_args: Json = {
            "scope": scope,
            "metadata": pending_commit_metadata,
            "threshold_messages": threshold,
            "force": should_boundary_commit and commit_reason != "idle_timeout",
            "commit_reason": commit_reason,
            **commit_extraction_options,
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
            if should_immediate_idle_commit:
                commit_args["commit_before_ms"] = now
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
    tail_idle_commit_scheduled = False
    tail_idle_commit_deadline_ms = 0
    tail_idle_commit_cutoff_ms = 0
    tail_pending_event_count = 0
    tail_pending_message_count = 0
    if (
        should_threshold_commit
        and callable(pending_session_events)
        and successful_session_commit_status(session_commit_result.get("status") if isinstance(session_commit_result, dict) else "")
        and idle_timeout_ms > 0
    ):
        try:
            tail_pending = list(pending_session_events(scope))
        except Exception:
            tail_pending = []
        tail_pending_event_count = len(tail_pending)
        tail_pending_message_count = pending_session_message_count(tail_pending)
        if tail_pending_event_count > 0:
            tail_lineage = pending_session_buffer_lineage(
                tail_pending,
                fallback_role=role,
                fallback_hook_type=hook_type,
                fallback_codex_event=args.event,
            )
            tail_idle_commit_deadline_ms = now + idle_timeout_ms
            tail_idle_commit_cutoff_ms = now
            tail_idle_commit_scheduled = True
            enqueue(
                [
                    {
                        "record_type": "matrixark_async_pipeline_task",
                        "task_hash": stable_int_hash(f"async_pipeline_idle_commit_tail:{event_id_hash}:{tail_pending_event_count}"),
                        "event_id_hash": event_id_hash,
                        "node_hash": node_hash,
                        "node_path": node_path,
                        "scope": scope,
                        "status": "idle_commit_scheduled",
                        "stages": ["extraction", "summary", "compression", "embedding"],
                        "reason": "session_buffer_threshold_tail_idle_deadline",
                        "trigger_policy": "idle_timeout",
                        "auto_batch_extract": auto_batch_extract_on_ingest,
                        "threshold_messages": threshold,
                        "idle_commit_timeout_ms": idle_timeout_ms,
                        "idle_commit_deadline_ms": tail_idle_commit_deadline_ms,
                        "idle_commit_cutoff_ms": tail_idle_commit_cutoff_ms,
                        "idle_commit_pending_event_count": tail_pending_event_count,
                        "idle_commit_pending_message_count": tail_pending_message_count,
                        **tail_lineage,
                        "source_extraction_phases": ["provisional"],
                        "extraction_phase": "provisional",
                        "final_session_boundary": False,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                        "created_at_ms": now,
                        "updated_at_ms": now,
                    }
                ]
            )
    auto_batch_extract_result: Json = {}
    if should_threshold_commit:
        auto_batch_extract_result = session_commit_result or {
            "status": "deferred",
            "trigger_policy": "threshold",
            "commit_reason": "threshold",
            "reason": "session_commit_missing_threshold_task_scheduled"
            if threshold_commit_scheduled
            else "session_commit_missing",
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold,
            "threshold_commit_scheduled": threshold_commit_scheduled,
            "extraction_phase": "provisional",
            "final_session_boundary": False,
        }
        if tail_idle_commit_scheduled and isinstance(auto_batch_extract_result, dict):
            auto_batch_extract_result = {
                **auto_batch_extract_result,
                "tail_idle_commit_scheduled": True,
                "tail_pending_event_count": tail_pending_event_count,
                "tail_pending_message_count": tail_pending_message_count,
                "tail_idle_commit_deadline_ms": tail_idle_commit_deadline_ms,
                "tail_idle_commit_cutoff_ms": tail_idle_commit_cutoff_ms,
            }
    elif should_pre_ingest_idle_commit:
        auto_batch_extract_result = pre_ingest_idle_commit_result
    elif should_idle_commit:
        auto_batch_extract_result = session_commit_result
    elif should_immediate_idle_commit:
        auto_batch_extract_result = session_commit_result or {
            "status": "deferred",
            "trigger_policy": "idle_timeout",
            "commit_reason": "idle_timeout",
            "reason": "session_commit_missing_immediate_idle",
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold,
            "idle_commit_timeout_ms": idle_timeout_ms,
            "idle_commit_cutoff_ms": now,
            "extraction_phase": "provisional",
            "final_session_boundary": False,
        }
    elif should_schedule_idle_commit:
        auto_batch_extract_result = {
            "status": "deferred",
            "trigger_policy": "idle_timeout",
            "commit_reason": "idle_timeout",
            "reason": "session_buffer_idle_deadline_scheduled",
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold,
            "idle_commit_timeout_ms": idle_timeout_ms,
            "idle_commit_deadline_ms": idle_commit_deadline_ms,
            "idle_commit_cutoff_ms": idle_commit_cutoff_ms,
            "idle_commit_scheduled": True,
            "extraction_phase": "provisional",
            "final_session_boundary": False,
        }
    elif should_boundary_commit:
        auto_batch_extract_result = session_commit_result
    return {
        "status": "accepted",
        "sync_write_mode": "hook_fast_async_direct_queue",
        "raw_ingestion_status": raw_ingestion_status,
        "serving_projection_status": "accepted",
        "serving_projection_record_count": len(serving_records),
        "async_processing": True,
        "async_pipeline_status": "pending",
        "async_pipeline_task_hash": pipeline_task["task_hash"],
        "summary_dirty_count": len(summary_dirty_records),
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "session_buffer": {
            "registered": callable(append_session_buffer),
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "pending_before_ingest_count": len(pending_before_ingest),
            "pending_before_ingest_message_count": pending_before_ingest_message_count,
            "pending_after_ingest_count": pending_event_count,
            "pending_after_ingest_message_count": pending_message_count,
            "threshold_messages": threshold,
            "threshold_ready": should_threshold_commit,
            "idle_commit_timeout_ms": idle_timeout_ms,
            "idle_commit_deadline_ms": tail_idle_commit_deadline_ms if tail_idle_commit_scheduled else idle_commit_deadline_ms if should_schedule_idle_commit else 0,
            "idle_commit_cutoff_ms": tail_idle_commit_cutoff_ms if tail_idle_commit_scheduled else idle_commit_cutoff_ms if should_schedule_idle_commit else 0,
            "idle_commit_scheduled": bool(should_schedule_idle_commit or tail_idle_commit_scheduled),
            "tail_idle_commit_scheduled": tail_idle_commit_scheduled,
            "tail_pending_event_count": tail_pending_event_count,
            "tail_pending_message_count": tail_pending_message_count,
            "threshold_commit_scheduled": threshold_commit_scheduled,
            "idle_ready": bool(should_idle_commit or should_immediate_idle_commit),
            "immediate_idle_ready": should_immediate_idle_commit,
            "pre_ingest_idle_ready": should_pre_ingest_idle_commit,
            "pre_ingest_idle_elapsed_ms": idle_elapsed_before_ingest_ms,
            "commit_after_current_ingest": bool(
                should_threshold_commit or should_boundary_commit or should_immediate_idle_commit
            ),
            "auto_batch_extract": auto_batch_extract_on_ingest,
            "boundary_commit_requested": should_boundary_commit,
        },
        "idle_commit_result": pre_ingest_idle_commit_result,
        "auto_batch_extract_result": auto_batch_extract_result,
        "session_commit": session_commit_result if should_boundary_commit else {},
        "storage_options": storage_options,
        "hook_captured": hook is not None,
        "extraction_mode": "async_pending",
    }


def rollout_role_and_text(event: str, payload: Json) -> tuple[str, str, str, str, str]:
    if event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        original_text = latest_codex_tool_output_from_rollout(payload)
        text = selected_tool_memory_text(original_text, payload)
        return "tool", text, original_text, "PreviousToolOutputBackfill", "previous-tool-output"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        original_text = latest_codex_assistant_message_from_rollout_raw(payload)
        text = selected_assistant_memory_text(original_text)
        return "assistant", text, original_text, "PreviousAssistantBackfill", "previous-assistant"
    return "", "", "", "", ""



def _append_cli_value(cmd: list[str], flag: str, value: Any) -> None:
    if value in (None, ""):
        return
    cmd.extend([flag, str(value)])


def spawn_idle_commit_worker_child(args: argparse.Namespace, *, ingest: Json, session_id_source: str) -> Json:
    if getattr(args, "idle_commit_worker_only", False):
        return {"status": "skipped", "reason": "already_idle_commit_worker"}
    if os.environ.get("MATRIXARK_DISABLE_IDLE_COMMIT_WORKER", "").strip().lower() in {"1", "true", "yes", "on"}:
        return {"status": "disabled", "reason": "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER"}
    session_buffer = ingest.get("session_buffer") if isinstance(ingest, dict) else {}
    if not isinstance(session_buffer, dict) or not session_buffer.get("idle_commit_scheduled"):
        return {"status": "skipped", "reason": "idle_commit_not_scheduled"}
    auto_batch = ingest.get("auto_batch_extract_result") if isinstance(ingest.get("auto_batch_extract_result"), dict) else {}
    if auto_batch and auto_batch.get("trigger_policy") != "idle_timeout":
        return {"status": "skipped", "reason": "scheduled_trigger_is_not_idle_timeout"}
    timeout_ms = int(getattr(args, "idle_commit_timeout_ms", 0) or 0)
    if timeout_ms <= 0:
        return {"status": "skipped", "reason": "idle_commit_timeout_disabled"}
    deadline_ms = int(session_buffer.get("idle_commit_deadline_ms") or auto_batch.get("idle_commit_deadline_ms") or 0)
    now_ms_value = int(time.time() * 1000)
    cutoff_ms = int(session_buffer.get("idle_commit_cutoff_ms") or auto_batch.get("idle_commit_cutoff_ms") or now_ms_value)
    delay_ms = max(0, deadline_ms - now_ms_value) if deadline_ms > 0 else timeout_ms
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--idle-commit-worker-only",
        "--event",
        "IdleTimeout",
        "--backend",
        str(getattr(args, "backend", default_hook_backend())),
        "--account-id",
        str(getattr(args, "account_id", "")),
        "--tenant-id",
        str(getattr(args, "tenant_id", "")),
        "--user-id",
        str(getattr(args, "user_id", "")),
        "--session-id",
        str(getattr(args, "session_id", "")),
        "--team",
        str(getattr(args, "team", "codex")),
        "--project",
        str(getattr(args, "project", "local")),
        "--session-commit-threshold",
        str(getattr(args, "session_commit_threshold", 20)),
        "--idle-commit-timeout-ms",
        str(timeout_ms),
        "--idle-commit-cutoff-ms",
        str(cutoff_ms),
        "--understanding-provider",
        str(getattr(args, "understanding_provider", "rules")),
        "--segment-provider",
        str(getattr(args, "segment_provider", "deterministic")),
        "--request-timeout-ms",
        str(getattr(args, "request_timeout_ms", 60000)),
        "--io-timeout-ms",
        str(getattr(args, "io_timeout_ms", 60000)),
        "--repo-root",
        str(getattr(args, "repo_root", Path(__file__).resolve().parents[1])),
    ]
    for flag, attr in [
        ("--event-log", "event_log"),
        ("--api-key", "api_key"),
        ("--metaserver", "metaserver"),
        ("--namespace", "namespace"),
        ("--table", "table"),
        ("--temporalstore-lib", "temporalstore_lib"),
        ("--rust-proxy", "rust_proxy"),
        ("--rust-direct-sdk", "rust_direct_sdk"),
        ("--rust-cli", "rust_cli"),
        ("--storage-prefix", "storage_prefix"),
        ("--session-state-dir", "session_state_dir"),
        ("--extraction-provider", "extraction_provider"),
        ("--segment-model", "segment_model"),
        ("--segment-model-path", "segment_model_path"),
        ("--segment-max-new-tokens", "segment_max_new_tokens"),
        ("--segment-provider-fallback", "segment_provider_fallback"),
    ]:
        _append_cli_value(cmd, flag, getattr(args, attr, ""))
    if bool(getattr(args, "skip_prior_context", False)):
        cmd.append("--skip-prior-context")
    if session_id_source:
        cmd.extend(["--query", f"idle commit worker for {session_id_source}"])
    env = os.environ.copy()
    env["MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS"] = str(delay_ms)
    env["MATRIXARK_IDLE_COMMIT_CUTOFF_MS"] = str(cutoff_ms)
    env["MATRIXARK_IDLE_COMMIT_WORKER_PARENT_EVENT"] = str(getattr(args, "event", ""))
    try:
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=str(getattr(args, "repo_root", Path(__file__).resolve().parents[1])),
            env=env,
        )
    except OSError as exc:
        return {
            "status": "error",
            "reason": "idle_commit_worker_spawn_failed",
            "error": _compact_one_line(str(exc), max_chars=300),
            "delay_ms": delay_ms,
        }
    return {
        "status": "spawned",
        "reason": "session_buffer_idle_deadline_worker",
        "delay_ms": delay_ms,
        "idle_commit_timeout_ms": timeout_ms,
        "idle_commit_deadline_ms": deadline_ms,
        "idle_commit_cutoff_ms": cutoff_ms,
    }


def run_idle_commit_worker_only(args: argparse.Namespace, session_id_source: str, codex_identity: Json) -> int:
    delay_ms = int(os.environ.get("MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS", "0") or 0)
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)
    server = build_server(args)
    try:
        common: Json = {"scope": scope_from_args(args)}
        if args.api_key:
            common["api_key"] = args.api_key
        result = call_tool(
            server,
            "matrixark_session_commit",
            {
                **common,
                "threshold_messages": args.session_commit_threshold,
                "force": False,
                "commit_reason": "idle_timeout",
                "idle_timeout_ms": args.idle_commit_timeout_ms,
                "commit_before_ms": args.idle_commit_cutoff_ms or None,
                **hook_session_commit_extraction_options(args),
                "storage_options": hook_storage_options(),
                "agent_hook": {
                    **codex_agent_hook(
                        hook_type="session_commit",
                        hook_id=f"idle_commit_worker:{args.session_id}:{int(time.time() * 1000)}",
                        idempotency_key=f"idle-commit-worker:{args.session_id}:{args.idle_commit_cutoff_ms or int(time.time() * 1000)}",
                        trigger="IdleTimeout:worker",
                        session_id_source=session_id_source,
                        identity=codex_identity,
                    ),
                },
            },
        )
        print(json.dumps({"status": "ok", "worker": "idle_commit", "result": session_commit_summary(result)}, sort_keys=True))
    finally:
        close_server_best_effort(server)
    return 0


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
    role, text, original_text, codex_event, idempotency_prefix = rollout_role_and_text(args.event, payload)
    if not role or not text:
        return 0
    if role == "tool" and not should_rollout_backfill_tool_result():
        return 0
    codex_identity = codex_hook_lineage_from_payload(payload, args, session_id_source=session_id_source)
    agent_context = agent_context_from_payload(payload, event=args.event, session_id_source=session_id_source, args=args)
    server = build_server(args)
    try:
        common: Json = {"scope": scope_from_args(args)}
        if args.api_key:
            common["api_key"] = args.api_key
        backfill_hook = {
            **codex_agent_hook(
                hook_type="tool_result" if role == "tool" else "after_llm",
                hook_id=f"Async{codex_event}:{stable_short_hash(text)}",
                idempotency_key=f"{idempotency_prefix}:{stable_short_hash(text)}",
                trigger=f"{args.event}:async_rollout_backfill",
                session_id_source=session_id_source,
                identity=codex_identity,
            ),
        }
        ingest_result: Json = {}
        if HOOK_FAST_ASYNC_INGEST:
            ingest_result = fast_async_hook_ingest(
                server,
                args=args,
                text=text,
                role=role,
                agent_context=agent_context,
                hook=backfill_hook,
                original_text=original_text,
                codex_event=codex_event,
            )
        if not ingest_result:
            ingest_result = call_tool(
                server,
                "matrixark_ingest",
                hook_async_message_ingest_args(
                    common,
                    args,
                    event=codex_event,
                    role=role,
                    text=text,
                    original_text=original_text,
                    metadata=codex_hook_metadata(
                        source="codex_hook_rollout_async_backfill",
                        event=codex_event,
                        agent_context=agent_context,
                        session_id_source=session_id_source,
                        backfill_reason="codex_rollout_is_readable_after_synchronous_hook_boundary",
                    ),
                    agent_hook=backfill_hook,
                ),
            )
        spawn_idle_commit_worker_child(
            args,
            ingest=ingest_result,
            session_id_source=session_id_source,
        )
        if should_commit_session(args.event):
            call_tool(
                server,
                "matrixark_session_commit",
                {
                    **common,
                    **hook_session_commit_extraction_options(args),
                    "threshold_messages": 1,
                    "force": True,
                    "commit_reason": "async_rollout_backfill",
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


def successful_session_commit_status(status: Any) -> bool:
    return str(status or "").strip().lower() in SESSION_COMMIT_SUCCESS_STATUSES


def main() -> int:
    args = parse_args()
    validate_hook_backend_policy(args.backend)
    payload = read_stdin_payload()
    resolved_session_id, session_id_source = resolve_session_id(payload, args)
    args.session_id = resolved_session_id
    codex_identity = codex_hook_lineage_from_payload(payload, args, session_id_source=session_id_source)
    if args.idle_commit_worker_only:
        return run_idle_commit_worker_only(args, session_id_source, codex_identity)
    if args.rollout_backfill_only:
        return run_rollout_backfill_only(args, payload, session_id_source)
    text = payload_text(payload, event=args.event) or args.query
    original_hook_text = text
    if args.event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        text = ""
        original_hook_text = ""
        for _attempt in range(12):
            rollout_raw = latest_codex_tool_output_from_rollout(payload)
            rollout_text = selected_tool_memory_text(rollout_raw, payload)
            if rollout_text:
                text = rollout_text
                original_hook_text = rollout_raw
                break
            time.sleep(0.2)
    if args.event in {"Stop", "PostCompact", "SubagentStop"}:
        fallback_text = text
        text = ""
        for _attempt in range(12):
            rollout_raw = latest_codex_assistant_message_from_rollout_raw(payload)
            rollout_text = selected_assistant_memory_text(rollout_raw)
            if rollout_text:
                text = rollout_text
                original_hook_text = rollout_raw
                break
            time.sleep(0.2)
        if not text:
            text = selected_assistant_memory_text(fallback_text)
            original_hook_text = fallback_text
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
    if not text and not raw_uri and not should_commit_session(args.event):
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
        if args.event == "UserPromptSubmit":
            backfill_warnings: list[str] = []
            previous_tool_raw = latest_codex_tool_output_from_rollout(payload) if should_rollout_backfill_tool_result() else ""
            previous_tool_output = selected_tool_memory_text(previous_tool_raw, payload)
            if previous_tool_output and previous_tool_output != text:
                previous_tool_hook = {
                    "source": "codex",
                    "hook_type": "tool_result",
                    "hook_id": f"PreviousToolOutputBackfill:{stable_short_hash(previous_tool_output)}",
                    "observed_at_ms": int(time.time() * 1000),
                    "idempotency_key": f"previous-tool-output:{stable_short_hash(previous_tool_output)}",
                    "trigger": "UserPromptSubmit:previous_tool_output_backfill",
                    "auto_captured": True,
                    "session_id_source": session_id_source,
                }
                backfill_result = {}
                if HOOK_FAST_ASYNC_INGEST:
                    backfill_result = fast_async_hook_ingest(
                        server,
                        args=args,
                        text=previous_tool_output,
                        role="tool",
                        agent_context=agent_context,
                        hook=previous_tool_hook,
                        original_text=previous_tool_raw,
                        codex_event="PreviousToolOutputBackfill",
                    )
                    if backfill_result:
                        trace.setdefault("fast_async_backfill", {})["previous_tool_output"] = backfill_result
                if not backfill_result:
                    backfill_result = trace_tool_call(
                        server,
                        "matrixark_ingest",
                        hook_async_message_ingest_args(
                            common,
                            args,
                            event="PreviousToolOutputBackfill",
                            role="tool",
                            text=previous_tool_output,
                            original_text=previous_tool_raw,
                            metadata=codex_hook_metadata(
                                source="codex_hook_rollout_backfill",
                                event="PreviousToolOutputBackfill",
                                agent_context=agent_context,
                                session_id_source=session_id_source,
                                backfill_reason="post_tool_hook_payload_can_arrive_before_rollout_tool_output_is_visible",
                            ),
                            agent_hook=previous_tool_hook,
                        ),
                        trace,
                    )
                tool_warning = timeout_warning(backfill_result)
                if tool_warning:
                    backfill_warnings.append(tool_warning)
                spawn_idle_commit_worker_child(
                    args,
                    ingest=backfill_result,
                    session_id_source=session_id_source,
                )
            previous_assistant_raw = latest_codex_assistant_message_from_rollout_raw(payload)
            previous_assistant = selected_assistant_memory_text(previous_assistant_raw)
            if previous_assistant and previous_assistant != text:
                previous_assistant_hook = {
                    "source": "codex",
                    "hook_type": "after_llm",
                    "hook_id": f"PreviousAssistantBackfill:{stable_short_hash(previous_assistant)}",
                    "observed_at_ms": int(time.time() * 1000),
                    "idempotency_key": f"previous-assistant:{stable_short_hash(previous_assistant)}",
                    "trigger": "UserPromptSubmit:previous_assistant_backfill",
                    "auto_captured": True,
                    "session_id_source": session_id_source,
                }
                backfill_result = {}
                if HOOK_FAST_ASYNC_INGEST:
                    backfill_result = fast_async_hook_ingest(
                        server,
                        args=args,
                        text=previous_assistant,
                        role="assistant",
                        agent_context=agent_context,
                        hook=previous_assistant_hook,
                        original_text=previous_assistant_raw,
                        codex_event="PreviousAssistantBackfill",
                    )
                    if backfill_result:
                        trace.setdefault("fast_async_backfill", {})["previous_assistant"] = backfill_result
                if not backfill_result:
                    backfill_result = trace_tool_call(
                        server,
                        "matrixark_ingest",
                        hook_async_message_ingest_args(
                            common,
                            args,
                            event="PreviousAssistantBackfill",
                            role="assistant",
                            text=previous_assistant,
                            original_text=previous_assistant_raw,
                            metadata=codex_hook_metadata(
                                source="codex_hook_rollout_backfill",
                                event="PreviousAssistantBackfill",
                                agent_context=agent_context,
                                session_id_source=session_id_source,
                                backfill_reason="stop_hook_runs_before_rollout_final_answer_is_visible",
                            ),
                            agent_hook=previous_assistant_hook,
                        ),
                        trace,
                    )
                assistant_warning = timeout_warning(backfill_result)
                if assistant_warning:
                    backfill_warnings.append(assistant_warning)
                spawn_idle_commit_worker_child(
                    args,
                    ingest=backfill_result,
                    session_id_source=session_id_source,
                )
            if backfill_warnings:
                trace["backfill_warnings"] = backfill_warnings
                hook_warning = backfill_warnings[0]
        if raw_uri and is_resource_event(args.event):
            kind = "skill" if resource_type == "skill" or Path(raw_uri).name.lower() == "skill.md" else "resource"
            ingest_args = {
                **common,
                "kind": kind,
                "messages": [{"role": "user", "content": text or f"{kind} added: {raw_uri}"}],
                "raw_uri": raw_uri,
                "resource_type": resource_type or kind,
                "metadata": codex_hook_metadata(
                    source="codex_hook",
                    event=args.event,
                    agent_context=agent_context,
                    session_id_source=session_id_source,
                    payload=payload,
                    compacted_session_summary=False,
                    raw_uri=raw_uri,
                    resource_type=resource_type or kind,
                ),
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
                    original_text=original_hook_text,
                )
                if not ingest:
                    trace.setdefault("fast_async_ingest", {})["fallback_reason"] = "direct_write_queue_unavailable"
            if ingest:
                hook_warning = timeout_warning(ingest)
            if not ingest and not hook_warning:
                ingest_args = hook_async_message_ingest_args(
                    common,
                    args,
                    event=args.event,
                    role=role_for_event(args.event),
                    text=text,
                    original_text=original_hook_text,
                    metadata=codex_hook_metadata(
                        source="codex_hook",
                        event=args.event,
                        agent_context=agent_context,
                        session_id_source=session_id_source,
                        payload=payload,
                        compacted_session_summary=args.event == "PostCompact",
                    ),
                    agent_hook=main_hook,
                )
                ingest = trace_tool_call(server, "matrixark_ingest", ingest_args, trace)
                hook_warning = timeout_warning(ingest)

        idle_commit_worker: Json = {}
        if ingest and not hook_warning:
            worker_result = spawn_idle_commit_worker_child(
                args,
                ingest=ingest,
                session_id_source=session_id_source,
            )
            if isinstance(worker_result, dict) and worker_result.get("status") != "skipped":
                idle_commit_worker = worker_result

        commit = {}
        fast_async_boundary_commit = fast_async_boundary_commit_from_ingest(ingest)
        if fast_async_boundary_commit:
            commit = fast_async_boundary_commit
        if should_run_session_commit_after_ingest(args.event, hook_warning):
            if fast_async_boundary_commit:
                trace.setdefault("fast_async_ingest", {})["boundary_commit_reused"] = True
            else:
                commit_reason = commit_reason_for_event(args.event)
                commit = trace_tool_call(
                    server,
                    "matrixark_session_commit",
                    {
                        **common,
                        "threshold_messages": args.session_commit_threshold,
                        "force": commit_reason != "idle_timeout",
                        "commit_reason": commit_reason,
                        **hook_session_commit_extraction_options(args),
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

        if idle_commit_worker:
            trace.setdefault("idle_commit_worker", idle_commit_worker)

        retrieve = {}
        query = args.query or text[:500]
        if (args.event == "UserPromptSubmit" or args.query) and not hook_warning and not HOOK_COMPACT_HOT_PREFIX_ONLY:
            question_type = codex_retrieve_question_type(query)
            retrieve = trace_tool_call(
                server,
                "matrixark_retrieve",
                {
                    **common,
                    "query": query,
                    "question_type": question_type,
                    "max_context_tokens": args.max_context_tokens,
                    "session_buffer_threshold": args.session_commit_threshold,
                    "idle_commit_timeout_ms": args.idle_commit_timeout_ms,
                    "storage_options": hook_storage_options(),
                    **codex_retrieve_audit_options(query),
                    **(
                        {
                            "pre_retrieval_summary_refresh": True,
                            "pre_retrieval_summary_refresh_limit": HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT,
                            "pre_retrieval_summary_refresh_skip_dirty_reasons": ["new_event"],
                        }
                        if HOOK_PRE_RETRIEVAL_SUMMARY_REFRESH and args.event == "UserPromptSubmit"
                        else {}
                    ),
                    "ranking": codex_retrieve_ranking_options(),
                    "cross_session": codex_retrieve_cross_session_options(query),
                    **hook_session_commit_extraction_options(args),
                    "metadata": {
                        "retrieval_source": "codex_hook_retrieve",
                        "codex_event": args.event,
                        "hook_type": hook_type_for_event(args.event),
                        "question_type": question_type,
                        "codex_session_id_source": session_id_source,
                        "session_id_source": session_id_source,
                        "lifecycle_stage": "before_llm_retrieve" if args.event == "UserPromptSubmit" else "explicit_query_retrieve",
                    },
                    **({"local_context": agent_context.get("local_context", [])} if agent_context.get("local_context") else {}),
                    # Flag synthetic/debug probes so retrieve serves them from the remote
                    # TemporalStore pack only; real prompts keep local + remote.
                    "synthetic": is_synthetic_hook_text(query),
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
        if idle_commit_worker:
            output["idle_commit_worker"] = idle_commit_worker
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
