#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Local MatrixArk adapter and in-memory serving backend."""

from __future__ import annotations

from contextlib import contextmanager
import queue as thread_queue
from typing import Any

try:
    from tools.matrixark_mcp_core import *
    from tools.matrixark_mcp_core import _mcp_debug_log  # import * skips underscore names
    from tools.matrixark_mcp_core import compact_context_pack_for_serving_flat as compact_context_pack_for_serving
    from tools.matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        context_debug_records_enabled,
        materialize_serving_record_batch,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *
    from matrixark_mcp_core import _mcp_debug_log  # import * skips underscore names
    from matrixark_mcp_core import compact_context_pack_for_serving_flat as compact_context_pack_for_serving
    from matrixark_mcp_serving_records import (
        compact_latest_context_state_records,
        context_debug_records_enabled,
        materialize_serving_record_batch,
    )

try:
    from tools.matrixark_mcp_metrics import MatrixArkServiceMetrics
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_metrics import MatrixArkServiceMetrics

try:
    from tools.matrixark_mcp_session_policy import auto_batch_extract_enabled, session_boundary_commit_requested
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_session_policy import auto_batch_extract_enabled, session_boundary_commit_requested

try:
    from tools.matrixark_mcp_retrieve_pack_builder import (
        dropped_ref_layer_budget,
        memory_layer_pressure_summary,
        selected_ref_layer_budget,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pack_builder import (
        dropped_ref_layer_budget,
        memory_layer_pressure_summary,
        selected_ref_layer_budget,
    )

try:
    from tools.matrixark_mcp_summary_runtime import async_summary_progress_records
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summary_runtime import async_summary_progress_records

try:
    from tools.matrixark_mcp_summary_dirty import pending_dirty_node_records
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_summary_dirty import pending_dirty_node_records

try:
    from tools.matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness, latest_async_pipeline_rows
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_async_readiness import async_pipeline_retrieval_readiness, latest_async_pipeline_rows

try:
    from tools.matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_request import pre_retrieval_idle_commit_flush

try:
    from tools.matrixark_mcp_retrieve_pre_refresh import (
        auto_extraction_phase_budget_tokens as shared_auto_extraction_phase_budget_tokens,
        auto_memory_layer_budget_tokens as shared_auto_memory_layer_budget_tokens,
        auto_memory_selection_policy_budget_tokens as shared_auto_memory_selection_policy_budget_tokens,
        pre_retrieval_summary_refresh_memory_layer_budget_tokens as shared_pre_retrieval_summary_refresh_memory_layer_budget_tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_pre_refresh import (
        auto_extraction_phase_budget_tokens as shared_auto_extraction_phase_budget_tokens,
        auto_memory_layer_budget_tokens as shared_auto_memory_layer_budget_tokens,
        auto_memory_selection_policy_budget_tokens as shared_auto_memory_selection_policy_budget_tokens,
        pre_retrieval_summary_refresh_memory_layer_budget_tokens as shared_pre_retrieval_summary_refresh_memory_layer_budget_tokens,
    )

RETRIEVAL_HOT_RECORD_TYPES = {
    "context_compression_event",
    "context_embedding",
    "context_entity",
    "context_event",
    "context_index",
    "context_segment",
    "context_summary",
    "matrixark_async_pipeline_task",
    "resource_chunk",
    "resource_manifest",
    "skill_registry_update",
    "skill_section",
}

RESOURCE_IMPORT_IGNORE_DIRS = {".git", "node_modules", "target", "build", "dist", ".venv", "__pycache__"}
LOCAL_READ_CACHE_COPY = os.environ.get("MATRIXARK_LOCAL_READ_CACHE_COPY", "1").strip().lower() not in {"0", "false", "no"}
LOCAL_DURABLE_READ_CACHE_ENABLED = os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS = max(0.0, float(os.environ.get("MATRIXARK_LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS", "0")))
LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION = 1
PRE_RETRIEVAL_SUMMARY_REFRESH = os.environ.get("MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH", "0").strip().lower() in {"1", "true", "yes"}

QUALITY_FIRST_UNDERFILL_DROP_KEYS = {
    "cross_session_budget",
    "cross_session_session_cap",
    "cross_session_candidate_cap",
    "low_score",
    "memory_layer_budget",
    "memory_layer_floor",
    "memory_selection_policy_budget",
    "shared_resource_budget",
    "shared_skill_budget",
    "source_role_budget",
    "stale",
}


def quality_first_underfill_summary(
    *,
    budget_fill_policy: str,
    selected_ref_count: int,
    used_context_tokens: int,
    remote_context_budget_tokens: int,
    dropped_over_budget: Json,
) -> Json:
    if str(budget_fill_policy or "").strip().lower() != "quality_first":
        return {"enabled": False}
    if selected_ref_count <= 0:
        return {"enabled": False}
    unused_tokens = max(0, int(remote_context_budget_tokens or 0) - int(used_context_tokens or 0))
    if unused_tokens <= 0:
        return {"enabled": False}
    dropped_reason_counts: Json = {}
    for key in sorted(QUALITY_FIRST_UNDERFILL_DROP_KEYS):
        try:
            count = int(dropped_over_budget.get(key) or 0)
        except (AttributeError, TypeError, ValueError):
            count = 0
        if count > 0:
            dropped_reason_counts[key] = count
    dropped_ref_count = sum(int(count or 0) for count in dropped_reason_counts.values())
    if dropped_ref_count <= 0:
        return {"enabled": False}
    return {
        "enabled": True,
        "policy": "quality_first",
        "unused_remote_context_tokens": unused_tokens,
        "dropped_ref_count": dropped_ref_count,
        "dropped_reason_counts": dropped_reason_counts,
        "warning": f"quality_first_budget_underfill:unused_tokens={unused_tokens},dropped_refs={dropped_ref_count}",
    }


def retrieval_memory_inventory(records: list[Json], retrieval_scope: Json) -> Json:
    """Summarize memory models available after retrieval scope filtering.

    This is serving-facing, not debug lineage: it helps a client distinguish
    "no profile memory exists" from "profile memory exists but was not selected
    under the current query/budget."
    """

    inventory: Json = {
        "session": {
            "context_events": 0,
            "context_segments": 0,
            "context_entities": 0,
            "context_embeddings": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0,
        },
        "profile": {
            "context_entities": 0,
            "context_embeddings": 0,
            "context_indexes": 0,
            "context_summaries": 0,
            "summary_dirty_markers": 0,
        },
        "shared": {
            "resource_chunks": 0,
            "resource_manifests": 0,
            "skill_sections": 0,
            "skill_manifests": 0,
            "context_entities": 0,
            "context_embeddings": 0,
            "context_indexes": 0,
        },
        "available_layers": [],
        "query_scope": {
            "session_scope": session_scope_mode(retrieval_scope),
            "has_session_id": bool(str(retrieval_scope.get("session_id") or "").strip()),
            "has_user_id": bool(str(retrieval_scope.get("user_id") or "").strip()),
            "has_tenant_id": bool(str(retrieval_scope.get("tenant_id") or "").strip()),
        },
    }

    def count(layer: str, field: str, amount: int = 1) -> None:
        bucket = inventory.setdefault(layer, {})
        bucket[field] = int(bucket.get(field) or 0) + amount

    for record in records:
        if not isinstance(record, dict):
            continue
        record_type = str(record.get("record_type") or "")
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        memory_scope = str(record.get("memory_scope") or metadata.get("memory_scope") or "").strip().lower()
        session_continuity = str(
            record.get("session_continuity") or metadata.get("session_continuity") or ""
        ).strip().lower()
        data_model = str(record.get("data_model") or metadata.get("data_model") or "").strip().lower()
        access_scope = candidate_access_scope(record)
        sharing_scope = str(access_scope.get("sharing_scope") or record.get("sharing_scope") or "").strip().lower()
        is_shared = (
            sharing_scope in {"tenant_shared", "global_shared"}
            or record_type in {"resource_chunk", "resource_manifest", "skill_section", "skill_manifest", "skill_registry_update"}
            or data_model in {"resource_chunk", "skill_section"}
        )
        is_profile = (
            memory_scope in {"user_profile", "profile", "cross_session_profile"}
            or data_model == "context_profile_entity"
            or (
                record_type in {"context_entity", "context_embedding", "context_summary", "context_summary_dirty"}
                and session_continuity == "cross_session"
            )
        )
        is_session = memory_scope in {"session", "session_memory"} or session_continuity == "same_session"

        if is_shared:
            if record_type == "resource_chunk":
                count("shared", "resource_chunks")
            elif record_type == "resource_manifest":
                count("shared", "resource_manifests")
            elif record_type == "skill_section":
                count("shared", "skill_sections")
            elif record_type in {"skill_manifest", "skill_registry_update"}:
                count("shared", "skill_manifests")
            elif record_type == "context_entity":
                count("shared", "context_entities")
            elif record_type == "context_embedding":
                count("shared", "context_embeddings")
            elif record_type == "context_index":
                count("shared", "context_indexes")
            continue

        if is_profile:
            if record_type == "context_entity":
                count("profile", "context_entities")
            elif record_type == "context_embedding":
                count("profile", "context_embeddings")
            elif record_type == "context_index":
                count("profile", "context_indexes")
            elif record_type == "context_summary":
                count("profile", "context_summaries")
            elif record_type == "context_summary_dirty":
                count("profile", "summary_dirty_markers")
            continue

        if is_session or record_type in {"context_event", "context_segment"}:
            if record_type == "context_event":
                count("session", "context_events")
            elif record_type == "context_segment":
                count("session", "context_segments")
            elif record_type == "context_entity":
                count("session", "context_entities")
            elif record_type == "context_embedding":
                count("session", "context_embeddings")
            elif record_type == "context_index":
                count("session", "context_indexes")
            elif record_type == "context_summary":
                count("session", "context_summaries")
            elif record_type == "context_summary_dirty":
                count("session", "summary_dirty_markers")

    availability = {
        "session": any(int(value or 0) > 0 for value in inventory["session"].values()),
        "profile": any(int(value or 0) > 0 for value in inventory["profile"].values()),
        "shared": any(int(value or 0) > 0 for value in inventory["shared"].values()),
    }
    inventory["available_layers"] = [layer for layer, available in availability.items() if available]
    inventory["has_session_memory"] = availability["session"]
    inventory["has_profile_memory"] = availability["profile"]
    inventory["has_shared_memory"] = availability["shared"]
    inventory["profile_records_available_but_not_selected"] = False
    return inventory


def positive_int_value(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return max(1, int(default))


def positive_int_env(name: str, default: int) -> int:
    return positive_int_value(os.environ.get(name, str(default)), default)


def bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT = positive_int_env("MATRIXARK_PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT", 2)
LOCAL_JSONL_ENABLED = bool_env("MATRIXARK_LOCAL_JSONL_ENABLED", True)
LOCAL_JSONL_INCLUDE_BULKY_FIELDS = bool_env("MATRIXARK_LOCAL_JSONL_INCLUDE_BULKY_FIELDS", False)
LOCAL_JSONL_MAX_BYTES = positive_int_env("MATRIXARK_LOCAL_JSONL_MAX_BYTES", 64 * 1024 * 1024)
LOCAL_JSONL_RETENTION_COUNT = positive_int_env("MATRIXARK_LOCAL_JSONL_RETENTION_COUNT", 4)
LOCAL_JSONL_RETENTION_AGE_MS = positive_int_env("MATRIXARK_LOCAL_JSONL_RETENTION_AGE_MS", 7 * 24 * 60 * 60 * 1000)
LOCAL_JSONL_BULKY_FIELDS = {
    "agent_debug",
    "debug",
    "debug_payload",
    "full_tool_output",
    "internal_extraction",
    "raw",
    "raw_hook_payload",
    "raw_payload",
    "raw_request",
    "raw_response",
    "replay_payload",
    "tool_payload",
    "tool_result",
    "tool_stdout",
    "tool_stderr",
    "transcript",
}
PROFILE_PROMOTION_POLICY_ALWAYS = "always_when_profile_scope_available"
PROFILE_PROMOTION_SCOPE_MISSING_BLOCKER = "profile_scope_missing"

_LOCAL_READ_CACHE_LOCK = threading.RLock()
_LOCAL_READ_CACHE: dict[str, tuple[int, int, list[Json]]] = {}


def profile_promotion_decision(profile_node_hash: int) -> Json:
    scope_available = bool(profile_node_hash)
    return {
        "policy": PROFILE_PROMOTION_POLICY_ALWAYS,
        "importance_gate": False,
        "scope_available": scope_available,
        "blocker": "" if scope_available else PROFILE_PROMOTION_SCOPE_MISSING_BLOCKER,
    }


def should_promote_session_entity_to_profile(entity: Json) -> bool:
    return bool(entity)


def compact_context_embedding_record(record: Json) -> Json:
    return compact_hot_context_embedding_record(record)


def _ref_list_value(item: Json, field: str) -> list[Any]:
    values = item.get(field)
    if isinstance(values, list):
        return values
    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        values = metadata.get(field)
    return values if isinstance(values, list) else []


def _metadata_value(item: Json, field: str) -> Any:
    value = item.get(field)
    if value not in (None, "", [], {}):
        return value
    metadata = item.get("metadata")
    if isinstance(metadata, dict):
        return metadata.get(field)
    return None


def _source_bucket_names(item: Json, list_field: str, count_field: str, *, normalize_roles: bool = False) -> list[str]:
    values = _metadata_value(item, list_field)
    names: set[str] = set()
    if isinstance(values, list):
        for value in values:
            name = normalize_message_role(value) if normalize_roles else str(value or "").strip()
            if name:
                names.add(name)
    counts = _metadata_value(item, count_field)
    if isinstance(counts, dict):
        for value, count in counts.items():
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                continue
            if amount <= 0:
                continue
            name = normalize_message_role(value) if normalize_roles else str(value or "").strip()
            if name:
                names.add(name)
    return sorted(names)


def _selected_ref_tokens(item: Json) -> int:
    try:
        return max(1, int(item.get("token_estimate") or 0))
    except (TypeError, ValueError):
        return max(1, token_count(str(item.get("text") or "")))


def _refresh_selected_counter_policy(
    *,
    selected: list[Json],
    dropped_over_budget: Json,
    policy_field: str,
    token_field: str,
    count_field: str,
    bucket_names,
) -> None:
    policy = dropped_over_budget.get(policy_field)
    if not isinstance(policy, dict):
        return
    budget_tokens = policy.get("budget_tokens")
    if not isinstance(budget_tokens, dict) or not budget_tokens:
        return
    selected_tokens = {str(name): 0 for name in budget_tokens.keys()}
    selected_counts = {str(name): 0 for name in budget_tokens.keys()}
    for item in selected:
        ref_tokens = _selected_ref_tokens(item)
        for name in bucket_names(item):
            if name in selected_tokens:
                selected_tokens[name] += ref_tokens
                selected_counts[name] += 1
    policy[token_field] = {key: value for key, value in selected_tokens.items() if value > 0 or key in budget_tokens}
    policy[count_field] = {key: value for key, value in selected_counts.items() if value > 0}
    policy["selected_counter_source"] = "final_context_pack_selection_after_profile_pending_dedupe"


def refresh_final_selected_budget_policies(selected: list[Json], dropped_over_budget: Json) -> None:
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="source_role_budget_policy",
        token_field="selected_tokens_by_role",
        count_field="selected_ref_count_by_role",
        bucket_names=lambda item: _source_bucket_names(
            item,
            "budget_source_roles" if _metadata_value(item, "budget_source_roles") else "source_roles",
            "budget_source_role_counts" if _metadata_value(item, "budget_source_role_counts") else "source_role_counts",
            normalize_roles=True,
        ),
    )
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="memory_selection_policy_budget_policy",
        token_field="selected_tokens_by_policy",
        count_field="selected_ref_count_by_policy",
        bucket_names=lambda item: _source_bucket_names(
            item,
            "source_memory_selection_policies",
            "source_memory_selection_policy_counts",
        ),
    )
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="memory_layer_budget_policy",
        token_field="selected_tokens_by_layer",
        count_field="selected_ref_count_by_layer",
        bucket_names=lambda item: [candidate_memory_layer_name(item)],
    )
    _refresh_selected_counter_policy(
        selected=selected,
        dropped_over_budget=dropped_over_budget,
        policy_field="extraction_phase_budget_policy",
        token_field="selected_tokens_by_phase",
        count_field="selected_ref_count_by_phase",
        bucket_names=lambda item: [
            str(_metadata_value(item, "extraction_phase") or "").strip()
            or "unknown"
        ],
    )


def suppress_extracted_represented_pending_events(selected: list[Json], dropped_over_budget: Json) -> tuple[list[Json], int]:
    extracted_selected_event_ids: set[int] = set()
    for item in selected:
        if is_pending_async_candidate(item):
            continue
        for field in [
            "source_event_ids",
            "extraction_context_event_ids",
            "source_ref_hashes",
        ]:
            for value in _ref_list_value(item, field):
                try:
                    event_id = int(value or 0)
                except (TypeError, ValueError):
                    event_id = 0
                if event_id:
                    extracted_selected_event_ids.add(event_id)
        if str(item.get("ref_type") or "") == "event":
            try:
                event_id = int(item.get("ref_hash") or 0)
            except (TypeError, ValueError):
                event_id = 0
            if event_id:
                extracted_selected_event_ids.add(event_id)
    if not extracted_selected_event_ids:
        return selected, 0
    extracted_preferred_selected: list[Json] = []
    removed_tokens = 0
    removed_pending_count = 0
    for item in selected:
        try:
            metadata = item.get("metadata")
            metadata_ref_hash = metadata.get("ref_hash") if isinstance(metadata, dict) else 0
            pending_event_id = int(item.get("ref_hash") or metadata_ref_hash or 0)
        except (TypeError, ValueError):
            pending_event_id = 0
        if (
            is_pending_async_candidate(item)
            and pending_event_id
            and pending_event_id in extracted_selected_event_ids
        ):
            removed_pending_count += 1
            removed_tokens += int(item.get("token_estimate") or max(1, token_count(str(item.get("text") or ""))))
            continue
        extracted_preferred_selected.append(item)
    if removed_pending_count and extracted_preferred_selected:
        dropped_over_budget["pending_async_event_superseded_by_extracted_refs"] = (
            int(dropped_over_budget.get("pending_async_event_superseded_by_extracted_refs") or 0)
            + removed_pending_count
        )
        return extracted_preferred_selected, removed_tokens
    return selected, 0


def suppress_overlapping_profile_current_entities(selected: list[Json], dropped_over_budget: Json) -> tuple[list[Json], int]:
    kept: list[Json] = []
    removed_tokens = 0
    profile_text_tokens_by_type: dict[str, list[set[str]]] = {}
    for item in selected:
        entity_type = str(item.get("entity_type") or "").strip().lower()
        is_profile_current_entity = (
            item.get("ref_type") == "entity"
            and str(item.get("memory_scope") or "").strip().lower() == "user_profile"
            and str(item.get("session_continuity") or "").strip().lower() == "cross_session"
            and bool(item.get("profile_current_state_representative"))
            and entity_type
        )
        if not is_profile_current_entity:
            kept.append(item)
            continue
        item_tokens = {token for token in tokens(str(item.get("text") or "")) if len(token) > 2}
        overlaps_existing = False
        for prior_tokens in profile_text_tokens_by_type.get(entity_type, []):
            if not item_tokens or not prior_tokens:
                continue
            intersection = len(item_tokens.intersection(prior_tokens))
            smaller = max(1, min(len(item_tokens), len(prior_tokens)))
            if intersection / smaller >= 0.60:
                overlaps_existing = True
                break
        if overlaps_existing:
            item_token_count = max(1, token_count(str(item.get("text") or "")))
            removed_tokens += item_token_count
            dropped_over_budget.setdefault("profile_current_entity_overlap_suppressed", 0)
            dropped_over_budget["profile_current_entity_overlap_suppressed"] += 1
            record_dropped_candidate(
                dropped_over_budget,
                item,
                reason="duplicate",
                token_estimate=item_token_count,
            )
            continue
        kept.append(item)
        profile_text_tokens_by_type.setdefault(entity_type, []).append(item_tokens)
    return kept, removed_tokens


def suppress_profile_shadowed_session_entities(selected: list[Json], dropped_over_budget: Json) -> tuple[list[Json], int]:
    profile_entity_source_hashes: set[Any] = set()
    profile_entity_identity_keys: set[tuple[str, str]] = set()
    for item in selected:
        if (
            item.get("ref_type") == "entity"
            and item.get("memory_scope") == "user_profile"
            and item.get("session_continuity") == "cross_session"
        ):
            entity_type = str(item.get("entity_type") or "").strip().lower()
            if is_codex_outcome_entity_type(entity_type) or str(item.get("profile_memory_kind") or "").strip().lower() == "codex_outcome":
                continue
            profile_entity_source_hashes.update(_ref_list_value(item, "source_entity_hashes"))
            entity_name = str(item.get("entity_name") or "").strip().lower()
            if entity_type and entity_name:
                profile_entity_identity_keys.add((entity_type, entity_name))
    if not profile_entity_source_hashes and not profile_entity_identity_keys:
        return selected, 0

    deduped_selected: list[Json] = []
    removed_tokens = 0
    removed_count = 0
    for item in selected:
        if (
            item.get("ref_type") == "entity"
            and item.get("memory_scope") == "session"
            and item.get("session_continuity") == "same_session"
        ):
            item_entity_type = str(item.get("entity_type") or "").strip().lower()
            if is_codex_outcome_entity_type(item_entity_type) or str(item.get("profile_memory_kind") or "").strip().lower() == "codex_outcome":
                deduped_selected.append(item)
                continue
            item_key = (
                item_entity_type,
                str(item.get("entity_name") or "").strip().lower(),
            )
            represented_by_profile = item.get("ref_hash") in profile_entity_source_hashes or (
                bool(item_key[0] and item_key[1]) and item_key in profile_entity_identity_keys
            )
            if represented_by_profile:
                token_estimate = int(item.get("token_estimate") or max(1, token_count(str(item.get("text") or ""))))
                removed_tokens += token_estimate
                removed_count += 1
                record_dropped_candidate(
                    dropped_over_budget,
                    {
                        **item,
                        "profile_shadowed_reason": "selected_profile_entity_supersedes_session_entity",
                    },
                    reason="profile_entity_shadowed_session_entity",
                    token_estimate=token_estimate,
                )
                continue
        deduped_selected.append(item)
    if not removed_count or not deduped_selected:
        return selected, 0
    dropped_over_budget["profile_entity_shadowed_session_entities"] = (
        int(dropped_over_budget.get("profile_entity_shadowed_session_entities") or 0) + removed_count
    )
    return deduped_selected, removed_tokens


def codex_session_identity_policy(session_id_source: str) -> Json:
    source = str(session_id_source or "").strip()
    strong_sources = {"explicit", "payload_field", "payload_path_hash"}
    fallback_sources = {"state_file", "state_file_created", "workspace_hash"}
    strong = source in strong_sources or source.startswith(("payload.", "env."))
    fallback = source in fallback_sources
    return {
        "session_id_source": source,
        "strong_session_identity": strong,
        "fallback_session_identity": fallback,
        "risk": "workspace_fallback_may_merge_multiple_codex_tasks" if fallback else "",
    }


AUTO_BUDGET_QUERY_TYPES = {
    "current_state",
    "latest",
    "profile_memory",
    "multi_hop",
    "date",
    "broad_exploration",
    "evidence",
    "benchmark_quality",
}

FEATURE_MEMORY_BUDGET_QUERY_RE = re.compile(
    r"\b(?:openviking|vikingmem|mem0|feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionalit(?:y|ies)|algorithms?|memory feature|session memory|profile memory|cross[- ]session memory|long[- ]term memory|threshold|idle batch|batch extraction)\b"
)


def _explicit_cross_session_requested(args: Json, ranking: Json) -> bool:
    raw = args.get("cross_session", ranking.get("cross_session"))
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, dict):
        return bool(raw.get("enabled"))
    return False


def feature_profile_memory_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type == "profile_memory":
        return True
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        PROFILE_MEMORY_QUERY_RE.search(lower)
        or PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(lower)
        or FEATURE_MEMORY_BUDGET_QUERY_RE.search(lower)
        or feature_scope_excludes_outcome_evidence(query)
    )


def effective_retrieval_question_type(query: str, requested_question_type: Any = "") -> str:
    question_type = str(requested_question_type or infer_query_type(query)).strip().lower()
    if question_type in {"", "fact"} and PROFILE_MEMORY_STANDING_RULE_QUERY_RE.search(query.lower()):
        return "profile_memory"
    return question_type or "fact"


def _default_memory_budget_mode(args: Json, ranking: Json, *, field: str, question_type: str) -> str:
    mode = str(args.get(field) or ranking.get(field) or "").strip().lower()
    if mode:
        return mode
    normalized_question_type = str(question_type or "fact").strip().lower()
    if (
        normalized_question_type in AUTO_BUDGET_QUERY_TYPES
        or feature_profile_memory_budget_query(args, ranking, question_type=question_type)
        or _explicit_cross_session_requested(args, ranking)
    ):
        return "auto"
    return ""


def codex_outcome_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type not in {"evidence", "current_state", "latest", "benchmark_quality", "profile_memory", "multi_hop", "date"}:
        return False
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(CODEX_OUTCOME_QUERY_RE.search(lower) or re.search(
        r"\b(?:assistant decision|tool evidence|validation evidence|pushed commit|blocked work|next action|what did codex|what was done)\b",
        lower,
    ))


def codex_user_goal_budget_query(args: Json, ranking: Json, *, question_type: str = "fact") -> bool:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type not in {"profile_memory", "current_state", "latest", "multi_hop", "date"}:
        return False
    query = str(args.get("query") or ranking.get("query") or "").strip()
    if not query:
        return False
    lower = query.lower()
    return bool(
        re.search(
            r"\b(?:what|which|show|list|recall|remember|find)\b.{0,80}\b(?:goal|task|plan|requirement|request|asked|ask|instruction|directive)\b",
            lower,
        )
        or re.search(
            r"\b(?:goal|task|plan|requirement|request|instruction|directive)\b.{0,80}\b(?:codex|implement|fix|add|remove|replace|move|build|work)\b",
            lower,
        )
        or re.search(r"\b(?:what did i ask|what have i asked|user asked|user request|current plan)\b", lower)
    )


def feature_scope_budget_query(args: Json, ranking: Json) -> bool:
    query = str(args.get("query") or ranking.get("query") or ranking.get("question") or "")
    return feature_scope_excludes_outcome_evidence(query)


def auto_source_role_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="source_role_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "source_role_budget_fractions") or optional_object(ranking, "source_role_budget_fractions")
    defaults = {"assistant": 0.45, "tool": 0.35, "user": 0.60}
    normalized_question_type = str(question_type or "fact").strip().lower()
    if codex_outcome_budget_query(args, ranking, question_type=question_type):
        defaults.update({"assistant": 0.55, "tool": 0.55, "user": 0.40})
    elif codex_user_goal_budget_query(args, ranking, question_type=question_type):
        defaults.update({"assistant": 0.35, "tool": 0.25, "user": 0.70})
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update({"assistant": 0.50, "tool": 0.40, "user": 0.50})
    elif normalized_question_type == "profile_memory":
        defaults.update({"assistant": 0.50, "tool": 0.45, "user": 0.50})
    elif normalized_question_type == "evidence":
        defaults.update({"assistant": 0.35, "tool": 0.50, "user": 0.45})
    elif normalized_question_type == "benchmark_quality":
        defaults.update({"assistant": 0.50, "tool": 0.60, "user": 0.30})
    elif normalized_question_type in {"broad_exploration", "multi_hop", "date"}:
        defaults.update({"assistant": 0.45, "tool": 0.45, "user": 0.50})
    if feature_scope_budget_query(args, ranking):
        defaults["tool"] = 0.0
    budgets: Json = {}
    for role, default_fraction in defaults.items():
        raw_fraction = fractions.get(role, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[role] = amount
    return budgets, mode


def memory_layer_budget_question_reason(question_type: str) -> str:
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type == "profile_memory":
        return "profile_memory_queries_prioritize user_profile entities, profile summaries, and cross-session bridges"
    if normalized_question_type in {"current_state", "latest"}:
        return "current_state_or_latest_queries_prioritize_profile_entity and cross-session current state"
    if normalized_question_type in {"multi_hop", "date"}:
        return "multi_hop_or_date_queries_expand cross-session events, segments, summaries, and profile bridges"
    if normalized_question_type == "benchmark_quality":
        return "benchmark_quality_queries_prioritize tool evidence, assistant outcomes, quality metrics, and cross-session/profile summaries"
    if normalized_question_type in {"broad_exploration", "evidence"}:
        return "broad_or_evidence_queries_expand summaries, cross-session evidence, and profile bridges"
    return "normal_queries_keep_profile_and_cross_session_budget compact so same-session context dominates"


def auto_memory_selection_policy_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="memory_selection_policy_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        sibling_mode = str(
            args.get("source_role_budget_mode")
            or ranking.get("source_role_budget_mode")
            or args.get("memory_layer_budget_mode")
            or ranking.get("memory_layer_budget_mode")
            or ""
        ).strip().lower()
        if sibling_mode in {"auto", "balanced", "codex_auto"}:
            mode = sibling_mode
        else:
            return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = (
        optional_object(args, "memory_selection_policy_budget_fractions")
        or optional_object(ranking, "memory_selection_policy_budget_fractions")
    )
    defaults = {
        "selected_user_prompt": 0.45,
        "selected_user_profile_fact": 0.35,
        "selected_assistant_profile_fact": 0.35,
        "selected_assistant_decision_outcome_only": 0.30,
        "selected_tool_evidence_only": 0.30,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    if codex_outcome_budget_query(args, ranking, question_type=question_type):
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.45,
                "selected_assistant_profile_fact": 0.35,
                "selected_assistant_decision_outcome_only": 0.58,
                "selected_tool_evidence_only": 0.55,
                "selected_profile_current_state": 0.55,
            }
        )
    elif codex_user_goal_budget_query(args, ranking, question_type=question_type):
        defaults.update(
            {
                "selected_user_prompt": 0.70,
                "selected_user_profile_fact": 0.55,
                "selected_assistant_profile_fact": 0.45,
                "selected_assistant_decision_outcome_only": 0.30,
                "selected_tool_evidence_only": 0.25,
                "selected_profile_current_state": 0.55,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "selected_user_prompt": 0.40,
                "selected_user_profile_fact": 0.60,
                "selected_assistant_profile_fact": 0.55,
                "selected_assistant_decision_outcome_only": 0.45,
                "selected_tool_evidence_only": 0.30,
                "selected_profile_current_state": 0.50,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.70,
                "selected_assistant_profile_fact": 0.65,
                "selected_assistant_decision_outcome_only": 0.40,
                "selected_tool_evidence_only": 0.30,
                "selected_profile_current_state": 0.65,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "selected_user_prompt": 0.25,
                "selected_user_profile_fact": 0.35,
                "selected_assistant_profile_fact": 0.30,
                "selected_assistant_decision_outcome_only": 0.50,
                "selected_tool_evidence_only": 0.65,
                "selected_profile_current_state": 0.40,
            }
        )
    elif normalized_question_type in {"multi_hop", "date", "broad_exploration", "evidence"}:
        defaults.update(
            {
                "selected_user_prompt": 0.35,
                "selected_user_profile_fact": 0.45,
                "selected_assistant_profile_fact": 0.45,
                "selected_assistant_decision_outcome_only": 0.45,
                "selected_tool_evidence_only": 0.50,
            }
        )
    if feature_scope_budget_query(args, ranking):
        defaults["selected_assistant_decision_outcome_only"] = 0.0
        defaults["selected_tool_evidence_only"] = 0.0
    budgets: Json = {}
    for policy, default_fraction in defaults.items():
        raw_fraction = fractions.get(policy, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[policy] = amount
    return budgets, mode


def codex_outcome_event_segment_layer_fractions(question_type: str, *, outcome_query: bool = False) -> Json:
    normalized_question_type = str(question_type or "fact").strip().lower()
    defaults: Json = {
        "same_session_codex_outcome_event": 0.22,
        "cross_session_codex_outcome_event": 0.20,
        "same_session_codex_outcome_segment": 0.20,
        "cross_session_codex_outcome_segment": 0.18,
    }
    if outcome_query:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.45,
                "cross_session_codex_outcome_event": 0.42,
                "same_session_codex_outcome_segment": 0.38,
                "cross_session_codex_outcome_segment": 0.36,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.35,
                "cross_session_codex_outcome_event": 0.30,
                "same_session_codex_outcome_segment": 0.30,
                "cross_session_codex_outcome_segment": 0.28,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.25,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.22,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.35,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.32,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.42,
                "cross_session_codex_outcome_event": 0.45,
                "same_session_codex_outcome_segment": 0.35,
                "cross_session_codex_outcome_segment": 0.40,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update(
            {
                "same_session_codex_outcome_event": 0.38,
                "cross_session_codex_outcome_event": 0.35,
                "same_session_codex_outcome_segment": 0.34,
                "cross_session_codex_outcome_segment": 0.32,
            }
        )
    return defaults


def auto_memory_layer_budget_tokens(args: Json, ranking: Json, *, remote_budget_tokens: int, question_type: str = "fact") -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="memory_layer_budget_mode",
        question_type=question_type,
    )
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "memory_layer_budget_fractions") or optional_object(ranking, "memory_layer_budget_fractions")
    defaults = {
        "summary": 0.20,
        "profile_summary": 0.30,
        "same_session_summary": 0.20,
        "cross_session_summary": 0.20,
        "compression": 0.25,
        "profile_compression": 0.25,
        "same_session_compression": 0.20,
        "cross_session_compression": 0.20,
        "pending_async_event": 0.20,
        "pending_async_codex_outcome_event": 0.20,
        "pending_async_memory_feature_event": 0.20,
        "same_session_event": 0.45,
        "same_session_memory_feature_event": 0.35,
        "cross_session_memory_feature_event": 0.25,
        "cross_session_event": 0.25,
        "same_session_segment": 0.35,
        "same_session_memory_feature_segment": 0.30,
        "cross_session_memory_feature_segment": 0.25,
        "cross_session_segment": 0.25,
        "same_session_memory_feature_entity": 0.35,
        "profile_entity": 0.40,
        "cross_session_codex_outcome_entity": 0.25,
        "cross_session_memory_feature_entity": 0.25,
        "cross_session_codex_outcome_summary": 0.25,
        "cross_session_codex_outcome_compression": 0.25,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    outcome_query = codex_outcome_budget_query(args, ranking, question_type=question_type)
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    if outcome_query:
        defaults.update(
            {
                "summary": 0.18,
                "profile_summary": 0.35,
                "same_session_summary": 0.18,
                "cross_session_summary": 0.32,
                "compression": 0.25,
                "profile_compression": 0.35,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.32,
                "pending_async_event": 0.20,
                "pending_async_codex_outcome_event": 0.42,
                "same_session_event": 0.35,
                "cross_session_event": 0.38,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.35,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.62,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif feature_profile_query:
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.50,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.45,
                "compression": 0.20,
                "profile_compression": 0.45,
                "same_session_compression": 0.15,
                "cross_session_compression": 0.40,
                "pending_async_event": 0.12,
                "pending_async_codex_outcome_event": 0.10,
                "pending_async_memory_feature_event": 0.55,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.55,
                "cross_session_memory_feature_event": 0.70,
                "cross_session_event": 0.35,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.50,
                "cross_session_memory_feature_segment": 0.68,
                "cross_session_segment": 0.35,
                "profile_entity": 0.65,
                "cross_session_codex_outcome_entity": 0.20,
                "cross_session_memory_feature_entity": 0.75,
                "cross_session_codex_outcome_summary": 0.20,
                "cross_session_codex_outcome_compression": 0.20,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.20,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.15,
                "compression": 0.20,
                "profile_compression": 0.25,
                "same_session_compression": 0.15,
                "cross_session_compression": 0.20,
                "pending_async_event": 0.15,
                "same_session_event": 0.35,
                "cross_session_event": 0.30,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.30,
                "profile_entity": 0.55,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.50,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "profile_memory":
        defaults.update(
            {
                "summary": 0.15,
                "profile_summary": 0.45,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.40,
                "compression": 0.25,
                "profile_compression": 0.40,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.15,
                "pending_async_memory_feature_event": 0.50,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.50,
                "cross_session_memory_feature_event": 0.62,
                "cross_session_event": 0.40,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.48,
                "cross_session_memory_feature_segment": 0.60,
                "cross_session_segment": 0.40,
                "profile_entity": 0.60,
                "cross_session_codex_outcome_entity": 0.30,
                "cross_session_memory_feature_entity": 0.65,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.20,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.25,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.20,
                "same_session_event": 0.40,
                "cross_session_event": 0.35,
                "same_session_segment": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.40,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.20,
                "cross_session_summary": 0.35,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.20,
                "cross_session_compression": 0.35,
                "pending_async_event": 0.20,
                "same_session_event": 0.35,
                "cross_session_event": 0.35,
                "same_session_segment": 0.30,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.58,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "same_session_summary": 0.25,
                "cross_session_summary": 0.30,
                "compression": 0.30,
                "profile_compression": 0.35,
                "same_session_compression": 0.30,
                "cross_session_compression": 0.30,
                "pending_async_event": 0.25,
                "same_session_event": 0.45,
                "cross_session_event": 0.30,
                "same_session_segment": 0.40,
                "cross_session_segment": 0.30,
                "profile_entity": 0.45,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    defaults.update(
        codex_outcome_event_segment_layer_fractions(
            normalized_question_type,
            outcome_query=outcome_query,
        )
    )
    if feature_scope_budget_query(args, ranking):
        for outcome_layer in [
            "same_session_codex_outcome_event",
            "pending_async_codex_outcome_event",
            "cross_session_codex_outcome_event",
            "same_session_codex_outcome_segment",
            "cross_session_codex_outcome_segment",
            "cross_session_codex_outcome_entity",
            "cross_session_codex_outcome_summary",
            "cross_session_codex_outcome_compression",
        ]:
            defaults[outcome_layer] = 0.0
    defaults["cross_session_memory_feature_summary"] = max(
        defaults.get("cross_session_memory_feature_entity", 0.25),
        defaults.get("profile_summary", 0.30),
    )
    defaults["cross_session_memory_feature_compression"] = max(
        defaults.get("cross_session_memory_feature_entity", 0.25),
        defaults.get("profile_compression", 0.25),
    )
    defaults["same_session_memory_feature_summary"] = max(
        defaults.get("same_session_memory_feature_entity", 0.25),
        defaults.get("same_session_summary", 0.20),
    )
    defaults["same_session_memory_feature_compression"] = max(
        defaults.get("same_session_memory_feature_entity", 0.25),
        defaults.get("same_session_compression", 0.20),
    )
    budgets: Json = {}
    for layer, default_fraction in defaults.items():
        raw_fraction = fractions.get(layer, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[layer] = amount
    return budgets, mode


def auto_extraction_phase_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    mode = _default_memory_budget_mode(
        args,
        ranking,
        field="extraction_phase_budget_mode",
        question_type=question_type,
    )
    if not mode:
        sibling_mode = str(
            args.get("source_role_budget_mode")
            or ranking.get("source_role_budget_mode")
            or args.get("memory_layer_budget_mode")
            or ranking.get("memory_layer_budget_mode")
            or args.get("memory_selection_policy_budget_mode")
            or ranking.get("memory_selection_policy_budget_mode")
            or ""
        ).strip().lower()
        if sibling_mode in {"auto", "balanced", "codex_auto"}:
            mode = sibling_mode
    if mode not in {"auto", "balanced", "codex_auto"}:
        return {}, ""
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    if remote_budget <= 0:
        return {}, mode
    fractions = optional_object(args, "extraction_phase_budget_fractions") or optional_object(
        ranking,
        "extraction_phase_budget_fractions",
    )
    defaults = {
        "pending_async": 0.12,
        "provisional": 0.25,
        "final": 0.70,
    }
    normalized_question_type = str(question_type or "fact").strip().lower()
    if normalized_question_type in {"current_state", "latest"}:
        defaults.update({"pending_async": 0.12, "provisional": 0.25, "final": 0.75})
    elif normalized_question_type == "profile_memory":
        defaults.update({"pending_async": 0.10, "provisional": 0.20, "final": 0.80})
    elif normalized_question_type in {"multi_hop", "date"}:
        defaults.update({"pending_async": 0.15, "provisional": 0.30, "final": 0.70})
    elif normalized_question_type == "benchmark_quality":
        defaults.update({"pending_async": 0.12, "provisional": 0.25, "final": 0.75})
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        defaults.update({"pending_async": 0.15, "provisional": 0.35, "final": 0.70})
    budgets: Json = {}
    for phase, default_fraction in defaults.items():
        raw_fraction = fractions.get(phase, default_fraction) if isinstance(fractions, dict) else default_fraction
        try:
            fraction = max(0.0, min(1.0, float(raw_fraction)))
        except (TypeError, ValueError):
            fraction = default_fraction
        if fraction <= 0.0:
            continue
        amount = max(1, int(remote_budget * fraction))
        if amount:
            budgets[phase] = amount
    return budgets, mode


def pre_retrieval_summary_refresh_memory_layer_budget_tokens(
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
    outcome_query: bool = False,
    args: Json | None = None,
    ranking: Json | None = None,
) -> tuple[Json, str]:
    try:
        remote_budget = max(0, int(remote_budget_tokens or 0))
    except (TypeError, ValueError):
        remote_budget = 0
    normalized_question_type = str(question_type or "fact").strip().lower()
    mode = "pre_retrieval_summary_refresh_balanced"
    if normalized_question_type in {"current_state", "latest"}:
        mode = "pre_retrieval_summary_refresh_current_state"
    elif normalized_question_type == "profile_memory":
        mode = "pre_retrieval_summary_refresh_profile_memory"
    elif normalized_question_type in {"multi_hop", "date"}:
        mode = "pre_retrieval_summary_refresh_multi_hop"
    elif normalized_question_type == "benchmark_quality":
        mode = "pre_retrieval_summary_refresh_benchmark_quality"
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        mode = "pre_retrieval_summary_refresh_evidence"
    if remote_budget <= 0:
        return {}, mode
    args = args if isinstance(args, dict) else {}
    ranking = ranking if isinstance(ranking, dict) else {}
    feature_profile_query = feature_profile_memory_budget_query(args, ranking, question_type=question_type)
    fractions = {
        "summary": 0.15,
        "profile_summary": 0.30,
        "same_session_summary": 0.20,
        "cross_session_summary": 0.25,
        "compression": 0.20,
        "profile_compression": 0.25,
        "same_session_compression": 0.20,
        "cross_session_compression": 0.25,
        "pending_async_event": 0.20,
        "pending_async_codex_outcome_event": 0.20,
        "pending_async_memory_feature_event": 0.20,
        "same_session_event": 0.45,
        "same_session_memory_feature_event": 0.35,
        "cross_session_memory_feature_event": 0.25,
        "cross_session_event": 0.25,
        "same_session_segment": 0.30,
        "same_session_memory_feature_segment": 0.30,
        "cross_session_memory_feature_segment": 0.25,
        "cross_session_segment": 0.25,
        "same_session_memory_feature_entity": 0.35,
        "profile_entity": 0.45,
        "cross_session_codex_outcome_entity": 0.25,
        "cross_session_memory_feature_entity": 0.25,
        "cross_session_codex_outcome_summary": 0.25,
        "cross_session_codex_outcome_compression": 0.25,
    }
    if feature_profile_query:
        fractions.update(
            {
                "summary": 0.15,
                "profile_summary": 0.50,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.45,
                "profile_compression": 0.45,
                "cross_session_compression": 0.40,
                "pending_async_codex_outcome_event": 0.10,
                "pending_async_memory_feature_event": 0.55,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.55,
                "cross_session_memory_feature_event": 0.70,
                "cross_session_event": 0.35,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.50,
                "cross_session_memory_feature_segment": 0.68,
                "cross_session_segment": 0.35,
                "profile_entity": 0.65,
                "cross_session_codex_outcome_entity": 0.20,
                "cross_session_memory_feature_entity": 0.75,
                "cross_session_codex_outcome_summary": 0.20,
                "cross_session_codex_outcome_compression": 0.20,
            }
        )
    elif normalized_question_type in {"current_state", "latest"}:
        fractions.update(
            {
                "profile_summary": 0.35,
                "cross_session_summary": 0.30,
                "profile_compression": 0.35,
                "cross_session_compression": 0.30,
                "cross_session_event": 0.30,
                "cross_session_segment": 0.30,
                "profile_entity": 0.55,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.50,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "profile_memory":
        fractions.update(
            {
                "summary": 0.15,
                "profile_summary": 0.45,
                "same_session_summary": 0.15,
                "cross_session_summary": 0.40,
                "profile_compression": 0.40,
                "cross_session_compression": 0.35,
                "pending_async_codex_outcome_event": 0.15,
                "pending_async_memory_feature_event": 0.50,
                "same_session_event": 0.25,
                "same_session_memory_feature_event": 0.50,
                "cross_session_memory_feature_event": 0.62,
                "cross_session_event": 0.40,
                "same_session_segment": 0.25,
                "same_session_memory_feature_segment": 0.48,
                "cross_session_memory_feature_segment": 0.60,
                "cross_session_segment": 0.40,
                "profile_entity": 0.60,
                "cross_session_codex_outcome_entity": 0.30,
                "cross_session_memory_feature_entity": 0.65,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type in {"multi_hop", "date"}:
        fractions.update(
            {
                "cross_session_summary": 0.35,
                "cross_session_compression": 0.35,
                "cross_session_event": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.40,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    elif normalized_question_type == "benchmark_quality":
        fractions.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "cross_session_summary": 0.35,
                "profile_compression": 0.35,
                "cross_session_compression": 0.35,
                "cross_session_event": 0.35,
                "cross_session_segment": 0.35,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.58,
                "cross_session_memory_feature_entity": 0.35,
                "cross_session_codex_outcome_summary": 0.45,
                "cross_session_codex_outcome_compression": 0.45,
            }
        )
    elif normalized_question_type in {"broad_exploration", "evidence"}:
        fractions.update(
            {
                "summary": 0.20,
                "profile_summary": 0.35,
                "cross_session_summary": 0.30,
                "profile_compression": 0.30,
                "cross_session_compression": 0.30,
                "same_session_event": 0.35,
                "cross_session_event": 0.30,
                "same_session_segment": 0.35,
                "cross_session_segment": 0.30,
                "profile_entity": 0.50,
                "cross_session_codex_outcome_entity": 0.45,
                "cross_session_memory_feature_entity": 0.45,
                "cross_session_codex_outcome_summary": 0.35,
                "cross_session_codex_outcome_compression": 0.35,
            }
        )
    fractions.update(
        codex_outcome_event_segment_layer_fractions(
            normalized_question_type,
            outcome_query=outcome_query,
        )
    )
    if feature_scope_budget_query(args, ranking):
        for outcome_layer in [
            "same_session_codex_outcome_event",
            "cross_session_codex_outcome_event",
            "same_session_codex_outcome_segment",
            "cross_session_codex_outcome_segment",
            "cross_session_codex_outcome_entity",
            "cross_session_codex_outcome_summary",
            "cross_session_codex_outcome_compression",
        ]:
            fractions[outcome_layer] = 0.0
    fractions["cross_session_memory_feature_summary"] = max(
        fractions.get("cross_session_memory_feature_entity", 0.25),
        fractions.get("profile_summary", 0.30),
    )
    fractions["cross_session_memory_feature_compression"] = max(
        fractions.get("cross_session_memory_feature_entity", 0.25),
        fractions.get("profile_compression", 0.25),
    )
    fractions["same_session_memory_feature_summary"] = max(
        fractions.get("same_session_memory_feature_entity", 0.25),
        fractions.get("same_session_summary", 0.20),
    )
    fractions["same_session_memory_feature_compression"] = max(
        fractions.get("same_session_memory_feature_entity", 0.25),
        fractions.get("same_session_compression", 0.20),
    )
    return {
        layer: max(1, int(remote_budget * fraction))
        for layer, fraction in fractions.items()
        if fraction > 0.0
    }, mode


def auto_memory_selection_policy_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    return shared_auto_memory_selection_policy_budget_tokens(
        args,
        ranking,
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
    )


def auto_memory_layer_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    return shared_auto_memory_layer_budget_tokens(
        args,
        ranking,
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
    )


def auto_extraction_phase_budget_tokens(
    args: Json,
    ranking: Json,
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
) -> tuple[Json, str]:
    return shared_auto_extraction_phase_budget_tokens(
        args,
        ranking,
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
    )


def pre_retrieval_summary_refresh_memory_layer_budget_tokens(
    *,
    remote_budget_tokens: int,
    question_type: str = "fact",
    outcome_query: bool = False,
    args: Json | None = None,
    ranking: Json | None = None,
) -> tuple[Json, str]:
    del outcome_query
    return shared_pre_retrieval_summary_refresh_memory_layer_budget_tokens(
        remote_budget_tokens=remote_budget_tokens,
        question_type=question_type,
        args=args,
        ranking=ranking,
    )


def pre_retrieval_summary_refresh_enabled(args: Json, ranking: Json) -> bool:
    value = (
        args.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in args
        else ranking.get("pre_retrieval_summary_refresh")
        if "pre_retrieval_summary_refresh" in ranking
        else PRE_RETRIEVAL_SUMMARY_REFRESH
    )
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "auto", "bounded"}
    return bool(value)


def pre_retrieval_summary_refresh_explicitly_configured(args: Json, ranking: Json) -> bool:
    return "pre_retrieval_summary_refresh" in args or "pre_retrieval_summary_refresh" in ranking


def pre_retrieval_summary_refresh_limit(args: Json, ranking: Json) -> int:
    raw_limit = (
        args.get("pre_retrieval_summary_refresh_limit")
        or ranking.get("pre_retrieval_summary_refresh_limit")
        or PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT
    )
    try:
        return max(1, int(raw_limit))
    except (TypeError, ValueError):
        return PRE_RETRIEVAL_SUMMARY_REFRESH_LIMIT


def session_event_message_count(records: list[Json]) -> int:
    return sum(len(messages_from_event_record(record)) for record in records)


def session_events_by_message_limit(records: list[Json], limit: int | None) -> list[Json]:
    if limit is None:
        return records
    selected: list[Json] = []
    message_count = 0
    for record in records:
        selected.append(record)
        message_count += max(1, len(messages_from_event_record(record)))
        if message_count >= limit:
            break
    return selected


def deferred_idle_auto_batch_result(
    *,
    idle_commit_result: Json | None,
    pending_event_count: int,
    pending_message_count: int,
    threshold_messages: int,
    idle_commit_timeout_ms: int | None,
) -> Json | None:
    if not isinstance(idle_commit_result, dict):
        return None
    if idle_commit_result.get("status") != "deferred":
        return None
    if str(idle_commit_result.get("commit_reason") or "") != "idle_timeout":
        return None
    if idle_commit_timeout_ms is None or idle_commit_timeout_ms <= 0:
        return None
    return {
        "status": "deferred",
        "trigger_policy": "idle_timeout",
        "commit_reason": "idle_timeout",
        "reason": "session_buffer_idle_deadline_armed",
        "pending_event_count": pending_event_count,
        "pending_message_count": pending_message_count,
        "threshold_messages": threshold_messages,
        "idle_commit_timeout_ms": idle_commit_timeout_ms,
        "idle_elapsed_ms": idle_commit_result.get("idle_elapsed_ms", 0),
        "idle_commit_scheduled": pending_event_count > 0,
        "extraction_phase": "provisional",
        "final_session_boundary": False,
        "trigger_evidence": {
            **(idle_commit_result.get("trigger_evidence") if isinstance(idle_commit_result.get("trigger_evidence"), dict) else {}),
            "pending_event_count": pending_event_count,
            "pending_message_count": pending_message_count,
            "threshold_messages": threshold_messages,
            "threshold_ready": False,
            "idle_timeout_ms": idle_commit_timeout_ms,
            "idle_ready": False,
            "force": False,
            "commit_reason": "idle_timeout",
        },
    }


def idle_commit_scheduled_task_record(
    *,
    event_id_hash: int,
    node_hash: int,
    node_path: list[str],
    scope: Json,
    storage_options: Json | None = None,
    ingestion_time_ms: int,
    idle_commit_timeout_ms: int,
    pending_event_count: int,
    pending_message_count: int,
    threshold_messages: int,
) -> Json:
    deadline_ms = int(ingestion_time_ms or 0) + max(0, int(idle_commit_timeout_ms or 0))
    requested_storage_options = dict(storage_options or {})
    return {
        "record_type": "matrixark_async_pipeline_task",
        "task_hash": stable_hash(f"async_pipeline_idle_commit:{event_id_hash}"),
        "event_id_hash": event_id_hash,
        "node_hash": node_hash,
        "node_path": node_path,
        "scope": scope,
        "status": "idle_commit_scheduled",
        "stages": ["extraction", "summary", "compression", "embedding"],
        "reason": "session_buffer_idle_deadline",
        "trigger_policy": "idle_timeout",
        "auto_batch_extract": True,
        "threshold_messages": threshold_messages,
        "idle_commit_timeout_ms": idle_commit_timeout_ms,
        "idle_commit_deadline_ms": deadline_ms,
        "idle_commit_cutoff_ms": int(ingestion_time_ms or 0),
        "idle_commit_pending_event_count": pending_event_count,
        "idle_commit_pending_message_count": pending_message_count,
        "requested_storage_options": requested_storage_options,
        "storage_options": requested_storage_options,
        "source_extraction_phases": ["provisional"],
        "extraction_phase": "provisional",
        "final_session_boundary": False,
        "created_at_ms": int(ingestion_time_ms or 0),
        "updated_at_ms": int(ingestion_time_ms or 0),
    }


ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in [
        r"\b(?:user|you)\b.{0,96}\b(?:prefer|prefers|preference|likes|wants|needs|asked|requires|required|always|never|avoid|remember)\b",
        r"\b(?:i(?:'ll| will)? remember|remembered|noted|got it|understood)\b.{0,140}\b(?:prefer|preference|want|need|always|never|avoid|profile|memory|workspace|repo|branch|reply|respond|format|style)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will)\b.{0,64}\b(?:remember|keep|use|follow|prefer|avoid|not use|always use|make sure)\b",
        r"\b(?:standing instruction|standing preference|user profile|long[- ]term memor(?:y|ies)|saved preference|persistent instruction)\b",
        r"\b(?:call me|my name is|user(?:'s)? name is|user goes by|pronouns?|address (?:me|the user))\b",
        r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|timezone|time zone|locale)\b.{0,120}\b(?:concise|brief|detailed|bullets?|markdown|language|tone|style|format|timezone|locale)\b",
        r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b.{0,140}\b(?:always|prefer|use|keep|must|should|avoid|never|don't|push|build|deploy)\b",
        r"\b(?:i(?:'ll| will)|codex will|assistant will|going forward|from now on)\b.{0,80}\b(?:use|keep|follow|prefer|avoid|never use|not use|always use|push|build|deploy)\b.{0,140}\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|build|deploy|deployment|rustraft|temporalstore|matrixark)\b",
    ]
]


def assistant_profile_fact_lineage_text(text: Any) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(compact and any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS))


def user_profile_fact_lineage_text(text: Any) -> bool:
    compact = " ".join(str(text or "").split())
    return bool(compact and any(pattern.search(compact) for pattern in ASSISTANT_PROFILE_FACT_LINEAGE_PATTERNS))


def context_source_lineage(envelope: Json, hook: Json | None = None) -> Json:
    metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
    role_counts: Json = {}
    assistant_text_parts: list[str] = []
    user_text_parts: list[str] = []
    for message in envelope.get("messages", []):
        if not isinstance(message, dict):
            continue
        role = normalize_message_role(message.get("role"))
        if role:
            role_counts[role] = int(role_counts.get(role, 0)) + 1
        if role == "assistant":
            assistant_text_parts.append(str(message.get("content") or ""))
        elif role == "user":
            user_text_parts.append(str(message.get("content") or ""))
    roles = set(role_counts)
    metadata_scalar_role = normalize_message_role(metadata.get("source_role"))
    if metadata_scalar_role:
        roles.add(metadata_scalar_role)
        role_counts[metadata_scalar_role] = max(1, int(role_counts.get(metadata_scalar_role, 0)))
    for value in metadata.get("source_roles", []) if isinstance(metadata.get("source_roles"), list) else []:
        role = normalize_message_role(value)
        if role:
            roles.add(role)
            role_counts[role] = max(1, int(role_counts.get(role, 0)))
    if isinstance(metadata.get("source_role_counts"), dict):
        for value, count in metadata["source_role_counts"].items():
            role = normalize_message_role(value)
            if not role:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                roles.add(role)
                role_counts[role] = max(int(role_counts.get(role, 0)), amount)
    hook_types = set()
    hook_type = str(metadata.get("hook_type") or "").strip()
    if hook_type:
        hook_types.add(hook_type)
    for value in metadata.get("source_hook_types", []) if isinstance(metadata.get("source_hook_types"), list) else []:
        if str(value or "").strip():
            hook_types.add(str(value).strip())
    codex_events = set()
    codex_event = str(metadata.get("codex_event") or "").strip()
    if codex_event:
        codex_events.add(codex_event)
    for value in metadata.get("source_codex_events", []) if isinstance(metadata.get("source_codex_events"), list) else []:
        if str(value or "").strip():
            codex_events.add(str(value).strip())
    agent_event = str(metadata.get("agent_event") or "").strip()
    if agent_event:
        codex_events.add(agent_event)
    if isinstance(hook, dict):
        hook_type = str(hook.get("hook_type") or "").strip()
        if hook_type:
            hook_types.add(hook_type)
        trigger = str(hook.get("trigger") or "").strip()
        if trigger:
            codex_events.add(trigger)
    if not hook_types:
        for event in sorted(codex_events):
            legacy_hook_type = legacy_hook_type_from_codex_event(event)
            if legacy_hook_type:
                hook_types.add(legacy_hook_type)
    source_lineage_count = max(1, sum(int(value or 0) for value in role_counts.values()))
    hook_type_counts: Json = {}
    if isinstance(metadata.get("source_hook_type_counts"), dict):
        for value, count in metadata["source_hook_type_counts"].items():
            hook_name = str(value or "").strip()
            if not hook_name:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                hook_types.add(hook_name)
                hook_type_counts[hook_name] = max(int(hook_type_counts.get(hook_name, 0)), amount)
    for hook_name in hook_types:
        hook_type_counts.setdefault(hook_name, source_lineage_count)
    codex_event_counts: Json = {}
    if isinstance(metadata.get("source_codex_event_counts"), dict):
        for value, count in metadata["source_codex_event_counts"].items():
            event_name = str(value or "").strip()
            if not event_name:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                codex_events.add(event_name)
                codex_event_counts[event_name] = max(int(codex_event_counts.get(event_name, 0)), amount)
    for event_name in codex_events:
        codex_event_counts.setdefault(event_name, source_lineage_count)
    memory_selection_policy_counts: Json = {}
    explicit_policy_counts = (
        metadata.get("source_memory_selection_policy_counts")
        if isinstance(metadata.get("source_memory_selection_policy_counts"), dict)
        else {}
    )
    for policy, count in explicit_policy_counts.items():
        policy_name = str(policy or "").strip()
        if not policy_name:
            continue
        try:
            amount = max(0, int(count or 0))
        except (TypeError, ValueError):
            amount = 0
        if amount:
            memory_selection_policy_counts[policy_name] = int(memory_selection_policy_counts.get(policy_name, 0)) + amount
    for policy in metadata.get("source_memory_selection_policies", []) if isinstance(metadata.get("source_memory_selection_policies"), list) else []:
        policy_name = str(policy or "").strip()
        if policy_name and policy_name not in memory_selection_policy_counts:
            memory_selection_policy_counts[policy_name] = 1
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    if isinstance(selection.get("policies"), list):
        for policy in selection.get("policies", []):
            policy_name = str(policy or "").strip()
            if policy_name and policy_name not in memory_selection_policy_counts:
                memory_selection_policy_counts[policy_name] = 1
    selection_policy = str(selection.get("policy") or "").strip()
    if selection_policy and selection_policy not in memory_selection_policy_counts:
        memory_selection_policy_counts[selection_policy] = 1
    selection_lossy_count = 0
    selection_complete_count = 0
    selection_retained_text_ratio_sum = 0.0
    selection_retained_line_ratio_sum = 0.0
    selection_stats_count = 0
    selection_dropped_text_chars = 0
    selection_dropped_line_count = 0
    if selection:
        try:
            selection_dropped_text_chars += max(0, int(selection.get("dropped_text_chars") or 0))
        except (TypeError, ValueError):
            pass
        try:
            selection_dropped_line_count += max(0, int(selection.get("dropped_line_count") or 0))
        except (TypeError, ValueError):
            pass
        try:
            selection_retained_text_ratio_sum += float(selection.get("retained_text_ratio"))
            selection_retained_line_ratio_sum += float(selection.get("retained_line_ratio"))
            selection_stats_count += 1
        except (TypeError, ValueError):
            pass
        if bool(selection.get("selection_lossy")):
            selection_lossy_count += 1
        else:
            selection_complete_count += 1
    assistant_policies: list[str] = []
    assistant_lineage_text = "\n".join(assistant_text_parts) or metadata.get("text") or envelope.get("text")
    assistant_feature_memory_only = feature_scope_excludes_outcome_evidence(assistant_lineage_text)
    if assistant_profile_fact_lineage_text(assistant_lineage_text):
        assistant_policies.append("selected_assistant_profile_fact")
    if assistant_lineage_text and not assistant_feature_memory_only:
        assistant_policies.append("selected_assistant_decision_outcome_only")
    if not assistant_policies:
        assistant_policies.append(
            "selected_assistant_profile_fact"
            if assistant_feature_memory_only
            else "selected_assistant_decision_outcome_only"
        )
    user_lineage_text = "\n".join(user_text_parts) or (metadata.get("text") if "user" in roles else "")
    user_policies = ["selected_user_prompt"]
    if user_profile_fact_lineage_text(user_lineage_text):
        user_policies.append("selected_user_profile_fact")
    inferred_policy_by_role = {
        "assistant": assistant_policies,
        "tool": "selected_tool_evidence_only",
        "user": user_policies,
    }
    for role, count in role_counts.items():
        policies = inferred_policy_by_role.get(role)
        if isinstance(policies, str):
            policies = [policies]
        if not policies:
            continue
        for policy in policies:
            if not policy or policy in memory_selection_policy_counts:
                continue
            memory_selection_policy_counts[policy] = max(1, int(count or 0))
    entity_type = ""
    if "tool" in roles:
        entity_type = "tool_evidence"
    elif "assistant" in roles:
        entity_type = "memory_feature_profile" if assistant_feature_memory_only else "assistant_decision"
    memory_layer_counts: Json = {}
    for layer in metadata.get("source_memory_layers", []) if isinstance(metadata.get("source_memory_layers"), list) else []:
        layer_name = str(layer or "").strip()
        if layer_name:
            memory_layer_counts[layer_name] = int(memory_layer_counts.get(layer_name, 0)) + source_lineage_count
    if isinstance(metadata.get("source_memory_layer_counts"), dict):
        for layer, count in metadata["source_memory_layer_counts"].items():
            layer_name = str(layer or "").strip()
            if not layer_name:
                continue
            try:
                amount = max(0, int(count or 0))
            except (TypeError, ValueError):
                amount = 0
            if amount:
                memory_layer_counts[layer_name] = int(memory_layer_counts.get(layer_name, 0)) + amount
    explicit_memory_layer = str(metadata.get("memory_layer") or "").strip()
    if explicit_memory_layer:
        memory_layer_counts.setdefault(explicit_memory_layer, source_lineage_count)
    inferred_memory_layer = candidate_memory_layer_name(
        {
            "record_type": "context_event",
            "ref_type": "event",
            "memory_scope": "session",
            "session_continuity": "same_session",
            "entity_type": entity_type,
            "event_type": entity_type,
        }
    )
    if inferred_memory_layer:
        memory_layer_counts.setdefault(inferred_memory_layer, source_lineage_count)
    return {
        "memory_scope": "session",
        "session_continuity": "same_session",
        **({"entity_type": entity_type} if entity_type else {}),
        "source_roles": sorted(roles),
        "source_role_counts": {role: int(role_counts.get(role, 0)) for role in sorted(roles) if int(role_counts.get(role, 0)) > 0},
        "source_hook_types": sorted(hook_types),
        "source_hook_type_counts": {name: int(hook_type_counts.get(name, 0)) for name in sorted(hook_types) if int(hook_type_counts.get(name, 0)) > 0},
        "source_codex_events": sorted(codex_events),
        "source_codex_event_counts": {name: int(codex_event_counts.get(name, 0)) for name in sorted(codex_events) if int(codex_event_counts.get(name, 0)) > 0},
        "source_memory_selection_policies": sorted(memory_selection_policy_counts),
        "source_memory_selection_policy_counts": memory_selection_policy_counts,
        "source_memory_layers": sorted(memory_layer_counts),
        "source_memory_layer_counts": memory_layer_counts,
        "source_memory_selection_lossy_count": selection_lossy_count,
        "source_memory_selection_complete_count": selection_complete_count,
        "source_memory_selection_dropped_text_chars": selection_dropped_text_chars,
        "source_memory_selection_dropped_line_count": selection_dropped_line_count,
        "source_memory_selection_retained_text_ratio_avg": round(selection_retained_text_ratio_sum / selection_stats_count, 6) if selection_stats_count else 1.0,
        "source_memory_selection_retained_line_ratio_avg": round(selection_retained_line_ratio_sum / selection_stats_count, 6) if selection_stats_count else 1.0,
    }


def context_event_type_for_message(message: Json, default_event_type: str) -> str:
    role = normalize_message_role(message.get("role")) if isinstance(message, dict) else ""
    content = str(message.get("content") or "") if isinstance(message, dict) else ""
    metadata = message.get("metadata") if isinstance(message, dict) and isinstance(message.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    policies = {
        str(policy or "").strip()
        for policy in (selection.get("policies") if isinstance(selection.get("policies"), list) else [])
        if str(policy or "").strip()
    }
    policy = str(selection.get("policy") or "").strip()
    if policy:
        policies.add(policy)
    if role == "assistant" and feature_scope_excludes_outcome_evidence(content):
        return "memory_feature"
    if role == "user" and feature_scope_excludes_outcome_evidence(content):
        return "memory_feature"
    by_role = {
        "user": "user_prompt",
        "assistant": "assistant_response",
        "tool": "tool_evidence",
    }
    if role in by_role:
        return by_role[role]
    by_policy = {
        "selected_user_prompt": "user_prompt",
        "selected_user_profile_fact": "user_prompt",
        "selected_assistant_profile_fact": "assistant_response",
        "selected_assistant_decision_outcome_only": "assistant_response",
        "selected_tool_evidence_only": "tool_evidence",
    }
    for policy_value in policies:
        event_type = by_policy.get(policy_value)
        if event_type:
            return event_type
    return default_event_type or "conversation_event"


def memory_selection_policy_counts_for_message(message: Json, *, default_counts: Json | None = None) -> Json:
    role = normalize_message_role(message.get("role")) if isinstance(message, dict) else ""
    content = str(message.get("content") or "") if isinstance(message, dict) else ""
    metadata = message.get("metadata") if isinstance(message, dict) and isinstance(message.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    policies: list[str] = []
    if isinstance(selection.get("policies"), list):
        policies.extend(str(policy or "").strip() for policy in selection.get("policies", []))
    selection_policy = str(selection.get("policy") or "").strip()
    if selection_policy:
        policies.append(selection_policy)
    if not policies and role == "user":
        policies.append("selected_user_prompt")
        if user_profile_fact_lineage_text(content):
            policies.append("selected_user_profile_fact")
    elif not policies and role == "assistant":
        feature_memory_only = feature_scope_excludes_outcome_evidence(content)
        if assistant_profile_fact_lineage_text(content) or feature_memory_only:
            policies.append("selected_assistant_profile_fact")
        if content and not feature_memory_only:
            policies.append("selected_assistant_decision_outcome_only")
    elif not policies and role == "tool":
        policies.append("selected_tool_evidence_only")
    counts: Json = {}
    for policy in ordered_unique_any([policy for policy in policies if policy]):
        counts[policy] = 1
    if counts:
        return counts
    return dict(default_counts or {})


def memory_selection_retention_for_message(message: Json, *, default_retention: Json | None = None) -> Json:
    metadata = message.get("metadata") if isinstance(message, dict) and isinstance(message.get("metadata"), dict) else {}
    selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
    if not selection:
        return dict(default_retention or {})
    retention: Json = {
        "source_memory_selection_lossy_count": 1 if bool(selection.get("selection_lossy")) else 0,
        "source_memory_selection_complete_count": 0 if bool(selection.get("selection_lossy")) else 1,
    }
    for source_key, target_key in [
        ("dropped_text_chars", "source_memory_selection_dropped_text_chars"),
        ("dropped_line_count", "source_memory_selection_dropped_line_count"),
        ("retained_text_ratio", "source_memory_selection_retained_text_ratio_avg"),
        ("retained_line_ratio", "source_memory_selection_retained_line_ratio_avg"),
    ]:
        if selection.get(source_key) not in (None, ""):
            retention[target_key] = selection.get(source_key)
    return retention


def source_event_lineage_summary(records: list[Json]) -> Json:
    role_counts: Json = {}
    hook_type_counts: Json = {}
    codex_event_counts: Json = {}
    memory_scopes: list[str] = []
    session_continuities: list[str] = []
    extraction_phases: list[str] = []
    target_memory_scopes: list[str] = []
    target_session_continuities: list[str] = []
    target_extraction_phases: list[str] = []
    memory_selection_policy_counts: Json = {}
    memory_selection_lossy_count = 0
    memory_selection_complete_count = 0
    memory_selection_dropped_text_chars = 0
    memory_selection_dropped_line_count = 0
    memory_selection_retained_text_ratio_sum = 0.0
    memory_selection_retained_line_ratio_sum = 0.0
    memory_selection_retained_ratio_count = 0
    profile_promotion_policies: list[str] = []
    profile_promotion_blockers: list[str] = []
    profile_memory_classes: list[str] = []
    profile_memory_kinds: list[str] = []
    memory_layer_counts: Json = {}
    final_session_boundary_count = 0

    def add_count(counts: Json, name: Any, count: Any = 1) -> None:
        label = str(name or "").strip()
        if not label:
            return
        try:
            amount = max(0, int(count or 0))
        except (TypeError, ValueError):
            amount = 0
        if amount:
            counts[label] = int(counts.get(label, 0)) + amount

    def add_role_count(name: Any, count: Any = 1) -> None:
        role = normalize_message_role(name)
        if role:
            add_count(role_counts, role, count)

    def add_values(values: list[str], source: Any) -> None:
        if isinstance(source, list):
            for item in source:
                label = str(item or "").strip()
                if label:
                    values.append(label)
        else:
            label = str(source or "").strip()
            if label:
                values.append(label)

    for record in records:
        if not isinstance(record, dict):
            continue
        record_messages = messages_from_event_record(record)
        existing_role_counts = record.get("source_role_counts") if isinstance(record.get("source_role_counts"), dict) else {}
        if len(record_messages) > 1:
            for message in record_messages:
                add_role_count(message.get("role"), 1)
        elif existing_role_counts:
            for role, count in existing_role_counts.items():
                add_role_count(role, count)
        else:
            roles = record.get("source_roles") if isinstance(record.get("source_roles"), list) else []
            if roles:
                for role in roles:
                    add_role_count(role, 1)
            else:
                event_role = str(record.get("source_role") or "").strip()
                if event_role:
                    add_role_count(event_role, 1)
                else:
                    for message in record_messages:
                        add_role_count(message.get("role"), 1)

        existing_hook_counts = record.get("source_hook_type_counts") if isinstance(record.get("source_hook_type_counts"), dict) else {}
        if existing_hook_counts:
            for hook_type, count in existing_hook_counts.items():
                add_count(hook_type_counts, hook_type, count)
        else:
            hook_values: list[str] = []
            add_values(hook_values, record.get("source_hook_types"))
            envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
            metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
            add_values(hook_values, envelope.get("hook_type"))
            add_values(hook_values, metadata.get("hook_type"))
            add_values(hook_values, metadata.get("source_hook_types"))
            add_values(hook_values, hook.get("hook_type"))
            for hook_type in ordered_unique_any(hook_values):
                add_count(hook_type_counts, hook_type, 1)

        existing_codex_counts = record.get("source_codex_event_counts") if isinstance(record.get("source_codex_event_counts"), dict) else {}
        if existing_codex_counts:
            for codex_event, count in existing_codex_counts.items():
                add_count(codex_event_counts, codex_event, count)
        else:
            codex_values: list[str] = []
            add_values(codex_values, record.get("source_codex_events"))
            envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
            metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
            add_values(codex_values, envelope.get("codex_event"))
            add_values(codex_values, metadata.get("codex_event"))
            add_values(codex_values, metadata.get("source_codex_events"))
            add_values(codex_values, hook.get("codex_event"))
            add_values(codex_values, hook.get("trigger"))
            for codex_event in ordered_unique_any(codex_values):
                add_count(codex_event_counts, codex_event, 1)
        if not hook_type_counts:
            for codex_event in sorted(codex_event_counts):
                add_count(hook_type_counts, legacy_hook_type_from_codex_event(codex_event), codex_event_counts[codex_event])

        existing_selection_counts = (
            record.get("source_memory_selection_policy_counts")
            if isinstance(record.get("source_memory_selection_policy_counts"), dict)
            else {}
        )
        if existing_selection_counts:
            for policy, count in existing_selection_counts.items():
                add_count(memory_selection_policy_counts, policy, count)
        else:
            selection_values: list[str] = []
            add_values(selection_values, record.get("source_memory_selection_policies"))
            envelope = record.get("envelope") if isinstance(record.get("envelope"), dict) else {}
            metadata = envelope.get("metadata") if isinstance(envelope.get("metadata"), dict) else {}
            selection = record.get("codex_memory_selection") if isinstance(record.get("codex_memory_selection"), dict) else {}
            envelope_selection = (
                envelope.get("codex_memory_selection")
                if isinstance(envelope.get("codex_memory_selection"), dict)
                else {}
            )
            metadata_selection = (
                metadata.get("codex_memory_selection")
                if isinstance(metadata.get("codex_memory_selection"), dict)
                else {}
            )
            add_values(selection_values, selection.get("policy"))
            add_values(selection_values, envelope_selection.get("policy"))
            add_values(selection_values, metadata_selection.get("policy"))
            for policy in ordered_unique_any(selection_values):
                add_count(memory_selection_policy_counts, policy, 1)
        record_has_retention_counts = False
        for field, target in [
            ("source_memory_selection_lossy_count", "lossy"),
            ("source_memory_selection_complete_count", "complete"),
        ]:
            try:
                amount = max(0, int(record.get(field) or 0))
            except (TypeError, ValueError):
                amount = 0
            if target == "lossy":
                memory_selection_lossy_count += amount
            else:
                memory_selection_complete_count += amount
            record_has_retention_counts = record_has_retention_counts or amount > 0
        if not record_has_retention_counts:
            seen_selection_sources: set[tuple[Any, ...]] = set()
            for source in [
                record.get("codex_memory_selection") if isinstance(record.get("codex_memory_selection"), dict) else {},
                (record.get("envelope") or {}).get("codex_memory_selection")
                if isinstance(record.get("envelope"), dict)
                and isinstance((record.get("envelope") or {}).get("codex_memory_selection"), dict)
                else {},
                ((record.get("envelope") or {}).get("metadata") or {}).get("codex_memory_selection")
                if isinstance(record.get("envelope"), dict)
                and isinstance((record.get("envelope") or {}).get("metadata"), dict)
                and isinstance(((record.get("envelope") or {}).get("metadata") or {}).get("codex_memory_selection"), dict)
                else {},
            ]:
                if not source:
                    continue
                selection_key = (
                    source.get("policy"),
                    bool(source.get("selection_lossy")),
                    source.get("dropped_text_chars"),
                    source.get("dropped_line_count"),
                    source.get("retained_text_ratio"),
                    source.get("retained_line_ratio"),
                )
                if selection_key in seen_selection_sources:
                    continue
                seen_selection_sources.add(selection_key)
                if bool(source.get("selection_lossy")):
                    memory_selection_lossy_count += 1
                else:
                    memory_selection_complete_count += 1
                try:
                    memory_selection_dropped_text_chars += max(0, int(source.get("dropped_text_chars") or 0))
                except (TypeError, ValueError):
                    pass
                try:
                    memory_selection_dropped_line_count += max(0, int(source.get("dropped_line_count") or 0))
                except (TypeError, ValueError):
                    pass
                try:
                    memory_selection_retained_text_ratio_sum += float(source.get("retained_text_ratio"))
                    memory_selection_retained_line_ratio_sum += float(source.get("retained_line_ratio"))
                    memory_selection_retained_ratio_count += 1
                except (TypeError, ValueError):
                    pass
        for field, accumulator in [
            ("source_memory_selection_dropped_text_chars", "text"),
            ("source_memory_selection_dropped_line_count", "line"),
        ]:
            try:
                amount = max(0, int(record.get(field) or 0))
            except (TypeError, ValueError):
                amount = 0
            if accumulator == "text":
                memory_selection_dropped_text_chars += amount
            else:
                memory_selection_dropped_line_count += amount
        if "source_memory_selection_retained_text_ratio_avg" in record:
            try:
                memory_selection_retained_text_ratio_sum += float(record.get("source_memory_selection_retained_text_ratio_avg"))
                memory_selection_retained_line_ratio_sum += float(record.get("source_memory_selection_retained_line_ratio_avg", 1.0))
                memory_selection_retained_ratio_count += 1
            except (TypeError, ValueError):
                pass

        add_values(memory_scopes, record.get("source_memory_scopes"))
        add_values(memory_scopes, record.get("memory_scope"))
        add_values(session_continuities, record.get("source_session_continuities"))
        add_values(session_continuities, record.get("session_continuity"))
        add_values(extraction_phases, record.get("source_extraction_phases"))
        add_values(extraction_phases, record.get("extraction_phase"))
        add_values(target_memory_scopes, record.get("memory_scope"))
        add_values(target_session_continuities, record.get("session_continuity"))
        add_values(target_extraction_phases, record.get("extraction_phase"))
        add_values(profile_promotion_policies, record.get("source_profile_promotion_policies"))
        add_values(profile_promotion_policies, record.get("profile_promotion_policy"))
        add_values(profile_promotion_blockers, record.get("source_profile_promotion_blockers"))
        add_values(profile_promotion_blockers, record.get("profile_promotion_blocker"))
        add_values(profile_memory_classes, record.get("source_profile_memory_classes"))
        add_values(profile_memory_classes, record.get("profile_memory_class"))
        add_values(profile_memory_kinds, record.get("source_profile_memory_kinds"))
        add_values(profile_memory_kinds, record.get("profile_memory_kind"))
        existing_layer_counts = (
            record.get("source_memory_layer_counts")
            if isinstance(record.get("source_memory_layer_counts"), dict)
            else {}
        )
        if existing_layer_counts:
            for layer, count in existing_layer_counts.items():
                add_count(memory_layer_counts, layer, count)
        else:
            layer_values: list[str] = []
            add_values(layer_values, record.get("source_memory_layers"))
            add_values(layer_values, record.get("memory_layer"))
            inferred_layer = candidate_memory_layer_name(record)
            if inferred_layer:
                add_values(layer_values, inferred_layer)
            for layer in ordered_unique_any(layer_values):
                add_count(memory_layer_counts, layer, 1)
        try:
            final_session_boundary_count += max(0, int(record.get("source_final_session_boundary_count") or 0))
        except (TypeError, ValueError):
            pass
        if bool(record.get("final_session_boundary")):
            final_session_boundary_count += 1

    source_roles = sorted(role_counts)
    source_hook_types = sorted(hook_type_counts)
    source_codex_events = sorted(codex_event_counts)
    source_memory_scopes = ordered_unique_any(memory_scopes)
    source_session_continuities = ordered_unique_any(session_continuities)
    source_extraction_phases = ordered_unique_any(extraction_phases)
    source_memory_selection_policies = sorted(memory_selection_policy_counts)
    explicit_memory_scopes = ordered_unique_any(target_memory_scopes)
    explicit_session_continuities = ordered_unique_any(target_session_continuities)
    explicit_extraction_phases = ordered_unique_any(target_extraction_phases)
    source_profile_promotion_policies = ordered_unique_any(profile_promotion_policies)
    source_profile_promotion_blockers = ordered_unique_any(profile_promotion_blockers)
    source_profile_memory_classes = ordered_unique_any(profile_memory_classes)
    source_profile_memory_kinds = ordered_unique_any(profile_memory_kinds)
    source_memory_layers = sorted(memory_layer_counts)
    memory_scope = (
        explicit_memory_scopes[0]
        if len(explicit_memory_scopes) == 1
        else "user_profile"
        if source_memory_scopes == ["user_profile"]
        else "session"
        if "session" in source_memory_scopes
        else source_memory_scopes[0]
        if source_memory_scopes
        else ""
    )
    session_continuity = (
        explicit_session_continuities[0]
        if len(explicit_session_continuities) == 1
        else "cross_session"
        if source_session_continuities == ["cross_session"]
        else "same_session"
        if "same_session" in source_session_continuities
        else source_session_continuities[0]
        if source_session_continuities
        else ""
    )
    extraction_phase = (
        explicit_extraction_phases[0]
        if len(explicit_extraction_phases) == 1
        else "final"
        if source_extraction_phases == ["final"]
        else "provisional"
        if "provisional" in source_extraction_phases
        else source_extraction_phases[0]
        if source_extraction_phases
        else ""
    )
    lineage = {
        "source_roles": source_roles,
        "source_role_counts": role_counts,
        "source_hook_types": source_hook_types,
        "source_hook_type_counts": hook_type_counts,
        "source_codex_events": source_codex_events,
        "source_codex_event_counts": codex_event_counts,
        "source_memory_selection_policies": source_memory_selection_policies,
        "source_memory_selection_policy_counts": memory_selection_policy_counts,
        "source_memory_selection_lossy_count": memory_selection_lossy_count,
        "source_memory_selection_complete_count": memory_selection_complete_count,
        "source_memory_selection_dropped_text_chars": memory_selection_dropped_text_chars,
        "source_memory_selection_dropped_line_count": memory_selection_dropped_line_count,
        "source_memory_selection_retained_text_ratio_avg": round(memory_selection_retained_text_ratio_sum / memory_selection_retained_ratio_count, 6) if memory_selection_retained_ratio_count else 1.0,
        "source_memory_selection_retained_line_ratio_avg": round(memory_selection_retained_line_ratio_sum / memory_selection_retained_ratio_count, 6) if memory_selection_retained_ratio_count else 1.0,
        "source_memory_scopes": source_memory_scopes,
        "source_session_continuities": source_session_continuities,
        "source_extraction_phases": source_extraction_phases,
        "source_profile_promotion_policies": source_profile_promotion_policies,
        "source_profile_promotion_blockers": source_profile_promotion_blockers,
        "source_profile_memory_classes": source_profile_memory_classes,
        "source_profile_memory_kinds": source_profile_memory_kinds,
        "source_memory_layers": source_memory_layers,
        "source_memory_layer_counts": memory_layer_counts,
        "source_final_session_boundary_count": final_session_boundary_count,
    }
    if memory_scope:
        lineage["memory_scope"] = memory_scope
    if session_continuity:
        lineage["session_continuity"] = session_continuity
    if extraction_phase:
        lineage["extraction_phase"] = extraction_phase
    if final_session_boundary_count:
        lineage["final_session_boundary"] = True
    return lineage


def compression_profile_layer_values(records: list[Json]) -> Json:
    profile_classes: set[str] = set()
    profile_kinds: set[str] = set()
    for record in records:
        for value in record.get("source_profile_memory_classes", []) if isinstance(record.get("source_profile_memory_classes"), list) else []:
            text = str(value or "").strip()
            if text:
                profile_classes.add(text)
        for value in record.get("source_profile_memory_kinds", []) if isinstance(record.get("source_profile_memory_kinds"), list) else []:
            text = str(value or "").strip()
            if text:
                profile_kinds.add(text)
        profile_class = str(record.get("profile_memory_class") or "").strip()
        profile_kind = str(record.get("profile_memory_kind") or "").strip()
        if profile_class:
            profile_classes.add(profile_class)
        if profile_kind:
            profile_kinds.add(profile_kind)
        event_type = str(record.get("event_type") or "").strip().lower()
        policies = record.get("source_memory_selection_policies") if isinstance(record.get("source_memory_selection_policies"), list) else []
        if event_type in {"assistant_response", "tool_evidence", "assistant_decision"} or any(
            str(policy or "") in {"selected_assistant_decision_outcome_only", "selected_tool_evidence_only"}
            for policy in policies
        ):
            profile_classes.add("codex_outcome")
            profile_kinds.add("codex_outcome")
    classes = sorted(profile_classes)
    kinds = sorted(profile_kinds)
    return {
        "source_profile_memory_classes": classes,
        "source_profile_memory_kinds": kinds,
        "profile_memory_class": classes[0] if len(classes) == 1 else ("mixed" if classes else ""),
        "profile_memory_kind": "codex_outcome" if "codex_outcome" in kinds else (kinds[0] if len(kinds) == 1 else ("mixed" if kinds else "")),
    }


def compression_context_index_terms(record: Json) -> list[str]:
    try:
        from tools.matrixark_mcp_indexing import benchmark_quality_index_terms
    except ModuleNotFoundError:  # Direct script execution from tools/.
        from matrixark_mcp_indexing import benchmark_quality_index_terms
    terms = ["operator:TIME_COMPRESS", "context_class:compression", "source_type:message"]
    terms.extend(benchmark_quality_index_terms(record.get("summary_text"), record.get("text")))
    for token in tokens(str(record.get("summary_text") or "")):
        if token:
            terms.append(f"keyword:{token}")
    for field, prefix in [
        ("source_roles", "source_role"),
        ("source_hook_types", "hook_type"),
        ("source_codex_events", "codex_event"),
        ("source_memory_selection_policies", "memory_selection_policy"),
        ("source_memory_scopes", "source_memory_scope"),
        ("source_session_continuities", "source_session_continuity"),
        ("source_extraction_phases", "extraction_phase"),
        ("source_profile_promotion_policies", "profile_promotion_policy"),
        ("source_profile_promotion_blockers", "profile_promotion_blocker"),
        ("source_profile_memory_classes", "profile_memory_class"),
        ("source_profile_memory_kinds", "profile_memory_kind"),
    ]:
        values = record.get(field)
        if isinstance(values, list):
            terms.extend(f"{prefix}:{str(value).strip()}" for value in values if str(value or "").strip())
    for field, prefix in [
        ("memory_scope", "memory_scope"),
        ("session_continuity", "session_continuity"),
        ("extraction_phase", "extraction_phase"),
        ("profile_memory_class", "profile_memory_class"),
        ("profile_memory_kind", "profile_memory_kind"),
    ]:
        value = str(record.get(field) or "").strip()
        if value:
            terms.append(f"{prefix}:{value}")
    try:
        source_final_session_boundary_count = int(record.get("source_final_session_boundary_count") or 0)
    except (TypeError, ValueError):
        source_final_session_boundary_count = 0
    if bool(record.get("final_session_boundary")) or source_final_session_boundary_count > 0:
        terms.append("final_session_boundary:true")
    return ordered_unique_any(terms)


def compression_context_index_records(record: Json) -> list[Json]:
    compression_hash = record.get("compression_id_hash")
    if compression_hash is None:
        return []
    scope = candidate_access_scope(record)
    return [
        context_index_posting_record(
            index_name=index_name,
            data_model="context_compression_event",
            ref_type="compression",
            ref_hashes=[compression_hash],
            node_hash=record.get("node_hash"),
            scope=scope,
            updated_at_ms=record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
        )
        for index_name in compression_context_index_terms(record)
    ]


def latest_value_record_key(record: Json) -> tuple[Any, ...] | None:
    record_type = str(record.get("record_type") or "")
    if record_type == "context_node":
        return (record_type, record.get("node_hash"))
    if record_type == "context_child_ref":
        return (record_type, record.get("child_ref_hash"))
    if record_type == "context_event":
        return (record_type, record.get("event_id_hash"))
    if record_type == "context_summary":
        return (record_type, record.get("summary_type"), record.get("summary_hash") or record.get("node_hash"))
    if record_type == "context_embedding":
        return (record_type, record.get("embedding_type"), record.get("ref_type"), record.get("ref_hash"))
    if record_type == "context_index":
        return (
            record_type,
            record.get("index_name"),
            record.get("scope_key") or canonical_scope_key(record.get("scope", {})) if isinstance(record.get("scope", {}), dict) else record.get("scope_key"),
            record.get("node_hash") or record.get("node_id"),
            record.get("data_model") or record.get("ref_type"),
            record.get("timestamp_key_ms") or record.get("updated_at_ms"),
        )
    if record_type == "context_entity":
        return (record_type, record.get("entity_hash"))
    if record_type == "context_summary_dirty":
        return (record_type, record.get("dirty_hash"))
    if record_type == "session_buffer_event":
        return (record_type, tuple(record.get("buffer_key", [])), record.get("event_id_hash"))
    if record_type == "resource_manifest":
        return (record_type, record.get("resource_hash"))
    if record_type == "skill_registry_update":
        return (record_type, record.get("skill_hash"))
    if record_type == "resource_import_task":
        return (record_type, record.get("resource_import_task_hash"))
    return None


def compact_latest_value_records(records: list[Json]) -> list[Json]:
    latest: dict[tuple[Any, ...], Json] = {}
    output: list[Json] = []
    latest_positions: dict[tuple[Any, ...], int] = {}
    for record in records:
        key = latest_value_record_key(record)
        if key is None or any(part in (None, "") for part in key[1:]):
            output.append(record)
            continue
        existing = latest.get(key)
        if existing is None:
            latest[key] = record
            latest_positions[key] = len(output)
            output.append(record)
            continue
        record_ts = int(record.get("updated_at_ms") or record.get("created_at_ms") or 0)
        existing_ts = int(existing.get("updated_at_ms") or existing.get("created_at_ms") or 0)
        record_revision = int(record.get("profile_revision") or record.get("revision") or 0)
        existing_revision = int(existing.get("profile_revision") or existing.get("revision") or 0)
        if (record_ts, record_revision) >= (existing_ts, existing_revision):
            latest[key] = record
            output[latest_positions[key]] = record
    return output


try:  # mixin
    from tools.matrixark_local_adapter_retrieve import _LocalAdapterRetrieveMixin
except ImportError:
    from matrixark_local_adapter_retrieve import _LocalAdapterRetrieveMixin

try:  # mixin
    from tools.matrixark_local_adapter_ingest import _LocalAdapterIngestMixin
except ImportError:
    from matrixark_local_adapter_ingest import _LocalAdapterIngestMixin

try:  # mixin
    from tools.matrixark_local_adapter_dashboard import _LocalAdapterDashboardMixin
except ImportError:
    from matrixark_local_adapter_dashboard import _LocalAdapterDashboardMixin

try:  # mixin
    from tools.matrixark_local_adapter_summaries import _LocalAdapterSummariesMixin
except ImportError:
    from matrixark_local_adapter_summaries import _LocalAdapterSummariesMixin

try:  # mixin
    from tools.matrixark_local_adapter_session_commit import _LocalAdapterSessionCommitMixin
except ImportError:
    from matrixark_local_adapter_session_commit import _LocalAdapterSessionCommitMixin

try:  # mixin
    from tools.matrixark_local_adapter_context_node import _LocalAdapterContextNodeMixin
except ImportError:
    from matrixark_local_adapter_context_node import _LocalAdapterContextNodeMixin

try:  # mixin
    from tools.matrixark_local_adapter_retrieval import _LocalAdapterRetrievalMixin
except ImportError:
    from matrixark_local_adapter_retrieval import _LocalAdapterRetrievalMixin

@dataclass
class MatrixArkLocalAdapter(_LocalAdapterRetrieveMixin, _LocalAdapterIngestMixin, _LocalAdapterDashboardMixin, _LocalAdapterSummariesMixin, _LocalAdapterSessionCommitMixin, _LocalAdapterContextNodeMixin, _LocalAdapterRetrievalMixin):
    event_log: Path

    def __post_init__(self) -> None:
        self._init_local_runtime_state()

    def _init_local_runtime_state(self) -> None:
        self.event_log.parent.mkdir(parents=True, exist_ok=True)
        # Per-instance JSONL toggle. The proxy/direct-backed adapters persist
        # durably through their Rust client and construct the base adapter with a
        # sentinel "…-unused-…" event_log path to signal that the local JSONL
        # mirror should not be used. Honor that intent: without this, the global
        # LOCAL_JSONL_ENABLED default left the inherited append()/read_all() writing
        # and, crucially, re-reading + re-compacting that redundant log on every
        # call. It grew to the rotation cap (hundreds of MB / tens of thousands of
        # records) and made each retrieve/ingest take tens of seconds -- blowing the
        # request deadline so context never committed. The pure-local adapter (real
        # event_log path) keeps the JSONL; MATRIXARK_LOCAL_JSONL_ENABLED still forces
        # it off globally.
        self._local_jsonl_enabled = LOCAL_JSONL_ENABLED and "-unused-" not in self.event_log.name
        self._write_batch_local = threading.local()
        self._event_log_lock = threading.RLock()
        self._resource_import_worker_count = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_WORKERS", "2")))
        self._resource_import_queue_max = max(1, int(os.environ.get("MATRIXARK_RESOURCE_IMPORT_QUEUE_MAX", "64")))
        self._resource_import_queue: thread_queue.Queue[Json] = thread_queue.Queue(maxsize=self._resource_import_queue_max)
        self._resource_import_workers_started = False
        self._resource_import_worker_lock = threading.RLock()
        self._resource_import_stop = threading.Event()
        self._resource_import_threads: list[threading.Thread] = []
        self._latest_entity_by_hash: dict[int, Json] = {}
        self._entity_cache_loaded = False
        self._session_buffer_cache_lock = threading.RLock()
        self._context_event_by_hash: dict[int, Json] = {}
        self._session_pending_event_ids_by_key: dict[tuple[str, str, str, str], list[int]] = {}
        self._session_committed_event_ids_by_key: dict[tuple[str, str, str, str], set[int]] = {}
        self._context_node_hashes: set[int] = set()
        self._context_child_ref_hashes: set[int] = set()
        self._context_node_cache_loaded = False
        self._read_cache_lock = threading.RLock()
        self._read_cache_records: list[Json] | None = None
        self._read_cache_size = -1
        self._read_cache_mtime_ns = -1
        self._read_cache_source = "empty"
        self._durable_read_cache_last_write_ms = 0.0
        self._retrieval_records_cache_lock = threading.RLock()
        self._retrieval_records_cache_generation = 0
        self._retrieval_records_cache: dict[tuple[Any, ...], Json] = {}
        self._context_pack_cache_lock = threading.RLock()
        self._context_pack_cache: dict[tuple[Any, ...], tuple[float, Json]] = {}
        self._context_pack_cache_max_entries = max(0, int(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_MAX_ENTRIES", "256")))
        self._context_pack_cache_ttl_s = max(0.0, float(os.environ.get("MATRIXARK_CONTEXT_PACK_CACHE_TTL_S", "30")))

    def _write_batch_stack(self) -> list[list[Json]]:
        local = getattr(self, "_write_batch_local", None)
        if local is None:
            self._write_batch_local = threading.local()
            local = self._write_batch_local
        stack = getattr(local, "stack", None)
        if stack is None:
            stack = []
            local.stack = stack
        return stack

    def _current_write_batch(self) -> list[Json] | None:
        stack = self._write_batch_stack()
        return stack[-1] if stack else None

    def _queue_batched_records(self, records: list[Json]) -> bool:
        batch = self._current_write_batch()
        if batch is None:
            return False
        batch.extend(records)
        return True

    def _local_jsonl_guardrails(self) -> Json:
        return {
            "enabled": self._local_jsonl_enabled,
            "max_bytes": LOCAL_JSONL_MAX_BYTES,
            "retention_count": LOCAL_JSONL_RETENTION_COUNT,
            "retention_age_ms": LOCAL_JSONL_RETENTION_AGE_MS,
            "include_bulky_fields": LOCAL_JSONL_INCLUDE_BULKY_FIELDS,
            "durable_read_cache": {
                "enabled": LOCAL_DURABLE_READ_CACHE_ENABLED,
                "path": str(self._durable_read_cache_path()),
                "schema_version": LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
                "last_load_source": self._read_cache_source,
                "min_write_ms": LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS,
            },
            "usage": "testing_debug_only",
        }

    def _sanitize_jsonl_record(self, record: Json) -> Json:
        if LOCAL_JSONL_INCLUDE_BULKY_FIELDS:
            return record
        sanitized = dict(record)
        dropped = sorted(field for field in LOCAL_JSONL_BULKY_FIELDS if field in sanitized)
        for field in dropped:
            sanitized.pop(field, None)
        if dropped:
            metadata = dict(sanitized.get("jsonl_guardrails", {})) if isinstance(sanitized.get("jsonl_guardrails"), dict) else {}
            metadata["dropped_bulky_fields"] = dropped
            sanitized["jsonl_guardrails"] = metadata
        return sanitized

    def _jsonl_rotated_path(self, index: int) -> Path:
        return self.event_log.with_name(f"{self.event_log.name}.{index}")

    def _retained_jsonl_paths(self) -> list[Path]:
        if not self._local_jsonl_enabled:
            return []
        max_rotated = max(0, LOCAL_JSONL_RETENTION_COUNT - 1)
        paths = [self._jsonl_rotated_path(index) for index in range(max_rotated, 0, -1)]
        paths.append(self.event_log)
        return [path for path in paths if path.exists()]

    def _durable_read_cache_path(self) -> Path:
        return self.event_log.with_name(f"{self.event_log.name}.read-cache.json")

    def _jsonl_cache_signature_detail(self, paths: list[Path] | None = None) -> Json:
        total_size = 0
        max_mtime_ns = -1
        entries: list[Json] = []
        for path in paths if paths is not None else self._retained_jsonl_paths():
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            size = int(stat.st_size)
            mtime_ns = int(stat.st_mtime_ns)
            total_size += size
            max_mtime_ns = max(max_mtime_ns, mtime_ns)
            entries.append({"path": str(path.resolve()), "size": size, "mtime_ns": mtime_ns})
        if total_size <= 0 and max_mtime_ns < 0:
            return {"total_size": -1, "max_mtime_ns": -1, "paths": []}
        return {"total_size": total_size, "max_mtime_ns": max_mtime_ns, "paths": entries}

    def _jsonl_cache_signature(self) -> tuple[int, int]:
        signature = self._jsonl_cache_signature_detail()
        return int(signature.get("total_size", -1)), int(signature.get("max_mtime_ns", -1))

    def _load_durable_read_cache(self, signature: Json) -> list[Json] | None:
        if not self._local_jsonl_enabled or not LOCAL_DURABLE_READ_CACHE_ENABLED:
            return None
        try:
            with self._durable_read_cache_path().open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None
        if not isinstance(payload, dict):
            return None
        if payload.get("schema_version") != LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION:
            return None
        if payload.get("cache_key") != str(self.event_log.resolve()):
            return None
        if payload.get("signature") != signature:
            return None
        records = payload.get("records")
        if not isinstance(records, list):
            return None
        return [record for record in records if isinstance(record, dict)]

    def _write_durable_read_cache(self, records: list[Json], signature: Json, *, force: bool = False) -> None:
        if not self._local_jsonl_enabled or not LOCAL_DURABLE_READ_CACHE_ENABLED:
            return
        if int(signature.get("total_size", -1)) < 0:
            return
        now = now_ms()
        if not force and LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS > 0:
            if now - self._durable_read_cache_last_write_ms < LOCAL_DURABLE_READ_CACHE_MIN_WRITE_MS:
                return
        path = self._durable_read_cache_path()
        tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        payload = {
            "schema_version": LOCAL_DURABLE_READ_CACHE_SCHEMA_VERSION,
            "cache_key": str(self.event_log.resolve()),
            "signature": signature,
            "record_count": len(records),
            "records": records,
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with tmp_path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"))
                handle.write("\n")
            tmp_path.replace(path)
            self._durable_read_cache_last_write_ms = now
        except OSError:
            try:
                tmp_path.unlink()
            except OSError:
                pass

    def _clear_jsonl_read_caches(self) -> None:
        cache_key = str(self.event_log.resolve())
        with self._read_cache_lock:
            self._read_cache_records = None
            self._read_cache_size = -1
            self._read_cache_mtime_ns = -1
            self._read_cache_source = "empty"
        with _LOCAL_READ_CACHE_LOCK:
            _LOCAL_READ_CACHE.pop(cache_key, None)
        try:
            self._durable_read_cache_path().unlink()
        except FileNotFoundError:
            pass

    def _prune_jsonl_retention_locked(self) -> None:
        max_rotated = max(0, LOCAL_JSONL_RETENTION_COUNT - 1)
        now_timestamp = max(0.0, now_ms() / 1000.0)
        max_age_s = max(0.0, LOCAL_JSONL_RETENTION_AGE_MS / 1000.0)
        index = max_rotated + 1
        while True:
            path = self._jsonl_rotated_path(index)
            if not path.exists():
                break
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            index += 1
        if max_age_s <= 0:
            return
        for path in [self._jsonl_rotated_path(index) for index in range(1, max_rotated + 1)]:
            try:
                if now_timestamp - float(path.stat().st_mtime) > max_age_s:
                    path.unlink()
            except FileNotFoundError:
                continue

    def _rotate_jsonl_if_needed_locked(self, incoming_bytes: int) -> None:
        if not self._local_jsonl_enabled:
            return
        self._prune_jsonl_retention_locked()
        max_bytes = max(1, LOCAL_JSONL_MAX_BYTES)
        try:
            current_size = int(self.event_log.stat().st_size)
        except FileNotFoundError:
            current_size = 0
        if current_size <= 0 or current_size + max(0, incoming_bytes) <= max_bytes:
            return
        max_rotated = max(0, LOCAL_JSONL_RETENTION_COUNT - 1)
        if max_rotated <= 0:
            try:
                self.event_log.unlink()
            except FileNotFoundError:
                pass
            self._clear_jsonl_read_caches()
            return
        oldest = self._jsonl_rotated_path(max_rotated)
        try:
            oldest.unlink()
        except FileNotFoundError:
            pass
        for index in range(max_rotated - 1, 0, -1):
            source = self._jsonl_rotated_path(index)
            if source.exists():
                source.replace(self._jsonl_rotated_path(index + 1))
        if self.event_log.exists():
            self.event_log.replace(self._jsonl_rotated_path(1))
        self._clear_jsonl_read_caches()

    @contextmanager
    def write_batch(self, label: str = "hot_path"):
        stack = self._write_batch_stack()
        batch: list[Json] = []
        stack.append(batch)
        try:
            yield batch
        except Exception:
            stack.pop()
            raise
        else:
            stack.pop()
            if batch:
                self.append_many(batch)

    def ensure_backend_ready(self, *, reason: str = "manual", probe: bool = True, timeout_ms: int | None = None) -> Json:
        return {
            "status": "ready",
            "backend": "local",
            "reason": reason,
            "probe": bool(probe),
            "attempts": 1,
            "topology": {"mode": "local-jsonl", "event_log": str(self.event_log), "jsonl_guardrails": self._local_jsonl_guardrails()},
            "checks": {
                "mcp_process_started": True,
                "namespace_table_opened": True,
                "slot_coverage_verified_by_warmup_hset_hget": True,
            },
        }

    def backend_metrics(self) -> Json:
        return {
            "backend": getattr(self, "_backend_label", lambda: "local")(),
            "metrics_format": "json",
            "metrics": {
                "mode": "local-jsonl",
                "event_log": str(self.event_log),
                "jsonl_guardrails": self._local_jsonl_guardrails(),
            },
        }

    def _observe_model_latency(self, stage: str, elapsed_ms: float) -> None:
        metrics = getattr(self, "_matrixark_service_metrics", None)
        if metrics is not None:
            try:
                metrics.observe_model_latency(stage, elapsed_ms)
            except Exception:
                pass

    def _update_read_cache_after_append(self, records: list[Json]) -> None:
        if not records:
            return
        cache_key = str(self.event_log.resolve())
        signature = self._jsonl_cache_signature_detail()
        size = int(signature.get("total_size", -1))
        mtime_ns = int(signature.get("max_mtime_ns", -1))
        durable_records: list[Json] | None = None
        with self._read_cache_lock:
            if self._read_cache_records is not None:
                self._read_cache_records.extend(records)
                self._read_cache_records = compact_latest_context_state_records(
                    compact_latest_value_records(self._read_cache_records)
                )
            if size >= 0:
                self._read_cache_size = size
                self._read_cache_mtime_ns = mtime_ns
                if self._read_cache_records is not None:
                    durable_records = list(self._read_cache_records)
            else:
                self._read_cache_records = None
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
                self._read_cache_source = "empty"
        with _LOCAL_READ_CACHE_LOCK:
            cached = _LOCAL_READ_CACHE.get(cache_key)
            if cached is not None:
                _, _, cached_records = cached
                cached_records = compact_latest_context_state_records(
                    compact_latest_value_records(list(cached_records) + list(records))
                )
                _LOCAL_READ_CACHE[cache_key] = (self._read_cache_size, self._read_cache_mtime_ns, cached_records)
                if durable_records is None:
                    durable_records = list(cached_records)
            elif self._read_cache_records is not None:
                _LOCAL_READ_CACHE[cache_key] = (
                    self._read_cache_size,
                    self._read_cache_mtime_ns,
                    compact_latest_context_state_records(compact_latest_value_records(list(self._read_cache_records))),
                )
        if durable_records is not None:
            self._write_durable_read_cache(durable_records, signature)
        if any(str(record.get("record_type") or "") in RETRIEVAL_HOT_RECORD_TYPES for record in records):
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
                with self._context_pack_cache_lock:
                    self._context_pack_cache.clear()

    def append(self, record: Json) -> None:
        records = materialize_serving_record_batch([record])
        if self._queue_batched_records(records):
            return
        jsonl_records = [self._sanitize_jsonl_record(item) for item in records]
        jsonl_lines = [json.dumps(item, separators=(",", ":")) + "\n" for item in jsonl_records]
        if self._local_jsonl_enabled:
            with self._event_log_lock:
                self._rotate_jsonl_if_needed_locked(sum(len(line.encode("utf-8")) for line in jsonl_lines))
                with self.event_log.open("a", encoding="utf-8") as handle:
                    for line in jsonl_lines:
                        handle.write(line)
                self._prune_jsonl_retention_locked()
        self._update_latest_entity_cache(records)
        self._update_read_cache_after_append(jsonl_records)

    def append_many(self, records: list[Json]) -> None:
        records = materialize_serving_record_batch(records)
        if not records:
            return
        if self._queue_batched_records(records):
            return
        jsonl_records = [self._sanitize_jsonl_record(record) for record in records]
        jsonl_lines = [json.dumps(record, separators=(",", ":")) + "\n" for record in jsonl_records]
        if self._local_jsonl_enabled:
            with self._event_log_lock:
                self._rotate_jsonl_if_needed_locked(sum(len(line.encode("utf-8")) for line in jsonl_lines))
                with self.event_log.open("a", encoding="utf-8") as handle:
                    for line in jsonl_lines:
                        handle.write(line)
                self._prune_jsonl_retention_locked()
        self._update_latest_entity_cache(records)
        self._update_read_cache_after_append(jsonl_records)

    def _update_latest_entity_cache(self, records: list[Json]) -> None:
        if not hasattr(self, "_session_buffer_cache_lock"):
            self._session_buffer_cache_lock = threading.RLock()
        if not hasattr(self, "_context_event_by_hash"):
            self._context_event_by_hash = {}
        if not hasattr(self, "_session_pending_event_ids_by_key"):
            self._session_pending_event_ids_by_key = {}
        if not hasattr(self, "_session_committed_event_ids_by_key"):
            self._session_committed_event_ids_by_key = {}
        for record in records:
            record_type = record.get("record_type")
            if record_type == "context_event":
                try:
                    event_hash = int(record.get("event_id_hash", 0))
                except (TypeError, ValueError):
                    event_hash = 0
                if event_hash:
                    with self._session_buffer_cache_lock:
                        self._context_event_by_hash[event_hash] = record
                continue
            if record_type == "session_buffer_event":
                try:
                    event_hash = int(record.get("event_id_hash", 0))
                except (TypeError, ValueError):
                    event_hash = 0
                raw_key = record.get("buffer_key", [])
                if event_hash and isinstance(raw_key, list) and len(raw_key) == 4:
                    key = tuple(str(item) for item in raw_key)
                    with self._session_buffer_cache_lock:
                        committed = self._session_committed_event_ids_by_key.setdefault(key, set())
                        pending = self._session_pending_event_ids_by_key.setdefault(key, [])
                        if event_hash in self._context_event_by_hash:
                            enriched_event = dict(self._context_event_by_hash[event_hash])
                            if isinstance(record.get("envelope"), dict) and "envelope" not in enriched_event:
                                enriched_event["envelope"] = record["envelope"]
                            if isinstance(record.get("agent_hook"), dict) and "agent_hook" not in enriched_event:
                                enriched_event["agent_hook"] = record["agent_hook"]
                            self._context_event_by_hash[event_hash] = enriched_event
                        if event_hash not in committed and event_hash not in pending:
                            pending.append(event_hash)
                continue
            if record_type == "context_batch_commit":
                key = session_buffer_key_from_scope(record.get("scope", {}))
                source_ids: list[int] = []
                for ref in record.get("source_event_ids", []):
                    try:
                        source_ids.append(int(ref))
                    except (TypeError, ValueError):
                        continue
                if source_ids:
                    with self._session_buffer_cache_lock:
                        committed = self._session_committed_event_ids_by_key.setdefault(key, set())
                        committed.update(source_ids)
                        pending = self._session_pending_event_ids_by_key.setdefault(key, [])
                        if pending:
                            source_set = set(source_ids)
                            self._session_pending_event_ids_by_key[key] = [event_id for event_id in pending if event_id not in source_set]
                continue
            if record_type == "context_node":
                try:
                    node_hash = int(record.get("node_hash", 0))
                except (TypeError, ValueError):
                    node_hash = 0
                if node_hash:
                    self._context_node_hashes.add(node_hash)
                continue
            if record_type == "context_child_ref":
                try:
                    child_ref_hash = int(record.get("child_ref_hash", 0))
                except (TypeError, ValueError):
                    child_ref_hash = 0
                if child_ref_hash:
                    self._context_child_ref_hashes.add(child_ref_hash)
                continue
            if record_type != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash", 0))
            except (TypeError, ValueError):
                continue
            if entity_hash:
                self._latest_entity_by_hash[entity_hash] = record

    def _ensure_context_node_cache_loaded(self) -> None:
        if self._context_node_cache_loaded:
            return
        self._context_node_hashes = set()
        self._context_child_ref_hashes = set()
        for record in self.read_all():
            if record.get("record_type") == "context_node" and record.get("node_hash") is not None:
                try:
                    self._context_node_hashes.add(int(record.get("node_hash")))
                except (TypeError, ValueError):
                    pass
            elif record.get("record_type") == "context_child_ref" and record.get("child_ref_hash") is not None:
                try:
                    self._context_child_ref_hashes.add(int(record.get("child_ref_hash")))
                except (TypeError, ValueError):
                    pass
        self._context_node_cache_loaded = True

    def _ensure_latest_entity_cache_loaded(self) -> None:
        if self._entity_cache_loaded:
            return
        records = self.read_all()
        self._latest_entity_by_hash = {}
        for record in records:
            if record.get("record_type") != "context_entity":
                continue
            try:
                entity_hash = int(record.get("entity_hash", 0))
            except (TypeError, ValueError):
                continue
            if entity_hash:
                self._latest_entity_by_hash[entity_hash] = record
        self._entity_cache_loaded = True

    def append_audit(self, record: Json) -> None:
        self.append(record)

    def telemetry_record_for_context_pack(
        self,
        pack: Json,
        *,
        query: str,
        scope: Json,
        audit_mode: str,
        request_metadata: Json | None = None,
    ) -> Json:
        recall_policy = pack.get("recall_policy", {}) if isinstance(pack.get("recall_policy"), dict) else {}
        retrieval_metrics = pack.get("retrieval_metrics", {}) if isinstance(pack.get("retrieval_metrics"), dict) else {}
        memory_layer_budget = (
            retrieval_metrics.get("memory_layer_budget")
            if isinstance(retrieval_metrics.get("memory_layer_budget"), dict)
            else recall_policy.get("memory_layer_budget")
            if isinstance(recall_policy.get("memory_layer_budget"), dict)
            else {}
        )
        dropped_memory_layer_budget = (
            retrieval_metrics.get("dropped_memory_layer_budget")
            if isinstance(retrieval_metrics.get("dropped_memory_layer_budget"), dict)
            else recall_policy.get("dropped_memory_layer_budget")
            if isinstance(recall_policy.get("dropped_memory_layer_budget"), dict)
            else {}
        )
        memory_layer_pressure = (
            retrieval_metrics.get("memory_layer_pressure")
            if isinstance(retrieval_metrics.get("memory_layer_pressure"), dict)
            else recall_policy.get("memory_layer_pressure")
            if isinstance(recall_policy.get("memory_layer_pressure"), dict)
            else {}
        )
        if not memory_layer_pressure:
            memory_layer_pressure = memory_layer_pressure_summary(
                memory_layer_budget,
                dropped_memory_layer_budget,
            )
        stage_budgets = recall_policy.get("stage_latency_budgets", {}) if isinstance(recall_policy.get("stage_latency_budgets"), dict) else {}
        async_pipeline_readiness = (
            retrieval_metrics.get("async_pipeline_readiness")
            if isinstance(retrieval_metrics.get("async_pipeline_readiness"), dict)
            else recall_policy.get("async_pipeline_readiness")
            if isinstance(recall_policy.get("async_pipeline_readiness"), dict)
            else {}
        )
        memory_selection_policy_budget = (
            recall_policy.get("memory_selection_policy_budget_policy")
            if isinstance(recall_policy.get("memory_selection_policy_budget_policy"), dict)
            else {}
        )
        tree = recall_policy.get("tree_traversal", {}) if isinstance(recall_policy.get("tree_traversal"), dict) else {}
        secondary = recall_policy.get("secondary_index_filter", {}) if isinstance(recall_policy.get("secondary_index_filter"), dict) else {}
        rerank = recall_policy.get("rerank", {}) if isinstance(recall_policy.get("rerank"), dict) else {}
        time_weighted = recall_policy.get("time_weighted_recall", {}) if isinstance(recall_policy.get("time_weighted_recall"), dict) else {}
        session_identity = recall_policy.get("session_identity", {}) if isinstance(recall_policy.get("session_identity"), dict) else {}
        dropped_refs = pack.get("dropped_refs", {}) if isinstance(pack.get("dropped_refs"), dict) else {}
        metric_bucket_counts = (
            retrieval_metrics.get("dropped_ref_bucket_counts")
            if isinstance(retrieval_metrics.get("dropped_ref_bucket_counts"), dict)
            else {}
        )
        dropped_ref_bucket_counts = {
            str(key): int(value)
            for key, value in (
                metric_bucket_counts.items()
                if metric_bucket_counts
                else ((key, value) for key, value in dropped_refs.items() if isinstance(value, int))
            )
            if str(key) != "deadline_exceeded" and int(value) > 0
        }
        dropped_ref_count = int(retrieval_metrics.get("dropped_refs") or dropped_refs.get("dropped_ref_count") or 0)
        if not dropped_ref_count and isinstance(dropped_refs.get("refs"), list):
            dropped_ref_count = len(dropped_refs.get("refs") or [])
        if not dropped_ref_count:
            dropped_ref_count = sum(dropped_ref_bucket_counts.values())
        record = {
            "record_type": "context_pack_telemetry",
            "context_pack_id": pack.get("context_pack_id", ""),
            "query_hash": stable_hash(query),
            "scope": scope,
            "audit_mode": audit_mode,
            "question_type": pack.get("question_type", ""),
            "query_plan": recall_policy.get("query_plan", {}),
            "selected_ref_count": len(pack.get("selected_refs", []) or []),
            "selected_ref_counts": pack.get("selected_ref_counts", {}),
            "dropped_ref_count": dropped_ref_count,
            "dropped_ref_bucket_counts": dropped_ref_bucket_counts,
            "stale_dropped_refs": int(
                retrieval_metrics.get("stale_dropped_refs")
                or dropped_ref_bucket_counts.get("stale", 0)
            ),
            "used_local_context_tokens": pack.get("used_local_context_tokens", 0),
            "used_remote_context_tokens": pack.get("used_remote_context_tokens", 0),
            "total_prompt_context_tokens": pack.get("total_prompt_context_tokens", 0),
            "remote_context_budget_tokens": pack.get("remote_context_budget_tokens", 0),
            "requested_max_context_tokens": pack.get("requested_max_context_tokens", 0),
            "memory_layer_budget": memory_layer_budget,
            "dropped_memory_layer_budget": dropped_memory_layer_budget,
            "memory_layer_pressure": memory_layer_pressure,
            "memory_selection_policy_budget": memory_selection_policy_budget,
            "async_pipeline_readiness": async_pipeline_readiness,
            "session_identity": session_identity,
            "quality_warnings": pack.get("quality_warnings", []) or [],
            "partial_context_pack": bool(pack.get("partial_context_pack", False)),
            "insufficient_context": bool(pack.get("insufficient_context", False)),
            "quality_warning_count": len(pack.get("quality_warnings", []) or []),
            "primary_candidate_count": pack.get("primary_candidate_count", 0),
            "auxiliary_candidate_count": pack.get("auxiliary_candidate_count", 0),
            "tree_fallback_to_flat": bool(tree.get("fallback_to_flat", False)),
            "tree_selected_node_count": tree.get("selected_node_count", 0),
            "secondary_index_matched_candidate_count": secondary.get("matched_candidate_count", 0),
            "secondary_index_dropped_candidate_count": secondary.get("dropped_candidate_count", 0),
            "rerank_mode": rerank.get("mode", ""),
            "rerank_candidate_count": rerank.get("reranked_candidate_count", 0),
            "time_weighted_recall": time_weighted,
            "stage_latency_budgets": stage_budgets,
            "created_at_ms": now_ms(),
        }
        if request_metadata:
            record["retrieval_request_metadata"] = {
                key: request_metadata.get(key)
                for key in [
                    "source",
                    "retrieval_source",
                    "codex_event",
                    "hook_type",
                    "codex_session_id_source",
                    "session_id_source",
                    "lifecycle_stage",
                ]
                if request_metadata.get(key) not in (None, "", [], {})
            }
        return record

    def append_context_pack_visibility(
        self,
        *,
        pack: Json,
        audit_record: Json,
        query: str,
        scope: Json,
        audit_mode: str,
        request_metadata: Json | None = None,
        audit_sample_rate: float = 1.0,
    ) -> Json:
        telemetry_write_mode = CONTEXT_TELEMETRY_WRITE_MODE
        if telemetry_write_mode not in {"inline", "async", "sync", "off"}:
            raise MatrixArkError("MATRIXARK_CONTEXT_TELEMETRY_WRITE_MODE must be inline, async, sync, or off")
        force_rich_audit = bool(
            pack.get("partial_context_pack")
            or pack.get("insufficient_context")
            or pack.get("quality_warnings")
        )
        sample_basis = stable_hash(f"{pack.get('context_pack_id', '')}:{query}") % 1_000_000
        sample_value = sample_basis / 1_000_000.0
        rich_audit_sampled = bool(audit_mode == "full" and (force_rich_audit or sample_value < audit_sample_rate))
        telemetry_enabled = audit_mode != "off" and telemetry_write_mode != "off"
        visibility_decision = {
            "audit_mode": audit_mode,
            "audit_sample_rate": round(audit_sample_rate, 6),
            "audit_sample_value": round(sample_value, 6),
            "rich_replay_audit": rich_audit_sampled,
            "full_replay_audit_enabled": audit_mode == "full",
            "rich_replay_audit_force_reason": (
                "partial_or_warning" if force_rich_audit and audit_mode == "full" else "sampled" if rich_audit_sampled else "not_sampled"
            ),
            "telemetry_record": telemetry_enabled,
            "telemetry_write_mode": telemetry_write_mode,
            "serving_blocked_on_full_audit": False,
            "full_replay_audit_requires_full_mode": True,
        }
        telemetry = self.telemetry_record_for_context_pack(
            pack,
            query=query,
            scope=scope,
            audit_mode=audit_mode,
            request_metadata=request_metadata,
        )
        telemetry["visibility_decision"] = visibility_decision
        if telemetry_enabled and telemetry_write_mode in {"inline", "sync"}:
            self.append(telemetry)
        elif telemetry_enabled and telemetry_write_mode == "async":
            self.append_audit(telemetry)
        if rich_audit_sampled:
            audit_record["operational_visibility_policy"] = visibility_decision
            if isinstance(telemetry.get("memory_layer_budget"), dict) and "memory_layer_budget" not in audit_record:
                audit_record["memory_layer_budget"] = telemetry["memory_layer_budget"]
            if isinstance(telemetry.get("dropped_memory_layer_budget"), dict) and "dropped_memory_layer_budget" not in audit_record:
                audit_record["dropped_memory_layer_budget"] = telemetry["dropped_memory_layer_budget"]
            if isinstance(telemetry.get("async_pipeline_readiness"), dict) and "async_pipeline_readiness" not in audit_record:
                audit_record["async_pipeline_readiness"] = telemetry["async_pipeline_readiness"]
            if audit_mode == "full":
                self.append_audit(audit_record)
            else:
                self.append_audit(compact_context_pack_audit_record(audit_record))
        return visibility_decision

    def flush_audits(self) -> None:
        return

    def find_idempotency_record(self, key_hash: int) -> Json | None:
        for record in reversed(self.read_all()):
            if record.get("record_type") == "matrixark_idempotency" and record.get("key_hash") == key_hash:
                return record
        return None

    def append_idempotency_record(self, *, key_hash: int, tool_name: str, raw_key: str, identity: Json, response: Json) -> None:
        self.append(
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

    def ensure_backend_ready(self, *, reason: str = "matrixark") -> Json:
        return {"status": "ready", "backend": "local", "reason": reason}

    def recent_records(self, limit: int = 128) -> list[Json]:
        limit = max(1, int(limit or 1))
        records = self.read_all()
        if len(records) <= limit:
            return records
        return records[-limit:] if LOCAL_READ_CACHE_COPY else list(records[-limit:])

    def read_all(self) -> list[Json]:
        cache_key = str(self.event_log.resolve())
        paths = self._retained_jsonl_paths()
        if not paths:
            with self._read_cache_lock:
                self._read_cache_records = []
                self._read_cache_size = -1
                self._read_cache_mtime_ns = -1
                self._read_cache_source = "empty"
            with _LOCAL_READ_CACHE_LOCK:
                _LOCAL_READ_CACHE.pop(cache_key, None)
            try:
                self._durable_read_cache_path().unlink()
            except FileNotFoundError:
                pass
            return []
        signature = self._jsonl_cache_signature_detail(paths)
        size = int(signature.get("total_size", -1))
        mtime_ns = int(signature.get("max_mtime_ns", -1))
        with self._read_cache_lock:
            if (
                self._read_cache_records is not None
                and self._read_cache_size == size
                and self._read_cache_mtime_ns == mtime_ns
            ):
                self._read_cache_source = "instance"
                return list(self._read_cache_records)
        with _LOCAL_READ_CACHE_LOCK:
            cached = _LOCAL_READ_CACHE.get(cache_key)
            if cached is not None:
                cached_size, cached_mtime_ns, cached_records = cached
                if cached_size == size and cached_mtime_ns == mtime_ns:
                    records = list(cached_records)
                    with self._read_cache_lock:
                        self._read_cache_records = records
                        self._read_cache_size = size
                        self._read_cache_mtime_ns = mtime_ns
                        self._read_cache_source = "process"
                    return list(records)
                _LOCAL_READ_CACHE.pop(cache_key, None)
        durable_records = self._load_durable_read_cache(signature)
        if durable_records is not None:
            records = list(durable_records)
            with self._read_cache_lock:
                self._read_cache_records = list(records)
                self._read_cache_size = size
                self._read_cache_mtime_ns = mtime_ns
                self._read_cache_source = "durable"
            with _LOCAL_READ_CACHE_LOCK:
                _LOCAL_READ_CACHE[cache_key] = (size, mtime_ns, list(records))
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
            with self._context_pack_cache_lock:
                self._context_pack_cache.clear()
            return list(records)
        records = []
        with self._event_log_lock:
            for path in paths:
                with path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        line = line.strip()
                        if line:
                            records.append(json.loads(line))
        records = compact_latest_context_state_records(compact_latest_value_records(records))
        with self._read_cache_lock:
            cache_changed = (
                self._read_cache_records is None
                or self._read_cache_size != size
                or self._read_cache_mtime_ns != mtime_ns
            )
            self._read_cache_records = list(records)
            self._read_cache_size = size
            self._read_cache_mtime_ns = mtime_ns
            self._read_cache_source = "jsonl"
        with _LOCAL_READ_CACHE_LOCK:
            _LOCAL_READ_CACHE[cache_key] = (size, mtime_ns, list(records))
        self._write_durable_read_cache(list(records), signature, force=True)
        if cache_changed:
            with self._retrieval_records_cache_lock:
                self._retrieval_records_cache_generation += 1
                self._retrieval_records_cache.clear()
            with self._context_pack_cache_lock:
                self._context_pack_cache.clear()
        return list(records)









