# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import re
from typing import Any

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        CODEX_OUTCOME_ENTITY_TYPES,
        DEFAULT_BUSINESS_TYPE_WEIGHTS,
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        HARD_MAX_CHILDREN_SCORED_PER_PARENT,
        Json,
        MatrixArkError,
        business_score_for_candidate,
        clamp01,
        codex_outcome_fact_index_terms,
        cross_session_rerank_adjustment,
        final_recall_score,
        integer_arg,
        is_codex_outcome_entity_type,
        normalize_message_role,
        optional_object,
        session_continuity_boost,
        time_decay_score,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        CODEX_OUTCOME_ENTITY_TYPES,
        DEFAULT_BUSINESS_TYPE_WEIGHTS,
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        HARD_MAX_CHILDREN_SCORED_PER_PARENT,
        Json,
        MatrixArkError,
        business_score_for_candidate,
        clamp01,
        codex_outcome_fact_index_terms,
        cross_session_rerank_adjustment,
        final_recall_score,
        integer_arg,
        is_codex_outcome_entity_type,
        normalize_message_role,
        optional_object,
        session_continuity_boost,
        time_decay_score,
    )

__all__ = ['sharing_scope_from_candidate', 'is_shared_resource_candidate', 'is_shared_skill_candidate', 'is_pending_async_candidate', 'bounded_max_children_scored_per_parent', 'score_recall_candidate', 'numeric_field', 'apply_statistical_operator', 'latest_record', 'merge_ranked_paths', 'candidate_codex_outcome_terms', 'inferred_memory_selection_policies_for_candidate', 'candidate_memory_selection_policies', 'candidate_is_feature_profile_memory', 'memory_selection_policy_ref_boost', 'question_type_ref_boost']


def sharing_scope_from_candidate(candidate: Json) -> str:
    for source in [candidate, candidate.get("access_scope", {}), candidate.get("metadata", {}), candidate.get("scope", {})]:
        if isinstance(source, dict):
            value = str(source.get("sharing_scope") or "").strip().lower()
            if value:
                return value
    node_path = [str(part).lower() for part in candidate.get("node_path", []) if str(part)]
    if node_path[:2] == ["global", "shared"]:
        return "global_shared"
    if len(node_path) >= 2 and node_path[0].startswith("tenant:") and node_path[1] == "shared":
        return "tenant_shared"
    return "private_user"


def is_shared_resource_candidate(candidate: Json) -> bool:
    return str(candidate.get("ref_type") or "") == "resource_chunk" and sharing_scope_from_candidate(candidate) in {"tenant_shared", "global_shared"}


def is_shared_skill_candidate(candidate: Json) -> bool:
    return str(candidate.get("ref_type") or "") == "skill_section" and sharing_scope_from_candidate(candidate) in {"tenant_shared", "global_shared"}


def is_pending_async_candidate(candidate: Json) -> bool:
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    record_type = str(candidate.get("record_type") or metadata.get("record_type") or "")
    ref_type = str(candidate.get("ref_type") or metadata.get("ref_type") or "")
    if not ref_type and record_type == "context_event":
        ref_type = "event"
    if ref_type != "event":
        return False
    event_type = str(candidate.get("event_type") or metadata.get("event_type") or "").strip().lower()
    classification = str(candidate.get("classification") or metadata.get("classification") or "").strip().upper()
    extraction_status = str(candidate.get("extraction_status") or metadata.get("extraction_status") or "").strip().lower()
    extraction_mode = str(candidate.get("extraction_mode") or metadata.get("extraction_mode") or "").strip().lower()
    extraction_phase = str(candidate.get("extraction_phase") or metadata.get("extraction_phase") or "").strip().lower()
    return (
        event_type == "pending_async"
        or classification == "PENDING_ASYNC_EXTRACTION"
        or extraction_phase == "pending_async"
        or extraction_status in {"pending", "async_pending"}
        or extraction_mode == "async_pending"
    )


def bounded_max_children_scored_per_parent(value: int) -> int:
    hard_cap = max(1, HARD_MAX_CHILDREN_SCORED_PER_PARENT)
    if value > hard_cap:
        raise MatrixArkError(
            "max_children_scored_per_parent must be <= "
            f"{hard_cap}; split over-wide ContextNode children into deeper node layers"
        )
    return value


def score_recall_candidate(candidate: Json, ranking: Json, *, reference_time_ms: int) -> Json:
    freshness_tolerance_ms = integer_arg(
        ranking,
        "freshness_tolerance_ms",
        DEFAULT_TIME_DECAY_TOLERANCE_MS,
        minimum=0,
    )
    half_life_ms = integer_arg(
        ranking,
        "half_life_ms",
        DEFAULT_TIME_DECAY_HALFLIFE_MS,
        minimum=1,
    )
    type_weights = {**DEFAULT_BUSINESS_TYPE_WEIGHTS, **optional_object(ranking, "business_type_weights")}
    weights = optional_object(ranking, "weights")
    origin_score = clamp01(candidate.get("origin_score"))
    s_time = time_decay_score(
        candidate.get("updated_at_ms"),
        reference_time_ms=reference_time_ms,
        freshness_tolerance_ms=freshness_tolerance_ms,
        half_life_ms=half_life_ms,
    )
    s_busi = business_score_for_candidate(candidate, type_weights)
    continuity_boost = session_continuity_boost(candidate, str(candidate.get("question_type") or candidate.get("packing_policy") or "fact"))
    cross_session_rerank_boost = cross_session_rerank_adjustment(candidate, str(candidate.get("question_type") or candidate.get("packing_policy") or "fact"))
    final_score = clamp01(final_recall_score(origin_score, s_time, s_busi, weights) + continuity_boost + cross_session_rerank_boost)
    return {
        **candidate,
        "origin_score": origin_score,
        "time_score": s_time,
        "business_score": s_busi,
        "continuity_boost": round(continuity_boost, 6),
        "cross_session_rerank_boost": round(cross_session_rerank_boost, 6),
        "final_score": final_score,
        "score": final_score,
        "ranking_formula": "Sfinal=(1-wtime-wbusi)*Sorigin+wtime*Stime+wbusi*Sbusi+continuity_boost+cross_session_rerank_boost",
    }


def numeric_field(record: Json, field: str = "value") -> float | None:
    for source in [record, record.get("metadata", {}), record.get("envelope", {}).get("metadata", {})]:
        if not isinstance(source, dict) or field not in source:
            continue
        try:
            return float(source[field])
        except (TypeError, ValueError):
            return None
    return None


def apply_statistical_operator(operator: str, records: list[Json], *, field: str = "value") -> float | int | None:
    values = [value for record in records if (value := numeric_field(record, field)) is not None]
    op = operator.upper()
    if op == "COUNT":
        return len(records)
    if not values:
        return None
    if op == "SUM":
        return round(sum(values), 6)
    if op == "AVG":
        return round(sum(values) / len(values), 6)
    if op == "MAX":
        return max(values)
    raise MatrixArkError(f"unsupported statistical operator: {operator}")


def latest_record(records: list[Json], *, time_field: str = "updated_at_ms") -> Json | None:
    if not records:
        return None
    return max(records, key=lambda record: int(record.get(time_field) or 0))


def merge_ranked_paths(primary: list[Json], auxiliary: list[Json], *, total_limit: int, auxiliary_quota: int) -> list[Json]:
    selected: list[Json] = []
    seen: set[tuple[str, Any]] = set()

    def take(items: list[Json], limit: int) -> None:
        for item in items:
            key = (str(item.get("ref_type", "")), item.get("ref_hash"))
            if key in seen:
                continue
            selected.append(item)
            seen.add(key)
            if len(selected) >= limit:
                return

    auxiliary_quota = max(0, min(auxiliary_quota, total_limit))
    primary_quota = max(0, total_limit - auxiliary_quota)
    take(primary, primary_quota)
    take(auxiliary, total_limit)
    if len(selected) < total_limit:
        take(primary, total_limit)
    return selected[:total_limit]


def candidate_codex_outcome_terms(candidate: Json) -> set[str]:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return codex_outcome_fact_index_terms(
        candidate.get("entity_name"),
        candidate.get("entity_type"),
        candidate.get("event_type"),
        candidate.get("topic"),
        candidate.get("text"),
        metadata.get("entity_name"),
        metadata.get("entity_type"),
        metadata.get("event_type"),
    )


def inferred_memory_selection_policies_for_candidate(candidate: Json) -> set[str]:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    sources = [candidate, metadata]
    role_names: set[str] = set()
    for source in sources:
        scalar_role = normalize_message_role(source.get("role") or source.get("source_role"))
        if scalar_role:
            role_names.add(scalar_role)
        roles = source.get("source_roles")
        if isinstance(roles, list):
            role_names.update(normalize_message_role(role) for role in roles if normalize_message_role(role))
        role_counts = source.get("source_role_counts") if isinstance(source.get("source_role_counts"), dict) else {}
        for role, count in role_counts.items():
            try:
                amount = int(count or 0)
            except (TypeError, ValueError):
                amount = 0
            role_name = normalize_message_role(role)
            if role_name and amount > 0:
                role_names.add(role_name)
    entity_type = str(
        candidate.get("entity_type")
        or candidate.get("event_type")
        or metadata.get("entity_type")
        or metadata.get("event_type")
        or ""
    ).strip().lower()
    profile_memory_kind = str(candidate.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "").strip().lower()
    profile_memory_class = str(candidate.get("profile_memory_class") or metadata.get("profile_memory_class") or "").strip().lower()
    memory_scope = str(candidate.get("memory_scope") or metadata.get("memory_scope") or "").strip().lower()
    policies: set[str] = set()
    if "user" in role_names or entity_type == "user_prompt":
        policies.add("selected_user_prompt")
    if (
        "user" in role_names
        and (
            profile_memory_kind in {"durable_profile", "memory_feature"}
            or profile_memory_class in {"identity", "communication", "workspace", "memory_feature", "preference", "personal_context", "task_context"}
            or memory_scope in {"user_profile", "profile", "cross_session_profile"}
        )
    ):
        policies.add("selected_user_profile_fact")
    if "tool" in role_names or entity_type == "tool_evidence":
        policies.add("selected_tool_evidence_only")
    if "assistant" in role_names or entity_type in CODEX_OUTCOME_ENTITY_TYPES or entity_type == "assistant_decision":
        policies.add("selected_assistant_decision_outcome_only")
    if (
        "assistant" in role_names
        and (
            profile_memory_kind in {"durable_profile", "memory_feature"}
            or profile_memory_class in {"identity", "communication", "workspace", "memory_feature", "preference", "personal_context", "task_context"}
            or memory_scope in {"user_profile", "profile", "cross_session_profile"}
        )
    ):
        policies.add("selected_assistant_profile_fact")
    return policies


def candidate_memory_selection_policies(candidate: Json) -> set[str]:
    policy_names: set[str] = set()
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    for source in [candidate, metadata]:
        policies = source.get("source_memory_selection_policies")
        if isinstance(policies, list):
            policy_names.update(str(policy or "").strip() for policy in policies if str(policy or "").strip())
        counts = (
            source.get("source_memory_selection_policy_counts")
            if isinstance(source.get("source_memory_selection_policy_counts"), dict)
            else {}
        )
        for policy, count in counts.items():
            policy_name = str(policy or "").strip()
            if not policy_name:
                continue
            try:
                amount = int(count or 0)
            except (TypeError, ValueError):
                amount = 0
            if amount > 0:
                policy_names.add(policy_name)
        selection = source.get("codex_memory_selection") if isinstance(source.get("codex_memory_selection"), dict) else {}
        if isinstance(selection.get("policies"), list):
            policy_names.update(str(policy or "").strip() for policy in selection.get("policies", []) if str(policy or "").strip())
        selection_policy = str(selection.get("policy") or "").strip()
        if selection_policy:
            policy_names.add(selection_policy)
    policy_names.update(inferred_memory_selection_policies_for_candidate(candidate))
    return policy_names


def candidate_is_feature_profile_memory(candidate: Json) -> bool:
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    profile_memory_kind = str(candidate.get("profile_memory_kind") or metadata.get("profile_memory_kind") or "").strip().lower()
    profile_memory_class = str(candidate.get("profile_memory_class") or metadata.get("profile_memory_class") or "").strip().lower()
    for source in [candidate, metadata]:
        source_classes = source.get("source_profile_memory_classes")
        if isinstance(source_classes, list) and any(str(value or "").strip().lower() == "memory_feature" for value in source_classes):
            return True
        source_kinds = source.get("source_profile_memory_kinds")
        if isinstance(source_kinds, list) and any(str(value or "").strip().lower() == "memory_feature" for value in source_kinds):
            return True
    return profile_memory_kind == "memory_feature" or profile_memory_class == "memory_feature"


def memory_selection_policy_ref_boost(candidate: Json, question_type: str) -> float:
    policies = candidate_memory_selection_policies(candidate)
    if not policies:
        return 0.0
    normalized_question_type = str(question_type or "fact").strip().lower()
    boost = 0.0
    if {"selected_assistant_profile_fact", "selected_user_profile_fact"} & policies and normalized_question_type == "profile_memory":
        boost = max(boost, 0.24)
    if "selected_user_profile_fact" in policies and normalized_question_type in {"current_state", "latest", "multi_hop", "date"}:
        boost = max(boost, 0.20)
    if "selected_user_prompt" in policies and normalized_question_type in {"profile_memory", "current_state", "latest", "multi_hop", "date"}:
        boost = max(boost, 0.18)
    if "selected_assistant_decision_outcome_only" in policies and normalized_question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
        boost = max(boost, 0.22)
    if "selected_tool_evidence_only" in policies and normalized_question_type in {"evidence", "benchmark_quality", "current_state", "latest"}:
        boost = max(boost, 0.24)
    if normalized_question_type == "fact":
        if "selected_user_prompt" in policies:
            boost = max(boost, 0.08)
        if {"selected_assistant_decision_outcome_only", "selected_tool_evidence_only"} & policies:
            boost = max(boost, 0.10)
    return boost


def question_type_ref_boost(candidate: Json, question_type: str) -> float:
    ref_type = str(candidate.get("ref_type", ""))
    context_class = str(candidate.get("context_class") or ref_type)
    text = str(candidate.get("text", "")).lower()
    event_type = str(candidate.get("event_type") or candidate.get("entity_type") or candidate.get("topic") or "").lower()
    has_citation = bool(candidate.get("source_ref") or candidate.get("citation") or candidate.get("source_chunk_hash"))
    profile_memory_kind = str(candidate.get("profile_memory_kind") or "").strip().lower()
    is_feature_profile_memory = candidate_is_feature_profile_memory(candidate)
    memory_scope = str(candidate.get("memory_scope") or "").strip().lower()
    session_continuity = str(candidate.get("session_continuity") or "").strip().lower()
    policy_boost = memory_selection_policy_ref_boost(candidate, question_type)
    is_codex_outcome_compression = ref_type == "compression" and profile_memory_kind == "codex_outcome"
    if is_codex_outcome_compression and question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
        return 0.46 + policy_boost
    if is_codex_outcome_compression and question_type in {"profile_memory", "multi_hop", "date"}:
        return 0.34 + policy_boost
    if ref_type == "segment" and profile_memory_kind == "codex_outcome" and question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
        return 0.30 + policy_boost
    if ref_type == "segment" and profile_memory_kind == "codex_outcome" and question_type in {"profile_memory", "multi_hop", "date"}:
        return 0.20 + policy_boost
    if ref_type == "entity" and profile_memory_kind == "codex_outcome" and question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
        return 0.52 + policy_boost
    if ref_type == "entity" and is_codex_outcome_entity_type(event_type) and question_type in {"current_state", "latest", "evidence", "benchmark_quality"}:
        return 0.48 + policy_boost
    if question_type == "profile_memory" and is_feature_profile_memory:
        if ref_type == "entity":
            return 0.64 + policy_boost
        if ref_type in {"summary", "compression"}:
            return 0.46 + policy_boost
        if ref_type == "segment":
            return 0.30 + policy_boost
    if question_type == "profile_memory" and profile_memory_kind == "durable_profile":
        if ref_type == "entity":
            return 0.50 + policy_boost
        if ref_type in {"summary", "compression"}:
            return 0.38 + policy_boost
        if ref_type == "segment":
            return 0.24 + policy_boost
    if question_type == "profile_memory" and memory_scope == "user_profile" and session_continuity == "cross_session":
        if ref_type == "entity":
            return 0.42 + policy_boost
        if ref_type in {"summary", "compression"}:
            return 0.30 + policy_boost
    if question_type == "procedure":
        if ref_type == "skill_section":
            return 0.36
        if context_class in {"resource_fact", "resource_entity_fact"} and re.search(r"\b(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)\b", event_type + " " + text):
            return 0.34
        if ref_type == "resource_chunk" and re.search(r"\b(procedure|troubleshoot|debug|rollback|runbook|checklist|alert|fix|remediation|mitigation)\b", text):
            return 0.30
        return 0.0
    if question_type == "broad_exploration":
        if ref_type == "summary":
            return 0.35
        if ref_type in {"segment", "compression"}:
            return 0.16
        return 0.02 if ref_type in {"resource_chunk", "event", "entity"} else 0.0
    if ref_type == "compression" and question_type in {"fact", "current_state", "latest", "multi_hop"}:
        source_count = len(candidate.get("source_event_ids", []) or [])
        # Multi-event TIME_COMPRESS records should win tight fact/current/multi-hop packs
        # because they preserve an old answer-bearing window in fewer tokens.
        return 0.50 if source_count >= 2 else 0.24
    if question_type in {"current_state", "latest"}:
        if ref_type == "entity" and event_type == "assistant_decision":
            return 0.44
        if ref_type == "entity" and event_type == "tool_evidence":
            return 0.40
        if ref_type == "entity":
            return 0.30
        if context_class == "resource_entity_fact":
            return 0.28
        if context_class == "resource_fact" or "correction" in event_type or "confirmation" in event_type:
            return 0.18
        if ref_type == "resource_chunk" and has_citation:
            return 0.10
        return 0.0
    if question_type == "evidence":
        if ref_type == "entity" and event_type == "tool_evidence":
            if candidate_codex_outcome_terms(candidate):
                return 0.42 + policy_boost
            return 0.36 + policy_boost
        if ref_type == "entity" and event_type == "assistant_decision":
            return (0.50 if candidate_codex_outcome_terms(candidate) else 0.28) + policy_boost
        if ref_type == "resource_chunk" and has_citation:
            return 0.30
        if ref_type == "event":
            if event_type in {"assistant_response", "assistant_decision", "tool_evidence"} and candidate_codex_outcome_terms(candidate):
                return 0.30
            return 0.24
        return (0.05 if ref_type == "segment" else 0.0) + policy_boost
    if question_type == "date":
        if ref_type == "event" and re.search(r"\b(20\d{2}|19\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|monday|tuesday|wednesday|thursday|friday|saturday|sunday|before|after|on)\b", text):
            return 0.28 + policy_boost
        return (0.08 if ref_type == "entity" else 0.0) + policy_boost
    if question_type == "multi_hop":
        return (0.14 if ref_type in {"entity", "segment"} else 0.04) + policy_boost
    if question_type == "why_emotion":
        return 0.18 if re.search(r"\b(because|reason|felt|feel|happy|sad|angry|worried|excited|concerned)\b", text) else 0.0
    if question_type == "fact":
        negated_approval = bool(
            re.search(r"\b(no|not|without|missing|lacks?|lacked)\b.{0,48}\b(approval|approved|decision)\b", text)
            or re.search(r"\b(approval|approved|decision)\b.{0,48}\b(no|not|without|missing|lacks?|lacked)\b", text)
        )
        affirmative_approval = bool(re.search(r"\b(approved|approval granted|approval confirmed|confirmed approval)\b", text))
        if negated_approval and not affirmative_approval:
            return -0.12
        if affirmative_approval:
            return (0.38 if ref_type in {"event", "entity"} else 0.26) + policy_boost
        if context_class in {"resource_fact", "resource_entity_fact"}:
            return 0.30
        if ref_type in {"entity", "event"}:
            return 0.18 + policy_boost
        if ref_type == "resource_chunk" and has_citation:
            return 0.06
        return 0.03 + policy_boost
    return policy_boost


