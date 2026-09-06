# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
from typing import Any, Callable

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        Json,
        candidate_memory_layer_name,
        candidate_memory_selection_policies,
        clamp01,
        context_text_hashes,
        is_codex_outcome_entity_type,
        is_pending_async_candidate,
        is_resource_or_skill_candidate,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        normalize_message_role,
        packing_sort_key,
        semantic_source_role_for_entity_type,
        stable_hash,
        token_count,
        tokens,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        Json,
        candidate_memory_layer_name,
        candidate_memory_selection_policies,
        clamp01,
        context_text_hashes,
        is_codex_outcome_entity_type,
        is_pending_async_candidate,
        is_resource_or_skill_candidate,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        normalize_message_role,
        packing_sort_key,
        semantic_source_role_for_entity_type,
        stable_hash,
        token_count,
        tokens,
    )

__all__ = ['dropped_candidate_audit_ref', 'record_dropped_candidate', 'diversify_for_question_type', 'entity_current_state_key', 'prefer_profile_entities_for_current_state', 'is_stale_or_superseded_candidate', 'suppress_profile_shadowed_session_entity_refs', 'normalized_token_set', 'near_duplicate_overlap_ratio', 'clamp_refs_to_token_budget', 'select_token_budgeted_refs']


# `normalized_token_set` and `near_duplicate_overlap_ratio` are not defined here any more. They
# moved to matrixark_mcp_scoring so matrixark_mcp_budget_pack can use them too: the gateway reaches
# that packer, and it had no near-duplicate suppression at all while the setting that governs it,
# MATRIXARK_NEAR_DUPLICATE_OVERLAP_THRESHOLD, is offered by matrixark_gateway_config and defaults
# to 0.85 -- on. This module could not be the shared home because it imports matrixark_mcp_core.
try:
    from tools.matrixark_mcp_scoring import (
        near_duplicate_overlap_ratio,
        normalized_token_set,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_scoring import (
        near_duplicate_overlap_ratio,
        normalized_token_set,
    )


def clamp_refs_to_token_budget(
    refs: list[Json],
    max_context_tokens: int,
    *,
    reserved_tokens: int = 0,
) -> tuple[list[Json], list[Json], int]:
    """Enforce the token ceiling on an already ranked list of refs.

    ``refs`` must be ordered best-first. Keeps the highest-ranked prefix whose
    cumulative token estimate fits within ``max_context_tokens - reserved_tokens``
    and returns ``(kept, trimmed, used_tokens)``. Ceiling-not-target: a short
    ranked list that already fits is returned unchanged (never padded). At least
    the single top ref is kept when the list is non-empty so a relevant pack is
    never emptied by an unusually tight budget. This is the symmetric partner to
    the in-loop budget check in ``select_token_budgeted_refs`` and is applied to
    packs assembled outside that loop (e.g. the native backend pack path), which
    otherwise could return a pack exceeding the caller's budget.
    """
    remote_budget = max(0, int(max_context_tokens) - max(0, int(reserved_tokens)))
    kept: list[Json] = []
    trimmed: list[Json] = []
    used_tokens = 0
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        ref_tokens = ref.get("token_count")
        if ref_tokens is None:
            ref_tokens = ref.get("token_estimate")
        if ref_tokens is None:
            ref_tokens = token_count(str(ref.get("text", "")))
        try:
            ref_tokens = max(1, int(ref_tokens))
        except (TypeError, ValueError):
            ref_tokens = max(1, token_count(str(ref.get("text", ""))))
        # Always keep the top ref so a relevant pack is not zeroed by a tight budget.
        if kept and (remote_budget <= 0 or used_tokens + ref_tokens > remote_budget):
            trimmed.append(ref)
            continue
        kept.append(ref)
        used_tokens += ref_tokens
    return kept, trimmed, used_tokens


# `record_dropped_candidate` is not defined here. It was, with a WIDER rule than the copy in
# matrixark_mcp_recall_scoring -- that one recorded only a resource/skill candidate or a stale
# entity, so an audit built there counted drops it never explained. The two had also drifted the
# other way: its `dropped_candidate_audit_ref` emits six fields this module's did not
# (entity_name, entity_type, memory_scope, session_continuity, and the two profile_shadowed_*).
# Neither was the complete one, so the surviving pair takes the wider rule and the richer record.
#
# It lives there rather than here because that module does not import matrixark_mcp_core and this
# one does; matrixark_mcp_budget_pack, which the gateway reaches, already resolved both names
# there.
try:
    from tools.matrixark_mcp_recall_scoring import (
        dropped_candidate_audit_ref,
        record_dropped_candidate,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_recall_scoring import (
        dropped_candidate_audit_ref,
        record_dropped_candidate,
    )


def diversify_for_question_type(candidates: list[Json], question_type: str, *, total_limit: int) -> list[Json]:
    if question_type == "broad_exploration":
        summary = next((candidate for candidate in candidates if candidate.get("ref_type") == "summary"), None)
        if summary is None:
            return candidates[:total_limit]
        selected = [summary]
        selected.extend(candidate for candidate in candidates if candidate is not summary)
        return selected[:total_limit]
    if question_type != "multi_hop":
        return candidates[:total_limit]
    selected: list[Json] = []
    deferred: list[Json] = []
    seen_nodes: set[Any] = set()
    for candidate in candidates:
        node_hash = candidate.get("node_hash")
        if node_hash not in seen_nodes:
            selected.append(candidate)
            seen_nodes.add(node_hash)
        else:
            deferred.append(candidate)
        if len(selected) >= total_limit:
            return selected
    selected.extend(deferred)
    return selected[:total_limit]


def entity_current_state_key(candidate: Json) -> tuple[str, str] | None:
    if str(candidate.get("ref_type") or "") != "entity":
        return None
    metadata = candidate.get("metadata", {}) if isinstance(candidate.get("metadata"), dict) else {}
    entity_type = str(candidate.get("entity_type") or metadata.get("entity_type") or "").strip().lower()
    entity_name = str(candidate.get("entity_name") or metadata.get("entity_name") or "").strip().lower()
    if not entity_type or not entity_name:
        return None
    return entity_type, entity_name


def prefer_profile_entities_for_current_state(candidates: list[Json], question_type: str) -> list[Json]:
    if question_type not in {"current_state", "latest", "profile_memory"}:
        return candidates
    latest_profile_by_entity: dict[tuple[str, str], Json] = {}
    latest_profile_by_source_entity_hash: dict[Any, Json] = {}
    for candidate in candidates:
        key = entity_current_state_key(candidate)
        if key is None:
            continue
        if str(candidate.get("memory_scope") or "") != "user_profile":
            continue
        if str(candidate.get("session_continuity") or "") != "cross_session":
            continue
        existing = latest_profile_by_entity.get(key)
        if existing is None or int(candidate.get("updated_at_ms") or 0) >= int(existing.get("updated_at_ms") or 0):
            latest_profile_by_entity[key] = candidate
        for source_entity_hash in candidate.get("source_entity_hashes", []):
            existing_by_source = latest_profile_by_source_entity_hash.get(source_entity_hash)
            if existing_by_source is None or int(candidate.get("updated_at_ms") or 0) >= int(existing_by_source.get("updated_at_ms") or 0):
                latest_profile_by_source_entity_hash[source_entity_hash] = candidate
    if not latest_profile_by_entity:
        return candidates
    adjusted: list[Json] = []
    for candidate in candidates:
        key = entity_current_state_key(candidate)
        profile = latest_profile_by_source_entity_hash.get(candidate.get("ref_hash"))
        if profile is None and key is not None:
            profile = latest_profile_by_entity.get(key)
        if profile is None:
            adjusted.append(candidate)
            continue
        if candidate is profile or candidate.get("ref_hash") == profile.get("ref_hash"):
            adjusted.append({
                **candidate,
                "score": clamp01(float(candidate.get("score", 0.0)) + 0.18),
                "profile_current_state_boost": 0.18,
                "selection_reason": candidate.get("selection_reason") or "current profile entity preferred over session-local historical state",
            })
            continue
        if str(candidate.get("memory_scope") or "") == "session":
            adjusted.append({
                **candidate,
                "stale_or_superseded": True,
                "profile_shadowed_by_ref_hash": profile.get("ref_hash"),
                "profile_shadowed_reason": (
                    "source_entity_lineage"
                    if candidate.get("ref_hash") in set(profile.get("source_entity_hashes", []))
                    else "same_entity_identity"
                ),
                "selection_reason": candidate.get("selection_reason") or "session-local entity kept as historical evidence behind current profile state",
            })
            continue
        adjusted.append(candidate)
    return adjusted


def is_stale_or_superseded_candidate(candidate: Json) -> bool:
    if bool(candidate.get("stale_or_superseded") or candidate.get("stale")):
        return True
    if candidate.get("superseded_by_ref_hash") or candidate.get("superseded_by_entity_hash"):
        return True
    version_state = str(candidate.get("version_state") or candidate.get("current_state_policy") or "").strip().lower()
    return version_state in {"stale", "superseded", "historical_superseded"}


def suppress_profile_shadowed_session_entity_refs(selected: list[Json], dropped: Json) -> tuple[list[Json], int]:
    profile_source_hashes: set[Any] = set()
    profile_identity_keys: set[tuple[str, str]] = set()
    for item in selected:
        if (
            item.get("ref_type") != "entity"
            or item.get("memory_scope") != "user_profile"
            or item.get("session_continuity") != "cross_session"
        ):
            continue
        entity_type = str(item.get("entity_type") or "").strip().lower()
        if is_codex_outcome_entity_type(entity_type) or str(item.get("profile_memory_kind") or "").strip().lower() == "codex_outcome":
            continue
        source_hashes = item.get("source_entity_hashes")
        if isinstance(source_hashes, list):
            profile_source_hashes.update(value for value in source_hashes if value not in (None, ""))
        entity_name = str(item.get("entity_name") or "").strip().lower()
        if entity_type and entity_name:
            profile_identity_keys.add((entity_type, entity_name))
    if not profile_source_hashes and not profile_identity_keys:
        return selected, 0

    kept: list[Json] = []
    removed_tokens = 0
    removed_count = 0
    for item in selected:
        if (
            item.get("ref_type") == "entity"
            and item.get("memory_scope") == "session"
            and item.get("session_continuity") == "same_session"
        ):
            entity_type = str(item.get("entity_type") or "").strip().lower()
            if is_codex_outcome_entity_type(entity_type) or str(item.get("profile_memory_kind") or "").strip().lower() == "codex_outcome":
                kept.append(item)
                continue
            identity_key = (entity_type, str(item.get("entity_name") or "").strip().lower())
            represented_by_profile = item.get("ref_hash") in profile_source_hashes or (
                bool(identity_key[0] and identity_key[1]) and identity_key in profile_identity_keys
            )
            if represented_by_profile:
                token_estimate = int(item.get("token_estimate") or max(1, token_count(str(item.get("text") or ""))))
                removed_tokens += token_estimate
                removed_count += 1
                record_dropped_candidate(
                    dropped,
                    {
                        **item,
                        "stale_or_superseded": True,
                        "profile_shadowed_reason": "selected_profile_entity_supersedes_session_entity",
                    },
                    reason="stale",
                    token_estimate=token_estimate,
                )
                continue
        kept.append(item)
    if removed_count and kept:
        dropped["stale"] = int(dropped.get("stale") or 0) + removed_count
        dropped["profile_entity_shadowed_session_entities"] = (
            int(dropped.get("profile_entity_shadowed_session_entities") or 0) + removed_count
        )
        dropped.setdefault("estimated_tokens", {}).setdefault("stale", 0)
        dropped["estimated_tokens"]["stale"] = int(dropped["estimated_tokens"].get("stale") or 0) + removed_tokens
        return kept, removed_tokens
    return selected, 0


def select_token_budgeted_refs(
    primary: list[Json],
    auxiliary: list[Json],
    *,
    max_context_tokens: int,
    auxiliary_quota: int,
    question_type: str = "fact",
    reserved_tokens: int = 0,
    max_selected_refs: int | None = None,
    min_score: float = DEFAULT_RETRIEVAL_MIN_SCORE,
    max_global_candidates: int | None = None,
    budget_fill_policy: str = DEFAULT_BUDGET_FILL_POLICY,
    duplicate_text_hashes: set[int] | None = None,
    near_duplicate_overlap_threshold: float = DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD,
    deadline_exceeded: Callable[[], bool] | None = None,
    deadline_reason: str = "deadline_during_pack",
    cross_session_policy: Json | None = None,
    shared_context_policy: Json | None = None,
    source_role_budget_tokens: Json | None = None,
    memory_layer_budget_tokens: Json | None = None,
    memory_selection_policy_budget_tokens: Json | None = None,
    extraction_phase_budget_tokens: Json | None = None,
) -> tuple[list[Json], int, Json]:
    duplicate_text_hashes = duplicate_text_hashes or set()
    normalized_question_type = str(question_type or "fact").strip().lower()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
    candidate_pool_limit = max(
        selected_ref_cap,
        max(1, int(max_global_candidates or DEFAULT_MAX_GLOBAL_CANDIDATES)),
    )
    min_score = max(0.0, min(1.0, float(min_score)))
    budget_fill_policy = (budget_fill_policy or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
    try:
        near_duplicate_overlap_threshold = float(near_duplicate_overlap_threshold)
    except (TypeError, ValueError):
        near_duplicate_overlap_threshold = DEFAULT_NEAR_DUPLICATE_OVERLAP_THRESHOLD
    # <= 0 disables near-dup suppression; > 1 can never trigger (overlap ratio is
    # bounded at 1.0), so clamp to a sane [0, 1] operating range.
    near_duplicate_overlap_threshold = max(0.0, min(1.0, near_duplicate_overlap_threshold))
    near_duplicate_enabled = near_duplicate_overlap_threshold > 0.0
    selected_token_sets: list[frozenset[str]] = []
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=candidate_pool_limit,
        auxiliary_quota=auxiliary_quota,
    )
    candidates = prefer_profile_entities_for_current_state(candidates, question_type)
    candidates.sort(key=lambda item: packing_sort_key(item, question_type), reverse=True)
    candidates = diversify_for_question_type(candidates, question_type, total_limit=candidate_pool_limit)
    selected: list[Json] = []
    used_tokens = 0
    cross_session_policy = cross_session_policy or {"enabled": False, "budget_tokens": 0, "max_sessions": 0, "max_candidates": 0, "min_entity_bridge_refs": 0}
    cross_enabled = bool(cross_session_policy.get("enabled"))
    cross_budget_tokens = int(cross_session_policy.get("budget_tokens") or 0)
    cross_max_sessions = int(cross_session_policy.get("max_sessions") or 0)
    cross_max_candidates = int(cross_session_policy.get("max_candidates") or 0)
    cross_min_score = max(0.0, min(1.0, float(cross_session_policy.get("min_score") or 0.0)))
    cross_raw_evidence_min_score = max(0.0, min(1.0, float(cross_session_policy.get("raw_evidence_min_score") or 0.0)))
    cross_min_entity_bridge_refs = int(cross_session_policy.get("min_entity_bridge_refs") or 0)
    shared_context_policy = shared_context_policy or {"enabled": False, "resource_budget_tokens": 0, "skill_budget_tokens": 0, "min_score": 0.0}
    shared_enabled = bool(shared_context_policy.get("enabled"))
    shared_resource_budget_tokens = int(shared_context_policy.get("resource_budget_tokens") or 0)
    shared_skill_budget_tokens = int(shared_context_policy.get("skill_budget_tokens") or 0)
    shared_min_score = max(0.0, min(1.0, float(shared_context_policy.get("min_score") or 0.0)))
    cross_used_tokens = 0
    cross_selected_ref_count = 0
    entity_bridge_selected_ref_count = 0
    shared_resource_used_tokens = 0
    shared_skill_used_tokens = 0
    shared_resource_selected_ref_count = 0
    shared_skill_selected_ref_count = 0
    source_role_budget_tokens = source_role_budget_tokens if isinstance(source_role_budget_tokens, dict) else {}
    normalized_source_role_budget_tokens: Json = {}
    for role, budget in source_role_budget_tokens.items():
        role_name = normalize_message_role(role)
        if not role_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_source_role_budget_tokens[role_name] = budget_tokens
    source_role_used_tokens: Json = {role: 0 for role in normalized_source_role_budget_tokens}
    source_role_selected_ref_counts: Json = {role: 0 for role in normalized_source_role_budget_tokens}
    memory_layer_budget_tokens = memory_layer_budget_tokens if isinstance(memory_layer_budget_tokens, dict) else {}
    normalized_memory_layer_budget_tokens: Json = {}
    for layer, budget in memory_layer_budget_tokens.items():
        layer_name = str(layer or "").strip().lower()
        if not layer_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_memory_layer_budget_tokens[layer_name] = budget_tokens
    memory_layer_used_tokens: Json = {layer: 0 for layer in normalized_memory_layer_budget_tokens}
    memory_layer_selected_ref_counts: Json = {layer: 0 for layer in normalized_memory_layer_budget_tokens}
    memory_selection_policy_budget_tokens = (
        memory_selection_policy_budget_tokens if isinstance(memory_selection_policy_budget_tokens, dict) else {}
    )
    normalized_memory_selection_policy_budget_tokens: Json = {}
    for policy, budget in memory_selection_policy_budget_tokens.items():
        policy_name = str(policy or "").strip()
        if not policy_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_memory_selection_policy_budget_tokens[policy_name] = budget_tokens
    memory_selection_policy_used_tokens: Json = {
        policy: 0 for policy in normalized_memory_selection_policy_budget_tokens
    }
    memory_selection_policy_selected_ref_counts: Json = {
        policy: 0 for policy in normalized_memory_selection_policy_budget_tokens
    }
    extraction_phase_budget_tokens = extraction_phase_budget_tokens if isinstance(extraction_phase_budget_tokens, dict) else {}
    normalized_extraction_phase_budget_tokens: Json = {}
    for phase, budget in extraction_phase_budget_tokens.items():
        phase_name = str(phase or "").strip().lower()
        if not phase_name:
            continue
        try:
            budget_tokens = max(0, int(budget or 0))
        except (TypeError, ValueError):
            continue
        normalized_extraction_phase_budget_tokens[phase_name] = budget_tokens
    extraction_phase_used_tokens: Json = {phase: 0 for phase in normalized_extraction_phase_budget_tokens}
    extraction_phase_selected_ref_counts: Json = {phase: 0 for phase in normalized_extraction_phase_budget_tokens}
    profile_entity_bridge_layers = {
        "same_session_memory_feature_entity",
        "profile_entity",
        "cross_session_codex_outcome_entity",
        "cross_session_memory_feature_entity",
    }
    codex_outcome_evidence_layers = {
        "same_session_codex_outcome_event",
        "cross_session_codex_outcome_event",
        "same_session_codex_outcome_segment",
        "cross_session_codex_outcome_segment",
    }
    high_level_profile_memory_layers = {
        "summary",
        "profile_summary",
        "cross_session_codex_outcome_summary",
        "cross_session_memory_feature_summary",
        "same_session_summary",
        "same_session_memory_feature_summary",
        "cross_session_summary",
        "profile_compression",
        "cross_session_codex_outcome_compression",
        "cross_session_memory_feature_compression",
        "same_session_memory_feature_compression",
    }
    summary_profile_entity_floor_enabled = bool(
        any(normalized_memory_layer_budget_tokens.get(layer) for layer in profile_entity_bridge_layers)
        and any(normalized_memory_layer_budget_tokens.get(layer) for layer in high_level_profile_memory_layers)
    )
    profile_overview_floor_enabled = bool(
        normalized_question_type == "profile_memory"
        and selected_ref_cap > 1
        and any(normalized_memory_layer_budget_tokens.get(layer) for layer in high_level_profile_memory_layers)
    )
    cross_session_profile_entity_floor_enabled = bool(
        cross_enabled
        and cross_min_entity_bridge_refs > 0
    )
    codex_outcome_evidence_floor_enabled = bool(
        any(normalized_memory_layer_budget_tokens.get(layer) for layer in codex_outcome_evidence_layers)
        and any(normalized_memory_layer_budget_tokens.get(layer) for layer in high_level_profile_memory_layers)
    )

    def profile_entity_floor_satisfied() -> bool:
        return any(int(memory_layer_selected_ref_counts.get(layer, 0) or 0) > 0 for layer in profile_entity_bridge_layers) or any(
            candidate_memory_layer_name(item) in profile_entity_bridge_layers
            for item in selected
        )

    def profile_overview_floor_satisfied() -> bool:
        return any(int(memory_layer_selected_ref_counts.get(layer, 0) or 0) > 0 for layer in high_level_profile_memory_layers) or any(
            candidate_memory_layer_name(item) in high_level_profile_memory_layers
            for item in selected
        )

    def remaining_profile_overview_floor_layer(start_index: int) -> str:
        for remaining in candidates[start_index:]:
            remaining_layer = candidate_memory_layer_name(remaining)
            if remaining_layer not in high_level_profile_memory_layers:
                continue
            try:
                remaining_score = float(remaining.get("score", 0.0))
            except (TypeError, ValueError):
                remaining_score = 0.0
            if remaining_score < min_score:
                continue
            remaining_tokens = max(1, token_count(str(remaining.get("text", ""))))
            if remote_budget <= 0 or (selected and used_tokens + remaining_tokens > remote_budget):
                continue
            if remaining.get("session_continuity") == "cross_session" and not cross_enabled:
                continue
            layer_budget = int(normalized_memory_layer_budget_tokens.get(remaining_layer, 0) or 0)
            if layer_budget > 0 and int(memory_layer_used_tokens.get(remaining_layer, 0) or 0) + remaining_tokens <= layer_budget:
                return remaining_layer
        return ""

    def remaining_profile_entity_floor_layer(start_index: int) -> str:
        for remaining in candidates[start_index:]:
            remaining_layer = candidate_memory_layer_name(remaining)
            if remaining_layer not in profile_entity_bridge_layers:
                continue
            try:
                remaining_score = float(remaining.get("score", 0.0))
            except (TypeError, ValueError):
                remaining_score = 0.0
            if remaining_score < min_score:
                continue
            if question_type in {"current_state", "latest", "profile_memory"} and is_stale_or_superseded_candidate(remaining):
                continue
            remaining_tokens = max(1, token_count(str(remaining.get("text", ""))))
            if remote_budget <= 0 or (selected and used_tokens + remaining_tokens > remote_budget):
                continue
            if remaining.get("session_continuity") == "cross_session" and not cross_enabled:
                continue
            return remaining_layer
        return ""

    def codex_outcome_evidence_floor_satisfied() -> bool:
        return any(int(memory_layer_selected_ref_counts.get(layer, 0) or 0) > 0 for layer in codex_outcome_evidence_layers) or any(
            candidate_memory_layer_name(item) in codex_outcome_evidence_layers
            for item in selected
        )

    def remaining_codex_outcome_evidence_floor_layer(start_index: int) -> str:
        for remaining in candidates[start_index:]:
            remaining_layer = candidate_memory_layer_name(remaining)
            if remaining_layer not in codex_outcome_evidence_layers:
                continue
            try:
                remaining_score = float(remaining.get("score", 0.0))
            except (TypeError, ValueError):
                remaining_score = 0.0
            if remaining_score < min_score:
                continue
            if question_type in {"current_state", "latest", "profile_memory"} and is_stale_or_superseded_candidate(remaining):
                continue
            remaining_tokens = max(1, token_count(str(remaining.get("text", ""))))
            if remote_budget <= 0 or (selected and used_tokens + remaining_tokens > remote_budget):
                continue
            layer_budget = int(normalized_memory_layer_budget_tokens.get(remaining_layer, 0) or 0)
            if layer_budget > 0 and int(memory_layer_used_tokens.get(remaining_layer, 0) or 0) + remaining_tokens <= layer_budget:
                return remaining_layer
        return ""

    def candidate_source_role_names(candidate: Json) -> set[str]:
        role_names: set[str] = set()
        sources = [candidate]
        metadata = candidate.get("metadata")
        if isinstance(metadata, dict):
            sources.append(metadata)
        for source in sources:
            scalar_role = normalize_message_role(source.get("source_role"))
            if scalar_role:
                role_names.add(scalar_role)
            roles = source.get("source_roles")
            if isinstance(roles, list):
                role_names.update(normalize_message_role(role) for role in roles if normalize_message_role(role))
        metadata_entity_type = metadata.get("entity_type") if isinstance(metadata, dict) else ""
        entity_type = str(candidate.get("entity_type") or metadata_entity_type or "").strip().lower()
        # Only an entity type it actually HAS can speak for a candidate. An unknown one maps to
        # "durable_profile", so an empty one used to answer "profile" and that answer replaced
        # the roles the candidate declared. Summaries carry no entity type, so every summary
        # budgeted as profile and a per-role budget could never reach one -- an assistant budget
        # of one token let a twelve-token assistant summary through untouched.
        semantic_role = semantic_source_role_for_entity_type(entity_type, role_names) if entity_type else ""
        if semantic_role == "profile":
            return {semantic_role}
        if semantic_role and semantic_role in role_names:
            return {semantic_role}
        for source in sources:
            source_counts = source.get("source_role_counts") if isinstance(source.get("source_role_counts"), dict) else {}
            for role, count in source_counts.items():
                role_name = normalize_message_role(role)
                if not role_name:
                    continue
                try:
                    source_count = int(count or 0)
                except (TypeError, ValueError):
                    source_count = 0
                if source_count > 0:
                    role_names.add(role_name)
        if semantic_role and semantic_role in role_names:
            return {semantic_role}
        return role_names

    def candidate_budget_source_role_counts(candidate: Json, role_names: set[str]) -> Json:
        metadata = candidate.get("metadata")
        metadata_entity_type = metadata.get("entity_type") if isinstance(metadata, dict) else ""
        entity_type = str(candidate.get("entity_type") or metadata_entity_type or "").strip().lower()
        # Same rule as candidate_source_role_names above, so the counts describe the roles the
        # budget actually charged.
        semantic_role = semantic_source_role_for_entity_type(entity_type, role_names) if entity_type else ""
        if semantic_role == "profile":
            return {semantic_role: 1}
        if semantic_role and semantic_role in role_names:
            return {semantic_role: 1}
        normalized_source_counts: Json = {}
        sources = [candidate]
        if isinstance(metadata, dict):
            sources.append(metadata)
        for source in sources:
            scalar_role = normalize_message_role(source.get("source_role"))
            if scalar_role:
                normalized_source_counts[scalar_role] = max(1, int(normalized_source_counts.get(scalar_role, 0) or 0))
            source_counts = source.get("source_role_counts") if isinstance(source.get("source_role_counts"), dict) else {}
            for role, count in source_counts.items():
                role_name = normalize_message_role(role)
                if not role_name:
                    continue
                try:
                    normalized_source_counts[role_name] = int(normalized_source_counts.get(role_name, 0)) + max(0, int(count or 0))
                except (TypeError, ValueError):
                    continue
        result: Json = {}
        for role in sorted(role_names):
            try:
                source_count = max(0, int(normalized_source_counts.get(role, 0) or 0))
            except (TypeError, ValueError):
                source_count = 0
            result[role] = source_count if source_count > 0 else 1
        return result

    def candidate_memory_selection_policy_names(candidate: Json) -> set[str]:
        return candidate_memory_selection_policies(candidate)

    def candidate_extraction_phase_name(candidate: Json) -> str:
        phase = str(candidate.get("extraction_phase") or "").strip().lower()
        if not phase and isinstance(candidate.get("metadata"), dict):
            phase = str(candidate.get("metadata", {}).get("extraction_phase") or "").strip().lower()
        if not phase and is_pending_async_candidate(candidate):
            phase = "pending_async"
        return phase or "unknown"
    selected_cross_sessions: set[str] = set()
    def cross_session_key(candidate: Json) -> str:
        for source in [candidate, candidate.get("access_scope", {}), candidate.get("scope", {}), candidate.get("metadata", {}).get("access_scope", {}) if isinstance(candidate.get("metadata"), dict) else {}]:
            if isinstance(source, dict):
                for field in ["session_id", "scope_key", "node_hash"]:
                    value = source.get(field)
                    if value:
                        return str(value)
        return "unknown_cross_session"

    dropped: Json = {
        "over_budget": 0,
        "duplicate": 0,
        "low_score": 0,
        "stale": 0,
        "summary": 0,
        "raw_l2": 0,
        "near_duplicate": 0,
        "cross_session_budget": 0,
        "cross_session_session_cap": 0,
        "cross_session_candidate_cap": 0,
        "shared_resource_budget": 0,
        "shared_skill_budget": 0,
        "source_role_budget": 0,
        "memory_layer_budget": 0,
        "memory_selection_policy_budget": 0,
        "extraction_phase_budget": 0,
        "memory_layer_floor": 0,
        "deadline": 0,
        "max_selected_refs": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "near_duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
            "near_duplicate": 0,
            "cross_session_budget": 0,
            "cross_session_session_cap": 0,
            "cross_session_candidate_cap": 0,
            "shared_resource_budget": 0,
            "shared_skill_budget": 0,
            "source_role_budget": 0,
            "memory_layer_budget": 0,
            "memory_selection_policy_budget": 0,
            "extraction_phase_budget": 0,
            "memory_layer_floor": 0,
            "deadline": 0,
            "max_selected_refs": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "near_duplicate": "candidate text near-duplicated a higher-ranked already selected ref (token overlap above the configured threshold)",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
            "cross_session_budget": "cross-session candidate exceeded the configured cross-session token budget",
            "cross_session_session_cap": "cross-session candidate came from a session beyond max cross-session session fanout",
            "cross_session_candidate_cap": "cross-session candidate exceeded the configured cross-session candidate cap",
            "shared_resource_budget": "shared resource candidate exceeded the configured shared-resource token budget",
            "shared_skill_budget": "shared skill candidate exceeded the configured shared-skill token budget",
            "source_role_budget": "candidate exceeded a configured source-role token budget",
            "memory_layer_budget": "candidate exceeded a configured memory-layer token budget",
            "memory_selection_policy_budget": "candidate exceeded a configured memory-selection-policy token budget",
            "extraction_phase_budget": "candidate exceeded a configured extraction-phase token budget",
            "memory_layer_floor": "candidate was deferred so a required profile overview or lower-layer entity could be selected first",
            "deadline": "candidate was not packed because the hard retrieval deadline was reached",
            "max_selected_refs": "candidate was relevant but dropped because max_selected_refs was reached",
        },
        "refs": [],
        "deadline_exceeded": False,
        "deadline_reason": "",
        "min_score": min_score,
        "budget_fill_policy": budget_fill_policy,
    }
    seen_text_hashes: set[int] = set()
    for index, candidate in enumerate(candidates):
        if len(selected) >= selected_ref_cap:
            remaining_candidates = candidates[index:]
            for skipped in remaining_candidates:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                if question_type in {"current_state", "latest", "profile_memory"} and bool(skipped.get("stale_or_superseded")):
                    dropped["stale"] += 1
                    dropped["estimated_tokens"]["stale"] += skipped_tokens
                    record_dropped_candidate(dropped, skipped, reason="stale", token_estimate=skipped_tokens)
                else:
                    dropped["max_selected_refs"] += 1
                    dropped["estimated_tokens"]["max_selected_refs"] += skipped_tokens
                    record_dropped_candidate(dropped, skipped, reason="max_selected_refs", token_estimate=skipped_tokens)
            break
        if deadline_exceeded is not None and deadline_exceeded():
            dropped["deadline_exceeded"] = True
            dropped["deadline_reason"] = deadline_reason
            remaining = max(0, len(candidates) - index)
            dropped["deadline"] += remaining
            for skipped in candidates[index:]:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
                dropped["estimated_tokens"]["deadline"] += skipped_tokens
                record_dropped_candidate(dropped, skipped, reason="deadline", token_estimate=skipped_tokens)
            break
        ref_tokens = max(1, token_count(str(candidate.get("text", ""))))
        candidate_text_hashes = context_text_hashes(str(candidate.get("text", "")))
        if candidate_text_hashes.intersection(duplicate_text_hashes):
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        text_hash = stable_hash(str(candidate.get("text", ""))[:512])
        if text_hash in seen_text_hashes:
            dropped["duplicate"] += 1
            dropped["estimated_tokens"]["duplicate"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="duplicate", token_estimate=ref_tokens)
            continue
        if float(candidate.get("score", 0.0)) < min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if question_type in {"current_state", "latest", "profile_memory"} and is_stale_or_superseded_candidate(candidate):
            dropped["stale"] += 1
            dropped["estimated_tokens"]["stale"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="stale", token_estimate=ref_tokens)
            continue
        # Near-duplicate suppression: drop a candidate whose normalized token set
        # overlaps an already-selected (strictly higher-ranked, since selection is
        # in ranked order) ref above the configured threshold, so repetitive refs
        # do not dilute precision or spend budget on redundant content. The
        # highest-ranked instance is the one already in `selected`, so it is kept.
        if near_duplicate_enabled and selected_token_sets:
            candidate_token_set = normalized_token_set(candidate.get("text", ""))
            if candidate_token_set and any(
                near_duplicate_overlap_ratio(candidate_token_set, selected_tokens) >= near_duplicate_overlap_threshold
                for selected_tokens in selected_token_sets
            ):
                dropped["near_duplicate"] += 1
                dropped["estimated_tokens"]["near_duplicate"] += ref_tokens
                record_dropped_candidate(dropped, candidate, reason="near_duplicate", token_estimate=ref_tokens)
                continue
        if remote_budget <= 0 or (selected and used_tokens + ref_tokens > remote_budget):
            dropped["over_budget"] += 1
            dropped["estimated_tokens"]["over_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="over_budget", token_estimate=ref_tokens)
            continue
        is_cross_session = candidate.get("session_continuity") == "cross_session"
        ref_type = str(candidate.get("ref_type") or "")
        is_entity_bridge = is_cross_session and ref_type == "entity"
        is_cross_session_raw_evidence = is_cross_session and ref_type in {"event", "segment"}
        candidate_score = float(candidate.get("score", 0.0))
        candidate_cross_key = cross_session_key(candidate) if is_cross_session else ""
        candidate_memory_layer = candidate_memory_layer_name(candidate)
        is_broad_profile_summary = (
            candidate_memory_layer in high_level_profile_memory_layers
            and normalized_question_type in {"broad_exploration", "profile_memory"}
        )
        if is_cross_session and not cross_enabled:
            dropped["cross_session_budget"] += 1
            dropped["estimated_tokens"]["cross_session_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_budget", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_min_score > 0.0 and candidate_score < cross_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_cross_session_raw_evidence and cross_raw_evidence_min_score > 0.0 and candidate_score < cross_raw_evidence_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if (
            is_cross_session
            and cross_max_candidates > 0
            and cross_selected_ref_count >= cross_max_candidates
            and not is_broad_profile_summary
        ):
            dropped["cross_session_candidate_cap"] += 1
            dropped["estimated_tokens"]["cross_session_candidate_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_candidate_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_max_sessions > 0 and candidate_cross_key not in selected_cross_sessions and len(selected_cross_sessions) >= cross_max_sessions:
            dropped["cross_session_session_cap"] += 1
            dropped["estimated_tokens"]["cross_session_session_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_session_cap", token_estimate=ref_tokens)
            continue
        if (
            is_cross_session
            and cross_budget_tokens > 0
            and cross_used_tokens + ref_tokens > cross_budget_tokens
            and not (is_entity_bridge and entity_bridge_selected_ref_count < cross_min_entity_bridge_refs)
            and not is_broad_profile_summary
        ):
            dropped["cross_session_budget"] += 1
            dropped["estimated_tokens"]["cross_session_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_budget", token_estimate=ref_tokens)
            continue
        is_shared_resource = is_shared_resource_candidate(candidate)
        is_shared_skill = is_shared_skill_candidate(candidate)
        if (is_shared_resource or is_shared_skill) and not shared_enabled:
            reason = "shared_resource_budget" if is_shared_resource else "shared_skill_budget"
            dropped[reason] += 1
            dropped["estimated_tokens"][reason] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason=reason, token_estimate=ref_tokens)
            continue
        if (is_shared_resource or is_shared_skill) and shared_min_score > 0.0 and candidate_score < shared_min_score:
            dropped["low_score"] += 1
            dropped["estimated_tokens"]["low_score"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="low_score", token_estimate=ref_tokens)
            continue
        if is_shared_resource and shared_resource_budget_tokens > 0 and shared_resource_used_tokens + ref_tokens > shared_resource_budget_tokens:
            dropped["shared_resource_budget"] += 1
            dropped["estimated_tokens"]["shared_resource_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="shared_resource_budget", token_estimate=ref_tokens)
            continue
        if is_shared_skill and shared_skill_budget_tokens > 0 and shared_skill_used_tokens + ref_tokens > shared_skill_budget_tokens:
            dropped["shared_skill_budget"] += 1
            dropped["estimated_tokens"]["shared_skill_budget"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="shared_skill_budget", token_estimate=ref_tokens)
            continue
        should_reserve_profile_entity_floor = bool(
            (
                summary_profile_entity_floor_enabled
                and candidate_memory_layer in {
                    "summary",
                    "profile_summary",
                    "cross_session_codex_outcome_summary",
                    "cross_session_memory_feature_summary",
                    "same_session_summary",
                    "same_session_memory_feature_summary",
                    "cross_session_summary",
                }
                and not (
                    selected_ref_cap > 1
                    and
                    normalized_question_type in {"broad_exploration", "profile_memory"}
                    and candidate_memory_layer in {
                        "profile_summary",
                        "cross_session_summary",
                        "profile_compression",
                        "cross_session_codex_outcome_compression",
                        "cross_session_memory_feature_summary",
                        "cross_session_memory_feature_compression",
                        "same_session_memory_feature_summary",
                        "same_session_memory_feature_compression",
                    }
                )
            )
            or (
                cross_session_profile_entity_floor_enabled
                and candidate_memory_layer not in profile_entity_bridge_layers
                and not (
                    selected_ref_cap > 1
                    and
                    normalized_question_type in {"broad_exploration", "profile_memory"}
                    and candidate_memory_layer in high_level_profile_memory_layers
                )
            )
        )
        should_reserve_profile_overview_floor = bool(
            profile_overview_floor_enabled
            and candidate_memory_layer not in high_level_profile_memory_layers
        )
        should_reserve_codex_outcome_evidence_floor = bool(
            codex_outcome_evidence_floor_enabled
            and candidate_memory_layer in high_level_profile_memory_layers
            and normalized_question_type in {"current_state", "latest", "evidence", "benchmark_quality", "profile_memory", "multi_hop", "date"}
        )
        reserved_outcome_layer = remaining_codex_outcome_evidence_floor_layer(index + 1)
        if (
            should_reserve_codex_outcome_evidence_floor
            and not codex_outcome_evidence_floor_satisfied()
            and reserved_outcome_layer
        ):
            dropped["memory_layer_floor"] += 1
            dropped["estimated_tokens"]["memory_layer_floor"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_layer": candidate_memory_layer,
                    "memory_layer_budget_capped_layer": candidate_memory_layer,
                    "memory_layer_floor_reserved_layer": reserved_outcome_layer,
                },
                reason="memory_layer_floor",
                token_estimate=ref_tokens,
            )
            continue
        reserved_overview_layer = remaining_profile_overview_floor_layer(index + 1)
        if (
            should_reserve_profile_overview_floor
            and not profile_overview_floor_satisfied()
            and reserved_overview_layer
        ):
            dropped["memory_layer_floor"] += 1
            dropped["estimated_tokens"]["memory_layer_floor"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_layer": candidate_memory_layer,
                    "memory_layer_budget_capped_layer": candidate_memory_layer,
                    "memory_layer_floor_reserved_layer": reserved_overview_layer,
                },
                reason="memory_layer_floor",
                token_estimate=ref_tokens,
            )
            continue
        reserved_profile_layer = remaining_profile_entity_floor_layer(index + 1)
        if (
            should_reserve_profile_entity_floor
            and not profile_entity_floor_satisfied()
            and reserved_profile_layer
        ):
            dropped["memory_layer_floor"] += 1
            dropped["estimated_tokens"]["memory_layer_floor"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_layer": candidate_memory_layer,
                    "memory_layer_budget_capped_layer": candidate_memory_layer,
                    "memory_layer_floor_reserved_layer": reserved_profile_layer,
                },
                reason="memory_layer_floor",
                token_estimate=ref_tokens,
            )
            continue
        if (
            candidate_memory_layer in normalized_memory_layer_budget_tokens
            and int(memory_layer_used_tokens.get(candidate_memory_layer, 0)) + ref_tokens
            > int(normalized_memory_layer_budget_tokens[candidate_memory_layer])
        ):
            dropped["memory_layer_budget"] += 1
            dropped["estimated_tokens"]["memory_layer_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_layer": candidate_memory_layer,
                    "memory_layer_budget_capped_layer": candidate_memory_layer,
                },
                reason="memory_layer_budget",
                token_estimate=ref_tokens,
            )
            continue
        candidate_source_roles = candidate_source_role_names(candidate)
        capped_roles = [
            role
            for role in sorted(candidate_source_roles)
            if role in normalized_source_role_budget_tokens
            and int(source_role_used_tokens.get(role, 0)) + ref_tokens > int(normalized_source_role_budget_tokens[role])
        ]
        if capped_roles:
            dropped["source_role_budget"] += 1
            dropped["estimated_tokens"]["source_role_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_source_roles": sorted(candidate_source_roles),
                    "budget_source_role_counts": candidate_budget_source_role_counts(candidate, candidate_source_roles),
                    "source_role_budget_capped_roles": capped_roles,
                },
                reason="source_role_budget",
                token_estimate=ref_tokens,
            )
            continue
        candidate_memory_selection_policy_set = candidate_memory_selection_policy_names(candidate)
        capped_memory_selection_policies = [
            policy
            for policy in sorted(candidate_memory_selection_policy_set)
            if policy in normalized_memory_selection_policy_budget_tokens
            and int(memory_selection_policy_used_tokens.get(policy, 0)) + ref_tokens
            > int(normalized_memory_selection_policy_budget_tokens[policy])
        ]
        if capped_memory_selection_policies:
            dropped["memory_selection_policy_budget"] += 1
            dropped["estimated_tokens"]["memory_selection_policy_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_memory_selection_policies": sorted(candidate_memory_selection_policy_set),
                    "memory_selection_policy_budget_capped_policies": capped_memory_selection_policies,
                },
                reason="memory_selection_policy_budget",
                token_estimate=ref_tokens,
            )
            continue
        candidate_extraction_phase = candidate_extraction_phase_name(candidate)
        if (
            candidate_extraction_phase in normalized_extraction_phase_budget_tokens
            and int(extraction_phase_used_tokens.get(candidate_extraction_phase, 0)) + ref_tokens
            > int(normalized_extraction_phase_budget_tokens[candidate_extraction_phase])
        ):
            dropped["extraction_phase_budget"] += 1
            dropped["estimated_tokens"]["extraction_phase_budget"] += ref_tokens
            record_dropped_candidate(
                dropped,
                {
                    **candidate,
                    "budget_extraction_phase": candidate_extraction_phase,
                    "extraction_phase_budget_capped_phase": candidate_extraction_phase,
                },
                reason="extraction_phase_budget",
                token_estimate=ref_tokens,
            )
            continue
        seen_text_hashes.add(text_hash)
        if near_duplicate_enabled:
            selected_token_sets.append(normalized_token_set(candidate.get("text", "")))
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
                "budget_memory_layer": candidate_memory_layer,
                "budget_source_roles": sorted(candidate_source_roles),
                "budget_source_role_counts": candidate_budget_source_role_counts(candidate, candidate_source_roles),
                "budget_memory_selection_policies": sorted(candidate_memory_selection_policy_set),
                "budget_extraction_phase": candidate_extraction_phase,
            }
        )
        used_tokens += ref_tokens
        if is_cross_session:
            cross_used_tokens += ref_tokens
            cross_selected_ref_count += 1
            selected_cross_sessions.add(candidate_cross_key)
            if is_entity_bridge:
                entity_bridge_selected_ref_count += 1
        if is_shared_resource_candidate(candidate):
            shared_resource_used_tokens += ref_tokens
            shared_resource_selected_ref_count += 1
        if is_shared_skill_candidate(candidate):
            shared_skill_used_tokens += ref_tokens
            shared_skill_selected_ref_count += 1
        if candidate_memory_layer in normalized_memory_layer_budget_tokens:
            memory_layer_used_tokens[candidate_memory_layer] = int(memory_layer_used_tokens.get(candidate_memory_layer, 0)) + ref_tokens
            memory_layer_selected_ref_counts[candidate_memory_layer] = int(memory_layer_selected_ref_counts.get(candidate_memory_layer, 0)) + 1
        for role in candidate_source_roles:
            if role in normalized_source_role_budget_tokens:
                source_role_used_tokens[role] = int(source_role_used_tokens.get(role, 0)) + ref_tokens
                source_role_selected_ref_counts[role] = int(source_role_selected_ref_counts.get(role, 0)) + 1
        for policy in candidate_memory_selection_policy_set:
            if policy in normalized_memory_selection_policy_budget_tokens:
                memory_selection_policy_used_tokens[policy] = int(memory_selection_policy_used_tokens.get(policy, 0)) + ref_tokens
                memory_selection_policy_selected_ref_counts[policy] = int(
                    memory_selection_policy_selected_ref_counts.get(policy, 0)
                ) + 1
        if candidate_extraction_phase in normalized_extraction_phase_budget_tokens:
            extraction_phase_used_tokens[candidate_extraction_phase] = (
                int(extraction_phase_used_tokens.get(candidate_extraction_phase, 0)) + ref_tokens
            )
            extraction_phase_selected_ref_counts[candidate_extraction_phase] = (
                int(extraction_phase_selected_ref_counts.get(candidate_extraction_phase, 0)) + 1
            )
        if used_tokens >= remote_budget:
            break
    if normalized_question_type in {"current_state", "latest", "profile_memory"}:
        selected, removed_shadowed_tokens = suppress_profile_shadowed_session_entity_refs(selected, dropped)
        if removed_shadowed_tokens:
            used_tokens = max(0, used_tokens - removed_shadowed_tokens)
    dropped["cross_session_policy"] = {
        **cross_session_policy,
        "selected_tokens": cross_used_tokens,
        "selected_ref_count": cross_selected_ref_count,
        "selected_session_count": len(selected_cross_sessions),
        "entity_bridge_selected_ref_count": entity_bridge_selected_ref_count,
    }
    dropped["shared_context_policy"] = {
        **shared_context_policy,
        "resource_selected_tokens": shared_resource_used_tokens,
        "skill_selected_tokens": shared_skill_used_tokens,
        "resource_selected_ref_count": shared_resource_selected_ref_count,
        "skill_selected_ref_count": shared_skill_selected_ref_count,
    }
    dropped["source_role_budget_policy"] = {
        "enabled": bool(normalized_source_role_budget_tokens),
        "budget_tokens": normalized_source_role_budget_tokens,
        "selected_tokens_by_role": source_role_used_tokens,
        "selected_ref_count_by_role": source_role_selected_ref_counts,
    }
    dropped["memory_layer_budget_policy"] = {
        "enabled": bool(normalized_memory_layer_budget_tokens),
        "budget_tokens": normalized_memory_layer_budget_tokens,
        "selected_tokens_by_layer": memory_layer_used_tokens,
        "selected_ref_count_by_layer": memory_layer_selected_ref_counts,
    }
    dropped["memory_selection_policy_budget_policy"] = {
        "enabled": bool(normalized_memory_selection_policy_budget_tokens),
        "budget_tokens": normalized_memory_selection_policy_budget_tokens,
        "selected_tokens_by_policy": memory_selection_policy_used_tokens,
        "selected_ref_count_by_policy": memory_selection_policy_selected_ref_counts,
    }
    dropped["extraction_phase_budget_policy"] = {
        "enabled": bool(normalized_extraction_phase_budget_tokens),
        "budget_tokens": normalized_extraction_phase_budget_tokens,
        "selected_tokens_by_phase": extraction_phase_used_tokens,
        "selected_ref_count_by_phase": extraction_phase_selected_ref_counts,
    }
    if not selected and candidates and remote_budget > 0 and budget_fill_policy != "quality_first":
        first: Json | None = None
        first_source_roles: set[str] = set()
        first_memory_selection_policies: set[str] = set()
        first_extraction_phase = ""
        first_memory_layer = ""
        first_clipped_words: list[str] = []
        for candidate in candidates:
            if context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes):
                continue
            clipped_words_for_candidate = tokens(str(candidate.get("text", "")))[:remote_budget]
            fallback_tokens = len(clipped_words_for_candidate)
            if fallback_tokens <= 0:
                continue
            candidate_memory_layer = candidate_memory_layer_name(candidate)
            if (
                candidate_memory_layer in normalized_memory_layer_budget_tokens
                and int(memory_layer_used_tokens.get(candidate_memory_layer, 0)) + fallback_tokens
                > int(normalized_memory_layer_budget_tokens[candidate_memory_layer])
            ):
                continue
            candidate_source_roles = candidate_source_role_names(candidate)
            if any(
                role in normalized_source_role_budget_tokens
                and int(source_role_used_tokens.get(role, 0)) + fallback_tokens > int(normalized_source_role_budget_tokens[role])
                for role in candidate_source_roles
            ):
                continue
            candidate_memory_selection_policy_set = candidate_memory_selection_policy_names(candidate)
            if any(
                policy in normalized_memory_selection_policy_budget_tokens
                and int(memory_selection_policy_used_tokens.get(policy, 0)) + fallback_tokens
                > int(normalized_memory_selection_policy_budget_tokens[policy])
                for policy in candidate_memory_selection_policy_set
            ):
                continue
            candidate_extraction_phase = candidate_extraction_phase_name(candidate)
            if (
                candidate_extraction_phase in normalized_extraction_phase_budget_tokens
                and int(extraction_phase_used_tokens.get(candidate_extraction_phase, 0)) + fallback_tokens
                > int(normalized_extraction_phase_budget_tokens[candidate_extraction_phase])
            ):
                continue
            first = candidate
            first_source_roles = candidate_source_roles
            first_memory_selection_policies = candidate_memory_selection_policy_set
            first_extraction_phase = candidate_extraction_phase
            first_memory_layer = candidate_memory_layer
            first_clipped_words = clipped_words_for_candidate
            break
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = first_clipped_words
        selected = [
            {
                **first,
                "text": " ".join(clipped_words),
                "token_estimate": len(clipped_words),
                "budget_memory_layer": first_memory_layer,
                "budget_source_roles": sorted(first_source_roles),
                "budget_source_role_counts": candidate_budget_source_role_counts(first, first_source_roles),
                "budget_memory_selection_policies": sorted(first_memory_selection_policies),
                "budget_extraction_phase": first_extraction_phase,
            }
        ]
        used_tokens = len(clipped_words)
        removed_source_role_budget_tokens = 0
        kept_dropped_refs: list[Json] = []
        for dropped_ref in dropped.get("refs", []):
            if dropped_ref.get("drop_reason") == "source_role_budget" and dropped_ref.get("ref_hash") == first.get("ref_hash"):
                try:
                    removed_source_role_budget_tokens += max(0, int(dropped_ref.get("token_estimate") or 0))
                except (TypeError, ValueError):
                    pass
                continue
            kept_dropped_refs.append(dropped_ref)
        if len(kept_dropped_refs) != len(dropped.get("refs", [])):
            dropped["refs"] = kept_dropped_refs
            dropped["source_role_budget"] = max(0, int(dropped.get("source_role_budget", 0)) - 1)
            dropped["estimated_tokens"]["source_role_budget"] = max(
                0,
                int(dropped.get("estimated_tokens", {}).get("source_role_budget", 0)) - removed_source_role_budget_tokens,
            )
        for role in first_source_roles:
            if role in normalized_source_role_budget_tokens:
                source_role_used_tokens[role] = int(source_role_used_tokens.get(role, 0)) + used_tokens
                source_role_selected_ref_counts[role] = int(source_role_selected_ref_counts.get(role, 0)) + 1
        if first_memory_layer in normalized_memory_layer_budget_tokens:
            memory_layer_used_tokens[first_memory_layer] = int(memory_layer_used_tokens.get(first_memory_layer, 0)) + used_tokens
            memory_layer_selected_ref_counts[first_memory_layer] = int(memory_layer_selected_ref_counts.get(first_memory_layer, 0)) + 1
        for policy in first_memory_selection_policies:
            if policy in normalized_memory_selection_policy_budget_tokens:
                memory_selection_policy_used_tokens[policy] = int(memory_selection_policy_used_tokens.get(policy, 0)) + used_tokens
                memory_selection_policy_selected_ref_counts[policy] = int(
                    memory_selection_policy_selected_ref_counts.get(policy, 0)
                ) + 1
        if first_extraction_phase in normalized_extraction_phase_budget_tokens:
            extraction_phase_used_tokens[first_extraction_phase] = (
                int(extraction_phase_used_tokens.get(first_extraction_phase, 0)) + used_tokens
            )
            extraction_phase_selected_ref_counts[first_extraction_phase] = (
                int(extraction_phase_selected_ref_counts.get(first_extraction_phase, 0)) + 1
            )
        dropped["memory_selection_policy_budget_policy"] = {
            "enabled": bool(normalized_memory_selection_policy_budget_tokens),
            "budget_tokens": normalized_memory_selection_policy_budget_tokens,
            "selected_tokens_by_policy": memory_selection_policy_used_tokens,
            "selected_ref_count_by_policy": memory_selection_policy_selected_ref_counts,
        }
        dropped["extraction_phase_budget_policy"] = {
            "enabled": bool(normalized_extraction_phase_budget_tokens),
            "budget_tokens": normalized_extraction_phase_budget_tokens,
            "selected_tokens_by_phase": extraction_phase_used_tokens,
            "selected_ref_count_by_phase": extraction_phase_selected_ref_counts,
        }
        dropped["over_budget"] = max(0, len(candidates) - 1)
        for candidate in candidates[1:]:
            record_dropped_candidate(
                dropped,
                candidate,
                reason="over_budget",
                token_estimate=max(1, token_count(str(candidate.get("text", "")))),
            )
    return selected, used_tokens, dropped


