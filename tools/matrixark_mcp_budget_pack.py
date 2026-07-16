#!/usr/bin/env python3
"""Token-budget packing helpers for MatrixArk retrieval."""

from __future__ import annotations

import os
import re
from typing import Callable

try:
    from tools.matrixark_mcp_core import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        Json,
        MatrixArkError,
        clip_context_text,
        diversify_for_question_type,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        packing_sort_key,
        record_dropped_candidate,
        stable_hash,
        token_count,
        tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        DEFAULT_BUDGET_FILL_POLICY,
        DEFAULT_MAX_CONTEXT_TOKENS,
        DEFAULT_MAX_GLOBAL_CANDIDATES,
        DEFAULT_MAX_SELECTED_REFS,
        DEFAULT_RETRIEVAL_MIN_SCORE,
        Json,
        MatrixArkError,
        clip_context_text,
        diversify_for_question_type,
        is_shared_resource_candidate,
        is_shared_skill_candidate,
        merge_ranked_paths,
        packing_sort_key,
        record_dropped_candidate,
        stable_hash,
        token_count,
        tokens,
    )


def context_text_hashes(text: str) -> set[int]:
    compact = " ".join(str(text).split())
    variants = {compact[:512]}
    without_role = re.sub(r"^(user|assistant|tool|system):\s*", "", compact, flags=re.IGNORECASE)
    variants.add(without_role[:512])
    tokenized = tokens(compact)
    if tokenized:
        variants.add(" ".join(tokenized)[:512])
        if tokenized[0] in {"user", "assistant", "tool", "system"}:
            variants.add(" ".join(tokenized[1:])[:512])
    return {stable_hash(variant) for variant in variants if variant}


def local_context_budget(args: Json) -> Json:
    raw_items = args.get("local_context", [])
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise MatrixArkError("local_context must be an array")
    items: list[Json] = []
    text_hashes: set[int] = set()
    token_total = 0
    for index, item in enumerate(raw_items):
        if isinstance(item, str):
            text = item
            source = f"local:{index}"
            ref_type = "local_context"
        elif isinstance(item, dict):
            text = str(item.get("text") or item.get("content") or "")
            source = str(item.get("source") or item.get("ref") or f"local:{index}")
            ref_type = str(item.get("ref_type") or "local_context")
        else:
            raise MatrixArkError("local_context items must be strings or objects")
        text = clip_context_text(text)
        if not text:
            continue
        item_tokens = token_count(text)
        token_total += item_tokens
        text_hashes.update(context_text_hashes(text))
        items.append(
            {
                "ref_type": ref_type,
                "source": source,
                "text": text,
                "token_estimate": item_tokens,
                "text_hash": stable_hash(text[:512]),
            }
        )
    explicit_tokens = args.get("local_context_tokens")
    token_source = "estimated_from_local_context"
    if explicit_tokens is not None:
        if not isinstance(explicit_tokens, int) or explicit_tokens < 0:
            raise MatrixArkError("local_context_tokens must be a non-negative integer")
        token_total = max(token_total, explicit_tokens)
        token_source = "agent_provided_local_context_tokens"
    raw_safety_margin = args.get("local_context_safety_margin_tokens")
    if raw_safety_margin is None:
        raw_safety_margin = os.environ.get("MATRIXARK_LOCAL_CONTEXT_SAFETY_MARGIN_TOKENS")
    if raw_safety_margin is None:
        raw_max_context = args.get("max_context_tokens", DEFAULT_MAX_CONTEXT_TOKENS)
        try:
            max_context_tokens = max(0, int(raw_max_context or DEFAULT_MAX_CONTEXT_TOKENS))
        except (TypeError, ValueError):
            max_context_tokens = DEFAULT_MAX_CONTEXT_TOKENS
        safety_margin_tokens = min(512, max_context_tokens // 20)
        safety_margin_source = "matrixark_default_5_percent_capped"
    else:
        try:
            safety_margin_tokens = int(raw_safety_margin or 0)
        except (TypeError, ValueError):
            raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
        safety_margin_source = "agent_provided_safety_margin" if "local_context_safety_margin_tokens" in args else "env_safety_margin"
    if safety_margin_tokens < 0:
        raise MatrixArkError("local_context_safety_margin_tokens must be a non-negative integer")
    return {
        "items": items,
        "token_estimate": token_total,
        "text_hashes": text_hashes,
        "token_source": token_source,
        "safety_margin_tokens": safety_margin_tokens,
        "safety_margin_source": safety_margin_source,
    }


def compact_local_context_refs(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
            }
        )
    return refs


def local_context_refs_for_pack(local_budget: Json) -> list[Json]:
    refs: list[Json] = []
    for item in local_budget.get("items", []):
        if not isinstance(item, dict):
            continue
        refs.append(
            {
                "ref_type": item.get("ref_type", "local_context"),
                "source": item.get("source", ""),
                "token_estimate": item.get("token_estimate", 0),
                "text_hash": item.get("text_hash"),
                "text": item.get("text", ""),
                "selection_reason": "provided by agent-visible local context before MatrixArk remote retrieval",
            }
        )
    return refs


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
    deadline_exceeded: Callable[[], bool] | None = None,
    deadline_reason: str = "deadline_during_pack",
    cross_session_policy: Json | None = None,
    shared_context_policy: Json | None = None,
) -> tuple[list[Json], int, Json]:
    duplicate_text_hashes = duplicate_text_hashes or set()
    remote_budget = max(0, max_context_tokens - max(0, reserved_tokens))
    selected_ref_cap = max(1, int(max_selected_refs or DEFAULT_MAX_SELECTED_REFS))
    candidate_pool_limit = max(
        selected_ref_cap,
        max(1, int(max_global_candidates or DEFAULT_MAX_GLOBAL_CANDIDATES)),
    )
    min_score = max(0.0, min(1.0, float(min_score)))
    budget_fill_policy = (budget_fill_policy or DEFAULT_BUDGET_FILL_POLICY).strip().lower()
    candidates = merge_ranked_paths(
        primary,
        auxiliary,
        total_limit=candidate_pool_limit,
        auxiliary_quota=auxiliary_quota,
    )
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
        "cross_session_budget": 0,
        "cross_session_session_cap": 0,
        "cross_session_candidate_cap": 0,
        "shared_resource_budget": 0,
        "shared_skill_budget": 0,
        "deadline": 0,
        "max_selected_refs": 0,
        "estimated_tokens": {
            "over_budget": 0,
            "duplicate": 0,
            "low_score": 0,
            "stale": 0,
            "summary": 0,
            "raw_l2": 0,
            "cross_session_budget": 0,
            "cross_session_session_cap": 0,
            "cross_session_candidate_cap": 0,
            "shared_resource_budget": 0,
            "shared_skill_budget": 0,
            "deadline": 0,
            "max_selected_refs": 0,
        },
        "reason_descriptions": {
            "over_budget": "candidate was relevant but exceeded the remaining remote context token budget",
            "duplicate": "candidate duplicated local context or an already selected ref",
            "low_score": "candidate score was below the minimum packing threshold",
            "stale": "candidate was stale or superseded for the query policy",
            "summary": "summary text was dropped in favor of denser raw/evidence refs",
            "raw_l2": "raw L2 content was dropped because a smaller cited chunk or summary was enough",
            "cross_session_budget": "cross-session candidate exceeded the configured cross-session token budget",
            "cross_session_session_cap": "cross-session candidate came from a session beyond max cross-session session fanout",
            "cross_session_candidate_cap": "cross-session candidate exceeded the configured cross-session candidate cap",
            "shared_resource_budget": "shared resource candidate exceeded the configured shared-resource token budget",
            "shared_skill_budget": "shared skill candidate exceeded the configured shared-skill token budget",
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
            dropped["max_selected_refs"] += len(remaining_candidates)
            for skipped in remaining_candidates:
                skipped_tokens = max(1, token_count(str(skipped.get("text", ""))))
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
        if is_cross_session and cross_max_candidates > 0 and cross_selected_ref_count >= cross_max_candidates:
            dropped["cross_session_candidate_cap"] += 1
            dropped["estimated_tokens"]["cross_session_candidate_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_candidate_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_max_sessions > 0 and candidate_cross_key not in selected_cross_sessions and len(selected_cross_sessions) >= cross_max_sessions:
            dropped["cross_session_session_cap"] += 1
            dropped["estimated_tokens"]["cross_session_session_cap"] += ref_tokens
            record_dropped_candidate(dropped, candidate, reason="cross_session_session_cap", token_estimate=ref_tokens)
            continue
        if is_cross_session and cross_budget_tokens > 0 and cross_used_tokens + ref_tokens > cross_budget_tokens and not (is_entity_bridge and entity_bridge_selected_ref_count < cross_min_entity_bridge_refs):
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
        seen_text_hashes.add(text_hash)
        selected.append(
            {
                **candidate,
                "token_estimate": ref_tokens,
                "packing_score": round(packing_sort_key(candidate, question_type)[0], 6),
                "packing_policy": question_type,
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
        if used_tokens >= remote_budget:
            break
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
    if not selected and candidates and remote_budget > 0 and budget_fill_policy != "quality_first":
        first = next(
            (
                candidate
                for candidate in candidates
                if not context_text_hashes(str(candidate.get("text", ""))).intersection(duplicate_text_hashes)
            ),
            None,
        )
        if first is None:
            return selected, used_tokens, dropped
        clipped_words = tokens(str(first.get("text", "")))[:remote_budget]
        selected = [{**first, "text": " ".join(clipped_words), "token_estimate": len(clipped_words)}]
        used_tokens = len(clipped_words)
        dropped["over_budget"] = max(0, len(candidates) - 1)
        for candidate in candidates[1:]:
            record_dropped_candidate(
                dropped,
                candidate,
                reason="over_budget",
                token_estimate=max(1, token_count(str(candidate.get("text", "")))),
            )
    return selected, used_tokens, dropped
