#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Extraction normalization and deterministic entity helpers for MatrixArk."""

from __future__ import annotations

import re
from typing import Any

Json = dict[str, Any]

ROLE_ALIASES = {
    "human": "user",
    "prompt": "user",
    "assistant_response": "assistant",
    "agent": "assistant",
    "ai": "assistant",
    "bot": "assistant",
    "llm": "assistant",
    "model": "assistant",
    "tool_result": "tool",
    "tool-output": "tool",
    "tooloutput": "tool",
    "tool_output": "tool",
    "function": "tool",
    "function_call_output": "tool",
    "custom_tool_call_output": "tool",
    "tool_call_output": "tool",
}

try:
    from tools.matrixark_mcp_entity_ops import entity_patch
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_resources import resource_fact_entity_name
    from tools.matrixark_mcp_runtime_config import DEFAULT_ENTITY_MERGE_OPERATOR, ENABLE_LLM_MERGE_OPERATOR
    from tools.matrixark_mcp_summaries import summarize_text
    from tools.matrixark_mcp_text import text_from_messages
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_entity_ops import entity_patch
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_resources import resource_fact_entity_name
    from matrixark_mcp_runtime_config import DEFAULT_ENTITY_MERGE_OPERATOR, ENABLE_LLM_MERGE_OPERATOR
    from matrixark_mcp_summaries import summarize_text
    from matrixark_mcp_text import text_from_messages


try:  # the implementation lives in matrixark_mcp_core_extraction; this module re-exports it
    from .matrixark_mcp_core_extraction import normalize_entity_operator
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_extraction import normalize_entity_operator


def normalize_source_role(raw_role: Any) -> str:
    role = str(raw_role or "").strip().lower()
    return ROLE_ALIASES.get(role, role)


def normalize_source_role_counts(raw_counts: Any, fallback_roles: list[str] | None = None) -> Json:
    counts: Json = {}
    if isinstance(raw_counts, dict):
        for raw_role, raw_count in raw_counts.items():
            role = normalize_source_role(raw_role)
            if not role:
                continue
            try:
                count = max(0, int(raw_count or 0))
            except (TypeError, ValueError):
                count = 0
            if count:
                counts[role] = int(counts.get(role, 0)) + count
    if not counts and fallback_roles:
        for role in fallback_roles:
            normalized_role = normalize_source_role(role)
            if normalized_role:
                counts[normalized_role] = int(counts.get(normalized_role, 0)) + 1
    return counts


# NOT re-exported, and this is the one that goes the other way.
#
# matrixark_mcp_core_extraction defines normalize_extracted_entities too, and THIS copy is the
# fuller one: it populates `source_roles` and `source_role_counts` on each entity and the live copy
# does not. Those are not decorative -- `entity_retention_priority`, re-exported just above, ranks
# on `"user" in source_roles`, so the field decides what survives a dedupe.
#
# It is not evidence of a live hole either: matrixark_mcp_core_codex_outcome and
# matrixark_mcp_recovery both write source_roles onto entities on live paths, so entities reaching
# that ranking are not all role-less. Whether THIS normaliser should populate it as well is a
# question about the extraction path, not a consolidation, and consolidating toward the live copy
# would silently drop the field.


def normalize_extracted_entities(raw_entities: Any, *, fallback_text: str, source_refs: list[str], extracted_by: str) -> list[Json]:
    if not isinstance(raw_entities, list):
        return []
    entities: list[Json] = []
    for raw in raw_entities[:12]:
        if not isinstance(raw, dict):
            continue
        entity_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("entity_type") or raw.get("type") or "entity").lower()).strip("_") or "entity"
        raw_entity_name = str(raw.get("entity_name") or raw.get("name") or "").strip()
        entity_name = summarize_text(raw_entity_name or entity_type, limit=96)
        state = summarize_text(str(raw.get("state") or raw.get("value") or raw.get("summary") or fallback_text).strip(), limit=320)
        if not state:
            continue
        entity_name = canonical_entity_name(entity_type, raw_entity_name or state)
        try:
            confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.82))))
        except (TypeError, ValueError):
            confidence = 0.82
        operator = normalize_entity_operator(raw.get("operator"), entity_type)
        patches = raw.get("field_patches") if isinstance(raw.get("field_patches"), list) else []
        if not patches and entity_type not in {"confirmation", "correction"}:
            patches = [entity_patch("", state)]
        refs = raw.get("source_refs") if isinstance(raw.get("source_refs"), list) else source_refs
        source_roles = [
            role
            for role in [normalize_source_role(value) for value in raw.get("source_roles", [])]
            if role
        ] if isinstance(raw.get("source_roles"), list) else []
        source_role_counts = normalize_source_role_counts(raw.get("source_role_counts"), source_roles)
        entity = {
            "entity_type": entity_type,
            "entity_name": entity_name or entity_type,
            "state": state,
            "confidence": round(confidence, 6),
            "source_refs": [str(ref) for ref in refs] if refs else source_refs,
            "operator": operator,
            "field_patches": patches[:3],
            "extracted_by": extracted_by,
        }
        if source_roles:
            entity["source_roles"] = ordered_unique(source_roles)
        if source_role_counts:
            entity["source_role_counts"] = source_role_counts
        entities.append(
            entity
        )
    return dedupe_entities(entities)


# NOT re-exported, and the reason is the opposite of how it reads.
#
# This copy differs from matrixark_mcp_core_extraction's by ONE thing: a lazy import of
# normalize_model_segments from matrixark_mcp_extraction_runtime, inside the function, where the
# live one has the name at module scope. Pure plumbing -- and it was moved on that basis, until
# test_no_module_is_orphaned_quietly failed naming matrixark_mcp_extraction_runtime as a NEW
# orphan.
#
# That lazy import is the last reference to that module anywhere in the tree. KNOWN_ORPHANS is
# empty, so consolidating this would create the first orphan in a tree that has none -- and the
# module it orphans holds diverged copies of live names, one_pass_memory_extraction and
# openai_compatible_resource_facts among them. That guard's docstring calls the combination the
# worst one: a copy that is both wrong and unreachable cannot fail today, and is what somebody
# reaches for tomorrow.
#
# So a difference that reads as plumbing was load-bearing. Removing this copy is a decision about
# whether matrixark_mcp_extraction_runtime should exist, not a cleanup.
#
# `extract_batch_entities` is the other one left here, for a different reason: thirty-four hunks
# against matrixark_mcp_core's, with real work on both sides. The live one has a content-matching
# lineage builder, an assistant-profile filter, and a location pattern stopping at a clause
# boundary -- "I live in Seattle and prefer metric units" captures Seattle there and
# "Seattle and prefer metric units" here. This copy has helpers the live one does not. Neither is
# the complete one.


def normalize_extracted_segments(raw_segments: Any, messages: list[Json]) -> list[Json]:
    if isinstance(raw_segments, list):
        try:
            try:
                from tools.matrixark_mcp_extraction_runtime import normalize_model_segments
            except ModuleNotFoundError:  # Direct script execution from tools/.
                from matrixark_mcp_extraction_runtime import normalize_model_segments
            return normalize_model_segments({"segments": raw_segments}, messages)
        except MatrixArkError:
            return []
    return []


# Not defined here: the implementation lives in matrixark_mcp_core_extraction and this module carried an
# identical second copy of each.
try:
    from tools.matrixark_mcp_core_extraction import (
        normalize_extracted_facts,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_extraction import (
        normalize_extracted_facts,
    )


try:  # the implementation lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from .matrixark_mcp_core_codex_outcome import assistant_decision_memory_text
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import assistant_decision_memory_text


try:  # the implementation lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from .matrixark_mcp_core_codex_outcome import tool_evidence_memory_text
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import tool_evidence_memory_text


# Two copies that classify the same text differently.
#
# profile_entity_type_for_memory_text tests the same branches as the live one in a
# different ORDER: the live copy asks whether the text is about a memory feature before
# asking whether it is about a response style, and this copy asked the other way round. Any
# text that mentions both lands in a different class, and that is most of them --
# "respond about long-term memory settings", "answer using session memory only",
# "preferred language for profile memory" all classify as a communication profile here and
# as a memory feature profile there.
#
# feature_scope_excludes_outcome_evidence had collapsed to a single expression that keeps
# only one of the live copy's three tests, so "focus on features only" excludes outcome
# evidence through the live path and does not through this one.
try:  # the implementation lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from tools.matrixark_mcp_core_codex_outcome import (
        feature_scope_excludes_outcome_evidence,
        profile_entity_type_for_memory_text,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import (
        feature_scope_excludes_outcome_evidence,
        profile_entity_type_for_memory_text,
    )


FEATURE_SCOPE_EXCLUSION_RE = re.compile(
    r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\s+"
    r"(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b"
)


#: Not an index value -- the one use is the dedup key in `codex_outcome_fact_entities` below. It
#: had its own definition, and that definition was the strictest of the three copies of this name:
#: `[^a-z0-9]+` drops `_ . : / -` along with every non-ASCII character. Two distinct facts written
#: in Chinese therefore normalised to "" alike, shared a key, and the second was dropped as a
#: duplicate -- the extraction lost it. The shared normaliser keeps CJK, hiragana, katakana, hangul
#: and accented Latin, so those facts now key apart. It also keeps `_ . : / -`, which makes the key
#: slightly less folding for ASCII punctuation: "a.b" and "a-b" used to share a key and no longer
#: do. That is the same direction -- two things that are not the same are no longer called the same.
try:
    from tools.matrixark_mcp_indexing import normalized_index_value
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_indexing import normalized_index_value


try:  # the four patterns live in matrixark_mcp_core_codex_outcome; this module re-exports them
    from .matrixark_mcp_core_codex_outcome import (
        CODEX_OUTCOME_BENCHMARK_RE,
        CODEX_OUTCOME_CHANGE_RE,
        CODEX_OUTCOME_PUBLISH_RE,
        CODEX_OUTCOME_VALIDATION_RE,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import (
        CODEX_OUTCOME_BENCHMARK_RE,
        CODEX_OUTCOME_CHANGE_RE,
        CODEX_OUTCOME_PUBLISH_RE,
        CODEX_OUTCOME_VALIDATION_RE,
    )


try:  # the implementation lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from .matrixark_mcp_core_codex_outcome import codex_outcome_fact_kind
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import codex_outcome_fact_kind


try:  # the implementation lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from .matrixark_mcp_core_codex_outcome import codex_outcome_entity_type
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import codex_outcome_entity_type


try:  # the set lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from .matrixark_mcp_core_codex_outcome import CODEX_OUTCOME_ENTITY_TYPES
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import CODEX_OUTCOME_ENTITY_TYPES


def extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    entities: list[Json] = []
    text = text_from_messages(messages)
    lower = text.lower()
    feature_scope_memory_only = feature_scope_excludes_outcome_evidence(text)
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    def source_ref_for_message_index(index: int) -> str:
        if isinstance(source_event_ids, list) and index < len(source_event_ids):
            return str(source_event_ids[index])
        return str(index)

    def source_refs_for_role(role_name: str) -> list[str]:
        normalized_role_name = normalize_source_role(role_name)
        refs: list[str] = []
        for index, item in enumerate(messages):
            if normalize_source_role(item.get("role")) != normalized_role_name:
                continue
            if not str(item.get("content") or "").strip():
                continue
            refs.append(source_ref_for_message_index(index))
        return refs or source_refs

    def source_count_for_role(role_name: str) -> int:
        normalized_role_name = normalize_source_role(role_name)
        return sum(
            1
            for item in messages
            if normalize_source_role(item.get("role")) == normalized_role_name
            and str(item.get("content") or "").strip()
        )

    def role_lineage(role_name: str) -> Json:
        normalized_role_name = normalize_source_role(role_name)
        count = source_count_for_role(normalized_role_name)
        if not normalized_role_name:
            return {}
        return {
            "source_roles": [normalized_role_name],
            "source_role_counts": {normalized_role_name: max(1, count)},
        }

    def profile_lineage_for_match(entity_type: str, value: str) -> Json:
        probe = str(value or "").strip().lower()
        if entity_type == "tool_evidence":
            return role_lineage("tool")
        if probe and probe in user_text.lower():
            return role_lineage("user")
        if probe and probe in assistant_text.lower():
            return role_lineage("assistant")
        if entity_type in {
            "preference",
            "location",
            "job_status",
            "current_plan",
            "family_profile",
            "identity_profile",
            "communication_profile",
            "memory_feature_profile",
            "workspace_profile",
            "approval_state",
            "correction",
            "confirmation",
        }:
            return role_lineage("user") if user_text else {}
        return {}

    def source_refs_for_match(entity_type: str, value: str) -> list[str]:
        probe = str(value or "").strip().lower()
        if entity_type == "tool_evidence":
            return source_refs_for_role("tool")
        if probe and probe in user_text.lower():
            return source_refs_for_role("user")
        if probe and probe in assistant_text.lower():
            return source_refs_for_role("assistant")
        lineage = profile_lineage_for_match(entity_type, value)
        roles = lineage.get("source_roles") if isinstance(lineage.get("source_roles"), list) else []
        if len(roles) == 1:
            return source_refs_for_role(str(roles[0]))
        return source_refs
    user_messages = [
        item
        for item in messages
        if str(item.get("role") or "").lower() in {"user", "human"}
        and str(item.get("content") or "").strip()
    ]
    user_text = text_from_messages(user_messages) if user_messages else ""
    user_profile_entity_type = profile_entity_type_for_memory_text(user_text)
    if user_profile_entity_type == "memory_feature_profile":
        state = summarize_text(f"memory feature policy: {user_text}", limit=220)
        entities.append(
            {
                "entity_type": user_profile_entity_type,
                "entity_name": user_profile_entity_type,
                "state": state,
                "confidence": 0.86,
                "source_refs": source_refs_for_role("user"),
                **role_lineage("user"),
                "operator": normalize_entity_operator(None, user_profile_entity_type),
                "field_patches": [entity_patch("", summarize_text(state, limit=180))],
            }
        )
    if user_text:
        user_directive_patterns = [
            ("current_plan", r"\bgoal\s*:\s*([^.;!?\n]{4,220})"),
            ("current_plan", r"\b(?:please\s+)?(?:implement|fix|add|remove|replace|move)\s+([^.;!?\n]{4,180})"),
            ("preference", r"\b(?:remember(?:\s+that)?|please\s+always|always|keep|use|prefer|make\s+sure(?:\s+to)?)\b[:\s]+([^.;!?\n]{4,180})"),
            ("preference", r"\b(?:do\s+not|don't|never|avoid|stop)\s+([^.;!?\n]{4,180})"),
            ("current_plan", r"\b(?:we\s+should|should|need\s+to|must|have\s+to|let's|lets|please)\s+([^.;!?\n]{4,180})"),
        ]
        for entity_type, pattern in user_directive_patterns:
            for match in re.finditer(pattern, user_text, re.IGNORECASE):
                directive = clean_patch_value(match.group(0))
                if not directive:
                    continue
                directive_entity_type = profile_entity_type_for_memory_text(directive) or entity_type
                prefix = "user profile" if directive_entity_type.endswith("_profile") else (
                    "user directive" if directive_entity_type == "preference" else "user plan"
                )
                state = summarize_text(f"{prefix}: {directive}", limit=220)
                entities.append(
                    {
                        "entity_type": directive_entity_type,
                        "entity_name": summarize_text(f"{directive_entity_type}:{directive}", limit=96),
                        "state": state,
                        "confidence": 0.86,
                        "source_refs": source_refs_for_role("user"),
                        **role_lineage("user"),
                        "operator": normalize_entity_operator(None, directive_entity_type),
                        "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                    }
                )
    tool_messages = [
        item
        for item in messages
        if normalize_source_role(item.get("role")) == "tool"
        and str(item.get("content") or "").strip()
    ]
    tool_text = text_from_messages(tool_messages) if tool_messages else ""
    if tool_text and not feature_scope_memory_only:
        tool_refs = source_refs_for_role("tool")
        evidence_state = summarize_text(tool_evidence_memory_text(tool_text), limit=220)
        entities.append(
            {
                "entity_type": "tool_evidence",
                "entity_name": "tool_evidence",
                "state": evidence_state,
                "confidence": 0.86,
                "source_refs": tool_refs,
                **role_lineage("tool"),
                "operator": normalize_entity_operator(None, "tool_evidence"),
                "field_patches": [entity_patch("", summarize_text(evidence_state, limit=180))],
            }
        )
        for message_index, message in enumerate(messages):
            role = normalize_source_role(message.get("role"))
            if role != "tool":
                continue
            content = str(message.get("content") or "").strip()
            if not content:
                continue
            entities.extend(
                codex_outcome_fact_entities(
                    content,
                    role_name="tool",
                    source_refs=[source_ref_for_message_index(message_index)],
                    source_count=1,
                )
            )
    assistant_messages = [
        item
        for item in messages
        if normalize_source_role(item.get("role")) == "assistant"
        and str(item.get("content") or "").strip()
    ]
    assistant_text = text_from_messages(assistant_messages) if assistant_messages else ""
    if assistant_text and re.search(
        r"\b(?:decision|decided|done|implemented|fixed|committed|pushed|will|next|choose|chose|use|keep|remove|blocked|updated|changed|validated|verified|profile|cross[- ]session|memory|gap|risk|warning)\b",
        assistant_text,
        re.IGNORECASE,
    ):
        assistant_refs = source_refs_for_role("assistant")
        if not feature_scope_memory_only:
            decision_state = summarize_text(assistant_decision_memory_text(assistant_text), limit=220)
            entities.append(
                {
                    "entity_type": "assistant_decision",
                    "entity_name": "assistant_decision",
                    "state": decision_state,
                    "confidence": 0.82,
                    "source_refs": assistant_refs,
                    **role_lineage("assistant"),
                    "operator": normalize_entity_operator(None, "assistant_decision"),
                    "field_patches": [entity_patch("", summarize_text(decision_state, limit=180))],
                }
            )
            for message_index, message in enumerate(messages):
                if normalize_source_role(message.get("role")) != "assistant":
                    continue
                content = str(message.get("content") or "").strip()
                if not content:
                    continue
                entities.extend(
                    codex_outcome_fact_entities(
                        content,
                        role_name="assistant",
                        source_refs=[source_ref_for_message_index(message_index)],
                        source_count=1,
                    )
                )
        assistant_profile_fact_patterns = [
            r"\b(?:i(?:'ll| will)?|codex will|assistant will)\s+(?:remember|keep|use|follow|prefer|avoid|stop using|not use|always use|make sure)\b[:\s]+([^.;!?\n]{4,220})",
            r"\b(?:noted|got it|understood|i(?:'ll| will)? remember|remembered)\b[:\s]+(?:that\s+)?([^.;!?\n]{4,220})",
            r"\b(?:i(?:'ll| will) keep|i(?:'ll| will) use|i(?:'ll| will) avoid|i(?:'ll| will) make sure)\s+([^.;!?\n]{4,220})",
        ]
        seen_assistant_profile_facts: set[str] = set()
        for pattern in assistant_profile_fact_patterns:
            for match in re.finditer(pattern, assistant_text, re.IGNORECASE):
                fact_text = clean_patch_value(match.group(1) if match.groups() else match.group(0))
                if not fact_text:
                    continue
                fact_key = re.sub(r"\s+", " ", fact_text.lower()).strip(" .,:;-")
                if any(fact_key in seen or seen in fact_key for seen in seen_assistant_profile_facts):
                    continue
                seen_assistant_profile_facts.add(fact_key)
                fact_entity_type = profile_entity_type_for_memory_text(fact_text) or "preference"
                state = summarize_text(f"assistant profile fact: {fact_text}", limit=220)
                entities.append(
                    {
                        "entity_type": fact_entity_type,
                        "entity_name": summarize_text(f"{fact_entity_type}:{fact_text}", limit=96),
                        "state": state,
                        "confidence": 0.84,
                        "source_refs": assistant_refs,
                        **role_lineage("assistant"),
                        "operator": normalize_entity_operator(None, fact_entity_type),
                        "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                    }
                )
    patterns = [
        ("preference", r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]{2,120})"),
        ("preference", r"\b(?:you|user)\s+(?:always|usually|prefer(?:s)?|like(?:s)?|want(?:s)?|need(?:s)?)\s+([^.;!?]{2,140})"),
        ("preference", r"\b(?:you|user)\s+(?:never|avoid(?:s)?|do(?:es)?\s+not|don't|doesn't|cannot|can't|should\s+not|must\s+not)\s+([^.;!?]{2,140})"),
        ("preference", r"\b(?:i(?:'ll| will)?\s+remember|remembered|noted|got it)[:\s]+(?:that\s+)?(?:you|user)\s+([^.;!?]{2,160})"),
        ("preference", r"\b(?:standing instruction|standing preference|saved preference|persistent instruction)[:\s]+([^.;!?]{2,180})"),
        ("relationship", r"\b(?:friend|partner|mother|father|sister|brother|wife|husband|manager|teammate)\s+([^.;!?]{0,120})"),
        ("location", r"\b(?:live|lives|moved|moving|located|staying)\s+(?:in|to|at)?\s*([^.;!?]{2,120})"),
        ("job_status", r"\b(?:job|role|work|works|position|status)\s+(?:is|as|at|with)?\s*([^.;!?]{2,120})"),
        ("current_plan", r"\b(?:plan|plans|planning|going to|will)\s+([^.;!?]{2,140})"),
        ("current_plan", r"\b(?:you|user)\s+(?:asked|requested|required|requires|need(?:s)?|want(?:s)?)\s+(?:me\s+|codex\s+|us\s+|to\s+)?([^.;!?]{2,160})"),
        ("family_profile", r"\b(?:family|child|children|son|daughter|pet|dog|cat)\s+([^.;!?]{0,120})"),
        ("identity_profile", r"\b(?:call me|my name is|i am called|i'm called)\s+([^.;!?]{2,80})"),
        ("identity_profile", r"\b(?:user(?:'s)? name is|user goes by|user prefers to be called)\s+([^.;!?]{2,80})"),
        ("identity_profile", r"\b(?:my pronouns are|user(?:'s)? pronouns are)\s+([^.;!?]{2,80})"),
        ("communication_profile", r"\b(?:reply|respond|answer|write)\s+(?:to\s+me\s+)?(?:in|with|using)\s+([^.;!?]{2,140})"),
        ("communication_profile", r"\b(?:use|prefer|likes?|wants?)\s+([^.;!?]{2,120}?\b(?:tone|style|format|bullets?|bullet points?|markdown|language|locale|timezone|time zone|concise|detailed|brief))"),
        ("communication_profile", r"\b(?:communication style|response style|answer style|writing style|preferred language|preferred format|timezone|time zone|locale)[:\s]+([^.;!?]{2,160})"),
        ("workspace_profile", r"\b(?:always|please|must|should|use|keep|prefer)\s+([^.;!?]{2,180}?\b(?:ubuntu|wsl|linux|repo|repository|workspace|worktree|folder|branch|main|remote|github|rustraft|temporalstore|matrixark|build|deploy|deployment))"),
        ("workspace_profile", r"\b(?:do not|don't|never|avoid|stop)\s+([^.;!?]{2,180}?\b(?:windows|folder|repo|repository|worktree|branch|remote|build|deploy|deployment))"),
        ("workspace_profile", r"\b(?:workspace|repo|repository|branch|remote|github|build|deployment|deploy|ubuntu|wsl|linux|rustraft|temporalstore|matrixark)[:\s]+([^.;!?]{2,180})"),
        ("correction", r"\b(?:correction|correct|wrong|instead|updated|changed)\s+([^.;!?]{2,140})"),
        ("approval_state", r"\b(?:approved|approval)\s+([^.;!?]{2,140})"),
        ("confirmation", r"\b(?:yes|confirmed|approved|correct|looks good)\b([^.;!?]{0,120})"),
        ("tool_evidence", r"\b(?:exit code:\s*-?\d+|ran\s+\d+\s+tests?|tests?\s+(?:passed|failed)|pushed|commit\s+[0-9a-f]{7,40}|error|failed|fatal)\b([^.;!?]{0,180})"),
    ]
    for entity_type, pattern in patterns:
        if feature_scope_memory_only and entity_type == "tool_evidence":
            continue
        for match in re.finditer(pattern, text, re.IGNORECASE):
            value = " ".join(match.group(1).split()).strip(" :-") if match.groups() else ""
            if entity_type == "confirmation" and not envelope.get("context_pack_id") and not lower.strip() in {
                "yes",
                "yes.",
                "correct",
                "correct.",
                "approved",
                "approved.",
            }:
                continue
            entity_name = canonical_entity_name(entity_type, value)
            field_patches = infer_entity_field_patches(entity_type, value, text)
            role_lineage_fields = profile_lineage_for_match(entity_type, value or match.group(0))
            matched_source_refs = source_refs_for_match(entity_type, value or match.group(0))
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name or entity_type,
                    "state": summarize_text(value or text, limit=220),
                    "confidence": 0.82 if value else 0.66,
                    "source_refs": matched_source_refs,
                    **role_lineage_fields,
                    "operator": normalize_entity_operator(None, entity_type),
                    "field_patches": field_patches,
                }
            )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.6,
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, "session"),
                "field_patches": [],
            }
        )
    return dedupe_entities(entities)


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import clean_patch_value
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import clean_patch_value


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import canonical_entity_name
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import canonical_entity_name


# `dedupe_entities` joined them, and the copy here was missing a whole step: the live one calls
# `drop_directive_duplicates(out)` before ranking and this one did not. That is the example the
# diverged-copy guard cites in its own docstring, and the function it dropped is defined only in
# matrixark_mcp_core -- so the copy could not have called it without this import.
try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import dedupe_entities
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import dedupe_entities


# `entity_retention_priority` joined them, and this copy was the poorer one in a way that changes
# what survives a dedupe. It normalised each source role with a bare `strip().lower()` where the
# live copy calls `normalized_extraction_message_role`, which MAPS aliases -- human and prompt to
# user, agent/ai/bot/llm/model to assistant, tool_result to tool. Executed both:
#
#     source_roles=["user"]     live 1   orphan 1
#     source_roles=["human"]    live 1   orphan 4
#     source_roles=["prompt"]   live 1   orphan 4
#
# Priority 1 against 4 is kept against dropped when dedupe_entities ranks, so an entity whose role
# was recorded as "human" was retained through one path and discarded through the other.
try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import entity_retention_priority
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import entity_retention_priority


# `infer_entity_field_patches` joined them, and the live copy is better in two ways.
#
# It has a third branch this one lacked: a negative preference such as "we should avoid tabs in
# this repository" produces a patch there and nothing here -- executed both ways.
#
# And it reads the document through `_whole_text_patch_scans`, an lru_cached helper whose docstring
# records why: this function is called once per extracted entity and each call re-ran three
# whole-document regexes, so the work grew as the square of the entity count -- ~33 seconds on a
# 256 KB markdown ingest against ~1.2 s for a JSON document of the same size. The copy here still
# inlined the three searches, so it never got that fix either.
try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import infer_entity_field_patches
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import infer_entity_field_patches


# `codex_outcome_fact_entities` differed only by inlining a local: the live copy binds
# `entity_name` and then uses it, this one called summarize_text in place. Same value.
try:  # the implementation lives in matrixark_mcp_core_codex_outcome; this module re-exports it
    from .matrixark_mcp_core_codex_outcome import codex_outcome_fact_entities
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_codex_outcome import codex_outcome_fact_entities


def ordered_unique(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        value = value.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out
