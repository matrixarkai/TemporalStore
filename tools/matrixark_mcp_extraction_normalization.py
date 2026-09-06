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


def normalize_entity_operator(raw_operator: Any, entity_type: str) -> str:
    """Return the operator actually used by runtime entity maintenance.

    LLM_MERGE is a future/optional production operator because it implies a
    separate LLM merge pass. The current online runtime applies field patches
    deterministically, so serving records should say EUA_MERGE unless the LLM
    merge feature is explicitly enabled.
    """
    if entity_type in {"confirmation", "correction"}:
        return "LATEST"
    operator = str(raw_operator or DEFAULT_ENTITY_MERGE_OPERATOR).strip().upper() or DEFAULT_ENTITY_MERGE_OPERATOR
    if operator == "LLM_MERGE" and not ENABLE_LLM_MERGE_OPERATOR:
        return DEFAULT_ENTITY_MERGE_OPERATOR
    return operator


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


def normalize_extracted_facts(raw_facts: Any, *, chunk: Any, chunk_metadata: Json, raw_uri: str, resource_version: str, provider: str) -> list[Json]:
    if not isinstance(raw_facts, list):
        return []
    facts: list[Json] = []
    for raw in raw_facts[:12]:
        if not isinstance(raw, dict):
            continue
        event_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("event_type") or raw.get("fact_type") or "resource_fact").lower()).strip("_") or "resource_fact"
        if not event_type.startswith("resource_"):
            event_type = f"resource_{event_type}"
        entity_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("entity_type") or event_type).lower()).strip("_") or event_type
        if not entity_type.startswith("resource_"):
            entity_type = f"resource_{entity_type}"
        value = summarize_text(str(raw.get("value") or raw.get("summary_text") or raw.get("state") or "").strip(), limit=260)
        if not value:
            continue
        entity_name = summarize_text(str(raw.get("entity_name") or raw.get("name") or "").strip(), limit=140)
        if not entity_name:
            entity_name = resource_fact_entity_name({"entity_type": entity_type, "entity_prefix": entity_type.removeprefix("resource_")}, value, chunk_metadata, raw_uri)
        try:
            confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.86))))
        except (TypeError, ValueError):
            confidence = 0.86
        facts.append(
            {
                "mode": "matrixark_resource_schema_openai_compatible",
                "classification": "RESOURCE_FACT",
                "event_type": event_type,
                "entity_type": entity_type,
                "status": str(raw.get("status") or "observed"),
                "value": value,
                "entity_name": entity_name,
                "confidence": round(confidence, 6),
                "source_chunk_hash": chunk.chunk_hash,
                "source_ref": chunk.source_ref,
                "resource_version": resource_version,
                "extraction_provider": provider,
            }
        )
    return facts


def assistant_decision_memory_text(text: str) -> str:
    """Keep durable assistant memory focused on decisions, results, and next actions."""
    compact = " ".join(str(text or "").split())
    if not compact:
        return ""
    selected: list[str] = []
    primary_decision_line_pattern = re.compile(
        r"\b(?:decision|decided|done|implemented|fixed|committed|pushed|published|deployed|released|merged|rebased|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|blocked|next|follow[- ]?up|will|use|keep|remove|updated|changed|validated|verified|promoted|indexed|budgeted|batched|flushed)\b",
        re.IGNORECASE,
    )
    secondary_decision_line_pattern = re.compile(
        r"\b(?:profile|cross[- ]session|memory|gap|risk|warning)\b",
        re.IGNORECASE,
    )
    normalized_lines = [" ".join(raw_line.split()).strip(" -*") for raw_line in str(text).splitlines()]
    for pattern in [primary_decision_line_pattern, secondary_decision_line_pattern]:
        for line in normalized_lines:
            if not line:
                continue
            if pattern.search(line):
                selected.append(line)
            if len(selected) >= 4:
                break
        if selected:
            break
    if not selected:
        selected = [
            match.group(0).strip()
            for match in re.finditer(
                r"[^.!?\n]*(?:decision|decided|done|implemented|fixed|committed|pushed|published|deployed|released|merged|rebased|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|blocked|next|will|updated|changed|validated|verified|promoted|indexed|budgeted|batched|flushed|profile|cross[- ]session|memory|gap|risk|warning)[^.!?\n]*[.!?]?",
                str(text),
                flags=re.IGNORECASE,
            )
        ][:4]
    return summarize_text(" ".join(selected) if selected else compact, limit=260)


def tool_evidence_memory_text(text: str) -> str:
    """Keep durable tool memory to result evidence, not complete stdout/stderr blobs."""
    compact = " ".join(str(text or "").split())
    if not compact:
        return ""
    selected: list[str] = []
    evidence_line_pattern = re.compile(
        r"\b(?:exit code:\s*-?\d+|ran\s+\d+\s+tests?|\d+\s+passed\b|tests?\s+(?:passed|failed)|test\s+result:\s+ok|ok\b|failed\b|error\b|fatal\b|commit\s+[0-9a-f]{7,40}|[0-9a-f]{7,40}\.\.[0-9a-f]{7,40}\s+(?:HEAD|[^\s]+)\s*->\s*(?:main|origin/main)|[0-9a-f]{7,40}\s+(?:HEAD|[^\s]+)\s*->\s*(?:main|origin/main)|pushed|published|deployed|released|merged|rebased|rebase|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|promoted|indexed|budgeted|batched|flushed|benchmark|validation|built|compiled)\b",
        re.IGNORECASE,
    )
    for raw_line in str(text).splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        if evidence_line_pattern.search(line):
            selected.append(line)
        if len(selected) >= 6:
            break
    if not selected:
        selected = [
            match.group(0).strip()
            for match in re.finditer(
                r"[^.!?\n]*(?:exit code:\s*-?\d+|ran\s+\d+\s+tests?|tests?\s+(?:passed|failed)|ok\b|failed\b|error\b|fatal\b|commit\s+[0-9a-f]{7,40}|[0-9a-f]{7,40}\.\.[0-9a-f]{7,40}\s+(?:HEAD|[^\s]+)\s*->\s*(?:main|origin/main)|[0-9a-f]{7,40}\s+(?:HEAD|[^\s]+)\s*->\s*(?:main|origin/main)|pushed|published|deployed|released|merged|rebased|rebase|configured|enabled|disabled|installed|migrated|recovered|restored|cleaned|promoted|indexed|budgeted|batched|flushed|benchmark|validation|built|compiled)[^.!?\n]*[.!?]?",
                str(text),
                flags=re.IGNORECASE,
            )
        ][:6]
    return summarize_text(" ".join(selected) if selected else compact, limit=260)


def profile_entity_type_for_memory_text(text: str) -> str:
    """Classify durable personal memory into profile layers used by retrieval."""
    lower = " ".join(str(text or "").lower().split())
    if not lower:
        return ""
    if re.search(r"\b(?:call me|my name is|i am called|i'm called|user(?:'s)? name|user goes by|pronouns?|address (?:me|the user)|nickname)\b", lower):
        return "identity_profile"
    if re.search(r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|language|locale|timezone|time zone|tone|style|format|bullets?|bullet points?|markdown|concise|brief|detailed)\b", lower):
        return "communication_profile"
    if re.search(r"\b(?:feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality|functionalities|functionality only|algorithms?|algos?|implementation focus|no testing|no teseting|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no evident|no eviden[ct]e|feature work only|code changes only|mem0|long[- ]term memory|session memory|profile memory|cross[- ]session memory|threshold|idle batch|batch extraction)\b", lower):
        return "memory_feature_profile"
    if re.search(r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|folder|build|deploy|deployment|rustraft|temporalstore|matrixark)\b", lower):
        return "workspace_profile"
    return ""


FEATURE_SCOPE_EXCLUSION_RE = re.compile(
    r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\s+"
    r"(?:testing|teseting|tests?|monitoring|debugging|debug|evidence|evident|validation|benchmarks?)\b"
)


def feature_scope_excludes_outcome_evidence(text: str) -> bool:
    return bool(FEATURE_SCOPE_EXCLUSION_RE.search(str(text or "").lower())) and profile_entity_type_for_memory_text(text) == "memory_feature_profile"


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


CODEX_OUTCOME_CHANGE_RE = re.compile(
    r"\b(?:changed|updated|implemented|added|removed|fixed|configured|enabled|disabled|installed|upgraded|downgraded|migrated|recovered|restored|cleaned|deleted|moved|renamed|wired|integrated|extracted|promoted|indexed|budgeted|ranked|batched|flushed|synced|consumed|hooked|captured)\b",
    re.IGNORECASE,
)
CODEX_OUTCOME_PUBLISH_RE = re.compile(
    r"\b(?:outcome|pushed|published|deployed|released|uploaded|merged|rebased|fast[- ]?forward(?:ed)?|commit\s+[0-9a-f]{7,40}|origin/main|refs/heads/main|[0-9a-f]{7,40}\.\.[0-9a-f]{7,40}\s+(?:head|[^\s]+)\s*->\s*(?:main|origin/main)|[0-9a-f]{7,40}\s+(?:head|[^\s]+)\s*->\s*(?:main|origin/main))\b",
    re.IGNORECASE,
)
CODEX_OUTCOME_VALIDATION_RE = re.compile(
    r"\b(?:validation|validated|verified|tests?|py_compile|unittest|pytest|cargo test|cargo check|build(?: succeeded)?|built|compiled|syntax check)\b",
    re.IGNORECASE,
)
CODEX_OUTCOME_BENCHMARK_RE = re.compile(
    r"\b(?:benchmark|benchmarked|p50|p99|throughput|latency|qps|ops/sec|requests/sec)\b",
    re.IGNORECASE,
)


def codex_outcome_fact_kind(line: str) -> str:
    normalized = " ".join(str(line or "").split()).strip().lower()
    if not normalized:
        return ""
    has_real_blocker = bool(
        re.search(r"\b(?:blocked|blocker|failure|error|missing|rejected|fatal)\b", normalized)
        or re.search(r"\b[1-9]\d*\s+(?:failed|failures|errors)\b", normalized)
        or (re.search(r"\bfailed\b", normalized) and not re.search(r"\b0\s+failed\b", normalized))
    )
    if normalized.startswith("next:") or re.search(r"\b(?:next|follow[- ]?up)\b", normalized):
        return "next"
    if normalized.startswith("blocker:") or has_real_blocker:
        return "blocker"
    if normalized.startswith("validation:") or CODEX_OUTCOME_VALIDATION_RE.search(normalized):
        return "validation"
    if normalized.startswith("outcome:") or CODEX_OUTCOME_PUBLISH_RE.search(normalized):
        return "outcome"
    if normalized.startswith("changed:") or CODEX_OUTCOME_CHANGE_RE.search(normalized):
        return "changed"
    if normalized.startswith("benchmark:") or CODEX_OUTCOME_BENCHMARK_RE.search(normalized):
        return "benchmark"
    return ""


def codex_outcome_entity_type(kind: str) -> str:
    return {
        "next": "codex_next_action",
        "blocker": "codex_blocker",
        "validation": "codex_validation",
        "outcome": "codex_publish_outcome",
        "changed": "codex_code_change",
        "benchmark": "codex_benchmark_result",
    }.get(str(kind or "").strip().lower(), "codex_outcome_fact")


CODEX_OUTCOME_ENTITY_TYPES = {
    "codex_next_action",
    "codex_blocker",
    "codex_validation",
    "codex_publish_outcome",
    "codex_code_change",
    "codex_benchmark_result",
}


def codex_outcome_fact_entities(
    text: str,
    *,
    role_name: str,
    source_refs: list[str],
    source_count: int,
) -> list[Json]:
    source_role = "tool" if role_name == "tool" else "assistant"
    entities: list[Json] = []
    seen: set[tuple[str, str, str]] = set()

    def outcome_candidate_chunks(compact_line: str) -> list[str]:
        chunks: list[str] = []
        for semicolon_part in re.split(r"\s*;\s*", compact_line):
            for sentence in re.split(
                r"(?<=[.!?])\s+(?=(?:I|We|Codex|Assistant|Tool|Next|Changed|Outcome|Validation|Blocked|Implemented|Fixed|Added|Removed|Updated|Configured|Installed|Pushed|Published|Deployed|Merged|Rebased|Recovered|Promoted|Indexed|Budgeted|Batched|Flushed)\b)",
                semicolon_part,
            ):
                chunk = sentence.strip()
                if chunk:
                    chunks.append(chunk)
        return chunks or ([compact_line] if compact_line else [])

    for raw_line in str(text or "").splitlines():
        compact_line = " ".join(raw_line.split()).strip(" -*")
        for candidate in outcome_candidate_chunks(compact_line):
            line = summarize_text(re.sub(r"^(?:assistant|tool)\s*:\s*", "", candidate, flags=re.IGNORECASE), limit=220)
            if not line:
                continue
            kind = codex_outcome_fact_kind(line)
            if not kind:
                continue
            entity_type = codex_outcome_entity_type(kind)
            normalized_fact = normalized_index_value(line)
            key = (entity_type, source_role, normalized_fact)
            if key in seen:
                continue
            seen.add(key)
            state = summarize_text(f"{source_role} {kind}: {line}", limit=220)
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": summarize_text(f"{entity_type}:{normalized_fact or line}", limit=96),
                    "state": state,
                    "confidence": 0.9 if kind in {"outcome", "validation"} else 0.86,
                    "source_refs": source_refs,
                    "source_roles": [source_role],
                    "source_role_counts": {source_role: source_count},
                    "operator": normalize_entity_operator(None, entity_type),
                    "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                }
            )
            if len(entities) >= 8:
                break
        if len(entities) >= 8:
            break
    return entities


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


def infer_entity_field_patches(entity_type: str, value: str, text: str) -> list[Json]:
    patches: list[Json] = []
    correction = re.search(
        r"\b(?:correction|correct|wrong|updated|changed)[:\s]+([^.;!?]+?)\s+(?:instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if correction:
        replace = clean_patch_value(correction.group(1))
        search = clean_patch_value(correction.group(2))
        patches.append(entity_patch(search, replace))
    preference = re.search(
        r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]+?)\s+(?:now|instead\s+of|not)\s+([^.;!?]+)",
        text,
        flags=re.IGNORECASE,
    )
    if entity_type == "preference" and preference:
        replace = clean_patch_value(preference.group(1))
        search = clean_patch_value(preference.group(2))
        patches.append(entity_patch(search, replace))
    evolving_entity_types = {
        "preference",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "identity_profile",
        "communication_profile",
        "memory_feature_profile",
        "workspace_profile",
        "relationship",
        "approval_state",
        "correction",
        "confirmation",
        "assistant_decision",
        "tool_evidence",
        *CODEX_OUTCOME_ENTITY_TYPES,
    }
    if entity_type in evolving_entity_types and not patches and value:
        patches.append(entity_patch("", summarize_text(value, limit=180)))
    return patches[:3]


def clean_patch_value(value: str) -> str:
    return summarize_text(" ".join(value.split()).strip(" ,;:-"), limit=180)


def canonical_entity_name(entity_type: str, value: str) -> str:
    compact_value = " ".join(str(value or "").split()).strip(" ,;:-")
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
        "correction",
        "confirmation",
        "assistant_decision",
        "tool_evidence",
        *CODEX_OUTCOME_ENTITY_TYPES,
    }:
        return entity_type
    if entity_type == "approval_state":
        subject = compact_value
        subject_patterns = [
            r"^(?:the\s+)?(.+?)\s+(?:is|was|are|were|has been|have been)\s+(?:approved|required|missing|blocked|ready|done|complete|needed)\b",
            r"^(?:the\s+)?(.+?)\s+as\s+(?:a\s+|an\s+|the\s+)?(?:blocker|requirement|approval|decision|status)\b",
            r"^(?:the\s+)?(.+?)\s+after\b",
            r"^(?:the\s+)?(.+?)\s+before\b",
            r"^(?:the\s+)?(.+?)\s+because\b",
            r"^(?:the\s+)?(.+?)\s+as\b",
        ]
        for pattern in subject_patterns:
            match = re.search(pattern, subject, flags=re.IGNORECASE)
            if match:
                subject = match.group(1)
                break
        subject = re.sub(r"^(?:the|a|an)\s+", "", subject, flags=re.IGNORECASE).strip(" ,;:-")
        return summarize_text(subject or compact_value, limit=80) if (subject or compact_value) else entity_type
    return compact_value[:80] if compact_value else entity_type


def entity_retention_priority(entity: Json) -> int:
    entity_type = str(entity.get("entity_type") or "").strip().lower()
    entity_name = str(entity.get("entity_name") or "").strip().lower()
    source_roles = {
        str(role or "").strip().lower()
        for role in entity.get("source_roles", [])
        if str(role or "").strip()
    }
    if entity_type in {
        "identity_profile",
        "communication_profile",
        "workspace_profile",
        "preference",
        "approval_state",
        "correction",
        "memory_feature_profile",
    }:
        return 0
    if "user" in source_roles or entity_type in {"current_plan", "confirmation"}:
        return 1
    if entity_type in CODEX_OUTCOME_ENTITY_TYPES:
        return 2
    if entity_type in {"assistant_decision", "tool_evidence"} and ":" in entity_name:
        return 2
    if entity_type in {"assistant_decision", "tool_evidence"}:
        return 3
    return 4


def dedupe_entities(entities: list[Json]) -> list[Json]:
    seen = set()
    positions: dict[tuple[Any, str], int] = {}
    out = []
    for entity in entities:
        key = (entity.get("entity_type"), str(entity.get("entity_name", "")).lower())
        if key in seen:
            existing = out[positions[key]]
            if entity_retention_priority(entity) < entity_retention_priority(existing):
                out[positions[key]] = entity
                continue
            if entity.get("entity_type") == "tool_evidence" and existing.get("state"):
                continue
            if entity.get("entity_name") == entity.get("entity_type"):
                out[positions[key]] = entity
            continue
        seen.add(key)
        positions[key] = len(out)
        out.append(entity)
    ranked = sorted(enumerate(out), key=lambda item: (entity_retention_priority(item[1]), item[0]))
    kept_indexes = {index for index, _entity in ranked[:20]}
    return [entity for index, entity in enumerate(out) if index in kept_indexes]


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
