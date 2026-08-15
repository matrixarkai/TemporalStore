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
        FEATURE_SCOPE_EXCLUDED_DIMENSION_RE,
        FEATURE_SCOPE_EXCLUSION_RE,
        FEATURE_SCOPE_ONLY_RE,
        Json,
        context_index_name,
        entity_patch,
        normalize_entity_operator,
        normalize_message_role,
        normalized_index_value,
        profile_memory_class_for_entity_type,
        profile_memory_kind_for_entity_type,
        summarize_text,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        FEATURE_SCOPE_EXCLUDED_DIMENSION_RE,
        FEATURE_SCOPE_EXCLUSION_RE,
        FEATURE_SCOPE_ONLY_RE,
        Json,
        context_index_name,
        entity_patch,
        normalize_entity_operator,
        normalize_message_role,
        normalized_index_value,
        profile_memory_class_for_entity_type,
        profile_memory_kind_for_entity_type,
        summarize_text,
    )

__all__ = ['assistant_decision_memory_text', 'tool_evidence_memory_text', 'profile_entity_type_for_memory_text', 'feature_scope_excludes_outcome_evidence', 'assistant_profile_fact_memory_text', 'normalized_extraction_message_role', 'CODEX_OUTCOME_CHANGE_RE', 'CODEX_OUTCOME_PUBLISH_RE', 'CODEX_OUTCOME_VALIDATION_RE', 'CODEX_OUTCOME_BENCHMARK_RE', 'codex_outcome_fact_kind', 'CODEX_OUTCOME_ENTITY_TYPES', 'codex_outcome_entity_type', 'is_codex_outcome_entity_type', 'semantic_source_role_for_entity_type', 'codex_outcome_fact_index_terms', 'codex_outcome_fact_entities']


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
    if re.search(r"\b(?:feature parity|feature[- ]focused|features? only|features? referring to|focuns on features?|focus(?:ed)? on features?|functionality|functionalities|functionality only|algorithms?|algos?|implementation focus|no testing|no teseting|no tests?|skip tests?|without tests?|no monitoring|no debugging|no debug|no evidence|no evident|no eviden[ct]e|feature work only|code changes only|mem0|long[- ]term memory|session memory|profile memory|cross[- ]session memory|live ingestion|memory ingestion|threshold|idle batch|batch extraction|profile promotion|retrieval budgets?|memory retrieval|secondary indexes?|context events?|context entit(?:y|ies)|context summaries?|contextpacks?)\b", lower):
        return "memory_feature_profile"
    if re.search(r"\b(?:reply|respond|answer|write|communication style|response style|answer style|preferred language|preferred format|language|locale|timezone|time zone|tone|style|format|bullets?|bullet points?|markdown|concise|brief|detailed)\b", lower):
        return "communication_profile"
    if re.search(r"\b(?:workspace|repo|repository|branch|remote|github|origin/main|main branch|ubuntu|wsl|linux|windows folder|worktree|folder|build|deploy|deployment|rustraft|temporalstore|matrixark)\b", lower):
        return "workspace_profile"
    return ""


def feature_scope_excludes_outcome_evidence(text: str) -> bool:
    lower = str(text or "").lower()
    feature_memory = profile_entity_type_for_memory_text(text) == "memory_feature_profile"
    if not feature_memory:
        return False
    if FEATURE_SCOPE_ONLY_RE.search(lower):
        return True
    if FEATURE_SCOPE_EXCLUSION_RE.search(lower):
        return True
    if re.search(r"\b(?:no|not|skip|without|exclude|excluding|ignore|omit)\b", lower):
        return bool(FEATURE_SCOPE_EXCLUDED_DIMENSION_RE.search(lower))
    return False


def assistant_profile_fact_memory_text(text: str) -> bool:
    """Detect assistant confirmations that should refine profile memory, not session plans."""
    compact = " ".join(str(text or "").lower().split())
    if not compact:
        return False
    if not re.search(
        r"\b(?:going forward|from now on|noted|got it|understood|remembered|i(?:'ll| will)?|codex will|assistant will)\b",
        compact,
    ):
        return False
    if profile_entity_type_for_memory_text(compact):
        return True
    return bool(
        re.search(
            r"\b(?:remember|keep|use|follow|prefer|avoid|stop using|not use|always use|make sure)\b",
            compact,
        )
    )


def normalized_extraction_message_role(role: Any) -> str:
    role_name = str(role or "").strip().lower()
    return {
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
    }.get(role_name, role_name)


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


CODEX_OUTCOME_ENTITY_TYPES = {
    "codex_next_action",
    "codex_blocker",
    "codex_validation",
    "codex_publish_outcome",
    "codex_code_change",
    "codex_benchmark_result",
}


def codex_outcome_entity_type(kind: str) -> str:
    return {
        "next": "codex_next_action",
        "blocker": "codex_blocker",
        "validation": "codex_validation",
        "outcome": "codex_publish_outcome",
        "changed": "codex_code_change",
        "benchmark": "codex_benchmark_result",
    }.get(str(kind or "").strip().lower(), "codex_outcome_fact")


def is_codex_outcome_entity_type(entity_type: Any) -> bool:
    normalized = normalized_index_value(entity_type)
    return normalized in CODEX_OUTCOME_ENTITY_TYPES or normalized in {"assistant_decision", "tool_evidence", "codex_outcome_fact"}


def semantic_source_role_for_entity_type(entity_type: Any, role_names: set[str] | None = None) -> str:
    normalized = normalized_index_value(entity_type)
    if profile_memory_kind_for_entity_type(normalized) == "durable_profile" or profile_memory_class_for_entity_type(normalized) in {
        "identity",
        "communication",
        "workspace",
        "memory_feature",
        "preference",
        "personal_context",
        "task_context",
    }:
        return "profile"
    if normalized in {"assistant_decision", "assistant_response"}:
        return "assistant"
    if normalized == "tool_evidence":
        return "tool"
    if normalized in {"user_requirement", "user_preference"}:
        return "user"
    if normalized in CODEX_OUTCOME_ENTITY_TYPES or normalized == "codex_outcome_fact":
        roles = {normalize_message_role(role) for role in (role_names or set()) if normalize_message_role(role)}
        if "tool" in roles:
            return "tool"
        if "assistant" in roles:
            return "assistant"
        return "assistant"
    return ""


def codex_outcome_fact_index_terms(*values: Any) -> set[str]:
    text = " ".join(str(value or "") for value in values)
    lower = text.lower()
    terms: set[str] = set()
    has_real_blocker = bool(
        re.search(r"\b(?:blocked|blocker|failure|error|missing|rejected|fatal)\b", lower)
        or re.search(r"\b[1-9]\d*\s+(?:failed|failures|errors)\b", lower)
        or (re.search(r"\bfailed\b", lower) and not re.search(r"\b0\s+failed\b", lower))
    )
    for kind in ["next", "blocker", "validation", "outcome", "changed", "benchmark"]:
        if kind == "next" and re.search(r"\b(?:next|follow[- ]?up)\b", lower):
            terms.add(context_index_name("codex_outcome", kind))
        elif kind == "blocker" and has_real_blocker:
            terms.add(context_index_name("codex_outcome", kind))
        elif kind == "validation" and CODEX_OUTCOME_VALIDATION_RE.search(lower):
            terms.add(context_index_name("codex_outcome", kind))
        elif kind == "outcome" and CODEX_OUTCOME_PUBLISH_RE.search(lower):
            terms.add(context_index_name("codex_outcome", kind))
        elif kind == "changed" and CODEX_OUTCOME_CHANGE_RE.search(lower):
            terms.add(context_index_name("codex_outcome", kind))
        elif kind == "benchmark" and CODEX_OUTCOME_BENCHMARK_RE.search(lower):
            terms.add(context_index_name("codex_outcome", kind))
    return terms


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
            entity_name = summarize_text(f"{entity_type}:{normalized_fact or line}", limit=96)
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name,
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


