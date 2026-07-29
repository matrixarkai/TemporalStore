#!/usr/bin/env python3
"""Extraction normalization and deterministic entity helpers for MatrixArk."""

from __future__ import annotations

import re
from typing import Any

Json = dict[str, Any]

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


def normalize_extracted_entities(raw_entities: Any, *, fallback_text: str, source_refs: list[str], extracted_by: str) -> list[Json]:
    if not isinstance(raw_entities, list):
        return []
    entities: list[Json] = []
    for raw in raw_entities[:12]:
        if not isinstance(raw, dict):
            continue
        entity_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("entity_type") or raw.get("type") or "entity").lower()).strip("_") or "entity"
        entity_name = summarize_text(str(raw.get("entity_name") or raw.get("name") or entity_type).strip(), limit=96)
        state = summarize_text(str(raw.get("state") or raw.get("value") or raw.get("summary") or fallback_text).strip(), limit=320)
        if not state:
            continue
        try:
            confidence = max(0.0, min(1.0, float(raw.get("confidence", 0.82))))
        except (TypeError, ValueError):
            confidence = 0.82
        operator = normalize_entity_operator(raw.get("operator"), entity_type)
        patches = raw.get("field_patches") if isinstance(raw.get("field_patches"), list) else []
        if not patches and entity_type not in {"confirmation", "correction"}:
            patches = [entity_patch("", state)]
        refs = raw.get("source_refs") if isinstance(raw.get("source_refs"), list) else source_refs
        entities.append(
            {
                "entity_type": entity_type,
                "entity_name": entity_name or entity_type,
                "state": state,
                "confidence": round(confidence, 6),
                "source_refs": [str(ref) for ref in refs] if refs else source_refs,
                "operator": operator,
                "field_patches": patches[:3],
                "extracted_by": extracted_by,
            }
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
    decision_line_pattern = re.compile(
        r"\b(?:decision|decided|done|implemented|fixed|committed|pushed|blocked|next|follow[- ]?up|will|use|keep|remove)\b",
        re.IGNORECASE,
    )
    for raw_line in str(text).splitlines():
        line = " ".join(raw_line.split()).strip(" -*")
        if not line:
            continue
        if decision_line_pattern.search(line):
            selected.append(line)
        if len(selected) >= 4:
            break
    if not selected:
        selected = [
            match.group(0).strip()
            for match in re.finditer(
                r"[^.!?\n]*(?:decision|decided|done|implemented|fixed|committed|pushed|blocked|next|will)[^.!?\n]*[.!?]?",
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
        r"\b(?:exit code:\s*-?\d+|ran\s+\d+\s+tests?|tests?\s+(?:passed|failed)|ok\b|failed\b|error\b|fatal\b|commit\s+[0-9a-f]{7,40}|pushed|rebase|benchmark|validation)\b",
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
                r"[^.!?\n]*(?:exit code:\s*-?\d+|ran\s+\d+\s+tests?|tests?\s+(?:passed|failed)|ok\b|failed\b|error\b|fatal\b|commit\s+[0-9a-f]{7,40}|pushed|rebase|benchmark|validation)[^.!?\n]*[.!?]?",
                str(text),
                flags=re.IGNORECASE,
            )
        ][:6]
    return summarize_text(" ".join(selected) if selected else compact, limit=260)


def extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    entities: list[Json] = []
    text = text_from_messages(messages)
    lower = text.lower()
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    user_messages = [
        item
        for item in messages
        if str(item.get("role") or "").lower() in {"user", "human"}
        and str(item.get("content") or "").strip()
    ]
    user_text = text_from_messages(user_messages) if user_messages else ""
    if user_text:
        for match in re.finditer(
            r"\b(?:remember(?:\s+that)?|please\s+always|always|keep|use)\b[:\s]+([^.;!?\n]{4,180})",
            user_text,
            re.IGNORECASE,
        ):
            directive = clean_patch_value(match.group(1))
            if not directive:
                continue
            state = summarize_text(f"user directive: {directive}", limit=220)
            entities.append(
                {
                    "entity_type": "preference",
                    "entity_name": "preference",
                    "state": state,
                    "confidence": 0.84,
                    "source_refs": source_refs,
                    "operator": normalize_entity_operator(None, "preference"),
                    "field_patches": [entity_patch("", summarize_text(state, limit=180))],
                }
            )
    tool_messages = [
        item
        for item in messages
        if str(item.get("role") or "").lower() in {"tool", "tool_result"}
        and str(item.get("content") or "").strip()
    ]
    tool_text = text_from_messages(tool_messages) if tool_messages else ""
    if tool_text:
        evidence_state = summarize_text(tool_evidence_memory_text(tool_text), limit=220)
        entities.append(
            {
                "entity_type": "tool_evidence",
                "entity_name": "tool_evidence",
                "state": evidence_state,
                "confidence": 0.86,
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, "tool_evidence"),
                "field_patches": [entity_patch("", summarize_text(evidence_state, limit=180))],
            }
        )
    assistant_messages = [
        item
        for item in messages
        if str(item.get("role") or "").lower() in {"assistant", "agent", "llm"}
        and str(item.get("content") or "").strip()
    ]
    assistant_text = text_from_messages(assistant_messages) if assistant_messages else ""
    if assistant_text and re.search(
        r"\b(?:decision|decided|done|implemented|fixed|committed|pushed|will|next|choose|chose|use|keep|remove|blocked)\b",
        assistant_text,
        re.IGNORECASE,
    ):
        decision_state = summarize_text(assistant_decision_memory_text(assistant_text), limit=220)
        entities.append(
            {
                "entity_type": "assistant_decision",
                "entity_name": "assistant_decision",
                "state": decision_state,
                "confidence": 0.82,
                "source_refs": source_refs,
                "operator": normalize_entity_operator(None, "assistant_decision"),
                "field_patches": [entity_patch("", summarize_text(decision_state, limit=180))],
            }
        )
    patterns = [
        ("preference", r"\b(?:prefer|prefers|favorite|likes?|loves?)\s+([^.;!?]{2,120})"),
        ("relationship", r"\b(?:friend|partner|mother|father|sister|brother|wife|husband|manager|teammate)\s+([^.;!?]{0,120})"),
        ("location", r"\b(?:live|lives|moved|moving|located|staying)\s+(?:in|to|at)?\s*([^.;!?]{2,120})"),
        ("job_status", r"\b(?:job|role|work|works|position|status)\s+(?:is|as|at|with)?\s*([^.;!?]{2,120})"),
        ("current_plan", r"\b(?:plan|plans|planning|going to|will)\s+([^.;!?]{2,140})"),
        ("family_profile", r"\b(?:family|child|children|son|daughter|pet|dog|cat)\s+([^.;!?]{0,120})"),
        ("correction", r"\b(?:correction|correct|wrong|instead|updated|changed)\s+([^.;!?]{2,140})"),
        ("approval_state", r"\b(?:approved|approval)\s+([^.;!?]{2,140})"),
        ("confirmation", r"\b(?:yes|confirmed|approved|correct|looks good)\b([^.;!?]{0,120})"),
        ("tool_evidence", r"\b(?:exit code:\s*-?\d+|ran\s+\d+\s+tests?|tests?\s+(?:passed|failed)|pushed|commit\s+[0-9a-f]{7,40}|error|failed|fatal)\b([^.;!?]{0,180})"),
    ]
    for entity_type, pattern in patterns:
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
            entities.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name or entity_type,
                    "state": summarize_text(value or text, limit=220),
                    "confidence": 0.82 if value else 0.66,
                    "source_refs": source_refs,
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
        "relationship",
        "approval_state",
        "correction",
        "confirmation",
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
        "correction",
        "confirmation",
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


def dedupe_entities(entities: list[Json]) -> list[Json]:
    seen = set()
    positions: dict[tuple[Any, str], int] = {}
    out = []
    for entity in entities:
        key = (entity.get("entity_type"), str(entity.get("entity_name", "")).lower())
        if key in seen:
            if entity.get("entity_name") == entity.get("entity_type"):
                out[positions[key]] = entity
            continue
        seen.add(key)
        positions[key] = len(out)
        out.append(entity)
    return out[:12]


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
