# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of matrixark_mcp_core.py; re-exported at core end via the dual
relative/absolute import pattern so the same core module object is reused under
both the package path (tools.matrixark_mcp_core) and the top-level path. No
import-time cycle. __all__ lists every moved name for total re-export."""
import json
import os
import re
import urllib
from typing import Any

try:  # package path (tools.matrixark_mcp_core)
    from .matrixark_mcp_core import (
        ANTHROPIC_API_BASE,
        ANTHROPIC_API_VERSION,
        ANTHROPIC_LLM_API_KEY_ENV,
        ANTHROPIC_LLM_MAX_TOKENS,
        ANTHROPIC_LLM_MODEL,
        ANTHROPIC_LLM_TIMEOUT_SEC,
        DEFAULT_ENTITY_MERGE_OPERATOR,
        ENABLE_LLM_MERGE_OPERATOR,
        EXTRACTION_LLM_API_KEY_ENV,
        EXTRACTION_LLM_BASE_URL,
        EXTRACTION_LLM_MAX_TOKENS,
        EXTRACTION_LLM_MODEL,
        EXTRACTION_LLM_TIMEOUT_SEC,
        Json,
        MatrixArkError,
        canonical_entity_name,
        context_index_name,
        dedupe_entities,
        detect_memory_segments,
        entity_patch,
        extract_batch_entities,
        infer_event_type,
        normalize_model_segments,
        ordered_unique,
        oss_encoder_compact_extraction,
        parse_first_json_object,
        require_oss_understanding,
        resource_fact_entity_name,
        summarize_text,
        text_from_messages,
        tokens,
        understanding_provider,
    )
except ImportError:  # top-level path (matrixark_mcp_core)
    from matrixark_mcp_core import (
        ANTHROPIC_API_BASE,
        ANTHROPIC_API_VERSION,
        ANTHROPIC_LLM_API_KEY_ENV,
        ANTHROPIC_LLM_MAX_TOKENS,
        ANTHROPIC_LLM_MODEL,
        ANTHROPIC_LLM_TIMEOUT_SEC,
        DEFAULT_ENTITY_MERGE_OPERATOR,
        ENABLE_LLM_MERGE_OPERATOR,
        EXTRACTION_LLM_API_KEY_ENV,
        EXTRACTION_LLM_BASE_URL,
        EXTRACTION_LLM_MAX_TOKENS,
        EXTRACTION_LLM_MODEL,
        EXTRACTION_LLM_TIMEOUT_SEC,
        Json,
        MatrixArkError,
        canonical_entity_name,
        context_index_name,
        dedupe_entities,
        detect_memory_segments,
        entity_patch,
        extract_batch_entities,
        infer_event_type,
        normalize_model_segments,
        ordered_unique,
        oss_encoder_compact_extraction,
        parse_first_json_object,
        require_oss_understanding,
        resource_fact_entity_name,
        summarize_text,
        text_from_messages,
        tokens,
        understanding_provider,
    )

__all__ = ['openai_compatible_json_call', 'anthropic_json_call', 'normalize_entity_operator', 'normalize_extracted_entities', 'normalize_extracted_segments', 'normalize_extracted_facts', 'openai_compatible_one_pass_memory_extraction', 'anthropic_one_pass_memory_extraction', 'openai_compatible_resource_facts', 'compact_internal_extraction', 'ONE_PASS_MEMORY_SCHEMA']


def openai_compatible_json_call(*, system: str, user: str, model: str | None = None, max_tokens: int | None = None) -> Json:
    payload = {
        "model": model or EXTRACTION_LLM_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": max_tokens or EXTRACTION_LLM_MAX_TOKENS,
        "response_format": {"type": "json_object"},
    }
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get(EXTRACTION_LLM_API_KEY_ENV, "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        f"{EXTRACTION_LLM_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=EXTRACTION_LLM_TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode("utf-8"))
        content = str(data.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
        if not content:
            raise MatrixArkError("OpenAI-compatible extraction provider returned empty content")
        return parse_first_json_object(content)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError, MatrixArkError) as exc:
        raise MatrixArkError(f"OpenAI-compatible extraction provider failed: {exc}") from exc


def anthropic_json_call(*, system: str, user: str, model: str | None = None, max_tokens: int | None = None) -> Json:
    """Anthropic Messages API counterpart of openai_compatible_json_call.

    Same raw-stdlib-urllib transport as the OpenAI-compatible path (no new dep). The
    Messages API has no OpenAI-style response_format=json_object, so the JSON-only
    instruction rides in the system + user prompt (identical to the OpenAI provider),
    and parse_first_json_object extracts the object from .content[0].text.
    """
    api_key = os.environ.get(ANTHROPIC_LLM_API_KEY_ENV, "")
    if not api_key:
        raise MatrixArkError(f"{ANTHROPIC_LLM_API_KEY_ENV} is required for the Anthropic extraction provider")
    payload = {
        "model": model or ANTHROPIC_LLM_MODEL,
        "max_tokens": max_tokens or ANTHROPIC_LLM_MAX_TOKENS,
        "system": system,
        "messages": [
            {"role": "user", "content": user},
        ],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_API_VERSION,
    }
    request = urllib.request.Request(
        f"{ANTHROPIC_API_BASE}/v1/messages",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=ANTHROPIC_LLM_TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode("utf-8"))
        blocks = data.get("content", [])
        content = "".join(
            str(block.get("text", ""))
            for block in blocks
            if isinstance(block, dict) and block.get("type", "text") == "text"
        ).strip() if isinstance(blocks, list) else ""
        if not content:
            raise MatrixArkError("Anthropic extraction provider returned empty content")
        return parse_first_json_object(content)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError, MatrixArkError) as exc:
        raise MatrixArkError(f"Anthropic extraction provider failed: {exc}") from exc


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


def openai_compatible_one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    messages = envelope["messages"]
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    system = "Return only JSON. You are a one-pass memory extractor for MatrixArk."
    user = (
        "Extract memory from this logical conversation batch in one pass. "
        "Return JSON with keys classification, event_type, batch_summary, entities, segments, indexes. "
        "Entities must use a stable concise entity_name and a separate state sentence; do not copy the same phrase into both. "
        "Each entity shape: {entity_type, entity_name, state, confidence, field_patches}; operator is optional and MatrixArk will coerce it to the actual runtime operator. "
        "Segments shape: {topic, coordinate_tuples, message_indexes, saliency_score, summary_text}. "
        "Use event_type values like approval_state, budget_update, deadline, procedure, correction, status_update.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )
    raw = openai_compatible_json_call(system=system, user=user)
    batch_text = text_from_messages(messages)
    entities = normalize_extracted_entities(raw.get("entities"), fallback_text=batch_text, source_refs=source_refs, extracted_by="openai_compatible")
    if not entities:
        if require_oss_understanding():
            raise MatrixArkError("OpenAI-compatible extraction returned no entities")
        entities = extract_batch_entities(messages, envelope)
        for entity in entities:
            entity["extracted_by"] = "deterministic_fallback"
    segments = normalize_extracted_segments(raw.get("segments"), messages)
    if not segments:
        if require_oss_understanding():
            raise MatrixArkError("OpenAI-compatible extraction returned no segments")
        segments, _segment_meta = detect_memory_segments(messages, {**envelope, "segment_provider": "deterministic"})
    event_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("event_type") or infer_event_type(batch_text)).lower()).strip("_") or "session"
    classification = re.sub(r"[^A-Z0-9_]+", "_", str(raw.get("classification") or "BATCH_MEMORY").upper()).strip("_") or "BATCH_MEMORY"
    indexes = raw.get("indexes") if isinstance(raw.get("indexes"), list) else []
    normalized_indexes = [str(item) for item in indexes if isinstance(item, str)]
    normalized_indexes = ordered_unique(normalized_indexes + [context_index_name("event_type", event_type), context_index_name("classification", classification)])
    return {
        "mode": "matrixark_one_pass_schema_openai_compatible",
        "understanding_provider": "openai_compatible",
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": str(raw.get("status") or "observed"),
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": {"provider": "openai_compatible", "execution_mode": "llm_json", "model": EXTRACTION_LLM_MODEL, "fallback_used": False, "segment_count": len(segments)},
        "indexes": normalized_indexes[:8],
        "batch_summary": summarize_text(str(raw.get("batch_summary") or raw.get("summary") or batch_text), limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(tokens(batch_text)),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }


def anthropic_one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Anthropic Messages counterpart of openai_compatible_one_pass_memory_extraction.

    Byte-compatible output dict shape: same keys, same normalization helpers, same
    deterministic fallback + require_oss gate behavior. Only the transport
    (anthropic_json_call), the extracted_by tag, and the provider-identifying
    mode / understanding_provider / segment_provider values differ, so everything
    downstream (storage, indexes, replay) stays identical to the OpenAI path.
    """
    messages = envelope["messages"]
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    system = "Return only JSON. You are a one-pass memory extractor for MatrixArk."
    user = (
        "Extract memory from this logical conversation batch in one pass. "
        "Return JSON with keys classification, event_type, batch_summary, entities, segments, indexes. "
        "Entities must use a stable concise entity_name and a separate state sentence; do not copy the same phrase into both. "
        "Each entity shape: {entity_type, entity_name, state, confidence, field_patches}; operator is optional and MatrixArk will coerce it to the actual runtime operator. "
        "Segments shape: {topic, coordinate_tuples, message_indexes, saliency_score, summary_text}. "
        "Use event_type values like approval_state, budget_update, deadline, procedure, correction, status_update.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )
    raw = anthropic_json_call(system=system, user=user)
    batch_text = text_from_messages(messages)
    entities = normalize_extracted_entities(raw.get("entities"), fallback_text=batch_text, source_refs=source_refs, extracted_by="anthropic")
    if not entities:
        if require_oss_understanding():
            raise MatrixArkError("Anthropic extraction returned no entities")
        entities = extract_batch_entities(messages, envelope)
        for entity in entities:
            entity["extracted_by"] = "deterministic_fallback"
    segments = normalize_extracted_segments(raw.get("segments"), messages)
    if not segments:
        if require_oss_understanding():
            raise MatrixArkError("Anthropic extraction returned no segments")
        segments, _segment_meta = detect_memory_segments(messages, {**envelope, "segment_provider": "deterministic"})
    event_type = re.sub(r"[^a-z0-9_]+", "_", str(raw.get("event_type") or infer_event_type(batch_text)).lower()).strip("_") or "session"
    classification = re.sub(r"[^A-Z0-9_]+", "_", str(raw.get("classification") or "BATCH_MEMORY").upper()).strip("_") or "BATCH_MEMORY"
    indexes = raw.get("indexes") if isinstance(raw.get("indexes"), list) else []
    normalized_indexes = [str(item) for item in indexes if isinstance(item, str)]
    normalized_indexes = ordered_unique(normalized_indexes + [context_index_name("event_type", event_type), context_index_name("classification", classification)])
    return {
        "mode": "matrixark_one_pass_schema_anthropic",
        "understanding_provider": "anthropic",
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": str(raw.get("status") or "observed"),
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": {"provider": "anthropic", "execution_mode": "llm_json", "model": ANTHROPIC_LLM_MODEL, "fallback_used": False, "segment_count": len(segments)},
        "indexes": normalized_indexes[:8],
        "batch_summary": summarize_text(str(raw.get("batch_summary") or raw.get("summary") or batch_text), limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(tokens(batch_text)),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }


def openai_compatible_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    system = "Return only JSON. You extract cited resource facts for MatrixArk."
    user = (
        "Extract decisions, owners, costs, deadlines, API contracts, troubleshooting steps, policies, approvals, risks, and procedures from the resource chunk. "
        "Return JSON with {facts:[...]}. Each fact shape: {event_type, entity_type, entity_name, value, confidence}. "
        "event_type and entity_type should use resource_* names. entity_name must be a stable subject, while value is the factual state. "
        "Do not invent facts; use an empty facts list if nothing is useful.\n\n"
        f"Source ref: {chunk.source_ref}\nMetadata: {json.dumps(chunk_metadata, sort_keys=True)[:1200]}\nChunk text:\n{chunk.text[:6000]}\n\nJSON:"
    )
    raw = openai_compatible_json_call(system=system, user=user)
    return normalize_extracted_facts(raw.get("facts"), chunk=chunk, chunk_metadata=chunk_metadata, raw_uri=raw_uri, resource_version=resource_version, provider="openai_compatible")


def compact_internal_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Rules-first internal extraction used by the local MCP MVP.

    Production MatrixArk can replace this with OSS/OpenAI/provider extraction,
    but callers still see the same Mem0-style envelope contract.
    """

    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return oss_encoder_compact_extraction(envelope, prior_context=prior_context)

    text = text_from_messages(envelope["messages"]).lower()
    if envelope["kind"] == "feedback":
        positive = any(term in text for term in ["yes", "confirmed", "approved", "correct", "looks good"])
        negative = any(term in text for term in ["no", "wrong", "incorrect", "reject", "not correct"])
        prior_level = prior_context.get("level", "")
        if not prior_level:
            return {
                "mode": "matrixark_internal",
                "classification": "AMBIGUOUS",
                "quality_warning": "short feedback lacks prior context",
                "prior_refs": [],
            }
        warning = ""
        if prior_level == "user":
            warning = "session_id missing; used user_id fallback for prior context"
        prior_refs = prior_context.get("refs", [])
        if positive:
            return {
                "mode": "matrixark_internal",
                "classification": "CONFIRMATION",
                "status": "accepted",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        if negative:
            return {
                "mode": "matrixark_internal",
                "classification": "CORRECTION",
                "status": "rejected",
                "prior_context": prior_level,
                "prior_refs": prior_refs,
                "prior_message_count": len(prior_context.get("messages", [])),
                "prior_summary_count": len(prior_context.get("summaries", [])),
                "quality_warning": warning,
            }
        return {
            "mode": "matrixark_internal",
            "classification": "FEEDBACK",
            "status": "observed",
            "prior_context": prior_level,
            "prior_refs": prior_refs,
            "prior_message_count": len(prior_context.get("messages", [])),
            "prior_summary_count": len(prior_context.get("summaries", [])),
            "quality_warning": warning,
        }
    return {
        "mode": "matrixark_internal",
        "classification": "NEW_EVENT",
        "status": "observed",
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
        "quality_warning": "",
    }


ONE_PASS_MEMORY_SCHEMA: Json = {
    "version": "matrixark-one-pass-memory-v1",
    "input": "logical session batch",
    "outputs": [
        "ContextEvent",
        "ContextEntity",
        "ContextSummary",
        "ContextIndex",
        "stale_blocker",
        "EntityPatch",
        "MemorySegment",
        "extraction_audit",
    ],
    "entity_types": [
        "preference",
        "relationship",
        "location",
        "job_status",
        "current_plan",
        "family_profile",
        "identity_profile",
        "communication_profile",
        "workspace_profile",
        "correction",
        "confirmation",
    ],
    "segmentation": {
        "phase_1": "semantic_saliency_filtering",
        "phase_2": "event_centric_partitioning",
        "output": "topic plus coordinate tuples over message indexes",
    },
}


