#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk message/resource extraction runtime helpers."""

from __future__ import annotations

import json
import re
from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_extraction_provider import EXTRACTION_LLM_MODEL, openai_compatible_json_call, parse_first_json_object
    from tools.matrixark_mcp_extraction_normalization import (
        canonical_entity_name,
        clean_patch_value,
        dedupe_entities,
        extract_batch_entities,
        infer_entity_field_patches,
        normalize_entity_operator,
        normalize_extracted_entities,
        normalize_extracted_facts,
        normalize_extracted_segments,
        ordered_unique,
    )
    from tools.matrixark_mcp_indexing import context_index_name
    from tools.matrixark_mcp_oss_understanding import (
        UNDERSTANDING_LABELS,
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_extract_batch_entities,
        oss_encoder_rank_labels,
        prototype_vectors,
        require_oss_understanding,
        understanding_provider,
    )
    from tools.matrixark_mcp_resources import extract_resource_fact_value, matched_resource_fact_schemas, resource_fact_entity_name
    from tools.matrixark_mcp_scoring import tokens
    from tools.matrixark_mcp_summaries import summarize_text
    from tools.matrixark_mcp_text import text_from_messages
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_extraction_provider import EXTRACTION_LLM_MODEL, openai_compatible_json_call, parse_first_json_object
    from matrixark_mcp_extraction_normalization import (
        canonical_entity_name,
        clean_patch_value,
        dedupe_entities,
        extract_batch_entities,
        infer_entity_field_patches,
        normalize_entity_operator,
        normalize_extracted_entities,
        normalize_extracted_facts,
        normalize_extracted_segments,
        ordered_unique,
    )
    from matrixark_mcp_indexing import context_index_name
    from matrixark_mcp_oss_understanding import (
        UNDERSTANDING_LABELS,
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_extract_batch_entities,
        oss_encoder_rank_labels,
        prototype_vectors,
        require_oss_understanding,
        understanding_provider,
    )
    from matrixark_mcp_resources import extract_resource_fact_value, matched_resource_fact_schemas, resource_fact_entity_name
    from matrixark_mcp_scoring import tokens
    from matrixark_mcp_summaries import summarize_text
    from matrixark_mcp_text import text_from_messages


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


def openai_compatible_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    system = "Return only JSON. You extract cited resource facts for MatrixArk."
    user = (
        "Extract decisions, owners, costs, deadlines, API contracts, troubleshooting steps, policies, approvals, control_states, and procedures from the resource chunk. "
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
        "correction",
        "confirmation",
    ],
    "segmentation": {
        "phase_1": "semantic_saliency_filtering",
        "phase_2": "event_centric_partitioning",
        "output": "topic plus coordinate tuples over message indexes",
    },
}


def one_pass_memory_extraction(envelope: Json, *, prior_context: Json) -> Json:
    """Extract events, entities, summaries, and indexes from one batch pass.

    This mirrors a single-pass extraction idea: compile the desired memory outputs
    into one schema and process the input session once. The local MVP uses
    deterministic rules, while a production provider can replace this function
    with one GPT-4o-mini/OSS call that emits the same JSON shape.
    """

    provider = understanding_provider(envelope)
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        try:
            return openai_compatible_one_pass_memory_extraction(envelope, prior_context=prior_context)
        except MatrixArkError:
            if require_oss_understanding():
                raise
    messages = envelope["messages"]
    batch_text = text_from_messages(messages)
    batch_terms = tokens(batch_text)
    segments, segment_provider_meta = detect_memory_segments(messages, envelope)
    if provider == "oss_encoder":
        entities = oss_encoder_extract_batch_entities(messages, envelope)
        event_type = oss_encoder_event_type(batch_text)
    else:
        entities = extract_batch_entities(messages, envelope)
        event_type = infer_event_type(batch_text)
    classification = "BATCH_MEMORY"
    if any(entity["entity_type"] == "confirmation" for entity in entities):
        classification = "CONFIRMATION"
    elif any(entity["entity_type"] == "correction" for entity in entities):
        classification = "CORRECTION"
    indexes = ordered_unique(
        [
            context_index_name("event_type", event_type),
            context_index_name("classification", classification),
            context_index_name("status", "observed"),
            context_index_name("source_type", envelope.get("kind", "message")),
        ]
        + [context_index_name("entity_type", entity["entity_type"]) for entity in entities]
        + [context_index_name("segment_topic", segment["topic"]) for segment in segments]
    )
    return {
        "mode": "matrixark_one_pass_schema_oss_encoder" if provider == "oss_encoder" else "matrixark_one_pass_schema",
        "understanding_provider": provider,
        "schema": ONE_PASS_MEMORY_SCHEMA,
        "classification": classification,
        "status": "observed",
        "event_type": event_type,
        "entities": entities,
        "segments": segments,
        "segment_provider": segment_provider_meta,
        "indexes": indexes[:8],
        "batch_summary": summarize_text(batch_text, limit=700),
        "message_count": len(messages),
        "token_count_estimate": len(batch_terms),
        "prior_context": prior_context.get("level", ""),
        "prior_refs": prior_context.get("refs", []),
        "prior_message_count": len(prior_context.get("messages", [])),
        "prior_summary_count": len(prior_context.get("summaries", [])),
    }



try:
    from tools.matrixark_mcp_segments import (
        build_segment_prompt,
        contiguous_ranges,
        detect_memory_segments,
        infer_segment_topic,
        intelligent_memory_segments,
        normalize_coordinate_tuples,
        normalize_message_indexes,
        normalize_model_segments,
        oss_encoder_memory_segments,
        oss_model_memory_segments,
        semantic_saliency_score,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_segments import (
        build_segment_prompt,
        contiguous_ranges,
        detect_memory_segments,
        infer_segment_topic,
        intelligent_memory_segments,
        normalize_coordinate_tuples,
        normalize_message_indexes,
        normalize_model_segments,
        oss_encoder_memory_segments,
        oss_model_memory_segments,
        semantic_saliency_score,
    )


def infer_event_type(text: str) -> str:
    lower = text.lower()
    if any(term in lower for term in ["correct", "correction", "wrong", "instead", "updated", "changed"]):
        return "correction"
    if any(term in lower for term in ["yes", "confirmed", "approved", "looks good"]):
        return "confirmation"
    if any(term in lower for term in ["prefer", "favorite", "like", "love"]):
        return "preference_update"
    if any(term in lower for term in ["plan", "going to", "will ", "schedule"]):
        return "plan_update"
    if any(term in lower for term in ["work", "job", "role", "status", "position"]):
        return "status_update"
    return "dialogue_batch"



def resource_extraction_mode(envelope: Json) -> str:
    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return "matrixark_resource_schema_oss_encoder"
    if provider in {"openai", "openai_compatible", "openai-compatible"}:
        return "matrixark_resource_schema_openai_compatible"
    return "matrixark_resource_schema"


def extract_resource_facts(chunk: Any, *, chunk_metadata: Json, envelope: Json, raw_uri: str, resource_version: str) -> list[Json]:
    """Extract cited resource facts through the same provider-shaped contract as messages.

    The local implementation is deterministic for CI. OSS/OpenAI-compatible
    providers should emit the same fields so storage, indexes, and replay stay
    unchanged.
    """
    mode = resource_extraction_mode(envelope)
    provider = understanding_provider(envelope)
    if provider in {"openai", "openai_compatible", "openai_compatible_llm"}:
        try:
            model_facts = openai_compatible_resource_facts(
                chunk,
                chunk_metadata=chunk_metadata,
                envelope=envelope,
                raw_uri=raw_uri,
                resource_version=resource_version,
            )
            if model_facts or require_oss_understanding():
                return model_facts
        except MatrixArkError:
            if require_oss_understanding():
                raise
    facts: list[Json] = []
    for fact_schema in matched_resource_fact_schemas(chunk.text, chunk.metadata):
        fact_event_type = str(fact_schema["fact_type"])
        fact_entity_type = str(fact_schema["entity_type"])
        fact_value = extract_resource_fact_value(chunk.text, fact_event_type)
        facts.append(
            {
                "mode": mode,
                "classification": "RESOURCE_FACT",
                "event_type": fact_event_type,
                "entity_type": fact_entity_type,
                "status": "observed",
                "value": fact_value,
                "entity_name": resource_fact_entity_name(fact_schema, fact_value, chunk_metadata, raw_uri),
                "confidence": 0.82 if fact_event_type != "resource_fact" else 0.68,
                "source_chunk_hash": chunk.chunk_hash,
                "source_ref": chunk.source_ref,
                "resource_version": resource_version,
                "extraction_provider": understanding_provider(envelope),
            }
        )
    return facts
