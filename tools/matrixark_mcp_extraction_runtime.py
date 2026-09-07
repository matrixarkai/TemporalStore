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


try:  # the implementation lives in matrixark_mcp_core_extraction; this module re-exports it
    from tools.matrixark_mcp_core_extraction import openai_compatible_one_pass_memory_extraction
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_extraction import openai_compatible_one_pass_memory_extraction


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


try:  # the implementation lives in matrixark_mcp_core_extraction; this module re-exports it
    from tools.matrixark_mcp_core_extraction import compact_internal_extraction
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core_extraction import compact_internal_extraction


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


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import infer_event_type
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import infer_event_type



def resource_extraction_mode(envelope: Json) -> str:
    provider = understanding_provider(envelope)
    if provider == "oss_encoder":
        return "matrixark_resource_schema_oss_encoder"
    if provider in {"openai", "openai_compatible", "openai-compatible"}:
        return "matrixark_resource_schema_openai_compatible"
    return "matrixark_resource_schema"


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from tools.matrixark_mcp_core import extract_resource_facts
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import extract_resource_facts
