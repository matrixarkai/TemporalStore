#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""OSS/embedding-based MatrixArk understanding helpers."""

from __future__ import annotations

import json
import os
from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_embeddings import embedding_for_text, embedding_model_name
    from tools.matrixark_mcp_entity_ops import entity_patch
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_scoring import cosine, normalized_dense_score
    from tools.matrixark_mcp_summaries import summarize_text
    from tools.matrixark_mcp_text import text_from_messages
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_embeddings import embedding_for_text, embedding_model_name
    from matrixark_mcp_entity_ops import entity_patch
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_scoring import cosine, normalized_dense_score
    from matrixark_mcp_summaries import summarize_text
    from matrixark_mcp_text import text_from_messages


UNDERSTANDING_LABELS: dict[str, str] = {
    "confirmation": "confirmation approval accepted answer yes correct looks good",
    "correction": "correction wrong changed updated instead stale fact",
    "preference_update": "user preference likes prefers favorite language tool choice",
    "plan_update": "future plan schedule going to next step planned trip",
    "status_update": "job role work status position current responsibility",
    "approval": "business approval purchase approval budget approval confirmed cost",
    "location": "current location city moved to lives in staying at",
    "relationship": "relationship manager sister brother teammate family person",
    "family_profile": "family profile pet dog cat child sibling household fact",
    "current_plan": "current plan upcoming action task to complete next milestone",
    "session": "general conversation memory useful session fact",
}

_OSS_UNDERSTANDING_PROTOTYPE_CACHE: dict[str, dict[str, list[float]]] = {}


def _core_runtime() -> Any:
    try:
        from tools import matrixark_mcp_core as core
    except ModuleNotFoundError:  # Direct script execution from tools/.
        import matrixark_mcp_core as core
    return core


def require_oss_understanding() -> bool:
    return os.getenv("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "").strip().lower() in {"1", "true", "yes"}


# Not defined here: the implementation lives in matrixark_mcp_core and this module carried an
# identical second copy of each.
try:
    from tools.matrixark_mcp_core import (
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_rank_labels,
        prototype_vectors,
        understanding_provider,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_rank_labels,
        prototype_vectors,
        understanding_provider,
    )


def oss_encoder_extract_batch_entities(messages: list[Json], envelope: Json) -> list[Json]:
    core = _core_runtime()
    text = text_from_messages(messages)
    ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=8)
    source_event_ids = envelope.get("source_event_ids", [])
    source_refs = [str(ref) for ref in source_event_ids] if isinstance(source_event_ids, list) and source_event_ids else [str(index) for index, _ in enumerate(messages)]
    entities: list[Json] = []
    for item in ranked:
        label = str(item["label"])
        if label == "approval":
            entity_type = "approval_state"
        elif label == "status_update":
            entity_type = "job_status"
        elif label == "plan_update":
            entity_type = "current_plan"
        elif label == "preference_update":
            entity_type = "preference"
        else:
            entity_type = label
        if float(item["score"]) < 0.42 and entity_type != "session":
            continue
        state = summarize_text(f"{entity_type}: {text}", limit=220)
        entities.append(
            {
                "entity_type": entity_type,
                "entity_name": core.canonical_entity_name(entity_type, state) or entity_type,
                "state": state,
                "confidence": round(float(item["score"]), 6),
                "source_refs": source_refs,
                "operator": core.normalize_entity_operator(None, entity_type),
                "field_patches": [entity_patch("", state)] if entity_type != "session" else [],
                "extracted_by": "oss_encoder",
            }
        )
    if not entities:
        entities.append(
            {
                "entity_type": "session",
                "entity_name": "session_memory",
                "state": summarize_text(text, limit=220),
                "confidence": 0.5,
                "source_refs": source_refs,
                "operator": core.normalize_entity_operator(None, "session"),
                "field_patches": [],
                "extracted_by": "oss_encoder",
            }
        )
    return core.dedupe_entities(entities)


def oss_encoder_memory_segments(messages: list[Json]) -> list[Json]:
    core = _core_runtime()
    labeled: dict[str, list[tuple[int, Json, float]]] = {}
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        if not text.strip():
            continue
        ranked = oss_encoder_rank_labels(text, UNDERSTANDING_LABELS, limit=1)
        label = str(ranked[0]["label"]) if ranked else "session"
        score = float(ranked[0]["score"]) if ranked else 0.5
        labeled.setdefault(label, []).append((index, message, score))
    segments = []
    for label, items in labeled.items():
        indexes = [index for index, _message, _score in items]
        ranges = core.contiguous_ranges(indexes)
        segment_text = "\n".join(f"{index}: {message.get('content', '')}" for index, message, _score in items)
        segments.append(
            {
                "topic": label,
                "coordinate_tuples": ranges,
                "message_indexes": indexes,
                "saliency_score": round(sum(score for _index, _message, score in items) / max(len(items), 1), 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(ranges) > 1,
                "detected_by": "oss_encoder",
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]
