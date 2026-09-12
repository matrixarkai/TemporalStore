#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk memory segmentation helpers."""

from __future__ import annotations

from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_embeddings import embedding_model_name
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_extraction_provider import parse_first_json_object
    from tools.matrixark_mcp_oss_understanding import oss_encoder_memory_segments, require_oss_understanding
    from tools.matrixark_mcp_scoring import tokens
    from tools.matrixark_mcp_summaries import summarize_text
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_embeddings import embedding_model_name
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_extraction_provider import parse_first_json_object
    from matrixark_mcp_oss_understanding import oss_encoder_memory_segments, require_oss_understanding
    from matrixark_mcp_scoring import tokens
    from matrixark_mcp_summaries import summarize_text


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from tools.matrixark_mcp_core import detect_memory_segments
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import detect_memory_segments


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import build_segment_prompt
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import build_segment_prompt


# `semantic_saliency_score` had a copy here whose keyword list matched `control_state` where
# core's matches `risk`. Neither is a typo: that is the same open question recorded against
# RESOURCE_FACT_KEYWORDS in test_there_is_one_copy_of_each_helper, where it is blocked on
# RESOURCE_FACT_SCHEMAS differing per host. Re-exporting does not answer it -- it takes this
# module out of the argument, so the answer only has to be written in one place.
try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from tools.matrixark_mcp_core import oss_model_memory_segments, semantic_saliency_score
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import oss_model_memory_segments, semantic_saliency_score


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from tools.matrixark_mcp_core import normalize_model_segments
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import normalize_model_segments


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import normalize_coordinate_tuples
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import normalize_coordinate_tuples


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import normalize_message_indexes
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import normalize_message_indexes

# NOT re-exported, unlike the four names above, and this is the interesting one.
#
# matrixark_mcp_core defines `intelligent_memory_segments` too, and the two bodies differ by two
# dict fields that ONLY this copy sets:
#
#     "segment_origin": "semantic_derived_from_events",
#     "derived_from_context_events": True,
#
# Those two fields are read by live code -- matrixark_mcp_context_pack copies them onto pack items
# in three places, matrixark_local_adapter_retrieve emits them, and
# matrixark_mcp_local_batch_extract_runtime falls back through `segment_origin` to `detected_by`.
# So the ORPHAN is the fuller copy here, which is exactly the case the guard in
# test_an_unreachable_module_does_not_hold_a_diverged_copy warns about and the reason the other two
# were checked one at a time rather than swept.
#
# It also says something about the LIVE path rather than this one: matrixark_mcp_core sets
# segment_origin="fallback_derived_from_events" on its fallback segment path and sets nothing on
# the semantic path, so a live semantic segment reaches the pack with no origin at all. Whether
# that is a hole or a deliberate silence is a question about the pack, not about this module, and
# adopting these two fields into the live function would change what every pack carries. That is
# not a consolidation, so it is not done here.


def intelligent_memory_segments(messages: list[Json]) -> list[Json]:
    """Segment a batch into salient, event-centric memories.

    The production provider can emit the same coordinate tuples from one LLM
    call. The local implementation does deterministic semantic saliency and
    topic grouping, including non-contiguous segment consolidation.
    """

    salient: list[tuple[int, Json, str, str, float]] = []
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        saliency = semantic_saliency_score(text)
        if saliency < 0.5:
            continue
        topic = infer_segment_topic(text)
        salient.append((index, message, text, topic, saliency))
    grouped: dict[str, list[tuple[int, Json, str, float]]] = {}
    for index, message, text, topic, saliency in salient:
        grouped.setdefault(topic, []).append((index, message, text, saliency))

    segments = []
    for topic, items in grouped.items():
        if not items:
            continue
        coordinate_tuples = contiguous_ranges([item[0] for item in items])
        segment_text = "\n".join(f"{index}: {text}" for index, _message, text, _score in items)
        avg_saliency = sum(item[3] for item in items) / len(items)
        segments.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": [item[0] for item in items],
                "saliency_score": round(avg_saliency, 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
                "segment_origin": "semantic_derived_from_events",
                "derived_from_context_events": True,
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import infer_segment_topic
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import infer_segment_topic


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import contiguous_ranges
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import contiguous_ranges
