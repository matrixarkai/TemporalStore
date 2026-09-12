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


def require_oss_understanding() -> bool:
    return os.getenv("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "").strip().lower() in {"1", "true", "yes", "on"}


# Not defined here: the implementation lives in matrixark_mcp_core and this module carried an
# identical second copy of each.
#
# The last two joined them by losing a lazy accessor -- import matrixark_mcp_core inside the call
# and reach names off the module object. The only thing it bought was that this module's copies
# could say `core.dedupe_entities(...)` where the live ones say `dedupe_entities(...)`. That one
# token was the entire divergence between them.
#
# It looked like cycle avoidance and was not: this import block already binds core at module scope,
# and matrixark_mcp_core does not import this module at all.
try:
    from tools.matrixark_mcp_core import (
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_extract_batch_entities,
        oss_encoder_memory_segments,
        oss_encoder_rank_labels,
        prototype_vectors,
        understanding_provider,
    )
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        oss_encoder_compact_extraction,
        oss_encoder_event_type,
        oss_encoder_extract_batch_entities,
        oss_encoder_memory_segments,
        oss_encoder_rank_labels,
        prototype_vectors,
        understanding_provider,
    )


