#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Embedding-vector collection helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import record_vector, Json
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import record_vector, Json


def add_context_embedding_vector(
    record: Json,
    *,
    event_embedding_vectors: dict[int, list[float]],
    entity_embedding_vectors: dict[int, list[float]],
    segment_embedding_vectors: dict[int, list[float]],
    compression_embedding_vectors: dict[int, list[float]],
    resource_embedding_vectors: dict[int, list[float]],
    skill_embedding_vectors: dict[int, list[float]],
) -> bool:
    if record.get("record_type") != "context_embedding":
        return False
    ref_hash = record.get("ref_hash")
    vector = record_vector(record)
    embedding_type = record.get("embedding_type")
    if embedding_type == "event_text":
        event_embedding_vectors[ref_hash] = vector
    elif embedding_type == "entity_state":
        entity_embedding_vectors[ref_hash] = vector
    elif embedding_type == "segment_text":
        segment_embedding_vectors[ref_hash] = vector
    elif embedding_type == "compression_summary":
        compression_embedding_vectors[ref_hash] = vector
    elif embedding_type in {"resource_chunk", "skill_section"}:
        resource_embedding_vectors[ref_hash] = vector
    elif embedding_type == "skill_summary":
        skill_embedding_vectors[ref_hash] = vector
    else:
        return False
    return True
