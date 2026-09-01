#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Context-node score helpers for MatrixArk retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import (
        record_vector,
        Json,
        candidate_access_scope,
        clamp01,
        cosine,
        normalized_dense_score,
        scope_matches,
        sparse_lexical_score,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        record_vector,
        Json,
        candidate_access_scope,
        clamp01,
        cosine,
        normalized_dense_score,
        scope_matches,
        sparse_lexical_score,
    )


def add_context_summary_text(
    record: Json,
    *,
    scope: Json,
    node_summary_text_by_hash: dict[int, str],
) -> bool:
    if record.get("record_type") != "context_summary":
        return False
    if not scope_matches(candidate_access_scope(record), scope):
        return False
    summary_type = str(record.get("summary_type", ""))
    if summary_type not in {"node_l0", "node_l1", "batch_l0", "session_l0"}:
        return False
    try:
        node_hash = int(record.get("node_hash"))
    except (TypeError, ValueError):
        return False
    existing = node_summary_text_by_hash.get(node_hash, "")
    summary_text = str(record.get("summary_text", ""))
    if len(summary_text) > len(existing):
        node_summary_text_by_hash[node_hash] = summary_text
    return True


def add_node_embedding_score(
    record: Json,
    *,
    query_embedding: list[float],
    query_terms: set[str],
    node_summary_text_by_hash: dict[int, str],
    secondary_index_prefilter_node_hashes: set[int],
    node_scores: dict[int, Json],
) -> bool:
    if record.get("record_type") != "context_embedding":
        return False
    if record.get("embedding_type") not in {"node_l0", "node_l1"}:
        return False
    dense_score = cosine(query_embedding, record_vector(record))
    node_hash = record["node_hash"]
    node_text = " ".join(record.get("node_path", [])) + " " + node_summary_text_by_hash.get(node_hash, "")
    sparse_score = sparse_lexical_score(query_terms, node_text)
    index_hint_boost = 0.08 if node_hash in secondary_index_prefilter_node_hashes else 0.0
    score = round(clamp01(0.72 * normalized_dense_score(dense_score) + 0.28 * sparse_score + index_hint_boost), 6)
    current = node_scores.get(node_hash)
    if current is None or score > current["score"]:
        node_scores[node_hash] = {
            "node_hash": node_hash,
            "node_path": record.get("node_path", []),
            "depth": record.get("depth", len(record.get("node_path", []))),
            "score": score,
            "dense_score": dense_score,
            "sparse_score": sparse_score,
            "embedding_type": record.get("embedding_type"),
        }
    return True


def add_secondary_index_hint_node_scores(
    records: list[Json],
    *,
    secondary_index_prefilter_node_hashes: set[int],
    node_scores: dict[int, Json],
) -> None:
    for record in records:
        if record.get("record_type") != "context_node":
            continue
        try:
            node_hash = int(record.get("node_hash"))
        except (TypeError, ValueError):
            continue
        if node_hash not in secondary_index_prefilter_node_hashes or node_hash in node_scores:
            continue
        node_scores[node_hash] = {
            "node_hash": node_hash,
            "node_path": record.get("node_path", []),
            "depth": record.get("depth", len(record.get("node_path", []))),
            "score": 0.58,
            "dense_score": 0.0,
            "sparse_score": 0.0,
            "embedding_type": "secondary_index_hint",
        }
