#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Temporal compression candidate scan for MatrixArk local retrieval."""

from __future__ import annotations

from typing import Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_index_terms,
        cosine,
        embedding_for_text,
        hybrid_origin_score,
        passes_secondary_index_filters,
        score_recall_candidate,
        sparse_lexical_score,
        summarize_text,
        tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_index_terms,
        cosine,
        embedding_for_text,
        hybrid_origin_score,
        passes_secondary_index_filters,
        score_recall_candidate,
        sparse_lexical_score,
        summarize_text,
        tokens,
    )

try:
    from tools import matrixark_mcp_retrieve_candidate_builders as candidate_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_candidate_builders as candidate_builders


RecordPredicate = Callable[[Json], bool]
DeadlinePredicate = Callable[[], bool]
CandidateAnnotator = Callable[[Json, Json], Json]


def scan_compression_candidates(
    tree_candidate_records: list[Json],
    *,
    retrieval_scope: Json,
    selected_by_tree: RecordPredicate,
    index_terms_by_batch: dict[object, list[str]],
    index_terms_by_node: dict[object, list[str]],
    index_terms_by_ref: dict[object, list[str]],
    secondary_index_filter_groups: list[list[str]],
    secondary_index_filter_mode: str,
    admit_candidate_for_node: RecordPredicate,
    query_terms: set[str],
    query_embedding: list[float],
    compression_embedding_vectors: dict[int, list[float]],
    node_scores: dict[int, Json],
    annotate_session_continuity: CandidateAnnotator,
    ranking: Json,
    reference_time_ms: int,
    deadline_exceeded: DeadlinePredicate,
) -> tuple[list[Json], list[Json], int, int, str]:
    primary_matches: list[Json] = []
    auxiliary_matches: list[Json] = []
    secondary_index_dropped_count = 0
    secondary_index_matched_count = 0
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return (
                primary_matches,
                auxiliary_matches,
                secondary_index_dropped_count,
                secondary_index_matched_count,
                "deadline_during_compression_scan",
            )
        if record.get("record_type") != "context_compression_event":
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        if not passes_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        text = f"TIME_COMPRESS: {summarize_text(str(record.get('summary_text', '')), limit=96)}"
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        compression_hash = int(record.get("compression_id_hash") or 0)
        embedding_score = cosine(query_embedding, compression_embedding_vectors.get(compression_hash, embedding_for_text(text)))
        node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
        origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
        candidate = candidate_builders.compression_candidate(
            record,
            compression_hash=compression_hash,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            text=text,
        )
        candidate["matched_index_terms"] = sorted(index_terms)
        if origin_score > 0:
            primary_matches.append(
                score_recall_candidate(
                    annotate_session_continuity({**candidate, "recall_path": "primary_time_compression"}, record),
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
        graph_score = sparse_lexical_score(query_terms, " ".join(record.get("node_path", []) + sorted(index_terms) + [text, "time_compress"]))
        if graph_score > 0:
            auxiliary_matches.append(
                score_recall_candidate(
                    {
                        **annotate_session_continuity(candidate, record),
                        "recall_path": "auxiliary_keyword_graph",
                        "origin_score": graph_score,
                        "keyword_graph_score": graph_score,
                    },
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
    return primary_matches, auxiliary_matches, secondary_index_dropped_count, secondary_index_matched_count, ""
