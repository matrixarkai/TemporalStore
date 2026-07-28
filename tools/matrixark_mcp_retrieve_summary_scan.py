#!/usr/bin/env python3
"""Summary candidate scan for MatrixArk local retrieval."""

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
        passes_applicable_secondary_index_filters,
        score_recall_candidate,
        sparse_lexical_score,
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
        passes_applicable_secondary_index_filters,
        score_recall_candidate,
        sparse_lexical_score,
        tokens,
    )

try:
    from tools import matrixark_mcp_retrieve_candidate_builders as candidate_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_retrieve_candidate_builders as candidate_builders


RecordPredicate = Callable[[Json], bool]
DeadlinePredicate = Callable[[], bool]
CandidateAnnotator = Callable[[Json, Json], Json]

SUMMARY_TYPES = {"node_l0", "node_l1", "resource_l0", "batch_l0", "session_l0"}


def scan_summary_candidates(
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
    node_scores: dict[int, Json],
    annotate_session_continuity: CandidateAnnotator,
    ranking: Json,
    reference_time_ms: int,
    deadline_exceeded: DeadlinePredicate,
) -> tuple[list[Json], int, int, str]:
    primary_matches: list[Json] = []
    secondary_index_dropped_count = 0
    secondary_index_matched_count = 0
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return primary_matches, secondary_index_dropped_count, secondary_index_matched_count, "deadline_during_summary_scan"
        if record.get("record_type") != "context_summary":
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        summary_type = str(record.get("summary_type") or "")
        if summary_type not in SUMMARY_TYPES:
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        text = str(record.get("summary_text", ""))
        if not text:
            continue
        is_profile_summary_bridge = (
            str(record.get("memory_scope") or "") == "user_profile"
            and str(record.get("session_continuity") or "") == "cross_session"
        )
        lineage_text = " ".join(
            [
                str(record.get("memory_scope") or ""),
                str(record.get("session_continuity") or ""),
                *[str(value) for value in record.get("source_roles", []) if str(value or "")],
                *[str(value) for value in record.get("source_hook_types", []) if str(value or "")],
                *[str(value) for value in record.get("source_codex_events", []) if str(value or "")],
                *[str(value) for value in record.get("source_memory_scopes", []) if str(value or "")],
                *[str(value) for value in record.get("source_session_continuities", []) if str(value or "")],
                *[str(value) for value in record.get("source_entity_types", []) if str(value or "")],
                *[str(value).replace("_", " ") for value in record.get("source_entity_types", []) if str(value or "")],
                *sorted(index_terms),
            ]
        )
        summary_filter_text = " ".join([text, text.replace("_", " "), lineage_text])
        filter_terms = set(index_terms)
        if is_profile_summary_bridge:
            filter_terms.update(tokens(summary_filter_text))
        if not passes_applicable_secondary_index_filters(
            filter_terms,
            secondary_index_filter_groups,
            mode=secondary_index_filter_mode,
        ):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        sparse_score = sparse_lexical_score(query_terms, summary_filter_text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        embedding_score = cosine(query_embedding, embedding_for_text(" ".join(record.get("node_path", []) + [summary_type, text])))
        node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
        origin_score = min(1.0, 0.06 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
        if origin_score <= 0:
            continue
        candidate = candidate_builders.summary_candidate(
            record,
            summary_type=summary_type,
            index_terms=index_terms,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            text=text,
        )
        primary_matches.append(
            score_recall_candidate(
                annotate_session_continuity(candidate, record),
                ranking,
                reference_time_ms=reference_time_ms,
            )
        )
    return primary_matches, secondary_index_dropped_count, secondary_index_matched_count, ""
