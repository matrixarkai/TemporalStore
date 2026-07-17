#!/usr/bin/env python3
"""Event candidate scan for MatrixArk local retrieval."""

from __future__ import annotations

from typing import Any, Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        cosine,
        hybrid_origin_score,
        passes_secondary_index_filters,
        score_recall_candidate,
        sparse_lexical_score,
        tokens,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        access_scope_matches_before_scoring,
        candidate_access_scope,
        candidate_index_terms,
        cosine,
        hybrid_origin_score,
        passes_secondary_index_filters,
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


def scan_event_candidates(
    tree_candidate_records: list[Json],
    *,
    raw_event_ids_by_node: dict[Any, set[int]],
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
    event_embedding_vectors: dict[int, list[float]],
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
            return primary_matches, auxiliary_matches, secondary_index_dropped_count, secondary_index_matched_count, "deadline_during_event_scan"
        if record.get("record_type") != "context_event":
            continue
        event_node_key: Any = record.get("node_hash")
        if event_node_key is None:
            event_node_key = tuple(record.get("node_path", []))
        if (
            not record.get("source_chunk_hash")
            and event_node_key in raw_event_ids_by_node
            and int(record.get("event_id_hash") or 0) not in raw_event_ids_by_node[event_node_key]
        ):
            continue
        envelope = record.get("envelope", {}) if isinstance(record.get("envelope"), dict) else {}
        record_scope = candidate_access_scope(record)
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
        text = str(record.get("text", ""))
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        embedding_score = cosine(query_embedding, event_embedding_vectors.get(record["event_id_hash"], []))
        node_score = node_scores.get(record["node_hash"], {}).get("score", 0.0)
        origin_score = hybrid_origin_score(query_terms, text, embedding_score, node_score)
        event_type = str(record.get("event_type") or record.get("classification") or "")
        candidate_metadata: Json = {}
        record_metadata = record.get("metadata")
        envelope_metadata = envelope.get("metadata")
        if isinstance(record_metadata, dict):
            candidate_metadata.update(record_metadata)
        if isinstance(envelope_metadata, dict):
            candidate_metadata.update(envelope_metadata)
        candidate = candidate_builders.event_candidate(
            record,
            envelope=envelope,
            record_scope=record_scope,
            index_terms=index_terms,
            event_type=event_type,
            origin_score=origin_score,
            keyword_score=keyword_score,
            sparse_score=sparse_score,
            embedding_score=embedding_score,
            node_score=node_score,
            metadata=candidate_metadata,
            text=text,
        )
        if origin_score > 0:
            primary_matches.append(
                score_recall_candidate(
                    annotate_session_continuity({**candidate, "recall_path": "primary_hybrid"}, record),
                    ranking,
                    reference_time_ms=reference_time_ms,
                )
            )
        graph_text = " ".join(record.get("node_path", []) + sorted(index_terms) + [event_type, text])
        graph_score = sparse_lexical_score(query_terms, graph_text)
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
