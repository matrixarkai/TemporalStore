#!/usr/bin/env python3
"""Resource and skill candidate scan for MatrixArk local retrieval."""

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
        source_ref_from_locator,
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
        source_ref_from_locator,
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


def scan_resource_skill_candidates(
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
    resource_embedding_vectors: dict[int, list[float]],
    node_scores: dict[int, Json],
    annotate_session_continuity: CandidateAnnotator,
    ranking: Json,
    reference_time_ms: int,
    skill_controls: dict[int, Json],
    latest_resource_version_by_hash: dict[int, str],
    resource_uri_by_hash: dict[int, str],
    include_superseded_resources: bool,
    deadline_exceeded: DeadlinePredicate,
) -> tuple[list[Json], int, int, str]:
    primary_matches: list[Json] = []
    secondary_index_dropped_count = 0
    secondary_index_matched_count = 0
    for scan_index, record in enumerate(reversed(tree_candidate_records), 1):
        if scan_index % 64 == 0 and deadline_exceeded():
            return primary_matches, secondary_index_dropped_count, secondary_index_matched_count, "deadline_during_resource_skill_scan"
        if record.get("record_type") not in {"resource_chunk", "skill_section"}:
            continue
        if not access_scope_matches_before_scoring(record, retrieval_scope):
            continue
        if not selected_by_tree(record):
            continue
        if record.get("record_type") == "resource_chunk" and record.get("resource_type") == "skill":
            continue
        index_terms = candidate_index_terms(record, index_terms_by_batch, index_terms_by_node, index_terms_by_ref)
        if not passes_applicable_secondary_index_filters(index_terms, secondary_index_filter_groups, mode=secondary_index_filter_mode):
            secondary_index_dropped_count += 1
            continue
        secondary_index_matched_count += 1
        if not admit_candidate_for_node(record):
            continue
        if record.get("record_type") == "skill_section":
            ref_type = "skill_section"
            ref_hash = int(record.get("section_hash") or 0)
            parent_skill_hash = int(record.get("skill_hash") or 0)
            control = skill_controls.get(parent_skill_hash, {})
            if str(control.get("status") or "active") != "active":
                continue
            resource_hash = parent_skill_hash
            raw_uri_value = str(record.get("raw_uri") or "")
            source_locator = str(record.get("source_locator") or "")
            citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
            metadata = {**record.get("metadata", {}), "skill_registry": control}
            resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
            version_state = "current"
            is_superseded_version = False
            text = f"skill section {record.get('heading', '')}: {record.get('text', '')}"
            embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
            business_type = "skill"
        else:
            ref_type = "resource_chunk"
            ref_hash = int(record.get("chunk_hash") or 0)
            metadata = record.get("metadata", {})
            resource_hash = int(record.get("resource_hash") or 0)
            raw_uri_value = str(record.get("raw_uri") or resource_uri_by_hash.get(resource_hash, ""))
            source_locator = str(record.get("source_locator") or metadata.get("source_locator") or "")
            citation = str(record.get("source_ref") or source_ref_from_locator(raw_uri_value, source_locator))
            resource_version_value = str(metadata.get("resource_version") or record.get("resource_version") or "")
            latest_version = latest_resource_version_by_hash.get(resource_hash, resource_version_value)
            is_superseded_version = bool(
                resource_version_value
                and latest_version
                and resource_version_value != latest_version
            )
            if is_superseded_version and not include_superseded_resources:
                secondary_index_dropped_count += 1
                continue
            version_state = "historical" if is_superseded_version else "current"
            text = f"resource {source_locator}: {record.get('text', '')}"
            embedding_score = cosine(query_embedding, resource_embedding_vectors.get(ref_hash, embedding_for_text(text)))
            business_type = str(record.get("resource_type") or "resource")
        sparse_score = sparse_lexical_score(query_terms, text)
        keyword_score = len(query_terms.intersection(tokens(text)))
        node_score = node_scores.get(record.get("node_hash"), {}).get("score", 0.0)
        origin_score = min(1.0, 0.08 + hybrid_origin_score(query_terms, text, embedding_score, node_score))
        if origin_score <= 0:
            continue
        candidate = candidate_builders.resource_skill_candidate(
            record,
            ref_type=ref_type,
            ref_hash=ref_hash,
            resource_hash=resource_hash,
            source_locator=source_locator,
            resource_version=resource_version_value,
            supersedes_chunk_hash=metadata.get("supersedes_chunk_hash"),
            version_state=version_state,
            stale_or_superseded=is_superseded_version,
            citation=citation,
            metadata=metadata,
            business_type=business_type,
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
