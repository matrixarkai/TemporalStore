#!/usr/bin/env python3
"""Scan-state helpers for MatrixArk local retrieval."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

try:
    from tools.matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        passes_secondary_index_filters,
        scope_matches,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        candidate_access_scope,
        passes_secondary_index_filters,
        scope_matches,
    )

try:
    from tools.matrixark_mcp_retrieve_embeddings import add_context_embedding_vector
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_embeddings import add_context_embedding_vector

try:
    from tools.matrixark_mcp_retrieve_index_terms import add_context_index_terms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_index_terms import add_context_index_terms

try:
    from tools.matrixark_mcp_retrieve_node_scores import (
        add_context_summary_text,
        add_node_embedding_score,
        add_secondary_index_hint_node_scores,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_retrieve_node_scores import (
        add_context_summary_text,
        add_node_embedding_score,
        add_secondary_index_hint_node_scores,
    )


DeadlineExceeded = Callable[[], bool]


@dataclass
class RetrieveScanState:
    node_scores: dict[int, Json] = field(default_factory=dict)
    event_embedding_vectors: dict[int, list[float]] = field(default_factory=dict)
    entity_embedding_vectors: dict[int, list[float]] = field(default_factory=dict)
    segment_embedding_vectors: dict[int, list[float]] = field(default_factory=dict)
    compression_embedding_vectors: dict[int, list[float]] = field(default_factory=dict)
    resource_embedding_vectors: dict[int, list[float]] = field(default_factory=dict)
    skill_embedding_vectors: dict[int, list[float]] = field(default_factory=dict)
    index_terms_by_batch: dict[Any, list[str]] = field(default_factory=dict)
    index_terms_by_node: dict[Any, list[str]] = field(default_factory=dict)
    index_terms_by_ref: dict[Any, list[str]] = field(default_factory=dict)
    index_terms_by_node_for_prefilter: dict[int, list[str]] = field(default_factory=dict)
    node_summary_text_by_hash: dict[int, str] = field(default_factory=dict)


def scan_context_indexes(
    records: list[Json],
    *,
    retrieval_scope: Json,
    scope: Json,
    query_plan: Json,
    secondary_index_filter_groups: list[list[str]],
    secondary_index_filter_mode: str,
    state: RetrieveScanState,
    deadline_exceeded: DeadlineExceeded,
) -> tuple[set[int], str]:
    for scan_index, record in enumerate(records, 1):
        if scan_index % 128 == 0 and deadline_exceeded():
            return set(), "deadline_during_embedding_index_scan"
        record_type = record.get("record_type")
        if record_type == "context_index" and scope_matches(candidate_access_scope(record), retrieval_scope):
            add_context_index_terms(
                record,
                index_terms_by_batch=state.index_terms_by_batch,
                index_terms_by_node=state.index_terms_by_node,
                index_terms_by_ref=state.index_terms_by_ref,
                index_terms_by_node_for_prefilter=state.index_terms_by_node_for_prefilter,
            )
        add_context_summary_text(record, scope=scope, node_summary_text_by_hash=state.node_summary_text_by_hash)
    secondary_index_prefilter_node_hashes = {
        node_hash
        for node_hash, terms in state.index_terms_by_node_for_prefilter.items()
        if passes_secondary_index_filters(set(terms), secondary_index_filter_groups, mode=secondary_index_filter_mode)
    } if secondary_index_filter_groups else set()
    query_plan["secondary_index_prefilter"] = {
        "applied_before_l0_l1_traversal": True,
        "matched_node_count": len(secondary_index_prefilter_node_hashes),
        "fallback_when_no_index_matches": True,
        "strategy": "ContextIndex node hints boost L0/L1 traversal; leaf candidates still verify filters before embedding scoring",
    }
    return secondary_index_prefilter_node_hashes, ""


def scan_context_embeddings(
    records: list[Json],
    *,
    scope: Json,
    query_embedding: list[float],
    query_terms: set[str],
    secondary_index_prefilter_node_hashes: set[int],
    state: RetrieveScanState,
    deadline_exceeded: DeadlineExceeded,
) -> str:
    for scan_index, record in enumerate(records, 1):
        if scan_index % 128 == 0 and deadline_exceeded():
            return "deadline_during_embedding_vector_scan"
        record_type = record.get("record_type")
        if record_type == "context_embedding" and not scope_matches(candidate_access_scope(record), scope):
            continue
        if record_type == "context_embedding" and record.get("embedding_type") in {"node_l0", "node_l1"}:
            add_node_embedding_score(
                record,
                query_embedding=query_embedding,
                query_terms=query_terms,
                node_summary_text_by_hash=state.node_summary_text_by_hash,
                secondary_index_prefilter_node_hashes=secondary_index_prefilter_node_hashes,
                node_scores=state.node_scores,
            )
        elif record_type == "context_embedding":
            add_embedding_vector(record, state=state)
    add_secondary_index_hint_node_scores(
        records,
        secondary_index_prefilter_node_hashes=secondary_index_prefilter_node_hashes,
        node_scores=state.node_scores,
    )
    return ""


def add_embedding_vector(record: Json, *, state: RetrieveScanState) -> None:
    add_context_embedding_vector(
        record,
        event_embedding_vectors=state.event_embedding_vectors,
        entity_embedding_vectors=state.entity_embedding_vectors,
        segment_embedding_vectors=state.segment_embedding_vectors,
        compression_embedding_vectors=state.compression_embedding_vectors,
        resource_embedding_vectors=state.resource_embedding_vectors,
        skill_embedding_vectors=state.skill_embedding_vectors,
    )


def add_index_terms(record: Json, *, state: RetrieveScanState) -> bool:
    return add_context_index_terms(
        record,
        index_terms_by_batch=state.index_terms_by_batch,
        index_terms_by_node=state.index_terms_by_node,
        index_terms_by_ref=state.index_terms_by_ref,
        index_terms_by_node_for_prefilter=state.index_terms_by_node_for_prefilter,
    )
