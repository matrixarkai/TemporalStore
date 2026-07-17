#!/usr/bin/env python3
"""Candidate payload builders for MatrixArk local retrieval."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, candidate_access_scope, clip_context_text, now_ms
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, candidate_access_scope, clip_context_text, now_ms


def event_candidate(
    record: Json,
    *,
    envelope: Json,
    record_scope: Json,
    index_terms: set[str],
    event_type: str,
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    metadata: Json,
    text: str,
) -> Json:
    return {
        "ref_type": "event",
        "ref_hash": record["event_id_hash"],
        "node_hash": record["node_hash"],
        "node_path": record.get("node_path", []),
        "origin_score": origin_score,
        "keyword_score": keyword_score,
        "sparse_score": sparse_score,
        "embedding_score": embedding_score,
        "node_score": node_score,
        "matched_index_terms": sorted(index_terms),
        "selection_reason": (
            "selected by tree path, secondary indexes, and resource fact/event hybrid score"
            if record.get("source_chunk_hash")
            else "selected by tree path, secondary indexes, and event hybrid score"
        ),
        "event_type": event_type,
        "context_class": "resource_fact" if record.get("source_chunk_hash") else "event",
        "source_chunk_hash": record.get("source_chunk_hash"),
        "source_ref": record.get("source_ref", ""),
        "metadata": metadata,
        "scope": record_scope,
        "updated_at_ms": record.get("updated_at_ms") or envelope.get("ingestion_time_ms", now_ms()),
        "text": clip_context_text(text),
    }


def entity_candidate(
    record: Json,
    *,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    return {
        "ref_type": "entity",
        "ref_hash": record["entity_hash"],
        "node_hash": record["node_hash"],
        "node_path": record.get("node_path", []),
        "origin_score": origin_score,
        "keyword_score": keyword_score,
        "sparse_score": sparse_score,
        "embedding_score": embedding_score,
        "node_score": node_score,
        "matched_index_terms": sorted(index_terms),
        "selection_reason": (
            "selected by tree path, secondary indexes, and resource entity state score"
            if record.get("source_chunk_hash")
            else "selected by tree path, secondary indexes, and entity state score"
        ),
        "entity_type": record.get("entity_type", ""),
        "entity_name": record.get("entity_name", ""),
        "context_class": "resource_entity_fact" if record.get("source_chunk_hash") else "entity",
        "source_chunk_hash": record.get("source_chunk_hash"),
        "source_ref": record.get("source_ref", ""),
        "metadata": record.get("metadata", {}),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(text),
    }


def segment_candidate(
    record: Json,
    *,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    saliency_score: float,
) -> Json:
    return {
        "ref_type": "segment",
        "ref_hash": record["segment_hash"],
        "node_hash": record["node_hash"],
        "node_path": record.get("node_path", []),
        "origin_score": origin_score,
        "keyword_score": keyword_score,
        "sparse_score": sparse_score,
        "embedding_score": embedding_score,
        "node_score": node_score,
        "matched_index_terms": sorted(index_terms),
        "selection_reason": "selected by tree path, secondary indexes, segment saliency, and segment hybrid score",
        "saliency_score": saliency_score,
        "topic": record.get("topic", ""),
        "coordinate_tuples": record.get("coordinate_tuples", []),
        "non_contiguous": record.get("non_contiguous", False),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(str(record.get("summary_text", ""))),
    }


def compression_candidate(
    record: Json,
    *,
    compression_hash: int,
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    return {
        "ref_type": "compression",
        "ref_hash": compression_hash,
        "node_hash": record["node_hash"],
        "node_path": record.get("node_path", []),
        "origin_score": origin_score,
        "keyword_score": keyword_score,
        "sparse_score": sparse_score,
        "embedding_score": embedding_score,
        "node_score": node_score,
        "event_type": "time_compress",
        "operator": "TIME_COMPRESS",
        "source_event_ids": record.get("source_event_ids", []),
        "source_start_ms": record.get("source_start_ms"),
        "source_end_ms": record.get("source_end_ms"),
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("compressed_time_ms", record.get("updated_at_ms", now_ms())),
        "text": clip_context_text(text),
    }


def resource_skill_candidate(
    record: Json,
    *,
    ref_type: str,
    ref_hash: int,
    resource_hash: int,
    source_locator: str,
    resource_version: str,
    supersedes_chunk_hash: object,
    version_state: str,
    stale_or_superseded: bool,
    citation: str,
    metadata: Json,
    business_type: str,
    index_terms: set[str],
    origin_score: float,
    keyword_score: int,
    sparse_score: float,
    embedding_score: float,
    node_score: float,
    text: str,
) -> Json:
    return {
        "ref_type": ref_type,
        "ref_hash": ref_hash,
        "node_hash": record.get("node_hash"),
        "node_path": record.get("node_path", []),
        "origin_score": origin_score,
        "keyword_score": keyword_score,
        "sparse_score": sparse_score,
        "embedding_score": embedding_score,
        "node_score": node_score,
        "matched_index_terms": sorted(index_terms),
        "selection_reason": (
            "selected by tree path, secondary indexes, and resource/skill hybrid score"
            if index_terms
            else "selected by tree path and resource/skill hybrid score"
        ),
        "event_type": business_type,
        "context_class": ref_type,
        "resource_hash": resource_hash,
        "source_locator": source_locator,
        "resource_type": record.get("resource_type", ""),
        "resource_version": resource_version,
        "supersedes_chunk_hash": supersedes_chunk_hash,
        "version_state": version_state,
        "stale_or_superseded": stale_or_superseded,
        "access_decision": "allowed_by_registry_scope_before_scoring",
        "access_scope": candidate_access_scope(record),
        "deployment_scope": record.get("deployment_scope", "local"),
        "citation": citation,
        "metadata": metadata,
        "scope": candidate_access_scope(record),
        "updated_at_ms": record.get("updated_at_ms", now_ms()),
        "text": clip_context_text(text),
        "recall_path": "primary_resource_skill",
    }
