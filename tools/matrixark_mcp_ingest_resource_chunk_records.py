#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource/skill chunk record append helpers for MatrixArk local ingest."""

from __future__ import annotations
import os

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        Json,
        context_index_name,
        debug_resource_metadata,
        limited_index_terms,
        metadata_index_terms,
        ordered_unique,
        serving_resource_metadata,
        secondary_index_budget_summary,
        source_locator_from_ref,
        take_secondary_index_terms,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        Json,
        context_index_name,
        debug_resource_metadata,
        limited_index_terms,
        metadata_index_terms,
        ordered_unique,
        serving_resource_metadata,
        secondary_index_budget_summary,
        source_locator_from_ref,
        take_secondary_index_terms,
    )

try:
    from tools import matrixark_mcp_ingest_resource_records as resource_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_records as resource_record_builders


# One append per record meant one backend write per record: a 2126-chunk skill emits
# 8504 of them, four per chunk. append() is defined as append_many([record]), so the
# records can be gathered and handed over in batches without changing what is written
# or the order it is written in.
DEDUPE_SKILL_CHUNK_EMBEDDING = os.environ.get(
    "MATRIXARK_DEDUPE_SKILL_CHUNK_EMBEDDING", "1"
) not in {"0", "false", "False", ""}

RESOURCE_APPEND_BATCH_RECORDS = int(
    os.environ.get("MATRIXARK_RESOURCE_APPEND_BATCH_RECORDS", "512")
)


def _flush_pending_records(adapter: Any, pending: list) -> list:
    """Hand the gathered records to the adapter and start a fresh batch."""
    if not pending:
        return pending
    append_many = getattr(adapter, "append_many", None)
    if callable(append_many):
        append_many(pending)
    else:
        for record in pending:
            adapter.append(record)
    return []


def append_resource_chunk_records(
    adapter: Any,
    *,
    envelope: Json,
    parsed_chunks: list[Any],
    chunk_vectors: list[list[float]],
    raw_uri: str,
    raw_uri_hash: int,
    resource_type: str,
    resource_manifest_hash: int,
    resource_import_task_hash: int,
    node_hash: int,
    node_path: list[str],
    access_scope: Json,
    deployment_scope: str,
    resource_record_scope: Json,
    skill_hash: int | None,
    skill_name: str,
    skill_metadata: Json,
    secondary_index_budget: Json,
) -> Json:
    resource_chunk_hashes: list[int] = []
    pending_records: list = []
    index_candidate_count = 0
    index_write_count = 0
    index_dropped_by_cap_count = 0
    for chunk, vector in zip(parsed_chunks, chunk_vectors):
        resource_chunk_hashes.append(chunk.chunk_hash)
        source_locator = source_locator_from_ref(chunk.source_ref, raw_uri)
        chunk_metadata_source = {**chunk.metadata, "source_locator": source_locator}
        chunk_metadata = serving_resource_metadata(chunk_metadata_source)
        chunk_debug_metadata = debug_resource_metadata(chunk.metadata)
        chunk_resource_hash = resource_manifest_hash if skill_hash is None else skill_hash
        if skill_hash is not None:
            pending_records.append(
                resource_record_builders.skill_section_record(
                    import_task_hash=resource_import_task_hash,
                    skill_hash=skill_hash,
                    section_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    raw_uri_hash=raw_uri_hash,
                    source_locator=source_locator,
                    heading=str(chunk_metadata.get("heading", "")),
                    text=chunk.text,
                    token_estimate=chunk.token_estimate,
                    metadata=chunk_metadata,
                    access_scope=access_scope,
                    deployment_scope=deployment_scope,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        pending_records.append(
            resource_record_builders.resource_chunk_record(
                import_task_hash=resource_import_task_hash,
                chunk_hash=chunk.chunk_hash,
                node_hash=node_hash,
                node_path=node_path,
                resource_hash=chunk_resource_hash,
                raw_uri_hash=raw_uri_hash,
                resource_type=str(chunk_metadata.get("resource_type") or resource_type),
                source_locator=source_locator,
                text=chunk.text,
                token_estimate=chunk.token_estimate,
                metadata=chunk_metadata,
                access_scope=access_scope,
                deployment_scope=deployment_scope,
                scope=resource_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
            )
        )
        if chunk_debug_metadata:
            pending_records.append(
                resource_record_builders.resource_chunk_debug_record(
                    ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                    chunk_hash=chunk.chunk_hash,
                    import_task_hash=resource_import_task_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    resource_hash=chunk_resource_hash,
                    raw_uri_hash=raw_uri_hash,
                    raw_uri=raw_uri,
                    source_locator=source_locator,
                    source_ref=chunk.source_ref,
                    metadata_debug=chunk_debug_metadata,
                    text=chunk.text,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        # A skill chunk used to store the SAME vector twice, once as embedding_type
        # resource_chunk and once as skill_section, with the same ref_hash. Retrieval keys
        # its vector map on ref_hash ALONE and both land in it, so the second copy only ever
        # overwrote the first with an identical value - about 37% of what a skill ingest
        # writes, for nothing. Skills now store the skill_section copy only.
        if skill_hash is None or not DEDUPE_SKILL_CHUNK_EMBEDDING:
            pending_records.append(
                resource_record_builders.context_embedding_record(
                    embedding_type="resource_chunk",
                    ref_type="resource_chunk",
                    ref_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    vector=vector,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        if skill_hash is not None:
            pending_records.append(
                resource_record_builders.context_embedding_record(
                    embedding_type="skill_section",
                    ref_type="skill_section",
                    ref_hash=chunk.chunk_hash,
                    node_hash=node_hash,
                    node_path=node_path,
                    vector=vector,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        raw_chunk_index_terms = (
            [
                context_index_name("source_type", "skill" if skill_hash is not None else "resource"),
                context_index_name("resource_type", chunk_metadata.get("resource_type") or resource_type),
            ]
            + metadata_index_terms(chunk.metadata)
            + (
                [context_index_name("skill_name", skill_name)]
                + [context_index_name("skill_trigger", trigger) for trigger in skill_metadata.get("triggers", [])]
                + [context_index_name("skill_tool", tool) for tool in skill_metadata.get("allowed_tools", [])]
                if skill_hash is not None
                else []
            )
        )
        index_candidate_count += len([term for term in raw_chunk_index_terms if term])
        chunk_index_terms = limited_index_terms(
            raw_chunk_index_terms,
            limit=MAX_INDEX_TERMS_PER_RESOURCE_CHUNK,
        )
        index_dropped_by_cap_count += max(
            0,
            len(ordered_unique([term for term in raw_chunk_index_terms if term])) - len(chunk_index_terms),
        )
        chunk_index_terms = take_secondary_index_terms(chunk_index_terms, secondary_index_budget)
        for index_name in chunk_index_terms:
            index_write_count += 1
            pending_records.append(
                resource_record_builders.resource_chunk_index_record(
                    index_name=index_name,
                    ref_type="skill_section" if skill_hash is not None else "resource_chunk",
                    chunk_hash=chunk.chunk_hash,
                    resource_hash=chunk_resource_hash,
                    source_locator=source_locator,
                    node_hash=node_hash,
                    node_path=node_path,
                    scope=resource_record_scope,
                    updated_at_ms=envelope["ingestion_time_ms"],
                )
            )
        if len(pending_records) >= RESOURCE_APPEND_BATCH_RECORDS:
            pending_records = _flush_pending_records(adapter, pending_records)
    pending_records = _flush_pending_records(adapter, pending_records)
    # index_dropped_by_cap_count only counts terms lost to the PER-CHUNK term cap, which is
    # applied before the per-operation budget. The budget is what actually truncates a large
    # document: at its default of 128 records per ingest call, a 2,011-chunk document offers
    # 10,055 candidate terms and writes 128, and every count above reported that as no drop.
    # Report the budget alongside them so the truncation is visible to the caller.
    budget_summary = secondary_index_budget_summary(secondary_index_budget)
    return {
        "resource_chunk_hashes": resource_chunk_hashes,
        "index_candidate_count": index_candidate_count,
        "index_write_count": index_write_count,
        "index_dropped_by_cap_count": index_dropped_by_cap_count,
        "index_budget": budget_summary,
    }
