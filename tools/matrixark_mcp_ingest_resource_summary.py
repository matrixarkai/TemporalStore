#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Resource summary/index orchestration for MatrixArk local ingest."""

from __future__ import annotations

from typing import Any

try:
    from tools.matrixark_mcp_core import (
        Json,
        context_index_name,
        context_index_posting_record,
        embedding_for_text,
        ordered_unique,
        stable_hash,
        summarize_resource_chunks,
        summarize_text,
        take_secondary_index_terms,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import (
        Json,
        context_index_name,
        context_index_posting_record,
        embedding_for_text,
        ordered_unique,
        stable_hash,
        summarize_resource_chunks,
        summarize_text,
        take_secondary_index_terms,
    )

try:
    from tools import matrixark_mcp_ingest_resource_records as resource_record_builders
except ModuleNotFoundError:  # Direct script execution from tools/.
    import matrixark_mcp_ingest_resource_records as resource_record_builders


def append_resource_summary_and_indexes(
    adapter: Any,
    *,
    envelope: Json,
    parsed_chunks: list[Any],
    raw_uri: str,
    resource_type: str,
    resource_import_task_hash: int,
    node_hash: int,
    node_path: list[str],
    resource_record_scope: Json,
    skill_hash: int | None,
    skill_name: str,
    skill_metadata: Json,
    secondary_index_budget: Json,
) -> Json:
    resource_kind = "skill" if skill_hash is not None else "resource"
    resource_l0_text = summarize_text(
        summarize_resource_chunks(parsed_chunks, raw_uri=raw_uri, resource_kind=resource_kind),
        limit=700,
    )
    resource_summary_hash = stable_hash(f"{resource_kind}_l0:{raw_uri}:{node_hash}")
    resource_summary_vector = embedding_for_text(" ".join(node_path + [resource_l0_text]))
    adapter.append(
        resource_record_builders.resource_l0_summary_record(
            resource_kind=resource_kind,
            summary_hash=resource_summary_hash,
            import_task_hash=resource_import_task_hash,
            node_hash=node_hash,
            node_path=node_path,
            raw_uri=raw_uri,
            summary_text=resource_l0_text,
            source_chunk_hashes=[chunk.chunk_hash for chunk in parsed_chunks],
            scope=resource_record_scope,
            updated_at_ms=envelope["ingestion_time_ms"],
        )
    )
    adapter.append(
        resource_record_builders.context_embedding_record(
            embedding_type=f"{resource_kind}_l0",
            ref_type="summary",
            ref_hash=resource_summary_hash,
            node_hash=node_hash,
            node_path=node_path,
            vector=resource_summary_vector,
            scope=resource_record_scope,
            updated_at_ms=envelope["ingestion_time_ms"],
        )
    )
    resource_dirty_hashes = adapter.mark_node_summary_dirty(
        node_path=node_path,
        scope=envelope["scope"],
        updated_at_ms=envelope["ingestion_time_ms"],
        source_ref_type=f"{resource_kind}_summary",
        source_hash_field="source_summary_hash",
        source_hash=resource_summary_hash,
        dirty_reason=f"{resource_kind}_update",
    )
    raw_resource_indexes = ordered_unique(
        [
            context_index_name("source_type", envelope["kind"]),
            context_index_name("resource_type", resource_type or parsed_chunks[0].metadata.get("resource_type", "txt")),
        ]
        + (
            [context_index_name("skill_name", skill_name)]
            + [context_index_name("skill_trigger", trigger) for trigger in skill_metadata.get("triggers", [])]
            + [context_index_name("skill_tool", tool) for tool in skill_metadata.get("allowed_tools", [])]
            if skill_hash is not None
            else []
        )
    )
    index_write_count = 0
    resource_indexes = take_secondary_index_terms(raw_resource_indexes, secondary_index_budget)
    for index_name in resource_indexes:
        index_write_count += 1
        adapter.append(
            context_index_posting_record(
                index_name=index_name,
                capability=f"{resource_kind}_summary",
                ref_type="summary",
                ref_hashes=[resource_summary_hash],
                node_hash=node_hash,
                scope=resource_record_scope,
                updated_at_ms=envelope["ingestion_time_ms"],
                storage_options=envelope.get("storage_options", {}),
            )
        )
    return {
        "resource_kind": resource_kind,
        "resource_summary_hash": resource_summary_hash,
        "resource_dirty_hashes": resource_dirty_hashes,
        "index_candidate_count": len(raw_resource_indexes),
        "index_write_count": index_write_count,
    }
